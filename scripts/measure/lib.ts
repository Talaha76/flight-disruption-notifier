/** AWS-facing helpers for the measurement scripts. Results are written to results/*.json. */
import * as fs from 'node:fs';
import * as path from 'node:path';
import { randomBytes } from 'node:crypto';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { BatchGetCommand, DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DeleteMessageBatchCommand, GetQueueAttributesCommand, PurgeQueueCommand, ReceiveMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { CloudWatchLogsClient, GetQueryResultsCommand, StartQueryCommand } from '@aws-sdk/client-cloudwatch-logs';
import { GetFunctionConfigurationCommand, LambdaClient } from '@aws-sdk/client-lambda';
import { loadOutputs, type StackOutputs } from '../lib/outputs';
import { summarize } from './stats';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
export const RESULTS_DIR = path.resolve(__dirname, '..', '..', 'results');

export interface Ctx {
  out: StackOutputs & Record<string, string>;
  sqs: SQSClient;
  ddb: DynamoDBDocumentClient;
  logs: CloudWatchLogsClient;
  lambda: LambdaClient;
}

export function context(): Ctx {
  const out = loadOutputs() as StackOutputs & Record<string, string>;
  const region = out.region;
  return {
    out,
    sqs: new SQSClient({ region }),
    ddb: DynamoDBDocumentClient.from(new DynamoDBClient({ region })),
    logs: new CloudWatchLogsClient({ region }),
    lambda: new LambdaClient({ region }),
  };
}

export function newRunId(prefix: string): string {
  return `${prefix}-${new Date().toISOString().replace(/[-:]/g, '').slice(0, 15)}-${randomBytes(2).toString('hex')}`;
}

/** A flight date unique to this run (far future), so each run gets fresh flight partitions. */
export function runFlightDate(): string {
  const d = new Date(Date.UTC(2030, 0, 1) + Math.floor(Math.random() * 3000) * 86_400_000);
  return d.toISOString().slice(0, 10);
}

export function log(msg: string): void {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${msg}`);
}

export function writeResult(name: string, data: unknown): string {
  fs.mkdirSync(RESULTS_DIR, { recursive: true });
  const file = path.join(RESULTS_DIR, `${name}.json`);
  fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n');
  log(`results written to ${path.relative(process.cwd(), file)}`);
  return file;
}

export function arg(name: string, fallback?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  if (i >= 0 && process.argv[i + 1]) return process.argv[i + 1];
  return fallback;
}
export const numArg = (name: string, fallback: number) => Number(arg(name, String(fallback)));

/* ------------------------------------------------------------------ HTTP load */

export interface LoadOptions {
  apiUrl: string;
  count: number;
  eventsPerRequest: number;
  requestsPerSecond: number;
  makeEvent: (i: number) => Record<string, unknown> & { eventId: string };
  maxInFlight?: number;
}

export interface LoadResult {
  startedAt: number;
  finishedAt: number;
  requests: number;
  acceptedIds: string[];
  notAcceptedIds: string[];
  statusCounts: Record<string, number>;
  retries: number;
  requestLatencyMs: ReturnType<typeof summarize>;
}

/**
 * Sends `count` events to POST /flight-events at a fixed request rate. Throttled (429) or 5xx
 * requests are retried with backoff using the SAME eventIds (the pipeline is idempotent), so
 * every event is eventually accepted unless retries are exhausted.
 */
export async function sendLoad(o: LoadOptions): Promise<LoadResult> {
  const maxInFlight = o.maxInFlight ?? 64;
  const totalRequests = Math.ceil(o.count / o.eventsPerRequest);
  const statusCounts: Record<string, number> = {};
  const latencies: number[] = [];
  const accepted: string[] = [];
  const notAccepted: string[] = [];
  let retries = 0;
  let inFlight = 0;
  const startedAt = Date.now();
  const pending: Promise<void>[] = [];

  const sendOne = async (events: Record<string, unknown>[]) => {
    let remaining = events;
    for (let attempt = 0; attempt < 8 && remaining.length > 0; attempt++) {
      if (attempt > 0) {
        retries++;
        await sleep(Math.min(8000, 250 * 2 ** attempt) + Math.random() * 250);
      }
      const t0 = Date.now();
      let status = 'network-error';
      try {
        const res = await fetch(`${o.apiUrl}/flight-events`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ events: remaining }) });
        status = String(res.status);
        const body = (await res.json().catch(() => ({}))) as { accepted?: string[] };
        if (res.status === 202 || res.status === 207) {
          latencies.push(Date.now() - t0);
          const ok = new Set(body.accepted ?? []);
          accepted.push(...[...ok]);
          remaining = remaining.filter((e) => !ok.has(e.eventId as string));
        } else if (res.status === 400) {
          statusCounts[status] = (statusCounts[status] ?? 0) + 1;
          break; // not retryable
        }
      } catch {
        /* retry */
      }
      statusCounts[status] = (statusCounts[status] ?? 0) + 1;
    }
    notAccepted.push(...remaining.map((e) => e.eventId as string));
  };

  for (let r = 0; r < totalRequests; r++) {
    const due = startedAt + (r * 1000) / o.requestsPerSecond;
    const wait = due - Date.now();
    if (wait > 0) await sleep(wait);
    while (inFlight >= maxInFlight) await sleep(5);
    const events = [];
    for (let i = r * o.eventsPerRequest; i < Math.min(o.count, (r + 1) * o.eventsPerRequest); i++) events.push(o.makeEvent(i));
    inFlight++;
    pending.push(sendOne(events).finally(() => inFlight--));
    if ((r + 1) % Math.max(1, Math.round(o.requestsPerSecond * 5)) === 0) log(`  sent ${r + 1}/${totalRequests} requests (${accepted.length} events accepted so far)`);
  }
  await Promise.all(pending);
  return {
    startedAt, finishedAt: Date.now(), requests: totalRequests, acceptedIds: accepted, notAcceptedIds: notAccepted,
    statusCounts, retries, requestLatencyMs: summarize(latencies),
  };
}

/* ------------------------------------------------------------------ queue monitor */

export class QueueMonitor {
  samples: { t: number; visible: number; inFlight: number }[] = [];
  peakVisible = 0;
  peakTotal = 0;
  /** First poll of the final run of 3 consecutive empty polls after the load finished. */
  drainedAt?: number;
  private loadDone = false;
  private stopped = false;
  private finished = false;

  constructor(private ctx: Ctx, private queueUrl: string, private pollMs = 2000) {}

  start(): void {
    void (async () => {
      let zeros = 0;
      while (!this.stopped) {
        try {
          const res = await this.ctx.sqs.send(new GetQueueAttributesCommand({
            QueueUrl: this.queueUrl,
            AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible', 'ApproximateNumberOfMessagesDelayed'],
          }));
          const visible = Number(res.Attributes?.ApproximateNumberOfMessages ?? 0) + Number(res.Attributes?.ApproximateNumberOfMessagesDelayed ?? 0);
          const inFlight = Number(res.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
          const t = Date.now();
          this.samples.push({ t, visible, inFlight });
          this.peakVisible = Math.max(this.peakVisible, visible);
          this.peakTotal = Math.max(this.peakTotal, visible + inFlight);
          if (this.loadDone && visible + inFlight === 0) {
            if (zeros === 0) this.drainedAt = t;
            zeros++;
            if (zeros >= 3) break;
          } else {
            zeros = 0;
            this.drainedAt = undefined;
          }
        } catch (err) {
          log(`  queue poll failed (${(err as Error).name}); retrying`);
        }
        await sleep(this.pollMs);
      }
      this.finished = true;
    })();
  }

  markLoadDone(): void { this.loadDone = true; }

  async waitDrained(timeoutMs: number): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    let lastLog = Date.now();
    while (!this.finished) {
      if (Date.now() > deadline) { this.stopped = true; throw new Error('Timed out waiting for the queue to drain'); }
      if (Date.now() - lastLog > 10000) {
        const s = this.samples[this.samples.length - 1];
        if (s) log(`  queue depth: ${s.visible} waiting, ${s.inFlight} in flight (peak ${this.peakTotal})`);
        lastLog = Date.now();
      }
      await sleep(500);
    }
  }
}

export async function queueDepth(ctx: Ctx, queueUrl: string): Promise<number> {
  const res = await ctx.sqs.send(new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'] }));
  return Number(res.Attributes?.ApproximateNumberOfMessages ?? 0) + Number(res.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
}

/* ------------------------------------------------------------------ completion check */

export interface CompletionReport {
  expected: number;
  completed: number;
  outcomes: Record<string, number>;
  notCompletedIds: string[];
  firstStartedAt?: string;
  lastCompletedAt?: string;
}

/**
 * Proof of "zero lost": every accepted eventId must have a COMPLETED idempotency record.
 * Polls until all are complete or the timeout expires.
 */
export async function verifyCompleted(ctx: Ctx, ids: string[], timeoutMs: number): Promise<CompletionReport> {
  const table = ctx.out.TableName;
  const done = new Map<string, { outcome: string; createdAt?: string; completedAt?: string }>();
  const deadline = Date.now() + timeoutMs;
  while (true) {
    const todo = ids.filter((id) => !done.has(id));
    for (let i = 0; i < todo.length; i += 100) {
      let keys: Record<string, unknown>[] | undefined = todo.slice(i, i + 100).map((id) => ({ PK: `IDEMP#${id}`, SK: 'IDEMP' }));
      for (let attempt = 0; keys && keys.length && attempt < 8; attempt++) {
        if (attempt) await sleep(100 * 2 ** attempt);
        const res = await ctx.ddb.send(new BatchGetCommand({
          RequestItems: { [table]: { Keys: keys, ProjectionExpression: 'PK, #s, #r, createdAt, completedAt', ExpressionAttributeNames: { '#s': 'status', '#r': 'result' } } },
        }));
        for (const item of res.Responses?.[table] ?? []) {
          if (item.status !== 'COMPLETED') continue;
          let outcome = 'UNKNOWN';
          try { outcome = JSON.parse(item.result as string).outcome ?? 'UNKNOWN'; } catch { /* ignore */ }
          done.set((item.PK as string).slice('IDEMP#'.length), { outcome, createdAt: item.createdAt as string, completedAt: item.completedAt as string });
        }
        keys = res.UnprocessedKeys?.[table]?.Keys as Record<string, unknown>[] | undefined;
      }
    }
    if (done.size >= ids.length || Date.now() > deadline) break;
    log(`  completed ${done.size}/${ids.length}; checking again in 5 s`);
    await sleep(5000);
  }
  const outcomes: Record<string, number> = {};
  let first: string | undefined;
  let last: string | undefined;
  for (const v of done.values()) {
    outcomes[v.outcome] = (outcomes[v.outcome] ?? 0) + 1;
    if (v.createdAt && (!first || v.createdAt < first)) first = v.createdAt;
    if (v.completedAt && (!last || v.completedAt > last)) last = v.completedAt;
  }
  return { expected: ids.length, completed: done.size, outcomes, notCompletedIds: ids.filter((id) => !done.has(id)), firstStartedAt: first, lastCompletedAt: last };
}

/* ------------------------------------------------------------------ outbox */

export async function purgeQueue(ctx: Ctx, queueUrl: string): Promise<void> {
  try {
    await ctx.sqs.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
    log('  purged outbox (takes up to 60 s to finish)');
    await sleep(5000);
  } catch (err) {
    log(`  outbox purge skipped: ${(err as Error).name}`);
  }
}

/** Reads (and deletes) notifications from the outbox whose eventId starts with `prefix`. */
export async function drainOutbox(ctx: Ctx, prefix: string, expected: number, timeoutMs: number): Promise<Record<string, unknown>[]> {
  const found: Record<string, unknown>[] = [];
  const deadline = Date.now() + timeoutMs;
  let emptyPolls = 0;
  while (Date.now() < deadline && found.length < expected && emptyPolls < 6) {
    const res = await ctx.sqs.send(new ReceiveMessageCommand({ QueueUrl: ctx.out.OutboxQueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 2, VisibilityTimeout: 30 }));
    const msgs = res.Messages ?? [];
    emptyPolls = msgs.length === 0 ? emptyPolls + 1 : 0;
    const toDelete: { Id: string; ReceiptHandle: string }[] = [];
    for (const m of msgs) {
      try {
        const body = JSON.parse(m.Body ?? '{}');
        if (typeof body.eventId === 'string' && body.eventId.startsWith(prefix)) found.push(body);
      } catch { /* not ours */ }
      toDelete.push({ Id: m.MessageId!, ReceiptHandle: m.ReceiptHandle! }); // outbox is test-only: clear everything we read
    }
    if (toDelete.length) await ctx.sqs.send(new DeleteMessageBatchCommand({ QueueUrl: ctx.out.OutboxQueueUrl, Entries: toDelete }));
  }
  return found;
}

/* ------------------------------------------------------------------ Lambda + Logs Insights */

export async function functionInfo(ctx: Ctx, functionName: string) {
  const c = await ctx.lambda.send(new GetFunctionConfigurationCommand({ FunctionName: functionName }));
  return {
    functionName,
    logGroup: c.LoggingConfig?.LogGroup ?? `/aws/lambda/${functionName}`,
    memoryMb: c.MemorySize ?? 0,
    arch: c.Architectures?.[0] ?? 'x86_64',
    codeSizeBytes: c.CodeSize ?? 0,
    profile: c.Environment?.Variables?.COLD_START_PROFILE,
    env: c.Environment?.Variables ?? {},
  };
}

/** Runs a CloudWatch Logs Insights query and returns rows as objects. */
export async function insights(ctx: Ctx, logGroup: string, startMs: number, endMs: number, query: string): Promise<Record<string, string>[]> {
  const { queryId } = await ctx.logs.send(new StartQueryCommand({
    logGroupName: logGroup, startTime: Math.floor(startMs / 1000), endTime: Math.ceil(endMs / 1000), queryString: query,
  }));
  for (let i = 0; i < 60; i++) {
    await sleep(2000);
    const res = await ctx.logs.send(new GetQueryResultsCommand({ queryId }));
    if (res.status === 'Complete') return (res.results ?? []).map((row) => Object.fromEntries(row.map((f) => [f.field!, f.value!])));
    if (res.status === 'Failed' || res.status === 'Cancelled' || res.status === 'Timeout') throw new Error(`Logs Insights query ${res.status}`);
  }
  throw new Error('Logs Insights query did not finish');
}

export const PROCESSOR_REPORT_QUERY = [
  'filter @type = "REPORT"',
  '| stats count() as invocations, sum(@billedDuration) as billedMs, avg(@duration) as avgMs, pct(@duration, 50) as p50Ms, pct(@duration, 95) as p95Ms, count(@initDuration) as coldStarts',
].join('\n');

/** Seeds one test-channel subscriber per flight via the API (idempotent). */
export async function seedSubscriptions(apiUrl: string, flights: { flightNumber: string; flightDate: string }[], userPrefix: string): Promise<number> {
  let ok = 0;
  for (let i = 0; i < flights.length; i += 10) {
    const chunk = flights.slice(i, i + 10);
    const results = await Promise.all(chunk.map(async (f, j) => {
      const userId = `${userPrefix}-${i + j}`;
      for (let attempt = 0; attempt < 5; attempt++) {
        const res = await fetch(`${apiUrl}/subscriptions`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ userId, ...f, channel: 'test', contact: `${userId}@example.com` }),
        }).catch(() => undefined);
        if (res?.status === 201) return true;
        await sleep(300 * 2 ** attempt);
      }
      return false;
    }));
    ok += results.filter(Boolean).length;
  }
  return ok;
}
