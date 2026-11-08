/**
 * End-to-end tests against the DEPLOYED stack (reads cdk-outputs.json).
 * Run with: npm run test:integration   (needs AWS credentials for the same account/region)
 *
 * Uses the free SQS "outbox" subscribed to the notifications topic, never email.
 */
import { randomUUID } from 'node:crypto';
import { DeleteMessageCommand, ReceiveMessageCommand, SendMessageBatchCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { loadOutputs, type StackOutputs } from '../../scripts/lib/outputs';

let out: StackOutputs;
let sqs: SQSClient;
const runId = randomUUID().slice(0, 8);
const today = new Date().toISOString().slice(0, 10);
const flightNumber = `ZZ${Math.floor(1000 + Math.random() * 8999)}`; // unique per run

beforeAll(() => {
  out = loadOutputs();
  sqs = new SQSClient({ region: out.region });
});

async function api(method: string, path: string, body?: unknown, headers: Record<string, string> = {}) {
  const res = await fetch(`${out.ApiUrl}${path}`, { method, headers: { 'content-type': 'application/json', ...headers }, body: body ? JSON.stringify(body) : undefined });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined, headers: res.headers };
}

/** Drains the queue looking for messages matching `match`, deleting matches, until timeout. */
async function collect(queueUrl: string, match: (body: string, attrs: Record<string, string | undefined>) => boolean, opts: { timeoutMs: number; stopAfter?: number }) {
  const found: string[] = [];
  const deadline = Date.now() + opts.timeoutMs;
  while (Date.now() < deadline) {
    const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 5, VisibilityTimeout: 2, MessageAttributeNames: ['All'] }));
    for (const m of res.Messages ?? []) {
      const attrs = Object.fromEntries(Object.entries(m.MessageAttributes ?? {}).map(([k, v]) => [k, v.StringValue]));
      if (match(m.Body ?? '', attrs)) {
        found.push(m.Body!);
        await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle! }));
      }
    }
    if (opts.stopAfter !== undefined && found.length >= opts.stopAfter) break;
  }
  return found;
}

const forEvent = (eventId: string) => (body: string) => { try { return JSON.parse(body).eventId === eventId; } catch { return false; } };

describe('deployed pipeline', () => {
  it('API event -> SQS -> processor -> DynamoDB -> SNS -> subscriber notification', async () => {
    const sub = await api('POST', '/subscriptions', { userId: `it-${runId}`, flightNumber, flightDate: today, channel: 'test', contact: `it-${runId}@example.com` }, { 'Idempotency-Key': `it-sub-${runId}` });
    expect(sub.status).toBe(201);
    const replay = await api('POST', '/subscriptions', { userId: `it-${runId}`, flightNumber, flightDate: today, channel: 'test', contact: `it-${runId}@example.com` }, { 'Idempotency-Key': `it-sub-${runId}` });
    expect(replay.status).toBe(201);
    expect(replay.headers.get('idempotent-replayed')).toBe('true');

    const eventId = `it-delay-${runId}`;
    const t0 = Date.now();
    const ingest = await api('POST', '/flight-events', { eventId, type: 'DELAY', version: 1, flightNumber, flightDate: today, occurredAt: new Date().toISOString(), delayMinutes: 45, reason: 'Integration test' });
    expect(ingest.status).toBe(202);

    const notes = await collect(out.OutboxQueueUrl, forEvent(eventId), { timeoutMs: 60000, stopAfter: 1 });
    expect(notes).toHaveLength(1);
    const n = JSON.parse(notes[0]);
    expect(n).toMatchObject({ userId: `it-${runId}`, type: 'DELAY', flightNumber });
    expect(n.text).toContain('delayed by 45 minutes');
    console.log(`end-to-end latency (API -> outbox receive): ${Date.now() - t0} ms`);

    const status = await api('GET', `/flights/${flightNumber}/${today}`);
    expect(status.status).toBe(200);
    expect(status.body).toMatchObject({ status: 'DELAYED', delayMinutes: 45, lastEventId: eventId });

    const mine = await api('GET', `/users/it-${runId}/subscriptions`);
    expect(mine.body.subscriptions).toHaveLength(1);
  });

  it('20 duplicate deliveries of one eventId produce exactly 1 notification', async () => {
    const eventId = `it-dup-${runId}`;
    const body = JSON.stringify({ eventId, source: 'skybook', type: 'GATE_CHANGE', version: 1, flightNumber, flightDate: today, occurredAt: new Date(Date.now() + 1000).toISOString(), newGate: 'E12' });
    for (let b = 0; b < 2; b++) {
      await sqs.send(new SendMessageBatchCommand({ QueueUrl: out.QueueUrl, Entries: Array.from({ length: 10 }, (_, i) => ({ Id: `d${i}`, MessageBody: body })) }));
    }
    // Wait long enough for all 20 to be processed (in-progress duplicates retry after the visibility timeout,
    // but most collapse within a batch or hit the COMPLETED record).
    const notes = await collect(out.OutboxQueueUrl, forEvent(eventId), { timeoutMs: 45000 });
    expect(notes).toHaveLength(1);
  });

  it('a poison message is parked in the DLQ with its reason', async () => {
    const marker = `poison-${runId}`;
    await sqs.send(new SendMessageCommand({ QueueUrl: out.QueueUrl, MessageBody: JSON.stringify({ marker, eventId: marker, version: 99 }) }));
    const parked = await collect(out.DlqUrl, (b, attrs) => b.includes(marker) && (attrs.errorReason ?? '').includes('Schema validation failed'), { timeoutMs: 60000, stopAfter: 1 });
    expect(parked).toHaveLength(1);
  });

  it('rejects invalid events at the API with 400', async () => {
    const res = await api('POST', '/flight-events', { type: 'DELAY', version: 1, flightNumber: 'bad', flightDate: today, occurredAt: 'now' });
    expect(res.status).toBe(400);
    expect(res.body.issues.length).toBeGreaterThan(0);
  });
});
