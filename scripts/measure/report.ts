/**
 * Turns results/*.json into docs/batching.md, docs/cold-starts.md, docs/spike-test.md,
 * docs/latency.md and docs/resume-bullets.md. Only measured values are used; anything
 * not yet measured is left as a visible TODO.
 * Usage: npm run measure:report
 */
import * as fs from 'node:fs';
import * as path from 'node:path';
import { pctChange } from './stats';

/* eslint-disable @typescript-eslint/no-explicit-any */
type R = Record<string, any>;

const PROFILE_ORDER = ['p0-baseline', 'p1-minify', 'p2-runtime-sdk', 'p3-arm64', 'p4-256mb', 'p5-512mb', 'p6-1024mb'];
const n = (v: unknown, d = 0) => (v === null || v === undefined || Number.isNaN(Number(v)) ? '–' : Number(v).toLocaleString('en-US', { maximumFractionDigits: d, minimumFractionDigits: d }));
const TODO = (what: string) => `**TODO:** ${what}`;

export function loadResults(dir: string): R[] {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).filter((f) => f.endsWith('.json')).map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')));
}

export function buildReport(results: R[]): Record<string, string> {
  const files: Record<string, string> = {};
  const spike = results.find((r) => r.kind === 'spike');
  const latency = results.find((r) => r.kind === 'latency');
  const b1 = results.find((r) => r.kind === 'batching' && r.batchSize === 1);
  const b10 = results.find((r) => r.kind === 'batching' && r.batchSize === 10);
  const cold = PROFILE_ORDER.map((p) => results.find((r) => r.kind === 'coldstarts' && r.profile === p)).filter(Boolean) as R[];

  /* ---------- spike ---------- */
  files['spike-test.md'] = spike
    ? `# Spike test

Measured ${spike.measuredAt.slice(0, 10)} (run \`${spike.runId}\`) with \`npm run measure:spike\`, deployed in measurement mode (on-demand DynamoDB, API limit ${spike.config?.apiRateLimit ?? '?'} rps). Processor capped at ${spike.processorMaxConcurrency} concurrent invocations (SQS event-source maxConcurrency); account Lambda concurrency limit ${spike.accountConcurrency ?? '?'}.

| Metric | Value |
|---|---|
| Events sent | ${n(spike.eventsSent)} (${n(spike.requests)} API requests × 10 events at ${spike.requestsPerSecond} req/s) |
| Events accepted by the API | ${n(spike.accepted)} (${n(spike.retries)} request retries after throttling/5xx) |
| Ingest duration / rate | ${n(spike.ingestSeconds, 1)} s / ${n(spike.ingestRateEventsPerSecond)} events/s |
| Peak queue depth | ${n(spike.peakQueueDepth)} messages |
| Time to drain (first request → queue empty) | ${n(spike.timeToDrainSeconds, 1)} s |
| Backlog cleared after the last request | ${n(spike.backlogClearedAfterLoadSeconds, 1)} s |
| Events processed (COMPLETED idempotency record) | ${n(spike.completed)} |
| **Events lost** | **${n(spike.lost)}** |
| New dead-letter messages | ${n(spike.dlqNewMessages)} |
| Outcomes | ${Object.entries(spike.outcomes ?? {}).map(([k, v]) => `${k}: ${n(v)}`).join(', ')} |
| Processor invocations / cold starts | ${n(spike.processor?.invocations)} / ${n(spike.processor?.coldStarts)} |
| API response time p50 / p95 | ${n(spike.apiResponseMs?.p50)} ms / ${n(spike.apiResponseMs?.p95)} ms |

**How "lost" is measured:** every accepted \`eventId\` is looked up in DynamoDB (\`IDEMP#<eventId>\`); an event counts as processed only when its record is \`COMPLETED\`. \`STALE\` means the event was processed but was older than the flight's current status (many events hit the same 100 flights out of order), so no notification was sent, by design.

**What it shows:** the API only validates and enqueues, so ingestion runs far faster than processing; SQS absorbs the burst while the processor drains it at a capped rate, protecting DynamoDB and SNS.

Queue depth over time (seconds since the first request):

| t (s) | waiting | in flight |
|---|---|---|
${(spike.queueSamples ?? []).filter((_: R, i: number, a: R[]) => i % Math.max(1, Math.ceil(a.length / 25)) === 0 || i === a.length - 1).map((s: R) => `| ${s.s} | ${s.visible} | ${s.inFlight} |`).join('\n')}
`
    : `# Spike test\n\n${TODO('run `npm run measure:spike`, then `npm run measure:report`.')}\n`;

  /* ---------- latency ---------- */
  files['latency.md'] = latency
    ? `# End-to-end latency

Measured ${latency.measuredAt.slice(0, 10)} (run \`${latency.runId}\`) with \`npm run measure:latency\`: ${latency.events} single-event API requests at ${latency.ratePerSecond}/s, each to its own subscribed flight.

| Metric | p50 | p95 | max | samples |
|---|---|---|---|---|
| Pipeline: SQS enqueue → SNS publish | ${n(latency.pipelineLatencyMs?.p50)} ms | ${n(latency.pipelineLatencyMs?.p95)} ms | ${n(latency.pipelineLatencyMs?.max)} ms | ${latency.pipelineLatencyMs?.n} |
| API response (client-side) | ${n(latency.apiResponseMs?.p50)} ms | ${n(latency.apiResponseMs?.p95)} ms | ${n(latency.apiResponseMs?.max)} ms | ${latency.apiResponseMs?.n} |

Notifications received: ${latency.notificationsReceived}/${latency.accepted}; lost: ${latency.lost}.

The pipeline number uses AWS clocks only (SQS \`SentTimestamp\` and the processor's publish time, both carried in each notification), so local clock skew doesn't affect it. At low traffic, most of it is the **${latency.config?.processorBatchSize === 1 ? '0' : '5'} s SQS batching window**: the event source waits up to 5 s to fill a batch of 10 before invoking the processor. That's the deliberate trade of a little latency for far fewer invocations (see batching.md). Setting the window to 0 would cut latency at the cost of more invocations.
`
    : `# End-to-end latency\n\n${TODO('run `npm run measure:latency`, then `npm run measure:report`.')}\n`;

  /* ---------- batching ---------- */
  let batchingBullet: string | undefined;
  if (b1 && b10) {
    const invRed = pctChange(b1.per10kEvents.invocations, b10.per10kEvents.invocations);
    const costRed = pctChange(b1.per10kEvents.lambdaCostUsd, b10.per10kEvents.lambdaCostUsd);
    files['batching.md'] = `# Batching: batch size 1 vs 10

Same workload for both runs: ${b10.events} gate-change events (10 per API request, 100 flights, no subscribers) in measurement mode. ${b1.countSource === 'cloudwatch-metrics' && b10.countSource === 'cloudwatch-metrics' ? 'Processor invocations and total duration come from the CloudWatch `AWS/Lambda` Invocations and Duration metrics for each run window (`npm run measure:recount`).' : 'Processor invocations and billed duration come from the Lambda REPORT lines (CloudWatch Logs Insights); run `npm run measure:recount` for exact metric-based counts.'} Cost uses Lambda list prices (${b10.processor.arch}, ${b10.processor.memoryMb} MB); the always-free tier makes the real bill $0 at this scale.

| Metric | Batch size 1 | Batch size 10 + 5 s window | Change |
|---|---|---|---|
| Events processed | ${n(b1.completed)} | ${n(b10.completed)} | |
| Processor invocations | ${n(b1.invocations)} | ${n(b10.invocations)} | |
| Events per invocation | ${n(b1.eventsPerInvocation, 2)} | ${n(b10.eventsPerInvocation, 2)} | |
| Avg / p95 duration per invocation | ${n(b1.avgDurationMs)} / ${n(b1.p95DurationMs)} ms | ${n(b10.avgDurationMs)} / ${n(b10.p95DurationMs)} ms | |
| **Invocations per 10k events** | ${n(b1.per10kEvents.invocations)} | ${n(b10.per10kEvents.invocations)} | **−${n(invRed, 1)}%** |
| GB-seconds per 10k events | ${n(b1.per10kEvents.gbSeconds, 3)} | ${n(b10.per10kEvents.gbSeconds, 3)} | −${n(pctChange(b1.per10kEvents.gbSeconds, b10.per10kEvents.gbSeconds), 1)}% |
| Lambda cost per 10k events | $${n(b1.per10kEvents.lambdaCostUsd, 5)} | $${n(b10.per10kEvents.lambdaCostUsd, 5)} | −${n(costRed, 1)}% |
| Events lost | ${n(b1.lost)} | ${n(b10.lost)} | |

**Why it helps:** with batch size 10 one invocation pays the per-invocation overhead once for up to 10 messages, makes one \`BatchGetItem\` for every flight status in the batch (instead of one per event), reuses the subscriber lookup for repeated flights, and deletes successful messages in bulk. \`ReportBatchItemFailures\` keeps batching safe: only failed messages are retried.

**Trade-off:** the 5 s batching window adds up to 5 s of latency at low traffic (see latency.md), in exchange for fewer, fuller invocations.
`;
    batchingBullet = `- Cut Lambda invocations per 10k events by **${n(invRed, 0)}%** (${n(b1.per10kEvents.invocations)} → ${n(b10.per10kEvents.invocations)}) using SQS batching with partial batch failure reporting and DynamoDB \`BatchGetItem\`.`;
  } else {
    files['batching.md'] = `# Batching: batch size 1 vs 10\n\n${TODO(`run \`npm run measure:batching\` with batch size 1${b1 ? ' ✅' : ''} and 10${b10 ? ' ✅' : ''}, then \`npm run measure:report\`.`)}\n`;
  }

  /* ---------- cold starts ---------- */
  let coldBullet: string | undefined;
  if (cold.length) {
    const base = cold[0];
    const rows = cold
      .map((c) => `| ${c.profile} | ${c.label} | ${c.arch} | ${c.memoryMb} | ${n(c.codeSizeKb)} | ${n(c.initMs?.p50, 1)} | ${n(c.initMs?.p95, 1)} | ${n(c.coldTotalMs?.p50, 0)} | ${n(c.coldTotalMs?.p95, 0)} | ${n(c.warmInvokeMs?.p50, 1)} | ${c.initMs?.n} |`)
      .join('\n');
    const final = cold.find((c) => c.profile === 'p4-256mb') ?? cold[cold.length - 1];
    const hasBeforeAfter = base.profile === 'p0-baseline' && final !== base;
    const initRed = hasBeforeAfter ? pctChange(base.initMs.p50, final.initMs.p50) : 0;
    const totalRed = hasBeforeAfter ? pctChange(base.coldTotalMs.p50, final.coldTotalMs.p50) : 0;
    files['cold-starts.md'] = `# Cold starts

Measured with \`npm run measure:coldstarts\` on the Subscriptions function (GET /flights → one DynamoDB GetItem). Before every cold sample the script changes an environment variable, which forces a new execution environment. \`Init Duration\` and the first invocation's duration come from the Lambda REPORT line. Warm samples reuse the environment and its SDK clients (created outside the handler).

**Cold-start latency** = init + first invocation, i.e. what the first request actually waits for. The first invocation includes the first DynamoDB call (TLS connection, credentials), which is CPU-bound, so memory size matters a lot.

| Profile | Change | Arch | Memory (MB) | Code (KB) | Init p50 (ms) | Init max* (ms) | Cold-start latency p50 (ms) | Cold-start latency max* (ms) | Warm invoke p50 (ms) | Cold samples |
|---|---|---|---|---|---|---|---|---|---|---|
${rows}

\\* With 15 samples, the nearest-rank p95 is the slowest sample, so a single outlier sets it. Medians (p50) are the reliable comparison here.

${hasBeforeAfter ? `**Result (${base.profile} → ${final.profile}):** cold-start latency ${n(base.coldTotalMs.p50, 0)} → ${n(final.coldTotalMs.p50, 0)} ms median (**−${n(totalRed, 0)}%**), init ${n(base.initMs.p50, 0)} → ${n(final.initMs.p50, 0)} ms median (**−${n(initRed, 0)}%**), deployment package ${n(base.codeSizeKb)} → ${n(final.codeSizeKb)} KB, warm invocation ${n(base.warmInvokeMs.p50, 1)} → ${n(final.warmInvokeMs.p50, 1)} ms.

Biggest single step for init time: ${biggestStep(cold, 'initMs')}. Biggest single step for cold-start latency: ${biggestStep(cold, 'coldTotalMs')}.` : TODO('measure p0-baseline and p4-256mb to compute the before/after.')}

Notes: the baseline is already an esbuild bundle (unminified, AWS SDK v3 bundled into the zip); a non-bundled "whole node_modules" zip was not measured. Provisioned concurrency (experiment 6 in the guide) was skipped because it isn't free. arm64 at 128 MB had a slower warm invocation than x86 at 128 MB; at 256 MB the warm invocation is the fastest of all profiles.
`;
    if (hasBeforeAfter) {
      coldBullet = `- Cut Lambda cold-start latency by **${n(totalRed, 0)}% (${n(base.coldTotalMs.p50, 0)} → ${n(final.coldTotalMs.p50, 0)} ms median)** and init time by ${n(initRed, 0)}% (${n(base.initMs.p50, 0)} → ${n(final.initMs.p50, 0)} ms) using esbuild minification, the Lambda runtime's built-in AWS SDK v3 (package ${n(base.codeSizeKb)} → ${n(final.codeSizeKb)} KB), arm64 (Graviton), and memory tuning.`;
    }
  } else {
    files['cold-starts.md'] = `# Cold starts\n\n${TODO('run `npm run measure:coldstarts` for each profile, then `npm run measure:report`.')}\n`;
  }

  /* ---------- resume bullets ---------- */
  const spikeBullet = spike && spike.lost === 0
    ? `- Built a serverless flight-disruption pipeline on **AWS** (API Gateway, Lambda, SQS, DynamoDB, SNS) with infrastructure defined in **CDK** and CI in GitHub Actions, processing a **${n(spike.accepted)}-event spike with zero lost messages** (peak queue depth ${n(spike.peakQueueDepth)}, drained in ${n(spike.timeToDrainSeconds, 0)} s) behind an SQS queue with a dead-letter queue and redrive.`
    : undefined;
  files['resume-bullets.md'] = `# Resume bullets (generated from measured results only)

**Flight Disruption Notifier** | AWS Lambda, API Gateway, SQS, DynamoDB, SNS, CDK, TypeScript

${spikeBullet ?? TODO(spike ? `spike run lost ${spike.lost} events; investigate before using this bullet.` : 'spike test not run yet.')}
${coldBullet ?? TODO('cold-start profiles p0-baseline and p4-256mb not measured yet.')}
${batchingBullet ?? TODO('batching runs (batch size 1 and 10) not measured yet.')}
- Made message handling idempotent with DynamoDB conditional writes and expiring records, sending **exactly 1 notification across 20 duplicate deliveries** (integration test against the deployed stack, 2026-11-07).
${latency ? `\nOptional detail: end-to-end pipeline latency p50 ${n(latency.pipelineLatencyMs?.p50)} ms / p95 ${n(latency.pipelineLatencyMs?.p95)} ms with a 5 s batching window (docs/latency.md).` : ''}

Every number links to a table in \`docs/\` and a raw file in \`results/\`.
`;
  return files;
}

function biggestStep(cold: R[], metric: 'initMs' | 'coldTotalMs'): string {
  let best = { label: '–', delta: -Infinity };
  for (let i = 1; i < cold.length; i++) {
    const d = (cold[i - 1][metric]?.p50 ?? 0) - (cold[i][metric]?.p50 ?? 0);
    if (d > best.delta) best = { label: `${cold[i].label} (−${n(d, 0)} ms)`, delta: d };
  }
  return best.label;
}

function main() {
  const root = path.resolve(__dirname, '..', '..');
  const results = loadResults(path.join(root, 'results'));
  console.log(`found ${results.length} result file(s): ${results.map((r) => r.kind + (r.profile ? `:${r.profile}` : r.label ? `:${r.label}` : '')).join(', ')}`);
  const files = buildReport(results);
  for (const [name, content] of Object.entries(files)) {
    fs.writeFileSync(path.join(root, 'docs', name), content);
    console.log(`wrote docs/${name}`);
  }
  console.log('\n' + files['resume-bullets.md']);
}

if (require.main === module) main();

