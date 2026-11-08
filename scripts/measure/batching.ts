/**
 * Batching comparison: the same workload against the processor deployed with batch size 1 vs 10.
 * Measures processor invocations and billed GB-seconds from the Lambda REPORT lines (Logs Insights).
 *   $env:FDN_PROCESSOR_BATCH_SIZE="1";  npm run deploy; npm run measure:batching
 *   Remove-Item Env:FDN_PROCESSOR_BATCH_SIZE; npm run deploy; npm run measure:batching
 * Usage: npm run measure:batching [-- --events 1000 --rps 10]
 */
import { context, functionInfo, insights, log, newRunId, numArg, PROCESSOR_REPORT_QUERY, QueueMonitor, runFlightDate, sendLoad, sleep, verifyCompleted, writeResult } from './lib';
import { experimentConfig } from './config';
import { lambdaCost, round } from './stats';

async function stableInsights(ctx: ReturnType<typeof context>, logGroup: string, startMs: number) {
  let prev: Record<string, string> | undefined;
  for (let i = 0; i < 6; i++) {
    const [row] = await insights(ctx, logGroup, startMs, Date.now(), PROCESSOR_REPORT_QUERY);
    if (prev && row && prev.invocations === row.invocations) return row;
    prev = row;
    log(`  Logs Insights sees ${row?.invocations ?? 0} invocations so far; re-checking in 20 s`);
    await sleep(20_000);
  }
  return prev ?? {};
}

async function main() {
  const ctx = context();
  const cfg = experimentConfig(ctx);
  const batchSize = cfg.processorBatchSize ?? 10;
  const label = `batch${batchSize}`;
  if (cfg.billingMode !== 'PAY_PER_REQUEST' && !process.argv.includes('--force')) {
    throw new Error('Batching test needs measurement mode (on-demand DynamoDB). Run: $env:FDN_MEASURE="true"; npm run deploy   (or pass --force)');
  }
  const events = numArg('events', 1000);
  const rps = numArg('rps', 10);
  const runId = newRunId(`bt${batchSize}`);
  const flightDate = runFlightDate();
  log(`batching run ${runId}: processor batch size ${batchSize}; ${events} events (10/request) at ${rps} req/s`);

  const monitor = new QueueMonitor(ctx, ctx.out.QueueUrl);
  monitor.start();
  const load = await sendLoad({
    apiUrl: ctx.out.ApiUrl, count: events, eventsPerRequest: 10, requestsPerSecond: rps,
    makeEvent: (i) => ({ eventId: `${runId}-${i}`, type: 'GATE_CHANGE', version: 1, flightNumber: `BT${(i % 100) + 1}`, flightDate, occurredAt: new Date(Date.now() + i).toISOString(), newGate: `G${i % 50}` }),
  });
  monitor.markLoadDone();
  log('waiting for the queue to drain...');
  await monitor.waitDrained(30 * 60_000);
  const completion = await verifyCompleted(ctx, load.acceptedIds, 10 * 60_000);

  log('waiting 30 s for processor logs, then querying Logs Insights...');
  await sleep(30_000);
  const proc = await functionInfo(ctx, ctx.out.ProcessorFunctionName);
  const stats = await stableInsights(ctx, proc.logGroup, load.startedAt - 5000);
  const invocations = Number(stats.invocations ?? 0);
  const billedMs = Number(stats.billedMs ?? 0);
  const cost = lambdaCost(invocations, billedMs, proc.memoryMb, proc.arch);
  const per10k = completion.completed ? 10_000 / completion.completed : 0;

  const result = {
    kind: 'batching', label, runId, measuredAt: new Date().toISOString(), config: cfg, batchSize,
    window: { startMs: load.startedAt, endMs: monitor.drainedAt ?? Date.now() },
    batchingWindowSeconds: batchSize > 1 ? 5 : 0,
    events, accepted: load.acceptedIds.length, completed: completion.completed, lost: completion.expected - completion.completed,
    outcomes: completion.outcomes,
    processor: { memoryMb: proc.memoryMb, arch: proc.arch },
    invocations, eventsPerInvocation: round(completion.completed / Math.max(1, invocations), 2),
    billedMs, avgDurationMs: round(Number(stats.avgMs)), p95DurationMs: round(Number(stats.p95Ms)), coldStarts: Number(stats.coldStarts ?? 0),
    timeToDrainSeconds: monitor.drainedAt ? round((monitor.drainedAt - load.startedAt) / 1000) : null,
    per10kEvents: {
      invocations: Math.round(invocations * per10k),
      gbSeconds: round(cost.gbSeconds * per10k, 3),
      lambdaCostUsd: round(cost.total * per10k, 5),
    },
  };
  writeResult(`batching-${label}`, result);
  log('tip: run "npm run measure:recount" a few minutes later to replace Logs Insights counts with exact CloudWatch metrics');
  log(`${label}: ${invocations} invocations for ${completion.completed} events (${result.eventsPerInvocation}/invocation); per 10k events: ${result.per10kEvents.invocations} invocations, $${result.per10kEvents.lambdaCostUsd}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
