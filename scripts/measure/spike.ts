/**
 * Spike test: a burst of events through the API (default 10,000 events = 1,000 requests of 10 at 25 req/s).
 * Proves "zero lost": every accepted eventId must end up COMPLETED in DynamoDB, and nothing new in the DLQ.
 * Requires measurement mode (on-demand DynamoDB):  $env:FDN_MEASURE="true"; npm run deploy
 * Usage: npm run measure:spike [-- --events 10000 --rps 25]
 */
import { GetAccountSettingsCommand } from '@aws-sdk/client-lambda';
import { context, functionInfo, insights, log, newRunId, numArg, PROCESSOR_REPORT_QUERY, purgeQueue, QueueMonitor, queueDepth, runFlightDate, seedSubscriptions, sendLoad, sleep, verifyCompleted, writeResult } from './lib';
import { experimentConfig } from './config';
import { round } from './stats';

async function main() {
  const ctx = context();
  const cfg = experimentConfig(ctx);
  if (cfg.billingMode !== 'PAY_PER_REQUEST' && !process.argv.includes('--force')) {
    throw new Error('Spike test needs measurement mode (on-demand DynamoDB). Run: $env:FDN_MEASURE="true"; npm run deploy   (or pass --force)');
  }
  const events = numArg('events', 10_000);
  const rps = numArg('rps', 25);
  const flights = numArg('flights', 100);
  const runId = newRunId('spike');
  const flightDate = runFlightDate();
  const flight = (i: number) => ({ flightNumber: `SP${(i % flights) + 1}`, flightDate });

  const account = await ctx.lambda.send(new GetAccountSettingsCommand({}));
  const accountConcurrency = account.AccountLimit?.ConcurrentExecutions;
  log(`spike run ${runId}: ${events} events, ${Math.ceil(events / 10)} requests at ${rps} req/s; account Lambda concurrency limit ${accountConcurrency}`);

  const dlqBefore = await queueDepth(ctx, ctx.out.DlqUrl);
  log(`seeding ${flights} subscriptions (1 per flight)...`);
  const seeded = await seedSubscriptions(ctx.out.ApiUrl, Array.from({ length: flights }, (_, i) => flight(i)), runId);

  const monitor = new QueueMonitor(ctx, ctx.out.QueueUrl, 2000);
  monitor.start();
  log('sending spike...');
  const t0 = Date.now();
  const load = await sendLoad({
    apiUrl: ctx.out.ApiUrl, count: events, eventsPerRequest: 10, requestsPerSecond: rps, maxInFlight: 100,
    makeEvent: (i) => ({ eventId: `${runId}-${i}`, type: 'DELAY', version: 1, ...flight(i), occurredAt: new Date(t0 + i).toISOString(), delayMinutes: 5 + (i % 180) }),
  });
  monitor.markLoadDone();
  log(`spike sent in ${((load.finishedAt - load.startedAt) / 1000).toFixed(1)} s; waiting for the queue to drain...`);
  await monitor.waitDrained(45 * 60_000);
  const completion = await verifyCompleted(ctx, load.acceptedIds, 10 * 60_000);
  const dlqAfter = await queueDepth(ctx, ctx.out.DlqUrl);

  log('waiting 45 s for processor logs, then querying Logs Insights...');
  await sleep(45_000);
  const proc = await functionInfo(ctx, ctx.out.ProcessorFunctionName);
  const [stats] = await insights(ctx, proc.logGroup, load.startedAt - 5000, Date.now(), PROCESSOR_REPORT_QUERY);
  await purgeQueue(ctx, ctx.out.OutboxQueueUrl);

  const drainS = monitor.drainedAt ? (monitor.drainedAt - load.startedAt) / 1000 : null;
  const result = {
    kind: 'spike', runId, measuredAt: new Date().toISOString(), config: cfg, accountConcurrency,
    processorMaxConcurrency: 2,
    eventsSent: events, requests: load.requests, requestsPerSecond: rps,
    accepted: load.acceptedIds.length, notAccepted: load.notAcceptedIds.length,
    statusCounts: load.statusCounts, retries: load.retries, apiResponseMs: load.requestLatencyMs,
    ingestSeconds: round((load.finishedAt - load.startedAt) / 1000),
    ingestRateEventsPerSecond: round(load.acceptedIds.length / ((load.finishedAt - load.startedAt) / 1000)),
    peakQueueDepth: monitor.peakTotal, peakQueueVisible: monitor.peakVisible,
    timeToDrainSeconds: round(drainS),
    backlogClearedAfterLoadSeconds: monitor.drainedAt ? round((monitor.drainedAt - load.finishedAt) / 1000) : null,
    completed: completion.completed, outcomes: completion.outcomes,
    lost: completion.expected - completion.completed,
    lostIdsSample: completion.notCompletedIds.slice(0, 20),
    dlqNewMessages: dlqAfter - dlqBefore,
    processingSpan: { firstStartedAt: completion.firstStartedAt, lastCompletedAt: completion.lastCompletedAt },
    processor: { ...stats, memoryMb: proc.memoryMb, arch: proc.arch },
    queueSamples: monitor.samples.map((s) => ({ s: round((s.t - load.startedAt) / 1000), visible: s.visible, inFlight: s.inFlight })),
    seededSubscriptions: seeded,
  };
  writeResult('spike', result);
  log(`accepted ${result.accepted}/${events}, completed ${result.completed}, LOST ${result.lost}, new DLQ ${result.dlqNewMessages}`);
  log(`peak queue depth ${result.peakQueueDepth}, drained ${result.timeToDrainSeconds} s after the first request`);
}

main().catch((e) => { console.error(e); process.exit(1); });
