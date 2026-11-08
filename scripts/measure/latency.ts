/**
 * Steady-load latency: N single-event requests at a gentle rate to N distinct, subscribed flights.
 * Pipeline latency = SQS SentTimestamp -> SNS publish (both AWS clocks), read from each notification.
 * Usage: npm run measure:latency [-- --events 100 --rate 2]
 */
import { context, drainOutbox, log, newRunId, numArg, purgeQueue, QueueMonitor, runFlightDate, seedSubscriptions, sendLoad, verifyCompleted, writeResult } from './lib';
import { summarize } from './stats';
import { experimentConfig } from './config';

async function main() {
  const ctx = context();
  const events = numArg('events', 100);
  const rate = numArg('rate', 2);
  const runId = newRunId('lat');
  const flightDate = runFlightDate();
  const flight = (i: number) => ({ flightNumber: `LT${i + 1}`, flightDate });
  log(`latency run ${runId}: ${events} events at ${rate}/s (config ${JSON.stringify(experimentConfig(ctx))})`);

  await purgeQueue(ctx, ctx.out.OutboxQueueUrl);
  log(`seeding ${events} subscriptions...`);
  const seeded = await seedSubscriptions(ctx.out.ApiUrl, Array.from({ length: events }, (_, i) => flight(i)), runId);
  log(`  seeded ${seeded}/${events}`);

  const monitor = new QueueMonitor(ctx, ctx.out.QueueUrl);
  monitor.start();
  log('sending events...');
  const load = await sendLoad({
    apiUrl: ctx.out.ApiUrl, count: events, eventsPerRequest: 1, requestsPerSecond: rate,
    makeEvent: (i) => ({ eventId: `${runId}-${i}`, type: 'DELAY', version: 1, ...flight(i), occurredAt: new Date().toISOString(), delayMinutes: 30 + (i % 60) }),
  });
  monitor.markLoadDone();
  log('waiting for the queue to drain...');
  await monitor.waitDrained(10 * 60_000);
  const completion = await verifyCompleted(ctx, load.acceptedIds, 5 * 60_000);
  log('reading notifications from the outbox...');
  const notes = await drainOutbox(ctx, runId, load.acceptedIds.length, 3 * 60_000);
  const latencies = notes
    .map((n) => Date.parse(n.sentAt as string) - Date.parse(n.enqueuedAt as string))
    .filter((v) => Number.isFinite(v) && v >= 0);

  const result = {
    kind: 'latency', runId, measuredAt: new Date().toISOString(), config: experimentConfig(ctx),
    events, ratePerSecond: rate, seededSubscriptions: seeded,
    accepted: load.acceptedIds.length, notAccepted: load.notAcceptedIds.length, statusCounts: load.statusCounts,
    apiResponseMs: load.requestLatencyMs,
    pipelineLatencyMs: summarize(latencies),
    notificationsReceived: notes.length,
    completed: completion.completed, outcomes: completion.outcomes, lost: completion.expected - completion.completed,
  };
  writeResult('latency', result);
  log(`pipeline latency (queue -> notification): p50 ${result.pipelineLatencyMs.p50} ms, p95 ${result.pipelineLatencyMs.p95} ms over ${latencies.length} notifications`);
  log(`API response: p50 ${result.apiResponseMs.p50} ms, p95 ${result.apiResponseMs.p95} ms; lost ${result.lost}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
