import { lambdaCost, parseReportLine, pctChange, percentile, summarize } from '../../scripts/measure/stats';
import { buildReport } from '../../scripts/measure/report';

describe('measurement stats', () => {
  it('computes nearest-rank percentiles', () => {
    const v = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(percentile(v, 50)).toBe(50);
    expect(percentile(v, 95)).toBe(95);
    expect(percentile([7], 95)).toBe(7);
    expect(percentile([], 50)).toBeNull();
    expect(summarize([3, 1, 2])).toMatchObject({ n: 3, p50: 2, min: 1, max: 3, avg: 2 });
  });

  it('parses cold and warm REPORT lines', () => {
    const cold = 'START RequestId: abc\nEND RequestId: abc\nREPORT RequestId: abc\tDuration: 45.67 ms\tBilled Duration: 46 ms\tMemory Size: 256 MB\tMax Memory Used: 88 MB\tInit Duration: 312.45 ms\t\nXRAY TraceId: 1-x';
    expect(parseReportLine(cold)).toEqual({ durationMs: 45.67, billedMs: 46, memoryMb: 256, maxMemoryUsedMb: 88, initMs: 312.45 });
    const warm = 'REPORT RequestId: def\tDuration: 3.10 ms\tBilled Duration: 4 ms\tMemory Size: 128 MB\tMax Memory Used: 70 MB\t';
    expect(parseReportLine(warm)).toEqual({ durationMs: 3.1, billedMs: 4, memoryMb: 128, maxMemoryUsedMb: 70, initMs: undefined });
    expect(parseReportLine('nothing here')).toBeNull();
  });

  it('prices Lambda usage', () => {
    const c = lambdaCost(1000, 100_000, 256, 'arm64'); // 100 s at 0.25 GB = 25 GB-s
    expect(c.gbSeconds).toBeCloseTo(25);
    expect(c.requestCost).toBeCloseTo(0.0002);
    expect(c.computeCost).toBeCloseTo(25 * 0.0000133334);
    expect(pctChange(200, 50)).toBe(75);
  });
});

describe('report generator', () => {
  const base = { measuredAt: '2026-11-07T23:00:00Z', config: { apiRateLimit: 200, processorBatchSize: 10 } };
  const results = [
    { ...base, kind: 'spike', runId: 's', eventsSent: 10000, requests: 1000, requestsPerSecond: 25, accepted: 10000, retries: 0, ingestSeconds: 40, ingestRateEventsPerSecond: 250, peakQueueDepth: 6200, timeToDrainSeconds: 210, backlogClearedAfterLoadSeconds: 170, completed: 10000, lost: 0, dlqNewMessages: 0, outcomes: { NOTIFIED: 4000, STALE: 6000 }, processor: { invocations: 1100, coldStarts: 2 }, apiResponseMs: { p50: 80, p95: 150 }, processorMaxConcurrency: 2, accountConcurrency: 10, queueSamples: [{ s: 0, visible: 0, inFlight: 0 }] },
    { ...base, kind: 'batching', batchSize: 1, events: 1000, completed: 1000, lost: 0, invocations: 1000, eventsPerInvocation: 1, avgDurationMs: 60, p95DurationMs: 90, processor: { arch: 'arm64', memoryMb: 256 }, per10kEvents: { invocations: 10000, gbSeconds: 160, lambdaCostUsd: 0.0041 } },
    { ...base, kind: 'batching', batchSize: 10, events: 1000, completed: 1000, lost: 0, invocations: 110, eventsPerInvocation: 9.09, avgDurationMs: 300, p95DurationMs: 450, processor: { arch: 'arm64', memoryMb: 256 }, per10kEvents: { invocations: 1100, gbSeconds: 85, lambdaCostUsd: 0.0013 } },
    { ...base, kind: 'coldstarts', profile: 'p4-256mb', label: 'final', arch: 'arm64', memoryMb: 256, codeSizeKb: 94, initMs: { p50: 305, p95: 415, n: 15 }, coldTotalMs: { p50: 743, p95: 887 }, warmInvokeMs: { p50: 12.7 } },
    { ...base, kind: 'coldstarts', profile: 'p0-baseline', label: 'baseline', arch: 'x86_64', memoryMb: 128, codeSizeKb: 374, initMs: { p50: 392, p95: 433, n: 15 }, coldTotalMs: { p50: 1238, p95: 1301 }, warmInvokeMs: { p50: 33.8 } },
  ];

  it('fills resume bullets from measured values only', () => {
    const files = buildReport(results);
    const bullets = files['resume-bullets.md'];
    expect(bullets).toContain('10,000-event spike with zero lost messages');
    expect(bullets).toContain('cold-start latency by **40% (1,238 → 743 ms median)** and init time by 22% (392 → 305 ms)');
    expect(bullets).toContain('374 → 94 KB');
    expect(bullets).toContain('by **89%** (10,000 → 1,100)');
    expect(bullets).not.toContain('TODO');
    expect(files['cold-starts.md'].indexOf('p0-baseline')).toBeLessThan(files['cold-starts.md'].indexOf('p4-256mb'));
  });

  it('leaves TODOs instead of inventing numbers', () => {
    const files = buildReport([]);
    expect(files['resume-bullets.md'].match(/TODO/g)).toHaveLength(3);
    expect(files['spike-test.md']).toContain('TODO');
  });

  it('refuses the zero-lost bullet when events were lost', () => {
    const files = buildReport([{ ...results[0], lost: 3 }]);
    expect(files['resume-bullets.md']).toContain('spike run lost 3 events');
  });
});

describe('batching recount', () => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { runWindow, applyMetrics } = require('../../scripts/measure/recount');
  it('derives the run window from the runId timestamp (start padded 1 min, no end padding)', () => {
    const w = runWindow({ runId: 'bt1-20261108T003704-5f21', measuredAt: '2026-11-08T00:39:13.000Z' });
    expect(w.start.toISOString()).toBe('2026-11-08T00:36:04.000Z');
    expect(w.end.toISOString()).toBe('2026-11-08T00:39:13.000Z');
  });
  it('recomputes per-10k numbers from exact metric totals and keeps the Logs Insights values', () => {
    const r = applyMetrics({ completed: 1000, invocations: 985, billedMs: 50000, processor: { memoryMb: 256, arch: 'arm64' } }, 1000, 60000, 0);
    expect(r.countSource).toBe('cloudwatch-metrics');
    expect(r.logsInsights).toEqual({ invocations: 985, billedMs: 50000 });
    expect(r.per10kEvents.invocations).toBe(10000);
    expect(r.eventsPerInvocation).toBe(1);
    expect(r.avgDurationMs).toBe(60);
  });
});
