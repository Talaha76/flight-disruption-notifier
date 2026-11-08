/**
 * Recounts the batching runs from the CloudWatch AWS/Lambda metrics (Invocations, Duration, Errors),
 * which are exact once settled, unlike Logs Insights counts taken right after a run.
 * Updates results/batching-*.json in place (the original Logs Insights numbers are kept under `logsInsights`).
 * Usage: npm run measure:recount
 */
import * as fs from 'node:fs';
import * as path from 'node:path';
import { CloudWatchClient, GetMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { loadOutputs } from '../lib/outputs';
import { lambdaCost, round } from './stats';

/* eslint-disable @typescript-eslint/no-explicit-any */
type R = Record<string, any>;
const PAD_MS = 60_000;

/**
 * Run window. Start: the runId timestamp (e.g. bt1-20261108T003704-5f21) minus 1 min. End: measuredAt, which is
 * already ~1 min after the queue drained. No end padding, so back-to-back runs (≥ 2 min apart) don't overlap
 * once CloudWatch aligns the window to whole minutes.
 */
export function runWindow(r: R): { start: Date; end: Date } {
  if (r.window?.startMs && r.window?.endMs) return { start: new Date(r.window.startMs - PAD_MS), end: new Date(r.window.endMs + PAD_MS) };
  const m = /-(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})-/.exec(r.runId ?? '');
  if (!m) throw new Error(`Cannot derive a time window from runId ${r.runId}`);
  const start = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]);
  return { start: new Date(start - PAD_MS), end: new Date(Date.parse(r.measuredAt)) };
}

/** Recomputes the derived batching fields from exact metric totals. */
export function applyMetrics(r: R, invocations: number, durationMsSum: number, errors: number): R {
  const completed = r.completed || 0;
  const per10k = completed ? 10_000 / completed : 0;
  const cost = lambdaCost(invocations, durationMsSum, r.processor.memoryMb, r.processor.arch);
  return {
    ...r,
    logsInsights: r.logsInsights ?? { invocations: r.invocations, billedMs: r.billedMs },
    countSource: 'cloudwatch-metrics',
    invocations,
    processorErrors: errors,
    billedMs: Math.round(durationMsSum),
    eventsPerInvocation: round(completed / Math.max(1, invocations), 2),
    avgDurationMs: round(durationMsSum / Math.max(1, invocations)),
    per10kEvents: {
      invocations: Math.round(invocations * per10k),
      gbSeconds: round(cost.gbSeconds * per10k, 3),
      lambdaCostUsd: round(cost.total * per10k, 5),
    },
  };
}

async function main() {
  const out = loadOutputs() as ReturnType<typeof loadOutputs> & Record<string, string>;
  const cw = new CloudWatchClient({ region: out.region });
  const dir = path.resolve(__dirname, '..', '..', 'results');
  const files = fs.readdirSync(dir).filter((f) => /^batching-.*\.json$/.test(f));
  if (!files.length) throw new Error('No results/batching-*.json files found');
  for (const f of files) {
    const file = path.join(dir, f);
    const r: R = JSON.parse(fs.readFileSync(file, 'utf8'));
    const { start, end } = runWindow(r);
    const metric = (id: string, name: string) => ({
      Id: id,
      MetricStat: { Metric: { Namespace: 'AWS/Lambda', MetricName: name, Dimensions: [{ Name: 'FunctionName', Value: out.ProcessorFunctionName }] }, Period: 60, Stat: 'Sum' },
    });
    const res = await cw.send(new GetMetricDataCommand({
      StartTime: start, EndTime: end,
      MetricDataQueries: [metric('inv', 'Invocations'), metric('dur', 'Duration'), metric('err', 'Errors')],
    }));
    const sum = (id: string) => (res.MetricDataResults?.find((m) => m.Id === id)?.Values ?? []).reduce((a, b) => a + b, 0);
    const updated = applyMetrics(r, sum('inv'), sum('dur'), sum('err'));
    fs.writeFileSync(file, JSON.stringify(updated, null, 2) + '\n');
    console.log(`${f}: window ${start.toISOString().slice(11, 19)}–${end.toISOString().slice(11, 19)} UTC | Logs Insights ${updated.logsInsights.invocations} -> CloudWatch metric ${updated.invocations} invocations (${updated.eventsPerInvocation} events/invocation, ${updated.processorErrors} errors); per 10k: ${updated.per10kEvents.invocations} invocations, $${updated.per10kEvents.lambdaCostUsd}`);
  }
}

if (require.main === module) main().catch((e) => { console.error(e); process.exit(1); });
