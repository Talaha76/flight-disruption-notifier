/** Pure helpers shared by the measurement scripts (unit-tested in test/unit/measure-stats.test.ts). */

export function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  // nearest-rank method
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank - 1))];
}

export function summarize(values: number[]): { n: number; p50: number | null; p95: number | null; min: number | null; max: number | null; avg: number | null } {
  const n = values.length;
  return {
    n,
    p50: percentile(values, 50),
    p95: percentile(values, 95),
    min: n ? Math.min(...values) : null,
    max: n ? Math.max(...values) : null,
    avg: n ? values.reduce((a, b) => a + b, 0) / n : null,
  };
}

export interface ReportLine {
  durationMs: number;
  billedMs: number;
  memoryMb: number;
  maxMemoryUsedMb: number;
  initMs?: number;
}

/** Parses the Lambda platform REPORT line (text log format). */
export function parseReportLine(log: string): ReportLine | null {
  const m = /REPORT RequestId:.*?Duration: ([\d.]+) ms\s+Billed Duration: (\d+) ms\s+Memory Size: (\d+) MB\s+Max Memory Used: (\d+) MB(?:\s+Init Duration: ([\d.]+) ms)?/.exec(log);
  if (!m) return null;
  return {
    durationMs: Number(m[1]),
    billedMs: Number(m[2]),
    memoryMb: Number(m[3]),
    maxMemoryUsedMb: Number(m[4]),
    initMs: m[5] !== undefined ? Number(m[5]) : undefined,
  };
}

/**
 * Lambda list prices (us-east-1/us-east-2, checked Nov 2026: https://aws.amazon.com/lambda/pricing/).
 * The always-free tier (1M requests + 400k GB-s/month) makes the real bill $0 at this scale;
 * these numbers are for comparing configurations.
 */
export const LAMBDA_PRICE = {
  perRequest: 0.2 / 1_000_000,
  perGbSecond: { arm64: 0.0000133334, x86_64: 0.0000166667 } as Record<string, number>,
};

export function lambdaCost(invocations: number, billedMs: number, memoryMb: number, arch = 'arm64'): { gbSeconds: number; requestCost: number; computeCost: number; total: number } {
  const gbSeconds = (billedMs / 1000) * (memoryMb / 1024);
  const requestCost = invocations * LAMBDA_PRICE.perRequest;
  const computeCost = gbSeconds * (LAMBDA_PRICE.perGbSecond[arch] ?? LAMBDA_PRICE.perGbSecond.x86_64);
  return { gbSeconds, requestCost, computeCost, total: requestCost + computeCost };
}

export const round = (v: number | null | undefined, digits = 1): number | null =>
  v === null || v === undefined || !Number.isFinite(v) ? null : Math.round(v * 10 ** digits) / 10 ** digits;

export const pctChange = (before: number, after: number): number => (before === 0 ? 0 : ((before - after) / before) * 100);
