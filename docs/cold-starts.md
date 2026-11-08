# Cold starts

Measured with `npm run measure:coldstarts` on the Subscriptions function (GET /flights → one DynamoDB GetItem). Before every cold sample the script changes an environment variable, which forces a new execution environment. `Init Duration` and the first invocation's duration come from the Lambda REPORT line. Warm samples reuse the environment and its SDK clients (created outside the handler).

**Cold-start latency** = init + first invocation, i.e. what the first request actually waits for. The first invocation includes the first DynamoDB call (TLS connection, credentials), which is CPU-bound, so memory size matters a lot.

| Profile | Change | Arch | Memory (MB) | Code (KB) | Init p50 (ms) | Init max* (ms) | Cold-start latency p50 (ms) | Cold-start latency max* (ms) | Warm invoke p50 (ms) | Cold samples |
|---|---|---|---|---|---|---|---|---|---|---|
| p0-baseline | Baseline: unminified bundle incl. AWS SDK, x86_64, 128 MB | x86_64 | 128 | 374 | 391.5 | 433.1 | 1,238 | 1,301 | 33.8 | 15 |
| p1-minify | + esbuild minify | x86_64 | 128 | 269 | 378.6 | 422.2 | 1,178 | 1,275 | 36.3 | 15 |
| p2-runtime-sdk | + use the Lambda runtime's built-in AWS SDK v3 (not bundled) | x86_64 | 128 | 94 | 369.3 | 417.2 | 1,175 | 1,252 | 30.1 | 15 |
| p3-arm64 | + arm64 (Graviton) | arm64 | 128 | 94 | 312.5 | 326.4 | 1,175 | 1,236 | 52.5 | 15 |
| p4-256mb | + 256 MB memory (default) | arm64 | 256 | 94 | 305.2 | 414.9 | 743 | 886 | 12.7 | 15 |

\* With 15 samples, the nearest-rank p95 is the slowest sample, so a single outlier sets it. Medians (p50) are the reliable comparison here.

**Result (p0-baseline → p4-256mb):** cold-start latency 1,238 → 743 ms median (**−40%**), init 392 → 305 ms median (**−22%**), deployment package 374 → 94 KB, warm invocation 33.8 → 12.7 ms.

Biggest single step for init time: + arm64 (Graviton) (−57 ms). Biggest single step for cold-start latency: + 256 MB memory (default) (−433 ms).

Notes: the baseline is already an esbuild bundle (unminified, AWS SDK v3 bundled into the zip); a non-bundled "whole node_modules" zip was not measured. Provisioned concurrency (experiment 6 in the guide) was skipped because it isn't free. arm64 at 128 MB had a slower warm invocation than x86 at 128 MB; at 256 MB the warm invocation is the fastest of all profiles.
