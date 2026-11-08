# Batching: batch size 1 vs 10

Same workload for both runs: 1000 gate-change events (10 per API request, 100 flights, no subscribers) in measurement mode. Processor invocations and total duration come from the CloudWatch `AWS/Lambda` Invocations and Duration metrics for each run window (`npm run measure:recount`). Cost uses Lambda list prices (arm64, 256 MB); the always-free tier makes the real bill $0 at this scale.

| Metric | Batch size 1 | Batch size 10 + 5 s window | Change |
|---|---|---|---|
| Events processed | 1,000 | 1,000 | |
| Processor invocations | 985 | 104 | |
| Events per invocation | 1.02 | 9.62 | |
| Avg / p95 duration per invocation | 64 / 116 ms | 475 / 865 ms | |
| **Invocations per 10k events** | 9,850 | 1,040 | **−89.4%** |
| GB-seconds per 10k events | 156.436 | 123.533 | −21.0% |
| Lambda cost per 10k events | $0.00406 | $0.00186 | −54.2% |
| Events lost | 0 | 0 | |

**Why it helps:** with batch size 10 one invocation pays the per-invocation overhead once for up to 10 messages, makes one `BatchGetItem` for every flight status in the batch (instead of one per event), reuses the subscriber lookup for repeated flights, and deletes successful messages in bulk. `ReportBatchItemFailures` keeps batching safe: only failed messages are retried.

**Trade-off:** the 5 s batching window adds up to 5 s of latency at low traffic (see latency.md), in exchange for fewer, fuller invocations.
