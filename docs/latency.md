# End-to-end latency

Measured 2026-11-08 (run `lat-20261108T001601-4a21`) with `npm run measure:latency`: 100 single-event API requests at 2/s, each to its own subscribed flight.

| Metric | p50 | p95 | max | samples |
|---|---|---|---|---|
| Pipeline: SQS enqueue → SNS publish | 3,862 ms | 8,156 ms | 21,300 ms | 100 |
| API response (client-side) | 60 ms | 96 ms | 1,068 ms | 100 |

Notifications received: 100/100; lost: 0.

The pipeline number uses AWS clocks only (SQS `SentTimestamp` and the processor's publish time, both carried in each notification), so local clock skew doesn't affect it. At low traffic, most of it is the **5 s SQS batching window**: the event source waits up to 5 s to fill a batch of 10 before invoking the processor. That's the deliberate trade of a little latency for far fewer invocations (see batching.md). Setting the window to 0 would cut latency at the cost of more invocations.
