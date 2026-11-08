# Spike test

Measured 2026-11-08 (run `spike-20261108T002315-f6b0`) with `npm run measure:spike`, deployed in measurement mode (on-demand DynamoDB, API limit 200 rps). Processor capped at 2 concurrent invocations (SQS event-source maxConcurrency); account Lambda concurrency limit 10.

| Metric | Value |
|---|---|
| Events sent | 10,000 (1,000 API requests × 10 events at 25 req/s) |
| Events accepted by the API | 10,000 (12 request retries after throttling/5xx) |
| Ingest duration / rate | 40.0 s / 250 events/s |
| Peak queue depth | 9,590 messages |
| Time to drain (first request → queue empty) | 506.3 s |
| Backlog cleared after the last request | 466.3 s |
| Events processed (COMPLETED idempotency record) | 10,000 |
| **Events lost** | **0** |
| New dead-letter messages | 0 |
| Outcomes | STALE: 5,389, NOTIFIED: 4,611 |
| Processor invocations / cold starts | 1,001 / 2 |
| API response time p50 / p95 | 60 ms / 97 ms |

**How "lost" is measured:** every accepted `eventId` is looked up in DynamoDB (`IDEMP#<eventId>`); an event counts as processed only when its record is `COMPLETED`. `STALE` means the event was processed but was older than the flight's current status (many events hit the same 100 flights out of order), so no notification was sent, by design.

**What it shows:** the API only validates and enqueues, so ingestion runs far faster than processing; SQS absorbs the burst while the processor drains it at a capped rate, protecting DynamoDB and SNS.

Queue depth over time (seconds since the first request):

| t (s) | waiting | in flight |
|---|---|---|
| 0 | 0 | 0 |
| 20.5 | 2010 | 12 |
| 40.9 | 4800 | 10 |
| 61.4 | 9430 | 20 |
| 81.9 | 9330 | 20 |
| 102.4 | 8950 | 20 |
| 122.8 | 8540 | 10 |
| 143.4 | 8320 | 20 |
| 164 | 7800 | 20 |
| 184.5 | 8220 | 20 |
| 205 | 7410 | 20 |
| 225.5 | 6470 | 20 |
| 245.9 | 6120 | 10 |
| 266.4 | 5740 | 20 |
| 287 | 5140 | 20 |
| 307.5 | 4750 | 20 |
| 328.1 | 4430 | 20 |
| 348.5 | 3930 | 20 |
| 368.9 | 3560 | 20 |
| 389.4 | 3300 | 10 |
| 409.8 | 2500 | 20 |
| 430.3 | 2240 | 10 |
| 450.8 | 1440 | 0 |
| 471.3 | 1066 | 14 |
| 491.9 | 192 | 18 |
| 510.4 | 0 | 0 |
