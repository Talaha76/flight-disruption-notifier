# Architecture

## Components
| Component | Resource | Notes |
|---|---|---|
| API | API Gateway **HTTP API**, `$default` stage, throttled 20 rps / burst 50 | Cheaper than REST API |
| Ingest λ | `src/handlers/ingest.ts` | zod validation, fills `eventId`/`source`, `SendMessageBatch` |
| Queue | SQS standard `flight-events`, SSE, visibility 180 s, retention 4 d | redrive → DLQ after 3 receives |
| DLQ | SQS `flight-events-dlq`, retention 14 d | CloudWatch alarm when > 0 |
| Processor λ | `src/handlers/processor.ts`, timeout 30 s | batch 10, window 5 s, `ReportBatchItemFailures`, maxConcurrency 2 |
| Subscriptions λ | `src/handlers/subscriptions.ts` | subscriptions CRUD + flight status |
| Feed adapter λ | `src/handlers/feed-adapter.ts` | legacy XML → versioned JSON events; bad records → DLQ |
| Scheduler | EventBridge Scheduler, rate(1 minute), **disabled by default** | |
| Table | DynamoDB single table, provisioned 5/5 (+ GSI1 5/5), TTL `ttl` | |
| Notifications | SNS topic → SQS outbox (raw) + optional email (filter `channel=email`) | |
| Monitoring | 1 dashboard, 3 alarms → alarm topic, X-Ray active tracing, JSON logs, 7-day retention | |

## Event envelope (v1)
```json
{ "eventId": "evt-123", "source": "api|legacy-feed|skybook", "type": "DELAY|CANCELLATION|GATE_CHANGE",
  "version": 1, "flightNumber": "AC123", "flightDate": "2026-11-20", "occurredAt": "2026-11-20T10:00:00Z",
  "delayMinutes": 95, "newDepartureTime": "…", "newGate": "C32", "oldGate": "C30", "reason": "Weather" }
```
Unknown `version`/`type` or invalid fields → DLQ (non-retryable).

## Single-table design
| Entity | PK | SK | Attributes |
|---|---|---|---|
| Subscription | `FLIGHT#AC123#2026-11-20` | `SUB#<userId>` | userId, channel, contact, createdAt, GSI1PK=`USER#<userId>`, GSI1SK=`FLIGHT#AC123#2026-11-20` |
| Flight status | `FLIGHT#AC123#2026-11-20` | `STATUS` | status, gate, delayMinutes, lastEventAt, lastEventId, updatedAt |
| Event idempotency | `IDEMP#<eventId>` | `IDEMP` | status (IN_PROGRESS/COMPLETED), lockExpiresAt, result, ttl (24 h) |
| API idempotency | `APIKEY#<Idempotency-Key>` | `APIKEY` | requestHash, statusCode, body, ttl (24 h) |

## Processor algorithm
1. Validate each record → poison records sent to DLQ with `errorReason`.
2. Group by `eventId` (duplicates in one batch collapse to one leader).
3. `acquire()` idempotency per leader: `ACQUIRED` → process; `ALREADY_COMPLETED` → ack; `IN_PROGRESS_ELSEWHERE` → batch item failure (retry).
4. `BatchGetItem` STATUS items for all flights (UnprocessedKeys retried with backoff).
5. Sort by `occurredAt`; for each: skip if stale → conditional `UpdateItem` STATUS → `Query` subscribers (cached per flight) → SNS `PublishBatch` → `complete()`.
6. Any error → `release()` the lock and report only that message in `batchItemFailures`.

Why BatchGetItem is used for statuses rather than subscribers: subscribers are 1:N per flight, so they need `Query` (BatchGetItem fetches exact keys only). The per-batch cache means N events on the same flight cost 1 Query.

## Timeouts
Lambda timeout 30 s < idempotency lock 120 s < SQS visibility 180 s (6× timeout, AWS guidance for batching windows).
