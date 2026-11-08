# Progress tracker

Legend: ✅ done and verified here · 🟡 written, needs your AWS account to verify · ⏳ planned

## Phase 1: core pipeline (this release)
| Item | Status | Evidence |
|---|---|---|
| Repo layout, TypeScript, ESLint, Jest | ✅ | `npm run verify` |
| CDK stack (all resources, least-privilege grants) | ✅ | `cdk synth` + 10 assertion tests (`test/infra`) |
| HTTP API: flight-events, subscriptions (+Idempotency-Key), list, delete, flight status | ✅ | unit tests |
| Ingest validation (zod) + SendMessageBatch | ✅ | unit tests |
| SQS + DLQ (maxReceiveCount 3) + partial batch failures | ✅ | unit + infra tests |
| Idempotent processing (conditional put, TTL, lock takeover) | ✅ | unit test: 20 duplicates → 1 notification |
| Poison messages → DLQ without blocking the batch | ✅ | unit tests |
| BatchGetItem (UnprocessedKeys retry), Query subscribers, SNS PublishBatch | ✅ | unit tests |
| Out-of-order / stale event protection | ✅ | unit tests |
| Legacy XML feed adapter + EventBridge Scheduler | ✅ | unit tests (schedule disabled by default) |
| CloudWatch dashboard, 3 alarms, X-Ray, JSON logs, 7-day retention | ✅ (synth) | infra tests |
| DLQ redrive + seed scripts | 🟡 | written; not yet run against AWS |
| Deploy to AWS (us-east-2, 50 resources) | ✅ | `npm run deploy` 2026-11-07, 99 s |
| End-to-end integration tests on AWS | ✅ | 4/4 passed 2026-11-07: API→SQS→λ→DDB→SNS→outbox; 20 duplicates → 1 notification; poison → DLQ; 400 on invalid |
| GitHub Actions CI + OIDC deploy stack | 🟡 | CI runs on first push; deploy needs OIDC setup |
| README, architecture doc, teardown | ✅ | |

## Phase 2: verify on AWS ✅
- Deployed and verified 2026-11-07. First-run API→notification latency: 22.4 s (single cold sample, includes cold starts, the 5 s batching window and test polling; not a resume number).
- ⏳ Look at dashboard/alarms in the console; measure p50/p95 latency properly in Phase 3.

## Phase 3: measurements (resume numbers; real data only)
| Item | Status |
|---|---|
| Measurement tooling: latency, spike, batching, cold-start scripts + report generator (unit-tested) | ✅ |
| `docs/latency.md`: p50/p95 queue → notification | ⏳ run `npm run measure:latency` |
| `docs/spike-test.md`: 10k-event spike, peak depth, drain time, lost = 0 | ⏳ run `npm run measure:spike` |
| `docs/batching.md`: invocations + cost per 10k, batch 1 vs 10 | ⏳ run `npm run measure:batching` twice |
| `docs/cold-starts.md`: p50/p95 init per profile | ⏳ run `npm run measure:coldstarts` per profile |
| `docs/resume-bullets.md` | ⏳ `npm run measure:report` fills it |
| Powertools idempotency comparison | ⏳ optional |

## Not yet done / optional
- API authentication (IAM or Lambda authorizer); the API is currently public but throttled.
- AppSync, Step Functions, FIFO benchmark, Bedrock message rewriting (stretch goals).
