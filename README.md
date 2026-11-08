<div align="center">

# Flight Disruption Notifier

**Serverless, Event-Driven Flight Disruption Alerts on AWS**

[![CI](https://github.com/Talaha76/flight-disruption-notifier/actions/workflows/ci.yml/badge.svg)](https://github.com/Talaha76/flight-disruption-notifier/actions/workflows/ci.yml)
[![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?logo=typescript&logoColor=white)](https://www.typescriptlang.org/)
[![AWS Lambda](https://img.shields.io/badge/AWS_Lambda-Node.js_22-FF9900?logo=awslambda&logoColor=white)](https://aws.amazon.com/lambda/)
[![Amazon SQS](https://img.shields.io/badge/Amazon_SQS-DLQ_%2B_Batching-FF4F8B?logo=amazonsqs&logoColor=white)](https://aws.amazon.com/sqs/)
[![DynamoDB](https://img.shields.io/badge/DynamoDB-Single_Table-4053D6?logo=amazondynamodb&logoColor=white)](https://aws.amazon.com/dynamodb/)
[![AWS CDK](https://img.shields.io/badge/AWS_CDK-Infrastructure_as_Code-232F3E?logo=amazonwebservices&logoColor=white)](https://aws.amazon.com/cdk/)

**10,000-event spike · 0 lost messages · −40% cold-start latency · −89% Lambda invocations**

</div>


## Table of Contents

- [Overview](#overview)
- [Measured Results](#measured-results)
- [Key Features](#key-features)
- [System Architecture](#system-architecture)
- [Event Processing Flow](#event-processing-flow)
- [Technology Stack](#technology-stack)
- [Repository Structure](#repository-structure)
- [Local Development](#local-development)
- [Deploying to AWS](#deploying-to-aws)
- [Verifying the Deployment](#verifying-the-deployment)
- [Running the Measurements](#running-the-measurements)
- [Tearing Down](#tearing-down)
- [API Overview](#api-overview)
- [Data Model](#data-model)
- [Configuration](#configuration)
- [Testing](#testing)
- [CI/CD](#cicd)
- [Cost](#cost)
- [Design Notes](#design-notes)
- [Limitations](#limitations)
- [Future Improvements](#future-improvements)
- [Project Goal](#project-goal)

---

## Overview

Flight Disruption Notifier ingests flight status changes (**delays, cancellations and gate changes**), works out which passengers are subscribed to each affected flight, and notifies them.

Events arrive through a REST API or from a simulated "legacy" XML status feed. API Gateway and Lambda validate them and put them on an SQS queue, which absorbs traffic spikes. A batch processor makes every message duplicate-safe with DynamoDB conditional writes, records the latest flight status, looks up subscribers and publishes notifications through SNS. Malformed messages are routed to a dead-letter queue instead of blocking the pipeline.

Everything is defined in **AWS CDK (TypeScript)**, tested with Jest (unit, infrastructure-assertion and integration tests against the deployed stack), and measured with scripts in this repository. Every number below comes from those scripts.

> **Portfolio project:** this repository demonstrates asynchronous messaging, idempotent processing, batching, cold-start tuning, dead-letter handling, infrastructure as code and evidence-based performance measurement on AWS serverless.

---

## Measured Results

Measured on AWS (us-east-2) on 2026-11-07. Raw data is in [`results/`](results/) and full tables are in [`docs/`](docs/).

| Measurement | Result | Details |
|---|---|---|
| **Spike test** | **10,000 events, 0 lost**, 0 new dead-letter messages | Sent in 40 s (~250 events/s); peak queue depth 9,590; drained in 506 s. [spike-test.md](docs/spike-test.md) |
| **Idempotency** | **20 duplicate deliveries → exactly 1 notification** | Integration test against the deployed stack |
| **Batching** | **−89% Lambda invocations per 10k events** (9,850 → 1,040) | Batch size 1 vs 10 + 5 s window; Lambda cost per 10k events −54%. [batching.md](docs/batching.md) |
| **Cold starts** | **−40% cold-start latency** (1,238 → 743 ms median) | Init −22% (392 → 305 ms), package 374 → 94 KB, warm invocation 33.8 → 12.7 ms. [cold-starts.md](docs/cold-starts.md) |
| **Latency** | API response p50 60 ms / p95 96 ms | Queue → notification p50 3.9 s / p95 8.2 s, dominated by the deliberate 5 s batching window. [latency.md](docs/latency.md) |

"Lost" is verified, not assumed: every accepted `eventId` must have a `COMPLETED` idempotency record in DynamoDB.

---

## Key Features

- **REST API** on API Gateway HTTP API for flight events and passenger subscriptions.
- **Schema validation** with zod and a versioned event envelope (`source` + `type` + `version`).
- **SQS buffering** so ingestion stays fast while processing runs at a controlled rate.
- **Dead-letter queue** with `maxReceiveCount: 3`, a CloudWatch alarm and a redrive script.
- **Poison-message isolation:** invalid messages go straight to the DLQ with an `errorReason`, without blocking their batch.
- **Partial batch failures** (`ReportBatchItemFailures`): only failed messages are retried.
- **Idempotent processing** with DynamoDB conditional writes, a 24-hour TTL and an in-progress lock.
- **Out-of-order protection:** stale events update nothing and send no notification.
- **Batching end to end:** `SendMessageBatch`, SQS batch size 10, `BatchGetItem`, per-batch subscriber cache, SNS `PublishBatch`.
- **Idempotency-Key header** on `POST /subscriptions` replays the stored response for retried requests.
- **Legacy XML feed adapter** on an EventBridge Scheduler rule (disabled by default).
- **Observability:** CloudWatch dashboard, 3 alarms, X-Ray tracing on every Lambda, structured JSON logs, 7-day retention.
- **Cost guard-rails:** stage throttling, provisioned DynamoDB inside the always-free tier, no VPC or NAT gateway.

---

## System Architecture

```text
  EventBridge Scheduler (1 min, off by default)          Clients / load tests
            │                                                   │
            ▼                                                   ▼
  ┌─────────────────────┐                     ┌──────────────────────────────┐
  │ Feed Adapter λ      │                     │ API Gateway (HTTP API)       │
  │ legacy XML → JSON   │                     │ POST /flight-events          │
  │ bad records → DLQ   │                     │ POST /subscriptions  ...     │
  └──────────┬──────────┘                     └───────┬──────────────┬───────┘
             │                                        ▼              ▼
             │                         ┌──────────────────────┐  ┌───────────────────┐
             │                         │ Ingest λ             │  │ Subscriptions λ   │
             │                         │ zod → SendMessageBatch│  │ CRUD + status     │
             │                         └──────────┬───────────┘  └─────────┬─────────┘
             ▼                                    ▼                        │
  ┌──────────────────────────────────────────────────────────┐            │
  │ SQS: flight-events (standard, SSE)                       │──(3 fails)─▶ SQS DLQ ─▶ CloudWatch alarm
  └─────────────────────────────┬────────────────────────────┘            │
                                │ batch 10 · 5 s window · max concurrency 2
                                ▼                                          │
  ┌──────────────────────────────────────────────────────────┐            │
  │ Processor λ                                              │            │
  │ • validate (poison → DLQ)   • idempotency check (DDB)    │            │
  │ • BatchGetItem statuses     • conditional status update  │◀───────────┤
  │ • Query subscribers         • SNS PublishBatch           │            │
  │ • report partial batch failures                          │            │
  └─────────────────────────────┬────────────────────────────┘            │
                                ▼                                          ▼
  ┌──────────────────────────────────┐          ┌──────────────────────────────────┐
  │ SNS topic                        │          │ DynamoDB (single table)          │
  │ → SQS outbox (test mode)         │          │ subscriptions · flight status    │
  │ → email (filter: channel=email)  │          │ idempotency records (TTL) · GSI1 │
  └──────────────────────────────────┘          └──────────────────────────────────┘

  CloudWatch: dashboard, alarms, Logs Insights · X-Ray: Lambda traces
```

More detail: [docs/architecture.md](docs/architecture.md).

### Why a queue sits in the middle

The Ingest Lambda only validates and enqueues, so it answers in about 60 ms even during a spike. SQS holds the backlog, and the processor drains it at a capped concurrency, which protects DynamoDB and SNS downstream. In the spike test the API accepted ~250 events/s while the processor worked through the backlog at ~21 events/s, and nothing was lost.

---

## Event Processing Flow

```text
POST /flight-events (1–10 events)
    ↓
zod validation (400 with field-level issues on failure)
    ↓
SendMessageBatch → SQS flight-events        → 202 { accepted: [eventIds] }
    ↓
Processor batch (up to 10 messages)
    ↓
Invalid / unknown version?  ──yes──▶ DLQ with errorReason (not retried)
    ↓ no
Duplicate eventIds in batch collapsed (leader / followers)
    ↓
Idempotency: conditional PutItem IDEMP#<eventId>
    ├─ already COMPLETED  → skip (ack)
    ├─ in progress elsewhere → report as batch item failure (retry later)
    └─ acquired ↓
BatchGetItem current flight statuses
    ↓
Older than current status? ──yes──▶ mark COMPLETED (STALE), no notification
    ↓ no
Conditional UpdateItem flight STATUS
    ↓
Query subscribers (cached per flight within the batch)
    ↓
SNS PublishBatch (10 per call, transient failures retried)
    ↓
Mark idempotency record COMPLETED
```

Any error releases that event's lock and reports only its message in `batchItemFailures`. After three failed receives, SQS moves it to the DLQ.

---

## Technology Stack

### Application

- TypeScript
- Node.js 22 on AWS Lambda (arm64 / Graviton)
- AWS SDK for JavaScript v3 (modular clients)
- zod (schema validation)
- fast-xml-parser (legacy feed)

### AWS Services

- API Gateway HTTP API
- AWS Lambda
- Amazon SQS (standard queue + dead-letter queue)
- Amazon DynamoDB (single table, TTL, GSI)
- Amazon SNS
- Amazon EventBridge Scheduler
- Amazon CloudWatch (logs, metrics, alarms, dashboard, Logs Insights)
- AWS X-Ray

### Infrastructure and Tooling

- AWS CDK v2 (TypeScript), with esbuild bundling through `NodejsFunction`
- Jest, ts-jest and `aws-sdk-client-mock`
- ESLint with typescript-eslint
- GitHub Actions (CI, plus an opt-in OIDC deploy workflow)

---

## Repository Structure

```text
flight-disruption-notifier/
├── infra/
│   ├── app.ts                 # CDK app: reads context flags / env vars
│   ├── stack.ts               # every AWS resource
│   └── github-oidc-stack.ts   # optional role for GitHub Actions deploys
│
├── src/
│   ├── handlers/
│   │   ├── ingest.ts          # POST /flight-events
│   │   ├── processor.ts       # SQS consumer
│   │   ├── subscriptions.ts   # subscription + flight status routes
│   │   └── feed-adapter.ts    # legacy XML feed → JSON events
│   ├── lib/                   # idempotency, repository, queue, notifications, logger
│   └── schemas/               # zod event + subscription schemas
│
├── test/
│   ├── unit/                  # handlers, schemas, measurement maths
│   ├── infra/                 # CDK template assertions
│   ├── integration/           # end-to-end tests against the deployed stack
│   └── helpers/               # fixtures + in-memory DynamoDB fake
│
├── scripts/
│   ├── measure/               # latency, spike, batching, cold-start, recount, report
│   ├── dlq-redrive.ts
│   └── seed-subscriptions.ts
│
├── docs/                      # architecture + measured results
├── results/                   # raw measurement data (JSON)
├── load-tests/                # optional k6 script
├── sample-data/               # sample XML feed + events
├── .github/workflows/         # ci.yml, deploy.yml
├── cdk.json
├── package.json
└── README.md
```

---

## Local Development

### Prerequisites

- Node.js 22 LTS
- Git
- AWS CLI v2 and an AWS account (only for deploying)

### 1. Clone and install

```bash
git clone https://github.com/Talaha76/flight-disruption-notifier.git
cd flight-disruption-notifier
npm ci
```

### 2. Verify everything locally (no AWS needed)

```bash
npm run verify
```

This runs the TypeScript compiler, ESLint, 56 unit and infrastructure tests, and `cdk synth`.

> On Windows, if PowerShell blocks `npm` scripts, run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.

---

## Deploying to AWS

> Deploying may cost a few cents. Create an **AWS Budget** with email alerts first.

### 1. Configure credentials

Create an IAM user for CLI access and run:

```powershell
aws configure                 # access key, secret, region (e.g. us-east-2), json
aws sts get-caller-identity   # confirms the account
```

Never commit credentials. `.gitignore` excludes `.env` and `cdk-outputs.json`.

### 2. Bootstrap CDK (once per account and region)

```powershell
$env:CDK_DEFAULT_REGION = "us-east-2"
npx cdk bootstrap
```

### 3. Deploy

```powershell
npm run deploy
```

The stack outputs (API URL, queue URLs, table name and more) are written to `cdk-outputs.json`.

> **Region note:** the account used for this project belongs to an AWS Organization whose service control policy blocks CloudFormation in `ca-central-1`, so it is deployed to `us-east-2`. Any region you're allowed to use works.

---

## Verifying the Deployment

```powershell
npm run test:integration
```

These tests run against the live stack and check that:

- an event travels API → SQS → Processor → DynamoDB → SNS and reaches the subscriber,
- 20 duplicate deliveries of one `eventId` produce exactly one notification,
- a poison message is parked in the DLQ with its reason,
- invalid events are rejected with HTTP 400.

### Try it manually

```powershell
$api = (Get-Content cdk-outputs.json | ConvertFrom-Json).FlightDisruptionNotifier.ApiUrl
$today = (Get-Date).ToString("yyyy-MM-dd")

Invoke-RestMethod -Method Post -Uri "$api/subscriptions" -ContentType "application/json" `
  -Body (@{ userId="demo"; flightNumber="AC123"; flightDate=$today; channel="test"; contact="demo@example.com" } | ConvertTo-Json)

Invoke-RestMethod -Method Post -Uri "$api/flight-events" -ContentType "application/json" `
  -Body (@{ type="DELAY"; version=1; flightNumber="AC123"; flightDate=$today; occurredAt=(Get-Date).ToUniversalTime().ToString("o"); delayMinutes=95; reason="Weather" } | ConvertTo-Json)

Start-Sleep 10
Invoke-RestMethod -Uri "$api/flights/AC123/$today"
```

---

## Running the Measurements

Measurement mode switches DynamoDB to on-demand and raises the API limit so load tests aren't throttled.

```powershell
$env:CDK_DEFAULT_REGION = "us-east-2"
$env:FDN_MEASURE = "true"
npm run deploy

npm run measure:latency     # p50/p95 queue → notification
npm run measure:spike       # 10,000-event spike, peak depth, drain time, lost = 0

$env:FDN_PROCESSOR_BATCH_SIZE = "1"; npm run deploy; npm run measure:batching
Remove-Item Env:FDN_PROCESSOR_BATCH_SIZE; npm run deploy; npm run measure:batching
npm run measure:recount     # exact invocation counts from CloudWatch metrics

foreach ($p in "p0-baseline","p1-minify","p2-runtime-sdk","p3-arm64","p4-256mb") {
  $env:FDN_COLD_START_PROFILE = $p
  npm run deploy; npm run measure:coldstarts
}
Remove-Item Env:FDN_COLD_START_PROFILE

npm run measure:report      # writes docs/*.md from results/*.json
```

The whole sequence costs well under $0.10.

---

## Tearing Down

```powershell
$env:CDK_DEFAULT_REGION = "us-east-2"
npm run destroy
```

This removes every resource in the stack. To remove CDK's bootstrap resources too, delete the **CDKToolkit** stack in CloudFormation and empty and delete its `cdk-hnb659fds-assets-…` S3 bucket. Then delete the IAM access key.

---

## API Overview

```text
POST   /flight-events                                      1–10 events → 202 { accepted }
POST   /subscriptions                                      supports Idempotency-Key header
GET    /users/{userId}/subscriptions                       a passenger's subscriptions
DELETE /subscriptions/{flightNumber}/{flightDate}/{userId} 204 / 404
GET    /flights/{flightNumber}/{flightDate}                current flight status
```

### Event shape (version 1)

```json
{
  "eventId": "optional; generated if missing",
  "type": "DELAY | CANCELLATION | GATE_CHANGE",
  "version": 1,
  "flightNumber": "AC123",
  "flightDate": "2026-11-20",
  "occurredAt": "2026-11-20T10:00:00Z",
  "delayMinutes": 95,
  "newGate": "C32",
  "reason": "Weather"
}
```

`DELAY` requires `delayMinutes`, `GATE_CHANGE` requires `newGate`, and `CANCELLATION` needs no extra fields.

---

## Data Model

A single DynamoDB table holds every entity:

| Entity | PK | SK | Notes |
|---|---|---|---|
| Subscription | `FLIGHT#AC123#2026-11-20` | `SUB#<userId>` | channel, contact; GSI1 = `USER#<userId>` |
| Flight status | `FLIGHT#AC123#2026-11-20` | `STATUS` | status, gate, delay, lastEventAt, lastEventId |
| Event idempotency | `IDEMP#<eventId>` | `IDEMP` | IN_PROGRESS / COMPLETED, result, 24 h TTL |
| API idempotency | `APIKEY#<key>` | `APIKEY` | stored response, request hash, 24 h TTL |

---

## Configuration

Set these as CDK context flags (`-c key=value`) or as PowerShell environment variables:

| Context flag | Environment variable | Default | Purpose |
|---|---|---|---|
| `notificationEmail` | `FDN_NOTIFICATION_EMAIL` | none | Email for `channel=email` notifications |
| `alarmEmail` | `FDN_ALARM_EMAIL` | none | Email for CloudWatch alarms |
| `measure` | `FDN_MEASURE` | `false` | On-demand DynamoDB + higher API limit |
| `billingMode` | `FDN_BILLING_MODE` | `PROVISIONED` | DynamoDB capacity mode |
| `processorBatchSize` | `FDN_PROCESSOR_BATCH_SIZE` | `10` | SQS batch size |
| `processorMaxConcurrency` | `FDN_PROCESSOR_MAX_CONCURRENCY` | `2` | Max concurrent processors |
| `coldStartProfile` | `FDN_COLD_START_PROFILE` | `p4-256mb` | Cold-start experiment profile |
| `feedScheduleEnabled` | `FDN_FEED_SCHEDULE_ENABLED` | `false` | Run the XML feed every minute |

See [`.env.example`](.env.example).

---

## Testing

```bash
npm test                    # 56 unit + infrastructure tests (no AWS needed)
npm run test:integration    # 4 end-to-end tests against the deployed stack
```

- **Unit tests** mock AWS with `aws-sdk-client-mock`. They use an in-memory DynamoDB fake that enforces the same condition expressions, so idempotency, ordering and partial-failure behaviour are tested for real.
- **Infrastructure tests** assert the synthesized CloudFormation template: DLQ wiring, batching settings, arm64, log retention, throttling, IAM scope, and no VPC or NAT gateway.
- **Integration tests** exercise the deployed stack end to end.

---

## CI/CD

- **`ci.yml`** runs the typecheck, lint, unit and infrastructure tests, and `cdk synth` on every push and pull request.
- **`deploy.yml`** runs `cdk diff` on pull requests, and `cdk deploy` plus the integration tests on merges to `main`. It uses GitHub OIDC, so no AWS keys are stored in GitHub. It stays disabled until you deploy the `FdnGithubOidc` stack and set the repository variable `AWS_DEPLOY_ENABLED=true`.

---

## Cost

The project is designed for the AWS always-free tier:

- provisioned DynamoDB at 10 RCU/WCU (25 are free),
- 7-day log retention,
- 3 alarms and 1 dashboard,
- notifications to an SQS outbox unless you add an email,
- API throttled to 20 requests/s,
- no VPC, NAT gateway or provisioned concurrency.

API Gateway HTTP API costs about $1 per million requests. Idle SQS triggers still poll, so run `npm run destroy` when you're finished.

---

## Design Notes

### SQS standard + idempotent handler (not FIFO)

Standard queues give higher throughput at lower cost but deliver **at least once**. Instead of paying for exactly-once delivery, the processor is duplicate-safe: a conditional `PutItem` on `IDEMP#<eventId>` lets exactly one attempt through. The in-progress lock (120 s) is longer than the Lambda timeout (30 s) and shorter than the visibility timeout (180 s), so a crashed attempt never blocks its own retry.

### Poison messages vs transient failures

Invalid messages can never succeed, so they go straight to the DLQ with the reason attached instead of using up three retries. Transient failures (throttling, a downstream outage) are reported as partial batch failures and retried. After three failed receives they move to the DLQ too.

### Concurrency cap on the event source

The processor's concurrency is capped with the SQS event source's `maxConcurrency` rather than reserved concurrency. The account used here has a total Lambda concurrency limit of 10, which reserved concurrency would have to come out of.

### Out-of-order safety

Flight status updates are conditional on `occurredAt`, so an older event can never overwrite a newer one. A retry of the *same* event is allowed through, so a crash between "status written" and "notified" never loses a notification.

### Batching trade-off

A 5-second batching window cuts invocations per 10k events by 89%, but at low traffic it adds up to 5 seconds of latency (median queue → notification: 3.9 s). For flight alerts that's an acceptable trade. Setting the window to 0 would favour latency over cost.

### Cold starts

The biggest single gain came from 256 MB of memory, because more memory also means more CPU for the first DynamoDB call. Using the Lambda runtime's built-in SDK cut the deployment package from 374 KB to 94 KB, and arm64 gave the largest drop in init time. SDK clients are created once per execution environment and reused across invocations.

---

## Limitations

- The API has **no authentication** yet. It is public but throttled.
- X-Ray traces cover the Lambda functions. AWS SDK calls (DynamoDB, SNS) aren't instrumented as subsegments, and HTTP APIs don't support X-Ray.
- If SNS accepts only part of a `PublishBatch` and the message is retried, some subscribers can get a duplicate notification (at-least-once delivery).
- The `sms` channel is validated and stored, but no SMS is sent; notifications reach the SQS outbox and, optionally, email.
- Measurements were taken in one region on a small account (10 concurrent Lambdas) with 15 cold-start samples per profile. They show relative improvements, not production capacity.

---

## Future Improvements

- Authentication (IAM authorization or a Lambda authorizer).
- X-Ray instrumentation of AWS SDK calls for full end-to-end traces.
- Comparison with the Powertools for AWS Lambda idempotency utility.
- Per-subscriber delivery records to close the partial-publish duplicate gap.
- SMS and push delivery channels.
- SQS FIFO with `MessageGroupId = flightId` for strictly ordered updates, benchmarked against standard.
- Step Functions rebooking workflow.
- AppSync subscriptions for live flight status in a web page.
- Kafka (MSK) instead of SQS where events need replay or multiple consumer groups.

---

## Project Goal

Flight Disruption Notifier was built to show how an event-driven system behaves under real conditions: duplicate deliveries, malformed input, traffic spikes and cold starts. It **measures** that behaviour instead of just claiming it.

The project combines:

**Serverless Compute + Asynchronous Messaging + Idempotent Processing + Infrastructure as Code + Automated Testing + Evidence-Based Performance Tuning**

into one production-style AWS application.
