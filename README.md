# Flight Disruption Notifier (Serverless on AWS)

A serverless, event-driven pipeline that ingests flight disruptions (delays, cancellations, gate changes) and notifies subscribed passengers.

**Stack:** TypeScript · AWS CDK v2 · API Gateway HTTP API · Lambda (Node.js 22, arm64) · SQS + dead-letter queue · DynamoDB (single table, TTL, GSI) · SNS · EventBridge Scheduler · CloudWatch · X-Ray · Jest · GitHub Actions (OIDC)

```
 POST /flight-events ─▶ Ingest λ (zod validate, SendMessageBatch) ─▶ SQS flight-events ──(3 failed receives)──▶ DLQ ─▶ alarm
 EventBridge Scheduler ─▶ Feed Adapter λ (legacy XML ─▶ JSON) ─────────▲          │ batch 10 / 5 s window
 SkyBook Kafka bridge ─────────────────────────────────────────────────┘          ▼
                                                      Processor λ: poison ─▶ DLQ · idempotency (DDB conditional put)
                                                      · BatchGetItem flight statuses · conditional status update
                                                      · Query subscribers · SNS PublishBatch · partial batch failures
                                                                                   ▼
                                                      SNS topic ─▶ SQS outbox (test mode, free) / email (filter: channel=email)
 POST /subscriptions (Idempotency-Key) · GET /users/{id}/subscriptions · DELETE /subscriptions/... · GET /flights/{no}/{date}
```

Details: [docs/architecture.md](docs/architecture.md). Status of every requirement: [docs/PROGRESS.md](docs/PROGRESS.md).

---

## 1. Run locally (no AWS needed)

Prerequisite: **Node.js 22 LTS** (https://nodejs.org). In **Windows PowerShell**, from the project folder:

```powershell
npm ci
npm run verify      # typecheck + lint + 47 unit/infra tests + cdk synth
```

> If PowerShell says *"running scripts is disabled on this system"*, run once:
> `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` (or use `npm.cmd` instead of `npm`).

## 2. Deploy to AWS (needs your approval: may cost a few cents)

> **Region:** this account belongs to an AWS Organization whose service control policy blocks CloudFormation in `ca-central-1`, so the project is deployed to **`us-east-2`**. Any allowed region works; check one with `aws cloudformation list-stacks --region <region> --max-items 1`.

### 2.1 One-time setup
1. **AWS Budget:** Billing console → Budgets → *Create budget* → *Zero spend* or *$5 monthly* template → your email.
2. **AWS CLI v2:** install from https://aws.amazon.com/cli/ then check `aws --version`.
3. **Credentials** (never put keys in this repo):
   - IAM console → Users → *Create user* (e.g. `fdn-dev`) → attach `AdministratorAccess` → *Security credentials* → *Create access key* (CLI).
   - Then:
     ```powershell
     aws configure                      # paste key id + secret, region: us-east-2, output: json
     aws sts get-caller-identity        # confirm: prints your account id
     ```
4. **Bootstrap CDK** (creates a small S3 bucket + roles in the region; cents/month):
   ```powershell
   $env:CDK_DEFAULT_REGION = "us-east-2"
   npx cdk bootstrap
   ```

### 2.2 Deploy
```powershell
$env:CDK_DEFAULT_REGION = "us-east-2"
npm run deploy
# optional: real email notifications + alarm emails (confirm the SNS emails AWS sends you)
npm run deploy -- -c notificationEmail=you@example.com -c alarmEmail=you@example.com
```
`cdk-outputs.json` is written with `ApiUrl`, queue URLs, table name, etc.

### 2.3 Verify end to end
```powershell
npm run test:integration
```
This runs against the deployed stack and asserts:
- API event → SQS → Processor → DynamoDB → SNS → subscriber notification (via the SQS outbox)
- 20 duplicate deliveries of one `eventId` → exactly **1** notification
- a poison message is parked in the DLQ with its error reason
- invalid events are rejected with HTTP 400

Manual try-out:
```powershell
$api = (Get-Content cdk-outputs.json | ConvertFrom-Json).FlightDisruptionNotifier.ApiUrl
$today = (Get-Date).ToString("yyyy-MM-dd")
Invoke-RestMethod -Method Post -Uri "$api/subscriptions" -ContentType "application/json" `
  -Body (@{ userId="talaha"; flightNumber="AC123"; flightDate=$today; channel="email"; contact="you@example.com" } | ConvertTo-Json)
Invoke-RestMethod -Method Post -Uri "$api/flight-events" -ContentType "application/json" `
  -Body (@{ type="DELAY"; version=1; flightNumber="AC123"; flightDate=$today; occurredAt=(Get-Date).ToUniversalTime().ToString("o"); delayMinutes=95; reason="Weather" } | ConvertTo-Json)
Start-Sleep 10
Invoke-RestMethod -Uri "$api/flights/AC123/$today"
```

Other tools:
```powershell
npm run seed -- AC123 2026-11-20 25          # 25 test-channel subscriptions
npm run redrive -- --peek                    # list DLQ messages + reasons
npm run redrive                              # move DLQ messages back to the main queue
aws lambda invoke --function-name (Get-Content cdk-outputs.json | ConvertFrom-Json).FlightDisruptionNotifier.FeedAdapterFunctionName out.json; Get-Content out.json   # one legacy-feed run
```

### 2.4 Measurements (Phase 3: resume numbers)
All scripts use your deployed stack (`cdk-outputs.json`) and write raw results to `results/*.json`; `npm run measure:report` turns them into `docs/spike-test.md`, `docs/latency.md`, `docs/batching.md`, `docs/cold-starts.md` and `docs/resume-bullets.md` (measured values only; anything missing stays a TODO).

```powershell
$env:CDK_DEFAULT_REGION = "us-east-2"
$env:FDN_MEASURE = "true"            # measurement mode: on-demand DynamoDB + API limit 200 rps
npm run deploy
npm run measure:latency              # ~3 min   p50/p95 queue -> notification
npm run measure:spike                # ~5-10 min 10,000-event spike, peak depth, drain time, lost = 0

$env:FDN_PROCESSOR_BATCH_SIZE = "1"; npm run deploy; npm run measure:batching     # ~3 min
Remove-Item Env:FDN_PROCESSOR_BATCH_SIZE; npm run deploy; npm run measure:batching # ~2 min
npm run measure:recount              # exact invocation counts from CloudWatch metrics (a few min after the runs)

foreach ($p in "p0-baseline","p1-minify","p2-runtime-sdk","p3-arm64","p4-256mb") {
  $env:FDN_COLD_START_PROFILE = $p
  npm run deploy; if ($LASTEXITCODE -ne 0) { break }
  npm run measure:coldstarts; if ($LASTEXITCODE -ne 0) { break }
}
Remove-Item Env:FDN_COLD_START_PROFILE

npm run measure:report
```
Cost: well under $0.10 (about 15k events through on-demand DynamoDB, ~1.5k API requests).

## 3. Tear down (do this when finished)
```powershell
npm run destroy
```
Removes every resource in the stack (table, queues, topic, functions, logs, dashboard, alarms). To also remove CDK's bootstrap resources: CloudFormation console → delete stack **CDKToolkit**, then empty + delete the `cdk-hnb659fds-assets-…` S3 bucket. Finally delete the IAM access key you created.

## 4. Configuration

| Context flag (`-c key=value`) | Env var | Default | Purpose |
|---|---|---|---|
| `notificationEmail` | `FDN_NOTIFICATION_EMAIL` | none | Email that receives `channel=email` notifications |
| `alarmEmail` | `FDN_ALARM_EMAIL` | none | Email for CloudWatch alarms |
| `billingMode` | `FDN_BILLING_MODE` | `PROVISIONED` | `PAY_PER_REQUEST` for spike tests only |
| `feedScheduleEnabled` | `FDN_FEED_SCHEDULE_ENABLED` | `false` | Run the XML feed simulator every minute |
| `processorMaxConcurrency` | `FDN_PROCESSOR_MAX_CONCURRENCY` | `2` | Max concurrent processor invocations |
| `githubRepo` | `FDN_GITHUB_REPO` | none | Adds the `FdnGithubOidc` stack for CI deploys |

## 5. API

| Method | Path | Notes |
|---|---|---|
| POST | `/flight-events` | One event, an array of ≤10, or `{ "events": [...] }`. `eventId`/`source` optional. → `202 {accepted:[ids]}` |
| POST | `/subscriptions` | `{userId, flightNumber, flightDate, channel: email\|sms\|test, contact}`; optional `Idempotency-Key` header |
| GET | `/users/{userId}/subscriptions` | A passenger's subscriptions (GSI1) |
| DELETE | `/subscriptions/{flightNumber}/{flightDate}/{userId}` | 204 / 404 |
| GET | `/flights/{flightNumber}/{flightDate}` | Current status |

Event shapes (`version: 1`): `DELAY` needs `delayMinutes`; `GATE_CHANGE` needs `newGate` (optional `oldGate`); `CANCELLATION` needs nothing extra. All need `flightNumber` (e.g. `AC123`), `flightDate` (`YYYY-MM-DD`), `occurredAt` (ISO-8601).

## 6. CI/CD (GitHub Actions + OIDC, no stored AWS keys)
- `ci.yml`: typecheck, lint, unit + infra tests, `cdk synth` on every PR and push.
- `deploy.yml`: `cdk diff` on PRs, `cdk deploy` + integration tests on merge to `main`. Disabled until you enable it:
  1. `npx cdk deploy FdnGithubOidc -c githubRepo=<owner>/<repo>` → copy the `DeployRoleArn` output.
     (If the account already has a GitHub OIDC provider, add `-c githubOidcProviderArn=<arn>`.)
  2. GitHub repo → Settings → Secrets and variables → Actions: secret `AWS_DEPLOY_ROLE_ARN`; variables `AWS_REGION=us-east-2`, `AWS_DEPLOY_ENABLED=true`.

## 7. Cost notes
Designed for the always-free tier: provisioned DynamoDB (10 RCU/WCU incl. GSI, of 25 free), no VPC/NAT, no provisioned concurrency, 7-day log retention, 3 alarms + 1 dashboard, notifications go to an SQS outbox unless you add an email, API stage throttled to 20 req/s, feed schedule off by default. API Gateway HTTP API is ~$1 per million requests. Idle SQS triggers still poll, so **run `npm run destroy` when finished**.

## 8. Design decisions worth explaining
- **SQS standard + idempotent handler** rather than FIFO: higher throughput, cheaper; duplicates handled by DynamoDB conditional writes (`IDEMP#<eventId>`, 24 h TTL, 120 s in-progress lock that is longer than the 30 s Lambda timeout and shorter than the 180 s visibility timeout).
- **Poison messages go straight to the DLQ** (with an `errorReason` attribute) instead of burning 3 retries; transient failures use partial batch failure + `maxReceiveCount: 3`.
- **Out-of-order safety:** status updates are conditional on `occurredAt`; stale events are completed without notifying. A retry of the same event is allowed through so a crash between "status written" and "notified" never loses a notification.
- **Batching everywhere:** `SendMessageBatch` at ingest, SQS batch 10 + 5 s window, `BatchGetItem` for flight statuses, per-batch subscriber cache, SNS `PublishBatch` (10/call).
- **`maxConcurrency` on the event source** (not reserved concurrency) caps the processor, because new accounts often have a 10-concurrency limit that reserved concurrency can't dip into.
- **Known trade-off:** if SNS accepts some entries of a PublishBatch and the record then fails, the retry can re-notify those subscribers (at-least-once). Per-subscriber delivery records would close this gap.

## 9. Publish to GitHub
```powershell
git init
git add .
git commit -m "Flight Disruption Notifier: serverless AWS pipeline with CDK, tests and measurements"
git branch -M main
git remote add origin https://github.com/<you>/flight-disruption-notifier.git
git push -u origin main
```
`.gitignore` keeps `node_modules/`, `cdk.out/`, `cdk-outputs.json` and `.env` out of the repo. The CI workflow runs on every push.
