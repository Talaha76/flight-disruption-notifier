/**
 * Cold-start measurement for one profile (see COLD_START_PROFILES in infra/stack.ts).
 * Forces a fresh execution environment before each sample by changing an environment variable,
 * invokes the Subscriptions function (GET /flights/... -> one DynamoDB GetItem), and parses the
 * REPORT line from the invoke log tail. Then takes warm samples on the same environment.
 *   $env:FDN_COLD_START_PROFILE="p0-baseline"; npm run deploy; npm run measure:coldstarts
 * Usage: npm run measure:coldstarts [-- --samples 15 --warm 10]
 */
import { GetFunctionConfigurationCommand, InvokeCommand, UpdateFunctionConfigurationCommand, waitUntilFunctionUpdatedV2 } from '@aws-sdk/client-lambda';
import { context, functionInfo, log, numArg, sleep, writeResult } from './lib';
import { parseReportLine, summarize, type ReportLine } from './stats';

const PROFILE_LABELS: Record<string, string> = {
  'p0-baseline': 'Baseline: unminified bundle incl. AWS SDK, x86_64, 128 MB',
  'p1-minify': '+ esbuild minify',
  'p2-runtime-sdk': "+ use the Lambda runtime's built-in AWS SDK v3 (not bundled)",
  'p3-arm64': '+ arm64 (Graviton)',
  'p4-256mb': '+ 256 MB memory (default)',
  'p5-512mb': '512 MB memory',
  'p6-1024mb': '1024 MB memory',
};

const payload = JSON.stringify({
  version: '2.0', routeKey: 'GET /flights/{flightNumber}/{flightDate}', rawPath: '/flights/ZZ1/2030-01-01', rawQueryString: '',
  headers: {}, pathParameters: { flightNumber: 'ZZ1', flightDate: '2030-01-01' }, isBase64Encoded: false,
  requestContext: { requestId: 'coldstart-probe', routeKey: 'GET /flights/{flightNumber}/{flightDate}', http: { method: 'GET', path: '/flights/ZZ1/2030-01-01' } },
});

async function main() {
  const ctx = context();
  const fnName = ctx.out.SubscriptionsFunctionName;
  if (!fnName) throw new Error('SubscriptionsFunctionName missing from cdk-outputs.json: redeploy with the Phase 3 code (npm run deploy).');
  const samples = numArg('samples', 15);
  const warmSamples = numArg('warm', 10);
  const info = await functionInfo(ctx, fnName);
  const profile = info.profile ?? 'unknown';
  log(`cold starts for profile ${profile} (${info.arch}, ${info.memoryMb} MB, code ${(info.codeSizeBytes / 1024).toFixed(0)} KB): ${samples} cold + ${warmSamples} warm samples`);

  const invoke = async (): Promise<ReportLine> => {
    const res = await ctx.lambda.send(new InvokeCommand({ FunctionName: fnName, LogType: 'Tail', Payload: Buffer.from(payload) }));
    const tail = Buffer.from(res.LogResult ?? '', 'base64').toString('utf8');
    const report = parseReportLine(tail);
    if (res.FunctionError || !report) throw new Error(`Invoke failed: ${res.FunctionError ?? 'no REPORT line'}\n${tail}`);
    return report;
  };

  const cold: ReportLine[] = [];
  for (let i = 0; i < samples; i++) {
    const cfg = await ctx.lambda.send(new GetFunctionConfigurationCommand({ FunctionName: fnName }));
    await ctx.lambda.send(new UpdateFunctionConfigurationCommand({
      FunctionName: fnName,
      Environment: { Variables: { ...(cfg.Environment?.Variables ?? {}), COLD_START_NONCE: `${Date.now()}-${i}` } },
    }));
    await waitUntilFunctionUpdatedV2({ client: ctx.lambda, maxWaitTime: 180 }, { FunctionName: fnName });
    await sleep(1000);
    const r = await invoke();
    if (r.initMs === undefined) log(`  sample ${i + 1}: no Init Duration (environment was reused); skipping`);
    else {
      cold.push(r);
      log(`  cold ${i + 1}/${samples}: init ${r.initMs.toFixed(1)} ms, first invoke ${r.durationMs.toFixed(1)} ms`);
    }
  }
  const warm: ReportLine[] = [];
  for (let i = 0; i < warmSamples; i++) warm.push(await invoke());

  const result = {
    kind: 'coldstarts', profile, label: PROFILE_LABELS[profile] ?? profile, measuredAt: new Date().toISOString(),
    function: 'SubscriptionsFn (GET /flights -> DynamoDB GetItem)',
    arch: info.arch, memoryMb: info.memoryMb, codeSizeKb: Math.round(info.codeSizeBytes / 1024),
    initMs: summarize(cold.map((r) => r.initMs!)),
    coldInvokeMs: summarize(cold.map((r) => r.durationMs)),
    coldTotalMs: summarize(cold.map((r) => r.initMs! + r.durationMs)),
    warmInvokeMs: summarize(warm.map((r) => r.durationMs)),
    maxMemoryUsedMb: Math.max(0, ...cold.map((r) => r.maxMemoryUsedMb)),
    raw: { cold, warm },
  };
  writeResult(`coldstarts-${profile}`, result);
  log(`${profile}: init p50 ${result.initMs.p50} ms / p95 ${result.initMs.p95} ms; warm invoke p50 ${result.warmInvokeMs.p50} ms`);
}

main().catch((e) => { console.error(e); process.exit(1); });
