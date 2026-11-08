#!/usr/bin/env node
import { App, Tags } from 'aws-cdk-lib';
import { COLD_START_PROFILES, FlightDisruptionStack, type ColdStartProfileName } from './stack';
import { GithubOidcStack } from './github-oidc-stack';

const app = new App();
const ctx = (k: string): string | undefined => {
  const v = app.node.tryGetContext(k) ?? process.env[`FDN_${k.replace(/([A-Z])/g, '_$1').toUpperCase()}`];
  return v === undefined || v === '' ? undefined : String(v);
};
const env = { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION };

// Measurement mode (Phase 3): on-demand DynamoDB so spike/batching runs aren't throttled, higher API limits.
const measure = ctx('measure') === 'true';
const billingMode = (ctx('billingMode') ?? (measure ? 'PAY_PER_REQUEST' : 'PROVISIONED')).toUpperCase();
const coldStartProfile = ctx('coldStartProfile');
if (coldStartProfile && !(coldStartProfile in COLD_START_PROFILES)) {
  throw new Error(`coldStartProfile must be one of: ${Object.keys(COLD_START_PROFILES).join(', ')}`);
}
if (billingMode !== 'PROVISIONED' && billingMode !== 'PAY_PER_REQUEST') throw new Error('billingMode must be PROVISIONED or PAY_PER_REQUEST');

const stack = new FlightDisruptionStack(app, ctx('stackName') ?? 'FlightDisruptionNotifier', {
  env,
  description: 'Flight Disruption Notifier: API Gateway + Lambda + SQS/DLQ + DynamoDB + SNS',
  notificationEmail: ctx('notificationEmail'),
  alarmEmail: ctx('alarmEmail'),
  billingMode,
  feedScheduleEnabled: ctx('feedScheduleEnabled') === 'true',
  processorMaxConcurrency: ctx('processorMaxConcurrency') ? Number(ctx('processorMaxConcurrency')) : undefined,
  processorBatchSize: ctx('processorBatchSize') ? Number(ctx('processorBatchSize')) : undefined,
  apiRateLimit: measure ? 200 : undefined,
  apiBurstLimit: measure ? 400 : undefined,
  coldStartProfile: coldStartProfile as ColdStartProfileName | undefined,
});
Tags.of(stack).add('project', 'flight-disruption-notifier');

// Optional one-time stack for GitHub Actions OIDC deploys:  -c githubRepo=owner/repo
const githubRepo = ctx('githubRepo');
if (githubRepo) {
  const oidc = new GithubOidcStack(app, 'FdnGithubOidc', { env, githubRepo, existingProviderArn: ctx('githubOidcProviderArn') });
  Tags.of(oidc).add('project', 'flight-disruption-notifier');
}
