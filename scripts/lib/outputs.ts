/** Reads the stack outputs written by `npm run deploy` (cdk deploy --outputs-file cdk-outputs.json). */
import * as fs from 'node:fs';
import * as path from 'node:path';

export interface StackOutputs {
  ApiUrl: string;
  TableName: string;
  QueueUrl: string;
  QueueArn: string;
  DlqUrl: string;
  DlqArn: string;
  OutboxQueueUrl: string;
  TopicArn: string;
  FeedAdapterFunctionName: string;
  ProcessorFunctionName: string;
  DashboardName: string;
  region: string;
}

export function loadOutputs(stackName = process.env.FDN_STACK_NAME ?? 'FlightDisruptionNotifier'): StackOutputs {
  const file = path.resolve(__dirname, '..', '..', 'cdk-outputs.json');
  if (!fs.existsSync(file)) throw new Error(`cdk-outputs.json not found at ${file}. Run "npm run deploy" first.`);
  const all = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, Record<string, string>>;
  const o = all[stackName];
  if (!o) throw new Error(`Stack ${stackName} not found in cdk-outputs.json`);
  const region = /sqs\.([a-z0-9-]+)\.amazonaws\.com/.exec(o.QueueUrl)?.[1];
  if (!region) throw new Error('Could not infer region from QueueUrl');
  process.env.AWS_REGION ??= region;
  return { ...(o as unknown as StackOutputs), ApiUrl: o.ApiUrl.replace(/\/$/, ''), region };
}
