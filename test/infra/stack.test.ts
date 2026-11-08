import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { FlightDisruptionStack } from '../../infra/stack';

let template: Template;
beforeAll(() => {
  const app = new App();
  template = Template.fromStack(new FlightDisruptionStack(app, 'TestStack', { env: { account: '123456789012', region: 'ca-central-1' } }));
}, 120000);

describe('FlightDisruptionStack', () => {
  it('creates a provisioned single table with TTL and GSI1 inside the always-free limits', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      KeySchema: [{ AttributeName: 'PK', KeyType: 'HASH' }, { AttributeName: 'SK', KeyType: 'RANGE' }],
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 },
      TimeToLiveSpecification: { AttributeName: 'ttl', Enabled: true },
      GlobalSecondaryIndexes: [Match.objectLike({ IndexName: 'GSI1', ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 } })],
    });
  });

  it('wires the main queue to a DLQ with maxReceiveCount 3 and visibility = 6x processor timeout', () => {
    template.hasResourceProperties('AWS::SQS::Queue', {
      VisibilityTimeout: 180,
      RedrivePolicy: { deadLetterTargetArn: Match.anyValue(), maxReceiveCount: 3 },
    });
  });

  it('configures the processor SQS trigger for batching + partial batch failures', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 10,
      MaximumBatchingWindowInSeconds: 5,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
      ScalingConfig: { MaximumConcurrency: 2 },
    });
  });

  it('uses arm64 Node 22 Lambdas with X-Ray tracing and 7-day logs', () => {
    template.resourcePropertiesCountIs('AWS::Lambda::Function', { Architectures: ['arm64'], Runtime: 'nodejs22.x', TracingConfig: { Mode: 'Active' } }, 4);
    template.resourcePropertiesCountIs('AWS::Logs::LogGroup', { RetentionInDays: 7 }, 4);
  });

  it('exposes the HTTP API routes with stage throttling', () => {
    for (const route of ['POST /flight-events', 'POST /subscriptions', 'GET /users/{userId}/subscriptions', 'DELETE /subscriptions/{flightNumber}/{flightDate}/{userId}', 'GET /flights/{flightNumber}/{flightDate}']) {
      template.hasResourceProperties('AWS::ApiGatewayV2::Route', { RouteKey: route });
    }
    template.hasResourceProperties('AWS::ApiGatewayV2::Stage', { StageName: '$default', DefaultRouteSettings: { ThrottlingBurstLimit: 50, ThrottlingRateLimit: 20 } });
  });

  it('subscribes the test-mode outbox queue to the notifications topic (raw delivery)', () => {
    template.hasResourceProperties('AWS::SNS::Subscription', { Protocol: 'sqs', RawMessageDelivery: true });
  });

  it('creates the feed schedule disabled by default and 3 alarms (free tier: <=10)', () => {
    template.hasResourceProperties('AWS::Scheduler::Schedule', { ScheduleExpression: 'rate(1 minute)', State: 'DISABLED' });
    template.resourceCountIs('AWS::CloudWatch::Alarm', 3);
    template.resourceCountIs('AWS::CloudWatch::Dashboard', 1);
  });

  it('does not use paid-by-default features (no VPC, no provisioned concurrency)', () => {
    template.resourceCountIs('AWS::EC2::VPC', 0);
    template.resourceCountIs('AWS::EC2::NatGateway', 0);
    template.resourceCountIs('AWS::Lambda::Alias', 0);
  });

  it('grants the ingest function only SQS send on the main queue', () => {
    const policies = template.findResources('AWS::IAM::Policy');
    const ingest = Object.entries(policies).find(([id]) => id.startsWith('IngestFnServiceRoleDefaultPolicy'))![1];
    const actions = (ingest.Properties.PolicyDocument.Statement as { Action: string | string[] }[]).flatMap((s) => [s.Action].flat());
    expect(actions.every((a) => a.startsWith('sqs:') || a.startsWith('xray:'))).toBe(true);
  });

  it('can switch to on-demand for spike tests', () => {
    const t = Template.fromStack(new FlightDisruptionStack(new App(), 'OnDemand', { billingMode: 'PAY_PER_REQUEST', feedScheduleEnabled: true }));
    t.hasResourceProperties('AWS::DynamoDB::Table', { BillingMode: 'PAY_PER_REQUEST' });
    t.hasResourceProperties('AWS::Scheduler::Schedule', { State: 'ENABLED' });
  });
});

describe('experiment configuration', () => {
  it('applies cold-start profiles and batch size', () => {
    const t = Template.fromStack(new FlightDisruptionStack(new App(), 'Exp', { coldStartProfile: 'p0-baseline', processorBatchSize: 1, apiRateLimit: 200, apiBurstLimit: 400 }));
    t.resourcePropertiesCountIs('AWS::Lambda::Function', { Architectures: ['x86_64'], MemorySize: 128 }, 4);
    t.hasResourceProperties('AWS::Lambda::EventSourceMapping', { BatchSize: 1, MaximumBatchingWindowInSeconds: Match.absent() });
    t.hasResourceProperties('AWS::ApiGatewayV2::Stage', { DefaultRouteSettings: { ThrottlingRateLimit: 200, ThrottlingBurstLimit: 400 } });
  });
});
