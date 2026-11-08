import * as path from 'node:path';
import { CfnOutput, Duration, RemovalPolicy, Stack, type StackProps } from 'aws-cdk-lib';
import { HttpApi, HttpMethod, HttpStage, CorsHttpMethod } from 'aws-cdk-lib/aws-apigatewayv2';
import { HttpLambdaIntegration } from 'aws-cdk-lib/aws-apigatewayv2-integrations';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cwActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { NodejsFunction, OutputFormat } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import * as schedulerTargets from 'aws-cdk-lib/aws-scheduler-targets';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import type { Construct } from 'constructs';

export interface FlightDisruptionStackProps extends StackProps {
  /** Email that receives channel=email passenger notifications (SNS email subscription; must be confirmed). */
  notificationEmail?: string;
  /** Email that receives CloudWatch alarm notifications. */
  alarmEmail?: string;
  /** PROVISIONED (default, always-free 25 RCU/WCU) or PAY_PER_REQUEST (spike tests only). */
  billingMode?: 'PROVISIONED' | 'PAY_PER_REQUEST';
  /** Enable the 1-minute EventBridge schedule for the legacy feed simulator (default false). */
  feedScheduleEnabled?: boolean;
  /** Max concurrent processor invocations from the SQS event source (2-1000, default 2). */
  processorMaxConcurrency?: number;
  /** SQS batch size for the processor (default 10). Batch size 1 disables the batching window. Used by the batching experiment. */
  processorBatchSize?: number;
  /** API stage throttling (default 20 rps / burst 50; measurement mode raises it). */
  apiRateLimit?: number;
  apiBurstLimit?: number;
  /** Cold-start experiment profile applied to every function (default p4-256mb = the optimized configuration). */
  coldStartProfile?: ColdStartProfileName;
}

/**
 * Cold-start experiment profiles (docs/cold-starts.md). Each step adds one optimization.
 * p4-256mb is the production default.
 */
export const COLD_START_PROFILES = {
  'p0-baseline': { arch: 'x86_64', memoryMb: 128, minify: false, externalSdk: false, label: 'Baseline: unminified bundle incl. AWS SDK, x86_64, 128 MB' },
  'p1-minify': { arch: 'x86_64', memoryMb: 128, minify: true, externalSdk: false, label: '+ esbuild minify' },
  'p2-runtime-sdk': { arch: 'x86_64', memoryMb: 128, minify: true, externalSdk: true, label: '+ use the Lambda runtime\'s built-in AWS SDK v3 (not bundled)' },
  'p3-arm64': { arch: 'arm64', memoryMb: 128, minify: true, externalSdk: true, label: '+ arm64 (Graviton)' },
  'p4-256mb': { arch: 'arm64', memoryMb: 256, minify: true, externalSdk: true, label: '+ 256 MB memory (default)' },
  'p5-512mb': { arch: 'arm64', memoryMb: 512, minify: true, externalSdk: true, label: '512 MB memory' },
  'p6-1024mb': { arch: 'arm64', memoryMb: 1024, minify: true, externalSdk: true, label: '1024 MB memory' },
} as const;
export type ColdStartProfileName = keyof typeof COLD_START_PROFILES;

const HANDLERS = path.join(__dirname, '..', 'src', 'handlers');
const PROCESSOR_TIMEOUT = Duration.seconds(30);

export class FlightDisruptionStack extends Stack {
  constructor(scope: Construct, id: string, props: FlightDisruptionStackProps = {}) {
    super(scope, id, props);

    // ---------- DynamoDB single table ----------
    const provisioned = (props.billingMode ?? 'PROVISIONED') === 'PROVISIONED';
    const table = new dynamodb.Table(this, 'Table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      billingMode: provisioned ? dynamodb.BillingMode.PROVISIONED : dynamodb.BillingMode.PAY_PER_REQUEST,
      ...(provisioned ? { readCapacity: 5, writeCapacity: 5 } : {}),
      timeToLiveAttribute: 'ttl',
      removalPolicy: RemovalPolicy.DESTROY,
    });
    table.addGlobalSecondaryIndex({
      indexName: 'GSI1',
      partitionKey: { name: 'GSI1PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'GSI1SK', type: dynamodb.AttributeType.STRING },
      projectionType: dynamodb.ProjectionType.ALL,
      ...(provisioned ? { readCapacity: 5, writeCapacity: 5 } : {}),
    });

    // ---------- SQS ----------
    const dlq = new sqs.Queue(this, 'FlightEventsDlq', {
      retentionPeriod: Duration.days(14),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const queue = new sqs.Queue(this, 'FlightEventsQueue', {
      // AWS guidance: visibility timeout >= 6x function timeout when batching.
      visibilityTimeout: Duration.seconds(PROCESSOR_TIMEOUT.toSeconds() * 6),
      retentionPeriod: Duration.days(4),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      deadLetterQueue: { queue: dlq, maxReceiveCount: 3 },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // ---------- SNS ----------
    const topic = new sns.Topic(this, 'NotificationsTopic', { displayName: 'Flight disruption notifications' });
    // Test-mode outbox: every notification lands here (free), so tests can assert delivery without email.
    const outbox = new sqs.Queue(this, 'NotificationsOutbox', {
      retentionPeriod: Duration.days(1),
      encryption: sqs.QueueEncryption.SQS_MANAGED,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    topic.addSubscription(new subs.SqsSubscription(outbox, { rawMessageDelivery: true }));
    if (props.notificationEmail) {
      topic.addSubscription(
        new subs.EmailSubscription(props.notificationEmail, {
          filterPolicy: { channel: sns.SubscriptionFilter.stringFilter({ allowlist: ['email'] }) },
        }),
      );
    }

    // ---------- Lambda ----------
    const profileName: ColdStartProfileName = props.coldStartProfile ?? 'p4-256mb';
    const profile = COLD_START_PROFILES[profileName];
    if (!profile) throw new Error(`Unknown coldStartProfile ${profileName}`);
    const fn = (name: string, entry: string, env: Record<string, string>, extra: Partial<ConstructorParameters<typeof NodejsFunction>[2]> = {}) => {
      const logGroup = new logs.LogGroup(this, `${name}Logs`, { retention: logs.RetentionDays.ONE_WEEK, removalPolicy: RemovalPolicy.DESTROY });
      return new NodejsFunction(this, name, {
        entry: path.join(HANDLERS, entry),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_22_X,
        architecture: profile.arch === 'arm64' ? lambda.Architecture.ARM_64 : lambda.Architecture.X86_64,
        memorySize: profile.memoryMb,
        timeout: Duration.seconds(10),
        tracing: lambda.Tracing.ACTIVE,
        logGroup,
        environment: { LOG_LEVEL: 'info', SERVICE_NAME: `fdn-${name.toLowerCase()}`, COLD_START_PROFILE: profileName, ...env },
        bundling: {
          minify: profile.minify,
          target: 'node22',
          format: OutputFormat.CJS,
          externalModules: profile.externalSdk ? ['@aws-sdk/*'] : [],
        },
        ...extra,
      });
    };

    const ingestFn = fn('IngestFn', 'ingest.ts', { QUEUE_URL: queue.queueUrl });
    queue.grantSendMessages(ingestFn);

    const processorFn = fn(
      'ProcessorFn',
      'processor.ts',
      { TABLE_NAME: table.tableName, DLQ_URL: dlq.queueUrl, TOPIC_ARN: topic.topicArn },
      { timeout: PROCESSOR_TIMEOUT },
    );
    table.grantReadWriteData(processorFn);
    dlq.grantSendMessages(processorFn);
    topic.grantPublish(processorFn);
    processorFn.addEventSource(
      new SqsEventSource(queue, {
        batchSize: props.processorBatchSize ?? 10,
        maxBatchingWindow: (props.processorBatchSize ?? 10) > 1 ? Duration.seconds(5) : undefined,
        reportBatchItemFailures: true,
        // Caps concurrent processors (protects DynamoDB/SNS during spikes) without
        // consuming account-level reserved concurrency (new accounts may have only 10).
        maxConcurrency: props.processorMaxConcurrency ?? 2,
      }),
    );

    const subscriptionsFn = fn('SubscriptionsFn', 'subscriptions.ts', { TABLE_NAME: table.tableName });
    table.grantReadWriteData(subscriptionsFn);

    const feedFn = fn('FeedAdapterFn', 'feed-adapter.ts', { QUEUE_URL: queue.queueUrl, DLQ_URL: dlq.queueUrl });
    queue.grantSendMessages(feedFn);
    dlq.grantSendMessages(feedFn);

    // ---------- EventBridge Scheduler (legacy feed simulator) ----------
    new scheduler.Schedule(this, 'FeedSchedule', {
      schedule: scheduler.ScheduleExpression.rate(Duration.minutes(1)),
      target: new schedulerTargets.LambdaInvoke(feedFn, { retryAttempts: 2 }),
      enabled: props.feedScheduleEnabled ?? false,
      description: 'Invokes the legacy XML feed adapter every minute',
    });

    // ---------- HTTP API ----------
    const api = new HttpApi(this, 'HttpApi', {
      description: 'Flight Disruption Notifier API',
      createDefaultStage: false,
      corsPreflight: { allowOrigins: ['*'], allowMethods: [CorsHttpMethod.GET, CorsHttpMethod.POST, CorsHttpMethod.DELETE], allowHeaders: ['content-type', 'idempotency-key'] },
    });
    const stage = new HttpStage(this, 'DefaultStage', {
      httpApi: api,
      stageName: '$default',
      autoDeploy: true,
      throttle: { rateLimit: props.apiRateLimit ?? 20, burstLimit: props.apiBurstLimit ?? 50 }, // cost guard-rail
    });
    const ingestInt = new HttpLambdaIntegration('IngestInt', ingestFn);
    const subsInt = new HttpLambdaIntegration('SubsInt', subscriptionsFn);
    api.addRoutes({ path: '/flight-events', methods: [HttpMethod.POST], integration: ingestInt });
    api.addRoutes({ path: '/subscriptions', methods: [HttpMethod.POST], integration: subsInt });
    api.addRoutes({ path: '/subscriptions/{flightNumber}/{flightDate}/{userId}', methods: [HttpMethod.DELETE], integration: subsInt });
    api.addRoutes({ path: '/users/{userId}/subscriptions', methods: [HttpMethod.GET], integration: subsInt });
    api.addRoutes({ path: '/flights/{flightNumber}/{flightDate}', methods: [HttpMethod.GET], integration: subsInt });

    // ---------- Monitoring ----------
    const alarmTopic = new sns.Topic(this, 'AlarmTopic', { displayName: 'Flight Disruption Notifier alarms' });
    if (props.alarmEmail) alarmTopic.addSubscription(new subs.EmailSubscription(props.alarmEmail));
    const alarmAction = new cwActions.SnsAction(alarmTopic);

    const dlqVisible = dlq.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(1), statistic: 'Maximum' });
    const oldestAge = queue.metricApproximateAgeOfOldestMessage({ period: Duration.minutes(1), statistic: 'Maximum' });
    const procErrors = processorFn.metricErrors({ period: Duration.minutes(5), statistic: 'Sum' });
    const procInvocations = processorFn.metricInvocations({ period: Duration.minutes(5), statistic: 'Sum' });
    const errorRate = new cloudwatch.MathExpression({
      expression: 'IF(inv > 0, 100 * err / inv, 0)',
      usingMetrics: { err: procErrors, inv: procInvocations },
      period: Duration.minutes(5),
      label: 'Processor error rate (%)',
    });

    const alarms = [
      new cloudwatch.Alarm(this, 'DlqNotEmptyAlarm', {
        metric: dlqVisible, threshold: 0, evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        alarmDescription: 'Messages are sitting in the flight-events dead-letter queue',
      }),
      new cloudwatch.Alarm(this, 'QueueBacklogAlarm', {
        metric: oldestAge, threshold: 300, evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        alarmDescription: 'Oldest flight event has waited more than 5 minutes',
      }),
      new cloudwatch.Alarm(this, 'ProcessorErrorRateAlarm', {
        metric: errorRate, threshold: 1, evaluationPeriods: 1,
        comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
        alarmDescription: 'Processor Lambda error rate above 1%',
      }),
    ];
    alarms.forEach((a) => a.addAlarmAction(alarmAction));

    const lambdas = [ingestFn, processorFn, subscriptionsFn, feedFn];
    const dashboard = new cloudwatch.Dashboard(this, 'Dashboard', { dashboardName: `${this.stackName}-overview` });
    dashboard.addWidgets(
      new cloudwatch.GraphWidget({ title: 'Queue depth', left: [queue.metricApproximateNumberOfMessagesVisible(), queue.metricApproximateNumberOfMessagesNotVisible()], width: 8 }),
      new cloudwatch.GraphWidget({ title: 'Age of oldest message (s)', left: [oldestAge], width: 8 }),
      new cloudwatch.GraphWidget({ title: 'DLQ messages', left: [dlqVisible], width: 8 }),
    );
    dashboard.addWidgets(
      new cloudwatch.GraphWidget({ title: 'Lambda errors', left: lambdas.map((f) => f.metricErrors()), width: 8 }),
      new cloudwatch.GraphWidget({ title: 'Lambda throttles', left: lambdas.map((f) => f.metricThrottles()), width: 8 }),
      new cloudwatch.GraphWidget({ title: 'Lambda duration p95 (ms)', left: lambdas.map((f) => f.metricDuration({ statistic: 'p95' })), width: 8 }),
    );
    dashboard.addWidgets(
      new cloudwatch.LogQueryWidget({
        title: 'Cold starts (Init Duration)',
        logGroupNames: lambdas.map((f) => f.logGroup.logGroupName),
        queryLines: [
          'filter @type = "REPORT" and ispresent(@initDuration)',
          'stats count() as coldStarts, avg(@initDuration) as avgInit, pct(@initDuration, 95) as p95Init by bin(1h)',
        ],
        width: 24,
      }),
    );

    // ---------- Outputs ----------
    new CfnOutput(this, 'ApiUrl', { value: stage.url });
    new CfnOutput(this, 'TableName', { value: table.tableName });
    new CfnOutput(this, 'QueueUrl', { value: queue.queueUrl });
    new CfnOutput(this, 'QueueArn', { value: queue.queueArn });
    new CfnOutput(this, 'DlqUrl', { value: dlq.queueUrl });
    new CfnOutput(this, 'DlqArn', { value: dlq.queueArn });
    new CfnOutput(this, 'OutboxQueueUrl', { value: outbox.queueUrl });
    new CfnOutput(this, 'TopicArn', { value: topic.topicArn });
    new CfnOutput(this, 'FeedAdapterFunctionName', { value: feedFn.functionName });
    new CfnOutput(this, 'ProcessorFunctionName', { value: processorFn.functionName });
    new CfnOutput(this, 'IngestFunctionName', { value: ingestFn.functionName });
    new CfnOutput(this, 'SubscriptionsFunctionName', { value: subscriptionsFn.functionName });
    new CfnOutput(this, 'ExperimentConfig', {
      value: JSON.stringify({ coldStartProfile: profileName, processorBatchSize: props.processorBatchSize ?? 10, billingMode: props.billingMode ?? 'PROVISIONED', apiRateLimit: props.apiRateLimit ?? 20 }),
    });
    new CfnOutput(this, 'DashboardName', { value: dashboard.dashboardName });
  }
}
