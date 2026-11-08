import type { APIGatewayProxyEventV2, SQSEvent, SQSRecord } from 'aws-lambda';

export const delayEvent = (over: Record<string, unknown> = {}) => ({
  eventId: 'evt-1',
  source: 'api',
  version: 1,
  type: 'DELAY',
  flightNumber: 'AC123',
  flightDate: '2026-11-20',
  occurredAt: '2026-11-20T10:00:00Z',
  delayMinutes: 95,
  reason: 'Weather',
  ...over,
});

export const gateEvent = (over: Record<string, unknown> = {}) => ({
  eventId: 'evt-gate',
  source: 'api',
  version: 1,
  type: 'GATE_CHANGE',
  flightNumber: 'AC123',
  flightDate: '2026-11-20',
  occurredAt: '2026-11-20T10:05:00Z',
  newGate: 'C32',
  oldGate: 'C30',
  ...over,
});

let seq = 0;
export function sqsRecord(body: unknown, messageId = `msg-${++seq}`): SQSRecord {
  return {
    messageId,
    receiptHandle: 'rh',
    body: typeof body === 'string' ? body : JSON.stringify(body),
    attributes: { ApproximateReceiveCount: '1', SentTimestamp: '1795176000000', SenderId: 'x', ApproximateFirstReceiveTimestamp: '0' },
    messageAttributes: {},
    md5OfBody: 'x',
    eventSource: 'aws:sqs',
    eventSourceARN: 'arn:aws:sqs:ca-central-1:123456789012:flight-events',
    awsRegion: 'ca-central-1',
  };
}

export const sqsEvent = (...records: SQSRecord[]): SQSEvent => ({ Records: records });

export function httpEvent(routeKey: string, opts: { body?: unknown; headers?: Record<string, string>; pathParameters?: Record<string, string> } = {}): APIGatewayProxyEventV2 {
  const [method, rawPath] = routeKey.split(' ');
  return {
    version: '2.0',
    routeKey,
    rawPath,
    rawQueryString: '',
    headers: opts.headers ?? {},
    pathParameters: opts.pathParameters,
    isBase64Encoded: false,
    body: opts.body === undefined ? undefined : typeof opts.body === 'string' ? opts.body : JSON.stringify(opts.body),
    requestContext: {
      accountId: '123456789012', apiId: 'api', domainName: 'x', domainPrefix: 'x', requestId: 'req-1', routeKey, stage: '$default', time: '', timeEpoch: 0,
      http: { method, path: rawPath, protocol: 'HTTP/1.1', sourceIp: '1.1.1.1', userAgent: 'jest' },
    },
  } as APIGatewayProxyEventV2;
}

export const ccfe = (item?: Record<string, unknown>) => Object.assign(new Error('The conditional request failed'), { name: 'ConditionalCheckFailedException', Item: item, $metadata: {} });
