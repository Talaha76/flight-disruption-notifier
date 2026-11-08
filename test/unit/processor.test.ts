import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocumentClient, BatchGetCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { PublishBatchCommand, SNSClient } from '@aws-sdk/client-sns';
import { SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { handler } from '../../src/handlers/processor';
import { FakeTable } from '../helpers/fake-table';
import { delayEvent, gateEvent, sqsEvent, sqsRecord } from '../helpers/fixtures';

const ddbMock = mockClient(DynamoDBDocumentClient);
const snsMock = mockClient(SNSClient);
const sqsMock = mockClient(SQSClient);
let table: FakeTable;

const subscribe = (userId: string, flightNumber = 'AC123', flightDate = '2026-11-20') =>
  table.put({ PK: `FLIGHT#${flightNumber}#${flightDate}`, SK: `SUB#${userId}`, userId, channel: 'test', contact: `${userId}@example.com`, flightNumber, flightDate });

const published = () => snsMock.commandCalls(PublishBatchCommand).flatMap((c) => c.args[0].input.PublishBatchRequestEntries!);

beforeEach(() => {
  ddbMock.reset(); snsMock.reset(); sqsMock.reset();
  table = new FakeTable();
  table.wire(ddbMock);
  snsMock.on(PublishBatchCommand).callsFake((input) => ({ Successful: input.PublishBatchRequestEntries.map((e: { Id: string }) => ({ Id: e.Id, MessageId: 'm' })), Failed: [] }));
  sqsMock.on(SendMessageCommand).resolves({ MessageId: 'dlq-1' });
});

describe('processor handler', () => {
  it('updates status, notifies every subscriber and marks the event COMPLETED', async () => {
    subscribe('u1'); subscribe('u2'); subscribe('u3', 'AC999');
    const res = await handler(sqsEvent(sqsRecord(delayEvent())));
    expect(res.batchItemFailures).toEqual([]);
    const entries = published();
    expect(entries).toHaveLength(2);
    const payload = JSON.parse(entries[0].Message!);
    expect(payload).toMatchObject({ eventId: 'evt-1', type: 'DELAY', channel: 'test', enqueuedAt: new Date(1795176000000).toISOString() });
    expect(Date.parse(payload.sentAt)).toBeGreaterThan(0);
    expect(payload.text).toContain('delayed by 95 minutes');
    expect(entries[0].MessageAttributes!.channel.StringValue).toBe('test');
    expect(table.get('FLIGHT#AC123#2026-11-20', 'STATUS')).toMatchObject({ status: 'DELAYED', delayMinutes: 95, lastEventId: 'evt-1' });
    expect(table.get('IDEMP#evt-1', 'IDEMP')).toMatchObject({ status: 'COMPLETED' });
    expect(JSON.parse(table.get('IDEMP#evt-1', 'IDEMP')!.result as string)).toEqual({ outcome: 'NOTIFIED', notified: 2 });
  });

  it('sends exactly 1 notification for 20 deliveries of the same eventId (same batch + later batches)', async () => {
    subscribe('u1');
    const batch1 = Array.from({ length: 10 }, () => sqsRecord(delayEvent()));
    const r1 = await handler(sqsEvent(...batch1));
    expect(r1.batchItemFailures).toEqual([]);
    for (let i = 0; i < 10; i++) {
      const r = await handler(sqsEvent(sqsRecord(delayEvent())));
      expect(r.batchItemFailures).toEqual([]);
    }
    expect(published()).toHaveLength(1);
  });

  it('routes poison messages to the DLQ without blocking the rest of the batch', async () => {
    subscribe('u1');
    const good = sqsRecord(delayEvent());
    const notJson = sqsRecord('{{{');
    const badVersion = sqsRecord(delayEvent({ eventId: 'evt-v2', version: 2 }));
    const res = await handler(sqsEvent(good, notJson, badVersion));
    expect(res.batchItemFailures).toEqual([]);
    expect(published()).toHaveLength(1);
    const dlqCalls = sqsMock.commandCalls(SendMessageCommand).map((c) => c.args[0].input);
    expect(dlqCalls).toHaveLength(2);
    expect(dlqCalls[0].QueueUrl).toBe(process.env.DLQ_URL);
    expect(dlqCalls[0].MessageAttributes!.errorReason.StringValue).toMatch(/not valid JSON/);
    expect(dlqCalls[1].MessageAttributes!.errorReason.StringValue).toMatch(/Schema validation failed/);
  });

  it('reports a poison message as a batch failure if the DLQ send itself fails', async () => {
    sqsMock.on(SendMessageCommand).rejects(new Error('sqs down'));
    const bad = sqsRecord('nope', 'bad-msg');
    const res = await handler(sqsEvent(bad));
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'bad-msg' }]);
  });

  it('reports only the failed message when SNS fails, and releases its idempotency lock', async () => {
    subscribe('u1'); subscribe('u9', 'AC777');
    snsMock.on(PublishBatchCommand).callsFake((input) => {
      if (JSON.parse(input.PublishBatchRequestEntries[0].Message).flightNumber === 'AC777') throw new Error('sns down');
      return { Successful: input.PublishBatchRequestEntries.map((e: { Id: string }) => ({ Id: e.Id })), Failed: [] };
    });
    const ok = sqsRecord(delayEvent(), 'ok-msg');
    const fail = sqsRecord(delayEvent({ eventId: 'evt-777', flightNumber: 'AC777' }), 'fail-msg');
    const res = await handler(sqsEvent(ok, fail));
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'fail-msg' }]);
    expect(table.get('IDEMP#evt-1', 'IDEMP')!.status).toBe('COMPLETED');
    expect(table.get('IDEMP#evt-777', 'IDEMP')).toBeUndefined();

    // retry succeeds and still notifies (status was already written by the same eventId)
    snsMock.on(PublishBatchCommand).callsFake((input) => ({ Successful: input.PublishBatchRequestEntries.map((e: { Id: string }) => ({ Id: e.Id })), Failed: [] }));
    const retry = await handler(sqsEvent(sqsRecord(delayEvent({ eventId: 'evt-777', flightNumber: 'AC777' }))));
    expect(retry.batchItemFailures).toEqual([]);
    expect(JSON.parse(table.get('IDEMP#evt-777', 'IDEMP')!.result as string)).toEqual({ outcome: 'NOTIFIED', notified: 1 });
  });

  it('retries SNS entries that fail transiently, fails the record on sender faults', async () => {
    subscribe('u1');
    let calls = 0;
    snsMock.on(PublishBatchCommand).callsFake((input) => {
      calls++;
      const id = input.PublishBatchRequestEntries[0].Id;
      return calls === 1 ? { Successful: [], Failed: [{ Id: id, Code: 'Throttled', SenderFault: false }] } : { Successful: [{ Id: id }], Failed: [] };
    });
    expect((await handler(sqsEvent(sqsRecord(delayEvent())))).batchItemFailures).toEqual([]);
    expect(calls).toBe(2);

    snsMock.on(PublishBatchCommand).callsFake((input) => ({ Successful: [], Failed: [{ Id: input.PublishBatchRequestEntries[0].Id, Code: 'InvalidParameter', SenderFault: true }] }));
    const res = await handler(sqsEvent(sqsRecord(delayEvent({ eventId: 'evt-2', occurredAt: '2026-11-20T11:00:00Z' }), 'm2')));
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'm2' }]);
  });

  it('skips notifications for out-of-order (stale) events but still completes them', async () => {
    subscribe('u1');
    await handler(sqsEvent(sqsRecord(gateEvent({ occurredAt: '2026-11-20T10:30:00Z' }))));
    const res = await handler(sqsEvent(sqsRecord(delayEvent({ occurredAt: '2026-11-20T10:00:00Z' }))));
    expect(res.batchItemFailures).toEqual([]);
    expect(published()).toHaveLength(1);
    expect(JSON.parse(table.get('IDEMP#evt-1', 'IDEMP')!.result as string)).toEqual({ outcome: 'STALE', notified: 0 });
    expect(table.get('FLIGHT#AC123#2026-11-20', 'STATUS')).toMatchObject({ status: 'SCHEDULED', gate: 'C32' });
  });

  it('applies events within a batch in occurredAt order', async () => {
    subscribe('u1');
    const later = sqsRecord(gateEvent({ occurredAt: '2026-11-20T10:30:00Z' }));
    const earlier = sqsRecord(delayEvent({ occurredAt: '2026-11-20T10:00:00Z' }));
    await handler(sqsEvent(later, earlier));
    expect(published()).toHaveLength(2);
    expect(table.get('FLIGHT#AC123#2026-11-20', 'STATUS')).toMatchObject({ status: 'DELAYED', gate: 'C32', lastEventId: 'evt-gate' });
  });

  it('fails messages whose event is in progress in another invocation', async () => {
    table.put({ PK: 'IDEMP#evt-1', SK: 'IDEMP', status: 'IN_PROGRESS', lockExpiresAt: Math.floor(Date.now() / 1000) + 60 });
    const res = await handler(sqsEvent(sqsRecord(delayEvent(), 'a'), sqsRecord(delayEvent(), 'b')));
    expect(res.batchItemFailures.map((f) => f.itemIdentifier).sort()).toEqual(['a', 'b']);
    expect(published()).toHaveLength(0);
  });

  it('takes over an expired in-progress lock (crashed earlier attempt)', async () => {
    subscribe('u1');
    table.put({ PK: 'IDEMP#evt-1', SK: 'IDEMP', status: 'IN_PROGRESS', lockExpiresAt: Math.floor(Date.now() / 1000) - 1 });
    const res = await handler(sqsEvent(sqsRecord(delayEvent())));
    expect(res.batchItemFailures).toEqual([]);
    expect(published()).toHaveLength(1);
  });

  it('retries BatchGetItem UnprocessedKeys', async () => {
    subscribe('u1');
    let n = 0;
    ddbMock.on(BatchGetCommand).callsFake((input) => {
      n++;
      const [t, req] = Object.entries(input.RequestItems as Record<string, { Keys: unknown[] }>)[0];
      return n === 1 ? { Responses: { [t]: [] }, UnprocessedKeys: { [t]: { Keys: req.Keys } } } : { Responses: { [t]: [] }, UnprocessedKeys: {} };
    });
    const res = await handler(sqsEvent(sqsRecord(delayEvent())));
    expect(res.batchItemFailures).toEqual([]);
    expect(n).toBe(2);
  });

  it('fails the whole batch (and releases locks) when status lookup fails', async () => {
    ddbMock.on(BatchGetCommand).rejects(new Error('ddb down'));
    const res = await handler(sqsEvent(sqsRecord(delayEvent(), 'x')));
    expect(res.batchItemFailures).toEqual([{ itemIdentifier: 'x' }]);
    expect(table.get('IDEMP#evt-1', 'IDEMP')).toBeUndefined();
  });

  it('completes without publishing when a flight has no subscribers', async () => {
    const res = await handler(sqsEvent(sqsRecord(delayEvent())));
    expect(res.batchItemFailures).toEqual([]);
    expect(snsMock.commandCalls(PublishBatchCommand)).toHaveLength(0);
    expect(ddbMock.commandCalls(UpdateCommand).length).toBe(2); // status + complete
  });

  it('publishes in chunks of 10 for large subscriber lists', async () => {
    for (let i = 0; i < 23; i++) subscribe(`u${i}`);
    await handler(sqsEvent(sqsRecord(delayEvent())));
    const calls = snsMock.commandCalls(PublishBatchCommand);
    expect(calls.map((c) => c.args[0].input.PublishBatchRequestEntries!.length)).toEqual([10, 10, 3]);
  });
});
