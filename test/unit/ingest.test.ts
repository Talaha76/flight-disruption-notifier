import { mockClient } from 'aws-sdk-client-mock';
import { SendMessageBatchCommand, SQSClient } from '@aws-sdk/client-sqs';
import { handler } from '../../src/handlers/ingest';
import { delayEvent, gateEvent, httpEvent } from '../helpers/fixtures';

const sqsMock = mockClient(SQSClient);
beforeEach(() => sqsMock.reset());

const post = (body: unknown) => handler(httpEvent('POST /flight-events', { body }));

describe('ingest handler', () => {
  it('enqueues a single event and fills in eventId and source', async () => {
    sqsMock.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });
    const { eventId: _e, source: _s, ...input } = delayEvent();
    const res = await post(input);
    expect(res.statusCode).toBe(202);
    const accepted = JSON.parse(res.body).accepted;
    expect(accepted).toHaveLength(1);
    const call = sqsMock.commandCalls(SendMessageBatchCommand)[0].args[0].input;
    expect(call.QueueUrl).toBe(process.env.QUEUE_URL);
    const body = JSON.parse(call.Entries![0].MessageBody!);
    expect(body.source).toBe('api');
    expect(body.eventId).toBe(accepted[0]);
    expect(call.Entries![0].MessageAttributes!.eventId.StringValue).toBe(accepted[0]);
  });

  it('sends up to 10 events in one SendMessageBatch call', async () => {
    sqsMock.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] });
    const events = Array.from({ length: 10 }, (_, i) => delayEvent({ eventId: `e${i}` }));
    const res = await post({ events });
    expect(res.statusCode).toBe(202);
    expect(sqsMock.commandCalls(SendMessageBatchCommand)).toHaveLength(1);
    expect(sqsMock.commandCalls(SendMessageBatchCommand)[0].args[0].input.Entries).toHaveLength(10);
  });

  it('returns 400 for invalid JSON, invalid events and duplicate ids', async () => {
    expect((await handler(httpEvent('POST /flight-events', { body: '{not json' }))).statusCode).toBe(400);
    const bad = await post(delayEvent({ delayMinutes: -5 }));
    expect(bad.statusCode).toBe(400);
    expect(JSON.parse(bad.body).issues.length).toBeGreaterThan(0);
    expect((await post([delayEvent(), delayEvent()])).statusCode).toBe(400);
    expect(sqsMock.commandCalls(SendMessageBatchCommand)).toHaveLength(0);
  });

  it('returns 207 on partial enqueue failure and 502 on total failure', async () => {
    sqsMock.on(SendMessageBatchCommand).resolves({ Successful: [{ Id: '0', MessageId: 'm', MD5OfMessageBody: 'x' }], Failed: [{ Id: '1', Code: 'InternalError', SenderFault: false }] });
    const res = await post([delayEvent(), gateEvent()]);
    expect(res.statusCode).toBe(207);
    expect(JSON.parse(res.body)).toEqual({ accepted: ['evt-1'], failed: [{ eventId: 'evt-gate', code: 'InternalError', message: undefined }] });

    sqsMock.reset();
    sqsMock.on(SendMessageBatchCommand).rejects(new Error('boom'));
    expect((await post(delayEvent())).statusCode).toBe(502);
  });
});
