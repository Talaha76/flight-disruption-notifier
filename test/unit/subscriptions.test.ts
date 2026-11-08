import { mockClient } from 'aws-sdk-client-mock';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { handler } from '../../src/handlers/subscriptions';
import { FakeTable } from '../helpers/fake-table';
import { httpEvent } from '../helpers/fixtures';

const ddbMock = mockClient(DynamoDBDocumentClient);
let table: FakeTable;
beforeEach(() => { ddbMock.reset(); table = new FakeTable(); table.wire(ddbMock); });

const sub = { userId: 'u1', flightNumber: 'AC123', flightDate: '2026-11-20', channel: 'email', contact: 'talaha@example.com' };
const create = (body: unknown, headers: Record<string, string> = {}) => handler(httpEvent('POST /subscriptions', { body, headers }));

describe('subscriptions handler', () => {
  it('creates a subscription with flight + user (GSI1) keys', async () => {
    const res = await create(sub);
    expect(res.statusCode).toBe(201);
    expect(JSON.parse(res.body)).toMatchObject(sub);
    expect(table.get('FLIGHT#AC123#2026-11-20', 'SUB#u1')).toMatchObject({ GSI1PK: 'USER#u1', GSI1SK: 'FLIGHT#AC123#2026-11-20', channel: 'email' });
  });

  it('rejects invalid bodies', async () => {
    expect((await create({ ...sub, contact: 'not-an-email' })).statusCode).toBe(400);
    expect((await handler(httpEvent('POST /subscriptions', { body: 'oops' }))).statusCode).toBe(400);
    expect((await create(sub, { 'Idempotency-Key': 'short' })).statusCode).toBe(400);
  });

  it('replays the stored response for a repeated Idempotency-Key', async () => {
    const h = { 'Idempotency-Key': 'key-12345678' };
    const first = await create(sub, h);
    const second = await create(sub, { 'idempotency-key': 'key-12345678' });
    expect(second.statusCode).toBe(201);
    expect(second.body).toBe(first.body);
    expect(second.headers['idempotent-replayed']).toBe('true');
    expect(ddbMock.commandCalls(PutCommand).filter((c) => String(c.args[0].input.Item!.SK).startsWith('SUB#'))).toHaveLength(1);
  });

  it('returns 422 when an Idempotency-Key is reused with a different body', async () => {
    const h = { 'Idempotency-Key': 'key-12345678' };
    await create(sub, h);
    const res = await create({ ...sub, userId: 'someone-else' }, h);
    expect(res.statusCode).toBe(422);
  });

  it('lists a user subscriptions via GSI1 and deletes one', async () => {
    await create(sub);
    await create({ ...sub, flightNumber: 'AC456' });
    const list = await handler(httpEvent('GET /users/{userId}/subscriptions', { pathParameters: { userId: 'u1' } }));
    expect(list.statusCode).toBe(200);
    expect(JSON.parse(list.body).subscriptions).toHaveLength(2);

    const del = await handler(httpEvent('DELETE /subscriptions/{flightNumber}/{flightDate}/{userId}', { pathParameters: { flightNumber: 'AC123', flightDate: '2026-11-20', userId: 'u1' } }));
    expect(del.statusCode).toBe(204);
    const again = await handler(httpEvent('DELETE /subscriptions/{flightNumber}/{flightDate}/{userId}', { pathParameters: { flightNumber: 'AC123', flightDate: '2026-11-20', userId: 'u1' } }));
    expect(again.statusCode).toBe(404);
  });

  it('returns flight status, 404 when unknown, 400 on bad params', async () => {
    table.put({ PK: 'FLIGHT#AC123#2026-11-20', SK: 'STATUS', status: 'DELAYED', delayMinutes: 30, flightNumber: 'AC123', flightDate: '2026-11-20' });
    const ok = await handler(httpEvent('GET /flights/{flightNumber}/{flightDate}', { pathParameters: { flightNumber: 'AC123', flightDate: '2026-11-20' } }));
    expect(ok.statusCode).toBe(200);
    expect(JSON.parse(ok.body)).toEqual({ status: 'DELAYED', delayMinutes: 30, flightNumber: 'AC123', flightDate: '2026-11-20' });
    expect((await handler(httpEvent('GET /flights/{flightNumber}/{flightDate}', { pathParameters: { flightNumber: 'AC999', flightDate: '2026-11-20' } }))).statusCode).toBe(404);
    expect((await handler(httpEvent('GET /flights/{flightNumber}/{flightDate}', { pathParameters: { flightNumber: 'x', flightDate: 'y' } }))).statusCode).toBe(400);
  });

  it('returns 500 (not a crash) when DynamoDB fails', async () => {
    ddbMock.on(PutCommand).rejects(new Error('ddb down'));
    expect((await create(sub)).statusCode).toBe(500);
  });
});
