import { BatchGetCommand, DeleteCommand, GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ddb } from './clients';
import { config } from './config';
import { keys } from './keys';
import type { FlightEvent } from '../schemas/flight-event';
import type { Subscriber } from './notifications';
import type { SubscriptionInput } from '../schemas/subscription';

export interface FlightStatus {
  flightNumber: string;
  flightDate: string;
  status: string;
  gate?: string;
  delayMinutes?: number;
  lastEventAt: string;
  lastEventId: string;
  updatedAt: string;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Loads STATUS items for many flights with one BatchGetItem call (100 keys max per call),
 * retrying UnprocessedKeys with exponential backoff.
 */
export async function batchGetStatuses(flights: { flightNumber: string; flightDate: string }[], maxAttempts = 5): Promise<Map<string, FlightStatus>> {
  const table = config.tableName();
  const out = new Map<string, FlightStatus>();
  const unique = [...new Map(flights.map((f) => [keys.flightPk(f.flightNumber, f.flightDate), f])).keys()];
  for (let i = 0; i < unique.length; i += 100) {
    let pending: Record<string, unknown>[] | undefined = unique.slice(i, i + 100).map((pk) => ({ PK: pk, SK: keys.statusSk() }));
    for (let attempt = 0; pending && pending.length > 0; attempt++) {
      if (attempt >= maxAttempts) throw new Error(`BatchGetItem left ${pending.length} unprocessed keys after ${maxAttempts} attempts`);
      if (attempt > 0) await sleep(Math.min(1000, 50 * 2 ** attempt) + Math.floor(Math.random() * 50));
      const res = await ddb.send(new BatchGetCommand({ RequestItems: { [table]: { Keys: pending } } }));
      for (const item of res.Responses?.[table] ?? []) out.set(item.PK as string, item as unknown as FlightStatus);
      pending = res.UnprocessedKeys?.[table]?.Keys as Record<string, unknown>[] | undefined;
    }
  }
  return out;
}

/** Returns true when the event is newer than (or the same event as) the stored status. */
export function isNewer(current: FlightStatus | undefined, e: FlightEvent): boolean {
  if (!current) return true;
  if (current.lastEventId === e.eventId) return true; // retry of the same event after a partial failure
  return new Date(e.occurredAt).getTime() > new Date(current.lastEventAt).getTime();
}

/**
 * Applies the event to the flight's STATUS item. The condition rejects out-of-order (older) events,
 * but lets a retry of the same eventId through. Returns the new item, or null when stale.
 */
export async function applyStatus(e: FlightEvent): Promise<FlightStatus | null> {
  const names: Record<string, string> = { '#s': 'status' };
  const values: Record<string, unknown> = {
    ':ts': e.occurredAt,
    ':eid': e.eventId,
    ':now': new Date().toISOString(),
    ':fn': e.flightNumber,
    ':fd': e.flightDate,
  };
  const sets = ['lastEventAt = :ts', 'lastEventId = :eid', 'updatedAt = :now', 'flightNumber = :fn', 'flightDate = :fd'];
  switch (e.type) {
    case 'DELAY':
      sets.push('#s = :status', 'delayMinutes = :delay');
      values[':status'] = 'DELAYED';
      values[':delay'] = e.delayMinutes;
      if (e.newDepartureTime) {
        sets.push('newDepartureTime = :ndt');
        values[':ndt'] = e.newDepartureTime;
      }
      break;
    case 'CANCELLATION':
      sets.push('#s = :status');
      values[':status'] = 'CANCELLED';
      break;
    case 'GATE_CHANGE':
      sets.push('#s = if_not_exists(#s, :scheduled)', 'gate = :gate');
      values[':scheduled'] = 'SCHEDULED';
      values[':gate'] = e.newGate;
      break;
  }
  try {
    const res = await ddb.send(
      new UpdateCommand({
        TableName: config.tableName(),
        Key: { PK: keys.flightPk(e.flightNumber, e.flightDate), SK: keys.statusSk() },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ConditionExpression: 'attribute_not_exists(PK) OR lastEventAt < :ts OR lastEventId = :eid',
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        ReturnValues: 'ALL_NEW',
      }),
    );
    return res.Attributes as FlightStatus;
  } catch (err) {
    if ((err as { name?: string }).name === 'ConditionalCheckFailedException') return null;
    throw err;
  }
}

/** All subscribers of one flight (paginated Query on the flight partition, SK begins_with SUB#). */
export async function querySubscribers(flightNumber: string, flightDate: string): Promise<Subscriber[]> {
  const subs: Subscriber[] = [];
  let startKey: Record<string, unknown> | undefined;
  do {
    const res = await ddb.send(
      new QueryCommand({
        TableName: config.tableName(),
        KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sub)',
        ExpressionAttributeValues: { ':pk': keys.flightPk(flightNumber, flightDate), ':sub': 'SUB#' },
        ProjectionExpression: 'userId, channel, contact',
        ExclusiveStartKey: startKey,
      }),
    );
    for (const i of res.Items ?? []) subs.push({ userId: i.userId as string, channel: i.channel as string, contact: i.contact as string });
    startKey = res.LastEvaluatedKey;
  } while (startKey);
  return subs;
}

export async function getFlightStatus(flightNumber: string, flightDate: string): Promise<FlightStatus | undefined> {
  const res = await ddb.send(new GetCommand({ TableName: config.tableName(), Key: { PK: keys.flightPk(flightNumber, flightDate), SK: keys.statusSk() } }));
  return res.Item as FlightStatus | undefined;
}

export interface SubscriptionRecord extends SubscriptionInput {
  createdAt: string;
}

export async function putSubscription(input: SubscriptionInput): Promise<SubscriptionRecord> {
  const record: SubscriptionRecord = { ...input, createdAt: new Date().toISOString() };
  await ddb.send(
    new PutCommand({
      TableName: config.tableName(),
      Item: {
        PK: keys.flightPk(input.flightNumber, input.flightDate),
        SK: keys.subSk(input.userId),
        GSI1PK: keys.userPk(input.userId),
        GSI1SK: keys.userFlightSk(input.flightNumber, input.flightDate),
        entity: 'Subscription',
        ...record,
      },
    }),
  );
  return record;
}

export async function deleteSubscription(userId: string, flightNumber: string, flightDate: string): Promise<boolean> {
  const res = await ddb.send(
    new DeleteCommand({
      TableName: config.tableName(),
      Key: { PK: keys.flightPk(flightNumber, flightDate), SK: keys.subSk(userId) },
      ReturnValues: 'ALL_OLD',
    }),
  );
  return !!res.Attributes;
}

export async function listUserSubscriptions(userId: string): Promise<SubscriptionRecord[]> {
  const out: SubscriptionRecord[] = [];
  let startKey: Record<string, unknown> | undefined;
  do {
    const res = await ddb.send(
      new QueryCommand({
        TableName: config.tableName(),
        IndexName: 'GSI1',
        KeyConditionExpression: 'GSI1PK = :pk',
        ExpressionAttributeValues: { ':pk': keys.userPk(userId) },
        ExclusiveStartKey: startKey,
      }),
    );
    for (const i of res.Items ?? []) {
      out.push({ userId: i.userId, flightNumber: i.flightNumber, flightDate: i.flightDate, channel: i.channel, contact: i.contact, createdAt: i.createdAt } as SubscriptionRecord);
    }
    startKey = res.LastEvaluatedKey;
  } while (startKey);
  return out;
}

/** API Idempotency-Key storage for POST /subscriptions. */
export async function getApiIdempotency(key: string): Promise<{ requestHash: string; statusCode: number; body: string } | undefined> {
  const res = await ddb.send(new GetCommand({ TableName: config.tableName(), Key: { PK: keys.apiIdempPk(key), SK: keys.apiIdempSk() } }));
  return res.Item as { requestHash: string; statusCode: number; body: string } | undefined;
}

export async function saveApiIdempotency(key: string, requestHash: string, statusCode: number, body: string, ttl: number): Promise<void> {
  try {
    await ddb.send(
      new PutCommand({
        TableName: config.tableName(),
        Item: { PK: keys.apiIdempPk(key), SK: keys.apiIdempSk(), requestHash, statusCode, body, ttl },
        ConditionExpression: 'attribute_not_exists(PK)',
      }),
    );
  } catch (err) {
    if ((err as { name?: string }).name !== 'ConditionalCheckFailedException') throw err;
  }
}
