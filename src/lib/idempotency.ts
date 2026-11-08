/**
 * Hand-built idempotency for at-least-once SQS delivery.
 *
 *   acquire()  -> PutItem IDEMP#<eventId> status=IN_PROGRESS
 *                 Condition: attribute_not_exists(PK) OR (status=IN_PROGRESS AND lockExpiresAt < now)
 *   complete() -> status=COMPLETED + result
 *   release()  -> delete the IN_PROGRESS record so a retry can run again
 *
 * Records expire through DynamoDB TTL after 24 hours. The in-progress lock
 * (LOCK_SECONDS) is longer than the processor timeout but shorter than the SQS
 * visibility timeout, so a crashed attempt never blocks its own retry.
 */
import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import { DeleteCommand, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ddb } from './clients';
import { config } from './config';
import { IDEMPOTENCY_TTL_SECONDS, keys, nowEpochSeconds } from './keys';

export const LOCK_SECONDS = 120;

export type AcquireResult =
  | { state: 'ACQUIRED' }
  | { state: 'ALREADY_COMPLETED'; result?: unknown }
  | { state: 'IN_PROGRESS_ELSEWHERE' };

export async function acquire(eventId: string): Promise<AcquireResult> {
  const now = nowEpochSeconds();
  try {
    await ddb.send(
      new PutCommand({
        TableName: config.tableName(),
        Item: {
          PK: keys.idempPk(eventId),
          SK: keys.idempSk(),
          status: 'IN_PROGRESS',
          lockExpiresAt: now + LOCK_SECONDS,
          createdAt: new Date().toISOString(),
          ttl: now + IDEMPOTENCY_TTL_SECONDS,
        },
        ConditionExpression: 'attribute_not_exists(PK) OR (#s = :inprog AND lockExpiresAt < :now)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':inprog': 'IN_PROGRESS', ':now': now },
        ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
      }),
    );
    return { state: 'ACQUIRED' };
  } catch (err) {
    if (err instanceof ConditionalCheckFailedException || (err as { name?: string }).name === 'ConditionalCheckFailedException') {
      const item = (err as ConditionalCheckFailedException).Item as Record<string, { S?: string }> | undefined;
      const status = item?.status?.S;
      if (status === 'COMPLETED') {
        let result: unknown;
        try {
          result = item?.result?.S ? JSON.parse(item.result.S) : undefined;
        } catch {
          result = undefined;
        }
        return { state: 'ALREADY_COMPLETED', result };
      }
      // Status unknown (no ALL_OLD returned) is treated conservatively as in-progress -> retry later.
      return { state: 'IN_PROGRESS_ELSEWHERE' };
    }
    throw err;
  }
}

export async function complete(eventId: string, result: unknown): Promise<void> {
  await ddb.send(
    new UpdateCommand({
      TableName: config.tableName(),
      Key: { PK: keys.idempPk(eventId), SK: keys.idempSk() },
      UpdateExpression: 'SET #s = :done, #r = :result, completedAt = :at REMOVE lockExpiresAt',
      ExpressionAttributeNames: { '#s': 'status', '#r': 'result' },
      ExpressionAttributeValues: { ':done': 'COMPLETED', ':result': JSON.stringify(result ?? null), ':at': new Date().toISOString() },
    }),
  );
}

export async function release(eventId: string): Promise<void> {
  try {
    await ddb.send(
      new DeleteCommand({
        TableName: config.tableName(),
        Key: { PK: keys.idempPk(eventId), SK: keys.idempSk() },
        ConditionExpression: '#s = :inprog',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':inprog': 'IN_PROGRESS' },
      }),
    );
  } catch (err) {
    if ((err as { name?: string }).name === 'ConditionalCheckFailedException') return;
    throw err;
  }
}
