/**
 * Small in-memory stand-in for the single DynamoDB table, wired into aws-sdk-client-mock.
 * It implements just the condition semantics the handlers rely on, so tests exercise
 * real idempotency / ordering behaviour rather than canned responses.
 */
import type { AwsStub } from 'aws-sdk-client-mock';
import { BatchGetCommand, DeleteCommand, GetCommand, PutCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { ccfe } from './fixtures';

type Item = Record<string, unknown>;
const k = (pk: unknown, sk: unknown) => `${pk}|${sk}`;
const marshallAllOld = (item: Item) => Object.fromEntries(Object.entries(item).map(([a, v]) => [a, typeof v === 'number' ? { N: String(v) } : { S: String(v) }]));

export class FakeTable {
  items = new Map<string, Item>();
  put(item: Item) { this.items.set(k(item.PK, item.SK), { ...item }); }
  get(pk: string, sk: string) { return this.items.get(k(pk, sk)); }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  wire(mock: AwsStub<any, any, any>) {
    mock.on(PutCommand).callsFake((input) => {
      const item = input.Item as Item;
      const existing = this.get(item.PK as string, item.SK as string);
      if (String(item.PK).startsWith('IDEMP#') && existing) {
        const now = input.ExpressionAttributeValues[':now'];
        const takeover = existing.status === 'IN_PROGRESS' && (existing.lockExpiresAt as number) < now;
        if (!takeover) throw ccfe(marshallAllOld(existing));
      }
      if (input.ConditionExpression === 'attribute_not_exists(PK)' && existing) throw ccfe();
      this.put(item);
      return {};
    });
    mock.on(GetCommand).callsFake((input) => ({ Item: this.get(input.Key.PK, input.Key.SK) }));
    mock.on(DeleteCommand).callsFake((input) => {
      const existing = this.get(input.Key.PK, input.Key.SK);
      if (input.ConditionExpression && (!existing || existing.status !== input.ExpressionAttributeValues[':inprog'])) throw ccfe();
      this.items.delete(k(input.Key.PK, input.Key.SK));
      return { Attributes: existing };
    });
    mock.on(UpdateCommand).callsFake((input) => {
      const { PK, SK } = input.Key;
      const existing = this.get(PK, SK);
      const v = input.ExpressionAttributeValues;
      if (String(PK).startsWith('IDEMP#')) {
        this.put({ ...existing, PK, SK, status: 'COMPLETED', result: v[':result'] });
        return {};
      }
      // STATUS update
      if (existing && !(existing.lastEventAt as string < v[':ts'] || existing.lastEventId === v[':eid'])) throw ccfe();
      const next: Item = { ...existing, PK, SK, lastEventAt: v[':ts'], lastEventId: v[':eid'], updatedAt: v[':now'], flightNumber: v[':fn'], flightDate: v[':fd'] };
      if (v[':status']) next.status = v[':status'];
      if (v[':scheduled']) next.status = existing?.status ?? v[':scheduled'];
      if (v[':delay'] !== undefined) next.delayMinutes = v[':delay'];
      if (v[':gate']) next.gate = v[':gate'];
      this.put(next);
      return { Attributes: next };
    });
    mock.on(BatchGetCommand).callsFake((input) => {
      const [table, req] = Object.entries(input.RequestItems as Record<string, { Keys: Item[] }>)[0];
      const found = req.Keys.map((key) => this.get(key.PK as string, key.SK as string)).filter(Boolean);
      return { Responses: { [table]: found }, UnprocessedKeys: {} };
    });
    mock.on(QueryCommand).callsFake((input) => {
      const pk = input.ExpressionAttributeValues[':pk'];
      const prefix = input.ExpressionAttributeValues[':sub'];
      const items = [...this.items.values()].filter((i) =>
        input.IndexName === 'GSI1' ? i.GSI1PK === pk : i.PK === pk && String(i.SK).startsWith(prefix ?? ''),
      );
      return { Items: items };
    });
  }
}
