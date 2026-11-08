/**
 * SQS consumer for flight-events (batch size 10, batching window 5 s, ReportBatchItemFailures).
 *
 * Per batch:
 *  1. Parse + validate each record. Invalid/unknown-version records are poison: they are
 *     sent straight to the DLQ (with the reason) and NOT retried, so they never block the batch.
 *  2. Collapse duplicate eventIds inside the batch (leader/follower) and run the DynamoDB
 *     idempotency check for each leader. Already-completed events are skipped.
 *  3. Load current STATUS items for all flights in the batch with one BatchGetItem.
 *  4. In occurredAt order: conditionally update STATUS (stale events are recorded but not
 *     notified), Query subscribers (cached per flight within the batch), PublishBatch to SNS.
 *  5. Mark the idempotency record COMPLETED. On any error the record is released and only
 *     that message is reported in batchItemFailures, so SQS retries it alone.
 *     After maxReceiveCount (3) failed receives SQS moves it to the DLQ.
 */
import type { SQSBatchItemFailure, SQSBatchResponse, SQSEvent, SQSRecord } from 'aws-lambda';
import { FlightEvent, formatIssues } from '../schemas/flight-event';
import { config } from '../lib/config';
import { acquire, complete, release } from '../lib/idempotency';
import { keys } from '../lib/keys';
import { logger } from '../lib/logger';
import { publishNotifications, type Subscriber } from '../lib/notifications';
import { sendToDlq } from '../lib/queue';
import { applyStatus, batchGetStatuses, isNewer, querySubscribers, type FlightStatus } from '../lib/repo';

export interface ProcessResult {
  outcome: 'NOTIFIED' | 'STALE';
  notified: number;
}

interface Parsed {
  record: SQSRecord;
  event: FlightEvent;
}

export const handler = async (sqsEvent: SQSEvent): Promise<SQSBatchResponse> => {
  const failures = new Set<string>();
  const valid: Parsed[] = [];

  // 1. Validate
  for (const record of sqsEvent.Records) {
    let body: unknown;
    let reason: string | undefined;
    try {
      body = JSON.parse(record.body);
    } catch {
      reason = 'Body is not valid JSON';
    }
    if (!reason) {
      const parsed = FlightEvent.safeParse(body);
      if (parsed.success) {
        valid.push({ record, event: parsed.data });
        continue;
      }
      reason = `Schema validation failed: ${JSON.stringify(formatIssues(parsed.error))}`;
    }
    logger.warn('poison message routed to DLQ', { messageId: record.messageId, reason });
    try {
      await sendToDlq(config.dlqUrl(), record.body, reason!, 'processor');
    } catch (err) {
      logger.error('failed to route poison message to DLQ; will retry via redrive', { messageId: record.messageId, err });
      failures.add(record.messageId);
    }
  }

  // 2. Idempotency (leader per eventId, followers inherit the leader's fate)
  const groups = new Map<string, Parsed[]>();
  for (const p of valid) {
    const g = groups.get(p.event.eventId);
    if (g) g.push(p);
    else groups.set(p.event.eventId, [p]);
  }
  const failGroup = (eventId: string) => groups.get(eventId)!.forEach((p) => failures.add(p.record.messageId));

  const toProcess: Parsed[] = [];
  for (const [eventId, members] of groups) {
    const log = logger.child({ eventId, flightId: `${members[0].event.flightNumber}#${members[0].event.flightDate}` });
    if (members.length > 1) log.info('duplicate deliveries collapsed within batch', { count: members.length });
    try {
      const r = await acquire(eventId);
      if (r.state === 'ACQUIRED') toProcess.push(members[0]);
      else if (r.state === 'ALREADY_COMPLETED') log.info('duplicate event skipped (already completed)', { result: r.result });
      else {
        log.warn('event in progress elsewhere; will retry');
        failGroup(eventId);
      }
    } catch (err) {
      log.error('idempotency check failed', { err });
      failGroup(eventId);
    }
  }

  if (toProcess.length === 0) return toResponse(failures);

  // 3. Current status for every flight in the batch (one BatchGetItem)
  let statuses: Map<string, FlightStatus>;
  try {
    statuses = await batchGetStatuses(toProcess.map((p) => p.event));
  } catch (err) {
    logger.error('BatchGetItem for flight statuses failed; failing batch items', { err });
    for (const p of toProcess) {
      await release(p.event.eventId).catch(() => undefined);
      failGroup(p.event.eventId);
    }
    return toResponse(failures);
  }

  // 4. Process oldest first so status ends at the latest event
  toProcess.sort((a, b) => new Date(a.event.occurredAt).getTime() - new Date(b.event.occurredAt).getTime());
  const subscriberCache = new Map<string, Subscriber[]>();

  for (const { event, record } of toProcess) {
    const pk = keys.flightPk(event.flightNumber, event.flightDate);
    const log = logger.child({ eventId: event.eventId, flightId: `${event.flightNumber}#${event.flightDate}`, type: event.type, source: event.source });
    try {
      const result = await processEvent(event, statuses, subscriberCache, enqueuedAt(record));
      await complete(event.eventId, result);
      log.info('event processed', { ...result });
    } catch (err) {
      log.error('event processing failed; will retry', { err });
      statuses.delete(pk); // force a fresh read on retry paths within this batch
      await release(event.eventId).catch((e) => log.error('failed to release idempotency lock', { err: e }));
      failGroup(event.eventId);
    }
  }

  return toResponse(failures);
};

function enqueuedAt(record: SQSRecord): string | undefined {
  const ms = Number(record.attributes?.SentTimestamp);
  return Number.isFinite(ms) && ms > 0 ? new Date(ms).toISOString() : undefined;
}

async function processEvent(
  event: FlightEvent,
  statuses: Map<string, FlightStatus>,
  subscriberCache: Map<string, Subscriber[]>,
  enqueued?: string,
): Promise<ProcessResult> {
  const pk = keys.flightPk(event.flightNumber, event.flightDate);
  if (!isNewer(statuses.get(pk), event)) return { outcome: 'STALE', notified: 0 };

  const updated = await applyStatus(event);
  if (!updated) return { outcome: 'STALE', notified: 0 };
  statuses.set(pk, updated);

  let subs = subscriberCache.get(pk);
  if (!subs) {
    subs = await querySubscribers(event.flightNumber, event.flightDate);
    subscriberCache.set(pk, subs);
  }
  const notified = subs.length > 0 ? await publishNotifications(config.topicArn(), event, subs, enqueued) : 0;
  return { outcome: 'NOTIFIED', notified };
}

function toResponse(failures: Set<string>): SQSBatchResponse {
  const batchItemFailures: SQSBatchItemFailure[] = [...failures].map((itemIdentifier) => ({ itemIdentifier }));
  return { batchItemFailures };
}
