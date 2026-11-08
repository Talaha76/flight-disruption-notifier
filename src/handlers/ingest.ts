/**
 * POST /flight-events
 * Validates 1–10 events (zod), fills in eventId/source, and enqueues them with
 * SendMessageBatch. Returns 202 quickly; the processor does the real work.
 */
import { randomUUID } from 'node:crypto';
import type { APIGatewayProxyEventV2 } from 'aws-lambda';
import { FlightEvent, IngestRequest, formatIssues, type FlightEventInput } from '../schemas/flight-event';
import { config } from '../lib/config';
import { json, parseJsonBody, type HttpResult } from '../lib/http';
import { logger } from '../lib/logger';
import { sendEvents } from '../lib/queue';

export const handler = async (event: APIGatewayProxyEventV2): Promise<HttpResult> => {
  const log = logger.child({ requestId: event.requestContext?.requestId, route: 'POST /flight-events' });
  const body = parseJsonBody(event);
  if (!body.ok) return json(400, { error: 'Request body must be valid JSON' });

  const parsed = IngestRequest.safeParse(body.value);
  if (!parsed.success) {
    log.warn('validation failed', { issues: formatIssues(parsed.error) });
    return json(400, { error: 'Validation failed', issues: formatIssues(parsed.error) });
  }

  const inputs: FlightEventInput[] = Array.isArray(parsed.data) ? parsed.data : 'events' in parsed.data ? parsed.data.events : [parsed.data];
  const events: FlightEvent[] = inputs.map((i) => FlightEvent.parse({ ...i, eventId: i.eventId ?? randomUUID(), source: i.source ?? 'api' }));

  const ids = events.map((e) => e.eventId);
  if (new Set(ids).size !== ids.length) return json(400, { error: 'Duplicate eventId within one request' });

  try {
    const failed = await sendEvents(config.queueUrl(), events);
    const failedIds = new Set(failed.map((f) => f.eventId));
    const accepted = ids.filter((id) => !failedIds.has(id));
    for (const e of events) {
      if (!failedIds.has(e.eventId)) log.info('event enqueued', { eventId: e.eventId, flightId: `${e.flightNumber}#${e.flightDate}`, type: e.type });
    }
    if (failed.length > 0) {
      log.error('some events failed to enqueue', { failed });
      return json(failed.length === events.length ? 502 : 207, { accepted, failed });
    }
    return json(202, { accepted });
  } catch (err) {
    log.error('enqueue failed', { err });
    return json(502, { error: 'Failed to enqueue events' });
  }
};
