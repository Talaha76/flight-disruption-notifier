/**
 * Passenger-facing routes:
 *   POST   /subscriptions                                   (supports Idempotency-Key header)
 *   GET    /users/{userId}/subscriptions                    (GSI1)
 *   DELETE /subscriptions/{flightNumber}/{flightDate}/{userId}
 *   GET    /flights/{flightNumber}/{flightDate}             (current flight status)
 */
import { createHash } from 'node:crypto';
import type { APIGatewayProxyEventV2 } from 'aws-lambda';
import { SubscriptionInput } from '../schemas/subscription';
import { DATE_RE, FLIGHT_NUMBER_RE, formatIssues } from '../schemas/flight-event';
import { header, json, parseJsonBody, type HttpResult } from '../lib/http';
import { IDEMPOTENCY_TTL_SECONDS, nowEpochSeconds } from '../lib/keys';
import { logger } from '../lib/logger';
import {
  deleteSubscription, getApiIdempotency, getFlightStatus, listUserSubscriptions, putSubscription, saveApiIdempotency,
} from '../lib/repo';

const IDEMPOTENCY_KEY_RE = /^[A-Za-z0-9_-]{8,128}$/;

export const handler = async (event: APIGatewayProxyEventV2): Promise<HttpResult> => {
  const log = logger.child({ requestId: event.requestContext?.requestId, route: event.routeKey });
  try {
    switch (event.routeKey) {
      case 'POST /subscriptions':
        return await createSubscription(event, log);
      case 'GET /users/{userId}/subscriptions': {
        const userId = event.pathParameters?.userId ?? '';
        if (!/^[A-Za-z0-9_-]{1,64}$/.test(userId)) return json(400, { error: 'Invalid userId' });
        return json(200, { userId, subscriptions: await listUserSubscriptions(userId) });
      }
      case 'DELETE /subscriptions/{flightNumber}/{flightDate}/{userId}': {
        const { flightNumber = '', flightDate = '', userId = '' } = event.pathParameters ?? {};
        if (!FLIGHT_NUMBER_RE.test(flightNumber) || !DATE_RE.test(flightDate) || !/^[A-Za-z0-9_-]{1,64}$/.test(userId)) {
          return json(400, { error: 'Invalid path parameters' });
        }
        const existed = await deleteSubscription(userId, flightNumber, flightDate);
        return existed ? { statusCode: 204, headers: {}, body: '' } : json(404, { error: 'Subscription not found' });
      }
      case 'GET /flights/{flightNumber}/{flightDate}': {
        const { flightNumber = '', flightDate = '' } = event.pathParameters ?? {};
        if (!FLIGHT_NUMBER_RE.test(flightNumber) || !DATE_RE.test(flightDate)) return json(400, { error: 'Invalid path parameters' });
        const status = await getFlightStatus(flightNumber, flightDate);
        if (!status) return json(404, { error: 'No status recorded for this flight' });
        const { PK: _pk, SK: _sk, ...rest } = status as unknown as Record<string, unknown>;
        return json(200, rest);
      }
      default:
        return json(404, { error: 'Not found' });
    }
  } catch (err) {
    log.error('unhandled error', { err });
    return json(500, { error: 'Internal error' });
  }
};

async function createSubscription(event: APIGatewayProxyEventV2, log: ReturnType<typeof logger.child>): Promise<HttpResult> {
  const body = parseJsonBody(event);
  if (!body.ok) return json(400, { error: 'Request body must be valid JSON' });

  const idemKey = header(event, 'Idempotency-Key');
  if (idemKey !== undefined && !IDEMPOTENCY_KEY_RE.test(idemKey)) {
    return json(400, { error: 'Idempotency-Key must be 8-128 chars of letters, digits, _ or -' });
  }

  const parsed = SubscriptionInput.safeParse(body.value);
  if (!parsed.success) return json(400, { error: 'Validation failed', issues: formatIssues(parsed.error) });

  const requestHash = createHash('sha256').update(JSON.stringify(parsed.data)).digest('hex');
  if (idemKey) {
    const prior = await getApiIdempotency(idemKey);
    if (prior) {
      if (prior.requestHash !== requestHash) return json(422, { error: 'Idempotency-Key was already used with a different request body' });
      log.info('idempotent replay', { idempotencyKey: idemKey });
      return { statusCode: prior.statusCode, headers: { 'content-type': 'application/json', 'idempotent-replayed': 'true' }, body: prior.body };
    }
  }

  const record = await putSubscription(parsed.data);
  const res = json(201, record);
  if (idemKey) await saveApiIdempotency(idemKey, requestHash, res.statusCode, res.body, nowEpochSeconds() + IDEMPOTENCY_TTL_SECONDS);
  log.info('subscription created', { userId: record.userId, flightId: `${record.flightNumber}#${record.flightDate}`, channel: record.channel });
  return res;
}
