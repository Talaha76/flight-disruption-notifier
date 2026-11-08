import { FlightEvent, IngestRequest } from '../../src/schemas/flight-event';
import { SubscriptionInput } from '../../src/schemas/subscription';
import { delayEvent, gateEvent } from '../helpers/fixtures';

describe('FlightEvent schema', () => {
  it('accepts valid delay, gate change and cancellation events', () => {
    expect(FlightEvent.safeParse(delayEvent()).success).toBe(true);
    expect(FlightEvent.safeParse(gateEvent()).success).toBe(true);
    expect(FlightEvent.safeParse({ ...delayEvent(), type: 'CANCELLATION', delayMinutes: undefined }).success).toBe(true);
  });
  it('requires type-specific fields', () => {
    expect(FlightEvent.safeParse({ ...delayEvent(), delayMinutes: undefined }).success).toBe(false);
    expect(FlightEvent.safeParse({ ...gateEvent(), newGate: undefined }).success).toBe(false);
  });
  it('rejects unknown versions, types and malformed fields', () => {
    expect(FlightEvent.safeParse(delayEvent({ version: 2 })).success).toBe(false);
    expect(FlightEvent.safeParse(delayEvent({ type: 'DIVERSION' })).success).toBe(false);
    expect(FlightEvent.safeParse(delayEvent({ flightNumber: 'ac 123' })).success).toBe(false);
    expect(FlightEvent.safeParse(delayEvent({ flightDate: '20-11-2026' })).success).toBe(false);
    expect(FlightEvent.safeParse(delayEvent({ occurredAt: 'yesterday' })).success).toBe(false);
    expect(FlightEvent.safeParse(delayEvent({ eventId: 'bad id with spaces' })).success).toBe(false);
  });
  it('IngestRequest accepts single, array and {events} shapes, max 10', () => {
    const { eventId: _e, source: _s, ...input } = delayEvent();
    expect(IngestRequest.safeParse(input).success).toBe(true);
    expect(IngestRequest.safeParse([input, input]).success).toBe(true);
    expect(IngestRequest.safeParse({ events: [input] }).success).toBe(true);
    expect(IngestRequest.safeParse(Array(11).fill(input)).success).toBe(false);
  });
});

describe('SubscriptionInput schema', () => {
  const base = { userId: 'u1', flightNumber: 'AC123', flightDate: '2026-11-20' };
  it('validates contact per channel', () => {
    expect(SubscriptionInput.safeParse({ ...base, channel: 'email', contact: 'a@b.com' }).success).toBe(true);
    expect(SubscriptionInput.safeParse({ ...base, channel: 'email', contact: 'nope' }).success).toBe(false);
    expect(SubscriptionInput.safeParse({ ...base, channel: 'sms', contact: '+14165550123' }).success).toBe(true);
    expect(SubscriptionInput.safeParse({ ...base, channel: 'sms', contact: '4165550123' }).success).toBe(false);
    expect(SubscriptionInput.safeParse({ ...base, channel: 'test', contact: 'anything' }).success).toBe(true);
  });
});
