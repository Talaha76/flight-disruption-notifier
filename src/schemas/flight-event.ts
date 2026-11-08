import { z } from 'zod';

/**
 * Versioned event envelope shared by every producer (REST API, legacy XML feed,
 * SkyBook Kafka bridge). `source` + `type` + `version` let the processor route
 * messages and reject unknown contracts.
 */
export const FLIGHT_NUMBER_RE = /^[A-Z0-9]{2}\d{1,4}$/;
export const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

export const EventSource = z.enum(['api', 'legacy-feed', 'skybook']);
export const EventType = z.enum(['DELAY', 'CANCELLATION', 'GATE_CHANGE']);

const base = {
  eventId: z.string().min(1).max(128).regex(/^[A-Za-z0-9._:-]+$/, 'eventId may only contain letters, digits, . _ : -'),
  source: EventSource,
  version: z.literal(1),
  flightNumber: z.string().regex(FLIGHT_NUMBER_RE, 'flightNumber must look like AC123'),
  flightDate: z.string().regex(DATE_RE, 'flightDate must be YYYY-MM-DD'),
  occurredAt: z.iso.datetime({ offset: true }),
  reason: z.string().max(280).optional(),
};

export const DelayEvent = z.object({
  ...base,
  type: z.literal('DELAY'),
  delayMinutes: z.number().int().min(1).max(2880),
  newDepartureTime: z.iso.datetime({ offset: true }).optional(),
});

export const CancellationEvent = z.object({
  ...base,
  type: z.literal('CANCELLATION'),
});

export const GateChangeEvent = z.object({
  ...base,
  type: z.literal('GATE_CHANGE'),
  newGate: z.string().min(1).max(10),
  oldGate: z.string().min(1).max(10).optional(),
});

export const FlightEvent = z.discriminatedUnion('type', [DelayEvent, CancellationEvent, GateChangeEvent]);
export type FlightEvent = z.infer<typeof FlightEvent>;

/** API input: eventId and source are optional (server fills them in). */
export const FlightEventInput = z.discriminatedUnion('type', [
  DelayEvent.extend({ eventId: base.eventId.optional(), source: EventSource.optional() }),
  CancellationEvent.extend({ eventId: base.eventId.optional(), source: EventSource.optional() }),
  GateChangeEvent.extend({ eventId: base.eventId.optional(), source: EventSource.optional() }),
]);
export type FlightEventInput = z.infer<typeof FlightEventInput>;

export const IngestRequest = z.union([
  FlightEventInput,
  z.array(FlightEventInput).min(1).max(10),
  z.object({ events: z.array(FlightEventInput).min(1).max(10) }),
]);

export const flightId = (flightNumber: string, flightDate: string): string => `${flightNumber}#${flightDate}`;

export function formatIssues(err: z.ZodError): { path: string; message: string }[] {
  return err.issues.map((i) => ({ path: i.path.join('.'), message: i.message }));
}
