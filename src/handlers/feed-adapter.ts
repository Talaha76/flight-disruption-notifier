/**
 * "Legacy" XML flight-status feed adapter.
 *
 * Triggered by EventBridge Scheduler (disabled by default to keep the account quiet).
 * Reads an XML feed (the event payload's `xml` field if present, otherwise a generated
 * simulator feed), converts each <FlightStatus> record to a versioned JSON event and
 * sends valid ones to the flight-events queue with SendMessageBatch. Bad records go to
 * the DLQ instead of failing the whole run.
 *
 * Feed format:
 * <FlightStatusFeed generatedAt="2026-11-20T12:00:00Z">
 *   <FlightStatus recordId="R1">
 *     <Carrier>AC</Carrier><FlightNo>123</FlightNo><DepDate>2026-11-20</DepDate>
 *     <StatusCode>DL</StatusCode>  <!-- DL=delay, CX=cancel, GC=gate change -->
 *     <DelayMin>45</DelayMin><Gate>C32</Gate><PrevGate>C30</PrevGate>
 *     <Remark>Crew</Remark><Timestamp>2026-11-20T11:59:00Z</Timestamp>
 *   </FlightStatus>
 * </FlightStatusFeed>
 */
import { createHash } from 'node:crypto';
import { XMLParser } from 'fast-xml-parser';
import { FlightEvent, formatIssues } from '../schemas/flight-event';
import { config } from '../lib/config';
import { logger } from '../lib/logger';
import { sendEvents, sendToDlq } from '../lib/queue';

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_',
  parseTagValue: false, // keep everything as strings; we convert explicitly
  isArray: (name) => name === 'FlightStatus',
});

export interface FeedRecordError {
  recordId: string;
  raw: unknown;
  reason: string;
}

const STATUS_MAP: Record<string, FlightEvent['type']> = { DL: 'DELAY', CX: 'CANCELLATION', GC: 'GATE_CHANGE' };

/** Pure function: XML string -> valid events + per-record errors. */
export function parseFeed(xml: string): { events: FlightEvent[]; errors: FeedRecordError[] } {
  const events: FlightEvent[] = [];
  const errors: FeedRecordError[] = [];
  let doc: Record<string, unknown>;
  try {
    doc = parser.parse(xml, true) as Record<string, unknown>;
  } catch (err) {
    errors.push({ recordId: 'FEED', raw: xml.slice(0, 500), reason: `Malformed XML: ${(err as Error).message}` });
    return { events, errors };
  }
  const feed = doc.FlightStatusFeed as Record<string, unknown> | undefined;
  const records = (feed?.FlightStatus as Record<string, string>[] | undefined) ?? [];
  if (!feed) errors.push({ recordId: 'FEED', raw: xml.slice(0, 500), reason: 'Missing <FlightStatusFeed> root' });

  for (const r of records) {
    const recordId = String(r['@_recordId'] ?? 'unknown');
    const type = STATUS_MAP[String(r.StatusCode ?? '').trim()];
    if (!type) {
      errors.push({ recordId, raw: r, reason: `Unknown StatusCode "${r.StatusCode}"` });
      continue;
    }
    const flightNumber = `${String(r.Carrier ?? '').trim()}${String(r.FlightNo ?? '').trim()}`;
    const occurredAt = String(r.Timestamp ?? '').trim();
    // Deterministic eventId: re-reading the same record yields the same id -> idempotency dedupes it.
    const eventId = `feed-${createHash('sha256').update(`${recordId}|${flightNumber}|${r.DepDate}|${occurredAt}|${r.StatusCode}`).digest('hex').slice(0, 32)}`;
    const candidate: Record<string, unknown> = {
      eventId, source: 'legacy-feed', version: 1, type, flightNumber,
      flightDate: String(r.DepDate ?? '').trim(), occurredAt,
      reason: r.Remark ? String(r.Remark).trim() : undefined,
    };
    if (type === 'DELAY') candidate.delayMinutes = Number(r.DelayMin);
    if (type === 'GATE_CHANGE') {
      candidate.newGate = r.Gate ? String(r.Gate).trim() : undefined;
      candidate.oldGate = r.PrevGate ? String(r.PrevGate).trim() : undefined;
    }
    const parsed = FlightEvent.safeParse(candidate);
    if (parsed.success) events.push(parsed.data);
    else errors.push({ recordId, raw: r, reason: JSON.stringify(formatIssues(parsed.error)) });
  }
  return { events, errors };
}

/** Simulator: a small feed whose records change once per minute (so each run yields fresh events). */
export function generateSampleFeed(now = new Date(), includeBadRecord = false): string {
  const minute = new Date(Math.floor(now.getTime() / 60000) * 60000).toISOString().replace('.000Z', 'Z');
  const date = minute.slice(0, 10);
  const m = now.getUTCMinutes();
  const rec = (id: string, inner: string) => `  <FlightStatus recordId="${id}-${minute}">${inner}<Timestamp>${minute}</Timestamp></FlightStatus>`;
  return [
    `<FlightStatusFeed generatedAt="${minute}">`,
    rec('R1', `<Carrier>AC</Carrier><FlightNo>101</FlightNo><DepDate>${date}</DepDate><StatusCode>DL</StatusCode><DelayMin>${15 + (m % 6) * 15}</DelayMin><Remark>Late inbound aircraft</Remark>`),
    rec('R2', `<Carrier>AC</Carrier><FlightNo>202</FlightNo><DepDate>${date}</DepDate><StatusCode>GC</StatusCode><Gate>C${30 + (m % 10)}</Gate>`),
    ...(includeBadRecord ? [rec('R3', `<Carrier>AC</Carrier><FlightNo>303</FlightNo><DepDate>${date}</DepDate><StatusCode>XX</StatusCode>`)] : []), // deliberately bad record
    `</FlightStatusFeed>`,
  ].join('\n');
}

export const handler = async (input: { xml?: string; includeBadRecord?: boolean } = {}): Promise<{ sent: number; failed: number; badRecords: number }> => {
  const xml = typeof input?.xml === 'string' ? input.xml : generateSampleFeed(new Date(), input?.includeBadRecord === true);
  const { events, errors } = parseFeed(xml);

  for (const e of errors) {
    logger.warn('bad feed record routed to DLQ', { recordId: e.recordId, reason: e.reason });
    await sendToDlq(config.dlqUrl(), JSON.stringify({ recordId: e.recordId, raw: e.raw }), e.reason, 'legacy-feed');
  }
  const failed = events.length ? await sendEvents(config.queueUrl(), events) : [];
  if (failed.length) logger.error('feed events failed to enqueue', { failed });
  const summary = { sent: events.length - failed.length, failed: failed.length, badRecords: errors.length };
  logger.info('feed run complete', summary);
  if (failed.length) throw new Error(`${failed.length} feed events failed to enqueue`); // scheduler retry policy re-runs; ids are deterministic
  return summary;
};
