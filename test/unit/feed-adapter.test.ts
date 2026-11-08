import { mockClient } from 'aws-sdk-client-mock';
import { SendMessageBatchCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { generateSampleFeed, handler, parseFeed } from '../../src/handlers/feed-adapter';

const sqsMock = mockClient(SQSClient);
beforeEach(() => { sqsMock.reset(); sqsMock.on(SendMessageBatchCommand).resolves({ Successful: [], Failed: [] }); sqsMock.on(SendMessageCommand).resolves({}); });

const xml = `<?xml version="1.0"?>
<FlightStatusFeed generatedAt="2026-11-20T12:00:00Z">
  <FlightStatus recordId="R1"><Carrier>AC</Carrier><FlightNo>123</FlightNo><DepDate>2026-11-20</DepDate><StatusCode>DL</StatusCode><DelayMin>45</DelayMin><Remark>Crew</Remark><Timestamp>2026-11-20T11:59:00Z</Timestamp></FlightStatus>
  <FlightStatus recordId="R2"><Carrier>AC</Carrier><FlightNo>456</FlightNo><DepDate>2026-11-20</DepDate><StatusCode>GC</StatusCode><Gate>D7</Gate><PrevGate>D5</PrevGate><Timestamp>2026-11-20T11:59:00Z</Timestamp></FlightStatus>
  <FlightStatus recordId="R3"><Carrier>AC</Carrier><FlightNo>789</FlightNo><DepDate>2026-11-20</DepDate><StatusCode>CX</StatusCode><Timestamp>2026-11-20T11:59:00Z</Timestamp></FlightStatus>
  <FlightStatus recordId="R4"><Carrier>AC</Carrier><FlightNo>999</FlightNo><DepDate>2026-11-20</DepDate><StatusCode>ZZ</StatusCode><Timestamp>2026-11-20T11:59:00Z</Timestamp></FlightStatus>
  <FlightStatus recordId="R5"><Carrier>AC</Carrier><FlightNo>111</FlightNo><DepDate>2026-11-20</DepDate><StatusCode>DL</StatusCode><DelayMin>abc</DelayMin><Timestamp>2026-11-20T11:59:00Z</Timestamp></FlightStatus>
</FlightStatusFeed>`;

describe('legacy feed adapter', () => {
  it('converts XML records to versioned JSON events and isolates bad records', () => {
    const { events, errors } = parseFeed(xml);
    expect(events.map((e) => [e.type, e.flightNumber])).toEqual([['DELAY', 'AC123'], ['GATE_CHANGE', 'AC456'], ['CANCELLATION', 'AC789']]);
    expect(events[0]).toMatchObject({ source: 'legacy-feed', version: 1, delayMinutes: 45, reason: 'Crew' });
    expect(events[1]).toMatchObject({ newGate: 'D7', oldGate: 'D5' });
    expect(errors.map((e) => e.recordId)).toEqual(['R4', 'R5']);
  });

  it('produces deterministic eventIds so re-reading the feed is deduplicated', () => {
    expect(parseFeed(xml).events.map((e) => e.eventId)).toEqual(parseFeed(xml).events.map((e) => e.eventId));
  });

  it('reports malformed XML as a feed-level error', () => {
    const { events, errors } = parseFeed('<FlightStatusFeed><FlightStatus>');
    expect(events).toHaveLength(0);
    expect(errors[0].recordId).toBe('FEED');
  });

  it('handler enqueues good events and sends bad records to the DLQ', async () => {
    const res = await handler({ xml });
    expect(res).toEqual({ sent: 3, failed: 0, badRecords: 2 });
    expect(sqsMock.commandCalls(SendMessageBatchCommand)[0].args[0].input.Entries).toHaveLength(3);
    const dlq = sqsMock.commandCalls(SendMessageCommand).map((c) => c.args[0].input);
    expect(dlq).toHaveLength(2);
    expect(dlq.every((d) => d.QueueUrl === process.env.DLQ_URL && d.MessageAttributes!.origin.StringValue === 'legacy-feed')).toBe(true);
  });

  it('simulator feed parses cleanly (bad record only when requested)', () => {
    const now = new Date('2026-11-20T12:34:56Z');
    expect(parseFeed(generateSampleFeed(now))).toMatchObject({ errors: [] });
    expect(parseFeed(generateSampleFeed(now)).events).toHaveLength(2);
    expect(parseFeed(generateSampleFeed(now, true)).errors).toHaveLength(1);
  });

  it('throws when enqueueing fails so the scheduler retries', async () => {
    sqsMock.on(SendMessageBatchCommand).resolves({ Failed: [{ Id: '0', Code: 'X', SenderFault: false }], Successful: [] });
    await expect(handler({ xml })).rejects.toThrow(/failed to enqueue/);
  });
});
