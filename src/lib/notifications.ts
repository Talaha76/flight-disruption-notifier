import { PublishBatchCommand, type PublishBatchRequestEntry } from '@aws-sdk/client-sns';
import { sns } from './clients';
import type { FlightEvent } from '../schemas/flight-event';

export interface Subscriber {
  userId: string;
  channel: string;
  contact: string;
}

export interface NotificationPayload {
  eventId: string;
  flightNumber: string;
  flightDate: string;
  type: FlightEvent['type'];
  userId: string;
  channel: string;
  contact: string;
  subject: string;
  text: string;
  /** When the event entered the flight-events queue (SQS SentTimestamp), if known. */
  enqueuedAt?: string;
  /** When the processor published the notification. enqueuedAt -> sentAt = pipeline latency (AWS clocks only). */
  sentAt: string;
}

export function renderMessage(e: FlightEvent): { subject: string; text: string } {
  const flight = `${e.flightNumber} on ${e.flightDate}`;
  const reason = e.reason ? ` Reason: ${e.reason}.` : '';
  switch (e.type) {
    case 'DELAY': {
      const newTime = e.newDepartureTime ? ` New departure time: ${e.newDepartureTime}.` : '';
      return { subject: `Flight ${e.flightNumber} delayed`, text: `Your flight ${flight} is delayed by ${e.delayMinutes} minutes.${newTime}${reason}` };
    }
    case 'CANCELLATION':
      return { subject: `Flight ${e.flightNumber} cancelled`, text: `Your flight ${flight} has been cancelled.${reason} Please check rebooking options.` };
    case 'GATE_CHANGE': {
      const from = e.oldGate ? ` from ${e.oldGate}` : '';
      return { subject: `Gate change for ${e.flightNumber}`, text: `The gate for your flight ${flight} has changed${from} to ${e.newGate}.${reason}` };
    }
  }
}

/**
 * Publishes one notification per subscriber with SNS PublishBatch (10 per call).
 * Message attributes allow per-channel filter policies on topic subscriptions.
 * Failed entries are retried once; anything still failing throws so the SQS
 * record is retried (idempotency prevents re-sending for completed events only,
 * so a partial SNS failure can produce a duplicate for some subscribers; this
 * is a documented at-least-once trade-off).
 */
export async function publishNotifications(topicArn: string, e: FlightEvent, subscribers: Subscriber[], enqueuedAt?: string): Promise<number> {
  const { subject, text } = renderMessage(e);
  const sentAt = new Date().toISOString();
  let entries: PublishBatchRequestEntry[] = subscribers.map((s, i) => {
    const payload: NotificationPayload = {
      eventId: e.eventId, flightNumber: e.flightNumber, flightDate: e.flightDate, type: e.type,
      userId: s.userId, channel: s.channel, contact: s.contact, subject, text, enqueuedAt, sentAt,
    };
    return {
      Id: `n${i}`,
      Subject: subject,
      Message: JSON.stringify(payload),
      MessageAttributes: {
        channel: { DataType: 'String', StringValue: s.channel },
        eventType: { DataType: 'String', StringValue: e.type },
        flightNumber: { DataType: 'String', StringValue: e.flightNumber },
      },
    };
  });

  let sent = 0;
  for (let attempt = 0; attempt < 2 && entries.length > 0; attempt++) {
    const retry: PublishBatchRequestEntry[] = [];
    for (let i = 0; i < entries.length; i += 10) {
      const chunk = entries.slice(i, i + 10);
      const res = await sns.send(new PublishBatchCommand({ TopicArn: topicArn, PublishBatchRequestEntries: chunk }));
      sent += res.Successful?.length ?? 0;
      for (const f of res.Failed ?? []) {
        const entry = chunk.find((c) => c.Id === f.Id);
        if (entry && !f.SenderFault) retry.push(entry);
        else if (entry) throw new Error(`SNS rejected notification ${f.Id}: ${f.Code} ${f.Message}`);
      }
    }
    entries = retry;
  }
  if (entries.length > 0) throw new Error(`${entries.length} notifications failed to publish after retry`);
  return sent;
}
