import { SendMessageBatchCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { sqs } from './clients';
import type { FlightEvent } from '../schemas/flight-event';

export interface SendFailure {
  eventId: string;
  code?: string;
  message?: string;
}

/** Sends up to N events to the flight-events queue in SendMessageBatch chunks of 10. */
export async function sendEvents(queueUrl: string, events: FlightEvent[]): Promise<SendFailure[]> {
  const failures: SendFailure[] = [];
  for (let i = 0; i < events.length; i += 10) {
    const chunk = events.slice(i, i + 10);
    const res = await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: chunk.map((e, idx) => ({
          Id: String(idx),
          MessageBody: JSON.stringify(e),
          MessageAttributes: {
            eventId: { DataType: 'String', StringValue: e.eventId },
            source: { DataType: 'String', StringValue: e.source },
            type: { DataType: 'String', StringValue: e.type },
            version: { DataType: 'Number', StringValue: String(e.version) },
          },
        })),
      }),
    );
    for (const f of res.Failed ?? []) {
      const ev = chunk[Number(f.Id)];
      failures.push({ eventId: ev?.eventId ?? `unknown-${f.Id}`, code: f.Code, message: f.Message });
    }
  }
  return failures;
}

/** Sends a non-retryable (poison) payload straight to the dead-letter queue with the reason attached. */
export async function sendToDlq(dlqUrl: string, body: string, reason: string, origin: string): Promise<void> {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: dlqUrl,
      MessageBody: body,
      MessageAttributes: {
        errorReason: { DataType: 'String', StringValue: reason.slice(0, 1000) },
        origin: { DataType: 'String', StringValue: origin },
      },
    }),
  );
}
