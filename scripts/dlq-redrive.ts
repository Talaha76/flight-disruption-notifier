/**
 * Moves every message in the dead-letter queue back to the main queue (SQS StartMessageMoveTask),
 * after the underlying bug is fixed. Usage: npm run redrive   (add -- --peek to only list messages)
 */
import { GetQueueAttributesCommand, ListMessageMoveTasksCommand, ReceiveMessageCommand, SQSClient, StartMessageMoveTaskCommand } from '@aws-sdk/client-sqs';
import { loadOutputs } from './lib/outputs';

async function main() {
  const out = loadOutputs();
  const sqs = new SQSClient({ region: out.region });
  const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: out.DlqUrl, AttributeNames: ['ApproximateNumberOfMessages'] }));
  const count = Number(attrs.Attributes?.ApproximateNumberOfMessages ?? 0);
  console.log(`DLQ currently holds ~${count} message(s).`);

  if (process.argv.includes('--peek')) {
    const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl: out.DlqUrl, MaxNumberOfMessages: 10, VisibilityTimeout: 1, MessageAttributeNames: ['All'] }));
    for (const m of res.Messages ?? []) console.log(JSON.stringify({ messageId: m.MessageId, reason: m.MessageAttributes?.errorReason?.StringValue, body: m.Body?.slice(0, 300) }));
    return;
  }
  if (count === 0) return;

  // Messages the processor itself parked here (poison) have no "source" queue recorded, so redrive
  // explicitly to the main queue. Poison messages will simply be parked again until fixed.
  const task = await sqs.send(new StartMessageMoveTaskCommand({ SourceArn: out.DlqArn, DestinationArn: out.QueueArn }));
  console.log(`Started redrive task ${task.TaskHandle}`);
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 2000));
    const tasks = await sqs.send(new ListMessageMoveTasksCommand({ SourceArn: out.DlqArn, MaxResults: 1 }));
    const t = tasks.Results?.[0];
    console.log(`status=${t?.Status} moved=${t?.ApproximateNumberOfMessagesMoved}/${t?.ApproximateNumberOfMessagesToMove ?? '?'}`);
    if (t && t.Status !== 'RUNNING') break;
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
