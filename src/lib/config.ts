/** Reads required environment variables lazily so tests can set them per case. */
export function env(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing required environment variable ${name}`);
  return v;
}

export const config = {
  tableName: () => env('TABLE_NAME'),
  queueUrl: () => env('QUEUE_URL'),
  dlqUrl: () => env('DLQ_URL'),
  topicArn: () => env('TOPIC_ARN'),
};
