process.env.AWS_REGION ??= 'ca-central-1';
process.env.TABLE_NAME = 'test-table';
process.env.QUEUE_URL = 'https://sqs.ca-central-1.amazonaws.com/123456789012/flight-events';
process.env.DLQ_URL = 'https://sqs.ca-central-1.amazonaws.com/123456789012/flight-events-dlq';
process.env.TOPIC_ARN = 'arn:aws:sns:ca-central-1:123456789012:notifications';
process.env.LOG_LEVEL = 'error';
