/**
 * AWS SDK v3 modular clients, created once per Lambda execution environment
 * (outside the handler) so warm invocations reuse connections.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { SQSClient } from '@aws-sdk/client-sqs';
import { SNSClient } from '@aws-sdk/client-sns';

export const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
export const sqs = new SQSClient({});
export const sns = new SNSClient({});
