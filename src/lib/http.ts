import type { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from 'aws-lambda';

export type HttpResult = APIGatewayProxyResultV2 & { statusCode: number; body: string; headers: Record<string, string> };

export function json(statusCode: number, body: unknown, headers: Record<string, string> = {}): HttpResult {
  return { statusCode, headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) };
}

export function parseJsonBody(event: APIGatewayProxyEventV2): { ok: true; value: unknown } | { ok: false } {
  if (!event.body) return { ok: false };
  try {
    const raw = event.isBase64Encoded ? Buffer.from(event.body, 'base64').toString('utf8') : event.body;
    return { ok: true, value: JSON.parse(raw) };
  } catch {
    return { ok: false };
  }
}

export function header(event: APIGatewayProxyEventV2, name: string): string | undefined {
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(event.headers ?? {})) if (k.toLowerCase() === lower) return v;
  return undefined;
}
