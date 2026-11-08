/** Minimal structured JSON logger. Every line carries the bound context (eventId, flightId, ...). */
type Level = 'debug' | 'info' | 'warn' | 'error';
const ORDER: Record<Level, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export interface Logger {
  debug(msg: string, extra?: Record<string, unknown>): void;
  info(msg: string, extra?: Record<string, unknown>): void;
  warn(msg: string, extra?: Record<string, unknown>): void;
  error(msg: string, extra?: Record<string, unknown>): void;
  child(ctx: Record<string, unknown>): Logger;
}

function serializeError(e: unknown): unknown {
  if (e instanceof Error) return { name: e.name, message: e.message, stack: e.stack };
  return e;
}

export function createLogger(ctx: Record<string, unknown> = {}): Logger {
  const min = ORDER[(process.env.LOG_LEVEL?.toLowerCase() as Level) ?? 'info'] ?? ORDER.info;
  const emit = (level: Level, msg: string, extra?: Record<string, unknown>) => {
    if (ORDER[level] < min) return;
    const line: Record<string, unknown> = { level, msg, time: new Date().toISOString(), service: process.env.SERVICE_NAME ?? 'flight-disruption-notifier', ...ctx };
    if (extra) for (const [k, v] of Object.entries(extra)) line[k] = k === 'err' || k === 'error' ? serializeError(v) : v;
    const out = JSON.stringify(line);
    if (level === 'error') console.error(out);
    else if (level === 'warn') console.warn(out);
    else console.log(out);
  };
  return {
    debug: (m, e) => emit('debug', m, e),
    info: (m, e) => emit('info', m, e),
    warn: (m, e) => emit('warn', m, e),
    error: (m, e) => emit('error', m, e),
    child: (more) => createLogger({ ...ctx, ...more }),
  };
}

export const logger = createLogger();
