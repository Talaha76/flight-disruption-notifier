/** Single-table key builders (see docs/architecture.md). */
export const keys = {
  flightPk: (flightNumber: string, flightDate: string) => `FLIGHT#${flightNumber}#${flightDate}`,
  statusSk: () => 'STATUS',
  subSk: (userId: string) => `SUB#${userId}`,
  userPk: (userId: string) => `USER#${userId}`,
  userFlightSk: (flightNumber: string, flightDate: string) => `FLIGHT#${flightNumber}#${flightDate}`,
  idempPk: (eventId: string) => `IDEMP#${eventId}`,
  idempSk: () => 'IDEMP',
  apiIdempPk: (key: string) => `APIKEY#${key}`,
  apiIdempSk: () => 'APIKEY',
};

export const nowEpochSeconds = (): number => Math.floor(Date.now() / 1000);
export const IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60;
