/**
 * Seeds test-channel subscriptions through the deployed API.
 * Usage: npm run seed -- AC123 2026-11-20 25
 */
import { loadOutputs } from './lib/outputs';

async function main() {
  const [flightNumber = 'AC123', flightDate = new Date().toISOString().slice(0, 10), countArg = '5'] = process.argv.slice(2);
  const count = Math.min(Number(countArg) || 5, 200);
  const { ApiUrl } = loadOutputs();
  let ok = 0;
  for (let i = 0; i < count; i++) {
    const userId = `seed-user-${i}`;
    const res = await fetch(`${ApiUrl}/subscriptions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': `seed-${flightNumber}-${flightDate}-${i}` },
      body: JSON.stringify({ userId, flightNumber, flightDate, channel: 'test', contact: `${userId}@example.com` }),
    });
    if (res.ok) ok++;
    else console.error(`subscription ${userId} failed: ${res.status} ${await res.text()}`);
  }
  console.log(`Seeded ${ok}/${count} subscriptions for ${flightNumber} on ${flightDate}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
