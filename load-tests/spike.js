// k6 spike test (Phase 3). Keep it modest: ~3,000 requests ≈ $0.003 of API Gateway.
// PowerShell:  $env:API_URL="https://xxxx.execute-api.ca-central-1.amazonaws.com"; k6 run load-tests/spike.js
// Recommended: deploy with -c billingMode=PAY_PER_REQUEST first so DynamoDB doesn't throttle.
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    spike: { executor: 'ramping-arrival-rate', startRate: 1, timeUnit: '1s', preAllocatedVUs: 20, maxVUs: 60,
      stages: [{ target: 5, duration: '20s' }, { target: 18, duration: '10s' }, { target: 18, duration: '60s' }, { target: 0, duration: '10s' }] },
  },
  thresholds: { http_req_failed: ['rate<0.01'] },
};

const flights = ['AC101', 'AC202', 'AC303', 'AC404', 'AC505'];
const today = new Date().toISOString().slice(0, 10);

export default function () {
  const events = Array.from({ length: 10 }, (_, i) => ({
    type: 'DELAY', version: 1, flightNumber: flights[(__ITER + i) % flights.length], flightDate: today,
    occurredAt: new Date().toISOString(), delayMinutes: 5 + ((__ITER + i) % 120),
  }));
  const res = http.post(`${__ENV.API_URL}/flight-events`, JSON.stringify({ events }), { headers: { 'content-type': 'application/json' } });
  check(res, { 'accepted (202)': (r) => r.status === 202 });
}
