// Dashboard API load: many viewers across tenants hitting all four widgets.
//   docker compose --profile tools run --rm k6 run /scripts/dashboard.js
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.BASE_URL || 'http://web:8080/api';

export const options = {
  scenarios: {
    viewers: { executor: 'constant-vus', vus: 50, duration: '60s' },
  },
  thresholds: {
    'http_req_duration{kind:dashboard}': ['p(95)<200', 'p(99)<500'],
    http_req_failed: ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const subjects = http.get(`${BASE}/v1/dev/subjects`).json('tenants').filter((t) => t.status === 'active');
  return subjects.map((t) => {
    const token = http.post(`${BASE}/v1/dev/login`, JSON.stringify({ subject: t.subject }), {
      headers: { 'Content-Type': 'application/json' },
    }).json('token');
    return { slug: t.slug, token };
  });
}

const ranges = ['today', '7d', '30d', '90d'];
const paths = ['orders/summary', 'orders/breakdown', 'orders/trends', 'products/top'];

export default function (tenants) {
  const t = tenants[Math.floor(Math.random() * tenants.length)];
  const range = ranges[Math.floor(Math.random() * ranges.length)];
  const params = { headers: { Authorization: `Bearer ${t.token}` }, tags: { kind: 'dashboard' } };
  const res = http.batch(paths.map((p) => ['GET', `${BASE}/v1/tenants/${t.slug}/${p}?range=${range}`, null, params]));
  for (const r of res) check(r, { 'status 200': (x) => x.status === 200 });
}

export function handleSummary(data) {
  return { stdout: JSON.stringify(data.metrics.http_req_duration.values, null, 2) + '\n', '/results/k6-dashboard.json': JSON.stringify(data, null, 2) };
}
