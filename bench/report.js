// Builds the Markdown result tables from two suite runs (1M and 10M).
// Usage: node bench/report.js bench/results/suite-1M-<t>.log bench/results/suite-10M-<t>.log
// Every number comes from the JSON files the suite wrote in bench/results/
// during that run's time window, plus the verify summary line in the log.
const fs = require('fs');
const path = require('path');
const dir = path.join(__dirname, 'results');

function stamp(name) {
  const m = name.match(/(\d{8})-(\d{6})/);
  if (!m) return 0;
  const [d, t] = [m[1], m[2]];
  return Date.UTC(+d.slice(0, 4), +d.slice(4, 6) - 1, +d.slice(6, 8), +t.slice(0, 2), +t.slice(2, 4), +t.slice(4, 6));
}

// the newest seed file written before the suite started (seeding, then snapshots, then the suite)
function latestSeed(before) {
  const f = fs.readdirSync(dir).filter((x) => /^seed-.*.json$/.test(x) && stamp(x) <= before).sort((x, y) => stamp(x) - stamp(y)).pop();
  return f && JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8'));
}

function load(logPath) {
  const log = fs.readFileSync(logPath, 'utf8');
  const start = stamp(path.basename(logPath)) - 60_000; // the seed file is written just before the suite
  const end = fs.statSync(logPath).mtimeMs + 60_000;
  const files = fs.readdirSync(dir).filter((f) => f.endsWith('.json')).map((f) => ({ f, t: stamp(f) }))
    .filter((x) => x.t >= start && x.t <= end).sort((a, b) => a.t - b.t);
  const pick = (re) => files.filter((x) => re.test(x.f)).map((x) => JSON.parse(fs.readFileSync(path.join(dir, x.f), 'utf8')));
  const verify = (log.match(/(\d+) tenants, (\d+) checks, (\d+) tenants with mismatches \(([^)]+)\)/) || []).slice(1);
  const storms = pick(/^writestorm-/);
  return {
    seed: latestSeed(start + 60_000), verify, ladder: pick(/^ladder-/)[0], fresh: pick(/^freshness-/)[0],
    off: pick(/^oltp-capture-off-/)[0], on: pick(/^oltp-capture-on-/)[0], stress: pick(/^stress-/)[0],
    heavy: storms.find((s) => s.target_tps >= 1000), safety: storms.find((s) => s.target_tps < 1000),
  };
}

const [a, b] = process.argv.slice(2).map(load);
const ms = (v) => (v == null ? '—' : v >= 1000 ? `${(v / 1000).toFixed(2)} s` : v >= 100 ? `${Math.round(v)} ms` : `${v.toFixed(1)} ms`);
const pct = (v) => `${v >= 0 ? '+' : ''}${v.toFixed(1)}%`;
const int = (v) => Math.round(v).toLocaleString('en-US');
const trio = (p) => (p && p.count ? `${ms(p.p50_ms)} / ${ms(p.p95_ms)} / ${ms(p.p99_ms)}` : '—');
const out = [];
const p = (s = '') => out.push(s);

// ---------------------------------------------------------------- data
const tot = (s) => {
  const t = { orders: 0, order_items: 0, products: 0, customers: 0, total: 0, n: 0, maxOrders: 0 };
  for (const v of Object.values(s.per_tenant)) {
    const c = v.counts || v;
    t.orders += c.orders || 0; t.order_items += c.order_items || 0; t.products += c.products || 0; t.customers += c.customers || 0;
    t.total += (c.orders || 0) + (c.order_items || 0) + (c.products || 0) + (c.customers || 0);
    t.maxOrders = Math.max(t.maxOrders, c.orders || 0); t.n++;
  }
  return t;
};
const [da, db] = [tot(a.seed), tot(b.seed)];
p('### Data');
p();
p('| | 1M run | 10M run |');
p('|---|---:|---:|');
p(`| Stores | ${da.n} | ${db.n} |`);
p(`| Rows (customers + products + orders + order lines) | ${int(da.total)} | ${int(db.total)} |`);
p(`| Orders | ${int(da.orders)} | ${int(db.orders)} |`);
p(`| Order lines | ${int(da.order_items)} | ${int(db.order_items)} |`);
p(`| Largest store's orders | ${int(da.maxOrders)} | ${int(db.maxOrders)} |`);
p();

// ---------------------------------------------------------------- correctness
p('### Correctness');
p();
p('| | 1M | 10M |');
p('|---|---:|---:|');
p(`| Stores checked | ${a.verify[0]} | ${b.verify[0]} |`);
p(`| Checks (every widget, ClickHouse and in-memory views, vs direct SQL) | ${int(+a.verify[1])} | ${int(+b.verify[1])} |`);
p(`| Stores with any mismatch | ${a.verify[2]} | ${b.verify[2]} |`);
p();

// ---------------------------------------------------------------- query speed + growth (ladder)
p('### Query speed and growth (largest store, one query at a time, p50 / p95)');
p();
p(`1M: ${a.ladder.tenant} with ${int(a.ladder.orders)} orders. 10M: ${b.ladder.tenant} with ${int(b.ladder.orders)} orders.`);
p();
p('| Query | Store DB, B-tree index (1M → 10M) | Store DB, covering index (10M) | **ClickHouse 1M** | **ClickHouse 10M** | ClickHouse growth | Store DB growth |');
p('|---|---:|---:|---:|---:|---:|---:|');
let worstGrowth = 0, worstCH = 0;
const order = ['totals', 'daily trend', 'top-10 products'].flatMap((k) => ['7d', '30d', '365d'].map((r) => k + ' ' + r));
for (const q of order.filter((x) => b.ladder.results[x]).concat(Object.keys(b.ladder.results).filter((x) => !order.includes(x)))) {
  const r1 = a.ladder.results[q], r2 = b.ladder.results[q];
  if (!r1 || !r2) continue;
  const g = r2[3].p50_ms / r1[3].p50_ms, gpg = r2[1].p50_ms / r1[1].p50_ms;
  worstGrowth = Math.max(worstGrowth, g);
  worstCH = Math.max(worstCH, r2[3].p95_ms);
  p(`| ${q} | ${ms(r1[1].p50_ms)} → ${ms(r2[1].p50_ms)} | ${ms(r2[2].p50_ms)} / ${ms(r2[2].p95_ms)} | ${ms(r1[3].p50_ms)} / ${ms(r1[3].p95_ms)} | ${ms(r2[3].p50_ms)} / ${ms(r2[3].p95_ms)} | **${g.toFixed(2)}×** | ${gpg.toFixed(1)}× |`);
}
p();

// ---------------------------------------------------------------- freshness
p('### Freshness (order committed → visible, 30 samples, p50 / p95 / p99)');
p();
p('| Measured to | 1M | 10M |');
p('|---|---:|---:|');
p(`| Queryable in ClickHouse | ${trio(a.fresh.commit_to_analytics)} | ${trio(b.fresh.commit_to_analytics)} |`);
p(`| Visible through the dashboard API | ${trio(a.fresh.commit_to_dashboard_api)} | ${trio(b.fresh.commit_to_dashboard_api)} |`);
p();

// ---------------------------------------------------------------- database safety
const phase = (s, k) => (s ? s.phases.find((x) => x.key === k) : null);
p('### Database safety (store transactions, CPU-pinned, p50 / p95 / p99)');
p();
p('The PRD measures normal store query time with and without dashboard load. In both arms the analytics pipeline is running; the only difference is 100 dashboard viewers.');
p();
p('| | 1M | 10M |');
p('|---|---:|---:|');
const sa = [a.safety, b.safety];
const row = (name, f) => p(`| ${name} | ${sa.map((s) => (s ? f(s) : '—')).join(' | ')} |`);
row('Store rate (transactions/s, target)', (s) => `${int(phase(s, 'aggregation').store_tps)} (${s.target_tps})`);
row('Without dashboards', (s) => trio(phase(s, 'baseline').store_latency));
row('With 100 dashboard viewers', (s) => trio(phase(s, 'aggregation').store_latency));
row('**Change at p95**', (s) => `**${pct((phase(s, 'aggregation').store_latency.p95_ms / phase(s, 'baseline').store_latency.p95_ms - 1) * 100)}**`);
row('Change at p50', (s) => pct((phase(s, 'aggregation').store_latency.p50_ms / phase(s, 'baseline').store_latency.p50_ms - 1) * 100));
row('Dashboard queries that reached the store databases', (s) => {
  const ag = phase(s, 'aggregation'), bl = phase(s, 'baseline');
  return `${int(ag.store_db_foreign_transactions ?? 0)} (baseline noise: ${int(bl.store_db_foreign_transactions ?? 0)})`;
});
row('Dashboard reads served (p50 / p95 / p99)', (s) => trio(phase(s, 'aggregation').dashboard_latency));
p();
p('Separately, the cost of change capture itself (capture paused vs running, same store workload, no dashboards):');
p();
p('| | 1M | 10M |');
p('|---|---:|---:|');
p(`| Capture paused (p50 / p95 / p99) | ${trio(a.off.overall)} | ${trio(b.off.overall)} |`);
p(`| Capture running | ${trio(a.on.overall)} | ${trio(b.on.overall)} |`);
p(`| Change at p95 | ${pct((a.on.overall.p95_ms / a.off.overall.p95_ms - 1) * 100)} | ${pct((b.on.overall.p95_ms / b.off.overall.p95_ms - 1) * 100)} |`);
p();

// ---------------------------------------------------------------- read stress
p('### Read stress: dashboards hammered (20 users with no pause, CPU-pinned)');
p();
p('| Phase | 1M req/s | 1M p50 / p95 / p99 | 10M req/s | 10M p50 / p95 / p99 | Errors (1M / 10M) |');
p('|---|---:|---:|---:|---:|---:|');
const errs = (ph) => Object.values(ph.dashboard_errors || {}).reduce((n, v) => n + v, 0);
for (const k of ['direct', 'aggregation', 'clickhouse']) {
  const find = (s) => s.phases.find((x) => (k === 'direct' ? x.target === 'api-direct' : k === 'clickhouse' ? /bypass/i.test(x.phase) : x.target === 'api' && !/bypass/i.test(x.phase)));
  const x = find(a.stress), y = find(b.stress);
  p(`| ${y.phase} | ${int(x.dashboard_rps)} | ${trio(x.dashboard_latency)} | ${int(y.dashboard_rps)} | ${trio(y.dashboard_latency)} | ${errs(x)} / ${errs(y)} |`);
}
p();

// ---------------------------------------------------------------- write storm
for (const [title, sel] of [['Write storm: every store writing at 1,000 transactions/s in total, 100 dashboard viewers', 'heavy']]) {
  p(`### ${title}`);
  p();
  p('| Phase | Scale | Store tx/s | Store p50 / p95 / p99 | Dashboard p50 / p95 / p99 | New order visible p50 / p95 / p99 | Ingest events/s |');
  p('|---|---|---:|---:|---:|---:|---:|');
  for (const [label, s] of [['1M', a[sel]], ['10M', b[sel]]]) {
    if (!s) continue;
    for (const ph of s.phases) {
      p(`| ${ph.phase} | ${label} | ${int(ph.store_tps)} | ${trio(ph.store_latency)} | ${trio(ph.dashboard_latency)} | ${ph.key === 'direct' ? 'instant (reads the DB)' : trio(ph.commit_to_dashboard)} | ${ph.ingest_events_per_s ? int(ph.ingest_events_per_s) : '—'} |`);
    }
  }
  p();
}

// ---------------------------------------------------------------- summary numbers for the scorecard
const aggStress = (s) => s.phases.find((x) => x.target === 'api' && !/bypass/i.test(x.phase));
const safeChange = (s) => (s ? (phase(s, 'aggregation').store_latency.p95_ms / phase(s, 'baseline').store_latency.p95_ms - 1) * 100 : NaN);
p('<!-- scorecard inputs');
p(JSON.stringify({
  ladderWorstP95_10M: worstCH, worstGrowth,
  apiP95: [aggStress(a.stress).dashboard_latency.p95_ms, aggStress(b.stress).dashboard_latency.p95_ms],
  freshP95: [a.fresh.commit_to_dashboard_api.p95_ms, b.fresh.commit_to_dashboard_api.p95_ms],
  safety: [safeChange(a.safety), safeChange(b.safety)],
  verify: [a.verify, b.verify],
}));
p('-->');
console.log(out.join('\n'));
