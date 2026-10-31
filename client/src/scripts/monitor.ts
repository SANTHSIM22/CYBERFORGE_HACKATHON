import { get } from '../lib/api';
import { $, compactNum, el, int, ms, pct } from '../lib/format';
import { lineChart, type ChartHandle } from '../lib/charts';
import { loadBench, READ_TEST, renderLadder, renderStress, renderWrites, wireStressButtons, WRITE_TEST } from '../lib/bench';
import { demoAvailable } from '../lib/api';

type Pct = { p50: number; p95: number; p99: number; count: number };
type Stats = {
  api: { all: Pct; cache_hit: Pct; database: Pct; probes: Pct; rps: number; hit_ratio: number; stale_ratio: number; requests: number; series: { t: number; rps: number; p95_ms: number; hit_ratio: number }[] };
  ingesters: { worker: string; events_total: number; events_per_sec: number; flush_p95_ms: number; streams: number; snapshots_running: number; max_slot_lag_bytes: number }[];
  pipeline: {
    tenants: number;
    data_age_p95_ms: number;
    data_age_max_ms: number;
    worst: { slug: string; tier: string; snapshot_state: string; processing_lag_ms: number | null; data_age_ms: number | null; slot_lag_bytes: number; events_applied: number }[];
  };
  store: { available: boolean; tables?: { table: string; rows: number; bytes_on_disk: number; uncompressed_bytes: number }[] };
};

const charts: Record<string, ChartHandle | null> = {};
const clock = new Intl.DateTimeFormat('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit' });
const fmtX = (s: number) => clock.format(new Date(s * 1000));
const TIER: Record<string, string> = { XL: 'Extra large', L: 'Large', M: 'Medium', S: 'Small' };

function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const u = ['KB', 'MB', 'GB', 'TB'];
  let i = -1;
  do {
    n /= 1024;
    i++;
  } while (n >= 1024 && i < u.length - 1);
  return `${n.toFixed(n < 10 ? 1 : 0)} ${u[i]}`;
}

function tile(id: string, value: string, sub: string, status?: 'good' | 'warn' | 'bad') {
  const v = $(`m-${id}`);
  v.classList.remove('skel');
  v.textContent = value;
  v.style.color = status === 'bad' ? 'var(--critical)' : '';
  $(`m-${id}-sub`).textContent = sub;
}

async function chart(id: string, label: string, xs: number[], ys: number[], fmt: (v: number) => string, axis: (v: number) => string, color?: string) {
  if (charts[id]) return charts[id]!.setData(xs, ys);
  charts[id] = await lineChart($(id), { xs, ys, label, tz: 'UTC', daily: false, fmtValue: fmt, fmtAxis: axis, fmtX, height: 180, color });
}

async function tick() {
  let s: Stats;
  const state = $('mon-state');
  const dot = el('span', 'chip-dot');
  try {
    s = await get<Stats>('/v1/admin/stats', 'operator');
  } catch (e) {
    dot.style.background = 'var(--critical)';
    state.replaceChildren(dot, el('span', '', 'API unreachable'));
    return;
  }
  const a = s.api;
  const healthy = s.ingesters.length > 0 && s.pipeline.data_age_max_ms < 30_000;
  dot.style.background = `var(${healthy ? '--good' : '--warning'})`;
  state.replaceChildren(dot, el('span', '', healthy ? 'All pipelines healthy' : s.ingesters.length ? 'Some stores lagging' : 'No ingester reporting'));
  $('updated').textContent = `Updated ${clock.format(new Date())} · every 2 s`;

  tile('p50', a.all.count ? ms(a.all.p50) : '—', a.all.count ? `${int(a.requests)} reads in 60 s` : 'no reads yet — open the dashboard');
  tile('p95', a.all.count ? ms(a.all.p95) : '—', a.database.count ? `computed in ClickHouse: p95 ${ms(a.database.p95)}` : 'target < 200 ms', a.all.p95 > 200 ? 'bad' : 'good');
  tile('p99', a.all.count ? ms(a.all.p99) : '—', a.probes.count
    ? `excludes ${int(a.probes.count)} benchmark reads (cache bypassed, p95 ${ms(a.probes.p95)})`
    : a.cache_hit.count ? `cache hits p99 ${ms(a.cache_hit.p99)}` : '');
  tile('hit', a.requests ? pct(a.hit_ratio * 100, 0) : '—', a.stale_ratio > 0.005
    ? `${pct(a.stale_ratio * 100, 0)} previous version while refreshing (≤ 5 s)`
    : 'keyed by each store data version');
  const eps = s.ingesters.reduce((n, i) => n + i.events_per_sec, 0);
  const streams = s.ingesters.reduce((n, i) => n + i.streams, 0);
  tile('eps', compactNum(Math.round(eps)), `${s.ingesters.length} ingester · ${streams} store streams`);
  const lags = s.pipeline.worst.map((w) => w.processing_lag_ms).filter((x): x is number => x != null);
  tile('lag', lags.length ? ms(Math.min(...lags)) : '—', `data age p95 ${ms(s.pipeline.data_age_p95_ms)} · ${s.pipeline.tenants} stores`, s.pipeline.data_age_max_ms > 30_000 ? 'bad' : 'good');

  const xs = a.series.map((p) => p.t);
  $('c-rps-sub').textContent = `${a.rps.toFixed(1)} req/s average`;
  $('c-p95-sub').textContent = a.all.count ? `p95 ${ms(a.all.p95)} over 60 s` : 'waiting for traffic';
  $('c-hit-sub').textContent = a.requests ? `${pct(a.hit_ratio * 100, 0)} of reads served from cache` : 'waiting for traffic';
  await chart('c-rps', 'Requests / s', xs, a.series.map((p) => p.rps), (v) => `${v.toFixed(0)} req/s`, (v) => v.toFixed(0));
  await chart('c-p95', 'p95', xs, a.series.map((p) => p.p95_ms), (v) => ms(v), (v) => `${v < 10 ? v.toFixed(1) : v.toFixed(0)} ms`);
  await chart('c-hit', 'Hit rate', xs, a.series.map((p) => p.hit_ratio * 100), (v) => `${v.toFixed(0)}%`, (v) => `${v.toFixed(0)}%`, '--good');

  const row = (cells: (string | HTMLElement)[], right: number) => {
    const r = el('tr', 'border-b hairline last:border-0');
    cells.forEach((c, i) => {
      const td = el('td', `py-2 ${i >= right ? 'text-right' : ''}`);
      td.append(c);
      r.appendChild(td);
    });
    return r;
  };
  const stateChip = (st: string) => {
    const c = el('span', 'chip');
    const d = el('span', 'chip-dot');
    d.style.background = `var(${st === 'completed' ? '--good' : '--warning'})`;
    c.append(d, st === 'completed' ? 'streaming' : st);
    return c;
  };
  $('worst').replaceChildren(
    ...s.pipeline.worst.map((w) =>
      row([w.slug, TIER[w.tier] ?? w.tier, stateChip(w.snapshot_state), w.data_age_ms == null ? '—' : ms(w.data_age_ms),
        w.processing_lag_ms == null ? '—' : ms(w.processing_lag_ms), bytes(w.slot_lag_bytes), int(w.events_applied)], 3),
    ),
  );
  $('store').replaceChildren(
    ...(s.store.tables ?? []).filter((t) => t.rows > 10).map((t) =>
      row([t.table, compactNum(t.rows), bytes(t.bytes_on_disk), t.bytes_on_disk ? `${(t.uncompressed_bytes / t.bytes_on_disk).toFixed(1)}×` : '—'], 1),
    ),
  );
  $('ingesters').replaceChildren(
    ...s.ingesters.map((i) => {
      const box = el('div', 'rounded-lg px-3 py-2');
      box.style.background = 'var(--surface-2)';
      box.append(el('div', 'font-medium', i.worker),
        el('div', 'muted tabnum', `${int(i.events_total)} rows · flush p95 ${ms(i.flush_p95_ms)} · ${i.snapshots_running} snapshot(s) · max WAL retained ${bytes(i.max_slot_lag_bytes)}`));
      return box;
    }),
  );
}

async function benchLoop() {
  for (;;) {
    try {
      const b = await loadBench('operator');
      renderStress($('bench-stress'), b.stress);
      renderWrites($('bench-writes'), b.writestorm);
      renderLadder($('bench-ladder'), b.ladder);
    } catch {
      $('bench-stress').textContent = 'Benchmark results unavailable.';
    }
    await new Promise((r) => setTimeout(r, 30_000));
  }
}
benchLoop();
demoAvailable().then((on) => {
  if (!on) return;
  wireStressButtons(
    [
      { btn: $('stress-run') as HTMLButtonElement, body: READ_TEST, idle: '▶ Run stress test', busy: 'Stress test running…' },
      { btn: $('writes-run') as HTMLButtonElement, body: WRITE_TEST, idle: '▶ Run write storm', busy: 'Write storm running…' },
    ],
    $('stress-progress'),
    'operator',
    async () => {
      const b = await loadBench('operator');
      renderStress($('bench-stress'), b.stress);
      renderWrites($('bench-writes'), b.writestorm);
      renderLadder($('bench-ladder'), b.ladder);
    },
  );
});

async function loop() {
  for (;;) {
    if (!document.hidden) await tick();
    await new Promise((r) => setTimeout(r, 2000));
  }
}
loop();
window.addEventListener('tp-theme', () => {
  for (const k of Object.keys(charts)) {
    charts[k]?.destroy();
    charts[k] = null;
  }
});
