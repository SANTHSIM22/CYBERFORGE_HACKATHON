// "Direct primary DB vs aggregation layer" panels, rendered from the newest
// `tpctl stress` / `tpctl ladder` results (stored in ClickHouse by tpctl).
import { get, token } from './api';
import { compactNum, el, int, ms } from './format';

type Pctl = { count: number; p50_ms: number; p95_ms: number; p99_ms: number };
type Phase = {
  phase: string;
  target: string;
  dashboard_requests: number;
  dashboard_rps: number;
  dashboard_latency: Pctl;
  dashboard_errors: Record<string, number> | null;
  per_endpoint: Record<string, Pctl> | null;
  store_oltp_latency: Pctl;
  store_oltp_p95_slowdown_pct: number;
  cache_hit_ratio?: number;
  cache_stale_ratio?: number;
};
type Stress = { vus: number; oltp_rate: number; duration_s: number; tenants: number; phases: Phase[]; source?: string; cpu_pinning?: boolean };
type Ladder = { tenant: string; orders: number; iterations: number; levels: string[]; results: Record<string, { p50_ms: number; p95_ms: number }[]> };
type WPhase = {
  key: string;
  phase: string;
  target: string;
  duration_s: number;
  store_tps: number;
  store_write_tps: number;
  store_latency: Pctl;
  store_write_latency: Pctl;
  store_errors: number;
  store_p95_slowdown_pct: number;
  viewers: number;
  dashboard_requests: number;
  dashboard_rps: number;
  dashboard_latency: Pctl;
  freshness_poll_latency: Pctl;
  dashboard_errors: Record<string, number> | null;
  cache_hit_ratio: number;
  commit_to_dashboard: Pctl;
  commit_to_dashboard_timeouts: number;
  ingest_events_per_s: number;
  ingest_events_per_s_peak: number;
  data_age_p95_ms_max: number;
  catch_up_s?: number;
};
type Writes = { tenants: number; target_tps: number; workers: number; viewers: number; source: string; cpu_pinning: boolean; phases: WPhase[] };
export type Bench = {
  stress: { at: string; result: Stress } | null;
  ladder: { at: string; result: Ladder } | null;
  writestorm?: { at: string; result: Writes } | null;
};

export const loadBench = (subject: string) => get<Bench>('/v1/benchmarks', subject);

const SERIES = [
  { key: 'direct', label: 'Direct: primary DB', color: '--series-2' },
  { key: 'clickhouse', label: 'ClickHouse only (no cache)', color: '--series-3' },
  { key: 'aggregation', label: 'Aggregation layer (ClickHouse + cache)', color: '--series-1' },
] as const;

function pick(s: Stress) {
  const by = { baseline: undefined as Phase | undefined, direct: undefined as Phase | undefined, clickhouse: undefined as Phase | undefined, aggregation: undefined as Phase | undefined };
  for (const p of s.phases) {
    if (!p.target) by.baseline = p;
    else if (p.target === 'api-direct') by.direct = p;
    else if (/bypass/i.test(p.phase)) by.clickhouse = p;
    else by.aggregation = p;
  }
  return by;
}

const times = (a: number, b: number) => (b > 0 ? a / b : 0);
const fx = (x: number) => (x >= 100 ? `${Math.round(x).toLocaleString('en-US')}×` : `${x.toFixed(x >= 10 ? 0 : 1)}×`);
// better(a, b): how b compares with a when lower is better (latency).
const vs = (direct: number, layer: number, lowerIsBetter = true) => {
  const r = lowerIsBetter ? times(direct, layer) : times(layer, direct);
  return r >= 1 ? { text: fx(r) + (lowerIsBetter ? ' faster' : ' more'), good: true } : { text: fx(1 / r) + (lowerIsBetter ? ' slower' : ' less'), good: false };
};
const errCount = (p?: Phase) => Object.values(p?.dashboard_errors ?? {}).reduce((n, v) => n + v, 0);

function stat(label: string, value: string, sub: string, good = true) {
  const c = el('div', 'rounded-xl px-4 py-3');
  c.style.background = 'var(--surface-2)';
  const v = el('div', 'mt-1 text-2xl font-semibold tracking-tight tabnum', value);
  v.style.color = good ? 'var(--good-text)' : 'var(--critical)';
  c.append(el('div', 'ink-2 text-xs font-medium', label), v, el('div', 'muted mt-0.5 text-[11px]', sub));
  return c;
}

export function renderStress(host: HTMLElement, data: Bench['stress'], compact = false) {
  host.replaceChildren();
  if (!data) {
    host.append(el('p', 'muted text-sm', 'No stress test recorded yet. Run: make bench-stress (or .\\tp.ps1 bench-stress), then reload.'));
    return;
  }
  const s = data.result;
  const ph = pick(s);
  const d = ph.direct, a = ph.aggregation, c = ph.clickhouse;
  if (!d || !a) {
    host.append(el('p', 'muted text-sm', 'The latest stress run is missing the direct or aggregation phase.'));
    return;
  }
  // Headline
  const grid = el('div', 'grid grid-cols-2 gap-3 lg:grid-cols-4');
  grid.append(
    stat('p95 latency', vs(d.dashboard_latency.p95_ms, a.dashboard_latency.p95_ms).text, `${ms(d.dashboard_latency.p95_ms)} direct → ${ms(a.dashboard_latency.p95_ms)} aggregation`, vs(d.dashboard_latency.p95_ms, a.dashboard_latency.p95_ms).good),
    stat('p99 latency', vs(d.dashboard_latency.p99_ms, a.dashboard_latency.p99_ms).text, `${ms(d.dashboard_latency.p99_ms)} → ${ms(a.dashboard_latency.p99_ms)}`, vs(d.dashboard_latency.p99_ms, a.dashboard_latency.p99_ms).good),
    stat('Throughput', vs(d.dashboard_rps, a.dashboard_rps, false).text, `${compactNum(Math.round(d.dashboard_rps))} → ${compactNum(Math.round(a.dashboard_rps))} requests/s with ${s.vus} users`, vs(d.dashboard_rps, a.dashboard_rps, false).good),
    stat('Store checkout p95 impact', `${d.store_oltp_p95_slowdown_pct >= 0 ? '+' : ''}${d.store_oltp_p95_slowdown_pct.toFixed(0)}% vs ${a.store_oltp_p95_slowdown_pct >= 0 ? '+' : ''}${a.store_oltp_p95_slowdown_pct.toFixed(0)}%`,
      'direct vs aggregation · PRD target < 10%', a.store_oltp_p95_slowdown_pct < 10),
  );
  host.append(grid);

  // Percentile bars (one shared linear scale per percentile; values always labelled)
  const legend = el('div', 'ink-2 mt-4 flex flex-wrap gap-x-4 gap-y-1 text-[11px]');
  const phases: Record<string, Phase | undefined> = { direct: d, clickhouse: c, aggregation: a };
  for (const sr of SERIES) {
    if (!phases[sr.key]) continue;
    const item = el('span', 'inline-flex items-center gap-1.5');
    const sw = el('span', 'inline-block h-2.5 w-2.5 rounded-sm');
    sw.style.background = `var(${sr.color})`;
    item.append(sw, sr.label);
    legend.append(item);
  }
  host.append(legend);
  const bars = el('div', `mt-3 grid gap-4 ${compact ? 'md:grid-cols-3' : 'lg:grid-cols-3'}`);
  for (const [pk, label] of [['p50_ms', 'p50 (median)'], ['p95_ms', 'p95'], ['p99_ms', 'p99']] as const) {
    const box = el('div');
    box.append(el('div', 'mb-1.5 text-xs font-semibold', `${label} response time`));
    const max = Math.max(...SERIES.map((sr) => phases[sr.key]?.dashboard_latency[pk] ?? 0), 0.001);
    for (const sr of SERIES) {
      const p = phases[sr.key];
      if (!p) continue;
      const v = p.dashboard_latency[pk];
      const row = el('div', 'mb-1.5 grid grid-cols-[1fr_auto] items-center gap-2');
      const track = el('div', 'h-3 rounded-r');
      const fill = el('div', 'h-3');
      fill.style.cssText = `width:${Math.max(0.8, (v / max) * 100)}%;background:var(${sr.color});border-radius:0 4px 4px 0`;
      track.appendChild(fill);
      row.title = `${sr.label}: ${ms(v)}`;
      row.append(track, el('span', 'tabnum w-16 text-right text-xs font-medium', ms(v)));
      box.append(row);
    }
    bars.append(box);
  }
  host.append(bars);

  if (!compact) {
    const table = el('table', 'mt-5 w-full text-sm tabnum');
    const head = el('tr', 'muted border-b hairline text-left text-[11px] uppercase tracking-wide');
    for (const [h, r] of [['Phase', false], ['Req/s', true], ['p50', true], ['p95', true], ['p99', true], ['Cache hits', true], ['Errors', true], ['Store p95', true], ['vs baseline', true]] as const) {
      head.append(el('th', `py-2 font-medium ${r ? 'text-right' : ''}`, h));
    }
    const thead = el('thead');
    thead.append(head);
    const tbody = el('tbody');
    for (const p of s.phases) {
      const r = el('tr', 'border-b hairline last:border-0');
      const has = p.dashboard_requests > 0;
      r.append(
        el('td', 'py-2 pr-2', p.phase),
        el('td', 'py-2 text-right', has ? int(Math.round(p.dashboard_rps)) : '—'),
        el('td', 'py-2 text-right', has ? ms(p.dashboard_latency.p50_ms) : '—'),
        el('td', 'py-2 text-right', has ? ms(p.dashboard_latency.p95_ms) : '—'),
        el('td', 'py-2 text-right', has ? ms(p.dashboard_latency.p99_ms) : '—'),
        Object.assign(el('td', 'py-2 text-right', p.target ? `${Math.round((p.cache_hit_ratio ?? 0) * 100)}%` : '—'), {
          title: p.cache_stale_ratio ? `incl. ${Math.round(p.cache_stale_ratio * 100)}% served from the previous data version (≤ 5 s old) while the new answer was computed` : '',
        }),
        el('td', 'py-2 text-right', p.target ? String(errCount(p)) : '—'),
        el('td', 'py-2 text-right', ms(p.store_oltp_latency.p95_ms)),
        el('td', 'py-2 text-right', p.target ? `${p.store_oltp_p95_slowdown_pct >= 0 ? '+' : ''}${p.store_oltp_p95_slowdown_pct.toFixed(1)}%` : 'baseline'),
      );
      tbody.append(r);
    }
    table.append(thead, tbody);
    const wrap = el('div', 'overflow-x-auto');
    wrap.append(table);
    host.append(wrap);
  }
  const when = new Date(data.at).toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' });
  host.append(el('p', 'muted mt-3 text-[11px]',
    `Run ${when} · ${s.tenants} stores, ${s.vus} concurrent dashboard users with no think time, store checkout workload ${s.oltp_rate} ops/s, ${s.duration_s}s per phase. ` +
    (s.cpu_pinning === false ? `Started from the dashboard: all services share this machine's CPUs, so ClickHouse competes with the load generator. ` : `Store Postgres pinned to its own CPU cores (like a separate server). `) + `Latencies are round trips measured by the load generator.`));
}

const pct = (x: number) => `${x >= 0 ? '+' : ''}${x.toFixed(x > -10 && x < 10 ? 1 : 0)}%`;
const trio = (p: Pctl) => (p.count ? `${ms(p.p50_ms)} / ${ms(p.p95_ms)} / ${ms(p.p99_ms)}` : '—');

/** Write storm: every store writing at a high rate while dashboards watch. */
export function renderWrites(host: HTMLElement, data: Bench['writestorm'], compact = false) {
  host.replaceChildren();
  if (!data) {
    host.append(el('p', 'muted text-sm', 'No write storm recorded yet. Click ▶ Run write storm, or run: make bench-writes.'));
    return;
  }
  const w = data.result;
  const by: Record<string, WPhase | undefined> = {};
  for (const p of w.phases) by[p.key] = p;
  const ref = by.pipeline_off ?? by.baseline;
  const a = by.aggregation, d = by.direct;
  if (!a || !ref) {
    host.append(el('p', 'muted text-sm', 'The latest write storm is missing a phase.'));
    return;
  }
  const refName = by.pipeline_off ? 'with the analytics pipeline off' : 'with nobody viewing';
  const grid = el('div', 'grid grid-cols-2 gap-3 lg:grid-cols-4');
  grid.append(
    stat('Store writes sustained', `${int(Math.round(a.store_write_tps))}/s`,
      `${int(Math.round(a.store_tps))} store transactions/s across ${w.tenants} stores (target ${int(w.target_tps)})`, a.store_tps >= 0.9 * w.target_tps),
    stat('Store p95 with dashboards open', pct(a.store_p95_slowdown_pct),
      `vs ${refName}${d ? ` · direct-to-DB dashboards: ${pct(d.store_p95_slowdown_pct)}` : ''} · PRD < 10%`, a.store_p95_slowdown_pct < 10),
    stat('New order on the dashboard', a.commit_to_dashboard.count ? ms(a.commit_to_dashboard.p95_ms) : '—',
      `p95 from commit · p50 ${ms(a.commit_to_dashboard.p50_ms)} · p99 ${ms(a.commit_to_dashboard.p99_ms)} · PRD < 30 s`, (a.commit_to_dashboard.p95_ms || 0) < 30_000),
    stat('Dashboard p95 during the storm', ms(a.dashboard_latency.p95_ms),
      `p99 ${ms(a.dashboard_latency.p99_ms)}${d ? ` · direct-to-DB: ${ms(d.dashboard_latency.p95_ms)}` : ''} · PRD < 200 ms`, a.dashboard_latency.p95_ms < 200),
  );
  host.append(grid);
  if (!compact) {
    const table = el('table', 'mt-5 w-full text-sm tabnum');
    const head = el('tr', 'muted border-b hairline text-left text-[11px] uppercase tracking-wide');
    for (const [h, r] of [['Phase', false], ['Store tx/s', true], ['Store p50 / p95 / p99', true], ['vs first', true], ['Dashboard p50 / p95 / p99', true],
      ['Order visible p50 / p95 / p99', true], ['Ingest events/s', true], ['Catch-up', true]] as const) {
      head.append(el('th', `py-2 font-medium ${r ? 'text-right' : ''}`, h));
    }
    const thead = el('thead');
    thead.append(head);
    const tbody = el('tbody');
    for (const p of w.phases) {
      const r = el('tr', 'border-b hairline last:border-0');
      r.append(
        el('td', 'py-2 pr-2', p.phase),
        el('td', 'py-2 text-right', int(Math.round(p.store_tps))),
        el('td', 'py-2 text-right', trio(p.store_latency)),
        el('td', 'py-2 text-right', p === w.phases[0] ? 'baseline' : pct(p.store_p95_slowdown_pct)),
        el('td', 'py-2 text-right', trio(p.dashboard_latency)),
        el('td', 'py-2 text-right', p.key === 'direct' ? 'instant (reads the DB)' : trio(p.commit_to_dashboard)),
        el('td', 'py-2 text-right', p.ingest_events_per_s ? int(Math.round(p.ingest_events_per_s)) : '—'),
        el('td', 'py-2 text-right', p.catch_up_s ? `${p.catch_up_s.toFixed(1)} s` : '—'),
      );
      tbody.append(r);
    }
    table.append(thead, tbody);
    const wrap = el('div', 'overflow-x-auto');
    wrap.append(table);
    host.append(wrap);
  }
  const when = new Date(data.at).toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' });
  host.append(el('p', 'muted mt-3 text-[11px]',
    `Run ${when} · ${w.tenants} stores taking checkouts, status changes and cancellations at ${int(w.target_tps)} transactions/s in total, ` +
    `while ${w.viewers} dashboard viewers each load the four widgets, poll freshness every second and reload when their store's data changes. ` +
    `"Order visible" commits a marker order twice a second and times until the dashboard can show it. ` +
    (w.cpu_pinning ? 'Store Postgres pinned to its own CPU cores. ' : 'All services share this machine\'s CPUs. ')));
}

export function renderLadder(host: HTMLElement, data: Bench['ladder']) {
  host.replaceChildren();
  if (!data) {
    host.append(el('p', 'muted text-sm', 'No ladder run recorded yet. Run: make bench-ladder.'));
    return;
  }
  const l = data.result;
  const table = el('table', 'w-full text-sm tabnum');
  const head = el('tr', 'muted border-b hairline text-left text-[11px] uppercase tracking-wide');
  head.append(el('th', 'py-2 font-medium', 'Query (single user, p50 / p95)'));
  for (const lv of ['No index', 'B-tree index', 'Covering index', 'ClickHouse']) head.append(el('th', 'py-2 text-right font-medium', lv));
  const thead = el('thead');
  thead.append(head);
  const tbody = el('tbody');
  for (const [q, r] of Object.entries(l.results)) {
    const row = el('tr', 'border-b hairline last:border-0');
    row.append(el('td', 'py-2', q));
    const best = Math.min(...r.map((x) => x.p50_ms));
    for (const x of r) {
      const td = el('td', 'py-2 text-right', `${ms(x.p50_ms)} / ${ms(x.p95_ms)}`);
      if (x.p50_ms === best) td.classList.add('font-semibold');
      row.append(td);
    }
    tbody.append(row);
  }
  table.append(thead, tbody);
  const wrap = el('div', 'overflow-x-auto');
  wrap.append(table);
  host.append(wrap, el('p', 'muted mt-2 text-[11px]',
    `${l.tenant} (${int(l.orders)} orders) · ${l.iterations} runs per cell, one query at a time · bold = fastest. ` +
    `Single queries on an idle database: this isolates query speed. The stress test above shows what happens under concurrent load.`));
}

// ---------------------------------------------------------------- "Run stress test" button

type StressStatus = {
  running: boolean;
  error: string;
  run_id: string;
  scenario?: string;
  progress: { phase: string; phase_index: number; phase_count: number; stage: string; phase_elapsed_s: number; phase_total_s: number; completed: (Phase | WPhase)[] | null } | null;
};

async function stressCall(method: 'GET' | 'POST', subject: string, body?: unknown): Promise<{ status: number; json: any }> {
  const r = await fetch('/demo/stress', {
    method,
    headers: { Authorization: 'Bearer ' + (await token(subject)), 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  let json: any = null;
  try {
    json = await r.json();
  } catch {}
  return { status: r.status, json };
}

function renderProgress(host: HTMLElement, s: StressStatus) {
  host.replaceChildren();
  const p = s.progress;
  if (!p) {
    host.append(el('div', 'muted text-xs', 'Preparing store workload and connections…'));
    return;
  }
  const frac = p.stage === 'done' ? 1 : Math.min(1, (p.phase_index + Math.min(1, p.phase_elapsed_s / Math.max(p.phase_total_s, 1))) / Math.max(p.phase_count, 1));
  const head = el('div', 'flex flex-wrap items-baseline justify-between gap-2 text-xs');
  const stage = p.stage === 'warming_up' ? 'warming up' : 'measuring';
  head.append(
    el('span', 'font-medium', p.stage === 'done' ? 'All phases finished, publishing results…' : `Phase ${p.phase_index + 1} of ${p.phase_count}: ${p.phase}`),
    el('span', 'muted tabnum', p.stage === 'done' ? '' : `${stage} · ${Math.floor(p.phase_elapsed_s)} / ${Math.round(p.phase_total_s)} s`),
  );
  const track = el('div', 'mt-2 h-2 w-full overflow-hidden rounded-full');
  track.style.background = 'var(--surface-3)';
  const fill = el('div', 'h-2 rounded-full');
  fill.style.cssText = `width:${(frac * 100).toFixed(1)}%;background:var(--accent);transition:width .8s linear`;
  track.append(fill);
  host.append(head, track);
  const done = p.completed ?? [];
  if (done.length) {
    const list = el('div', 'mt-2 flex flex-wrap gap-2');
    for (const any of done) {
      if ('store_tps' in any) {
        const w = any as WPhase;
        const bits = [`${int(Math.round(w.store_write_tps))} writes/s`, `store p95 ${ms(w.store_latency.p95_ms)}`];
        if (w.dashboard_latency.count) bits.push(`dashboard p95 ${ms(w.dashboard_latency.p95_ms)}`);
        if (w.commit_to_dashboard.count) bits.push(`order visible p95 ${ms(w.commit_to_dashboard.p95_ms)}`);
        if (w.catch_up_s) bits.push(`caught up in ${w.catch_up_s.toFixed(1)} s`);
        list.append(el('span', 'chip tabnum', `✓ ${w.phase.split(':')[0]}: ${bits.join(' · ')}`));
        continue;
      }
      const ph = any as Phase;
      const name = ph.phase.split(':')[0].split(' (')[0];
      const slow = `${ph.store_oltp_p95_slowdown_pct >= 0 ? '+' : ''}${ph.store_oltp_p95_slowdown_pct.toFixed(0)}%`;
      const txt = ph.target
        ? `${name}: p95 ${ms(ph.dashboard_latency.p95_ms)} · ${Math.round(ph.dashboard_rps)} req/s · store ${slow}`
        : `Baseline: store p95 ${ms(ph.store_oltp_latency.p95_ms)}`;
      list.append(el('span', 'chip tabnum', '✓ ' + txt));
    }
    host.append(list);
  }
}

export type TestButton = { btn: HTMLButtonElement; body: Record<string, unknown>; idle: string; busy: string };

/** The dashboard stress test (closed-loop readers) and the write storm. */
export const READ_TEST = { vus: 20, duration_s: 15 };
export const WRITE_TEST = { scenario: 'writes', duration_s: 20, rate: 1000, viewers: 100 };

/** Wires the test buttons (one run at a time): starts a run or resumes watching one, and refreshes the panels when it finishes. */
export function wireStressButtons(buttons: TestButton[], host: HTMLElement, subject: string, onDone: () => void) {
  for (const b of buttons) b.btn.classList.remove('hidden');
  let polling = false;
  const setBusy = (busy: boolean, scenario?: string) => {
    for (const b of buttons) {
      b.btn.disabled = busy;
      const mine = (b.body.scenario ?? 'reads') === (scenario ?? 'reads');
      b.btn.textContent = busy && mine ? b.busy : b.idle;
    }
  };
  const poll = async () => {
    if (polling) return;
    polling = true;
    setBusy(true);
    host.classList.remove('hidden');
    for (;;) {
      let s: StressStatus | null = null;
      try {
        s = (await stressCall('GET', subject)).json as StressStatus;
      } catch {}
      if (s) {
        setBusy(!!s.running, s.scenario);
        renderProgress(host, s);
      }
      if (s && !s.running) {
        polling = false;
        setBusy(false);
        if (s.error) {
          const msg = el('div', 'text-xs', `Test failed: ${s.error}`);
          msg.style.color = 'var(--critical)';
          host.replaceChildren(msg);
        } else {
          host.classList.add('hidden');
          onDone();
        }
        return;
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
  };
  for (const b of buttons) {
    b.btn.addEventListener('click', async () => {
      setBusy(true, b.body.scenario as string | undefined);
      let res = { status: 0, json: null as any };
      try {
        res = await stressCall('POST', subject, b.body);
      } catch {}
      if (res.status === 202 || res.status === 409) return poll();
      setBusy(false);
      host.classList.remove('hidden');
      host.replaceChildren(el('div', 'text-xs', `Could not start: ${res.json?.detail ?? res.json?.error ?? 'demo service unavailable'}`));
    });
  }
  // Resume watching if a run is already in progress (e.g. after a page reload).
  stressCall('GET', subject)
    .then(({ json }) => {
      if ((json as StressStatus | null)?.running) poll();
    })
    .catch(() => {});
}
