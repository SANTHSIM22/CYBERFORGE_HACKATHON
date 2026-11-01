import { ApiError, demo, demoAvailable, directory, get, stream, type Envelope, type OrgSubject, type Subject } from '../lib/api';
import { $, ago, compactNum, cssVar, currencySymbol, el, int, money, ms, pct, toast } from '../lib/format';
import { compactTick, lineChart, sparkline, type ChartHandle } from '../lib/charts';
import { icon } from '../lib/icons';

type Totals = { currency: string; orders: number; gross_sales: string; recognized_sales: string };
type OrgStore = { slug: string; name: string; timezone: string; currency: string; orders: number; gross_sales: string; recognized_sales: string };
type ByCurrency = { currency: string; orders: number; gross_sales: string; gross_sales_converted: string };
type FX = { reporting_currency: string; rates_as_of: string; units_per_usd: Record<string, string>; missing_rates: string[] };
type Summary = {
  totals: Totals[]; total_orders: number; timezone: string; from: string; to: string;
  previous: { totals: Totals[]; total_orders: number; from: string; to: string };
  stores?: OrgStore[]; by_currency?: ByCurrency[]; fx?: FX; // org dashboards
};
type Point = { bucket: string; orders: number; gross_sales: string; recognized_sales: string };
type Trends = { points: Point[]; currency: string; grain: 'day' | 'hour'; timezone: string };
type StatusRow = { status: string; orders: number; recognized: boolean };
type Breakdown = { statuses: StatusRow[]; total_orders: number };
type Product = { rank: number; product_id: string; name: string; sku: string | null; quantity: string; revenue: string; store_name?: string };
type Top = { products: Product[]; currency: string; limit: number; offset: number; total: number; next_offset: number | null };
type Fresh = {
  scope: string; status: string; data_as_of: string | null; last_source_commit_at: string | null; fresh_within_ms?: number;
  data_version: string; server_time: string; snapshot_state?: string; stores?: number; stores_behind?: number;
};
type TenantInfo = { slug: string; name: string; tier?: string; status: string; timezone?: string; default_currency: string; currencies: string[]; stores?: unknown[]; reporting_currency?: string };
type Metric = 'orders' | 'gross' | 'recognized';
type Scope = 'store' | 'org';

const RANGES = ['today', '7d', '30d', '90d', 'custom'];
const TIER: Record<string, string> = { XL: 'Extra-large store', L: 'Large store', M: 'Medium store', S: 'Small store' };

const state = {
  tenants: [] as Subject[],
  orgs: [] as OrgSubject[],
  kind: 'store' as Scope, // what the page shows: one store, or an org across its stores
  slug: '',
  as: '', // the identity it reads with: tenant:<slug>, or org:<slug> (an org admin, also when opening one of its stores)
  parentOrg: null as OrgSubject | null, // set while an org admin looks at one of the org's stores
  topN: 10,
  topOffset: 0,
  info: null as TenantInfo | null,
  range: '7d',
  from: '',
  to: '',
  currency: '',
  grain: 'day' as 'day' | 'hour',
  metric: 'orders' as Metric,
  view: 'chart' as 'chart' | 'table',
  version: '',
  trend: null as Trends | null,
  chart: null as ChartHandle | null,
  chartKey: '',
  pending: null as null | { label: string; t0: number },
  loading: false,
  again: false,
  sawStale: false, // a widget got the previous data version; refresh once the new one is ready
  firstLoad: true,
};

const subject = () => state.as;
const base = () => (state.kind === 'org' ? `/v1/orgs/${state.slug}` : `/v1/tenants/${state.slug}`);
const scopeKey = () => (state.kind === 'org' ? 'org:' : 'tenant:') + state.slug;
const viewerTZ = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';

function qs(extra: Record<string, string> = {}): string {
  const p = new URLSearchParams();
  if (state.range === 'custom') {
    p.set('from', state.from);
    p.set('to', state.to);
  } else p.set('range', state.range);
  if (state.kind === 'org') p.set('tz', viewerTZ); // org days follow the viewer's clock
  for (const [k, v] of Object.entries(extra)) if (v) p.set(k, v);
  if (state.version) p.set('min_version', state.version); // never show data older than the freshness badge
  return '?' + p.toString();
}

function latency(id: string, env: { meta: Envelope<unknown>['meta']; rttMs: number }) {
  const n = $(id);
  const c = env.meta.cache;
  const hit = c === 'HIT' || c === 'STALE';
  if (c === 'STALE') state.sawStale = true;
  const dot = el('span', 'chip-dot');
  dot.style.background = `var(${hit ? '--good' : '--series-3'})`;
  const computed = env.meta.source === 'view' ? 'computed in memory' : 'computed in ClickHouse';
  const how = c === 'HIT' ? 'served from cache' : c === 'STALE' ? 'from cache · refreshing' : c === 'MISS' ? computed : 'cache bypassed';
  n.replaceChildren(dot, el('span', 'tabnum', `${ms(env.meta.took_ms)} · ${how}`));
  const origin = env.meta.source === 'view' ? 'in-memory rollups' : env.meta.source === 'clickhouse' ? 'ClickHouse' : '';
  n.title = `Server time ${ms(env.meta.took_ms)} · browser round trip ${ms(env.rttMs)} · data version ${env.meta.data_version}`
    + (origin ? ` · computed from ${origin}` : '');
}

// ------------------------------------------------------------------ init

async function init() {
  wireControls();
  try {
    ({ tenants: state.tenants, orgs: state.orgs } = await directory());
  } catch {
    banner('Cannot reach the API. Start the stack with docker compose up -d, then reload.');
    renderFresh(null);
    return;
  }
  // Each option is an identity: an org admin (the org's consolidated
  // dashboard) or a store owner (that store only).
  const sel = $('tenant') as HTMLSelectElement;
  if (state.orgs.length) {
    const g = el('optgroup');
    g.label = 'Organizations (sign in as org admin)';
    for (const o of state.orgs) {
      const opt = el('option', '', `${o.name} · ${o.stores} stores · ${o.reporting_currency}`);
      opt.value = o.subject;
      g.appendChild(opt);
    }
    sel.appendChild(g);
  }
  for (const tier of ['XL', 'L', 'M', 'S']) {
    const list = state.tenants.filter((t) => t.tier === tier);
    if (!list.length) continue;
    const g = el('optgroup');
    g.label = `${TIER[tier]}s (sign in as store owner)`;
    for (const t of list) {
      const o = el('option', '', `${t.name} (${t.slug})${t.status !== 'active' ? ` · ${t.status}` : ''}`);
      o.value = t.subject;
      g.appendChild(o);
    }
    sel.appendChild(g);
  }
  const url = new URL(location.href);
  const wantedOrg = url.searchParams.get('org');
  const wanted = url.searchParams.get('tenant');
  if (wantedOrg && state.orgs.some((o) => o.slug === wantedOrg)) {
    state.kind = 'org';
    state.slug = wantedOrg;
  } else {
    state.slug = state.tenants.some((t) => t.slug === wanted) ? wanted! : state.tenants.find((t) => t.tier === 'XL')?.slug ?? state.tenants[0]?.slug ?? '';
  }
  state.as = (state.kind === 'org' ? 'org:' : 'tenant:') + state.slug;
  const r = url.searchParams.get('range') ?? '7d';
  state.range = RANGES.includes(r) && r !== 'custom' ? r : '7d';
  if (r === 'custom' && url.searchParams.get('from') && url.searchParams.get('to')) {
    state.range = 'custom';
    state.from = url.searchParams.get('from')!;
    state.to = url.searchParams.get('to')!;
  }
  markRange();
  sel.value = state.as;
  sel.addEventListener('change', () => {
    const [kind, slug] = sel.value.startsWith('org:') ? ['org', sel.value.slice(4)] : ['store', sel.value.slice(7)];
    selectScope(kind as Scope, slug, sel.value);
  });
  $('back-org').addEventListener('click', () => {
    if (state.parentOrg) selectScope('org', state.parentOrg.slug, state.parentOrg.subject);
  });
  demoOn = await demoAvailable();
  if (demoOn) wireDemo();
  if (!state.slug) {
    banner('No stores yet. Seed some with: docker compose --profile tools run --rm tpctl seed');
    return;
  }
  await selectScope(state.kind, state.slug, state.as);
  watch();
  window.addEventListener('tp-theme', () => {
    state.chartKey = '';
    renderTrend();
    renderSparks();
  });
}

function syncURL() {
  const u = new URL(location.href);
  u.searchParams.delete(state.kind === 'org' ? 'tenant' : 'org');
  u.searchParams.set(state.kind === 'org' ? 'org' : 'tenant', state.slug);
  u.searchParams.set('range', state.range);
  if (state.range === 'custom') {
    u.searchParams.set('from', state.from);
    u.searchParams.set('to', state.to);
  } else {
    u.searchParams.delete('from');
    u.searchParams.delete('to');
  }
  history.replaceState(null, '', u);
}

function markRange() {
  for (const b of $('range').querySelectorAll<HTMLButtonElement>('button')) b.setAttribute('aria-pressed', String(b.dataset.range === state.range));
  const form = $('custom');
  const custom = state.range === 'custom';
  form.classList.toggle('hidden', !custom);
  form.classList.toggle('flex', custom);
}

let demoOn = false;

// selectScope shows one store or one org, read with the identity `as`. An org
// admin opening one of its stores keeps its org identity: that is what lets it
// read the store (role-based access), and the back button returns to the org.
async function selectScope(kind: Scope, slug: string, as: string) {
  state.parentOrg = kind === 'store' && as.startsWith('org:') ? state.orgs.find((o) => o.subject === as) ?? null : null;
  state.kind = kind;
  state.slug = slug;
  state.as = as;
  state.version = '';
  state.topOffset = 0;
  watching?.abort(); // reopen the freshness stream for the new scope
  state.firstLoad = true;
  syncURL();
  try {
    state.info = await get<TenantInfo>(base(), subject());
  } catch (e) {
    banner(`Cannot open ${slug}: ${(e as Error).message}`);
    return;
  }
  banner(null);
  const org = kind === 'org';
  const cur = $('currency') as HTMLSelectElement;
  cur.replaceChildren(...state.info.currencies.map((c) => Object.assign(el('option', '', `${c} ${currencySymbol(c)}`), { value: c })));
  state.currency = state.info.currencies.includes(state.info.default_currency) ? state.info.default_currency : state.info.currencies[0];
  cur.value = state.currency;
  cur.disabled = org; // org money is converted into one reporting currency
  $('tenant-tier').textContent = org ? `${state.info.stores?.length ?? 0} stores` : TIER[state.info.tier ?? ''] ?? state.info.tier ?? '';
  $('role').textContent = state.as === 'operator' ? 'Superuser' : state.as.startsWith('org:') ? 'Org admin' : 'Store owner';
  $('tz').textContent = org
    ? `Days in your time (${viewerTZ}) · converted to ${state.info.reporting_currency}`
    : `${state.info.timezone} · ${state.info.currencies.join(', ')}`;
  $('back-org').classList.toggle('hidden', !state.parentOrg);
  if (state.parentOrg) $('back-org-name').textContent = state.parentOrg.name;
  $('org-card').classList.toggle('hidden', !org);
  $('demo').classList.toggle('hidden', org || !demoOn); // the live demo writes into one store's database
  $('demo').classList.toggle('flex', !org && demoOn);
  setGrainForRange();
  await refresh();
}

function wireControls() {
  $('range').addEventListener('click', (ev) => {
    const b = (ev.target as HTMLElement).closest<HTMLButtonElement>('button');
    if (!b) return;
    if (b.dataset.range === 'custom') {
      state.range = 'custom';
      markRange();
      const toInput = $('to') as HTMLInputElement;
      const fromInput = $('from') as HTMLInputElement;
      const today = new Date().toISOString().slice(0, 10);
      toInput.value ||= today;
      fromInput.value ||= new Date(Date.now() - 13 * 864e5).toISOString().slice(0, 10);
      fromInput.focus();
      return;
    }
    state.range = b.dataset.range!;
    state.topOffset = 0;
    markRange();
    setGrainForRange();
    syncURL();
    refresh();
  });
  $('custom').addEventListener('submit', (ev) => {
    ev.preventDefault();
    const from = ($('from') as HTMLInputElement).value;
    const toIncl = ($('to') as HTMLInputElement).value;
    if (!from || !toIncl || toIncl < from) return toast('Pick a start date on or before the end date', 'bad');
    const next = new Date(toIncl + 'T00:00:00Z');
    next.setUTCDate(next.getUTCDate() + 1);
    state.range = 'custom';
    state.from = from; // store-local dates; the API resolves them in the store's timezone
    state.to = next.toISOString().slice(0, 10);
    state.topOffset = 0;
    setGrainForRange();
    syncURL();
    refresh();
  });
  $('currency').addEventListener('change', (ev) => {
    state.currency = (ev.target as HTMLSelectElement).value;
    state.topOffset = 0;
    refresh();
  });
  // Top N: N is the page size; Previous and Next page through the full ranking.
  const reloadTop = () => loadTop().catch((e) => toast(`Top products failed: ${(e as Error).message}`, 'bad'));
  $('top-n').addEventListener('change', (ev) => {
    state.topN = Number((ev.target as HTMLSelectElement).value);
    state.topOffset = 0;
    reloadTop();
  });
  $('top-prev').addEventListener('click', () => {
    state.topOffset = Math.max(0, state.topOffset - state.topN);
    reloadTop();
  });
  $('top-next').addEventListener('click', () => {
    state.topOffset += state.topN;
    reloadTop();
  });
  $('metric').addEventListener('click', (ev) => {
    const b = (ev.target as HTMLElement).closest<HTMLButtonElement>('button');
    if (!b) return;
    pressed('metric', b);
    state.metric = b.dataset.metric as Metric;
    state.chartKey = '';
    renderTrend();
  });
  $('grain').addEventListener('click', (ev) => {
    const b = (ev.target as HTMLElement).closest<HTMLButtonElement>('button');
    if (!b || b.disabled) return;
    pressed('grain', b);
    state.grain = b.dataset.grain as 'day' | 'hour';
    state.chartKey = '';
    loadTrend().catch((e) => toast(`Trend failed: ${(e as Error).message}`, 'bad'));
  });
  for (const [id, view] of [['view-chart', 'chart'], ['view-table', 'table']] as const) {
    $(id).addEventListener('click', () => {
      state.view = view;
      $('view-chart').setAttribute('aria-pressed', String(view === 'chart'));
      $('view-table').setAttribute('aria-pressed', String(view === 'table'));
      $('trend-chart').classList.toggle('hidden', view !== 'chart');
      $('trend-table').classList.toggle('hidden', view !== 'table');
      state.chartKey = '';
      renderTrend();
    });
  }
}

function pressed(group: string, btn: HTMLElement) {
  for (const b of $(group).querySelectorAll('button')) b.setAttribute('aria-pressed', String(b === btn));
}

function rangeDays(): number {
  if (state.range === 'custom') return (Date.parse(state.to) - Date.parse(state.from)) / 864e5;
  return { today: 1, '7d': 7, '30d': 30, '90d': 90 }[state.range] ?? 7;
}

function setGrainForRange() {
  const days = rangeDays();
  const hourBtn = $('grain').querySelector<HTMLButtonElement>('[data-grain="hour"]')!;
  hourBtn.disabled = days > 31;
  hourBtn.title = hourBtn.disabled ? 'Hourly is available for ranges up to 31 days' : '';
  state.grain = days <= 2 ? 'hour' : 'day';
  for (const b of $('grain').querySelectorAll('button')) b.setAttribute('aria-pressed', String(b.getAttribute('data-grain') === state.grain));
  state.chartKey = '';
}

function banner(text: string | null) {
  const n = $('notready');
  if (!text) return n.classList.add('hidden');
  n.textContent = text;
  n.classList.remove('hidden');
}

// ------------------------------------------------------------------ data

const LOADERS = ['kpis', 'trend-card', 'status-bars', 'top-body'];

async function refresh(): Promise<void> {
  if (!state.slug) return;
  if (state.loading) {
    state.again = true; // a click during a load is never dropped
    return;
  }
  state.loading = true;
  state.sawStale = false;
  if (!state.firstLoad) for (const id of LOADERS) $(id).dataset.loading = 'true';
  try {
    await Promise.all([loadSummary(), loadTrend(), loadBreakdown(), loadTop()]);
    banner(null);
    state.firstLoad = false;
  } catch (e) {
    if (e instanceof ApiError && e.status === 409) {
      banner(`This store is still ${e.body?.tenant_status ?? 'provisioning'}: its initial snapshot is loading. Numbers appear automatically when it completes.`);
    } else banner(`Could not load data: ${(e as Error).message}. Check that the API is running (docker compose ps).`);
  } finally {
    state.loading = false;
    for (const id of LOADERS) $(id).dataset.loading = 'false';
  }
  if (state.again) {
    state.again = false;
    return refresh();
  }
}

let summary: Summary | null = null;

async function loadSummary() {
  const env = await get<Envelope<Summary>>(`${base()}/orders/summary${qs()}`, subject());
  summary = env.data;
  const s = env.data;
  const c = state.currency;
  const zero = { currency: c, orders: 0, gross_sales: '0', recognized_sales: '0' };
  const cur = s.totals.find((x) => x.currency === c) ?? zero;
  const prev = s.previous.totals.find((x) => x.currency === c) ?? zero;
  const others = s.totals.filter((x) => x.currency !== c);
  const aov = cur.orders ? Number(cur.gross_sales) / cur.orders : 0;
  const prevAov = prev.orders ? Number(prev.gross_sales) / prev.orders : 0;

  setKPI('orders', compactNum(s.total_orders), int(s.total_orders), s.total_orders, s.previous.total_orders,
    others.length ? `${int(cur.orders)} in ${c} · ${int(s.total_orders - cur.orders)} in other currencies` : 'every order created in the range');
  setKPI('gross', money(cur.gross_sales, c, { compact: true }), money(cur.gross_sales, c), Number(cur.gross_sales), Number(prev.gross_sales),
    others.length ? `+ ${others.map((o) => money(o.gross_sales, o.currency, { compact: true })).join(' · ')} in other currencies` : 'includes pending, cancelled and refunded');
  setKPI('recognized', money(cur.recognized_sales, c, { compact: true }), money(cur.recognized_sales, c), Number(cur.recognized_sales), Number(prev.recognized_sales),
    Number(cur.gross_sales) ? `${pct((Number(cur.recognized_sales) / Number(cur.gross_sales)) * 100)} of gross` : 'placed → delivered');
  setKPI('aov', money(aov, c), money(aov, c), aov, prevAov, `gross ÷ ${int(cur.orders)} ${c} orders`);
  const fmt = new Intl.DateTimeFormat('en-US', { timeZone: s.timezone, month: 'short', day: 'numeric' });
  const span = (f: string, t: string) => `${fmt.format(new Date(f))} – ${fmt.format(new Date(Date.parse(t) - 1))}`;
  $('period').textContent = `${span(s.from, s.to)} · change vs ${span(s.previous.from, s.previous.to)}`;
  latency('lat-summary', env);
  if (state.kind === 'org') renderOrg(s);
}

// renderOrg shows where the org totals come from: each store's share (opening
// a store reads it as this org's admin), the exact amounts per currency, and
// the fixed rates used to convert them.
function renderOrg(s: Summary) {
  const c = s.totals[0]?.currency ?? state.currency;
  const stores = s.stores ?? [];
  const total = stores.reduce((n, st) => n + Number(st.gross_sales), 0) || 1;
  $('org-sub').textContent = `${stores.length} stores with orders in this range · gross converted to ${c}`;
  $('org-gross-h').textContent = `Gross (${c})`;
  $('org-stores').replaceChildren(
    ...stores.map((st) => {
      const r = el('tr', 'border-b hairline last:border-0');
      const name = el('td', 'max-w-0 py-2 pr-3');
      const open = el('button', 'truncate text-left font-medium underline-offset-2 hover:underline', st.name);
      open.type = 'button';
      open.title = `Open ${st.name} as this org's admin`;
      open.addEventListener('click', () => selectScope('store', st.slug, state.as));
      name.append(open, el('div', 'muted truncate text-[11px]', `${st.slug} · ${st.timezone} · sells in ${st.currency}`));
      const share = (Number(st.gross_sales) / total) * 100;
      const shareCell = el('td', 'hidden py-2 md:table-cell');
      const wrap = el('div', 'flex items-center gap-2');
      const track = el('div', 'h-1.5 flex-1 rounded-full');
      track.style.background = 'var(--surface-2)';
      const fill = el('div', 'h-1.5 rounded-full');
      fill.style.cssText = `width:${share}%;background:var(--series-1)`;
      track.appendChild(fill);
      wrap.append(track, el('span', 'muted tabnum w-12 text-right text-xs', pct(share)));
      shareCell.appendChild(wrap);
      r.append(name, el('td', 'tabnum py-2 pr-3 text-right', int(st.orders)), el('td', 'tabnum py-2 pr-3 text-right font-semibold whitespace-nowrap', money(st.gross_sales, c)), shareCell);
      return r;
    }),
  );
  $('org-currencies').replaceChildren(
    ...(s.by_currency ?? []).map((b) => {
      const row = el('div', 'flex items-baseline justify-between gap-2');
      row.title = `${int(b.orders)} orders in ${b.currency}`;
      row.append(el('span', 'tabnum font-medium', money(b.gross_sales, b.currency)), el('span', 'muted tabnum text-xs', b.currency === c ? '' : `≈ ${money(b.gross_sales_converted, c)}`));
      return row;
    }),
  );
  const fx = s.fx;
  if (fx) {
    const rates = Object.entries(fx.units_per_usd).filter(([k]) => k !== 'USD').map(([k, v]) => `${k} ${Number(v)}`).join(' · ');
    $('org-fx').textContent = `Converted at fixed rates per 1 USD (as of ${fx.rates_as_of}): ${rates}. Converted totals are approximate; the amounts above are exact.`
      + (fx.missing_rates.length ? ` No rate for ${fx.missing_rates.join(', ')}: left out of converted totals.` : '');
  }
}

function setKPI(id: string, value: string, full: string, now: number, before: number, sub: string) {
  const v = $(`kpi-${id}`);
  v.classList.remove('skel');
  v.textContent = value;
  v.title = full;
  $(`kpi-${id}-sub`).textContent = sub;
  const d = $(`kpi-${id}-delta`);
  if (!before) {
    d.classList.add('hidden');
    return;
  }
  const change = ((now - before) / before) * 100;
  d.className = `chip tabnum shrink-0 ${change >= 0 ? 'delta-up' : 'delta-down'}`;
  d.replaceChildren(icon(change >= 0 ? 'trending-up' : 'trending-down', 12), el('span', '', pct(Math.abs(change))));
  d.title = `Previous period: ${id === 'orders' ? int(before) : money(before, state.currency)}`;
}

function renderSparks() {
  const pts = state.trend?.points ?? [];
  if (!pts.length) return;
  const gross = pts.map((p) => Number(p.gross_sales));
  sparkline($('kpi-orders-spark'), pts.map((p) => p.orders));
  sparkline($('kpi-gross-spark'), gross);
  sparkline($('kpi-recognized-spark'), pts.map((p) => Number(p.recognized_sales)));
  sparkline($('kpi-aov-spark'), pts.map((p, i) => (p.orders ? gross[i] / p.orders : 0)));
}

async function loadTrend() {
  const env = await get<Envelope<Trends>>(`${base()}/orders/trends${qs({ grain: state.grain, currency: state.currency })}`, subject());
  state.trend = env.data;
  latency('lat-trends', env);
  renderTrend();
  renderSparks();
}

function renderTrend() {
  const tr = state.trend;
  if (!tr) return;
  const per = tr.grain === 'day' ? 'day' : 'hour';
  const label = { orders: 'Orders', gross: 'Gross sales', recognized: 'Recognized sales' }[state.metric];
  $('trend-title').textContent = `${label} per ${per}`;
  const total = tr.points.reduce((n, p) => n + (state.metric === 'orders' ? p.orders : Number(state.metric === 'gross' ? p.gross_sales : p.recognized_sales)), 0);
  const peak = tr.points.reduce((best, p) => (p.orders > best.orders ? p : best), tr.points[0] ?? { orders: 0, bucket: '' } as Point);
  $('trend-sub').textContent = state.metric === 'orders'
    ? `${int(total)} orders · busiest ${per}: ${peak.bucket ? fmtBucket(peak.bucket, tr) : '—'} (${int(peak.orders)})`
    : `${money(total, tr.currency)} in ${tr.currency} · ${tr.points.length} ${per}s · times in ${tr.timezone}`;

  const value = (p: Point) => (state.metric === 'orders' ? p.orders : Number(state.metric === 'gross' ? p.gross_sales : p.recognized_sales));
  if (state.view === 'table') {
    const table = el('table', 'w-full text-sm tabnum');
    const head = el('tr', 'border-b hairline text-left');
    for (const [h, right] of [[tr.grain === 'day' ? 'Day' : 'Hour', false], ['Orders', true], [`Gross (${tr.currency})`, true], [`Recognized (${tr.currency})`, true]] as const) {
      head.appendChild(el('th', `py-2 pr-2 font-medium ${right ? 'text-right' : ''}`, h));
    }
    table.appendChild(head);
    for (const p of [...tr.points].reverse()) {
      const r = el('tr', 'border-b hairline');
      r.append(el('td', 'py-1.5', fmtBucket(p.bucket, tr)), el('td', 'py-1.5 text-right', int(p.orders)),
        el('td', 'py-1.5 text-right', money(p.gross_sales, tr.currency)), el('td', 'py-1.5 text-right', money(p.recognized_sales, tr.currency)));
      table.appendChild(r);
    }
    $('trend-table').replaceChildren(table);
    return;
  }
  const xs = tr.points.map((p) => (tr.grain === 'day' ? Date.parse(p.bucket + 'T00:00:00Z') / 1000 : Date.parse(p.bucket) / 1000));
  const ys = tr.points.map(value);
  const key = `${state.slug}|${tr.grain}|${state.metric}|${xs.length}|${xs[0]}|${tr.currency}`;
  if (state.chart && key === state.chartKey) {
    state.chart.setData(xs, ys); // keep the frame; only the data moves
    return;
  }
  state.chart?.destroy();
  state.chart = null;
  state.chartKey = key;
  const sym = currencySymbol(tr.currency);
  lineChart($('trend-chart'), {
    xs, ys, label: state.metric === 'orders' ? 'Orders' : `${label} (${tr.currency})`,
    tz: tr.grain === 'day' ? 'UTC' : tr.timezone, daily: tr.grain === 'day',
    fmtValue: (v) => (state.metric === 'orders' ? `${int(v)} orders` : money(v, tr.currency)),
    fmtAxis: (v) => (state.metric === 'orders' ? compactTick(v) : sym + compactTick(v)),
    height: 280,
  }).then((c) => {
    if (state.chartKey === key) state.chart = c;
    else c.destroy();
  });
}

function fmtBucket(b: string, tr: Trends): string {
  if (tr.grain === 'day') return new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', weekday: 'short', month: 'short', day: 'numeric' }).format(new Date(b + 'T00:00:00Z'));
  return new Intl.DateTimeFormat('en-US', { timeZone: tr.timezone, month: 'short', day: 'numeric', hour: 'numeric' }).format(new Date(b));
}

async function loadBreakdown() {
  const env = await get<Envelope<Breakdown>>(`${base()}/orders/breakdown${qs()}`, subject());
  const rows = env.data.statuses;
  const total = Math.max(1, env.data.total_orders);
  const max = Math.max(1, ...rows.map((r) => r.orders));
  const stack = $('status-stack');
  stack.replaceChildren(
    ...[...rows.filter((r) => r.recognized), ...rows.filter((r) => !r.recognized)]
      .filter((r) => r.orders > 0)
      .map((r, i, arr) => {
        const seg = el('div', 'h-full');
        seg.style.cssText = `width:${(r.orders / total) * 100}%;background:var(${r.recognized ? '--series-1' : '--series-2'});${i < arr.length - 1 ? 'border-right:2px solid var(--surface)' : ''}`;
        seg.title = `${r.status}: ${int(r.orders)} (${pct((r.orders / total) * 100)})`;
        return seg;
      }),
  );
  $('status-bars').replaceChildren(
    ...rows.map((r) => {
      const row = el('div', 'grid grid-cols-[5.75rem_1fr_auto] items-center gap-3 text-xs');
      row.tabIndex = 0;
      const share = (r.orders / total) * 100;
      row.title = `${int(r.orders)} orders (${pct(share)}) are ${r.status} — ${r.recognized ? 'counted in' : 'excluded from'} recognized sales`;
      const name = el('span', 'ink-2 truncate capitalize', r.status);
      const track = el('div', 'h-2 rounded-full');
      track.style.background = 'var(--surface-2)';
      const bar = el('div', 'h-2 rounded-full');
      bar.style.cssText = `width:${r.orders ? Math.max(1.5, (r.orders / max) * 100) : 0}%;background:var(${r.recognized ? '--series-1' : '--series-2'})`;
      track.appendChild(bar);
      const val = el('span', 'tabnum w-24 text-right');
      val.append(el('span', 'font-medium', int(r.orders)), el('span', 'muted', `  ${pct(share)}`));
      row.append(name, track, val);
      return row;
    }),
  );
  latency('lat-breakdown', env);
}

async function loadTop() {
  const env = await get<Envelope<Top>>(
    `${base()}/products/top${qs({ currency: state.currency, limit: String(state.topN), offset: String(state.topOffset) })}`, subject());
  const t = env.data;
  if (!t.products.length && t.offset > 0 && t.total > 0) { // the list got shorter than this page: show its last page
    state.topOffset = Math.max(0, Math.floor((t.total - 1) / state.topN) * state.topN);
    return loadTop();
  }
  const ps = t.products;
  $('top-title').textContent = `Top ${state.topN} products`;
  $('top-range').textContent = ps.length ? `${int(t.offset + 1)}–${int(t.offset + ps.length)} of ${int(t.total)}` : '';
  ($('top-prev') as HTMLButtonElement).disabled = t.offset === 0;
  ($('top-next') as HTMLButtonElement).disabled = t.next_offset == null;
  const sum = ps.reduce((n, p) => n + Number(p.revenue), 0) || 1;
  const max = Math.max(1, ...ps.map((p) => Number(p.revenue)));
  const body = $('top-body');
  if (!ps.length) {
    const r = el('tr');
    const c = el('td', 'muted py-6 text-center text-sm', `No recognized ${env.data.currency} sales in this range.`);
    c.colSpan = 5;
    r.appendChild(c);
    body.replaceChildren(r);
  } else {
    body.replaceChildren(
      ...ps.map((p) => {
        const r = el('tr', 'border-b hairline last:border-0');
        const rank = el('td', 'py-2.5 pr-3 align-middle');
        const badge = el('span', 'tabnum inline-flex h-6 w-6 items-center justify-center rounded-lg text-xs font-semibold', String(p.rank));
        badge.style.cssText = p.rank <= 3 ? 'background:var(--accent-wash);color:var(--accent-text)' : 'background:var(--surface-2);color:var(--ink-2)';
        rank.append(badge);
        const name = el('td', 'w-full max-w-0 py-2.5 pr-4'); // max-w-0 + w-full: long names truncate instead of widening the table
        name.append(el('div', 'truncate font-medium', p.name));
        const sub = [p.store_name, p.sku].filter(Boolean).join(' · '); // org view: which store sold it
        if (sub) name.append(el('div', 'muted truncate text-[11px]', sub));
        const share = (Number(p.revenue) / sum) * 100;
        const shareCell = el('td', 'hidden py-2.5 md:table-cell');
        const wrap = el('div', 'flex items-center gap-2');
        const track = el('div', 'h-1.5 flex-1 rounded-full');
        track.style.background = 'var(--surface-2)';
        const fill = el('div', 'h-1.5 rounded-full');
        fill.style.cssText = `width:${(Number(p.revenue) / max) * 100}%;background:var(--series-1)`;
        track.appendChild(fill);
        wrap.append(track, el('span', 'muted tabnum w-12 text-right text-xs', pct(share)));
        shareCell.appendChild(wrap);
        r.append(rank, name, el('td', 'tabnum py-2.5 pr-3 text-right whitespace-nowrap sm:pr-6', compactNum(Number(p.quantity))),
          el('td', 'tabnum py-2.5 pr-3 text-right font-semibold whitespace-nowrap sm:pr-6', money(p.revenue, env.data.currency)), shareCell);
        return r;
      }),
    );
  }
  latency('lat-top', env);
}

// ------------------------------------------------------------------ freshness

let skew = 0;
let lastFresh: Fresh | null = null;
let watching: AbortController | null = null;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// The API pushes the store's freshness over one streamed response only when
// something changes: new data, or the pipeline's health (live, lagging,
// stalled). With no changes nothing arrives but an invisible keepalive, so an
// open dashboard sends no requests and receives no events. Hidden tabs hold no
// stream. If the stream cannot be opened, the page polls /freshness every
// second for 30 s, then tries the stream again.
async function watch() {
  setInterval(() => renderFresh(lastFresh), 1000); // "last change N ago" ticks locally, no network
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) watching?.abort();
  });
  for (;;) {
    if (document.hidden || !state.slug) {
      await sleep(500);
      continue;
    }
    const key = scopeKey();
    const ctl = new AbortController();
    watching = ctl;
    let opened = false;
    try {
      await stream<Fresh>(`${base()}/stream`, subject(), ctl.signal, () => (opened = true), onFresh);
    } catch {
      // network error, API restart, or aborted (scope switched, tab hidden)
    }
    if (ctl.signal.aborted) continue;
    if (opened) {
      await sleep(1000); // the stream ended (e.g. API restart): reconnect
      continue;
    }
    for (const until = Date.now() + 30_000; Date.now() < until && key === scopeKey() && !document.hidden; ) {
      await pollOnce();
      await sleep(1000);
    }
  }
}

async function pollOnce() {
  try {
    await onFresh(await get<Fresh>(`${base()}/freshness`, subject()));
  } catch {
    lastFresh = null;
    renderFresh(null);
  }
}

async function onFresh(f: Fresh) {
  if (f.scope !== scopeKey()) return; // from the store or org the user just switched away from
  skew = Date.parse(f.server_time) - Date.now();
  lastFresh = f;
  renderFresh(f);
  if (f.data_version !== state.version || state.sawStale) {
    const first = state.version === '';
    state.version = f.data_version;
    if (!first || state.sawStale) {
      await refresh();
      if (state.pending) {
        const took = (performance.now() - state.pending.t0) / 1000;
        toast(`${state.pending.label} is on the dashboard ${took.toFixed(2)} s after commit`, 'good', 9000);
        state.pending = null;
      }
    }
  }
}

// renderFresh shows how fresh the page is: within how many seconds of the
// store's own database (org: of its slowest store). The server sends a new
// value only when that changes; while the data is behind, its age counts up
// here in the browser.
function renderFresh(f: Fresh | null) {
  const host = $('fresh');
  const dot = el('span', 'chip-dot');
  let label = 'Connecting…', color = '--ink-muted';
  let tip = 'Connecting to the change feed';
  const org = state.kind === 'org';
  const of = org ? 'its stores' : 'the store';
  const dbs = org ? "its stores' own databases" : "the store's own database";
  const feed = org ? "the stores' change feeds" : "the store's change feed";
  if (f?.status === 'live') dot.classList.add('live');
  if (f) {
    const since = (t: string | null) => (t ? Date.now() + skew - Date.parse(t) : null);
    const age = since(f.data_as_of);
    const change = since(f.last_source_commit_at);
    const within = f.fresh_within_ms ? `${f.fresh_within_ms / 1000} s` : '';
    tip = `Last change: ${change == null ? 'none since the pipeline started' : ago(change)}.`;
    switch (f.status) {
      case 'live':
        color = '--good';
        label = within ? `Fresh · within ${within} of ${of}` : 'Fresh';
        tip = `The numbers here are within ${within || '30 s'} of ${dbs}; new data is pushed as soon as it lands. ` + tip;
        break;
      case 'lagging':
      case 'stalled':
        color = age != null && age > 5 * 60_000 ? '--critical' : '--warning';
        label = `Behind · data from ${ago(age)}`;
        tip = `The numbers here are as of ${ago(age)}; newer changes are waiting in ${feed}${f.stores_behind ? ` (${f.stores_behind} of ${f.stores} stores behind)` : ''} and appear as soon as they land. ` + tip;
        break;
      default:
        label = f.status === 'snapshotting' ? 'Loading snapshot…' : 'Provisioning…';
    }
  }
  dot.style.background = `var(${color})`;
  host.title = tip;
  host.replaceChildren(dot, el('span', 'tabnum', label));
}

// ------------------------------------------------------------------ demo

function wireDemo() {
  const busy = async (b: HTMLButtonElement, fn: () => Promise<void>) => {
    b.disabled = true;
    try {
      await fn();
    } finally {
      b.disabled = false;
    }
  };
  $('add-order').addEventListener('click', (ev) =>
    busy(ev.currentTarget as HTMLButtonElement, async () => {
      try {
        const r = await demo<{ total_amount: string; currency: string; lines: unknown[] }>(`/tenants/${state.slug}/orders/random`, subject());
        state.pending = { label: `New order ${money(r.total_amount, r.currency)}`, t0: performance.now() };
        toast(`Order ${money(r.total_amount, r.currency)} committed to the store database…`);
      } catch (e) {
        toast(`Add order failed: ${(e as Error).message}`, 'bad');
      }
    }),
  );
  $('cancel-order').addEventListener('click', (ev) =>
    busy(ev.currentTarget as HTMLButtonElement, async () => {
      try {
        const r = await demo<{ total_amount: string; currency: string; previous_status: string }>(`/tenants/${state.slug}/orders/cancel`, subject());
        state.pending = { label: `Cancelled a ${r.previous_status} order (${money(r.total_amount, r.currency)})`, t0: performance.now() };
        toast(`Order (${money(r.total_amount, r.currency)}) changed from ${r.previous_status} to cancelled…`);
      } catch (e) {
        toast(`Cancel failed: ${(e as Error).message}`, 'bad');
      }
    }),
  );
  $('isolation').addEventListener('click', async () => {
    // A store this identity may not read: any other store for a store owner;
    // for an org admin, a store outside its org.
    const myOrg = state.as.startsWith('org:') ? state.as.slice(4) : '';
    const other = state.tenants.find((t) => t.slug !== state.slug && (!myOrg || t.org !== myOrg));
    if (!other) return toast('Need a store this identity may not read for this test', 'bad');
    try {
      await get(`/v1/tenants/${other.slug}/orders/summary?range=7d`, subject());
      toast(`Unexpected: ${state.as} could read ${other.slug}`, 'bad');
    } catch (e) {
      const err = e as ApiError;
      toast(`Blocked: the ${state.as} token asked for ${other.slug} and got HTTP ${err.status}: “${err.body?.detail ?? err.message}”`, 'good', 10000, 'shield-check');
    }
  });
  $('compare').addEventListener('click', (ev) => busy(ev.currentTarget as HTMLButtonElement, compare));
}

type Compare = {
  kind: string;
  match: boolean;
  differences: string[] | null;
  attempts: number;
  watermark_wait_ms: number;
  speedup_vs_cold: number;
  direct: { took_ms: number; sql: string };
  analytics_cold: { took_ms: number; rtt_ms: number };
  analytics_warm: { took_ms: number; cache: string };
};

async function compare() {
  const kind = ($('compare-kind') as HTMLSelectElement).value;
  const dlg = $('compare-dialog') as HTMLDialogElement;
  const body = $('compare-body');
  body.replaceChildren(el('p', 'muted', 'Writing a watermark into the store database, waiting for the pipeline to apply it, then running both queries…'));
  dlg.showModal();
  try {
    const req: Record<string, string> = { kind, currency: state.currency };
    if (state.range === 'custom') Object.assign(req, { from: state.from, to: state.to });
    else req.range = state.range;
    if (kind === 'trends') req.grain = state.grain;
    if (kind === 'summary' || kind === 'breakdown') delete req.currency;
    renderCompare(body, await demo<Compare>(`/tenants/${state.slug}/compare`, subject(), req));
  } catch (e) {
    body.replaceChildren(el('p', '', `Comparison failed: ${(e as Error).message}`));
  }
}

function renderCompare(body: HTMLElement, r: Compare) {
  const rows = [
    { label: 'Direct SQL on the store database', ms: r.direct.took_ms, note: 'Runs on the checkout database for every view', color: '--series-2' },
    { label: 'ClickHouse, first view (cache bypassed)', ms: r.analytics_cold.took_ms, note: 'Once per data change; zero load on the store database', color: '--series-3' },
    { label: 'Every repeat view (cache hit)', ms: r.analytics_warm.took_ms, note: 'Until the data changes', color: '--series-1' },
  ];
  const max = Math.max(...rows.map((x) => x.ms), 0.001);
  const verdict = el('div', `mb-4 flex items-center gap-2 rounded-xl px-3 py-2.5 text-sm font-medium ${r.match ? 'delta-up' : 'delta-down'}`);
  verdict.append(icon(r.match ? 'circle-check' : 'circle-alert', 16), el('span', '', r.match
    ? `Identical results${r.attempts > 1 ? ` (re-read ${r.attempts}× as writes landed in between)` : ''} · pipeline caught up in ${ms(r.watermark_wait_ms)}`
    : `${r.differences?.length ?? 0} differences, listed below`));
  const bars = el('div', 'space-y-4');
  for (const x of rows) {
    const wrap = el('div');
    const head = el('div', 'flex items-baseline justify-between gap-3');
    head.append(el('span', 'text-xs font-medium', x.label), el('span', 'tabnum text-base font-semibold', ms(x.ms)));
    const track = el('div', 'mt-1.5 h-2.5 w-full rounded-full');
    track.style.background = 'var(--surface-2)';
    const fill = el('div', 'h-2.5 rounded-full');
    fill.style.cssText = `width:${Math.max(0.6, (x.ms / max) * 100)}%;background:var(${x.color})`;
    track.appendChild(fill);
    wrap.append(head, track, el('div', 'muted mt-1 text-[11px]', x.note));
    bars.appendChild(wrap);
  }
  const speed = el('p', 'ink-2 mt-4 text-sm', r.speedup_vs_cold >= 1
    ? `ClickHouse answered ${r.speedup_vs_cold.toFixed(1)}× faster than direct SQL, and repeat views are ${Math.round(r.direct.took_ms / Math.max(r.analytics_warm.took_ms, 0.01)).toLocaleString('en-US')}× faster.`
    : `Direct SQL was faster for this query (${ms(r.direct.took_ms)} vs ${ms(r.analytics_cold.took_ms)}). Repeat views are ${Math.round(r.direct.took_ms / Math.max(r.analytics_warm.took_ms, 0.01)).toLocaleString('en-US')}× faster, and the store database does no dashboard work.`);
  const sqlHead = el('div', 'mt-5 flex items-center justify-between');
  sqlHead.append(el('h3', 'eyebrow', 'Direct SQL that was run'));
  const copy = el('button', 'btn h-8 px-3 text-xs');
  copy.append(icon('copy', 13), 'Copy');
  copy.addEventListener('click', () => navigator.clipboard?.writeText(r.direct.sql).then(() => copy.replaceChildren(icon('check', 13), 'Copied')));
  sqlHead.append(copy);
  const pre = el('pre', 'panel mt-2 p-3 font-mono text-[11px] leading-relaxed break-words whitespace-pre-wrap', r.direct.sql);
  body.replaceChildren(verdict, bars, speed, sqlHead, pre);
  if (r.differences?.length) {
    const ul = el('ul', 'mt-3 list-disc pl-5 text-xs');
    for (const d of r.differences.slice(0, 20)) ul.appendChild(el('li', '', d));
    body.appendChild(ul);
  }
}

init();
