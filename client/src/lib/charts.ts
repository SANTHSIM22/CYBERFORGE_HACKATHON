// Time-series charts on uPlot (JS loaded lazily; its stylesheet is bundled in
// global.css). One series and one y-axis per chart, never dual-axis.
import type uPlotType from 'uplot';
import { cssVar, el } from './format';

let mod: Promise<typeof uPlotType> | null = null;
const loadUPlot = () => (mod ??= import('uplot').then((m) => m.default));

export type LineSpec = {
  xs: number[]; // unix seconds
  ys: number[];
  label: string;
  tz: string;
  daily: boolean;
  fmtValue: (v: number) => string; // tooltip
  fmtAxis?: (v: number) => string; // y ticks
  height?: number;
  color?: string; // css var, default --series-1
  fmtX?: (unixSeconds: number) => string; // overrides date labels (axis + tooltip)
};

export type ChartHandle = { setData: (xs: number[], ys: number[]) => void; destroy: () => void };

const tip = (() => {
  let t: HTMLDivElement | null = null;
  return () => {
    if (!t) {
      t = el('div', 'tip');
      t.style.display = 'none';
      document.body.appendChild(t);
    }
    return t;
  };
})();

export function compactTick(v: number): string {
  if (v === 0) return '0';
  const a = Math.abs(v);
  const f = (n: number, s: string) => (n >= 100 ? n.toFixed(0) : n.toFixed(1).replace(/\.0$/, '')) + s;
  if (a >= 1e9) return f(v / 1e9, 'B');
  if (a >= 1e6) return f(v / 1e6, 'M');
  if (a >= 1e3) return f(v / 1e3, 'K');
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
}

function rgba(color: string, alpha: number): string {
  const m = /^#([0-9a-f]{6})$/i.exec(color);
  if (!m) return color;
  const n = parseInt(m[1], 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}

export async function lineChart(host: HTMLElement, spec: LineSpec): Promise<ChartHandle> {
  const uPlot = await loadUPlot();
  host.replaceChildren();
  const stroke = cssVar(spec.color ?? '--series-1');
  const muted = cssVar('--ink-muted');
  const grid = cssVar('--grid');
  const surface = cssVar('--surface');
  const height = spec.height ?? 260;
  const font = '11px system-ui, -apple-system, "Segoe UI", sans-serif';
  const dayFmt = new Intl.DateTimeFormat('en-US', { timeZone: spec.tz, month: 'short', day: 'numeric' });
  const hourFmt = new Intl.DateTimeFormat('en-US', { timeZone: spec.tz, hour: 'numeric' });
  const tipFmt = new Intl.DateTimeFormat('en-US', spec.daily
    ? { timeZone: spec.tz, weekday: 'short', month: 'short', day: 'numeric', year: 'numeric' }
    : { timeZone: spec.tz, weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' });

  const opts: uPlotType.Options = {
    width: Math.max(260, host.clientWidth),
    height,
    padding: [12, 8, 0, 0],
    legend: { show: false },
    cursor: {
      drag: { x: false, y: false },
      points: { size: 9, width: 2, fill: stroke, stroke: surface },
      y: false,
    },
    scales: { x: { time: true }, y: { range: (_u, _min, max) => [0, Math.max(1, max) * 1.15] } },
    axes: [
      {
        stroke: muted, font, grid: { show: false }, ticks: { stroke: grid, width: 1, size: 4 }, space: 64,
        values: (_u, splits) => splits.map((s) => {
          if (spec.fmtX) return spec.fmtX(s);
          const d = new Date(s * 1000);
          if (spec.daily) return dayFmt.format(d);
          const h = hourFmt.format(d);
          return h === '12 AM' ? dayFmt.format(d) : h;
        }),
      },
      {
        stroke: muted, font, size: 60, gap: 8, space: 36,
        grid: { stroke: grid, width: 1 }, ticks: { show: false },
        values: (_u, vals) => vals.map((v) => (spec.fmtAxis ?? compactTick)(v)),
      },
    ],
    series: [
      {},
      {
        label: spec.label, stroke, width: 2,
        fill: (u: uPlotType) => {
          const g = u.ctx.createLinearGradient(0, u.bbox.top, 0, u.bbox.top + u.bbox.height);
          g.addColorStop(0, rgba(stroke, 0.18));
          g.addColorStop(1, rgba(stroke, 0.01));
          return g;
        },
        points: { show: (u: uPlotType) => (u.data[0]?.length ?? 0) <= 2, size: 6, fill: stroke },
      },
    ],
    hooks: {
      setCursor: [
        (u: uPlotType) => {
          const t = tip();
          const i = u.cursor.idx;
          if (i == null || u.cursor.left == null || u.cursor.left < 0) {
            t.style.display = 'none';
            return;
          }
          const y = u.data[1][i] as number | null;
          t.replaceChildren();
          const v = el('div', 'text-sm font-semibold tabnum', y == null ? '—' : spec.fmtValue(y));
          const row = el('div', 'mt-0.5 flex items-center gap-2 text-[11px]');
          const key = el('span');
          key.style.cssText = `display:inline-block;width:12px;height:2px;border-radius:1px;background:${stroke}`;
          row.append(key, el('span', 'ink-2', spec.label));
          const x = u.data[0][i] as number;
          t.append(v, row, el('div', 'muted mt-0.5 text-[11px]', spec.fmtX ? spec.fmtX(x) : tipFmt.format(new Date(x * 1000))));
          const r = u.over.getBoundingClientRect();
          t.style.display = 'block';
          const left = r.left + u.cursor.left + 14;
          t.style.left = (left + t.offsetWidth > window.innerWidth - 8 ? r.left + u.cursor.left - t.offsetWidth - 14 : left) + 'px';
          t.style.top = Math.max(8, r.top + (u.cursor.top ?? 0) - t.offsetHeight / 2) + 'px';
        },
      ],
    },
  };
  const u = new uPlot(opts, [spec.xs, spec.ys], host);
  u.over.addEventListener('mouseleave', () => (tip().style.display = 'none'));
  const ro = new ResizeObserver(() => u.setSize({ width: Math.max(260, host.clientWidth), height }));
  ro.observe(host);
  return {
    setData: (xs, ys) => u.setData([xs, ys]),
    destroy: () => {
      ro.disconnect();
      u.destroy();
    },
  };
}

// Sparkline: tiny inline SVG trend for KPI cards (no axes, no interaction).
export function sparkline(host: HTMLElement, ys: number[], color = '--series-1') {
  const w = 120, h = 32;
  host.replaceChildren();
  if (ys.length < 2) return;
  const max = Math.max(...ys), min = Math.min(0, ...ys);
  const span = max - min || 1;
  const pts = ys.map((y, i) => [(i / (ys.length - 1)) * w, h - 2 - ((y - min) / span) * (h - 4)]);
  const d = pts.map((p, i) => `${i ? 'L' : 'M'}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join('');
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
  svg.setAttribute('preserveAspectRatio', 'none');
  svg.setAttribute('aria-hidden', 'true');
  svg.style.cssText = 'width:100%;height:32px;overflow:visible';
  const area = document.createElementNS(ns, 'path');
  area.setAttribute('d', `${d}L${w},${h}L0,${h}Z`);
  area.setAttribute('fill', rgba(cssVar(color), 0.1));
  const line = document.createElementNS(ns, 'path');
  line.setAttribute('d', d);
  line.setAttribute('fill', 'none');
  line.setAttribute('stroke', cssVar(color));
  line.setAttribute('stroke-width', '1.5');
  line.setAttribute('vector-effect', 'non-scaling-stroke');
  const end = document.createElementNS(ns, 'circle');
  const last = pts[pts.length - 1];
  end.setAttribute('cx', String(last[0]));
  end.setAttribute('cy', String(last[1]));
  end.setAttribute('r', '2.5');
  end.setAttribute('fill', cssVar(color));
  svg.append(area, line, end);
  host.appendChild(svg);
}
