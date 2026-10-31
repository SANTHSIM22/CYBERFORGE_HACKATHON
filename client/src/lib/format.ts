// One consistent number style (en-US grouping, K/M/B) for every currency.
const LOCALE = 'en-US';
const nf = new Intl.NumberFormat(LOCALE);
const compact = new Intl.NumberFormat(LOCALE, { notation: 'compact', maximumFractionDigits: 1 });

export const int = (n: number) => nf.format(n);
export const compactNum = (n: number) => (Math.abs(n) >= 10_000 ? compact.format(n) : nf.format(n));

export function money(value: string | number, currency: string, opts: { compact?: boolean } = {}): string {
  const n = typeof value === 'string' ? Number(value) : value;
  const short = opts.compact && Math.abs(n) >= 10_000;
  try {
    return new Intl.NumberFormat(LOCALE, {
      style: 'currency',
      currency,
      notation: short ? 'compact' : 'standard',
      minimumFractionDigits: short ? 0 : 2,
      maximumFractionDigits: short ? 1 : 2,
    }).format(n);
  } catch {
    return `${n.toFixed(2)} ${currency}`;
  }
}

export function currencySymbol(currency: string): string {
  try {
    return new Intl.NumberFormat(LOCALE, { style: 'currency', currency, currencyDisplay: 'narrowSymbol' })
      .formatToParts(0).find((p) => p.type === 'currency')?.value ?? currency;
  } catch {
    return currency;
  }
}

export function ms(n: number): string {
  if (n < 1) return `${n.toFixed(2)} ms`;
  if (n < 100) return `${n.toFixed(1)} ms`;
  if (n < 10_000) return `${Math.round(n)} ms`;
  return `${(n / 1000).toFixed(1)} s`;
}

export function pct(n: number, digits = 1): string {
  return `${n.toFixed(digits)}%`;
}

export function ago(msAgo: number | null | undefined): string {
  if (msAgo == null) return '—';
  const s = Math.max(0, Math.round(msAgo / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  return `${Math.round(s / 3600)}h ago`;
}

export function el<K extends keyof HTMLElementTagNameMap>(tag: K, cls?: string, text?: string): HTMLElementTagNameMap[K] {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}

export function $(id: string): HTMLElement {
  const e = document.getElementById(id);
  if (!e) throw new Error(`#${id} missing`);
  return e;
}

export function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function toast(msg: string, kind: 'info' | 'good' | 'bad' = 'info', ttl = 6000) {
  const host = document.getElementById('toasts');
  if (!host) return;
  const t = el('div', `toast ${kind === 'info' ? '' : kind}`, msg);
  host.appendChild(t);
  setTimeout(() => t.remove(), ttl);
  while (host.children.length > 4) host.firstElementChild?.remove();
}
