// Same-origin client for the Go API (/api) and the demo service (/demo).
// Tokens come from the dev identity stub and live in memory only.

export type Meta = { took_ms: number; cache: 'HIT' | 'STALE' | 'MISS' | 'BYPASS'; generation: number; data_version: string };
export type Envelope<T> = { tenant: string; data: T; meta: Meta };
export type Timed<T> = Envelope<T> & { rttMs: number };

export class ApiError extends Error {
  constructor(public status: number, public body: any) {
    super(body?.detail || body?.error || `HTTP ${status}`);
  }
}

const tokens = new Map<string, string>();

export async function token(subject: string): Promise<string> {
  const hit = tokens.get(subject);
  if (hit) return hit;
  const r = await fetch('/api/v1/dev/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ subject }),
  });
  if (!r.ok) throw new ApiError(r.status, await safeJson(r));
  const t = (await r.json()).token as string;
  tokens.set(subject, t);
  return t;
}

async function safeJson(r: Response) {
  try {
    return await r.json();
  } catch {
    return { error: r.statusText };
  }
}

export async function get<T>(path: string, subject: string, signal?: AbortSignal): Promise<T & { rttMs: number }> {
  const t0 = performance.now();
  const r = await fetch('/api' + path, { headers: { Authorization: 'Bearer ' + (await token(subject)) }, signal });
  const body = await safeJson(r);
  if (!r.ok) throw new ApiError(r.status, body);
  return Object.assign(body, { rttMs: performance.now() - t0 });
}

export async function demo<T>(path: string, subject: string, body?: unknown): Promise<T> {
  const r = await fetch('/demo' + path, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + (await token(subject)), 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  const json = await safeJson(r);
  if (!r.ok) throw new ApiError(r.status, json);
  return json as T;
}

export async function demoAvailable(): Promise<boolean> {
  try {
    const r = await fetch('/demo/health', { cache: 'no-store' });
    return r.ok;
  } catch {
    return false;
  }
}

export type Subject = {
  subject: string;
  slug: string;
  name: string;
  tier: 'S' | 'M' | 'L' | 'XL';
  status: string;
  default_currency: string;
  timezone: string;
};

export async function subjects(): Promise<Subject[]> {
  const r = await fetch('/api/v1/dev/subjects');
  if (!r.ok) throw new ApiError(r.status, await safeJson(r));
  return (await r.json()).tenants;
}
