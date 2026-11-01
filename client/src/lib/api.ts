// Same-origin client for the Go API (/api) and the demo service (/demo).
// Tokens come from the dev identity stub and live in memory only.

// cache: whether the answer was cached; source: what computed it (the API's
// in-memory rollups, ClickHouse, or the store database on the benchmark twin).
export type Meta = {
  took_ms: number;
  cache: 'HIT' | 'STALE' | 'MISS' | 'BYPASS' | 'DIRECT';
  source?: 'view' | 'clickhouse' | 'direct';
  generation: number;
  data_version: string;
};
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

// stream reads a Server-Sent Events response. It uses fetch rather than
// EventSource, which cannot send the Authorization header (a token in the URL
// would end up in logs). onOpen runs once the server accepts the stream.
// onEvent is awaited with the newest event of each chunk, so events that
// arrive while it runs collapse into the latest one. The server sends events
// only when something changes and a keepalive comment otherwise, so a stream
// silent for idleMs is treated as dead (a dropped network sends no close).
// Resolves when the stream ends; throws if it cannot be opened or fails.
export async function stream<T>(
  path: string,
  subject: string,
  signal: AbortSignal,
  onOpen: () => void,
  onEvent: (data: T) => Promise<void>,
  idleMs = 45_000,
): Promise<void> {
  const r = await fetch('/api' + path, {
    headers: { Authorization: 'Bearer ' + (await token(subject)), Accept: 'text/event-stream' },
    cache: 'no-store',
    signal,
  });
  if (!r.ok || !r.body || !r.headers.get('content-type')?.startsWith('text/event-stream')) throw new ApiError(r.status, await safeJson(r));
  onOpen();
  const reader = r.body.pipeThrough(new TextDecoderStream()).getReader();
  let idle = 0;
  const watch = () => {
    clearTimeout(idle);
    idle = window.setTimeout(() => reader.cancel().catch(() => {}), idleMs);
  };
  let buf = '';
  try {
    for (;;) {
      watch();
      const { value, done } = await reader.read();
      if (done) return;
      buf += value;
      let latest: T | undefined;
      for (let i = buf.indexOf('\n\n'); i >= 0; i = buf.indexOf('\n\n')) {
        const data = buf.slice(0, i).split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim()).join('\n');
        buf = buf.slice(i + 2);
        if (data) latest = JSON.parse(data) as T;
      }
      if (latest !== undefined) {
        clearTimeout(idle); // a slow reload is not a silent stream
        await onEvent(latest);
      }
    }
  } finally {
    clearTimeout(idle);
  }
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
