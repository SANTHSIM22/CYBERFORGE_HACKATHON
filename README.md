# TenantPulse

Fast, fresh, tenant-isolated commerce analytics.

```
Store databases ──logical replication──▶ Go ingester ──batched──▶ ClickHouse ◀── Go API ◀── Dashboard
(one Postgres DB per tenant)                                      (row policies)   (no Redis)
```

Dashboard queries never touch the store databases. Changes reach the
dashboard in about a second, and every number matches a direct SQL query
exactly (`tpctl verify`). Start with **[docs/GUIDE.md](docs/GUIDE.md)**: how
it works step by step, why it is built this way, every edge case, how it was
tested and the measured results. [docs/architecture.md](docs/architecture.md)
is the shorter design reference.

## Quick start

Prerequisite: Docker Desktop (8 GB+ for Docker; 12 GB+ for the 100-tenant /
10M-row benchmark). Nothing else is needed on the host.

```bash
cp .env.example .env
docker compose up -d --build
```

This builds everything, creates the schemas, seeds **3 tenants (~60k rows)**,
snapshots them into ClickHouse and starts streaming. It takes about 1–2
minutes the first time.

| Open | What |
|---|---|
| http://localhost:8088 | Dashboard |
| http://localhost:8088/monitor | Monitoring: p50/p95/p99, lag, events/s, and live charts (req/s, p95, cache hit rate) for each read path: direct DB, ClickHouse only, aggregation layer |
| http://localhost:8080 | REST API (Postman / curl) |

Check status: `docker compose ps` (all services `healthy`), then
`docker compose --profile tools run --rm tpctl tenant list` (every tenant `active`).

> Windows without `make`: use `.\tp.ps1 <target>` with the same target names as below.

## Using the dashboard

1. **Pick a tenant** (top-left). Picking a tenant logs in *as that store*.
   Its token can only read that store.
2. **Pick a range**: Today / 7 / 30 / 90 days / Custom. Presets are whole
   days in the store's own timezone.
3. **Live demo row** (needs `DEMO_MODE=true`, the default in `.env.example`):
   - **+ Add random order** writes a real order into the store's database.
     The message shows how many seconds it took to appear on the dashboard.
   - **Cancel an order** changes a recent order to `cancelled`. Recognized
     sales, the status bars and top products all update.
   - **Compare with direct SQL** waits until the pipeline has caught up, then
     runs the same question on the store database and on ClickHouse. It shows
     both timings, the cache-hit timing, the exact SQL, and whether the
     results match exactly.
   - **Try cross-tenant read** uses this store's token to request another
     store's numbers. The API refuses with HTTP 403.
4. Every widget states its own speed: *"Answered in 0.1 ms (cache hit) · 3 ms round trip"*.
5. The badge at the top right shows freshness: *"Live · last change 4m ago"*.
   New data is pushed to the page the moment it lands; the badge turns
   *Lagging* or *Pipeline stalled* if the pipeline falls behind or stops.

## REST API

```bash
# dev login (AUTH_MODE=dev): get a token for one store, or "operator" for all
TOKEN=$(curl -s -X POST localhost:8080/v1/dev/login -H 'Content-Type: application/json' \
  -d '{"subject":"tenant:tenant-001"}' | jq -r .token)

curl -H "Authorization: Bearer $TOKEN" "localhost:8080/v1/tenants/tenant-001/orders/summary?range=7d"
```

| Endpoint | PRD |
|---|---|
| `GET /v1/tenants/{t}/orders/summary?range=7d` (or `from`/`to`, `currency`) | FR-1 counts and totals per currency |
| `GET /v1/tenants/{t}/orders/count?...` | FR-1 alias from the PRD |
| `GET /v1/tenants/{t}/orders/trends?range=30d&grain=day\|hour&currency=` | FR-2 |
| `GET /v1/tenants/{t}/products/top?range=7d&limit=10&currency=` | FR-3 |
| `GET /v1/tenants/{t}/orders/breakdown?range=7d` | FR-4 current status of orders created in range |
| `GET /v1/tenants/{t}/freshness` | B-1 |
| `GET /v1/tenants/{t}/stream` | B-1 pushed: the same body as Server-Sent Events, sent only when the store's data or pipeline health changes (the dashboard uses this instead of polling) |
| `GET /v1/tenants/{t}` | timezone, currencies, status |
| `GET /v1/admin/stats` (operator) | B-5; per-second series for dashboard reads and, separately, cache-bypassed benchmark reads |
| `GET /metrics` | Prometheus |

Ranges are half-open `[from, to)`. `from`/`to` accept RFC 3339 instants or
plain dates, which mean midnight in the tenant's timezone. Money is returned
as exact decimal strings, never added across currencies. Gross sales include
every order; recognized sales cover placed, paid, processing, shipped and
delivered orders.

## Operating it (`tpctl`)

All commands run inside Docker: `docker compose --profile tools run --rm tpctl <command>`.

| Task | Command |
|---|---|
| Add one tenant (live, no restart) | `tenant add --slug tenant-004` |
| Add 5 more / ensure at least N (never removes any) | `tenant add-batch --count 5` · `tenant ensure --count 10` |
| List / inspect | `tenant list` · `tenant status --slug tenant-001` |
| Add rows to a live tenant (streams through) | `seed-additive --slug tenant-002 --rows 20000` |
| **Prove correctness** against direct SQL | `verify` |
| Rebuild a tenant into a new generation (B-2) | `rebuild --slug tenant-002` |
| Apply new source migrations | `migrate-source-all` |

## Benchmarks (full scale: 100 tenants, 10M rows)

```bash
make dev-reset CONFIRM=yes          # optional: start clean
docker compose up -d --build
make bench-setup TENANTS=100 TOTAL_ROWS=10000000
make verify                         # exact match, all tenants
make bench-stress                   # two instances: direct-on-primary vs aggregation layer, under load
make bench-ladder SLUG=tenant-001   # no index vs B-tree vs covering index vs ClickHouse
make bench-freshness                # commit -> visible, p50/p95/p99
make bench-oltp                     # store latency with capture paused vs running
make bench-api                      # k6 load on the dashboard API
```

Results are printed and saved as JSON in `bench/results/`. The 10M rows are
`customers + products + orders + order_items` (counts per table are reported).

## Troubleshooting

| Symptom | Fix |
|---|---|
| Dashboard empty / "Could not load data" | `docker compose ps`: `api` must be healthy; `docker compose logs api` |
| Tenant stuck "snapshotting" | `docker compose logs ingester`; `tpctl tenant status --slug …` |
| No live-demo row | `DEMO_MODE=true` and `COMPOSE_PROFILES=demo` in `.env`, then `docker compose up -d` |
| `npm run dev` in `client/` | Works against the running stack (proxies `/api` → :8080, `/demo` → :8081) |
| Start over | `make dev-reset CONFIRM=yes` (deletes all local data) |
