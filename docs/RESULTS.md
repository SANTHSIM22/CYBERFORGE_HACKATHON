# TenantPulse: measured results (1M and 10M records)

Every number below comes from result files in `bench/results/`, produced on a
fresh fleet at each size and turned into tables by `node bench/report.js`.
Nothing is hand-copied or estimated. How the system works is in
[GUIDE.md](GUIDE.md).

## Scorecard against the PRD targets

| Target | Good / Great | 1M (10 stores) | 10M (100 stores) | Verdict |
|---|---|---|---|---|
| **Query speed** at 10M | < 1 s / < 200 ms | Cold ClickHouse ≤ 62 ms p95 | Cold ClickHouse **≤ 77 ms p95** on all 9 queries; through the API at 1,297 req/s: **p95 41 ms, p99 84 ms, 0 errors** | ✅ **Great** |
| **Freshness** (order → numbers) | < 5 min / < 30 s | p95 1.7 s | p95 **1.8 s** | ✅ **Great** |
| **Correctness** vs direct SQL | most / all | **701 / 701** exact | **6,932 / 6,932** exact | ✅ **Great** |
| **Growth** 1M → 10M | < 5× / < 2× | — | all 9 queries **1.11–1.86×** (p50) | ✅ **Great** |
| **Database safety** (store query time with vs without dashboard load) | < 25% / < 10% | — | Dashboards sent **0 queries** to the store databases, but on one laptop store p95 still rose **+97%** (shared CPU) | ⚠️ **Not met as measured** (by design: zero dashboard load reaches the stores) |

### Reading the database-safety result honestly

- **By design, dashboards cannot slow the stores:** they never query them.
  Postgres' own transaction counters prove it: during the 30 s with 100
  dashboard viewers the store databases ran 1,476 transactions not issued by
  the store workload, against 1,458 in the same window with nobody viewing,
  i.e. background noise.
- **Measured on one laptop, the store still slowed (+97% p95).** The 4-core /
  8-thread chip runs the store database, ClickHouse, the API, the ingester and
  the load generators at once; CPU pinning cannot separate hyperthreads, the
  network stack or heat. On separate servers this interference disappears.
- **The analytics feed itself has a real cost at 100 stores per Postgres
  server** (+261% p95 with capture running vs paused). Postgres decodes changes
  per database, so 100 store databases mean 100 change decoders, each woken by
  every commit. Production fix: run decoding on a standby replica (Postgres
  16+), or cap stores per database server. See [GUIDE.md](GUIDE.md#known-limits-and-next-optimizations).

### Beyond the PRD: sustained heavy writes on all 100 stores

The write storm (all stores writing at once while 100 viewers watch) is our own
extra test. On this laptop, with every store changing every second, ingestion
falls behind: new orders take ~24 s to appear at 300 transactions/s and longer
at 1,000/s, and dashboards that insist on the newest data wait for it. The
cause is the laptop running the store database, 100 change decoders and
ClickHouse on the same 4 cores; on separate servers each tier gets its own
CPU. Two fixes made under this test are in the build: rollups now refresh
only the changed 15-minute buckets (previously each refresh re-read ~8M rows),
and dashboard reads never wait on a store's first in-memory load.

## Detailed results

### Data

| | 1M run | 10M run |
|---|---:|---:|
| Stores | 10 | 100 |
| Rows (customers + products + orders + order lines) | 1,000,029 | 9,995,884 |
| Orders | 314,462 | 3,144,565 |
| Order lines | 644,700 | 6,445,771 |
| Largest store's orders | 133,814 | 816,793 |

### Correctness

| | 1M | 10M |
|---|---:|---:|
| Stores checked | 10 | 100 |
| Checks (every widget, ClickHouse and in-memory views, vs direct SQL) | 701 | 6,932 |
| Stores with any mismatch | 0 | 0 |

### Query speed and growth (largest store, one query at a time, p50 / p95)

1M: tenant-001 with 133,814 orders. 10M: tenant-001 with 816,797 orders.

| Query | Store DB, B-tree index (1M → 10M) | Store DB, covering index (10M) | **ClickHouse 1M** | **ClickHouse 10M** | ClickHouse growth | Store DB growth |
|---|---:|---:|---:|---:|---:|---:|
| totals 7d | 10.4 ms → 56.9 ms | 13.8 ms / 17.3 ms | 15.3 ms / 20.8 ms | 17.0 ms / 27.0 ms | **1.11×** | 5.5× |
| totals 30d | 20.3 ms → 72.1 ms | 40.5 ms / 61.0 ms | 12.6 ms / 20.1 ms | 17.3 ms / 24.9 ms | **1.37×** | 3.5× |
| totals 365d | 69.3 ms → 299 ms | 285 ms / 529 ms | 12.7 ms / 17.6 ms | 23.6 ms / 32.8 ms | **1.86×** | 4.3× |
| daily trend 7d | 24.0 ms → 78.1 ms | 20.7 ms / 24.4 ms | 13.6 ms / 21.1 ms | 15.1 ms / 32.7 ms | **1.11×** | 3.3× |
| daily trend 30d | 52.0 ms → 160 ms | 99.4 ms / 113 ms | 16.6 ms / 31.9 ms | 18.8 ms / 41.1 ms | **1.13×** | 3.1× |
| daily trend 365d | 324 ms → 1.33 s | 1.04 s / 1.53 s | 16.2 ms / 48.2 ms | 18.7 ms / 34.0 ms | **1.15×** | 4.1× |
| top-10 products 7d | 46.3 ms → 228 ms | 143 ms / 272 ms | 26.6 ms / 40.7 ms | 39.3 ms / 76.9 ms | **1.48×** | 4.9× |
| top-10 products 30d | 76.7 ms → 439 ms | 479 ms / 1.09 s | 26.8 ms / 44.4 ms | 44.0 ms / 65.4 ms | **1.64×** | 5.7× |
| top-10 products 365d | 403 ms → 2.47 s | 2.76 s / 4.13 s | 44.9 ms / 62.2 ms | 59.9 ms / 76.3 ms | **1.33×** | 6.1× |

### Freshness (order committed → visible, 30 samples, p50 / p95 / p99)

| Measured to | 1M | 10M |
|---|---:|---:|
| Queryable in ClickHouse | 87.1 ms / 1.03 s / 1.04 s | 981 ms / 1.04 s / 1.10 s |
| Visible through the dashboard API | 834 ms / 1.73 s / 1.78 s | 1.48 s / 1.79 s / 1.94 s |

### Database safety (store transactions, CPU-pinned, p50 / p95 / p99)

The PRD measures normal store query time with and without dashboard load. In both arms the analytics pipeline is running; the only difference is 100 dashboard viewers.

| | 1M | 10M |
|---|---:|---:|
| Store rate (transactions/s, target) | — | 263 (300) |
| Without dashboards | — | 68.9 ms / 181 ms / 351 ms |
| With 100 dashboard viewers | — | 104 ms / 355 ms / 628 ms |
| **Change at p95** | — | **+96.6%** |
| Change at p50 | — | +51.2% |
| Dashboard queries that reached the store databases | — | 1,476 (baseline noise: 1,458) |
| Dashboard reads served (p50 / p95 / p99) | — | 48.5 ms / 4.79 s / 5.45 s |

Separately, the cost of change capture itself (capture paused vs running, same store workload, no dashboards):

| | 1M | 10M |
|---|---:|---:|
| Capture paused (p50 / p95 / p99) | 1.2 ms / 13.9 ms / 19.7 ms | 3.1 ms / 27.9 ms / 65.3 ms |
| Capture running | 1.9 ms / 17.4 ms / 30.5 ms | 10.4 ms / 101 ms / 194 ms |
| Change at p95 | +25.1% | +260.6% |

### Read stress: dashboards hammered (20 users with no pause, CPU-pinned)

| Phase | 1M req/s | 1M p50 / p95 / p99 | 10M req/s | 10M p50 / p95 / p99 | Errors (1M / 10M) |
|---|---:|---:|---:|---:|---:|
| Direct: query the primary DB | 90 | 86.1 ms / 838 ms / 2.07 s | 7 | 1.03 s / 4.78 s / 5.41 s | 0 / 37 |
| Aggregation layer (ClickHouse + cache) | 3,034 | 4.2 ms / 18.2 ms / 31.8 ms | 1,297 | 8.7 ms / 40.7 ms / 84.3 ms | 0 / 0 |
| ClickHouse only (cache bypassed) | 64 | 277 ms / 600 ms / 919 ms | 45 | 391 ms / 891 ms / 1.19 s | 0 / 0 |

### Write storm: every store writing at 1,000 transactions/s in total, 100 dashboard viewers

| Phase | Scale | Store tx/s | Store p50 / p95 / p99 | Dashboard p50 / p95 / p99 | New order visible p50 / p95 / p99 | Ingest events/s |
|---|---|---:|---:|---:|---:|---:|
| Pipeline off: store writes only, capture paused | 1M | 747 | 23.2 ms / 122 ms / 491 ms | — | — | — |
| Store writes, pipeline on, nobody viewing | 1M | 399 | 75.2 ms / 340 ms / 697 ms | — | 4.12 s / 6.37 s / 9.38 s | 3,973 |
| Store writes + dashboards on the aggregation layer | 1M | 377 | 54.0 ms / 428 ms / 1.92 s | 17.4 ms / 566 ms / 951 ms | 4.27 s / 6.29 s / 7.18 s | 683 |
| Store writes + dashboards querying the primary DB | 1M | 147 | 200 ms / 1.11 s / 1.52 s | 637 ms / 4.30 s / 5.48 s | instant (reads the DB) | 379 |
| Pipeline off: store writes only, capture paused | 10M | 866 | 33.0 ms / 113 ms / 211 ms | — | — | — |
| Store writes, pipeline on, nobody viewing | 10M | 362 | 105 ms / 339 ms / 520 ms | — | 26.75 s / 32.17 s / 33.17 s | 6 |
| Store writes + dashboards on the aggregation layer | 10M | 448 | 87.3 ms / 253 ms / 402 ms | 28.4 ms / 3.72 s / 4.31 s | — | 760 |
| Store writes + dashboards querying the primary DB | 10M | 144 | 242 ms / 877 ms / 1.47 s | 436 ms / 4.36 s / 5.58 s | instant (reads the DB) | 134 |

Note on growth: the 1M ladder was taken before monthly product cells were
added, so its "top-10 products 365d" figure read daily cells. With monthly
cells at 1M too, that row's growth ratio would be somewhat higher than 1.33×.
Every other row ran the same query path at both sizes.

## Read-side changes (2026-11-01, 10M data, 100 stores)

Three changes to the read side, measured on the same 10M data. Nothing in the
ingester or the ClickHouse schema changed. `tpctl verify` after the query
change: **6,932 / 6,932 checks exact**.

### 1. ClickHouse fallback: one read per table

Queries the in-memory view cannot answer exactly (exact-time ranges, 365-day
top products) used to issue one `FINAL` read per time range, about 10 ms each
however few rows they read: an exact-time summary was 14 reads. Top products
re-read every order line from each partial-hour edge until now. Now each table
is read once with its ranges combined, and order lines are de-duplicated with
`argMax`, as the ingester already does. Largest store, cache and views
bypassed, median of 7 to 9 calls:

| Query (exact time range, 9 days) | Before | After |
|---|---:|---:|
| Top 10 products | 4,762 ms | 103–119 ms |
| Summary (with previous period) | 167 ms | 68–86 ms |
| Status breakdown | 70 ms | 52–62 ms |
| Daily trend | 77 ms | 57–72 ms |

Preset ranges (today to 365 days) run the same SQL as before and stayed within
run-to-run noise (±20%); the ladder's ClickHouse column is unchanged at p50.

### 2. Freshness pushed instead of polled

Each open dashboard used to poll `/freshness` every second. It now holds one
stream (`/stream`, Server-Sent Events) and receives an event only when its
store's data version or pipeline health changes, plus an invisible keepalive
every 20 s. API CPU over 30 s (`process_cpu_seconds_total`), 2,000 dashboards
held open by k6, stores idle:

| 2,000 open dashboards | API CPU (one core = 100%) |
|---|---:|
| Idle, no dashboards | 2% |
| Polling `/freshness` every second | 77% |
| Stream, event every 2 s (first version) | 32% |
| **Stream, events only on change (shipped)** | **5%** |

ClickHouse CPU did not move in any of these: freshness is answered from the
API's memory. A pushed order reached an open dashboard 1.7–3.2 s after commit.

### 3. `meta.source` on every answer

Responses now say what computed them (`view` = in-memory rollups,
`clickhouse` = a query), and Monitor splits miss latency the same way. Before,
every miss was labelled "computed in ClickHouse", including the ones added up
in memory in under a millisecond.

### Re-runs after the changes (CPU-pinned)

| Test | Published (10/31) | After (11/01) |
|---|---|---|
| Read stress, aggregation layer: req/s, p50 / p95 / p99, hits | 1,297, 8.7 / 40.7 / 84.3 ms, 97% | run 1: 784, 7.0 / 39.7 / 356 ms, 87% · run 2: 3,006, 3.5 / 16.9 / 31.7 ms, 97% |
| Read stress, ClickHouse only: req/s, p95 | 45, 891 ms | 50, 896–970 ms |
| Write storm 300 tx/s, aggregation phase: store p95, dashboard p95, ingest/s | 355 ms, 4.79 s, 286 | 263 ms, 2.74 s, 388 |
| Write storm 1,000 tx/s, aggregation phase: store p95, dashboard p95, ingest/s | 253 ms, 3.72 s, 760 | 584 ms, 3.59 s, 0 (no flush finished in the window) |

The two read-stress runs on the same build differ by almost 4×: on one laptop
the result depends on what else is running (ClickHouse merges, the previous
phase's load). The 1,000 tx/s storm is beyond what this laptop's store
database sustains, as noted above, and varies the same way. Evidence:
`stress-20261101-025847`, `stress-20261101-032029`, `writestorm-20261101-031610`,
`writestorm-20261101-031941`, `ladder-tenant-001-20261101-025607`.

## How it was measured

| | |
|---|---|
| **Machine** | One laptop: AMD Ryzen 5 7520U (4 cores / 8 threads, 15 W), Docker Desktop on Windows 11. Every service (the store databases, control plane, ClickHouse, ingester, API, web and the load generators) runs on it. |
| **Data** | Generated by `tpctl seed` (deterministic). Stores follow a real-platform size skew: 1 extra-large, 9% large, 20% medium, the rest small. "Rows" = customers + products + orders + order lines. Each store has 365 days of history in its own timezone; 1 in 3 stores sells in two currencies. |
| **1M run** | `bench/scale.sh 10 1000000 1M`: fresh databases, 10 stores, full suite (`bench/results/suite-1M-20261031-173754.log`). |
| **10M run** | `bench/scale.sh 100 10000000 10M`: fresh databases, 100 stores, full suite (`bench/results/suite-10M-20261031-191603.log`). After the last two fixes, read stress, write storm and database safety were re-run on the same 10M data (`stress-20261031-223911`, `writestorm-20261031-224247`, `writestorm-20261031-224553`). |
| **CPU pinning** | Timing tests run through `bench/run-pinned.sh`: store Postgres on logical CPUs 0–2, ClickHouse 3–5, everything else 6–7, to imitate separate servers. On a 4-core chip these still share physical cores, so store-side numbers are a worst case. |

| Test | What it does |
|---|---|
| `verify` | Waits until the pipeline has caught up (a watermark written to each store), then compares every widget (summary with previous period, status breakdown, daily and hourly trends, top 10 per currency) over presets, odd ranges, partial hours and three daylight-saving changes. Each answer is checked from ClickHouse and from the API's in-memory views against plain SQL on the store database, value by value. |
| `ladder` | The largest store, one query at a time, 20 runs per cell: no index, default B-tree index, covering indexes (best-case tuning of the store database), and ClickHouse. Cold ClickHouse: no cache, no in-memory view. |
| `freshness` | Inserts 30 orders into the largest store and times until each is queryable in ClickHouse and until the API reports data current past it. |
| `oltp` | The same fixed store workload (300 operations/s) with change capture paused, then running. |
| `stress` | 20 simulated users with no pause between requests against (1) an API that queries the store databases directly, (2) the aggregation layer, (3) ClickHouse with cache and views switched off. |
| `write-storm` | All stores take checkouts, status changes and cancellations at a target rate while 100 viewers use dashboards like the real page (load four widgets, follow the store's freshness stream, or poll freshness every second on the direct-primary instance, which has no stream; reload when the store's data changes). Run at 1,000 transactions/s (beyond what the laptop's store database sustains) and at 300/s for the database-safety comparison. A marker order committed twice a second times "new order visible"; Postgres' transaction counters show how many dashboard queries reached the store databases. |

## Evidence

All files are in `bench/results/`:

| Size | Files |
|---|---|
| 1M | `seed-20261031-173725.json`, `suite-1M-20261031-173754.log`, `ladder-tenant-001-20261031-174004.json`, `freshness-tenant-001-20261031-174054.json`, `oltp-capture-off-20261031-174138.json`, `oltp-capture-on-20261031-174217.json`, `stress-20261031-174442.json`, `writestorm-20261031-174722.json` |
| 10M | `final-10M.log`, `suite-10M-20261031-191603.log`, `ladder-tenant-001-20261031-193317.json`, `freshness-tenant-001-20261031-193423.json`, `oltp-capture-*-20261031-1935*.json`, `stress-20261031-223911.json`, `writestorm-20261031-224247.json` (1,000/s), `writestorm-20261031-224553.json` (300/s, database safety) |

## Reproduce

```bash
bench/scale.sh 10 1000000 1M         # fresh 10-store / 1M fleet + full suite
bench/scale.sh 100 10000000 10M      # fresh 100-store / 10M fleet + full suite
node bench/report.js bench/results/suite-1M-<time>.log bench/results/suite-10M-<time>.log
```

Single tests: `make verify`, `bench/run-pinned.sh ladder --slug tenant-001`,
`make bench-freshness`, `make bench-oltp`, `bench/run-pinned.sh stress`,
`bench/run-pinned.sh write-storm --rate 300 --viewers 100 --direct-url=`. The
Dashboard and Monitor pages also have **▶ Run stress test** and **▶ Run write
storm** buttons (unpinned, so store-side numbers there are higher).
