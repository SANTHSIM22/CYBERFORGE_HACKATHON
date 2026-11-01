# TenantPulse architecture

```mermaid
flowchart LR
    subgraph SRC[Source cluster(s) — one logical DB per tenant]
      T1[(tenant_001)]
      T2[(tenant_002)]
      TN[(tenant_N)]
    end
    subgraph CP[Control plane]
      REG[(control Postgres<br/>tenants · generations · audit)]
    end
    ING[Go ingester ×N<br/>snapshot + pgoutput stream<br/>batched, ack after write]
    CH[(ClickHouse<br/>ReplacingMergeTree + FINAL<br/>row policies per tenant)]
    API[Go/Gin API ×N<br/>authz · limits · version-keyed cache]
    WEB[nginx + static Astro<br/>dashboard · /monitor]
    DEMO[demo service<br/>DEMO_MODE only]

    T1 & T2 & TN -- logical replication --> ING
    REG --> ING
    ING -- batched INSERT --> CH
    API -- SQL_tenant_id + FINAL --> CH
    REG --> API
    WEB -- /api --> API
    WEB -- /demo --> DEMO
    DEMO -- add / cancel / direct SQL --> T1
```

## Data path for one order

1. The store commits `INSERT orders` and `INSERT order_items` in one transaction.
2. Postgres logical decoding (`pgoutput`) sends the transaction to the tenant's
   replication slot reader (the ingester) **after commit, in commit order**.
3. The ingester turns each change into a row with `_version = WAL position`
   and buffers whole transactions. Every second (`INGEST_FLUSH_INTERVAL`, or 50k rows) it writes one
   `INSERT` per table for **all tenants**. ClickHouse prefers few, large inserts.
4. Only after ClickHouse accepts the batch does the ingester confirm the WAL
   position to Postgres. A crash at any point re-sends unconfirmed WAL, and the
   re-sent rows are identical (same `_version`), so ReplacingMergeTree collapses
   them. This gives at-least-once delivery with idempotent application.
5. Right after the raw rows land, the ingester **refreshes the rollup hierarchy
   bottom-up, for the touched 15-minute buckets only**: it re-reads raw rows for
   those quarter-hours (`order_q_rollup`, `product_q_rollup`; order lines are
   de-duplicated with `argMax` rather than `FINAL`, which would undo index
   skipping), then sums hour cells (`order_hour_rollup`), store-local day cells
   (`order_day_rollup`, `product_day_rollup`, the latter only for products that
   changed) and calendar-month cells (`product_month_rollup`) from them. Every
   timezone offset is a multiple of 15 minutes, so a local day is exactly a set
   of quarter buckets. Each level is recomputed (not adjusted by deltas), so
   replays and restarts can never double count; groups that disappear are
   rewritten as zeros. Refresh cost follows the write rate, not the data size.
6. The ingester then writes `tp.tenant_status` (freshness, `data_version`), so a
   new `data_version` always means raw rows *and* rollups are queryable.
7. The API keeps an **in-memory view** of each store's rollups (`chq.View`):
   it loads the store's day cells, last 72 h of hour cells, last 95 days of
   product-day cells and product names once, then on every new `data_version`
   reads only the cells written since (`_version` above the last applied), in
   one query. Summary, status breakdown and trends over whole local days
   (every preset and date-picker range), hourly trends over the last 72 h and
   top products for today / 7 / 30 / 90 days are computed in memory. So
   ClickHouse work follows how often a store's data changes, not how many
   people watch it or how many widgets and ranges they open.
   Anything else is a ClickHouse query: whole calendar months from the
   product-month cells, whole tenant-local days from the day rollups, whole
   hours from the hour rollup, and only partial-hour edges from
   raw rows (`FINAL` = newest version of each row; tombstones removed). A
   30-day query reads tens of rows instead of every order in the range.
   Responses are cached in-process under `(tenant, generation, query)`, and each
   entry records the `data_version` it was computed at. An entry at the current
   version is a `HIT`. When new data arrives, the previous answer (at most
   `API_CACHE_STALE` = 5 s old) is served as `STALE` while one background
   query computes the new one (stale-while-revalidate). Without this, the
   ingester's flush makes every entry of every tenant invalid at once, and the
   resulting burst of ClickHouse queries was the p95/p99 tail under load. The
   dashboard learns new versions from a freshness stream (`/stream`, Server-Sent
   Events pushed only when its store's version or pipeline health changes), not
   by polling, and sends `min_version` (the version its freshness badge shows), so it
   never displays data older than the badge; its auto-refresh after new data
   always waits for the new answer (singleflight: one query per widget shared
   by all viewers). Background refreshes queue for at most half of the
   per-store and global ClickHouse slots, so the other half is always free
   for readers; a reader waits up to `API_LIMIT_WAIT` (1 s) for a slot before
   getting a 429.

## Why these choices (vs. the original spec)

| Decision | Why | Trade-off |
|---|---|---|
| **ClickHouse** instead of analytics Postgres rollups | Columnar scans at hundreds of millions of rows/s and ~10× compression. Any new metric is just a query, with no backfill (B-4). Scales by sharding on `tenant_id` | Updates go through ReplacingMergeTree + `FINAL`. Kept exact because the sorting key uses only immutable columns |
| **Go ingester on logical replication** instead of Debezium + Kafka | Same pattern as ClickHouse's own Postgres CDC (PeerDB / ClickPipes). One goroutine per tenant instead of one Connect task and JVM queue per tenant. No Kafka or Connect cluster to run. The first load is a `COPY`-speed snapshot | The replication slot is the buffer, bounded by `max_slot_wal_keep_size`, so slot lag is monitored. A Kafka sink can be added behind the same sink interface if fan-out is needed |
| **No Redis** | ClickHouse answers in milliseconds. A per-replica cache keyed on the data version gives sub-ms hits with no extra service and no invalidation races | Each API replica warms its own cache |
| **Exported-snapshot initial load** | The snapshot and the stream start at exactly the same point: no gaps, no duplicates | Interrupted snapshots are discarded and redone (rows cleared first) |
| **Control plane in its own Postgres** | The registry is small and transactional, and is separated from tenant data | One more small database |
| **Static Astro + nginx** | The pages contain no tenant data; every number is fetched with an identity token and authorized server-side | Rendering happens in the browser |

## Correctness rules

- `created_at` (orders and items) is immutable, and an item is never created
  before its order. Both are application invariants, and `tpctl verify` checks
  the result against direct SQL.
- Money is `Decimal`, end to end. Currencies are never summed together.
- Ranges are half-open `[from, to)`. Day buckets use the tenant's IANA
  timezone (DST-correct, including half/quarter-hour offsets).
- Status breakdown = **current** status of orders created in the range.

## Scaling out (production)

| Layer | How it scales |
|---|---|
| Source | More source clusters; new tenants are placed by capacity (`source_clusters`) |
| Ingester | `INGESTER_COUNT=N`; tenants are hash-partitioned, so each slot has one reader. Restarting or moving an ingester is safe (slot + idempotent rows) |
| ClickHouse | `ReplicatedReplacingMergeTree` + `Distributed` sharded by `cityHash64(tenant_id)`: one tenant's rows live on one shard, so `FINAL` stays local. Replicas per shard (ClickHouse Keeper) |
| API | Stateless replicas behind nginx, which routes each tenant to one replica (hash of the tenant in the URL, `docker-compose.scale.yml`), so a tenant's cache and in-memory view are held once; views unread for 15 min are dropped; per-tenant and global query limits (B-3) |

## Isolation (FR-5)

1. Source: database per tenant.
2. Ingest: tenant identity comes from the control-plane registry (slot →
   tenant), never from row data.
3. ClickHouse: the `tp_api` user is bound by **row policies**. Every query must
   carry `SQL_tenant_id`; without it the query fails (fail closed).
   `tp_monitor` sees pipeline health only, never orders or products.
4. API: the bearer token's identity must be a member of the tenant (403 otherwise).
