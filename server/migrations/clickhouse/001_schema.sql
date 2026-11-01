-- TenantPulse analytics store (ClickHouse). Applied idempotently by `tpctl init`.
--
-- Model (docs/architecture.md):
--  * One row per captured change. ReplacingMergeTree(_version, is_deleted)
--    keeps the newest version per sorting key; queries read with FINAL, so
--    updates (placed -> cancelled) and deletes are exact.
--  * _version = the change's WAL position (LSN); snapshot rows use 0. Per row,
--    a later change always has a higher LSN, and replays insert identical rows,
--    so at-least-once delivery is idempotent by construction.
--  * created_at is immutable at the source, so it is safe in the sorting key:
--    every version of a row lands in the same place, and time ranges prune
--    through the primary index.
--  * Multi-node production: swap to ReplicatedReplacingMergeTree + Distributed
--    tables sharded by cityHash64(tenant_id); queries do not change.

CREATE DATABASE IF NOT EXISTS tp;

CREATE TABLE IF NOT EXISTS tp.orders
(
    tenant_id      UUID,
    generation     UInt32,
    order_id       UUID,
    created_at     DateTime64(6, 'UTC'),
    updated_at     DateTime64(6, 'UTC'),
    status         LowCardinality(String),
    currency       LowCardinality(String),
    total_amount   Decimal(20, 4),
    source_version UInt64,
    is_deleted     UInt8,
    _version       UInt64,
    _ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX idx_order_id order_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
PARTITION BY toYear(created_at)
-- 1024-row granules: 100 stores x a few new rows each must not cost 100 x 8192 rows.
ORDER BY (tenant_id, generation, created_at, order_id)
SETTINGS index_granularity = 1024;

CREATE TABLE IF NOT EXISTS tp.order_items
(
    tenant_id      UUID,
    generation     UInt32,
    item_id        UUID,
    order_id       UUID,
    product_id     UUID,
    -- Invariant: an item is never created before its order, so
    -- item.created_at >= order.created_at; queries use it as a lower-bound prune.
    created_at     DateTime64(6, 'UTC'),
    quantity       Decimal(22, 6),
    unit_price     Decimal(22, 6),
    source_version UInt64,
    is_deleted     UInt8,
    _version       UInt64,
    _ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX idx_item_id item_id TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_item_order order_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
PARTITION BY toYear(created_at)
ORDER BY (tenant_id, generation, created_at, item_id)
SETTINGS index_granularity = 1024;

CREATE TABLE IF NOT EXISTS tp.products
(
    tenant_id      UUID,
    generation     UInt32,
    product_id     UUID,
    sku            Nullable(String),
    name           String,
    is_active      UInt8,
    source_version UInt64,
    is_deleted     UInt8,
    _version       UInt64,
    _ingested_at   DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(_version, is_deleted)
ORDER BY (tenant_id, generation, product_id);

-- Freshness per tenant/generation (B-1). data_version changes whenever new
-- data for the tenant is flushed; API caches key on it.
CREATE TABLE IF NOT EXISTS tp.tenant_status
(
    tenant_id                UUID,
    generation               UInt32,
    snapshot_state           LowCardinality(String),
    last_source_commit_at    Nullable(DateTime64(3, 'UTC')),
    last_heartbeat_source_at Nullable(DateTime64(3, 'UTC')),
    last_applied_at          Nullable(DateTime64(3, 'UTC')),
    stream_checked_at        Nullable(DateTime64(3, 'UTC')),
    last_apply_lag_ms        Nullable(Int64),
    confirmed_lsn            UInt64,
    slot_lag_bytes           UInt64,
    events_total             UInt64,
    data_version             UInt64,
    _version                 UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation);

CREATE TABLE IF NOT EXISTS tp.ingester_stats
(
    worker            String,
    started_at        DateTime64(3, 'UTC'),
    events_total      UInt64,
    events_per_sec    Float64,
    flush_p50_ms      Float64,
    flush_p95_ms      Float64,
    flush_p99_ms      Float64,
    streams           UInt32,
    snapshots_running UInt32,
    max_slot_lag_bytes UInt64,
    updated_at        DateTime64(3, 'UTC'),
    _version          UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY worker;

-- Least-privilege users. tp_api is bound by row policies: every query must
-- carry the setting SQL_tenant_id, otherwise getSetting() fails (fail closed).
CREATE USER IF NOT EXISTS tp_ingest IDENTIFIED WITH sha256_password BY '{{TP_CH_INGEST_PASSWORD}}';
CREATE USER IF NOT EXISTS tp_api IDENTIFIED WITH sha256_password BY '{{TP_CH_API_PASSWORD}}';
CREATE USER IF NOT EXISTS tp_monitor IDENTIFIED WITH sha256_password BY '{{TP_CH_MONITOR_PASSWORD}}';

GRANT SELECT, INSERT, ALTER DELETE ON tp.* TO tp_ingest;
GRANT SELECT ON tp.orders TO tp_api;
GRANT SELECT ON tp.order_items TO tp_api;
GRANT SELECT ON tp.products TO tp_api;
GRANT SELECT ON tp.tenant_status TO tp_api;
-- tp_monitor sees pipeline health only — never orders, items or products.
GRANT SELECT ON tp.tenant_status TO tp_monitor;
GRANT SELECT ON tp.ingester_stats TO tp_monitor;
GRANT SELECT ON system.parts TO tp_monitor;

CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.orders FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.order_items FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.products FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.tenant_status FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
-- Other users keep full visibility on these tables (a row policy otherwise hides rows from them).
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.orders FOR SELECT USING 1 TO tp_ingest;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.order_items FOR SELECT USING 1 TO tp_ingest;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.products FOR SELECT USING 1 TO tp_ingest;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.tenant_status FOR SELECT USING 1 TO tp_ingest, tp_monitor
;

-- Benchmark results (tpctl stress / ladder), shown on the dashboard and /monitor.
CREATE TABLE IF NOT EXISTS tp.benchmark_runs
(
    run_id  String,
    kind    LowCardinality(String),
    at      DateTime64(3, 'UTC'),
    payload String
)
ENGINE = MergeTree
ORDER BY (kind, at);

GRANT INSERT, SELECT ON tp.benchmark_runs TO tp_ingest;
GRANT SELECT ON tp.benchmark_runs TO tp_monitor
;

-- Rollups (pre-aggregated, so dashboard reads touch tens of rows, not raw orders).
-- The ingester recomputes only the cells (store-day / store-hour) that changed,
-- from the raw tables, and writes them with a newer _version. Recomputing is
-- idempotent, so replays and restarts can never double count. A cell whose
-- groups disappear is rewritten with zeros.
-- Small granules (256 rows): one store's slice of a rollup is a few hundred
-- to a few thousand rows, so 8192-row granules made every lookup read other
-- stores' rows. The _version minmax index lets the API's in-memory views read
-- only the cells written since their last sync.
CREATE TABLE IF NOT EXISTS tp.order_day_rollup
(
    tenant_id  UUID,
    generation UInt32,
    day        Date,
    currency   LowCardinality(String),
    status     LowCardinality(String),
    orders     UInt64,
    gross      Decimal(38, 4),
    recognized Decimal(38, 4),
    _version   UInt64,
    INDEX idx_version _version TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation, day, currency, status)
SETTINGS index_granularity = 256;

CREATE TABLE IF NOT EXISTS tp.order_hour_rollup
(
    tenant_id  UUID,
    generation UInt32,
    hour       DateTime('UTC'),
    currency   LowCardinality(String),
    status     LowCardinality(String),
    orders     UInt64,
    gross      Decimal(38, 4),
    recognized Decimal(38, 4),
    _version   UInt64,
    INDEX idx_version _version TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation, hour, currency, status)
SETTINGS index_granularity = 256;

CREATE TABLE IF NOT EXISTS tp.product_day_rollup
(
    tenant_id  UUID,
    generation UInt32,
    day        Date,
    currency   LowCardinality(String),
    product_id UUID,
    quantity   Decimal(38, 6),
    revenue    Decimal(38, 12),
    _version   UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation, day, currency, product_id)
SETTINGS index_granularity = 256;

GRANT SELECT ON tp.order_day_rollup TO tp_api;
GRANT SELECT ON tp.order_hour_rollup TO tp_api;
GRANT SELECT ON tp.product_day_rollup TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.order_day_rollup FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.order_hour_rollup FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.product_day_rollup FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.order_day_rollup FOR SELECT USING 1 TO tp_ingest;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.order_hour_rollup FOR SELECT USING 1 TO tp_ingest;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.product_day_rollup FOR SELECT USING 1 TO tp_ingest
;

-- Product sales per tenant-local calendar month, built from the product day
-- cells. Long ranges (e.g. 365-day top products) read ~12 month cells per
-- product instead of 365 day cells, so their cost no longer grows with how
-- many days each product sells on.
CREATE TABLE IF NOT EXISTS tp.product_month_rollup
(
    tenant_id  UUID,
    generation UInt32,
    month      Date,
    currency   LowCardinality(String),
    product_id UUID,
    quantity   Decimal(38, 6),
    revenue    Decimal(38, 12),
    _version   UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation, month, currency, product_id)
SETTINGS index_granularity = 256;

GRANT SELECT ON tp.product_month_rollup TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.product_month_rollup FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.product_month_rollup FOR SELECT USING 1 TO tp_ingest
;

-- 15-minute base cells (ingest-internal; the API never reads them). Every
-- IANA offset is a multiple of 15 minutes, so a tenant-local day is exactly a
-- set of these UTC buckets. A refresh re-reads raw rows only for the
-- quarter-hours a batch touched; hour, day and month cells are summed from
-- these cells, so refresh cost follows the write rate, not the data size.
CREATE TABLE IF NOT EXISTS tp.order_q_rollup
(
    tenant_id  UUID,
    generation UInt32,
    q          DateTime('UTC'),
    currency   LowCardinality(String),
    status     LowCardinality(String),
    orders     UInt64,
    gross      Decimal(38, 4),
    recognized Decimal(38, 4),
    _version   UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation, q, currency, status)
SETTINGS index_granularity = 256;

CREATE TABLE IF NOT EXISTS tp.product_q_rollup
(
    tenant_id  UUID,
    generation UInt32,
    q          DateTime('UTC'),
    currency   LowCardinality(String),
    product_id UUID,
    quantity   Decimal(38, 6),
    revenue    Decimal(38, 12),
    _version   UInt64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (tenant_id, generation, q, currency, product_id)
SETTINGS index_granularity = 256
;

-- Org dashboards read the 15-minute cells (any viewer-local day is a whole set
-- of them), under the same per-store row policy as every other API read.
-- The ingester's allow-all policy is created first: once a table has a row
-- policy, users it does not name may see no rows, and the ingester rebuilds
-- the rollups from these cells.
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.order_q_rollup FOR SELECT USING 1 TO tp_ingest;
CREATE ROW POLICY IF NOT EXISTS allow_service ON tp.product_q_rollup FOR SELECT USING 1 TO tp_ingest;
GRANT SELECT ON tp.order_q_rollup TO tp_api;
GRANT SELECT ON tp.product_q_rollup TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.order_q_rollup FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api;
CREATE ROW POLICY IF NOT EXISTS tenant_scope ON tp.product_q_rollup FOR SELECT USING tenant_id = toUUID(getSetting('SQL_tenant_id')) TO tp_api
