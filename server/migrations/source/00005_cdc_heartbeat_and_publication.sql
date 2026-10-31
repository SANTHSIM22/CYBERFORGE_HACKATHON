-- +goose Up
-- Watermark row: the demo service and the reconciliation tool update it and wait
-- until the ingester has applied it. The change stream is in commit order, so
-- everything committed earlier is then in ClickHouse too.
CREATE TABLE tp_heartbeat (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO tp_heartbeat (id, ts) VALUES (1, now());

-- Column list keeps customer_id (PII link) out of logical decoding entirely.
CREATE PUBLICATION tp_publication FOR TABLE
    public.orders (id, created_at, updated_at, status, currency, total_amount, version, deleted_at),
    public.order_items,
    public.products,
    public.tp_heartbeat;

-- +goose Down
DROP PUBLICATION tp_publication;
DROP TABLE tp_heartbeat;
