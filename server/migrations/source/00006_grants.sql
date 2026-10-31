-- +goose Up
-- Roles are cluster-wide and created by deploy/postgres/source-init.sh.
-- tp_replicator: logical replication + consistent snapshot reads. Never sees customers.
GRANT USAGE ON SCHEMA public TO tp_replicator, tp_demo, tp_app;
GRANT SELECT ON orders, order_items, products, tp_heartbeat TO tp_replicator;

-- tp_demo: the demo service (DEMO_MODE only) — add/cancel orders, direct-SQL compare.
GRANT SELECT ON orders, order_items, products, tp_heartbeat TO tp_demo;
GRANT INSERT ON orders, order_items TO tp_demo;
GRANT UPDATE (status, updated_at, version) ON orders TO tp_demo;
GRANT INSERT, UPDATE ON tp_heartbeat TO tp_demo;

-- tp_app: the OLTP workload used by the database-safety benchmark.
GRANT SELECT, INSERT, UPDATE ON customers, orders, order_items, products TO tp_app;

-- +goose Down
REVOKE ALL ON customers, orders, order_items, products, tp_heartbeat FROM tp_replicator, tp_demo, tp_app;
REVOKE USAGE ON SCHEMA public FROM tp_replicator, tp_demo, tp_app;
