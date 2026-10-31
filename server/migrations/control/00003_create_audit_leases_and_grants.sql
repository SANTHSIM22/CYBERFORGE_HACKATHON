-- +goose Up
CREATE TABLE tenant_admin_audit (
    audit_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id  UUID,
    action     TEXT NOT NULL,
    actor      TEXT NOT NULL,
    details    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Only needed when the API runs as several replicas; one API process uses
-- in-memory semaphores (docs/architecture.md).
CREATE TABLE tenant_query_leases (
    lease_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants (tenant_id),
    request_id  UUID NOT NULL UNIQUE,
    query_class TEXT NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_tenant_query_leases_active ON tenant_query_leases (tenant_id, query_class, expires_at);

GRANT SELECT ON source_clusters, analytics_shards, tenants, tenant_generations TO tp_api, tp_ingest;
GRANT UPDATE (status, updated_at) ON tenants TO tp_ingest;
GRANT UPDATE (state, observed_state, snapshot_done, snapshot_rows, last_error, completed_at) ON tenant_generations TO tp_ingest;

-- +goose Down
DROP TABLE tenant_query_leases;
DROP TABLE tenant_admin_audit;
