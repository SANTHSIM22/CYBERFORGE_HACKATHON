-- +goose Up
CREATE TABLE source_clusters (
    source_cluster_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_name         TEXT NOT NULL UNIQUE,
    region_code          TEXT NOT NULL,
    -- Reference resolved by the env-var secret adapter (e.g. "env:SOURCE_A"); never a credential.
    endpoint_secret_ref  TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('active', 'maintenance', 'disabled')),
    max_tenant_databases INTEGER NOT NULL DEFAULT 150,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE analytics_shards (
    analytics_shard_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shard_name          TEXT NOT NULL UNIQUE,
    region_code         TEXT NOT NULL,
    endpoint_secret_ref TEXT NOT NULL,
    database_name       TEXT NOT NULL,
    status              TEXT NOT NULL CHECK (status IN ('active', 'maintenance', 'disabled')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE analytics_shards;
DROP TABLE source_clusters;
