-- +goose Up
CREATE TABLE tenants (
    tenant_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_slug           TEXT NOT NULL UNIQUE CHECK (tenant_slug ~ '^[a-z0-9][a-z0-9-]{1,40}$'),
    display_name          TEXT NOT NULL,
    size_tier             TEXT NOT NULL DEFAULT 'S' CHECK (size_tier IN ('S', 'M', 'L', 'XL')),
    source_cluster_id     UUID NOT NULL REFERENCES source_clusters (source_cluster_id),
    source_database       TEXT NOT NULL,
    source_schema_version INTEGER NOT NULL DEFAULT 0,
    analytics_shard_id    UUID NOT NULL REFERENCES analytics_shards (analytics_shard_id),
    timezone              TEXT NOT NULL DEFAULT 'UTC',
    default_currency      CHAR(3) NOT NULL DEFAULT 'USD',
    status                TEXT NOT NULL CHECK (status IN (
        'provisioning', 'snapshotting', 'active', 'paused', 'migrating', 'deleting', 'failed'
    )),
    active_generation     BIGINT NOT NULL DEFAULT 1,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_cluster_id, source_database)
);
CREATE INDEX idx_tenants_source_cluster ON tenants (source_cluster_id, status);
CREATE INDEX idx_tenants_analytics_shard ON tenants (analytics_shard_id, status);

CREATE TABLE tenant_generations (
    tenant_id             UUID NOT NULL REFERENCES tenants (tenant_id),
    generation            BIGINT NOT NULL,
    replication_slot_name TEXT NOT NULL UNIQUE,
    publication_name      TEXT NOT NULL,
    state                 TEXT NOT NULL CHECK (state IN ('building', 'catching_up', 'active', 'retired', 'failed')),
    -- Change capture: 'deferred' = provisioned but not streaming yet (bulk seed first).
    desired_state         TEXT NOT NULL DEFAULT 'running' CHECK (desired_state IN ('running', 'paused', 'deferred')),
    observed_state        TEXT,
    snapshot_done         BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot_rows         BIGINT,
    last_error            TEXT,
    started_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ,
    failure_reason        TEXT,
    PRIMARY KEY (tenant_id, generation)
);

-- Idempotent, resumable provisioning: one row per (tenant, step).
CREATE TABLE provisioning_steps (
    tenant_id   UUID NOT NULL REFERENCES tenants (tenant_id),
    step        TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('pending', 'done', 'failed')),
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, step)
);

-- +goose Down
DROP TABLE provisioning_steps;
DROP TABLE tenant_generations;
DROP TABLE tenants;
