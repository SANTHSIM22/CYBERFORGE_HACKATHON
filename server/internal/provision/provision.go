// Package provision implements additive, idempotent, resumable tenant
// provisioning. Nothing here ever deletes or scales down a tenant.
package provision

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tenantpulse/internal/config"
	"tenantpulse/internal/pg"
	"tenantpulse/migrations"
)

type Provisioner struct {
	Control *pgxpool.Pool // control DB, owner role
	Actor   string
}

type AddRequest struct {
	Slug     string
	Name     string
	Tier     string
	Timezone string
	Currency string
	CDC      string // "running" (default) or "deferred" (bulk seed first)
}

type Tenant struct {
	ID, Slug, Status, SourceDatabase, SourceRef, ClusterID string
	Created                                                bool
}

var slugRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,40}$`)

// EnsureDefaults registers the local source cluster and analytics shard.
func (p *Provisioner) EnsureDefaults(ctx context.Context) error {
	_, err := p.Control.Exec(ctx, `
		INSERT INTO source_clusters (cluster_name, region_code, endpoint_secret_ref, status)
		VALUES ('source-a', 'local', 'env:SOURCE_ADMIN_DSN', 'active') ON CONFLICT (cluster_name) DO NOTHING`)
	if err != nil {
		return err
	}
	_, err = p.Control.Exec(ctx, `
		INSERT INTO analytics_shards (shard_name, region_code, endpoint_secret_ref, database_name, status)
		VALUES ('shard-1', 'local', 'env:CLICKHOUSE_API_DSN', 'tp', 'active')
		ON CONFLICT (shard_name) DO NOTHING`)
	return err
}

// Add provisions one tenant. Re-running it for the same slug resumes any
// unfinished step and never duplicates a database, generation or connector.
func (p *Provisioner) Add(ctx context.Context, req AddRequest) (*Tenant, error) {
	if !slugRe.MatchString(req.Slug) {
		return nil, fmt.Errorf("invalid slug %q (lowercase letters, digits, dashes)", req.Slug)
	}
	if req.Name == "" {
		req.Name = req.Slug
	}
	if req.Tier == "" {
		req.Tier = "S"
	}
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if _, err := time.LoadLocation(req.Timezone); err != nil {
		return nil, fmt.Errorf("invalid timezone %q", req.Timezone)
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	if req.CDC == "" {
		req.CDC = "running"
	}
	if req.CDC != "running" && req.CDC != "deferred" {
		return nil, fmt.Errorf("cdc must be running or deferred")
	}

	t, err := p.registerTenant(ctx, req)
	if err != nil {
		return nil, err
	}
	names := NamesFor(t.Slug, 1)
	steps := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"create_database", func(ctx context.Context) error { return p.createDatabase(ctx, t) }},
		{"migrate_source", func(ctx context.Context) error { return p.migrateSource(ctx, t) }},
		{"register_generation", func(ctx context.Context) error { return p.registerGeneration(ctx, t, names, req.CDC) }},
	}
	for _, s := range steps {
		if done, _ := p.stepDone(ctx, t.ID, s.name); done {
			continue
		}
		if err := withRetry(ctx, s.fn); err != nil {
			p.markStep(ctx, t.ID, s.name, "failed", err)
			_, _ = p.Control.Exec(ctx, `UPDATE tenants SET status = 'failed', updated_at = now() WHERE tenant_id = $1`, t.ID)
			return t, fmt.Errorf("step %s: %w", s.name, err)
		}
		p.markStep(ctx, t.ID, s.name, "done", nil)
	}
	// A previously failed tenant whose steps now all succeeded goes back to provisioning.
	_, _ = p.Control.Exec(ctx, `UPDATE tenants SET status = 'provisioning', updated_at = now() WHERE tenant_id = $1 AND status = 'failed'`, t.ID)
	if t.Created {
		details, _ := json.Marshal(req)
		_, _ = p.Control.Exec(ctx, `INSERT INTO tenant_admin_audit (tenant_id, action, actor, details) VALUES ($1, 'tenant.add', $2, $3)`,
			t.ID, p.Actor, details)
	}
	return t, nil
}

func (p *Provisioner) registerTenant(ctx context.Context, req AddRequest) (*Tenant, error) {
	t := &Tenant{Slug: req.Slug}
	err := p.Control.QueryRow(ctx, `
		WITH cluster AS (
			SELECT c.source_cluster_id FROM source_clusters c
			WHERE c.status = 'active'
			  AND (SELECT count(*) FROM tenants t WHERE t.source_cluster_id = c.source_cluster_id) < c.max_tenant_databases
			ORDER BY (SELECT count(*) FROM tenants t WHERE t.source_cluster_id = c.source_cluster_id), c.cluster_name
			LIMIT 1
		), shard AS (
			SELECT analytics_shard_id FROM analytics_shards WHERE status = 'active' ORDER BY shard_name LIMIT 1
		)
		INSERT INTO tenants (tenant_slug, display_name, size_tier, source_cluster_id, source_database,
		                     analytics_shard_id, timezone, default_currency, status)
		SELECT $1, $2, $3, cluster.source_cluster_id, $4, shard.analytics_shard_id, $5, $6, 'provisioning'
		FROM cluster, shard
		ON CONFLICT (tenant_slug) DO NOTHING
		RETURNING tenant_id::text`,
		req.Slug, req.Name, req.Tier, NamesFor(req.Slug, 1).Database, req.Timezone, req.Currency).Scan(&t.ID)
	switch {
	case err == nil:
		t.Created = true
	case errors.Is(err, pgx.ErrNoRows):
		// existing tenant (or no capacity: detected below)
	default:
		return nil, err
	}
	err = p.Control.QueryRow(ctx, `
		SELECT t.tenant_id::text, t.status, t.source_database, c.endpoint_secret_ref, c.source_cluster_id::text
		FROM tenants t JOIN source_clusters c USING (source_cluster_id) WHERE t.tenant_slug = $1`, req.Slug).
		Scan(&t.ID, &t.Status, &t.SourceDatabase, &t.SourceRef, &t.ClusterID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errors.New("no active source cluster with free capacity (register another cluster)")
	}
	return t, err
}

func (p *Provisioner) stepDone(ctx context.Context, tenantID, step string) (bool, error) {
	var st string
	err := p.Control.QueryRow(ctx, `SELECT status FROM provisioning_steps WHERE tenant_id = $1 AND step = $2`, tenantID, step).Scan(&st)
	return err == nil && st == "done", err
}

func (p *Provisioner) markStep(ctx context.Context, tenantID, step, status string, cause error) {
	var msg *string
	if cause != nil {
		s := cause.Error()
		msg = &s
	}
	_, err := p.Control.Exec(ctx, `
		INSERT INTO provisioning_steps (tenant_id, step, status, attempts, last_error, updated_at)
		VALUES ($1, $2, $3, 1, $4, now())
		ON CONFLICT (tenant_id, step) DO UPDATE SET status = EXCLUDED.status,
		  attempts = provisioning_steps.attempts + 1, last_error = EXCLUDED.last_error, updated_at = now()`,
		tenantID, step, status, msg)
	if err != nil {
		slog.Warn("record provisioning step", "err", err)
	}
}

func (p *Provisioner) adminDSN(t *Tenant) (string, error) { return config.SecretRef(t.SourceRef) }

// createDatabase runs outside any transaction (CREATE DATABASE requires it).
func (p *Provisioner) createDatabase(ctx context.Context, t *Tenant) error {
	dsn, err := p.adminDSN(t)
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, t.SourceDatabase).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{t.SourceDatabase}.Sanitize())
	return err
}

func (p *Provisioner) migrateSource(ctx context.Context, t *Tenant) error {
	dsn, err := p.adminDSN(t)
	if err != nil {
		return err
	}
	dsn, err = pg.WithDatabase(dsn, t.SourceDatabase)
	if err != nil {
		return err
	}
	v, err := pg.Migrate(ctx, dsn, migrations.Source, "source")
	if err != nil {
		return err
	}
	if v != migrations.SourceSchemaVersion {
		return fmt.Errorf("source schema at version %d, expected %d", v, migrations.SourceSchemaVersion)
	}
	_, err = p.Control.Exec(ctx, `UPDATE tenants SET source_schema_version = $2, updated_at = now() WHERE tenant_id = $1`, t.ID, v)
	return err
}

func (p *Provisioner) registerGeneration(ctx context.Context, t *Tenant, n Names, cdc string) error {
	_, err := p.Control.Exec(ctx, `
		INSERT INTO tenant_generations (tenant_id, generation, replication_slot_name, publication_name, state, desired_state)
		VALUES ($1, 1, $2, $3, 'building', $4) ON CONFLICT (tenant_id, generation) DO NOTHING`,
		t.ID, n.Slot, n.Publication, cdc)
	return err
}

// StartCDC flips deferred tenants to streaming (after a bulk seed); the
// ingester then snapshots them with bounded concurrency.
func (p *Provisioner) StartCDC(ctx context.Context, slugs []string) (int64, error) {
	tag, err := p.Control.Exec(ctx, `
		UPDATE tenant_generations g SET desired_state = 'running'
		FROM tenants t WHERE t.tenant_id = g.tenant_id AND g.desired_state = 'deferred'
		  AND ($1::text[] IS NULL OR t.tenant_slug = ANY($1))`, slugs)
	return tag.RowsAffected(), err
}

// Names derives the per-tenant/generation identifiers from the slug.
type Names struct {
	Database    string // tenant_004
	Slot        string // tp_tenant_004_g1 (unique across the source cluster)
	Publication string
}

func NamesFor(slug string, generation int64) Names {
	db := strings.ReplaceAll(slug, "-", "_")
	return Names{Database: db, Slot: "tp_" + db + "_g" + strconv.FormatInt(generation, 10), Publication: "tp_publication"}
}

// withRetry: bounded exponential backoff with jitter for transient failures.
func withRetry(ctx context.Context, fn func(context.Context) error) error {
	backoff := 200 * time.Millisecond
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		slog.Warn("provisioning step failed; retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + time.Duration(time.Now().UnixNano()%int64(backoff/2))):
		}
		backoff *= 2
	}
	return err
}
