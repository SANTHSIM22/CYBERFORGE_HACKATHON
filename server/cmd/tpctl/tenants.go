package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/config"
	"tenantpulse/internal/gen"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/provision"
	"tenantpulse/migrations"
)

func tenantCmd(ctx context.Context, e *env, sub string, args []string) error {
	switch sub {
	case "add":
		var req provision.AddRequest
		if _, err := flags("tenant add", args, func(fs *flag.FlagSet) {
			fs.StringVar(&req.Slug, "slug", "", "tenant slug, e.g. tenant-004")
			fs.StringVar(&req.Name, "name", "", "display name")
			fs.StringVar(&req.Tier, "tier", "", "S|M|L|XL")
			fs.StringVar(&req.Timezone, "tz", "", "IANA timezone")
			fs.StringVar(&req.Currency, "currency", "", "default currency")
			fs.StringVar(&req.CDC, "cdc", "running", "running|deferred")
		}); err != nil {
			return err
		}
		if req.Slug == "" {
			return fmt.Errorf("--slug is required")
		}
		if i, ok := profileIndex(req.Slug); ok { // fill unset fields from the deterministic profile
			p := gen.ProfileFor(i, max(i+1, 100))
			req.Name, req.Tier, req.Timezone, req.Currency = or(req.Name, p.Name), or(req.Tier, p.Tier), or(req.Timezone, p.Timezone), or(req.Currency, p.Currency)
		}
		t, err := e.prov.Add(ctx, req)
		if err != nil {
			return err
		}
		fmt.Printf("tenant %s (%s) %s; the ingester snapshots and streams it automatically\n", t.Slug, t.ID, map[bool]string{true: "created", false: "already existed — resumed/verified"}[t.Created])
		return nil
	case "add-batch", "ensure":
		var count int
		var cdc string
		if _, err := flags("tenant "+sub, args, func(fs *flag.FlagSet) {
			fs.IntVar(&count, "count", 0, "number of tenants")
			fs.StringVar(&cdc, "cdc", "running", "running|deferred")
		}); err != nil {
			return err
		}
		existing, err := tenantCount(ctx, e)
		if err != nil {
			return err
		}
		target := count
		if sub == "add-batch" {
			target = existing + count
		}
		if target <= existing {
			fmt.Printf("%d tenants exist; nothing to add (provisioning is additive, never removes)\n", existing)
			return nil
		}
		return addRange(ctx, e, existing, target, cdc)
	case "list":
		return listTenants(ctx, e)
	case "status":
		var slug string
		if _, err := flags("tenant status", args, func(fs *flag.FlagSet) { fs.StringVar(&slug, "slug", "", "tenant slug") }); err != nil {
			return err
		}
		return tenantStatus(ctx, e, slug)
	}
	return fmt.Errorf("unknown tenant subcommand %q", sub)
}

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func profileIndex(slug string) (int, bool) {
	if !strings.HasPrefix(slug, "tenant-") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(slug, "tenant-"))
	return n - 1, err == nil && n > 0
}

func tenantCount(ctx context.Context, e *env) (int, error) {
	var n int
	err := e.control.QueryRow(ctx, `SELECT count(*) FROM tenants`).Scan(&n)
	return n, err
}

// addRange provisions tenant-(from+1) .. tenant-(to) using the skew profile of
// a fleet of size `to`. Existing slugs are left untouched.
func addRange(ctx context.Context, e *env, from, to int, cdc string) error {
	for i := from; i < to; i++ {
		p := gen.ProfileFor(i, max(to, 100))
		t, err := e.prov.Add(ctx, provision.AddRequest{Slug: p.Slug, Name: p.Name, Tier: p.Tier, Timezone: p.Timezone, Currency: p.Currency, CDC: cdc})
		if err != nil {
			return fmt.Errorf("%s: %w", p.Slug, err)
		}
		slog.Info("tenant provisioned", "tenant", t.Slug, "tier", p.Tier, "tz", p.Timezone, "currency", p.Currency, "created", t.Created, "cdc", cdc)
	}
	return nil
}

func listTenants(ctx context.Context, e *env) error {
	rows, err := e.control.Query(ctx, `
		SELECT t.tenant_slug, t.display_name, t.size_tier, t.status, t.timezone, t.default_currency, t.source_database,
		       t.source_schema_version, t.active_generation, COALESCE(g.desired_state, '-'), COALESCE(g.observed_state, '-')
		FROM tenants t LEFT JOIN tenant_generations g ON g.tenant_id = t.tenant_id AND g.generation = t.active_generation
		ORDER BY t.tenant_slug`)
	if err != nil {
		return err
	}
	defer rows.Close()
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "SLUG\tNAME\tTIER\tSTATUS\tTZ\tCUR\tDB\tSCHEMA\tGEN\tCDC DESIRED\tCDC OBSERVED")
	n := 0
	for rows.Next() {
		var slug, name, tier, status, tz, cur, db, des, obs string
		var schema int
		var genN int64
		if err := rows.Scan(&slug, &name, &tier, &status, &tz, &cur, &db, &schema, &genN, &des, &obs); err != nil {
			return err
		}
		n++
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s\t%s\n", slug, name, tier, status, tz, cur, db, schema, genN, des, obs)
	}
	w.Flush()
	fmt.Printf("%d tenants\n", n)
	return rows.Err()
}

func tenantStatus(ctx context.Context, e *env, slug string) error {
	var id, status, db, slot, des, obs string
	var genN int64
	var schema int
	var lastErr *string
	err := e.control.QueryRow(ctx, `
		SELECT t.tenant_id::text, t.status, t.source_database, t.source_schema_version, t.active_generation,
		       g.replication_slot_name, g.desired_state, COALESCE(g.observed_state, '-'), g.last_error
		FROM tenants t JOIN tenant_generations g ON g.tenant_id = t.tenant_id AND g.generation = t.active_generation
		WHERE t.tenant_slug = $1`, slug).Scan(&id, &status, &db, &schema, &genN, &slot, &des, &obs, &lastErr)
	if err != nil {
		return fmt.Errorf("tenant %s: %w", slug, err)
	}
	fmt.Printf("tenant      %s (%s)\nstatus      %s\ndatabase    %s (schema v%d, expected v%d)\ngeneration  %d\ncapture     slot=%s desired=%s observed=%s\n",
		slug, id, status, db, schema, migrations.SourceSchemaVersion, genN, slot, des, obs)
	if lastErr != nil {
		fmt.Printf("last error  %s\n", *lastErr)
	}
	if st, err := chq.TenantStatus(ctx, e.chAPI, id, genN); err == nil && st.Found {
		fmt.Printf("snapshot    %s\nevents      %d applied since ingester start\nlast commit %v\ndata as of  %v\napply lag   %v ms\nslot lag    %d bytes\n",
			st.SnapshotState, st.EventsTotal, deref(st.LastSourceCommitAt), deref(st.DataAsOf()), deref(st.LastApplyLagMs), st.SlotLagBytes)
	}
	rows, err := e.control.Query(ctx, `SELECT step, status, attempts, COALESCE(last_error, '') FROM provisioning_steps WHERE tenant_id = $1 ORDER BY updated_at`, id)
	if err == nil {
		for rows.Next() {
			var step, st, le string
			var att int
			_ = rows.Scan(&step, &st, &att, &le)
			fmt.Printf("step        %-20s %-6s attempts=%d %s\n", step, st, att, le)
		}
		rows.Close()
	}
	return nil
}

func deref[T any](p *T) any {
	if p == nil {
		return "-"
	}
	return *p
}

func migrateSourceAll(ctx context.Context, e *env) error {
	rows, err := e.control.Query(ctx, `SELECT t.tenant_slug, t.source_database, c.endpoint_secret_ref FROM tenants t JOIN source_clusters c USING (source_cluster_id) ORDER BY 1`)
	if err != nil {
		return err
	}
	type t struct{ slug, db, ref string }
	ts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (t, error) {
		var x t
		return x, r.Scan(&x.slug, &x.db, &x.ref)
	})
	if err != nil {
		return err
	}
	for _, x := range ts { // sequential by design: never migrate every tenant at once
		dsn, err := config.SecretRef(x.ref)
		if err != nil {
			return err
		}
		dsn, _ = pg.WithDatabase(dsn, x.db)
		v, err := pg.Migrate(ctx, dsn, migrations.Source, "source")
		if err != nil {
			return fmt.Errorf("%s: %w", x.slug, err)
		}
		_, _ = e.control.Exec(ctx, `UPDATE tenants SET source_schema_version = $2 WHERE tenant_slug = $1`, x.slug, v)
		fmt.Printf("%s: schema v%d\n", x.slug, v)
	}
	return nil
}

// captureCmd pauses or resumes change capture for every tenant (used to
// measure source OLTP latency with and without replication).
func captureCmd(ctx context.Context, e *env, sub string) error {
	state := map[string]string{"pause-all": "paused", "resume-all": "running"}[sub]
	if state == "" {
		return fmt.Errorf("unknown capture subcommand %q (pause-all | resume-all)", sub)
	}
	tag, err := e.control.Exec(ctx, `UPDATE tenant_generations SET desired_state = $1 WHERE desired_state IN ('running', 'paused') AND state NOT IN ('retired', 'failed')`, state)
	if err != nil {
		return err
	}
	fmt.Printf("%d tenant streams set to %s (the ingester applies it within ~2 s)\n", tag.RowsAffected(), state)
	time.Sleep(3 * time.Second)
	return nil
}

func waitActive(ctx context.Context, e *env, args []string) error {
	var timeout time.Duration
	if _, err := flags("wait-active", args, func(fs *flag.FlagSet) { fs.DurationVar(&timeout, "timeout", 30*time.Minute, "max wait") }); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		var total, active, failed int
		if err := e.control.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status = 'active'), count(*) FILTER (WHERE status = 'failed') FROM tenants`).
			Scan(&total, &active, &failed); err != nil {
			return err
		}
		fmt.Printf("\r%s  active %d/%d  failed %d   ", time.Now().Format("15:04:05"), active, total, failed)
		if total > 0 && active == total {
			fmt.Println("\nall tenants active")
			return nil
		}
		if failed > 0 {
			fmt.Println()
			return fmt.Errorf("%d tenants failed provisioning (tpctl tenant list)", failed)
		}
		if time.Now().After(deadline) {
			fmt.Println()
			return fmt.Errorf("timeout waiting for tenants to become active")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
