package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/provision"
)

// rebuildCmd (B-2) rebuilds a tenant into a shadow generation while the
// current one keeps serving: new replication slot, consistent snapshot into
// generation g+1, catch-up, exact verification, then an atomic registry
// cutover. The old generation is kept for rollback; nothing is truncated.
func rebuildCmd(ctx context.Context, e *env, args []string) error {
	var slug string
	var timeout time.Duration
	var cutover bool
	if _, err := flags("rebuild", args, func(fs *flag.FlagSet) {
		fs.StringVar(&slug, "slug", "", "tenant slug")
		fs.DurationVar(&timeout, "timeout", 60*time.Minute, "max time for snapshot + catch-up")
		fs.BoolVar(&cutover, "cutover", true, "switch active_generation after verification")
	}); err != nil {
		return err
	}
	var id string
	var active, next int64
	if err := e.control.QueryRow(ctx, `SELECT t.tenant_id::text, t.active_generation,
	        (SELECT max(generation) + 1 FROM tenant_generations g WHERE g.tenant_id = t.tenant_id)
	        FROM tenants t WHERE t.tenant_slug = $1 AND t.status = 'active'`, slug).Scan(&id, &active, &next); err != nil {
		return fmt.Errorf("tenant %s must exist and be active: %w", slug, err)
	}
	n := provision.NamesFor(slug, next)
	fmt.Printf("rebuilding %s: generation %d keeps serving; building generation %d (slot %s)\n", slug, active, next, n.Slot)
	err := pgx.BeginFunc(ctx, e.control, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO tenant_generations (tenant_id, generation, replication_slot_name, publication_name, state, desired_state)
		                           VALUES ($1, $2, $3, $4, 'building', 'running')`, id, next, n.Slot, n.Publication); err != nil {
			return err
		}
		details, _ := json.Marshal(map[string]any{"from_generation": active, "to_generation": next})
		_, err := tx.Exec(ctx, `INSERT INTO tenant_admin_audit (tenant_id, action, actor, details) VALUES ($1, 'tenant.rebuild.start', 'tpctl', $2)`, id, details)
		return err
	})
	if err != nil {
		return err
	}

	deadline := time.Now().Add(timeout)
	for {
		st, _ := chq.TenantStatus(ctx, e.chAPI, id, next)
		fmt.Printf("\rgeneration %d: %-10s", next, st.SnapshotState)
		if st.SnapshotState == "completed" {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("generation %d did not catch up in time (generation %d still serving)", next, active)
		}
		time.Sleep(time.Second)
	}
	fmt.Println()

	ts, err := activeTenants(ctx, e, slug)
	if err != nil || len(ts) == 0 {
		return fmt.Errorf("load tenant: %v", err)
	}
	checks, diffs, err := verifyTenant(ctx, e, ts[0], next)
	if err != nil {
		return err
	}
	if len(diffs) > 0 {
		_, _ = e.control.Exec(ctx, `UPDATE tenant_generations SET state = 'failed', failure_reason = $3 WHERE tenant_id = $1 AND generation = $2`,
			id, next, fmt.Sprintf("%d reconciliation mismatches", len(diffs)))
		return fmt.Errorf("generation %d failed verification (%d mismatches); generation %d still serving", next, len(diffs), active)
	}
	fmt.Printf("generation %d verified: %d/%d checks match direct SQL\n", next, checks, checks)
	if !cutover {
		return nil
	}
	err = pgx.BeginFunc(ctx, e.control, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE tenants SET active_generation = $2, updated_at = now() WHERE tenant_id = $1`, id, next); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE tenant_generations SET state = 'active', completed_at = now() WHERE tenant_id = $1 AND generation = $2`, id, next); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO tenant_admin_audit (tenant_id, action, actor, details) VALUES ($1, 'tenant.rebuild.cutover', 'tpctl', jsonb_build_object('generation', $2::bigint))`, id, next)
		return err
	})
	if err != nil {
		return err
	}
	fmt.Printf("cutover complete: %s now serves generation %d (APIs pick it up within 5 s)\n", slug, next)
	fmt.Printf("generation %d keeps streaming for rollback: UPDATE tenants SET active_generation = %d WHERE tenant_slug = '%s';\n", active, active, slug)
	return nil
}
