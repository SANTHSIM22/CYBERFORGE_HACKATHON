package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/config"
	"tenantpulse/internal/directsql"
	"tenantpulse/internal/domain"
	"tenantpulse/internal/pg"
)

type vTenant struct {
	id, slug, tz, cur, db, ref string
	gen                        int64
	loc                        *time.Location
}

func verifyCmd(ctx context.Context, e *env, args []string) error {
	var slug string
	var limit int
	if _, err := flags("verify", args, func(fs *flag.FlagSet) {
		fs.StringVar(&slug, "slug", "", "only this tenant")
		fs.IntVar(&limit, "max", 0, "verify at most N tenants (0 = all)")
	}); err != nil {
		return err
	}
	ts, err := activeTenants(ctx, e, slug)
	if err != nil {
		return err
	}
	if limit > 0 && len(ts) > limit {
		ts = ts[:limit]
	}
	totalChecks, failed := 0, 0
	start := time.Now()
	for _, t := range ts {
		checks, diffs, err := verifyTenant(ctx, e, t, t.gen)
		if err != nil {
			return fmt.Errorf("%s: %w", t.slug, err)
		}
		totalChecks += checks
		if len(diffs) == 0 {
			fmt.Printf("✓ %-12s %3d/%d checks match direct SQL exactly\n", t.slug, checks, checks)
			continue
		}
		failed++
		fmt.Printf("✗ %-12s %d mismatches\n", t.slug, len(diffs))
		for i, d := range diffs {
			if i == 8 {
				fmt.Printf("    ... %d more\n", len(diffs)-8)
				break
			}
			fmt.Println("    " + d)
		}
	}
	fmt.Printf("\n%d tenants, %d checks, %d tenants with mismatches (%s)\n", len(ts), totalChecks, failed, time.Since(start).Round(time.Millisecond))
	if slug == "" && limit == 0 {
		orgs, checks, bad, err := verifyOrgs(ctx, e)
		if err != nil {
			return err
		}
		fmt.Printf("%d orgs, %d checks, %d orgs with mismatches\n", orgs, checks, bad)
		failed += bad
	}
	if failed > 0 {
		return fmt.Errorf("reconciliation found mismatches")
	}
	return nil
}

// verifyOrgs checks every org dashboard's exact numbers: for three viewer
// timezones and three presets, the org's per-currency totals (added up from
// each store's 15-minute cells, as the API does) must equal plain SQL on each
// member store's own database, added up.
func verifyOrgs(ctx context.Context, e *env) (orgs, checks, failed int, err error) {
	rows, err := e.control.Query(ctx, `
		SELECT o.org_slug, t.tenant_id::text, t.tenant_slug, t.timezone, t.default_currency, t.source_database, c.endpoint_secret_ref, t.active_generation
		FROM tenants t JOIN orgs o USING (org_id) JOIN source_clusters c USING (source_cluster_id)
		WHERE t.status = 'active' ORDER BY o.org_slug, t.tenant_slug`)
	if err != nil {
		return 0, 0, 0, err
	}
	members := map[string][]vTenant{}
	var order []string
	for rows.Next() {
		var org string
		var t vTenant
		if err := rows.Scan(&org, &t.id, &t.slug, &t.tz, &t.cur, &t.db, &t.ref, &t.gen); err != nil {
			rows.Close()
			return 0, 0, 0, err
		}
		if _, ok := members[org]; !ok {
			order = append(order, org)
		}
		members[org] = append(members[org], t)
	}
	rows.Close()
	for _, org := range order {
		var n int
		var diffs []string
		for attempt := 1; attempt <= 3; attempt++ { // retry only if writes landed between the two reads
			if n, diffs, err = compareOrg(ctx, e, members[org]); err != nil || len(diffs) == 0 {
				break
			}
		}
		if err != nil {
			return 0, 0, 0, fmt.Errorf("org %s: %w", org, err)
		}
		orgs++
		checks += n
		if len(diffs) == 0 {
			fmt.Printf("✓ org %-14s %3d/%d checks match direct SQL on its %d stores exactly\n", org, n, n, len(members[org]))
			continue
		}
		failed++
		fmt.Printf("✗ org %-14s %d mismatches\n", org, len(diffs))
		for i, d := range diffs {
			if i == 8 {
				fmt.Printf("    ... %d more\n", len(diffs)-8)
				break
			}
			fmt.Println("    " + d)
		}
	}
	return orgs, checks, failed, nil
}

type exactTotals struct {
	orders     int64
	gross, rec decimal.Decimal
}

func compareOrg(ctx context.Context, e *env, ts []vTenant) (int, []string, error) {
	srcs := make([]*pgx.Conn, 0, len(ts))
	defer func() {
		for _, c := range srcs {
			c.Close(context.Background())
		}
	}()
	for _, t := range ts {
		src, err := sourceConn(ctx, t)
		if err != nil {
			return 0, nil, err
		}
		srcs = append(srcs, src)
		if err := watermark(ctx, e, src, t.id, t.gen); err != nil {
			return 0, nil, err
		}
	}
	checks := 0
	var diffs []string
	for _, tz := range []string{"UTC", "Asia/Kolkata", "America/New_York"} {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return 0, nil, err
		}
		for _, days := range []int{1, 7, 30} {
			r := domain.PresetRange(time.Now(), days, loc)
			got, want := map[string]exactTotals{}, map[string]exactTotals{}
			for i, t := range ts {
				cells, err := chq.OrgOrders(ctx, e.chAPI, chq.Params{TenantID: t.id, Generation: t.gen, TZ: tz,
					From: r.From.UTC(), To: r.To.UTC(), Timeout: 30 * time.Second}, "", time.Time{})
				if err != nil {
					return 0, nil, err
				}
				for _, c := range cells {
					g := got[c.Currency]
					got[c.Currency] = exactTotals{g.orders + c.Orders, g.gross.Add(c.Gross), g.rec.Add(c.Recognized)}
				}
				cur, _, err := directsql.Summary(ctx, srcs[i], r.From.Add(-r.To.Sub(r.From)), r.From, r.To)
				if err != nil {
					return 0, nil, err
				}
				for _, c := range cur {
					w := want[c.Currency]
					want[c.Currency] = exactTotals{w.orders + c.Orders, w.gross.Add(decimal.RequireFromString(c.GrossSales)),
						w.rec.Add(decimal.RequireFromString(c.RecognizedSales))}
				}
			}
			checks++
			label := fmt.Sprintf("viewer in %s, %dd", tz, days)
			for c, w := range want {
				if g := got[c]; g.orders != w.orders || !g.gross.Equal(w.gross) || !g.rec.Equal(w.rec) {
					diffs = append(diffs, fmt.Sprintf("%s %s: got %d / %s / %s, want %d / %s / %s", label, c, g.orders, g.gross, g.rec, w.orders, w.gross, w.rec))
				}
			}
			for c := range got {
				if _, ok := want[c]; !ok {
					diffs = append(diffs, fmt.Sprintf("%s %s: in the org total but not in the stores", label, c))
				}
			}
		}
	}
	return checks, diffs, nil
}

func activeTenants(ctx context.Context, e *env, slug string) ([]vTenant, error) {
	rows, err := e.control.Query(ctx, `
		SELECT t.tenant_id::text, t.tenant_slug, t.timezone, t.default_currency, t.source_database, c.endpoint_secret_ref, t.active_generation
		FROM tenants t JOIN source_clusters c USING (source_cluster_id)
		WHERE t.status = 'active' AND ($1 = '' OR t.tenant_slug = $1) ORDER BY t.tenant_slug`, slug)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (vTenant, error) {
		var t vTenant
		err := r.Scan(&t.id, &t.slug, &t.tz, &t.cur, &t.db, &t.ref, &t.gen)
		t.cur = strings.TrimSpace(t.cur)
		t.loc, _ = time.LoadLocation(t.tz)
		return t, err
	})
}

// verifyRanges mixes dashboard presets with awkward ranges: partial seconds,
// minutes and hours at both edges, local-midnight crossings and DST days.
func verifyRanges(now time.Time, loc *time.Location) []domain.TimeRange {
	var out []domain.TimeRange
	for _, d := range []int{1, 7, 30, 365} {
		out = append(out, domain.PresetRange(now, d, loc))
	}
	three := domain.PresetRange(now, 3, loc).From
	out = append(out,
		domain.TimeRange{From: now.Add(-72*time.Hour + 17*time.Minute + 13500*time.Millisecond), To: now.Add(-11*time.Hour + 7*time.Minute)},
		domain.TimeRange{From: now.Add(-36 * time.Hour), To: now.Add(time.Hour)},
		domain.TimeRange{From: three.Add(-90 * time.Minute), To: three.Add(26*time.Hour + 37*time.Minute)},
		domain.TimeRange{From: time.Date(2026, 3, 7, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)},     // US DST start
		domain.TimeRange{From: time.Date(2026, 3, 28, 12, 0, 0, 0, time.UTC), To: time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)},  // EU DST start
		domain.TimeRange{From: time.Date(2025, 11, 1, 18, 30, 0, 0, time.UTC), To: time.Date(2025, 11, 3, 6, 15, 0, 0, time.UTC)}, // US DST end
	)
	return out
}

func sourceConn(ctx context.Context, t vTenant) (*pgx.Conn, error) {
	dsn, err := config.SecretRef(t.ref)
	if err != nil {
		return nil, err
	}
	dsn, _ = pg.WithDatabase(dsn, t.db)
	return pgx.Connect(ctx, dsn)
}

func verifyTenant(ctx context.Context, e *env, t vTenant, gen int64) (int, []string, error) {
	src, err := sourceConn(ctx, t)
	if err != nil {
		return 0, nil, err
	}
	defer src.Close(ctx)
	var diffs []string
	checks := 0
	for attempt := 1; attempt <= 3; attempt++ { // retry only if writes landed between the two reads
		if err := watermark(ctx, e, src, t.id, gen); err != nil {
			return 0, nil, err
		}
		checks, diffs, err = compareAll(ctx, e, src, t, gen)
		if err != nil || len(diffs) == 0 {
			return checks, diffs, err
		}
	}
	return checks, diffs, nil
}

// watermark writes a source heartbeat and waits until ClickHouse holds it. The
// change stream is in commit order, so everything committed before is there too.
func watermark(ctx context.Context, e *env, src *pgx.Conn, tenantID string, gen int64) error {
	var ts time.Time
	if err := src.QueryRow(ctx, `UPDATE tp_heartbeat SET ts = clock_timestamp() WHERE id = 1 RETURNING ts`).Scan(&ts); err != nil {
		return err
	}
	target := ts.Truncate(time.Millisecond)
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		st, err := chq.TenantStatus(ctx, e.chAPI, tenantID, gen)
		if err == nil && st.LastHeartbeatSourceAt != nil && !st.LastHeartbeatSourceAt.Before(target) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("pipeline did not reach the watermark within 2 minutes")
}

func compareAll(ctx context.Context, e *env, src *pgx.Conn, t vTenant, gen int64) (int, []string, error) {
	currencies, err := chq.Currencies(ctx, e.chAPI, t.id, gen)
	if err != nil {
		return 0, nil, err
	}
	if len(currencies) == 0 {
		currencies = []string{t.cur}
	}
	tx, err := src.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The API answers most reads from an in-memory view of the rollups; every
	// answer it can give is checked against the store database too.
	view := chq.NewView(t.id, gen, t.tz)
	if err := view.Sync(ctx, e.chAPI, 0, 30*time.Second); err != nil {
		return 0, nil, fmt.Errorf("view: %w", err)
	}

	var diffs []string
	checks := 0
	add := func(label string, ds []string) {
		checks++
		for _, d := range ds {
			diffs = append(diffs, label+": "+d)
		}
	}
	for _, r := range verifyRanges(time.Now(), t.loc) {
		label := fmt.Sprintf("[%s, %s)", r.From.Format(time.RFC3339), r.To.Format(time.RFC3339))
		p := chq.Params{TenantID: t.id, Generation: gen, TZ: t.tz, From: r.From, To: r.To, Limit: 10, Timeout: 30 * time.Second}

		got, err := chq.StatusRows(ctx, e.chAPI, p)
		if err != nil {
			return 0, nil, err
		}
		want, err := directsql.StatusRows(ctx, tx, r.From, r.To, "")
		if err != nil {
			return 0, nil, err
		}
		add(label+" status×currency", chq.DiffStatusRows(got, want))
		if rows, ok := view.StatusRows(p); ok {
			add(label+" view status×currency", chq.DiffStatusRows(rows, want))
		}

		wantC, wantP, err := directsql.Summary(ctx, tx, r.From.Add(-r.To.Sub(r.From)), r.From, r.To)
		if err != nil {
			return 0, nil, err
		}
		gotC, gotP, err := chq.Summary(ctx, e.chAPI, p)
		if err != nil {
			return 0, nil, err
		}
		add(label+" summary", append(chq.DiffTotals(gotC, wantC), chq.DiffTotals(gotP, wantP)...))
		if c, pv, ok := view.Summary(p); ok {
			add(label+" view summary", append(chq.DiffTotals(c, wantC), chq.DiffTotals(pv, wantP)...))
		}

		grains := []string{"day"}
		if r.To.Sub(r.From) <= 31*24*time.Hour {
			grains = append(grains, "hour")
		}
		for _, g := range grains {
			pg := p
			pg.Grain, pg.Currency = g, t.cur
			gotT, err := chq.Trend(ctx, e.chAPI, pg)
			if err != nil {
				return 0, nil, err
			}
			wantT, err := directsql.Trend(ctx, tx, r.From, r.To, t.tz, g, t.cur)
			if err != nil {
				return 0, nil, err
			}
			add(label+" "+g+" trend", chq.DiffTrend(gotT, wantT))
			if pts, ok := view.Trend(pg); ok {
				add(label+" view "+g+" trend", chq.DiffTrend(pts, wantT))
			}
		}
		for _, c := range currencies {
			for _, offset := range []int{0, 10} { // first page and the next: ranks 1-10, 11-20
				pc := p
				pc.Currency, pc.Offset = c, offset
				page := fmt.Sprintf(" top products %d-%d %s", offset+1, offset+pc.Limit, c)
				gotP, gotN, err := chq.TopProducts(ctx, e.chAPI, pc)
				if err != nil {
					return 0, nil, err
				}
				wantP, wantN, err := directsql.TopProducts(ctx, tx, r.From, r.To, c, pc.Limit, offset)
				if err != nil {
					return 0, nil, err
				}
				if len(wantP) == 0 && gotN == 0 {
					wantN = 0 // past the last page: no row carries the total
				}
				add(label+page, append(chq.DiffTop(gotP, wantP), diffCount(gotN, wantN)...))
				if top, n, ok := view.TopProducts(pc); ok {
					if len(wantP) == 0 {
						n = wantN // the view knows the total even past the last page
					}
					add(label+" view"+page, append(chq.DiffTop(top, wantP), diffCount(n, wantN)...))
				}
			}
		}
	}
	return checks, diffs, nil
}

// diffCount compares the number of products that sold in a range.
func diffCount(got, want int) []string {
	if got == want {
		return nil
	}
	return []string{fmt.Sprintf("products that sold: got %d, want %d", got, want)}
}
