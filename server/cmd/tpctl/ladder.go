package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/directsql"
	"tenantpulse/internal/domain"
)

// ladderCmd answers "optimize the main database, or not?" with numbers: the
// same dashboard queries at four optimization levels, on the same data.
//
//	L0  source Postgres, index scans disabled (unoptimized baseline)
//	L1  source Postgres, B-tree on created_at (schema default)
//	L2  source Postgres + covering indexes (best-case main-DB tuning)
//	L3  ClickHouse (what the dashboard uses; API cache not involved)
//
// L2's covering indexes are created CONCURRENTLY and dropped afterwards;
// their write cost is what `tpctl oltp` / pgbench measure separately.
func ladderCmd(ctx context.Context, e *env, args []string) error {
	var slug string
	var iters int
	var keep bool
	if _, err := flags("ladder", args, func(fs *flag.FlagSet) {
		fs.StringVar(&slug, "slug", "tenant-001", "tenant (use the largest for the worst case)")
		fs.IntVar(&iters, "iterations", 20, "runs per query per level")
		fs.BoolVar(&keep, "keep-indexes", false, "keep the L2 covering indexes")
	}); err != nil {
		return err
	}
	ts, err := activeTenants(ctx, e, slug)
	if err != nil || len(ts) == 0 {
		return fmt.Errorf("tenant %s not active", slug)
	}
	t := ts[0]
	src, err := sourceConn(ctx, t)
	if err != nil {
		return err
	}
	defer src.Close(ctx)
	var orders int64
	_ = src.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&orders)

	type query struct {
		name   string
		r      domain.TimeRange
		direct func(pgx.Tx) error
		ch     func() error
	}
	now := time.Now()
	var qs []query
	for _, days := range []int{7, 30, 365} {
		r := domain.PresetRange(now, days, t.loc)
		p := chq.Params{TenantID: t.id, Generation: t.gen, TZ: t.tz, From: r.From, To: r.To, Currency: t.cur, Grain: "day", Limit: 10, Timeout: 60 * time.Second}
		ps := p
		ps.Currency = ""
		qs = append(qs,
			query{fmt.Sprintf("totals %dd", days), r,
				func(tx pgx.Tx) error { _, err := directsql.StatusRows(ctx, tx, r.From, r.To, ""); return err },
				func() error { _, err := chq.StatusRows(ctx, e.chAPI, ps); return err }},
			query{fmt.Sprintf("daily trend %dd", days), r,
				func(tx pgx.Tx) error {
					_, err := directsql.Trend(ctx, tx, r.From, r.To, t.tz, "day", t.cur)
					return err
				},
				func() error { _, err := chq.Trend(ctx, e.chAPI, p); return err }},
			query{fmt.Sprintf("top-10 products %dd", days), r,
				func(tx pgx.Tx) error { _, err := directsql.TopProducts(ctx, tx, r.From, r.To, t.cur, 10); return err },
				func() error { _, err := chq.TopProducts(ctx, e.chAPI, p); return err }},
		)
	}

	runDirect := func(q query, noIndex bool) (pctl, error) {
		var d []float64
		for i := 0; i < iters; i++ {
			tx, err := src.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
			if err != nil {
				return pctl{}, err
			}
			if noIndex {
				_, _ = tx.Exec(ctx, `SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = off`)
			}
			s := time.Now()
			err = q.direct(tx)
			d = append(d, float64(time.Since(s).Microseconds())/1000)
			_ = tx.Rollback(ctx)
			if err != nil {
				return pctl{}, err
			}
		}
		return summarize(d), nil
	}
	results := map[string][4]pctl{}
	fmt.Printf("ladder for %s (%d orders in its database), %d runs each\n", t.slug, orders, iters)
	for _, q := range qs {
		var r [4]pctl
		if r[0], err = runDirect(q, true); err != nil {
			return err
		}
		if r[1], err = runDirect(q, false); err != nil {
			return err
		}
		results[q.name] = r
	}

	fmt.Println("creating covering indexes for L2 (CONCURRENTLY)...")
	for _, ddl := range []string{
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS tp_ladder_orders_cover ON orders (created_at) INCLUDE (status, currency, total_amount, deleted_at)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS tp_ladder_items_cover ON order_items (order_id) INCLUDE (product_id, quantity, unit_price, deleted_at)`,
		`VACUUM (ANALYZE) orders`, `VACUUM (ANALYZE) order_items`,
	} {
		if _, err := src.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("%s: %w", ddl, err)
		}
	}
	for _, q := range qs {
		r := results[q.name]
		if r[2], err = runDirect(q, false); err != nil {
			return err
		}
		var d []float64
		for i := 0; i < iters; i++ {
			s := time.Now()
			if err := q.ch(); err != nil {
				return err
			}
			d = append(d, float64(time.Since(s).Microseconds())/1000)
		}
		r[3] = summarize(d)
		results[q.name] = r
	}
	if !keep {
		_, _ = src.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS tp_ladder_orders_cover`)
		_, _ = src.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS tp_ladder_items_cover`)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "query (p50 / p95 ms)\tL0 no index\tL1 B-tree\tL2 covering\tL3 ClickHouse\tL1→L3 speedup\t")
	for _, q := range qs {
		r := results[q.name]
		f := func(p pctl) string { return fmt.Sprintf("%.1f / %.1f", p.P50, p.P95) }
		speed := 0.0
		if r[3].P50 > 0 {
			speed = r[1].P50 / r[3].P50
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%.1fx\t\n", q.name, f(r[0]), f(r[1]), f(r[2]), f(r[3]), speed)
	}
	w.Flush()
	out := map[string]any{"tenant": t.slug, "orders": orders, "iterations": iters, "levels": []string{"L0 no index", "L1 btree", "L2 covering", "L3 clickhouse"}, "results": results}
	b, _ := json.MarshalIndent(out, "", "  ")
	saveResult("ladder-"+t.slug, b)
	publishBench(ctx, "ladder", time.Now(), b)
	return nil
}
