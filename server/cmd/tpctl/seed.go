package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"

	"tenantpulse/internal/config"
	"tenantpulse/internal/gen"
	"tenantpulse/internal/pg"
)

type seedTenant struct {
	slug, tier, tz, cur, db, ref string
	alt                          string
	rows                         int64
	counts                       gen.Counts
	skipped                      bool
	took                         time.Duration
}

func seedCmd(ctx context.Context, e *env, args []string) error {
	var n, days, workers int
	var rows int64
	var seed uint64
	var startCDC bool
	if _, err := flags("seed", args, func(fs *flag.FlagSet) {
		fs.IntVar(&n, "tenants", 3, "number of tenants to seed (missing ones are provisioned)")
		fs.Int64Var(&rows, "rows", 30000, "total rows across all seeded tenants")
		fs.IntVar(&days, "days", 365, "history window in days")
		fs.IntVar(&workers, "workers", 4, "tenants loaded in parallel")
		fs.Uint64Var(&seed, "seed", 42, "deterministic seed")
		fs.BoolVar(&startCDC, "start-cdc", true, "start CDC for seeded tenants afterwards")
	}); err != nil {
		return err
	}
	existing, err := tenantCount(ctx, e)
	if err != nil {
		return err
	}
	if existing < n {
		// New tenants get deferred CDC: load first, then snapshot once, instead
		// of streaming a bulk load through 100 walsenders.
		if err := addRange(ctx, e, existing, n, "deferred"); err != nil {
			return err
		}
	}
	ts, err := loadSeedTenants(ctx, e, n)
	if err != nil {
		return err
	}
	var weight int64
	for _, t := range ts {
		weight += int64(gen.WeightFor(t.tier))
	}
	for _, t := range ts {
		t.rows = rows * int64(gen.WeightFor(t.tier)) / weight
	}

	now := time.Now().UTC().Truncate(time.Second)
	var mu sync.Mutex
	var loaded int64
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for _, t := range ts {
		g.Go(func() error {
			dsn, err := config.SecretRef(t.ref)
			if err != nil {
				return err
			}
			dsn, _ = pg.WithDatabase(dsn, t.db)
			conn, err := pgx.Connect(gctx, dsn)
			if err != nil {
				return fmt.Errorf("%s: %w", t.slug, err)
			}
			defer conn.Close(gctx)
			var has bool
			if err := conn.QueryRow(gctx, `SELECT EXISTS (SELECT 1 FROM orders)`).Scan(&has); err != nil {
				return err
			}
			if has {
				t.skipped = true
				fmt.Printf("%s already has data — skipped (use seed-additive to add rows)\n", t.slug)
				return nil
			}
			loc, _ := time.LoadLocation(t.tz)
			ts0 := time.Now()
			c, err := gen.Load(gctx, conn, gen.Plan{Slug: t.slug, Rows: t.rows, Currency: t.cur, AltCurrency: t.alt, Loc: loc,
				Days: days, Seed: seed, Now: now}, func(c gen.Counts) {
				mu.Lock()
				defer mu.Unlock()
				fmt.Printf("\r%-12s %9d orders  (%s elapsed)        ", t.slug, c.Orders, time.Since(start).Round(time.Second))
			})
			if err != nil {
				return fmt.Errorf("%s: %w", t.slug, err)
			}
			t.counts, t.took = c, time.Since(ts0)
			mu.Lock()
			loaded += c.Total()
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	fmt.Println()
	report(ts, time.Since(start))

	if startCDC {
		var slugs []string
		for _, t := range ts {
			slugs = append(slugs, t.slug)
		}
		k, err := e.prov.StartCDC(ctx, slugs)
		if err != nil {
			return err
		}
		fmt.Printf("capture started for %d deferred tenants; the ingester snapshots them (max %d at a time per cluster)\n",
			k, config.Int("MAX_CONCURRENT_SNAPSHOTS", 2))
	}
	return demoOrgs(ctx, e, 10, false) // a fresh fleet gets demo orgs; existing orgs are left alone
}

func loadSeedTenants(ctx context.Context, e *env, n int) ([]*seedTenant, error) {
	rows, err := e.control.Query(ctx, `
		SELECT t.tenant_slug, t.size_tier, t.timezone, t.default_currency, t.source_database, c.endpoint_secret_ref
		FROM tenants t JOIN source_clusters c USING (source_cluster_id) ORDER BY t.tenant_slug LIMIT $1`, n)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (*seedTenant, error) {
		t := &seedTenant{}
		err := r.Scan(&t.slug, &t.tier, &t.tz, &t.cur, &t.db, &t.ref)
		if i, ok := profileIndex(t.slug); ok {
			t.alt = gen.ProfileFor(i, 100).AltCurrency
		}
		return t, err
	})
}

func report(ts []*seedTenant, took time.Duration) {
	sort.Slice(ts, func(i, j int) bool { return ts[i].counts.Total() > ts[j].counts.Total() })
	w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "tenant\ttier\tcustomers\tproducts\torders\torder_items\ttotal_rows\t")
	var tot gen.Counts
	shown := 0
	for _, t := range ts {
		c := t.counts
		tot.Customers, tot.Products, tot.Orders, tot.OrderItems = tot.Customers+c.Customers, tot.Products+c.Products, tot.Orders+c.Orders, tot.OrderItems+c.OrderItems
		if shown < 12 && !t.skipped {
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%d\t%d\t\n", t.slug, t.tier, c.Customers, c.Products, c.Orders, c.OrderItems, c.Total())
			shown++
		}
	}
	fmt.Fprintf(w, "ALL (%d tenants)\t\t%d\t%d\t%d\t%d\t%d\t\n", len(ts), tot.Customers, tot.Products, tot.Orders, tot.OrderItems, tot.Total())
	w.Flush()
	rate := float64(tot.Total()) / took.Seconds()
	fmt.Printf("loaded %d rows in %s (%.0f rows/s)\n", tot.Total(), took.Round(time.Second), rate)
	if dir := config.String("RESULTS_DIR", ""); dir != "" {
		per := map[string]gen.Counts{}
		for _, t := range ts {
			per[t.slug] = t.counts
		}
		b, _ := json.MarshalIndent(map[string]any{"at": time.Now().UTC(), "totals": tot, "total_rows": tot.Total(),
			"definition": "total_rows = customers + products + orders + order_items", "per_tenant": per, "seconds": took.Seconds()}, "", "  ")
		_ = os.MkdirAll(dir, 0o755)
		_ = os.WriteFile(filepath.Join(dir, "seed-"+time.Now().UTC().Format("20060102-150405")+".json"), b, 0o644)
	}
}

func seedAdditiveCmd(ctx context.Context, e *env, args []string) error {
	var slug string
	var rows int64
	var days int
	if _, err := flags("seed-additive", args, func(fs *flag.FlagSet) {
		fs.StringVar(&slug, "slug", "", "tenant slug")
		fs.Int64Var(&rows, "rows", 10000, "rows to add (orders + items)")
		fs.IntVar(&days, "days", 30, "spread new orders over the last N days")
	}); err != nil {
		return err
	}
	var t seedTenant
	if err := e.control.QueryRow(ctx, `SELECT t.tenant_slug, t.timezone, t.default_currency, t.source_database, c.endpoint_secret_ref
		FROM tenants t JOIN source_clusters c USING (source_cluster_id) WHERE t.tenant_slug = $1`, slug).
		Scan(&t.slug, &t.tz, &t.cur, &t.db, &t.ref); err != nil {
		return fmt.Errorf("tenant %s: %w", slug, err)
	}
	if i, ok := profileIndex(slug); ok {
		t.alt = gen.ProfileFor(i, 100).AltCurrency
	}
	dsn, err := config.SecretRef(t.ref)
	if err != nil {
		return err
	}
	dsn, _ = pg.WithDatabase(dsn, t.db)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	loc, _ := time.LoadLocation(t.tz)
	start := time.Now()
	c, err := gen.Load(ctx, conn, gen.Plan{Slug: slug, Rows: rows, Currency: t.cur, AltCurrency: t.alt, Loc: loc, Days: days, Seed: uint64(start.UnixNano()), Now: time.Now().UTC()}, nil)
	if err != nil {
		return err
	}
	fmt.Printf("%s: added %d orders + %d items in %s; they stream through CDC now\n", slug, c.Orders, c.OrderItems, time.Since(start).Round(time.Millisecond))
	return nil
}
