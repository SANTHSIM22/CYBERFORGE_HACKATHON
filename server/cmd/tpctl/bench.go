package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ClickHouse/clickhouse-go/v2"
	"tenantpulse/internal/auth"
	"tenantpulse/internal/ch"
	"tenantpulse/internal/config"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/stress"
)

type pctl = stress.Pctl

func summarize(v []float64) pctl { return stress.Summarize(v) }

// oltpCmd runs the fixed-rate store workload (point reads, recent-order lists,
// checkouts, status updates) on tenant source databases. Run it with capture
// paused and running to measure what change capture costs the store.
func oltpCmd(ctx context.Context, e *env, args []string) error {
	var dur time.Duration
	var workers, nTenants, rate int
	var label string
	if _, err := flags("oltp", args, func(fs *flag.FlagSet) {
		fs.DurationVar(&dur, "duration", 60*time.Second, "run time")
		fs.IntVar(&workers, "workers", 8, "concurrent clients")
		fs.IntVar(&nTenants, "tenants", 10, "tenants to spread load over")
		fs.IntVar(&rate, "rate", 300, "total operations per second (fixed rate keeps runs comparable)")
		fs.StringVar(&label, "label", "run", "label for the result file, e.g. capture-off / capture-on")
	}); err != nil {
		return err
	}
	ts, err := activeTenants(ctx, e, "")
	if err != nil {
		return err
	}
	if len(ts) > nTenants {
		ts = ts[:nTenants]
	}
	var st []stress.Tenant
	for _, t := range ts {
		st = append(st, stress.Tenant{Slug: t.slug, Database: t.db, Currency: t.cur})
	}
	pools, cleanup, err := stress.PrepareOLTP(ctx, config.MustString("SOURCE_APP_DSN"), st, int32(max(2, workers/2)))
	if err != nil {
		return err
	}
	defer cleanup()
	if len(pools) == 0 {
		return fmt.Errorf("no seeded tenants to run against")
	}
	l := stress.NewLatencies()
	rctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()
	start := time.Now()
	stress.RunOLTP(rctx, pools, workers, rate, l)
	elapsed := time.Since(start)
	all := l.All()
	per := map[string]pctl{}
	for op, v := range l.Ops {
		per[op] = summarize(v)
	}
	res := map[string]any{"label": label, "duration_s": elapsed.Seconds(), "target_rate": rate, "workers": workers, "tenants": len(pools),
		"errors": l.Errors, "overall": summarize(all), "per_op": per, "throughput_ops": float64(len(all)) / elapsed.Seconds()}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	saveResult("oltp-"+label, b)
	return nil
}

func ignoreNoRows(err error) error {
	if err == pgx.ErrNoRows {
		return nil
	}
	return err
}

func saveResult(name string, b []byte) {
	if dir := config.String("RESULTS_DIR", ""); dir != "" {
		_ = os.MkdirAll(dir, 0o755)
		_ = os.WriteFile(filepath.Join(dir, name+"-"+time.Now().UTC().Format("20060102-150405")+".json"), b, 0o644)
	}
}

// freshnessCmd measures source commit -> (a) analytics projection visible and
// (b) visible through the dashboard API's normal cached path.
func freshnessCmd(ctx context.Context, e *env, args []string) error {
	var slug string
	var samples int
	var gap time.Duration
	if _, err := flags("freshness", args, func(fs *flag.FlagSet) {
		fs.StringVar(&slug, "slug", "tenant-001", "tenant")
		fs.IntVar(&samples, "samples", 30, "orders to insert")
		fs.DurationVar(&gap, "interval", 500*time.Millisecond, "pause between samples")
	}); err != nil {
		return err
	}
	ts, err := activeTenants(ctx, e, slug)
	if err != nil || len(ts) == 0 {
		return fmt.Errorf("tenant %s not active", slug)
	}
	t := ts[0]
	dsn, _ := pg.WithDatabase(config.MustString("SOURCE_APP_DSN"), t.db)
	src, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer src.Close(ctx)
	signer, err := auth.NewSigner(config.MustString("AUTH_SECRET"))
	if err != nil {
		return err
	}
	token := signer.Issue("operator")
	apiBase := config.String("API_INTERNAL_URL", "http://api:8080")
	hc := &http.Client{Timeout: 5 * time.Second}
	apiCount := func() (int64, error) {
		req, _ := http.NewRequestWithContext(ctx, "GET", apiBase+"/v1/tenants/"+slug+"/orders/summary?range=today", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := hc.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		var env struct {
			Data struct {
				Total int64 `json:"total_orders"`
			} `json:"data"`
		}
		return env.Data.Total, json.NewDecoder(resp.Body).Decode(&env)
	}

	var proj, api []float64
	for i := 0; i < samples; i++ {
		before, _ := apiCount()
		var id string
		if err := src.QueryRow(ctx, `INSERT INTO orders (status, currency, total_amount) VALUES ('placed', $1, 10) RETURNING id::text`, t.cur).Scan(&id); err != nil {
			return err
		}
		committed := time.Now()
		gotProj, gotAPI := false, false
		for time.Since(committed) < 60*time.Second && !(gotProj && gotAPI) {
			if !gotProj {
				if visible(ctx, e, t, id) {
					gotProj = true
					proj = append(proj, float64(time.Since(committed).Microseconds())/1000)
				}
			}
			if !gotAPI {
				if c, err := apiCount(); err == nil && c > before {
					gotAPI = true
					api = append(api, float64(time.Since(committed).Microseconds())/1000)
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Printf("\rsample %d/%d", i+1, samples)
		time.Sleep(gap)
	}
	fmt.Println()
	res := map[string]any{"tenant": slug, "samples": samples,
		"commit_to_analytics": summarize(proj), "commit_to_dashboard_api": summarize(api),
		"note": "dashboard path includes coalesced cache invalidation (CACHE_BUMP_INTERVAL)"}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	saveResult("freshness-"+slug, b)
	return nil
}

// visible reports whether an order id is queryable in ClickHouse (tenant-scoped, as the API reads).
func visible(ctx context.Context, e *env, t vTenant, id string) bool {
	qctx := ch.TenantCtx(ctx, t.id, 2*time.Second, clickhouse.Parameters{"t": t.id, "g": strconv.FormatInt(t.gen, 10), "id": id})
	var n uint64
	if err := e.chAPI.QueryRow(qctx, `SELECT count() FROM tp.orders WHERE tenant_id = {t:UUID} AND generation = {g:UInt32} AND order_id = {id:UUID}`).Scan(&n); err != nil {
		return false
	}
	return n > 0
}
