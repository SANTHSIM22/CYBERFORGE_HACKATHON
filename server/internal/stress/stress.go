// Package stress compares two API instances under identical concurrent load
// while a fixed-rate store (OLTP) workload runs on the primary databases:
//
//	baseline     store workload only
//	direct       dashboards query each store's primary Postgres (api-direct)
//	aggregation  dashboards use ClickHouse rollups + version-keyed cache (api)
//	clickhouse   same instance, cache bypassed (ClickHouse alone)
//
// Used by `tpctl stress` and by the dashboard's "Run stress test" button.
package stress

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"tenantpulse/internal/ch"
	"tenantpulse/internal/domain"
	"tenantpulse/internal/pg"
)

// ---------------------------------------------------------------- latency helpers

type Latencies struct {
	mu     sync.Mutex
	Ops    map[string][]float64
	Errors int
}

func NewLatencies() *Latencies { return &Latencies{Ops: map[string][]float64{}} }

func (l *Latencies) Add(op string, d time.Duration, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		l.Errors++
		return
	}
	l.Ops[op] = append(l.Ops[op], float64(d.Microseconds())/1000)
}

// Reset drops everything recorded so far (end of a warm-up).
func (l *Latencies) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.Ops, l.Errors = map[string][]float64{}, 0
}

// Some returns the samples of the given operations.
func (l *Latencies) Some(ops ...string) []float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []float64
	for _, op := range ops {
		out = append(out, l.Ops[op]...)
	}
	return out
}

func (l *Latencies) All() []float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var all []float64
	for _, v := range l.Ops {
		all = append(all, v...)
	}
	return all
}

type Pctl struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
}

func Summarize(v []float64) Pctl {
	if len(v) == 0 {
		return Pctl{}
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	at := func(q float64) float64 { return s[min(len(s)-1, int(q*float64(len(s))))] }
	return Pctl{len(s), at(.5), at(.95), at(.99)}
}

// ---------------------------------------------------------------- store workload

type Tenant struct {
	Slug     string
	Database string
	Currency string
}

type OLTPTenant struct {
	pool   *pgxpool.Pool
	ids    []string
	prods  []string
	cur    string
	slug   string
	recent *recentOrders // orders created during the run: status changes act on these
}

type recentOrders struct {
	mu   sync.Mutex
	ids  []string
	next int
}

func (r *recentOrders) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ids) < 512 {
		r.ids = append(r.ids, id)
		return
	}
	r.ids[r.next] = id
	r.next = (r.next + 1) % len(r.ids)
}

func (r *recentOrders) pick(rnd *rand.Rand) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ids) == 0 {
		return "", false
	}
	return r.ids[rnd.IntN(len(r.ids))], true
}

// PrepareOLTP opens one pool per tenant with the application role and samples
// existing order/product ids for point reads and updates.
func PrepareOLTP(ctx context.Context, appDSN string, ts []Tenant, maxConns int32) ([]OLTPTenant, func(), error) {
	var out []OLTPTenant
	for _, t := range ts {
		dsn, _ := pg.WithDatabase(appDSN, t.Database)
		p, err := pg.Connect(ctx, dsn, pg.PoolOptions{MaxConns: maxConns, AppName: "tp-oltp"})
		if err != nil {
			return nil, nil, err
		}
		rows, _ := p.Query(ctx, `SELECT id::text FROM orders TABLESAMPLE SYSTEM (2) LIMIT 2000`)
		ids, _ := pgx.CollectRows(rows, pgx.RowTo[string])
		rows, _ = p.Query(ctx, `SELECT id::text FROM products WHERE is_active LIMIT 200`)
		prods, _ := pgx.CollectRows(rows, pgx.RowTo[string])
		if len(ids) == 0 || len(prods) == 0 {
			p.Close()
			continue
		}
		out = append(out, OLTPTenant{p, ids, prods, t.Currency, t.Slug, &recentOrders{}})
	}
	return out, func() {
		for _, t := range out {
			t.pool.Close()
		}
	}, nil
}

// Mix is a store workload as percentages: point reads, recent-order lists,
// checkouts (order + two lines), status advances and cancellations.
type Mix struct{ Read, List, Checkout, Advance, Cancel int }

// ReadMix is the light store traffic of the dashboard stress test.
var ReadMix = Mix{Read: 40, List: 20, Checkout: 25, Advance: 15}

// RunOLTP drives the ReadMix store workload at a fixed rate until ctx ends.
func RunOLTP(ctx context.Context, pools []OLTPTenant, workers, rate int, l *Latencies) {
	RunWorkload(ctx, pools, workers, rate, ReadMix, l)
}

// RunWorkload drives a fixed-rate store workload with the given mix until ctx ends.
func RunWorkload(ctx context.Context, pools []OLTPTenant, workers, rate int, mix Mix, l *Latencies) {
	if len(pools) == 0 || workers <= 0 || rate <= 0 {
		<-ctx.Done()
		return
	}
	// Work cut off by the end of the run is not a store error.
	add := func(op string, d time.Duration, err error) {
		if ctx.Err() == nil {
			l.Add(op, d, err)
		}
	}
	interval := time.Duration(float64(time.Second) * float64(workers) / float64(rate))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), uint64(time.Now().UnixNano())))
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
				t := pools[r.IntN(len(pools))]
				op, s := r.IntN(100), time.Now()
				var err error
				switch {
				case op < mix.Read:
					var st string
					err = t.pool.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1`, t.ids[r.IntN(len(t.ids))]).Scan(&st)
					if err == pgx.ErrNoRows {
						err = nil
					}
					add("point_read", time.Since(s), err)
				case op < mix.Read+mix.List:
					rows, e2 := t.pool.Query(ctx, `SELECT id, status, total_amount FROM orders WHERE created_at >= now() - interval '1 day' ORDER BY created_at DESC LIMIT 20`)
					if e2 == nil {
						for rows.Next() {
						}
						rows.Close()
						e2 = rows.Err()
					}
					add("recent_list", time.Since(s), e2)
				case op < mix.Read+mix.List+mix.Checkout:
					err = pgx.BeginFunc(ctx, t.pool, func(tx pgx.Tx) error {
						var id string
						p1, p2 := t.prods[r.IntN(len(t.prods))], t.prods[r.IntN(len(t.prods))]
						a, b := domain.UnitPrice(p1), domain.UnitPrice(p2)
						if err := tx.QueryRow(ctx, `INSERT INTO orders (status, currency, total_amount) VALUES ('placed', $1, $2) RETURNING id::text`,
							t.cur, a.Add(b).String()).Scan(&id); err != nil {
							return err
						}
						_, err := tx.Exec(ctx, `INSERT INTO order_items (order_id, product_id, line_number, quantity, unit_price)
						                       VALUES ($1, $2, 1, 1, $3), ($1, $4, 2, 1, $5)`, id, p1, a.String(), p2, b.String())
						if err == nil {
							t.recent.add(id)
						}
						return err
					})
					add("checkout", time.Since(s), err)
				case op < mix.Read+mix.List+mix.Checkout+mix.Advance:
					id, ok := t.recent.pick(r)
					if !ok {
						id = t.ids[r.IntN(len(t.ids))]
					}
					_, err = t.pool.Exec(ctx, `UPDATE orders SET status = CASE status WHEN 'placed' THEN 'paid' WHEN 'paid' THEN 'processing' ELSE 'shipped' END,
					                           version = version + 1, updated_at = now()
					                           WHERE id = $1 AND status IN ('placed', 'paid', 'processing')`, id)
					add("status_update", time.Since(s), err)
				default:
					id, ok := t.recent.pick(r)
					if !ok {
						id = t.ids[r.IntN(len(t.ids))]
					}
					_, err = t.pool.Exec(ctx, `UPDATE orders SET status = 'cancelled', version = version + 1, updated_at = now()
					                           WHERE id = $1 AND status IN ('placed', 'paid')`, id)
					add("cancel", time.Since(s), err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

// ---------------------------------------------------------------- the test

type Config struct {
	Tenants     []Tenant
	AppDSN      string            // store workload role on the source cluster
	Tokens      map[string]string // tenant slug -> bearer token
	Operator    string            // operator token (cache-bypass phase)
	DirectURL   string            // instance that queries the primary DB
	APIURL      string            // aggregation-layer instance
	VUs         int
	Duration    time.Duration
	Warmup      time.Duration
	OLTPRate    int
	OLTPWorkers int
	Phases      string // comma list: baseline,direct,aggregation,clickhouse
	Source      string // "cli" or "dashboard"
	CPUPinning  bool
}

type PhaseResult struct {
	Key          string          `json:"key"`
	Phase        string          `json:"phase"`
	Target       string          `json:"target"`
	DashRequests int             `json:"dashboard_requests"`
	DashRPS      float64         `json:"dashboard_rps"`
	Dash         Pctl            `json:"dashboard_latency"`
	DashErrors   map[string]int  `json:"dashboard_errors"`
	PerEndpoint  map[string]Pctl `json:"per_endpoint"`
	OLTP         Pctl            `json:"store_oltp_latency"`
	OLTPOps      int             `json:"store_oltp_ops"`
	OLTPSlowdown float64         `json:"store_oltp_p95_slowdown_pct"`
	DurationSecs float64         `json:"duration_s"`
	CacheHit     float64         `json:"cache_hit_ratio"`   // answered from cache (current or previous version)
	CacheStale   float64         `json:"cache_stale_ratio"` // previous version served while refreshing
}

type Result struct {
	At         time.Time     `json:"at"`
	VUs        int           `json:"vus"`
	OLTPRate   int           `json:"oltp_rate"`
	DurationS  float64       `json:"duration_s"`
	Tenants    int           `json:"tenants"`
	Source     string        `json:"source"`
	CPUPinning bool          `json:"cpu_pinning"`
	Phases     []PhaseResult `json:"phases"`
}

// Progress is reported while a run is in flight.
type Progress struct {
	Phase      string  `json:"phase"`
	PhaseIndex int     `json:"phase_index"`
	PhaseCount int     `json:"phase_count"`
	Stage      string  `json:"stage"` // warming_up | measuring | done
	Elapsed    float64 `json:"phase_elapsed_s"`
	Total      float64 `json:"phase_total_s"`
	Completed  any     `json:"completed"` // []PhaseResult or []WritePhase
}

var endpoints = []string{"orders/summary", "orders/breakdown", "orders/trends", "products/top"}
var ranges = []string{"today", "7d", "30d", "90d"}

type phaseDef struct {
	key, label, target string
	url                string
	bypass             bool
}

// Run executes the phases and returns the results. progress (optional) is
// called about once a second.
func Run(ctx context.Context, cfg Config, progress func(Progress)) (Result, error) {
	if len(cfg.Tenants) == 0 {
		return Result{}, fmt.Errorf("no active tenants")
	}
	if cfg.Phases == "" {
		cfg.Phases = "baseline,direct,aggregation,clickhouse"
	}
	pools, cleanup, err := PrepareOLTP(ctx, cfg.AppDSN, cfg.Tenants, int32(max(2, cfg.OLTPWorkers)))
	if err != nil {
		return Result{}, err
	}
	defer cleanup()
	hc := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: cfg.VUs * 2, MaxConnsPerHost: cfg.VUs * 2}}
	defer hc.CloseIdleConnections()

	all := []phaseDef{
		{"baseline", "Baseline: store workload only", "", "", false},
		{"direct", "Direct: query the primary DB", "api-direct", cfg.DirectURL, false},
		{"aggregation", "Aggregation layer (ClickHouse + cache)", "api", cfg.APIURL, false},
		{"clickhouse", "ClickHouse only (cache bypassed)", "api", cfg.APIURL, true},
	}
	var phases []phaseDef
	for _, p := range all {
		if strings.Contains(cfg.Phases, p.key) {
			phases = append(phases, p)
		}
	}
	res := Result{At: time.Now().UTC(), VUs: cfg.VUs, OLTPRate: cfg.OLTPRate, DurationS: cfg.Duration.Seconds(),
		Tenants: len(cfg.Tenants), Source: cfg.Source, CPUPinning: cfg.CPUPinning}
	var baselineP95 float64
	report := func(i int, stage string, started time.Time, total time.Duration) {
		if progress != nil {
			progress(Progress{Phase: phases[i].label, PhaseIndex: i, PhaseCount: len(phases), Stage: stage,
				Elapsed: time.Since(started).Seconds(), Total: total.Seconds(), Completed: append([]PhaseResult(nil), res.Phases...)})
		}
	}
	for i, ph := range phases {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		total := cfg.Duration
		if ph.url != "" {
			total += cfg.Warmup
		}
		started := time.Now()
		tick := time.NewTicker(time.Second)
		stopTick := make(chan struct{})
		stage := "measuring"
		if ph.url != "" && cfg.Warmup > 0 {
			stage = "warming_up"
		}
		var stageMu sync.Mutex
		go func() {
			for {
				select {
				case <-stopTick:
					return
				case <-tick.C:
					stageMu.Lock()
					s := stage
					stageMu.Unlock()
					report(i, s, started, total)
				}
			}
		}()
		report(i, stage, started, total)
		if ph.url != "" && cfg.Warmup > 0 {
			wctx, cancel := context.WithTimeout(ctx, cfg.Warmup)
			runDash(wctx, hc, ph, cfg, nil)
			cancel()
			stageMu.Lock()
			stage = "measuring"
			stageMu.Unlock()
		}
		pctx, cancel := context.WithTimeout(ctx, cfg.Duration)
		ol := NewLatencies()
		ds := &dashStats{per: map[string][]float64{}, errs: map[string]int{}}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); RunOLTP(pctx, pools, cfg.OLTPWorkers, cfg.OLTPRate, ol) }()
		mStart := time.Now()
		if ph.url != "" {
			runDash(pctx, hc, ph, cfg, ds)
		} else {
			<-pctx.Done()
		}
		wg.Wait()
		cancel()
		tick.Stop()
		close(stopTick)
		elapsed := time.Since(mStart).Seconds()

		per := map[string]Pctl{}
		var allDash []float64
		for k, v := range ds.per {
			per[k] = Summarize(v)
			allDash = append(allDash, v...)
		}
		r := PhaseResult{Key: ph.key, Phase: ph.label, Target: ph.target, DashRequests: len(allDash), DashRPS: float64(len(allDash)) / elapsed,
			Dash: Summarize(allDash), DashErrors: ds.errs, PerEndpoint: per, OLTP: Summarize(ol.All()), OLTPOps: len(ol.All()), DurationSecs: elapsed}
		if len(allDash) > 0 {
			r.CacheHit = float64(ds.hits) / float64(len(allDash))
			r.CacheStale = float64(ds.stale) / float64(len(allDash))
		}
		if ph.key == "baseline" {
			baselineP95 = r.OLTP.P95
		} else if baselineP95 > 0 {
			r.OLTPSlowdown = (r.OLTP.P95 - baselineP95) / baselineP95 * 100
		}
		res.Phases = append(res.Phases, r)
	}
	if progress != nil {
		progress(Progress{Phase: "done", PhaseIndex: len(phases), PhaseCount: len(phases), Stage: "done", Completed: res.Phases})
	}
	return res, nil
}

type dashStats struct {
	mu    sync.Mutex
	per   map[string][]float64
	errs  map[string]int
	hits  int
	stale int
}

func runDash(ctx context.Context, hc *http.Client, ph phaseDef, cfg Config, ds *dashStats) {
	var wg sync.WaitGroup
	for v := 0; v < cfg.VUs; v++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(v)+7, uint64(time.Now().UnixNano())))
			for ctx.Err() == nil {
				t := cfg.Tenants[r.IntN(len(cfg.Tenants))]
				ep := endpoints[r.IntN(len(endpoints))]
				u := fmt.Sprintf("%s/v1/tenants/%s/%s?range=%s", ph.url, t.Slug, ep, ranges[r.IntN(len(ranges))])
				req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
				if ph.bypass {
					req.Header.Set("Authorization", "Bearer "+cfg.Operator)
					req.Header.Set("X-TP-Cache", "bypass")
				} else {
					req.Header.Set("Authorization", "Bearer "+cfg.Tokens[t.Slug])
				}
				s := time.Now()
				resp, err := hc.Do(req)
				d := float64(time.Since(s).Microseconds()) / 1000
				if ctx.Err() != nil {
					if resp != nil {
						resp.Body.Close()
					}
					return
				}
				key, cache := "", ""
				if err != nil {
					key = "network"
				} else {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode != 200 {
						key = fmt.Sprintf("http_%d", resp.StatusCode)
					}
					cache = resp.Header.Get("X-Cache")
				}
				if ds == nil {
					continue // warm-up
				}
				ds.mu.Lock()
				if key != "" {
					ds.errs[key]++
				} else {
					ds.per[ep] = append(ds.per[ep], d)
					switch cache {
					case "HIT":
						ds.hits++
					case "STALE":
						ds.hits++
						ds.stale++
					}
				}
				ds.mu.Unlock()
			}
		}(v)
	}
	wg.Wait()
}

// ---------------------------------------------------------------- report

func Report(res Result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## Stress test: direct primary DB vs aggregation layer\n\n")
	fmt.Fprintf(&b, "%d tenants · %d concurrent dashboard users (no think time) · store workload %d ops/s · %.0fs per phase · latencies are client-measured round trips in ms\n\n",
		res.Tenants, res.VUs, res.OLTPRate, res.DurationS)
	fmt.Fprintf(&b, "| Phase | Dashboard req/s | p50 | p95 | p99 | Cache hits | Errors | Store p95 | Store p95 vs baseline |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|---:|\n")
	var direct, agg, chOnly *PhaseResult
	for i := range res.Phases {
		r := &res.Phases[i]
		switch r.Key {
		case "direct":
			direct = r
		case "aggregation":
			agg = r
		case "clickhouse":
			chOnly = r
		}
		errs := 0
		for _, n := range r.DashErrors {
			errs += n
		}
		if r.Target == "" {
			fmt.Fprintf(&b, "| %s | — | — | — | — | — | — | %.1f | baseline |\n", r.Phase, r.OLTP.P95)
			continue
		}
		fmt.Fprintf(&b, "| %s | %.0f | %.1f | %.1f | %.1f | %.0f%% | %d | %.1f | %+.1f%% |\n", r.Phase, r.DashRPS,
			r.Dash.P50, r.Dash.P95, r.Dash.P99, r.CacheHit*100, errs, r.OLTP.P95, r.OLTPSlowdown)
	}
	if direct != nil && agg != nil {
		fmt.Fprintf(&b, "\n### Verdict\n\n")
		fmt.Fprintf(&b, "- Aggregation layer vs direct: p50 %s, p95 %s, p99 %s, throughput %s.\n",
			cmpx(direct.Dash.P50, agg.Dash.P50), cmpx(direct.Dash.P95, agg.Dash.P95), cmpx(direct.Dash.P99, agg.Dash.P99), cmpx(agg.DashRPS, direct.DashRPS))
		if chOnly != nil {
			fmt.Fprintf(&b, "- ClickHouse alone (no cache) vs direct: p50 %s, p95 %s, p99 %s, throughput %s.\n",
				cmpx(direct.Dash.P50, chOnly.Dash.P50), cmpx(direct.Dash.P95, chOnly.Dash.P95), cmpx(direct.Dash.P99, chOnly.Dash.P99), cmpx(chOnly.DashRPS, direct.DashRPS))
		}
		fmt.Fprintf(&b, "- Store p95 slowdown under dashboard load: direct %+.1f%% vs aggregation %+.1f%% (PRD: < 10%% great).\n", direct.OLTPSlowdown, agg.OLTPSlowdown)
	}
	return b.String()
}

// cmpx describes b relative to a (a = direct): "N× better" or "N× worse".
func cmpx(a, b float64) string {
	if a <= 0 || b <= 0 {
		return "n/a"
	}
	if r := a / b; r >= 1 {
		return fmt.Sprintf("%.1f× better", r)
	}
	return fmt.Sprintf("%.1f× worse", b/a)
}

// Publish stores a benchmark result in ClickHouse for the dashboard panels.
func Publish(ctx context.Context, dsn, kind string, at time.Time, payload []byte) (string, error) {
	conn, err := ch.Connect(ctx, dsn, 1)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	runID := kind + "-" + at.UTC().Format("20060102-150405")
	err = conn.Exec(ctx, `INSERT INTO tp.benchmark_runs (run_id, kind, at, payload) VALUES (?, ?, ?, ?)`, runID, kind, at.UTC(), string(payload))
	return runID, err
}
