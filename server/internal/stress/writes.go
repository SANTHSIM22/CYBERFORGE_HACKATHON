package stress

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------- write storm
//
// The read stress test hammers dashboards while stores take light traffic.
// The write storm is the other real-world case: every store taking orders,
// status changes and cancellations at a high rate while people watch their
// dashboards the way the real page does (load four widgets, poll freshness
// every second, reload when the data version changes). It measures what each
// side feels: store transaction latency and throughput, how long a committed
// order takes to be shown on a dashboard, ingestion throughput, and dashboard
// latency, with the dashboards on the aggregation layer and, for contrast, on
// the store primaries.

// WriteConfig configures RunWrites.
type WriteConfig struct {
	Tenants    []Tenant
	AppDSN     string            // store workload role
	Tokens     map[string]string // tenant slug -> bearer token (viewers, freshness probe)
	Operator   string            // operator token (pipeline stats)
	APIURL     string            // aggregation layer
	DirectURL  string            // primary-DB instance; "" skips that phase
	Rate       int               // target store transactions per second, all stores together
	Workers    int               // store clients
	Viewers    int               // concurrent dashboard viewers
	Duration   time.Duration     // measured time per phase
	Warmup     time.Duration     // unmeasured time per phase
	Source     string            // cli | dashboard
	CPUPinning bool
	// Optional (CLI): pause and resume change capture to measure the stores
	// with the analytics pipeline fully off, and how fast it catches up after.
	PauseCapture, ResumeCapture func(context.Context) error
}

// WritePhase is one phase of the write storm.
type WritePhase struct {
	Key           string         `json:"key"`
	Phase         string         `json:"phase"`
	Target        string         `json:"target"`
	DurationSecs  float64        `json:"duration_s"`
	StoreTPS      float64        `json:"store_tps"`       // store transactions per second (all kinds)
	StoreWriteTPS float64        `json:"store_write_tps"` // checkouts + status changes + cancellations per second
	Store         Pctl           `json:"store_latency"`
	StoreWrites   Pctl           `json:"store_write_latency"`
	StoreErrors   int            `json:"store_errors"`
	StoreSlowdown float64        `json:"store_p95_slowdown_pct"` // vs the first phase
	Viewers       int            `json:"viewers"`
	DashRequests  int            `json:"dashboard_requests"`
	DashRPS       float64        `json:"dashboard_rps"`
	Dash          Pctl           `json:"dashboard_latency"`
	FreshPoll     Pctl           `json:"freshness_poll_latency"`
	DashErrors    map[string]int `json:"dashboard_errors"`
	CacheHit      float64        `json:"cache_hit_ratio"`
	Visible       Pctl           `json:"commit_to_dashboard"` // marker order committed -> dashboard can show it
	VisibleMissed int            `json:"commit_to_dashboard_timeouts"`
	IngestEPS     float64        `json:"ingest_events_per_s"`
	IngestPeak    float64        `json:"ingest_events_per_s_peak"`
	DataAgeP95    float64        `json:"data_age_p95_ms_max"` // worst per-sample p95 data age across stores
	CatchUpSecs   float64        `json:"catch_up_s,omitempty"`
	// Transactions the store databases ran that the store workload did not
	// issue (Postgres pg_stat_database): dashboard queries reaching the stores.
	ForeignStoreTx    int64   `json:"store_db_foreign_transactions"`
	ForeignStoreTxSec float64 `json:"store_db_foreign_transactions_per_s"`
}

// WriteResult is a complete write-storm run.
type WriteResult struct {
	Kind       string       `json:"kind"`
	At         time.Time    `json:"at"`
	Source     string       `json:"source"`
	Tenants    int          `json:"tenants"`
	TargetTPS  int          `json:"target_tps"`
	Workers    int          `json:"workers"`
	Viewers    int          `json:"viewers"`
	CPUPinning bool         `json:"cpu_pinning"`
	Phases     []WritePhase `json:"phases"`
}

// WriteMix is the store workload during a write storm.
var WriteMix = Mix{Read: 10, Checkout: 60, Advance: 20, Cancel: 10}

type writeDef struct {
	key, label, target, url string
	viewers, probe, off     bool
}

// RunWrites executes the write storm. progress (optional) is called about
// once a second.
func RunWrites(ctx context.Context, cfg WriteConfig, progress func(Progress)) (WriteResult, error) {
	if len(cfg.Tenants) == 0 {
		return WriteResult{}, fmt.Errorf("no active tenants")
	}
	cfg.Workers = max(cfg.Workers, 8)
	pools, closeAll, err := PrepareOLTP(ctx, cfg.AppDSN, cfg.Tenants, 2)
	if err != nil {
		return WriteResult{}, err
	}
	defer closeAll()
	hc := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 512, IdleConnTimeout: 30 * time.Second}}
	defer hc.CloseIdleConnections()

	var defs []writeDef
	if cfg.PauseCapture != nil && cfg.ResumeCapture != nil {
		defs = append(defs, writeDef{"pipeline_off", "Pipeline off: store writes only, capture paused", "", "", false, false, true})
	}
	defs = append(defs,
		writeDef{"baseline", "Store writes, pipeline on, nobody viewing", "", "", false, true, false},
		writeDef{"aggregation", "Store writes + dashboards on the aggregation layer", "api", cfg.APIURL, true, true, false})
	if cfg.DirectURL != "" {
		defs = append(defs, writeDef{"direct", "Store writes + dashboards querying the primary DB", "api-direct", cfg.DirectURL, true, false, false})
	}

	res := WriteResult{Kind: "writestorm", At: time.Now().UTC(), Source: cfg.Source, Tenants: len(cfg.Tenants), TargetTPS: cfg.Rate,
		Workers: cfg.Workers, Viewers: cfg.Viewers, CPUPinning: cfg.CPUPinning}
	for i, d := range defs {
		ph, err := runWritePhase(ctx, cfg, pools, hc, d, func(stage string, el, total time.Duration) {
			if progress != nil {
				progress(Progress{Phase: d.label, PhaseIndex: i, PhaseCount: len(defs), Stage: stage,
					Elapsed: el.Seconds(), Total: total.Seconds(), Completed: append([]WritePhase(nil), res.Phases...)})
			}
		})
		if err != nil {
			return res, fmt.Errorf("%s: %w", d.key, err)
		}
		if base := res.Phases; len(base) > 0 && base[0].Store.P95 > 0 {
			ph.StoreSlowdown = (ph.Store.P95/base[0].Store.P95 - 1) * 100
		}
		res.Phases = append(res.Phases, ph)
	}
	if progress != nil {
		progress(Progress{Phase: "done", PhaseIndex: len(defs), PhaseCount: len(defs), Stage: "done", Completed: res.Phases})
	}
	return res, nil
}

type writeRun struct {
	cfg       WriteConfig
	hc        *http.Client
	measuring atomic.Bool

	mu       sync.Mutex
	dash     []float64
	poll     []float64
	errs     map[string]int
	hits     int
	lookups  int
	visible  []float64
	missed   int
	probeTx  int64 // transactions issued by the freshness probe
	eps      []float64
	dataAge  float64
	ingestOK bool
}

func runWritePhase(ctx context.Context, cfg WriteConfig, pools []OLTPTenant, hc *http.Client, d writeDef,
	report func(stage string, el, total time.Duration)) (WritePhase, error) {
	if d.off {
		if err := cfg.PauseCapture(ctx); err != nil {
			return WritePhase{}, fmt.Errorf("pause capture: %w", err)
		}
	}
	w := &writeRun{cfg: cfg, hc: hc, errs: map[string]int{}}
	total := cfg.Warmup + cfg.Duration
	pctx, cancel := context.WithTimeout(ctx, total)
	defer cancel()
	store := NewLatencies()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); RunWorkload(pctx, pools, cfg.Workers, cfg.Rate, WriteMix, store) }()
	if d.viewers {
		for v := 0; v < cfg.Viewers; v++ {
			wg.Add(1)
			go func(v int) { defer wg.Done(); w.viewer(pctx, d.url, uint64(v)) }(v)
		}
	}
	if d.probe {
		wg.Add(1)
		go func() { defer wg.Done(); w.probe(pctx, pools) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); w.sampleStats(pctx) }()

	start := time.Now()
	tick := time.NewTicker(time.Second)
	var measured time.Time
	var x0 int64
	for running := true; running; {
		select {
		case <-pctx.Done():
			running = false
		case <-tick.C:
			el := time.Since(start)
			if !w.measuring.Load() && el >= cfg.Warmup {
				store.Reset()
				w.reset()
				x0, _ = storeXacts(ctx, pools)
				measured = time.Now()
				w.measuring.Store(true)
			}
			stage := "warming_up"
			if w.measuring.Load() {
				stage = "measuring"
			}
			report(stage, el, total)
		}
	}
	tick.Stop()
	w.measuring.Store(false)
	elapsed := time.Since(measured).Seconds()
	x1, xerr := storeXacts(ctx, pools)
	wg.Wait()
	if measured.IsZero() || elapsed <= 0 {
		return WritePhase{}, fmt.Errorf("phase ended before measuring")
	}

	ph := WritePhase{Key: d.key, Phase: d.label, Target: d.target, DurationSecs: elapsed, Viewers: 0, DashErrors: w.errs}
	all, writes := store.All(), store.Some("checkout", "status_update", "cancel")
	ph.Store, ph.StoreWrites, ph.StoreErrors = Summarize(all), Summarize(writes), store.Errors
	ph.StoreTPS, ph.StoreWriteTPS = float64(len(all))/elapsed, float64(len(writes))/elapsed
	if d.viewers {
		ph.Viewers = cfg.Viewers
		ph.Dash, ph.FreshPoll, ph.DashRequests = Summarize(w.dash), Summarize(w.poll), len(w.dash)+len(w.poll)
		ph.DashRPS = float64(ph.DashRequests) / elapsed
		if w.lookups > 0 {
			ph.CacheHit = float64(w.hits) / float64(w.lookups)
		}
	}
	ph.Visible, ph.VisibleMissed = Summarize(w.visible), w.missed
	if len(w.eps) > 0 {
		var sum float64
		for _, v := range w.eps {
			sum += v
			ph.IngestPeak = max(ph.IngestPeak, v)
		}
		ph.IngestEPS = sum / float64(len(w.eps))
	}
	ph.DataAgeP95 = w.dataAge
	if xerr == nil && x0 > 0 {
		issued := int64(len(all)+store.Errors) + w.probeTx
		ph.ForeignStoreTx = max(0, x1-x0-issued)
		ph.ForeignStoreTxSec = float64(ph.ForeignStoreTx) / elapsed
	}

	if d.off { // the pipeline comes back: how long until every store is current again?
		if err := cfg.ResumeCapture(ctx); err != nil {
			return ph, fmt.Errorf("resume capture: %w", err)
		}
		t0 := time.Now()
		for time.Since(t0) < 5*time.Minute {
			if age, ok := w.maxDataAge(ctx); ok && age < 5000 {
				ph.CatchUpSecs = time.Since(t0).Seconds()
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return ph, nil
}

func (w *writeRun) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dash, w.poll, w.errs, w.hits, w.lookups, w.visible, w.missed, w.eps, w.dataAge, w.probeTx = nil, nil, map[string]int{}, 0, 0, nil, 0, nil, 0, 0
}

// get performs one API read and returns its latency (ms), cache status and HTTP status.
func (w *writeRun) get(ctx context.Context, url, token string, into any) (float64, string, int, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	s := time.Now()
	resp, err := w.hc.Do(req)
	if err != nil {
		return 0, "", 0, err
	}
	defer resp.Body.Close()
	if into != nil && resp.StatusCode == 200 {
		err = json.NewDecoder(resp.Body).Decode(into)
	} else {
		var sink [4096]byte
		for {
			if _, e := resp.Body.Read(sink[:]); e != nil {
				break
			}
		}
	}
	return float64(time.Since(s).Microseconds()) / 1000, resp.Header.Get("X-Cache"), resp.StatusCode, err
}

func (w *writeRun) record(kind string, ms float64, cache string, status int, err error) {
	if !w.measuring.Load() {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case err != nil:
		w.errs["network"]++
	case status != 200:
		w.errs[fmt.Sprintf("http_%d", status)]++
	case kind == "poll":
		w.poll = append(w.poll, ms)
	default:
		w.dash = append(w.dash, ms)
		if cache != "" && cache != "DIRECT" {
			w.lookups++
			if cache == "HIT" || cache == "STALE" {
				w.hits++
			}
		}
	}
}

// viewer behaves like the dashboard page: load the four widgets, poll
// freshness every second, reload (asking for at least the new version) when
// the store's data changes, and switch store or range every 20-40 s.
func (w *writeRun) viewer(ctx context.Context, base string, seed uint64) {
	r := rand.New(rand.NewPCG(seed+101, uint64(time.Now().UnixNano())))
	pick := func() (Tenant, string) {
		return w.cfg.Tenants[r.IntN(len(w.cfg.Tenants))], ranges[r.IntN(len(ranges))]
	}
	t, rng := pick()
	load := func(minV string) {
		for _, ep := range endpoints {
			u := base + "/v1/tenants/" + t.Slug + "/" + ep + "?range=" + rng
			if minV != "" {
				u += "&min_version=" + minV
			}
			ms, cache, status, err := w.get(ctx, u, w.cfg.Tokens[t.Slug], nil)
			if ctx.Err() != nil {
				return
			}
			w.record("widget", ms, cache, status, err)
		}
	}
	if !sleepCtx(ctx, time.Duration(r.IntN(1000))*time.Millisecond) { // spread viewers over the first second
		return
	}
	load("")
	version := ""
	switchAt := time.Now().Add(time.Duration(20+r.IntN(20)) * time.Second)
	for sleepCtx(ctx, 900*time.Millisecond+time.Duration(r.IntN(200))*time.Millisecond) {
		var f struct {
			DataVersion string `json:"data_version"`
		}
		ms, _, status, err := w.get(ctx, base+"/v1/tenants/"+t.Slug+"/freshness", w.cfg.Tokens[t.Slug], &f)
		if ctx.Err() != nil {
			return
		}
		w.record("poll", ms, "", status, err)
		if f.DataVersion != "" && f.DataVersion != version {
			first := version == ""
			version = f.DataVersion
			if !first {
				load(version)
			}
		}
		if time.Now().After(switchAt) {
			t, rng = pick()
			version = ""
			load("")
			switchAt = time.Now().Add(time.Duration(20+r.IntN(20)) * time.Second)
		}
	}
}

// probe commits a marker order in a random store twice a second and times
// how long until the aggregation API reports the store's data as current to
// a moment after that commit (so the order can be on the dashboard).
func (w *writeRun) probe(ctx context.Context, pools []OLTPTenant) {
	r := rand.New(rand.NewPCG(7, uint64(time.Now().UnixNano())))
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		t := pools[r.IntN(len(pools))]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w.measuring.Load() {
				w.mu.Lock()
				w.probeTx += 2 // the marker insert and the clock read
				w.mu.Unlock()
			}
			if _, err := t.pool.Exec(ctx, `INSERT INTO orders (status, currency, total_amount) VALUES ('placed', $1, 1)`, t.cur); err != nil {
				return
			}
			committed := time.Now()
			var after time.Time // source clock, just after the commit
			if err := t.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&after); err != nil {
				return
			}
			for ctx.Err() == nil && time.Since(committed) < 60*time.Second {
				var f struct {
					AsOf *time.Time `json:"data_as_of"`
				}
				_, _, status, err := w.get(ctx, w.cfg.APIURL+"/v1/tenants/"+t.slug+"/freshness", w.cfg.Tokens[t.slug], &f)
				if err == nil && status == 200 && f.AsOf != nil && !f.AsOf.Before(after) {
					if w.measuring.Load() {
						w.mu.Lock()
						w.visible = append(w.visible, float64(time.Since(committed).Microseconds())/1000)
						w.mu.Unlock()
					}
					return
				}
				if !sleepCtx(ctx, 25*time.Millisecond) {
					return
				}
			}
			if ctx.Err() == nil && w.measuring.Load() {
				w.mu.Lock()
				w.missed++
				w.mu.Unlock()
			}
		}()
	}
}

type adminStats struct {
	Ingesters []struct {
		EventsPerSec float64 `json:"events_per_sec"`
	} `json:"ingesters"`
	Pipeline struct {
		DataAgeP95 float64 `json:"data_age_p95_ms"`
		DataAgeMax float64 `json:"data_age_max_ms"`
	} `json:"pipeline"`
}

// sampleStats records ingestion throughput and data age every 2 s.
func (w *writeRun) sampleStats(ctx context.Context) {
	for sleepCtx(ctx, 2*time.Second) {
		var s adminStats
		_, _, status, err := w.get(ctx, w.cfg.APIURL+"/v1/admin/stats", w.cfg.Operator, &s)
		if err != nil || status != 200 || !w.measuring.Load() {
			continue
		}
		var eps float64
		for _, i := range s.Ingesters {
			eps += i.EventsPerSec
		}
		w.mu.Lock()
		w.eps = append(w.eps, eps)
		w.dataAge = max(w.dataAge, s.Pipeline.DataAgeP95)
		w.mu.Unlock()
	}
}

func (w *writeRun) maxDataAge(ctx context.Context) (float64, bool) {
	var s adminStats
	_, _, status, err := w.get(ctx, w.cfg.APIURL+"/v1/admin/stats", w.cfg.Operator, &s)
	return s.Pipeline.DataAgeMax, err == nil && status == 200
}

// storeXacts sums committed and rolled-back transactions over every store database.
func storeXacts(ctx context.Context, pools []OLTPTenant) (int64, error) {
	var n int64
	err := pools[0].pool.QueryRow(ctx, `SELECT coalesce(sum(xact_commit + xact_rollback), 0) FROM pg_stat_database WHERE datname LIKE 'tenant_%'`).Scan(&n)
	return n, err
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// WriteReport renders a write-storm result as a Markdown table.
func WriteReport(res WriteResult) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Write storm: %d stores, target %d store transactions/s, %d dashboard viewers\n\n", res.Tenants, res.TargetTPS, res.Viewers)
	fmt.Fprintf(&b, "| Phase | Store tx/s | Store p50 / p95 / p99 | vs first | Dashboard p50 / p95 / p99 | Order visible p50 / p95 / p99 | Ingest events/s | Catch-up |\n")
	fmt.Fprintf(&b, "|---|---:|---:|---:|---:|---:|---:|---:|\n")
	for _, p := range res.Phases {
		dash, vis, cu := "—", "—", "—"
		if p.Dash.Count > 0 {
			dash = fmt.Sprintf("%.1f / %.1f / %.1f ms", p.Dash.P50, p.Dash.P95, p.Dash.P99)
		}
		if p.Visible.Count > 0 {
			vis = fmt.Sprintf("%.0f / %.0f / %.0f ms", p.Visible.P50, p.Visible.P95, p.Visible.P99)
		}
		if p.CatchUpSecs > 0 {
			cu = fmt.Sprintf("%.1f s", p.CatchUpSecs)
		}
		fmt.Fprintf(&b, "| %s | %.0f | %.1f / %.1f / %.1f ms | %+.1f%% | %s | %s | %.0f | %s |\n", p.Phase, p.StoreTPS,
			p.Store.P50, p.Store.P95, p.Store.P99, p.StoreSlowdown, dash, vis, p.IngestEPS, cu)
	}
	return b.String()
}
