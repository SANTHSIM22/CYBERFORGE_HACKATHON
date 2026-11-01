package api

import (
	"math"
	"sort"
	"sync"
	"time"
)

// Window keeps per-second latency histograms for the last two minutes so the
// /monitor page works without Prometheus. Histograms count every request
// (no sampling), so a busy fast second weighs as much as it should against a
// quiet slow one.
type Window struct {
	mu      sync.Mutex
	buckets [120]second
}

// Benchmark probes (X-TP-Cache: bypass, sent by the stress test's
// "ClickHouse only" phase and the Compare button) skip the cache on purpose,
// so they are tracked apart from dashboard traffic.
type second struct {
	unix                 int64
	hits, stale, miss    int
	all, cached, db, byp hist
	view, ch             hist // misses split by what computed them (db = both)
}

// hist is a log-scale latency histogram: bucket i covers
// histMin*histBase^i .. histMin*histBase^(i+1) ms (2% resolution, 0.01 ms to ~80 s).
type hist struct {
	n int
	c [histBuckets]uint32
}

const (
	histMin     = 0.01
	histBase    = 1.02
	histBuckets = 800
)

var logBase = math.Log(histBase)

func (h *hist) add(ms float64) {
	i := 0
	if ms > histMin {
		i = min(histBuckets-1, int(math.Log(ms/histMin)/logBase))
	}
	h.c[i]++
	h.n++
}

func (h *hist) merge(o *hist) {
	if o.n == 0 {
		return
	}
	for i, v := range o.c {
		h.c[i] += v
	}
	h.n += o.n
}

// quantile returns the nearest-rank q-quantile (bucket midpoint).
func (h *hist) quantile(q float64) float64 {
	if h.n == 0 {
		return 0
	}
	rank := max(1, int(math.Ceil(q*float64(h.n))))
	seen := 0
	for i, v := range h.c {
		if seen += int(v); seen >= rank {
			if i == 0 {
				return histMin
			}
			return histMin * math.Pow(histBase, float64(i)+0.5)
		}
	}
	return histMin * math.Pow(histBase, histBuckets)
}

func (h *hist) summary() Percentiles {
	return Percentiles{P50: h.quantile(0.50), P95: h.quantile(0.95), P99: h.quantile(0.99), Count: h.n}
}

// Observe records one dashboard read; source ("view" or "clickhouse") splits
// the misses.
func (w *Window) Observe(now time.Time, ms float64, cache, source string) {
	sec := now.Unix()
	w.mu.Lock()
	defer w.mu.Unlock()
	b := &w.buckets[sec%int64(len(w.buckets))]
	if b.unix != sec {
		*b = second{unix: sec}
	}
	switch cache {
	case "BYPASS":
		b.byp.add(ms)
		return
	case "HIT":
		b.hits++
		b.cached.add(ms)
	case "STALE":
		b.stale++
		b.cached.add(ms)
	case "MISS":
		b.miss++
		b.db.add(ms)
		if source == "view" {
			b.view.add(ms)
		} else {
			b.ch.add(ms)
		}
	}
	b.all.add(ms)
}

type Percentiles struct {
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Count int     `json:"count"`
}

type Point struct {
	T        int64   `json:"t"`
	RPS      int     `json:"rps"`
	P95      float64 `json:"p95_ms"`
	HitRatio float64 `json:"hit_ratio"`
	ProbeRPS int     `json:"probe_rps"` // cache-bypassed benchmark reads (ClickHouse only)
	ProbeP95 float64 `json:"probe_p95_ms"`
}

type WindowStats struct {
	WindowSeconds int         `json:"window_seconds"`
	All           Percentiles `json:"all"`        // dashboard traffic (cache + ClickHouse)
	CacheHit      Percentiles `json:"cache_hit"`  // answered from cache (current or stale)
	Database      Percentiles `json:"database"`   // computed on a miss: View + ClickHouse
	View          Percentiles `json:"view"`       // misses answered from the in-memory rollups
	ClickHouse    Percentiles `json:"clickhouse"` // misses that queried ClickHouse
	Probes        Percentiles `json:"probes"`     // benchmark reads that bypass the cache
	Requests      int         `json:"requests"`
	RPS           float64     `json:"rps"`
	HitRatio      float64     `json:"hit_ratio"`   // cache-served share (current + stale)
	StaleRatio    float64     `json:"stale_ratio"` // served from the previous version while refreshing
	Series        []Point     `json:"series"`
}

func (w *Window) Stats(now time.Time, window int) WindowStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	var all, cached, db, byp, view, clk hist
	st := WindowStats{WindowSeconds: window}
	hits, stale, lookups := 0, 0, 0
	cur := now.Unix()
	for s := cur - int64(window) + 1; s <= cur; s++ {
		b := &w.buckets[s%int64(len(w.buckets))]
		p := Point{T: s}
		if b.unix == s {
			all.merge(&b.all)
			cached.merge(&b.cached)
			db.merge(&b.db)
			byp.merge(&b.byp)
			view.merge(&b.view)
			clk.merge(&b.ch)
			hits += b.hits + b.stale
			stale += b.stale
			lookups += b.hits + b.stale + b.miss
			p.RPS = b.all.n
			p.P95 = b.all.quantile(0.95)
			p.ProbeRPS = b.byp.n
			p.ProbeP95 = b.byp.quantile(0.95)
			if n := b.hits + b.stale + b.miss; n > 0 {
				p.HitRatio = float64(b.hits+b.stale) / float64(n)
			}
		}
		st.Series = append(st.Series, p)
	}
	st.All, st.CacheHit, st.Database, st.Probes = all.summary(), cached.summary(), db.summary(), byp.summary()
	st.View, st.ClickHouse = view.summary(), clk.summary()
	st.Requests = all.n
	st.RPS = float64(all.n) / float64(window)
	if lookups > 0 {
		st.HitRatio = float64(hits) / float64(lookups)
		st.StaleRatio = float64(stale) / float64(lookups)
	}
	return st
}

func sorted(v []float64) []float64 {
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	return c
}

func pct(s []float64, q float64) float64 {
	if len(s) == 0 {
		return 0
	}
	return s[min(len(s)-1, int(q*float64(len(s))))]
}
