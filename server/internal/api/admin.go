package api

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/gin-gonic/gin"

	"tenantpulse/internal/auth"
)

// benchmarks returns the newest stress-test and ladder results recorded by
// tpctl, for the comparison panels on the dashboard and /monitor.
func (s *Server) benchmarks(c *gin.Context) {
	out := gin.H{"stress": nil, "ladder": nil}
	if s.Mon == nil {
		c.JSON(200, out)
		return
	}
	ctx, cancel := context.WithTimeout(c, 3*time.Second)
	defer cancel()
	for _, kind := range []string{"stress", "ladder", "writestorm"} {
		var payload string
		var at time.Time
		if err := s.Mon.QueryRow(ctx, `SELECT payload, at FROM tp.benchmark_runs WHERE kind = ? ORDER BY at DESC LIMIT 1`, kind).Scan(&payload, &at); err == nil {
			out[kind] = gin.H{"at": at, "result": json.RawMessage(payload)}
		}
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, out)
}

type tenantLag struct {
	Slug          string `json:"slug"`
	Tier          string `json:"tier"`
	SnapshotState string `json:"snapshot_state"`
	ProcessingMs  *int64 `json:"processing_lag_ms"`
	DataAgeMs     *int64 `json:"data_age_ms"`
	SlotLagBytes  uint64 `json:"slot_lag_bytes"`
	Events        uint64 `json:"events_applied"`
}

// adminStats feeds /monitor (B-5): API latency percentiles from the in-process
// window, ingester throughput, per-tenant lag and ClickHouse storage. Operator
// only; cross-tenant reads use tp_monitor, which cannot see business tables.
func (s *Server) adminStats(c *gin.Context) {
	if id := c.MustGet("identity").(auth.Identity); !id.Operator {
		abort(c, 403, "forbidden", "operator identity required")
		return
	}
	ctx, cancel := context.WithTimeout(c, 3*time.Second)
	defer cancel()
	now := time.Now()
	out := gin.H{"server_time": now.UTC(), "api": s.Win.Stats(now, 60), "backend": s.B.Name()}
	if s.Mon == nil {
		out["ingesters"], out["pipeline"], out["store"] = []gin.H{}, gin.H{"tenants": 0, "worst": []gin.H{}}, gin.H{"available": false}
		c.JSON(200, out)
		return
	}

	ingesters := []gin.H{}
	if rows, err := s.Mon.Query(ctx, `SELECT worker, events_total, events_per_sec, flush_p50_ms, flush_p95_ms, flush_p99_ms,
	        streams, snapshots_running, max_slot_lag_bytes, updated_at FROM tp.ingester_stats FINAL
	        WHERE updated_at > now64(3) - INTERVAL 30 SECOND`); err == nil {
		for rows.Next() {
			var w string
			var ev, lag uint64
			var eps, p50, p95, p99 float64
			var streams, snaps uint32
			var at time.Time
			if rows.Scan(&w, &ev, &eps, &p50, &p95, &p99, &streams, &snaps, &lag, &at) == nil {
				ingesters = append(ingesters, gin.H{"worker": w, "events_total": ev, "events_per_sec": eps, "flush_p50_ms": p50,
					"flush_p95_ms": p95, "flush_p99_ms": p99, "streams": streams, "snapshots_running": snaps,
					"max_slot_lag_bytes": lag, "updated_at": at})
			}
		}
		rows.Close()
	}
	out["ingesters"] = ingesters

	snap := s.Reg.Current()
	var lags []tenantLag
	states := map[string]int{}
	if rows, err := s.Mon.Query(ctx, `SELECT toString(tenant_id), generation, snapshot_state, last_source_commit_at,
	        last_heartbeat_source_at, stream_checked_at, last_apply_lag_ms, slot_lag_bytes, events_total FROM tp.tenant_status FINAL`); err == nil {
		for rows.Next() {
			var tid, st string
			var gen uint32
			var src, hb, checked *time.Time
			var lag *int64
			var slot, events uint64
			if rows.Scan(&tid, &gen, &st, &src, &hb, &checked, &lag, &slot, &events) != nil {
				continue
			}
			t := snap.ByID[tid]
			if t == nil || t.ActiveGeneration != int64(gen) {
				continue
			}
			states[st]++
			tl := tenantLag{Slug: t.Slug, Tier: t.Tier, SnapshotState: st, ProcessingMs: lag, SlotLagBytes: slot, Events: events}
			var best *time.Time
			for _, x := range []*time.Time{src, hb, checked} {
				if x != nil && (best == nil || x.After(*best)) {
					best = x
				}
			}
			if best != nil {
				v := now.Sub(*best).Milliseconds()
				tl.DataAgeMs = &v
			}
			lags = append(lags, tl)
		}
		rows.Close()
	}
	val := func(p *int64) int64 {
		if p == nil {
			return 1 << 62
		}
		return *p
	}
	sort.Slice(lags, func(i, j int) bool { return val(lags[i].DataAgeMs) > val(lags[j].DataAgeMs) })
	var ages []float64
	var slotTotal uint64
	for _, l := range lags {
		if l.DataAgeMs != nil {
			ages = append(ages, float64(*l.DataAgeMs))
		}
		slotTotal += l.SlotLagBytes
	}
	sort.Float64s(ages)
	worst := lags
	if len(worst) > 8 {
		worst = worst[:8]
	}
	out["pipeline"] = gin.H{"tenants": len(lags), "snapshot_states": states, "slot_lag_bytes_total": slotTotal,
		"data_age_p50_ms": pct(ages, 0.5), "data_age_p95_ms": pct(ages, 0.95), "data_age_max_ms": pct(ages, 1), "worst": worst}

	store := gin.H{"available": false}
	if rows, err := s.Mon.Query(ctx, `SELECT table, sum(rows), sum(bytes_on_disk), sum(data_uncompressed_bytes), count()
	        FROM system.parts WHERE database = 'tp' AND active GROUP BY table ORDER BY table`); err == nil {
		tables := []gin.H{}
		for rows.Next() {
			var tbl string
			var n, disk, raw, parts uint64
			if rows.Scan(&tbl, &n, &disk, &raw, &parts) == nil {
				tables = append(tables, gin.H{"table": tbl, "rows": n, "bytes_on_disk": disk, "uncompressed_bytes": raw, "parts": parts})
			}
		}
		rows.Close()
		store = gin.H{"available": true, "engine": "ClickHouse", "tables": tables}
	}
	out["store"] = store
	c.JSON(200, out)
}
