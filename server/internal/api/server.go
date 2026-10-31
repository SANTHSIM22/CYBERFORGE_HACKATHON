// Package api is the Gin REST API. Dashboard handlers read ClickHouse only
// (as tp_api, under row policies); this package has no source-database access.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"

	"crypto/sha256"
	"encoding/hex"

	"tenantpulse/internal/auth"
	"tenantpulse/internal/chq"
	"tenantpulse/internal/domain"
	"tenantpulse/internal/registry"
)

var (
	promReqDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "tenantpulse_api_request_duration_seconds", Help: "Dashboard API latency.",
		Buckets: []float64{.0005, .001, .002, .005, .01, .025, .05, .1, .2, .5, 1, 2, 5}}, []string{"route", "cache"})
	promQueryDur = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "tenantpulse_analytics_query_duration_seconds", Help: "ClickHouse time on cache miss.",
		Buckets: []float64{.0005, .001, .002, .005, .01, .025, .05, .1, .2, .5, 1, 2}}, []string{"query_name"})
)

type Server struct {
	B            Backend     // ClickHouse in production; "direct" only for benchmarks
	Direct       bool        // benchmark mode: every request queries the store's primary, no cache
	Mon          driver.Conn // tp_monitor: pipeline health only, no business tables (nil in direct mode)
	Reg          *registry.Registry
	Cache        *RespCache
	Signer       *auth.Signer
	DevAuth      bool
	Limiter      *Limiter
	Win          *Window
	QueryTimeout time.Duration

	sf singleflight.Group
}

func (s *Server) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), func(c *gin.Context) { c.Set("start", time.Now()); c.Next() })

	r.GET("/health/live", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/health/ready", s.ready)
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	v1 := r.Group("/v1")
	v1.GET("/metrics", gin.WrapH(promhttp.Handler()))
	if s.DevAuth {
		v1.GET("/dev/subjects", s.devSubjects)
		v1.POST("/dev/login", s.devLogin)
	}
	authed := v1.Group("", s.authenticate)
	authed.GET("/tenants", s.listTenants)
	authed.GET("/admin/stats", s.adminStats)
	authed.GET("/benchmarks", s.benchmarks)

	t := authed.Group("/tenants/:tenant", s.authorizeTenant)
	t.GET("", s.tenantInfo)
	t.GET("/freshness", s.freshness)
	t.GET("/orders/summary", s.ready4(s.summary))
	t.GET("/orders/count", s.ready4(s.summary)) // PRD example alias
	t.GET("/orders/trends", s.ready4(s.trends))
	t.GET("/orders/breakdown", s.ready4(s.breakdown))
	t.GET("/products/top", s.ready4(s.topProducts))
	return r
}

func (s *Server) ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	if err := s.B.Ping(ctx); err != nil {
		c.JSON(503, gin.H{"status": s.B.Name() + " unavailable"})
		return
	}
	c.JSON(200, gin.H{"status": "ready", "backend": s.B.Name(), "tenants": len(s.Reg.Current().Ordered)})
}

// ------------------------------------------------------------ auth

func (s *Server) authenticate(c *gin.Context) {
	h := c.GetHeader("Authorization")
	if h == "" {
		abort(c, 401, "unauthenticated", "send Authorization: Bearer <token> (POST /v1/dev/login in development)")
		return
	}
	id, err := s.Signer.Verify(h)
	if err != nil {
		abort(c, 401, "unauthenticated", "invalid token")
		return
	}
	c.Set("identity", id)
}

// authorizeTenant resolves the URL tenant and checks membership. The URL is
// only a selector: access comes from the authenticated identity.
func (s *Server) authorizeTenant(c *gin.Context) {
	key := c.Param("tenant")
	t := s.lookup(key)
	if t == nil {
		s.Reg.RefreshIfStale(c)
		t = s.lookup(key)
	}
	id := c.MustGet("identity").(auth.Identity)
	if t == nil {
		if id.Operator {
			abort(c, 404, "not_found", "unknown tenant")
		} else { // same answer as "not yours": tenant existence is not leaked
			abort(c, 403, "forbidden", "identity "+id.Subject+" is not a member of this tenant")
		}
		return
	}
	if !id.CanRead(t.Slug) {
		abort(c, 403, "forbidden", "identity "+id.Subject+" is not a member of this tenant")
		return
	}
	c.Set("tenant", t)
}

func (s *Server) lookup(key string) *registry.Tenant {
	snap := s.Reg.Current()
	if t := snap.BySlug[key]; t != nil {
		return t
	}
	return snap.ByID[key]
}

// ready4 rejects metric reads until the tenant's initial snapshot is loaded
// and streaming, so the dashboard shows "provisioning" rather than partial numbers.
func (s *Server) ready4(h gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		t := c.MustGet("tenant").(*registry.Tenant)
		if t.Status != "active" {
			c.JSON(409, gin.H{"error": "tenant_not_ready", "tenant_status": t.Status,
				"detail": "initial snapshot in progress; metrics appear when the tenant becomes active"})
			return
		}
		h(c)
	}
}

func (s *Server) devSubjects(c *gin.Context) {
	out := []gin.H{}
	for _, t := range s.Reg.Current().Ordered {
		out = append(out, gin.H{"subject": "tenant:" + t.Slug, "slug": t.Slug, "name": t.Name, "tier": t.Tier,
			"status": t.Status, "default_currency": t.DefaultCurrency, "timezone": t.Timezone})
	}
	c.JSON(200, gin.H{"operator": "operator", "tenants": out})
}

func (s *Server) devLogin(c *gin.Context) {
	var body struct {
		Subject string `json:"subject"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		abort(c, 400, "bad_request", `body must be {"subject": "tenant:<slug>" | "operator"}`)
		return
	}
	if _, err := auth.ParseSubject(body.Subject); err != nil {
		abort(c, 400, "bad_request", "unknown subject format")
		return
	}
	c.JSON(200, gin.H{"token": s.Signer.Issue(body.Subject), "subject": body.Subject, "dev_only": true})
}

func (s *Server) listTenants(c *gin.Context) {
	id := c.MustGet("identity").(auth.Identity)
	out := []gin.H{}
	for _, t := range s.Reg.Current().Ordered {
		if id.CanRead(t.Slug) {
			out = append(out, gin.H{"slug": t.Slug, "name": t.Name, "tier": t.Tier, "status": t.Status,
				"timezone": t.Timezone, "default_currency": t.DefaultCurrency})
		}
	}
	c.JSON(200, gin.H{"tenants": out})
}

// ------------------------------------------------------------ cached read path

// serve answers from the version-keyed cache when possible; otherwise it runs
// the ClickHouse query once per key (singleflight), bounded by the limiter.
func (s *Server) serve(c *gin.Context, route string, norm normalized, compute func(ctx context.Context) (any, error)) {
	start := c.MustGet("start").(time.Time)
	t := c.MustGet("tenant").(*registry.Tenant)
	id := c.MustGet("identity").(auth.Identity)
	if s.Direct { // benchmark baseline: no cache, no coalescing, straight to the primary
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c), s.QueryTimeout+time.Second)
		defer cancel()
		release, ok := s.Limiter.Acquire(ctx, t.ID)
		if !ok {
			c.Header("Retry-After", "1")
			abort(c, 429, "too_many_requests", "tenant query capacity exhausted; retry shortly")
			return
		}
		v, err := compute(ctx)
		release()
		if err != nil {
			slog.Error("direct query failed", "tenant", t.Slug, "route", route, "err", err)
			abort(c, 500, "query_failed", "direct query failed")
			return
		}
		body, _ := json.Marshal(v)
		s.respond(c, t, route, body, "DIRECT", 0, start)
		return
	}
	b, _ := json.Marshal(norm)
	sum := sha256.Sum256(b)
	version := s.Cache.Version(t.ID, t.ActiveGeneration)
	key := t.ID + ":" + strconv.FormatInt(t.ActiveGeneration, 10) + ":" + hex.EncodeToString(sum[:12])
	flight := key + ":" + strconv.FormatUint(version, 10)
	// min_version: the caller has already seen this data version (the
	// dashboard's auto-refresh sends it), so an older answer is not acceptable.
	minVersion, _ := strconv.ParseUint(c.Query("min_version"), 10, 64)

	bypass := id.Operator && c.GetHeader("X-TP-Cache") == "bypass"
	run := func(ctx context.Context) ([]byte, error) { // caller holds a limiter slot
		qStart := time.Now()
		v, err := compute(ctx)
		promQueryDur.WithLabelValues(route).Observe(time.Since(qStart).Seconds())
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(v)
		if err == nil && !bypass {
			s.Cache.Put(key, version, body)
		}
		return body, err
	}
	fill := func(ctx context.Context) ([]byte, error) {
		release, ok := s.Limiter.Acquire(ctx, t.ID)
		if !ok {
			return nil, errBusy
		}
		defer release()
		return run(ctx)
	}
	if !bypass {
		if body, at, fresh, ok := s.Cache.Get(key, version, minVersion); ok {
			if fresh {
				s.respond(c, t, route, body, "HIT", at, start)
				return
			}
			s.respond(c, t, route, body, "STALE", at, start)
			s.revalidate(t.ID, flight, run)
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c), s.QueryTimeout+time.Second)
	defer cancel()
	var body []byte
	var err error
	status := "MISS"
	if bypass {
		status = "BYPASS"
		body, err = fill(WithoutView(ctx)) // an explicit cold ClickHouse measurement: no view, no coalescing, no caching
	} else {
		var v any
		v, err, _ = s.sf.Do(flight, func() (any, error) { return fill(ctx) })
		if err == nil {
			body = v.([]byte)
		}
	}
	switch {
	case errors.Is(err, errBusy):
		c.Header("Retry-After", "1")
		abort(c, 429, "too_many_requests", "tenant query capacity exhausted; retry shortly")
	case err != nil:
		slog.Error("query failed", "tenant", t.Slug, "route", route, "err", err)
		abort(c, 500, "query_failed", "analytics query failed")
	default:
		s.respond(c, t, route, body, status, version, start)
	}
}

// revalidate recomputes a stale entry off the request path (the reader already
// has its answer). Background refreshes queue for at most half of the
// limiter's slots, so they never crowd out a reader who is waiting.
func (s *Server) revalidate(tenantID, flight string, run func(context.Context) ([]byte, error)) {
	done, running := s.Cache.Refreshing(flight)
	if running {
		return
	}
	go func() {
		defer done()
		ctx, cancel := context.WithTimeout(context.Background(), s.QueryTimeout+time.Second)
		defer cancel()
		release, ok := s.Limiter.AcquireBackground(ctx, tenantID)
		if !ok {
			return // the stale entry stays; the next read retries
		}
		defer release()
		_, _, _ = s.sf.Do(flight, func() (any, error) { return run(ctx) })
	}()
}

func (s *Server) respond(c *gin.Context, t *registry.Tenant, route string, data []byte, cache string, version uint64, start time.Time) {
	took := float64(time.Since(start).Microseconds()) / 1000
	c.Header("X-Cache", cache)
	c.Header("Server-Timing", fmt.Sprintf(`app;dur=%.2f, cache;desc="%s"`, took, cache))
	c.Header("Cache-Control", "no-store")
	body := make([]byte, 0, len(data)+256)
	body = append(body, `{"tenant":`...)
	body = strconv.AppendQuote(body, t.Slug)
	body = append(body, `,"data":`...)
	body = append(body, data...)
	body = fmt.Appendf(body, `,"meta":{"took_ms":%.3f,"cache":"%s","generation":%d,"data_version":"%d","server_time":"%s"}}`,
		took, cache, t.ActiveGeneration, version, time.Now().UTC().Format(time.RFC3339Nano))
	c.Data(200, "application/json", body)
	s.Win.Observe(time.Now(), took, cache)
	promReqDur.WithLabelValues(route, cache).Observe(took / 1000)
}

func abort(c *gin.Context, code int, kind, detail string) {
	c.AbortWithStatusJSON(code, gin.H{"error": kind, "detail": detail})
}

func (s *Server) params(c *gin.Context, t *registry.Tenant) (chq.Params, bool) {
	from, to, err := parseRange(c, t, time.Now())
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return chq.Params{}, false
	}
	return chq.Params{TenantID: t.ID, Generation: t.ActiveGeneration, TZ: t.Timezone, From: from, To: to, Timeout: s.QueryTimeout}, true
}

// asOf comes from the in-memory status poll: no extra ClickHouse round trip.
func (s *Server) asOf(_ context.Context, t *registry.Tenant) *time.Time {
	if s.Direct {
		now := time.Now().UTC()
		return &now
	}
	return s.Cache.AsOf(t.ID, t.ActiveGeneration)
}

// ------------------------------------------------------------ handlers

func (s *Server) summary(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	p, ok := s.params(c, t)
	if !ok {
		return
	}
	cur, err := parseCurrency(c, "")
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	p.Currency = cur
	s.serve(c, "summary", normalized{Route: "summary", From: p.From.UnixMilli(), To: p.To.UnixMilli(), Currency: cur, TZ: t.Timezone},
		func(ctx context.Context) (any, error) {
			totals, prev, err := s.B.Summary(ctx, t, p)
			if err != nil {
				return nil, err
			}
			var total, prevTotal int64
			for _, r := range totals {
				total += r.Orders
			}
			for _, r := range prev {
				prevTotal += r.Orders
			}
			return gin.H{"from": p.From, "to": p.To, "timezone": t.Timezone, "currency": nilIfEmpty(cur),
				"totals": totals, "total_orders": total,
				"previous": gin.H{"from": p.From.Add(-p.To.Sub(p.From)), "to": p.From, "totals": prev, "total_orders": prevTotal},
				"as_of":    s.asOf(ctx, t)}, nil
		})
}

type breakdownRow struct {
	Status     string  `json:"status"`
	Orders     int64   `json:"orders"`
	GrossSales *string `json:"gross_sales,omitempty"`
	Recognized bool    `json:"recognized"`
}

func (s *Server) breakdown(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	p, ok := s.params(c, t)
	if !ok {
		return
	}
	cur, err := parseCurrency(c, "")
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	p.Currency = cur
	s.serve(c, "breakdown", normalized{Route: "breakdown", From: p.From.UnixMilli(), To: p.To.UnixMilli(), Currency: cur, TZ: t.Timezone},
		func(ctx context.Context) (any, error) {
			rows, err := s.B.StatusRows(ctx, t, p)
			if err != nil {
				return nil, err
			}
			counts, gross := map[string]int64{}, map[string]string{}
			for _, r := range rows {
				counts[r.Status] += r.Orders
				if cur != "" {
					gross[r.Status] = r.Gross.String()
				}
			}
			out := make([]breakdownRow, 0, len(domain.AllStatuses))
			var total int64
			for _, st := range domain.AllStatuses {
				row := breakdownRow{Status: st, Orders: counts[st], Recognized: domain.IsRecognized(st)}
				if cur != "" {
					g := gross[st]
					if g == "" {
						g = "0"
					}
					row.GrossSales = &g
				}
				total += counts[st]
				out = append(out, row)
			}
			return gin.H{"from": p.From, "to": p.To, "timezone": t.Timezone, "currency": nilIfEmpty(cur),
				"statuses": out, "total_orders": total, "as_of": s.asOf(ctx, t)}, nil
		})
}

func (s *Server) trends(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	p, ok := s.params(c, t)
	if !ok {
		return
	}
	cur, err := parseCurrency(c, t.DefaultCurrency)
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	grain, err := parseGrain(c, p.From, p.To)
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	p.Currency, p.Grain = cur, grain
	s.serve(c, "trends", normalized{Route: "trends", From: p.From.UnixMilli(), To: p.To.UnixMilli(), Currency: cur, Grain: grain, TZ: t.Timezone},
		func(ctx context.Context) (any, error) {
			pts, err := s.B.Trend(ctx, t, p)
			if err != nil {
				return nil, err
			}
			return gin.H{"from": p.From, "to": p.To, "timezone": t.Timezone, "currency": cur, "grain": grain,
				"points": fillBuckets(pts, p, t.Loc, grain), "as_of": s.asOf(ctx, t)}, nil
		})
}

// fillBuckets returns one point per bucket (zeros included) so charts never
// draw misleading lines across gaps.
func fillBuckets(pts []chq.TrendPoint, p chq.Params, loc *time.Location, grain string) []chq.TrendPoint {
	have := make(map[string]chq.TrendPoint, len(pts))
	for _, pt := range pts {
		have[pt.Bucket] = pt
	}
	out := []chq.TrendPoint{}
	add := func(b string) {
		if pt, ok := have[b]; ok {
			out = append(out, pt)
		} else {
			out = append(out, chq.TrendPoint{Bucket: b, GrossSales: "0", RecognizedSales: "0"})
		}
	}
	if grain == "hour" {
		for h := p.From.UTC().Truncate(time.Hour); h.Before(p.To); h = h.Add(time.Hour) {
			add(h.Format("2006-01-02T15:04:05Z"))
		}
		return out
	}
	y, m, d := p.From.In(loc).Date()
	for i := 0; i < maxRangeDays+2; i++ {
		day := time.Date(y, m, d+i, 12, 0, 0, 0, loc)
		dy, dm, dd := day.Date()
		if !domain.StartOfLocalDay(dy, dm, dd, loc).Before(p.To) {
			break
		}
		add(day.Format(time.DateOnly))
	}
	return out
}

func (s *Server) topProducts(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	p, ok := s.params(c, t)
	if !ok {
		return
	}
	cur, err := parseCurrency(c, t.DefaultCurrency)
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	limit, err := parseLimit(c)
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	p.Currency, p.Limit = cur, limit
	s.serve(c, "top", normalized{Route: "top", From: p.From.UnixMilli(), To: p.To.UnixMilli(), Currency: cur, Limit: limit, TZ: t.Timezone},
		func(ctx context.Context) (any, error) {
			rows, err := s.B.Top(ctx, t, p)
			if err != nil {
				return nil, err
			}
			if rows == nil {
				rows = []chq.TopProduct{}
			}
			return gin.H{"from": p.From, "to": p.To, "timezone": t.Timezone, "currency": cur, "limit": limit,
				"metric": "recognized line revenue (quantity x unit_price)", "products": rows, "as_of": s.asOf(ctx, t)}, nil
		})
}

func (s *Server) tenantInfo(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	ctx, cancel := context.WithTimeout(c, 3*time.Second)
	defer cancel()
	curs, err := s.B.Currencies(ctx, t)
	if err != nil || len(curs) == 0 {
		curs = []string{t.DefaultCurrency}
	}
	c.JSON(200, gin.H{"tenant_id": t.ID, "slug": t.Slug, "name": t.Name, "tier": t.Tier, "status": t.Status,
		"timezone": t.Timezone, "default_currency": t.DefaultCurrency, "currencies": curs, "generation": t.ActiveGeneration})
}

// freshness always bypasses the response cache (B-1).
func (s *Server) freshness(c *gin.Context) {
	start := c.MustGet("start").(time.Time)
	t := c.MustGet("tenant").(*registry.Tenant)
	if s.Direct { // reading the primary is always current
		now := time.Now().UTC()
		c.JSON(200, gin.H{"tenant": t.Slug, "status": "live", "snapshot_state": "completed", "data_as_of": now, "data_age_ms": 0,
			"data_version": strconv.FormatInt(now.Unix(), 10), "server_time": now, "backend": s.B.Name()})
		return
	}
	// From the 250 ms status poll: every open dashboard polls this each second,
	// so it must not cost a ClickHouse query per call.
	st, ok := s.Cache.Status(t.ID, t.ActiveGeneration)
	if !ok { // not polled yet (API just started, or a brand-new generation)
		var err error
		if st, err = chq.TenantStatus(c, s.B.(interface{ Conn() driver.Conn }).Conn(), t.ID, t.ActiveGeneration); err != nil {
			abort(c, 500, "query_failed", "freshness unavailable")
			return
		}
	}
	now := time.Now()
	age := func(p *time.Time) *int64 {
		if p == nil {
			return nil
		}
		v := now.Sub(*p).Milliseconds()
		return &v
	}
	asOf := st.DataAsOf()
	checkAge := age(st.StreamCheckedAt)
	if a := age(st.LastHeartbeatSourceAt); a != nil && (checkAge == nil || *a < *checkAge) {
		checkAge = a
	}
	status := "live"
	switch {
	case t.Status == "provisioning":
		status = "provisioning"
	case t.Status == "snapshotting" || !st.Found || st.SnapshotState != "completed":
		status = "snapshotting"
	case checkAge == nil || *checkAge > 30_000:
		status = "stalled"
	case age(asOf) != nil && *age(asOf) > 30_000:
		status = "lagging"
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, gin.H{
		"tenant": t.Slug, "generation": t.ActiveGeneration, "status": status, "tenant_status": t.Status,
		"snapshot_state": st.SnapshotState, "data_as_of": asOf, "data_age_ms": age(asOf),
		"last_source_commit_at": st.LastSourceCommitAt, "last_applied_at": st.LastAppliedAt, "processing_lag_ms": st.LastApplyLagMs,
		"last_heartbeat_source_at": st.LastHeartbeatSourceAt, "stream_checked_at": st.StreamCheckedAt,
		"slot_lag_bytes": st.SlotLagBytes, "events_applied_total": st.EventsTotal,
		"data_version": strconv.FormatUint(s.Cache.Version(t.ID, t.ActiveGeneration), 10),
		"server_time":  now.UTC(), "took_ms": float64(time.Since(start).Microseconds()) / 1000,
	})
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
