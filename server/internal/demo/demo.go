// Package demo is the DEMO_MODE-only service behind the dashboard's live
// buttons: add/cancel orders in a tenant's source database and compare the
// aggregation layer against a direct SQL query on that database.
package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/auth"
	"tenantpulse/internal/chq"
	"tenantpulse/internal/directsql"
	"tenantpulse/internal/domain"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/registry"
)

type Service struct {
	Reg           *registry.Registry
	Signer        *auth.Signer
	SourceDSN     string // tp_demo credentials; database swapped per tenant
	APIBase       string // internal analytics API
	OperatorToken string
	HC            *http.Client
	Stress        *StressJob

	poolsMu sync.Mutex
	pools   map[string]*pgxpool.Pool

	compareMu   sync.Mutex // one direct-SQL comparison at a time, globally
	lastCompare time.Time
}

func (s *Service) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok", "demo_mode": true}) })
	if s.Stress != nil {
		r.POST("/stress", s.authAny, s.startStress)
		r.GET("/stress", s.authAny, s.stressStatus)
	}
	t := r.Group("/tenants/:tenant", s.authorize)
	t.POST("/orders/random", s.randomOrder)
	t.POST("/orders/cancel", s.cancelOrder)
	t.POST("/compare", s.compare)
	return r
}

func (s *Service) authorize(c *gin.Context) {
	id, err := s.Signer.Verify(c.GetHeader("Authorization"))
	if err != nil {
		c.AbortWithStatusJSON(401, gin.H{"error": "unauthenticated"})
		return
	}
	t := s.Reg.Current().BySlug[c.Param("tenant")]
	if t == nil {
		s.Reg.RefreshIfStale(c)
		t = s.Reg.Current().BySlug[c.Param("tenant")]
	}
	if t == nil || !id.CanRead(t.Slug) {
		c.AbortWithStatusJSON(403, gin.H{"error": "forbidden"})
		return
	}
	if t.Status != "active" {
		c.AbortWithStatusJSON(409, gin.H{"error": "tenant_not_ready", "tenant_status": t.Status})
		return
	}
	c.Set("tenant", t)
}

func (s *Service) pool(ctx context.Context, t *registry.Tenant) (*pgxpool.Pool, error) {
	s.poolsMu.Lock()
	defer s.poolsMu.Unlock()
	if p, ok := s.pools[t.ID]; ok {
		return p, nil
	}
	dsn, err := pg.WithDatabase(s.SourceDSN, t.SourceDatabase)
	if err != nil {
		return nil, err
	}
	p, err := pg.Connect(ctx, dsn, pg.PoolOptions{MaxConns: 2, AppName: "tp-demo"})
	if err != nil {
		return nil, err
	}
	if s.pools == nil {
		s.pools = map[string]*pgxpool.Pool{}
	}
	s.pools[t.ID] = p
	return p, nil
}

// ------------------------------------------------------------ live buttons

func (s *Service) randomOrder(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	pool, err := s.pool(c, t)
	if err != nil {
		c.JSON(503, gin.H{"error": "source unavailable"})
		return
	}
	rows, err := pool.Query(c, `SELECT id::text, name FROM products WHERE is_active ORDER BY random() LIMIT 64`)
	if err != nil {
		c.JSON(500, gin.H{"error": "load products"})
		return
	}
	type prod struct{ ID, Name string }
	prods, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (prod, error) {
		var p prod
		return p, r.Scan(&p.ID, &p.Name)
	})
	if err != nil || len(prods) == 0 {
		c.JSON(500, gin.H{"error": "no products"})
		return
	}
	lines := 1 + rand.IntN(3)
	sum := decimal.Zero
	type line struct {
		Product string `json:"product"`
		Qty     int64  `json:"quantity"`
		Price   string `json:"unit_price"`
		id      string
	}
	var ls []line
	for i := 0; i < lines; i++ {
		p := prods[rand.IntN(len(prods))]
		q := int64(1 + rand.IntN(2))
		price := domain.UnitPrice(p.ID)
		sum = sum.Add(price.Mul(decimal.NewFromInt(q)))
		ls = append(ls, line{Product: p.Name, Qty: q, Price: price.String(), id: p.ID})
	}
	total := sum.Round(2)
	tx, err := pool.Begin(c)
	if err != nil {
		c.JSON(503, gin.H{"error": "source unavailable"})
		return
	}
	defer func() { _ = tx.Rollback(c) }()
	var orderID string
	var committedAt time.Time
	if err := tx.QueryRow(c, `INSERT INTO orders (status, currency, total_amount) VALUES ('placed', $1, $2)
	                          RETURNING id::text, created_at`, t.DefaultCurrency, total.String()).Scan(&orderID, &committedAt); err != nil {
		c.JSON(500, gin.H{"error": "insert order"})
		return
	}
	for i, l := range ls {
		if _, err := tx.Exec(c, `INSERT INTO order_items (order_id, product_id, line_number, quantity, unit_price) VALUES ($1, $2, $3, $4, $5)`,
			orderID, l.id, i+1, l.Qty, l.Price); err != nil {
			c.JSON(500, gin.H{"error": "insert item"})
			return
		}
	}
	if err := tx.Commit(c); err != nil {
		c.JSON(500, gin.H{"error": "commit"})
		return
	}
	c.JSON(201, gin.H{"order_id": orderID, "status": "placed", "currency": t.DefaultCurrency, "total_amount": total.String(),
		"lines": ls, "created_at": committedAt, "committed_at": time.Now().UTC()})
}

func (s *Service) cancelOrder(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	pool, err := s.pool(c, t)
	if err != nil {
		c.JSON(503, gin.H{"error": "source unavailable"})
		return
	}
	var id, cur, total, prev string
	var created time.Time
	err = pool.QueryRow(c, `
		WITH target AS (
			SELECT id, status FROM orders
			WHERE deleted_at IS NULL AND status IN ('placed', 'paid', 'processing')
			  AND created_at >= now() - interval '7 days'
			ORDER BY created_at DESC LIMIT 1 FOR UPDATE SKIP LOCKED
		)
		UPDATE orders o SET status = 'cancelled', version = o.version + 1, updated_at = now()
		FROM target WHERE o.id = target.id
		RETURNING o.id::text, o.currency, o.total_amount::text, o.created_at, target.status`).Scan(&id, &cur, &total, &created, &prev)
	if errors.Is(err, pgx.ErrNoRows) {
		c.JSON(404, gin.H{"error": "no cancellable order in the last 7 days"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": "cancel failed"})
		return
	}
	c.JSON(200, gin.H{"order_id": id, "previous_status": prev, "status": "cancelled", "currency": strings.TrimSpace(cur),
		"total_amount": total, "created_at": created, "committed_at": time.Now().UTC()})
}

// ------------------------------------------------------------ compare

type compareReq struct {
	Kind     string `json:"kind"` // summary | breakdown | trends | top
	Range    string `json:"range"`
	From     string `json:"from"`
	To       string `json:"to"`
	Currency string `json:"currency"`
	Grain    string `json:"grain"`
}

type timing struct {
	TookMs float64 `json:"took_ms"`
	RTTMs  float64 `json:"rtt_ms,omitempty"`
	Cache  string  `json:"cache,omitempty"`
}

// compare: watermark -> analytics (cache bypassed) -> direct SQL on the
// source -> exact comparison; retried when concurrent writes land between
// the two reads. Then a warm (cached) read for the third timing.
func (s *Service) compare(c *gin.Context) {
	t := c.MustGet("tenant").(*registry.Tenant)
	var req compareReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "bad_request"})
		return
	}
	if !s.compareMu.TryLock() {
		c.Header("Retry-After", "3")
		c.JSON(429, gin.H{"error": "busy", "detail": "another direct-SQL comparison is running; it is serialized to protect the source database"})
		return
	}
	defer s.compareMu.Unlock()
	if wait := 2*time.Second - time.Since(s.lastCompare); wait > 0 {
		c.Header("Retry-After", "2")
		c.JSON(429, gin.H{"error": "cooldown", "detail": "comparisons are rate-limited to protect the source database"})
		return
	}
	defer func() { s.lastCompare = time.Now() }()

	path, ok := map[string]string{"summary": "orders/summary", "breakdown": "orders/breakdown", "trends": "orders/trends", "top": "products/top"}[req.Kind]
	if !ok {
		c.JSON(400, gin.H{"error": "kind must be summary, breakdown, trends or top"})
		return
	}
	q := url.Values{}
	for k, v := range map[string]string{"range": req.Range, "from": req.From, "to": req.To, "currency": req.Currency, "grain": req.Grain} {
		if v != "" {
			q.Set(k, v)
		}
	}
	if req.Kind == "trends" || req.Kind == "top" {
		if q.Get("currency") == "" {
			q.Set("currency", t.DefaultCurrency)
		}
	}
	pool, err := s.pool(c, t)
	if err != nil {
		c.JSON(503, gin.H{"error": "source unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(c, 90*time.Second)
	defer cancel()

	var (
		attempts     int
		cold, direct timing
		waitMs       float64
		apiData      json.RawMessage
		diffs        []string
		directSQL    string
		directParams []any
		directResult any
		from, to     time.Time
	)
	for attempts = 1; attempts <= 3; attempts++ {
		w, err := s.watermark(ctx, t, pool)
		if err != nil {
			c.JSON(504, gin.H{"error": "watermark", "detail": err.Error()})
			return
		}
		waitMs = w
		var env apiEnvelope
		cold, env, err = s.callAPI(ctx, t, path, q, true)
		if err != nil {
			c.JSON(502, gin.H{"error": "analytics api", "detail": err.Error()})
			return
		}
		apiData = env.Data
		from, to = env.from(), env.to()

		start := time.Now()
		directSQL, directParams, directResult, diffs, err = s.runDirect(ctx, pool, t, req.Kind, env, q)
		direct = timing{TookMs: ms(time.Since(start))}
		if err != nil {
			c.JSON(500, gin.H{"error": "direct sql", "detail": err.Error()})
			return
		}
		if len(diffs) == 0 {
			break
		}
		slog.Info("compare mismatch; re-reading after a fresh watermark", "tenant", t.Slug, "attempt", attempts, "diffs", len(diffs))
	}
	if attempts > 3 {
		attempts = 3
	}

	// Warm path: prime then read through the cache.
	_, _, _ = s.callAPI(ctx, t, path, q, false)
	warm, _, _ := s.callAPI(ctx, t, path, q, false)

	speedup := 0.0
	if cold.TookMs > 0 {
		speedup = direct.TookMs / cold.TookMs
	}
	c.JSON(200, gin.H{
		"kind": req.Kind, "tenant": t.Slug, "from": from, "to": to,
		"match": len(diffs) == 0, "differences": diffs, "attempts": attempts, "watermark_wait_ms": waitMs,
		"direct":          gin.H{"took_ms": direct.TookMs, "sql": directSQL, "params": directParams, "result": directResult, "where": "tenant source database (" + t.SourceDatabase + ")"},
		"analytics_cold":  gin.H{"took_ms": cold.TookMs, "rtt_ms": cold.RTTMs, "cache": cold.Cache, "result": apiData, "where": "analytics rollups (cache bypassed)"},
		"analytics_warm":  gin.H{"took_ms": warm.TookMs, "rtt_ms": warm.RTTMs, "cache": warm.Cache, "where": "Redis response cache"},
		"speedup_vs_cold": speedup,
	})
}

// watermark writes a heartbeat into the source database and waits until the
// aggregator has applied it. The tenant topic is totally ordered, so every
// commit before the heartbeat is now in analytics.
func (s *Service) watermark(ctx context.Context, t *registry.Tenant, pool *pgxpool.Pool) (float64, error) {
	start := time.Now()
	var ts time.Time
	if err := pool.QueryRow(ctx, `UPDATE tp_heartbeat SET ts = clock_timestamp() WHERE id = 1 RETURNING ts`).Scan(&ts); err != nil {
		return 0, fmt.Errorf("write heartbeat: %w", err)
	}
	target := ts.Truncate(time.Millisecond)
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var f struct {
			HB *time.Time `json:"last_heartbeat_source_at"`
		}
		if err := s.getJSON(ctx, "/v1/tenants/"+t.Slug+"/freshness", nil, &f, nil); err == nil && f.HB != nil && !f.HB.Before(target) {
			return ms(time.Since(start)), nil
		}
		time.Sleep(40 * time.Millisecond)
	}
	return 0, errors.New("pipeline did not catch up within 20s")
}

type apiEnvelope struct {
	Data json.RawMessage `json:"data"`
	Meta struct {
		TookMs float64 `json:"took_ms"`
		Cache  string  `json:"cache"`
	} `json:"meta"`
	parsed struct {
		From     time.Time `json:"from"`
		To       time.Time `json:"to"`
		Currency *string   `json:"currency"`
		Grain    string    `json:"grain"`
		Limit    int       `json:"limit"`
		Timezone string    `json:"timezone"`
	}
}

func (e *apiEnvelope) from() time.Time { return e.parsed.From }
func (e *apiEnvelope) to() time.Time   { return e.parsed.To }

func (s *Service) callAPI(ctx context.Context, t *registry.Tenant, path string, q url.Values, bypass bool) (timing, apiEnvelope, error) {
	var env apiEnvelope
	hdr := map[string]string{}
	if bypass {
		hdr["X-TP-Cache"] = "bypass"
	}
	start := time.Now()
	if err := s.getJSON(ctx, "/v1/tenants/"+t.Slug+"/"+path, q, &env, hdr); err != nil {
		return timing{}, env, err
	}
	rtt := ms(time.Since(start))
	_ = json.Unmarshal(env.Data, &env.parsed)
	return timing{TookMs: env.Meta.TookMs, RTTMs: rtt, Cache: env.Meta.Cache}, env, nil
}

func (s *Service) getJSON(ctx context.Context, path string, q url.Values, out any, hdr map[string]string) error {
	u := s.APIBase + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.OperatorToken)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := s.HC.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != 200 {
		return fmt.Errorf("api %s: %d %s", path, resp.StatusCode, string(body))
	}
	return json.Unmarshal(body, out)
}

// runDirect runs the plain SQL a reviewer would write, in a read-only
// repeatable-read transaction with a statement timeout, and diffs it against
// the analytics answer.
func (s *Service) runDirect(ctx context.Context, pool *pgxpool.Pool, t *registry.Tenant, kind string, env apiEnvelope, q url.Values) (string, []any, any, []string, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return "", nil, nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '60s'`); err != nil {
		return "", nil, nil, nil, err
	}
	from, to := env.parsed.From, env.parsed.To
	cur := q.Get("currency")
	switch kind {
	case "summary", "breakdown":
		rows, err := directsql.StatusRows(ctx, tx, from, to, cur)
		if err != nil {
			return "", nil, nil, nil, err
		}
		params := []any{from, to, domain.RecognizedStatuses, cur}
		if kind == "summary" {
			var got struct {
				Totals []chq.CurrencyTotals `json:"totals"`
			}
			_ = json.Unmarshal(env.Data, &got)
			want := chq.Totals(rows)
			return directsql.StatusRowsSQL, params, gin.H{"totals": want}, chq.DiffTotals(got.Totals, want), nil
		}
		var got struct {
			Statuses []struct {
				Status string `json:"status"`
				Orders int64  `json:"orders"`
			} `json:"statuses"`
		}
		_ = json.Unmarshal(env.Data, &got)
		want := map[string]int64{}
		for _, r := range rows {
			want[r.Status] += r.Orders
		}
		var diffs []string
		for _, g := range got.Statuses {
			if g.Orders != want[g.Status] {
				diffs = append(diffs, fmt.Sprintf("%s: analytics %d vs direct %d", g.Status, g.Orders, want[g.Status]))
			}
		}
		return directsql.StatusRowsSQL, params, gin.H{"statuses": want}, diffs, nil
	case "trends":
		grain := env.parsed.Grain
		pts, err := directsql.Trend(ctx, tx, from, to, t.Timezone, grain, cur)
		if err != nil {
			return "", nil, nil, nil, err
		}
		var got struct {
			Points []chq.TrendPoint `json:"points"`
		}
		_ = json.Unmarshal(env.Data, &got)
		sql := directsql.TrendDaySQL
		if grain == "hour" {
			sql = directsql.TrendHourSQL
		}
		return sql, []any{from, to, t.Timezone, cur, domain.RecognizedStatuses}, gin.H{"points": pts}, chq.DiffTrend(got.Points, pts), nil
	default:
		limit := env.parsed.Limit
		if limit == 0 {
			limit = 10
		}
		top, err := directsql.TopProducts(ctx, tx, from, to, cur, limit)
		if err != nil {
			return "", nil, nil, nil, err
		}
		var got struct {
			Products []chq.TopProduct `json:"products"`
		}
		_ = json.Unmarshal(env.Data, &got)
		return directsql.TopProductsSQL, []any{from, to, cur, domain.RecognizedStatuses, limit}, gin.H{"products": top}, chq.DiffTop(got.Products, top), nil
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
