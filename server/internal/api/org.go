package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	"tenantpulse/internal/auth"
	"tenantpulse/internal/chq"
	"tenantpulse/internal/domain"
	"tenantpulse/internal/registry"
)

// ------------------------------------------------------------ org dashboards
//
// An org admin (or a superuser) sees one dashboard across every store in the
// org. Each store's slice is read on its own, under that store's row-policy
// setting exactly as its own dashboard is, and the slices are added up here.
// Money is added per currency (exact) and then converted into the org's
// reporting currency at fixed rates (approximate, and labelled so). Days
// follow the viewer's timezone (?tz=, IANA), cut from 15-minute cells.

const quarterSecs = 15 * 60

func (s *Server) authorizeOrg(c *gin.Context) {
	slug := c.Param("org")
	o := s.Reg.Current().OrgsBySlug[slug]
	if o == nil {
		s.Reg.RefreshIfStale(c)
		o = s.Reg.Current().OrgsBySlug[slug]
	}
	id := c.MustGet("identity").(auth.Identity)
	switch {
	case o == nil && id.Operator:
		abort(c, 404, "not_found", "unknown org")
	case o == nil || !id.CanReadOrg(o.Slug): // same answer for "not yours" and "does not exist"
		abort(c, 403, "forbidden", "identity "+id.Subject+" may not read this org")
	case s.Direct:
		abort(c, 404, "not_found", "org dashboards are served by the aggregation layer only")
	default:
		c.Set("org", o)
	}
}

func (s *Server) listOrgs(c *gin.Context) {
	id := c.MustGet("identity").(auth.Identity)
	out := []gin.H{}
	for _, o := range s.Reg.Current().Orgs {
		if id.CanReadOrg(o.Slug) {
			out = append(out, gin.H{"slug": o.Slug, "name": o.Name, "reporting_currency": o.ReportingCurrency, "stores": len(o.Stores)})
		}
	}
	c.JSON(200, gin.H{"orgs": out, "subject": id.Subject, "role": id.Role()})
}

func (s *Server) orgInfo(c *gin.Context) {
	o := c.MustGet("org").(*registry.Org)
	stores := make([]gin.H, 0, len(o.Stores))
	for _, t := range o.Stores {
		stores = append(stores, gin.H{"slug": t.Slug, "name": t.Name, "tier": t.Tier, "status": t.Status,
			"timezone": t.Timezone, "default_currency": t.DefaultCurrency})
	}
	c.JSON(200, gin.H{"org": o.Slug, "slug": o.Slug, "name": o.Name, "status": "active",
		"reporting_currency": o.ReportingCurrency, "default_currency": o.ReportingCurrency, "currencies": []string{o.ReportingCurrency},
		"stores": stores, "fx": s.converter(o).info()})
}

// orgQuery is one org read: the org, the viewer's timezone and the range.
type orgQuery struct {
	o        *registry.Org
	tz       string
	loc      *time.Location
	from, to time.Time
}

func (s *Server) orgQuery(c *gin.Context) (orgQuery, bool) {
	o := c.MustGet("org").(*registry.Org)
	tz := c.DefaultQuery("tz", "UTC")
	loc, err := time.LoadLocation(tz)
	if err != nil {
		abort(c, 400, "bad_request", "tz must be an IANA timezone such as Asia/Kolkata")
		return orgQuery{}, false
	}
	from, to, err := parseRange(c, loc, time.Now())
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return orgQuery{}, false
	}
	if from.Unix()%quarterSecs != 0 || to.Unix()%quarterSecs != 0 {
		abort(c, 400, "bad_request", "org ranges start and end on 15-minute boundaries (whole local days always do)")
		return orgQuery{}, false
	}
	return orgQuery{o: o, tz: loc.String(), loc: loc, from: from.UTC(), to: to.UTC()}, true
}

func (s *Server) chConn() driver.Conn { return s.B.(interface{ Conn() driver.Conn }).Conn() }

// eachStore runs fn for every active store of the org, a few at a time, each
// under a query slot of its own store (the same limits as store dashboards).
func (s *Server) eachStore(ctx context.Context, q orgQuery, fn func(ctx context.Context, t *registry.Tenant, p chq.Params) error) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, t := range q.o.Stores {
		if t.Status != "active" {
			continue
		}
		g.Go(func() error {
			release, ok := s.Limiter.Acquire(gctx, t.ID)
			if !ok {
				return errBusy
			}
			defer release()
			return fn(gctx, t, chq.Params{TenantID: t.ID, Generation: t.ActiveGeneration, TZ: q.tz, From: q.from, To: q.to, Timeout: s.QueryTimeout})
		})
	}
	return g.Wait()
}

// ------------------------------------------------------------ currency conversion

// converter turns exact per-currency amounts into the org's reporting
// currency at the fixed rates in the control plane. An amount in a currency
// without a rate is left out of converted totals and listed as missing.
type converter struct {
	to      string
	units   map[string]decimal.Decimal // units of a currency per 1 USD
	asOf    string
	mu      sync.Mutex
	missing map[string]bool
}

func (s *Server) converter(o *registry.Org) *converter {
	fx := s.Reg.Current().FX
	return &converter{to: o.ReportingCurrency, units: fx.UnitsPerUSD, asOf: fx.AsOf, missing: map[string]bool{}}
}

func (cv *converter) conv(cur string, v decimal.Decimal) decimal.Decimal {
	if cur == cv.to {
		return v
	}
	ru, ok1 := cv.units[cv.to]
	cu, ok2 := cv.units[cur]
	if !ok1 || !ok2 {
		cv.mu.Lock()
		cv.missing[cur] = true
		cv.mu.Unlock()
		return decimal.Zero
	}
	return v.Mul(ru).Div(cu)
}

// factors is the multiplier from each currency into the reporting currency.
func (cv *converter) factors() map[string]float64 {
	out := map[string]float64{}
	ru, ok := cv.units[cv.to]
	for c, u := range cv.units {
		if ok {
			out[c], _ = ru.Div(u).Float64()
		}
	}
	out[cv.to] = 1
	return out
}

func (cv *converter) info() gin.H {
	rates := map[string]string{}
	for c, u := range cv.units {
		rates[c] = u.String()
	}
	cv.mu.Lock()
	missing := make([]string, 0, len(cv.missing))
	for c := range cv.missing {
		missing = append(missing, c)
	}
	cv.mu.Unlock()
	sort.Strings(missing)
	return gin.H{"reporting_currency": cv.to, "rates_as_of": cv.asOf, "units_per_usd": rates, "missing_rates": missing,
		"note": "Converted at fixed rates (tpctl fx); approximate. Per-currency amounts are exact."}
}

func money2(d decimal.Decimal) string { return d.StringFixed(2) }

// ------------------------------------------------------------ cached org reads

// orgVersion changes whenever any store in the org gets new data, and only
// grows (every store's version only grows), so min_version works as for a store.
func (s *Server) orgVersion(o *registry.Org) uint64 {
	var v uint64
	for _, t := range o.Stores {
		v += s.Cache.Version(t.ID, t.ActiveGeneration)
	}
	return v
}

// serveOrg is serve for an org: cached per org data version, one computation
// per question at a time, the previous answer served while a new one is computed.
func (s *Server) serveOrg(c *gin.Context, route string, q orgQuery, norm normalized, compute func(ctx context.Context) (any, error)) {
	start := c.MustGet("start").(time.Time)
	id := c.MustGet("identity").(auth.Identity)
	if !s.orgReady(q.o) {
		c.JSON(409, gin.H{"error": "org_not_ready", "detail": "no store in this org has finished its initial snapshot yet"})
		return
	}
	b, _ := json.Marshal(norm)
	sum := sha256.Sum256(b)
	version := s.orgVersion(q.o)
	key := "org:" + q.o.ID + ":" + hex.EncodeToString(sum[:12])
	flight := key + ":" + strconv.FormatUint(version, 10)
	minVersion, _ := strconv.ParseUint(c.Query("min_version"), 10, 64)
	bypass := id.Operator && c.GetHeader("X-TP-Cache") == "bypass"
	run := func(ctx context.Context) (answer, error) {
		qStart := time.Now()
		v, err := compute(ctx)
		promQueryDur.WithLabelValues("org_" + route).Observe(time.Since(qStart).Seconds())
		if err != nil {
			return answer{}, err
		}
		body, err := json.Marshal(v)
		a := answer{body, "clickhouse"}
		if err == nil && !bypass {
			s.Cache.Put(key, version, a)
		}
		return a, err
	}
	if !bypass {
		if a, at, fresh, ok := s.Cache.Get(key, version, minVersion); ok {
			if fresh {
				s.respondOrg(c, q.o, route, a, "HIT", at, start)
				return
			}
			s.respondOrg(c, q.o, route, a, "STALE", at, start)
			s.revalidate(q.o.ID, flight, run)
			return
		}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c), s.QueryTimeout+time.Second)
	defer cancel()
	var a answer
	var err error
	status := "MISS"
	if bypass {
		status = "BYPASS"
		a, err = run(ctx)
	} else {
		var v any
		v, err, _ = s.sf.Do(flight, func() (any, error) { return run(ctx) })
		if err == nil {
			a = v.(answer)
		}
	}
	switch {
	case errors.Is(err, errBusy):
		c.Header("Retry-After", "1")
		abort(c, 429, "too_many_requests", "query capacity exhausted; retry shortly")
	case err != nil:
		slog.Error("org query failed", "org", q.o.Slug, "route", route, "err", err)
		abort(c, 500, "query_failed", "analytics query failed")
	default:
		s.respondOrg(c, q.o, route, a, status, version, start)
	}
}

func (s *Server) orgReady(o *registry.Org) bool {
	for _, t := range o.Stores {
		if t.Status == "active" {
			return true
		}
	}
	return false
}

func (s *Server) respondOrg(c *gin.Context, o *registry.Org, route string, a answer, cache string, version uint64, start time.Time) {
	took := float64(time.Since(start).Microseconds()) / 1000
	c.Header("X-Cache", cache)
	c.Header("Server-Timing", fmt.Sprintf(`app;dur=%.2f, cache;desc="%s"`, took, cache))
	c.Header("Cache-Control", "no-store")
	body := make([]byte, 0, len(a.body)+256)
	body = append(body, `{"org":`...)
	body = strconv.AppendQuote(body, o.Slug)
	body = append(body, `,"data":`...)
	body = append(body, a.body...)
	body = fmt.Appendf(body, `,"meta":{"took_ms":%.3f,"cache":"%s","source":"%s","data_version":"%d","server_time":"%s"}}`,
		took, cache, a.source, version, time.Now().UTC().Format(time.RFC3339Nano))
	c.Data(200, "application/json", body)
	s.Win.Observe(time.Now(), took, cache, a.source)
	promReqDur.WithLabelValues("org_"+route, cache).Observe(took / 1000)
}

// orgAsOf is when the org's data is complete up to: its oldest store's.
func (s *Server) orgAsOf(o *registry.Org) *time.Time {
	var out *time.Time
	for _, t := range o.Stores {
		if a := s.Cache.AsOf(t.ID, t.ActiveGeneration); a != nil && (out == nil || a.Before(*out)) {
			out = a
		}
	}
	return out
}

// ------------------------------------------------------------ handlers

type tally struct {
	orders     int64
	gross, rec decimal.Decimal
}

func (m tally) add(n int64, g, r decimal.Decimal) tally {
	return tally{m.orders + n, m.gross.Add(g), m.rec.Add(r)}
}

func (s *Server) orgSummary(c *gin.Context) {
	q, ok := s.orgQuery(c)
	if !ok {
		return
	}
	cur := q.o.ReportingCurrency
	s.serveOrg(c, "summary", q, normalized{Route: "org-summary", From: q.from.UnixMilli(), To: q.to.UnixMilli(), Currency: cur, TZ: q.tz},
		func(ctx context.Context) (any, error) {
			cv := s.converter(q.o)
			prevFrom := q.from.Add(-q.to.Sub(q.from))
			both := q
			both.from = prevFrom
			type slice struct{ now, before map[string]tally }
			var mu sync.Mutex
			per := map[string]slice{}
			err := s.eachStore(ctx, both, func(ctx context.Context, t *registry.Tenant, p chq.Params) error {
				cells, err := chq.OrgOrders(ctx, s.chConn(), p, "period", q.from)
				if err != nil {
					return err
				}
				sl := slice{map[string]tally{}, map[string]tally{}}
				for _, cl := range cells {
					m := sl.now
					if cl.Bucket == "p" {
						m = sl.before
					}
					m[cl.Currency] = m[cl.Currency].add(cl.Orders, cl.Gross, cl.Recognized)
				}
				mu.Lock()
				per[t.ID] = sl
				mu.Unlock()
				return nil
			})
			if err != nil {
				return nil, err
			}
			exact := map[string]tally{} // per currency, current period
			var now, before tally       // converted
			stores := []gin.H{}
			for _, t := range q.o.Stores {
				sl, ok := per[t.ID]
				if !ok {
					continue
				}
				var st tally
				for c, m := range sl.now {
					exact[c] = exact[c].add(m.orders, m.gross, m.rec)
					st = st.add(m.orders, cv.conv(c, m.gross), cv.conv(c, m.rec))
				}
				for c, m := range sl.before {
					before = before.add(m.orders, cv.conv(c, m.gross), cv.conv(c, m.rec))
				}
				now = now.add(st.orders, st.gross, st.rec)
				stores = append(stores, gin.H{"slug": t.Slug, "name": t.Name, "timezone": t.Timezone, "currency": t.DefaultCurrency,
					"orders": st.orders, "gross_sales": money2(st.gross), "recognized_sales": money2(st.rec), "gross": st.gross})
			}
			sort.SliceStable(stores, func(i, j int) bool {
				return stores[i]["gross"].(decimal.Decimal).GreaterThan(stores[j]["gross"].(decimal.Decimal))
			})
			for _, st := range stores {
				delete(st, "gross")
			}
			byCur := []gin.H{}
			curs := make([]string, 0, len(exact))
			for c := range exact {
				curs = append(curs, c)
			}
			sort.Strings(curs)
			for _, c := range curs {
				m := exact[c]
				byCur = append(byCur, gin.H{"currency": c, "orders": m.orders, "gross_sales": m.gross.String(),
					"recognized_sales": m.rec.String(), "gross_sales_converted": money2(cv.conv(c, m.gross))})
			}
			totals := func(m tally) []chq.CurrencyTotals {
				if m.orders == 0 {
					return []chq.CurrencyTotals{}
				}
				return []chq.CurrencyTotals{{Currency: cur, Orders: m.orders, GrossSales: money2(m.gross), RecognizedSales: money2(m.rec)}}
			}
			return gin.H{"from": q.from, "to": q.to, "timezone": q.tz, "currency": cur,
				"totals": totals(now), "total_orders": now.orders,
				"previous":    gin.H{"from": prevFrom, "to": q.from, "totals": totals(before), "total_orders": before.orders},
				"by_currency": byCur, "stores": stores, "fx": cv.info(), "as_of": s.orgAsOf(q.o)}, nil
		})
}

func (s *Server) orgBreakdown(c *gin.Context) {
	q, ok := s.orgQuery(c)
	if !ok {
		return
	}
	cur := q.o.ReportingCurrency
	s.serveOrg(c, "breakdown", q, normalized{Route: "org-breakdown", From: q.from.UnixMilli(), To: q.to.UnixMilli(), Currency: cur, TZ: q.tz},
		func(ctx context.Context) (any, error) {
			cv := s.converter(q.o)
			var mu sync.Mutex
			byStatus := map[string]tally{}
			err := s.eachStore(ctx, q, func(ctx context.Context, t *registry.Tenant, p chq.Params) error {
				cells, err := chq.OrgOrders(ctx, s.chConn(), p, "", time.Time{})
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				for _, cl := range cells {
					byStatus[cl.Status] = byStatus[cl.Status].add(cl.Orders, cv.conv(cl.Currency, cl.Gross), decimal.Zero)
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			out := make([]breakdownRow, 0, len(domain.AllStatuses))
			var total int64
			for _, st := range domain.AllStatuses {
				g := money2(byStatus[st].gross)
				out = append(out, breakdownRow{Status: st, Orders: byStatus[st].orders, GrossSales: &g, Recognized: domain.IsRecognized(st)})
				total += byStatus[st].orders
			}
			return gin.H{"from": q.from, "to": q.to, "timezone": q.tz, "currency": cur,
				"statuses": out, "total_orders": total, "fx": cv.info(), "as_of": s.orgAsOf(q.o)}, nil
		})
}

func (s *Server) orgTrends(c *gin.Context) {
	q, ok := s.orgQuery(c)
	if !ok {
		return
	}
	grain, err := parseGrain(c, q.from, q.to)
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	cur := q.o.ReportingCurrency
	s.serveOrg(c, "trends", q, normalized{Route: "org-trends", From: q.from.UnixMilli(), To: q.to.UnixMilli(), Currency: cur, Grain: grain, TZ: q.tz},
		func(ctx context.Context) (any, error) {
			cv := s.converter(q.o)
			var mu sync.Mutex
			byBucket := map[string]tally{}
			err := s.eachStore(ctx, q, func(ctx context.Context, t *registry.Tenant, p chq.Params) error {
				cells, err := chq.OrgOrders(ctx, s.chConn(), p, grain, time.Time{})
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				for _, cl := range cells {
					byBucket[cl.Bucket] = byBucket[cl.Bucket].add(cl.Orders, cv.conv(cl.Currency, cl.Gross), cv.conv(cl.Currency, cl.Recognized))
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			pts := make([]chq.TrendPoint, 0, len(byBucket))
			for b, m := range byBucket {
				pts = append(pts, chq.TrendPoint{Bucket: b, Orders: m.orders, GrossSales: money2(m.gross), RecognizedSales: money2(m.rec)})
			}
			return gin.H{"from": q.from, "to": q.to, "timezone": q.tz, "currency": cur, "grain": grain,
				"points": fillBuckets(pts, chq.Params{From: q.from, To: q.to}, q.loc, grain), "fx": cv.info(), "as_of": s.orgAsOf(q.o)}, nil
		})
}

type orgTopProduct struct {
	Rank      int     `json:"rank"`
	ProductID string  `json:"product_id"`
	Name      string  `json:"name"`
	SKU       *string `json:"sku"`
	Store     string  `json:"store"`
	StoreName string  `json:"store_name"`
	Quantity  string  `json:"quantity"`
	Revenue   string  `json:"revenue"`
	revenue   decimal.Decimal
	tenant    *registry.Tenant
	src       *chq.OrgProduct
}

func (s *Server) orgTop(c *gin.Context) {
	q, ok := s.orgQuery(c)
	if !ok {
		return
	}
	limit, offset, err := parsePage(c)
	if err != nil {
		abort(c, 400, "bad_request", err.Error())
		return
	}
	cur := q.o.ReportingCurrency
	s.serveOrg(c, "top", q, normalized{Route: "org-top", From: q.from.UnixMilli(), To: q.to.UnixMilli(), Currency: cur, Limit: limit, Offset: offset, TZ: q.tz},
		func(ctx context.Context) (any, error) {
			cv := s.converter(q.o)
			factors := cv.factors()
			var mu sync.Mutex
			var all []*orgTopProduct
			total := 0
			// Each store's first offset+limit products by converted revenue hold
			// the org's first offset+limit.
			err := s.eachStore(ctx, q, func(ctx context.Context, t *registry.Tenant, p chq.Params) error {
				ps, n, err := chq.OrgTopProducts(ctx, s.chConn(), p, factors, offset+limit)
				if err != nil {
					return err
				}
				mu.Lock()
				defer mu.Unlock()
				total += n
				for i := range ps {
					rv := decimal.Zero
					for c, v := range ps[i].Revenue {
						rv = rv.Add(cv.conv(c, v))
					}
					all = append(all, &orgTopProduct{ProductID: ps[i].ProductID, Store: t.Slug, StoreName: t.Name,
						Quantity: ps[i].Quantity.String(), revenue: rv, tenant: t, src: &ps[i]})
				}
				return nil
			})
			if err != nil {
				return nil, err
			}
			sort.Slice(all, func(i, j int) bool {
				if !all[i].revenue.Equal(all[j].revenue) {
					return all[i].revenue.GreaterThan(all[j].revenue)
				}
				if all[i].Store != all[j].Store {
					return all[i].Store < all[j].Store
				}
				return all[i].ProductID < all[j].ProductID
			})
			page := []*orgTopProduct{}
			if offset < len(all) {
				page = all[offset:min(len(all), offset+limit)]
			}
			if err := s.nameProducts(ctx, q, page); err != nil {
				return nil, err
			}
			for i, p := range page {
				p.Rank, p.Revenue = offset+i+1, money2(p.revenue)
				p.Name, p.SKU = p.src.Name, p.src.SKU
				if p.Name == "" {
					p.Name = "(unknown product)"
				}
			}
			return gin.H{"from": q.from, "to": q.to, "timezone": q.tz, "currency": cur,
				"metric": "recognized line revenue, converted at fixed rates", "products": page,
				"limit": limit, "offset": offset, "total": total, "next_offset": nextOffset(offset, len(page), total),
				"fx": cv.info(), "as_of": s.orgAsOf(q.o)}, nil
		})
}

// nameProducts looks up names for one page of products, one query per store.
func (s *Server) nameProducts(ctx context.Context, q orgQuery, page []*orgTopProduct) error {
	byStore := map[*registry.Tenant][]chq.OrgProduct{}
	for _, p := range page {
		byStore[p.tenant] = append(byStore[p.tenant], *p.src)
	}
	for t, ps := range byStore {
		p := chq.Params{TenantID: t.ID, Generation: t.ActiveGeneration, Timeout: s.QueryTimeout}
		if err := chq.ProductNames(ctx, s.chConn(), p, ps); err != nil {
			return err
		}
		names := map[string]chq.OrgProduct{}
		for _, op := range ps {
			names[op.ProductID] = op
		}
		for _, pp := range page {
			if pp.tenant == t {
				n := names[pp.ProductID]
				pp.src.Name, pp.src.SKU = n.Name, n.SKU
			}
		}
	}
	return nil
}

// ------------------------------------------------------------ org freshness

// orgFreshnessBody describes the org's data as of its slowest store: the
// oldest "complete up to", the largest delay of a latest change, and the
// worst pipeline health. Stores still loading their first copy are left out.
func (s *Server) orgFreshnessBody(o *registry.Org, start time.Time) (gin.H, string) {
	now := time.Now()
	rank := map[string]int{"live": 0, "lagging": 1, "stalled": 2}
	worst, n, behind := "live", 0, 0
	var bound int64 // the slowest store's freshness step; 0 once any store is beyond the last step
	beyond := false
	var asOf, lastChange *time.Time
	var lag *int64
	for _, t := range o.Stores {
		if t.Status != "active" {
			continue
		}
		st, _ := s.Cache.Status(t.ID, t.ActiveGeneration)
		h := pipelineHealth(st, now)
		if h == "snapshotting" {
			continue
		}
		n++
		if rank[h] > 0 {
			behind++
		}
		if rank[h] > rank[worst] {
			worst = h
		}
		if b := freshBound(st, now); b == 0 {
			beyond = true
		} else {
			bound = max(bound, b)
		}
		if a := st.DataAsOf(); a != nil && (asOf == nil || a.Before(*asOf)) {
			asOf = a
		}
		if lc := st.LastSourceCommitAt; lc != nil && (lastChange == nil || lc.After(*lastChange)) {
			lastChange = lc
		}
		if l := st.LastApplyLagMs; l != nil && (lag == nil || *l > *lag) {
			lag = l
		}
	}
	if n == 0 {
		worst = "snapshotting"
	}
	if beyond {
		bound = 0
	}
	var age *int64
	if asOf != nil {
		v := now.Sub(*asOf).Milliseconds()
		age = &v
	}
	version := strconv.FormatUint(s.orgVersion(o), 10)
	return gin.H{"scope": "org:" + o.Slug, "org": o.Slug, "status": worst, "stores": n, "stores_behind": behind,
			"data_as_of": asOf, "data_age_ms": age, "fresh_within_ms": bound, "processing_lag_ms": lag, "last_source_commit_at": lastChange,
			"data_version": version, "server_time": now.UTC(), "took_ms": float64(time.Since(start).Microseconds()) / 1000},
		fmt.Sprintf("%s|%d|%d|%s|%d", worst, n, behind, version, bound)
}

func (s *Server) orgFreshness(c *gin.Context) {
	body, _ := s.orgFreshnessBody(c.MustGet("org").(*registry.Org), c.MustGet("start").(time.Time))
	c.Header("Cache-Control", "no-store")
	c.JSON(200, body)
}

// orgStream is stream for an org: an event when any of its stores gets new
// data or changes health.
func (s *Server) orgStream(c *gin.Context) {
	slug := c.Param("org")
	o := c.MustGet("org").(*registry.Org)
	s.sse(c, func() (gin.H, string, <-chan struct{}, func()) {
		if cur := s.Reg.Current().OrgsBySlug[slug]; cur != nil {
			o = cur // stores can join or leave under a long-lived stream
		}
		keys := make([]string, 0, len(o.Stores))
		for _, t := range o.Stores {
			keys = append(keys, StoreKey(t.ID, t.ActiveGeneration))
		}
		changed, cancel := s.Cache.ChangedAny(keys) // before reading, so no change is missed
		body, state := s.orgFreshnessBody(o, time.Now())
		return body, state, changed, cancel
	})
}
