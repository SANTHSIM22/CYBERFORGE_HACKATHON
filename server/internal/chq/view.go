package chq

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/ch"
	"tenantpulse/internal/domain"
)

// View is an in-memory replica of one tenant generation's rollups. It loads
// the tenant's day cells, the last viewHours of hour cells, the last
// viewProductDays of product-day cells and the product names once, then
// applies only the rows written since: every rollup row carries a `_version`
// that only grows, so "rows above the highest version applied" is exactly the
// change set, read with ONE query per data version for all four. Summary,
// status breakdown and trends over whole tenant-local days, hourly trends over
// recent whole UTC hours and top products for the preset windows are then
// computed in memory. ClickHouse work therefore follows how often a store's
// data changes, not how many people view it or how many widgets and date
// ranges they open. Anything the view cannot answer exactly returns ok=false,
// and the caller queries ClickHouse as before.
type View struct {
	TenantID string
	Gen      int64
	loc      *time.Location

	syncMu sync.Mutex // one sync at a time

	mu        sync.RWMutex
	loaded    bool
	synced    uint64 // tenant data version reflected
	dayV      uint64 // highest _version applied, per table
	hourV     uint64
	prodV     uint64
	nameV     uint64
	days      map[dayKey]cell
	hours     map[hourKey]cell
	hourFloor int64 // unix second of the oldest hour held
	prods     map[prodKey]pcell
	prodFloor int32 // oldest product day held
	prodExact bool  // false if a product value did not fit the compact form (then top products use ClickHouse)
	pids      []string
	pidIndex  map[string]int32
	names     map[int32]prodName
	top       *topWindows
}

type dayKey struct {
	day              int32 // days since 1970-01-01 of the tenant-local calendar day
	currency, status string
}

type hourKey struct {
	hour             int64 // unix second of the UTC hour start
	currency, status string
}

type prodKey struct {
	day      int32
	currency string
	pid      int32 // index into View.pids
}

// cell holds money scaled by 1e4, the rollups' Decimal(38, 4) scale.
type cell struct{ n, g, r int64 }

// pcell holds quantity and revenue scaled by 1e6. Values that do not fit
// exactly turn product answers over to ClickHouse (prodExact = false).
type pcell struct{ q, rv int64 }

type prodName struct {
	name string
	sku  *string
}

const (
	viewHours       = 72
	viewProductDays = 95 // the longest preset window is 90 days
	prodScale       = 6
)

func NewView(tenantID string, gen int64, tz string) *View {
	return &View{TenantID: tenantID, Gen: gen, loc: location(tz)}
}

// Current reports whether the view reflects at least this data version.
func (v *View) Current(version uint64) bool { return v.current(version) }

// Loaded reports whether the first full load has completed.
func (v *View) Loaded() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.loaded
}

func (v *View) current(version uint64) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.loaded && v.synced >= version
}

func (v *View) qctx(ctx context.Context, timeout time.Duration, extra clickhouse.Parameters) context.Context {
	params := clickhouse.Parameters{"t": v.TenantID, "g": strconv.FormatInt(v.Gen, 10)}
	for k, val := range extra {
		params[k] = val
	}
	return ch.TenantCtx(ctx, v.TenantID, timeout, params)
}

// Sync brings the view up to the tenant's data version (a no-op when it is
// already there). The first call loads everything the view holds; later calls
// read only rows with a higher _version, in one query.
func (v *View) Sync(ctx context.Context, conn driver.Conn, version uint64, timeout time.Duration) error {
	if v.current(version) {
		return nil
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	if v.current(version) {
		return nil
	}
	v.mu.RLock()
	loaded, dayV, hourV, prodV, nameV := v.loaded, v.dayV, v.hourV, v.prodV, v.nameV
	v.mu.RUnlock()
	now := time.Now()
	hourFloor := now.UTC().Truncate(time.Hour).Add(-viewHours * time.Hour).Unix()
	prodFloorDay := domain.LocalDay(now.AddDate(0, 0, -viewProductDays), v.loc)

	// k: 0 day cell, 1 hour cell, 2 product-day cell, 3 product name.
	// For names, n carries flags: 1 = deleted, 2 = sku is NULL.
	final, dv, hv, pv, nv := " FINAL", "", "", "", ""
	params := clickhouse.Parameters{"hf": strconv.FormatInt(hourFloor, 10), "pf": prodFloorDay}
	if loaded { // deltas: every row written since the last sync
		final = ""
		dv, hv, pv, nv = " AND _version > {dv:UInt64}", " AND _version > {hv:UInt64}", " AND _version > {pv:UInt64}", " AND _version > {nv:UInt64}"
		params["dv"], params["hv"] = strconv.FormatUint(dayV, 10), strconv.FormatUint(hourV, 10)
		params["pv"], params["nv"] = strconv.FormatUint(prodV, 10), strconv.FormatUint(nameV, 10)
	}
	rows, err := conn.Query(v.qctx(ctx, timeout, params), `
		SELECT * FROM (
			SELECT toUInt8(0) AS k, toUInt32(day) AS at, toString(currency) AS c, toString(status) AS s, '' AS pid,
			       orders AS n, toString(gross) AS a, toString(recognized) AS b, _version AS ver
			FROM tp.order_day_rollup`+final+` WHERE `+tenantWhere+dv+`
			UNION ALL
			SELECT 1, toUInt32(hour), toString(currency), toString(status), '', orders, toString(gross), toString(recognized), _version
			FROM tp.order_hour_rollup`+final+` WHERE `+tenantWhere+` AND hour >= toDateTime({hf:UInt32}, 'UTC')`+hv+`
			UNION ALL
			SELECT 2, toUInt32(day), toString(currency), '', toString(product_id), toUInt64(0), toString(quantity), toString(revenue), _version
			FROM tp.product_day_rollup`+final+` WHERE `+tenantWhere+` AND day >= {pf:Date}`+pv+`
			UNION ALL
			SELECT 3, toUInt32(0), '', '', toString(product_id), toUInt64(is_deleted) + 2 * toUInt64(isNull(sku)), name, ifNull(sku, ''), _version
			FROM tp.products`+final+` WHERE `+tenantWhere+nv+`
		) ORDER BY ver`)
	if err != nil {
		return err
	}
	defer rows.Close()
	type row struct {
		k         uint8
		at        uint32
		c, s, pid string
		n         uint64
		a, b      string
		ver       uint64
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.k, &r.at, &r.c, &r.s, &r.pid, &r.n, &r.a, &r.b, &r.ver); err != nil {
			return err
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	if !loaded {
		v.days, v.hours, v.hourFloor = map[dayKey]cell{}, map[hourKey]cell{}, hourFloor
		v.prods, v.prodExact, v.pidIndex, v.names = map[prodKey]pcell{}, true, map[string]int32{}, map[int32]prodName{}
		v.prodFloor = dayNum(prodFloorDay)
	}
	for _, r := range got {
		switch r.k {
		case 0:
			v.days[dayKey{int32(r.at), intern(r.c), intern(r.s)}] = cell{int64(r.n), scaled(r.a), scaled(r.b)}
			v.dayV = max(v.dayV, r.ver)
		case 1:
			v.hours[hourKey{int64(r.at), intern(r.c), intern(r.s)}] = cell{int64(r.n), scaled(r.a), scaled(r.b)}
			v.hourV = max(v.hourV, r.ver)
		case 2:
			q, ok1 := compact(r.a)
			rv, ok2 := compact(r.b)
			if !ok1 || !ok2 {
				v.prodExact = false
			}
			v.prods[prodKey{int32(r.at), intern(r.c), v.pidOf(r.pid)}] = pcell{q, rv}
			v.prodV = max(v.prodV, r.ver)
		case 3:
			id := v.pidOf(r.pid)
			if r.n&1 == 1 {
				delete(v.names, id)
			} else {
				pn := prodName{name: r.a}
				if r.n&2 == 0 {
					sku := r.b
					pn.sku = &sku
				}
				v.names[id] = pn
			}
			v.nameV = max(v.nameV, r.ver)
		}
	}
	if hourFloor > v.hourFloor { // keep memory bounded: drop what left the windows
		for k := range v.hours {
			if k.hour < hourFloor {
				delete(v.hours, k)
			}
		}
		v.hourFloor = hourFloor
	}
	if pf := dayNum(prodFloorDay); pf > v.prodFloor {
		for k := range v.prods {
			if k.day < pf {
				delete(v.prods, k)
			}
		}
		v.prodFloor = pf
	}
	v.loaded, v.synced, v.top = true, max(v.synced, version), nil
	return nil
}

// pidOf interns a product id per view. Callers hold v.mu.
func (v *View) pidOf(pid string) int32 {
	if i, ok := v.pidIndex[pid]; ok {
		return i
	}
	i := int32(len(v.pids))
	v.pids = append(v.pids, pid)
	v.pidIndex[pid] = i
	return i
}

func scaled(s string) int64 { return decimal.RequireFromString(s).Shift(4).IntPart() }

var maxCompact = decimal.New(1, 18)

// compact returns s scaled by 1e6 as an int64, and whether that is exact.
func compact(s string) (int64, bool) {
	d := decimal.RequireFromString(s).Shift(prodScale)
	if !d.Equal(d.Truncate(0)) || d.Abs().GreaterThanOrEqual(maxCompact) {
		return 0, false
	}
	return d.IntPart(), true
}

func money(v int64) decimal.Decimal { return decimal.New(v, -4) }

var internMu sync.Mutex
var interned = map[string]string{}

// intern shares the handful of currency and status strings across all cells.
func intern(s string) string {
	internMu.Lock()
	defer internMu.Unlock()
	if v, ok := interned[s]; ok {
		return v
	}
	interned[s] = s
	return s
}

func dayNum(day string) int32 {
	t, _ := time.Parse(time.DateOnly, day)
	return int32(t.Unix() / 86400)
}

func dayString(d int32) string { return time.Unix(int64(d)*86400, 0).UTC().Format(time.DateOnly) }

// days returns [d1, d2) when [from, to) is exactly whole tenant-local days.
func (v *View) dayRange(from, to time.Time) (d1, d2 int32, ok bool) {
	plan := domain.Decompose(from, to, v.loc, true)
	if len(plan.Hours) > 0 || len(plan.Raw) > 0 {
		return 0, 0, false
	}
	if len(plan.Days) == 0 {
		return 0, 0, true // empty range
	}
	return dayNum(plan.Days[0].From), dayNum(plan.Days[len(plan.Days)-1].To), true
}

// Summary mirrors chq.Summary: per-currency totals for [from, to) and for the
// equally long period right before it.
func (v *View) Summary(p Params) (cur, prev []CurrencyTotals, ok bool) {
	c1, c2, ok1 := v.dayRange(p.From, p.To)
	p1, p2, ok2 := v.dayRange(p.From.Add(-p.To.Sub(p.From)), p.From)
	if !ok1 || !ok2 {
		return nil, nil, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if !v.loaded {
		return nil, nil, false
	}
	cm, pm := map[string]cell{}, map[string]cell{}
	for k, c := range v.days {
		if k.day >= c1 && k.day < c2 {
			cm[k.currency] = add(cm[k.currency], c)
		}
		if k.day >= p1 && k.day < p2 {
			pm[k.currency] = add(pm[k.currency], c)
		}
	}
	cur, prev = []CurrencyTotals{}, []CurrencyTotals{}
	for _, c := range sortedKeys(cm, pm) {
		if t := cm[c]; t.n > 0 {
			cur = append(cur, CurrencyTotals{c, t.n, money(t.g).String(), money(t.r).String()})
		}
		if t := pm[c]; t.n > 0 {
			prev = append(prev, CurrencyTotals{c, t.n, money(t.g).String(), money(t.r).String()})
		}
	}
	return cur, prev, true
}

// StatusRows mirrors chq.StatusRows.
func (v *View) StatusRows(p Params) ([]StatusRow, bool) {
	d1, d2, ok := v.dayRange(p.From, p.To)
	if !ok {
		return nil, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if !v.loaded {
		return nil, false
	}
	type cs struct{ currency, status string }
	m := map[cs]cell{}
	for k, c := range v.days {
		if k.day >= d1 && k.day < d2 && (p.Currency == "" || k.currency == p.Currency) {
			m[cs{k.currency, k.status}] = add(m[cs{k.currency, k.status}], c)
		}
	}
	var out []StatusRow
	for k, c := range m {
		if c.n > 0 {
			out = append(out, StatusRow{Currency: k.currency, Status: k.status, Orders: c.n, Gross: money(c.g), Recognized: money(c.r)})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Currency != out[j].Currency {
			return out[i].Currency < out[j].Currency
		}
		return out[i].Status < out[j].Status
	})
	return out, true
}

// Trend mirrors chq.Trend: orders count every currency, sales only p.Currency.
func (v *View) Trend(p Params) ([]TrendPoint, bool) {
	type acc struct{ n, g, r int64 }
	m := map[string]acc{}
	put := func(b string, cur string, c cell) {
		a := m[b]
		a.n += c.n
		if cur == p.Currency {
			a.g += c.g
			a.r += c.r
		}
		m[b] = a
	}
	if p.Grain == "hour" {
		plan := domain.Decompose(p.From, p.To, v.loc, false)
		if len(plan.Raw) > 0 || len(plan.Days) > 0 {
			return nil, false
		}
		v.mu.RLock()
		defer v.mu.RUnlock()
		if !v.loaded {
			return nil, false
		}
		for _, h := range plan.Hours {
			if h.From.Unix() < v.hourFloor {
				return nil, false
			}
		}
		for k, c := range v.hours {
			for _, h := range plan.Hours {
				if k.hour >= h.From.Unix() && k.hour < h.To.Unix() {
					put(time.Unix(k.hour, 0).UTC().Format("2006-01-02T15:04:05Z"), k.currency, c)
				}
			}
		}
	} else {
		d1, d2, ok := v.dayRange(p.From, p.To)
		if !ok {
			return nil, false
		}
		v.mu.RLock()
		defer v.mu.RUnlock()
		if !v.loaded {
			return nil, false
		}
		for k, c := range v.days {
			if k.day >= d1 && k.day < d2 {
				put(dayString(k.day), k.currency, c)
			}
		}
	}
	var out []TrendPoint
	for b, a := range m {
		if a.n > 0 {
			out = append(out, TrendPoint{Bucket: b, Orders: a.n, GrossSales: money(a.g).String(), RecognizedSales: money(a.r).String()})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bucket < out[j].Bucket })
	return out, true
}

func add(a, b cell) cell { return cell{a.n + b.n, a.g + b.g, a.r + b.r} }

func sortedKeys(ms ...map[string]cell) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, m := range ms {
		for k := range m {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

// ---------------------------------------------------------------- top products

// presetWindows are the "last N days" ranges whose top products the view
// answers: today, 7, 30 and 90 days ending today.
var presetWindows = []int{1, 7, 30, 90}

type topWindows struct {
	version uint64
	today   string                  // tenant-local day the windows end on (inclusive)
	ranked  []map[string][]topEntry // per window: currency -> products by revenue
}

type topEntry struct {
	pid   int32
	q, rv int64
}

// TopProducts mirrors chq.TopProducts for the preset windows, computed from
// the product-day cells in memory: one page and the number of products that
// sold. Other ranges return ok=false.
func (v *View) TopProducts(p Params) ([]TopProduct, int, bool) {
	now := time.Now()
	w, ok := v.window(p, now)
	if !ok {
		return nil, 0, false
	}
	tw := v.topFor(now)
	if tw == nil {
		return nil, 0, false
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	ranked := tw.ranked[w][p.Currency]
	out := []TopProduct{}
	for i := p.Offset; i < len(ranked) && len(out) < p.Limit; i++ {
		e := ranked[i]
		t := TopProduct{Rank: i + 1, ProductID: v.pids[e.pid], Name: "(unknown product)",
			Quantity: decimal.New(e.q, -prodScale).String(), Revenue: decimal.New(e.rv, -prodScale).String()}
		if n, ok := v.names[e.pid]; ok {
			t.Name, t.SKU = n.name, n.sku
		}
		out = append(out, t)
	}
	return out, len(ranked), true
}

// window returns the preset window index matching [p.From, p.To), if any.
func (v *View) window(p Params, now time.Time) (int, bool) {
	for i, n := range presetWindows {
		r := domain.PresetRange(now, n, v.loc)
		if r.From.Equal(p.From) && r.To.Equal(p.To) {
			return i, true
		}
	}
	return 0, false
}

// topFor ranks every preset window in one pass over the product cells, once
// per data version and tenant-local day.
func (v *View) topFor(now time.Time) *topWindows {
	today := domain.LocalDay(now, v.loc)
	v.mu.RLock()
	tw, loaded, exact, version := v.top, v.loaded, v.prodExact, v.synced
	v.mu.RUnlock()
	if !loaded || !exact {
		return nil
	}
	if tw != nil && tw.today == today && tw.version == version {
		return tw
	}
	starts := make([]int32, len(presetWindows))
	for i, n := range presetWindows {
		starts[i] = dayNum(domain.LocalDay(domain.PresetRange(now, n, v.loc).From, v.loc))
	}
	end := dayNum(domain.LocalDay(domain.PresetRange(now, 1, v.loc).To, v.loc))

	type key struct {
		w   int
		cur string
		pid int32
	}
	sums := map[key]pcell{}
	v.mu.RLock()
	if starts[len(starts)-1] < v.prodFloor || v.synced != version {
		v.mu.RUnlock()
		return nil
	}
	for k, c := range v.prods {
		if k.day >= end {
			continue
		}
		for i, s := range starts {
			if k.day >= s {
				kk := key{i, k.currency, k.pid}
				t := sums[kk]
				sums[kk] = pcell{t.q + c.q, t.rv + c.rv}
			}
		}
	}
	pids := v.pids
	v.mu.RUnlock()

	tw = &topWindows{version: version, today: today, ranked: make([]map[string][]topEntry, len(presetWindows))}
	for i := range tw.ranked {
		tw.ranked[i] = map[string][]topEntry{}
	}
	for k, t := range sums {
		if t.q == 0 && t.rv == 0 {
			continue // HAVING sum(qty) != 0 OR sum(rv) != 0
		}
		tw.ranked[k.w][k.cur] = append(tw.ranked[k.w][k.cur], topEntry{k.pid, t.q, t.rv})
	}
	for _, byCur := range tw.ranked {
		for _, es := range byCur {
			sort.Slice(es, func(i, j int) bool { // ORDER BY revenue DESC, pid
				if es[i].rv != es[j].rv {
					return es[i].rv > es[j].rv
				}
				return pids[es[i].pid] < pids[es[j].pid]
			})
		}
	}
	v.mu.Lock()
	if v.synced == version {
		v.top = tw
	}
	v.mu.Unlock()
	return tw
}
