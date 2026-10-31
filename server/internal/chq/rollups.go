package chq

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/ch"
	"tenantpulse/internal/domain"
)

// Query plan: whole tenant-local days come from the day rollups, whole UTC
// hours from the hour rollup, and only partial-hour edges from raw rows. The
// rollups are maintained by the ingester, so a typical dashboard query reads
// tens of rows instead of every order in the range.

var locs sync.Map

func location(tz string) *time.Location {
	if l, ok := locs.Load(tz); ok {
		return l.(*time.Location)
	}
	l, err := time.LoadLocation(tz)
	if err != nil {
		l = time.UTC
	}
	locs.Store(tz, l)
	return l
}

// qb accumulates named server-side parameters for one statement.
type qb struct {
	params clickhouse.Parameters
	n      int
}

func newQB(p Params) *qb {
	return &qb{params: clickhouse.Parameters{"t": p.TenantID, "g": strconv.FormatInt(p.Generation, 10)}}
}

func (q *qb) arg(typ, val string) string {
	q.n++
	name := "a" + strconv.Itoa(q.n)
	q.params[name] = val
	return "{" + name + ":" + typ + "}"
}

const tenantWhere = "tenant_id = {t:UUID} AND generation = {g:UInt32}"

// orderParts returns UNION ALL branches yielding (b, currency, status, n, g, r)
// for one plan; bucket(tier) gives the bucket/period expression per tier.
func orderParts(q *qb, plan domain.Plan, bucket func(tier string) string, curFilter string) []string {
	var parts []string
	for _, d := range plan.Days {
		parts = append(parts, fmt.Sprintf(`SELECT %s AS b, currency, status, orders AS n, gross AS g, recognized AS r
			FROM tp.order_day_rollup FINAL WHERE %s AND day >= %s AND day < %s%s`,
			bucket("day"), tenantWhere, q.arg("Date", d.From), q.arg("Date", d.To), curFilter))
	}
	for _, h := range plan.Hours {
		parts = append(parts, fmt.Sprintf(`SELECT %s AS b, currency, status, orders AS n, gross AS g, recognized AS r
			FROM tp.order_hour_rollup FINAL WHERE %s AND hour >= %s AND hour < %s%s`,
			bucket("hour"), tenantWhere, q.arg("DateTime('UTC')", h.From.UTC().Format(time.DateTime)), q.arg("DateTime('UTC')", h.To.UTC().Format(time.DateTime)), curFilter))
	}
	for _, r := range plan.Raw {
		parts = append(parts, fmt.Sprintf(`SELECT %s AS b, currency, status, toUInt64(1) AS n, toDecimal128(total_amount, 4) AS g,
			if(status IN %s, toDecimal128(total_amount, 4), toDecimal128(0, 4)) AS r
			FROM tp.orders FINAL WHERE %s AND created_at >= %s AND created_at < %s AND is_deleted = 0%s`,
			bucket("raw"), recognizedSQL, tenantWhere, q.arg("DateTime64(6, 'UTC')", ch.Time(r.From)), q.arg("DateTime64(6, 'UTC')", ch.Time(r.To)), curFilter))
	}
	return parts
}

func union(parts []string) string {
	if len(parts) == 0 {
		return `SELECT '' AS b, '' AS currency, '' AS status, toUInt64(0) AS n, toDecimal128(0, 4) AS g, toDecimal128(0, 4) AS r WHERE 0`
	}
	return strings.Join(parts, "\nUNION ALL\n")
}

func (p Params) qctx(ctx context.Context, q *qb) context.Context {
	timeout := p.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return ch.TenantCtx(ctx, p.TenantID, timeout, q.params)
}

// StatusRows returns (currency, status) aggregates for [from, to): the basis of
// the status breakdown. Currencies are never summed together.
func StatusRows(ctx context.Context, conn driver.Conn, p Params) ([]StatusRow, error) {
	q := newQB(p)
	cur := ""
	if p.Currency != "" {
		cur = " AND currency = " + q.arg("String", p.Currency)
	}
	plan := domain.Decompose(p.From, p.To, location(p.TZ), true)
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT currency, status, sum(n), toString(sum(g)), toString(sum(r))
		FROM (`+union(orderParts(q, plan, func(string) string { return "''" }, cur))+`)
		GROUP BY currency, status HAVING sum(n) > 0 ORDER BY currency, status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StatusRow
	for rows.Next() {
		var r StatusRow
		var n uint64
		var g, rec string
		if err := rows.Scan(&r.Currency, &r.Status, &n, &g, &rec); err != nil {
			return nil, err
		}
		r.Orders = int64(n)
		r.Gross, r.Recognized = decimal.RequireFromString(g), decimal.RequireFromString(rec)
		out = append(out, r)
	}
	return out, rows.Err()
}

// Summary returns per-currency totals for [from, to) and for the equally long
// period right before it, in one statement (KPI deltas).
func Summary(ctx context.Context, conn driver.Conn, p Params) (cur, prev []CurrencyTotals, err error) {
	q := newQB(p)
	loc := location(p.TZ)
	prevFrom := p.From.Add(-p.To.Sub(p.From))
	parts := orderParts(q, domain.Decompose(p.From, p.To, loc, true), func(string) string { return "'c'" }, "")
	parts = append(parts, orderParts(q, domain.Decompose(prevFrom, p.From, loc, true), func(string) string { return "'p'" }, "")...)
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT currency,
		       sumIf(n, b = 'c'), toString(sumIf(g, b = 'c')), toString(sumIf(r, b = 'c')),
		       sumIf(n, b = 'p'), toString(sumIf(g, b = 'p')), toString(sumIf(r, b = 'p'))
		FROM (`+union(parts)+`)
		GROUP BY currency ORDER BY currency`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cur, prev = []CurrencyTotals{}, []CurrencyTotals{}
	for rows.Next() {
		var c, g, r, pg, pr string
		var n, pn uint64
		if err := rows.Scan(&c, &n, &g, &r, &pn, &pg, &pr); err != nil {
			return nil, nil, err
		}
		if n > 0 {
			cur = append(cur, CurrencyTotals{c, int64(n), decimal.RequireFromString(g).String(), decimal.RequireFromString(r).String()})
		}
		if pn > 0 {
			prev = append(prev, CurrencyTotals{c, int64(pn), decimal.RequireFromString(pg).String(), decimal.RequireFromString(pr).String()})
		}
	}
	return cur, prev, rows.Err()
}

// Trend returns one point per non-empty bucket. Orders count every currency;
// sales are restricted to p.Currency.
func Trend(ctx context.Context, conn driver.Conn, p Params) ([]TrendPoint, error) {
	q := newQB(p)
	loc := location(p.TZ)
	var plan domain.Plan
	var bucket func(string) string
	if p.Grain == "hour" {
		plan = domain.Decompose(p.From, p.To, loc, false)
		bucket = func(tier string) string {
			if tier == "hour" {
				return "formatDateTime(hour, '%Y-%m-%dT%H:%i:%SZ', 'UTC')"
			}
			return "formatDateTime(toStartOfHour(created_at), '%Y-%m-%dT%H:%i:%SZ', 'UTC')"
		}
	} else {
		plan = domain.Decompose(p.From, p.To, loc, true)
		tz := q.arg("String", p.TZ)
		bucket = func(tier string) string {
			switch tier {
			case "day":
				return "toString(day)"
			case "hour":
				return "toString(toDate(hour, " + tz + "))"
			}
			return "toString(toDate(created_at, " + tz + "))"
		}
	}
	c := q.arg("String", p.Currency)
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT b, sum(n), toString(sumIf(g, currency = `+c+`)), toString(sumIf(r, currency = `+c+`))
		FROM (`+union(orderParts(q, plan, bucket, ""))+`)
		GROUP BY b HAVING sum(n) > 0 ORDER BY b`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TrendPoint
	for rows.Next() {
		var tp TrendPoint
		var n uint64
		var g, r string
		if err := rows.Scan(&tp.Bucket, &n, &g, &r); err != nil {
			return nil, err
		}
		tp.Orders = int64(n)
		tp.GrossSales, tp.RecognizedSales = decimal.RequireFromString(g).String(), decimal.RequireFromString(r).String()
		out = append(out, tp)
	}
	return out, rows.Err()
}

type daySegment struct {
	domain.DayRange
	months bool // [From, To) are whole calendar months (both on the 1st)
}

// splitMonths splits a range of local days into leading days, whole calendar
// months and trailing days.
func splitMonths(d domain.DayRange) []daySegment {
	from, err1 := time.Parse(time.DateOnly, d.From)
	to, err2 := time.Parse(time.DateOnly, d.To)
	if err1 != nil || err2 != nil {
		return []daySegment{{DayRange: d}}
	}
	m1 := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
	if !m1.Equal(from) {
		m1 = m1.AddDate(0, 1, 0)
	}
	m2 := time.Date(to.Year(), to.Month(), 1, 0, 0, 0, 0, time.UTC)
	if !m1.Before(m2) {
		return []daySegment{{DayRange: d}}
	}
	f := func(t time.Time) string { return t.Format(time.DateOnly) }
	var out []daySegment
	if from.Before(m1) {
		out = append(out, daySegment{DayRange: domain.DayRange{From: d.From, To: f(m1)}})
	}
	out = append(out, daySegment{DayRange: domain.DayRange{From: f(m1), To: f(m2)}, months: true})
	if m2.Before(to) {
		out = append(out, daySegment{DayRange: domain.DayRange{From: f(m2), To: d.To}})
	}
	return out
}

// TopProducts ranks products by recognized line revenue: whole days from the
// product day rollup, partial days from raw items of recognized orders.
func TopProducts(ctx context.Context, conn driver.Conn, p Params) ([]TopProduct, error) {
	q := newQB(p)
	plan := domain.Decompose(p.From, p.To, location(p.TZ), true)
	c := q.arg("String", p.Currency)
	var parts []string
	for _, d := range plan.Days {
		for _, seg := range splitMonths(d) { // whole calendar months from the month cells, the rest from day cells
			table, col := "tp.product_day_rollup", "day"
			if seg.months {
				table, col = "tp.product_month_rollup", "month"
			}
			parts = append(parts, fmt.Sprintf(`SELECT product_id, quantity AS qty, revenue AS rv FROM %s FINAL
				WHERE %s AND currency = %s AND %s >= %s AND %s < %s`, table, tenantWhere, c, col, q.arg("Date", seg.From), col, q.arg("Date", seg.To)))
		}
	}
	edges := append(append([]domain.TimeRange{}, plan.Hours...), plan.Raw...)
	for _, r := range edges {
		from, to := q.arg("DateTime64(6, 'UTC')", ch.Time(r.From)), q.arg("DateTime64(6, 'UTC')", ch.Time(r.To))
		parts = append(parts, fmt.Sprintf(`SELECT product_id, toDecimal128(quantity, 6) AS qty, toDecimal128(quantity * unit_price, 12) AS rv
			FROM tp.order_items FINAL
			WHERE %s AND created_at >= %s AND is_deleted = 0
			  AND order_id IN (SELECT order_id FROM tp.orders FINAL WHERE %s AND created_at >= %s AND created_at < %s
			                   AND is_deleted = 0 AND currency = %s AND status IN %s)`,
			tenantWhere, from, tenantWhere, from, to, c, recognizedSQL))
	}
	if len(parts) == 0 {
		return []TopProduct{}, nil
	}
	limit := q.arg("UInt32", strconv.Itoa(p.Limit))
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT toString(product_id) AS pid, toString(sum(qty)), toString(sum(rv)), sum(rv) AS revenue
		FROM (`+strings.Join(parts, "\nUNION ALL\n")+`)
		GROUP BY pid HAVING sum(qty) != 0 OR sum(rv) != 0
		ORDER BY revenue DESC, pid LIMIT `+limit)
	if err != nil {
		return nil, err
	}
	var out []TopProduct
	var ids []string
	for rows.Next() {
		var t TopProduct
		var qty, rv string
		var rvd decimal.Decimal
		if err := rows.Scan(&t.ProductID, &qty, &rv, &rvd); err != nil {
			rows.Close()
			return nil, err
		}
		t.Rank = len(out) + 1
		t.Quantity, t.Revenue = decimal.RequireFromString(qty).String(), decimal.RequireFromString(rv).String()
		t.Name = "(unknown product)"
		out = append(out, t)
		ids = append(ids, "'"+t.ProductID+"'")
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, err
	}
	nq := newQB(p)
	nq.params["ids"] = "[" + strings.Join(ids, ",") + "]"
	names, err := conn.Query(p.qctx(ctx, nq), `
		SELECT toString(product_id), name, sku FROM tp.products FINAL
		WHERE `+tenantWhere+` AND product_id IN {ids:Array(UUID)}`)
	if err != nil {
		return nil, err
	}
	defer names.Close()
	byID := map[string]int{}
	for i, t := range out {
		byID[t.ProductID] = i
	}
	for names.Next() {
		var id, name string
		var sku *string
		if err := names.Scan(&id, &name, &sku); err != nil {
			return nil, err
		}
		if i, ok := byID[id]; ok {
			out[i].Name, out[i].SKU = name, sku
		}
	}
	return out, names.Err()
}
