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

// dayPred renders "(col in any of the day ranges)", or "" for none.
func (q *qb) dayPred(col string, rs []domain.DayRange) string {
	parts := make([]string, len(rs))
	for i, d := range rs {
		parts[i] = fmt.Sprintf("(%s >= %s AND %s < %s)", col, q.arg("Date", d.From), col, q.arg("Date", d.To))
	}
	return anyOf(parts)
}

// timePred renders "(col in any of the time ranges)", or "" for none.
func (q *qb) timePred(col, typ string, rs []domain.TimeRange, lit func(time.Time) string) string {
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = fmt.Sprintf("(%s >= %s AND %s < %s)", col, q.arg(typ, lit(r.From)), col, q.arg(typ, lit(r.To)))
	}
	return anyOf(parts)
}

func hourLit(t time.Time) string { return t.UTC().Format(time.DateTime) }

// anyOf ORs the non-empty conditions; "" when there are none.
func anyOf(conds []string) string {
	var out []string
	for _, c := range conds {
		if c != "" {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return ""
	}
	return "(" + strings.Join(out, " OR ") + ")"
}

// orderParts returns UNION ALL branches yielding (b, currency, status, n, g, r):
// one per tier (day rollup, hour rollup, raw orders) covering the ranges of
// every plan. A FINAL read costs ~10 ms however few rows it reads, so a tier's
// ranges are OR-ed into one read rather than one read per range (an exact-time
// summary used to be 14 reads). bucket(tier, in) gives the bucket expression
// per tier; in[i] is plan i's condition on that tier ("" if it has none there).
func orderParts(q *qb, plans []domain.Plan, bucket func(tier string, in []string) string, curFilter string) []string {
	days, hours, raw := make([]string, len(plans)), make([]string, len(plans)), make([]string, len(plans))
	for i, pl := range plans {
		days[i] = q.dayPred("day", pl.Days)
		hours[i] = q.timePred("hour", "DateTime('UTC')", pl.Hours, hourLit)
		raw[i] = q.timePred("created_at", "DateTime64(6, 'UTC')", pl.Raw, ch.Time)
	}
	var parts []string
	add := func(tier string, in []string, cols, from string) {
		if w := anyOf(in); w != "" {
			parts = append(parts, fmt.Sprintf("SELECT %s AS b, %s\n\t\t\tFROM %s AND %s%s", bucket(tier, in), cols, from, w, curFilter))
		}
	}
	rollupCols := "currency, status, orders AS n, gross AS g, recognized AS r"
	add("day", days, rollupCols, "tp.order_day_rollup FINAL WHERE "+tenantWhere)
	add("hour", hours, rollupCols, "tp.order_hour_rollup FINAL WHERE "+tenantWhere)
	add("raw", raw, `currency, status, toUInt64(1) AS n, toDecimal128(total_amount, 4) AS g,
			if(status IN `+recognizedSQL+`, toDecimal128(total_amount, 4), toDecimal128(0, 4)) AS r`,
		"tp.orders FINAL WHERE "+tenantWhere+" AND is_deleted = 0")
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
		FROM (`+union(orderParts(q, []domain.Plan{plan}, func(string, []string) string { return "''" }, cur))+`)
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
	// Both periods share one read per tier; a row belongs to the current period
	// exactly when it matches the current plan's ranges (the periods never overlap).
	plans := []domain.Plan{domain.Decompose(p.From, p.To, loc, true), domain.Decompose(prevFrom, p.From, loc, true)}
	parts := orderParts(q, plans, func(_ string, in []string) string {
		if in[0] == "" {
			return "'p'"
		}
		return "if(" + in[0] + ", 'c', 'p')"
	}, "")
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
	var bucket func(string, []string) string
	if p.Grain == "hour" {
		plan = domain.Decompose(p.From, p.To, loc, false)
		bucket = func(tier string, _ []string) string {
			if tier == "hour" {
				return "formatDateTime(hour, '%Y-%m-%dT%H:%i:%SZ', 'UTC')"
			}
			return "formatDateTime(toStartOfHour(created_at), '%Y-%m-%dT%H:%i:%SZ', 'UTC')"
		}
	} else {
		plan = domain.Decompose(p.From, p.To, loc, true)
		tz := q.arg("String", p.TZ)
		bucket = func(tier string, _ []string) string {
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
		FROM (`+union(orderParts(q, []domain.Plan{plan}, bucket, ""))+`)
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
// product day rollup, partial days from raw items of recognized orders. It
// returns one page (ranks p.Offset+1 .. p.Offset+p.Limit) and how many
// products sold in the range.
func TopProducts(ctx context.Context, conn driver.Conn, p Params) ([]TopProduct, int, error) {
	q := newQB(p)
	plan := domain.Decompose(p.From, p.To, location(p.TZ), true)
	c := q.arg("String", p.Currency)
	var parts []string
	// Whole calendar months from the month cells, the rest from day cells: one
	// read per table, however many segments.
	var dayRs, monthRs []domain.DayRange
	for _, d := range plan.Days {
		for _, seg := range splitMonths(d) {
			if seg.months {
				monthRs = append(monthRs, seg.DayRange)
			} else {
				dayRs = append(dayRs, seg.DayRange)
			}
		}
	}
	if w := q.dayPred("day", dayRs); w != "" {
		parts = append(parts, `SELECT product_id, quantity AS qty, revenue AS rv FROM tp.product_day_rollup FINAL
			WHERE `+tenantWhere+` AND currency = `+c+` AND `+w)
	}
	if w := q.dayPred("month", monthRs); w != "" {
		parts = append(parts, `SELECT product_id, quantity AS qty, revenue AS rv FROM tp.product_month_rollup FINAL
			WHERE `+tenantWhere+` AND currency = `+c+` AND `+w)
	}
	// Partial days (whole hours and partial-hour edges) from raw lines of the
	// recognized orders created there, in one read for all edges. Lines are
	// deduplicated by their newest version (argMax) instead of FINAL: FINAL
	// widens the read to whole key ranges and undoes the order_id bloom-filter
	// skipping, and lines can only be bounded below (never older than their
	// order), so each edge used to re-read every line from its start until now.
	// Every version of a line shares its order_id and created_at.
	edges := append(append([]domain.TimeRange{}, plan.Hours...), plan.Raw...)
	if w := q.timePred("created_at", "DateTime64(6, 'UTC')", edges, ch.Time); w != "" {
		first := edges[0].From
		for _, r := range edges {
			if r.From.Before(first) {
				first = r.From
			}
		}
		parts = append(parts, `SELECT pid AS product_id, toDecimal128(qn, 6) AS qty, toDecimal128(qn * price, 12) AS rv FROM (
			SELECT item_id, argMax(product_id, _version) AS pid, argMax(quantity, _version) AS qn,
			       argMax(unit_price, _version) AS price, argMax(is_deleted, _version) AS deleted
			FROM tp.order_items
			WHERE `+tenantWhere+` AND created_at >= `+q.arg("DateTime64(6, 'UTC')", ch.Time(first))+`
			  AND order_id IN (SELECT order_id FROM tp.orders FINAL WHERE `+tenantWhere+` AND `+w+`
			                   AND is_deleted = 0 AND currency = `+c+` AND status IN `+recognizedSQL+`)
			GROUP BY item_id
		) WHERE deleted = 0`)
	}
	if len(parts) == 0 {
		return []TopProduct{}, 0, nil
	}
	limit, offset := q.arg("UInt32", strconv.Itoa(p.Limit)), q.arg("UInt32", strconv.Itoa(p.Offset))
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT toString(product_id) AS pid, toString(sum(qty)), toString(sum(rv)), sum(rv) AS revenue, count() OVER () AS total
		FROM (`+strings.Join(parts, "\nUNION ALL\n")+`)
		GROUP BY pid HAVING sum(qty) != 0 OR sum(rv) != 0
		ORDER BY revenue DESC, pid LIMIT `+limit+` OFFSET `+offset)
	if err != nil {
		return nil, 0, err
	}
	var out []TopProduct
	var ids []string
	var total uint64
	for rows.Next() {
		var t TopProduct
		var qty, rv string
		var rvd decimal.Decimal
		if err := rows.Scan(&t.ProductID, &qty, &rv, &rvd, &total); err != nil {
			rows.Close()
			return nil, 0, err
		}
		t.Rank = p.Offset + len(out) + 1
		t.Quantity, t.Revenue = decimal.RequireFromString(qty).String(), decimal.RequireFromString(rv).String()
		t.Name = "(unknown product)"
		out = append(out, t)
		ids = append(ids, "'"+t.ProductID+"'")
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, int(total), err
	}
	nq := newQB(p)
	nq.params["ids"] = "[" + strings.Join(ids, ",") + "]"
	names, err := conn.Query(p.qctx(ctx, nq), `
		SELECT toString(product_id), name, sku FROM tp.products FINAL
		WHERE `+tenantWhere+` AND product_id IN {ids:Array(UUID)}`)
	if err != nil {
		return nil, 0, err
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
			return nil, 0, err
		}
		if i, ok := byID[id]; ok {
			out[i].Name, out[i].SKU = name, sku
		}
	}
	return out, int(total), names.Err()
}
