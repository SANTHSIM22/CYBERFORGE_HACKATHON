package ingest

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"tenantpulse/internal/domain"
)

// Rollups form a hierarchy so that a refresh reads only what changed:
//
//	raw orders/items --(touched 15-minute buckets)--> order_q, product_q
//	order_q   --> order_hour (4 quarters) and order_day (the quarters of a tenant-local day)
//	product_q --> product_day (only products that changed) --> product_month (same products)
//
// Every IANA offset is a multiple of 15 minutes, so a tenant-local day is
// exactly a set of 15-minute UTC buckets. Raw rows are re-read only for the
// quarter-hours a batch touched (at most 15 minutes of one store's orders);
// everything above is summed from cells. Cost therefore follows the write
// rate, not the data size or the time of day. Cells are recomputed, never
// adjusted by deltas, so replays and restarts cannot double count; groups
// that disappear are rewritten as zeros.

// Cells records which 15-minute buckets a batch touched for one stream: the
// creation time of every affected order (for items: of the parent order).
// Full=true rebuilds the whole tenant generation.
type Cells struct {
	Quarters map[int64]struct{} // unix second of the UTC 15-minute bucket start
	Full     bool
}

const quarter = 15 * 60

func (c *Cells) addOrderTime(t time.Time, _ *time.Location) {
	if c.Quarters == nil {
		c.Quarters = map[int64]struct{}{}
	}
	u := t.Unix()
	c.Quarters[u-((u%quarter)+quarter)%quarter] = struct{}{}
}

func (c *Cells) merge(o Cells) {
	if o.Full {
		c.Full = true
	}
	for q := range o.Quarters {
		if c.Quarters == nil {
			c.Quarters = map[int64]struct{}{}
		}
		c.Quarters[q] = struct{}{}
	}
}

func (c *Cells) empty() bool { return !c.Full && len(c.Quarters) == 0 }

type streamCells struct {
	st    *Stream
	cells Cells
}

const (
	recognizedList = "('placed','paid','processing','shipped','delivered')"
)

func lit(t time.Time) string {
	return "toDateTime64('" + t.UTC().Format("2006-01-02 15:04:05.000000") + "', 6, 'UTC')"
}

func dt(unix int64) string { return "toDateTime(" + strconv.FormatInt(unix, 10) + ", 'UTC')" }

func tenantPred(st *Stream) string {
	return fmt.Sprintf("tenant_id = toUUID('%s') AND generation = %d", st.Tenant, st.Gen)
}

// plan holds one stream's touched time ranges at every level of the
// hierarchy. Touched quarter-hours are merged into a few contiguous ranges, so
// a batch of 100 stores yields a flat OR of ~100 simple range conditions
// (cheap to evaluate and to prune the primary key with), never per-quarter
// lists or per-store subqueries.
type plan struct {
	id, tp, tz string
	full       bool
	q          [][2]int64  // quarter ranges [lo, hi), unix seconds (raw rows, quarter cells)
	hours      [][2]int64  // hour ranges
	days       [][2]int64  // tenant-local day ranges as instants [first midnight, end midnight)
	dayNames   [][2]string // the same ranges as first and last local day
	months     [][2]string // [first month, month after the last) as YYYY-MM-DD
}

const clusterGap = 3600 // touched quarters closer than this are refreshed as one range

func planFor(st *Stream, c Cells) (plan, error) {
	p := plan{id: st.Tenant.String(), tp: tenantPred(st), tz: st.Loc.String(), full: c.Full}
	if c.Full {
		return p, nil
	}
	qs := sortedInts(c.Quarters)
	for _, q := range qs {
		if n := len(p.q); n > 0 && q <= p.q[n-1][1]+clusterGap {
			p.q[n-1][1] = max(p.q[n-1][1], q+quarter)
		} else {
			p.q = append(p.q, [2]int64{q, q + quarter})
		}
	}
	for _, r := range p.q {
		lo, hi := r[0]-r[0]%3600, r[1]
		if hi%3600 != 0 {
			hi += 3600 - hi%3600
		}
		p.hours = mergeRange(p.hours, lo, hi)

		first := domain.LocalDay(time.Unix(r[0], 0), st.Loc)
		last := domain.LocalDay(time.Unix(r[1]-1, 0), st.Loc)
		dlo, _, err1 := domain.LocalDayBounds(first, st.Loc)
		_, dhi, err2 := domain.LocalDayBounds(last, st.Loc)
		if err1 != nil || err2 != nil {
			return p, fmt.Errorf("bad local day %q..%q", first, last)
		}
		if n := len(p.days); n > 0 && dlo.Unix() <= p.days[n-1][1] {
			if dhi.Unix() > p.days[n-1][1] {
				p.days[n-1][1], p.dayNames[n-1][1] = dhi.Unix(), last
			}
		} else {
			p.days = append(p.days, [2]int64{dlo.Unix(), dhi.Unix()})
			p.dayNames = append(p.dayNames, [2]string{first, last})
		}
	}
	for _, d := range p.dayNames {
		m1 := d[0][:8] + "01"
		lt, _ := time.Parse(time.DateOnly, d[1][:8]+"01")
		m2 := lt.AddDate(0, 1, 0).Format(time.DateOnly)
		if n := len(p.months); n > 0 && m1 <= p.months[n-1][1] {
			if m2 > p.months[n-1][1] {
				p.months[n-1][1] = m2
			}
		} else {
			p.months = append(p.months, [2]string{m1, m2})
		}
	}
	return p, nil
}

func mergeRange(rs [][2]int64, lo, hi int64) [][2]int64 {
	if n := len(rs); n > 0 && lo <= rs[n-1][1] {
		rs[n-1][1] = max(rs[n-1][1], hi)
		return rs
	}
	return append(rs, [2]int64{lo, hi})
}

// ranges renders "tenant AND (col in any of the ranges)"; a full rebuild is the tenant alone.
func (p plan) ranges(col string, rs [][2]int64, val func(int64) string) string {
	if p.full {
		return p.tp
	}
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = fmt.Sprintf("(%s >= %s AND %s < %s)", col, val(r[0]), col, val(r[1]))
	}
	return p.tp + " AND (" + strings.Join(parts, " OR ") + ")"
}

func litUnix(u int64) string { return lit(time.Unix(u, 0)) }

func (p plan) rawOrders() string { return p.ranges("created_at", p.q, litUnix) }
func (p plan) qCells() string    { return p.ranges("q", p.q, dt) }
func (p plan) hourSrc() string   { return p.ranges("q", p.hours, dt) }
func (p plan) hourCells() string { return p.ranges("hour", p.hours, dt) }
func (p plan) daySrc() string    { return p.ranges("q", p.days, dt) }

// rawItems: items are never older than their order.
func (p plan) rawItems() string {
	if p.full {
		return p.tp
	}
	return p.tp + " AND created_at >= " + litUnix(p.q[0][0])
}

func (p plan) dayCells() string {
	if p.full {
		return p.tp
	}
	parts := make([]string, len(p.dayNames))
	for i, d := range p.dayNames {
		parts[i] = fmt.Sprintf("(day >= toDate('%s') AND day <= toDate('%s'))", d[0], d[1])
	}
	return p.tp + " AND (" + strings.Join(parts, " OR ") + ")"
}

func (p plan) monthPred(col string) string {
	if p.full {
		return p.tp
	}
	parts := make([]string, len(p.months))
	for i, m := range p.months {
		parts[i] = fmt.Sprintf("(%s >= toDate('%s') AND %s < toDate('%s'))", col, m[0], col, m[1])
	}
	return p.tp + " AND (" + strings.Join(parts, " OR ") + ")"
}

func sortedInts(m map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// RefreshRollups recomputes the touched cells of every stream: one INSERT per
// rollup table for the whole batch, bottom of the hierarchy first.
func RefreshRollups(ctx context.Context, conn driver.Conn, sets []streamCells, version uint64) error {
	var ps []plan
	for _, s := range sets {
		if s.cells.empty() {
			continue
		}
		p, err := planFor(s.st, s.cells)
		if err != nil {
			return err
		}
		ps = append(ps, p)
	}
	if len(ps) == 0 {
		return nil
	}
	qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_query_size": 16 << 20,
		"do_not_merge_across_partitions_select_final": 1,
	}))
	for _, s := range buildSteps(ps, strconv.FormatUint(version, 10)) {
		if err := conn.Exec(qctx, s.sql); err != nil {
			return fmt.Errorf("%s rollup: %w", s.name, err)
		}
	}
	return nil
}

type rollupStep struct{ name, sql string }

// buildSteps returns the refresh statements for these plans, bottom of the
// hierarchy first.
func buildSteps(ps []plan, v string) []rollupStep {
	or := func(f func(plan) string) string {
		out := make([]string, len(ps))
		for i, p := range ps {
			out[i] = "(" + f(p) + ")"
		}
		return strings.Join(out, " OR ")
	}
	// Product cells above the quarter level are rebuilt only for products that
	// changed: one shared (tenant, product) set, not a subquery per store.
	var fullIDs []string
	var partial []string
	for _, p := range ps {
		if p.full {
			fullIDs = append(fullIDs, "toUUID('"+p.id+"')")
		} else {
			partial = append(partial, "("+p.qCells()+")")
		}
	}
	var restrict []string
	if len(fullIDs) > 0 {
		restrict = append(restrict, "tenant_id IN ("+strings.Join(fullIDs, ",")+")")
	}
	if len(partial) > 0 {
		restrict = append(restrict, "(tenant_id, product_id) IN (SELECT DISTINCT tenant_id, product_id FROM tp.product_q_rollup FINAL WHERE "+strings.Join(partial, " OR ")+")")
	}
	products := " AND (" + strings.Join(restrict, " OR ") + ")"
	if len(partial) == 0 {
		products = ""
	}
	// Tenant-local day of a quarter: toDate needs a constant timezone, so the
	// source is one branch per timezone in the batch.
	byTZ := map[string][]plan{}
	var zones []string
	for _, p := range ps {
		if _, ok := byTZ[p.tz]; !ok {
			zones = append(zones, p.tz)
		}
		byTZ[p.tz] = append(byTZ[p.tz], p)
	}
	sort.Strings(zones)
	dayBranches := func(table, keys, sums, extra string) string {
		var bs []string
		for _, z := range zones {
			var preds []string
			for _, p := range byTZ[z] {
				preds = append(preds, "("+p.daySrc()+")")
			}
			bs = append(bs, `SELECT tenant_id, generation, toDate(q, '`+z+`') AS day, `+keys+`, `+sums+`
				FROM `+table+` FINAL WHERE (`+strings.Join(preds, " OR ")+`)`+extra+`
				GROUP BY tenant_id, generation, day, `+keys)
		}
		return strings.Join(bs, "\n\t\t\t\tUNION ALL\n\t\t\t\t")
	}
	rawOrders := or(plan.rawOrders)
	recognizedOrders := `SELECT order_id FROM tp.orders FINAL WHERE is_deleted = 0 AND status IN ` + recognizedList + ` AND (` + rawOrders + `)`

	return []rollupStep{
		{"order quarter", `
			INSERT INTO tp.order_q_rollup (tenant_id, generation, q, currency, status, orders, gross, recognized, _version)
			SELECT tenant_id, generation, q, currency, status, sum(n), sum(g), sum(r), ` + v + `
			FROM (
				SELECT tenant_id, generation, toDateTime(toStartOfFifteenMinutes(created_at), 'UTC') AS q, currency, status,
				       count() AS n, sum(total_amount) AS g, sumIf(total_amount, status IN ` + recognizedList + `) AS r
				FROM tp.orders FINAL WHERE is_deleted = 0 AND (` + rawOrders + `)
				GROUP BY tenant_id, generation, q, currency, status
				UNION ALL
				SELECT tenant_id, generation, q, currency, status, toUInt64(0), toDecimal128(0, 4), toDecimal128(0, 4)
				FROM tp.order_q_rollup FINAL WHERE ` + or(plan.qCells) + `
			)
			GROUP BY tenant_id, generation, q, currency, status`},
		// Items are deduplicated by their newest version (argMax) instead of
		// FINAL: FINAL would widen the read back to whole key ranges and undo
		// the order_id bloom-filter skipping. Every version of an item shares
		// its created_at and order_id, so none can be skipped.
		{"product quarter", `
			INSERT INTO tp.product_q_rollup (tenant_id, generation, q, currency, product_id, quantity, revenue, _version)
			SELECT tenant_id, generation, q, currency, product_id, sum(qty), sum(rv), ` + v + `
			FROM (
				SELECT o.tenant_id AS tenant_id, o.generation AS generation, o.q AS q, o.currency AS currency, i.product_id AS product_id,
				       sum(i.quantity) AS qty, sum(i.quantity * i.unit_price) AS rv
				FROM (
					SELECT tenant_id, generation, order_id, currency, toDateTime(toStartOfFifteenMinutes(created_at), 'UTC') AS q
					FROM tp.orders FINAL WHERE is_deleted = 0 AND status IN ` + recognizedList + ` AND (` + rawOrders + `)
				) AS o
				INNER JOIN (
					SELECT tenant_id, generation, oid AS order_id, pid AS product_id, qn AS quantity, price AS unit_price FROM (
						SELECT tenant_id, generation, item_id, any(order_id) AS oid,
						       argMax(product_id, _version) AS pid, argMax(quantity, _version) AS qn,
						       argMax(unit_price, _version) AS price, argMax(is_deleted, _version) AS deleted
						FROM tp.order_items
						WHERE (` + or(plan.rawItems) + `) AND order_id IN (` + recognizedOrders + `)
						GROUP BY tenant_id, generation, item_id
					) WHERE deleted = 0
				) AS i ON i.tenant_id = o.tenant_id AND i.generation = o.generation AND i.order_id = o.order_id
				GROUP BY tenant_id, generation, q, currency, product_id
				UNION ALL
				SELECT tenant_id, generation, q, currency, product_id, toDecimal128(0, 6), toDecimal128(0, 12)
				FROM tp.product_q_rollup FINAL WHERE ` + or(plan.qCells) + `
			)
			GROUP BY tenant_id, generation, q, currency, product_id`},
		{"order hour", `
			INSERT INTO tp.order_hour_rollup (tenant_id, generation, hour, currency, status, orders, gross, recognized, _version)
			SELECT tenant_id, generation, hour, currency, status, sum(n), sum(g), sum(r), ` + v + `
			FROM (
				SELECT tenant_id, generation, toStartOfHour(q) AS hour, currency, status, sum(orders) AS n, sum(gross) AS g, sum(recognized) AS r
				FROM tp.order_q_rollup FINAL WHERE ` + or(plan.hourSrc) + `
				GROUP BY tenant_id, generation, hour, currency, status
				UNION ALL
				SELECT tenant_id, generation, hour, currency, status, toUInt64(0), toDecimal128(0, 4), toDecimal128(0, 4)
				FROM tp.order_hour_rollup FINAL WHERE ` + or(plan.hourCells) + `
			)
			GROUP BY tenant_id, generation, hour, currency, status`},
		{"order day", `
			INSERT INTO tp.order_day_rollup (tenant_id, generation, day, currency, status, orders, gross, recognized, _version)
			SELECT tenant_id, generation, day, currency, status, sum(n), sum(g), sum(r), ` + v + `
			FROM (
				` + dayBranches("tp.order_q_rollup", "currency, status", "sum(orders) AS n, sum(gross) AS g, sum(recognized) AS r", "") + `
				UNION ALL
				SELECT tenant_id, generation, day, currency, status, toUInt64(0), toDecimal128(0, 4), toDecimal128(0, 4)
				FROM tp.order_day_rollup FINAL WHERE ` + or(plan.dayCells) + `
			)
			GROUP BY tenant_id, generation, day, currency, status`},
		{"product day", `
			INSERT INTO tp.product_day_rollup (tenant_id, generation, day, currency, product_id, quantity, revenue, _version)
			SELECT tenant_id, generation, day, currency, product_id, sum(qty), sum(rv), ` + v + `
			FROM (
				` + dayBranches("tp.product_q_rollup", "currency, product_id", "sum(quantity) AS qty, sum(revenue) AS rv", products) + `
				UNION ALL
				SELECT tenant_id, generation, day, currency, product_id, toDecimal128(0, 6), toDecimal128(0, 12)
				FROM tp.product_day_rollup FINAL WHERE (` + or(plan.dayCells) + `)` + products + `
			)
			GROUP BY tenant_id, generation, day, currency, product_id`},
		{"product month", `
			INSERT INTO tp.product_month_rollup (tenant_id, generation, month, currency, product_id, quantity, revenue, _version)
			SELECT tenant_id, generation, month, currency, product_id, sum(qty), sum(rv), ` + v + `
			FROM (
				SELECT tenant_id, generation, toStartOfMonth(day) AS month, currency, product_id, sum(quantity) AS qty, sum(revenue) AS rv
				FROM tp.product_day_rollup FINAL WHERE (` + or(func(p plan) string { return p.monthPred("day") }) + `)` + products + `
				GROUP BY tenant_id, generation, month, currency, product_id
				UNION ALL
				SELECT tenant_id, generation, month, currency, product_id, toDecimal128(0, 6), toDecimal128(0, 12)
				FROM tp.product_month_rollup FINAL WHERE (` + or(func(p plan) string { return p.monthPred("month") }) + `)` + products + `
			)
			GROUP BY tenant_id, generation, month, currency, product_id`},
	}
}
