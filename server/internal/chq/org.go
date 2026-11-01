package chq

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"
)

// Org dashboards add up their stores' 15-minute cells. Every IANA offset is a
// multiple of 15 minutes, so any viewer-local day is a whole set of these UTC
// buckets: org totals follow the viewer's clock exactly, whatever timezone
// each store is in. Each store is read on its own, under its own tenant
// setting (the row policy), exactly as its own dashboard is; the API adds the
// slices up.

// OrgCell is one store's orders for one bucket, currency and status.
type OrgCell struct {
	Bucket, Currency, Status string
	Orders                   int64
	Gross, Recognized        decimal.Decimal
}

// OrgOrders reads one store's order cells in [p.From, p.To), both on
// 15-minute boundaries. bucket: "" (all in one), "period" ("c" from split on,
// "p" before it), "day" (local date in p.TZ) or "hour" (UTC hour).
func OrgOrders(ctx context.Context, conn driver.Conn, p Params, bucket string, split time.Time) ([]OrgCell, error) {
	q := newQB(p)
	b := "''"
	switch bucket {
	case "period":
		b = "if(q >= " + q.arg("DateTime('UTC')", hourLit(split)) + ", 'c', 'p')"
	case "day":
		b = "toString(toDate(q, " + q.arg("String", p.TZ) + "))"
	case "hour":
		b = "formatDateTime(toStartOfHour(q), '%Y-%m-%dT%H:%i:%SZ', 'UTC')"
	}
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT `+b+` AS b, currency, status, sum(orders), toString(sum(gross)), toString(sum(recognized))
		FROM tp.order_q_rollup FINAL
		WHERE `+tenantWhere+` AND q >= `+q.arg("DateTime('UTC')", hourLit(p.From))+` AND q < `+q.arg("DateTime('UTC')", hourLit(p.To))+`
		GROUP BY b, currency, status HAVING sum(orders) > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OrgCell
	for rows.Next() {
		var c OrgCell
		var n uint64
		var g, r string
		if err := rows.Scan(&c.Bucket, &c.Currency, &c.Status, &n, &g, &r); err != nil {
			return nil, err
		}
		c.Orders, c.Gross, c.Recognized = int64(n), decimal.RequireFromString(g), decimal.RequireFromString(r)
		out = append(out, c)
	}
	return out, rows.Err()
}

// OrgProduct is one store's recognized sales of one product in a range.
type OrgProduct struct {
	ProductID string
	Name      string
	SKU       *string
	Quantity  decimal.Decimal
	Revenue   map[string]decimal.Decimal // per currency, exact
}

// OrgTopProducts returns one store's k best-selling products ranked by
// revenue converted into the org's reporting currency (factor per currency;
// a currency without one counts as 0), and how many of its products sold.
// The org's top N is exact from each store's top N: a product among the org's
// first N has fewer than N products above it in its own store.
func OrgTopProducts(ctx context.Context, conn driver.Conn, p Params, factors map[string]float64, k int) ([]OrgProduct, int, error) {
	q := newQB(p)
	curs, fs := make([]string, 0, len(factors)), make([]string, 0, len(factors))
	for c, f := range factors {
		curs = append(curs, "'"+c+"'")
		fs = append(fs, strconv.FormatFloat(f, 'g', -1, 64))
	}
	q.params["curs"], q.params["fs"] = "["+strings.Join(curs, ",")+"]", "["+strings.Join(fs, ",")+"]"
	rows, err := conn.Query(p.qctx(ctx, q), `
		SELECT toString(pid), toString(sum(qty)), groupArray(cur), groupArray(toString(rv)),
		       sum(toFloat64(rv) * transform(cur, {curs:Array(String)}, {fs:Array(Float64)}, toFloat64(0))) AS conv,
		       count() OVER () AS total
		FROM (
			SELECT product_id AS pid, currency AS cur, sum(quantity) AS qty, sum(revenue) AS rv
			FROM tp.product_q_rollup FINAL
			WHERE `+tenantWhere+` AND q >= `+q.arg("DateTime('UTC')", hourLit(p.From))+` AND q < `+q.arg("DateTime('UTC')", hourLit(p.To))+`
			GROUP BY pid, cur
		)
		GROUP BY pid HAVING sum(qty) != 0 OR conv != 0
		ORDER BY conv DESC, pid LIMIT `+q.arg("UInt32", strconv.Itoa(k)))
	if err != nil {
		return nil, 0, err
	}
	var out []OrgProduct
	var total uint64
	for rows.Next() {
		var op OrgProduct
		var qty string
		var cs, rvs []string
		var conv float64
		if err := rows.Scan(&op.ProductID, &qty, &cs, &rvs, &conv, &total); err != nil {
			rows.Close()
			return nil, 0, err
		}
		op.Quantity, op.Revenue = decimal.RequireFromString(qty), map[string]decimal.Decimal{}
		for i, c := range cs {
			op.Revenue[c] = op.Revenue[c].Add(decimal.RequireFromString(rvs[i]))
		}
		out = append(out, op)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return out, int(total), nil
}

// ProductNames fills in name and SKU for one store's products.
func ProductNames(ctx context.Context, conn driver.Conn, p Params, ps []OrgProduct) error {
	if len(ps) == 0 {
		return nil
	}
	q := newQB(p)
	ids := make([]string, len(ps))
	byID := make(map[string]int, len(ps))
	for i, op := range ps {
		ids[i], byID[op.ProductID] = "'"+op.ProductID+"'", i
	}
	q.params["ids"] = "[" + strings.Join(ids, ",") + "]"
	rows, err := conn.Query(p.qctx(ctx, q), `SELECT toString(product_id), name, sku FROM tp.products FINAL
		WHERE `+tenantWhere+` AND product_id IN {ids:Array(UUID)}`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, name string
		var sku *string
		if err := rows.Scan(&id, &name, &sku); err != nil {
			return err
		}
		if i, ok := byID[id]; ok {
			ps[i].Name, ps[i].SKU = name, sku
		}
	}
	return rows.Err()
}
