// Package directsql answers the same dashboard questions straight from a
// tenant's source database — the "slow way". It exists for the demo-only
// comparison and for the reconciliation tool; dashboard endpoints never use it.
// The SQL is intentionally the plain query a reviewer would write by hand.
package directsql

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/domain"
)

const StatusRowsSQL = `SELECT currency::text, status, count(*),
       sum(total_amount)::text,
       COALESCE(sum(total_amount) FILTER (WHERE status = ANY($3::text[])), 0)::text
FROM orders
WHERE deleted_at IS NULL AND created_at >= $1 AND created_at < $2
  AND ($4::text = '' OR currency = $4)
GROUP BY 1, 2
ORDER BY 1, 2`

const TrendDaySQL = `SELECT (created_at AT TIME ZONE $3)::date::text AS day, count(*),
       COALESCE(sum(total_amount) FILTER (WHERE currency = $4), 0)::text,
       COALESCE(sum(total_amount) FILTER (WHERE currency = $4 AND status = ANY($5::text[])), 0)::text
FROM orders
WHERE deleted_at IS NULL AND created_at >= $1 AND created_at < $2
GROUP BY 1
ORDER BY 1`

const TrendHourSQL = `SELECT to_char(date_trunc('hour', created_at, 'UTC') AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS hour, count(*),
       COALESCE(sum(total_amount) FILTER (WHERE currency = $4), 0)::text,
       COALESCE(sum(total_amount) FILTER (WHERE currency = $4 AND status = ANY($5::text[])), 0)::text
FROM orders
WHERE deleted_at IS NULL AND created_at >= $1 AND created_at < $2 AND $3::text IS NOT NULL
GROUP BY 1
ORDER BY 1`

const TopProductsSQL = `SELECT i.product_id::text, p.name, p.sku,
       sum(i.quantity)::text AS quantity,
       sum(i.quantity * i.unit_price) AS revenue
FROM orders o
JOIN order_items i ON i.order_id = o.id AND i.deleted_at IS NULL
JOIN products p ON p.id = i.product_id
WHERE o.deleted_at IS NULL AND o.currency = $3 AND o.status = ANY($4::text[])
  AND o.created_at >= $1 AND o.created_at < $2
GROUP BY i.product_id, p.name, p.sku
ORDER BY revenue DESC, i.product_id
LIMIT $5`

const SummarySQL = `SELECT currency::text,
       count(*) FILTER (WHERE created_at >= $2),
       COALESCE(sum(total_amount) FILTER (WHERE created_at >= $2), 0)::text,
       COALESCE(sum(total_amount) FILTER (WHERE created_at >= $2 AND status = ANY($4::text[])), 0)::text,
       count(*) FILTER (WHERE created_at < $2),
       COALESCE(sum(total_amount) FILTER (WHERE created_at < $2), 0)::text,
       COALESCE(sum(total_amount) FILTER (WHERE created_at < $2 AND status = ANY($4::text[])), 0)::text
FROM orders
WHERE deleted_at IS NULL AND created_at >= $1 AND created_at < $3
GROUP BY 1
ORDER BY 1`

// Summary returns per-currency totals for [from, to) and [prevFrom, from) in one pass.
func Summary(ctx context.Context, q Querier, prevFrom, from, to time.Time) (cur, prev []chq.CurrencyTotals, err error) {
	rows, err := q.Query(ctx, SummarySQL, prevFrom, from, to, domain.RecognizedStatuses)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cur, prev = []chq.CurrencyTotals{}, []chq.CurrencyTotals{}
	for rows.Next() {
		var c, g, r, pg, pr string
		var n, pn int64
		if err := rows.Scan(&c, &n, &g, &r, &pn, &pg, &pr); err != nil {
			return nil, nil, err
		}
		c = strings.TrimSpace(c)
		if n > 0 {
			cur = append(cur, chq.CurrencyTotals{Currency: c, Orders: n, GrossSales: decimal.RequireFromString(g).String(), RecognizedSales: decimal.RequireFromString(r).String()})
		}
		if pn > 0 {
			prev = append(prev, chq.CurrencyTotals{Currency: c, Orders: pn, GrossSales: decimal.RequireFromString(pg).String(), RecognizedSales: decimal.RequireFromString(pr).String()})
		}
	}
	return cur, prev, rows.Err()
}

// Querier is satisfied by pgx.Tx, *pgx.Conn and *pgxpool.Pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func StatusRows(ctx context.Context, q Querier, from, to time.Time, currency string) ([]chq.StatusRow, error) {
	rows, err := q.Query(ctx, StatusRowsSQL, from, to, domain.RecognizedStatuses, currency)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []chq.StatusRow
	for rows.Next() {
		var r chq.StatusRow
		var g, rec string
		if err := rows.Scan(&r.Currency, &r.Status, &r.Orders, &g, &rec); err != nil {
			return nil, err
		}
		r.Currency = strings.TrimSpace(r.Currency)
		r.Gross, r.Recognized = decimal.RequireFromString(g), decimal.RequireFromString(rec)
		out = append(out, r)
	}
	return out, rows.Err()
}

func Trend(ctx context.Context, q Querier, from, to time.Time, tz, grain, currency string) ([]chq.TrendPoint, error) {
	sql := TrendDaySQL
	if grain == "hour" {
		sql = TrendHourSQL
	}
	rows, err := q.Query(ctx, sql, from, to, tz, currency, domain.RecognizedStatuses)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []chq.TrendPoint
	for rows.Next() {
		var tp chq.TrendPoint
		var g, r string
		if err := rows.Scan(&tp.Bucket, &tp.Orders, &g, &r); err != nil {
			return nil, err
		}
		tp.GrossSales, tp.RecognizedSales = decimal.RequireFromString(g).String(), decimal.RequireFromString(r).String()
		out = append(out, tp)
	}
	return out, rows.Err()
}

func TopProducts(ctx context.Context, q Querier, from, to time.Time, currency string, limit int) ([]chq.TopProduct, error) {
	rows, err := q.Query(ctx, TopProductsSQL, from, to, currency, domain.RecognizedStatuses, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []chq.TopProduct
	for rows.Next() {
		var tp chq.TopProduct
		var qty string
		var rev decimal.Decimal
		var revText string
		if err := rows.Scan(&tp.ProductID, &tp.Name, &tp.SKU, &qty, &revText); err != nil {
			return nil, err
		}
		rev = decimal.RequireFromString(revText)
		tp.Rank = len(out) + 1
		tp.Quantity, tp.Revenue = decimal.RequireFromString(qty).String(), rev.String()
		out = append(out, tp)
	}
	return out, rows.Err()
}
