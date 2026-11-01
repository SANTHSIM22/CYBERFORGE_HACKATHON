// Package chq answers the dashboard questions from ClickHouse. Every query
// runs as tp_api under row policies (SQL_tenant_id) AND filters tenant_id and
// generation explicitly (primary-key pruning + defence in depth). FINAL gives
// the latest version of every row, so status changes and deletes are exact.
package chq

import (
	"context"

	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"

	"tenantpulse/internal/ch"
	"tenantpulse/internal/domain"
)

type Params struct {
	TenantID   string
	Generation int64
	TZ         string
	From, To   time.Time
	Currency   string // optional for status rows; required for trends/top
	Grain      string // hour | day
	Limit      int    // top products: page size
	Offset     int    // top products: products ranked before this page
	Timeout    time.Duration
}

type StatusRow struct {
	Currency   string
	Status     string
	Orders     int64
	Gross      decimal.Decimal
	Recognized decimal.Decimal
}

type CurrencyTotals struct {
	Currency        string `json:"currency"`
	Orders          int64  `json:"orders"`
	GrossSales      string `json:"gross_sales"`
	RecognizedSales string `json:"recognized_sales"`
}

type TrendPoint struct {
	Bucket          string `json:"bucket"`
	Orders          int64  `json:"orders"`
	GrossSales      string `json:"gross_sales"`
	RecognizedSales string `json:"recognized_sales"`
}

type TopProduct struct {
	Rank      int     `json:"rank"`
	ProductID string  `json:"product_id"`
	Name      string  `json:"name"`
	SKU       *string `json:"sku"`
	Quantity  string  `json:"quantity"`
	Revenue   string  `json:"revenue"`
}

var recognizedSQL = "('" + strings.Join(domain.RecognizedStatuses, "','") + "')"

func (p Params) ctx(ctx context.Context, extra clickhouse.Parameters) context.Context {
	params := clickhouse.Parameters{
		"t":    p.TenantID,
		"g":    strconv.FormatInt(p.Generation, 10),
		"from": ch.Time(p.From),
		"to":   ch.Time(p.To),
	}
	for k, v := range extra {
		params[k] = v
	}
	timeout := p.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	return ch.TenantCtx(ctx, p.TenantID, timeout, params)
}

func Totals(rows []StatusRow) []CurrencyTotals {
	idx := map[string]int{}
	out := []CurrencyTotals{}
	sums := map[string]*[2]decimal.Decimal{}
	for _, r := range rows {
		i, ok := idx[r.Currency]
		if !ok {
			i = len(out)
			idx[r.Currency] = i
			out = append(out, CurrencyTotals{Currency: r.Currency})
			sums[r.Currency] = &[2]decimal.Decimal{}
		}
		out[i].Orders += r.Orders
		s := sums[r.Currency]
		s[0], s[1] = s[0].Add(r.Gross), s[1].Add(r.Recognized)
	}
	for i := range out {
		s := sums[out[i].Currency]
		out[i].GrossSales, out[i].RecognizedSales = s[0].String(), s[1].String()
	}
	return out
}

// Currencies lists the currencies a tenant has sold in (immutable per order,
// so no FINAL is needed).
func Currencies(ctx context.Context, conn driver.Conn, tenantID string, gen int64) ([]string, error) {
	p := Params{TenantID: tenantID, Generation: gen, From: time.Unix(0, 0), To: time.Now().Add(24 * time.Hour)}
	rows, err := conn.Query(p.ctx(ctx, nil), `SELECT DISTINCT currency FROM tp.orders WHERE tenant_id = {t:UUID} AND generation = {g:UInt32} ORDER BY currency`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Status is the freshness row for one tenant generation.
type Status struct {
	Found                 bool
	SnapshotState         string
	LastSourceCommitAt    *time.Time
	LastHeartbeatSourceAt *time.Time
	LastAppliedAt         *time.Time
	StreamCheckedAt       *time.Time
	LastApplyLagMs        *int64
	SlotLagBytes          uint64
	EventsTotal           uint64
	DataVersion           uint64
}

func TenantStatus(ctx context.Context, conn driver.Conn, tenantID string, gen int64) (Status, error) {
	p := Params{TenantID: tenantID, Generation: gen, From: time.Unix(0, 0), To: time.Unix(0, 0)}
	rows, err := conn.Query(p.ctx(ctx, nil), `
		SELECT snapshot_state, last_source_commit_at, last_heartbeat_source_at, last_applied_at, stream_checked_at,
		       last_apply_lag_ms, slot_lag_bytes, events_total, data_version
		FROM tp.tenant_status FINAL WHERE tenant_id = {t:UUID} AND generation = {g:UInt32}`)
	if err != nil {
		return Status{}, err
	}
	defer rows.Close()
	var s Status
	for rows.Next() {
		s.Found = true
		if err := rows.Scan(&s.SnapshotState, &s.LastSourceCommitAt, &s.LastHeartbeatSourceAt, &s.LastAppliedAt, &s.StreamCheckedAt,
			&s.LastApplyLagMs, &s.SlotLagBytes, &s.EventsTotal, &s.DataVersion); err != nil {
			return s, err
		}
	}
	return s, rows.Err()
}

// DataAsOf: the source state is fully reflected up to the newest of the last
// applied commit, the last applied watermark and the last caught-up check.
func (s Status) DataAsOf() *time.Time {
	var best *time.Time
	for _, t := range []*time.Time{s.LastSourceCommitAt, s.LastHeartbeatSourceAt, s.StreamCheckedAt} {
		if t != nil && (best == nil || t.After(*best)) {
			best = t
		}
	}
	return best
}
