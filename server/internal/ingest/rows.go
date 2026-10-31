// Package ingest streams tenant source changes into ClickHouse: a consistent
// snapshot from an exported replication-slot snapshot, then pgoutput logical
// replication, batched across tenants, acknowledged only after ClickHouse has
// the rows (at-least-once; replays are idempotent in ReplacingMergeTree).
package ingest

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/shopspring/decimal"
)

type OrderRow struct {
	Tenant     uuid.UUID
	Gen        uint32
	ID         uuid.UUID
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Status     string
	Currency   string
	Total      decimal.Decimal
	SrcVersion uint64
	Deleted    uint8
	Version    uint64
}

type ItemRow struct {
	Tenant     uuid.UUID
	Gen        uint32
	ID         uuid.UUID
	OrderID    uuid.UUID
	ProductID  uuid.UUID
	CreatedAt  time.Time
	Quantity   decimal.Decimal
	UnitPrice  decimal.Decimal
	SrcVersion uint64
	Deleted    uint8
	Version    uint64
}

type ProductRow struct {
	Tenant     uuid.UUID
	Gen        uint32
	ID         uuid.UUID
	SKU        *string
	Name       string
	IsActive   uint8
	SrcVersion uint64
	Deleted    uint8
	Version    uint64
}

// tuple maps a pgoutput tuple to column name -> text value (nil = NULL).
type tuple map[string]*string

func decodeTuple(rel *pglogrepl.RelationMessage, td *pglogrepl.TupleData) (tuple, error) {
	if td == nil {
		return nil, fmt.Errorf("missing tuple")
	}
	t := make(tuple, len(td.Columns))
	for i, col := range td.Columns {
		if i >= len(rel.Columns) {
			break
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			t[name] = nil
		case pglogrepl.TupleDataTypeText:
			s := string(col.Data)
			t[name] = &s
		case pglogrepl.TupleDataTypeToast:
			// unchanged TOAST value: not present for the columns we read
		default:
			return nil, fmt.Errorf("unsupported tuple data type %q", col.DataType)
		}
	}
	return t, nil
}

func (t tuple) str(k string) (string, error) {
	v, ok := t[k]
	if !ok || v == nil {
		return "", fmt.Errorf("column %s missing", k)
	}
	return *v, nil
}

func (t tuple) uuid(k string) (uuid.UUID, error) {
	s, err := t.str(k)
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(s)
}

func (t tuple) dec(k string) (decimal.Decimal, error) {
	s, err := t.str(k)
	if err != nil {
		return decimal.Zero, err
	}
	return decimal.NewFromString(s)
}

func (t tuple) u64(k string) (uint64, error) {
	s, err := t.str(k)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(s, 10, 64)
}

func (t tuple) ts(k string) (time.Time, error) {
	s, err := t.str(k)
	if err != nil {
		return time.Time{}, err
	}
	return parseTimestamptz(s)
}

func (t tuple) isNull(k string) bool { v, ok := t[k]; return !ok || v == nil }

// parseTimestamptz parses Postgres text output (replication session TimeZone=UTC).
func parseTimestamptz(s string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999-07", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999-07:00:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamptz %q", s)
}

func orderFromTuple(t tuple) (OrderRow, error) {
	var r OrderRow
	var err error
	if r.ID, err = t.uuid("id"); err != nil {
		return r, err
	}
	if r.CreatedAt, err = t.ts("created_at"); err != nil {
		return r, err
	}
	if r.UpdatedAt, err = t.ts("updated_at"); err != nil {
		r.UpdatedAt = r.CreatedAt
	}
	if r.Status, err = t.str("status"); err != nil {
		return r, err
	}
	cur, err := t.str("currency")
	if err != nil {
		return r, err
	}
	r.Currency = strings.TrimSpace(cur)
	if r.Total, err = t.dec("total_amount"); err != nil {
		return r, err
	}
	if r.SrcVersion, err = t.u64("version"); err != nil {
		return r, err
	}
	if !t.isNull("deleted_at") {
		r.Deleted = 1
	}
	return r, nil
}

func itemFromTuple(t tuple) (ItemRow, error) {
	var r ItemRow
	var err error
	if r.ID, err = t.uuid("id"); err != nil {
		return r, err
	}
	if r.OrderID, err = t.uuid("order_id"); err != nil {
		return r, err
	}
	if r.ProductID, err = t.uuid("product_id"); err != nil {
		return r, err
	}
	if r.CreatedAt, err = t.ts("created_at"); err != nil {
		return r, err
	}
	if r.Quantity, err = t.dec("quantity"); err != nil {
		return r, err
	}
	if r.UnitPrice, err = t.dec("unit_price"); err != nil {
		return r, err
	}
	if r.SrcVersion, err = t.u64("version"); err != nil {
		return r, err
	}
	if !t.isNull("deleted_at") {
		r.Deleted = 1
	}
	return r, nil
}

func productFromTuple(t tuple) (ProductRow, error) {
	var r ProductRow
	var err error
	if r.ID, err = t.uuid("id"); err != nil {
		return r, err
	}
	if !t.isNull("sku") {
		s, _ := t.str("sku")
		r.SKU = &s
	}
	if r.Name, err = t.str("name"); err != nil {
		return r, err
	}
	if a, _ := t.str("is_active"); a == "t" {
		r.IsActive = 1
	}
	if r.SrcVersion, err = t.u64("version"); err != nil {
		return r, err
	}
	return r, nil
}
