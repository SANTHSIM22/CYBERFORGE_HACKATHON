// Package domain holds the metric semantics shared by the aggregator, the API,
// the demo service and the reconciliation tool. Changing a definition here
// changes it everywhere at once.
package domain

import (
	"hash/fnv"
	"time"

	"github.com/shopspring/decimal"
)

// RecognizedStatuses count toward recognized_sales and product revenue.
// pending, cancelled and refunded orders still count in orders_count and gross_sales.
var RecognizedStatuses = []string{"placed", "paid", "processing", "shipped", "delivered"}

// AllStatuses in display order.
var AllStatuses = []string{"pending", "placed", "paid", "processing", "shipped", "delivered", "cancelled", "refunded"}

func IsRecognized(status string) bool {
	switch status {
	case "placed", "paid", "processing", "shipped", "delivered":
		return true
	}
	return false
}

func ValidStatus(s string) bool {
	for _, v := range AllStatuses {
		if v == s {
			return true
		}
	}
	return false
}

// Order is the projected state of a source order.
type Order struct {
	ID        string
	CreatedAt time.Time
	UpdatedAt time.Time
	Status    string
	Currency  string
	Total     decimal.Decimal
	Version   int64
	Deleted   bool
	LSN       int64
	Offset    int64
}

// Item is the projected state of a source order line.
type Item struct {
	ID        string
	OrderID   string
	ProductID string
	Quantity  decimal.Decimal
	UnitPrice decimal.Decimal
	Version   int64
	Deleted   bool
	LSN       int64
	Offset    int64
}

// Product is the projected display metadata of a product.
type Product struct {
	ID       string
	SKU      *string
	Name     string
	IsActive bool
	Version  int64
	Deleted  bool
	LSN      int64
	Offset   int64
}

// HourBucket is the UTC hour an instant belongs to.
func HourBucket(t time.Time) time.Time { return t.UTC().Truncate(time.Hour) }

// LocalDay is the calendar day (YYYY-MM-DD) of an instant in the tenant timezone.
func LocalDay(t time.Time, loc *time.Location) string { return t.In(loc).Format(time.DateOnly) }

// UnitPrice is the deterministic list price of a product, shared by the data
// generator and the demo service (the source schema has no price column; the
// price is captured per order line).
func UnitPrice(productID string) decimal.Decimal {
	h := fnv.New32a()
	_, _ = h.Write([]byte(productID))
	cents := int64(200 + h.Sum32()%49800) // 2.00 .. 499.99
	return decimal.New(cents, -2)
}
