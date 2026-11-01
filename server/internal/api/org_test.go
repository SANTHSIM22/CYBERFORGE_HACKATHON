package api

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"tenantpulse/internal/chq"
)

func TestConverter(t *testing.T) {
	d := decimal.RequireFromString
	cv := &converter{to: "INR", units: map[string]decimal.Decimal{"USD": d("1"), "INR": d("88"), "EUR": d("0.8")}, missing: map[string]bool{}}
	cases := []struct{ cur, amount, want string }{
		{"INR", "100", "100.00"}, // the reporting currency is never converted
		{"USD", "10", "880.00"},  // 10 USD × 88 INR/USD
		{"EUR", "8", "880.00"},   // 8 EUR = 10 USD
		{"XYZ", "500", "0.00"},   // no rate: left out, reported
		{"USD", "0.01", "0.88"},  // cents survive
	}
	for _, c := range cases {
		if got := money2(cv.conv(c.cur, d(c.amount))); got != c.want {
			t.Errorf("%s %s -> %s, want %s", c.amount, c.cur, got, c.want)
		}
	}
	if !cv.missing["XYZ"] || len(cv.missing) != 1 {
		t.Errorf("missing rates: %v", cv.missing)
	}
	if f := cv.factors(); f["USD"] != 88 || f["INR"] != 1 || f["EUR"] != 110 {
		t.Errorf("ranking factors: %v", f)
	}
}

// A live store is reported in steps, so its dashboards hear about a change of
// step, not every second's re-confirmation.
func TestFreshBound(t *testing.T) {
	now := time.Now()
	at := func(ago time.Duration) chq.Status {
		t := now.Add(-ago)
		return chq.Status{Found: true, StreamCheckedAt: &t}
	}
	for ago, want := range map[time.Duration]int64{
		800 * time.Millisecond: 2_000, 1900 * time.Millisecond: 2_000, 3 * time.Second: 5_000,
		9 * time.Second: 10_000, 29 * time.Second: 30_000, 45 * time.Second: 0,
	} {
		if got := freshBound(at(ago), now); got != want {
			t.Errorf("data %v old: step %d, want %d", ago, got, want)
		}
	}
	if freshBound(chq.Status{}, now) != 0 {
		t.Error("unknown age has no step")
	}
}

func TestNextOffset(t *testing.T) {
	ptr := func(p *int) int {
		if p == nil {
			return -1
		}
		return *p
	}
	for _, c := range []struct{ offset, n, total, want int }{
		{0, 10, 25, 10}, {10, 10, 25, 20}, {20, 5, 25, -1}, {30, 0, 25, -1}, {0, 0, 0, -1},
	} {
		if got := ptr(nextOffset(c.offset, c.n, c.total)); got != c.want {
			t.Errorf("offset %d, %d shown of %d: next %d, want %d", c.offset, c.n, c.total, got, c.want)
		}
	}
}
