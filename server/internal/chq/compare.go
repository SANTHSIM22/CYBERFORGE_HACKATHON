package chq

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Diff helpers compare an analytics answer with the direct-SQL answer.
// Decimals compare by value (exact), never by formatting.

func DiffStatusRows(got, want []StatusRow) []string {
	key := func(r StatusRow) string { return r.Currency + "/" + r.Status }
	gm := map[string]StatusRow{}
	for _, r := range got {
		gm[key(r)] = r
	}
	var diffs []string
	seen := map[string]bool{}
	for _, w := range want {
		k := key(w)
		seen[k] = true
		g, ok := gm[k]
		switch {
		case !ok:
			diffs = append(diffs, fmt.Sprintf("%s: missing in analytics (direct: %d orders, gross %s)", k, w.Orders, w.Gross))
		case g.Orders != w.Orders || !g.Gross.Equal(w.Gross) || !g.Recognized.Equal(w.Recognized):
			diffs = append(diffs, fmt.Sprintf("%s: analytics %d/%s/%s vs direct %d/%s/%s", k,
				g.Orders, g.Gross, g.Recognized, w.Orders, w.Gross, w.Recognized))
		}
	}
	for _, g := range got {
		if !seen[key(g)] {
			diffs = append(diffs, fmt.Sprintf("%s: only in analytics (%d orders)", key(g), g.Orders))
		}
	}
	return diffs
}

func DiffTotals(got, want []CurrencyTotals) []string {
	gm := map[string]CurrencyTotals{}
	for _, g := range got {
		gm[g.Currency] = g
	}
	var diffs []string
	for _, w := range want {
		g := gm[w.Currency]
		if g.Orders != w.Orders || !decEq(g.GrossSales, w.GrossSales) || !decEq(g.RecognizedSales, w.RecognizedSales) {
			diffs = append(diffs, fmt.Sprintf("%s: analytics %d / %s / %s vs direct %d / %s / %s", w.Currency,
				g.Orders, g.GrossSales, g.RecognizedSales, w.Orders, w.GrossSales, w.RecognizedSales))
		}
		delete(gm, w.Currency)
	}
	for cur := range gm {
		diffs = append(diffs, cur+": only in analytics")
	}
	return diffs
}

func DiffTrend(got, want []TrendPoint) []string {
	gm := map[string]TrendPoint{}
	for _, p := range got {
		gm[p.Bucket] = p
	}
	var diffs []string
	for _, w := range want {
		g, ok := gm[w.Bucket]
		if !ok || g.Orders != w.Orders || !decEq(g.GrossSales, w.GrossSales) || !decEq(g.RecognizedSales, w.RecognizedSales) {
			diffs = append(diffs, fmt.Sprintf("%s: analytics %+v vs direct %+v", w.Bucket, g, w))
		}
		delete(gm, w.Bucket)
	}
	for b, p := range gm {
		if p.Orders != 0 { // zero-filled chart buckets are not differences
			diffs = append(diffs, fmt.Sprintf("%s: only in analytics", b))
		}
	}
	return diffs
}

func DiffTop(got, want []TopProduct) []string {
	var diffs []string
	if len(got) != len(want) {
		diffs = append(diffs, fmt.Sprintf("row count: analytics %d vs direct %d", len(got), len(want)))
	}
	for i := 0; i < min(len(got), len(want)); i++ {
		g, w := got[i], want[i]
		if g.ProductID != w.ProductID || !decEq(g.Quantity, w.Quantity) || !decEq(g.Revenue, w.Revenue) {
			diffs = append(diffs, fmt.Sprintf("rank %d: analytics %s qty=%s rev=%s vs direct %s qty=%s rev=%s",
				i+1, g.ProductID, g.Quantity, g.Revenue, w.ProductID, w.Quantity, w.Revenue))
		}
	}
	return diffs
}

func decEq(a, b string) bool {
	x, err1 := decimal.NewFromString(a)
	y, err2 := decimal.NewFromString(b)
	return err1 == nil && err2 == nil && x.Equal(y)
}
