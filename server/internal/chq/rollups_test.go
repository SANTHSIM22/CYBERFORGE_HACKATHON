package chq

import (
	"testing"

	"tenantpulse/internal/domain"
)

func TestSplitMonths(t *testing.T) {
	type seg struct {
		from, to string
		months   bool
	}
	cases := []struct {
		from, to string
		want     []seg
	}{
		{"2025-11-15", "2026-11-01", []seg{{"2025-11-15", "2025-12-01", false}, {"2025-12-01", "2026-11-01", true}}},
		{"2025-11-01", "2026-11-01", []seg{{"2025-11-01", "2026-11-01", true}}},
		{"2025-10-31", "2026-11-01", []seg{{"2025-10-31", "2025-11-01", false}, {"2025-11-01", "2026-11-01", true}}},
		{"2026-01-20", "2026-03-10", []seg{{"2026-01-20", "2026-02-01", false}, {"2026-02-01", "2026-03-01", true}, {"2026-03-01", "2026-03-10", false}}},
		{"2026-10-03", "2026-10-20", []seg{{"2026-10-03", "2026-10-20", false}}}, // inside one month
		{"2026-09-30", "2026-10-02", []seg{{"2026-09-30", "2026-10-02", false}}}, // crosses a month, no whole month
		{"2026-10-25", "2026-12-01", []seg{{"2026-10-25", "2026-11-01", false}, {"2026-11-01", "2026-12-01", true}}},
	}
	for _, c := range cases {
		got := splitMonths(domain.DayRange{From: c.from, To: c.to})
		if len(got) != len(c.want) {
			t.Fatalf("%s..%s: got %d segments %+v", c.from, c.to, len(got), got)
		}
		for i, w := range c.want {
			if got[i].From != w.from || got[i].To != w.to || got[i].months != w.months {
				t.Fatalf("%s..%s segment %d: got %+v, want %+v", c.from, c.to, i, got[i], w)
			}
		}
		// segments tile the range exactly
		if got[0].From != c.from || got[len(got)-1].To != c.to {
			t.Fatalf("%s..%s: segments do not cover the range: %+v", c.from, c.to, got)
		}
		for i := 1; i < len(got); i++ {
			if got[i].From != got[i-1].To {
				t.Fatalf("%s..%s: gap or overlap at %d: %+v", c.from, c.to, i, got)
			}
		}
	}
}
