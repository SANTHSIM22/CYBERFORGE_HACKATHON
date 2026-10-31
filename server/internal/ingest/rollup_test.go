package ingest

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestQuarterBuckets(t *testing.T) {
	var c Cells
	for _, s := range []string{"2026-10-31T18:14:59Z", "2026-10-31T18:15:00Z", "2026-10-31T18:29:59.999Z", "2026-10-31T18:00:00Z"} {
		tm, _ := time.Parse(time.RFC3339Nano, s)
		c.addOrderTime(tm, time.UTC)
	}
	want := map[int64]bool{}
	for _, s := range []string{"2026-10-31T18:00:00Z", "2026-10-31T18:15:00Z"} {
		tm, _ := time.Parse(time.RFC3339, s)
		want[tm.Unix()] = true
	}
	if len(c.Quarters) != len(want) {
		t.Fatalf("got %d quarters, want %d", len(c.Quarters), len(want))
	}
	for q := range c.Quarters {
		if !want[q] {
			t.Fatalf("unexpected quarter %s", time.Unix(q, 0).UTC())
		}
	}
}

// A +5:45 store: local midnight is 18:15 UTC, so the quarter 18:15-18:30 UTC
// belongs to the next local day and the 18:00 quarter to the previous one.
func TestPlanSplitsLocalDaysOnQuarters(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Kathmandu")
	if err != nil {
		t.Skip("no tzdata")
	}
	st := &Stream{Tenant: uuid.New(), Gen: 1, Loc: loc}
	var c Cells
	for _, s := range []string{"2026-10-31T18:05:00Z", "2026-10-31T18:20:00Z"} {
		tm, _ := time.Parse(time.RFC3339, s)
		c.addOrderTime(tm, loc)
	}
	p, err := planFor(st, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"toDate('2026-10-31')", "toDate('2026-11-01')"} {
		if !strings.Contains(p.dayCells(), d) {
			t.Fatalf("day %s missing from %s", d, p.dayCells())
		}
	}
	// local 2026-10-31 starts 2026-10-30 18:15 UTC; local 2026-11-02 starts 2026-11-01 18:15 UTC
	for _, s := range []string{"2026-10-30T18:15:00Z", "2026-11-01T18:15:00Z"} {
		b, _ := time.Parse(time.RFC3339, s)
		if !strings.Contains(p.daySrc(), dt(b.Unix())) {
			t.Fatalf("local day boundary %s missing from %s", s, p.daySrc())
		}
	}
	if m := p.monthPred("month"); !strings.Contains(m, "toDate('2026-10-01')") || !strings.Contains(m, "toDate('2026-12-01')") {
		t.Fatalf("months: %s", m)
	}
	if !strings.Contains(p.rawItems(), "created_at >=") {
		t.Fatalf("items must be bounded: %s", p.rawItems())
	}
	full, _ := planFor(st, Cells{Full: true})
	if full.rawOrders() != tenantPred(st) || full.dayCells() != tenantPred(st) {
		t.Fatalf("full rebuild must cover everything: %+v", full)
	}
}

func TestPlanMergesNearbyQuarters(t *testing.T) {
	st := &Stream{Tenant: uuid.New(), Gen: 1, Loc: time.UTC}
	var c Cells
	for _, s := range []string{"2026-10-31T10:00:00Z", "2026-10-31T10:45:00Z", "2026-10-31T13:00:00Z"} {
		tm, _ := time.Parse(time.RFC3339, s)
		c.addOrderTime(tm, time.UTC)
	}
	p, _ := planFor(st, c)
	if len(p.q) != 2 || p.q[0][1]-p.q[0][0] != 3600 || p.q[1][1]-p.q[1][0] != quarter {
		t.Fatalf("quarter ranges: %v", p.q)
	}
	if len(p.days) != 1 || len(p.months) != 1 {
		t.Fatalf("days %v months %v", p.days, p.months)
	}
}

// TestDumpRollupSQL writes the refresh statements to $TP_DUMP_SQL (if set) so
// they can be checked against ClickHouse with EXPLAIN without running them.
func TestDumpRollupSQL(t *testing.T) {
	dir := os.Getenv("TP_DUMP_SQL")
	if dir == "" {
		t.Skip("set TP_DUMP_SQL to a directory")
	}
	loc, _ := time.LoadLocation("Asia/Kolkata")
	a := &Stream{Tenant: uuid.New(), Gen: 1, Loc: loc}
	b := &Stream{Tenant: uuid.New(), Gen: 1, Loc: time.UTC}
	var c Cells
	c.addOrderTime(time.Now().Add(-48*time.Hour), loc)
	c.addOrderTime(time.Now(), loc)
	batches := map[string][]streamCells{"incremental": {{a, c}, {b, c}}, "full": {{a, Cells{Full: true}}}}
	// TP_DUMP_TENANTS="uuid:tz,...": a batch where each real store touched its current quarter-hour
	if list := os.Getenv("TP_DUMP_TENANTS"); list != "" {
		var real []streamCells
		for _, e := range strings.Split(list, ",") {
			id, tz, _ := strings.Cut(e, ":")
			l, err := time.LoadLocation(tz)
			if err != nil {
				l = time.UTC
			}
			var cc Cells
			cc.addOrderTime(time.Now(), l)
			real = append(real, streamCells{&Stream{Tenant: uuid.MustParse(id), Gen: 1, Loc: l}, cc})
		}
		batches["real"] = real
	}
	for name, sets := range batches {
		var ps []plan
		for _, s := range sets {
			p, err := planFor(s.st, s.cells)
			if err != nil {
				t.Fatal(err)
			}
			ps = append(ps, p)
		}
		steps := buildSteps(ps, "1")
		for i, s := range steps {
			if err := os.WriteFile(fmt.Sprintf("%s/%s-%d.sql", dir, name, i), []byte(s.sql), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}
