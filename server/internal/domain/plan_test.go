package domain

import (
	"math/rand/v2"
	"testing"
	"time"
)

// Every instant inside [from,to) must be covered by exactly one plan part and
// no instant outside it may be covered, for awkward zones including DST and
// half/quarter-hour offsets.
func TestDecomposeCoversRangeExactly(t *testing.T) {
	zones := []string{"UTC", "Asia/Kolkata", "Asia/Kathmandu", "America/New_York", "Australia/Lord_Howe", "America/Santiago", "Europe/London"}
	r := rand.New(rand.NewPCG(7, 7))
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, z := range zones {
		loc, err := time.LoadLocation(z)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 400; i++ {
			from := base.Add(time.Duration(r.Int64N(int64(365 * 24 * time.Hour))))
			if r.IntN(3) == 0 {
				from = from.Truncate(time.Hour)
			}
			to := from.Add(time.Duration(r.Int64N(int64(40 * 24 * time.Hour))))
			p := Decompose(from, to, loc, r.IntN(4) != 0)
			for j := 0; j < 200; j++ {
				span := to.Sub(from) + 4*time.Hour
				x := from.Add(-2*time.Hour + time.Duration(r.Int64N(int64(span)+1)))
				inside := !x.Before(from) && x.Before(to)
				if n := covering(p, x, loc); (inside && n != 1) || (!inside && n != 0) {
					t.Fatalf("%s from=%v to=%v x=%v inside=%v covered=%d plan=%+v", z, from, to, x, inside, n, p)
				}
			}
		}
	}
}

// With day buckets, no hour or raw part may span two local days, otherwise a
// daily trend would put some orders on the wrong day (+5:30 / +5:45 zones).
func TestDecomposeDayAlignedParts(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 9))
	for _, z := range []string{"Asia/Kolkata", "Asia/Kathmandu", "America/New_York", "Australia/Lord_Howe"} {
		loc, _ := time.LoadLocation(z)
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 2000; i++ {
			from := base.Add(time.Duration(r.Int64N(int64(365 * 24 * time.Hour))))
			to := from.Add(time.Duration(r.Int64N(int64(72 * time.Hour))))
			p := Decompose(from, to, loc, true)
			for _, part := range append(append([]TimeRange{}, p.Hours...), p.Raw...) {
				last := part.To.Add(-time.Nanosecond)
				if LocalDay(part.From, loc) != LocalDay(last, loc) {
					t.Fatalf("%s: part %v spans two local days (range %v-%v)", z, part, from, to)
				}
			}
		}
	}
}

func covering(p Plan, x time.Time, loc *time.Location) int {
	n := 0
	day := LocalDay(x, loc)
	for _, d := range p.Days {
		if day >= d.From && day < d.To {
			n++
		}
	}
	h := HourBucket(x)
	for _, r := range p.Hours {
		if !h.Before(r.From) && h.Before(r.To) {
			n++
		}
	}
	for _, r := range p.Raw {
		if !x.Before(r.From) && x.Before(r.To) {
			n++
		}
	}
	return n
}

func TestLocalDayBoundsDST(t *testing.T) {
	loc, _ := time.LoadLocation("America/New_York")
	s, e, err := LocalDayBounds("2026-11-01", loc) // 25-hour day
	if err != nil || e.Sub(s) != 25*time.Hour {
		t.Fatalf("got %v %v %v", s, e, err)
	}
}
