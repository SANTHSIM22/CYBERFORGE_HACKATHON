package domain

import "time"

// TimeRange is a half-open UTC interval [From, To).
type TimeRange struct {
	From time.Time
	To   time.Time
}

// StartOfLocalDay returns the earliest instant whose local date is y-m-d. It is
// correct on days where local midnight is skipped by a DST transition.
func StartOfLocalDay(y int, m time.Month, d int, loc *time.Location) time.Time {
	t := time.Date(y, m, d, 0, 0, 0, 0, loc)
	for i := 0; i < 4; i++ {
		ly, lm, ld := t.In(loc).Date()
		if ly == y && lm == m && ld == d {
			break
		}
		t = t.Add(time.Hour)
	}
	// Walk back in case a full hour before t is still on the same local date.
	for {
		prev := t.Add(-time.Minute)
		py, pm, pd := prev.In(loc).Date()
		if py != y || pm != m || pd != d {
			return t.UTC()
		}
		t = prev
	}
}

// NextLocalMidnight is the first local-day boundary at or after t.
func NextLocalMidnight(t time.Time, loc *time.Location) time.Time {
	y, m, d := t.In(loc).Date()
	s := StartOfLocalDay(y, m, d, loc)
	if s.Equal(t) {
		return s
	}
	n := time.Date(y, m, d+1, 12, 0, 0, 0, loc)
	ny, nm, nd := n.Date()
	return StartOfLocalDay(ny, nm, nd, loc)
}

// PrevLocalMidnight is the last local-day boundary at or before t.
func PrevLocalMidnight(t time.Time, loc *time.Location) time.Time {
	y, m, d := t.In(loc).Date()
	return StartOfLocalDay(y, m, d, loc)
}

// PresetRange returns the tenant-local, day-aligned range for "last N days":
// [start of today-(n-1), start of tomorrow). It is stable for the whole day (so
// cache keys stay stable) and always includes orders created a moment ago.
func PresetRange(now time.Time, days int, loc *time.Location) TimeRange {
	y, m, d := now.In(loc).Date()
	fy, fm, fd := time.Date(y, m, d-(days-1), 12, 0, 0, 0, loc).Date()
	ty, tm, td := time.Date(y, m, d+1, 12, 0, 0, 0, loc).Date()
	return TimeRange{StartOfLocalDay(fy, fm, fd, loc), StartOfLocalDay(ty, tm, td, loc)}
}
