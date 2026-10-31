package domain

import "time"

// DayRange is a half-open range of tenant-local calendar days [From, To) as YYYY-MM-DD.
type DayRange struct {
	From string
	To   string
}

// Plan splits a query range into the cheapest exact combination of tiers:
// whole tenant-local days (day rollups), whole UTC hours (hour rollups) and
// the remaining partial-hour edges (raw rows). The parts never overlap and
// always cover [from, to) exactly.
type Plan struct {
	Days  []DayRange
	Hours []TimeRange
	Raw   []TimeRange
}

// Decompose builds a Plan. useDays=false is used for hourly trends, which need
// hour-level buckets everywhere.
func Decompose(from, to time.Time, loc *time.Location, useDays bool) Plan {
	var p Plan
	if !to.After(from) {
		return p
	}
	rest := []TimeRange{{from, to}}
	if useDays {
		d1 := NextLocalMidnight(from, loc)
		d2 := PrevLocalMidnight(to, loc)
		switch {
		case d1.Before(d2):
			p.Days = append(p.Days, DayRange{LocalDay(d1, loc), LocalDay(d2, loc)})
			rest = rest[:0]
			if from.Before(d1) {
				rest = append(rest, TimeRange{from, d1})
			}
			if d2.Before(to) {
				rest = append(rest, TimeRange{d2, to})
			}
		case d1.Equal(d2) && from.Before(d1) && d1.Before(to):
			// No whole day, but the range crosses one local midnight: split there
			// so no part (e.g. a UTC hour in a +5:30 zone) spans two local days.
			rest = []TimeRange{{from, d1}, {d1, to}}
		}
	}
	for _, r := range rest {
		h1 := ceilHour(r.From)
		h2 := r.To.UTC().Truncate(time.Hour)
		if h1.Before(h2) {
			p.Hours = append(p.Hours, TimeRange{h1, h2})
			if r.From.Before(h1) {
				p.Raw = append(p.Raw, TimeRange{r.From, h1})
			}
			if h2.Before(r.To) {
				p.Raw = append(p.Raw, TimeRange{h2, r.To})
			}
		} else {
			p.Raw = append(p.Raw, r)
		}
	}
	return p
}

func ceilHour(t time.Time) time.Time {
	f := t.UTC().Truncate(time.Hour)
	if f.Equal(t) {
		return f
	}
	return f.Add(time.Hour)
}

// LocalDayBounds returns the UTC instants [start, end) of a tenant-local day.
func LocalDayBounds(day string, loc *time.Location) (time.Time, time.Time, error) {
	d, err := time.Parse(time.DateOnly, day)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	next := d.AddDate(0, 0, 1)
	return StartOfLocalDay(d.Year(), d.Month(), d.Day(), loc), StartOfLocalDay(next.Year(), next.Month(), next.Day(), loc), nil
}
