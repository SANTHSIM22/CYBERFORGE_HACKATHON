package api

import (
	"errors"
	"regexp"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"tenantpulse/internal/domain"
)

const maxRangeDays = 400

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

// normalized is hashed into the cache key, so it must contain every input
// that changes the answer and nothing else.
type normalized struct {
	Route    string `json:"r"`
	From     int64  `json:"f"`
	To       int64  `json:"t"`
	Currency string `json:"c"`
	Grain    string `json:"g,omitempty"`
	Limit    int    `json:"l,omitempty"`
	Offset   int    `json:"o,omitempty"`
	TZ       string `json:"z"`
}

type badRequest struct{ msg string }

func (e badRequest) Error() string { return e.msg }

// parseRange accepts either from/to (RFC 3339, half-open) or a preset
// range=today|7d|30d|90d aligned to local days in loc (the store's timezone, or
// the viewer's on org dashboards).
func parseRange(c *gin.Context, loc *time.Location, now time.Time) (time.Time, time.Time, error) {
	fromS, toS := c.Query("from"), c.Query("to")
	if fromS == "" && toS == "" {
		days := 7
		switch c.DefaultQuery("range", "7d") {
		case "today", "1d":
			days = 1
		case "7d":
			days = 7
		case "30d":
			days = 30
		case "90d":
			days = 90
		case "365d":
			days = 365
		default:
			return time.Time{}, time.Time{}, badRequest{"range must be one of today, 7d, 30d, 90d, 365d"}
		}
		r := domain.PresetRange(now, days, loc)
		return r.From, r.To, nil
	}
	from, err := parseInstant(fromS, loc)
	if err != nil {
		return time.Time{}, time.Time{}, badRequest{"from must be RFC 3339 (2026-10-24T00:00:00Z) or a tenant-local date (2026-10-24)"}
	}
	to, err := parseInstant(toS, loc)
	if err != nil {
		return time.Time{}, time.Time{}, badRequest{"to must be RFC 3339 (2026-10-31T00:00:00Z) or a tenant-local date (2026-10-31)"}
	}
	if !to.After(from) {
		return time.Time{}, time.Time{}, badRequest{"to must be after from (ranges are half-open [from, to))"}
	}
	if to.Sub(from) > maxRangeDays*24*time.Hour {
		return time.Time{}, time.Time{}, badRequest{"range may not exceed " + strconv.Itoa(maxRangeDays) + " days"}
	}
	return from.UTC(), to.UTC(), nil
}

// parseInstant accepts RFC 3339 or a bare date, which means local midnight in
// the tenant's timezone (DST-correct), so date pickers need no tz math.
func parseInstant(s string, loc *time.Location) (time.Time, error) {
	if len(s) == len(time.DateOnly) {
		d, err := time.Parse(time.DateOnly, s)
		if err != nil {
			return time.Time{}, err
		}
		return domain.StartOfLocalDay(d.Year(), d.Month(), d.Day(), loc), nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

func parseCurrency(c *gin.Context, def string) (string, error) {
	cur := c.Query("currency")
	if cur == "" {
		return def, nil
	}
	if !currencyRe.MatchString(cur) {
		return "", badRequest{"currency must be a 3-letter ISO code"}
	}
	return cur, nil
}

func parseGrain(c *gin.Context, from, to time.Time) (string, error) {
	g := c.DefaultQuery("grain", "day")
	switch g {
	case "day":
	case "hour":
		if to.Sub(from) > 31*24*time.Hour {
			return "", badRequest{"grain=hour supports ranges up to 31 days"}
		}
	default:
		return "", badRequest{"grain must be hour or day"}
	}
	return g, nil
}

// parsePage reads a top-N page: limit = N (1..100, default 10) and offset =
// how many ranked products come before this page (0..100000, default 0).
func parsePage(c *gin.Context) (limit, offset int, err error) {
	limit, err = strconv.Atoi(c.DefaultQuery("limit", "10"))
	if err != nil || limit < 1 || limit > 100 {
		return 0, 0, badRequest{"limit must be between 1 and 100"}
	}
	offset, err = strconv.Atoi(c.DefaultQuery("offset", "0"))
	if err != nil || offset < 0 || offset > 100_000 {
		return 0, 0, badRequest{"offset must be between 0 and 100000"}
	}
	return limit, offset, nil
}

var errBusy = errors.New("tenant query capacity exhausted")
