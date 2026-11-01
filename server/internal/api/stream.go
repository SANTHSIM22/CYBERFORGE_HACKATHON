package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"tenantpulse/internal/registry"
)

var promStreams = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "tenantpulse_api_freshness_streams", Help: "Open freshness streams (dashboards watching a store)."})

// stream pushes the store's freshness (the /freshness body) as Server-Sent
// Events, only when something the dashboard acts on changes: new data (a new
// data version), the pipeline's health (live, lagging, stalled), the snapshot
// state or the generation. The API already learns every store's state from one
// ClickHouse query every 250 ms and re-evaluates health on the clock, so a
// store with no changes and a healthy pipeline sends nothing but a keepalive
// comment every StreamKeepalive, which proxies need (nginx closes a response
// silent for 30 s) and the browser uses to notice a dead connection. Polling
// cost one HTTP request per viewer per second.
func (s *Server) stream(c *gin.Context) {
	key := c.Param("tenant")
	t := c.MustGet("tenant").(*registry.Tenant)
	if s.Direct {
		abort(c, 404, "not_found", "the direct benchmark backend has no stream; poll /freshness")
		return
	}
	s.sse(c, func() (gin.H, string, <-chan struct{}, func()) {
		if cur := s.lookup(key); cur != nil {
			t = cur // status and generation can change under a long-lived stream
		}
		changed := s.Cache.Changed(t.ID, t.ActiveGeneration) // taken before reading, so no change is missed
		st, _ := s.Cache.Status(t.ID, t.ActiveGeneration)
		body, state := s.freshnessBody(t, st, time.Now())
		return body, state, changed, func() {}
	})
}

// sse runs one Server-Sent Events response: step returns the current body,
// a state string (an event is written only when it changes), a channel that
// closes when the state may have changed, and a release for that channel.
func (s *Server) sse(c *gin.Context, step func() (body gin.H, state string, changed <-chan struct{}, release func())) {
	rc := http.NewResponseController(c.Writer)
	// The server's WriteTimeout bounds a whole response; a stream is open-ended.
	if err := rc.SetWriteDeadline(time.Time{}); err != nil {
		abort(c, 500, "stream_unsupported", "streaming is not supported here; poll /freshness")
		return
	}
	h := c.Writer.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no") // nginx: pass each event through at once
	c.Status(200)
	promStreams.Inc()
	defer promStreams.Dec()

	ctx := c.Request.Context()
	keepalive := time.NewTicker(s.StreamKeepalive)
	defer keepalive.Stop()
	var last string
	for {
		body, state, changed, release := step()
		if state != last {
			b, err := json.Marshal(body)
			if err == nil {
				_, err = fmt.Fprintf(c.Writer, "event: freshness\ndata: %s\n\n", b)
			}
			if err == nil {
				err = rc.Flush()
			}
			if err != nil {
				release()
				return
			}
			last = state
		}
		var err error
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-changed:
		case <-keepalive.C: // not an event: the dashboard never sees it
			if _, err = io.WriteString(c.Writer, ": keepalive\n\n"); err == nil {
				err = rc.Flush()
			}
		}
		release()
		if err != nil {
			return
		}
	}
}
