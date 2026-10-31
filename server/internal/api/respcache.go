package api

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"tenantpulse/internal/chq"
)

// RespCache is a per-process response cache. Each entry remembers the tenant
// data version (bumped by the ingester on every flush that contains the
// tenant's rows) it was computed at: an entry at the current version is a HIT.
// An entry one or more versions behind may still be served as STALE for at
// most `stale` while the new answer is computed in the background
// (stale-while-revalidate), so a version bump never sends every reader to
// ClickHouse at once. No external cache service needed; API replicas each
// keep their own (stateless, horizontally scalable).
type RespCache struct {
	mu       sync.Mutex
	entries  map[string]cacheEntry
	maxItems int
	ttl      time.Duration
	stale    time.Duration // 0 disables stale serving
	inflight sync.Map      // background refreshes in progress

	vmu      sync.RWMutex
	versions map[string]uint64    // tenantID:generation -> data_version
	asOf     map[string]time.Time // tenantID:generation -> source state fully reflected up to
	status   map[string]chq.Status
}

type cacheEntry struct {
	body    []byte
	version uint64
	at      time.Time
}

func NewRespCache(maxItems int, ttl, stale time.Duration) *RespCache {
	return &RespCache{entries: map[string]cacheEntry{}, maxItems: maxItems, ttl: ttl, stale: stale, versions: map[string]uint64{}, asOf: map[string]time.Time{}}
}

func (c *RespCache) Version(tenantID string, gen int64) uint64 {
	c.vmu.RLock()
	defer c.vmu.RUnlock()
	return c.versions[tenantID+":"+strconv.FormatInt(gen, 10)]
}

// AsOf returns the newest source state reflected for the tenant (from the
// same poll as the versions), so responses need no extra status query.
func (c *RespCache) AsOf(tenantID string, gen int64) *time.Time {
	c.vmu.RLock()
	defer c.vmu.RUnlock()
	t, ok := c.asOf[tenantID+":"+strconv.FormatInt(gen, 10)]
	if !ok {
		return nil
	}
	return &t
}

// Get returns the entry for key if it is at `version` (fresh), or behind it
// but at least `minVersion` and computed within the stale window (fresh=false).
func (c *RespCache) Get(key string, version, minVersion uint64) (body []byte, at uint64, fresh, ok bool) {
	c.mu.Lock()
	e, found := c.entries[key]
	c.mu.Unlock()
	age := time.Since(e.at)
	switch {
	case !found || age > c.ttl:
		return nil, 0, false, false
	case e.version >= version:
		return e.body, e.version, true, true
	case c.stale > 0 && e.version >= minVersion && age <= c.stale:
		return e.body, e.version, false, true
	}
	return nil, 0, false, false
}

// Refreshing reports whether a background refresh for key is already running;
// if not, it marks one as started (call the returned func when it finishes).
func (c *RespCache) Refreshing(key string) (done func(), running bool) {
	if _, loaded := c.inflight.LoadOrStore(key, struct{}{}); loaded {
		return nil, true
	}
	return func() { c.inflight.Delete(key) }, false
}

func (c *RespCache) Put(key string, version uint64, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxItems { // bounded memory: drop expired, else everything
		now := time.Now()
		for k, e := range c.entries {
			if now.Sub(e.at) > c.ttl {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.maxItems {
			c.entries = map[string]cacheEntry{}
		}
	}
	if e, ok := c.entries[key]; ok && e.version > version {
		return // a slower, older computation never replaces a newer answer
	}
	c.entries[key] = cacheEntry{body, version, time.Now()}
}

// Status returns the tenant's freshness row from the last poll, so
// /freshness (polled every second by every open dashboard) costs no query.
func (c *RespCache) Status(tenantID string, gen int64) (chq.Status, bool) {
	c.vmu.RLock()
	defer c.vmu.RUnlock()
	s, ok := c.status[tenantID+":"+strconv.FormatInt(gen, 10)]
	return s, ok
}

// PollVersions refreshes every tenant's freshness row and data version (one
// small query for all tenants).
func (c *RespCache) PollVersions(ctx context.Context, mon driver.Conn, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		rows, err := mon.Query(ctx, `SELECT toString(tenant_id), generation, snapshot_state, last_source_commit_at, last_heartbeat_source_at,
			last_applied_at, stream_checked_at, last_apply_lag_ms, slot_lag_bytes, events_total, data_version
			FROM tp.tenant_status FINAL`)
		if err == nil {
			next, asOf, status := map[string]uint64{}, map[string]time.Time{}, map[string]chq.Status{}
			for rows.Next() {
				var id string
				var gen uint32
				s := chq.Status{Found: true}
				if rows.Scan(&id, &gen, &s.SnapshotState, &s.LastSourceCommitAt, &s.LastHeartbeatSourceAt, &s.LastAppliedAt,
					&s.StreamCheckedAt, &s.LastApplyLagMs, &s.SlotLagBytes, &s.EventsTotal, &s.DataVersion) == nil {
					k := id + ":" + strconv.FormatUint(uint64(gen), 10)
					next[k], status[k] = s.DataVersion, s
					if at := s.DataAsOf(); at != nil && at.Unix() > 0 {
						asOf[k] = at.UTC()
					}
				}
			}
			rows.Close()
			c.vmu.Lock()
			c.versions, c.asOf, c.status = next, asOf, status
			c.vmu.Unlock()
		} else if ctx.Err() == nil {
			slog.Warn("poll data versions", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
