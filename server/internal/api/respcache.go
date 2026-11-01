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

	umu     sync.Mutex
	subs    map[string]chan struct{} // tenantID:generation -> closed when that store's data, snapshot state or health changes
	anySubs map[*anySub]struct{}     // org streams: closed when any of their stores changes
	health  map[string]string        // tenantID:generation -> pipeline health at the last poll
	now     func() time.Time
}

type anySub struct {
	keys map[string]bool
	ch   chan struct{}
}

// pipelineHealth is a store's pipeline state from its status row:
// "snapshotting" (first copy still loading), "stalled" (the change stream has
// not confirmed in 30 s), "lagging" (the newest data is over 30 s old) or
// "live". It depends on the clock, not only on new rows.
func pipelineHealth(st chq.Status, now time.Time) string {
	if !st.Found || st.SnapshotState != "completed" {
		return "snapshotting"
	}
	checked := st.StreamCheckedAt
	if hb := st.LastHeartbeatSourceAt; hb != nil && (checked == nil || hb.After(*checked)) {
		checked = hb
	}
	if checked == nil || now.Sub(*checked) > 30*time.Second {
		return "stalled"
	}
	if at := st.DataAsOf(); at != nil && now.Sub(*at) > 30*time.Second {
		return "lagging"
	}
	return "live"
}

// freshBounds are the steps a live store's freshness is reported in: its data
// is within this many milliseconds of the store's own database. Steps, not
// the exact age, so a healthy store (re-confirmed about every second) stays
// on one step and its dashboards get no events until something changes.
var freshBounds = []int64{2_000, 5_000, 10_000, 30_000}

// freshBound is the smallest step the store's data age fits in (0 when it is
// not known or beyond the last step).
func freshBound(st chq.Status, now time.Time) int64 {
	at := st.DataAsOf()
	if at == nil {
		return 0
	}
	age := now.Sub(*at).Milliseconds()
	for _, b := range freshBounds {
		if age <= b {
			return b
		}
	}
	return 0
}

// answer is a response body and the path that computed it: "view" (the
// in-memory rollups) or "clickhouse". It is reported as meta.source, also on
// cache hits.
type answer struct {
	body   []byte
	source string
}

type cacheEntry struct {
	answer
	version uint64
	at      time.Time
}

func NewRespCache(maxItems int, ttl, stale time.Duration) *RespCache {
	return &RespCache{entries: map[string]cacheEntry{}, maxItems: maxItems, ttl: ttl, stale: stale, versions: map[string]uint64{}, asOf: map[string]time.Time{},
		subs: map[string]chan struct{}{}, anySubs: map[*anySub]struct{}{}, now: time.Now}
}

// ChangedAny is Changed for a set of stores (an org's): the channel closes
// when any of them changes. Call cancel once no longer waiting on it.
func (c *RespCache) ChangedAny(keys []string) (ch <-chan struct{}, cancel func()) {
	s := &anySub{keys: make(map[string]bool, len(keys)), ch: make(chan struct{})}
	for _, k := range keys {
		s.keys[k] = true
	}
	c.umu.Lock()
	c.anySubs[s] = struct{}{}
	c.umu.Unlock()
	return s.ch, func() {
		c.umu.Lock()
		delete(c.anySubs, s)
		c.umu.Unlock()
	}
}

// StoreKey is how the cache names a store's data: tenantID:generation.
func StoreKey(tenantID string, gen int64) string { return tenantID + ":" + strconv.FormatInt(gen, 10) }

// Changed returns a channel that is closed when a version poll sees the
// store's data version, snapshot state or pipeline health change, so a
// freshness stream wakes only when its own store has something to report
// (waking every stream on every poll cost ~0.5 core at 2,000 streams).
func (c *RespCache) Changed(tenantID string, gen int64) <-chan struct{} {
	k := tenantID + ":" + strconv.FormatInt(gen, 10)
	c.umu.Lock()
	defer c.umu.Unlock()
	ch, ok := c.subs[k]
	if !ok {
		ch = make(chan struct{})
		c.subs[k] = ch
	}
	return ch
}

// publish installs one poll's results (status nil: the poll failed, keep the
// last ones) and wakes the streams of every store whose data version,
// snapshot state or pipeline health changed. Health is re-evaluated after
// failed polls too, so a pipeline that stops confirming turns "stalled" even
// while ClickHouse cannot be reached.
func (c *RespCache) publish(versions map[string]uint64, asOf map[string]time.Time, status map[string]chq.Status) {
	now := c.now()
	c.vmu.Lock()
	old := c.status
	if status != nil {
		c.versions, c.asOf, c.status = versions, asOf, status
	}
	cur := c.status
	c.vmu.Unlock()
	health := make(map[string]string, len(cur)) // health and freshness step: a change in either wakes streams
	for k, st := range cur {
		health[k] = pipelineHealth(st, now) + "/" + strconv.FormatInt(freshBound(st, now), 10)
	}
	c.umu.Lock()
	defer c.umu.Unlock()
	prev := c.health
	c.health = health
	changed := map[string]bool{}
	for k, n := range cur {
		if o := old[k]; o.Found != n.Found || o.DataVersion != n.DataVersion || o.SnapshotState != n.SnapshotState || prev[k] != health[k] {
			changed[k] = true
		}
	}
	for k := range old {
		if _, ok := cur[k]; !ok {
			changed[k] = true
		}
	}
	for k, ch := range c.subs {
		if changed[k] {
			close(ch)
			delete(c.subs, k)
		}
	}
	for s := range c.anySubs {
		for k := range s.keys {
			if changed[k] {
				close(s.ch)
				delete(c.anySubs, s)
				break
			}
		}
	}
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
func (c *RespCache) Get(key string, version, minVersion uint64) (a answer, at uint64, fresh, ok bool) {
	c.mu.Lock()
	e, found := c.entries[key]
	c.mu.Unlock()
	age := time.Since(e.at)
	switch {
	case !found || age > c.ttl:
		return answer{}, 0, false, false
	case e.version >= version:
		return e.answer, e.version, true, true
	case c.stale > 0 && e.version >= minVersion && age <= c.stale:
		return e.answer, e.version, false, true
	}
	return answer{}, 0, false, false
}

// Refreshing reports whether a background refresh for key is already running;
// if not, it marks one as started (call the returned func when it finishes).
func (c *RespCache) Refreshing(key string) (done func(), running bool) {
	if _, loaded := c.inflight.LoadOrStore(key, struct{}{}); loaded {
		return nil, true
	}
	return func() { c.inflight.Delete(key) }, false
}

func (c *RespCache) Put(key string, version uint64, a answer) {
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
	c.entries[key] = cacheEntry{a, version, time.Now()}
}

// SweepExpired drops, every `every`, answers older than the TTL (they can
// never be served again), so cache memory follows recent traffic instead of
// waiting for the item cap.
func (c *RespCache) SweepExpired(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			c.sweep(now)
		}
	}
}

func (c *RespCache) sweep(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if now.Sub(e.at) > c.ttl {
			delete(c.entries, k)
		}
	}
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
			c.publish(next, asOf, status)
		} else if ctx.Err() == nil {
			slog.Warn("poll data versions", "err", err)
			c.publish(nil, nil, nil)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
