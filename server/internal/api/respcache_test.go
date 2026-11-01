package api

import (
	"context"
	"testing"
	"time"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/registry"
)

func TestRespCacheStaleWhileRevalidate(t *testing.T) {
	c := NewRespCache(100, time.Minute, 5*time.Second)
	c.Put("k", 7, answer{body: []byte("v7")})

	if a, at, fresh, ok := c.Get("k", 7, 0); !ok || !fresh || at != 7 || string(a.body) != "v7" {
		t.Fatalf("current version: got ok=%v fresh=%v at=%d", ok, fresh, at)
	}
	// New data arrived (version 8): the previous answer is served as stale.
	if _, at, fresh, ok := c.Get("k", 8, 0); !ok || fresh || at != 7 {
		t.Fatalf("stale within window: got ok=%v fresh=%v at=%d", ok, fresh, at)
	}
	// A caller that has already seen version 8 never gets version 7.
	if _, _, _, ok := c.Get("k", 8, 8); ok {
		t.Fatal("min_version must reject an older entry")
	}
	// An older, slower computation never replaces a newer answer.
	c.Put("k", 9, answer{body: []byte("v9")})
	c.Put("k", 8, answer{body: []byte("v8")})
	if a, at, fresh, _ := c.Get("k", 9, 0); !fresh || at != 9 || string(a.body) != "v9" {
		t.Fatalf("newer entry replaced: at=%d body=%s", at, a.body)
	}
}

func TestRespCacheStaleWindowExpires(t *testing.T) {
	c := NewRespCache(100, time.Minute, 5*time.Second)
	c.Put("k", 1, answer{body: []byte("v1")})
	c.mu.Lock()
	e := c.entries["k"]
	e.at = time.Now().Add(-6 * time.Second)
	c.entries["k"] = e
	c.mu.Unlock()
	if _, _, _, ok := c.Get("k", 2, 0); ok {
		t.Fatal("an entry older than the stale window must not be served for a newer version")
	}
	if _, _, fresh, ok := c.Get("k", 1, 0); !ok || !fresh {
		t.Fatal("an entry at the current version is fresh regardless of the stale window")
	}
	disabled := NewRespCache(100, time.Minute, 0)
	disabled.Put("k", 1, answer{body: []byte("v1")})
	if _, _, _, ok := disabled.Get("k", 2, 0); ok {
		t.Fatal("stale window 0 disables stale serving")
	}
}

func TestRespCacheRefreshingDedup(t *testing.T) {
	c := NewRespCache(100, time.Minute, time.Second)
	done, running := c.Refreshing("k:2")
	if running || done == nil {
		t.Fatal("first refresh should start")
	}
	if _, running := c.Refreshing("k:2"); !running {
		t.Fatal("second refresh for the same key/version must be deduplicated")
	}
	done()
	if _, running := c.Refreshing("k:2"); running {
		t.Fatal("refresh slot must be released when done")
	}
}

func TestLimiterBackgroundKeepsHalfFree(t *testing.T) {
	l := NewLimiter(64, 4, 10*time.Millisecond)
	short := func() context.Context {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	r1, ok1 := l.AcquireBackground(short(), "a")
	r2, ok2 := l.AcquireBackground(short(), "a")
	if !ok1 || !ok2 {
		t.Fatal("background work may use half of the tenant's slots")
	}
	if _, ok := l.AcquireBackground(short(), "a"); ok {
		t.Fatal("background work must leave half of the slots for readers")
	}
	// Readers still get the remaining two slots.
	f1, ok3 := l.Acquire(t.Context(), "a")
	f2, ok4 := l.Acquire(t.Context(), "a")
	if !ok3 || !ok4 {
		t.Fatal("readers must get the reserved slots")
	}
	if _, ok := l.Acquire(t.Context(), "a"); ok {
		t.Fatal("tenant cap must still hold")
	}
	for _, r := range []func(){r1, r2, f1, f2} {
		r()
	}
	if _, ok := l.AcquireBackground(short(), "b"); !ok {
		t.Fatal("other tenants are unaffected")
	}
	// All slots were returned: the tenant has full capacity again.
	for range 4 {
		if _, ok := l.Acquire(t.Context(), "a"); !ok {
			t.Fatal("slots leaked")
		}
	}
}

func TestWindowExcludesBenchmarkProbes(t *testing.T) {
	var w Window
	now := time.Now()
	w.Observe(now, 0.1, "HIT", "view")
	w.Observe(now, 0.2, "STALE", "view")
	w.Observe(now, 20, "MISS", "clickhouse")
	w.Observe(now, 400, "BYPASS", "clickhouse")
	st := w.Stats(now, 60)
	if st.Requests != 3 || st.All.Count != 3 || st.Probes.Count != 1 {
		t.Fatalf("requests=%d all=%d probes=%d", st.Requests, st.All.Count, st.Probes.Count)
	}
	if st.All.P99 >= 400 {
		t.Fatalf("benchmark probe leaked into dashboard latency: p99=%v", st.All.P99)
	}
	if st.HitRatio < 0.66 || st.HitRatio > 0.67 || st.StaleRatio < 0.33 || st.StaleRatio > 0.34 {
		t.Fatalf("hit=%v stale=%v", st.HitRatio, st.StaleRatio)
	}
}

// A busy fast second (3000 reads at 0.5 ms) next to a quiet slow one (20 reads
// at 300 ms): 20/3020 = 0.66% are slow, so p99 must be fast. Per-second
// sampling capped at 512 used to report ~300 ms here.
func TestWindowWeighsEveryRequest(t *testing.T) {
	var w Window
	now := time.Now()
	for range 3000 {
		w.Observe(now.Add(-time.Second), 0.5, "HIT", "view")
	}
	for range 20 {
		w.Observe(now, 300, "MISS", "clickhouse")
	}
	st := w.Stats(now, 60)
	if st.Requests != 3020 {
		t.Fatalf("requests=%d", st.Requests)
	}
	if st.All.P99 > 0.52 || st.All.P99 < 0.48 {
		t.Fatalf("p99=%v, want ~0.5 ms (2%% histogram resolution)", st.All.P99)
	}
	if st.Database.P50 < 294 || st.Database.P50 > 306 {
		t.Fatalf("db p50=%v, want ~300 ms", st.Database.P50)
	}
}

// Misses are split by what computed them, so a fast in-memory answer is never
// reported as a ClickHouse query (and does not hide slow ones).
func TestWindowSplitsMissesBySource(t *testing.T) {
	var w Window
	now := time.Now()
	for range 90 {
		w.Observe(now, 0.4, "MISS", "view")
	}
	for range 10 {
		w.Observe(now, 120, "MISS", "clickhouse")
	}
	w.Observe(now, 0.05, "HIT", "clickhouse") // hits count as cache hits whatever computed them
	st := w.Stats(now, 60)
	if st.Database.Count != 100 || st.View.Count != 90 || st.ClickHouse.Count != 10 {
		t.Fatalf("misses=%d view=%d clickhouse=%d", st.Database.Count, st.View.Count, st.ClickHouse.Count)
	}
	if st.ClickHouse.P50 < 117 || st.ClickHouse.P50 > 123 || st.View.P99 > 0.42 {
		t.Fatalf("clickhouse p50=%v view p99=%v", st.ClickHouse.P50, st.View.P99)
	}
}

// A poll wakes the streams of the stores whose data changed, and only those.
func TestRespCacheChangedWakesOnlyThatStore(t *testing.T) {
	c := NewRespCache(100, time.Minute, time.Second)
	st := func(v uint64) chq.Status { return chq.Status{Found: true, SnapshotState: "completed", DataVersion: v} }
	c.publish(map[string]uint64{"a:1": 1, "b:1": 1}, nil, map[string]chq.Status{"a:1": st(1), "b:1": st(1)})
	a1, a2, b := c.Changed("a", 1), c.Changed("a", 1), c.Changed("b", 1)
	c.publish(map[string]uint64{"a:1": 2, "b:1": 1}, nil, map[string]chq.Status{"a:1": st(2), "b:1": st(1)})
	for _, ch := range []<-chan struct{}{a1, a2} {
		select {
		case <-ch:
		default:
			t.Fatal("a new data version must wake every stream of that store")
		}
	}
	select {
	case <-b:
		t.Fatal("an unchanged store's streams must stay asleep")
	default:
	}
	select {
	case <-c.Changed("a", 1):
		t.Fatal("after waking, a store's next signal must wait for its next change")
	default:
	}
}

// With no new data, a store's streams wake only when its pipeline health
// changes, including when the clock alone makes it "stalled" while ClickHouse
// cannot be polled.
func TestRespCacheWakesOnHealthChange(t *testing.T) {
	c := NewRespCache(100, time.Minute, time.Second)
	now := time.Now()
	c.now = func() time.Time { return now }
	checked := now.Add(-20 * time.Second)
	st := chq.Status{Found: true, SnapshotState: "completed", DataVersion: 1, StreamCheckedAt: &checked}
	c.publish(map[string]uint64{"a:1": 1}, nil, map[string]chq.Status{"a:1": st})
	quiet := c.Changed("a", 1)
	c.publish(map[string]uint64{"a:1": 1}, nil, map[string]chq.Status{"a:1": st}) // same data, still live
	select {
	case <-quiet:
		t.Fatal("no new data and unchanged health must not wake the stream")
	default:
	}
	now = now.Add(15 * time.Second) // last confirmation now 35 s old
	c.publish(nil, nil, nil)        // the poll failed: only the clock moved
	select {
	case <-quiet:
	default:
		t.Fatal("a pipeline that stopped confirming must wake its streams as stalled")
	}
	if h := pipelineHealth(st, now); h != "stalled" {
		t.Fatalf("health=%s, want stalled", h)
	}
}

// Views nobody reads are dropped; a view being read, or still loading, stays.
func TestViewsEvictedWhenIdle(t *testing.T) {
	b := &ViewBackend{Idle: 15 * time.Minute}
	stale := &registry.Tenant{ID: "a", ActiveGeneration: 1, Timezone: "UTC"}
	read := &registry.Tenant{ID: "b", ActiveGeneration: 1, Timezone: "UTC"}
	loading := &registry.Tenant{ID: "c", ActiveGeneration: 1, Timezone: "UTC"}
	for _, tn := range []*registry.Tenant{stale, read, loading} {
		b.get(tn)
	}
	old := time.Now().Add(-20 * time.Minute).UnixNano()
	for _, id := range []string{"a", "c"} {
		si, _ := b.views.Load(id)
		si.(*viewSlot).used.Store(old)
	}
	si, _ := b.views.Load("c")
	b.syncs.Store(si.(*viewSlot).view, &viewSync{done: make(chan struct{})})

	if kept, dropped := b.evict(time.Now()); kept != 2 || dropped != 1 {
		t.Fatalf("kept=%d dropped=%d, want 2 and 1", kept, dropped)
	}
	if _, ok := b.views.Load("a"); ok {
		t.Fatal("an idle view must be dropped")
	}
	if v := b.get(stale); v == nil || v.Loaded() {
		t.Fatal("a dropped view comes back empty on the next read and loads again")
	}
}

func TestRespCacheSweepDropsExpired(t *testing.T) {
	c := NewRespCache(100, time.Minute, 5*time.Second)
	c.Put("old", 1, answer{body: []byte("x")})
	c.Put("new", 1, answer{body: []byte("y")})
	c.mu.Lock()
	e := c.entries["old"]
	e.at = time.Now().Add(-2 * time.Minute)
	c.entries["old"] = e
	c.mu.Unlock()
	c.sweep(time.Now())
	if _, _, _, ok := c.Get("old", 1, 0); ok {
		t.Fatal("an expired answer must be swept")
	}
	if _, _, _, ok := c.Get("new", 1, 0); !ok {
		t.Fatal("a fresh answer must stay")
	}
}

func TestRespCacheKeepsSource(t *testing.T) {
	c := NewRespCache(100, time.Minute, 5*time.Second)
	c.Put("k", 3, answer{[]byte("v3"), "view"})
	if a, _, _, ok := c.Get("k", 3, 0); !ok || a.source != "view" {
		t.Fatalf("source lost: ok=%v source=%q", ok, a.source)
	}
}
