package api

import (
	"context"
	"testing"
	"time"
)

func TestRespCacheStaleWhileRevalidate(t *testing.T) {
	c := NewRespCache(100, time.Minute, 5*time.Second)
	c.Put("k", 7, []byte("v7"))

	if b, at, fresh, ok := c.Get("k", 7, 0); !ok || !fresh || at != 7 || string(b) != "v7" {
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
	c.Put("k", 9, []byte("v9"))
	c.Put("k", 8, []byte("v8"))
	if b, at, fresh, _ := c.Get("k", 9, 0); !fresh || at != 9 || string(b) != "v9" {
		t.Fatalf("newer entry replaced: at=%d body=%s", at, b)
	}
}

func TestRespCacheStaleWindowExpires(t *testing.T) {
	c := NewRespCache(100, time.Minute, 5*time.Second)
	c.Put("k", 1, []byte("v1"))
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
	disabled.Put("k", 1, []byte("v1"))
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
	w.Observe(now, 0.1, "HIT")
	w.Observe(now, 0.2, "STALE")
	w.Observe(now, 20, "MISS")
	w.Observe(now, 400, "BYPASS")
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
		w.Observe(now.Add(-time.Second), 0.5, "HIT")
	}
	for range 20 {
		w.Observe(now, 300, "MISS")
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
