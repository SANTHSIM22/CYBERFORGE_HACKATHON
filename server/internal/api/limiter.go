package api

import (
	"context"
	"sync"
	"time"
)

// Limiter bounds concurrent analytics-database queries per tenant and
// globally (B-3). Cache hits never take a slot. Background cache refreshes
// additionally pass a gate sized at half the slots, so at least half of the
// capacity is always left for reads a user is waiting on. Single-process
// semaphores; multiple API replicas would switch to control-plane leases (ADR 0006).
type Limiter struct {
	global, globalBG chan struct{}
	perTenant        int
	wait             time.Duration
	mu               sync.Mutex
	tenants          map[string]*tenantSlots
}

type tenantSlots struct{ all, bg chan struct{} }

func NewLimiter(global, perTenant int, wait time.Duration) *Limiter {
	return &Limiter{global: make(chan struct{}, global), globalBG: make(chan struct{}, max(1, global/2)),
		perTenant: perTenant, wait: wait, tenants: map[string]*tenantSlots{}}
}

func (l *Limiter) tenant(id string) *tenantSlots {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts, ok := l.tenants[id]
	if !ok {
		ts = &tenantSlots{all: make(chan struct{}, l.perTenant), bg: make(chan struct{}, max(1, l.perTenant/2))}
		l.tenants[id] = ts
	}
	return ts
}

// Acquire returns a release func, or ok=false when the tenant (or the whole
// API) is at capacity for longer than the configured wait.
func (l *Limiter) Acquire(ctx context.Context, tenantID string) (release func(), ok bool) {
	ctx, cancel := context.WithTimeout(ctx, l.wait)
	defer cancel()
	return take(ctx, l.tenant(tenantID).all, l.global)
}

// AcquireBackground is Acquire for work nobody is waiting on (cache
// refreshes): it first takes a background slot, so background work never
// holds more than half of the tenant's or the API's slots. It waits as long
// as ctx allows.
func (l *Limiter) AcquireBackground(ctx context.Context, tenantID string) (release func(), ok bool) {
	ts := l.tenant(tenantID)
	gate, ok := take(ctx, ts.bg, l.globalBG)
	if !ok {
		return nil, false
	}
	slot, ok := take(ctx, ts.all, l.global)
	if !ok {
		gate()
		return nil, false
	}
	return func() { slot(); gate() }, true
}

// take acquires one slot from each semaphore in order, or none.
func take(ctx context.Context, sems ...chan struct{}) (release func(), ok bool) {
	for i, s := range sems {
		select {
		case s <- struct{}{}:
		case <-ctx.Done():
			for _, h := range sems[:i] {
				<-h
			}
			return nil, false
		}
	}
	return func() {
		for _, s := range sems {
			<-s
		}
	}, true
}
