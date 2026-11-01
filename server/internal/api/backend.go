package api

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"tenantpulse/internal/chq"
	"tenantpulse/internal/directsql"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/registry"
)

// Backend answers the dashboard questions. Production uses ClickHouse; the
// "direct" backend exists only to benchmark the alternative this project
// replaces: querying each store's primary Postgres on every dashboard request.
type Backend interface {
	Name() string
	Ping(ctx context.Context) error
	Summary(ctx context.Context, t *registry.Tenant, p chq.Params) (cur, prev []chq.CurrencyTotals, err error)
	StatusRows(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.StatusRow, error)
	Trend(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.TrendPoint, error)
	Top(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.TopProduct, error)
	Currencies(ctx context.Context, t *registry.Tenant) ([]string, error)
}

// sourceKey carries a *string through a read's context; the backend that
// answers records itself there ("view", "clickhouse" or "direct").
type sourceKey struct{}

func withSource(ctx context.Context) (context.Context, *string) {
	src := new(string)
	return context.WithValue(ctx, sourceKey{}, src), src
}

func setSource(ctx context.Context, source string) {
	if src, ok := ctx.Value(sourceKey{}).(*string); ok {
		*src = source
	}
}

// ---------------------------------------------------------------- ClickHouse

type ClickHouseBackend struct{ CH driver.Conn }

func (b ClickHouseBackend) Name() string                   { return "clickhouse" }
func (b ClickHouseBackend) Ping(ctx context.Context) error { return b.CH.Ping(ctx) }
func (b ClickHouseBackend) Summary(ctx context.Context, _ *registry.Tenant, p chq.Params) ([]chq.CurrencyTotals, []chq.CurrencyTotals, error) {
	setSource(ctx, "clickhouse")
	return chq.Summary(ctx, b.CH, p)
}
func (b ClickHouseBackend) StatusRows(ctx context.Context, _ *registry.Tenant, p chq.Params) ([]chq.StatusRow, error) {
	setSource(ctx, "clickhouse")
	return chq.StatusRows(ctx, b.CH, p)
}
func (b ClickHouseBackend) Trend(ctx context.Context, _ *registry.Tenant, p chq.Params) ([]chq.TrendPoint, error) {
	setSource(ctx, "clickhouse")
	return chq.Trend(ctx, b.CH, p)
}
func (b ClickHouseBackend) Top(ctx context.Context, _ *registry.Tenant, p chq.Params) ([]chq.TopProduct, error) {
	setSource(ctx, "clickhouse")
	return chq.TopProducts(ctx, b.CH, p)
}
func (b ClickHouseBackend) Currencies(ctx context.Context, t *registry.Tenant) ([]string, error) {
	return chq.Currencies(ctx, b.CH, t.ID, t.ActiveGeneration)
}
func (b ClickHouseBackend) Conn() driver.Conn { return b.CH }

// ---------------------------------------------------------------- in-memory views over ClickHouse

// ViewBackend answers from per-tenant in-memory views (chq.View), synced to
// each store's data version with one small delta read per change, and falls
// back to ClickHouse for anything a view cannot answer exactly.
type ViewBackend struct {
	ClickHouseBackend
	Version func(tenantID string, gen int64) uint64
	Timeout time.Duration
	Wait    time.Duration // longest a reader waits for a view to catch up before ClickHouse answers
	views   sync.Map      // tenant ID -> *chq.View (active generation)
	syncs   sync.Map      // *chq.View -> *viewSync in flight
}

type noViewKey struct{}

// WithoutView makes a read skip the views: cache-bypass benchmark reads
// measure ClickHouse itself.
func WithoutView(ctx context.Context) context.Context {
	return context.WithValue(ctx, noViewKey{}, true)
}

func (b *ViewBackend) get(t *registry.Tenant) *chq.View {
	vi, ok := b.views.Load(t.ID)
	if !ok || vi.(*chq.View).Gen != t.ActiveGeneration { // first use, or a rebuild cut over
		vi = chq.NewView(t.ID, t.ActiveGeneration, t.Timezone)
		b.views.Store(t.ID, vi)
	}
	return vi.(*chq.View)
}

type viewSync struct {
	done chan struct{}
	err  error
}

// startSync runs (or joins) a background sync of v. It never runs on a
// request's context, so a slow first load cannot use up a reader's time budget.
func (b *ViewBackend) startSync(v *chq.View, version uint64) *viewSync {
	s := &viewSync{done: make(chan struct{})}
	if cur, loaded := b.syncs.LoadOrStore(v, s); loaded {
		return cur.(*viewSync)
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute) // first loads of the largest stores
		defer cancel()
		s.err = v.Sync(ctx, b.CH, version, time.Minute)
		b.syncs.Delete(v)
		close(s.done)
	}()
	return s
}

// view returns the store's view if it is current. A reader waits briefly for
// a sync of new changes (normally a few ms) but never for a first load: then
// it is answered from ClickHouse while the view loads in the background.
func (b *ViewBackend) view(ctx context.Context, t *registry.Tenant) *chq.View {
	if ctx.Value(noViewKey{}) != nil {
		return nil
	}
	v := b.get(t)
	version := b.Version(t.ID, t.ActiveGeneration)
	if v.Current(version) {
		return v
	}
	s := b.startSync(v, version)
	wait := b.Wait
	if !v.Loaded() {
		wait = min(wait, 150*time.Millisecond)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-s.done:
		if s.err != nil {
			slog.Warn("view sync failed; querying ClickHouse", "tenant", t.Slug, "err", s.err)
			return nil
		}
		if v.Current(version) {
			return v
		}
	case <-timer.C:
	case <-ctx.Done():
	}
	return nil
}

// Prewarm loads every active store's view in the background, a few at a
// time, so the first viewer of each store finds it ready.
func (b *ViewBackend) Prewarm(ctx context.Context, tenants []*registry.Tenant, parallel int) {
	sem := make(chan struct{}, max(1, parallel))
	for _, t := range tenants {
		if t.Status != "active" {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		go func() {
			defer func() { <-sem }()
			s := b.startSync(b.get(t), b.Version(t.ID, t.ActiveGeneration))
			select {
			case <-s.done:
			case <-ctx.Done():
			}
		}()
	}
}

func (b *ViewBackend) Summary(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.CurrencyTotals, []chq.CurrencyTotals, error) {
	if v := b.view(ctx, t); v != nil {
		if cur, prev, ok := v.Summary(p); ok {
			setSource(ctx, "view")
			return cur, prev, nil
		}
	}
	return b.ClickHouseBackend.Summary(ctx, t, p)
}

func (b *ViewBackend) StatusRows(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.StatusRow, error) {
	if v := b.view(ctx, t); v != nil {
		if rows, ok := v.StatusRows(p); ok {
			setSource(ctx, "view")
			return rows, nil
		}
	}
	return b.ClickHouseBackend.StatusRows(ctx, t, p)
}

func (b *ViewBackend) Trend(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.TrendPoint, error) {
	if v := b.view(ctx, t); v != nil {
		if pts, ok := v.Trend(p); ok {
			setSource(ctx, "view")
			return pts, nil
		}
	}
	return b.ClickHouseBackend.Trend(ctx, t, p)
}

func (b *ViewBackend) Top(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.TopProduct, error) {
	if v := b.view(ctx, t); v != nil {
		if top, ok := v.TopProducts(p); ok {
			setSource(ctx, "view")
			return top, nil
		}
	}
	return b.ClickHouseBackend.Top(ctx, t, p)
}

// ---------------------------------------------------------------- direct (benchmark only)

type DirectBackend struct {
	DSN      string // read role on the source cluster; database swapped per tenant
	MaxConns int32
	mu       sync.Mutex
	pools    map[string]*pgxpool.Pool
}

func (b *DirectBackend) Name() string               { return "direct-primary" }
func (b *DirectBackend) Ping(context.Context) error { return nil }
func (b *DirectBackend) pool(ctx context.Context, t *registry.Tenant) (*pgxpool.Pool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p, ok := b.pools[t.ID]; ok {
		return p, nil
	}
	dsn, err := pg.WithDatabase(b.DSN, t.SourceDatabase)
	if err != nil {
		return nil, err
	}
	p, err := pg.Connect(ctx, dsn, pg.PoolOptions{MaxConns: b.MaxConns, AppName: "tp-api-direct"})
	if err != nil {
		return nil, err
	}
	if b.pools == nil {
		b.pools = map[string]*pgxpool.Pool{}
	}
	b.pools[t.ID] = p
	return p, nil
}

func (b *DirectBackend) Summary(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.CurrencyTotals, []chq.CurrencyTotals, error) {
	pool, err := b.pool(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	return directsql.Summary(ctx, pool, p.From.Add(-p.To.Sub(p.From)), p.From, p.To)
}

func (b *DirectBackend) StatusRows(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.StatusRow, error) {
	pool, err := b.pool(ctx, t)
	if err != nil {
		return nil, err
	}
	return directsql.StatusRows(ctx, pool, p.From, p.To, p.Currency)
}

func (b *DirectBackend) Trend(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.TrendPoint, error) {
	pool, err := b.pool(ctx, t)
	if err != nil {
		return nil, err
	}
	return directsql.Trend(ctx, pool, p.From, p.To, t.Timezone, p.Grain, p.Currency)
}

func (b *DirectBackend) Top(ctx context.Context, t *registry.Tenant, p chq.Params) ([]chq.TopProduct, error) {
	pool, err := b.pool(ctx, t)
	if err != nil {
		return nil, err
	}
	return directsql.TopProducts(ctx, pool, p.From, p.To, p.Currency, p.Limit)
}

func (b *DirectBackend) Currencies(ctx context.Context, t *registry.Tenant) ([]string, error) {
	pool, err := b.pool(ctx, t)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := pool.Query(ctx, `SELECT DISTINCT currency::text FROM orders ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, strings.TrimSpace(c))
	}
	return out, rows.Err()
}
