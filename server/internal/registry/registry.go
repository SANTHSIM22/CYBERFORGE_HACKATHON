// Package registry is a periodically refreshed copy of the control-plane
// tenant registry (slug/UUID -> tenant, timezone, active generation).
package registry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type Tenant struct {
	ID               string
	Slug             string
	Name             string
	Tier             string
	SourceClusterID  string
	SourceDatabase   string
	SourceEndpoint   string // secret ref of the source cluster
	AnalyticsShardID string
	Timezone         string
	Loc              *time.Location
	DefaultCurrency  string
	Status           string
	ActiveGeneration int64
	Org              *Org // nil: the store belongs to no org
}

// OrgSlug is the slug of the store's org, "" when it belongs to none.
func (t *Tenant) OrgSlug() string {
	if t.Org == nil {
		return ""
	}
	return t.Org.Slug
}

// Org groups stores under one owner, whose admins see a consolidated dashboard.
type Org struct {
	ID                string
	Slug              string
	Name              string
	ReportingCurrency string
	Stores            []*Tenant // by slug
}

// FX holds the fixed conversion rates for org dashboards.
type FX struct {
	UnitsPerUSD map[string]decimal.Decimal
	AsOf        string // YYYY-MM-DD of the oldest rate in use
}

type Snapshot struct {
	ByID       map[string]*Tenant
	BySlug     map[string]*Tenant
	Ordered    []*Tenant // by slug
	OrgsBySlug map[string]*Org
	Orgs       []*Org // by slug
	FX         FX
	Loaded     time.Time
}

type Registry struct {
	pool      *pgxpool.Pool
	snap      atomic.Pointer[Snapshot]
	refreshMu sync.Mutex
	lastForce time.Time
}

func New(pool *pgxpool.Pool) *Registry { return &Registry{pool: pool} }

func (r *Registry) Current() *Snapshot {
	if s := r.snap.Load(); s != nil {
		return s
	}
	return &Snapshot{ByID: map[string]*Tenant{}, BySlug: map[string]*Tenant{}, OrgsBySlug: map[string]*Org{}}
}

func (r *Registry) Refresh(ctx context.Context) error {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	return r.refreshLocked(ctx)
}

// RefreshIfStale forces a reload at most once per second; used when an
// unknown topic or tenant shows up before the periodic refresh.
func (r *Registry) RefreshIfStale(ctx context.Context) {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	if time.Since(r.lastForce) < time.Second {
		return
	}
	r.lastForce = time.Now()
	if err := r.refreshLocked(ctx); err != nil {
		slog.Warn("registry refresh failed", "err", err)
	}
}

func (r *Registry) refreshLocked(ctx context.Context) error {
	s := &Snapshot{ByID: map[string]*Tenant{}, BySlug: map[string]*Tenant{}, OrgsBySlug: map[string]*Org{}, Loaded: time.Now()}
	orgsByID := map[string]*Org{}
	orows, err := r.pool.Query(ctx, `SELECT org_id::text, org_slug, display_name, reporting_currency FROM orgs ORDER BY org_slug`)
	if err != nil {
		return fmt.Errorf("load orgs: %w", err)
	}
	for orows.Next() {
		o := &Org{}
		if err := orows.Scan(&o.ID, &o.Slug, &o.Name, &o.ReportingCurrency); err != nil {
			orows.Close()
			return err
		}
		o.ReportingCurrency = strings.TrimSpace(o.ReportingCurrency)
		orgsByID[o.ID], s.OrgsBySlug[o.Slug] = o, o
		s.Orgs = append(s.Orgs, o)
	}
	orows.Close()
	if err := orows.Err(); err != nil {
		return err
	}
	s.FX.UnitsPerUSD = map[string]decimal.Decimal{}
	frows, err := r.pool.Query(ctx, `SELECT currency, units_per_usd::text, as_of::text FROM fx_rates`)
	if err != nil {
		return fmt.Errorf("load fx rates: %w", err)
	}
	for frows.Next() {
		var cur, rate, asOf string
		if err := frows.Scan(&cur, &rate, &asOf); err != nil {
			frows.Close()
			return err
		}
		s.FX.UnitsPerUSD[strings.TrimSpace(cur)] = decimal.RequireFromString(rate)
		if s.FX.AsOf == "" || asOf < s.FX.AsOf {
			s.FX.AsOf = asOf
		}
	}
	frows.Close()
	if err := frows.Err(); err != nil {
		return err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT t.tenant_id::text, t.tenant_slug, t.display_name, t.size_tier, t.source_cluster_id::text,
		       t.source_database, c.endpoint_secret_ref, t.analytics_shard_id::text, t.timezone,
		       t.default_currency, t.status, t.active_generation, coalesce(t.org_id::text, '')
		FROM tenants t JOIN source_clusters c USING (source_cluster_id)
		ORDER BY t.tenant_slug`)
	if err != nil {
		return fmt.Errorf("load tenants: %w", err)
	}
	for rows.Next() {
		t := &Tenant{}
		var orgID string
		if err := rows.Scan(&t.ID, &t.Slug, &t.Name, &t.Tier, &t.SourceClusterID, &t.SourceDatabase, &t.SourceEndpoint,
			&t.AnalyticsShardID, &t.Timezone, &t.DefaultCurrency, &t.Status, &t.ActiveGeneration, &orgID); err != nil {
			rows.Close()
			return err
		}
		if o := orgsByID[orgID]; o != nil {
			t.Org = o
			o.Stores = append(o.Stores, t)
		}
		t.DefaultCurrency = strings.TrimSpace(t.DefaultCurrency)
		loc, err := time.LoadLocation(t.Timezone)
		if err != nil {
			slog.Error("tenant has invalid timezone; using UTC", "tenant", t.Slug, "tz", t.Timezone)
			loc = time.UTC
		}
		t.Loc = loc
		s.ByID[t.ID], s.BySlug[t.Slug] = t, t
		s.Ordered = append(s.Ordered, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	r.snap.Store(s)
	return nil
}

// Run refreshes on an interval until ctx ends.
func (r *Registry) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := r.Refresh(ctx); err != nil {
				slog.Warn("registry refresh failed", "err", err)
			}
		}
	}
}
