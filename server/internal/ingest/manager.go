package ingest

import (
	"context"
	"hash/fnv"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Manager runs one Stream per tenant generation assigned to this ingester.
// Scale-out: run N ingesters with INGESTER_INDEX=0..N-1 and INGESTER_COUNT=N;
// tenants are partitioned by hash, so each slot has exactly one reader.
type Manager struct {
	Control *pgxpool.Pool
	CH      driver.Conn
	Sink    *Sink
	ReplDSN string // tp_replicator on the source cluster (database swapped per tenant)
	Index   int
	Count   int

	sem     chan struct{}
	mu      sync.Mutex
	running map[string]*handle
	started time.Time
}

type handle struct {
	st     *Stream
	cancel context.CancelFunc
}

func NewManager(control *pgxpool.Pool, chConn driver.Conn, sink *Sink, replDSN string, index, count, maxSnapshots int) *Manager {
	return &Manager{Control: control, CH: chConn, Sink: sink, ReplDSN: replDSN, Index: index, Count: max(count, 1),
		sem: make(chan struct{}, max(maxSnapshots, 1)), running: map[string]*handle{}, started: time.Now()}
}

func (m *Manager) dsn(db string, replication bool) string {
	u, _ := url.Parse(m.ReplDSN)
	u.Path = "/" + db
	q := u.Query()
	q.Set("timezone", "UTC")
	q.Set("application_name", "tp-ingester")
	if replication {
		q.Set("replication", "database")
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func (m *Manager) owns(tenantID string) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(tenantID))
	return int(h.Sum32()%uint32(m.Count)) == m.Index
}

type genRow struct {
	tenantID, slug, db, slot, pub, desired, tz string
	gen, active                                int64
	snapshotDone                               bool
}

func (m *Manager) Run(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	statsEvery := 0
	for {
		if err := m.reconcile(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("reconcile streams", "err", err)
		}
		if statsEvery%2 == 0 {
			m.publishStats(ctx)
		}
		statsEvery++
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Manager) reconcile(ctx context.Context) error {
	rows, err := m.Control.Query(ctx, `
		SELECT g.tenant_id::text, t.tenant_slug, t.source_database, g.replication_slot_name, g.publication_name,
		       g.desired_state, g.generation, t.active_generation, g.snapshot_done, t.timezone
		FROM tenant_generations g JOIN tenants t USING (tenant_id)
		WHERE g.state NOT IN ('retired', 'failed')`)
	if err != nil {
		return err
	}
	gens, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (genRow, error) {
		var g genRow
		return g, r.Scan(&g.tenantID, &g.slug, &g.db, &g.slot, &g.pub, &g.desired, &g.gen, &g.active, &g.snapshotDone, &g.tz)
	})
	if err != nil {
		return err
	}
	want := map[string]genRow{}
	for _, g := range gens {
		if g.desired == "running" && m.owns(g.tenantID) {
			want[g.tenantID+":"+strconv.FormatInt(g.gen, 10)] = g
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, h := range m.running {
		if _, ok := want[key]; !ok {
			h.cancel()
			delete(m.running, key)
			slog.Info("stream stopped", "tenant", h.st.Slug, "generation", h.st.Gen)
		}
	}
	for key, g := range want {
		if h, ok := m.running[key]; ok {
			h.st.Serving = g.gen == g.active
			continue
		}
		loc, err := time.LoadLocation(g.tz)
		if err != nil {
			loc = time.UTC
		}
		st := &Stream{Tenant: uuid.MustParse(g.tenantID), Slug: g.slug, DB: g.db, Slot: g.slot, Publication: g.pub,
			Gen: uint32(g.gen), Loc: loc, Serving: g.gen == g.active, m: m}
		st.v.DataVersion = uint64(time.Now().UnixMicro()) // never reuse a version after a restart
		sctx, cancel := context.WithCancel(ctx)
		m.running[key] = &handle{st: st, cancel: cancel}
		go m.supervise(sctx, st)
	}
	return nil
}

// supervise restarts a stream with bounded exponential backoff. Restarting is
// always safe: replication resumes at the slot's confirmed position and
// replayed rows collapse in ReplacingMergeTree.
func (m *Manager) supervise(ctx context.Context, st *Stream) {
	backoff := time.Second
	for ctx.Err() == nil {
		var done bool
		err := m.Control.QueryRow(ctx, `SELECT snapshot_done FROM tenant_generations WHERE tenant_id = $1 AND generation = $2`,
			st.Tenant, int64(st.Gen)).Scan(&done)
		if err == nil {
			_, _ = m.Control.Exec(ctx, `UPDATE tenant_generations SET observed_state = 'running', last_error = NULL WHERE tenant_id = $1 AND generation = $2`, st.Tenant, int64(st.Gen))
			started := time.Now()
			err = st.Run(ctx, done)
			if time.Since(started) > time.Minute {
				backoff = time.Second
			}
		}
		if ctx.Err() != nil {
			return
		}
		slog.Error("stream failed; restarting", "tenant", st.Slug, "generation", st.Gen, "err", err, "backoff", backoff)
		msg := err.Error()
		_, _ = m.Control.Exec(context.Background(), `UPDATE tenant_generations SET observed_state = 'error', last_error = $3 WHERE tenant_id = $1 AND generation = $2`,
			st.Tenant, int64(st.Gen), msg)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (m *Manager) setTenantStatus(ctx context.Context, st *Stream, tenantStatus, genState string) {
	if !st.Serving {
		return
	}
	if tenantStatus == "active" {
		_, _ = m.Control.Exec(ctx, `UPDATE tenants SET status = 'active', updated_at = now() WHERE tenant_id = $1 AND status IN ('provisioning', 'snapshotting')`, st.Tenant)
		_, _ = m.Control.Exec(ctx, `UPDATE tenant_generations SET state = 'active', completed_at = now() WHERE tenant_id = $1 AND generation = $2 AND state IN ('building', 'catching_up')`, st.Tenant, int64(st.Gen))
		slog.Info("tenant active: snapshot loaded and stream caught up", "tenant", st.Slug)
		return
	}
	_, _ = m.Control.Exec(ctx, `UPDATE tenants SET status = $2, updated_at = now() WHERE tenant_id = $1 AND status = 'provisioning'`, st.Tenant, tenantStatus)
}

func (m *Manager) publishStats(ctx context.Context) {
	m.mu.Lock()
	streams := make([]*Stream, 0, len(m.running))
	for _, h := range m.running {
		streams = append(streams, h.st)
	}
	m.mu.Unlock()

	// Retained WAL per slot: the key safety metric when ClickHouse is slow.
	var maxLag uint64
	if conn, err := pgx.Connect(ctx, m.dsn("postgres", false)); err == nil {
		rows, err := conn.Query(ctx, `SELECT slot_name, COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
		                              FROM pg_replication_slots WHERE slot_type = 'logical'`)
		if err == nil {
			lag := map[string]uint64{}
			for rows.Next() {
				var s string
				var l int64
				if rows.Scan(&s, &l) == nil && l > 0 {
					lag[s] = uint64(l)
				}
			}
			rows.Close()
			for _, st := range streams {
				st.mu.Lock()
				st.v.SlotLagBytes = lag[st.Slot]
				st.mu.Unlock()
				maxLag = max(maxLag, lag[st.Slot])
			}
		}
		conn.Close(ctx)
	}

	ev := m.Sink.events.Load()
	p50, p95, p99 := m.Sink.Percentiles()
	host, _ := os.Hostname()
	worker := host + "#" + strconv.Itoa(m.Index)
	b, err := m.CH.PrepareBatch(ctx, `INSERT INTO tp.ingester_stats (worker, started_at, events_total, events_per_sec, flush_p50_ms, flush_p95_ms,
		flush_p99_ms, streams, snapshots_running, max_slot_lag_bytes, updated_at, _version)`)
	if err != nil {
		return
	}
	rate := m.rate(ev)
	now := time.Now().UTC()
	_ = b.Append(worker, m.started.UTC(), ev, rate, p50, p95, p99, uint32(len(streams)), uint32(len(m.sem)), maxLag, now, uint64(now.UnixMicro()))
	_ = b.Send()
}

var (
	rateMu   sync.Mutex
	rateLast uint64
	rateAt   time.Time
)

func (m *Manager) rate(ev uint64) float64 {
	rateMu.Lock()
	defer rateMu.Unlock()
	now := time.Now()
	r := 0.0
	if !rateAt.IsZero() {
		r = float64(ev-rateLast) / now.Sub(rateAt).Seconds()
	}
	rateLast, rateAt = ev, now
	return r
}
