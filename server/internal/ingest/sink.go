package ingest

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	promEvents = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tenantpulse_events_processed_total", Help: "Row changes written to ClickHouse."})
	promFlush = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "tenantpulse_ingest_flush_duration_seconds", Help: "Time to write one batch to ClickHouse.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14)})
	promFlushErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tenantpulse_ingest_flush_failures_total", Help: "Failed ClickHouse batch writes (retried)."})
)

// Txn is one committed source transaction's changes for one stream.
type Txn struct {
	Orders     []OrderRow
	Items      []ItemRow
	Products   []ProductRow
	CommitTime time.Time
	EndLSN     pglogrepl.LSN
	Heartbeat  bool // touched tp_heartbeat (watermark)
	DataRows   int
	Cells      Cells // rollup cells this transaction touched
}

// Sink buffers committed transactions from every stream and writes them to
// ClickHouse in one INSERT per table per flush (few, large parts — what
// ClickHouse wants), then advances each stream's acknowledgeable LSN.
type Sink struct {
	CH       driver.Conn
	Interval time.Duration
	MaxRows  int

	mu       sync.Mutex
	orders   []OrderRow
	items    []ItemRow
	products []ProductRow
	acks     map[*Stream]pglogrepl.LSN
	dirty    map[*Stream]bool
	cells    map[*Stream]*Cells
	rows     int

	kick    chan struct{}
	events  atomic.Uint64
	statsMu sync.Mutex
	durs    []float64
}

func NewSink(ch driver.Conn, interval time.Duration, maxRows int) *Sink {
	return &Sink{CH: ch, Interval: interval, MaxRows: maxRows, acks: map[*Stream]pglogrepl.LSN{},
		dirty: map[*Stream]bool{}, cells: map[*Stream]*Cells{}, kick: make(chan struct{}, 1)}
}

// Add enqueues one committed transaction. It applies backpressure when the
// buffer is far above the flush size (ClickHouse slow or down): the stream
// stops reading, so unacknowledged WAL stays safely in the slot.
func (s *Sink) Add(ctx context.Context, st *Stream, tx *Txn) {
	for {
		s.mu.Lock()
		if s.rows < s.MaxRows*8 {
			break
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	s.orders = append(s.orders, tx.Orders...)
	s.items = append(s.items, tx.Items...)
	s.products = append(s.products, tx.Products...)
	n := len(tx.Orders) + len(tx.Items) + len(tx.Products)
	s.rows += n
	s.acks[st] = tx.EndLSN
	s.dirty[st] = true
	if !tx.Cells.empty() {
		c := s.cells[st]
		if c == nil {
			c = &Cells{}
			s.cells[st] = c
		}
		c.merge(tx.Cells)
	}
	st.noteTxn(tx) // under s.mu: pending markers stay aligned with buffered rows
	full := s.rows >= s.MaxRows
	s.mu.Unlock()
	if full {
		select {
		case s.kick <- struct{}{}:
		default:
		}
	}
}

// MarkDirty schedules a status row for a stream (keepalive progress).
func (s *Sink) MarkDirty(st *Stream) {
	s.mu.Lock()
	s.dirty[st] = true
	s.mu.Unlock()
}

func (s *Sink) Run(ctx context.Context) {
	t := time.NewTicker(s.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.flush(context.Background())
			return
		case <-t.C:
		case <-s.kick:
		}
		s.flush(ctx)
	}
}

func (s *Sink) flush(ctx context.Context) {
	s.mu.Lock()
	orders, items, products := s.orders, s.items, s.products
	acks, dirty, cells := s.acks, s.dirty, s.cells
	if len(dirty) == 0 && len(orders)+len(items)+len(products) == 0 {
		s.mu.Unlock()
		return
	}
	pend := make(map[*Stream]pending, len(dirty))
	for st := range dirty {
		pend[st] = st.takePending()
	}
	s.orders, s.items, s.products, s.rows = nil, nil, nil, 0
	s.acks, s.dirty, s.cells = map[*Stream]pglogrepl.LSN{}, map[*Stream]bool{}, map[*Stream]*Cells{}
	s.mu.Unlock()

	start := time.Now()
	backoff := 100 * time.Millisecond
	for {
		err := s.write(ctx, orders, items, products)
		if err == nil && len(cells) > 0 {
			// Rollups are refreshed from the raw rows just written, before the
			// new data version is published, so cached answers are never stale.
			sets := make([]streamCells, 0, len(cells))
			for st, c := range cells {
				sets = append(sets, streamCells{st, *c})
			}
			err = RefreshRollups(ctx, s.CH, sets, uint64(time.Now().UnixMicro()))
		}
		if err == nil {
			break
		}
		promFlushErrors.Inc()
		slog.Warn("clickhouse write failed; retrying (WAL stays in the slots until acknowledged)", "rows", len(orders)+len(items)+len(products), "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
	n := len(orders) + len(items) + len(products)
	s.events.Add(uint64(n))
	promEvents.Add(float64(n))

	// Rows are in ClickHouse: publish freshness, then let streams acknowledge.
	version := uint64(time.Now().UnixMicro())
	now := time.Now().UTC()
	for st, p := range pend {
		st.markApplied(p, now, version)
	}
	if err := s.writeStatus(ctx, dirty, version); err != nil {
		slog.Warn("write tenant status", "err", err)
	}
	for st, lsn := range acks {
		st.setFlushed(lsn)
	}
	if n > 0 {
		d := time.Since(start)
		promFlush.Observe(d.Seconds())
		s.statsMu.Lock()
		s.durs = append(s.durs, float64(d.Microseconds())/1000)
		if len(s.durs) > 2048 {
			s.durs = s.durs[len(s.durs)-2048:]
		}
		s.statsMu.Unlock()
	}
}

func (s *Sink) write(ctx context.Context, orders []OrderRow, items []ItemRow, products []ProductRow) error {
	if err := InsertOrders(ctx, s.CH, orders); err != nil {
		return err
	}
	if err := InsertItems(ctx, s.CH, items); err != nil {
		return err
	}
	return InsertProducts(ctx, s.CH, products)
}

func InsertOrders(ctx context.Context, conn driver.Conn, rows []OrderRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, `INSERT INTO tp.orders (tenant_id, generation, order_id, created_at, updated_at, status, currency, total_amount, source_version, is_deleted, _version)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(r.Tenant, r.Gen, r.ID, r.CreatedAt, r.UpdatedAt, r.Status, r.Currency, r.Total, r.SrcVersion, r.Deleted, r.Version); err != nil {
			return err
		}
	}
	return b.Send()
}

func InsertItems(ctx context.Context, conn driver.Conn, rows []ItemRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, `INSERT INTO tp.order_items (tenant_id, generation, item_id, order_id, product_id, created_at, quantity, unit_price, source_version, is_deleted, _version)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(r.Tenant, r.Gen, r.ID, r.OrderID, r.ProductID, r.CreatedAt, r.Quantity, r.UnitPrice, r.SrcVersion, r.Deleted, r.Version); err != nil {
			return err
		}
	}
	return b.Send()
}

func InsertProducts(ctx context.Context, conn driver.Conn, rows []ProductRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, `INSERT INTO tp.products (tenant_id, generation, product_id, sku, name, is_active, source_version, is_deleted, _version)`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(r.Tenant, r.Gen, r.ID, r.SKU, r.Name, r.IsActive, r.SrcVersion, r.Deleted, r.Version); err != nil {
			return err
		}
	}
	return b.Send()
}

func (s *Sink) writeStatus(ctx context.Context, streams map[*Stream]bool, version uint64) error {
	if len(streams) == 0 {
		return nil
	}
	b, err := s.CH.PrepareBatch(ctx, `INSERT INTO tp.tenant_status (tenant_id, generation, snapshot_state, last_source_commit_at,
		last_heartbeat_source_at, last_applied_at, stream_checked_at, last_apply_lag_ms, confirmed_lsn, slot_lag_bytes, events_total, data_version, _version)`)
	if err != nil {
		return err
	}
	for st := range streams {
		v := st.status()
		if err := b.Append(st.Tenant, st.Gen, v.SnapshotState, v.LastCommit, v.LastHeartbeat, v.LastApplied, v.StreamChecked,
			v.LagMs, uint64(v.Confirmed), v.SlotLagBytes, v.Events, v.DataVersion, version); err != nil {
			return err
		}
	}
	return b.Send()
}

func (s *Sink) Percentiles() (p50, p95, p99 float64) {
	s.statsMu.Lock()
	c := append([]float64(nil), s.durs...)
	s.statsMu.Unlock()
	if len(c) == 0 {
		return
	}
	sort.Float64s(c)
	at := func(q float64) float64 { return c[min(len(c)-1, int(q*float64(len(c))))] }
	return at(.5), at(.95), at(.99)
}
