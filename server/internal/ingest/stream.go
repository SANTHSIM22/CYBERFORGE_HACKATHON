package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/shopspring/decimal"
)

// Stream replicates one tenant database (one generation) into ClickHouse.
type Stream struct {
	Tenant      uuid.UUID
	Slug        string
	DB          string
	Slot        string
	Publication string
	Gen         uint32
	Loc         *time.Location // tenant timezone: day rollups are tenant-local days
	Serving     bool           // this generation is the tenant's active one

	m        *Manager
	flushed  atomic.Uint64 // highest commit end LSN durably in ClickHouse
	enqueued atomic.Uint64 // highest commit end LSN handed to the sink

	mu      sync.Mutex
	v       StatusValues
	pendCom *time.Time
	pendHB  *time.Time
	pendDat bool
	// data-version coalescing
	lastBump    time.Time
	bumpPending bool
	recent      map[uuid.UUID]recentRow // id -> created_at (+ parent order for items)
}

type StatusValues struct {
	SnapshotState string
	LastCommit    *time.Time
	LastHeartbeat *time.Time
	LastApplied   *time.Time
	StreamChecked *time.Time
	LagMs         *int64
	Confirmed     pglogrepl.LSN
	SlotLagBytes  uint64
	Events        uint64
	DataVersion   uint64
}

func (st *Stream) status() StatusValues {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.v
}

// noteTxn is called by the sink under its lock, so pending values always
// belong to rows that are in the same flush or a later one.
func (st *Stream) noteTxn(tx *Txn) {
	st.mu.Lock()
	defer st.mu.Unlock()
	t := tx.CommitTime.UTC()
	if tx.DataRows > 0 {
		st.pendCom, st.pendDat = &t, true
	}
	if tx.Heartbeat {
		st.pendHB = &t
	}
	st.v.Events += uint64(tx.DataRows)
	st.enqueued.Store(uint64(tx.EndLSN))
}

type pending struct {
	commit, heartbeat *time.Time
	data              bool
}

// takePending is called when the sink swaps buffers: it captures exactly the
// transactions whose rows are in that flush.
func (st *Stream) takePending() pending {
	st.mu.Lock()
	defer st.mu.Unlock()
	p := pending{st.pendCom, st.pendHB, st.pendDat}
	st.pendCom, st.pendHB, st.pendDat = nil, nil, false
	return p
}

// markApplied publishes freshness after the flush's rows are in ClickHouse.
func (st *Stream) markApplied(p pending, now time.Time, version uint64) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.v.LastApplied = &now
	if p.commit != nil {
		st.v.LastCommit = p.commit
		lag := now.Sub(*p.commit).Milliseconds()
		st.v.LagMs = &lag
	}
	if p.heartbeat != nil {
		st.v.LastHeartbeat = p.heartbeat
	}
	if p.data {
		st.bumpPending = true
	}
	if st.bumpPending && now.Sub(st.lastBump) >= versionEvery {
		st.v.DataVersion = version
		st.lastBump, st.bumpPending = now, false
	}
}

// versionEvery bounds how often a store's data version (the API cache key)
// changes; a pending bump is published by the next flush or keepalive after it.
var versionEvery = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("INGEST_VERSION_EVERY")); err == nil && d >= 0 {
		return d
	}
	return time.Second
}()

func (st *Stream) setFlushed(lsn pglogrepl.LSN) {
	if uint64(lsn) > st.flushed.Load() {
		st.flushed.Store(uint64(lsn))
	}
}

func (st *Stream) caughtUp() bool { return st.flushed.Load() >= st.enqueued.Load() }

func (st *Stream) setState(s string) {
	st.mu.Lock()
	st.v.SnapshotState = s
	st.mu.Unlock()
	st.m.Sink.MarkDirty(st)
}

type recentRow struct {
	created time.Time
	parent  uuid.UUID // order id, for items
}

func (st *Stream) remember(id uuid.UUID, created time.Time, parent uuid.UUID) {
	st.mu.Lock()
	if st.recent == nil || len(st.recent) > 200_000 {
		st.recent = make(map[uuid.UUID]recentRow, 1024)
	}
	st.recent[id] = recentRow{created, parent}
	st.mu.Unlock()
}

func (st *Stream) recall(id uuid.UUID) (time.Time, uuid.UUID, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	r, ok := st.recent[id]
	return r.created, r.parent, ok
}

// Run snapshots (once per generation) and then streams until ctx ends or the
// connection fails; the manager restarts it with backoff.
func (st *Stream) Run(ctx context.Context, snapshotDone bool) error {
	conn, err := pgconn.Connect(ctx, st.m.dsn(st.DB, true))
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer conn.Close(context.Background())
	if !snapshotDone {
		if err := st.snapshot(ctx, conn); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
	} else {
		st.setState("streaming")
	}
	return st.stream(ctx, conn)
}

func (st *Stream) snapshot(ctx context.Context, repl *pgconn.PgConn) error {
	m := st.m
	// A previous attempt may have died midway: drop its slot and its rows,
	// otherwise rows deleted in between could survive as ghosts.
	if err := pglogrepl.DropReplicationSlot(ctx, repl, st.Slot, pglogrepl.DropReplicationSlotOptions{Wait: true}); err != nil &&
		!strings.Contains(err.Error(), "does not exist") {
		return fmt.Errorf("drop stale slot: %w", err)
	}
	for _, t := range []string{"orders", "order_items", "products", "order_q_rollup", "product_q_rollup", "order_day_rollup", "order_hour_rollup", "product_day_rollup", "product_month_rollup"} {
		var n uint64
		if err := m.CH.QueryRow(ctx, "SELECT count() FROM tp."+t+" WHERE tenant_id = ? AND generation = ?", st.Tenant, st.Gen).Scan(&n); err != nil {
			return fmt.Errorf("check partial generation: %w", err)
		}
		if n == 0 {
			continue
		}
		slog.Warn("removing rows of an interrupted snapshot", "tenant", st.Slug, "table", t, "rows", n)
		wait := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": 2}))
		if err := m.CH.Exec(wait, "ALTER TABLE tp."+t+" DELETE WHERE tenant_id = ? AND generation = ?", st.Tenant, st.Gen); err != nil {
			return fmt.Errorf("clear partial generation: %w", err)
		}
	}

	select { // bounded concurrent snapshots per source cluster
	case m.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-m.sem }()
	st.setState("running")
	m.setTenantStatus(ctx, st, "snapshotting", "snapshotting")
	start := time.Now()

	res, err := pglogrepl.CreateReplicationSlot(ctx, repl, st.Slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return fmt.Errorf("create slot: %w", err)
	}
	src, err := pgx.Connect(ctx, m.dsn(st.DB, false))
	if err != nil {
		return err
	}
	defer src.Close(context.Background())
	tx, err := src.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// The exported snapshot is exactly the slot's consistent point: the copy
	// plus the stream that follows see every committed change exactly once.
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+res.SnapshotName+"'"); err != nil {
		return fmt.Errorf("use exported snapshot: %w", err)
	}
	n, err := st.copyTables(ctx, tx)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if err := RefreshRollups(ctx, m.CH, []streamCells{{st, Cells{Full: true}}}, uint64(time.Now().UnixMicro())); err != nil {
		return fmt.Errorf("build rollups: %w", err)
	}
	if _, err := m.Control.Exec(ctx, `UPDATE tenant_generations SET snapshot_done = true, snapshot_rows = $3, state = 'catching_up', last_error = NULL
	                                  WHERE tenant_id = $1 AND generation = $2`, st.Tenant, int64(st.Gen), n); err != nil {
		return err
	}
	slog.Info("snapshot copied", "tenant", st.Slug, "generation", st.Gen, "rows", n, "took", time.Since(start).Round(time.Millisecond),
		"rows_per_sec", int(float64(n)/time.Since(start).Seconds()))
	st.setState("streaming")
	return nil
}

const snapshotBatch = 100_000

func (st *Stream) copyTables(ctx context.Context, tx pgx.Tx) (int64, error) {
	var total int64
	ch := st.m.CH
	// products
	rows, err := tx.Query(ctx, `SELECT id, sku, name, is_active, version FROM products`)
	if err != nil {
		return 0, err
	}
	var ps []ProductRow
	for rows.Next() {
		r := ProductRow{Tenant: st.Tenant, Gen: st.Gen}
		var active bool
		var ver int64
		if err := rows.Scan(&r.ID, &r.SKU, &r.Name, &active, &ver); err != nil {
			return 0, err
		}
		if active {
			r.IsActive = 1
		}
		r.SrcVersion = uint64(ver)
		ps = append(ps, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := InsertProducts(ctx, ch, ps); err != nil {
		return 0, err
	}
	total += int64(len(ps))

	// orders
	rows, err = tx.Query(ctx, `SELECT id, created_at, updated_at, status, currency::text, total_amount::text, version, deleted_at IS NOT NULL FROM orders`)
	if err != nil {
		return 0, err
	}
	os := make([]OrderRow, 0, snapshotBatch)
	for rows.Next() {
		r := OrderRow{Tenant: st.Tenant, Gen: st.Gen}
		var amount, cur string
		var ver int64
		var del bool
		if err := rows.Scan(&r.ID, &r.CreatedAt, &r.UpdatedAt, &r.Status, &cur, &amount, &ver, &del); err != nil {
			return 0, err
		}
		r.Currency, r.Total, r.SrcVersion = strings.TrimSpace(cur), decimal.RequireFromString(amount), uint64(ver)
		r.CreatedAt, r.UpdatedAt = r.CreatedAt.UTC(), r.UpdatedAt.UTC()
		if del {
			r.Deleted = 1
		}
		os = append(os, r)
		if len(os) == snapshotBatch {
			if err := InsertOrders(ctx, ch, os); err != nil {
				return 0, err
			}
			total += int64(len(os))
			os = os[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := InsertOrders(ctx, ch, os); err != nil {
		return 0, err
	}
	total += int64(len(os))

	// order items
	rows, err = tx.Query(ctx, `SELECT id, order_id, product_id, created_at, quantity::text, unit_price::text, version, deleted_at IS NOT NULL FROM order_items`)
	if err != nil {
		return 0, err
	}
	is := make([]ItemRow, 0, snapshotBatch)
	for rows.Next() {
		r := ItemRow{Tenant: st.Tenant, Gen: st.Gen}
		var q, p string
		var ver int64
		var del bool
		if err := rows.Scan(&r.ID, &r.OrderID, &r.ProductID, &r.CreatedAt, &q, &p, &ver, &del); err != nil {
			return 0, err
		}
		r.CreatedAt = r.CreatedAt.UTC()
		r.Quantity, r.UnitPrice, r.SrcVersion = decimal.RequireFromString(q), decimal.RequireFromString(p), uint64(ver)
		if del {
			r.Deleted = 1
		}
		is = append(is, r)
		if len(is) == snapshotBatch {
			if err := InsertItems(ctx, ch, is); err != nil {
				return 0, err
			}
			total += int64(len(is))
			is = is[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if err := InsertItems(ctx, ch, is); err != nil {
		return 0, err
	}
	total += int64(len(is))
	return total, nil
}

type pendingDelete struct {
	table   string
	id      uuid.UUID
	version uint64
}

func (st *Stream) stream(ctx context.Context, conn *pgconn.PgConn) error {
	err := pglogrepl.StartReplication(ctx, conn, st.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + st.Publication + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	slog.Info("streaming", "tenant", st.Slug, "generation", st.Gen, "slot", st.Slot)
	rels := map[uint32]*pglogrepl.RelationMessage{}
	var tx *Txn
	var deletes []pendingDelete
	var idleEnd pglogrepl.LSN
	inTxn := false
	activated := false
	nextStatus := time.Now()

	for {
		if !time.Now().Before(nextStatus) {
			ack := pglogrepl.LSN(st.flushed.Load())
			if !inTxn && st.caughtUp() && idleEnd > ack {
				ack = idleEnd // nothing pending: let an idle tenant's slot advance (no WAL build-up)
			}
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: ack, WALFlushPosition: ack, WALApplyPosition: ack, ClientTime: time.Now(), ReplyRequested: true,
			}); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}
			st.mu.Lock()
			st.v.Confirmed = ack
			st.mu.Unlock()
			nextStatus = time.Now().Add(time.Second)
		}
		rctx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return err
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("replication error: %s", msg.Message)
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				if !inTxn && st.caughtUp() {
					// Everything up to ServerWALEnd is in ClickHouse: the tenant's
					// data is current as of the server's clock.
					idleEnd = pkm.ServerWALEnd
					t := pkm.ServerTime.UTC()
					st.mu.Lock()
					st.v.StreamChecked = &t
					st.mu.Unlock()
					st.m.Sink.MarkDirty(st)
					if !activated {
						activated = true
						st.setState("completed")
						if st.Serving {
							st.m.setTenantStatus(ctx, st, "active", "active")
						}
					}
				}
				if pkm.ReplyRequested {
					nextStatus = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}
				lm, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("parse pgoutput: %w", err)
				}
				version := uint64(xld.WALStart)
				switch m := lm.(type) {
				case *pglogrepl.RelationMessage:
					rels[m.RelationID] = m
				case *pglogrepl.BeginMessage:
					inTxn, tx, deletes = true, &Txn{CommitTime: m.CommitTime}, deletes[:0]
				case *pglogrepl.InsertMessage:
					if err := st.applyTuple(tx, rels[m.RelationID], m.Tuple, version); err != nil {
						return err
					}
				case *pglogrepl.UpdateMessage:
					if err := st.applyTuple(tx, rels[m.RelationID], m.NewTuple, version); err != nil {
						return err
					}
				case *pglogrepl.DeleteMessage:
					rel := rels[m.RelationID]
					if rel == nil {
						continue
					}
					t, err := decodeTuple(rel, m.OldTuple)
					if err != nil {
						return err
					}
					id, err := t.uuid("id")
					if err != nil {
						continue // tp_heartbeat has an integer key; nothing to tombstone
					}
					deletes = append(deletes, pendingDelete{rel.RelationName, id, version})
				case *pglogrepl.TruncateMessage:
					slog.Error("TRUNCATE on a captured table is not supported; rebuild this tenant (tpctl rebuild)", "tenant", st.Slug)
				case *pglogrepl.CommitMessage:
					if tx == nil {
						continue
					}
					if err := st.resolveDeletes(ctx, tx, deletes); err != nil {
						return err
					}
					if err := st.collectCells(ctx, tx); err != nil {
						return err
					}
					tx.EndLSN = m.TransactionEndLSN
					st.m.Sink.Add(ctx, st, tx)
					inTxn, tx = false, nil
				}
			}
		}
	}
}

// collectCells marks the 15-minute buckets a transaction touches: the order's
// creation time for order changes, the parent order's for item changes
// (product revenue is bucketed by order creation time).
func (st *Stream) collectCells(ctx context.Context, tx *Txn) error {
	created := map[uuid.UUID]time.Time{}
	for _, o := range tx.Orders {
		created[o.ID] = o.CreatedAt
		tx.Cells.addOrderTime(o.CreatedAt, st.Loc)
	}
	var need []string
	pending := map[uuid.UUID]struct{}{}
	for _, it := range tx.Items {
		if it.OrderID == uuid.Nil {
			continue
		}
		if t, ok := created[it.OrderID]; ok {
			tx.Cells.addOrderTime(t, st.Loc)
		} else if t, _, ok := st.recall(it.OrderID); ok {
			tx.Cells.addOrderTime(t, st.Loc)
		} else if _, dup := pending[it.OrderID]; !dup {
			pending[it.OrderID] = struct{}{}
			need = append(need, it.OrderID.String())
		}
	}
	if len(need) == 0 {
		return nil
	}
	// Bulk loads (e.g. seed-additive) insert order_items in a separate
	// transaction from their parent orders, and the in-memory recall cache is
	// bounded, so one transaction can need thousands of parent lookups.
	// ClickHouse's default max_query_size is 256 KiB and a quoted UUID is ~39
	// bytes, so an unbounded IN-list overflows it and wedges the replication
	// slot (the same batch is re-read on every restart). Look up parents in
	// fixed-size batches instead.
	const parentBatch = 4000 // ~156 KiB of UUIDs, well under the 256 KiB limit
	for start := 0; start < len(need); start += parentBatch {
		end := start + parentBatch
		if end > len(need) {
			end = len(need)
		}
		rows, err := st.m.CH.Query(ctx, `SELECT order_id, any(created_at) FROM tp.orders WHERE tenant_id = ? AND generation = ? AND order_id IN (?) GROUP BY order_id`,
			st.Tenant, st.Gen, need[start:end])
		if err != nil {
			return fmt.Errorf("parent order lookup: %w", err)
		}
		for rows.Next() {
			var id uuid.UUID
			var t time.Time
			if err := rows.Scan(&id, &t); err != nil {
				rows.Close()
				return err
			}
			st.remember(id, t, uuid.Nil)
			tx.Cells.addOrderTime(t, st.Loc)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}
	return nil
}

func (st *Stream) applyTuple(tx *Txn, rel *pglogrepl.RelationMessage, td *pglogrepl.TupleData, version uint64) error {
	if rel == nil || tx == nil {
		return nil
	}
	t, err := decodeTuple(rel, td)
	if err != nil {
		return err
	}
	switch rel.RelationName {
	case "orders":
		r, err := orderFromTuple(t)
		if err != nil {
			return fmt.Errorf("orders: %w", err)
		}
		r.Tenant, r.Gen, r.Version = st.Tenant, st.Gen, version
		tx.Orders = append(tx.Orders, r)
		st.remember(r.ID, r.CreatedAt, uuid.Nil)
	case "order_items":
		r, err := itemFromTuple(t)
		if err != nil {
			return fmt.Errorf("order_items: %w", err)
		}
		r.Tenant, r.Gen, r.Version = st.Tenant, st.Gen, version
		tx.Items = append(tx.Items, r)
		st.remember(r.ID, r.CreatedAt, r.OrderID)
	case "products":
		r, err := productFromTuple(t)
		if err != nil {
			return fmt.Errorf("products: %w", err)
		}
		r.Tenant, r.Gen, r.Version = st.Tenant, st.Gen, version
		tx.Products = append(tx.Products, r)
	case "tp_heartbeat":
		tx.Heartbeat = true
		return nil
	default:
		return nil
	}
	tx.DataRows++
	return nil
}

// resolveDeletes turns hard deletes into tombstones. Default replica identity
// only sends the primary key, but the tombstone needs the row's sorting key
// (created_at): it comes from recently seen rows or a bloom-indexed lookup.
func (st *Stream) resolveDeletes(ctx context.Context, tx *Txn, ds []pendingDelete) error {
	if len(ds) == 0 {
		return nil
	}
	need := map[string][]uuid.UUID{}
	created := map[uuid.UUID]time.Time{}
	parents := map[uuid.UUID]uuid.UUID{} // item -> order, for rollup cells
	for _, it := range tx.Items {
		parents[it.ID] = it.OrderID
	}
	for _, d := range ds {
		if d.table == "products" {
			continue
		}
		if t, parent, ok := st.recall(d.id); ok && (d.table == "orders" || parent != uuid.Nil) {
			created[d.id] = t
			if parent != uuid.Nil {
				parents[d.id] = parent
			}
		} else {
			need[d.table] = append(need[d.table], d.id)
		}
	}
	for table, ids := range need {
		col := map[string]string{"orders": "order_id", "order_items": "item_id"}[table]
		if col == "" {
			continue
		}
		strs := make([]string, len(ids))
		for i, id := range ids {
			strs[i] = id.String()
		}
		parentCol := "order_id"
		// As above: keep the IN-list under ClickHouse's max_query_size.
		const lookupBatch = 4000
		for start := 0; start < len(strs); start += lookupBatch {
			end := start + lookupBatch
			if end > len(strs) {
				end = len(strs)
			}
			rows, err := st.m.CH.Query(ctx, fmt.Sprintf(`SELECT %s, any(created_at), any(%s) FROM tp.%s WHERE tenant_id = ? AND generation = ? AND %s IN (?) GROUP BY %s`, col, parentCol, table, col, col),
				st.Tenant, st.Gen, strs[start:end])
			if err != nil {
				return fmt.Errorf("tombstone lookup: %w", err)
			}
			for rows.Next() {
				var id, parent uuid.UUID
				var t time.Time
				if err := rows.Scan(&id, &t, &parent); err != nil {
					rows.Close()
					return err
				}
				created[id] = t
				parents[id] = parent
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		}
	}
	for _, d := range ds {
		switch d.table {
		case "products":
			tx.Products = append(tx.Products, ProductRow{Tenant: st.Tenant, Gen: st.Gen, ID: d.id, Deleted: 1, Version: d.version})
		case "orders":
			if t, ok := created[d.id]; ok {
				tx.Orders = append(tx.Orders, OrderRow{Tenant: st.Tenant, Gen: st.Gen, ID: d.id, CreatedAt: t, UpdatedAt: t, Deleted: 1, Version: d.version})
			}
		case "order_items":
			if t, ok := created[d.id]; ok {
				tx.Items = append(tx.Items, ItemRow{Tenant: st.Tenant, Gen: st.Gen, ID: d.id, OrderID: parents[d.id], CreatedAt: t, Deleted: 1, Version: d.version})
			}
		default:
			continue
		}
		tx.DataRows++
	}
	return nil
}
