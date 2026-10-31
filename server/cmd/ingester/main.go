// Command ingester captures tenant database changes (logical replication,
// pgoutput) and writes them to ClickHouse. Scale out by running several with
// INGESTER_INDEX / INGESTER_COUNT; each tenant slot has exactly one reader.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"tenantpulse/internal/ch"
	"tenantpulse/internal/config"
	"tenantpulse/internal/ingest"
	"tenantpulse/internal/logging"
	"tenantpulse/internal/pg"
)

func main() {
	logging.Setup("ingester")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("ingester exited", "err", err)
		os.Exit(1)
	}
	slog.Info("ingester stopped")
}

func run(ctx context.Context) error {
	chConn, err := ch.Connect(ctx, config.MustString("CLICKHOUSE_INGEST_DSN"), config.Int("INGEST_CH_POOL", 16))
	if err != nil {
		return err
	}
	defer chConn.Close()
	control, err := pg.Connect(ctx, config.MustString("CONTROL_INGEST_DSN"), pg.PoolOptions{MaxConns: 8, AppName: "tp-ingester"})
	if err != nil {
		return err
	}
	defer control.Close()

	sink := ingest.NewSink(chConn, config.Duration("INGEST_FLUSH_INTERVAL", time.Second), config.Int("INGEST_FLUSH_ROWS", 50000))
	sinkCtx, stopSink := context.WithCancel(context.Background())
	sinkDone := make(chan struct{})
	go func() { sink.Run(sinkCtx); close(sinkDone) }()

	mgr := ingest.NewManager(control, chConn, sink, config.MustString("SOURCE_REPL_DSN"),
		config.Int("INGESTER_INDEX", 0), config.Int("INGESTER_COUNT", 1), config.Int("MAX_CONCURRENT_SNAPSHOTS", 2))

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if err := chConn.Ping(r.Context()); err != nil {
			http.Error(w, `{"status":"clickhouse unavailable"}`, 503)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	hs := &http.Server{Addr: config.String("INGEST_ADDR", ":9100"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = hs.ListenAndServe() }()
	defer hs.Close()

	slog.Info("ingester running", "index", config.Int("INGESTER_INDEX", 0), "count", config.Int("INGESTER_COUNT", 1))
	mgr.Run(ctx)
	// Graceful stop: streams are cancelled; flush what is buffered (its WAL is
	// re-sent on restart anyway, so this only shortens the replay).
	stopSink()
	<-sinkDone
	return nil
}
