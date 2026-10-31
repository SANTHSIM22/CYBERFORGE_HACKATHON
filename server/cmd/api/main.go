// Command api serves the tenant dashboard REST API (ClickHouse only).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"tenantpulse/internal/api"
	"tenantpulse/internal/auth"
	"tenantpulse/internal/ch"
	"tenantpulse/internal/config"
	"tenantpulse/internal/logging"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/registry"
)

func main() {
	logging.Setup("api")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("api exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	direct := config.String("API_BACKEND", "clickhouse") == "direct"
	var backend api.Backend
	var chMon driver.Conn
	if direct {
		// Benchmark-only baseline: dashboard queries hit each store's primary.
		backend = &api.DirectBackend{DSN: config.MustString("SOURCE_READ_DSN"), MaxConns: int32(config.Int("DIRECT_POOL_PER_TENANT", 16))}
		slog.Warn("API_BACKEND=direct: every dashboard request queries the store primary (benchmark baseline, not for production)")
	} else {
		chAPI, err := ch.Connect(ctx, config.MustString("CLICKHOUSE_API_DSN"), config.Int("API_CH_POOL", 32))
		if err != nil {
			return err
		}
		defer chAPI.Close()
		backend = api.ClickHouseBackend{CH: chAPI}
		if chMon, err = ch.Connect(ctx, config.MustString("CLICKHOUSE_MONITOR_DSN"), 4); err != nil {
			return err
		}
		defer chMon.Close()
	}
	control, err := pg.Connect(ctx, config.MustString("CONTROL_API_DSN"), pg.PoolOptions{MaxConns: 4, AppName: "tp-api"})
	if err != nil {
		return err
	}
	defer control.Close()

	reg := registry.New(control)
	if err := reg.Refresh(ctx); err != nil {
		return err
	}
	go reg.Run(ctx, 5*time.Second)

	signer, err := auth.NewSigner(config.MustString("AUTH_SECRET"))
	if err != nil {
		return err
	}
	cache := api.NewRespCache(config.Int("API_CACHE_ITEMS", 20000), config.Duration("API_CACHE_TTL", 60*time.Second), config.Duration("API_CACHE_STALE", 5*time.Second))
	queryTimeout := config.Duration("API_QUERY_TIMEOUT", 5*time.Second)
	if !direct {
		go cache.PollVersions(ctx, chMon, config.Duration("API_VERSION_POLL", 250*time.Millisecond))
		if config.Bool("API_VIEWS", true) { // in-memory rollup views; ClickHouse answers the rest
			vb := &api.ViewBackend{ClickHouseBackend: backend.(api.ClickHouseBackend), Version: cache.Version, Timeout: queryTimeout,
				Wait: config.Duration("API_VIEW_WAIT", time.Second)}
			backend = vb
			go vb.Prewarm(ctx, reg.Current().Ordered, config.Int("API_VIEW_PREWARM", 4))
		}
	}
	srv := &api.Server{
		B: backend, Direct: direct, Mon: chMon, Reg: reg, Cache: cache, Signer: signer,
		DevAuth:      config.String("AUTH_MODE", "dev") == "dev",
		Limiter:      api.NewLimiter(config.Int("API_DB_CONCURRENCY", 24), config.Int("API_TENANT_CONCURRENCY", 4), config.Duration("API_LIMIT_WAIT", time.Second)),
		Win:          &api.Window{},
		QueryTimeout: queryTimeout,
	}
	hs := &http.Server{
		Addr:              config.String("API_ADDR", ":8080"),
		Handler:           srv.Router(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	slog.Info("api listening", "addr", hs.Addr, "tenants", len(reg.Current().Ordered), "dev_auth", srv.DevAuth)
	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}
