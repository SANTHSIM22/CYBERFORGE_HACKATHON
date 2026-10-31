// Command demo runs the DEMO_MODE-only live-demo service. It refuses to start
// unless DEMO_MODE=true, and it is the only runtime service (besides the
// provisioner) holding source-database credentials.
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

	"tenantpulse/internal/auth"
	"tenantpulse/internal/config"
	"tenantpulse/internal/demo"
	"tenantpulse/internal/logging"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/registry"
)

func main() {
	logging.Setup("demo")
	if !config.Bool("DEMO_MODE", false) {
		slog.Error("demo service disabled: set DEMO_MODE=true to enable the live demo endpoints")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("demo exited", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	control, err := pg.Connect(ctx, config.MustString("CONTROL_API_DSN"), pg.PoolOptions{MaxConns: 2, AppName: "tp-demo"})
	if err != nil {
		return err
	}
	defer control.Close()
	reg := registry.New(control)
	if err := reg.Refresh(ctx); err != nil {
		return err
	}
	go reg.Run(ctx, 10*time.Second)

	signer, err := auth.NewSigner(config.MustString("AUTH_SECRET"))
	if err != nil {
		return err
	}
	svc := &demo.Service{
		Reg:           reg,
		Signer:        signer,
		SourceDSN:     config.MustString("SOURCE_DEMO_DSN"),
		APIBase:       config.String("API_INTERNAL_URL", "http://api:8080"),
		OperatorToken: signer.Issue("operator"),
		HC:            &http.Client{Timeout: 70 * time.Second},
		Stress: &demo.StressJob{
			AppDSN:    config.MustString("SOURCE_APP_DSN"),
			DirectURL: config.String("API_DIRECT_URL", "http://api-direct:8080"),
			APIURL:    config.String("API_INTERNAL_URL", "http://api:8080"),
			IngestDSN: config.MustString("CLICKHOUSE_INGEST_DSN"),
		},
	}
	hs := &http.Server{Addr: config.String("DEMO_ADDR", ":8081"), Handler: svc.Router(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- hs.ListenAndServe() }()
	slog.Info("demo service listening (DEMO_MODE=true)", "addr", hs.Addr)
	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return hs.Shutdown(sctx)
}
