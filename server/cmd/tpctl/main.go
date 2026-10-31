// Command tpctl is the TenantPulse operator CLI: schemas, additive tenant
// provisioning, data generation, exact reconciliation against source SQL,
// rebuilds and benchmarks. It runs inside the Compose network
// (docker compose run --rm tpctl ...), so no host tooling is needed.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	_ "time/tzdata"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"

	"tenantpulse/internal/ch"
	"tenantpulse/internal/config"
	"tenantpulse/internal/logging"
	"tenantpulse/internal/pg"
	"tenantpulse/internal/provision"
	"tenantpulse/migrations"
)

const usage = `tpctl — TenantPulse operator CLI

  init                                   apply control-plane + ClickHouse schemas, register local cluster/shard
  tenant add --slug S [--name --tier --tz --currency --cdc running|deferred]
  tenant add-batch --count N [--cdc ...]  add N new tenants (never removes any)
  tenant ensure --count N [--cdc ...]     add tenants until at least N exist (monotonic)
  tenant list | tenant status --slug S
  migrate-source-all                      apply pending source migrations to every tenant DB (sequentially)
  capture pause-all|resume-all            pause/resume change capture (for OLTP impact runs)
  seed --tenants N --rows R [--days 365] [--workers 4]
  seed-additive --slug S --rows R         add rows to a live tenant (streams through capture)
  wait-active [--timeout 30m]
  verify [--slug S]                       compare ClickHouse answers with direct source SQL (exact)
  rebuild --slug S                        shadow-generation rebuild + verify + atomic cutover (B-2)
  ladder --slug S                         read-latency ladder: no index / index / covering index / ClickHouse
  stress [--vus 20 --duration 30s]       direct-primary vs aggregation instances under load + store impact
  write-storm [--rate 1000 --viewers 100] every store writing hard while dashboards watch (pipeline off/on, direct)
  bench-publish                           show the newest saved stress/ladder results on the dashboard
  oltp --duration 60s --workers 8 [--tenants 10] [--label x]
  freshness --slug S [--samples 30]
`

func main() {
	logging.Setup("tpctl")
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := dispatch(ctx, os.Args[1], os.Args[2:]); err != nil {
		slog.Error("tpctl failed", "cmd", strings.Join(os.Args[1:], " "), "err", err)
		os.Exit(1)
	}
}

type env struct {
	control *pgxpool.Pool
	chAPI   driver.Conn // tenant-scoped reads (row policies), same path as the API
	prov    *provision.Provisioner
}

func open(ctx context.Context) (*env, error) {
	control, err := pg.Connect(ctx, config.MustString("CONTROL_OWNER_DSN"), pg.PoolOptions{MaxConns: 8, AppName: "tpctl"})
	if err != nil {
		return nil, err
	}
	chAPI, err := ch.Connect(ctx, config.MustString("CLICKHOUSE_API_DSN"), 8)
	if err != nil {
		return nil, err
	}
	return &env{control: control, chAPI: chAPI,
		prov: &provision.Provisioner{Control: control, Actor: config.String("TP_ACTOR", "tpctl")}}, nil
}

func (e *env) close() { e.control.Close(); _ = e.chAPI.Close() }

func dispatch(ctx context.Context, cmd string, args []string) error {
	switch cmd {
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	case "init":
		return initCmd(ctx)
	case "bench-publish":
		return benchPublishCmd(ctx, args)
	}
	e, err := open(ctx)
	if err != nil {
		return err
	}
	defer e.close()
	switch cmd {
	case "tenant":
		if len(args) == 0 {
			return fmt.Errorf("tenant: missing subcommand")
		}
		return tenantCmd(ctx, e, args[0], args[1:])
	case "migrate-source-all":
		return migrateSourceAll(ctx, e)
	case "capture":
		if len(args) == 0 {
			return fmt.Errorf("capture: missing subcommand")
		}
		return captureCmd(ctx, e, args[0])
	case "seed":
		return seedCmd(ctx, e, args)
	case "seed-additive":
		return seedAdditiveCmd(ctx, e, args)
	case "wait-active":
		return waitActive(ctx, e, args)
	case "verify":
		return verifyCmd(ctx, e, args)
	case "rebuild":
		return rebuildCmd(ctx, e, args)
	case "ladder":
		return ladderCmd(ctx, e, args)
	case "oltp":
		return oltpCmd(ctx, e, args)
	case "stress":
		return stressCmd(ctx, e, args)
	case "write-storm":
		return writeStormCmd(ctx, e, args)
	case "freshness":
		return freshnessCmd(ctx, e, args)
	}
	return fmt.Errorf("unknown command %q\n%s", cmd, usage)
}

func initCmd(ctx context.Context) error {
	v, err := pg.Migrate(ctx, config.MustString("CONTROL_OWNER_DSN"), migrations.Control, "control")
	if err != nil {
		return fmt.Errorf("control migrations: %w", err)
	}
	slog.Info("control plane migrated", "version", v)
	admin, err := ch.Connect(ctx, config.MustString("CLICKHOUSE_ADMIN_DSN"), 2)
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := ch.ApplySchema(ctx, admin, migrations.ClickHouseSchema); err != nil {
		return fmt.Errorf("clickhouse schema: %w", err)
	}
	slog.Info("clickhouse schema applied")
	control, err := pg.Connect(ctx, config.MustString("CONTROL_OWNER_DSN"), pg.PoolOptions{MaxConns: 2})
	if err != nil {
		return err
	}
	defer control.Close()
	return (&provision.Provisioner{Control: control}).EnsureDefaults(ctx)
}

func flags(name string, args []string, def func(fs *flag.FlagSet)) (*flag.FlagSet, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	def(fs)
	return fs, fs.Parse(args)
}
