package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"tenantpulse/internal/auth"
	"tenantpulse/internal/config"
	"tenantpulse/internal/stress"
)

// stressCmd runs the direct-vs-aggregation stress test (internal/stress) and
// publishes the result to the dashboard.
func stressCmd(ctx context.Context, e *env, args []string) error {
	var dur, warm time.Duration
	var vus, oltpRate, oltpWorkers int
	var directURL, apiURL, phases string
	var pinned bool
	if _, err := flags("stress", args, func(fs *flag.FlagSet) {
		fs.DurationVar(&dur, "duration", 30*time.Second, "measured time per phase")
		fs.DurationVar(&warm, "warmup", 5*time.Second, "unmeasured warm-up per phase")
		fs.IntVar(&vus, "vus", 20, "concurrent dashboard users (closed loop, no think time)")
		fs.IntVar(&oltpRate, "oltp-rate", 150, "store operations per second during every phase")
		fs.IntVar(&oltpWorkers, "oltp-workers", 8, "store workload clients")
		fs.StringVar(&directURL, "direct-url", "http://api-direct:8080", "instance that queries the primary DB")
		fs.StringVar(&apiURL, "api-url", "http://api:8080", "aggregation-layer instance")
		fs.StringVar(&phases, "phases", "baseline,direct,aggregation,clickhouse", "phases to run")
		fs.BoolVar(&pinned, "cpu-pinned", false, "record that the run used dedicated CPU sets")
	}); err != nil {
		return err
	}
	ts, err := activeTenants(ctx, e, "")
	if err != nil {
		return err
	}
	signer, err := auth.NewSigner(config.MustString("AUTH_SECRET"))
	if err != nil {
		return err
	}
	cfg := stress.Config{AppDSN: config.MustString("SOURCE_APP_DSN"), Tokens: map[string]string{}, Operator: signer.Issue("operator"),
		DirectURL: directURL, APIURL: apiURL, VUs: vus, Duration: dur, Warmup: warm, OLTPRate: oltpRate, OLTPWorkers: oltpWorkers,
		Phases: phases, Source: "cli", CPUPinning: pinned}
	for _, t := range ts {
		cfg.Tenants = append(cfg.Tenants, stress.Tenant{Slug: t.slug, Database: t.db, Currency: t.cur})
		cfg.Tokens[t.slug] = signer.Issue("tenant:" + t.slug)
	}
	last := -1
	res, err := stress.Run(ctx, cfg, func(p stress.Progress) {
		if p.PhaseIndex != last && p.Stage != "done" {
			last = p.PhaseIndex
			fmt.Printf("▶ [%d/%d] %s\n", p.PhaseIndex+1, p.PhaseCount, p.Phase)
		}
	})
	if err != nil {
		return err
	}
	report := stress.Report(res)
	fmt.Println("\n" + report)
	b, _ := json.MarshalIndent(res, "", "  ")
	saveResult("stress", b)
	if dir := config.String("RESULTS_DIR", ""); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "stress-"+time.Now().UTC().Format("20060102-150405")+".md"), []byte(report), 0o644)
	}
	publishBench(ctx, "stress", res.At, b)
	return nil
}

// writeStormCmd runs the write storm (internal/stress): every store writing
// at a high rate while dashboards watch, first with the analytics pipeline
// paused (pure store baseline), then on, then with dashboards on the primaries.
func writeStormCmd(ctx context.Context, e *env, args []string) error {
	var dur, warm time.Duration
	var rate, workers, viewers int
	var directURL, apiURL string
	var pipelineOff, pinned bool
	if _, err := flags("write-storm", args, func(fs *flag.FlagSet) {
		fs.DurationVar(&dur, "duration", 30*time.Second, "measured time per phase")
		fs.DurationVar(&warm, "warmup", 5*time.Second, "unmeasured warm-up per phase")
		fs.IntVar(&rate, "rate", 1000, "target store transactions per second, all stores together")
		fs.IntVar(&workers, "workers", 48, "store clients")
		fs.IntVar(&viewers, "viewers", 100, "concurrent dashboard viewers")
		fs.StringVar(&directURL, "direct-url", "http://api-direct:8080", "instance that queries the primary DB (\"\" skips that phase)")
		fs.StringVar(&apiURL, "api-url", "http://api:8080", "aggregation-layer instance")
		fs.BoolVar(&pipelineOff, "pipeline-off-baseline", true, "first phase with change capture paused (pure store baseline + catch-up time)")
		fs.BoolVar(&pinned, "cpu-pinned", false, "record that the run used dedicated CPU sets")
	}); err != nil {
		return err
	}
	ts, err := activeTenants(ctx, e, "")
	if err != nil {
		return err
	}
	signer, err := auth.NewSigner(config.MustString("AUTH_SECRET"))
	if err != nil {
		return err
	}
	cfg := stress.WriteConfig{AppDSN: config.MustString("SOURCE_APP_DSN"), Tokens: map[string]string{}, Operator: signer.Issue("operator"),
		APIURL: apiURL, DirectURL: directURL, Rate: rate, Workers: workers, Viewers: viewers, Duration: dur, Warmup: warm,
		Source: "cli", CPUPinning: pinned}
	for _, t := range ts {
		cfg.Tenants = append(cfg.Tenants, stress.Tenant{Slug: t.slug, Database: t.db, Currency: t.cur})
		cfg.Tokens[t.slug] = signer.Issue("tenant:" + t.slug)
	}
	if pipelineOff {
		set := func(state string) func(context.Context) error {
			return func(ctx context.Context) error {
				if _, err := e.control.Exec(ctx, `UPDATE tenant_generations SET desired_state = $1
					WHERE desired_state IN ('running', 'paused') AND state NOT IN ('retired', 'failed')`, state); err != nil {
					return err
				}
				time.Sleep(3 * time.Second) // the ingester applies desired_state within ~2 s
				return nil
			}
		}
		cfg.PauseCapture, cfg.ResumeCapture = set("paused"), set("running")
	}
	last := -1
	res, err := stress.RunWrites(ctx, cfg, func(p stress.Progress) {
		if p.PhaseIndex != last && p.Stage != "done" {
			last = p.PhaseIndex
			fmt.Printf("▶ [%d/%d] %s\n", p.PhaseIndex+1, p.PhaseCount, p.Phase)
		}
	})
	if err != nil {
		if cfg.ResumeCapture != nil { // never leave capture paused
			_ = cfg.ResumeCapture(context.WithoutCancel(ctx))
		}
		return err
	}
	report := stress.WriteReport(res)
	fmt.Println("\n" + report)
	b, _ := json.MarshalIndent(res, "", "  ")
	saveResult("writestorm", b)
	if dir := config.String("RESULTS_DIR", ""); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "writestorm-"+time.Now().UTC().Format("20060102-150405")+".md"), []byte(report), 0o644)
	}
	publishBench(ctx, "writestorm", res.At, b)
	return nil
}
