package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"tenantpulse/internal/config"
	"tenantpulse/internal/stress"
)

// publishBench stores a benchmark result in ClickHouse so the dashboard and
// /monitor can show it. Failures are reported but never fail the benchmark.
func publishBench(ctx context.Context, kind string, at time.Time, payload []byte) {
	runID, err := stress.Publish(ctx, config.MustString("CLICKHOUSE_INGEST_DSN"), kind, at, payload)
	if err != nil {
		fmt.Fprintln(os.Stderr, "publish benchmark:", err)
		return
	}
	fmt.Printf("published %s to the dashboard (run %s)\n", kind, runID)
}

// benchPublishCmd publishes the newest saved stress and ladder results from
// RESULTS_DIR (e.g. runs made before publishing existed).
func benchPublishCmd(ctx context.Context, args []string) error {
	var dir string
	if _, err := flags("bench-publish", args, func(fs *flag.FlagSet) {
		fs.StringVar(&dir, "dir", config.String("RESULTS_DIR", "/results"), "results directory")
	}); err != nil {
		return err
	}
	for _, kind := range []string{"stress", "ladder"} {
		files, _ := filepath.Glob(filepath.Join(dir, kind+"-*.json"))
		if len(files) == 0 {
			fmt.Printf("no %s results in %s\n", kind, dir)
			continue
		}
		sort.Strings(files)
		latest := files[len(files)-1]
		b, err := os.ReadFile(latest)
		if err != nil {
			return err
		}
		stamp := strings.TrimSuffix(filepath.Base(latest), ".json")
		stamp = stamp[len(stamp)-len("20060102-150405"):]
		at, err := time.Parse("20060102-150405", stamp)
		if err != nil {
			at = time.Now()
		}
		publishBench(ctx, kind, at, b)
	}
	return nil
}
