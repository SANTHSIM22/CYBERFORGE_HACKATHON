// Package ch holds ClickHouse connection and schema helpers.
package ch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Connect opens a native-protocol connection pool, retrying until ClickHouse
// is reachable (it starts after the services that depend on it under Compose).
func Connect(ctx context.Context, dsn string, maxOpen int) (driver.Conn, error) {
	opt, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	opt.MaxOpenConns = maxOpen
	opt.MaxIdleConns = maxOpen
	opt.ConnMaxLifetime = 30 * time.Minute
	opt.DialTimeout = 5 * time.Second
	opt.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		conn, err := clickhouse.Open(opt)
		if err == nil {
			if err = conn.Ping(ctx); err == nil {
				return conn, nil
			}
			_ = conn.Close()
		}
		if attempt >= 40 || ctx.Err() != nil {
			return nil, fmt.Errorf("connect clickhouse: %w", err)
		}
		slog.Warn("clickhouse not ready, retrying", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// ApplySchema runs idempotent DDL statements, substituting {{ENV_NAME}}
// placeholders (user passwords) from the environment.
func ApplySchema(ctx context.Context, conn driver.Conn, schema string) error {
	for _, stmt := range strings.Split(schema, ";\n") {
		var lines []string
		for _, l := range strings.Split(stmt, "\n") {
			if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "--") {
				lines = append(lines, l)
			}
		}
		sql := strings.TrimSpace(strings.Join(lines, "\n"))
		if sql == "" {
			continue
		}
		for {
			i := strings.Index(sql, "{{")
			if i < 0 {
				break
			}
			j := strings.Index(sql[i:], "}}")
			name := sql[i+2 : i+j]
			v := os.Getenv(name)
			if v == "" {
				return fmt.Errorf("schema needs environment variable %s", name)
			}
			sql = sql[:i] + strings.ReplaceAll(v, "'", "") + sql[i+j+2:]
		}
		if err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("%s: %w", firstLine(sql), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return s[:i]
	}
	return s
}

var queryThreads = func() int {
	if n, err := strconv.Atoi(os.Getenv("CH_QUERY_THREADS")); err == nil && n > 0 {
		return n
	}
	return 2
}()

// TenantCtx scopes a query to one tenant: the row policies on tp_api read
// SQL_tenant_id, and the timeout bounds runaway queries.
func TenantCtx(ctx context.Context, tenantID string, timeout time.Duration, params clickhouse.Parameters) context.Context {
	settings := clickhouse.Settings{
		"SQL_tenant_id":      clickhouse.CustomSetting{Value: tenantID},
		"max_execution_time": int(max(timeout.Seconds(), 1)),
		// Versions of a row never span partitions (created_at is immutable),
		// so FINAL can merge each partition independently and in parallel.
		"do_not_merge_across_partitions_select_final": 1,
		// Dashboards run many small concurrent queries: a couple of threads
		// each beats every query grabbing every core (oversubscription).
		"max_threads": queryThreads,
	}
	opts := []clickhouse.QueryOption{clickhouse.WithSettings(settings)}
	if params != nil {
		opts = append(opts, clickhouse.WithParameters(params))
	}
	return clickhouse.Context(ctx, opts...)
}

// Time formats an instant for DateTime64(6, 'UTC') query parameters.
func Time(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000") }
