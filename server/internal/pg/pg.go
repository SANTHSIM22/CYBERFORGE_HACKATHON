// Package pg holds connection-pool and migration helpers shared by all services.
package pg

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// PoolOptions tunes a pool; zero values keep pgx defaults.
type PoolOptions struct {
	MaxConns     int32
	MinConns     int32
	AppName      string
	AfterConnect func(context.Context, *pgx.Conn) error
}

// Connect opens a pool and retries with bounded backoff until the database is
// reachable or ctx ends (services start before Postgres is ready under Compose).
func Connect(ctx context.Context, dsn string, o PoolOptions) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if o.MaxConns > 0 {
		cfg.MaxConns = o.MaxConns
	}
	if o.MinConns > 0 {
		cfg.MinConns = o.MinConns
	}
	if o.AppName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = o.AppName
	}
	cfg.AfterConnect = o.AfterConnect
	cfg.MaxConnIdleTime = 2 * time.Minute

	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		if attempt >= 40 || ctx.Err() != nil {
			return nil, fmt.Errorf("connect %s: %w", redact(dsn), err)
		}
		slog.Warn("database not ready, retrying", "dsn", redact(dsn), "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// Migrate applies embedded goose migrations under a Postgres session advisory
// lock, so concurrent runners against the same database serialize.
func Migrate(ctx context.Context, dsn string, fsys fs.FS, dir string) (int64, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		return 0, err
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return 0, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, sub, goose.WithSessionLocker(locker))
	if err != nil {
		return 0, err
	}
	if _, err := p.Up(ctx); err != nil {
		return 0, err
	}
	return p.GetDBVersion(ctx)
}

// WithDatabase swaps the database name in a postgres:// DSN.
func WithDatabase(dsn, db string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	u.Path = "/" + db
	return u.String(), nil
}

// WithUser swaps the credentials in a postgres:// DSN.
func WithUser(dsn, user, password string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	u.User = url.UserPassword(user, password)
	return u.String(), nil
}

func redact(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "<invalid dsn>"
	}
	return u.Redacted()
}
