package repositories

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Config holds tuning parameters for repository infrastructure.
type Config struct {
	HLLFlushInterval   time.Duration
	ActiveKeysCacheTTL time.Duration
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		HLLFlushInterval:   60 * time.Second,
		ActiveKeysCacheTTL: 5 * time.Minute,
	}
}

// Connect opens and pings a PostgreSQL connection pool.
func Connect(ctx context.Context, dsn string, maxConns int) (*sqlx.DB, error) {
	db, err := sqlx.ConnectContext(ctx, "postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxConns)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
