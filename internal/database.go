package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// NewPostgresPool creates a new connection pool to PostgreSQL.
func NewPostgresPool(ctx context.Context, databaseURL string) *pgxpool.Pool {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		slog.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping PostgreSQL", "error", err)
		os.Exit(1)
	}

	slog.Info("connected to PostgreSQL", "url", maskDSN(databaseURL))
	return pool
}

// NewRedisClient creates a new Redis client and verifies connectivity.
func NewRedisClient(ctx context.Context, addr string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		slog.Error("failed to connect to Redis", "addr", addr, "error", err)
		os.Exit(1)
	}

	slog.Info("connected to Redis", "addr", addr)
	return client
}

// maskDSN masks the password in a database URL for safe logging.
func maskDSN(dsn string) string {
	// Simple approach: find :// and @ to mask the password portion
	start := 0
	for i := 0; i < len(dsn)-2; i++ {
		if dsn[i] == '/' && dsn[i+1] == '/' {
			start = i + 2
			break
		}
	}

	atIdx := -1
	for i := start; i < len(dsn); i++ {
		if dsn[i] == '@' {
			atIdx = i
			break
		}
	}

	if atIdx == -1 {
		return dsn
	}

	colonIdx := -1
	for i := start; i < atIdx; i++ {
		if dsn[i] == ':' {
			colonIdx = i
			break
		}
	}

	if colonIdx == -1 {
		return dsn
	}

	return fmt.Sprintf("%s:****%s", dsn[:colonIdx], dsn[atIdx:])
}
