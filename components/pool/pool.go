// Package pool caches database connections across component invocations.
// Postgres pools are keyed by DSN; Redis clients are keyed by URL.
// Each unique DSN/URL produces one shared pool that lives for the process lifetime.
package pool

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	pgPools      sync.Map // map[string]*pgxpool.Pool
	redisClients sync.Map // map[string]*redis.Client
)

// Postgres returns a cached pgx pool for the given DSN, creating one on first use.
func Postgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if v, ok := pgPools.Load(dsn); ok {
		return v.(*pgxpool.Pool), nil
	}
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if actual, loaded := pgPools.LoadOrStore(dsn, p); loaded {
		p.Close()
		return actual.(*pgxpool.Pool), nil
	}
	return p, nil
}

// Redis returns a cached Redis client for the given URL, creating one on first use.
func Redis(url string) (*redis.Client, error) {
	if v, ok := redisClients.Load(url); ok {
		return v.(*redis.Client), nil
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	c := redis.NewClient(opts)
	if actual, loaded := redisClients.LoadOrStore(url, c); loaded {
		if closeErr := c.Close(); closeErr != nil {
			_ = closeErr
		}
		return actual.(*redis.Client), nil
	}
	return c, nil
}
