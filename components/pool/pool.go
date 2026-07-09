// Package pool caches database connections across component invocations.
// Postgres pools are keyed by DSN; Redis clients are keyed by URL.
// Each unique DSN/URL produces one shared pool that lives for the process lifetime.
package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
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

// TenantSchema derives a per-tenant Postgres schema name from a node's
// namespace + project. Deterministic, collision-resistant, and always a
// valid lowercase SQL identifier ("t_" + 16 hex chars), so it needs no
// quoting or injection-guarding. This is how the shared zero-config
// pgvector bundle isolates tenants: projects that share one database (the
// playground packs every trial into one namespace) each get their own
// schema. The value is derived entirely from the platform-injected node
// identity — a flow author cannot choose, guess, or override another
// tenant's schema. Empty identity yields "" (no scoping).
func TenantSchema(namespace, project string) string {
	if namespace == "" && project == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(namespace + "/" + project))
	return "t_" + hex.EncodeToString(sum[:8])
}

// PostgresScoped is Postgres with an optional per-tenant schema. When
// schema is non-empty it returns a separately-cached pool whose every
// connection runs `CREATE SCHEMA IF NOT EXISTS <schema>; SET search_path
// TO <schema>, public` — so all SQL (table creation, upserts, queries)
// is transparently confined to that schema while extension types like
// `vector` still resolve from public. schema=="" behaves exactly like
// Postgres, so an explicit user DSN (their own database) is never rescoped.
func PostgresScoped(ctx context.Context, dsn, schema string) (*pgxpool.Pool, error) {
	if schema == "" {
		return Postgres(ctx, dsn)
	}
	key := dsn + "\x00" + schema
	if v, ok := pgPools.Load(key); ok {
		return v.(*pgxpool.Pool), nil
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	stmt := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s; SET search_path TO %s, public", schema, schema)
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, stmt)
		return err
	}
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if actual, loaded := pgPools.LoadOrStore(key, p); loaded {
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
