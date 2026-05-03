# Tiny Systems Database Module

Postgres and Redis components for Tiny Systems flows.

## Components

| Name | Purpose |
|---|---|
| `postgres_exec` | Run INSERT/UPDATE/DELETE with positional parameters; emits `rowsAffected`. |
| `postgres_query` | Run SELECT; emits `rows[]` keyed by column name with a configurable row shape. |
| `redis_dedup` | Atomic "first seen" check via `SET NX EX`; routes new IDs to **out_new** and duplicates to **out_seen**. |
| `redis_set` | Set a key, with optional TTL and NX. |
| `redis_get` | Get a key, returns `found=false` for missing keys without raising an error. |

All components take their connection string (`dsn` for Postgres, `url` for Redis) **per message**, so a single deployed module can talk to many databases. Connections are pooled by DSN/URL across calls.

## Patterns

### Dedup-then-route

```
ticker → http_request(api) → json_decode → array_split → redis_dedup
                                                            ├── out_new  → … process and store ──→ postgres_exec
                                                            └── out_seen → drop
```

Use `redis_dedup` for "have I already processed this ID?" checks. `keyPrefix` + `id` form the composite key. TTL determines how long Redis remembers — set it longer than the polling cycle plus margin.

### Insert with parameters

```
redis_dedup:out_new → postgres_exec
                       sql: INSERT INTO matched_posts (id, source, title, score) VALUES ($1, $2, $3, $4)
                       params: ["{{$.id}}", "reddit", "{{$.context.title}}", "{{$.context.score}}"]
```

### Query with configurable row shape

In `postgres_query` settings, define the expected row shape:

```json
{
  "row": { "id": "abc", "title": "title", "score": 0 }
}
```

Downstream edges can then navigate `$.rows[0].title`, `$.count`, etc.

## Run Locally

```shell
go run cmd/main.go run \
  --name=tiny-systems/database-module-v0 \
  --namespace=tinysystems \
  --version=0.1.0
```

## License

MIT for this module's source. Depends on [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1).
