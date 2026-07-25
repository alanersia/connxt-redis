# connxt-redis

Native synchronous Redis TCP client. It does not use a Redis client library.

```bash
REDIS_TEST_URL='redis://:dev@127.0.0.1:6379/0' cargo run --example ping
```

The optional `tokio` and `tls` features are compile-gated. `rediss://` never
falls back to cleartext. Cluster routing and automatic `MOVED`/`ASK` handling
are outside this crate's current boundary.

Operational hooks are available through `ClientConfig::metrics`,
`ops::Backoff`, and `ops::CircuitBreaker`. Always call `Pool` handles' drops
before shutdown; dropping the pool closes idle connections. Use bounded command
timeouts for health checks and never retry writes unless they are known to be
idempotent.

For Redis 6/7 checks:

```bash
docker compose -f docker-compose.redis.yml up -d
REDIS_TEST_URL='redis://:dev@127.0.0.1:6380/0' cargo run --example ping
REDIS_TEST_URL='redis://:dev@127.0.0.1:6381/0' cargo run --example ping
```

CI runs the integration suite against Redis 6 and Redis 7. Sentinel failover
and TLS server certificates should be supplied by the deployment environment;
the client validates the endpoint and reports connection failure rather than
silently downgrading security.

Sentinel fixtures are available with `docker compose -f
docker-compose.sentinel.yml up -d`; query the `mymaster` service with
`sentinel::discover` before creating a client. Cluster `MOVED`/`ASK` routing is
intentionally not implemented.
