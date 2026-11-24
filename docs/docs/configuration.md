# Configuration

This document describes how to configure the AP Explanation service for different environments.

## Environment Variables

Copy `.env.example` to `.env` and fill in the values.

### PostgreSQL

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_USER` | Yes | — | PostgreSQL username |
| `POSTGRES_PASSWORD` | Yes | — | PostgreSQL password |
| `POSTGRES_HOST` | Yes | — | Primary PostgreSQL host |
| `POSTGRES_PORT` | No | `5432` | Primary PostgreSQL port |
| `POSTGRES_TIMESCALE_HOST` | No | — | Fallback PostgreSQL host (e.g. a Timescale instance). If set, the service retries here when the target database is not found on the primary host |
| `POSTGRES_TIMESCALE_PORT` | No | `5433` | Port for the fallback PostgreSQL host |

### Infrastructure

| Variable | Required | Default | Description |
|---|---|---|---|
| `REDIS_BROKER_URI` | No | `redis://redis:6379/0` | Redis URL used as Celery broker, result backend, cache and distributed lock |
| `USE_EMBEDDED_CELERY_WORKER` | No | `true` | Start a Celery worker inside the FastAPI process. Set to `false` when using standalone workers |
| `WORKER_CONCURRENCY` | No | `2` | Tasks a worker runs at once, embedded or standalone. A task waiting for its database's lock or for the row budget holds its slot, so with 2, two tasks on a busy database block those of the others. It does not bound PostgreSQL's memory, but more tasks need more free disk: see [Queries at once](#queries-at-once) |
| `S3_MOUNT_PATH` | No | `/mnt/s3` | Local path where CSV source files are mounted (used by the CSV data source) |
| `ROOT_PATH` | No | `""` | API root path when behind a reverse proxy |

### Provenance query limits

Set on the transaction running each provenance query only (`SET LOCAL`): annotation and the other clients of the database keep the server's values. Values use PostgreSQL's syntax; an empty value leaves the server's setting. A query stopped by one of them fails its task with a message naming the limit.

| Variable | Required | Default | Description |
|---|---|---|---|
| `PROVENANCE_STATEMENT_TIMEOUT` | No | `10min` | `statement_timeout`. The one bound that holds whatever process grows, ProvSQL's store worker included (ProvSQL honours cancellation). It also bounds how long a query holds the per-database lock |
| `PROVENANCE_MAX_MEMORY` | No | `1GB` | `provsql.max_memory`: the memory ProvSQL's evaluations may add to the query's backend. Does not cover ProvSQL's store worker |
| `PROVENANCE_TEMP_FILE_LIMIT` | No | `5GB` | `temp_file_limit`. PostgreSQL's default is no limit, so a query can fill the disk. Needs a superuser (or `GRANT SET ON PARAMETER temp_file_limit`); skipped with a warning otherwise |
| `PROVENANCE_MAX_PARALLEL_WORKERS` | No | `0` | `max_parallel_workers_per_gather`. Parallel workers grow outside `provsql.max_memory`'s budget |
| `PROVENANCE_MAX_AGGREGATED_ROWS` | No | `8000000` | Most rows the aggregates of the provenance queries running at once on one PostgreSQL server may receive, all its databases together (see below). ProvSQL builds a gate per aggregated row while the query runs, before `provsql.max_memory` applies, and past about 16 million rows on a 3 or 4 GB server its store worker is killed (the server restarts) or hangs. The query is first run without provenance, under `EXPLAIN ANALYZE`, to count them: PostgreSQL's estimates are far off on joins on expressions. A query over more rows is refused; one that fits waits until the others leave it enough. Empty skips the check and the wait |
| `PROVENANCE_MIN_RESERVED_ROWS` | No | an eighth of `PROVENANCE_MAX_AGGREGATED_ROWS` | Rows of that budget each query reserves at least, with or without aggregates: their evaluations add up in ProvSQL's store worker too. With the default, at most eight provenance queries run at once on a server |
| `PROVENANCE_PRECHECK_TIMEOUT` | No | `180s` | `statement_timeout` of that plain run. A query slower than this without provenance is refused. PostgreSQL may plan the plain query much worse than ProvSQL's rewritten one: a question of the meteo use case whose provenance query takes 0.3 s was refused at 60 s, and explained in 157 s with 300 s |
| `PROVENANCE_MAX_FORMULA_ROWS` | No | `100` | Most rows of an aggregate's formula returned (`sr_formula_rows`): the first is the aggregate and its number of terms (`#1 = #1.1+…+#1.n (n terms)`), each other a term of a few dozen symbols. The formula is `truncated` past it |

#### Choosing `PROVENANCE_MAX_AGGREGATED_ROWS`

It depends on the database server's RAM, which ap-explanation cannot see. Measured with `provsql.max_memory = 1GB`, on a single query summing a two-table join, set to half of the largest aggregate the server builds within `PROVENANCE_STATEMENT_TIMEOUT`'s default, as a margin for the other applications of the server:

| Server RAM | Largest aggregate | `PROVENANCE_MAX_AGGREGATED_ROWS` |
|---|---|---|
| 3 GB | 16 million built, crash at 20 million | **8,000,000** |
| 4 GiB | 16 million built, hang at 25 million | **8,000,000** |
| 8 GB | 25 million built in 2 minutes, 36 million in 19 minutes (past `statement_timeout`), hang at 49 million | **12,500,000** |

The limit protects the building of the aggregate only: past it, ProvSQL stops the evaluation at `provsql.max_memory` or `statement_timeout`.

To measure it on a given server, run `scripts/calibrate_max_aggregated_rows.sh` against a copy of it, with the same RAM and settings: it crashes the server it runs on. A larger `PROVENANCE_MAX_MEMORY`, joins of more tables, or other applications on the server all lower the threshold.

#### One budget per server

ProvSQL builds the aggregates of every database of a server in one store worker, so the queries of different databases add up: on a 3 GB server, two queries of 8 million rows each run together, but two of 10 million each stall until `statement_timeout`, where each alone takes a minute. The memory is freed when a query ends: the same two run one after the other without trouble.

So `PROVENANCE_MAX_AGGREGATED_ROWS` is shared by all the provenance queries of a server (its host and port), whatever their database or worker. Each one reserves the rows of its aggregates, counted by the pre-check, or `PROVENANCE_MIN_RESERVED_ROWS` if more, and waits until they are free; the first one that does not fit goes next, so a large query is not starved by small ones. Reservations are kept in Redis, and expire a minute after a worker that crashed holding them.

The minimum is because evaluations add up as well: two `sr_formula` evaluations of 4 million rows at once crash a 3 GB server, where one alone is stopped by `provsql.max_memory` (ap-explanation reads an aggregate's formula with `sr_formula_rows` instead, which stops at its first rows). Queries without aggregates weigh little: ProvSQL keeps a gate per result row, about 140 bytes, and evaluating each row's provenance takes about 85 µs, so `statement_timeout` stops them first. On a 3 GB server, eight of 1 million result rows run together (the store worker reaches 0.7 GB), and eight of 2 million are all stopped by `statement_timeout`, without a crash. Hence the default of an eighth.

#### Queries at once

Three things bound the provenance queries running together:

- **Per database**: one at a time (the per-database lock). A server with a single database never runs two.
- **Per worker**: `WORKER_CONCURRENCY` tasks, waiting ones included. It sets throughput, not load: a task waiting for a lock or for the budget holds its slot, so raising it keeps a busy database from holding back the others.
- **Per server**: the row budget, `PROVENANCE_MAX_AGGREGATED_ROWS / PROVENANCE_MIN_RESERVED_ROWS` queries at most, whatever the number of workers. This is the bound that protects the server's memory.

`provsql.max_memory` applies to each query: a query's backend measured up to 1.16 GB with `1GB`. On a 4 GB server with the default limits, the meteo use case's heaviest questions, sent all at once:

| Queries at once on the server | Peak memory (processes) | Result |
|---|---|---|
| 1 (one database) | 1.6 GB | no crash |
| 4 (four databases) | 2.9 GB | no crash |
| 3 (five databases, `PROVENANCE_MIN_RESERVED_ROWS=2000000`, `PROVENANCE_TEMP_FILE_LIMIT=1GB`) | 2.1 GB | no out-of-memory kill; the test stopped when the disk ran out |

So on a 4 GB server, set `PROVENANCE_MIN_RESERVED_ROWS` to a quarter of `PROVENANCE_MAX_AGGREGATED_ROWS` (2,000,000) for at most four queries, or to half (4,000,000) for two, the most that cannot exceed the RAM even if each reaches `provsql.max_memory`. The default, an eighth, allows eight. Aggregates of millions of rows lower it further: two of 3.2 million rows leave too little of 8 million for a third.

The disk, not the memory, gave out first. ProvSQL writes the circuit of every query to its store, on disk, and keeps it (see [Reclaiming the circuit store](usage.md#reclaiming-the-circuit-store)); queries also write up to `PROVENANCE_TEMP_FILE_LIMIT` of temporary files each, the pre-check included, which the budget does not count. Three of the meteo use case's heaviest questions running together took 4.5 GB in two minutes. When the disk fills, ProvSQL's store worker dies (`SIGBUS`) and the whole server restarts. The service reclaims the store after a task that grew it, so the disk needs room for the largest single task (0.7 GB for the meteo use case's heaviest question on the City of Zurich data), not for every query ever run. A server runs at most one task per database, and no more than the `WORKER_CONCURRENCY` of all workers together: keep that many times the largest task free, plus `PROVENANCE_TEMP_FILE_LIMIT` each.

### LLM — Natural-language explanations (optional)

When `LLM_API_BASE` is not set, natural-language explanations are silently disabled and the service falls back to returning only the raw semiring result.

| Variable | Required | Default | Description |
|---|---|---|---|
| `LLM_API_BASE` | No | — | Base URL of an OpenAI-compatible LLM API. If omitted, NL explanations are disabled |
| `LLM_API_MODEL` | If `LLM_API_BASE` set | — | Model name to pass to the LLM API (e.g. `gpt-4o`, `mistral-7b`) |
| `LLM_API_KEY` | No | — | API key for the LLM endpoint. Leave empty for unauthenticated / local endpoints |
| `LLM_SSL_VERIFY` | No | `true` | Set to `false` to disable TLS certificate verification for the LLM endpoint (useful for self-hosted models with self-signed certs) |
| `LLM_TIMEOUT` | No | `300` | Seconds to wait for a completion before giving up. Without it an unresponsive endpoint stalls the explain task for as long as it holds the per-database lock |

### Authentication (OIDC)

When `OIDC_ISSUER` is not set, authentication is **disabled** and all endpoints are publicly accessible.

| Variable | Required | Default | Description |
|---|---|---|---|
| `OIDC_ISSUER` | No | — | OIDC issuer URL (e.g. `https://keycloak.example.com/realms/myrealm`). If not set, authentication is disabled |
| `OIDC_CLIENT_ID` | If `OIDC_ISSUER` set | — | Client ID used to validate the JWT audience and for token exchange |
| `OIDC_CLIENT_SECRET` | If `OIDC_ISSUER` set | — | Client secret used for token exchange |
| `OIDC_EXCHANGE_SCOPE` | If `OIDC_ISSUER` set | — | Scope requested during the OIDC token exchange flow |
| `JWKS_TTL_SECONDS` | No | `300` | How long (seconds) the JWKS public key cache is valid before being refreshed |

When enabled, every protected endpoint requires `Authorization: Bearer <token>`. The service:

1. Fetches the JWKS from `{OIDC_ISSUER}/protocol/openid-connect/certs` (cached for `JWKS_TTL_SECONDS`)
2. Validates the token signature (RS256), issuer, and audience
3. Binds `UserId` (`sub`) and `ClientId` (`azp`) from the JWT claims to the structured logs for the request

### Observability (OpenTelemetry)

The app and the Celery worker are launched via [zero-code auto-instrumentation](https://signoz.io/docs/instrumentation/opentelemetry-python/) (`opentelemetry-instrument`, see `Dockerfile`), so no code changes are needed to get traces for FastAPI, Celery, psycopg, redis, httpx, and the LLM client. Export is **disabled by default** — set an OTLP endpoint and switch the exporters on to enable it.

| Variable | Required | Default | Description |
|---|---|---|---|
| `OTEL_SERVICE_NAME` | No | `ap-explanation` | Service name reported to the OTLP backend |
| `OTEL_TRACES_EXPORTER` | No | `none` | Set to `otlp` to export traces |
| `OTEL_METRICS_EXPORTER` | No | `none` | Set to `otlp` to export metrics |
| `OTEL_LOGS_EXPORTER` | No | `none` | Set to `otlp` to export logs |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | If any exporter is enabled | — | OTLP collector endpoint, e.g. `http://otel-collector:4317` |
| `OTEL_EXPORTER_OTLP_HEADERS` | No | — | Extra headers for the OTLP exporter (e.g. an ingestion key), comma-separated `key=value` pairs |

### Database Connection Behavior

The service supports a dual-database architecture:

1. **Primary Connection**: The service first attempts to connect to the database specified in the Analytical Pattern on the primary PostgreSQL server (`POSTGRES_HOST:POSTGRES_PORT`)
2. **Fallback Connection**: If the database doesn't exist on the primary server, the service automatically falls back to the Timescale server (`POSTGRES_TIMESCALE_HOST:POSTGRES_TIMESCALE_PORT`)
3. **Error Handling**: If the database doesn't exist on either server, a `DatabaseNotFoundError` is raised

This architecture allows for flexible database deployment, supporting scenarios where databases are distributed across multiple PostgreSQL instances.

## PostgreSQL with ProvSQL

### Database Requirements

The service **requires** a PostgreSQL database with the following:

1. **ProvSQL Extension**: The database must have the [ProvSQL extension](https://github.com/PierreSenellart/provsql) installed and enabled. ProvSQL provides provenance tracking capabilities for SQL queries.

2. **Semirings**: ProvSQL's built-in semirings are used directly; the service creates no custom types or functions in your database.

### Using the Pre-configured Docker Image

A pre-configured Docker image with PostgreSQL and ProvSQL is available:

```bash
cd dependencies/postgres-provsql
docker build -t postgres-provsql .
```

This image includes:
- PostgreSQL with the ProvSQL extension pre-installed
- All necessary dependencies and configurations

## Testing Configuration

Tests automatically use testcontainers to spin up a PostgreSQL instance with ProvSQL. No manual configuration needed for running tests:

```bash
pytest tests/
```

The test configuration is defined in `tests/conftest.py`.
