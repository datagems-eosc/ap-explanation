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
| `S3_MOUNT_PATH` | No | `/mnt/s3` | Local path where CSV source files are mounted (used by the CSV data source) |
| `ROOT_PATH` | No | `""` | API root path when behind a reverse proxy |

### Provenance query limits

Set on the transaction running each provenance query only (`SET LOCAL`): annotation and the other clients of the database keep the server's values. Values use PostgreSQL's syntax; an empty value leaves the server's setting. A query stopped by one of them fails its task with a message naming the limit.

| Variable | Required | Default | Description |
|---|---|---|---|
| `PROVENANCE_STATEMENT_TIMEOUT` | No | `10min` | `statement_timeout`. The one bound that holds whatever process grows, ProvSQL's store worker included (ProvSQL >= 1.13 honours cancellation). It also bounds how long a query holds the per-database lock |
| `PROVENANCE_MAX_MEMORY` | No | `1GB` | `provsql.max_memory`: the memory ProvSQL's evaluations may add to the query's backend. Does not cover ProvSQL's store worker |
| `PROVENANCE_TEMP_FILE_LIMIT` | No | `5GB` | `temp_file_limit`. PostgreSQL's default is no limit, so a query can fill the disk. Needs a superuser (or `GRANT SET ON PARAMETER temp_file_limit`); skipped with a warning otherwise |
| `PROVENANCE_MAX_PARALLEL_WORKERS` | No | `0` | `max_parallel_workers_per_gather`. Parallel workers grow outside `provsql.max_memory`'s budget |
| `PROVENANCE_MAX_AGGREGATED_ROWS` | No | `8000000` | Most rows an aggregate of the query may receive. ProvSQL builds a gate per aggregated row while the query runs, before `provsql.max_memory` applies, and past about 16 million rows on a 3 or 4 GB server its store worker is killed (the server restarts) or hangs. The default is for ProvSQL 1.14; see below for 1.13. The query is first run without provenance, under `EXPLAIN ANALYZE`, to count them: PostgreSQL's estimates are far off on joins on expressions. Empty skips the check |
| `PROVENANCE_PRECHECK_TIMEOUT` | No | `180s` | `statement_timeout` of that plain run. A query slower than this without provenance is refused. PostgreSQL may plan the plain query much worse than ProvSQL's rewritten one: a question of the meteo use case whose provenance query takes 0.3 s was refused at 60 s, and explained in 157 s with 300 s |
| `PROVENANCE_MAX_FORMULA_ROWS` | No | `100` | On ProvSQL 1.14, most rows of an aggregate's formula returned (`sr_formula_rows`): the first is the aggregate and its number of terms (`#1 = #1.1+…+#1.n (n terms)`), each other a term of a few dozen symbols. The formula is `truncated` past it. On ProvSQL 1.13 the whole formula is returned, as one value |

#### Choosing `PROVENANCE_MAX_AGGREGATED_ROWS`

It depends on the database server's RAM, which ap-explanation cannot see, and on the ProvSQL version. Measured with `provsql.max_memory = 1GB`, on a single query summing a two-table join, set to half of the largest aggregate the server survives, as a margin for other sessions:

| Server RAM | ProvSQL 1.14 (master `3b8d3098`) | ProvSQL 1.13.0 |
|---|---|---|
| 3 GB | 16 million built, crash at 20 million: **8,000,000** | crash at 6 million: **2,000,000** |
| 4 GiB | 16 million built, hang at 25 million: **8,000,000** | crash at 9 million: **4,500,000** |

On 1.14 the limit protects the building of the aggregate only: past it, ProvSQL stops the evaluation at `provsql.max_memory` or `statement_timeout`. On 1.13.0 the evaluation itself crashes the server first, hence its lower limits.

To measure it on a given server, run `scripts/calibrate_max_aggregated_rows.sh` against a copy of it, with the same RAM and settings: it crashes the server it runs on. A larger `PROVENANCE_MAX_MEMORY`, joins of more tables, or other applications on the server all lower the threshold.

The limit holds for one provenance query at a time. ap-explanation runs one per database, but databases on the same server can run theirs at once: their memory adds up.

### Maintenance

| Variable | Required | Default | Description |
|---|---|---|---|
| `CIRCUIT_CLEANUP_WAIT` | No | `60` | Seconds a circuit cleanup retries while other sessions are connected to the database, before failing |

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

2. **Semiring Definitions**: The service automatically initializes semiring type definitions and aggregate functions when connecting to the database. This includes:
   - Custom composite types (`formula_state`, `whyprov_state`, etc.)
   - Semiring operation functions (plus, times, monus)
   - Aggregate functions for provenance tracking

   These definitions are automatically created from the SQL script at `ap_explanation/repository/resources/03_setup_semiring_parallel.sql` during the first connection.

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
