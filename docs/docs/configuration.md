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

### LLM — Natural-language explanations (optional)

When `LLM_API_BASE` is not set, natural-language explanations are silently disabled and the service falls back to returning only the raw semiring result.

| Variable | Required | Default | Description |
|---|---|---|---|
| `LLM_API_BASE` | No | — | Base URL of an OpenAI-compatible LLM API. If omitted, NL explanations are disabled |
| `LLM_API_MODEL` | If `LLM_API_BASE` set | — | Model name to pass to the LLM API (e.g. `gpt-4o`, `mistral-7b`) |
| `LLM_API_KEY` | No | — | API key for the LLM endpoint. Leave empty for unauthenticated / local endpoints |
| `LLM_SSL_VERIFY` | No | `true` | Set to `false` to disable TLS certificate verification for the LLM endpoint (useful for self-hosted models with self-signed certs) |

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
