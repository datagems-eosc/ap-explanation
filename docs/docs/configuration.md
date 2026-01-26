# Configuration

This document describes how to configure the AP Explanation service for different environments.

## Environment Variables

The AP Explanation service uses environment variables for configuration. A `.env` file can be used to define these variables:

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `POSTGRES_HOST` | PostgreSQL database host | `localhost` | Yes |
| `POSTGRES_PORT` | PostgreSQL database port | `5432` | Yes |
| `POSTGRES_DB` | PostgreSQL database name | `mathe` | Yes |
| `POSTGRES_USER` | PostgreSQL username | `provdemo` | Yes |
| `POSTGRES_PASSWORD` | PostgreSQL password | - | Yes |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL) | `INFO` | No |

### Example `.env` File

```bash
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mathe
POSTGRES_USER=provdemo
POSTGRES_PASSWORD=provdemo

# Logging
LOG_LEVEL=INFO
```

## PostgreSQL with ProvSQL

The service requires PostgreSQL with the ProvSQL extension. A pre-configured Docker image is available:

```bash
cd dependencies/postgres-provsql
docker build -t postgres-provsql .
```

## API Documentation

Once configured and running, access the interactive API documentation:

- **Swagger UI**: http://localhost:5000/docs
- **ReDoc**: http://localhost:5000/redoc
- **OpenAPI JSON**: http://localhost:5000/openapi.json

## Testing Configuration

Tests automatically use testcontainers to spin up a PostgreSQL instance with ProvSQL. No manual configuration needed for running tests:

```bash
pytest tests/
```

The test configuration is defined in `tests/conftest.py`.
