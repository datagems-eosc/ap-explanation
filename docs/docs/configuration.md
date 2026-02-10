# Configuration

This document describes how to configure the AP Explanation service for different environments.

## Environment Variables

The service requires the following environment variables to connect to PostgreSQL databases:

### Required Variables

- `POSTGRES_USER`: PostgreSQL username
- `POSTGRES_PASSWORD`: PostgreSQL password  
- `POSTGRES_HOST`: Hostname or IP address of the primary PostgreSQL server

### Optional Variables

- `POSTGRES_PORT`: Port for the primary PostgreSQL server (default: `5432`)
- `POSTGRES_TIMESCALE_HOST`: Hostname for the Timescale/secondary PostgreSQL server
- `POSTGRES_TIMESCALE_PORT`: Port for the Timescale server (default: `5433`)
- `ROOT_PATH`: Root path for the API when behind a reverse proxy (default: `""`)

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
