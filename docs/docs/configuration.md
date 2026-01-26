# Configuration

This document describes how to configure the AP Explanation service for different environments.

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
