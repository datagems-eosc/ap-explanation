# Development Guide

## Setup

### With Dev Container (Recommended)

Open in VS Code with the Dev Containers extension. PostgreSQL with ProvSQL is pre-configured.

### Local Setup

Requirements: Python 3.14, `uv` package manager, PostgreSQL with ProvSQL

```bash
# Install dependencies
uv sync --all-groups

# Configure environment
cp .env.example .env
# Edit .env with your database connection details:
#   - POSTGRES_USER
#   - POSTGRES_PASSWORD
#   - POSTGRES_HOST
#   - POSTGRES_PORT (optional, defaults to 5432)
#   - POSTGRES_TIMESCALE_HOST (optional, for fallback database)
#   - POSTGRES_TIMESCALE_PORT (optional, defaults to 5433)
#   - ROOT_PATH (optional, for reverse proxy setups)

# Run service
uv run ap_explanation/main.py
```

---

## Running Tests

```bash
# All tests
pytest tests/

# Specific test file
pytest tests/test_int_annotate.py

# With coverage
pytest tests/ --cov=ap_explanation
```

Tests use testcontainers to spin up PostgreSQL with ProvSQL automatically.

---

## Project Structure

```
ap_explanation/
├── api/
│   ├── middlewares/     # Authentication, correlation ID
│   └── v1/              # API endpoints
│       ├── provenance/managed/  # Async lifecycle (dispatch + poll)
│       └── health.py    # Health & readiness endpoints
├── tasks/               # Celery tasks
├── services/            # Business logic
├── repositories/        # Data access layer
│   ├── provsql.py       # ProvSqlRepository: enable, disable, probabilities, compute, circuit cleanup
│   ├── query_guard.py   # Limits of a provenance query, aggregate check, row budget
│   ├── schema.py        # Schema description for the LLM
│   ├── seeding.py       # Sets a database up for a data source (CSV)
│   └── legacy_mappings.py   # What earlier versions left in annotated databases
├── internal/            # Internal modules
│   ├── cache.py         # Redis cache provider
│   └── distributed_lock.py  # Redis distributed lock
├── domains/             # One directory per domain
│   ├── provenance/
│   │   ├── data_sources/    # Data source types (relational DB, CSV set)
│   │   ├── derivation.py    # Results: Derivation, Provenance
│   │   ├── provenance_analytical_pattern.py
│   │   ├── semiring.py      # DbSemiring and the configured semirings
│   │   └── sql_rewriter.py  # SQL rewriting for provenance
│   └── nl_explanation/  # LLM-powered NL explanations
├── errors/              # Custom exceptions
├── di.py                # Dependency injection & lifespan
├── settings.py          # Configuration, read from the environment
├── celery_app.py        # Celery application
└── main.py              # FastAPI application entry point
```

---

## Adding a New Semiring

1. **Define the semiring** in `ap_explanation/domains/provenance/semiring.py`:

```python
DbSemiring(
    name="custom",
    retrieval_function="custom_prov",
    supports_aggregates=True,  # Optional
)
```

2. **Test** the new semiring:

```python
# Add test in tests/
```

---

## Code Quality

```bash
# Format & lint
pre-commit run --all-files

# Type checking (if using mypy)
mypy ap_explanation/
```

---

## Documentation

Build documentation locally:

```bash
make docs
```

Visit http://localhost:8000
