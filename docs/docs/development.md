# Development Guide

## Setup

### With Dev Container (Recommended)

Open in VS Code with the Dev Containers extension. PostgreSQL with ProvSQL is pre-configured.

### Local Setup

Requirements: Python ≥3.14, PostgreSQL with ProvSQL

```bash
# Install dependencies
uv sync --all-groups

# Configure environment
cp .env.example .env
# Edit .env with your database connection

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
├── api/v1/              # API endpoints
│   ├── annotate/        # Annotation endpoints
│   ├── explain/         # Explanation endpoints
│   └── dependencies/    # FastAPI dependencies
├── services/            # Business logic
├── repository/          # Data access layer
├── internal/            # SQL rewriting
├── types/               # Type definitions
├── errors/              # Custom exceptions
├── semirings.py         # Semiring configurations
└── di.py               # Dependency injection
```

---

## Adding a New Semiring

1. **Define the semiring** in `ap_explanation/semirings.py`:

```python
DbSemiring(
    name="custom",
    retrieval_function="custom_prov",
    aggregate_function="custom_agg",  # Optional
    mapping_table="custom_mapping",
    mappingStrategy=CtidMapping(),
)
```

2. **Create database functions** in `repository/resources/03_setup_semiring_parallel.sql`:

```sql
CREATE OR REPLACE FUNCTION custom_prov(...)
-- Implementation
```

3. **Test** the new semiring:

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
cd docs/
mkdocs serve
```

Visit http://localhost:8000
