# AP Explanation

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/ap-explanation)](https://img.shields.io/github/commit-activity/m/datagems-eosc/ap-explanation)
[![License](https://img.shields.io/github/license/datagems-eosc/ap-explanation)](https://img.shields.io/github/license/datagems-eosc/ap-explanation)

## Overview

A FastAPI service that explains **where your SQL query results come from**. Given a query like `SELECT name FROM students WHERE grade > 80`, this service shows which source rows contributed to each result and how they were combined.

Uses PostgreSQL with ProvSQL extension to track data lineage through joins, aggregations, and transformations.

### Example

```bash
# 1. Annotate tables (one-time setup)
curl -X POST http://localhost:5000/api/v1/ap/annotate -d @analytical_pattern.json

# 2. Get provenance explanation
curl -X POST http://localhost:5000/api/v1/ap/explain -d @analytical_pattern.json
```

**Response shows:**
- **Formula semiring**: How results were computed: `(students₁ ⊗ grades₂) ⊕ students₃`
- **Why semiring**: Which rows contributed: `["students(1)", "grades(2)", "students(3)"]`

Perfect for debugging queries, compliance tracking, and understanding data transformations.

## Quick Start

```bash
# Install dependencies (you can remove '--all-groups' for production)
uv sync --all-groups

# Start the service
uv run ap_explanation/main.py
```

## Testing

Run tests with pytest:

```bash
pytest tests/
```

Tests use testcontainers to run a PostgreSQL instance with ProvSQL automatically.

## Documentation

Full documentation is available at: https://datagems-eosc.github.io/ap-explanation/