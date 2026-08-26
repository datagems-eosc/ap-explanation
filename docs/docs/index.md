# AP Explanation API

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/ap-explanation)](https://img.shields.io/github/commit-activity/m/datagems-eosc/ap-explanation)
[![License](https://img.shields.io/github/license/datagems-eosc/ap-explanation)](https://img.shields.io/github/license/datagems-eosc/ap-explanation)

This is the documentation site for the AP Explanation service. The service provides a RESTful API for annotating and explaining data provenance using semiring annotations in PostgreSQL with ProvSQL.

## What is Data Provenance?

**Data provenance** tracks the origin and transformation history of data. This service uses **semiring annotations** to capture how query results are derived from source data.

Provenance annotations enable:
- Understanding data lineage and transformations
- Debugging complex queries
- Ensuring reproducibility and compliance
- Analyzing data dependencies

## Quick Links

- [API](openapi.md) - OpenAPI specification
- [Configuration](configuration.md) - How to configure the service
- [Architecture](architecture.md) - Technical architecture details

## Working with Analytical Patterns (AP)

The service processes **Analytical Patterns (AP)** in PG-JSON format—a graph structure with nodes and edges representing database operations.

### Example AP Structure

Node and edge ids must be valid UUIDs, and every node must carry a `properties`
object. Edge direction matters: `input` runs *from* the data node *to* the
operator, and `containedIn` runs *from* the container *to* what it contains.

```json
{
  "nodes": [
    {
      "id": "0f1a2b3c-0001-4a00-8a00-000000000001",
      "labels": ["Analytical_Pattern"],
      "properties": {"name": "Query Students AP"}
    },
    {
      "id": "0f1a2b3c-0002-4a00-8a00-000000000002",
      "labels": ["Operator", "Provenance_SQL_Operator"],
      "properties": {
        "query": "SELECT name FROM students WHERE grade > 80"
      }
    },
    {
      "id": "0f1a2b3c-0003-4a00-8a00-000000000003",
      "labels": ["RelationalDatabase"],
      "properties": {"name": "school"}
    },
    {
      "id": "0f1a2b3c-0004-4a00-8a00-000000000004",
      "labels": ["Table"],
      "properties": {"name": "public.students"}
    }
  ],
  "edges": [
    {
      "from": "0f1a2b3c-0001-4a00-8a00-000000000001",
      "to": "0f1a2b3c-0002-4a00-8a00-000000000002",
      "labels": ["consist_of"]
    },
    {
      "from": "0f1a2b3c-0003-4a00-8a00-000000000003",
      "to": "0f1a2b3c-0002-4a00-8a00-000000000002",
      "labels": ["input"]
    },
    {
      "from": "0f1a2b3c-0003-4a00-8a00-000000000003",
      "to": "0f1a2b3c-0004-4a00-8a00-000000000004",
      "labels": ["containedIn"]
    }
  ]
}
```

### Workflow

**Managed** — the API handles everything asynchronously:

1. **Dispatch**: `POST /api/v1/aps/explanation` — returns a `task_id` immediately (HTTP 202)
2. **Poll**: `GET /api/v1/aps/explanation/{task_id}` — returns status and result when done

To target a single semiring: `POST /api/v1/aps/explanation/{semiring_name}`

The AP graph defines the database connection, tables, and query. The service extracts these components and applies provenance tracking.

## Getting Started

The best solution is to use the provided `.devcontainer` configuration. The PostgreSQL database with ProvSQL will already be configured.

To run it locally without the devcontainer:

```bash
# Requirements: Python 3.14, uv, PostgreSQL with ProvSQL extension
uv sync --all-groups
cp .env.example .env
# Fill in the required variables in .env:
#   POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST
#   Optional: POSTGRES_PORT, POSTGRES_TIMESCALE_HOST, POSTGRES_TIMESCALE_PORT
uv run ap_explanation/main.py
```

The API will be available at `http://localhost:5000/api/v1`

### Interactive Documentation

- **Swagger UI**: http://localhost:5000/docs
- **ReDoc**: http://localhost:5000/redoc
