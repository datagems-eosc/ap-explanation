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

```json
{
  "nodes": [
    {
      "id": "db-node-id",
      "labels": ["Relational_Database"],
      "properties": {
        "contentUrl": "postgresql://user:pass@host/db",
        "name": "public"
      }
    },
    {
      "id": "table-node-id",
      "labels": ["Table"],
      "properties": {"name": "students"}
    },
    {
      "id": "query-node-id",
      "labels": ["Provenance_SQL_Operator"],
      "properties": {
        "query": "SELECT name FROM students WHERE grade > 80"
      }
    }
  ],
  "edges": [
    {"from": "query-node-id", "to": "table-node-id", "labels": ["input"]},
    {"from": "table-node-id", "to": "db-node-id", "labels": ["contain"]}
  ]
}
```

### Workflow

1. **Annotate tables (one-time)**: `POST /api/v1/ap/annotate` — Prepares tables for provenance tracking
2. **Explain queries**: `POST /api/v1/ap/explain` — Returns provenance information showing how results derive from source data

The AP graph defines the database connection, tables, and query. The service extracts these components and applies provenance tracking.

## Getting Started

The best solution is to use the provided .devcontainer file. The PostgreSQL database with ProvSQL will already be configured.

To run it locally without the devcontainer:

```bash
# Requirements: python >=3.14, uv, PostgreSQL with ProvSQL extension
uv sync --all-groups
cp .env.example .env
# (Fill all the required variables in .env)
uv run ap_explanation/main.py
```

The API will be available at `http://localhost:5000/api/v1`

### Interactive Documentation

- **Swagger UI**: http://localhost:5000/docs
- **ReDoc**: http://localhost:5000/redoc
