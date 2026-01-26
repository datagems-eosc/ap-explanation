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
