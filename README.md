# AP Explanation

[![Commit activity](https://img.shields.io/github/commit-activity/m/datagems-eosc/ap-explanation)](https://img.shields.io/github/commit-activity/m/datagems-eosc/ap-explanation)
[![License](https://img.shields.io/github/license/datagems-eosc/ap-explanation)](https://img.shields.io/github/license/datagems-eosc/ap-explanation)

## Overview

A FastAPI-based service for annotating and explaining data provenance using **semiring annotations**. This project integrates with PostgreSQL and the ProvSQL extension to track and analyze data lineage in SQL queries.

Provenance annotations help understand how data was derived, enabling reproducibility, debugging, and compliance in data workflows.

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