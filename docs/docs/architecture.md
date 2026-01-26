# Service Architecture

The AP Explanation service is a RESTful API designed to annotate SQL queries and explain data provenance using PostgreSQL with the ProvSQL extension. This document outlines the key components and their interactions.

## High-Level Architecture

The AP Explanation service follows a layered architecture:

```
┌─────────────────────────────────────────┐
│       FastAPI REST API Layer            │
│                                         │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│      Business Logic (Services) Layer    │
|                                         |
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│    Data Access / Repository Layer       │
|    (Provenance queries and mappings)    |
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│    PostgreSQL + ProvSQL Extension       │
└─────────────────────────────────────────┘
```

## Core Concepts

### Semiring Annotations

The service supports multiple semiring types for provenance tracking:

- **formula**: Tracks the complete lineage formula showing how results derive from source data
- **why**: Provides why-provenance explaining which source tuples contributed to results

Each semiring has:
- A retrieval function (e.g., `formula`, `whyprov_now`)
- An optional aggregate function for combining provenance
- A mapping table for tracking source data
- A mapping strategy (e.g., CTID-based mapping)

### SQL Rewriting

The service includes an SQL rewriter (`internal/sql_rewriter.py`) that transforms queries to work with provenance tracking:
- Rewrites queries to include provenance columns
- Handles aggregations and complex operators
- Ensures compatibility with ProvSQL

## Key Design Patterns

### Dependency Injection

The service uses a DI container (defined in `di.py`) to manage dependencies:
- Service instances are created with their repositories
- Repositories are created with database connections
- Enables easier testing and component isolation