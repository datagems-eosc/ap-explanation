# Usage Guide

## Basic Workflow

### 1. Annotate Tables

Before explaining queries, tables must be annotated (one-time per table):

```bash
curl -X POST http://localhost:5000/api/v1/ap/annotate \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

**Response:**
```json
[
  {
    "table_name": "students",
    "semiring": "formula",
    "status": "success",
    "message": "Table 'students' was successfully annotated with semiring 'formula'"
  }
]
```

### 2. Explain Queries

After annotation, get provenance explanations:

```bash
curl -X POST http://localhost:5000/api/v1/ap/explain \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```


## Specific Semiring

Use a specific semiring instead of all:

```bash
# Annotate with formula semiring only
POST /api/v1/ap/annotate/formula

# Explain with why semiring only
POST /api/v1/ap/explain/why
```

## Remove Annotations

Clean up annotations when done:

```bash
curl -X POST http://localhost:5000/api/v1/ap/remove \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

## Semiring Types

### Formula Semiring
Tracks complete data lineage as algebraic formulas:
- Shows **how** results were computed
- Operators: ⊕ (union), ⊗ (join), ⊖ (difference)

### Why Semiring
Lists source tuples that contributed to results:
- Shows **which** rows were used
- Format: `table_name(row_id)`
