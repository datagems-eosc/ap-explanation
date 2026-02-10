# Usage Guide

## Basic Workflow

### 1. Annotate Tables

Before explaining queries, tables must be annotated (one-time per table):

```bash
curl -X POST http://localhost:5000/api/v1/aps/annotate \
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
curl -X POST http://localhost:5000/api/v1/aps/explain \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

**Important:** The `/explain` endpoint **automatically removes provenance annotations** from tables after computing the provenance. This is a workaround to mitigate a known issue in ProvSQL ([issue #67](https://github.com/PierreSenellart/provsql/issues/67)) where leaving provenance enabled can block certain database operations. 

**Implications:**
- After calling `/explain`, tables will need to be **re-annotated** before you can explain queries again
- This makes provenance computation more expensive, but it's necessary to prevent database blocking
- If you need to explain multiple queries, consider doing so sequentially in a single session before the annotations are removed


## Specific Semiring

Use a specific semiring instead of all:

```bash
# Annotate with formula semiring only
POST /api/v1/aps/annotate/formula

# Explain with why semiring only
POST /api/v1/aps/explain/why
```

## Remove Annotations

Clean up annotations when done:

```bash
curl -X DELETE http://localhost:5000/api/v1/aps/annotate \
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
