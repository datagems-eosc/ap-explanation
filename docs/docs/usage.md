# Usage Guide

The API exposes two lifecycles for computing provenance explanations.

## Managed lifecycle (recommended)

The API handles annotation, computation, and cleanup asynchronously via Celery.

### 1. Dispatch an explanation task

```bash
curl -X POST http://localhost:5000/api/v1/aps/explanation \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

**Response (HTTP 202):**
```json
{ "task_id": "abc123", "status": "pending" }
```

To target a specific semiring:
```bash
POST /api/v1/aps/explanation/{semiring_name}
```

### 2. Poll for the result

```bash
curl http://localhost:5000/api/v1/aps/explanation/abc123
```

**Response when complete:**
```json
{ "task_id": "abc123", "status": "success", "result": [...] }
```

---

## Manual lifecycle

Full control over each step: annotate → compute → remove annotation.

### 1. Annotate tables

```bash
curl -X POST http://localhost:5000/api/v1/aps/explanation/manual/annotations \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

### 2. Compute provenance

```bash
curl -X POST http://localhost:5000/api/v1/aps/explanation/manual/computations \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

> **Note:** `computations` automatically removes annotations after computing, as a workaround for [ProvSQL issue #67](https://github.com/PierreSenellart/provsql/issues/67). Use `DELETE /annotations` only if you need to clean up without computing.

### 3. Remove annotations (optional)

```bash
curl -X DELETE http://localhost:5000/api/v1/aps/explanation/manual/annotations \
  -H "Content-Type: application/json" \
  -d @fixtures/explain_sql_query.json
```

All manual endpoints accept a `/{semiring_name}` suffix to target a specific semiring.

---

## Semiring Types

| Name | Description |
|---|---|
| `formula` | Algebraic formula showing how results derive from source data |
| `why` | Lists source tuples that contributed to each result row |
