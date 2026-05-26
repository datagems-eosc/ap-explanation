# Usage Guide

The API exposes a managed lifecycle for computing provenance explanations.

## Managed lifecycle

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

## Semiring Types

| Name | Description |
|---|---|
| `formula` | Algebraic formula showing how results derive from source data |
| `why` | Lists source tuples that contributed to each result row |
