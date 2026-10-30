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

## Probabilities

Add `?probability=true` to either dispatch endpoint to also compute, for each result row, the probability that it belongs to the result (ProvSQL's `probability_evaluate`). It is returned as `probability` on each derivation, and is `null` when not requested.

Tuple probabilities come from a column of each queried table, named by the `probabilityColumn` property of its `Table` (or `CSV`) node:

```json
{
  "labels": ["Table"],
  "properties": {
    "name": "mathe.assessment",
    "probabilityColumn": "reliability"
  }
}
```

- Tables without `probabilityColumn`, and NULL values in the column, count as certain (probability 1).
- Values must be numeric and within [0, 1]; otherwise the task fails with an `Invalid probability column` error.
- For a `GROUP BY` query, the probability is that of the group existing, i.e. at least one of its tuples being present.
- Probabilities are exact, computed with ProvSQL's default method. Hard queries (e.g. `SUM` over large values) may be slow.

---

## Reclaiming the circuit store

ProvSQL keeps the circuit of every provenance query in its store (`provsql_*.mmap` in the database directory), including failed and stopped ones, so the store only grows. One costly query can add gigabytes. To reclaim it:

```bash
curl -X POST http://localhost:5000/api/v1/aps/explanation/maintenance/circuit-cleanup/mathe
# {"task_id": "...", "status": "pending"}
```

Poll the task like an explanation. On success its result holds ProvSQL's counts before and after (`gates_before`, `gates_after`, `wires_before`, ...).

- The task waits for the explain tasks running on the database (it takes their per-database lock), and none starts until it ends.
- Like `DROP DATABASE`, `provsql.circuit_cleanup()` refuses to run while another session is connected to the database. The task retries for `CIRCUIT_CLEANUP_WAIT` seconds, then fails, listing who is connected. Other clients of the database must be disconnected first, so schedule it with operations, at a quiet time.
- Requires ProvSQL >= 1.13.

---

## Semiring Types

All semirings use ProvSQL's built-in `sr_*` functions and identify rows by their provenance token.

| Name | ProvSQL function | Description |
|---|---|---|
| `formula` | `sr_formula` | Algebraic expression showing how each result was derived |
| `why` | `sr_why` | Flat list of source tuples that contributed to each result row |
| `boolexpr` | `sr_boolexpr` | Boolean provenance expression over source identifiers |
| `how` | `sr_how` | How-provenance: multiset polynomial showing multiplicities |
| `which` | `sr_which` | Which-provenance (lineage): set of contributing tuple identifiers |
