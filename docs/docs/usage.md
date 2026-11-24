# Usage Guide

The API exposes a managed lifecycle for computing provenance explanations.

## Managed lifecycle

The API handles annotation and computation asynchronously via Celery.

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
{
  "task_id": "abc123",
  "status": "success",
  "result": {
    "derivations": [
      {
        "answer": { "name": "Alice" },
        "provenance": { "formula": { "expression": "students@k3fa1…", "data": [...], "truncated": false } },
        "probability": null
      }
    ],
    "explanation": "..."
  },
  "error": null
}
```

On failure, `status` is `failure` and `error` says why, e.g. a [limit](troubleshooting.md#query-stopped-by-a-limit) the query exceeded.

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

ProvSQL keeps the circuit of every provenance query in its store (`provsql_*.mmap` in the database directory), including failed and stopped ones, and never frees it. One costly query can add gigabytes: the meteo use case's 46 MB database reached a 1.6 GB store after its 188 questions, 90% of it from three of them. `pg_database_size()` counts the store.

The service keeps none of the tokens it builds, so after a task that grew its database by 100 MB or more, it reclaims the store with `provsql.circuit_cleanup()`, still holding the database's lock. That keeps the gates the database's tables reference, and frees the rest: 1.6 GB in 0.4 s. A query whose gates were freed runs at its first-time speed the next time it is asked, 0 to 25% slower than with its gates stored; the same request within an hour comes from the cache.

- `circuit_cleanup` refuses to run while another client is connected to the database, after holding new connections back for 5 s. The service skips it then (logged as `Circuit store not reclaimed`), so a database other clients always hold is never reclaimed: schedule it with operations, at a quiet time, in a session on the database:

    ```sql
    SELECT * FROM provsql.circuit_cleanup();
    ```

- It reads the provenance column of every table of the database: a service role that is not a superuser needs `SELECT` on all of them, ProvSQL's own `provsql.update_provenance` included, or the cleanup fails (logged, the task is not affected).
- Requires ProvSQL >= 1.14.

---

## Semiring Types

All semirings use ProvSQL's built-in `sr_*` functions and identify rows by their provenance token.

| Name | ProvSQL function | Description |
|---|---|---|
| `formula` | `sr_formula` (`sr_formula_rows` on aggregates) | Algebraic expression showing how each result was derived. The only one that explains aggregates, cut at `PROVENANCE_MAX_FORMULA_ROWS` rows (`truncated`) |
| `why` | `sr_why` | Flat list of source tuples that contributed to each result row |
| `boolexpr` | `sr_boolexpr` | Boolean provenance expression over source identifiers |
| `how` | `sr_how` | How-provenance: multiset polynomial showing multiplicities |
| `which` | `sr_which` | Which-provenance (lineage): set of contributing tuple identifiers |
