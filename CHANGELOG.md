# Changelog

## Unreleased

### New Features

- **`WORKER_CONCURRENCY`** — tasks a worker runs at once, embedded or standalone (default 2, as before for the embedded worker). A standalone `celery worker` now runs 2 by default, instead of one per CPU, unless given `--concurrency`.

### Docs

- How many queries a 4 GB server runs at once, and how to bound them with `PROVENANCE_MIN_RESERVED_ROWS`.
- Architecture, usage, troubleshooting, README and `.env.example` brought up to date with 2.0.0.

## v2.0.0 — 2026-11-23

### Breaking Changes

- **ProvSQL 1.14 required** — queries use ProvSQL's possible-worlds functions (`present()`, `LIMIT plain(k)`, `replace_input`, `sr_formula_rows`). Upgrade every database server first: the `postgres-provsql:17-v1.14.0` image upgrades the extension of an existing volume on start.
- **No mapping tables** — annotating a table only adds ProvSQL's `provsql` column; rows are labelled through a temporary view. On a database annotated by an earlier version, drop the leftovers by hand (`provsql_ref` column, `<table>_provkey` and `<table>_prov<semiring>` tables, `*_mapping` views), or `provsql_ref` shows in `SELECT *` answers. Row references now change when a row's probability changes. The database role needs the `TEMP` privilege (granted by default).
- **APs are validated when posted** — Data nodes need a `name`, the provenance operator a `query`, and CSV nodes a `contentUrl`. An invalid AP gets a 422 instead of a failed task.
- **DISTINCT queries return one row per result** — with the provenance of all the tuples it merges, instead of one row per tuple.
- **Configuration is checked at startup** — an invalid value (`LLM_TIMEOUT=abc`) stops the service. `LLM_SSL_VERIFY` and `USE_EMBEDDED_CELERY_WORKER` accept `true/false`, `1/0`, `yes/no`, `on/off`; empty now means `true` (was `false`).

### New Features

- **Result probabilities** — `?probability=true` returns each result row's probability. Tuple probabilities come from the column named by a `Table`/`CSV` node's `probabilityColumn`; other tables are certain.
- **Query limits** — each provenance query runs with `statement_timeout` (10 min), `provsql.max_memory` (1 GB), `temp_file_limit` (5 GB) and no parallel workers, set with `PROVENANCE_*` variables. A query past a limit fails with an error naming it.
- **Large aggregates refused before they run** — a plain `EXPLAIN ANALYZE` run counts the rows each aggregate receives; past `PROVENANCE_MAX_AGGREGATED_ROWS` (8 million), or if that run exceeds `PROVENANCE_PRECHECK_TIMEOUT` (180 s), the query is refused instead of crashing the database server. The budget is shared by all queries of a server, which wait their turn.
- **Aggregate formulas truncated** — an aggregate's formula is returned as its first `PROVENANCE_MAX_FORMULA_ROWS` (100) lines, with `truncated` set, so aggregates over millions of rows are explained in seconds.

### Misc

- The per-database lock and the row budget share one Redis semaphore. The lock is renewed while a task runs and expires a minute after a crash (was an hour). Deploy with no task running: the lock's Redis key changes type.
- A task that waited for the lock checks the cache again before running.
- `containedIn` is read from item to container, as MoMa stores it; the reverse direction is still accepted.
- The schema given to the LLM now lists its tables' columns and primary keys.
- Cached results are keyed on `probability` too: older cache entries are not reused.
- Internal modules reorganised (`domains/provenance`, `domains/nl_explanation`, `repositories`, `api/middlewares`).

## v1.0.0 — 2026-08-26

### Breaking Changes

- **provsql 1.12.0 required** — Existing database volumes upgrade automatically on restart via `ALTER EXTENSION provsql UPDATE` in the entrypoint; annotating a table afterwards (`add_semiring`) migrates its mapping to the new format
- **AP labels now follow the MoMa vocabulary** — `Relational_Database` → `RelationalDatabase`, `CSV_Set` → `CsvSet`, and the `contain` edge → `containedIn`. APs using the old labels are rejected. The `Provenance_SQL_Operator` / `Provenance_Annotate_Dataset_Operator` operators are unchanged — MoMa has no provenance node types yet.
- **APs are validated against the MoMa graph schema** — node and edge ids must be valid UUIDs, every node must carry a `properties` object whose keys match `^[a-z@][a-zA-Z0-9_]*$`, and edge labels are restricted to MoMa's closed set. Any string was previously accepted.

### New Features

- **Stable row references**:  Instead of using CTIDs, every annotated table now carries a `provsql_ref` column instead of relying on physical row location. `CtidMapping` is removed; `KeyMapping` replaces it. 
- **AP models come from `moma-domain`** — the hand-maintained `PgJson` copy is gone; `AnalyticalPattern` now builds on `moma_management.domain`, so the graph vocabulary stays owned by moma-management. Only moma's `StructureStep` runs, not its full `SchemaStep & StructureStep & MappingStep` chain, since `SchemaStep` would reject the provenance operators.

### Misc

- Provsql >=1.11.0 add_provenance() and remove_provenance() are now idempotent, unnecessary workaround were removed as a consequence.

## v0.12.0 — 2026-06-01

### Breaking Changes

- **Removed manual endpoints** — `POST /api/v1/aps/explanation/manual/annotations`, `POST /api/v1/aps/explanation/manual/computations`, and `DELETE /api/v1/aps/explanation/manual/annotations` have been removed. Use the managed lifecycle (`POST /api/v1/aps/explanation`) instead.
- **`aggregate_function` removed from `DbSemiring`** — The `aggregate_function` field on semiring definitions has been removed. Both aggregate and non-aggregate queries now use `retrieval_function`. Custom semiring configurations that set `aggregate_function` must be updated.
- **Semiring names and functions changed** — The `boolean` semiring has been renamed to `boolexpr`. All semiring retrieval functions have been renamed to match their native provsql equivalents: `formula` → `sr_formula`, `whyPROV_now` → `sr_why`, `bool_formula` → `sr_boolexpr`. The custom SQL semiring implementation (`03_setup_semiring_parallel.sql`) has been removed; provsql's built-in C++ semirings are now used directly.
- **provsql 1.8.0 required** — The postgres-provsql image has been updated to `sotrx/postgres-provsql:17-v1.8.0`. Environments running an older provsql extension must upgrade.

### New Features

- **Authentication (OIDC)** — All explanation endpoints now support bearer token authentication via OIDC. Set `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, and `OIDC_EXCHANGE_SCOPE` to enable it. When `OIDC_ISSUER` is unset, authentication is disabled and endpoints remain publicly accessible. `UserId` and `ClientId` are now bound to structured logs per request.
- **`how` and `which` semirings** — Two new built-in semirings (`how`, `which`) are now available, backed by provsql's native `sr_how` and `sr_which` C++ functions.
- **Automatic provsql extension upgrade** — The postgres-provsql container now runs `ALTER EXTENSION provsql UPDATE` on every start. Existing database volumes are automatically upgraded when the image is rebuilt with a newer provsql version; no manual intervention required.
- **Semiring dropdown in API** — The explanation endpoint now exposes available semirings as a selectable list.
- **`build-postgres-provsql` Makefile rule** — Added a convenience target to build the postgres-provsql image locally.

### Bug Fixes

- **Correlation ID always present in response headers** — The `x-tracking-correlation` header is now set on all responses, including error responses. Previously, unhandled exceptions caused the header to be omitted.
- **Authentication dependency not invoked** — `require_authentication` was passed to `Depends()` without being called, causing FastAPI to inject the factory function itself rather than executing the auth check. Fixed to `Depends(require_authentication())`.
- **`POST /api/v1/aps/explanation/{semiring_name}` unauthenticated** — The semiring-specific explanation endpoint was missing the `require_authentication()` dependency entirely.
- **`boolexpr` semiring returning empty data** — The `sr_boolexpr` function requires a `mapping_table` argument in provsql 1.8.0. The rewriter was not passing it, causing `data: []` in all responses. Fixed by ensuring the mapping argument is included when `mapping_table` is set on the semiring.
