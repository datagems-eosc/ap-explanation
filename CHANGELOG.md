# Changelog

## Unreleased

### Breaking Changes

- **DISTINCT queries return one derivation per result row** — semirings are now evaluated outside the `DISTINCT`, as they already were for `GROUP BY`. Previously the semiring value was part of the `DISTINCT` key, so a result row merged from several tuples came back once per tuple, each with that tuple's provenance only; it now comes back once, with provenance combining all of them.

- **One mapping table per table** — annotating a table creates `<table>_provkey`, shared by every semiring, instead of five identical `<table>_prov<semiring>` tables (5 × 5 GB for 41 million rows). Tables annotated by earlier versions keep working, and are migrated the next time they are annotated: one per-semiring table is renamed, the others dropped.

### New Features

- **Aggregates over too many rows are refused before they run** — ProvSQL builds a gate per aggregated row while the query runs, before `provsql.max_memory` applies: past a few million rows on a 4 GB server, its store worker is killed and the server restarts. The query now first runs without provenance, under `EXPLAIN ANALYZE` (PostgreSQL's estimates are far off on joins on expressions), and is refused with `QueryLimitExceededError` if an aggregate receives more than `PROVENANCE_MAX_AGGREGATED_ROWS` rows (8,000,000, for ProvSQL 1.14; 2,000,000 to 4,500,000 on 1.13.0, see the configuration docs) or the plain run exceeds `PROVENANCE_PRECHECK_TIMEOUT` (180s).

- **Provenance query limits** — each provenance query runs with `statement_timeout` (`10min`), `provsql.max_memory` (`1GB`), `temp_file_limit` (`5GB`) and no parallel workers, configurable with `PROVENANCE_*` variables. A query past them fails with an error naming the limit, rather than running until the database dies.
- **Circuit cleanup** — `POST /api/v1/aps/explanation/maintenance/circuit-cleanup/{db_name}` runs ProvSQL's `circuit_cleanup()` under the database's lock (ProvSQL >= 1.13).
- **Result probabilities** — `POST /api/v1/aps/explanation[/{semiring_name}]?probability=true` evaluates each result row's probability with ProvSQL's `probability_evaluate`, returned as `probability` on each derivation. Tuple probabilities are read from the column named by each `Table`/`CSV` node's `probabilityColumn` property; tables without one are certain.

### Misc

- Cached results are keyed on the new `probability` flag too, so entries cached before the upgrade are not reused.
- The per-database lock is extended while its work runs, so an hour-long annotation no longer outlives it (letting a second worker in, then failing a finished task on release). It now expires 5 minutes after a crashed holder, instead of an hour.

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
