# Changelog

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
