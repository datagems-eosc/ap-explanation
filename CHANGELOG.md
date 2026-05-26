# Changelog

## Unreleased

### Breaking Changes

- **Removed manual endpoints** — `POST /api/v1/aps/explanation/manual/annotations`, `POST /api/v1/aps/explanation/manual/computations`, and `DELETE /api/v1/aps/explanation/manual/annotations` have been removed. Use the managed lifecycle (`POST /api/v1/aps/explanation`) instead.

### New Features

- **Authentication (OIDC)** — All explanation endpoints now support bearer token authentication via OIDC. Set `OIDC_ISSUER`, `OIDC_CLIENT_ID`, `OIDC_CLIENT_SECRET`, and `OIDC_EXCHANGE_SCOPE` to enable it. When `OIDC_ISSUER` is unset, authentication is disabled and endpoints remain publicly accessible. `UserId` and `ClientId` are now bound to structured logs per request.

### Bug Fixes

- **Correlation ID always present in response headers** — The `x-tracking-correlation` header is now set on all responses, including error responses. Previously, unhandled exceptions caused the header to be omitted.
- **Authentication dependency not invoked** — `require_authentication` was passed to `Depends()` without being called, causing FastAPI to inject the factory function itself rather than executing the auth check. Fixed to `Depends(require_authentication())`.
- **`POST /api/v1/aps/explanation/{semiring_name}` unauthenticated** — The semiring-specific explanation endpoint was missing the `require_authentication()` dependency entirely.
