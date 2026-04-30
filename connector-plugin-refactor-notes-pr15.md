# PR 15 — Web console in-product `connectors.toml` editor: Discrepancies from Plan

### 1. `PluginTransportConfig` schema was absent from `openapi.json`

The `TransportConfig` one-of in `openapi.json` already referenced `#/components/schemas/PluginTransportConfig` (added as part of the Phase 7 Rust work), but the schema definition was never listed in `main.rs`'s `components(schemas(...))` macro. The SDK generator (`@hey-api/openapi-ts`) failed with "Reference not found: PluginTransportConfig" when regenerating the SDK. Fix: add `feldera_types::config::PluginTransportConfig` to the `components(schemas(...))` list, rebuild the binary, and re-dump `openapi.json`.

### 2. Every endpoint consumed via `mapResponse` must declare a typed error response in utoipa

`mapResponse` requires `E extends { message: string }`. If an `#[utoipa::path]` annotation declares no error responses, the SDK generator emits `unknown` for the error type, which does not satisfy that constraint and causes a TypeScript compile error.

Rule: for every endpoint whose SDK-generated function is wrapped with `mapResponse`, add at least `(status = INTERNAL_SERVER_ERROR, body = ErrorResponse)` to the `responses(...)` list and import `use feldera_types::error::ErrorResponse`.

### 3. Integration / e2e tests deferred

The plan requires two integration/e2e tests (happy-path Apply cycle and ETag mismatch 412 path). These require a live Feldera instance with the Phase 8 describer infrastructure, which is not yet deployed. Both checklist items remain open; they are deferred to the integration test pass in a follow-up PR.

### 4. `If-None-Match` polling not implemented (intentional simplification)

The plan says to poll `GET /v0/connectors/status` with `If-None-Match` for cheap 304 responses. The SDK-generated client and the `@hey-api/client-fetch` abstraction do not naturally surface HTTP status codes for successful responses (304 is treated as a redirect at the fetch layer in most environments). The current implementation polls with unconditional GETs every 3 s, which is acceptable for the initial in-product editor. Adding `If-None-Match` support can be done in a follow-up once the polling interval and server load are characterised in production.
