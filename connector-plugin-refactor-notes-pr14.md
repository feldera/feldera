# PR 14 — OpenAPI surface for connector endpoints: Discrepancies from Plan

### 1. Endpoints were not missing their `#[utoipa::path]` annotations — only the `paths()` registration was absent

The plan says "Add OpenAPI schemas for…" as if the utoipa annotations needed to be written. In practice, `connectors.rs` already had complete `#[utoipa::path]` annotations on all four handlers (from PR 10). The only missing pieces were: (a) the four entries in the `paths()` macro in `api/main.rs`, (b) the three schema types in `components(schemas(…))`, and (c) the `tag` field on each annotation. The plan's phrasing should distinguish "wire existing annotations into the spec" from "write annotations from scratch" to avoid over-scoping the work.

### 2. `StatusName` visibility must be at least `pub(crate)` for `components(schemas(…))`

`StatusName` was declared as a fully-private `enum` (no `pub`). The `components(schemas(…))` macro in `main.rs` refers to it via its full path (`crate::api::endpoints::connectors::StatusName`), which requires at minimum `pub(crate)` visibility. Private types referenced there produce a compile error, not a silent omission. The plan does not mention visibility as a concern for schema registration; it should note that types added to `components(schemas(…))` must be `pub(crate)` or wider.

### 3. `openapi.json` regeneration requires building the full binary, not just `cargo check`

`cargo check` verifies the utoipa annotations are structurally valid but does not produce `openapi.json`. Regeneration requires `cargo build -p pipeline-manager --bin pipeline-manager` followed by `./target/debug/pipeline-manager --dump-openapi`. The build pulls in the full dependency graph (adapters, dbsp, sqllib) and takes roughly 2× as long as `cargo check` alone. The plan should call this out explicitly so the reviewer knows to recheck the JSON file after any annotation change.

### 4. `rest-api/build.rs` type replacements do not need updating for connector-management types

The plan warns to keep `rest-api/build.rs:38-208` unchanged. This is correct and unambiguous: the connector-management response types (`ConnectorsStatusResponse`, `ConnectorsConfigPutResponse`, `StatusName`) are internal to the pipeline-manager crate and have no `feldera_types` equivalents, so progenitor generates them as new Rust structs from the schema. No entry in `type_replacement()` is needed or appropriate. The warning in the plan is accurate but could be clearer: the concern is that the existing typed connector transport schemas (Kafka, Delta, etc.) are mapped to `feldera_types` types and must remain that way; the new connector-management types are unrelated and are correctly left for progenitor to generate.
