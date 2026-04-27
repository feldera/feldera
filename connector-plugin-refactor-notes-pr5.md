# Connector Plugin Refactor — PR 5 Notes

## What was done

Phase 5: Converted `ConnectorFlags` to a `bitflags!`-generated type, added `HTTP_DIRECT` and `AUTO_RECREATED_ON_RESTART` flag constants, registered `http_input`, `http_output`, `adhoc_input`, and `datagen` descriptors, and rewrote four scattered controller special cases as descriptor lookups.

### Files changed

| File | Change |
|------|--------|
| `crates/adapterlib/Cargo.toml` | Added `bitflags = "2"` dependency. |
| `crates/adapterlib/src/connector.rs` | Replaced manual `ConnectorFlags(u32)` struct with `bitflags!` macro; added `HTTP_DIRECT = 0x01` and `AUTO_RECREATED_ON_RESTART = 0x02` flag constants; kept `EMPTY` as `Self::empty()` const; updated `connector_flags_contains` test to use named flag constants. |
| `crates/adapters/src/transport/clock.rs` | Changed `flags: ConnectorFlags::EMPTY` to `flags: ConnectorFlags::AUTO_RECREATED_ON_RESTART` in `CLOCK_DESCRIPTOR`. |
| `crates/adapters/src/transport/http/input.rs` | Added `HTTP_INPUT_DESCRIPTOR` with `HTTP_DIRECT` flag, `Transient` kind, `build_input: Some(build_http_input)`; `inventory::submit!`. |
| `crates/adapters/src/transport/http/output.rs` | Added `HTTP_OUTPUT_DESCRIPTOR` with `Transient` kind, `build_output: None` (server creates it directly); `inventory::submit!`. |
| `crates/adapters/src/transport/adhoc.rs` | Added `ADHOC_INPUT_DESCRIPTOR` with `Transient` kind, `build_input: Some(build_adhoc_input)`; `inventory::submit!`. |
| `crates/datagen/Cargo.toml` | Added `inventory = { workspace = true }` dependency. |
| `crates/datagen/src/lib.rs` | Added `DATAGEN_DESCRIPTOR` with `default_format: Some(datagen_default_format)`, `Regular` kind, `build_input: Some(build_datagen_input)`; `inventory::submit!`. |
| `crates/adapters/src/transport.rs` | Removed now-unused `use adhoc::AdHocInputEndpoint`, `use http::HttpInputEndpoint`, `use feldera_datagen::GeneratorEndpoint` imports; moved `HttpInput`, `AdHocInput`, `Datagen` arms to the `Ok(None)` catch arm (they are now registry-dispatched). |
| `crates/adapters/src/controller.rs` | Added `use feldera_adapterlib::connector::{ConnectorFlags, connector_by_name}`; removed `use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat}` and `use feldera_types::format::json::JsonLines` (no longer needed); rewrote `is_http_input()` → `HTTP_DIRECT` flag lookup; rewrote `ClockInput` match → `AUTO_RECREATED_ON_RESTART` flag lookup; rewrote `Datagen` default format → `descriptor.default_format` lookup. |
| `crates/adapters/src/controller/pipeline_diff.rs` | Added `use feldera_adapterlib::connector::{ConnectorKind, connector_by_name}`; rewrote two `is_transient()` uses → `descriptor.kind == ConnectorKind::Transient` lookups. |

---

## Insights and factors not accounted for in the plan

### 1. Phase 5 partially does Phase 4b work

The plan stages Phase 4b (sweeping remaining connectors) AFTER Phase 6. But Phase 5's goal — replacing `is_http_input()`, `ClockInput` filter, `Datagen` default format, and `is_transient()` with descriptor lookups — requires that `http_input`, `http_output`, `datagen`, and `adhoc_input` descriptors already exist in the registry. This means Phase 5 necessarily migrates those four connectors to the registry ahead of the planned Phase 4b order.

In practice, this is fine: the Phase 4a two-phase factory pattern already handles it. Adding a descriptor causes the registry path to dispatch the connector before the fallback match is reached, so the match arms become dead code and are safely moved to the `Ok(None)` catch arm. No functionality change.

**Plan update**: Document that Phase 5 implicitly migrates `http_input`, `http_output`, `datagen`, `adhoc_input` to the registry as a prerequisite for the descriptor-lookup rewrites. Phase 4b's sweep then covers the remaining connectors (`s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, plus integrated).

### 2. `http_output` has no `BuildOutputFn` (server creates it directly)

`HttpOutputEndpoint` requires `name`, `format`, and `backpressure` parameters that come from the HTTP request at runtime (see `server.rs:2238`), not from a stored JSON config. These parameters are not part of `TransportConfig::HttpOutput` (which is a unit variant with no inner config). Therefore the `HTTP_OUTPUT_DESCRIPTOR` has `build_output: None`.

The output factory's registry dispatch path handles this correctly: `connector_by_name("http_output")` finds the descriptor, `descriptor.build_output` is `None`, so the factory returns `Ok(None)` — same behavior as the existing `_ => Ok(None)` fallback. The descriptor exists purely to expose `kind: ConnectorKind::Transient` for controller lookups.

**Plan update**: Note that some connectors may register descriptors with `build_*: None` for metadata purposes only (kind/flags queries), without replacing the factory path. This is valid design — the descriptor is not required to provide a build function.

### 3. `bitflags` was not a workspace dependency

The plan says "The `bitflags` crate is already a transitive dep of the workspace, so promoting it to a direct dep is zero incremental build cost." This is true for incremental build cost, but `bitflags` was not in the workspace `[dependencies]` table in `Cargo.toml`. Adding `bitflags = "2"` directly to `adapterlib/Cargo.toml` (not via `{ workspace = true }`) is correct; it pins the version to 2.x in the one place that needs it.

**Consideration**: If future crates also need `bitflags`, add it to the workspace `[dependencies]` table at that point. For now, a single direct dep in adapterlib is simpler.

### 4. `datagen` needed `inventory` as a direct dependency

The `register_connector!` macro (from `feldera_adapterlib`) expands to `::inventory::submit! { ... }`. The `::inventory` path resolves to the `inventory` crate, which must be a direct dependency of the calling crate. The `datagen` crate only had `feldera-adapterlib` (which itself depends on `inventory`), but that transitive dep is not accessible as `::inventory`. Adding `inventory = { workspace = true }` to `datagen/Cargo.toml` is the correct fix.

**Lesson**: Any crate that calls `inventory::submit!` (directly or via a macro that expands to it) must list `inventory` as a direct dependency, even if a transitive dep already has it.

### 5. `default_format` function moves JSON-specific knowledge to `datagen`

Before Phase 5, the JSON datagen format (format name `"json"`, `JsonFlavor::Datagen`, `JsonUpdateFormat::Raw`, `array: true`, `lines: Multiple`) was hardcoded in `controller.rs`. After Phase 5, this knowledge lives in `datagen/src/lib.rs:datagen_default_format()`.

This is the correct placement — the datagen connector owns its default format. The controller no longer needs to import `JsonParserConfig`, `JsonFlavor`, `JsonUpdateFormat`, or `JsonLines` for this purpose; all four imports were removed.

**Side effect**: the controller's error message for "custom format not supported" is now generic (`"{name} endpoints do not support custom formats: ..."` instead of `"datagen endpoints do not support custom formats: ..."`). This makes the mechanism work for any future connector that also sets `default_format` but rejects user-specified formats. Tests that match on the exact error text may need updating if they depend on the `"datagen"` literal in the message.

### 6. `is_transient()` in `pipeline_diff.rs` requires ALL transient connectors in the registry

`is_transient()` returns `true` for `AdHocInput`, `HttpInput`, `HttpOutput`, and `ClockInput`. The replacement `connector_by_name(...).map(|d| matches!(d.kind, ConnectorKind::Transient)).unwrap_or(false)` only returns `true` for connectors IN the registry. If any transient connector is NOT registered, its `pipeline_diff.rs` filtering would silently fail (treating it as non-transient).

In Phase 5 we ensured all four transient connectors are registered:
- `clock`: Phase 4a
- `http_input`: Phase 5
- `http_output`: Phase 5
- `adhoc_input`: Phase 5

**Plan update**: Document this registry completeness requirement. Whenever a new transient connector is added, it MUST also register a descriptor with `ConnectorKind::Transient`. The `is_transient()` method on `TransportConfig` should be deprecated/removed only after all transient connectors have registered descriptors (completed in Phase 5 for existing connectors; must be maintained for future additions).

### 7. `ConnectorFlags::EMPTY.contains(ConnectorFlags::EMPTY)` still holds

The `bitflags`-generated `contains()` uses `self.intersection(other) == other`, so `empty().contains(empty())` evaluates `empty().intersection(empty()) == empty()` → `empty() == empty()` → `true`. This matches the old manual implementation `(0 & 0) == 0` → `true`. No behavioral change.

### 8. `HttpOutput` descriptor in the output factory returns `Ok(None)` correctly

When the output factory encounters `TransportConfig::HttpOutput`:
1. `config.name()` → `"http_output"`
2. `connector_by_name("http_output")` → finds `HTTP_OUTPUT_DESCRIPTOR`
3. `descriptor.build_output` is `None` → returns `Ok(None)`

This is identical to the previous behavior where `HttpOutput` fell through to `_ => Ok(None)`. The descriptor lookup adds zero overhead (the fallback was already there).

### 9. Connector flag values are now stable constants

`HTTP_DIRECT = 0x01` and `AUTO_RECREATED_ON_RESTART = 0x02` are now defined as named `bitflags!` constants. Future flags should use the next available bit (`0x04`, `0x08`, etc.) to avoid collisions. The `EMPTY` constant is defined as `Self::empty()` for source compatibility with existing code.

### 10. Tests verified

- 11 adapterlib tests pass (including updated `connector_flags_contains`)
- 3 file/clock transport tests pass (registry dispatch still works)
- 87 format tests pass

---

## Plan updates to carry forward

1. **Phase 5 implicitly migrates 4 connectors**: `http_input`, `http_output`, `datagen`, `adhoc_input` are moved to the registry in Phase 5 (not Phase 4b). Phase 4b then covers `s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, and integrated.

2. **`build_*: None` descriptors are valid**: A connector can register a descriptor with all `build_*` fields as `None` purely for metadata (kind/flags). Document this as a valid pattern for connectors whose endpoints are constructed through non-standard paths (e.g., HTTP server creates `HttpOutputEndpoint` directly).

3. **`inventory` must be a direct dep for `submit!`**: Any crate calling `inventory::submit!` (or a macro wrapping it) needs `inventory` as a direct dep. Update the connector authoring guide accordingly.

4. **Phase 4b remaining connectors**: `s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, integrated (`postgres`, `delta_table`, `iceberg`). After Phase 5, the `Ok(None)` catch arm in the input factory contains: `FileInput`, `ClockInput`, `HttpInput`, `AdHocInput`, `Datagen`, plus all output-only variants. Phase 4b removes these as they are migrated.

5. **Error message change**: The datagen "custom format not supported" error message is now generic. If any test asserts on the literal `"datagen endpoints do not support custom formats"`, it needs updating to match the new message format.
