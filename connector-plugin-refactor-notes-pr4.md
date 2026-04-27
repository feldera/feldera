# Connector Plugin Refactor — PR 4 Notes

## What was done

Phase 4a: Migrated `file` and `clock` connectors onto the `ConnectorDescriptor` registry.

### Files changed

| File | Change |
|------|--------|
| `crates/adapters/src/transport/file.rs` | Added `FILE_INPUT_DESCRIPTOR` and `FILE_OUTPUT_DESCRIPTOR` statics with `build_file_input` / `build_file_output` factory functions; `inventory::submit!` for both. |
| `crates/adapters/src/transport/clock.rs` | Added `CLOCK_DESCRIPTOR` static with `build_clock_input` factory function; `inventory::submit!`. |
| `crates/adapters/src/transport.rs` | Added `connector_by_name` import and `JsonValue` import; added `transport_config_inner_as_json` helper; added registry-dispatch path before each factory match; removed `FileInput`/`ClockInput` arms from input match (moved to `Ok(None)` arm); removed `FileOutput` arm from output match (falls to `_ => Ok(None)`); removed now-unused `use clock::ClockEndpoint` and `use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint}` imports. |

### Descriptor names

The `ConnectorDescriptor.name` field matches `TransportConfig::name()` return values (not serde tag names):

| Variant | `name()` | Serde tag (`rename_all = "snake_case"`) |
|---------|----------|----------------------------------------|
| `FileInput` | `"file_input"` | `"file_input"` (matches) |
| `FileOutput` | `"file_output"` | `"file_output"` (matches) |
| `ClockInput` | `"clock"` | `"clock_input"` (diverges) |

The `name()` method is what the registry lookup uses (`connector_by_name(&config.name())`), so the descriptor name must match `name()`.

### Registry dispatch structure

Both factory functions now follow the same two-phase pattern:

```rust
// Phase 1 — registry path (migrated connectors)
let name = config.name();
if let Some(descriptor) = connector_by_name(&name) {
    return match descriptor.build_input {   // or build_output
        Some(build_fn) => {
            let config_value = transport_config_inner_as_json(&config)?;
            Ok(Some(build_fn(&config_value, endpoint_name, secrets_dir)?))
        }
        None => Ok(None),
    };
}

// Phase 2 — fallback match (unmigrated connectors)
match config { ... }
```

`transport_config_inner_as_json` serialises the whole `TransportConfig` to `{"name": "...", "config": {...}}` and extracts the `"config"` field. For unit variants (`HttpOutput`) that have no content, it returns `JsonValue::Null`.

### `ClockKind::Transient`

The clock connector is classified as `ConnectorKind::Transient` in its descriptor (matching the existing `is_transient()` return value in `TransportConfig::is_transient()`). The `ConnectorKind::Transient` variant means "controller creates and destroys this connector automatically rather than user-configured persistence". This is consistent with what the plan defines for that kind.

### `config_schema` placeholder

Both file and clock descriptors use `fn() -> JsonValue::Object(Default::default())` (empty JSON object) as the `config_schema` implementation. The config schema field will be populated with real JSON Schema in Phase 9 when the `GET /v0/connectors` discovery endpoint is added. An empty object is correct for Phase 4a — it's not queried until Phase 9.

---

## Insights and factors not accounted for in the plan

### 1. `name()` vs serde tag mismatch for `ClockInput`

`TransportConfig::ClockInput` has `#[serde(rename_all = "snake_case")]` which gives it the serde tag `"clock_input"`, but `TransportConfig::name()` returns `"clock"`. This inconsistency predates this refactor. The descriptor name must match `name()` (since that is what registry lookup uses in the factory), not the serde tag (which is only relevant for serialisation/deserialisation of `TransportConfig` from JSON). **The descriptor name for the clock connector is `"clock"`, not `"clock_input"`.**

This means: if a user sends `{"name": "clock_input", "config": {...}}` over the API, serde deserialises it to `TransportConfig::ClockInput`, and then `name()` returns `"clock"`, and the registry lookup succeeds. The mismatch is invisible at the call site because `name()` is the only bridge between the typed variant and the registry string. **Update the plan** to note that descriptor names must equal `name()`, not the serde tag.

### 2. Secrets resolution happens before registry dispatch

`resolve_secret_references_via_json` is called first (before registry dispatch), so by the time `build_fn` is called the `JsonValue` passed to it is already fully resolved. The `secrets_dir: &Path` argument on `BuildInputFn` / `BuildOutputFn` is redundant for connectors that let the factory resolve secrets before calling the descriptor. External connectors calling `build_fn` directly (bypassing the factory) will still need `secrets_dir` if they need to resolve their own secrets. This is correct by design — the type signature is stable.

### 3. `transport_config_inner_as_json` must be in `adapters`, not `adapterlib`

The helper converts a `TransportConfig` (which lives in `feldera-types`, a heavy crate) to `JsonValue`. `adapterlib` does not depend on `feldera-types::config::TransportConfig` — its `BuildInputFn` takes `&JsonValue` precisely to avoid that dependency. Therefore the helper belongs in `adapters/src/transport.rs` (which does depend on `feldera-types`), not in `adapterlib`. External connectors never need this helper — they receive an already-extracted `JsonValue` from the factory.

### 4. Removing `ClockEndpoint` import from `transport.rs`

`use clock::ClockEndpoint;` was previously imported in `transport.rs` to construct `ClockEndpoint::new(config)` in the match arm. After migrating clock to the registry, the clock module's `build_clock_input` function creates the endpoint internally. The import in `transport.rs` is now unused and was removed. There is no break in the public API since `ClockEndpoint` was never a public export of `transport.rs`.

### 5. `FileInputEndpoint` / `FileOutputEndpoint` imports removed from `transport.rs`

Similarly, `use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint};` was only used in the factory match arms. After registry dispatch, the `build_file_input` / `build_file_output` functions in `file.rs` create the endpoints directly. The import in `transport.rs` is no longer needed.

### 6. Dead `FileInput`/`ClockInput`/`FileOutput` arms in the match

After adding registry dispatch, the `FileInput`, `ClockInput`, and `FileOutput` match arms in the factory functions became dead code — the registry dispatch short-circuits before reaching them. Rather than leaving them as dead code (which might generate "unreachable pattern" warnings in a future Rust edition or with stricter lints), I moved `FileInput` and `ClockInput` into the `Ok(None)` catch arm of the input factory, and `FileOutput` falls into the existing `_ => Ok(None)` arm of the output factory. This is safe: if for any reason the registry lookup fails (e.g., a test binary that doesn't link `dbsp_adapters`), the factory returns `None` rather than panicking. In practice, when `dbsp_adapters` is linked, the registry always has these connectors.

### 7. The output factory's catch-all `_ => Ok(None)` already covers `FileOutput`

The output factory had an explicit `FileOutput` arm before. After registry migration, `FileOutput` is handled by the registry dispatch before reaching the match. The remaining match uses `_ => Ok(None)` as a catch-all. Since `FileOutput` would match that arm anyway (it returns `None` if the registry somehow missed it), removing the explicit arm is equivalent but cleaner.

### 8. Phase 4a doesn't touch `ConnectorKind::Transient` semantics

The plan note says: "Move any per-variant capability bit (transient / auto-recreated / default format) into the descriptor — depends on Phase 5 having added the relevant fields". However, `ConnectorKind::Transient` already exists in the Phase 2 `ConnectorDescriptor` (it was defined as a kind variant, not a flags bit). The clock descriptor correctly uses `kind: ConnectorKind::Transient`. No Phase 5 dependency needed for this particular case. The Phase 5 work would add `ConnectorFlags::AUTO_RECREATED_ON_RESTART` as a distinct flag bit (from the `ClockInput` arm in `controller.rs:4521`), which is a different concept.

### 9. Test scope verification

The three affected tests (`transport::file::test::test_csv_file_nofollow`, `test_csv_file_follow`, `transport::clock::test::test_clock`) all pass after migration. The tests construct `mock_input_pipeline` (which calls `input_transport_config_to_endpoint` internally), so they exercise the full registry dispatch path, not just the endpoint constructors directly.

### 10. Phase 6 full-migration structure is confirmed by Phase 4a

Phase 4a establishes the two-phase factory pattern (registry dispatch + fallback match) that Phase 6 will simplify. When Phase 6 lands, the fallback match body shrinks to zero arms and the `_ => Ok(None)` arm for the output factory becomes the only arm. The `FileInput`/`ClockInput` entries in the fallback match's `Ok(None)` arm will be removed as dead code at that time (since the registry always handles them). This is consistent with the plan's statement that "the fallback shrinks until empty."

---

## Plan updates to carry forward

1. **Clarify descriptor name = `TransportConfig::name()`**: The plan says "name: matches TransportConfig 'name' tag" but is ambiguous. The correct rule is: the descriptor name equals `config.name()` (the `name()` method return value), not the serde tag name. Document the `ClockInput` example explicitly: descriptor name is `"clock"`, serde tag is `"clock_input"`.

2. **Registry dispatch before secrets-resolution vs after**: The plan's Phase 6 describes resolving `TransportConfig → (name, config_value)` as step 1. In Phase 4a (transitional), the secrets are resolved first, then registry dispatch happens on the already-resolved config. Phase 6 should keep this order (secrets first, then registry) because `resolve_secret_references_via_json` is generic and the factory is the right place for it. The `build_*` function receives an already-resolved `JsonValue` — this should be stated explicitly in the plan.

3. **`transport_config_inner_as_json` should be noted in Phase 6**: Phase 6 needs this helper (or its equivalent) to convert a typed `TransportConfig` variant to the `JsonValue` that `BuildInputFn` / `BuildOutputFn` expects. Document it as part of the Phase 6 implementation.

4. **Phase 4a exercises only input-only and output-only descriptors**: The next connector migrated in Phase 4b with a `Direction::InputOutput` descriptor (none exist for file/clock) will need careful attention to ensure the registry dispatch correctly handles the case where a descriptor allows both directions but is used as input only or output only.
