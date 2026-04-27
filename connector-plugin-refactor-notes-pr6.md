# Connector Plugin Refactor — PR 6 Notes

## What was done

Phase 6: Added registry dispatch to the two integrated factory functions (`create_integrated_output_endpoint` and `create_integrated_input_endpoint` in `crates/adapters/src/integrated.rs`), mirroring the two-phase pattern already in place for the transport factories since PR 4. Also implemented `impl OutputControllerRef for ControllerInner` (required to compile the integrated output registry path) and exposed `transport_config_inner_as_json` as `pub(crate)` for use from `integrated.rs`.

### Files changed

| File | Change |
|------|--------|
| `crates/adapters/src/transport.rs` | Changed `transport_config_inner_as_json` from `fn` to `pub(crate) fn` so `integrated.rs` can call it. |
| `crates/adapters/src/controller.rs` | Added `impl feldera_adapterlib::connector::OutputControllerRef for ControllerInner`; four methods delegate to existing `ControllerInner` inherent methods and `self.status.*`. |
| `crates/adapters/src/integrated.rs` | Added registry dispatch block (`connector_by_name` → `build_integrated_input/output`) at the top of each factory function; existing hardcoded match arms remain as the fallback for connectors not yet in the registry. Added imports `Arc`, `connector_by_name`, `OutputControllerRef`, `transport_config_inner_as_json`. |

---

## Insights and factors not accounted for in the plan

### 1. Phase 6 transport factories were already done in Phase 4

The plan describes Phase 6 as "Rewrite the four factory functions" to introduce the two-phase pattern. However, Phase 4a already introduced this pattern for the two transport factories (`input_transport_config_to_endpoint` and `output_transport_config_to_endpoint`). The registry dispatch was added to those factories as part of the migration of `file_input`, `file_output`, and `clock`. The `transport_config_inner_as_json` helper was also introduced then.

**Plan update**: Phase 6 is effectively "complete the two-phase pattern for the integrated factories". The transport factory portion was already done in Phase 4a. Document this clearly: Phase 4a does 2 of the 4 factories; Phase 6 does the other 2.

### 2. `impl OutputControllerRef for ControllerInner` needed in Phase 6, not Phase 4b

The plan says "Land the impl block with the first integrated-output connector migrated (PR 7f, Postgres writer / Delta)". However, the integrated output registry dispatch in `integrated.rs` calls `build_integrated_output` with an `Arc<dyn OutputControllerRef>`. Even as dead code, this coercion `Arc<ControllerInner>` → `Arc<dyn OutputControllerRef>` requires `ControllerInner: OutputControllerRef` at compile time.

Therefore, `impl OutputControllerRef for ControllerInner` must be added in Phase 6, not Phase 4b, because Phase 6 writes the code that requires it.

**Plan update**: Move `impl OutputControllerRef for ControllerInner` from "first integrated-output connector migrated (PR 7f)" to Phase 6. The impl is zero-cost (all four methods delegate to existing ControllerInner or ControllerStatus methods with no body changes); it's purely an upcast enabler for the `Weak<ControllerInner>` → `Arc<dyn OutputControllerRef>` path.

### 3. `impl OutputControllerRef for ControllerInner` method delegation is clean

`EndpointId = u64` (a type alias, not a newtype), so the `OutputControllerRef` trait's `u64` parameter types are directly compatible with `ControllerInner`'s existing methods. All four methods delegate cleanly:
- `output_transport_error` → `ControllerInner::output_transport_error(self, ...)` (UFCS to avoid shadowing the trait method)
- `update_output_connector_health` → `ControllerInner::update_output_connector_health(self, ...)` (UFCS for same reason)
- `register_batch_progress_counter` → `self.status.register_batch_progress_counter(...)`
- `output_buffer` → `self.status.output_buffer(...)`

The two `self.*` delegations use UFCS (`ControllerInner::foo(self, ...)`) rather than `self.foo(...)` to avoid the trait method shadowing the inherent method during name resolution inside the `impl OutputControllerRef for ControllerInner` block.

### 4. `transport_config_inner_as_json` was private; needed pub(crate)

The helper was originally `fn transport_config_inner_as_json` (private to `transport.rs`). Phase 6 needs it in `integrated.rs` (a sibling module in the same crate). Changing to `pub(crate)` is the right scope — not `pub` (don't expose it outside the crate) but not private either.

**Lesson**: When a helper function introduced in Phase 4 is reused in Phase 6, check its visibility. The plan didn't call this out explicitly.

### 5. Integrated output factory error for "controller dropped"

When the registry dispatch calls `controller.upgrade()`, the upgrade can fail if the controller has been dropped. The plan's `build_integrated_output` signature passes `Arc<dyn OutputControllerRef>` (not Weak), with the intent that connectors call `Arc::downgrade()` for storage. At dispatch time, the factory upgrades `Weak<ControllerInner>` to `Arc<ControllerInner>` and coerces to `Arc<dyn OutputControllerRef>`.

If `upgrade()` returns `None` at factory time, this is a programming error (factory is called after teardown). The error is mapped to `ControllerError::invalid_transport_configuration` with a descriptive message. This is semantically imperfect (it's not really a config error), but it's dead code for now (no integrated connectors are in the registry in Phase 6). When Phase 4b migrates integrated connectors, their `new()` functions will receive the arc directly, so this error path is never exercised in practice.

**Plan update**: If a more precise error variant is desired for "controller dropped at construction time", it could be added as a future improvement. For now, `invalid_transport_configuration` is an acceptable placeholder in dead code.

### 6. Format check is duplicated across registry and fallback paths

The format check (`if connector_config.format.is_some()`) was originally a single post-match check. In the refactored version, there are now two copies:
1. Inside the registry dispatch block (before calling `build_fn`)
2. After the fallback match (original location)

This is correct: the registry path returns early, so the post-match check would never fire for registry-dispatched connectors. The duplication is unavoidable — it's the same pattern used in the transport factory (which inlines the format check within each match arm).

**Alternative considered**: Move the format check to before the match (at the top of the function). This would be a single check but would remove the option for future integrated connectors to accept a format field if they choose. The current approach (per-path check) is more flexible.

### 7. `consumer: Box<dyn InputConsumer>` borrow/move analysis

`create_integrated_input_endpoint` passes `consumer` by value. In the registry dispatch, `consumer` is moved into `build_fn(...)`. If the registry block is NOT entered (descriptor not found or `build_integrated_input` is `None`), `consumer` falls through to the fallback match unemoved. Rust's borrow checker correctly handles this because:
- `build_fn(...)` is called only inside `if let Some(build_fn)` (when Some)
- The entire `if let Some(descriptor) = connector_by_name(...)` block is not entered if descriptor is None
- The fallback match uses `consumer` only in exactly one arm

This is the same pattern as the existing fallback match (which has multiple arms each using `consumer` — Rust allows this because only one arm executes at runtime).

### 8. Registry dispatch is dead code in Phase 6 but compiles correctly

No integrated connectors (`postgres`, `delta_table`, `iceberg`) have registered descriptors in Phase 6. `connector_by_name` will always return `None` for the transport names they use (e.g., `"postgres_input"`, `"delta_table_output"`). Both registry dispatch blocks are therefore dead code in Phase 6. They compile cleanly because:
- All types resolve correctly (`OutputControllerRef`, `transport_config_inner_as_json`, `Arc<dyn OutputControllerRef>`)
- The dead code paths don't trigger Rust's "unreachable code" lint (they're not statically unreachable — `connector_by_name` COULD return Some if a connector registers itself)

This is intentional design: the two-phase pattern is in place and ready for Phase 4b to activate by adding descriptors.

### 9. `impl OutputControllerRef for ControllerInner` method name collision via UFCS

Both `OutputControllerRef` and `ControllerInner` define `output_transport_error` and `update_output_connector_health`. Inside `impl OutputControllerRef for ControllerInner`, writing `self.output_transport_error(...)` would be ambiguous (trait method vs inherent method). Rust resolves it to the inherent method in most cases, but using UFCS (`ControllerInner::output_transport_error(self, ...)`) is explicit and avoids any ambiguity. This is the correct pattern when implementing a trait that shadows existing inherent method names.

---

## Plan updates to carry forward

1. **Phase 6 covers 2 of 4 factories, not all 4**: Transport factories (2) were done in Phase 4a; integrated factories (2) are done in Phase 6. Document as "Phase 6 completes the two-phase factory pattern for the integrated factories."

2. **`impl OutputControllerRef for ControllerInner` belongs in Phase 6**: Not Phase 4b / PR 7f as previously noted. The integrated output registry dispatch code (even as dead code) requires the impl at compile time. Zero additional work — the methods all delegate to existing ControllerInner or ControllerStatus methods.

3. **Format check duplication is by design**: Two copies of the format check (registry path + fallback path) is the correct pattern; a single pre-dispatch check would be wrong because the registry path returns early. Document this pattern for future integrated connectors.

4. **Phase 4b integrated connectors**: `postgres`, `delta_table`, `iceberg` remain in the fallback match. After Phase 6, the `if let Some(descriptor) = connector_by_name(&name)` block in both integrated factories is live infrastructure waiting for Phase 4b to populate the registry. Phase 4b must add descriptors with `build_integrated_input` / `build_integrated_output` and then remove the corresponding fallback match arms.

5. **`transport_config_inner_as_json` is now pub(crate)**: Its original private scope was fine for transport.rs alone. Promoting to pub(crate) for use by integrated.rs is the right call. If more modules in the adapters crate need it, no further change is needed.
