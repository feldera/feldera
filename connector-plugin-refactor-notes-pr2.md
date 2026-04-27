# PR 2 — Implementation Notes

## Deviations from the Plan

### `register_connector!` macro — simplified to expr-form only

**Plan said**: ergonomic named-fields macro with closure syntax:
```rust
register_connector! {
    name: "my_kafka_clone",
    build_input: |cfg, ctx| Ok(Box::new(MyEndpoint::new(cfg)?)),
}
```

**What was implemented**: bare `inventory::submit!` wrapper:
```rust
register_connector!(&MY_DESCRIPTOR);
```

**Why**: `inventory::submit!` generates a `#[used] static`, so the expression inside must be const-evaluable. Named functions (`fn foo(...)`) are const-evaluable as function pointers; non-capturing closures coerced to `fn` pointers are NOT reliable in `static` initializers in Rust 1.93. The named-fields form with closures from the plan would fail to compile when the user writes `build_input: |cfg, name, path| { ... }` because the closure-to-fn-pointer coercion is not const.

**Action for plan update**: The ergonomic named-fields macro must either:
1. Accept only named function items (not closures): `build_input: my_build_fn`
2. Use a different registration mechanism that doesn't require const evaluation (e.g., `LazyLock` with `inventory::submit!` inside a `#[ctor]`-style initializer, but that adds a dependency)
3. Or accept the limitation and document it in the macro

Option 1 is the most practical. The plan's examples should show `build_input: my_build_fn` (named function), not `build_input: |cfg, ctx| ...` (closure). Closures are fine in `LazyLock` contexts, but `inventory`-based registration requires statics.

---

### `BuildIntegratedOutputFn` controller parameter — `Arc` not `Weak`

**Plan said**: "plus the contextual params each factory currently passes … controller weak ref"

**What was implemented**: `controller: Arc<dyn OutputControllerRef>` (strong reference)

**Why**: `Weak<dyn Trait>` is valid Rust (via `CoerceUnsized`), but converting `Weak<ControllerInner>` to `Weak<dyn OutputControllerRef>` at the call site requires an explicit coercion:
```rust
// Requires: let arc = controller.upgrade().unwrap(); then downgrade back
let dyn_weak: Weak<dyn OutputControllerRef> = arc.downgrade(); // extra alloc round-trip
```
Passing `Arc<dyn OutputControllerRef>` at build time is cleaner — the connector implementation stores `Arc::downgrade()` internally to avoid cycles, exactly as it does today with `Weak<ControllerInner>`. The build function boundary passes ownership of the `Arc` rather than a `Weak`.

**Consequence for Phase 4b**: When migrating integrated output connectors onto the registry, each connector's `new(...)` function will receive `Arc<dyn OutputControllerRef>` and should immediately downgrade to `Weak<dyn OutputControllerRef>` for storage (matching today's `Weak<ControllerInner>` pattern).

**Finalized trait shape** (verified: `impl OutputControllerRef for ControllerInner` compiles with zero body changes to `ControllerInner`):

| Method | Callers | Notes |
|--------|---------|-------|
| `output_transport_error(&self, endpoint_id: u64, endpoint_name: &str, fatal: bool, error: AnyError, tag: Option<&str>)` | Postgres (4×), Delta (1×) | Direct method on `ControllerInner` |
| `update_output_connector_health(&self, endpoint_id: u64, health: ConnectorHealth)` | Delta (3×) | Direct method on `ControllerInner` |
| `register_batch_progress_counter(&self, endpoint_id: &u64, counter: Arc<AtomicU64>)` | Delta (1×, in `new()`) | Delegated to `self.status.*`; note `endpoint_id` is `&u64` (borrowed) |
| `output_buffer(&self, endpoint_id: u64, num_bytes: usize, num_records: usize)` | Postgres (1×), Delta (1×) | Delegated to `self.status.*` |

The trait flattens the `controller.status.*` indirection — connectors migrated in Phase 4b will call `controller.register_batch_progress_counter(...)` and `controller.output_buffer(...)` directly on `Arc<dyn OutputControllerRef>`.

**Phase 4b impl skeleton** (drop this into `controller.rs`; no changes to `ControllerInner` needed):
```rust
impl feldera_adapterlib::connector::OutputControllerRef for ControllerInner {
    fn output_transport_error(&self, endpoint_id: u64, endpoint_name: &str, fatal: bool, error: anyhow::Error, tag: Option<&str>) {
        ControllerInner::output_transport_error(self, endpoint_id, endpoint_name, fatal, error, tag);
    }
    fn update_output_connector_health(&self, endpoint_id: u64, health: feldera_types::adapter_stats::ConnectorHealth) {
        ControllerInner::update_output_connector_health(self, endpoint_id, health);
    }
    fn register_batch_progress_counter(&self, endpoint_id: &u64, counter: std::sync::Arc<std::sync::atomic::AtomicU64>) {
        self.status.register_batch_progress_counter(endpoint_id, counter);
    }
    fn output_buffer(&self, endpoint_id: u64, num_bytes: usize, num_records: usize) {
        self.status.output_buffer(endpoint_id, num_bytes, num_records);
    }
}
```

---

### `unsafe impl Sync + Send` on `ConnectorDescriptor` — not needed

Added `unsafe impl Sync for ConnectorDescriptor` and `unsafe impl Send for ConnectorDescriptor` as a precaution. **These are unnecessary**: `fn(...)` function pointers are always `Send + Sync` in Rust, and all other fields (`&'static str`, plain enums, `Option<fn(...)>`) are also automatically `Send + Sync`. The compiler would derive the bounds automatically.

**Action**: Remove the two `unsafe impl` blocks in a follow-up. Not a correctness problem (extra `unsafe` is harmless here) but misleads readers about why they're there.

---

## Observations Useful for Later Phases

### Phase 4b: `integrated.rs` has a two-level re-export chain

After PR 2: `adapters::integrated` re-exports `IntegratedOutputEndpoint` from `feldera_adapterlib::transport`. `adapters::lib.rs` re-exports it again from `adapters::integrated`. This two-hop chain (`adapterlib → adapters::integrated → adapters::lib`) works but is slightly unusual. When Phase 4b removes `adapters::integrated` as the dispatch home, clean up the re-export path — `adapters::lib.rs` should import directly from `feldera_adapterlib::transport`.

### Phase 4b: `Encoder` and `OutputEndpoint` are no longer imported in `integrated.rs`

When moving `IntegratedOutputEndpoint` out of `integrated.rs`, the `Encoder` and `OutputEndpoint` imports were also removed because neither is referenced directly in the factory functions (they're used only via trait objects). The integrated connectors themselves still import `Encoder` and `OutputEndpoint` from their own modules — no transitive breakage.

### Phase 3 (format registry): same `inventory::submit!` const-eval constraint applies

The plan's Phase 3 replaces `Lazy<BTreeMap>` with `inventory::collect!` for format registries. If format module initializers use closures or complex expressions in `submit!`, the same const-eval limitation applies. `&MyFormatFactory as &dyn InputFormat` (where `MyFormatFactory` is a unit struct) is fine; lambdas are not.

### Phase 6: `connector_by_name` must be cheap — OK

`registered_connectors()` iterates an `inventory::iter` (a `#[link_section]`-based linked list), which is O(n) in the number of registered connectors. For the 17 built-in connectors, this is fast. If the lookup is called on every pipeline step (it won't be — it's called at pipeline startup), cache the result in a `HashMap<&str, &ConnectorDescriptor>`.

### Phase 8: name collision detection is not yet implemented

The plan calls for the describer binary to fail fast on duplicate names (§8.6). Currently `connector_by_name` silently returns the first match. The collision check should live in the describer binary's startup, not in `connector_by_name` (which is intentionally a simple lookup). The adapterlib-level API is correct; the enforcement is Phase 8's job.

### Phase 5: `ConnectorFlags` should use `bitflags`

Currently a plain `u32` newtype. Phase 5 should replace it with `bitflags!` — the `|` operator for combining flags, proper `Display`/`Debug`, and self-documenting macro syntax are worth the dependency. `bitflags` is not in the workspace `Cargo.toml` today, but it is already compiled as a transitive dep, so adding it as a direct dep is zero incremental build cost.

```rust
// Phase 5 target shape:
bitflags::bitflags! {
    pub struct ConnectorFlags: u32 {
        const HTTP_DIRECT               = 0x01;
        const AUTO_RECREATED_ON_RESTART = 0x02;
    }
}
```

The current `ConnectorFlags::EMPTY` and `contains()` method are drop-in compatible with what `bitflags` generates, so no call-site changes outside of the struct definition itself.
