# PR 7f Notes — integrated connectors (postgres, delta_table, iceberg)

## Unexpected findings

### 1. Constructors must change signature, not just the descriptor build functions

The plan said "each connector's `new()` receives `Arc<dyn OutputControllerRef>` and immediately downgrades to `Weak` for storage." This turned out to be required, not optional: the descriptor's `BuildIntegratedOutputFn` signature provides `Arc<dyn OutputControllerRef>`, so any build function that delegates to an existing constructor must either:
- (a) Change the constructor to accept `Arc<dyn OutputControllerRef>` and downgrade internally, or
- (b) Wrap the build function to downcast `Arc<dyn OutputControllerRef>` back to `Arc<ControllerInner>` — impossible without `Any`.

Option (a) is the only viable path. Both `DeltaTableWriter::new()` and `PostgresOutputEndpoint::new()` were updated to take `Arc<dyn OutputControllerRef>` and call `Arc::downgrade(&controller)` internally. All inner struct fields (`DeltaTableWriterInner.controller`, `PostgresWorker.controller`, `PostgresOutputEndpoint.controller`) changed from `Weak<ControllerInner>` to `Weak<dyn OutputControllerRef>`.

### 2. `.status.foo()` method calls must be replaced with trait methods

Both `DeltaTableWriter` and `PostgresOutputEndpoint` accessed the controller via:
```rust
controller.status.register_batch_progress_counter(...)
controller.status.output_buffer(...)
```

After the field type changed to `Weak<dyn OutputControllerRef>`, these direct field accesses are no longer possible. The `OutputControllerRef` trait provides these methods directly (`register_batch_progress_counter`, `output_buffer`), and `ControllerInner`'s `impl OutputControllerRef` delegates to `self.status.foo(...)` internally. So the call sites become:
```rust
controller.register_batch_progress_counter(...)
controller.output_buffer(...)
```

This is a clean improvement: output connectors no longer depend on `ControllerInner`'s internal `status` field layout. `ControllerInner` is no longer imported in `delta_table/output.rs` or `postgres/output.rs`.

### 3. Tests used `Weak::new()` (dangling weak) as a null controller — must be replaced with a stub

Both `delta_table/output.rs` and `postgres/output.rs` test modules used `Weak::<ControllerInner>::new()` (a dangling weak with no backing allocation) to satisfy the `controller` parameter. This worked because `Weak::upgrade()` on a dangling weak always returns `None`, so all controller callbacks were silently dropped.

After the signature change to `Arc<dyn OutputControllerRef>`, a real `Arc` value is required. A `NoOpControllerRef` stub struct was added to each test module implementing all four `OutputControllerRef` methods as no-ops. All `Weak::new()` calls in tests were replaced with `Arc::new(NoOpControllerRef)`.

### 4. Postgres output has a nested `parallel` test submodule that doesn't inherit parent module items

`postgres/output.rs` has `#[cfg(test)] mod tests { ... mod parallel { ... } }`. The `NoOpControllerRef` stub was defined in `tests`, but `tests::parallel` cannot see it without an explicit `use super::NoOpControllerRef;`. In contrast, `delta_table/output.rs` has only one flat test module (`mod parallel`) so there was no nesting issue. The fix was to add `use super::NoOpControllerRef;` at the top of `tests::parallel`.

### 5. The fallback match in `integrated.rs` now collapses entirely — all arms were constructing

The input fallback had 4 constructing arms (`DeltaTableInput`, `IcebergInput`, `PostgresInput`, `PostgresCdcInput`) plus an error catch-all. The output fallback had 2 constructing arms (`DeltaTableOutput`, `PostgresOutput`) plus an error catch-all. After migrating all 6 connectors to the registry:
- Both fallback `match` blocks had type `!` (only the `return Err(...)` arm remained)
- The `let ep: Box<dyn ...> = match { ... }; ... Ok(ep)` structure became unreachable code
- Rather than use `#[allow(unreachable_code)]`, both fallbacks were collapsed to a direct `Err(ControllerError::unknown_*_transport(...))` return — same outcome PR 7g would have applied

The trailing `if connector_config.format.is_some()` checks after each match (which guard against format specifications on integrated connectors) were also removed since they only executed for connectors matched in the specific arms (now gone). The registry path already performs this check before calling any build function.

### 6. The iceberg crate needed no changes — descriptor shim lives in `adapters`

The plan's note that each connector descriptor lives "in the crate that owns the implementation" doesn't apply to iceberg: `feldera-iceberg` is a separate crate without `inventory` as a dependency. Adding `inventory` to `feldera-iceberg` would be an unnecessary coupling. Instead, the iceberg descriptor lives in a new `crates/adapters/src/integrated/iceberg.rs` shim module (gated by `#[cfg(feature = "with-iceberg")]`). This shim only calls `feldera_iceberg::IcebergInputEndpoint::new(...)` — all logic stays in the iceberg crate.

### 7. `postgres_cdc_input` descriptor required a nested feature-gated submodule

`postgres_cdc_input` is gated by `with-postgres-cdc`. But `postgres.rs` already has top-level `inventory::submit!` calls for the always-enabled connectors. Attempting to put the CDC descriptor at the top level with a `#[cfg]` attribute on just the `inventory::submit!` would leave the `fn build_postgres_cdc_input` and `static POSTGRES_CDC_INPUT_DESCRIPTOR` reachable in all configurations (causing type errors since `PostgresCdcReaderConfig` comes from `postgres_cdc` module which is feature-gated). The solution was a single `#[cfg(feature = "with-postgres-cdc")] mod postgres_cdc_descriptor { ... }` containing the config type import, build function, static descriptor, and `inventory::submit!` together. This keeps all CDC-specific items behind the feature gate.

### 8. PR 7g scope further reduced — `#[allow(unused_variables)]` already removed in PR 7e

The `#[allow(unused_variables)]` on `input_transport_config_to_endpoint` was identified in PR 7e notes as dead. It was not removed then. PR 7f's integrated changes are in `integrated.rs`, not `transport.rs`, so the attribute in `transport.rs` remains. PR 7g should still remove it.

Additionally, PR 7g's "re-export-chain cleanup" now applies to `integrated.rs`: with the fallback gone, `pub use crate::integrated::postgres::PostgresOutputEndpoint` may be the only remaining reason to import the postgres module. PR 7g should audit whether this re-export is still needed.
