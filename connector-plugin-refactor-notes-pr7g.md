# PR 7g Notes — final cleanup (transport.rs match shells, re-exports, doc(hidden))

## Unexpected findings

### 1. Benches use the same constructors as integration tests — and they were missed in PR 7f

PR 7f changed `PostgresOutputEndpoint::new` and `DeltaTableWriter::new` to take
`Arc<dyn OutputControllerRef>` (downgrading internally). All four `Weak::new()`
call-sites in `#[cfg(test)] mod tests` were updated, plus the matching nested
`mod tests::parallel` modules. **But the criterion benches at
`crates/adapters/benches/{postgres_output.rs,delta_encoder.rs}` use the very
same constructors** — and they live outside the test cfg, so the type-checker
didn't flag them until `cargo build --benches`. PR 7f's verification was
`cargo check --all-features`, which doesn't compile bench targets.

The fix mirrors the test-side fix: introduce a `NoOpControllerRef` stub. To
avoid duplicating it across two benches, it lives in
`benches/bench_common.rs` (with `#[allow(dead_code)]` because the `avro_encoder`
bench also `mod`-includes `bench_common` but doesn't reference the stub, which
otherwise produces a `dead_code` warning).

**Lesson for future PRs that change a public connector ctor signature**: run
`cargo build --all-features --benches` (or `cargo check --tests --benches`)
not just `cargo check`.

### 2. `pub use crate::integrated::postgres::PostgresOutputEndpoint` is still load-bearing — for benches

The PR 7f notes flagged this re-export as a candidate for removal once the
fallback `match` was gone. **It cannot be removed** because
`benches/postgres_output.rs` imports
`dbsp_adapters::integrated::PostgresOutputEndpoint`. If that bench is ever
deleted or migrated to construct via the registry's
`build_integrated_output_fn` (which would require synthesising config JSON and
casting the resulting `Box<dyn IntegratedOutputEndpoint>` back to a concrete
type — not trivial), the re-export can go.

For now, the audit conclusion is: **keep the re-export**.

### 3. `IntegratedOutputEndpoint` was already imported `pub use` from `feldera_adapterlib::transport` in `integrated.rs`

The plan for commit 2 said to "switch `adapters::lib.rs` to import
`IntegratedOutputEndpoint` directly from `feldera_adapterlib::transport`."  In
practice `integrated.rs` already had
`pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;` (added in
PR 7f).  So `lib.rs`'s
`pub use integrated::{IntegratedOutputEndpoint, create_integrated_output_endpoint};`
was already going through a one-hop re-export.

The fix:
1. `lib.rs` now does `pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;`
   directly (skipping the integrated module).
2. `integrated.rs` downgrades its `pub use` to a private `use` — the trait is
   only needed in scope inside that module's function signature; nothing
   external imports it via `crate::integrated::IntegratedOutputEndpoint`
   (verified with grep).

### 4. `OutputConsumer` is also `#[doc(hidden)]` — but no plan-listed type for it referenced "phase 4b"

The plan listed four types to un-hide:
`IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle`,
`OutputConsumer`.  Three of them had no comment explaining the marker.  Only
`IntegratedOutputEndpoint` had an inline comment saying
"`#[doc(hidden)]` is a temporary marker — it will be removed in Phase 4b…".
That comment was removed alongside the attribute.

`InputCollectionHandle::new()` (the constructor) also had its own
`#[doc(hidden)]` annotation — it was un-hidden too, since hiding the
constructor of a now-public struct would be inconsistent.

Each newly-public type was given a real doc comment (one short sentence
summarising its role), since `#[doc(hidden)]` was the only thing previously
suppressing the missing-doc lint for these items.

### 5. `lib.rs` ABI overview text needed three string edits, not just attribute removal

`crates/adapterlib/src/lib.rs` documents the plugin ABI in its module-level
doc comment.  Four lines mentioned `(`#[doc(hidden)]`)` annotations that needed
removing:
- Line 10: the introductory paragraph explaining "Types marked `#[doc(hidden)]`
  are still part of the contract"
- Lines 22, 24, 31: the per-type bullet items in the "Supported plugin-facing
  types" lists

The introductory paragraph was rewritten (the `#[doc(hidden)]` carve-out is
no longer relevant), and the inline `(\`#[doc(hidden)]\`)` annotations on the
three bullets were dropped.

### 6. The `match config { _ => Ok(None) }` shells in `transport.rs` were genuinely dead — but the `#[allow(unused_variables)]` was tied to them

`output_transport_config_to_endpoint`'s `#[allow(unused_variables)]` was
suppressing a warning on `fault_tolerant`/`secrets_dir` that arose only in the
fully-empty fallback `match` after all arms migrated. With the match shell
collapsed to a direct `Ok(None)`, both arguments are still used in the
registry-dispatch path above the `Ok(None)`, so no warning fires.  The
`#[allow]` and the match shell were therefore co-dependent dead weight — and
both got removed in the same commit.

The `transport_config_inner_as_json` helper, `TransportConfig` import, and
`secrets_dir` arg are all still used by the registry path, so no further
import pruning is needed.

### 7. `PR 7h` candidate: bench-side construction of integrated connectors via the registry

The benches still construct `PostgresOutputEndpoint` / `DeltaTableWriter`
directly (not via `connector_by_name(...).build_integrated_output`). That's
fine — but it locks in the public `pub use crate::integrated::postgres::
PostgresOutputEndpoint` and `pub use crate::integrated::delta_table::
DeltaTableWriter` re-exports as load-bearing surface area.  If we ever want to
hide those concrete types entirely, the benches would have to:

1. Synthesise a `TransportConfig` JSON value matching `PostgresWriterConfig`
   / `DeltaTableWriterConfig`.
2. Look up the descriptor by name (`postgres_output`, `delta_table_output`).
3. Call `descriptor.build_integrated_output.unwrap()(...)` with an
   `Arc<NoOpControllerRef>`.
4. The result is `Box<dyn IntegratedOutputEndpoint>`, which implements both
   `OutputEndpoint` and `Encoder`, so the existing
   `endpoint.consumer().batch_start(...)` / `endpoint.encode(...)` /
   `endpoint.consumer().batch_end()` calls would still work via the trait
   bounds — no concrete-type access needed.

This is a clean follow-up but not in the PR 7g scope.
