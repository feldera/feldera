# PR 7h Notes — bench migration to registry dispatch + concrete-type exposure removal

## Unexpected findings

### 1. The descriptor's `build_*` consumes the inner config JSON directly — not the `TransportConfig` envelope

The plan said: "Synthesise a `TransportConfig` JSON value matching `PostgresWriterConfig` /
`DeltaTableWriterConfig`." This is misleading.

`build_postgres_output` and `build_delta_table_output` deserialise via
`serde_json::from_value::<PostgresWriterConfig>(config.clone())` — so the
`config: &JsonValue` parameter holds *just the inner config*, not the
`{"name": "...", "config": {...}}` envelope. The factory entry
(`create_integrated_output_endpoint`) uses `transport_config_inner_as_json` to
strip the envelope before calling `build_fn`.

For benches that already hold a raw `PostgresWriterConfig` /
`DeltaTableWriterConfig`, the right call is
`serde_json::to_value(&config).unwrap()` — *not* wrapping into a
`TransportConfig::PostgresOutput(config)` first. This is also simpler: benches
don't need to import or pattern-match `TransportConfig`.

`transport_config_inner_as_json` itself is `pub(crate)` in `crates/adapters/src/transport.rs`, so external callers (e.g. benches) can't use it anyway — they must serialise the inner config directly.

### 2. Supertrait imports are not needed for trait-object dispatch — and produce `unused_imports` warnings if kept

The plan said `endpoint.consumer().batch_start(...)` and `endpoint.encode(...)`
"continue to work via trait bounds" once the bench holds
`Box<dyn IntegratedOutputEndpoint>`. True, but the implication that bench
imports stay roughly the same is wrong.

`Encoder::consumer` and `Encoder::encode` are reachable through
`dyn IntegratedOutputEndpoint`'s vtable *without* `Encoder` being in scope
(supertrait methods on a trait object don't need the supertrait imported).
The `use dbsp_adapters::Encoder` (or `feldera_adapterlib::format::Encoder`)
that the original bench had is therefore not just unnecessary — it's an
*unused import* that fails the lint. PR 7h removed both.

Plan should explicitly say: "After migration, drop the
`Encoder`/`OutputEndpoint` imports from the bench file — they're not in scope
references; trait-object dispatch covers supertrait methods natively."

### 3. `&mut Box<dyn Trait>` does not auto-coerce to `&mut dyn Trait` in function arguments

Bench helper signatures changed from `&mut PostgresOutputEndpoint` (concrete)
to `&mut dyn IntegratedOutputEndpoint` (trait object). The call site

```rust
let mut endpoint = create_endpoint(&config);  // Box<dyn IntegratedOutputEndpoint>
bench_encode_iter(&mut endpoint, ...);        // expects &mut dyn IntegratedOutputEndpoint
```

does not compile — `&mut Box<dyn Trait>` is not implicitly the same as
`&mut dyn Trait`. The fix is a manual reborrow at the call site:
`bench_encode_iter(&mut *endpoint, ...)`.

(Method-call auto-deref does work — `endpoint.encode(...)` resolves through
the `Box`. The coercion gap is only for explicit function arguments.)

This is a small mechanical detail but easy to miss when reviewing the diff.
Plan should note the reborrow.

### 4. The `pub mod delta_table` exposure was load-bearing *only* for the bench

Inside `crates/adapters/src/integrated/`, the `delta_table` submodule's
helpers (`register_storage_handlers`, `delta_input_serde_config`) are reached
by sibling files (`delta_table/input.rs`, `delta_table/output.rs`,
`delta_table/test.rs`) via `crate::integrated::delta_table::*` paths. Those
paths work fine whether the module is `pub mod` or `mod` — `crate::` resolves
through private modules within the same crate.

So the `pub` qualifier on `mod delta_table` had exactly one consumer: the
bench's `use dbsp_adapters::integrated::delta_table::DeltaTableWriter`. After
bench migration, downgrading to `mod delta_table` is a no-op for everything
except the deleted bench import. **Audit before removal**: grep for
`crate::integrated::delta_table::` from outside the `delta_table` subtree to
confirm no other internal caller reaches in transitively. (None did, in this
repo.)

The plan's "drop `pub` from `mod delta_table`" line should explicitly note
the audit step — for delta_table the `pub` was bench-only, but in general a
`pub mod` could be hiding multiple consumers.

### 5. `pub use crate::integrated::postgres::PostgresOutputEndpoint` had only the postgres bench as consumer

Mirror of #4 for postgres: the `pub use` re-export was reachable only by
`benches/postgres_output.rs`. PR 7g's audit comment ("keep — load-bearing for
benches") was correct, and PR 7h's removal is symmetric. Same audit pattern
applies: `rg 'integrated::PostgresOutputEndpoint'` outside the
`crates/adapters/src/integrated/postgres` subtree returned only the bench
file before migration, and zero hits after.

### 6. `pub` qualifiers on internal helpers (`register_storage_handlers`, `delta_input_serde_config`) become orphaned `pub`

After dropping `pub` from `mod delta_table`, the public-ness of
`register_storage_handlers` and `delta_input_serde_config` (defined at the
`delta_table.rs` module root) becomes vacuous — they're now reachable only
from inside the now-private module's subtree, where the `pub` is unnecessary.
Not strictly broken, but it's dead `pub` qualifiers.

PR 7h did not tighten these (out of scope for the bench migration). A future
cleanup PR could downgrade them to `pub(super)` or remove `pub` entirely.
Worth a one-line note in the plan that this pattern (helpers exported from
the module root for sibling-file use) leaves orphaned `pub`s after the
parent module visibility is tightened.

### 7. The bench's remaining `dbsp_adapters` import surface is one type (`SerBatch`)

After migration, `benches/postgres_output.rs` imports from `dbsp_adapters`
*only* `SerBatch`, while everything else routes through `feldera_adapterlib`
directly (`connector_by_name`, `IntegratedOutputEndpoint`).

`SerBatch` itself is defined in `feldera_adapterlib::catalog` — it's only
re-exported by `dbsp_adapters`. The bench could use
`feldera_adapterlib::catalog::SerBatch` for consistency with the other
`feldera_adapterlib::*` imports. PR 7h kept the `dbsp_adapters::SerBatch`
path (the existing convention in the bench file) because the
`feldera_adapterlib::catalog` re-export status wasn't verified during the
migration.

Minor stylistic point — the plan could either say "leave `SerBatch` import
alone, even though it's re-routable through `feldera_adapterlib::catalog`"
or "route everything through `feldera_adapterlib` for consistency." Either
is fine; just pick one.

### 8. PR 7g already lifted `pub use IntegratedOutputEndpoint` to a private `use` in `integrated.rs`

Before PR 7h, the plan referenced `pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;`
in `integrated.rs` as the trait-name-in-scope source. PR 7g, commit 2 already
downgraded this to a private `use` — so by the time PR 7h runs, the file is
already correct on this front and PR 7h doesn't touch the import.

Plan note: the chain of state changes across 7f → 7g → 7h means each PR's
working assumptions need to be checked against the *current* state of
`integrated.rs`, not the PR 7f baseline. Specifically: PR 7h's surface
removal is a two-line edit (`pub use ... PostgresOutputEndpoint;` deletion
and `pub mod` → `mod` change), nothing more. Anything beyond that is dead
text from earlier drafts.
