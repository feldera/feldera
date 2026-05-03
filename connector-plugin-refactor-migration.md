Important! Do not refer to `connector-plugin-refactor-plan.md`.

# Connector Plugin Refactor — Migration Plan (Current Impl → New Design)

PRs 1–16 of the original `connector-plugin-refactor-plan.md` have already
landed (commits `5013ff40f` … `c0ca4550d`, plus a WIP commit on top). The
implementation follows the **original** plan: single `inventory` registry
with `BuildInputFn`/`BuildOutputFn`/`BuildIntegratedInputFn`/
`BuildIntegratedOutputFn` on the descriptor, `register_connector!` macro,
`connector_by_name()` registry-walk dispatch, generated `force_link.rs`
files, platform-manifest build with the full data-plane dep tree, etc.

This document plans the migration from that current state to the new
design captured in `connector-plugin-refactor-design.md` (and reflected
in the updated `connector-plugin-refactor-plan.md`). The work is broken
into independently-mergeable steps; the next agent will fill in technical
detail per step.

## Goals (recap from design doc)

1. **Zero data-plane overhead from the plugin machinery.** No
   `connector_by_name` walk on the dispatch path — direct symbol calls.
2. **`connectors.toml` cost proportional only to the plugins listed.**
   Adding one plugin should not relink every other plugin's data-plane
   code into every per-pipeline build.
3. **Registering a plugin compiles only metadata.** The describer /
   platform-manifest path must not drag in `dbsp`, `rdkafka`,
   `datafusion`, `deltalake`, etc. just to learn a connector's name,
   direction, kind, and JSON schema.

The current implementation hits goal 1 weakly (`connector_by_name` is
called once at endpoint open) and misses goals 2 and 3 entirely.

## Scope of the registry migration

The new design migrates **only the connector-descriptor registry** from
`inventory` to `linkme`. linkme-vs-inventory is a side preference (~5%
of the value); the structural win is the metadata/impl split, not the
slice mechanism. The other `inventory` users in the repo split into
two groups:

**Unrelated to the plugin system — leave alone**:

- `StorageBackendFactory` (`crates/storage/src/lib.rs`).
- `CheckpointSynchronizer`
  (`crates/storage/src/checkpoint_synchronizer.rs`).

These predate the plugin work and have nothing to do with connectors.

**Part of the plugin MVP — open question**:

- The format registry from Phase 3 / PR 3 of the original plan —
  `inventory::collect!` slots for `&'static dyn InputFormat` /
  `OutputFormat` in `crates/adapterlib/src/format.rs`, the per-format
  `inventory::submit!` calls in
  `crates/adapters/src/format/{csv,json,parquet,raw,avro}*.rs`, and
  the `StubInputFormat` / `StubOutputFormat` fixtures in
  `adapterlib/src/format.rs`'s `#[cfg(test)] mod tests`. Pre-Phase-3
  this was a `Lazy<BTreeMap>` (no stub fixture needed); Phase 3
  converted it specifically to enable third-party format plugins and
  to prove the inventory pattern before applying it to connectors.
  The new design doesn't require it: format plugins are theoretical
  (no third-party format crate exists in-tree or in the reference
  plugin).

  Three reasonable choices, ranked by effort:

  1. **Status quo (default).** Keep the format registry on
     `inventory` exactly as it is. The `StubInputFormat`/
     `StubOutputFormat` fixtures stay (they validate the registry
     mechanism). Mixing two registry mechanisms in the codebase is
     fine; the format side imposes no cost on the connector
     migration.
  2. **Migrate formats to `linkme` for symmetry.** Mechanical sweep
     analogous to Step 3 for connectors: replace `inventory::collect!`
     with `#[linkme::distributed_slice]`, replace `inventory::submit!`
     with `#[linkme::distributed_slice(...)] pub static`. The stub
     fixtures stay, ported to the new shape. Cost is small but
     non-zero, with no functional benefit. Worth doing only if the
     team wants exactly one registry mechanism in the codebase.
  3. **Revert formats to `Lazy<BTreeMap>`.** If the team decides
     format plugins are not a product goal, undo Phase 3 entirely —
     remove the inventory machinery, the per-format `submit!` calls,
     and the stub fixtures; reintroduce a static map of built-in
     formats. Smaller surface, but closes the door on format
     plugins.

  Pick one before Step 5 lands and document the choice. Default is
  (1); flag the question so the team makes it explicitly rather than
  by accident.

The only `inventory` machinery that **definitely** goes away is the
`ConnectorDescriptor` slot, the `register_connector!` macro, and the
`Build*Fn` types — see Step 5.

## Step 0 — Stabilise WIP

The top commit (`85b337ff0` "[adapters] WIP making the plugin system
work") is in flight. Land or revert before starting the migration so the
baseline is clean. Do not interleave WIP fixes with refactor commits.

## Step 1 — Introduce `feldera-adapterlib-meta` (additive, no migration yet)

Create the new tiny crate alongside the existing `feldera-adapterlib`.
Mirror the existing `ConnectorDescriptor` shape minus the four `Build*Fn`
pointer fields, and add `builder_path_override: Option<BuilderPath>`.

- New crate `crates/adapterlib-meta/`, deps: `serde`, `serde_json`,
  `linkme`. Defines `ConnectorDescriptor`, `BuilderPath`, `Direction`,
  `ConnectorKind`, `ConnectorFlags`, `FtModel`, plus the
  `#[linkme::distributed_slice] CONNECTOR_METADATA_REGISTRY` slot and
  `metadata_registry()` / `descriptor_by_name()` discovery fns.
- Re-export the descriptor types from `feldera-adapterlib` so existing
  imports continue to resolve. The runtime trait surface stays in
  `feldera-adapterlib`.
- `OutputControllerRef` stays in `feldera-adapterlib` (impl-side trait;
  no change).

At this step both registries coexist (the existing `inventory` slot for
`&'static ConnectorDescriptor` and the new `linkme` slot in
`-meta`). The next steps cut over and remove the inventory slot.

**Independently mergeable.** No callers change.

## Step 2 — Add `default = ["impl"]` / `metadata = []` feature shape to `dbsp_adapters`

The existing `dbsp_adapters` builds with everything always on. Introduce
the metadata/impl split:

- `dbsp_adapters/Cargo.toml`: add `default = ["impl"]`, `metadata = []`,
  make every heavy dep `optional = true` and gate them behind `impl`.
  Existing `with-*` features become sub-features that imply `impl`.
- `crates/adapters/src/lib.rs`: gate `pub mod controller`, `pub mod
  format`, `pub mod adhoc`, `pub mod static_compile`, `mod test` (and
  any other purely-impl modules) on `#[cfg(feature = "impl")]`. Leave
  `pub mod transport` and `pub mod integrated` unconditional — their
  submodule declarations must stay visible in the metadata-only build.
- Move impl-only re-exports (`IntegratedOutputEndpoint`, etc.) under
  the gate.
- Apply the same shape to `feldera-datagen` (it ships its own
  descriptor and is built as a build-dep of `feldera-platform-manifest`,
  so it needs the metadata-only mode too).

At the end of this step, `cargo build -p dbsp_adapters
--no-default-features` must compile in seconds — only metadata
descriptors and their cheap deps. The same for `feldera-datagen`.

**CI gate to add now**: `cargo build -p dbsp_adapters
--no-default-features` on every PR. Catches anyone placing impl code
outside the gate from this point forward.

**Independently mergeable.** Default builds behave identically to today.

## Step 3 — Per-connector source: descriptor unconditional, impl in `<name>_impl` mod

Mechanical sweep of every built-in connector module (`transport/file.rs`,
`transport/clock.rs`, `transport/kafka.rs`, …, `integrated/postgres.rs`,
`integrated/delta_table.rs`, `integrated/iceberg.rs` shim, etc.):

For each connector:

- Replace the existing `register_connector!` invocation with a `pub
  static <NAME>_META: ConnectorDescriptor` annotated with
  `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` at module scope.
  The `static` is unconditional (no `#[cfg]`).
- The descriptor is **pure data** — strip the four `build_*: Some(...)`
  fields. They no longer exist on the type.
- Move existing impl content into `#[cfg(feature = "impl")] pub(crate)
  mod <name>_impl { … }`. The build function inside becomes
  `pub fn build_<name>_input(...)` / `build_<name>_output(...)` /
  `build_<name>_integrated_*(...)` so the in-tree match and codegen can
  name it directly.
- For external-crate shims (`feldera-iceberg`), the metadata `static`
  and `<name>_impl` mod live in `crates/adapters/src/integrated/<name>.rs`;
  the impl mod calls into the external crate's constructor.

Add `linkme = { workspace = true }` as a direct dep of every crate that
annotates a `static` with `#[linkme::distributed_slice]` (`dbsp_adapters`,
`feldera-datagen`, the eventual reference plugin). Transitive through
`-meta` is not enough.

**Per-connector commits keep blast radius low.** Each connector's
descriptor + impl-mod move is one independent commit; the in-tree
factory `match` (Step 4) updates one arm at a time.

## Step 4 — Replace `connector_by_name`-walk dispatch with name-keyed `match`

The current factory code in `crates/adapters/src/transport.rs` and
`crates/adapters/src/integrated.rs` looks up a descriptor via
`connector_by_name(...)` and invokes its `build_*` fn pointer. This
becomes a hand-written `match` on `TransportConfig::name()` that calls
each connector's `<name>_impl::build_<name>_*` directly:

```rust
match name.as_str() {
    "kafka_input" => kafka::kafka_impl::build_kafka_input(cfg, ctx),
    "file_input"  => file::file_impl::build_file_input(cfg, ctx),
    // … one arm per built-in
    _ => Err(ControllerError::unknown_input_transport(name)),
}
```

Each arm is gated by the same `with-*` feature that gates the impl mod.
Production pipelines do **not** call this match — they go through
codegen (Step 6). This match serves `mock_input_pipeline`, in-tree
controller tests, and the in-tree controller in general.

Migrate one connector at a time alongside Step 3, or batch the Step 4
arms in a follow-up commit; either order works.

## Step 5 — Drop force-link plumbing and `IMPL_REGISTRY`-style indirection

After Steps 3 and 4, the descriptor no longer carries `Build*Fn`
pointers, and dispatch no longer walks the registry. Several pieces of
scaffolding from PRs 5/10 of the current implementation become dead
code:

- `extern crate feldera_datagen as _;` in `crates/adapters/src/lib.rs`
  (PR 5 added this to keep the rlib alive for `inventory::submit!`).
  linkme references through `CONNECTOR_METADATA_REGISTRY` keep it alive on its
  own — delete the line.
- `generate_force_link_rs(...)` helper and the `include_builtin_datagen`
  boolean. Per-tenant `force_link.rs` codegen in the user describer
  workspace and the per-pipeline globals crate. None of these have a
  job once linkme is the registry mechanism.
- `inventory::collect!(&'static ConnectorDescriptor)` slot in
  `feldera-adapterlib` and the wrapping `register_connector!` macro.
  Once every connector has migrated to `#[linkme::distributed_slice]`,
  delete the slot, the macro, and the `connector_by_name`/
  `registered_connectors` functions that walked it. (The new equivalents
  `descriptor_by_name` / `metadata_registry` live in `-meta`.)
- Inventory-collected `BuildInputFn`/`BuildOutputFn`/
  `BuildIntegratedInputFn`/`BuildIntegratedOutputFn` types.

**Also delete the now-obsolete registry tests and fixtures** that
target the inventory mechanism rather than per-connector behaviour:

- `STUB_INPUT_DESCRIPTOR` / `STUB_OUTPUT_DESCRIPTOR` and the
  surrounding `#[cfg(test)] mod tests` block in
  `crates/adapterlib/src/connector.rs` — these test
  `registered_connectors()`/`connector_by_name()` round-trip on a stub
  descriptor; replace with equivalent thin coverage of
  `metadata_registry()` / `descriptor_by_name()` from `-meta` (Step 1
  already adds those). One small unit-test module in `-meta` is
  enough; per-connector crates do not need to re-test the registry
  mechanism.
- Per-connector `mod registry_test { … }` / `mod registry_tests { … }`
  blocks in
  `crates/adapters/src/transport/{nexmark,kafka,redis/output}.rs`,
  `crates/adapters/src/transport/{nats,pubsub}/input.rs`,
  `crates/adapters/src/integrated/{delta_table,iceberg,postgres}.rs`,
  and any others that exist. Most of these assert
  `connector_by_name(...).build_input.is_some()` /
  `.build_output.is_some()` shape — meaningless after the
  `Build*Fn` fields are gone. Delete the blocks; per-connector
  descriptor presence is structurally guaranteed by
  `#[linkme::distributed_slice]` at module scope and covered by the
  end-to-end pipeline tests that already exist for each connector.
  Keep only those assertions that still test something real (e.g.
  `descriptor.kind == ConnectorKind::Transient` for `clock` if a
  controller rewrite depends on the flag).
- `crates/connector-example/WRITING_A_CONNECTOR.md` references to
  `register_connector!`, `inventory = "0.3"`, `registered_connectors()`,
  `connector_by_name()` — rewrite to the linkme + `metadata_registry`
  shape (Step 11 covers the convention; the doc page goes with it).
- `crates/platform-manifest/src/lib.rs` doc comment referencing
  `feldera_adapterlib::connector::registered_connectors` — update to
  `feldera_adapterlib_meta::metadata_registry`.
- The `pub use connector::{connector_by_name, registered_connectors}`
  re-export in `crates/adapterlib/src/lib.rs` and the entire
  `crates/adapterlib/src/connector.rs` module (descriptor types now
  live in `-meta`; `feldera-adapterlib` only needs the `pub use
  feldera_adapterlib_meta::*;` re-export). Audit for any docs in
  `lib.rs` that still describe the old registration story.
- The `controller.rs` / `pipeline_diff.rs` import alias
  `descriptor_by_name as connector_by_name` (kept temporarily during
  migration so call sites don't churn) — once Step 5 lands the
  identifier `connector_by_name` no longer exists and the alias goes
  with it; update call sites to use `descriptor_by_name` directly.

Anything **unrelated to the connector plugin system** stays as-is —
e.g. `StubInputFormat`/`StubOutputFormat` test fixtures in
`adapterlib/src/format.rs` are part of the format registry (still on
`inventory` per the "Out of scope" note above) and are not touched.

Audit for any other "force-link" comments or workarounds; none should
survive this step.

**Mergeable as a single cleanup commit** once Steps 3 and 4 are complete
for every built-in connector.

## Step 6 — Per-pipeline `connector_dispatch.rs` codegen in pipeline-manager

Currently the per-pipeline runtime calls into `dbsp_adapters`'s factory
match. The new design has pipeline-manager codegen a per-pipeline
dispatch file:

- Walk the SQL program's `INPUT/OUTPUT FROM 'name'` clauses.
- For each name, look up the merged manifest entry (platform manifest +
  user manifest) to recover the `BuilderPath` (or default
  `<crate>::build_input` / `build_output` / `build_integrated_*`
  convention).
- Emit `<pipeline>_globals/src/connector_dispatch.rs` containing
  `pub fn build_input(...)` / `pub fn build_output(...)` etc. with one
  `match` arm per name, each calling the named symbol directly.
- The per-pipeline runtime path uses the generated dispatch instead of
  routing through `dbsp_adapters`'s in-tree match.

Touchpoints: `crates/pipeline-manager/src/compiler/rust_compiler.rs`
(extend `prepare_workspace` and the globals-crate `src_content` list),
the SQL compiler's program-info plumbing (it already knows the
`INPUT/OUTPUT FROM` set; thread the manifest through to the codegen
site).

This step also drops any per-pipeline `force_link.rs` injection.

## Step 7 — Per-pipeline external-impl set: only SQL-referenced crates

The current implementation splices the entire `connectors.toml` blob
into every per-pipeline workspace's `[dependencies]`. The new design
lists only the external connector crates the SQL actually references:

- The same SQL walk that drives `connector_dispatch.rs` arms drives
  the per-pipeline `[dependencies]`.
- Bundled connectors (those that live in `dbsp_adapters`) are reached
  transitively through `dbsp_adapters` and **do not** appear in the
  per-pipeline workspace's deps individually — the bundle is one rlib.
- External plugins from `connectors.toml` appear in `[dependencies]`
  only if the SQL references them.

Cache-friendliness: each external rlib's hash is identical across
pipelines (same source, same lockfile, same RUSTFLAGS), so sccache hits
unconditionally. Only the link step varies, and link is never cached.

## Step 8 — Refactor `feldera-platform-manifest` to metadata-only build deps

Today the platform-manifest crate's `build.rs` build-deps include the
full `dbsp_adapters` + `feldera-datagen` data-plane tree (5–10 minute
cold build). After Step 2 it can switch to:

```toml
[build-dependencies]
feldera-adapterlib-meta = { workspace = true }
dbsp_adapters           = { path = "../adapters", default-features = false }
feldera-datagen         = { workspace = true,     default-features = false }
serde_json              = { workspace = true }
```

`build.rs` references `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` (no
`extern crate _;` lines), walks the slice, writes
`platform_manifest.json`. Cold build of the build-dep tree drops to
seconds.

Verify resolver v2 isolates this build-dep configuration from
pipeline-manager's runtime use of `dbsp_adapters` (which keeps default
features on). Both flavours coexist in one `cargo build`.

## Step 9 — User describer: drop `dbsp_adapters` / `feldera-datagen` from its workspace

The user describer currently lists `dbsp_adapters` and `feldera-datagen`
as deps and generates a `force_link.rs`. New shape:

- Generated `Cargo.toml` lists `feldera-adapterlib-meta` + user crates
  only.
- No `force_link.rs` (linkme handles it).
- Reference external plugin crates with `default-features = false` so
  the user describer compiles the metadata side only — assumes plugin
  crates follow the `default = ["impl"]` / `metadata = []` convention
  (Step 11). Plugins that don't follow it still work but pay the
  describer-time data-plane compile.

## Step 10 — `Plugin` variant becomes optional, not load-bearing

`TransportConfig::Plugin` (added by PR 8 of the current implementation)
was the only escape hatch for unknown connector names. With the
metadata registry available everywhere `feldera-types` is, the typed-
variant + `Plugin` split becomes a deserialisation implementation
detail rather than the only available shape. No code change strictly
required at this step; flag the simplification opportunity for a later
PR if/when it's worth the churn.

## Step 11 — Update reference plugin (`crates/connector-example/`) to new convention

The reference plugin from PR 16 currently uses `register_connector!` +
`inventory::submit!`. Migrate it to the new shape:

- `default = ["impl"]` / `metadata = []` features in its `Cargo.toml`.
- `feldera-adapterlib-meta` always; `feldera-adapterlib` optional under
  `impl`.
- `linkme` direct dep.
- `pub static HELLO_LINES_META` annotated with
  `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` at module scope.
- Impl in `#[cfg(feature = "impl")] mod hello_lines_impl { … }` exposing
  `pub fn build_hello_lines_input(...)` (or set `builder_path_override`).
- Verify `cargo build -p connector-example --no-default-features`
  compiles in seconds.

The "Writing a connector" doc page (PR 16) gets updated to require
this convention as part of the public plugin contract.

## Step 12 — Sweep documentation and clean up

- Update `connector-plugin-refactor-plan.md`'s "Status note" banner
  (already done) and any per-PR "Design delta" callouts that reference
  obsolete-now-completed steps.
- Update doc comments inside the affected crates that still mention
  `register_connector!`, `Build*Fn`, `connector_by_name`, force-link.
- Remove dead helper modules and tests left behind by Step 5.
- Double-check `cargo build -p dbsp_adapters --no-default-features` is
  in CI and green.

## Suggested commit order

The steps above are roughly in dependency order. A workable PR breakdown:

1. **Migration PR 1**: Step 0 + Step 1. Add `-meta` crate, no migration yet.
2. **Migration PR 2**: Step 2. `default = ["impl"]` feature on
   `dbsp_adapters` + `feldera-datagen`; CI gate added.
3. **Migration PRs 3a–3n**: Step 3 + Step 4 per connector (one PR per
   connector or small group; each PR migrates the metadata `static`,
   wraps impl in `<name>_impl`, switches the in-tree factory arm to
   call the named symbol directly).
4. **Migration PR 4**: Step 5. Cleanup commit deleting force-link
   plumbing, the inventory slot, `register_connector!`, and
   `Build*Fn` types now that nothing references them.
5. **Migration PR 5**: Step 6. pipeline-manager codegen of
   `connector_dispatch.rs`.
6. **Migration PR 6**: Step 7. SQL-driven per-pipeline external impl set.
7. **Migration PR 7**: Step 8. Platform-manifest build-deps
   `default-features = false`.
8. **Migration PR 8**: Step 9. User describer workspace cleanup.
9. **Migration PR 9**: Step 11. Reference plugin convention update.
10. **Migration PR 10**: Step 12. Doc sweep + dead-code cleanup.

Step 10 (`Plugin` variant simplification) is optional and can ship
later or never.

## Risks to watch

- **resolver v2 build-dep isolation** is what allows
  `feldera-platform-manifest` to compile `dbsp_adapters` with
  `default-features = false` while pipeline-manager compiles it with
  defaults in the same `cargo build`. Verify the workspace is on
  resolver = "2" before Step 8 lands.
- **linkme + cfg interaction**: a `pub static` annotated with
  `#[linkme::distributed_slice]` inside a `#[cfg(...)]`-gated module
  registers only when that cfg is active. Make sure `nexmark`'s
  feature-gated module declaration keeps the descriptor correctly
  absent in the metadata-only build (and that the corresponding match
  arm in `transport.rs` is gated identically).
- **`cargo build -p dbsp_adapters --no-default-features` regressions**
  during Step 3's mechanical sweep: any stray `use feldera_adapterlib::*`
  at the top of a connector file (visible in the metadata build) will
  break the no-default-features build. Keep meta-side imports referencing
  `feldera_adapterlib_meta::*`.
- **Existing tests that use `connector_by_name`** in adapterlib /
  bench / connector-example modules: convert them to use
  `descriptor_by_name` from `-meta` (or, where appropriate, drop them
  in favour of direct `<crate>::build_*` imports — the bench case from
  PR 7h).
