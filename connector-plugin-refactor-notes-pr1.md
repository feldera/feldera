# Connector Plugin Refactor — Implementation Notes

Post-implementation observations as PR 1 lands. Items are noted in rough discovery order.

---

## PR 1 — Tighten the plugin ABI surface

### Unexpected: two different import paths for `StagedBuffers`

The plan referenced one leak at `format.rs:13` (`use dbsp::operator::input::StagedBuffers`).
In practice there was a second distinct leak in `catalog.rs:17` (`use dbsp::operator::StagedBuffers`
— the re-exported path, not the internal one). Both now use `crate::StagedBuffers`. The inconsistency
was harmless (both paths resolve to the same type) but confirms the need for a single canonical path.

### `DynData` / `DynVec` / `Factory` in the output-path ABI

The plan flagged the leak points as `IntegratedInputEndpoint::open` (via `InputCollectionHandle`)
and `InputFormat::new_parser`. A third leak category was not called out explicitly: several methods
on the public traits `SerBatchReader` and `SerCursor` (used by `Encoder` implementors) expose
`dbsp::dynamic::{DynData, DynVec, Factory}`:

- `SerBatchReader::keys_factory() -> &'static dyn Factory<DynVec<DynData>>`
- `SerBatchReader::key_factory() -> &'static dyn Factory<DynData>`
- `SerBatchReader::sample_keys(&mut DynVec<DynData>)`
- `SerBatchReader::partition_keys(&mut DynVec<DynData>)`
- `SerCursor::key() -> &DynData`
- `SerCursor::get_key() -> Option<&DynData>`
- `SerCursor::seek_key_exact(&DynData)`
- `SerCursor::seek_key(&DynData)`

These are only needed by advanced output connectors doing key-based batch partitioning (e.g. the
Kafka partitioned output). Most plugin encoders use only `cursor(format)` → `serialize_key()` /
`serialize_val()` and never need to name `DynData` at all.

**Resolution**: Added `feldera_adapterlib::reexports::{DynData, DynVec, Factory}`. Advanced
connectors can import from there; typical connectors ignore it.

### `NodeId` does not leak through `InputCollectionHandle` in practice

The `InputCollectionHandle.node_id: NodeId` field is `pub` and `NodeId` comes from
`dbsp::circuit`. However, surveying all existing integrated input connectors (Postgres, Delta
Lake, Iceberg), none access `node_id`. The `node_id` field is only used by the controller in
`controller.rs` to decide whether to trigger backfill. A third-party integrated connector would
not need to check this — backfill direction is handled externally. No re-export of `NodeId` was
added; it is documented in the audit as "internal to the controller, not part of the connector
contract".

### `IntegratedInputEndpoint` is `#[doc(hidden)]` but IS part of the plugin ABI

The plan lists `IntegratedInputEndpoint` in the supported plugin ABI surface. It is currently
marked `#[doc(hidden)]`. The PR 1 module docs document it as a supported but doc-hidden trait.
Removing `#[doc(hidden)]` is deferred because `InputCollectionHandle` (passed to `open`) is
also doc-hidden, and making one visible without the other would be confusing. Both should be
surfaced together in a later phase (likely Phase 4b when integrated connectors migrate onto
the descriptor registry).

### `OutputConsumer` is `#[doc(hidden)]` but is passed to `Encoder` implementors

`OutputConsumer` is passed to `Encoder::consumer()` and the plan lists it as part of the plugin
ABI. It is marked `#[doc(hidden)]` but documented in the module docs as a supported type. Same
treatment as `IntegratedInputEndpoint`.

### `StagedInputBuffer` is a useful concrete type not mentioned in the plan

`StagedInputBuffer` (in `format.rs`) wraps `Box<dyn StagedBuffers>` to implement `InputBuffer`.
Plugin-implemented parsers that want to use staged flushing need this concrete type, but it is not
listed in the plan's plugin ABI surface. Added it to the module docs under "Input connectors".

### `ClonableTrait` is imported in `catalog.rs` but not in any public API signature

`use dbsp::dynamic::{ClonableTrait, ...}` in `catalog.rs` — but `ClonableTrait` only appears in
`clone_to()` call sites inside `SplitCursorBuilder` (a `#[doc(hidden)]` struct). No plugin needs
to name `ClonableTrait`. Not re-exported.

### `cargo-semver-checks` requires a published baseline

`cargo-semver-checks check-release` downloads the previously published crate version from
crates.io and compares against the local source. This works because `feldera-adapterlib` is
already published (`publish = true`). Currently at `0.290.0` on crates.io, workspace is at
`0.291.0`. **If `feldera-adapterlib` is ever temporarily unpublished or renamed, the CI job
will fail with a missing-baseline error**, not a semver violation. Add a note to the deployment
guide that the crate must remain published on crates.io for the CI check to function.

### `obi1kenobi/cargo-semver-checks-action` installs `cargo-semver-checks` automatically

The action downloads and caches the tool binary. No need to install it in the `feldera-dev`
container or add it to workspace dev-dependencies. The CI job uses `ubuntu-latest-amd64` (no
`feldera-dev` container needed) because it only needs `cargo` and network access to crates.io.

### The `ci.yml` pattern: every substantive job needs a cancel sentinel

Per the comment at the top of `ci.yml`, every new job that can fail must have a matching
`cancel-if-<name>-failed` sentinel. The `invoke-check-semver` / `cancel-if-check-semver-failed`
pair was added following the existing pattern exactly.

---

## Items not yet accounted for in the plan

1. **`StagedInputBuffer` missing from the documented ABI** — should be added to the Phase 2
   `ConnectorDescriptor` discussion (a plugin implementing a staged parser needs to reference
   it as the concrete return type of a utility helper).

2. **`IntegratedOutputEndpoint` is absent from the plan's connector inventory** — The table
   in the plan lists "Postgres (writer)" as an integrated output but the plan never explicitly
   lists `IntegratedOutputEndpoint` in the Phase 1 ABI audit. It is defined in `integrated.rs`
   (in the `adapters` crate, not `adapterlib`), which means it is not currently part of the
   plugin ABI at all. A third-party integrated output connector cannot implement it without
   depending on `dbsp_adapters`. This should be addressed in Phase 2 (move
   `IntegratedOutputEndpoint` to `feldera-adapterlib`).

3. **`ConnectorFlags` field added in Phase 5 but descriptor struct defined in Phase 2** —
   The plan defines `ConnectorDescriptor` in Phase 2 with `flags: ConnectorFlags` already in
   it, but says "populated in PR 5". This creates a forward-reference in the type that will
   confuse Phase 2 reviewers. Consider defining `ConnectorFlags` as an empty bitfield in Phase 2
   with a TODO comment, or define the descriptor without `flags` and add it as a non-breaking
   extension in Phase 5.

4. **`ci-pre-mergequeue.yml` does not call the semver check** — Only `ci.yml` (merge queue)
   calls `invoke-check-semver`. The fast-feedback pre-merge workflow (`ci-pre-mergequeue.yml`)
   does not, which means semver regressions will not be caught until the merge queue. Adding the
   semver check to `ci-pre-mergequeue.yml` would give earlier feedback, but it requires the
   `ubuntu-latest-amd64` runner to be available in that workflow context (verify before adding).
