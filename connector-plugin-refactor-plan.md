# Connector Plugin Refactor — Comprehensive Plan

## Context

**Goal**: enterprise teams and OSS contributors can publish a Feldera connector as an independent crate (depending only on `feldera-adapterlib`), have it discovered by the controller and pipeline-manager, get fault-tolerance integration for free, and surface in the web console — without forking the engine.

**Non-goals**: dynamic-library / `dlopen`-style ABI plugins (Rust has no stable ABI; not worth the cost). Splitting every existing connector into its own crate (orthogonal; can be done later if desired). Changing what each connector does or how it implements `InputReader` / `OutputEndpoint`.

### Background — connector inventory and unifiability verdict

The connector trait surface in `feldera-adapterlib` is already plugin-shaped. Existing connectors (regular = transport+format separated; integrated = transport+format combined):

| Direction | Connector | FT model | Seekable / replay | Notes |
|---|---|---|---|---|
| in/out | `file` | ExactlyOnce | Yes (byte offset) | Follow mode |
| in | `url` | ExactlyOnce | Yes (HTTP Range) | Resumable downloads |
| in/out | `http` | ExactlyOnce | No (replay-buffer) | Direct, transient (not API-instantiable) |
| in/out | `kafka/ft` | ExactlyOnce | Offsets + txns | librdkafka, transactional |
| in/out | `kafka/nonft` | AtLeastOnce | No | High-throughput path |
| in | `s3` | ExactlyOnce | Yes (object/key) | aws-sdk-s3 |
| in | `nats` | AtLeastOnce | No | JetStream |
| in | `pubsub` | None | No | GCP Pub/Sub |
| out | `redis` | AtLeastOnce | No | Streams/lists/pub-sub |
| in | `nexmark` | ExactlyOnce | No | Synthetic generator (gated) |
| in | `clock` | ExactlyOnce | No | System-time generator, transient |
| in | `adhoc` | ExactlyOnce | No | Programmatic injection (DataFusion/Arrow), transient |
| in | `datagen` | ExactlyOnce | No | Test data generator, default JSON-Datagen format |
| **integrated** in | `postgres` (reader) | ExactlyOnce | n/a | Query execution + type mapping |
| **integrated** out | `postgres` (writer) | ExactlyOnce | n/a | Prepared INSERT/UPSERT/DELETE; has `CommandHandler` |
| **integrated** in | `postgres-cdc` | varies | n/a | Logical replication slot |
| **integrated** in/out | `delta_table` | ExactlyOnce | n/a | Parquet + DataFusion pushdown |
| **integrated** in | `iceberg` | ExactlyOnce | n/a | Catalog-driven reads, schema evolution |

**Verdict (from prior feasibility analysis)**: no single API fits all, but the existing two-trait split is the right boundary:
- `TransportInputEndpoint + Parser` covers all 13 regular shapes (seekable byte streams, message buses, generators) — seek/replay/barrier already encoded as runtime values in `Resume`, FT level via `Option<FtModel>`.
- `IntegratedInputEndpoint` / `IntegratedOutputEndpoint` covers all 5 query-engine-integrated cases — format inseparable from transport.
- `OutputEndpoint` covers every output uniformly; optional methods (`push_key`, `command_handler`, `batch_start`/`batch_end`) absorb Kafka/Postgres-specific concerns.

What blocks 3rd-party connectors today is **dispatch and tooling around those traits**, not the traits themselves.

### Coupling sites to remove

| # | Site | File / line | Nature |
| --- | --- | --- | --- |
| 1 | Closed `TransportConfig` enum | `crates/feldera-types/src/config.rs:1609` | 21 closed variants + `name()` / `is_transient()` / `is_http_input()` helpers also closed |
| 2 | Input regular factory match | `crates/adapters/src/transport.rs:85` | Hardcoded `match` |
| 3 | Output regular factory match | `crates/adapters/src/transport.rs:139` | Hardcoded `match` |
| 4 | Integrated output factory | `crates/adapters/src/integrated.rs:40` | Hardcoded `match` |
| 5 | Integrated input factory | `crates/adapters/src/integrated.rs:89` | Hardcoded `match` |
| 6 | Format registries | `crates/adapters/src/format.rs:30,49` | `Lazy<BTreeMap>` with TODO for runtime registration — **resolved by PR 3**: `inventory::collect!` slots and discovery functions moved to `adapterlib/src/format.rs`; `adapters/src/format.rs` no longer holds the registries |
| 7 | Direction validation (input vs output) | `crates/pipeline-manager/src/db/types/program.rs:682,735` | Exhaustive variant lists |
| 8 | Per-variant special cases in controller | `controller.rs:4448` (`is_http_input`), `controller.rs:4521` (`ClockInput` filter), `controller.rs:6013` (`Datagen` default-format), `controller/pipeline_diff.rs:97,106` (`is_transient`) | Reach into the enum |
| 9 | OpenAPI client codegen list | `crates/rest-api/build.rs:38-208` | `progenitor` type-replacement list, hardcoded |
| 10 | DBSP type leakage in plugin ABI | `crates/adapterlib/src/format.rs:13` and `catalog.rs:17` (two distinct `StagedBuffers` import paths — internal vs. re-exported), `format.rs:63` and `transport.rs:82` (`InputCollectionHandle` parameter), `catalog.rs` (`SerBatchReader`/`SerCursor` expose `dbsp::dynamic::{DynData, DynVec, Factory}` in 8 method signatures used by partitioned-output encoders) | Plugin ABI surface reaches transitively into `dbsp::*` |
| 11 | `IntegratedOutputEndpoint` lives in `dbsp_adapters`, not `feldera-adapterlib` | `crates/adapters/src/integrated.rs:20` | A third-party integrated output connector cannot implement it without depending on `dbsp_adapters` — **resolved by PR 2** (step 0): trait + blanket impl moved to `feldera-adapterlib`; PR 2 also wires the clean re-export topology (private `use` in `integrated.rs`, direct `pub use` in `lib.rs` from `feldera_adapterlib::transport`) so no two-hop chain ever exists |

**Existing precedent**: `inventory` is already used in this repo for `StorageBackendFactory` (`crates/storage/src/lib.rs:49`) and `CheckpointSynchronizer` (`crates/storage/src/checkpoint_synchronizer.rs:25`). pipeline-manager already runs Cargo per-pipeline (`crates/pipeline-manager/src/compiler/rust_compiler.rs:1242`, `prepare_workspace`) and already wires sccache (`rust_compiler.rs:1532-1534, 1548-1550`), `CARGO_INCREMENTAL` (`:1553`), and `cargo build --workspace --profile <profile>` (`:1574-1577`).

---

## Plan

Phases are numbered in **execution order**. Each phase is independently mergeable; nothing forces a flag day.

### Phase 1 — Stable plugin ABI in `feldera-adapterlib`

The "API contract" 3rd parties target. Most of it already exists; this phase tightens it.

1. **Audit re-exports** in `crates/adapterlib/src/lib.rs`. The plugin-facing surface is: `InputEndpoint`, `TransportInputEndpoint`, `IntegratedInputEndpoint`, `OutputEndpoint`, `InputReader`, `InputConsumer`, `OutputConsumer`, `Resume`, `InputReaderCommand`, `Parser`, `InputFormat`, `Encoder`, `OutputFormat`, `InputBuffer`, `StagedInputBuffer`, `InputCollectionHandle`, `Relation` (re-exported from `feldera-types`), `FtModel`, `ConnectorMetrics`, `CommandHandler`. Mark these as the supported plugin ABI in module docs. **Note**: `IntegratedInputEndpoint`, `InputCollectionHandle`, and `OutputConsumer` are currently `#[doc(hidden)]`; document them as supported in module docs but defer removing the `#[doc(hidden)]` attributes to Phase 4b (when integrated connectors migrate to the descriptor registry — surfacing them earlier without their callers being plugin-reachable would be confusing).
2. **Wrap both `StagedBuffers` import paths**: `dbsp::operator::input::StagedBuffers` in `crates/adapterlib/src/format.rs:13` AND `dbsp::operator::StagedBuffers` (the re-exported path) in `catalog.rs:17`. Both resolve to the same type, but the inconsistency is itself a smell. Re-export as `feldera_adapterlib::StagedBuffers` (zero-cost type alias); use this single canonical path everywhere in adapterlib. Plugins must never need to name `dbsp::*` types directly.
3. **Audit `InputCollectionHandle`** (the leak point reached transitively by `IntegratedInputEndpoint::open` at `transport.rs:82` and `InputFormat::new_parser` at `format.rs:63`). Walk every method on `InputCollectionHandle` reachable from a plugin; for any that returns DBSP-internal types, either re-export through `feldera-adapterlib::reexports::*` (zero cost) or wrap in opaque newtypes. Per-method judgment based on whether the type is genuinely public DBSP API or internal. **Explicit non-exports** (documented in the audit checklist as not part of the contract): `NodeId` (`pub` field on `InputCollectionHandle` but only consulted by the controller for backfill — third-party integrated connectors do not need it); `ClonableTrait` (only appears inside `#[doc(hidden)]` `SplitCursorBuilder` internals).
4. **Audit `SerBatchReader` and `SerCursor`** for output-path leakage. Eight methods used by partitioned-output encoders (`keys_factory`, `key_factory`, `sample_keys`, `partition_keys`, `key`, `get_key`, `seek_key_exact`, `seek_key`) expose `dbsp::dynamic::{DynData, DynVec, Factory}`. **Resolution**: add `feldera_adapterlib::reexports::{DynData, DynVec, Factory}`. Most plugin encoders never name these types and only use `cursor(format)` → `serialize_key()` / `serialize_val()`; advanced connectors doing key-based partitioning import from `reexports`.
5. **Document the fault-tolerance contract** in adapterlib module docs: a plugin advertises FT level via `InputEndpoint::fault_tolerance()`; on each step it returns `Resume::{Barrier,Seek,Replay}` from `InputConsumer::extended()`; the controller drives replay via `InputReaderCommand::Replay`. Already implemented; just under-documented.

**Why first**: every later phase produces or consumes types from this surface. Pin the surface before opening the registry.

**CI / SemVer note**: `cargo-semver-checks check-release` (added in this phase via `obi1kenobi/cargo-semver-checks-action`) downloads the previously published crate from crates.io and compares against local source. This requires `feldera-adapterlib` to remain published continuously; a temporary unpublish or rename will fail CI with a missing-baseline error rather than a semver violation. Document this in the deployment / release guide. The action installs the tool itself, so no addition to `feldera-dev` container or workspace dev-dependencies is needed; the CI job runs on `ubuntu-latest-amd64`. Following the existing `ci.yml` pattern, every new substantive job needs a matching `cancel-if-<name>-failed` sentinel — pair `invoke-check-semver` with `cancel-if-check-semver-failed`. Consider mirroring the check in `ci-pre-mergequeue.yml` for earlier feedback (verify runner availability first).

---

### Phase 2 — Connector descriptor & registration macro

0. **Move `IntegratedOutputEndpoint` from `dbsp_adapters` to `feldera-adapterlib`**. Currently defined at `crates/adapters/src/integrated.rs:20` along with its blanket impl `impl<EP: OutputEndpoint + Encoder + 'static> IntegratedOutputEndpoint for EP`. A third-party integrated output connector cannot implement it without depending on `dbsp_adapters` (the heavy crate). Move the trait + blanket impl into `feldera-adapterlib` so it joins `IntegratedInputEndpoint` on the plugin-reachable surface. Update `dbsp_adapters` to import from there. No semantic change.

1. **New type `ConnectorDescriptor`** in `feldera-adapterlib`:
   ```rust
   pub struct ConnectorDescriptor {
       pub name: &'static str,                    // MUST equal TransportConfig::name() return value (NOT the serde tag — they can differ; e.g. ClockInput: name()="clock", serde tag="clock_input")
       pub direction: Direction,                  // Input | Output
       pub kind: ConnectorKind,                   // Regular | Integrated | Transient
       pub fault_tolerance: Option<FtModel>,      // best-case; runtime can downgrade
       pub config_schema: fn() -> serde_json::Value, // JSON Schema for config
       pub default_format: Option<fn() -> FormatConfig>, // for Datagen-style
       pub flags: ConnectorFlags,                 // empty bitfield in Phase 2; populated in Phase 5
       pub build_input:  Option<BuildInputFn>,
       pub build_output: Option<BuildOutputFn>,
       pub build_integrated_input:  Option<BuildIntegratedInputFn>,
       pub build_integrated_output: Option<BuildIntegratedOutputFn>,
   }
   inventory::collect!(&'static ConnectorDescriptor);
   ```
   The four `build_*` fields take `&serde_json::Value` (the config blob) plus the contextual params each factory currently passes (consumer, parser, schema, secrets dir, controller ref). For `BuildIntegratedOutputFn` the controller param is `Arc<dyn OutputControllerRef>` (**not** `Weak`): coercing `Weak<ControllerInner>` to `Weak<dyn OutputControllerRef>` requires an `Arc` round-trip at the call site, so passing the strong reference at build time and letting the connector immediately `Arc::downgrade()` for storage is cleaner — exactly matching today's `Weak<ControllerInner>` storage pattern. One or more build fns are set per descriptor; the descriptor's `kind`+`direction` says which.

   **`ConnectorFlags` is defined as an empty bitfield in this phase** with a TODO comment naming the flags Phase 5 will add (`HTTP_DIRECT`, `AUTO_RECREATED_ON_RESTART`). Defining the field here lets Phase 5 extend it as a non-breaking change without touching descriptor consumers.

2. **`register_connector!` macro** wrapping `inventory::submit!`. Plugin authors define a `static MY_DESCRIPTOR: ConnectorDescriptor = …;` (or build it from `const fn`s) and submit it:
   ```rust
   static MY_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
       name: "my_kafka_clone_input",
       direction: Direction::Input,
       kind: ConnectorKind::Regular,
       fault_tolerance: Some(FtModel::AtLeastOnce),
       config_schema: my_config_schema,        // fn() -> serde_json::Value
       default_format: None,
       flags: ConnectorFlags::EMPTY,
       build_input:  Some(my_build_input),     // named fn, NOT a closure
       build_output: None,
       build_integrated_input:  None,
       build_integrated_output: None,
   };
   feldera_adapterlib::register_connector!(&MY_DESCRIPTOR);
   ```

   **Implementation constraint** (`inventory::submit!` requires const-evaluable expressions): `build_*` fields must be **named function items**, not closures. Non-capturing closures coerce to `fn` pointers in expression position but are not const-evaluable in `static` initializers in current Rust. The macro therefore takes a `&'static ConnectorDescriptor` expression directly. An ergonomic named-fields macro over this can be added later if every field is restricted to const-evaluable expressions (function items, not closures); the simpler expr-form shipped here is forward-compatible with that.

3. **Discovery API** on `feldera-adapterlib`: `pub fn registered_connectors() -> impl Iterator<Item = &'static ConnectorDescriptor>` and `pub fn connector_by_name(name: &str) -> Option<&'static ConnectorDescriptor>`.

**Note on naming**: `ConnectorMetadata` already exists in adapterlib (`crates/adapterlib/src/connector_metadata.rs`) but means *per-record metadata* (Kafka topic name, Avro schema id). Use the distinct name `ConnectorDescriptor` to avoid collision.

---

### Phase 3 — Format registry

Add `inventory::collect!(&'static dyn InputFormat)` and `inventory::collect!(&'static dyn OutputFormat)` slots to **`adapterlib/src/format.rs`** (alongside the trait definitions, not in `adapters/src/format.rs` where the old `Lazy<BTreeMap>` lives). The discovery functions `get_input_format()` / `get_output_format()` move to adapterlib with them. The `Lazy<BTreeMap>` statics and old discovery functions in `adapters/src/format.rs` are removed entirely; existing call sites in `adapters` pick up the new functions via the existing `pub use feldera_adapterlib::format::*` re-export — **zero call-site churn**. Built-in formats (`csv`, `json`, `parquet`, `avro`, `raw`) submit themselves from their own modules.

**Why placement matters**: external format crates can only depend on `feldera-adapterlib`. Putting `inventory::collect!` in `adapters` would silently exclude them — they would compile but their `submit!` calls would land in a slot nothing iterates. This is the same constraint that drives `ConnectorDescriptor`'s placement in adapterlib (Phase 2).

**Why now**: small, isolated, proves the inventory-based pattern in this codebase before applying it to connectors.

**Const-eval constraint** (same as Phase 2): `inventory::submit!` requires const-evaluable expressions. The five built-in format types are zero-sized unit structs, so `&CsvInputFormat as &dyn InputFormat` is const-evaluable directly (fat-pointer ZST coercions are const-stable). A future format with fields must declare a `static MY_FORMAT: MyFormat = MyFormat { ... };` and submit `&MY_FORMAT as &dyn InputFormat`. Closures will not compile.

**No `unsafe impl Sync`**: the trait bound `InputFormat: Send + Sync` makes `&'static dyn InputFormat: Sync` automatically. This differs from `ConnectorDescriptor` (PR 2), which carries `fn` pointers in a struct and cannot derive `Sync` without auto-trait propagation working through. Both end up `Sync`, but the format case needs no annotation.

**In-tree vs external submission**: in-tree format modules call `inventory::submit!` directly (matching the storage-backend pattern in `storage/backend/posixio_impl.rs`). The `register_input_format!` / `register_output_format!` macros exported from adapterlib are pass-through wrappers for external crates that don't have `inventory` as a direct dep. (Earlier draft tried `$crate`-prefixed casts inside the macro for ergonomics; dropped due to nested-macro hygiene issues. The simple pass-through is consistent with the storage precedent.)

**Feature-gated formats**: avro lives entirely behind `#[cfg(feature = "with-avro")]` at the module boundary, so its `inventory::submit!` calls are naturally feature-gated without per-call `#[cfg]` annotations — cleaner than the original BTreeMap which gated individual map entries.

**Asymmetric formats**: `raw` is input-only; the inventory approach handles this naturally (one `submit!` instead of two). The original BTreeMap reflected the same asymmetry by simply omitting `raw` from `OUTPUT_FORMATS`.

---

### Phase 4 — Migrate built-in connectors onto the registry

Per connector (mechanical):
1. Add `register_connector! { … }` block in the connector's module (or in a small `lib.rs` for that connector's submodule). The descriptor's `name` field must equal `TransportConfig::name()` exactly — verify against the `name()` impl in `feldera-types/src/config.rs` rather than guessing from the type or serde tag (e.g. `PubSubInput::name() == "pub_sub_input"`, not `"pubsub_input"`). **Connectors in separate crates** (`feldera-iceberg`, future bundled connectors that live outside `dbsp_adapters`): do not add `inventory` as a dependency to that crate. Instead, place the descriptor in a shim module under `crates/adapters/src/integrated/<name>.rs` (gated by the connector's `with-*` feature) whose `build_*` calls into the external crate's constructor. The shim keeps `inventory` confined to `adapters`; the implementation crate stays clean.
2. Set `kind: ConnectorKind::{Regular,Integrated,Transient}` directly — `ConnectorKind` is a Phase 2 enum, available immediately. Per-variant **flag** bits (`AUTO_RECREATED_ON_RESTART`) and the `default_format` field are populated only after Phase 5 adds them, so connectors with those special cases (clock's auto-recreate behaviour, datagen's default format) are revisited in Phase 4b. Connectors that only need `kind` (file, the message buses, …) need no Phase 5 dependency.
3. Delete the corresponding match arm from `crates/adapters/src/transport.rs` / `crates/adapters/src/integrated.rs`. **For feature-gated connectors** (`nats`, `pubsub`, `redis`, `kafka`, …): delete *both* the `#[cfg(feature = "with-X")]` arm and the `#[cfg(not(feature = "with-X"))]` stub arm. The catch arm's `Ok(None)` handles the feature-disabled case unconditionally; the `cfg(not)` stub existed only to keep the match exhaustive when the impl types didn't exist, and is no longer needed. **When the deletion removes the last constructing arm**: the surrounding `let endpoint = match config { ... }; Ok(Some(endpoint))` block becomes `!`-typed (every remaining arm diverges via `return`/`Err(...)`), making the trailing `Ok(Some(endpoint))` unreachable. Do **not** add `#[allow(unreachable_code)]`; instead collapse the match in the same PR. For the regular-transport factories: replace with `match config { _ => Ok(None) }`. For the integrated factories (where the catch arm is `return Err(unknown_*_transport(...))`): replace with a direct `Err(...)` return and drop the trailing `if connector_config.format.is_some()` guard (the registry path already enforces it). PR 7g strips the now-empty `match` shells; not collapsing here would push an `#[allow(unreachable_code)]` into a downstream PR.
4. Delete now-dead re-exports in the connector's wrapper module. Modules like `nats.rs` / `pubsub.rs` typically have `pub use input::NatsInputEndpoint` re-exports whose only consumer was the fallback match in `transport.rs`. After migration the build function in the connector module constructs the endpoint directly; the re-export becomes an unused-import warning. Delete it. **Exception**: when the migrated connector's `build_*` lives in the same wrapper file as the re-export (kafka pattern), the re-export stays alive because `build_kafka_input` / `build_kafka_output` consume it in-module. Only the corresponding `use` lines in `transport.rs` are removed.
5. **Integrated-output constructors** (`postgres`, `delta_table`): change `Endpoint::new()` to accept `Arc<dyn OutputControllerRef>` (not `Arc<ControllerInner>`) and call `Arc::downgrade(&controller)` internally. Stored field types become `Weak<dyn OutputControllerRef>`. There is no Any-based downcast back to `Arc<ControllerInner>` — the constructor change is forced by the `BuildIntegratedOutputFn` signature. Replace existing `controller.status.foo()` call sites with the trait calls `controller.foo()` (the `OutputControllerRef` impl on `ControllerInner` from PR 6 delegates to `self.status.*` internally); after this, `ControllerInner` is no longer imported by the output module. Update tests: any `Weak::<ControllerInner>::new()` (dangling weak used as a null controller) is replaced with `Arc::new(NoOpControllerRef)`, where `NoOpControllerRef` is a local stub struct implementing the four `OutputControllerRef` methods as no-ops. Nested test submodules (e.g. `tests::parallel`) need `use super::NoOpControllerRef;`. **Also audit `crates/adapters/benches/`**: criterion benches construct these same endpoints directly and live *outside* the `#[cfg(test)]` gate, so `cargo check --all-features` will not flag broken bench call-sites. Run `cargo build --all-features --benches` (or `cargo check --tests --benches`) as part of the PR's CI gate. Place a shared `NoOpControllerRef` in `benches/bench_common.rs` (with `#[allow(dead_code)]` because not every bench that includes the module references the stub).
6. **Feature-gated sub-connectors in always-enabled modules** (e.g. `postgres_cdc_input` inside `postgres.rs`, which itself has top-level `inventory::submit!` for the always-enabled postgres connectors): wrap the gated descriptor in a feature-gated submodule — `#[cfg(feature = "with-X")] mod X_descriptor { use super::*; ... static X_DESCRIPTOR: ConnectorDescriptor = ...; inventory::submit!(&X_DESCRIPTOR); }`. Putting `#[cfg]` on just the `inventory::submit!` leaves the build function and config-type references reachable when the feature is off, causing type errors. The `mod` wrapper hides everything together.
7. Add a `connector_by_name` descriptor test in the connector module (e.g. `input.rs::registry_test::pub_sub_input_descriptor`). This test must run in the default build — gate the connector's *integration* tests on whatever live infrastructure they need (`with-pubsub-emulator-test`, etc.), but the registry test only touches `connector_by_name` and has no external dependencies.
8. Keep the typed `TransportConfig` variant (recommended path); only `name()` learns to handle the upcoming `Plugin` variant in Phase 7.

**Staging** (this phase is split around Phase 5+6):
- **Phase 4a (before Phase 5)**: migrate `file` and `clock` end-to-end — descriptor + `build_*` fns + registry-dispatch path inserted before the existing match in both factory functions. The migrated arms move into the `Ok(None)` catch arm (input) or are deleted entirely (output, where `_ => Ok(None)` already absorbs them); other connectors' match arms stay intact. This is the smallest end-to-end exercise of the registry: descriptor → `connector_by_name` → `build_*` → endpoint construction. PR 4 also introduces `transport_config_inner_as_json` (in `adapters/src/transport.rs`, NOT adapterlib), which Phase 6 reuses unchanged.
- **Phase 5 implicit migrations**: replacing `is_http_input()`, the `ClockInput` filter, the `Datagen` default-format injection, and `is_transient()` with descriptor lookups (Phase 5's actual goal) **requires those connectors to already be in the registry**. So Phase 5 unavoidably migrates `http_input`, `http_output`, `adhoc_input`, and `datagen` ahead of Phase 4b. The two-phase factory pattern from Phase 4a absorbs them automatically — adding a descriptor short-circuits the registry path before the fallback match, and the dead arms move to `Ok(None)`. After Phase 5, the registered set is `{file_input, file_output, clock, http_input, http_output, adhoc_input, datagen}`.
- **Phase 4b (after Phase 6)**: sweep the **remaining** connectors — `s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, plus integrated (`postgres`, `delta_table`, `iceberg`). Kafka late because of its `ft`/`nonft` split: register `KAFKA_INPUT_DESCRIPTOR` (name="kafka_input") and `KAFKA_OUTPUT_DESCRIPTOR` (name="kafka_output") as separate descriptors — `TransportConfig::name()` differs between `KafkaInput` and `KafkaOutput`, so registry lookup demands one descriptor per name. The output descriptor's `build_output` receives the existing `fault_tolerant: bool` argument and branches between `KafkaFtOutputEndpoint` and `KafkaOutputEndpoint` internally.

After Phase 4b, the four factory `match` statements in `transport.rs` and `integrated.rs` consist purely of registry calls, with no per-variant arms remaining.

**`#[doc(hidden)]` cleanup (PR 7g, commit 2)**: with integrated connectors now reachable through the descriptor registry from out-of-tree code, remove `#[doc(hidden)]` from `IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle` (the struct *and* its `new()` constructor — independently annotated, would otherwise be inconsistent), and `OutputConsumer`. These were kept hidden in Phase 1 because surfacing them without their callers being plugin-reachable would have been confusing. They're now first-class plugin ABI. Each newly-public type needs a real one-sentence doc comment — `#[doc(hidden)]` was the only thing previously suppressing the missing-doc lint, so naked attribute removal will fail the lint. The cleanup also includes editing `crates/adapterlib/src/lib.rs`'s module-level ABI overview: rewrite the introductory paragraph that explains the `#[doc(hidden)]` carve-out (no longer relevant) and drop inline `(\`#[doc(hidden)]\`)` annotations from the per-type bullets in the "Supported plugin-facing types" lists. Any inline comments next to the attribute (e.g. `"#[doc(hidden)] is a temporary marker — it will be removed in Phase 4b…"`) go alongside the attribute they referenced.

**Bench-driven concrete-type exposure (deferred to PR 7h)**: `pub use crate::integrated::postgres::PostgresOutputEndpoint` and the `pub` qualifier on `mod delta_table` are not part of the connector plumbing — they were added by the original postgres-bench and parallel-delta-encoder commits to let criterion benches import the concrete types directly. They predate the registry refactor and remain load-bearing for benches throughout PRs 1–7g. PR 7h migrates the benches to registry dispatch and then removes both. Outside the benches, no caller imports either path (verified with `rg`); inside `crates/adapters/src/integrated/`, sibling files reach `delta_table`'s helpers via `crate::integrated::delta_table::*` paths that work identically with `mod delta_table` (private modules are reachable through `crate::` from inside the same crate).

**Integrated output controller plumbing**: `ControllerInner` must implement `OutputControllerRef` so the build function's `Arc<dyn OutputControllerRef>` parameter has a concrete impl. The trait's four methods (audited against actual call sites in Postgres writer + Delta during PR 2; `impl OutputControllerRef for ControllerInner` compiles with zero body changes to `ControllerInner`):

| Method | Source on `ControllerInner` |
| --- | --- |
| `output_transport_error(&self, endpoint_id: u64, endpoint_name: &str, fatal: bool, error: AnyError, tag: Option<&str>)` | direct method |
| `update_output_connector_health(&self, endpoint_id: u64, health: ConnectorHealth)` | direct method |
| `register_batch_progress_counter(&self, endpoint_id: &u64, counter: Arc<AtomicU64>)` | delegates to `self.status.*` |
| `output_buffer(&self, endpoint_id: u64, num_bytes: usize, num_records: usize)` | delegates to `self.status.*` |

The trait flattens the `controller.status.*` indirection — connectors call `controller.register_batch_progress_counter(...)` directly on `Arc<dyn OutputControllerRef>` instead of reaching through `controller.status`. **Existing constructors must change signature**: `DeltaTableWriter::new()` and `PostgresOutputEndpoint::new()` (and any future integrated-output `new()`) take `Arc<dyn OutputControllerRef>` and `Arc::downgrade()` internally; stored fields become `Weak<dyn OutputControllerRef>`. There is no path from `Arc<dyn OutputControllerRef>` back to `Arc<ControllerInner>` (no `Any` downcast), so a wrapper that "translates" at the build-fn boundary is impossible — the change reaches the connector's public constructor. After the change, the output module stops importing `ControllerInner` entirely. Test modules that used `Weak::<ControllerInner>::new()` as a dangling-weak null controller need a local `NoOpControllerRef` stub implementing the four trait methods as no-ops; pass `Arc::new(NoOpControllerRef)` to constructors. **Bench call-sites are silent until built**: criterion benches under `crates/adapters/benches/` construct these endpoints directly but live outside `#[cfg(test)]`; verifying the migration with `cargo check --all-features` will miss them. The migrating PR must run `cargo build --all-features --benches` and place a shared `NoOpControllerRef` in `benches/bench_common.rs`. **Land the impl block in PR 6 (Phase 6), not with the first integrated-output connector migration**: the integrated output factory's registry dispatch coerces `Arc<ControllerInner>` → `Arc<dyn OutputControllerRef>`, and that coercion requires `ControllerInner: OutputControllerRef` at compile time even when the dispatch path is dead code (no integrated descriptors registered yet). The skeleton is recorded in `connector-plugin-refactor-notes-pr2.md`. Two of the four method names (`output_transport_error`, `update_output_connector_health`) shadow existing inherent methods on `ControllerInner` — the impl must use UFCS (`ControllerInner::output_transport_error(self, …)`) inside those two arms to avoid the trait method shadowing the inherent method during name resolution.

---

### Phase 5 — Capability methods replace per-variant special cases

Convert `ConnectorFlags` from the Phase 2 placeholder (plain `u32` newtype with `EMPTY` and `contains()`) to a `bitflags!`-generated type. **`bitflags` is NOT currently a workspace dep** (the earlier draft of this plan assumed it was a transitive dep — it isn't); add `bitflags = "2"` directly to `crates/adapterlib/Cargo.toml` rather than to the workspace `[dependencies]` table. Single-crate direct dep is simpler than workspace promotion until a second crate needs it. `EMPTY` is preserved as `Self::empty()` for source compatibility; `contains()` is drop-in compatible. No call-site changes outside the struct definition.

Concrete flag values (stable from this point forward — future flags use the next available bits):
- `HTTP_DIRECT = 0x01`
- `AUTO_RECREATED_ON_RESTART = 0x02`

Add capability fields to `ConnectorDescriptor` (extending the type from Phase 2) and rewrite the four scattered uses of "what kind of connector is this" as descriptor lookups:

| Today | After |
| --- | --- |
| `transport.is_transient()` (`controller/pipeline_diff.rs:97,106`) | `descriptor_for(transport).kind == ConnectorKind::Transient` |
| `transport.is_http_input()` (`controller.rs:4448`) | `descriptor.flags.contains(ConnectorFlags::HTTP_DIRECT)` |
| `match TransportConfig::ClockInput(_)` filter (`controller.rs:4521`) | `descriptor.flags.contains(ConnectorFlags::AUTO_RECREATED_ON_RESTART)` |
| `match (TransportConfig::Datagen(_), None)` default format (`controller.rs:6013`) | `descriptor.default_format` returns `Some(JSON-Datagen)` |

These special cases stop being grep-targets; they become metadata that any plugin can also declare.

**Registry-completeness invariant** (introduced by this phase, must be maintained going forward): every transient connector — and every connector with a flag the controller queries — MUST register a descriptor. The new lookups `connector_by_name(...).map(|d| d.kind == Transient).unwrap_or(false)` silently degrade to `false` for unregistered connectors. Phase 5 ensures all four existing transient connectors (`clock`, `http_input`, `http_output`, `adhoc_input`) are registered; the `is_transient()` method on `TransportConfig` can be removed only after this is verified. Add this invariant to the connector-authoring guide (Phase 11): a new transient connector must register a `ConnectorKind::Transient` descriptor, not just implement the trait.

**Default-format ownership moves to the connector**: the JSON-Datagen format spec (`format_name="json"`, `JsonFlavor::Datagen`, `JsonUpdateFormat::Raw`, `array=true`, `lines=Multiple`) was hardcoded in `controller.rs`. After Phase 5 it lives in `datagen/src/lib.rs::datagen_default_format()`. The controller no longer imports `JsonParserConfig` / `JsonFlavor` / `JsonUpdateFormat` / `JsonLines` for this purpose. **Side effect**: the "custom format not supported" error message becomes generic (`"{name} endpoints do not support custom formats"` instead of `"datagen endpoints do not support custom formats"`); any test asserting on the literal `datagen` substring needs updating.

**`build_*: None` is a valid descriptor pattern.** `http_output` registers a descriptor with `build_output: None` because `HttpOutputEndpoint` is constructed by the HTTP server (`server.rs:2238`) at request time — its `name`/`format`/`backpressure` parameters come from the request, not from a stored config. The descriptor exists purely to expose `kind: ConnectorKind::Transient` for `pipeline_diff.rs` lookups. The output factory's registry path handles this correctly: descriptor found → `build_output` is `None` → `Ok(None)`, identical to the previous fallback. Document this as a valid pattern: descriptors-for-metadata-only, no factory dispatch required.

**`inventory` must be a direct dep**: `register_connector!` expands to `::inventory::submit!`, which requires `inventory` as a direct dep of the calling crate (transitive through `feldera-adapterlib` is not enough). The `datagen` crate had to add `inventory = { workspace = true }` to its `Cargo.toml` for its `submit!` to compile. Document this in the connector-authoring guide. **Bundled-connector exception**: when adding `inventory` to a separate crate (e.g. `feldera-iceberg`) is undesirable coupling, write a thin descriptor shim in `crates/adapters/src/integrated/<name>.rs` that calls into the external crate's constructor — `inventory` stays in `adapters` only. Out-of-tree plugin authors take `inventory` as a direct dep regardless; the shim is purely a bundled-only convenience for keeping non-adapter crates clean.

**`secrets_dir` and `fault_tolerant` are opt-in `build_*` parameters**: the factory hands `secrets_dir: &Path` to every `build_*` function and `fault_tolerant: bool` to every `build_output`, but most connectors do not need them. Bind `_secrets_dir` in `build_*` for every bundled connector — `kafka` included. Secret reference resolution happens at the factory entry via `resolve_secret_references_via_json` *before* the build function is called; the build function receives an already-resolved config value. The `secrets_dir` parameter is reserved for connectors that store credential *file paths* in their config and must resolve them at construction time; no bundled connector currently does. Bind `_fault_tolerant` in `build_output` for `redis` and any other output whose `is_fault_tolerant()` is unconditionally `false`. Only `kafka` consumes `fault_tolerant` (to switch between its ft and nonft output impls). Document in the connector-authoring guide: prefix-with-underscore is the correct declaration for these parameters unless your connector materially uses them; the factory passes them unconditionally for signature uniformity.

**Add a CI lint** that fails if `controller.rs` matches on a `TransportConfig` variant outside the dispatch entry points. Stops anyone from re-introducing per-variant special-casing.

---

### Phase 6 — Replace factory matches with registry lookup

**Scope**: Phase 6 covers **two of four** factory functions — `create_integrated_output_endpoint` (`integrated.rs:40`) and `create_integrated_input_endpoint` (`integrated.rs:89`). The other two — `input_transport_config_to_endpoint` (`transport.rs:85`) and `output_transport_config_to_endpoint` (`transport.rs:139`) — already received the two-phase dispatch pattern in Phase 4a / PR 4 (along with the `transport_config_inner_as_json` helper). Phase 6 mirrors that pattern in the integrated factories.

Each factory function is rewritten to:

1. **Resolve secrets first** via `resolve_secret_references_via_json` (unchanged from today's behaviour). The factory is the right home for secrets resolution because it's generic across connectors; `build_*` functions receive an already-resolved `JsonValue` and never need to look at `secrets_dir` themselves. The `secrets_dir: &Path` argument on `BuildInputFn` / `BuildOutputFn` is kept on the signature for external connectors that bypass the factory and call `build_fn` directly with their own secret resolution.
2. Resolve `TransportConfig` → `(name, config_value)` via `transport_config_inner_as_json`. The helper serialises the whole `TransportConfig` to `{"name": "...", "config": {...}}` and extracts the `"config"` field; for unit variants (e.g. `HttpOutput`) with no content, it returns `JsonValue::Null`. The helper **lives in `adapters`, not `adapterlib`**, because it depends on `feldera-types::config::TransportConfig`; adapterlib's `BuildInputFn` takes `&JsonValue` precisely so adapterlib stays free of that dep. Visibility was `fn` (private to `transport.rs`) after PR 4; PR 6 promotes it to `pub(crate) fn` so `integrated.rs` can call it. No promotion to `pub` — keep the helper crate-private.
3. Look up descriptor by `config.name()` via `connector_by_name(&name)`. **Critical**: the lookup string is `TransportConfig::name()`, NOT the serde tag — these can differ (e.g. `ClockInput::name() == "clock"` while its serde tag is `"clock_input"`). Each descriptor's `name` field must equal the `name()` return value.
4. Match `descriptor.direction × kind` against the call site (input vs output, regular vs integrated). Mismatches return the same `Ok(None)` / `unknown_*_transport` errors as today.
5. Invoke `descriptor.build_*` with the resolved config value and contextual params.

**`impl OutputControllerRef for ControllerInner` lands here**: the integrated output registry dispatch coerces `Arc<ControllerInner>` (obtained via `controller.upgrade()`) to `Arc<dyn OutputControllerRef>` before calling `build_integrated_output`. Even though no integrated descriptors are registered yet (Phase 4b adds them in PR 7f), the coercion appears in source and must compile. The impl is zero-cost — all four methods delegate to existing `ControllerInner` or `ControllerInner.status.*` methods. Two methods (`output_transport_error`, `update_output_connector_health`) name-collide with inherent methods on `ControllerInner`; the impl uses UFCS (`ControllerInner::output_transport_error(self, …)`) inside those arms to avoid the trait method shadowing the inherent one during name resolution. `EndpointId = u64` is a type alias (not a newtype), so the trait's `u64` parameter types are directly compatible with the inherent methods.

**`controller.upgrade()` failing at dispatch time**: a `Weak<ControllerInner>` upgrade can return `None` if the controller has been dropped. This is a programming error (factory called after teardown), not a config error. The integrated factory maps it to `ControllerError::invalid_transport_configuration` as a placeholder; semantically imperfect but acceptable in dead code (no integrated connectors are dispatched via the registry until PR 7f). A more precise variant can be added later if real call sites surface it.

**Format-check duplication is by design**: integrated connectors do not accept formats (the post-match check `if connector_config.format.is_some()` rejects the config). After Phase 6, this check appears twice — once inside the registry dispatch block (before calling `build_fn`), and once after the fallback match. A single pre-dispatch check would be wrong because the registry path returns early before reaching the fallback. Future integrated connectors that *do* accept formats can opt out by removing the check from their registry path. The same per-path-check pattern is already used in the transport factory.

**Lookup performance**: `connector_by_name()` walks the `inventory::iter` (a `#[link_section]`-based linked list, O(n)). For 17 built-in connectors plus a typical handful from `connectors.toml`, this is fast enough at pipeline startup. The factory functions are called once per endpoint at pipeline start, never on hot paths, so no caching layer is needed in adapterlib. If a future caller ends up in a hot path, cache as `HashMap<&'static str, &'static ConnectorDescriptor>` at the call site rather than complicating the adapterlib API.

**Transitional state**: when Phase 6 lands, the registered set is `{file_input, file_output, clock, http_input, http_output, adhoc_input, datagen}` (from PR 4 + PR 5). All seven are *transport* connectors — none are integrated. The integrated factory's registry-dispatch block is therefore live infrastructure waiting on dead data: `connector_by_name` returns `None` for every integrated connector name, and execution always falls through to the existing match. PR 7f activates the path by registering integrated descriptors. **Pattern from PR 4**: as each connector migrates, its match arm is moved into the `Ok(None)` catch arm rather than left as dead code (Rust's exhaustiveness check + future "unreachable pattern" lints would otherwise flag it). For the output factory, `_ => Ok(None)` already absorbs them — explicit arms can be deleted. **When the last constructing arm is removed** (output factory after PR 7d, input after PR 7e, both integrated factories within PR 7f) the match's residual type becomes `!`, making the `let endpoint = match { … }; Ok(Some(endpoint))` framing unreachable. Per Phase 4 step 3, that PR collapses the framing — `match config { _ => Ok(None) }` for transport, direct `Err(unknown_*_transport(...))` for integrated — rather than carrying `#[allow(unreachable_code)]` forward. PR 7g, commit 1 then strips the empty `match { _ => Ok(None) }` shells. This keeps every commit functional and testable, with no `#[allow]` annotations along the way.

This is the largest mechanical change but does **not** touch any connector implementation. Every `TransportInputEndpoint` / `OutputEndpoint` / `IntegratedInputEndpoint` / `IntegratedOutputEndpoint` impl is unchanged.

---

### Phase 7 — Open the `TransportConfig` enum

The serde tag `#[serde(tag = "name", content = "config")]` already addresses connectors by string name. Add an escape hatch for unknown names while keeping in-tree connectors strongly-typed:

```rust
pub enum TransportConfig {
    FileInput(FileInputConfig),
    // ...all existing variants...
    Plugin(PluginTransportConfig),
}
pub struct PluginTransportConfig {
    pub name: String,
    pub config: serde_json::Value,
}
```

Implement `Deserialize` manually: match known names to existing variants, route unknown names into `Plugin{name, config}`. The serialized form for in-tree connectors stays byte-for-byte unchanged — **no migration of stored pipelines**.

Side updates:
- `TransportConfig::name()` adds an arm `Plugin(p) => p.name.clone()` (`crates/feldera-types/src/config.rs:1638-1662`).
- `is_transient()`, `is_http_input()` removed; replaced by descriptor lookups (Phase 5).

**Alternative considered and rejected** — flatten the enum entirely to `struct TransportConfig { name: String, config: serde_json::Value }`. Cleaner but loses type-safe variant exhaustiveness across the codebase; not worth the churn.

**Why after Phase 4 completes**: with every bundled connector already registry-driven, the `Plugin` variant is purely an extension point, not a workaround for migration in flight.

---

### Phase 8 — `connectors.toml` + describer binary

The exhaustive match at `crates/pipeline-manager/src/db/types/program.rs:682,735` validates that an `INPUT` SQL relation gets an input-direction connector and vice versa. This is the only structural reason pipeline-manager needs to "know" connectors. The deployer-controlled list lives in a `connectors.toml` file owned by pipeline-manager; descriptors are extracted by building a tiny "describer" binary against that list.

**Design principle**: pipeline-manager carries zero hardcoded connector knowledge. The set of connectors is whatever the deployer declared in `connectors.toml`, plus the bundled connectors compiled into `dbsp_adapters`. The descriptor *type* and `register_connector!` macro live in `feldera-adapterlib` (a contract, not a list); each connector crate registers itself; pipeline-manager learns the set via a build step it already runs.

#### 8.1 `connectors.toml` content and storage

The unit of configuration is a **per-tenant `connectors.toml` blob** stored as a row in the pipeline-manager database. The blob's text is exactly what Cargo accepts as a `[dependencies]` body — one dep per line, **no section header** — mirroring the `udf_toml` mechanism at `rust_compiler.rs:1336-1346` (opaque text spliced into a `[dependencies]` block downstream):

```toml
acme_snowflake = { version = "0.3", registry = "crates.io" }
my_sap_cdc     = { git = "https://github.com/acme/feldera-sap-cdc", tag = "v1.2.0" }
local_thing    = { path = "/opt/feldera/connectors/local-thing" }
```

Whatever Cargo accepts (versions, git refs, `path`, `[patch]`, alternative registries, auth) is accepted here without translation. **Single-line entries only** — multi-line `[dependencies.<name>]` table form is not supported (force-link extraction is line-based; see 8.3). Bundled connectors (`file`, `kafka`, `postgres`, `delta_table`, `iceberg`, …) stay in `dbsp_adapters` and are not listed here — an empty blob = OSS defaults, behavior matches today's bundled-only deployments bit-for-bit.

**Database is the source of truth.** Storing the blob in the DB (rather than on the deployer's filesystem) gives:
- a clean multi-tenant model (one row per tenant, no filesystem path components or sanitization);
- audit trail (who edited, when, with what value) for free;
- optimistic concurrency via row version (ETag → `If-Match`);
- HA-clean (no per-node filesystem state);
- a usable in-product editor surface (Phase 8.12).

**Optional bootstrap seed.** A single deployment-wide file at `connectors_toml_path` (a `CompilerConfig` field — see PR 9) is read **once per tenant**: on tenant creation, if the new tenant has no row yet, the manager imports the file as that tenant's initial blob. After that the DB is authoritative; the file is never re-read for that tenant. Operators who pre-bake a default plugin set into the deployment image use this; everyone else leaves the field unset and accepts an empty default blob. The seed mechanism is explicitly removable in a future PR if it adds friction.

**Schema sketch** (DDL details in PR 9):
```
table tenant_connector_config (
    tenant_id     UUID    primary key references tenant(id),
    content       TEXT    not null,           -- the toml blob, possibly empty
    content_hash  TEXT    not null,           -- sha256 of content; the cache key + ETag
    version       BIGINT  not null,           -- monotonic, incremented on every PUT
    edited_at     TIMESTAMPTZ not null,
    edited_by     TEXT    not null            -- user identifier from auth context
);
```

#### 8.2 Reuse pipeline-manager's existing Cargo orchestration

pipeline-manager already runs Cargo per-pipeline. `prepare_workspace` (`crates/pipeline-manager/src/compiler/rust_compiler.rs:1242`) creates a `rust-compilation` workspace, copies a templated `Cargo.toml` into it, integrates UDF dependencies, and invokes Cargo. Two extensions to that flow:

- **Per-pipeline workspace**: read the tenant's blob from `tenant_connector_config` and splice it verbatim into the per-pipeline `Cargo.toml`'s `[dependencies]` block (same string-substitution pattern as `udf_toml` at `rust_compiler.rs:1336-1346` — Cargo is the authoritative parser; pipeline-manager treats the text as opaque). **No per-pipeline feature gating** — see 8.5.
- **Describer build (new)**: a separate, much smaller workspace whose only purpose is producing the descriptor manifest. See 8.3.
- **Force-link generation (new)**: a Cargo dep alone is not enough — `inventory::submit!` only fires if the linker actually pulls the rlib in, and rustc drops unused deps. Both the per-pipeline workspace and the describer crate need a generated `force_link.rs` (or equivalent) containing one `extern crate <crate_name> as _;` line per connector dep. See 8.3 for the implicit built-in entry for `feldera-datagen` (always present, not user-visible).

#### 8.3 The describer binary

A 30-line Rust crate generated by pipeline-manager whenever `connectors.toml` changes:

```rust
// crates/feldera-describer/src/main.rs (generated)
fn main() {
    let descriptors: Vec<&'static ConnectorDescriptor> =
        feldera_adapterlib::registered_connectors().collect();
    println!("{}", serde_json::to_string(&descriptors).unwrap());
}
```

Its generated `Cargo.toml` lists `feldera-adapterlib` + `dbsp_adapters` (for bundled connectors) + every entry from `connectors.toml`. pipeline-manager builds it once per connector-set, runs it, captures the JSON. That JSON is the manifest used by `program.rs:682,735` validation:

```rust
let manifest = self.descriptors(); // cached HashMap<String, ConnectorDescriptor>
let descriptor = manifest.get(&connector.config.transport.name())
    .ok_or(ConnectorGenerationError::UnknownConnector { ... })?;
if !descriptor.direction.allows_input() {
    return Err(ConnectorGenerationError::ExpectedInputConnector { ... });
}
```

**Generated force-link list.** Every out-of-tree connector — built-in or third-party — needs an `extern crate <name> as _;` in any build output that calls `connector_by_name`, or rustc drops the rlib. The describer codegen writes `force_link.rs` alongside `main.rs` (declared via `mod force_link;`) with one line per dep:

```rust
// crates/feldera-describer/src/force_link.rs (generated)
// === Built-in (implicit; not user-configurable) ===
extern crate feldera_datagen as _;
// === User-listed (from connectors.toml) ===
extern crate acme_snowflake as _;
extern crate my_sap_cdc as _;
```

**Extraction rule.** Pipeline-manager scans the blob's text line-by-line, skips blank lines and `#`-prefixed comments, takes the substring before the first `=` on each remaining line, trims it, and emits `extern crate <ident> as _;` (mapping any `-` in the dep key to `_` for Rust identifier rules — Cargo does the same internally). No `toml` parser dep is needed; the only structural assumption is that each dep occupies one line. The built-in section is hardcoded in pipeline-manager's codegen and not exposed in `connectors.toml`. The same `force_link.rs` is generated into every per-pipeline workspace.

**Per-pipeline placement.** Inside the per-pipeline workspace, `force_link.rs` lives in the existing **globals crate** (`<pipeline>_globals`), not in a new dedicated crate — the globals crate already exists, already runs `prepare_workspace`'s injection pass, and already declares `mod force_link;`. A new crate would add `[workspace.members]` plumbing for no benefit. The codegen must add `("force_link.rs", true)` to the globals crate's expected `src_content` list at `rust_compiler.rs:1435-1448` or `DirectoryContent::validate()` will reject the directory.

**One additional force-link source: `dbsp_adapters`'s own tests.** Test builds aren't covered by any codegen, so the line is hand-written in `lib.rs` (PR 5). It only needs datagen — no third-party connector is exercised by `dbsp_adapters` tests. The axis is *which build output runs `connector_by_name`*, not built-in vs. third-party.

**Why a describer rather than describing-via-pipeline-binary**: the describer compiles in seconds (no DBSP, no SQL-generated code), is pinned independently from per-pipeline builds, and serves as the single source of truth for the lockfile shared across all builds in this deployment (see 8.4).

#### 8.4 Caching and lockfile policy

Treat the describer as a long-lived deployment artifact, and **share its lockfile with per-pipeline builds**:

- **Cache key**: `sha256(connectors.toml content || ADAPTERLIB_API_VERSION)`. Same key → reuse the cached manifest and lockfile, skip rebuild. Using the plugin ABI version (not the full Feldera release version) means a Feldera patch or minor release that does not change the connector plugin ABI does **not** force a describer rebuild or a connector recompile.
- **`describer.lock` is the per-tenant single lockfile** shared by every per-pipeline build for that tenant. Persisted at `<working_dir>/describer/<tenant_id>/<cache_key>/describer.lock` where `cache_key = sha256(content || ADAPTERLIB_API_VERSION)` — note this is **not** the same as `tenant_connector_config.content_hash` (which is `sha256(content)` alone). pipeline-manager copies it into both (a) the describer's workspace and (b) **every per-pipeline workspace for that tenant** as `Cargo.lock`. Same lock per tenant = identical transitive resolution within a tenant = identical sccache keys for all shared crates. **This is what makes the design build-cache-friendly** (see 8.5 for full analysis). Tenants do not share lockfiles — that's intentional, see 8.8.
- **`cargo build --locked`** for both describer and per-pipeline builds — once a lock exists, with a three-tier warm-path discipline:
  - **Cold path** (no `describer.lock` yet, or `POST /refresh`): build without `--locked`; Cargo resolves and writes `Cargo.lock`.
  - **Full warm path** (`describer.lock` exists AND `describer.build_version` matches the current Feldera release version): copy the lock as `Cargo.lock` and pass `--locked`. Both Feldera path deps and external connector pins are exactly as recorded.
  - **Partial warm path** (`describer.lock` exists but `describer.build_version` differs, i.e. Feldera was upgraded within the same `ADAPTERLIB_API_VERSION`): copy the lock as a resolver seed (preserving external connector pins) but omit `--locked` so Cargo can update the Feldera path-dep versions. sccache serves previously compiled external connector rlibs, so effective build time is minimal.
  - After every successful build, persist the updated `Cargo.lock` as `describer.lock` and write the current Feldera release version to `describer.build_version`.
  - Document this explicitly — silently dropping `--locked` on the cold path defeats the drift guarantee.
- **Updating the lock**: two paths. (1) `PUT /v0/connectors/connectors.toml` (Phase 8.12) auto-triggers a rebuild after a config edit. (2) An explicit `POST /v0/connectors/refresh` admin endpoint runs `cargo update` against the describer workspace (lock churn even when the blob is unchanged) and rewrites the lock — useful for CI to pick up upstream patch releases on a schedule. No automatic drift in production.
- **Manifest cache**: the produced JSON manifest is persisted as `manifest.json` next to `describer.lock` and `describer.build_version`. pipeline-manager loads it at startup without invoking Cargo unless the cache key has changed. In-process, an `Arc<ManifestCache>` held in `ServerState` tracks build state per tenant (`NotConfigured | Building | Ready | Failed`) with staleness guards so a slow build completing after a newer PUT does not overwrite a fresher state.

#### 8.5 Build-cache-friendliness analysis

This section explicitly works through what makes the design cache-friendly and what would break it.

**Compilation graph after Phase 8**:

```
rust-compilation/                      ← per-pipeline workspace
  Cargo.toml                           ← per-pipeline (lists deps)
  Cargo.lock                           ← copied from describer.lock  ★
  crates/
    pipeline_globals/  ← per-pipeline (UDFs)
    pipeline/          ← per-pipeline (SQL→Rust output)
  + path-resolved or registry deps:
    dbsp_adapters/                       ← shared, bundled
    dbsp/, feldera-{adapterlib,types}/   ← shared
    acme-snowflake/, my-sap-cdc/, …      ← shared, from connectors.toml
```

**Cache keys** (sccache wraps `rustc`): each crate's `.rlib` is keyed on `(source hash, rustc version, RUSTFLAGS, features, transitive dep rlib hashes)`. Hit if all match a previous compile. **Linking is never cached.**

**Per-pipeline steady-state cost** (after first build with given `connectors.toml`):

| Stage | Cost | Cacheable |
|---|---|---|
| Compile shared deps (`dbsp_adapters`, listed connectors, transitive) | 0 | ✅ sccache hit |
| Compile `pipeline_globals` (UDF) | seconds | ✗ |
| Compile `pipeline` crate (SQL→Rust) | seconds–tens of seconds | ✗ |
| Link final binary | 5–20 s | ✗ (always) |
| **Total per pipeline** | **~10–40 s** | |

**One-time cost** per `(connectors.toml, feldera-adapterlib)` combo: cold compile of every listed connector + transitive deps, minutes once, then cached forever.

**Things that defeat caching — and how the plan prevents each**:

1. **Lockfile drift between describer and per-pipeline workspace.** Different transitive resolution = different rlib hashes = full rebuild every pipeline. **Prevented**: `describer.lock` is copied verbatim as `Cargo.lock` into every per-pipeline workspace; both builds use `--locked`.
2. **Per-pipeline feature variation.** If pipelines enabled different feature subsets, every shared crate would get a unique cache key per pipeline. **Prevented**: do *not* feature-gate per pipeline. List all `connectors.toml` connectors as direct deps in every per-pipeline build; unused ones get dead-code-eliminated by the linker. (An earlier draft of this plan proposed per-pipeline feature gating as an optimization; that was dropped because the cache-key combinatorics defeat the rest of the design.)
3. **`RUSTFLAGS` variation per pipeline.** Different flags = different cache keys for shared crates. **Prevented**: pipeline-manager fixes `RUSTFLAGS` once at startup from config and never varies per-pipeline. Already the existing behavior at `rust_compiler.rs:1544` (passes through manager-level env), but document it as a constraint.
4. **Build script non-determinism in 3rd-party connectors.** A `build.rs` that reads timestamps or unstable env vars caches differently each run. **Mitigated, not prevented**: document the well-behaved-build-script requirement; the reference connector example (Phase 11) demonstrates the rule.
5. **Profile variation.** `--profile unoptimized` vs `--profile optimized` produces different rlibs. **Accepted**: switching profile = cold rebuild. Already true today.

**Linker is the per-pipeline floor.** Concrete recommendation: set `RUSTFLAGS="-C link-arg=-fuse-ld=mold"` (or lld) at pipeline-manager startup. mold/lld cuts link time 5–10× vs. system ld. Single biggest per-pipeline speedup available.

**Future optimization (do not implement on day one)**: a "platform archive" — pre-link `dbsp_adapters` + listed connectors into a single static rlib once per `(connectors.toml, feldera-adapterlib)` combo; per-pipeline build links one rlib instead of N. Material speedup for large `connectors.toml` (≥10 listed). Defer until measurements justify it.

**Steady-state expectation**: with the discipline above, **per-pipeline build time is approximately the same as today** (no `connectors.toml`) plus a small linker delta proportional to the number of listed connectors. The cold-build cost moves from "per pipeline" to "per `connectors.toml` change" — paid by one pipeline build after each refresh, then amortized.

#### 8.6 Name collisions

Two listed crates registering `name: "kafka"` is a real risk. Behavior: the describer fails fast at startup with a clear message naming both source crates. Resolution options for the deployer: drop one crate from `connectors.toml`, or ask the upstream to namespace (`acme:kafka`). Detect by walking the `inventory` and checking for duplicate names; emit an error from the describer rather than letting later code see ambiguous lookups.

**Where the check lives**: in the describer binary's startup, **not** in `connector_by_name()` (which is intentionally a simple O(n) lookup that returns the first match). Keeping enforcement in the describer keeps the adapterlib API minimal and lets the manager surface the duplicate-name diagnostic with deployment context (which `connectors.toml` entries collided).

#### 8.7 `feldera-adapterlib` version pinning

Connector plugin ABI compatibility is enforced at **two levels**:

**Level 1 — compile time (primary guard):** the describer's `Cargo.lock` pins exactly one `feldera-adapterlib` version. Every listed connector must compile against it. Cargo rejects incompatible majors at dependency-resolution time; type errors catch subtler breakage. Because this is static linking, an incompatible connector binary can never be produced — the describer build simply fails. CI on `feldera-adapterlib` runs `cargo semver-checks` to catch accidental breaking changes that don't come with a major bump.

**Level 2 — runtime manifest guard (defence in depth):** `pipeline-manager` stamps `ADAPTERLIB_API_VERSION` into every `ConnectorManifestEntry` and verifies the field when it reads `manifest.json` back from disk. This check does *not* fire in normal operation — when the ABI version changes the cache key changes, a fresh build is triggered, and the newly produced manifest carries the current version. The check exists to catch **stale cached manifests**: if a `manifest.json` from a previous ABI generation ends up in the wrong cache directory (file copy, concurrent upgrade, operator error, cache-management bug), the version mismatch is detected before the stale manifest can be used for SQL direction validation. Without this check, a silently reused stale manifest would let through SQL programs that reference connector names or capabilities from a different ABI generation.

**`ADAPTERLIB_API_VERSION` is also the cache key component** (see 8.4). This is what allows Feldera minor/patch releases — which don't touch the plugin ABI — to share the same cache directory and reuse the connector manifest without triggering a rebuild. Operators upgrading Feldera need not recompile connectors unless the ABI version increments.

**Implementation**: `ADAPTERLIB_API_VERSION: u32 = 0` in `feldera-types/src/constants.rs`. `ConnectorManifestEntry` carries `adapterlib_api_version: u32` as its first field, set by `from_descriptor` from the constant. After running the describer, `pipeline-manager` calls `check_manifest_api_versions(json)`, which deserializes as `Vec<ConnectorManifestEntry>` and returns `DescriberError::IncompatibleApiVersion { connector_name, required_version, deployment_version }` on the first mismatch. `feldera-adapterlib` is added as a direct dependency of `pipeline-manager` for this import. Increment `ADAPTERLIB_API_VERSION` (with a `CHANGELOG.md` entry) only on breaking ABI changes; never on additive extensions.

#### 8.8 Multi-tenant deployments

Per-tenant config blobs are rows in `tenant_connector_config` keyed by `tenant_id` — no per-tenant filesystem layout, no path-component sanitization, no `connectors.d/` directory.

**Build artifacts are still per-tenant on the filesystem.** Cache directory at `<working_dir>/describer/<tenant_id>/<cache_key>/{describer.lock, describer.build_version, manifest.json, target/}` where `cache_key = sha256(content || ADAPTERLIB_API_VERSION)`. Tenants share **nothing** in the build cache: each pays its own first-build cost. Intentionally less efficient than a global cache but eliminates cross-tenant correctness bugs (tenant A's `[patch]` doesn't shadow tenant B's deps; tenant A's git-rev pins don't poison tenant B's resolution).

**`tenant_id` as a path component is safe by construction.** Tenant IDs are UUIDs (existing schema), so the path component is always 36 hex+dash characters — no sanitization needed. (The previous plan worried about email-style tenant identifiers; with DB storage, the human-readable tenant *name* never reaches the filesystem.)

#### 8.9 Hardening `connectors.toml` (deployment guidance)

Same supply-chain trust model as adding any Rust dep. Document in deployment guide:
- Prefer pinned `rev = "<sha>"` over `tag` for git deps (tags can be moved).
- For high-security deployments, use `path = "..."` pointing at vendored sources via `cargo vendor`.
- Audit any `[patch]` section.
- Build scripts run with deployment privileges; review them before adding a connector.

#### 8.10 Fallback — defer validation

If, for a first iteration, even the describer build is too much complexity, pipeline-manager can accept any `{name, config}` pair and let the controller surface direction errors at pipeline startup. Worse UX (errors at start instead of at SQL parse) but minimal pipeline-manager change. Documented escape hatch; not the recommended default.

#### 8.11 Interaction with `ProgramStatus`

The per-program compilation state machine at `crates/pipeline-manager/src/db/types/program.rs:153` (`Pending → CompilingSql → SqlCompiled → CompilingRust → Success | SqlError | RustError | SystemError`) is intentionally **orthogonal to the describer**. The describer is a deployment-level artifact, not a per-pipeline phase, and adding a new `ProgramStatus` variant for "describer building" would conflate two scopes (one manifest serves N programs).

**Concrete rules:**

- **No new `ProgramStatus` variant.** The describer either has a current manifest in cache or it doesn't. Per-program lifecycle does not change.
- **Manifest is a precondition for `Pending → CompilingSql`.** When the SQL compiler picks up a `Pending` program, it consults the tenant's cached manifest. If the manifest is missing (cold tenant, first build still in progress, or describer build failed), the program stays `Pending` — it does **not** advance and immediately fail. The user-visible channel for "why is my program stuck" is `GET /v0/connectors/status` (Phase 8.12), which returns the tenant's describer state (`ready`/`building`/`failed`/`not_configured`) and any captured build error.
- **Bundled-only tenants skip the describer entirely.** A tenant with an empty `tenant_connector_config.content` (the default for newly-created tenants when `connectors_toml_path` is unset, or after explicit clearing) means the manifest is constructed in-process from the `inventory` walk of the manager's own linked descriptors — no Cargo invocation, no pre-condition. `GET /v0/connectors/status` reports `state: "not_configured"`. Program lifecycle is bit-for-bit unchanged from today.
- **Refresh-vs-in-flight policy: snapshot semantics.** Both `PUT /v0/connectors/connectors.toml` (auto-rebuild) and `POST /v0/connectors/refresh` (lock churn) rebuild the manifest in the background. **In-flight `CompilingSql` jobs keep using the manifest snapshot they loaded at job start.** New transitions out of `Pending` after the rebuild completes pick up the new manifest. This avoids cancelling work mid-compile and keeps the per-program state machine deterministic; the cost is that a config edit does not retroactively re-validate already-successful compilations (which is correct — the user is asking what to do *next*, not to invalidate prior outputs).
- **Validation failures map to `SqlError`, not `SystemError`.** When `program.rs:682,735` rejects an unknown connector or a direction mismatch (PR 11), the program transitions to `SqlError` with a message naming the connector and pointing at the tenant's `connectors.toml`. `SystemError` remains reserved for OS/process-level faults (Cargo failed to spawn, disk full, etc.) — manifest-content disagreements are not those.
- **Describer build failure for one tenant does not block other tenants.** Each tenant's describer build is independent. `GET /v0/connectors/status` and the refresh endpoint are tenant-scoped; an operator diagnoses and retries the failing tenant in place. Pipelines belonging to tenants that don't reference plugin connectors still build (their SQL compile only consults bundled descriptors, which need no manifest).

**Tests to add in PR 11**: program stuck in `Pending` while describer is still building; program advances to `CompilingSql` once manifest is available; rebuild after `PUT /v0/connectors/connectors.toml` during in-flight compile uses old snapshot; unknown plugin connector → `SqlError` not `SystemError`.

#### 8.12 Config storage and editing API

The tenant blob is read/written through three endpoints. All are tenant-scoped via the existing auth context.

**`GET /v0/connectors/status`** — returns the unified status + manifest envelope:

```json
{
  "state": "ready" | "building" | "failed" | "not_configured",
  "content_hash": "abc123…",        // present when state in {ready, building, failed}
  "version": 7,                     // tenant_connector_config.version
  "last_built_at": "2026-04-29T…",  // present when state in {ready, failed}
  "error": "stderr from describer", // present iff state == failed
  "descriptors": [ … ]              // present iff state == ready; null otherwise
}
```

`ETag: "<content_hash>"`; clients use `If-None-Match` for cheap polling (304 with no body when nothing changed). One endpoint serves both consumers — the connectors-management status badge polls it ignoring `descriptors`; the SQL editor's connector picker reads `descriptors`.

**`GET /v0/connectors/connectors.toml`** — returns the raw blob plus headers needed for safe edits:

```
200 OK
ETag: "<content_hash>"
Content-Type: text/plain; charset=utf-8
X-Connectors-Version: 7
X-Connectors-Edited-At: 2026-04-29T…
X-Connectors-Edited-By: alice@acme.example

acme_snowflake = { version = "0.3", registry = "crates.io" }
…
```

**`PUT /v0/connectors/connectors.toml`** — updates the blob and **auto-triggers a rebuild**:

- Body: the new toml text. Content-Type `text/plain`. Empty body = clear the blob (returns to bundled-only).
- `If-Match: "<content_hash>"` is **required**. Mismatch → `412 Precondition Failed` (optimistic concurrency; another operator beat this PUT).
- Manager validates the body cheaply (line shape — see force-link extraction rule in 8.3) and rejects on parse-shape error. Deeper validation (does it actually compile?) happens during the rebuild and surfaces via `GET /v0/connectors/status` returning `state: "failed"`.
- On success: writes the row (incrementing `version`, recomputing `content_hash`), enqueues a describer rebuild, returns `202 Accepted` with `Location: /v0/connectors/status` and the new `content_hash`/`version` in the body. PUT does **not** block on the rebuild.
**AuthZ.** All four endpoints use the existing tenant-scoped auth — any authenticated user with access to the tenant can read and edit the blob. No role distinction in the initial release. Rationale: the supply-chain trust model already says "review before adding a connector" and applies equally to any operator; introducing an admin role now would gate the in-product editor behind RBAC plumbing the deployment may not have. The audit trail (`edited_by` / `edited_at`) is the review surface. A future PR can add role gating without breaking the API shape.

**Save semantics summary** — auto-rebuild on PUT (option (a) from earlier discussion). Rationale: the alternative (require explicit refresh after edit) splits a single user intent across two API calls; pre-validate (run `cargo metadata` before accepting) blocks the PUT for seconds and still doesn't catch every error. Auto-rebuild keeps the contract simple: PUT is the edit; status reports the outcome.

---

### Phase 9 — REST API / OpenAPI for plugin configs

The hardcoded list at `crates/rest-api/build.rs:38-208` exists because the *generated client* (Rust SDK) needs strongly-typed structs for in-tree connector configs. For 3rd-party plugins, two acceptable answers:

1. **Opaque-on-client (recommended)**: plugin configs serialize as `serde_json::Value` in client SDKs. The full schema is available at runtime via `GET /v0/connectors/status` (Phase 8.12), whose `descriptors` field returns every registered descriptor's JSON Schema. The web console reads this list to populate the connector picker and config form (see Phase 10). Matches how Kafka Connect, Airbyte, and Trino plugins work.
2. **Re-generate per deployment**: each enterprise build of the manager regenerates `openapi.json` with its plugin types replacement-listed. Possible but high-friction.

Take (1). Keep the existing list for in-tree connectors (no change for OSS users); plugins are opaque on the client and discovered at runtime.

The status endpoint also unblocks documentation tooling. Phase 9's PR is now mostly OpenAPI plumbing — schema definitions for `GET /v0/connectors/status`, `GET /v0/connectors/connectors.toml`, `PUT /v0/connectors/connectors.toml` (the endpoints themselves ship in PR 10, Phase 8.12).

---

### Phase 10 — Web console connector discovery

The web console currently knows about each connector via the typed OpenAPI client. Switch to:

1. On first connector-picker render, call `GET /v0/connectors/status` and cache its `descriptors` field.
2. Render the connector dropdown from descriptor list.
3. Render the config form from descriptor's JSON Schema (any of the existing schema-driven form libraries works; the project already uses Monaco for SQL).
4. For in-tree connectors the rich existing forms remain and are preferred; the descriptor lookup gracefully falls back to a generic JSON-Schema form.

Files: connector picker components in `js-packages/web-console/src/lib/`. The existing forms can be migrated incrementally — there is no flag day.

---

### Phase 11 — Reference plugin + integration test

Add `crates/connector-example/` containing a minimal "hello" input connector that:
- Implements `TransportInputEndpoint` and `InputReader`.
- Calls `register_connector!`.
- Has its own `Cargo.toml` depending only on `feldera-adapterlib` — proves the surface is closed.
- A pipeline-manager integration test enables this crate via a build feature flag (test-time, not pipeline-time), registers it, and runs a pipeline that ingests from "hello://".

The test exercises the full plugin path end-to-end: `connectors.toml` → describer → manifest → SQL parse → pipeline build → runtime dispatch. Guards against regressions where someone re-introduces a hardcoded match arm or special-case somewhere downstream.

---

## What changes per existing connector

For each in-tree connector, the diff is:

- **Add**: one `register_connector! { ... }` block (≤30 lines) listing name, direction, kind, FT level, schema fn, and named build functions (not closures — `inventory::submit!` requires const-evaluable expressions).
- **Remove**: the matching arm in `transport.rs` / `integrated.rs`. When that deletion empties the surrounding `match`, collapse the residual framing in the same PR (see Phase 4 step 3).
- **No change for regular connectors**: trait impls (`TransportInputEndpoint`, `InputReader`, `OutputEndpoint`, `Parser`, `Encoder`, …). The connector logic, threading model, FT machinery, error handling — all untouched.
- **Change for integrated-output connectors only** (`postgres`, `delta_table`): `Endpoint::new()` signature switches from accepting `Arc<ControllerInner>` (or whatever the existing controller-typed parameter is) to `Arc<dyn OutputControllerRef>`; stored fields move from `Weak<ControllerInner>` to `Weak<dyn OutputControllerRef>`; `controller.status.foo()` call sites become `controller.foo()`; test stubs replace dangling `Weak::<ControllerInner>::new()` with `Arc::new(NoOpControllerRef)`; criterion bench stubs (in `benches/bench_common.rs`) follow the same swap. The trait surface (`IntegratedOutputEndpoint`) is unchanged.

Connector code that today uses `dbsp::*` directly remains in-tree; only `feldera-adapterlib`-only code is portable to a separate crate.

## What does not change

- The trait surface — exactly as it stands today.
- Wire/serde format of `TransportConfig` for in-tree connectors (existing pipelines deserialize identically).
- The `Resume`/`Replay`/`Seek`/`Barrier` fault-tolerance protocol.
- The `InputReaderCommand` state machine.
- The controller's checkpoint, replay, transaction, and step-driving logic.

---

## Risks and open questions

Status legend: ✅ resolved by plan / ⚠️ accepted with mitigation / 🔧 audit task before implementation.

1. ✅ **No global registry in pipeline-manager**. pipeline-manager intentionally has no central list of connectors. It learns the descriptor set from the describer binary it builds against `connectors.toml` (Phase 8). Trade-off: pipeline-manager cannot validate against a connector that the deployer hasn't listed — that is the correct behavior. Verify with a build test that mutates `connectors.toml` and observes the manifest changing.

2. ⚠️ **Versioning the plugin ABI**. `feldera-adapterlib` becomes a versioned public surface; major bumps force plugin recompiles. Same model as `tokio`-based ecosystems. **Mitigation**: SemVer policy; `cargo semver-checks` in CI on `feldera-adapterlib` to break on accidental ABI changes without a major bump. Document the supported-versions matrix per Feldera release.

3. 🔧 **DBSP type leakage in `IntegratedInputEndpoint`** (Phase 1 audit task). `IntegratedInputEndpoint::open(&InputCollectionHandle, …)` and `InputFormat::new_parser(&InputCollectionHandle, …)` are the leak points; `InputCollectionHandle`'s methods may transitively expose `dbsp::*` types. **Action**: walk every method on `InputCollectionHandle` reachable from a plugin; for any returning DBSP-internal types, either re-export through `feldera-adapterlib::reexports::*` (zero cost) or wrap in opaque newtypes. Per-method judgment based on whether the type is genuinely public DBSP API or internal.

4. ✅ **`Datagen` default-format quirk**. After Phase 5 this becomes `descriptor.default_format`. **Action**: add a CI lint that fails if `controller.rs` matches on a `TransportConfig` variant outside the dispatch entry points.

5. ✅ **Cargo-feature interaction with registry**. `register_connector!` calls go inside the same `#[cfg(feature = "with-…")]` blocks that already gate the connector module. Pattern confirmed at `transport.rs:42-55`.

6. ✅ **Describer rebuild stability**. `describer.lock` is the pin; `cargo build --locked` is enforced; `PUT /v0/connectors/connectors.toml` (auto-rebuild) and `POST /v0/connectors/refresh` (lock churn) are the only paths that update the lock. Documented in 8.4.

7. ⚠️ **Supply-chain trust**. Same model as any Rust dep. Documented in 8.9 (prefer rev pins over tags, vendor for high-security deployments, audit `[patch]`, build scripts run with deployment privileges).

8. ✅ **Compile time for large `connectors.toml`**. Worked through in 8.5. **Recipe**: shared lockfile (`describer.lock` copied as per-pipeline `Cargo.lock`), `--locked` everywhere, no per-pipeline feature gating, `RUSTFLAGS` fixed at manager level, `mold`/`lld` linker. Steady-state per-pipeline cost ≈ today's cost + small linker delta. The earlier-proposed per-pipeline feature-gating optimization is **dropped** because it would create cache-key combinatorics that defeat the rest of the design. Future "platform archive" optimization remains an option if measurements demand it.

9. ✅ **Multi-tenant cache invalidation**. Per-tenant directory at `/var/lib/feldera/describer/<tenant>/<content-hash>/`; no cross-tenant sharing. Documented in 8.8.

---

## Phase summary (execution checklist)

1. **Phase 1 + Phase 2** — descriptor type + macro + ABI tightening. Foundational, no behavior change.
2. **Phase 3** — format registry. Small, isolated, proves the inventory-based pattern.
3. **Phase 4a** — migrate `file` and `clock` (no special cases). Factory functions still have match arms for everything else.
4. **Phase 5 + Phase 6** — capability methods on the descriptor, then rewrite the four factory functions to use the registry. Phase 5 implicitly migrates `http_input`, `http_output`, `adhoc_input`, and `datagen` because the controller-rewrites query their descriptors. After Phase 6, the factory body is "registry path + fallback match for unmigrated connectors".
5. **Phase 4b** — sweep the remaining bundled connectors (`s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, integrated). The fallback match shrinks until empty. At this point, all bundled connectors are registry-driven, but pipeline-manager still uses the old exhaustive match for direction validation.
6. **Phase 7** — open `TransportConfig` enum. The `Plugin` variant accepts unknown names as `{name, config}`; bundled connectors keep their typed variants. No deserialization breakage for existing pipelines.
7. **Phase 8** — `connectors.toml` + describer + lockfile policy. After this, pipeline-manager validates against the described set; with an empty `connectors.toml`, behavior matches today's bundled-only deployments.
8. **Phase 9** — OpenAPI surface for `GET /v0/connectors/status`, `GET/PUT /v0/connectors/connectors.toml`, `POST /v0/connectors/refresh`.
9. **Phase 10** — web console consumes the discovery endpoint; in-tree connector forms remain.
10. **Phase 11** — reference plugin + integration test seals the contract end-to-end (a real out-of-tree connector goes through `connectors.toml` → describer → manifest → SQL parse → pipeline build → runtime dispatch).

Each phase is independently mergeable; nothing forces a flag day. The describer (Phase 8) is the largest single chunk of new infrastructure and is the right gate before exposing plugins externally.

---

## Suggested PR breakdown

Each PR below is sized to be independently reviewable and mergeable. Dependencies are explicit. Phases that are too large for one PR are split (Phase 4b across PR 7a–7g; Phase 8 across PR 9–13). Phases that are small enough to combine are combined.

### PR 1 — Tighten the plugin ABI surface (Phase 1)
- [ ] Audit `crates/adapterlib/src/lib.rs` re-exports; document the supported plugin-facing types in module docs (including `OutputConsumer` and `StagedInputBuffer`).
- [ ] Replace **both** `StagedBuffers` import paths (`format.rs:13` uses `dbsp::operator::input::StagedBuffers`; `catalog.rs:17` uses `dbsp::operator::StagedBuffers`) with a single canonical `feldera_adapterlib::StagedBuffers` re-export. Both paths resolve to the same type; pick one source of truth.
- [ ] Walk every method on `InputCollectionHandle` reachable from a plugin; for any returning DBSP-internal types, re-export through `feldera-adapterlib::reexports::*` or wrap in opaque newtypes. Document the audit results in a checklist file. Explicitly mark `NodeId` and `ClonableTrait` as **not** part of the contract (controller-internal / `#[doc(hidden)]`-internal respectively).
- [ ] Audit `SerBatchReader` / `SerCursor` for the eight methods that expose `dbsp::dynamic::{DynData, DynVec, Factory}` (used by partitioned-output encoders). Add `feldera_adapterlib::reexports::{DynData, DynVec, Factory}`. Most plugin encoders never need to name these.
- [ ] Document `IntegratedInputEndpoint`, `InputCollectionHandle` (the struct and its independently-annotated `new()` constructor), and `OutputConsumer` in module docs as supported plugin ABI **even though they remain `#[doc(hidden)]`** — removing the attribute is deferred to Phase 4b. The Phase 1 module docs in `crates/adapterlib/src/lib.rs` will list these types with `(\`#[doc(hidden)]\`)` annotations and an introductory paragraph explaining the carve-out; both will be edited away in PR 7g, commit 2.
- [ ] Add module docs explaining the FT contract (`fault_tolerance()` → `Resume::*` → `InputReaderCommand::Replay`).
- [ ] Add `cargo-semver-checks` CI job (`obi1kenobi/cargo-semver-checks-action`) on `feldera-adapterlib`; pair `invoke-check-semver` with `cancel-if-check-semver-failed` per the existing `ci.yml` pattern.
- [ ] Document the SemVer policy in the crate README, including the operational note that **`feldera-adapterlib` must remain published on crates.io** for the CI baseline check to function (a temporary unpublish or rename surfaces as a missing-baseline error, not a semver violation).
- [ ] Optional: mirror the semver check in `ci-pre-mergequeue.yml` for earlier feedback (verify the `ubuntu-latest-amd64` runner is available in that workflow context first).
- [ ] Verify all existing connectors still build against the tightened surface.
- **Depends on**: nothing.
- **Unblocks**: PR 2.

### PR 2 — `ConnectorDescriptor` + `register_connector!` macro (Phase 2)
- [ ] **Move `IntegratedOutputEndpoint`** and its blanket impl from `crates/adapters/src/integrated.rs:20` to `feldera-adapterlib`, alongside `IntegratedInputEndpoint`. Update `dbsp_adapters` to import from the new location. No semantic change. Without this move, third-party integrated output connectors would have to depend on `dbsp_adapters`. **Re-export topology — get this right in PR 2, not later**: `crates/adapters/src/integrated.rs` should `use feldera_adapterlib::transport::IntegratedOutputEndpoint;` (private — just enough to bring the trait into scope for function signatures inside the module), and `crates/adapters/src/lib.rs` should `pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;` directly. **Do not** leave a `pub use` in `integrated.rs` re-exporting through to `lib.rs` — that creates a two-hop chain (`feldera_adapterlib::transport → adapters::integrated → adapters::lib`) with no consumer (verified: no caller ever imports `dbsp_adapters::integrated::IntegratedOutputEndpoint`), and PR 7g would otherwise have to collapse it. The public path stays `dbsp_adapters::IntegratedOutputEndpoint` either way; this just chooses the cleaner internal topology from the start.
- [ ] Define `ConnectorDescriptor` struct, `Direction` enum, `ConnectorKind` enum.
- [ ] Define `ConnectorFlags` as an **empty bitfield with a TODO comment** naming the flags Phase 5 will add (`HTTP_DIRECT`, `AUTO_RECREATED_ON_RESTART`). Plain `u32` newtype with `EMPTY` and `contains()` is fine here; Phase 5/PR 5 promotes it to `bitflags!`. Drop-in compatible.
- [ ] Define the four `Build*Fn` function-pointer types matching the existing factory signatures. `BuildIntegratedOutputFn` takes `Arc<dyn OutputControllerRef>` (**not** `Weak`) — the connector's `new()` immediately downgrades for storage. Define `OutputControllerRef` trait with the **four** methods audited from the actual call sites: `output_transport_error`, `update_output_connector_health`, `register_batch_progress_counter`, `output_buffer` (the last two flatten the `controller.status.*` indirection). Exact signatures are recorded in `connector-plugin-refactor-notes-pr2.md`. `ControllerInner` will impl this in PR 6 (the integrated factory's registry dispatch coerces to `Arc<dyn OutputControllerRef>`, requiring the impl at compile time).
- [ ] Add `inventory::collect!(&'static ConnectorDescriptor)` slot.
- [ ] Implement `register_connector!` macro as a **bare wrapper around `inventory::submit!`** taking a `&'static ConnectorDescriptor` expression. **Do not** ship the named-fields/closure form: `inventory::submit!` requires const-evaluable expressions, and non-capturing closures are not const in `static` initializers in current Rust. Document the constraint and the named-function-only path; an ergonomic macro restricted to function items can be added later.
- [ ] **Do not** add `unsafe impl Sync for ConnectorDescriptor` / `Send`. Every field auto-derives `Send + Sync` by construction: bare `fn(...)` pointers are unconditionally `Send + Sync` regardless of argument/return types, `Direction` / `ConnectorKind` are unit-variant enums, `FtModel` is a `Copy + Clone` enum, `ConnectorFlags` is a `bitflags!` newtype over `u32`, and `&'static str` is auto-`Send + Sync`. The auto-derived `Send + Sync` is **architecturally required** (descriptors live in `static`s and are iterated by `inventory::iter` from any thread), not coincidental. Add a doc-comment invariant on the struct: *"All fields must auto-derive `Send + Sync`. Descriptors are stored in `static`s and shared across threads via `inventory::iter`; `Send + Sync` is a required invariant, not a coincidence. Use bare `fn(...)` pointers instead of `Box<dyn …>` for extension points; if a future field cannot auto-derive these traits, redesign the field rather than adding `unsafe impl`."* This converts the safety property from "happens to hold today" into "structural rule the compiler enforces" — stronger than an `unsafe impl` ever could be.
- [ ] Add discovery API: `registered_connectors()`, `connector_by_name()`. The latter is intentionally a simple O(n) walk; collision detection is the describer's job (Phase 8/PR 10), not adapterlib's.
- [ ] Unit tests: register a stub descriptor, look it up, verify both Iterator and by-name access.
- [ ] Document a name distinct from existing per-record `ConnectorMetadata`.
- **Depends on**: PR 1.
- **Unblocks**: PR 3, PR 4.

### PR 3 — Convert format registries to `inventory` (Phase 3)
- [ ] **Add `inventory::collect!` slots and discovery functions in `adapterlib/src/format.rs`**, alongside the trait definitions — *not* in `adapters/src/format.rs`. External format crates can only depend on adapterlib; placing the slots in `adapters` would silently exclude them.
- [ ] Remove the `Lazy<BTreeMap>` statics (`INPUT_FORMATS`, `OUTPUT_FORMATS`) and the old `get_input_format` / `get_output_format` from `adapters/src/format.rs` entirely. Drop the `once_cell::sync::Lazy` and `BTreeMap` imports there. (`once_cell` stays in `Cargo.toml` — used elsewhere in `adapters`.)
- [ ] Verify call sites in `adapters` (`controller.rs`, `server.rs`, `test.rs`, etc.) pick up the new functions via the existing `pub use feldera_adapterlib::format::*` re-export — **expect zero call-site churn**.
- [ ] Each built-in format (`csv`, `json`, `parquet`, `avro`, `raw`) submits itself from its module via `inventory::submit! { &FORMAT_FACTORY as &dyn InputFormat }` (or `OutputFormat`). Built-ins use `inventory::submit!` directly — no need to route through the macros.
- [ ] Add `register_input_format!` and `register_output_format!` macros in `adapterlib/src/lib.rs` as **pass-through wrappers** around `inventory::submit!`. These are for external crates that don't have `inventory` as a direct dep. Do **not** attempt the `$crate`-prefixed-cast form — nested-macro hygiene is unreliable; the pass-through pattern matches the storage-backend precedent.
- [ ] Const-eval note: built-in formats are unit structs, so `&CsvInputFormat as &dyn InputFormat` works directly. Document the `static MY_FORMAT: MyFormat = ...; submit!(&MY_FORMAT as &dyn InputFormat)` pattern for future formats with fields. Closures will not compile.
- [ ] No `unsafe impl Sync` on format factories — `InputFormat: Send + Sync` makes `&'static dyn InputFormat` automatically `Sync`. (Differs from `ConnectorDescriptor` in PR 2, which needs explicit annotation.)
- [ ] Asymmetry: `raw` is input-only — only one `submit!` call in `raw.rs`, no `RawOutputFormat`.
- [ ] Feature gating: `avro/input.rs` and `avro/output.rs` `submit!` calls compile only when `with-avro` is active because the entire `avro` module sits under `#[cfg(feature = "with-avro")]` in `format.rs`. No per-call `#[cfg]` needed.
- [ ] Cleanup: remove the now-unused `pub use input::JsonInputFormat` and `pub use output::JsonOutputFormat` re-exports in `format/json.rs` (they only existed to feed the old BTreeMap; the compiler will flag them as unused). Aligns `json.rs` with `csv.rs` / `parquet.rs` / `raw.rs`.
- [ ] Add unit tests in `adapterlib/src/format.rs` covering registry mechanics with stub format types (iterate, look up by name, miss on unknown). The built-in format names are NOT visible to `cargo test -p feldera-adapterlib` (link-time discovery), which is correct.
- [ ] Add a follow-up test in `adapters/src/format.rs` (or a dedicated test file) that asserts every expected built-in name (`csv`, `json`, `parquet`, `raw`, plus `avro` under feature) is present in the registry. Guards against accidental removal of a `submit!` call.
- [ ] Remove the TODO comment about runtime registration in `adapters/src/format.rs`.
- [ ] Verify all format-using tests still pass.
- **Depends on**: PR 2.
- **Unblocks**: nothing strictly (proves the pattern; the pass-through macros also become the model for the format-side of the Phase 11 reference plugin doc).

### PR 4 — First connector migrations: `file` and `clock` (Phase 4a)
- [ ] Add `FILE_INPUT_DESCRIPTOR` + `FILE_OUTPUT_DESCRIPTOR` statics with `build_file_input` / `build_file_output` factory functions in `crates/adapters/src/transport/file.rs`; `inventory::submit!` both. Use `Direction::Input` and `Direction::Output` — registry lookup is keyed on `TransportConfig::name()` (`"file_input"` / `"file_output"`), which differs per direction.
- [ ] Add `CLOCK_DESCRIPTOR` static with `build_clock_input` factory function in `crates/adapters/src/transport/clock.rs`; `inventory::submit!`. Use `kind: ConnectorKind::Transient` (matches today's `TransportConfig::is_transient()` behaviour for `ClockInput` — Phase 5 will replace the call site, but the kind classification belongs in PR 4 already).
- [ ] **Descriptor `name` must equal `TransportConfig::name()`, not the serde tag.** Concretely: `FILE_INPUT_DESCRIPTOR.name = "file_input"`, `FILE_OUTPUT_DESCRIPTOR.name = "file_output"`, `CLOCK_DESCRIPTOR.name = "clock"` (not `"clock_input"` — the serde tag and `name()` diverge for clock; the registry lookup uses `name()`).
- [ ] `config_schema` placeholder: `fn() -> JsonValue::Object(Default::default())` for now. Real JSON Schema lands in PR 14 alongside the discovery endpoint; an empty object is correct until then because nothing reads `config_schema` before Phase 9.
- [ ] **Add `transport_config_inner_as_json` helper in `crates/adapters/src/transport.rs`** (NOT in adapterlib — it depends on `TransportConfig`). Serialises the whole `TransportConfig` to `{"name", "config"}` and returns the `"config"` field; for unit variants (e.g. `HttpOutput`) with no content, return `JsonValue::Null`. This is the bridge between typed `TransportConfig` variants and `BuildInputFn`'s `&JsonValue` parameter, and is reused by Phase 6 / PR 6.
- [ ] **Add registry-dispatch path in both factory functions** (`input_transport_config_to_endpoint` and `output_transport_config_to_endpoint`) BEFORE the existing match. Pattern: resolve secrets first (unchanged), then `connector_by_name(&config.name())` → if `Some(descriptor)` and the appropriate `build_*` is `Some`, call it with the extracted config JSON. Otherwise fall through to the legacy match.
- [ ] **Move `FileInput` / `ClockInput` arms into the `Ok(None)` catch arm of the input match**; **delete the `FileOutput` arm entirely** (the output match's existing `_ => Ok(None)` already covers it). Don't leave them as dead arms — Rust's "unreachable pattern" lint and future exhaustiveness changes will flag them.
- [ ] Remove now-unused imports: `use clock::ClockEndpoint`, `use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint}` from `transport.rs`. The `build_*` functions in the connector modules construct the endpoints directly, so the factory no longer names these types.
- [ ] Verify the existing tests `transport::file::test::{test_csv_file_nofollow, test_csv_file_follow}` and `transport::clock::test::test_clock` pass — they call `mock_input_pipeline`, which exercises the full factory dispatch path (registry hit + endpoint construction).
- [ ] Add a unit test per migrated connector that resolves it via `connector_by_name()` and asserts `direction` / `kind` / `build_*` slots. Tests live alongside the connector module (e.g. `file.rs::test::file_input_descriptor`, `clock.rs::test::clock_descriptor`) and must run in the default build with no external dependencies.
- [ ] Document the migration recipe in a short developer note for use in PR 7+. Include the descriptor-name-vs-serde-tag rule and the `Ok(None)` arm pattern.
- **Depends on**: PR 2.
- **Unblocks**: PR 5, PR 6.

### PR 5 — Capability fields on descriptor + controller refactor (Phase 5)
- [ ] Convert `ConnectorFlags` from the PR 2 plain-`u32` placeholder to `bitflags!`. Add `bitflags = "2"` **directly to `crates/adapterlib/Cargo.toml`** (NOT to the workspace `[dependencies]` table — `bitflags` is not currently a workspace dep; single-crate dep is simpler until a second crate needs it). Keep `EMPTY` as `Self::empty()` const for source compatibility; `contains()` is drop-in compatible. No call-site changes outside the struct definition.
- [ ] Add concrete flag constants with stable bit values: `HTTP_DIRECT = 0x01`, `AUTO_RECREATED_ON_RESTART = 0x02`. Future flags use the next available bits; document the bit layout in source.
- [ ] Wire `default_format` field on descriptor (already declared in PR 2).
- [ ] **Register descriptors for the four connectors the controller-rewrites depend on**: `http_input` (with `HTTP_DIRECT` flag, `Transient` kind, `build_input: Some(build_http_input)`), `http_output` (`Transient` kind, `build_output: None` — server creates `HttpOutputEndpoint` directly at request time, descriptor is metadata-only), `adhoc_input` (`Transient` kind, `build_input: Some(build_adhoc_input)`), `datagen` (`Regular` kind, `default_format: Some(datagen_default_format)`, `build_input: Some(build_datagen_input)`). The other unmigrated connectors (`s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, integrated) do NOT need descriptors yet — none of the four controller rewrites query them. Phase 4b sweeps them later.
- [ ] **`build_*: None` pattern is valid**: `http_output`'s descriptor has `build_output: None` because the HTTP server constructs the endpoint directly with runtime parameters. The output factory's registry path returns `Ok(None)` in this case, matching today's `_ => Ok(None)` fallback. Document this pattern.
- [ ] **Add `inventory = { workspace = true }` to `crates/datagen/Cargo.toml`**. The `register_connector!` macro expands to `::inventory::submit!`, which requires `inventory` as a direct dep of the calling crate — transitive through `feldera-adapterlib` is not enough. Any future per-connector crate must do the same.
- [ ] **Add `extern crate feldera_datagen as _;` to `crates/adapters/src/lib.rs`** (near the top, after the doc comment). This PR is the moment the linkage breaks: it moves the datagen descriptor from `dbsp_adapters` source into `feldera-datagen` (an external crate), AND removes the last `feldera_datagen::*` reference from `transport.rs` (the `use feldera_datagen::GeneratorEndpoint` line). Without an explicit `extern crate`, the linker sees `feldera-datagen` as an unused dep and drops the rlib — `inventory::submit!` never runs, `connector_by_name("datagen")` returns `None`, and every datagen test fails with `Option::unwrap()` on a `None` value at `test.rs:217`. The `extern crate ... as _;` form is the standard "force linkage" idiom in edition 2021+; the `as _` suppresses the unused-name warning. The accompanying comment should explain *why* — the linker would otherwise drop the rlib and the `inventory::submit!` would never run. This line is permanent: PR 10's generated `force_link.rs` covers the describer + per-pipeline workspaces, but `dbsp_adapters` tests build as a separate output and need their own force-link source. Other connectors don't need this treatment because their descriptors live inside `dbsp_adapters` itself; `feldera-datagen` is the only out-of-tree crate whose descriptor and impl ship together.
- [ ] Move the JSON-Datagen format spec from `controller.rs` into `datagen/src/lib.rs::datagen_default_format()`. Remove the now-unused `use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat, JsonLines}` imports from `controller.rs`.
- [ ] Replace `transport.is_transient()` callers in `controller/pipeline_diff.rs:97,106` with descriptor lookups (`connector_by_name(&transport.name()).map(|d| d.kind == ConnectorKind::Transient).unwrap_or(false)`).
- [ ] Replace `transport.is_http_input()` at `controller.rs:4448` with `descriptor.flags.contains(HTTP_DIRECT)`.
- [ ] Replace `match TransportConfig::ClockInput(_)` filter at `controller.rs:4521` with `AUTO_RECREATED_ON_RESTART` check.
- [ ] Replace `match (TransportConfig::Datagen(_), None)` default-format injection at `controller.rs:6013` with `descriptor.default_format()`. The error message changes from `"datagen endpoints do not support custom formats"` to `"{name} endpoints do not support custom formats"`; update any test that asserts on the literal `"datagen"` substring.
- [ ] **Move `HttpInput`/`HttpOutput`/`AdHocInput`/`Datagen` arms to the input factory's `Ok(None)` catch arm** (mirrors PR 4's pattern). After this PR the catch arm contains: `FileInput`, `ClockInput`, `HttpInput`, `AdHocInput`, `Datagen`, plus all output-only variants. PR 7a–7g shrinks it.
- [ ] Remove now-unused `use adhoc::AdHocInputEndpoint`, `use http::HttpInputEndpoint`, `use feldera_datagen::GeneratorEndpoint` imports from `transport.rs`.
- [ ] Document the **registry-completeness invariant**: every transient connector and every connector whose flags the controller queries MUST register a descriptor. Lookups silently degrade to `false`/`None` for unregistered connectors. Removing `TransportConfig::is_transient()` is gated on verifying all four transient connectors are registered (done in this PR).
- [ ] Add CI lint that fails if any `match TransportConfig::*` arm appears in `controller.rs` outside the four dispatch sites.
- [ ] Add `connector_by_name` descriptor tests for the four connectors registered here (`http_input`, `http_output`, `adhoc_input`, `datagen`), alongside their modules, in the default build. Same pattern as PR 4's tests for `file`/`clock`.
- [ ] Verify `connector_flags_contains` test, file/clock transport tests (3), and format tests (87) still pass.
- **Depends on**: PR 4.
- **Unblocks**: PR 6.

### PR 6 — Add registry dispatch to the integrated factories (Phase 6)
- [ ] Add registry-dispatch block (`connector_by_name` → `build_integrated_input`) at the top of `create_integrated_input_endpoint` (`integrated.rs:89`); existing match arms remain as the fallback.
- [ ] Same for `create_integrated_output_endpoint` (`integrated.rs:40`) — registry dispatch calls `build_integrated_output` with `Arc<dyn OutputControllerRef>`. The transport factories already received this pattern in PR 4 — do not re-touch them.
- [ ] **Add `impl feldera_adapterlib::connector::OutputControllerRef for ControllerInner`** in `controller.rs`. Required at compile time by the integrated output registry dispatch (the `Arc<ControllerInner>` → `Arc<dyn OutputControllerRef>` coercion), even though the dispatch path is dead code in this PR. Methods delegate to existing inherent methods (`output_transport_error`, `update_output_connector_health`) and `self.status.*` methods (`register_batch_progress_counter`, `output_buffer`). For the two name-colliding methods, write `ControllerInner::output_transport_error(self, …)` (UFCS) rather than `self.output_transport_error(…)` to avoid the trait method shadowing the inherent one.
- [ ] **Promote `transport_config_inner_as_json` to `pub(crate) fn`** in `crates/adapters/src/transport.rs` so `integrated.rs` can call it. Do not promote to `pub` — keep crate-private.
- [ ] In the integrated output factory, `controller.upgrade()` failure maps to `ControllerError::invalid_transport_configuration` with a "controller dropped" message. Acceptable placeholder; the path is dead code until PR 7f.
- [ ] The format-check (`if connector_config.format.is_some()`) appears once inside the registry-dispatch block (before calling `build_fn`) and once after the fallback match — duplication is by design (registry path returns early). Document inline.
- [ ] Dispatch logic checks `descriptor.build_input.is_some()` / `descriptor.build_output.is_some()` per call site, not `direction`. `direction` is consumed by pipeline-manager (PR 11 direction-validation check via the connectors manifest) and the connector-listing API; the build factories don't read it.
- [ ] Verify integrated factories pass `Arc<dyn OutputControllerRef>` (not `Weak`) to `BuildIntegratedOutputFn` — connector's `new()` immediately downgrades for storage. (Trait + signature defined in PR 2; impl now lands here.)
- [ ] Verify the registry dispatch block compiles cleanly even though it's dead code (no integrated descriptors registered until PR 7f). Rust does not flag it as unreachable because `connector_by_name` could return `Some` for any name.
- [ ] Verify `cargo build -p dbsp_adapters` succeeds and existing test suites (transport, format, adhoc) still pass. After this PR: registered set unchanged from PR 5 (`{file_input, file_output, clock, http_input, http_output, adhoc_input, datagen}`); the integrated registry path is wired but unreachable.
- **Depends on**: PR 5.
- **Unblocks**: PR 7a–7g.

### PR 7a–7g — Sweep remaining bundled connectors onto the registry, then clean up (Phase 4b)
PRs 7a–7f follow the per-connector migration recipe in Phase 4 (steps 1–8): add `register_connector!` (or a shim in `adapters/src/integrated/` for connectors in separate crates), delete the fallback match arm (and the `#[cfg(not(feature))]` stub for feature-gated connectors); when removing the last constructing arm, collapse the match shell in the same PR rather than carrying an `#[allow(unreachable_code)]`; delete now-dead wrapper-module re-exports; for integrated-output connectors, change `new()` to take `Arc<dyn OutputControllerRef>` and replace dangling-`Weak` test stubs with a `NoOpControllerRef`; for feature-gated sub-connectors inside an always-enabled module, wrap the descriptor in a feature-gated `mod`; add a default-build `connector_by_name` descriptor test alongside the connector module; verify existing tests pass. PR 7g is the post-migration cleanup. Group as small PRs to keep blast radius low. **Note**: `http`, `adhoc`, and `datagen` were already migrated in PR 5 as prerequisites for the controller-rewrites; this sweep covers only the connectors that PR 5 didn't need to touch.
- [ ] **PR 7a**: `s3` + `url`.
- [ ] **PR 7b**: `nats` + `pubsub` (each behind its `with-*` feature gate). Delete both the `#[cfg(feature)]` arm and the `#[cfg(not(feature))]` stub for each; delete dead `pub use input::{NatsInputEndpoint,PubSubInputEndpoint}` re-exports in `nats.rs` / `pubsub.rs`. Verify the descriptor `name` matches `TransportConfig::name()` exactly — `pub_sub_input` (three-word split), not `pubsub_input`.
- [ ] **PR 7c**: `redis` (output only); first regular connector with `Direction::Output`. Behind `with-redis`; same feature-gate-arm cleanup as PR 7b. No wrapper re-export to delete (`redis.rs` only declares `pub mod output;`). `build_output` binds `_fault_tolerant` (redis ignores it; `is_fault_tolerant()` is unconditionally `false`).
- [ ] **PR 7d**: `kafka` — behind `with-kafka`; same feature-gate-arm cleanup as PR 7b. Register **two** descriptors: `KAFKA_INPUT_DESCRIPTOR` (name=`"kafka_input"`, `Direction::Input`) and `KAFKA_OUTPUT_DESCRIPTOR` (name=`"kafka_output"`, `Direction::Output`) — registry lookup is keyed on `TransportConfig::name()`, which differs between `KafkaInput` and `KafkaOutput`. `build_kafka_output` receives `fault_tolerant: bool` and branches between `KafkaFtOutputEndpoint` and `KafkaOutputEndpoint`. The existing `pub use ft::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint}` and `pub use nonft::KafkaOutputEndpoint` re-exports in `kafka.rs` stay alive because `build_kafka_input` / `build_kafka_output` consume them in-module — only the corresponding `use` lines in `transport.rs` are deleted. `build_*` signatures bind `_secrets_dir` (the factory resolves secret references via `resolve_secret_references_via_json` before calling `build_*`; the build function never sees credential file paths). After this PR lands, the output factory's fallback match contains only `_ => Ok(None)` (redis migrated in 7c, kafka here) — leave the `match` in place; PR 7g strips it.
- [ ] **PR 7e**: `nexmark` — behind `with-nexmark`. `ConnectorKind::Regular`, **not** `Transient`: nexmark implements `fault_tolerance() → Some(FtModel::ExactlyOnce)` and participates in checkpoint/replay; `Transient` is reserved for connectors not re-created on restart (`http_input`, `http_output`, `clock`, `adhoc_input`). The descriptor lives inside the `#[cfg(feature = "with-nexmark")]` module (single `nexmark.rs` file, no wrapper) so the `inventory::submit!` only fires when the feature is on; with the feature off, `connector_by_name("nexmark")` returns `None` and the catch arm handles it. No wrapper re-exports to delete (`mod nexmark;` had none); only the `use crate::transport::nexmark::NexmarkEndpoint` line in `transport.rs` is removed. After this PR lands, the input factory's fallback match contains only `_ => Ok(None)` — collapse it to that form in this PR (do not leave it as `let endpoint: Box<dyn TransportInputEndpoint> = match config { _ => Ok(None) }; Ok(Some(endpoint))` with `#[allow(unreachable_code)]`); PR 7g's input-fallback-strip work is pre-empted.
- [ ] **PR 7f**: integrated — `postgres` (reader, writer), `postgres-cdc`, `delta_table`, `iceberg`. Each behind its `with-*` feature gate; same feature-gate cleanup. Wires `IntegratedOutputEndpoint`/`IntegratedInputEndpoint` builds; this is the PR that finally **activates** the integrated registry dispatch path that PR 6 wired up. **Output constructors must change signature**: `DeltaTableWriter::new()` and `PostgresOutputEndpoint::new()` accept `Arc<dyn OutputControllerRef>` (not `Arc<ControllerInner>`) and call `Arc::downgrade(&controller)` internally — there is no Any-based downcast path from `Arc<dyn OutputControllerRef>` back to `Arc<ControllerInner>`. The stored field types change from `Weak<ControllerInner>` to `Weak<dyn OutputControllerRef>` (`DeltaTableWriterInner.controller`, `PostgresWorker.controller`, `PostgresOutputEndpoint.controller`). Direct field accesses like `controller.status.register_batch_progress_counter(…)` and `controller.status.output_buffer(…)` become trait calls (`controller.register_batch_progress_counter(…)`, `controller.output_buffer(…)`) — the `OutputControllerRef` impl on `ControllerInner` (PR 6) delegates through to `self.status.*`. After this change, `ControllerInner` is no longer imported by `delta_table/output.rs` or `postgres/output.rs`. **Test stubs**: existing tests use `Weak::<ControllerInner>::new()` (a dangling weak) as a null controller. Replace with a `NoOpControllerRef` stub struct in each test module implementing the four `OutputControllerRef` methods as no-ops, and pass `Arc::new(NoOpControllerRef)` to constructors. In `postgres/output.rs` the nested `tests::parallel` submodule needs `use super::NoOpControllerRef;` (delta_table has only one flat test module). **Bench call-sites — bench-safety contract**: PR 7f changes a public constructor signature, so the migration must keep `cargo build --all-features --benches` green at the PR's commit boundary. Criterion benches at `crates/adapters/benches/{postgres_output.rs,delta_encoder.rs}` construct these endpoints directly and live outside the `#[cfg(test)]` gate; `cargo check --all-features` does not compile bench targets, so the PR's CI gate runs `cargo build --all-features --benches`. As part of this PR: place a single `NoOpControllerRef` in `benches/bench_common.rs` with `#[allow(dead_code)]` (the `avro_encoder` bench `mod`-includes `bench_common` without referencing the stub, which would otherwise warn) and rewrite both bench `::new(...)` call-sites to pass `Arc::new(NoOpControllerRef)`. **iceberg shim**: do NOT add `inventory` as a dependency to `feldera-iceberg` (separate crate, unwanted coupling). Instead, place the descriptor in a new `crates/adapters/src/integrated/iceberg.rs` shim gated by `#[cfg(feature = "with-iceberg")]` whose `build_*` calls `feldera_iceberg::IcebergInputEndpoint::new(...)`. **postgres-cdc gating**: wrap the CDC descriptor in `#[cfg(feature = "with-postgres-cdc")] mod postgres_cdc_descriptor { … }` containing the config type import, build function, static descriptor, and `inventory::submit!` together — putting `#[cfg]` on just the `inventory::submit!` leaves `PostgresCdcReaderConfig` references reachable when the feature is off. **Fallback collapse**: after migrating all 6 connectors, the input fallback in `integrated.rs` had 4 constructing arms and the output fallback had 2; both collapse to a direct `Err(ControllerError::unknown_*_transport(...))` return in this PR (the trailing `if connector_config.format.is_some()` checks go with them — registry path already enforces it). PR 7g's integrated-factory cleanup is pre-empted, mirroring the 7c/7e pattern for the transport factories.
- [ ] **PR 7g — final cleanup** (one PR, two commits): with all bundled connectors registry-driven after 7f, strip the post-migration leftovers. The two commits share the "PR 7 swept the connectors, now sweep the scaffolding" theme; splitting into separate top-level PRs would be needless review overhead. **Bench-safety invariant** (every commit boundary, including each commit within 7g): `cargo build --all-features --benches` succeeds. PR 7g leaves bench-driven exposure (`pub use crate::integrated::postgres::PostgresOutputEndpoint`, `pub mod delta_table`) untouched — those go in PR 7h, which first migrates benches off concrete types.
  - **Commit 1 — strip dead match arms**: most of this work has already happened during PRs 7d (output collapsed after kafka), 7e (input collapsed after nexmark), and 7f (both integrated factories collapsed to direct `Err` returns). This commit deletes the now-empty `match` shells in `input_transport_config_to_endpoint` and `output_transport_config_to_endpoint` (returning `Ok(None)` directly). Also remove `#[allow(unused_variables)]` from `input_transport_config_to_endpoint` (still present in `transport.rs` after PR 7f) — with the match gone, every parameter is either consumed by the registry path or by the `Ok(None)` return. Drop any unused `Encoder` / `OutputEndpoint` imports from `integrated.rs` while you're there. (PR 2 already chose the clean re-export topology — `lib.rs` imports `IntegratedOutputEndpoint` directly from `feldera_adapterlib::transport` and `integrated.rs` keeps it as a private `use` — so there is no chain to collapse here.)
  - **Commit 2 — `#[doc(hidden)]` removal**: with integrated connectors now reachable from out-of-tree code via the descriptor registry, remove `#[doc(hidden)]` from `IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle` (struct), `InputCollectionHandle::new()` (the constructor — independently annotated, would otherwise be inconsistent with a now-public struct), and `OutputConsumer`. These were kept hidden in PR 1 because surfacing them without their callers being plugin-reachable would have been confusing. They become first-class plugin ABI here. **Doc-comment requirement**: each newly-public type needs a real doc comment (one short sentence summarising its role) — `#[doc(hidden)]` was the only thing previously suppressing the missing-doc lint for these items, so naked attribute removal will fail the lint. **`lib.rs` ABI overview text edits**: `crates/adapterlib/src/lib.rs`'s module-level doc comment mentions `(\`#[doc(hidden)]\`)` annotations on four lines (the introductory paragraph explaining the carve-out, plus three per-type bullets in the "Supported plugin-facing types" lists). Rewrite the introductory paragraph (the carve-out is no longer relevant) and drop the inline annotations on the bullets. Inline comments like `"#[doc(hidden)] is a temporary marker — it will be removed in Phase 4b…"` (currently on `IntegratedOutputEndpoint`) go alongside the attribute they referenced.
- [ ] **PR 7h — bench migration + concrete-type removal** (lands after 7g, two commits):
  - **Commit 1 — migrate benches to registry dispatch**: rewrite `benches/postgres_output.rs` and `benches/delta_encoder.rs` to construct endpoints via `connector_by_name("postgres_output" | "delta_table_output").build_integrated_output.unwrap()(...)` and `Arc::new(NoOpControllerRef)`. The config argument is the **inner** config JSON value, *not* a `TransportConfig` envelope: pass `serde_json::to_value(&config)?` where `config` is the raw `PostgresWriterConfig` / `DeltaTableWriterConfig` — the descriptor's `build_*` deserialises via `serde_json::from_value::<InnerConfig>(...)`. (The factory's `transport_config_inner_as_json` helper is `pub(crate)`, so external callers serialise the inner type directly.) The returned `Box<dyn IntegratedOutputEndpoint>` reaches its supertrait methods through the vtable: `endpoint.consumer().batch_start(...)`, `endpoint.encode(...)`, `endpoint.consumer().batch_end()` continue to work — but **drop the `use dbsp_adapters::Encoder` (and any `OutputEndpoint`) supertrait imports**, since trait-object dispatch reaches supertrait methods without the trait being in scope, and Rust's `unused_imports` lint will flag them. Also drop `use dbsp_adapters::integrated::{PostgresOutputEndpoint, delta_table::DeltaTableWriter}`. Helper signatures change from `&mut PostgresOutputEndpoint` to `&mut dyn IntegratedOutputEndpoint`; **call sites need an explicit reborrow**: `bench_encode_iter(&mut *endpoint, ...)` — `&mut Box<dyn T>` does not coerce to `&mut dyn T` in function arguments (only in method-call autoderef). Optional consistency call: route the remaining `dbsp_adapters::SerBatch` import through `feldera_adapterlib::catalog::SerBatch` so all bench imports flow through `feldera_adapterlib`; either path works.
  - **Commit 2 — remove now-dead concrete-type exposure**: before editing, audit each removal target with `rg` outside the relevant module subtree to confirm the bench was the sole external consumer (both `pub use crate::integrated::postgres::PostgresOutputEndpoint` and the `pub` qualifier on `mod delta_table` are bench-driven exposures predating the registry refactor — added by the original postgres-bench and parallel-delta-encoder commits respectively, so the audit should return zero non-bench non-internal hits). Then delete `pub use crate::integrated::postgres::PostgresOutputEndpoint;` from `integrated.rs` and change `pub mod delta_table;` to `mod delta_table;`. Internal callers under `crates/adapters/src/integrated/delta_table/` reach helpers via `crate::integrated::delta_table::*` paths, which work identically through a private module — no further changes needed inside the subtree. Re-run `cargo build --all-features --benches` to confirm. **Follow-up out of scope for 7h**: with `delta_table` now private, the `pub` qualifiers on `register_storage_handlers` and `delta_input_serde_config` (defined at the `delta_table.rs` module root) become orphaned — they're only used by sibling files within the now-private module. A future cleanup PR can downgrade to `pub(super)` or drop `pub` entirely.
  - **Why split from 7g**: 7g is *cleanup* (strip dead match shells, surface previously-hidden types). 7h is *behavior-equivalent rewrite* of two test harnesses — a different review profile (correctness of inner-config-JSON synthesis, equivalence of trait-call vs concrete-call paths, coercion subtleties around `&mut Box<dyn T>`). Folding them would conflate the two review profiles. The two commits in 7h must stay in order: commit 2's removals are only safe after commit 1's migration.
- **Depends on**: PR 6 (PRs 7a–7f are independent of each other; PR 7g depends on 7f having landed).
- **Unblocks**: PR 8.

### PR 8 — Open the `TransportConfig` enum (Phase 7)
- [ ] Add `Plugin(PluginTransportConfig)` variant in `crates/feldera-types/src/config.rs:1609`.
- [ ] **Delete the `#[serde(tag = "name", content = "config", rename_all = "snake_case")]` attribute** from the enum. `#[serde(...)]` is a derive helper attribute injected by the serde proc-macro; it cannot be retained alongside manual `Serialize`/`Deserialize` impls (the compiler errors with `cannot find attribute 'serde' in this scope`). Implement **both `Serialize` and `Deserialize` manually** — the derive cannot be partially retained.
- [ ] **`HttpOutput` is a unit variant**: its serialization must emit `{"name":"http_output"}` with **no `"config"` key** (use `serialize_map(Some(1))`). Every other variant uses `Some(2)` and emits `{"name":..., "config":...}`. The `Plugin` arm forwards the stored `name`/`config` directly without re-nesting. Add an explicit test asserting `HttpOutput`'s serialized form contains no `config` field — silently emitting `"config": null` would silently break stored pipeline configs from before this PR.
- [ ] Manual `Deserialize`: parse a `{name, config}` envelope, then dispatch on `name` to `serde_json::from_value::<TypedConfig>(config)` for known names; route unknown names to `Plugin { name, config }`. The intermediate `serde_json::Value` allocation is acceptable (`TransportConfig` deserializes once at pipeline startup, not on hot paths).
- [ ] **`ToSchema` schema regression (known limitation)**: removing `#[serde(tag, content)]` strips the adjacently-tagged hint that `utoipa::ToSchema` consumed for OpenAPI generation. The generated schema for `TransportConfig` will be less precise (likely `oneOf` of inline variant structs). Accept as placeholder; PR 14's `GET /v0/connectors/status` `descriptors` field is the designated owner of the connector schema surface and will replace this.
- [ ] Update `name()` (`config.rs:1638-1662`) to handle the `Plugin` arm (returns `p.name.clone()`).
- [ ] Remove `is_transient()` and `is_http_input()` helpers (already replaced by descriptor lookups in PR 5).
- [ ] **Audit every exhaustive `match TransportConfig` in the workspace** (`rg 'match.*TransportConfig'`). Most matches use `_` wildcards, `if let`, or `matches!` and require **no code change** — Rust's exhaustiveness check only fires on `match` arms without a catch-all. The only file that needs explicit `Plugin` arms is `crates/pipeline-manager/src/db/types/program.rs` at the input/output validation matches (lines 682, 735): both already have `_ =>` arms, but those return the misleading `ExpectedInputConnector`/`ExpectedOutputConnector` errors. Add explicit `TransportConfig::Plugin(p) => Err(ConnectorGenerationError::UnknownConnector { ..., name: p.name.clone() })` arms ahead of the `_` arm at each site.
- [ ] **Add `UnknownConnector` to `ConnectorGenerationError`** (in `program.rs`). Update `new_from_connector_generation_error` (`program.rs:127`) — that function exhaustively matches the error enum without a `_` arm, so the compiler forces the new branch.
- [ ] Tests: serde round-trip for every known variant (byte-for-byte unchanged); `HttpOutput` serialized form contains no `config` field; unknown name routes to `Plugin`; a `Plugin` config submitted at the SQL boundary surfaces `UnknownConnector` (not a panic) until PR 11 lands. The `program.rs` test for `UnknownConnector` constructs the error variant with plain strings and **does not** need `use feldera_types::config::PluginTransportConfig;` — leaving such an import will trip `unused_imports` and force a follow-up cleanup PR.
- [ ] Verify a stored pipeline configuration from before this PR deserializes identically (include `HttpOutput` explicitly in the fixture set — it's the unit-variant edge case).
- [ ] **What this PR enables**: `TransportConfig::Plugin` configs round-trip through serde without a JSON parse error on unknown names. **What it does not**: pipeline SQL referencing a plugin connector still fails at SQL compilation with `UnknownConnector` until PR 11 wires manifest-based direction validation. Bundled in-tree connectors are unaffected — they hit their existing typed match arms and remain fully operational; only plugin connectors transition from rejected → accepted at PR 11.
- **Depends on**: PR 7g (all bundled connectors registry-driven AND cleanup landed; the `Plugin` variant is purely an extension point at this point — no risk of dead match arms colliding with the new `Plugin` arm being added).
- **Unblocks**: PR 9.

### PR 9 — `tenant_connector_config` schema + bootstrap seed (Phase 8.1)
- [ ] **Document the `connectors.toml` blob format** (concrete example in 8.1): one Cargo dep per line, **no section header**, mirroring the `udf_toml` shape at `rust_compiler.rs:1336-1346`. Each line is `<key> = <cargo-dep-spec>`. Multi-line `[dependencies.<name>]` table form is **not** supported — line-based force-link extraction in PR 10 assumes one dep per line.
- [ ] **DB migration**: add `tenant_connector_config(tenant_id, content, content_hash, version, edited_at, edited_by)` per the schema sketch in 8.1. `content_hash = sha256(content)` is recomputed on every write. `version` is monotonic. `tenant_id` is the primary key (one row per tenant). Migration creates the empty table — rows are inserted lazily on first read or on tenant creation (see bootstrap below). **Footgun**: declare timestamp columns as `TIMESTAMPTZ` (not `TIMESTAMP`) — `chrono::DateTime<Utc>` requires timezone-aware columns; a plain `TIMESTAMP` binds silently during migration but fails at runtime when sqlx tries to decode the value.
- [ ] **Add a single `connectors_toml_path: Option<String>` field to `CompilerConfig`** — bootstrap seed only. Drop the previously-planned `connectors_d_dir` field; per-tenant variants live in the DB now, not on the filesystem.
- [ ] **Update every `CompilerConfig { ... }` struct-literal site**. `CompilerConfig` has no `Default` impl; the compiler enforces this at the same five call sites as before — `compiler/test.rs` (×1), `compiler/main.rs` (×3), `compiler/rust_compiler.rs` (×1; the sibling site uses `..config`). Set the field to `None` in tests.
- [ ] **Place the bootstrap loader at `crates/pipeline-manager/src/compiler/connectors.rs`** with `pub mod connectors;` declared in `compiler.rs`. The loader is `fn load_bootstrap_seed(config: &CompilerConfig) -> io::Result<Option<String>>` — returns `Ok(None)` when `connectors_toml_path` is unset; otherwise reads the file with synchronous `std::fs::read_to_string` (15+ unit tests stay as plain `#[test]`).
- [ ] **Bootstrap rule**: on tenant creation (and as a one-shot migration for existing tenants when the field is first set), if no `tenant_connector_config` row exists for the tenant *and* `connectors_toml_path` is set, insert a row with `content` = file contents, `version = 1`. After that, the file is **never re-read** for that tenant — DB is authoritative. If the field is unset, insert a row with empty `content`. Bootstrap is idempotent (uses `INSERT … ON CONFLICT DO NOTHING`).
- [ ] **Wrap reads in a `ConnectorsTomlContent` newtype** with `as_str(&self) -> &str` and `is_empty(&self) -> bool` helpers. PR 10 uses `is_empty()` to decide whether to splice a deps section into `Cargo.toml` and `as_str()` for the splice itself.
- [ ] **DB-access functions**: `tenant_connector_config_get(tenant_id) -> Option<Row>`, `tenant_connector_config_put(tenant_id, content, edited_by, expected_hash) -> Result<Row, OptimisticConcurrencyError>` (`expected_hash` mismatch maps to a 412 in PR 10). Empty `content` is valid (bundled-only).
- [ ] Tests: bootstrap inserts a row from the seed file on first tenant access; second access returns the DB row even after the file changes; tenants without bootstrap field get an empty-content row; PUT with stale `expected_hash` returns the concurrency error; arbitrary Cargo spec shapes (plain version, git+rev inline table, path inline table, features array) round-trip verbatim through DB storage.
- **Depends on**: PR 8.
- **Unblocks**: PR 10.

### PR 10 — Describer binary + DB-backed config endpoints (Phase 8.2-8.4, 8.12)
- [ ] Generate the describer crate (`crates/feldera-describer/`) on first use per tenant; its `Cargo.toml` lists `feldera-adapterlib` + `dbsp_adapters` + every entry from the tenant's `tenant_connector_config.content`.
- [ ] **Generate `force_link.rs` alongside `main.rs`** in the describer workspace, with two sections in fixed order: a hardcoded built-in section (currently `extern crate feldera_datagen as _;`) and a user-listed section iterating over the tenant's blob. `main.rs` declares `mod force_link;` so the lines are part of the compilation unit. The built-in list is a `const &[&str]` in pipeline-manager's codegen — not driven by config, not user-editable, not visible in any error message that says "from `connectors.toml`". See Phase 8.3.
- [ ] **Generate the same `force_link.rs` into every per-pipeline workspace** (extends `prepare_workspace` at `rust_compiler.rs:1242`). Identical two-section layout. **Place it inside the existing globals crate** (`<pipeline>_globals`) — its `lib.rs` declares `mod force_link;`. Add `("force_link.rs", true)` to the `src_content` list at `rust_compiler.rs:1435-1448` (the `_globals` arm) so `DirectoryContent::validate()` does not reject the injected file.
- [ ] **The hand-written `extern crate feldera_datagen as _;` in `crates/adapters/src/lib.rs`** (added in PR 5) stays — it covers `dbsp_adapters`'s own lib/integration tests, which build as a separate output and aren't reached by any codegen path. The describer and per-pipeline workspaces get their own generated `force_link.rs`; the three sources are parallel, not duplicate.
- [ ] `prepare_describer_workspace` analogous to existing `prepare_workspace`. Reads the tenant's blob from `tenant_connector_config` rather than the filesystem.
- [ ] **Extend `CompilerConfig::canonicalize()`** to absolute-resolve `connectors_toml_path` (the bootstrap seed field added in PR 9). The field is optional — add a sibling helper `help_canonicalize_path_if_exists` that returns `Ok(None)` when the path is unset and skips canonicalization when the path is set but does not yet exist on disk. Reusing the existing `help_canonicalize_path` would error on absent optional fields and break deployments that ship no seed file.
- [ ] Run the describer and capture JSON; persist as `manifest.json` next to `describer.lock` at `<working_dir>/describer/<tenant_id>/<cache_key>/` (where `cache_key` = `describer_cache_key` = `sha256(content || ADAPTERLIB_API_VERSION)`, distinct from `tenant_connector_config.content_hash`).

- [ ] **`ManifestCache`** (`compiler/manifest_cache.rs`): in-process state machine, one entry per tenant, transitions `NotConfigured → Building → Ready | Failed`. `try_begin_build` is idempotent for in-flight builds; `force = true` bypasses the `Ready` check (used by refresh). Staleness guards on `mark_ready` / `mark_failed`: silently drop a completion if the tenant's current hash has already moved on to a newer build. **Create the `Arc<ManifestCache>` in `pipeline-manager.rs` before spawning either the compiler task or the API server, then pass it to both.** `ServerState::new` accepts `manifest_cache: Arc<ManifestCache>` as a parameter rather than constructing one internally — the SQL compiler (a separate tokio task) reads the same cache to gate program advancement, so it must share the same instance. Background `tokio::spawn` tasks call `mark_ready` / `mark_failed` on completion.

**Endpoints (Phase 8.12):**
- [ ] **`GET /v0/connectors/status`** returning the unified envelope (`state`, `content_hash`, `version`, `last_built_at`, `error`, `descriptors`). `ETag: "<content_hash>"`; honors `If-None-Match` with 304. Tenant-scoped via existing auth.
- [ ] **`GET /v0/connectors/connectors.toml`** returning the raw blob with `ETag`, `X-Connectors-Version`, `X-Connectors-Edited-At`, `X-Connectors-Edited-By`. Content-Type `text/plain; charset=utf-8`.
- [ ] **`PUT /v0/connectors/connectors.toml`** — body is the new toml text, `If-Match: "<content_hash>"` required. On match: write the row (recomputing `content_hash`, incrementing `version`, recording `edited_by` from auth context), enqueue a background describer rebuild, return `202 Accepted` with `Location: /v0/connectors/status` and the new hash/version. On mismatch: `412 Precondition Failed`. Body parse-shape validation rejects malformed-line bodies pre-write; deeper errors surface via `state: "failed"` on `/status`. **Footgun**: call `get_or_bootstrap` (the lazy-insert helper) at the start of the PUT handler, before the CAS UPDATE. Without it, a fresh tenant that PUTs without a prior GET has no row; the UPDATE matches zero rows, the error-disambiguation query finds no row either, and the handler returns `UnknownTenant` (401) instead of `OutdatedHash` (412).
- [ ] **`POST /v0/connectors/refresh`** admin endpoint runs `cargo update` against the tenant's describer workspace and rebuilds — useful for picking up upstream patch releases without a config edit.
- [ ] **AuthZ**: all four endpoints use the existing tenant-scoped auth — no role distinction in the initial release (Phase 8.12). The audit trail (`edited_by` / `edited_at` columns) is the review surface; role gating can be added later without breaking the API shape.
- [ ] **Define `ApiError` variants** with semantic names: `ConnectorsConfigConflict` (→ 412, ETag mismatch on PUT), `ConnectorsConfigInvalidShape` (→ 400, body fails line-shape check), `ConnectorManifestBuildFailed { error: String }` (→ 500, wraps describer build failures surfaced through status). `ApiError` does not have generic `NotFound`/`InternalServerError` variants, so name them explicitly. Wire each into `error_code`, `Display`, and `ResponseError::status_code`.
- [ ] **Plumb `compiler_config: Option<CompilerConfig>` through `ServerState::new`** — there are two call sites: `api/main.rs` and a `#[cfg(test)]` site in `auth.rs`.
- [ ] Detect duplicate connector names **in the describer's startup** (walk `inventory`, error if any name appears twice). Failure message names both source crates from the tenant's blob. Do not push this check into adapterlib — `connector_by_name()` is intentionally simple and the describer has the deployment context to render the diagnostic.
- [ ] **Cache key + lockfile discipline** (Phase 8.4 / 8.7): `describer_cache_key` hashes `connectors.toml` content + `ADAPTERLIB_API_VERSION` — **not** the full Feldera release version. A Feldera upgrade that does not change the plugin ABI lands in the same cache directory and skips the rebuild. Implement the three-tier warm-path discipline in `build_and_run_describer`: cold path (no lock yet) builds without `--locked`; full warm path (`describer.lock` + `describer.build_version` = current `CARGO_PKG_VERSION`) builds with `--locked`; partial warm path (lock exists but Feldera version changed within the same ABI) copies the lock as a Cargo resolver seed and builds without `--locked` so only Feldera path-dep versions update while external connector pins are preserved and sccache serves previously compiled rlibs. Refresh both sidecar files after every successful build.
- [ ] **API version stamping and validation** (Phase 8.7): stamp `adapterlib_api_version: u32` into each `ConnectorManifestEntry` and call `check_manifest_api_versions` in `build_and_run_describer` after the binary exits. The check is *not* the primary guard against incompatible connectors (Cargo handles that at compile time) — it guards the *manifest file* against stale-cache scenarios: a `manifest.json` from a previous ABI generation that somehow ends up in the current cache directory is rejected before it can poison SQL direction validation. See Phase 8.7 for the full two-level framing. Tests: matching version accepted; future version rejected with connector name and both version numbers in the error message; empty manifest accepted; malformed JSON returns `Parse` error; cache key stable for identical content, distinct for different content.
- [ ] Tests: cache hit, cache miss (cold-path bootstrap without `--locked`), refresh, name collision, version mismatch; `GET /status` response shape across all four states; `PUT` with stale ETag returns 412; `PUT` with malformed body returns 400; `PUT` increments `version` and triggers rebuild observable via `/status` going `ready → building → ready`; tenant isolation (tenant A's PUT does not affect tenant B's `/status`). **Footgun**: actix test helpers that spin up embedded Postgres must return a struct that owns the `TempDir`, not a bare tuple. If `TempDir` is a local in the helper function, it drops when the function returns and deletes the data directory before any test request fires. The `app` field must be `impl Service<…>` — use a named struct (e.g. `struct TestApp<S> { app: S, _state: Arc<…>, _tmp: TempDir }`) so the borrow checker keeps the directory alive for the test's lifetime.
- **Depends on**: PR 9.
- **Unblocks**: PR 11, PR 12.

### PR 11 — Wire descriptor manifest into direction validation (Phase 8 cont.)
- [ ] Replace exhaustive matches at `pipeline-manager/src/db/types/program.rs:682,735` with manifest lookup + `direction.allows_input()` / `allows_output()` checks.
- [ ] Error messages name the unknown connector and suggest checking `connectors.toml`.
- [ ] **Thread the manifest through the call chain**: `attempt_end_to_end_sql_compilation` → `perform_sql_compilation` → `generate_program_info`. The manifest is a new parameter at each level; `generate_program_info` is where the `program.rs:682,735` matches live. **Two call sites for `perform_sql_compilation`**: the main compiler loop path and the precompile path at `compiler/main.rs:592` — the precompile path passes `build_in_process_manifest()` explicitly (it runs without a database-backed manifest; bundled-only descriptors suffice for precompile validation).
- [ ] **Manifest-missing path keeps the program in `Pending` rather than transitioning it to a failure state** (see Phase 8.11). **Check order**: the compiler loop first dequeues the oldest `Pending` pipeline via `get_next_sql_compilation` (the tenant ID is unknown until then), then looks up the manifest for *that specific tenant*, then returns `Ok(false)` if the manifest is not yet ready — leaving the pipeline in `Pending` and sleeping the full `POLL_INTERVAL` (250 ms). Returning `Ok(false)` is correct here: the "no work found" sleep applies equally when a manifest is temporarily unavailable. Describer build failures are surfaced via the existing `ConnectorManifestBuildFailed` / `CompilerConfigNotAvailable` `ApiError` variants from PR 10.
- [ ] **Validation failures map to `SqlError`, not `SystemError`** — unknown connector and direction mismatch are SQL-level semantic errors per Phase 8.11.
- [ ] **Refresh uses snapshot semantics**: in-flight `CompilingSql` jobs keep the manifest snapshot they loaded at job start; new `Pending → CompilingSql` transitions after `POST /v0/connectors/refresh` pick up the new manifest. No mid-compile cancellation.
- [ ] **Force-link `feldera-datagen` for tests that call `build_in_process_manifest()`**: the `pipeline-manager` test binary does not transitively link `feldera-datagen` (only the production binary does, via `dbsp_adapters`). Without an explicit `extern crate feldera_datagen as _` in the test module that calls `build_in_process_manifest()`, the datagen descriptor is absent from the test inventory and tests expecting it present fail silently with an empty manifest. Fix: add `feldera-datagen = { workspace = true }` to `pipeline-manager`'s `[dev-dependencies]` and `extern crate feldera_datagen as _` at the top of the affected test file.
- [ ] Tests: known connector validates, unknown connector fails with a useful message, direction mismatch fails, program stuck in `Pending` while describer is still building, program advances once manifest is available, refresh during in-flight compile uses old snapshot, unknown plugin connector → `SqlError` (not `SystemError`).
- [ ] Verify with empty `connectors.toml` that behavior matches today's bundled-only deployments bit-for-bit (no describer invocation, no `Pending` stall).
- **Depends on**: PR 10.
- **Unblocks**: PR 13.

### PR 12 — Build-cache discipline: shared lockfile + linker (Phase 8.5)
- [ ] **Add `tenant_id: TenantId` to `prepare_workspace`'s signature** — `describer_workspace_dir(config, tenant_id, cache_key)` requires the tenant ID to locate the per-tenant describer workspace at `<working_dir>/describer/<tenant_id>/<cache_key>/`; `prepare_workspace` did not previously receive it.
- [ ] **`prepare_workspace` returns `bool` (`is_full_warm`)** rather than `()`: `true` when `describer.lock` was successfully copied AND its `describer.build_version` matches the current Feldera `CARGO_PKG_VERSION` (full-warm path); `false` on the cold or partial-warm path. **`call_compiler` accepts a matching `use_locked: bool` parameter** and passes `--locked` to `cargo build` only when `use_locked` is `true`. These two items are causally coupled — treat them as one atomic change, not two independent checklist items.
- [ ] **`--locked` is gated on `is_full_warm`**: cold path (no `describer.lock` yet) builds without `--locked`; partial-warm path (`describer.lock` exists but its `describer.build_version` differs from the current Feldera version — e.g. after an upgrade) copies the lock as a Cargo resolver seed and builds without `--locked` so external-connector pins are preserved while Feldera path-dep versions update. `--locked` is safe only on the full-warm path; the "add `--locked` to per-pipeline `cargo build`" formulation is correct only for that case. This mirrors the three-tier logic already in `build_and_run_describer` (PR 10 checklist).
- [ ] **Mold detection is per-build, not a deployment-config knob**: probe `mold --version` once per `cargo build` invocation; if it exits 0, append `-C link-arg=-fuse-ld=mold` to `RUSTFLAGS`; if it fails or is absent from PATH, omit silently. An explicit operator-supplied `RUSTFLAGS` (via the manager process env) always wins — the probe result is added only when no `-fuse-ld` flag is already present. Do **not** require operators to edit Docker Compose / Helm charts.
- [ ] **`RUSTFLAGS` constraint is architectural (documentation-only)**: the constraint that `RUSTFLAGS` must be set at the manager level, not per pipeline, is already enforced by the existing code (manager propagates `std::env::var_os("RUSTFLAGS")` and clears all other env vars before spawning `cargo build`). The deliverable is a comment in `call_compiler` explaining the invariant — no new code guard is needed.
- [ ] Measurement appendix: build times before/after on a sample pipeline.
- **Depends on**: PR 10.
- **Unblocks**: nothing; orthogonal to PR 11.

### PR 13 — Multi-tenant cache layout + supply-chain hardening docs (Phase 8.8-8.9)
- [ ] **Verify per-tenant cache directory layout (no new code needed)** — `describer_workspace_dir(config, tenant_id, cache_key)` already constructs `<working_dir>/describer/<tenant_id>/<cache_key>/` and `workspace_dir_keys_by_tenant_then_hash` already tests it as of PR 9/10. This item is a confirmation step only: check that the function and test exist, then close it without writing new enforcement code.
- [ ] **Tenant isolation test is a unit test, not an HTTP integration test**: the isolation property is structural — it follows deterministically from `describer_workspace_dir` embedding the tenant UUID — so the useful assertion is a unit test that generates paths for two distinct tenant IDs and asserts they are always disjoint. A full multi-tenant HTTP integration fixture requires a running server, which the current `connectors.rs` test harness (synchronous unit tests) does not provide. Scope this item to the unit-test form unless a multi-tenant integration fixture is explicitly added.
- [ ] Deployment guide section: pin `rev = "<sha>"` not `tag` for git deps; vendoring with `cargo vendor`; **`[patch]` sections are not blocked by Feldera** — `validate_connectors_toml_shape` checks only line shape; `[patch]` is valid line-shape but redirects dependency resolution globally. The hardening doc must explicitly state that `[patch]` is not enforced (Cargo is the authoritative parser; operator-supplied resolver semantics are intentionally preserved) and direct operators to audit it manually — the plan's "auditing `[patch]`" phrasing implies review rather than enforcement, but that distinction is easy to miss; build scripts run with deployment privileges; the in-product editor's audit log (`edited_at` / `edited_by`) as the review surface — **note that `edited_at` / `edited_by` headers on `GET /v0/connectors/connectors.toml` are not implemented until PR 15 (in-product editor); the doc should describe the intended behavior and call out this dependency explicitly**.
- [ ] Add a "Hardening `connectors.toml`" page to `docs.feldera.com/`.
- **Depends on**: PR 11.
- **Unblocks**: PR 14.

### PR 14 — OpenAPI surface for connector endpoints (Phase 9)
- [ ] **Wire existing `#[utoipa::path]` annotations into the OpenAPI spec — do not rewrite them**: `connectors.rs` already has complete `#[utoipa::path]` annotations on all four handlers as of PR 10. The actual work is: (a) add the four handler paths to the `paths()` macro in `api/main.rs`, (b) add the three schema types (`ConnectorsStatusResponse`, `ConnectorsConfigPutResponse`, `StatusName`) to `components(schemas(…))`, and (c) add the `tag` field on each annotation. Status envelope spec: enum for `state`, optional `descriptors` array, ETag header documented.
- [ ] **Types registered in `components(schemas(…))` must be `pub(crate)` or wider**: private types referenced by path in the macro produce a compile error (not a silent omission). `StatusName` was originally declared without `pub`; it must be at least `pub(crate)` before it can be named in `components(schemas(…))`.
- [ ] **Register schemas referenced by other one-ofs**: utoipa silently emits `#/components/schemas/X` references even when `X` is not in `components(schemas(…))`; `--dump-openapi` succeeds but the web console SDK generator fails with "Reference not found". Add `feldera_types::config::PluginTransportConfig` (referenced from the `TransportConfig` one-of in Phase 7) plus any sibling plugin types. Sanity check before merge: `jq '.. | objects | .["$ref"]? // empty' openapi.json | sort -u` and cross-check.
- [ ] **Every endpoint consumed via `mapResponse` needs a typed error response**: the web console's `mapResponse` requires `E extends { message: string }`; an annotation with no `responses(...)` errors generates `unknown` and breaks TypeScript. Add `(status = INTERNAL_SERVER_ERROR, body = ErrorResponse)` to all four connector-management handlers and `use feldera_types::error::ErrorResponse;`.
- [ ] **`openapi.json` regeneration requires a full binary build, not `cargo check`**: run `cargo build -p pipeline-manager --bin pipeline-manager` (pulls in the full dependency graph — adapters, dbsp, sqllib — and takes roughly 2× longer than `cargo check` alone), then `./target/debug/pipeline-manager --dump-openapi` to produce the updated JSON. Re-run this after every annotation change; `cargo check` alone does not update the file.
- [ ] **`rest-api/build.rs:38-208` does not need new `type_replacement()` entries**: the existing entries map typed connector transport schemas (Kafka, Delta, etc.) to `feldera_types` equivalents and must remain untouched. The new connector-management response types (`ConnectorsStatusResponse`, `ConnectorsConfigPutResponse`, `StatusName`) are internal to `pipeline-manager` and have no `feldera_types` equivalents; progenitor correctly generates them as new Rust structs from the schema — adding them to `type_replacement()` would be wrong.
- [ ] Tests: typed-client codegen still compiles; OpenAPI schema validates against actual responses.
- **Depends on**: PR 11.
- **Unblocks**: PR 15.

### PR 15 — Web console in-product `connectors.toml` editor (Phase 10)

#### Account menu entry

- [ ] Add a **"Plugins" item** to the account menu opened by `ProfileButton`, positioned above the "Feldera Health" entry, separated by a separator. It should follow the same visual pattern — icon, label, right-chevron — but carry **no status dot**.
- [ ] Clicking "Plugins" opens a **modal dialog** (not a page navigation). The account menu closes on click.

#### Plugins dialog

- [ ] The dialog has an **"X" close button** in the top-right corner, and **"Close" / "Apply" buttons** in the bottom-right. "Close" discards any unsaved edits; "Apply" commits them (see save flow below).
- [ ] The dialog body contains an **editor panel** styled like the Code Editor panel inside `PipelineEditLayout`: a tab bar along the top, with the editor surface filling the remaining space.

#### Tab bar

- [ ] There is a **single tab, labelled `connectors.toml`**. The tab label carries a **colored status dot** reflecting the current describer compilation state:
  - Green — build succeeded (state `ready`).
  - Yellow — build in progress (state `building`).
  - Red — build failed (state `failed` or `not_configured`).
- [ ] The dot is always visible (not an unsaved-changes indicator). Poll `GET /v0/connectors/status` (with `If-None-Match` for cheap polling) while the dialog is open to keep the dot current.

#### Editor surface

- [ ] When `connectors.toml` content is available, display it in a **Monaco editor** configured as a plain-text surface (no schema, just syntax highlight using `graphql` monaco-editor language) — Cargo is the parser. Editor options (font, theme, minimap) should match the per-pipeline code editor's defaults.
- [ ] When the tenant has no `connectors.toml` yet (state `not_configured`), display a **placeholder** in the same style as the `udf.toml` tab placeholder in the pipeline code editor: a greyed-out hint text suggesting a minimal starting point, rendered inside the otherwise-empty editor area.
- [ ] On dialog open, fetch `GET /v0/connectors/connectors.toml` and store the returned **ETag**.

#### Save flow ("Apply")

- [ ] "Apply" issues `PUT /v0/connectors/connectors.toml` with `If-Match: <etag>` and `Content-Type: text/plain`.
- [ ] On **200**: store the new ETag; the status dot transitions to yellow (`building`) and eventually green or red as the describer runs.
- [ ] On **412 Precondition Failed**: surface an inline message — "Another operator updated this file. Close and reopen to see the latest version." Do not close the dialog; preserve the user's edits.
- [ ] On **400 Bad Request** (line-shape validation failure): display the error message inline below the editor — do not close the dialog.

#### Tests

- [ ] Unit test: status-dot color maps correctly to each `state` value from the API.
- [ ] Integration / e2e: open dialog → editor shows existing content → edit → Apply → status dot cycles through `building` → `ready`.
- [ ] ETag mismatch path: concurrent PUT from another session triggers the 412 message without discarding local edits.

- **Depends on**: PR 14.
- **Unblocks**: PR 16.

### PR 16 — Reference plugin + end-to-end integration test (Phase 11)

#### The `hello-lines` connector

- [ ] Add `crates/connector-example/` implementing a **`hello-lines` input connector**: reads a plain-text file and emits one line per second as a single-string record. The SQL table must have exactly one `TEXT` column; no format parsing is performed — each raw line becomes the column value. Config: `{ "path": "...", "interval_ms": 1000 }` (configurable interval so tests can set it to 0).
- [ ] `Cargo.toml` depends only on `feldera-adapterlib` — proves the plugin surface is closed.
- [ ] Implements `TransportInputEndpoint` + `InputReader` and registers via `register_connector!`.

#### Fault tolerance: `ExactlyOnce` via byte offset

- [ ] The connector advertises `FtModel::ExactlyOnce`. The seek datum is a **`u64` byte offset** into the file, serialized as 8 bytes. Using a byte offset (rather than a line number) means resume is a single `File::seek(SeekFrom::Start(offset))` call — O(1), no re-scan from the top.
- [ ] **Checkpoint**: on `Queue { checkpoint_requested: true }`, record `file.stream_position()` as the checkpoint payload.
- [ ] **Resume** (`Resume::Seek { seek }`): deserialize the offset and seek before entering the read loop.
- [ ] **Replay** (`Resume::Replay { seek, replay, hash }`): seek to `seek`, re-emit lines up through the `replay` offset into the replay buffer, then set position to `replay` and continue. For a sequential line reader this is a small bounded re-scan.

#### Integration test

- [ ] Enable `crates/connector-example` in `pipeline-manager` via a build feature flag and populate a test `connectors.toml` pointing at it.
- [ ] Write a known test file (e.g. five lines). Run the pipeline with `interval_ms = 0`. Assert the full plugin path: `connectors.toml` → describer → manifest → SQL parse → pipeline build → runtime dispatch → five rows in the output view.
- [ ] Checkpoint/restore test: checkpoint after three rows, restart the pipeline, assert only the remaining two rows are emitted (not all five) — exercising the `ExactlyOnce` seek path.

#### Documentation

- [ ] Add a "Writing a connector" doc page citing `crates/connector-example` as the canonical reference. Cover: the two traits (`TransportInputEndpoint`, `InputReader`), `register_connector!`, choosing a seek datum, the three FT commands (`Queue`, `Seek`, `Replay`), and the single-column schema constraint of this specific demo.

- **Depends on**: PR 15.
- **Unblocks**: nothing; ships the contract.

---

**Total: 23 PRs** — 16 top-level numbered PRs, with PR 7 split into 8 sub-PRs (7a–7f sweep the remaining bundled connectors; 7g is the post-migration cleanup; 7h migrates the criterion benches off concrete-type construction). PR 1–3 are pre-work; PR 4–6 are the core registry plumbing (PR 5 absorbed `http`/`adhoc`/`datagen` as prerequisites for the controller-rewrites, leaving Phase 4b shorter); PR 7a–7f are the remaining mechanical sweeps, PR 7g strips the scaffolding, PR 7h shrinks the public surface; PR 8–11 enable plugins; PR 12–13 harden the build; PR 14–16 expose plugins externally.
