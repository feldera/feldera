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
| 11 | `IntegratedOutputEndpoint` lives in `dbsp_adapters`, not `feldera-adapterlib` | `crates/adapters/src/integrated.rs:20` | A third-party integrated output connector cannot implement it without depending on `dbsp_adapters` — **resolved by PR 2** (step 0): trait + blanket impl moved to `feldera-adapterlib`; transitional two-hop re-export chain collapsed in PR 7g |

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
       pub direction: Direction,                  // Input | Output | InputOutput
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
       name: "my_kafka_clone",
       direction: Direction::InputOutput,
       kind: ConnectorKind::Regular,
       fault_tolerance: Some(FtModel::AtLeastOnce),
       config_schema: my_config_schema,        // fn() -> serde_json::Value
       default_format: None,
       flags: ConnectorFlags::EMPTY,
       build_input:  Some(my_build_input),     // named fn, NOT a closure
       build_output: Some(my_build_output),
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
1. Add `register_connector! { … }` block in the connector's module (or in a small `lib.rs` for that connector's submodule).
2. Set `kind: ConnectorKind::{Regular,Integrated,Transient}` directly — `ConnectorKind` is a Phase 2 enum, available immediately. Per-variant **flag** bits (`AUTO_RECREATED_ON_RESTART`) and the `default_format` field are populated only after Phase 5 adds them, so connectors with those special cases (clock's auto-recreate behaviour, datagen's default format) are revisited in Phase 4b. Connectors that only need `kind` (file, the message buses, …) need no Phase 5 dependency.
3. Delete the corresponding match arm from `crates/adapters/src/transport.rs` / `crates/adapters/src/integrated.rs`.
4. Keep the typed `TransportConfig` variant (recommended path); only `name()` learns to handle the upcoming `Plugin` variant in Phase 7.

**Staging** (this phase is split around Phase 5+6):
- **Phase 4a (before Phase 5)**: migrate `file` and `clock` end-to-end — descriptor + `build_*` fns + registry-dispatch path inserted before the existing match in both factory functions. The migrated arms move into the `Ok(None)` catch arm (input) or are deleted entirely (output, where `_ => Ok(None)` already absorbs them); other connectors' match arms stay intact. This is the smallest end-to-end exercise of the registry: descriptor → `connector_by_name` → `build_*` → endpoint construction. PR 4 also introduces `transport_config_inner_as_json` (in `adapters/src/transport.rs`, NOT adapterlib), which Phase 6 reuses unchanged.
- **Phase 5 implicit migrations**: replacing `is_http_input()`, the `ClockInput` filter, the `Datagen` default-format injection, and `is_transient()` with descriptor lookups (Phase 5's actual goal) **requires those connectors to already be in the registry**. So Phase 5 unavoidably migrates `http_input`, `http_output`, `adhoc_input`, and `datagen` ahead of Phase 4b. The two-phase factory pattern from Phase 4a absorbs them automatically — adding a descriptor short-circuits the registry path before the fallback match, and the dead arms move to `Ok(None)`. After Phase 5, the registered set is `{file_input, file_output, clock, http_input, http_output, adhoc_input, datagen}`.
- **Phase 4b (after Phase 6)**: sweep the **remaining** connectors — `s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, plus integrated (`postgres`, `delta_table`, `iceberg`). Kafka late because of its `ft`/`nonft` split (the descriptor must accept the `fault_tolerant: bool` argument the output factory currently uses — pass it through to `build_output`).

After Phase 4b, the four factory `match` statements in `transport.rs` and `integrated.rs` consist purely of registry calls, with no per-variant arms remaining.

**`#[doc(hidden)]` cleanup (PR 7g, commit 3)**: with integrated connectors now reachable through the descriptor registry from out-of-tree code, remove `#[doc(hidden)]` from `IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle`, and `OutputConsumer`. These were kept hidden in Phase 1 because surfacing them without their callers being plugin-reachable would have been confusing. They're now first-class plugin ABI.

**Re-export-chain cleanup (PR 7g, commit 2)**: PR 2 moves `IntegratedOutputEndpoint` to `feldera-adapterlib` but leaves a two-hop re-export chain (`feldera_adapterlib::transport → adapters::integrated → adapters::lib`). When PR 7g strips the integrated-factory match arms, the `adapters::integrated` module no longer needs to re-export the trait — switch `adapters::lib.rs` to import directly from `feldera_adapterlib::transport`. Same applies to `Encoder`/`OutputEndpoint` imports in `integrated.rs` (no longer needed there once the factory bodies are trivial registry calls).

**Integrated output controller plumbing**: `ControllerInner` must implement `OutputControllerRef` so the build function's `Arc<dyn OutputControllerRef>` parameter has a concrete impl. The trait's four methods (audited against actual call sites in Postgres writer + Delta during PR 2; `impl OutputControllerRef for ControllerInner` compiles with zero body changes to `ControllerInner`):

| Method | Source on `ControllerInner` |
| --- | --- |
| `output_transport_error(&self, endpoint_id: u64, endpoint_name: &str, fatal: bool, error: AnyError, tag: Option<&str>)` | direct method |
| `update_output_connector_health(&self, endpoint_id: u64, health: ConnectorHealth)` | direct method |
| `register_batch_progress_counter(&self, endpoint_id: &u64, counter: Arc<AtomicU64>)` | delegates to `self.status.*` |
| `output_buffer(&self, endpoint_id: u64, num_bytes: usize, num_records: usize)` | delegates to `self.status.*` |

The trait flattens the `controller.status.*` indirection — connectors call `controller.register_batch_progress_counter(...)` directly on `Arc<dyn OutputControllerRef>` instead of reaching through `controller.status`. Land the impl block with the first integrated-output connector migrated (PR 7f, Postgres writer / Delta); the skeleton is recorded in `connector-plugin-refactor-notes-pr2.md`.

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

**`inventory` must be a direct dep**: `register_connector!` expands to `::inventory::submit!`, which requires `inventory` as a direct dep of the calling crate (transitive through `feldera-adapterlib` is not enough). The `datagen` crate had to add `inventory = { workspace = true }` to its `Cargo.toml` for its `submit!` to compile. Document this in the connector-authoring guide; any future per-connector crate must do the same.

**Add a CI lint** that fails if `controller.rs` matches on a `TransportConfig` variant outside the dispatch entry points. Stops anyone from re-introducing per-variant special-casing.

---

### Phase 6 — Replace factory matches with registry lookup

Rewrite the four factory functions (`input_transport_config_to_endpoint` at `transport.rs:85`, `output_transport_config_to_endpoint` at `transport.rs:139`, `create_integrated_output_endpoint` at `integrated.rs:40`, `create_integrated_input_endpoint` at `integrated.rs:89`) to:

1. **Resolve secrets first** via `resolve_secret_references_via_json` (unchanged from today's behaviour). The factory is the right home for secrets resolution because it's generic across connectors; `build_*` functions receive an already-resolved `JsonValue` and never need to look at `secrets_dir` themselves. The `secrets_dir: &Path` argument on `BuildInputFn` / `BuildOutputFn` is kept on the signature for external connectors that bypass the factory and call `build_fn` directly with their own secret resolution.
2. Resolve `TransportConfig` → `(name, config_value)` via a helper (PR 4 introduced `transport_config_inner_as_json` in `crates/adapters/src/transport.rs`). The helper serialises the whole `TransportConfig` to `{"name": "...", "config": {...}}` and extracts the `"config"` field; for unit variants (e.g. `HttpOutput`) with no content, it returns `JsonValue::Null`. The helper **lives in `adapters`, not `adapterlib`**, because it depends on `feldera-types::config::TransportConfig`; adapterlib's `BuildInputFn` takes `&JsonValue` precisely so adapterlib stays free of that dep.
3. Look up descriptor by `config.name()` via `connector_by_name(&name)`. **Critical**: the lookup string is `TransportConfig::name()`, NOT the serde tag — these can differ (e.g. `ClockInput::name() == "clock"` while its serde tag is `"clock_input"`). Each descriptor's `name` field must equal the `name()` return value.
4. Match `descriptor.direction × kind` against the call site (input vs output, regular vs integrated). Mismatches return the same `Ok(None)` / `unknown_*_transport` errors as today.
5. Invoke `descriptor.build_*` with the resolved config value and contextual params.

**Lookup performance**: `connector_by_name()` walks the `inventory::iter` (a `#[link_section]`-based linked list, O(n)). For 17 built-in connectors plus a typical handful from `connectors.toml`, this is fast enough at pipeline startup. The factory functions are called once per endpoint at pipeline start, never on hot paths, so no caching layer is needed in adapterlib. If a future caller ends up in a hot path, cache as `HashMap<&'static str, &'static ConnectorDescriptor>` at the call site rather than complicating the adapterlib API.

**Transitional state**: when this phase lands, Phase 4a has migrated only `file`/`clock`. The factory's body is split into "registry path for migrated connectors" + "fallback `match` for unmigrated ones". As Phase 4b proceeds, the fallback shrinks until empty. **Pattern from PR 4**: as each connector migrates, its match arm is moved into the `Ok(None)` catch arm rather than left as dead code (Rust's exhaustiveness check + future "unreachable pattern" lints would otherwise flag it). For the output factory, `_ => Ok(None)` already absorbs them — explicit arms can be deleted. After Phase 4b lands, the `Ok(None)` arms shrink to empty and the explicit fallback `match` is removed entirely. This keeps every commit in this sequence functional and testable.

**`Direction::InputOutput` descriptors**: PR 4 only exercised input-only (`ClockInput`, `FileInput`) and output-only (`FileOutput`) descriptors. The first connector with `Direction::InputOutput` (e.g. `kafka` in PR 7d — same descriptor used from both factories) needs the registry dispatch to correctly check `descriptor.build_input.is_some()` from the input factory and `descriptor.build_output.is_some()` from the output factory, not just rely on `direction`. Document this in the Phase 6 implementation notes.

This is the largest mechanical change but does **not** touch any connector implementation. Every `TransportInputEndpoint` / `OutputEndpoint` impl is unchanged.

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

#### 8.1 The `connectors.toml` file

Lives at a deployment-configured path (e.g. `/etc/feldera/connectors.toml`, with per-tenant variants for multi-tenant setups). Format reuses Cargo's `[dependencies]` schema verbatim — no new syntax to learn:

```toml
# /etc/feldera/connectors.toml
[connectors]
acme-snowflake  = { version = "0.3", registry = "crates.io" }
my-sap-cdc      = { git = "https://github.com/acme/feldera-sap-cdc", tag = "v1.2.0" }
local-thing     = { path = "/opt/feldera/connectors/local-thing" }
```

Whatever Cargo accepts (versions, git refs, `path`, `[patch]`, alternative registries, auth) is accepted here without translation. Bundled connectors (`file`, `kafka`, `postgres`, `delta_table`, `iceberg`, …) stay in `dbsp_adapters` and are not listed here — empty `connectors.toml` = OSS defaults, behavior matches today's bundled-only deployments bit-for-bit.

#### 8.2 Reuse pipeline-manager's existing Cargo orchestration

pipeline-manager already runs Cargo per-pipeline. `prepare_workspace` (`crates/pipeline-manager/src/compiler/rust_compiler.rs:1242`) creates a `rust-compilation` workspace, copies a templated `Cargo.toml` into it, integrates UDF dependencies, and invokes Cargo. Two extensions to that flow:

- **Per-pipeline workspace**: append the `[connectors]` table from `connectors.toml` to the per-pipeline `Cargo.toml`'s `[dependencies]`, so the pipeline binary links the chosen connectors. **No per-pipeline feature gating** — see 8.5.
- **Describer build (new)**: a separate, much smaller workspace whose only purpose is producing the descriptor manifest. See 8.3.

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

**Why a describer rather than describing-via-pipeline-binary**: the describer compiles in seconds (no DBSP, no SQL-generated code), is pinned independently from per-pipeline builds, and serves as the single source of truth for the lockfile shared across all builds in this deployment (see 8.4).

#### 8.4 Caching and lockfile policy

Treat the describer as a long-lived deployment artifact, and **share its lockfile with per-pipeline builds**:

- **Cache key**: hash of `connectors.toml` content + `feldera-adapterlib` major version. Same key → reuse the cached manifest and lockfile, skip rebuild.
- **`describer.lock` is the single lockfile for the entire deployment.** Persisted at `/var/lib/feldera/describer/<tenant>/<content-hash>/describer.lock`. pipeline-manager copies it into both (a) the describer's workspace and (b) **every per-pipeline workspace** as `Cargo.lock`. Same lock everywhere = identical transitive resolution = identical sccache keys for all shared crates. **This is what makes the design build-cache-friendly** (see 8.5 for full analysis).
- **`cargo build --locked`** for both describer and per-pipeline builds. Cargo refuses to run if the lockfile would be modified, so transient resolution drift cannot creep in mid-build.
- **Updating the lock**: explicit `POST /v0/connectors/refresh` admin endpoint (or `feldera-cli connectors refresh`) runs `cargo update` against the describer workspace, rebuilds, and writes the new lock. CI can run this on a schedule. No automatic drift in production.
- **Manifest cache**: the produced JSON manifest is persisted next to `describer.lock` (`describer.manifest.json`). pipeline-manager loads it at startup without invoking Cargo unless the cache key has invalidated.

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

The describer's `Cargo.lock` defines exactly one `feldera-adapterlib` version. Every listed connector must be compatible (same major). Cargo will reject incompatible majors at resolution time; surface that with a clear "connector X depends on feldera-adapterlib Y, but this deployment uses Z" message rather than a raw Cargo error. CI on `feldera-adapterlib` itself runs `cargo semver-checks` to break on accidental breaking changes without a major bump.

#### 8.8 Multi-tenant deployments

Per-tenant `connectors.toml` at `/etc/feldera/connectors.d/<tenant>.toml`. Per-tenant cache directory at `/var/lib/feldera/describer/<tenant>/<content-hash>/{describer.lock, describer.manifest.json, target/}`. Tenants share **nothing**: each pays its own first-build cost. Intentionally less efficient than a global cache but eliminates cross-tenant correctness bugs.

#### 8.9 Hardening `connectors.toml` (deployment guidance)

Same supply-chain trust model as adding any Rust dep. Document in deployment guide:
- Prefer pinned `rev = "<sha>"` over `tag` for git deps (tags can be moved).
- For high-security deployments, use `path = "..."` pointing at vendored sources via `cargo vendor`.
- Audit any `[patch]` section.
- Build scripts run with deployment privileges; review them before adding a connector.

#### 8.10 Fallback — defer validation

If, for a first iteration, even the describer build is too much complexity, pipeline-manager can accept any `{name, config}` pair and let the controller surface direction errors at pipeline startup. Worse UX (errors at start instead of at SQL parse) but minimal pipeline-manager change. Documented escape hatch; not the recommended default.

---

### Phase 9 — REST API / OpenAPI for plugin configs

The hardcoded list at `crates/rest-api/build.rs:38-208` exists because the *generated client* (Rust SDK) needs strongly-typed structs for in-tree connector configs. For 3rd-party plugins, two acceptable answers:

1. **Opaque-on-client (recommended)**: plugin configs serialize as `serde_json::Value` in client SDKs. The full schema is available at runtime via a new endpoint `GET /v0/connectors`, returning every registered descriptor's JSON Schema. The web console reads this list to populate the connector picker and config form (see Phase 10). Matches how Kafka Connect, Airbyte, and Trino plugins work.
2. **Re-generate per deployment**: each enterprise build of the manager regenerates `openapi.json` with its plugin types replacement-listed. Possible but high-friction.

Take (1). Keep the existing list for in-tree connectors (no change for OSS users); plugins are opaque on the client and discovered at runtime.

The `GET /v0/connectors` endpoint also unblocks documentation tooling.

---

### Phase 10 — Web console connector discovery

The web console currently knows about each connector via the typed OpenAPI client. Switch to:

1. On first connector-picker render, call `GET /v0/connectors` and cache.
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

- **Add**: one `register_connector! { ... }` block (≤30 lines) listing name, direction, kind, FT level, schema fn, build closures.
- **Remove**: the matching arm in `transport.rs` / `integrated.rs`.
- **No change**: trait impls (`TransportInputEndpoint`, `InputReader`, `OutputEndpoint`, `Parser`, `Encoder`, …). The connector logic, threading model, FT machinery, error handling — all untouched.

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

6. ✅ **Describer rebuild stability**. `describer.lock` is the pin; `cargo build --locked` is enforced; `POST /v0/connectors/refresh` is the only path that updates the lock. Documented in 8.4.

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
8. **Phase 9** — `GET /v0/connectors` discovery endpoint reads the cached manifest.
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
- [ ] Document `IntegratedInputEndpoint`, `InputCollectionHandle`, and `OutputConsumer` in module docs as supported plugin ABI **even though they remain `#[doc(hidden)]`** — removing the attribute is deferred to Phase 4b.
- [ ] Add module docs explaining the FT contract (`fault_tolerance()` → `Resume::*` → `InputReaderCommand::Replay`).
- [ ] Add `cargo-semver-checks` CI job (`obi1kenobi/cargo-semver-checks-action`) on `feldera-adapterlib`; pair `invoke-check-semver` with `cancel-if-check-semver-failed` per the existing `ci.yml` pattern.
- [ ] Document the SemVer policy in the crate README, including the operational note that **`feldera-adapterlib` must remain published on crates.io** for the CI baseline check to function (a temporary unpublish or rename surfaces as a missing-baseline error, not a semver violation).
- [ ] Optional: mirror the semver check in `ci-pre-mergequeue.yml` for earlier feedback (verify the `ubuntu-latest-amd64` runner is available in that workflow context first).
- [ ] Verify all existing connectors still build against the tightened surface.
- **Depends on**: nothing.
- **Unblocks**: PR 2.

### PR 2 — `ConnectorDescriptor` + `register_connector!` macro (Phase 2)
- [ ] **Move `IntegratedOutputEndpoint`** and its blanket impl from `crates/adapters/src/integrated.rs:20` to `feldera-adapterlib`, alongside `IntegratedInputEndpoint`. Update `dbsp_adapters` to import from the new location. No semantic change. Without this move, third-party integrated output connectors would have to depend on `dbsp_adapters`. The intermediate two-hop re-export (`feldera_adapterlib::transport → adapters::integrated → adapters::lib`) is acceptable here; Phase 4b cleans it up.
- [ ] Define `ConnectorDescriptor` struct, `Direction` enum, `ConnectorKind` enum.
- [ ] Define `ConnectorFlags` as an **empty bitfield with a TODO comment** naming the flags Phase 5 will add (`HTTP_DIRECT`, `AUTO_RECREATED_ON_RESTART`). Plain `u32` newtype with `EMPTY` and `contains()` is fine here; Phase 5/PR 5 promotes it to `bitflags!`. Drop-in compatible.
- [ ] Define the four `Build*Fn` function-pointer types matching the existing factory signatures. `BuildIntegratedOutputFn` takes `Arc<dyn OutputControllerRef>` (**not** `Weak`) — the connector's `new()` immediately downgrades for storage. Define `OutputControllerRef` trait with the **four** methods audited from the actual call sites: `output_transport_error`, `update_output_connector_health`, `register_batch_progress_counter`, `output_buffer` (the last two flatten the `controller.status.*` indirection). Exact signatures are recorded in `connector-plugin-refactor-notes-pr2.md`. `ControllerInner` will impl this in PR 7f.
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
- [ ] Add `FILE_INPUT_DESCRIPTOR` + `FILE_OUTPUT_DESCRIPTOR` statics with `build_file_input` / `build_file_output` factory functions in `crates/adapters/src/transport/file.rs`; `inventory::submit!` both. Use `Direction::Input` and `Direction::Output` (the file connector exposes the two as separate descriptors, NOT a single `InputOutput`).
- [ ] Add `CLOCK_DESCRIPTOR` static with `build_clock_input` factory function in `crates/adapters/src/transport/clock.rs`; `inventory::submit!`. Use `kind: ConnectorKind::Transient` (matches today's `TransportConfig::is_transient()` behaviour for `ClockInput` — Phase 5 will replace the call site, but the kind classification belongs in PR 4 already).
- [ ] **Descriptor `name` must equal `TransportConfig::name()`, not the serde tag.** Concretely: `FILE_INPUT_DESCRIPTOR.name = "file_input"`, `FILE_OUTPUT_DESCRIPTOR.name = "file_output"`, `CLOCK_DESCRIPTOR.name = "clock"` (not `"clock_input"` — the serde tag and `name()` diverge for clock; the registry lookup uses `name()`).
- [ ] `config_schema` placeholder: `fn() -> JsonValue::Object(Default::default())` for now. Real JSON Schema lands in PR 14 alongside the discovery endpoint; an empty object is correct until then because nothing reads `config_schema` before Phase 9.
- [ ] **Add `transport_config_inner_as_json` helper in `crates/adapters/src/transport.rs`** (NOT in adapterlib — it depends on `TransportConfig`). Serialises the whole `TransportConfig` to `{"name", "config"}` and returns the `"config"` field; for unit variants (e.g. `HttpOutput`) with no content, return `JsonValue::Null`. This is the bridge between typed `TransportConfig` variants and `BuildInputFn`'s `&JsonValue` parameter, and is reused by Phase 6 / PR 6.
- [ ] **Add registry-dispatch path in both factory functions** (`input_transport_config_to_endpoint` and `output_transport_config_to_endpoint`) BEFORE the existing match. Pattern: resolve secrets first (unchanged), then `connector_by_name(&config.name())` → if `Some(descriptor)` and the appropriate `build_*` is `Some`, call it with the extracted config JSON. Otherwise fall through to the legacy match.
- [ ] **Move `FileInput` / `ClockInput` arms into the `Ok(None)` catch arm of the input match**; **delete the `FileOutput` arm entirely** (the output match's existing `_ => Ok(None)` already covers it). Don't leave them as dead arms — Rust's "unreachable pattern" lint and future exhaustiveness changes will flag them.
- [ ] Remove now-unused imports: `use clock::ClockEndpoint`, `use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint}` from `transport.rs`. The `build_*` functions in the connector modules construct the endpoints directly, so the factory no longer names these types.
- [ ] Verify the existing tests `transport::file::test::{test_csv_file_nofollow, test_csv_file_follow}` and `transport::clock::test::test_clock` pass — they call `mock_input_pipeline`, which exercises the full factory dispatch path (registry hit + endpoint construction).
- [ ] Add a unit test that resolves `"file_input"`, `"file_output"`, `"clock"` via `connector_by_name()` and asserts the descriptor's `direction` / `kind` / `build_*` slots are populated as expected.
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
- [ ] Move the JSON-Datagen format spec from `controller.rs` into `datagen/src/lib.rs::datagen_default_format()`. Remove the now-unused `use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat, JsonLines}` imports from `controller.rs`.
- [ ] Replace `transport.is_transient()` callers in `controller/pipeline_diff.rs:97,106` with descriptor lookups (`connector_by_name(&transport.name()).map(|d| d.kind == ConnectorKind::Transient).unwrap_or(false)`).
- [ ] Replace `transport.is_http_input()` at `controller.rs:4448` with `descriptor.flags.contains(HTTP_DIRECT)`.
- [ ] Replace `match TransportConfig::ClockInput(_)` filter at `controller.rs:4521` with `AUTO_RECREATED_ON_RESTART` check.
- [ ] Replace `match (TransportConfig::Datagen(_), None)` default-format injection at `controller.rs:6013` with `descriptor.default_format()`. The error message changes from `"datagen endpoints do not support custom formats"` to `"{name} endpoints do not support custom formats"`; update any test that asserts on the literal `"datagen"` substring.
- [ ] **Move `HttpInput`/`HttpOutput`/`AdHocInput`/`Datagen` arms to the input factory's `Ok(None)` catch arm** (mirrors PR 4's pattern). After this PR the catch arm contains: `FileInput`, `ClockInput`, `HttpInput`, `AdHocInput`, `Datagen`, plus all output-only variants. PR 7a–7g shrinks it.
- [ ] Remove now-unused `use adhoc::AdHocInputEndpoint`, `use http::HttpInputEndpoint`, `use feldera_datagen::GeneratorEndpoint` imports from `transport.rs`.
- [ ] Document the **registry-completeness invariant**: every transient connector and every connector whose flags the controller queries MUST register a descriptor. Lookups silently degrade to `false`/`None` for unregistered connectors. Removing `TransportConfig::is_transient()` is gated on verifying all four transient connectors are registered (done in this PR).
- [ ] Add CI lint that fails if any `match TransportConfig::*` arm appears in `controller.rs` outside the four dispatch sites.
- [ ] Verify `connector_flags_contains` test, file/clock transport tests (3), and format tests (87) still pass.
- **Depends on**: PR 4.
- **Unblocks**: PR 6.

### PR 6 — Rewrite factory functions to use the registry (Phase 6)
- [ ] Rewrite `input_transport_config_to_endpoint` (`transport.rs:85`) to: resolve secrets first (unchanged), then extract `(name, config_value)` via `transport_config_inner_as_json` (introduced in PR 4), look up descriptor via `connector_by_name(&config.name())` (NOT serde tag), call `build_input` if `Some`; fall back to existing match for unmigrated connectors.
- [ ] Same for `output_transport_config_to_endpoint` (`transport.rs:139`).
- [ ] Same for `create_integrated_input_endpoint` (`integrated.rs:89`).
- [ ] Same for `create_integrated_output_endpoint` (`integrated.rs:40`).
- [ ] Dispatch logic checks `descriptor.build_input.is_some()` / `descriptor.build_output.is_some()` per call site, not just `direction`. A `Direction::InputOutput` descriptor (first appears in PR 7d for kafka) must be reachable from both factories — direction alone is insufficient because the factory only knows whether it is the input or output entry point.
- [ ] Verify integrated factories pass `Arc<dyn OutputControllerRef>` (not `Weak`) to `BuildIntegratedOutputFn` — connector's `new()` immediately downgrades for storage. (Defined in PR 2; PR 7f lands the `impl OutputControllerRef for ControllerInner`.)
- [ ] Tests cover both code paths (registry hit and fallback hit) for at least one connector each.
- [ ] After this PR: `file`, `clock`, `http_input`, `http_output`, `adhoc_input`, `datagen` are reached only via the registry; everything else still hits the fallback match. (PR 4 migrated `file`/`clock`; PR 5 added the other four as a side effect of the controller-rewrites.)
- **Depends on**: PR 5.
- **Unblocks**: PR 7a–7g.

### PR 7a–7g — Sweep remaining bundled connectors onto the registry, then clean up (Phase 4b)
PRs 7a–7f follow the same recipe: add `register_connector!`, remove the corresponding fallback match arm, verify tests pass. PR 7g is the post-migration cleanup. Group as small PRs to keep blast radius low. **Note**: `http`, `adhoc`, and `datagen` were already migrated in PR 5 as prerequisites for the controller-rewrites; this sweep covers only the connectors that PR 5 didn't need to touch.
- [ ] **PR 7a**: `s3` + `url`.
- [ ] **PR 7b**: `nats` + `pubsub` (each behind its `with-*` feature gate).
- [ ] **PR 7c**: `redis` (output only).
- [ ] **PR 7d**: `kafka` — handles ft/nonft split; `build_output` accepts the existing `fault_tolerant: bool` parameter. First connector with `Direction::InputOutput`; verify the PR 6 dispatch logic correctly routes to `build_input` from the input factory and `build_output` from the output factory.
- [ ] **PR 7e**: `nexmark` (transient/generator).
- [ ] **PR 7f**: integrated — `postgres` (reader, writer), `postgres-cdc`, `delta_table`, `iceberg`. Wires `IntegratedOutputEndpoint`/`IntegratedInputEndpoint` builds. Land the `OutputControllerRef` impl on `ControllerInner` here (declared in PR 2). Each connector's `new()` receives `Arc<dyn OutputControllerRef>` and immediately downgrades to `Weak` for storage, mirroring today's `Weak<ControllerInner>` pattern.
- [ ] **PR 7g — final cleanup** (one PR, three commits): with all bundled connectors registry-driven after 7f, strip the post-migration leftovers. Each commit is independently small and reviewable, but they share the "PR 7 swept the connectors, now sweep the scaffolding" theme. Splitting into three separate top-level PRs would be needless review overhead.
  - **Commit 1 — strip dead match arms**: the four factory functions in `transport.rs` and `integrated.rs` should now contain only registry calls. The input factory's `Ok(None)` catch arm shrinks from `{FileInput, ClockInput, HttpInput, AdHocInput, Datagen, S3Input, UrlInput, NatsInput, PubSubInput, KafkaInput, NexmarkInput, postgres-reader, postgres-cdc, delta_table-input, iceberg}` (post-PR 5) → empty. Output factory and integrated factories likewise.
  - **Commit 2 — re-export-chain cleanup**: with `adapters::integrated` no longer the dispatch home, switch `adapters::lib.rs` to import `IntegratedOutputEndpoint` directly from `feldera_adapterlib::transport` (collapsing the two-hop chain established by PR 2). Drop `Encoder` / `OutputEndpoint` imports from `integrated.rs` if no longer referenced.
  - **Commit 3 — `#[doc(hidden)]` removal**: with integrated connectors now reachable from out-of-tree code via the descriptor registry, remove `#[doc(hidden)]` from `IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle`, and `OutputConsumer`. These were kept hidden in PR 1 because surfacing them without their callers being plugin-reachable would have been confusing. They become first-class plugin ABI here.
- **Depends on**: PR 6 (PRs 7a–7f are independent of each other; PR 7g depends on 7f having landed).
- **Unblocks**: PR 8.

### PR 8 — Open the `TransportConfig` enum (Phase 7)
- [ ] Add `Plugin(PluginTransportConfig)` variant in `crates/feldera-types/src/config.rs:1609`.
- [ ] Implement manual `Deserialize` that matches known names to typed variants and routes unknown names to `Plugin{name, config}`.
- [ ] Update `name()` (`config.rs:1638-1662`) to handle the `Plugin` arm.
- [ ] Remove `is_transient()` and `is_http_input()` helpers (already replaced by descriptor lookups in PR 5).
- [ ] **Audit every exhaustive `match TransportConfig` in the workspace** (`rg 'match.*TransportConfig'`); add a `Plugin` arm to each. Rust's exhaustiveness check makes this a compile-time forcing function — the work is mechanical, but the reviewer should expect this PR to touch ~5 files outside `config.rs`. Until PR 11 wires manifest-based validation, the `Plugin` arm in `program.rs:682,735` should reject the config with `UnknownConnector { name }` rather than panic with `unreachable!()`. Other sites (display/debug/serde-related) typically just forward `name` and `config` opaquely.
- [ ] Tests: serde round-trip for every known variant (byte-for-byte unchanged) plus a synthetic unknown name routing to `Plugin`. Add a test that a `Plugin` config submitted before PR 11 surfaces a clean `UnknownConnector` error rather than crashing.
- [ ] Verify a stored pipeline configuration from before this PR deserializes identically.
- **Depends on**: PR 7g (all bundled connectors registry-driven AND cleanup landed; the `Plugin` variant is purely an extension point at this point — no risk of dead match arms colliding with the new `Plugin` arm being added).
- **Unblocks**: PR 9.

### PR 9 — `connectors.toml` schema, parser, config plumbing (Phase 8.1)
- [ ] Define `connectors.toml` format reusing Cargo's `[dependencies]` schema verbatim (under `[connectors]` table).
- [ ] Add a config option to pipeline-manager pointing at the file path.
- [ ] Add per-tenant variants discovered via `/etc/feldera/connectors.d/<tenant>.toml`.
- [ ] Parser + validation (no describer yet — this PR is plumbing only).
- [ ] Tests: parsing valid files, rejecting malformed ones, multi-tenant lookup.
- **Depends on**: PR 8.
- **Unblocks**: PR 10.

### PR 10 — Describer binary + lockfile-based caching (Phase 8.2-8.4)
- [ ] Generate the describer crate (`crates/feldera-describer/`) on first use; its `Cargo.toml` lists `feldera-adapterlib` + `dbsp_adapters` + every `connectors.toml` entry.
- [ ] `prepare_describer_workspace` analogous to existing `prepare_workspace`.
- [ ] Build with `cargo build --locked` against persisted `describer.lock` at `/var/lib/feldera/describer/<tenant>/<content-hash>/`.
- [ ] Run the describer and capture JSON; persist as `describer.manifest.json` next to the lock.
- [ ] Cache key = hash of `connectors.toml` + `feldera-adapterlib` major version.
- [ ] Add `POST /v0/connectors/refresh` admin endpoint that runs `cargo update`, rebuilds, writes new lock.
- [ ] Detect duplicate connector names **in the describer's startup** (walk `inventory`, error if any name appears twice). Failure message names both source crates from `connectors.toml`. Do not push this check into adapterlib — `connector_by_name()` is intentionally simple and the describer has the deployment context to render the diagnostic.
- [ ] Surface incompatible-major errors with a clear "connector X needs feldera-adapterlib Y, deployment uses Z" message.
- [ ] Tests: cache hit, cache miss, refresh, name collision, version mismatch.
- **Depends on**: PR 9.
- **Unblocks**: PR 11, PR 12.

### PR 11 — Wire descriptor manifest into direction validation (Phase 8 cont.)
- [ ] Replace exhaustive matches at `pipeline-manager/src/db/types/program.rs:682,735` with manifest lookup + `direction.allows_input()` / `allows_output()` checks.
- [ ] Error messages name the unknown connector and suggest checking `connectors.toml`.
- [ ] Tests: known connector validates, unknown connector fails with a useful message, direction mismatch fails.
- [ ] Verify with empty `connectors.toml` that behavior matches today's bundled-only deployments bit-for-bit.
- **Depends on**: PR 10.
- **Unblocks**: PR 13.

### PR 12 — Build-cache discipline: shared lockfile + linker (Phase 8.5)
- [ ] Copy `describer.lock` as `Cargo.lock` into every per-pipeline workspace (extends `prepare_workspace`).
- [ ] Add `--locked` to per-pipeline `cargo build` invocation at `rust_compiler.rs:1574-1577`.
- [ ] Document `RUSTFLAGS` constraint: must be set at manager level, not per pipeline.
- [ ] Optionally set `RUSTFLAGS="-C link-arg=-fuse-ld=mold"` in default deployment configs (gated on host having `mold` available).
- [ ] Measurement appendix: build times before/after on a sample pipeline.
- **Depends on**: PR 10.
- **Unblocks**: nothing; orthogonal to PR 11.

### PR 13 — Multi-tenant cache layout + supply-chain hardening docs (Phase 8.8-8.9)
- [ ] Confirm cache directory layout `/var/lib/feldera/describer/<tenant>/<content-hash>/` is enforced.
- [ ] Test that one tenant's `connectors.toml` edit does not invalidate another tenant's manifest.
- [ ] Deployment guide section: pin `rev = "<sha>"` not `tag` for git deps; vendoring with `cargo vendor`; auditing `[patch]`; build scripts run with deployment privileges.
- [ ] Add a "Hardening connectors.toml" page to `docs.feldera.com/`.
- **Depends on**: PR 11.
- **Unblocks**: PR 14.

### PR 14 — `GET /v0/connectors` discovery endpoint (Phase 9)
- [ ] New endpoint reads the cached manifest and returns each descriptor with name, direction, kind, FT level, and JSON Schema.
- [ ] Update `openapi.json` (regenerate via existing build flow).
- [ ] Keep the typed connector schemas in `rest-api/build.rs:38-208` unchanged; plugin connectors are opaque on the typed client.
- [ ] Tests: endpoint response shape, with and without plugin connectors listed.
- **Depends on**: PR 11.
- **Unblocks**: PR 15.

### PR 15 — Web console connector discovery + dynamic config form (Phase 10)
- [ ] Connector picker in `js-packages/web-console/src/lib/` calls `GET /v0/connectors` on first render and caches.
- [ ] Render dropdown from descriptor list; preserve in-tree typed forms as the preferred UI for known connectors.
- [ ] Render JSON-Schema-driven form for plugin connectors.
- [ ] Tests: e2e test with a stub plugin descriptor returned by a mocked endpoint.
- **Depends on**: PR 14.
- **Unblocks**: PR 16.

### PR 16 — Reference plugin + end-to-end integration test (Phase 11)
- [ ] Add `crates/connector-example/` containing a minimal "hello" input connector implementing `TransportInputEndpoint` + `InputReader`, calling `register_connector!`.
- [ ] Its `Cargo.toml` depends only on `feldera-adapterlib` — proves the surface is closed.
- [ ] pipeline-manager integration test enables this crate via a build feature flag, populates a test `connectors.toml` pointing at it, and runs a pipeline ingesting from `hello://`.
- [ ] Test asserts the full plugin path: `connectors.toml` → describer → manifest → SQL parse → pipeline build → runtime dispatch.
- [ ] Add a "Writing a connector" doc page citing this crate as the canonical reference.
- **Depends on**: PR 15.
- **Unblocks**: nothing; ships the contract.

---

**Total: 22 PRs** — 16 top-level numbered PRs, with PR 7 split into 7 sub-PRs (7a–7f sweep the remaining bundled connectors; 7g is the post-migration cleanup). PR 1–3 are pre-work; PR 4–6 are the core registry plumbing (PR 5 absorbed `http`/`adhoc`/`datagen` as prerequisites for the controller-rewrites, leaving Phase 4b shorter); PR 7a–7f are the remaining mechanical sweeps and PR 7g strips the scaffolding; PR 8–11 enable plugins; PR 12–13 harden the build; PR 14–16 expose plugins externally.
