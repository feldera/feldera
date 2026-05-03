# Connector Plugin Refactor — Comprehensive Plan

This plan implements the design captured in
`connector-plugin-refactor-design.md`. Key structural moves:

1. **Metadata/impl split via Cargo feature.** A tiny crate
   `feldera-adapterlib-meta` holds the descriptor type, enums, and a
   `linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)`. `dbsp_adapters` carries
   `default = ["impl"]` / `metadata = []`; `--no-default-features` compiles
   only the descriptors and their cheap deps (seconds, not minutes).
2. **Pure-data descriptor.** `ConnectorDescriptor` carries name,
   direction, kind, FT, JSON schema, flags, and an optional
   `builder_path_override` — no `fn` pointers to builders. Builder
   dispatch is direct — never through a registry — in three flavours:
   pipeline-manager codegen emits a per-pipeline `match` over names that
   calls `<crate>::build_*` directly; in-tree controller tests use a
   hand-written `match` in `transport.rs` / `integrated.rs`; benches use
   plain `use` imports.
3. **`linkme` for both plugin-shaped registries.** The
   `CONNECTOR_METADATA_REGISTRY` and the format input/output registries
   from Phase 3 both use `#[linkme::distributed_slice]`. Plugin-shaped
   surfaces share one registration mechanism; `StorageBackendFactory`
   and `CheckpointSynchronizer` are orthogonal and remain on
   `inventory`.
4. **No force-link discipline anywhere.** Metadata is reached through
   `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` by name; impl
   is reached through `<crate>::build_*` by name (in codegen and the
   in-tree `match`). Both keep their rlibs alive on their own — no
   `extern crate <foo> as _;` workarounds, no per-tenant `force_link.rs`
   codegen.

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
| 6 | Format registries | `crates/adapters/src/format.rs:30,49` | `Lazy<BTreeMap>` with TODO for runtime registration — **resolved by PR 3**: `#[linkme::distributed_slice] FORMAT_INPUT_REGISTRY` / `FORMAT_OUTPUT_REGISTRY` slots and discovery functions live in `adapterlib/src/format.rs`; `adapters/src/format.rs` carries no registry state |
| 7 | Direction validation (input vs output) | `crates/pipeline-manager/src/db/types/program.rs:682,735` | Exhaustive variant lists |
| 8 | Per-variant special cases in controller | `controller.rs:4448` (`is_http_input`), `controller.rs:4521` (`ClockInput` filter), `controller.rs:6013` (`Datagen` default-format), `controller/pipeline_diff.rs:97,106` (`is_transient`) | Reach into the enum |
| 9 | OpenAPI client codegen list | `crates/rest-api/build.rs:38-208` | `progenitor` type-replacement list, hardcoded |
| 10 | DBSP type leakage in plugin ABI | `crates/adapterlib/src/format.rs:13` and `catalog.rs:17` (two distinct `StagedBuffers` import paths — internal vs. re-exported), `format.rs:63` and `transport.rs:82` (`InputCollectionHandle` parameter), `catalog.rs` (`SerBatchReader`/`SerCursor` expose `dbsp::dynamic::{DynData, DynVec, Factory}` in 8 method signatures used by partitioned-output encoders) | Plugin ABI surface reaches transitively into `dbsp::*` |
| 11 | `IntegratedOutputEndpoint` lives in `dbsp_adapters`, not `feldera-adapterlib` | `crates/adapters/src/integrated.rs:20` | A third-party integrated output connector cannot implement it without depending on `dbsp_adapters` — **resolved by PR 2** (step 0): trait + blanket impl moved to `feldera-adapterlib`; PR 2 also wires the clean re-export topology (private `use` in `integrated.rs`, direct `pub use` in `lib.rs` from `feldera_adapterlib::transport`) so no two-hop chain ever exists |

**Existing precedent**: pipeline-manager already runs Cargo per-pipeline (`crates/pipeline-manager/src/compiler/rust_compiler.rs:1242`, `prepare_workspace`) and already wires sccache (`rust_compiler.rs:1532-1534, 1548-1550`), `CARGO_INCREMENTAL` (`:1553`), and `cargo build --workspace --profile <profile>` (`:1574-1577`). Distributed-slice registries are already in use elsewhere in the repo (`StorageBackendFactory` in `crates/storage/src/lib.rs:49`, `CheckpointSynchronizer` in `crates/storage/src/checkpoint_synchronizer.rs:25` — both via `inventory`, both orthogonal to the plugin work). The plugin-shaped registries introduced here (`CONNECTOR_METADATA_REGISTRY`, format input/output registries) use `#[linkme::distributed_slice]` so the metadata and dispatch story is uniform.

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

**CI / SemVer note**: `cargo-semver-checks check-release` (run via `obi1kenobi/cargo-semver-checks-action`) downloads the published crate from crates.io and compares against local source. This requires `feldera-adapterlib` to remain published continuously; a temporary unpublish or rename fails CI with a missing-baseline error rather than a semver violation. Document this in the deployment / release guide. The action installs the tool itself, so no addition to `feldera-dev` container or workspace dev-dependencies is needed; the CI job runs on `ubuntu-latest-amd64`. Following the existing `ci.yml` pattern, every new substantive job needs a matching `cancel-if-<name>-failed` sentinel — pair `invoke-check-semver` with `cancel-if-check-semver-failed`. Consider mirroring the check in `ci-pre-mergequeue.yml` for earlier feedback (verify runner availability first).

---

### Phase 2 — Connector descriptor type + metadata crate split

0. **Move `IntegratedOutputEndpoint` from `dbsp_adapters` to `feldera-adapterlib`**. Currently defined at `crates/adapters/src/integrated.rs:20` along with its blanket impl `impl<EP: OutputEndpoint + Encoder + 'static> IntegratedOutputEndpoint for EP`. A third-party integrated output connector cannot implement it without depending on `dbsp_adapters` (the heavy crate). Move the trait + blanket impl into `feldera-adapterlib` so it joins `IntegratedInputEndpoint` on the plugin-reachable surface. Update `dbsp_adapters` to import from there. No semantic change.

1. **New crate `feldera-adapterlib-meta`** (~200 LOC, deps: `serde`, `serde_json`, `linkme`). Holds the descriptor type and the registry slot:

   ```rust
   pub struct ConnectorDescriptor {
       pub name: &'static str,                    // MUST equal TransportConfig::name() return value (NOT the serde tag — they can differ; e.g. ClockInput: name()="clock", serde tag="clock_input")
       pub direction: Direction,                  // Input | Output
       pub kind: ConnectorKind,                   // Regular | Integrated | Transient
       pub fault_tolerance: Option<FtModel>,      // best-case; runtime can downgrade
       pub config_schema: fn() -> serde_json::Value, // JSON Schema for config
       pub default_format: Option<fn() -> FormatConfig>, // for Datagen-style
       pub flags: ConnectorFlags,                 // empty bitfield in Phase 2; populated in Phase 5
       pub builder_path_override: Option<BuilderPath>, // None → default convention
   }

   pub struct BuilderPath {
       pub crate_name: &'static str,
       pub build_input:             Option<&'static str>,
       pub build_output:            Option<&'static str>,
       pub build_integrated_input:  Option<&'static str>,
       pub build_integrated_output: Option<&'static str>,
   }

   #[linkme::distributed_slice]
   pub static CONNECTOR_METADATA_REGISTRY: [ConnectorDescriptor] = [..];

   pub fn metadata_registry() -> impl Iterator<Item = &'static ConnectorDescriptor> {
       CONNECTOR_METADATA_REGISTRY.iter()
   }
   ```

   **Pure data — no `fn` pointers to builders.** Builder dispatch is direct (Phase 6); the descriptor only carries metadata + JSON schema + flags + an optional `BuilderPath` that names the builder symbol path for codegen consumers. `BuilderPath` strings are pure `&'static str` — no symbol references — so the metadata crate stays free of any `dep:` on impl crates.

   **Why a new crate.** Three goals fall out of this split:
   - External plugin authors register metadata against `feldera-adapterlib-meta` only (~3 cheap deps), not the full `feldera-adapterlib` data-plane surface (DBSP re-exports, `InputCollectionHandle`, the trait surface).
   - `feldera-platform-manifest`'s build.rs depends on `dbsp_adapters` with `default-features = false` — the metadata crate is the only adapter dep that has to compile.
   - The CI gate `cargo build -p dbsp_adapters --no-default-features` is structurally enforced (no impl-side code can reach into metadata-only territory).

   `feldera-adapterlib` (the existing crate) re-exports the descriptor types from `-meta` so plugin authors keep importing from `feldera_adapterlib::*` if they prefer one import root. This is the **only** crate split the design requires; `dbsp_adapters` stays a single multi-connector crate.

2. **`linkme::distributed_slice` for `CONNECTOR_METADATA_REGISTRY`.** Produces `&'static [T]` directly — the registry IS the slice. Stable since 1.62, no section-name collisions, no ctor init. The format registries from Phase 3 use the same mechanism so all plugin-shaped surfaces share one approach.

   **No `register_connector!` macro.** Connectors register by annotating a `pub static` directly with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]`. The linkme attribute is already the registration form; a macro layer would only introduce hygiene quirks for forwarding the descriptor literal. Example:

   ```rust
   #[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]
   pub static MY_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
       name: "my_kafka_clone_input",
       direction: Direction::Input,
       kind: ConnectorKind::Regular,
       fault_tolerance: Some(FtModel::AtLeastOnce),
       config_schema: my_config_schema,        // fn() -> serde_json::Value
       default_format: None,
       flags: ConnectorFlags::EMPTY,
       builder_path_override: None,            // default: <crate>::build_input/output/...
   };
   ```

   `ConnectorFlags` is defined as an empty bitfield in this phase with a TODO comment naming the flags Phase 5 will add (`HTTP_DIRECT`, `AUTO_RECREATED_ON_RESTART`). Defining the field here lets Phase 5 extend it as a non-breaking change without touching descriptor consumers.

3. **`dbsp_adapters` gains `default = ["impl"]` / `metadata = []` features.** The metadata-only build is gated by absence of `impl`:

   ```toml
   [features]
   default = ["impl"]
   metadata = []                # marker; absence of "impl" is what matters
   impl = [
       "dep:feldera-adapterlib", "dep:dbsp", "dep:rdkafka",
       "dep:datafusion", "dep:deltalake", "dep:tokio-postgres",
       # …every heavy dep
   ]
   with-kafka      = ["impl", "dep:rdkafka"]
   with-postgres   = ["impl", "dep:tokio-postgres"]
   # existing per-connector features become sub-features that imply impl

   [dependencies]
   feldera-adapterlib-meta = { workspace = true }                     # always
   feldera-adapterlib      = { workspace = true, optional = true }    # impl only
   dbsp                    = { workspace = true, optional = true }
   rdkafka                 = { workspace = true, optional = true }
   # …everything else heavy is optional
   linkme                  = { workspace = true }                     # for the slice
   ```

   `cargo build -p dbsp_adapters` behaves exactly as today (default `impl` is on). `cargo build -p dbsp_adapters --no-default-features` compiles in seconds — only the connector descriptors and their cheap deps.

4. **Discovery API** on `feldera-adapterlib-meta`: `pub fn metadata_registry() -> impl Iterator<Item = &'static ConnectorDescriptor>` and `pub fn descriptor_by_name(name: &str) -> Option<&'static ConnectorDescriptor>` (simple `iter().find()`; collision detection is the platform-manifest / describer's job, not the registry's).

5. **`OutputControllerRef`** lives in `feldera-adapterlib` (impl-side trait — connectors implementing integrated output need its methods). Its four methods are `output_transport_error`, `update_output_connector_health`, `register_batch_progress_counter`, `output_buffer`; `ControllerInner` impls it (Phase 6). The trait is **not** plumbed through the descriptor — it's passed directly to the connector's `build_*` fn parameter at the in-tree `match` arm and at codegen-emitted `match` arms. No registry-mediated `Arc<ControllerInner>` → `Arc<dyn OutputControllerRef>` coercion.

**Note on naming**: `ConnectorMetadata` already exists in adapterlib (`crates/adapterlib/src/connector_metadata.rs`) but means *per-record metadata* (Kafka topic name, Avro schema id). Use the distinct name `ConnectorDescriptor` to avoid collision.

---

### Phase 3 — Format registry

Add `#[linkme::distributed_slice] pub static FORMAT_INPUT_REGISTRY: [&'static dyn InputFormat]` and `#[linkme::distributed_slice] pub static FORMAT_OUTPUT_REGISTRY: [&'static dyn OutputFormat]` slots to **`adapterlib/src/format.rs`** (alongside the trait definitions, not in `adapters/src/format.rs` where the old `Lazy<BTreeMap>` lives). The discovery functions `get_input_format()` / `get_output_format()` move to adapterlib with them, implemented as `iter().find(|f| f.name() == name)`. The `Lazy<BTreeMap>` statics and old discovery functions in `adapters/src/format.rs` are removed entirely; existing call sites in `adapters` pick up the new functions via the existing `pub use feldera_adapterlib::format::*` re-export — **zero call-site churn**. Built-in formats (`csv`, `json`, `parquet`, `avro`, `raw`) register themselves from their own modules by annotating a `pub static` with the matching slice attribute.

**Why placement matters**: external format crates can only depend on `feldera-adapterlib`. Putting the slot in `adapters` would silently exclude them — they would compile but their registrations would land in a slot nothing iterates. This is the same constraint that drives `ConnectorDescriptor`'s placement in `feldera-adapterlib-meta` (Phase 2).

**Phase scope**: small, isolated. Formats and connectors share `linkme`, so plugin-shaped surfaces share one registration mechanism and the "Writing a connector" doc (PR 16) does not need a separate "Writing a format" idiom.

**linkme properties**: `linkme::distributed_slice` produces `&'static [T]` directly. Lookup is `iter().find()`, no linked-list traversal, no ctor init. No section-name collisions across crates linked into the same binary. Referencing the slice by name from `feldera-platform-manifest`'s build.rs (or any consumer) keeps the contributing rlibs alive — no `extern crate _;` lines per built-in format. Stable Rust since 1.62.

**Const-eval constraint**: a `pub static` annotated with `#[linkme::distributed_slice(...)]` requires a const initializer. The five built-in format types are zero-sized unit structs, so `pub static CSV_INPUT_FORMAT: &dyn InputFormat = &CsvInputFormat;` is const-evaluable directly (fat-pointer ZST coercions are const-stable). A future format with fields declares a separate `static MY_FORMAT: MyFormat = MyFormat { ... };` and registers `pub static MY_FORMAT_DYN: &dyn InputFormat = &MY_FORMAT;`. Closures will not compile.

**No `unsafe impl Sync`**: the trait bound `InputFormat: Send + Sync` makes `&'static dyn InputFormat: Sync` automatically. Format slots need no annotation.

**No registration macro**: in-tree format modules annotate their `pub static` directly with `#[linkme::distributed_slice(FORMAT_INPUT_REGISTRY)]`. External format crates use the same form — `linkme = { workspace = true }` becomes a direct dep of any crate that registers a format (transitive through `feldera-adapterlib` is not enough). The linkme attribute is the registration form.

**Feature-gated formats**: avro lives entirely behind `#[cfg(feature = "with-avro")]` at the module boundary, so its registration `static`s are naturally feature-gated without per-`static` `#[cfg]` annotations.

**Asymmetric formats**: `raw` is input-only — one `pub static` annotated `FORMAT_INPUT_REGISTRY`, no output counterpart.

**Future-proofing for external format plugins**: the metadata-only-build discipline that Phase 2 establishes for connectors applies symmetrically to formats — a separate `format-plugin-design.md` describes the external-format-plugin design (`FormatDescriptor` in `feldera-adapterlib-meta`, `FORMAT_METADATA_REGISTRY`, `default = ["impl"]` shape on format crates). Phase 3 ships the in-tree `&'static dyn InputFormat` / `OutputFormat` slices needed today; the descriptor split lands when external formats become a product goal. Keeping the in-tree slot on linkme makes that extension mechanical.

---

### Phase 4 — Migrate built-in connectors onto the descriptor + impl-mod split

Per connector (mechanical):
1. **Add the metadata `static`** with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` at module scope. Unconditional — no `#[cfg]`. The `name` field must equal `TransportConfig::name()` exactly (verify against the `name()` impl in `feldera-types/src/config.rs` rather than guessing from the type or serde tag — e.g. `PubSubInput::name() == "pub_sub_input"`, not `"pubsub_input"`). Add an unconditional `fn <name>_schema() -> serde_json::Value { … }` (cheap; placeholder `JsonValue::Object(Default::default())` until PR 14 fills in real JSON Schema).

   **Connectors in separate crates** (`feldera-iceberg`, future bundled connectors that live outside `dbsp_adapters`): the metadata `static` lives in a shim under `crates/adapters/src/integrated/<name>.rs` whose impl section calls into the external crate's constructor. The shim keeps `linkme` confined to `dbsp_adapters` (and prevents the external crate from needing the `metadata = []` feature shape).

2. **Wrap impl content in `#[cfg(feature = "impl")] pub(crate) mod <name>_impl { … }`.** This is the single cfg gate per connector file. The build function becomes `pub fn build_<name>_input(…)` / `build_<name>_output(…)` (or `_integrated_input` / `_integrated_output`) so the in-tree `match` and pipeline-manager codegen can name it directly. **The descriptor never names the build fn** — decoupling builder symbols from descriptors is what keeps the cfg surface to one gate per connector and removes force-link gymnastics on the impl side.

3. **Set `kind: ConnectorKind::{Regular,Integrated,Transient}` directly.** Per-variant **flag** bits (`AUTO_RECREATED_ON_RESTART`) and the `default_format` field are populated only after Phase 5 adds them, so connectors with those special cases (clock's auto-recreate behaviour, datagen's default format) are revisited in Phase 4b. Connectors that only need `kind` (file, the message buses, …) need no Phase 5 dependency.

4. **Add one arm per builder to the in-tree factory `match` in `crates/adapters/src/transport.rs` (or `integrated.rs`),** gated by the same `with-*` feature as the impl module. The match is keyed on `TransportConfig::name()` (a string) and calls `<name>_impl::build_<name>_*` directly. Production pipelines do **not** use this match — pipeline-manager's codegen emits a per-pipeline match (see Phase 6). The in-tree match is for `mock_input_pipeline`, controller integration tests, and the in-tree controller in general.

5. **Remove the per-variant arm in the factory `match`** that constructs the endpoint from `TransportConfig::Foo(...)` — the name-keyed arm in step 4 is the dispatch site. **For feature-gated connectors** (`nats`, `pubsub`, `redis`, `kafka`, …): remove *both* the `#[cfg(feature = "with-X")]` arm and the `#[cfg(not(feature = "with-X"))]` stub arm. **When the removal empties the match**, collapse the residual `let endpoint = match { ... }; Ok(Some(endpoint))` framing in the same PR; do not add `#[allow(unreachable_code)]`.

6. **Delete now-dead re-exports** in the connector's wrapper module. Modules like `nats.rs` / `pubsub.rs` typically have `pub use input::NatsInputEndpoint` re-exports whose only consumer was the fallback match in `transport.rs`. After migration the build function in `<name>_impl` constructs the endpoint directly; the re-export becomes an unused-import warning. Delete it. **Exception**: when the wrapper file's `<name>_impl` mod consumes it in-module (kafka pattern), the re-export stays alive — only the corresponding `use` lines in `transport.rs` are removed.

7. **Integrated-output constructors** (`postgres`, `delta_table`): `Endpoint::new()` accepts `Arc<dyn OutputControllerRef>` and calls `Arc::downgrade(&controller)` internally. Stored field types are `Weak<dyn OutputControllerRef>`. Trait calls `controller.foo()` reach the `OutputControllerRef` impl on `ControllerInner` from PR 6, which delegates to `self.status.*` internally; the output module does not import `ControllerInner`. Tests use `Arc::new(NoOpControllerRef)` where `NoOpControllerRef` is a local stub implementing the four `OutputControllerRef` methods as no-ops. Nested test submodules (e.g. `tests::parallel`) need `use super::NoOpControllerRef;`. **Also audit `crates/adapters/benches/`**: criterion benches construct these endpoints directly and live *outside* the `#[cfg(test)]` gate, so `cargo check --all-features` will not flag broken bench call-sites. Run `cargo build --all-features --benches` as part of the PR's CI gate. Place a shared `NoOpControllerRef` in `benches/bench_common.rs` with `#[allow(dead_code)]`.

8. **Feature-gated sub-connectors in always-enabled modules** (e.g. `postgres_cdc_input` inside `postgres.rs`): the metadata `static` and its schema fn stay unconditional; the impl branch lives inside `<name>_impl` (already feature-gated as a whole) and an inner `#[cfg(feature = "with-X")]` only applies to the *additional* heavy deps that the sub-connector requires. There is no separate cdc-specific `mod` wrapper for registration — `linkme` registrations happen at the unconditional metadata `static`, and impl deps are gated inside `<name>_impl`.

9. Add a `descriptor_by_name` test in the connector module (e.g. `input.rs::registry_test::pub_sub_input_descriptor`). This test must run in the default build — gate the connector's *integration* tests on whatever live infrastructure they need (`with-pubsub-emulator-test`, etc.), but the descriptor test only touches `descriptor_by_name` and has no external dependencies.

10. Keep the typed `TransportConfig` variant (recommended path); only `name()` learns to handle the upcoming `Plugin` variant in Phase 7.

**Staging** (this phase is split around Phase 5+6):
- **Phase 4a (before Phase 5)**: migrate `file` and `clock` end-to-end — metadata `static` + `<name>_impl` mod + one arm in the in-tree name-keyed match (Phase 6's hand-written dispatch site). PR 4 introduces `transport_config_inner_as_json` (in `adapters/src/transport.rs`, NOT adapterlib), which Phase 6 reuses unchanged for both transport and integrated factories.
- **Phase 5 implicit migrations**: replacing `is_http_input()`, the `ClockInput` filter, the `Datagen` default-format injection, and `is_transient()` with descriptor lookups (Phase 5's actual goal) **requires those connectors to already be in the registry**. So Phase 5 unavoidably migrates `http_input`, `http_output`, `adhoc_input`, and `datagen` ahead of Phase 4b. After Phase 5, the registered set is `{file_input, file_output, clock, http_input, http_output, adhoc_input, datagen}`.
- **Phase 4b (after Phase 6)**: sweep the **remaining** connectors — `s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, plus integrated (`postgres`, `delta_table`, `iceberg`). Kafka late because of its `ft`/`nonft` split: register `KAFKA_INPUT_META` (name="kafka_input") and `KAFKA_OUTPUT_META` (name="kafka_output") as separate metadata `static`s. `<kafka_impl>::build_kafka_output` receives the existing `fault_tolerant: bool` argument and branches between `KafkaFtOutputEndpoint` and `KafkaOutputEndpoint` internally.

After Phase 4b, the four factory `match` statements in `transport.rs` and `integrated.rs` are name-keyed dispatchers with one arm per built-in connector — there are no more typed-`TransportConfig::Foo` arms.

**`#[doc(hidden)]` cleanup (PR 7g, commit 2)**: integrated connectors are reachable through the descriptor registry from out-of-tree code, so `IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle` (the struct *and* its `new()` constructor — independently annotated), and `OutputConsumer` become first-class plugin ABI. PR 1 leaves `#[doc(hidden)]` on these types until callers are plugin-reachable; PR 7g removes the attribute. Each newly-public type needs a real one-sentence doc comment to satisfy the missing-doc lint. The cleanup also edits `crates/adapterlib/src/lib.rs`'s module-level ABI overview to drop the introductory paragraph about the `#[doc(hidden)]` carve-out and the inline `(\`#[doc(hidden)]\`)` annotations on the "Supported plugin-facing types" bullets.

**Bench-driven concrete-type exposure (deferred to PR 7h)**: `pub use crate::integrated::postgres::PostgresOutputEndpoint` and the `pub` qualifier on `mod delta_table` exist for criterion benches that import the concrete types directly. PR 7h migrates the benches to direct builder imports and then removes both. Outside the benches, no caller imports either path (verified with `rg`); inside `crates/adapters/src/integrated/`, sibling files reach `delta_table`'s helpers via `crate::integrated::delta_table::*` paths that work identically with `mod delta_table` (private modules are reachable through `crate::` from inside the same crate).

**Integrated output controller plumbing**: `ControllerInner` must implement `OutputControllerRef` so the build function's `Arc<dyn OutputControllerRef>` parameter has a concrete impl. The trait's four methods (audited against actual call sites in Postgres writer + Delta during PR 2; `impl OutputControllerRef for ControllerInner` compiles with zero body changes to `ControllerInner`):

| Method | Source on `ControllerInner` |
| --- | --- |
| `output_transport_error(&self, endpoint_id: u64, endpoint_name: &str, fatal: bool, error: AnyError, tag: Option<&str>)` | direct method |
| `update_output_connector_health(&self, endpoint_id: u64, health: ConnectorHealth)` | direct method |
| `register_batch_progress_counter(&self, endpoint_id: &u64, counter: Arc<AtomicU64>)` | delegates to `self.status.*` |
| `output_buffer(&self, endpoint_id: u64, num_bytes: usize, num_records: usize)` | delegates to `self.status.*` |

The trait flattens the `controller.status.*` indirection — connectors call `controller.register_batch_progress_counter(...)` directly on `Arc<dyn OutputControllerRef>` instead of reaching through `controller.status`. **Existing constructors must change signature**: `DeltaTableWriter::new()` and `PostgresOutputEndpoint::new()` (and any future integrated-output `new()`) take `Arc<dyn OutputControllerRef>` and `Arc::downgrade()` internally; stored fields become `Weak<dyn OutputControllerRef>`. There is no path from `Arc<dyn OutputControllerRef>` back to `Arc<ControllerInner>` (no `Any` downcast), so a wrapper that "translates" at the call-site boundary is impossible — the change reaches the connector's public constructor. After the change, the output module stops importing `ControllerInner` entirely. Test modules that used `Weak::<ControllerInner>::new()` as a dangling-weak null controller need a local `NoOpControllerRef` stub implementing the four trait methods as no-ops; pass `Arc::new(NoOpControllerRef)` to constructors. **Bench call-sites are silent until built**: criterion benches under `crates/adapters/benches/` construct these endpoints directly but live outside `#[cfg(test)]`; verifying the migration with `cargo check --all-features` will miss them. The migrating PR must run `cargo build --all-features --benches` and place a shared `NoOpControllerRef` in `benches/bench_common.rs`. **Land the impl block in PR 6 (Phase 6), not with the first integrated-output connector migration**: the integrated output factory's name-keyed match has arms that pass `Arc<dyn OutputControllerRef>` to the connector's build fn, so `ControllerInner: OutputControllerRef` must compile by then even though no integrated arms exist yet. The skeleton is recorded in `connector-plugin-refactor-notes-pr2.md`. Two of the four method names (`output_transport_error`, `update_output_connector_health`) shadow existing inherent methods on `ControllerInner` — the impl must use UFCS (`ControllerInner::output_transport_error(self, …)`) inside those two arms to avoid the trait method shadowing the inherent method during name resolution.

---

### Phase 5 — Capability methods replace per-variant special cases

Convert `ConnectorFlags` from the Phase 2 placeholder (plain `u32` newtype with `EMPTY` and `contains()`) to a `bitflags!`-generated type. **`bitflags` is not a workspace dep**; add `bitflags = "2"` directly to `crates/adapterlib/Cargo.toml` rather than to the workspace `[dependencies]` table. Single-crate direct dep is simpler than workspace promotion until a second crate needs it. `EMPTY` is preserved as `Self::empty()` for source compatibility; `contains()` is drop-in compatible. No call-site changes outside the struct definition.

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

**Default-format ownership lives on the connector**: the JSON-Datagen format spec (`format_name="json"`, `JsonFlavor::Datagen`, `JsonUpdateFormat::Raw`, `array=true`, `lines=Multiple`) lives in `datagen/src/lib.rs::datagen_default_format()`. The controller does not import `JsonParserConfig` / `JsonFlavor` / `JsonUpdateFormat` / `JsonLines` for this purpose. **Side effect**: the "custom format not supported" error message is generic (`"{name} endpoints do not support custom formats"`); any test asserting on a literal connector name in this message needs to match the templated form.

**`build_*: None` is a valid descriptor pattern.** `http_output` registers a descriptor with `build_output: None` because `HttpOutputEndpoint` is constructed by the HTTP server (`server.rs:2238`) at request time — its `name`/`format`/`backpressure` parameters come from the request, not from a stored config. The descriptor exists purely to expose `kind: ConnectorKind::Transient` for `pipeline_diff.rs` lookups. The output factory's registry path handles this correctly: descriptor found → `build_output` is `None` → `Ok(None)`, identical to the previous fallback. Document this as a valid pattern: descriptors-for-metadata-only, no factory dispatch required.

**`linkme` must be a direct dep of any crate that registers metadata**: `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` requires `linkme` as a direct dep of the annotating crate (transitive through `feldera-adapterlib-meta` is not enough). The `datagen` crate adds `linkme = { workspace = true }` to its `Cargo.toml`. The `feldera-adapterlib-meta` re-export pattern (`pub use linkme;`) does not solve this — the attribute path resolution does. Document in the connector-authoring guide.

**No force-link `extern crate _;` workaround.** linkme's `&'static [T]` slice form means referencing `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` (or any other pub item from a crate) by name is enough to keep the rlib alive — the linker pulls in everything that registers into the slice. Phase 5 does **not** add `extern crate feldera_datagen as _;` to `crates/adapters/src/lib.rs`; the platform-manifest crate's build.rs (Phase 8.13) and pipeline-manager codegen (Phase 8.4) reach `CONNECTOR_METADATA_REGISTRY` directly and the datagen rlib is pulled in as a transitive consequence. **Bundled-connector exception remains**: connectors in separate crates (`feldera-iceberg`) still register metadata via a shim under `crates/adapters/src/integrated/<name>.rs` to avoid forcing the external crate to take `linkme` as a direct dep.

**`secrets_dir` and `fault_tolerant` are opt-in `build_*` parameters**: the factory hands `secrets_dir: &Path` to every `build_*` function and `fault_tolerant: bool` to every `build_output`, but most connectors do not need them. Bind `_secrets_dir` in `build_*` for every bundled connector — `kafka` included. Secret reference resolution happens at the factory entry via `resolve_secret_references_via_json` *before* the build function is called; the build function receives an already-resolved config value. The `secrets_dir` parameter is reserved for connectors that store credential *file paths* in their config and must resolve them at construction time; no bundled connector currently does. Bind `_fault_tolerant` in `build_output` for `redis` and any other output whose `is_fault_tolerant()` is unconditionally `false`. Only `kafka` consumes `fault_tolerant` (to switch between its ft and nonft output impls). Document in the connector-authoring guide: prefix-with-underscore is the correct declaration for these parameters unless your connector materially uses them; the factory passes them unconditionally for signature uniformity.

**Add a CI lint** that fails if `controller.rs` matches on a `TransportConfig` variant outside the dispatch entry points. Stops anyone from re-introducing per-variant special-casing.

---

### Phase 6 — Hand-written name-keyed factory match + per-pipeline codegen path

**Scope**: Phase 6 rewrites the four factory functions in `transport.rs` / `integrated.rs` from typed-`TransportConfig::Foo(...)` arms to a name-keyed `match` over `TransportConfig::name()` calling each connector's `<name>_impl::build_<name>_*` directly. **There is no registry walk on the dispatch path** — neither in-tree nor in production pipelines.

There are three dispatch paths total. Phase 6 covers paths (1) and (2):

1. **Production pipelines (pipeline-manager codegen).** The SQL compiler walks `INPUT/OUTPUT FROM 'name'` clauses and emits a `match` directly into the per-pipeline globals crate at `<pipeline>_globals/src/connector_dispatch.rs`:

   ```rust
   pub fn build_input(name: &str, cfg: &serde_json::Value, ctx: &InputCtx)
       -> Result<Box<dyn TransportInputEndpoint>, ControllerError>
   {
       match name {
           "kafka_input"       => dbsp_adapters::transport::kafka::kafka_impl::build_kafka_input(cfg, ctx),
           "file_input"        => dbsp_adapters::transport::file::file_impl::build_file_input(cfg, ctx),
           "acme_snowflake_in" => acme_snowflake::build_input(cfg, ctx),
           _ => Err(ControllerError::unknown_input_transport(name)),
       }
   }
   ```

   The codegen reads `BuilderPath` from the descriptor (when set) or falls back to the default convention (`<crate>::build_input` / `build_output` / `build_integrated_input` / `build_integrated_output`). Production pipelines do **not** call into `dbsp_adapters::transport.rs`'s in-tree match.

2. **In-tree controller and tests** — `mock_input_pipeline`, `Controller::*`, `transport/kafka/ft/test.rs`. A hand-written `match` in `crates/adapters/src/transport.rs` and `crates/adapters/src/integrated.rs` maps `TransportConfig::name()` → direct `<name>_impl::build_*` call. ~17 arms, each gated by the same per-connector `with-*` feature that already gates the impl module. Adding a connector adds one arm; no force-link discipline.

3. **Benches and ad-hoc tools** — plain `use dbsp_adapters::transport::kafka::kafka_impl::build_kafka_input;`. No indirection at all (PR 7h migration).

**Each in-tree factory function is rewritten to:**

1. **Resolve secrets first** via `resolve_secret_references_via_json` (unchanged from today's behaviour). The factory is the right home for secrets resolution because it's generic across connectors; `build_*` functions receive an already-resolved `JsonValue` and never need to look at `secrets_dir` themselves.
2. Resolve `TransportConfig` → `(name, config_value)` via `transport_config_inner_as_json` (introduced in PR 4 in `adapters/src/transport.rs`; promoted to `pub(crate)` in PR 6 so `integrated.rs` can call it). The helper serialises the whole `TransportConfig` to `{"name": "...", "config": {...}}` and extracts the `"config"` field; for unit variants (e.g. `HttpOutput`) with no content, it returns `JsonValue::Null`.
3. `match name { "kafka_input" => kafka_impl::build_kafka_input(cfg, …), … }` over the per-connector arms. **Critical**: the match key is `TransportConfig::name()`, NOT the serde tag — these can differ (e.g. `ClockInput::name() == "clock"` while its serde tag is `"clock_input"`).
4. The catch arm returns `Ok(None)` (transport factories) or `Err(unknown_*_transport(name))` (integrated factories), matching today's behaviour.

**`impl OutputControllerRef for ControllerInner` lands here**: the integrated output factory's match arms pass `Arc<dyn OutputControllerRef>` to each connector's build fn (after `controller.upgrade()`), which requires `ControllerInner: OutputControllerRef` at compile time. The first arms that consume it land in PR 7f; the impl must compile before that. The impl is zero-cost — all four methods delegate to existing `ControllerInner` inherent methods or `ControllerInner.status.*` methods. Two methods (`output_transport_error`, `update_output_connector_health`) name-collide with inherent methods; the impl uses UFCS (`ControllerInner::output_transport_error(self, …)`) inside those arms.

**`controller.upgrade()` failing at dispatch time**: a `Weak<ControllerInner>` upgrade can return `None` if the controller has been dropped. This is a programming error (factory called after teardown), not a config error. The integrated factory maps it to `ControllerError::invalid_transport_configuration` as a placeholder; a more precise variant can be added later if real call sites surface it.

**Format-check is single-site**: integrated connectors do not accept formats (the check `if connector_config.format.is_some()` rejects the config). The check sits once **before** the name-keyed match — the catch arm `Err(unknown_*_transport(...))` does not need its own check. Future integrated connectors that *do* accept formats can opt into a per-arm exception.

**Dispatch performance**: a name-keyed `match` over ~17 string literals lowers to a jump table or short cmp chain — no walk, no allocation. Production pipelines never hit this match (codegen replaces it); for in-tree tests and `mock_input_pipeline` it's called once per endpoint at startup.

**Transitional state during the migration**: PR 4 introduces the name-keyed match alongside the existing typed match (two-phase dispatch — name lookup first, then fall through). As each connector migrates in PR 5/7a-f its typed arm is deleted; by PR 7f all four factories are pure name-keyed matches with no typed-`TransportConfig::Foo` arms remaining. PR 7g strips any residual `_ => Ok(None)` shells around the name-keyed match.

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

**Design principle**: pipeline-manager carries zero hardcoded connector knowledge. The set of connectors is whatever the deployer declared in `connectors.toml`, plus the bundled connectors compiled into `dbsp_adapters`. The descriptor *type* lives in `feldera-adapterlib-meta` (a contract, not a list) alongside `CONNECTOR_METADATA_REGISTRY`; each connector crate registers itself by annotating a `pub static` with `#[linkme::distributed_slice]`; pipeline-manager learns the set via a build step it already runs.

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

- **Per-pipeline workspace**: pipeline-manager reads the tenant's blob from `tenant_connector_config` and the SQL compiler walks the program's `INPUT/OUTPUT FROM 'name'` clauses to compute the **set of external connector crates the SQL actually references**. Only those are spliced into the per-pipeline `Cargo.toml`'s `[dependencies]` block (string-substitution pattern from `udf_toml` at `rust_compiler.rs:1336-1346`). Bundled connectors stay all-in via `dbsp_adapters` (single rlib; tree-shaking within `dbsp_adapters` is the existing build-time `with-*` feature decision, not a per-pipeline knob — see 8.5).
- **Describer build (new)**: a separate, much smaller workspace whose only purpose is producing the descriptor manifest from the user's `connectors.toml`. See 8.3.
- **No force-link `extern crate _;` codegen.** linkme references `CONNECTOR_METADATA_REGISTRY` by name, which keeps every contributing rlib alive. The describer's `build.rs` references `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY`; pipeline-manager's per-pipeline codegen emits `match` arms naming each connector's `<crate>::build_*` symbol directly. There are no per-tenant `force_link.rs` files in either workspace.

#### 8.3 The user describer binary

A small Rust crate generated by pipeline-manager whenever a tenant's `connectors.toml` changes. It enumerates the **user-listed** connector descriptors only — the platform manifest covers built-ins (see 8.13).

```rust
// generated user describer's main.rs
fn main() {
    let manifest: Vec<&'static ConnectorDescriptor> =
        feldera_adapterlib_meta::metadata_registry().collect();
    println!("{}", serde_json::to_string(&manifest).unwrap());
}
```

Its generated `Cargo.toml` lists `feldera-adapterlib-meta` + every entry from the tenant's `connectors.toml`. **It does not list `dbsp_adapters` or `feldera-datagen`** — those are owned by the platform manifest (8.13). pipeline-manager builds it once per connector-set, runs it, captures the JSON, and merges with the platform manifest at consumer sites.

The merged manifest feeds `program.rs:682,735` validation:

```rust
let manifest = self.descriptors(); // cached HashMap<String, ConnectorDescriptor>
let descriptor = manifest.get(&connector.config.transport.name())
    .ok_or(ConnectorGenerationError::UnknownConnector { ... })?;
if !descriptor.direction.allows_input() {
    return Err(ConnectorGenerationError::ExpectedInputConnector { ... });
}
```

**No `force_link.rs` codegen.** The user describer's `build.rs` references `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` by name, which keeps every contributing rlib (every user-listed crate that registered metadata via `#[linkme::distributed_slice]`) alive. There is no per-tenant `force_link.rs`, no `extern crate _;` line per dep, and no equivalent in the per-pipeline workspace. linkme's `&'static [T]` form is not tree-shaken when the slice symbol is referenced.

**Per-pipeline workspace.** Pipeline-manager's codegen reads the SQL program's `INPUT/OUTPUT FROM 'name'` clauses, looks each name up in the merged manifest to recover the `BuilderPath` (or default `<crate>::build_*` convention), and emits a `connector_dispatch.rs` file in the per-pipeline globals crate containing one `match` arm per name (see Phase 6 for the example). The same name-walk drives the per-pipeline `[dependencies]` list — only external crates the SQL actually references are listed. Bundled connectors don't appear in the codegen's `[dependencies]`; they're reached transitively through `dbsp_adapters`, which is always linked.

**Why a describer rather than describing-via-pipeline-binary**: the user describer is metadata-only — `feldera-adapterlib-meta` plus user connector crates with `default-features = false` (when external crates respect the convention; see 8.13). Cold build is seconds, not minutes. It's pinned independently from per-pipeline builds and serves as the single source of truth for the lockfile shared across all builds in this deployment (see 8.4).

#### 8.4 Caching and lockfile policy

Treat the describer as a long-lived deployment artifact, and **share its lockfile with per-pipeline builds**:

- **Cache key**: `sha256(connectors.toml content || ADAPTERLIB_API_VERSION)`. Same key → reuse the cached manifest and lockfile, skip rebuild. Using the plugin ABI version (not the full Feldera release version) means a Feldera patch or minor release that does not change the connector plugin ABI does **not** force a describer rebuild or a connector recompile.
- **`describer.lock` is the per-tenant single lockfile** shared by every per-pipeline build for that tenant. Persisted at `<working_dir>/describer/<tenant_id>/<cache_key>/describer.lock` where `cache_key = sha256(content || ADAPTERLIB_API_VERSION)` — note this is **not** the same as `tenant_connector_config.content_hash` (which is `sha256(content)` alone). pipeline-manager copies it into both (a) the describer's workspace and (b) **every per-pipeline workspace for that tenant** as `Cargo.lock`. Same lock per tenant = identical transitive resolution within a tenant = identical sccache keys for all shared crates. **This is what makes the design build-cache-friendly** (see 8.5 for full analysis). Tenants do not share lockfiles — that's intentional, see 8.8.
- **`cargo build --locked`** for both describer and per-pipeline builds — once a lock exists, with a three-tier warm-path discipline:
  - **Cold path** (no `describer.lock` yet, or `POST /refresh`): build without `--locked`; Cargo resolves and writes `Cargo.lock`.
  - **Full warm path** (`describer.lock` exists AND `describer.build_version` matches the current Feldera release version): copy the lock as `Cargo.lock` and pass `--locked`. Both Feldera path deps and external connector pins are exactly as recorded.
  - **Partial warm path** (`describer.lock` exists but `describer.build_version` differs, i.e. Feldera was upgraded within the same `ADAPTERLIB_API_VERSION`): copy the lock as a resolver seed (preserving external connector pins) but omit `--locked` so Cargo can update the Feldera path-dep versions. sccache serves cached external connector rlibs, so effective build time is minimal.
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
2. **Per-pipeline feature variation of `dbsp_adapters` itself.** If pipelines enabled different feature subsets, every bundled-connector rlib would get a unique cache key per pipeline. **Prevented**: bundled connectors continue to be all-in. `dbsp_adapters` is one rlib with a single feature configuration determined at manager build time; tree-shaking inside `dbsp_adapters` is the existing build-time `with-*` features, fixed once per deployment.
3. **Per-pipeline external impl set is SQL-driven, not feature-driven.** Each per-pipeline workspace's `[dependencies]` lists only the external connector crates the SQL actually references (computed by walking `INPUT/OUTPUT FROM 'name'` clauses against the manifest). This is **not** Cargo feature variation — each external connector crate's rlib hash is identical across pipelines, so sccache hits unconditionally. Only the *link step* varies, and the link step is never cached anyway. Per-pipeline external impl sets are a strict win on link time.
4. **`RUSTFLAGS` variation per pipeline.** Different flags = different cache keys for shared crates. **Prevented**: pipeline-manager fixes `RUSTFLAGS` once at startup from config and never varies per-pipeline. Already the existing behavior at `rust_compiler.rs:1544` (passes through manager-level env), but document it as a constraint.
5. **Build script non-determinism in 3rd-party connectors.** A `build.rs` that reads timestamps or unstable env vars caches differently each run. **Mitigated, not prevented**: document the well-behaved-build-script requirement; the reference connector example (Phase 11) demonstrates the rule.
6. **Profile variation.** `--profile unoptimized` vs `--profile optimized` produces different rlibs. **Accepted**: switching profile = cold rebuild. Already true today.

**Linker is the per-pipeline floor.** Concrete recommendation: set `RUSTFLAGS="-C link-arg=-fuse-ld=mold"` (or lld) at pipeline-manager startup. mold/lld cuts link time 5–10× vs. system ld. Single biggest per-pipeline speedup available.

**Future optimization (do not implement on day one)**: a "platform archive" — pre-link `dbsp_adapters` + listed connectors into a single static rlib once per `(connectors.toml, feldera-adapterlib)` combo; per-pipeline build links one rlib instead of N. Material speedup for large `connectors.toml` (≥10 listed). Defer until measurements justify it.

**Steady-state expectation**: with the discipline above, **per-pipeline build time is approximately the same as today** (no `connectors.toml`) plus a small linker delta proportional to the number of listed connectors. The cold-build cost moves from "per pipeline" to "per `connectors.toml` change" — paid by one pipeline build after each refresh, then amortized.

#### 8.6 Name collisions

Two listed crates registering `name: "kafka"` is a real risk. Behavior: the describer fails fast at startup with a clear message naming both source crates. Resolution options for the deployer: drop one crate from `connectors.toml`, or ask the upstream to namespace (`acme:kafka`). Detect by walking `CONNECTOR_METADATA_REGISTRY` and checking for duplicate names; emit an error from the describer rather than letting later code see ambiguous lookups.

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
- **Bundled-only tenants skip the describer entirely.** A tenant with an empty `tenant_connector_config.content` (the default for newly-created tenants when `connectors_toml_path` is unset, or after explicit clearing) means the manifest is constructed in-process from a `metadata_registry()` walk of the manager's own linked descriptors — no Cargo invocation, no pre-condition. `GET /v0/connectors/status` reports `state: "not_configured"`. Program lifecycle is bit-for-bit unchanged from today.
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

#### 8.13 Platform manifest baked at platform build time; user describer trimmed to user crates only

The connector descriptor manifest is split across two production paths:

- **Built-in connectors** (everything shipped in `dbsp_adapters` + `feldera-datagen`) are enumerated at platform build time by the workspace member `crates/platform-manifest/` (`feldera-platform-manifest`).
- **User-listed connectors** (entries in a tenant's `connectors.toml`) are enumerated by a per-tenant user describer that compiles only those crates against `feldera-adapterlib`.

Both manifests are merged at every consumer site (SQL compiler `attempt_end_to_end_sql_compilation`, `compiler_precompile`, `GET /v0/connectors/status` `descriptors` field). Platform entries win on name collisions; the merge logs a warning.

**`feldera-platform-manifest` crate.** Library only; runtime `[dependencies]` is empty. `[build-dependencies]`:

```toml
[build-dependencies]
feldera-adapterlib-meta = { workspace = true }
dbsp_adapters           = { path = "../adapters", default-features = false }
feldera-datagen         = { workspace = true, default-features = false }
serde_json              = { workspace = true }
```

The build-dep tree is **metadata-only** — `dbsp_adapters` and `feldera-datagen` build with `default-features = false`, which excludes the `impl` feature and skips compiling `dbsp`, `rdkafka`, `datafusion`, `deltalake`, etc. Cold build of the build-dep tree completes in seconds. Cargo's resolver v2 isolates this build-dep feature configuration from pipeline-manager's runtime use of `dbsp_adapters` (which keeps `default-features = true`), so the same `cargo build` produces both flavours under distinct artifact slots.

The `build.rs`:

1. References `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` (or any pub item from `dbsp_adapters` / `feldera-datagen`) so the linker keeps the rlibs reachable. **No `extern crate _;` lines** — linkme's slice-form registration fires for every crate whose `pub static` is annotated, and naming the slice keeps those rlibs alive.
2. Walks `metadata_registry()` and serialises the resulting `ConnectorManifestEntry` array via `serde_json`.
3. Writes the JSON to `$OUT_DIR/platform_manifest.json`.
4. Panics on duplicate built-in names — the same defensive check the user describer runs at startup, applied here so a programming error in `dbsp_adapters` fails the platform build instead of shipping an ambiguous manifest.
5. Declares `cargo:rerun-if-changed=build.rs`. Cargo's build-dep fingerprint tracking fires the build script automatically when any contributing rlib hash changes (descriptor edit, schema fn edit), so the rerun-if-changed line only needs to cover the build script itself. Impl-only edits in `dbsp_adapters` do **not** change the metadata-only rlib hash and do not re-run the build script.

`src/lib.rs` is one line: `pub const PLATFORM_MANIFEST_JSON: &str = include_str!(concat!(env!("OUT_DIR"), "/platform_manifest.json"));`.

**pipeline-manager wiring.** `feldera-platform-manifest = { workspace = true }` in pipeline-manager's runtime `[dependencies]`. The crate exposes a `&'static str`; pipeline-manager does not inherit the data-plane dep tree at runtime. `load_platform_manifest() -> HashMap<...>` is a plain `serde_json::from_str(feldera_platform_manifest::PLATFORM_MANIFEST_JSON)` — no `config` argument, no on-disk read, no subprocess. On parse failure (a build.rs bug), it falls back to `build_in_process_manifest()` (a `metadata_registry()` walk over whatever pipeline-manager itself happens to link) and warns.

**User describer.** Per-tenant workspace at `<working_dir>/describer/<tenant_id>/<cache_key>/`. Generated Cargo.toml lists `feldera-adapterlib-meta` + user crates only — never `dbsp_adapters` or `feldera-datagen`. **External plugin crates are expected to expose the same `default = ["impl"]` / `metadata = []` shape as `dbsp_adapters`**; the user describer references their metadata via `default-features = false` so the impl tree doesn't get compiled. Without that convention, the describer pulls the plugin's full data-plane tree just to extract the descriptor — correctness is unaffected, but that plugin pays a 5–10 minute compile cost the first time it appears. The reference plugin (PR 16) demonstrates the convention and the "Writing a connector" doc (PR 16) requires it. Empty `connectors.toml` skips the user describer entirely.

**No `force_link.rs` generation.** linkme references through `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` keep all contributing rlibs alive. The per-pipeline globals crate has no force-link source either — the codegen-emitted `connector_dispatch.rs` references each connector's `<crate>::build_*` symbol by name, which keeps each external crate's rlib alive on its own. Bundled connectors are reached through `dbsp_adapters` (always linked).

**Lockfile.** The user describer keeps the seed-from-main-`Cargo.lock` + `--offline` discipline from 8.4. The platform manifest crate is a normal workspace member; it uses the workspace's own `Cargo.lock` and needs no separate coordination.

**Build-cache properties.** A clean `cargo build -p pipeline-manager` compiles the build-dep tree once. With `dbsp_adapters` and `feldera-datagen` building as `--no-default-features` for the build-dep slot, this is **seconds, not minutes** — only the metadata side (descriptors + cheap deps) compiles. After that:
- Editing pipeline-manager source only: nothing else rebuilds.
- Editing a built-in connector's **descriptor or schema fn** (`kafka.rs`'s metadata `static`): the metadata-only `dbsp_adapters` rlib regenerates, build.rs re-runs (driven by Cargo's rlib-hash fingerprint tracking), `feldera-platform-manifest`'s lib recompiles its `include_str!`, pipeline-manager recompiles. Seconds.
- Editing a built-in connector's **impl** (`kafka_impl::build_kafka_input` body): the metadata-only `dbsp_adapters` rlib hash is **unchanged** (the impl mod is gated by `feature = "impl"` and isn't part of the metadata-only build). build.rs does not re-run. The full-feature `dbsp_adapters` rlib used by pipeline-manager's runtime path rebuilds, but `feldera-platform-manifest` does not. Impl-only edits do not cascade through the platform-manifest path.
- Editing a deep transitive of `dbsp_adapters`'s impl tree cascades through the runtime path only — the metadata path is insulated.

**Target sharing.** `feldera-platform-manifest`'s build.rs runs as part of `cargo build -p pipeline-manager`, so its build-dep artifacts land in the workspace's main `target/` automatically. With resolver=2, build-deps and regular deps occupy distinct artifact slots under that root. Per-tenant user describers keep isolated target directories at `<working_dir>/describer/<tenant_id>/<cache_key>/target/` so concurrent tenant edits do not serialize on a shared `.cargo-lock`.

**Tests** (Phase 8.13):
- `feldera-platform-manifest` builds cleanly and emits a non-empty JSON array containing every expected built-in (`datagen`, `kafka_input`/`kafka_output`, `delta_table_input`/`delta_table_output`, `nats_input`, `pub_sub_input`, `redis_output`, `s3_input`, `url_input`, `file_input`/`file_output`, `clock`, `http_input`/`http_output`, `adhoc_input`, `nexmark`, `iceberg_input`/`iceberg_output`, `postgres_input`/`postgres_output`, `postgres_cdc_input`).
- `merge_manifests` keeps the platform entry on name collisions and emits a `warn!`.
- A user describer's generated Cargo.toml does not contain `dbsp_adapters` or `feldera-datagen`; its `force_link.rs` does not contain the `feldera_datagen` line.
- End-to-end: empty `connectors.toml` → status returns `not_configured`, SQL compilation against built-in connectors works (platform manifest only); add a user crate → status returns `ready` with platform-merged descriptors, SQL compilation against either side works.

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
- Implements `TransportInputEndpoint` and `InputReader` inside `#[cfg(feature = "impl")] mod hello_lines_impl`.
- Registers metadata via a `pub static HELLO_LINES_META` annotated with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` at module scope (unconditional).
- Has its own `Cargo.toml` adopting the `default = ["impl"]` / `metadata = []` shape and depending only on `feldera-adapterlib-meta` (always) + `feldera-adapterlib` (impl) — proves the surface is closed and the metadata-only build path works for external crates.
- A pipeline-manager integration test enables this crate via a `connectors.toml` `path` dependency, the describer picks it up, and the test runs a pipeline that ingests from `"hello_lines"`.

The test exercises the full plugin path end-to-end: `connectors.toml` → describer → manifest → SQL parse → pipeline build → runtime dispatch. Guards against regressions where someone re-introduces a hardcoded match arm or special-case somewhere downstream.

---

## What changes per existing connector

For each in-tree connector, the diff is:

- **Add**: one `pub static <NAME>_META: ConnectorDescriptor = …;` annotated with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` (unconditional), plus a cheap `fn <name>_schema() -> serde_json::Value` (unconditional).
- **Wrap**: existing impl content in `#[cfg(feature = "impl")] pub(crate) mod <name>_impl { … }`. The build function inside becomes `pub fn build_<name>_input(…)` / `build_<name>_output(…)` so the in-tree match and pipeline-manager codegen can name it directly. **The descriptor never names the build fn.**
- **Add**: one arm to the in-tree name-keyed `match` in `transport.rs` (or `integrated.rs`), gated by the same `with-*` feature as the impl module.
- **Remove**: the typed-`TransportConfig::Foo(...)` arm from the existing factory `match`. When that deletion empties the surrounding framing, collapse it in the same PR (see Phase 4 step 5).
- **No change for regular connectors**: trait impls (`TransportInputEndpoint`, `InputReader`, `OutputEndpoint`, `Parser`, `Encoder`, …). The connector logic, threading model, FT machinery, error handling — all untouched.
- **Change for integrated-output connectors only** (`postgres`, `delta_table`): `Endpoint::new()` signature switches from accepting `Arc<ControllerInner>` to `Arc<dyn OutputControllerRef>`; stored fields move from `Weak<ControllerInner>` to `Weak<dyn OutputControllerRef>`; `controller.status.foo()` call sites become `controller.foo()`; test stubs replace dangling `Weak::<ControllerInner>::new()` with `Arc::new(NoOpControllerRef)`; criterion bench stubs (in `benches/bench_common.rs`) follow the same swap. The trait surface (`IntegratedOutputEndpoint`) is unchanged.

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

5. ✅ **Cargo-feature interaction with registry**. The metadata `pub static`s are unconditional (outside any `#[cfg]`) so they remain visible in the metadata-only build; only the `<name>_impl` mod and the in-tree match arm are gated by the same `#[cfg(feature = "with-…")]` that already gates the connector's heavy deps. Linkme picks up only the descriptors whose containing rlib is linked, so the metadata-only build (`cargo build -p dbsp_adapters --no-default-features`) sees every bundled descriptor regardless of `with-*` feature state.

6. ✅ **Describer rebuild stability**. `describer.lock` is the pin; `cargo build --locked` is enforced; `PUT /v0/connectors/connectors.toml` (auto-rebuild) and `POST /v0/connectors/refresh` (lock churn) are the only paths that update the lock. Documented in 8.4.

7. ⚠️ **Supply-chain trust**. Same model as any Rust dep. Documented in 8.9 (prefer rev pins over tags, vendor for high-security deployments, audit `[patch]`, build scripts run with deployment privileges).

8. ✅ **Compile time for large `connectors.toml`**. Worked through in 8.5. **Recipe**: shared lockfile (`describer.lock` copied as per-pipeline `Cargo.lock`), `--locked` everywhere, no per-pipeline feature variation of `dbsp_adapters` itself, per-pipeline external impl set driven by SQL `INPUT/OUTPUT FROM 'name'` walk (each external rlib's hash is identical across pipelines so sccache hits), `RUSTFLAGS` fixed at manager level, `mold`/`lld` linker. Steady-state per-pipeline cost ≈ today's cost + small linker delta. Future "platform archive" optimization remains an option if measurements demand it.

9. ✅ **Multi-tenant cache invalidation**. Per-tenant directory at `/var/lib/feldera/describer/<tenant>/<content-hash>/`; no cross-tenant sharing. Documented in 8.8.

---

## Phase summary (execution checklist)

1. **Phase 1 + Phase 2** — descriptor type + new `feldera-adapterlib-meta` crate + `default = ["impl"]` feature on `dbsp_adapters` + ABI tightening. Foundational, no behavior change.
2. **Phase 3** — format registry. Small, isolated, lands on `linkme::distributed_slice` so plugin-shaped surfaces (the new `CONNECTOR_METADATA_REGISTRY` and the format input/output registries) share one mechanism.
3. **Phase 4a** — migrate `file` and `clock` (descriptor + impl-mod + name-keyed match arms). Other connectors' typed-`TransportConfig::Foo` arms stay until 4b.
4. **Phase 5 + Phase 6** — capability flags on the descriptor; rewrite the four factory functions as name-keyed `match` over `TransportConfig::name()`. Phase 5 implicitly migrates `http_input`, `http_output`, `adhoc_input`, and `datagen` because the controller-rewrites query their descriptors.
5. **Phase 4b** — sweep the remaining bundled connectors (`s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, integrated). Each connector adds a metadata `static`, an `<name>_impl` mod, and a name-keyed match arm; its old typed arm goes away.
6. **Phase 7** — open `TransportConfig` enum. The `Plugin` variant accepts unknown names as `{name, config}`; bundled connectors keep their typed variants. No deserialization breakage for existing pipelines. (The metadata registry is available everywhere `feldera-types` is, so the typed-variant + `Plugin` split is now an implementation detail rather than the only available shape.)
7. **Phase 8** — `connectors.toml` + user describer + platform manifest (8.13) + lockfile policy. The user describer's build-dep tree is metadata-only; cold build is seconds. With an empty `connectors.toml`, behavior matches today's bundled-only deployments.
8. **Phase 9** — OpenAPI surface for `GET /v0/connectors/status`, `GET/PUT /v0/connectors/connectors.toml`, `POST /v0/connectors/refresh`.
9. **Phase 10** — web console consumes the discovery endpoint; in-tree connector forms remain.
10. **Phase 11** — reference plugin + integration test seals the contract end-to-end. The plugin demonstrates the `default = ["impl"]` / `metadata = []` feature shape that the user describer expects.

**CI gate added throughout**: `cargo build -p dbsp_adapters --no-default-features` runs on every PR. It catches anyone accidentally placing impl code outside the `impl` gate.

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

### PR 2 — `ConnectorDescriptor` + `feldera-adapterlib-meta` + metadata feature (Phase 2)

The descriptor is pure data; builder dispatch is direct (Phase 6). No `BuildInputFn` / `BuildOutputFn` / `BuildIntegratedInputFn` / `BuildIntegratedOutputFn` types and no `register_connector!` macro.

- [ ] **Move `IntegratedOutputEndpoint`** and its blanket impl from `crates/adapters/src/integrated.rs:20` to `feldera-adapterlib`, alongside `IntegratedInputEndpoint`. Update `dbsp_adapters` to import from the new location. No semantic change. Without this move, third-party integrated output connectors would have to depend on `dbsp_adapters`. **Re-export topology — get this right in PR 2, not later**: `crates/adapters/src/integrated.rs` should `use feldera_adapterlib::transport::IntegratedOutputEndpoint;` (private — just enough to bring the trait into scope for function signatures inside the module), and `crates/adapters/src/lib.rs` should `pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;` directly. **Do not** leave a `pub use` in `integrated.rs` re-exporting through to `lib.rs` — that creates a two-hop chain (`feldera_adapterlib::transport → adapters::integrated → adapters::lib`) with no consumer, and PR 7g would have to collapse it. The public path is `dbsp_adapters::IntegratedOutputEndpoint` either way; the internal topology stays clean from the start.
- [ ] **New crate `feldera-adapterlib-meta`** (~200 LOC) under `crates/adapterlib-meta/`. Deps: `serde`, `serde_json`, `linkme`. Defines `ConnectorDescriptor`, `BuilderPath`, `Direction`, `ConnectorKind`, `FtModel` (re-exported from `feldera-types` if convenient, else duplicated as a stable plugin-side enum), `ConnectorFlags`, the `#[linkme::distributed_slice] pub static CONNECTOR_METADATA_REGISTRY: [ConnectorDescriptor]` slot, and `metadata_registry()` / `descriptor_by_name()` discovery functions.
- [ ] **Re-export the descriptor types from `feldera-adapterlib`**. Plugin authors keep importing from `feldera_adapterlib::*` if they prefer one import root. `feldera-adapterlib` adds a `pub use feldera_adapterlib_meta::*;` line; the runtime trait surface is unchanged.
- [ ] **Define `ConnectorFlags` as an empty bitfield with a TODO comment** naming the flags Phase 5 will add (`HTTP_DIRECT`, `AUTO_RECREATED_ON_RESTART`). Plain `u32` newtype with `EMPTY` and `contains()` is fine here; Phase 5/PR 5 promotes it to `bitflags!`. Drop-in compatible.
- [ ] **Define the `OutputControllerRef` trait in `feldera-adapterlib`** with the four methods audited from actual call sites: `output_transport_error`, `update_output_connector_health`, `register_batch_progress_counter`, `output_buffer` (the last two flatten the `controller.status.*` indirection). The trait is **not** referenced from any descriptor field — it's a parameter to per-connector `build_*` fns (Phase 4) — but its shape is defined here so PR 4's first connector migration can adopt it. Exact signatures are recorded in `connector-plugin-refactor-notes-pr2.md`. `ControllerInner` impls the trait in PR 6.
- [ ] **`dbsp_adapters` Cargo.toml: add `default = ["impl"]`, `metadata = []`, and gate the heavy deps as `optional = true` behind `impl`** (per Phase 2 section 3 above). Existing per-connector `with-*` features become sub-features that imply `impl`. Verify that `cargo build -p dbsp_adapters` still produces an identical binary by default; verify that `cargo build -p dbsp_adapters --no-default-features` compiles in seconds with no `dbsp` / `rdkafka` / `datafusion` / `deltalake` / `tokio-postgres` / etc. in the build tree.
- [ ] **Crate-root gating in `crates/adapters/src/lib.rs`** (Phase 4 section 4): wrap `pub mod controller;`, `pub mod format;`, `pub mod adhoc;`, `pub mod static_compile;`, and `mod test;` with `#[cfg(feature = "impl")]`. Leave `pub mod transport;` and `pub mod integrated;` unconditional (their submodule declarations must remain visible in the metadata-only build so descriptors are reachable). Move `pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;` and other impl-only re-exports inside the `impl` gate.
- [ ] **CI: add a `cargo build -p dbsp_adapters --no-default-features` job** to every PR. It catches anyone accidentally placing impl code outside an `impl` gate. Pair with `cancel-if-<name>-failed` per the existing `ci.yml` pattern.
- [ ] **Do not** add `unsafe impl Sync for ConnectorDescriptor` / `Send`. Every field auto-derives `Send + Sync` by construction. Add a doc-comment invariant on the struct: *"All fields must auto-derive `Send + Sync`. Descriptors are stored in `static`s and shared across threads via `linkme::iter`; if a future field cannot auto-derive these traits, redesign the field rather than adding `unsafe impl`."*
- [ ] Add discovery API on `feldera-adapterlib-meta`: `metadata_registry()` and `descriptor_by_name()`. Both are simple — `iter()` and `iter().find()` respectively. Collision detection is the describer's / platform-manifest's job (PR 10), not the registry's.
- [ ] Unit tests: register a stub descriptor via `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]`, look it up, verify both Iterator and by-name access.
- [ ] Document a name distinct from existing per-record `ConnectorMetadata`.
- **Depends on**: PR 1.
- **Unblocks**: PR 3, PR 4.

### PR 3 — Format registries on `linkme` (Phase 3)

Both registries use `#[linkme::distributed_slice]` so plugin-shaped surfaces share one mechanism with the connector descriptor (PR 2). No `inventory::collect!` slots, no `register_input_format!` / `register_output_format!` macros.

- [ ] **Add `#[linkme::distributed_slice] pub static FORMAT_INPUT_REGISTRY: [&'static dyn InputFormat]` and `FORMAT_OUTPUT_REGISTRY: [&'static dyn OutputFormat]` in `adapterlib/src/format.rs`**, alongside the trait definitions — *not* in `adapters/src/format.rs`. External format crates can only depend on adapterlib; placing the slots in `adapters` would silently exclude them. Add discovery functions `get_input_format(name)` / `get_output_format(name)` next to the slots, implemented as `iter().find(|f| f.name() == name).copied()`.
- [ ] **Add `linkme = { workspace = true }` to `crates/adapterlib/Cargo.toml`** (the slice declaration site). Built-in formats live in `crates/adapters/`, which inherits `linkme` from PR 2's connector-descriptor work; external format crates declare `linkme` directly the same way connector plugins do (transitive through `feldera-adapterlib` is not enough — see Phase 5's note).
- [ ] Remove the `Lazy<BTreeMap>` statics (`INPUT_FORMATS`, `OUTPUT_FORMATS`) and the old `get_input_format` / `get_output_format` from `adapters/src/format.rs` entirely. Drop the `once_cell::sync::Lazy` and `BTreeMap` imports there. (`once_cell` stays in `Cargo.toml` — used elsewhere in `adapters`.)
- [ ] Verify call sites in `adapters` (`controller.rs`, `server.rs`, `test.rs`, etc.) pick up the new functions via the existing `pub use feldera_adapterlib::format::*` re-export — **expect zero call-site churn**.
- [ ] Each built-in format (`csv`, `json`, `parquet`, `avro`, `raw`) registers itself from its module:
    ```rust
    #[linkme::distributed_slice(FORMAT_INPUT_REGISTRY)]
    pub static CSV_INPUT_FORMAT: &dyn InputFormat = &CsvInputFormat;
    ```
    No macro layer — the linkme attribute is the registration form. Mirrors the connector descriptor pattern (PR 2 / Phase 2 section 3).
- [ ] Const-eval note: built-in formats are unit structs, so `&CsvInputFormat` coerces to `&dyn InputFormat` in a const initializer directly. For future formats with fields, declare a separate `static MY_FORMAT: MyFormat = MyFormat { ... };` and register `pub static MY_FORMAT_DYN: &dyn InputFormat = &MY_FORMAT;`. Closures will not compile.
- [ ] No `unsafe impl Sync` on format factories — `InputFormat: Send + Sync` makes `&'static dyn InputFormat` automatically `Sync`.
- [ ] Asymmetry: `raw` is input-only — only one registration `static` in `raw.rs`, no `RawOutputFormat`.
- [ ] Feature gating: `avro/input.rs` and `avro/output.rs` registration `static`s compile only when `with-avro` is active because the entire `avro` module sits under `#[cfg(feature = "with-avro")]` in `format.rs`. No per-`static` `#[cfg]` needed.
- [ ] **`StubInputFormat` / `StubOutputFormat` test fixtures** in `adapterlib/src/format.rs`'s `#[cfg(test)] mod tests` register through the same linkme slice attribute, exercising the slot mechanism end-to-end.
- [ ] Cleanup: remove the now-unused `pub use input::JsonInputFormat` and `pub use output::JsonOutputFormat` re-exports in `format/json.rs` (they only existed to feed the old BTreeMap; the compiler will flag them as unused). Aligns `json.rs` with `csv.rs` / `parquet.rs` / `raw.rs`.
- [ ] Add unit tests in `adapterlib/src/format.rs` covering registry mechanics with stub format types (iterate, look up by name, miss on unknown). The built-in format names are NOT visible to `cargo test -p feldera-adapterlib` (link-time discovery), which is correct.
- [ ] Add a follow-up test in `adapters/src/format.rs` (or a dedicated test file) that asserts every expected built-in name (`csv`, `json`, `parquet`, `raw`, plus `avro` under feature) is present in the registry. Guards against accidental removal of a registration `static`.
- [ ] Remove the TODO comment about runtime registration in `adapters/src/format.rs`.
- [ ] Verify all format-using tests still pass.
- **Depends on**: PR 2.
- **Unblocks**: nothing strictly (proves the linkme pattern across both registries; the pure-attribute registration form also becomes the model for the format-side of the Phase 11 reference plugin doc and for the future external-format-plugin work in `format-plugin-design.md`).

### PR 4 — First connector migrations: `file` and `clock` (Phase 4a)

Dispatch goes through a name-keyed `match` on `TransportConfig::name()` calling each connector's `<name>_impl::build_<name>_*` directly. The descriptor carries metadata only.

- [ ] Add `FILE_INPUT_META` + `FILE_OUTPUT_META` `pub static`s annotated with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` in `crates/adapters/src/transport/file.rs`. Unconditional (no `#[cfg]`). Use `Direction::Input` and `Direction::Output`; `name = "file_input"` / `"file_output"`.
- [ ] Add `CLOCK_META` `pub static` annotated with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` in `crates/adapters/src/transport/clock.rs`. `kind: ConnectorKind::Transient`. `name = "clock"` (not `"clock_input"` — the serde tag and `name()` diverge; the in-tree match keys on `name()`).
- [ ] **Move existing impl content into `#[cfg(feature = "impl")] pub(crate) mod file_impl { … }`** in `file.rs`, exposing `pub fn build_file_input(cfg: &JsonValue, ctx: &InputCtx) -> …` and `pub fn build_file_output(...)`. Same wrapping in `clock.rs`'s `clock_impl` mod with `pub fn build_clock_input(...)`.
- [ ] `config_schema` placeholder: `fn() -> JsonValue::Object(Default::default())` for now. Real JSON Schema lands in PR 14 alongside the discovery endpoint; an empty object is correct until then because nothing reads `config_schema` before Phase 9.
- [ ] **Add `transport_config_inner_as_json` helper in `crates/adapters/src/transport.rs`** (NOT in adapterlib — it depends on `TransportConfig`). Serialises the whole `TransportConfig` to `{"name", "config"}` and returns the `"config"` field; for unit variants (e.g. `HttpOutput`) with no content, return `JsonValue::Null`. PR 6 promotes it to `pub(crate)` so `integrated.rs` can call it.
- [ ] **Introduce the name-keyed dispatch in both factory functions** (`input_transport_config_to_endpoint` and `output_transport_config_to_endpoint`) ahead of the existing typed-variant match. Pattern:
   ```rust
   let (name, cfg_json) = transport_config_inner_as_json(config);
   let cfg_json = resolve_secrets(...);
   let endpoint = match name.as_str() {
       "file_input"  => Some(file::file_impl::build_file_input(&cfg_json, ctx)?),
       "clock"       => Some(clock::clock_impl::build_clock_input(&cfg_json, ctx)?),
       _ => None,
   };
   if let Some(ep) = endpoint { return Ok(Some(ep)); }
   // … fall through to legacy typed match for unmigrated connectors
   ```
- [ ] **Delete the `FileInput` / `ClockInput` typed-variant arms** from the input factory's typed match (their replacements are in the new name-keyed match above). **Delete the `FileOutput` arm entirely** (the output match's `_ => Ok(None)` already covers it).
- [ ] Remove now-unused imports: `use clock::ClockEndpoint`, `use crate::transport::file::{FileInputEndpoint, FileOutputEndpoint}` from `transport.rs`. The build fns in `<name>_impl` mods construct the endpoints directly.
- [ ] Verify the existing tests `transport::file::test::{test_csv_file_nofollow, test_csv_file_follow}` and `transport::clock::test::test_clock` pass — they call `mock_input_pipeline`, which exercises the full factory dispatch path.
- [ ] Add a unit test per migrated connector that resolves the metadata via `descriptor_by_name()` and asserts `direction` / `kind`. Tests live alongside the connector module (e.g. `file.rs::test::file_input_descriptor`, `clock.rs::test::clock_descriptor`) and must run in the default build with no external dependencies.
- [ ] Verify `cargo build -p dbsp_adapters --no-default-features` still succeeds after this PR — descriptor `static`s and schema fns must compile without the impl side.
- [ ] Document the migration recipe in a short developer note for use in PR 7+. Include the descriptor-name-vs-serde-tag rule and the `<name>_impl` mod-wrapping pattern.
- **Depends on**: PR 2.
- **Unblocks**: PR 5, PR 6.

### PR 5 — Capability fields on descriptor + controller refactor (Phase 5)
- [ ] Convert `ConnectorFlags` from the PR 2 plain-`u32` placeholder to `bitflags!`. Add `bitflags = "2"` **directly to `crates/adapterlib/Cargo.toml`** (NOT to the workspace `[dependencies]` table — `bitflags` is not currently a workspace dep; single-crate dep is simpler until a second crate needs it). Keep `EMPTY` as `Self::empty()` const for source compatibility; `contains()` is drop-in compatible. No call-site changes outside the struct definition.
- [ ] Add concrete flag constants with stable bit values: `HTTP_DIRECT = 0x01`, `AUTO_RECREATED_ON_RESTART = 0x02`. Future flags use the next available bits; document the bit layout in source.
- [ ] Wire `default_format` field on descriptor (already declared in PR 2).
- [ ] **Register metadata `static`s for the four connectors the controller-rewrites depend on**: `http_input` (`Transient` kind, `HTTP_DIRECT` flag), `http_output` (`Transient` kind, no flags — the HTTP server constructs `HttpOutputEndpoint` directly at request time; the metadata exists purely for `kind`/`flags` lookups), `adhoc_input` (`Transient` kind), `datagen` (`Regular` kind, `default_format: Some(datagen_default_format)`). The other unmigrated connectors (`s3`, `url`, `nats`, `pubsub`, `redis`, `kafka`, `nexmark`, integrated) do NOT need metadata yet — none of the four controller rewrites query them. Phase 4b sweeps them later.
- [ ] **Wrap http/adhoc/datagen impl content in `<name>_impl` mods** (gated by `feature = "impl"`) and add an arm to the in-tree name-keyed match in `transport.rs` for each (`"http_input"`, `"adhoc_input"`, `"datagen"`). `http_output`'s metadata is descriptor-only — there is no `build_http_output` because the server constructs the endpoint directly. Document this metadata-only-no-builder pattern; the in-tree match has no `"http_output"` arm and the catch arm returns `Ok(None)` for it.
- [ ] **Add `linkme = { workspace = true }` to `crates/datagen/Cargo.toml`**. `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` requires `linkme` as a direct dep of the annotating crate — transitive through `feldera-adapterlib-meta` is not enough. Any future per-connector crate must do the same.
- [ ] **No `extern crate feldera_datagen as _;` in `crates/adapters/src/lib.rs`.** linkme references through `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` keep the datagen rlib alive on their own — the platform-manifest crate's build.rs and pipeline-manager codegen reach the slice by name.
- [ ] Move the JSON-Datagen format spec from `controller.rs` into `datagen/src/lib.rs::datagen_default_format()`. Remove the now-unused `use feldera_types::format::json::{JsonFlavor, JsonParserConfig, JsonUpdateFormat, JsonLines}` imports from `controller.rs`.
- [ ] Replace `transport.is_transient()` callers in `controller/pipeline_diff.rs:97,106` with descriptor lookups (`connector_by_name(&transport.name()).map(|d| d.kind == ConnectorKind::Transient).unwrap_or(false)`).
- [ ] Replace `transport.is_http_input()` at `controller.rs:4448` with `descriptor.flags.contains(HTTP_DIRECT)`.
- [ ] Replace `match TransportConfig::ClockInput(_)` filter at `controller.rs:4521` with `AUTO_RECREATED_ON_RESTART` check.
- [ ] Replace `match (TransportConfig::Datagen(_), None)` default-format injection at `controller.rs:6013` with `descriptor.default_format()`. The error message changes from `"datagen endpoints do not support custom formats"` to `"{name} endpoints do not support custom formats"`; update any test that asserts on the literal `"datagen"` substring.
- [ ] **Delete the `HttpInput`/`AdHocInput`/`Datagen` typed-variant arms from the input factory's typed match** (replacements live in the new name-keyed match introduced in PR 4). `HttpOutput` is metadata-only — its typed-variant arm in the output factory stays unchanged for now (the typed match still uses it; the name-keyed match has no arm for `"http_output"`).
- [ ] Remove now-unused `use adhoc::AdHocInputEndpoint`, `use http::HttpInputEndpoint`, `use feldera_datagen::GeneratorEndpoint` imports from `transport.rs`.
- [ ] Document the **registry-completeness invariant**: every transient connector and every connector whose flags the controller queries MUST register a descriptor. Lookups silently degrade to `false`/`None` for unregistered connectors. Removing `TransportConfig::is_transient()` is gated on verifying all four transient connectors are registered (done in this PR).
- [ ] Add CI lint that fails if any `match TransportConfig::*` arm appears in `controller.rs` outside the four dispatch sites.
- [ ] Add `descriptor_by_name` tests for the four connectors registered here (`http_input`, `http_output`, `adhoc_input`, `datagen`), alongside their modules, in the default build. Same pattern as PR 4's tests for `file`/`clock`.
- [ ] Verify `cargo build -p dbsp_adapters --no-default-features` continues to succeed.
- [ ] Verify `connector_flags_contains` test, file/clock transport tests (3), and format tests (87) still pass.
- **Depends on**: PR 4.
- **Unblocks**: PR 6.

### PR 6 — Name-keyed dispatch in integrated factories + `OutputControllerRef` impl (Phase 6)

Dispatch in the integrated factories is a hand-written name-keyed `match` over `TransportConfig::name()` calling each connector's build fn directly. PR 4 wires this for the transport factories; PR 6 mirrors it for the integrated factories.

- [ ] Add the same name-keyed match pattern from PR 4 to the integrated factories (`create_integrated_input_endpoint` / `create_integrated_output_endpoint`). Pattern: resolve secrets, extract `(name, cfg_json)` via `transport_config_inner_as_json`, `match name.as_str() { "postgres_input" => …, _ => Err(unknown_*_transport(name)) }`. The match has no arms in this PR (no integrated connectors are migrated yet); PR 7f populates it.
- [ ] **Promote `transport_config_inner_as_json` to `pub(crate) fn`** in `crates/adapters/src/transport.rs` so `integrated.rs` can call it. Do not promote to `pub` — keep crate-private.
- [ ] **Add `impl feldera_adapterlib::connector::OutputControllerRef for ControllerInner`** in `controller.rs`. Required at compile time so future arms in the integrated output match can pass `Arc<dyn OutputControllerRef>` to per-connector build fns. Methods delegate to existing inherent methods (`output_transport_error`, `update_output_connector_health`) and `self.status.*` methods (`register_batch_progress_counter`, `output_buffer`). For the two name-colliding methods, write `ControllerInner::output_transport_error(self, …)` (UFCS) rather than `self.output_transport_error(…)` to avoid the trait method shadowing the inherent one.
- [ ] In the integrated output factory, `controller.upgrade()` failure maps to `ControllerError::invalid_transport_configuration` with a "controller dropped" message. Acceptable placeholder; the path is dead code until PR 7f.
- [ ] The format-check (`if connector_config.format.is_some()`) sits once **before** the name-keyed match. Single-site (no fallback match in the integrated factories anymore — the catch arm is `Err(unknown_*_transport(...))`). Future integrated connectors that *do* accept formats can opt into a per-arm exception.
- [ ] Verify `cargo build -p dbsp_adapters` succeeds and existing test suites (transport, format, adhoc) still pass. After this PR: registered set unchanged from PR 5; the integrated factories' name-keyed match exists with no arms.
- [ ] Verify `cargo build -p dbsp_adapters --no-default-features` continues to succeed.
- **Depends on**: PR 5.
- **Unblocks**: PR 7a–7g.

### PR 7a–7g — Sweep remaining bundled connectors, then clean up (Phase 4b)

Each per-connector migration adds a `pub static <NAME>_META` annotated with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]`, wraps the impl in `#[cfg(feature = "impl")] pub(crate) mod <name>_impl { … }`, adds an arm to the in-tree name-keyed `match` in `transport.rs` / `integrated.rs`, and removes the typed-`TransportConfig::Foo(...)` arm.

PRs 7a–7f follow Phase 4 (steps 1–10): add the metadata `static`, wrap impl in `<name>_impl` mod, add the name-keyed match arm, remove the typed-variant arm (and `#[cfg(not(feature))]` stub for feature-gated connectors); when removing the last typed-variant arm, collapse the match shell in the same PR rather than carrying an `#[allow(unreachable_code)]`; remove now-dead wrapper-module re-exports; for integrated-output connectors, `new()` takes `Arc<dyn OutputControllerRef>` and test stubs use `Arc::new(NoOpControllerRef)`; add a default-build `descriptor_by_name` test alongside the connector module; verify existing tests pass and `cargo build -p dbsp_adapters --no-default-features` continues to succeed. PR 7g is the post-migration cleanup. Group as small PRs to keep blast radius low. **Note**: `http`, `adhoc`, and `datagen` are migrated in PR 5 as prerequisites for the controller-rewrites; this sweep covers the connectors that PR 5 does not touch.
- [ ] **PR 7a**: `s3` + `url`.
- [ ] **PR 7b**: `nats` + `pubsub` (each behind its `with-*` feature gate). Delete both the `#[cfg(feature)]` arm and the `#[cfg(not(feature))]` stub for each; delete dead `pub use input::{NatsInputEndpoint,PubSubInputEndpoint}` re-exports in `nats.rs` / `pubsub.rs`. Verify the descriptor `name` matches `TransportConfig::name()` exactly — `pub_sub_input` (three-word split), not `pubsub_input`.
- [ ] **PR 7c**: `redis` (output only); first regular connector with `Direction::Output`. Behind `with-redis`; same feature-gate-arm cleanup as PR 7b. No wrapper re-export to delete (`redis.rs` only declares `pub mod output;`). `build_output` binds `_fault_tolerant` (redis ignores it; `is_fault_tolerant()` is unconditionally `false`).
- [ ] **PR 7d**: `kafka` — behind `with-kafka`; same feature-gate-arm cleanup as PR 7b. Register **two** metadata `static`s: `KAFKA_INPUT_META` (name=`"kafka_input"`, `Direction::Input`) and `KAFKA_OUTPUT_META` (name=`"kafka_output"`, `Direction::Output`). `kafka_impl::build_kafka_output` receives `fault_tolerant: bool` and branches between `KafkaFtOutputEndpoint` and `KafkaOutputEndpoint`. The existing `pub use ft::{KafkaFtInputEndpoint, KafkaFtOutputEndpoint}` and `pub use nonft::KafkaOutputEndpoint` re-exports in `kafka.rs` stay alive because `kafka_impl`'s build fns consume them in-module — only the corresponding `use` lines in `transport.rs` are deleted. `build_*` signatures bind `_secrets_dir` (the factory resolves secret references via `resolve_secret_references_via_json` before calling `build_*`; the build function never sees credential file paths). After this PR lands, the output factory's typed-variant match shrinks to `_ => Ok(None)` (redis migrated in 7c, kafka here) — leave the empty shell in place; PR 7g strips it.
- [ ] **PR 7e**: `nexmark` — behind `with-nexmark`. `ConnectorKind::Regular`, **not** `Transient`: nexmark implements `fault_tolerance() → Some(FtModel::ExactlyOnce)` and participates in checkpoint/replay. **Metadata gating**: nexmark's `mod nexmark;` declaration in `transport.rs` is itself behind `#[cfg(feature = "with-nexmark")]`, which means the `NEXMARK_META` `pub static` is also gated by that feature — the `cargo build -p dbsp_adapters --no-default-features` job builds with no `with-nexmark`, and the descriptor is correctly absent. With the feature off, the in-tree match's `"nexmark"` arm is also `#[cfg]`-gated and the catch arm handles the lookup. No wrapper re-exports to delete; only the `use crate::transport::nexmark::NexmarkEndpoint` line in `transport.rs` is removed. After this PR lands, the input factory's typed-variant match shell contains only `_ => Ok(None)` — collapse it in this PR; PR 7g's input-fallback-strip work is pre-empted.
- [ ] **PR 7f**: integrated — `postgres` (reader, writer), `postgres-cdc`, `delta_table`, `iceberg`. Each behind its `with-*` feature gate; same feature-gate cleanup. Wires `IntegratedOutputEndpoint`/`IntegratedInputEndpoint` builds; this PR populates the integrated factories' name-keyed match (introduced empty in PR 6). **Output constructor signatures**: `DeltaTableWriter::new()` and `PostgresOutputEndpoint::new()` accept `Arc<dyn OutputControllerRef>` and call `Arc::downgrade(&controller)` internally — there is no Any-based downcast path from `Arc<dyn OutputControllerRef>` back to `Arc<ControllerInner>`. Stored field types are `Weak<dyn OutputControllerRef>` (`DeltaTableWriterInner.controller`, `PostgresWorker.controller`, `PostgresOutputEndpoint.controller`). Field accesses like `controller.status.register_batch_progress_counter(…)` and `controller.status.output_buffer(…)` are trait calls (`controller.register_batch_progress_counter(…)`, `controller.output_buffer(…)`) — the `OutputControllerRef` impl on `ControllerInner` (PR 6) delegates to `self.status.*`. `ControllerInner` is not imported by `delta_table/output.rs` or `postgres/output.rs`. **Test stubs**: a `NoOpControllerRef` stub struct in each test module implements the four `OutputControllerRef` methods as no-ops; `Arc::new(NoOpControllerRef)` flows into constructors. In `postgres/output.rs` the nested `tests::parallel` submodule needs `use super::NoOpControllerRef;`. **Bench call-sites — bench-safety contract**: PR 7f changes a public constructor signature, so the migration must keep `cargo build --all-features --benches` green at the PR's commit boundary. Place a single `NoOpControllerRef` in `benches/bench_common.rs` with `#[allow(dead_code)]` and pass `Arc::new(NoOpControllerRef)` at both bench `::new(...)` call-sites. **iceberg shim**: do NOT add `linkme` as a dependency to `feldera-iceberg` (separate crate, unwanted coupling). Instead, place the metadata `static` in a new `crates/adapters/src/integrated/iceberg.rs` shim gated by `#[cfg(feature = "with-iceberg")]` whose `iceberg_impl::build_*` calls `feldera_iceberg::IcebergInputEndpoint::new(...)`. **postgres-cdc gating**: the metadata `static` for the CDC connector lives in `postgres.rs` next to the regular postgres metadata (both unconditional); the cdc impl branch lives inside `postgres_impl` gated by an inner `#[cfg(feature = "with-postgres-cdc")]` and the in-tree match arm is similarly gated. **Match collapse**: after migrating all 6 connectors, both integrated factories have `unknown_*_transport(...)` as their catch arm and a populated name-keyed match — no typed-variant arms remain.
- [ ] **PR 7g — final cleanup** (one PR, two commits): with all bundled connectors registry-driven after 7f, strip the post-migration leftovers. The two commits share the "PR 7 swept the connectors, now sweep the scaffolding" theme; splitting into separate top-level PRs would be needless review overhead. **Bench-safety invariant** (every commit boundary, including each commit within 7g): `cargo build --all-features --benches` succeeds. PR 7g leaves bench-driven exposure (`pub use crate::integrated::postgres::PostgresOutputEndpoint`, `pub mod delta_table`) untouched — those go in PR 7h, which first migrates benches off concrete types.
  - **Commit 1 — strip dead match arms**: most of this work has already happened during PRs 7d (output collapsed after kafka), 7e (input collapsed after nexmark), and 7f (both integrated factories collapsed to direct `Err` returns). This commit deletes the now-empty `match` shells in `input_transport_config_to_endpoint` and `output_transport_config_to_endpoint` (returning `Ok(None)` directly). Also remove `#[allow(unused_variables)]` from `input_transport_config_to_endpoint` (still present in `transport.rs` after PR 7f) — with the match gone, every parameter is either consumed by the registry path or by the `Ok(None)` return. Drop any unused `Encoder` / `OutputEndpoint` imports from `integrated.rs` while you're there. (PR 2 already chose the clean re-export topology — `lib.rs` imports `IntegratedOutputEndpoint` directly from `feldera_adapterlib::transport` and `integrated.rs` keeps it as a private `use` — so there is no chain to collapse here.)
  - **Commit 2 — `#[doc(hidden)]` removal**: integrated connectors are now reachable from out-of-tree code via the descriptor registry, so `IntegratedInputEndpoint`, `IntegratedOutputEndpoint`, `InputCollectionHandle` (struct), `InputCollectionHandle::new()` (the constructor — independently annotated), and `OutputConsumer` become first-class plugin ABI; remove `#[doc(hidden)]` from each. **Doc-comment requirement**: each newly-public type needs a real doc comment (one short sentence summarising its role) to satisfy the missing-doc lint. **`lib.rs` ABI overview text edits**: `crates/adapterlib/src/lib.rs`'s module-level doc comment mentions `(\`#[doc(hidden)]\`)` annotations on four lines (the introductory paragraph explaining the carve-out, plus three per-type bullets in the "Supported plugin-facing types" lists). Rewrite the introductory paragraph and drop the inline annotations on the bullets. Any inline comments next to the attribute go with it.
- [ ] **PR 7h — bench migration + concrete-type removal** (lands after 7g, two commits):
  - **Commit 1 — migrate benches to direct builder imports**: rewrite `benches/postgres_output.rs` and `benches/delta_encoder.rs` to construct endpoints via plain `use dbsp_adapters::integrated::postgres::postgres_impl::build_postgres_output;` (and analogous for delta) — no registry indirection. Pass `serde_json::to_value(&config)?` where `config` is the raw `PostgresWriterConfig` / `DeltaTableWriterConfig` (the build fn deserialises via `serde_json::from_value::<InnerConfig>(...)` internally). Pass `Arc::new(NoOpControllerRef)` for the controller param. Drop `use dbsp_adapters::Encoder` and any `OutputEndpoint` supertrait imports (trait-object dispatch reaches supertrait methods through the vtable; Rust's `unused_imports` lint will flag the leftover imports). Helper signatures change from `&mut PostgresOutputEndpoint` to `&mut dyn IntegratedOutputEndpoint`; call sites need an explicit reborrow: `bench_encode_iter(&mut *endpoint, ...)` — `&mut Box<dyn T>` does not coerce to `&mut dyn T` in function arguments (only in method-call autoderef). Optional consistency call: route the remaining `dbsp_adapters::SerBatch` import through `feldera_adapterlib::catalog::SerBatch` so all bench imports flow through `feldera_adapterlib`.
  - **Commit 2 — remove now-dead concrete-type exposure**: before editing, audit each removal target with `rg` outside the relevant module subtree to confirm the bench is the sole external consumer (both `pub use crate::integrated::postgres::PostgresOutputEndpoint` and the `pub` qualifier on `mod delta_table` are bench-driven exposures; the audit should return zero non-bench non-internal hits). Then remove `pub use crate::integrated::postgres::PostgresOutputEndpoint;` from `integrated.rs` and change `pub mod delta_table;` to `mod delta_table;`. Internal callers under `crates/adapters/src/integrated/delta_table/` reach helpers via `crate::integrated::delta_table::*` paths, which work identically through a private module — no further changes needed inside the subtree. Re-run `cargo build --all-features --benches` to confirm. **Follow-up out of scope for 7h**: with `delta_table` private, the `pub` qualifiers on `register_storage_handlers` and `delta_input_serde_config` (defined at the `delta_table.rs` module root) become orphaned — they're used only by sibling files within the now-private module. A future cleanup PR can downgrade to `pub(super)` or drop `pub` entirely.
  - **Why split from 7g**: 7g is *cleanup* (strip dead match shells, surface previously-`#[doc(hidden)]` types). 7h is *behavior-equivalent rewrite* of two test harnesses — a different review profile (correctness of inner-config-JSON synthesis, equivalence of trait-call vs concrete-call paths, coercion subtleties around `&mut Box<dyn T>`). Folding them would conflate the two review profiles. The two commits in 7h must stay in order: commit 2's removals are only safe after commit 1's migration.
- **Depends on**: PR 6 (PRs 7a–7f are independent of each other; PR 7g depends on 7f having landed).
- **Unblocks**: PR 8.

### PR 8 — Open the `TransportConfig` enum (Phase 7)
- [ ] Add `Plugin(PluginTransportConfig)` variant in `crates/feldera-types/src/config.rs:1609`.
- [ ] **Delete the `#[serde(tag = "name", content = "config", rename_all = "snake_case")]` attribute** from the enum. `#[serde(...)]` is a derive helper attribute injected by the serde proc-macro; it cannot be retained alongside manual `Serialize`/`Deserialize` impls (the compiler errors with `cannot find attribute 'serde' in this scope`). Implement **both `Serialize` and `Deserialize` manually** — the derive cannot be partially retained.
- [ ] **`HttpOutput` is a unit variant**: its serialization must emit `{"name":"http_output"}` with **no `"config"` key** (use `serialize_map(Some(1))`). Every other variant uses `Some(2)` and emits `{"name":..., "config":...}`. The `Plugin` arm forwards the stored `name`/`config` directly without re-nesting. Add an explicit test asserting `HttpOutput`'s serialized form contains no `config` field — silently emitting `"config": null` would break compatibility with the existing wire format.
- [ ] Manual `Deserialize`: parse a `{name, config}` envelope, then dispatch on `name` to `serde_json::from_value::<TypedConfig>(config)` for known names; route unknown names to `Plugin { name, config }`. The intermediate `serde_json::Value` allocation is acceptable (`TransportConfig` deserializes once at pipeline startup, not on hot paths).
- [ ] **`ToSchema` schema regression (known limitation)**: removing `#[serde(tag, content)]` strips the adjacently-tagged hint that `utoipa::ToSchema` consumed for OpenAPI generation. The generated schema for `TransportConfig` will be less precise (likely `oneOf` of inline variant structs). Accept as placeholder; PR 14's `GET /v0/connectors/status` `descriptors` field is the designated owner of the connector schema surface and will replace this.
- [ ] Update `name()` (`config.rs:1638-1662`) to handle the `Plugin` arm (returns `p.name.clone()`).
- [ ] Remove `is_transient()` and `is_http_input()` helpers (already replaced by descriptor lookups in PR 5).
- [ ] **Audit every exhaustive `match TransportConfig` in the workspace** (`rg 'match.*TransportConfig'`). Most matches use `_` wildcards, `if let`, or `matches!` and require **no code change** — Rust's exhaustiveness check only fires on `match` arms without a catch-all. The only file that needs explicit `Plugin` arms is `crates/pipeline-manager/src/db/types/program.rs` at the input/output validation matches (lines 682, 735): both already have `_ =>` arms, but those return the misleading `ExpectedInputConnector`/`ExpectedOutputConnector` errors. Add explicit `TransportConfig::Plugin(p) => Err(ConnectorGenerationError::UnknownConnector { ..., name: p.name.clone() })` arms ahead of the `_` arm at each site.
- [ ] **Add `UnknownConnector` to `ConnectorGenerationError`** (in `program.rs`). Update `new_from_connector_generation_error` (`program.rs:127`) — that function exhaustively matches the error enum without a `_` arm, so the compiler forces the new branch.
- [ ] Tests: serde round-trip for every known variant (byte-for-byte unchanged); `HttpOutput` serialized form contains no `config` field; unknown name routes to `Plugin`; a `Plugin` config submitted at the SQL boundary surfaces `UnknownConnector` (not a panic) until PR 11 lands. The `program.rs` test for `UnknownConnector` constructs the error variant with plain strings and **does not** need `use feldera_types::config::PluginTransportConfig;` — leaving such an import will trip `unused_imports` and force a follow-up cleanup PR.
- [ ] Verify the existing stored-pipeline-configuration fixtures deserialize identically (include `HttpOutput` explicitly — it's the unit-variant edge case).
- [ ] **What this PR enables**: `TransportConfig::Plugin` configs round-trip through serde without a JSON parse error on unknown names. **What it does not**: pipeline SQL referencing a plugin connector still fails at SQL compilation with `UnknownConnector` until PR 11 wires manifest-based direction validation. Bundled in-tree connectors are unaffected — they hit their existing typed match arms and remain fully operational; only plugin connectors transition from rejected → accepted at PR 11.
- **Depends on**: PR 7g (all bundled connectors registry-driven AND cleanup landed; the `Plugin` variant is purely an extension point at this point — no risk of dead match arms colliding with the new `Plugin` arm being added).
- **Unblocks**: PR 9.

### PR 9 — `tenant_connector_config` schema + bootstrap seed (Phase 8.1)
- [ ] **Document the `connectors.toml` blob format** (concrete example in 8.1): one Cargo dep per line, **no section header**, mirroring the `udf_toml` shape at `rust_compiler.rs:1336-1346`. Each line is `<key> = <cargo-dep-spec>`. Multi-line `[dependencies.<name>]` table form is **not** supported — line-based force-link extraction in PR 10 assumes one dep per line.
- [ ] **DB migration**: add `tenant_connector_config(tenant_id, content, content_hash, version, edited_at, edited_by)` per the schema sketch in 8.1. `content_hash = sha256(content)` is recomputed on every write. `version` is monotonic. `tenant_id` is the primary key (one row per tenant). Migration creates the empty table — rows are inserted lazily on first read or on tenant creation (see bootstrap below). **Footgun**: declare timestamp columns as `TIMESTAMPTZ` (not `TIMESTAMP`) — `chrono::DateTime<Utc>` requires timezone-aware columns; a plain `TIMESTAMP` binds silently during migration but fails at runtime when sqlx tries to decode the value.
- [ ] **Add a single `connectors_toml_path: Option<String>` field to `CompilerConfig`** — bootstrap seed only. Per-tenant variants live in the DB, not on the filesystem.
- [ ] **Update every `CompilerConfig { ... }` struct-literal site**. `CompilerConfig` has no `Default` impl; the compiler enforces this at the same five call sites as before — `compiler/test.rs` (×1), `compiler/main.rs` (×3), `compiler/rust_compiler.rs` (×1; the sibling site uses `..config`). Set the field to `None` in tests.
- [ ] **Place the bootstrap loader at `crates/pipeline-manager/src/compiler/connectors.rs`** with `pub mod connectors;` declared in `compiler.rs`. The loader is `fn load_bootstrap_seed(config: &CompilerConfig) -> io::Result<Option<String>>` — returns `Ok(None)` when `connectors_toml_path` is unset; otherwise reads the file with synchronous `std::fs::read_to_string` (15+ unit tests stay as plain `#[test]`).
- [ ] **Bootstrap rule**: on tenant creation (and as a one-shot migration for existing tenants when the field is first set), if no `tenant_connector_config` row exists for the tenant *and* `connectors_toml_path` is set, insert a row with `content` = file contents, `version = 1`. After that, the file is **never re-read** for that tenant — DB is authoritative. If the field is unset, insert a row with empty `content`. Bootstrap is idempotent (uses `INSERT … ON CONFLICT DO NOTHING`).
- [ ] **Wrap reads in a `ConnectorsTomlContent` newtype** with `as_str(&self) -> &str` and `is_empty(&self) -> bool` helpers. PR 10 uses `is_empty()` to decide whether to splice a deps section into `Cargo.toml` and `as_str()` for the splice itself.
- [ ] **DB-access functions**: `tenant_connector_config_get(tenant_id) -> Option<Row>`, `tenant_connector_config_put(tenant_id, content, edited_by, expected_hash) -> Result<Row, OptimisticConcurrencyError>` (`expected_hash` mismatch maps to a 412 in PR 10). Empty `content` is valid (bundled-only).
- [ ] Tests: bootstrap inserts a row from the seed file on first tenant access; second access returns the DB row even after the file changes; tenants without bootstrap field get an empty-content row; PUT with stale `expected_hash` returns the concurrency error; arbitrary Cargo spec shapes (plain version, git+rev inline table, path inline table, features array) round-trip verbatim through DB storage.
- **Depends on**: PR 8.
- **Unblocks**: PR 10.

### PR 10 — User describer binary + platform manifest + DB-backed config endpoints (Phase 8.2-8.4, 8.12, 8.13)

The platform manifest's build-deps build with `default-features = false` (metadata-only). No `force_link.rs` codegen anywhere. Per-pipeline `[dependencies]` lists only the external connector crates the SQL actually references; pipeline-manager codegens a `connector_dispatch.rs` `match` per pipeline.

- [ ] **New workspace crate `crates/platform-manifest/`** (`feldera-platform-manifest`). Library only; runtime `[dependencies]` is empty. `[build-dependencies]`:
    ```toml
    feldera-adapterlib-meta = { workspace = true }
    dbsp_adapters           = { path = "../adapters", default-features = false }
    feldera-datagen         = { workspace = true,     default-features = false }
    serde_json              = { workspace = true }
    ```
    `build.rs` references `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` (linkme keeps the rlibs alive — no `extern crate _;` lines), walks `metadata_registry()`, serializes to `$OUT_DIR/platform_manifest.json`, panics on duplicate built-in names. `lib.rs` exposes a single `pub const PLATFORM_MANIFEST_JSON: &str = include_str!(concat!(env!("OUT_DIR"), "/platform_manifest.json"));`.
- [ ] **Add `default-features = false` to `feldera-datagen`'s metadata-only build path**, ensuring `feldera-datagen` itself adopts a `default = ["impl"]` / `metadata = []` feature shape symmetric with `dbsp_adapters`. (Datagen lives in its own crate with its own dep tree — give it the same metadata/impl split so the platform-manifest build pulls in only the descriptor + cheap deps.) Verify that `cargo build -p feldera-datagen --no-default-features` compiles in seconds.
- [ ] Add `feldera-platform-manifest = { workspace = true }` to `pipeline-manager`'s runtime `[dependencies]`. The crate exposes a `&'static str`; pipeline-manager does not inherit the data-plane dep tree at runtime.
- [ ] **`load_platform_manifest() -> HashMap<...>`** parses `feldera_platform_manifest::PLATFORM_MANIFEST_JSON`. No `config` parameter — there is nothing on disk to consult. On parse failure (a programming error in build.rs), fall back to `build_in_process_manifest()` and `warn!`.
- [ ] **`merge_manifests(platform, user) -> HashMap<...>`** — platform wins on name collisions, `warn!`s the user-side shadow.
- [ ] **Wire the merge into every manifest consumer**: `attempt_end_to_end_sql_compilation` (in `sql_compiler.rs`), `compiler_precompile` (in `compiler/main.rs`), `GET /v0/connectors/status` `descriptors` field (in `api/endpoints/connectors.rs::build_status_envelope`). The `NotConfigured` / cache-miss path uses `load_platform_manifest()` alone; the `Ready` path merges the user JSON on top.
- [ ] **No `force_link.rs` generation.** linkme references through `feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY` keep contributing rlibs alive. Codegen-emitted `connector_dispatch.rs` references each external connector's `<crate>::build_*` symbol by name, which keeps that crate's rlib alive. There is nothing to generate.
- [ ] Generate the **user** describer crate (path `<working_dir>/describer/<tenant_id>/<cache_key>/`) on first use per tenant; its `Cargo.toml` lists `feldera-adapterlib-meta` + every entry from the tenant's `tenant_connector_config.content`. `dbsp_adapters` and `feldera-datagen` are not declared — those live in the platform manifest. External plugin crates listed by the tenant should be referenced with `default-features = false` (the convention documented in PR 16's "Writing a connector" page); plugins that don't follow the convention compile their full data-plane tree as a one-time cost when the user describer first sees them.
- [ ] **Built-ins registered from a separate crate hardcode `crate_name: "dbsp_adapters"`** (not `env!("CARGO_CRATE_NAME")`), since per-pipeline globals only depend on `dbsp_adapters` and the codegen calls `<crate_name>::build_<slot>_<name>`. Today only `feldera-datagen` is affected; descriptors inside `crates/adapters/` resolve `env!` to `dbsp_adapters` already.
- [ ] **Per-pipeline `connector_dispatch.rs` codegen** (extends `prepare_workspace` at `rust_compiler.rs:1242`). For each `INPUT/OUTPUT FROM 'name'` clause in the SQL program, look up the merged manifest entry, recover the `BuilderPath` (or default `<crate>::build_*` convention), and emit a `match` arm. The generated file lives in the per-pipeline globals crate (`<pipeline>_globals/src/connector_dispatch.rs`) with `pub mod connector_dispatch;` declared in the globals `lib.rs`. Add `("connector_dispatch.rs", true)` to the `src_content` list at `rust_compiler.rs:1435-1448` so `DirectoryContent::validate()` does not reject the injected file. **Footgun: `mod connector_dispatch;` cannot be naively prepended at line 0 of the SQL-compiler-generated `lib.rs`** — that file starts with a block of inner attributes (`#![allow(dead_code)]`, etc.) that rustc rejects when displaced from the top. Splice the `mod` declaration *after* the leading prelude, not at the very top.
- [ ] **Per-pipeline `[dependencies]` codegen.** The same SQL walk that drives `connector_dispatch.rs` arms drives the per-pipeline `Cargo.toml` `[dependencies]` block. Bundled connectors are reached transitively through `dbsp_adapters` (always linked); only external crates the SQL references appear in the per-pipeline workspace's deps. No tenant-level "list everything from `connectors.toml`" splice — only the SQL-referenced subset goes in.
- [ ] **No hand-written `extern crate feldera_datagen as _;` in `crates/adapters/src/lib.rs`.** PR 5 explicitly does not add this line; metadata reaches datagen via linkme.
- [ ] `prepare_describer_workspace` analogous to existing `prepare_workspace`. Reads the tenant's blob from `tenant_connector_config` rather than the filesystem.
- [ ] **Extend `CompilerConfig::canonicalize()`** to absolute-resolve `connectors_toml_path` (the bootstrap seed field from PR 9). The field is optional — add a sibling helper `help_canonicalize_path_if_exists` that returns `Ok(None)` when the path is unset and skips canonicalization when the path is set but does not yet exist on disk. Reusing the existing `help_canonicalize_path` would error on absent optional fields and break deployments that ship no seed file.
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
- [ ] Detect duplicate connector names **in the describer's startup** (walk `metadata_registry()`, error if any name appears twice). Failure message names both source crates from the tenant's blob. Do not push this check into adapterlib-meta — `descriptor_by_name()` is intentionally simple and the describer has the deployment context to render the diagnostic.
- [ ] **Cache key + lockfile discipline** (Phase 8.4 / 8.7): `describer_cache_key` hashes `connectors.toml` content + `ADAPTERLIB_API_VERSION` — **not** the full Feldera release version. A Feldera upgrade that does not change the plugin ABI lands in the same cache directory and skips the rebuild. Implement a **two-tier discipline** in `build_and_run_describer`: warm path (`describer.lock` exists) copies the lock as `Cargo.lock`; cold path seeds `Cargo.lock` from the main repo's lock at `{dbsp_path}/Cargo.lock` (mirroring the pipeline platform-lock pattern). Either way, build with `--offline` rather than `--locked` — `--offline` preserves the no-network guarantee but lets Cargo extend the lock from the local registry cache for the describer's own package and any user-listed connector crates that the seed does not cover. **Do not use `--locked`**: any change that rewrites the manifest (user editing `connectors.toml`, workspace pin updates, path-dep version drift) makes Cargo want to update the lock, and `--locked` rejects that with `cannot update the lock file ... because --locked was passed`. **Do not introduce a `describer.build_version` cache key**: it adds no value once the lock is seeded from main (the main lock already encodes the version) and silently breaks the warm path on every patch release. Refresh `describer.lock` after every successful build.
- [ ] **API version stamping and validation** (Phase 8.7): stamp `adapterlib_api_version: u32` into each `ConnectorManifestEntry` and call `check_manifest_api_versions` in `build_and_run_describer` after the binary exits. The check is *not* the primary guard against incompatible connectors (Cargo handles that at compile time) — it guards the *manifest file* against stale-cache scenarios: a `manifest.json` from a previous ABI generation that somehow ends up in the current cache directory is rejected before it can poison SQL direction validation. See Phase 8.7 for the full two-level framing. Tests: matching version accepted; future version rejected with connector name and both version numbers in the error message; empty manifest accepted; malformed JSON returns `Parse` error; cache key stable for identical content, distinct for different content.
- [ ] Tests: cache hit, cache miss (cold-path seeding from main `Cargo.lock`), refresh, name collision, version mismatch; `GET /status` response shape across all four states; `PUT` with stale ETag returns 412; `PUT` with malformed body returns 400; `PUT` increments `version` and triggers rebuild observable via `/status` going `ready → building → ready`; tenant isolation (tenant A's PUT does not affect tenant B's `/status`); platform manifest baked at build time (Phase 8.13): `feldera-platform-manifest` build.rs emits a non-empty JSON array containing every expected built-in name, `merge_manifests` keeps platform on collision, user describer Cargo.toml lacks `dbsp_adapters`/`feldera-datagen`, `GET /status` `descriptors` field returns the merged set. **Footgun**: actix test helpers that spin up embedded Postgres must return a struct that owns the `TempDir`, not a bare tuple. If `TempDir` is a local in the helper function, it drops when the function returns and deletes the data directory before any test request fires. The `app` field must be `impl Service<…>` — use a named struct (e.g. `struct TestApp<S> { app: S, _state: Arc<…>, _tmp: TempDir }`) so the borrow checker keeps the directory alive for the test's lifetime.
- **Depends on**: PR 9.
- **Unblocks**: PR 11, PR 12.

### PR 11 — Wire descriptor manifest into direction validation (Phase 8 cont.)
- [ ] Replace exhaustive matches at `pipeline-manager/src/db/types/program.rs:682,735` with manifest lookup + `direction.allows_input()` / `allows_output()` checks.
- [ ] Error messages name the unknown connector and suggest checking `connectors.toml`.
- [ ] **Thread the manifest through the call chain**: `attempt_end_to_end_sql_compilation` → `perform_sql_compilation` → `generate_program_info`. The manifest is a new parameter at each level; `generate_program_info` is where the `program.rs:682,735` matches live. **Two call sites for `perform_sql_compilation`**: the main compiler loop path and the precompile path at `compiler/main.rs:592` — the precompile path passes `build_in_process_manifest()` explicitly (it runs without a database-backed manifest; bundled-only descriptors suffice for precompile validation).
- [ ] **Manifest-missing path keeps the program in `Pending` rather than transitioning it to a failure state** (see Phase 8.11). **Check order**: the compiler loop first dequeues the oldest `Pending` pipeline via `get_next_sql_compilation` (the tenant ID is unknown until then), then looks up the manifest for *that specific tenant*, then returns `Ok(false)` if the manifest is not yet ready — leaving the pipeline in `Pending` and sleeping the full `POLL_INTERVAL` (250 ms). Returning `Ok(false)` is correct here: the "no work found" sleep applies equally when a manifest is temporarily unavailable. Describer build failures are surfaced via the existing `ConnectorManifestBuildFailed` / `CompilerConfigNotAvailable` `ApiError` variants from PR 10.
- [ ] **Validation failures map to `SqlError`, not `SystemError`** — unknown connector and direction mismatch are SQL-level semantic errors per Phase 8.11.
- [ ] **Refresh uses snapshot semantics**: in-flight `CompilingSql` jobs keep the manifest snapshot they loaded at job start; new `Pending → CompilingSql` transitions after `POST /v0/connectors/refresh` pick up the new manifest. No mid-compile cancellation.
- [ ] **Force-link `feldera-datagen` for tests that call `build_in_process_manifest()`**: the `pipeline-manager` test binary does not transitively link `feldera-datagen` (only the production binary does, via `dbsp_adapters`). linkme's `&'static [T]` only includes contributions from rlibs the linker has actually pulled in; without an explicit reference to the datagen crate in the test binary, the datagen descriptor is absent from the runtime registry and tests expecting it present fail silently with an empty manifest. Fix: add `feldera-datagen = { workspace = true }` to `pipeline-manager`'s `[dev-dependencies]` and `extern crate feldera_datagen as _` at the top of the affected test file. Production builds reach datagen through `dbsp_adapters`'s own consumer of the datagen API and need no force-link line — this is a test-only concern.
- [ ] Tests: known connector validates, unknown connector fails with a useful message, direction mismatch fails, program stuck in `Pending` while describer is still building, program advances once manifest is available, refresh during in-flight compile uses old snapshot, unknown plugin connector → `SqlError` (not `SystemError`).
- [ ] Verify with empty `connectors.toml` that behavior matches today's bundled-only deployments bit-for-bit (no describer invocation, no `Pending` stall).
- **Depends on**: PR 10.
- **Unblocks**: PR 13.

### PR 12 — Build-cache discipline: shared lockfile + linker (Phase 8.5)
- [ ] **`prepare_workspace`'s signature takes `tenant_id: TenantId`** — `describer_workspace_dir(config, tenant_id, cache_key)` requires the tenant ID to locate the per-tenant describer workspace at `<working_dir>/describer/<tenant_id>/<cache_key>/`.
- [ ] **`prepare_workspace` returns `bool` (`is_full_warm`)** rather than `()`: `true` when `describer.lock` was successfully copied AND its `describer.build_version` matches the current Feldera `CARGO_PKG_VERSION` (full-warm path); `false` on the cold or partial-warm path. **`call_compiler` accepts a matching `use_locked: bool` parameter** and passes `--locked` to `cargo build` only when `use_locked` is `true`. These two items are causally coupled — treat them as one atomic change, not two independent checklist items.
- [ ] **`--locked` is gated on `is_full_warm`**: cold path (no `describer.lock` yet) builds without `--locked`; partial-warm path (`describer.lock` exists but its `describer.build_version` differs from the current Feldera version — e.g. after an upgrade) copies the lock as a Cargo resolver seed and builds without `--locked` so external-connector pins are preserved while Feldera path-dep versions update. `--locked` is safe only on the full-warm path. This mirrors the three-tier logic in `build_and_run_describer` (PR 10 checklist).
- [ ] **Mold detection is per-build, not a deployment-config knob**: probe `mold --version` once per `cargo build` invocation; if it exits 0, append `-C link-arg=-fuse-ld=mold` to `RUSTFLAGS`; if it fails or is absent from PATH, omit silently. An explicit operator-supplied `RUSTFLAGS` (via the manager process env) always wins — the probe result is added only when no `-fuse-ld` flag is already present. Do **not** require operators to edit Docker Compose / Helm charts.
- [ ] **`RUSTFLAGS` constraint is architectural (documentation-only)**: the constraint that `RUSTFLAGS` must be set at the manager level, not per pipeline, is already enforced by the existing code (manager propagates `std::env::var_os("RUSTFLAGS")` and clears all other env vars before spawning `cargo build`). The deliverable is a comment in `call_compiler` explaining the invariant — no new code guard is needed.
- [ ] Measurement appendix: build times before/after on a sample pipeline.
- [ ] **Wrap `build_and_run_describer()` in `tokio::time::timeout`** with an operator-configurable upper bound (default ~30 min). On timeout, kill the cargo subprocess and call `mark_failed("build timed out after Ns")`. Without this, the describer state stays `Building` forever if the build hangs.
- [ ] **Hold the cargo `Child` handle and surface unexpected exits**: today `connectors.rs:622-642` uses fire-and-forget `tokio::spawn`, so a panicked task or OOM-killed subprocess never calls `mark_failed()` and the cache state is orphaned. Track the `Child`, await its `status()`, and on non-zero exit / signal call `mark_failed("describer process died: <signal/code>")`.
- [ ] **Persist describer build log to `<workspace>/build.log`**: stdout/stderr is currently captured in-memory only and lost on stuck builds. Tee it to a log file in the per-tenant workspace dir so operators have something to `tail` when the cache is wedged.
- [ ] **Expose the build log as an HTTP stream** so the web console can tail it live. Tee `cargo`'s stdout/stderr through a `tokio::sync::mpsc` channel (mirroring the per-pipeline runtime-log fan-out in `runner/main.rs`) into both the on-disk `build.log` and a per-tenant broadcast that backs a new endpoint (see PR 14). The channel must replay the current build's accumulated lines on subscribe — operators arriving mid-build need history, not just the live tail. On build completion, hold the buffer until the next build replaces it so the failure log remains readable.
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
- [ ] **Add a fifth endpoint, `GET /v0/connectors/build-log`** (chunked `text/plain`, one line per newline, `Content-Encoding: identity`). Streams the active describer build's combined stdout/stderr for the requesting tenant. Replays the current build's accumulated lines on connect, then tails live; closes when the build completes (or stays open and tails the next build — pick one and document it). When no build is in progress, returns an empty 200 immediately. Same shape and proxying pattern as the runtime-logs endpoint in `runner/main.rs`. Fed by the broadcast channel from PR 12.
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
- [ ] Clicking "Plugins" **navigates to a new `/plugins` page** and closes the account menu. The earlier modal-dialog framing is replaced by a full page so the build log has room to breathe and the URL is bookmarkable / linkable.

#### `/plugins` page layout

- [ ] **Top section: editor panel** styled like the Code Editor inside `PipelineEditLayout` — tab bar along the top, editor surface below. Single tab labelled `connectors.toml` with a status dot in the tab label (mapping below). The **"Apply" button sits in the same row as the tab labels** (right-aligned), not in a separate footer; there is no "Close" button on a page (the user navigates away).
- [ ] **Bottom section: build log panel** rendering `LogsStreamList.svelte` against the live describer build output (`GET /v0/connectors/build-log`, see PR 14). Reuse the existing `TabLogs.svelte` composition pattern: stream → `SplitNewlineTransformStream` → circular buffer (10k lines) → `<LogsStreamList logs={...} />`. Auto-reconnect on stream failure with the same backoff schedule used for runtime logs.
- [ ] **Vertical split** between editor and log panel is resizable (mirror whatever splitter is used in `PipelineEditLayout`); editor gets the larger default share.

#### Tab status dot

- [ ] The dot reflects the describer compilation state, polled via `GET /v0/connectors/status`:
  - Green — `ready` (build succeeded).
  - Yellow — `building`.
  - Red — `failed` or `not_configured`.
- [ ] Always visible — not an unsaved-changes indicator.

#### Editor surface

- [ ] When `connectors.toml` content is available, display it in a **Monaco editor** configured as a plain-text surface with `graphql` monaco-editor language for cheap TOML-ish highlighting — Cargo is the authoritative parser. Editor options (font, theme, minimap) match the per-pipeline code editor's defaults.
- [ ] When the tenant has no `connectors.toml` yet (state `not_configured`), display a **placeholder** in the same style as the `udf.toml` tab placeholder in the pipeline code editor.
- [ ] On page mount, fetch `GET /v0/connectors/connectors.toml` and store the returned **ETag**.

#### Save flow ("Apply")

- [ ] "Apply" issues `PUT /v0/connectors/connectors.toml` with `If-Match: <etag>` and `Content-Type: text/plain`.
- [ ] On **200**: store the new ETag; the status dot transitions to yellow (`building`) and the build-log panel begins streaming the new build's output.
- [ ] On **412 Precondition Failed**: surface an inline message — "Another operator updated this file. Reload to see the latest version." Do not navigate away; preserve the user's edits.
- [ ] On **400 Bad Request** (line-shape validation failure): display the error inline below the editor; do not clear edits.

#### Tests

- [ ] Unit test: status-dot color maps correctly to each `state` value.
- [ ] Integration / e2e: navigate to `/plugins` → editor shows existing content → edit → Apply → status dot cycles `building → ready`, build log panel streams output during the build.
- [ ] ETag mismatch path: concurrent PUT from another session triggers the 412 message without discarding local edits.
- [ ] Build-log stream reconnection: kill the stream mid-build; assert the panel reconnects and resumes tailing.

- **Depends on**: PR 14.
- **Unblocks**: PR 16.

### PR 16 — Reference plugin + end-to-end integration test (Phase 11)

#### The `hello-lines` connector

- [ ] Add `crates/connector-example/` implementing a **`hello-lines` input connector**: reads a plain-text file and emits one line per second as a single-string record. The SQL table must have exactly one `TEXT` column; no format parsing is performed — each raw line becomes the column value. Config: `{ "path": "...", "interval_ms": 1000 }` (configurable interval so tests can set it to 0). Set `default_format` to `JsonLines::Single` so each `parser.parse()` call delivers exactly one newline-terminated JSON object — `Multiple` (the parser default) buffers across calls and is the wrong semantic here.
- [ ] **`Cargo.toml` plugin-surface dependencies and feature shape**. The reference plugin demonstrates the convention every external plugin should follow:

    ```toml
    [features]
    default = ["impl"]
    metadata = []
    impl = ["dep:feldera-adapterlib"]   # plus any heavy runtime deps the connector needs

    [dependencies]
    feldera-adapterlib-meta = { workspace = false, version = "..." }     # always
    feldera-adapterlib      = { version = "...", optional = true }       # impl only
    feldera-types           = { version = "..." }                        # shared config / FT / schema types
    linkme                  = "0.3"                                      # for the metadata slice
    serde                   = { version = "1", features = ["derive"] }
    anyhow                  = "1"
    ```

    **Excluded** are the engine internals: `dbsp`, the in-tree adapters crate, and `pipeline-manager`. The user describer (Phase 8.13) compiles the plugin with `default-features = false` to extract metadata only; per-pipeline workspaces compile it with `default-features = true` (or whichever features the deployment needs). Document this convention in the "Writing a connector" page so plugin authors adopt it from day one.
- [ ] **Add an empty `[workspace]` table** to `connector-example/Cargo.toml`. The crate lives under `crates/` next to the main Feldera workspace, so Cargo otherwise auto-associates it with the parent workspace and `cargo check` fails with "current package believes it's in a workspace when it's not". An empty `[workspace]` makes it its own root. Document this for any connector developed inside an existing source tree.
- [ ] Implements `TransportInputEndpoint` + `InputReader` (inside `#[cfg(feature = "impl")] mod hello_lines_impl`) and registers metadata via a `pub static HELLO_LINES_META` annotated with `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` at module scope (unconditional). The descriptor's `BuilderPath` either follows the default convention (build fns named `build_input` etc. at the crate root) or sets `builder_path_override` to `Some(BuilderPath { crate_name: "connector_example", build_input: Some("connector_example::hello_lines_impl::build_hello_lines_input"), .. })`.

#### Fault tolerance: `ExactlyOnce` via byte offset

- [ ] The connector advertises `FtModel::ExactlyOnce`. The seek datum is a **JSON byte-range object `{"start": u64, "end": u64}`**, not a raw 8-byte `u64` — the `Replay { seek, replay, hash }` shape requires two offsets (the replay window's start and end), and the JSON form keeps metadata human-readable and consistent with the `file` transport's `Metadata { offsets: Range<u64> }`. Resume is still O(1) via `File::seek(SeekFrom::Start(start))`.
- [ ] **Checkpoint**: on `Queue { checkpoint_requested: true }`, record the current `{start, end}` byte range as the checkpoint payload.
- [ ] **Resume** (`Resume::Seek { seek }`): deserialize the JSON range and seek to `start` before entering the read loop.
- [ ] **Replay** (`Resume::Replay { seek, replay, hash }`): seek to `seek.start`, re-emit lines up through `replay.end` into the replay buffer, then set position to `replay.end` and continue. For a sequential line reader this is a small bounded re-scan.

#### Integration test

- [ ] Populate a test `connectors.toml` with a **path dependency** pointing at `crates/connector-example` in the same checkout. No build feature flag — the example must resolve through the normal `connectors.toml` → describer path, identical to what an end user would write. CI already has the source tree, so the path dep needs no network or git fetch.
- [ ] Write a known test file (e.g. five lines). Run the pipeline with `interval_ms = 0`. Assert the full plugin path: `connectors.toml` → describer → manifest → SQL parse → pipeline build → runtime dispatch → five rows in the output view.
- [ ] Checkpoint/restore test (`@enterprise_only` — explicit checkpoint is an enterprise feature): consume all five rows, take a checkpoint, force-stop, restart. Assert the row count remains **5, not 10**. Feldera exposes no row-by-row stepping API for running pipelines, so the no-duplicate property is the right assertion — a connector that ignores the seek payload would re-read from offset 0 and double the row count.

#### Documentation

- [ ] Add a "Writing a connector" doc page citing `crates/connector-example` as the canonical reference. Cover: the two traits (`TransportInputEndpoint`, `InputReader`), the linkme-annotated descriptor `pub static` (registration is the linkme attribute on the `pub static` — there is no registration macro), the `default = ["impl"]` / `metadata = []` feature shape and `<name>_impl` mod-wrapping convention, the `<crate_name>::build_<name>` builder fn naming rule, choosing a seek datum, the three FT commands (`Queue`, `Seek`, `Replay`), and the single-column schema constraint of this specific demo.
- [ ] **Document both import forms in `connectors.toml`** with concrete copy-pasteable snippets. The two forms must work identically — same describer build, same runtime dispatch — so the doc proves Feldera treats local and remote crates symmetrically.

  **From the official Feldera git repo** (the typical operator path — pin to a release tag or sha):
  ```toml
  [dependencies]
  connector-example = { git = "https://github.com/feldera/feldera", tag = "v<version>", package = "connector-example" }
  ```

  **From a local crate** (developers building `pipeline-manager` from source against an in-tree connector):
  ```toml
  [dependencies]
  connector-example = { path = "/absolute/path/to/feldera/crates/connector-example" }
  ```
  Note: paths must be absolute — the describer workspace lives in `<working_dir>/describer/<tenant_id>/<hash>/` and resolves relative paths against that directory, not the operator's CWD.

- **Depends on**: PR 15.
- **Unblocks**: nothing; ships the contract.

---

**Total: 23 PRs** — 16 top-level numbered PRs, with PR 7 split into 8 sub-PRs (7a–7f sweep the remaining bundled connectors; 7g is the post-migration cleanup; 7h migrates the criterion benches off concrete-type construction). PR 1–3 are pre-work (PR 2 introduces the `feldera-adapterlib-meta` crate split and `default = ["impl"]` feature on `dbsp_adapters`); PR 4–6 are the core dispatch plumbing (name-keyed match in the in-tree factories; PR 5 absorbed `http`/`adhoc`/`datagen` as prerequisites for the controller-rewrites, leaving Phase 4b shorter); PR 7a–7f are the remaining mechanical sweeps, PR 7g strips the scaffolding, PR 7h shrinks the public surface; PR 8–11 enable plugins (including PR 10's per-pipeline `connector_dispatch.rs` codegen and metadata-only platform manifest); PR 12–13 harden the build; PR 14–16 expose plugins externally.
