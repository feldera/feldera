# Connector Plugin Refactor — Cleaner Design

A revised design for the connector plugin system. Supersedes the inventory-only,
single-registry approach in `connector-plugin-refactor-plan.md`. The plan file
will be updated based on this design separately.

## Goals

1. **Zero data-plane overhead for connectors.** No per-record cost from the
   plugin machinery, and no runtime registry walk on the production dispatch
   path either — pipeline-manager's codegen emits direct calls into each
   connector's builder fn, so production endpoint construction is one direct
   symbol call (no `connector_by_name` walk, no inventory iteration).
2. **`connectors.toml` overhead proportional only to added plugins.** Adding one
   plugin should not force every pipeline build to relink every other listed
   plugin's data-plane code.
3. **Registering a plugin compiles only metadata, not data-plane code or its
   deps.** The describer / platform-manifest path must not drag in `dbsp`,
   `rdkafka`, `datafusion`, `deltalake`, etc. just to learn a connector's name,
   direction, kind, and JSON schema.

## Why the current plan falls short

| Goal | Current plan status |
|---|---|
| Zero data-plane overhead | Already met at runtime (factory dispatch is one-time, O(n)). |
| `connectors.toml` cost proportional to additions | Not met. Phase 8.5 explicitly forbids per-pipeline feature gating, so every listed plugin is linked into every per-pipeline build regardless of whether the SQL uses it. |
| Metadata-only registration | Not met. `feldera-platform-manifest`'s `build.rs` build-deps are `dbsp_adapters` + `feldera-datagen` + transitive (rdkafka, deltalake, datafusion, …). Phase 8.13 acknowledges 5–10 min cold build just to emit a JSON manifest. The user describer drags the full `feldera-adapterlib` data-plane surface into every external connector compile too. |

The plan's Appendix A names the metadata/impl split as a future option; it
should be the baseline.

## Design

Three structural changes (Sections 1–3) and four consequences (Sections 4–7).
Sections 1–3 are the moves the design actually makes; the rest follow from
them.

### 1. Split the descriptor type out of `feldera-adapterlib`

A new tiny crate `feldera-adapterlib-meta` (~200 LOC, deps: `serde`,
`serde_json`, `linkme`):

- `ConnectorDescriptor` (data-only — no builder fn pointers)
- `Direction`, `ConnectorKind`, `FtModel`, `ConnectorFlags` enums
- `CONNECTOR_METADATA_REGISTRY` linkme slot
- `metadata_registry()` discovery iterator

Connectors register by annotating a `pub static` directly with
`#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` (Section 3). No wrapper
macro — the linkme attribute is already the registration form, and adding a
macro layer would only introduce hygiene quirks for forwarding the descriptor
literal.

`feldera-adapterlib` (the existing crate, unchanged in intent) keeps the
traits, `InputCollectionHandle`, dbsp re-exports. It re-exports the descriptor
types from `-meta` so plugin authors keep importing from
`feldera_adapterlib::*`.

**The descriptor never names a `fn` pointer.** Builder dispatch is direct:
pipeline-manager's per-pipeline codegen emits one `match` covering every
SQL-referenced name (built-in or external). `dbsp_adapters` carries no
hand-written name match in its production hot path — the bundled factory
(`input_transport_config_to_endpoint` and friends) reduces to a single
walk of `CONNECTOR_DISPATCH_REGISTRY` (a linkme slice with one entry per
pipeline). That slice is the only impl-side registry: no per-connector
lookup, just a vtable of four fn pointers. In-crate test contexts that
run without codegen call the per-builder fns
(`crate::build_<name>`) directly. The constraint is what keeps cfg
surface to one gate per connector and removes the force-link gymnastics
on the impl side.

**This is the only crate split the design requires.** `dbsp_adapters` stays a
single multi-connector crate. Connector source files do not move.

### 2. `dbsp_adapters` gets a `default = ["impl"]` feature

```toml
# crates/adapters/Cargo.toml
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
serde                   = { workspace = true }                     # cheap, stays
serde_json              = { workspace = true }                     # cheap, stays
linkme                  = { workspace = true }                     # for the slice
```

`cargo build -p dbsp_adapters` behaves exactly as today (default `impl` is on).
`cargo build -p dbsp_adapters --no-default-features` compiles in seconds — only
the connector descriptors and their cheap deps.

### 3. Per-connector files: descriptor unconditional, impl behind one cfg gate

```rust
// crates/adapters/src/transport/kafka.rs
use feldera_adapterlib_meta::*;

#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]
pub static KAFKA_INPUT_META: ConnectorDescriptor = ConnectorDescriptor {
    name: "kafka_input",
    direction: Direction::Input,
    kind: ConnectorKind::Regular,
    fault_tolerance: Some(FtModel::AtLeastOnce),
    config_schema: kafka_input_schema,
    flags: ConnectorFlags::EMPTY,
};

#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]
pub static KAFKA_OUTPUT_META: ConnectorDescriptor = ConnectorDescriptor {
    name: "kafka_output", direction: Direction::Output, ..
};

fn kafka_input_schema() -> serde_json::Value { /* cheap */ }
fn kafka_output_schema() -> serde_json::Value { /* cheap */ }

#[cfg(feature = "impl")]
pub(crate) mod kafka_impl {
    use super::*;
    use feldera_adapterlib::*;
    use rdkafka::*;

    pub fn build_kafka_input(/* … */) -> _ { /* rdkafka logic */ }
    pub fn build_kafka_output(/* … */) -> _ { /* rdkafka logic */ }
}
```

Two key discipline points:

- **The descriptor never names the build fn.** Builder dispatch is direct (see
  Section 6) — codegen for production pipelines, hand-written `match` for
  in-tree tests, plain `use` for benches. If the descriptor held a `build_*:
  fn(...)` field, every metadata `static` would need a cfg-feature stub on the
  metadata-only build path. Decoupling builders from descriptors is what keeps
  the cfg surface to one gate per connector.
- **One cfg gate per connector file**, on the wrapping `mod kafka_impl`. No
  scattered `#[cfg]` attributes through data-plane code.

### 4. Crate-root gating for pure-impl modules

```rust
// crates/adapters/src/lib.rs
pub mod transport;        // unconditional — connector files live here
pub mod integrated;       // unconditional — same reason

#[cfg(feature = "impl")] pub mod controller;
#[cfg(feature = "impl")] pub mod format;
#[cfg(feature = "impl")] pub mod adhoc;
#[cfg(feature = "impl")] pub mod static_compile;
#[cfg(all(test, feature = "impl"))] mod test;

#[cfg(feature = "impl")] pub use feldera_adapterlib::transport::IntegratedOutputEndpoint;
// …other impl-only re-exports gated identically
```

`transport.rs` and `integrated.rs` themselves stay unconditional — they
declare the per-connector submodules (`pub mod kafka;`, `pub mod file;`, …)
that hold the metadata `static`s, and those declarations must remain visible
in the metadata-only build so the descriptors are linked in. Only the factory
*dispatch fn body* (the `match`) is gated by `#[cfg(feature = "impl")]`; the
`use` lines that pull in `feldera_adapterlib::*` types referenced only from
the dispatch fn move inside the gated fn (or under a separate
`#[cfg(feature = "impl")] use ...;` block).

### 5. `feldera-platform-manifest` becomes trivial

```toml
# crates/platform-manifest/Cargo.toml
[build-dependencies]
dbsp_adapters             = { path = "../adapters", default-features = false }
feldera-adapterlib-meta   = { workspace = true }
feldera-datagen           = { workspace = true, default-features = false }
serde_json                = { workspace = true }
```

```rust
// build.rs
fn main() {
    // The reference keeps the rlib linked; linkme entries fire.
    let registry = feldera_adapterlib_meta::metadata_registry();
    let json = serde_json::to_string(&registry.collect::<Vec<_>>()).unwrap();
    std::fs::write(concat!(env!("OUT_DIR"), "/platform_manifest.json"), json).unwrap();
}
```

Build cost drops from 5–10 minutes to seconds — the build-dep tree is
`dbsp_adapters` (metadata-only) + `feldera-datagen` (metadata-only) +
`feldera-adapterlib-meta` + `serde_json`. None of `dbsp`, `rdkafka`,
`datafusion`, `deltalake` is built.

**Adding a new built-in connector requires no edit to
`feldera-platform-manifest`** — a `pub static` annotated with
`#[distributed_slice(CONNECTOR_METADATA_REGISTRY)]` anywhere in `dbsp_adapters` (or
`feldera-datagen`) is automatically picked up by the linkme slice. This is the
same property `inventory` provides, but linkme's `&'static [T]` form means no
per-tenant `force_link.rs` codegen on the metadata side.

**External plugin crates are expected to expose the same
`default = ["impl"]` / `metadata = []` shape.** Without it, a tenant's user
describer pulls the plugin's full data-plane tree just to extract its
descriptor — undoing goal 3 for that plugin. The reference plugin (existing
plan's PR 16, `crates/connector-example/`) demonstrates the convention; the
"Writing a connector" doc requires it. Plugins that don't follow the
convention still work — they just pay the same describer-cost penalty the
existing plan accepts for all connectors.

**Rebuild cost is incremental, not per-build.** Cargo's build-script
fingerprint tracks build.rs source + build-deps' rlib hashes. Resolver v2
compiles `dbsp_adapters` separately for its `--no-default-features` build-dep
role, so impl-side edits do not change the metadata rlib hash and build.rs
does not re-run. The script re-runs only when a descriptor `static` or
schema fn changes (seconds); pipeline-manager-only edits and impl-only
connector edits cost zero. Keep `build.rs`'s `rerun-if-changed` declarations
narrow (just `build.rs` itself) — let cargo's rlib-hash tracking do the work.

### 6. Builder dispatch: direct, with a thin runtime hook

`CONNECTOR_METADATA_REGISTRY` is the only per-connector registry — used by the SQL
compiler, `feldera-platform-manifest`, the user describer, and the
registration unit tests. A second linkme slice,
`CONNECTOR_DISPATCH_REGISTRY`, exists solely to plumb the per-pipeline
generated dispatch into the bundled runtime; it carries no per-connector
lookup, only a fixed four-fn vtable, and production builds always have
exactly one entry. Builder dispatch — turning a connector name into a
`build_*` call — is direct in all three real call patterns.

**Survey of `connector_by_name` callers in the current plan** (verified against
the live tree on the `connectors` branch):

| Caller | What it actually wants |
|---|---|
| `crates/adapters/benches/{delta,postgres}_*.rs` | Direct calls to `build_delta_table_output` / `build_postgres_output`. The registry indirection was added by PR 7h. |
| `crates/adapters/src/transport/nexmark.rs::registry_test` and similar `registry_test`s | Asserting the **metadata** descriptor registered correctly. `CONNECTOR_METADATA_REGISTRY` only. |
| `crates/adapterlib/src/connector.rs` unit tests | Same — testing the registry mechanism itself. |
| `crates/connector-example/src/lib.rs` | Same — proves the descriptor is present in `CONNECTOR_METADATA_REGISTRY`. |
| `transport/kafka/ft/test.rs` | Calls the factory, not the registry. |

Nothing outside the factory itself looks up a builder by name to invoke it. The
benches do the moral equivalent only because PR 7h was written that way.

**Three dispatch paths** cover all production and test consumers:

1. **Production pipelines (pipeline-manager codegen).** The SQL compiler
   walks `INPUT/OUTPUT FROM 'name'` clauses and emits a `match` into the
   per-pipeline globals crate. Built-in and external connectors are
   treated identically — the codegen calls
   `<crate_name>::build_<name>(…)` for both, using each
   descriptor's `crate_name` field directly.  The manifest's
   `has_build_input`, `has_build_output`, `has_build_integrated_input`,
   and `has_build_integrated_output` flags determine which generated
   dispatch fn receives an arm for each connector.  Connectors flagged
   `HTTP_DIRECT` are excluded (the HTTP server constructs them):

   ```rust
   // generated by pipeline-manager into <pipeline>_globals/src/connector_dispatch.rs
   fn build_input(
       name: &str,
       config: &serde_json::Value,
       endpoint_name: &str,
       secrets_dir: &std::path::Path,
   ) -> anyhow::Result<Option<Box<dyn TransportInputEndpoint>>> {
       match name {
           "kafka_input" => Ok(Some(
               dbsp_adapters::build_kafka_input(config, endpoint_name, secrets_dir)?
           )),
           "file_input" => Ok(Some(
               dbsp_adapters::build_file_input(config, endpoint_name, secrets_dir)?
           )),
           "hello_lines" => Ok(Some(
               connector_example::build_hello_lines(config, endpoint_name, secrets_dir)?
           )),
           "acme_snowflake_in" => Ok(Some(
               acme_snowflake::build_acme_snowflake_in(config, endpoint_name, secrets_dir)?
           )),
           // …one arm per name actually referenced by the SQL
           _ => Ok(None),
       }
   }
   ```

   The four dispatch fns are bundled into a `ConnectorDispatch`
   static registered into `CONNECTOR_DISPATCH_REGISTRY` via a single
   `#[linkme::distributed_slice]` annotation. The bundled factory
   consults the registry **first**; the in-tree match in path 2 is only
   reached when the registry is empty. No registry walk on the dispatch
   path — each `match` arm lowers to a jump table or short cmp chain and
   a direct call into the named symbol.

2. **In-crate tests** (`mock_input_pipeline`, `transport/kafka/ft/test.rs`,
   etc.) — contexts that run without per-pipeline codegen. A
   `#[cfg(test)]`-only helper pair in `crates/adapters/src/test.rs`
   (`build_test_input_endpoint` / `build_test_output_endpoint`) carries
   the name → built-in builder match. It is the *only* hand-written name
   dispatch that survives, lives entirely in the test module, and is
   absent from production binaries. Adding a built-in adds one arm to the
   helper — bookkeeping comparable to adding an `inventory::submit!`,
   with no force-link discipline.

3. **Benches and ad-hoc tools.** Plain
   `use dbsp_adapters::build_kafka_input;` — no indirection at all.
   Built-in builder fns are re-exported at the `dbsp_adapters` crate root
   specifically so callers can construct endpoints by direct symbol
   reference, bypassing dispatch entirely.

**Symbol-path convention for codegen.** pipeline-manager's codegen derives
the fully-qualified path of each connector's builder fn from the descriptor
alone. The convention is `<crate_name>::build_<name>` where `<name>` is
the connector's `name` field — e.g. `connector_example::build_hello_lines`
for the `hello_lines` connector in crate `connector_example`.  Direction is
already encoded in connector names (`kafka_input`, `kafka_output`), so no
slot prefix is needed.  The manifest's `has_build_*` flags tell the codegen
which of the four dispatch fns receives an arm for this connector — one fn
name, one or more arms.

`Direction::InputOutput` is therefore disallowed under a single connector
name: input and output builder fns would share a name but require different
call signatures.  Connectors that serve both directions register two
separate named connectors (e.g. `"my_transport_input"` and
`"my_transport_output"`).

```rust
pub struct ConnectorDescriptor {
    pub name: &'static str,
    /// Rust crate identifier of the crate that exports the builder fns.
    /// Conventionally `env!("CARGO_CRATE_NAME")`. Built-in connectors whose
    /// descriptor is registered from a separate crate but re-exported through
    /// `dbsp_adapters` (currently only `feldera-datagen`) hardcode
    /// `"dbsp_adapters"`. pipeline-manager's codegen calls
    /// `<crate_name>::build_<name>(…)` for both built-ins and external
    /// plugins identically.
    pub crate_name: &'static str,
    // …
}
```

**The `crate_name` field is the single source of truth** for built-in vs
external classification — no second flag, no `Option`. Connector authors
must name their builder fns to match the convention; no override mechanism
exists.

**Manifest-without-impl error path.** A name in `CONNECTOR_METADATA_REGISTRY` that
nothing covers at runtime — neither the in-tree `match` (e.g. the
deployment doesn't link `dbsp_adapters` with `kafka_*`) nor the
`CONNECTOR_DISPATCH_REGISTRY` entry — surfaces a clean
`unknown_*_transport(name)` error at startup. The SQL compiler validates
against metadata; the runtime is allowed to be a subset and reports the
gap.

### 7. Per-pipeline impl set (proportional cost — external plugins only)

The pipeline workspace's generated `Cargo.toml` lists **external** impl crates
only for connectors actually referenced by the SQL. The SQL compiler already
has the list — it walks `INPUT/OUTPUT FROM 'connector_X'` clauses;
`pipeline-manager` resolves each name to its `crate_name` via the merged
manifest and filters the `connectors.toml` blob to that subset before
splicing it into the per-pipeline globals `Cargo.toml`.

`dbsp_adapters` itself is one rlib containing every bundled connector; you
can't selectively link "just kafka and file" without per-pipeline feature
variation, which the existing plan rightly rejects (Phase 8.5: cache-key
combinatorics). So bundled connectors continue to be all-in, just like today.
Tree-shaking within `dbsp_adapters` remains a build-time decision driven by
the existing `with-*` features, fixed once at the manager level.

The plan's "per-pipeline feature variation defeats sccache" objection does
not apply to external plugins, because we are not varying *features* of a
shared crate; we are varying *which crates participate in the link*. Each
external impl crate's rlib hash is identical across pipelines, so sccache
hits unconditionally. Only the link step varies, and the link step is never
cached anyway. Per-pipeline external impl sets are a strict win on link time.

For the bundled-only case (`dbsp_adapters` linked as today, no
`connectors.toml` entries), nothing changes. The proportional-cost benefit
accrues to deployments that list third-party connectors.

**Build cache.** Within a tenant, the per-tenant describer and
that tenant's per-pipeline builds **share rlibs via sccache** (not a shared
`target/`): pipeline-manager copies `describer.lock` verbatim as each
per-pipeline `Cargo.lock`, so shared crates resolve to identical hashes
and sccache hits unconditionally. **Across tenants**, caches are
intentionally isolated — each tenant has its own describer workspace and
lockfile. The platform manifest's `build.rs` lives in the main workspace
`target/` (resolver v2 keeps its `--no-default-features` build-dep slot
distinct from pipeline-manager's runtime use of `dbsp_adapters`). Link is
never cached; mold/lld keeps the per-pipeline floor low.

### 8. Component diagram

What gets compiled where, and what the linker stitches together:

```
                        ┌────────────────────────────────────────┐
                        │ COMPILED ONCE (shared across all       │
                        │ pipelines, tenants, tests)             │
                        └────────────────────────────────────────┘

  ┌────────────────────────────┐  ┌──────────────────────────────────┐
  │  feldera-adapterlib-meta   │  │       feldera-adapterlib         │
  │  (data-only, cheap deps)   │  │  • TransportInputEndpoint trait  │
  │                            │  │  • OutputEndpoint trait          │
  │  • ConnectorDescriptor     │  │  • InputConsumer trait           │
  │  • Direction, Kind, Flags  │  │  • OutputControllerRef trait     │
  │  • default_builder_fn      │  │  • IntegratedInput/OutputEndpoint│
  │                            │  │                                  │
  │                            │  │  ┌────────────────────────────┐  │
  │  ┌──────────────────────┐  │  │  │ ConnectorDispatch struct   │  │
  │  │ CONNECTOR_METADATA_REGISTRY    │◀─┼──┼──│ (4 fn-ptr vtable)          │  │
  │  │ #[distributed_slice] │  │  │  └────────────────────────────┘  │
  │  └──────────────────────┘  │  │  ┌────────────────────────────┐  │
  │                            │  │  │ CONNECTOR_DISPATCH_REGISTRY│  │
  │                            │  │  │ #[distributed_slice]       │  │
  └────────────────────────────┘  │  └────────────────────────────┘  │
                                  └──────────────────────────────────┘
                                                ▲
                                                │ depends on
                                                │
  ┌────────────────────────────────────────────────────────────────┐
  │                       dbsp_adapters                            │
  │   (built-in connectors, controller, server, factory match)    │
  │                                                                │
  │   built-in connector files (each contributes one descriptor): │
  │     transport/kafka.rs   ── KAFKA_INPUT_META,                  │
  │                             pub fn build_kafka_input           │
  │     transport/file.rs    ── FILE_INPUT_META,                   │
  │                             pub fn build_file_input            │
  │     integrated/postgres.rs ─ POSTGRES_INPUT_META,              │
  │                             pub fn build_postgres_input        │
  │     ... (~17 entries, all crate_name = "dbsp_adapters")        │
  │                                                                │
  │   crate-root re-exports lift each `build_<name>` so            │
  │   `dbsp_adapters::build_kafka_input` resolves like any         │
  │   plugin's symbol (codegen treats built-ins identically).      │
  │                                                                │
  │   transport.rs::input_transport_config_to_endpoint reduces to  │
  │   a single walk of CONNECTOR_DISPATCH_REGISTRY. No hand-       │
  │   written name match in the production hot path.              │
  │                                                                │
  │   #[cfg(test)] helper `crate::test::build_test_input_endpoint` │
  │   carries the test-only name match for `mock_input_pipeline`.  │
  └────────────────────────────────────────────────────────────────┘


                        ┌────────────────────────────────────────┐
                        │ COMPILED ONCE PER EXTERNAL PLUGIN      │
                        │ (cached, shared across pipelines)      │
                        └────────────────────────────────────────┘

  ┌────────────────────────────────────────────────────┐
  │           connector-example  (external plugin)     │
  │                                                    │
  │   pub static HELLO_LINES_META: ConnectorDescriptor │
  │     = ConnectorDescriptor {                        │
  │        name: "hello_lines",                        │
  │        crate_name: env!("CARGO_CRATE_NAME"),       │
  │        ...                                         │
  │     };                                             │
  │   #[distributed_slice(CONNECTOR_METADATA_REGISTRY)] ─────────┼──┐
  │                                                    │  │ contributes
  │   pub fn build_hello_lines(cfg, ep, dir)           │  │ entry
  │       -> AnyResult<Box<dyn TransportInputEndpoint>>│  │
  └────────────────────────────────────────────────────┘  │
                                                          │
                        ┌─────────────────────────────────┼─┐
                        │ GENERATED & COMPILED PER        │ │
                        │ PIPELINE (UUID-suffixed)        │ │
                        └─────────────────────────────────┼─┘
                                                          │
  ┌────────────────────────────────────────────────────┐  │
  │  feldera_pipe_<UUID>_globals  (generated)          │  │
  │                                                    │  │
  │  src/connector_dispatch.rs (codegen):              │  │
  │    fn build_input(name, cfg, ep, dir) {            │  │
  │      match name {                                  │  │
  │        "kafka_input"  =>                           │  │
  │           dbsp_adapters::                          │  │
  │             build_kafka_input(...),            ───┼──┼──▶ direct
  │        "hello_lines"  =>                           │  │   call to
  │           connector_example::                      │  │   plugin's
  │             build_hello_lines(...),           ─────┼──┼──▶ build fn
  │        _ => Ok(None),                              │  │
  │      }                                             │  │
  │    }                                               │  │
  │    pub static DISPATCH: ConnectorDispatch = {...}; │  │
  │  #[distributed_slice(CONNECTOR_DISPATCH_REGISTRY)]─┼──┼──┐
  │                                                    │  │  │
  │  src/udf.rs   (user UDFs)                          │  │  │
  │  src/stubs.rs                                      │  │  │
  │                                                    │  │  │
  │  (no force_link.rs — codegen's symbol references   │  │  │
  │   keep plugin rlibs alive on their own)            │  │  │
  └────────────────────────────────────────────────────┘  │  │
                                                          │  │
  ┌────────────────────────────────────────────────────┐  │  │
  │  feldera_pipe_<UUID>_main  (generated)             │  │  │
  │  fn circuit(cconf) { ... uses globals::* ... }     │  │  │
  │  fn main() { dbsp_adapters::server::server_main()} │  │  │
  └────────────────────────────────────────────────────┘  │  │
                                                          │  │
                                                          │  │
  ════════════════════════════════════════════════════════▼══▼═══════
                          LINKER  (cargo build --bin)
  ═══════════════════════════════════════════════════════════════════
                                  │
                                  │ produces
                                  ▼
  ┌────────────────────────────────────────────────────────────┐
  │           feldera_pipe_<UUID>_main  (final binary)         │
  │                                                            │
  │   Two slices the linker aggregates from all contributors:  │
  │                                                            │
  │   CONNECTOR_METADATA_REGISTRY =                                      │
  │     [ KAFKA_INPUT_META,    ← from dbsp_adapters            │
  │       FILE_INPUT_META,     ← from dbsp_adapters            │
  │       ...,                                                 │
  │       HELLO_LINES_META,    ← from connector-example ]      │
  │   (used at SQL-compile / describer time, not at runtime)   │
  │                                                            │
  │   CONNECTOR_DISPATCH_REGISTRY =                            │
  │     [ DISPATCH ← from feldera_pipe_<UUID>_globals ]        │
  │   (walked once per endpoint open at runtime)               │
  │                                                            │
  │   At runtime, the controller calls dispatch_input() which  │
  │   walks the slice → DISPATCH.build_input → matches the     │
  │   name → calls the named symbol directly.                  │
  └────────────────────────────────────────────────────────────┘
```

**What the linker glues:**

| What | Source | Destination |
|---|---|---|
| Built-in descriptor entries (~17) | `dbsp_adapters` connector files | `CONNECTOR_METADATA_REGISTRY` |
| Plugin descriptor entry (per plugin) | external plugin crate | same `CONNECTOR_METADATA_REGISTRY` |
| Per-pipeline dispatch entry (1) | `<UUID>_globals/connector_dispatch.rs` | `CONNECTOR_DISPATCH_REGISTRY` |
| Built-in builder calls (`dbsp_adapters::build_kafka_input`, …) | `dbsp_adapters` crate-root re-exports | resolved by the linker because the codegen's `match` references them by name |
| External builder call (`connector_example::build_hello_lines`) | plugin crate | resolved because the codegen references it by name; that reference is also what keeps the plugin rlib alive |
| `dbsp_adapters` ↔ `<UUID>_globals` runtime hook | neither side names the other | both reference `CONNECTOR_DISPATCH_REGISTRY` by name; linkme aggregates contributions |

The two `#[distributed_slice]` slots are the only places the linker has to *aggregate* contributions from crates that don't otherwise know about each other. Everything else is conventional name-on-name symbol resolution.

## linkme vs inventory

Both work. linkme is preferred for `CONNECTOR_METADATA_REGISTRY`:

- `linkme::distributed_slice` produces `&'static [T]` directly — the registry IS
  the slice. Lookup is `iter().find()`, no linked-list traversal, no ctor init.
- No section-name collisions across crates linked into the same binary (a
  recurring `inventory` footgun in mixed test/bench setups).
- Stable Rust since 1.62.
- Force-link discipline is nicer: referencing any `pub static` from a crate by
  name keeps its rlib linked. `feldera-platform-manifest`'s build.rs naming
  `dbsp_adapters::CONNECTOR_METADATA_REGISTRY` (or any pub item) is enough — no
  `extern crate ... as _;` lines per connector, no per-tenant `force_link.rs`
  codegen on the metadata side.

Because builder dispatch is direct (Section 6), there is no impl-side
inventory or linkme slot, and **no force-link discipline on the impl side
either** — the codegen `match` and the in-tree `match` both reference
builder symbols by name, which keeps the rlibs alive on their own. The
hand-written `extern crate feldera_datagen as _;` workaround in
`dbsp_adapters/src/lib.rs` (added by PR 5 of the existing plan) disappears
entirely.

Existing inventory users in the repo (`StorageBackendFactory`,
`CheckpointSynchronizer`, the format registry from Phase 3) can stay; this is
purely about the new metadata registry. The bigger win is the metadata/impl
split — linkme vs inventory is a side preference, ~5% of the value.

## What changes per existing connector

Mechanically per file (e.g. `transport/kafka.rs`):

1. Add `pub static <NAME>_META: ConnectorDescriptor = …;` annotated with
   `#[distributed_slice(CONNECTOR_METADATA_REGISTRY)]`. Unconditional.
2. Add `fn <name>_schema() -> serde_json::Value { … }`. Unconditional, cheap.
3. Wrap existing impl content in
   `#[cfg(feature = "impl")] pub(crate) mod <name>_impl { … }`. The build
   function becomes `pub fn build_<name>(…)` so it can be named directly
   by the in-tree factory `match` and by pipeline-manager's codegen.
4. Add one `match` arm per builder in `crates/adapters/src/transport.rs` (or
   `integrated.rs`), gated by the same `with-*` feature as the impl module.
   This is the in-tree dispatch path; production pipelines use codegen and
   bypass it.

For pure-impl modules (`controller`, `adhoc`, `format`, `static_compile`),
gate the `pub mod` declaration in `lib.rs`. No source changes inside the
module.

For `transport.rs` / `integrated.rs` (the factory files), the file itself
stays unconditional — its `pub mod kafka;` / `pub mod file;` / … submodule
declarations must remain visible in the metadata-only build so the
descriptors are reachable. Only the dispatch fn body is gated by
`#[cfg(feature = "impl")]`, with `match` arms that follow the same
per-connector feature gating as the impl modules they call into. Where a
connector has no `with-*` feature today (file, http, clock, adhoc), its arm
is gated by `#[cfg(feature = "impl")]` alone.

## What does not change

- Connector trait surface (`TransportInputEndpoint`, `Parser`, `Encoder`,
  `IntegratedInputEndpoint`, …) — exactly as today.
- Wire/serde format of `TransportConfig` for in-tree connectors.
- The `Resume`/`Replay`/`Seek`/`Barrier` FT protocol.
- The `InputReaderCommand` state machine.
- The controller's checkpoint / replay / step-driving logic.
- `dbsp_adapters` source layout. No connector code moves.

## Concrete benefits vs the existing plan

| Property | Existing plan | This design |
|---|---|---|
| Describer cold build | 5–10 min (full data-plane dep tree) | Seconds (metadata-only deps) |
| Adding one plugin to `connectors.toml` | Rebuilds describer with full impl tree; relinks every per-pipeline binary | Rebuilds describer with one extra meta crate; only pipelines whose SQL references it relink |
| `feldera-platform-manifest` build.rs deps | `dbsp_adapters` + `feldera-datagen` + transitive (rdkafka, datafusion, deltalake, …) | Same crates, all `--no-default-features` (metadata-only) |
| Force-link `extern crate ... as _;` | Required for both meta and impl, generated per tenant | None on either side. Metadata is reached through linkme's `&'static [T]`; impl is reached by codegen / in-tree `match` naming the symbol directly |
| Plugin author's compile-time surface (just to register) | Full `feldera-adapterlib` (audited DBSP re-exports, `InputCollectionHandle`) | `feldera-adapterlib-meta` only |
| Phase 7's `Plugin(PluginTransportConfig)` enum-opening | Workaround required | `feldera-types` can validate names directly against the metadata registry; the typed-variant + `Plugin` split becomes optional |
| Per-connector cfg surface | N/A — single registry, no split | Exactly one `#[cfg(feature = "impl")]` per connector file, on the inner `mod` |
| Production dispatch cost | O(n) `connector_by_name` walk at endpoint open | One direct symbol call (jump-table `match`); no registry walk |
| In-tree dispatch cost | O(n) walk via inventory | Hand-written `match` over names (n ≤ 30, called once at startup); behaviorally equivalent |

## Gotchas worth naming up front

1. **Feature unification across the workspace.** Workspace members that depend
   on `dbsp_adapters` with default features (e.g. pipeline-manager) would, under
   resolver v2 within a single dep kind, unify features. The thing that keeps
   `feldera-platform-manifest`'s build clean is that it lists `dbsp_adapters` as
   a `[build-dependencies]` entry, and resolver v2 isolates build-dep feature
   resolution from runtime-dep features. So `feldera-platform-manifest`'s
   `build.rs` gets `dbsp_adapters` with `default-features = false` while
   pipeline-manager's runtime gets it with `default-features = true`, in the
   same `cargo build`. Per-tenant describer workspaces are separate workspaces;
   no unification concern there.

2. **Optional-dep visibility in metadata code.** `use feldera_adapterlib::*;` at
   the top of a connector file would resolve only under `impl`. Keep meta-side
   imports referencing `feldera_adapterlib_meta::*` (or use the re-export path
   `feldera_adapterlib::meta::*` if you prefer one import root). Connector
   authors do not see this complexity unless they deliberately split the file.

3. **Force-link is gone.** linkme has the same dead-code-elimination
   property as inventory: if no symbol from an rlib is referenced, the
   linker drops the rlib including any registration. With this design that
   property is incidental — both dispatch paths name builder symbols
   directly (codegen `match` arms reference `<crate>::build_<name>`; the
   in-tree factory `match` references `kafka::build_kafka_input` etc.,
   one fn per built-in), and
   `feldera-platform-manifest`'s `build.rs` references
   `dbsp_adapters::CONNECTOR_METADATA_REGISTRY` by name. No `extern crate ... as _;`
   workarounds anywhere. The hand-written line in `dbsp_adapters/src/lib.rs`
   added by PR 5 of the existing plan is deleted.

4. **CI matrix gains one fast job.**
   `cargo build -p dbsp_adapters --no-default-features` should run on every PR.
   It catches anyone accidentally placing impl code outside an `impl` gate. This
   is the structural rule the compiler enforces — same logic as the current
   plan's `Send + Sync` argument for `ConnectorDescriptor`.

5. **Runtime hook for the generated dispatch.** The per-pipeline `main.rs`
   is emitted by the SQL compiler (Java); it cannot be made to pass a
   dispatch table to `Controller`/`server_main` without a Java-side
   change. Use a single `#[linkme::distributed_slice]
   pub static CONNECTOR_DISPATCH_REGISTRY: [ConnectorDispatch]` slot in
   `feldera-adapterlib::transport`, into which the generated
   `<pipeline>_globals/src/connector_dispatch.rs` registers one entry.
   The runtime endpoint factory in `dbsp_adapters` reduces to a single
   walk of that slice. Production builds register exactly one entry; the
   bundled controller never has to fall through to anything else because
   in-crate test code that runs without codegen reaches the per-builder
   fns through `crate::build_<name>` directly, bypassing the
   factory entirely. This slice is the *only* "registry" in the design —
   walked once per endpoint construction, not per record — and it is
   what frees the design from requiring a Java-side change to plumb the
   dispatch in.

6. **Built-in and external connectors share one dispatch path.** The
   descriptor carries `crate_name: &'static str` (always populated, by
   convention via `env!("CARGO_CRATE_NAME")`). pipeline-manager's codegen
   emits one match arm per SQL-referenced name — built-in or external —
   calling `<crate_name>::build_<name>(…)`. Bundled built-ins re-export
   their `build_*` fns at the `dbsp_adapters` crate root so
   `dbsp_adapters::build_kafka_input(…)` resolves like any plugin crate's
   symbol. Connectors flagged `HTTP_DIRECT` are excluded (no factory fn).
   The `build_<name>` naming convention is mandatory; connector authors
   must name their builder fns to match — there is no
   override mechanism.

7. **Globals Cargo.toml deps must be explicit pins, not workspace
   inheritance.** The SQL-compiler-emitted workspace `Cargo.toml` declares a
   fixed `[workspace.dependencies]` set that does not include `linkme` or
   `anyhow`; injecting `linkme = { workspace = true }` into the globals
   crate fails with *"dependency.linkme was not found in
   workspace.dependencies"*. Use explicit pins (`linkme = "0.3"`,
   `anyhow = "1"`) in the globals injection block. Everything else the
   codegen needs (`TransportInputEndpoint`, `OutputControllerRef`,
   `ConnectorDispatch`, `CONNECTOR_DISPATCH_REGISTRY`, etc.) should be
   re-exported from `dbsp_adapters` so the codegen reaches it through one
   already-workspace-inherited crate root — including `OutputControllerRef`
   and `IntegratedInputEndpoint`, which are not re-exported from
   `dbsp_adapters` today.

8. **Describer cache invalidation when a plugin's *source* changes.** The
   describer cache key is `sha256(connectors.toml || ADAPTERLIB_API_VERSION)`,
   so editing a plugin crate's source (e.g. changing a `config_schema` fn
   or renaming a builder fn) does not invalidate the cached `manifest.json`.
   The codegen then reads stale `builder_crate` / schema values. Either
   expose a refresh path that rebuilds the describer when a plugin crate's
   `path =` target's mtime changes, or accept that operators run
   `POST /v0/connectors/refresh` after editing in-tree plugin sources.
   Bumping `ADAPTERLIB_API_VERSION` invalidates as a sledgehammer fallback.

## Trade-offs

- One new crate (`feldera-adapterlib-meta`). Genuinely small (~200 LOC); no
  ongoing maintenance cost.
- Connector files grow by one nested `mod` block (the impl section). The
  outer-vs-inner split is the price of the single-cfg-gate discipline.
- Adding a built-in connector touches three places: descriptor `static`, impl
  `mod`, factory `match` arm. Symmetric with the existing plan (descriptor,
  impl `mod`, `inventory::submit!`). No net cost.
- pipeline-manager's codegen takes on responsibility for emitting the
  per-pipeline dispatch `match` and `[dependencies]` list. This is a moderate
  addition to existing codegen. The reward is goals 1 and 2 falling out of one
  mechanism.
- External plugin builder symbols become part of the ABI by convention:
  `<crate_name>::build_<name>`. The descriptor's `crate_name`
  (auto-populated by `env!("CARGO_CRATE_NAME")`) is the single source of
  truth for the owning crate; fn names must match the convention with no
  override mechanism. Same SemVer discipline already required of
  `feldera-adapterlib`.

## Plan delta (sketch — to be applied to `connector-plugin-refactor-plan.md`)

- **Phase 1 unchanged.** ABI tightening still applies.
- **Phase 2 collapses to 2a only**: define `feldera-adapterlib-meta` and the
  metadata descriptor (data-only — no builder fn pointers, no `BuilderFn`
  types). The earlier 2b for `IMPL_REGISTRY` is deleted.
- **Phase 4 / 7a–7g**: each connector migration adds a `metadata = []` /
  `default = ["impl"]` discipline as part of the per-connector recipe. Source
  changes per connector: add the unconditional descriptor, wrap impl in one
  `#[cfg(feature = "impl")] pub(crate) mod` exposing `pub fn build_*`, add one
  arm to the factory `match` in `transport.rs` / `integrated.rs`. No
  impl-side registration call.
- **Phase 6**: factory dispatch in `transport.rs` / `integrated.rs` is a
  hand-written `match` over `name()` strings. There is no registry walk on
  the dispatch path. For production pipelines, pipeline-manager's codegen
  replaces this `match` with its own per-pipeline generated `match` (see
  Phase 8 delta below).
- **Phase 8.13**: `feldera-platform-manifest`'s build-dep tree is
  metadata-only; cold-build cost drops from minutes to seconds.
- **Phase 8.4**: per-pipeline workspace generates its `Cargo.toml` from the
  SQL's connector references (`INPUT/OUTPUT FROM 'name'` walk), not from
  `connectors.toml` wholesale. The same walk drives a generated
  `connector_dispatch.rs` in the globals crate with one `match` arm per
  connector the SQL references; the in-tree controller in production
  pipelines calls into the generated dispatch instead of the in-tree
  factory `match`. Arm bodies use the descriptor's `crate_name` field
  (auto-populated via `env!("CARGO_CRATE_NAME")`) and the mandatory
  `<crate_name>::build_<name>` convention.
- **Phase 7**: `TransportConfig` can flatten earlier; the metadata registry is
  available everywhere `feldera-types` is. The typed-variant + `Plugin` split
  becomes a deserialization implementation detail rather than the only
  available shape.
- **Add CI job**: `cargo build -p dbsp_adapters --no-default-features`.
- **Drop**: per-tenant `force_link.rs` generation on **both** metadata and
  impl sides, the hand-written `extern crate feldera_datagen as _;`
  workaround in `dbsp_adapters/src/lib.rs`, and the entire `IMPL_REGISTRY` /
  `BuildInputFn` / `BuildOutputFn` / `BuildIntegratedInputFn` /
  `BuildIntegratedOutputFn` / `OutputControllerRef`-via-Arc-coercion
  apparatus from the existing plan's PR 2. `OutputControllerRef` itself
  stays — it is still useful for decoupling integrated-output connectors
  from `ControllerInner` — but its callers receive it via the build fn's
  `Arc<dyn OutputControllerRef>` parameter directly, not through a
  registry-mediated coercion.
- **PR 7h**: rewrite the bench migration to use plain
  `use dbsp_adapters::build_delta_table_output;`
  (and similar for postgres) instead of routing through any registry.
  Drops three lines per bench and clarifies the call site.
