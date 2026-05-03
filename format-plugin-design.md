# Format Plugin Design

Technical design for externally-supplied format plugins in Feldera.  The
context is the post-connector-plugin state described in
`connector-plugin-refactor-design.md`.

---

## Goals

1. **Format plugins can register metadata without pulling in heavy deps.**
   A format like Avro depends on `apache-avro` (~200 kLOC of generated code),
   which in turn brings in Tokio, Rayon, and `serde_json` in ways that interact
   poorly with the platform-manifest build.  A metadata-only build of a format
   plugin should compile in seconds.

2. **Proportional link cost.** A pipeline that uses `csv` and `json` should not
   link Avro or Parquet.  Adding a format plugin to `connectors.toml` should only
   affect pipeline binaries whose SQL references that format.

3. **Zero per-record overhead.** Format plugin machinery must not add cost on the
   hot path (`Parser::parse`, `Encoder::encode`).  Factory construction (called
   once at endpoint open time) may tolerate a small dispatch cost.

4. **Consistent with connector plugins.** The patterns a connector-plugin author
   already knows — descriptor statics, `impl` feature gate, `crate_name` + fn
   convention, `connectors.toml` registration — carry over with only cosmetic
   differences.

---

## Why the current system falls short

| Goal | Current state |
|------|---------------|
| Metadata-only builds | Not supported.  `inventory::submit! { &AvroInputFormat }` compiles iff `apache-avro` is in scope; there is no metadata-only build path for format crates. |
| Proportional link cost | Not met.  All formats are in `dbsp_adapters` and link into every pipeline binary. |
| Zero per-record overhead | Met for built-in formats (stateless `&'static dyn` factories, called once). |
| External format plugins | No mechanism.  `get_input_format(name)` only finds formats compiled into `dbsp_adapters`. |

The `inventory` crate's collect/submit pattern is adequate for a single crate
with no per-pipeline selection; it becomes a liability when formats need to be
externally supplied and only linked when used.

---

## Design

Four structural changes, organized in sections that mirror the connector plugin
design.

### 1. Add `FormatDescriptor` and `FORMAT_METADATA_REGISTRY` to `feldera-adapterlib-meta`

`feldera-adapterlib-meta` gains format metadata types alongside its existing
connector types.  The new additions (~80 LOC) are:

```rust
/// Data-only metadata descriptor for a Feldera format.
///
/// Register by annotating a `pub static` with
/// `#[linkme::distributed_slice(FORMAT_METADATA_REGISTRY)]`.  Keep the static
/// unconditional (outside any `#[cfg]` gate) so the metadata-only build path
/// can reach it.
///
/// # Builder symbol convention
///
/// pipeline-manager's codegen calls `<crate_name>::build_<name>_input_format`
/// and/or `<crate_name>::build_<name>_output_format`, where `<name>` is the
/// `name` field.  Direction is NOT encoded in the format name ("avro", "csv"),
/// so the direction suffix (`_input_format` / `_output_format`) is part of the
/// fn name, unlike connectors.
pub struct FormatDescriptor {
    /// Unique format name, e.g. `"avro"`, `"csv"`, `"json"`, `"parquet"`.
    pub name: &'static str,
    /// Rust crate identifier.  Use `env!("CARGO_CRATE_NAME")`.
    pub crate_name: &'static str,
    /// Whether the format supports parsing input bytes.
    pub supports_input: bool,
    /// Whether the format supports encoding output bytes.
    pub supports_output: bool,
    /// JSON Schema for the input (parser) configuration, or `None`.
    pub input_config_schema: Option<fn() -> serde_json::Value>,
    /// JSON Schema for the output (encoder) configuration, or `None`.
    pub output_config_schema: Option<fn() -> serde_json::Value>,
}

unsafe impl Send for FormatDescriptor {}
unsafe impl Sync for FormatDescriptor {}

#[linkme::distributed_slice]
pub static FORMAT_METADATA_REGISTRY: [FormatDescriptor];

pub fn format_metadata_registry() -> impl Iterator<Item = &'static FormatDescriptor> {
    FORMAT_METADATA_REGISTRY.iter()
}

pub fn format_descriptor_by_name(name: &str) -> Option<&'static FormatDescriptor> {
    FORMAT_METADATA_REGISTRY.iter().find(|d| d.name == name)
}
```

`FormatManifestEntry` (serializable snapshot, parallel to `ConnectorManifestEntry`)
lives in the same file:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FormatManifestEntry {
    pub adapterlib_api_version: u32,
    pub name: String,
    pub crate_name: String,
    pub supports_input: bool,
    pub supports_output: bool,
}

impl FormatManifestEntry {
    pub fn from_descriptor(d: &FormatDescriptor) -> Self {
        Self {
            adapterlib_api_version: feldera_types::constants::ADAPTERLIB_API_VERSION,
            name: d.name.to_owned(),
            crate_name: d.crate_name.to_owned(),
            supports_input: d.supports_input,
            supports_output: d.supports_output,
        }
    }
}
```

### 2. `dbsp_adapters` format feature split

Format implementations join the existing `impl` feature gate:

```toml
# crates/adapters/Cargo.toml (additions to existing [dependencies])
apache-avro     = { workspace = true, optional = true }   # was unconditional
parquet         = { workspace = true, optional = true }   # was unconditional
# csv, serde_json are already lightweight; keep unconditional
```

The format module hierarchy follows the same one-gate rule as connectors:

```rust
// crates/adapters/src/lib.rs
// Already present:
#[cfg(feature = "impl")] pub mod format;   // <-- only the implementations

// The format *descriptors* live in the transport/format files themselves
// (unconditional), not in the `format` module.
```

Wait — format descriptor statics need to be reachable in a `--no-default-features`
build.  The cleanest placement is a new top-level `format_meta.rs` (unconditional)
that holds only the descriptor statics and schema fns.  The implementations remain
in the existing `format/` tree (gated by `impl`).

```
crates/adapters/src/
  format_meta.rs          # new — unconditional FormatDescriptor statics
  format/
    csv.rs                # existing impl, gated by #[cfg(feature = "impl")]
    json/                 # existing impl, gated
    avro/                 # existing impl, gated
    parquet.rs            # existing impl, gated
    raw.rs                # existing impl, gated
```

```rust
// crates/adapters/src/lib.rs
pub mod format_meta;          // unconditional — descriptors only
#[cfg(feature = "impl")] pub mod format;  // existing
```

### 3. Per-format file structure in `format_meta.rs`

```rust
// crates/adapters/src/format_meta.rs
use feldera_adapterlib_meta::{FormatDescriptor, FORMAT_METADATA_REGISTRY};

#[linkme::distributed_slice(FORMAT_METADATA_REGISTRY)]
pub static CSV_FORMAT_META: FormatDescriptor = FormatDescriptor {
    name: "csv",
    crate_name: env!("CARGO_CRATE_NAME"),
    supports_input: true,
    supports_output: true,
    input_config_schema: Some(csv_input_schema),
    output_config_schema: Some(csv_output_schema),
};

fn csv_input_schema() -> serde_json::Value {
    serde_json::json!({ /* cheap, no heavy deps */ })
}

#[linkme::distributed_slice(FORMAT_METADATA_REGISTRY)]
pub static AVRO_FORMAT_META: FormatDescriptor = FormatDescriptor {
    name: "avro",
    crate_name: env!("CARGO_CRATE_NAME"),
    supports_input: true,
    supports_output: true,
    input_config_schema: Some(avro_input_schema),
    output_config_schema: Some(avro_output_schema),
};

// … JSON, Parquet, raw …
```

Builder fns live in the `format/` tree behind the `impl` gate:

```rust
// crates/adapters/src/format/csv.rs  (existing file, additions)
#[cfg(feature = "impl")]
pub fn build_csv_input_format() -> &'static dyn feldera_adapterlib::format::InputFormat {
    &CsvInputFormat
}
#[cfg(feature = "impl")]
pub fn build_csv_output_format() -> &'static dyn feldera_adapterlib::format::OutputFormat {
    &CsvOutputFormat
}
```

Re-exported at crate root so codegen can call `dbsp_adapters::build_csv_input_format()`:

```rust
// crates/adapters/src/lib.rs
#[cfg(feature = "impl")]
pub use format::csv::{build_csv_input_format, build_csv_output_format};
#[cfg(feature = "impl")]
pub use format::avro::{build_avro_input_format, build_avro_output_format};
// … etc.
```

### 4. Format dispatch: `FORMAT_DISPATCH_REGISTRY` and per-pipeline codegen

The runtime `get_input_format(name)` and `get_output_format(name)` functions
currently do a linear `inventory` scan.  In production pipelines the format names
are known at SQL-compile time (they appear in `WITH ('format' = ...)` clauses
alongside connector names), so the same codegen trick works.

**`FORMAT_DISPATCH_REGISTRY`** in `feldera-adapterlib::format`:

```rust
// crates/adapterlib/src/format.rs  (additions)
pub struct FormatDispatch {
    pub get_input_format:
        fn(name: &str) -> Option<&'static dyn InputFormat>,
    pub get_output_format:
        fn(name: &str) -> Option<&'static dyn OutputFormat>,
}

#[linkme::distributed_slice]
pub static FORMAT_DISPATCH_REGISTRY: [FormatDispatch];

/// Runtime lookup — walks the registry (exactly one entry in production).
pub fn get_input_format(name: &str) -> Option<&'static dyn InputFormat> {
    FORMAT_DISPATCH_REGISTRY
        .iter()
        .find_map(|d| (d.get_input_format)(name))
}

pub fn get_output_format(name: &str) -> Option<&'static dyn OutputFormat> {
    FORMAT_DISPATCH_REGISTRY
        .iter()
        .find_map(|d| (d.get_output_format)(name))
}
```

**Generated `format_dispatch.rs`** (emitted by pipeline-manager alongside
`connector_dispatch.rs`):

```rust
// feldera_pipe_<UUID>_globals/src/format_dispatch.rs  (generated)
use feldera_adapterlib::format::{InputFormat, OutputFormat, FormatDispatch, FORMAT_DISPATCH_REGISTRY};

fn get_input_format(name: &str) -> Option<&'static dyn InputFormat> {
    match name {
        "csv"  => Some(dbsp_adapters::build_csv_input_format()),
        "json" => Some(dbsp_adapters::build_json_input_format()),
        "avro" => Some(my_avro_plugin::build_avro_input_format()),
        _      => None,
    }
}

fn get_output_format(name: &str) -> Option<&'static dyn OutputFormat> {
    match name {
        "csv"  => Some(dbsp_adapters::build_csv_output_format()),
        "json" => Some(dbsp_adapters::build_json_output_format()),
        "avro" => Some(my_avro_plugin::build_avro_output_format()),
        _      => None,
    }
}

static FORMAT_DISPATCH: FormatDispatch = FormatDispatch {
    get_input_format,
    get_output_format,
};
#[linkme::distributed_slice(FORMAT_DISPATCH_REGISTRY)]
static _FORMAT_DISPATCH_ENTRY: FormatDispatch = FORMAT_DISPATCH;
```

The `FORMAT_DISPATCH_REGISTRY` slice has exactly one entry in production
(the generated one).  The existing `inventory`-backed `get_input_format` /
`get_output_format` functions in `feldera-adapterlib` become thin wrappers that
walk the registry; in-crate tests that run without codegen fall through to a
test-only `inventory`-backed fallback (or call builder fns directly).

**Backward compatibility during the transition.** The `inventory::submit!` calls
in the existing format files can remain until all callers are migrated to the
generated dispatch.  During that window, `get_input_format` checks
`FORMAT_DISPATCH_REGISTRY` first (one entry in production, zero in unit-test
builds without codegen), then falls back to `inventory`.  Once the fallback
is deleted, `inventory::collect!` and all `inventory::submit!` calls in format
files are removed.

### 5. `feldera-platform-manifest` extension

`build.rs` emits both the connector and format manifests:

```rust
// crates/platform-manifest/build.rs
fn main() {
    let connector_manifest = feldera_adapterlib_meta::metadata_registry()
        .map(ConnectorManifestEntry::from_descriptor)
        .collect::<Vec<_>>();

    let format_manifest = feldera_adapterlib_meta::format_metadata_registry()
        .map(FormatManifestEntry::from_descriptor)
        .collect::<Vec<_>>();

    let json = serde_json::json!({
        "connectors": connector_manifest,
        "formats": format_manifest,
    });
    std::fs::write(concat!(env!("OUT_DIR"), "/platform_manifest.json"),
                   serde_json::to_string(&json).unwrap()).unwrap();
}
```

No new build-dep is required: `feldera-adapterlib-meta` is already a
build-dependency, and the format descriptor types are in the same crate.
Build cost is unchanged (seconds).

The same rules apply to incremental rebuilds: `build.rs` re-runs only when a
descriptor static or schema fn changes.

### 6. External format plugins

A third-party format plugin follows the same two-feature pattern as a connector
plugin:

```toml
# Cargo.toml for my-msgpack-format
[package]
name = "my-msgpack-format"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["rlib"]

[features]
default = ["impl"]
impl = ["dep:feldera-adapterlib", "dep:rmpv"]

[dependencies]
feldera-adapterlib-meta = { path = "/…/crates/adapterlib-meta" }
feldera-adapterlib      = { path = "/…/crates/adapterlib", optional = true }
rmpv                    = { version = "1", optional = true }
linkme                  = "0.3"
serde_json              = "1"
```

```rust
// src/lib.rs — descriptor, unconditional
use feldera_adapterlib_meta::{FormatDescriptor, FORMAT_METADATA_REGISTRY};

#[linkme::distributed_slice(FORMAT_METADATA_REGISTRY)]
pub static MSGPACK_FORMAT_META: FormatDescriptor = FormatDescriptor {
    name: "msgpack",
    crate_name: env!("CARGO_CRATE_NAME"),
    supports_input: true,
    supports_output: true,
    input_config_schema: None,
    output_config_schema: None,
};

// Builder fns — gated
#[cfg(feature = "impl")]
pub fn build_msgpack_input_format()
    -> &'static dyn feldera_adapterlib::format::InputFormat { &MsgpackInputFormat }

#[cfg(feature = "impl")]
pub fn build_msgpack_output_format()
    -> &'static dyn feldera_adapterlib::format::OutputFormat { &MsgpackOutputFormat }
```

**`connectors.toml`** registers external format crates alongside connector
crates — no separate file is needed:

```toml
# connectors.toml  (existing file — accepts any plugin crate)
my-connector       = { path = "/…/my-connector" }
my-msgpack-format  = { path = "/…/my-msgpack-format" }
```

The describer binary scans every listed crate for both `CONNECTOR_METADATA_REGISTRY`
entries and `FORMAT_METADATA_REGISTRY` entries, regardless of which a crate
contributes.  A crate may contribute connectors, formats, or both.  One combined
`platform_manifest.json` is produced:

```json
{ "connectors": [...], "formats": [...] }
```

pipeline-manager processes format plugin crates from `connectors.toml`
identically to connector plugin crates:

1. The describer workspace gains the crate as a dep with
   `default-features = false`.  It emits the crate's `FormatManifestEntry`s.
2. For each pipeline whose SQL references `"msgpack"`, pipeline-manager adds
   the crate (full impl) to the globals `Cargo.toml` and emits an arm in
   `format_dispatch.rs`.

### 7. Builder fn naming convention

| Setting | Function name |
|---------|--------------|
| Input format factory | `build_<name>_input_format() -> &'static dyn InputFormat` |
| Output format factory | `build_<name>_output_format() -> &'static dyn OutputFormat` |

Example: `"avro"` in crate `my_avro_plugin` exports
`my_avro_plugin::build_avro_input_format` and
`my_avro_plugin::build_avro_output_format`.

**Why direction suffixes here but not in connector names?**  Connector direction
is encoded in the transport *name* (`"kafka_input"`, `"kafka_output"`) because
two directions require two separate builder signatures.  Format direction IS
encoded in the fn suffix (`_input_format`, `_output_format`) because a single
format name ("avro") covers both directions from one crate, and the return types
(`&'static dyn InputFormat` vs `&'static dyn OutputFormat`) differ.  The codegen
uses `supports_input` / `supports_output` from `FormatManifestEntry` to decide
which arms to emit, exactly as `has_build_input` / `has_build_output` drive
connector dispatch.

---

## Component diagram

```
  ┌────────────────────────────────────────────────────────────┐
  │           feldera-adapterlib-meta  (existing + new)        │
  │                                                            │
  │   existing:  ConnectorDescriptor, CONNECTOR_METADATA_REGISTRY, …     │
  │                                                            │
  │   NEW:  FormatDescriptor, FormatManifestEntry              │
  │   NEW:  FORMAT_METADATA_REGISTRY  #[distributed_slice]     │
  │   NEW:  format_metadata_registry(), format_descriptor_by_name()│
  └────────────────────────────────────────────────────────────┘
                         ▲
              descriptor statics registered from:
                         │
  ┌─────────────────────────────────────┐
  │    dbsp_adapters (new format_meta.rs│
  │    + existing impl/ gated)          │
  │                                     │
  │  CSV_FORMAT_META    ─────────────────┼──▶ FORMAT_METADATA_REGISTRY
  │  AVRO_FORMAT_META   ─────────────────┼──▶
  │  JSON_FORMAT_META   ─────────────────┼──▶
  │  PARQUET_FORMAT_META ────────────────┼──▶
  │                                     │
  │  #[cfg(impl)] build_csv_input_format│
  │  #[cfg(impl)] build_avro_input_format│
  │  … re-exported at crate root         │
  └─────────────────────────────────────┘

  ┌───────────────────────────────────────────────┐
  │   feldera-adapterlib (existing + new)         │
  │                                               │
  │   NEW:  FormatDispatch struct                 │
  │   NEW:  FORMAT_DISPATCH_REGISTRY [distributed]│
  │   UPDATED:  get_input_format() / get_output_format()│
  │           → walks FORMAT_DISPATCH_REGISTRY    │
  └───────────────────────────────────────────────┘

  ┌───────────────────────────────────────────────────────────┐
  │  feldera-platform-manifest / build.rs  (extended)         │
  │                                                           │
  │  emits platform_manifest.json:                            │
  │    { "connectors": [...], "formats": [...] }              │
  └───────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────┐
  │  feldera_pipe_<UUID>_globals  (generated, per pipeline)          │
  │                                                                  │
  │  src/connector_dispatch.rs  (existing)                           │
  │  src/format_dispatch.rs  (NEW):                                  │
  │    fn get_input_format(name) -> Option<&'static dyn InputFormat> │
  │    fn get_output_format(name) -> Option<&'static …OutputFormat>  │
  │    static FORMAT_DISPATCH: FormatDispatch = { … };               │
  │    #[distributed_slice(FORMAT_DISPATCH_REGISTRY)]                │
  │    static _ENTRY: FormatDispatch = FORMAT_DISPATCH;              │
  └──────────────────────────────────────────────────────────────────┘
```

---

## What changes per existing format

1. **`format_meta.rs` (new file, unconditional)**: add a `pub static
   <NAME>_FORMAT_META: FormatDescriptor` annotated with
   `#[distributed_slice(FORMAT_METADATA_REGISTRY)]` and the two cheap schema fns.
2. **`format/<name>.rs` (existing, gated)**: add `pub fn build_<name>_input_format`
   and/or `pub fn build_<name>_output_format` returning the existing unit-struct
   factory reference.  Keep `inventory::submit!` in place during the transition
   (see §4).
3. **`lib.rs`**: re-export the builder fns at crate root (gated by `impl`).

No existing impl code moves.  No trait surface changes.

---

## What does not change

- `InputFormat`, `OutputFormat`, `Parser`, `Encoder`, `InputBuffer`,
  `OutputConsumer`, `Splitter` traits — exactly as today.
- `FormatConfig { name, config }` in `ConnectorConfig`.
- `ConnectorDescriptor::default_format` field.
- `ParseError` / `ParseErrorInner` error types.
- The format config deserialization contract (`new_parser` / `new_encoder`
  receive `&serde_json::Value`).

---

## Gotchas

1. **`actix_web::HttpRequest` in `InputFormat::config_from_http_request`.**
   This method ties `InputFormat` to `actix_web`, making it impossible to
   implement `InputFormat` in a crate that wants to avoid pulling in actix.
   The format plugin design does not change this — `InputFormat` stays in
   `feldera-adapterlib` (the full runtime crate), and format plugins depend on
   `feldera-adapterlib`.  However, future cleanup could replace
   `&HttpRequest` with a thin `&dyn FormatHttpRequest` abstraction, which
   would reduce the actix footprint in external plugin crates.

2. **`inventory` fallback during the transition.** Until `FORMAT_DISPATCH_REGISTRY`
   has an entry in every build context (it won't in unit-test builds that predate
   the generated `format_dispatch.rs`), `get_input_format` falls back to
   `inventory`.  The two registries must stay in sync during the transition
   window.  A CI job `cargo test -p dbsp_adapters` that asserts every format
   descriptor in `FORMAT_METADATA_REGISTRY` has a matching `inventory` entry
   catches divergence.

3. **Schema fns must stay cheap.** The describer builds `dbsp_adapters` with
   `--no-default-features`; `input_config_schema` / `output_config_schema` fns
   are called in that context.  No heavy dep (Avro schema library, DataFusion
   type system) may be referenced in those fns.  Schema values are plain
   `serde_json::Value` literals or simple `json!()` macros.

4. **Format names referenced only in runtime JSON configs, not in SQL.**
   Connector names appear explicitly in SQL (`INPUT FROM 'kafka_input'`); format
   names may appear only in JSON connector configs (`'format' = '{"name":"avro"}'`).
   pipeline-manager must scan connector config JSON blobs, not just SQL ASTs, to
   collect the full set of referenced format names for the `format_dispatch.rs`
   arms and per-pipeline `Cargo.toml` entries.

5. **`connectors.toml` cache key covers format plugins too.** The describer cache
   key is `sha256(connectors.toml || ADAPTERLIB_API_VERSION)`.  Adding or
   updating a format-only crate in `connectors.toml` correctly invalidates the
   cached manifest via the same path as connector changes — no separate
   cache key needed.

6. **`FORMAT_DISPATCH_REGISTRY` has zero entries in unit-test builds.**
   Unlike `CONNECTOR_DISPATCH_REGISTRY` (which tests can skip by calling
   `build_kafka_input` directly), format-using tests typically go through
   `get_input_format`.  Tests in `dbsp_adapters` that run without codegen will
   hit the `inventory` fallback path indefinitely.  This is acceptable: the
   test-only `inventory` path is the existing behavior, preserved without
   regression.

7. **`supports_input`/`supports_output` vs. feature availability.** A deployment
   that builds `dbsp_adapters` without the `avro` feature will have
   `AVRO_FORMAT_META.supports_input = true` in the metadata registry but no
   actual Avro `InputFormat` at runtime.  The `FORMAT_DISPATCH_REGISTRY` walk
   will return `None` for `"avro"` and the endpoint open will fail with a clean
   error.  This is the same "manifest vs. impl" gap that exists for connectors
   (§6 of the connector design).

---

## Trade-offs

- One new `format_meta.rs` file in `dbsp_adapters`.  Small; no ongoing
  maintenance cost beyond keeping descriptor fields in sync with the impl.
- Format files grow two builder fns (`build_<name>_input_format`,
  `build_<name>_output_format`).  Each is a one-liner.
- pipeline-manager's codegen gains responsibility for emitting `format_dispatch.rs`
  alongside the existing `connector_dispatch.rs`.  Scope is small: the same
  manifest-walk + match-arm-emit logic, applied to format entries.
- No new operator-facing configuration file.  External format plugin crates go
  in the existing `connectors.toml` alongside connector plugin crates.  Operators
  who add no external formats see no change.
- The transition window (during which both `inventory` and `FORMAT_DISPATCH_REGISTRY`
  are populated) requires keeping two registration points in sync per format.
  The window ends as soon as pipeline-manager emits `format_dispatch.rs` for all
  build paths.

The payoff is the same as for connector plugins: format plugin authors get
fast metadata-only builds, pipeline builds link only the formats they use, and
the runtime dispatch path is a direct call (a `match` arm), not an `inventory`
iteration.
