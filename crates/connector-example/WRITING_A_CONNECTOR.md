# Writing a Feldera Connector

This document explains how to build a custom I/O connector for Feldera using the
`feldera-adapterlib` plugin API.  The `connector-example` crate (this directory)
implements the `hello-lines` input connector and serves as the canonical reference.

---

## Prerequisites

* Rust 1.70+.
* `feldera-adapterlib-meta` for metadata registration (always compiled).
* `feldera-adapterlib` and `feldera-types` for the runtime implementation (compiled
  only when the `impl` feature is active — see §4 below).

---

## Quick start

### 1 — Create a crate

```toml
# Cargo.toml
[package]
name = "my-connector"
version = "0.1.0"
edition = "2021"

[workspace]  # keeps the crate out of any parent workspace

[lib]
crate-type = ["rlib"]

[features]
default = ["impl"]
impl = ["dep:feldera-adapterlib"]

[dependencies]
feldera-adapterlib-meta = { path = "/path/to/feldera/crates/adapterlib-meta" }
feldera-types           = { path = "/path/to/feldera/crates/feldera-types" }
feldera-adapterlib      = { path = "/path/to/feldera/crates/adapterlib", optional = true }
linkme                  = "0.3"
anyhow                  = "1"
serde                   = { version = "1", features = ["derive"] }
serde_json              = "1"
```

### 2 — Register the metadata descriptor

At crate-root scope (unconditional — no feature gate):

```rust
use feldera_adapterlib_meta::{ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction};
use feldera_types::config::FtModel;

#[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
static MY_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "my_connector",          // globally unique transport name
    crate_name: env!("CARGO_CRATE_NAME"),
    direction: Direction::Input,
    kind: ConnectorKind::Regular,
    fault_tolerance: Some(FtModel::ExactlyOnce),
    config_schema: my_config_schema,
    default_format: Some(my_default_format), // or None
    flags: ConnectorFlags::EMPTY,
};
```

### 3 — Implement the builder fn and traits (feature-gated)

```rust
#[cfg(feature = "impl")]
mod impl_module {
    use feldera_adapterlib::transport::{
        InputEndpoint, TransportInputEndpoint, InputReader, InputConsumer,
        InputReaderCommand, Resume,
    };
    use feldera_types::{config::FtModel, program_schema::Relation};
    use serde_json::Value as JsonValue;

    pub struct MyEndpoint { /* config */ }

    impl InputEndpoint for MyEndpoint {
        fn fault_tolerance(&self) -> Option<FtModel> { Some(FtModel::ExactlyOnce) }
    }

    impl TransportInputEndpoint for MyEndpoint {
        fn open(
            &self,
            consumer: Box<dyn InputConsumer>,
            parser:   Box<dyn feldera_adapterlib::format::Parser>,
            schema:   Relation,
            resume_info: Option<JsonValue>,
        ) -> anyhow::Result<Box<dyn InputReader>> {
            // validate, seek, spawn worker, return handle
            todo!()
        }
    }

    // Builder fn — name must match `build_<connector_name>` convention.
    pub fn build_my_connector(
        config: &JsonValue,
        _endpoint_name: &str,
        _secrets_dir: &std::path::Path,
    ) -> anyhow::Result<Box<dyn TransportInputEndpoint>> {
        // deserialize config, return MyEndpoint
        todo!()
    }
}

#[cfg(feature = "impl")]
pub use impl_module::build_my_connector;
```

### 4 — Why the feature split?

The describer binary (which enumerates registered connectors and emits the
manifest JSON) builds your crate with `default-features = false`.  Gating the
`feldera-adapterlib` import behind `#[cfg(feature = "impl")]` keeps the
describer compile fast — it only compiles the metadata side, not the full
data-plane implementation.

### 5 — Builder fn naming convention

pipeline-manager's codegen calls `<crate_name>::build_<name>` where:

* `<crate_name>` comes from the descriptor's `crate_name` field
  (`env!("CARGO_CRATE_NAME")` in normal connectors).
* `<name>` is the connector's `name` field (e.g. `"my_connector"`).

The manifest's `has_build_input`, `has_build_output`, `has_build_integrated_input`,
and `has_build_integrated_output` flags — derived from `direction` and `kind` at
manifest-build time — determine which call signature the codegen uses when invoking
the fn.  Direction is encoded in the connector's `name` (e.g. `"kafka_input"`,
`"kafka_output"`), so there is no `_input` / `_output` suffix in the fn name.

So a connector named `"my_connector"` in crate `my_connector` must export
`pub fn build_my_connector(…)` at the crate root.

### 6 — The worker thread

The controller communicates with the worker via `InputReaderCommand` values on
an unbounded channel.  The worker must handle:

| Command | Action |
|---------|--------|
| `Extend` | Start (or resume) producing records. |
| `Pause` | Stop producing until the next `Extend`. |
| `Queue { checkpoint_requested }` | Flush the current step: call `consumer.extended(amt, Some(resume), watermarks)`. |
| `Replay { metadata, data }` | Re-emit the step identified by `metadata`, then call `consumer.replayed(total, hash)`. |
| `Disconnect` | Tear down; return `Ok(())`. |

### 7 — Fault tolerance

Advertise your FT level via `InputEndpoint::fault_tolerance()`.

**For `FtModel::ExactlyOnce`** (file-based connectors):

1. Track `step_start: u64` (position at the beginning of the current step).
2. On `Queue`: record `step_end = source.position()`. Build a JSON seek payload
   and call:
   ```rust
   consumer.extended(
       step_amt,
       Some(Resume::new_metadata_only(seek_json, hash)),
       watermarks,
   );
   step_start = step_end;
   ```
3. On `Replay { metadata }`: deserialize `metadata`, seek to `start`, re-read
   through `end`, then call `consumer.replayed(total, hash)`.
4. On `open(…, resume_info)`: if `Some`, deserialize and seek to `end` before
   normal read begins.

---

## Registering with Feldera

Add a line to `connectors.toml`.  Paths must be **absolute** — the describer
workspace lives at `<working_dir>/describer/<tenant_id>/<hash>/` and resolves
relative paths against *that* directory.

**Local crate (development / CI):**
```toml
my-connector = { path = "/absolute/path/to/my-connector" }
```

**From the Feldera git repo (operators pinning a release):**
```toml
my-connector = { git = "https://github.com/feldera/feldera", tag = "v0.x.y", package = "my-connector" }
```

Both forms produce the same result: the describer workspace gains the crate as
a Cargo dependency, builds a small binary that iterates
`feldera_adapterlib_meta::metadata_registry()`, and emits a JSON manifest.
pipeline-manager reads this manifest and validates connector names in SQL
`WITH ('connectors' = …)` clauses before compiling the pipeline.

---

## The `hello-lines` reference connector

`src/lib.rs` in this crate implements the full contract:

| Feature | Detail |
|---------|--------|
| `TransportInputEndpoint` | Config deserialization, schema validation, file open, thread spawn. |
| `InputReader` | Channel + `Unparker`; forwards commands to worker thread. |
| Worker thread | Reads lines one at a time, sleeps `interval_ms` ms between each. |
| FT `ExactlyOnce` | Seek datum `{"start": u64, "end": u64}` (byte range of the step). |
| `Queue` | Records `step_end = file.stream_position()`, builds Resume, resets accumulators. |
| `Replay` | Seeks to `start`, re-reads to `end`, calls `consumer.replayed`. |
| `default_format` | JSON with `update_format = "raw"` — no separate format config required. |
| Schema constraint | Exactly one `TEXT`/`VARCHAR`/`CHAR` column; validated in `open()`. |

See `python/tests/platform/test_connector_example.py` for an end-to-end test
that exercises the full `connectors.toml → describer → SQL → pipeline → rows` path.
