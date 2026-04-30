# Writing a Feldera Connector

This document explains how to build a custom I/O connector for Feldera using the
`feldera-adapterlib` plugin API.  The `connector-example` crate (this directory)
implements the `hello-lines` input connector and serves as the canonical reference.

---

## Prerequisites

* Rust 1.70+.
* `feldera-adapterlib` (and `feldera-types` for shared config types) as Cargo
  dependencies.  **No other Feldera crates are required.**

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

[dependencies]
feldera-adapterlib = { path = "/path/to/feldera/crates/adapterlib" }
feldera-types      = { path = "/path/to/feldera/crates/feldera-types" }
inventory          = "0.3"   # required by register_connector!
anyhow             = "1"
serde              = { version = "1", features = ["derive"] }
serde_json         = "1"
```

### 2 — Implement the two traits

```rust
use feldera_adapterlib::transport::{
    InputEndpoint, TransportInputEndpoint, InputReader, InputConsumer,
    InputReaderCommand, Resume, Watermark,
};
use feldera_types::{config::FtModel, program_schema::Relation};
use serde_json::Value as JsonValue;

struct MyEndpoint { /* config */ }

impl InputEndpoint for MyEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for MyEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser:   Box<dyn feldera_adapterlib::format::Parser>,
        schema:   Relation,
        resume_info: Option<JsonValue>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        // 1. Validate schema / config.
        // 2. Decode resume_info (if any) to seek to the right position.
        // 3. Spawn a background thread.
        // 4. Return a channel-based InputReader handle.
        todo!()
    }
}
```

### 3 — The worker thread

The controller communicates with the worker thread by sending
`InputReaderCommand` values over an unbounded channel.  The worker must handle:

| Command | Action |
|---------|--------|
| `Extend` | Start (or resume) producing records. |
| `Pause` | Stop producing until the next `Extend`. |
| `Queue { checkpoint_requested }` | Flush the current step: call `consumer.extended(amt, Some(resume), watermarks)`. |
| `Replay { metadata, data }` | Re-emit the step identified by `metadata`, then call `consumer.replayed(total, hash)`. |
| `Disconnect` | Tear down; return `Ok(())`. |

### 4 — Fault tolerance

The connector advertises its FT level via `InputEndpoint::fault_tolerance()`.

**For `FtModel::ExactlyOnce`** (file-based connectors):

1. Track a `step_start: u64` (position at the beginning of the current step).
2. On `Queue`: record `step_end = source.position()`.  Build a JSON seek payload
   `{"start": step_start, "end": step_end}` and call:
   ```rust
   consumer.extended(
       step_amt,
       Some(Resume::new_metadata_only(seek, hash)),
       watermarks,
   );
   step_start = step_end;
   ```
3. On `Replay { metadata }`: deserialize `metadata` as your seek struct, seek to
   `start`, re-read up through `end`, then call `consumer.replayed(total, hash)`.
4. On `open(…, resume_info)`: if `resume_info` is `Some`, deserialize it as your
   seek struct and seek to `end` before beginning normal read.

**Choosing a seek datum**:

* **Byte offset** (this example): O(1) resume, works for any sequential source.
* **Kafka offset**: partition + offset tuple per partition.
* **Timestamp / version**: for sources that support time-travel (Delta Lake, etc.).

### 5 — Register the descriptor

```rust
use feldera_adapterlib::connector::{
    ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction,
};
use feldera_types::config::FtModel;

static MY_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "my_connector",       // must be globally unique
    direction: Direction::Input,
    kind: ConnectorKind::Regular,
    fault_tolerance: Some(FtModel::ExactlyOnce),
    config_schema: my_config_schema,
    default_format: Some(my_default_format),  // or None
    flags: ConnectorFlags::EMPTY,
    build_input: Some(build_my_connector),
    build_output: None,
    build_integrated_input: None,
    build_integrated_output: None,
};

feldera_adapterlib::register_connector!(&MY_DESCRIPTOR);
```

The `register_connector!` macro submits the descriptor to the `inventory`
registry so `registered_connectors()` and `connector_by_name()` discover it
at runtime.

---

## Registering with Feldera

### Using a local crate (development / CI)

In `connectors.toml` (absolute paths are required — the describer workspace
lives in a temporary directory):

```toml
my-connector = { path = "/absolute/path/to/my-connector" }
```

### Using the Feldera git repo (operators pinning a release)

```toml
my-connector = {
    git     = "https://github.com/feldera/feldera",
    tag     = "v0.x.y",
    package = "connector-example"
}
```

Both forms produce the same result: the describer workspace gains the crate
as a Cargo dependency, builds a small binary that invokes `registered_connectors()`,
and emits a JSON manifest.  The pipeline-manager reads this manifest and
validates connector names in SQL `WITH ('connectors' = …)` clauses before
compiling the pipeline.

> **Note**: paths in `connectors.toml` must be **absolute**.  The describer
> workspace lives at `<working_dir>/describer/<tenant_id>/<content_hash>/` and
> resolves relative paths against *that* directory, not the operator's CWD.

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
