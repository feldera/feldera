//! Reference `hello-lines` input connector for Feldera.
//!
//! Reads a plain-text file and emits one line per pipeline step as a
//! single-TEXT-column record.  Intended as a minimal, self-contained example
//! of the Feldera connector plugin surface; see the integration test at
//! `python/tests/platform/test_connector_example.py` for an end-to-end walk-through.
//!
//! # Quick start
//!
//! Add a line to `connectors.toml` (absolute path required — the describer
//! workspace lives in a temporary directory):
//!
//! ```toml
//! connector-example = { path = "/absolute/path/to/feldera/crates/connector-example" }
//! ```
//!
//! Or, when pulling from the official Feldera git repository (typical operator path):
//!
//! ```toml
//! connector-example = { git = "https://github.com/feldera/feldera", tag = "v0.x.y", package = "connector-example" }
//! ```
//!
//! Then create a table with exactly one `TEXT` column and attach the connector:
//!
//! ```sql
//! CREATE TABLE lines (line TEXT)
//!     WITH ('connectors' = '[{
//!         "transport": {
//!             "name": "hello_lines",
//!             "config": {"path": "/data/input.txt", "interval_ms": 0}
//!         }
//!     }]');
//! ```
//!
//! # Configuration
//!
//! | field | type | default | description |
//! |-------|------|---------|-------------|
//! | `path` | string | **required** | Absolute or relative path to the plain-text file. |
//! | `interval_ms` | integer | `1000` | Milliseconds to sleep between emitting consecutive lines.  Set to `0` for fast (test) mode. |
//!
//! # Fault tolerance
//!
//! The connector advertises [`FtModel::ExactlyOnce`].  The seek datum is a
//! JSON object `{"start": u64, "end": u64}` storing the byte range of the
//! step's data inside the file.  Resume jumps directly to `end` in O(1).
//! Replay re-reads `start..end` to reconstruct the exact set of records.
//!
//! # Writing your own connector
//!
//! To write a new input connector:
//!
//! 1. Implement [`InputEndpoint`] + [`TransportInputEndpoint`] on a config-holding struct.
//!    - `fault_tolerance()` — advertise `None`, `AtLeastOnce`, or `ExactlyOnce`.
//!    - `open()` — validate config, open the source, spawn a worker thread,
//!      return a [`InputReader`] that forwards commands over a channel.
//!
//! 2. Implement [`InputReader`] on the handle returned from `open`.
//!    - `request()` — forward the command to the worker thread, then unpark it.
//!    - `is_closed()` — return `true` when the sender is closed (thread exited).
//!
//! 3. In the worker thread, handle the following [`InputReaderCommand`] variants:
//!    - `Extend` — start or resume emitting records.
//!    - `Pause` — stop emitting until the next `Extend`.
//!    - `Queue { checkpoint_requested }` — flush the current step.  Call
//!      [`InputConsumer::extended`] with a [`Resume`] carrying seek metadata.
//!    - `Replay { metadata, data }` — seek to `metadata`, re-emit the step's
//!      records byte-for-byte, then call [`InputConsumer::replayed`].
//!    - `Disconnect` — tear down and return.
//!
//! 4. Declare a [`ConnectorDescriptor`] and register it with
//!    [`feldera_adapterlib::register_connector!`].
//!
//! [`InputEndpoint`]: feldera_adapterlib::transport::InputEndpoint
//! [`TransportInputEndpoint`]: feldera_adapterlib::transport::TransportInputEndpoint
//! [`InputReader`]: feldera_adapterlib::transport::InputReader
//! [`InputConsumer`]: feldera_adapterlib::transport::InputConsumer
//! [`InputReaderCommand`]: feldera_adapterlib::transport::InputReaderCommand
//! [`Resume`]: feldera_adapterlib::transport::Resume
//! [`ConnectorDescriptor`]: feldera_adapterlib::connector::ConnectorDescriptor
//! [`FtModel::ExactlyOnce`]: feldera_types::config::FtModel

use anyhow::{Result as AnyResult, bail};
use chrono::Utc;
use crossbeam::sync::{Parker, Unparker};
use feldera_adapterlib::{
    connector::{ConnectorDescriptor, ConnectorFlags, ConnectorKind, Direction},
    format::{BufferSize, Parser},
    transport::{
        InputConsumer, InputEndpoint, InputReader, InputReaderCommand, Resume,
        TransportInputEndpoint, Watermark, parse_resume_info,
    },
};
use feldera_types::{
    config::{FormatConfig, FtModel},
    format::json::{JsonFlavor, JsonLines, JsonParserConfig, JsonUpdateFormat},
    program_schema::{Relation, SqlType},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    borrow::Cow,
    fs::File,
    hash::Hasher as _,
    io::{BufRead, BufReader, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    thread,
    time::Duration,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::mpsc::error::TryRecvError;
use xxhash_rust::xxh3::Xxh3Default;

// ── Config ────────────────────────────────────────────────────────────────────

/// Runtime configuration supplied by the user in the connector's `config` block.
#[derive(Debug, Deserialize)]
pub struct HelloLinesConfig {
    /// Path to the plain-text input file.
    pub path: String,
    /// Milliseconds to sleep between emitting consecutive lines.
    /// Use `0` for fast (no-delay) mode; defaults to `1000`.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
}

fn default_interval_ms() -> u64 {
    1000
}

// ── Checkpoint metadata ───────────────────────────────────────────────────────

/// Byte range of one step's data in the input file, stored as the seek payload.
///
/// On normal resume the reader seeks to `end`.
/// On replay it seeks to `start` and re-reads up through `end`.
#[derive(Debug, Serialize, Deserialize)]
struct StepMeta {
    start: u64,
    end: u64,
}

// ── HelloLinesEndpoint ────────────────────────────────────────────────────────

struct HelloLinesEndpoint {
    config: HelloLinesConfig,
}

impl InputEndpoint for HelloLinesEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for HelloLinesEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        // Validate: exactly one VARCHAR/TEXT column.
        if schema.fields.len() != 1 {
            bail!(
                "hello_lines: table '{}' must have exactly one column, found {}",
                schema.name.name(),
                schema.fields.len()
            );
        }
        let field = &schema.fields[0];
        if !matches!(field.columntype.typ, SqlType::Varchar | SqlType::Char) {
            bail!(
                "hello_lines: column '{}' must be a TEXT/VARCHAR/CHAR type, got '{}'",
                field.name.name(),
                field.columntype.typ,
            );
        }
        let col_name = field.name.name();

        // Decode resume info (the seek payload from the previous checkpoint).
        let resume_meta: Option<StepMeta> = match resume_info {
            Some(info) => Some(parse_resume_info::<StepMeta>(&info)?),
            None => None,
        };

        // Open the file eagerly so that a bad path fails before the pipeline starts.
        let path = PathBuf::from(&self.config.path);
        let file = File::open(&path).map_err(|e| {
            anyhow::anyhow!("hello_lines: failed to open '{}': {e}", path.display())
        })?;

        let (sender, receiver) = unbounded_channel();
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        let interval_ms = self.config.interval_ms;

        thread::Builder::new()
            .name("hello-lines-input".into())
            .spawn({
                move || {
                    if let Err(e) = worker_thread(
                        file,
                        col_name,
                        interval_ms,
                        consumer.as_ref(),
                        parser,
                        receiver,
                        resume_meta,
                        parker,
                    ) {
                        consumer.error(true, e, None);
                    }
                }
            })
            .expect("failed to spawn hello-lines-input thread");

        Ok(Box::new(HelloLinesReader { sender, unparker }))
    }
}

// ── HelloLinesReader ──────────────────────────────────────────────────────────

struct HelloLinesReader {
    sender: UnboundedSender<InputReaderCommand>,
    unparker: Unparker,
}

impl InputReader for HelloLinesReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.sender.send(command);
        self.unparker.unpark();
    }

    fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

impl Drop for HelloLinesReader {
    fn drop(&mut self) {
        self.disconnect();
    }
}

// ── Worker thread ─────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn worker_thread(
    file: File,
    col_name: String,
    interval_ms: u64,
    consumer: &dyn InputConsumer,
    mut parser: Box<dyn Parser>,
    mut receiver: UnboundedReceiver<InputReaderCommand>,
    resume_info: Option<StepMeta>,
    parker: Parker,
) -> AnyResult<()> {
    let mut reader = BufReader::new(file);

    // Jump past data already committed in the checkpointed step.
    if let Some(ref meta) = resume_info {
        reader.seek(SeekFrom::Start(meta.end))?;
    }

    let mut step_start: u64 = reader.stream_position()?;
    let mut step_amt = BufferSize::default();
    let mut step_hasher = consumer.hasher();
    let mut step_timestamp = None;
    let mut extending = false;
    let mut eof = false;

    loop {
        // Drain all pending commands before doing I/O.
        loop {
            match receiver.try_recv() {
                Ok(InputReaderCommand::Extend) => extending = true,

                Ok(InputReaderCommand::Pause) => extending = false,

                Ok(InputReaderCommand::Queue { .. }) => {
                    let step_end = reader.stream_position()?;
                    let seek = serde_json::to_value(StepMeta {
                        start: step_start,
                        end: step_end,
                    })
                    .unwrap();
                    let hash = step_hasher.as_ref().map(|h| h.finish());
                    let watermarks = step_timestamp
                        .take()
                        .map(|ts| vec![Watermark::new(ts, None)])
                        .unwrap_or_default();
                    consumer.extended(
                        step_amt,
                        Some(Resume::new_metadata_only(seek, hash)),
                        watermarks,
                    );
                    // Reset per-step accumulators.
                    step_start = step_end;
                    step_amt = BufferSize::default();
                    step_hasher = consumer.hasher();
                }

                Ok(InputReaderCommand::Replay { metadata, .. }) => {
                    let meta: StepMeta = parse_resume_info(&metadata)?;
                    replay_step(&mut reader, &col_name, consumer, &mut *parser, &meta)?;
                }

                Ok(InputReaderCommand::Disconnect) => return Ok(()),

                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        if !extending || eof {
            parker.park();
            continue;
        }

        // Read the next line from the file.
        let mut raw = String::new();
        let n = reader.read_line(&mut raw)?;
        if n == 0 {
            eof = true;
            consumer.eoi();
            parker.park();
            continue;
        }

        // Strip the trailing newline (handles both LF and CRLF).
        let line = raw.trim_end_matches('\n').trim_end_matches('\r');
        let json_bytes = encode_record(&col_name, line);

        let (buf_opt, errors) = parser.parse(&json_bytes, None);
        consumer.parse_errors(errors);

        if let Some(mut buf) = buf_opt {
            if step_timestamp.is_none() {
                step_timestamp = Some(Utc::now());
            }
            let amt = buf.len();
            step_amt += amt;
            if let Some(ref mut h) = step_hasher {
                buf.hash(h);
            }
            buf.flush();
            consumer.buffered(amt);
        }

        if interval_ms > 0 {
            thread::sleep(Duration::from_millis(interval_ms));
        }
    }
}

/// Seek to `meta.start`, re-parse every line up through `meta.end`, then call
/// [`InputConsumer::replayed`] with the accumulated record count and hash.
fn replay_step(
    reader: &mut BufReader<File>,
    col_name: &str,
    consumer: &dyn InputConsumer,
    parser: &mut dyn Parser,
    meta: &StepMeta,
) -> AnyResult<()> {
    reader.seek(SeekFrom::Start(meta.start))?;

    let mut total = BufferSize::default();
    let mut hasher = Xxh3Default::new();

    loop {
        let pos = reader.stream_position()?;
        if pos >= meta.end {
            break;
        }
        let mut raw = String::new();
        let n = reader.read_line(&mut raw)?;
        if n == 0 {
            break;
        }
        let line = raw.trim_end_matches('\n').trim_end_matches('\r');
        let json_bytes = encode_record(col_name, line);

        let (buf_opt, errors) = parser.parse(&json_bytes, None);
        consumer.parse_errors(errors);
        if let Some(mut buf) = buf_opt {
            let amt = buf.len();
            total += amt;
            buf.hash(&mut hasher);
            buf.flush();
            consumer.buffered(amt);
        }
    }

    // Guarantee the file position is exactly at meta.end regardless of line boundaries.
    reader.seek(SeekFrom::Start(meta.end))?;
    consumer.replayed(total, hasher.finish());
    Ok(())
}

/// Encode one text line as a newline-terminated JSON record: `{"col": "value"}\n`.
fn encode_record(col_name: &str, line: &str) -> Vec<u8> {
    let obj = serde_json::json!({ col_name: line });
    let mut bytes = serde_json::to_vec(&obj).expect("serde_json::to_vec should not fail");
    bytes.push(b'\n');
    bytes
}

// ── Descriptor callbacks ──────────────────────────────────────────────────────

fn hello_lines_config_schema() -> JsonValue {
    serde_json::json!({
        "type": "object",
        "required": ["path"],
        "properties": {
            "path": {
                "type": "string",
                "description": "Path to the plain-text input file."
            },
            "interval_ms": {
                "type": "integer",
                "minimum": 0,
                "default": 1000,
                "description": "Milliseconds to sleep between emitting consecutive lines. 0 = no delay."
            }
        }
    })
}

fn hello_lines_default_format() -> FormatConfig {
    FormatConfig {
        name: Cow::from("json"),
        config: serde_json::to_value(JsonParserConfig {
            update_format: JsonUpdateFormat::Raw,
            json_flavor: JsonFlavor::Default,
            array: false,
            lines: JsonLines::Single,
        })
        .expect("JsonParserConfig serialization should not fail"),
    }
}

fn build_hello_lines(
    config: &JsonValue,
    _endpoint_name: &str,
    _secrets_dir: &std::path::Path,
) -> AnyResult<Box<dyn TransportInputEndpoint>> {
    let config: HelloLinesConfig = serde_json::from_value(config.clone())?;
    Ok(Box::new(HelloLinesEndpoint { config }))
}

// ── Static descriptor + registry entry ───────────────────────────────────────

/// Connector descriptor for `hello_lines`.
///
/// The `register_connector!` call below submits this descriptor to the global
/// `inventory` registry so that `registered_connectors()` and `connector_by_name()`
/// in `feldera-adapterlib` can discover it at runtime.
static HELLO_LINES_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
    name: "hello_lines",
    direction: Direction::Input,
    kind: ConnectorKind::Regular,
    fault_tolerance: Some(FtModel::ExactlyOnce),
    config_schema: hello_lines_config_schema,
    default_format: Some(hello_lines_default_format),
    flags: ConnectorFlags::EMPTY,
    build_input: Some(build_hello_lines),
    build_output: None,
    build_integrated_input: None,
    build_integrated_output: None,
};

feldera_adapterlib::register_connector!(&HELLO_LINES_DESCRIPTOR);

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn config_deserializes_with_defaults() {
        let v = serde_json::json!({ "path": "/tmp/test.txt" });
        let cfg: HelloLinesConfig = serde_json::from_value(v).unwrap();
        assert_eq!(cfg.path, "/tmp/test.txt");
        assert_eq!(cfg.interval_ms, 1000);
    }

    #[test]
    fn config_deserializes_interval_override() {
        let v = serde_json::json!({ "path": "/tmp/test.txt", "interval_ms": 0 });
        let cfg: HelloLinesConfig = serde_json::from_value(v).unwrap();
        assert_eq!(cfg.interval_ms, 0);
    }

    #[test]
    fn config_missing_path_fails() {
        let v = serde_json::json!({ "interval_ms": 0 });
        assert!(serde_json::from_value::<HelloLinesConfig>(v).is_err());
    }

    #[test]
    fn step_meta_roundtrip() {
        let meta = StepMeta { start: 42, end: 100 };
        let json = serde_json::to_value(&meta).unwrap();
        let decoded: StepMeta = serde_json::from_value(json).unwrap();
        assert_eq!(decoded.start, 42);
        assert_eq!(decoded.end, 100);
    }

    #[test]
    fn encode_record_produces_valid_json() {
        let bytes = encode_record("line", "hello world");
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.trim_end().starts_with('{'));
        let v: serde_json::Value = serde_json::from_str(text.trim_end()).unwrap();
        assert_eq!(v["line"], "hello world");
    }

    #[test]
    fn encode_record_ends_with_newline() {
        let bytes = encode_record("x", "val");
        assert_eq!(bytes.last(), Some(&b'\n'));
    }

    #[test]
    fn config_schema_is_valid_json_object() {
        let schema = hello_lines_config_schema();
        assert!(schema.is_object());
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["path"].is_object());
        assert!(schema["properties"]["interval_ms"].is_object());
    }

    #[test]
    fn default_format_is_json() {
        let fmt = hello_lines_default_format();
        assert_eq!(fmt.name.as_ref(), "json");
        let cfg: JsonParserConfig = serde_json::from_value(fmt.config).unwrap();
        assert!(matches!(cfg.update_format, JsonUpdateFormat::Raw));
        assert!(!cfg.array);
    }

    /// Smoke-test that seek/resume metadata round-trips through `parse_resume_info`.
    #[test]
    fn parse_resume_info_roundtrip() {
        let original = StepMeta { start: 0, end: 50 };
        let seek = serde_json::to_value(&original).unwrap();
        let decoded: StepMeta = parse_resume_info(&seek).unwrap();
        assert_eq!(decoded.start, 0);
        assert_eq!(decoded.end, 50);
    }

    /// Verify that the connector name appears in `registered_connectors()` once
    /// the crate is linked.  This exercises the `register_connector!` macro.
    #[test]
    fn connector_is_registered() {
        use feldera_adapterlib::connector::{connector_by_name, registered_connectors};
        let names: Vec<&str> = registered_connectors().map(|d| d.name).collect();
        assert!(
            names.contains(&"hello_lines"),
            "hello_lines not found in registered_connectors; got {names:?}"
        );
        let desc = connector_by_name("hello_lines").expect("hello_lines not found by name");
        assert_eq!(desc.direction, Direction::Input);
        assert_eq!(desc.kind, ConnectorKind::Regular);
        assert_eq!(desc.fault_tolerance, Some(FtModel::ExactlyOnce));
        assert!(desc.build_input.is_some());
    }

    /// Write a 3-line file, measure its byte length, then verify that reading
    /// line-by-line with a positional guard stops at the correct offset —
    /// the same logic used by `replay_step`.
    #[test]
    fn line_reading_honours_byte_range() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "alpha").unwrap();
        writeln!(tmp, "beta").unwrap();
        writeln!(tmp, "gamma").unwrap();
        tmp.flush().unwrap();

        let file_len = std::fs::metadata(tmp.path()).unwrap().len();

        let file = File::open(tmp.path()).unwrap();
        let mut reader = BufReader::new(file);

        let end = file_len;
        let mut line_count = 0usize;
        loop {
            let pos = reader.stream_position().unwrap();
            if pos >= end { break; }
            let mut raw = String::new();
            let n = reader.read_line(&mut raw).unwrap();
            if n == 0 { break; }
            line_count += 1;
        }
        assert_eq!(line_count, 3);

        // Seeking to a mid-file offset and reading from there produces the remaining lines.
        let first_line_len = "alpha\n".len() as u64;
        reader.seek(SeekFrom::Start(first_line_len)).unwrap();
        let mut rest = 0usize;
        loop {
            let pos = reader.stream_position().unwrap();
            if pos >= end { break; }
            let mut raw = String::new();
            let n = reader.read_line(&mut raw).unwrap();
            if n == 0 { break; }
            rest += 1;
        }
        assert_eq!(rest, 2, "expected 2 lines after skipping the first");
    }
}
