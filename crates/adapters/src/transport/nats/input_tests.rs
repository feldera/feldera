//! The NATS input connector on its own: reading a JetStream stream, retrying while the
//! server or the stream is unavailable, replaying ranges of sequence numbers, and stopping on
//! disconnect.
//!
//! Each case is a sequence of `NatsMockAction`s that `run_nats_mock_test` executes against a
//! private `nats-server`.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::{Error as AnyError, Result as AnyResult};
use feldera_adapterlib::test::{
    ConnectorTestEngine, InputOptions, MockInputConsumer, TestCollection, Updates,
    default_timeout_ms, wait,
};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use serde_json::{Value as JsonValue, json};

use crate::test::nats::{self as util, STREAM_NAME, SUBJECT_NAME};
use crate::test::{init_test_logger, input_pipeline, published_schema};
use crate::transport::InputReader;

/// The table that each case reads the stream into.
pub fn relation() -> Relation {
    Relation::new(
        "nats_input".into(),
        vec![
            Field::new("s".into(), ColumnType::varchar(false)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Atomic actions for the declarative cases of these tests.
///
/// Each test is expressed as a sequence of actions executed by
/// [`run_nats_mock_test`]. The runner maintains internal state (NATS process
/// guard, endpoint, mock input consumer, published records, etc.) and executes actions
/// in order.
#[derive(Clone, Debug)]
enum NatsMockAction {
    /// Start a NATS server process. Stores guard + URL in runner state.
    StartNats,
    /// Create the JetStream stream (hardcoded stream="str", subject="sub").
    CreateStream,
    /// Call [`input_pipeline`], wire up the `on_error` callback.
    CreatePipeline,

    /// Publish `n` deterministic records using the runner's auto-increment counter.
    /// Record `i` is `{"s": "msg{i}", "b": i % 2 == 0, "i": i}`.
    ///
    /// Records are appended to the runner's 1-based `published` vec so that
    /// `published[seq]` is the record with JetStream sequence number `seq`.
    Publish(usize),

    /// Call `endpoint.extend()` to start reading.
    Extend,
    /// Call `endpoint.pause()` to stop ingest/retry loop.
    Pause,
    /// Queue repeatedly until `n` total records have been flushed to the collection.
    /// Aborts early with an error if a fatal endpoint error is detected.
    WaitForRecords(usize),
    /// Like `WaitForRecords`, but tolerates non-fatal endpoint errors as long
    /// as a fatal error never occurs.
    WaitForRecordsNoFatal(usize),
    /// Issue a replay for the half-open JetStream sequence range `[start, end)`.
    ///
    /// JetStream sequences are 1-based (first message = sequence 1), and
    /// the runner's `published` vec uses the same numbering, so sequence `s`
    /// is simply `published[s]`.
    Replay { start: u64, end: u64 },
    /// Call `endpoint.disconnect()`.
    Disconnect,
    /// Disconnect without asserting absence of non-fatal endpoint errors.
    DisconnectAllowNonFatal,

    /// Sleep for the given duration.
    Sleep(Duration),

    /// Drop the NATS process guard (kills the server).
    KillServer,
    /// Restart NATS on the same TCP port used previously.
    RestartNatsSamePort,
    /// Delete the stream via the JetStream API.
    DeleteStream,

    /// Wait until `n` records have been flushed to the collection by replay.
    /// Unlike `WaitForRecords`, this does not call `endpoint.queue()`:
    /// replay flushes records directly via `buffer.flush()`.
    WaitForReplayedRecords(usize),
    /// Like `WaitForReplayedRecords`, but tolerates non-fatal endpoint errors
    /// while still asserting that no fatal error occurred.
    WaitForReplayedRecordsNoFatal(usize),
    /// Purge all messages from the JetStream stream.
    PurgeStream,

    /// Assert that a fatal error appears within [min, max] time window.
    ExpectFatalErrorWithin { min: Duration, max: Duration },
    /// Assert that a fatal error appears within `timeout` and includes `needle`.
    ExpectFatalErrorContains {
        timeout: Duration,
        needle: &'static str,
    },
    /// Wait until `error_count` reaches at least `count`.
    WaitForErrorCountAtLeast { count: usize, timeout: Duration },
    /// Assert that no additional endpoint errors are reported for `duration`.
    AssertNoErrorCountIncrease { duration: Duration },

    /// Verify that `count` output records starting at `output_index` match
    /// the corresponding published records. The NATS sequence is derived
    /// automatically (`output_index + 1`), since `published` is 1-based.
    VerifyRecords { output_index: usize, count: usize },
    /// Verify that the output records starting at `output_index` match
    /// the published records starting at JetStream sequence `nats_seq`.
    ///
    /// `output_index` is a 0-based index into the rows flushed to the collection.
    /// `nats_seq` is a 1-based JetStream sequence number that indexes
    /// directly into the runner's `published` vec.
    VerifyOutputSlice {
        output_index: usize,
        nats_seq: usize,
        count: usize,
    },
    /// Assert that the collection holds exactly `n` flushed records (no waiting).
    AssertRecordCount(usize),
}

/// Internal state held by the test runner.
struct NatsMockRunner<'a> {
    engine: &'a dyn ConnectorTestEngine,
    nats_guard: Option<util::ProcessKillGuard>,
    nats_url: Option<String>,
    /// Published records, 1-indexed to match JetStream sequence numbers.
    /// `published[seq]` is the record with JetStream sequence number `seq`.
    /// Index 0 is a dummy sentinel and must never be accessed.
    published: Vec<JsonValue>,
    endpoint: Option<Box<dyn InputReader>>,
    mock_input_consumer: Option<MockInputConsumer>,
    collection: Option<Box<dyn TestCollection>>,
    pipeline_create_error: Option<AnyError>,
    got_fatal: Arc<AtomicBool>,
    error_count: Arc<AtomicUsize>,
    rt: tokio::runtime::Runtime,
}

impl<'a> NatsMockRunner<'a> {
    fn new(engine: &'a dyn ConnectorTestEngine) -> AnyResult<Self> {
        Ok(Self {
            engine,
            nats_guard: None,
            nats_url: None,
            // Sentinel at index 0 so that `published[seq]` works directly
            // with 1-based JetStream sequence numbers.
            published: vec![json!({"s": "", "b": false, "i": -1})],
            endpoint: None,
            mock_input_consumer: None,
            collection: None,
            pipeline_create_error: None,
            got_fatal: Arc::new(AtomicBool::new(false)),
            error_count: Arc::new(AtomicUsize::new(0)),
            rt: tokio::runtime::Runtime::new()?,
        })
    }

    fn nats_url(&self) -> &str {
        self.nats_url
            .as_deref()
            .expect("StartNats must be called before this action")
    }

    fn endpoint(&self) -> &dyn InputReader {
        if let Some(endpoint) = self.endpoint.as_deref() {
            endpoint
        } else if let Some(error) = self.pipeline_create_error.as_ref() {
            panic!("CreatePipeline failed: {error:#}");
        } else {
            panic!("CreatePipeline must be called before this action");
        }
    }

    fn mock_input_consumer(&self) -> &MockInputConsumer {
        if let Some(consumer) = self.mock_input_consumer.as_ref() {
            consumer
        } else if let Some(error) = self.pipeline_create_error.as_ref() {
            panic!("CreatePipeline failed: {error:#}");
        } else {
            panic!("CreatePipeline must be called before this action");
        }
    }

    /// Waits for the worker task to fully exit after `disconnect()`.
    ///
    /// `disconnect()` only enqueues a command; the background reader task
    /// keeps pulling and queuing messages until the worker task processes it
    /// and cancels the reader. Without waiting here, a caller that publishes
    /// more records right after `disconnect()` races the background reader,
    /// which can still pick them up and have a stale, already-queued `Queue`
    /// command flush them. `is_closed()` only goes true once the worker task
    /// has returned and dropped the command channel, so waiting for it here
    /// proves the reader is gone before the caller does anything else.
    fn wait_for_disconnect(&self) -> AnyResult<()> {
        let endpoint = self.endpoint();
        wait(|| endpoint.is_closed(), default_timeout_ms())
            .map_err(|()| anyhow::anyhow!("Timed out waiting for endpoint to disconnect"))?;
        Ok(())
    }

    fn collection(&self) -> &dyn TestCollection {
        if let Some(collection) = self.collection.as_deref() {
            collection
        } else if let Some(error) = self.pipeline_create_error.as_ref() {
            panic!("CreatePipeline failed: {error:#}");
        } else {
            panic!("CreatePipeline must be called before this action");
        }
    }

    /// Returns the rows that the collection received at the positions in `range`, with their
    /// weights.
    fn received(&self, range: Range<usize>) -> Updates {
        let updates = self.collection().updates().unwrap();
        let indices: Vec<u32> = range.clone().map(|index| index as u32).collect();
        updates
            .take(&indices, updates.weights()[range].to_vec())
            .unwrap()
    }

    /// Returns the published records with the JetStream sequence numbers in `sequences`, each
    /// inserted once.
    fn published(&self, sequences: Range<usize>) -> Updates {
        let rows: Vec<String> = self.published[sequences]
            .iter()
            .map(|record| record.to_string())
            .collect();
        let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
        Updates::from_json(published_schema(&relation()), &rows).unwrap()
    }

    fn exec(
        &mut self,
        action: &NatsMockAction,
        pipeline_config: &dyn Fn(&str) -> serde_json::Value,
    ) -> AnyResult<()> {
        match action {
            NatsMockAction::StartNats => {
                let (guard, url) = util::start_nats_and_get_address()?;
                self.nats_guard = Some(guard);
                self.nats_url = Some(url);
            }

            NatsMockAction::CreateStream => {
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::create_stream(&nats_url, STREAM_NAME, SUBJECT_NAME))?;
            }

            NatsMockAction::CreatePipeline => {
                let config = pipeline_config(self.nats_url());
                println!("Config:\n{config:#}");

                match input_pipeline(
                    self.engine,
                    &relation(),
                    &InputOptions::default(),
                    serde_json::from_value(config).unwrap(),
                ) {
                    Ok(pipeline) => {
                        // Reset error state so a fresh pipeline starts clean.
                        self.got_fatal.store(false, Ordering::Release);
                        self.error_count.store(0, Ordering::Release);
                        self.pipeline_create_error = None;

                        let got_fatal_clone = self.got_fatal.clone();
                        let error_count_clone = self.error_count.clone();
                        pipeline
                            .consumer
                            .on_error(Some(Box::new(move |fatal, _err| {
                                error_count_clone.fetch_add(1, Ordering::AcqRel);
                                if fatal {
                                    got_fatal_clone.store(true, Ordering::Release);
                                }
                            })));

                        self.endpoint = Some(pipeline.reader);
                        self.mock_input_consumer = Some(pipeline.consumer);
                        self.collection = Some(pipeline.collection);
                    }
                    Err(error) => {
                        self.endpoint = None;
                        self.mock_input_consumer = None;
                        self.collection = None;
                        self.pipeline_create_error = Some(error);
                    }
                }
            }

            NatsMockAction::Publish(n) => {
                let start = self.published.len();
                let records: Vec<JsonValue> = (start..start + n)
                    .map(|i| json!({"s": format!("msg{i}"), "b": i % 2 == 0, "i": i as i64}))
                    .collect();
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::publish_json(&nats_url, SUBJECT_NAME, &records))?;
                self.published.extend(records);
            }

            NatsMockAction::Extend => {
                self.endpoint().extend();
            }

            NatsMockAction::Pause => {
                self.endpoint().pause();
            }

            NatsMockAction::WaitForRecords(n) => {
                let n = *n;
                let endpoint = self.endpoint();
                let collection = self.collection();
                let mock_input_consumer = self.mock_input_consumer();
                wait(
                    || {
                        endpoint.queue(false);
                        if mock_input_consumer.state().endpoint_error.is_some() {
                            return true;
                        }
                        collection.len() >= n
                    },
                    default_timeout_ms(),
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} records (got {})",
                        collection.len()
                    )
                })?;
                assert!(
                    mock_input_consumer.state().endpoint_error.is_none(),
                    "Unexpected endpoint error while waiting for records: {:?}",
                    mock_input_consumer.state().endpoint_error
                );
                assert!(
                    collection.len() >= n,
                    "Expected at least {n} records, got {}",
                    collection.len()
                );
            }

            NatsMockAction::WaitForRecordsNoFatal(n) => {
                let n = *n;
                let endpoint = self.endpoint();
                let collection = self.collection();
                wait(
                    || {
                        endpoint.queue(false);
                        if self.got_fatal.load(Ordering::Acquire) {
                            return true;
                        }
                        collection.len() >= n
                    },
                    default_timeout_ms(),
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} records without fatal error (got {})",
                        collection.len()
                    )
                })?;
                assert!(
                    !self.got_fatal.load(Ordering::Acquire),
                    "Unexpected fatal endpoint error while waiting for records: {:?}",
                    self.mock_input_consumer().state().endpoint_error
                );
                assert!(
                    collection.len() >= n,
                    "Expected at least {n} records, got {}",
                    collection.len()
                );
            }

            NatsMockAction::Replay { start, end } => {
                let metadata = json!({
                    "sequence_numbers": {
                        "start": *start,
                        "end": *end
                    }
                });
                self.endpoint().replay(metadata, rmpv::Value::Nil);
            }

            NatsMockAction::Disconnect => {
                assert!(
                    !self.mock_input_consumer().state().eoi,
                    "Streaming NATS connector should never signal end-of-input"
                );
                // Unless a fatal error was already expected, assert that no
                // errors have occurred before disconnecting.
                if !self.got_fatal.load(Ordering::Acquire) {
                    assert!(
                        self.mock_input_consumer().state().endpoint_error.is_none(),
                        "No endpoint error should have occurred"
                    );
                }
                self.endpoint().disconnect();
                self.wait_for_disconnect()?;
            }

            NatsMockAction::DisconnectAllowNonFatal => {
                assert!(
                    !self.mock_input_consumer().state().eoi,
                    "Streaming NATS connector should never signal end-of-input"
                );
                self.endpoint().disconnect();
                self.wait_for_disconnect()?;
            }

            NatsMockAction::Sleep(dur) => {
                sleep(*dur);
            }

            NatsMockAction::KillServer => {
                self.nats_guard
                    .take()
                    .expect("KillServer: no NATS server to kill");
            }

            NatsMockAction::RestartNatsSamePort => {
                let current_url = self.nats_url().to_string();
                let port = current_url
                    .rsplit_once(':')
                    .ok_or_else(|| anyhow::anyhow!("Invalid NATS URL: {current_url}"))?
                    .1
                    .parse::<u16>()
                    .map_err(|e| anyhow::anyhow!("Invalid NATS URL port in {current_url}: {e}"))?;
                let (guard, url) = util::start_nats_on_port(port)?;
                self.nats_guard = Some(guard);
                self.nats_url = Some(url);
            }

            NatsMockAction::DeleteStream => {
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::delete_stream(&nats_url, STREAM_NAME))?;
            }

            NatsMockAction::WaitForReplayedRecords(n) => {
                let n = *n;
                let collection = self.collection();
                let mock_input_consumer = self.mock_input_consumer();
                wait(
                    || {
                        if mock_input_consumer.state().endpoint_error.is_some() {
                            return true;
                        }
                        collection.len() >= n
                    },
                    default_timeout_ms(),
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} replayed records (got {})",
                        collection.len()
                    )
                })?;
                assert!(
                    mock_input_consumer.state().endpoint_error.is_none(),
                    "Unexpected endpoint error while waiting for replayed records: {:?}",
                    mock_input_consumer.state().endpoint_error
                );
            }

            NatsMockAction::WaitForReplayedRecordsNoFatal(n) => {
                let n = *n;
                let collection = self.collection();
                wait(
                    || {
                        if self.got_fatal.load(Ordering::Acquire) {
                            return true;
                        }
                        collection.len() >= n
                    },
                    default_timeout_ms(),
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} replayed records without fatal error (got {})",
                        collection.len()
                    )
                })?;
                assert!(
                    !self.got_fatal.load(Ordering::Acquire),
                    "Unexpected fatal endpoint error while waiting for replayed records: {:?}",
                    self.mock_input_consumer().state().endpoint_error
                );
            }

            NatsMockAction::PurgeStream => {
                let nats_url = self.nats_url().to_string();
                self.rt
                    .block_on(util::purge_stream(&nats_url, STREAM_NAME))?;
            }

            NatsMockAction::ExpectFatalErrorWithin { min, max } => {
                let min_ms = min.as_millis();
                let max_ms = max.as_millis();
                let endpoint = self.endpoint();
                let mock_input_consumer = self.mock_input_consumer();
                let start = Instant::now();
                wait(
                    || {
                        endpoint.queue(false);
                        mock_input_consumer.state().endpoint_error.is_some()
                    },
                    max_ms,
                )
                .map_err(|()| {
                    anyhow::anyhow!("Timed out after {max_ms}ms waiting for fatal error")
                })?;
                let elapsed_ms = start.elapsed().as_millis();
                assert!(
                    elapsed_ms >= min_ms,
                    "Fatal error arrived too early: {elapsed_ms}ms < {min_ms}ms"
                );
                assert!(
                    elapsed_ms <= max_ms,
                    "Fatal error arrived too late: {elapsed_ms}ms > {max_ms}ms"
                );
                assert!(
                    self.got_fatal.load(Ordering::Acquire),
                    "Error should be fatal"
                );
            }

            NatsMockAction::ExpectFatalErrorContains { timeout, needle } => {
                if let Some(error) = self.pipeline_create_error.as_ref() {
                    let error_text = format!("{error:#}");
                    assert!(
                        error_text.contains(needle),
                        "Expected fatal error to contain '{needle}', got: {error_text}"
                    );
                    return Ok(());
                }

                let timeout_ms = timeout.as_millis();
                let endpoint = self.endpoint();
                let mock_input_consumer = self.mock_input_consumer();
                wait(
                    || {
                        endpoint.queue(false);
                        mock_input_consumer.state().endpoint_error.is_some()
                    },
                    timeout_ms,
                )
                .map_err(|()| {
                    anyhow::anyhow!("Timed out after {timeout_ms}ms waiting for fatal error")
                })?;

                let error_text = format!(
                    "{:#}",
                    mock_input_consumer
                        .state()
                        .endpoint_error
                        .as_ref()
                        .expect("fatal error should be present")
                );

                assert!(
                    self.got_fatal.load(Ordering::Acquire),
                    "Error should be fatal"
                );
                assert!(
                    error_text.contains(needle),
                    "Expected fatal error to contain '{needle}', got: {error_text}"
                );
            }

            NatsMockAction::WaitForErrorCountAtLeast { count, timeout } => {
                let timeout_ms = timeout.as_millis();
                let endpoint = self.endpoint();
                let count = *count;
                wait(
                    || {
                        endpoint.queue(false);
                        self.error_count.load(Ordering::Acquire) >= count
                    },
                    timeout_ms,
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out after {timeout_ms}ms waiting for endpoint error count >= {count}; got {}",
                        self.error_count.load(Ordering::Acquire)
                    )
                })?;
            }

            NatsMockAction::AssertNoErrorCountIncrease { duration } => {
                let start = self.error_count.load(Ordering::Acquire);
                sleep(*duration);
                let end = self.error_count.load(Ordering::Acquire);
                assert_eq!(
                    start, end,
                    "Expected no additional endpoint errors during {:?}, but count increased from {start} to {end}",
                    duration
                );
            }

            NatsMockAction::VerifyRecords {
                output_index,
                count,
            } => {
                let flushed = self.collection().len();
                assert!(
                    flushed >= output_index + count,
                    "VerifyRecords: expected at least {} flushed records, got {flushed}",
                    output_index + count,
                );
                // The output is 0-based, published is 1-based (index = sequence number).
                let seq = output_index + 1;
                assert_eq!(
                    self.received(*output_index..output_index + count),
                    self.published(seq..seq + count),
                    "Record mismatch: output[{output_index}..] vs published[{seq}..] (seq {seq})",
                );
            }

            NatsMockAction::VerifyOutputSlice {
                output_index,
                nats_seq,
                count,
            } => {
                let flushed = self.collection().len();
                assert!(
                    flushed >= output_index + count,
                    "VerifyOutputSlice: expected at least {} flushed records, got {flushed}",
                    output_index + count,
                );
                assert_eq!(
                    self.received(*output_index..output_index + count),
                    self.published(*nats_seq..nats_seq + count),
                    "Record mismatch: output[{output_index}..] vs published[{nats_seq}..] (seq {nats_seq})",
                );
            }

            NatsMockAction::AssertRecordCount(n) => {
                let actual = self.collection().len();
                assert_eq!(
                    actual, *n,
                    "AssertRecordCount: expected exactly {n} records, got {actual}"
                );
            }
        }
        Ok(())
    }
}

fn run_nats_mock_test(
    engine: &dyn ConnectorTestEngine,
    pipeline_config: impl Fn(&str) -> serde_json::Value,
    actions: &[NatsMockAction],
) -> AnyResult<()> {
    init_test_logger();

    let mut runner = NatsMockRunner::new(engine)?;
    for (i, action) in actions.iter().enumerate() {
        println!("--- action {i}: {action:?} ---");
        runner.exec(action, &pipeline_config)?;
    }
    Ok(())
}

fn basic_nats_config(nats_url: &str) -> serde_json::Value {
    json!({
        "stream": "test_input",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": nats_url,
                },
                "stream_name": STREAM_NAME,
                "consumer_config": {
                    "deliver_policy": "All",
                    "filter_subjects": [SUBJECT_NAME],
                },
            },
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
            },
        },
    })
}

/// Inactivity timeout used by [`nats_stall_config`] (seconds).
const STALL_INACTIVITY_TIMEOUT_SECS: u64 = 2;

/// Config helper: NATS config with inactivity timeout and request_timeout_secs=2.
fn nats_stall_config(nats_url: &str) -> serde_json::Value {
    nats_stall_config_with_timeout(nats_url, STALL_INACTIVITY_TIMEOUT_SECS, 2)
}

fn nats_stall_config_with_timeout(
    nats_url: &str,
    inactivity_timeout_secs: u64,
    request_timeout_secs: u64,
) -> serde_json::Value {
    json!({
        "stream": "test_input",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": nats_url,
                    "request_timeout_secs": request_timeout_secs,
                },
                "stream_name": STREAM_NAME,
                "inactivity_timeout_secs": inactivity_timeout_secs,
                "consumer_config": {
                    "deliver_policy": "All",
                    "filter_subjects": [SUBJECT_NAME],
                },
            },
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
            },
        },
    })
}

/// Stall detection budget: inactivity_timeout + request_timeout(2) + slack(8).
fn stall_timeout() -> Duration {
    Duration::from_secs(STALL_INACTIVITY_TIMEOUT_SECS + 10)
}

// ---------------------------------------------------------------------------
// Basic Ingestion and Lifecycle
// ---------------------------------------------------------------------------

/// Records published before the endpoint starts arrive in order.
pub fn basic_input_consumption(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(2),
            CreatePipeline,
            Extend,
            WaitForRecords(2),
            VerifyRecords {
                output_index: 0,
                count: 2,
            },
            AssertRecordCount(2),
            Disconnect,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Retry and Recovery
// ---------------------------------------------------------------------------

/// Retry loop should report non-fatal errors every retry interval while
/// NATS is down and continue attempting reconnects.
pub fn retry_loop_when_server_unavailable(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertRecordCount(1),
            KillServer,
            WaitForErrorCountAtLeast {
                count: 2,
                timeout: Duration::from_secs(12),
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Connector should enter retrying ERROR state, then recover automatically
/// once the server and stream become available again.
pub fn resume_after_server_becomes_available(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertRecordCount(1),
            KillServer,
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(8),
            },
            RestartNatsSamePort,
            CreateStream,
            // Resume cursor is 2, so after restart publish enough messages to
            // include sequences 2.. so at least two new records are consumed.
            Publish(5),
            WaitForRecordsNoFatal(5),
            AssertRecordCount(5),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// PAUSE should stop the retry loop while in ERROR state; RESUME should restart
/// retries and eventually ingest when availability is restored.
pub fn pause_during_retry_loop(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            KillServer,
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(8),
            },
            Pause,
            AssertNoErrorCountIncrease {
                duration: Duration::from_secs(6),
            },
            Extend,
            WaitForErrorCountAtLeast {
                count: 2,
                timeout: Duration::from_secs(8),
            },
            RestartNatsSamePort,
            CreateStream,
            // Resume cursor is 2 after first record; publish enough records so
            // sequences >=2 exist in the new stream.
            Publish(4),
            WaitForRecordsNoFatal(4),
            AssertRecordCount(4),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Tests that pausing while the reader is Running and may have queued an error
/// (e.g. server just died) does not cause the next Extend to immediately enter
/// ErrorRetrying due to a stale error left in the channel.
///
/// Sequence: consume 1 record → kill server → pause immediately (reader error
/// may already be in-flight) → restart server + recreate stream → extend →
/// new records should arrive without spurious error-retry transitions.
pub fn no_stale_error_after_pause_extend(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertRecordCount(1),
            // Kill server: the reader task will notice and send an error,
            // but we pause immediately so the worker may not have processed it yet.
            KillServer,
            Pause,
            // Restart server and recreate stream before extending.
            RestartNatsSamePort,
            CreateStream,
            // Resume cursor is 2 after the first record; publish records with
            // sequences >= 2.
            Publish(3),
            Extend,
            WaitForRecordsNoFatal(3),
            AssertRecordCount(3),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Tests that when the stream is healthy but quiet (no messages arriving),
/// the inactivity timeout fires, the health check succeeds (consumer recreation
/// works), and the connector does NOT produce a fatal error. After the quiet
/// period, new messages published to the stream are still received correctly.
pub fn quiet_but_healthy_no_false_alarm(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            // Wait long enough for the inactivity timeout to fire at least twice
            // (2 * timeout + slack). The health check should succeed each time.
            Sleep(Duration::from_secs(2 * 2 + 3)),
            // Publish new messages after the quiet period and confirm they arrive.
            Publish(2),
            WaitForRecords(3),
            VerifyRecords {
                output_index: 1,
                count: 2,
            },
            AssertRecordCount(3),
            Disconnect,
        ],
    )
    .unwrap();
}

/// Tests that a short server outage does not cause a false fatal error if NATS
/// comes back on the same address before inactivity timeout expires.
pub fn mid_run_server_restart_recovers_no_fatal(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            KillServer,
            RestartNatsSamePort,
            CreateStream,
            Publish(5),
            // At least one post-restart record should be consumed and no fatal error.
            WaitForRecordsNoFatal(2),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Tests that inactivity timeout still drives first transport error emission.
pub fn inactivity_timeout_config_is_honored(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| nats_stall_config_with_timeout(nats_url, 1, 2),
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            KillServer,
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(6),
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Test that startup no longer fails fast: unavailable server should trigger
/// retrying non-fatal errors after `Extend`.
pub fn connection_refused_enters_retry_loop(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    let nonexistent_url = "nats://127.0.0.1:59999";
    run_nats_mock_test(
        engine,
        |_: &str| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": nonexistent_url,
                            "request_timeout_secs": 1,
                        },
                        "stream_name": STREAM_NAME,
                        "inactivity_timeout_secs": 1,
                        "consumer_config": {
                            "deliver_policy": "All",
                            "subjects": [SUBJECT_NAME],
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            Extend,
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(8),
            },
            Pause,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Startup retry loop should continue emitting non-fatal transport errors at
/// retry cadence while the server remains unreachable.
pub fn startup_connection_refused_retries_repeatedly(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    let nonexistent_url = "nats://127.0.0.1:59999";
    run_nats_mock_test(
        engine,
        |_: &str| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": nonexistent_url,
                            "request_timeout_secs": 1,
                        },
                        "stream_name": STREAM_NAME,
                        "inactivity_timeout_secs": 1,
                        "retry_interval_secs": 1,
                        "consumer_config": {
                            "deliver_policy": "All",
                            "subjects": [SUBJECT_NAME],
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            Extend,
            WaitForErrorCountAtLeast {
                count: 2,
                timeout: Duration::from_secs(8),
            },
            Pause,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Test that missing stream enters retry loop instead of failing pipeline open.
pub fn stream_not_found_enters_retry_loop(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": nats_url,
                            "request_timeout_secs": 1,
                        },
                        "stream_name": "this_stream_does_not_exist",
                        "inactivity_timeout_secs": 1,
                        "consumer_config": {
                            "deliver_policy": "All",
                            "subjects": [SUBJECT_NAME],
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            Extend,
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(8),
            },
            Pause,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Test that connection timeout contributes to retry cadence instead of blocking open.
pub fn connection_timeout(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    // Use a non-routable IP address that will cause a connection timeout
    // 10.255.255.1 is a reserved address that should not respond
    let non_routable_url = "nats://10.255.255.1:4222";
    run_nats_mock_test(
        engine,
        |_: &str| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": non_routable_url,
                            "connection_timeout_secs": 1,
                            "request_timeout_secs": 1,
                        },
                        "stream_name": "some_stream",
                        "inactivity_timeout_secs": 1,
                        "consumer_config": {
                            "deliver_policy": "All",
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            Extend,
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(10),
            },
            Pause,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Test that pausing while a startup retry attempt is in-flight interrupts the
/// attempt instead of waiting for connect/request timeouts to elapse.
pub fn pause_interrupts_inflight_retry_attempt(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    // Use a non-routable IP address that causes a long connect timeout.
    let non_routable_url = "nats://10.255.255.1:4222";
    run_nats_mock_test(
        engine,
        |_: &str| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": non_routable_url,
                            "connection_timeout_secs": 8,
                            "request_timeout_secs": 8,
                        },
                        "stream_name": "some_stream",
                        "inactivity_timeout_secs": 1,
                        "retry_interval_secs": 1,
                        "consumer_config": {
                            "deliver_policy": "All",
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            Extend,
            Sleep(Duration::from_millis(150)),
            Pause,
            AssertNoErrorCountIncrease {
                duration: Duration::from_secs(3),
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Test that retry_interval_secs controls retry cadence in ERROR state.
pub fn retry_interval_config_is_honored(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": nats_url,
                            "request_timeout_secs": 1,
                        },
                        "stream_name": "missing_stream",
                        "inactivity_timeout_secs": 1,
                        "retry_interval_secs": 1,
                        "consumer_config": {
                            "deliver_policy": "All",
                            "subjects": [SUBJECT_NAME],
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            Extend,
            WaitForErrorCountAtLeast {
                count: 3,
                timeout: Duration::from_secs(4),
            },
            Pause,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Test that retry_interval_secs=0 is rejected early by configuration validation.
pub fn retry_interval_zero_rejected(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |_nats_url| {
            json!({
                "stream": "test_input",
                "transport": {
                    "name": "nats_input",
                    "config": {
                        "connection_config": {
                            "server_url": "nats://127.0.0.1:4222",
                        },
                        "stream_name": "some_stream",
                        "inactivity_timeout_secs": 1,
                        "retry_interval_secs": 0,
                        "consumer_config": {
                            "deliver_policy": "All",
                        },
                    },
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                    },
                },
            })
        },
        &[
            StartNats,
            CreatePipeline,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(1),
                needle: "`retry_interval`",
            },
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Replay Happy Paths
// ---------------------------------------------------------------------------

/// Replay all published records from a fresh pipeline and verify correctness.
pub fn replay_basic(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            CreatePipeline,
            Extend,
            WaitForRecords(5),
            VerifyRecords {
                output_index: 0,
                count: 5,
            },
            AssertRecordCount(5),
            Disconnect,
            // Replay all 5 records: sequences [1, 6).
            CreatePipeline,
            Replay { start: 1, end: 6 },
            WaitForReplayedRecords(5),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(5),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 1,
                count: 5,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay a subset of published records (partial range).
pub fn replay_partial_range(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(10),
            // First pipeline: consume all records.
            CreatePipeline,
            Extend,
            WaitForRecords(10),
            AssertRecordCount(10),
            Disconnect,
            // Second pipeline: replay sequences [3, 8), i.e. sequences 3, 4, 5, 6, 7.
            CreatePipeline,
            Replay { start: 3, end: 8 },
            WaitForReplayedRecords(5),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(5),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 3,
                count: 5,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay records then extend to consume new records published after the replay range.
pub fn replay_then_extend(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            CreatePipeline,
            Extend,
            WaitForRecords(5),
            AssertRecordCount(5),
            Disconnect,
            Publish(10),
            CreatePipeline,
            Replay { start: 1, end: 6 },
            WaitForReplayedRecords(5),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(5),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 1,
                count: 5,
            },
            // Now extend to consume the 5 new records (sequences [6, 11)).
            Extend,
            WaitForRecords(15),
            AssertRecordCount(15),
            VerifyOutputSlice {
                output_index: 5,
                nats_seq: 6,
                count: 10,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay with an empty range should succeed immediately without errors.
pub fn replay_empty_range(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(3),
            CreatePipeline,
            // Empty range: start == end (e.g., nothing to replay).
            Replay { start: 1, end: 1 },
            // Empty replay produces no records; give the command time to be processed.
            Sleep(Duration::from_millis(100)),
            // Verify the empty replay produced exactly zero records.
            AssertRecordCount(0),
            // Extend to consume records normally.
            Extend,
            WaitForRecords(3),
            VerifyRecords {
                output_index: 0,
                count: 3,
            },
            AssertRecordCount(3),
            Disconnect,
        ],
    )
    .unwrap();
}

/// Multiple sequential replays before extending.
pub fn replay_multiple_ranges(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(10),
            CreatePipeline,
            Extend,
            WaitForRecords(10),
            AssertRecordCount(10),
            Disconnect,
            CreatePipeline,
            Replay { start: 1, end: 4 },
            WaitForReplayedRecords(3),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(3),
            VerifyOutputSlice {
                output_index: 0,
                nats_seq: 1,
                count: 3,
            },
            Replay { start: 4, end: 8 },
            WaitForReplayedRecords(7),
            Sleep(Duration::from_millis(200)),
            AssertRecordCount(7),
            VerifyOutputSlice {
                output_index: 3,
                nats_seq: 4,
                count: 4,
            },
            Extend,
            WaitForRecords(10),
            AssertRecordCount(10),
            VerifyOutputSlice {
                output_index: 7,
                nats_seq: 8,
                count: 3,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Replay Failure Modes
// ---------------------------------------------------------------------------

/// Replay should treat transient stream unavailability as retryable (non-fatal)
/// and keep retrying until the stream is recreated with replayable data.
pub fn replay_stream_deleted_retries_and_recovers(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5_000),
            CreatePipeline,
            // Delete before starting the replay: fast machines finish a
            // started replay before a disruption can land, so disrupting
            // mid-replay is inherently racy.
            DeleteStream,
            Replay {
                start: 1,
                end: 5_001,
            },
            // Let the first replay attempt fail before pumping the command
            // queue: a pending Queue command makes the reader abandon the
            // replay retry loop instead of retrying.
            Sleep(Duration::from_secs(3)),
            CreateStream,
            Publish(5_000),
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: stall_timeout(),
            },
            WaitForReplayedRecordsNoFatal(200),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Replay should keep retrying with non-fatal errors while the server is
/// down and complete once the server and stream come back.
pub fn replay_server_killed_retries_non_fatal(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5_000),
            CreatePipeline,
            // Kill before starting the replay: fast machines finish a
            // started replay before a disruption can land, so disrupting
            // mid-replay is inherently racy.
            KillServer,
            Replay {
                start: 1,
                end: 5_001,
            },
            // Let the first replay attempt fail before pumping the command
            // queue: a pending Queue command makes the reader abandon the
            // replay retry loop instead of retrying.
            Sleep(Duration::from_secs(3)),
            RestartNatsSamePort,
            CreateStream,
            Publish(5_000),
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: stall_timeout(),
            },
            WaitForReplayedRecordsNoFatal(200),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Replay fails with a fatal error when the stream has been purged and the
/// requested sequence numbers no longer exist.
pub fn replay_after_purge_errors(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            // First pipeline: consume all 5 records.
            CreatePipeline,
            Extend,
            WaitForRecords(5),
            AssertRecordCount(5),
            Disconnect,
            // Purge the stream: all messages deleted.
            PurgeStream,
            // Second pipeline: attempt to replay sequences [1, 6), which should fail.
            CreatePipeline,
            Replay { start: 1, end: 6 },
            ExpectFatalErrorContains {
                timeout: stall_timeout(),
                needle: "Replay requested sequences",
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay fails fast when the requested end sequence is above the stream tail.
pub fn replay_end_after_last_sequence_fails_fast(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            CreatePipeline,
            // Tail is 5, but end-1 is 99 -> validation must fail immediately.
            Replay { start: 1, end: 100 },
            ExpectFatalErrorWithin {
                min: Duration::from_millis(0),
                max: Duration::from_secs(2),
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay fails fast when the requested start sequence is older than the
/// stream head (messages have been purged and replaced with newer ones).
pub fn replay_start_before_first_sequence_fails_fast(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5),
            // Consume once to establish realistic sequence progression.
            CreatePipeline,
            Extend,
            WaitForRecords(5),
            Disconnect,
            // Remove old messages and publish new ones at higher sequences.
            PurgeStream,
            Publish(3),
            // Replay old sequence range should fail fast.
            CreatePipeline,
            Replay { start: 1, end: 2 },
            ExpectFatalErrorWithin {
                min: Duration::from_millis(0),
                max: Duration::from_secs(2),
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Replay fails fast when requesting a non-empty range from an empty stream.
pub fn replay_empty_stream_fails_fast(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            CreatePipeline,
            Replay { start: 1, end: 2 },
            ExpectFatalErrorWithin {
                min: Duration::from_millis(0),
                max: Duration::from_secs(2),
            },
            Disconnect,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Disconnect Behavior
// ---------------------------------------------------------------------------

/// Verifies that after calling `Disconnect`, records published to the stream
/// are NOT delivered to the pipeline.
pub fn disconnect_stops_delivery(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            // Publish and consume 3 records.
            Publish(3),
            CreatePipeline,
            Extend,
            WaitForRecords(3),
            VerifyRecords {
                output_index: 0,
                count: 3,
            },
            // Disconnect the endpoint.
            Disconnect,
            // Publish 5 more records (indices 3..8) while disconnected.
            Publish(5),
            // Give the endpoint time to (incorrectly) receive them.
            Sleep(Duration::from_millis(500)),
            // Assert that no new records arrived: still exactly 3.
            AssertRecordCount(3),
        ],
    )
    .unwrap();
}
