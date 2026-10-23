//! The NATS input connector on its own: reading a JetStream stream, retrying while the
//! server or the stream is unavailable, replaying ranges of sequence numbers, and stopping on
//! disconnect.
//!
//! Each case is a sequence of `NatsMockAction`s that `run_nats_mock_test` executes against a
//! private `nats-server`.

use crate::test::engine_test;
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

use crate::test::nats::jetstream_proxy::{JetStreamProxy, ProxyFaults};
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
    Replay {
        start: u64,
        end: u64,
    },
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
    ExpectFatalErrorWithin {
        min: Duration,
        max: Duration,
    },
    /// Assert that a fatal error appears within `timeout` and includes `needle`.
    ExpectFatalErrorContains {
        timeout: Duration,
        needle: &'static str,
    },
    /// Wait until `error_count` reaches at least `count`.
    WaitForErrorCountAtLeast {
        count: usize,
        timeout: Duration,
    },
    /// Assert that no additional endpoint errors are reported for `duration`.
    AssertNoErrorCountIncrease {
        duration: Duration,
    },

    /// Verify that `count` output records starting at `output_index` match
    /// the corresponding published records. The NATS sequence is derived
    /// automatically (`output_index + 1`), since `published` is 1-based.
    VerifyRecords {
        output_index: usize,
        count: usize,
    },
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

    /// Wait until the server reports exactly `count` consumers on the stream.
    ///
    /// Verifies that the connector deletes the consumers it abandons instead
    /// of leaving them for the server's `inactive_threshold` to reap.
    WaitForConsumerCount {
        count: usize,
        timeout: Duration,
    },
    /// Assert that every consumer on the stream is named `<prefix>_<suffix>`.
    AssertConsumerNamesStartWith(&'static str),

    /// Put a fault-injecting proxy in front of the server. Pipelines created
    /// afterwards connect through it; the runner's own requests do not.
    StartProxy(ProxyFaults),
    /// Gate replay's stream validation while direct-server publication runs.
    HoldStreamInfo,
    ReleaseStreamInfo,
    /// Observe retry errors without sending Queue (which interrupts replay).
    WaitForReplayErrorCountAtLeast {
        count: usize,
        timeout: Duration,
    },
    /// Suspend the server (SIGSTOP), so requests time out.
    FreezeServer,
    /// Resume a frozen server (SIGCONT).
    ThawServer,
    /// Delete every consumer on the stream through the JetStream API.
    DeleteConsumers,
    /// Call `endpoint.queue()` repeatedly for the given duration.
    QueueFor(Duration),
    /// Call `endpoint.queue()` once and assert that the endpoint reports the
    /// step (`extended`) within the given duration.
    QueueCompletesWithin(Duration),
    /// Call `endpoint.disconnect()` and assert that the worker exits within
    /// the given duration.
    DisconnectWithin(Duration),
    /// Wait until a delete reaches the proxy's gate.
    WaitForDeleteRequests {
        count: usize,
        timeout: Duration,
    },
    /// Disconnect with cleanup held at the proxy. The worker must stay alive
    /// until the test releases the delete, then exit with no consumer left.
    DisconnectWaitsForHeldDelete,
    /// Dropping the command sender must still join earlier cleanup.
    DropWaitsForHeldDelete,
    /// Assert that the consumer counters agree with the server: the
    /// connector cannot have deleted more consumers than it created, and
    /// `created - deleted` equals the consumers left on the stream.
    AssertConsumerCountersMatchServer,
    /// Assert that the server has exactly `n` consumers on the stream now,
    /// without waiting.
    AssertServerConsumerCount(usize),
    /// Make the proxy drop the replies to the next `n` stream info
    /// requests, so they time out.
    DropStreamInfoReplies(usize),
    /// Assert that the endpoint has not reported a fatal error.
    AssertNoFatalError,
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
    proxy: Option<JetStreamProxy>,
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
            proxy: None,
            rt: tokio::runtime::Runtime::new()?,
        })
    }

    fn nats_url(&self) -> &str {
        self.nats_url
            .as_deref()
            .expect("StartNats must be called before this action")
    }

    /// The URL pipelines connect to: the proxy when one is running.
    fn pipeline_url(&self) -> &str {
        self.proxy
            .as_ref()
            .map(|proxy| proxy.url())
            .unwrap_or_else(|| self.nats_url())
    }

    fn nats_guard(&self) -> &util::ProcessKillGuard {
        self.nats_guard
            .as_ref()
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
                let config = pipeline_config(self.pipeline_url());
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
                let got_fatal = self.got_fatal.clone();
                // Wait for the fatal error specifically: retry-mode tests
                // report retryable errors before the fatal one.
                wait(
                    || {
                        endpoint.queue(false);
                        got_fatal.load(Ordering::Acquire)
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

            NatsMockAction::WaitForConsumerCount { count, timeout } => {
                // The connector's own `created - deleted` counter is updated
                // the instant its create/delete call returns, in process,
                // with no network round trip. NATS acks a create or delete
                // only after applying it server-side (confirmed with
                // Synadia), so this counter is never ahead of the server's
                // real state. Polling it instead of the server itself turns
                // a test that raced a live network round trip under
                // whatever load the CI host was under into one bounded only
                // by how long our own code takes, which is normally
                // microseconds. `timeout` is a backstop against a genuinely
                // stuck connector, not a budget this loop is expected to
                // spend.
                //
                // A fully event-driven wait (e.g. a `Notify` the connector
                // signals on every create/delete) would remove the poll
                // loop entirely; tracked as a possible follow-up. The other
                // alternative, `tokio::time::pause`, doesn't apply here:
                // that would make our own `sleep`/`sleep_until` calls
                // deterministic, but this wait is bounded by the
                // connector's real work finishing, not by one of its
                // timers.
                let mock_input_consumer = self.mock_input_consumer().clone();
                let net_consumers = |consumer: &MockInputConsumer| -> Option<i64> {
                    let created =
                        consumer.custom_metric("input_connector_nats_consumers_created_total")?;
                    let deleted =
                        consumer.custom_metric("input_connector_nats_consumers_deleted_total")?;
                    Some((created - deleted).round() as i64)
                };
                let deadline = Instant::now() + *timeout;
                loop {
                    if net_consumers(&mock_input_consumer) == Some(*count as i64) {
                        break;
                    }
                    if Instant::now() >= deadline {
                        anyhow::bail!(
                            "Timed out after {timeout:?} waiting for the connector's own count to \
                             reach {count} consumers on stream '{STREAM_NAME}', last seen: {:?}",
                            net_consumers(&mock_input_consumer)
                        );
                    }
                    sleep(Duration::from_millis(1));
                }

                // One real check, not a retry loop: the in-process count
                // above is already known to match the server (see above),
                // so this exists only to catch a bug in that reasoning, not
                // to wait out a race.
                let nats_url = self.nats_url().to_string();
                let consumers = self
                    .rt
                    .block_on(util::list_consumers(&nats_url, STREAM_NAME))?;
                anyhow::ensure!(
                    consumers.len() == *count,
                    "Connector's own count reached {count}, but the server disagrees: found {} \
                     consumers on stream '{STREAM_NAME}': {:?}",
                    consumers.len(),
                    consumers
                        .iter()
                        .map(|info| info.name.as_str())
                        .collect::<Vec<_>>()
                );
            }

            NatsMockAction::AssertConsumerNamesStartWith(prefix) => {
                let nats_url = self.nats_url().to_string();
                let consumers = self
                    .rt
                    .block_on(util::list_consumers(&nats_url, STREAM_NAME))?;
                assert!(
                    !consumers.is_empty(),
                    "AssertConsumerNamesStartWith: no consumers on stream '{STREAM_NAME}'"
                );
                let expected = format!("{prefix}_");
                for info in &consumers {
                    assert!(
                        info.name.starts_with(&expected),
                        "Consumer '{}' is not named '{expected}<suffix>'",
                        info.name
                    );
                }
            }

            NatsMockAction::StartProxy(faults) => {
                let proxy = JetStreamProxy::start(&self.rt, self.nats_url(), faults.clone())?;
                self.proxy = Some(proxy);
            }

            NatsMockAction::HoldStreamInfo => {
                self.proxy
                    .as_ref()
                    .expect("StartProxy must be called first")
                    .hold_stream_info();
            }
            NatsMockAction::ReleaseStreamInfo => {
                self.proxy
                    .as_ref()
                    .expect("StartProxy must be called first")
                    .release_stream_info();
            }
            NatsMockAction::WaitForReplayErrorCountAtLeast { count, timeout } => {
                wait(
                    || self.error_count.load(Ordering::Acquire) >= *count,
                    timeout.as_millis(),
                )
                .map_err(|()| anyhow::anyhow!("no replay retry error within {timeout:?}"))?;
            }

            NatsMockAction::FreezeServer => self.nats_guard().freeze()?,

            NatsMockAction::ThawServer => self.nats_guard().thaw()?,

            NatsMockAction::DeleteConsumers => {
                let nats_url = self.nats_url().to_string();
                let deleted = self
                    .rt
                    .block_on(util::delete_consumers(&nats_url, STREAM_NAME))?;
                println!("Deleted {deleted} consumers");
            }

            NatsMockAction::QueueFor(duration) => {
                let deadline = Instant::now() + *duration;
                while Instant::now() < deadline {
                    self.endpoint().queue(false);
                    sleep(Duration::from_millis(50));
                }
            }

            NatsMockAction::QueueCompletesWithin(duration) => {
                let before = self.mock_input_consumer().state().n_extended;
                let start = Instant::now();
                self.endpoint().queue(false);
                let completed = wait(
                    || self.mock_input_consumer().state().n_extended > before,
                    default_timeout_ms(),
                );
                let elapsed = start.elapsed();
                assert!(completed.is_ok(), "queue() never completed");
                assert!(
                    elapsed <= *duration,
                    "queue() took {elapsed:?}, more than {duration:?}"
                );
            }

            NatsMockAction::DisconnectWithin(duration) => {
                let endpoint = self.endpoint();
                endpoint.disconnect();
                assert!(
                    wait(|| endpoint.is_closed(), duration.as_millis()).is_ok(),
                    "disconnect took more than {duration:?}"
                );
            }

            NatsMockAction::DropWaitsForHeldDelete => {
                let endpoint = self
                    .endpoint
                    .take()
                    .expect("CreatePipeline must be called first");
                let completion = endpoint
                    .take_completion()
                    .expect("reader has no completion future");
                let proxy = self
                    .proxy
                    .as_ref()
                    .expect("StartProxy must be called first");
                let task = self.rt.spawn(completion);
                drop(endpoint);
                let exited_while_held = wait(|| task.is_finished(), 200).is_ok();
                proxy.release_deletes();
                let joined = self
                    .rt
                    .block_on(async { tokio::time::timeout(Duration::from_secs(10), task).await });
                joined
                    .expect("worker did not exit after releasing delete")
                    .unwrap();
                assert!(
                    !exited_while_held,
                    "dropping the reader skipped pending cleanup"
                );
                assert!(
                    self.rt
                        .block_on(util::list_consumers(self.nats_url(), STREAM_NAME))?
                        .is_empty()
                );
            }

            NatsMockAction::WaitForDeleteRequests { count, timeout } => {
                self.proxy
                    .as_ref()
                    .expect("StartProxy must be called before this action")
                    .wait_for_delete_requests(*count, *timeout)?;
            }

            NatsMockAction::DisconnectWaitsForHeldDelete => {
                let proxy = self
                    .proxy
                    .as_ref()
                    .expect("StartProxy must be called first");
                self.endpoint().disconnect();
                let exited_while_held = wait(
                    || self.endpoint().is_closed(),
                    Duration::from_millis(200).as_millis(),
                )
                .is_ok();
                // Release before any assertion, even on the buggy connector,
                // so failure does not leave cleanup blocked in the proxy.
                proxy.release_deletes();
                assert!(
                    wait(|| self.endpoint().is_closed(), 10_000).is_ok(),
                    "worker did not exit after releasing the consumer delete"
                );
                assert!(
                    !exited_while_held,
                    "worker exited while the consumer delete from Pause was still held"
                );
                let consumers = self
                    .rt
                    .block_on(util::list_consumers(self.nats_url(), STREAM_NAME))?;
                assert!(
                    consumers.is_empty(),
                    "consumer still exists after worker exit"
                );
            }

            NatsMockAction::AssertServerConsumerCount(n) => {
                let nats_url = self.nats_url().to_string();
                let consumers = self
                    .rt
                    .block_on(util::list_consumers(&nats_url, STREAM_NAME))?;
                assert_eq!(
                    consumers.len(),
                    *n,
                    "expected {n} consumers on stream '{STREAM_NAME}', found {:?}",
                    consumers
                        .iter()
                        .map(|info| info.name.as_str())
                        .collect::<Vec<_>>()
                );
            }

            NatsMockAction::DropStreamInfoReplies(n) => {
                self.proxy
                    .as_ref()
                    .expect("StartProxy must be called before this action")
                    .drop_stream_info_replies(*n);
            }

            NatsMockAction::AssertNoFatalError => {
                assert!(
                    !self.got_fatal.load(Ordering::Acquire),
                    "unexpected fatal endpoint error: {:?}",
                    self.mock_input_consumer().state().endpoint_error
                );
            }

            NatsMockAction::AssertConsumerCountersMatchServer => {
                let consumer = self.mock_input_consumer();
                let created = consumer
                    .custom_metric("input_connector_nats_consumers_created_total")
                    .expect("connector should register its metrics");
                let deleted = consumer
                    .custom_metric("input_connector_nats_consumers_deleted_total")
                    .expect("connector should register its metrics");
                let nats_url = self.nats_url().to_string();
                let live = self
                    .rt
                    .block_on(util::list_consumers(&nats_url, STREAM_NAME))?
                    .len() as f64;
                assert!(
                    deleted <= created,
                    "consumers_deleted_total ({deleted}) exceeds consumers_created_total ({created})"
                );
                assert_eq!(
                    created - deleted,
                    live,
                    "created ({created}) - deleted ({deleted}) should equal the {live} consumers on the server"
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
            // Stall detection and the health check take a few seconds, then
            // the first retry waits the default 5s plus up to 25% jitter.
            WaitForErrorCountAtLeast {
                count: 2,
                timeout: Duration::from_secs(15),
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Connector should enter retrying ERROR state, then recover automatically
/// once the server and stream become available again.
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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

/// Test that retry_interval controls retry cadence in ERROR state.
///
/// Pinning `retry_max_interval` to `retry_interval` disables the
/// exponential growth, so retries run at a fixed (jittered) interval.
#[engine_test(services(Nats))]
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
                        "retry_interval": "1s",
                        "retry_max_interval": "1s",
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
#[engine_test(services(Nats))]
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

/// Config for a connector that retries against a stream that does not exist,
/// so every attempt fails fast and deterministically.
fn missing_stream_retry_config(
    nats_url: &str,
    retry_interval_secs: u64,
    retry_max_interval_secs: u64,
    retry_max_attempts: Option<u32>,
) -> serde_json::Value {
    let mut config = json!({
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
                "retry_interval": format!("{retry_interval_secs}s"),
                "retry_max_interval": format!("{retry_max_interval_secs}s"),
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
    });
    if let Some(n) = retry_max_attempts {
        config["transport"]["config"]["retry_max_attempts"] = json!(n);
    }
    config
}

/// The delay between retries doubles on each consecutive failure.
///
/// With `retry_interval: 1s`, the third error (initial failure plus two
/// failed retries) arrives within about three seconds. The next retry is
/// then at least `nominal(3) / 2 = 2s` away, so no error may appear in the
/// following 1.5s. A fixed 1s interval would fail this check.
#[engine_test(services(Nats))]
pub fn retry_backs_off_exponentially(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| missing_stream_retry_config(nats_url, 1, 60, None),
        &[
            StartNats,
            CreatePipeline,
            Extend,
            WaitForErrorCountAtLeast {
                count: 3,
                timeout: Duration::from_secs(6),
            },
            AssertNoErrorCountIncrease {
                duration: Duration::from_millis(1500),
            },
            Pause,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// With `retry_max_attempts` set, the connector gives up with a fatal error
/// after that many consecutive failed retries instead of retrying forever.
#[engine_test(services(Nats))]
pub fn retry_max_attempts_ends_in_fatal_error(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| missing_stream_retry_config(nats_url, 1, 1, Some(2)),
        &[
            StartNats,
            CreatePipeline,
            Extend,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(10),
                needle: "gave up after 2 consecutive failed attempts (retry_max_attempts=2)",
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// The circuit breaker applies to replay retries too.
#[engine_test(services(Nats))]
pub fn replay_retry_max_attempts_ends_in_fatal_error(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| missing_stream_retry_config(nats_url, 1, 1, Some(1)),
        &[
            StartNats,
            CreatePipeline,
            Replay { start: 1, end: 2 },
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(10),
                needle: "NATS replay gave up after",
            },
        ],
    )
    .unwrap();
}

/// `retry_max_interval` below `retry_interval` is rejected when the
/// connector is created.
#[engine_test(services(Nats))]
pub fn retry_max_interval_below_interval_rejected(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |_| missing_stream_retry_config("nats://127.0.0.1:4222", 10, 5, None),
        &[
            StartNats,
            CreatePipeline,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(1),
                needle: "`retry_max_interval` is 5s, but it must be at least `retry_interval` (10s)",
            },
        ],
    )
    .unwrap();
}

/// `retry_max_attempts: 0` is rejected when the connector is created.
#[engine_test(services(Nats))]
pub fn retry_max_attempts_zero_rejected(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |_| missing_stream_retry_config("nats://127.0.0.1:4222", 1, 1, Some(0)),
        &[
            StartNats,
            CreatePipeline,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(1),
                needle: "retry_max_attempts must be at least 1",
            },
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Consumer Lifecycle
// ---------------------------------------------------------------------------

/// Timeout for the server to reflect a consumer being created or deleted.
///
/// Well below the 30s `inactive_threshold` of the ordered consumer, so a
/// passing test proves explicit deletion rather than server-side expiry.
const CONSUMER_COUNT_TIMEOUT: Duration = Duration::from_secs(5);

/// Pausing and disconnecting delete the connector's consumer on the server.
#[engine_test(services(Nats))]
pub fn pause_and_disconnect_delete_consumer(engine: &dyn ConnectorTestEngine) {
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
            WaitForConsumerCount {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Extend,
            Publish(1),
            WaitForRecords(3),
            WaitForConsumerCount {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Disconnect,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
        ],
    )
    .unwrap();
}

/// A replay consumer is deleted as soon as the replay completes.
#[engine_test(services(Nats))]
pub fn replay_deletes_consumer(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(3),
            CreatePipeline,
            Replay { start: 1, end: 4 },
            WaitForReplayedRecords(3),
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Disconnect,
        ],
    )
    .unwrap();
}

/// Without a configured name, consumers are named after the table.
#[engine_test(services(Nats))]
pub fn consumer_name_defaults_to_table_prefix(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertConsumerNamesStartWith("nats_input"),
            Disconnect,
        ],
    )
    .unwrap();
}

/// A configured consumer name is sanitized and used as the prefix.
#[engine_test(services(Nats))]
pub fn consumer_name_uses_sanitized_configured_prefix(engine: &dyn ConnectorTestEngine) {
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
                        },
                        "stream_name": STREAM_NAME,
                        "consumer_config": {
                            "name": "orders.v1 reader",
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
        },
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertConsumerNamesStartWith("orders_v1_reader"),
            Disconnect,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Replay Happy Paths
// ---------------------------------------------------------------------------

/// Replay all published records from a fresh pipeline and verify correctness.
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
pub fn replay_stream_deleted_retries_and_recovers(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5_000),
            StartProxy(Default::default()),
            CreatePipeline,
            // Delete before starting the replay: fast machines finish a
            // started replay before a disruption can land, so disrupting
            // mid-replay is inherently racy.
            DeleteStream,
            Replay {
                start: 1,
                end: 5_001,
            },
            WaitForReplayErrorCountAtLeast {
                count: 1,
                timeout: stall_timeout(),
            },
            // Replays validate the stream tail before reading. Do not expose
            // an incomplete replacement stream while publication is running.
            HoldStreamInfo,
            CreateStream,
            Publish(5_000),
            ReleaseStreamInfo,
            WaitForReplayedRecordsNoFatal(5_000),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Replay should keep retrying with non-fatal errors while the server is
/// down and complete once the server and stream come back.
#[engine_test(services(Nats))]
pub fn replay_server_killed_retries_non_fatal(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(5_000),
            StartProxy(Default::default()),
            CreatePipeline,
            // Kill before starting the replay: fast machines finish a
            // started replay before a disruption can land, so disrupting
            // mid-replay is inherently racy.
            KillServer,
            Replay {
                start: 1,
                end: 5_001,
            },
            WaitForReplayErrorCountAtLeast {
                count: 1,
                timeout: stall_timeout(),
            },
            HoldStreamInfo,
            RestartNatsSamePort,
            CreateStream,
            Publish(5_000),
            ReleaseStreamInfo,
            WaitForReplayedRecordsNoFatal(5_000),
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Replay fails with a fatal error when the stream has been purged and the
/// requested sequence numbers no longer exist.
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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
#[engine_test(services(Nats))]
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

/// Config with one-second timeouts and retries, so a test sees many retry
/// cycles quickly. The entries of `extra` are added to the connector
/// configuration.
fn fast_retry_config(nats_url: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut config = json!({
        "stream": "test_input",
        "transport": {
            "name": "nats_input",
            "config": {
                "connection_config": {
                    "server_url": nats_url,
                    "request_timeout_secs": 1,
                    "connection_timeout_secs": 1,
                },
                "stream_name": STREAM_NAME,
                "inactivity_timeout_secs": 1,
                "retry_interval": "1s",
                "retry_max_interval": "1s",
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
    });
    if let serde_json::Value::Object(extra) = extra {
        for (key, value) in extra {
            config["transport"]["config"][key] = value;
        }
    }
    config
}

// ---------------------------------------------------------------------------
// retry_max_attempts
// ---------------------------------------------------------------------------

/// `retry_max_attempts` must also stop a connector whose consumer creates
/// succeed but whose readers fail at once.
///
/// Every reconnect then creates a consumer and fails, which is the
/// consumer-creation storm the breaker exists to stop. A successful create
/// resets the failure count, and a reader failure sets it back to 1, so
/// the breaker never trips.
#[engine_test(services(Nats))]
pub fn retry_max_attempts_stops_create_then_fail_loop(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({"retry_max_attempts": 2})),
        &[
            StartNats,
            CreateStream,
            Publish(1),
            StartProxy(ProxyFaults {
                fail_pulls: true,
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(20),
                needle: "gave up after",
            },
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

/// Separate outages on a stream with no new messages must not add up to
/// `retry_max_attempts`.
///
/// The failure count resets only when a step delivers records. On a quiet
/// stream, each outage that the connector recovers from adds to the count,
/// so the connector gives up after `retry_max_attempts` outages spread over
/// any length of time, while NATS is healthy.
#[engine_test(services(Nats))]
pub fn retry_max_attempts_counts_one_outage_at_a_time(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({"retry_max_attempts": 1})),
        &[
            StartNats,
            CreateStream,
            Publish(1),
            StartProxy(ProxyFaults::default()),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            // One failed inactivity health check: the probe and its fallback.
            DropStreamInfoReplies(2),
            WaitForErrorCountAtLeast {
                count: 1,
                timeout: Duration::from_secs(20),
            },
            // Recover, then stay healthy for several retry intervals.
            Sleep(Duration::from_secs(5)),
            DropStreamInfoReplies(2),
            WaitForErrorCountAtLeast {
                count: 2,
                timeout: Duration::from_secs(20),
            },
            AssertNoFatalError,
            DisconnectAllowNonFatal,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Responsiveness
// ---------------------------------------------------------------------------

/// A disconnect must interrupt the wait between replay retries.
///
/// The replay loop checks for commands once, then sleeps for the whole retry
/// delay, which can now reach `retry_max_interval_secs` (300s by default).
#[engine_test(services(Nats))]
pub fn disconnect_interrupts_replay_retry_wait(engine: &dyn ConnectorTestEngine) {
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
                        "retry_interval": "30s",
                        "retry_max_interval": "30s",
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
        },
        &[
            StartNats,
            CreatePipeline,
            Replay { start: 1, end: 2 },
            // The first replay attempt fails at once on the missing stream;
            // let the loop pass its single command check and start sleeping.
            // (No `queue()` calls here: the replay loop would buffer them
            // ahead of the disconnect.)
            Sleep(Duration::from_secs(2)),
            DisconnectWithin(Duration::from_secs(5)),
        ],
    )
    .unwrap();
}

/// A disconnect must interrupt the wait between replay retries, also when
/// other commands arrived before it.
///
/// The wait buffers the first command it receives and then stops reading
/// commands. After the wait, that command is put back and read first by the
/// next wait, so a disconnect that arrives behind it is never read while the
/// replay keeps failing. The controller sends a command on every step, so
/// this is the usual order.
#[engine_test(services(Nats))]
pub fn disconnect_behind_queue_interrupts_replay_retry_wait(engine: &dyn ConnectorTestEngine) {
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
                        "retry_interval": "30s",
                        "retry_max_interval": "30s",
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
        },
        &[
            StartNats,
            CreatePipeline,
            Replay { start: 1, end: 2 },
            Sleep(Duration::from_secs(2)),
            QueueFor(Duration::from_millis(200)),
            DisconnectWithin(Duration::from_secs(5)),
        ],
    )
    .unwrap();
}

/// Pausing a connector must not delay its next step while the server is
/// slow to answer the consumer delete.
///
/// The delete runs inside the worker's command loop, so the `Queue` command
/// that follows a `Pause` waits for it (up to 5s). The controller waits for
/// every connector's step, so this stalls the whole pipeline.
#[engine_test(services(Nats))]
pub fn pause_does_not_stall_next_step(engine: &dyn ConnectorTestEngine) {
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
            FreezeServer,
            Pause,
            QueueCompletesWithin(Duration::from_secs(1)),
            ThawServer,
            Disconnect,
        ],
    )
    .unwrap();
}

/// When the connector stops, its consumer must be deleted before the
/// connector's worker exits.
///
/// The delete now runs in a background task. When a pipeline stops, its
/// process can exit as soon as the connectors have stopped, before that task
/// runs, which leaves the consumer on the server until `inactive_threshold`
/// removes it. The proxy makes the delete slow, so the task is still running
/// when the worker exits.
#[engine_test(services(Nats))]
pub fn disconnect_deletes_consumer_before_exit(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(2),
            StartProxy(ProxyFaults {
                delay_deletes: Some(Duration::from_secs(1)),
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            WaitForRecords(2),
            Disconnect,
            AssertServerConsumerCount(0),
        ],
    )
    .unwrap();
}

/// Disconnect must join cleanup already detached by Pause, not just cleanup
/// belonging to a reader still active at the time of Disconnect.
#[engine_test(services(Nats))]
pub fn pause_then_disconnect_waits_for_consumer_delete(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(2),
            StartProxy(ProxyFaults {
                hold_deletes: true,
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            WaitForRecords(2),
            Pause,
            WaitForDeleteRequests {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            AssertServerConsumerCount(1),
            DisconnectWaitsForHeldDelete,
        ],
    )
    .unwrap();
}

/// Channel closure is also a worker exit: it must not bypass Pause cleanup.
#[engine_test(services(Nats))]
pub fn pause_then_drop_waits_for_consumer_delete(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(2),
            StartProxy(ProxyFaults {
                hold_deletes: true,
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            WaitForRecords(2),
            Pause,
            WaitForDeleteRequests {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            DropWaitsForHeldDelete,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Exactly-once
// ---------------------------------------------------------------------------

/// Deleting the connector's consumer must not make it ingest the stream a
/// second time.
///
/// After a pause and resume, the connector reads from a consumer positioned
/// after the last message. If that consumer is deleted before it delivers
/// anything (for example by an operator cleaning up consumers by prefix),
/// the client library recreates it with `DeliverPolicy::All` and the
/// connector ingests every message again.
#[engine_test(services(Nats))]
pub fn deleted_positioned_consumer_does_not_reingest(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(3),
            CreatePipeline,
            Extend,
            WaitForRecords(3),
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Extend,
            WaitForConsumerCount {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            DeleteConsumers,
            QueueFor(Duration::from_secs(5)),
            AssertRecordCount(3),
            Disconnect,
        ],
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// Consumer accounting and cleanup
// ---------------------------------------------------------------------------

/// The consumer counters must agree with the server when a create times out
/// but the server created the consumer anyway.
///
/// The connector deletes that consumer and counts the delete, but never
/// counted the create, so `consumers_deleted_total` ends up larger than
/// `consumers_created_total`.
#[engine_test(services(Nats))]
pub fn consumer_counters_count_timed_out_creates(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({})),
        &[
            StartNats,
            CreateStream,
            Publish(2),
            StartProxy(ProxyFaults {
                drop_create_reply: Some(1),
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            WaitForRecordsNoFatal(2),
            DisconnectAllowNonFatal,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            AssertConsumerCountersMatchServer,
        ],
    )
    .unwrap();
}

/// A consumer whose create timed out must not outlive the connector, even
/// when the server applies the create after the cleanup delete.
///
/// The connector assumes the delete is "queued behind the create". That
/// holds on one server, but a JetStream meta-leader change between the two
/// requests can apply them in the other order. The proxy forces that order.
#[engine_test(services(Nats))]
pub fn timed_out_create_applied_after_delete_is_cleaned_up(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        |nats_url| fast_retry_config(nats_url, json!({})),
        &[
            StartNats,
            CreateStream,
            Publish(2),
            StartProxy(ProxyFaults {
                hold_create_until_delete: Some(1),
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            WaitForRecordsNoFatal(2),
            DisconnectAllowNonFatal,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
        ],
    )
    .unwrap();
}

/// A consumer that the client library is recreating when the connector
/// pauses must not outlive the pause.
///
/// The connector assumes that once the reader task is gone, "nothing can
/// race the delete". A recreate the library already sent can still be
/// applied after the delete; the proxy forces that order.
#[engine_test(services(Nats))]
pub fn inflight_library_recreate_is_cleaned_up_on_pause(engine: &dyn ConnectorTestEngine) {
    use NatsMockAction::*;
    run_nats_mock_test(
        engine,
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            StartProxy(ProxyFaults {
                consumer_deleted_on_first_pull: true,
                hold_create_until_delete: Some(2),
                ..Default::default()
            }),
            CreatePipeline,
            Extend,
            // The library's recreate (create number 2) is now held.
            Sleep(Duration::from_secs(1)),
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Disconnect,
        ],
    )
    .unwrap();
}
