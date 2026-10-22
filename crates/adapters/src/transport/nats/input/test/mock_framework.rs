use super::NatsTestRecord;
use super::jetstream_proxy::{JetStreamProxy, ProxyFaults};
use super::util;
use crate::test::{
    DEFAULT_TIMEOUT_MS, MockDeZSet, MockInputConsumer, init_test_logger, mock_input_pipeline, wait,
};
use crate::transport::InputReader;
use anyhow::{Error as AnyError, Result as AnyResult};
use feldera_types::program_schema::Relation;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

/// Atomic actions for declarative mock_input_pipeline tests.
///
/// Each test is expressed as a sequence of actions executed by
/// [`run_nats_mock_test`]. The runner maintains internal state (NATS process
/// guard, endpoint, mock input consumer, published records, etc.) and executes actions
/// in order.
#[derive(Clone, Debug)]
pub(super) enum NatsMockAction {
    /// Start a NATS server process. Stores guard + URL in runner state.
    StartNats,
    /// Create the JetStream stream (hardcoded stream="str", subject="sub").
    CreateStream,
    /// Call `mock_input_pipeline`, wire up the `on_error` callback.
    CreatePipeline,

    /// Publish `n` deterministic records using the runner's auto-increment counter.
    /// Record `i` is `NatsTestRecord { s: "msg{i}", b: i % 2 == 0, i: i }`.
    ///
    /// Records are appended to the runner's 1-based `published` vec so that
    /// `published[seq]` is the record with JetStream sequence number `seq`.
    Publish(usize),

    /// Call `endpoint.extend()` to start reading.
    Extend,
    /// Call `endpoint.pause()` to stop ingest/retry loop.
    Pause,
    /// Queue repeatedly until `n` total records have been flushed to the zset.
    /// Aborts early with an error if a fatal endpoint error is detected.
    WaitForRecords(usize),
    /// Like `WaitForRecords`, but tolerates non-fatal endpoint errors as long
    /// as a fatal error never occurs.
    WaitForRecordsNoFatal(usize),
    /// Issue a replay for the half-open JetStream sequence range `[start, end)`.
    ///
    /// JetStream sequences are **1-based** (first message = sequence 1), and
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

    /// Wait until `n` records have been flushed to the zset by replay.
    /// Unlike `WaitForRecords`, this does NOT call `endpoint.queue()` —
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
    /// `output_index` is a 0-based index into the flushed output zset.
    /// `nats_seq` is a 1-based JetStream sequence number that indexes
    /// directly into the runner's `published` vec.
    VerifyOutputSlice {
        output_index: usize,
        nats_seq: usize,
        count: usize,
    },
    /// Assert that the zset contains exactly `n` flushed records (no waiting).
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
struct NatsMockRunner {
    nats_guard: Option<util::ProcessKillGuard>,
    nats_url: Option<String>,
    /// Published records, **1-indexed** to match JetStream sequence numbers.
    /// `published[seq]` is the record with JetStream sequence number `seq`.
    /// Index 0 is a dummy sentinel and must never be accessed.
    published: Vec<NatsTestRecord>,
    endpoint: Option<Box<dyn InputReader>>,
    mock_input_consumer: Option<MockInputConsumer>,
    zset: Option<MockDeZSet<NatsTestRecord, NatsTestRecord>>,
    pipeline_create_error: Option<AnyError>,
    got_fatal: Arc<AtomicBool>,
    error_count: Arc<AtomicUsize>,
    proxy: Option<JetStreamProxy>,
    rt: tokio::runtime::Runtime,
}

pub(super) const STREAM_NAME: &str = "str";
pub(super) const SUBJECT_NAME: &str = "sub";

impl NatsMockRunner {
    fn new() -> AnyResult<Self> {
        Ok(Self {
            nats_guard: None,
            nats_url: None,
            // Sentinel at index 0 so that `published[seq]` works directly
            // with 1-based JetStream sequence numbers.
            published: vec![NatsTestRecord::new(String::new(), false, -1)],
            endpoint: None,
            mock_input_consumer: None,
            zset: None,
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
        wait(|| endpoint.is_closed(), DEFAULT_TIMEOUT_MS)
            .map_err(|()| anyhow::anyhow!("Timed out waiting for endpoint to disconnect"))?;
        Ok(())
    }

    fn zset(&self) -> &MockDeZSet<NatsTestRecord, NatsTestRecord> {
        if let Some(zset) = self.zset.as_ref() {
            zset
        } else if let Some(error) = self.pipeline_create_error.as_ref() {
            panic!("CreatePipeline failed: {error:#}");
        } else {
            panic!("CreatePipeline must be called before this action");
        }
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

                match mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
                    serde_json::from_value(config).unwrap(),
                    Relation::empty(),
                ) {
                    Ok((endpoint, mock_input_consumer, _parser, zset)) => {
                        // Reset error state so a fresh pipeline starts clean.
                        self.got_fatal.store(false, Ordering::Release);
                        self.error_count.store(0, Ordering::Release);
                        self.pipeline_create_error = None;

                        let got_fatal_clone = self.got_fatal.clone();
                        let error_count_clone = self.error_count.clone();
                        mock_input_consumer.on_error(Some(Box::new(move |fatal, _err| {
                            error_count_clone.fetch_add(1, Ordering::AcqRel);
                            if fatal {
                                got_fatal_clone.store(true, Ordering::Release);
                            }
                        })));

                        self.endpoint = Some(endpoint);
                        self.mock_input_consumer = Some(mock_input_consumer);
                        self.zset = Some(zset);
                    }
                    Err(error) => {
                        self.endpoint = None;
                        self.mock_input_consumer = None;
                        self.zset = None;
                        self.pipeline_create_error = Some(error);
                    }
                }
            }

            NatsMockAction::Publish(n) => {
                let start = self.published.len();
                let records: Vec<NatsTestRecord> = (start..start + n)
                    .map(|i| NatsTestRecord::new(format!("msg{i}"), i % 2 == 0, i as i64))
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
                let zset = self.zset();
                let mock_input_consumer = self.mock_input_consumer();
                wait(
                    || {
                        endpoint.queue(false);
                        if mock_input_consumer.state().endpoint_error.is_some() {
                            return true;
                        }
                        zset.state().flushed.len() >= n
                    },
                    DEFAULT_TIMEOUT_MS,
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} records (got {})",
                        zset.state().flushed.len()
                    )
                })?;
                assert!(
                    mock_input_consumer.state().endpoint_error.is_none(),
                    "Unexpected endpoint error while waiting for records: {:?}",
                    mock_input_consumer.state().endpoint_error
                );
                assert!(
                    zset.state().flushed.len() >= n,
                    "Expected at least {n} records, got {}",
                    zset.state().flushed.len()
                );
            }

            NatsMockAction::WaitForRecordsNoFatal(n) => {
                let n = *n;
                let endpoint = self.endpoint();
                let zset = self.zset();
                wait(
                    || {
                        endpoint.queue(false);
                        if self.got_fatal.load(Ordering::Acquire) {
                            return true;
                        }
                        zset.state().flushed.len() >= n
                    },
                    DEFAULT_TIMEOUT_MS,
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} records without fatal error (got {})",
                        zset.state().flushed.len()
                    )
                })?;
                assert!(
                    !self.got_fatal.load(Ordering::Acquire),
                    "Unexpected fatal endpoint error while waiting for records: {:?}",
                    self.mock_input_consumer().state().endpoint_error
                );
                assert!(
                    zset.state().flushed.len() >= n,
                    "Expected at least {n} records, got {}",
                    zset.state().flushed.len()
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
                let zset = self.zset();
                let mock_input_consumer = self.mock_input_consumer();
                wait(
                    || {
                        if mock_input_consumer.state().endpoint_error.is_some() {
                            return true;
                        }
                        zset.state().flushed.len() >= n
                    },
                    DEFAULT_TIMEOUT_MS,
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} replayed records (got {})",
                        zset.state().flushed.len()
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
                let zset = self.zset();
                wait(
                    || {
                        if self.got_fatal.load(Ordering::Acquire) {
                            return true;
                        }
                        zset.state().flushed.len() >= n
                    },
                    DEFAULT_TIMEOUT_MS,
                )
                .map_err(|()| {
                    anyhow::anyhow!(
                        "Timed out waiting for {n} replayed records without fatal error (got {})",
                        zset.state().flushed.len()
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
                let zset = self.zset();
                let flushed = &zset.state().flushed;
                assert!(
                    flushed.len() >= output_index + count,
                    "VerifyRecords: expected at least {} flushed records, got {}",
                    output_index + count,
                    flushed.len()
                );
                for i in 0..*count {
                    // flushed is 0-based, published is 1-based (index = sequence number).
                    let out_idx = output_index + i;
                    let seq = out_idx + 1;
                    assert_eq!(
                        flushed[out_idx].unwrap_insert(),
                        &self.published[seq],
                        "Record mismatch: output[{}] vs published[{}] (seq {})",
                        out_idx,
                        seq,
                        seq,
                    );
                }
            }

            NatsMockAction::VerifyOutputSlice {
                output_index,
                nats_seq,
                count,
            } => {
                let zset = self.zset();
                let flushed = &zset.state().flushed;
                assert!(
                    flushed.len() >= output_index + count,
                    "VerifyOutputSlice: expected at least {} flushed records, got {}",
                    output_index + count,
                    flushed.len()
                );
                for i in 0..*count {
                    let seq = nats_seq + i;
                    assert_eq!(
                        flushed[output_index + i].unwrap_insert(),
                        &self.published[seq],
                        "Record mismatch: output[{}] vs published[{}] (seq {})",
                        output_index + i,
                        seq,
                        seq,
                    );
                }
            }

            NatsMockAction::AssertRecordCount(n) => {
                let actual = self.zset().state().flushed.len();
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
                    DEFAULT_TIMEOUT_MS,
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

pub(super) fn run_nats_mock_test(
    pipeline_config: impl Fn(&str) -> serde_json::Value,
    actions: &[NatsMockAction],
) -> AnyResult<()> {
    init_test_logger();

    let mut runner = NatsMockRunner::new()?;
    for (i, action) in actions.iter().enumerate() {
        println!("--- action {i}: {action:?} ---");
        runner.exec(action, &pipeline_config)?;
    }
    Ok(())
}

pub(super) fn basic_nats_config(nats_url: &str) -> serde_json::Value {
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
pub(super) fn nats_stall_config(nats_url: &str) -> serde_json::Value {
    nats_stall_config_with_timeout(nats_url, STALL_INACTIVITY_TIMEOUT_SECS, 2)
}

pub(super) fn nats_stall_config_with_timeout(
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
pub(super) fn stall_timeout() -> Duration {
    Duration::from_secs(STALL_INACTIVITY_TIMEOUT_SECS + 10)
}
