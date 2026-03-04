use super::NatsTestRecord;
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
        pipeline_config: &dyn Fn(&str) -> String,
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
                let config_str = pipeline_config(self.nats_url());
                println!("Config:\n{config_str}");

                match mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
                    serde_yaml::from_str(&config_str).unwrap(),
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
            }

            NatsMockAction::DisconnectAllowNonFatal => {
                assert!(
                    !self.mock_input_consumer().state().eoi,
                    "Streaming NATS connector should never signal end-of-input"
                );
                self.endpoint().disconnect();
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
        }
        Ok(())
    }
}

pub(super) fn run_nats_mock_test(
    pipeline_config: impl Fn(&str) -> String,
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

pub(super) fn basic_nats_config(nats_url: &str) -> String {
    format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
        stream_name: {STREAM_NAME}
        consumer_config:
            deliver_policy: All
            filter_subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
    )
}

/// Inactivity timeout used by [`nats_stall_config`] (seconds).
const STALL_INACTIVITY_TIMEOUT_SECS: u64 = 2;

/// Config helper: NATS config with inactivity timeout and request_timeout_secs=2.
pub(super) fn nats_stall_config(nats_url: &str) -> String {
    nats_stall_config_with_timeout(nats_url, STALL_INACTIVITY_TIMEOUT_SECS, 2)
}

pub(super) fn nats_stall_config_with_timeout(
    nats_url: &str,
    inactivity_timeout_secs: u64,
    request_timeout_secs: u64,
) -> String {
    format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
            request_timeout_secs: {request_timeout_secs}
        stream_name: {STREAM_NAME}
        inactivity_timeout_secs: {inactivity_timeout_secs}
        consumer_config:
            deliver_policy: All
            filter_subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
    )
}

/// Stall detection budget: inactivity_timeout + request_timeout(2) + slack(8).
pub(super) fn stall_timeout() -> Duration {
    Duration::from_secs(STALL_INACTIVITY_TIMEOUT_SECS + 10)
}
