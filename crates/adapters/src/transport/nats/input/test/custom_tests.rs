use super::super::ConnectorError;
use super::NatsTestRecord;
use super::util;
use crate::test::mock_input_pipeline;
use async_nats::jetstream;
use feldera_types::program_schema::Relation;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration Validation
// ---------------------------------------------------------------------------

/// Test that inactivity_timeout_secs=0 is rejected early by configuration validation.
#[test]
fn test_nats_inactivity_timeout_zero_rejected() {
    let config_str = r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: nats://127.0.0.1:4222
        stream_name: some_stream
        inactivity_timeout_secs: 0
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#;

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(config_str).unwrap(),
        Relation::empty(),
    );

    match result {
        Ok(_) => panic!("Expected inactivity_timeout_secs=0 to be rejected"),
        Err(err) => {
            let err_msg = format!("{err:#}");
            assert!(
                err_msg.contains("inactivity_timeout_secs"),
                "Error message should mention inactivity_timeout_secs, got: {err_msg}"
            );
        }
    }
}

/// Test that retry_interval_secs=0 is rejected early by configuration validation.
#[test]
fn test_nats_retry_interval_zero_rejected() {
    let config_str = r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: nats://127.0.0.1:4222
        stream_name: some_stream
        retry_interval_secs: 0
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#;

    let result = mock_input_pipeline::<NatsTestRecord, NatsTestRecord>(
        serde_yaml::from_str(config_str).unwrap(),
        Relation::empty(),
    );

    match result {
        Ok(_) => panic!("Expected retry_interval_secs=0 to be rejected"),
        Err(err) => {
            let err_msg = format!("{err:#}");
            assert!(
                err_msg.contains("retry_interval_secs"),
                "Error message should mention retry_interval_secs, got: {err_msg}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Validation Error Classification
// ---------------------------------------------------------------------------
//
// These tests verify that `validate_resume_position` and `validate_replay_range`
// classify errors correctly:
//   - Transient I/O errors (server down) → ConnectorError::Retryable
//   - Logical failures (out of bounds)   → ConnectorError::Fatal
//   - Valid inputs                        → Ok

const STREAM: &str = "validation_test_stream";
const SUBJECT: &str = "validation_test_subject";

/// Lightweight test harness for validation functions.
///
/// Manages a NATS server, JetStream context, and tokio runtime. The context
/// is created once during `start()` and survives `kill_server()`, so tests
/// can verify behavior against a dead server.
struct NatsTestFixture {
    _guard: Option<util::ProcessKillGuard>,
    js: jetstream::Context,
    rt: tokio::runtime::Runtime,
}

impl NatsTestFixture {
    /// Start a NATS server, create the test stream, and connect a JetStream context.
    fn start() -> Self {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let (guard, url) = util::start_nats_and_get_address().unwrap();
        let js = rt.block_on(async {
            util::create_stream(&url, STREAM, SUBJECT).await.unwrap();
            let client = util::wait_for_nats_ready(&url, Duration::from_secs(5))
                .await
                .unwrap();
            jetstream::new(client)
        });
        Self {
            _guard: Some(guard),
            js,
            rt,
        }
    }

    /// Publish `n` dummy JSON messages to the test subject.
    fn publish(&self, n: usize) {
        let msgs: Vec<_> = (0..n).map(|i| serde_json::json!({"x": i})).collect();
        self.rt.block_on(async {
            for msg in &msgs {
                let ack = self
                    .js
                    .publish(
                        SUBJECT.to_string(),
                        serde_json::to_string(msg).unwrap().into(),
                    )
                    .await
                    .unwrap();
                ack.await.unwrap();
            }
        });
    }

    /// Purge all messages from the test stream.
    fn purge(&self) {
        self.rt.block_on(async {
            let stream = self.js.get_stream(STREAM).await.unwrap();
            stream.purge().await.unwrap();
        });
    }

    /// Kill the NATS server. The JetStream context remains usable for
    /// testing — subsequent operations will fail with transient I/O errors.
    fn kill_server(&mut self) {
        self._guard.take();
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn validate_resume(nats: &NatsTestFixture, cursor: u64) -> Result<(), ConnectorError> {
    nats.rt.block_on(super::super::validate_resume_position(
        &nats.js, STREAM, cursor,
    ))
}

fn validate_replay(
    nats: &NatsTestFixture,
    range: std::ops::Range<u64>,
) -> Result<(), ConnectorError> {
    nats.rt.block_on(super::super::validate_replay_range(
        &nats.js, STREAM, &range,
    ))
}

// -- Resume validation --

#[test]
fn test_nats_validate_resume_server_down_is_retryable() {
    let mut nats = NatsTestFixture::start();
    nats.publish(1);
    nats.kill_server();
    assert!(matches!(
        validate_resume(&nats, 1),
        Err(ConnectorError::Retryable(_))
    ));
}

#[test]
fn test_nats_validate_resume_before_head_is_fatal() {
    let nats = NatsTestFixture::start();
    nats.publish(5);
    nats.purge();
    nats.publish(3);
    // Stream first_sequence is now 6; cursor 2 is before that.
    assert!(matches!(
        validate_resume(&nats, 2),
        Err(ConnectorError::Fatal(_))
    ));
}

#[test]
fn test_nats_validate_resume_with_gap_from_tail_is_fatal() {
    let nats = NatsTestFixture::start();
    nats.publish(3);
    // Message 1,2,3 in stream, try resume from 5, so a gap.
    assert!(matches!(
        validate_resume(&nats, 5),
        Err(ConnectorError::Fatal(_))
    ));
}

/// A fresh start (resume_cursor=0) do not need any stream seqeunce validation
/// and should succeeds on an empty stream.
#[test]
fn test_nats_validate_resume_fresh_start_is_ok() {
    let nats = NatsTestFixture::start();
    assert!(validate_resume(&nats, 0).is_ok());
}

#[test]
fn test_nats_validate_resume_continue_is_ok() {
    let nats = NatsTestFixture::start();
    nats.publish(3);
    assert!(validate_resume(&nats, 3).is_ok());
}

// -- Replay validation --

#[test]
fn test_nats_validate_replay_server_down_is_retryable() {
    let mut nats = NatsTestFixture::start();
    nats.publish(1);
    nats.kill_server();
    assert!(matches!(
        validate_replay(&nats, 1..2),
        Err(ConnectorError::Retryable(_))
    ));
}

#[test]
fn test_nats_validate_replay_range_exceeds_tail_is_fatal() {
    let nats = NatsTestFixture::start();
    nats.publish(3);
    // last_sequence=3; requesting [1, 100) exceeds tail.
    assert!(matches!(
        validate_replay(&nats, 1..100),
        Err(ConnectorError::Fatal(_))
    ));
}

#[test]
fn test_nats_validate_replay_range_precedes_head_is_fatal() {
    let nats = NatsTestFixture::start();
    nats.publish(3);
    nats.purge();
    nats.publish(4);
    // Sequence in stream 4,5,6,7. Missing 1,2,3.
    assert!(matches!(
        validate_replay(&nats, 1..8),
        Err(ConnectorError::Fatal(_))
    ));
}

#[test]
fn test_nats_validate_replay_empty_range_is_ok() {
    let nats = NatsTestFixture::start();
    assert!(validate_replay(&nats, 5..5).is_ok());
}

#[test]
fn test_nats_validate_replay_range_is_ok() {
    let nats = NatsTestFixture::start();
    nats.publish(6);
    assert!(validate_replay(&nats, 3..7).is_ok());
}
