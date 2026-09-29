use super::mock_framework::*;
use anyhow::Result as AnyResult;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Basic Ingestion and Lifecycle
// ---------------------------------------------------------------------------

#[test]
fn test_nats_basic_input_consumption() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

// ---------------------------------------------------------------------------
// Retry and Recovery
// ---------------------------------------------------------------------------

/// Retry loop should report non-fatal errors every retry interval while
/// NATS is down and continue attempting reconnects.
#[test]
fn test_nats_retry_loop_when_server_unavailable() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Connector should enter retrying ERROR state, then recover automatically
/// once the server and stream become available again.
#[test]
fn test_nats_resume_after_server_becomes_available() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// PAUSE should stop the retry loop while in ERROR state; RESUME should restart
/// retries and eventually ingest when availability is restored.
#[test]
fn test_nats_pause_during_retry_loop() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Tests that pausing while the reader is Running and may have queued an error
/// (e.g. server just died) does not cause the next Extend to immediately enter
/// ErrorRetrying due to a stale error left in the channel.
///
/// Sequence: consume 1 record → kill server → pause immediately (reader error
/// may already be in-flight) → restart server + recreate stream → extend →
/// new records should arrive without spurious error-retry transitions.
#[test]
fn test_nats_no_stale_error_after_pause_extend() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        nats_stall_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertRecordCount(1),
            // Kill server — the reader task will notice and send an error,
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
}

/// Tests that when the stream is healthy but quiet (no messages arriving),
/// the inactivity timeout fires, the health check succeeds (consumer recreation
/// works), and the connector does NOT produce a fatal error. After the quiet
/// period, new messages published to the stream are still received correctly.
#[test]
fn test_nats_quiet_but_healthy_no_false_alarm() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Tests that a short server outage does not cause a false fatal error if NATS
/// comes back on the same address before inactivity timeout expires.
#[test]
fn test_nats_mid_run_server_restart_recovers_no_fatal() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Tests that inactivity timeout still drives first transport error emission.
#[test]
fn test_nats_inactivity_timeout_config_is_honored() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Test that startup no longer fails fast: unavailable server should trigger
/// retrying non-fatal errors after `Extend`.
#[test]
fn test_nats_connection_refused_enters_retry_loop() -> AnyResult<()> {
    use NatsMockAction::*;
    let nonexistent_url = "nats://127.0.0.1:59999";
    run_nats_mock_test(
        |_: &str| {
            format!(
                r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nonexistent_url}
            request_timeout_secs: 1
        stream_name: {STREAM_NAME}
        inactivity_timeout_secs: 1
        consumer_config:
            deliver_policy: All
            subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

/// Startup retry loop should continue emitting non-fatal transport errors at
/// retry cadence while the server remains unreachable.
#[test]
fn test_nats_startup_connection_refused_retries_repeatedly() -> AnyResult<()> {
    use NatsMockAction::*;
    let nonexistent_url = "nats://127.0.0.1:59999";
    run_nats_mock_test(
        |_: &str| {
            format!(
                r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nonexistent_url}
            request_timeout_secs: 1
        stream_name: {STREAM_NAME}
        inactivity_timeout_secs: 1
        retry_interval_secs: 1
        consumer_config:
            deliver_policy: All
            subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

/// Test that missing stream enters retry loop instead of failing pipeline open.
#[test]
fn test_nats_stream_not_found_enters_retry_loop() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        |nats_url| {
            format!(
                r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
            request_timeout_secs: 1
        stream_name: this_stream_does_not_exist
        inactivity_timeout_secs: 1
        consumer_config:
            deliver_policy: All
            subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

/// Test that connection timeout contributes to retry cadence instead of blocking open.
#[test]
fn test_nats_connection_timeout() -> AnyResult<()> {
    use NatsMockAction::*;
    // Use a non-routable IP address that will cause a connection timeout
    // 10.255.255.1 is a reserved address that should not respond
    let non_routable_url = "nats://10.255.255.1:4222";
    run_nats_mock_test(
        |_: &str| {
            format!(
                r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {non_routable_url}
            connection_timeout_secs: 1
            request_timeout_secs: 1
        stream_name: some_stream
        inactivity_timeout_secs: 1
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

/// Test that pausing while a startup retry attempt is in-flight interrupts the
/// attempt instead of waiting for connect/request timeouts to elapse.
#[test]
fn test_nats_pause_interrupts_inflight_retry_attempt() -> AnyResult<()> {
    use NatsMockAction::*;
    // Use a non-routable IP address that causes a long connect timeout.
    let non_routable_url = "nats://10.255.255.1:4222";
    run_nats_mock_test(
        |_: &str| {
            format!(
                r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {non_routable_url}
            connection_timeout_secs: 8
            request_timeout_secs: 8
        stream_name: some_stream
        inactivity_timeout_secs: 1
        retry_interval_secs: 1
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

/// Test that retry_interval_secs controls retry cadence in ERROR state.
///
/// Pinning `retry_max_interval_secs` to `retry_interval_secs` disables the
/// exponential growth, so retries run at a fixed (jittered) interval.
#[test]
fn test_nats_retry_interval_config_is_honored() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        |nats_url| {
            format!(
                r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
            request_timeout_secs: 1
        stream_name: missing_stream
        inactivity_timeout_secs: 1
        retry_interval_secs: 1
        retry_max_interval_secs: 1
        consumer_config:
            deliver_policy: All
            subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

/// Test that retry_interval_secs=0 is rejected early by configuration validation.
#[test]
fn test_nats_retry_interval_zero_rejected() {
    use NatsMockAction::*;
    run_nats_mock_test(
        |_nats_url| {
            r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: nats://127.0.0.1:4222
        stream_name: some_stream
        inactivity_timeout_secs: 1
        retry_interval_secs: 0
        consumer_config:
            deliver_policy: All
format:
    name: json
    config:
        update_format: raw
"#
            .to_string()
        },
        &[
            StartNats,
            CreatePipeline,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(1),
                needle: "retry_interval_secs",
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
) -> String {
    let max_attempts = retry_max_attempts
        .map(|n| format!("retry_max_attempts: {n}"))
        .unwrap_or_default();
    format!(
        r#"
stream: test_input
transport:
    name: nats_input
    config:
        connection_config:
            server_url: {nats_url}
            request_timeout_secs: 1
        stream_name: missing_stream
        inactivity_timeout_secs: 1
        retry_interval_secs: {retry_interval_secs}
        retry_max_interval_secs: {retry_max_interval_secs}
        {max_attempts}
        consumer_config:
            deliver_policy: All
            subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
    )
}

/// The delay between retries doubles on each consecutive failure.
///
/// With `retry_interval_secs: 1`, the third error (initial failure plus two
/// failed retries) arrives within about three seconds. The next retry is
/// then at least `nominal(3) / 2 = 2s` away, so no error may appear in the
/// following 1.5s. A fixed 1s interval would fail this check.
#[test]
fn test_nats_retry_backs_off_exponentially() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// With `retry_max_attempts` set, the connector gives up with a fatal error
/// after that many consecutive failed retries instead of retrying forever.
#[test]
fn test_nats_retry_max_attempts_ends_in_fatal_error() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// The circuit breaker applies to replay retries too.
#[test]
fn test_nats_replay_retry_max_attempts_ends_in_fatal_error() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// `retry_max_interval_secs` below `retry_interval_secs` is rejected when the
/// connector is created.
#[test]
fn test_nats_retry_max_interval_below_interval_rejected() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        |_| missing_stream_retry_config("nats://127.0.0.1:4222", 10, 5, None),
        &[
            StartNats,
            CreatePipeline,
            ExpectFatalErrorContains {
                timeout: Duration::from_secs(1),
                needle: "retry_max_interval_secs (5) must be at least retry_interval_secs (10)",
            },
        ],
    )
}

/// `retry_max_attempts: 0` is rejected when the connector is created.
#[test]
fn test_nats_retry_max_attempts_zero_rejected() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
#[test]
fn test_nats_pause_and_disconnect_delete_consumer() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// A replay consumer is deleted as soon as the replay completes.
#[test]
fn test_nats_replay_deletes_consumer() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Without a configured name, consumers are named after the table. The mock
/// pipeline's table has an empty name, which falls back to `feldera`.
#[test]
fn test_nats_consumer_name_defaults_to_table_prefix() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        basic_nats_config,
        &[
            StartNats,
            CreateStream,
            Publish(1),
            CreatePipeline,
            Extend,
            WaitForRecords(1),
            AssertConsumerNamesStartWith("feldera"),
            Disconnect,
        ],
    )
}

/// A configured consumer name is sanitized and used as the prefix.
#[test]
fn test_nats_consumer_name_uses_sanitized_configured_prefix() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
        |nats_url| {
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
            name: "orders.v1 reader"
            deliver_policy: All
            filter_subjects: [{SUBJECT_NAME}]
format:
    name: json
    config:
        update_format: raw
"#
            )
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
}

// ---------------------------------------------------------------------------
// Replay Happy Paths
// ---------------------------------------------------------------------------

/// Replay all published records from a fresh pipeline and verify correctness.
#[test]
fn test_nats_replay_basic() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay a subset of published records (partial range).
#[test]
fn test_nats_replay_partial_range() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay records then extend to consume new records published after the replay range.
#[test]
fn test_nats_replay_then_extend() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay with an empty range should succeed immediately without errors.
#[test]
fn test_nats_replay_empty_range() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Multiple sequential replays before extending.
#[test]
fn test_nats_replay_multiple_ranges() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

// ---------------------------------------------------------------------------
// Replay Failure Modes
// ---------------------------------------------------------------------------

/// Replay should treat transient stream unavailability as retryable (non-fatal)
/// and keep retrying until the stream is recreated with replayable data.
#[test]
fn test_nats_replay_stream_deleted_retries_and_recovers() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay should keep retrying with non-fatal errors while the server is
/// down and complete once the server and stream come back.
#[test]
fn test_nats_replay_server_killed_retries_non_fatal() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay fails with a fatal error when the stream has been purged and the
/// requested sequence numbers no longer exist.
#[test]
fn test_nats_replay_after_purge_errors() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
            // Purge the stream — all messages deleted.
            PurgeStream,
            // Second pipeline: attempt to replay sequences [1, 6) — should fail.
            CreatePipeline,
            Replay { start: 1, end: 6 },
            ExpectFatalErrorContains {
                timeout: stall_timeout(),
                needle: "Replay requested sequences",
            },
            Disconnect,
        ],
    )
}

/// Replay fails fast when the requested end sequence is above the stream tail.
#[test]
fn test_nats_replay_end_after_last_sequence_fails_fast() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay fails fast when the requested start sequence is older than the
/// stream head (messages have been purged and replaced with newer ones).
#[test]
fn test_nats_replay_start_before_first_sequence_fails_fast() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

/// Replay fails fast when requesting a non-empty range from an empty stream.
#[test]
fn test_nats_replay_empty_stream_fails_fast() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
}

// ---------------------------------------------------------------------------
// Disconnect Behavior
// ---------------------------------------------------------------------------

/// Verifies that after calling `Disconnect`, records published to the stream
/// are NOT delivered to the pipeline.
#[test]
fn test_nats_disconnect_stops_delivery() -> AnyResult<()> {
    use NatsMockAction::*;
    run_nats_mock_test(
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
            // Assert that no new records arrived — still exactly 3.
            AssertRecordCount(3),
        ],
    )
}
