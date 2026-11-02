//! Tests for defects found while reviewing the NATS input hardening
//! (retry backoff, `retry_max_attempts`, named and deleted consumers).
//!
//! Each test states the behavior the connector should have. They fail until
//! the corresponding defect is fixed.

use crate::{
    test::{engine_test, pipeline::PipelineTestEngine},
    transport::nats::input::test::{
        control_tests::{
            NatsControllerAction, NatsControllerRunner, fatal_runner, run_nats_controller_test,
            test_controller_stop_waits_for_delete,
        },
        jetstream_proxy::ProxyFaults,
        mock_tests::{
            CONSUMER_COUNT_TIMEOUT, NatsMockAction, basic_nats_config, fast_retry_config,
            run_nats_mock_test,
        },
        util::SUBJECT_NAME,
    },
};
use feldera_adapterlib::test::ConnectorTestEngine;
use serde_json::json;
use std::time::Duration;

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

/// After the breaker gives up, the pipeline must keep showing the failed
/// connector and its fatal error, so that an operator can find it and decide
/// when to resume, as the documentation promises.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Nats))]
pub fn fatal_connector_stays_in_pipeline_status(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        fatal_runner(engine).unwrap(),
        &[
            StartNats,
            CreateStream,
            RunUntilConnectorFatal { publish: 5 },
            AssertInputKeepsFatalError,
        ],
    )
    .unwrap();
}

/// The controller injects the pipeline's `given_name` into the consumer's
/// metadata under `pipeline`, so consumers are traceable from the NATS side.
/// Consumers are named after the input table and are deleted when the
/// pipeline stops.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Nats))]
pub fn pipeline_metadata_on_consumer(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine)
            .unwrap()
            .with_given_name("orders_pipeline"),
        &[
            StartNats,
            CreateStream,
            RunAndInspectConsumers {
                publish: 3,
                name_prefix: "test_input1",
                metadata: &[("pipeline", "orders_pipeline")],
            },
        ],
    )
    .unwrap();
}

/// A user-supplied `pipeline` metadata value takes precedence over the
/// injected pipeline name, and other user metadata is preserved.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Nats))]
pub fn pipeline_metadata_does_not_overwrite_user_value(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new(engine)
            .unwrap()
            .with_given_name("orders_pipeline")
            .with_consumer_name("orders_reader")
            .with_consumer_metadata("pipeline", "user_specified")
            .with_consumer_metadata("team", "ingest"),
        &[
            StartNats,
            CreateStream,
            RunAndInspectConsumers {
                publish: 3,
                name_prefix: "orders_reader",
                metadata: &[("pipeline", "user_specified"), ("team", "ingest")],
            },
        ],
    )
    .unwrap();
}

/// A checkpoint taken after a connector failed must keep that connector,
/// so that a restart with the same configuration sees no changes (and does
/// not wait for approval of a connector the user never added).
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Nats))]
pub fn fatal_connector_survives_checkpoint(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        fatal_runner(engine).unwrap(),
        &[
            StartNats,
            CreateStream,
            RunUntilConnectorFatal { publish: 5 },
            CheckpointAndStop,
            AssertRestartSeesNoConnectorChanges,
        ],
    )
    .unwrap();
}

/// A connector that failed keeps its position: restarting from a checkpoint
/// taken after the failure must not ingest the stream again from the start.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Nats))]
pub fn fatal_connector_keeps_cursor_across_restart(engine: &dyn PipelineTestEngine) {
    use NatsControllerAction::*;
    run_nats_controller_test(
        fatal_runner(engine).unwrap(),
        &[
            StartNats,
            CreateStream,
            RunUntilConnectorFatal { publish: 5 },
            CheckpointAndStop,
            AssertRestartIngestsNothingNew,
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

/// A process may exit when Controller::stop returns. Waiting only for the
/// circuit thread does not guarantee the NATS worker finished its delete.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Nats))]
pub fn controller_stop_waits_for_consumer_delete(engine: &dyn PipelineTestEngine) {
    test_controller_stop_waits_for_delete(engine, false, false).unwrap();
}

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

/// A consumer the client library recreated must not outlive a pause.
///
/// The library names a recreated ordered consumer `{name}_{n}` and deletes
/// the one it replaces, so the connector must delete the recreated consumer
/// rather than the name it chose itself.
#[engine_test(services(Nats))]
pub fn library_recreated_consumer_is_deleted_on_pause(engine: &dyn ConnectorTestEngine) {
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
            DeleteConsumers,
            // The library notices the deletion and recreates the consumer.
            WaitForConsumerCount {
                count: 1,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            Pause,
            WaitForConsumerCount {
                count: 0,
                timeout: CONSUMER_COUNT_TIMEOUT,
            },
            AssertConsumerCountersMatchServer,
            Disconnect,
        ],
    )
    .unwrap();
}

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
