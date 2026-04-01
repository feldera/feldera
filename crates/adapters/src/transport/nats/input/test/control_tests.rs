use super::controller_framework::*;
use anyhow::Result as AnyResult;

// ---------------------------------------------------------------------------
// Fault Tolerance Cycles
// ---------------------------------------------------------------------------

#[test]
fn test_nats_ft_simple() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 5,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_with_checkpoints() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_without_checkpoints() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: false,
        },
    ]);
}

#[test]
fn test_nats_ft_alternating() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_initially_zero_without_checkpoint() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 0,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_initially_zero_with_checkpoint() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 15,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: false,
        },
        RunFtCycle {
            publish: 20,
            checkpoint: true,
        },
    ]);
}

#[test]
fn test_nats_ft_empty_step_checkpoint() {
    use NatsControllerAction::*;
    run_nats_ft_default(&[
        StartNats,
        CreateStream,
        RunFtCycle {
            publish: 5,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 0,
            checkpoint: true,
        },
        RunFtCycle {
            publish: 10,
            checkpoint: true,
        },
    ]);
}

// ---------------------------------------------------------------------------
// Fault Tolerance Replay and Stream Lifecycle
// ---------------------------------------------------------------------------

/// Tests that replay fails with an error (rather than looping forever) when the
/// stream has been purged and the checkpointed messages no longer exist.
///
/// Scenario:
/// 1. Publish 5 messages, run pipeline, checkpoint (sequences committed)
/// 2. Publish 5 more messages, consume them but do NOT checkpoint, stop
/// 3. Purge the stream (all messages deleted)
/// 4. Start pipeline — FT framework sends a Replay for the uncommitted
///    sequences, but those messages are gone -> expect fatal error
#[test]
fn test_nats_ft_replay_after_stream_purge() -> AnyResult<()> {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new()?.with_inactivity_timeout_secs(1),
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 5,
                checkpoint: false,
            },
            PurgeStream,
            ExpectStartupFatal,
        ],
    )
}

/// Tests rapid restart+replay with a named consumer.
///
/// This reproduces a bug where the previous ordered consumer hadn't expired yet,
/// causing "consumer already exists" errors. The fix generates unique consumer names
/// by appending a UUID suffix when a name is explicitly configured.
#[test]
fn test_nats_ft_with_named_consumer() -> AnyResult<()> {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new()?.with_consumer_name("my_named_consumer"),
        &[
            StartNats,
            CreateStream,
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            RunFtCycle {
                publish: 0,
                checkpoint: true,
            },
        ],
    )
}

/// Checkpoint, delete+recreate stream, then restart. Replay should fail
/// because the committed sequence numbers no longer exist in the new stream.
#[test]
fn test_nats_ft_stream_deletion_and_recreation() -> AnyResult<()> {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new()?.with_inactivity_timeout_secs(1),
        &[
            StartNats,
            CreateStream,
            // Round 1: publish 5 records, consume and checkpoint.
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            // Round 2: publish 5 more, consume but do NOT checkpoint.
            RunFtCycle {
                publish: 5,
                checkpoint: false,
            },
            // Delete and recreate the stream — all old messages are gone.
            DeleteStream,
            CreateStream,
            // Restart: FT framework tries to replay uncommitted sequences,
            // but they don't exist in the fresh stream -> fatal startup error.
            ExpectStartupFatal,
        ],
    )
}

/// Checkpoint, delete+recreate stream, publish new data, and restart.
/// Even with a fully checkpointed round, the checkpoint resume sequence no
/// longer matches the recreated stream and startup should fail fast.
#[test]
fn test_nats_ft_stream_deletion_after_full_checkpoint() -> AnyResult<()> {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new()?,
        &[
            StartNats,
            CreateStream,
            // Round 1: publish and checkpoint everything.
            RunFtCycle {
                publish: 5,
                checkpoint: true,
            },
            // Delete and recreate stream — but everything was checkpointed,
            // so nothing needs replaying.
            DeleteStream,
            CreateStream,
            // Resume metadata points at old stream sequence space; startup fails fatally.
            ExpectStartupFatal,
        ],
    )
}

// ---------------------------------------------------------------------------
// Startup Behavior
// ---------------------------------------------------------------------------

/// Startup should enter retrying error mode when the server is up but the
/// configured stream has not been created yet.
#[test]
fn test_nats_ft_startup_retries_when_stream_missing() -> AnyResult<()> {
    use NatsControllerAction::*;
    run_nats_controller_test(
        NatsControllerRunner::new()?.with_inactivity_timeout_secs(1),
        &[
            StartNats,
            // Intentionally skip CreateStream to force startup retry mode.
            ExpectStartupRetrying,
        ],
    )
}
