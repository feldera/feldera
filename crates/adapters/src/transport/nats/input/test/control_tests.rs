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
