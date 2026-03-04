use super::mock_framework::*;
use anyhow::Result as AnyResult;

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
