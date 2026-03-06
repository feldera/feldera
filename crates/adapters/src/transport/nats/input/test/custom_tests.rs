use super::NatsTestRecord;
use crate::test::mock_input_pipeline;
use feldera_types::program_schema::Relation;

// ---------------------------------------------------------------------------
// Configuration Validation (No Test Framework)
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
