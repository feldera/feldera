use super::OutputEndpointControl;
use crate::{
    Controller, PipelineConfig,
    controller::{ControllerStatusContext, TransactionInfo},
    test::{
        TestStruct, default_timeout_ms, init_test_logger, test_circuit,
        test_circuit_without_persistent_ids, wait,
    },
};
use anyhow::anyhow;
use crossbeam::sync::Parker;
use feldera_adapterlib::format::BufferSize;
use feldera_types::{config::OutputEndpointConfig, memory_pressure::MemoryPressure};
use serde_json::json;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::{File, create_dir},
    io::Write,
    sync::{Arc, Mutex, atomic::Ordering, mpsc},
    thread,
    time::Duration,
};
use tempfile::TempDir;
use tokio::sync::oneshot;
use tracing::info;

use dbsp::DetailedError;

/// A full disk must fail the pipeline with the storage error.
///
/// A background merger that cannot write panics.  A worker waiting on that
/// merger never replies, so the circuit thread must notice the panic while it
/// waits; otherwise the step blocks forever and the pipeline reports
/// `Running`.
#[test]
fn test_storage_full_reports_storage_error() {
    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir.path().join("input.csv");
    let mut input = File::create_new(&input_path).unwrap();
    let padding = "x".repeat(100);
    let mut next_id = 0;
    let mut append_rows = |n: usize| {
        for _ in 0..n {
            writeln!(input, "{next_id},true,{next_id},{padding}{next_id}").unwrap();
            next_id += 1;
        }
        input.flush().unwrap();
    };
    append_rows(200_000);

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 2,
        "storage_config": { "path": storage_dir },
        "storage": { "min_storage_bytes": 0 },
        "dev_tweaks": { "storage_mb_max": 1 },
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": input_path.display().to_string(), "follow": true },
                },
                "format": { "name": "csv" },
            },
        },
    }))
    .unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));
    let errors_clone = errors.clone();
    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(move |e, _| errors_clone.lock().unwrap().push(e)),
    )
    .unwrap();
    controller.start();

    // The circuit notices dead workers only when it steps, so keep input
    // flowing, as a live source would.
    wait(
        || {
            append_rows(100);
            !errors.lock().unwrap().is_empty()
        },
        60_000,
    )
    .expect("filling storage did not fail the pipeline");
    let error = errors.lock().unwrap()[0].clone();
    assert_eq!(error.error_code(), "RuntimeError.WorkerPanic");
    assert!(
        error.to_string().contains("no storage space"),
        "the error should explain that storage is full: {error}"
    );

    controller.stop().unwrap();
}

/// A worker-side checkpoint failure must reach the caller of
/// [Controller::checkpoint] rather than deadlock the circuit thread.
///
/// `DBSPHandle::broadcast_command` kills the circuit when a worker answers a
/// command with an error, and killing the runtime joins its aux threads, one
/// of which is each output endpoint's thread.  Those threads therefore have to
/// exit on the kill: it runs on the circuit thread, so a thread that instead
/// waits for the controller to reach `Terminated` waits on the thread that is
/// waiting on it.  Regression test for issue #7075.
#[test]
fn checkpoint_error_does_not_deadlock() {
    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir.path().join("input.csv");
    let output_path = tempdir.path().join("output.csv");
    File::create_new(&input_path).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 2,
        "storage_config": { "path": storage_dir },
        "storage": true,
        "fault_tolerance": {},
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": input_path.display().to_string(), "follow": true },
                },
                "format": { "name": "csv" },
            },
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path.display().to_string() },
                },
                "format": { "name": "csv", "config": {} },
            },
        },
    }))
    .unwrap();

    // The circuit has no persistent operator ids, so every worker fails the
    // `Checkpoint` command with `StorageError::NoPersistentId`.
    let controller = Arc::new(
        Controller::with_test_config(
            |circuit_config| {
                Ok(test_circuit_without_persistent_ids::<TestStruct>(
                    circuit_config,
                    &TestStruct::schema(),
                ))
            },
            &config,
            Box::new(|e, _| info!("error: {e}")),
        )
        .unwrap(),
    );
    controller.start();
    wait(|| !controller.is_replaying(), default_timeout_ms()).unwrap();

    // Check from another thread so that the test reports a failure instead of
    // hanging the whole test binary if the deadlock comes back.
    let (sender, receiver) = mpsc::channel();
    let checkpointer = controller.clone();
    thread::Builder::new()
        .name("checkpoint-requester".into())
        .spawn(move || {
            let _ = sender.send(checkpointer.checkpoint().err());
        })
        .unwrap();

    let error = receiver
        .recv_timeout(Duration::from_secs(60))
        .expect("`checkpoint()` must return instead of blocking forever")
        .expect("`checkpoint()` must fail on a circuit with no persistent operator ids");
    assert!(
        error.to_string().contains("persistent id"),
        "unexpected checkpoint error: {error}"
    );

    // The kill is what put the pipeline in this state: this path never reaches
    // `Terminated`, so a shutdown signal that only tracked the state would
    // still read as running here.
    assert!(controller.inner.shutdown_token().is_cancelled());

    // The circuit is already dead, but the controller still owns the endpoints
    // and this drops them before `tempdir` goes away underneath them.
    controller.initiate_stop();
}

#[test]
fn lir() {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": null,
        "storage": null,
        "clock_resolution_usecs": null,
        "inputs": {},
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &[],
                &[Some("output")],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    let lir = controller.lir().as_json().clone();
    let actual: serde_json::Value = serde_json::from_str(&lir).unwrap();
    let expected: serde_json::Value = serde_json::from_str(
        r#"{
  "edges": [
    {
      "from": "0",
      "stream_id": 1,
      "to": "1"
    },
    {
      "from": "1",
      "stream_id": null,
      "to": "2"
    },
    {
      "from": "2",
      "stream_id": 2,
      "to": "3"
    },
    {
      "from": "3",
      "stream_id": 3,
      "to": "4"
    },
    {
      "from": "3",
      "stream_id": 3,
      "to": "9"
    },
    {
      "from": "4",
      "stream_id": 4,
      "to": "5"
    },
    {
      "from": "4",
      "stream_id": 4,
      "to": "7"
    },
    {
      "from": "4",
      "stream_id": 4,
      "to": "12"
    },
    {
      "from": "6",
      "stream_id": 5,
      "to": "7"
    },
    {
      "from": "6",
      "stream_id": null,
      "to": "8"
    },
    {
      "from": "7",
      "stream_id": 8,
      "to": "8"
    },
    {
      "from": "7",
      "stream_id": 8,
      "to": "10"
    },
    {
      "from": "7",
      "stream_id": 8,
      "to": "13"
    },
    {
      "from": "10",
      "stream_id": 9,
      "to": "11"
    },
    {
      "from": "13",
      "stream_id": 10,
      "to": "14"
    }
  ],
  "nodes": [
    {
      "id": "0",
      "implements": [
        "input"
      ],
      "operation": "Input"
    },
    {
      "id": "1",
      "implements": [
        "input"
      ],
      "operation": "ExchangeSender"
    },
    {
      "id": "2",
      "implements": [
        "input"
      ],
      "operation": "ExchangeReceiver"
    },
    {
      "id": "3",
      "implements": [
        "input"
      ],
      "operation": "merge shards"
    },
    {
      "id": "4",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "Accumulator"
    },
    {
      "id": "5",
      "implements": [
        "input.output"
      ],
      "operation": "AccumulateOutput"
    },
    {
      "id": "6",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "Z1 (trace)"
    },
    {
      "id": "7",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "AccumulateUntimedTraceAppend"
    },
    {
      "id": "8",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "Z1 (trace)"
    },
    {
      "id": "9",
      "implements": [],
      "operation": "Recorder"
    },
    {
      "id": "10",
      "implements": [
        "input.output"
      ],
      "operation": "Apply"
    },
    {
      "id": "11",
      "implements": [
        "input.output"
      ],
      "operation": "Output"
    },
    {
      "id": "12",
      "implements": [
        "output"
      ],
      "operation": "AccumulateOutput"
    },
    {
      "id": "13",
      "implements": [
        "output"
      ],
      "operation": "Apply"
    },
    {
      "id": "14",
      "implements": [
        "output"
      ],
      "operation": "Output"
    }
  ]
}"#,
    )
    .unwrap();

    println!("{actual:#}");
    assert_eq!(actual, expected);
}

/// Test that `ControllerStatus` and its `ExternalControllerStatus` conversion produce identical JSON.
///
/// This test verifies that:
/// 1. A `ControllerStatus` instance with detailed mock data can be serialized to JSON
/// 2. Converting it to `ExternalControllerStatus` via `to_api_type()` and serializing produces the same JSON
/// 3. All fields (global metrics, suspend errors, input/output endpoints) are correctly preserved
///
/// This ensures consistency between the internal representation (with atomics/mutexes) and
/// the external API type used for HTTP responses.
#[test]
fn test_external_controller_status_serialization() {
    use crate::controller::stats::InputEndpointStatus;
    use crate::{ControllerStatus, OutputEndpointConfig, PipelineState};
    use chrono::{TimeZone, Utc};
    use feldera_types::suspend::{PermanentSuspendError, SuspendError};
    use std::sync::atomic::Ordering;
    use uuid::Uuid;

    // Helper function to create a ControllerStatus with detailed mock values
    fn create_mock_status() -> ControllerStatus {
        let config = serde_json::from_value(json!({
            "name": "test_pipeline",
            "workers": 4,
        }))
        .unwrap();

        let mut status = ControllerStatus::new(config, 998500, None, Uuid::nil());

        // Configure global metrics using available public API
        status.set_state(PipelineState::Paused);
        status.set_bootstrap_in_progress(false);

        // Set public atomic fields
        status.global_metrics.start_time = Utc.timestamp_opt(1700000000, 0).unwrap();
        status.global_metrics.incarnation_uuid =
            Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        status.global_metrics.initial_start_time = Utc.timestamp_opt(1699999000, 0).unwrap();
        status
            .global_metrics
            .storage_bytes
            .store(1024 * 1024 * 1024 * 2, Ordering::Relaxed); // 2 GB
        status
            .global_metrics
            .storage_mb_secs
            .store(500000, Ordering::Relaxed);
        status
            .global_metrics
            .runtime_elapsed_msecs
            .store(98000, Ordering::Relaxed);
        status
            .global_metrics
            .buffered_input_records
            .store(1500, Ordering::Relaxed);
        status
            .global_metrics
            .buffered_input_bytes
            .store(150000, Ordering::Relaxed);
        status
            .global_metrics
            .total_input_records
            .store(1000000, Ordering::Relaxed);
        status
            .global_metrics
            .total_input_bytes
            .store(500000000, Ordering::Relaxed);
        status
            .global_metrics
            .total_processed_records
            .store(998500, Ordering::Relaxed);
        status
            .global_metrics
            .total_processed_bytes
            .store(499250000, Ordering::Relaxed);
        status
            .global_metrics
            .total_completed_records
            .store(998000, Ordering::Relaxed);

        // Add input endpoints
        let mut inputs = status.inputs.write();

        // Input 1: kafka_input with errors and paused
        let kafka_endpoint = InputEndpointStatus::new(
            "kafka_input",
            serde_json::from_value(json!({
                "stream": "test_stream",
                "transport": {
                    "name": "kafka_input",
                    "config": {
                        "topics": ["test-topic"],
                        "bootstrap.servers": "localhost:9092"
                    }
                },
                "format": {
                    "name": "json",
                    "config": {}
                }
            }))
            .unwrap(),
            None,
            None,
            None,
        );
        kafka_endpoint
            .metrics
            .total_bytes
            .store(250000000, Ordering::Relaxed);
        kafka_endpoint
            .metrics
            .total_records
            .store(500000, Ordering::Relaxed);
        kafka_endpoint
            .metrics
            .buffered_records
            .store(750, Ordering::Relaxed);
        kafka_endpoint
            .metrics
            .buffered_bytes
            .store(75000, Ordering::Relaxed);
        kafka_endpoint
            .metrics
            .num_transport_errors
            .store(4, Ordering::Relaxed);
        kafka_endpoint
            .metrics
            .num_parse_errors
            .store(12, Ordering::Relaxed);
        kafka_endpoint
            .metrics
            .end_of_input
            .store(false, Ordering::Relaxed);
        kafka_endpoint.set_paused_by_user(true);
        kafka_endpoint.transport_error(
            true,
            None,
            &anyhow!("Connection refused: Unable to connect to Kafka broker"),
        );
        inputs.insert(0, kafka_endpoint);

        // Input 2: file_input with barrier and no errors
        let file_endpoint = InputEndpointStatus::new(
            "file_input",
            serde_json::from_value(json!({
                "stream": "file_stream",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": "/data/input.csv"
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }))
            .unwrap(),
            None,
            None,
            None,
        );
        file_endpoint
            .metrics
            .total_bytes
            .store(250000000, Ordering::Relaxed);
        file_endpoint
            .metrics
            .total_records
            .store(500000, Ordering::Relaxed);
        file_endpoint
            .metrics
            .buffered_records
            .store(750, Ordering::Relaxed);
        file_endpoint
            .metrics
            .buffered_bytes
            .store(75000, Ordering::Relaxed);
        file_endpoint
            .metrics
            .num_transport_errors
            .store(0, Ordering::Relaxed);
        file_endpoint
            .metrics
            .num_parse_errors
            .store(3, Ordering::Relaxed);
        file_endpoint
            .metrics
            .end_of_input
            .store(true, Ordering::Relaxed);
        file_endpoint.barrier.store(true, Ordering::Relaxed);
        inputs.insert(1, file_endpoint);

        drop(inputs);

        // Add output endpoint
        let output_config: OutputEndpointConfig = serde_json::from_value(json!({
            "stream": "test_output",
            "transport": {
                "name": "http_output",
                "config": {}
            },
            "format": {
                "name": "json",
                "config": {}
            }
        }))
        .unwrap();
        status.add_output(&0, "http_output", &output_config, None, true);

        // Set output metrics
        if let Some(output) = status.output_status().get(&0) {
            output
                .metrics
                .transmitted_records
                .store(998000, Ordering::Relaxed);
            output
                .metrics
                .transmitted_bytes
                .store(499000000, Ordering::Relaxed);
            output.metrics.queued_records.store(100, Ordering::Relaxed);
            output.metrics.queued_batches.store(5, Ordering::Relaxed);
            output.metrics.buffered_records.store(50, Ordering::Relaxed);
            output.metrics.buffered_batches.store(2, Ordering::Relaxed);
            output.metrics.num_encode_errors.store(8, Ordering::Relaxed);
            output
                .metrics
                .num_transport_errors
                .store(3, Ordering::Relaxed);
            output
                .metrics
                .total_processed_input_records
                .store(998500, Ordering::Relaxed);
            output
                .metrics
                .memory
                .store(1024 * 1024 * 10, Ordering::Relaxed); // 10 MB
        }

        status
    }

    // Create ControllerStatus with mock values
    let controller_status = create_mock_status();

    // Convert to ExternalControllerStatus using the to_api_type method
    let mut external_status = controller_status.to_api_type(ControllerStatusContext {
        suspend_error: Err(SuspendError::Permanent(vec![
            PermanentSuspendError::StorageRequired,
            PermanentSuspendError::UnsupportedInputEndpoint("kafka_input".to_string()),
        ])),
        checkpoint_activity: feldera_types::checkpoint::CheckpointActivity::Idle,
        permanent_checkpoint_errors: None,
        pipeline_complete: false,
        transaction_info: TransactionInfo::default(),
        memory_pressure: MemoryPressure::default(),
        memory_pressure_epoch: 0,
        disk_usage: None,
        include_connector_errors: false,
    });
    external_status.global_metrics.rss_bytes = 1024 * 1024 * 512; // 512 MB
    external_status.global_metrics.cpu_msecs = 45_000;
    external_status.global_metrics.uptime_msecs = 120_000;

    // Serialize the ExternalControllerStatus to JSON
    let external_json = serde_json::to_value(&external_status)
        .expect("Failed to serialize ExternalControllerStatus");

    // Verify specific fields to ensure they were properly converted
    assert_eq!(
        external_json["global_metrics"]["state"],
        json!("Paused"),
        "Pipeline state should be Paused"
    );
    assert_eq!(
        external_json["global_metrics"]["rss_bytes"],
        json!(1024 * 1024 * 512),
        "RSS bytes should match"
    );
    assert_eq!(
        external_json["global_metrics"]["total_input_records"],
        json!(1000000),
        "Total input records should match"
    );

    // Verify suspend error
    assert!(
        external_json["suspend_error"].is_object(),
        "Suspend error should be present"
    );
    assert_eq!(
        external_json["suspend_error"]["Permanent"][0]["StorageRequired"],
        json!(null),
        "Should have StorageRequired error"
    );

    // Verify inputs array
    let inputs = external_json["inputs"]
        .as_array()
        .expect("inputs should be an array");
    assert_eq!(inputs.len(), 2, "Should have 2 input endpoints");

    // Verify first input (kafka_input)
    assert_eq!(
        inputs[1]["endpoint_name"],
        json!("kafka_input"),
        "First input should be kafka_input"
    );
    assert_eq!(
        inputs[1]["metrics"]["total_records"],
        json!(500000),
        "kafka_input should have 500000 total records"
    );
    assert_eq!(
        inputs[1]["paused"],
        json!(true),
        "kafka_input should be paused"
    );
    assert_eq!(
        inputs[1]["fatal_error"],
        json!("Connection refused: Unable to connect to Kafka broker"),
        "kafka_input should have fatal error"
    );

    // Verify second input (file_input)
    assert_eq!(
        inputs[0]["endpoint_name"],
        json!("file_input"),
        "Second input should be file_input"
    );
    assert_eq!(
        inputs[0]["barrier"],
        json!(true),
        "file_input should have barrier"
    );
    assert_eq!(
        inputs[0]["metrics"]["end_of_input"],
        json!(true),
        "file_input should have end_of_input"
    );

    // Verify outputs array
    let outputs = external_json["outputs"]
        .as_array()
        .expect("outputs should be an array");
    assert_eq!(outputs.len(), 1, "Should have 1 output endpoint");

    // Verify output (http_output)
    assert_eq!(
        outputs[0]["endpoint_name"],
        json!("http_output"),
        "Output should be http_output"
    );
    assert_eq!(
        outputs[0]["metrics"]["transmitted_records"],
        json!(998000),
        "http_output should have 998000 transmitted records"
    );
    assert_eq!(
        outputs[0]["metrics"]["num_encode_errors"],
        json!(8),
        "http_output should have 8 encode errors"
    );
}

#[test]
fn test_stats_reports_disk_usage_from_context() {
    use crate::{ControllerStatus, controller::stats::ControllerStatusContext};
    use feldera_storage::disk::DiskUsage;
    use feldera_types::suspend::SuspendError;
    use uuid::Uuid;

    let global_metrics = |disk_usage: Option<DiskUsage>| {
        ControllerStatus::new(
            serde_json::from_value(json!({})).unwrap(),
            0,
            None,
            Uuid::nil(),
        )
        .to_api_type(ControllerStatusContext {
            suspend_error: Ok::<(), SuspendError>(()),
            checkpoint_activity: feldera_types::checkpoint::CheckpointActivity::Idle,
            permanent_checkpoint_errors: None,
            pipeline_complete: false,
            transaction_info: TransactionInfo::default(),
            memory_pressure: MemoryPressure::default(),
            memory_pressure_epoch: 0,
            disk_usage,
            include_connector_errors: false,
        })
        .global_metrics
    };

    // Absence means unknown, never zero.
    let without = global_metrics(None);
    assert_eq!(without.disk_total_bytes, None);
    assert_eq!(without.disk_available_bytes, None);

    let with = global_metrics(Some(DiskUsage {
        total_bytes: 1000,
        available_bytes: 200,
    }));
    assert_eq!(with.disk_total_bytes, Some(1000));
    assert_eq!(with.disk_available_bytes, Some(200));
}

/// Test that custom connector metrics registered via `set_input_custom_metrics` are
/// included in the Prometheus output produced by the custom-metrics loop in
/// `write_metrics`.
#[test]
fn test_custom_connector_metrics_prometheus_output() {
    use crate::{
        ControllerStatus,
        controller::{stats::InputEndpointStatus, write_custom_metrics},
        server::metrics::{LabelStack, MetricsWriter, PrometheusFormatter},
    };
    use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
    use std::sync::Arc;
    use uuid::Uuid;

    struct MockMetrics;

    impl ConnectorMetrics for MockMetrics {
        fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
            vec![
                (
                    "kafka_consumer_lag",
                    "Consumer lag for the Kafka topic partition.",
                    ValueType::Gauge,
                    42.0,
                ),
                (
                    "kafka_messages_fetched_total",
                    "Total number of messages fetched from Kafka.",
                    ValueType::Counter,
                    1000.0,
                ),
            ]
        }
    }

    let config = serde_json::from_value(json!({
        "name": "test_custom_metrics",
        "workers": 1,
    }))
    .unwrap();

    let status = ControllerStatus::new(config, 0, None, Uuid::nil());

    let endpoint = InputEndpointStatus::new(
        "kafka_in",
        serde_json::from_value(json!({
            "stream": "s",
            "transport": { "name": "kafka_input", "config": { "topics": ["t"], "bootstrap.servers": "localhost:9092" } },
            "format": { "name": "json", "config": {} }
        }))
        .unwrap(),
        None,
        None,
        None,
    );
    status.inputs.write().insert(0, endpoint);

    // Register custom metrics on the endpoint.
    status.set_input_custom_metrics(0, Arc::new(MockMetrics));

    let mut writer = MetricsWriter::<PrometheusFormatter>::new();
    let labels = LabelStack::new();
    write_custom_metrics(&status, &mut writer, &labels);
    let output = writer.into_output();

    // Verify custom metric names and values appear with the endpoint label.
    assert!(
        output.contains("kafka_consumer_lag"),
        "output missing kafka_consumer_lag:\n{output}"
    );
    assert!(
        output.contains("kafka_messages_fetched_total"),
        "output missing kafka_messages_fetched_total:\n{output}"
    );
    assert!(
        output.contains(r#"endpoint="kafka_in""#),
        "output missing endpoint label:\n{output}"
    );
    assert!(
        output.contains("42"),
        "output missing gauge value 42:\n{output}"
    );
    assert!(
        output.contains("1000"),
        "output missing counter value 1000:\n{output}"
    );
}

/// Test that custom connector metrics registered on output endpoints are
/// included in Prometheus output.
#[test]
fn test_custom_output_connector_metrics_prometheus_output() {
    use crate::{
        ControllerStatus,
        controller::write_custom_metrics,
        server::metrics::{LabelStack, MetricsWriter, PrometheusFormatter},
    };
    use feldera_adapterlib::metrics::{ConnectorHistogram, ConnectorMetrics, ValueType};
    use feldera_storage::histogram::ExponentialHistogram;
    use feldera_types::config::OutputEndpointConfig;
    use std::sync::Arc;
    use uuid::Uuid;

    struct MockMetrics;

    impl ConnectorMetrics for MockMetrics {
        fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
            vec![(
                "mock_metric_total",
                "Mock connector-specific metric for the output connector.",
                ValueType::Counter,
                3.0,
            )]
        }

        fn histograms(&self) -> Vec<ConnectorHistogram> {
            let histogram = ExponentialHistogram::new();
            histogram.record(5u64);
            histogram.record(15u64);
            vec![ConnectorHistogram {
                name: "mock_latency_microseconds",
                help: "Mock connector-specific histogram for the output connector.",
                snapshot: histogram.snapshot(),
            }]
        }
    }

    let config = serde_json::from_value(json!({
        "name": "test_output_custom_metrics",
        "workers": 1,
    }))
    .unwrap();
    let status = ControllerStatus::new(config, 0, None, Uuid::nil());

    let output_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "s",
        "transport": { "name": "http_output", "config": {} },
        "format": { "name": "json", "config": {} }
    }))
    .unwrap();
    status.add_output(&0, "mock_output", &output_config, None, true);
    status.set_output_custom_metrics(0, Arc::new(MockMetrics));

    let mut writer = MetricsWriter::<PrometheusFormatter>::new();
    let labels = LabelStack::new();
    write_custom_metrics(&status, &mut writer, &labels);
    let output = writer.into_output();

    assert!(
        output.contains("mock_metric_total"),
        "output missing mock_metric_total:\n{output}"
    );
    assert!(
        output.contains(r#"endpoint="mock_output""#),
        "output missing endpoint label:\n{output}"
    );
    assert!(output.contains("3"), "output missing value 3:\n{output}");

    // The histogram is exported with its standard `_bucket`, `_sum`, and
    // `_count` series under a single `# TYPE ... histogram` header.  The mock
    // records observations of 5 and 15, so sum is 20 and count is 2.
    assert!(
        output.contains("# TYPE mock_latency_microseconds histogram"),
        "output missing histogram type header:\n{output}"
    );
    assert!(
        output.contains(r#"mock_latency_microseconds_bucket{endpoint="mock_output","#),
        "output missing histogram bucket:\n{output}"
    );
    assert!(
        output.contains(r#"mock_latency_microseconds_sum{endpoint="mock_output"} 20"#),
        "output missing histogram sum:\n{output}"
    );
    assert!(
        output.contains(r#"mock_latency_microseconds_count{endpoint="mock_output"} 2"#),
        "output missing histogram count:\n{output}"
    );
}

/// Test that when two endpoints register the same custom metric name, the
/// grouping logic emits a single `# TYPE` header for that metric (Prometheus
/// format violation fix).
#[test]
fn test_custom_connector_metrics_prometheus_grouping() {
    use crate::{
        ControllerStatus,
        controller::{stats::InputEndpointStatus, write_custom_metrics},
        server::metrics::{LabelStack, MetricsWriter, PrometheusFormatter},
    };
    use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
    use std::sync::Arc;
    use uuid::Uuid;

    struct MockMetrics;

    impl ConnectorMetrics for MockMetrics {
        fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
            vec![(
                "kafka_consumer_lag",
                "Consumer lag for the Kafka topic partition.",
                ValueType::Gauge,
                42.0,
            )]
        }
    }

    let config = serde_json::from_value(json!({
        "name": "test_grouping",
        "workers": 1,
    }))
    .unwrap();

    let status = ControllerStatus::new(config, 0, None, Uuid::nil());

    for (id, name) in [(0u64, "kafka_in_1"), (1u64, "kafka_in_2")] {
        let endpoint = InputEndpointStatus::new(
            name,
            serde_json::from_value(json!({
                "stream": "s",
                "transport": { "name": "kafka_input", "config": { "topics": ["t"], "bootstrap.servers": "localhost:9092" } },
                "format": { "name": "json", "config": {} }
            }))
            .unwrap(),
            None,
            None,
            None,
        );
        status.inputs.write().insert(id, endpoint);
        status.set_input_custom_metrics(id, Arc::new(MockMetrics));
    }

    let mut writer = MetricsWriter::<PrometheusFormatter>::new();
    let labels = LabelStack::new();
    write_custom_metrics(&status, &mut writer, &labels);
    let output = writer.into_output();

    // `# TYPE kafka_consumer_lag gauge` must appear exactly once.
    let type_header = "# TYPE kafka_consumer_lag gauge";
    let occurrences = output.matches(type_header).count();
    assert_eq!(
        occurrences, 1,
        "expected exactly one '# TYPE kafka_consumer_lag gauge', got {occurrences}:\n{output}"
    );

    // Both endpoint labels must appear in the output.
    assert!(
        output.contains(r#"endpoint="kafka_in_1""#),
        "output missing kafka_in_1 label:\n{output}"
    );
    assert!(
        output.contains(r#"endpoint="kafka_in_2""#),
        "output missing kafka_in_2 label:\n{output}"
    );

    // There must be no second `# TYPE` header between the two endpoint values.
    let first_pos = output.find(r#"endpoint="kafka_in_1""#).unwrap();
    let second_pos = output.find(r#"endpoint="kafka_in_2""#).unwrap();
    let between = &output[first_pos.min(second_pos)..first_pos.max(second_pos)];
    assert!(
        !between.contains("# TYPE"),
        "unexpected '# TYPE' header between endpoint values:\n{output}"
    );
}

/// `send_snapshot: false` connectors never need an initial snapshot, so
/// they start "delivered".
#[test]
fn send_snapshot_false_starts_delivered() {
    let control = OutputEndpointControl::new(false, /*snapshot_already_sent=*/ false);
    assert!(control.initial_snapshot_sent());
    assert!(!control.is_snapshot_pending());
}

/// `send_snapshot: true` with `snapshot_already_sent: true` (resumed from a
/// checkpoint that records the snapshot was delivered) also starts
/// "delivered".
#[test]
fn send_snapshot_true_with_already_sent_starts_delivered() {
    let control = OutputEndpointControl::new(true, /*snapshot_already_sent=*/ true);
    assert!(control.initial_snapshot_sent());
    assert!(!control.is_snapshot_pending());
}

/// `send_snapshot: true` with `snapshot_already_sent: false` (fresh start)
/// is pending until `mark_snapshot_delivered` flips the flag.
#[test]
fn send_snapshot_true_starts_pending_then_delivered() {
    let control = OutputEndpointControl::new(true, /*snapshot_already_sent=*/ false);
    assert!(!control.initial_snapshot_sent());
    assert!(control.is_snapshot_pending());

    control.mark_snapshot_delivered();
    assert!(control.initial_snapshot_sent());
    assert!(!control.is_snapshot_pending());
}

// Tests for max_queued_records and max_queued_bytes backpressure limits.
mod queue_limit_tests {
    use super::*;
    use crate::ControllerStatus;
    use crate::controller::stats::InputEndpointStatus;
    use feldera_types::config::InputEndpointConfig;
    use std::sync::mpsc;
    use std::thread;
    use uuid::Uuid;

    fn make_endpoint(
        max_queued_records: u64,
        max_queued_bytes: Option<u64>,
    ) -> InputEndpointStatus {
        let mut config = json!({
            "stream": "test_stream",
            "transport": {
                "name": "file_input",
                "config": { "path": "/dev/null" }
            },
            "format": { "name": "csv" },
            "max_queued_records": max_queued_records,
        });
        if let Some(bytes) = max_queued_bytes {
            config["max_queued_bytes"] = json!(bytes);
        }
        let config: InputEndpointConfig = serde_json::from_value(config).unwrap();
        InputEndpointStatus::new("test_endpoint", config, None, None, None)
    }

    fn make_status_with_endpoint(
        max_queued_records: u64,
        max_queued_bytes: Option<u64>,
    ) -> (ControllerStatus, u64) {
        let pipeline_config = serde_json::from_value(json!({
            "name": "test",
            "workers": 1,
        }))
        .unwrap();
        let status = ControllerStatus::new(pipeline_config, 0, None, Uuid::nil());
        let endpoint = make_endpoint(max_queued_records, max_queued_bytes);
        let endpoint_id: u64 = 0;
        status.inputs.write().insert(endpoint_id, endpoint);
        (status, endpoint_id)
    }

    // --- ConnectorConfig::max_queued_bytes() ---

    /// When `max_queued_size` is unset, it defaults to `max_queued_records * 1000`.
    #[test]
    fn max_queued_bytes_defaults_to_1000x_records() {
        let endpoint = make_endpoint(100, None);
        assert_eq!(endpoint.config.connector_config.max_queued_bytes(), 100_000);
    }

    /// When `max_queued_size` is explicitly set, that value is used directly.
    #[test]
    fn max_queued_bytes_explicit_value() {
        let endpoint = make_endpoint(100, Some(50_000));
        assert_eq!(endpoint.config.connector_config.max_queued_bytes(), 50_000);
    }

    /// Saturation: very large `max_queued_records` must not overflow the default.
    #[test]
    fn max_queued_bytes_default_saturates() {
        let endpoint = make_endpoint(u64::MAX / 500, None);
        assert_eq!(
            endpoint.config.connector_config.max_queued_bytes(),
            u64::MAX
        );
    }

    // --- InputEndpointStatus::is_full() for records ---

    /// Below the records threshold the endpoint is not full.
    #[test]
    fn is_full_records_below_threshold() {
        // Use a very high bytes limit so only the records limit matters.
        let endpoint = make_endpoint(10, Some(u64::MAX));
        endpoint
            .metrics
            .buffered_records
            .store(9, Ordering::Relaxed);
        assert!(!endpoint.is_full());
    }

    /// At exactly the records threshold the endpoint is full.
    #[test]
    fn is_full_records_at_threshold() {
        let endpoint = make_endpoint(10, Some(u64::MAX));
        endpoint
            .metrics
            .buffered_records
            .store(10, Ordering::Relaxed);
        assert!(endpoint.is_full());
    }

    /// Above the records threshold the endpoint remains full.
    #[test]
    fn is_full_records_above_threshold() {
        let endpoint = make_endpoint(10, Some(u64::MAX));
        endpoint
            .metrics
            .buffered_records
            .store(11, Ordering::Relaxed);
        assert!(endpoint.is_full());
    }

    // --- InputEndpointStatus::is_full() for bytes ---

    /// Below the bytes threshold the endpoint is not full.
    #[test]
    fn is_full_bytes_below_threshold() {
        // Use a very high records limit so only the bytes limit matters.
        let endpoint = make_endpoint(u64::MAX, Some(500));
        endpoint
            .metrics
            .buffered_bytes
            .store(499, Ordering::Relaxed);
        assert!(!endpoint.is_full());
    }

    /// At exactly the bytes threshold the endpoint is full.
    #[test]
    fn is_full_bytes_at_threshold() {
        let endpoint = make_endpoint(u64::MAX, Some(500));
        endpoint
            .metrics
            .buffered_bytes
            .store(500, Ordering::Relaxed);
        assert!(endpoint.is_full());
    }

    /// Exceeding the bytes limit makes the endpoint full even when the
    /// records count is far below `max_queued_records`.
    #[test]
    fn is_full_bytes_triggers_independently_of_records() {
        let endpoint = make_endpoint(1_000_000, Some(100));
        endpoint
            .metrics
            .buffered_records
            .store(1, Ordering::Relaxed);
        endpoint
            .metrics
            .buffered_bytes
            .store(100, Ordering::Relaxed);
        assert!(endpoint.is_full());
    }

    /// With no explicit `max_queued_size`, the default byte threshold
    /// (`max_queued_records * 1000`) is used for the `is_full` check.
    #[test]
    fn is_full_uses_default_bytes_threshold() {
        // max_queued_records=10 → default byte threshold = 10_000.
        let endpoint = make_endpoint(10, None);
        endpoint
            .metrics
            .buffered_records
            .store(1, Ordering::Relaxed);
        endpoint
            .metrics
            .buffered_bytes
            .store(9_999, Ordering::Relaxed);
        assert!(!endpoint.is_full());

        endpoint
            .metrics
            .buffered_bytes
            .store(10_000, Ordering::Relaxed);
        assert!(endpoint.is_full());
    }

    // --- ControllerStatus::input_batch_from_endpoint backpressure signaling ---

    /// The backpressure thread is unparked when a batch causes the buffered
    /// record count to cross `max_queued_records`.
    #[test]
    fn backpressure_unparked_when_records_threshold_crossed() {
        let (status, endpoint_id) = make_status_with_endpoint(10, Some(u64::MAX));

        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        let (tx, rx) = mpsc::channel::<()>();

        // Park a thread so we can observe the unpark signal.
        thread::spawn(move || {
            parker.park();
            let _ = tx.send(());
        });

        // Add exactly the threshold number of records in one batch.
        status.input_batch_from_endpoint(
            endpoint_id,
            BufferSize {
                records: 10,
                bytes: 1,
            },
            &unparker,
        );

        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_ok(),
            "backpressure thread was not unparked when records threshold was crossed"
        );
    }

    /// The backpressure thread is unparked when a batch causes the buffered
    /// byte count to cross `max_queued_size`, even when the records count
    /// remains well below `max_queued_records`.
    #[test]
    fn backpressure_unparked_when_bytes_threshold_crossed() {
        let (status, endpoint_id) = make_status_with_endpoint(1_000_000, Some(500));

        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        let (tx, rx) = mpsc::channel::<()>();

        thread::spawn(move || {
            parker.park();
            let _ = tx.send(());
        });

        // One record + exactly the byte threshold; records stays far below limit.
        status.input_batch_from_endpoint(
            endpoint_id,
            BufferSize {
                records: 1,
                bytes: 500,
            },
            &unparker,
        );

        assert!(
            rx.recv_timeout(Duration::from_millis(100)).is_ok(),
            "backpressure thread was not unparked when bytes threshold was crossed"
        );
    }

    /// The backpressure thread is unparked only when a threshold is *crossed*,
    /// not on every subsequent batch once already above the threshold.
    #[test]
    fn backpressure_unparked_only_on_threshold_crossing() {
        let (status, endpoint_id) = make_status_with_endpoint(10, Some(u64::MAX));

        // First batch: crosses the records threshold → unpark.
        let parker1 = Parker::new();
        let unparker1 = parker1.unparker().clone();
        let (tx1, rx1) = mpsc::channel::<()>();
        thread::spawn(move || {
            parker1.park();
            let _ = tx1.send(());
        });
        status.input_batch_from_endpoint(
            endpoint_id,
            BufferSize {
                records: 10,
                bytes: 0,
            },
            &unparker1,
        );
        assert!(
            rx1.recv_timeout(Duration::from_millis(100)).is_ok(),
            "first batch crossing threshold should unpark"
        );

        // Second batch: already above threshold, no new crossing → no unpark.
        let parker2 = Parker::new();
        let unparker2 = parker2.unparker().clone();
        let (tx2, rx2) = mpsc::channel::<()>();
        thread::spawn(move || {
            parker2.park();
            let _ = tx2.send(());
        });
        status.input_batch_from_endpoint(
            endpoint_id,
            BufferSize {
                records: 5,
                bytes: 0,
            },
            &unparker2,
        );
        assert!(
            rx2.recv_timeout(Duration::from_millis(50)).is_err(),
            "second batch above threshold should not unpark again"
        );
    }
}

/// A command whose reply channel closes without a reply must report that the
/// controller is gone, not panic.
///
/// [`Command::flush`] answers every command the circuit thread can still see when
/// it exits, but a command that arrives while the thread is already tearing down
/// is never seen, and neither is anything queued when the thread unwinds on a
/// panic: those commands are dropped unanswered. `/suspend` awaits its reply
/// inside a spawned task whose `JoinHandle` is discarded, so panicking there is
/// silent and leaves the pipeline with no outcome recorded at all.
#[test]
fn a_dropped_reply_or_controller_exit_reports_controller_exit() {
    use crate::controller::{Command, ControllerError, reply_or_controller_exit};
    use std::sync::Arc;

    type SuspendReply = oneshot::Receiver<Result<(), Arc<ControllerError>>>;
    fn suspend_command() -> (Command, SuspendReply) {
        let (sender, receiver) = oneshot::channel();
        let command = Command::Suspend(Box::new(move |result| {
            let _ = sender.send(result);
        }));
        (command, receiver)
    }

    // Flushed: the caller is told the controller exited.
    let (command, receiver) = suspend_command();
    command.flush();
    let error =
        reply_or_controller_exit(receiver.blocking_recv()).expect_err("a flush reports an error");
    assert!(matches!(*error, ControllerError::ControllerExit));

    // Dropped unanswered: the caller must be told the same thing.
    let (command, receiver) = suspend_command();
    drop(command);
    let error = reply_or_controller_exit(receiver.blocking_recv())
        .expect_err("a dropped reply reports an error");
    assert!(matches!(*error, ControllerError::ControllerExit));
}

/// An output endpoint that is still owed output must not report the pipeline's
/// progress before it has delivered that output.
///
/// `total_processed_input_records` promises that the endpoint's output equals the
/// circuit's output after that many input records. Seeding it from the restored
/// global counter breaks the promise for an endpoint whose relation the bootstrap
/// re-emits: the counter reaches its target while the re-emitted batch is still
/// queued, so a reader concludes the sink is up to date before anything reaches it.
#[test]
fn test_output_progress_counter_waits_for_owed_output() {
    use super::stats::ProcessedRecords;
    use crate::{ControllerStatus, OutputEndpointConfig};
    use uuid::Uuid;

    // Records the pipeline had processed when the checkpoint was taken.
    const RESTORED_RECORDS: u64 = 3;

    let config = serde_json::from_value(json!({
        "name": "test_output_progress_counter",
        "workers": 1,
    }))
    .unwrap();
    let status = ControllerStatus::new(config, RESTORED_RECORDS, None, Uuid::nil());

    let output_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "v1",
        "transport": { "name": "http_output", "config": {} },
        "format": { "name": "json", "config": {} }
    }))
    .unwrap();

    let processed = |endpoint_id| {
        status
            .output_status()
            .get(&endpoint_id)
            .unwrap()
            .metrics
            .total_processed_input_records
            .load(Ordering::Acquire)
    };

    // A caught-up endpoint handles only future output, so it adopts the pipeline's
    // progress right away.
    status.add_output(&0, "caught_up", &output_config, None, true);
    assert_eq!(processed(0), RESTORED_RECORDS);

    // An endpoint still owed output starts from zero.
    status.add_output(&1, "owed_output", &output_config, None, false);
    assert_eq!(processed(1), 0);

    // Starting behind must not drag `total_completed_records` backwards. The
    // checkpoint path blocks until that counter reaches the records processed when
    // the checkpoint started, so a regression here would stall checkpoints rather
    // than merely misreport progress.
    status.update_total_completed_records();
    assert_eq!(status.num_total_completed_records(), RESTORED_RECORDS);

    // Queueing the owed output does not count as delivering it.
    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    status.enqueue_batch(1, 2);
    assert_eq!(processed(1), 0);

    // Processing the batch that carries the owed output brings the endpoint up to
    // the pipeline's progress.
    status.output_batch(
        1,
        Some(ProcessedRecords {
            total_processed_input_records: RESTORED_RECORDS,
            total_processed_steps: 1,
        }),
        2,
        &unparker,
    );
    assert_eq!(processed(1), RESTORED_RECORDS);
}

/// Dropping an output endpoint that is behind must republish the pipeline's
/// completion counters.
///
/// `total_completed_records` and `total_completed_steps` are the minimum over the
/// registered output endpoints, so an endpoint that has not delivered a step holds
/// both back. An abandoned `/egress` stream is disconnected with its queue still
/// unread, which means the endpoint disappears without ever reaching that step. If
/// removal does not recompute the minimum, nothing else does until the next step,
/// and an idle pipeline runs no further step: `/completion_status` then reports
/// `inprogress` forever and a checkpoint waiting on `total_completed_records`
/// never unblocks.
#[test]
fn test_removing_a_lagging_output_endpoint_republishes_completion() {
    use super::stats::ProcessedRecords;
    use crate::{ControllerStatus, OutputEndpointConfig};
    use uuid::Uuid;

    // Records the circuit processes in the one step of this test.
    const STEP_RECORDS: u64 = 10;

    let config = serde_json::from_value(json!({
        "name": "test_remove_lagging_output",
        "workers": 1,
    }))
    .unwrap();
    let status = ControllerStatus::new(config, 0, None, Uuid::nil());

    let output_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "v1",
        "transport": { "name": "http_output", "config": {} },
        "format": { "name": "json", "config": {} }
    }))
    .unwrap();

    // Two `/egress` streams of the same view: one client reads, the other walks
    // away.
    status.add_output(&0, "reader", &output_config, None, true);
    status.add_output(&1, "abandoned", &output_config, None, true);

    // The circuit initiates and evaluates one step, and its output is queued for
    // both endpoints.
    status
        .global_metrics
        .total_initiated_steps
        .store(1, Ordering::Release);
    status.processed_data(BufferSize {
        records: STEP_RECORDS as usize,
        bytes: 0,
    });
    status.set_committed(ProcessedRecords {
        total_processed_input_records: STEP_RECORDS,
        total_processed_steps: 1,
    });
    status.enqueue_batch(0, STEP_RECORDS as usize);
    status.enqueue_batch(1, STEP_RECORDS as usize);

    // Only `reader` transmits the batch.
    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    let step_processed = ProcessedRecords {
        total_processed_input_records: STEP_RECORDS,
        total_processed_steps: 1,
    };
    status.output_batch(0, Some(step_processed), STEP_RECORDS as usize, &unparker);

    assert_eq!(
        status.global_metrics.total_completed_steps(),
        0,
        "the step is not complete while `abandoned` still owes its output"
    );
    assert_eq!(status.num_total_completed_records(), 0);

    // The abandoned client disconnects, so the endpoint goes away with the batch
    // still queued. The step is now complete as far as any endpoint is concerned.
    status.remove_output(&1);

    assert_eq!(
        status.global_metrics.total_completed_steps(),
        1,
        "removing the endpoint that owed the output must complete the step"
    );
    assert_eq!(
        status.num_total_completed_records(),
        STEP_RECORDS,
        "removing the endpoint that owed the output must complete its records"
    );
}

/// A bare [`ControllerStatus`] for the output-endpoint accounting tests.
fn test_controller_status(name: &str) -> crate::ControllerStatus {
    crate::ControllerStatus::new(
        serde_json::from_value(json!({ "name": name, "workers": 1 })).unwrap(),
        0,
        None,
        uuid::Uuid::nil(),
    )
}

/// A file sink writing CSV, which transmits one record per tuple.
fn test_output_config() -> OutputEndpointConfig {
    serde_json::from_value(json!({
        "stream": "v1",
        "transport": { "name": "file_output", "config": { "path": "/dev/null" } },
        "format": { "name": "csv", "config": {} }
    }))
    .unwrap()
}

/// An output endpoint must report what it had transmitted at the moment it
/// caught up with a checkpoint: not less, because the endpoint was still
/// writing when the checkpoint started, and not more, because it keeps working
/// while the checkpoint commits.
///
/// The checkpoint stores this as the endpoint's `transmitted_records`, so
/// getting it wrong makes a resumed connector misreport for the rest of the
/// pipeline's life. Adding up the live counters cannot produce it: a batch
/// stays queued until the endpoint has finished writing it, by which time
/// `transmitted_records` already covers what went out, so the two overlap.
#[test]
fn test_output_endpoint_reports_totals_at_the_checkpoint() {
    use super::stats::ProcessedRecords;

    // Two steps of output that the circuit queued before the endpoint thread
    // got to either. These are the batch sizes from the `suspend_barrier4` CI
    // failure that exposed the double count.
    const FIRST_BATCH: usize = 630;
    const SECOND_BATCH: usize = 370;
    const TOTAL: u64 = (FIRST_BATCH + SECOND_BATCH) as u64;

    let status = test_controller_status("test_checkpoint_output_stats");
    let output_config = test_output_config();
    status.add_output(&0, "out", &output_config, None, true);

    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    let processed = |records: u64, steps| ProcessedRecords {
        total_processed_input_records: records,
        total_processed_steps: steps,
    };
    let transmitted = || {
        status
            .output_status()
            .get(&0)
            .unwrap()
            .transmitted_records()
    };
    let reported = || {
        status
            .take_output_checkpoint_totals()
            .remove("out")
            .map(|totals| totals.records)
    };

    // The circuit queues two steps of output, then takes a checkpoint covering
    // every record it has ingested.
    status.enqueue_batch(0, FIRST_BATCH);
    status.enqueue_batch(0, SECOND_BATCH);
    status.arm_output_checkpoint(TOTAL);

    // The endpoint writes the first batch to the transport. That write does not
    // take the batch off the queue; the endpoint thread does that once it is
    // done with the whole batch. The endpoint owes the checkpoint the rest, so
    // it has nothing to report yet.
    status.output_buffer(0, 4096, FIRST_BATCH);
    status.output_batch(
        0,
        Some(processed(FIRST_BATCH as u64, 1)),
        FIRST_BATCH,
        &unparker,
    );

    // The second batch takes it to the checkpoint.
    status.output_buffer(0, 4096, SECOND_BATCH);
    status.output_batch(0, Some(processed(TOTAL, 2)), SECOND_BATCH, &unparker);
    assert_eq!(transmitted(), TOTAL);

    // The circuit keeps running while the checkpoint commits. What the endpoint
    // transmits now belongs to the steps after the checkpoint, and the
    // pipeline re-emits it on resume, so the checkpoint must not count it.
    const AFTERWARDS: usize = 500;
    status.enqueue_batch(0, AFTERWARDS);
    status.output_buffer(0, 4096, AFTERWARDS);
    status.output_batch(
        0,
        Some(processed(TOTAL + AFTERWARDS as u64, 3)),
        AFTERWARDS,
        &unparker,
    );
    assert_eq!(transmitted(), TOTAL + AFTERWARDS as u64);

    assert_eq!(reported(), Some(TOTAL));
    assert_eq!(reported(), None, "collecting the reading disarms the latch");
}

/// An endpoint that is already caught up when the checkpoint starts must
/// report right away.
///
/// Nothing more reaches it on an idle pipeline, so it would never take the
/// reading otherwise, and the checkpoint would wait on it forever.
#[test]
fn test_idle_output_endpoint_reports_at_once() {
    use super::stats::ProcessedRecords;

    const RECORDS: usize = 100;

    let status = test_controller_status("test_checkpoint_idle_endpoint");
    let output_config = test_output_config();
    status.add_output(&0, "out", &output_config, None, true);

    let parker = Parker::new();
    let unparker = parker.unparker().clone();

    status.enqueue_batch(0, RECORDS);
    status.output_buffer(0, 4096, RECORDS);
    status.output_batch(
        0,
        Some(ProcessedRecords {
            total_processed_input_records: RECORDS as u64,
            total_processed_steps: 1,
        }),
        RECORDS,
        &unparker,
    );

    status.arm_output_checkpoint(RECORDS as u64);
    assert_eq!(
        status
            .take_output_checkpoint_totals()
            .remove("out")
            .map(|totals| totals.records),
        Some(RECORDS as u64)
    );
}

/// An endpoint removed before it catches up reports nothing, and the
/// checkpoint keeps the figures it captured when it started.
#[test]
fn test_removed_output_endpoint_reports_nothing() {
    const RECORDS: usize = 100;

    let status = test_controller_status("test_checkpoint_removed_endpoint");
    let output_config = test_output_config();
    status.add_output(&0, "out", &output_config, None, true);

    // The endpoint is still holding the batch when the checkpoint starts.
    status.enqueue_batch(0, RECORDS);
    status.arm_output_checkpoint(RECORDS as u64);

    status.remove_output(&0);
    assert!(status.take_output_checkpoint_totals().is_empty());
}

/// The reading an endpoint reports must be exact however the checkpoint
/// interleaves with the endpoint's own work.
///
/// Each round arms the checkpoint at a different point, so it lands before the
/// endpoint has caught up, at the moment it does, and after. The endpoint
/// transmits one record per record the circuit ingested, so whatever the
/// checkpoint covers is exactly what it must report.
#[test]
fn test_output_endpoint_reports_totals_under_concurrency() {
    use super::stats::ProcessedRecords;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};
    use std::thread;

    const BATCHES: usize = 400;
    // Transmitting a batch in two chunks widens the window in which the
    // endpoint has put part of a batch on the transport but has not yet
    // released it.
    const FIRST_CHUNK: usize = 630;
    const SECOND_CHUNK: usize = 370;
    const BATCH: usize = FIRST_CHUNK + SECOND_CHUNK;
    const ROUNDS: usize = 16;

    for round in 0..ROUNDS {
        let status = Arc::new(test_controller_status("test_checkpoint_concurrency"));
        status.add_output(&0, "out", &test_output_config(), None, true);

        let (sender, receiver) = channel();
        // Stands in for the circuit thread. The thread that produces output is
        // the thread that arms the checkpoint, so no output can appear while a
        // checkpoint is being armed, and the endpoint can never be past what
        // the checkpoint covers.
        let ingested = Arc::new(Mutex::new(0u64));

        let circuit = {
            let status = status.clone();
            let ingested = ingested.clone();
            thread::spawn(move || {
                for _ in 0..BATCHES {
                    let mut ingested = ingested.lock().unwrap();
                    status.enqueue_batch(0, BATCH);
                    *ingested += BATCH as u64;
                    let processed = *ingested;
                    drop(ingested);
                    sender.send(processed).unwrap();
                }
            })
        };

        let endpoint = {
            let status = status.clone();
            thread::spawn(move || {
                let parker = Parker::new();
                let unparker = parker.unparker().clone();
                while let Ok(processed) = receiver.recv() {
                    status.output_buffer(0, 4096, FIRST_CHUNK);
                    thread::yield_now();
                    status.output_buffer(0, 4096, SECOND_CHUNK);
                    status.output_batch(
                        0,
                        Some(ProcessedRecords {
                            total_processed_input_records: processed,
                            total_processed_steps: 0,
                        }),
                        BATCH,
                        &unparker,
                    );
                }
            })
        };

        let arm_after = ((round * BATCHES) / ROUNDS * BATCH) as u64;
        while *ingested.lock().unwrap() < arm_after {
            thread::yield_now();
        }
        let threshold = {
            let ingested = ingested.lock().unwrap();
            status.arm_output_checkpoint(*ingested);
            *ingested
        };

        circuit.join().unwrap();
        endpoint.join().unwrap();

        assert_eq!(
            status
                .take_output_checkpoint_totals()
                .remove("out")
                .map(|totals| totals.records),
            Some(threshold),
            "round {round}: the checkpoint covers {threshold} records"
        );
    }
}

/// Only a changed relation makes an output endpoint fall behind. A connector whose
/// own definition changed still emits nothing for inputs already processed, so it
/// stays caught up and keeps its seeded progress counter.
#[test]
fn test_bootstrapped_output_endpoints_ignores_connector_changes() {
    use super::bootstrapped_output_endpoints;
    use feldera_types::pipeline_diff::{PipelineDiff, ProgramDiff};

    let output_config = |stream: &str| -> OutputEndpointConfig {
        serde_json::from_value(json!({
            "stream": stream,
            "transport": { "name": "http_output", "config": {} },
            "format": { "name": "json", "config": {} }
        }))
        .unwrap()
    };

    let outputs = BTreeMap::from([
        (Cow::from("changed_view"), output_config("v_changed")),
        (Cow::from("changed_connector"), output_config("v_stable")),
        (Cow::from("untouched"), output_config("v_stable")),
    ]);

    let diff = PipelineDiff::new_with_program_diff(
        ProgramDiff::new().with_modified_views(vec!["v_changed".to_string()]),
    )
    .with_modified_output_connectors(vec!["changed_connector".to_string()]);

    assert_eq!(
        bootstrapped_output_endpoints(&outputs, Some(&diff)),
        std::collections::HashSet::from(["changed_view".to_string()])
    );

    // Without a diff nothing is re-emitted, so every endpoint starts caught up.
    assert!(bootstrapped_output_endpoints(&outputs, None).is_empty());
}

/// Removing the last output endpoint while a step is in flight must not report
/// that step complete.
///
/// The completion frontier is the minimum over the output endpoints, so removing
/// the last one leaves whatever ceiling the computation started from. Starting it
/// from the steps the circuit has initiated hands out the step the circuit is
/// still evaluating: a Postgres CDC connector deferring its acknowledgment until
/// the frontier passes that step then acknowledges rows the circuit does not yet
/// hold, and `/completion_status` reports them complete.
#[test]
fn test_removing_last_output_endpoint_mid_step_keeps_the_frontier() {
    use super::stats::ProcessedRecords;

    let status = test_controller_status("test_remove_last_output_mid_step");
    let output_config = test_output_config();
    let parker = Parker::new();
    let unparker = parker.unparker().clone();

    // A `/egress` stream is the only output endpoint of this pipeline.
    status.add_output(&0, "egress", &output_config, None, true);

    // Step 0: initiated, evaluated, and delivered by the endpoint.
    status
        .global_metrics
        .total_initiated_steps
        .store(1, Ordering::Release);
    status.processed_data(BufferSize {
        records: 10,
        bytes: 0,
    });
    status.set_committed(ProcessedRecords {
        total_processed_input_records: 10,
        total_processed_steps: 1,
    });
    status.enqueue_batch(0, 10);
    status.output_batch(
        0,
        Some(ProcessedRecords {
            total_processed_input_records: 10,
            total_processed_steps: 1,
        }),
        10,
        &unparker,
    );
    assert_eq!(status.global_metrics.total_completed_steps(), 1);

    // Step 1 starts. The circuit is still evaluating it, so it has committed
    // neither the step nor its records.
    status
        .global_metrics
        .total_initiated_steps
        .store(2, Ordering::Release);

    // The client closes the stream in the middle of that step.
    status.remove_output(&0);

    assert_eq!(
        status.global_metrics.total_completed_steps(),
        1,
        "step 1 is still in flight, so the completion frontier must stay at 1"
    );
}

/// Removing the last output endpoint during a transaction must not report the
/// transaction's records complete.
///
/// Records ingested inside a transaction are in the circuit but are not committed
/// until it ends, which is why a step taken during one advances
/// `total_processed_records` and leaves the completion frontier where it was.
/// Removal has to respect the same rule.
#[test]
fn test_removing_last_output_endpoint_mid_transaction_keeps_the_frontier() {
    let status = test_controller_status("test_remove_last_output_mid_transaction");
    let output_config = test_output_config();

    status.add_output(&0, "egress", &output_config, None, true);

    // One step ingests 10 records inside an open transaction: processed by the
    // circuit, not committed, so nothing is published.
    status
        .global_metrics
        .total_initiated_steps
        .store(1, Ordering::Release);
    status.processed_data(BufferSize {
        records: 10,
        bytes: 0,
    });
    status.update_total_completed_records();
    assert_eq!(status.num_total_completed_records(), 0);

    // The client closes the stream while the transaction is still open.
    status.remove_output(&0);

    assert_eq!(
        status.num_total_completed_records(),
        0,
        "the transaction has not committed, so its records are not complete"
    );
}

/// The completion watch must never move backwards.
///
/// A caller publishes its count and then announces it, and the two steps are not one operation,
/// so the caller holding the smaller count can announce last. Watchers act on what they read
/// here rather than on the published counter: the Postgres CDC connector records the count at
/// flush time and acknowledges the write once the watch passes it, so a watch reading below the
/// steps that are really complete acknowledges a write one step early.
#[test]
fn test_completion_watch_never_moves_backwards() {
    let status = test_controller_status("test_completion_watch_monotonic");

    status.publish_completed_steps(7);
    assert_eq!(status.completion_notifier.borrow().total_completed_steps, 7);

    // What the racing caller with the smaller count announces.
    status.publish_completed_steps(5);
    assert_eq!(
        status.completion_notifier.borrow().total_completed_steps,
        7,
        "a smaller count announced last must not lower the watch"
    );
}

/// A caught-up output endpoint must not hold the step frontier at the count it was added at.
///
/// `/egress` adds an endpoint to a running pipeline, and such an endpoint only receives output
/// produced from then on, so it owes nothing for the steps before it. Claiming zero steps for it
/// makes the minimum over the endpoints zero until it delivers its first batch, which leaves the
/// frontier where it was even once every other endpoint has delivered the steps in between.
#[test]
fn test_caught_up_output_endpoint_does_not_hold_back_the_frontier() {
    use super::stats::ProcessedRecords;

    let status = test_controller_status("test_caught_up_output_seeding");
    let output_config = test_output_config();
    let parker = Parker::new();
    let unparker = parker.unparker().clone();

    let step = ProcessedRecords {
        total_processed_input_records: 10,
        total_processed_steps: 1,
    };

    // A configured connector that is slow to deliver.
    status.add_output(&0, "slow", &output_config, None, true);

    // The circuit commits one step, which the slow connector has not delivered.
    status
        .global_metrics
        .total_initiated_steps
        .store(1, Ordering::Release);
    status.processed_data(BufferSize {
        records: 10,
        bytes: 0,
    });
    status.set_committed(step);
    status.enqueue_batch(0, 10);
    status.update_total_completed_records();
    assert_eq!(status.global_metrics.total_completed_steps(), 0);

    // Someone opens an `/egress` stream on the same pipeline.
    status.add_output(&1, "egress", &output_config, None, true);

    // The slow connector delivers the step it owed.
    status.output_batch(0, Some(step), 10, &unparker);

    assert_eq!(
        status.global_metrics.total_completed_steps(),
        1,
        "the step is complete: the endpoint that owed it delivered, and the stream opened later"
    );
}

/// What the circuit has committed is neither what it has processed nor the completion frontier.
///
/// An output connector that owes nothing claims this pair, and so does a connector receiving an
/// initial snapshot mid-transaction. Reading it from `total_processed_records` would claim records
/// an open transaction has not committed; reading it from `total_completed_steps` would claim
/// fewer steps whenever another connector is behind.
#[test]
fn test_committed_is_neither_the_processed_count_nor_the_frontier() {
    use super::stats::ProcessedRecords;

    let status = test_controller_status("test_committed_pair");
    let output_config = test_output_config();

    // One step commits 10 records, and a connector that never delivers holds the frontier at 0.
    status.add_output(&0, "lagging", &output_config, None, true);
    status
        .global_metrics
        .total_initiated_steps
        .store(1, Ordering::Release);
    status.processed_data(BufferSize {
        records: 10,
        bytes: 0,
    });
    status.set_committed(ProcessedRecords {
        total_processed_input_records: 10,
        total_processed_steps: 1,
    });
    status.enqueue_batch(0, 10);
    status.update_total_completed_records();
    assert_eq!(status.global_metrics.total_completed_steps(), 0);

    // A second step ingests 5 more records inside an open transaction: processed by the circuit,
    // not committed, so nothing is published.
    status
        .global_metrics
        .total_initiated_steps
        .store(2, Ordering::Release);
    status.processed_data(BufferSize {
        records: 5,
        bytes: 0,
    });
    assert_eq!(status.num_total_processed_records(), 15);

    let committed = status.committed();
    assert_eq!(
        committed.total_processed_input_records, 10,
        "the transaction's records are processed but not committed"
    );
    assert_eq!(
        committed.total_processed_steps, 1,
        "the lagging connector holds the frontier, not what the circuit committed"
    );
}

/// An input reader asks which step it is feeding while handling a `Queue`
/// command, and the controller answers from the count of steps it has
/// initiated. A connector that defers acknowledgment stamps its writes with
/// that step, so an off-by-one here would acknowledge rows against a
/// checkpoint that predates them.
#[test]
fn the_step_a_reader_feeds_is_one_below_the_count_initiated() {
    use super::current_step_of;

    // `CircuitThread::step` stores the count before it collects input, so
    // while the reader feeds step 0 the count already reads 1.
    assert_eq!(current_step_of(1), Some(0));
    assert_eq!(current_step_of(42), Some(41));
    // No step has started, so there is none to feed. A reader never asks
    // then, and saying `Some(0)` would claim a step that has not begun.
    assert_eq!(current_step_of(0), None);
}
