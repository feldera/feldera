//! Controller tests of the static engine itself: a full disk, a checkpoint of a circuit
//! without persistent ids, and the LIR dump. The shared suites in `dbsp_adapters` cover the
//! controller with any engine.

use std::fs::{File, create_dir};
use std::io::Write;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use dbsp_adapters::PipelineConfig;
use feldera_types::error::DetailedError;
use serde_json::json;
use tempfile::TempDir;
use tracing::info;

use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::{StaticTestEngine, TestStruct, default_timeout_ms, init_test_logger, wait};

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
    let controller = StaticTestEngine::new()
        .controller(
            &TestCircuit::identity(TestStruct::relation_schema()),
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
    let circuit = TestCircuit::Identity {
        relation: TestStruct::relation_schema(),
        persistent_output_ids: vec![None],
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: false,
        table_version: 0,
        attribute_defaults: Vec::new(),
    };
    let controller = Arc::new(
        StaticTestEngine::new()
            .controller(&circuit, &config, Box::new(|e, _| info!("error: {e}")))
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
    assert!(controller.shutdown_token().is_cancelled());

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

    let circuit = TestCircuit::Identity {
        relation: TestStruct::relation_schema(),
        persistent_output_ids: vec![Some("output".to_string())],
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
        table_version: 0,
        attribute_defaults: Vec::new(),
    };
    let controller = StaticTestEngine::new()
        .controller(&circuit, &config, Box::new(|e, _| panic!("error: {e}")))
        .unwrap();

    let lir = controller.lir().unwrap().as_json().clone();
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
