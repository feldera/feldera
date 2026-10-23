//! The controller: pipelines of the identity and aliased-index circuits; connectors that
//! start after others, fail to open, or join and leave a running pipeline; the controller's
//! own bookkeeping of connector transactions, input probes, and output endpoints; and the
//! helpers that the other controller tests share.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{File, create_dir};
use std::io::Write;
use std::iter::repeat_n;
use std::ops::Range;
use std::panic::resume_unwind;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arrow::array::{Array, Int64Array};
use chrono::Utc;
use crossbeam::sync::Parker;
use feldera_adapterlib::format::{BufferSize, InputBuffer, Parser};
use feldera_adapterlib::test::{Updates, default_timeout_ms, wait};
use feldera_types::adapter_stats::ExternalOutputEndpointMetrics;
use feldera_types::config::{FtModel, InputEndpointConfig, OutputEndpointConfig, PipelineConfig};
use feldera_types::error::DetailedError;
use feldera_types::program_schema::{Relation, SqlIdentifier};
use proptest::prelude::*;
use serde_json::json;
use tempfile::{NamedTempFile, TempDir};
use tracing::info;

use super::{OutputEndpointDescr, TransactionState};
use crate::test::pipeline::{PipelineTestEngine, TestCircuit, query};
use crate::test::{init_test_logger, published_schema, read, relations, render, rows};
use crate::transport::{
    InputQueue, InputQueueEntry, InputReader, InputReaderCommand,
    input_transport_config_to_endpoint, set_barrier,
};
use crate::{Controller, InputConsumer, InputEndpoint, OutputEndpoint, TransportInputEndpoint};

/// The table and view of the cases.
pub fn relation() -> Relation {
    relations::basic("controller_rows")
}

/// The key of [`relation`], for the cases of the aliased-index circuit.
pub fn key_relation() -> Relation {
    relations::basic_key("controller_key")
}

/// Returns the identity circuit over [`relation`]: one table and one view per entry of
/// `persistent_output_ids`, each view with that persistent id.
pub(crate) fn identity(persistent_output_ids: &[Option<&str>]) -> TestCircuit {
    TestCircuit::Identity {
        relation: relation(),
        persistent_output_ids: persistent_output_ids
            .iter()
            .map(|id| id.map(str::to_string))
            .collect(),
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
    }
}

/// Returns the CSV record of the row of [`relation`] with `id`: `b` false, `i` NULL, and `s`
/// empty.
pub(crate) fn for_id(id: usize) -> (usize, bool, Option<i64>, &'static str) {
    (id, false, None, "")
}

/// Returns the rows of [`relation`] with `ids`, as [`for_id`] writes them, each inserted
/// once.
pub(crate) fn for_ids(ids: impl IntoIterator<Item = usize>) -> Updates {
    let rows: Vec<String> = ids
        .into_iter()
        .map(|id| json!({"id": id, "b": false, "i": null, "s": ""}).to_string())
        .collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    Updates::from_json(published_schema(&relation()), &rows).unwrap()
}

/// Reads `path` and ensures that it contains the rows of [`for_ids`] with the ids in
/// `expect`.
#[track_caller]
pub(crate) fn check_file_contents(path: &Path, expect: Range<usize>) {
    check_file_ids(path, expect);
}

/// Reads `path` and ensures that it contains exactly the rows of [`for_ids`] with the ids in
/// `expect`, which need not be contiguous, each with weight 1.
#[track_caller]
pub(crate) fn check_file_ids(path: &Path, expect: impl IntoIterator<Item = usize>) {
    let actual = read::csv_updates(&relation(), &std::fs::read(path).unwrap()).unwrap();
    assert!(actual.weights().iter().all(|weight| *weight == 1));
    let expect = for_ids(expect);
    assert_eq!(actual.len(), expect.len());
    assert_eq!(actual.zset().unwrap(), expect.zset().unwrap());
}

pub(crate) fn count_endpoint_records(controller: &Controller, output_index: usize) -> u64 {
    let endpoint_id = controller
        .inner
        .output_endpoint_id_by_name(&format!("test_output{}", output_index + 1))
        .unwrap();
    controller
        .status()
        .output_status()
        .get(&endpoint_id)
        .unwrap()
        .transmitted_records()
}

pub(crate) fn collect_endpoint_records(controller: &Controller, n: usize) -> Vec<usize> {
    (0..n)
        .map(|i| count_endpoint_records(controller, i) as usize)
        .collect()
}

/// Default budget for [`wait_for_records`] to observe the expected output.
pub(crate) const OUTPUT_TIMEOUT_MS: u128 = 10_000;

/// Longer output budget for waits taken while a suspend is in progress.
///
/// While a suspend is pending, the controller advances only the barrier inputs
/// while re-attempting the checkpoint each step, so under CI CPU contention
/// output can lag far behind input even though stepping keeps making forward
/// progress. Using the default [`OUTPUT_TIMEOUT_MS`] in that window makes the
/// suspend tests flake. Matches the 100s budget already used for the
/// suspend-completion `recv_timeout` in these tests.
pub(crate) const SUSPEND_OUTPUT_TIMEOUT_MS: u128 = 100_000;

/// Like `println!`, but prefixes a UTC timestamp so test output interleaves
/// legibly with the controller's tracing logs when diagnosing timing issues.
macro_rules! tprintln {
    ($($arg:tt)*) => {
        println!("[{}] {}", chrono::Utc::now().format("%H:%M:%S%.6f"), format_args!($($arg)*))
    };
}
pub(crate) use tprintln;

/// Wait until every output endpoint has received at least the expected number
/// of records, then verify the counts match exactly, using the default
/// [`OUTPUT_TIMEOUT_MS`] budget.
#[track_caller]
pub(crate) fn wait_for_records(controller: &Controller, expect_n: &[usize]) {
    wait_for_records_within(controller, expect_n, OUTPUT_TIMEOUT_MS);
}

/// Like [`wait_for_records`], but waits up to `timeout_ms` for the records to
/// arrive.
#[track_caller]
pub(crate) fn wait_for_records_within(
    controller: &Controller,
    expect_n: &[usize],
    timeout_ms: u128,
) {
    tprintln!("waiting for {expect_n:?} records...");
    let n = expect_n.len();
    let mut last_n = repeat_n(0, n).collect::<Vec<_>>();
    wait(
        || {
            let new_n = collect_endpoint_records(controller, n);
            for i in 0..n {
                if new_n[i] > last_n[i] {
                    tprintln!("received {} records on test_output{}", new_n[i], i + 1);
                }
            }
            last_n = new_n;
            last_n
                .iter()
                .zip(expect_n.iter())
                .all(|(&last, &expect)| last >= expect)
        },
        timeout_ms,
    )
    .unwrap();

    // No more records should arrive, but give the controller some time
    // to send some more in case there's a bug.
    sleep(Duration::from_millis(100));

    // Then verify that the number is as expected.
    assert_eq!(&collect_endpoint_records(controller, n), expect_n);
}

/// Appends the records with ids in `ids` to the input file.
/// Appends the CSV records of the rows with `ids` to the file at `input_path`.
pub(crate) fn append_input(input_path: &Path, ids: Range<usize>) {
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(File::options().append(true).open(input_path).unwrap());
    for id in ids {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();
}

/// Returns the metrics of output endpoint `endpoint_name`.
pub(crate) fn output_endpoint_metrics(
    controller: &Controller,
    endpoint_name: &str,
) -> ExternalOutputEndpointMetrics {
    let endpoint_id = controller
        .inner
        .output_endpoint_id_by_name(endpoint_name)
        .unwrap();
    controller
        .status()
        .output_status()
        .get(&endpoint_id)
        .unwrap()
        .metrics
        .to_api_type()
}

/// An output endpoint that blocks in `push_buffer` until the test releases it,
/// standing in for a sink that has stopped accepting data.
pub(crate) struct BlockedOutputEndpoint {
    /// Set once the endpoint is blocked inside `push_buffer`, so that the test
    /// can wait for the connector to be genuinely stuck rather than guess.
    pub(crate) blocked: Arc<AtomicBool>,

    /// Cleared by the test to let the endpoint (and thus the pipeline
    /// shutdown) proceed.
    pub(crate) hold: Arc<AtomicBool>,
}

impl OutputEndpoint for BlockedOutputEndpoint {
    fn connect(
        &mut self,
        _async_error_callback: Box<dyn Fn(bool, anyhow::Error, Option<&'static str>) + Send + Sync>,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        usize::MAX
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> Result<(), anyhow::Error> {
        self.blocked.store(true, Ordering::Release);
        while self.hold.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> Result<(), anyhow::Error> {
        unreachable!("the test uses a format that pushes buffers, not keys")
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

/// Random rows, written as CSV to a file that a secret names, reach the output file that
/// another secret names, whatever the batch sizes and buffering of the pipeline.
pub fn proptest_csv_file(engine: &dyn PipelineTestEngine) {
    let relation = relation();
    let cases = (
        rows::batches(&relation, "id", 1..=1, 5000),
        1..100usize,
        1..2000usize,
        1..1000usize,
        1..100usize,
    );
    proptest!(ProptestConfig::with_cases(30), |((
        batches,
        min_batch_size_records,
        max_buffering_delay_usecs,
        input_buffer_size_bytes,
        output_buffer_size_records,
    ) in cases)| {
        let rows = &batches[0];
        let input_file = NamedTempFile::new().unwrap();
        let output_path = NamedTempFile::new().unwrap().into_temp_path();
        let output = output_path.to_str().unwrap().to_string();
        output_path.close().unwrap();

        let secrets_dir = TempDir::new().unwrap();
        create_dir(secrets_dir.path().join("kubernetes")).unwrap();
        create_dir(secrets_dir.path().join("kubernetes/paths")).unwrap();
        std::fs::write(
            secrets_dir.path().join("kubernetes/paths/input"),
            input_file.path().as_os_str().as_encoded_bytes(),
        )
        .unwrap();
        std::fs::write(secrets_dir.path().join("kubernetes/paths/output"), &output).unwrap();

        let config: PipelineConfig = serde_json::from_value(json!({
            "secrets_dir": secrets_dir.path(),
            "min_batch_size_records": min_batch_size_records,
            "max_buffering_delay_usecs": max_buffering_delay_usecs,
            "name": "test",
            "workers": 4,
            "inputs": {
                "test_input1": {
                    "stream": "test_input1",
                    "transport": {
                        "name": "file_input",
                        "config": {
                            "path": "${secret:kubernetes:paths/input}",
                            "buffer_size_bytes": input_buffer_size_bytes,
                            "follow": false
                        }
                    },
                    "format": { "name": "csv" }
                }
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": { "path": "${secret:kubernetes:paths/output}" }
                    },
                    "format": {
                        "name": "csv",
                        "config": { "buffer_size_records": output_buffer_size_records }
                    }
                }
            }
        }))
        .unwrap();

        std::fs::write(input_file.path(), render::csv(&relation, rows.rows())).unwrap();
        let controller = engine
            .controller(
                &TestCircuit::identity(relation.clone()),
                &config,
                Box::new(|error, _| panic!("error: {error}")),
            )
            .unwrap();
        controller.start();
        wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();
        prop_assert_eq!(
            controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records(),
            rows.len() as u64
        );
        controller.stop().unwrap();

        let output_rows = read::csv_updates(&relation, &std::fs::read(&output).unwrap()).unwrap();
        prop_assert!(output_rows.weights().iter().all(|weight| *weight == 1));
        prop_assert_eq!(output_rows.zset().unwrap(), rows.zset().unwrap());
        std::fs::remove_file(&output).unwrap();
    });
}

pub fn start_after_cyclic(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1.endpoint1": {
                "stream": "test_input1",
                "labels": [
                    "label1"
                ],
                "start_after": "label2",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": "file1"
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            },
            "test_input1.endpoint2": {
                "stream": "test_input1",
                "labels": [
                    "label2"
                ],
                "start_after": "label1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": "file2"
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            }
        }
    }))
    .unwrap();
    let Err(err) = engine.controller(
        &identity(&[None]),
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    ) else {
        panic!("expected to fail")
    };

    assert_eq!(
        &err.to_string(),
        "invalid controller configuration: cyclic 'start_after' dependency detected: endpoint 'test_input1.endpoint1' with label 'label1' waits for endpoint 'test_input1.endpoint2' with label 'label2', which waits for endpoint 'test_input1.endpoint1' with label 'label1'"
    );
}

pub fn start_after(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    // Two JSON files with a few records each.
    let temp_input_file1 = NamedTempFile::new().unwrap();
    let temp_input_file2 = NamedTempFile::new().unwrap();

    temp_input_file1
        .as_file()
        .write_all(br#"[{"id": 1, "b": true, "s": "foo"}, {"id": 2, "b": true, "s": "foo"}]"#)
        .unwrap();
    temp_input_file2
        .as_file()
        .write_all(br#"[{"id": 3, "b": true, "s": "foo"}, {"id": 4, "b": true, "s": "foo"}]"#)
        .unwrap();

    // Controller configuration with two input connectors;
    // the second starts after the first one finishes.
    let config: PipelineConfig = serde_json::from_value(json! ({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1.endpoint1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "labels": [
                        "backfill"
                    ],
                    "config": {
                        "path": temp_input_file1.path(),
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            },
            "test_input1.endpoint2": {
                "stream": "test_input1",
                "start_after": "backfill",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file2.path(),
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();

    // Wait 3 seconds, assert(no data in the output table)

    // Unpause the first connector.

    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+------+---+-----+
| id | b    | i | s   |
+----+------+---+-----+
| 1  | true |   | foo |
| 2  | true |   | foo |
| 3  | true |   | foo |
| 4  | true |   | foo |
+----+------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// A panic on the circuit thread after initialization must reach the error
/// callback as `ControllerPanic`; otherwise the server never learns that the
/// pipeline died and keeps reporting it as running.
pub fn circuit_thread_panic_is_reported(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "inputs": {}
    }))
    .unwrap();

    let errors = Arc::new(Mutex::new(Vec::new()));
    let errors_clone = errors.clone();
    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(move |e, _| errors_clone.lock().unwrap().push(e.error_code())),
        )
        .unwrap();
    controller.start();

    // The circuit thread runs this callback, so the panic unwinds through it.
    // `resume_unwind` skips the panic hook.  With `RUST_BACKTRACE=1`, the hook
    // resolves a backtrace under a process-wide lock that other panicking
    // tests share, which can delay the report past the timeout below.
    controller.start_graph_profile(Box::new(|_| {
        resume_unwind(Box::new("injected circuit thread panic"))
    }));

    // A short timeout: without the fix the report never arrives.
    wait(|| !errors.lock().unwrap().is_empty(), 10_000)
        .expect("the circuit thread panic was not reported");
    assert_eq!(errors.lock().unwrap().as_slice(), ["ControllerPanic"]);

    let stop_error = controller
        .stop()
        .expect_err("stopping a controller whose circuit thread panicked must fail");
    assert_eq!(stop_error.error_code(), "ControllerPanic");
}

fn multiple_input_files(
    n: usize,
) -> (
    Vec<NamedTempFile>,
    BTreeMap<Cow<'static, str>, InputEndpointConfig>,
) {
    let mut temp_input_files = Vec::new();
    let mut inputs: BTreeMap<Cow<'static, str>, InputEndpointConfig> = BTreeMap::new();
    for i in 0..n {
        let file = NamedTempFile::new().unwrap();
        file.as_file()
            .write_all(&format!(r#"[{{"id": {i}, "b": true, "s": "foo"}}]"#).into_bytes())
            .unwrap();
        let config: InputEndpointConfig = serde_json::from_value(json!({
            "stream": "test_input1",
            "transport": {
                "name": "file_input",
                "config": {
                    "path": file.path(),
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "array": true,
                    "update_format": "raw"
                }
            }
        }))
        .unwrap();
        inputs.insert(format!("test_input1.endpoint{i}").into(), config);
        temp_input_files.push(file);
    }
    (temp_input_files, inputs)
}

/// Starts a pipeline whose 100 file connectors initialize at most
/// `max_parallel_connector_init` at a time, and checks that every record arrives.
fn check_concurrent_init(engine: &dyn PipelineTestEngine, max_parallel_connector_init: u64) {
    init_test_logger();

    let (_temp_input_files, inputs) = multiple_input_files(100);

    // Controller configuration with 100 input connectors.
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "max_parallel_connector_init": max_parallel_connector_init,
        "inputs": inputs,
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();

    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select count(*) from test_output1")
        .unwrap();

    let expected = r#"+----------+
| count(*) |
+----------+
| 100      |
+----------+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

pub fn concurrent_init(engine: &dyn PipelineTestEngine) {
    check_concurrent_init(engine, 1);
    check_concurrent_init(engine, 10);
    check_concurrent_init(engine, 100);
}

pub fn connector_init_error(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let (_temp_input_files, mut connectors) = multiple_input_files(20);
    connectors.insert(
        Cow::from("test_input1.error_endpoint"),
        serde_json::from_value(json!({
            "stream": "test_input1",
            "transport": {
                "name": "file_input",
                "config": {
                    "path": "path_does_not_exist"
                }
            },
            "format": {
                "name": "json",
                "config": {
                    "array": true,
                    "update_format": "raw"
                }
            }
        }))
        .unwrap(),
    );

    // Controller configuration with two input connectors;
    // the second starts after the first one finishes.
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": connectors,
    }))
    .unwrap();

    let result = engine.controller(
        &identity(&[None]),
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    );

    let Err(error) = result else {
        panic!("the pipeline started")
    };
    assert_eq!(
        error.to_string(),
        "FATAL error on input endpoint 'test_input1.error_endpoint': Failed to open input file 'path_does_not_exist': No such file or directory (os error 2)"
    );
}

/// A connector rejected by the fault-tolerance check must leave nothing behind.
///
/// `add_input_endpoint` used to return the error while leaving the endpoint in
/// the status map, where the endpoint could never run yet still reserved its
/// own name against a retry.
pub fn fault_tolerance_mismatch_unregisters_the_endpoint(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let controller = start_exactly_once_controller(engine, &tempdir);
    let input_file = tempdir.path().join("input.csv");
    File::create(&input_file).unwrap();

    // `file_input` reports at-least-once for a path with a barrier, weaker than
    // this pipeline's exactly-once requirement.
    set_barrier(input_file.to_str().unwrap(), 0);

    let connector = json!({
        "name": "file_input",
        "config": {
            "path": input_file,
            "follow": true
        }
    });
    let endpoint = input_transport_config_to_endpoint(
        &serde_json::from_value(connector.clone()).unwrap(),
        "weak_ft",
        tempdir.path(),
        None,
    )
    .unwrap()
    .unwrap();
    let endpoint_config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": connector,
        "format": { "name": "csv" }
    }))
    .unwrap();

    let error = controller
        .add_input_endpoint("weak_ft", endpoint_config, endpoint, None)
        .expect_err("an at-least-once connector must be rejected by an exactly-once pipeline");
    assert!(error.to_string().contains("fault tolerance"), "{error}");

    assert!(
        controller.input_endpoint_id_by_name("weak_ft").is_err(),
        "the rejected endpoint stayed registered"
    );
    assert!(controller.status().input_status().is_empty());

    controller.stop().unwrap();
}

/// Starts an exactly-once pipeline with storage in `tempdir` and waits until it
/// accepts new input endpoints.
fn start_exactly_once_controller(engine: &dyn PipelineTestEngine, tempdir: &TempDir) -> Controller {
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "fault_tolerance": {},
        "clock_resolution_usecs": null,
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    // `add_input_endpoint` refuses to run while the pipeline is still restoring,
    // which on a fault-tolerant pipeline is the state it starts in.
    controller.start();
    wait(|| !controller.is_replaying(), default_timeout_ms()).unwrap();
    controller
}

/// A non-fault-tolerant input endpoint that records whether it was opened.
struct OpenRecordingEndpoint {
    opened: Arc<AtomicBool>,
}

impl InputEndpoint for OpenRecordingEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for OpenRecordingEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        self.opened.store(true, Ordering::Release);
        TransactionalInputEndpoint::new().open(consumer, parser, schema, resume_info)
    }
}

/// A connector rejected by the fault-tolerance check must never be opened.
///
/// Opening a connector can connect to external systems, start threads, and
/// report end of input, so the check must come before `open`, not after it.
pub fn fault_tolerance_mismatch_does_not_open_the_endpoint(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let controller = start_exactly_once_controller(engine, &tempdir);

    let opened = Arc::new(AtomicBool::new(false));
    let endpoint_config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": { "name": "file_input", "config": { "path": "unused" } },
        "format": { "name": "csv" }
    }))
    .unwrap();

    let error = controller
        .add_input_endpoint(
            "weak_ft",
            endpoint_config,
            Box::new(OpenRecordingEndpoint {
                opened: opened.clone(),
            }),
            None,
        )
        .expect_err(
            "a connector without fault tolerance must be rejected by an exactly-once pipeline",
        );
    assert!(error.to_string().contains("fault tolerance"), "{error}");
    assert!(
        !opened.load(Ordering::Acquire),
        "the rejected endpoint was opened"
    );
    assert!(controller.status().input_status().is_empty());

    controller.stop().unwrap();
}

/// An input endpoint whose `open()` blocks until the test releases it.
struct GatedOpenEndpoint {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    inner: TransactionalInputEndpoint,
}

impl InputEndpoint for GatedOpenEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for GatedOpenEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        self.entered.send(()).unwrap();
        self.release.lock().unwrap().recv().unwrap();
        self.inner.open(consumer, parser, schema, resume_info)
    }
}

/// A step that starts while a new endpoint is still in `open()` must not wait
/// for that endpoint.
///
/// The endpoint is visible to the circuit thread before it has a reader, and an
/// endpoint without a reader never gets `queue()`, so it can never report
/// progress.  Waiting for it stalls the pipeline forever.  Once `open()`
/// returns, later steps must consume the endpoint's input.
pub fn step_does_not_wait_for_endpoint_without_reader(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "clock_resolution_usecs": null,
    }))
    .unwrap();
    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let endpoint_config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": { "name": "file_input", "config": { "path": "unused" } },
        "format": { "name": "csv" }
    }))
    .unwrap();
    let endpoint = TransactionalInputEndpoint::new();
    let adder = {
        let controller = controller.clone();
        let endpoint = endpoint.clone();
        thread::spawn(move || {
            controller.add_input_endpoint(
                "gated",
                endpoint_config,
                Box::new(GatedOpenEndpoint {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                    inner: endpoint,
                }),
                None,
            )
        })
    };

    // The endpoint is now visible, without a reader.
    entered_rx.recv().unwrap();
    let steps = controller.status().global_metrics.total_completed_steps();
    controller.request_step();
    let progressed = wait(
        || controller.status().global_metrics.total_completed_steps() > steps,
        10_000,
    )
    .is_ok();

    release_tx.send(()).unwrap();
    adder.join().unwrap().unwrap();
    endpoint.push("1,true,,foo\n");
    let consumed = wait(
        || controller.status().num_total_processed_records() == 1,
        10_000,
    )
    .is_ok();
    controller.stop().unwrap();
    assert!(progressed, "step waited for an endpoint that had no reader");
    assert!(
        consumed,
        "input pushed after open() returned was never processed"
    );
}

/// An input endpoint that opens and commits Feldera transactions, the way a
/// connector configured with a `transaction_mode` does.
///
/// It hands over whatever it has queued in reply to a `Queue` command, like an
/// ordinary connector, so an endpoint that keeps input buffered across steps is
/// one the controller chose not to poll.
#[derive(Clone)]
struct TransactionalInputEndpoint {
    details: Arc<Mutex<Option<TransactionalInputEndpointDetails>>>,
}

struct TransactionalInputEndpointDetails {
    parser: Box<dyn Parser>,
    queue: InputQueue,
}

impl TransactionalInputEndpoint {
    fn new() -> Self {
        Self {
            details: Arc::new(Mutex::new(None)),
        }
    }

    /// Opens the transaction labeled `label`, or joins it if another connector
    /// has already opened it, ahead of the input queued next.
    fn start_transaction(&self, label: &str) {
        self.push_entry(
            InputQueueEntry::new_with_aux(Utc::now(), ())
                .with_start_transaction(Some(Some(label.to_string()))),
        );
    }

    /// Commits this connector's part of the transaction, after the input
    /// queued so far.  The transaction itself commits once every participant
    /// has done this.
    fn commit_transaction(&self) {
        self.push_entry(
            InputQueueEntry::new_with_aux(Utc::now(), ()).with_commit_transaction(true),
        );
    }

    /// Parses `data` and queues it, reporting the records as buffered.
    fn push(&self, data: &str) {
        let mut guard = self.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        let parsed = details.parser.parse(data.as_bytes(), None);
        details.queue.push(parsed, Utc::now());
    }

    fn push_entry(&self, entry: InputQueueEntry<(), Box<dyn InputBuffer>>) {
        self.details
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .queue
            .push_entry(entry, Vec::new());
    }
}

impl InputEndpoint for TransactionalInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for TransactionalInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        *self.details.lock().unwrap() = Some(TransactionalInputEndpointDetails {
            parser,
            queue: InputQueue::new(consumer),
        });
        Ok(Box::new(self.clone()))
    }
}

impl InputReader for TransactionalInputEndpoint {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command {
            InputReaderCommand::Queue { .. } => {
                self.details.lock().unwrap().as_ref().unwrap().queue.queue()
            }
            InputReaderCommand::Extend
            | InputReaderCommand::Pause
            | InputReaderCommand::Disconnect => (),
            InputReaderCommand::Replay { .. } => {
                unreachable!("this endpoint is not fault tolerant")
            }
        }
    }

    fn is_closed(&self) -> bool {
        false
    }
}

/// Input buffered by a connector that has committed the in-progress
/// transaction must not keep the circuit thread stepping.
///
/// Such a connector is not polled until the transaction commits, so that a
/// participant cannot extend forever the transaction it just committed.  Its
/// input therefore stays buffered across every step.  A step trigger that
/// counts it decides to step, takes a step that consumes nothing, finds the
/// same input still buffered, and steps again: the circuit thread spins on
/// empty steps for the rest of the transaction, evaluating the whole circuit
/// each time.
pub fn committed_connector_input_does_not_spin_the_circuit(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    // Step as soon as anything is buffered, with no clock connector and no
    // checkpoint timer to ask for a step of its own.  Any step the test
    // observes is then one the trigger decided to take on buffered input.
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "min_batch_size_records": 0,
        "max_buffering_delay_usecs": 0,
        "clock_resolution_usecs": null,
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None, None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();

    let add_endpoint = |name: &str, stream: &str| {
        let endpoint = TransactionalInputEndpoint::new();
        let endpoint_config: InputEndpointConfig = serde_json::from_value(json!({
            "stream": stream,
            "transport": { "name": "file_input", "config": { "path": "unused" } },
            "format": { "name": "csv" }
        }))
        .unwrap();
        controller
            .add_input_endpoint(name, endpoint_config, Box::new(endpoint.clone()), None)
            .unwrap();
        endpoint
    };
    let committer = add_endpoint("committer", "test_input1");
    let holder = add_endpoint("holder", "test_input2");

    let committed = |name: &str| {
        controller
            .inner
            .transaction_info
            .lock()
            .unwrap()
            .committed_connectors()
            .contains(name)
    };
    let participates = |name: &str| {
        controller
            .inner
            .transaction_info
            .lock()
            .unwrap()
            .initiators
            .initiated_by_connectors
            .contains_key(name)
    };

    // Both connectors open the same transaction. `holder` never commits, so
    // the transaction stays open for the rest of the test.
    holder.start_transaction("t");
    committer.start_transaction("t");
    committer.push("1,true,,foo\n");
    wait(
        || participates("holder") && participates("committer"),
        default_timeout_ms(),
    )
    .expect("the connectors never opened a transaction");

    // `committer` commits its part. From here the controller stops polling it.
    committer.commit_transaction();
    wait(|| committed("committer"), default_timeout_ms())
        .expect("the connector never committed its part of the transaction");

    // Input it buffers now cannot be consumed until the transaction commits.
    committer.push(&"2,true,,foo\n".repeat(100));
    wait(
        || controller.status().num_buffered_input_records() == 100,
        default_timeout_ms(),
    )
    .expect("the committed connector's input was consumed after all");

    // Nothing the next step can consume is buffered, so the circuit thread
    // must stay parked.
    let before = controller.status().global_metrics.total_initiated_steps();
    sleep(Duration::from_secs(1));
    let steps = controller.status().global_metrics.total_initiated_steps() - before;
    assert!(
        steps < 10,
        "the circuit thread took {steps} steps in a second with nothing to consume"
    );

    // The pipeline is parked, not wedged: input on the connector that is still
    // polled gets processed.
    let processed = controller.status().num_total_processed_records();
    holder.push("3,true,,foo\n");
    wait(
        || controller.status().num_total_processed_records() > processed,
        default_timeout_ms(),
    )
    .expect("input on a polled connector was never processed");

    // Committing the transaction releases the backlog.
    holder.commit_transaction();
    wait(
        || controller.status().num_buffered_input_records() == 0,
        default_timeout_ms(),
    )
    .expect("the backlog was never consumed after the transaction committed");

    // The step that drains the backlog is not the one that ends the
    // transaction: the circuit thread clears the state on a later pass, once
    // it sees the commit finished. Waiting for that is not the same as
    // asserting it, which raced the commit under load.
    wait(
        || controller.inner.get_transaction_state() == TransactionState::None,
        default_timeout_ms(),
    )
    .expect("the transaction never finished committing");

    controller.stop().unwrap();
}

/// An input endpoint that hands its input to the circuit when the test says so,
/// rather than in reply to a `Queue` command.
///
/// The controller collects an endpoint's input in two steps: the endpoint
/// reports it with [`InputConsumer::extended`], which parks the result in the
/// endpoint's status entry, and the circuit thread then picks the result up and
/// counts the records as processed.  Releasing the input outside a `Queue`
/// command puts the endpoint between those two steps for as long as the test
/// needs, which is the window that a removed endpoint has to lose input in.
#[derive(Clone)]
struct ManualInputEndpoint {
    details: Arc<Mutex<Option<ManualInputEndpointDetails>>>,
}

struct ManualInputEndpointDetails {
    parser: Box<dyn Parser>,
    queue: InputQueue,
}

impl ManualInputEndpoint {
    fn new() -> Self {
        Self {
            details: Arc::new(Mutex::new(None)),
        }
    }

    /// Parses `data` and queues it, reporting the records as buffered.
    fn push(&self, data: &str) {
        let mut guard = self.details.lock().unwrap();
        let details = guard.as_mut().unwrap();
        let parsed = details.parser.parse(data.as_bytes(), None);
        details.queue.push(parsed, Utc::now());
    }

    /// Flushes the queued records to the circuit and reports them to the
    /// controller.
    fn release(&self) {
        self.details.lock().unwrap().as_ref().unwrap().queue.queue();
    }
}

impl InputEndpoint for ManualInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for ManualInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<serde_json::Value>,
    ) -> anyhow::Result<Box<dyn InputReader>> {
        *self.details.lock().unwrap() = Some(ManualInputEndpointDetails {
            parser,
            queue: InputQueue::new(consumer),
        });
        Ok(Box::new(self.clone()))
    }
}

impl InputReader for ManualInputEndpoint {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        match command {
            // Report no input: the test releases it with
            // `ManualInputEndpoint::release`.
            InputReaderCommand::Queue { .. } => self
                .details
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .queue
                .consumer
                .extended(BufferSize::empty(), None, Vec::new()),
            InputReaderCommand::Extend
            | InputReaderCommand::Pause
            | InputReaderCommand::Disconnect => (),
            InputReaderCommand::Replay { .. } => {
                unreachable!("this endpoint is not fault tolerant")
            }
        }
    }

    fn is_closed(&self) -> bool {
        false
    }
}

/// Removing an input endpoint must not take the count of the input it already
/// handed to the circuit with it.
///
/// The endpoint reports its input with `extended`, which stores the result in
/// the endpoint's status entry for the circuit thread to count as processed.
/// A `disconnect_input` removes the endpoint, losing this count.  The records
/// reach the circuit either way, so losing their count would leave
/// `total_processed_records` permanently below `total_input_records`.
pub fn removed_input_endpoint_input_is_still_counted(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    // Buffer enough that the trigger runs no step on its own: the test decides
    // when the circuit sees the input.
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "min_batch_size_records": 1_000_000,
        "max_buffering_delay_usecs": 1_000_000_000,
        "clock_resolution_usecs": null,
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();

    let endpoint = ManualInputEndpoint::new();
    let endpoint_config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": {
            "name": "file_input",
            "config": { "path": "unused" }
        },
        "format": { "name": "csv" }
    }))
    .unwrap();
    let endpoint_id = controller
        .add_input_endpoint("manual", endpoint_config, Box::new(endpoint.clone()), None)
        .unwrap();

    // The endpoint hands three records to the circuit and is removed before the
    // circuit thread collects the result.
    endpoint.push("1,true,,foo\n2,true,,foo\n3,true,,foo\n");
    endpoint.release();
    let status = controller.status();
    assert_eq!(status.num_total_input_records(), 3);
    assert_eq!(status.num_buffered_input_records(), 0);
    assert_eq!(
        status.num_total_processed_records(),
        0,
        "the test needs the result to still be uncollected here"
    );

    controller.disconnect_input(&endpoint_id);
    wait(|| controller.pipeline_complete(), 10_000)
        .expect("the input of a removed endpoint was never counted as processed");
    assert_eq!(controller.status().num_total_processed_records(), 3);

    // Same again, except that the endpoint reports the input only after it has
    // been removed, which is the window an endpoint that replies on its own
    // thread has between `remove_input` and losing its reader.
    endpoint.push("4,true,,foo\n5,true,,foo\n6,true,,foo\n");
    endpoint.release();
    wait(|| controller.pipeline_complete(), 10_000)
        .expect("input reported after the endpoint was removed was never counted as processed");
    assert_eq!(controller.status().num_total_processed_records(), 6);

    controller.stop().unwrap();
}

/// The other half of `ControllerInner::shutdown_token`: an ordinary stop cancels the
/// shutdown signal without any help from the DBSP runtime.
pub fn stop_cancels_the_shutdown_signal(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let temp_output = NamedTempFile::new().unwrap();
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "clock_resolution_usecs": null,
        "inputs": {},
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": temp_output.path() },
                },
                "format": { "name": "csv" },
            },
        },
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();

    assert!(!controller.inner.shutdown_token().is_cancelled());

    // Checked right after `initiate_stop`, which cancels the signal before it
    // returns.  Waiting for `stop` to join the circuit thread would let the
    // circuit's own kill cancel it and hide a missing cancel here.
    controller.initiate_stop();
    assert!(controller.inner.shutdown_token().is_cancelled());

    controller.stop().unwrap();
}

/// An output endpoint records the transaction it was created during only
/// after it enables its stream's accumulator.  Recording it first would let a
/// transaction that starts in between miss the enable, and yet reach the
/// endpoint as if its output were complete.
///
/// The view is not materialized, because a materialized view's accumulator
/// is always enabled.
pub fn output_endpoint_records_transaction_after_enable(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {},
        "outputs": {},
    }))
    .unwrap();
    let circuit = TestCircuit::Identity {
        relation: relation(),
        persistent_output_ids: vec![None],
        input_properties: Vec::new(),
        materialized: false,
        persistent_ids: true,
    };
    let controller = engine
        .controller(&circuit, &config, Box::new(|e, _| panic!("error: {e}")))
        .unwrap();
    let catalog = controller.catalog().clone();
    let enable_count = &catalog
        .output_handles(&SqlIdentifier::from("test_output1"))
        .unwrap()
        .enable_count;
    assert!(!enable_count.is_enabled());

    let endpoint = OutputEndpointDescr::new(
        "endpoint",
        "test_output1",
        false,
        false,
        None,
        Parker::new().unparker().clone(),
    );
    let mut outputs = controller.inner.outputs.write().unwrap();
    outputs
        .insert(12345, endpoint, || {
            assert!(enable_count.is_enabled());
            7
        })
        .unwrap();
    assert_eq!(
        outputs
            .lookup_by_id(&12345)
            .unwrap()
            .created_during_transaction_number,
        7
    );
    outputs.remove(&12345);
    drop(outputs);
    assert!(!enable_count.is_enabled());

    controller.stop().unwrap();
}

/// Name of the view in the aliased-index tests.
const ALIASED_VIEW: &str = "test_output1";

/// Index names for the aliased-index tests.  The first sorts after the view's
/// name, as in feldera/feldera#7281; the second sorts before it.
const ALIASED_INDEXES: [&str; 2] = ["test_output1_idx", "idx1"];

/// Configuration of a CSV file connector writing to `path`.
///
/// # Arguments
///
/// * `path` - File the connector writes to.
/// * `index` - Index the connector reads from, or `None` to read from
///   [`ALIASED_VIEW`] itself.
///
/// # Returns
///
/// The connector configuration as JSON.
fn aliased_index_output(path: &Path, index: Option<&str>) -> serde_json::Value {
    let mut config = json!({
        "stream": ALIASED_VIEW,
        "transport": {
            "name": "file_output",
            "config": { "path": path.display().to_string() },
        },
        "format": { "name": "csv", "config": {} },
    });
    if let Some(index) = index {
        config["index"] = json!(index);
    }
    config
}

/// Starts a controller for [`TestCircuit::AliasedIndex`].
///
/// # Arguments
///
/// * `input_path` - CSV file the pipeline reads in follow mode.
/// * `index` - Name of the view's index.
/// * `outputs` - Output connector configurations, keyed by connector name.
///
/// # Returns
///
/// The running controller.
fn start_aliased_index_controller(
    engine: &dyn PipelineTestEngine,
    input_path: &Path,
    index: &str,
    outputs: serde_json::Value,
) -> Controller {
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_path.display().to_string(),
                        "follow": true,
                    },
                },
                "format": { "name": "csv" },
            },
        },
        "outputs": outputs,
    }))
    .unwrap();

    let circuit = TestCircuit::AliasedIndex {
        key: key_relation(),
        relation: relation(),
        index: index.to_string(),
    };
    let controller = engine
        .controller(&circuit, &config, Box::new(|e, _| panic!("error: {e}")))
        .unwrap();
    controller.start();
    controller
}

/// Checks that output endpoint `endpoint_name` transmits one record for each
/// of the first `n_records` input records.
///
/// Waits for the endpoint to process those input records first.  An endpoint
/// processes a step's input records even when the step gives it no output, so
/// a starved endpoint fails the check instead of timing out.
///
/// # Arguments
///
/// * `controller` - The pipeline's controller.
/// * `endpoint_name` - Name of the output endpoint.
/// * `n_records` - Number of input records, each producing one output record.
#[track_caller]
fn check_endpoint_transmitted(controller: &Controller, endpoint_name: &str, n_records: u64) {
    wait(
        || {
            output_endpoint_metrics(controller, endpoint_name).total_processed_input_records
                >= n_records
        },
        OUTPUT_TIMEOUT_MS,
    )
    .unwrap();
    assert_eq!(
        output_endpoint_metrics(controller, endpoint_name).transmitted_records,
        n_records,
        "endpoint '{endpoint_name}' processed the input without transmitting its output"
    );
}

/// Connectors on a materialized view and on its first index receive the same
/// output.
///
/// The circuit publishes the view and its first index through one output
/// handle, and reading a handle removes the step's updates from it.  The
/// controller must read the handle once and hand the updates to every
/// connector on either relation, whichever of the two names sorts first.
pub fn view_and_aliased_index_connectors_receive_the_same_output(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    for index in ALIASED_INDEXES {
        let tempdir = TempDir::new().unwrap();
        let input_path = tempdir.path().join("input.csv");
        let view_path = tempdir.path().join("view.csv");
        let index_path = tempdir.path().join("index.csv");
        File::create_new(&input_path).unwrap();

        let controller = start_aliased_index_controller(
            engine,
            &input_path,
            index,
            json!({
                "view_output": aliased_index_output(&view_path, None),
                "index_output": aliased_index_output(&index_path, Some(index)),
            }),
        );
        append_input(&input_path, 0..100);
        check_endpoint_transmitted(&controller, "view_output", 100);
        check_endpoint_transmitted(&controller, "index_output", 100);
        controller.stop().unwrap();

        check_file_contents(&view_path, 0..100);
        check_file_contents(&index_path, 0..100);
    }
}

/// Closing a connector on a materialized view does not starve a connector on
/// the view's first index, which shares the view's output handle.
///
/// This is feldera/feldera#7281: a client opened and closed an `/egress`
/// stream on the view before any input arrived.  From then on, the PostgreSQL
/// connector on the index received nothing, although the view held every
/// record.
pub fn closing_a_view_connector_does_not_starve_its_aliased_index(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    for index in ALIASED_INDEXES {
        let tempdir = TempDir::new().unwrap();
        let input_path = tempdir.path().join("input.csv");
        let view_path = tempdir.path().join("view.csv");
        let index_path = tempdir.path().join("index.csv");
        File::create_new(&input_path).unwrap();

        let controller = start_aliased_index_controller(
            engine,
            &input_path,
            index,
            json!({ "index_output": aliased_index_output(&index_path, Some(index)) }),
        );

        // Connect a connector to the view and disconnect it, as an `/egress`
        // request does when its client goes away.
        let view_config: OutputEndpointConfig =
            serde_json::from_value(aliased_index_output(&view_path, None)).unwrap();
        let view_endpoint = controller
            .inner
            .connect_output("view_output", &view_config, None)
            .unwrap();
        controller.disconnect_output(&view_endpoint);

        append_input(&input_path, 0..100);
        check_endpoint_transmitted(&controller, "index_output", 100);
        controller.stop().unwrap();

        check_file_contents(&index_path, 0..100);
    }
}

/// A connector with `send_snapshot` on a materialized view's first index
/// starts with the view's contents.
///
/// The index shares the view's output handles, so the controller files the
/// connector under the view's name, and finds the view's snapshot under that
/// name.
pub fn aliased_index_connector_receives_the_snapshot(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    for index in ALIASED_INDEXES {
        let tempdir = TempDir::new().unwrap();
        let input_path = tempdir.path().join("input.csv");
        let index_path = tempdir.path().join("index.csv");
        File::create_new(&input_path).unwrap();

        let controller = start_aliased_index_controller(engine, &input_path, index, json!({}));
        append_input(&input_path, 0..100);
        wait(
            || {
                controller
                    .status()
                    .global_metrics
                    .num_total_processed_records()
                    >= 100
            },
            OUTPUT_TIMEOUT_MS,
        )
        .unwrap();

        // Attach the connector after the view holds every record, so that
        // only the snapshot can deliver them.
        let mut index_config = aliased_index_output(&index_path, Some(index));
        index_config["send_snapshot"] = json!(true);
        let index_config: OutputEndpointConfig = serde_json::from_value(index_config).unwrap();
        controller
            .inner
            .connect_output("index_output", &index_config, None)
            .unwrap();

        wait(
            || output_endpoint_metrics(&controller, "index_output").transmitted_records >= 100,
            OUTPUT_TIMEOUT_MS,
        )
        .unwrap_or_else(|()| panic!("the connector on '{index}' received no snapshot"));
        controller.stop().unwrap();

        check_file_contents(&index_path, 0..100);
    }
}

/// A connector attached to a materialized view between transactions receives
/// no output from before it was attached, even when the next transaction
/// takes several steps.
///
/// A materialized view's output handle receives every transaction's output,
/// with or without connectors.  In an explicit transaction, the connector's
/// group first reads the handle before the transaction commits, so the handle
/// must not still hold the output of the transaction that committed before
/// the connector was attached.
pub fn connector_attached_between_transactions_receives_no_earlier_output(
    engine: &dyn PipelineTestEngine,
) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let input_path = tempdir.path().join("input.csv");
    let output_path = tempdir.path().join("output.csv");
    File::create_new(&input_path).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_path.display().to_string(),
                        "follow": true,
                    },
                },
                "format": { "name": "csv" },
            },
        },
        "outputs": {},
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[Some("output")]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();

    // The view has no connector while it processes these records.
    append_input(&input_path, 0..100);
    wait(
        || {
            controller
                .status()
                .global_metrics
                .num_total_processed_records()
                >= 100
        },
        OUTPUT_TIMEOUT_MS,
    )
    .unwrap();

    // Attach a connector between transactions, then feed an explicit
    // transaction, whose step reads the view's handle before the commit.
    let output_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_output1",
        "transport": {
            "name": "file_output",
            "config": { "path": output_path.display().to_string() },
        },
        "format": { "name": "csv", "config": {} },
    }))
    .unwrap();
    controller
        .inner
        .connect_output("test_output1", &output_config, None)
        .unwrap();
    controller.start_transaction().unwrap();
    append_input(&input_path, 100..200);
    wait(
        || controller.status().num_total_circuit_input_records() >= 200,
        OUTPUT_TIMEOUT_MS,
    )
    .unwrap();

    controller.start_commit_transaction().unwrap();
    wait(
        || {
            output_endpoint_metrics(&controller, "test_output1").total_processed_input_records
                >= 200
        },
        OUTPUT_TIMEOUT_MS,
    )
    .unwrap();
    controller.stop().unwrap();

    check_file_contents(&output_path, 100..200);
}

/// Buffering input wakes the circuit thread, whatever else is buffered.
///
/// A wake rule that reasons about how much is buffered has to know which
/// endpoints the next step polls, and gets it wrong every time that set
/// narrows: input buffered by an endpoint the step skips stays buffered, so
/// the global count never falls back and every other endpoint's wakeup is
/// swallowed.  Waking unconditionally and leaving the decision to
/// `StepTrigger` costs one trigger evaluation and cannot go stale.
///
/// A missed wakeup here is a hang, not a slowdown:
/// [`committed_connector_input_does_not_spin_the_circuit`] covers the case
/// end to end.
pub fn input_batch_wakes_the_circuit_thread(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        // High enough that a wake rule reading the buffered count would
        // suppress the wakeup below.
        "min_batch_size_records": 1_000_000,
        "clock_resolution_usecs": null,
    }))
    .unwrap();

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    let status = controller.status();

    let parker = Parker::new();
    let batch = BufferSize {
        records: 1,
        bytes: 1,
    };
    // A backlog no step will consume, of the kind a connector that has
    // committed the in-progress transaction leaves behind.
    status.input_batch_global(
        BufferSize {
            records: 1_000,
            bytes: 1_000,
        },
        parker.unparker(),
    );
    parker.park_timeout(Duration::ZERO);

    status.input_batch_global(batch, parker.unparker());

    controller.stop().unwrap();

    // Set, so `park` returns without blocking. Unset, this waits the full
    // timeout and the assertion fails.
    let start = Instant::now();
    parker.park_timeout(Duration::from_secs(10));
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "buffering input did not wake the circuit thread"
    );
}

/// The disk reported on the stats endpoint is the one holding the storage
/// backend's own path, so a pipeline with local storage reports it and one
/// without storage omits it.
pub fn api_status_reports_disk_usage_from_storage_backend(engine: &dyn PipelineTestEngine) {
    use feldera_storage::disk::DiskUsage;

    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir.path().join("input.csv");
    let output_path = tempdir.path().join("output.csv");
    File::create_new(&input_path).unwrap();

    let global_metrics = |storage: serde_json::Value| {
        let mut config = json!({
            "name": "test",
            "workers": 2,
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
        });
        config
            .as_object_mut()
            .unwrap()
            .extend(storage.as_object().unwrap().clone());
        let config: PipelineConfig = serde_json::from_value(config).unwrap();

        let controller = engine
            .controller(
                &identity(&[None]),
                &config,
                Box::new(|e, _| info!("error: {e}")),
            )
            .unwrap();
        controller.start();
        let status = controller.api_status(false);
        controller.stop().unwrap();
        status.global_metrics
    };

    let with = global_metrics(json!({
        "storage_config": { "path": storage_dir },
        "storage": true,
    }));
    let total = with
        .disk_total_bytes
        .expect("local storage reports its disk");
    let available = with
        .disk_available_bytes
        .expect("local storage reports its disk");
    assert!(available <= total, "{available} > {total}");
    // Capacity is stable, so it must equal a direct reading of the same directory.
    let direct = DiskUsage::from_path(&storage_dir).unwrap();
    assert_eq!(total, direct.total_bytes);

    let without = global_metrics(json!({}));
    assert_eq!(without.disk_total_bytes, None);
    assert_eq!(without.disk_available_bytes, None);
}

/// The step a reader feeds comes from the steps the controller has started,
/// never from the steps its output connectors have finished. The two agree
/// while every output keeps up, which is why reading the wrong one hid for so
/// long, and they part as soon as one falls behind. A connector that defers
/// acknowledgment stamps its writes with this step, so reading the lagging
/// count acknowledges rows against a checkpoint that predates them (#7122).
///
/// Reading `total_completed_steps` here instead of `total_initiated_steps`
/// makes this test fail, which the arithmetic test above cannot do.
pub fn a_reader_is_told_the_step_being_fed_not_the_one_the_outputs_finished(
    engine: &dyn PipelineTestEngine,
) {
    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "current_step_test",
        "workers": 1,
        "inputs": {},
        "outputs": {},
    }))
    .unwrap();
    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    // An output connector two steps behind the circuit, which is what a slow
    // sink looks like to the counters.
    let metrics = &controller.inner.status.global_metrics;
    metrics.total_initiated_steps.store(7, Ordering::Release);
    metrics.total_completed_steps.store(5, Ordering::Release);

    let probe = super::InputProbe {
        endpoint_id: 0,
        endpoint_name: "test_input".to_string(),
        controller: controller.inner.clone(),
        max_batch_size: 1024,
        transaction_in_progress: AtomicBool::new(false),
    };

    assert_eq!(
        probe.current_step(),
        Some(6),
        "the controller started seven steps, so it is feeding the seventh"
    );
    assert_ne!(
        probe.current_step(),
        Some(metrics.total_completed_steps.load(Ordering::Acquire)),
        "the outputs have finished fewer steps than the circuit has started, \
         and an answer stamped with their count would go out too early"
    );

    controller.stop().unwrap();
}

/// Returns a controller for `config` and a probe for input connector
/// `test_input` with endpoint id 0, which has a status entry but no reader.
fn controller_with_probe(
    engine: &dyn PipelineTestEngine,
    config: serde_json::Value,
) -> (Controller, super::InputProbe) {
    init_test_logger();
    let controller = engine
        .controller(
            &identity(&[None]),
            &serde_json::from_value(config).unwrap(),
            Box::new(|_e, _| ()),
        )
        .unwrap();
    let input_config = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": {"name": "datagen", "config": {}},
    }))
    .unwrap();
    controller.inner.status.inputs.write().insert(
        0,
        super::stats::InputEndpointStatus::new("test_input", input_config, None, None),
    );
    let probe = super::InputProbe {
        endpoint_id: 0,
        endpoint_name: "test_input".to_string(),
        controller: controller.inner.clone(),
        max_batch_size: 1024,
        transaction_in_progress: AtomicBool::new(false),
    };
    (controller, probe)
}

/// True if `controller`'s open transaction waits for a connector to commit.
fn waits_for_connector(controller: &Controller) -> bool {
    let transaction_info = controller.inner.transaction_info.lock().unwrap();
    transaction_info
        .initiators
        .is_ongoing(transaction_info.is_multihost)
}

/// A connector that fails, or is disconnected, in the middle of a
/// transaction can never commit it, so its part ends.  Otherwise the
/// transaction would stay open forever.  A connector that recovers from an
/// error keeps its part.
pub fn stopped_connector_ends_its_transaction(engine: &dyn PipelineTestEngine) {
    let config = json!({"name": "test", "workers": 1, "inputs": {}, "outputs": {}});
    for stop in ["nonfatal", "fatal", "disconnect"] {
        let (controller, probe) = controller_with_probe(engine, config.clone());
        probe.start_transaction(Some("t"));
        assert!(waits_for_connector(&controller));
        match stop {
            "nonfatal" => probe.error(false, anyhow!("retrying"), None),
            "fatal" => probe.error(true, anyhow!("failed"), None),
            _ => controller.disconnect_input(&0),
        }
        assert_eq!(
            waits_for_connector(&controller),
            stop == "nonfatal",
            "{stop}"
        );
        controller.stop().unwrap();
    }
}

/// In a multihost pipeline, a host whose part of a transaction that needs
/// every host fails reports the connector as done, so that the coordinator
/// does not wait for it forever.
pub fn stopped_connector_finishes_all_hosts_transaction(engine: &dyn PipelineTestEngine) {
    let (controller, probe) = controller_with_probe(
        engine,
        json!({
            "name": "test",
            "workers": 1,
            "inputs": {},
            "outputs": {},
            "multihost": {"hosts": 1},
        }),
    );
    probe.start_transaction_on_all_hosts(Some("snapshot-0"));
    let all_hosts_done = || {
        controller
            .inner
            .transaction_info
            .lock()
            .unwrap()
            .sender
            .borrow()
            .all_hosts_done
            .clone()
    };
    assert!(all_hosts_done().is_empty());
    probe.error(true, anyhow!("failed"), None);
    assert_eq!(
        all_hosts_done(),
        std::collections::HashMap::from([("test_input".to_string(), None)])
    );
    controller.stop().unwrap();
}

/// The controller reports a paused input connector that the open transaction
/// waits for, because the transaction cannot commit until it is started.
pub fn paused_connector_holds_transaction(engine: &dyn PipelineTestEngine) {
    let (controller, probe) = controller_with_probe(
        engine,
        json!({
            "name": "test",
            "workers": 1,
            "inputs": {},
            "outputs": {},
        }),
    );
    let paused = || controller.inner.paused_transaction_connectors();
    let test_input = std::collections::BTreeSet::from(["test_input".to_string()]);

    // A paused connector outside a transaction holds nothing.
    controller.inner.status.pause_input_endpoint(&0);
    assert!(paused().is_empty());

    // A connector in the transaction holds it only while it is paused.
    probe.start_transaction(Some("t"));
    assert_eq!(paused(), test_input);
    controller.inner.status.start_input_endpoint(&0);
    assert!(paused().is_empty());
    controller.inner.status.pause_input_endpoint(&0);
    assert_eq!(paused(), test_input);

    // Once it commits its part, it holds the transaction no longer.
    probe.commit_transaction();
    assert!(paused().is_empty());
    controller.stop().unwrap();
}

/// Every materialized view must land on the same input prefix when a
/// transaction commits.
///
/// Shape of the failure in feldera/cloud#1934: an ad-hoc read of a `group by`
/// view returned the count over the first 839,785 of 1,000,000 ingested
/// records, while the view feeding it returned all 1,000,000, in a pipeline
/// that had committed its transaction and reported completion.  Here
/// `passthrough` stands in for the complete view and `counts` for the short
/// one.
///
/// The ingest deliberately runs many small steps with the connector racing
/// ahead of the circuit, so that the commit boundary falls while records are
/// still queued.
pub fn transaction_leaves_views_on_the_same_input_prefix(engine: &dyn PipelineTestEngine) {
    // The circuit keeps up with the connector: nothing is queued when the
    // commit starts.
    transaction_view_consistency(engine, 200_000, 5_000, true, 0);
}

/// The shape of the CI failure: the commit lands when nearly everything has
/// been fed into the transaction and only a couple of batches are still
/// queued.  In the failing run the aggregate view was short by 160,215
/// records, two of that pipeline's 80,000-record batches.
pub fn transaction_committing_on_a_short_tail_leaves_views_on_the_same_input_prefix(
    engine: &dyn PipelineTestEngine,
) {
    transaction_view_consistency(engine, 200_000, 5_000, true, 10_000);
}

/// Same, with the circuit far behind the connector, so that end of input (and
/// therefore the commit) arrives while a large backlog is still queued.  That
/// is the state the CI pipeline was in: at the last progress sample it had
/// received 830,486 records and processed 814,623 of them.
pub fn transaction_with_a_backlog_leaves_views_on_the_same_input_prefix(
    engine: &dyn PipelineTestEngine,
) {
    transaction_view_consistency(engine, 200_000, 100, true, 0);
}

/// How long to keep repeating a reproduction test, in seconds.
///
/// One pass by default, so the tests' cost is unchanged.  The failure in
/// feldera/cloud#1934 is a rare race that has only ever been seen on arm64, and
/// one pass is one sample; set `FELDERA_REPRO_SECONDS` to spend a CI job
/// hunting it on real arm64 hardware instead.
fn repro_budget() -> Option<Duration> {
    std::env::var("FELDERA_REPRO_SECONDS")
        .ok()
        .and_then(|seconds| seconds.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

/// Runs `body` once, or repeatedly until `FELDERA_REPRO_SECONDS` is spent.
fn repeat_for_budget(name: &str, mut body: impl FnMut()) {
    let Some(budget) = repro_budget() else {
        body();
        return;
    };
    let start = Instant::now();
    let mut passes = 0;
    while start.elapsed() < budget {
        body();
        passes += 1;
    }
    info!(
        "{name}: {passes} passes in {:.1}s",
        start.elapsed().as_secs_f64()
    );
}

/// Like [`wait`], but polls every 100us instead of every 10ms.  Used around
/// transaction boundaries, where a 10ms gap lets the circuit drain thousands of
/// records outside any transaction.
fn wait_fast<P>(mut predicate: P, timeout_ms: u128) -> Result<(), ()>
where
    P: FnMut() -> bool,
{
    let start = Instant::now();
    while !predicate() {
        if start.elapsed().as_millis() >= timeout_ms {
            return Err(());
        }
        sleep(Duration::from_micros(100));
    }
    Ok(())
}

/// The same invariant across many transactions instead of one.
///
/// The single-transaction tests observe only one or two distinct snapshots per
/// run, because the pipeline goes idle as soon as the ingest is over, so they
/// barely sample the window where the failure would show.  Here the connector
/// queue is filled up front and drained a slice at a time, one transaction per
/// slice: a run produces dozens of commits, and every one of them lands while
/// a large backlog is still queued, which is the state the CI pipeline
/// committed in.
///
/// Two invariants are checked over every snapshot the run produces:
///
/// * `passthrough` and `counts` agree, so no snapshot is internally torn.
/// * neither view ever goes backwards from one snapshot to the next.
pub fn many_transactions_leave_views_on_the_same_input_prefix(engine: &dyn PipelineTestEngine) {
    repeat_for_budget("many_transactions", || {
        transaction_rounds(engine, 200_000, 500, 4_000, 16, 0)
    });
}

/// Same, with one group per record instead of sixteen, so the aggregate carries
/// a large trace and the circuit merges and spills while the transaction runs.
pub fn many_transactions_with_a_high_cardinality_aggregate(engine: &dyn PipelineTestEngine) {
    repeat_for_budget("high_cardinality", || {
        transaction_rounds(engine, 200_000, 500, 4_000, 200_000, 0)
    });
}

/// Same, with kilobyte records, so each step carries far more bytes and spends
/// far longer in parse, exchange, and spill.
pub fn many_transactions_with_large_records(engine: &dyn PipelineTestEngine) {
    repeat_for_budget("large_records", || {
        transaction_rounds(engine, 50_000, 500, 4_000, 16, 1_024)
    });
}

/// Ingests `records` in slices of `per_transaction`, one transaction each,
/// grouping into `keys` groups with `payload_bytes` of filler per record.
fn transaction_rounds(
    engine: &dyn PipelineTestEngine,
    records: u64,
    max_batch_size: u64,
    per_transaction: u64,
    keys: i64,
    payload_bytes: usize,
) {
    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();

    let payload = "x".repeat(payload_bytes);

    let mut file = NamedTempFile::new().unwrap();
    {
        let mut out = std::io::BufWriter::new(file.as_file_mut());
        for id in 0..records {
            writeln!(
                out,
                r#"{{"id": {id}, "b": true, "i": {id}, "s": "record-{id}{payload}"}}"#
            )
            .unwrap();
        }
        out.flush().unwrap();
    }

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 8,
        "storage_config": { "path": storage_dir },
        "storage": true,
        "inputs": {
            "test_input1.file": {
                "stream": "test_input1",
                "max_batch_size": max_batch_size,
                "transport": {
                    "name": "file_input",
                    "config": { "path": file.path(), "buffer_size_bytes": 1_000_000 }
                },
                "format": {
                    "name": "json",
                    "config": { "array": false, "update_format": "raw" }
                }
            }
        },
    }))
    .unwrap();

    let circuit = TestCircuit::Aggregate {
        relation: relation(),
        n_keys: keys,
    };
    let controller = engine
        .controller(
            &circuit,
            &config,
            Box::new(|e, _| panic!("controller error: {e}")),
        )
        .unwrap();

    let done = AtomicBool::new(false);
    let mut torn: Vec<Sample> = Vec::new();
    let mut regressions: Vec<(Sample, Sample)> = Vec::new();
    let mut rounds = 0u64;
    let mut snapshots = 0usize;

    let processed = || {
        controller
            .status()
            .global_metrics
            .num_total_processed_records()
    };
    let end_of_input = || {
        controller
            .status()
            .input_status()
            .values()
            .all(|status| status.metrics.end_of_input.load(Ordering::Acquire))
    };
    let drained = || end_of_input() && controller.status().num_buffered_input_records() == 0;

    std::thread::scope(|scope| {
        let samplers: Vec<_> = (0..4)
            .map(|_| {
                scope.spawn(|| {
                    let mut samples = Vec::new();
                    let mut last = None;
                    while !done.load(Ordering::Acquire) {
                        if let Some(sample) = sample_views(&controller)
                            && Some(sample) != last
                        {
                            samples.push(sample);
                            last = Some(sample);
                        }
                    }
                    samples
                })
            })
            .collect();

        controller.start();

        while processed() < records && !drained() {
            let target = std::cmp::min(processed() + per_transaction, records);

            controller.start_transaction().unwrap();
            wait_fast(|| processed() >= target || drained(), default_timeout_ms())
                .expect("timed out waiting for a transaction to ingest its slice");

            // Every commit lands with the rest of the input still queued.
            controller.start_commit_transaction().unwrap();
            wait_fast(
                || controller.inner.get_transaction_state() == TransactionState::None,
                default_timeout_ms(),
            )
            .expect("timed out waiting for a commit to finish");

            rounds += 1;
        }

        wait(|| controller.pipeline_complete(), default_timeout_ms())
            .expect("timed out waiting for the pipeline to complete");

        done.store(true, Ordering::Release);
        // Each sampler's own sequence is ordered in time, so a view going
        // backwards within one sequence is a real regression.  Across
        // samplers the interleaving is arbitrary, so they are checked
        // separately.
        for sampler in samplers {
            let samples = sampler.join().unwrap();
            snapshots += samples.len();
            torn.extend(
                samples
                    .iter()
                    .copied()
                    .filter(|sample| sample.passthrough != sample.counts),
            );
            regressions.extend(
                samples
                    .windows(2)
                    .filter(|pair| {
                        pair[1].passthrough < pair[0].passthrough || pair[1].counts < pair[0].counts
                    })
                    .map(|pair| (pair[0], pair[1])),
            );
        }
    });

    let final_sample = sample_views(&controller).expect("no final snapshot");
    info!("{rounds} transactions, {snapshots} distinct snapshots, final {final_sample:?}");

    assert!(
        torn.is_empty(),
        "views disagree within a single snapshot: {torn:?}"
    );
    assert!(
        regressions.is_empty(),
        "a view went backwards between snapshots: {regressions:?}"
    );
    assert!(rounds > 1, "expected many transactions, got {rounds}");
    assert_eq!(
        final_sample.passthrough, records as i64,
        "passthrough view is short"
    );
    assert_eq!(
        final_sample.counts, records as i64,
        "aggregate view is short"
    );

    controller.stop().unwrap();
}

/// `commit_tail` delays the commit until the connector queue has drained to
/// that many records, so the transaction covers everything except a known
/// tail.  Zero commits as soon as the connector reports end of input.
fn transaction_view_consistency(
    engine: &dyn PipelineTestEngine,
    records: u64,
    max_batch_size: u64,
    storage: bool,
    commit_tail: u64,
) {
    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();

    #[allow(dead_code)]
    const KEYS: i64 = 16;

    // One JSON record per line, written up front, so the connector can fill
    // its queue much faster than the circuit drains it.
    let mut file = NamedTempFile::new().unwrap();
    {
        let mut out = std::io::BufWriter::new(file.as_file_mut());
        for id in 0..records {
            writeln!(
                out,
                r#"{{"id": {id}, "b": true, "i": {id}, "s": "record-{id}"}}"#
            )
            .unwrap();
        }
        out.flush().unwrap();
    }

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 8,
        "storage_config": { "path": storage_dir },
        "storage": storage,
        "inputs": {
            "test_input1.file": {
                "stream": "test_input1",
                "max_batch_size": max_batch_size,
                "transport": {
                    "name": "file_input",
                    "config": { "path": file.path(), "buffer_size_bytes": 1_000_000 }
                },
                "format": {
                    "name": "json",
                    "config": { "array": false, "update_format": "raw" }
                }
            }
        },
    }))
    .unwrap();

    let circuit = TestCircuit::Aggregate {
        relation: relation(),
        n_keys: KEYS,
    };
    let controller = engine
        .controller(
            &circuit,
            &config,
            Box::new(|e, _| panic!("controller error: {e}")),
        )
        .unwrap();

    // Open the transaction before any record can be ingested, so the whole
    // ingest belongs to it.
    controller.start_transaction().unwrap();

    let done = AtomicBool::new(false);
    let mut torn: Vec<Sample> = Vec::new();

    std::thread::scope(|scope| {
        // Sample every view out of one snapshot, over and over, for as long as
        // the pipeline runs.  Each sample is a single SQL statement, so all
        // three counts come from the same snapshot and any disagreement
        // between them is an inconsistency within that snapshot, not a race
        // between two reads.
        let sampler = scope.spawn(|| {
            let mut samples = Vec::new();
            let mut last = None;
            while !done.load(Ordering::Acquire) {
                if let Some(sample) = sample_views(&controller)
                    && Some(sample) != last
                {
                    samples.push(sample);
                    last = Some(sample);
                }
            }
            samples
        });

        controller.start();

        wait(
            || {
                controller
                    .status()
                    .input_status()
                    .values()
                    .all(|status| status.metrics.end_of_input.load(Ordering::Acquire))
            },
            default_timeout_ms(),
        )
        .expect("timed out waiting for end of input");

        if commit_tail > 0 {
            wait(
                || controller.status().num_buffered_input_records() <= commit_tail,
                default_timeout_ms(),
            )
            .expect("timed out waiting for the queue to drain to the commit tail");
        }

        info!(
            "end of input: {} processed, {} still queued",
            controller
                .status()
                .global_metrics
                .num_total_processed_records(),
            controller.status().num_buffered_input_records()
        );

        controller.start_commit_transaction().unwrap();
        wait(|| controller.pipeline_complete(), default_timeout_ms())
            .expect("timed out waiting for the pipeline to complete");

        done.store(true, Ordering::Release);
        let samples = sampler.join().unwrap();
        info!("{} distinct snapshots observed", samples.len());
        torn = samples
            .into_iter()
            .filter(|sample| sample.passthrough != sample.counts)
            .collect();
    });

    let final_sample = sample_views(&controller).expect("no final snapshot");
    info!("final snapshot: {final_sample:?}");

    assert!(
        torn.is_empty(),
        "views disagree within a single snapshot: {torn:?}"
    );
    assert_eq!(
        final_sample.passthrough, records as i64,
        "passthrough view is short"
    );
    assert_eq!(
        final_sample.counts, records as i64,
        "aggregate view is short"
    );

    controller.stop().unwrap();
}

/// One ad-hoc snapshot, read through a single query so that every count comes
/// from the same snapshot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Sample {
    /// `count(*)` over the view that mirrors the input.
    passthrough: i64,
    /// `sum(n)` over the `group by` view, which must equal `passthrough`.
    counts: i64,
}

/// Returns the row counts of the views of [`TestCircuit::Aggregate`] in one ad-hoc snapshot,
/// or `None` if the query fails or returns no row.
fn sample_views(controller: &Controller) -> Option<Sample> {
    let sql = "select \
         (select count(*) from passthrough) as passthrough_rows, \
         (select coalesce(sum(n), 0) from counts) as counted";
    let batches = query(controller, sql).ok()?;
    let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
    let column = |i: usize| -> i64 {
        batch
            .column(i)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("column {i} is not a BIGINT"))
            .value(0)
    };
    Some(Sample {
        passthrough: column(0),
        counts: column(1),
    })
}
