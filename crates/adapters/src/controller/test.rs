use super::{OutputEndpointControl, stats::BufferedInput};
use crate::{
    Controller, PipelineConfig,
    controller::{ControllerStatusContext, TransactionInfo},
    preprocess::{DecryptionPreprocessorFactory, PassthroughPreprocessorFactory},
    test::{
        DEFAULT_TIMEOUT_MS, TestStruct, generate_test_batch, init_test_logger, test_circuit, wait,
    },
    transport::set_barrier,
};
use anyhow::anyhow;
use crossbeam::sync::Parker;
use csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
use feldera_adapterlib::format::BufferSize;
use feldera_types::{
    config::{InputEndpointConfig, OutputEndpointConfig},
    constants::STATE_FILE,
    memory_pressure::MemoryPressure,
};
use serde_json::json;
use std::{
    borrow::Cow,
    cmp::min,
    collections::BTreeMap,
    fs::{File, create_dir, remove_file},
    io::Write,
    iter::repeat_n,
    ops::Range,
    path::{Path, PathBuf},
    sync::{atomic::Ordering, mpsc},
    thread::sleep,
    time::Duration,
};
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::oneshot;
use tracing::info;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use proptest::prelude::*;

#[test]
fn test_start_after_cyclic() {
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
    let Err(err) = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
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

#[test]
fn test_start_after() {
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

    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();

    // Wait 3 seconds, assert(no data in the output table)

    // Unpause the first connector.

    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

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

// A typo in the CSV parser config (e.g. "header" instead of "headers") must
// be reported as an error rather than silently using the default value.
#[test]
fn test_csv_input_config_unknown_field() {
    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {
                        "header": true
                    }
                }
            }
        }
    }))
    .unwrap();

    let Err(err) = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    ) else {
        panic!("expected controller creation to fail due to unknown CSV config field 'header'")
    };

    let msg = err.to_string();
    // Full error looks like:
    //   invalid controller configuration: Error parsing format configuration
    //   for input endpoint 'test_input1': unknown field `header`, expected
    //   one of `delimiter`, `headers`, ...
    //   Invalid configuration: {"header":true}
    assert!(
        msg.contains("Error parsing format configuration for input endpoint 'test_input1'"),
        "error should identify the affected endpoint: {msg}"
    );
    assert!(
        msg.contains("unknown field `header`"),
        "error should name the unknown field: {msg}"
    );
    assert!(
        msg.contains("`headers`"),
        "error should suggest the correct field name: {msg}"
    );
}

// TODO: Parameterize this with config string, so we can test different
// input/output formats and transports when we support more than one.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]
    #[test]
    fn proptest_csv_file(
        data in generate_test_batch(5000),
        min_batch_size_records in 1..100usize,
        max_buffering_delay_usecs in 1..2000usize,
        input_buffer_size_bytes in 1..1000usize,
        output_buffer_size_records in 1..100usize)
    {
        let temp_input_file = NamedTempFile::new().unwrap();
        let temp_output_path = NamedTempFile::new().unwrap().into_temp_path();
        let output_path = temp_output_path.to_str().unwrap().to_string();
        temp_output_path.close().unwrap();

        let secrets_dir = TempDir::new().unwrap();
        create_dir(secrets_dir.path().join("kubernetes")).unwrap();
        create_dir(secrets_dir.path().join("kubernetes/paths")).unwrap();
        std::fs::write(secrets_dir.path().join("kubernetes/paths/input"), temp_input_file.path().as_os_str().as_encoded_bytes()).unwrap();
        std::fs::write(secrets_dir.path().join("kubernetes/paths/output"), &output_path).unwrap();

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
                    "format": {
                        "name": "csv"
                    }
                }
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": "${secret:kubernetes:paths/output}"
                        }
                    },
                    "format": {
                        "name": "csv",
                        "config": {
                            "buffer_size_records": output_buffer_size_records,
                        }
                    }
                }
            }
        })).unwrap();

        info!("input file: {}", temp_input_file.path().display());
        info!("output file: {output_path}");
        let controller = Controller::with_test_config(
                |circuit_config| Ok(test_circuit::<TestStruct>(circuit_config, &[], &[None])),
                &config,
                Box::new(|e, _| panic!("error: {e}")),
            )
            .unwrap();

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_input_file.as_file());

        for val in data.iter().cloned() {
            writer.serialize(val).unwrap();
        }
        writer.flush().unwrap();
        println!("wait for {} records", data.len());
        controller.start();

        // Wait for the pipeline to output all records.
        wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

        assert_eq!(controller.status().output_status().get(&0).unwrap().transmitted_records(), data.len() as u64);

        controller.stop().unwrap();

        let mut expected = data;
        expected.sort();

        let mut actual: Vec<_> = CsvReaderBuilder::new()
            .has_headers(false)
            .from_path(&output_path)
            .unwrap()
            .deserialize::<(TestStruct, i32)>()
            .map(|res| {
                let (val, weight) = res.unwrap();
                assert_eq!(weight, 1);
                val
            })
            .collect();
        actual.sort();

        // Don't leave garbage in the FS.
        remove_file(&output_path).unwrap();

        assert_eq!(actual, expected);
    }
}

#[derive(Clone)]
struct FtTestRound {
    n_records: usize,
    do_checkpoint: bool,
    pause_afterward: bool,
    immediate_checkpoint: bool,

    /// Apply function to modify pipeline configuration before starting the pipeline.
    /// Modified config prevents the pipeline from replaying the journal; however it
    /// should still produce the same result by replaying inputs from the last checkpointed
    /// offset.
    modify_config: Option<fn(PipelineConfig) -> PipelineConfig>,
}

impl FtTestRound {
    fn with_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: true,
            pause_afterward: false,
            immediate_checkpoint: false,
            modify_config: None,
        }
    }
    fn without_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: false,
            pause_afterward: false,
            immediate_checkpoint: false,
            modify_config: None,
        }
    }
    fn with_pause_afterward(self) -> Self {
        Self {
            pause_afterward: true,
            ..self
        }
    }

    fn with_modify_config(self, f: fn(PipelineConfig) -> PipelineConfig) -> Self {
        Self {
            modify_config: Some(f),
            ..self
        }
    }

    /// Requests a checkpoint immediately upon resume, without waiting for
    /// records to be replayed.  This helps catch regression for bugs in
    /// handling this case (usually because the initial input positions are
    /// written as empty instead of as a copy of the previous positions).
    fn immediate_checkpoint() -> Self {
        Self {
            n_records: 0,
            do_checkpoint: false,
            pause_afterward: false,
            immediate_checkpoint: true,
            modify_config: None,
        }
    }
}

/// Reads `path` and ensures that it contains the [TestStruct] records in
/// `expect`.
#[track_caller]
fn check_file_contents(path: &Path, expect: Range<usize>) {
    let mut actual = CsvReaderBuilder::new()
        .has_headers(false)
        .from_path(path)
        .unwrap()
        .deserialize::<(TestStruct, i32)>()
        .map(|res| {
            let (val, weight) = res.unwrap();
            assert_eq!(weight, 1);
            val
        })
        .collect::<Vec<_>>();
    actual.sort();

    assert_eq!(actual.len(), expect.len());
    for (record, expect_record) in actual
        .into_iter()
        .zip(expect.map(|id| TestStruct::for_id(id as u32)))
    {
        assert_eq!(record, expect_record);
    }
}

fn count_endpoint_records(controller: &Controller, output_index: usize) -> u64 {
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

fn collect_endpoint_records(controller: &Controller, n: usize) -> Vec<usize> {
    (0..n)
        .map(|i| count_endpoint_records(controller, i) as usize)
        .collect()
}

#[track_caller]
fn wait_for_records(controller: &Controller, expect_n: &[usize]) {
    println!("waiting for {expect_n:?} records...");
    let n = expect_n.len();
    let mut last_n = repeat_n(0, n).collect::<Vec<_>>();
    wait(
        || {
            let new_n = collect_endpoint_records(controller, n);
            for i in 0..n {
                if new_n[i] > last_n[i] {
                    println!("received {} records on test_output{}", new_n[i], i + 1);
                }
            }
            last_n = new_n;
            last_n
                .iter()
                .zip(expect_n.iter())
                .all(|(&last, &expect)| last >= expect)
        },
        10_000,
    )
    .unwrap();

    // No more records should arrive, but give the controller some time
    // to send some more in case there's a bug.
    sleep(Duration::from_millis(100));

    // Then verify that the number is as expected.
    assert_eq!(&collect_endpoint_records(controller, n), expect_n);
}

fn assert_bounded_suspend_steps(controller: &Controller, suspend_request_step: u64) {
    let suspend_complete_step = controller.status().global_metrics.total_initiated_steps();
    let suspend_steps = suspend_complete_step.saturating_sub(suspend_request_step);

    assert!(
        suspend_steps <= 1000,
        "suspend advanced {suspend_steps} steps while waiting for barriers \
         ({suspend_request_step}..{suspend_complete_step})"
    );
}

/// Runs a basic test of fault tolerance.
///
/// The test proceeds in multiple rounds. For each element of `rounds`, the
/// test writes `n_records` records to the input file, and starts the
/// pipeline and waits for it to process the data.  If `do_checkpoint` is
/// true, it creates a new checkpoint. Then it stops the pipeline, checks
/// that the output is as expected, and goes on to the next round.
///
/// This also tests that the controller resolves secrets and that it freshly
/// resolves them every time it resumes from a checkpoint, by using a secret for
/// the location of its input and output files and renaming these files before
/// each round.  If the controller did not re-resolve the secrets when it
/// resumes, then it would try to read an input file that was no longer there,
/// or it would try to write output to an old location, and in either case that
/// would cause an error.
fn test_ft(rounds: &[FtTestRound]) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();

    // This allows the temporary directory to be deleted when we finish.  If
    // you want to keep it for inspection instead, comment out the following
    // line and then remove the comment markers on the two lines after that.
    let tempdir_path = tempdir.path();
    //let tempdir_path = tempdir.into_path();
    //println!("{}", tempdir_path.display());

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();

    create_dir(tempdir.path().join("kubernetes")).unwrap();
    create_dir(tempdir.path().join("kubernetes/paths")).unwrap();

    const INPUT_SECRET_REFERENCE: &str = "${secret:kubernetes:paths/input}";
    const OUTPUT_SECRET_REFERENCE: &str = "${secret:kubernetes:paths/output}";

    let mut config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "fault_tolerance": {},
        "clock_resolution_usecs": null,
        "secrets_dir": tempdir_path,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": INPUT_SECRET_REFERENCE,
                        "follow": true
                    }
                },
                "format": {
                    "name": "csv"
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": OUTPUT_SECRET_REFERENCE,
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }
        }
    }))
    .unwrap();

    // Number of records written to the input.
    let mut total_records = 0usize;

    // Number of input records included in the latest checkpoint (always <=
    // total_records).
    let mut checkpointed_records = 0usize;

    let mut paused = false;

    let mut prev_input_path = None;
    let mut prev_output_path = None;

    const TABOO: &str = "TABOO";

    for (
        round,
        FtTestRound {
            n_records,
            do_checkpoint,
            pause_afterward,
            immediate_checkpoint,
            modify_config,
        },
    ) in rounds.iter().cloned().enumerate()
    {
        // Create input file, or move it from its previous location.
        let input_path = tempdir_path.join(format!("{TABOO}-input{round}.csv"));
        let input_file = if let Some(prev_input_path) = &prev_input_path {
            std::fs::rename(prev_input_path, &input_path).unwrap();
            File::options().append(true).open(&input_path).unwrap()
        } else {
            File::create_new(&input_path).unwrap()
        };
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(&input_file);

        // Move output file from its previous location, if any.
        let output_path = tempdir_path.join(format!("{TABOO}-output{round}.csv"));
        if let Some(prev_output_path) = &prev_output_path {
            std::fs::rename(prev_output_path, &output_path).unwrap();
        };

        // Update secrets to point to new locations.
        for (name, path) in [("input", &input_path), ("output", &output_path)] {
            std::fs::write(
                tempdir.path().join("kubernetes/paths").join(name),
                path.as_os_str().as_encoded_bytes(),
            )
            .unwrap();
        }
        std::fs::write(
            tempdir.path().join("kubernetes/paths/input"),
            input_path.as_os_str().as_encoded_bytes(),
        )
        .unwrap();
        std::fs::write(
            tempdir.path().join("kubernetes/paths/output"),
            output_path.as_os_str().as_encoded_bytes(),
        )
        .unwrap();

        println!(
            "--- round {round}: {}{}{}add {n_records} records{}, {} --- ",
            if modify_config.is_some() {
                "run pipeline with modified config, "
            } else {
                ""
            },
            if paused { "unpause the input, " } else { "" },
            if immediate_checkpoint {
                "immediately initiate a checkpoint, "
            } else {
                ""
            },
            if pause_afterward {
                ", then pause the input"
            } else {
                ""
            },
            if do_checkpoint {
                "and checkpoint"
            } else {
                "no checkpoint"
            },
        );

        // Write records to the input file.
        println!(
            "Writing records {total_records}..{}",
            total_records + n_records
        );
        if n_records > 0 {
            for id in total_records..total_records + n_records {
                writer.serialize(TestStruct::for_id(id as u32)).unwrap();
            }
            writer.flush().unwrap();
            total_records += n_records;
        }

        // Start pipeline.
        println!("start pipeline");

        if let Some(modify_config) = modify_config {
            config = modify_config(config);
        }

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
        controller.start();

        let (sender, receiver) = oneshot::channel();
        if immediate_checkpoint {
            println!("start checkpoint in background");
            controller.start_checkpoint(Box::new(move |result| sender.send(result).unwrap()));
        } else {
            // Wait for replay for finish and then check that the input endpoint's
            // pause state matches what it should be.
            wait(|| !controller.is_replaying(), 1000).unwrap();
            assert_eq!(
                controller.is_input_endpoint_paused("test_input1").unwrap(),
                paused
            );
        }

        // Wait for the records that are not in the checkpoint to be
        // processed or replayed.
        let expect_n = total_records - checkpointed_records;
        if paused && expect_n > 0 {
            controller.start_input_endpoint("test_input1").unwrap();
            paused = false;
        }
        println!(
            "wait for {} records {checkpointed_records}..{total_records}",
            expect_n
        );
        wait_for_records(&controller, &[total_records]);

        if pause_afterward {
            controller.pause_input_endpoint("test_input1").unwrap();
            paused = true;

            // Wait to journal the pause.
            wait(
                || {
                    !controller
                        .status()
                        .global_metrics
                        .step_requested
                        .load(Ordering::Relaxed)
                },
                1000,
            )
            .unwrap();

            // Make sure the step gets executed.
            sleep(Duration::from_millis(1000));
        }

        // Checkpoint, if requested.
        if immediate_checkpoint {
            println!("wait for checkpoint to complete");
            receiver.blocking_recv().unwrap().unwrap();
        } else if do_checkpoint {
            println!("checkpoint");
            controller.checkpoint().unwrap();
        }

        // Stop controller.
        println!("stop controller");
        controller.stop().unwrap();

        // Read output and compare. Our output adapter, which is not
        // fault-tolerant, truncates the output file to length 0 each
        // time. Therefore, the output file should contain all the records
        // in `checkpointed_records..total_records`.
        check_file_contents(&output_path, checkpointed_records..total_records);

        if do_checkpoint || immediate_checkpoint {
            checkpointed_records = total_records;

            // Read the checkpoint file and make sure that:
            //
            // - The TABOO string does not appear in it.  It must not be in
            //   there because it appears only in the expanded version of the
            //   secret, not in the name of the secret.
            //
            // - The INPUT_SECRET_REFERENCE and OUTPUT_SECRET_REFERENCE strings
            //   do appear in it.  These must be there because they are what
            //   expands to the secrets.
            //
            //   It is valuable to check for these because this gives us some
            //   confidence that the file isn't encoded in some form such that
            //   TABOO appears but just not literally.  That is, our check for
            //   TABOO would fail if TABOO were to be base64-encoded, but in
            //   that case it's likely that INPUT_SECRET_REFERENCE and
            //   OUTPUT_SECRET_REFERENCE are also base64-encoded, so that these
            //   tests would fail.
            //
            //   (As of this writing, the checkpoint file is encoded in
            //   plaintext JSON, but this check will alert us if that changes
            //   without updating this test.)
            let checkpoint = std::fs::read(storage_dir.join(STATE_FILE)).unwrap();
            assert!(!contains_subslice(&checkpoint, TABOO.as_bytes()));
            assert!(contains_subslice(
                &checkpoint,
                INPUT_SECRET_REFERENCE.as_bytes()
            ));
            assert!(contains_subslice(
                &checkpoint,
                OUTPUT_SECRET_REFERENCE.as_bytes()
            ));
        }

        prev_input_path = Some(input_path);
        prev_output_path = Some(output_path);
    }
}

/// Returns true if `needle` is present as any subslice of `haystack`.
///
/// Yes it's appalling that this isn't part of the standard library, see
/// https://github.com/rust-lang/rust/issues/54961
fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
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

fn _test_concurrent_init(max_parallel_connector_init: u64) {
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

    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();

    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

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

#[test]
fn test_concurrent_init() {
    _test_concurrent_init(1);
    _test_concurrent_init(10);
    _test_concurrent_init(100);
}

#[test]
fn test_connector_init_error() {
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

    let result = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    );

    assert!(result.is_err());
}

#[test]
fn ft_with_checkpoints() {
    test_ft(&[
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
    ]);
}

#[test]
fn ft_immediate_checkpoints() {
    test_ft(&[
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::immediate_checkpoint(),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::immediate_checkpoint(),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::immediate_checkpoint(),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::immediate_checkpoint(),
        FtTestRound::with_checkpoint(2500),
    ]);
}

#[test]
fn ft_without_checkpoints() {
    test_ft(&[
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
    ]);
}

#[test]
fn ft_with_checkpoints_with_pauses() {
    test_ft(&[
        FtTestRound::with_checkpoint(2500).with_pause_afterward(),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500).with_pause_afterward(),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::with_checkpoint(2500).with_pause_afterward(),
    ]);
}

#[test]
fn ft_without_checkpoints_with_pauses() {
    test_ft(&[
        FtTestRound::without_checkpoint(2500).with_pause_afterward(),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500).with_pause_afterward(),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(2500).with_pause_afterward(),
    ]);
}

#[test]
fn ft_alternating() {
    test_ft(&[
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
    ]);
}

#[test]
fn ft_initially_zero_without_checkpoint() {
    test_ft(&[
        FtTestRound::without_checkpoint(0),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(0),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
    ]);
}

#[test]
fn ft_initially_zero_with_checkpoint() {
    test_ft(&[
        FtTestRound::with_checkpoint(0),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::without_checkpoint(0),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
    ]);
}

fn add_output(mut config: PipelineConfig) -> PipelineConfig {
    let tmpfile = NamedTempFile::new().unwrap();
    println!("adding output to {}", tmpfile.path().display());

    let output_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_output1",
        "transport": {
            "name": "file_output",
            "config": {
                "path": tmpfile.path(),
            }
        },
        "format": {
            "name": "csv",
            "config": {}
        }
    }))
    .unwrap();

    config
        .outputs
        .insert(Cow::Borrowed("test_output2"), output_config);
    config
}

fn add_input(mut config: PipelineConfig) -> PipelineConfig {
    let input_config: InputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_input1",
        "transport": {
            "name": "file_input",
            "config": {
                "path": "/dev/null",
                "follow": true
            }
        },
        "format": {
            "name": "csv"
        }
    }))
    .unwrap();

    config
        .inputs
        .insert(Cow::Borrowed("test_input2"), input_config);
    config
}

fn remove_output(mut config: PipelineConfig) -> PipelineConfig {
    config.outputs.remove(&Cow::Borrowed("test_output2"));
    config
}

#[test]
fn ft_modified_connectors() {
    test_ft(&[
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500).with_modify_config(add_output),
        FtTestRound::without_checkpoint(2500).with_modify_config(remove_output),
        FtTestRound::with_checkpoint(2500).with_modify_config(add_input),
        FtTestRound::without_checkpoint(2500),
        FtTestRound::with_checkpoint(2500),
        FtTestRound::without_checkpoint(2500),
    ]);
}

/// Verifies that a `send_snapshot: true` output connector delivers the
/// snapshot exactly once across the pipeline's lifetime.
///
/// If the `snapshot_sent` flag were not persisted in the checkpoint, the
/// connector would re-send the full snapshot every time the pipeline
/// restarts, which would overwrite the file with the cumulative view state
/// (all records so far) instead of just the new records produced since the
/// last checkpoint. The assertion below catches that regression: in every
/// round after the first, the file must contain only records in
/// `checkpointed_records..total_records`.
///
/// Round 0 is handled separately because that is the one round where the
/// (empty) snapshot is sent.
#[test]
fn ft_send_snapshot_delivered_once() {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();

    let input_path = tempdir_path.join("input.csv");
    let output_path = tempdir_path.join("output.csv");

    // Pre-create the (empty) input file so `file_input` can open it on the
    // first start; the connector keeps the file open in follow mode and we
    // append new rows at the start of each round.
    File::create_new(&input_path).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "fault_tolerance": {},
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
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "send_snapshot": true,
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path.display().to_string(),
                    },
                },
                "format": { "name": "csv", "config": {} },
            },
        },
    }))
    .unwrap();

    // Three rounds, each appending 500 records and taking a checkpoint.
    let rounds: &[usize] = &[500, 500, 500];
    let mut total_records = 0usize;
    let mut checkpointed_records = 0usize;

    for (round, n_records) in rounds.iter().copied().enumerate() {
        let input_file = File::options().append(true).open(&input_path).unwrap();
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(&input_file);
        for id in total_records..total_records + n_records {
            writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        }
        writer.flush().unwrap();
        total_records += n_records;

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
        controller.start();

        wait(|| !controller.is_replaying(), 10_000).unwrap();
        wait_for_records(&controller, &[total_records]);
        controller.checkpoint().unwrap();
        controller.stop().unwrap();

        if round == 0 {
            // Round 0: the initial snapshot carries every record seen so
            // far, so the file must reflect the full cumulative state.
            check_file_contents(&output_path, 0..total_records);
        } else {
            // Round 1+: snapshot was already delivered in round 0 and
            // checkpointed as `snapshot_sent = true`. The restarted
            // connector truncates the file (FileOutputEndpoint opens with
            // File::create) and then writes only the delta records emitted
            // during this round. If the checkpoint-aware guard regressed
            // and the snapshot were re-sent, we would see all
            // `0..total_records` records here instead.
            check_file_contents(&output_path, checkpointed_records..total_records);
        }
        checkpointed_records = total_records;
    }
}

/// Verifies that flipping `send_snapshot` from `false` to `true` across a
/// checkpoint restart re-delivers the full snapshot as the first batch
/// the connector sees after restart, before any new deltas. Models the
/// realistic scenario where a user decides after the fact that a sink
/// needs a snapshot, edits the config, and restarts the pipeline.
///
/// Three rounds:
/// * Round 0 (`send_snapshot: false`, 500 records in) -- only deltas land
///   in the file; no snapshot is emitted.
/// * Round 1 (`send_snapshot: true`, no new input) -- the flag flip counts
///   as a connector modification, so the snapshot is re-delivered; the
///   file is repopulated with the full view state.
/// * Round 2 (`send_snapshot: true` unchanged, 500 more records) -- the
///   snapshot is not re-sent; only the new deltas reach the file.
#[test]
fn ft_send_snapshot_resent_on_flag_flip() {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();

    let input_path = tempdir_path.join("input.csv");
    let output_path = tempdir_path.join("output.csv");
    File::create_new(&input_path).unwrap();

    fn build_config(
        input_path: &Path,
        output_path: &Path,
        storage_dir: &Path,
        send_snapshot: bool,
    ) -> PipelineConfig {
        serde_json::from_value(json!({
            "name": "test",
            "workers": 4,
            "storage_config": { "path": storage_dir },
            "storage": true,
            "fault_tolerance": {},
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
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "send_snapshot": send_snapshot,
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path.display().to_string(),
                        },
                    },
                    "format": { "name": "csv", "config": {} },
                },
            },
        }))
        .unwrap()
    }

    let run_round = |config: &PipelineConfig, expected_total: usize| {
        let controller = Controller::with_test_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
        controller.start();
        wait(|| !controller.is_replaying(), 10_000).unwrap();
        wait_for_records(&controller, &[expected_total]);
        controller.checkpoint().unwrap();
        controller.stop().unwrap();
    };

    // `run_round` passes an expected total `transmitted_records` count
    // straight to `wait_for_records`. That counter is persisted across
    // checkpoints (see `CheckpointOutputEndpointMetrics`), so every record
    // the connector emits across all rounds contributes cumulatively.

    // Round 0: send_snapshot=false, push 500 records. Only deltas hit the
    // file; cumulative transmitted = 500.
    {
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(File::options().append(true).open(&input_path).unwrap());
        for id in 0..500 {
            writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        }
        writer.flush().unwrap();
    }
    let config_off = build_config(&input_path, &output_path, &storage_dir, false);
    run_round(&config_off, 500);
    check_file_contents(&output_path, 0..500);

    // Round 1: flip send_snapshot to true with no new input. Changing the
    // field makes pipeline_diff classify the connector as modified, which
    // overrides the checkpointed `snapshot_sent=true` back to `false`, so
    // the initial snapshot fires again as the first (and only) batch on
    // the queue. The output file starts empty here because
    // `FileOutputEndpoint::new` opens the path with `File::create` (which
    // truncates) on every controller startup, and the fresh snapshot
    // writes 0..500 into it. Cumulative transmitted jumps to 1000. If the
    // modified-connector path did not clear `snapshot_sent`, nothing
    // would land in the file on this restart and transmitted would stay
    // at 500.
    let config_on = build_config(&input_path, &output_path, &storage_dir, true);
    assert_ne!(
        config_off, config_on,
        "flipping send_snapshot must actually change the config so pipeline_diff classifies it as modified"
    );
    run_round(&config_on, 1000);
    check_file_contents(&output_path, 0..500);

    // Round 2: send_snapshot=true unchanged, append 500 more records. The
    // snapshot_sent flag was set to true at the end of round 1, so the
    // snapshot does not fire again; only the new delta records 500..1000
    // hit the file on this restart (cumulative transmitted 1500).
    {
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(File::options().append(true).open(&input_path).unwrap());
        for id in 500..1000 {
            writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        }
        writer.flush().unwrap();
    }
    run_round(&config_on, 1500);
    check_file_contents(&output_path, 500..1000);
}

/// End-to-end: modified `send_snapshot: true` delta sink delivers its
/// snapshot on a restart that never calls `start()` (stays paused).
#[cfg(feature = "with-deltalake")]
#[test]
fn ft_send_snapshot_delta_delivered_while_paused() {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let delta_dir = tempdir_path.join("delta_output");
    let delta_uri = format!("file://{}", delta_dir.display());
    File::create_new(&input_path).unwrap();

    fn build_config(
        input_path: &Path,
        delta_uri: &str,
        storage_dir: &Path,
        send_snapshot: bool,
    ) -> PipelineConfig {
        serde_json::from_value(json!({
            "name": "test",
            "workers": 4,
            "storage_config": { "path": storage_dir },
            "storage": true,
            "fault_tolerance": {},
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
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "send_snapshot": send_snapshot,
                    "transport": {
                        "name": "delta_table_output",
                        "config": {
                            "uri": delta_uri,
                            "mode": "truncate",
                        },
                    },
                },
            },
        }))
        .unwrap()
    }

    // Round 0: send_snapshot=false; push 500 records. The delta sink writes
    // them as CDC inserts via the normal delta path. Checkpoint, stop.
    {
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(File::options().append(true).open(&input_path).unwrap());
        for id in 0..500 {
            writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        }
        writer.flush().unwrap();
    }
    let config_off = build_config(&input_path, &delta_uri, &storage_dir, false);
    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[Some("output")],
            ))
        },
        &config_off,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();
    controller.start();
    wait(|| !controller.is_replaying(), 10_000).unwrap();
    wait_for_records(&controller, &[500]);
    controller.checkpoint().unwrap();
    controller.stop().unwrap();

    // Round 1: flip send_snapshot=true and restart, but do NOT call
    // `controller.start()` — the pipeline stays in the default `Paused`
    // state. The connector definition changed, so `is_snapshot_pending`
    // becomes true; with the fix, registration requests a step that fires
    // even while paused and delivers the snapshot (500 records) to the
    // truncated delta table. `transmitted_records` should reach 1000
    // (500 carried over from the checkpoint seed + 500 from the snapshot).
    let config_on = build_config(&input_path, &delta_uri, &storage_dir, true);
    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[Some("output")],
            ))
        },
        &config_on,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();
    // Notably: NO `controller.start()` — pipeline stays paused.
    wait_for_records(&controller, &[1000]);
    controller.stop().unwrap();
}

/// Runs a basic test of suspend and resume, without fault tolerance.
///
/// For each element of `rounds`, the test writes the specified number of
/// records to the input file, and starts the pipeline and waits for it to
/// process the data.  Then it suspends the pipeline, checks that the output
/// is as expected, and goes on to the next round.
fn test_suspend(rounds: &[usize]) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();

    // This allows the temporary directory to be deleted when we finish.  If
    // you want to keep it for inspection instead, comment out the following
    // line and then remove the comment markers on the two lines after that.
    let tempdir_path = tempdir.path();
    //let tempdir_path = tempdir.into_path();
    //println!("{}", tempdir_path.display());

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let input_file = File::create(&input_path).unwrap();
    let output_path = tempdir_path.join("output.csv");

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_path,
                        "follow": true
                    }
                },
                "format": {
                    "name": "csv"
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path,
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }
        }
    }))
    .unwrap();

    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);

    // Number of records written to the input.
    let mut total_records = 0usize;

    // Number of input records included in the latest checkpoint (always <=
    // total_records).
    let mut checkpointed_records = 0usize;

    for (round, n_records) in rounds.iter().copied().enumerate() {
        println!("--- round {round}: add {n_records} records and suspend --- ");

        // Write records to the input file.
        println!(
            "Writing records {total_records}..{}",
            total_records + n_records
        );
        if n_records > 0 {
            for id in total_records..total_records + n_records {
                writer.serialize(TestStruct::for_id(id as u32)).unwrap();
            }
            writer.flush().unwrap();
            total_records += n_records;
        }

        // Start pipeline.
        println!("start pipeline");
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
        controller.start();

        // Wait for the records that are not in the checkpoint to be
        // processed or replayed.
        wait_for_output(
            &controller,
            checkpointed_records,
            &(checkpointed_records..total_records)
                .map(|id| TestStruct::for_id(id as u32))
                .collect::<Vec<_>>(),
            &output_path,
        );

        // Suspend.
        println!("suspend");
        controller.suspend().unwrap();

        // Stop controller.
        println!("stop controller");
        controller.stop().unwrap();

        // Read output and compare. Our output adapter, which is not
        // fault-tolerant, truncates the output file to length 0 each
        // time. Therefore, the output file should contain all the records
        // in `checkpointed_records..total_records`.
        check_file_contents(&output_path, checkpointed_records..total_records);

        checkpointed_records = total_records;
        println!();
    }
}

/// Test suspend and resume with bootstrapping of modified parts of the circuit
/// on resume.
///
/// Starts the pipeline, then for each element of `rounds`:
///
/// * append the specified number of records to the input file
/// * wait for the newly added records to show up in the output file
/// * suspend the pipeline
/// * modify the pipeline by changing the persistent id of the output stream
/// * resume the pipeline, make sure that the entire input shows up in the
///   output stream.
fn test_bootstrap(rounds: &[usize]) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();

    // This allows the temporary directory to be deleted when we finish.  If
    // you want to keep it for inspection instead, comment out the following
    // line and then remove the comment markers on the two lines after that.
    let tempdir_path = tempdir.path();
    //let tempdir_path = tempdir.into_path();
    //println!("{}", tempdir_path.display());

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    let input_file = File::create(&input_path).unwrap();
    let output_path = tempdir_path.join("output.csv");

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_path,
                        "follow": true
                    }
                },
                "format": {
                    "name": "csv"
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path,
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }
        }
    }))
    .unwrap();

    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);

    // Number of records written to the input.
    let mut total_records = 0usize;

    // Start pipeline.
    println!("start pipeline");
    let mut controller = Controller::with_test_config_keep_program_diff(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &[],
                &[Some("v0")],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();
    controller.start();

    let mut prev_output = 0;
    for (round, n_records) in rounds.iter().copied().enumerate() {
        println!("--- round {round}: add {n_records} records and suspend --- ");

        // Write records to the input file.
        println!(
            "Writing records {total_records}..{}",
            total_records + n_records
        );
        if n_records > 0 {
            for id in total_records..total_records + n_records {
                writer.serialize(TestStruct::for_id(id as u32)).unwrap();
            }
            writer.flush().unwrap();
            total_records += n_records;
        }

        // Wait for the new records to be processed.
        wait_for_output(
            &controller,
            prev_output,
            &(0..total_records)
                .map(|id| TestStruct::for_id(id as u32))
                .collect::<Vec<_>>(),
            &output_path,
        );
        prev_output += total_records;

        // Suspend.
        println!("suspend");
        controller.suspend().unwrap();

        // Stop controller.
        println!("stop controller");
        controller.stop().unwrap();

        // Resume modified pipeline.
        controller = Controller::with_test_config_keep_program_diff(
            move |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some(&format!("v{}", round + 1))],
                ))
            },
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
        controller.start();

        // Wait for the entire input to show up in the output stream.
        wait_for_output(
            &controller,
            prev_output,
            &(0..total_records)
                .map(|id| TestStruct::for_id(id as u32))
                .collect::<Vec<_>>(),
            &output_path,
        );

        println!();
    }
}

fn wait_for_output(
    controller: &Controller,
    initial_n_records: usize,
    expected_output: &[TestStruct],
    csv_file_path: &Path,
) {
    let expect_n = initial_n_records + expected_output.len();

    println!("wait for records {initial_n_records}..{}", expect_n);

    let mut last_n = 0;

    // Make sure that the entire input shows up in the output file.
    wait(
        || {
            let n = controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records() as usize;
            if n > last_n {
                println!("received {n} records");
                last_n = n;
            }
            n >= expect_n
        },
        10_000,
    )
    .unwrap();

    // No more records should arrive, but give the controller some time
    // to send some more in case there's a bug.
    sleep(Duration::from_millis(100));

    // Then verify that the number is as expected.
    assert_eq!(
        controller
            .status()
            .output_status()
            .get(&0)
            .unwrap()
            .transmitted_records(),
        expect_n as u64
    );

    // Read output and compare. Our output adapter, which is not
    // fault-tolerant, truncates the output file to length 0 each
    // time. Therefore, the output file should contain all the records
    // in `checkpointed_records..total_records`.
    let mut actual = CsvReaderBuilder::new()
        .has_headers(false)
        .from_path(csv_file_path)
        .unwrap()
        .deserialize::<(TestStruct, i32)>()
        .map(|res| {
            let (val, weight) = res.unwrap();
            assert_eq!(weight, 1);
            val
        })
        .collect::<Vec<_>>();
    actual.sort();

    assert_eq!(actual.len(), expected_output.len());
    for (record, expect_record) in actual.into_iter().zip(expected_output.iter().cloned()) {
        assert_eq!(record, expect_record);
    }
}

#[test]
fn suspend() {
    test_suspend(&[2500, 2500, 2500, 2500, 2500]);
}

#[test]
fn suspend_empty() {
    test_suspend(&[0, 2500, 0, 2500, 0, 0, 0, 2500, 2500, 2500, 0]);
}

fn input_path(storage_dir: &Path, i: usize) -> PathBuf {
    storage_dir.join(format!("input{}.csv", i + 1))
}

fn output_path(storage_dir: &Path, i: usize) -> PathBuf {
    storage_dir.join(format!("output{}.csv", i + 1))
}

#[test]
fn barrier_input_batch_wakes_when_endpoint_already_has_buffered_input() {
    init_test_logger();

    // During suspend, every new batch from a barrier endpoint can be the batch
    // that clears the barrier.
    //
    // https://github.com/feldera/feldera/actions/runs/26535433930/job/78166265316
    // That test likely failed because the endpoint already had buffered
    // barrier input, so the old > 0 condition was not enough to wake the
    // circuit thread.
    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    File::create(input_path(&storage_dir, 0)).unwrap();

    let controller = start_controller(&storage_dir, &[0]);

    let endpoint_id = {
        let input_status = controller.status().input_status();
        *input_status.keys().next().unwrap()
    };
    {
        let input_status = controller.status().input_status();
        let endpoint = input_status.get(&endpoint_id).unwrap();
        endpoint.set_barrier(true);
        endpoint
            .metrics
            .buffered_records
            .store(1, Ordering::Relaxed);
    }

    let parker = Parker::new();
    let unparker = parker.unparker().clone();
    let buffered_input = controller.status().input_batch_from_endpoint(
        endpoint_id,
        BufferSize {
            records: 1,
            bytes: 1,
        },
        &unparker,
    );

    controller.stop().unwrap();

    assert_eq!(buffered_input, BufferedInput::Barrier);
}

fn start_controller(storage_dir: &Path, barriers: &[usize]) -> Controller {
    let n = barriers.len();
    for (i, barrier) in barriers.iter().copied().enumerate() {
        set_barrier(input_path(storage_dir, i).to_str().unwrap(), barrier);
    }

    let mut config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "clock_resolution_usecs": null,
        "inputs": {}
    }))
    .unwrap();

    for i in 0..n {
        config.inputs.insert(
            format!("test_input{}", i + 1).into(),
            serde_json::from_value(json!({
                "stream": format!("test_input{}", i + 1),
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": input_path(storage_dir, i),
                        "follow": true
                    }
                },
                "format": {
                    "name": "csv"
                }
            }))
            .unwrap(),
        );

        let output_path = output_path(storage_dir, i);
        let _ = std::fs::remove_file(&output_path);
        config.outputs.insert(
            format!("test_output{}", i + 1).into(),
            serde_json::from_value(json!({
                "stream": format!("test_output{}", i + 1),
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path,
                    }
                },
                "format": {
                    "name": "csv"
                }
            }))
            .unwrap(),
        );
    }

    let controller = Controller::with_test_config(
        move |circuit_config| {
            let persistent_output_ids = (1..=n).map(|i| format!("output{i}")).collect::<Vec<_>>();
            let persistent_strs = persistent_output_ids
                .iter()
                .map(|s| Some(s.as_str()))
                .collect::<Vec<_>>();

            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &[],
                &persistent_strs,
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    controller
}

/// Test suspend when barriers are involved, with one input
///
/// An input endpoint that supports suspend and resume can emit a barrier,
/// specifically [feldera_adapterlib::transport::Resume::Barrier], when it
/// can't suspend after a particular step.  When a suspend is requested and
/// there's a barrier, the controller advances the input endpoints with
/// barriers (and only those input endpoints) until past the barrier.
///
/// This tests:
///
/// - A barrier on one input that is released after one step.
///
/// - A barrier on one input that is released after multiple steps.
#[test]
#[cfg_attr(target_arch = "aarch64", ignore)]
fn suspend_barrier() {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();

    // This allows the temporary directory to be deleted when we finish.  If
    // you want to keep it for inspection instead, comment out the following
    // line and then remove the comment markers on the two lines after that.
    let tempdir_path = tempdir.path();
    //let tempdir_path = tempdir.into_path();
    //println!("{}", tempdir_path.display());

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_file = File::create(input_path(&storage_dir, 0)).unwrap();

    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);

    // Write records to the input file.
    println!("Writing records 0..4000");
    for id in 0..4000 {
        writer.serialize(TestStruct::for_id(id as u32)).unwrap();
    }
    writer.flush().unwrap();

    // Start pipeline.
    println!("start pipeline");

    let controller = start_controller(&storage_dir, &[5000]);

    // Wait for the records that are not in the checkpoint to be
    // processed or replayed.
    println!("wait for 4000 records 0..4000");
    wait_for_records(&controller, &[4000]);

    // Suspend.
    let (sender, receiver) = mpsc::channel();
    println!("start suspend");
    let suspend_request_step = controller.status().global_metrics.total_initiated_steps();
    controller.start_suspend(Box::new(move |result| sender.send(result).unwrap()));

    // Suspend should not succeed, because of the barrier.
    sleep(Duration::from_millis(1000));
    receiver.try_recv().unwrap_err();

    // Write 1000 more records.
    for id in 4000..5000 {
        writer.serialize(TestStruct::for_id(id as u32)).unwrap();
    }
    writer.flush().unwrap();

    // Suspend should now succeed, because we crossed the barrier.
    receiver
        .recv_timeout(Duration::from_millis(10000))
        .unwrap()
        .unwrap();
    assert_bounded_suspend_steps(&controller, suspend_request_step);

    // Stop controller.
    println!("stop controller");
    controller.stop().unwrap();

    // Read output and compare. Our output adapter, which is not
    // fault-tolerant, truncates the output file to length 0 each
    // time. Therefore, the output file should contain all the records
    // in `0..5000`
    check_file_contents(&output_path(&storage_dir, 0), 0..5000);

    // Restart the controller.  It should output no records initially
    // because the checkpoint covered all of them.
    let controller = start_controller(&storage_dir, &[5000]);
    wait_for_records(&controller, &[5000]);

    println!("start suspend");
    let (sender, receiver) = mpsc::channel();
    let mut sender = Some(sender);

    for i in 0..5 {
        // Make sure suspend hasn't completed yet.
        receiver.try_recv().unwrap_err();

        let start = (i + 5) * 1000;
        let end = start + 1000;
        println!("writing records {start}..{end}");
        for id in start..end {
            writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        }
        writer.flush().unwrap();
        println!("waiting for {end} records");
        wait_for_records(&controller, &[end]);
        check_file_contents(&(output_path(&storage_dir, 0)), 5000..end);

        if let Some(sender) = sender.take() {
            // Start suspend.  It should not succeed, because of the barrier.
            //
            // (We need to output some records before we do this, otherwise
            // it would succeed immediately because we haven't stepped past
            // the checkpoint.)
            controller.start_suspend(Box::new(move |result| sender.send(result).unwrap()));
        }
    }

    // Suspend should now succeed, because we crossed the barrier.
    receiver
        .recv_timeout(Duration::from_millis(10000))
        .unwrap()
        .unwrap();
}

/// Test suspend when barriers are involved, with multiple inputs.
///
/// An input endpoint that supports suspend and resume can emit a barrier,
/// specifically [feldera_adapterlib::transport::Resume::Barrier], when it
/// can't suspend after a particular step.  When a suspend is requested and
/// there's a barrier, the controller advances the input endpoints with
/// barriers (and only those input endpoints) until past the barrier.
///
/// This tests writing to multiple inputs that all have different barriers,
/// and ensures that suspend occurs as soon as all the barriers are
/// released, and that the other inputs don't advance further than needed
/// for their barriers.
fn suspend_multiple_barriers(n_inputs: usize) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();

    // This allows the temporary directory to be deleted when we finish.  If
    // you want to keep it for inspection instead, comment out the following
    // line and then remove the comment markers on the two lines after that.
    let tempdir_path = tempdir.path();
    //let tempdir_path = tempdir.into_path();
    //println!("{}", tempdir_path.display());

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();

    let input_files = (0..n_inputs)
        .map(|i| File::create(input_path(&storage_dir, i)).unwrap())
        .collect::<Vec<_>>();

    let mut writers = (0..n_inputs)
        .map(|i| {
            CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(&input_files[i])
        })
        .collect::<Vec<_>>();

    // Write records to the input files.
    println!("Writing 1000 records to each of {n_inputs} files");
    for writer in writers.iter_mut().take(n_inputs) {
        for id in 0..1000 {
            writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        }
        writer.flush().unwrap();
    }

    // Start pipeline.
    println!("start pipeline");

    // The barrier for input 0 is record 0,
    // for input 1 is record 1000,
    // for input 2 is record 2000,
    // and so on.
    let barriers = (0..n_inputs).map(|i| i * 1000).collect::<Vec<_>>();
    let controller = start_controller(&storage_dir, &barriers);

    // Wait for the first 1000 records in each file to be read and copied to the
    // output.
    println!("wait for 1000 records in each file");
    let mut written = repeat_n(1000, n_inputs).collect::<Vec<_>>();
    wait_for_records(&controller, &written);

    // Start a suspend operation.
    //
    // If we have 1 or 2 inputs, the suspend will complete quickly, because
    // we're past all the barriers; otherwise, it will not complete due to
    // barriers, since each input only has 1000 records so far.
    let (sender, receiver) = mpsc::channel();
    println!("start suspend");
    let suspend_request_step = controller.status().global_metrics.total_initiated_steps();
    controller.start_suspend(Box::new(move |result| sender.send(result).unwrap()));

    // Iterate as long as we shouldn't have reached the barrier, adding records
    // round-robin to input 0, then to input 1, and so on, 1000 records at a
    // time.
    fn expectations(written: &[usize], barriers: &[usize]) -> Vec<usize> {
        written
            .iter()
            .zip(barriers.iter())
            .map(|(&written, &barrier)| min(written, barrier).max(1000))
            .collect()
    }
    let mut next = 0;
    while written
        .iter()
        .zip(barriers.iter())
        .any(|(&written, &barrier)| written < barrier)
    {
        // Suspend shouldn't have succeeded; check.
        sleep(Duration::from_millis(1000));
        receiver.try_recv().unwrap_err();

        // Write 1000 more records to the `next` input.
        println!("writing 1000 more records to test_input{}", next + 1);
        for id in written[next]..written[next] + 1000 {
            writers[next]
                .serialize(TestStruct::for_id(id as u32))
                .unwrap();
        }
        writers[next].flush().unwrap();
        written[next] += 1000;
        next = (next + 1) % n_inputs;

        // Wait for the records to be written to the output, then check that
        // we got exactly what we expect on output.
        //
        // We won't get any more records on output from inputs that have reached
        // their barrier, so the writes to inputs 0 and 1 won't have any effect
        // here.
        println!("total written: {written:?}");
        let expect = expectations(&written, &barriers);
        wait_for_records(&controller, &expect);
        for (i, expectation) in expect.iter().enumerate().take(n_inputs) {
            check_file_contents(&output_path(&storage_dir, i), 0..*expectation);
        }
    }

    // Suspend should now succeed, because we crossed the barrier.
    receiver
        .recv_timeout(Duration::from_millis(10000))
        .unwrap()
        .unwrap();
    assert_bounded_suspend_steps(&controller, suspend_request_step);

    // Stop controller.
    println!("stop controller");
    controller.stop().unwrap();

    // Check output one more time.
    println!("check output one more time now that controller is stopped");
    let expect = expectations(&written, &barriers);
    for (i, e) in expect.iter().enumerate().take(n_inputs) {
        check_file_contents(&output_path(&storage_dir, i), 0..*e);
    }

    // Now restart the controller and wait for all the records that we wrote
    // beyond the barriers get copied to output (and nothing else).
    println!("restart controller and wait for records beyond the barriers");
    let controller = start_controller(&storage_dir, &barriers);
    wait_for_records(&controller, &written);
    for i in 0..n_inputs {
        check_file_contents(&output_path(&storage_dir, i), expect[i]..written[i]);
    }
}

#[test]
fn suspend_barrier2() {
    suspend_multiple_barriers(2);
}

#[test]
fn suspend_barrier3() {
    suspend_multiple_barriers(3);
}

#[test]
fn suspend_barrier4() {
    suspend_multiple_barriers(4);
}

#[test]
fn suspend_barrier5() {
    suspend_multiple_barriers(5);
}

#[test]
fn bootstrap() {
    test_bootstrap(&[2500, 2500, 2500, 2500, 2500]);
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
      "to": "11"
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
      "to": "9"
    },
    {
      "from": "7",
      "stream_id": 8,
      "to": "12"
    },
    {
      "from": "9",
      "stream_id": 9,
      "to": "10"
    },
    {
      "from": "12",
      "stream_id": 10,
      "to": "13"
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
      "implements": [
        "input.output"
      ],
      "operation": "Apply"
    },
    {
      "id": "10",
      "implements": [
        "input.output"
      ],
      "operation": "Output"
    },
    {
      "id": "11",
      "implements": [
        "output"
      ],
      "operation": "AccumulateOutput"
    },
    {
      "id": "12",
      "implements": [
        "output"
      ],
      "operation": "Apply"
    },
    {
      "id": "13",
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
        status.add_output(&0, "http_output", &output_config, None);

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
    status.add_output(&0, "mock_output", &output_config, None);
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

#[test]
fn test_preprocessor() {
    init_test_logger();

    // Create test JSON data with various strings that will be filtered and transformed
    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    // Controller configuration with a preprocessor :
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                },
                "preprocessor": [
                    {
                        "name": "passthrough",
                        "message_oriented": true,
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            // Register the factory before connectors are initialized.
            catalog
                .preprocessor_registry()
                .lock()
                .unwrap()
                .register("passthrough", Box::new(PassthroughPreprocessorFactory));
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

#[test]
fn test_decryption_preprocessor() {
    use crate::preprocess::aes256gcm_encrypt;

    init_test_logger();

    // AES-256 key (32 bytes) and nonce (12 bytes).
    let key = b"0123456789abcdef0123456789abcdef";
    let nonce = b"test_nonce_1";

    // Encrypt JSON data that the pipeline will decrypt and parse.
    let json_input = br#"[
        {"id": 1, "b": true, "s": "one"},
        {"id": 2, "b": false, "s": "two"}
    ]"#;
    let encrypted = aes256gcm_encrypt(key, nonce, json_input);

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file.as_file().write_all(&encrypted).unwrap();

    let key_b64 = BASE64.encode(key);

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_decryption_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                },
                "preprocessor": [
                    {
                        "name": "decryption",
                        "message_oriented": true,
                        "config": {
                            "key": key_b64
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            // Register the factory before connectors are initialized.
            catalog
                .preprocessor_registry()
                .lock()
                .unwrap()
                .register("decryption", Box::new(DecryptionPreprocessorFactory));
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

// ---------------------------------------------------------------------------
// Helpers for `test_base64_csv_preprocessor`
// ---------------------------------------------------------------------------

use crate::format::{LineSplitter, SpongeSplitter};
use feldera_adapterlib::format::Splitter;
use feldera_adapterlib::preprocess::{Preprocessor, PreprocessorCreateError, PreprocessorFactory};
use feldera_types::preprocess::PreprocessorConfig;

/// Preprocessor that base64-decodes each line it receives.
///
/// The [`NewlineSplitter`] above ensures that `process` is called with exactly
/// one base64-encoded line (including the trailing `\n`) at a time.
/// `process` strips the newline and decodes the rest, returning the original
/// CSV bytes (e.g. `b"1,true,,one\n"`).
#[cfg(test)]
struct Base64LinePreprocessor;

#[cfg(test)]
impl Preprocessor for Base64LinePreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<crate::format::ParseError>) {
        let without_newline = data.strip_suffix(b"\n").unwrap_or(data);
        match BASE64.decode(without_newline) {
            Ok(decoded) => (decoded, vec![]),
            Err(e) => (
                vec![],
                vec![crate::format::ParseError::bin_envelope_error(
                    format!("base64 decode error: {e}"),
                    without_newline,
                    None,
                )],
            ),
        }
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Base64LinePreprocessor)
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(LineSplitter))
    }
}

#[cfg(test)]
struct Base64LinePreprocessorFactory;

#[cfg(test)]
impl PreprocessorFactory for Base64LinePreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(Base64LinePreprocessor))
    }
}

/// Verify that [`PreprocessedParser`] exercises both the preprocessor splitter
/// and the parser splitter in the same pipeline.
///
/// **Input format**: the file contains newline-separated base64-encoded CSV rows.
///
/// **Preprocessor splitter** (`NewlineSplitter`): the file transport calls
/// `PreprocessedParser::splitter()`, which returns the preprocessor's splitter.
/// That splitter finds `\n` boundaries in the raw byte stream so that each
/// call to `PreprocessedParser::parse` receives exactly one base64-encoded
/// line.
///
/// **Parser splitter** (`CsvSplitter`): inside `PreprocessedParser::parse`,
/// after `process` decodes a base64 line back to a CSV row (e.g.
/// `b"1,true,,one\n"`), the result is appended to the internal
/// `StreamSplitter`, which uses the CSV parser's own splitter to find the
/// CSV record boundary before forwarding the complete row to `CsvParser`.
#[test]
fn test_base64_csv_preprocessor() {
    init_test_logger();

    // Two CSV rows for `TestStruct { id: u32, b: bool, i: Option<i64>, s: String }`.
    // `i` is None, which serialises as an empty field in CSV.
    let rows: &[&[u8]] = &[b"1,true,,one\n", b"2,false,,two\n"];

    // Build the input file: each CSV row is base64-encoded, one per line.
    let mut file_contents = Vec::new();
    for row in rows {
        file_contents.extend_from_slice(BASE64.encode(row).as_bytes());
        file_contents.push(b'\n');
    }
    // skip newline after last row
    file_contents.pop();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file.as_file().write_all(&file_contents).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_base64_csv_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "preprocessor": [
                    {
                        "name": "base64LinePreprocessor",
                        "message_oriented": true,
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            catalog.preprocessor_registry().lock().unwrap().register(
                "base64LinePreprocessor",
                Box::new(Base64LinePreprocessorFactory),
            );
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Preprocessor that base64-decodes the input it receives.
#[cfg(test)]
struct Base64SpongePreprocessor;

#[cfg(test)]
impl Preprocessor for Base64SpongePreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<crate::format::ParseError>) {
        let without_newline = data.strip_suffix(b"\n").unwrap_or(data);
        match BASE64.decode(without_newline) {
            Ok(decoded) => (decoded, vec![]),
            Err(e) => (
                vec![],
                vec![crate::format::ParseError::bin_envelope_error(
                    format!("base64 decode error: {e}"),
                    without_newline,
                    None,
                )],
            ),
        }
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Base64SpongePreprocessor)
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(SpongeSplitter))
    }
}

#[cfg(test)]
struct Base64SpongePreprocessorFactory;

#[cfg(test)]
impl PreprocessorFactory for Base64SpongePreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(Base64SpongePreprocessor))
    }
}

/// Like the previous test, but use a SpongeSplitter in the preprocessor and
/// uudecode everything, not at line boundaries.
#[test]
fn test_base64_sponge_csv_preprocessor() {
    init_test_logger();

    // Two CSV rows for `TestStruct { id: u32, b: bool, i: Option<i64>, s: String }`.
    // `i` is None, which serialises as an empty field in CSV.
    let rows: &[u8] = b"1,true,,one\n2,false,,two";

    // Build the input file: each CSV row is base64-encoded, one per line.
    let mut file_contents = Vec::new();
    file_contents.extend_from_slice(BASE64.encode(rows).as_bytes());

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file.as_file().write_all(&file_contents).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_base64_sponge_csv_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "preprocessor": [
                    {
                        "name": "base64SpongePreprocessor",
                        "message_oriented": true,
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            catalog.preprocessor_registry().lock().unwrap().register(
                "base64SpongePreprocessor",
                Box::new(Base64SpongePreprocessorFactory),
            );
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Tests that `checkpoint_activity()` returns correct states and timestamps
/// during a coordinated checkpoint flow (prepare → delayed → in_progress → idle).
#[test]
fn test_checkpoint_activity_state_transitions() {
    use chrono::Utc;
    use feldera_types::checkpoint::CheckpointActivity;
    use feldera_types::coordination::CheckpointCoordination;
    use feldera_types::suspend::TemporarySuspendError;

    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();

    let temp_input_file = NamedTempFile::new().unwrap();
    {
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_input_file.as_file());
        for id in 0..10u32 {
            writer.serialize(TestStruct::for_id(id)).unwrap();
        }
        writer.flush().unwrap();
    }

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_checkpoint_activity",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "fault_tolerance": {},
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": true
                    }
                },
                "format": {
                    "name": "csv"
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": tempdir.path().join("output.csv"),
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }
        }
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

    controller.start();

    // Wait for data to be processed.
    wait_for_records(&controller, &[10]);

    // --- Before any checkpoint, activity should be Idle ---
    let activity = controller.checkpoint_activity();
    assert!(
        matches!(activity, CheckpointActivity::Idle),
        "expected Idle before checkpoint, got {activity:?}"
    );
    assert!(controller.checkpoint_delay_started().is_none());
    assert!(controller.checkpoint_started().is_none());

    // --- Initiate a coordinated (prepare) checkpoint ---
    // This sets coordination_prepare_checkpoint = true, which will
    // cause the checkpoint to stall in "Ready" state (only the
    // Coordination temporary error remains).
    let before_prepare = Utc::now();
    controller.prepare_checkpoint();

    // Wait for the checkpoint coordination to reach Ready state.
    // In Ready state, only TemporarySuspendError::Coordination is blocking.
    wait(
        || {
            matches!(
                *controller.checkpoint_watcher().borrow(),
                Some(CheckpointCoordination::Ready)
            )
        },
        10_000,
    )
    .expect("checkpoint coordination should reach Ready state");

    // At this point, checkpoint_activity() should report Delayed with
    // the Coordination reason.
    let activity = controller.checkpoint_activity();
    match &activity {
        CheckpointActivity::Delayed {
            reasons,
            delayed_since,
        } => {
            assert!(
                reasons
                    .iter()
                    .any(|r| matches!(r, TemporarySuspendError::Coordination)),
                "expected Coordination reason in {reasons:?}"
            );
            // The delay timestamp should have been set around our prepare time.
            assert!(
                *delayed_since >= before_prepare,
                "delayed_since {delayed_since:?} should be >= before_prepare {before_prepare:?}"
            );
        }
        other => panic!("expected Delayed, got {other:?}"),
    }

    // checkpoint_delay_started should be set, checkpoint_started should not.
    assert!(
        controller.checkpoint_delay_started().is_some(),
        "checkpoint_delay_started should be set in Delayed state"
    );
    assert!(
        controller.checkpoint_started().is_none(),
        "checkpoint_started should not be set before InProgress"
    );

    // --- Release the checkpoint so it proceeds to InProgress/Done ---
    controller.release_checkpoint();

    // Wait for the checkpoint to complete (Done).
    wait(
        || {
            matches!(
                *controller.checkpoint_watcher().borrow(),
                Some(CheckpointCoordination::Done)
            )
        },
        10_000,
    )
    .expect("checkpoint coordination should reach Done state");

    // After completion, activity should be Idle and timestamps cleared.
    let activity = controller.checkpoint_activity();
    assert!(
        matches!(activity, CheckpointActivity::Idle),
        "expected Idle after checkpoint done, got {activity:?}"
    );
    assert!(
        controller.checkpoint_delay_started().is_none(),
        "checkpoint_delay_started should be cleared after Done"
    );
    assert!(
        controller.checkpoint_started().is_none(),
        "checkpoint_started should be cleared after Done"
    );

    controller.stop().unwrap();
}

/// Tests that `checkpoint_activity()` transitions through InProgress state
/// during a `start_checkpoint()` call, and that the `checkpoint_started`
/// timestamp is set during the write.
#[test]
fn test_checkpoint_activity_async_checkpoint() {
    use chrono::Utc;
    use feldera_types::checkpoint::CheckpointActivity;
    use feldera_types::coordination::CheckpointCoordination;

    init_test_logger();

    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();

    let temp_input_file = NamedTempFile::new().unwrap();
    {
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(temp_input_file.as_file());
        for id in 0..10u32 {
            writer.serialize(TestStruct::for_id(id)).unwrap();
        }
        writer.flush().unwrap();
    }

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_checkpoint_activity_async",
        "workers": 4,
        "storage_config": {
            "path": storage_dir,
        },
        "storage": true,
        "fault_tolerance": {},
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": true
                    }
                },
                "format": {
                    "name": "csv"
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": tempdir.path().join("output.csv"),
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }
        }
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

    controller.start();
    wait_for_records(&controller, &[10]);

    // Use the async start_checkpoint so we can observe the InProgress state
    // from another thread.
    let (sender, receiver) = oneshot::channel();
    let before_checkpoint = Utc::now();
    controller.start_checkpoint(Box::new(move |result| sender.send(result).unwrap()));

    // Poll until we see either InProgress or Done.  The checkpoint may
    // complete very quickly, so we accept either.
    let mut saw_in_progress = false;
    wait(
        || {
            let coord = controller.checkpoint_watcher().borrow().clone();
            if matches!(coord, Some(CheckpointCoordination::InProgress)) {
                saw_in_progress = true;
                // Also verify the timestamp.
                if let Some(started) = controller.checkpoint_started() {
                    assert!(
                        started >= before_checkpoint,
                        "checkpoint_started {started:?} should be >= before_checkpoint {before_checkpoint:?}"
                    );
                }
            }
            matches!(coord, Some(CheckpointCoordination::Done))
        },
        10_000,
    )
    .expect("checkpoint should complete");

    // After completion, both timestamps should be cleared.
    assert!(
        controller.checkpoint_delay_started().is_none(),
        "checkpoint_delay_started should be cleared after checkpoint"
    );
    assert!(
        controller.checkpoint_started().is_none(),
        "checkpoint_started should be cleared after checkpoint"
    );

    // Activity should be Idle.
    let activity = controller.checkpoint_activity();
    assert!(
        matches!(activity, CheckpointActivity::Idle),
        "expected Idle after checkpoint, got {activity:?}"
    );

    // The checkpoint should have succeeded.
    receiver.blocking_recv().unwrap().unwrap();

    controller.stop().unwrap();
}

/// Tests that `permanent_suspend_errors()` returns errors when storage
/// is explicitly disabled (the pipeline cannot checkpoint).
#[test]
fn test_permanent_suspend_errors_without_storage() {
    use feldera_types::suspend::PermanentSuspendError;

    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(b"1,true,,foo\n")
        .unwrap();

    // Storage explicitly disabled — pipeline cannot checkpoint.
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_no_storage",
        "workers": 1,
        "storage": false,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "csv"
                }
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    // With storage disabled, permanent_suspend_errors should report StorageRequired.
    let errors = controller.permanent_suspend_errors();
    assert!(
        errors.is_some(),
        "expected permanent errors with storage disabled"
    );
    let errors = errors.unwrap();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, PermanentSuspendError::StorageRequired)),
        "expected StorageRequired error in {errors:?}"
    );

    // checkpoint_activity should still be Idle (no checkpoint was requested).
    let activity = controller.checkpoint_activity();
    assert!(
        matches!(
            activity,
            feldera_types::checkpoint::CheckpointActivity::Idle
        ),
        "expected Idle, got {activity:?}"
    );

    controller.stop().unwrap();
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
        InputEndpointStatus::new("test_endpoint", config, None, None)
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

    /// When `max_queued_bytes` is unset, it defaults to `max_queued_records * 1000`.
    #[test]
    fn max_queued_bytes_defaults_to_1000x_records() {
        let endpoint = make_endpoint(100, None);
        assert_eq!(endpoint.config.connector_config.max_queued_bytes(), 100_000);
    }

    /// When `max_queued_bytes` is explicitly set, that value is used directly.
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

    /// With no explicit `max_queued_bytes`, the default byte threshold
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
    /// byte count to cross `max_queued_bytes`, even when the records count
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

// ---------------------------------------------------------------------------
// Postprocessor integration tests
// ---------------------------------------------------------------------------

use feldera_adapterlib::postprocess::{
    Postprocessor, PostprocessorCreateError, PostprocessorFactory,
};
use feldera_types::postprocess::PostprocessorConfig;

/// Verify that a passthrough postprocessor does not alter output.
///
/// The output JSON produced by the encoder passes through the postprocessor
/// unchanged and lands in the output file as valid JSON that the query engine
/// can read back.
#[test]
fn test_postprocessor() {
    use crate::postprocess::PassthroughPostprocessorFactory;

    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_postprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
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
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "postprocessor": [
                    {
                        "name": "passthrough",
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            catalog
                .postprocessor_registry()
                .lock()
                .unwrap()
                .register("passthrough", Box::new(PassthroughPostprocessorFactory));
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Verify that the encryption postprocessor encrypts output and the file
/// cannot be parsed as plain JSON.  Decrypt the raw file bytes and confirm
/// that the decrypted payload matches what the pipeline produced.
#[test]
fn test_encryption_postprocessor() {
    use crate::postprocess::EncryptionPostprocessorFactory;
    use openssl::symm::{Cipher, decrypt_aead};

    init_test_logger();

    let key = b"0123456789abcdef0123456789abcdef"; // 32 bytes
    let nonce = b"test_nonce_1"; // 12 bytes

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let key_b64 = BASE64.encode(key);
    let nonce_b64 = BASE64.encode(nonce);

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_encryption_postprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
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
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path.clone() }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "postprocessor": [
                    {
                        "name": "encryption",
                        "config": {
                            "key": key_b64,
                            "nonce": nonce_b64
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            catalog
                .postprocessor_registry()
                .lock()
                .unwrap()
                .register("encryption", Box::new(EncryptionPostprocessorFactory));
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();
    controller.stop().unwrap();

    // Read the raw output file and decrypt it.
    let encrypted = std::fs::read(&output_path).unwrap();
    assert!(
        !encrypted.is_empty(),
        "output file should not be empty after encryption"
    );

    // The encrypted blob is not valid UTF-8 CSV.
    assert!(
        std::str::from_utf8(&encrypted).is_err() || !encrypted.contains(&b','),
        "encrypted output should not look like plain CSV"
    );

    // Decrypt and verify the result contains CSV records.
    let enc_nonce = &encrypted[..12];
    let tag_start = encrypted.len() - 16;
    let ciphertext = &encrypted[12..tag_start];
    let tag = &encrypted[tag_start..];
    let plaintext = decrypt_aead(
        Cipher::aes_256_gcm(),
        key,
        Some(enc_nonce),
        &[],
        ciphertext,
        tag,
    )
    .expect("decryption of postprocessed output failed");

    // The decrypted payload must be parseable CSV with 2 records.
    let mut rdr = CsvReaderBuilder::new()
        .has_headers(false)
        .from_reader(plaintext.as_slice());
    let rows: Vec<_> = rdr.records().collect();
    assert_eq!(rows.len(), 2, "expected 2 rows in decrypted CSV output");
}

/// Verify that a base64-encoding postprocessor transforms the output and that
/// decoding it yields the original JSON payload.
#[cfg(test)]
struct Base64EncodePostprocessor;

#[cfg(test)]
impl Postprocessor for Base64EncodePostprocessor {
    fn push_buffer(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(BASE64.encode(data).into_bytes())
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(Base64EncodePostprocessor)
    }
}

#[cfg(test)]
struct Base64EncodePostprocessorFactory;

#[cfg(test)]
impl PostprocessorFactory for Base64EncodePostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(Base64EncodePostprocessor))
    }
}

/// Verify that a custom postprocessor can transform output bytes and that
/// decoding the result yields the original JSON payload.
#[test]
fn test_base64_postprocessor() {
    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_base64_postprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
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
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path.clone() }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "postprocessor": [
                    {
                        "name": "base64Encode",
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            catalog
                .postprocessor_registry()
                .lock()
                .unwrap()
                .register("base64Encode", Box::new(Base64EncodePostprocessorFactory));
            Ok((circuit, catalog))
        },
        &config,
        Box::new(|e, _| panic!("error: {e}")),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();
    controller.stop().unwrap();

    // Read the raw output and base64-decode it; the result must be parseable CSV.
    let encoded = std::fs::read(&output_path).unwrap();
    assert!(
        !encoded.is_empty(),
        "output file should not be empty after base64 encoding"
    );

    let decoded = BASE64
        .decode(&encoded)
        .expect("output file should be valid base64");
    let mut rdr = CsvReaderBuilder::new()
        .has_headers(false)
        .from_reader(decoded.as_slice());
    let rows: Vec<_> = rdr.records().collect();
    assert_eq!(rows.len(), 2, "expected 2 rows in decoded CSV output");
}

/// A postprocessor that rejects every record with an error, used in
/// [`test_postprocessor_error_reported`].
#[cfg(test)]
struct AlwaysErrorPostprocessor;

#[cfg(test)]
impl Postprocessor for AlwaysErrorPostprocessor {
    fn push_buffer(&mut self, _buffer: &[u8]) -> anyhow::Result<Vec<u8>> {
        Err(anyhow::anyhow!("deliberate postprocessor error"))
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(AlwaysErrorPostprocessor)
    }
}

#[cfg(test)]
struct AlwaysErrorPostprocessorFactory;

#[cfg(test)]
impl PostprocessorFactory for AlwaysErrorPostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(AlwaysErrorPostprocessor))
    }
}

/// Verify that a postprocessor error is forwarded to the controller error callback.
///
/// When [`Postprocessor::push_buffer`] returns [`Err`], the record is dropped
/// and the error is delivered to the controller's error callback as a non-fatal
/// [`ControllerError::OutputTransportError`].  The pipeline must continue
/// running without panicking or hanging.
#[test]
fn test_postprocessor_error_reported() {
    use crate::controller::ControllerError;
    use std::sync::{Arc, Mutex};

    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_postprocessor_error_reported",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": temp_input_file.path(), "follow": false }
                },
                "format": {
                    "name": "json",
                    "config": { "array": true, "update_format": "raw" }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path }
                },
                "format": { "name": "csv", "config": {} },
                "postprocessor": [{ "name": "always_error", "config": {} }]
            }
        }
    }))
    .unwrap();

    let captured: Arc<Mutex<Vec<Arc<ControllerError>>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();

    let controller = Controller::with_test_config(
        |circuit_config| {
            let (circuit, catalog) =
                test_circuit::<TestStruct>(circuit_config, &TestStruct::schema(), &[None]);
            catalog
                .postprocessor_registry()
                .lock()
                .unwrap()
                .register("always_error", Box::new(AlwaysErrorPostprocessorFactory));
            Ok((circuit, catalog))
        },
        &config,
        Box::new(move |e, _| {
            captured_clone.lock().unwrap().push(e);
        }),
    )
    .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), DEFAULT_TIMEOUT_MS).unwrap();
    controller.stop().unwrap();

    let errors = captured.lock().unwrap();
    assert!(
        !errors.is_empty(),
        "error callback should have been invoked at least once by the postprocessor"
    );
    for err in errors.iter() {
        let ControllerError::OutputTransportError {
            endpoint_name,
            fatal,
            ..
        } = err.as_ref()
        else {
            panic!("expected OutputTransportError, got: {err}");
        };
        assert_eq!(endpoint_name, "test_output1");
        assert!(!fatal, "postprocessor errors must be non-fatal");
    }
}

/// Verify that attaching a postprocessor to a `delta_table_output` (integrated)
/// connector is rejected at startup.
///
/// Integrated connectors own their own serialization pipeline and bypass the
/// postprocessor layer, so a postprocessor cannot be wired into them.  The
/// controller should return a [`ControllerError::PostprocessorCreateError`]
/// before the circuit starts running.
#[test]
fn test_postprocessor_on_delta_output_fails() {
    use crate::controller::ControllerError;

    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_postprocessor_delta_fails",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": "/dev/null", "follow": false }
                },
                "format": { "name": "csv" }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "delta_table_output",
                    "config": { "uri": "file:///tmp/test_delta_postprocessor" }
                },
                "postprocessor": [{ "name": "passthrough", "config": {} }]
            }
        }
    }))
    .unwrap();

    let err = Controller::with_test_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e, _| panic!("unexpected error callback: {e}")),
    )
    .err()
    .expect("expected an error when attaching a postprocessor to a delta_table_output connector");

    let ControllerError::PostprocessorCreateError {
        ref endpoint_name,
        ref error,
    } = err
    else {
        panic!("expected PostprocessorCreateError, got: {err}");
    };
    assert_eq!(endpoint_name, "test_output1");
    assert!(
        error.contains("delta_table_output"),
        "error should name the unsupported transport, got: {error}"
    );
}
