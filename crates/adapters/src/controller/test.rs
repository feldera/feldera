use crate::{
    test::{
        generate_test_batch, init_test_logger, test_circuit, wait, TestStruct, DEFAULT_TIMEOUT_MS,
    },
    transport::set_barrier,
    Controller, PipelineConfig,
};
use csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
use feldera_types::{
    config::{InputEndpointConfig, OutputEndpointConfig},
    constants::STATE_FILE,
};
use serde_json::json;
use std::{
    borrow::Cow,
    cmp::min,
    collections::BTreeMap,
    fs::{create_dir, remove_file, File},
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

    assert_eq!(&err.to_string(), "invalid controller configuration: cyclic 'start_after' dependency detected: endpoint 'test_input1.endpoint1' with label 'label1' waits for endpoint 'test_input1.endpoint2' with label 'label2', which waits for endpoint 'test_input1.endpoint1' with label 'label1'");
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
