use crate::{
    test::{
        generate_test_batch, init_test_logger, test_circuit, wait, TestStruct, DEFAULT_TIMEOUT_MS,
    },
    transport::set_barrier,
    Controller, PipelineConfig,
};
use csv::{ReaderBuilder as CsvReaderBuilder, WriterBuilder as CsvWriterBuilder};
use std::{
    cmp::min,
    fmt::Write as _,
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

    let config_str = r#"
name: test
workers: 4
inputs:
    test_input1.endpoint1:
        stream: test_input1
        labels:
            - label1
        start_after: label2
        transport:
            name: file_input
            config:
                path: "file1"
        format:
            name: json
            config:
                array: true
                update_format: raw
    test_input1.endpoint2:
        stream: test_input1
        labels:
            - label2
        start_after: label1
        transport:
            name: file_input
            config:
                path: file2
        format:
            name: json
            config:
                array: true
                update_format: raw
    "#
    .to_string();

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let Err(err) = Controller::with_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e| panic!("error: {e}")),
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
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
    test_input1.endpoint1:
        stream: test_input1
        transport:
            name: file_input
            labels:
                - backfill
            config:
                path: {:?}
        format:
            name: json
            config:
                array: true
                update_format: raw
    test_input1.endpoint2:
        stream: test_input1
        start_after: backfill
        transport:
            name: file_input
            config:
                path: {:?}
        format:
            name: json
            config:
                array: true
                update_format: raw
    "#,
        temp_input_file1.path().to_str().unwrap(),
        temp_input_file2.path().to_str().unwrap(),
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let controller = Controller::with_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e| panic!("error: {e}")),
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

        let config_str = format!(
            r#"
min_batch_size_records: {min_batch_size_records}
max_buffering_delay_usecs: {max_buffering_delay_usecs}
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {:?}
                buffer_size_bytes: {input_buffer_size_bytes}
                follow: false
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {:?}
        format:
            name: csv
            config:
                buffer_size_records: {output_buffer_size_records}
        "#,
        temp_input_file.path().to_str().unwrap(),
        output_path,
        );

        info!("input file: {}", temp_input_file.path().to_str().unwrap());
        info!("output file: {output_path}");
        let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
        let controller = Controller::with_config(
                |circuit_config| Ok(test_circuit::<TestStruct>(circuit_config, &[], &[None])),
                &config,
                Box::new(|e| panic!("error: {e}")),
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
    immedate_checkpoint: bool,
}

impl FtTestRound {
    fn with_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: true,
            pause_afterward: false,
            immedate_checkpoint: false,
        }
    }
    fn without_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: false,
            pause_afterward: false,
            immedate_checkpoint: false,
        }
    }
    fn with_pause_afterward(self) -> Self {
        Self {
            pause_afterward: true,
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
            immedate_checkpoint: true,
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
    let n = expect_n.len();
    let mut last_n = repeat_n(0, n).collect::<Vec<_>>();
    wait(
        || {
            let new_n = collect_endpoint_records(controller, n);
            for i in 0..n {
                if new_n[i] > last_n[i] {
                    println!("received {n} records on test_output{}", i + 1);
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
    let input_path = tempdir_path.join("input.csv");
    let input_file = File::create(&input_path).unwrap();
    let output_path = tempdir_path.join("output.csv");

    let config_str = format!(
        r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
fault_tolerance: {{}}
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {input_path:?}
                follow: true
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
            config:
        "#
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);

    // Number of records written to the input.
    let mut total_records = 0usize;

    // Number of input records included in the latest checkpoint (always <=
    // total_records).
    let mut checkpointed_records = 0usize;

    let mut paused = false;

    for (
        round,
        FtTestRound {
            n_records,
            do_checkpoint,
            pause_afterward,
            immedate_checkpoint,
        },
    ) in rounds.iter().cloned().enumerate()
    {
        println!(
            "--- round {round}: {}{}add {n_records} records{}, {} --- ",
            if paused { "unpause the input, " } else { "" },
            if immedate_checkpoint {
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
            }
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
        let controller = Controller::with_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();
        controller.start();

        let (sender, receiver) = oneshot::channel();
        if immedate_checkpoint {
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
        wait_for_records(&controller, &[expect_n]);

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
        if immedate_checkpoint {
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

        if do_checkpoint || immedate_checkpoint {
            checkpointed_records = total_records;
        }
        println!();
    }
}

fn _test_concurrent_init(max_parallel_connector_init: u64) {
    init_test_logger();

    // Two JSON files with a few records each.
    let (_temp_input_files, connectors): (Vec<_>, Vec<_>) = (0..100)
        .map(|i| {
            let file = NamedTempFile::new().unwrap();
            file.as_file()
                .write_all(&format!(r#"[{{"id": {i}, "b": true, "s": "foo"}}]"#).into_bytes())
                .unwrap();
            let path = file.path().to_str().unwrap();
            let config = format!(
                r#"
    test_input1.endpoint{i}:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {path:?}
        format:
            name: json
            config:
                array: true
                update_format: raw"#
            );
            (file, config)
        })
        .unzip();

    let connectors = connectors.join("\n");

    // Controller configuration with 100 input connectors.
    let config_str = format!(
        r#"
name: test
workers: 4
max_parallel_connector_init: {max_parallel_connector_init}
inputs:
{connectors}
    "#
    );

    println!("config: {config_str}");

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let controller = Controller::with_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e| panic!("error: {e}")),
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

    // Two JSON files with a few records each.
    let (_temp_input_files, connectors): (Vec<_>, Vec<_>) = (0..20)
        .map(|i| {
            let file = NamedTempFile::new().unwrap();
            file.as_file()
                .write_all(&format!(r#"[{{"id": {i}, "b": true, "s": "foo"}}]"#).into_bytes())
                .unwrap();
            let path = file.path().to_str().unwrap();
            let config = format!(
                r#"
    test_input1.endpoint{i}:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {path:?}
        format:
            name: json
            config:
                array: true
                update_format: raw"#
            );
            (file, config)
        })
        .unzip();

    let connectors = connectors.join("\n");

    // Controller configuration with two input connectors;
    // the second starts after the first one finishes.
    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
{connectors}
    test_input1.error_endpoint:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: path_does_not_exist
        format:
            name: json
            config:
                array: true
                update_format: raw"#
    );

    println!("config: {config_str}");

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let result = Controller::with_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &TestStruct::schema(),
                &[None],
            ))
        },
        &config,
        Box::new(|e| panic!("error: {e}")),
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

    let config_str = format!(
        r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {input_path:?}
                follow: true
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
            config:
        "#
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

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
        let controller = Controller::with_config(
            |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some("output")],
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();
        controller.start();

        // Wait for the records that are not in the checkpoint to be
        // processed or replayed.
        wait_for_output(
            &controller,
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

    let config_str = format!(
        r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: file_input
            config:
                path: {input_path:?}
                follow: true
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
            config:
        "#
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);

    // Number of records written to the input.
    let mut total_records = 0usize;

    // Start pipeline.
    println!("start pipeline");
    let mut controller = Controller::with_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &[],
                &[Some("v0")],
            ))
        },
        &config,
        Box::new(|e| panic!("error: {e}")),
    )
    .unwrap();
    controller.start();

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
            &(0..total_records)
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

        // Resume modified pipeline.
        controller = Controller::with_config(
            move |circuit_config| {
                Ok(test_circuit::<TestStruct>(
                    circuit_config,
                    &[],
                    &[Some(&format!("v{}", round + 1))],
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();
        controller.start();

        // Wait for the entire input to show up in the output stream.
        wait_for_output(
            &controller,
            &(0..total_records)
                .map(|id| TestStruct::for_id(id as u32))
                .collect::<Vec<_>>(),
            &output_path,
        );

        println!();
    }
}

fn wait_for_output(controller: &Controller, expected_output: &[TestStruct], csv_file_path: &Path) {
    let expect_n = expected_output.len();

    println!("wait for {expect_n} records");

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

    assert_eq!(actual.len(), expect_n);
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

    let mut config_str = format!(
        "\
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
clock_resolution_usecs: null
inputs:
"
    )
    .to_string();

    for i in 0..n {
        writeln!(
            &mut config_str,
            "    test_input{}:
        stream: test_input{}
        transport:
            name: file_input
            config:
                path: {:?}
                follow: true
        format:
            name: csv",
            i + 1,
            i + 1,
            input_path(storage_dir, i).to_str().unwrap()
        )
        .unwrap();
    }

    writeln!(&mut config_str, "outputs:").unwrap();
    for i in 0..n {
        let output_path = output_path(storage_dir, i);
        let _ = std::fs::remove_file(&output_path);
        writeln!(
            &mut config_str,
            "    test_output{}:
        stream: test_output{}
        transport:
            name: file_output
            config:
                path: {:?}
        format:
            name: csv
            config:",
            i + 1,
            i + 1,
            output_path.to_str().unwrap()
        )
        .unwrap();
    }
    println!("{config_str}");

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();
    let controller = Controller::with_config(
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
        Box::new(|e| panic!("error: {e}")),
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
    wait_for_records(&controller, &[0]);

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
        println!("waiting for {} records", (i + 1) * 1000);
        wait_for_records(&controller, &[(i + 1) * 1000]);
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

    let barriers = (0..n_inputs).map(|i| i * 1000).collect::<Vec<_>>();
    let controller = start_controller(&storage_dir, &barriers);

    // Wait for the records that are not in the checkpoint to be
    // processed or replayed.
    println!("wait for 1000 records in each file");
    let mut written = repeat_n(1000, n_inputs).collect::<Vec<_>>();
    wait_for_records(&controller, &written);

    // Suspend.
    let (sender, receiver) = mpsc::channel();
    println!("start suspend");
    controller.start_suspend(Box::new(move |result| sender.send(result).unwrap()));

    // Iterate as long as we shouldn't have reached the barrier, adding
    // records round-robin to one input at a time.
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
    let expect = expectations(&written, &barriers);
    for i in 0..n_inputs {
        check_file_contents(&output_path(&storage_dir, i), 0..expect[i]);
    }

    // Now restart the controller and wait for all the records that we wrote
    // beyond the barriers get copied to output (and nothing else).
    let controller = start_controller(&storage_dir, &barriers);
    let remaining = expect
        .iter()
        .zip(written.iter())
        .map(|(&expect, &written)| written - expect)
        .collect::<Vec<_>>();
    wait_for_records(&controller, &remaining);
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

    let config_str = r#"
name: test
workers: 4
storage_config: null
storage: null
clock_resolution_usecs: null
inputs:
outputs:
        "#
    .to_string();

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let controller = Controller::with_config(
        |circuit_config| {
            Ok(test_circuit::<TestStruct>(
                circuit_config,
                &[],
                &[Some("output")],
            ))
        },
        &config,
        Box::new(|e| panic!("error: {e}")),
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
      "to": "6"
    },
    {
      "from": "3",
      "stream_id": 3,
      "to": "10"
    },
    {
      "from": "5",
      "stream_id": 4,
      "to": "6"
    },
    {
      "from": "5",
      "stream_id": null,
      "to": "7"
    },
    {
      "from": "6",
      "stream_id": 7,
      "to": "7"
    },
    {
      "from": "6",
      "stream_id": 7,
      "to": "8"
    },
    {
      "from": "6",
      "stream_id": 7,
      "to": "11"
    },
    {
      "from": "8",
      "stream_id": 8,
      "to": "9"
    },
    {
      "from": "11",
      "stream_id": 9,
      "to": "12"
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
        "input.output"
      ],
      "operation": "Output"
    },
    {
      "id": "5",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "Z1 (trace)"
    },
    {
      "id": "6",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "UntimedTraceAppend"
    },
    {
      "id": "7",
      "implements": [
        "input.output",
        "output"
      ],
      "operation": "Z1 (trace)"
    },
    {
      "id": "8",
      "implements": [
        "input.output"
      ],
      "operation": "Apply"
    },
    {
      "id": "9",
      "implements": [
        "input.output"
      ],
      "operation": "Output"
    },
    {
      "id": "10",
      "implements": [
        "output"
      ],
      "operation": "Output"
    },
    {
      "id": "11",
      "implements": [
        "output"
      ],
      "operation": "Apply"
    },
    {
      "id": "12",
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
