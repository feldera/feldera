//! The controller's suspend and resume: rounds of input with a suspend after each, the
//! bootstrap of a changed circuit on resume, and barriers that hold a suspend back until the
//! inputs that set them pass them.

use crate::test::engine_test;
use std::cmp::min;
use std::fs::{File, create_dir};
use std::iter::repeat_n;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::sleep;
use std::time::Duration;

use csv::WriterBuilder as CsvWriterBuilder;
use feldera_adapterlib::test::{default_timeout_ms, wait};
use feldera_types::config::PipelineConfig;
use serde_json::json;
use tempfile::TempDir;

use crate::Controller;
use crate::test::init_test_logger;
use crate::test::pipeline::{ControllerErrorCallback, ControllerOptions, PipelineTestEngine};
use crate::test::suites::controller::{
    SUSPEND_OUTPUT_TIMEOUT_MS, check_file_contents, check_file_ids, for_id, identity, tprintln,
    wait_for_records, wait_for_records_within,
};
use crate::transport::{set_barrier, set_failure};

fn assert_bounded_suspend_steps(controller: &Controller, suspend_request_step: u64) {
    let suspend_complete_step = controller.status().global_metrics.total_initiated_steps();
    let suspend_steps = suspend_complete_step.saturating_sub(suspend_request_step);

    assert!(
        suspend_steps <= 1000,
        "suspend advanced {suspend_steps} steps while waiting for barriers \
         ({suspend_request_step}..{suspend_complete_step})"
    );
}

/// Runs a basic test of suspend and resume, without fault tolerance.
///
/// For each element of `rounds`, the test writes the specified number of
/// records to the input file, and starts the pipeline and waits for it to
/// process the data.  Then it suspends the pipeline, checks that the output
/// is as expected, and goes on to the next round.
fn test_suspend(engine: &dyn PipelineTestEngine, rounds: &[usize]) {
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
                writer.serialize(for_id(id)).unwrap();
            }
            writer.flush().unwrap();
            total_records += n_records;
        }

        // Start pipeline.
        println!("start pipeline");
        let controller = engine
            .controller(
                &identity(&[Some("output")]),
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
            checkpointed_records..total_records,
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
fn test_bootstrap(engine: &dyn PipelineTestEngine, rounds: &[usize]) {
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
    let keep_program_diff = || ControllerOptions {
        keep_program_diff: true,
        ..ControllerOptions::default()
    };
    let mut controller = engine
        .controller_with_options(
            &identity(&[Some("v0")]),
            &config,
            keep_program_diff(),
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
                writer.serialize(for_id(id)).unwrap();
            }
            writer.flush().unwrap();
            total_records += n_records;
        }

        // Wait for the new records to be processed.
        wait_for_output(&controller, prev_output, 0..total_records, &output_path);
        prev_output += total_records;

        // Suspend.
        println!("suspend");
        controller.suspend().unwrap();

        // Stop controller.
        println!("stop controller");
        controller.stop().unwrap();

        // Resume modified pipeline.
        controller = engine
            .controller_with_options(
                &identity(&[Some(&format!("v{}", round + 1))]),
                &config,
                keep_program_diff(),
                Box::new(|e, _| panic!("error: {e}")),
            )
            .unwrap();
        controller.start();

        // Wait for the entire input to show up in the output stream.
        wait_for_output(&controller, prev_output, 0..total_records, &output_path);

        println!();
    }
}

/// Waits for output endpoint 0 of `controller` to transmit `initial_n_records` records and
/// then the rows with `expected_ids`, and checks that the file at `csv_file_path` holds those
/// rows.
fn wait_for_output(
    controller: &Controller,
    initial_n_records: usize,
    expected_ids: Range<usize>,
    csv_file_path: &Path,
) {
    let expect_n = initial_n_records + expected_ids.len();

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
    check_file_ids(csv_file_path, expected_ids);
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn suspend(engine: &dyn PipelineTestEngine) {
    test_suspend(engine, &[2500, 2500, 2500, 2500, 2500]);
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn suspend_empty(engine: &dyn PipelineTestEngine) {
    test_suspend(engine, &[0, 2500, 0, 2500, 0, 0, 0, 2500, 2500, 2500, 0]);
}

fn input_path(storage_dir: &Path, i: usize) -> PathBuf {
    storage_dir.join(format!("input{}.csv", i + 1))
}

fn output_path(storage_dir: &Path, i: usize) -> PathBuf {
    storage_dir.join(format!("output{}.csv", i + 1))
}

/// Starts a pipeline with one file input and one file output per entry of `barriers`, each
/// input in `storage_dir` with that barrier.
fn start_controller(
    engine: &dyn PipelineTestEngine,
    storage_dir: &Path,
    barriers: &[usize],
) -> Controller {
    start_controller_with_error_cb(
        engine,
        storage_dir,
        barriers,
        Box::new(|e, _| panic!("error: {e}")),
    )
}

/// Like [`start_controller`], with `error_cb` as the controller's error
/// callback.
fn start_controller_with_error_cb(
    engine: &dyn PipelineTestEngine,
    storage_dir: &Path,
    barriers: &[usize],
    error_cb: ControllerErrorCallback,
) -> Controller {
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

    let persistent_output_ids = (1..=n).map(|i| format!("output{i}")).collect::<Vec<_>>();
    let persistent_strs = persistent_output_ids
        .iter()
        .map(|s| Some(s.as_str()))
        .collect::<Vec<_>>();
    let controller = engine
        .controller(&identity(&persistent_strs), &config, error_cb)
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
///
/// Ignored on aarch64.
#[engine_test(
    ignore_if(target_arch = "aarch64"),
    requires(Storage, Checkpoints, SuspendResume)
)]
pub fn suspend_barrier(engine: &dyn PipelineTestEngine) {
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
    tprintln!("Writing records 0..4000");
    for id in 0..4000 {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();

    // Start pipeline.
    tprintln!("start pipeline");

    let controller = start_controller(engine, &storage_dir, &[5000]);

    // Wait for the records that are not in the checkpoint to be
    // processed or replayed.
    tprintln!("wait for 4000 records 0..4000");
    wait_for_records(&controller, &[4000]);

    // Suspend.
    let (sender, receiver) = mpsc::channel();
    tprintln!("start suspend");
    let suspend_request_step = controller.status().global_metrics.total_initiated_steps();
    controller.start_suspend(Box::new(move |result| sender.send(result).unwrap()));

    // Suspend should not succeed, because of the barrier.
    sleep(Duration::from_millis(1000));
    receiver.try_recv().unwrap_err();

    // Write 1000 more records.
    for id in 4000..5000 {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();

    // Suspend should now succeed, because we crossed the barrier.
    receiver
        .recv_timeout(Duration::from_millis(100000))
        .unwrap()
        .unwrap();
    assert_bounded_suspend_steps(&controller, suspend_request_step);

    // Stop controller.
    tprintln!("stop controller");
    controller.stop().unwrap();

    // Read output and compare. Our output adapter, which is not
    // fault-tolerant, truncates the output file to length 0 each
    // time. Therefore, the output file should contain all the records
    // in `0..5000`
    check_file_contents(&output_path(&storage_dir, 0), 0..5000);

    // Restart the controller.  It should output no records initially
    // because the checkpoint covered all of them.
    let controller = start_controller(engine, &storage_dir, &[5000]);
    wait_for_records(&controller, &[5000]);

    tprintln!("start suspend");
    let (sender, receiver) = mpsc::channel();
    let mut sender = Some(sender);

    for i in 0..5 {
        // Make sure suspend hasn't completed yet.
        receiver.try_recv().unwrap_err();

        let start = (i + 5) * 1000;
        let end = start + 1000;
        tprintln!("writing records {start}..{end}");
        for id in start..end {
            writer.serialize(for_id(id)).unwrap();
        }
        writer.flush().unwrap();
        tprintln!("waiting for {end} records");
        // After the first iteration a suspend is pending: the controller only
        // advances barrier inputs while re-attempting the checkpoint each step,
        // so output can lag under CI CPU load. Use the longer budget.
        wait_for_records_within(&controller, &[end], SUSPEND_OUTPUT_TIMEOUT_MS);
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
        .recv_timeout(Duration::from_millis(100000))
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
fn suspend_multiple_barriers(engine: &dyn PipelineTestEngine, n_inputs: usize) {
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
    tprintln!("Writing 1000 records to each of {n_inputs} files");
    for writer in writers.iter_mut().take(n_inputs) {
        for id in 0..1000 {
            writer.serialize(for_id(id)).unwrap();
        }
        writer.flush().unwrap();
    }

    // Start pipeline.
    tprintln!("start pipeline");

    // The barrier for input 0 is record 0,
    // for input 1 is record 1000,
    // for input 2 is record 2000,
    // and so on.
    let barriers = (0..n_inputs).map(|i| i * 1000).collect::<Vec<_>>();
    let controller = start_controller(engine, &storage_dir, &barriers);

    // Wait for the first 1000 records in each file to be read and copied to the
    // output.
    tprintln!("wait for 1000 records in each file");
    let mut written = repeat_n(1000, n_inputs).collect::<Vec<_>>();
    wait_for_records(&controller, &written);

    // Start a suspend operation.
    //
    // If we have 1 or 2 inputs, the suspend will complete quickly, because
    // we're past all the barriers; otherwise, it will not complete due to
    // barriers, since each input only has 1000 records so far.
    let (sender, receiver) = mpsc::channel();
    tprintln!("start suspend");
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
        tprintln!("writing 1000 more records to test_input{}", next + 1);
        for id in written[next]..written[next] + 1000 {
            writers[next].serialize(for_id(id)).unwrap();
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
        tprintln!("total written: {written:?}");
        let expect = expectations(&written, &barriers);
        // A suspend is pending here: the controller only advances barrier inputs
        // while re-attempting the checkpoint each step, so output can lag far
        // behind under CI CPU load. Use the longer budget.
        wait_for_records_within(&controller, &expect, SUSPEND_OUTPUT_TIMEOUT_MS);
        for (i, expectation) in expect.iter().enumerate().take(n_inputs) {
            check_file_contents(&output_path(&storage_dir, i), 0..*expectation);
        }
    }

    // Suspend should now succeed, because we crossed the barrier.
    receiver
        .recv_timeout(Duration::from_millis(100000))
        .unwrap()
        .unwrap();
    assert_bounded_suspend_steps(&controller, suspend_request_step);

    // Stop controller.
    tprintln!("stop controller");
    controller.stop().unwrap();

    // Check output one more time.
    tprintln!("check output one more time now that controller is stopped");
    let expect = expectations(&written, &barriers);
    for (i, e) in expect.iter().enumerate().take(n_inputs) {
        check_file_contents(&output_path(&storage_dir, i), 0..*e);
    }

    // Now restart the controller and wait for all the records that we wrote
    // beyond the barriers get copied to output (and nothing else).
    tprintln!("restart controller and wait for records beyond the barriers");
    let controller = start_controller(engine, &storage_dir, &barriers);
    wait_for_records(&controller, &written);
    for i in 0..n_inputs {
        check_file_contents(&output_path(&storage_dir, i), expect[i]..written[i]);
    }
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn suspend_barrier2(engine: &dyn PipelineTestEngine) {
    suspend_multiple_barriers(engine, 2);
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn suspend_barrier3(engine: &dyn PipelineTestEngine) {
    suspend_multiple_barriers(engine, 3);
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn suspend_barrier4(engine: &dyn PipelineTestEngine) {
    suspend_multiple_barriers(engine, 4);
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn suspend_barrier5(engine: &dyn PipelineTestEngine) {
    suspend_multiple_barriers(engine, 5);
}

#[engine_test(requires(Storage, Checkpoints, SuspendResume, Bootstrap))]
pub fn bootstrap(engine: &dyn PipelineTestEngine) {
    test_bootstrap(engine, &[2500, 2500, 2500, 2500, 2500]);
}

/// Suspend fails with an actionable error, instead of waiting forever, when an
/// input endpoint failed while it held a barrier.
///
/// The controller keeps a failed endpoint in the pipeline status. Its barrier
/// flag is set by the last step it reported, and a failed endpoint never
/// reports a step again, so the flag never clears.
#[engine_test(requires(Storage, Checkpoints, SuspendResume))]
pub fn failed_input_at_barrier_fails_suspend(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let input = input_path(&storage_dir, 0);
    let input_file = File::create(&input).unwrap();
    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);
    for id in 0..1000 {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();

    // The barrier lies past the end of the file, so the endpoint holds it.
    let controller = start_controller_with_error_cb(
        engine,
        &storage_dir,
        &[2000],
        Box::new(|e, _| tprintln!("controller error: {e}")),
    );
    wait_for_records(&controller, &[1000]);
    assert!(
        controller
            .status()
            .input_status()
            .get(&0)
            .unwrap()
            .is_barrier()
    );

    set_failure(input.to_str().unwrap());
    wait(
        || {
            controller
                .status()
                .input_status()
                .get(&0)
                .is_none_or(|status| status.fatal_error.lock().unwrap().is_some())
        },
        default_timeout_ms(),
    )
    .expect("the input endpoint did not fail");

    // The controller sees that the endpoint stopped during a step. This
    // pipeline has no clock and no new input, so request steps.
    for _ in 0..10 {
        controller.request_step();
        sleep(Duration::from_millis(200));
    }

    let (sender, receiver) = mpsc::channel();
    controller.start_suspend(Box::new(move |result| sender.send(result).unwrap()));
    let result = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("suspend did not complete within 30s of the input endpoint failing at a barrier");
    let error = result
        .expect_err("suspend succeeded although the failed input endpoint holds a barrier")
        .to_string();
    assert!(
        error.contains("failed with a fatal error at a position that cannot be checkpointed"),
        "unexpected suspend error: {error}"
    );
    let endpoint_name = controller.status().input_status()[&0].endpoint_name.clone();
    assert_eq!(
        controller.permanent_checkpoint_errors(),
        Some(vec![
            feldera_types::suspend::PermanentSuspendError::FailedInputEndpointBarrier(
                endpoint_name
            )
        ])
    );

    controller.stop().unwrap();
}

/// A checkpoint taken after an input endpoint was removed while it held a
/// barrier must not crash the pipeline.
///
/// Pruning resume entries during an input step is not enough: an immediate
/// checkpoint can run without another step. A removed barrier entry has no
/// seek position, and must not be serialized into that checkpoint.
#[engine_test(requires(Storage, Checkpoints))]
pub fn checkpoint_after_removing_input_at_barrier(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let input_file = File::create(input_path(&storage_dir, 0)).unwrap();
    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);
    for id in 0..1000 {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();

    // The barrier lies past the end of the file, so the endpoint holds it.
    let controller = start_controller(engine, &storage_dir, &[2000]);
    wait_for_records(&controller, &[1000]);
    assert!(
        controller
            .status()
            .input_status()
            .get(&0)
            .unwrap()
            .is_barrier()
    );

    controller.disconnect_input(&0);
    // Do not request intervening steps: they would prune the stale entry and
    // hide the immediate-checkpoint failure.
    let checkpoint = controller
        .checkpoint()
        .expect("checkpoint immediately after removing the input endpoint failed");
    assert!(checkpoint.config.inputs.is_empty());
    assert!(checkpoint.input_metadata.0.is_empty());
    assert!(checkpoint.input_statistics.is_empty());
    controller.stop().unwrap();
}

/// Removal after capturing configuration must not discard that input's cursor
/// or statistics from the same checkpoint.
#[engine_test(requires(Storage, Checkpoints))]
pub fn checkpoint_removal_between_snapshots_keeps_cursor(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let input_file = File::create(input_path(&storage_dir, 0)).unwrap();
    let mut writer = CsvWriterBuilder::new()
        .has_headers(false)
        .from_writer(&input_file);
    for id in 0..1000 {
        writer.serialize(for_id(id)).unwrap();
    }
    writer.flush().unwrap();
    let controller = start_controller(engine, &storage_dir, &[0]);
    wait_for_records(&controller, &[1000]);
    let inner = controller.inner.clone();
    *controller.inner.checkpoint_snapshot_hook.lock().unwrap() =
        Some(Box::new(move || inner.disconnect_input(&0)));
    let checkpoint = controller.checkpoint().unwrap();
    controller.stop().unwrap();
    assert!(checkpoint.config.inputs.contains_key("test_input1"));
    assert!(
        checkpoint.input_metadata.0.contains_key("test_input1"),
        "checkpoint restores the input, but omits its seek cursor"
    );
    assert!(checkpoint.input_statistics.contains_key("test_input1"));
}
