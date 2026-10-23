//! The controller's fault tolerance: restarts with and without checkpoints, with pauses and
//! changed connectors, snapshots that output connectors receive once, and the publication
//! and the activity of checkpoints.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fs::{File, create_dir};
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

use csv::WriterBuilder as CsvWriterBuilder;
use feldera_adapterlib::test::{default_timeout_ms, wait};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::config::{InputEndpointConfig, OutputEndpointConfig, PipelineConfig};
use feldera_types::constants::{CHECKPOINT_FILE_NAME, STATE_FILE};
use serde_json::json;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::Controller;
use crate::test::init_test_logger;
use crate::test::pipeline::PipelineTestEngine;
use crate::test::suites::controller::{
    BlockedOutputEndpoint, append_input, check_file_contents, for_id, identity,
    output_endpoint_metrics, wait_for_records,
};

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
fn test_ft(engine: &dyn PipelineTestEngine, rounds: &[FtTestRound]) {
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
                writer.serialize(for_id(id)).unwrap();
            }
            writer.flush().unwrap();
            total_records += n_records;
        }

        // Start pipeline.
        println!("start pipeline");

        if let Some(modify_config) = modify_config {
            config = modify_config(config);
        }

        let controller = engine
            .controller(
                &identity(&[Some("output")]),
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
            wait(|| !controller.is_replaying(), 10_000).unwrap();
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

pub fn ft_with_checkpoints(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ],
    );
}

pub fn ft_immediate_checkpoints(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
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
        ],
    );
}

pub fn ft_without_checkpoints(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
        ],
    );
}

pub fn ft_with_checkpoints_with_pauses(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
            FtTestRound::with_checkpoint(2500).with_pause_afterward(),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500).with_pause_afterward(),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500).with_pause_afterward(),
        ],
    );
}

pub fn ft_without_checkpoints_with_pauses(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
            FtTestRound::without_checkpoint(2500).with_pause_afterward(),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500).with_pause_afterward(),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500).with_pause_afterward(),
        ],
    );
}

pub fn ft_alternating(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
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
        ],
    );
}

pub fn ft_initially_zero_without_checkpoint(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
            FtTestRound::without_checkpoint(0),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(0),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ],
    );
}

pub fn ft_initially_zero_with_checkpoint(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
            FtTestRound::with_checkpoint(0),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(0),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ],
    );
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

pub fn ft_modified_connectors(engine: &dyn PipelineTestEngine) {
    test_ft(
        engine,
        &[
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
        ],
    );
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
pub fn ft_send_snapshot_delivered_once(engine: &dyn PipelineTestEngine) {
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
            writer.serialize(for_id(id)).unwrap();
        }
        writer.flush().unwrap();
        total_records += n_records;

        let controller = engine
            .controller(
                &identity(&[Some("output")]),
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
pub fn ft_send_snapshot_resent_on_flag_flip(engine: &dyn PipelineTestEngine) {
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
        let controller = engine
            .controller(
                &identity(&[Some("output")]),
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
            writer.serialize(for_id(id)).unwrap();
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
            writer.serialize(for_id(id)).unwrap();
        }
        writer.flush().unwrap();
    }
    run_round(&config_on, 1500);
    check_file_contents(&output_path, 500..1000);
}

/// End-to-end: modified `send_snapshot: true` delta sink delivers its
/// snapshot on a restart that never calls `start()` (stays paused).
#[cfg(feature = "with-deltalake")]
pub fn ft_send_snapshot_delta_delivered_while_paused(engine: &dyn PipelineTestEngine) {
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
            writer.serialize(for_id(id)).unwrap();
        }
        writer.flush().unwrap();
    }
    let config_off = build_config(&input_path, &delta_uri, &storage_dir, false);
    let controller = engine
        .controller(
            &identity(&[Some("output")]),
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
    // `controller.start()`: the pipeline stays in the default `Paused`
    // state. The connector definition changed, so `is_snapshot_pending`
    // becomes true; with the fix, registration requests a step that fires
    // even while paused and delivers the snapshot (500 records) to the
    // truncated delta table. `transmitted_records` should reach 1000
    // (500 carried over from the checkpoint seed + 500 from the snapshot).
    let config_on = build_config(&input_path, &delta_uri, &storage_dir, true);
    let controller = engine
        .controller(
            &identity(&[Some("output")]),
            &config_on,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    // Notably: no `controller.start()`, so the pipeline stays paused.
    wait_for_records(&controller, &[1000]);
    controller.stop().unwrap();
}

/// The UUIDs in the checkpoint catalog, oldest first, or an empty list if the
/// catalog does not exist yet.
fn published_checkpoints(storage_dir: &Path) -> Vec<Uuid> {
    match std::fs::read(storage_dir.join(CHECKPOINT_FILE_NAME)) {
        Ok(bytes) => serde_json::from_slice::<VecDeque<CheckpointMetadata>>(&bytes)
            .unwrap()
            .iter()
            .map(|metadata| metadata.uuid)
            .collect(),
        Err(error) if error.kind() == ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("cannot read checkpoint catalog: {error}"),
    }
}

/// True if a checkpoint directory exists in `storage_dir`, which means the
/// operators have written their state for a checkpoint.
fn checkpoint_dir_exists(storage_dir: &Path) -> bool {
    std::fs::read_dir(storage_dir).unwrap().any(|entry| {
        let entry = entry.unwrap();
        entry.file_type().unwrap().is_dir()
            && Uuid::parse_str(&entry.file_name().to_string_lossy()).is_ok()
    })
}

/// A checkpoint reaches the catalog only after the output it covers has reached
/// the output connectors.
///
/// The controller commits the checkpoint's operator data, waits for every
/// output connector to transmit the output for the records the checkpoint
/// covers, and publishes the checkpoint only then.  Publishing before the wait
/// would leave a checkpoint that looks restorable but whose output never
/// reached its sinks (feldera/feldera#7031).
pub fn checkpoint_publishes_only_after_output_completes(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    File::create_new(&input_path).unwrap();

    const RECORDS: usize = 100;

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "storage": true,
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
        "outputs": {},
    }))
    .unwrap();

    append_input(&input_path, 0..RECORDS);

    let controller = engine
        .controller(
            &identity(&[Some("output")]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    let blocked = Arc::new(AtomicBool::new(false));
    let hold = Arc::new(AtomicBool::new(true));
    let endpoint_config: OutputEndpointConfig = serde_json::from_value(json!({
        "stream": "test_output1",
        "transport": { "name": "null_output" },
        "format": { "name": "csv", "config": {} },
    }))
    .unwrap();
    controller
        .add_output_endpoint(
            "test_output1",
            &endpoint_config,
            Box::new(BlockedOutputEndpoint {
                blocked: blocked.clone(),
                hold: hold.clone(),
            }),
            None,
        )
        .unwrap();
    controller.start();

    // The pipeline processes every record, but the connector is stuck in a
    // write that never finishes, so none of them can complete.
    wait(|| blocked.load(Ordering::Acquire), default_timeout_ms()).unwrap();
    wait(
        || {
            controller
                .status()
                .global_metrics
                .num_total_processed_records()
                >= RECORDS as u64
        },
        default_timeout_ms(),
    )
    .unwrap();
    assert_eq!(
        output_endpoint_metrics(&controller, "test_output1").transmitted_records,
        0
    );

    let before = published_checkpoints(&storage_dir);
    let (sender, receiver) = oneshot::channel();
    controller.start_checkpoint(Box::new(move |result| sender.send(result).unwrap()));

    // A checkpoint directory means the operators have written their state, so
    // the checkpoint is past the point where it used to be published.  Give a
    // premature publication time to appear: committing that state is a set of
    // flushes on files that are already written, so it finishes long inside
    // this window.
    wait(|| checkpoint_dir_exists(&storage_dir), default_timeout_ms()).unwrap();
    sleep(Duration::from_secs(2));

    assert_eq!(
        output_endpoint_metrics(&controller, "test_output1").transmitted_records,
        0,
        "the connector drained, so it never held the checkpoint back"
    );
    assert_eq!(
        published_checkpoints(&storage_dir),
        before,
        "the checkpoint was published while its output was still stuck in a connector"
    );

    // Release the connector.  The output completes, and the checkpoint is
    // published.
    hold.store(false, Ordering::Release);
    let checkpoint = receiver.blocking_recv().unwrap().unwrap();
    let uuid = checkpoint.circuit.as_ref().unwrap().uuid;

    let mut expected = before;
    expected.push(uuid);
    assert_eq!(published_checkpoints(&storage_dir), expected);
    assert_eq!(
        output_endpoint_metrics(&controller, "test_output1").transmitted_records,
        RECORDS as u64
    );

    controller.stop().unwrap();
}

/// The `CheckpointMetadata` named by checkpoint `uuid`'s own `state.json`,
/// read the way checkpoint sync reads it.
fn checkpoint_state_circuit(storage_dir: &Path, uuid: Uuid) -> CheckpointMetadata {
    let bytes = std::fs::read(storage_dir.join(uuid.to_string()).join(STATE_FILE)).unwrap();
    let state: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let circuit = state
        .get("circuit")
        .expect("the checkpoint's state.json has no `circuit` field")
        .clone();
    serde_json::from_value(circuit)
        .expect("`circuit` in the checkpoint's state.json is not checkpoint metadata")
}

/// A checkpoint's own `state.json` names the circuit checkpoint it belongs to.
///
/// Checkpoint sync reads `circuit` from this file to describe the checkpoint it
/// uploads, and a multihost host restoring a checkpoint the coordinator names
/// reads it to find the circuit to open.  Taking the metadata from
/// `CheckpointPublisher::metadata` puts it there; taking it from `publish`
/// instead, now that the file is written first, records a null circuit, which
/// fails the first reader and makes the second restore an empty circuit while
/// replaying from the checkpoint's input offsets, with no error to show for it.
///
/// This guards the ordering that the fix for #7031 introduced.  It is not a
/// regression test for the issue itself: before that fix the metadata was
/// already in hand when this file was written, so `circuit` was never null.
/// The other half of the fix, that the catalog entry appears only once this
/// file is on disk, is a crash window that no test here covers.
pub fn checkpoint_state_file_names_its_own_circuit(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    File::create_new(&input_path).unwrap();

    const RECORDS: usize = 100;

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "storage": true,
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
        "outputs": {},
    }))
    .unwrap();

    append_input(&input_path, 0..RECORDS);

    let controller = engine
        .controller(
            &identity(&[Some("output")]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();
    controller.start();
    wait(
        || {
            controller
                .status()
                .global_metrics
                .num_total_processed_records()
                >= RECORDS as u64
        },
        default_timeout_ms(),
    )
    .unwrap();

    // Two checkpoints, because a `state.json` that names the checkpoint before
    // it is wrong in a way that only the second checkpoint can show.
    for round in 0..2 {
        append_input(&input_path, (round + 1) * RECORDS..(round + 2) * RECORDS);
        let uuid = controller
            .checkpoint()
            .unwrap()
            .circuit
            .expect("the checkpoint the controller reports has no circuit metadata")
            .uuid;
        assert_eq!(
            checkpoint_state_circuit(&storage_dir, uuid).uuid,
            uuid,
            "checkpoint {uuid}'s state.json names a different checkpoint"
        );
        assert!(published_checkpoints(&storage_dir).contains(&uuid));
    }

    controller.stop().unwrap();
}

/// A restart resumes from the newest published checkpoint, even when the root
/// `state.json` still names the one before it, as a crash between publishing a
/// checkpoint and rewriting the root file leaves it.
pub fn restart_resumes_from_newest_published_checkpoint(engine: &dyn PipelineTestEngine) {
    test_restart_from_newest_published_checkpoint(engine, false);
}

/// Exactly-once variant: journal replay starts at the newest checkpoint and
/// counts no record twice.
pub fn ft_restart_resumes_from_newest_published_checkpoint(engine: &dyn PipelineTestEngine) {
    test_restart_from_newest_published_checkpoint(engine, true);
}

/// Restarts a pipeline, fault-tolerant if `fault_tolerant`, after a crash left the root
/// `state.json` naming the checkpoint before the newest, and checks that it resumes from the
/// newest.
fn test_restart_from_newest_published_checkpoint(
    engine: &dyn PipelineTestEngine,
    fault_tolerant: bool,
) {
    init_test_logger();
    let tempdir = TempDir::new().unwrap();
    let tempdir_path = tempdir.path();
    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let input_path = tempdir_path.join("input.csv");
    File::create_new(&input_path).unwrap();

    const RECORDS: usize = 100;

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "storage": true,
        "fault_tolerance": if fault_tolerant { json!({}) } else { json!(null) },
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
        "outputs": {},
    }))
    .unwrap();
    let start_controller = || {
        engine
            .controller(
                &identity(&[Some("output")]),
                &config,
                Box::new(|e, _| panic!("error: {e}")),
            )
            .unwrap()
    };
    let processed_records = |controller: &Controller| {
        controller
            .status()
            .global_metrics
            .num_total_processed_records()
    };
    let wait_processed = |controller: &Controller, records: usize| {
        wait(
            || processed_records(controller) >= records as u64,
            default_timeout_ms(),
        )
        .unwrap();
    };
    let checkpoint_after = |controller: &Controller, records: usize| {
        wait_processed(controller, records);
        controller.checkpoint().unwrap()
    };

    let controller = start_controller();
    controller.start();
    append_input(&input_path, 0..RECORDS);
    let older = checkpoint_after(&controller, RECORDS);
    append_input(&input_path, RECORDS..2 * RECORDS);
    let newest = checkpoint_after(&controller, 2 * RECORDS);
    // Steps after the newest checkpoint, which the restart reads again from
    // the input or replays from the journal.
    append_input(&input_path, 2 * RECORDS..3 * RECORDS);
    wait_processed(&controller, 3 * RECORDS);
    controller.stop().unwrap();

    let older_uuid = older.circuit.unwrap().uuid;
    std::fs::copy(
        storage_dir.join(older_uuid.to_string()).join(STATE_FILE),
        storage_dir.join(STATE_FILE),
    )
    .unwrap();

    let controller = start_controller();
    if fault_tolerant {
        // Replay runs before `start`, so check the count after it ends.
        wait(|| !controller.is_replaying(), default_timeout_ms()).unwrap();
        assert_eq!(
            processed_records(&controller),
            3 * RECORDS as u64,
            "replay did not start at checkpoint {}",
            newest.circuit.unwrap().uuid
        );
    } else {
        assert_eq!(
            processed_records(&controller),
            newest.processed_records,
            "the restart resumed from checkpoint {older_uuid}, which the root state.json names"
        );
    }

    // Later checkpoints, and the cleanup after each, leave a checkpoint that
    // the next restart can open.
    controller.start();
    checkpoint_after(&controller, 3 * RECORDS);
    append_input(&input_path, 3 * RECORDS..4 * RECORDS);
    checkpoint_after(&controller, 4 * RECORDS);
    controller.stop().unwrap();
    let controller = start_controller();
    assert_eq!(processed_records(&controller), 4 * RECORDS as u64);
    controller.stop().unwrap();
}

/// Tests that `checkpoint_activity()` returns correct states and timestamps
/// during a coordinated checkpoint flow (prepare → delayed → in_progress → idle).
pub fn checkpoint_activity_state_transitions(engine: &dyn PipelineTestEngine) {
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
        for id in 0..10 {
            writer.serialize(for_id(id)).unwrap();
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

    let controller = engine
        .controller(
            &identity(&[Some("output")]),
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

/// Tests that `checkpoint_activity()` transitions correctly during a
/// `start_checkpoint()` call, and that the `checkpoint_started` timestamp is
/// set during the write.
pub fn checkpoint_activity_async_checkpoint(engine: &dyn PipelineTestEngine) {
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
        for id in 0..10 {
            writer.serialize(for_id(id)).unwrap();
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

    let controller = engine
        .controller(
            &identity(&[Some("output")]),
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
    wait(
        || {
            let coord = controller.checkpoint_watcher().borrow().clone();
            if matches!(coord, Some(CheckpointCoordination::InProgress)) {
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

/// Tests that `permanent_checkpoint_errors()` returns errors when storage
/// is explicitly disabled (the pipeline cannot checkpoint).
pub fn permanent_checkpoint_errors_without_storage(engine: &dyn PipelineTestEngine) {
    use feldera_types::suspend::PermanentSuspendError;

    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(b"1,true,,foo\n")
        .unwrap();

    // Storage explicitly disabled: the pipeline cannot checkpoint.
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

    let controller = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    // With storage disabled, permanent_checkpoint_errors should report
    // StorageRequired.
    let errors = controller.permanent_checkpoint_errors();
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
