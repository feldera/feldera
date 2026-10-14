//! Checks that checkpoint GC accounts for every batch file when spines in one
//! worker share a persistent id.
//!
//! Each circuit integrates several copies of one input stream.  The copies
//! either get distinct persistent ids or share one.  A checkpoint names a
//! spine's files after the worker and the persistent id, so copies that share
//! an id write their batch lists to the same file.

use std::collections::HashSet;
use std::path::Path;

use feldera_types::config::StorageConfig;
use tempfile::tempdir;

use super::{
    CircuitConfig,
    dbsp_handle::{CircuitStorageConfig, Mode},
};
use crate::{RootCircuit, Runtime, ZSetHandle};

/// Integral counts and id sharing that each test runs.
const CASES: [(usize, bool); 3] = [(2, false), (2, true), (6, true)];

/// Records loaded, one transaction each, before the checkpoint rounds.
const RECORDS: u64 = 25;

/// Checkpoint rounds in each run.
const CHECKPOINTS: usize = 40;

/// Returns a single-worker persistent-mode config that keeps its state in
/// `path`, with default storage thresholds.
fn config(path: &Path) -> CircuitConfig {
    CircuitConfig::with_workers(1)
        .with_mode(Mode::Persistent)
        .with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: Default::default(),
                },
                Default::default(),
            )
            .unwrap(),
        ))
}

/// Returns the sizes of the batch files in the storage root.
fn batch_file_sizes(path: &Path) -> Vec<u64> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with('w') && name.ends_with(".feldera")
        })
        .map(|entry| entry.metadata().unwrap().len())
        .collect()
}

/// Builds a circuit that integrates copies of one input stream.
///
/// Arguments:
/// - `circuit`: the circuit to build.
/// - `copies`: how many integrals to build over the input.
/// - `shared_id`: gives every copy the same persistent id if true, and a
///   distinct one otherwise.
///
/// Returns the input handle.
fn build(
    circuit: &mut RootCircuit,
    copies: usize,
    shared_id: bool,
) -> Result<ZSetHandle<u64>, anyhow::Error> {
    let (stream, handle) = circuit.add_input_zset::<u64>();
    stream.set_persistent_id(Some("input"));
    for copy in 0..copies {
        let id = if shared_id {
            "copy".to_string()
        } else {
            format!("copy{copy}")
        };
        stream
            .map(|x| *x)
            .set_persistent_id(Some(&id))
            .integrate_trace();
    }
    Ok(handle)
}

/// What a run left on storage.
struct Outcome {
    /// Batch files after each checkpoint round.
    files_per_checkpoint: Vec<usize>,
    /// Batch files that startup GC deleted at the restart.
    deleted_files: usize,
    /// Total size of the batch files that startup GC deleted.
    deleted_bytes: u64,
}

/// Loads [RECORDS] records, runs [CHECKPOINTS] rounds of a transaction, a
/// checkpoint and checkpoint GC, then restarts the circuit on the same
/// storage.
///
/// Arguments:
/// - `copies`: how many integrals to build over the input.
/// - `shared_id`: whether the integrals share one persistent id.
/// - `input_per_round`: whether each round pushes one more record.
///
/// Returns the batch files after each round and what startup GC deleted.
fn run(copies: usize, shared_id: bool, input_per_round: bool) -> Outcome {
    let temp = tempdir().unwrap();
    let (mut dbsp, input) = Runtime::init_circuit(config(temp.path()), move |circuit| {
        build(circuit, copies, shared_id)
    })
    .unwrap();
    for record in 0..RECORDS {
        input.push(record, 1);
        dbsp.transaction().unwrap();
    }
    let mut files_per_checkpoint = Vec::new();
    for round in 0..CHECKPOINTS as u64 {
        if input_per_round {
            input.push(RECORDS + round, 1);
        }
        dbsp.transaction().unwrap();
        dbsp.checkpoint().run().unwrap();
        dbsp.gc_checkpoint(HashSet::new()).unwrap();
        files_per_checkpoint.push(batch_file_sizes(temp.path()).len());
    }
    dbsp.kill().unwrap();

    let before = batch_file_sizes(temp.path());
    let (dbsp, _input) = Runtime::init_circuit(config(temp.path()), move |circuit| {
        build(circuit, copies, shared_id)
    })
    .unwrap();
    let after = batch_file_sizes(temp.path());
    dbsp.kill().unwrap();
    Outcome {
        files_per_checkpoint,
        deleted_files: before.len() - after.len(),
        deleted_bytes: before.iter().sum::<u64>() - after.iter().sum::<u64>(),
    }
}

/// Runs every case in [CASES] and prints what each one left on storage.
///
/// Arguments:
/// - `input_per_round`: whether each checkpoint round pushes one more record.
///
/// Returns each case with its outcome.
fn run_cases(input_per_round: bool) -> Vec<(usize, bool, Outcome)> {
    CASES
        .into_iter()
        .map(|(copies, shared_id)| {
            let outcome = run(copies, shared_id, input_per_round);
            println!(
                "copies={copies} shared_id={shared_id} input_per_round={input_per_round}: \
                 batch files after each checkpoint {:?}; startup GC deleted {} files, {} bytes",
                outcome.files_per_checkpoint, outcome.deleted_files, outcome.deleted_bytes
            );
            (copies, shared_id, outcome)
        })
        .collect()
}

/// Checkpoints without input must not add batch files.
#[test]
fn idle_checkpoints_do_not_grow_storage() {
    for (copies, shared_id, outcome) in run_cases(false) {
        // Each of the two retained checkpoints may hold its own copies of the
        // in-memory batches, so the count settles after the second round.
        let settled = outcome.files_per_checkpoint[1];
        let last = *outcome.files_per_checkpoint.last().unwrap();
        assert!(
            last <= settled,
            "copies={copies} shared_id={shared_id}: batch files grew from {settled} to {last} \
             over {CHECKPOINTS} checkpoints without input"
        );
    }
}

/// Every batch file left on storage must belong to a retained checkpoint, so
/// startup GC finds nothing to delete.
#[test]
fn restart_finds_no_unreferenced_batch_files() {
    for input_per_round in [false, true] {
        for (copies, shared_id, outcome) in run_cases(input_per_round) {
            assert_eq!(
                outcome.deleted_files, 0,
                "copies={copies} shared_id={shared_id} input_per_round={input_per_round}: \
                 startup GC deleted {} unreferenced batch files ({} bytes)",
                outcome.deleted_files, outcome.deleted_bytes
            );
        }
    }
}
