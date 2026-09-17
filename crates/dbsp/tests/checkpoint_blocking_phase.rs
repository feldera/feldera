//! The blocking phase of a checkpoint must not fsync.
//!
//! `prepare` runs between transactions with the pipeline stopped, so every sync
//! it performs is time the pipeline is down. Durability belongs to `commit`,
//! which runs after the pipeline has resumed. Operators reach that phase by
//! handing their committers to it rather than syncing their own files, and one
//! operator forgetting to is invisible except as a slower checkpoint, which is
//! what this test is here to catch.
//!
//! The histograms are process-wide, so this must be the only test in this file;
//! a second test would run concurrently and record into the same totals.

use dbsp::Circuit;
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::operator::Generator;
use dbsp::typed_batch::OrdZSet;
use dbsp::utils::Tup2;
use dbsp::{DBSPHandle, Runtime};
use feldera_storage::metrics::{COMMIT_ALL_LATENCY_MICROSECONDS, SYNC_LATENCY_MICROSECONDS};
use feldera_types::checkpoint::CheckpointMetadata;
use feldera_types::config::{
    FileBackendConfig, StorageBackendConfig, StorageCacheConfig, StorageConfig, StorageOptions,
    StorageSyncMode,
};

/// Observations recorded so far, as (per-file syncs, commit_all calls).
fn counts() -> (u64, u64) {
    let total = |histogram: &feldera_storage::histogram::ExponentialHistogram| {
        histogram
            .snapshot()
            .iter_buckets()
            .map(|bucket| bucket.count)
            .sum()
    };
    (
        total(&SYNC_LATENCY_MICROSECONDS),
        total(&COMMIT_ALL_LATENCY_MICROSECONDS),
    )
}

/// A circuit with storage, whose integral gives the spines something to
/// persist, so a checkpoint writes batches and per-operator state rather than
/// nothing at all.
fn circuit_with_storage(path: &std::path::Path) -> DBSPHandle {
    // Pin the per-file strategy: syncfs would make one call however many files
    // there are, which cannot distinguish "nothing synced in prepare" from
    // "everything did".
    let config = CircuitConfig::with_workers(2).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                min_storage_bytes: Some(0),
                backend: StorageBackendConfig::File(Box::new(FileBackendConfig {
                    sync_mode: Some(StorageSyncMode::PerFile),
                    ..FileBackendConfig::default()
                })),
                ..StorageOptions::default()
            },
        )
        .unwrap(),
    ));

    let (handle, ()) = Runtime::init_circuit(config, |circuit| {
        let source = circuit.add_source(Generator::new(|| {
            let keys: Vec<Tup2<u64, i64>> = (0..256)
                .map(|k| Tup2(k * 7 + Runtime::worker_index() as u64, 1i64))
                .collect();
            OrdZSet::from_keys((), keys)
        }));
        source.integrate_trace().apply(|_| ());
        Ok(())
    })
    .unwrap();
    handle
}

/// The files a checkpoint names: `batches` at the storage root, `state_files`
/// in the checkpoint's own directory.
#[derive(serde::Deserialize)]
struct CheckpointDependencies {
    batches: Vec<String>,
    state_files: Vec<String>,
}

/// Reads the dependency list that `commit` wrote for checkpoint `metadata`.
fn checkpoint_dependencies(
    storage: &std::path::Path,
    metadata: &CheckpointMetadata,
) -> CheckpointDependencies {
    let path = storage
        .join(metadata.uuid.to_string())
        .join("dependencies.json");
    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap()
}

/// Keep this the only `#[test]` in this file: it reads process-global
/// counters, so a second test would run concurrently and make both flaky.
#[test]
fn prepare_does_not_sync() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut handle = circuit_with_storage(tempdir.path());
    handle.transaction().unwrap();

    // The first checkpoint also creates the checkpoint catalog, which commits a
    // file of its own from inside prepare. Take it and measure the next one.
    handle.checkpoint().run().unwrap();
    handle.transaction().unwrap();

    let before = counts();
    let committer = handle.checkpoint().prepare().unwrap();
    assert_eq!(
        counts(),
        before,
        "the blocking phase of a checkpoint must not sync"
    );

    let publisher = committer.commit().unwrap();
    let (synced, commit_alls) = counts();

    // Every file the checkpoint names must be synced, not merely some of them.
    // An operator that keeps its committer to itself loses exactly one file,
    // which is what a mere "something was synced" would miss. The expected
    // count is what `dependencies.json` names plus `dependencies.json` itself,
    // which lists the others and so cannot list itself.
    let dependencies = checkpoint_dependencies(tempdir.path(), publisher.metadata());
    let expected = dependencies.state_files.len() + dependencies.batches.len() + 1;
    assert_eq!(
        synced - before.0,
        expected as u64,
        "commit must sync exactly the checkpoint's {} state files, {} batches \
         and dependencies.json",
        dependencies.state_files.len(),
        dependencies.batches.len()
    );
    assert!(
        commit_alls > before.1,
        "commit must be timed as one batch, saw {commit_alls} vs {}",
        before.1
    );

    publisher.publish().unwrap();
    handle.kill().unwrap();
}
