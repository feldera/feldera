//! A layer file records the runtime version of the process that wrote it, and a
//! restore from a checkpoint counts its batches by that version.
//!
//! `RuntimeVersion::set_current` is process-wide and cannot be undone, so this
//! test runs in a binary of its own, and it must be the only test in this file.

use std::path::Path;
use std::sync::Arc;

use dbsp::circuit::checkpointer::RestoredBatchVersions;
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::dynamic::DynData;
use dbsp::operator::Generator;
use dbsp::storage::backend::StorageBackend;
use dbsp::storage::buffer_cache::BufferCache;
use dbsp::storage::file::format::BatchMetadata;
use dbsp::storage::file::reader::read_metadata;
use dbsp::storage::file::writer::{Parameters, Writer1};
use dbsp::storage::file::{Factories, FilterPlan, RuntimeVersion};
use dbsp::typed_batch::OrdZSet;
use dbsp::utils::Tup2;
use dbsp::{Circuit, DBSPHandle, Runtime};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use feldera_types::size::ConfigSize;
use uuid::Uuid;

fn buffer_cache() -> Option<Arc<BufferCache>> {
    thread_local! {
        static BUFFER_CACHE: Arc<BufferCache> = Arc::new(BufferCache::new(1024 * 1024));
    }
    Some(BUFFER_CACHE.with(|cache| cache.clone()))
}

/// Writes a one-row file, passing `caller_version` in its metadata, and
/// returns the version that the file records.
fn write_and_read_version(backend: &dyn StorageBackend, caller_version: &str) -> Option<String> {
    let factories = Factories::<DynData, DynData>::new::<u32, ()>();
    let mut writer = Writer1::new(
        &factories,
        buffer_cache,
        backend,
        Parameters::default(),
        FilterPlan::<DynData>::decide_filter(None, 1),
    )
    .unwrap();
    writer.write0((&1u32, &())).unwrap();
    let path = writer.path().clone();
    let metadata = BatchMetadata {
        runtime_version: RuntimeVersion::new(caller_version),
        ..BatchMetadata::default()
    };
    // Keep the file handle: dropping it deletes the file.
    let (_file, _filter, _bounds) = writer.close(metadata).unwrap();
    read_metadata(buffer_cache, backend, &path)
        .unwrap()
        .runtime_version
        .get()
        .map(|version| version.into_owned())
}

/// A circuit in `path` whose integrals give a checkpoint batches to store, in a
/// flat and an indexed spine.  It restores from `init_checkpoint`, if any.
fn circuit(path: &Path, init_checkpoint: Option<Uuid>) -> DBSPHandle {
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: path.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            min_spill_size: Some(ConfigSize::ZERO),
            ..StorageOptions::default()
        },
    )
    .unwrap()
    .with_init_checkpoint(init_checkpoint);
    let config = CircuitConfig::with_workers(2).with_storage(Some(storage));
    let (handle, ()) = Runtime::init_circuit(config, |circuit| {
        let source = circuit.add_source(Generator::new(|| {
            let keys: Vec<Tup2<u64, i64>> = (0..256).map(|k| Tup2(k, 1i64)).collect();
            OrdZSet::from_keys((), keys)
        }));
        source.integrate_trace().apply(|_| ());
        source
            .map_index(|k| (*k % 7, *k))
            .integrate_trace()
            .apply(|_| ());
        Ok(())
    })
    .unwrap();
    handle
}

/// Runs a circuit in `path` for a step, checkpoints it, restores the checkpoint
/// in a new circuit, and returns what the restore counted.
fn checkpoint_and_restore(path: &Path) -> RestoredBatchVersions {
    let mut handle = circuit(path, None);
    handle.transaction().unwrap();
    let uuid = handle.checkpoint().run().unwrap().uuid;
    drop(handle);
    circuit(path, Some(uuid)).restored_batch_versions().clone()
}

/// Asserts that `versions` counts some batches, all with `version`.
fn assert_all_batches(versions: &RestoredBatchVersions, version: Option<&str>) {
    assert_eq!(
        versions.keys().map(Option::as_deref).collect::<Vec<_>>(),
        vec![version],
        "{versions:?}"
    );
    assert!(versions.values().sum::<usize>() > 0, "{versions:?}");
}

/// Keep this the only `#[test]` in this file: it sets process-wide state.
#[test]
fn files_record_and_restores_count_runtime_version() {
    let tempdir = tempfile::tempdir().unwrap();
    let backend = <dyn StorageBackend>::new(
        &StorageConfig {
            path: tempdir.path().to_string_lossy().to_string(),
            cache: Default::default(),
        },
        &StorageOptions::default(),
    )
    .unwrap();

    // Before the version is set, a file records an unknown version, whatever
    // the caller passes.
    assert_eq!(write_and_read_version(&*backend, "ignored"), None);
    let unknown_dir = tempfile::tempdir().unwrap();
    assert_all_batches(&checkpoint_and_restore(unknown_dir.path()), None);

    RuntimeVersion::set_current("test-runtime-version");
    let known_dir = tempfile::tempdir().unwrap();
    assert_all_batches(
        &checkpoint_and_restore(known_dir.path()),
        Some("test-runtime-version"),
    );
    assert_eq!(
        write_and_read_version(&*backend, "ignored").as_deref(),
        Some("test-runtime-version")
    );

    // Only the first call has an effect.
    RuntimeVersion::set_current("other-version");
    assert_eq!(
        write_and_read_version(&*backend, "ignored").as_deref(),
        Some("test-runtime-version")
    );
}
