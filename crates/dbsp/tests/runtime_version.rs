//! A layer file records the runtime version of the process that wrote it.
//!
//! `RuntimeVersion::set_current` is process-wide and cannot be undone, so this
//! test runs in a binary of its own, and it must be the only test in this file.

use std::sync::Arc;

use dbsp::dynamic::DynData;
use dbsp::storage::backend::StorageBackend;
use dbsp::storage::buffer_cache::BufferCache;
use dbsp::storage::file::format::BatchMetadata;
use dbsp::storage::file::reader::read_metadata;
use dbsp::storage::file::writer::{Parameters, Writer1};
use dbsp::storage::file::{Factories, FilterPlan, RuntimeVersion};
use feldera_types::config::{StorageConfig, StorageOptions};

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

/// Keep this the only `#[test]` in this file: it sets process-wide state.
#[test]
fn writer_records_current_runtime_version() {
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

    RuntimeVersion::set_current("test-runtime-version");
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
