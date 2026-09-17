//! `StorageBackend::commit_all` must make a checkpoint's files durable in one
//! batch rather than one fsync at a time.
//!
//! The two strategies differ in what they call, not in what they guarantee, so
//! each is pinned by how many per-file syncs it records: `per_file` fsyncs once
//! per file, `syncfs` makes one syscall and fsyncs nothing. Either way the call
//! itself is timed once, which is what makes them comparable.
//!
//! Both histograms are process-wide, so this must be the only test in this
//! file; a second test would run concurrently and record into the same totals.

use dbsp::storage::backend::posixio_impl::PosixBackend;
use feldera_storage::metrics::{COMMIT_ALL_LATENCY_MICROSECONDS, SYNC_LATENCY_MICROSECONDS};
use feldera_storage::{FileCommitter, StorageBackend, fbuf::FBuf};
use feldera_types::config::{FileBackendConfig, StorageCacheConfig, StorageSyncMode};
use std::sync::Arc;

const FILES: usize = 12;

fn backend(path: &std::path::Path, sync_mode: StorageSyncMode) -> Arc<dyn StorageBackend> {
    Arc::new(PosixBackend::new(
        path,
        StorageCacheConfig::default(),
        &FileBackendConfig {
            sync_mode: Some(sync_mode),
            ..Default::default()
        },
    ))
}

/// Writes [FILES] files and returns their committers.
fn write_files(backend: &Arc<dyn StorageBackend>) -> Vec<Arc<dyn FileCommitter>> {
    (0..FILES)
        .map(|i| {
            let path = format!("file-{i}").into();
            backend
                .write(&path, FBuf::from_slice(&[i as u8; 512]))
                .unwrap()
        })
        .collect()
}

/// Observations recorded so far, as (per-file syncs, commit_all calls).
///
/// A histogram counts its observations as well as timing them, so these need no
/// separate counters.
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

/// Keep this the only `#[test]` in this file: it reads process-global
/// histograms, so a second test would run concurrently and make both flaky.
#[test]
fn commit_all_batches_by_strategy() {
    let tmpdir = tempfile::tempdir().unwrap();
    assert_eq!(counts(), (0, 0), "test must own the process");

    // per_file: one fsync per file, no filesystem sync. Writing the files must
    // not sync anything on its own, which is what keeps them off the worker
    // threads.
    let per_file = backend(&tmpdir.path().join("per-file"), StorageSyncMode::PerFile);
    let files = write_files(&per_file);
    assert_eq!(counts(), (0, 0), "writing must not sync");
    per_file.commit_all(&files).unwrap();
    assert_eq!(
        counts(),
        (FILES as u64, 1),
        "per_file fsyncs each file, and the call is timed once"
    );

    // syncfs only exists on Linux; anywhere else `Syncfs` is refused and falls
    // back to per-file, which would fail every assertion below.
    #[cfg(not(target_os = "linux"))]
    let _ = tmpdir;
    #[cfg(target_os = "linux")]
    syncfs_half(&tmpdir);
}

/// The syncfs half of the test above, which only applies on Linux.
///
/// Note that this really does sync the whole filesystem holding the temporary
/// directory, usually the machine's root. That is correct and it is what the
/// strategy does in production, but it writes back unrelated data, so keep the
/// file count here small.
#[cfg(target_os = "linux")]
fn syncfs_half(tmpdir: &tempfile::TempDir) {
    // One syscall, whatever the file count, and no per-file fsync.
    let syncfs = backend(&tmpdir.path().join("syncfs"), StorageSyncMode::Syncfs);
    let files = write_files(&syncfs);
    assert_eq!(counts(), (FILES as u64, 1), "writing must not sync");
    syncfs.commit_all(&files).unwrap();
    assert_eq!(
        counts(),
        (FILES as u64, 2),
        "syncfs fsyncs no individual file, and the call is timed once"
    );

    // An empty checkpoint has nothing to make durable, so it must neither sync
    // nor record a timing that says a sync of nothing was fast.
    syncfs.commit_all(&[]).unwrap();
    assert_eq!(
        counts(),
        (FILES as u64, 2),
        "no files means no sync and no timing"
    );
}
