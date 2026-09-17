//! `FileWriter::complete` must not fsync.
//!
//! Durability belongs to `FileCommitter::commit`, which the checkpoint commit
//! phase runs after the pipeline has resumed. Syncing in `complete` instead
//! fsyncs every file twice, both times on the thread that wrote it, and
//! serializes each sync behind the previous one so no two are ever in flight.
//!
//! This test reads `FILES_SYNCED`, a process-wide counter, so it must be the
//! only test in this file: a second test would run concurrently and count into
//! the same total.

use dbsp::circuit::metrics::FILES_SYNCED;
use dbsp::storage::backend::posixio_impl::PosixBackend;
use feldera_storage::{StorageBackend, fbuf::FBuf};
use feldera_types::config::{FileBackendConfig, StorageCacheConfig};
use std::sync::Arc;
use std::sync::atomic::Ordering;

fn files_synced() -> u64 {
    FILES_SYNCED.load(Ordering::Relaxed)
}

/// Keep this the only `#[test]` in this file: it reads process-global
/// counters, so a second test would run concurrently and make both flaky.
#[test]
fn complete_defers_fsync_to_commit() {
    let tmpdir = tempfile::tempdir().unwrap();
    let backend: Arc<dyn StorageBackend> = Arc::new(PosixBackend::new(
        tmpdir.path(),
        StorageCacheConfig::default(),
        &FileBackendConfig::default(),
    ));
    assert_eq!(
        files_synced(),
        0,
        "test must own the process to read a global counter"
    );

    let contents = [0xa5; 512];
    let mut writer = backend.create().unwrap();
    writer.write_block(FBuf::from_slice(&contents)).unwrap();

    let reader = writer.complete().unwrap();
    assert_eq!(files_synced(), 0, "complete() must not fsync");

    // Readable regardless: complete() renamed the file out of its .mut name,
    // which is all it promises.
    let path = reader.path().clone();
    reader.mark_for_checkpoint();
    let size = reader.get_size().unwrap();
    assert_eq!(size, contents.len() as u64);

    reader.commit().unwrap();
    assert_eq!(files_synced(), 1, "commit() must fsync");

    // Reopening does not sync, so a second commit is the only way the count
    // moves again. This pins commit() as the single fsync site.
    drop(reader);
    let reopened = backend.open(&path).unwrap();
    assert_eq!(files_synced(), 1, "open() must not fsync");
    reopened.commit().unwrap();
    assert_eq!(files_synced(), 2, "commit() must fsync every time");
}
