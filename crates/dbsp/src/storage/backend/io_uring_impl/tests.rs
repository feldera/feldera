//! Tests for [`IoUringBackend`].
//!
//! The main test makes sure we correspond to the model defined in
//! [`InMemoryBackend`].
use std::{path::Path, rc::Rc};

use feldera_types::config::StorageCacheConfig;

use crate::storage::backend::io_uring_impl::IoUringBackend;
use crate::storage::backend::tests::{random_sizes, test_backend};
use crate::storage::backend::StorageBackend;

fn create_iouring_backend(path: &Path) -> Rc<dyn StorageBackend> {
    Rc::new(IoUringBackend::new(path, StorageCacheConfig::default()).unwrap())
}

/// Write 10 MiB total in 1 KiB chunks.  `VectoredWrite` flushes its buffer when it
/// reaches 1 MiB of sequential data, and we limit the amount of queued work
/// to 4 MiB, so this has a chance to trigger both limits.
#[test]
fn sequential_1024() {
    test_backend(
        Box::new(create_iouring_backend),
        &[1024; 1024 * 10],
        true,
        true,
    )
}

/// Write 10 MiB total in 1 KiB chunks.  We skip over a chunk occasionally,
/// which leaves a "hole" in the file that is all zeros and has the side effect
/// of forcing `VectoredWrite` to flush its buffer.  Our actual btree writer
/// never leaves holes but it seems best to test this anyhow.
#[test]
fn holes_1024() {
    test_backend(
        Box::new(create_iouring_backend),
        &[1024; 1024 * 10],
        false,
        true,
    )
}

/// Verify that files get deleted if not marked for a checkpoint.
#[test]
fn delete_1024() {
    test_backend(
        Box::new(create_iouring_backend),
        &[1024; 1024 * 10],
        true,
        false,
    )
}

#[test]
fn sequential_random() {
    test_backend(
        Box::new(create_iouring_backend),
        &random_sizes(),
        true,
        true,
    );
}

#[test]
fn holes_random() {
    test_backend(
        Box::new(create_iouring_backend),
        &random_sizes(),
        false,
        true,
    );
}

#[test]
fn empty() {
    test_backend(Box::new(create_iouring_backend), &[], true, true);
}
