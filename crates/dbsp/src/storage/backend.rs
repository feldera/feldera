//! Storage backend APIs for Feldera.
//!
//! This module provides the [`StorageBackend`] trait that need to be
//! implemented by a storage backend.
//!
//! A file transitions from being created to being written to, to being read
//! to (eventually) deleted.
//! The API prevents writing to a file again that is completed/sealed.
//! The API also prevents reading from a file that is not completed.
#![warn(missing_docs)]

use feldera_types::config::StorageCacheConfig;
use std::{fs::OpenOptions, path::PathBuf};
use tempfile::TempDir;
use tracing::warn;

pub mod memory_impl;
pub mod posixio_impl;

#[cfg(test)]
mod tests;

pub use feldera_storage::{
    block::{BlockLocation, InvalidBlockLocation},
    error::StorageError,
    file::FileId,
    FileReader, FileRw, FileWriter, StorageBackend, StorageFileType, StoragePath, StoragePathPart,
};

/// Extension added to files that are incomplete/being written to.
///
/// A file that is created with `create` or `create_named` will add
/// `.mut` to its filename which is removed when we call `complete()`.
const MUTABLE_EXTENSION: &str = ".mut";

/// Returns a per-thread temporary directory.
pub fn tempdir_for_thread() -> PathBuf {
    thread_local! {
        pub static TEMPDIR: TempDir = tempfile::tempdir().unwrap();
    }
    TEMPDIR.with(|dir| dir.path().to_path_buf())
}

trait StorageCacheFlags {
    fn cache_flags(&mut self, cache: &StorageCacheConfig) -> &mut Self;
}

impl StorageCacheFlags for OpenOptions {
    fn cache_flags(&mut self, cache: &StorageCacheConfig) -> &mut Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            self.custom_flags(cache.to_custom_open_flags());
        }
        self
    }
}
