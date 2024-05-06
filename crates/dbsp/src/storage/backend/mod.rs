//! Storage backend APIs for Feldera.
//!
//! This module provides the [`Storage`] trait that need to be implemented by a
//! storage backend.
//!
//! A file transitions from being created to being written to, to being read
//! to (eventually) deleted.
//! The API prevents writing to a file again that is completed/sealed.
//! The API also prevents reading from a file that is not completed.
#![warn(missing_docs)]

use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, OnceLock,
    },
};

use log::warn;
use pipeline_types::config::StorageCacheConfig;
use serde::{ser::SerializeStruct, Serialize, Serializer};
use tempfile::TempDir;
use thiserror::Error;
use uuid::Uuid;

use crate::storage::buffer_cache::FBuf;

pub mod metrics;

#[cfg(feature = "io_uring")]
pub mod io_uring_impl;
pub mod memory_impl;
pub mod monoio_impl;
pub mod posixio_impl;

#[cfg(test)]
pub(crate) mod tests;

/// Extension added to files that are incomplete/being written to.
///
/// A file that is created with `create` or `create_named` will add
/// `.mut` to its filename which is removed when we call `complete()`.
const MUTABLE_EXTENSION: &str = ".mut";

/// Extension for batch files used by the engine.
const CREATE_FILE_EXTENSION: &str = ".feldera";

/// A global counter for default backends that are initiated per-core.
static NEXT_FILE_HANDLE: OnceLock<Arc<AtomicIncrementOnlyI64>> = OnceLock::new();

/// Helper function that appends to a [`PathBuf`].
fn append_to_path(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p.into_os_string();
    p.push(s);
    p.into()
}

/// An Increment Only Atomic.
///
/// Usable as a global counter to get unique identifiers for file-handles.
///
/// Because the buffer cache can be shared among multiple threads.
#[derive(Debug, Default)]
pub struct AtomicIncrementOnlyI64 {
    value: AtomicI64,
}

impl AtomicIncrementOnlyI64 {
    /// Creates a new counter with an initial value of 0.
    pub const fn new() -> Self {
        Self {
            value: AtomicI64::new(0),
        }
    }

    fn increment(&self) -> i64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }
}

/// A file-descriptor we can write to.
pub struct FileHandle(i64);

impl From<&FileHandle> for i64 {
    fn from(fd: &FileHandle) -> Self {
        fd.0
    }
}

/// A file-descriptor we can read or prefetch from.
pub struct ImmutableFileHandle(i64);

impl From<&ImmutableFileHandle> for i64 {
    fn from(fd: &ImmutableFileHandle) -> Self {
        fd.0
    }
}

/// An error that can occur when using the storage backend.
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error.
    #[error("{0}")]
    StdIo(#[from] std::io::Error),

    /// Range to be written overlaps with previous write.
    #[error("The range to be written overlaps with a previous write")]
    OverlappingWrites,

    /// Read ended before the full request length.
    #[error("The read would have returned less data than requested.")]
    ShortRead,

    /// Storage location not found.
    #[error("The requested (base) directory for storage does not exist.")]
    StorageLocationNotFound,

    /// A process already locked the provided storage directory.
    ///
    /// If this is not expected, please remove the lock file manually, after verifying
    /// that the process with the given PID no longer exists.
    #[error("A process with PID {0} is already using the storage directory {1:?}.")]
    StorageLocked(u32, PathBuf),

    /// Unable to lock the PID file for the storage directory.
    ///
    /// This means another pipeline started and tried to lock it at the same time.
    #[error("Unable to lock the PID file ({1:?}) for the storage directory.")]
    UnableToLockPidFile(i32, PathBuf),

    /// Unknown checkpoint specified in configuration.
    #[error("Couldn't find the specified checkpoint ({0:?}).")]
    CheckpointNotFound(Uuid),
}

impl Serialize for StorageError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::StdIo(error) => {
                let mut ser = serializer.serialize_struct("IOError", 2)?;
                ser.serialize_field("kind", &error.kind().to_string())?;
                ser.serialize_field("os_error", &error.raw_os_error())?;
                ser.end()
            }
            error => error.serialize(serializer),
        }
    }
}

#[cfg(test)]
/// Implementation of PartialEq for StorageError.
///
/// This is for testing only and therefore intentionally not a complete
/// implementation.
impl PartialEq for StorageError {
    fn eq(&self, other: &Self) -> bool {
        fn is_unexpected_eof(error: &StorageError) -> bool {
            use std::io::ErrorKind;
            matches!(error, StorageError::StdIo(error) if error.kind() == ErrorKind::UnexpectedEof)
        }
        #[allow(clippy::match_like_matches_macro)]
        match (self, other) {
            (Self::OverlappingWrites, Self::OverlappingWrites) => true,
            (Self::ShortRead, Self::ShortRead) => true,
            (Self::StorageLocationNotFound, Self::StorageLocationNotFound) => true,
            (Self::StorageLocked(pid, path), Self::StorageLocked(other_pid, other_path)) => {
                pid == other_pid && path == other_path
            }
            _ => is_unexpected_eof(self) && is_unexpected_eof(other),
        }
    }
}

#[cfg(test)]
impl Eq for StorageError {}

/// A storage backend.
pub trait Storage {
    /// Create a new file. See also [`create`](Self::create).
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError>;

    /// Creates a new persistent file used for writing data.
    ///
    /// Returns a file-descriptor that can be used for writing data.  Note that
    /// it is not possible to read from this file until [`Storage::complete`] is
    /// called and the [`FileHandle`] is converted to an
    /// [`ImmutableFileHandle`].
    fn create(&self) -> Result<FileHandle, StorageError> {
        let uuid = Uuid::now_v7();
        let name = uuid.to_string() + CREATE_FILE_EXTENSION;
        let name_path = Path::new(&name);
        self.create_named(name_path)
    }

    /// Opens a file for reading.
    ///
    /// # Arguments
    /// - `name` is the name of the file to open. It is relative to the base of
    ///   the storage backend.
    fn open(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError>;

    /// Deletes a previously completed file.
    ///
    /// This removes the file from the storage backend and makes it unavailable
    /// for reading.
    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError>;

    /// Deletes a previously created file.
    ///
    /// This removes the file from the storage backend and makes it unavailable
    /// for writing.
    ///
    /// Use [`delete`](Self::delete) for deleting a file that has been
    /// completed.
    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError>;

    /// Evicts in-memory, cached contents for a file.
    ///
    /// This is useful if we're sure that a file is not going to be read again
    /// during this run of the program, and we want to free up memory.
    ///
    /// This is a no-op for storage backends that do not cache data.
    fn evict(&self, _fd: ImmutableFileHandle) -> Result<(), StorageError> {
        Ok(())
    }

    /// Returns the root of the storage backend.
    fn base(&self) -> &Path;

    /// Allocates a buffer suitable for writing to a file using Direct I/O over
    /// `io_uring`.
    fn allocate_buffer(&self, sz: usize) -> FBuf {
        FBuf::with_capacity(sz)
    }

    /// Writes a block of data to a file.
    ///
    /// ## Arguments
    /// - `fd` is the file-handle to write to.
    /// - `offset` is the offset in the file to write to.
    /// - `data` is the data to write.
    ///
    /// ## Preconditions
    /// - `offset.is_power_of_two()`
    /// - `data.len() >= 512 && data.len().is_power_of_two()`
    ///
    /// ## Returns
    /// A reference to the (now cached) buffer.
    ///
    /// API returns an error if any of the above preconditions are not met.
    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError>;

    /// Completes writing of a file.
    ///
    /// This makes the file available for reading by returning a file-descriptor
    /// that can be used for reading data.
    ///
    /// ## Arguments
    /// - `fd` is the file-handle to complete.
    ///
    /// ## Returns
    /// - A file-descriptor that can be used for reading data.
    /// - The on-disk location of the file.
    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError>;

    /// Prefetches a block of data from a file.
    ///
    /// This is an hronous operation that will be completed in the
    /// background. The data is likely available for reading from DRAM once the
    /// prefetch operation has completed.
    /// The implementation is free to choose how to prefetch the data. This may
    /// not be implemented for all storage backends.
    ///
    /// ## Arguments
    /// - `fd` is the file-handle to prefetch from.
    /// - `offset` is the offset in the file to prefetch from.
    /// - `size` is the size of the block to prefetch.
    ///
    /// ## Pre-conditions
    /// - `offset.is_power_of_two()`
    /// - `size >= 512 && size.is_power_of_two()`
    fn prefetch(&self, fd: &ImmutableFileHandle, offset: u64, size: usize);

    /// Reads a block of data from a file.
    ///
    /// ## Arguments
    /// - `fd` is the file-handle to read from.
    /// - `offset` is the offset in the file to read from.
    /// - `size` is the size of the block to read.
    ///
    /// ## Pre-conditions
    /// - `offset.is_power_of_two()`
    /// - `size >= 512 && size.is_power_of_two()`
    ///
    /// ## Post-conditions
    /// - `result.len() == size`: In case we read less than the required size,
    ///   we return [`UnexpectedEof`], as opposed to a partial result.
    ///
    /// API returns an error if any of the above pre/post-conditions are not
    /// met.
    ///
    /// ## Returns
    /// A [`FBuf`] containing the data read from the file.
    ///
    /// [`UnexpectedEof`]: std::io::ErrorKind::UnexpectedEof
    fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError>;

    /// Returns the file's size in bytes.
    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError>;
}

impl<S> Storage for Box<S>
where
    S: Storage + ?Sized,
{
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError> {
        (**self).create_named(name)
    }

    fn open(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError> {
        (**self).open(name)
    }

    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        (**self).delete(fd)
    }

    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        (**self).delete_mut(fd)
    }

    fn base(&self) -> &Path {
        (**self).base()
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        (**self).write_block(fd, offset, data)
    }

    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        (**self).complete(fd)
    }

    fn prefetch(&self, fd: &ImmutableFileHandle, offset: u64, size: usize) {
        (**self).prefetch(fd, offset, size)
    }

    fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError> {
        (**self).read_block(fd, offset, size)
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        (**self).get_size(fd)
    }
}

/// A dynamically chosen storage engine.
pub type Backend = Box<dyn Storage>;

/// Returns a per-thread temporary directory.
pub fn tempdir_for_thread() -> PathBuf {
    thread_local! {
        pub static TEMPDIR: TempDir = tempfile::tempdir().unwrap();
    }
    TEMPDIR.with(|dir| dir.path().to_path_buf())
}

/// Create and returns a backend of the default kind.
pub fn new_default_backend(tempdir: PathBuf, cache: StorageCacheConfig) -> Backend {
    #[cfg(target_os = "linux")]
    {
        use nix::sys::statfs::{statfs, TMPFS_MAGIC};
        if let Ok(s) = statfs(&tempdir) {
            if s.filesystem_type() == TMPFS_MAGIC {
                static ONCE: std::sync::Once = std::sync::Once::new();
                ONCE.call_once(|| {
                    warn!("initializing storage on in-memory tmpfs filesystem at {}; consider configuring physical storage",
                          tempdir.to_string_lossy())
                });
            }
        }
    }

    #[cfg(feature = "io_uring")]
    match io_uring_impl::IoUringBackend::with_base(&tempdir) {
        Ok(backend) => Box::new(backend),
        Err(error) => {
            static ONCE: std::sync::Once = std::sync::Once::new();
            ONCE.call_once(|| {
                warn!("could not initialize io_uring backend ({error}), falling back to POSIX I/O")
            });
            Box::new(posixio_impl::PosixBackend::with_base(&tempdir, cache))
        }
    }
    Box::new(posixio_impl::PosixBackend::with_base(tempdir, cache))
}

/// Returns a thread-local default backend.
pub fn default_backend_for_thread() -> Rc<Backend> {
    thread_local! {
        pub static DEFAULT_BACKEND: Rc<Backend>
            = Rc::new(new_default_backend(tempdir_for_thread(), StorageCacheConfig::default()));
    }
    DEFAULT_BACKEND.with(|rc| rc.clone())
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
