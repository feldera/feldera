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
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, OnceLock,
    },
};

use serde::{ser::SerializeStruct, Serialize, Serializer};
use thiserror::Error;
use uuid::Uuid;

use crate::storage::buffer_cache::FBuf;
pub mod metrics;

pub mod io_uring_impl;
pub mod memory_impl;
pub mod monoio_impl;
pub mod posixio_impl;

#[cfg(test)]
pub(crate) mod tests;

/// A global counter for default backends that are initiated per-core.
static NEXT_FILE_HANDLE: OnceLock<Arc<AtomicIncrementOnlyI64>> = OnceLock::new();

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
        let name = uuid.to_string() + ".feldera";
        self.create_named(Path::new(&name))
    }

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

//pub use monoio_impl::MonoioBackend as DefaultBackend;
//pub use memory_impl::MemoryBackend as DefaultBackend;
//pub use posixio_impl::PosixBackend as DefaultBackend;
pub use io_uring_impl::IoUringBackend as DefaultBackend;
