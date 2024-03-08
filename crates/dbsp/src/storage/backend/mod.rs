//! Storage backend APIs for Feldera.
//!
//! This file provides the traits that need to be implemented by a storage
//! backend. The traits are split into three parts:
//! - [`StorageControl`]: for creating and deleting files.
//! - [`StorageWrite`]: for writing data to files.
//! - [`StorageRead`]: for reading data from files.
//! - [`StorageExecutor`]: for executing `Future`s from the other traits.
//!
//! A file transitions from being created to being written to, to being read
//! to (eventually) deleted.
//! The API prevents writing to a file again that is completed/sealed.
//! The API also prevents reading from a file that is not completed.
#![allow(async_fn_in_trait)]
#![warn(missing_docs)]

use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, OnceLock,
    },
};

use metrics::{describe_counter, describe_histogram, Unit};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use thiserror::Error;
use uuid::Uuid;

use crate::storage::buffer_cache::FBuf;

pub mod monoio_impl;

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

#[cfg(test)]
impl FileHandle {
    /// Creating arbitrary file-handles is only necessary for testing, and
    /// dangerous otherwise. Use the StorageControl API instead.
    pub(crate) fn new(fd: i64) -> Self {
        Self(fd)
    }
}

impl From<&FileHandle> for i64 {
    fn from(fd: &FileHandle) -> Self {
        fd.0
    }
}

/// A file-descriptor we can read or prefetch from.
pub struct ImmutableFileHandle(i64);

#[cfg(test)]
impl ImmutableFileHandle {
    /// Creating arbitrary file-handles is only necessary for testing, and
    /// dangerous otherwise. Use the StorageControl API instead.
    pub(crate) fn new(fd: i64) -> Self {
        Self(fd)
    }
}

impl From<&ImmutableFileHandle> for i64 {
    fn from(fd: &ImmutableFileHandle) -> Self {
        fd.0
    }
}

/// An error that can occur when using the storage backend.
#[derive(Error, Debug)]
pub enum StorageError {
    /// I/O error from `monoio` backend.
    #[error("Got IO error during monoio operation")]
    StdIo(#[from] std::io::Error),

    /// Range to be written overlaps with previous write.
    #[error("The range to be written overlaps with a previous write")]
    OverlappingWrites,

    /// Read ended before the full request length.
    #[error("The read would have returned less data than requested.")]
    ShortRead,
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
        #[allow(clippy::match_like_matches_macro)]
        match (self, other) {
            (Self::OverlappingWrites, Self::OverlappingWrites) => true,
            (Self::ShortRead, Self::ShortRead) => true,
            _ => false,
        }
    }
}

pub(crate) const METRIC_FILES_CREATED: &str = "disk.total_files_created";
pub(crate) const METRIC_FILES_DELETED: &str = "disk.total_files_deleted";
pub(crate) const METRIC_WRITES_SUCCESS: &str = "disk.total_writes_success";
pub(crate) const METRIC_WRITES_FAILED: &str = "disk.total_writes_failed";
pub(crate) const METRIC_READS_SUCCESS: &str = "disk.total_reads_success";
pub(crate) const METRIC_READS_FAILED: &str = "disk.total_reads_failed";
pub(crate) const METRIC_TOTAL_BYTES_WRITTEN: &str = "disk.total_bytes_written";
pub(crate) const METRIC_TOTAL_BYTES_READ: &str = "disk.total_bytes_read";
pub(crate) const METRIC_READ_LATENCY: &str = "disk.read_latency";
pub(crate) const METRIC_WRITE_LATENCY: &str = "disk.write_latency";
pub(crate) const METRIC_BUFFER_CACHE_HIT: &str = "disk.buffer_cache_hit";
pub(crate) const METRIC_BUFFER_CACHE_MISS: &str = "disk.buffer_cache_miss";
pub(crate) const METRIC_BUFFER_CACHE_LATENCY: &str = "disk.buffer_cache_latency";

/// Adds descriptions for the metrics we expose.
fn describe_disk_metrics() {
    // Storage backend metrics.
    describe_counter!(METRIC_FILES_CREATED, "total number of files created");
    describe_counter!(METRIC_FILES_DELETED, "total number of files deleted");
    describe_counter!(METRIC_WRITES_SUCCESS, "total number of disk writes");
    describe_counter!(METRIC_WRITES_FAILED, "total number of failed writes");
    describe_counter!(METRIC_READS_SUCCESS, "total number of disk reads");
    describe_counter!(METRIC_READS_FAILED, "total number of failed reads");

    describe_counter!(
        METRIC_TOTAL_BYTES_WRITTEN,
        Unit::Bytes,
        "total number of bytes written to disk"
    );
    describe_counter!(
        METRIC_TOTAL_BYTES_READ,
        Unit::Bytes,
        "total number of bytes read from disk"
    );

    describe_histogram!(METRIC_READ_LATENCY, Unit::Seconds, "Read request latency");
    describe_histogram!(METRIC_WRITE_LATENCY, Unit::Seconds, "Write request latency");

    // Buffer cache metrics.
    describe_counter!(METRIC_BUFFER_CACHE_HIT, "total number of buffer cache hits");
    describe_counter!(
        METRIC_BUFFER_CACHE_MISS,
        "total number of buffer cache misses"
    );
    describe_histogram!(
        METRIC_BUFFER_CACHE_LATENCY,
        Unit::Seconds,
        "Buffer cache lookup latency"
    );
}

#[cfg(test)]
impl Eq for StorageError {}

/// A trait for a storage backend to implement so client can create/delete
/// files.
pub trait StorageControl {
    /// Create a new file. See also [`create`](Self::create).
    async fn create_named<P: AsRef<Path>>(&self, name: P) -> Result<FileHandle, StorageError>;

    /// Creates a new persistent file used for writing data.
    ///
    /// Returns a file-descriptor that can be used for writing data.
    /// Note that it is not possible to read from this file until
    /// [`StorageWrite::complete`] is called and the [`FileHandle`] is
    /// converted to an [`ImmutableFileHandle`].
    async fn create(&self) -> Result<FileHandle, StorageError> {
        let uuid = Uuid::now_v7();
        let name = uuid.to_string() + ".feldera";
        self.create_named(&name).await
    }

    /// Deletes a previously completed file.
    ///
    /// This removes the file from the storage backend and makes it unavailable
    /// for reading.
    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError>;

    /// Deletes a previously created file.
    ///
    /// This removes the file from the storage backend and makes it unavailable
    /// for writing.
    ///
    /// Use [`delete`](Self::delete) for deleting a file that has been
    /// completed.
    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError>;
}

/// A trait for a storage backend to implement so clients can write to files.
pub trait StorageWrite {
    /// Allocates a buffer suitable for writing to a file using Direct I/O over
    /// `io_uring`.
    fn allocate_buffer(sz: usize) -> FBuf {
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
    async fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Arc<FBuf>, StorageError>;

    /// Completes writing of a file.
    ///
    /// This makes the file available for reading by returning a file-descriptor
    /// that can be used for reading data.
    ///
    /// ## Arguments
    /// - `fd` is the file-handle to complete.
    ///
    /// ## Returns
    /// - A file-descriptor that can be used for reading data. See also
    /// [`StorageRead`].
    /// - The on-disk location of the file.
    async fn complete(
        &self,
        fd: FileHandle,
    ) -> Result<(ImmutableFileHandle, PathBuf), StorageError>;
}

/// A trait for a storage backend to implement so clients can read from files.
pub trait StorageRead {
    /// Prefetches a block of data from a file.
    ///
    /// This is an asynchronous operation that will be completed in the
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
    async fn prefetch(&self, fd: &ImmutableFileHandle, offset: u64, size: usize);

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
    ///   we return [`StorageError::ShortRead`], as opposed to a partial result.
    ///
    /// API returns an error if any of the above pre/post-conditions are not
    /// met.
    ///
    /// ## Returns
    /// A [`FBuf`] containing the data read from the file.
    async fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Arc<FBuf>, StorageError>;

    /// Returns the file's size in bytes.
    async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError>;
}

/// A trait for a storage backend to implement so clients can wait on
/// [`Future`]s.
pub trait StorageExecutor {
    /// Runs `future` to completion in the storage backend's executor.
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future;
}

pub use monoio_impl::MonoioBackend as DefaultBackend;
