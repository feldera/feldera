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

use feldera_types::config::{
    FileBackendConfig, StorageBackendConfig, StorageCacheConfig, StorageConfig, StorageOptions,
};
use serde::{ser::SerializeStruct, Serialize, Serializer};
use std::{
    fmt::Display,
    fs::OpenOptions,
    io::ErrorKind,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
};
use tempfile::TempDir;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use crate::storage::buffer_cache::FBuf;

#[cfg(target_os = "linux")]
pub mod io_uring_impl;
pub mod memory_impl;
pub mod posixio_impl;

#[cfg(test)]
mod tests;

/// Extension added to files that are incomplete/being written to.
///
/// A file that is created with `create` or `create_named` will add
/// `.mut` to its filename which is removed when we call `complete()`.
const MUTABLE_EXTENSION: &str = ".mut";

/// Extension for batch files used by the engine.
const CREATE_FILE_EXTENSION: &str = ".feldera";

/// Helper function that appends to a [`PathBuf`].
fn append_to_path(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p.into_os_string();
    p.push(s);
    p.into()
}

/// An error that can occur when using the storage backend.
#[derive(Clone, Error, Debug)]
pub enum StorageError {
    /// I/O error.
    #[error("{0}")]
    StdIo(ErrorKind),

    /// A process already locked the provided storage directory.
    ///
    /// If this is not expected, please remove the lock file manually, after verifying
    /// that the process with the given PID no longer exists.
    #[error("A process with PID {0} is already using the storage directory {1:?}.")]
    StorageLocked(u32, PathBuf),

    /// Unknown checkpoint specified in configuration.
    #[error("Couldn't find the specified checkpoint ({0:?}).")]
    CheckpointNotFound(Uuid),

    /// Cannot perform operation because storage is not enabled.
    #[error("Cannot perform operation because storage is not enabled.")]
    StorageDisabled,
}

impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::StdIo(value.kind())
    }
}

impl Serialize for StorageError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::StdIo(error) => {
                let mut ser = serializer.serialize_struct("IOError", 2)?;
                ser.serialize_field("kind", &error.to_string())?;
                ser.end()
            }
            error => error.serialize(serializer),
        }
    }
}

/// A unique identifier for a [FileReader] or [FileWriter].
///
/// The buffer cache uses this ID for indexing.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(u64);

impl FileId {
    /// Creates a fresh unique identifier.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn after(&self) -> Self {
        Self(self.0 + 1)
    }
}

/// An object that has a unique ID.
pub trait HasFileId {
    /// Returns the object's unique ID.
    fn file_id(&self) -> FileId;
}

/// A file being written.
///
/// The file can't be read until it is completed with
/// [FileWriter::complete]. Until then, the file is temporary and will be
/// deleted if it is dropped.
pub trait FileWriter: HasFileId {
    /// Writes `data` at the given byte `offset`.  `offset` must be a multiple
    /// of 512 and `data.len()` must be a multiple of 512.  Returns the data
    /// that was written encapsulated in an `Arc`.
    fn write_block(&mut self, offset: u64, data: FBuf) -> Result<Arc<FBuf>, StorageError>;

    /// Completes writing of a file and returns a reader for the file and the
    /// file's path. The file is treated as temporary and will be deleted if the
    /// reader is dropped without first calling
    /// [FileReader::mark_for_checkpoint].
    fn complete(self: Box<Self>) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError>;
}

/// A readable file.
pub trait FileReader: Send + Sync + HasFileId {
    /// Marks a file to be part of a checkpoint.
    ///
    /// This is used to prevent the file from being deleted when it is dropped.
    /// This is only useful for files obtained via [FileWriter::complete],
    /// because files that were opened with [StorageBackend::open] are never
    /// deleted on drop.
    fn mark_for_checkpoint(&self);

    /// Reads data at `location` from the file.  If successful, the result will
    /// be exactly the requested length; that is, this API treats read past EOF
    /// as an error.
    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError>;

    /// Initiates an asynchronous read.  When the read completes, `callback`
    /// will be called.
    ///
    /// The default implementation is not actually asynchronous.
    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        default_read_async(self, blocks, callback);
    }

    /// Returns the file's size in bytes.
    fn get_size(&self) -> Result<u64, StorageError>;
}

/// Default implementation for [FileReader::read_async].
///
/// This implementation is not actually asynchronous.
pub fn default_read_async<R>(
    reader: &R,
    blocks: Vec<BlockLocation>,
    callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
) where
    R: FileReader + ?Sized,
{
    callback(
        blocks
            .into_iter()
            .map(|location| reader.read_block(location))
            .collect(),
    )
}

/// A storage backend.
pub trait StorageBackend {
    /// Create a new file with the given `name`, which is relative to the
    /// backend's base directory.
    fn create_named(&self, name: &Path) -> Result<Box<dyn FileWriter>, StorageError>;

    /// Creates a new persistent file used for writing data. The backend selects
    /// a name.
    fn create(&self) -> Result<Box<dyn FileWriter>, StorageError> {
        self.create_with_prefix("")
    }

    /// Creates a new persistent file used for writing data, giving the file's
    /// name the specified `prefix`. See also [`create`](Self::create).
    fn create_with_prefix(&self, prefix: &str) -> Result<Box<dyn FileWriter>, StorageError> {
        let uuid = Uuid::now_v7();
        let name = format!("{}{}{}", prefix, uuid, CREATE_FILE_EXTENSION);
        let name_path = Path::new(&name);
        self.create_named(name_path)
    }

    /// Opens a file for reading.  The file `name` is relative to the base of
    /// the storage backend.
    fn open(&self, name: &Path) -> Result<Arc<dyn FileReader>, StorageError>;
}

/// Returns a per-thread temporary directory.
pub fn tempdir_for_thread() -> PathBuf {
    thread_local! {
        pub static TEMPDIR: TempDir = tempfile::tempdir().unwrap();
    }
    TEMPDIR.with(|dir| dir.path().to_path_buf())
}

impl dyn StorageBackend {
    /// Creates and returns a new backend configured according to `config` and `options`.
    pub fn new(config: &StorageConfig, options: &StorageOptions) -> Result<Rc<Self>, StorageError> {
        Self::warn_about_tmpfs(config.path());

        match &options.backend {
            StorageBackendConfig::Default => Ok(Self::new_default(config)),
            StorageBackendConfig::File(options) => Ok(Self::new_file(config, options)),
            StorageBackendConfig::IoUring => Ok(Self::new_iouring(config)),
        }
    }

    fn is_tmpfs(_path: &Path) -> bool {
        #[cfg(target_os = "linux")]
        {
            use nix::sys::statfs;
            statfs::statfs(_path).is_ok_and(|s| s.filesystem_type() == statfs::TMPFS_MAGIC)
        }

        #[cfg(not(target_os = "linux"))]
        false
    }

    fn warn_about_tmpfs(path: &Path) {
        if Self::is_tmpfs(path) {
            static ONCE: std::sync::Once = std::sync::Once::new();
            ONCE.call_once(|| {
                warn!("initializing storage on in-memory tmpfs filesystem at {}; consider configuring physical storage", path.display())
            });
        }
    }

    fn new_file(config: &StorageConfig, options: &FileBackendConfig) -> Rc<Self> {
        Rc::new(posixio_impl::PosixBackend::new(
            config.path(),
            config.cache,
            options,
        ))
    }

    fn new_default(config: &StorageConfig) -> Rc<Self> {
        Self::new_file(config, &FileBackendConfig::default())
    }

    fn new_iouring(config: &StorageConfig) -> Rc<Self> {
        static ONCE: std::sync::Once = std::sync::Once::new();

        #[cfg(target_os = "linux")]
        match io_uring_impl::IoUringBackend::new(config.path(), config.cache) {
            Ok(backend) => return Rc::new(backend),
            Err(error) => {
                ONCE.call_once(|| {
                    warn!("could not initialize io_uring backend ({error}), falling back to POSIX I/O")
                });
            }
        }

        #[cfg(not(target_os = "linux"))]
        ONCE.call_once(|| {
            warn!("io_uring backend not supported on this operating system, falling back to POSIX I/O")
        });

        Self::new_default(config)
    }
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

/// Maximum number of buffers that system calls accept in one operation.
///
/// We only use multibuffer system calls on Linux, so the value is arbitrary
/// elsewhere.
static IOV_MAX: LazyLock<usize> = LazyLock::new(|| {
    #[cfg(target_os = "linux")]
    match nix::unistd::sysconf(nix::unistd::SysconfVar::IOV_MAX) {
        Ok(Some(iov_max)) if iov_max > 0 => iov_max as usize,
        _ => {
            // Typical Linux value.
            1024
        }
    }

    #[cfg(not(target_os = "linux"))]
    1024
});

/// A range of bytes in a file that doesn't satisfy the constraints for
/// [BlockLocation].
#[derive(Copy, Clone, Debug)]
pub struct InvalidBlockLocation {
    /// Byte offset.
    pub offset: u64,

    /// Number of bytes.
    pub size: usize,
}

/// A block that can be read or written in a [FileReader] or [FileWriter].
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockLocation {
    /// Byte offset, a multiple of 512.
    pub offset: u64,

    /// Size in bytes, a multiple of 512, less than `2**31`.
    ///
    /// (The upper limit is because some kernel APIs return the number of bytes
    /// read as an `i32`.)
    pub size: usize,
}

impl BlockLocation {
    /// Constructs a new [BlockLocation], validating `offset` and `size`.
    pub fn new(offset: u64, size: usize) -> Result<Self, InvalidBlockLocation> {
        if (offset % 512) != 0 || !(512..1 << 31).contains(&size) || (size % 512) != 0 {
            Err(InvalidBlockLocation { offset, size })
        } else {
            Ok(Self { offset, size })
        }
    }

    /// File offset just after this block.
    pub fn after(&self) -> u64 {
        self.offset + self.size as u64
    }
}

impl Display for BlockLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} bytes at offset {}", self.size, self.offset)
    }
}
