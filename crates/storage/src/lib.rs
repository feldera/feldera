//! Common Types and Trait Definition for Storage in Feldera.

use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use feldera_types::config::{StorageBackendConfig, StorageConfig, StorageOptions};
use tracing::warn;
use uuid::Uuid;

use crate::block::BlockLocation;
use crate::error::StorageError;
use crate::fbuf::FBuf;
use crate::file::HasFileId;

pub mod block;
pub mod error;
pub mod fbuf;
pub mod file;
pub mod tokio;

/// Extension for batch files used by the engine.
const CREATE_FILE_EXTENSION: &str = ".feldera";

/// Helper function that appends to a [`PathBuf`].
pub fn append_to_path(p: PathBuf, s: &str) -> PathBuf {
    let mut p = p.into_os_string();
    p.push(s);
    p.into()
}

pub trait StorageBackendFactory: Sync {
    fn backend(&self) -> &'static str;
    fn create(
        &self,
        storage_config: &StorageConfig,
        backend_config: &StorageBackendConfig,
    ) -> Result<Rc<dyn StorageBackend>, StorageError>;
}

inventory::collect!(&'static dyn StorageBackendFactory);

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

impl dyn StorageBackend {
    /// Creates and returns a new backend configured according to `config` and `options`.
    pub fn new(config: &StorageConfig, options: &StorageOptions) -> Result<Rc<Self>, StorageError> {
        Self::warn_about_tmpfs(config.path());
        for variable_provider in inventory::iter::<&dyn StorageBackendFactory> {
            if variable_provider.backend() == options.backend.to_string() {
                return variable_provider.create(config, &options.backend);
            }
        }
        Err(StorageError::BackendNotSupported(options.backend.clone()))
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

    /// Returns the file's size in bytes.
    fn get_size(&self) -> Result<u64, StorageError>;
}
