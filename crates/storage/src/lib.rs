//! Common Types and Trait Definition for Storage in Feldera.

use std::collections::HashSet;
use std::io::{Cursor, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

use feldera_types::checkpoint::{CheckpointMetadata, PSpineBatches};
use feldera_types::config::{StorageBackendConfig, StorageConfig, StorageOptions};
use serde::de::DeserializeOwned;
use tracing::warn;
use uuid::Uuid;

use crate::block::BlockLocation;
use crate::error::StorageError;
use crate::fbuf::FBuf;
use crate::file::HasFileId;

pub use object_store::path::{Path as StoragePath, PathPart as StoragePathPart};

pub mod block;
pub mod checkpoint_synchronizer;
pub mod error;
pub mod fbuf;
pub mod file;
pub mod histogram;
pub mod metrics;
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
    ) -> Result<Arc<dyn StorageBackend>, StorageError>;
}

inventory::collect!(&'static dyn StorageBackendFactory);

/// A storage backend.
pub trait StorageBackend: Send + Sync {
    /// Create a new file with the given `name`, automatically creating any
    /// parent directories within `name` that don't already exist.
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError>;

    /// Creates a new persistent file used for writing data. The backend selects
    /// a name.
    fn create(&self) -> Result<Box<dyn FileWriter>, StorageError> {
        self.create_with_prefix(&StoragePath::default())
    }

    /// Creates a new persistent file used for writing data, giving the file's
    /// name the specified `prefix`. See also [`create`](Self::create).
    fn create_with_prefix(
        &self,
        prefix: &StoragePath,
    ) -> Result<Box<dyn FileWriter>, StorageError> {
        let uuid = Uuid::now_v7();
        let name = format!("{}{}{}", prefix, uuid, CREATE_FILE_EXTENSION);
        self.create_named(&name.into())
    }

    /// Opens `name` for reading.
    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError>;

    /// Returns the base directory path on the local file system if the storage backend
    /// uses local disk.
    fn file_system_path(&self) -> Option<&Path> {
        None
    }

    /// Calls `cb` with the name of each of the files under `parent`. This is a
    /// non-recursive list: it does not include files under sub-directories of
    /// `parent`.
    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(&StoragePath, StorageFileType),
    ) -> Result<(), StorageError>;

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError>;

    fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError>;

    fn delete_if_exists(&self, name: &StoragePath) -> Result<(), StorageError> {
        match self.delete(name) {
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            other => other,
        }
    }

    fn exists(&self, name: &StoragePath) -> Result<bool, StorageError> {
        match self.open(name) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Reads `name` and returns its contents.  The file `name` is relative to
    /// the base of the storage backend.
    fn read(&self, name: &StoragePath) -> Result<Arc<FBuf>, StorageError> {
        let reader = self.open(name)?;
        let size = reader.get_size()?.try_into().unwrap();
        reader.read_block(BlockLocation { offset: 0, size })
    }

    /// Writes `content` to `name`, automatically creating any parent
    /// directories within `name` that don't already exist.
    fn write(&self, name: &StoragePath, content: FBuf) -> Result<(), StorageError> {
        let mut writer = self.create_named(name)?;
        writer.write_block(content)?;
        let (reader, _path) = writer.complete()?;
        reader.mark_for_checkpoint();
        Ok(())
    }

    /// Returns a value that represents the number of bytes of storage in use.
    /// The storage backend updates this value when its own functions cause more
    /// or less storage to be used:
    ///
    /// - Writing to a file.
    ///
    /// - Deleting a file (by dropping a [FileWriter] without completing, or by
    ///   dropping a [FileReader] without marking it for a checkpoint, or by
    ///   calling functions to delete files.
    ///
    /// The backend is *not* required to:
    ///
    /// - Initially report how much storage is in use. Instead, it just starts
    ///   out at zero. The client can traverse the storage itself and store the
    ///   correct initial value.
    ///
    /// - Detect changes made by a different backend or outside any backend.
    ///
    /// The value is signed because the problems above can cause it to become
    /// negative.
    fn usage(&self) -> Arc<AtomicI64>;
}

impl dyn StorageBackend {
    /// Creates and returns a new backend configured according to `config` and `options`.
    pub fn new(
        config: &StorageConfig,
        options: &StorageOptions,
    ) -> Result<Arc<Self>, StorageError> {
        Self::warn_about_tmpfs(config.path());
        for variable_provider in inventory::iter::<&dyn StorageBackendFactory> {
            if variable_provider.backend() == options.backend.to_string() {
                return variable_provider.create(config, &options.backend);
            }
        }
        Err(StorageError::BackendNotSupported(Box::new(
            options.backend.clone(),
        )))
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

    pub fn gather_batches_for_checkpoint_uuid(
        &self,
        cpm: uuid::Uuid,
    ) -> Result<HashSet<StoragePath>, StorageError> {
        assert!(!cpm.is_nil());

        let mut spines = Vec::new();
        self.list(&cpm.to_string().into(), &mut |path, _file_type| {
            if path
                .filename()
                .is_some_and(|filename| filename.starts_with("pspine-batches"))
            {
                spines.push(path.clone());
            }
        })?;

        let mut batch_files_in_commit: HashSet<StoragePath> = HashSet::new();
        for spine in spines {
            let pspine_batches = self.read_json::<PSpineBatches>(&spine)?;
            for file in pspine_batches.files {
                batch_files_in_commit.insert(file.into());
            }
        }

        Ok(batch_files_in_commit)
    }

    pub fn gather_batches_for_checkpoint(
        &self,
        cpm: &CheckpointMetadata,
    ) -> Result<HashSet<StoragePath>, StorageError> {
        self.gather_batches_for_checkpoint_uuid(cpm.uuid)
    }

    /// Writes `content` to `name` as JSON, automatically creating any parent
    /// directories within `name` that don't already exist.
    pub fn write_json<V>(&self, name: &StoragePath, value: &V) -> Result<(), StorageError>
    where
        V: serde::Serialize,
    {
        let mut content = FBuf::new();
        serde_json::to_writer(&mut content, value).unwrap();
        self.write(name, content)
    }
}

// For an explanation of the `+ '_` here, see:
// https://stackoverflow.com/questions/73495603/trait-problem-borrowed-data-escapes-outside-of-associated-function
impl dyn StorageBackend + '_ {
    /// Reads `name` as JSON.
    pub fn read_json<V>(&self, name: &StoragePath) -> Result<V, StorageError>
    where
        V: DeserializeOwned,
    {
        let content = self.read(name)?;
        serde_json::from_reader(Cursor::new(content.as_ref()))
            .map_err(|e| StorageError::JsonError(e.to_string()))
    }
}

/// A file being written.
///
/// The file can't be read until it is completed with
/// [FileWriter::complete]. Until then, the file is temporary and will be
/// deleted if it is dropped.
pub trait FileWriter: Send + Sync + HasFileId {
    /// Writes `data` at the end of the file. len()` must be a multiple of 512.
    /// Returns the data that was written encapsulated in an `Arc`.
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError>;

    /// Completes writing of a file and returns a reader for the file and the
    /// file's path. The file is treated as temporary and will be deleted if the
    /// reader is dropped without first calling
    /// [FileReader::mark_for_checkpoint].
    fn complete(self: Box<Self>) -> Result<(Arc<dyn FileReader>, StoragePath), StorageError>;
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
    #[allow(clippy::type_complexity)]
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
#[allow(clippy::type_complexity)]
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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StorageFileType {
    /// A regular file.
    File {
        /// File size in bytes.
        size: u64,
    },

    /// A directory.
    ///
    /// Only some kinds of storage backends support directories. The ones that
    /// don't still allow files to be named hierarchically, but they don't
    /// support creating or deleting directories independently from the files in
    /// them. That is, with such a backend, a directory is effectively created
    /// by creating a file in it, and is effectively deleted when the last file
    /// in it is deleted.
    Directory,

    /// Something else.
    Other,
}
