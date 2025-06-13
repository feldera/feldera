//! [StorageBackend] implementation using POSIX I/O.

use super::{
    BlockLocation, FileId, FileReader, FileWriter, HasFileId, StorageCacheFlags, StorageError,
    MUTABLE_EXTENSION,
};
use crate::circuit::metrics::{FILES_CREATED, FILES_DELETED};
use crate::storage::{buffer_cache::FBuf, init};
use feldera_storage::tokio::TOKIO;
use feldera_storage::{
    append_to_path, default_read_async, StorageBackend, StorageBackendFactory, StorageFileType,
    StoragePath, StoragePathPart,
};
use feldera_types::config::{
    FileBackendConfig, StorageBackendConfig, StorageCacheConfig, StorageConfig,
};
use metrics::counter;
use std::ffi::OsString;
use std::fs::{create_dir_all, DirEntry};
use std::io::{ErrorKind, IoSlice, Write};
use std::thread::sleep;
use std::time::Duration;
use std::{
    fs::{self, File, OpenOptions},
    io::Error as IoError,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
};
use tracing::warn;

pub(super) struct PosixReader {
    file: Arc<File>,
    file_id: FileId,
    drop: DeleteOnDrop,

    /// Whether to use background threads for file I/O.
    async_threads: bool,

    /// Per-I/O operation sleep delay, for simulating slow storage devices.
    ioop_delay: Duration,
}

impl PosixReader {
    fn new(
        file: Arc<File>,
        file_id: FileId,
        drop: DeleteOnDrop,
        async_threads: bool,
        ioop_delay: Duration,
    ) -> Self {
        Self {
            file,
            file_id,
            drop,
            async_threads,
            ioop_delay,
        }
    }
    fn open(
        path: PathBuf,
        cache: StorageCacheConfig,
        usage: Arc<AtomicI64>,
        async_threads: bool,
        ioop_delay: Duration,
    ) -> Result<Arc<dyn FileReader>, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&cache)
            .open(&path)?;
        let size = file.metadata()?.size();

        Ok(Arc::new(Self::new(
            Arc::new(file),
            FileId::new(),
            DeleteOnDrop::new(path, true, size, usage),
            async_threads,
            ioop_delay,
        )))
    }
}

impl HasFileId for PosixReader {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl FileReader for PosixReader {
    fn mark_for_checkpoint(&self) {
        self.drop.keep();
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        sleep(self.ioop_delay);
        let mut buffer = FBuf::with_capacity(location.size);

        match buffer.read_exact_at(&self.file, location.offset, location.size) {
            Ok(()) => Ok(Arc::new(buffer)),
            Err(e) => Err(e.into()),
        }
    }

    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        if self.async_threads {
            let file = self.file.clone();
            let ioop_delay = self.ioop_delay;
            TOKIO.spawn_blocking(move || {
                // For background reads, we only sleep once, not once per block.
                sleep(ioop_delay);
                callback(
                    blocks
                        .into_iter()
                        .map(|location| {
                            let mut buffer = FBuf::with_capacity(location.size);
                            match buffer.read_exact_at(&file, location.offset, location.size) {
                                Ok(()) => Ok(Arc::new(buffer)),
                                Err(e) => Err(e.into()),
                            }
                        })
                        .collect(),
                );
            });
        } else {
            default_read_async(self, blocks, callback);
        }
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        Ok(self.drop.size)
    }
}

/// Deletes a file when dropped (unless [Self::keep] is called first).
pub struct DeleteOnDrop {
    path: PathBuf,
    keep: AtomicBool,
    size: u64,
    usage: Arc<AtomicI64>,
}

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        if !self.keep.load(Ordering::Relaxed) {
            if let Err(e) = fs::remove_file(&self.path) {
                warn!("Unable to delete file {:?}: {:?}", self.path, e);
            } else {
                self.usage.fetch_sub(self.size as i64, Ordering::Relaxed);
                counter!(FILES_DELETED).increment(1);
            }
        }
    }
}

impl DeleteOnDrop {
    fn new(path: PathBuf, keep: bool, size: u64, usage: Arc<AtomicI64>) -> Self {
        Self {
            path,
            keep: AtomicBool::new(keep),
            size,
            usage,
        }
    }

    /// Disables deleting the file when dropped.
    pub fn keep(&self) {
        self.keep.store(true, Ordering::Relaxed);
    }
    fn with_path(mut self, path: PathBuf) -> Self {
        self.path = path;
        self
    }
}

/// Meta-data we keep per file we created.
struct PosixWriter {
    file_id: FileId,
    file: File,
    drop: DeleteOnDrop,
    name: StoragePath,

    buffers: Vec<Arc<FBuf>>,
    len: u64,

    async_threads: bool,
    ioop_delay: Duration,
}

impl HasFileId for PosixWriter {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl FileWriter for PosixWriter {
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let block = Arc::new(data);
        self.write(&block)?;
        Ok(block)
    }

    fn complete(mut self: Box<Self>) -> Result<(Arc<dyn FileReader>, StoragePath), StorageError> {
        if !self.buffers.is_empty() {
            self.flush()?;
        }
        self.file.sync_all()?;

        // Remove the .mut extension from the file.
        let finalized_path = self.drop.path.with_extension("");
        fs::rename(&self.drop.path, &finalized_path)?;

        Ok((
            Arc::new(PosixReader::new(
                Arc::new(self.file),
                self.file_id,
                self.drop.with_path(finalized_path),
                self.async_threads,
                self.ioop_delay,
            )),
            self.name,
        ))
    }
}

impl PosixWriter {
    fn new(
        file: File,
        name: StoragePath,
        path: PathBuf,
        usage: Arc<AtomicI64>,
        async_threads: bool,
        ioop_delay: Duration,
    ) -> Self {
        Self {
            file_id: FileId::new(),
            file,
            name,
            drop: DeleteOnDrop::new(path, false, 0, usage),
            buffers: Vec::new(),
            len: 0,
            async_threads,
            ioop_delay,
        }
    }

    fn flush(&mut self) -> Result<(), IoError> {
        let mut bufs = self
            .buffers
            .iter()
            .map(|buf| IoSlice::new(buf.as_slice()))
            .collect::<Vec<_>>();
        let mut cursor = bufs.as_mut_slice();
        while !cursor.is_empty() {
            let n = self.file.write_vectored(cursor)?;
            self.drop.size += n as u64;
            self.drop.usage.fetch_add(n as i64, Ordering::Relaxed);
            IoSlice::advance_slices(&mut cursor, n);
        }
        self.buffers.clear();
        Ok(())
    }

    fn write(&mut self, buffer: &Arc<FBuf>) -> Result<(), StorageError> {
        if self.len >= 1024 * 1024 {
            self.flush()?;
        }
        self.len += buffer.len() as u64;
        self.buffers.push(buffer.clone());
        Ok(())
    }
}

/// State of the backend needed to satisfy the storage APIs.
pub struct PosixBackend {
    /// Directory in which we keep the files.
    base: Arc<PathBuf>,

    /// Cache configuration.
    cache: StorageCacheConfig,

    /// Usage.
    usage: Arc<AtomicI64>,

    /// Whether to use background threads for file I/O.
    async_threads: bool,

    /// Per-I/O operation sleep delay, for simulating slow storage devices.
    ioop_delay: Duration,
}

impl PosixBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(
        base: P,
        cache: StorageCacheConfig,
        options: &FileBackendConfig,
    ) -> Self {
        init();
        Self {
            base: Arc::new(base.as_ref().to_path_buf()),
            cache,
            usage: Arc::new(AtomicI64::new(0)),
            async_threads: options.async_threads.unwrap_or(true),
            ioop_delay: Duration::from_millis(options.ioop_delay.unwrap_or_default()),
        }
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    /// Returns the filesystem path to `name` in this storage.
    fn fs_path(&self, name: &StoragePath) -> Result<PathBuf, StorageError> {
        Ok(self.base.join(name.as_ref()))
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), IoError> {
        let file_type = fs::symlink_metadata(path)?.file_type();
        if file_type.is_symlink() {
            fs::remove_file(path)
        } else {
            self.remove_dir_all_recursive(path)
        }
    }

    fn remove_dir_all_recursive(&self, path: &Path) -> Result<(), IoError> {
        fn ignore_notfound(result: Result<(), IoError>) -> Result<(), IoError> {
            match result {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                _ => result,
            }
        }

        for child in fs::read_dir(path)? {
            let child = child?;
            let path = child.path();
            let result = child.file_type().and_then(|file_type| {
                if file_type.is_dir() {
                    self.remove_dir_all_recursive(&path)
                } else if file_type.is_file() {
                    let size = child.metadata().map_or(0, |metadata| metadata.size());
                    fs::remove_file(&path).inspect(|_| {
                        self.usage.fetch_sub(size as i64, Ordering::Relaxed);
                    })
                } else {
                    fs::remove_file(&path)
                }
            });
            ignore_notfound(result)?;
        }
        ignore_notfound(fs::remove_dir(path))
    }
}

impl StorageBackend for PosixBackend {
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
        fn try_create_named(this: &PosixBackend, path: &Path) -> Result<File, IoError> {
            OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .cache_flags(&this.cache)
                .open(path)
        }

        let path = append_to_path(self.fs_path(name)?, MUTABLE_EXTENSION);
        let file = match try_create_named(self, &path) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    create_dir_all(parent)?;
                }
                try_create_named(self, &path)
            }
            other => other,
        }?;
        counter!(FILES_CREATED).increment(1);
        Ok(Box::new(PosixWriter::new(
            file,
            name.clone(),
            path,
            self.usage.clone(),
            self.async_threads,
            self.ioop_delay,
        )))
    }

    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
        PosixReader::open(
            self.fs_path(name)?,
            self.cache,
            self.usage.clone(),
            self.async_threads,
            self.ioop_delay,
        )
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(&StoragePath, StorageFileType),
    ) -> Result<(), StorageError> {
        fn parse_entry(entry: DirEntry) -> Result<(OsString, StorageFileType), IoError> {
            let file_type = entry.file_type()?;
            let file_type = if file_type.is_file() {
                StorageFileType::File {
                    size: entry.metadata()?.size(),
                }
            } else if file_type.is_dir() {
                StorageFileType::Directory
            } else {
                StorageFileType::Other
            };
            Ok((entry.file_name(), file_type))
        }

        let mut result = Ok(());
        for entry in self.fs_path(parent)?.read_dir()? {
            match entry.and_then(parse_entry) {
                Err(e) => {
                    result = Err(e.into());
                }
                Ok((name, file_type)) => cb(
                    &parent.child(StoragePathPart::from(name.as_encoded_bytes())),
                    file_type,
                ),
            }
        }
        result
    }

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = self.fs_path(name)?;
        let metadata = fs::metadata(&path)?;
        fs::remove_file(&path)?;
        if metadata.file_type().is_file() {
            self.usage
                .fetch_sub(metadata.size() as i64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = self.fs_path(name)?;
        match self.remove_dir_all(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => (),
            Err(error) if error.kind() == ErrorKind::NotADirectory => self.delete(name)?,
            Err(error) => return Err(error)?,
            Ok(()) => (),
        }
        Ok(())
    }

    fn usage(&self) -> Arc<AtomicI64> {
        self.usage.clone()
    }

    fn file_system_path(&self) -> Option<&Path> {
        Some(self.base.as_path())
    }
}

pub(crate) struct DefaultBackendFactory;
impl StorageBackendFactory for DefaultBackendFactory {
    fn backend(&self) -> &'static str {
        "default"
    }

    fn create(
        &self,
        storage_config: &StorageConfig,
        _backend_config: &StorageBackendConfig,
    ) -> Result<Arc<dyn StorageBackend>, StorageError> {
        Ok(Arc::new(PosixBackend::new(
            storage_config.path(),
            storage_config.cache,
            &FileBackendConfig::default(),
        )))
    }
}

inventory::submit! {
    &DefaultBackendFactory as &dyn StorageBackendFactory
}

pub(crate) struct FileBackendFactory;
impl StorageBackendFactory for FileBackendFactory {
    fn backend(&self) -> &'static str {
        "file"
    }

    fn create(
        &self,
        storage_config: &StorageConfig,
        backend_config: &StorageBackendConfig,
    ) -> Result<Arc<dyn StorageBackend>, StorageError> {
        let StorageBackendConfig::File(config) = &backend_config else {
            return Err(StorageError::InvalidBackendConfig {
                backend: self.backend().into(),
                config: backend_config.clone(),
            });
        };
        Ok(Arc::new(PosixBackend::new(
            storage_config.path(),
            storage_config.cache,
            config,
        )))
    }
}

inventory::submit! {
    &FileBackendFactory as &dyn StorageBackendFactory
}

#[cfg(test)]
mod tests {
    use feldera_storage::StorageBackend;
    use feldera_types::config::{FileBackendConfig, StorageCacheConfig};
    use std::{path::Path, sync::Arc};

    use crate::storage::backend::tests::{random_sizes, test_backend};

    use super::PosixBackend;

    fn create_posix_backend(path: &Path) -> Arc<dyn StorageBackend> {
        Arc::new(PosixBackend::new(
            path,
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ))
    }

    /// Write 10 MiB total in 1 KiB chunks.  `VectoredWrite` flushes its buffer when it
    /// reaches 1 MiB of sequential data, and we limit the amount of queued work
    /// to 4 MiB, so this has a chance to trigger both limits.
    #[test]
    fn sequential_1024() {
        test_backend(Box::new(create_posix_backend), &[1024; 1024 * 10], true)
    }

    /// Verify that files get deleted if not marked for a checkpoint.
    #[test]
    fn delete_1024() {
        test_backend(Box::new(create_posix_backend), &[1024; 1024 * 10], false)
    }

    #[test]
    fn sequential_random() {
        test_backend(Box::new(create_posix_backend), &random_sizes(), true);
    }

    #[test]
    fn empty() {
        test_backend(Box::new(create_posix_backend), &[], true);
    }
}
