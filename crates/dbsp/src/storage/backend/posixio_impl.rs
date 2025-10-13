//! [StorageBackend] implementation using POSIX I/O.

use super::{
    BlockLocation, FileId, FileReader, FileWriter, HasFileId, StorageCacheFlags, StorageError,
    MUTABLE_EXTENSION,
};
use crate::circuit::metrics::{FILES_CREATED, FILES_DELETED};
use crate::storage::{buffer_cache::FBuf, init};
use crate::Runtime;
use feldera_storage::metrics::{
    READ_BLOCKS_BYTES, READ_LATENCY_MICROSECONDS, SYNC_LATENCY_MICROSECONDS, WRITE_BLOCKS_BYTES,
    WRITE_LATENCY_MICROSECONDS,
};
use feldera_storage::tokio::TOKIO;
use feldera_storage::{
    append_to_path, default_read_async, StorageBackend, StorageBackendFactory, StorageFileType,
    StoragePath, StoragePathPart,
};
use feldera_types::config::{
    FileBackendConfig, StorageBackendConfig, StorageCacheConfig, StorageConfig,
};
use std::ffi::OsString;
use std::fs::{create_dir_all, DirEntry};
use std::io::{ErrorKind, IoSlice, Write};
use std::thread::sleep;
use std::time::{Duration, Instant};
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
            .open(&path)
            .map_err(|e| StorageError::stdio(e.kind(), "open", path.display()))?;
        let size = file
            .metadata()
            .map_err(|e| StorageError::stdio(e.kind(), "fstat", path.display()))?
            .size();

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
        READ_BLOCKS_BYTES.record(location.size);
        READ_LATENCY_MICROSECONDS
            .record_callback(|| {
                sleep(self.ioop_delay);
                let mut buffer = FBuf::with_capacity(location.size);

                match buffer.read_exact_at(&self.file, location.offset, location.size) {
                    Ok(()) => Ok(Arc::new(buffer)),
                    Err(e) => Err(StorageError::stdio(
                        e.kind(),
                        "read",
                        self.drop.path.display(),
                    )),
                }
            })
            .inspect_err(|e| warn!("{}: read failed: {e}", self.drop.path.display()))
    }

    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        if self.async_threads {
            let file = self.file.clone();
            let ioop_delay = self.ioop_delay;
            let start = Instant::now();
            TOKIO.spawn_blocking(move || {
                // For background reads, we only sleep once, not once per block.
                sleep(ioop_delay);
                let blocks = blocks
                    .into_iter()
                    .map(|location| {
                        READ_BLOCKS_BYTES.record(location.size);
                        let mut buffer = FBuf::with_capacity(location.size);
                        match buffer.read_exact_at(&file, location.offset, location.size) {
                            Ok(()) => Ok(Arc::new(buffer)),
                            Err(e) => Err(StorageError::StdIo {
                                kind: e.kind(),
                                operation: "async read",
                                path: None,
                            }),
                        }
                    })
                    .collect();
                READ_LATENCY_MICROSECONDS.record_elapsed(start);
                callback(blocks);
            });
        } else {
            // This will call back into [Self::read_block], so don't measure the
            // latency directly (that would double-count).
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
                warn!(
                    "{}: unable to delete dropped file: {e}",
                    self.path.display(),
                );
            } else {
                self.usage.fetch_sub(self.size as i64, Ordering::Relaxed);
                FILES_DELETED.fetch_add(1, Ordering::Relaxed);
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
        self.write(&block)
            .inspect_err(|e| warn!("{}: write failed: {e}", self.drop.path.display()))?;
        Ok(block)
    }

    fn complete(mut self: Box<Self>) -> Result<(Arc<dyn FileReader>, StoragePath), StorageError> {
        if !self.buffers.is_empty() {
            self.flush()?;
        }

        SYNC_LATENCY_MICROSECONDS.record_callback(|| {
            self.file
                .sync_all()
                .inspect_err(|e| warn!("{}: fsync failed: {e}", self.drop.path.display()))
                .map_err(|e| StorageError::stdio(e.kind(), "fsync", self.drop.path.display()))?;

            // Remove the .mut extension from the file.
            let finalized_path = self.drop.path.with_extension("");
            fs::rename(&self.drop.path, &finalized_path)
                .inspect_err(|e| {
                    warn!(
                        "{}: failed to rename to {}: {e}",
                        self.drop.path.display(),
                        finalized_path.display()
                    )
                })
                .map_err(|e| StorageError::stdio(e.kind(), "rename", self.drop.path.display()))?;

            Ok((
                Arc::new(PosixReader::new(
                    Arc::new(self.file),
                    self.file_id,
                    self.drop.with_path(finalized_path),
                    self.async_threads,
                    self.ioop_delay,
                )) as Arc<dyn FileReader>,
                self.name,
            ))
        })
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

    fn flush(&mut self) -> Result<(), StorageError> {
        WRITE_LATENCY_MICROSECONDS.record_callback(|| {
            if let Some(storage_mb_max) = Runtime::with_dev_tweaks(|tweaks| tweaks.storage_mb_max) {
                let usage_mb = (self.drop.usage.load(Ordering::Relaxed) / 1024 / 1024)
                    .max(0)
                    .cast_unsigned();
                if usage_mb > storage_mb_max {
                    warn!("{}: failing write because {usage_mb} MIB > {} MiB storage limit from DevTweak",
                          self.drop.path.display(), storage_mb_max);
                    return Err(StorageError::stdio(ErrorKind::StorageFull, "write", self.drop.path.display()));
                }
            }

            let mut bufs = self
                .buffers
                .iter()
                .map(|buf| IoSlice::new(buf.as_slice()))
                .collect::<Vec<_>>();
            let mut cursor = bufs.as_mut_slice();
            while !cursor.is_empty() {
                let n = self
                    .file
                    .write_vectored(cursor)
                    .inspect_err(|e| warn!("{}: write failed: {e}", self.drop.path.display()))
                    .map_err(|e| StorageError::stdio(e.kind(),"write", self.drop.path.display()))?;
                WRITE_BLOCKS_BYTES.record(n);
                self.drop.size += n as u64;
                self.drop.usage.fetch_add(n as i64, Ordering::Relaxed);
                IoSlice::advance_slices(&mut cursor, n);
            }
            self.buffers.clear();
            Ok(())
        })
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
    fn fs_path(&self, name: &StoragePath) -> PathBuf {
        self.base.join(name.as_ref())
    }

    fn remove_dir_all(&self, path: &Path) -> Result<(), IoError> {
        let file_type = fs::symlink_metadata(path)
            .inspect_err(|e| warn!("{}: lstat failed: {e}", path.display()))?
            .file_type();
        if file_type.is_symlink() {
            fs::remove_file(path).inspect_err(|e| warn!("{}: unlink failed: {e}", path.display()))
        } else {
            self.remove_dir_all_recursive(path)
        }
    }

    fn remove_dir_all_recursive(&self, path: &Path) -> Result<(), IoError> {
        fn ignore_notfound(result: Result<(), IoError>, path: &Path) -> Result<(), IoError> {
            match result {
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    warn!(
                        "{}: ignoring not found error during removal",
                        path.display()
                    );
                    Ok(())
                }
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
            ignore_notfound(result, &path).inspect_err(|e| {
                warn!(
                    "{}: removing {:?} failed: {e}",
                    path.display(),
                    child.file_type()
                )
            })?;
        }
        ignore_notfound(fs::remove_dir(path), path)
            .inspect_err(|e| warn!("{}: rmdir failed: {e}", path.display()))
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

        let path = append_to_path(self.fs_path(name), MUTABLE_EXTENSION);
        let file = match try_create_named(self, &path) {
            Err(error) if error.kind() == ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    create_dir_all(parent)
                        .inspect_err(|e| {
                            warn!("{}: create parent directories failed:  {e}", path.display())
                        })
                        .map_err(|e| {
                            StorageError::stdio(e.kind(), "recursive mkdir", path.display())
                        })?;
                }
                try_create_named(self, &path)
            }
            other => other,
        }
        .inspect_err(|e| warn!("{}: create failed: {e}", path.display()))
        .map_err(|e| StorageError::stdio(e.kind(), "create", path.display()))?;
        FILES_CREATED.fetch_add(1, Ordering::Relaxed);
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
            self.fs_path(name),
            self.cache,
            self.usage.clone(),
            self.async_threads,
            self.ioop_delay,
        )
        .inspect_err(|e| warn!("{name}: failed to open ({e})"))
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(&StoragePath, StorageFileType),
    ) -> Result<(), StorageError> {
        fn parse_entry(entry: &DirEntry) -> Result<(OsString, StorageFileType), IoError> {
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
        let path = self.fs_path(parent);
        for entry in path
            .read_dir()
            .inspect_err(|e| warn!("{}: readdir failed: {e}", self.fs_path(parent).display()))
            .map_err(|e| StorageError::stdio(e.kind(), "readdir", self.fs_path(parent).display()))?
        {
            match entry
                .inspect_err(|e| warn!("{}: readdir entry failed: {e}", path.display()))
                .and_then(|entry| {
                    parse_entry(&entry).inspect_err(|e| {
                        warn!(
                            "{}: parsing directory entry failed: {e}",
                            entry.path().display()
                        )
                    })
                }) {
                Err(e) => {
                    result = Err(StorageError::stdio(
                        e.kind(),
                        "readdir entry",
                        path.display(),
                    ));
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
        let path = self.fs_path(name);
        let metadata = fs::metadata(&path)
            .inspect_err(|e| warn!("{}: stat failed: {e}", path.display()))
            .map_err(|e| StorageError::stdio(e.kind(), "stat", path.display()))?;
        fs::remove_file(&path)
            .inspect_err(|e| warn!("{}: unlink failed: {e}", path.display()))
            .map_err(|e| StorageError::stdio(e.kind(), "unlink", path.display()))?;
        if metadata.file_type().is_file() {
            self.usage
                .fetch_sub(metadata.size() as i64, Ordering::Relaxed);
        }
        Ok(())
    }

    fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = self.fs_path(name);
        match self.remove_dir_all(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => (),
            Err(error) if error.kind() == ErrorKind::NotADirectory => self
                .delete(name)
                .inspect_err(|e| warn!("{}: delete nondirectory failed: {e}", path.display()))?,
            Err(error) => {
                return Err(StorageError::stdio(
                    error.kind(),
                    "recursive unlink",
                    path.display(),
                ))?
            }
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
                config: Box::new(backend_config.clone()),
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
