//! [StorageBackend] implementation using POSIX I/O.

use super::{
    tempdir_for_thread, BlockLocation, FileId, FileReader, FileWriter, HasFileId,
    StorageCacheFlags, StorageError, IOV_MAX, MUTABLE_EXTENSION,
};
use crate::circuit::metrics::{
    FILES_CREATED, FILES_DELETED, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
};
use crate::storage::{buffer_cache::FBuf, init};
use feldera_storage::{
    append_to_path, StorageBackend, StorageBackendFactory, StorageFileType, StoragePath,
    StoragePathPart,
};
use feldera_types::config::{StorageBackendConfig, StorageCacheConfig, StorageConfig};
use metrics::{counter, histogram};
use std::fs::create_dir_all;
use std::io::ErrorKind;
use std::{
    fs::{self, File, OpenOptions},
    io::Error as IoError,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Instant,
};
use tracing::warn;

pub(super) struct PosixReader {
    file: Arc<File>,
    file_id: FileId,
    drop: DeleteOnDrop,
    /// File size.
    ///
    /// -1 if the file size is unknown.
    size: AtomicI64,
}

impl PosixReader {
    fn new(file: Arc<File>, file_id: FileId, drop: DeleteOnDrop) -> Self {
        Self {
            file,
            file_id,
            drop,
            size: AtomicI64::new(-1),
        }
    }
    fn open(path: PathBuf, cache: StorageCacheConfig) -> Result<Arc<dyn FileReader>, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&cache)
            .open(&path)?;

        Ok(Arc::new(Self::new(
            Arc::new(file),
            FileId::new(),
            DeleteOnDrop::new(path, true),
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
        let mut buffer = FBuf::with_capacity(location.size);

        match buffer.read_exact_at(&self.file, location.offset, location.size) {
            Ok(()) => Ok(Arc::new(buffer)),
            Err(e) => Err(e.into()),
        }
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        let sz = self.size.load(Ordering::Relaxed);
        if sz >= 0 {
            Ok(sz as u64)
        } else {
            let sz = self.file.metadata()?.size();
            self.size.store(sz.try_into().unwrap(), Ordering::Relaxed);
            Ok(sz)
        }
    }
}

struct DeleteOnDrop {
    path: PathBuf,
    keep: AtomicBool,
}

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        if !self.keep.load(Ordering::Relaxed) {
            if let Err(e) = fs::remove_file(&self.path) {
                warn!("Unable to delete file {:?}: {:?}", self.path, e);
            } else {
                counter!(FILES_DELETED).increment(1);
            }
        }
    }
}

impl DeleteOnDrop {
    fn new(path: PathBuf, keep: bool) -> Self {
        Self {
            path,
            keep: AtomicBool::new(keep),
        }
    }
    fn keep(&self) {
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
    offset: u64,
    len: u64,
}

impl HasFileId for PosixWriter {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl FileWriter for PosixWriter {
    fn write_block(&mut self, offset: u64, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let block = Arc::new(data);
        let request_start = Instant::now();
        self.write_at(&block, offset)?;

        counter!(TOTAL_BYTES_WRITTEN).increment(block.len() as u64);
        counter!(WRITES_SUCCESS).increment(1);
        histogram!(WRITE_LATENCY).record(request_start.elapsed().as_secs_f64());

        Ok(block)
    }

    fn complete(mut self: Box<Self>) -> Result<(Arc<dyn FileReader>, StoragePath), StorageError> {
        self.flush()?;
        self.file.sync_all()?;

        // Remove the .mut extension from the file.
        let finalized_path = self.drop.path.with_extension("");
        fs::rename(&self.drop.path, &finalized_path)?;

        Ok((
            Arc::new(PosixReader::new(
                Arc::new(self.file),
                self.file_id,
                self.drop.with_path(finalized_path),
            )),
            self.name,
        ))
    }
}

impl PosixWriter {
    fn new(file: File, name: StoragePath, path: PathBuf) -> Self {
        Self {
            file_id: FileId::new(),
            file,
            name,
            drop: DeleteOnDrop::new(path, false),
            buffers: Vec::new(),
            offset: 0,
            len: 0,
        }
    }

    #[cfg(target_family = "unix")]
    fn flush(&mut self) -> Result<(), IoError> {
        use nix::sys::uio::pwritev;
        use std::io::IoSlice;
        if !self.buffers.is_empty() {
            let bufs: Vec<_> = self
                .buffers
                .iter()
                .map(|buf| IoSlice::new(buf.as_slice()))
                .collect();
            pwritev(&self.file, &bufs, self.offset as i64)?;
            self.buffers.clear();
        }
        Ok(())
    }

    #[cfg(not(target_family = "unix"))]
    fn flush(&mut self) -> Result<(), IoError> {
        use std::os::unix::fs::FileExt;
        if !self.buffers.is_empty() {
            let mut offset = self.offset;
            for buf in self.buffers.drain(..) {
                self.file.write_all_at(&buf, offset)?;
                offset += buf.len() as u64;
            }
        }
        Ok(())
    }

    fn write_at(&mut self, buffer: &Arc<FBuf>, offset: u64) -> Result<(), StorageError> {
        if self.len >= 1024 * 1024
            || (!self.buffers.is_empty() && self.offset + self.len != offset)
            || self.buffers.len() >= *IOV_MAX
        {
            self.flush()?;
        }
        if self.buffers.is_empty() {
            self.offset = offset;
            self.len = 0;
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
}

impl PosixBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(base: P, cache: StorageCacheConfig) -> Self {
        init();
        Self {
            base: Arc::new(base.as_ref().to_path_buf()),
            cache,
        }
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    fn new_default() -> Rc<Self> {
        Rc::new(PosixBackend::new(
            tempdir_for_thread(),
            StorageCacheConfig::default(),
        ))
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<Self> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<PosixBackend> = PosixBackend::new_default();
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }

    /// Returns the filesystem path to `name` in this storage.
    fn fs_path(&self, name: &StoragePath) -> Result<PathBuf, StorageError> {
        Ok(self.base.join(name.as_ref()))
    }
}

impl StorageBackend for PosixBackend {
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
        fn try_create_named(this: &PosixBackend, path: &Path) -> Result<File, IoError> {
            OpenOptions::new()
                .create_new(true)
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
        Ok(Box::new(PosixWriter::new(file, name.clone(), path)))
    }

    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
        PosixReader::open(self.fs_path(name)?, self.cache)
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(&StoragePath, StorageFileType),
    ) -> Result<(), StorageError> {
        let mut result = Ok(());
        for entry in self.fs_path(parent)?.read_dir()? {
            match entry.and_then(|entry| {
                entry
                    .file_type()
                    .map(|file_type| (entry.file_name(), file_type))
            }) {
                Err(e) => {
                    result = Err(e.into());
                }
                Ok((name, file_type)) => {
                    let file_type = if file_type.is_file() {
                        StorageFileType::File
                    } else if file_type.is_dir() {
                        StorageFileType::Directory
                    } else {
                        StorageFileType::Other
                    };
                    cb(
                        &parent.child(StoragePathPart::from(name.as_encoded_bytes())),
                        file_type,
                    )
                }
            }
        }
        result
    }

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
        fs::remove_file(self.fs_path(name)?)?;
        Ok(())
    }

    fn delete_recursive(&self, name: &StoragePath) -> Result<(), StorageError> {
        let path = self.fs_path(name)?;
        match fs::remove_dir_all(&path) {
            Err(error) if error.kind() == ErrorKind::NotFound => (),
            Err(error) if error.kind() == ErrorKind::NotADirectory => fs::remove_file(&path)?,
            Err(error) => return Err(error)?,
            Ok(()) => (),
        }
        Ok(())
    }
}

pub(crate) struct PosixBackendFactory;
impl StorageBackendFactory for PosixBackendFactory {
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
        )))
    }
}

inventory::submit! {
    &PosixBackendFactory as &dyn StorageBackendFactory
}

#[cfg(test)]
mod tests {
    use feldera_storage::StorageBackend;
    use feldera_types::config::StorageCacheConfig;
    use std::{path::Path, sync::Arc};

    use crate::storage::backend::tests::{random_sizes, test_backend};

    use super::PosixBackend;

    fn create_posix_backend(path: &Path) -> Arc<dyn StorageBackend> {
        Arc::new(PosixBackend::new(path, StorageCacheConfig::default()))
    }

    /// Write 10 MiB total in 1 KiB chunks.  `VectoredWrite` flushes its buffer when it
    /// reaches 1 MiB of sequential data, and we limit the amount of queued work
    /// to 4 MiB, so this has a chance to trigger both limits.
    #[test]
    fn sequential_1024() {
        test_backend(
            Box::new(create_posix_backend),
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
            Box::new(create_posix_backend),
            &[1024; 1024 * 10],
            false,
            true,
        )
    }

    /// Verify that files get deleted if not marked for a checkpoint.
    #[test]
    fn delete_1024() {
        test_backend(
            Box::new(create_posix_backend),
            &[1024; 1024 * 10],
            true,
            false,
        )
    }

    #[test]
    fn sequential_random() {
        test_backend(Box::new(create_posix_backend), &random_sizes(), true, true);
    }

    #[test]
    fn holes_random() {
        test_backend(Box::new(create_posix_backend), &random_sizes(), false, true);
    }

    #[test]
    fn empty() {
        test_backend(Box::new(create_posix_backend), &[], true, true);
    }
}
