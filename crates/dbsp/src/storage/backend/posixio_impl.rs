//! [StorageBackend] implementation using POSIX I/O.

use feldera_types::config::{FileBackendConfig, StorageCacheConfig};
use metrics::{counter, histogram};
use std::{
    fs::{self, remove_file, File, OpenOptions},
    io::Error as IoError,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc,
    },
    time::Instant,
};
use tracing::warn;

use super::{
    append_to_path, default_read_async, BlockLocation, FileId, FileReader, FileWriter, HasFileId,
    StorageBackend, StorageCacheFlags, StorageError, IOV_MAX, MUTABLE_EXTENSION,
};
use crate::circuit::{
    metrics::{FILES_CREATED, FILES_DELETED, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY},
    tokio::TOKIO,
};
use crate::storage::{buffer_cache::FBuf, init};

struct PosixReader {
    file: Arc<File>,
    file_id: FileId,
    drop: DeleteOnDrop,
    /// File size.
    ///
    /// -1 if the file size is unknown.
    size: AtomicI64,
    /// Whether to use background threads for file I/O.
    async_threads: bool,
}

impl PosixReader {
    fn new(
        file: Arc<File>,
        file_id: FileId,
        path: PathBuf,
        keep: bool,
        async_threads: bool,
    ) -> Self {
        Self {
            file,
            file_id,
            drop: DeleteOnDrop::new(path, keep),
            size: AtomicI64::new(-1),
            async_threads,
        }
    }
    fn open(
        path: PathBuf,
        cache: StorageCacheConfig,
        async_threads: bool,
    ) -> Result<Arc<dyn FileReader>, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&cache)
            .open(&path)?;

        Ok(Arc::new(Self::new(
            Arc::new(file),
            FileId::new(),
            path,
            true,
            async_threads,
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

    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        if self.async_threads {
            let file = self.file.clone();
            TOKIO.spawn_blocking(move || {
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

/// Deletes a file when dropped (unless [Self::keep] is called first).
pub struct DeleteOnDrop {
    path: PathBuf,
    keep: AtomicBool,
}

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        if !self.keep.load(Ordering::Relaxed) {
            if let Err(e) = remove_file(&self.path) {
                warn!("Unable to delete file {:?}: {:?}", self.path, e);
            } else {
                counter!(FILES_DELETED).increment(1);
            }
        }
    }
}

impl DeleteOnDrop {
    /// Returns an object that will delete `path` when dropped (unless `keep` is
    /// true or [Self::keep] is called before this dropping).
    pub fn new(path: PathBuf, keep: bool) -> Self {
        Self {
            path,
            keep: AtomicBool::new(keep),
        }
    }

    /// Disables deleting the file when dropped.
    pub fn keep(&self) {
        self.keep.store(true, Ordering::Relaxed);
    }
}

/// Meta-data we keep per file we created.
struct PosixWriter {
    file_id: FileId,
    file: File,
    drop: DeleteOnDrop,

    buffers: Vec<Arc<FBuf>>,
    offset: u64,
    len: u64,

    async_threads: bool,
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

    fn complete(mut self: Box<Self>) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError> {
        self.flush()?;
        self.file.sync_all()?;

        // Remove the .mut extension from the file.
        let finalized_path = self.path().with_extension("");
        let mut ppath = self.path().clone();
        ppath.pop();
        fs::rename(self.path(), &finalized_path)?;
        self.drop.keep();

        Ok((
            Arc::new(PosixReader::new(
                Arc::new(self.file),
                self.file_id,
                finalized_path.clone(),
                false,
                self.async_threads,
            )),
            finalized_path,
        ))
    }
}

impl PosixWriter {
    fn new(file: File, path: PathBuf, async_threads: bool) -> Self {
        Self {
            file_id: FileId::new(),
            file,
            drop: DeleteOnDrop::new(path, false),
            buffers: Vec::new(),
            offset: 0,
            len: 0,
            async_threads,
        }
    }

    fn path(&self) -> &PathBuf {
        &self.drop.path
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
    base: PathBuf,

    /// Cache configuration.
    cache: StorageCacheConfig,

    /// Whether to use background threads for file I/O.
    async_threads: bool,
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
            base: base.as_ref().to_path_buf(),
            cache,
            async_threads: options.async_threads.unwrap_or(false),
        }
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }
}

impl StorageBackend for PosixBackend {
    fn create_named(&self, name: &Path) -> Result<Box<dyn FileWriter>, StorageError> {
        let path = append_to_path(self.base.join(name), MUTABLE_EXTENSION);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .cache_flags(&self.cache)
            .open(&path)?;
        counter!(FILES_CREATED).increment(1);
        Ok(Box::new(PosixWriter::new(file, path, self.async_threads)))
    }

    fn open(&self, name: &Path) -> Result<Arc<dyn FileReader>, StorageError> {
        PosixReader::open(self.base.join(name), self.cache, self.async_threads)
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, rc::Rc};

    use feldera_types::config::{FileBackendConfig, StorageCacheConfig};

    use crate::storage::backend::{
        tests::{random_sizes, test_backend},
        StorageBackend,
    };

    use super::PosixBackend;

    fn create_posix_backend(path: &Path) -> Rc<dyn StorageBackend> {
        Rc::new(PosixBackend::new(
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
