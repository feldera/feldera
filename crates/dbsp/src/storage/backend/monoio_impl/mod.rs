//! Implementation of the storage backend ([`Storage`] API, using the Monoio
//! library.

use std::{
    cell::RefCell,
    cmp::max,
    collections::HashMap,
    fs,
    future::Future,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
    time::Instant,
};

use async_lock::RwLock;
use metrics::{counter, histogram};
#[cfg(target_os = "linux")]
use monoio::IoUringDriver;
use monoio::{
    fs::{File, OpenOptions},
    FusionDriver, FusionRuntime, LegacyDriver, RuntimeBuilder,
};
use pipeline_types::config::StorageCacheConfig;

use super::{
    append_to_path,
    metrics::{
        describe_disk_metrics, FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS,
        READ_LATENCY, TOTAL_BYTES_READ, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
    },
    tempdir_for_thread, AtomicIncrementOnlyI64, FileHandle, ImmutableFileHandle, Storage,
    StorageCacheFlags, StorageError, MUTABLE_EXTENSION, NEXT_FILE_HANDLE,
};
use crate::storage::{buffer_cache::FBuf, init};

#[cfg(test)]
pub(crate) mod tests;

/// Number of entries an IO-ring will have.
pub const MAX_RING_ENTRIES: u32 = 4096;

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

/// Meta-data we keep per file we created.
struct FileMetaData {
    file: File,
    path: PathBuf,
    size: RefCell<u64>,
}

/// State of the backend needed to satisfy the storage APIs.
pub struct MonoioBackend {
    /// Directory in which we keep the files.
    base: PathBuf,
    /// Meta-data of all files we created so far.
    files: RwLock<HashMap<i64, FileMetaData>>,
    /// A global counter to get unique identifiers for file-handles.
    next_file_id: Arc<AtomicIncrementOnlyI64>,
    /// How to cache file access in this backend.
    cache: StorageCacheConfig,
}

impl MonoioBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    /// - `cache`: How to cache files read and written by this backend.
    /// - `next_file_id`: A counter to get unique identifiers for file-handles.
    ///   Note that in case we use a global buffer cache, this counter should be
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(
        base: P,
        cache: StorageCacheConfig,
        next_file_id: Arc<AtomicIncrementOnlyI64>,
    ) -> Self {
        init();
        describe_disk_metrics();
        Self {
            base: base.as_ref().to_path_buf(),
            files: RwLock::new(HashMap::new()),
            next_file_id,
            cache,
        }
    }

    /// See [`MonoioBackend::new`]. This function is a convenience function that
    /// creates a new backend with global unique file-handle counter.
    pub fn with_base<P: AsRef<Path>>(base: P, cache: StorageCacheConfig) -> Self {
        Self::new(
            base,
            cache,
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        )
    }

    /// Helper function to delete (mutable and immutable) files.
    async fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        let fm = self.files.write().await.remove(&fd).unwrap();
        fm.file.close().await?;
        std::fs::remove_file(fm.path)?;
        counter!(FILES_DELETED).increment(1);
        Ok(())
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    fn new_default() -> Rc<Self> {
        Rc::new(MonoioBackend::new(
            tempdir_for_thread(),
            StorageCacheConfig::default(),
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        ))
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<Self> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<MonoioBackend> = MonoioBackend::new_default();
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }

    async fn create_named_inner(&self, name: &Path) -> Result<FileHandle, StorageError> {
        let path = append_to_path(self.base.join(name), MUTABLE_EXTENSION);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .cache_flags(&self.cache)
            .open(&path)
            .await?;
        let mut files = self.files.write().await;

        let file_counter = self.next_file_id.increment();
        files.insert(
            file_counter,
            FileMetaData {
                file,
                path,
                size: RefCell::new(0),
            },
        );
        counter!(FILES_CREATED).increment(1);

        Ok(FileHandle(file_counter))
    }

    /// Opens a file for reading.
    async fn open_inner(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError> {
        let path = self.base.join(name);
        let attr = fs::metadata(&path)?;

        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&self.cache)
            .open(&path)
            .await?;
        let mut files = self.files.write().await;

        let file_counter = self.next_file_id.increment();
        files.insert(
            file_counter,
            FileMetaData {
                file,
                path,
                size: RefCell::new(attr.size()),
            },
        );

        Ok(ImmutableFileHandle(file_counter))
    }

    async fn write_block_inner(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        let files = self.files.read().await;
        let request_start = Instant::now();
        let fm = files.get(&fd.0).unwrap();
        let end_offset = offset + data.len() as u64;
        let (res, buf) = fm.file.write_all_at(data, offset).await;
        res?;

        fm.size.replace_with(|size| max(*size, end_offset));
        counter!(TOTAL_BYTES_WRITTEN).increment(buf.len() as u64);
        counter!(WRITES_SUCCESS).increment(1);
        histogram!(WRITE_LATENCY).record(request_start.elapsed().as_secs_f64());

        Ok(Rc::new(buf))
    }

    async fn complete_inner(
        &self,
        fd: FileHandle,
    ) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        let mut files = self.files.write().await;

        let mut fm = files.remove(&fd.0).unwrap();
        assert_eq!(
            fm.path.extension().and_then(|s| s.to_str()),
            Some(&MUTABLE_EXTENSION[1..]),
            "Mutable file does not have the right extension"
        );
        fm.file.sync_all().await?;

        // Remove the MUTABLE_EXTENSION from the file name.
        // This should ideally also happen through monoio/uring but it
        // doesn't seem to support this at the moment, I'll have to submit a PR.
        // See also POSIX impl about assumption of fd validity after rename.
        let new_name = fm.path.with_extension("");
        fs::rename(&fm.path, &new_name)?;
        fm.path = new_name;

        let path = fm.path.clone();
        files.insert(fd.0, fm);

        Ok((ImmutableFileHandle(fd.0), path))
    }

    async fn read_block_inner(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError> {
        let buffer = FBuf::with_capacity(size);

        let files = self.files.read().await;
        let fm = files.get(&fd.0).unwrap();
        let request_start = Instant::now();
        let (res, buf) = fm.file.read_exact_at(buffer, offset).await;
        match res {
            Ok(()) => {
                counter!(TOTAL_BYTES_READ).increment(size as u64);
                histogram!(READ_LATENCY).record(request_start.elapsed().as_secs_f64());
                counter!(READS_SUCCESS).increment(1);
                Ok(Rc::new(buf))
            }
            Err(e) => {
                counter!(READS_FAILED).increment(1);
                Err(e.into())
            }
        }
    }

    async fn get_size_inner(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let files = self.files.read().await;
        let fm = files.get(&fd.0).unwrap();
        let size = *fm.size.borrow();
        Ok(size)
    }

    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        #[cfg(target_os = "linux")]
        thread_local! {
            pub static RUNTIME: RefCell<FusionRuntime<IoUringDriver, LegacyDriver>> = {
                RefCell::new(RuntimeBuilder::<FusionDriver>::new()
                             .with_entries(MAX_RING_ENTRIES)
                             .build()
                             .unwrap())
            }
        };
        #[cfg(not(target_os = "linux"))]
        thread_local! {
            pub static RUNTIME: RefCell<FusionRuntime<LegacyDriver>> = {
                RefCell::new(RuntimeBuilder::<FusionDriver>::new()
                             .with_entries(MAX_RING_ENTRIES)
                             .build()
                             .unwrap())
            }
        }
        RUNTIME.with(|runtime| runtime.borrow_mut().block_on(future))
    }
}

impl Storage for MonoioBackend {
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError> {
        self.block_on(self.create_named_inner(name))
    }

    fn open(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError> {
        self.block_on(self.open_inner(name))
    }

    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.block_on(self.delete_inner(fd.0))
    }

    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.block_on(self.delete_inner(fd.0))
    }

    fn base(&self) -> &Path {
        self.base.as_path()
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        self.block_on(self.write_block_inner(fd, offset, data))
    }

    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        self.block_on(self.complete_inner(fd))
    }

    fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {
        unimplemented!()
    }

    fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError> {
        self.block_on(self.read_block_inner(fd, offset, size))
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        self.block_on(self.get_size_inner(fd))
    }
}
