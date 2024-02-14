//! Implementation of the storage backend APIs ([`StorageControl`],
//! [`StorageRead`], and [`StorageWrite`]) using the Monoio library.

use std::{
    cell::RefCell,
    cmp::max,
    collections::HashMap,
    future::Future,
    io,
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
use tempfile::TempDir;

use crate::storage::{buffer_cache::FBuf, init};

use super::{
    metrics::{
        describe_disk_metrics, FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS,
        READ_LATENCY, TOTAL_BYTES_READ, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
    },
    AtomicIncrementOnlyI64, FileHandle, ImmutableFileHandle, StorageControl, StorageError,
    StorageExecutor, StorageRead, StorageWrite, NEXT_FILE_HANDLE,
};

#[cfg(test)]
pub(crate) mod tests;

/// Number of entries an IO-ring will have.
pub const MAX_RING_ENTRIES: u32 = 4096;

/// Helper function that opens files as direct IO files on linux.
async fn open_as_direct<P: AsRef<Path>>(
    p: P,
    options: &mut OpenOptions,
) -> Result<File, io::Error> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }
    options.open(p).await
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
}

impl MonoioBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    /// - `next_file_id`: A counter to get unique identifiers for file-handles.
    ///   Note that in case we use a global buffer cache, this counter should be
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(base: P, next_file_id: Arc<AtomicIncrementOnlyI64>) -> Self {
        init();
        describe_disk_metrics();
        Self {
            base: base.as_ref().to_path_buf(),
            files: RwLock::new(HashMap::new()),
            next_file_id,
        }
    }

    /// See [`MonoioBackend::new`]. This function is a convenience function that
    /// creates a new backend with global unique file-handle counter.
    pub fn with_base<P: AsRef<Path>>(base: P) -> Self {
        Self::new(
            base,
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        )
    }

    /// Helper function to delete (mutable and immutable) files.
    async fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        let fm = self.files.write().await.remove(&fd).unwrap();
        fm.file.close().await?;
        std::fs::remove_file(fm.path).unwrap();
        Ok(())
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<Self> {
        thread_local! {
            pub static TEMPDIR: TempDir = tempfile::tempdir().unwrap();
            pub static DEFAULT_BACKEND: Rc<MonoioBackend> = {
                 Rc::new(MonoioBackend::new(TEMPDIR.with(|dir| dir.path().to_path_buf()), NEXT_FILE_HANDLE.get_or_init(|| {
                    Arc::new(Default::default())
                }).clone()))
            };
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }
}

impl StorageControl for MonoioBackend {
    async fn create_named<P: AsRef<Path>>(&self, name: P) -> Result<FileHandle, StorageError> {
        let path = self.base.join(name);
        let file = open_as_direct(
            &path,
            OpenOptions::new().create_new(true).write(true).read(true),
        )
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

    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
            .await
            .map(|_| counter!(FILES_DELETED).increment(1))
    }

    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
            .await
            .map(|_| counter!(FILES_DELETED).increment(1))
    }
}

impl StorageWrite for MonoioBackend {
    async fn write_block(
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

    async fn complete(
        &self,
        fd: FileHandle,
    ) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        let mut files = self.files.write().await;

        let fm = files.remove(&fd.0).unwrap();
        fm.file.sync_all().await?;
        let path = fm.path.clone();
        files.insert(fd.0, fm);

        Ok((ImmutableFileHandle(fd.0), path))
    }
}

impl StorageRead for MonoioBackend {
    async fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {
        unimplemented!()
    }

    async fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError> {
        let buffer = FBuf::with_capacity(size);

        let files = self.files.read().await;
        let fm = files.get(&fd.0).unwrap();
        let request_start = Instant::now();
        let (res, buf) = fm.file.read_at(buffer, offset).await;
        match res {
            Ok(len) => {
                counter!(TOTAL_BYTES_READ).increment(len as u64);
                histogram!(READ_LATENCY).record(request_start.elapsed().as_secs_f64());
                if size != buf.len() {
                    counter!(READS_FAILED).increment(1);
                    Err(StorageError::ShortRead)
                } else {
                    counter!(READS_SUCCESS).increment(1);
                    Ok(Rc::new(buf))
                }
            }
            Err(e) => {
                counter!(READS_FAILED).increment(1);
                Err(e.into())
            }
        }
    }

    async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let files = self.files.read().await;
        let fm = files.get(&fd.0).unwrap();
        let size = *fm.size.borrow();
        Ok(size)
    }
}

impl StorageExecutor for MonoioBackend {
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
