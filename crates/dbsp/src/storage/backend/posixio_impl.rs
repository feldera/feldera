//! Implementation of the storage backend ([`Storage`] APIs using POSIX I/O.

use metrics::{counter, histogram};
use pipeline_types::config::StorageCacheConfig;
use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Error as IoError,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use super::{
    append_to_path, tempdir_for_thread, AtomicIncrementOnlyI64, FileHandle, ImmutableFile,
    ImmutableFileHandle, ImmutableFiles, Storage, StorageCacheFlags, StorageError,
    IMMUTABLE_FILE_METADATA, MUTABLE_EXTENSION,
};
use crate::circuit::metrics::{
    FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS, READ_LATENCY, TOTAL_BYTES_READ,
    TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
};
use crate::storage::{backend::NEXT_FILE_HANDLE, buffer_cache::FBuf, init};

/// Meta-data we keep per file we created.
struct FileMetaData {
    file: File,
    path: PathBuf,

    buffers: Vec<Arc<FBuf>>,
    offset: u64,
    len: u64,
}

impl FileMetaData {
    #[cfg(target_os = "linux")]
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

    #[cfg(not(target_os = "linux"))]
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
        if self.len >= 1024 * 1024 || (!self.buffers.is_empty() && self.offset + self.len != offset)
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
    /// Meta-data of all files we are currently writing to.
    files: RefCell<HashMap<i64, FileMetaData>>,
    /// All files which are completed and can read from.
    immutable_files: Arc<ImmutableFiles>,
    /// A global counter to get unique identifiers for file-handles.
    next_file_id: Arc<AtomicIncrementOnlyI64>,
    /// Cache configuration.
    cache: StorageCacheConfig,
}

impl PosixBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    /// - `next_file_id`: A counter to get unique identifiers for file-handles.
    ///   Note that in case we use a global buffer cache, this counter should be
    ///   shared among all instances of the backend.
    /// - `immutable_files`: The storage for meta-data of immutable files
    ///   which can be read from all threads.
    pub fn new<P: AsRef<Path>>(
        base: P,
        cache: StorageCacheConfig,
        next_file_id: Arc<AtomicIncrementOnlyI64>,
        immutable_files: Arc<ImmutableFiles>,
    ) -> Self {
        init();
        Self {
            base: base.as_ref().to_path_buf(),
            files: RefCell::new(HashMap::new()),
            immutable_files,
            next_file_id,
            cache,
        }
    }

    /// See [`PosixBackend::new`]. This function is a convenience function that
    /// creates a new backend with global unique file-handle counter.
    pub fn with_base<P: AsRef<Path>>(base: P, cache: StorageCacheConfig) -> Self {
        Self::new(
            base,
            cache,
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
            IMMUTABLE_FILE_METADATA
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        )
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    fn new_default() -> Rc<Self> {
        Rc::new(PosixBackend::new(
            tempdir_for_thread(),
            StorageCacheConfig::default(),
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
            IMMUTABLE_FILE_METADATA
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        ))
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<Self> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<PosixBackend> = PosixBackend::new_default();
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }
}

impl Storage for PosixBackend {
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError> {
        let path = append_to_path(self.base.join(name), MUTABLE_EXTENSION);
        let file_counter = self.next_file_id.increment();
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .cache_flags(&self.cache)
            .open(&path)?;
        let mut files = self.files.borrow_mut();
        files.insert(
            file_counter,
            FileMetaData {
                file,
                path,
                buffers: Vec::new(),
                offset: 0,
                len: 0,
            },
        );
        counter!(FILES_CREATED).increment(1);

        Ok(FileHandle(file_counter))
    }

    fn open(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError> {
        let path = self.base.join(name);
        let attr = fs::metadata(&path)?;

        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&self.cache)
            .open(&path)?;

        let file_counter = self.next_file_id.increment();
        self.immutable_files.insert(
            file_counter,
            ImmutableFile::new(Arc::new(file), path.clone(), attr.size()),
        );

        Ok(ImmutableFileHandle(file_counter))
    }

    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        let ifh = self.immutable_files.get(fd.0).unwrap();
        fs::remove_file(&ifh.path)?;
        counter!(FILES_DELETED).increment(1);
        Ok(())
    }

    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        let FileMetaData { path, .. } = self.files.borrow_mut().remove(&fd.0).unwrap();
        fs::remove_file(path)?;
        counter!(FILES_DELETED).increment(1);
        Ok(())
    }

    fn base(&self) -> PathBuf {
        self.base.clone()
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Arc<FBuf>, StorageError> {
        let block = Arc::new(data);

        let mut files = self.files.borrow_mut();
        let request_start = Instant::now();
        let fm = files.get_mut(&fd.0).unwrap();
        fm.write_at(&block, offset)?;

        counter!(TOTAL_BYTES_WRITTEN).increment(block.len() as u64);
        counter!(WRITES_SUCCESS).increment(1);
        histogram!(WRITE_LATENCY).record(request_start.elapsed().as_secs_f64());

        Ok(block)
    }

    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        let mut files = self.files.borrow_mut();
        let mut fm = files.remove(&fd.0).unwrap();
        fm.flush()?;
        fm.file.sync_all()?;

        // Remove the .mut extension from the file.
        let finalized_path = fm.path.with_extension("");
        let mut ppath = fm.path.clone();
        ppath.pop();
        fs::rename(&fm.path, &finalized_path)?;
        fm.path = finalized_path;
        let path = fm.path.clone();
        let attr = fs::metadata(&path)?;

        let imf = ImmutableFile::new(Arc::new(fm.file), path.clone(), attr.size());
        self.immutable_files.insert(fd.0, imf);

        Ok((ImmutableFileHandle(fd.0), path))
    }

    fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {
        unimplemented!()
    }

    fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Arc<FBuf>, StorageError> {
        let mut buffer = FBuf::with_capacity(size);

        let imf = self.immutable_files.get(fd.0).unwrap();
        let request_start = Instant::now();
        match buffer.read_exact_at(&imf.file, offset, size) {
            Ok(()) => {
                counter!(TOTAL_BYTES_READ).increment(buffer.len() as u64);
                histogram!(READ_LATENCY).record(request_start.elapsed().as_secs_f64());
                counter!(READS_SUCCESS).increment(1);
                Ok(Arc::new(buffer))
            }
            Err(e) => {
                counter!(READS_FAILED).increment(1);
                Err(e.into())
            }
        }
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let imf = self.immutable_files.get(fd.0).unwrap();
        Ok(imf.size)
    }
}
