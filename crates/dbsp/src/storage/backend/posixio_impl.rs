//! Implementation of the storage backend ([`Storage`] APIs using POSIX I/O.

use metrics::{counter, histogram};
use std::{
    cell::RefCell,
    collections::HashMap,
    fs,
    fs::{File, OpenOptions},
    io::{Error as IoError, Seek},
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use crate::storage::{backend::NEXT_FILE_HANDLE, buffer_cache::FBuf, init};

use super::{
    append_to_path,
    metrics::{
        describe_disk_metrics, FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS,
        READ_LATENCY, TOTAL_BYTES_READ, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
    },
    tempdir_for_thread, AtomicIncrementOnlyI64, FileHandle, ImmutableFileHandle, Storage,
    StorageError, MUTABLE_EXTENSION,
};

/// Helper function that opens files as direct IO files on linux.
fn open_as_direct<P: AsRef<Path>>(p: P, options: &mut OpenOptions) -> Result<File, IoError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_DIRECT);
    }
    options.open(p)
}

/// Meta-data we keep per file we created.
struct FileMetaData {
    file: File,
    path: PathBuf,

    buffers: Vec<Rc<FBuf>>,
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

    fn write_at(&mut self, buffer: &Rc<FBuf>, offset: u64) -> Result<(), StorageError> {
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
    /// Meta-data of all files we created so far.
    files: RefCell<HashMap<i64, FileMetaData>>,
    /// A global counter to get unique identifiers for file-handles.
    next_file_id: Arc<AtomicIncrementOnlyI64>,
}

impl PosixBackend {
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
            files: RefCell::new(HashMap::new()),
            next_file_id,
        }
    }

    /// See [`PosixBackend::new`]. This function is a convenience function that
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
    fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        let FileMetaData { path, .. } = self.files.borrow_mut().remove(&fd).unwrap();
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    fn new_default() -> Rc<Self> {
        Rc::new(PosixBackend::new(
            tempdir_for_thread(),
            NEXT_FILE_HANDLE
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
        let file = open_as_direct(
            &path,
            OpenOptions::new().create_new(true).write(true).read(true),
        )?;
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
        let attr = std::fs::metadata(&path)?;

        let file = open_as_direct(&path, OpenOptions::new().read(true))?;
        let mut files = self.files.borrow_mut();

        let file_counter = self.next_file_id.increment();
        files.insert(
            file_counter,
            FileMetaData {
                file,
                path,
                buffers: Vec::new(),
                offset: 0,
                len: attr.size(),
            },
        );

        Ok(ImmutableFileHandle(file_counter))
    }

    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
            .map(|_| counter!(FILES_DELETED).increment(1))
    }

    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
            .map(|_| counter!(FILES_DELETED).increment(1))
    }

    fn base(&self) -> &Path {
        &self.base
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        let block = Rc::new(data);

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
        files.insert(fd.0, fm);

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
    ) -> Result<Rc<FBuf>, StorageError> {
        let mut buffer = FBuf::with_capacity(size);

        let files = self.files.borrow();
        let fm = files.get(&fd.0).unwrap();
        let request_start = Instant::now();
        match buffer.read_exact_at(&fm.file, offset, size) {
            Ok(()) => {
                counter!(TOTAL_BYTES_READ).increment(buffer.len() as u64);
                histogram!(READ_LATENCY).record(request_start.elapsed().as_secs_f64());
                counter!(READS_SUCCESS).increment(1);
                Ok(Rc::new(buffer))
            }
            Err(e) => {
                counter!(READS_FAILED).increment(1);
                Err(e.into())
            }
        }
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let mut files = self.files.borrow_mut();
        let fm = files.get_mut(&fd.0).unwrap();
        let size = fm.file.seek(std::io::SeekFrom::End(0))?;
        Ok(size)
    }
}
