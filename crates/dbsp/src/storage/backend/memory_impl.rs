//! Implementation of the storage backend ([`Storage`] APIs using memory.
//!
//! This is useful for performance testing, not as part of a production system.

use metrics::counter;
use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, RwLock},
};

use crate::storage::{backend::NEXT_FILE_HANDLE, buffer_cache::FBuf};

use super::{
    metrics::{
        describe_disk_metrics, FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS,
        TOTAL_BYTES_READ, TOTAL_BYTES_WRITTEN, WRITES_SUCCESS,
    },
    AtomicIncrementOnlyI64, FileHandle, ImmutableFileHandle, Storage, StorageError,
};

/// Meta-data we keep per file we created.
#[derive(Default)]
struct FileMetaData {
    name: PathBuf,
    blocks: HashMap<u64, Rc<FBuf>>,
    size: u64,
}

/// State of the backend needed to satisfy the storage APIs.
pub struct MemoryBackend {
    /// Meta-data of all files we created so far.
    files: RwLock<HashMap<i64, FileMetaData>>,
    /// A global counter to get unique identifiers for file-handles.
    next_file_id: Arc<AtomicIncrementOnlyI64>,
}

impl MemoryBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `next_file_id`: A counter to get unique identifiers for file-handles.
    ///   Note that in case we use a global buffer cache, this counter should be
    ///   shared among all instances of the backend.
    pub fn new(next_file_id: Arc<AtomicIncrementOnlyI64>) -> Self {
        describe_disk_metrics();
        Self {
            files: RwLock::new(HashMap::new()),
            next_file_id,
        }
    }

    /// See [`MemoryBackend::new`]. This function is a convenience function that
    /// creates a new backend with global unique file-handle counter.
    pub fn with_base<P: AsRef<Path>>(_base: P) -> Self {
        Self::new(
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        )
    }

    /// Helper function to delete (mutable and immutable) files.
    fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        self.files.write().unwrap().remove(&fd).unwrap();
        counter!(FILES_DELETED).increment(1);
        Ok(())
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<Self> {
        thread_local! {
            pub static DEFAULT_BACKEND: Rc<MemoryBackend> = {
                Rc::new(MemoryBackend::new(NEXT_FILE_HANDLE.get_or_init(|| {
                    Arc::new(Default::default())
                }).clone()))
            };
        }
        DEFAULT_BACKEND.with(|rc| rc.clone())
    }
}

impl Storage for MemoryBackend {
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError> {
        let file_counter = self.next_file_id.increment();
        let mut files = self.files.write().unwrap();
        files.insert(
            file_counter,
            FileMetaData {
                name: name.to_path_buf(),
                blocks: HashMap::new(),
                size: 0,
            },
        );
        counter!(FILES_CREATED).increment(1);

        Ok(FileHandle(file_counter))
    }

    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
    }

    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0)
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        let data = Rc::new(data);
        let mut files = self.files.write().unwrap();
        let fm = files.get_mut(&fd.0).unwrap();
        fm.blocks.insert(offset, data.clone());

        let min_size = offset + data.len() as u64;
        if min_size > fm.size {
            fm.size = min_size;
        }

        counter!(TOTAL_BYTES_WRITTEN).increment(data.len() as u64);
        counter!(WRITES_SUCCESS).increment(1);

        Ok(data)
    }

    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        let files = self.files.read().unwrap();
        let fm = files.get(&fd.0).unwrap();
        let path = fm.name.clone();

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
        let files = self.files.read().unwrap();
        let fm = files.get(&fd.0).unwrap();
        let block = fm.blocks.get(&offset);
        if let Some(block) = block {
            if size == block.len() {
                counter!(TOTAL_BYTES_READ).increment(block.len() as u64);
                counter!(READS_SUCCESS).increment(1);
                return Ok(block.clone());
            }
        }
        counter!(READS_FAILED).increment(1);
        Err(IoError::from(ErrorKind::UnexpectedEof).into())
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let files = self.files.read().unwrap();
        let fm = files.get(&fd.0).unwrap();
        Ok(fm.size)
    }
}
