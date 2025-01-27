//! [StorageBackend] implementation in memory.
//!
//! This is useful for performance testing, not as part of a production system.

use metrics::counter;
use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, RwLock},
};

use super::{
    BlockLocation, FileId, FileReader, FileWriter, HasFileId, StorageBackend, StorageError,
};
use crate::circuit::metrics::{
    FILES_CREATED, READS_FAILED, READS_SUCCESS, TOTAL_BYTES_READ, TOTAL_BYTES_WRITTEN,
    WRITES_SUCCESS,
};
use crate::storage::buffer_cache::FBuf;

struct MemoryFile {
    file_id: FileId,
    path: PathBuf,
    blocks: HashMap<u64, Arc<FBuf>>,
    size: u64,
}

impl HasFileId for MemoryFile {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

/// State of the backend needed to satisfy the storage APIs.
pub struct MemoryBackend {
    /// Meta-data of all files we created so far.
    files: RwLock<HashMap<PathBuf, Arc<MemoryFile>>>,
}

impl MemoryBackend {
    fn get() -> Arc<Self> {
        static BACKEND: LazyLock<Arc<MemoryBackend>> = LazyLock::new(|| {
            Arc::new(MemoryBackend {
                files: RwLock::new(HashMap::new()),
            })
        });
        Arc::clone(&BACKEND)
    }

    fn insert(&self, mf: Arc<MemoryFile>) {
        self.files.write().unwrap().insert(mf.path.clone(), mf);
    }
}

impl FileWriter for MemoryFile {
    fn write_block(&mut self, offset: u64, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let data = Arc::new(data);
        self.blocks.insert(offset, data.clone());

        let min_size = offset + data.len() as u64;
        if min_size > self.size {
            self.size = min_size;
        }

        counter!(TOTAL_BYTES_WRITTEN).increment(data.len() as u64);
        counter!(WRITES_SUCCESS).increment(1);

        Ok(data)
    }

    fn complete(self: Box<Self>) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError> {
        let path = self.path.clone();
        let file = Arc::from(*self);
        MemoryBackend::get().insert(file.clone());
        Ok((file, path))
    }
}

impl FileReader for MemoryFile {
    fn mark_for_checkpoint(&self) {}

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        let block = self.blocks.get(&location.offset);
        if let Some(block) = block {
            if location.size == block.len() {
                counter!(TOTAL_BYTES_READ).increment(block.len() as u64);
                counter!(READS_SUCCESS).increment(1);
                return Ok(block.clone());
            }
        }
        counter!(READS_FAILED).increment(1);
        Err(IoError::from(ErrorKind::UnexpectedEof).into())
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        Ok(self.size)
    }
}

impl StorageBackend for MemoryBackend {
    fn create_named(&self, name: &Path) -> Result<Box<dyn FileWriter>, StorageError> {
        let file_id = FileId::new();
        let fm = MemoryFile {
            file_id,
            path: name.to_path_buf(),
            blocks: HashMap::new(),
            size: 0,
        };
        counter!(FILES_CREATED).increment(1);
        Ok(Box::new(fm))
    }

    fn open(&self, name: &Path) -> Result<Arc<dyn FileReader>, StorageError> {
        let files = self.files.read().unwrap();
        match files.get(name) {
            Some(file) => Ok(file.clone()),
            None => Err(StorageError::StdIo(ErrorKind::NotFound)),
        }
    }
}
