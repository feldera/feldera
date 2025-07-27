//! [StorageBackend] implementation in memory.
//!
//! This is useful for performance testing, not as part of a production system.

use super::{
    BlockLocation, FileId, FileReader, FileWriter, HasFileId, StorageBackend, StorageError,
};
use crate::circuit::metrics::FILES_CREATED;
use crate::storage::buffer_cache::FBuf;
use feldera_storage::{StorageFileType, StoragePath};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    sync::{Arc, RwLock},
};

struct MemoryFile {
    file_id: FileId,
    path: StoragePath,
    blocks: Vec<(u64, Arc<FBuf>)>,
    size: u64,
}

impl HasFileId for MemoryFile {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

struct MemoryBackendInner {
    /// Meta-data of all files we created so far.
    files: RwLock<HashMap<StoragePath, Arc<MemoryFile>>>,

    /// Tracks the total size of all the files.
    usage: Arc<AtomicI64>,
}

/// State of the backend needed to satisfy the storage APIs.
#[derive(Clone)]
pub struct MemoryBackend(Arc<MemoryBackendInner>);

impl Default for MemoryBackend {
    fn default() -> Self {
        Self(Arc::new(MemoryBackendInner {
            files: RwLock::new(HashMap::new()),
            usage: Arc::new(AtomicI64::new(0)),
        }))
    }
}

impl MemoryBackend {
    /// Creates and returns a new memory backend.
    pub fn new() -> Self {
        Self::default()
    }
}

struct MemoryWriter {
    backend: MemoryBackend,
    file: MemoryFile,
    drop: DeleteOnDrop,
}

impl MemoryWriter {
    fn new(backend: MemoryBackend, name: &StoragePath) -> Self {
        Self {
            file: MemoryFile {
                file_id: FileId::new(),
                path: name.clone(),
                blocks: Vec::new(),
                size: 0,
            },
            drop: DeleteOnDrop {
                usage: backend.0.usage.clone(),
                size: 0,
            },
            backend,
        }
    }
}

impl HasFileId for MemoryWriter {
    fn file_id(&self) -> FileId {
        self.file.file_id
    }
}

impl FileWriter for MemoryWriter {
    fn write_block(&mut self, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let data = Arc::new(data);
        self.file.blocks.push((self.file.size, data.clone()));

        self.file.size += data.len() as u64;
        self.drop.size += data.len() as u64;

        self.drop
            .usage
            .fetch_add(data.len() as i64, Ordering::Relaxed);

        Ok(data)
    }

    fn complete(mut self: Box<Self>) -> Result<(Arc<dyn FileReader>, StoragePath), StorageError> {
        let path = self.file.path.clone();
        self.drop.size = 0;
        let file = Arc::new(self.file);
        self.backend
            .0
            .files
            .write()
            .unwrap()
            .insert(file.path.clone(), file.clone());
        let reader = Arc::new(MemoryReader {
            backend: self.backend,
            file,
            keep: AtomicBool::new(false),
        });
        Ok((reader, path))
    }
}

struct DeleteOnDrop {
    usage: Arc<AtomicI64>,
    size: u64,
}

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        self.usage.fetch_sub(self.size as i64, Ordering::Relaxed);
    }
}

struct MemoryReader {
    backend: MemoryBackend,
    file: Arc<MemoryFile>,
    keep: AtomicBool,
}

impl HasFileId for MemoryReader {
    fn file_id(&self) -> FileId {
        self.file.file_id
    }
}

impl FileReader for MemoryReader {
    fn mark_for_checkpoint(&self) {
        self.keep.store(true, Ordering::Relaxed);
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        if location.after() > self.file.size {
            return Err(IoError::from(ErrorKind::UnexpectedEof).into());
        }

        let index = self
            .file
            .blocks
            .partition_point(|(offset, _)| *offset <= location.offset);

        let index = match self.file.blocks.get(index) {
            Some((offset, data)) if location.offset == *offset => {
                if location.size == data.len() {
                    return Ok(data.clone());
                }
                index
            }
            _ => index - 1,
        };

        let mut block = FBuf::with_capacity(location.size);
        for (offset, data) in &self.file.blocks[index..] {
            let start = location.offset.max(*offset) - *offset;
            let end = location.after().min(*offset + data.len() as u64) - *offset;
            block.extend_from_slice(&data[start as usize..end as usize]);
            if block.len() >= location.size {
                debug_assert_eq!(block.len(), location.size);
                return Ok(Arc::new(block));
            }
        }
        unreachable!();
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        Ok(self.file.size)
    }
}

impl Drop for MemoryReader {
    fn drop(&mut self) {
        if !self.keep.load(Ordering::Relaxed) {
            let _ = self.backend.delete(&self.file.path);
        }
    }
}

impl StorageBackend for MemoryBackend {
    fn create_named(&self, name: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError> {
        let fm = MemoryWriter::new(self.clone(), name);
        FILES_CREATED.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(fm))
    }

    fn open(&self, name: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError> {
        let files = self.0.files.read().unwrap();
        match files.get(name) {
            Some(file) => Ok(Arc::new(MemoryReader {
                backend: self.clone(),
                file: file.clone(),
                keep: AtomicBool::new(true),
            })),
            None => Err(StorageError::StdIo(ErrorKind::NotFound)),
        }
    }

    fn list(
        &self,
        parent: &StoragePath,
        cb: &mut dyn FnMut(&StoragePath, StorageFileType),
    ) -> Result<(), StorageError> {
        let entries = self
            .0
            .files
            .read()
            .unwrap()
            .iter()
            .filter(|&(name, _file)| name.prefix_matches(parent))
            .map(|(name, file)| (name.clone(), file.size))
            .collect::<Vec<_>>();
        for (path, size) in entries {
            cb(&path, StorageFileType::File { size });
        }
        Ok(())
    }

    fn delete(&self, name: &StoragePath) -> Result<(), StorageError> {
        let mut files = self.0.files.write().unwrap();
        match files.remove(name) {
            Some(file) => {
                self.0.usage.fetch_sub(file.size as i64, Ordering::Relaxed);
                Ok(())
            }
            None => Err(StorageError::StdIo(ErrorKind::NotFound)),
        }
    }

    fn delete_recursive(&self, parent: &StoragePath) -> Result<(), StorageError> {
        self.0
            .files
            .write()
            .unwrap()
            .retain(|name, _content| !name.prefix_matches(parent));
        Ok(())
    }

    fn usage(&self) -> Arc<AtomicI64> {
        self.0.usage.clone()
    }
}

#[cfg(test)]
mod tests {
    use feldera_storage::StorageBackend;
    use std::{path::Path, sync::Arc};

    use crate::storage::backend::{
        memory_impl::MemoryBackend,
        tests::{random_sizes, test_backend},
    };

    fn create_memory_backend(_path: &Path) -> Arc<dyn StorageBackend> {
        Arc::new(MemoryBackend::new())
    }

    /// Write 10 MiB total in 1 KiB chunks.  `VectoredWrite` flushes its buffer when it
    /// reaches 1 MiB of sequential data, and we limit the amount of queued work
    /// to 4 MiB, so this has a chance to trigger both limits.
    #[test]
    fn sequential_1024() {
        test_backend(Box::new(create_memory_backend), &[1024; 1024 * 10], true)
    }

    /// Verify that files get deleted if not marked for a checkpoint.
    #[test]
    fn delete_1024() {
        test_backend(Box::new(create_memory_backend), &[1024; 1024 * 10], false)
    }

    #[test]
    fn sequential_random() {
        test_backend(Box::new(create_memory_backend), &random_sizes(), true);
    }

    #[test]
    fn empty() {
        test_backend(Box::new(create_memory_backend), &[], true);
    }
}
