//! Implementation of the storage backend APIs ([`StorageControl`],
//! [`StorageRead`], and [`StorageWrite`]) using the Glommio library.
//!
//! Note: this backend is currently not efficient due to API mismatch between
//! Glommio and the storage APIs (we need extra copies -- see TODO's in this
//! file). Fixing it involves some work in Glommio, which we will eventually
//! submit to Glommio.

use std::collections::HashMap;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};

use glommio::io::DmaFile;
use glommio::sync::RwLock;
use uuid::Uuid;

use crate::backend::{
    FileHandle, ImmutableFileHandle, StorageControl, StorageError, StorageRead, StorageWrite,
};
use crate::buffer_cache::FBuf;

#[cfg(test)]
mod tests;

pub struct GlommioBackend {
    base: PathBuf,
    files: RwLock<HashMap<i64, DmaFile>>,
    file_counter: AtomicI64,
}

impl GlommioBackend {
    pub fn new<P: AsRef<std::path::Path>>(base: P) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
            files: RwLock::new(HashMap::new()),
            file_counter: AtomicI64::default(),
        }
    }

    async fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        let mut files = self.files.write().await?;
        let file = files.remove(&fd).unwrap();
        file.remove().await?;
        file.close().await?;
        Ok(())
    }
}

impl StorageControl for GlommioBackend {
    async fn create(&self) -> Result<FileHandle, StorageError> {
        let file_counter = self.file_counter.fetch_add(1, Ordering::Relaxed);
        let name = Uuid::now_v7();
        let path = self.base.join(name.to_string() + ".feldera");
        let file = DmaFile::create(path).await?;

        let mut files = self.files.write().await?;
        files.insert(file_counter, file);

        Ok(FileHandle(file_counter))
    }

    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0).await
    }

    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0).await
    }
}

impl StorageWrite for GlommioBackend {
    async fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        let to_write = data.len();
        let files = self.files.read().await?;

        // TODO: Inefficient copy to DmaBuffer
        let mut dma_buf = glommio::allocate_dma_buffer(data.len());
        dma_buf.as_bytes_mut().copy_from_slice(&data);

        let written = files.get(&fd.0).unwrap().write_at(dma_buf, offset).await?;
        assert_eq!(written, to_write);
        Ok(Rc::new(data))
    }

    async fn complete(&self, fd: FileHandle) -> Result<ImmutableFileHandle, StorageError> {
        let mut files = self.files.write().await?;

        let file = files.remove(&fd.0).unwrap();
        file.fdatasync().await?;
        let mut path_buf = PathBuf::new();
        file.path().unwrap().clone_into(&mut path_buf);
        file.close().await?;

        let readable_file = DmaFile::open(path_buf).await?;
        files.insert(fd.0, readable_file);

        Ok(ImmutableFileHandle(fd.0))
    }
}

impl StorageRead for GlommioBackend {
    async fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {
        unimplemented!()
    }

    async fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError> {
        let files = self.files.read().await?;
        let file = files.get(&fd.0).unwrap();
        let dma_buf = file.read_at_aligned(offset, size).await?;
        if dma_buf.len() != size {
            return Err(StorageError::ShortRead);
        }

        // TODO: Inefficient copy from ReadResult's DmaBuffer
        let mut fbuf = FBuf::with_capacity(dma_buf.len());
        fbuf.extend_from_slice(&dma_buf);

        Ok(Rc::new(fbuf))
    }
}
