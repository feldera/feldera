//! Implementation of the storage backend APIs ([`StorageControl`],
//! [`StorageRead`], and [`StorageWrite`]) using the Monoio library.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use futures_locks::RwLock;
use metrics::{counter, histogram};
use monoio::fs::File;
use uuid::Uuid;

use crate::backend::{
    describe_disk_metrics, FileHandle, ImmutableFileHandle, StorageControl, StorageError,
    StorageRead, StorageWrite,
};
use crate::buffer_cache::FBuf;

#[cfg(test)]
pub(crate) mod tests;

/// Number of entries an IO-ring will have.
#[cfg(test)]
pub(self) const MAX_RING_ENTRIES: u32 = 32768;

/// Meta-data we keep per file we created.
struct FileMetaData {
    file: File,
    path: PathBuf,
}

/// State of the backend needed to satisfy the storage APIs.
pub struct MonoioBackend {
    /// Directory in which we keep the files.
    base: PathBuf,
    /// All files we created so far inside `base`.
    files: RwLock<HashMap<i64, FileMetaData>>,
    /// Counter used to generate unique file handles (`i64` key in `files`).
    file_counter: AtomicI64,
}

impl MonoioBackend {
    /// Instantiates a new backend.
    pub fn new<P: AsRef<Path>>(base: P) -> Self {
        describe_disk_metrics();
        Self {
            base: base.as_ref().to_path_buf(),
            files: RwLock::new(HashMap::new()),
            file_counter: AtomicI64::default(),
        }
    }

    /// Helper function to delete (mutable and immutable) files.
    async fn delete_inner(&self, fd: i64) -> Result<(), StorageError> {
        let fm = self.files.write().await.remove(&fd).unwrap();
        fm.file.close().await?;
        std::fs::remove_file(fm.path).unwrap();
        Ok(())
    }
}

impl StorageControl for MonoioBackend {
    async fn create(&self) -> Result<FileHandle, StorageError> {
        let file_counter = self.file_counter.fetch_add(1, Ordering::Relaxed);
        let name = Uuid::now_v7();
        let path = self.base.join(name.to_string() + ".feldera");
        let file = File::create(path.clone()).await?;

        let mut files = self.files.write().await;
        files.insert(file_counter, FileMetaData { file, path });

        Ok(FileHandle(file_counter))
    }

    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0).await
    }

    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.delete_inner(fd.0).await
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
        let (res, buf) = files
            .get(&fd.0)
            .unwrap()
            .file
            .write_all_at(data, offset)
            .await;
        res?;
        counter!("disk.total_bytes_written").increment(buf.len() as u64);
        counter!("disk.total_writes_success").increment(1);
        histogram!("disk.write_latency").record(request_start.elapsed().as_secs_f64());

        Ok(Rc::new(buf))
    }

    async fn complete(&self, fd: FileHandle) -> Result<ImmutableFileHandle, StorageError> {
        let mut files = self.files.write().await;

        let fm = files.remove(&fd.0).unwrap();
        fm.file.sync_all().await?;
        fm.file.close().await?;

        let readable_file = File::open(&fm.path).await?;
        files.insert(
            fd.0,
            FileMetaData {
                file: readable_file,
                path: fm.path,
            },
        );

        Ok(ImmutableFileHandle(fd.0))
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
            Ok(_len) => {
                counter!("disk.total_bytes_read").increment(buf.len() as u64);
                counter!("disk.total_reads_success").increment(1);
                histogram!("disk.read_latency").record(request_start.elapsed().as_secs_f64());

                if size != buf.len() {
                    Err(StorageError::ShortRead)
                } else {
                    Ok(Rc::new(buf))
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}
