//! Implementation of the storage backend APIs ([`StorageControl`],
//! [`StorageRead`], and [`StorageWrite`]) using the Monoio library.

use std::cell::RefCell;
use std::cmp::max;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Instant;

use async_lock::RwLock;
use metrics::{counter, histogram};
use monoio::fs::{File, OpenOptions};
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
const MAX_RING_ENTRIES: u32 = 32768;

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
        let file = open_as_direct(
            &path,
            OpenOptions::new().create_new(true).write(true).read(true),
        )
        .await?;
        let mut files = self.files.write().await;
        files.insert(
            file_counter,
            FileMetaData {
                file,
                path,
                size: RefCell::new(0),
            },
        );

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
        let fm = files.get(&fd.0).unwrap();
        let end_offset = offset + data.len() as u64;
        let (res, buf) = fm.file.write_all_at(data, offset).await;
        res?;
        fm.size.replace_with(|size| max(*size, end_offset));
        counter!("disk.total_bytes_written").increment(buf.len() as u64);
        counter!("disk.total_writes_success").increment(1);
        histogram!("disk.write_latency").record(request_start.elapsed().as_secs_f64());

        Ok(Rc::new(buf))
    }

    async fn complete(&self, fd: FileHandle) -> Result<ImmutableFileHandle, StorageError> {
        let mut files = self.files.write().await;

        let fm = files.remove(&fd.0).unwrap();
        fm.file.sync_all().await?;
        files.insert(fd.0, fm);

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

    async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        let files = self.files.read().await;
        let fm = files.get(&fd.0).unwrap();
        let size = *fm.size.borrow();
        Ok(size)
    }
}
