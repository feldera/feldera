//! [StorageBackend] implementation with [io_uring].
//!
//! This backend's [FileReader] and [FileWriter] both use `io_uring`, but in
//! different ways:
//!
//! - The [FileWriter] implementation instantiates a per-backend (and thus
//!   effectively per-thread) io_uring. This io_uring has a very small
//!   submission and completion queue. This should generally be enough because
//!   writes are sequential and issued only when about 4 MiB of data accumulates
//!   on a file.
//!
//! - The [FileReader] implementation is a per-process singleton with larger
//!   submission and completion queues and a thread dedicated to servicing it.
//!   Because [FileReader] is `Send + Sync`, it's difficult to maintain the
//!   per-thread `io_uring` model. Because `io_uring` completions arrive
//!   asynchronously, it's also difficult to avoid dedicating a thread to it and
//!   still achieve low latency.

use std::{
    cell::RefCell,
    fs::OpenOptions,
    io::Error as IoError,
    path::{Path, PathBuf},
    rc::Rc,
    sync::Arc,
};

use feldera_types::config::StorageCacheConfig;
use metrics::counter;
use read::IoUringReader;
use write::{IoUringWriter, WriterInner};

use crate::storage::init;
use crate::{circuit::metrics::FILES_CREATED, storage::backend::StorageBackend};

use super::{FileReader, FileWriter, StorageCacheFlags, StorageError};

mod read;
mod write;

#[cfg(test)]
mod tests;

/// [io_uring] storage backend.
pub struct IoUringBackend {
    /// Internals for the writer, which is per-backend.
    ///
    /// The reader is a singleton per-process, so we don't have a reference to
    /// it here.
    writer_inner: Rc<RefCell<WriterInner>>,

    /// How we're caching data.
    cache: StorageCacheConfig,

    /// Directory in which we keep the files.
    base: PathBuf,
}

impl IoUringBackend {
    /// Instantiates a new backend in directory `base` with cache behavior
    /// `cache`.
    pub fn new<P: AsRef<Path>>(base: P, cache: StorageCacheConfig) -> Result<Self, IoError> {
        init();
        IoUringReader::init()?;
        Ok(Self {
            base: base.as_ref().to_path_buf(),
            cache,
            writer_inner: Rc::new(RefCell::new(WriterInner::new()?)),
        })
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    /// Flushes all data and waits for it to reach stable storage.
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        // There is nothing to do because the current implementation always
        // flushes data on `complete`, and any data that is in an uncompleted
        // file can't be read back anyway.
        Ok(())
    }
}

impl StorageBackend for IoUringBackend {
    fn create_named(&self, name: &Path) -> Result<Box<dyn FileWriter>, StorageError> {
        let path = self.base.join(name);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .cache_flags(&self.cache)
            .open(&path)?;

        counter!(FILES_CREATED).increment(1);
        Ok(Box::new(IoUringWriter::new(
            self.writer_inner.clone(),
            Arc::new(file),
            path,
        )))
    }

    fn open(&self, name: &Path) -> Result<Arc<dyn FileReader>, StorageError> {
        IoUringReader::open(self.base.join(name), self.cache)
    }
}
