//! Implementation of the storage backend [`Storage`] API using the [`io_uring`]
//! library.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error as IoError, ErrorKind};
use std::mem::ManuallyDrop;
use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use io_uring::squeue::Entry;
use io_uring::{opcode, types::Fd, IoUring};
use libc::{c_void, iovec};
use metrics::{counter, histogram};
use pipeline_types::config::StorageCacheConfig;

use crate::storage::backend::{
    metrics::{
        FILES_CREATED, FILES_DELETED, READS_FAILED, READS_SUCCESS, READ_LATENCY, TOTAL_BYTES_READ,
        TOTAL_BYTES_WRITTEN, WRITES_SUCCESS, WRITE_LATENCY,
    },
    AtomicIncrementOnlyI64, FileHandle, ImmutableFileHandle, Storage, NEXT_FILE_HANDLE,
};
use crate::storage::buffer_cache::FBuf;
use crate::storage::init;

use super::metrics::describe_disk_metrics;
use super::{StorageCacheFlags, StorageError};

#[cfg(test)]
mod tests;

/// Meta-data we keep per file we created.
struct FileMetaData {
    /// The file.
    file: Rc<File>,

    /// File name.
    path: PathBuf,

    /// File size.
    size: u64,

    /// Total of the `work` elements in the [`Request`]s that reference this
    /// file within [`Inner::requests`].
    queued_work: u64,

    /// If non-[`None`], the error that occurred while writing the file.  Errors
    /// are reported by request completions, which might be processed while work
    /// is happening for a file other than the one we're currently working on,
    /// so we have to save the error and report it for the correct file.
    error: Option<ErrorKind>,

    /// Write operation we're currently accumulating.
    write: VectoredWrite,
}

impl FileMetaData {
    fn error(&self) -> Result<(), IoError> {
        self.error.map_or(Ok(()), |kind| Err(kind.into()))
    }
}

/// An accumulator for a multi-buffer sequential write operation.
///
/// It's significantly faster to write multiple buffers in a single operation
/// than in multiple operations, even over `io_uring`, so this gathers them up.
#[derive(Default)]
struct VectoredWrite {
    /// Buffers accumulated to write so far.
    buffers: Vec<Arc<FBuf>>,

    /// Starting offset for the write.  If there are no buffers yet, this is not
    /// meaningful (and should be 0).
    offset: u64,

    /// Sum of the lengths of the buffers in `buffers`.
    len: u64,
}

impl VectoredWrite {
    /// Tries to add a write of `block` at `offset` to this vectored write.
    /// This will fail if adding `block` would make the vectored write too big
    /// or non-sequential.
    fn append(&mut self, block: &Arc<FBuf>, offset: u64) -> Result<(), ()> {
        if self.len >= 1024 * 1024 || (self.len > 0 && self.offset + self.len != offset) {
            Err(())
        } else {
            if self.buffers.is_empty() {
                self.offset = offset;
                self.len = 0;
            }
            self.len += block.len() as u64;
            self.buffers.push(block.clone());
            Ok(())
        }
    }
    fn take(&mut self) -> Self {
        std::mem::take(self)
    }
}

/// Resources associated with a request that must be kept around until the
/// request is completed.
#[allow(dead_code)]
struct RequestResources {
    /// Needs to be kept until request completion.
    buffers: Vec<Arc<FBuf>>,

    /// Needs to be kept until request completion.  (The kernel does not copy
    /// out the iovec at submission time, it accesses it throughout request
    /// execution.)
    ///
    /// ([`std::io::IoSlice`] would be better if its `advance_slices` method
    /// were ever to be stabilized.)
    iovec: Vec<iovec>,

    /// Needs to be kept until request submission (but it's easier to just keep
    /// it until completion).
    file: Rc<File>,
}

/// A "write" or "fsync" request passed to the kernel via `io_uring`.
struct Request {
    /// Handle from `FileHandle`.
    fd: i64,

    /// A measure of the amount of work done by this request.  We limit the
    /// total amount of queued work per-file because of the resource cost,
    /// mainly memory (since we can't free write buffers until the write
    /// completes).
    ///
    /// We currently measure work for a write request as the number of bytes
    /// written.  We give fsync requests an arbitrary work of 0 because we don't
    /// need to track the amount of work they do.
    work: u64,

    /// Expected result from the request (otherwise the request is considered to
    /// have failed).
    expected_result: i32,

    /// Tracks resources that must not be dropped until the request is
    /// completed, to prevent the kernel from scribbling on memory as a
    /// use-after-free error.
    resources: ManuallyDrop<RequestResources>,
}

impl Request {
    /// Drops our resources.  Must only be called after this request has
    /// completed (otherwise the kernel might scribble on freed memory).
    unsafe fn complete(self) {
        ManuallyDrop::into_inner(self.resources);
    }
}

struct Inner {
    /// Kernel `io_uring` structure.
    io_uring: IoUring,

    /// All files we created so far.
    files: HashMap<i64, FileMetaData>,

    /// All requests that have been submitted to `io_uring` and not yet
    /// completed..
    requests: HashMap<u64, Request>,

    /// Next request ID to use.
    ///
    /// The request IDs are passed to the kernel in the submission queue.  The
    /// kernel passes them back in completion queue entries to identify the
    /// request that completed.
    next_request_id: u64,

    /// A global counter to get unique identifiers for file-handles.
    next_file_id: Arc<AtomicIncrementOnlyI64>,
}

impl Inner {
    fn new(next_file_id: Arc<AtomicIncrementOnlyI64>) -> Result<Self, IoError> {
        // Create our io_uring.
        //
        // We specify a submission queue size of 1 because the kernel sizes the
        // maximum number of I/O threads as `min(n_sqes, 4 * n_cpus)` and,
        // generally speaking, creating a lot of I/O threads for our file access
        // is counterproductive.  One thread is enough and more than that slows
        // us down.
        //
        // We specify a larger completion queue size so that we can leave
        // completions around to be processed in a batch.
        let io_uring = IoUring::builder()
            .setup_coop_taskrun()
            .setup_single_issuer()
            .setup_cqsize(512)
            .build(1)?;

        Ok(Self {
            io_uring,
            files: HashMap::new(),
            requests: HashMap::new(),
            next_request_id: 0,
            next_file_id,
        })
    }

    /// Processes all of the entries in the completion queue.
    fn run_completions(&mut self) -> usize {
        let mut cq = self.io_uring.completion();
        cq.sync();
        assert_eq!(cq.overflow(), 0);
        let n = cq.len();
        for entry in cq {
            let request = self.requests.remove(&entry.user_data()).unwrap();
            if let Some(fm) = self.files.get_mut(&request.fd) {
                fm.queued_work -= request.work;
                if request.expected_result != entry.result()
                    && matches!(&fm.error, None | Some(ErrorKind::WriteZero))
                {
                    fm.error = if entry.result() < 0 {
                        Some(IoError::from_raw_os_error(-entry.result()).kind())
                    } else {
                        // Short write.
                        //
                        // The likely issues are a full disk or an
                        // underlying device error.  The best response would
                        // be to re-issue the remainder of the write so that
                        // we can get the real error (or recover and
                        // finish).  It would be hard to test this behavior
                        // properly.  For now, just provide a placeholder
                        // error.
                        //
                        // If we get a more specific error later, we'll
                        // update it.
                        Some(ErrorKind::WriteZero)
                    }
                }
            } else {
                // The file was deleted with `delete_mut` without ever calling
                // `complete`.
            }
            unsafe { request.complete() };
        }
        n
    }

    /// Adds `entry` to the submission queue and prepares for its completion.
    /// `entry` should be an operation on `fd` that accounts for `work` work
    /// units (see [`Request::work`]), whose expected return value is
    /// `expected_result` and that requires `resources` not to be released
    /// before completion.
    fn submit_request(
        &mut self,
        fd: i64,
        entry: Entry,
        work: u64,
        expected_result: i32,
        resources: RequestResources,
    ) -> Result<(), IoError> {
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let entry = entry.user_data(request_id);

        // Submit the entry.  The only reason submission can fail is that the
        // submission queue is full of unsubmitted requests, and we submit for
        // every operation, so this should never fail.
        unsafe { self.io_uring.submission().push(&entry) }.unwrap();
        self.io_uring.submit()?;

        self.requests.insert(
            request_id,
            Request {
                fd,
                work,
                expected_result,
                resources: ManuallyDrop::new(resources),
            },
        );

        self.files.get_mut(&fd).unwrap().queued_work += work;
        Ok(())
    }

    /// If `fd` has any pending writes, submits them for processing.
    fn flush(&mut self, fd: i64) -> Result<(), IoError> {
        let fm = self.files.get_mut(&fd).unwrap();
        let write = fm.write.take();
        if write.len == 0 {
            return Ok(());
        }
        let request_start = Instant::now();

        let file = fm.file.clone();
        let iovec = write
            .buffers
            .iter()
            .map(|buf| iovec {
                iov_base: buf.as_ptr() as *mut c_void,
                iov_len: buf.len(),
            })
            .collect::<Vec<_>>();
        let entry = opcode::Writev::new(Fd(file.as_raw_fd()), iovec.as_ptr(), iovec.len() as u32)
            .offset(write.offset)
            .build();

        self.submit_request(
            fd,
            entry,
            write.len,
            write.len as i32,
            RequestResources {
                iovec,
                buffers: write.buffers,
                file,
            },
        )?;

        counter!(TOTAL_BYTES_WRITTEN).increment(write.len);
        counter!(WRITES_SUCCESS).increment(1);
        histogram!(WRITE_LATENCY).record(request_start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Blocks until `fd` has no more than `max_queued_work` units of work in
    /// flight.
    fn limit_queued_work(&mut self, fd: i64, max_queued_work: u64) -> Result<(), IoError> {
        loop {
            let file = self.files.get(&fd).unwrap();
            file.error()?;
            if file.queued_work <= max_queued_work {
                return Ok(());
            }

            if self.run_completions() == 0 {
                self.io_uring.submit_and_wait(1)?;
            }
        }
    }

    fn create_named(
        &mut self,
        path: PathBuf,
        cache: StorageCacheConfig,
    ) -> Result<FileHandle, StorageError> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .cache_flags(&cache)
            .open(&path)?;

        let file_counter = self.next_file_id.increment();
        self.files.insert(
            file_counter,
            FileMetaData {
                file: Rc::new(file),
                path,
                size: 0,
                queued_work: 0,
                error: None,
                write: VectoredWrite::default(),
            },
        );
        counter!(FILES_CREATED).increment(1);

        Ok(FileHandle(file_counter))
    }

    fn open(
        &mut self,
        path: PathBuf,
        cache: StorageCacheConfig,
    ) -> Result<ImmutableFileHandle, StorageError> {
        let attr = std::fs::metadata(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&cache)
            .open(&path)?;

        let file_counter = self.next_file_id.increment();
        self.files.insert(
            file_counter,
            FileMetaData {
                file: Rc::new(file),
                path,
                size: attr.size(),
                queued_work: 0,
                error: None,
                write: VectoredWrite::default(),
            },
        );

        Ok(ImmutableFileHandle(file_counter))
    }

    fn write_block(
        &mut self,
        fd: &FileHandle,
        offset: u64,
        block: Arc<FBuf>,
    ) -> Result<Arc<FBuf>, StorageError> {
        let end_offset = offset + block.len() as u64;

        let fm = self.files.get_mut(&fd.0).unwrap();
        fm.error()?;
        fm.size = fm.size.max(end_offset);

        if fm.write.append(&block, offset).is_err() {
            self.flush(fd.0)?;
            self.limit_queued_work(fd.0, 4 * 1024 * 1024)?;

            let fm = self.files.get_mut(&fd.0).unwrap();
            fm.write.append(&block, offset).unwrap();
        }

        Ok(block)
    }

    fn do_complete(&mut self, fd: i64) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        // Submit any pending writes and then wait for the writes to complete.
        //
        // This has two purposes.  First, it means that, on the read side, we
        // don't have to be prepared to service uncompleted reads ourselves
        // through an internal cache lookup (although we could put the
        // `IO_DRAIN` flag on the first "read" operation as a barrier).
        //
        // Second, it ensures that we don't allow reads of data that might
        // encounter an error before they make it to disk.  This property might
        // not be important.
        self.flush(fd)?;
        self.limit_queued_work(fd, 0)?;

        let fm = self.files.get(&fd).unwrap();
        let file = fm.file.clone();
        let path = fm.path.clone();

        // Submit an `fsync` request for the file.  We don't wait for it to
        // complete.  This ensures that the metadata (and data, but we probably
        // used `O_DIRECT`) for the file will be committed "soon".  It also
        // ensures that we can make sure that everything we've written is on
        // stable storage for the purpose of a checkpoint (which we don't do
        // yet) simply by waiting for the entire io_uring to drain.
        self.submit_request(
            fd,
            opcode::Fsync::new(Fd(file.as_raw_fd())).build(),
            0,
            0,
            RequestResources {
                iovec: Vec::new(),
                buffers: Vec::new(),
                file,
            },
        )?;

        Ok((ImmutableFileHandle(fd), path))
    }

    fn complete(&mut self, fd: i64) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        let retval = self.do_complete(fd);
        if retval.is_err() {
            self.delete(fd)?;
        }
        retval
    }

    fn read_block(&self, fd: i64, offset: u64, size: usize) -> Result<Arc<FBuf>, StorageError> {
        let mut buffer = FBuf::with_capacity(size);

        let fm = self.files.get(&fd).unwrap();

        let request_start = Instant::now();
        match fm
            .error()
            .and_then(|_| buffer.read_exact_at(&fm.file, offset, size))
        {
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

    fn delete(&mut self, fd: i64) -> Result<(), StorageError> {
        let fm = self.files.remove(&fd).unwrap();
        std::fs::remove_file(fm.path)?;
        counter!(FILES_DELETED).increment(1);
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<(), StorageError> {
        let fds = self.files.keys().copied().collect::<Vec<_>>();
        for fd in fds {
            self.flush(fd)?;
        }
        while !self.requests.is_empty() {
            if self.run_completions() == 0 {
                self.io_uring.submit_and_wait(1)?;
            }
        }
        Ok(())
    }
}

/// State of the backend needed to satisfy the storage APIs.
pub struct IoUringBackend {
    inner: RefCell<Inner>,
    cache: StorageCacheConfig,
    /// Directory in which we keep the files.
    base: PathBuf,
}

impl IoUringBackend {
    /// Instantiates a new backend.
    ///
    /// ## Parameters
    /// - `base`: Directory in which we keep the files.
    /// - `next_file_id`: A counter to get unique identifiers for file-handles.
    ///   Note that in case we use a global buffer cache, this counter should be
    ///   shared among all instances of the backend.
    pub fn new<P: AsRef<Path>>(
        base: P,
        cache: StorageCacheConfig,
        next_file_id: Arc<AtomicIncrementOnlyI64>,
    ) -> Result<Self, IoError> {
        init();
        describe_disk_metrics();
        Ok(Self {
            base: base.as_ref().to_path_buf(),
            cache,
            inner: RefCell::new(Inner::new(next_file_id)?),
        })
    }

    /// See [`IoUringBackend::new`]. This function is a convenience function
    /// that creates a new backend with global unique file-handle counter.
    pub fn with_base<P: AsRef<Path>>(base: P, cache: StorageCacheConfig) -> Result<Self, IoError> {
        Self::new(
            base,
            cache,
            NEXT_FILE_HANDLE
                .get_or_init(|| Arc::new(Default::default()))
                .clone(),
        )
    }

    /// Returns the directory in which the backend creates files.
    pub fn path(&self) -> &Path {
        self.base.as_path()
    }

    /// Flushes all data and waits for it to reach stable storage.
    pub fn checkpoint(&self) -> Result<(), StorageError> {
        self.inner.borrow_mut().checkpoint()
    }
}

impl Storage for IoUringBackend {
    fn create_named(&self, name: &Path) -> Result<FileHandle, StorageError> {
        self.inner
            .borrow_mut()
            .create_named(self.base.join(name), self.cache)
    }

    fn open(&self, name: &Path) -> Result<ImmutableFileHandle, StorageError> {
        self.inner
            .borrow_mut()
            .open(self.base.join(name), self.cache)
    }

    fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.inner.borrow_mut().delete(fd.0)
    }

    fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        self.inner.borrow_mut().delete(fd.0)
    }

    fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Arc<FBuf>, StorageError> {
        self.inner
            .borrow_mut()
            .write_block(fd, offset, Arc::new(data))
    }

    fn complete(&self, fd: FileHandle) -> Result<(ImmutableFileHandle, PathBuf), StorageError> {
        self.inner.borrow_mut().complete(fd.0)
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
        self.inner.borrow().read_block(fd.0, offset, size)
    }

    fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        Ok(self.inner.borrow().files.get(&fd.0).unwrap().size)
    }

    fn base(&self) -> PathBuf {
        self.base.clone()
    }
}
