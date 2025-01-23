//! Implementation of the storage backend [`Storage`] API using the [`io_uring`]
//! library.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Error as IoError, ErrorKind};
use std::mem::ManuallyDrop;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::rc::Weak;
use std::{rc::Rc, sync::Arc};

use feldera_types::config::StorageCacheConfig;
use io_uring::squeue::Entry;
use io_uring::{opcode, types::Fd, IoUring};
use libc::{c_void, iovec};
use metrics::counter;

use crate::circuit::metrics::FILES_CREATED;
use crate::storage::backend::Storage;
use crate::storage::buffer_cache::FBuf;
use crate::storage::init;

use super::posixio_impl::PosixReader;
use super::{FileId, FileReader, FileWriter, HasFileId, StorageCacheFlags, StorageError, IOV_MAX};

#[cfg(test)]
mod tests;

/// Meta-data we keep per file we created.
struct IoUringWriter {
    backend: Rc<RefCell<Inner>>,

    file_id: FileId,
    path: PathBuf,

    status: Rc<RefCell<WorkStatus>>,

    /// The file we're writing.
    file: Arc<File>,

    /// Write operation we're currently accumulating.
    write: VectoredWrite,
}

#[derive(Default)]
struct WorkStatus {
    /// Total of the `work` elements in the [`Request`]s that reference this
    /// file within [`Inner::requests`].
    queued_work: u64,

    /// If non-[`None`], the error that occurred while writing the file.  Errors
    /// are reported by request completions, which might be processed while work
    /// is happening for a file other than the one we're currently working on,
    /// so we have to save the error and report it for the correct file.
    error: Option<ErrorKind>,
}

impl IoUringWriter {
    fn new(backend: Rc<RefCell<Inner>>, file: Arc<File>, path: PathBuf) -> Self {
        Self {
            backend,
            file,
            file_id: FileId::new(),
            path,
            status: Rc::new(RefCell::new(WorkStatus::default())),
            write: VectoredWrite::default(),
        }
    }
    fn error(&self) -> Result<(), IoError> {
        self.status
            .borrow()
            .error
            .map_or(Ok(()), |kind| Err(kind.into()))
    }

    /// If this file has any pending writes, submits them for processing.
    fn flush(&mut self) -> Result<(), IoError> {
        let write = self.write.take();
        if write.len == 0 {
            return Ok(());
        }

        let iovec = write
            .buffers
            .iter()
            .map(|buf| iovec {
                iov_base: buf.as_ptr() as *mut c_void,
                iov_len: buf.len(),
            })
            .collect::<Vec<_>>();
        let entry = opcode::Writev::new(
            Fd(self.file.as_raw_fd()),
            iovec.as_ptr(),
            iovec.len() as u32,
        )
        .offset(write.offset)
        .build();

        self.backend.borrow_mut().submit_request(
            &self.status,
            entry,
            write.len,
            write.len as i32,
            RequestResources {
                iovec,
                buffers: write.buffers,
                file: self.file.clone(),
            },
        )?;
        Ok(())
    }

    /// Blocks until `fd` has no more than `max_queued_work` units of work in
    /// flight.
    fn limit_queued_work(&mut self, max_queued_work: u64) -> Result<(), IoError> {
        loop {
            self.error()?;
            if self.status.borrow().queued_work <= max_queued_work {
                return Ok(());
            }

            if self.backend.borrow_mut().run_completions() == 0 {
                self.backend.borrow().io_uring.submit_and_wait(1)?;
            }
        }
    }
}

impl HasFileId for IoUringWriter {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl FileWriter for IoUringWriter {
    fn write_block(&mut self, offset: u64, data: FBuf) -> Result<Arc<FBuf>, StorageError> {
        let block = Arc::new(data);
        self.error()?;

        if self.write.append(&block, offset).is_err() {
            self.flush()?;
            self.limit_queued_work(4 * 1024 * 1024)?;
            self.write.append(&block, offset).unwrap();
        }

        Ok(block)
    }

    fn complete(mut self: Box<Self>) -> Result<(Arc<dyn FileReader>, PathBuf), StorageError> {
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
        self.flush()?;
        self.limit_queued_work(0)?;

        // Submit an `fsync` request for the file.  This ensures that the
        // metadata (and data, but we probably used `O_DIRECT`) for the file
        // will be committed "soon".  It also ensures that we can make sure that
        // everything we've written is on stable storage for the purpose of a
        // checkpoint (which we don't do yet) simply by waiting for the entire
        // io_uring to drain.
        self.backend.borrow_mut().submit_request(
            &self.status,
            opcode::Fsync::new(Fd(self.file.as_raw_fd())).build(),
            0,
            0,
            RequestResources {
                iovec: Vec::new(),
                buffers: Vec::new(),
                file: self.file.clone(),
            },
        )?;
        self.limit_queued_work(0)?;

        Ok((
            Arc::new(PosixReader::new(
                self.file.clone(),
                self.file_id,
                self.path.clone(),
                false,
            )),
            self.path.clone(),
        ))
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

    /// Starting offset for the write. If there are no buffers yet, this is not
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
        if self.len >= 1024 * 1024
            || (self.len > 0 && self.offset + self.len != offset)
            || self.buffers.len() >= *IOV_MAX
        {
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
    file: Arc<File>,
}

/// A "write" or "fsync" request passed to the kernel via `io_uring`.
struct Request {
    /// Handle from `FileHandle`.
    status: Weak<RefCell<WorkStatus>>,

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

    /// All requests that have been submitted to `io_uring` and not yet
    /// completed.
    requests: HashMap<u64, Request>,

    /// Next request ID to use.
    ///
    /// The request IDs are passed to the kernel in the submission queue.  The
    /// kernel passes them back in completion queue entries to identify the
    /// request that completed.
    next_request_id: u64,
}

impl Inner {
    fn new() -> Result<Self, IoError> {
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
            requests: HashMap::new(),
            next_request_id: 0,
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
            if let Some(status) = request.status.upgrade() {
                status.borrow_mut().queued_work -= request.work;
                if request.expected_result != entry.result()
                    && matches!(&status.borrow().error, None | Some(ErrorKind::WriteZero))
                {
                    status.borrow_mut().error = if entry.result() < 0 {
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
                // The file was dropped.
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
        status: &Rc<RefCell<WorkStatus>>,
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

        status.borrow_mut().queued_work += work;
        self.requests.insert(
            request_id,
            Request {
                status: Rc::downgrade(status),
                work,
                expected_result,
                resources: ManuallyDrop::new(resources),
            },
        );
        Ok(())
    }
}

/// State of the backend needed to satisfy the storage APIs.
pub struct IoUringBackend {
    inner: Rc<RefCell<Inner>>,
    cache: StorageCacheConfig,
    /// Directory in which we keep the files.
    base: PathBuf,
}

impl IoUringBackend {
    /// Instantiates a new backend in directory `base` with cache behavior
    /// `cache`.
    pub fn new<P: AsRef<Path>>(base: P, cache: StorageCacheConfig) -> Result<Self, IoError> {
        init();
        Ok(Self {
            base: base.as_ref().to_path_buf(),
            cache,
            inner: Rc::new(RefCell::new(Inner::new()?)),
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

impl Storage for IoUringBackend {
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
            self.inner.clone(),
            Arc::new(file),
            path,
        )))
    }

    fn open(&self, name: &Path) -> Result<Arc<dyn FileReader>, StorageError> {
        PosixReader::open(self.base.join(name), self.cache)
    }
}
