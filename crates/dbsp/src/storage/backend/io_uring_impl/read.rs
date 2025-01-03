use std::{
    cmp::min,
    collections::{BTreeMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Error as IoError, ErrorKind},
    mem::{self, ManuallyDrop},
    ops::Range,
    os::{
        fd::{AsFd, AsRawFd, BorrowedFd},
        unix::fs::MetadataExt,
    },
    path::PathBuf,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        mpsc::{channel, Receiver, Sender},
        Arc, LazyLock,
    },
    thread,
};

use feldera_types::config::StorageCacheConfig;
use io_uring::{opcode, squeue::Entry, types::Fd, IoUring};
use nix::{
    poll::{PollFd, PollFlags, PollTimeout},
    sys::eventfd::{EfdFlags, EventFd},
};
use tokio::sync::oneshot;
use tracing::error;

use crate::storage::{
    backend::{
        posixio_impl::DeleteOnDrop, BlockLocation, FileId, FileReader, HasFileId,
        StorageCacheFlags, StorageError,
    },
    buffer_cache::FBuf,
};

/// Sends a request to the [ReadThread].
///
/// Like [ReadThread], this is a singleton.
struct Submitter {
    /// Sends a read request to the read thread.
    sender: Sender<ReadRequest>,

    /// For waking up the read thread after we send a request.
    waker: Arc<EventFd>,
}

impl Submitter {
    fn new() -> Result<Self, IoError> {
        let waker = match EventFd::from_flags(EfdFlags::EFD_NONBLOCK) {
            Ok(eventfd) => Arc::new(eventfd),
            Err(errno) => {
                tracing::warn!("could not create eventfd ({errno}), falling back to POSIX I/O");
                return Err(errno.into());
            }
        };

        let (sender, receiver) = channel();
        let (init_sender, init_receiver) = oneshot::channel();
        thread::Builder::new()
            .name("io_uring".into())
            .spawn({
                let waker = waker.clone();
                move || match ReadThread::new(receiver, waker) {
                    Ok(mut thread) => {
                        init_sender.send(Ok(())).unwrap();
                        thread.run();
                    }
                    Err(error) => {
                        init_sender.send(Err(error)).unwrap();
                    }
                }
            })
            .unwrap();
        init_receiver
            .blocking_recv()
            .unwrap()
            .inspect_err(|error| {
                tracing::warn!(
                "could not create io_uring for read thread ({error}), falling back to POSIX I/O"
            );
            })?;

        Ok(Self { waker, sender })
    }

    fn singleton() -> Result<&'static Self, IoError> {
        static SUBMITTER: LazyLock<Result<Submitter, IoError>> = LazyLock::new(Submitter::new);
        match &*SUBMITTER {
            Ok(submitter) => Ok(submitter),
            Err(error) => Err(IoError::from(error.kind())),
        }
    }

    /// Submits `request` to the [ReadThread].
    fn submit(&self, request: ReadRequest) {
        self.sender.send(request).unwrap();
        self.waker.arm().unwrap();
    }
}

/// The I/O uring background read thread.
///
/// This is a per-process singleton.
struct ReadThread {
    /// The kernel io_uring structure.
    io_uring: IoUring,

    /// Receives new read requests from the rest of the process.
    receiver: Receiver<ReadRequest>,

    /// A kernel eventfd for waking us up to receive a new read request.
    ///
    /// In an ideal world, this would not exist. This does exist because our
    /// thread needs to be able to wait for the first of two different kinds of
    /// events:
    ///
    /// - A completion arriving on [Self::io_uring].
    ///
    /// - A new read request arriving on [Self::receiver].
    ///
    /// Either one of these by itself is easy, but `receiver` is not exposed to
    /// the kernel, so there's no way to ask the kernel to wait for one or the
    /// other. So, we have to add a kernel-visible notification that there's a
    /// new read request, via `waker`.
    waker: Arc<EventFd>,

    /// All requests that have been submitted to `io_uring` and not yet
    /// completed.
    ///
    /// These are indexed by the smallest ID in their ID range (by
    /// `r.ids.start`, for [ReadRequest] `r`).
    requests: BTreeMap<u64, ReadRequest>,
}

fn retry_submit(io_uring: &IoUring, want: usize) -> Result<usize, IoError> {
    loop {
        match io_uring.submit_and_wait(want) {
            Err(error)
                if error.kind() == ErrorKind::WouldBlock
                    || error.kind() == ErrorKind::Interrupted =>
            {
                // Retry following transient error.
            }
            result => return result,
        }
    }
}

impl ReadThread {
    /// Finds the request that holds the given `id`, if any.
    fn get_request(requests: &mut BTreeMap<u64, ReadRequest>, id: u64) -> Option<&mut ReadRequest> {
        match requests.range_mut(..=id).next_back() {
            Some((_, request)) if request.ids.contains(&id) => Some(request),
            _ => None,
        }
    }

    /// Process all completion queue entries in the completion queue.
    fn process_cqes(&mut self) -> usize {
        let mut n_completions = 0;
        for cqe in self.io_uring.completion() {
            n_completions += 1;

            // Find the corresponding `ReadRequest`.
            let id = cqe.user_data();
            let Some(request) = Self::get_request(&mut self.requests, id) else {
                error!("received io_uring completion for unknown request {id}");
                continue;
            };

            // Take the corresponding `PendingRead`, complete it, and store the result.
            //
            // If this `unwrap` fails, then the kernel sent two completions for
            // the same read operation, which it should not.
            let index = (id - request.ids.start) as usize;
            let pending = mem::take(&mut request.pending[index]).unwrap();
            request.results[index] = Some(pending.complete(cqe.result()));

            // If that was the last unfinished read operation in this
            // `ReadRequest`, complete the request as a whole.
            request.n_incomplete -= 1;
            if request.n_incomplete == 0 {
                // The first `unwrap` below cannot fail unless a single pending
                // read has completed more than once, which cannot happen.
                //
                // The second `unwrap` cannot fail because every `ReadRequest`
                // is in `self.request` under its starting ID.
                let results = request.results.drain(..).map(|r| r.unwrap()).collect();
                let start = request.ids.start;
                let request = self.requests.remove(&start).unwrap();
                (request.callback)(results);
            }
        }
        n_completions
    }

    /// Initiates I/O for `request` and adds it to our tracking for requests.
    fn add_request(&mut self, mut request: ReadRequest) {
        // Assemble all of the submission queue entries in advance so that we
        // can insert the request in the I/O queue. (There's a small chance we
        // might need to process some CQEs for the request before we finish
        // submitting all the SQEs, so this makes sure that that wouldn't fail.)
        let mut entries = request
            .pending
            .iter_mut()
            .enumerate()
            .map(|(i, pending)| {
                // This `unwrap` cannot panic because all of the read operations
                // are pending before we initiate I/O.
                pending
                    .as_mut()
                    .unwrap()
                    .sqe(&request.file)
                    .user_data(i as u64 + request.ids.start)
            })
            .collect::<VecDeque<_>>();
        assert!(self.requests.insert(request.ids.start, request).is_none());

        // Initiate I/O for each of the read operations in `request`.
        //
        // Ordinarily this loop will iterate once. It only iterates multiple
        // times if the submission queue is full, so that we need to wait for
        // some operations to complete before we submit more.
        while !entries.is_empty() {
            let mut submission = self.io_uring.submission();
            let needed = entries.len();
            let available = submission.capacity() - submission.len();
            let chunk = min(needed, available);
            if chunk > 0 {
                // We can submit `chunk` of the operations (most commonly,
                // `index == 0` and `chunk == n`).
                //
                // The `unwrap`s cannot fail because we already verified that
                // there is enough room for these SQEs.
                for entry in entries.drain(..chunk) {
                    unsafe { submission.push(&entry) }.unwrap();
                }
                drop(submission);
                retry_submit(&self.io_uring, 0).unwrap();
            } else {
                // We can't submit any operations because the submission queue
                // is full.  This must be because there are many operations
                // pending.  Wait for at least one operation to complete, then
                // process the completion queue, which ought to free up
                // submission queue space.
                drop(submission);
                retry_submit(&self.io_uring, 1).unwrap();
                self.process_cqes();
            }
        }
    }

    fn run(&mut self) {
        loop {
            let n_cqes = self.process_cqes();

            let mut n_new_requests = 0;
            while let Ok(new_request) = self.receiver.try_recv() {
                n_new_requests += 1;
                println!("adding {}-block request", new_request.pending.len());
                self.add_request(new_request);
            }
            if n_new_requests > 0 {
                // Ignore the return value because we don't want to treat
                // `EAGAIN` as an error.
                let _ = self.waker.read();
            }

            if n_cqes == 0 && n_new_requests == 0 {
                let _ = nix::poll::poll(
                    &mut [
                        PollFd::new(
                            unsafe { BorrowedFd::borrow_raw(self.io_uring.as_raw_fd()) },
                            PollFlags::POLLIN,
                        ),
                        PollFd::new(self.waker.as_fd(), PollFlags::POLLIN),
                    ],
                    PollTimeout::NONE,
                );
            }
        }
    }

    fn new(receiver: Receiver<ReadRequest>, waker: Arc<EventFd>) -> Result<Self, IoError> {
        Ok(Self {
            io_uring: IoUring::builder()
                .setup_coop_taskrun()
                .setup_single_issuer()
                .build(4096)?,
            requests: BTreeMap::new(),
            receiver,
            waker,
        })
    }
}

/// A "read" request passed to the kernel via `io_uring`.
struct ReadRequest {
    ids: Range<u64>,

    callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,

    pending: Vec<Option<PendingRead>>,
    results: Vec<Option<Result<Arc<FBuf>, StorageError>>>,

    n_incomplete: usize,

    file: Arc<File>,
}

impl ReadRequest {
    fn new(
        file: Arc<File>,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) -> Self {
        let n = blocks.len();
        Self {
            ids: Self::allocate_ids(blocks.len()),
            pending: blocks
                .into_iter()
                .map(|location| Some(PendingRead::new(location)))
                .collect(),
            results: vec![None; n],
            n_incomplete: n,
            callback,
            file,
        }
    }

    fn allocate_ids(n: usize) -> Range<u64> {
        static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(0);
        let id_base = NEXT_REQUEST_ID.fetch_add(n as u64, Ordering::Relaxed);
        id_base..id_base + n as u64
    }
}

struct PendingRead {
    buffer: ManuallyDrop<FBuf>,
    location: BlockLocation,
}

impl PendingRead {
    fn new(location: BlockLocation) -> Self {
        Self {
            buffer: ManuallyDrop::new(FBuf::with_capacity(location.size)),
            location,
        }
    }

    fn complete(self, retval: i32) -> Result<Arc<FBuf>, StorageError> {
        let mut buffer = ManuallyDrop::into_inner(self.buffer);
        if retval < 0 {
            Err(StorageError::from(IoError::from_raw_os_error(-retval)))
        } else if retval != self.location.size as i32 {
            Err(StorageError::from(IoError::from(ErrorKind::UnexpectedEof)))
        } else {
            unsafe { buffer.set_len(self.location.size) };
            Ok(Arc::new(buffer))
        }
    }

    /// Returns a submission queue entry for this read, in `file`.
    fn sqe(&mut self, file: &File) -> Entry {
        opcode::Read::new(
            Fd(file.as_raw_fd()),
            self.buffer.as_mut_ptr(),
            self.location.size as u32,
        )
        .offset(self.location.offset)
        .build()
    }
}

pub(super) struct IoUringReader {
    file: Arc<File>,
    file_id: FileId,
    drop: DeleteOnDrop,
    /// File size.
    ///
    /// -1 if the file size is unknown.
    size: AtomicI64,
}

impl IoUringReader {
    pub(super) fn init() -> Result<(), IoError> {
        Submitter::singleton().map(|_| ())
    }

    pub(super) fn new(file: Arc<File>, file_id: FileId, path: PathBuf, keep: bool) -> Self {
        Self {
            file,
            file_id,
            drop: DeleteOnDrop::new(path, keep),
            size: AtomicI64::new(-1),
        }
    }
    pub(super) fn open(
        path: PathBuf,
        cache: StorageCacheConfig,
    ) -> Result<Arc<dyn FileReader>, StorageError> {
        let file = OpenOptions::new()
            .read(true)
            .cache_flags(&cache)
            .open(&path)?;

        Ok(Arc::new(Self::new(
            Arc::new(file),
            FileId::new(),
            path,
            true,
        )))
    }
}

impl HasFileId for IoUringReader {
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

impl FileReader for IoUringReader {
    fn mark_for_checkpoint(&self) {
        self.drop.keep();
    }

    fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        let mut buffer = FBuf::with_capacity(location.size);

        match buffer.read_exact_at(&self.file, location.offset, location.size) {
            Ok(()) => Ok(Arc::new(buffer)),
            Err(e) => Err(e.into()),
        }
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        let sz = self.size.load(Ordering::Relaxed);
        if sz >= 0 {
            Ok(sz as u64)
        } else {
            let sz = self.file.metadata()?.size();
            self.size.store(sz.try_into().unwrap(), Ordering::Relaxed);
            Ok(sz)
        }
    }

    fn read_async(
        &self,
        blocks: Vec<BlockLocation>,
        callback: Box<dyn FnOnce(Vec<Result<Arc<FBuf>, StorageError>>) + Send>,
    ) {
        if !blocks.is_empty() {
            let request = ReadRequest::new(self.file.clone(), blocks, callback);
            Submitter::singleton().unwrap().submit(request);
        } else {
            // The read thread doesn't tolerate empty requests. Might as well
            // complete them early.
            callback(Vec::new())
        }
    }
}
