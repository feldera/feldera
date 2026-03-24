//! Exchange operators implement a N-to-N communication pattern where
//! each participant sends exactly one value to and receives exactly one
//! value from each peer at every clock cycle.

// TODO: We may want to generalize these operators to implement N-to-M
// communication, including 1-to-N and N-to-1.

use crate::{
    NumEntries, WeakRuntime,
    circuit::{
        Host, LocalStoreMarker, OwnershipPreference, Runtime, Scope,
        metadata::{
            BatchSizeStats, EXCHANGE_DESERIALIZATION_TIME_SECONDS, EXCHANGE_DESERIALIZED_BYTES,
            EXCHANGE_WAIT_TIME_SECONDS, INPUT_BATCHES_STATS, MetaItem, OUTPUT_BATCHES_STATS,
            OperatorLocation, OperatorMeta,
        },
        operator_traits::{Operator, SinkOperator, SourceOperator},
        runtime::{WorkerLocation, WorkerLocations},
        tokio::TOKIO,
    },
    circuit_cache_key,
    storage::file::format::FixedLen,
};
use binrw::{BinRead, BinWrite};
use crossbeam_utils::CachePadded;
use feldera_samply::Span;
use feldera_storage::fbuf::FBuf;
use futures::{prelude::*, stream::FuturesUnordered};
use itertools::Itertools;
use rkyv::AlignedVec;
use size_of::HumanBytes;
use std::{
    borrow::Cow,
    collections::HashMap,
    io::{Cursor, ErrorKind, IoSlice},
    marker::PhantomData,
    mem::MaybeUninit,
    net::SocketAddr,
    ops::Range,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, OnceCell as TokioOnceCell},
    time::sleep,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::warn;
use typedmap::TypedMapKey;

/// Current time in microseconds.
fn current_time_usecs() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

// We use the `Runtime::local_store` mechanism to connect multiple workers
// to an `Exchange` instance.  During circuit construction, each worker
// allocates a unique id that happens to be the same across all workers.
// The worker then allocates a new `Exchange` and adds it to the local store
// using the id as a key.  If there already is an `Exchange` with this id in
// the store, created by another worker, a reference to that `Exchange` will
// be used instead.
circuit_cache_key!(local ExchangeCacheId<T>(ExchangeId => Arc<Exchange<T>>));

/// Header for data exchange from one host to another.
///
/// For a given exchange, all of the data from one host to another is sent in a
/// collection of contiguous blocks.  If host A with workers in the range
/// `senders` is sending to host B with workers in the range `receivers`, then
/// they will be sent in this order:
///
/// - `(senders.start, receivers.start)`
/// - `(senders.start, receivers.start + 1)`
/// - ...
/// - `(senders.start, receivers.end - 1)`
/// - `(senders.start + 1, receivers.start)`
/// - `(senders.start + 1, receivers.start + 1)`
/// - ...
/// - `(senders.start + 1, receivers.end - 1)`
/// - ...
/// - `(senders.end - 1, receivers.start)`
/// - `(senders.end - 1, receivers.start + 1)`
/// - ...
/// - `(senders.end - 1, receivers.end - 1)`
///
/// Only the start and end point of the sending range is indicated in the
/// header; the receiver knows its own range.  The receiver also keeps track of
/// incrementing the positions as it reads the data blocks.
#[binrw::binrw]
#[brw(little)]
struct ExchangeHeader {
    /// The unique identifier for the exchange.
    exchange_id: ExchangeId,
    /// `senders.start`
    sender_start: u32,
    /// `senders.end`
    sender_end: u32,
    /// Number of bytes of payload data following the header.
    ///
    /// The payload data is followed by zeros that align the data length to a
    /// multiple of 16 bytes.
    #[brw(align_after(16))]
    data_len: u32,
}

impl FixedLen for ExchangeHeader {
    const LEN: usize = 16;
}

impl ExchangeHeader {
    fn to_bytes(&self) -> [u8; Self::LEN] {
        let mut cursor = Cursor::new([0; Self::LEN]);
        self.write_le(&mut cursor).unwrap();
        assert_eq!(cursor.position(), Self::LEN as u64);
        cursor.into_inner()
    }

    fn from_bytes(bytes: &[u8; Self::LEN]) -> Self {
        let mut cursor = Cursor::new(bytes);
        let this = Self::read_le(&mut cursor).unwrap();
        assert_eq!(cursor.position(), Self::LEN as u64);
        this
    }

    async fn read<S>(stream: &mut S) -> std::io::Result<Option<Self>>
    where
        S: AsyncRead + Unpin,
    {
        let mut buf = [0; Self::LEN];
        match stream.read(&mut buf).await? {
            0 => Ok(None),
            n => {
                stream.read_exact(&mut buf[n..]).await?;
                Ok(Some(ExchangeHeader::from_bytes(&buf)))
            }
        }
    }
}

struct ExchangeServiceClient {
    receivers: Range<usize>,
    stream: tokio::sync::Mutex<TcpStream>,
}

impl ExchangeServiceClient {
    /// Sends messages in `exchange_id` from all of the worker threads in
    /// `senders` to all of the worker thread receivers in the server that
    /// processes the message.  The Bincode-encoded message from `sender` to
    /// `receiver` is `data[sender - senders.start][receiver -
    /// self.receivers.start]`.
    async fn exchange(
        &self,
        exchange_id: ExchangeId,
        senders: Range<usize>,
        data: Vec<Vec<FBuf>>,
    ) -> std::io::Result<()> {
        // We want to write each data buffer preceded by a header and followed
        // by padding.  To minimize the system calls required to do this, we
        // assemble them into one big collection of IoSlices.  First, create the
        // headers.
        let n = senders.len() * self.receivers.len();
        let mut headers = Vec::with_capacity(n);
        for data in data.iter().flatten() {
            headers.push(
                ExchangeHeader {
                    exchange_id,
                    sender_start: senders.start.try_into().unwrap(),
                    sender_end: senders.end.try_into().unwrap(),
                    data_len: data.len().try_into().unwrap(),
                }
                .to_bytes(),
            );
        }

        let zeros = [0; 16];

        // Now that we've got the headers, assemble the IoSlices.
        let mut slices = Vec::with_capacity(n * 3);
        let mut header = headers.iter();
        for data in data.iter().flatten() {
            slices.push(IoSlice::new(header.next().unwrap()));
            if !data.is_empty() {
                slices.push(IoSlice::new(data.as_slice()));
            }
            let pad = &zeros[..data.len().next_multiple_of(16) - data.len()];
            if !pad.is_empty() {
                slices.push(IoSlice::new(pad));
            }
        }

        // Finally, send the whole assembly.
        let size = slices.iter().map(|slice| slice.len()).sum::<usize>();
        let mut bufs = slices.as_mut_slice();
        let mut stream = self.stream.lock().await;
        let _span = Span::new("send")
            .with_category("Exchange")
            .with_tooltip(|| format!("send {} for exchange {exchange_id}", HumanBytes::from(size)));
        while !bufs.is_empty() {
            let n = stream.write_vectored(bufs).await?;
            IoSlice::advance_slices(&mut bufs, n);
        }
        Ok(())
    }
}

type ExchangeId = u32;

// Maps from an `exchange_id` to the `Inner` that implements the exchange.
type ExchangeDirectory = Arc<RwLock<HashMap<ExchangeId, Arc<InnerExchange>>>>;

struct ExchangeServer {
    receivers: Range<usize>,
    directory: ExchangeDirectory,
    stream: TcpStream,
}

impl ExchangeServer {
    async fn serve(mut self) -> std::io::Result<()> {
        while let Some(header) = ExchangeHeader::read(&mut self.stream).await? {
            let start = Instant::now();
            let exchange = self
                .directory
                .read()
                .unwrap()
                .get(&header.exchange_id)
                .unwrap_or_else(|| panic!("unknown exchange ID {}", header.exchange_id))
                .clone();
            let senders = (header.sender_start as usize)..(header.sender_end as usize);
            let mut bytes = senders.len() * self.receivers.len() * ExchangeHeader::LEN;
            let mut data = Vec::with_capacity(senders.len());
            let mut header = Some(header);
            for _ in senders.clone() {
                let mut receivers_data = Vec::with_capacity(self.receivers.len());
                for _ in self.receivers.clone() {
                    // Read the header (if we didn't already).
                    let header = if let Some(header) = header.take() {
                        header
                    } else {
                        ExchangeHeader::read(&mut self.stream)
                            .await?
                            .ok_or_else(|| std::io::Error::from(ErrorKind::UnexpectedEof))?
                    };

                    // Read the payload, which consists of `header.data_len`
                    // bytes followed by padding up to a multiple of 16 bytes.
                    //
                    // We read it into an `AlignedVec` so that we can pass it to
                    // `rkyv` later without copying.
                    //
                    // # Safety
                    //
                    // [std::slice::from_raw_parts_mut] has 4 undefined behavior
                    // conditions which we satisfy as follows:
                    //
                    // - Our pointer is nonnull and valid for reads and writes
                    //   (because of MaybeUninit) and aligned properly (no
                    //   alignment is needed).
                    //
                    // - The data is initialized (because of MaybeUninit).
                    //
                    // - There's no aliasing.
                    //
                    // - The slice has limited size.
                    let len = header.data_len as usize;
                    let padded_len = len.next_multiple_of(16);
                    let mut payload = AlignedVec::with_capacity(padded_len);
                    let pointer = payload.as_mut_ptr() as *mut MaybeUninit<u8>;
                    let mut slice = unsafe { std::slice::from_raw_parts_mut(pointer, padded_len) };
                    while !slice.is_empty() {
                        self.stream.read_buf(&mut slice).await?;
                    }
                    unsafe { payload.set_len(len) };
                    receivers_data.push(payload);

                    bytes += padded_len;
                }
                data.push(receivers_data);
            }
            Span::new("receive")
                .with_start(start)
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "exchange {} receive {} from workers {}..{}",
                        exchange.exchange_id,
                        HumanBytes::from(bytes),
                        senders.start,
                        senders.end
                    )
                })
                .record();

            tokio::spawn(async move { exchange.received(senders, data).await });
        }
        Ok(())
    }
}

struct Clients {
    runtime: WeakRuntime,

    /// Cached `runtime.layout().local_workers()`.
    local_workers: Range<usize>,

    /// Listens for connections from other hosts.
    ///
    /// We create this lazily upon the first attempt to connect to other hosts.
    /// If we create it before we've completely initialized the circuit, then we
    /// might not have created all of the exchanges yet when some other host
    /// tries to send data to one.
    listener: OnceLock<Option<ExchangeListener>>,

    /// Maps from a range of worker IDs to the RPC client used to contact those
    /// workers.  Only worker IDs for remote workers appear in the map.
    clients: Vec<(Host, TokioOnceCell<ExchangeServiceClient>)>,
}

impl Clients {
    fn new(runtime: &Runtime) -> Clients {
        Self {
            local_workers: runtime.layout().local_workers(),
            runtime: runtime.downgrade(),
            listener: Default::default(),
            clients: runtime
                .layout()
                .other_hosts()
                .map(|host| (host.clone(), TokioOnceCell::new()))
                .collect(),
        }
    }

    /// Returns a client for `worker`, which must be a remote worker ID, first
    /// establishing a connection if there isn't one yet.
    async fn connect(&self, worker: usize) -> &ExchangeServiceClient {
        self.listener.get_or_init(|| {
            if let Some(runtime) = self.runtime.upgrade()
                && let Some(local_address) = runtime.layout().local_address()
            {
                let directory = runtime.local_store().get(&DirectoryId).unwrap().clone();
                Some(ExchangeListener::new(
                    local_address,
                    directory,
                    self.local_workers.clone(),
                ))
            } else {
                None
            }
        });

        let (host, cell) = self
            .clients
            .iter()
            .find(|(host, _client)| host.workers.contains(&worker))
            .unwrap();
        cell.get_or_init(|| async {
            let stream = loop {
                match TcpStream::connect(host.address).await {
                    Ok(stream) => break stream,
                    Err(error) => println!(
                        "connection to {} failed ({error}), waiting to retry",
                        host.address
                    ),
                }
                sleep(std::time::Duration::from_millis(1000)).await;
            };
            stream.set_nodelay(true).unwrap();
            stream.set_zero_linger().unwrap();
            ExchangeServiceClient {
                receivers: self.local_workers.clone(),
                stream: tokio::sync::Mutex::new(stream),
            }
        })
        .await
    }
}

struct CallbackInner {
    cb: Option<Box<dyn Fn() + Send + Sync>>,
}

impl CallbackInner {
    fn empty() -> Self {
        Self { cb: None }
    }

    fn new<F>(cb: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        let cb = Box::new(cb) as Box<dyn Fn() + Send + Sync>;
        Self { cb: Some(cb) }
    }
}

struct Callback(AtomicPtr<CallbackInner>);

impl Callback {
    fn empty() -> Self {
        Self(AtomicPtr::new(Box::into_raw(Box::new(
            CallbackInner::empty(),
        ))))
    }

    fn set_callback(&self, cb: impl Fn() + Send + Sync + 'static) {
        let old_callback = self.0.swap(
            Box::into_raw(Box::new(CallbackInner::new(cb))),
            Ordering::AcqRel,
        );

        let old_callback = unsafe { Box::from_raw(old_callback) };
        drop(old_callback);
    }

    fn call(&self) {
        if let Some(cb) = &unsafe { &*self.0.load(Ordering::Acquire) }.cb {
            cb()
        }
    }
}

struct InnerExchange {
    exchange_id: ExchangeId,
    /// The number of communicating peers.
    npeers: usize,
    /// Range of worker IDs on the local host.
    local_workers: Range<usize>,
    /// Counts the number of messages yet to be received in the current round of
    /// communication per receiver.  The receiver must wait until it has all
    /// `npeers` messages before reading all of them from mailboxes in one
    /// pass.
    receiver_counters: Vec<AtomicUsize>,
    /// Callback invoked when all `npeers` messages are ready for a receiver.
    receiver_callbacks: Vec<Callback>,
    /// Counts the number of empty mailboxes ready to accept new data per
    /// sender. The sender waits until it has `npeers` available mailboxes
    /// before writing all of them in one pass.
    sender_counters: Vec<CachePadded<AtomicUsize>>,
    /// Callback invoked when all `npeers` mailboxes are available.
    sender_callbacks: Vec<Callback>,
    /// The number of workers that have already sent their messages in the
    /// current round.
    sent: AtomicUsize,
    /// The RPC clients to contact remote hosts.
    clients: Arc<Clients>,
    /// This allows the `exchange` RPC to wait until the receiver has taken its
    /// data out of the mailbox.  There are `n_remote_workers * n_local_workers`
    /// elements.
    sender_notifies: Vec<Notify>,
    /// A callback that takes the raw data exchanged over RPC and deserializes
    /// and delivers it to the receiver's mailbox.
    deliver: Box<dyn Fn(AlignedVec, usize, usize) + Send + Sync + 'static>,
}

impl InnerExchange {
    fn new(
        exchange_id: ExchangeId,
        deliver: impl Fn(AlignedVec, usize, usize) + Send + Sync + 'static,
        clients: Arc<Clients>,
    ) -> InnerExchange {
        let runtime = Runtime::runtime().unwrap();
        let npeers = Runtime::num_workers();
        let local_workers = runtime.layout().local_workers();
        let n_local_workers = local_workers.len();
        let n_remote_workers = npeers - n_local_workers;
        Self {
            exchange_id,
            npeers,
            local_workers,
            clients,
            receiver_counters: (0..npeers).map(|_| AtomicUsize::new(0)).collect(),
            receiver_callbacks: (0..npeers).map(|_| Callback::empty()).collect(),
            sender_notifies: (0..n_local_workers * n_remote_workers)
                .map(|_| Notify::new())
                .collect(),
            sender_counters: (0..npeers)
                .map(|_| CachePadded::new(AtomicUsize::new(npeers)))
                .collect(),
            sender_callbacks: (0..npeers).map(|_| Callback::empty()).collect(),
            deliver: Box::new(deliver),
            sent: AtomicUsize::new(0),
        }
    }

    #[allow(dead_code)]
    fn exchange_id(&self) -> ExchangeId {
        self.exchange_id
    }

    /// Returns the `sender_notify` for a sender/receiver pair.  `receiver`
    /// must be a local worker ID, and `sender` must be a remote worker ID.
    fn sender_notify(&self, sender: usize, receiver: usize) -> &Notify {
        debug_assert!(sender < self.npeers && !self.local_workers.contains(&sender));
        debug_assert!(self.local_workers.contains(&receiver));
        let n_local_workers = self.local_workers.len();
        let sender_ofs = if sender >= self.local_workers.start {
            sender - n_local_workers
        } else {
            sender
        };
        let receiver_ofs = receiver - self.local_workers.start;
        &self.sender_notifies[sender_ofs * n_local_workers + receiver_ofs]
    }

    /// Receives messages sent from all of the worker threads in `senders` to
    /// all of the local worker threads `receivers` in `self`.  The
    /// Bincode-encoded `Vec<u8>` message from `sender` to `receiver` is
    /// `data[sender - senders.start][receiver - receivers.start]`.
    async fn received(self: &Arc<Self>, senders: Range<usize>, data: Vec<Vec<AlignedVec>>) {
        let _span = Span::new("deliver")
            .with_category("Exchange")
            .with_tooltip(|| {
                format!(
                    "exchange {} deliver from workers {}..{}",
                    self.exchange_id, senders.start, senders.end
                )
            });
        let receivers = &self.local_workers;

        // Deliver all of the data into the exchange's mailboxes.
        for (sender, data) in senders.clone().zip(data.into_iter()) {
            assert_eq!(data.len(), receivers.len());
            for (receiver, data) in receivers.clone().zip(data.into_iter()) {
                (self.deliver)(data, sender, receiver);
            }
        }

        // Increment the receiver counters and deliver callbacks if necessary.
        for receiver in receivers.clone() {
            let n = senders.len();
            let old_counter = self.receiver_counters[receiver].fetch_add(n, Ordering::AcqRel);
            if old_counter >= self.npeers - n {
                self.receiver_callbacks[receiver].call();
            }
        }

        // Wait for the receivers to pick up their mail before returning.
        for sender in senders {
            for receiver in receivers.clone() {
                self.sender_notify(sender, receiver).notified().await;
            }
        }
    }

    /// Returns an index for the sender/receiver pair.
    fn mailbox_index(&self, sender: usize, receiver: usize) -> usize {
        debug_assert!(sender < self.npeers);
        debug_assert!(receiver < self.npeers);
        sender * self.npeers + receiver
    }

    fn ready_to_send(&self, sender: usize) -> bool {
        debug_assert!(self.local_workers.contains(&sender));
        self.sender_counters[sender].load(Ordering::Acquire) == self.npeers
    }

    fn ready_to_receive(&self, receiver: usize) -> bool {
        debug_assert!(receiver < self.npeers);
        self.receiver_counters[receiver].load(Ordering::Acquire) == self.npeers
    }

    fn register_sender_callback<F>(&self, sender: usize, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        debug_assert!(sender < self.npeers);
        self.sender_callbacks[sender].set_callback(cb);
    }

    fn register_receiver_callback<F>(&self, receiver: usize, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        debug_assert!(receiver < self.npeers);

        self.receiver_callbacks[receiver].set_callback(cb);
    }
}

#[derive(Clone, Debug)]
pub enum Mailbox<T> {
    Tx(FBuf),
    Rx(AlignedVec),
    Plain(T),
}

impl<T> Mailbox<T> {
    fn into_tx(self) -> Option<FBuf> {
        match self {
            Mailbox::Tx(bytes) => Some(bytes),
            Mailbox::Rx(_) | Mailbox::Plain(_) => None,
        }
    }
}

/// `Exchange` is an N-to-N communication primitive that partitions data across
/// multiple concurrent threads.
///
/// An instance of `Exchange` can be shared by multiple threads that communicate
/// in rounds.  In each round each peer _first_ sends exactly one data value to
/// every other peer (and itself) and then receives one value from each peer.
/// The send operation can only proceed when all peers have retrieved data
/// produced at the previous round.  Likewise, the receive operation can proceed
/// once all incoming values are ready for the current round.
///
/// Each worker has one ExchangeServiceClient and ExchangeServer for every
/// worker (including itself), so N*N total.
///
/// In a round, each worker invokes exchange() once on each of its clients.
/// Each server handles N calls to exchange(), once for each other worker and
/// itself.
///
/// Each call to exchange populates a mailbox.  When all the mailboxes for a
/// worker have been populated, it can read and clear them.
pub(crate) struct Exchange<T> {
    inner: Arc<InnerExchange>,
    /// `npeers^2` mailboxes, one for each sender/receiver pair.  Each mailbox
    /// is accessed by exactly two threads, so contention is low.
    ///
    /// We only use the mailboxes where either the sender or the receiver is one
    /// of our local workers. In the diagram below, L is mailboxes used for
    /// local exchange, S mailboxes used for sending RPC exchange, and R
    /// mailboxes used for receiving exchange via RPC:
    ///
    /// ```text
    ///           <-------receivers------->
    ///                  local
    ///                 workers
    /// ^         -------------------------
    /// |         |     |RRRRR|     |     |
    ///           |     |RRRRR|     |     |
    /// s         |-----|-----|-----|-----|
    /// e  local  |SSSSS|LLLLL|SSSSS|SSSSS|
    /// n workers |SSSSS|LLLLL|SSSSS|SSSSS|
    /// d         |-----|-----|-----|-----|
    /// e         |     |RRRRR|     |     |
    /// r         |     |RRRRR|     |     |
    /// s         |-----|-----|-----|-----|
    ///           |     |RRRRR|     |     |
    /// |         |     |RRRRR|     |     |
    /// v         |-----|-----|-----|-----|
    /// ```
    mailboxes: Arc<Vec<Mutex<Option<Mailbox<T>>>>>,
    deserialize: Box<dyn Fn(AlignedVec) -> T + Send + Sync>,

    /// The amount of time spent calling `deserialize`.
    deserialization_usecs: AtomicU64,

    /// The number of bytes passed to `deserialize`.
    deserialized_bytes: AtomicUsize,
}

// Stop Rust from complaining about unused field.
#[allow(dead_code)]
struct ExchangeListener(DropGuard);

impl ExchangeListener {
    fn new(address: SocketAddr, directory: ExchangeDirectory, receivers: Range<usize>) -> Self {
        let token = CancellationToken::new();
        let drop = token.clone().drop_guard();
        TOKIO.spawn(async move {
            println!("listening on {address}");
            let listener = TcpListener::bind(address).await.unwrap();
            while let Some(stream) = tokio::select! {
                stream = listener.accept() => Some(stream),
                _ = token.cancelled() => None,
            } {
                match stream {
                    Ok((stream, _address)) => {
                        tokio::spawn(
                            ExchangeServer {
                                receivers: receivers.clone(),
                                directory: directory.clone(),
                                stream,
                            }
                            .serve(),
                        );
                    }
                    Err(error) => warn!("Error accepting connection from {address}: {error}"),
                }
            }
        });
        Self(drop)
    }
}

impl<T> Exchange<T>
where
    T: Clone + Send + 'static,
{
    /// Create a new exchange operator for `npeers` communicating threads.
    fn new(
        exchange_id: ExchangeId,
        clients: Arc<Clients>,
        directory: ExchangeDirectory,
        deserialize: Box<dyn Fn(AlignedVec) -> T + Send + Sync>,
    ) -> Self {
        let npeers = Runtime::num_workers();
        let mailboxes: Arc<Vec<Mutex<Option<Mailbox<T>>>>> =
            Arc::new((0..npeers * npeers).map(|_| Mutex::new(None)).collect());
        let mailboxes2 = mailboxes.clone();
        let deliver = move |data, sender, receiver| {
            let index: usize = sender * npeers + receiver;
            let mut mailbox = mailboxes2[index].lock().unwrap();
            assert!((*mailbox).is_none());
            *mailbox = Some(Mailbox::Rx(data));
        };

        let inner = Arc::new(InnerExchange::new(exchange_id, deliver, clients));
        directory
            .write()
            .unwrap()
            .entry(exchange_id)
            .and_modify(|_| panic!())
            .or_insert(inner.clone());
        Self {
            inner,
            mailboxes,
            deserialize,
            deserialization_usecs: AtomicU64::new(0),
            deserialized_bytes: AtomicUsize::new(0),
        }
    }

    #[allow(dead_code)]
    fn exchange_id(&self) -> ExchangeId {
        self.inner.exchange_id()
    }

    /// Returns a reference to a mailbox for the sender/receiver pair.
    fn mailbox(&self, sender: usize, receiver: usize) -> &Mutex<Option<Mailbox<T>>> {
        &self.mailboxes[self.inner.mailbox_index(sender, receiver)]
    }

    /// Create a new `Exchange` instance if an instance with the same id
    /// (created by another thread) does not yet exist within `runtime`.
    /// The number of peers will be set to `runtime.num_workers()`.
    pub(crate) fn with_runtime(
        runtime: &Runtime,
        exchange_id: ExchangeId,
        deserialize: Box<dyn Fn(AlignedVec) -> T + Send + Sync>,
    ) -> Arc<Self> {
        let directory = runtime
            .local_store()
            .entry(DirectoryId)
            .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
            .clone();

        let clients = runtime
            .local_store()
            .entry(ClientsId)
            .or_insert_with(|| {
                // Create clients for remote exchange.
                Arc::new(Clients::new(runtime))
            })
            .clone();

        runtime
            .local_store()
            .entry(ExchangeCacheId::new(exchange_id))
            .or_insert_with(|| {
                Arc::new(Exchange::new(
                    exchange_id,
                    clients.clone(),
                    directory,
                    deserialize,
                ))
            })
            .value()
            .clone()
    }

    /// True if all `sender`'s outgoing mailboxes are free and ready to accept
    /// data.
    ///
    /// Once this function returns true, a subsequent `try_send_all` operation
    /// is guaranteed to succeed for `sender`.
    pub fn ready_to_send(&self, sender: usize) -> bool {
        self.inner.ready_to_send(sender)
    }

    pub(crate) fn try_send_all_with_serializer<F>(
        self: &Arc<Self>,
        sender: usize,
        data: impl Iterator<Item = T>,
        mut serialize: F,
    ) -> bool
    where
        F: FnMut(T) -> FBuf + Send + Sync,
    {
        self.try_send_all(
            sender,
            data.zip(WorkerLocations::new())
                .map(|(data, location)| match location {
                    WorkerLocation::Local => Mailbox::Plain(data),
                    WorkerLocation::Remote => Mailbox::Tx(serialize(data)),
                }),
        )
    }

    /// Write all outgoing messages for `sender` to mailboxes.
    ///
    /// Values to be sent are retrieved from the `data` iterator, with the
    /// first value delivered to receiver 0, second value delivered to receiver
    /// 1, and so on.
    ///
    /// # Errors
    ///
    /// Fails if at least one of the sender's outgoing mailboxes is not empty.
    ///
    /// # Panics
    ///
    /// Panics if `data` yields fewer than `self.npeers` items.
    pub(crate) fn try_send_all(
        self: &Arc<Self>,
        sender: usize,
        data: impl Iterator<Item = Mailbox<T>>,
    ) -> bool {
        let npeers = self.inner.npeers;
        if self.inner.sender_counters[sender]
            .compare_exchange(npeers, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        // Deliver all of the data to local mailboxes.
        let local_workers = &self.inner.local_workers;
        for (receiver, item) in (0..npeers).zip_eq(data.take(npeers)) {
            let is_local = local_workers.contains(&receiver);
            let mailbox_is_local = matches!(&item, Mailbox::Plain(_));
            assert_eq!(is_local, mailbox_is_local);
            *self.mailbox(sender, receiver).lock().unwrap() = Some(item);

            if is_local {
                let old_counter =
                    self.inner.receiver_counters[receiver].fetch_add(1, Ordering::AcqRel);
                if old_counter >= npeers - 1 {
                    self.inner.receiver_callbacks[receiver].call();
                }
            }
        }

        // In a single-host layout, or if some of our local workers haven't yet
        // sent in this round, we're all done for now.
        if npeers == local_workers.len()
            || self.inner.sent.fetch_add(1, Ordering::AcqRel) + 1 != local_workers.len()
        {
            return true;
        }
        self.inner.sent.store(0, Ordering::Release);

        // All of the local workers have sent their data in this round.  Take
        // all of their data and send it to the remote hosts.
        let this = self.clone();
        let runtime = Runtime::runtime().unwrap();
        TOKIO.spawn(async move {
            let mut futures = FuturesUnordered::new();

            // For each range of worker IDs `receivers` on a remote host,
            // accumulate all of the data from our local `senders` to all
            // of the `receivers` on that host.
            let senders = &this.inner.local_workers;
            for host in runtime.layout().other_hosts() {
                let receivers = &host.workers;
                let mut serialized_bytes = 0;
                let items: Vec<Vec<FBuf>> = senders
                    .clone()
                    .map(|sender| {
                        receivers
                            .clone()
                            .map(|receiver| {
                                let serialized = this
                                    .mailbox(sender, receiver)
                                    .lock()
                                    .unwrap()
                                    .take()
                                    .unwrap()
                                    .into_tx()
                                    .expect("remote mailboxes should always be serialized");
                                serialized_bytes += serialized.len();
                                serialized
                            })
                            .collect()
                    })
                    .collect();

                let client = this.inner.clients.connect(receivers.start).await;

                // Send it.
                futures.push(client.exchange(this.inner.exchange_id, senders.clone(), items));
            }

            // Wait for all the sends to complete.
            while let Some(result) = futures.next().await {
                result.unwrap();
            }

            // Record that the sends completed.
            let n = npeers - senders.len();
            for sender in senders.clone() {
                let old_counter = this.inner.sender_counters[sender].fetch_add(n, Ordering::AcqRel);
                if old_counter >= npeers - n {
                    this.inner.sender_callbacks[sender].call();
                }
            }
        });

        true
    }

    /// True if all `receiver`'s incoming mailboxes contain data.
    ///
    /// Once this function returns true, a subsequent `try_receive_all`
    /// operation is guaranteed for `receiver`.
    pub(crate) fn ready_to_receive(&self, receiver: usize) -> bool {
        self.inner.ready_to_receive(receiver)
    }

    /// Read all incoming messages for `receiver`.
    ///
    /// Values are passed to callback function `cb` in the order of worker indexes.
    ///
    /// # Errors
    ///
    /// Fails if at least one of the receiver's incoming mailboxes is empty.
    pub(crate) fn try_receive_all<F>(&self, receiver: usize, mut cb: F) -> bool
    where
        F: FnMut(T),
    {
        let npeers = self.inner.npeers;
        if self.inner.receiver_counters[receiver]
            .compare_exchange(npeers, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }

        let start = Instant::now();
        let mut deserialized_bytes = 0;
        let data = (0..self.inner.npeers)
            .map(|sender| {
                let mailbox = self
                    .mailbox(sender, receiver)
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap();
                match mailbox {
                    Mailbox::Plain(item) => item,
                    Mailbox::Tx(_) => unreachable!(),
                    Mailbox::Rx(bytes) => {
                        deserialized_bytes += bytes.len();

                        (self.deserialize)(bytes)
                    }
                }
            })
            .collect_vec();
        self.deserialized_bytes
            .fetch_add(deserialized_bytes, Ordering::Relaxed);
        self.deserialization_usecs
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        Span::new("deserialize")
            .with_category("Exchange")
            .with_start(start)
            .with_tooltip(|| {
                format!(
                    "exchange {} deserialize {}",
                    self.exchange_id(),
                    HumanBytes::from(deserialized_bytes)
                )
            })
            .record();

        for (sender, data) in data.into_iter().enumerate() {
            cb(data);
            if self.inner.local_workers.contains(&sender) {
                let old_counter = self.inner.sender_counters[sender].fetch_add(1, Ordering::AcqRel);
                if old_counter >= self.inner.npeers - 1 {
                    self.inner.sender_callbacks[sender].call();
                }
            } else {
                self.inner.sender_notify(sender, receiver).notify_one();
            }
        }
        true
    }

    /// Register callback to be invoked whenever the `ready_to_send` condition
    /// becomes true.
    ///
    /// The callback can be setup at most once (e.g., when a scheduler attaches
    /// to the circuit) and cannot be unregistered.  Notifications delivered
    /// before the callback is registered are lost.  The client should call
    /// `ready_to_send` after installing the callback to check the status.
    ///
    /// After the callback has been registered, notifications are delivered with
    /// at-least-once semantics: a notification is generated whenever the
    /// status changes from not ready to ready, but spurious notifications
    /// can occur occasionally.  Therefore, the user must check the status
    /// explicitly by calling `ready_to_send` or be prepared that `try_send_all`
    /// can fail.
    pub(crate) fn register_sender_callback<F>(&self, sender: usize, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner.register_sender_callback(sender, cb)
    }

    /// Register callback to be invoked whenever the `ready_to_receive`
    /// condition becomes true.
    ///
    /// The callback can be setup at most once (e.g., when a scheduler attaches
    /// to the circuit) and cannot be unregistered.  Notifications delivered
    /// before the callback is registered are lost.  The client should call
    /// `ready_to_receive` after installing the callback to check
    /// the status.
    ///
    /// After the callback has been registered, notifications are delivered with
    /// at-least-once semantics: a notification is generated whenever the
    /// status changes from not ready to ready, but spurious notifications
    /// can occur occasionally.  The user must check the status explicitly
    /// by calling `ready_to_receive` or be prepared that `try_receive_all`
    /// can fail.
    pub(crate) fn register_receiver_callback<F>(&self, receiver: usize, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.inner.register_receiver_callback(receiver, cb)
    }
}

/// Operator that partitions incoming data across all workers.
///
/// This operator works in tandem with [`ExchangeReceiver`], which reassembles
/// the data on the receiving side.  Together they implement an all-to-all
/// communication mechanism, where at every clock cycle each worker partitions
/// its incoming data into `N` values, one for each worker, using a
/// user-provided closure.  It then reads values sent to it by all peers and
/// reassembles them into a single value using another user-provided closure.
///
/// The exchange mechanism is split into two operators, so that after sending
/// the data the circuit does not need to block waiting for its peers to finish
/// sending and can instead schedule other operators.
///
/// ```text
///                    ExchangeSender  ExchangeReceiver
///                       ┌───────┐      ┌───────┐
///                       │       │      │       │
///        ┌───────┐      │       │      │       │          ┌───────┐
///        │source ├─────►│       │      │       ├─────────►│ sink  │
///        └───────┘      │       │      │       │          └───────┘
///                       │       ├───┬─►│       │
///                       │       │   │  │       │
///                       └───────┘   │  └───────┘
/// WORKER 1                          │
/// ──────────────────────────────────┼──────────────────────────────
/// WORKER 2                          │
///                                   │
///                       ┌───────┐   │  ┌───────┐
///                       │       ├───┴─►│       │
///        ┌───────┐      │       │      │       │          ┌───────┐
///        │source ├─────►│       │      │       ├─────────►│ sink  │
///        └───────┘      │       │      │       │          └───────┘
///                       │       │      │       │
///                       │       │      │       │
///                       └───────┘      └───────┘
///                    ExchangeSender  ExchangeReceiver
/// ```
///
/// `ExchangeSender` is an asynchronous operator., i.e.,
/// [`ExchangeSender::is_async`] returns `true`.  It becomes schedulable
/// ([`ExchangeSender::ready`] returns `true`) once all peers have retrieved
/// values written by the operator in the previous clock cycle.  The scheduler
/// should use [`ExchangeSender::register_ready_callback`] to get notified when
/// the operator becomes schedulable.
///
/// `ExchangeSender` doesn't have a public constructor and must be instantiated
/// using the [`new_exchange_operators`] function, which creates an
/// [`ExchangeSender`]/[`ExchangeReceiver`] pair of operators and connects them
/// to their counterparts in other workers as in the diagram above.
///
/// An [`ExchangeSender`]/[`ExchangeReceiver`] pair is added to a circuit using
/// the [`Circuit::add_exchange`](`crate::circuit::Circuit::add_exchange`)
/// method, which registers a dependency between them, making sure that
/// `ExchangeSender` is evaluated before `ExchangeReceiver`.
///
/// # Examples
///
/// The following example instantiates the circuit in the diagram above.
///
/// ```
/// # #[cfg(miri)]
/// # fn main() {}
///
/// # #[cfg(not(miri))]
/// # fn main() {
/// use dbsp::{
///     operator::{communication::new_exchange_operators, Generator},
///     circuit::{WorkerLocation, WorkerLocations},
///     operator::communication::Mailbox,
///     Circuit, RootCircuit, Runtime,
///     storage::file::to_bytes_dyn,
///     trace::aligned_deserialize,
/// };
///
/// const WORKERS: usize = 16;
/// const ROUNDS: usize = 10;
///
/// let hruntime = Runtime::run(WORKERS, |_parker| {
///     let circuit = RootCircuit::build(|circuit| {
///         // Create a data source that generates numbers 0, 1, 2, ...
///         let mut n: usize = 0;
///         let source = circuit.add_source(Generator::new(move || {
///             let result = n;
///             n += 1;
///             result
///         }));
///
///         // Create an `ExchangeSender`/`ExchangeReceiver pair`.
///         let (sender, receiver) = new_exchange_operators(
///             None,
///             Vec::new,
///             // Partitioning function sends a copy of the input `n` to each peer.
///             move |n, vals| {
///                 for location in WorkerLocations::new() {
///                     match location {
///                         WorkerLocation::Local => vals.push(Mailbox::Plain(n)),
///                         WorkerLocation::Remote => {
///                             vals.push(Mailbox::Tx(to_bytes_dyn(&n).unwrap()))
///                         }
///                     }
///                 }
///             },
///             |data| aligned_deserialize(&data[..]),///             // Reassemble received values into a vector.
///             |v: &mut Vec<usize>, n| v.push(n),
///         ).unwrap();
///
///         // Add exchange operators to the circuit.
///         let combined = circuit.add_exchange(sender, receiver, &source);
///         let mut round = 0;
///
///         // Expected output stream of`ExchangeReceiver`:
///         // [0,0,0,...]
///         // [1,1,1,...]
///         // [2,2,2,...]
///         // ...
///         combined.inspect(move |v| {
///             assert_eq!(&vec![round; WORKERS], v);
///             round += 1;
///         });
///         Ok(())
///     })
///     .unwrap()
///     .0;
///
///     for _ in 1..ROUNDS {
///         circuit.step();
///     }
/// }).expect("failed to start runtime");
///
/// hruntime.join().unwrap();
/// # }
/// ```
pub struct ExchangeSender<D, T, L>
where
    T: Send + 'static + Clone,
{
    worker_index: usize,
    location: OperatorLocation,
    partition: L,
    outputs: Vec<Mailbox<T>>,
    exchange: Arc<Exchange<(T, bool)>>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flushed: bool,

    // The instant when the sender produced its outputs, and the
    // receiver starts waiting for all other workers to produce their
    // outputs.
    start_wait_usecs: Arc<AtomicU64>,

    phantom: PhantomData<D>,
}

impl<D, T, L> ExchangeSender<D, T, L>
where
    T: Send + 'static + Clone,
{
    fn new(
        worker_index: usize,
        location: OperatorLocation,
        exchange: Arc<Exchange<(T, bool)>>,
        start_wait_usecs: Arc<AtomicU64>,
        partition: L,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());
        Self {
            worker_index,
            location,
            partition,
            outputs: Vec::with_capacity(Runtime::num_workers()),
            exchange,
            input_batch_stats: BatchSizeStats::new(),
            flushed: false,
            start_wait_usecs,
            phantom: PhantomData,
        }
    }
}

impl<D, T, L> Operator for ExchangeSender<D, T, L>
where
    D: 'static,
    T: Send + 'static + Clone,
    L: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ExchangeSender")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
        });
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}

    fn is_async(&self) -> bool {
        true
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.exchange
            .register_sender_callback(self.worker_index, cb)
    }

    fn ready(&self) -> bool {
        self.exchange.ready_to_send(self.worker_index)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flushed = true;
    }
}

impl<D, T, L> SinkOperator<D> for ExchangeSender<D, T, L>
where
    D: Clone + NumEntries + 'static,
    T: Clone + Send + 'static,
    L: FnMut(D, &mut Vec<Mailbox<T>>) + 'static,
{
    async fn eval(&mut self, input: &D) {
        self.eval_owned(input.clone()).await
    }

    async fn eval_owned(&mut self, input: D) {
        self.input_batch_stats.add_batch(input.num_entries_deep());

        debug_assert!(self.ready());
        self.outputs.clear();
        (self.partition)(input, &mut self.outputs);
        self.start_wait_usecs
            .store(current_time_usecs(), Ordering::Release);

        let res = self.exchange.try_send_all(
            self.worker_index,
            self.outputs.drain(..).map(|mailbox| match mailbox {
                Mailbox::Tx(mut data) => {
                    data.push(self.flushed as u8);
                    Mailbox::Tx(data)
                }
                Mailbox::Rx(_) => unreachable!(),
                Mailbox::Plain(item) => Mailbox::Plain((item, self.flushed)),
            }),
        );
        self.flushed = false;
        debug_assert!(res);
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

/// Operator that receives values sent by the `ExchangeSender` operator and
/// assembles them into a single output value.
///
/// The `init` closure returns the initial value for the result.  This value
/// is updated by the `combine` closure with each value received from a remote
/// peer.
///
/// See [`ExchangeSender`] documentation for details.
///
/// `ExchangeReceiver` is an asynchronous operator., i.e.,
/// [`ExchangeReceiver::is_async`] returns `true`.  It becomes schedulable
/// ([`ExchangeReceiver::ready`] returns `true`) once all peers have sent values
/// for this worker in the current clock cycle.  The scheduler should use
/// [`ExchangeReceiver::register_ready_callback`] to get notified when the
/// operator becomes schedulable.
pub struct ExchangeReceiver<IF, T, L>
where
    T: Send + 'static + Clone,
{
    worker_index: usize,
    location: OperatorLocation,
    init: IF,
    combine: L,
    exchange: Arc<Exchange<(T, bool)>>,
    flush_count: usize,
    flush_complete: bool,
    start_wait_usecs: Arc<AtomicU64>,
    total_wait_time: Arc<AtomicU64>,

    // Output batch sizes.
    output_batch_stats: BatchSizeStats,
}

impl<IF, T, L> ExchangeReceiver<IF, T, L>
where
    T: Send + 'static + Clone,
{
    pub(crate) fn new(
        worker_index: usize,
        location: OperatorLocation,
        exchange: Arc<Exchange<(T, bool)>>,
        init: IF,
        start_wait_usecs: Arc<AtomicU64>,
        combine: L,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());

        Self {
            worker_index,
            location,
            init,
            combine,
            exchange,
            flush_count: 0,
            flush_complete: false,
            output_batch_stats: BatchSizeStats::new(),
            start_wait_usecs,
            total_wait_time: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<D, T, L> Operator for ExchangeReceiver<D, T, L>
where
    D: 'static,
    T: Send + 'static + Clone,
    L: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ExchangeReceiver")
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            OUTPUT_BATCHES_STATS => self.output_batch_stats.metadata(),
            EXCHANGE_WAIT_TIME_SECONDS => MetaItem::Duration(Duration::from_micros(self.total_wait_time.load(Ordering::Acquire))),
            EXCHANGE_DESERIALIZATION_TIME_SECONDS => MetaItem::Duration(Duration::from_micros(self.exchange.deserialization_usecs.load(Ordering::Acquire))),
            EXCHANGE_DESERIALIZED_BYTES => MetaItem::bytes(self.exchange.deserialized_bytes.load(Ordering::Acquire)),
        });
    }

    fn is_async(&self) -> bool {
        true
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let start_wait_usecs = self.start_wait_usecs.clone();
        let total_wait_time = self.total_wait_time.clone();
        let exchange = self.exchange.clone();
        let worker_index = self.worker_index;

        let cb = move || {
            if exchange.ready_to_receive(worker_index) {
                // The callback can be invoked multiple times per step.
                // Reset start_wait_usecs to 0 to make sure we don't double-count.
                let start = start_wait_usecs.swap(0, Ordering::Acquire);
                if start != 0 {
                    let end = current_time_usecs();
                    if end > start {
                        let wait_time_usecs = end - start;
                        // if worker_index == 0 {
                        //     info!(
                        //         "{worker_index}: {} +{wait_time_usecs}",
                        //         exchange.exchange_id()
                        //     );
                        // }
                        total_wait_time.fetch_add(wait_time_usecs, Ordering::AcqRel);
                    }
                }
            }
            cb()
        };
        self.exchange
            .register_receiver_callback(self.worker_index, cb)
    }

    fn ready(&self) -> bool {
        self.exchange.ready_to_receive(self.worker_index)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        // println!("{} exchange_receiver::flush", Runtime::worker_index());
        self.flush_complete = false;
    }

    fn is_flush_complete(&self) -> bool {
        // println!(
        //     "{} exchange_receiver::is_flush_complete (flush_complete = {})",
        //     Runtime::worker_index(),
        //     self.flush_complete
        // );
        self.flush_complete
    }
}

impl<D, IF, T, L> SourceOperator<D> for ExchangeReceiver<IF, T, L>
where
    D: NumEntries + 'static,
    T: Clone + Send + 'static,
    IF: Fn() -> D + 'static,
    L: Fn(&mut D, T) + 'static,
{
    async fn eval(&mut self) -> D {
        debug_assert!(self.ready());
        let mut combined = (self.init)();
        let res = self
            .exchange
            .try_receive_all(self.worker_index, |(x, flushed)| {
                // println!(
                //     "{} exchange_receiver::eval received input with flushed={:?}",
                //     Runtime::worker_index(),
                //     flushed
                // );
                if flushed {
                    self.flush_count += 1;
                }
                (self.combine)(&mut combined, x)
            });
        if self.flush_count == Runtime::num_workers() {
            // println!(
            //     "{} exchange_receiver::eval received all inputs",
            //     Runtime::worker_index()
            // );

            self.flush_complete = true;
            self.flush_count = 0;
        }

        debug_assert!(res);

        self.output_batch_stats
            .add_batch(combined.num_entries_deep());
        combined
    }
}

#[derive(Hash, PartialEq, Eq)]
struct ClientsId;

impl TypedMapKey<LocalStoreMarker> for ClientsId {
    type Value = Arc<Clients>;
}

#[derive(Hash, PartialEq, Eq)]
struct DirectoryId;

impl TypedMapKey<LocalStoreMarker> for DirectoryId {
    type Value = ExchangeDirectory;
}

/// Create an [`ExchangeSender`]/[`ExchangeReceiver`] operator pair.
///
/// See [`ExchangeSender`] documentation for details and example usage.
///
/// # Arguments
///
/// * `runtime` - [`Runtime`](`crate::circuit::Runtime`) within which operators
///   are created.
/// * `worker_index` - index of the current worker.
/// * `partition` - partitioning logic that must push exactly
///   `runtime.num_workers()` values into its vector argument
/// * `serialize` - serializes exchanged data for transmission across a network
/// * `deserialize` - deserializes exchanged data that was transmitted across a network
/// * `combine` - re-assemble logic that combines values received from all peers
///   into a single output value.
///
/// # Type arguments
/// * `TI` - Type of values in the input stream consumed by `ExchangeSender`.
/// * `TO` - Type of values in the output stream produced by `ExchangeReceiver`.
/// * `TE` - Type of values sent across workers.
/// * `PL` - Type of closure that splits a value of type `TI` into
///   `runtime.num_workers()` values of type `TE`.
/// * `I` - Iterator returned by `PL`.
/// * `IF` - Type of closure used to initialize the output value of type `TO`.
/// * `CL` - Type of closure that folds `num_workers` values of type `TE` into a
///   value of type `TO`.
pub fn new_exchange_operators<TI, TO, TE, IF, PL, CL, D>(
    location: OperatorLocation,
    init: IF,
    partition: PL,
    deserialize: D,
    combine: CL,
) -> Option<(ExchangeSender<TI, TE, PL>, ExchangeReceiver<IF, TE, CL>)>
where
    TO: Clone,
    TE: Send + 'static + Clone,
    IF: Fn() -> TO + 'static,
    PL: FnMut(TI, &mut Vec<Mailbox<TE>>) + 'static,
    D: Fn(AlignedVec) -> TE + Send + Sync + 'static,
    CL: Fn(&mut TO, TE) + 'static,
{
    if Runtime::num_workers() == 1 {
        return None;
    }
    let runtime = Runtime::runtime().unwrap();
    let worker_index = Runtime::worker_index();

    let exchange_id = runtime.sequence_next().try_into().unwrap();
    let start_wait_usecs = Arc::new(AtomicU64::new(0));
    let exchange = Exchange::with_runtime(
        &runtime,
        exchange_id,
        Box::new(move |mut vec| {
            let flush = match vec.pop().unwrap() {
                0 => false,
                1 => true,
                _ => unreachable!(),
            };
            (deserialize(vec), flush)
        }),
    );
    let sender = ExchangeSender::new(
        worker_index,
        location,
        exchange.clone(),
        start_wait_usecs.clone(),
        partition,
    );
    let receiver = ExchangeReceiver::new(
        worker_index,
        location,
        exchange,
        init,
        start_wait_usecs,
        combine,
    );
    Some((sender, receiver))
}

#[cfg(test)]
mod tests {
    use super::Exchange;
    use crate::{
        Circuit, RootCircuit,
        circuit::{
            Runtime,
            runtime::{WorkerLocation, WorkerLocations},
            schedule::{DynamicScheduler, Scheduler},
        },
        operator::{
            Generator,
            communication::{Mailbox, new_exchange_operators},
        },
        storage::file::{to_bytes, to_bytes_dyn},
        trace::aligned_deserialize,
    };
    use std::{iter::repeat, thread::yield_now};

    // We decrease the number of rounds we do when we're running under miri,
    // otherwise it'll run forever
    const ROUNDS: usize = if cfg!(miri) { 128 } else { 2048 };

    // Create an exchange object with `WORKERS` concurrent senders/receivers.
    // Iterate for `ROUNDS` rounds with each sender sending value `N` to each
    // receiver in round number `N`.  Both senders and receivers may retry
    // sending/receiving multiple times, but in the end each receiver should get
    // all values in correct order.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_exchange() {
        const WORKERS: usize = 16;

        let hruntime = Runtime::run(WORKERS, |_parker| {
            let exchange = Exchange::with_runtime(
                &Runtime::runtime().unwrap(),
                0,
                Box::new(|data| aligned_deserialize(&data[..])),
            );

            for round in 0..ROUNDS {
                let output_data = vec![round; WORKERS];
                loop {
                    if exchange.try_send_all_with_serializer(
                        Runtime::worker_index(),
                        repeat(round),
                        |round| to_bytes(&round).unwrap(),
                    ) {
                        break;
                    }

                    yield_now();
                }

                let mut input_data = Vec::with_capacity(WORKERS);
                loop {
                    if exchange.try_receive_all(Runtime::worker_index(), |x| input_data.push(x)) {
                        break;
                    }

                    yield_now();
                }

                assert_eq!(input_data, output_data);
            }
        })
        .expect("failed to start runtime");

        hruntime.join().unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_exchange_operators_dynamic() {
        test_exchange_operators::<DynamicScheduler>();
    }

    // Create a circuit with `WORKERS` concurrent workers with the following
    // structure: `Generator - ExchangeSender -> ExchangeReceiver -> Inspect`.
    // `Generator` - yields sequential numbers 0, 1, 2, ...
    // `ExchangeSender` - sends each number to all peers.
    // `ExchangeReceiver` - combines all received numbers in a vector.
    // `Inspect` - validates the output of the receiver.
    fn test_exchange_operators<S>()
    where
        S: Scheduler + 'static,
    {
        fn do_test<S>(workers: usize)
        where
            S: Scheduler + 'static,
        {
            let hruntime = Runtime::run(workers, move |_parker| {
                let circuit = RootCircuit::build_with_scheduler::<_, _, S>(move |circuit| {
                    let mut n: usize = 0;
                    let source = circuit.add_source(Generator::new(move || {
                        let result = n;
                        n += 1;
                        result
                    }));

                    let (sender, receiver) = new_exchange_operators(
                        None,
                        Vec::new,
                        move |n, vals| {
                            for location in WorkerLocations::new() {
                                match location {
                                    WorkerLocation::Local => vals.push(Mailbox::Plain(n)),
                                    WorkerLocation::Remote => {
                                        vals.push(Mailbox::Tx(to_bytes_dyn(&n).unwrap()))
                                    }
                                }
                            }
                        },
                        |data| aligned_deserialize(&data[..]),
                        |v: &mut Vec<usize>, n| v.push(n),
                    )
                    .unwrap();

                    let mut round = 0;
                    circuit
                        .add_exchange(sender, receiver, &source)
                        .inspect(move |v| {
                            assert_eq!(&vec![round; workers], v);
                            round += 1;
                        });
                    Ok(())
                })
                .unwrap()
                .0;

                for _ in 1..ROUNDS {
                    circuit.transaction().unwrap();
                }
            })
            .expect("failed to start runtime");

            hruntime.join().unwrap();
        }

        do_test::<S>(2);
        do_test::<S>(16);
        do_test::<S>(32);
    }
}
