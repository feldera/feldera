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
use itertools::Itertools;
use rkyv::AlignedVec;
use size_of::HumanBytes;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    io::{Cursor, ErrorKind, IoSlice},
    iter::zip,
    marker::PhantomData,
    mem::MaybeUninit,
    net::SocketAddr,
    ops::Range,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicIsize, AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, OnceCell, mpsc::error::SendError},
    time::sleep,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{info, warn};
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
#[binrw::binrw]
#[brw(little)]
struct ExchangeHeader {
    /// The unique identifier for the exchange.
    exchange_id: ExchangeId,
    /// The sending worker.
    sender: u32,
    /// The receiving worker.
    ///
    /// For a given exchange, when a given sender sends data to another host, it
    /// sends the data for all the receivers together in sequential blocks, in
    /// order.  Thus, this field counts up sequentially across the workers on
    /// the destination.
    receiver: u32,
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

/// A multi-producer, single-consumer channel sender for [ExchangeMessage], with
/// the capacity of the channel limited by the number of bytes of messages.
struct ByteBoundedSender {
    tx: tokio::sync::mpsc::UnboundedSender<ExchangeMessage>,
    bound: Arc<ByteBound>,
}

impl ByteBoundedSender {
    /// Sends a message and blocks until there is room in the channel.  Returns
    /// an error if there is no receiver left.
    ///
    /// This simple implementation isn't going to work well if there are lots of
    /// senders running in parallel, since it doesn't prevent starvation.
    pub async fn send(&self, message: ExchangeMessage) -> Result<(), SendError<ExchangeMessage>> {
        let len = message.data.len().try_into().unwrap();
        self.tx.send(message)?;
        let remaining = self.bound.remaining.fetch_sub(len, Ordering::AcqRel) - len;
        if remaining < 0 {
            loop {
                let notified = self.bound.notify.notified();
                if self.bound.remaining.load(Ordering::Acquire) >= 0 {
                    break;
                }
                notified.await;
            }
        }
        Ok(())
    }
}

/// A multi-producer, single-consumer channel receiver for [ExchangeMessage],
/// with the capacity of the channel limited by the number of bytes of messages.
struct ByteBoundedReceiver {
    rx: tokio::sync::mpsc::UnboundedReceiver<ExchangeMessage>,
    bound: Arc<ByteBound>,
}

impl ByteBoundedReceiver {
    /// Receives a message, or returns `None` if no senders are left.
    pub async fn recv(&mut self) -> Option<ExchangeMessage> {
        let message = self.rx.recv().await?;
        let len = message.data.len().try_into().unwrap();
        let before = self.bound.remaining.fetch_add(len, Ordering::AcqRel);
        let after = before + len;
        if before < 0 && after >= 0 {
            self.bound.notify.notify_waiters();
        }
        Some(message)
    }
}

struct ByteBound {
    remaining: AtomicIsize,
    notify: Notify,
}

/// Returns a pair of multi-producer, single-consumer channel sender and
/// receiver for [ExchangeMessage], with the capacity of the channel limited by
/// the number of bytes of messages.
fn byte_bounded_channel(limit: usize) -> (ByteBoundedSender, ByteBoundedReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let bound = Arc::new(ByteBound {
        remaining: AtomicIsize::new(isize::try_from(limit).unwrap_or(isize::MAX)),
        notify: Notify::new(),
    });
    (
        ByteBoundedSender {
            tx,
            bound: bound.clone(),
        },
        ByteBoundedReceiver { rx, bound },
    )
}

struct ExchangeMessage {
    exchange_id: ExchangeId,
    sender: usize,
    data: Vec<FBuf>,
}

pub struct ExchangeClient {
    tx: ByteBoundedSender,
}

impl ExchangeClient {
    async fn new(remote_address: SocketAddr, remote_workers: &Range<usize>) -> Self {
        let (tx, rx) = byte_bounded_channel(10_000_000);
        TOKIO.spawn(Self::run(remote_address, remote_workers.clone(), rx));
        Self { tx }
    }

    async fn run(
        remote_address: SocketAddr,
        remote_workers: Range<usize>,
        mut rx: ByteBoundedReceiver,
    ) {
        let mut connection = loop {
            match TcpStream::connect(remote_address).await {
                Ok(stream) => break stream,
                Err(error) => {
                    info!("connection to {remote_address} failed ({error}), waiting to retry")
                }
            }
            sleep(std::time::Duration::from_millis(1000)).await;
        };
        connection.set_nodelay(true).unwrap();
        connection.set_zero_linger().unwrap();

        while let Some(message) = rx.recv().await {
            // We want to write each data buffer preceded by a header and followed
            // by padding.  To minimize the system calls required to do this, we
            // assemble them into one big collection of IoSlices.  First, create the
            // headers.
            let n = remote_workers.len();
            let mut headers = Vec::with_capacity(n);
            for (data, receiver) in zip(&message.data, remote_workers.clone()) {
                headers.push(
                    ExchangeHeader {
                        exchange_id: message.exchange_id,
                        sender: message.sender as u32,
                        receiver: receiver as u32,
                        data_len: data.len().try_into().unwrap(),
                    }
                    .to_bytes(),
                );
            }

            let zeros = [0; 16];

            // Now that we've got the headers, assemble the IoSlices.
            let mut slices = Vec::with_capacity(n * 3);
            let mut header = headers.iter();
            for data in &message.data {
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
            let _span = Span::new("send")
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "send {} for exchange {}",
                        HumanBytes::from(size),
                        message.exchange_id
                    )
                });
            while !bufs.is_empty() {
                let n = connection
                    .write_vectored(bufs)
                    .await
                    .expect("lost connection to remote host");
                IoSlice::advance_slices(&mut bufs, n);
            }
        }
    }

    pub async fn send(&self, exchange_id: ExchangeId, sender: usize, data: Vec<FBuf>) {
        self.tx
            .send(ExchangeMessage {
                exchange_id,
                sender,
                data,
            })
            .await
            .expect("remote exchange failed");
    }
}

pub type ExchangeId = u32;

pub trait ExchangeDelivery {
    fn received<'a>(
        &'a self,
        sender: usize,
        data: Vec<AlignedVec>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

// Maps from an `exchange_id` to an object for delivering to the exchange.
#[derive(Clone, Default)]
pub struct ExchangeDirectory(
    Arc<RwLock<HashMap<ExchangeId, Arc<dyn ExchangeDelivery + Send + Sync>>>>,
);

impl ExchangeDirectory {
    pub fn for_runtime(runtime: &Runtime) -> Self {
        runtime
            .local_store()
            .entry(DirectoryId)
            .or_insert_with(|| Self(Arc::new(RwLock::new(HashMap::new()))))
            .clone()
    }

    pub fn get(&self, exchange_id: ExchangeId) -> Option<Arc<dyn ExchangeDelivery + Send + Sync>> {
        self.0.read().unwrap().get(&exchange_id).cloned()
    }

    pub fn insert(
        &self,
        exchange_id: ExchangeId,
        exchange: Arc<dyn ExchangeDelivery + Send + Sync>,
    ) {
        self.0
            .write()
            .unwrap()
            .entry(exchange_id)
            .and_modify(|_| panic!())
            .or_insert_with(|| exchange);
    }
}

struct ExchangeServer {
    receivers: Range<usize>,
    directory: ExchangeDirectory,
    stream: TcpStream,
}

impl ExchangeServer {
    async fn serve(mut self) -> std::io::Result<()> {
        while let Some(header) = ExchangeHeader::read(&mut self.stream).await? {
            let start = Instant::now();
            let exchange_id = header.exchange_id;
            let sender = header.sender as usize;
            let mut bytes = self.receivers.len() * ExchangeHeader::LEN;
            let mut header = Some(header);
            let mut data = Vec::with_capacity(self.receivers.len());
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
                data.push(payload);

                bytes += padded_len;
            }
            Span::new("receive")
                .with_start(start)
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "exchange {exchange_id} receive {} from worker {sender}",
                        HumanBytes::from(bytes),
                    )
                })
                .record();

            let receiver = self
                .directory
                .get(exchange_id)
                .expect("should have exchange for received data");
            receiver.received(sender, data).await;
        }
        Ok(())
    }
}

pub struct ExchangeClients {
    runtime: WeakRuntime,

    /// Cached `runtime.layout().local_workers()`.
    local_workers: Range<usize>,

    /// Listens for connections from other hosts.
    ///
    /// We create this lazily upon the first attempt to connect to other hosts.
    /// If we create it before we've completely initialized the circuit, then we
    /// might not have created all of the exchanges yet when some other host
    /// tries to send data to one.
    listener: OnceCell<Option<ExchangeListener>>,

    /// Maps from a range of worker IDs to the RPC client used to contact those
    /// workers.  Only worker IDs for remote workers appear in the map.
    clients: Vec<(Host, OnceCell<ExchangeClient>)>,
}

impl ExchangeClients {
    pub fn for_runtime(runtime: &Runtime) -> Arc<ExchangeClients> {
        runtime
            .local_store()
            .entry(ClientsId)
            .or_insert_with(|| {
                // Create clients for remote exchange.
                Arc::new(ExchangeClients::new(runtime))
            })
            .clone()
    }

    fn new(runtime: &Runtime) -> ExchangeClients {
        Self {
            local_workers: runtime.layout().local_workers(),
            runtime: runtime.downgrade(),
            listener: Default::default(),
            clients: runtime
                .layout()
                .other_hosts()
                .map(|host| (host.clone(), OnceCell::new()))
                .collect(),
        }
    }

    /// Returns a client for `worker`, which must be a remote worker ID, first
    /// establishing a connection if there isn't one yet.
    pub async fn connect(&self, worker: usize) -> &ExchangeClient {
        self.listener
            .get_or_init(|| async {
                if let Some(runtime) = self.runtime.upgrade()
                    && let Some(local_address) = runtime.layout().local_address()
                {
                    let directory = ExchangeDirectory::for_runtime(&runtime);
                    Some(ExchangeListener::new(
                        local_address,
                        runtime.take_exchange_listener(),
                        directory,
                        self.local_workers.clone(),
                    ))
                } else {
                    None
                }
            })
            .await;

        let (host, cell) = self
            .clients
            .iter()
            .find(|(host, _client)| host.workers.contains(&worker))
            .unwrap();
        cell.get_or_init(|| ExchangeClient::new(host.address, &host.workers))
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

#[derive(Clone, Debug)]
pub enum Mailbox<T> {
    Tx(FBuf),
    Rx(AlignedVec),
    Plain(T),
}

impl<T> Mailbox<T> {
    pub fn into_plain(self) -> Option<T> {
        match self {
            Self::Plain(item) => Some(item),
            _ => None,
        }
    }
    pub fn into_tx(self) -> Option<FBuf> {
        match self {
            Mailbox::Tx(bytes) => Some(bytes),
            Mailbox::Rx(_) | Mailbox::Plain(_) => None,
        }
    }
    fn deserialize<D>(self, deserialize: D) -> T
    where
        D: Fn(AlignedVec) -> T,
    {
        match self {
            Mailbox::Plain(item) => item,
            Mailbox::Tx(_) => unreachable!(),
            Mailbox::Rx(bytes) => deserialize(bytes),
        }
    }
}

/// `Exchange` is an N-to-N communication primitive that partitions data across
/// multiple concurrent threads.
///
/// An instance of `Exchange` is shared by threads that communicate in rounds.
/// In each round each peer _first_ sends exactly one data value to every other
/// peer (and itself) and then receives one value from each peer.  The send
/// operation can only proceed when all peers have retrieved data produced at
/// the previous round.  Likewise, the receive operation can proceed once all
/// incoming values are ready for the current round.
pub(crate) struct Exchange<T> {
    exchange_id: ExchangeId,

    /// The number of communicating peers.
    npeers: usize,

    /// Range of worker IDs on the local host.
    local_workers: Range<usize>,

    /// Counts the number of messages received in the current round of
    /// communication per receiver.  The receiver must wait until it has all
    /// `npeers` messages before reading all of them from mailboxes in one
    /// pass.
    receiver_counters: Vec<AtomicUsize>,

    /// Callback invoked when all `npeers` messages are ready for a receiver.
    receiver_callbacks: Vec<Callback>,

    /// Notified when all `npeers` messages are ready for a receiver.
    receiver_notifies: Vec<Notify>,

    /// Counts the number of empty mailboxes ready to accept new data per
    /// sender.  Delivery from any given sender waits until all
    /// `local_workers.len()` mailboxes are available before writing them in one
    /// pass.
    sender_counters: Vec<CachePadded<AtomicUsize>>,

    /// Callback invoked when all `npeers` mailboxes are available.
    sender_callbacks: Vec<Callback>,

    /// Notified when all `npeers` mailboxes are available.
    sender_notifies: Vec<Notify>,

    /// The RPC clients to contact remote hosts.
    clients: Arc<ExchangeClients>,

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
    mailboxes: Vec<Mutex<Option<Mailbox<T>>>>,

    /// The amount of time deserializing remote exchange data.
    deserialization_usecs: AtomicU64,

    /// The number of bytes serialized.
    deserialized_bytes: AtomicUsize,
}

// Stop Rust from complaining about unused field.
#[allow(dead_code)]
struct ExchangeListener(DropGuard);

impl ExchangeListener {
    fn new(
        local_address: SocketAddr,
        exchange_listener: Option<std::net::TcpListener>,
        directory: ExchangeDirectory,
        receivers: Range<usize>,
    ) -> Self {
        let token = CancellationToken::new();
        let drop = token.clone().drop_guard();
        TOKIO.spawn(async move {
            info!("listening on {local_address}");
            let listener = match exchange_listener {
                Some(exchange_listener) => {
                    exchange_listener
                        .set_nonblocking(true)
                        .expect("should be able to set nonblocking mode");
                    TcpListener::from_std(exchange_listener).unwrap()
                }
                None => TcpListener::bind(local_address).await.unwrap(),
            };

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
                    Err(error) => warn!("Error accepting connection: {error}"),
                }
            }
        });
        Self(drop)
    }
}

impl<T> Exchange<T>
where
    T: Clone + Debug + Send + 'static,
{
    /// Create a new exchange operator for `npeers` communicating threads.
    fn new(
        runtime: &Runtime,
        clients: Arc<ExchangeClients>,
        exchange_id: ExchangeId,
        directory: &ExchangeDirectory,
    ) -> Arc<Self> {
        let npeers = Runtime::num_workers();
        let mailboxes: Vec<Mutex<Option<Mailbox<T>>>> =
            (0..npeers * npeers).map(|_| Default::default()).collect();

        let layout = runtime.layout();
        let npeers = layout.n_workers();

        let exchange = Arc::new(Self {
            exchange_id,
            npeers,
            local_workers: layout.local_workers(),
            clients,
            receiver_counters: (0..npeers).map(|_| AtomicUsize::new(0)).collect(),
            receiver_callbacks: (0..npeers).map(|_| Callback::empty()).collect(),
            receiver_notifies: (0..npeers).map(|_| Notify::new()).collect(),
            sender_counters: (0..npeers)
                .map(|_| CachePadded::new(AtomicUsize::new(layout.local_workers().len())))
                .collect(),
            sender_notifies: (0..npeers).map(|_| Notify::new()).collect(),
            sender_callbacks: (0..npeers).map(|_| Callback::empty()).collect(),
            mailboxes,
            deserialization_usecs: AtomicU64::new(0),
            deserialized_bytes: AtomicUsize::new(0),
        });

        directory.insert(exchange_id, exchange.clone());

        exchange
    }

    #[allow(dead_code)]
    fn exchange_id(&self) -> ExchangeId {
        self.exchange_id
    }

    /// Returns an index for the sender/receiver pair.
    fn mailbox_index(&self, sender: usize, receiver: usize) -> usize {
        debug_assert!(sender < self.npeers);
        debug_assert!(receiver < self.npeers);
        sender * self.npeers + receiver
    }

    /// True if all `receiver`'s incoming mailboxes contain data.
    ///
    /// Once this function returns true, a subsequent `try_receive_all`
    /// operation is guaranteed for `receiver`.
    fn ready_to_receive(&self, receiver: usize) -> bool {
        debug_assert!(receiver < self.npeers);
        self.receiver_counters[receiver].load(Ordering::Acquire) == self.npeers
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
    /// by calling `ready_to_receive` or be prepared that `receive_all`
    /// can fail.
    pub(crate) fn register_receiver_callback<F>(&self, receiver: usize, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        debug_assert!(receiver < self.npeers);

        self.receiver_callbacks[receiver].set_callback(cb);
    }

    /// Locks and returns the mailbox for the sender/receiver pair.
    fn mailbox(&self, sender: usize, receiver: usize) -> MutexGuard<'_, Option<Mailbox<T>>> {
        self.mailboxes[self.mailbox_index(sender, receiver)]
            .lock()
            .unwrap()
    }

    /// Create a new `Exchange` instance if an instance with the same id
    /// (created by another thread) does not yet exist within `runtime`.
    /// The number of peers will be set to `runtime.num_workers()`.
    pub(crate) fn with_runtime(runtime: &Runtime, exchange_id: ExchangeId) -> Arc<Self> {
        // It's tempting to move the following calls to create the
        // `ExchangeDirectory` and `ExchangeClients` into `Exchange::new`, but
        // don't do it: all three of these access `runtime.local_store` and
        // nesting them creates deadlocks at runtime.
        let directory = ExchangeDirectory::for_runtime(runtime);
        let clients = ExchangeClients::for_runtime(runtime);
        runtime
            .local_store()
            .entry(ExchangeCacheId::new(exchange_id))
            .or_insert_with(|| Exchange::new(runtime, clients, exchange_id, &directory))
            .value()
            .clone()
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
        debug_assert!(sender < self.npeers);
        self.sender_callbacks[sender].set_callback(cb);
    }

    pub fn ready_to_send(&self, sender: usize) -> bool {
        self.sender_counters[sender].load(Ordering::Acquire) == self.local_workers.len()
    }

    /// Waits until all the mailboxes to receive data from `sender` are empty.
    async fn wait_for_ready_to_send(&self, sender: usize) {
        fn ready_to_send<T>(this: &Exchange<T>, sender: usize) -> bool {
            this.sender_counters[sender]
                .compare_exchange(
                    this.local_workers.len(),
                    0,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
        }

        // Wait for the receivers to have empty mailboxes first.
        if !ready_to_send(self, sender) {
            loop {
                let notify = self.sender_notifies[sender].notified();
                if ready_to_send(self, sender) {
                    break;
                }
                notify.await;
            }
        }
    }

    pub(crate) async fn send_all_with_serializer<F>(
        self: &Arc<Self>,
        data: impl Iterator<Item = T>,
        mut serialize: F,
    ) where
        F: FnMut(T) -> FBuf + Send + Sync,
    {
        self.send_all(
            data.zip(WorkerLocations::new())
                .map(|(data, location)| match location {
                    WorkerLocation::Local => Mailbox::Plain(data),
                    WorkerLocation::Remote => Mailbox::Tx(serialize(data)),
                }),
        )
        .await
    }

    fn deliver(&self, sender: usize, receiver: usize, item: Mailbox<T>) {
        let mut mailbox = self.mailbox(sender, receiver);
        assert!(mailbox.is_none());
        *mailbox = Some(item);

        let old_counter = self.receiver_counters[receiver].fetch_add(1, Ordering::AcqRel);
        if old_counter >= self.npeers - 1 {
            self.receiver_callbacks[receiver].call();
            self.receiver_notifies[receiver].notify_waiters();
        }
    }

    /// Write all outgoing messages for this worker to mailboxes, first waiting
    /// for the mailboxes to become available if any of them are not empty yet.
    ///
    /// Values to be sent are retrieved from the `data` iterator, with the
    /// first value delivered to receiver 0, second value delivered to receiver
    /// 1, and so on.
    ///
    /// # Panics
    ///
    /// Panics if `data` yields fewer than `self.npeers` items.
    pub(crate) async fn send_all(self: &Arc<Self>, mut data: impl Iterator<Item = Mailbox<T>>) {
        let sender = Runtime::worker_index();

        self.wait_for_ready_to_send(sender).await;

        let runtime = Runtime::runtime().unwrap();
        let layout = runtime.layout();
        let worker_locations = WorkerLocations::for_layout(layout);
        for receivers in layout.all_hosts() {
            match worker_locations[receivers.start] {
                WorkerLocation::Local => {
                    for receiver in receivers.clone() {
                        let item = data.next().expect("data should include one item per peer");
                        self.deliver(sender, receiver, item);
                    }
                }
                WorkerLocation::Remote => {
                    let mut serialized_bytes = 0;
                    let items = receivers
                        .clone()
                        .map(|_| {
                            data.next()
                                .expect("data should include one item per peer")
                                .into_tx()
                                .expect("remote mailboxes should always be serialized")
                        })
                        .inspect(|serialized| {
                            serialized_bytes += serialized.len();
                        })
                        .collect_vec();
                    let this = self.clone();
                    this.clients
                        .connect(receivers.start)
                        .await
                        .send(this.exchange_id, sender, items)
                        .await;
                }
            }
        }
    }

    /// Read all incoming messages for this worker, waiting for data to arrive
    /// as needed.
    pub(crate) async fn receive_all<D>(&self, deserialize: D) -> Vec<T>
    where
        D: Fn(AlignedVec) -> T,
    {
        let receiver = Runtime::worker_index();
        fn may_receive<T>(exchange: &Exchange<T>, receiver: usize) -> bool {
            exchange.receiver_counters[receiver]
                .compare_exchange(exchange.npeers, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }
        if !may_receive(self, receiver) {
            loop {
                let notifier = self.receiver_notifies[receiver].notified();
                if may_receive(self, receiver) {
                    break;
                }
                notifier.await;
            }
        }

        let mut data = Vec::with_capacity(self.npeers);
        for sender in 0..self.npeers {
            let mailbox = self.mailbox(sender, receiver).take().unwrap();
            data.push(mailbox.deserialize(&deserialize));
            let old_counter = self.sender_counters[sender].fetch_add(1, Ordering::AcqRel);
            if old_counter + 1 >= self.local_workers.len() {
                self.sender_callbacks[sender].call();
                self.sender_notifies[sender].notify_waiters();
            }
        }

        data
    }
}

impl<T> ExchangeDelivery for Exchange<T>
where
    T: Clone + Debug + Send + 'static,
{
    fn received<'a>(
        &'a self,
        sender: usize,
        data: Vec<AlignedVec>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            self.wait_for_ready_to_send(sender).await;

            for (receiver, data) in zip(self.local_workers.clone(), data) {
                self.deliver(sender, receiver, Mailbox::Rx(data));
            }
        })
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
/// # Multihost
///
/// Workers can reside in different processes that need to exchange data over a
/// network.  There is little reason to do this if the processes are on the same
/// host, so we call this "multihost" exchange.  We tend to speak of processes
/// and hosts interchangeably in this context.
///
/// Multihost exchange works mostly as shown in the diagram above, except that
/// there is a network in the middle.  Suppose that we have two hosts with two
/// workers each, even though they ordinarily would have more than that.  Each
/// host listens on a network port with a single `ExchangeListener` and
/// constructs one `ExchangeClient` for each remote host.  Data destined to a
/// worker on the same host uses local mechanisms; data destined to a worker on
/// a different host flows through an appropriate `ExchangeClient` to the remote
/// `ExchangeListener` to the correct worker.
///
/// The diagram below shows how ExchangeSender in worker 1 (ES1) sends data to
/// the ExchangeReceivers (ERs) for other workers.  Data for workers 1 and 2
/// stays on the same host, so it goes directly.  Data for workers 3 and 4
/// passes through the local ExchangeClient (EC1) to the remote ExchangeListener
/// (EL2), which delivers it to the remote ExchangeReceivers.
///
/// ```text
///     ┌───┐      ┌───┐
///  ──►│ES1│──┬──>│ER1│   Worker 1
///     └───┘  │   └───┘
///            │
///     ┌───┐  │   ┌───┐
///  ──►│ES2│  ├──>│ER2│   Worker 2
///     └───┘  │   └───┘
///            ↓
///          ┌───┐
///          │EC1│
///          └───┘
///            │
/// HOST 1     │
/// ───NETWORK CONNECTION───────────────────────────────────────────
/// HOST 2     │
///            ↓
///          ┌───┐
///          │EL2│
///          └───┘
///            │
///     ┌───┐  │   ┌───┐
///  ──►│ES3│  ├──>│ER3│   Worker 3
///     └───┘  │   └───┘
///            │
///     ┌───┐  │   ┌───┐
///  ──►│ES4│  └──>│ER4│   Worker 4
///     └───┘      └───┘
/// ```
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
        location: OperatorLocation,
        exchange: Arc<Exchange<(T, bool)>>,
        start_wait_usecs: Arc<AtomicU64>,
        partition: L,
    ) -> Self {
        Self {
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
    T: Send + 'static + Clone + Debug,
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

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flushed = true;
    }
}

impl<D, T, L> SinkOperator<D> for ExchangeSender<D, T, L>
where
    D: Clone + Debug + NumEntries + 'static,
    T: Clone + Debug + Send + 'static,
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

        let data = self.outputs.drain(..).map(|mailbox| match mailbox {
            Mailbox::Tx(mut data) => {
                data.push(self.flushed as u8);
                Mailbox::Tx(data)
            }
            Mailbox::Rx(_) => unreachable!(),
            Mailbox::Plain(item) => Mailbox::Plain((item, self.flushed)),
        });

        self.exchange.send_all(data).await;

        self.flushed = false;
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
pub struct ExchangeReceiver<IF, T, L, D>
where
    T: Send + 'static + Clone,
{
    worker_index: usize,
    location: OperatorLocation,
    init: IF,
    deserialize: D,
    combine: L,
    exchange: Arc<Exchange<(T, bool)>>,
    flush_count: usize,
    flush_complete: bool,
    start_wait_usecs: Arc<AtomicU64>,
    total_wait_time: Arc<AtomicU64>,

    // Output batch sizes.
    output_batch_stats: BatchSizeStats,
}

impl<IF, T, L, D> ExchangeReceiver<IF, T, L, D>
where
    T: Send + 'static + Clone + Debug,
{
    pub(crate) fn new(
        worker_index: usize,
        location: OperatorLocation,
        exchange: Arc<Exchange<(T, bool)>>,
        init: IF,
        start_wait_usecs: Arc<AtomicU64>,
        deserialize: D,
        combine: L,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());

        Self {
            worker_index,
            location,
            init,
            combine,
            deserialize,
            exchange,
            flush_count: 0,
            flush_complete: false,
            output_batch_stats: BatchSizeStats::new(),
            start_wait_usecs,
            total_wait_time: Arc::new(AtomicU64::new(0)),
        }
    }

    fn is_ready(&self) -> bool {
        self.exchange.ready_to_receive(self.worker_index)
    }
}

impl<IF, T, L, D> Operator for ExchangeReceiver<IF, T, L, D>
where
    IF: 'static,
    T: Send + 'static + Clone + Debug,
    L: 'static,
    D: 'static,
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
        self.is_ready()
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

pub fn pop_flushed(vec: &mut AlignedVec) -> bool {
    match vec.pop().unwrap() {
        0 => false,
        1 => true,
        _ => unreachable!(),
    }
}

impl<O, IF, T, L, D> SourceOperator<O> for ExchangeReceiver<IF, T, L, D>
where
    O: NumEntries + 'static,
    T: Clone + Debug + Send + 'static,
    IF: Fn() -> O + 'static,
    L: Fn(&mut O, T) + 'static,
    D: Fn(AlignedVec) -> T + Send + Sync + 'static,
{
    async fn eval(&mut self) -> O {
        debug_assert!(self.ready());
        let deserialize = |mut vec: AlignedVec| {
            let flushed = pop_flushed(&mut vec);
            let value = (self.deserialize)(vec);
            (value, flushed)
        };

        let mut combined = (self.init)();
        let res = self.exchange.receive_all(deserialize).await;
        for (data, flushed) in res {
            if flushed {
                self.flush_count += 1;
            }
            (self.combine)(&mut combined, data)
        }

        if self.flush_count == Runtime::num_workers() {
            // println!(
            //     "{} exchange_receiver::eval received all inputs",
            //     Runtime::worker_index()
            // );

            self.flush_complete = true;
            self.flush_count = 0;
        }

        self.output_batch_stats
            .add_batch(combined.num_entries_deep());
        combined
    }
}

#[derive(Hash, PartialEq, Eq)]
struct ClientsId;

impl TypedMapKey<LocalStoreMarker> for ClientsId {
    type Value = Arc<ExchangeClients>;
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
) -> Option<(ExchangeSender<TI, TE, PL>, ExchangeReceiver<IF, TE, CL, D>)>
where
    TO: Clone,
    TE: Send + 'static + Clone + Debug,
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
    let exchange = Exchange::with_runtime(&runtime, exchange_id);
    let sender = ExchangeSender::new(
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
        deserialize,
        combine,
    );
    Some((sender, receiver))
}

#[cfg(test)]
mod tests {
    use feldera_storage::tokio::TOKIO;
    use itertools::Itertools;

    use super::Exchange;
    use crate::{
        Circuit, RootCircuit,
        circuit::{
            CircuitConfig, Layout, Runtime,
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
    use std::{
        iter::{repeat, zip},
        net::TcpListener,
    };

    /// Number of rounds for exchange.
    ///
    /// We decrease the number of rounds we do when we're running under miri,
    /// otherwise it'll run forever
    const ROUNDS: usize = if cfg!(miri) { 128 } else { 2048 };

    // A circuit that iterates for `ROUNDS` rounds with each sender sending
    // value `(sender, n)` to each receiver, where `sender` is the sender's
    // worker number in round `n`.
    fn circuit() {
        let exchange = Exchange::with_runtime(&Runtime::runtime().unwrap(), 0);
        TOKIO.block_on(async {
            let sender = Runtime::worker_index();
            let n_workers = Runtime::num_workers();
            for round in 0..ROUNDS {
                exchange
                    .send_all_with_serializer(repeat((sender, round)), |data| {
                        to_bytes(&data).unwrap()
                    })
                    .await;

                let received = exchange
                    .receive_all(|data| aligned_deserialize(&data[..]))
                    .await;

                let expected = (0..n_workers).map(|worker| (worker, round)).collect_vec();
                assert_eq!(received, expected);
            }
        });
    }

    fn test_circuit(
        workers: usize,
        hosts: usize,
        circuit: impl FnOnce() + Copy + Clone + Send + Sync + 'static,
    ) {
        match hosts {
            0 => unreachable!(),
            1 => {
                let hruntime = Runtime::run(workers, move |_parker| circuit())
                    .expect("failed to start runtime");

                hruntime.join().unwrap();
            }
            _ => {
                assert!(workers >= hosts);

                // Bind some listening sockets.
                let exchange_listeners = (0..hosts)
                    .map(|_| {
                        TcpListener::bind("127.0.0.1:0")
                            .expect("should be able to bind a port on localhost")
                    })
                    .collect_vec();

                // Assemble the listening sockets' addresses into something we can pass
                // to `Layout::new_multihost`.
                let params = exchange_listeners
                    .iter()
                    .enumerate()
                    .map(|(index, listener)| {
                        (
                            listener
                                .local_addr()
                                .expect("should be able to get local address"),
                            workers / hosts + (index < workers % hosts) as usize,
                        )
                    })
                    .collect_vec();

                // Create the runtimes.
                let mut runtimes = Vec::with_capacity(hosts);
                for ((local_address, _), exchange_listener) in
                    zip(params.iter(), exchange_listeners)
                {
                    let cconf = CircuitConfig::from(
                        Layout::new_multihost(&params, *local_address).unwrap(),
                    )
                    .with_exchange_listener(exchange_listener);

                    runtimes.push(
                        Runtime::run(cconf, move |_parker| circuit())
                            .expect("failed to start runtime"),
                    );
                }

                // Wait for the runtimes to finish.
                for runtime in runtimes {
                    runtime.join().unwrap();
                }
            }
        }
    }

    // Test an exchange object with multiple concurrent senders/receivers on a single host.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn single_host() {
        for workers in [2, 4, 8] {
            test_circuit(workers, 1, circuit);
        }
    }

    // Test an exchange object with multiple concurrent senders/receivers on multiple hosts.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn multihost() {
        for (workers, hosts) in [(2, 2), (4, 2), (8, 2), (3, 3), (4, 4), (16, 4)] {
            test_circuit(workers, hosts, circuit);
        }
    }

    fn operator_circuit<S>()
    where
        S: Scheduler + 'static,
    {
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
                    assert_eq!(&vec![round; Runtime::num_workers()], v);
                    round += 1;
                });
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 1..ROUNDS {
            circuit.transaction().unwrap();
        }
    }

    // Create a circuit with `WORKERS` concurrent workers with the following
    // structure: `Generator - ExchangeSender -> ExchangeReceiver -> Inspect`.
    // `Generator` - yields sequential numbers 0, 1, 2, ...
    // `ExchangeSender` - sends each number to all peers.
    // `ExchangeReceiver` - combines all received numbers in a vector.
    // `Inspect` - validates the output of the receiver.
    fn test_operators_single_host(circuit: impl FnOnce() + Copy + Clone + Send + Sync + 'static) {
        for workers in [2, 16, 32] {
            test_circuit(workers, 1, circuit);
        }
    }

    // Create a circuit with `WORKERS` concurrent workers with the following
    // structure: `Generator - ExchangeSender -> ExchangeReceiver -> Inspect`.
    // `Generator` - yields sequential numbers 0, 1, 2, ...
    // `ExchangeSender` - sends each number to all peers.
    // `ExchangeReceiver` - combines all received numbers in a vector.
    // `Inspect` - validates the output of the receiver.
    fn test_operators_multihost(circuit: impl FnOnce() + Copy + Clone + Send + Sync + 'static) {
        for (workers, hosts) in [(2, 2), (4, 2), (8, 2), (3, 3), (4, 4), (16, 4)] {
            test_circuit(workers, hosts, circuit);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn operators_single_host_dynamic() {
        test_operators_single_host(operator_circuit::<DynamicScheduler>);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn operators_multihost_dynamic() {
        test_operators_multihost(operator_circuit::<DynamicScheduler>);
    }
}
