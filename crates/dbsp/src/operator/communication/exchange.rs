//! Exchange operators implement a N-to-N communication pattern where
//! each participant sends exactly one value to and receives exactly one
//! value from each peer at every clock cycle.

// TODO: We may want to generalize these operators to implement N-to-M
// communication, including 1-to-N and N-to-1.

use crate::{
    NumEntries, WeakRuntime,
    circuit::{
        GlobalNodeId, Host, Layout, LocalStoreMarker, OwnershipPreference, Runtime, Scope,
        metadata::{
            BatchSizeStats, EXCHANGE_DESERIALIZATION_TIME_SECONDS, EXCHANGE_DESERIALIZED_BYTES,
            EXCHANGE_WAIT_TIME_SECONDS, INPUT_BATCHES_STATS, MetaItem, OUTPUT_BATCHES_STATS,
            OperatorLocation, OperatorMeta,
        },
        metrics::{DUPLICATE_EXCHANGE_MESSAGES_RECEIVED, EXCHANGE_MESSAGES_RECEIVED},
        operator_traits::{Operator, OperatorName, SinkOperator, SourceOperator},
        runtime::{WorkerLocation, WorkerLocations},
        tokio::TOKIO,
    },
    circuit_cache_key,
};
use binrw::{BinRead, BinResult, BinWrite};
use crossbeam_utils::CachePadded;
use enum_map::{Enum, EnumMap};
use feldera_samply::Span;
use feldera_storage::fbuf::FBuf;
use futures::future::select;
use itertools::Itertools;
use rkyv::AlignedVec;
use size_of::HumanBytes;
use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    io::{Cursor, IoSlice},
    iter::zip,
    marker::PhantomData,
    mem::MaybeUninit,
    net::SocketAddr,
    ops::Range,
    pin::{Pin, pin},
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Notify, OnceCell, futures::OwnedNotified},
    time::sleep,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, info, warn};
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
/// A complete exchange consists of [ExchangeHeader] followed by the payload for
/// each receiving worker in order.  There is no padding or alignment (which is
/// fine because we read and write each header and payload in a separate call).
#[binrw::binrw]
#[brw(little)]
#[br(import(count: usize))]
struct ExchangeHeader {
    /// The unique identifier for the exchange.
    exchange_id: ExchangeId,
    /// Sequence number for the collection of messages.
    sequence: u64,
    #[bw(write_with = MessageType::write)]
    #[br(parse_with = MessageType::read)]
    message_type: MessageType,
    /// The sending worker.
    sender: u32,
    /// The length of each payload.
    #[br(count = count)]
    payload_lens: Vec<u64>,
}

impl ExchangeHeader {
    /// Returns the number of bytes that `ExchangeHeader::to_bytes()` will
    /// return for the given `count`.
    fn len_for_count(count: usize) -> usize {
        (4 + 8 + 1 + 4) + 8 * count
    }

    /// Serializes this header into a `Vec` that contains
    /// `Self::len_for_count(count)` bytes, where `count` is
    /// `self.payload_lens()`.
    fn to_bytes(&self) -> Vec<u8> {
        let len = Self::len_for_count(self.payload_lens.len());
        let mut cursor = Cursor::new(Vec::with_capacity(len));
        self.write_le(&mut cursor).unwrap();
        assert_eq!(cursor.position(), len as u64);
        cursor.into_inner()
    }

    /// Deserializes an `ExchangeHeader` with `count` payload lengths from
    /// `bytes`.  `bytes.len()` must equal `Self::len_for_count(count)`.
    fn from_bytes(count: usize, bytes: &[u8]) -> Self {
        debug_assert_eq!(bytes.len(), Self::len_for_count(count));
        let mut cursor = Cursor::new(bytes);
        let this = Self::read_le_args(&mut cursor, (count,)).unwrap();
        assert_eq!(cursor.position(), bytes.len() as u64);
        this
    }

    /// Reads an `ExchangeHeader` with `count` payload lengths from `stream`.
    async fn read<S>(count: usize, stream: &mut S) -> std::io::Result<Option<Self>>
    where
        Self: Sized,
        S: AsyncRead + Unpin,
    {
        let mut buf = vec![0; Self::len_for_count(count)];
        match stream.read(&mut buf).await? {
            0 => Ok(None),
            n => {
                stream.read_exact(&mut buf[n..]).await?;
                Ok(Some(Self::from_bytes(count, &buf)))
            }
        }
    }
}

struct ExchangeMessage {
    /// The time at which the message was created.  This allows tracking the
    /// time elapsed from message creation to the time that it is sent, for CPU
    /// profiles.
    start: Instant,

    /// Global node ID of the exchange, for CPU profiles.
    global_node_id: Arc<String>,

    /// The exchange.
    exchange_id: ExchangeId,

    /// The sender's worker ID.
    sender: usize,

    /// The messages to send, one per worker on the destination remote host.
    ///
    /// The workers and the host are implicit in the [ExchangeClient] that this
    /// `ExchangeMessage` is sent to.
    data: Vec<FBuf>,
}

impl ExchangeMessage {
    fn isize_len(&self) -> isize {
        isize::try_from(self.data.len()).unwrap()
    }
}

/// Distinguishes messages by size.
///
/// We segregate big and small messages into separate queues so that simple
/// broadcast and consensus messages don't get delayed behind bigger messages.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Enum)]
pub(crate) enum MessageSize {
    /// A big message.
    Big,

    /// A small message.
    Small,
}

impl MessageSize {
    /// Constructs `MessageSize` from a count of `bytes`.
    pub(crate) fn from_bytes(bytes: usize) -> Self {
        if bytes <= 4096 {
            Self::Small
        } else {
            Self::Big
        }
    }
}

/// Categorizes an [ExchangeMessage].
///
/// We segregate messages in different categories into different queues so that
/// messages in one category do not delay those in other categories.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Enum)]
pub(crate) enum MessageType {
    /// Messages sent via synchronous exchange.
    Synchronous(
        /// Message size.
        MessageSize,
    ),
    /// Messages sent via streaming exchange.
    Streaming,
}

impl MessageType {
    #[binrw::parser(reader)]
    pub(crate) fn read() -> BinResult<Self> {
        let byte = <u8>::read(reader)? as usize;
        if byte < Self::LENGTH {
            Ok(Self::from_usize(byte))
        } else {
            Err(binrw::Error::NoVariantMatch { pos: 0 })
        }
    }
    #[binrw::writer(writer)]
    pub(crate) fn write(value: &Self) -> BinResult<()> {
        let byte = value.into_usize() as u8;
        byte.write(writer)
    }
}

struct ExchangeChannelInner {
    /// Remaining capacity.  When this is negative, the channel is
    /// oversubscribed and no more messages should be queued until the other end
    /// acknowledges some of the messages that have been sent.
    remaining: isize,

    /// Signaled when `remaining` become nonnegative.
    nonfull: Arc<Notify>,

    /// Signaled when a message is added to `messages`.
    nonempty: Arc<Notify>,

    /// Queued messages.
    messages: VecDeque<Arc<ExchangeMessage>>,

    /// Sequence number of the first message in `messages`.
    sequence: u64,
}

impl ExchangeChannelInner {
    fn new(capacity: usize) -> Self {
        Self {
            remaining: capacity.try_into().unwrap(),
            nonfull: Arc::new(Notify::new()),
            nonempty: Arc::new(Notify::new()),
            messages: VecDeque::new(),
            sequence: 0,
        }
    }

    fn get(&self, min_sequence: u64) -> Result<(Arc<ExchangeMessage>, u64), OwnedNotified> {
        let index = min_sequence.saturating_sub(self.sequence);
        match self.messages.get(index as usize) {
            Some(message) => Ok((message.clone(), self.sequence + index)),
            None => Err(self.nonempty.clone().notified_owned()),
        }
    }

    fn drain(&mut self, next_sequence: u64) {
        let before = self.remaining;
        while self.sequence < next_sequence
            && let Some(message) = self.messages.pop_front()
        {
            self.sequence += 1;
            self.remaining += message.isize_len();
        }
        if before < 0 && self.remaining >= 0 {
            self.nonfull.notify_waiters();
        }
    }

    fn push(&mut self, message: ExchangeMessage) {
        self.remaining -= message.isize_len();
        self.messages.push_back(Arc::new(message));
        self.nonempty.notify_waiters();
    }

    fn drain_waiter(&self) -> Option<OwnedNotified> {
        (self.remaining < 0).then(|| self.nonfull.clone().notified_owned())
    }
}

#[derive(Clone)]
struct ExchangeChannel(Arc<Mutex<ExchangeChannelInner>>);

impl ExchangeChannel {
    pub fn new(capacity: usize) -> Self {
        Self(Arc::new(Mutex::new(ExchangeChannelInner::new(capacity))))
    }

    fn inner(&self) -> MutexGuard<'_, ExchangeChannelInner> {
        self.0.lock().unwrap()
    }

    /// If the channel contains a message with a sequence number greater than or
    /// equal to `min_sequence`, returns it and its sequence number.  Otherwise,
    /// returns an [OwnedNotified] for waiting until a message is queued.
    pub fn get(&self, min_sequence: u64) -> Result<(Arc<ExchangeMessage>, u64), OwnedNotified> {
        self.inner().get(min_sequence)
    }

    /// Drops all of the messages in the channel with sequence numbers less than
    /// `next_sequence`.
    pub fn drain(&self, next_sequence: u64) {
        self.inner().drain(next_sequence)
    }

    /// Appends `message` to the queue.  If the channel is then overfull,
    /// returns an [OwnedNotified] that can be used to wait for it to drain.
    /// Otherwise, returns `None`.
    pub fn push(&self, message: ExchangeMessage) -> Option<OwnedNotified> {
        let mut inner = self.inner();
        inner.push(message);
        inner.drain_waiter()
    }

    /// If this channel is overfull, returns an [OwnedNotified] that can be used
    /// to wait for it to drain.  Otherwise, returns `None`.
    fn drain_waiter(&self) -> Option<OwnedNotified> {
        self.inner().drain_waiter()
    }
}

pub struct ExchangeClient {
    channel: ExchangeChannel,
}

impl ExchangeClient {
    async fn new(
        message_type: MessageType,
        remote_address: SocketAddr,
        remote_workers: &Range<usize>,
    ) -> Self {
        let channel = ExchangeChannel::new(10_000_000);
        TOKIO.spawn(Self::run(
            message_type,
            remote_address,
            remote_workers.clone(),
            channel.clone(),
        ));
        Self { channel }
    }

    async fn run_connection_rx(
        mut rx: OwnedReadHalf,
        channel: ExchangeChannel,
    ) -> std::io::Result<()> {
        loop {
            channel.drain(rx.read_u64_le().await?);
        }
    }

    async fn run_connection_tx(
        mut tx: OwnedWriteHalf,
        message_type: MessageType,
        remote_workers: Range<usize>,
        channel: ExchangeChannel,
    ) -> std::io::Result<()> {
        let mut min_sequence = 0;
        loop {
            // Get the next message to send.
            let (message, sequence) = match channel.get(min_sequence) {
                Ok(result) => result,
                Err(notified) => {
                    notified.await;
                    continue;
                }
            };
            min_sequence = sequence + 1;

            if inject_fault("connection failure") {
                return Err(std::io::Error::other("simulated connection failure"));
            }

            // We want to write a header, followed by all the data buffers.  To
            // minimize the system calls required to do this, we assemble them
            // into a collection of IoSlices.  First, create the header.
            let n = remote_workers.len();
            let header = ExchangeHeader {
                exchange_id: message.exchange_id,
                sequence,
                sender: message.sender as u32,
                message_type,
                payload_lens: message
                    .data
                    .iter()
                    .map(|message| message.len().try_into().unwrap())
                    .collect(),
            }
            .to_bytes();

            // Assemble the IoSlices.
            let mut slices = Vec::with_capacity(1 + n);
            slices.push(IoSlice::new(&header));
            for data in &message.data {
                if !data.is_empty() {
                    slices.push(IoSlice::new(data.as_slice()));
                }
            }
            if inject_fault("partial send failure") {
                return Err(std::io::Error::other("simulated partial send failure"));
            }

            // Send the assembly.
            let size = slices.iter().map(|slice| slice.len()).sum::<usize>();
            let mut bufs = slices.as_mut_slice();
            let _span = Span::new("send")
                .with_start(message.start)
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "{} send {}",
                        &message.global_node_id,
                        HumanBytes::from(size),
                    )
                });
            while !bufs.is_empty() {
                let n = tx.write_vectored(bufs).await?;
                IoSlice::advance_slices(&mut bufs, n);
            }
        }
    }

    async fn run_connection(
        stream: TcpStream,
        message_type: MessageType,
        remote_workers: &Range<usize>,
        channel: &ExchangeChannel,
    ) -> std::io::Result<()> {
        stream.set_nodelay(true)?;
        stream.set_zero_linger()?;
        let (rx, tx) = stream.into_split();

        let rx = pin!(Self::run_connection_rx(rx, channel.clone()));
        let tx = pin!(Self::run_connection_tx(
            tx,
            message_type,
            remote_workers.clone(),
            channel.clone()
        ));
        select(rx, tx).await.factor_first().0
    }

    async fn run(
        message_type: MessageType,
        remote_address: SocketAddr,
        remote_workers: Range<usize>,
        channel: ExchangeChannel,
    ) {
        loop {
            match TcpStream::connect(remote_address).await {
                Ok(stream) => {
                    if let Err(error) =
                        Self::run_connection(stream, message_type, &remote_workers, &channel).await
                    {
                        warn!("connection to {remote_address} dropped ({error}), waiting to retry")
                    }
                }
                Err(error) => {
                    info!("connection to {remote_address} failed ({error}), waiting to retry")
                }
            }

            fn sleep_time() -> Duration {
                #[cfg(test)]
                {
                    use rand::Rng;
                    return Duration::from_micros(rand::thread_rng().gen_range(0..1000));
                }

                #[cfg(not(test))]
                Duration::from_millis(1000)
            }
            sleep(sleep_time()).await;
        }
    }

    pub fn send(
        &self,
        global_node_id: Arc<String>,
        exchange_id: ExchangeId,
        sender: usize,
        data: Vec<FBuf>,
    ) -> Option<OwnedNotified> {
        self.channel.push(ExchangeMessage {
            start: Instant::now(),
            global_node_id,
            exchange_id,
            sender,
            data,
        })
    }

    pub async fn wait(&self) {
        if let Some(waiter) = self.channel.drain_waiter() {
            waiter.await;
        }
    }
}

/// Uniquely identifies an `Exchange` or `ShardedAccumulator` within a circuit.
pub type ExchangeId = u32;

pub trait ExchangeDelivery: Send + Sync {
    fn name(&self) -> Arc<String>;

    fn received<'a>(
        &'a self,
        sender: usize,
        data: Vec<AlignedVec>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

// Maps from an `exchange_id` to an object for delivering to the exchange.
#[derive(Clone, Default)]
pub struct ExchangeDirectory {
    /// The next sequence number to expect, indexed by sending host and type of
    /// message.
    ///
    /// The index for the local host is not used.
    next_sequence: Arc<Vec<EnumMap<MessageType, AtomicU64>>>,

    /// The delivery closure for each exchange.
    entries: Arc<RwLock<HashMap<ExchangeId, ExchangeDirectoryEntry>>>,
}

struct ExchangeDirectoryEntry {
    /// Delivery closure.
    delivery: Arc<dyn ExchangeDelivery>,
}

impl ExchangeDirectoryEntry {
    fn new(delivery: Arc<dyn ExchangeDelivery>) -> Self {
        Self { delivery }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum ExchangeGetError {
    /// No exchange with the given ID.
    UnknownExchange(ExchangeId),
    /// Received message with sequence number that is later than the next
    /// sequence number we expect.
    InvalidSequence {
        /// Received sequence number.
        sequence: u64,
        /// Next expected sequence number.
        next_sequence: u64,
    },
}

pub struct ExchangeGet {
    /// The delivery callback, if the data should be delivered to it, or `None`
    /// if the data should be dropped because it is a duplicate.
    delivery: Option<Arc<dyn ExchangeDelivery>>,
    /// Next sequence number we expect to receive.
    next_sequence: u64,
}

impl ExchangeDirectory {
    pub fn for_runtime(runtime: &Runtime) -> Self {
        runtime
            .local_store()
            .entry(DirectoryId)
            .or_insert_with(|| Self {
                next_sequence: Arc::new(
                    (0..runtime.layout().n_hosts())
                        .map(|_| EnumMap::from_fn(|_| AtomicU64::new(0)))
                        .collect(),
                ),
                entries: Arc::new(RwLock::new(HashMap::new())),
            })
            .clone()
    }

    pub fn get(
        &self,
        exchange_id: ExchangeId,
        sending_host_idx: usize,
        message_type: MessageType,
        sequence: u64,
    ) -> Result<ExchangeGet, ExchangeGetError> {
        let map = self.entries.read().unwrap();
        let entry = map
            .get(&exchange_id)
            .ok_or(ExchangeGetError::UnknownExchange(exchange_id))?;
        match self.next_sequence[sending_host_idx][message_type].compare_exchange(
            sequence,
            sequence + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                // Correct sequence number.
                Ok(ExchangeGet {
                    delivery: Some(entry.delivery.clone()),
                    next_sequence: sequence + 1,
                })
            }
            Err(next_sequence) => {
                if sequence < next_sequence {
                    // Duplicate sequence number.
                    Ok(ExchangeGet {
                        delivery: None,
                        next_sequence,
                    })
                } else {
                    // Skipped sequence number.
                    Err(ExchangeGetError::InvalidSequence {
                        sequence,
                        next_sequence,
                    })
                }
            }
        }
    }

    pub fn insert(&self, exchange_id: ExchangeId, delivery: Arc<dyn ExchangeDelivery>) {
        self.entries
            .write()
            .unwrap()
            .entry(exchange_id)
            .and_modify(|_| panic!())
            .or_insert_with(|| ExchangeDirectoryEntry::new(delivery));
    }
}

struct ExchangeServer {
    layout: Layout,
    directory: ExchangeDirectory,
    stream: TcpStream,
}

impl ExchangeServer {
    async fn serve(mut self) -> std::io::Result<()> {
        self.stream.set_nodelay(true)?;
        let receivers = self.layout.local_workers();
        while let Some(header) = ExchangeHeader::read(receivers.len(), &mut self.stream).await? {
            if inject_fault("server failure") {
                return Err(std::io::Error::other("simulated server failure"));
            }

            let start = Instant::now();
            let exchange_id = header.exchange_id;
            let sequence = header.sequence;
            let sender = header.sender as usize;
            let n = receivers.len();
            let payload_lens = header.payload_lens.iter().copied().map(|len| len as usize);
            let bytes = ExchangeHeader::len_for_count(n) + payload_lens.clone().sum::<usize>();
            let mut data = Vec::with_capacity(n);
            for len in payload_lens {
                // Read the payload into an `AlignedVec` so that we can pass it
                // to `rkyv` later without copying.
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
                let mut payload = AlignedVec::with_capacity(len);
                let pointer = payload.as_mut_ptr() as *mut MaybeUninit<u8>;
                let mut slice = unsafe { std::slice::from_raw_parts_mut(pointer, len) };
                while !slice.is_empty() {
                    self.stream.read_buf(&mut slice).await?;
                }
                unsafe { payload.set_len(len) };
                data.push(payload);
            }

            let ExchangeGet {
                delivery,
                next_sequence,
            } = match self.directory.get(
                exchange_id,
                self.layout
                    .worker_idx_to_host_idx(sender)
                    .expect("valid sender index"),
                header.message_type,
                sequence,
            ) {
                Ok(get) => get,
                Err(error @ ExchangeGetError::InvalidSequence { .. }) => {
                    // I don't think this error should occur in practice.
                    // However, distributed systems are tricky and I am not 100%
                    // confident of that.  If it does occur in practice, it is
                    // better to log it as an error and continue operating than
                    // to fail.
                    error!(
                        "failed to deliver {sequence} from {sender} to {receivers:?}: {error:?}"
                    );
                    continue;
                }
                Err(error) => {
                    panic!(
                        "failed to deliver {sequence} from {sender} to {receivers:?}: {error:?}"
                    );
                }
            };
            Span::new("receive")
                .with_start(start)
                .with_category("Exchange")
                .with_tooltip(|| {
                    if let Some(delivery) = &delivery {
                        format!(
                            "{} receive {} seq {sequence} from worker {sender}",
                            delivery.name(),
                            HumanBytes::from(bytes),
                        )
                    } else {
                        format!("receive duplicate seq {sequence} (expected {next_sequence}) from worker {sender}")
                    }
                })
                .record();

            EXCHANGE_MESSAGES_RECEIVED.fetch_add(n, Ordering::Relaxed);
            if let Some(delivery) = delivery {
                delivery.received(sender, data).await;
            } else {
                DUPLICATE_EXCHANGE_MESSAGES_RECEIVED.fetch_add(n, Ordering::Relaxed);
            }

            // Tell the client it can stop buffering this message.
            self.stream.write_u64_le(next_sequence).await?;
        }
        Ok(())
    }
}

pub struct ExchangeClients {
    runtime: WeakRuntime,

    /// Cached `runtime.layout()`.
    layout: Layout,

    /// Listens for connections from other hosts.
    ///
    /// We create this lazily upon the first attempt to connect to other hosts.
    /// If we create it before we've completely initialized the circuit, then we
    /// might not have created all of the exchanges yet when some other host
    /// tries to send data to one.
    listener: OnceCell<Option<ExchangeListener>>,

    /// Maps from a range of worker IDs to the RPC clients used to contact those
    /// workers.  Only worker IDs for remote workers appear in the map.
    ///
    /// We use one RPC client per [MessageType] per [Host].
    clients: Vec<(Host, EnumMap<MessageType, OnceCell<ExchangeClient>>)>,
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
            layout: runtime.layout().clone(),
            runtime: runtime.downgrade(),
            listener: Default::default(),
            clients: runtime
                .layout()
                .other_hosts()
                .map(|host| (host.clone(), Default::default()))
                .collect(),
        }
    }

    /// Returns a client for `worker`, which must be a remote worker ID, first
    /// establishing a connection if there isn't one yet.
    pub async fn connect(&self, worker: usize, message_type: MessageType) -> &ExchangeClient {
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
                        self.layout.clone(),
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
        cell[message_type]
            .get_or_init(|| ExchangeClient::new(message_type, host.address, &host.workers))
            .await
    }

    pub async fn wait(&self) {
        for (_, clients) in &self.clients {
            for client in clients.values() {
                if let Some(client) = client.get() {
                    client.wait().await;
                }
            }
        }
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
    /// Unique identifier within the circuit.
    exchange_id: ExchangeId,

    /// Identifies the `ExchangeReceiver` operator for use in profile data.
    name: OperatorName,

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

    /// When the exchange is active.
    activity: ExchangeActivity,
}

// Stop Rust from complaining about unused field.
#[allow(dead_code)]
struct ExchangeListener(DropGuard);

impl ExchangeListener {
    fn new(
        local_address: SocketAddr,
        exchange_listener: Option<std::net::TcpListener>,
        directory: ExchangeDirectory,
        layout: Layout,
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
                                layout: layout.clone(),
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
        activity: ExchangeActivity,
    ) -> Arc<Self> {
        let npeers = Runtime::num_workers();
        let mailboxes: Vec<Mutex<Option<Mailbox<T>>>> =
            (0..npeers * npeers).map(|_| Default::default()).collect();

        let layout = runtime.layout();
        let npeers = layout.n_workers();

        let exchange = Arc::new(Self {
            exchange_id,
            name: OperatorName::new("ExchangeReceiver"),
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
            activity: if runtime.dev_tweaks().optimize_input_during_commit()
                && !runtime.allow_input_during_commit()
            {
                activity
            } else {
                ExchangeActivity::AllSteps
            },
        });

        directory.insert(exchange_id, exchange.clone());

        exchange
    }

    pub fn exchange_id(&self) -> ExchangeId {
        self.exchange_id
    }

    /// Returns an index for the sender/receiver pair.
    fn mailbox_index(&self, sender: usize, receiver: usize) -> usize {
        debug_assert!(sender < self.npeers);
        debug_assert!(receiver < self.npeers);
        sender * self.npeers + receiver
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
    pub(crate) fn with_runtime(
        runtime: &Runtime,
        exchange_id: ExchangeId,
        activity: ExchangeActivity,
    ) -> Arc<Self> {
        // It's tempting to move the following calls to create the
        // `ExchangeDirectory` and `ExchangeClients` into `Exchange::new`, but
        // don't do it: all three of these access `runtime.local_store` and
        // nesting them creates deadlocks at runtime.
        let directory = ExchangeDirectory::for_runtime(runtime);
        let clients = ExchangeClients::for_runtime(runtime);
        runtime
            .local_store()
            .entry(ExchangeCacheId::new(exchange_id))
            .or_insert_with(|| Exchange::new(runtime, clients, exchange_id, &directory, activity))
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
        global_node_id: &Arc<String>,
        data: impl Iterator<Item = T>,
        mut serialize: F,
    ) where
        F: FnMut(T) -> FBuf + Send + Sync,
    {
        self.send_all(
            global_node_id,
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
    pub(crate) async fn send_all(
        self: &Arc<Self>,
        global_node_id: &Arc<String>,
        mut data: impl Iterator<Item = Mailbox<T>>,
    ) {
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
                    let items = receivers
                        .clone()
                        .map(|_| {
                            data.next()
                                .expect("data should include one item per peer")
                                .into_tx()
                                .expect("remote mailboxes should always be serialized")
                        })
                        .collect_vec();

                    let message_type = MessageType::Synchronous(MessageSize::from_bytes(
                        items.iter().map(|fbuf| fbuf.len()).sum(),
                    ));

                    // We discard the return value that could allow us to wait
                    // for the channel tx buffer to drain, because exchange is
                    // synchronous, meaning that it will drain before we send
                    // the next message.
                    let _ = self
                        .clients
                        .connect(receivers.start, message_type)
                        .await
                        .send(global_node_id.clone(), self.exchange_id, sender, items);
                }
            }
        }
    }

    /// Read all incoming messages for this worker, waiting for data to arrive
    /// as needed.
    ///
    /// When the data is ready, but before reading it, this method swaps
    /// `start_wait_usecs` with 0 and returns the old value along with the data.
    /// This allows the caller to obtain the waiting time incurred just after it
    /// became ready.
    pub(crate) async fn receive_all<D>(
        &self,
        deserialize: D,
        start_wait_usecs: Option<&AtomicU64>,
    ) -> (Vec<T>, Option<u64>)
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

        let start_wait_usecs = start_wait_usecs.and_then(|v| {
            let start_wait_usecs = v.swap(0, Ordering::Acquire);
            (start_wait_usecs != 0).then_some(start_wait_usecs)
        });

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

        (data, start_wait_usecs)
    }
}

impl<T> ExchangeDelivery for Exchange<T>
where
    T: Clone + Debug + Send + 'static,
{
    fn name(&self) -> Arc<String> {
        self.name.get()
    }

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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Phase {
    Active,
    Flush,
    Commit,
}

impl Phase {
    fn is_inactive(&self, activity: ExchangeActivity) -> bool {
        *self == Phase::Commit && activity == ExchangeActivity::InputOnly
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
///     operator::communication::{ExchangeActivity, Mailbox},
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
///             ExchangeActivity::AllSteps,
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
    global_node_id: Arc<String>,
    location: OperatorLocation,
    partition: L,
    outputs: Vec<Mailbox<T>>,
    exchange: Arc<Exchange<(T, bool)>>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    phase: Phase,

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
            global_node_id: Arc::new(format!("ExchangeSender {}", exchange.exchange_id)),
            location,
            partition,
            outputs: Vec::with_capacity(Runtime::num_workers()),
            exchange,
            input_batch_stats: BatchSizeStats::new(),
            phase: Phase::Active,
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

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_node_id = Arc::new(format!("ExchangeSender {}", global_id.node_identifier()));
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

    fn start_transaction(&mut self) {
        self.phase = Phase::Active;
    }

    fn flush(&mut self) {
        self.phase = Phase::Flush;
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
        if self.phase.is_inactive(self.exchange.activity) {
            assert_eq!(
                input.num_entries_deep(),
                0,
                "cannot process input received during commit (see [CircuitConfig::allow_input_during_commit] for more information)"
            );
            return;
        };
        let flushed = if self.phase == Phase::Flush {
            self.phase = Phase::Commit;
            true
        } else {
            false
        };

        self.input_batch_stats.add_batch(input.num_entries_deep());

        debug_assert!(self.ready());
        self.outputs.clear();
        (self.partition)(input, &mut self.outputs);
        self.start_wait_usecs
            .store(current_time_usecs(), Ordering::Release);

        let data = self.outputs.drain(..).map(|mailbox| match mailbox {
            Mailbox::Tx(mut data) => {
                data.push(flushed as u8);
                Mailbox::Tx(data)
            }
            Mailbox::Rx(_) => unreachable!(),
            Mailbox::Plain(item) => Mailbox::Plain((item, flushed)),
        });

        self.exchange.send_all(&self.global_node_id, data).await;
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
pub struct ExchangeReceiver<IF, T, L, D>
where
    T: Send + 'static + Clone,
{
    location: OperatorLocation,
    init: IF,
    deserialize: D,
    combine: L,
    exchange: Arc<Exchange<(T, bool)>>,
    flush_count: usize,
    flush_complete: bool,
    start_wait_usecs: Arc<AtomicU64>,
    total_wait_time: Arc<AtomicU64>,
    phase: Phase,

    // Output batch sizes.
    output_batch_stats: BatchSizeStats,
}

impl<IF, T, L, D> ExchangeReceiver<IF, T, L, D>
where
    T: Send + 'static + Clone + Debug,
{
    pub(crate) fn new(
        location: OperatorLocation,
        exchange: Arc<Exchange<(T, bool)>>,
        init: IF,
        start_wait_usecs: Arc<AtomicU64>,
        deserialize: D,
        combine: L,
    ) -> Self {
        Self {
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
            phase: Phase::Active,
        }
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

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.exchange.name.init(global_id);
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

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn start_transaction(&mut self) {
        self.phase = Phase::Active;
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
        if self.phase.is_inactive(self.exchange.activity) {
            return (self.init)();
        }

        let deserialize = |mut vec: AlignedVec| {
            let flushed = pop_flushed(&mut vec);
            let value = (self.deserialize)(vec);
            (value, flushed)
        };

        let mut combined = (self.init)();
        let (res, start_wait_usecs) = self
            .exchange
            .receive_all(deserialize, Some(&self.start_wait_usecs))
            .await;
        if let Some(start_wait_usecs) = start_wait_usecs {
            self.total_wait_time.fetch_add(
                current_time_usecs().saturating_sub(start_wait_usecs),
                Ordering::Release,
            );
        }
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
            self.phase = Phase::Commit;
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

/// The microsteps during which an exchange is active.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ExchangeActivity {
    /// The exchange is active in every microstep.
    ///
    /// This includes pre-commit and commit microstep.
    AllSteps,

    /// The exchange is active only during pre-commit microsteps.
    ///
    /// This allows for optimizations for exchanges used for sharding data from
    /// input operators, which don't exchange any data during commit.
    ///
    /// # Limitation
    ///
    /// The current implementation only works for operators that flush in the
    /// same (micro)step in every worker.  This is true for input operators,
    /// which flush as soon as the transaction starts committing, but it is not
    /// necessarily true for other operators.
    InputOnly,
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
    activity: ExchangeActivity,
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

    let exchange_id = runtime.sequence_next().try_into().unwrap();
    let start_wait_usecs = Arc::new(AtomicU64::new(0));
    let exchange = Exchange::with_runtime(&runtime, exchange_id, activity);
    let sender = ExchangeSender::new(
        location,
        exchange.clone(),
        start_wait_usecs.clone(),
        partition,
    );
    let receiver = ExchangeReceiver::new(
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
fn inject_fault(kind: impl Display) -> bool {
    use rand::Rng as _;

    if rand::thread_rng().gen_range(0..100) == 0 {
        warn!("injecting failure: {kind}");
        true
    } else {
        false
    }
}

#[cfg(not(test))]
fn inject_fault(_kind: impl Display) -> bool {
    false
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
            communication::{ExchangeActivity, Mailbox, new_exchange_operators},
        },
        storage::file::{to_bytes, to_bytes_dyn},
        trace::aligned_deserialize,
        utils::test::init_test_logger,
    };
    use std::{
        iter::{repeat, zip},
        net::TcpListener,
        sync::Arc,
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
        let exchange =
            Exchange::with_runtime(&Runtime::runtime().unwrap(), 0, ExchangeActivity::AllSteps);
        TOKIO.block_on(async {
            let sender = Runtime::worker_index();
            let n_workers = Runtime::num_workers();
            let global_node_id = Arc::new(String::from("test_global_node_id"));
            for round in 0..ROUNDS {
                exchange
                    .send_all_with_serializer(&global_node_id, repeat((sender, round)), |data| {
                        to_bytes(&data).unwrap()
                    })
                    .await;

                let (received, _) = exchange
                    .receive_all(|data| aligned_deserialize(&data[..]), None)
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
        init_test_logger();
        for workers in [2, 4, 8] {
            test_circuit(workers, 1, circuit);
        }
    }

    // Test an exchange object with multiple concurrent senders/receivers on multiple hosts.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn multihost() {
        init_test_logger();
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
                ExchangeActivity::AllSteps,
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
        init_test_logger();
        test_operators_single_host(operator_circuit::<DynamicScheduler>);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn operators_multihost_dynamic() {
        init_test_logger();
        test_operators_multihost(operator_circuit::<DynamicScheduler>);
    }
}
