use std::{
    borrow::Cow,
    collections::VecDeque,
    iter::repeat_n,
    ops::Range,
    panic::Location,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use feldera_samply::Span;
use itertools::{Itertools as _, zip_eq};
use rkyv::AlignedVec;
use size_of::{HumanBytes, SizeOf, TotalSize};

use crate::{
    Circuit, NumEntries, Runtime, Scope, Stream,
    circuit::{
        GlobalNodeId, NodeId, OwnershipPreference, StepSize, WorkerLocation, WorkerLocations,
        circuit_builder::{MetadataExchange, StreamId},
        metadata::{
            ALLOCATED_MEMORY_BYTES, BatchSizeStats, INPUT_BATCHES_STATS, MEMORY_ALLOCATIONS_COUNT,
            MetaItem, OUTPUT_BATCHES_STATS, OperatorLocation, OperatorMeta, SHARED_MEMORY_BYTES,
            SPINE_COUNT, STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, OperatorName, SinkOperator, SourceOperator},
    },
    circuit_cache_key,
    operator::{
        communication::{
            ExchangeClients, ExchangeDelivery, ExchangeDirectory, ExchangeId, MessageType,
            pop_flushed,
        },
        dynamic::{
            accumulator::{Accumulation, EnableCount},
            shard_batch,
        },
    },
    trace::{Batch, BatchReader as _, Spine, Trace, TraceRole, deserialize_indexed_wset},
};

circuit_cache_key!(local StreamingExchangeCacheId<B: Batch>(ExchangeId => Arc<ShardedAccumulator<B>>));

circuit_cache_key!(ShardedAccumulatorId<C, B: Batch>((StreamId, Range<usize>) => Accumulation<Stream<C, Option<Spine<B>>>>));

circuit_cache_key!(ShardedAccumulatorRemoteWaiterId(() => NodeId));

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    /// Implements a fused shard-accumulator operation, equivalent to
    /// `self.dyn_shard().dyn_accumulate()` but intended to be more efficient.
    #[track_caller]
    pub fn dyn_shard_accumulate(
        &self,
        factories: &B::Factories,
    ) -> Accumulation<Stream<C, Option<Spine<B>>>>
    where
        B: Batch<Time = ()>,
    {
        self.dyn_shard_workers_accumulate(factories, 0..Runtime::num_workers())
    }

    #[track_caller]
    pub fn dyn_shard_workers_accumulate(
        &self,
        factories: &B::Factories,
        workers: Range<usize>,
    ) -> Accumulation<Stream<C, Option<Spine<B>>>>
    where
        B: Batch<Time = ()>,
    {
        if !self.has_workers_sharded_version(workers.clone())
            && Runtime::with_dev_tweaks(|d| d.streaming_exchange())
            && let Some(runtime) = Runtime::runtime()
            && runtime.layout().n_workers() > 1
            && runtime.step_size() == StepSize::Microsteps
        {
            let remote_waiter_node_id = if runtime.layout().is_multihost() {
                let clients = ExchangeClients::for_runtime(&runtime);
                Some(*self.circuit().cache_get_or_insert_with(
                    ShardedAccumulatorRemoteWaiterId::new(()),
                    move || {
                        let waiter = self
                            .circuit()
                            .add_source(ShardedAccumulatorRemoteWaiter::new(clients.clone()));
                        waiter.local_node_id()
                    },
                ))
            } else {
                None
            };

            self.circuit()
                .cache_get_or_insert_with(
                    ShardedAccumulatorId::new((self.stream_id(), workers.clone())),
                    || {
                        let exchange_id: ExchangeId = runtime.sequence_next().try_into().unwrap();
                        let exchange = ShardedAccumulator::<B>::with_runtime(
                            &runtime,
                            workers.clone(),
                            exchange_id,
                            factories,
                        );
                        let enable_count = exchange.enable_count.clone();
                        let local_waiter =
                            self.circuit()
                                .add_source(ShardedAccumulatorLocalWaiter::new(
                                    Some(Location::caller()),
                                    exchange.clone(),
                                ));
                        let receiver = self
                            .circuit()
                            .add_exchange(
                                ShardedAccumulatorSender::new(
                                    Some(Location::caller()),
                                    exchange.clone(),
                                    // Only the root circuit exchanges metadata.
                                    (self.circuit().root_scope() == 0)
                                        .then(|| self.circuit().metadata_exchange().clone()),
                                ),
                                ShardedAccumulatorReceiver::new(Some(Location::caller()), exchange),
                                self,
                            )
                            .mark_sharded_workers(workers.clone());
                        self.circuit()
                            .add_dependency(receiver.local_node_id(), local_waiter.local_node_id());
                        if let Some(remote_waiter_node_id) = remote_waiter_node_id {
                            self.circuit()
                                .add_dependency(receiver.local_node_id(), remote_waiter_node_id);
                        }
                        Accumulation {
                            stream: receiver,
                            enable_count,
                        }
                    },
                )
                .clone()
        } else {
            self.dyn_shard_workers(workers, factories)
                .dyn_accumulate(factories)
        }
    }
}

struct ShardedAccumulator<B>
where
    B: Batch,
{
    runtime: Runtime,
    name: OperatorName,
    exchange_id: ExchangeId,

    /// The destination workers.
    ///
    /// Usually this is all workers, but output operators shard to the workers
    /// on one particular host.
    workers: Range<usize>,

    factories: B::Factories,

    /// Range of worker IDs on the local host.
    local_workers: Range<usize>,

    /// The RPC clients to contact remote hosts.
    clients: Arc<ExchangeClients>,

    /// One [Rxq] for each of `local_workers`.
    rxq: Vec<Mutex<Rxq<B>>>,

    enable_count: EnableCount,
}

impl<B> ShardedAccumulator<B>
where
    B: Batch<Time = ()>,
{
    fn with_runtime(
        runtime: &Runtime,
        workers: Range<usize>,
        exchange_id: ExchangeId,
        factories: &B::Factories,
    ) -> Arc<Self> {
        // It's tempting to move the following calls to create the
        // `ExchangeDirectory` and `ExchangeClients` into
        // `ShardedAccumulator::new`, but don't do it: all three of these access
        // `runtime.local_store` and nesting them creates deadlocks at runtime.
        let directory = ExchangeDirectory::for_runtime(runtime);
        let clients = ExchangeClients::for_runtime(runtime);
        runtime
            .local_store()
            .entry(StreamingExchangeCacheId::new(exchange_id))
            .or_insert_with(|| {
                ShardedAccumulator::new(
                    runtime,
                    workers,
                    clients,
                    exchange_id,
                    &directory,
                    factories,
                )
            })
            .value()
            .clone()
    }

    /// Create a new streaming exchange operator to shard and accumulate to the
    /// given `workers` (which is often all the workers in the runtime).
    fn new(
        runtime: &Runtime,
        workers: Range<usize>,
        clients: Arc<ExchangeClients>,
        exchange_id: ExchangeId,
        directory: &ExchangeDirectory,
        factories: &B::Factories,
    ) -> Arc<Self> {
        let layout = runtime.layout();
        let npeers = layout.n_workers();

        let name = OperatorName::new("ShardedAccumulatorReceiver");
        let exchange = Arc::new(Self {
            runtime: runtime.clone(),
            exchange_id,
            workers,
            local_workers: layout.local_workers(),
            factories: factories.clone(),
            clients,
            rxq: layout
                .local_workers()
                .map(|receiver| {
                    Mutex::new(Rxq::new(runtime, receiver, factories, npeers, name.get()))
                })
                .collect(),
            name,
            enable_count: EnableCount::default(),
        });

        directory.insert(exchange_id, exchange.clone());

        exchange
    }

    fn rxq(&self, receiver: usize) -> MutexGuard<'_, Rxq<B>> {
        assert!(self.local_workers.contains(&receiver));
        self.rxq[receiver - self.local_workers.start]
            .lock()
            .unwrap()
    }

    /// Delivers `batch`, sent by local or remote `sender`, to local worker
    /// `receiver`.  If `flush` is true, this is the final batch in the
    /// transaction.  Returns true if the receiving spine has too many batches.
    fn deliver(
        &self,
        factories: &B::Factories,
        sender: usize,
        receiver: usize,
        batch: B,
        flush: bool,
    ) -> bool {
        // Spill the batch to disk, if we should, without taking the rxq lock.
        let batch =
            Spine::maybe_flush_batch(Some(&self.runtime), batch, factories, || (None, None));
        if flush || !batch.is_empty() {
            self.rxq(receiver).deliver(factories, sender, batch, flush)
        } else {
            false
        }
    }

    async fn send(self: &Arc<Self>, name: Arc<String>, batch: B, flush: bool) {
        let sender = Runtime::worker_index();

        let runtime = Runtime::runtime().unwrap();
        let layout = runtime.layout();
        let mut builders = Vec::with_capacity(layout.n_workers());
        let mut batches = Vec::with_capacity(layout.n_workers());
        shard_batch(
            batch,
            &self.workers,
            &mut builders,
            &mut batches,
            &self.factories,
        );
        let worker_locations = WorkerLocations::for_layout(layout);
        let mut data = batches.into_iter();
        let mut remote_waiters = Vec::new();
        let mut serialized_bytes = 0;
        for receivers in layout.all_hosts() {
            match worker_locations[receivers.start] {
                WorkerLocation::Local => {
                    for receiver in receivers.clone() {
                        let item = data
                            .next()
                            .expect("data should include one item per peer")
                            .into_plain()
                            .expect("local data should not be serialized");
                        self.deliver(&self.factories, sender, receiver, item, flush);
                    }
                }
                WorkerLocation::Remote => {
                    let mut empty = true;
                    let mut items = Vec::with_capacity(receivers.len());
                    for _ in receivers.clone() {
                        let fbuf = data
                            .next()
                            .expect("data should include one item per peer")
                            .into_tx()
                            .expect("remote mailboxes should always be serialized");
                        if !fbuf.is_empty() {
                            serialized_bytes += fbuf.len();
                            empty = false;
                        }
                        items.push(fbuf);
                    }

                    // Skip sending to the remote host if there's no data to
                    // send (unless we're flushing).
                    //
                    // The common case for no data to send is when we're
                    // sharding to workers on only one host for an output
                    // connector.
                    if !empty || flush {
                        for item in &mut items {
                            item.push(flush as u8);
                        }
                        let this = self.clone();
                        if let Some(waiter) = this
                            .clients
                            .connect(receivers.start, MessageType::Streaming)
                            .await
                            .send(name.clone(), this.exchange_id, sender, items)
                        {
                            remote_waiters.push(waiter);
                        }
                    }
                }
            }
        }

        if !remote_waiters.is_empty() {
            let _span = Span::new("remote send wait")
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "{name} wait for {} to drain from {} tx buffers",
                        HumanBytes::from(serialized_bytes),
                        remote_waiters.len()
                    )
                });
            for waiter in remote_waiters {
                waiter.await;
            }
        }
    }

    fn receive(&self) -> Option<Spine<B>> {
        let receiver = Runtime::worker_index();
        self.rxq(receiver).receive()
    }

    fn set_name(&self, global_id: &GlobalNodeId) {
        self.name.init(global_id);
        for rxq in &self.rxq {
            rxq.lock().unwrap().set_name(self.name.get());
        }
    }
}

impl<B> ShardedAccumulator<B>
where
    B: Batch,
{
    async fn wait(&self, name: Arc<String>) {
        let start = Instant::now();
        let mut local_waiters = Vec::new();
        for (rxq, worker) in self.rxq.iter().zip(self.local_workers.clone()) {
            // This is intentionally two separate statements to avoid holding
            // the lock while waiting.
            let waiter = rxq
                .lock()
                .unwrap()
                .spines
                .front()
                .and_then(|entry| entry.spine.backpressure_waiter());
            if let Some((notified, report)) = waiter {
                local_waiters.push(worker);
                notified.await;
                // Report it, so the wait reaches
                // `merge_backpressure_wait_time_seconds` and the profile rather
                // than only showing up as time spent in this operator.
                if let Some(entry) = rxq.lock().unwrap().spines.front() {
                    entry.spine.record_backpressure_wait(report);
                }
            }
        }
        if !local_waiters.is_empty() {
            Span::new("local send wait")
                .with_start(start)
                .with_category("Exchange")
                .with_tooltip(|| {
                    format!(
                        "{name} wait for batches to merge in {} receive queues (for workers {})",
                        local_waiters.len(),
                        local_waiters.iter().format(", ")
                    )
                })
                .record();
        }
    }
}

impl<B> ExchangeDelivery for ShardedAccumulator<B>
where
    B: Batch<Time = ()>,
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
            for (receiver, mut data) in zip_eq(self.local_workers.clone(), data) {
                let flush = pop_flushed(&mut data);
                let batch = deserialize_indexed_wset(&self.factories, &data);
                self.deliver(&self.factories, sender, receiver, batch, flush);
            }
        })
    }
}

/// Queues data to a [ShardedAccumulatorReceiver].
struct Rxq<B>
where
    B: Batch,
{
    /// The runtime in which we're embedded.
    runtime: Runtime,

    /// Our worker index within the runtime.
    worker_index: usize,

    /// Total number of worker threads (senders) in this circuit.
    ///
    /// This is important because each sender must send, in addition to any
    /// data, a flush notification to indicate that all data for a step has been
    /// sent.
    npeers: usize,

    /// A deque of spines under construction for delivery to the circuit.  The
    /// first element will be delivered to the circuit next, the second element
    /// after that, and so on.
    ///
    /// In the common case, the deque has one element; it will never be empty.
    spines: VecDeque<RxqEntry<B>>,

    /// For each sender, the number of flushes it has sent.
    n_flushes: Vec<usize>,

    /// The number of entries that have been popped off `spines` and received by
    /// the circuit.
    n_received: usize,

    /// Name for use in profiles.
    name: Arc<String>,
}

/// A spine that a [ShardedAccumulatorReceiver] is building from batches
/// received from [ShardedAccumulatorSender]s.
struct RxqEntry<B>
where
    B: Batch,
{
    /// Number of senders that have not yet sent a flush notification.  This
    /// starts as the number of workers in the circuit and decrements with each
    /// flush.  When it reaches zero, every sender has sent a flush
    /// notification, so `spine` is ready to be delivered to the circuit.
    n_unflushed: usize,

    /// Spine under construction.
    spine: Spine<B>,
}

impl<B> RxqEntry<B>
where
    B: Batch,
{
    fn new(
        npeers: usize,
        runtime: &Runtime,
        worker_index: usize,
        factories: &B::Factories,
        name: Arc<String>,
    ) -> Self {
        Self {
            n_unflushed: npeers,
            spine: Spine::with_runtime(
                runtime.clone(),
                worker_index,
                factories,
                name,
                TraceRole::Accumulator,
            ),
        }
    }
}

impl<B> Rxq<B>
where
    B: Batch,
{
    fn new(
        runtime: &Runtime,
        worker_index: usize,
        factories: &B::Factories,
        npeers: usize,
        name: Arc<String>,
    ) -> Self {
        Self {
            runtime: runtime.clone(),
            worker_index,
            npeers,
            spines: VecDeque::from([RxqEntry::new(
                npeers,
                runtime,
                worker_index,
                factories,
                name.clone(),
            )]),
            name,
            n_flushes: repeat_n(0, npeers).collect(),
            n_received: 0,
        }
    }

    fn deliver(
        &mut self,
        factories: &B::Factories,
        sender: usize,
        batch: Arc<B>,
        flush: bool,
    ) -> bool {
        let index = self.n_flushes[sender] - self.n_received;
        let entry = &mut self.spines[index];

        let should_block = entry.spine.insert_without_blocking(batch);

        if flush {
            entry.n_unflushed -= 1;
            self.n_flushes[sender] += 1;
            if index + 1 >= self.spines.len() {
                self.spines.push_back(RxqEntry::new(
                    self.npeers,
                    &self.runtime,
                    self.worker_index,
                    factories,
                    self.name.clone(),
                ));
            }
        }

        should_block
    }

    fn receive(&mut self) -> Option<Spine<B>> {
        self.spines
            .pop_front_if(|entry| entry.n_unflushed == 0)
            .map(|entry| {
                self.n_received += 1;
                entry.spine
            })
    }

    fn set_name(&mut self, name: Arc<String>) {
        for entry in &mut self.spines {
            entry.spine.set_name(name.clone());
        }
        self.name = name;
    }
}

struct ShardedAccumulatorSender<B>
where
    B: Batch,
{
    location: OperatorLocation,
    local_node_id: NodeId,
    name: OperatorName,
    exchange: Arc<ShardedAccumulator<B>>,

    /// Used to share `enable_count` with the other hosts, in the root circuit.
    /// `None` in a nested circuit, which does not exchange metadata.
    metadata_exchange: Option<MetadataExchange>,

    /// Whether the accumulator is enabled during the current transaction.
    ///
    /// An output connector can be attached in the middle of a transaction; however if the
    /// accumulator was disabled at the start of the transaction, it shouldn't produce
    /// partial outputs. This flag remembers the status of the accumulator at the start of the
    /// transaction.
    enabled_during_current_transaction: Option<bool>,

    /// Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flushed: bool,
}

impl<B> ShardedAccumulatorSender<B>
where
    B: Batch,
{
    fn new(
        location: OperatorLocation,
        exchange: Arc<ShardedAccumulator<B>>,
        metadata_exchange: Option<MetadataExchange>,
    ) -> Self {
        Self {
            location,
            name: OperatorName::new("ShardedAccumulatorSender"),
            exchange,
            local_node_id: NodeId::root(),
            metadata_exchange,
            enabled_during_current_transaction: None,
            input_batch_stats: BatchSizeStats::new(),
            flushed: false,
        }
    }
}

impl<B> Operator for ShardedAccumulatorSender<B>
where
    B: Batch,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ShardedAccumulatorSender")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.local_node_id = global_id.local_node_id().unwrap();
        self.name.init(global_id);
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

    fn start_transaction(&mut self) {
        if let Some(metadata_exchange) = &self.metadata_exchange
            && Runtime::local_worker_offset() == 0
        {
            metadata_exchange.set_local_operator_metadata_typed(
                self.local_node_id,
                self.exchange.enable_count.is_enabled(),
            );
        }
    }
}

impl<B> ShardedAccumulatorSender<B>
where
    B: Batch<Time = ()>,
{
    async fn eval_inner<'a>(&mut self, batch: Cow<'a, B>) {
        // Sample enable_count when we get the first non-empty batch in a
        // transaction.  This batch should belong to the next transaction after
        // the last one that was flushed, since the accumulator should not
        // receive any non-empty batches from the previous transaction at that
        // point (in the top-level circuit).  This may not be the first batch in
        // the transaction, but it's ok to admit some empty batches.
        let len = batch.approximate_len();
        if (len > 0 || self.flushed) && self.enabled_during_current_transaction.is_none() {
            self.enabled_during_current_transaction = Some(match &self.metadata_exchange {
                Some(metadata_exchange) => metadata_exchange
                    .get_global_operator_metadata_typed(self.local_node_id)
                    .into_iter()
                    .any(|enable| enable == Some(true)),
                None => self.exchange.enable_count.is_enabled(),
            });
        }
        let Some(enabled) = self.enabled_during_current_transaction else {
            return;
        };

        if enabled {
            self.input_batch_stats.add_batch(len);
            self.exchange
                .send(self.name.get(), batch.into_owned(), self.flushed)
                .await;
        }

        if self.flushed {
            if !enabled {
                let batch = B::dyn_empty(&self.exchange.factories);
                self.exchange.send(self.name.get(), batch, true).await;
            }
            self.flushed = false;
            self.enabled_during_current_transaction = None;
        }
    }
}

impl<B> SinkOperator<B> for ShardedAccumulatorSender<B>
where
    B: Batch<Time = ()>,
{
    async fn eval(&mut self, batch: &B) {
        self.eval_inner(Cow::Borrowed(batch)).await
    }

    async fn eval_owned(&mut self, batch: B) {
        self.eval_inner(Cow::Owned(batch)).await
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

struct ShardedAccumulatorRemoteWaiter {
    clients: Arc<ExchangeClients>,
}

impl Operator for ShardedAccumulatorRemoteWaiter {
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("ShardedAccumulatorRemoteWaiter")
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        true
    }
}

impl SourceOperator<()> for ShardedAccumulatorRemoteWaiter {
    async fn eval(&mut self) {
        self.clients.wait(MessageType::Streaming).await;
    }
}

impl ShardedAccumulatorRemoteWaiter {
    fn new(clients: Arc<ExchangeClients>) -> Self {
        Self { clients }
    }
}

struct ShardedAccumulatorLocalWaiter<B>
where
    B: Batch,
{
    exchange: Arc<ShardedAccumulator<B>>,
    location: OperatorLocation,
    name: OperatorName,
}

impl<B> ShardedAccumulatorLocalWaiter<B>
where
    B: Batch,
{
    fn new(location: OperatorLocation, exchange: Arc<ShardedAccumulator<B>>) -> Self {
        Self {
            exchange,
            location,
            name: OperatorName::new("ShardedAccumulatorLocalWaiter"),
        }
    }
}

impl<B> Operator for ShardedAccumulatorLocalWaiter<B>
where
    B: Batch,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("ShardedAccumulatorLocalWaiter")
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.name.init(global_id);
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        true
    }
}

impl<B> SourceOperator<()> for ShardedAccumulatorLocalWaiter<B>
where
    B: Batch,
{
    async fn eval(&mut self) {
        self.exchange.wait(self.name.get()).await;
    }
}

struct ShardedAccumulatorReceiver<B>
where
    B: Batch,
{
    exchange: Arc<ShardedAccumulator<B>>,
    output_batch_stats: BatchSizeStats,
    location: OperatorLocation,
    flushed: bool,
}

impl<B> ShardedAccumulatorReceiver<B>
where
    B: Batch,
{
    fn new(location: OperatorLocation, exchange: Arc<ShardedAccumulator<B>>) -> Self {
        Self {
            exchange,
            output_batch_stats: BatchSizeStats::new(),
            location,
            flushed: false,
        }
    }
}

impl<B> Operator for ShardedAccumulatorReceiver<B>
where
    B: Batch<Time = ()>,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("ShardedAccumulatorReceiver")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.exchange.set_name(global_id);
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flushed = false;
    }

    fn is_flush_complete(&self) -> bool {
        self.flushed
    }

    fn ready(&self) -> bool {
        // This operator does not fit well into the DBSP evaluation model.  It
        // only has anything useful to contribute when it has received a flush.
        // At any other time, it can immediately evaluate to `None`.  However:
        //
        // - If it only reports that it is ready when it has received a flush,
        //   then it will prevent the step from completing until it has.  This
        //   will deadlock because the flush will never be received (since
        //   generally it takes more than one step to flush).
        //
        // - If it does what it does here, and always reports that it is ready,
        //   then this could cause livelock, spinning uselessly with 100% CPU,
        //   if there's nothing for the circuit to do while data transmits.
        //
        // `ShardedAccumulatorReceiver` operators in workers that are outside
        // the set of workers being sharded to seem like a further special case,
        // since we know in advance that they won't receive any data, but I
        // don't see a way to do better with them.
        //
        // I don't know the right solution.
        true
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let rxq = self.exchange.rxq(Runtime::worker_index());

        let mut total_size = 0;
        let mut bytes = TotalSize::zero();
        let mut n_spines = 0;
        for (index, spine) in rxq.spines.iter().map(|entry| &entry.spine).enumerate() {
            if index == 0 {
                spine.metadata(meta);
            }
            n_spines += 1;
            total_size += spine.num_entries_deep();
            bytes += spine.size_of();
        }

        meta.extend(metadata! {
            SPINE_COUNT =>  MetaItem::Count(n_spines),
            STATE_RECORDS_COUNT => MetaItem::Count(total_size),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
            OUTPUT_BATCHES_STATS => self.output_batch_stats.metadata(),
        });
    }
}

impl<B> SourceOperator<Option<Spine<B>>> for ShardedAccumulatorReceiver<B>
where
    B: Batch<Time = ()>,
{
    async fn eval(&mut self) -> Option<Spine<B>> {
        let output = self.exchange.receive();
        if let Some(spine) = &output {
            self.output_batch_stats.add_batch(spine.approximate_len());
            spine.backpressure_wait().await;
            self.flushed = true;
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::thread;
    use itertools::Itertools;

    use crate::{
        DBSPHandle, OrdZSet, OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
        circuit::{CircuitConfig, Layout, Runtime},
        dynamic::{Data, DowncastTrait, DynWeightTyped},
        operator::dynamic::accumulator::EnableCount,
        trace::{BatchReader, Cursor, FallbackWSet, Spine},
        typed_batch::TypedBatch,
        utils::Tup2,
    };
    use std::{collections::BTreeMap, iter::zip, net::TcpListener};

    /// Number of rounds for streaming exchange.
    ///
    /// We do fewer rounds for streaming exchange because the `n`th round does
    /// `n` steps and exchanges `O(n**2)` data.
    const STREAMING_ROUNDS: usize = 64;

    type TestOutput = OutputHandle<
        TypedBatch<usize, (), i64, Spine<FallbackWSet<dyn Data + 'static, DynWeightTyped<i64>>>>,
    >;

    /// How the test circuit gathers its output.
    #[derive(Copy, Clone, Debug)]
    struct Gather {
        /// Enables the streaming exchange.  Without it, `shard_accumulate`
        /// falls back to a plain shard and [Accumulator](super::super::accumulator::Accumulator).
        streaming_exchange: bool,

        /// Gathers to the first host's workers only, the way a multihost
        /// pipeline gathers a view to the host that owns its connectors.
        /// Otherwise, gathers to every worker.
        to_first_host: bool,
    }

    impl Gather {
        /// Gathers to every worker with `ShardedAccumulator`.
        const ALL_WORKERS: Self = Self {
            streaming_exchange: true,
            to_first_host: false,
        };
    }

    fn test_config(workers: usize, gather: Gather) -> CircuitConfig {
        CircuitConfig::with_workers(workers).with_streaming_exchange(gather.streaming_exchange)
    }

    /// The runtimes of a test circuit, one per host, with each host's handles.
    struct TestHosts {
        dbsp_handles: Vec<DBSPHandle>,
        input_handles: Vec<ZSetHandle<usize>>,
        output_handles: Vec<TestOutput>,

        /// Each host's enable count for the accumulator.  The accumulator
        /// starts out disabled on every host.
        enable_counts: Vec<EnableCount>,
    }

    impl TestHosts {
        /// Starts [circuit] with `workers` workers spread over `hosts` hosts.
        fn new(workers: usize, hosts: usize) -> Self {
            Self::with_gather(workers, hosts, Gather::ALL_WORKERS)
        }

        /// Like [TestHosts::new], with the circuit gathering its output as
        /// `gather` says.
        fn with_gather(workers: usize, hosts: usize, gather: Gather) -> Self {
            let (dbsp_handles, input_handles, output_handles, enable_counts) = match hosts {
                0 => unreachable!(),
                1 => {
                    let (dbsp_handle, (input_handle, output_handle, enable_count)) =
                        Runtime::init_circuit(test_config(workers, gather), move |c| {
                            circuit(c, gather)
                        })
                        .expect("failed to start runtime");
                    (
                        vec![dbsp_handle],
                        vec![input_handle],
                        vec![output_handle],
                        vec![enable_count],
                    )
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
                    let mut handles = Vec::with_capacity(params.len());
                    for ((local_address, _), exchange_listener) in
                        zip(params.iter(), exchange_listeners)
                    {
                        let cconf = CircuitConfig::from(
                            Layout::new_multihost(&params, *local_address).unwrap(),
                        )
                        .with_exchange_listener(exchange_listener)
                        .with_streaming_exchange(gather.streaming_exchange);

                        let (dbsp_handle, (input_handle, output_handle, enable_count)) =
                            Runtime::init_circuit(cconf, move |c| circuit(c, gather))
                                .expect("failed to start runtime");
                        handles.push((dbsp_handle, input_handle, output_handle, enable_count));
                    }
                    handles.into_iter().multiunzip()
                }
            };
            Self {
                dbsp_handles,
                input_handles,
                output_handles,
                enable_counts,
            }
        }

        /// Executes `f` on every host's handle in parallel and waits for all
        /// of them to complete.
        fn for_each_host<F>(&mut self, f: F)
        where
            F: Fn(&mut DBSPHandle) + Send + Sync + 'static,
        {
            thread::scope(|s| {
                self.dbsp_handles
                    .iter_mut()
                    .map(|h| s.spawn(|_| f(h)))
                    .collect_vec()
                    .into_iter()
                    .for_each(|h| h.join().unwrap())
            })
            .unwrap();
        }

        /// Runs one transaction that inserts the records `0..n_records`,
        /// spread round-robin over the hosts, one record per step.
        ///
        /// Calls `after_first_step` after the first step, for a test that
        /// changes the enable counts in the middle of the transaction.
        ///
        /// # Returns
        ///
        /// The output of the transaction, summed over every host, sorted by
        /// record.
        fn run_transaction(
            &mut self,
            n_records: usize,
            after_first_step: impl FnOnce(&Self),
        ) -> Vec<(usize, ZWeight)> {
            let hosts = self.dbsp_handles.len();
            let mut after_first_step = Some(after_first_step);
            self.for_each_host(|h| h.start_transaction().unwrap());
            for i in 0..n_records {
                self.input_handles[i % hosts].push(i, 1);
                self.for_each_host(|h| {
                    h.step().unwrap();
                });
                if let Some(f) = after_first_step.take() {
                    f(self);
                }
            }
            self.for_each_host(|h| h.commit_transaction().unwrap());

            let mut results = BTreeMap::<usize, ZWeight>::new();
            for spine in self
                .output_handles
                .iter()
                .flat_map(|handle| handle.take_from_all())
            {
                let mut cursor = spine.cursor();
                while let Some(key) = cursor.get_key() {
                    let key = *unsafe { key.downcast() };
                    let weight = *unsafe { cursor.weight().downcast::<ZWeight>() };
                    *results.entry(key).or_default() += weight;
                    cursor.step_key();
                }
            }
            results.into_iter().collect_vec()
        }
    }

    /// The output of a transaction that inserts `0..n_records` with the
    /// accumulator enabled.
    fn expected_output(n_records: usize) -> Vec<(usize, ZWeight)> {
        (0..n_records).map(|i| (i, 1)).collect_vec()
    }

    /// Enables the accumulator on the hosts in `enabled_hosts` and checks
    /// that every transaction gathers the records of every host.
    ///
    /// # Arguments
    ///
    /// * `workers` - total number of workers, across all hosts.
    /// * `hosts` - number of hosts to spread the workers across.
    /// * `enabled_hosts` - indexes of the hosts that enable the accumulator.
    fn test_circuit(workers: usize, hosts: usize, enabled_hosts: &[usize]) {
        test_gather(workers, hosts, Gather::ALL_WORKERS, enabled_hosts);
    }

    /// Like [test_circuit], with the circuit gathering its output as `gather`
    /// says.
    fn test_gather(workers: usize, hosts: usize, gather: Gather, enabled_hosts: &[usize]) {
        let mut test_hosts = TestHosts::with_gather(workers, hosts, gather);
        for &host in enabled_hosts {
            test_hosts.enable_counts[host].enable();
        }
        for round in 0..STREAMING_ROUNDS {
            let n_records = round + 1;
            assert_eq!(
                test_hosts.run_transaction(n_records, |_| ()),
                expected_output(n_records),
                "round {round} with {workers} workers on {hosts} hosts, {gather:?}"
            );
        }
    }

    fn circuit(
        circuit: &mut RootCircuit,
        gather: Gather,
    ) -> anyhow::Result<(ZSetHandle<usize>, TestOutput, EnableCount)> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();

        // An input stream is already sharded, so `shard_accumulate` on it would
        // fall back to a plain shard and accumulate.  `map` yields a stream
        // that is not, so that the test exercises `ShardedAccumulator`.
        let stream = input.map(|x| *x);
        let workers = match Runtime::runtime().unwrap().layout() {
            Layout::Multihost { hosts, .. } if gather.to_first_host => hosts[0].workers.clone(),
            _ => 0..Runtime::num_workers(),
        };
        let (stream, enable_count) = stream.shard_workers_accumulate(workers).into_parts();
        Ok((input_handle, stream.latest_output(), enable_count))
    }

    // Create a circuit with `WORKERS` concurrent workers with the following
    // structure: `Generator - ExchangeSender -> ExchangeReceiver -> Inspect`.
    // `Generator` - yields sequential numbers 0, 1, 2, ...
    // `ExchangeSender` - sends each number to all peers.
    // `ExchangeReceiver` - combines all received numbers in a vector.
    // `Inspect` - validates the output of the receiver.
    #[test]
    fn sharded_accumulator_single_host() {
        for workers in [2, 16, 32] {
            test_circuit(workers, 1, &[0]);
        }
    }

    #[test]
    fn sharded_accumulator_multihost() {
        for (workers, hosts) in [(2, 2), (4, 2), (8, 2), (3, 3), (4, 4), (16, 4)] {
            test_circuit(workers, hosts, &(0..hosts).collect_vec());
        }
    }

    /// An accumulator that only one host enables still gathers every host's
    /// records, the way an output connector on the host that a view is
    /// gathered to enables only that host's accumulator.
    #[test]
    fn sharded_accumulator_multihost_one_host_enables() {
        for (workers, hosts) in [(2, 2), (4, 2), (3, 3), (4, 4)] {
            for enabled_host in [0, hosts - 1] {
                test_circuit(workers, hosts, &[enabled_host]);
            }
        }
    }

    /// A gather to the first host that only that host enables still gathers
    /// every host's records, with and without the streaming exchange.  This
    /// is how a multihost pipeline gathers a view to the host that owns its
    /// output connectors.  Without the streaming exchange, the gather falls
    /// back to a plain shard to the first host and an `Accumulator` there.
    #[test]
    fn sharded_accumulator_multihost_gather_to_first_host() {
        for streaming_exchange in [true, false] {
            let gather = Gather {
                streaming_exchange,
                to_first_host: true,
            };
            for (workers, hosts) in [(2, 2), (4, 2), (3, 3), (4, 4)] {
                test_gather(workers, hosts, gather, &[0]);
            }
        }
    }

    /// A `ShardedAccumulator` in a nested circuit gathers every worker's
    /// records.  Only the root circuit exchanges metadata, so the accumulator
    /// must use its local enable count there.
    ///
    /// A join in a recursive scope uses a `ShardedAccumulator`, so this
    /// finds the nodes that are reachable from node 0 along a chain.
    #[test]
    fn sharded_accumulator_nested_circuit() {
        const N_NODES: usize = 16;
        for workers in [2, 4] {
            let (mut dbsp, (edges_handle, roots_handle, output)) =
                Runtime::init_circuit(test_config(workers, Gather::ALL_WORKERS), |circuit| {
                    let (edges, edges_handle) = circuit.add_input_zset::<Tup2<usize, usize>>();
                    let (roots, roots_handle) = circuit.add_input_zset::<usize>();
                    let reachable =
                        circuit.recursive(|child, reachable: Stream<_, OrdZSet<usize>>| {
                            let edges =
                                edges.delta0(child).map_index(|Tup2(from, to)| (*from, *to));
                            let roots = roots.delta0(child);
                            Ok(reachable
                                .map_index(|node| (*node, ()))
                                .join(&edges, |_from, _, to| *to)
                                .plus(&roots))
                        })?;
                    Ok((edges_handle, roots_handle, reachable.output()))
                })
                .unwrap();

            edges_handle.append(
                &mut (0..N_NODES - 1)
                    .map(|node| Tup2(Tup2(node, node + 1), 1))
                    .collect(),
            );
            roots_handle.push(0, 1);
            dbsp.transaction().unwrap();

            assert_eq!(
                output.consolidate(),
                OrdZSet::from_keys((), (0..N_NODES).map(|node| Tup2(node, 1)).collect()),
                "{workers} workers"
            );
            dbsp.kill().unwrap();
        }
    }

    /// An accumulator that one host enables in the middle of a transaction
    /// outputs nothing in that transaction and every host's records in the
    /// next one.  Disabling it again takes effect at the next transaction in
    /// the same way.
    #[test]
    fn sharded_accumulator_multihost_enable_mid_transaction() {
        const N_RECORDS: usize = 8;
        for (workers, hosts) in [(2, 2), (4, 2), (3, 3)] {
            let mut test_hosts = TestHosts::new(workers, hosts);
            assert_eq!(test_hosts.run_transaction(N_RECORDS, |_| ()), vec![]);

            assert_eq!(
                test_hosts.run_transaction(N_RECORDS, |hosts| hosts.enable_counts[0].enable()),
                vec![],
                "enabled during the transaction, with {workers} workers on {hosts} hosts"
            );
            assert_eq!(
                test_hosts.run_transaction(N_RECORDS, |_| ()),
                expected_output(N_RECORDS),
                "enabled before the transaction, with {workers} workers on {hosts} hosts"
            );

            assert_eq!(
                test_hosts.run_transaction(N_RECORDS, |hosts| hosts.enable_counts[0].disable()),
                expected_output(N_RECORDS),
                "disabled during the transaction, with {workers} workers on {hosts} hosts"
            );
            assert_eq!(
                test_hosts.run_transaction(N_RECORDS, |_| ()),
                vec![],
                "disabled before the transaction, with {workers} workers on {hosts} hosts"
            );
        }
    }
}
