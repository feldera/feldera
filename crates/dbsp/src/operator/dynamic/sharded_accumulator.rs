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
use feldera_storage::fbuf::FBuf;
use itertools::{Itertools as _, zip_eq};
use rkyv::AlignedVec;
use size_of::{HumanBytes, SizeOf, TotalSize};
use tokio::sync::Notify;

use crate::{
    Circuit, NumEntries, Runtime, Scope, Stream,
    circuit::{
        GlobalNodeId, NodeId, OwnershipPreference, StepSize, WorkerLocation, WorkerLocations,
        circuit_builder::StreamId,
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
    profile::{ParkReason, ParkingFor},
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
            let remote_waiter_node_id = self.sharded_accumulator_remote_waiter(&runtime);
            self.circuit()
                .cache_get_or_insert_with(
                    ShardedAccumulatorId::new((self.stream_id(), workers.clone())),
                    || {
                        self.new_sharded_accumulator(
                            &runtime,
                            factories,
                            workers.clone(),
                            remote_waiter_node_id,
                            Location::caller(),
                        )
                        .0
                    },
                )
                .clone()
        } else {
            self.dyn_shard_workers(workers, factories)
                .dyn_accumulate(factories)
        }
    }
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch<Time = ()>,
{
    /// Returns the node ID of the circuit's [ShardedAccumulatorRemoteWaiter],
    /// first adding it if necessary, or `None` on a single-host runtime.
    fn sharded_accumulator_remote_waiter(&self, runtime: &Runtime) -> Option<NodeId> {
        if !runtime.layout().is_multihost() {
            return None;
        }
        let clients = ExchangeClients::for_runtime(runtime);
        Some(*self.circuit().cache_get_or_insert_with(
            ShardedAccumulatorRemoteWaiterId::new(()),
            move || {
                let waiter = self
                    .circuit()
                    .add_source(ShardedAccumulatorRemoteWaiter::new(clients.clone()));
                waiter.local_node_id()
            },
        ))
    }

    /// Adds the operators for a new sharded accumulator over `self` to the
    /// circuit.  Returns the accumulated stream and the [ShardedAccumulator]
    /// that connects the operators.
    fn new_sharded_accumulator(
        &self,
        runtime: &Runtime,
        factories: &B::Factories,
        workers: Range<usize>,
        remote_waiter_node_id: Option<NodeId>,
        location: &'static Location<'static>,
    ) -> (
        Accumulation<Stream<C, Option<Spine<B>>>>,
        Arc<ShardedAccumulator<B>>,
    ) {
        let exchange_id: ExchangeId = runtime.sequence_next().try_into().unwrap();
        let exchange =
            ShardedAccumulator::<B>::with_runtime(runtime, workers.clone(), exchange_id, factories);
        let enable_count = exchange.enable_count.clone();
        let local_waiter = self
            .circuit()
            .add_source(ShardedAccumulatorLocalWaiter::new(
                Some(location),
                exchange.clone(),
            ));
        let receiver = self
            .circuit()
            .add_exchange(
                ShardedAccumulatorSender::new(Some(location), exchange.clone()),
                ShardedAccumulatorReceiver::new(Some(location), exchange.clone()),
                self,
            )
            .mark_sharded_workers(workers);
        self.circuit()
            .add_dependency(receiver.local_node_id(), local_waiter.local_node_id());
        if let Some(remote_waiter_node_id) = remote_waiter_node_id {
            self.circuit()
                .add_dependency(receiver.local_node_id(), remote_waiter_node_id);
        }
        let accumulation = Accumulation {
            stream: receiver,
            enable_count,
        };
        (accumulation, exchange)
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

    /// One [Notify] for each of `local_workers`, signaled when its [Rxq]
    /// records a step from any sender.
    arrivals: Vec<Notify>,

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
            arrivals: layout.local_workers().map(|_| Notify::new()).collect(),
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

    /// Delivers `batch`, sent by local or remote `sender` in its step number
    /// `step`, to local worker `receiver`.  If `flush` is true, this is the
    /// final batch in the transaction.  Returns true if the receiving spine has
    /// too many batches.
    fn deliver(
        &self,
        factories: &B::Factories,
        sender: usize,
        receiver: usize,
        batch: B,
        step: u64,
        flush: bool,
    ) -> bool {
        // Spill the batch to disk, if we should, without taking the rxq lock.
        let batch =
            Spine::maybe_flush_batch(Some(&self.runtime), batch, factories, || (None, None));
        let should_block = {
            let mut rxq = self.rxq(receiver);
            rxq.record_step(sender, step);
            if flush || !batch.is_empty() {
                rxq.deliver(factories, sender, batch, flush)
            } else {
                false
            }
        };
        self.arrivals[receiver - self.local_workers.start].notify_waiters();
        should_block
    }

    /// Shards `batch` and sends it to the receivers, marked with the sender's
    /// step number `step`.
    ///
    /// The sender must call this exactly once in every step, even if `batch`
    /// is empty, because [Self::wait_for_arrival] depends on every sender
    /// reporting every step.
    async fn send(self: &Arc<Self>, name: Arc<String>, batch: B, step: u64, flush: bool) {
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
                        self.deliver(&self.factories, sender, receiver, item, step, flush);
                    }
                }
                WorkerLocation::Remote => {
                    let mut items = Vec::with_capacity(receivers.len());
                    for _ in receivers.clone() {
                        let mut fbuf = data
                            .next()
                            .expect("data should include one item per peer")
                            .into_tx()
                            .expect("remote mailboxes should always be serialized");
                        serialized_bytes += fbuf.len();
                        push_trailer(&mut fbuf, step, flush);
                        items.push(fbuf);
                    }

                    // Send even if there's no data, because the trailer tells
                    // the remote host that this sender has finished `step`.
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

    /// Waits until this worker's receive queue holds all of the data that
    /// every sender sent in the current step.
    ///
    /// The current step is the last step in which this worker's own
    /// [ShardedAccumulatorSender] ran, so the caller must be an operator that
    /// the circuit evaluates after that sender in the same step.  Otherwise,
    /// this waits for the previous step.
    ///
    /// This does not wait for the receive queue to deliver the data to the
    /// circuit, which happens only at the end of the transaction.
    #[cfg_attr(not(test), expect(dead_code, reason = "awaiting first caller"))]
    async fn wait_for_arrival(&self) {
        let receiver = Runtime::worker_index();
        let arrivals = &self.arrivals[receiver - self.local_workers.start];
        loop {
            // Create the future before checking, so that a notification
            // between the check and the wait can't get lost.
            let arrival = arrivals.notified();
            if self.rxq(receiver).has_arrived() {
                return;
            }
            let _parked = ParkingFor::new(ParkReason::Peers);
            arrival.await;
        }
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

/// Appends the trailer to a serialized batch sent to a remote host: the
/// sender's `step` number as 8 bytes in little-endian order, then `flush` as
/// 1 byte.
fn push_trailer(fbuf: &mut FBuf, step: u64, flush: bool) {
    fbuf.extend_from_slice(&step.to_le_bytes());
    fbuf.push(flush as u8);
}

/// Removes the trailer that [push_trailer] appended to `data` and returns the
/// step number and flush flag in it.
fn pop_trailer(data: &mut AlignedVec) -> (u64, bool) {
    let flush = pop_flushed(data);
    let len = data
        .len()
        .checked_sub(size_of::<u64>())
        .expect("message should end in a step number");
    let step = u64::from_le_bytes(data[len..].try_into().unwrap());
    data.resize(len, 0);
    (step, flush)
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
                let (step, flush) = pop_trailer(&mut data);
                let batch = deserialize_indexed_wset(&self.factories, &data);
                self.deliver(&self.factories, sender, receiver, batch, step, flush);
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

    /// For each sender, the last step number that it has delivered, or 0 if it
    /// has not delivered any step yet.  Step numbers start at 1.
    last_steps: Vec<u64>,

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
            last_steps: repeat_n(0, npeers).collect(),
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

    /// Records that `sender` has delivered all of its data for `step`.
    fn record_step(&mut self, sender: usize, step: u64) {
        let last_step = &mut self.last_steps[sender];
        debug_assert!(step > *last_step, "sender {sender} repeated step {step}");
        *last_step = step;
    }

    /// Returns true if every sender has delivered its data for the last step
    /// that this worker's own sender delivered.
    fn has_arrived(&self) -> bool {
        let step = self.last_steps[self.worker_index];
        self.last_steps.iter().all(|&last_step| last_step >= step)
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
    name: OperatorName,
    exchange: Arc<ShardedAccumulator<B>>,

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

    /// The number of times this operator has been evaluated, which is the same
    /// in every worker.
    step: u64,
}

impl<B> ShardedAccumulatorSender<B>
where
    B: Batch,
{
    fn new(location: OperatorLocation, exchange: Arc<ShardedAccumulator<B>>) -> Self {
        Self {
            location,
            name: OperatorName::new("ShardedAccumulatorSender"),
            exchange,
            enabled_during_current_transaction: None,
            input_batch_stats: BatchSizeStats::new(),
            flushed: false,
            step: 0,
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
}

impl<B> ShardedAccumulatorSender<B>
where
    B: Batch<Time = ()>,
{
    async fn eval_inner<'a>(&mut self, batch: Cow<'a, B>) {
        // We don't have a start-of-transaction signal, so we sample enable_count when
        // we get the first non-empty batch.  This batch should belong to the next transaction
        // after the last one that was flushed, since the accumulator should not receive any
        // non-empty batches from the previous transaction at that point (in the top-level circuit).
        // This may not be the first batch in the transaction, but it's ok to admit some empty batches.
        let len = batch.len();
        if (len > 0 || self.flushed) && self.enabled_during_current_transaction.is_none() {
            self.enabled_during_current_transaction = Some(self.exchange.enable_count.is_enabled());
        }
        // Send in every step, even with no data, so that the receivers learn
        // that we finished the step (see `ShardedAccumulator::send`).
        self.step += 1;
        let batch = if self.enabled_during_current_transaction == Some(true) {
            self.input_batch_stats.add_batch(len);
            batch.into_owned()
        } else {
            B::dyn_empty(&self.exchange.factories)
        };
        self.exchange
            .send(self.name.get(), batch, self.step, self.flushed)
            .await;

        if self.flushed {
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
            self.output_batch_stats.add_batch(spine.len());
            spine.backpressure_wait().await;
            self.flushed = true;
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::thread;
    use feldera_storage::fbuf::FBuf;
    use itertools::Itertools;
    use rkyv::AlignedVec;

    use super::{ShardedAccumulator, pop_trailer, push_trailer};
    use crate::{
        Circuit, DBSPHandle, OutputHandle, RootCircuit, ZSetHandle, ZWeight,
        circuit::{
            CircuitConfig, Layout, Runtime,
            operator_traits::{Operator, SinkOperator},
        },
        dynamic::{Data, DowncastTrait, DynWeightTyped},
        trace::{Batch, BatchReader, BatchReaderFactories, Cursor, FallbackWSet, Spine},
        typed_batch::TypedBatch,
    };
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        iter::zip,
        net::TcpListener,
        panic::Location,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    /// Number of rounds for streaming exchange.
    ///
    /// We do fewer rounds for streaming exchange because the `n`th round does
    /// `n` steps and exchanges `O(n**2)` data.
    const STREAMING_ROUNDS: usize = 64;

    fn test_config(workers: usize) -> CircuitConfig {
        CircuitConfig::with_workers(workers).with_streaming_exchange(true)
    }

    /// Starts `hosts` runtimes with `workers` workers in total, each running
    /// the circuit that `constructor` builds.  Returns a handle for each
    /// runtime and what `constructor` returned for it.
    fn start_runtimes<F, T>(
        workers: usize,
        hosts: usize,
        constructor: F,
    ) -> (Vec<DBSPHandle>, Vec<T>)
    where
        F: FnOnce(&mut RootCircuit) -> anyhow::Result<T> + Clone + Send + 'static,
        T: Send + 'static,
    {
        match hosts {
            0 => unreachable!(),
            1 => {
                let (dbsp_handle, result) =
                    Runtime::init_circuit(test_config(workers), constructor)
                        .expect("failed to start runtime");
                (vec![dbsp_handle], vec![result])
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
                zip(params.iter(), exchange_listeners)
                    .map(|((local_address, _), exchange_listener)| {
                        let cconf = CircuitConfig::from(
                            Layout::new_multihost(&params, *local_address).unwrap(),
                        )
                        .with_exchange_listener(exchange_listener)
                        .with_streaming_exchange(true);

                        Runtime::init_circuit(cconf, constructor.clone())
                            .expect("failed to start runtime")
                    })
                    .unzip()
            }
        }
    }

    /// Executes `f` on all of the handles in `dbsp_handles` in parallel and
    /// waits for them to complete.
    fn for_each_host<F>(dbsp_handles: &mut [DBSPHandle], f: F)
    where
        F: Fn(&mut DBSPHandle) + Send + Sync + 'static,
    {
        thread::scope(|s| {
            dbsp_handles
                .iter_mut()
                .map(|h| s.spawn(|_| f(h)))
                .collect_vec()
                .into_iter()
                .for_each(|h| h.join().unwrap())
        })
        .unwrap();
    }

    fn test_circuit(workers: usize, hosts: usize) {
        let (mut dbsp_handles, handles) = start_runtimes(workers, hosts, circuit);
        let (input_handles, output_handles): (Vec<_>, Vec<_>) = handles.into_iter().unzip();

        for round in 0..STREAMING_ROUNDS {
            for_each_host(&mut dbsp_handles, |h| h.start_transaction().unwrap());

            for i in 0..=round {
                input_handles[i % hosts].push(i, 1);
                for_each_host(&mut dbsp_handles, |h| {
                    h.step().unwrap();
                });
            }
            for_each_host(&mut dbsp_handles, |h| h.commit_transaction().unwrap());

            let mut results = BTreeMap::<usize, ZWeight>::new();
            for spine in output_handles
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
            let results = results.into_iter().collect_vec();
            let expected = (0..=round).map(|i| (i, 1)).collect_vec();
            assert_eq!(&results, &expected);
        }
    }

    fn circuit(
        circuit: &mut RootCircuit,
    ) -> anyhow::Result<(
        ZSetHandle<usize>,
        OutputHandle<
            TypedBatch<
                usize,
                (),
                i64,
                Spine<FallbackWSet<dyn Data + 'static, DynWeightTyped<i64>>>,
            >,
        >,
    )> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        let output_handle = input
            .shard_accumulate()
            .into_enabled_stream()
            .latest_output();
        Ok((input_handle, output_handle))
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
            test_circuit(workers, 1);
        }
    }

    #[test]
    fn sharded_accumulator_multihost() {
        for (workers, hosts) in [(2, 2), (4, 2), (8, 2), (3, 3), (4, 4), (16, 4)] {
            test_circuit(workers, hosts);
        }
    }

    #[test]
    fn trailer_round_trip() {
        for payload in [&b""[..], b"x", b"some serialized batch"] {
            for step in [0, 1, 0x0102_0304_0506_0708, u64::MAX] {
                for flush in [false, true] {
                    let mut fbuf = FBuf::new();
                    fbuf.extend_from_slice(payload);
                    push_trailer(&mut fbuf, step, flush);

                    let mut data = AlignedVec::new();
                    data.extend_from_slice(fbuf.as_slice());
                    assert_eq!(pop_trailer(&mut data), (step, flush));
                    assert_eq!(data.as_slice(), payload);
                }
            }
        }
    }

    #[test]
    #[should_panic(expected = "message should end in a step number")]
    fn pop_trailer_without_step() {
        // A flush byte with no step number in front of it.
        let mut data = AlignedVec::new();
        data.extend_from_slice(&[1, 2, 0]);
        pop_trailer(&mut data);
    }

    /// Sink that waits for its worker's [ShardedAccumulator] to receive all of
    /// the current step's data, then records the number of records that the
    /// worker has received so far in the transaction.
    struct ArrivalProbe<B>
    where
        B: Batch,
    {
        exchange: Arc<ShardedAccumulator<B>>,

        /// Record counts, indexed by worker.
        counts: Arc<Vec<AtomicUsize>>,
    }

    impl<B> Operator for ArrivalProbe<B>
    where
        B: Batch,
    {
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("ArrivalProbe")
        }

        fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
            true
        }
    }

    impl<B> SinkOperator<Option<Spine<B>>> for ArrivalProbe<B>
    where
        B: Batch<Time = ()>,
    {
        async fn eval(&mut self, _input: &Option<Spine<B>>) {
            self.exchange.wait_for_arrival().await;
            let worker = Runtime::worker_index();
            let count = self
                .exchange
                .rxq(worker)
                .spines
                .iter()
                .map(|entry| entry.spine.len())
                .sum();
            self.counts[worker].store(count, Ordering::Release);
        }
    }

    /// Builds a circuit that feeds its input into a sharded accumulator with an
    /// [ArrivalProbe] after it.  If `enabled`, it enables the accumulator.
    ///
    /// The input handle already partitions records the way the sharded
    /// accumulator does, so the circuit changes each key to make records
    /// move between workers.  Then workers with input sleep before they send,
    /// so that the probes in the workers without input run before all of the
    /// data has been sent.
    fn arrival_circuit(
        circuit: &mut RootCircuit,
        counts: Arc<Vec<AtomicUsize>>,
        enabled: bool,
    ) -> anyhow::Result<ZSetHandle<usize>> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        let input = input
            .map(|key| key + 1)
            .apply(|batch| {
                if !batch.is_empty() {
                    std::thread::sleep(Duration::from_millis(5));
                }
                batch.clone()
            })
            .inner();
        let runtime = Runtime::runtime().unwrap();
        let factories = BatchReaderFactories::new::<usize, (), ZWeight>();
        let (accumulation, exchange) = input.new_sharded_accumulator(
            &runtime,
            &factories,
            0..Runtime::num_workers(),
            input.sharded_accumulator_remote_waiter(&runtime),
            Location::caller(),
        );
        let (stream, enable_count) = accumulation.into_parts();
        if enabled {
            enable_count.enable();
        }
        circuit.add_sink(ArrivalProbe { exchange, counts }, &stream);
        Ok(input_handle)
    }

    /// Checks that, after `wait_for_arrival`, each worker holds all of the data
    /// sent to it so far in the transaction, including in steps that send no
    /// data.  If the accumulator is disabled, checks that the wait still
    /// finishes and that no data arrives.
    fn test_arrival(workers: usize, hosts: usize, enabled: bool) {
        const ROUNDS: usize = 4;
        const STEPS: usize = 8;

        let counts = Arc::new((0..workers).map(|_| AtomicUsize::new(0)).collect_vec());
        let (mut dbsp_handles, input_handles) = start_runtimes(workers, hosts, {
            let counts = counts.clone();
            move |circuit| arrival_circuit(circuit, counts, enabled)
        });

        let mut next_key = 0;
        for _round in 0..ROUNDS {
            for_each_host(&mut dbsp_handles, |h| h.start_transaction().unwrap());
            let mut n_sent = 0;
            for step in 0..STEPS {
                // Leave every third step empty.  Send the rest from a
                // different host each time, so that data crosses hosts.
                if step % 3 != 2 {
                    for _ in 0..step {
                        input_handles[step % hosts].push(next_key, 1);
                        next_key += 1;
                        n_sent += 1;
                    }
                }
                for_each_host(&mut dbsp_handles, |h| {
                    h.step().unwrap();
                });

                let n_received: usize = counts.iter().map(|c| c.load(Ordering::Acquire)).sum();
                let expected = if enabled { n_sent } else { 0 };
                assert_eq!(
                    n_received, expected,
                    "workers={workers} hosts={hosts} enabled={enabled} step={step}"
                );
            }
            for_each_host(&mut dbsp_handles, |h| h.commit_transaction().unwrap());
        }
    }

    #[test]
    fn arrival_single_host() {
        for workers in [2, 16] {
            for enabled in [true, false] {
                test_arrival(workers, 1, enabled);
            }
        }
    }

    #[test]
    fn arrival_multihost() {
        for (workers, hosts) in [(2, 2), (8, 2), (3, 3), (16, 4)] {
            for enabled in [true, false] {
                test_arrival(workers, hosts, enabled);
            }
        }
    }
}
