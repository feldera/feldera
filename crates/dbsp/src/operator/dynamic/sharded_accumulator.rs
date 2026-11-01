//! A fused shard and accumulate operator over a streaming exchange.
//!
//! In every step, each worker's sender partitions its input among the workers,
//! and each worker's receiver accumulates the parts sent to it into a spine.  A
//! sender's last batch in a transaction is its flush, and a receiver outputs
//! the transaction's spine once it holds the flushes of all senders.
//! `ShardedAccumulator` describes how the receivers finish in the step of the
//! last flush.  The diagrams below show it with two workers, A and B:
//!
//! ```text
//! ---->  flush
//! ====>  lag notice
//!   #    the receiver waits within the step
//! [..]   the operator runs
//! - + -  end of a step
//! ```
//!
//! # Receivers wait for the flushes
//!
//! Both workers flush in step k.  A's receiver waits for B's flush; B's
//! receiver finds both flushes queued.  Neither worker sends anything extra.
//!
//! ```text
//!          worker A                                   worker B
//!    sender       receiver                    receiver         sender
//!       |             |                           |               |
//!       |--- flush -->|                           |               |
//!       |----------------------- flush ---------->|               |
//!       |    [flushing -> waits]                  |               |
//!       |             #                           |               |
//!       |             #                           |<--- flush ----|
//!       |             #<--------- flush --------------------------|
//!       |  [all flushed -> spine]                 |               |
//!       |             |                [all flushed -> spine]     |
//! - - - + - - - - - - + - - - end of step k - - - + - - - - - - - + - - - - -
//! ```
//!
//! # A lagging sender sends lag notices
//!
//! B's sender flushes two steps after A's.  In each step that B lags, its
//! sender finds A's flush in B's queue and sends a lag notice ahead of its
//! data, and A's receiver stops waiting and outputs nothing.  Both receivers
//! output the spine in step k+2, the step of the last flush.
//!
//! ```text
//!          worker A                                   worker B
//!    sender       receiver                    receiver         sender
//!       |             |                           |               |
//!       |--- flush -->|                           |               |
//!       |----------------------- flush ---------->|               |
//!       |    [flushing -> waits]                  |               |
//!       |             #                           |       [lags, A flushed]
//!       |             #<=== lag notice (B, k) ====================|
//!       |   [lag notice -> None]                  |               |
//! - - - + - - - - - - + - - - end of step k - - - + - - - - - - - + - - - - -
//!       |    [flushing -> waits]                  |               |
//!       |             #                           |         [still lags]
//!       |             #<== lag notice (B, k+1) ===================|
//!       |   [lag notice -> None]                  |               |
//! - - - + - - - - - - + - -  end of step k+1  - - + - - - - - - - + - - - - -
//!       |    [flushing -> waits]                  |               |
//!       |             #                           |<--- flush ----|
//!       |             #<--------- flush --------------------------|
//!       |  [all flushed -> spine]                 |               |
//!       |             |                [all flushed -> spine]     |
//! - - - + - - - - - - + - -  end of step k+2  - - + - - - - - - - + - - - - -
//! ```
//!
//! # A flush that arrives late still triggers the notice
//!
//! B's receiver runs before A's flush arrives, so it has nothing to report.  On
//! one host, A's sender delivers its flush into B's queue itself; the arrival
//! finds that B lagged in step k and sends the notice for B, before A's
//! receiver runs.
//!
//! ```text
//!          worker A                                   worker B
//!    sender       receiver                    receiver         sender
//!       |             |                           |               |
//!       |             |                           |          [no flush]
//!       |             |             [nothing queued: B lags in k] |
//!       |--- flush -->|                           |               |
//!       |----------------------- flush ---------->|               |
//!       |             |               [arrival: B lagged in k]    |
//!       |             |<=== lag notice (B, k) ====|               |
//!       |   [lag notice -> None]                  |               |
//! - - - + - - - - - - + - - - end of step k - - - + - - - - - - - + - - - - -
//! ```
//!
//! # Across hosts
//!
//! In step k, A's flush reaches host 2 after B's receiver ran, so the arrival
//! sends the notice back, and A's receiver waits a round trip.  In step k+1,
//! A's flush is already queued when B's sender runs, so the sender sends the
//! notice ahead of its data, and A's receiver waits one way.  A message between
//! hosts turns down at the host boundary while it is in flight.
//!
//! ```text
//!           host 1                   :                  host 2
//!          worker A                  :                 worker B
//!    sender       receiver           :          receiver       sender
//!       |             |              :              |             |
//!       |             |              :              |        [no flush]
//!       |             |              :    [nothing queued yet]    |
//!       |--- flush -->|              :              |             |
//!       |----------------------------.              |             |
//!       |    [flushing -> waits]     | flush        |             |
//!       |             #              | (step k)     |             |
//!       |             #              '------------->|             |
//!       |             #              :  [arrival: B lagged in k]  |
//!       |             #              .==============|             |
//!       |             #   lag notice |              |             |
//!       |             #       (B, k) |              |             |
//!       |             #<============='              |             |
//!       |   [lag notice -> None]     :              |             |
//! - - - + - - - - - - + - - -  end of step k  - - - + - - - - - - + - - - - -
//!       |    [flushing -> waits]     :              |             |
//!       |             #              :              | [lags, A's flush queued]
//!       |             #              .============================|
//!       |             #   lag notice |              |             |
//!       |             #     (B, k+1) |              |             |
//!       |             #<============='              |             |
//!       |   [lag notice -> None]     :              |             |
//! - - - + - - - - - - + - - - end of step k+1 - - - + - - - - - - + - - - - -
//! ```

use std::{
    borrow::Cow,
    collections::VecDeque,
    iter::repeat_n,
    ops::Range,
    panic::Location,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use feldera_buffer_cache::ThreadType;
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
        circuit_builder::{MetadataExchange, StreamId},
        metadata::{
            ALLOCATED_MEMORY_BYTES, BatchSizeStats, EXCHANGE_WAIT_TIME_SECONDS,
            INPUT_BATCHES_STATS, LAG_NOTICES_COUNT, MEMORY_ALLOCATIONS_COUNT, MetaItem,
            OUTPUT_BATCHES_STATS, OperatorLocation, OperatorMeta, SHARED_MEMORY_BYTES, SPINE_COUNT,
            STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, OperatorName, SinkOperator, SourceOperator},
    },
    circuit_cache_key,
    operator::{
        communication::{
            ExchangeClients, ExchangeDelivery, ExchangeDirectory, ExchangeId, Mailbox, MessageType,
            pop_flushed,
        },
        dynamic::{
            accumulator::{Accumulation, EnableCount},
            shard_batch,
        },
    },
    profile::{ParkReason, ParkingFor},
    trace::{
        Batch, BatchLayout, BatchLocation, BatchReader as _, Spine, Trace, TraceRole,
        deserialize_indexed_wset, ord::fallback::pick_insert_destination, serialize_indexed_wset,
        spine_async::scope_tokio_merger_locals,
    },
};

circuit_cache_key!(local StreamingExchangeCacheId<B: Batch>(ExchangeId => Arc<ShardedAccumulator<B>>));

circuit_cache_key!(ShardedAccumulatorId<C, B: Batch>((StreamId, Range<usize>) => Accumulation<Stream<C, Option<Spine<B>>>>));

circuit_cache_key!(ShardedAccumulatorRemoteWaiterId(() => NodeId));

/// The one-byte payload of a lag notice to a receiver on another host (see
/// [ShardedAccumulator]).  A batch's payload ends in a flush byte of 0 or 1
/// (see [pop_flushed]), and an arrival marker's payload is empty, so neither
/// can be mistaken for a lag notice.
const LAG_NOTICE: u8 = 2;

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

    /// Like [`dyn_shard_accumulate`](Self::dyn_shard_accumulate), with
    /// `batch_layout` configured for the accumulator's spine.
    ///
    /// # Arguments
    ///
    /// * `factories` - factories for the batches the spines hold.
    /// * `batch_layout` - how those spines lay out the batches they write for
    ///   themselves.
    ///
    /// # Returns
    ///
    /// The accumulation, whose stream carries this worker's shard of the transaction at
    /// the end of it.
    #[track_caller]
    pub fn dyn_shard_accumulate_with_layout(
        &self,
        factories: &B::Factories,
        batch_layout: BatchLayout,
    ) -> Accumulation<Stream<C, Option<Spine<B>>>>
    where
        B: Batch<Time = ()>,
    {
        self.dyn_shard_workers_accumulate_with_layout(
            factories,
            0..Runtime::num_workers(),
            batch_layout,
        )
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
        self.dyn_shard_workers_accumulate_with_layout(factories, workers, BatchLayout::default())
    }

    #[track_caller]
    pub fn dyn_shard_workers_accumulate_with_layout(
        &self,
        factories: &B::Factories,
        workers: Range<usize>,
        batch_layout: BatchLayout,
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
                            batch_layout,
                            remote_waiter_node_id,
                            Location::caller(),
                        )
                        .0
                    },
                )
                .clone()
        } else {
            self.dyn_shard_workers(workers, factories)
                .dyn_accumulate_with_layout(factories, batch_layout)
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
        batch_layout: BatchLayout,
        remote_waiter_node_id: Option<NodeId>,
        location: &'static Location<'static>,
    ) -> (
        Accumulation<Stream<C, Option<Spine<B>>>>,
        Arc<ShardedAccumulator<B>>,
    ) {
        let (receiver, exchange) = self.add_sharded_accumulator_receiver(
            runtime,
            factories,
            workers.clone(),
            batch_layout,
            remote_waiter_node_id,
            location,
        );
        let sender = self.circuit().add_sink(
            ShardedAccumulatorSender::new(
                Some(location),
                exchange.clone(),
                // Only the root circuit exchanges metadata.
                (self.circuit().root_scope() == 0)
                    .then(|| self.circuit().metadata_exchange().clone()),
            ),
            self,
        );
        self.circuit()
            .add_dependency(sender.local_node_id().unwrap(), receiver.local_node_id());
        let accumulation = Accumulation {
            stream: receiver.mark_sharded_workers(workers),
            enable_count: exchange.enable_count.clone(),
        };
        (accumulation, exchange)
    }

    /// Adds a new [ShardedAccumulator], with its receiver and the receiver's
    /// waiters, but no sender.
    ///
    /// Constructs the receiving half of a sharded accumulator circuit, leaving
    /// the following to the caller:
    /// - Create a sender - an operator that reads `self` and sends to
    ///   the accumulator
    /// - Add a dependency from the sender to the receiver.
    /// - Mark the receiver's output as sharded, if the sender produces sharded
    ///   output.
    ///
    /// # Arguments
    ///
    /// * `runtime` - the runtime to exchange data in.
    /// * `factories` - factories for the accumulated batches.
    /// * `workers` - the workers that receive data.
    /// * `batch_layout` - how the receiving spines lay out the batches that
    ///   they write.
    /// * `remote_waiter_node_id` - the circuit's
    ///   [ShardedAccumulatorRemoteWaiter], on a multihost runtime.
    /// * `location` - the source location to report for the operators.
    ///
    /// # Returns
    ///
    /// The receiver's output stream and the [ShardedAccumulator].
    fn add_sharded_accumulator_receiver(
        &self,
        runtime: &Runtime,
        factories: &B::Factories,
        workers: Range<usize>,
        batch_layout: BatchLayout,
        remote_waiter_node_id: Option<NodeId>,
        location: &'static Location<'static>,
    ) -> (Stream<C, Option<Spine<B>>>, Arc<ShardedAccumulator<B>>) {
        let exchange_id: ExchangeId = runtime.sequence_next().try_into().unwrap();
        let exchange = ShardedAccumulator::<B>::with_runtime(
            runtime,
            workers,
            exchange_id,
            factories,
            batch_layout,
        );
        let local_waiter = self
            .circuit()
            .add_source(ShardedAccumulatorLocalWaiter::new(
                Some(location),
                exchange.clone(),
            ));
        let receiver = self.circuit().add_source(ShardedAccumulatorReceiver::new(
            Some(location),
            exchange.clone(),
        ));
        self.circuit()
            .add_dependency(receiver.local_node_id(), local_waiter.local_node_id());
        if let Some(remote_waiter_node_id) = remote_waiter_node_id {
            self.circuit()
                .add_dependency(receiver.local_node_id(), remote_waiter_node_id);
        }
        (receiver, exchange)
    }
}

/// The state that the senders and receivers of a sharded accumulator share on
/// one host: a receive queue for each local worker, and the clients that send
/// to the other hosts.
///
/// # Flushes and lag notices
///
/// A sender's last batch in a transaction is its flush.  A receiver outputs
/// the transaction's spine once its queue holds the flushes of all senders.
/// After its own worker's sender flushes, a receiver waits in the same step for
/// the other flushes, so that it finishes in the step of the last flush.
///
/// A sender that runs in a step without having flushed the transaction lags in
/// that step.  If another sender has already flushed, the lagging sender's
/// worker sends a lag notice for the step to every receiver.  A waiting
/// receiver that gets a lag notice for its step stops waiting and outputs
/// nothing in that step.  A worker learns of a flush from its own receive
/// queue, and looks for one at three points:
///
/// - When its sender runs, so that the notice usually goes out ahead of the
///   sender's data.  The sender looks only if a flag says that a flush is
///   queued, and the flag can miss a flush that arrives at the same time.
/// - When its receiver runs.  This catches a flush that arrived after the
///   sender looked, which the arrival could not report: the worker had not
///   yet recorded that it lags in the step.
/// - When a flush arrives after the receiver ran.
///
/// When all senders flush in the same step, none lags behind a flush, and the
/// workers send no lag notices.
///
/// The runs of a worker's receiver number the steps.  Every worker runs the
/// same steps, so the numbers agree across workers.  A flush carries the step
/// in which its sender sent it, and a lag notice the step in which its worker
/// lagged.  A flush from another host can arrive after its step has ended,
/// and then it can trigger a notice for a step in which no receiver waits any
/// longer.
///
/// A waiting receiver waits only for its exchange's senders on the other
/// workers, which come before it in the circuit, and for lag notices, which a
/// lagging worker sends from its sender, from its receiver, which never
/// waits, or from the task that delivers a flush from another host, which
/// never waits for a worker.  So these waits cannot hang a step.  An operator
/// that makes a worker wait for other workers within a step must likewise
/// wait only for operators that come before it in the circuit.
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

    /// How the receiving spines lay out the batches they write for themselves.
    batch_layout: BatchLayout,

    /// Range of worker IDs on the local host.
    local_workers: Range<usize>,

    /// The RPC clients to contact remote hosts.
    clients: Arc<ExchangeClients>,

    /// One [Rxq] for each of `local_workers`.
    rxq: Vec<Mutex<Rxq<B>>>,

    /// One [Notify] for each of `local_workers`, signaled when its [Rxq]
    /// records an arrival marker from any sender.
    arrivals: Vec<Notify>,

    enable_count: EnableCount,

    /// The number of messages that this accumulator has received from remote
    /// hosts.
    #[cfg(test)]
    remote_messages: std::sync::atomic::AtomicUsize,

    /// For each of `local_workers`, the number of lag notices it has sent,
    /// each counted once however many receivers it goes to.
    lag_notices_sent: Vec<AtomicU64>,

    /// For each of `local_workers`, its [Rxq::flushes_queued].  A sender reads
    /// it without the queue's lock, before it takes the lock to look for
    /// another sender's flush.
    flushes_queued: Vec<Arc<AtomicBool>>,
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
        batch_layout: BatchLayout,
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
                    batch_layout,
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
        batch_layout: BatchLayout,
    ) -> Arc<Self> {
        let layout = runtime.layout();
        let npeers = layout.n_workers();

        let name = OperatorName::new("ShardedAccumulatorReceiver");
        let flushes_queued: Vec<_> = layout
            .local_workers()
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        let exchange = Arc::new(Self {
            runtime: runtime.clone(),
            exchange_id,
            workers,
            local_workers: layout.local_workers(),
            factories: factories.clone(),
            batch_layout,
            clients,
            rxq: zip_eq(layout.local_workers(), &flushes_queued)
                .map(|(receiver, flushes_queued)| {
                    Mutex::new(Rxq::new(
                        runtime,
                        receiver,
                        factories,
                        npeers,
                        name.get(),
                        batch_layout,
                        flushes_queued.clone(),
                    ))
                })
                .collect(),
            arrivals: layout.local_workers().map(|_| Notify::new()).collect(),
            name,
            enable_count: EnableCount::default(),
            #[cfg(test)]
            remote_messages: Default::default(),
            lag_notices_sent: layout.local_workers().map(|_| AtomicU64::new(0)).collect(),
            flushes_queued,
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

    /// Delivers `batch` from local `sender` to local worker `receiver`.
    ///
    /// # Arguments
    ///
    /// * `factories` - factories for the batch.
    /// * `sender` - the local worker that sent the batch.
    /// * `receiver` - the local worker that receives the batch.
    /// * `batch` - the batch.
    /// * `flush` - true if this is the sender's final batch in the transaction.
    /// * `step` - for a flush, the step in which the sender sent it.
    ///
    /// # Returns
    ///
    /// The step for which `receiver`'s worker must now send a lag notice, if
    /// any (see [Rxq::deliver]).
    fn deliver(
        &self,
        factories: &B::Factories,
        sender: usize,
        receiver: usize,
        batch: B,
        flush: bool,
        step: u64,
    ) -> Option<u64> {
        // Spill the batch to disk, if we should, without taking the rxq lock.
        let batch = Spine::maybe_flush_batch(batch, factories, self.batch_layout, || (None, None));
        self.enqueue(sender, receiver, batch, flush, step)
    }

    /// Delivers a batch from a remote worker to a local worker.
    ///
    /// The exchange calls this outside the runtime, so a batch that must spill
    /// is spilled on the merger runtime, as the receiving worker's merges are.
    ///
    /// # Arguments
    ///
    /// * `sender` - the remote worker that sent the batch.
    /// * `receiver` - the local worker that receives the batch.
    /// * `batch` - the batch.
    /// * `flush` - true if this is the sender's final batch in the transaction.
    /// * `step` - for a flush, the step in which the sender sent it.
    ///
    /// # Returns
    ///
    /// The step for which `receiver`'s worker must now send a lag notice, if
    /// any (see [Rxq::deliver]).
    async fn deliver_received(
        self: &Arc<Self>,
        sender: usize,
        receiver: usize,
        batch: B,
        flush: bool,
        step: u64,
    ) -> Option<u64> {
        let batch = if pick_insert_destination(Some(&self.runtime), &batch)
            == BatchLocation::Storage
            && let Some(merger_runtime) = self.runtime.tokio_merger_runtime()
        {
            let offset = receiver - self.local_workers.start;
            let buffer_cache = self
                .runtime
                .get_buffer_cache(offset, ThreadType::Background);
            let slab_allocator = self
                .runtime
                .get_fbuf_slab_allocator(offset, ThreadType::Background);
            let this = self.clone();
            let spill = merger_runtime.spawn(scope_tokio_merger_locals(
                receiver,
                buffer_cache,
                slab_allocator,
                async move {
                    Spine::maybe_flush_batch(batch, &this.factories, this.batch_layout, || {
                        (None, None)
                    })
                },
            ));
            match spill.await {
                Ok(batch) => batch,
                // The merger runtime shut down, or the spill panicked and the
                // panic hook reported it to the runtime.
                Err(_) => return None,
            }
        } else {
            Arc::new(batch)
        };
        self.enqueue(sender, receiver, batch, flush, step)
    }

    /// Queues a batch for a local worker without spilling it, and wakes the
    /// worker's receiver if the batch is a flush.
    ///
    /// # Arguments
    ///
    /// * `sender` - the worker that sent the batch.
    /// * `receiver` - the local worker that receives the batch.
    /// * `batch` - the batch.
    /// * `flush` - true if this is the sender's final batch in the transaction.
    /// * `step` - for a flush, the step in which the sender sent it.
    ///
    /// # Returns
    ///
    /// The step for which `receiver`'s worker must now send a lag notice, if
    /// any (see [Rxq::deliver]).
    fn enqueue(
        &self,
        sender: usize,
        receiver: usize,
        batch: Arc<B>,
        flush: bool,
        step: u64,
    ) -> Option<u64> {
        if !flush && batch.is_empty() {
            return None;
        }
        let lag_notice = self
            .rxq(receiver)
            .deliver(&self.factories, sender, batch, flush, step);
        if flush {
            self.arrivals[receiver - self.local_workers.start].notify_waiters();
        }
        lag_notice
    }

    /// Delivers an arrival marker from local or remote `sender` to local
    /// worker `receiver` (see [Self::wait_for_arrival]).
    fn deliver_marker(&self, sender: usize, receiver: usize) {
        self.rxq(receiver).record_marker(sender);
        self.arrivals[receiver - self.local_workers.start].notify_waiters();
    }

    /// Returns the number of lag notices that local worker `worker` has sent.
    fn lag_notices_sent(&self, worker: usize) -> usize {
        self.lag_notices_sent[worker - self.local_workers.start].load(Ordering::Relaxed) as usize
    }

    /// Delivers a lag notice from local or remote worker `lagger` for `step`
    /// to local worker `receiver`, and wakes the receiver.
    fn deliver_lag_notice(&self, lagger: usize, receiver: usize, step: u64) {
        self.rxq(receiver).record_lag_notice(lagger, step);
        self.arrivals[receiver - self.local_workers.start].notify_waiters();
    }

    /// Sends every receiver a lag notice: the sender on worker `lagger` ran in
    /// `step` without having flushed the transaction.
    ///
    /// Worker threads and the tasks that receive messages from other hosts
    /// both call this, so it takes the layout from [Self::runtime] rather than
    /// from the current thread.  It does not wait for the notices to leave the
    /// queues to other hosts.  Otherwise, a receive task could wait for a
    /// receive task on another host, which could be waiting for it.
    ///
    /// # Arguments
    ///
    /// * `name` - the operator's name, for profiles.
    /// * `lagger` - the worker whose sender lags.
    /// * `step` - the step in which the sender lags.
    async fn send_lag_notices(self: &Arc<Self>, name: Arc<String>, lagger: usize, step: u64) {
        self.lag_notices_sent[lagger - self.local_workers.start].fetch_add(1, Ordering::Relaxed);

        let layout = self.runtime.layout();
        let worker_locations = WorkerLocations::for_layout(layout);
        for receivers in layout.all_hosts() {
            match worker_locations[receivers.start] {
                WorkerLocation::Local => {
                    for receiver in receivers {
                        self.deliver_lag_notice(lagger, receiver, step);
                    }
                }
                WorkerLocation::Remote => {
                    let notices = receivers
                        .clone()
                        .map(|_| {
                            let mut notice = FBuf::new();
                            notice.push(LAG_NOTICE);
                            notice
                        })
                        .collect();
                    let _ = self
                        .clients
                        .connect(receivers.start, MessageType::Streaming)
                        .await
                        .send(name.clone(), self.exchange_id, lagger, step, notices);
                }
            }
        }
    }

    /// Shards `batch` by key and sends it to the receivers.
    async fn send(self: &Arc<Self>, name: Arc<String>, batch: B, flush: bool) {
        let n_workers = Runtime::num_workers();
        let mut builders = Vec::with_capacity(n_workers);
        let mut mailboxes = Vec::with_capacity(n_workers);
        shard_batch(
            batch,
            &self.workers,
            &mut builders,
            &mut mailboxes,
            &self.factories,
        );
        self.send_mailboxes(name, mailboxes, flush).await;
    }

    /// Sends `batches`, which the caller has partitioned, to the receivers.
    ///
    /// A sender that calls this in a step without flushing should first call
    /// [Self::sender_lags], as [ShardedAccumulatorSender] does before
    /// [Self::send], so that its lag notices go out ahead of its data.
    ///
    /// # Arguments
    ///
    /// * `name` - the sending operator's name, for profiles.
    /// * `batches` - one batch for each worker in the runtime, in order of
    ///   worker index.  The batch for a worker that is not a destination of
    ///   this accumulator must be empty.
    /// * `flush` - whether these are the sender's last batches in the
    ///   transaction.
    #[cfg_attr(not(test), expect(dead_code, reason = "awaiting first caller"))]
    async fn send_partitioned(self: &Arc<Self>, name: Arc<String>, batches: Vec<B>, flush: bool) {
        let mut serializer_inner = None;
        let mailboxes = zip_eq(batches, WorkerLocations::new())
            .enumerate()
            .map(|(worker, (batch, location))| {
                debug_assert!(
                    self.workers.contains(&worker) || batch.is_empty(),
                    "batch for worker {worker}, which does not receive from this accumulator"
                );
                match location {
                    WorkerLocation::Local => Mailbox::Plain(batch),
                    WorkerLocation::Remote => Mailbox::Tx(serialize_indexed_wset(
                        &batch,
                        serializer_inner.get_or_insert_default(),
                    )),
                }
            })
            .collect();
        self.send_mailboxes(name, mailboxes, flush).await;
    }

    /// Sends `mailboxes`, one for each worker in the runtime in order of
    /// worker index.
    ///
    /// Local workers' mailboxes hold batches, which this delivers directly.
    /// Remote workers' mailboxes hold serialized batches, which this sends to
    /// their hosts, each followed by a flush byte.  A flush can make a local
    /// worker send lag notices, which this sends too.
    async fn send_mailboxes(
        self: &Arc<Self>,
        name: Arc<String>,
        mailboxes: Vec<Mailbox<B>>,
        flush: bool,
    ) {
        let sender = Runtime::worker_index();
        let step = if flush {
            self.rxq(sender).sender_step()
        } else {
            0
        };

        let runtime = Runtime::runtime().unwrap();
        let layout = runtime.layout();
        let worker_locations = WorkerLocations::for_layout(layout);
        let mut data = mailboxes.into_iter();
        let mut remote_waiters = Vec::new();
        let mut serialized_bytes = 0;
        let mut lag_notices = Vec::new();
        for receivers in layout.all_hosts() {
            match worker_locations[receivers.start] {
                WorkerLocation::Local => {
                    for receiver in receivers.clone() {
                        let item = data
                            .next()
                            .expect("data should include one item per peer")
                            .into_plain()
                            .expect("local data should not be serialized");
                        if let Some(lag_step) =
                            self.deliver(&self.factories, sender, receiver, item, flush, step)
                        {
                            lag_notices.push((receiver, lag_step));
                        }
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
                            .send(name.clone(), this.exchange_id, sender, step, items)
                        {
                            remote_waiters.push(waiter);
                        }
                    }
                }
            }
        }

        for (lagger, lag_step) in lag_notices {
            Box::pin(self.send_lag_notices(name.clone(), lagger, lag_step)).await;
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

    /// Runs this worker's receiver in a new step.
    ///
    /// If `flushing`, this worker's sender has flushed the transaction, and
    /// this waits until the queue holds the flushes of all senders, or until a
    /// sender that has not flushed lags in this step.  Otherwise, this returns
    /// at once, after sending lag notices if this worker's sender lags behind
    /// another sender's flush.
    ///
    /// # Arguments
    ///
    /// * `flushing` - whether this worker's receiver is flushing the
    ///   transaction.
    ///
    /// # Returns
    ///
    /// The transaction's spine, if all senders have flushed it.
    async fn receive(self: &Arc<Self>, flushing: bool) -> Option<Spine<B>> {
        let worker = Runtime::worker_index();
        let lag_step = {
            let mut rxq = self.rxq(worker);
            match rxq.start_step() {
                (_, false) if !flushing => return rxq.receive(),
                (step, lags) => lags.then_some(step),
            }
        };

        // The lag notices and the wait are rare.  Boxing them keeps them out
        // of the future that every run of the receiver allocates.
        if let Some(step) = lag_step {
            Box::pin(self.send_lag_notices(self.name.get(), worker, step)).await;
        }
        if flushing {
            Box::pin(self.wait_for_flushes(worker)).await
        } else {
            // This worker's sender has not flushed, so the transaction is not
            // complete.
            None
        }
    }

    /// Sends lag notices for the current step if this worker's sender, which
    /// runs in it without flushing, lags behind another sender's flush.  The
    /// sender calls this before it sends anything in the step, so that the
    /// notices usually travel ahead of its data (see [ShardedAccumulator]).
    ///
    /// # Arguments
    ///
    /// * `name` - the sending operator's name, for profiles.
    async fn sender_lags(self: &Arc<Self>, name: Arc<String>) {
        let worker = Runtime::worker_index();
        if !self.flushes_queued[worker - self.local_workers.start].load(Ordering::Relaxed) {
            return;
        }
        let lag_step = self.rxq(worker).sender_lags();
        if let Some(step) = lag_step {
            Box::pin(self.send_lag_notices(name, worker, step)).await;
        }
    }

    /// Waits until local worker `worker`'s queue holds the flushes of all
    /// senders, or until it gets a lag notice for the current step, or until
    /// the runtime is killed.  Each flush, lag notice, and arrival marker that
    /// reaches the queue wakes the wait to check again.
    ///
    /// # Returns
    ///
    /// The transaction's spine, if all senders have flushed it.  `None` after
    /// a lag notice, and after a kill, which leaves the transaction unfinished.
    async fn wait_for_flushes(&self, worker: usize) -> Option<Spine<B>> {
        let start = Instant::now();
        let mut waited = false;
        let arrivals = &self.arrivals[worker - self.local_workers.start];
        let wait = async {
            loop {
                // Create the future before checking, so that a notification
                // between the check and the wait can't get lost.
                let arrival = arrivals.notified();
                match self.rxq(worker).flush_status() {
                    FlushStatus::Complete(spine) => return Some(spine),
                    FlushStatus::Lagging => return None,
                    FlushStatus::Pending => (),
                }
                waited = true;
                let _parked = ParkingFor::new(ParkReason::Peers);
                arrival.await;
            }
        };
        let spine = self
            .runtime
            .cancellation_token()
            .run_until_cancelled_owned(wait)
            .await
            .flatten();
        if waited {
            Span::new("flush wait")
                .with_start(start)
                .with_category("Exchange")
                .with_tooltip(|| format!("{} wait for the other workers' flushes", self.name.get()))
                .record();
        }
        spine
    }

    /// Waits until this worker's receive queue holds all of the data that each
    /// worker sent to it before that worker called this method.
    ///
    /// Every worker must call this in the same step, from the same operator.
    /// The call sends an arrival marker from this worker to every receiver,
    /// after the data that this worker has sent, and then waits for the
    /// markers from every worker.
    ///
    /// This does not wait for the receive queue to deliver the data to the
    /// circuit, which happens only at the end of the transaction.
    ///
    /// # Arguments
    ///
    /// * `name` - the calling operator's name, for profiles.
    #[cfg_attr(not(test), expect(dead_code, reason = "awaiting first caller"))]
    async fn wait_for_arrival(&self, name: Arc<String>) {
        let worker = Runtime::worker_index();
        let runtime = Runtime::runtime().unwrap();
        let layout = runtime.layout();
        let worker_locations = WorkerLocations::for_layout(layout);
        for receivers in layout.all_hosts() {
            match worker_locations[receivers.start] {
                WorkerLocation::Local => {
                    for receiver in receivers {
                        self.deliver_marker(worker, receiver);
                    }
                }
                WorkerLocation::Remote => {
                    // A marker is an empty message, without the flush byte
                    // that follows every batch.
                    let markers = receivers.clone().map(|_| FBuf::new()).collect();
                    if let Some(drained) = self
                        .clients
                        .connect(receivers.start, MessageType::Streaming)
                        .await
                        .send(name.clone(), self.exchange_id, worker, 0, markers)
                    {
                        drained.await;
                    }
                }
            }
        }

        let arrivals = &self.arrivals[worker - self.local_workers.start];
        loop {
            // Create the future before checking, so that a notification
            // between the check and the wait can't get lost.
            let arrival = arrivals.notified();
            if self.rxq(worker).has_arrived() {
                return;
            }
            let _parked = ParkingFor::new(ParkReason::Peers);
            arrival.await;
        }
    }

    /// Takes the batches that this worker has received in the current
    /// transaction, leaving its receive queue empty, so that the caller can
    /// redistribute them.
    ///
    /// Call this after [Self::wait_for_arrival], in a step in which no sender
    /// has flushed the transaction.  The result then holds every batch that
    /// any worker sent to this worker in the transaction before its call to
    /// `wait_for_arrival`.  It can also hold batches that workers sent after
    /// their calls in the same step, but none from later steps, because no
    /// sender starts the next step before every worker finishes this one.
    ///
    /// # Panics
    ///
    /// Panics if a sender has already flushed the current transaction.
    #[cfg_attr(not(test), expect(dead_code, reason = "awaiting first caller"))]
    fn take_received(&self) -> Vec<Arc<B>> {
        let receiver = Runtime::worker_index();
        self.rxq(receiver).take(&self.factories).get_batches()
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

    fn received(
        self: Arc<Self>,
        sender: usize,
        round: u64,
        data: Vec<AlignedVec>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            #[cfg(test)]
            self.remote_messages
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            for (receiver, mut data) in zip_eq(self.local_workers.clone(), data) {
                match data.last() {
                    // An arrival marker (see `wait_for_arrival`).
                    None => self.deliver_marker(sender, receiver),
                    Some(&LAG_NOTICE) => self.deliver_lag_notice(sender, receiver, round),
                    Some(_) => {
                        let flush = pop_flushed(&mut data);
                        let batch = deserialize_indexed_wset(&self.factories, &data);
                        if let Some(lag_step) = self
                            .deliver_received(sender, receiver, batch, flush, round)
                            .await
                        {
                            Box::pin(self.send_lag_notices(self.name.get(), receiver, lag_step))
                                .await;
                        }
                    }
                }
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

    /// For each sender, the number of arrival markers it has delivered.
    n_markers: Vec<usize>,

    /// The number of entries that have been popped off `spines` and received by
    /// the circuit.
    n_received: usize,

    /// The number of steps in which this worker's receiver has run.  The
    /// sender runs before the receiver in each step, so the sender's current
    /// step is one more (see [ShardedAccumulator] for how steps are numbered).
    steps: u64,

    /// The last step in which this worker's sender ran without having flushed
    /// the current transaction, or 0.
    lagging_step: u64,

    /// The last step for which this worker has sent lag notices, or 0.
    noticed_step: u64,

    /// For each sender, the last step for which it sent this worker a lag
    /// notice, or 0.
    lag_notices: Vec<u64>,

    /// Name for use in profiles.
    name: Arc<String>,

    /// How the spines lay out the batches they write for themselves.
    batch_layout: BatchLayout,

    /// True while the transaction that the receiver outputs next has a flush
    /// in this queue.  Only this queue changes it, under its lock, so it
    /// stays true no longer than the queue holds the flush.
    flushes_queued: Arc<AtomicBool>,
}

/// What a receiver whose own worker's sender has flushed finds in its queue
/// (see [Rxq::flush_status]).
enum FlushStatus<B>
where
    B: Batch,
{
    /// All senders have flushed: the transaction's spine.
    Complete(Spine<B>),

    /// A sender lags in the current step, so the transaction cannot complete
    /// in it.
    Lagging,

    /// Neither of the above, yet.
    Pending,
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
        batch_layout: BatchLayout,
    ) -> Self {
        Self {
            n_unflushed: npeers,
            spine: Spine::with_runtime(
                runtime.clone(),
                worker_index,
                factories,
                name,
                TraceRole::Accumulator,
            )
            .with_layout(batch_layout),
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
        batch_layout: BatchLayout,
        flushes_queued: Arc<AtomicBool>,
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
                batch_layout,
            )]),
            name,
            batch_layout,
            n_flushes: repeat_n(0, npeers).collect(),
            n_markers: repeat_n(0, npeers).collect(),
            n_received: 0,
            steps: 0,
            lagging_step: 0,
            noticed_step: 0,
            lag_notices: repeat_n(0, npeers).collect(),
            flushes_queued,
        }
    }

    /// Queues `batch` from `sender`.
    ///
    /// If `batch` is another sender's flush, and this worker's sender ran
    /// without having flushed in the flush's step or a later one, then this
    /// worker must send lag notices for the last step in which its sender ran.
    /// That step may have ended, if the flush comes from another host.
    ///
    /// # Arguments
    ///
    /// * `factories` - factories for the spines.
    /// * `sender` - the worker that sent the batch.
    /// * `batch` - the batch.
    /// * `flush` - true if this is the sender's final batch in the transaction.
    /// * `step` - for a flush, the step in which the sender sent it.
    ///
    /// # Returns
    ///
    /// The step for which this worker must now send lag notices, if any.
    fn deliver(
        &mut self,
        factories: &B::Factories,
        sender: usize,
        batch: Arc<B>,
        flush: bool,
        step: u64,
    ) -> Option<u64> {
        let index = self.n_flushes[sender] - self.n_received;
        let entry = &mut self.spines[index];

        entry.spine.insert_without_blocking(batch);

        if !flush {
            return None;
        }
        entry.n_unflushed -= 1;
        self.n_flushes[sender] += 1;
        if index + 1 >= self.spines.len() {
            self.spines.push_back(RxqEntry::new(
                self.npeers,
                &self.runtime,
                self.worker_index,
                factories,
                self.name.clone(),
                self.batch_layout,
            ));
        }
        self.flushes_queued
            .store(self.any_flushed(), Ordering::Relaxed);

        let lags_behind = !self.has_flushed(self.worker_index)
            && self.lagging_step >= step
            && self.noticed_step < self.lagging_step;
        lags_behind.then(|| {
            self.noticed_step = self.lagging_step;
            self.lagging_step
        })
    }

    /// Returns true if `sender` has flushed the transaction that the
    /// receiver outputs next.
    fn has_flushed(&self, sender: usize) -> bool {
        self.n_flushes[sender] > self.n_received
    }

    /// Returns true if any sender has flushed the transaction that the
    /// receiver outputs next.
    fn any_flushed(&self) -> bool {
        self.spines[0].n_unflushed < self.npeers
    }

    /// Returns the current step of this worker's sender, which runs before the
    /// receiver in each step.
    fn sender_step(&self) -> u64 {
        self.steps + 1
    }

    /// Starts a new step for this worker's receiver (see [Self::lags]).
    ///
    /// # Returns
    ///
    /// The step, and whether this worker must send lag notices for it.
    fn start_step(&mut self) -> (u64, bool) {
        self.steps += 1;
        let step = self.steps;
        (step, self.lags(step))
    }

    /// Records that this worker's sender runs in the current step without
    /// flushing (see [Self::lags]).
    ///
    /// # Returns
    ///
    /// The step, if this worker must send lag notices for it.
    fn sender_lags(&mut self) -> Option<u64> {
        let step = self.sender_step();
        self.lags(step).then_some(step)
    }

    /// Records that this worker's sender runs in `step` without having flushed
    /// the transaction, unless it has flushed it.
    ///
    /// # Returns
    ///
    /// True if this worker must send lag notices for `step`: its sender has
    /// not flushed the transaction, another sender has, and no notices for
    /// `step` have gone out yet.
    fn lags(&mut self, step: u64) -> bool {
        if self.has_flushed(self.worker_index) {
            return false;
        }
        self.lagging_step = step;
        let notice = self.any_flushed() && self.noticed_step < step;
        if notice {
            self.noticed_step = step;
        }
        notice
    }

    /// Records a lag notice from `lagger` for `step`.
    fn record_lag_notice(&mut self, lagger: usize, step: u64) {
        self.lag_notices[lagger] = self.lag_notices[lagger].max(step);
    }

    /// Checks whether the receiver, after its own worker's sender flushed,
    /// can output the transaction in the current step.
    fn flush_status(&mut self) -> FlushStatus<B> {
        if let Some(spine) = self.receive() {
            FlushStatus::Complete(spine)
        } else if self.lag_notices.iter().any(|&step| step >= self.steps) {
            FlushStatus::Lagging
        } else {
            FlushStatus::Pending
        }
    }

    /// Records an arrival marker from `sender`.
    fn record_marker(&mut self, sender: usize) {
        self.n_markers[sender] += 1;
    }

    /// Returns true if every sender has delivered as many arrival markers as
    /// this worker.
    fn has_arrived(&self) -> bool {
        let n = self.n_markers[self.worker_index];
        self.n_markers.iter().all(|&n_markers| n_markers >= n)
    }

    fn receive(&mut self) -> Option<Spine<B>> {
        let entry = self.spines.pop_front_if(|entry| entry.n_unflushed == 0)?;
        self.n_received += 1;
        self.flushes_queued
            .store(self.any_flushed(), Ordering::Relaxed);
        Some(entry.spine)
    }

    /// Takes the spine of the current transaction and leaves an empty one in
    /// its place (see [ShardedAccumulator::take_received]).
    ///
    /// # Panics
    ///
    /// Panics if a sender has already flushed the current transaction: its
    /// later batches belong to the next transaction.
    fn take(&mut self, factories: &B::Factories) -> Spine<B> {
        let entry = self
            .spines
            .front_mut()
            .expect("the receive queue always has a spine under construction");
        assert_eq!(
            entry.n_unflushed, self.npeers,
            "cannot take received batches after a sender flushed the transaction"
        );
        let empty = RxqEntry::new(
            self.npeers,
            &self.runtime,
            self.worker_index,
            factories,
            self.name.clone(),
            self.batch_layout,
        );
        std::mem::replace(entry, empty).spine
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

    /// Whether the scheduler has asked this sender to flush in the current
    /// step.
    flushed: bool,

    /// Whether this sender has sent its flush of the current transaction.  Its
    /// worker cannot lag behind another sender's flush after that, so the
    /// sender stops looking for one.
    sent_flush: bool,
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
            sent_flush: false,
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
        self.sent_flush = false;
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
        if !self.flushed && !self.sent_flush {
            self.exchange.sender_lags(self.name.get()).await;
        }

        // Sample enable_count when we get the first non-empty batch in a
        // transaction.  This batch should belong to the next transaction after
        // the last one that was flushed, since the accumulator should not
        // receive any non-empty batches from the previous transaction at that
        // point (in the top-level circuit).  This may not be the first batch in
        // the transaction, but it's ok to admit some empty batches.
        let len = batch.len_upper_bound();
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
            self.sent_flush = true;
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

    /// The remote waiter waits for every sharded accumulator in the circuit,
    /// and a bootstrap of one of them must not run all of them.
    fn is_waiter(&self) -> bool {
        true
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

    /// True from the scheduler's flush request until the receiver outputs the
    /// transaction's spine.
    flushing: bool,

    /// Total time spent waiting for the flushes of other workers.
    flush_wait_time: Duration,
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
            flushing: false,
            flush_wait_time: Duration::ZERO,
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
        self.flushing = true;
    }

    fn is_flush_complete(&self) -> bool {
        !self.flushing
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
            EXCHANGE_WAIT_TIME_SECONDS => MetaItem::Duration(self.flush_wait_time),
            LAG_NOTICES_COUNT => MetaItem::Count(self.exchange.lag_notices_sent(Runtime::worker_index())),
        });
    }
}

impl<B> SourceOperator<Option<Spine<B>>> for ShardedAccumulatorReceiver<B>
where
    B: Batch<Time = ()>,
{
    async fn eval(&mut self) -> Option<Spine<B>> {
        let output = if self.flushing {
            let start = Instant::now();
            let output = self.exchange.receive(true).await;
            self.flush_wait_time += start.elapsed();
            output
        } else {
            self.exchange.receive(false).await
        };
        if let Some(spine) = &output {
            self.output_batch_stats.add_batch(spine.len_upper_bound());
            spine.backpressure_wait().await;
            self.flushing = false;
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::{FlushStatus, Rxq, ShardedAccumulator};
    use crate::{
        Circuit, Error as DbspError, IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit,
        RuntimeError, Stream, ZSetHandle, ZWeight,
        algebra::{OrdZSet as DynOrdZSet, OrdZSetFactories},
        circuit::{
            CircuitConfig, Layout, Runtime,
            circuit_builder::CircuitBase,
            metadata::{LAG_NOTICES_COUNT, MetaItem},
            mkconfig,
            operator_traits::{Operator, SinkOperator, UnaryOperator},
        },
        dynamic::{Data, DowncastTrait, DynData, DynWeightTyped},
        operator::{communication::ExchangeDelivery, dynamic::accumulator::EnableCount},
        storage::file::SerializerInner,
        trace::{
            Batch, BatchLayout, BatchLocation, BatchReader, BatchReaderFactories, Cursor,
            FallbackWSet, Spine, Trace, WithSnapshot, serialize_indexed_wset,
        },
        typed_batch::{SpineSnapshot, TypedBatch},
        utils::{
            Tup2,
            test::multihost::{TestHosts, host_configs, with_host_storage},
        },
    };
    use feldera_storage::tokio::TOKIO;
    use feldera_types::memory_pressure::MemoryPressure;
    use proptest::prelude::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use rkyv::AlignedVec;
    use std::{
        borrow::Cow,
        collections::BTreeMap,
        panic::Location,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc::channel,
        },
        thread::sleep,
        time::{Duration, Instant},
    };
    use tempfile::tempdir;

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

    /// The handles of [circuit] on one host.
    struct Handles {
        input: ZSetHandle<usize>,
        output: TestOutput,

        /// The host's enable count for the accumulator.  The accumulator
        /// starts out disabled on every host.
        enable_count: EnableCount,
    }

    /// Starts [circuit] with `workers` workers spread over `hosts` hosts, with
    /// the circuit gathering its output as `gather` says.
    fn start_circuit(workers: usize, hosts: usize, gather: Gather) -> TestHosts<Handles> {
        TestHosts::start_with(
            workers,
            hosts,
            move |_host, config| config.with_streaming_exchange(gather.streaming_exchange),
            move |c| circuit(c, gather),
        )
    }

    /// Runs one transaction that inserts the records `0..n_records`, spread
    /// round-robin over the hosts, one record per step.
    ///
    /// Calls `after_first_step` after the first step, for a test that changes
    /// the enable counts in the middle of the transaction.
    ///
    /// # Returns
    ///
    /// The output of the transaction, summed over every host, sorted by
    /// record.
    fn run_transaction(
        test_hosts: &mut TestHosts<Handles>,
        n_records: usize,
        after_first_step: impl FnOnce(&TestHosts<Handles>),
    ) -> Vec<(usize, ZWeight)> {
        let hosts = test_hosts.n_hosts();
        let mut after_first_step = Some(after_first_step);
        test_hosts.start_transaction();
        for i in 0..n_records {
            test_hosts.result(i % hosts).input.push(i, 1);
            test_hosts.step();
            if let Some(f) = after_first_step.take() {
                f(test_hosts);
            }
        }
        test_hosts.commit_transaction();

        let mut results = BTreeMap::<usize, ZWeight>::new();
        for spine in test_hosts
            .results()
            .iter()
            .flat_map(|handles| handles.output.take_from_all())
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
        let mut test_hosts = start_circuit(workers, hosts, gather);
        for &host in enabled_hosts {
            test_hosts.result(host).enable_count.enable();
        }
        for round in 0..STREAMING_ROUNDS {
            let n_records = round + 1;
            assert_eq!(
                run_transaction(&mut test_hosts, n_records, |_| ()),
                expected_output(n_records),
                "round {round} with {workers} workers on {hosts} hosts, {gather:?}"
            );
        }
    }

    fn circuit(circuit: &mut RootCircuit, gather: Gather) -> anyhow::Result<Handles> {
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
        Ok(Handles {
            input: input_handle,
            output: stream.latest_output(),
            enable_count,
        })
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
            let mut test_hosts = start_circuit(workers, hosts, Gather::ALL_WORKERS);
            assert_eq!(run_transaction(&mut test_hosts, N_RECORDS, |_| ()), vec![]);

            assert_eq!(
                run_transaction(&mut test_hosts, N_RECORDS, |test_hosts| test_hosts
                    .result(0)
                    .enable_count
                    .enable()),
                vec![],
                "enabled during the transaction, with {workers} workers on {hosts} hosts"
            );
            assert_eq!(
                run_transaction(&mut test_hosts, N_RECORDS, |_| ()),
                expected_output(N_RECORDS),
                "enabled before the transaction, with {workers} workers on {hosts} hosts"
            );

            assert_eq!(
                run_transaction(&mut test_hosts, N_RECORDS, |test_hosts| test_hosts
                    .result(0)
                    .enable_count
                    .disable()),
                expected_output(N_RECORDS),
                "disabled during the transaction, with {workers} workers on {hosts} hosts"
            );
            assert_eq!(
                run_transaction(&mut test_hosts, N_RECORDS, |_| ()),
                vec![],
                "disabled before the transaction, with {workers} workers on {hosts} hosts"
            );
        }
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
            self.exchange
                .wait_for_arrival(Arc::new("ArrivalProbe".to_string()))
                .await;
            let worker = Runtime::worker_index();
            let count = self
                .exchange
                .rxq(worker)
                .spines
                .iter()
                .map(|entry| entry.spine.len_upper_bound())
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
            BatchLayout::default(),
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
        let mut test_hosts = TestHosts::start(workers, hosts, {
            let counts = counts.clone();
            move |circuit| arrival_circuit(circuit, counts, enabled)
        });

        let mut next_key = 0;
        for _round in 0..ROUNDS {
            test_hosts.start_transaction();
            let mut n_sent = 0;
            for step in 0..STEPS {
                // Leave every third step empty.  Send the rest from a
                // different host each time, so that data crosses hosts.
                if step % 3 != 2 {
                    for _ in 0..step {
                        test_hosts.result(step % hosts).push(next_key, 1);
                        next_key += 1;
                        n_sent += 1;
                    }
                }
                test_hosts.step();

                let n_received: usize = counts.iter().map(|c| c.load(Ordering::Acquire)).sum();
                let expected = if enabled { n_sent } else { 0 };
                assert_eq!(
                    n_received, expected,
                    "workers={workers} hosts={hosts} enabled={enabled} step={step}"
                );
            }
            test_hosts.commit_transaction();
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

    /// Sender that sends all of its input to one worker with
    /// [ShardedAccumulator::send_partitioned].
    struct PartitionedSender<B>
    where
        B: Batch,
    {
        exchange: Arc<ShardedAccumulator<B>>,

        /// The worker that receives every record.
        destination: usize,

        /// If set, every worker takes its received batches once per
        /// transaction.
        take: Option<Take>,

        /// The number of steps so far in the current transaction.
        steps_in_transaction: usize,

        flushed: bool,
    }

    /// When [PartitionedSender] takes its worker's received batches.
    #[derive(Clone)]
    struct Take {
        /// The step in each transaction, counting from 1, after which every
        /// worker takes its received batches.
        step: usize,

        /// A worker that sleeps before it sends in `step`, so that the other
        /// workers would take before its batches arrive if they did not wait
        /// for them.
        slow_worker: usize,

        /// Numbers of records taken, indexed by worker.
        taken: Arc<Vec<AtomicUsize>>,
    }

    impl<B> Operator for PartitionedSender<B>
    where
        B: Batch,
    {
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("PartitionedSender")
        }

        fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
            true
        }

        fn start_transaction(&mut self) {
            self.steps_in_transaction = 0;
        }

        fn flush(&mut self) {
            self.flushed = true;
        }
    }

    impl<B> SinkOperator<B> for PartitionedSender<B>
    where
        B: Batch<Time = ()>,
    {
        async fn eval(&mut self, batch: &B) {
            self.steps_in_transaction += 1;
            let take = self
                .take
                .as_ref()
                .filter(|take| take.step == self.steps_in_transaction);
            if let Some(take) = take
                && take.slow_worker == Runtime::worker_index()
            {
                std::thread::sleep(Duration::from_millis(20));
            }

            let mut batches = (0..Runtime::num_workers())
                .map(|_| B::dyn_empty(&self.exchange.factories))
                .collect_vec();
            batches[self.destination] = batch.clone();
            let name = Arc::new("PartitionedSender".to_string());
            self.exchange
                .send_partitioned(name.clone(), batches, self.flushed)
                .await;

            if let Some(take) = take {
                self.exchange.wait_for_arrival(name).await;
                let n_taken = self
                    .exchange
                    .take_received()
                    .iter()
                    .map(|batch| batch.len_upper_bound())
                    .sum();
                take.taken[Runtime::worker_index()].fetch_add(n_taken, Ordering::AcqRel);
            }
            self.flushed = false;
        }
    }

    /// Sink that adds the number of records in each spine that its worker's
    /// receiver outputs to the worker's count.
    struct OutputCounter {
        /// Record counts, indexed by worker.
        counts: Arc<Vec<AtomicUsize>>,
    }

    impl Operator for OutputCounter {
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("OutputCounter")
        }

        fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
            true
        }
    }

    impl<B> SinkOperator<Option<Spine<B>>> for OutputCounter
    where
        B: Batch<Time = ()>,
    {
        async fn eval(&mut self, spine: &Option<Spine<B>>) {
            if let Some(spine) = spine {
                self.counts[Runtime::worker_index()]
                    .fetch_add(spine.len_upper_bound(), Ordering::AcqRel);
            }
        }
    }

    /// Builds a circuit in which every worker sends its input to
    /// `destination` through a sharded accumulator, taking its received
    /// batches as `take` says, and the receivers count the records they
    /// output in `counts`.
    fn partitioned_circuit(
        circuit: &mut RootCircuit,
        destination: usize,
        take: Option<Take>,
        counts: Arc<Vec<AtomicUsize>>,
    ) -> anyhow::Result<ZSetHandle<usize>> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        let input = input.inner();
        let runtime = Runtime::runtime().unwrap();
        let factories = BatchReaderFactories::new::<usize, (), ZWeight>();
        let (stream, exchange) = input.add_sharded_accumulator_receiver(
            &runtime,
            &factories,
            0..Runtime::num_workers(),
            BatchLayout::default(),
            input.sharded_accumulator_remote_waiter(&runtime),
            Location::caller(),
        );
        let sender = circuit.add_sink(
            PartitionedSender {
                exchange,
                destination,
                take,
                steps_in_transaction: 0,
                flushed: false,
            },
            &input,
        );
        circuit.add_dependency(sender.local_node_id().unwrap(), stream.local_node_id());
        circuit.add_sink(OutputCounter { counts }, &stream);
        Ok(input_handle)
    }

    /// Checks that `send_partitioned` delivers each batch to the worker that
    /// it is for, whether local or remote, by sending every record to
    /// `destination`.
    fn test_partitioned_send(workers: usize, hosts: usize, destination: usize) {
        const TRANSACTIONS: usize = 3;
        const STEPS: usize = 4;

        let counts = Arc::new((0..workers).map(|_| AtomicUsize::new(0)).collect_vec());
        let mut test_hosts = TestHosts::start(workers, hosts, {
            let counts = counts.clone();
            move |circuit| partitioned_circuit(circuit, destination, None, counts)
        });

        let mut next_key = 0;
        for _transaction in 0..TRANSACTIONS {
            test_hosts.start_transaction();
            let mut n_sent = 0;
            for step in 0..STEPS {
                for _ in 0..=step {
                    test_hosts.result(step % hosts).push(next_key, 1);
                    next_key += 1;
                    n_sent += 1;
                }
                test_hosts.step();
            }
            test_hosts.commit_transaction();

            let received = counts
                .iter()
                .map(|count| count.swap(0, Ordering::AcqRel))
                .collect_vec();
            let expected = (0..workers)
                .map(|worker| if worker == destination { n_sent } else { 0 })
                .collect_vec();
            assert_eq!(
                received, expected,
                "workers={workers} hosts={hosts} destination={destination}"
            );
        }
    }

    #[test]
    fn partitioned_send() {
        for (workers, hosts) in [(4, 1), (4, 2), (3, 3)] {
            for destination in [0, workers - 1] {
                test_partitioned_send(workers, hosts, destination);
            }
        }
    }

    /// Checks that `take_received`, after `wait_for_arrival`, takes exactly
    /// the records sent so far in the transaction, even from a slow sender,
    /// and that the receiver outputs only the records sent afterward.
    fn test_take_received(workers: usize, hosts: usize, destination: usize) {
        const TRANSACTIONS: usize = 3;
        const STEPS: usize = 4;
        const TAKE_STEP: usize = 3;

        let taken = Arc::new((0..workers).map(|_| AtomicUsize::new(0)).collect_vec());
        let received = Arc::new((0..workers).map(|_| AtomicUsize::new(0)).collect_vec());
        let take = Take {
            step: TAKE_STEP,
            slow_worker: (destination + 1) % workers,
            taken: taken.clone(),
        };
        let mut test_hosts = TestHosts::start(workers, hosts, {
            let received = received.clone();
            move |circuit| partitioned_circuit(circuit, destination, Some(take), received)
        });

        let mut next_key = 0;
        for transaction in 0..TRANSACTIONS {
            test_hosts.start_transaction();
            let mut n_before = 0;
            let mut n_after = 0;
            for step in 1..=STEPS {
                for _ in 0..step {
                    test_hosts.result(step % hosts).push(next_key, 1);
                    next_key += 1;
                    if step <= TAKE_STEP {
                        n_before += 1;
                    } else {
                        n_after += 1;
                    }
                }
                test_hosts.step();
            }
            test_hosts.commit_transaction();

            let only_destination = |n| {
                (0..workers)
                    .map(|worker| if worker == destination { n } else { 0 })
                    .collect_vec()
            };
            let take_counts = |counts: &Arc<Vec<AtomicUsize>>| {
                counts
                    .iter()
                    .map(|count| count.swap(0, Ordering::AcqRel))
                    .collect_vec()
            };
            assert_eq!(
                take_counts(&taken),
                only_destination(n_before),
                "taken: workers={workers} hosts={hosts} destination={destination} transaction={transaction}"
            );
            assert_eq!(
                take_counts(&received),
                only_destination(n_after),
                "received: workers={workers} hosts={hosts} destination={destination}"
            );
        }
    }

    #[test]
    fn take_received() {
        for (workers, hosts) in [(4, 1), (4, 2), (3, 3)] {
            for destination in [0, workers - 1] {
                test_take_received(workers, hosts, destination);
            }
        }
    }

    /// Checks that the spine that `take` leaves in a receive queue has the
    /// queue's batch layout.
    #[test]
    fn take_keeps_batch_layout() {
        Runtime::run(CircuitConfig::with_workers(1), |_parker| {
            let layout = BatchLayout {
                key_block_bytes: Some(65536),
            };
            let factories = <OrdZSetFactories<DynData>>::new::<usize, (), ZWeight>();
            let mut rxq = Rxq::<DynOrdZSet<DynData>>::new(
                &Runtime::runtime().unwrap(),
                0,
                &factories,
                1,
                Arc::new("Rxq".to_string()),
                layout,
                Arc::new(AtomicBool::new(false)),
            );
            rxq.take(&factories);
            assert_eq!(rxq.spines[0].spine.layout(), layout);
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// Builds a circuit that feeds its input into an enabled sharded
    /// accumulator.  Returns the input handle and the accumulator.
    fn enabled_circuit(
        circuit: &mut RootCircuit,
    ) -> anyhow::Result<(
        ZSetHandle<usize>,
        Arc<ShardedAccumulator<DynOrdZSet<DynData>>>,
    )> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        let input = input.inner();
        let runtime = Runtime::runtime().unwrap();
        let factories = BatchReaderFactories::new::<usize, (), ZWeight>();
        let (accumulation, exchange) = input.new_sharded_accumulator(
            &runtime,
            &factories,
            0..Runtime::num_workers(),
            BatchLayout::default(),
            input.sharded_accumulator_remote_waiter(&runtime),
            Location::caller(),
        );
        accumulation.into_parts().1.enable();
        Ok((input_handle, exchange))
    }

    /// Checks that the senders send nothing to remote hosts in steps without
    /// data, and only their flushes when the transaction commits.
    #[test]
    fn no_remote_messages_without_data() {
        const WORKERS: usize = 4;
        const HOSTS: usize = 2;

        let mut test_hosts = TestHosts::start(WORKERS, HOSTS, enabled_circuit);
        let exchanges = test_hosts
            .results()
            .iter()
            .map(|(_input, exchange)| exchange.clone())
            .collect_vec();
        let remote_messages = || {
            exchanges
                .iter()
                .map(|exchange| exchange.remote_messages.load(Ordering::Acquire))
                .sum::<usize>()
        };

        test_hosts.start_transaction();
        for _ in 0..8 {
            test_hosts.step();
        }
        assert_eq!(remote_messages(), 0);

        // Each worker sends its flush to the other host in one message.
        test_hosts.commit_transaction();
        assert_eq!(remote_messages(), WORKERS * (HOSTS - 1));
    }

    /// A batch from another host spills to storage under memory pressure,
    /// although the exchange delivers it on a thread outside the runtime.
    #[test]
    fn remote_batch_spills_under_memory_pressure() {
        let temp_dir = tempdir().expect("Can't create temp dir for storage");

        // A one-byte budget puts the runtime under critical memory pressure.
        let config = mkconfig(temp_dir.path()).with_max_rss_bytes(Some(1));
        let (dbsp, (_input, exchange)) = Runtime::init_circuit(config, enabled_circuit).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while dbsp.runtime().current_memory_pressure() < MemoryPressure::High {
            assert!(Instant::now() < deadline, "memory pressure stayed low");
            sleep(Duration::from_millis(10));
        }

        // What a worker on another host sends: a serialized batch, then a byte
        // that says whether it is the sender's final batch.
        let batch = OrdZSet::from_keys((), vec![Tup2(7usize, 1)]).into_inner();
        let mut message = AlignedVec::new();
        message.extend_from_slice(&serialize_indexed_wset(&batch, &mut SerializerInner::new()));
        message.push(0);
        TOKIO
            .block_on(TOKIO.spawn(exchange.clone().received(0, 0, vec![message])))
            .unwrap();

        let batches = exchange.rxq(0).spines[0]
            .spine
            .ro_snapshot()
            .batches()
            .to_vec();
        assert!(!batches.is_empty());
        assert!(
            batches
                .iter()
                .all(|batch| batch.location() == BatchLocation::Storage)
        );
        dbsp.kill().unwrap();
    }

    /// A flush from another host whose spill fails is lost, but the spill's
    /// panic fails the runtime's next command, so that a receiver that waits
    /// for the flush cannot hang.
    #[test]
    fn failed_spill_of_remote_flush_fails_the_runtime() {
        let temp_dir = tempdir().expect("Can't create temp dir for storage");
        let storage = temp_dir.path().to_path_buf();

        // A one-byte budget puts the runtime under critical memory pressure.
        let config = mkconfig(&storage).with_max_rss_bytes(Some(1));
        let (mut dbsp, (_input, exchange)) =
            Runtime::init_circuit(config, enabled_circuit).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while dbsp.runtime().current_memory_pressure() < MemoryPressure::High {
            assert!(Instant::now() < deadline, "memory pressure stayed low");
            sleep(Duration::from_millis(10));
        }

        // A file in place of the storage directory makes every spill fail.
        std::fs::remove_dir_all(&storage).unwrap();
        std::fs::write(&storage, b"").unwrap();

        // What a worker on another host sends as its flush: a serialized
        // batch, then a byte that says it is the sender's final batch.
        let batch = OrdZSet::from_keys((), vec![Tup2(7usize, 1)]).into_inner();
        let mut message = AlignedVec::new();
        message.extend_from_slice(&serialize_indexed_wset(&batch, &mut SerializerInner::new()));
        message.push(1);
        TOKIO
            .block_on(TOKIO.spawn(exchange.clone().received(0, 1, vec![message])))
            .unwrap();
        assert!(!exchange.rxq(0).has_flushed(0), "the flush should be lost");

        let result = dbsp.transaction();
        assert!(
            matches!(
                result,
                Err(DbspError::Runtime(RuntimeError::WorkerPanic { .. }))
            ),
            "the spill's panic should fail the transaction, but it returned {result:?}"
        );
        std::fs::remove_file(&storage).unwrap();
    }

    /// A message between hosts in [FlushModel].
    enum ModelMessage {
        /// A flush from `sender`, sent in `step`.
        Flush {
            sender: usize,
            receiver: usize,
            step: u64,
        },

        /// A lag notice from `lagger` for `step`.
        LagNotice {
            lagger: usize,
            receiver: usize,
            step: u64,
        },
    }

    impl ModelMessage {
        fn receiver(&self) -> usize {
            match self {
                Self::Flush { receiver, .. } | Self::LagNotice { receiver, .. } => *receiver,
            }
        }
    }

    /// How far a worker has come in the current step of [FlushModel].
    #[derive(Copy, Clone, PartialEq, Eq)]
    enum ModelPhase {
        /// The sender has yet to run.
        Sender,

        /// The receiver has yet to run.
        Receiver,

        /// The receiver waits for flushes.
        Waiting,

        /// The worker has finished the step.
        Done,
    }

    /// Runs the receive queues of one sharded accumulator through the flush
    /// protocol, as [ShardedAccumulator] and its operators do, but runs the
    /// workers' actions and the arrivals of messages between hosts in random
    /// order.
    struct FlushModel {
        rxqs: Vec<Rxq<DynOrdZSet<DynData>>>,
        factories: OrdZSetFactories<DynData>,

        /// The host of each worker.
        hosts: Vec<usize>,

        /// Messages between hosts that have not arrived yet.
        in_flight: Vec<ModelMessage>,

        /// The lag notices sent so far, as (lagger, step).
        lag_notices: Vec<(usize, u64)>,

        rng: StdRng,
    }

    impl FlushModel {
        /// Creates the receive queues of `workers` workers, spread evenly over
        /// `hosts` hosts, and draws the order of events from `seed`.
        fn new(workers: usize, hosts: usize, seed: u64) -> Self {
            let runtime = Runtime::runtime().unwrap();
            let factories = <OrdZSetFactories<DynData>>::new::<usize, (), ZWeight>();
            let rxqs = (0..workers)
                .map(|worker| {
                    Rxq::new(
                        &runtime,
                        worker,
                        &factories,
                        workers,
                        Arc::new(format!("Rxq{worker}")),
                        BatchLayout::default(),
                        Arc::new(AtomicBool::new(false)),
                    )
                })
                .collect();
            Self {
                rxqs,
                factories,
                hosts: (0..workers)
                    .map(|worker| worker * hosts / workers)
                    .collect(),
                in_flight: Vec::new(),
                lag_notices: Vec::new(),
                rng: StdRng::seed_from_u64(seed),
            }
        }

        /// Sends `message` from worker `from`.  It arrives at once on the same
        /// host, and later on another host.
        fn send(&mut self, from: usize, message: ModelMessage) {
            if self.hosts[from] == self.hosts[message.receiver()] {
                self.arrive(message);
            } else {
                self.in_flight.push(message);
            }
        }

        /// Delivers `message`, and sends the lag notices that it triggers.
        fn arrive(&mut self, message: ModelMessage) {
            match message {
                ModelMessage::Flush {
                    sender,
                    receiver,
                    step,
                } => {
                    let batch = Arc::new(DynOrdZSet::dyn_empty(&self.factories));
                    if let Some(lag_step) =
                        self.rxqs[receiver].deliver(&self.factories, sender, batch, true, step)
                    {
                        self.send_lag_notices(receiver, lag_step);
                    }
                }
                ModelMessage::LagNotice {
                    lagger,
                    receiver,
                    step,
                } => self.rxqs[receiver].record_lag_notice(lagger, step),
            }
        }

        /// Sends every receiver a lag notice from `lagger` for `step`.
        fn send_lag_notices(&mut self, lagger: usize, step: u64) {
            assert!(
                !self.lag_notices.contains(&(lagger, step)),
                "worker {lagger} sent lag notices for step {step} twice"
            );
            self.lag_notices.push((lagger, step));
            for receiver in 0..self.rxqs.len() {
                self.send(
                    lagger,
                    ModelMessage::LagNotice {
                        lagger,
                        receiver,
                        step,
                    },
                );
            }
        }

        /// Checks the queue of `worker`, whose receiver waits, as the receiver
        /// does when it wakes up.
        ///
        /// # Returns
        ///
        /// True if the receiver stops waiting.
        fn check(
            &mut self,
            worker: usize,
            step: u64,
            phases: &mut [ModelPhase],
            completed: &mut [Option<u64>],
        ) -> bool {
            match self.rxqs[worker].flush_status() {
                FlushStatus::Complete(_) => {
                    completed[worker] = Some(step);
                    phases[worker] = ModelPhase::Done;
                    true
                }
                FlushStatus::Lagging => {
                    phases[worker] = ModelPhase::Done;
                    true
                }
                FlushStatus::Pending => false,
            }
        }

        /// Runs `step`, in which worker `w`'s sender flushes if
        /// `flush_steps[w]` is `step`.
        ///
        /// # Arguments
        ///
        /// * `step` - the step to run.
        /// * `flush_steps` - the step in which each worker's sender flushes.
        /// * `completed` - for each worker, the step in which its receiver
        ///   output the transaction, if it has.  This sets the entry of each
        ///   receiver that outputs it in `step`.
        ///
        /// # Panics
        ///
        /// Panics if the step cannot end because receivers wait for messages
        /// that never come.
        fn run_step(&mut self, step: u64, flush_steps: &[u64], completed: &mut [Option<u64>]) {
            let workers = self.rxqs.len();
            let mut phases = vec![ModelPhase::Sender; workers];
            loop {
                for (worker, rxq) in self.rxqs.iter().enumerate() {
                    assert_eq!(
                        rxq.flushes_queued.load(Ordering::Relaxed),
                        rxq.any_flushed(),
                        "worker {worker}'s flag disagrees with its queue in step {step}"
                    );
                }
                let running = phases
                    .iter()
                    .filter(|phase| matches!(phase, ModelPhase::Sender | ModelPhase::Receiver))
                    .count();
                let waiting = (0..workers)
                    .filter(|&worker| phases[worker] == ModelPhase::Waiting)
                    .collect_vec();
                if running == 0 && waiting.is_empty() {
                    // Messages in flight can arrive in a later step.
                    return;
                }
                if running == 0 && self.in_flight.is_empty() {
                    let progress = waiting
                        .iter()
                        .any(|&worker| self.check(worker, step, &mut phases, completed));
                    assert!(
                        progress,
                        "receivers {waiting:?} wait forever in step {step}"
                    );
                    continue;
                }

                let choice = self.rng.gen_range(0..workers + self.in_flight.len());
                if choice >= workers {
                    let message = self.in_flight.swap_remove(choice - workers);
                    self.arrive(message);
                    continue;
                }
                let worker = choice;
                match phases[worker] {
                    ModelPhase::Sender => {
                        assert_eq!(self.rxqs[worker].sender_step(), step);
                        if flush_steps[worker] == step {
                            for receiver in 0..workers {
                                self.send(
                                    worker,
                                    ModelMessage::Flush {
                                        sender: worker,
                                        receiver,
                                        step,
                                    },
                                );
                            }
                        } else if self.rxqs[worker].flushes_queued.load(Ordering::Relaxed)
                            && self.rng.gen_bool(0.5)
                            && let Some(lag_step) = self.rxqs[worker].sender_lags()
                        {
                            // A sender looks for a flush only when its flag
                            // says that one is queued, and its read of the
                            // flag can miss a flush that has just arrived.
                            self.send_lag_notices(worker, lag_step);
                        }
                        phases[worker] = ModelPhase::Receiver;
                    }
                    ModelPhase::Receiver => {
                        let (receiver_step, lags) = self.rxqs[worker].start_step();
                        assert_eq!(receiver_step, step);
                        if lags {
                            self.send_lag_notices(worker, step);
                        }
                        let flushing = flush_steps[worker] <= step && completed[worker].is_none();
                        phases[worker] = if flushing {
                            ModelPhase::Waiting
                        } else {
                            assert!(self.rxqs[worker].receive().is_none());
                            ModelPhase::Done
                        };
                    }
                    ModelPhase::Waiting => {
                        self.check(worker, step, &mut phases, completed);
                    }
                    ModelPhase::Done => (),
                }
            }
        }
    }

    /// Runs `transactions` through [FlushModel], with `workers` workers on
    /// `hosts` hosts, drawing the order of events from `seed`.
    ///
    /// Each transaction starts in the step after the previous one ends.  Each
    /// is a pair `(gap, delays)`: worker `w`'s sender flushes `gap + delays[w]`
    /// steps after the transaction starts.
    ///
    /// Checks that every receiver outputs each transaction in the step of its
    /// last flush, and that a worker sends lag notices only for steps in which
    /// its sender has not flushed but another sender has.  With one host, a
    /// worker must send them for every such step.
    fn check_flush_model(
        workers: usize,
        hosts: usize,
        transactions: &[(u64, Vec<u64>)],
        seed: u64,
    ) {
        let mut model = FlushModel::new(workers, hosts, seed);
        let mut step = 1;
        for (gap, delays) in transactions {
            let flush_steps = delays.iter().map(|delay| step + gap + delay).collect_vec();
            let first_flush = *flush_steps.iter().min().unwrap();
            let last_flush = *flush_steps.iter().max().unwrap();
            let notices_before = model.lag_notices.len();

            let mut completed = vec![None; workers];
            while completed.iter().any(Option::is_none) {
                model.run_step(step, &flush_steps, &mut completed);
                step += 1;
            }
            assert_eq!(completed, vec![Some(last_flush); workers]);

            let notices = model.lag_notices[notices_before..]
                .iter()
                .copied()
                .sorted()
                .collect_vec();
            let lagging_steps = (0..workers)
                .flat_map(|worker| {
                    (first_flush..flush_steps[worker]).map(move |step| (worker, step))
                })
                .sorted()
                .collect_vec();
            if hosts == 1 {
                assert_eq!(notices, lagging_steps);
            } else {
                for notice in &notices {
                    assert!(
                        lagging_steps.contains(notice),
                        "lag notice {notice:?} for flushes in steps {flush_steps:?}"
                    );
                }
            }
        }
    }

    /// Model cases: the numbers of workers and hosts, the transactions for
    /// [check_flush_model], and a seed for the order of events.
    fn flush_model_cases() -> impl Strategy<Value = (usize, usize, Vec<(u64, Vec<u64>)>, u64)> {
        (2usize..=5).prop_flat_map(|workers| {
            (
                Just(workers),
                1..=workers,
                prop::collection::vec((0u64..=2, prop::collection::vec(0u64..=3, workers)), 1..=3),
                any::<u64>(),
            )
        })
    }

    proptest! {
        /// The flush protocol ends every step, outputs every transaction in
        /// the step of its last flush, and sends only the lag notices that it
        /// must, in whatever order the workers act and messages arrive.
        #[test]
        fn flush_protocol((workers, hosts, transactions, seed) in flush_model_cases()) {
            Runtime::run(CircuitConfig::with_workers(workers), move |_parker| {
                if Runtime::worker_index() == 0 {
                    check_flush_model(workers, hosts, &transactions, seed);
                }
            })
            .unwrap()
            .join()
            .unwrap();
        }
    }

    /// Passes its input through, and finishes each flush `delays[worker]`
    /// steps after the step in which the flush starts, so that the workers'
    /// senders after it flush in different steps.
    struct SlowFlush {
        /// The delay of each worker, in steps.
        delays: Arc<Vec<AtomicUsize>>,

        /// This worker's delay in the current flush.
        delay: usize,

        /// The number of runs since the current flush started, or `None`
        /// before the first flush.
        runs_since_flush: Option<usize>,
    }

    impl SlowFlush {
        fn new(delays: Arc<Vec<AtomicUsize>>) -> Self {
            Self {
                delays,
                delay: 0,
                runs_since_flush: None,
            }
        }
    }

    impl Operator for SlowFlush {
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("SlowFlush")
        }

        fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
            true
        }

        fn flush(&mut self) {
            self.delay = self.delays[Runtime::worker_index()].load(Ordering::Acquire);
            self.runs_since_flush = Some(0);
        }

        fn is_flush_complete(&self) -> bool {
            self.runs_since_flush.is_some_and(|runs| runs > self.delay)
        }
    }

    impl<T> UnaryOperator<T, T> for SlowFlush
    where
        T: Clone + 'static,
    {
        async fn eval(&mut self, input: &T) -> T {
            if let Some(runs) = &mut self.runs_since_flush {
                *runs += 1;
            }
            input.clone()
        }
    }

    /// Passes its input through, but on worker 1 it blocks in the first run
    /// of each flush until the runtime is killed, so that worker 1's sender
    /// after it never flushes.
    struct BlockOnFlush {
        flushing: bool,
    }

    impl Operator for BlockOnFlush {
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("BlockOnFlush")
        }

        fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
            true
        }

        fn flush(&mut self) {
            self.flushing = true;
        }
    }

    impl<T> UnaryOperator<T, T> for BlockOnFlush
    where
        T: Clone + 'static,
    {
        async fn eval(&mut self, input: &T) -> T {
            if self.flushing && Runtime::worker_index() == 1 {
                Runtime::runtime()
                    .unwrap()
                    .cancellation_token()
                    .cancelled_owned()
                    .await;
            }
            input.clone()
        }
    }

    /// The batches that the circuits of the uneven flush tests exchange.
    type TestBatch = DynOrdZSet<DynData>;

    /// The input handle and the accumulator of [delayed_flush_circuit].
    type DelayedFlushHandles = (ZSetHandle<usize>, Arc<ShardedAccumulator<TestBatch>>);

    /// The input handle and the accumulators of [uneven_flush_circuit].
    type UnevenFlushHandles = (ZSetHandle<usize>, Vec<Arc<ShardedAccumulator<TestBatch>>>);

    /// Feeds `input` through `delay`, an operator that can hold up flushes,
    /// into a new enabled sharded accumulator.  The receivers add the number
    /// of records they output to `counts`.
    ///
    /// # Returns
    ///
    /// The receivers' output and the accumulator.
    fn delayed_accumulator<Op>(
        input: &Stream<RootCircuit, TestBatch>,
        delay: Op,
        counts: Arc<Vec<AtomicUsize>>,
    ) -> (
        Stream<RootCircuit, Option<Spine<TestBatch>>>,
        Arc<ShardedAccumulator<TestBatch>>,
    )
    where
        Op: UnaryOperator<TestBatch, TestBatch> + 'static,
    {
        let circuit = input.circuit();
        let input = circuit.add_unary_operator(delay, input);
        let runtime = Runtime::runtime().unwrap();
        let factories = BatchReaderFactories::new::<usize, (), ZWeight>();
        let (accumulation, exchange) = input.new_sharded_accumulator(
            &runtime,
            &factories,
            0..Runtime::num_workers(),
            BatchLayout::default(),
            input.sharded_accumulator_remote_waiter(&runtime),
            Location::caller(),
        );
        let (stream, enable_count) = accumulation.into_parts();
        enable_count.enable();
        circuit.add_sink(OutputCounter { counts }, &stream);
        (stream, exchange)
    }

    /// Builds a circuit that feeds its input through `delay`, an operator that
    /// can hold up flushes, into an enabled sharded accumulator.  The receivers
    /// add the number of records they output to `counts`.
    ///
    /// # Returns
    ///
    /// The input handle and the accumulator.
    fn delayed_flush_circuit<Op>(
        circuit: &mut RootCircuit,
        delay: Op,
        counts: Arc<Vec<AtomicUsize>>,
    ) -> anyhow::Result<DelayedFlushHandles>
    where
        Op: UnaryOperator<TestBatch, TestBatch> + 'static,
    {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        // An input stream is already sharded, so the circuit changes each key
        // to make records move between workers.
        let input = input.map(|key| key + 1).inner();
        let (_output, exchange) = delayed_accumulator(&input, delay, counts);
        Ok((input_handle, exchange))
    }

    /// Builds a circuit of enabled sharded accumulators.  Accumulator `i`
    /// takes the circuit's input if `upstream[i]` is `None`, and the output of
    /// accumulator `j` if it is `Some(j)`, so that its senders run after `j`'s
    /// receivers.  Accumulators that take the input exchange their data at the
    /// same time, over the same connections between hosts.  A [SlowFlush] in
    /// front of accumulator `i` holds up its senders' flushes by `delays[i]`,
    /// and its receivers add the number of records they output to
    /// `counts[i]`.
    ///
    /// # Returns
    ///
    /// The input handle and the accumulators.
    fn uneven_flush_circuit(
        circuit: &mut RootCircuit,
        upstream: &[Option<usize>],
        delays: &[Arc<Vec<AtomicUsize>>],
        counts: &[Arc<Vec<AtomicUsize>>],
    ) -> anyhow::Result<UnevenFlushHandles> {
        let (input, input_handle) = circuit.add_input_zset::<usize>();
        // An input stream is already sharded, so the circuit changes each key
        // to make records move between workers.
        let input = input.map(|key| key + 1).inner();
        let factories = <OrdZSetFactories<DynData>>::new::<usize, (), ZWeight>();
        let mut outputs: Vec<Stream<RootCircuit, Option<Spine<TestBatch>>>> = Vec::new();
        let mut exchanges = Vec::new();
        for (i, from) in upstream.iter().enumerate() {
            let source = match from {
                None => input.clone(),
                Some(j) => {
                    // The spine that accumulator `j` outputs, as one batch.
                    let factories = factories.clone();
                    outputs[*j].apply(move |spine: &Option<Spine<TestBatch>>| {
                        spine
                            .as_ref()
                            .and_then(|spine| spine.fork().consolidate())
                            .unwrap_or_else(|| TestBatch::dyn_empty(&factories))
                    })
                }
            };
            let (output, exchange) = delayed_accumulator(
                &source,
                SlowFlush::new(delays[i].clone()),
                counts[i].clone(),
            );
            outputs.push(output);
            exchanges.push(exchange);
        }
        Ok((input_handle, exchanges))
    }

    /// For each transaction in [test_uneven_flushes], how many steps after the
    /// commit starts each worker's sender flushes.
    fn flush_delays(workers: usize) -> Vec<Vec<usize>> {
        vec![
            vec![0; workers],
            (0..workers).collect(),
            (0..workers).rev().collect(),
            (0..workers)
                .map(|worker| if worker == workers - 1 { 4 } else { 0 })
                .collect(),
            (0..workers).map(|worker| worker % 2 * 2).collect(),
            vec![1; workers],
        ]
    }

    /// The number of lag notices that the workers send if their senders flush
    /// `delays` steps after the commit starts: one for each step in which a
    /// worker's sender has not flushed but another worker's sender has.
    fn expected_lag_notices(delays: &[usize]) -> usize {
        let first = *delays.iter().min().unwrap();
        let last = *delays.iter().max().unwrap();
        (first..last)
            .map(|step| delays.iter().filter(|&&delay| delay > step).count())
            .sum()
    }

    /// Returns the number of lag notices that the operators of `test_hosts`
    /// report in their circuit profiles.
    fn profiled_lag_notices<T>(test_hosts: &mut TestHosts<T>) -> usize
    where
        T: Send + 'static,
    {
        test_hosts
            .map(|handle| {
                let profile = handle.retrieve_profile().unwrap();
                profile
                    .worker_profiles
                    .iter()
                    .flat_map(|worker| worker.attribute_profile(&LAG_NOTICES_COUNT))
                    // The root circuit's entry sums those of its operators.
                    .filter(|(id, _)| !id.path().is_empty())
                    .map(|(_, count)| match count {
                        MetaItem::Count(count) => count,
                        other => panic!("lag notice count {other:?}"),
                    })
                    .sum::<usize>()
            })
            .into_iter()
            .sum()
    }

    /// Commits transactions in which the workers' senders flush in different
    /// steps, as [flush_delays] says, with `workers` workers on `hosts` hosts,
    /// through the accumulators of the [uneven_flush_circuit] that `upstream`
    /// describes.  In each transaction, accumulator `i` takes the delays that
    /// [flush_delays] lists `i` places after those of accumulator 0.
    ///
    /// Checks that each accumulator's receivers output every record in the
    /// step of its last flush.  Also checks that the workers send lag notices
    /// only when a sender lags behind another sender's flush: on one host,
    /// exactly once for each such step.  On several hosts, a flush can reach a
    /// lagging worker only after that worker's step ends.  The worker then
    /// sends its notice for that step late, or none for it if by then its
    /// receiver has run in the next step or its sender has flushed.
    ///
    /// With `spill`, every host runs under memory pressure, so that each batch
    /// and flush from another host spills to storage as it arrives (see
    /// [ShardedAccumulator::deliver_received]), and lag notices queue behind
    /// it.
    fn test_uneven_flushes(workers: usize, hosts: usize, upstream: &[Option<usize>], spill: bool) {
        const RECORDS: usize = 16;

        let new_counters = || Arc::new((0..workers).map(|_| AtomicUsize::new(0)).collect_vec());
        let delays = upstream.iter().map(|_| new_counters()).collect_vec();
        let counts = upstream.iter().map(|_| new_counters()).collect_vec();
        let storage = tempdir().unwrap();
        let storage_path = storage.path().to_path_buf();
        let mut test_hosts = TestHosts::start_with(
            workers,
            hosts,
            move |host, config| {
                if spill {
                    // A one-byte budget puts the host under critical memory
                    // pressure.
                    with_host_storage(config, &storage_path, host).with_max_rss_bytes(Some(1))
                } else {
                    config
                }
            },
            {
                let upstream = upstream.to_vec();
                let (delays, counts) = (delays.clone(), counts.clone());
                move |circuit| uneven_flush_circuit(circuit, &upstream, &delays, &counts)
            },
        );
        if spill {
            let deadline = Instant::now() + Duration::from_secs(10);
            while test_hosts
                .map(|handle| handle.runtime().current_memory_pressure())
                .into_iter()
                .any(|pressure| pressure < MemoryPressure::High)
            {
                assert!(Instant::now() < deadline, "memory pressure stayed low");
                sleep(Duration::from_millis(10));
            }
        }
        let lag_notices_sent = |test_hosts: &TestHosts<UnevenFlushHandles>, accumulator: usize| {
            test_hosts
                .results()
                .iter()
                .map(|(_input, exchanges)| {
                    let exchange = &exchanges[accumulator];
                    exchange
                        .local_workers
                        .clone()
                        .map(|worker| exchange.lag_notices_sent(worker))
                        .sum::<usize>()
                })
                .sum::<usize>()
        };

        let patterns = flush_delays(workers);
        let mut next_key = 0;
        for transaction in 0..patterns.len() {
            let transaction_delays = (0..upstream.len())
                .map(|i| &patterns[(transaction + i) % patterns.len()])
                .collect_vec();
            for (accumulator_delays, values) in delays.iter().zip(&transaction_delays) {
                for (delay, &value) in accumulator_delays.iter().zip(values.iter()) {
                    delay.store(value, Ordering::Release);
                }
            }
            let notices_before = (0..upstream.len())
                .map(|i| lag_notices_sent(&test_hosts, i))
                .collect_vec();

            test_hosts.start_transaction();
            for _ in 0..RECORDS {
                test_hosts.result(next_key % hosts).0.push(next_key, 1);
                next_key += 1;
            }
            test_hosts.for_each(|handle| handle.start_commit_transaction().unwrap());
            let mut steps = 1;
            while !test_hosts.step() {
                steps += 1;
            }

            let context = format!(
                "workers={workers} hosts={hosts} upstream={upstream:?} delays={transaction_delays:?}"
            );
            // An accumulator finishes in the step of its last flush.  One that
            // takes another's output starts to flush in the step in which the
            // other finishes.
            let mut finish = Vec::new();
            for (i, source) in upstream.iter().enumerate() {
                let start = source.map_or(0, |j| finish[j]);
                finish.push(start + transaction_delays[i].iter().max().unwrap());
            }
            assert_eq!(
                steps,
                finish.iter().max().unwrap() + 1,
                "commit steps, {context}"
            );
            for (i, counts) in counts.iter().enumerate() {
                let received: usize = counts
                    .iter()
                    .map(|count| count.swap(0, Ordering::AcqRel))
                    .sum();
                assert_eq!(received, RECORDS, "records of accumulator {i}, {context}");

                let notices = lag_notices_sent(&test_hosts, i) - notices_before[i];
                let expected = expected_lag_notices(transaction_delays[i]);
                if hosts == 1 {
                    assert_eq!(
                        notices, expected,
                        "lag notices of accumulator {i}, {context}"
                    );
                } else {
                    assert!(
                        notices <= expected && (notices > 0) == (expected > 0),
                        "{notices} lag notices of accumulator {i}, expected up to {expected}, \
                         {context}"
                    );
                }
            }
        }

        // The receivers report the same counts in the circuit profiles.
        let sent: usize = (0..upstream.len())
            .map(|i| lag_notices_sent(&test_hosts, i))
            .sum();
        assert_eq!(
            profiled_lag_notices(&mut test_hosts),
            sent,
            "profiled lag notices, workers={workers} hosts={hosts}"
        );
        test_hosts.kill();
    }

    #[test]
    fn uneven_flushes_single_host() {
        test_uneven_flushes(4, 1, &[None], false);
    }

    #[test]
    fn uneven_flushes_multihost() {
        for (workers, hosts) in [(4, 2), (3, 3)] {
            test_uneven_flushes(workers, hosts, &[None], false);
        }
    }

    /// Uneven flushes in three accumulators: the second takes the output of
    /// the first, so that its senders wait for receivers that wait for lag
    /// notices, and the third exchanges at the same time as the first, over
    /// the same connections between hosts.
    #[test]
    fn uneven_flushes_in_three_accumulators() {
        for (workers, hosts) in [(4, 1), (4, 2), (3, 3)] {
            test_uneven_flushes(workers, hosts, &[None, Some(0), None], false);
        }
    }

    /// Uneven flushes on hosts that spill each batch and flush from another
    /// host as it arrives, while lag notices queue behind it.
    #[test]
    fn uneven_flushes_spilled_on_arrival() {
        test_uneven_flushes(4, 2, &[None, Some(0), None], true);
    }

    /// The handles of [delayed_reachability_circuit].
    type ReachabilityHandles = (
        ZSetHandle<Tup2<usize, usize>>,
        ZSetHandle<usize>,
        OutputHandle<OrdZSet<usize>>,
    );

    /// Builds a circuit that finds the nodes reachable from `roots` along
    /// `edges` in a recursive scope, like [sharded_accumulator_nested_circuit],
    /// with a [SlowFlush] that holds up the flushes of the join's input as
    /// `delays` says.  So the senders of the join's sharded accumulator flush
    /// in different steps of each iteration.
    fn delayed_reachability_circuit(
        circuit: &mut RootCircuit,
        delays: Arc<Vec<AtomicUsize>>,
    ) -> anyhow::Result<ReachabilityHandles> {
        let (edges, edges_handle) = circuit.add_input_zset::<Tup2<usize, usize>>();
        let (roots, roots_handle) = circuit.add_input_zset::<usize>();
        let reachable = circuit.recursive(|child, reachable: Stream<_, OrdZSet<usize>>| {
            let edges = edges.delta0(child).map_index(|Tup2(from, to)| (*from, *to));
            let roots = roots.delta0(child);
            let reachable = child.add_unary_operator(SlowFlush::new(delays.clone()), &reachable);
            Ok(reachable
                .map_index(|node| (*node, ()))
                .join(&edges, |_from, _, to| *to)
                .plus(&roots))
        })?;
        Ok((edges_handle, roots_handle, reachable.output()))
    }

    /// Finds the nodes reachable along a chain with
    /// [delayed_reachability_circuit], with `workers` workers on `hosts`
    /// hosts, each worker's flushes delayed by as many steps as its index.
    ///
    /// Checks that the circuit finds every node, and that the workers send
    /// lag notices in the recursive scope.
    fn test_uneven_flushes_in_recursive_scope(workers: usize, hosts: usize) {
        const N_NODES: usize = 16;

        let delays = Arc::new((0..workers).map(AtomicUsize::new).collect_vec());
        let mut test_hosts = TestHosts::start_with(
            workers,
            hosts,
            |_host, config| config.with_streaming_exchange(true),
            move |circuit| delayed_reachability_circuit(circuit, delays.clone()),
        );
        test_hosts.result(0).0.append(
            &mut (0..N_NODES - 1)
                .map(|node| Tup2(Tup2(node, node + 1), 1))
                .collect(),
        );
        test_hosts.result(0).1.push(0, 1);
        test_hosts.transaction();

        let mut reachable = BTreeMap::<usize, ZWeight>::new();
        for (_edges, _roots, output) in test_hosts.results() {
            for (node, (), weight) in output.consolidate().iter() {
                *reachable.entry(node).or_default() += weight;
            }
        }
        let context = format!("{workers} workers on {hosts} hosts");
        assert_eq!(
            reachable,
            (0..N_NODES)
                .map(|node| (node, 1))
                .collect::<BTreeMap<_, _>>(),
            "{context}"
        );
        assert!(
            profiled_lag_notices(&mut test_hosts) > 0,
            "no lag notices, {context}"
        );
        test_hosts.kill();
    }

    #[test]
    fn uneven_flushes_in_recursive_scope() {
        for (workers, hosts) in [(4, 1), (4, 2)] {
            test_uneven_flushes_in_recursive_scope(workers, hosts);
        }
    }

    /// A sender that runs without flushing after another sender has flushed
    /// sends the lag notice itself, ahead of its data, and its receiver does
    /// not send a second one for the step.
    #[test]
    fn lagging_sender_sends_lag_notice() {
        Runtime::run(CircuitConfig::with_workers(2), |_parker| {
            if Runtime::worker_index() != 0 {
                return;
            }
            let factories = <OrdZSetFactories<DynData>>::new::<usize, (), ZWeight>();
            let exchange = ShardedAccumulator::<DynOrdZSet<DynData>>::with_runtime(
                &Runtime::runtime().unwrap(),
                0..2,
                Runtime::runtime()
                    .unwrap()
                    .sequence_next()
                    .try_into()
                    .unwrap(),
                &factories,
                BatchLayout::default(),
            );
            let name = Arc::new("Sender".to_string());

            // With no flush queued, the sender has nothing to report.
            futures::executor::block_on(exchange.sender_lags(name.clone()));
            assert_eq!(exchange.lag_notices_sent(0), 0);

            // Worker 1's flush in step 1 reaches worker 0 before worker 0's
            // sender runs in that step.
            let flush = Arc::new(DynOrdZSet::dyn_empty(&factories));
            assert_eq!(exchange.enqueue(1, 0, flush, true, 1), None);
            futures::executor::block_on(exchange.sender_lags(name));
            assert_eq!(exchange.lag_notices_sent(0), 1);
            assert_eq!(exchange.rxq(1).lag_notices[0], 1);

            assert_eq!(exchange.rxq(0).start_step(), (1, false));
            assert_eq!(exchange.lag_notices_sent(0), 1);
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// A flush that reaches a lagging worker after its sender looked for one,
    /// but before its receiver runs, makes the receiver send the lag notice.
    /// The flush's arrival cannot send it, because the worker has not yet
    /// recorded that it lags in the step.
    #[test]
    fn lagging_receiver_sends_lag_notice() {
        Runtime::run(CircuitConfig::with_workers(2), |_parker| {
            if Runtime::worker_index() != 0 {
                return;
            }
            let factories = <OrdZSetFactories<DynData>>::new::<usize, (), ZWeight>();
            let exchange = ShardedAccumulator::<DynOrdZSet<DynData>>::with_runtime(
                &Runtime::runtime().unwrap(),
                0..2,
                Runtime::runtime()
                    .unwrap()
                    .sequence_next()
                    .try_into()
                    .unwrap(),
                &factories,
                BatchLayout::default(),
            );

            // Worker 0's sender runs in step 1 with no flush queued.
            futures::executor::block_on(exchange.sender_lags(Arc::new("Sender".to_string())));

            // Worker 1's flush in step 1 arrives before worker 0's receiver
            // runs.
            let flush = Arc::new(DynOrdZSet::dyn_empty(&factories));
            assert_eq!(exchange.enqueue(1, 0, flush, true, 1), None);
            assert_eq!(exchange.lag_notices_sent(0), 0);

            // Worker 0's receiver runs in step 1 and sends the notice.
            assert!(futures::executor::block_on(exchange.receive(false)).is_none());
            assert_eq!(exchange.lag_notices_sent(0), 1);
            assert_eq!(exchange.rxq(1).lag_notices[0], 1);
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// A flush that reaches a lagging worker on another host after the flush's
    /// step has ended makes the worker owe a lag notice for that step, which
    /// cannot end a wait in a later step.
    #[test]
    fn late_flush_owes_late_lag_notice() {
        // Worker 0 lags, worker 1 flushes, and worker 2, which also lags,
        // ends worker 1's wait in the flush's step.
        Runtime::run(CircuitConfig::with_workers(3), |_parker| {
            if Runtime::worker_index() != 0 {
                return;
            }
            let factories = <OrdZSetFactories<DynData>>::new::<usize, (), ZWeight>();
            let new_rxq = |worker| {
                Rxq::<DynOrdZSet<DynData>>::new(
                    &Runtime::runtime().unwrap(),
                    worker,
                    &factories,
                    3,
                    Arc::new(format!("Rxq{worker}")),
                    BatchLayout::default(),
                    Arc::new(AtomicBool::new(false)),
                )
            };
            let flush = || Arc::new(DynOrdZSet::dyn_empty(&factories));
            let (mut lagger, mut flusher) = (new_rxq(0), new_rxq(1));

            // Step 1: worker 1 flushes, and worker 0's receiver runs before
            // the flush reaches it.  Worker 2's notice ends worker 1's wait.
            assert_eq!(flusher.deliver(&factories, 1, flush(), true, 1), None);
            assert_eq!(flusher.start_step(), (1, false));
            assert_eq!(lagger.start_step(), (1, false));
            flusher.record_lag_notice(2, 1);
            assert!(matches!(flusher.flush_status(), FlushStatus::Lagging));

            // Step 2: the flush reaches worker 0 before its receiver runs, and
            // worker 0 owes a notice for step 1, which has ended.  That notice
            // does not end worker 1's wait in step 2.
            assert_eq!(lagger.deliver(&factories, 1, flush(), true, 1), Some(1));
            assert_eq!(flusher.start_step(), (2, false));
            flusher.record_lag_notice(0, 1);
            assert!(matches!(flusher.flush_status(), FlushStatus::Pending));

            // Worker 0's receiver runs and owes the notice for step 2, which
            // does.
            assert_eq!(lagger.start_step(), (2, true));
            flusher.record_lag_notice(0, 2);
            assert!(matches!(flusher.flush_status(), FlushStatus::Lagging));
        })
        .unwrap()
        .join()
        .unwrap();
    }

    /// Checks that killing the runtimes ends a receiver's wait for a flush
    /// that never comes, with `workers` workers on `hosts` hosts.  Worker 1
    /// blocks before its sender flushes, so worker 0's receiver waits for it.
    fn test_kill_ends_flush_wait(workers: usize, hosts: usize) {
        let (exchange_sender, exchange_receiver) = channel();
        let runtimes = host_configs(workers, hosts)
            .into_iter()
            .map(|config| {
                let exchange_sender = exchange_sender.clone();
                Runtime::run(config, move |_parker| {
                    let (circuit, (_input, exchange)) = RootCircuit::build(|circuit| {
                        delayed_flush_circuit(
                            circuit,
                            BlockOnFlush { flushing: false },
                            Arc::new((0..workers).map(|_| AtomicUsize::new(0)).collect()),
                        )
                    })
                    .unwrap();
                    if Runtime::worker_index() == 0 {
                        exchange_sender.send(exchange).unwrap();
                    }
                    circuit.start_transaction().unwrap();
                    circuit.start_commit_transaction().unwrap();
                    // The step fails when the runtime is killed.
                    let _ = circuit.step();
                })
                .unwrap()
            })
            .collect_vec();

        // Wait until worker 0's sender has flushed and its receiver has
        // started the step, in which it waits for worker 1's flush.
        let exchange = exchange_receiver.recv().unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let rxq = exchange.rxq(0);
            if rxq.steps == 1 && rxq.has_flushed(0) {
                break;
            }
            drop(rxq);
            assert!(
                Instant::now() < deadline,
                "worker 0 never started to wait, with {workers} workers on {hosts} hosts"
            );
            sleep(Duration::from_millis(1));
        }

        // Kill on another thread, so that a hang fails the test.
        let (done_sender, done_receiver) = channel();
        std::thread::spawn(move || {
            let results = runtimes
                .into_iter()
                .map(|runtime| runtime.kill())
                .collect_vec();
            done_sender.send(results).unwrap();
        });
        for result in done_receiver
            .recv_timeout(Duration::from_secs(30))
            .unwrap_or_else(|_| {
                panic!("a killed runtime still waits for the flush, with {workers} workers on {hosts} hosts")
            })
        {
            result.unwrap();
        }
    }

    #[test]
    fn kill_ends_flush_wait_single_host() {
        test_kill_ends_flush_wait(2, 1);
    }

    #[test]
    fn kill_ends_flush_wait_multihost() {
        test_kill_ends_flush_wait(2, 2);
    }

    /// The number of joins in [join_chain].
    const CHAIN: u64 = 8;

    /// The number of keys in [join_chain]'s dimension table.
    const KEYS: u64 = 16;

    /// The handles of [join_chain].
    struct JoinChain {
        facts: ZSetHandle<Tup2<u64, u64>>,
        dim: ZSetHandle<Tup2<u64, u64>>,
        output: OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    }

    /// Builds a circuit that runs `facts` through a chain of [CHAIN] joins with
    /// `dim`.  Each join keys its input anew, so each one shards its input
    /// again.
    fn join_chain(circuit: &mut RootCircuit) -> anyhow::Result<JoinChain> {
        let (facts, facts_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
        let (dim, dim_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
        let dim = dim.map_index(|Tup2(key, value)| (*key, *value));
        let mut chain = facts;
        for i in 0..CHAIN {
            chain = chain
                .map_index(|Tup2(a, b)| (*b % KEYS, *a))
                .join(&dim, move |_key, a, d| Tup2(*a, *d + i));
        }
        Ok(JoinChain {
            facts: facts_handle,
            dim: dim_handle,
            output: chain.accumulate_output(),
        })
    }

    /// Commits transactions through [join_chain] with `workers` workers on
    /// `hosts` hosts.
    ///
    /// # Returns
    ///
    /// For each transaction, the number of steps its commit took, and the
    /// chain's output summed over all hosts.
    fn commit_join_chain(
        workers: usize,
        hosts: usize,
    ) -> Vec<(usize, Vec<(Tup2<u64, u64>, ZWeight)>)> {
        let mut test_hosts = TestHosts::start(workers, hosts, join_chain);
        let mut results = Vec::new();
        for transaction in 0..4 {
            test_hosts.start_transaction();
            if transaction == 0 {
                for key in 0..KEYS {
                    test_hosts
                        .result(key as usize % hosts)
                        .dim
                        .push(Tup2(key, key * 7 % KEYS), 1);
                }
            }
            for i in 0..10 {
                test_hosts
                    .result(i as usize % hosts)
                    .facts
                    .push(Tup2(100 * transaction + i, i * 3), 1);
            }
            test_hosts.for_each(|handle| handle.start_commit_transaction().unwrap());
            let mut steps = 1;
            while !test_hosts.step() {
                steps += 1;
            }

            let mut output = BTreeMap::<Tup2<u64, u64>, ZWeight>::new();
            for handles in test_hosts.results() {
                for (key, (), weight) in handles.output.concat().consolidate().iter() {
                    *output.entry(key).or_default() += weight;
                }
            }
            output.retain(|_key, weight| *weight != 0);
            results.push((steps, output.into_iter().collect()));
        }
        test_hosts.kill();
        results
    }

    /// Several workers, on one host or more, commit a chain of joins in as
    /// many steps as one worker, because each receiver finishes in the step
    /// of the last flush.
    #[test]
    fn join_chain_commits_in_as_many_steps_as_one_worker() {
        let expected = commit_join_chain(1, 1);
        for (workers, hosts) in [(4, 1), (4, 2)] {
            assert_eq!(
                commit_join_chain(workers, hosts),
                expected,
                "{workers} workers on {hosts} hosts"
            );
        }
    }
}
