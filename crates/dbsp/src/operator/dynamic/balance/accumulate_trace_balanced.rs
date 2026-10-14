use crate::{
    Circuit, Position, Runtime, RuntimeError, Scope, Stream, Timestamp, ZWeight,
    algebra::{IndexedZSet, ZBatch},
    circuit::{
        GlobalNodeId, NodeId, OwnershipPreference, WithClock,
        bootstrap::register_accumulated_replay_stream,
        circuit_builder::{MetadataExchange, StreamId},
        metadata::{
            ACCUMULATOR_RECORDS_TO_REPARTITION_COUNT, BALANCER_POLICY, BatchSizeStats,
            INPROGRESS_REBALANCING_TIME_SECONDS, INPUT_BATCHES_STATS,
            INTEGRAL_RECORDS_TO_REPARTITION_COUNT, KEY_DISTRIBUTION, LOCAL_SHARD_RECORDS_COUNT,
            MetaItem, OperatorLocation, OperatorMeta, RABALANCINGS_COUNT, REBALANCING_IN_PROGRESS,
            TOTAL_REBALANCING_TIME_SECONDS,
        },
        operator_traits::{FileOperatorCheckpoint, Operator, OperatorCheckpoint},
        splitter_output_chunk_size, splitter_output_first_chunk_size,
    },
    circuit_cache_key, default_hasher,
    dynamic::{ClonableTrait, Data as _, Erase},
    operator::{
        async_stream_operators::{StreamingBinarySinkOperator, StreamingBinarySinkWrapper},
        dynamic::{
            accumulate_trace::{AccumulateTraceAppend, AccumulateZ1Trace},
            balance::{Balancer, PartitioningPolicy},
            sharded_accumulator::ShardedAccumulator,
            trace::{DelayedTraceId, TimedSpine, TraceBounds},
        },
        require_persistent_id,
    },
    trace::{
        Batch, BatchLayout, BatchReader, BatchReaderFactories, Builder, Cursor, MergeCursor, Spine,
        SpineSnapshot, TupleBuilder, WithSnapshot, spine_async::SpineCursor,
    },
};
use async_stream::stream;
use feldera_storage::{StoragePath, fbuf::FBuf};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cell::{Cell, RefCell},
    fmt::{Display, Formatter},
    hash::Hasher,
    marker::PhantomData,
    panic::Location,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::info;

circuit_cache_key!(BalancedTraceId<C: Circuit, B: IndexedZSet>(StreamId => (Stream<C, Option<Spine<B>>>, Stream<C, TimedSpine<B, C>>)));

/// This is used in a heuristic to determine whether to flush the operator or wait for all
/// other operators in the same layer. See `ready_to_commit`.
const LONG_TRANSACTION_THRESHOLD: usize = 2;

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: IndexedZSet,
{
    /// Shard and integrate `self` using dynamic sharding policy selected by the balancer.
    ///
    /// Combines exchange, accumulator, and integral into a single operator. In steady state,
    /// applies the selected sharding policy to the input stream. When the sharding policy changes,
    /// retracts the previous content of the integral and re-distributes it using the new policy.
    ///
    /// # Returns
    ///
    /// The output of the accumulator and the integral.
    ///
    /// # Circuit
    ///
    /// ```text
    ///                                      │
    ///                                      │self
    ///                                      ▼
    ///                         ┌─────────────────────────┐
    ///              ┌─────────►│RebalancingExchangeSender│
    ///              │          └────────────┬────────────┘
    ///              │                       .
    ///              │                       . ShardedAccumulator
    ///              │                       ▼
    ///              │          ┌──────────────────────────┐
    ///              │          │ShardedAccumulatorReceiver│
    /// delayed_trace│          └────────────┬─────────────┘
    ///              │                       │
    ///              │                       │────────────────────┐
    ///              ├──────────────────┐    │                    │
    ///              │                  ▼    ▼                    │
    ///      ┌───────┴─────────┐    ┌─────────────────────┐       │
    ///      │AccumulateZ1Trace│◄───┤AccumulateTraceAppend│       │
    ///      └─────────────────┘    └────────┬────────────┘       │
    ///                                      │                    │
    ///                                      │trace               │accumulator_stream
    ///                                      │                    │
    ///                                      ▼                    ▼
    /// ```
    ///
    /// The sender and the receiver share a `ShardedAccumulator`.  The sender
    /// partitions its input by the current policy and sends it to every
    /// worker's receive queue.  When the policy changes, the sender takes back
    /// what its own worker has received in the current transaction and reads
    /// `delayed_trace` to retract the integral, so that it can redistribute
    /// both under the new policy.
    ///
    /// # Panics
    ///
    /// Panics if the runtime has more than one worker but no streaming
    /// exchange (see [`Runtime::streaming_exchange`]).
    #[track_caller]
    pub fn dyn_accumulate_trace_balanced(
        &self,
        trace_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> (Stream<C, Option<Spine<B>>>, Stream<C, TimedSpine<B, C>>) {
        if Runtime::num_workers() == 1 {
            let trace = self.dyn_accumulate_trace(trace_factories, batch_factories);
            return (
                self.dyn_accumulate(batch_factories).into_enabled_stream(),
                trace,
            );
        }

        let location = Location::caller();

        self.circuit()
            .cache_get_or_insert_with(BalancedTraceId::new(self.stream_id()), || {
                let circuit = self.circuit();
                circuit.region("accumulate_trace_with_balancer", || {
                    let runtime = Runtime::runtime().unwrap();
                    assert!(
                        runtime.streaming_exchange(),
                        "adaptive joins need the streaming exchange and microsteps"
                    );
                    let worker_index = Runtime::worker_index();
                    let balancer = circuit.balancer();

                    let persistent_id = self.get_persistent_id();

                    // Accumulator.  Its output is not sharded by key, because
                    // the sender partitions by the current policy.
                    let remote_waiter_node_id = self.sharded_accumulator_remote_waiter(&runtime);
                    let (accumulator_stream, exchange) = self.add_sharded_accumulator_receiver(
                        &runtime,
                        batch_factories,
                        0..Runtime::num_workers(),
                        BatchLayout::default(),
                        remote_waiter_node_id,
                        location,
                    );

                    // Integral.
                    let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                        persistent_id
                            .clone()
                            .map(|name| format!("{name}.balanced.integral"))
                            .as_deref(),
                        AccumulateZ1Trace::new(
                            trace_factories,
                            batch_factories,
                            false,
                            circuit.root_scope(),
                            TraceBounds::unbounded(),
                        ),
                    );

                    // The integral replays straight into the accumulator's output,
                    // bypassing the exchange and the accumulator.  The replay stays
                    // local to each worker, which keeps the integral partitioned as
                    // its checkpointed policy requires (under `Broadcast`, every
                    // worker replays its own full copy).
                    let replay_stream = z1feedback
                        .operator_mut()
                        .prepare_accumulated_replay_stream(&accumulator_stream);

                    let trace = circuit.add_binary_operator_with_preference(
                        <AccumulateTraceAppend<TimedSpine<B, C>, B, C>>::new(
                            trace_factories,
                            circuit.clone(),
                        ),
                        (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                        (&accumulator_stream, OwnershipPreference::PREFER_OWNED),
                    );

                    z1feedback.connect_with_preference(
                        &trace,
                        OwnershipPreference::STRONGLY_PREFER_OWNED,
                    );

                    register_accumulated_replay_stream(
                        circuit,
                        &accumulator_stream,
                        &replay_stream,
                        batch_factories,
                    );

                    circuit.cache_insert(
                        DelayedTraceId::new(trace.stream_id()),
                        delayed_trace.clone(),
                    );

                    // Exchange sender.
                    let exchange_sender = RebalancingExchangeSender::<B, C>::new(
                        batch_factories,
                        worker_index,
                        Some(location),
                        exchange,
                        balancer.clone(),
                        circuit.metadata_exchange().clone(),
                        self.local_node_id(),
                    );

                    let sender_node_id = circuit.add_binary_sink(
                        StreamingBinarySinkWrapper::new(exchange_sender),
                        self,
                        &delayed_trace,
                    );

                    // Exchange sender needs a persistent id.
                    circuit.set_persistent_node_id(
                        &sender_node_id,
                        persistent_id
                            .map(|persistent_id| {
                                format!("{persistent_id}.balanced_exchange_sender")
                            })
                            .as_deref(),
                    );

                    // The sender runs before the receiver in every step.
                    circuit.add_dependency(
                        sender_node_id.local_node_id().unwrap(),
                        accumulator_stream.local_node_id(),
                    );

                    // Register integral with the balancer.
                    balancer.register_integral(
                        self.local_node_id(),
                        sender_node_id.local_node_id().unwrap(),
                        self.get_persistent_id(),
                    );

                    (accumulator_stream, trace)
                })
            })
            .clone()
    }
}

/// The old contents of the integral and accumulator that needs to be rebalanced
/// according to the new policy.
struct RebalanceState<B: Batch<Time = ()>> {
    /// Accumulator state doesn't need to be retracted, because it will be cleared when the rebalancing starts.
    /// It only needs to be re-distributed using the new policy.
    accumulator: SpineSnapshot<B>,

    /// The policy that was used to accumulate the integral before the rebalancing.
    /// This is the policy used during the previous transaction.
    trace_policy: PartitioningPolicy,

    /// True if `trace_policy` differs from the current policy, i.e., the integral
    /// must be retracted and redistributed before the transaction commits.
    rebalance_trace: bool,
}

/// Information about the key distribution of the input stream.
// TODO: this can be refined in the future, e.g., it may be a useful to track heavy keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct KeyDistribution {
    /// Total weight of the data in each shard if we were using key-based sharding.
    // Ideally we would track the number of distinct records in each shard, but that
    // requires much more state.
    pub sizes: Vec<ZWeight>,
}

impl Display for KeyDistribution {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.sizes)
    }
}

impl KeyDistribution {
    pub fn new(shards: usize) -> Self {
        Self {
            sizes: vec![0; shards],
        }
    }

    /// Add `weight` to `shard`.
    pub fn update(&mut self, shard: usize, weight: ZWeight) {
        self.sizes[shard] += weight;
    }

    /// Combine two distributions.
    pub fn merge(&mut self, other: &Self) {
        for (i, size) in self.sizes.iter_mut().enumerate() {
            *size += other.sizes[i];
        }
    }

    /// Total weight of records across all shards.
    pub fn total_records(&self) -> ZWeight {
        self.sizes.iter().sum()
    }

    pub fn meta_item(&self) -> MetaItem {
        MetaItem::Array(
            self.sizes
                .iter()
                .map(|size| MetaItem::Count(*size as usize))
                .collect(),
        )
    }
}

/// Checkpointed state of the ExchangeSender operator.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RebalanceExchangeSenderCheckpoint {
    current_policy: Option<PartitioningPolicy>,
    key_distribution: KeyDistribution,
}

/// Exchange sender that rebalances the input stream based on the dynamically selected sharding policy.
pub struct RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
    C: WithClock + 'static,
{
    batch_factories: B::Factories,

    global_id: GlobalNodeId,
    global_id_string: Arc<String>,

    /// Origin node of the input to this operator.
    input_node_id: NodeId,

    worker_index: usize,
    location: OperatorLocation,

    /// The accumulator that this operator shares with the receiver.
    exchange: Arc<ShardedAccumulator<B>>,

    /// Balancer used to select the sharding policy.
    balancer: Balancer,

    /// Metadata exchange used to exchange key distribution and status info
    /// with balancer instances in all workers.
    metadata_exchange: MetadataExchange,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flush_state: Cell<FlushState>,

    /// The number of steps since the transaction started (see `ready_to_commit`).
    num_steps_in_transaction: Cell<usize>,

    /// Current partitioning policy.
    ///
    /// The policy is not set when the operator is first created. It must
    /// be set by the balance before the first step. It is also set when restoring
    /// from a checkpoint.
    current_policy: Cell<Option<PartitioningPolicy>>,

    /// When Some(), the policy has changed during the current transaction, and the operator must rebalance
    /// accumulator and integral state before committing the transaction.
    rebalance_state: RefCell<Option<RebalanceState<B>>>,

    /// The moment rebalancing started.
    ///
    /// None if the policy has changed but flush hasn't yet been invoked, so rebalancing has not started yet.
    rebalance_start_time: Cell<Option<Instant>>,

    /// Number of times the stream was rebalanced.
    num_rebalancings: Cell<usize>,

    /// Size of the accumulator state to be rebalanced.
    rebalance_accumulator_size: Cell<usize>,

    /// Size of the integral state to be rebalanced.
    rebalance_integral_size: Cell<usize>,

    /// Local key distribution of the input stream computed by the current worker.
    key_distribution: RefCell<KeyDistribution>,

    /// Total time spent rebalancing the stream.
    total_rebalancing_time: Cell<Duration>,

    phantom: PhantomData<C>,
}

impl<B, C> RebalancingExchangeSender<B, C>
where
    B: ZBatch<Time = ()>,
    C: WithClock,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        batch_factories: &B::Factories,
        worker_index: usize,
        location: OperatorLocation,
        exchange: Arc<ShardedAccumulator<B>>,
        balancer: Balancer,
        metadata_exchange: MetadataExchange,
        input_node_id: NodeId,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());
        Self {
            batch_factories: batch_factories.clone(),
            global_id: GlobalNodeId::root(),
            global_id_string: Arc::new("RebalancingExchangeSender".to_string()),
            input_node_id,
            worker_index,
            location,
            exchange,
            balancer,
            metadata_exchange,
            input_batch_stats: BatchSizeStats::new(),
            flush_state: Cell::new(FlushState::FlushCompleted),
            num_steps_in_transaction: Cell::new(0),
            current_policy: Cell::new(None),
            rebalance_state: RefCell::new(None),
            rebalance_start_time: Cell::new(None),
            num_rebalancings: Cell::new(0),
            rebalance_accumulator_size: Cell::new(0),
            rebalance_integral_size: Cell::new(0),
            key_distribution: RefCell::new(KeyDistribution::new(Runtime::num_workers())),
            total_rebalancing_time: Cell::new(Duration::ZERO),
            phantom: PhantomData,
        }
    }

    fn rebalancing_in_progress(&self) -> bool {
        self.flush_state.get() == FlushState::RebalanceInProgress
    }

    /// Hash a key/value pair for use with Policy::Balance.
    fn hash_key_val(key: &B::Key, val: &B::Val) -> u64 {
        let mut hasher = default_hasher();
        key.dyn_hash(&mut hasher);
        val.dyn_hash(&mut hasher);
        hasher.finish()
    }

    /// A heuristic to determine whether to flush the operator instantly or to wait for other operators
    /// in the same layer to be in the FlushRequested state.
    ///
    /// Once the operator starts flushing the transaction (which involves rebalancing its state based on
    /// the latest partitioning policy proposed by the balancer and setting state to FlushCompleted), its
    /// partitioning policy is fixed until the next transaction. Consider a large transaction (e.g., backfill).
    /// When committing such a transaction, an operator can receive a flush command long before other operators
    /// in the same join cluster. A partitioning decision based on the current key distributions may become
    /// suboptimal by the time other operators are flushed. It is therefore preferable to wait for other operators
    /// in the cluster to be in the FlushRequested state.  This may not be ideal in the common case of small transactions
    /// that would normally be committed in a single step. If every operator waits for its peers, such transactions
    /// will require at least two steps (since status exchange between operators has a delay of one step).
    ///
    /// There is an additional complication: since operators in a cluster can have mutual dependencies, they may
    /// not be in a FlushRequested state at the same time. We address this by splitting the cluster into layers and only
    /// waiting for operators in the same layer to be in the FlushRequested state (Balancer::ready_to_commit).
    fn ready_to_commit(&self) -> bool {
        self.flush_state.get() == FlushState::FlushRequested
            && (self.balancer.ready_to_commit(self.input_node_id)
                || self.num_steps_in_transaction.get() < LONG_TRANSACTION_THRESHOLD)
    }

    fn update_exchange_metadata(&self) {
        let metadata = RebalancingExchangeSenderExchangeMetadata {
            flush_state: self.flush_state.get(),
            current_policy: self.current_policy.get(),
            key_distribution: self.key_distribution.borrow().clone(),
        };
        // println!(
        //     "{}:{} update_exchange_metadata: {:?}",
        //     Runtime::worker_index(),
        //     self.global_id,
        //     metadata
        // );

        self.metadata_exchange
            .set_local_operator_metadata_typed(self.global_id.local_node_id().unwrap(), metadata);
    }

    /// Update the total rebalancing time metric when completing rebalancing started at `start_time`.
    fn update_total_rebalancing_time(&self, start_time: &Instant) {
        self.total_rebalancing_time
            .update(|time| time + start_time.elapsed());
    }

    /// Update the balancing policy. If the new policy is different from the current one:
    /// - Add `accumulator_batches`, which the caller took from this worker's
    ///   receive queue, to rebalance_state.
    /// - If this is the first rebalancing in the current transaction, save the current
    ///   policy in rebalance_state, so it can later be used to retract the old contents
    ///   of the integral.
    fn update_policy(&self, new_policy: PartitioningPolicy, accumulator_batches: Vec<Arc<B>>) {
        let old_policy = self.current_policy.get();
        if old_policy == Some(new_policy) {
            return;
        }

        // Only log rebalancing in worker 0 to avoid spamming the logs.
        if Runtime::worker_index() == 0 {
            info!(
                "Rebalancing stream {}: {}->{} (key distribution: {})",
                self.input_node_id,
                if let Some(policy) = old_policy {
                    policy.to_string()
                } else {
                    "none".to_string()
                },
                new_policy.to_string(),
                self.key_distribution.borrow()
            );
        }
        self.current_policy.set(Some(new_policy));
        self.num_rebalancings.update(|value| value + 1);

        // If the accumulator was populated with broadcast policy, we want to re-balance only
        // one copy of the data.
        let accumulator_batches =
            if old_policy != Some(PartitioningPolicy::Broadcast) || self.worker_index == 0 {
                accumulator_batches
            } else {
                vec![]
            };

        match &mut *self.rebalance_state.borrow_mut() {
            Some(retractions) => {
                retractions
                    .accumulator
                    .extend_with_batches(accumulator_batches);
                retractions.rebalance_trace = retractions.trace_policy != new_policy;
            }
            retractions @ None => {
                let trace_policy = old_policy.unwrap_or(PartitioningPolicy::Shard);
                *retractions = Some(RebalanceState {
                    accumulator: SpineSnapshot::with_batches(
                        &self.batch_factories,
                        accumulator_batches,
                    ),
                    trace_policy,
                    rebalance_trace: trace_policy != new_policy,
                });
            }
        }
    }

    /// Partition the `delta` batch based on the current policy.
    ///
    /// Update key distribution stats.
    fn partition_batch(&self, delta: B) -> Vec<B> {
        match self
            .current_policy
            .get()
            .expect("RebalancingExchangeSender::partition_batch: current_policy is not set")
        {
            PartitioningPolicy::Shard => self.shard_batch(delta),
            PartitioningPolicy::Balance => self.balance_batch(delta),
            PartitioningPolicy::Broadcast => self.broadcast_batch(delta),
        }
    }

    /// Repartition the cursor over a batch constructed using `old_policy` according to the current policy.
    ///
    /// If the old policy was broadcast, be careful to only use one copy of the data.
    fn repartition<TS>(
        &self,
        old_policy: PartitioningPolicy,
        cursor: &mut dyn MergeCursor<B::Key, B::Val, TS, B::R>,
        builders: &mut [B::Builder],
        chunk_size: usize,
    ) where
        TS: Timestamp,
    {
        assert_ne!(self.current_policy.get(), None);
        assert_ne!(old_policy, self.current_policy.get().unwrap());

        match old_policy {
            PartitioningPolicy::Broadcast => self.repartition_after_broadcast(
                cursor,
                &mut builders[self.worker_index],
                chunk_size,
            ),
            PartitioningPolicy::Shard | PartitioningPolicy::Balance => {
                self.repartition_after_unicast(cursor, builders, chunk_size)
            }
        }
    }

    /// Partition cursor based on the current policy.
    ///
    /// Assumes that the cursor was partitioned using Policy::Shard or Policy::Balance, i.e., partitions
    /// in different workers don't overlap.
    ///
    /// Returns after exhausting the cursor or when one of the builders reaches chunk_size entries,
    /// whichever comes first.
    fn repartition_after_unicast<TS>(
        &self,
        cursor: &mut dyn MergeCursor<B::Key, B::Val, TS, B::R>,
        builders: &mut [B::Builder],
        chunk_size: usize,
    ) where
        TS: Timestamp,
    {
        let shards = builders.len();

        match self.current_policy.get().expect(
            "RebalancingExchangeSender::repartition_after_unicast: current policy is not set",
        ) {
            PartitioningPolicy::Shard => {
                while cursor.key_valid() {
                    let b = &mut builders[cursor.key().default_hash() as usize % shards];
                    let mut has_values = false;

                    while cursor.val_valid() && b.num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        if weight != 0 {
                            has_values = true;
                            b.push_val_diff(cursor.val(), &weight);
                        }
                        cursor.step_val();
                    }
                    if has_values {
                        b.push_key(cursor.key());
                    }
                    if cursor.val_valid() {
                        break;
                    }
                    cursor.step_key();
                }
            }
            PartitioningPolicy::Balance => {
                let mut has_values = vec![false; shards];
                let mut shard = 0;

                while cursor.key_valid() {
                    while cursor.val_valid() && builders[shard].num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        if weight != 0 {
                            shard =
                                Self::hash_key_val(cursor.key(), cursor.val()) as usize % shards;
                            let b = &mut builders[shard];

                            has_values[shard] = true;
                            b.push_val_diff(cursor.val(), &weight);
                        }
                        cursor.step_val();
                    }

                    for (shard, builder) in builders.iter_mut().enumerate() {
                        if has_values[shard] {
                            builder.push_key(cursor.key());
                        }
                    }
                    if cursor.val_valid() {
                        break;
                    }
                    cursor.step_key();
                    has_values.fill(false);
                }
            }
            PartitioningPolicy::Broadcast => {
                while cursor.key_valid() {
                    let mut has_values = false;

                    while cursor.val_valid() && builders[0].num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        for b in builders.iter_mut() {
                            if weight != 0 {
                                has_values = true;
                                b.push_val_diff(cursor.val(), &weight);
                            }
                        }
                        cursor.step_val();
                    }
                    if has_values {
                        for b in builders.iter_mut() {
                            b.push_key(cursor.key());
                        }
                    }
                    if cursor.val_valid() {
                        break;
                    }
                    cursor.step_key();
                }
            }
        }
    }

    /// Partition cursor based on the current policy.
    ///
    /// Assumes that the cursor was partitioned using Policy::Broadcast, i.e., all partitions contain identical data.
    /// The implementation takes advantage of this by having each worker only send data that belongs to the same worker
    /// in the new configuration, thus minimizing cross-worker communication.
    ///
    /// Returns after exhausting the cursor or when the builder reaches chunk_size entries,
    /// whichever comes first.
    fn repartition_after_broadcast<TS>(
        &self,
        cursor: &mut dyn MergeCursor<B::Key, B::Val, TS, B::R>,
        builder: &mut B::Builder,
        chunk_size: usize,
    ) where
        TS: Timestamp,
    {
        let shards = Runtime::num_workers();

        match self.current_policy.get().expect(
            "RebalancingExchangeSender::repartition_after_broadcast: current policy is not set",
        ) {
            PartitioningPolicy::Shard => {
                while cursor.key_valid() {
                    let shard = cursor.key().default_hash() as usize % shards;
                    if shard != self.worker_index {
                        cursor.step_key();
                        continue;
                    }

                    let mut has_values = false;

                    while cursor.val_valid() && builder.num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        if weight != 0 {
                            has_values = true;
                            builder.push_val_diff(cursor.val(), &weight);
                        }
                        cursor.step_val();
                    }
                    if has_values {
                        builder.push_key(cursor.key());
                    }
                    if cursor.val_valid() {
                        break;
                    }
                    cursor.step_key();
                }
            }
            PartitioningPolicy::Balance => {
                let mut has_values = false;

                while cursor.key_valid() {
                    while cursor.val_valid() && builder.num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        if weight != 0 {
                            let shard =
                                Self::hash_key_val(cursor.key(), cursor.val()) as usize % shards;
                            if shard != self.worker_index {
                                cursor.step_val();
                                continue;
                            }

                            has_values = true;
                            builder.push_val_diff(cursor.val(), &weight);
                        }
                        cursor.step_val();
                    }

                    if has_values {
                        builder.push_key(cursor.key());
                    }
                    if cursor.val_valid() {
                        break;
                    }
                    cursor.step_key();
                    has_values = false;
                }
            }
            PartitioningPolicy::Broadcast => unreachable!(),
        }
    }

    /// Retract all values from the cursor from the local worker.
    ///
    /// Returns after exhausting the cursor or when the builder reaches chunk_size entries,
    /// whichever comes first.
    fn process_retractions<TB>(
        &self,
        cursor: &mut SpineCursor<TB>,
        builder: &mut B::Builder,
        chunk_size: usize,
    ) where
        TB: Batch<Key = B::Key, Val = B::Val, R = B::R>,
    {
        while cursor.key_valid() {
            let mut has_values = false;

            while cursor.val_valid() && builder.num_tuples() < chunk_size {
                let mut weight = 0;
                cursor.map_times(&mut |_t, w| {
                    weight -= **w;
                });
                if weight != 0 {
                    has_values = true;
                    builder.push_val_diff(cursor.val(), &weight);
                }
                cursor.step_val();
            }
            if has_values {
                builder.push_key(cursor.key());
            }
            if cursor.val_valid() {
                break;
            }
            cursor.step_key();
        }
    }

    /// Create num_workers empty builders.
    fn create_builders(&self, chunk_size: usize) -> Vec<B::Builder> {
        std::iter::repeat_with(|| {
            B::Builder::with_capacity(&self.batch_factories, chunk_size, chunk_size)
        })
        .take(Runtime::num_workers())
        .collect()
    }

    // Partitions the batch based on the hash of the key.
    //
    // Updates key distribution.
    pub fn shard_batch(&self, mut batch: B) -> Vec<B> {
        let shards = Runtime::num_workers();

        let mut builders = Vec::with_capacity(shards);

        for _ in 0..shards {
            // We iterate over tuples in the batch in order; hence tuples added
            // to each shard are also ordered, so we can use the more efficient
            // `Builder` API (instead of `Batcher`) to construct output batches.
            builders.push(B::Builder::with_capacity(
                &self.batch_factories,
                batch.key_count_upper_bound() / shards,
                batch.len_upper_bound() / shards,
            ));
        }

        let mut cursor = batch.consuming_cursor(None, None);
        let mut key_distribution = KeyDistribution::new(shards);

        if cursor.has_mut() {
            while cursor.key_valid() {
                let shard = cursor.key().default_hash() as usize % shards;
                let b = &mut builders[shard];
                while cursor.val_valid() {
                    let weight = **cursor.weight();
                    key_distribution.update(shard, weight);

                    b.push_diff(weight.erase());
                    b.push_val_mut(cursor.val_mut());
                    cursor.step_val();
                }
                b.push_key_mut(cursor.key_mut());
                cursor.step_key();
            }
        } else {
            while cursor.key_valid() {
                let shard = cursor.key().default_hash() as usize % shards;
                let b = &mut builders[shard];
                while cursor.val_valid() {
                    let weight = **cursor.weight();
                    key_distribution.update(shard, weight);

                    b.push_diff(weight.erase());
                    b.push_val(cursor.val());
                    cursor.step_val();
                }
                b.push_key(cursor.key());
                cursor.step_key();
            }
        }

        self.key_distribution.borrow_mut().merge(&key_distribution);

        builders.into_iter().map(|builder| builder.done()).collect()
    }

    // Partitions the batch based on the hash of the key/value pair.
    fn balance_batch(&self, mut batch: B) -> Vec<B> {
        let shards = Runtime::num_workers();
        let mut key_distribution = KeyDistribution::new(shards);

        let mut builders = Vec::with_capacity(shards);
        let mut key = self.batch_factories.key_factory().default_box();

        for _ in 0..shards {
            builders.push(TupleBuilder::new(
                &self.batch_factories,
                B::Builder::with_capacity(
                    &self.batch_factories,
                    batch.key_count_upper_bound() / shards,
                    batch.len_upper_bound() / shards,
                ),
            ));
        }

        let mut cursor = batch.consuming_cursor(None, None);
        if cursor.has_mut() {
            while cursor.key_valid() {
                // We still need to hash the key to update key distribution.
                let key_shard = cursor.key().default_hash() as usize % shards;

                while cursor.val_valid() {
                    let b = &mut builders
                        [Self::hash_key_val(cursor.key(), cursor.val()) as usize % shards];
                    cursor.key().clone_to(&mut key);

                    let mut w = **cursor.weight();
                    key_distribution.update(key_shard, w);
                    b.push_vals(&mut key, cursor.val_mut(), &mut (), w.erase_mut());
                    cursor.step_val();
                }
                cursor.step_key();
            }
        } else {
            while cursor.key_valid() {
                let key_shard = cursor.key().default_hash() as usize % shards;
                while cursor.val_valid() {
                    let b = &mut builders
                        [Self::hash_key_val(cursor.key(), cursor.val()) as usize % shards];
                    let w = **cursor.weight();
                    key_distribution.update(key_shard, w);
                    b.push_refs(cursor.key(), cursor.val(), &(), &w);
                    cursor.step_val();
                }
                cursor.step_key();
            }
        }

        self.key_distribution.borrow_mut().merge(&key_distribution);
        builders.into_iter().map(|builder| builder.done()).collect()
    }

    /// Return num_workers identical copies of `delta`.
    fn broadcast_batch(&self, batch: B) -> Vec<B> {
        self.update_key_distribution(&batch);

        let mut outputs = Vec::with_capacity(Runtime::num_workers());
        for _i in 0..Runtime::num_workers() - 1 {
            outputs.push(batch.clone());
        }
        outputs.push(batch);
        outputs
    }

    /// Adds the weights in `batch` to the key distribution, each under the
    /// shard that key sharding would send its key to.
    ///
    /// # Arguments
    ///
    /// * `batch` - a batch of this operator's input.
    fn update_key_distribution(&self, batch: &B) {
        let shards = Runtime::num_workers();
        let mut key_distribution = KeyDistribution::new(shards);

        let mut cursor = batch.cursor();
        while cursor.key_valid() {
            let key_shard = cursor.key().default_hash() as usize % shards;
            while cursor.val_valid() {
                let w = **cursor.weight();
                key_distribution.update(key_shard, w);
                cursor.step_val();
            }
            cursor.step_key();
        }

        self.key_distribution.borrow_mut().merge(&key_distribution);
    }

    /// Saves `delta`, which this worker received in a step in which the policy
    /// changed, in the rebalance state, so that the rebalancing at the end of
    /// the transaction sends it under the final policy.
    ///
    /// The rebalancing does not count what it sends in the key distribution,
    /// so this counts `delta`, as [Self::partition_batch] would.  Like the
    /// receive queue, it moves a large `delta` to storage.
    ///
    /// # Arguments
    ///
    /// * `delta` - this worker's input in the current step.
    ///
    /// # Panics
    ///
    /// Panics if `delta` is not empty and the policy has not changed in the
    /// current transaction, because then there is no rebalance state.
    fn defer_delta(&self, delta: B) {
        if delta.is_empty() {
            return;
        }
        self.update_key_distribution(&delta);
        let delta =
            Spine::maybe_flush_batch(delta, &self.batch_factories, BatchLayout::default(), || {
                (None, None)
            });
        self.rebalance_state
            .borrow_mut()
            .as_mut()
            .expect("a policy change creates the rebalance state")
            .accumulator
            .extend_with_batches([delta]);
    }

    fn checkpoint_file(persistent_id: &str) -> String {
        format!("rebalancing-exchange-{}.dat", persistent_id)
    }

    /// Returns one empty batch for each worker.
    fn empty_batches(&self) -> Vec<B> {
        (0..Runtime::num_workers())
            .map(|_| B::dyn_empty(&self.batch_factories))
            .collect()
    }

    /// Sends `batches`, one for each worker, to the receivers.
    async fn send(&self, batches: Vec<B>, flush_complete: bool) {
        self.exchange
            .send_partitioned(self.global_id_string.clone(), batches, flush_complete)
            .await;
    }

    async fn send_builders(
        &self,
        builders: Vec<B::Builder>,
        capacity: &mut usize,
        flush_complete: bool,
    ) -> Vec<B::Builder> {
        let batches = builders
            .into_iter()
            .map(|builder| builder.done())
            .inspect(|batch| {
                if batch.key_count_upper_bound() > *capacity {
                    *capacity = batch.key_count_upper_bound();
                }
            })
            .collect();

        self.send(batches, flush_complete).await;
        self.create_builders(*capacity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FlushState {
    /// The transaction has started, but `flush` has not been called.
    TransactionStarted,
    /// `flush` has been called, but the operator is still producing outputs.
    FlushRequested,
    /// The operator is in the process of rebalancing its output based on the latest partitioning policy proposed by the balancer.
    RebalanceInProgress,
    /// Flush completed -- no more outputs till the next transaction.
    /// Rebalancing is not allowed in the current transaction.
    FlushCompleted,
}

/// Metadata shared by the ExchangeSender operators with balancer instances in all workers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalancingExchangeSenderExchangeMetadata {
    /// The flush state of the operator.
    pub flush_state: FlushState,

    /// Currently selected sharding policy.
    pub current_policy: Option<PartitioningPolicy>,

    /// Key distribution of the input stream computed based on the inputs received by the current worker.
    pub key_distribution: KeyDistribution,
}

impl<B, C> Operator for RebalancingExchangeSender<B, C>
where
    B: ZBatch<Time = ()>,
    C: WithClock + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("RebalancingExchangeSender")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
            KEY_DISTRIBUTION => self.key_distribution.borrow().meta_item(),
            BALANCER_POLICY => MetaItem::String(self.current_policy.get().map(|policy| policy.to_string()).unwrap_or("none".to_string())),
            RABALANCINGS_COUNT => MetaItem::Count(self.num_rebalancings.get()),
            REBALANCING_IN_PROGRESS => MetaItem::Bool(self.rebalancing_in_progress()),
            ACCUMULATOR_RECORDS_TO_REPARTITION_COUNT => MetaItem::Count(self.rebalance_accumulator_size.get()),
            INTEGRAL_RECORDS_TO_REPARTITION_COUNT => MetaItem::Count(self.rebalance_integral_size.get()),
            TOTAL_REBALANCING_TIME_SECONDS => MetaItem::Duration(self.total_rebalancing_time.get()),
        });

        if let Some(rebalancing_start_time) = self.rebalance_start_time.get() {
            meta.extend(metadata! {
                INPROGRESS_REBALANCING_TIME_SECONDS => MetaItem::Duration(rebalancing_start_time.elapsed()),
            });
        }
        if let Some(local_shard_size) = self
            .balancer
            .key_distribtion_for_stream_local_worker(self.input_node_id)
        {
            meta.extend(metadata! {
                LOCAL_SHARD_RECORDS_COUNT => MetaItem::Count(local_shard_size.unsigned_abs() as usize),
            });
        }
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
        self.global_id_string = Arc::new(format!(
            "RebalancingExchangeSender {}",
            global_id.node_identifier()
        ));
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn start_transaction(&mut self) {
        // println!(
        //     "{}:{} start_transaction",
        //     Runtime::worker_index(),
        //     self.global_id
        // );
        self.flush_state.set(FlushState::TransactionStarted);
        self.num_steps_in_transaction.set(0);
        self.update_exchange_metadata();
    }

    fn flush(&mut self) {
        // println!("{}:{} flush", Runtime::worker_index(), self.global_id);
        self.flush_state.set(FlushState::FlushRequested);
    }

    fn is_flush_complete(&self) -> bool {
        // flushed == true and all rebalancing state has been flushed
        self.flush_state.get() == FlushState::FlushCompleted
    }

    /// Checkpoint the current sharding policy only.
    fn checkpoint(
        &mut self,
        persistent_id: Option<&str>,
    ) -> Result<Option<Box<dyn OperatorCheckpoint>>, crate::Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        let checkpoint = RebalanceExchangeSenderCheckpoint {
            current_policy: self.current_policy.get(),
            key_distribution: self.key_distribution.borrow().clone(),
        };

        Ok(Some(Box::new(FileOperatorCheckpoint {
            name: Self::checkpoint_file(persistent_id),
            content: FBuf::from_slice(&serde_json::to_vec(&checkpoint).unwrap()),
        })))
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), crate::Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;

        let file_path = base.clone().join(Self::checkpoint_file(pid));
        let content = Runtime::storage_backend().unwrap().read(&file_path)?;

        let checkpoint = serde_json::from_slice::<RebalanceExchangeSenderCheckpoint>(&content)
            .map_err(|e| {
                crate::Error::Runtime(RuntimeError::CheckpointParseError(format!(
                    "invalid RebalancingExchangeSender checkpoint for operator {}: {e}",
                    self.global_id
                )))
            })?;

        self.current_policy.set(checkpoint.current_policy);

        if let Some(policy) = checkpoint.current_policy {
            // Normally the current policy is communicated to the balancer before every step
            // via metadata_exchange, but if the restored node is disabled during bootstrapping,
            // it doesn't participate in steps, so we need to set the policy explicitly.
            // This policy will be:
            // - Used to determine if the cluster needs to be invalidated during bootstrapping
            //   (see `BalancerInner::invalidate_clusters_for_bootstrapping`).
            // - Frozen during replay (see `BalancerInner::get_fixed_policy`).
            self.balancer
                .set_policy_for_stream(self.input_node_id, policy);
        }
        *self.key_distribution.borrow_mut() = checkpoint.key_distribution;

        Ok(())
    }

    fn clear_state(&mut self) -> Result<(), crate::Error> {
        self.current_policy.set(None);
        *self.key_distribution.borrow_mut() = KeyDistribution::new(Runtime::num_workers());

        // Clear the metadata. If the operator is deactivated during bootstrapping, its metadata
        // shouldn't confuse the balancer.
        self.metadata_exchange
            .clear_local_operator_metadata(self.global_id.local_node_id().unwrap());
        Ok(())
    }

    fn swap_state(&mut self, other: &mut Self) -> Result<(), crate::Error> {
        // Cutover happens between transactions, so neither copy is mid-flush
        // or mid-rebalance.
        debug_assert_eq!(self.flush_state.get(), FlushState::FlushCompleted);
        debug_assert_eq!(other.flush_state.get(), FlushState::FlushCompleted);
        debug_assert!(self.rebalance_state.borrow().is_none());
        debug_assert!(other.rebalance_state.borrow().is_none());

        // `self` is the live copy and `other` the bootstrap copy (see
        // `CircuitHandle::cutover_from`).  The bootstrap copy's integral is
        // sharded under its `current_policy`, and that integral is transferred
        // by the integral node's own `swap_state`, so the policy must travel
        // with it.  We only move the policy-bearing state; the `balancer` and
        // `metadata_exchange` references are per-copy and stay put.
        self.current_policy.swap(&other.current_policy);
        std::mem::swap(
            &mut *self.key_distribution.borrow_mut(),
            &mut *other.key_distribution.borrow_mut(),
        );

        // Re-seed the live balancer with the transferred policy, exactly as
        // `restore` does, so the balancer's cluster solution matches the layout
        // of the integral we just installed.
        if let Some(policy) = self.current_policy.get() {
            self.balancer
                .set_policy_for_stream(self.input_node_id, policy);
        }

        Ok(())
    }
}

impl<B, C> StreamingBinarySinkOperator<B, TimedSpine<B, C>> for RebalancingExchangeSender<B, C>
where
    B: ZBatch<Time = ()>,
    C: WithClock + 'static,
{
    fn eval(
        self: Rc<Self>,
        delta: Cow<'_, B>,
        delayed_trace: Cow<'_, TimedSpine<B, C>>,
    ) -> impl futures::Stream<Item = (bool, Option<Position>)> + 'static {
        // println!("{}:{} eval", Runtime::worker_index(), self.global_id);

        let mut delayed_trace = delayed_trace.ro_snapshot();
        let chunk_size = splitter_output_chunk_size();

        // Capacity for the builders that we create.  We don't want to initially
        // allocate the full `chunk_size`, because it might be bigger than
        // memory, e.g. if it's `usize::MAX`.
        let mut builder_capacity = splitter_output_first_chunk_size();

        let delta = delta.into_owned();

        self.num_steps_in_transaction.update(|steps| steps + 1);

        assert_ne!(self.flush_state.get(), FlushState::RebalanceInProgress);

        // 1. If a rebalancing is requested (latest policy != current policy), we
        //   - take what this worker has received so far in the transaction and store it, with
        //     the integral state, in rebalance_state, so we can rebalance them at the end of
        //     the transaction.
        //   - switch to the latest policy.
        //   - store the `delta` batch in rebalance_state as well, instead of sending it.
        // 2. Otherwise, we partition the `delta` batch based on the current policy and send it.
        // 3. We start the actual rebalancing after receiving the `flush` notification for the
        //    current transaction. During rebalancing, we:
        //   - retract the old contents of the integral.
        //   - partition saved accumulator and integral state based on the latest policy.
        //
        // Multiple rebalancings can be requested during the same transaction. The new policy takes effect
        // for the deltas of later steps. The integral state only gets rebalanced at the very end of the
        // transaction. Accumulator state and the delta are taken when the policy changes, are preserved in
        // rebalance_state, and are repartitioned at the end of the transaction.
        //
        // Why not clear integral state instantly on rebalancing instead of producing and merging relatively expensive
        // retractions? Because otherwise retractions will not appear in the output of the accumulator, which is in turn
        // used as input to other operators such as join (see `accumulator_stream` in the diagram above).
        stream! {
            // Policy change? Update rebalancing state.
            let new_policy = self.balancer.get_optimal_policy(self.input_node_id).unwrap();

            if self.flush_state.get() == FlushState::FlushCompleted {
                assert!(delta.is_empty());
                // No policy change expected after commit.

                // if *self.current_policy.borrow() != new_policy  {
                //     println!("{}: current_policy: {:?}, new_policy: {:?}", self.input_node_id, *self.current_policy.borrow(), new_policy);
                // }
                assert_eq!(self.current_policy.get().unwrap(), new_policy);
                self.update_exchange_metadata();
                yield (true, None);
                return;
            }

            // The first policy takes effect at once, because nothing has been
            // sent under another policy yet.
            if self.current_policy.get().is_none() {
                self.update_policy(new_policy, Vec::new());
            }

            // println!("{}: old_policy: {:?}, new_policy: {:?}", Runtime::worker_index(), *self.current_policy.borrow(), new_policy);

            let batches = if self.current_policy.get() == Some(new_policy) {
                // Partition `delta` based on the current policy.
                self.partition_batch(delta)
            } else {
                // Every worker changes the policy in this step, calls
                // `wait_for_arrival` before it sends anything in this step,
                // and sends only empty batches in it.  After the wait, this
                // worker holds all the data sent to it in this transaction,
                // all of it under the old policy.  No data under the new
                // policy can arrive yet, because no worker starts the next
                // step before every worker finishes this one.
                self.exchange
                    .wait_for_arrival(self.global_id_string.clone())
                    .await;
                let accumulator_batches = self.exchange.take_received();
                self.update_policy(new_policy, accumulator_batches);
                self.defer_delta(delta);
                self.empty_batches()
            };

            let ready_to_commit = self.ready_to_commit();

            // println!("{}: delta: {:?}", Runtime::worker_index(), batches);

            let flush_complete = ready_to_commit && self.rebalance_state.borrow().is_none();
            self.send(batches, flush_complete).await;

            // If we're not yet flushing the transaction or we are flushing but there is no rebalancing to be done, return after
            // partitioning the `delta` batch. Otherwise, we'll continue to the rebalancing phase.
            let rebalance = ready_to_commit && self.rebalance_state.borrow().is_some();

            if !rebalance {
                if flush_complete {
                    // if Runtime::worker_index() == 0 {
                    //     println!("{}: flush complete 1 ({:?})", self.input_node_id, *self.current_policy.borrow());
                    // }
                    self.flush_state.set(FlushState::FlushCompleted);
                }
                // Let other workers know that no more rebalancing is allowed in the current transaction.
                self.update_exchange_metadata();
                yield (true, None);
                return;
            } else {
                self.flush_state.set(FlushState::RebalanceInProgress);
                self.update_exchange_metadata();
                yield (false, None);
            }

            // We have no more inputs to process, but there is rebalancing work to be done.
            self.rebalance_start_time.set(Some(Instant::now()));

            // Used to measure step duration and add it to the total rebalancing time.
            let mut step_start_time = Instant::now();

            let RebalanceState {
                mut accumulator,
                trace_policy,
                rebalance_trace,
            } = self.rebalance_state.borrow_mut().take().unwrap();

            self.rebalance_accumulator_size.set(accumulator.len_upper_bound());
            self.rebalance_integral_size.set(if rebalance_trace {
                delayed_trace.len_upper_bound()
            } else {
                0
            });

            let mut integral_cursor = delayed_trace.cursor();

            let mut builders = self.create_builders(builder_capacity);

            // Retract the contents of the integral by sending it with negated weights to the accumulator
            // in the same worker.
            if rebalance_trace {
                while integral_cursor.key_valid() {
                    self.process_retractions(
                        &mut integral_cursor,
                        &mut builders[self.worker_index],
                        chunk_size,
                    );

                    // println!("{}: integral retractions: {:?}", Runtime::worker_index(), batches);
                    builders = self
                        .send_builders(
                            builders,
                            &mut builder_capacity,
                            false)
                        .await;
                    self.update_exchange_metadata();
                    self.update_total_rebalancing_time(&step_start_time);
                    yield (false, None);
                    step_start_time = Instant::now();
                }
            }

            // Start new cursors.
            let mut accumulator_cursor = accumulator.consuming_cursor(None, None);
            let mut integral_cursor = delayed_trace.consuming_cursor(None, None);

            // Re-partition accumulator.
            while accumulator_cursor.key_valid() {
                self.repartition_after_unicast(&mut accumulator_cursor, &mut builders, chunk_size);

                // println!("{}: acc insertions: {:?}", Runtime::worker_index(), batches);
                builders = self.send_builders(builders, &mut builder_capacity, false).await;
                self.update_exchange_metadata();
                self.update_total_rebalancing_time(&step_start_time);
                yield (false, None);
                step_start_time = Instant::now();
            }

            // Repartition integral.
            if rebalance_trace {
                while integral_cursor.key_valid() {
                    self.repartition(trace_policy, &mut integral_cursor, &mut builders, chunk_size);

                    // println!("{}: integral insertions: {:?}", Runtime::worker_index(), batches);
                    builders = self
                        .send_builders(
                            builders,
                            &mut builder_capacity,
                            !integral_cursor.key_valid())
                        .await;
                    self.update_exchange_metadata();

                    if integral_cursor.key_valid() {
                        self.update_exchange_metadata();
                        self.update_total_rebalancing_time(&step_start_time);
                        yield (false, None);
                        step_start_time = Instant::now();
                    } else {
                        // if Runtime::worker_index() == 0 {
                        //     println!("{}: flush complete 2 ({:?})", self.input_node_id, *self.current_policy.borrow());
                        // }

                        self.flush_state.set(FlushState::FlushCompleted);
                        self.update_exchange_metadata();
                        self.rebalance_accumulator_size.set(0);
                        self.rebalance_integral_size.set(0);
                        self.update_total_rebalancing_time(&step_start_time);
                        self.rebalance_start_time.set(None);
                        yield (true, None);
                        return;
                    }
                }
            }

            // println!("{}: final batches: {:?}", Runtime::worker_index(), batches);
            self.send_builders(builders, &mut builder_capacity, true).await;

            // if Runtime::worker_index() == 0 {
            //     println!("{}: flush complete 3 ({:?})", self.input_node_id, *self.current_policy.borrow());
            // }

            self.flush_state.set(FlushState::FlushCompleted);
            self.update_exchange_metadata();
            self.rebalance_accumulator_size.set(0);
            self.rebalance_integral_size.set(0);
            self.update_total_rebalancing_time(&step_start_time);
            self.rebalance_start_time.set(None);
            yield (true, None);
        }
    }

    // fn input_preference(
    //     &self,
    // ) -> (
    //     OwnershipPreference,
    //     OwnershipPreference,
    //     OwnershipPreference,
    // ) {
    //     (
    //         OwnershipPreference::PREFER_OWNED,
    //         OwnershipPreference::INDIFFERENT,
    //         OwnershipPreference::INDIFFERENT,
    //     )
    // }
}
