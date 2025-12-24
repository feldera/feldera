use crate::{
    Circuit, Position, Runtime, RuntimeError, Scope, Stream, Timestamp, ZWeight,
    algebra::{IndexedZSet, ZBatch},
    circuit::{
        GlobalNodeId, NodeId, OwnershipPreference, WithClock,
        checkpointer::EmptyCheckpoint,
        circuit_builder::{MetadataExchange, StreamId, register_replay_stream},
        metadata::{
            BALANCER_POLICY_LABEL, BatchSizeStats, INPROGRESS_REBALANCING_TIME_LABEL,
            INPUT_BATCHES_LABEL, KEY_DISTRIBUTION_LABEL, LOCAL_SHARD_SIZE_LABEL, MetaItem,
            NUM_ACCUMULATOR_RECORDS_TO_REPARTITION_LABEL,
            NUM_INTEGRAL_RECORDS_TO_REPARTITION_LABEL, NUM_RABALANCINGS_LABEL, OperatorLocation,
            OperatorMeta, REBALANCING_IN_PROGRESS_LABEL, TOTAL_REBALANCING_TIME_LABEL,
        },
        operator_traits::Operator,
        splitter_output_chunk_size,
    },
    circuit_cache_key, default_hasher,
    dynamic::{ClonableTrait, Data as _, Erase},
    operator::{
        Z1,
        async_stream_operators::{StreamingTernarySinkOperator, StreamingTernarySinkWrapper},
        communication::{Exchange, ExchangeReceiver},
        dynamic::{
            accumulate_trace::{AccumulateBoundsId, AccumulateTraceAppend, AccumulateZ1Trace},
            balance::{
                Balancer, PartitioningPolicy, rebalancing_accumulator::RebalancingAccumulatorInner,
            },
            trace::{DelayedTraceId, TimedSpine, TraceBounds},
        },
        require_persistent_id,
    },
    trace::{
        Batch, BatchReader, BatchReaderFactories, Builder, Cursor, MergeCursor, Spine,
        SpineSnapshot, TupleBuilder, WithSnapshot, deserialize_indexed_wset, merge_batches,
        serialize_indexed_wset, spine_async::SpineCursor,
    },
};
use async_stream::stream;
use feldera_storage::{FileCommitter, StoragePath, fbuf::FBuf};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cell::RefCell,
    fmt::{Display, Formatter},
    hash::Hasher,
    marker::PhantomData,
    panic::Location,
    rc::Rc,
    sync::{Arc, atomic::AtomicU64},
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
    ///              ┌─────────►│RebalancingExchangeSender│◄───┐
    ///              │          └────────────┬────────────┘    │
    ///              │                       .                 │delayed_acc
    ///              │                       .                 |
    ///              │                       ▼                 │
    ///              │               ┌────────────────┐      ┌──┐
    ///              │               │ExchangeReceiver│      │Z1│
    ///              │               └────────────────┘      └──┘
    ///              │                       │                 ▲
    ///              │                       │                 │
    ///              │                       ▼                 │
    ///              │             ┌──────────────────────┐    │
    ///              │             │RebalancingAccumulator│────┘
    /// delayed_trace│             └──────────────────────┘
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
    /// The two feedback streams (delayed_acc and delayed_trace) are used by RebalancingExchangeSender to
    /// retract previously accumulated content of the integral when the sharding policy changes.
    #[track_caller]
    pub fn dyn_accumulate_trace_balanced(
        &self,
        trace_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> (Stream<C, Option<Spine<B>>>, Stream<C, TimedSpine<B, C>>) {
        if Runtime::num_workers() == 1 {
            let trace = self.dyn_accumulate_trace(trace_factories, batch_factories);
            return (self.dyn_accumulate(batch_factories), trace);
        }

        let location = Location::caller();

        self.circuit()
            .cache_get_or_insert_with(BalancedTraceId::new(self.stream_id()), || {
                let circuit = self.circuit();
                circuit.region("accumulate_trace_with_balancer", || {
                    // Exchange + receiver
                    let runtime = Runtime::runtime().unwrap();
                    let exchange_id = runtime.sequence_next();
                    let worker_index = Runtime::worker_index();
                    let batch_factories_clone = batch_factories.clone();
                    let start_wait_usecs = Arc::new(AtomicU64::new(0));
                    let balancer = circuit.balancer();

                    let persistent_id = self.get_persistent_id();

                    // Exchange object. The Boolean flag signals that the sender won't produce more outputs
                    // during the current transaction. It will be set to `true` exactly once per transaction.
                    // Once all senders are flushed, the receiver can report itself as flushed too.
                    let exchange: Arc<Exchange<(B, bool)>> = Exchange::with_runtime(
                        &runtime,
                        exchange_id,
                        Box::new(move |(batch, flush)| {
                            let mut vec = serialize_indexed_wset(&batch);
                            vec.push(flush as u8);
                            vec
                        }),
                        Box::new(move |mut vec| {
                            let flush = match vec.pop().unwrap() {
                                0 => false,
                                1 => true,
                                _ => unreachable!(),
                            };
                            (
                                deserialize_indexed_wset(&batch_factories_clone, &vec),
                                flush,
                            )
                        }),
                    );

                    // Exchange receiver
                    let batch_factories_clone = batch_factories.clone();

                    let receiver = circuit.add_source(ExchangeReceiver::new(
                        worker_index,
                        Some(location),
                        exchange.clone(),
                        || Vec::new(),
                        start_wait_usecs.clone(),
                        |batches: &mut Vec<B>, batch: B| batches.push(batch),
                    ));

                    let sharded_stream =
                        receiver.apply_owned_named("merge shards", move |batches| {
                            // TODO: instead of merging the batches here, modify the BalancingAccumulator operator to accept a vector of batches
                            // and have it to the merging in the background.
                            merge_batches(&batch_factories_clone, batches, &None, &None)
                        });

                    // Accumulator.
                    let (accumulator_stream, accumulator_inner, accumulator_snapshot_stream_val) =
                        sharded_stream.dyn_accumulate_with_feedback_stream(batch_factories);

                    // Integral.
                    let bounds = TraceBounds::unbounded();

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
                            bounds.clone(),
                        ),
                    );

                    let replay_stream = z1feedback
                        .operator_mut()
                        .prepare_replay_stream(&sharded_stream);

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

                    register_replay_stream(circuit, &sharded_stream, &replay_stream);

                    circuit.cache_insert(
                        DelayedTraceId::new(trace.stream_id()),
                        delayed_trace.clone(),
                    );
                    // circuit.cache_insert(
                    //     AccumulateTraceId::new(sharded_stream.stream_id()),
                    //     trace.clone(),
                    // );
                    circuit.cache_insert(
                        AccumulateBoundsId::<B>::new(sharded_stream.stream_id()),
                        bounds,
                    );

                    let (delayed_acc, acc_feedback) = circuit.add_feedback_persistent(
                        persistent_id
                            .as_ref()
                            .map(|name| format!("{name}.balanced_acc_snapshot"))
                            .as_deref(),
                        Z1::new(EmptyCheckpoint::<Vec<Arc<B>>>::new(Vec::new())),
                    );

                    let accumulator_feedback_stream = Stream::with_value(
                        circuit.clone(),
                        accumulator_stream.local_node_id(),
                        accumulator_snapshot_stream_val.clone(),
                    );

                    acc_feedback.connect(&accumulator_feedback_stream);

                    // Exchange sender.
                    let exchange_sender = RebalancingExchangeSender::<B, C>::new(
                        batch_factories,
                        worker_index,
                        Some(location),
                        exchange,
                        accumulator_inner,
                        balancer.clone(),
                        circuit.metadata_exchange().clone(),
                        self.local_node_id(),
                    );

                    let sender_node_id = circuit.add_ternary_sink(
                        StreamingTernarySinkWrapper::new(exchange_sender),
                        self,
                        &delayed_acc,
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

                    // Add ExchangeSender -> ExchangeReceiver dependency.
                    circuit.add_dependency(
                        sender_node_id.local_node_id().unwrap(),
                        receiver.local_node_id(),
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
    current_policy: PartitioningPolicy,
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

    /// Origin node of the input to this operator.
    input_node_id: NodeId,

    worker_index: usize,
    location: OperatorLocation,

    exchange: Arc<Exchange<(B, bool)>>,

    /// Reference to the accumulator inner state used to clear it when rebalancing.
    accumulator_inner: Rc<RefCell<RebalancingAccumulatorInner<B>>>,

    /// Balancer used to select the sharding policy.
    balancer: Balancer,

    /// Metadata exchange used to exchange key distribution and status info
    /// with balancer instances in all workers.
    metadata_exchange: MetadataExchange,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flush_state: RefCell<FlushState>,

    /// The number of steps since the transaction started (see `ready_to_commit`).
    num_steps_in_transaction: RefCell<usize>,

    /// Current partitioning policy.
    current_policy: RefCell<PartitioningPolicy>,

    /// When Some(), the policy has changed during the current transaction, and the operator must rebalance
    /// accumulator and integral state before committing the transaction.
    rebalance_state: RefCell<Option<RebalanceState<B>>>,

    /// The moment rebalancing started.
    ///
    /// None if the policy has changed but flush hasn't yet been invoked, so rebalancing has not started yet.
    rebalance_start_time: RefCell<Option<Instant>>,

    /// Number of times the stream was rebalanced.
    num_rebalancings: RefCell<usize>,

    /// Size of the accumulator state to be rebalanced.
    rebalance_accumulator_size: RefCell<usize>,

    /// Size of the integral state to be rebalanced.
    rebalance_integral_size: RefCell<usize>,

    /// Local key distribution of the input stream computed by the current worker.
    key_distribution: RefCell<KeyDistribution>,

    /// Total time spent rebalancing the stream.
    total_rebalancing_time: RefCell<Duration>,

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
        exchange: Arc<Exchange<(B, bool)>>,
        accumulator_inner: Rc<RefCell<RebalancingAccumulatorInner<B>>>,
        balancer: Balancer,
        metadata_exchange: MetadataExchange,
        input_node_id: NodeId,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());
        Self {
            batch_factories: batch_factories.clone(),
            global_id: GlobalNodeId::root(),
            input_node_id,
            worker_index,
            location,
            exchange,
            accumulator_inner,
            balancer,
            metadata_exchange,
            input_batch_stats: BatchSizeStats::new(),
            flush_state: RefCell::new(FlushState::FlushCompleted),
            num_steps_in_transaction: RefCell::new(0),
            current_policy: RefCell::new(PartitioningPolicy::Shard),
            rebalance_state: RefCell::new(None),
            rebalance_start_time: RefCell::new(None),
            num_rebalancings: RefCell::new(0),
            rebalance_accumulator_size: RefCell::new(0),
            rebalance_integral_size: RefCell::new(0),
            key_distribution: RefCell::new(KeyDistribution::new(Runtime::num_workers())),
            total_rebalancing_time: RefCell::new(Duration::from_secs(0)),
            phantom: PhantomData,
        }
    }

    fn rebalancing_in_progress(&self) -> bool {
        *self.flush_state.borrow() == FlushState::RebalanceInProgress
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
        *self.flush_state.borrow() == FlushState::FlushRequested
            && (self.balancer.ready_to_commit(self.input_node_id)
                || *self.num_steps_in_transaction.borrow() < LONG_TRANSACTION_THRESHOLD)
    }

    fn update_exchange_metadata(&self) {
        let metadata = RebalancingExchangeSenderExchangeMetadata {
            flush_state: *self.flush_state.borrow(),
            current_policy: *self.current_policy.borrow(),
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
        *self.total_rebalancing_time.borrow_mut() += start_time.elapsed();
    }

    /// Update the balancing policy. If the new policy is different from the current one:
    /// - Clear the accumulator.
    /// - Add accumulator state to rebalance_state.
    /// - If this is the first rebalancing in the current transaction, save the current
    ///   policy in rebalance_state, so it can later be used to retract the old contents
    ///   of the integral.
    fn update_policy(&self, new_policy: PartitioningPolicy, delayed_accumulator: Vec<Arc<B>>) {
        let old_policy = *self.current_policy.borrow();
        if old_policy == new_policy {
            return;
        }

        // Only log rebalancing in worker 0 to avoid spamming the logs.
        if Runtime::worker_index() == 0 {
            info!(
                "rebalancing stream {}: {}->{} (key distribution: {})",
                self.input_node_id,
                old_policy,
                new_policy,
                self.key_distribution.borrow()
            );
        }
        *self.current_policy.borrow_mut() = new_policy;
        *self.num_rebalancings.borrow_mut() += 1;

        // Discard the accumulator state, so we don't need to retract it explicitly.
        self.accumulator_inner.borrow_mut().clear_state();

        // If the accumulator was populated with broadcast policy, we want to re-balance only
        // one copy of the data.
        let accumulator_batches =
            if old_policy != PartitioningPolicy::Broadcast || self.worker_index == 0 {
                delayed_accumulator
            } else {
                vec![]
            };

        match &mut *self.rebalance_state.borrow_mut() {
            Some(retractions) => {
                retractions
                    .accumulator
                    .extend_with_batches(accumulator_batches);
            }
            retractions @ None => {
                *retractions = Some(RebalanceState {
                    accumulator: SpineSnapshot::with_batches(
                        &self.batch_factories,
                        accumulator_batches,
                    ),
                    trace_policy: old_policy,
                });
            }
        }
    }

    /// Partition the `delta` batch based on the current policy.
    ///
    /// Update key distribution stats.
    fn partition_batch(&self, delta: B) -> Vec<B> {
        match *self.current_policy.borrow() {
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
        assert!(old_policy != *self.current_policy.borrow());

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

        match *self.current_policy.borrow() {
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

        match *self.current_policy.borrow() {
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
                batch.key_count() / shards,
                batch.len() / shards,
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
                    batch.key_count() / shards,
                    batch.len() / shards,
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
        let shards = Runtime::num_workers();
        let mut key_distribution = KeyDistribution::new(shards);

        // Update key distribution.
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
        drop(cursor);

        self.key_distribution.borrow_mut().merge(&key_distribution);

        let mut outputs = Vec::with_capacity(Runtime::num_workers());
        for _i in 0..Runtime::num_workers() - 1 {
            outputs.push(batch.clone());
        }
        outputs.push(batch);
        outputs
    }

    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("rebalancing-exchange-{}.dat", persistent_id))
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
    pub current_policy: PartitioningPolicy,

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
            INPUT_BATCHES_LABEL => self.input_batch_stats.metadata(),
            KEY_DISTRIBUTION_LABEL => self.key_distribution.borrow().meta_item(),
            BALANCER_POLICY_LABEL => MetaItem::String(self.current_policy.borrow().to_string()),
            NUM_RABALANCINGS_LABEL => MetaItem::Count(*self.num_rebalancings.borrow()),
            REBALANCING_IN_PROGRESS_LABEL => MetaItem::Bool(self.rebalancing_in_progress()),
            NUM_ACCUMULATOR_RECORDS_TO_REPARTITION_LABEL => MetaItem::Count(*self.rebalance_accumulator_size.borrow()),
            NUM_INTEGRAL_RECORDS_TO_REPARTITION_LABEL => MetaItem::Count(*self.rebalance_integral_size.borrow()),
            TOTAL_REBALANCING_TIME_LABEL => MetaItem::Duration(*self.total_rebalancing_time.borrow()),
        });

        if let Some(rebalancing_start_time) = self.rebalance_start_time.borrow().as_ref() {
            meta.extend(metadata! {
                INPROGRESS_REBALANCING_TIME_LABEL => MetaItem::Duration(rebalancing_start_time.elapsed()),
            });
        }
        if let Some(local_shard_size) = self
            .balancer
            .key_distribtion_for_stream_local_worker(self.input_node_id)
        {
            meta.extend(metadata! {
                LOCAL_SHARD_SIZE_LABEL => MetaItem::Count(local_shard_size.unsigned_abs() as usize),
            });
        }
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}

    fn is_async(&self) -> bool {
        true
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
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

    fn start_transaction(&mut self) {
        // println!(
        //     "{}:{} start_transaction",
        //     Runtime::worker_index(),
        //     self.global_id
        // );
        *self.flush_state.borrow_mut() = FlushState::TransactionStarted;
        *self.num_steps_in_transaction.borrow_mut() = 0;
        self.update_exchange_metadata();
    }

    fn flush(&mut self) {
        // println!("{}:{} flush", Runtime::worker_index(), self.global_id);
        *self.flush_state.borrow_mut() = FlushState::FlushRequested;
    }

    fn is_flush_complete(&self) -> bool {
        // flushed == true and all rebalancing state has been flushed
        *self.flush_state.borrow() == FlushState::FlushCompleted
    }

    /// Checkpoint the current sharding policy only.
    fn checkpoint(
        &mut self,
        base: &StoragePath,
        pid: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), crate::Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;

        let checkpoint = RebalanceExchangeSenderCheckpoint {
            current_policy: *self.current_policy.borrow(),
            key_distribution: self.key_distribution.borrow().clone(),
        };

        let checkpoint_bytes = serde_json::to_vec(&checkpoint).unwrap();

        let mut buf = FBuf::new();
        buf.extend_from_slice(&checkpoint_bytes);

        files.push(
            Runtime::storage_backend()
                .unwrap()
                .write(&Self::checkpoint_file(base, pid), buf)?,
        );

        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), crate::Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;

        let file_path = Self::checkpoint_file(base, pid);
        let content = Runtime::storage_backend().unwrap().read(&file_path)?;

        let checkpoint = serde_json::from_slice::<RebalanceExchangeSenderCheckpoint>(&content)
            .map_err(|e| {
                crate::Error::Runtime(RuntimeError::CheckpointParseError(format!(
                    "invalid RebalancingExchangeSender checkpoint for operator {}: {e}",
                    self.global_id
                )))
            })?;

        *self.current_policy.borrow_mut() = checkpoint.current_policy;
        self.balancer
            .set_policy_for_stream(self.input_node_id, checkpoint.current_policy);
        *self.key_distribution.borrow_mut() = checkpoint.key_distribution;

        // Report current policy to metadata_exchange. See invalidate_clusters_for_bootstrapping.
        self.update_exchange_metadata();

        Ok(())
    }

    fn clear_state(&mut self) -> Result<(), crate::Error> {
        *self.current_policy.borrow_mut() = PartitioningPolicy::Shard;
        *self.key_distribution.borrow_mut() = KeyDistribution::new(Runtime::num_workers());

        // Clear the metadata. If the operator is deactivated during bootstrapping, its metadata
        // shouldn't confuse the balancer.
        self.metadata_exchange
            .clear_local_operator_metadata(self.global_id.local_node_id().unwrap());
        Ok(())
    }
}

impl<B, C> StreamingTernarySinkOperator<B, EmptyCheckpoint<Vec<Arc<B>>>, TimedSpine<B, C>>
    for RebalancingExchangeSender<B, C>
where
    B: ZBatch<Time = ()>,
    C: WithClock + 'static,
{
    fn eval(
        self: Rc<Self>,
        delta: Cow<'_, B>,
        delayed_accumulator: Cow<'_, EmptyCheckpoint<Vec<Arc<B>>>>,
        delayed_trace: Cow<'_, TimedSpine<B, C>>,
    ) -> impl futures::Stream<Item = (bool, Option<Position>)> + 'static {
        // println!("{}:{} eval", Runtime::worker_index(), self.global_id);

        let batch_factories = self.batch_factories.clone();
        let delayed_accumulator = delayed_accumulator.into_owned().val;
        let mut delayed_trace = delayed_trace.ro_snapshot();
        let chunk_size = splitter_output_chunk_size();

        let delta = delta.into_owned();

        *self.num_steps_in_transaction.borrow_mut() += 1;

        assert_ne!(*self.flush_state.borrow(), FlushState::RebalanceInProgress);

        // 1. We first partition the `delta` batch based on the latest policy.
        // 2. In addition if a rebalancing is requested (latest policy != current policy), we
        //   - store the current accumulator and integral state in rebalance_state, so we can rebalance them at the end of the transaction.
        //   - instantly clear the current state of the accumulator.
        // 3. We start the actual rebalancing after receiving the `flush` notification for the
        //    current transaction. During rebalancing, we:
        //   - retract the old contents of the integral.
        //   - partition saved accumulator and integral state based on the latest policy.
        //
        // Multiple rebalancings can be requested during the same transaction. The new policy instantly takes effect
        // for new deltas. The integral state only gets rebalanced at the very end of the transaction. Accumulator state
        // is cleared instantly, is preserved in rebalance_state, and is repartitioned at the end of the transaction.
        //
        // Why not clear integral state instantly on rebalancing instead of producing and merging relatively expensive
        // retractions? Because otherwise retractions will not appear in the output of the accumulator, which is in turn
        // used as input to other operators such as join (see `accumulator_stream` in the diagram above).
        stream! {
            // Policy change? Update rebalancing state.
            let new_policy = self.balancer.get_optimal_policy(self.input_node_id).unwrap();

            if *self.flush_state.borrow() == FlushState::FlushCompleted {
                assert!(delta.is_empty());
                // No policy change expected after commit.

                // if *self.current_policy.borrow() != new_policy  {
                //     println!("{}: current_policy: {:?}, new_policy: {:?}", self.input_node_id, *self.current_policy.borrow(), new_policy);
                // }
                assert_eq!(*self.current_policy.borrow(), new_policy);
                // ExchangeReceiver expects precisely one flush per transaction.
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut std::iter::repeat_with(move || (B::dyn_empty(&batch_factories), false)).take(Runtime::num_workers()),
                ));

                self.update_exchange_metadata();
                yield (true, None);
                return;
            }

            // println!("{}: old_policy: {:?}, new_policy: {:?}", Runtime::worker_index(), *self.current_policy.borrow(), new_policy);
            self.update_policy(new_policy, delayed_accumulator);

            // println!(
            //     "{} eval {:?} (current_policy: {:?})",
            //     Runtime::worker_index(),
            //     delta,
            //     *self.current_policy.borrow()
            // );

            // Partition `delta` based on the current policy.
            let batches = self.partition_batch(delta);

            let ready_to_commit = self.ready_to_commit();

            // If we're not yet flushing the transaction or we are flushing but there is no rebalancing to be done, return after
            // partitioning the `delta` batch. Otherwise, we'll continue to the rebalancing phase.
            let rebalance = ready_to_commit && self.rebalance_state.borrow().is_some();

            // println!("{}: delta: {:?}", Runtime::worker_index(), batches);

            let flush_complete = ready_to_commit && self.rebalance_state.borrow().is_none();

            assert!(self.exchange.try_send_all(
                self.worker_index,
                &mut batches.into_iter().map(|batch| (batch, flush_complete)),
            ));

            if !rebalance {
                if flush_complete {
                    // if Runtime::worker_index() == 0 {
                    //     println!("{}: flush complete 1 ({:?})", self.input_node_id, *self.current_policy.borrow());
                    // }
                    *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
                }
                // Let other workers know that no more rebalancing is allowed in the current transaction.
                self.update_exchange_metadata();
                yield (true, None);
                return;
            } else {
                *self.flush_state.borrow_mut() = FlushState::RebalanceInProgress;
                self.update_exchange_metadata();
                yield (false, None);
            }

            // We have no more inputs to process, but there is rebalancing work to be done.
            self.rebalance_start_time.borrow_mut().replace(Instant::now());

            // Used to measure step duration and add it to the total rebalancing time.
            let mut step_start_time = Instant::now();

            let RebalanceState { mut accumulator, trace_policy } = self.rebalance_state.borrow_mut().take().unwrap();

            *self.rebalance_accumulator_size.borrow_mut() = accumulator.len();
            *self.rebalance_integral_size.borrow_mut() = delayed_trace.len();

            let mut integral_cursor = delayed_trace.cursor();

            let mut builders = self.create_builders(chunk_size);

            // Retract the contents of the integral by sending it with negated weights to the accumulator
            // in the same worker.
            while integral_cursor.key_valid() {
                self.process_retractions(&mut integral_cursor, &mut builders[self.worker_index], chunk_size);

                let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                // println!("{}: integral retractions: {:?}", Runtime::worker_index(), batches);
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut batches.into_iter(),
                ));
                builders = self.create_builders(chunk_size);
                self.update_exchange_metadata();
                self.update_total_rebalancing_time(&step_start_time);
                yield (false, None);
                step_start_time = Instant::now();
            }

            // Start new cursors.
            let mut accumulator_cursor = accumulator.consuming_cursor(None, None);
            let mut integral_cursor = delayed_trace.consuming_cursor(None, None);

            // Re-partition accumulator.
            while accumulator_cursor.key_valid() {
                self.repartition_after_unicast(&mut accumulator_cursor, &mut builders, chunk_size);

                let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                // println!("{}: acc insertions: {:?}", Runtime::worker_index(), batches);
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut batches.into_iter(),
                ));
                builders = self.create_builders(chunk_size);
                self.update_exchange_metadata();
                self.update_total_rebalancing_time(&step_start_time);
                yield (false, None);
                step_start_time = Instant::now();
            }

            // Repartition integral.
            while integral_cursor.key_valid() {
                self.repartition(trace_policy, &mut integral_cursor, &mut builders, chunk_size);

                let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), !integral_cursor.key_valid())).collect();
                // println!("{}: integral insertions: {:?}", Runtime::worker_index(), batches);
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut batches.into_iter(),
                ));
                builders = self.create_builders(chunk_size);
                self.update_exchange_metadata();

                if integral_cursor.key_valid(){
                    self.update_exchange_metadata();
                    self.update_total_rebalancing_time(&step_start_time);
                    yield (false, None);
                    step_start_time = Instant::now();
                } else {
                    // if Runtime::worker_index() == 0 {
                    //     println!("{}: flush complete 2 ({:?})", self.input_node_id, *self.current_policy.borrow());
                    // }

                    *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
                    self.update_exchange_metadata();
                    *self.rebalance_accumulator_size.borrow_mut() = 0;
                    *self.rebalance_integral_size.borrow_mut() = 0;
                    self.update_total_rebalancing_time(&step_start_time);
                    yield (true, None);
                    return;
                }
            }

            let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), true)).collect();
            // println!("{}: final batches: {:?}", Runtime::worker_index(), batches);
            assert!(self.exchange.try_send_all(
                self.worker_index,
                &mut batches.into_iter(),
            ));

            // if Runtime::worker_index() == 0 {
            //     println!("{}: flush complete 3 ({:?})", self.input_node_id, *self.current_policy.borrow());
            // }

            *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
            self.update_exchange_metadata();
            *self.rebalance_accumulator_size.borrow_mut() = 0;
            *self.rebalance_integral_size.borrow_mut() = 0;
            self.update_total_rebalancing_time(&step_start_time);
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
