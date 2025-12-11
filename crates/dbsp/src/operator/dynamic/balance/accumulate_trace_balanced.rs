use crate::{
    algebra::{IndexedZSet, ZBatch},
    circuit::{
        checkpointer::EmptyCheckpoint,
        circuit_builder::{register_replay_stream, MetadataExchange, StreamId},
        metadata::{BatchSizeStats, OperatorLocation, OperatorMeta, INPUT_BATCHES_LABEL},
        operator_traits::Operator,
        splitter_output_chunk_size, GlobalNodeId, NodeId, OwnershipPreference, WithClock,
    },
    circuit_cache_key,
    dynamic::{ClonableTrait, Data as _, Erase},
    operator::{
        async_stream_operators::{StreamingTernarySinkOperator, StreamingTernarySinkWrapper},
        communication::{Exchange, ExchangeReceiver},
        dynamic::{
            accumulate_trace::{AccumulateBoundsId, AccumulateTraceAppend, AccumulateZ1Trace},
            balance::{rebalancing_accumulator::RebalancingAccumulatorInner, Balancer, Policy},
            trace::{DelayedTraceId, TimedSpine, TraceBounds},
        },
        require_persistent_id, Z1,
    },
    trace::{
        deserialize_indexed_wset, merge_batches, serialize_indexed_wset, spine_async::SpineCursor,
        Batch, BatchReader, BatchReaderFactories, Builder, Cursor, MergeCursor, Spine,
        SpineSnapshot, TupleBuilder, WithSnapshot,
    },
    Circuit, Position, Runtime, Scope, Stream, Timestamp, ZWeight,
};
use async_stream::stream;
use feldera_storage::{fbuf::FBuf, FileCommitter, StoragePath};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cell::RefCell,
    marker::PhantomData,
    panic::Location,
    rc::Rc,
    sync::{atomic::AtomicU64, Arc},
};

circuit_cache_key!(BalancedTraceId<C: Circuit, B: IndexedZSet>(StreamId => (Stream<C, Option<Spine<B>>>, Stream<C, TimedSpine<B, C>>)));

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
    ///              │                       │                 │delayed_acc
    ///              │                       ▼                 │
    ///              │               ┌────────────────┐      ┌──┐
    ///              │               │ExchangeReceiver│      │Z1│
    ///              │               └────────────────┘      └──┘
    ///              │                       .                 ▲
    ///              │                       .                 │
    ///              │                       ▼                 │
    ///              │             ┌──────────────────────┐    │
    ///              │             │RebalancingAccumulator│────┘
    /// delayed_trace│             └──────────────────────┘
    ///              │                       │
    ///              │                       │
    ///              ├──────────────────┐    │
    ///              │                  ▼    ▼
    ///      ┌───────┴─────────┐    ┌─────────────────────┐
    ///      │AccumulateZ1Trace│◄───┤AccumulateTraceAppend│
    ///      └─────────────────┘    └────────┬────────────┘
    ///                                      │
    ///                                      │trace
    ///                                      │
    ///                                      ▼
    /// ```
    ///
    /// The two feedback streams (delayed_acc and delayed_trace) are used by RebalancingExchangeSender to
    /// retract previously accumulated content of the integral when the sharding policy changes. The accumulator
    /// in particular is used to rebalance mid-transaction.
    #[track_caller]
    pub fn dyn_accumulate_trace_balanced(
        &self,
        trace_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> (Stream<C, Option<Spine<B>>>, Stream<C, TimedSpine<B, C>>) {
        if Runtime::num_workers() == 1 {
            let trace = self.dyn_accumulate_trace(trace_factories, batch_factories);
            return (self.dyn_accumulate(&batch_factories), trace);
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

                    let sharded_stream = circuit
                        .add_source(ExchangeReceiver::new(
                            worker_index,
                            Some(location),
                            exchange.clone(),
                            || Vec::new(),
                            start_wait_usecs.clone(),
                            |batches: &mut Vec<B>, batch: B| batches.push(batch),
                        ))
                        .apply_owned_named("merge shards", move |batches| {
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
                            &trace_factories,
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
                            .map(|name| format!("{name}.balanced.acc_snapshot"))
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

                    // Add ExchangeSender -> ExchangeReceiver dependency.
                    circuit.add_dependency(sender_node_id, sharded_stream.local_node_id());

                    // Register integral with the balancer.
                    balancer.register_integral(self.local_node_id(), sender_node_id);

                    (accumulator_stream, trace)
                })
            })
            .clone()
    }
}

/// The old contents of the integral and accumulator that needs to be rebalanced.
struct RebalanceState<B: Batch<Time = ()>, C: WithClock> {
    /// Accumulator state doesn't need to be retracted, because it will be cleared when the rebalancing starts.
    /// It only needs to be re-distributed using the new policy.
    accumulator: SpineSnapshot<B>,

    /// Integral state must be retracted and re-distributed using the new policy.
    trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,

    /// The policy that was used to accumulate the integral before the rebalancing.
    /// This is the policy used during the previous transaction.
    trace_policy: Policy,
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

    /// Current sharding policy.
    current_policy: RefCell<Policy>,

    /// When Some(), the policy has changed during the current transaction, and the operator must rebalance
    /// accumulator and integral state before committing the transaction.
    rebalance_state: RefCell<Option<RebalanceState<B, C>>>,

    /// Local key distribution of the input stream computed by the current worker.
    key_distribution: RefCell<KeyDistribution>,

    phantom: PhantomData<C>,
}

impl<B, C> RebalancingExchangeSender<B, C>
where
    B: ZBatch<Time = ()>,
    C: WithClock,
{
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
            flush_state: RefCell::new(FlushState::TransactionStarted),
            current_policy: RefCell::new(Policy::Shard),
            rebalance_state: RefCell::new(None),
            key_distribution: RefCell::new(KeyDistribution::new(Runtime::num_workers())),
            phantom: PhantomData,
        }
    }

    fn update_exchange_metadata(&self) {
        let metadata = RebalancingExchangeSenderExchangeMetadata {
            flushed: *self.flush_state.borrow() == FlushState::FlushCompleted,
            current_policy: *self.current_policy.borrow(),
            key_distribution: self.key_distribution.borrow().clone(),
        };
        // println!(
        //     "{} update_exchange_metadata: {:?}",
        //     Runtime::worker_index(),
        //     metadata
        // );

        self.metadata_exchange
            .set_local_operator_metadata_typed(self.global_id.local_node_id().unwrap(), metadata);
    }

    fn start_rebalancing(
        &self,
        old_policy: Policy,
        delayed_accumulator: Vec<Arc<B>>,
        delayed_trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
    ) {
        // Discard the accumulator state, so we don't need to retract it explicitly.
        self.accumulator_inner.borrow_mut().clear_state();

        // If the accumulator was populated with broadcast policy, we want to re-balance only
        // one copy of the data.
        let accumulator_batches = if old_policy != Policy::Broadcast || self.worker_index == 0 {
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
                    trace: delayed_trace,
                    trace_policy: old_policy,
                });
            }
        }
    }

    fn update_policy(
        &self,
        new_policy: Policy,
        delayed_accumulator: Vec<Arc<B>>,
        delayed_trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
    ) {
        let old_policy = *self.current_policy.borrow();
        if new_policy != old_policy {
            *self.current_policy.borrow_mut() = new_policy;
            self.start_rebalancing(old_policy, delayed_accumulator, delayed_trace);
        }
    }

    fn partition_batch(&self, delta: B) -> Vec<B> {
        match *self.current_policy.borrow() {
            Policy::Shard => self.shard_batch(delta),
            Policy::Balance => self.balance_batch(delta),
            Policy::Broadcast => self.broadcast_batch(delta),
        }
    }

    fn process_insertions<TS>(
        &self,
        old_policy: Policy,
        cursor: &mut dyn MergeCursor<B::Key, B::Val, TS, B::R>,
        builders: &mut [B::Builder],
        chunk_size: usize,
    ) where
        TS: Timestamp,
    {
        match old_policy {
            Policy::Broadcast => self.process_insertions_broadcast(
                cursor,
                &mut builders[self.worker_index],
                chunk_size,
            ),
            Policy::Shard | Policy::Balance => {
                self.process_insertions_shard(cursor, builders, chunk_size)
            }
        }
    }

    /// Partition cursor based on the current policy.
    ///
    /// Returns after exhausting the cursor or when one of the builders reaches chunk_size entries,
    /// whichever comes first.
    fn process_insertions_shard<TS>(
        &self,
        cursor: &mut dyn MergeCursor<B::Key, B::Val, TS, B::R>,
        builders: &mut [B::Builder],
        chunk_size: usize,
    ) where
        TS: Timestamp,
    {
        let shards = builders.len();

        match *self.current_policy.borrow() {
            Policy::Shard => {
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
                    if b.num_tuples() >= chunk_size {
                        break;
                    }
                    cursor.step_key();
                }
            }
            Policy::Balance => {
                let mut has_values = vec![false; shards];
                let mut shard = 0;

                while cursor.key_valid() {
                    while cursor.val_valid() && builders[shard].num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        if weight != 0 {
                            shard = cursor.val().default_hash() as usize % shards;
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
                    if builders[shard].num_tuples() >= chunk_size {
                        break;
                    }
                    cursor.step_key();
                    has_values.fill(false);
                }
            }
            Policy::Broadcast => {
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
                    if builders[0].num_tuples() >= chunk_size {
                        break;
                    }
                    cursor.step_key();
                }
            }
        }
    }

    fn process_insertions_broadcast<TS>(
        &self,
        cursor: &mut dyn MergeCursor<B::Key, B::Val, TS, B::R>,
        builder: &mut B::Builder,
        chunk_size: usize,
    ) where
        TS: Timestamp,
    {
        let shards = Runtime::num_workers();

        match *self.current_policy.borrow() {
            Policy::Shard => {
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
                    if builder.num_tuples() >= chunk_size {
                        break;
                    }
                    cursor.step_key();
                }
            }
            Policy::Balance => {
                let mut has_values = false;

                while cursor.key_valid() {
                    while cursor.val_valid() && builder.num_tuples() < chunk_size {
                        let mut weight = 0;
                        cursor.map_times(&mut |_t, w| {
                            weight += **w;
                        });
                        if weight != 0 {
                            let shard = cursor.val().default_hash() as usize % shards;
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
                    if builder.num_tuples() >= chunk_size {
                        break;
                    }
                    cursor.step_key();
                    has_values = false;
                }
            }
            Policy::Broadcast => unreachable!(),
        }
    }

    fn process_retractions<TB>(
        &self,
        cursor: &mut SpineCursor<TB>,
        builders: &mut [B::Builder],
        chunk_size: usize,
    ) where
        TB: Batch<Key = B::Key, Val = B::Val, R = B::R>,
    {
        let builder = &mut builders[self.worker_index];

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
            if builder.num_tuples() >= chunk_size {
                break;
            }
            cursor.step_key();
        }
    }

    fn create_builders(&self, chunk_size: usize) -> Vec<B::Builder> {
        std::iter::repeat_with(|| {
            B::Builder::with_capacity(&self.batch_factories, chunk_size, chunk_size)
        })
        .take(Runtime::num_workers())
        .collect()
    }

    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("rebalancing-exchange-{}.dat", persistent_id))
    }

    // Partitions the batch into `nshards` partitions based on the hash of the key.
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

    // Partitions the batch into `nshards` partitions based on the hash of the value.
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
                let key_shard = cursor.key().default_hash() as usize % shards;

                while cursor.val_valid() {
                    let b = &mut builders[cursor.val().default_hash() as usize % shards];
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
                    let b = &mut builders[cursor.key().default_hash() as usize % shards];
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

    // Partitions the batch into `nshards` partitions based on the hash of the value.
    fn broadcast_batch(&self, batch: B) -> Vec<B> {
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
        drop(cursor);

        self.key_distribution.borrow_mut().merge(&key_distribution);

        let mut outputs = Vec::with_capacity(Runtime::num_workers());
        for _i in 0..Runtime::num_workers() - 1 {
            outputs.push(batch.clone());
        }
        outputs.push(batch);
        outputs
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushState {
    TransactionStarted,
    FlushRequested,
    FlushCompleted,
}

/// Metadata shared by the ExchangeSender operators with balancer instances in all workers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebalancingExchangeSenderExchangeMetadata {
    /// The operator has flushed the current transaction - no more rebalancing is allowed in the current transaction.
    pub flushed: bool,

    /// Currently selected sharding policy.
    pub current_policy: Policy,

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
        // println!("{} start_transaction", Runtime::worker_index());
        *self.flush_state.borrow_mut() = FlushState::TransactionStarted;
        self.update_exchange_metadata();
    }

    fn flush(&mut self) {
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

        let mut buf = FBuf::new();
        buf.push(u8::from(*self.current_policy.borrow()));

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

        assert_eq!(content.len(), 1);

        let policy = Policy::try_from(content[0]).unwrap();

        *self.current_policy.borrow_mut() = policy;

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
        let batch_factories = self.batch_factories.clone();
        let delayed_accumulator = delayed_accumulator.into_owned().val;
        let delayed_trace = delayed_trace.ro_snapshot();
        let chunk_size = splitter_output_chunk_size();

        let delta = delta.into_owned();

        stream! {
            if *self.flush_state.borrow() == FlushState::FlushCompleted {
                // ExchangeReceiver expects precisely one flush per transaction.
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut std::iter::repeat_with(move || (B::dyn_empty(&batch_factories), false)).take(Runtime::num_workers()),
                ));

                self.update_exchange_metadata();
                yield (true, None);
                return;
            }

            // Policy change?
            // - Clean old rebalancing state
            // - Set new rebalancing state
            let new_policy = self.balancer.get_optimal_policy(self.input_node_id).unwrap();
            // println!("{}: old_policy: {:?}, new_policy: {:?}", Runtime::worker_index(), *self.current_policy.borrow(), new_policy);
            self.update_policy(new_policy, delayed_accumulator, delayed_trace);

            // println!(
            //     "{} eval {:?} (current_policy: {:?})",
            //     Runtime::worker_index(),
            //     delta,
            //     *self.current_policy.borrow()
            // );

            // Partition `delta` based on the current policy.

            // If this is not the last input or it is the last input and there is no rebalancing to be done, this will be the final output.
            let flushed = *self.flush_state.borrow() == FlushState::TransactionStarted || self.rebalance_state.borrow().is_none();

            let batches = self.partition_batch(delta);

            // println!("{}: delta: {:?}", Runtime::worker_index(), batches);

            assert!(self.exchange.try_send_all(
                self.worker_index,
                &mut batches.into_iter().map(|batch| (batch, flushed)),
            ));

            if flushed {
                if *self.flush_state.borrow() == FlushState::FlushRequested {
                    *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
                }
                self.update_exchange_metadata();
                yield (true, None);
                return;
            } else {
                yield (false, None);
            }

            // We have no more inputs to process, but there is rebalancing work to be done.

            let RebalanceState { mut accumulator, mut trace, trace_policy } = self.rebalance_state.borrow_mut().take().unwrap();

            // let mut accumulator_cursor = accumulator.cursor();
            let mut integral_cursor = trace.cursor();

            let mut builders = self.create_builders(chunk_size);

            // // Accumulator retractions
            // while accumulator_cursor.key_valid() {
            //     self.process_retractions(&mut accumulator_cursor, &mut builders, chunk_size);

            //     let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
            //     // println!("{}: acc retractions: {:?}", Runtime::worker_index(), batches);

            //     assert!(self.exchange.try_send_all(
            //         self.worker_index,
            //         &mut batches.into_iter(),
            //     ));
            //     builders = self.create_builders(chunk_size);
            //     self.update_exchange_metadata();
            //     yield (false, None);
            // }

            // Integral retractions
            while integral_cursor.key_valid() {
                self.process_retractions(&mut integral_cursor, &mut builders, chunk_size);

                let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                // println!("{}: integral retractions: {:?}", Runtime::worker_index(), batches);
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut batches.into_iter(),
                ));
                builders = self.create_builders(chunk_size);
                self.update_exchange_metadata();
                yield (false, None);
            }

            // Start new cursors.
            let mut accumulator_cursor = accumulator.consuming_cursor(None, None);
            let mut integral_cursor = trace.consuming_cursor(None, None);

            // Re-partition accumulator
            while accumulator_cursor.key_valid() {
                self.process_insertions_shard(&mut accumulator_cursor, &mut builders, chunk_size);

                let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                // println!("{}: acc insertions: {:?}", Runtime::worker_index(), batches);
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut batches.into_iter(),
                ));
                builders = self.create_builders(chunk_size);
                self.update_exchange_metadata();
                yield (false, None);
            }

            // Repartition integral
            while integral_cursor.key_valid() {
                self.process_insertions(trace_policy, &mut integral_cursor, &mut builders, chunk_size);

                let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), !integral_cursor.key_valid())).collect();
                // println!("{}: integral insertions: {:?}", Runtime::worker_index(), batches);
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut batches.into_iter(),
                ));
                builders = self.create_builders(chunk_size);
                self.update_exchange_metadata();

                if integral_cursor.key_valid(){
                    yield (false, None);
                } else {
                    *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
                    self.update_exchange_metadata();
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

            *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
            self.update_exchange_metadata();
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
