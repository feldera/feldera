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
            accumulate_trace::{
                AccumulateBoundsId, AccumulateTraceAppend, AccumulateTraceId, AccumulateZ1Trace,
            },
            trace::{DelayedTraceId, TimedSpine, TraceBounds},
        },
        require_persistent_id, Z1,
    },
    trace::{
        deserialize_indexed_wset, merge_batches, serialize_indexed_wset, spine_async::SpineCursor,
        Batch, BatchReader, BatchReaderFactories, Builder, Cursor, MergeCursor, Spine,
        SpineSnapshot, TupleBuilder, WithSnapshot,
    },
    utils::{
        components,
        maxsat::{Cost, HardConstraint, MaxSat, Variable, VariableIndex},
    },
    Circuit, Position, Runtime, Scope, Stream, Timestamp, ZWeight,
};
use async_stream::stream;
use feldera_storage::{fbuf::FBuf, FileCommitter, StoragePath};
use num::abs;
use petgraph::graph::UnGraph;
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    panic::Location,
    rc::Rc,
    sync::{atomic::AtomicU64, Arc},
};

circuit_cache_key!(BalancedTraceId<C: Circuit, B: IndexedZSet>(StreamId => (Stream<C, B>, Stream<C, Option<Spine<B>>>, Stream<C, TimedSpine<B, C>>)));

#[derive(Debug)]
pub struct JoinConstraint {
    v1: VariableIndex,
    v2: VariableIndex,
}

impl JoinConstraint {
    pub fn create_join_constraint(maxsat: &mut MaxSat, v1: VariableIndex, v2: VariableIndex) {
        let constraint = Self::new(v1, v2);
        maxsat.add_constraint(Box::new(constraint));
    }

    fn new(v1: VariableIndex, v2: VariableIndex) -> Self {
        Self { v1, v2 }
    }

    fn propagate_inner(
        &self,
        all_variables: &mut [Variable],
        i1: VariableIndex,
        i2: VariableIndex,
        affected_variables: &mut BTreeSet<VariableIndex>,
    ) {
        let v1 = &all_variables[i1.0].domain;
        //println!("{} = {:?}", all_variables[i1.0].name, v1);

        let remove_shard =
            !v1.contains_key(&Policy::Shard.into()) && !v1.contains_key(&Policy::Broadcast.into());
        let remove_broadcast = !v1.contains_key(&Policy::Broadcast.into())
            && !v1.contains_key(&Policy::Balance.into());
        let remove_balance = !v1.contains_key(&Policy::Balance.into());

        let v2 = &mut all_variables[i2.0].domain;

        let mut v2_modified = false;

        if remove_shard {
            //println!("removing SHARD from {}", all_variables[i2.0].name);
            v2_modified |= v2.remove(&Policy::Shard.into()).is_some();
        }
        if remove_broadcast {
            //println!("removing BROADCAST from {}", all_variables[i2.0].name);
            v2_modified |= v2.remove(&Policy::Broadcast.into()).is_some();
        }
        if remove_balance {
            //println!("removing BALANCE from {}", all_variables[i2.0].name);
            v2_modified |= v2.remove(&Policy::Balance.into()).is_some();
        }

        if v2_modified {
            affected_variables.insert(i2);
        }
    }
}

impl HardConstraint for JoinConstraint {
    fn propagate(
        &self,
        all_variables: &mut [Variable],
        affected_variables: &mut BTreeSet<VariableIndex>,
    ) {
        self.propagate_inner(all_variables, self.v1, self.v2, affected_variables);
        self.propagate_inner(all_variables, self.v2, self.v1, affected_variables);
    }

    fn variables(&self) -> Vec<VariableIndex> {
        vec![self.v1, self.v2, self.v2]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum Policy {
    Shard = 0,
    Broadcast = 1,
    Balance = 2,
}

impl TryFrom<u8> for Policy {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Policy::Shard),
            1 => Ok(Policy::Broadcast),
            2 => Ok(Policy::Balance),
            _ => Err(()),
        }
    }
}
impl From<Policy> for u8 {
    fn from(value: Policy) -> Self {
        value as u8
    }
}

#[derive(Default, Debug)]
struct Cluster {
    streams: BTreeSet<NodeId>,
    joins: BTreeMap<NodeId, (NodeId, NodeId)>,

    fixed_policy: BTreeMap<NodeId, Policy>,

    solution: Option<BTreeMap<NodeId, Policy>>,
}

#[derive(Default, Debug)]
struct BalancerInner {
    integrals: BTreeMap<NodeId, NodeId>,
    joins: BTreeMap<NodeId, (NodeId, NodeId)>,
    clusters: Vec<Cluster>,
    stream_to_cluster: BTreeMap<NodeId, usize>,
    metadata_exchange: MetadataExchange,
}

impl BalancerInner {
    fn new(metadata_exchange: &MetadataExchange) -> Self {
        Self {
            integrals: BTreeMap::new(),
            joins: BTreeMap::new(),
            clusters: Vec::new(),
            stream_to_cluster: BTreeMap::new(),
            metadata_exchange: metadata_exchange.clone(),
        }
    }

    fn solve_cluster(&mut self, cluster_index: usize) {
        //let cluster = &mut self.clusters[cluster_index];

        let mut maxsat = MaxSat::new();
        let mut variable_indexes = BTreeMap::new();

        for stream in self.clusters[cluster_index].streams.clone().iter() {
            let mut domain = self.compute_domain(*stream);
            if let Some(fixed_policy) = self.clusters[cluster_index].fixed_policy.get(stream) {
                domain.retain(|policy, _| *policy == *fixed_policy);
            }

            let variable_index = maxsat.add_variable(&stream.to_string(), domain);
            variable_indexes.insert(*stream, variable_index);
        }

        for (_join, (left, right)) in self.clusters[cluster_index].joins.iter() {
            maxsat.add_constraint(Box::new(JoinConstraint::new(
                variable_indexes[left],
                variable_indexes[right],
            )));
        }

        let solution = maxsat.solve().unwrap();
        let solution: BTreeMap<NodeId, Policy> = variable_indexes
            .iter()
            .map(|(stream, variable_index)| {
                (
                    *stream,
                    Policy::try_from(*solution.get(variable_index).unwrap()).unwrap(),
                )
            })
            .collect();

        self.clusters[cluster_index].solution = Some(solution);
    }

    fn solve_all_clusters(&mut self) {
        let num_clusters = self.clusters.len();

        for i in 0..num_clusters {
            self.solve_cluster(i);
        }
    }

    // Called when an exchange operator is flushed. At this point
    // it can no longer change its policy until the next transaction.
    pub fn fix_policy_for_transaction(&mut self, node_id: NodeId, policy: Policy) {
        let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();
        self.clusters[cluster_index]
            .fixed_policy
            .insert(node_id, policy);
    }

    fn compute_domain(&mut self, stream: NodeId) -> BTreeMap<Policy, Cost> {
        let exchange_sender = self.integrals.get(&stream).unwrap();

        let exchange_sender_metadata: Vec<Option<RebalancingExchangeSenderExchangeMetadata>> = self
            .metadata_exchange
            .get_global_operator_metadata_typed::<RebalancingExchangeSenderExchangeMetadata>(
                *exchange_sender,
            );

        if exchange_sender_metadata.iter().any(|metadata| {
            if let Some(metadata) = metadata {
                metadata.flushed
            } else {
                false
            }
        }) {
            assert!(exchange_sender_metadata
                .iter()
                .all(|metadata| metadata.is_some()));
            // TODO: assert: current policy is the same for all exchange senders.
            // assert!(exchange_sender_metadata
            //     .iter()
            //     .all(|metadata| metadata == &exchange_sender_metadata[0]));
            let policy = exchange_sender_metadata[0].as_ref().unwrap().current_policy;
            self.fix_policy_for_transaction(*exchange_sender, policy);
        }

        let key_distribution = exchange_sender_metadata.iter().fold(
            KeyDistribution::new(Runtime::num_workers()),
            |mut key_distribution, metadata| {
                if let Some(metadata) = metadata {
                    key_distribution.merge(&metadata.key_distribution);
                };
                key_distribution
            },
        );

        let total_size: i64 = key_distribution.total_records();
        let max_size = key_distribution.sizes.iter().cloned().max().unwrap();

        BTreeMap::from([
            (Policy::Shard, Cost(abs(max_size) as u64)),
            (Policy::Broadcast, Cost(abs(total_size) as u64)),
            (
                Policy::Balance,
                Cost((abs(total_size) as u64 / Runtime::num_workers() as u64) * 2),
            ),
        ])
    }

    fn get_policy(&self, node_id: NodeId) -> Option<Policy> {
        let cluster_index = *self.stream_to_cluster.get(&node_id).unwrap();
        self.clusters[cluster_index]
            .solution
            .as_ref()?
            .get(&node_id)
            .cloned()
    }
}

#[derive(Debug, Clone)]
pub struct Balancer {
    inner: Rc<RefCell<BalancerInner>>,
}

impl Balancer {
    pub fn new(metadata_exchange: &MetadataExchange) -> Self {
        Self {
            inner: Rc::new(RefCell::new(BalancerInner::new(metadata_exchange))),
        }
    }

    pub fn register_integral(&self, stream: NodeId, exchange_sender: NodeId) {
        self.inner
            .borrow_mut()
            .integrals
            .insert(stream, exchange_sender);
    }

    pub fn register_join(&self, join: NodeId, left: NodeId, right: NodeId) {
        self.inner.borrow_mut().joins.insert(join, (left, right));
    }

    // Invoked before the circuit starts running.
    pub fn prepare(&self) {
        self.compute_clusters();
    }

    // Invoked at the start of each transaction. Resets variable domains.
    pub fn start_transaction(&self) {
        for cluster in self.inner.borrow_mut().clusters.iter_mut() {
            cluster.fixed_policy.clear();
        }
    }

    // Invoked at the start of each step. Updates costs and solves the optimization problem.
    pub fn start_step(&self) {
        self.inner.borrow_mut().solve_all_clusters();
    }

    // Called when an exchange operator is flushed. At this point
    // it can no longer change its policy until the next transaction.
    pub fn fix_policy_for_transaction(&self, node_id: NodeId, policy: Policy) {
        self.inner
            .borrow_mut()
            .fix_policy_for_transaction(node_id, policy);
    }

    /// Optimal policy computed at the start of the step.
    pub fn get_optimal_policy(&self, node_id: NodeId) -> Option<Policy> {
        self.inner.borrow().get_policy(node_id)
    }

    fn compute_clusters(&self) {
        let mut inner = self.inner.borrow_mut();

        let mut join_graph = UnGraph::<NodeId, NodeId>::new_undirected();
        for (join, (left, right)) in inner.joins.iter() {
            let left_index = join_graph.add_node(*left);
            let right_index = join_graph.add_node(*right);
            join_graph.add_edge(left_index, right_index, *join);
        }

        let clusters = components(&join_graph);

        let clusters: Vec<Cluster> = clusters
            .into_iter()
            .map(|cluster| Cluster {
                streams: cluster
                    .0
                    .into_iter()
                    .map(|node| *join_graph.node_weight(node).unwrap())
                    .collect(),
                joins: cluster
                    .1
                    .into_iter()
                    .map(|edge| {
                        let join_id = *join_graph.edge_weight(edge).unwrap();
                        let (left, right) = inner.joins.get(&join_id).unwrap();
                        (join_id, (*left, *right))
                    })
                    .collect(),
                fixed_policy: BTreeMap::new(),
                solution: None,
            })
            .collect();

        for (index, cluster) in clusters.iter().enumerate() {
            for stream in cluster.streams.iter() {
                inner.stream_to_cluster.insert(*stream, index);
            }
        }

        inner.clusters = clusters;

        // TODO:
        // Validation:
        // - Each stream is in exactly one cluster.
        // - Each join is in exactly one cluster.
        // - Each cluster has at least one stream.
        // - Each cluster has at least one join.
    }
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: IndexedZSet,
{
    #[track_caller]
    pub fn dyn_accumulate_trace_with_balancer(
        &self,
        trace_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        batch_factories: &B::Factories,
    ) -> (
        Stream<C, B>,
        Stream<C, Option<Spine<B>>>,
        Stream<C, TimedSpine<B, C>>,
    ) {
        if Runtime::num_workers() == 1 {
            let trace = self.dyn_accumulate_trace(trace_factories, batch_factories);
            return (self.clone(), self.dyn_accumulate(&batch_factories), trace);
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

                    // Exchange object
                    let exchange = Exchange::with_runtime(
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
                            merge_batches(&batch_factories_clone, batches, &None, &None)
                        });

                    // Accumulator.
                    let (accumulator_stream, accumulator_snapshot_stream_val) =
                        sharded_stream.dyn_accumulate_with_feedback_stream(batch_factories);

                    // Integral.
                    let bounds = TraceBounds::new();

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

                    // Integral with metadata exchange
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
                    circuit.cache_insert(
                        AccumulateTraceId::new(sharded_stream.stream_id()),
                        trace.clone(),
                    );
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

                    // Connect the stream
                    acc_feedback.connect(&accumulator_feedback_stream);

                    // Exchange sender.
                    let sender_node_id = circuit.add_ternary_sink(
                        StreamingTernarySinkWrapper::new(RebalancingExchangeSender::<B, C>::new(
                            batch_factories,
                            worker_index,
                            Some(location),
                            exchange,
                            balancer.clone(),
                            circuit.metadata_exchange().clone(),
                        )),
                        self,
                        &delayed_acc,
                        &delayed_trace,
                    );

                    // Add ExchangeSender -> ExchangeReceiver dependency.
                    circuit.add_dependency(sender_node_id, sharded_stream.local_node_id());

                    // Register accumulator and integral with the balancer, so it knows their IDs.
                    balancer.register_integral(self.local_node_id(), sender_node_id);

                    (sharded_stream, accumulator_stream, trace)
                })
            })
            .clone()
    }
}

// impl<C, B> Stream<C, Spine<B>>
// where
//     C: Circuit,
//     B: Batch,
// {
//     /// Returns the trace of `self` delayed by one clock cycle.
//     pub fn accumulate_delay_trace_with_balancer(&self) -> Stream<C, SpineSnapshot<B>> {
//         todo!()
//     }
// }

struct RetractionState<B: Batch<Time = ()>, C: WithClock> {
    accumulator: SpineSnapshot<B>,
    trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
struct KeyDistribution {
    sizes: Vec<ZWeight>,
}

impl KeyDistribution {
    fn new(shards: usize) -> Self {
        Self {
            sizes: vec![0; shards],
        }
    }

    fn update(&mut self, shard: usize, weight: ZWeight) {
        self.sizes[shard] += weight;
    }

    fn merge(&mut self, other: &Self) {
        for (i, size) in self.sizes.iter_mut().enumerate() {
            *size += other.sizes[i];
        }
    }

    fn total_records(&self) -> ZWeight {
        self.sizes.iter().sum()
    }
}

pub struct RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
    C: WithClock + 'static,
{
    batch_factories: B::Factories,

    global_id: GlobalNodeId,
    worker_index: usize,
    location: OperatorLocation,

    exchange: Arc<Exchange<(B, bool)>>,

    balancer: Balancer,
    metadata_exchange: MetadataExchange,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flush_state: RefCell<FlushState>,

    current_policy: RefCell<Policy>,

    retractions: RefCell<Option<RetractionState<B, C>>>,

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
        balancer: Balancer,
        metadata_exchange: MetadataExchange,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());
        Self {
            batch_factories: batch_factories.clone(),
            global_id: GlobalNodeId::root(),
            worker_index,
            location,
            exchange,
            balancer,
            metadata_exchange,
            input_batch_stats: BatchSizeStats::new(),
            flush_state: RefCell::new(FlushState::TransactionStarted),
            current_policy: RefCell::new(Policy::Shard),
            retractions: RefCell::new(None),
            key_distribution: RefCell::new(KeyDistribution::new(Runtime::num_workers())),
            phantom: PhantomData,
        }
    }

    // fn exchange_metadata(&self) -> RebalancingExchangeSenderExchangeMetadata {
    //     RebalancingExchangeSenderExchangeMetadata {
    //         flushed: *self.flush_state.borrow() == FlushState::FlushCompleted,
    //         current_policy: *self.current_policy.borrow(),
    //     }
    // }

    fn update_exchange_metadata(&self) {
        let metadata = RebalancingExchangeSenderExchangeMetadata {
            flushed: *self.flush_state.borrow() == FlushState::FlushCompleted,
            current_policy: *self.current_policy.borrow(),
            key_distribution: self.key_distribution.borrow().clone(),
        };

        self.metadata_exchange
            .set_local_operator_metadata_typed(self.global_id.local_node_id().unwrap(), metadata);
    }

    fn start_rebalancing(
        &self,
        delayed_accumulator: Vec<Arc<B>>,
        delayed_trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
    ) {
        *self.retractions.borrow_mut() = Some(RetractionState {
            accumulator: SpineSnapshot::with_batches(&self.batch_factories, delayed_accumulator),
            trace: delayed_trace,
        });
    }

    fn update_policy(
        &self,
        new_policy: Policy,
        delayed_accumulator: Vec<Arc<B>>,
        delayed_trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
    ) {
        if new_policy != *self.current_policy.borrow() {
            *self.current_policy.borrow_mut() = new_policy;
            self.start_rebalancing(delayed_accumulator, delayed_trace);
        }
    }

    fn partition_batch(&self, delta: B) -> Vec<B> {
        match *self.current_policy.borrow() {
            Policy::Shard => self.shard_batch(delta),
            Policy::Balance => self.balance_batch(delta),
            Policy::Broadcast => self.broadcast_batch(delta),
        }
    }

    /// Partition delta based on the current policy.
    ///
    /// Returns after exhausting the cursor or when one of the builders reaches chunk_size entries,
    /// whichever comes first.
    fn process_insertions<TS>(
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
                    cursor.step_key();
                    if b.num_tuples() >= chunk_size {
                        break;
                    }
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
                            shard = cursor.key().default_hash() as usize % shards;
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

                    cursor.step_key();
                    has_values.fill(false);

                    if builders[shard].num_tuples() >= chunk_size {
                        break;
                    }
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
                    cursor.step_key();
                    if builders[0].num_tuples() >= chunk_size {
                        break;
                    }
                }
            }
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
        let builder = &mut builders[Runtime::worker_index()];

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
            cursor.step_key();
            if builder.num_tuples() >= chunk_size {
                break;
            }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RebalancingExchangeSenderExchangeMetadata {
    flushed: bool,
    current_policy: Policy,
    key_distribution: KeyDistribution,
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
        *self.flush_state.borrow_mut() = FlushState::TransactionStarted;
    }

    fn flush(&mut self) {
        *self.flush_state.borrow_mut() = FlushState::FlushRequested;
    }

    fn is_flush_complete(&self) -> bool {
        // flushed == true and all rebalancing state has been flushed
        *self.flush_state.borrow() == FlushState::FlushCompleted
    }

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
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut std::iter::repeat_with(move || (B::dyn_empty(&batch_factories), true)).take(Runtime::num_workers()),
                ));

                self.update_exchange_metadata();
                yield (true, None);
                return;
            }

            // Policy change?
            // - Clean old rebalancing state
            // - Set new rebalancing state
            let new_policy = self.balancer.get_optimal_policy(self.global_id.local_node_id().unwrap()).unwrap();
            self.update_policy(new_policy, delayed_accumulator, delayed_trace);

            // Partition `delta` based on the current policy.

            // If this is not the last input or it is the last input and there is no rebalancing to be done, this will be the final output.
            let flushed = *self.flush_state.borrow() == FlushState::TransactionStarted || self.retractions.borrow().is_none();

            let batches = self.partition_batch(delta);

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
            }

            // We have no more inputs to process, but there is rebalancing work to be done.

            let RetractionState { mut accumulator, mut trace } = self.retractions.borrow_mut().take().unwrap();

            let mut accumulator_cursor = accumulator.cursor();
            let mut integral_cursor = trace.cursor();

            let mut builders = self.create_builders(chunk_size);

            // Accumulator retractions
            while accumulator_cursor.key_valid() {
                self.process_retractions(&mut accumulator_cursor, &mut builders, chunk_size);

                if accumulator_cursor.key_valid() {
                    let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                    assert!(self.exchange.try_send_all(
                        self.worker_index,
                        &mut batches.into_iter(),
                    ));
                    builders = self.create_builders(chunk_size);
                    self.update_exchange_metadata();
                    yield (false, None);
                }
            }

            // Integral retractions
            while integral_cursor.key_valid() {
                self.process_retractions(&mut integral_cursor, &mut builders, chunk_size);

                if integral_cursor.key_valid() {
                    let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                    assert!(self.exchange.try_send_all(
                        self.worker_index,
                        &mut batches.into_iter(),
                    ));
                    builders = self.create_builders(chunk_size);
                    self.update_exchange_metadata();
                    yield (false, None);
                }
            }

            // Start new cursors.
            let mut accumulator_cursor = accumulator.consuming_cursor(None, None);
            let mut integral_cursor = trace.consuming_cursor(None, None);

            // Re-partition accumulator
            while accumulator_cursor.key_valid() {
                self.process_insertions(&mut accumulator_cursor, &mut builders, chunk_size);

                if accumulator_cursor.key_valid() {
                    let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                    assert!(self.exchange.try_send_all(
                        self.worker_index,
                        &mut batches.into_iter(),
                    ));
                    builders = self.create_builders(chunk_size);
                    self.update_exchange_metadata();
                    yield (false, None);
                }
            }

            // Repartition integral
            while integral_cursor.key_valid() {
                self.process_insertions(&mut integral_cursor, &mut builders, chunk_size);

                if integral_cursor.key_valid() {
                    let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), false)).collect();
                    assert!(self.exchange.try_send_all(
                        self.worker_index,
                        &mut batches.into_iter(),
                    ));
                    builders = self.create_builders(chunk_size);
                    self.update_exchange_metadata();
                    yield (false, None);
                }
            }

            // Send final output batches.
            let batches: Vec<(B, bool)> = builders.into_iter().map(|builder| (builder.done(), true)).collect();

            assert!(self.exchange.try_send_all(
                self.worker_index,
                &mut batches.into_iter(),
            ));
            self.update_exchange_metadata();
            yield (true, None);
            *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
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

#[cfg(test)]
mod test {
    use super::*;
}
