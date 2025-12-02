use crate::{
    algebra::IndexedZSet,
    circuit::{
        checkpointer::EmptyCheckpoint,
        circuit_builder::{register_replay_stream, MetadataExchange, StreamId},
        metadata::{BatchSizeStats, OperatorLocation, OperatorMeta, INPUT_BATCHES_LABEL},
        operator_traits::Operator,
        splitter_output_chunk_size, GlobalNodeId, NodeId, OwnershipPreference, WithClock,
    },
    circuit_cache_key,
    operator::{
        async_stream_operators::StreamingTernarySinkOperator,
        communication::{Exchange, ExchangeReceiver},
        dynamic::{
            accumulate_trace::{
                AccumulateBoundsId, AccumulateTraceAppend, AccumulateTraceId, AccumulateZ1Trace,
            },
            trace::{DelayedTraceId, TimedSpine, TraceBounds},
        },
        Z1,
    },
    trace::{
        deserialize_indexed_wset, merge_batches, serialize_indexed_wset, spine_async::SpineCursor,
        Batch, BatchReader, Builder, Cursor, SpineSnapshot, WithSnapshot,
    },
    utils::{
        components,
        maxsat::{Cost, HardConstraint, MaxSat, Variable, VariableIndex},
    },
    Circuit, Position, Runtime, Scope, Stream, Timestamp,
};
use async_stream::stream;
use futures::Stream as AsyncStream;
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

circuit_cache_key!(BalancedTraceId<C: Circuit, B: IndexedZSet>(StreamId => (Stream<C, B>, Stream<C, TimedSpine<B, C>>)));

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
    integrals: BTreeMap<NodeId, (NodeId, NodeId, NodeId)>,
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
        let (exchange_sender, accumulator, integral) = self.integrals.get(&stream).unwrap();

        let accumulator_metadata = self
            .metadata_exchange
            .get_global_operator_metadata_typed::<AccumulatorMeta>(*accumulator);
        let integral_metadata = self
            .metadata_exchange
            .get_global_operator_metadata_typed::<IntegralMeta>(*integral);

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
            assert!(exchange_sender_metadata
                .iter()
                .all(|metadata| metadata == &exchange_sender_metadata[0]));
            let policy = exchange_sender_metadata[0].as_ref().unwrap().current_policy;
            self.fix_policy_for_transaction(*exchange_sender, policy);
        }

        let worker_sizes: Vec<u64> = std::iter::zip(accumulator_metadata, integral_metadata)
            .map(|(accumulator_metadata, integral_metadata)| {
                accumulator_metadata.unwrap().num_entries + integral_metadata.unwrap().num_entries
            })
            .collect();

        let total_size: u64 = worker_sizes.iter().cloned().sum();
        let max_size = worker_sizes.iter().cloned().max().unwrap();
        let num_workers = worker_sizes.len() as u64;

        BTreeMap::from([
            (Policy::Shard, Cost(max_size)),
            (Policy::Broadcast, Cost(total_size)),
            (Policy::Balance, Cost((total_size / num_workers) * 2)),
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

    pub fn register_integral(
        &self,
        stream: NodeId,
        exchange_sender: NodeId,
        accumulator: NodeId,
        integral: NodeId,
    ) {
        self.inner
            .borrow_mut()
            .integrals
            .insert(stream, (exchange_sender, accumulator, integral));
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
    ) -> (Stream<C, B>, Stream<C, TimedSpine<B, C>>) {
        if Runtime::num_workers() == 1 {
            let trace = self.dyn_accumulate_trace(trace_factories, batch_factories);
            return (self.clone(), trace);
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
                        )
                        .with_metadata_exchange(circuit.metadata_exchange()),
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
                        Z1::new(EmptyCheckpoint::<Vec<Arc<B>>>::new()),
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
                        AsyncStreamingTernarySinkWrapper::new(
                            RebalancingExchangeSender::<B, C>::new(
                                batch_factories,
                                trace_factories,
                                worker_index,
                                Some(location),
                                exchange,
                                circuit.balancer().clone(),
                                circuit.metadata_exchange().clone(),
                            ),
                        ),
                        self,
                        &delayed_acc,
                        &delayed_trace,
                    );

                    // Add ExchangeSender -> ExchangeReceiver dependency.
                    circuit.add_dependency(sender_node_id, sharded_stream.local_node_id());

                    // Register accumulator and integral with the balancer, so it knows their IDs.
                    balancer.register_integral(
                        self.local_node_id(),
                        sender_node_id,
                        accumulator_stream.local_node_id(),
                        trace.local_node_id(),
                    );

                    (sharded_stream, trace)
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
    accumulator: Vec<Arc<B>>,
    trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
}

pub struct RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
    C: WithClock + 'static,
{
    batch_factories: B::Factories,
    trace_factories: <TimedSpine<B, C> as BatchReader>::Factories,

    node_id: NodeId,
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

    phantom: PhantomData<C>,
}

impl<B, C> RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
    C: WithClock,
{
    fn new(
        batch_factories: &B::Factories,
        trace_factories: &<TimedSpine<B, C> as BatchReader>::Factories,
        worker_index: usize,
        location: OperatorLocation,
        exchange: Arc<Exchange<(B, bool)>>,
        balancer: Balancer,
        metadata_exchange: MetadataExchange,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());
        Self {
            batch_factories: batch_factories.clone(),
            trace_factories: trace_factories.clone(),
            node_id: NodeId::new(0),
            worker_index,
            location,
            exchange,
            balancer,
            metadata_exchange,
            input_batch_stats: BatchSizeStats::new(),
            flush_state: RefCell::new(FlushState::TransactionStarted),
            current_policy: RefCell::new(Policy::Shard),
            retractions: RefCell::new(None),
            phantom: PhantomData,
        }
    }

    fn exchange_metadata(&self) -> RebalancingExchangeSenderExchangeMetadata {
        RebalancingExchangeSenderExchangeMetadata {
            flushed: self.flush_state.borrow() == FlushState::FlushCompleted,
            current_policy: *self.current_policy.borrow(),
        }
    }

    fn start_rebalancing(
        &self,
        delayed_accumulator: Vec<Arc<B>>,
        delayed_trace: SpineSnapshot<<C::Time as Timestamp>::TimedBatch<B>>,
    ) {
        *self.retractions.borrow_mut() = Some(RetractionState {
            accumulator: delayed_accumulator,
            trace: delayed_trace,
        });
    }

    // fn compute_outputs(&mut self, delta: &B) -> Vec<B> {
    //     let mut builders = Vec::with_capacity(Runtime::num_workers());

    //     let chunk_size = splitter_output_chunk_size();

    //     for _ in 0..Runtime::num_workers() {
    //         // We iterate over tuples in the batch in order; hence tuples added
    //         // to each shard are also ordered, so we can use the more efficient
    //         // `Builder` API (instead of `Batcher`) to construct output batches.
    //         builders.push(B::Builder::with_capacity(
    //             &self.batch_factories,
    //             chunk_size,
    //             chunk_size,
    //         ));
    //     }

    //     // Process all tuples in delta.
    //     let delta_cursor = delta.cursor();
    //     Self::compute_outputs_from_cursor(&mut Some(delta_cursor), &mut builders, false);

    //     // Process retractions from accumulator_cursor.
    //     Self::compute_outputs_from_cursor(
    //         &mut self.retractions.accumulator_cursor,
    //         &mut builders,
    //         true,
    //     );

    //     // Process retractions from integral_cursor.
    //     Self::compute_outputs_from_cursor(
    //         &mut self.retractions.integral_cursor,
    //         &mut builders,
    //         true,
    //     );

    //     // let mut cursor = batch.consuming_cursor(None, None);
    //     // if cursor.has_mut() {
    //     //     while cursor.key_valid() {
    //     //         let b = &mut builders[cursor.key().default_hash() as usize % shards];
    //     //         while cursor.val_valid() {
    //     //             b.push_diff_mut(cursor.weight_mut());
    //     //             b.push_val_mut(cursor.val_mut());
    //     //             cursor.step_val();
    //     //         }
    //     //         b.push_key_mut(cursor.key_mut());
    //     //         cursor.step_key();
    //     //     }
    //     // } else {
    //     //     while cursor.key_valid() {
    //     //         let b = &mut builders[cursor.key().default_hash() as usize % shards];
    //     //         while cursor.val_valid() {
    //     //             b.push_diff(cursor.weight());
    //     //             b.push_val(cursor.val());
    //     //             cursor.step_val();
    //     //         }
    //     //         b.push_key(cursor.key());
    //     //         cursor.step_key();
    //     //     }
    //     // }

    //     builders.into_iter().map(|builder| builder.done()).collect()
    // }

    // fn compute_outputs_from_cursor<Cur, T: Timestamp>(
    //     opt_cursor: &mut Option<Cur>,
    //     builders: &mut Vec<B::Builder>,
    //     invert: bool,
    // ) -> usize
    // where
    //     Cur: Cursor<B::Key, B::Val, T, B::R>,
    // {
    //     let Some(cursor) = opt_cursor else {
    //         return 0;
    //     };

    //     todo!();

    //     if !cursor.key_valid() {
    //         *opt_cursor = None;
    //     }

    //     todo!()
    // }

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

    /// Partition delta based on the current policy.
    fn partition_batch(&self, delta: B) -> Vec<B> {
        todo!()
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AccumulatorMeta {
    num_entries: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntegralMeta {
    num_entries: u64,
}

impl<B, C> Operator for RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
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
        self.node_id = global_id.local_node_id().unwrap();
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.exchange
            .register_sender_callback(self.worker_index, cb)
    }

    fn ready(&self) -> bool {
        todo!()
        //self.exchange.ready_to_send(self.worker_index)
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

    fn flush_progress(&self) -> Option<Position> {
        todo!()
    }

    fn checkpoint(
        &mut self,
        base: &feldera_storage::StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn feldera_storage::FileCommitter>>,
    ) -> Result<(), crate::Error> {
        todo!("checkpoint current policy")
    }

    fn restore(
        &mut self,
        base: &feldera_storage::StoragePath,
        persistent_id: Option<&str>,
    ) -> Result<(), crate::Error> {
        todo!()
    }
}

impl<B, C> StreamingTernarySinkOperator<B, EmptyCheckpoint<Vec<Arc<B>>>, TimedSpine<B, C>>
    for RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
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

        let new_policy = self.balancer.get_optimal_policy(self.node_id).unwrap();

        let delta = delta.into_owned();

        stream! {
            if *self.flush_state.borrow() == FlushState::FlushCompleted {
                assert!(self.exchange.try_send_all(
                    self.worker_index,
                    &mut std::iter::repeat_with(move || (B::dyn_empty(&batch_factories), true)).take(Runtime::num_workers()),
                ));

                yield (true, None);
                return;
            }

            // Policy change?
            // - Clean old rebalancing state
            // - Set new rebalancing state
            self.update_policy(new_policy, delayed_accumulator, delayed_trace);

            // Partition `delta` based on the current policy.

            // If the flush state is FlushRequested and there is no rebalancing to be done, this will be the final output.
            let flushed = *self.flush_state.borrow() == FlushState::FlushRequested && self.retractions.borrow().is_none();

            let outputs = self.partition_batch(delta);
            assert!(self.exchange.try_send_all(
                self.worker_index,
                &mut outputs.into_iter().map(|output| (output, flushed)),
            ));

            if flushed {
                *self.flush_state.borrow_mut() = FlushState::FlushCompleted;
                yield (true, None);
                return;
            }

            // We have no more inputs to process, but there is rebalancing work to be done.

            // match self.flush_state {
            //     FlushState::TransactionStarted | FlushState::FlushRequested => {
            //         let new_policy = self.balancer.get_optimal_policy(self.node_id).unwrap();

            //         // Policy change?
            //         // - Clean old rebalancing state
            //         // - Set new rebalancing state
            //         if new_policy != self.current_policy {
            //             self.current_policy = new_policy;
            //             self.start_rebalancing(delayed_accumulator, delayed_trace);
            //         }

            //         let outputs = self.compute_outputs();

            //         todo!("Send outputs to the exchange");

            //         if self.flush_state == FlushState::FlushRequested && self.retractions.is_empty() {
            //             self.flush_state = FlushState::FlushCompleted;
            //         }
            //     }
            //     FlushState::FlushCompleted => {
            //         // Policy changes shouldn't happen after the flush.
            //         assert_eq!(
            //             self.current_policy,
            //             self.balancer.get_optimal_policy(self.node_id).unwrap()
            //         );
            //         assert!(self.retractions.is_empty());
            //         // Output empty batch.
            //     }
            // }

            // self.metadata_exchange
            //     .set_local_operator_metadata_typed(self.node_id, self.exchange_metadata());

            // yield (true, None);
            // // stats:
            //   input batch size
            //   number of rebalancings
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
