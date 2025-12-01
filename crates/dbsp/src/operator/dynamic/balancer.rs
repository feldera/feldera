use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    panic::Location,
    sync::{atomic::AtomicU64, Arc},
};

use ouroboros::self_referencing;
use petgraph::graph::UnGraph;

use crate::{
    algebra::IndexedZSet,
    circuit::{
        checkpointer::EmptyCheckpoint,
        circuit_builder::{register_replay_stream, MetadataExchange, StreamId},
        metadata::{
            BatchSizeStats, OperatorLocation, OperatorMeta, INPUT_BATCHES_LABEL, NUM_ENTRIES_LABEL,
        },
        operator_traits::{Operator, TernarySinkOperator},
        NodeId, OwnershipPreference, WithClock,
    },
    circuit_cache_key,
    operator::{
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
        Batch, BatchReader, Spine, SpineSnapshot,
    },
    utils::{
        components,
        maxsat::{Cost, HardConstraint, MaxSat, Variable, VariableIndex},
    },
    Circuit, Runtime, Scope, Stream,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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
    integrals: BTreeMap<NodeId, (NodeId, NodeId)>,
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
            let mut domain = self.estimate_cost(*stream);
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

    fn estimate_cost(&self, stream: NodeId) -> BTreeMap<Policy, Cost> {
        let (accumulator, integral) = self.integrals.get(&stream).unwrap();

        let accumulator_metadata = self
            .metadata_exchange
            .get_global_operator_metadata(*accumulator);
        let integral_metadata = self
            .metadata_exchange
            .get_global_operator_metadata(*integral);

        let worker_sizes: Vec<u64> = std::iter::zip(accumulator_metadata, integral_metadata)
            .map(|(accumulator_metadata, integral_metadata)| {
                accumulator_metadata
                    .unwrap()
                    .get(NUM_ENTRIES_LABEL)
                    .unwrap()
                    .as_u64()
                    .unwrap()
                    + integral_metadata
                        .unwrap()
                        .get(NUM_ENTRIES_LABEL)
                        .unwrap()
                        .as_u64()
                        .unwrap()
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

#[derive(Debug)]
pub struct Balancer {
    inner: RefCell<BalancerInner>,
}

impl Balancer {
    pub fn new(metadata_exchange: &MetadataExchange) -> Self {
        Self {
            inner: RefCell::new(BalancerInner::new(metadata_exchange)),
        }
    }

    pub fn register_integral(&self, stream: NodeId, accumulator: NodeId, integral: NodeId) {
        self.inner
            .borrow_mut()
            .integrals
            .insert(stream, (accumulator, integral));
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
        let cluster_index = *self.inner.borrow().stream_to_cluster.get(&node_id).unwrap();
        self.inner.borrow_mut().clusters[cluster_index]
            .fixed_policy
            .insert(node_id, policy);
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
                        Z1::new(EmptyCheckpoint::<Vec<B>>::new()),
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
                        RebalancingExchangeSender::<B, C>::new(
                            worker_index,
                            Some(location),
                            exchange,
                            start_wait_usecs,
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

#[self_referencing]
struct RetractionState<B: Batch> {
    accumulator_snapshot: SpineSnapshot<B>,
    #[borrows(accumulator_snapshot)]
    accumulator_cursor: SpineCursor<B>,

    integral_snapshot: SpineSnapshot<B>,
    #[borrows(integral_snapshot)]
    integral_cursor: SpineCursor<B>,
}

pub struct RebalancingExchangeSender<B, C>
where
    B: Batch,
    C: WithClock + 'static,
{
    worker_index: usize,
    location: OperatorLocation,
    outputs: Vec<B>,
    exchange: Arc<Exchange<(B, bool)>>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    flush_requested: bool,

    current_policy: Policy,

    retractions: Option<RetractionState<B>>,

    // The instant when the sender produced its outputs, and the
    // receiver starts waiting for all other workers to produce their
    // outputs.
    start_wait_usecs: Arc<AtomicU64>,
    phantom: PhantomData<C>,
}

impl<B, C> RebalancingExchangeSender<B, C>
where
    B: Batch,
    C: WithClock,
{
    fn new(
        worker_index: usize,
        location: OperatorLocation,
        exchange: Arc<Exchange<(B, bool)>>,
        start_wait_usecs: Arc<AtomicU64>,
    ) -> Self {
        debug_assert!(worker_index < Runtime::num_workers());
        Self {
            worker_index,
            location,
            outputs: Vec::with_capacity(Runtime::num_workers()),
            exchange,
            input_batch_stats: BatchSizeStats::new(),
            flushed: false,
            current_policy: Policy::Shard,
            retractions: None,
            start_wait_usecs,
            phantom: PhantomData,
        }
    }
}

impl<B, C> Operator for RebalancingExchangeSender<B, C>
where
    B: Batch,
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

    fn flush(&mut self) {
        self.flushed = true;
    }

    fn is_flush_complete(&self) -> bool {
        // flushed == true and all rebalancing state has been flushed
        todo!()
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

impl<B, C> TernarySinkOperator<B, EmptyCheckpoint<Vec<B>>, TimedSpine<B, C>>
    for RebalancingExchangeSender<B, C>
where
    B: Batch<Time = ()>,
    C: WithClock + 'static,
{
    async fn eval(
        &mut self,
        delta: Cow<'_, B>,
        delayed_accumulator: Cow<'_, EmptyCheckpoint<Vec<B>>>,
        delayed_trace: Cow<'_, TimedSpine<B, C>>,
    ) {
        // Get metadata from metadata exchange.

        // stats:
        //   input batch size
        //   number of rebalancings

        // Reevaluate the policy.

        // If we (and all other workers) haven't flushed yet, update metadata, reevaluate policy.
        // Policy change?
        // - Clean old rebalancing state
        // - Set new rebalancing state

        // Input has been flushed, but there's rebalancing state remaining -- make progress rebalancing.
        //
    }

    fn input_preference(
        &self,
    ) -> (
        OwnershipPreference,
        OwnershipPreference,
        OwnershipPreference,
    ) {
        (
            OwnershipPreference::PREFER_OWNED,
            OwnershipPreference::INDIFFERENT,
            OwnershipPreference::INDIFFERENT,
        )
    }
}
