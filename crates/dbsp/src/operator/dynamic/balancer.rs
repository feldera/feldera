use std::{
    borrow::Cow,
    marker::PhantomData,
    ops::Deref,
    panic::Location,
    rc::Rc,
    sync::{atomic::AtomicU64, Arc},
};

use crate::{
    algebra::IndexedZSet,
    circuit::{
        checkpointer::EmptyCheckpoint,
        circuit_builder::{register_replay_stream, StreamId},
        metadata::{BatchSizeStats, OperatorLocation, OperatorMeta, INPUT_BATCHES_LABEL},
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
        deserialize_indexed_wset, merge_batches, serialize_indexed_wset, Batch, BatchReader, Spine,
        SpineSnapshot,
    },
    Circuit, Runtime, Scope, Stream,
};
circuit_cache_key!(BalancerId<C: Circuit>(() => Rc<Balancer<C>>));

circuit_cache_key!(BalancedTraceId<C: Circuit, B: IndexedZSet>(StreamId => (Stream<C, B>, Stream<C, TimedSpine<B, C>>)));

enum Policy {
    Shard,
    Broadcast,
}

pub struct Balancer<C> {
    phantom: PhantomData<C>,
}

impl<C> Balancer<C>
where
    C: Circuit,
{
    pub fn get_balancer(circuit: &C) -> Rc<Self> {
        circuit
            .cache_get_or_insert_with(BalancerId::new(()), || Rc::new(Self::new(circuit.clone())))
            .clone()
    }

    fn new(circuit: C) -> Self {
        Self {
            phantom: PhantomData,
        }
    }

    pub fn register_integral(&self, stream: NodeId, accumulator: NodeId, integral: NodeId) {
        todo!()
    }

    pub fn register_join(&self, left: NodeId, right: NodeId, join: NodeId) {
        todo!()
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
                    let balancer = Balancer::get_balancer(circuit);

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

impl<C, B> Stream<C, Spine<B>>
where
    C: Circuit,
    B: Batch,
{
    /// Returns the trace of `self` delayed by one clock cycle.
    pub fn accumulate_delay_trace_with_balancer(&self) -> Stream<C, SpineSnapshot<B>> {
        todo!()
    }
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

    flushed: bool,

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
