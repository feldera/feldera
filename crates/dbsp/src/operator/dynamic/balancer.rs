use std::{
    borrow::Cow,
    sync::{atomic::AtomicU64, Arc},
};

use crate::{
    circuit::{
        metadata::{BatchSizeStats, OperatorLocation, OperatorMeta, INPUT_BATCHES_LABEL},
        operator_traits::{Operator, TernarySinkOperator},
        OwnershipPreference,
    },
    operator::communication::Exchange,
    trace::{Batch, Spine},
    Circuit, Runtime, Scope, Stream,
};

enum Policy {
    Shard,
    Broadcast,
}

#[derive(Default, Clone)]
struct WorkerStats {
    size: usize,
}

struct Balancer {
    stats: Vec<Vec<WorkerStats>>,
}

impl Balancer {
    fn new(num_streams: usize, num_workers: usize) -> Self {
        Self {
            stats: vec![vec![WorkerStats::default(); num_workers]; num_streams],
        }
    }

    fn update_worker_stats(
        &mut self,
        stream_index: usize,
        worker_index: usize,
        stats: WorkerStats,
    ) {
        self.stats[stream_index][worker_index] = stats;
    }

    fn get_policy(&self, stream_index: usize, current_policy: &Policy) -> Policy {
        todo!()
    }
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    fn dyn_accumulate_trace_with_balancer(
        &self,
        balancer: &Balancer,
        stream_index: usize,
    ) -> Stream<C, B> {

        // Exchange sender

        // Extra stream for the accumulator

        // Accumulator

        // Connect the stream (and register it with the circuit)

        // Integral with feedback connector

        // Connect the loop.
    }
}

pub struct RebalancingExchangeSender<B>
where
    B: Batch,
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
}

impl<B> RebalancingExchangeSender<B>
where
    B: Batch,
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
        }
    }
}

impl<B> Operator for RebalancingExchangeSender<B>
where
    B: Batch,
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
        self.exchange.ready_to_send(self.worker_index)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        self.flushed = true;
    }
}

impl<B> TernarySinkOperator<B, Spine<B>, Spine<B>> for RebalancingExchangeSender<B>
where
    B: Batch,
{
    async fn eval(
        &mut self,
        delta: Cow<'_, B>,
        delayed_accumulator: Cow<'_, Spine<B>>,
        delayed_trace: Cow<'_, Spine<B>>,
    ) {
        // stats:
        //   input batch size
        //   number of rebalancings

        // Reevaluate the policy.

        // Policy change?
        // - Clean old rebalancing state
        // - Set new rebalancing state
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
