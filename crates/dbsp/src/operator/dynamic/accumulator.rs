use std::{borrow::Cow, panic::Location};

use size_of::SizeOf;

use crate::{
    circuit::{
        circuit_builder::StreamId,
        metadata::{
            MetaItem, OperatorLocation, OperatorMeta, ALLOCATED_BYTES_LABEL, NUM_ENTRIES_LABEL,
            SHARED_BYTES_LABEL, USED_BYTES_LABEL,
        },
        metrics::Gauge,
        operator_traits::{Operator, UnaryOperator},
        GlobalNodeId,
    },
    circuit_cache_key,
    trace::{Batch, BatchReader, Spine, Trace},
    Circuit, Error, NumEntries, Scope, Stream,
};

circuit_cache_key!(AccumulatorId<C, B: Batch>(StreamId => Stream<C, Option<Spine<B>>>));

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    pub fn dyn_accumulate(&self, factories: &B::Factories) -> Stream<C, Option<Spine<B>>> {
        self.circuit()
            .cache_get_or_insert_with(AccumulatorId::new(self.stream_id()), || {
                let stream = self.circuit().add_unary_operator(
                    Accumulator::new(factories, Location::caller()),
                    &self.try_sharded_version(),
                );
                stream.mark_sharded_if(self);
                stream
            })
            .clone()
    }
}

pub struct Accumulator<B>
where
    B: Batch,
{
    global_id: GlobalNodeId,
    factories: B::Factories,
    state: Spine<B>,
    flush: bool,
    location: &'static Location<'static>,
    total_size_metric: Option<Gauge>,
    trace_metrics: Option<<Spine<B> as Trace>::Metrics>,
}

impl<B> Accumulator<B>
where
    B: Batch,
{
    pub fn new(factories: &B::Factories, location: &'static Location<'static>) -> Self {
        Self {
            global_id: GlobalNodeId::root(),
            factories: factories.clone(),
            state: Spine::new(factories),
            flush: false,
            location,
            total_size_metric: None,
            trace_metrics: None,
        }
    }
}

impl<B> Operator for Accumulator<B>
where
    B: Batch,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("Accumulator")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
        self.total_size_metric = Some(Gauge::new(
            "total_size",
            None,
            Some("count"),
            global_id,
            vec![],
        ));

        self.trace_metrics = Some(Spine::<B>::init_operator_metrics(global_id));
    }

    fn metrics(&self) {
        let total_size = self.state.num_entries_deep();

        self.total_size_metric
            .as_ref()
            .unwrap()
            .set(total_size as f64);
        self.state.metrics(self.trace_metrics.as_ref().unwrap());
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let total_size = self.state.num_entries_deep();

        let bytes = self.state.size_of();

        meta.extend(metadata! {
            NUM_ENTRIES_LABEL => MetaItem::Count(total_size),
            ALLOCATED_BYTES_LABEL => MetaItem::bytes(bytes.total_bytes()),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
        });

        self.state.metadata(meta);
    }

    fn clock_start(&mut self, _scope: Scope) {
        debug_assert!(self.state.is_empty());
    }

    fn clock_end(&mut self, _scope: Scope) {
        debug_assert!(self.state.is_empty());
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        self.state.is_empty()
    }

    /// Clear the operator's state.
    fn clear_state(&mut self) -> Result<(), Error> {
        self.state = Spine::new(&self.factories);
        self.flush = false;
        Ok(())
    }

    fn flush(&mut self) {
        self.flush = true;
    }

    fn is_flush_complete(&self) -> bool {
        !self.flush
    }
}

impl<B> UnaryOperator<B, Option<Spine<B>>> for Accumulator<B>
where
    B: Batch,
{
    async fn eval(&mut self, batch: &B) -> Option<Spine<B>> {
        self.state.insert(batch.clone());
        if self.flush {
            self.flush = false;
            let mut spine = Spine::<B>::new(&self.factories);
            std::mem::swap(&mut self.state, &mut spine);
            Some(spine)
        } else {
            None
        }
    }
}
