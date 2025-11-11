use std::{borrow::Cow, cell::RefCell, panic::Location, rc::Rc, sync::Arc};

use size_of::SizeOf;

use crate::{
    Circuit, Error, NumEntries, Scope, Stream,
    circuit::{
        checkpointer::EmptyCheckpoint,
        circuit_builder::RefStreamValue,
        metadata::{
            ALLOCATED_BYTES_LABEL, BatchSizeStats, INPUT_BATCHES_LABEL, MetaItem,
            NUM_ALLOCATIONS_LABEL, NUM_ENTRIES_LABEL, OUTPUT_BATCHES_LABEL, OperatorLocation,
            OperatorMeta, SHARED_BYTES_LABEL, USED_BYTES_LABEL,
        },
        operator_traits::{Operator, UnaryOperator},
    },
    trace::{Batch, BatchReader, Spine, Trace},
};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    pub fn dyn_accumulate_with_feedback_stream(
        &self,
        factories: &B::Factories,
    ) -> (
        Stream<C, Option<Spine<B>>>,
        Rc<RefCell<RebalancingAccumulatorInner<B>>>,
        RefStreamValue<EmptyCheckpoint<Vec<Arc<B>>>>,
    ) {
        let accumulator = RebalancingAccumulator::<B>::new(factories, Location::caller());
        let inner = accumulator.0.clone();
        let accumulator_snapshot_stream_val = RefStreamValue::empty();
        accumulator
            .0
            .borrow_mut()
            .set_feedback_stream(accumulator_snapshot_stream_val.clone());

        let stream = self
            .circuit()
            .add_unary_operator(accumulator, &self.try_sharded_version());
        stream.mark_sharded_if(self);
        (stream, inner, accumulator_snapshot_stream_val)
    }
}

pub struct RebalancingAccumulatorInner<B>
where
    B: Batch,
{
    factories: B::Factories,
    state: Spine<B>,
    flush: bool,
    location: &'static Location<'static>,

    // Input batch sizes.
    input_batch_stats: BatchSizeStats,

    // Output batch sizes.
    output_batch_stats: BatchSizeStats,

    feedback_stream: Option<RefStreamValue<EmptyCheckpoint<Vec<Arc<B>>>>>,
}

impl<B> RebalancingAccumulatorInner<B>
where
    B: Batch,
{
    pub fn new(factories: &B::Factories, location: &'static Location<'static>) -> Self {
        Self {
            factories: factories.clone(),
            state: Spine::new(factories),
            flush: false,
            location,
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: BatchSizeStats::new(),
            feedback_stream: None,
        }
    }

    pub fn clear_state(&mut self) {
        self.state = Spine::new(&self.factories);
        self.flush = false;
    }

    pub fn set_feedback_stream(
        &mut self,
        feedback_stream: RefStreamValue<EmptyCheckpoint<Vec<Arc<B>>>>,
    ) {
        self.feedback_stream = Some(feedback_stream.clone());
    }
}

pub struct RebalancingAccumulator<B: Batch>(Rc<RefCell<RebalancingAccumulatorInner<B>>>);

impl<B> RebalancingAccumulator<B>
where
    B: Batch,
{
    pub fn new(factories: &B::Factories, location: &'static Location<'static>) -> Self {
        Self(Rc::new(RefCell::new(RebalancingAccumulatorInner::new(
            factories, location,
        ))))
    }
}

impl<B> Operator for RebalancingAccumulator<B>
where
    B: Batch,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("BalancingAccumulator")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.0.borrow().location)
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let total_size = self.0.borrow().state.num_entries_deep();

        let bytes = self.0.borrow().state.size_of();

        meta.extend(metadata! {
            NUM_ENTRIES_LABEL => MetaItem::Count(total_size),
            ALLOCATED_BYTES_LABEL => MetaItem::bytes(bytes.total_bytes()),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            NUM_ALLOCATIONS_LABEL => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
            INPUT_BATCHES_LABEL => self.0.borrow().input_batch_stats.metadata(),
            OUTPUT_BATCHES_LABEL => self.0.borrow().output_batch_stats.metadata(),

        });

        self.0.borrow().state.metadata(meta);
    }

    fn clock_start(&mut self, _scope: Scope) {
        debug_assert!(self.0.borrow().state.is_empty());
    }

    fn clock_end(&mut self, _scope: Scope) {
        debug_assert!(self.0.borrow().state.is_empty());
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        self.0.borrow().state.is_empty()
    }

    /// Clear the operator's state.
    fn clear_state(&mut self) -> Result<(), Error> {
        let state = Spine::new(&self.0.borrow().factories);
        self.0.borrow_mut().state = state;
        self.0.borrow_mut().flush = false;
        Ok(())
    }

    fn flush(&mut self) {
        self.0.borrow_mut().flush = true;
    }

    fn is_flush_complete(&self) -> bool {
        !self.0.borrow().flush
    }
}

impl<B> UnaryOperator<B, Option<Spine<B>>> for RebalancingAccumulator<B>
where
    B: Batch,
{
    async fn eval(&mut self, batch: &B) -> Option<Spine<B>> {
        let mut inner = self.0.borrow_mut();

        let len = batch.len();

        if len > 0 {
            inner.input_batch_stats.add_batch(len);
            inner.state.insert(batch.clone());
        }

        let result = if inner.flush {
            inner.flush = false;

            let mut spine = Spine::<B>::new(&inner.factories);
            std::mem::swap(&mut inner.state, &mut spine);

            inner.output_batch_stats.add_batch(spine.len());
            Some(spine)
        } else {
            None
        };

        // Write the current stat _after_ the flush, since the stream must reflect the
        // state of the accumulator at the end of the step.
        if let Some(feedback_stream) = &inner.feedback_stream {
            feedback_stream.put(EmptyCheckpoint::new(inner.state.get_batches()));
        }

        result
    }

    async fn eval_owned(&mut self, batch: B) -> Option<Spine<B>> {
        let mut inner = self.0.borrow_mut();

        let len = batch.len();

        if len > 0 {
            inner.input_batch_stats.add_batch(len);
            inner.state.insert(batch);
        }

        let result = if inner.flush {
            inner.flush = false;

            let mut spine = Spine::<B>::new(&inner.factories);
            std::mem::swap(&mut inner.state, &mut spine);

            inner.output_batch_stats.add_batch(spine.len());
            Some(spine)
        } else {
            None
        };

        if let Some(feedback_stream) = &inner.feedback_stream {
            feedback_stream.put(EmptyCheckpoint::new(inner.state.get_batches()));
        }

        result
    }
}
