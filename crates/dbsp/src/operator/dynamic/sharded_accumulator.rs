use std::{borrow::Cow, panic::Location};

use size_of::SizeOf;

use crate::{
    Circuit, Error, NumEntries, Runtime, Scope, Stream,
    circuit::{
        circuit_builder::StreamId,
        metadata::{
            ALLOCATED_MEMORY_BYTES, BatchSizeStats, INPUT_BATCHES_STATS, MEMORY_ALLOCATIONS_COUNT,
            MetaItem, OUTPUT_BATCHES_STATS, OperatorLocation, OperatorMeta, SHARED_MEMORY_BYTES,
            STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        operator_traits::{Operator, UnaryOperator},
    },
    circuit_cache_key,
    operator::communication::ExchangeKind,
    trace::{Batch, BatchReader, Spine, Trace},
};

circuit_cache_key!(ShardedAccumulatorId<C, B: Batch>(StreamId => Stream<C, Option<Spine<B>>>));

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    /// Implements a fused shard-accumulator operation, equivalent to
    /// `self.dyn_shard().dyn_accumulate()` but intended to be more efficient.
    pub fn dyn_shard_accumulate(&self, factories: &B::Factories) -> Stream<C, Option<Spine<B>>>
    where
        B: Batch<Time = ()>,
    {
        if let Some(sharded) = self.get_sharded_version() {
            sharded.dyn_accumulate(factories)
        } else if Runtime::num_workers() == 1 {
            self.dyn_accumulate(factories)
        } else {
            self.circuit()
                .cache_get_or_insert_with(ShardedAccumulatorId::new(self.stream_id()), || {
                    let sharded = self
                        .dyn_shard_bare(
                            0..Runtime::num_workers(),
                            factories.clone(),
                            ExchangeKind::Stream,
                        )
                        .unwrap();
                    let accumulator = Accumulator::<B>::new(factories, Location::caller());
                    let stream = self.circuit().add_unary_operator(accumulator, &sharded);
                    stream.mark_sharded();
                    stream
                })
                .clone()
        }
    }
}

pub struct Accumulator<B>
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
}

impl<B> Accumulator<B>
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

    fn metadata(&self, meta: &mut OperatorMeta) {
        let total_size = self.state.num_entries_deep();

        let bytes = self.state.size_of();

        meta.extend(metadata! {
            STATE_RECORDS_COUNT => MetaItem::Count(total_size),
            ALLOCATED_MEMORY_BYTES => MetaItem::bytes(bytes.total_bytes()),
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
            OUTPUT_BATCHES_STATS => self.output_batch_stats.metadata(),

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

impl<B> UnaryOperator<Vec<B>, Option<Spine<B>>> for Accumulator<B>
where
    B: Batch,
{
    async fn eval(&mut self, batches: &Vec<B>) -> Option<Spine<B>> {
        self.eval_owned(batches.clone()).await
    }

    async fn eval_owned(&mut self, batches: Vec<B>) -> Option<Spine<B>> {
        for batch in batches {
            let len = batch.len();

            if len > 0 {
                self.input_batch_stats.add_batch(len);
                self.state.insert(batch);
            }
        }

        if self.flush {
            self.flush = false;

            let mut spine = Spine::<B>::new(&self.factories);
            std::mem::swap(&mut self.state, &mut spine);

            self.output_batch_stats.add_batch(spine.len());
            Some(spine)
        } else {
            None
        }
    }
}
