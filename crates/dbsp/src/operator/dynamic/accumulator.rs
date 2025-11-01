use std::{
    borrow::Cow,
    panic::Location,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use size_of::SizeOf;
use typedmap::TypedMapKey;

use crate::{
    circuit::{
        circuit_builder::StreamId,
        metadata::{
            BatchSizeStats, MetaItem, OperatorLocation, OperatorMeta, ALLOCATED_BYTES_LABEL,
            INPUT_BATCHES_LABEL, NUM_ENTRIES_LABEL, OUTPUT_BATCHES_LABEL, SHARED_BYTES_LABEL,
            USED_BYTES_LABEL,
        },
        operator_traits::{Operator, UnaryOperator},
        LocalStoreMarker,
    },
    circuit_cache_key,
    trace::{Batch, BatchReader, Spine, Trace},
    Circuit, Error, NumEntries, Runtime, Scope, Stream,
};

circuit_cache_key!(AccumulatorId<C, B: Batch>(StreamId => (Stream<C, Option<Spine<B>>>, Arc<AtomicUsize>)));

/// `TypedMapKey` entry used to share `enable_count` across instances of the same accumulator in multiple workers.
#[derive(Hash, PartialEq, Eq)]
struct EnableCountId {
    id: usize,
}

impl EnableCountId {
    fn new(id: usize) -> Self {
        Self { id }
    }
}

impl TypedMapKey<LocalStoreMarker> for EnableCountId {
    type Value = Arc<AtomicUsize>;
}

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    /// See [`Stream::accumulate`].
    pub fn dyn_accumulate(&self, factories: &B::Factories) -> Stream<C, Option<Spine<B>>> {
        let (stream, enable_count) = self.dyn_accumulate_with_enable_count(factories);
        enable_count.fetch_add(1, Ordering::AcqRel);

        stream
    }

    /// See [`Stream::accumulate_with_enable_count`].
    pub fn dyn_accumulate_with_enable_count(
        &self,
        factories: &B::Factories,
    ) -> (Stream<C, Option<Spine<B>>>, Arc<AtomicUsize>) {
        self.circuit()
            .cache_get_or_insert_with(AccumulatorId::new(self.stream_id()), || {
                let accumulator = Accumulator::new(factories, Location::caller());
                let enable_count = accumulator.enable_count.clone();

                let stream = self
                    .circuit()
                    .add_unary_operator(accumulator, &self.try_sharded_version());
                stream.mark_sharded_if(self);
                (stream, enable_count)
            })
            .clone()
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

    /// Used to enable/disable the accumulator during a transaction.
    ///
    /// Most accumulators (created with dyn_accumulate()) are always enabled.
    /// One special case is when the accumulator is used as part of an output handle
    /// to collect updates to the output stream within a transaction. In this case,
    /// if there is no output connector attached to the stream, there is no need to
    /// store the updates (which during backfill can amount to storing a complete copy
    /// of the table or view).
    ///
    /// This flag enables this optimization by keeping track of the number of consumers
    /// of the accumulator's output. It is equal to the number of attached output connectors
    /// plus the number of times the same accumulator was instantiated as part of a regular
    /// (non-output) operator with dyn_accumulate().
    enable_count: Arc<AtomicUsize>,

    /// Whether the accumulator is enabled during the current transaction.
    ///
    /// An output connector can be attached in the middle of a transaction; however if the
    /// accumulator was disabled at the start of the transaction, it shouldn't produce
    /// partial outputs. This flag remembers the status of the accumulator at the start of the
    /// transaction.
    enabled_during_current_transaction: Option<bool>,
}

impl<B> Accumulator<B>
where
    B: Batch,
{
    pub fn new(factories: &B::Factories, location: &'static Location<'static>) -> Self {
        let enable_count = match Runtime::runtime() {
            None => Arc::new(AtomicUsize::new(0)),
            Some(runtime) => {
                let accumulator_id = runtime.sequence_next();
                runtime
                    .local_store()
                    .entry(EnableCountId::new(accumulator_id))
                    .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
                    .value()
                    .clone()
            }
        };

        Self {
            factories: factories.clone(),
            state: Spine::new(factories),
            flush: false,
            location,
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: BatchSizeStats::new(),
            enable_count,
            enabled_during_current_transaction: None,
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
            NUM_ENTRIES_LABEL => MetaItem::Count(total_size),
            ALLOCATED_BYTES_LABEL => MetaItem::bytes(bytes.total_bytes()),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            "allocations" => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
            INPUT_BATCHES_LABEL => self.input_batch_stats.metadata(),
            OUTPUT_BATCHES_LABEL => self.output_batch_stats.metadata(),

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
        // We don't have a start-of-transaction signal, so we sample enable_count when
        // we get the first non-empty batch.  This batch should belong to the next transaction
        // after the last one that was flushed, since the accumulator should not receive any
        // non-empty batches from the previous transaction at that point (in the top-level circuit).
        // This may not be the first batch in the transaction, but it's ok to admit some empty batches.
        let len = batch.len();

        if len > 0 {
            if self.enabled_during_current_transaction.is_none() {
                self.enabled_during_current_transaction =
                    Some(self.enable_count.load(Ordering::Acquire) > 0);
            }

            if self.enabled_during_current_transaction == Some(true) {
                self.input_batch_stats.add_batch(len);
                self.state.insert(batch.clone());
            }
        }

        if self.flush {
            self.flush = false;
            self.enabled_during_current_transaction = None;

            let mut spine = Spine::<B>::new(&self.factories);
            std::mem::swap(&mut self.state, &mut spine);

            self.output_batch_stats.add_batch(spine.len());
            Some(spine)
        } else {
            None
        }
    }

    async fn eval_owned(&mut self, batch: B) -> Option<Spine<B>> {
        let len = batch.len();

        if len > 0 {
            if self.enabled_during_current_transaction.is_none() {
                self.enabled_during_current_transaction =
                    Some(self.enable_count.load(Ordering::Acquire) > 0);
            }

            if self.enabled_during_current_transaction == Some(true) {
                self.input_batch_stats.add_batch(len);
                self.state.insert(batch);
            }
        }

        if self.flush {
            self.flush = false;
            self.enabled_during_current_transaction = None;

            let mut spine = Spine::<B>::new(&self.factories);
            std::mem::swap(&mut self.state, &mut spine);

            self.output_batch_stats.add_batch(spine.len());
            Some(spine)
        } else {
            None
        }
    }
}
