use std::{borrow::Cow, panic::Location};

use size_of::SizeOf;

use crate::{
    circuit::{
        circuit_builder::StreamId,
        metadata::{
            MetaItem, OperatorLocation, OperatorMeta, ALLOCATED_BYTES_LABEL, NUM_ENTRIES_LABEL,
            SHARED_BYTES_LABEL, USED_BYTES_LABEL,
        },
        operator_traits::{Operator, UnaryOperator},
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
    /// See [`Stream::accumulate`].
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
    factories: B::Factories,
    state: Spine<B>,
    flush: bool,
    location: &'static Location<'static>,
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

    async fn eval_owned(&mut self, batch: B) -> Option<Spine<B>> {
        self.state.insert(batch);
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
