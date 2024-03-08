//! A general purpose `Batcher` implementation based on radix sort.

use crate::{
    dynamic::{pair::DynPair, DynWeightedPairs},
    trace::{Batch, BatchFactories, Batcher, Builder},
};
use size_of::SizeOf;
use std::marker::PhantomData;

mod merge_sorter;
mod tests;
pub use merge_sorter::MergeSorter;

/// Creates batches from unordered tuples.
#[derive(SizeOf)]
pub struct MergeBatcher<B: Batch> {
    #[size_of(skip)]
    batch_factories: B::Factories,
    sorter: MergeSorter<DynPair<B::Key, B::Val>, B::R>,
    time: B::Time,
    phantom: PhantomData<B>,
}

impl<B> Batcher<B> for MergeBatcher<B>
where
    Self: SizeOf,
    B: Batch,
{
    fn new_batcher(batch_factories: &B::Factories, time: B::Time) -> Self {
        Self {
            batch_factories: batch_factories.clone(),
            sorter: MergeSorter::new(
                batch_factories.weighted_item_factory(),
                batch_factories.weighted_items_factory(),
            ),
            time,
            phantom: PhantomData,
        }
    }

    fn push_batch(&mut self, batch: &mut Box<DynWeightedPairs<DynPair<B::Key, B::Val>, B::R>>) {
        self.sorter.push_batch(batch);
    }

    fn push_consolidated_batch(
        &mut self,
        batch: &mut Box<DynWeightedPairs<DynPair<B::Key, B::Val>, B::R>>,
    ) {
        self.sorter.push_consolidated_batch(batch);
    }

    fn tuples(&self) -> usize {
        self.sorter.len()
    }

    // Sealing a batch means finding those updates with times not greater or equal
    // to any time in `upper`. All updates must have time greater or equal to
    // the previously used `upper`, which we call `lower`, by assumption that
    // after sealing a batcher we receive no more updates with times not greater
    // or equal to `upper`.
    // TODO: Since sealing takes self by value all of the buffers we've collected
    //       are just discarded, which isn't ideal
    // TODO: Should we just merge batches until completion instead of having
    //       the inner builder do it?
    fn seal(mut self) -> B {
        let mut merged = Vec::new();
        self.sorter.finish_into(&mut merged);

        // Try and pre-allocate our builder a little bit
        let mut builder = B::Builder::with_capacity(
            &self.batch_factories,
            self.time.clone(),
            merged.iter().map(|batch| batch.len()).sum(),
        );

        for mut buffer in merged.drain(..) {
            // Safety: buffer.into_iter() passes ownership to valid items of the correct
            // type to the caller.
            builder.extend(buffer.dyn_iter_mut());
        }

        builder.done()
    }
}
