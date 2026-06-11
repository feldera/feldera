//! Machinery for replaying the contents of a trace chunk-by-chunk.
//!
//! During bootstrapping, replay sources ([`Z1Trace`](super::trace::Z1Trace)
//! and `AccumulateZ1Trace`) stream the contents of their traces back into the
//! circuit one bounded chunk per step, so that a replay of arbitrary size
//! makes progress without unbounded per-step work.  This module holds the
//! replay position and the chunking logic shared by all replay sources.

use ouroboros::self_referencing;

use crate::{
    dynamic::{Weight, WeightTrait},
    trace::{Batch, BatchReaderFactories, Builder, MergeCursor, Trace},
};

/// A trace being replayed, together with a cursor tracking the replay
/// position.
#[self_referencing]
pub(crate) struct ReplayState<T: Trace> {
    trace: T,
    #[borrows(trace)]
    #[covariant]
    cursor: Box<dyn MergeCursor<T::Key, T::Val, T::Time, T::R> + Send + 'this>,
}

impl<T: Trace> ReplayState<T> {
    /// Takes ownership of `trace` and positions a merge cursor at its start.
    pub(crate) fn create(trace: T) -> Self {
        ReplayStateBuilder {
            trace,
            cursor_builder: |trace| trace.merge_cursor(None, None),
        }
        .build()
    }

    /// Returns `true` when the entire trace has been replayed.
    pub(crate) fn is_exhausted(&self) -> bool {
        !self.borrow_cursor().key_valid()
    }

    /// Builds the next replay chunk: a batch with at most `chunk_size`
    /// values, where each value's weights are consolidated across timestamps.
    /// Values whose weights sum to zero are skipped and do not count toward
    /// `chunk_size`.
    ///
    /// A chunk can end in the middle of a key; the next call continues with
    /// that key's remaining values.
    pub(crate) fn next_chunk<B>(&mut self, batch_factories: &B::Factories, chunk_size: usize) -> B
    where
        B: Batch<Key = T::Key, Val = T::Val, R = T::R, Time = ()>,
    {
        let mut builder =
            <B::Builder as Builder<B>>::with_capacity(batch_factories, chunk_size, chunk_size);

        let mut num_values = 0;
        let mut weight = batch_factories.weight_factory().default_box();

        while self.borrow_cursor().key_valid() && num_values < chunk_size {
            let mut values_added = false;
            while self.borrow_cursor().val_valid() && num_values < chunk_size {
                weight.set_zero();
                self.with_cursor_mut(|cursor| cursor.map_times(&mut |_t, w| weight.add_assign(w)));

                if !weight.is_zero() {
                    builder.push_val_diff(self.borrow_cursor().val(), weight.as_ref());
                    values_added = true;
                    num_values += 1;
                }
                self.with_cursor_mut(|cursor| cursor.step_val());
            }
            if values_added {
                builder.push_key(self.borrow_cursor().key());
            }
            if !self.borrow_cursor().val_valid() {
                self.with_cursor_mut(|cursor| cursor.step_key());
            }
        }

        builder.done()
    }
}
