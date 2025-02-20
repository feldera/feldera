use std::{mem::transmute, sync::Arc};

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, Weight, WeightTrait},
    time::Timestamp,
    trace::{
        cursor::CursorList, ord::filter, Batch, BatchFactories, BatchReaderFactories, Bounds,
        Builder, Cursor, Filter,
    },
};

/// Builder for `ListMerger` instances.
pub struct ListMergerBuilder<B>
where
    B: Batch,
{
    batches: Vec<Arc<B>>,
    cursors: Vec<B::Cursor<'static>>,
}

impl<B> ListMergerBuilder<B>
where
    B: Batch,
{
    /// Create builder for `capacity` batches.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            batches: Vec::with_capacity(capacity),
            cursors: Vec::with_capacity(capacity),
        }
    }

    /// Add a batch to the merger.
    pub fn with_batch(mut self, batch: Arc<B>) -> Self {
        self.cursors
            .push(unsafe { transmute::<B::Cursor<'_>, B::Cursor<'static>>(batch.cursor()) });
        self.batches.push(batch);
        self
    }

    /// Add an iterator of batches
    pub fn with_batches(mut self, batches: impl IntoIterator<Item = Arc<B>>) -> Self {
        for batch in batches {
            self = self.with_batch(batch);
        }
        self
    }

    /// Create merger
    ///
    /// # Panics
    ///
    /// Panics if the number of added batches is < 2.
    pub fn build(self) -> ListMerger<B> {
        assert!(self.batches.len() >= 2);
        // TODO
        let factories = self.batches[0].factories();
        let builder = B::Builder::for_merge(&factories, &self.batches);

        ListMerger {
            batches: self.batches,
            cursor: CursorList::new(factories.weight_factory(), self.cursors),
            builder,
            tmp_weight: factories.weight_factory().default_box(),
            time_diffs: factories.time_diffs_factory().map(|f| f.default_box()),
        }
    }
}

/// Merger that merges any number (>=2) of batches.
pub struct ListMerger<B>
where
    B: Batch,
{
    batches: Vec<Arc<B>>,
    cursor: CursorList<B::Key, B::Val, B::Time, B::R, B::Cursor<'static>>,
    builder: B::Builder,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<B> ListMerger<B>
where
    B: Batch,
{
    /// Perform `fuel` amount of work.
    ///
    /// When the function returns and fuel > 0, the batches should be guaranteed to be fully merged.
    pub fn work(
        &mut self,
        key_filter: &Option<Filter<B::Key>>,
        value_filter: &Option<Filter<B::Val>>,
        frontier: &B::Time,
        fuel: &mut isize,
    ) {
        let advance_func = |t: &mut DynDataTyped<B::Time>| t.join_assign(frontier);

        let time_map_func = if frontier == &B::Time::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<B::Time>))
        };

        while self.cursor.key_valid() && *fuel > 0 {
            self.copy_values_if(key_filter, value_filter, time_map_func, fuel);
        }
    }

    fn copy_values_if(
        &mut self,
        key_filter: &Option<Filter<B::Key>>,
        value_filter: &Option<Filter<B::Val>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        fuel: &mut isize,
    ) {
        if filter(key_filter, self.cursor.key()) {
            let mut any = false;
            while self.cursor.val_valid() {
                any = self.copy_time_diffs_if(value_filter, map_func, fuel) || any;
            }
            if any {
                self.builder.push_key(self.cursor.key());
            }
        }
        *fuel -= 1;
        self.cursor.step_key();
    }

    fn copy_time_diffs_if(
        &mut self,
        value_filter: &Option<Filter<B::Val>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        fuel: &mut isize,
    ) -> bool {
        let mut any = false;
        if filter(value_filter, self.cursor.val()) {
            // If this is a timed batch, we must consolidate the (time, weight) array; otherwise we
            // simply compute the total weight of the current value.
            if let Some(time_diffs) = &mut self.time_diffs {
                time_diffs.clear();

                if let Some(map_func) = map_func {
                    self.cursor.map_times(&mut |time, w| {
                        let mut time: B::Time = time.clone();
                        map_func(&mut time);

                        time_diffs.push_refs((&time, w));
                    });
                } else {
                    self.cursor.map_times(&mut |time, w| {
                        time_diffs.push_refs((time, w));
                    });
                }

                time_diffs.consolidate();

                for i in 0..time_diffs.len() {
                    let (time, diff) = time_diffs.index(i).split();
                    self.builder.push_time_diff(time, diff);
                }
                any = !time_diffs.is_empty();
            } else {
                self.tmp_weight.set_zero();
                self.cursor
                    .map_times(&mut |_time, w| self.tmp_weight.add_assign(w));
                if !self.tmp_weight.is_zero() {
                    self.builder
                        .push_time_diff(&B::Time::default(), &self.tmp_weight);
                    any = true;
                }
            }
            if any {
                self.builder.push_val(self.cursor.val());
            }
        }
        *fuel -= 1;
        self.cursor.step_val();
        any
    }

    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    pub fn done(self) -> B {
        let bounds = Bounds::for_merge(self.batches.iter().map(|b| &**b));
        self.builder.done_with_bounds(bounds)
    }
}
