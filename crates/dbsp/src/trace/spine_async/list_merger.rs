use std::{mem::transmute, sync::Arc};

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, Weight, WeightTrait},
    time::Timestamp,
    trace::{
        cursor::CursorList, ord::filter, Batch, BatchFactories, BatchReaderFactories, Cursor,
        Filter, TimedBuilder,
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
        let builder = B::Builder::timed_for_merge(&factories, &self.batches);

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
            while self.cursor.val_valid() {
                self.copy_time_diffs_if(value_filter, map_func, fuel);
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
    ) {
        if filter(value_filter, self.cursor.val()) {
            // If this is a timed batch, we must consolidate the (time, weight) array; otherwise we
            // simply compute the total weight of the current value.
            if let Some(time_diffs) = &mut self.time_diffs {
                time_diffs.clear();

                if let Some(map_func) = map_func {
                    self.cursor.map_times(&mut |time, w| {
                        *fuel -= 1;

                        let mut time: B::Time = time.clone();
                        map_func(&mut time);

                        time_diffs.push_refs((&time, w));
                    });
                } else {
                    self.cursor.map_times(&mut |time, w| {
                        *fuel -= 1;

                        time_diffs.push_refs((time, w));
                    });
                }

                time_diffs.consolidate();

                for i in 0..time_diffs.len() {
                    let (time, diff) = time_diffs.index(i).split();

                    self.builder
                        .push_time(self.cursor.key(), self.cursor.val(), time, diff);
                }
            } else {
                self.tmp_weight.set_zero();
                self.cursor.map_times(&mut |_time, w| {
                    *fuel -= 1;
                    self.tmp_weight.add_assign(w)
                });
                if !self.tmp_weight.is_zero() {
                    self.builder.push_time(
                        self.cursor.key(),
                        self.cursor.val(),
                        &B::Time::default(),
                        &self.tmp_weight,
                    );
                }
            }
        }
        *fuel -= 1;
        self.cursor.step_val();
    }

    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and
    /// has not brought `fuel` to zero. Otherwise, the merge is still in
    /// progress.
    pub fn done(self) -> B {
        let lower = self.batches[1..]
            .iter()
            .fold(self.batches[0].lower().to_owned(), |lower, batch| {
                batch.lower().meet(lower.as_ref())
            });
        let upper = self.batches[1..]
            .iter()
            .fold(self.batches[0].upper().to_owned(), |upper, batch| {
                batch.upper().join(upper.as_ref())
            });

        self.builder.done_with_bounds(lower, upper)
    }
}
