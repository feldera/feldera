use std::{cmp::Ordering, sync::Arc};

use ouroboros::self_referencing;

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, Weight, WeightTrait},
    time::Timestamp,
    trace::{
        spine_async::index_set::IndexSet, Batch, BatchFactories, BatchReaderFactories, Builder,
        Filter, MergeCursor,
    },
};

pub struct ArcListMerger<B>(ArcMergerInner<B>)
where
    B: Batch;

#[self_referencing]
struct ArcMergerInner<B>
where
    B: Batch,
{
    batches: Vec<Arc<B>>,
    builder: B::Builder,
    #[borrows(batches)]
    #[not_covariant]
    merger: ListMerger<Box<dyn MergeCursor<B::Key, B::Val, B::Time, B::R> + Send + 'this>, B>,
}

impl<B> ArcListMerger<B>
where
    B: Batch,
{
    pub fn new(
        factories: &B::Factories,
        builder: B::Builder,
        batches: Vec<Arc<B>>,
        key_filter: &Option<Filter<B::Key>>,
        value_filter: &Option<Filter<B::Val>>,
    ) -> Self {
        Self(
            ArcMergerInnerBuilder {
                batches,
                builder,
                merger_builder: |batches| {
                    ListMerger::new(
                        factories,
                        batches
                            .iter()
                            .map(|b| b.merge_cursor(key_filter.clone(), value_filter.clone()))
                            .collect(),
                    )
                },
            }
            .build(),
        )
    }

    pub fn work(&mut self, frontier: &B::Time, fuel: &mut isize) {
        self.0
            .with_mut(|fields| fields.merger.work(fields.builder, frontier, fuel))
    }

    pub fn done(self) -> B {
        self.0.into_heads().builder.done()
    }
}

/// Merger that merges up to 64 batches at a time.
pub struct ListMerger<C, B>
where
    C: MergeCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    cursors: Vec<C>,
    any_values: bool,
    has_mut: Vec<bool>,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<C, B> ListMerger<C, B>
where
    C: MergeCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    pub fn merge(factories: &B::Factories, mut builder: B::Builder, cursors: Vec<C>) -> B {
        let mut merger = Self::new(factories, cursors);
        let mut fuel = isize::MAX;
        merger.work(&mut builder, &B::Time::default(), &mut fuel);
        assert!(fuel > 0);
        builder.done()
    }

    /// Creates a new merger for `cursors`.
    pub fn new(factories: &B::Factories, cursors: Vec<C>) -> Self {
        // [IndexSet] supports a maximum of 64 batches.
        assert!(cursors.len() <= 64);

        let time_diffs = factories.time_diffs_factory().map(|f| f.default_box());
        let has_mut = cursors.iter().map(|c| c.has_mut()).collect();

        ListMerger {
            cursors,
            any_values: false,
            has_mut,
            tmp_weight: factories.weight_factory().default_box(),
            time_diffs,
        }
    }

    /// Perform `fuel` amount of work.
    ///
    /// When the function returns and fuel > 0, the batches should be guaranteed to be fully merged.
    pub fn work(&mut self, builder: &mut B::Builder, frontier: &B::Time, fuel: &mut isize) {
        assert!(self.cursors.len() <= 64);
        let mut remaining_cursors = self
            .cursors
            .iter()
            .enumerate()
            .filter_map(|(index, cursor)| cursor.key_valid().then_some(index))
            .collect::<IndexSet>();
        if remaining_cursors.is_empty() {
            return;
        }

        let advance_func = |t: &mut DynDataTyped<B::Time>| t.join_assign(frontier);

        let time_map_func = if frontier == &B::Time::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<B::Time>))
        };

        // As long as there are multiple cursors...
        while remaining_cursors.is_long() && *fuel > 0 {
            // Find the indexes of the cursors with minimum keys, among the
            // remaining cursors.
            let orig_min_keys = find_min_indexes(
                remaining_cursors
                    .into_iter()
                    .map(|index| (index, self.cursors[index].key())),
            );

            // If we're resuming after stopping due to running out of fuel in a
            // previous call, then we might have exhausted the values in some of
            // the keys, so drop them.  We still need them in `orig_min_keys` so
            // we can advance the key for all of them later.
            let mut min_keys = if self.any_values {
                orig_min_keys
                    .into_iter()
                    .filter(|index| self.cursors[*index].val_valid())
                    .collect::<IndexSet>()
            } else {
                orig_min_keys
            };

            // As long as there is more than one cursor with minimum keys...
            while min_keys.is_long() {
                // ...Find the indexes of the cursors with minimum values, among
                // those with minimum keys, and copy their time-diff pairs and
                // value into the output.
                let min_vals = find_min_indexes(
                    min_keys
                        .into_iter()
                        .map(|index| (index, self.cursors[index].val())),
                );
                self.any_values =
                    self.copy_times(builder, time_map_func, min_vals, fuel) || self.any_values;

                // Then go on to the next value in each cursor, dropping the keys
                // for which we've exhausted the values.
                for index in min_vals {
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        min_keys.remove(index);
                    }
                }
                if *fuel <= 0 {
                    return;
                }
            }

            // If there's exactly one cursor left with minimum key, copy its
            // values into the output.
            if let Some(index) = min_keys.first() {
                loop {
                    self.any_values =
                        self.copy_times(builder, time_map_func, min_keys, fuel) || self.any_values;
                    self.cursors[index].step_val();
                    if *fuel <= 0 {
                        return;
                    }
                    if !self.cursors[index].val_valid() {
                        break;
                    }
                }
            }

            // If we wrote any values for these minimum keys, write the key.
            if self.any_values {
                let index = orig_min_keys.first().unwrap();
                if self.has_mut[index] {
                    builder.push_key_mut(self.cursors[index].key_mut());
                } else {
                    builder.push_key(self.cursors[index].key());
                }
                self.any_values = false;
            }

            // Advance each minimum-key cursor, dropping the cursors for which
            // we've exhausted the data.
            for index in orig_min_keys {
                self.cursors[index].step_key();
                if !self.cursors[index].key_valid() {
                    remaining_cursors.remove(index);
                }
            }
        }

        // If there is a cursor left (there's either one or none), copy it
        // directly to the output.
        if let Some(index) = remaining_cursors.first() {
            while *fuel > 0 {
                loop {
                    self.any_values =
                        self.copy_times(builder, time_map_func, remaining_cursors, fuel)
                            || self.any_values;
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        break;
                    }
                    if *fuel <= 0 {
                        return;
                    }
                }
                debug_assert!(time_map_func.is_some() || self.any_values, "This assertion should fail only if B::Cursor is a spine or a CursorList, but we shouldn't be merging those");
                if self.any_values {
                    self.any_values = false;
                    if self.has_mut[index] {
                        builder.push_key_mut(self.cursors[index].key_mut());
                    } else {
                        builder.push_key(self.cursors[index].key());
                    }
                }
                self.cursors[index].step_key();
                if !self.cursors[index].key_valid() {
                    break;
                }
            }
        }
    }

    fn copy_times(
        &mut self,
        builder: &mut B::Builder,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        indexes: IndexSet,
        fuel: &mut isize,
    ) -> bool {
        // If this is a timed batch, we must consolidate the (time, weight) array; otherwise we
        // simply compute the total weight of the current value.
        if let Some(time_diffs) = &mut self.time_diffs {
            if let Some(map_func) = map_func {
                time_diffs.clear();
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        let mut time: B::Time = time.clone();
                        map_func(&mut time);

                        time_diffs.push_refs((&time, w));
                    });
                }
                time_diffs.consolidate();
                if time_diffs.is_empty() {
                    return false;
                }
                for (time, diff) in time_diffs.dyn_iter().map(|td| td.split()) {
                    builder.push_time_diff(time, diff);
                }
            } else if indexes.is_long() {
                time_diffs.clear();
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        time_diffs.push_refs((time, w));
                    });
                }
                time_diffs.consolidate();
                if time_diffs.is_empty() {
                    return false;
                }
                for (time, diff) in time_diffs.dyn_iter().map(|td| td.split()) {
                    builder.push_time_diff(time, diff);
                }
            } else {
                debug_assert_eq!(indexes.len(), 1);
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        builder.push_time_diff(time, w);
                    });
                }
            }
        } else {
            self.tmp_weight.set_zero();
            for i in indexes {
                self.cursors[i].map_times(&mut |_time, weight| {
                    self.tmp_weight.add_assign(weight);
                });
            }
            if self.tmp_weight.is_zero() {
                return false;
            }
            builder.push_time_diff_mut(&mut B::Time::default(), &mut self.tmp_weight);
        }

        let index = indexes.first().unwrap();
        if self.has_mut[index] {
            builder.push_val_mut(self.cursors[index].val_mut());
        } else {
            builder.push_val(self.cursors[index].val());
        }
        *fuel -= 1;
        true
    }
}

fn find_min_indexes<Item>(mut iterator: impl Iterator<Item = (usize, Item)>) -> IndexSet
where
    Item: Ord,
{
    let (min_index, mut min_value) = iterator.next().unwrap();
    let mut min_indexes = IndexSet::for_index(min_index);

    for (index, value) in iterator {
        match value.cmp(&min_value) {
            Ordering::Less => {
                min_value = value;
                min_indexes = IndexSet::for_index(index);
            }
            Ordering::Equal => {
                min_indexes.add(index);
            }
            Ordering::Greater => (),
        }
    }
    min_indexes
}
