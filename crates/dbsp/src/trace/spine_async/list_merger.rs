use std::{
    cmp::Ordering,
    ops::{BitOr, BitOrAssign},
    sync::Arc,
};

use ouroboros::self_referencing;
use ouroboros_impl_arc_merger_inner::Heads;

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, Weight, WeightTrait},
    time::Timestamp,
    trace::{
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Bounds, Builder, Filter,
        MergeCursor,
    },
};

pub struct ArcMerger<B>(ArcMergerInner<B>)
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

impl<B> ArcMerger<B>
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
        let Heads {
            batches, builder, ..
        } = self.0.into_heads();
        let bounds = Bounds::for_merge(batches.iter().map(|b| &**b));
        builder.done_with_bounds(bounds)
    }
}

/// Merger that merges up to 64 batches at a time.
pub struct ListMerger<C, B>
where
    C: MergeCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    cursors: Vec<C>,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<C, B> ListMerger<C, B>
where
    C: MergeCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    pub fn merge(
        factories: &B::Factories,
        mut builder: B::Builder,
        cursors: Vec<C>,
        bounds: Bounds<B::Time>,
    ) -> B {
        let mut merger = Self::new(factories, cursors);
        let mut fuel = isize::MAX;
        merger.work(&mut builder, &B::Time::default(), &mut fuel);
        assert!(fuel > 0);
        builder.done_with_bounds(bounds)
    }

    /// Creates a new merger for `batches`, using `key_filter` and
    /// `value_filter` to remove tuples.
    pub fn new(factories: &B::Factories, cursors: Vec<C>) -> Self {
        // [IndexSet] supports a maximum of 64 batches.
        assert!(cursors.len() <= 64);

        let time_diffs = factories.time_diffs_factory().map(|f| f.default_box());
        ListMerger {
            cursors,
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

        let has_mut = self.cursors[0].has_mut();

        // As long as there are multiple cursors...
        while remaining_cursors.is_long() && *fuel > 0 {
            // Find the indexes of the cursors with minimum keys, among the
            // remaining cursors.
            let orig_min_keys = find_min_indexes(
                remaining_cursors
                    .into_iter()
                    .map(|index| (index, self.cursors[index].key())),
            );

            // As long as there is more than one cursor with minimum keys...
            let mut any_values = false;
            let mut min_keys = orig_min_keys;
            while min_keys.is_long() {
                // ...Find the indexes of the cursors with minimum values, among
                // those with minimum keys, and copy their time-diff pairs and
                // value into the output.
                let min_vals = find_min_indexes(
                    min_keys
                        .into_iter()
                        .map(|index| (index, self.cursors[index].val())),
                );
                any_values =
                    self.copy_times(builder, time_map_func, min_vals, fuel, has_mut) || any_values;

                // Then go on to the next value in each cursor, dropping the keys
                // for which we've exhausted the values.
                for index in min_vals {
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        min_keys.remove(index);
                    }
                }
            }

            // If there's exactly one cursor left with minimum key, copy its
            // values into the output.
            if let Some(index) = min_keys.first() {
                loop {
                    any_values = self.copy_times(builder, time_map_func, min_keys, fuel, has_mut)
                        || any_values;
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        break;
                    }
                }
            }

            // If we wrote any values for these minimum keys, write the key.
            if any_values {
                if has_mut {
                    builder.push_key_mut(self.cursors[orig_min_keys.first().unwrap()].key_mut());
                } else {
                    builder.push_key(self.cursors[orig_min_keys.first().unwrap()].key());
                }
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
                let mut any_values = false;
                loop {
                    any_values =
                        self.copy_times(builder, time_map_func, remaining_cursors, fuel, has_mut)
                            || any_values;
                    self.cursors[index].step_val();
                    if !self.cursors[index].val_valid() {
                        break;
                    }
                }
                debug_assert!(any_values, "This assertion should fail only if B::Cursor is a spine or a CursorList, but we shouldn't be merging those");
                if has_mut {
                    builder.push_key_mut(self.cursors[index].key_mut());
                } else {
                    builder.push_key(self.cursors[index].key());
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
        has_mut: bool,
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

        if has_mut {
            builder.push_val_mut(self.cursors[indexes.first().unwrap()].val_mut());
        } else {
            builder.push_val(self.cursors[indexes.first().unwrap()].val());
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

/// A set of indexes (numbers) in the range 0..=63.
#[derive(Copy, Clone, PartialEq, Eq)]
struct IndexSet(u64);

impl IndexSet {
    /// Returns a bit-set with only `index` set to 1.
    fn for_index(index: usize) -> Self {
        debug_assert!(index < 64);
        Self(1 << index)
    }

    /// Returns an empty set.
    fn empty() -> Self {
        Self(0)
    }

    /// Returns true if the set is empty.
    fn is_empty(&self) -> bool {
        self.0 == 0
    }

    /// Returns true if the set contains exactly one index.
    #[allow(dead_code)]
    fn is_singleton(&self) -> bool {
        self.0.is_power_of_two()
    }

    /// Returns true if the set contains exactly zero or one indexes.
    fn is_short(&self) -> bool {
        self.without_first().is_empty()
    }

    /// Returns true if the set contains two or more indexes.
    fn is_long(&self) -> bool {
        !self.is_short()
    }

    /// Returns the number of indexes in the set.
    fn len(&self) -> usize {
        self.0.count_ones() as usize
    }

    /// Returns the smallest index in the set.
    fn first(&self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }

    // Returns the set with the first index removed. If this set is empty,
    // returns an empty set.
    fn without_first(&self) -> Self {
        Self(self.0 & (self.0.wrapping_sub(1)))
    }

    /// Adds `index` to the set.
    fn add(&mut self, index: usize) {
        self.0 |= 1 << index;
    }

    /// Removes `index` from the set.
    fn remove(&mut self, index: usize) {
        self.0 &= !(1 << index);
    }
}

impl FromIterator<usize> for IndexSet {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = usize>,
    {
        let mut set = Self::empty();
        for index in iter {
            set |= Self::for_index(index);
        }
        set
    }
}

impl BitOr for IndexSet {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for IndexSet {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

struct IndexSetIter(IndexSet);

impl Iterator for IndexSetIter {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.first().inspect(|_| self.0 = self.0.without_first())
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.0.len();
        (n, Some(n))
    }
}

impl IntoIterator for IndexSet {
    type Item = usize;
    type IntoIter = IndexSetIter;
    fn into_iter(self) -> Self::IntoIter {
        IndexSetIter(self)
    }
}
