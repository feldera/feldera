use itertools::Itertools;
use std::{cmp::Ordering, sync::Arc};

use ouroboros::self_referencing;

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, WeightTrait},
    time::Timestamp,
    trace::{
        cursor::{Pending, PushCursor},
        spine_async::index_set::IndexSet,
        Batch, BatchFactories, BatchReaderFactories, Builder, Filter, Weight,
    },
};

pub struct ArcPushMerger<B>(ArcPushMergerInner<B>)
where
    B: Batch;

#[self_referencing]
struct ArcPushMergerInner<B>
where
    B: Batch,
{
    batches: Vec<Arc<B>>,
    #[borrows(batches)]
    #[not_covariant]
    merger: PushMerger<Box<dyn PushCursor<B::Key, B::Val, B::Time, B::R> + Send + 'this>, B>,
}

impl<B> ArcPushMerger<B>
where
    B: Batch,
{
    pub fn new(
        factories: &B::Factories,
        batches: Vec<Arc<B>>,
        key_filter: &Option<Filter<B::Key>>,
        value_filter: &Option<Filter<B::Val>>,
    ) -> Self {
        Self(
            ArcPushMergerInnerBuilder {
                batches,
                merger_builder: |batches| {
                    PushMerger::new(
                        factories,
                        batches.iter().map(|b| b.push_cursor()).collect(),
                        key_filter.clone(),
                        value_filter.clone(),
                    )
                },
            }
            .build(),
        )
    }

    pub fn merge(&mut self, builder: &mut B::Builder, frontier: &B::Time) -> Result<(), Pending> {
        self.0
            .with_mut(|fields| fields.merger.merge(builder, frontier))
    }

    /// Gives all the cursors under this merger an opportunity to process I/O
    /// results and launch further I/O.  Should be called periodically.
    pub fn run(&mut self) {
        self.0.with_mut(|fields| fields.merger.run())
    }
}

/// Push-based merger.
///
/// Other mergers in DBSP do not take into account what data in their sources is
/// already in memory.  Instead, they read data from their sources blindly,
/// which results in blocking on I/O when they reach data that needs to be read
/// from disk.
///
/// `PushMerger` is different.  It takes [PushCursor]s, which distinguish data
/// that is currently readable from data that is pending I/O.  `PushMerger`
/// stops, instead of blocking, when it encounters data that is not yet
/// readable.  That is, the cursors "push" data into the merger, rather than the
/// merger pulling the data.  This allows the client to do other work while
/// further data is being fetched.
pub struct PushMerger<C, B>
where
    C: PushCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    cursors: Vec<C>,
    key_filter: Option<Filter<B::Key>>,
    value_filter: Option<Filter<B::Val>>,
    any_values: bool,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<C, B> PushMerger<C, B>
where
    C: PushCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    /// Creates a new merger for `cursors`.
    pub fn new(
        factories: &B::Factories,
        cursors: Vec<C>,
        key_filter: Option<Filter<B::Key>>,
        value_filter: Option<Filter<B::Val>>,
    ) -> Self {
        assert!(cursors.len() <= 64);
        Self {
            cursors,
            key_filter,
            value_filter,
            any_values: false,
            tmp_weight: factories.weight_factory().default_box(),
            time_diffs: factories.time_diffs_factory().map(|f| f.default_box()),
        }
    }

    /// Returns true if all merging is done.
    #[allow(dead_code)]
    pub fn is_done(&self) -> bool {
        self.cursors.iter().all(|cursor| cursor.key() == Ok(None))
    }

    /// Returns true if merging is not complete and there's enough data in
    /// memory to carry on.
    #[allow(dead_code)]
    pub fn is_ready(&self) -> bool {
        self.cursors.iter().all(|cursor| cursor.key().is_ok())
    }

    /// Continues merging, writing output to `builder`.  Returns `Ok(())` if the
    /// merge is complete, `Err(Pending)` otherwise.
    fn merge(&mut self, builder: &mut B::Builder, frontier: &B::Time) -> Result<(), Pending> {
        // We can drop all the cursors whose keys are at EOI.  If that
        // eliminates all of them, we're all done.  If any keys are pending,
        // then we can't do any work.
        assert!(self.cursors.len() <= 64);
        let mut remaining_cursors = IndexSet::empty();
        for (index, cursor) in self.cursors.iter_mut().enumerate() {
            skip_filtered_keys(cursor, &self.key_filter, &self.value_filter)?;
            if cursor.key()?.is_some() {
                remaining_cursors.add(index);
            }
        }
        if remaining_cursors.is_empty() {
            return Ok(());
        }

        let advance_func = |t: &mut DynDataTyped<B::Time>| t.join_assign(frontier);

        let time_map_func = if frontier == &B::Time::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<B::Time>))
        };

        // As long as there are multiple cursors...
        while remaining_cursors.is_long() {
            // Find the indexes of the cursors with minimum keys, among the
            // remaining cursors.
            let orig_min_keys = find_min_indexes(
                remaining_cursors
                    .into_iter()
                    .map(|index| (index, self.cursors[index].key().unwrap())),
            );

            // As long as there is more than one cursor with minimum keys...
            let mut min_keys = orig_min_keys;
            while min_keys.is_long() {
                // ...Find the indexes of the cursors with minimum values, among
                // those with minimum keys, and copy their time-diff pairs and
                // value into the output.
                let min_vals = try_find_min_indexes(
                    min_keys
                        .into_iter()
                        .map(|index| (index, self.cursors[index].val())),
                )?;
                self.any_values =
                    self.copy_times(builder, time_map_func, min_vals) || self.any_values;

                // Then go on to the next value in each cursor, dropping the keys
                // for which we've exhausted the values.
                for index in min_vals {
                    if self.step_val(index)?.is_none() {
                        min_keys.remove(index);
                    }
                }
            }

            // If there's exactly one cursor left with minimum key, copy its
            // values into the output.
            if let Some(index) = min_keys.first() {
                loop {
                    self.cursors[index].val()?;
                    self.any_values =
                        self.copy_times(builder, time_map_func, min_keys) || self.any_values;
                    if self.step_val(index)?.is_none() {
                        break;
                    }
                }
            }

            // If we wrote any values for these minimum keys, write the key.
            if self.any_values {
                let index = orig_min_keys.first().unwrap();
                builder.push_key(self.cursors[index].key().unwrap().unwrap());
                self.any_values = false;
            }

            // Advance each minimum-key cursor, dropping the cursors for which
            // we've exhausted the data.
            for index in orig_min_keys {
                if self.step_key(index)?.is_none() {
                    remaining_cursors.remove(index);
                }
            }
        }

        // If there is a cursor left (there's either one or none), copy it
        // directly to the output.
        if let Some(index) = remaining_cursors.first() {
            loop {
                loop {
                    self.cursors[index].val()?;
                    self.any_values = self.copy_times(builder, time_map_func, remaining_cursors)
                        || self.any_values;
                    if self.step_val(index)?.is_none() {
                        break;
                    }
                }
                debug_assert!(time_map_func.is_some() || self.any_values, "This assertion should fail only if B::Cursor is a spine or a CursorList, but we shouldn't be merging those");
                if self.any_values {
                    self.any_values = false;
                    builder.push_key(self.cursors[index].key().unwrap().unwrap());
                }
                if self.step_key(index)?.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Gives all the cursors under this merger an opportunity to process I/O
    /// results and launch further I/O.  Should be called periodically.
    pub fn run(&mut self) {
        for cursor in &mut self.cursors {
            cursor.run();
        }
    }

    fn step_val(&mut self, index: usize) -> Result<Option<&B::Val>, Pending> {
        self.cursors[index].step_val();
        skip_filtered_values(&mut self.cursors[index], &self.value_filter)?;
        self.cursors[index].val()
    }

    fn step_key(&mut self, index: usize) -> Result<Option<&B::Key>, Pending> {
        self.cursors[index].step_key();
        skip_filtered_keys(
            &mut self.cursors[index],
            &self.key_filter,
            &self.value_filter,
        )?;
        self.cursors[index].key()
    }

    #[track_caller]
    fn copy_times(
        &mut self,
        builder: &mut B::Builder,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        indexes: IndexSet,
    ) -> bool {
        // All of the cursors must have a valid value (hence the `unwrap()`),
        // and they must be equal.
        debug_assert!(indexes
            .into_iter()
            .map(|index| self.cursors[index].val().unwrap())
            .all_equal());

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
        builder.push_val(self.cursors[index].val().unwrap().unwrap());
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

fn try_find_min_indexes<Item>(
    mut iterator: impl Iterator<Item = (usize, Result<Item, Pending>)>,
) -> Result<IndexSet, Pending>
where
    Item: Ord,
{
    let (min_index, min_value) = iterator.next().unwrap();
    let mut min_indexes = IndexSet::for_index(min_index);
    let mut min_value = min_value?;

    for (index, value) in iterator {
        let value = value?;
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
    Ok(min_indexes)
}

fn skip_filtered_keys<C, K, V, T, R>(
    cursor: &mut C,
    key_filter: &Option<Filter<K>>,
    value_filter: &Option<Filter<V>>,
) -> Result<(), Pending>
where
    C: PushCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    if key_filter.is_some() || value_filter.is_some() {
        while let Some(key) = cursor.key()? {
            if Filter::include(key_filter, key) && skip_filtered_values(cursor, value_filter)? {
                return Ok(());
            }
            cursor.step_key();
        }
    }
    Ok(())
}

fn skip_filtered_values<C, K, V, T, R>(
    cursor: &mut C,
    value_filter: &Option<Filter<V>>,
) -> Result<bool, Pending>
where
    C: PushCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    if value_filter.is_some() {
        while let Some(value) = cursor.val()? {
            if Filter::include(value_filter, value) {
                return Ok(true);
            }
            cursor.step_val();
        }
        Ok(false)
    } else {
        Ok(true)
    }
}
