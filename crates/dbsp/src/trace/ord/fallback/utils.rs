use std::cmp::Ordering;

use dyn_clone::clone_box;
use size_of::SizeOf;

use crate::{
    dynamic::{DataTrait, DynDataTyped, DynWeightedPairs, Factory, WeightTrait},
    time::Antichain,
    trace::{ord::filter, Batch, BatchLocation, BatchReader, Builder, Cursor, Filter},
    Runtime, Timestamp,
};

/// The row position of a [`Cursor`], regardless of the underlying type of the
/// cursor.
///
/// [`GenericMerger`] uses this to save and restore positions in the batches
/// it's merging, since it can't keep a cursor around from one run to another
/// because of lifetime issues.
#[derive(SizeOf)]
enum Position<K>
where
    K: DataTrait + ?Sized,
{
    Start,
    At(Box<K>),
    End,
}

impl<K> Position<K>
where
    K: DataTrait + ?Sized,
{
    fn to_cursor<'s, B>(&self, source: &'s B) -> B::Cursor<'s>
    where
        B: BatchReader<Key = K>,
    {
        let mut cursor = source.cursor();
        match self {
            Position::Start => (),
            Position::At(key) => cursor.seek_key(key.as_ref()),
            Position::End => {
                cursor.fast_forward_keys();
                cursor.step_key()
            }
        }
        cursor
    }

    fn from_cursor<C, V, T, R>(cursor: &C) -> Position<K>
    where
        C: Cursor<K, V, T, R>,
        V: DataTrait + ?Sized,
        T: Timestamp,
        R: ?Sized,
    {
        if cursor.key_valid() {
            Self::At(clone_box(cursor.key()))
        } else {
            Self::End
        }
    }
}

#[derive(SizeOf)]
pub(super) struct GenericMerger<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = T>,
{
    builder: O::Builder,
    lower: Antichain<T>,
    upper: Antichain<T>,
    pos1: Position<K>,
    pos2: Position<K>,

    // scratch space
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<T>, R>>>,
}

impl<K, V, T, R, O> GenericMerger<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = T>,
{
    pub fn new<A, B>(
        output_factories: &O::Factories,
        time_diff_factory: Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>>,
        batch1: &A,
        batch2: &B,
    ) -> Self
    where
        A: BatchReader<Time = T>,
        B: BatchReader<Time = T>,
    {
        Self {
            builder: O::Builder::new_builder(output_factories),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            pos1: Position::Start,
            pos2: Position::Start,
            time_diffs: time_diff_factory.map(|factory| factory.default_box()),
        }
    }

    pub fn work<'s, A, B>(
        &mut self,
        source1: &'s A,
        source2: &'s B,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        frontier: &T,
        fuel: &mut isize,
    ) where
        A: BatchReader<Key = K, Val = V, R = R, Time = T>,
        B: BatchReader<Key = K, Val = V, R = R, Time = T>,
        A::Cursor<'s>: Cursor<K, V, T, R>,
        B::Cursor<'s>: Cursor<K, V, T, R>,
    {
        let advance_func = |t: &mut DynDataTyped<T>| t.join_assign(frontier);

        let time_map_func = if frontier == &T::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<T>))
        };

        let mut cursor1 = self.pos1.to_cursor(source1);
        let mut cursor2 = self.pos2.to_cursor(source2);
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => {
                    self.copy_values_if(&mut cursor1, key_filter, value_filter, time_map_func, fuel)
                }
                Ordering::Equal => self.merge_values_if(
                    &mut cursor1,
                    &mut cursor2,
                    key_filter,
                    value_filter,
                    time_map_func,
                    fuel,
                ),
                Ordering::Greater => {
                    self.copy_values_if(&mut cursor2, key_filter, value_filter, time_map_func, fuel)
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor1, key_filter, value_filter, time_map_func, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor2, key_filter, value_filter, time_map_func, fuel);
        }
        self.pos1 = Position::from_cursor(&cursor1);
        self.pos2 = Position::from_cursor(&cursor2);
    }

    pub fn done(self) -> O {
        self.builder.done_with_bounds((self.lower, self.upper))
    }

    fn copy_values_if<C>(
        &mut self,
        cursor: &mut C,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
        fuel: &mut isize,
    ) where
        C: Cursor<K, V, T, R>,
    {
        if filter(key_filter, cursor.key()) {
            let mut any = false;
            while cursor.val_valid() {
                any = self.copy_time_diffs_if(cursor, value_filter, map_func, fuel) || any;
            }
            if any {
                self.builder.push_key(cursor.key());
            }
        }
        *fuel -= 1;
        cursor.step_key();
    }

    fn copy_time_diffs_if<C>(
        &mut self,
        cursor: &mut C,
        value_filter: &Option<Filter<V>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
        fuel: &mut isize,
    ) -> bool
    where
        C: Cursor<K, V, T, R>,
    {
        let mut any = false;
        if filter(value_filter, cursor.val()) {
            if let Some(map_func) = map_func {
                let Some(time_diffs) = self.time_diffs.as_mut() else {
                    panic!("generic merger created without time_diff factory");
                };

                time_diffs.clear();
                cursor.map_times(&mut |time, diff| {
                    *fuel -= 1;

                    let mut time: T = time.clone();
                    map_func(&mut time);

                    time_diffs.push_refs((&time, diff));
                });
                time_diffs.consolidate();

                for i in 0..time_diffs.len() {
                    let (time, diff) = time_diffs.index(i).split();
                    self.builder.push_time_diff(time, diff);
                }
                any = !time_diffs.is_empty();
            } else {
                cursor.map_times(&mut |time, diff| {
                    self.builder.push_time_diff(time, diff);
                    *fuel -= 1;
                });
                any = true;
            }
        }
        if any {
            self.builder.push_val(cursor.val());
        }
        *fuel -= 1;
        cursor.step_val();
        any
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_values_if<C1, C2>(
        &mut self,
        cursor1: &mut C1,
        cursor2: &mut C2,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
        fuel: &mut isize,
    ) where
        C1: Cursor<K, V, T, R>,
        C2: Cursor<K, V, T, R>,
    {
        if filter(key_filter, cursor1.key()) {
            let mut any = false;
            while cursor1.val_valid() && cursor2.val_valid() {
                match cursor1.val().cmp(cursor2.val()) {
                    Ordering::Less => {
                        any = self.copy_time_diffs_if(cursor1, value_filter, map_func, fuel) || any;
                    }
                    Ordering::Equal => {
                        any = self.map_time_and_merge_diffs_if(
                            cursor1,
                            cursor2,
                            value_filter,
                            map_func,
                            fuel,
                        ) || any;
                    }
                    Ordering::Greater => {
                        any = self.copy_time_diffs_if(cursor2, value_filter, map_func, fuel) || any;
                    }
                }
            }
            while cursor1.val_valid() {
                any = self.copy_time_diffs_if(cursor1, value_filter, map_func, fuel) || any;
            }
            while cursor2.val_valid() {
                any = self.copy_time_diffs_if(cursor2, value_filter, map_func, fuel) || any;
            }
            if any {
                self.builder.push_key(cursor1.key());
            }
        }
        *fuel -= 1;
        cursor1.step_key();
        cursor2.step_key();
    }

    // Like `merge_time_diffs_if`, but additionally applies `map_func` to each timestamp.
    // Sorts and consolidate the resulting set of time/weight pairs.
    #[allow(clippy::too_many_arguments)]
    fn map_time_and_merge_diffs_if<C1, C2>(
        &mut self,
        cursor1: &mut C1,
        cursor2: &mut C2,
        value_filter: &Option<Filter<V>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
        fuel: &mut isize,
    ) -> bool
    where
        C1: Cursor<K, V, T, R>,
        C2: Cursor<K, V, T, R>,
    {
        let mut any = false;
        if filter(value_filter, cursor1.val()) {
            let time_diffs = self
                .time_diffs
                .as_mut()
                .expect("generic merger created without time_diff factory");

            time_diffs.clear();
            if let Some(map_func) = map_func {
                cursor1.map_times(&mut |time, diff| {
                    let mut time: T = time.clone();
                    map_func(&mut time);
                    time_diffs.push_refs((&time, diff));
                });
                cursor2.map_times(&mut |time, diff| {
                    let mut time: T = time.clone();
                    map_func(&mut time);
                    time_diffs.push_refs((&time, diff));
                });
            } else {
                cursor1.map_times(&mut |time, diff| time_diffs.push_refs((time, diff)));
                cursor2.map_times(&mut |time, diff| time_diffs.push_refs((time, diff)));
            }
            time_diffs.consolidate();

            if !time_diffs.is_empty() {
                for (t, d) in time_diffs.dyn_iter().map(|td| td.split()) {
                    self.builder.push_time_diff(t, d);
                }
                self.builder.push_val(cursor1.val());
                any = true;
            }
        }

        *fuel -= 1;
        cursor1.step_val();
        cursor2.step_val();
        any
    }
}

/// Reads all of the data from `cursor` and writes it to `builder`.
pub(super) fn copy_to_builder<B, Output, C, K, V, T, R>(builder: &mut B, mut cursor: C)
where
    B: Builder<Output>,
    Output: Batch<Key = K, Val = V, Time = T, R = R>,
    C: Cursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: WeightTrait + ?Sized,
{
    while cursor.key_valid() {
        while cursor.val_valid() {
            cursor.map_times(&mut |time, diff| builder.push_time_diff(time, diff));
            builder.push_val(cursor.val());
            cursor.step_val();
        }
        builder.push_key(cursor.key());
        cursor.step_key();
    }
}

pub(super) fn pick_merge_destination<'a, B, I>(
    batches: I,
    dst_hint: Option<BatchLocation>,
) -> BatchLocation
where
    B: BatchReader,
    I: IntoIterator<Item = &'a B>,
{
    if let Some(location) = dst_hint {
        return location;
    }

    // This is equivalent to `batch1.byte_size() + batch2.byte_size() >=
    // Runtime::min_storage_bytes()` but it avoids calling `byte_size()` any
    // more than necessary since it can be expensive.
    match Runtime::min_storage_bytes().unwrap_or(usize::MAX) {
        0 => BatchLocation::Storage,
        usize::MAX => BatchLocation::Memory,
        min_storage_bytes => {
            let mut size = 0;
            for b in batches {
                size += b.approximate_byte_size();
                if size >= min_storage_bytes {
                    return BatchLocation::Storage;
                }
            }

            BatchLocation::Memory
        }
    }
}
