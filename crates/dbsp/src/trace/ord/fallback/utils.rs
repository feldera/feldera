use std::cmp::Ordering;

use dyn_clone::clone_box;
use size_of::SizeOf;

use crate::{
    dynamic::{DataTrait, WeightTrait},
    time::Antichain,
    trace::{
        cursor::{HasTimeDiffCursor, TimeDiffCursor},
        ord::filter,
        Batch, BatchReader, BatchReaderFactories, Builder, Cursor, Filter, TimedBuilder,
    },
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
    O::Builder: TimedBuilder<O>,
{
    builder: O::Builder,
    lower: Antichain<T>,
    upper: Antichain<T>,
    pos1: Position<K>,
    pos2: Position<K>,
}

impl<K, V, T, R, O> GenericMerger<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = T>,
    O::Builder: TimedBuilder<O>,
{
    pub fn new<A, B>(factories: &O::Factories, batch1: &A, batch2: &B) -> Self
    where
        A: BatchReader<Time = T>,
        B: BatchReader<Time = T>,
    {
        Self {
            builder: O::Builder::new_builder(factories, T::default()),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            pos1: Position::Start,
            pos2: Position::Start,
        }
    }

    pub fn work<'s, A, B>(
        &mut self,
        source1: &'s A,
        source2: &'s B,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        A: BatchReader<Key = K, Val = V, R = R, Time = T>,
        B: BatchReader<Key = K, Val = V, R = R, Time = T>,
        A::Cursor<'s>: HasTimeDiffCursor<K, V, T, R>,
        B::Cursor<'s>: HasTimeDiffCursor<K, V, T, R>,
    {
        let mut cursor1 = self.pos1.to_cursor(source1);
        let mut cursor2 = self.pos2.to_cursor(source2);
        source1.factories().weight_factory().with(&mut |diff1| {
            source1.factories().weight_factory().with(&mut |diff2| {
                source1.factories().weight_factory().with(&mut |sum| {
                    while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
                        match cursor1.key().cmp(cursor2.key()) {
                            Ordering::Less => self.copy_values_if(
                                diff1,
                                &mut cursor1,
                                key_filter,
                                value_filter,
                                fuel,
                            ),
                            Ordering::Equal => self.merge_values_if(
                                &mut cursor1,
                                &mut cursor2,
                                diff1,
                                diff2,
                                sum,
                                key_filter,
                                value_filter,
                                fuel,
                            ),
                            Ordering::Greater => self.copy_values_if(
                                diff1,
                                &mut cursor2,
                                key_filter,
                                value_filter,
                                fuel,
                            ),
                        }
                    }

                    while cursor1.key_valid() && *fuel > 0 {
                        self.copy_values_if(diff1, &mut cursor1, key_filter, value_filter, fuel);
                    }
                    while cursor2.key_valid() && *fuel > 0 {
                        self.copy_values_if(diff1, &mut cursor2, key_filter, value_filter, fuel);
                    }
                })
            })
        });
        self.pos1 = Position::from_cursor(&cursor1);
        self.pos2 = Position::from_cursor(&cursor2);
    }

    pub fn done(self) -> O {
        self.builder.done_with_bounds(self.lower, self.upper)
    }

    fn copy_values_if<C>(
        &mut self,
        tmp: &mut R,
        cursor: &mut C,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        C: HasTimeDiffCursor<K, V, T, R>,
    {
        if filter(key_filter, cursor.key()) {
            while cursor.val_valid() {
                self.copy_time_diffs_if(cursor, tmp, value_filter, fuel);
            }
        }
        *fuel -= 1;
        cursor.step_key();
    }

    fn copy_time_diffs_if<C>(
        &mut self,
        cursor: &mut C,
        tmp: &mut R,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        C: HasTimeDiffCursor<K, V, T, R>,
    {
        if filter(value_filter, cursor.val()) {
            let mut tdc = cursor.time_diff_cursor();
            while let Some((time, diff)) = tdc.current(tmp) {
                self.builder
                    .push_time(cursor.key(), cursor.val(), time, diff);
                tdc.step();
                *fuel -= 1;
            }
        }
        *fuel -= 1;
        cursor.step_val();
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_values_if<C1, C2>(
        &mut self,
        cursor1: &mut C1,
        cursor2: &mut C2,
        tmp1: &mut R,
        tmp2: &mut R,
        sum: &mut R,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        C1: HasTimeDiffCursor<K, V, T, R>,
        C2: HasTimeDiffCursor<K, V, T, R>,
    {
        if filter(key_filter, cursor1.key()) {
            while cursor1.val_valid() && cursor2.val_valid() {
                match cursor1.val().cmp(cursor2.val()) {
                    Ordering::Less => self.copy_time_diffs_if(cursor1, tmp1, value_filter, fuel),
                    Ordering::Equal => self.merge_time_diffs_if(
                        cursor1,
                        cursor2,
                        tmp1,
                        tmp2,
                        sum,
                        value_filter,
                        fuel,
                    ),
                    Ordering::Greater => self.copy_time_diffs_if(cursor2, tmp1, value_filter, fuel),
                }
            }
            while cursor1.val_valid() {
                self.copy_time_diffs_if(cursor1, tmp1, value_filter, fuel);
            }
            while cursor2.val_valid() {
                self.copy_time_diffs_if(cursor2, tmp2, value_filter, fuel);
            }
        }
        *fuel -= 1;
        cursor1.step_key();
        cursor2.step_key();
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_time_diffs_if<C1, C2>(
        &mut self,
        cursor1: &mut C1,
        cursor2: &mut C2,
        tmp1: &mut R,
        tmp2: &mut R,
        sum: &mut R,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        C1: HasTimeDiffCursor<K, V, T, R>,
        C2: HasTimeDiffCursor<K, V, T, R>,
    {
        if filter(value_filter, cursor1.val()) {
            let mut tdc1 = cursor1.time_diff_cursor();
            let mut tdc2 = cursor2.time_diff_cursor();

            loop {
                let Some((time1, diff1)) = tdc1.current(tmp1) else {
                    break;
                };
                let Some((time2, diff2)) = tdc2.current(tmp2) else {
                    break;
                };

                match time1.cmp(time2) {
                    Ordering::Less => {
                        self.builder
                            .push_time(cursor1.key(), cursor1.val(), time1, diff1);
                        tdc1.step();
                    }
                    Ordering::Equal => {
                        diff1.add(diff2, sum);
                        if !sum.is_zero() {
                            self.builder
                                .push_time(cursor1.key(), cursor1.val(), time1, sum);
                        }
                        tdc1.step();
                        tdc2.step();
                    }
                    Ordering::Greater => {
                        self.builder
                            .push_time(cursor2.key(), cursor2.val(), time2, diff2);
                        tdc2.step();
                    }
                }
                *fuel -= 1;
            }
            while let Some((time1, diff1)) = tdc1.current(tmp1) {
                self.builder
                    .push_time(cursor1.key(), cursor1.val(), time1, diff1);
                tdc1.step();
                *fuel -= 1;
            }
            while let Some((time2, diff2)) = tdc2.current(tmp2) {
                self.builder
                    .push_time(cursor2.key(), cursor2.val(), time2, diff2);
                tdc2.step();
                *fuel -= 1;
            }
        }
        *fuel -= 1;
        cursor1.step_val();
        cursor2.step_val();
    }
}

/// Reads all of the data from `cursor` and writes it to `builder`.
pub(super) fn copy_to_builder<B, Output, C, K, V, T, R>(builder: &mut B, mut cursor: C)
where
    B: TimedBuilder<Output>,
    Output: Batch<Key = K, Val = V, Time = T, R = R>,
    C: HasTimeDiffCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: WeightTrait + ?Sized,
{
    let mut tmp = cursor.weight_factory().default_box();
    while cursor.key_valid() {
        while cursor.val_valid() {
            let mut td_cursor = cursor.time_diff_cursor();
            while let Some((time, diff)) = td_cursor.current(&mut tmp) {
                builder.push_time(cursor.key(), cursor.val(), time, diff);
                td_cursor.step();
            }
            drop(td_cursor);
            cursor.step_val();
        }
        cursor.step_key();
    }
}

pub(super) enum MergeTo {
    Memory,
    Storage,
}

impl<B> From<(&B, &B)> for MergeTo
where
    B: BatchReader,
{
    fn from((batch1, batch2): (&B, &B)) -> Self {
        // This is equivalent to `batch1.byte_size() + batch2.byte_size() >=
        // Runtime::min_storage_bytes()` but it avoids calling `byte_size()` any
        // more than necessary since it can be expensive.
        let spill = match Runtime::min_storage_bytes() {
            0 => true,
            usize::MAX => false,
            min_storage_bytes => {
                let size1 = batch1.approximate_byte_size();
                size1 >= min_storage_bytes || {
                    let size2 = batch2.approximate_byte_size();
                    size1 + size2 >= min_storage_bytes
                }
            }
        };

        if spill {
            Self::Storage
        } else {
            Self::Memory
        }
    }
}

pub(super) enum BuildTo<M, S> {
    Memory(M),
    Storage(S),
    Threshold(M, usize),
}

impl<M, S> BuildTo<M, S> {
    pub fn for_capacity<MF, SF, T, MC, SC>(
        vf: MF,
        sf: SF,
        time: T,
        capacity: usize,
        mc: MC,
        sc: SC,
    ) -> Self
    where
        MC: Fn(MF, T, usize) -> M,
        SC: Fn(SF, T, usize) -> S,
    {
        match Runtime::min_storage_bytes() {
            usize::MAX => {
                // Storage is disabled.
                Self::Memory(mc(vf, time, capacity))
            }

            min_storage_bytes if capacity >= min_storage_bytes => {
                // If `capacity` is filled up then we'll have at least 1 byte
                // per item (as a bottom of the barrel estimate) so we might as
                // well start out on storage.
                Self::Storage(sc(sf, time, capacity))
            }
            min_storage_bytes => {
                // Start out in memory and spill to storage if
                // `min_storage_bytes` is used.
                Self::Threshold(mc(vf, time, capacity), min_storage_bytes)
            }
        }
    }
}
