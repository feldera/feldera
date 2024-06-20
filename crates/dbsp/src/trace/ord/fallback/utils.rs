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
    Timestamp,
};

/// The row position of a [`Cursor`], regardless of the underlying type of the
/// cursor.
///
/// [`GenericMerger`] uses this to save and restore positions in the batches
/// it's merging, since it can't keep a cursor around from one run to another
/// because of lifetime issues.
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

impl<K, V, T, R, O> SizeOf for GenericMerger<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = T>,
    O::Builder: TimedBuilder<O>,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.builder.size_of_children(context)
    }
}
