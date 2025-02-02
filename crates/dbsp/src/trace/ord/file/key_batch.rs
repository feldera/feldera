use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WeightTrait, WithFactory,
    },
    storage::{
        buffer_cache::CacheStats,
        file::{
            reader::{Cursor as FileCursor, Error as ReaderError, Reader},
            writer::Writer2,
            Factories as FileFactories,
        },
    },
    time::{Antichain, AntichainRef},
    trace::{
        cursor::{HasTimeDiffCursor, TimeDiffCursor},
        ord::{filter, merge_batcher::MergeBatcher},
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor,
        Filter, Merger, TimedBuilder, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::{self, Debug},
    path::{Path, PathBuf},
};

pub struct FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    key_factory: &'static dyn Factory<K>,
    weight_factory: &'static dyn Factory<R>,
    weights_factory: &'static dyn Factory<DynVec<R>>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<DynDataTyped<T>, R>,
    opt_key_factory: &'static dyn Factory<DynOpt<K>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
    pub timediff_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
}

impl<K, T, R> Clone for FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            key_factory: self.key_factory,
            weight_factory: self.weight_factory,
            weights_factory: self.weights_factory,
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
            opt_key_factory: self.opt_key_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
            timediff_factory: self.timediff_factory,
        }
    }
}

impl<K, T, R> BatchReaderFactories<K, DynUnit, T, R> for FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            key_factory: WithFactory::<KType>::FACTORY,
            weight_factory: WithFactory::<RType>::FACTORY,
            weights_factory: WithFactory::<LeanVec<RType>>::FACTORY,
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            factories0: FileFactories::new::<KType, ()>(),
            factories1: FileFactories::new::<T, RType>(),
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
            timediff_factory: WithFactory::<LeanVec<Tup2<T, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        WithFactory::<()>::FACTORY
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }
}

impl<K, T, R> BatchFactories<K, DynUnit, T, R> for FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.weighted_items_factory
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>> {
        Some(self.timediff_factory)
    }
}

/// A batch of keys with weights and times.
///
/// Each tuple in `FileKeyBatch<K, T, R>` has key type `K`, value type `()`,
/// weight type `R`, and time type `R`.
#[derive(SizeOf)]
pub struct FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileKeyBatchFactories<K, T, R>,
    #[allow(clippy::type_complexity)]
    #[size_of(skip)]
    file: Reader<(
        &'static K,
        &'static DynUnit,
        (&'static DynDataTyped<T>, &'static R, ()),
    )>,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
}

impl<K, T, R> Debug for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileKeyBatch {{ data: ")?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while cursor.key_valid() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}(", cursor.key())?;
            let mut n_values = 0;
            cursor.map_times(&mut |time, diff| {
                if n_values > 0 {
                    let _ = write!(f, ", ");
                }
                let _ = write!(f, "({time:?}, {diff:+?})");
                n_values += 1;
            });
            write!(f, ")")?;
            n_keys += 1;
            cursor.step_key();
        }
        write!(f, " }}")
    }
}

impl<K, T, R> Clone for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            file: self.file.clone(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }
}

impl<K, T, R> NumEntries for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    fn num_entries_deep(&self) -> usize {
        self.file.n_rows(1) as usize
    }
}

impl<K, T, R> BatchReader for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FileKeyBatchFactories<K, T, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = T;
    type R = R;
    type Cursor<'s> = FileKeyCursor<'s, K, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        FileKeyCursor::new(self)
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.file.n_rows(0) as usize
    }

    #[inline]
    fn len(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn approximate_byte_size(&self) -> usize {
        self.file.byte_size().unwrap() as usize
    }

    #[inline]
    fn location(&self) -> BatchLocation {
        BatchLocation::Storage
    }

    fn cache_stats(&self) -> CacheStats {
        self.file.cache_stats()
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        let size = self.key_count();
        let mut cursor = self.cursor();
        if sample_size >= size {
            output.reserve(size);

            while cursor.key_valid() {
                output.push_ref(cursor.key());
                cursor.step_key();
            }
        } else {
            output.reserve(sample_size);

            let mut indexes = sample(rng, size, sample_size).into_vec();
            indexes.sort_unstable();
            for index in indexes.into_iter() {
                cursor.move_key(|key_cursor| key_cursor.move_to_row(index as u64));
                output.push_ref(cursor.key());
            }
        }
    }
}

impl<K, T, R> Batch for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileKeyBuilder<K, T, R>;
    type Merger = FileKeyMerger<K, T, R>;

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        Self::Merger::new_merger(self, other, dst_hint)
    }

    fn checkpoint_path(&self) -> Option<PathBuf> {
        self.file.mark_for_checkpoint();
        Some(self.file.path())
    }

    fn from_path(factories: &Self::Factories, path: &Path) -> Result<Self, ReaderError> {
        let any_factory0 = factories.factories0.any_factories();
        let any_factory1 = factories.factories1.any_factories();
        let file = Reader::open(
            &[&any_factory0, &any_factory1],
            Runtime::buffer_cache,
            &*Runtime::storage_backend().unwrap(),
            path,
        )?;

        Ok(Self {
            factories: factories.clone(),
            file,
            lower: Antichain::new(),
            upper: Antichain::new(),
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FileKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileKeyBatchFactories<K, T, R>,
    lower: Antichain<T>,
    upper: Antichain<T>,

    // Position in first batch.
    lower1: usize,
    // Position in second batch.
    lower2: usize,

    // Output so far.
    #[size_of(skip)]
    writer: Writer2<K, DynUnit, DynDataTyped<T>, R>,

    time_diffs: Box<DynWeightedPairs<DynDataTyped<T>, R>>,
}

impl<K, T, R> FileKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn copy_values_if(
        &mut self,
        cursor: &mut FileKeyCursor<K, T, R>,
        key_filter: &Option<Filter<K>>,
        map_func: Option<&dyn Fn(&mut DynDataTyped<T>)>,
        fuel: &mut isize,
    ) {
        if filter(key_filter, cursor.key.as_ref()) {
            if let Some(map_func) = map_func {
                self.time_diffs.clear();
                cursor.map_times(&mut |time, diff| {
                    let mut time = time.clone();
                    map_func(&mut time);
                    self.time_diffs.push_refs((&time, diff));
                    *fuel -= 1;
                });
                self.time_diffs.consolidate();
                for i in 0..self.time_diffs.len() {
                    let (time, diff) = self.time_diffs[i].split();
                    self.writer.write1((time, diff)).unwrap();
                }

                if !self.time_diffs.is_empty() {
                    self.writer
                        .write0((cursor.key.as_ref(), ().erase()))
                        .unwrap();
                }
            } else {
                cursor.map_times(&mut |time, diff| {
                    self.writer.write1((time, diff)).unwrap();
                    *fuel -= 1;
                });
                self.writer
                    .write0((cursor.key.as_ref(), ().erase()))
                    .unwrap();
            }
        } else {
            *fuel -= 1;
        }
        cursor.step_key();
    }

    fn merge_times<'a>(
        &mut self,
        cursor1: &mut FileKeyCursor<'a, K, T, R>,
        cursor2: &mut FileKeyCursor<'a, K, T, R>,
        sum: &mut R,
        fuel: &mut isize,
    ) -> bool {
        let mut n = 0;

        let mut subcursor1 = cursor1.cursor.next_column().unwrap().first().unwrap();
        let mut subcursor2 = cursor2.cursor.next_column().unwrap().first().unwrap();
        while subcursor1.has_value() && subcursor2.has_value() {
            let (time1, diff1) =
                unsafe { subcursor1.item((cursor1.time.as_mut(), &mut cursor1.diff)) }.unwrap();
            let (time2, diff2) =
                unsafe { subcursor2.item((cursor2.time.as_mut(), &mut cursor2.diff)) }.unwrap();
            let cmp = time1.cmp(&time2);
            match cmp {
                Ordering::Less => {
                    self.writer.write1((time1, diff1)).unwrap();
                    subcursor1.move_next().unwrap();
                    n += 1;
                }
                Ordering::Equal => {
                    diff1.add(diff2, sum);
                    if !sum.is_zero() {
                        self.writer.write1((time1, sum)).unwrap();
                        n += 1;
                    }
                    subcursor1.move_next().unwrap();
                    subcursor2.move_next().unwrap();
                }

                Ordering::Greater => {
                    self.writer.write1((time2, diff2)).unwrap();
                    n += 1;
                    subcursor2.move_next().unwrap();
                }
            }
            *fuel -= 1;
        }

        while let Some((time, diff)) =
            unsafe { subcursor1.item((cursor1.time.as_mut(), &mut cursor1.diff)) }
        {
            self.writer.write1((time, diff)).unwrap();
            subcursor1.move_next().unwrap();
            n += 1;
            *fuel -= 1;
        }
        while let Some((time, diff)) =
            unsafe { subcursor2.item((cursor2.time.as_mut(), &mut cursor2.diff)) }
        {
            self.writer.write1((time, diff)).unwrap();
            subcursor2.move_next().unwrap();
            n += 1;
            *fuel -= 1;
        }
        n > 0
    }

    // Like `merge_times`, but additionally applied `map_func` to each timestamp.
    // Sorts and consolidates the resulting array of time/diff pairs.
    fn map_and_merge_times<'a>(
        &mut self,
        cursor1: &mut FileKeyCursor<'a, K, T, R>,
        cursor2: &mut FileKeyCursor<'a, K, T, R>,
        map_func: &dyn Fn(&mut DynDataTyped<T>),
        fuel: &mut isize,
    ) -> bool {
        self.time_diffs.clear();

        let mut subcursor1 = cursor1.cursor.next_column().unwrap().first().unwrap();
        let mut subcursor2 = cursor2.cursor.next_column().unwrap().first().unwrap();

        while subcursor1.has_value() {
            let (time, diff) =
                unsafe { subcursor1.item((cursor1.time.as_mut(), &mut cursor1.diff)) }.unwrap();
            map_func(time);
            self.time_diffs.push_refs((time, diff));

            subcursor1.move_next().unwrap();
        }

        while subcursor2.has_value() {
            let (time, diff) =
                unsafe { subcursor2.item((cursor2.time.as_mut(), &mut cursor2.diff)) }.unwrap();
            map_func(time);
            self.time_diffs.push_refs((time, diff));

            subcursor2.move_next().unwrap();
        }

        self.time_diffs.consolidate();

        let len = self.time_diffs.len();

        for i in 0..len {
            let (t, w) = unsafe { self.time_diffs.index_unchecked(i) }.split();

            self.writer.write1((t, w)).unwrap();
        }

        *fuel -= len as isize;

        len > 0
    }
}

impl<K, T, R> Merger<K, DynUnit, T, R, FileKeyBatch<K, T, R>> for FileKeyMerger<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(
        batch1: &FileKeyBatch<K, T, R>,
        batch2: &FileKeyBatch<K, T, R>,
        _dst_hint: Option<BatchLocation>,
    ) -> Self {
        FileKeyMerger {
            factories: batch1.factories.clone(),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            lower1: 0,
            lower2: 0,
            writer: Writer2::new(
                &batch1.factories.factories0,
                &batch1.factories.factories1,
                Runtime::buffer_cache().unwrap(),
                &*Runtime::storage_backend().unwrap(),
                Runtime::file_writer_parameters(),
            )
            .unwrap(),
            time_diffs: batch1.factories.timediff_factory.default_box(),
        }
    }

    fn done(self) -> FileKeyBatch<K, T, R> {
        FileKeyBatch {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower: self.lower,
            upper: self.upper,
        }
    }

    fn work(
        &mut self,
        source1: &FileKeyBatch<K, T, R>,
        source2: &FileKeyBatch<K, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<DynUnit>>,
        frontier: &T,
        fuel: &mut isize,
    ) {
        if !filter(value_filter, &()) {
            return;
        }

        let advance_func = |t: &mut DynDataTyped<T>| t.join_assign(frontier);

        let time_map_func = if frontier == &T::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<T>))
        };

        let mut cursor1 = FileKeyCursor::new_from(source1, self.lower1);
        let mut cursor2 = FileKeyCursor::new_from(source2, self.lower2);
        let mut sum = self.factories.weight_factory.default_box();
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key.as_ref().cmp(cursor2.key.as_ref()) {
                Ordering::Less => {
                    self.copy_values_if(&mut cursor1, key_filter, time_map_func, fuel);
                }
                Ordering::Equal => {
                    if filter(key_filter, cursor1.key.as_ref()) {
                        let non_zero = if let Some(time_map_func) = &time_map_func {
                            self.map_and_merge_times(
                                &mut cursor1,
                                &mut cursor2,
                                time_map_func,
                                fuel,
                            )
                        } else {
                            self.merge_times(&mut cursor1, &mut cursor2, &mut sum, fuel)
                        };
                        if non_zero {
                            self.writer
                                .write0((cursor1.key.as_ref(), ().erase()))
                                .unwrap();
                        }
                    }
                    *fuel -= 1;
                    cursor1.step_key();
                    cursor2.step_key();
                }

                Ordering::Greater => {
                    self.copy_values_if(&mut cursor2, key_filter, time_map_func, fuel);
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor1, key_filter, time_map_func, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor2, key_filter, time_map_func, fuel);
        }
        self.lower1 = cursor1.cursor.absolute_position() as usize;
        self.lower2 = cursor2.cursor.absolute_position() as usize;
    }
}

type RawKeyCursor<'s, K, T, R> = FileCursor<
    's,
    K,
    DynUnit,
    (&'static DynDataTyped<T>, &'static R, ()),
    (
        &'static K,
        &'static DynUnit,
        (&'static DynDataTyped<T>, &'static R, ()),
    ),
>;

type RawTimeDiffCursor<'s, K, T, R> = FileCursor<
    's,
    DynDataTyped<T>,
    R,
    (),
    (
        &'static K,
        &'static DynUnit,
        (&'static DynDataTyped<T>, &'static R, ()),
    ),
>;

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    batch: &'s FileKeyBatch<K, T, R>,
    pub(crate) cursor: RawKeyCursor<'s, K, T, R>,
    key: Box<K>,
    val_valid: bool,

    pub(crate) time: Box<DynDataTyped<T>>,
    pub(crate) diff: Box<R>,
}

impl<K, T, R> Clone for FileKeyCursor<'_, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            batch: self.batch,
            cursor: self.cursor.clone(),
            key: clone_box(&self.key),
            val_valid: self.val_valid,

            // These don't need to be cloned because they're only used for
            // temporary storage.
            time: self.batch.factories.factories1.key_factory.default_box(),
            diff: self.batch.factories.weight_factory.default_box(),
        }
    }
}

impl<'s, K, T, R> FileKeyCursor<'s, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_from(batch: &'s FileKeyBatch<K, T, R>, lower_bound: usize) -> Self {
        let cursor = batch
            .file
            .rows()
            .subset(lower_bound as u64..)
            .first()
            .unwrap();
        let mut key = batch.factories.key_factory.default_box();
        let key_valid = unsafe { cursor.key(&mut key) }.is_some();

        Self {
            batch,
            cursor,
            key,
            val_valid: key_valid,
            time: batch.factories.factories1.key_factory.default_box(),
            diff: batch.factories.weight_factory.default_box(),
        }
    }

    fn new(batch: &'s FileKeyBatch<K, T, R>) -> Self {
        Self::new_from(batch, 0)
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawKeyCursor<'s, K, T, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.cursor).unwrap();
        let key_valid = unsafe { self.cursor.key(&mut self.key) }.is_some();
        self.val_valid = key_valid;
    }
}

impl<K, T, R> Cursor<K, DynUnit, T, R> for FileKeyCursor<'_, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.batch.factories.weight_factory
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        self.key.as_ref()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        let mut val_cursor = self.cursor.next_column().unwrap().first().unwrap();
        while unsafe { val_cursor.item((self.time.as_mut(), &mut self.diff)) }.is_some() {
            logic(self.time.as_ref(), self.diff.as_ref());
            val_cursor.move_next().unwrap();
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        let mut val_cursor = self.cursor.next_column().unwrap().first().unwrap();
        while unsafe { val_cursor.item((self.time.as_mut(), &mut self.diff)) }.is_some() {
            if self.time.less_equal(upper) {
                logic(self.time.as_ref(), self.diff.as_ref());
            }
            val_cursor.move_next().unwrap();
        }
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R))
    where
        T: PartialEq<()>,
    {
        if self.val_valid {
            logic(&(), self.weight())
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        let val_cursor = self.cursor.next_column().unwrap().first().unwrap();
        unsafe { val_cursor.aux(&mut self.diff) }.unwrap();
        self.diff.as_ref()
    }

    fn key_valid(&self) -> bool {
        self.cursor.has_value()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn step_key(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_next());
    }

    fn step_key_reverse(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_prev());
    }

    fn seek_key(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.advance_to_value_or_larger(key) });
    }

    fn seek_key_exact(&mut self, key: &K) -> bool {
        self.seek_key(key);
        self.key_valid() && self.key().eq(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_forward_until(predicate) });
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| unsafe { key_cursor.seek_backward_until(predicate) });
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.rewind_to_value_or_smaller(key) });
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }

    fn seek_val(&mut self, _val: &DynUnit) {}

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.val_valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_first());
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_last());
    }

    fn rewind_vals(&mut self) {
        self.val_valid = true;
    }

    fn step_val_reverse(&mut self) {
        self.val_valid = false;
    }

    fn seek_val_reverse(&mut self, _val: &DynUnit) {}

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.val_valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.val_valid = true;
    }
}

pub struct FileKeyTimeDiffCursor<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    cursor: RawTimeDiffCursor<'a, K, T, R>,
    time: T,
}

impl<'a, K, T, R> TimeDiffCursor<'a, T, R> for FileKeyTimeDiffCursor<'a, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn current<'b>(&'b mut self, tmp: &'b mut R) -> Option<(&'b T, &'b R)> {
        if unsafe { self.cursor.item((&mut self.time, tmp)) }.is_some() {
            Some((&self.time, tmp))
        } else {
            None
        }
    }

    fn step(&mut self) {
        self.cursor.move_next().unwrap();
    }
}

impl<K, T, R> HasTimeDiffCursor<K, DynUnit, T, R> for FileKeyCursor<'_, K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type TimeDiffCursor<'a>
        = FileKeyTimeDiffCursor<'a, K, T, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        FileKeyTimeDiffCursor {
            cursor: self.cursor.next_column().unwrap().first().unwrap(),
            time: T::default(),
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FileKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileKeyBatchFactories<K, T, R>,
    time: T,
    #[size_of(skip)]
    writer: Writer2<K, DynUnit, DynDataTyped<T>, R>,
    key: Box<DynOpt<K>>,
}

impl<K, T, R> TimedBuilder<FileKeyBatch<K, T, R>> for FileKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    /// Pushes a tuple including `time` into the builder.
    ///
    /// A caller that uses this must finalize the batch with
    /// [`Self::done_with_bounds`], supplying correct upper and lower bounds, to
    /// ensure that the final batch's invariants are correct.
    #[inline]
    fn push_time(&mut self, key: &K, _val: &DynUnit, time: &T, weight: &R) {
        if let Some(cur_key) = self.key.get() {
            if cur_key != key {
                self.writer.write0((cur_key, ().erase())).unwrap();
                self.key.from_ref(key);
            }
        } else {
            self.key.from_ref(key);
        }
        self.writer.write1((time, weight)).unwrap();
    }

    /// Finalizes a batch with lower bound `lower` and upper bound `upper`.
    /// This is only necessary if `push_time()` was used; otherwise, use
    /// [`Self::done`] instead.
    #[inline(never)]
    fn done_with_bounds(
        mut self,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> FileKeyBatch<K, T, R> {
        if let Some(key) = self.key.get() {
            self.writer.write0((key, ().erase())).unwrap();
        }
        FileKeyBatch {
            factories: self.factories,
            file: self.writer.into_reader().unwrap(),
            lower,
            upper,
        }
    }
}

impl<K, T, R> Builder<FileKeyBatch<K, T, R>> for FileKeyBuilder<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FileKeyBatchFactories<K, T, R>, time: T) -> Self {
        Self {
            factories: factories.clone(),
            time,
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                Runtime::buffer_cache().unwrap(),
                &*Runtime::storage_backend().unwrap(),
                Runtime::file_writer_parameters(),
            )
            .unwrap(),
            key: factories.opt_key_factory.default_box(),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &FileKeyBatchFactories<K, T, R>,
        time: T,
        _capacity: usize,
    ) -> Self {
        Self::new_builder(factories, time)
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut WeightedItem<K, DynUnit, R>) {
        let (kv, weight) = item.split();
        let k = kv.fst();
        self.push_refs(k, &(), weight);
    }

    #[inline]
    fn push_refs(&mut self, key: &K, _val: &DynUnit, weight: &R) {
        let time = self.time.clone();
        self.push_time(key, &(), &time, weight);
    }

    fn push_vals(&mut self, key: &mut K, val: &mut DynUnit, weight: &mut R) {
        self.push_refs(key, val, weight);
    }

    #[inline(never)]
    fn done(self) -> FileKeyBatch<K, T, R> {
        let lower = Antichain::from_elem(self.time.clone());
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        self.done_with_bounds(lower, upper)
    }
}

impl<K, T, R> Archive for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, T, R, S> Serialize<S> for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, T, R, D> Deserialize<FileKeyBatch<K, T, R>, D> for Archived<FileKeyBatch<K, T, R>>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileKeyBatch<K, T, R>, D::Error> {
        unimplemented!();
    }
}
