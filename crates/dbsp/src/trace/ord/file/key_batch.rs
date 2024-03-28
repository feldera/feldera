use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WeightTrait, WithFactory,
    },
    storage::{
        backend::Backend,
        file::{
            reader::{Cursor as FileCursor, Error as ReaderError, Reader},
            writer::{Parameters, Writer2},
            Factories as FileFactories,
        },
    },
    time::{Antichain, AntichainRef},
    trace::{
        ord::merge_batcher::MergeBatcher, Batch, BatchFactories, BatchReader, BatchReaderFactories,
        Builder, Cursor, Filter, Merger, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::{min, Ordering},
    fmt::{self, Debug},
    path::PathBuf,
};

pub struct FileKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    key_factory: &'static dyn Factory<K>,
    weight_factory: &'static dyn Factory<R>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<DynDataTyped<T>, R>,
    opt_key_factory: &'static dyn Factory<DynOpt<K>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
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
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
            opt_key_factory: self.opt_key_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
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
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            factories0: FileFactories::new::<KType, ()>(),
            factories1: FileFactories::new::<T, RType>(),
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
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
}

/// A batch of keys with weights and times.
///
/// Each tuple in `FileKeyBatch<K, T, R>` has key type `K`, value type `()`,
/// weight type `R`, and time type `R`.
pub struct FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileKeyBatchFactories<K, T, R>,
    #[allow(clippy::type_complexity)]
    file: Reader<
        Backend,
        (
            &'static K,
            &'static DynUnit,
            (&'static DynDataTyped<T>, &'static R, ()),
        ),
    >,
    lower_bound: usize,
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
        write!(
            f,
            "FileKeyBatch {{ lower_bound: {}, data: ",
            self.lower_bound
        )?;
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
            lower_bound: self.lower_bound,
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
        self.file.n_rows(0) as usize - self.lower_bound
    }

    #[inline]
    fn len(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        let mut cursor = self.file.rows().before();
        unsafe { cursor.advance_to_value_or_larger(lower_bound) }.unwrap();

        let lower_bound = cursor.absolute_position() as usize;
        if lower_bound > self.lower_bound {
            self.lower_bound = min(lower_bound, self.file.rows().len() as usize);
        }
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

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        Self::Merger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            // TODO: Optimize case where self.upper()==self.lower().
            self.do_recede_to(frontier);
        }
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        Some(self.file.path())
    }
}

impl<K, T, R> FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn do_recede_to(&mut self, _frontier: &T) {
        todo!()
    }
}

/// State for an in-progress merge.
pub struct FileKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileKeyBatchFactories<K, T, R>,
    lower: Antichain<T>,
    upper: Antichain<T>,

    // Position in first batch.
    lower1: usize,
    // Position in second batch.
    lower2: usize,

    // Output so far.
    writer: Writer2<Backend, K, DynUnit, DynDataTyped<T>, R>,
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
        fuel: &mut isize,
    ) {
        if filter(key_filter, cursor.key.as_ref()) {
            cursor.map_times(&mut |time, diff| {
                self.writer.write1((time, diff)).unwrap();
                *fuel -= 1;
            });
            self.writer
                .write0((cursor.key.as_ref(), ().erase()))
                .unwrap();
        } else {
            *fuel -= 1;
        }
        cursor.step_key();
    }

    fn merge_values<'a>(
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
}

impl<K, T, R> Merger<K, DynUnit, T, R, FileKeyBatch<K, T, R>> for FileKeyMerger<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(batch1: &FileKeyBatch<K, T, R>, batch2: &FileKeyBatch<K, T, R>) -> Self {
        FileKeyMerger {
            factories: batch1.factories.clone(),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            lower1: batch1.lower_bound,
            lower2: batch2.lower_bound,
            writer: Writer2::new(
                &batch1.factories.factories0,
                &batch1.factories.factories1,
                &Runtime::storage(),
                Parameters::default(),
            )
            .unwrap(),
        }
    }

    fn done(self) -> FileKeyBatch<K, T, R> {
        FileKeyBatch {
            factories: self.factories.clone(),
            file: self.writer.into_reader().unwrap(),
            lower_bound: 0,
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
        fuel: &mut isize,
    ) {
        if !filter(value_filter, &()) {
            return;
        }

        let mut cursor1 = FileKeyCursor::new_from(source1, self.lower1);
        let mut cursor2 = FileKeyCursor::new_from(source2, self.lower2);
        let mut sum = self.factories.weight_factory.default_box();
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key.as_ref().cmp(cursor2.key.as_ref()) {
                Ordering::Less => {
                    self.copy_values_if(&mut cursor1, key_filter, fuel);
                }
                Ordering::Equal => {
                    if filter(key_filter, cursor1.key.as_ref())
                        && self.merge_values(&mut cursor1, &mut cursor2, &mut sum, fuel)
                    {
                        self.writer
                            .write0((cursor1.key.as_ref(), ().erase()))
                            .unwrap();
                    }
                    *fuel -= 1;
                    cursor1.step_key();
                    cursor2.step_key();
                }

                Ordering::Greater => {
                    self.copy_values_if(&mut cursor2, key_filter, fuel);
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor1, key_filter, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor2, key_filter, fuel);
        }
        self.lower1 = cursor1.cursor.absolute_position() as usize;
        self.lower2 = cursor2.cursor.absolute_position() as usize;
    }
}

fn filter<T>(f: &Option<Filter<T>>, t: &T) -> bool
where
    T: ?Sized,
{
    f.as_ref().map_or(true, |f| f(t))
}

impl<K, T, R> SizeOf for FileKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

type RawCursor<'s, K, T, R> = FileCursor<
    's,
    Backend,
    K,
    DynUnit,
    (&'static DynDataTyped<T>, &'static R, ()),
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
    cursor: RawCursor<'s, K, T, R>,
    key: Box<K>,
    val_valid: bool,

    time: Box<DynDataTyped<T>>,
    diff: Box<R>,
}

impl<'s, K, T, R> Clone for FileKeyCursor<'s, K, T, R>
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
        Self::new_from(batch, batch.lower_bound)
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawCursor<'s, K, T, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.cursor).unwrap();
        let key_valid = unsafe { self.cursor.key(&mut self.key) }.is_some();
        self.val_valid = key_valid;
    }
}

impl<'s, K, T, R> Cursor<K, DynUnit, T, R> for FileKeyCursor<'s, K, T, R>
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

/// A builder for creating layers from unsorted update tuples.
pub struct FileKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories: FileKeyBatchFactories<K, T, R>,
    time: T,
    writer: Writer2<Backend, K, DynUnit, DynDataTyped<T>, R>,
    key: Box<DynOpt<K>>,
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
                &Runtime::storage(),
                Parameters::default(),
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
        if let Some(cur_key) = self.key.get() {
            if cur_key != key {
                self.writer.write0((cur_key, ().erase())).unwrap();
                self.key.from_ref(key);
            }
        } else {
            self.key.from_ref(key);
        }
        self.writer.write1((&self.time, weight)).unwrap();
    }

    fn push_vals(&mut self, key: &mut K, val: &mut DynUnit, weight: &mut R) {
        self.push_refs(key, val, weight);
    }

    #[inline(never)]
    fn done(mut self) -> FileKeyBatch<K, T, R> {
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };

        if let Some(key) = self.key.get() {
            self.writer.write0((key, ().erase())).unwrap();
        }
        FileKeyBatch {
            factories: self.factories,
            file: self.writer.into_reader().unwrap(),
            lower: Antichain::from_elem(self.time),
            upper,
            lower_bound: 0,
        }
    }
}

impl<K, T, R> SizeOf for FileKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
    }
}

impl<K, T, R> SizeOf for FileKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, _context: &mut size_of::Context) {
        // XXX
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
