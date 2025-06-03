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
    trace::{
        ord::merge_batcher::MergeBatcher, Batch, BatchFactories, BatchLocation, BatchReader,
        BatchReaderFactories, Builder, Cursor, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use dyn_clone::clone_box;
use feldera_storage::StoragePath;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug},
    sync::Arc,
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
    weighted_vals_factory: &'static dyn Factory<DynWeightedPairs<DynUnit, R>>,
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
            weighted_vals_factory: self.weighted_vals_factory,
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
            weighted_vals_factory: WithFactory::<LeanVec<Tup2<(), RType>>>::FACTORY,
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

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynUnit, R>> {
        self.weighted_vals_factory
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
    file: Arc<
        Reader<(
            &'static K,
            &'static DynUnit,
            (&'static DynDataTyped<T>, &'static R, ()),
        )>,
    >,
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

    fn maybe_contains_key(&self, key: &Self::Key) -> bool {
        self.file.maybe_contains_key(key)
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

    fn checkpoint_path(&self) -> Option<StoragePath> {
        self.file.mark_for_checkpoint();
        Some(self.file.path())
    }

    fn from_path(factories: &Self::Factories, path: &StoragePath) -> Result<Self, ReaderError> {
        let any_factory0 = factories.factories0.any_factories();
        let any_factory1 = factories.factories1.any_factories();
        let file = Arc::new(Reader::open(
            &[&any_factory0, &any_factory1],
            Runtime::buffer_cache,
            &*Runtime::storage_backend().unwrap(),
            path,
        )?);

        Ok(Self {
            factories: factories.clone(),
            file,
        })
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
        if !self.batch.maybe_contains_key(key) {
            return false;
        }
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
    #[size_of(skip)]
    writer: Writer2<K, DynUnit, DynDataTyped<T>, R>,
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
    fn with_capacity(factories: &FileKeyBatchFactories<K, T, R>, capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                Runtime::buffer_cache,
                &*Runtime::storage_backend().unwrap(),
                Runtime::file_writer_parameters(),
                capacity,
            )
            .unwrap(),
            key: factories.opt_key_factory.default_box(),
        }
    }

    fn push_key(&mut self, key: &K) {
        self.writer.write0((key, &())).unwrap();
    }

    fn push_val(&mut self, _val: &DynUnit) {}

    fn push_time_diff(&mut self, time: &T, weight: &R) {
        debug_assert!(!weight.is_zero());
        self.writer.write1((time, weight)).unwrap();
    }

    fn done(self) -> FileKeyBatch<K, T, R> {
        FileKeyBatch {
            factories: self.factories,
            file: Arc::new(self.writer.into_reader().unwrap()),
        }
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
