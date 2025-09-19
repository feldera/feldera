use crate::storage::buffer_cache::CacheStats;
use crate::trace::cursor::Position;
use crate::trace::ord::file::UnwrapStorage;
use crate::trace::BatchLocation;
use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, LeanVec, WeightTrait, WithFactory,
    },
    storage::file::{
        reader::{Cursor as FileCursor, Error as ReaderError, Reader},
        writer::Writer2,
        Factories as FileFactories,
    },
    trace::{
        ord::merge_batcher::MergeBatcher, Batch, BatchFactories, BatchReader, BatchReaderFactories,
        Builder, Cursor, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries, Runtime, Timestamp,
};
use derive_more::Debug;
use dyn_clone::clone_box;
use feldera_storage::StoragePath;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::sync::Arc;
use std::{
    fmt,
    fmt::{Display, Formatter},
};

pub struct FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<V, DynWeightedPairs<DynDataTyped<T>, R>>,
    pub timediff_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
    weight_factory: &'static dyn Factory<R>,
    optkey_factory: &'static dyn Factory<DynOpt<K>>,
    keys_factory: &'static dyn Factory<DynVec<K>>,
    item_factory: &'static dyn Factory<DynPair<K, V>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, V, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>,
    weighted_vals_factory: &'static dyn Factory<DynWeightedPairs<V, R>>,
}

impl<K, V, T, R> Clone for FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
            optkey_factory: self.optkey_factory,
            weight_factory: self.weight_factory,
            timediff_factory: self.timediff_factory,
            keys_factory: self.keys_factory,
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
            weighted_vals_factory: self.weighted_vals_factory,
        }
    }
}

impl<K, V, T, R> BatchReaderFactories<K, V, T, R> for FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            factories0: FileFactories::new::<KType, ()>(),
            factories1: FileFactories::new::<VType, LeanVec<Tup2<T, RType>>>(),
            optkey_factory: WithFactory::<Option<KType>>::FACTORY,
            weight_factory: WithFactory::<RType>::FACTORY,
            timediff_factory: WithFactory::<LeanVec<Tup2<T, RType>>>::FACTORY,
            keys_factory: WithFactory::<LeanVec<KType>>::FACTORY,
            item_factory: WithFactory::<Tup2<KType, VType>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, VType>, RType>>::FACTORY,
            weighted_items_factory:
                WithFactory::<LeanVec<Tup2<Tup2<KType, VType>, RType>>>::FACTORY,
            weighted_vals_factory: WithFactory::<LeanVec<Tup2<VType, RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.factories0.key_factory
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.keys_factory
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.factories1.key_factory
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }
}

impl<K, V, T, R> BatchFactories<K, V, T, R> for FileValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.weighted_items_factory
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>> {
        self.weighted_vals_factory
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>> {
        Some(self.timediff_factory)
    }
}

type RawValBatch<K, V, T, R> = Arc<
    Reader<(
        &'static K,
        &'static DynUnit,
        (
            &'static V,
            &'static DynWeightedPairs<DynDataTyped<T>, R>,
            (),
        ),
    )>,
>;

type RawKeyCursor<'s, K, V, T, R> = FileCursor<
    's,
    K,
    DynUnit,
    (
        &'static V,
        &'static DynWeightedPairs<DynDataTyped<T>, R>,
        (),
    ),
    (
        &'static K,
        &'static DynUnit,
        (
            &'static V,
            &'static DynWeightedPairs<DynDataTyped<T>, R>,
            (),
        ),
    ),
>;

type RawValCursor<'s, K, V, T, R> = FileCursor<
    's,
    V,
    DynWeightedPairs<DynDataTyped<T>, R>,
    (),
    (
        &'static K,
        &'static DynUnit,
        (
            &'static V,
            &'static DynWeightedPairs<DynDataTyped<T>, R>,
            (),
        ),
    ),
>;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(SizeOf, Debug)]
pub struct FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    #[debug(skip)]
    factories: FileValBatchFactories<K, V, T, R>,
    #[size_of(skip)]
    pub file: RawValBatch<K, V, T, R>,
}

impl<K, V, T, R> Clone for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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

impl<K, V, T, R> NumEntries for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.file.n_rows(1) as usize
    }
}

impl<K, V, T, R> Display for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(f, "FileValBatch")
    }
}

impl<K, V, T, R> BatchReader for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FileValBatchFactories<K, V, T, R>;
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Cursor<'s> = FileValCursor<'s, K, V, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        FileValCursor::new(self)
    }

    fn key_count(&self) -> usize {
        self.file.rows().len() as usize
    }

    fn len(&self) -> usize {
        self.file.n_rows(1) as usize
    }

    fn approximate_byte_size(&self) -> usize {
        self.file.byte_size().unwrap_storage() as usize
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
        self.factories.factories0.key_factory.with(&mut |key| {
            let size = self.key_count();
            let mut cursor = self.file.rows().first().unwrap_storage();
            if sample_size >= size {
                output.reserve(size);
                while let Some(key) = unsafe { cursor.key(key) } {
                    output.push_ref(key);
                    cursor.move_next().unwrap_storage();
                }
            } else {
                output.reserve(sample_size);

                let mut indexes = sample(rng, size, sample_size).into_vec();
                indexes.sort_unstable();
                for index in indexes {
                    cursor.move_to_row(index as u64).unwrap_storage();
                    output.push_ref(unsafe { cursor.key(key) }.unwrap());
                }
            }
        })
    }

    fn maybe_contains_key(&self, hash: u64) -> Option<bool> {
        Some(self.file.maybe_contains_key(hash))
    }
}

impl<K, V, T, R> Batch for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Timed<T2: Timestamp> = FileValBatch<K, V, T2, R>;
    type Batcher = MergeBatcher<Self>;
    type Builder = FileValBuilder<K, V, T, R>;

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
            &*Runtime::storage_backend().unwrap_storage(),
            path,
        )?);
        Ok(Self {
            factories: factories.clone(),
            file,
        })
    }
}

#[derive(SizeOf)]
pub struct FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    batch: &'s FileValBatch<K, V, T, R>,
    timediff_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
    weight_factory: &'static dyn Factory<R>,
    key_cursor: RawKeyCursor<'s, K, V, T, R>,
    val_cursor: RawValCursor<'s, K, V, T, R>,
    key: Box<K>,
    key_valid: bool,
    val: Box<V>,
    val_valid: bool,
    weight: Box<R>,
}

impl<K, V, T, R> Debug for FileValCursor<'_, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileValCursor")
            .field("key_cursor", &self.key_cursor)
            .field("val_cursor", &self.val)
            .field("key", &self.key)
            .field("key_valid", &self.key_valid)
            .field("val", &self.val)
            .field("val_valid", &self.val_valid)
            .field("weight", &self.weight)
            .finish()
    }
}

impl<K, V, T, R> Clone for FileValCursor<'_, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            batch: self.batch,
            timediff_factory: self.timediff_factory,
            weight_factory: self.weight_factory,
            key_cursor: self.key_cursor.clone(),
            val_cursor: self.val_cursor.clone(),
            key: clone_box(&self.key),
            key_valid: self.key_valid,
            val: clone_box(&self.val),
            val_valid: self.val_valid,
            weight: clone_box(&self.weight),
        }
    }
}

impl<'s, K, V, T, R> FileValCursor<'s, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new(batch: &'s FileValBatch<K, V, T, R>) -> Self {
        let key_cursor = batch.file.rows().first().unwrap_storage();
        let val_cursor = key_cursor
            .next_column()
            .unwrap_storage()
            .first()
            .unwrap_storage();
        let mut key = batch.factories.factories0.key_factory.default_box();
        let mut val = batch.factories.factories1.key_factory.default_box();

        let key_valid = unsafe { key_cursor.key(&mut key) }.is_some();
        let val_valid = unsafe { val_cursor.key(&mut val) }.is_some();
        Self {
            batch,
            timediff_factory: batch.factories.timediff_factory,
            weight_factory: batch.factories.weight_factory,
            key_cursor,
            val_cursor,
            key,
            key_valid,
            val,
            val_valid,
            weight: batch.factories.weight_factory.default_box(),
        }
    }
    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawKeyCursor<'s, K, V, T, R>),
    {
        op(&mut self.key_cursor);
        self.val_cursor = self
            .key_cursor
            .next_column()
            .unwrap_storage()
            .first()
            .unwrap_storage();
        self.key_valid = unsafe { self.key_cursor.key(&mut self.key) }.is_some();
        self.val_valid = unsafe { self.val_cursor.key(&mut self.val) }.is_some();
    }
    fn move_val<F>(&mut self, op: F)
    where
        F: Fn(&mut RawValCursor<'s, K, V, T, R>),
    {
        op(&mut self.val_cursor);
        self.val_valid = unsafe { self.val_cursor.key(&mut self.val) }.is_some();
    }
    fn times<'a>(
        &self,
        times: &'a mut DynWeightedPairs<DynDataTyped<T>, R>,
    ) -> &'a mut DynWeightedPairs<DynDataTyped<T>, R> {
        unsafe { self.val_cursor.aux(times) }.unwrap()
    }
}

impl<K, V, T, R> Cursor<K, V, T, R> for FileValCursor<'_, K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid);
        &self.key
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid);
        &self.val
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.timediff_factory.with(&mut |timediffs| {
            for timediff in self.times(timediffs).dyn_iter() {
                let (time, weight) = timediff.split();
                logic(time, weight);
            }
        })
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.timediff_factory.with(&mut |timediffs| {
            for timediff in self.times(timediffs).dyn_iter() {
                let (time, weight) = timediff.split();

                if time.less_equal(upper) {
                    logic(time, weight);
                }
            }
        })
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            self.weight();
            logic(self.val(), &self.weight);
            self.step_val()
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        self.weight.set_zero();

        self.timediff_factory.with(&mut |timediffs| {
            for timediff in self.times(timediffs).dyn_iter() {
                self.weight.add_assign(timediff.snd());
            }
        });

        debug_assert!(!self.weight.is_zero());
        &self.weight
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.has_value()
    }
    fn val_valid(&self) -> bool {
        self.val_cursor.has_value()
    }
    fn step_key(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_next().unwrap_storage());
    }

    fn step_key_reverse(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_prev().unwrap_storage());
    }

    fn seek_key(&mut self, key: &K) {
        self.move_key(|key_cursor| {
            unsafe { key_cursor.advance_to_value_or_larger(key) }.unwrap_storage()
        });
    }

    fn seek_key_exact(&mut self, key: &K, hash: Option<u64>) -> bool {
        let hash = hash.unwrap_or_else(|| key.default_hash());
        if self.batch.maybe_contains_key(hash) == Some(false) {
            return false;
        }
        self.seek_key(key);
        self.key_valid() && self.key().eq(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| {
            unsafe { key_cursor.seek_forward_until(predicate) }.unwrap_storage()
        });
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.move_key(|key_cursor| {
            unsafe { key_cursor.seek_backward_until(predicate) }.unwrap_storage()
        });
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.move_key(|key_cursor| {
            unsafe { key_cursor.rewind_to_value_or_smaller(key) }.unwrap_storage()
        });
    }
    fn step_val(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_next().unwrap_storage());
    }
    fn seek_val(&mut self, val: &V) {
        self.move_val(|val_cursor| {
            unsafe { val_cursor.advance_to_value_or_larger(val) }.unwrap_storage()
        });
    }
    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| {
            unsafe { val_cursor.seek_forward_until(&predicate) }.unwrap_storage()
        });
    }
    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_first().unwrap_storage());
    }
    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_last().unwrap_storage());
    }
    fn rewind_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_first().unwrap_storage());
    }

    fn step_val_reverse(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_prev().unwrap_storage());
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.move_val(|val_cursor| {
            unsafe { val_cursor.rewind_to_value_or_smaller(val) }.unwrap_storage()
        });
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| {
            unsafe { val_cursor.seek_backward_until(&predicate) }.unwrap_storage()
        });
    }

    fn fast_forward_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_last().unwrap_storage());
    }

    fn position(&self) -> Option<Position> {
        Some(Position {
            total: self.key_cursor.len(),
            offset: self.key_cursor.absolute_position(),
        })
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FileValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileValBatchFactories<K, V, T, R>,
    #[size_of(skip)]
    writer: Writer2<K, DynUnit, V, DynWeightedPairs<DynDataTyped<T>, R>>,
    time_diffs: Box<DynWeightedPairs<DynDataTyped<T>, R>>,
    num_tuples: usize,
}

impl<K, V, T, R> Builder<FileValBatch<K, V, T, R>> for FileValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(factories: &FileValBatchFactories<K, V, T, R>, capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                Runtime::buffer_cache,
                &*Runtime::storage_backend().unwrap_storage(),
                Runtime::file_writer_parameters(),
                capacity,
            )
            .unwrap_storage(),
            time_diffs: factories.timediff_factory.default_box(),
            num_tuples: 0,
        }
    }

    fn done(self) -> FileValBatch<K, V, T, R> {
        FileValBatch {
            factories: self.factories,
            file: Arc::new(self.writer.into_reader().unwrap_storage()),
        }
    }

    fn push_key(&mut self, key: &K) {
        self.writer.write0((key, &())).unwrap_storage();
    }

    fn push_time_diff(&mut self, time: &T, weight: &R) {
        debug_assert!(!weight.is_zero());
        self.time_diffs.push_refs((time, weight));
        self.num_tuples += 1;
    }

    fn push_val(&mut self, val: &V) {
        self.writer
            .write1((val, &*self.time_diffs))
            .unwrap_storage();
        self.time_diffs.clear();
    }

    fn num_tuples(&self) -> usize {
        self.num_tuples
    }
}

/*
pub struct FileValConsumer<K, V, T, R> {
    __type: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> Consumer<K, V, R, T> for FileValConsumer<K, V, T, R> {
    type ValueConsumer<'a> = FileValValueConsumer<'a, K, V, T, R>
        where
            Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &K {
        todo!()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}

pub struct FileValValueConsumer<'a, K, V, T, R> {
    __type: PhantomData<&'a (K, V, T, R)>,
}

impl<'a, K, V, T, R> ValueConsumer<'a, V, R, T> for FileValValueConsumer<'a, K, V, T, R> {
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (V, R, T) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}
*/

impl<K, V, T, R> Archive for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, T, R, S> Serialize<S> for FileValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, T, R, D> Deserialize<FileValBatch<K, V, T, R>, D> for Archived<FileValBatch<K, V, T, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileValBatch<K, V, T, R>, D::Error> {
        unimplemented!();
    }
}
