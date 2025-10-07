use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        WeightTrait, WeightTraitTyped, WithFactory,
    },
    storage::{
        buffer_cache::CacheStats,
        file::{
            reader::{BulkRows, Cursor as FileCursor, Error as ReaderError, Reader},
            writer::Writer1,
            Factories as FileFactories,
        },
    },
    trace::{
        cursor::{CursorFactoryWrapper, Pending, Position, PushCursor},
        merge_batches_by_reference,
        ord::{file::UnwrapStorage, merge_batcher::MergeBatcher},
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor,
        Deserializer, FileKeyBatch, Serializer, VecWSetFactories, WeightedItem,
    },
    DBData, DBWeight, NumEntries, Runtime,
};
use dyn_clone::clone_box;
use feldera_storage::StoragePath;
use rand::{seq::index::sample, Rng};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug},
    ops::Neg,
    sync::Arc,
};

pub struct FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    file_factories: FileFactories<K, R>,
    pub vec_wset_factory: VecWSetFactories<K, R>,
}

impl<K, R> Clone for FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            file_factories: self.file_factories.clone(),
            vec_wset_factory: self.vec_wset_factory.clone(),
        }
    }
}

impl<K, R> BatchReaderFactories<K, DynUnit, (), R> for FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            file_factories: FileFactories::new::<KType, RType>(),
            vec_wset_factory: VecWSetFactories::new::<KType, (), RType>(),
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.vec_wset_factory.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.vec_wset_factory.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        WithFactory::<()>::FACTORY
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.vec_wset_factory.weight_factory()
    }
}

impl<K, R> BatchFactories<K, DynUnit, (), R> for FileWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    //type BatchItemFactory = BatchItemFactory<K, (), K, R>;

    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.vec_wset_factory.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.vec_wset_factory.weighted_item_factory()
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.vec_wset_factory.weighted_items_factory()
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynUnit, R>> {
        self.vec_wset_factory.weighted_vals_factory()
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<()>, R>>> {
        None
    }
}

/// A batch of weighted tuples without values or times.
///
/// Each tuple in `FileWSet<K, R>` has key type `K`, value type `()`, weight
/// type `R`, and time type `()`.
#[derive(SizeOf)]
pub struct FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileWSetFactories<K, R>,
    #[size_of(skip)]
    file: Arc<Reader<(&'static K, &'static R, ())>>,
}

impl<K, R> Debug for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileWSet {{ data: ")?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while cursor.key_valid() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}(", cursor.key())?;
            let diff = cursor.weight();
            write!(f, "({diff:+?})")?;
            n_keys += 1;
            cursor.step_key();
        }
        write!(f, " }}")
    }
}

impl<K, R> Clone for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            file: self.file.clone(),
        }
    }
}

impl<K, R> FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    pub fn len(&self) -> usize {
        self.file.n_rows(0) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
impl<Other, K, R> PartialEq<Other> for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    Other: BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
{
    fn eq(&self, other: &Other) -> bool {
        use crate::trace::eq_batch;
        eq_batch(self, other)
    }
}

impl<K, R> Eq for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, R> NumEntries for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.file.rows().len() as usize
    }

    fn num_entries_deep(&self) -> usize {
        self.num_entries_shallow()
    }
}

impl<K, R> NegByRef for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        let mut writer = Writer1::new(
            &self.factories.file_factories,
            Runtime::buffer_cache,
            &*Runtime::storage_backend().unwrap(),
            Runtime::file_writer_parameters(),
            self.key_count(),
        )
        .unwrap_storage();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            let diff = cursor.diff.neg_by_ref();
            writer.write0((cursor.key(), diff.erase())).unwrap_storage();
            cursor.step_key();
        }
        Self {
            factories: self.factories.clone(),
            file: Arc::new(writer.into_reader().unwrap_storage()),
        }
    }
}

impl<K, R> Neg for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        self.neg_by_ref()
    }
}

impl<K, R> AddAssignByRef for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        if !rhs.is_empty() {
            *self = merge_batches_by_reference(&self.factories, [self as &Self, rhs], &None, &None);
        }
    }
}

impl<K, R> AddByRef for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        merge_batches_by_reference(&self.factories, [self, rhs], &None, &None)
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Deserialize<FileWSet<K, R>, Deserializer>
    for ()
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<FileWSet<K, R>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Archive for FileWSet<K, R> {
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Serialize<Serializer> for FileWSet<K, R> {
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, R> BatchReader for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FileWSetFactories<K, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = ();
    type R = R;
    type Cursor<'s> = FileWSetCursor<'s, K, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn push_cursor(
        &self,
    ) -> Box<dyn PushCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        Box::new(FileWSetPushCursor::new(self))
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileWSetCursor::new(self)
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.file.n_rows(0) as usize
    }

    #[inline]
    fn len(&self) -> usize {
        self.key_count()
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

    fn maybe_contains_key(&self, hash: u64) -> bool {
        self.file.maybe_contains_key(hash)
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
                cursor.move_key(|key_cursor| unsafe { key_cursor.move_to_row(index as u64) });
                output.push_ref(cursor.key());
            }
        }
    }

    async fn fetch<B>(
        &self,
        keys: &B,
    ) -> Option<
        Box<dyn crate::trace::cursor::CursorFactory<Self::Key, Self::Val, Self::Time, Self::R>>,
    >
    where
        B: BatchReader<Key = Self::Key, Time = ()>,
    {
        let mut keys_vec;
        let keys = if let Some(keys) = keys.keys() {
            keys
        } else {
            keys_vec = self.factories.vec_wset_factory.keys_factory().default_box();
            keys_vec.reserve(keys.len());
            let mut cursor = keys.cursor();
            while cursor.key_valid() {
                keys_vec.push_ref(cursor.key());
                cursor.step_key();
            }
            &*keys_vec
        };

        let results = self
            .file
            .fetch_zset(keys)
            .unwrap_storage()
            .async_results(self.factories.vec_wset_factory.clone())
            .await
            .unwrap_storage();

        Some(Box::new(CursorFactoryWrapper(results)))
    }
}

impl<K, R> Batch for FileWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Timed<T: crate::Timestamp> = FileKeyBatch<K, T, R>;
    type Batcher = MergeBatcher<Self>;
    type Builder = FileWSetBuilder<K, R>;

    fn checkpoint_path(&self) -> Option<&StoragePath> {
        self.file.mark_for_checkpoint();
        Some(self.file.path())
    }

    fn from_path(factories: &Self::Factories, path: &StoragePath) -> Result<Self, ReaderError> {
        let any_factory0 = factories.file_factories.any_factories();
        let file = Arc::new(Reader::open(
            &[&any_factory0],
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

type FileWSetBulkRows<'s, K, R> = BulkRows<'s, K, R, (), (&'static K, &'static R, ())>;

/// A [PushCursor] for [FileWSet].
#[derive(Debug, SizeOf)]
pub struct FileWSetPushCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    bulk_rows: FileWSetBulkRows<'s, K, R>,
    key: Box<K>,
    diff: Box<R>,
    #[size_of(skip)]
    key_valid: Result<bool, Pending>,
    #[size_of(skip)]
    val_valid: Result<bool, Pending>,
}

impl<'s, K, R> FileWSetPushCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(wset: &'s FileWSet<K, R>) -> Self {
        let mut this = Self {
            bulk_rows: wset.file.bulk_rows().unwrap_storage(),
            key: wset.factories.key_factory().default_box(),
            diff: wset.factories.weight_factory().default_box(),
            key_valid: Err(Pending),
            val_valid: Err(Pending),
        };
        this.fetch_item();
        this
    }

    fn fetch_item(&mut self) {
        let valid = if unsafe { self.bulk_rows.item((&mut self.key, &mut self.diff)) }.is_some() {
            Ok(true)
        } else if self.bulk_rows.at_eof() {
            Ok(false)
        } else {
            Err(Pending)
        };
        self.key_valid = valid;
        self.val_valid = valid;
    }
}

impl<'s, K, R> PushCursor<K, DynUnit, (), R> for FileWSetPushCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn key(&self) -> Result<Option<&K>, Pending> {
        self.key_valid.map(|valid| valid.then(|| self.key.as_ref()))
    }

    fn val(&self) -> Result<Option<&DynUnit>, Pending> {
        debug_assert_eq!(self.key_valid, Ok(true));
        self.val_valid.map(|valid| valid.then_some(&() as &DynUnit))
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        debug_assert_eq!(self.val_valid, Ok(true));
        logic(&(), self.diff.as_ref());
    }

    fn weight(&mut self) -> &R {
        debug_assert_eq!(self.val_valid, Ok(true));
        self.diff.as_ref()
    }

    fn step_key(&mut self) {
        self.bulk_rows.step();
        self.fetch_item();
    }

    fn step_val(&mut self) {
        debug_assert_eq!(self.val_valid, Ok(true));
        self.val_valid = Ok(false);
    }

    fn run(&mut self) {
        self.bulk_rows.work().unwrap_storage();
        if self.key_valid == Err(Pending) {
            self.fetch_item();
        }
    }
}

type RawCursor<'s, K, R> = FileCursor<'s, K, R, (), (&'static K, &'static R, ())>;

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    wset: &'s FileWSet<K, R>,
    cursor: RawCursor<'s, K, R>,
    pub(crate) diff: Box<R>,
    val_valid: bool,
}

impl<K, R> Clone for FileWSetCursor<'_, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            wset: self.wset,
            cursor: self.cursor.clone(),
            diff: clone_box(&self.diff),
            val_valid: self.val_valid,
        }
    }
}

impl<'s, K, R> FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(wset: &'s FileWSet<K, R>) -> Self {
        let cursor = unsafe { wset.file.rows().first().unwrap_storage() };
        let diff = wset.factories.weight_factory().default_box();
        let valid = cursor.has_value();

        Self {
            wset,
            cursor,
            diff,
            val_valid: valid,
        }
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut RawCursor<'s, K, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.cursor).unwrap_storage();
        self.val_valid = self.cursor.has_value();
    }
}

impl<K, R> Cursor<K, DynUnit, (), R> for FileWSetCursor<'_, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn key(&self) -> &K {
        self.cursor.key().unwrap()
    }

    fn val(&self) -> &DynUnit {
        debug_assert!(self.val_valid);
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.val_valid {
            unsafe { self.cursor.aux(&mut self.diff) };
            logic(&(), self.diff.as_ref());
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.val_valid);
        unsafe { self.cursor.aux(&mut self.diff) };
        self.diff.as_ref()
    }

    fn key_valid(&self) -> bool {
        self.cursor.has_value()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn step_key(&mut self) {
        self.move_key(|key_cursor| unsafe { key_cursor.move_next() });
        self.val_valid = self.cursor.has_value();
    }

    fn step_key_reverse(&mut self) {
        self.move_key(|key_cursor| unsafe { key_cursor.move_prev() });
    }

    fn seek_key(&mut self, key: &K) {
        self.move_key(|key_cursor| unsafe { key_cursor.advance_to_value_or_larger(key) });
    }

    fn seek_key_exact(&mut self, key: &K, hash: Option<u64>) -> bool {
        let hash = hash.unwrap_or_else(|| key.default_hash());
        if !self.wset.maybe_contains_key(hash) {
            return false;
        }
        self.seek_key(key);
        self.get_key() == Some(key)
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
        self.move_key(|key_cursor| unsafe { key_cursor.move_first() });
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| unsafe { key_cursor.move_last() });
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

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.wset.factories.weight_factory()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R)) {
        if self.val_valid {
            unsafe { self.cursor.aux(&mut self.diff) };
            logic(&(), self.diff.as_ref())
        }
    }

    fn position(&self) -> Option<Position> {
        Some(Position {
            total: self.cursor.len(),
            offset: self.cursor.absolute_position(),
        })
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FileWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileWSetFactories<K, R>,
    #[size_of(skip)]
    writer: Writer1<K, R>,
    weight: Box<R>,
    num_tuples: usize,
}

impl<K, R> Builder<FileWSet<K, R>> for FileWSetBuilder<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(
        factories: &<FileWSet<K, R> as BatchReader>::Factories,
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer1::new(
                &factories.file_factories,
                Runtime::buffer_cache,
                &*Runtime::storage_backend().unwrap_storage(),
                Runtime::file_writer_parameters(),
                capacity,
            )
            .unwrap_storage(),
            weight: factories.weight_factory().default_box(),
            num_tuples: 0,
        }
    }

    fn done(self) -> FileWSet<K, R> {
        FileWSet {
            factories: self.factories,
            file: Arc::new(self.writer.into_reader().unwrap_storage()),
        }
    }

    fn push_key(&mut self, key: &K) {
        self.writer.write0((key, &*self.weight)).unwrap_storage();
    }

    fn push_val(&mut self, _val: &DynUnit) {}

    fn push_time_diff(&mut self, _time: &(), weight: &R) {
        debug_assert!(!weight.is_zero());
        weight.clone_to(&mut self.weight);
        self.num_tuples += 1;
    }

    fn num_tuples(&self) -> usize {
        self.num_tuples
    }
}
