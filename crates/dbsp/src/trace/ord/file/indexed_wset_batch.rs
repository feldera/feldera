use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    dynamic::{
        DataTrait, DynDataTyped, DynOpt, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase,
        Factory, WeightTrait, WeightTraitTyped, WithFactory,
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
        merge_batches_by_reference, ord::merge_batcher::MergeBatcher, Batch, BatchFactories,
        BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor, VecIndexedWSetFactories,
        WeightedItem,
    },
    DBData, DBWeight, NumEntries, Runtime,
};
use dyn_clone::clone_box;
use rand::{seq::index::sample, Rng};
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug},
    ops::Neg,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct FileIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories0: FileFactories<K, DynUnit>,
    factories1: FileFactories<V, R>,
    opt_key_factory: &'static dyn Factory<DynOpt<K>>,
    pub vec_indexed_wset_factory: VecIndexedWSetFactories<K, V, R>,
}

impl<K, V, R> Clone for FileIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories0: self.factories0.clone(),
            factories1: self.factories1.clone(),
            opt_key_factory: self.opt_key_factory,
            vec_indexed_wset_factory: self.vec_indexed_wset_factory.clone(),
        }
    }
}

impl<K, V, R> BatchReaderFactories<K, V, (), R> for FileIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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
            factories1: FileFactories::new::<VType, RType>(),
            opt_key_factory: WithFactory::<Option<KType>>::FACTORY,
            vec_indexed_wset_factory: VecIndexedWSetFactories::new::<KType, VType, RType>(),
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.vec_indexed_wset_factory.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.vec_indexed_wset_factory.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.vec_indexed_wset_factory.val_factory()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.vec_indexed_wset_factory.weight_factory()
    }
}

impl<K, V, R> BatchFactories<K, V, (), R> for FileIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.vec_indexed_wset_factory.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.vec_indexed_wset_factory.weighted_item_factory()
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.vec_indexed_wset_factory.weighted_items_factory()
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>> {
        self.vec_indexed_wset_factory.weighted_vals_factory()
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<()>, R>>> {
        None
    }
}

/// A batch of key-value weighted tuples without timing information.
///
/// Each tuple in `FileIndexedWSet<K, V, R>` has key type `K`, value type `V`,
/// weight type `R`, and time `()`.
#[derive(SizeOf)]
pub struct FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileIndexedWSetFactories<K, V, R>,
    #[allow(clippy::type_complexity)]
    #[size_of(skip)]
    file: Arc<Reader<(&'static K, &'static DynUnit, (&'static V, &'static R, ()))>>,
}

impl<K, V, R> Debug for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileIndexedWSet {{ data: ")?;
        let mut cursor = self.cursor();
        let mut n_keys = 0;
        while cursor.key_valid() {
            if n_keys > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{:?}(", cursor.key())?;
            let mut n_values = 0;
            while cursor.val_valid() {
                if n_values > 0 {
                    write!(f, ", ")?;
                }
                let val = cursor.val();
                write!(f, "({val:?}")?;
                let diff = cursor.weight();
                write!(f, ", {diff:+?})")?;
                n_values += 1;
                cursor.step_val();
            }
            write!(f, ")")?;
            n_keys += 1;
            cursor.step_key();
        }
        write!(f, " }}")
    }
}

impl<K, V, R> Clone for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            file: self.file.clone(),
        }
    }
}

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
#[cfg(test)]
impl<Other, K, V, R> PartialEq<Other> for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    Other: BatchReader<Key = K, Val = V, R = R, Time = ()>,
{
    fn eq(&self, other: &Other) -> bool {
        use crate::trace::eq_batch;
        eq_batch(self, other)
    }
}

#[cfg(test)]
impl<K, V, R> Eq for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, V, R> NumEntries for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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

impl<K, V, R> NegByRef for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        let mut writer = Writer2::new(
            &self.factories.factories0,
            &self.factories.factories1,
            Runtime::buffer_cache(),
            &*Runtime::storage_backend().unwrap(),
            Runtime::file_writer_parameters(),
            self.key_count(),
        )
        .unwrap();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let diff = cursor.diff.neg_by_ref();
                writer.write1((cursor.val.as_ref(), diff.erase())).unwrap();
                cursor.step_val();
            }
            writer.write0((cursor.key.as_ref(), &())).unwrap();
            cursor.step_key();
        }
        Self {
            factories: self.factories.clone(),
            file: Arc::new(writer.into_reader().unwrap()),
        }
    }
}

impl<K, V, R> Neg for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + NegByRef + Erase<R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        self.neg_by_ref()
    }
}

impl<K, V, R> AddAssignByRef for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        if !rhs.is_empty() {
            *self = merge_batches_by_reference(&self.factories, [self as &Self, rhs], &None, &None);
        }
    }
}

impl<K, V, R> AddByRef for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        merge_batches_by_reference(&self.factories, [self, rhs], &None, &None)
    }
}

impl<K, V, R> BatchReader for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FileIndexedWSetFactories<K, V, R>;
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s>
        = FileIndexedWSetCursor<'s, K, V, R>
    where
        V: 's;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FileIndexedWSetCursor::new(self)
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

    fn maybe_contains_key(&self, key: &K) -> bool {
        self.file.maybe_contains_key(key)
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

impl<K, V, R> Batch for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FileIndexedWSetBuilder<K, V, R>;

    fn checkpoint_path(&self) -> Option<PathBuf> {
        self.file.mark_for_checkpoint();
        Some(self.file.path())
    }

    fn from_path(factories: &Self::Factories, path: &Path) -> Result<Self, ReaderError> {
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

type KeyCursor<'s, K, V, R> = FileCursor<
    's,
    K,
    DynUnit,
    (&'static V, &'static R, ()),
    (&'static K, &'static DynUnit, (&'static V, &'static R, ())),
>;

type ValCursor<'s, K, V, R> =
    FileCursor<'s, V, R, (), (&'static K, &'static DynUnit, (&'static V, &'static R, ()))>;

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FileIndexedWSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    wset: &'s FileIndexedWSet<K, V, R>,

    key_cursor: KeyCursor<'s, K, V, R>,
    key: Box<K>,

    val_cursor: ValCursor<'s, K, V, R>,
    val: Box<V>,
    pub(crate) diff: Box<R>,
}

impl<K, V, R> Clone for FileIndexedWSetCursor<'_, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            wset: self.wset,
            key_cursor: self.key_cursor.clone(),
            key: clone_box(&self.key),
            val_cursor: self.val_cursor.clone(),
            val: clone_box(&self.val),
            diff: clone_box(&self.diff),
        }
    }
}

impl<'s, K, V, R> FileIndexedWSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new_from(wset: &'s FileIndexedWSet<K, V, R>, lower_bound: usize) -> Self {
        let key_cursor = wset
            .file
            .rows()
            .subset(lower_bound as u64..)
            .first()
            .unwrap();
        let mut key = wset.factories.key_factory().default_box();
        unsafe { key_cursor.key(&mut key) };

        let val_cursor = key_cursor.next_column().unwrap().first().unwrap();
        let mut val = wset.factories.val_factory().default_box();
        let mut diff = wset.factories.weight_factory().default_box();
        unsafe { val_cursor.item((&mut val, &mut diff)) };
        Self {
            wset,
            key_cursor,
            key,
            val_cursor,
            val,
            diff,
        }
    }

    pub fn new(wset: &'s FileIndexedWSet<K, V, R>) -> Self {
        Self::new_from(wset, 0)
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut KeyCursor<'s, K, V, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.key_cursor).unwrap();
        unsafe { self.key_cursor.key(&mut self.key) };
        self.val_cursor = self
            .key_cursor
            .next_column()
            .unwrap()
            .first_with_hint(&self.val_cursor)
            .unwrap();
        unsafe { self.val_cursor.item((&mut self.val, &mut self.diff)) };
    }

    fn move_val<F>(&mut self, op: F)
    where
        F: Fn(&mut ValCursor<'s, K, V, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.val_cursor).unwrap();
        unsafe { self.val_cursor.item((&mut self.val, &mut self.diff)) };
    }
}

impl<K, V, R> Cursor<K, V, (), R> for FileIndexedWSetCursor<'_, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.wset.factories.weight_factory()
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        self.key.as_ref()
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid());
        self.val.as_ref()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.val_valid() {
            logic(&(), self.diff.as_ref())
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.val_valid() {
            logic(self.val(), self.diff.as_ref());
            self.step_val();
        }
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.val_valid());
        self.diff.as_ref()
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.has_value()
    }

    fn val_valid(&self) -> bool {
        self.val_cursor.has_value()
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
        if !self.wset.maybe_contains_key(key) {
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
        self.move_val(|val_cursor| val_cursor.move_next());
    }

    fn seek_val(&mut self, val: &V) {
        self.move_val(|val_cursor| unsafe { val_cursor.advance_to_value_or_larger(val) });
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_forward_until(predicate) });
    }

    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_first());
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| key_cursor.move_last());
    }

    fn rewind_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_first());
    }

    fn step_val_reverse(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_prev());
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.move_val(|val_cursor| unsafe { val_cursor.rewind_to_value_or_smaller(val) });
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.move_val(|val_cursor| unsafe { val_cursor.seek_backward_until(predicate) });
    }

    fn fast_forward_vals(&mut self) {
        self.move_val(|val_cursor| val_cursor.move_last());
    }
}

/// A builder for batches from ordered update tuples.
#[derive(SizeOf)]
pub struct FileIndexedWSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FileIndexedWSetFactories<K, V, R>,
    #[size_of(skip)]
    writer: Writer2<K, DynUnit, V, R>,
    weight: Box<R>,
}

impl<K, V, R> Builder<FileIndexedWSet<K, V, R>> for FileIndexedWSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn with_capacity(factories: &FileIndexedWSetFactories<K, V, R>, capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                Runtime::buffer_cache(),
                &*Runtime::storage_backend().unwrap(),
                Runtime::file_writer_parameters(),
                capacity,
            )
            .unwrap(),
            weight: factories.weight_factory().default_box(),
        }
    }

    fn done(self) -> FileIndexedWSet<K, V, R> {
        FileIndexedWSet {
            factories: self.factories,
            file: Arc::new(self.writer.into_reader().unwrap()),
        }
    }

    fn push_key(&mut self, key: &K) {
        self.writer.write0((key, &())).unwrap();
    }

    fn push_val(&mut self, val: &V) {
        self.writer.write1((val, &*self.weight)).unwrap();
    }

    fn push_time_diff(&mut self, _time: &(), weight: &R) {
        debug_assert!(!weight.is_zero());
        weight.clone_to(&mut self.weight);
    }

    fn push_val_diff(&mut self, val: &V, weight: &R) {
        debug_assert!(!weight.is_zero());
        self.writer.write1((val, weight)).unwrap();
    }
}

impl<K, V, R> Archive for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, R, S> Serialize<S> for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, R, D> Deserialize<FileIndexedWSet<K, V, R>, D> for Archived<FileIndexedWSet<K, V, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FileIndexedWSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}
