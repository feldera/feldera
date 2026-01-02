use crate::{
    DBData, DBWeight, NumEntries, Runtime,
    algebra::{AddAssignByRef, AddByRef, NegByRef},
    circuit::metadata::{MetaItem, OperatorMeta},
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WeightTraitTyped, WithFactory,
    },
    storage::{
        buffer_cache::CacheStats,
        file::{
            Factories as FileFactories,
            reader::{Cursor as FileCursor, Error as ReaderError, Reader},
            writer::Writer2,
        },
    },
    trace::{
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder, Cursor,
        FileValBatch, VecIndexedWSetFactories, WeightedItem,
        cursor::Position,
        merge_batches_by_reference,
        ord::{file::UnwrapStorage, merge_batcher::MergeBatcher},
    },
    utils::Tup2,
};
use feldera_storage::{FileReader, StoragePath};
use rand::{Rng, seq::index::sample};
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize, ser::Serializer};
use size_of::SizeOf;
use std::{
    cmp::{Ordering, max},
    fmt::{self, Debug},
    ops::Neg,
    sync::Arc,
};

pub struct FileIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories0: FileFactories<K, DynWeightedPairs<V, R>>,
    factories1: FileFactories<V, R>,
    vrs_factory: &'static dyn Factory<DynWeightedPairs<V, R>>,
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
            vrs_factory: self.vrs_factory,
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
            factories0: FileFactories::new::<KType, LeanVec<Tup2<VType, RType>>>(),
            factories1: FileFactories::new::<VType, RType>(),
            vrs_factory: WithFactory::<LeanVec<Tup2<VType, RType>>>::FACTORY,
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
    file: Arc<
        Reader<(
            &'static K,
            &'static DynWeightedPairs<V, R>,
            (&'static V, &'static R, ()),
        )>,
    >,

    /// Metadata, if we have it.  We only have it if we wrote this file
    /// ourselves; if we opened one, we don't because we can't write it to the
    /// file.
    metadata: Option<Metadata>,
}

#[derive(SizeOf)]
struct Metadata {
    /// Number of values inlined into the first column's auxdata rather than
    /// written in column 1.
    inlined_values: usize,

    /// Estimated number of bytes of bounds that weren't written to the file.
    /// This is `None` if we were able to omit all of them and therefore
    /// couldn't estimate the size of the ones that were omitted.  This is 0 if
    /// all the values were inlined.
    estimated_bounds_omitted_bytes: Option<usize>,
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
            metadata: None,
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
        let mut builder =
            <Self as Batch>::Builder::with_capacity(&self.factories, self.key_count(), 0);

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                unsafe { cursor.val_cursor.aux(&mut cursor.diff) };
                let diff = cursor.diff.neg_by_ref();
                builder.push_val_diff(cursor.val(), diff.erase());
                cursor.step_val();
            }
            builder.push_key(cursor.key());
            cursor.step_key();
        }
        builder.done()
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
        // We don't have an accurate count of the number of updates, because it
        // depends on the number of values in the auxdata in column 0.  But it's
        // at least as many as the larger of the number of rows in each column.
        //
        // We do need to take the max here; if we just take the number of rows
        // in column 1, and it ends up being 0 if all the values are in column 0
        // auxdata, then clients might assume that the batch is empty.
        max(self.file.n_rows(0), self.file.n_rows(1)) as usize
    }

    fn approximate_byte_size(&self) -> usize {
        self.file.byte_size().unwrap_storage() as usize
    }

    fn filter_size(&self) -> usize {
        self.file.filter_size()
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

    /// Metadata for file-based indexed Z-sets.
    ///
    /// This metadata is only available for indexed Z-sets that we wrote, not
    /// for ones that we read from a file, since it isn't saved to storage.
    fn metadata(&self, meta: &mut OperatorMeta) {
        if let Some(metadata) = &self.metadata {
            if let Some(bytes) = metadata.estimated_bounds_omitted_bytes {
                meta.extend(metadata! {
                    "estimated omitted bounds" => MetaItem::bytes(bytes)
                })
            } else {
                meta.extend(metadata! {
                    "batches with all bounds omitted" => MetaItem::Count(1)
                })
            }

            meta.extend(metadata! {
                "inlined values" => MetaItem::Percent {
                    numerator: metadata.inlined_values as u64,
                    denominator: metadata.inlined_values as u64 + self.file.n_rows(1)
                }
            });
        }
    }
}

impl<K, V, R> Batch for FileIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Timed<T: crate::Timestamp> = FileValBatch<K, V, T, R>;
    type Batcher = MergeBatcher<Self>;
    type Builder = FileIndexedWSetBuilder<K, V, R>;

    fn file_reader(&self) -> Option<Arc<dyn FileReader>> {
        self.file.mark_for_checkpoint();
        Some(self.file.file_handle().clone())
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
            metadata: None,
        })
    }
}

type KeyCursor<'s, K, V, R> = FileCursor<
    's,
    K,
    DynWeightedPairs<V, R>,
    (&'static V, &'static R, ()),
    (
        &'static K,
        &'static DynWeightedPairs<V, R>,
        (&'static V, &'static R, ()),
    ),
>;

type ValCursor<'s, K, V, R> = FileCursor<
    's,
    V,
    R,
    (),
    (
        &'static K,
        &'static DynWeightedPairs<V, R>,
        (&'static V, &'static R, ()),
    ),
>;

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

    vals: Box<DynWeightedPairs<V, R>>,
    val_index: Option<usize>,
    val_cursor: ValCursor<'s, K, V, R>,
    diff: Box<R>,
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
            vals: self.wset.factories.vrs_factory.default_box(),
            val_index: self.val_index,
            val_cursor: self.val_cursor.clone(),
            diff: self.weight_factory().default_box(),
        }
    }
}

impl<'s, K, V, R> FileIndexedWSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new(wset: &'s FileIndexedWSet<K, V, R>) -> Self {
        let key_cursor = unsafe { wset.file.rows().first().unwrap_storage() };

        let mut vals = wset.factories.vrs_factory.default_box();
        let val_index =
            if unsafe { key_cursor.aux(&mut *vals) }.is_some_and(|vals| !vals.is_empty()) {
                Some(0)
            } else {
                None
            };
        let val_cursor = unsafe {
            key_cursor
                .next_column()
                .unwrap_storage()
                .first()
                .unwrap_storage()
        };
        Self {
            wset,
            key_cursor,
            vals,
            val_index,
            val_cursor,
            diff: wset.factories.weight_factory().default_box(),
        }
    }

    fn move_key<F>(&mut self, op: F)
    where
        F: Fn(&mut KeyCursor<'s, K, V, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.key_cursor).unwrap_storage();
        self.val_index = if unsafe { self.key_cursor.aux(&mut *self.vals) }
            .is_some_and(|vals| !vals.is_empty())
        {
            Some(0)
        } else {
            self.val_cursor = unsafe {
                self.key_cursor
                    .next_column()
                    .unwrap_storage()
                    .first_with_hint(&self.val_cursor)
                    .unwrap_storage()
            };
            None
        };
    }

    fn move_val<F>(&mut self, op: F)
    where
        F: Fn(&mut ValCursor<'s, K, V, R>) -> Result<(), ReaderError>,
    {
        op(&mut self.val_cursor).unwrap_storage();
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
        self.key_cursor.key().unwrap()
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid());
        if let Some(val_index) = self.val_index {
            self.vals.index(val_index).fst()
        } else {
            self.val_cursor.key().unwrap()
        }
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.val_valid() {
            if let Some(val_index) = self.val_index {
                logic(&(), self.vals.index(val_index).snd());
            } else {
                unsafe { self.val_cursor.aux(&mut self.diff) };
                logic(&(), self.diff.as_ref());
            }
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.val_valid() {
            if let Some(val_index) = self.val_index {
                let (val, diff) = self.vals.index(val_index).split();
                logic(val, diff);
            } else {
                unsafe { self.val_cursor.aux(&mut self.diff) };
                logic(self.val(), self.diff.as_ref());
            }
            self.step_val();
        }
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.val_valid());
        if let Some(val_index) = self.val_index {
            self.vals.index(val_index).snd()
        } else {
            unsafe { self.val_cursor.aux(&mut self.diff) };
            self.diff.as_ref()
        }
    }

    fn key_valid(&self) -> bool {
        self.key_cursor.has_value()
    }

    fn val_valid(&self) -> bool {
        if let Some(val_index) = self.val_index {
            (0..self.vals.len()).contains(&val_index)
        } else {
            self.val_cursor.has_value()
        }
    }

    fn step_key(&mut self) {
        self.move_key(|key_cursor| unsafe { key_cursor.move_next() });
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
        if let Some(val_index) = &mut self.val_index {
            if *val_index < self.vals.len() {
                *val_index += 1;
            }
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.move_next() });
        }
    }

    fn seek_val(&mut self, val: &V) {
        if let Some(val_index) = &mut self.val_index {
            while *val_index < self.vals.len() && self.vals.index(*val_index).fst() < val {
                *val_index += 1;
            }
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.advance_to_value_or_larger(val) });
        }
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        if let Some(val_index) = &mut self.val_index {
            while *val_index < self.vals.len() && !predicate(self.vals.index(*val_index).fst()) {
                *val_index += 1;
            }
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.seek_forward_until(predicate) });
        }
    }

    fn rewind_keys(&mut self) {
        self.move_key(|key_cursor| unsafe { key_cursor.move_first() });
    }

    fn fast_forward_keys(&mut self) {
        self.move_key(|key_cursor| unsafe { key_cursor.move_last() });
    }

    fn rewind_vals(&mut self) {
        if let Some(val_index) = &mut self.val_index {
            *val_index = 0;
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.move_first() });
        }
    }

    fn step_val_reverse(&mut self) {
        if let Some(val_index) = &mut self.val_index {
            if (1..self.vals.len()).contains(val_index) {
                *val_index -= 1;
            } else {
                *val_index = self.vals.len();
            }
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.move_prev() });
        }
    }

    fn seek_val_reverse(&mut self, val: &V) {
        if let Some(val_index) = &mut self.val_index {
            if *val_index < self.vals.len() {
                while self.vals.index(*val_index).fst() > val {
                    if *val_index > 0 {
                        *val_index -= 1;
                    } else {
                        *val_index = self.vals.len();
                        break;
                    }
                }
            }
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.rewind_to_value_or_smaller(val) });
        }
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        if let Some(val_index) = &mut self.val_index {
            if *val_index < self.vals.len() {
                while !predicate(self.vals.index(*val_index).fst()) {
                    if *val_index > 0 {
                        *val_index -= 1;
                    } else {
                        *val_index = self.vals.len();
                        break;
                    }
                }
            }
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.seek_backward_until(predicate) });
        }
    }

    fn fast_forward_vals(&mut self) {
        if let Some(val_index) = &mut self.val_index {
            *val_index = self.vals.len().saturating_sub(1);
        } else {
            self.move_val(|val_cursor| unsafe { val_cursor.move_last() });
        }
    }

    fn position(&self) -> Option<Position> {
        Some(Position {
            total: self.key_cursor.len(),
            offset: self.key_cursor.absolute_position(),
        })
    }
}

/// Maximum number of values to inline with a key.
///
/// [FileIndexedWSet] uses a [layer file] with two columns.  The first column
/// contains keys and, as auxdata, a vector of value/weight pairs.  The second
/// column contains values and, as auxdata, weights.  For any given key in the
/// first column, the value/weight pairs are either inlined or added to the
/// second column.  This constant set the maximum number of values written in
/// the first column; if any key has more values than this, they are written to
/// the second column instead.
///
/// Many indexed Z-sets are used for updating indexes on primary keys.  Those
/// indexed Z-sets will typically have one value (for insertions and deletions)
/// or two values (for updates) per key.  Thus, a value of 1 or 2 here will
/// almost eliminate the contents of the second column in such indexed Z-sets.
///
/// Set this to 0 to disable inlining for writing new indexed Z-sets.  Inline
/// values will still be supported for reading indexed Z-sets.
pub const MAX_INLINE: usize = 2;

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
    writer: Writer2<K, DynWeightedPairs<V, R>, V, R>,
    pairs: Box<DynWeightedPairs<V, R>>,
    weight: Box<R>,
    num_tuples: usize,
    num_vals: usize,
    inlined_values: usize,
}

impl<K, V, R> FileIndexedWSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn add_pair(&mut self) -> bool {
        let num_vals = self.num_vals;
        self.num_vals += 1;
        match num_vals.cmp(&MAX_INLINE) {
            Ordering::Less => true,
            Ordering::Equal => {
                for pair in self.pairs.dyn_iter() {
                    self.writer.write1(pair.split()).unwrap_storage();
                }
                self.pairs.clear();
                false
            }
            Ordering::Greater => false,
        }
    }
}

impl<K, V, R> Builder<FileIndexedWSet<K, V, R>> for FileIndexedWSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn with_capacity(
        factories: &FileIndexedWSetFactories<K, V, R>,
        key_capacity: usize,
        _value_capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            writer: Writer2::new(
                &factories.factories0,
                &factories.factories1,
                Runtime::buffer_cache,
                &*Runtime::storage_backend().unwrap_storage(),
                Runtime::file_writer_parameters(),
                key_capacity,
            )
            .unwrap_storage(),
            weight: factories.weight_factory().default_box(),
            pairs: factories.vrs_factory.default_box(),
            num_tuples: 0,
            num_vals: 0,
            inlined_values: 0,
        }
    }

    fn done(self) -> FileIndexedWSet<K, V, R> {
        let (reader, info) = self.writer.into_reader().unwrap_storage();
        FileIndexedWSet {
            factories: self.factories,
            file: Arc::new(reader),
            metadata: Some(Metadata {
                inlined_values: self.inlined_values,
                estimated_bounds_omitted_bytes: info[1].estimated_omitted_bounds_bytes(),
            }),
        }
    }

    fn push_key(&mut self, key: &K) {
        self.inlined_values += self.pairs.len();
        self.writer.write0((key, &*self.pairs)).unwrap_storage();
        self.pairs.clear();
        self.num_vals = 0;
    }

    fn push_val(&mut self, val: &V) {
        debug_assert!(!self.weight.is_zero());
        self.num_tuples += 1;
        if self.add_pair() {
            self.pairs.push_refs((val, &self.weight));
        } else {
            self.writer.write1((val, &self.weight)).unwrap_storage();
        }
        #[cfg(debug_assertions)]
        self.weight.set_zero();
    }

    fn push_time_diff(&mut self, _time: &(), weight: &R) {
        debug_assert!(!weight.is_zero());
        weight.clone_to(&mut self.weight);
    }

    fn push_val_diff(&mut self, val: &V, weight: &R) {
        debug_assert!(!weight.is_zero());
        self.num_tuples += 1;
        if self.add_pair() {
            self.pairs.push_refs((val, weight));
        } else {
            self.writer.write1((val, weight)).unwrap_storage();
        }
    }

    fn num_keys(&self) -> usize {
        self.writer.n_rows() as usize
    }

    fn num_tuples(&self) -> usize {
        self.num_tuples
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
