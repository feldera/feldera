use super::utils::{copy_to_builder, pick_merge_destination};
use crate::storage::file::SerializerInner;
use crate::storage::file::{FilterKind, FilterStats, TouchedWindowCount};
use crate::{
    DBData, DBWeight, Error, NumEntries, Runtime,
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    circuit::checkpointer::Checkpoint,
    dynamic::{
        DataTrait, DynData, DynDataTyped, DynPair, DynVec, DynWeightedPairs, Erase, Factory,
        WeightTrait, WeightTraitTyped,
    },
    storage::{
        buffer_cache::CacheStats,
        file::reader::{Error as ReaderError, read_metadata},
    },
    trace::{
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder,
        FallbackValBatch, FileIndexedWSet, FileIndexedWSetFactories, Filter, GroupFilter,
        MergeCursor, WeightedItem,
        cursor::{
            Cursor, CursorFactory, DefaultPushCursor, DelegatingCursor, ProjectedValCursor,
            PushCursor, merge_cursor_over,
        },
        deserialize_indexed_wset, merge_batches_by_reference,
        ord::{
            fallback::utils::BuildTo,
            file::indexed_wset_batch::FileIndexedWSetBuilder,
            merge_batcher::MergeBatcher,
            vec::indexed_wset_batch::{VecIndexedWSet, VecIndexedWSetBuilder},
        },
        serialize_indexed_wset,
    },
    utils::Tup2,
};
use feldera_storage::{FileReader, StoragePath};
use rand::Rng;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize, ser::Serializer};
use size_of::SizeOf;
use std::ops::Neg;
use std::{
    fmt::{self, Debug},
    sync::Arc,
};

/// Factories for [`FallbackIndexedWSet`].
///
/// `plain` describes the batch's own value type.  `projected` is present only for
/// a batch that may adopt a value column carrying a hidden trailing column, and
/// describes that wider value type.
pub struct FallbackIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    plain: FileIndexedWSetFactories<K, V, R>,
    projected: Option<FileIndexedWSetFactories<K, DynPair<V, DynData>, R>>,
}

impl<K, V, R> Clone for FallbackIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            plain: self.plain.clone(),
            projected: self.projected.clone(),
        }
    }
}

impl<K, V, R> FallbackIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// Factories for a batch that may adopt a value column carrying a hidden
    /// trailing `u32`.
    pub fn with_projection<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
        Tup2<VType, u32>: DBData + Erase<DynPair<V, DynData>>,
    {
        Self {
            plain: FileIndexedWSetFactories::new::<KType, VType, RType>(),
            projected: Some(FileIndexedWSetFactories::stamped::<
                KType,
                Tup2<VType, u32>,
                RType,
            >()),
        }
    }

    /// Factories for the projected representation, whose values carry a hidden
    /// trailing column.
    ///
    /// # Panics
    ///
    /// Panics unless this bundle came from [`Self::with_projection`].  A batch
    /// only ever holds a projected variant when its factories describe one, so
    /// reaching this on a `None` bundle means a projected batch escaped into a
    /// trace that cannot interpret it.
    pub fn projected(&self) -> &FileIndexedWSetFactories<K, DynPair<V, DynData>, R> {
        self.projected.as_ref().expect(
            "projected batch requires factories built with `FallbackIndexedWSetFactories::with_projection`",
        )
    }

    /// True if this bundle can describe projected batches.
    pub fn has_projection(&self) -> bool {
        self.projected.is_some()
    }

    /// Hides the trailing value column of a projected variant's cursor.
    fn project<C>(&self, inner: C) -> ProjectedValCursor<K, V, DynData, R, C>
    where
        C: Cursor<K, DynPair<V, DynData>, (), R>,
    {
        ProjectedValCursor::new(inner, self.plain.val_factory())
    }
}

impl<K, V, R> BatchReaderFactories<K, V, (), R> for FallbackIndexedWSetFactories<K, V, R>
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
            plain: FileIndexedWSetFactories::new::<KType, VType, RType>(),
            projected: None,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.plain.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.plain.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.plain.val_factory()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.plain.weight_factory()
    }
}

impl<K, V, R> BatchFactories<K, V, (), R> for FallbackIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.plain.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.plain.weighted_item_factory()
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.plain.weighted_items_factory()
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>> {
        self.plain.weighted_vals_factory()
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<()>, R>>> {
        None
    }
}

#[derive(SizeOf)]
pub struct FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackIndexedWSetFactories<K, V, R>,
    inner: Inner<K, V, R>,
}

#[derive(SizeOf)]
#[allow(clippy::large_enum_variant)]
enum Inner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    Vec(VecIndexedWSet<K, V, R>),
    File(FileIndexedWSet<K, V, R>),

    /// In memory, over values that carry a trailing column this batch hides.
    VecProj(VecIndexedWSet<K, DynPair<V, DynData>, R>),

    /// On storage, over values that carry a trailing column this batch hides.
    FileProj(FileIndexedWSet<K, DynPair<V, DynData>, R>),
}

impl<K, V, R> FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// Presents `inner`, whose values carry a trailing column, as a batch over
    /// the leading column alone.
    ///
    /// # Panics
    ///
    /// Panics unless `factories` came from
    /// [`FallbackIndexedWSetFactories::with_projection`].
    pub fn from_projected_vec(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        inner: VecIndexedWSet<K, DynPair<V, DynData>, R>,
    ) -> Self {
        factories.projected();
        Self {
            factories: factories.clone(),
            inner: Inner::VecProj(inner),
        }
    }

    /// [`Self::from_projected_vec`] for a batch that lives on storage.
    ///
    /// # Panics
    ///
    /// Panics unless `factories` came from
    /// [`FallbackIndexedWSetFactories::with_projection`].
    pub fn from_projected_file(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        inner: FileIndexedWSet<K, DynPair<V, DynData>, R>,
    ) -> Self {
        factories.projected();
        Self {
            factories: factories.clone(),
            inner: Inner::FileProj(inner),
        }
    }

    /// Presents `batch`, whose values carry a trailing column, as a batch over
    /// the leading column alone.
    ///
    /// The two spellings describe the same records, so this rewraps the inner
    /// batch rather than copying it.
    ///
    /// # Panics
    ///
    /// Panics unless `factories` came from
    /// [`FallbackIndexedWSetFactories::with_projection`], and on a `batch` that
    /// is already projected, whose values would then carry two trailing columns.
    pub fn project_batch(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        batch: &FallbackIndexedWSet<K, DynPair<V, DynData>, R>,
    ) -> Self {
        match &batch.inner {
            Inner::Vec(vec) => Self::from_projected_vec(factories, vec.clone()),
            Inner::File(file) => Self::from_projected_file(factories, file.clone()),
            Inner::VecProj(_) | Inner::FileProj(_) => {
                panic!("a batch that already hides a trailing column cannot hide another")
            }
        }
    }
}

impl<K, V, R> Debug for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Vec(vec) => vec.fmt(f),
            Inner::File(file) => file.fmt(f),
            Inner::VecProj(vec) => vec.fmt(f),
            Inner::FileProj(file) => file.fmt(f),
        }
    }
}

impl<K, V, R> Clone for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            inner: match &self.inner {
                Inner::Vec(vec) => Inner::Vec(vec.clone()),
                Inner::File(file) => Inner::File(file.clone()),
                Inner::VecProj(vec) => Inner::VecProj(vec.clone()),
                Inner::FileProj(file) => Inner::FileProj(file.clone()),
            },
        }
    }
}

impl<Other, K, V, R> PartialEq<Other> for FallbackIndexedWSet<K, V, R>
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

impl<K, V, R> Eq for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, V, R> NumEntries for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.num_entries_shallow(),
            Inner::Vec(vec) => vec.num_entries_shallow(),
            Inner::FileProj(file) => file.num_entries_shallow(),
            Inner::VecProj(vec) => vec.num_entries_shallow(),
        }
    }

    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.num_entries_deep(),
            Inner::Vec(vec) => vec.num_entries_deep(),
            Inner::FileProj(file) => file.num_entries_deep(),
            Inner::VecProj(vec) => vec.num_entries_deep(),
        }
    }
}

impl<K, V, R> NegByRef for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + ZRingValue + NegByRef + Erase<R>,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            inner: match &self.inner {
                Inner::File(file) => Inner::File(file.neg_by_ref()),
                Inner::Vec(vec) => Inner::Vec(vec.neg_by_ref()),
                Inner::FileProj(file) => Inner::FileProj(file.neg_by_ref()),
                Inner::VecProj(vec) => Inner::VecProj(vec.neg_by_ref()),
            },
        }
    }
}

impl<K, V, R> Neg for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + ZRingValue + NegByRef + Erase<R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        self.neg_by_ref()
    }
}

impl<K, V, R> AddAssignByRef for FallbackIndexedWSet<K, V, R>
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

impl<K, V, R> AddByRef for FallbackIndexedWSet<K, V, R>
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

impl<K, V, R> BatchReader for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn is_empty(&self) -> bool {
        self.approx_len() == 0
    }
    type Factories = FallbackIndexedWSetFactories<K, V, R>;
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s>
        = DelegatingCursor<'s, K, V, (), R>
    where
        V: 's;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        DelegatingCursor(match &self.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
            Inner::VecProj(vec) => Box::new(self.factories.project(vec.cursor())),
            Inner::FileProj(file) => Box::new(self.factories.project(file.cursor())),
        })
    }

    fn push_cursor(
        &self,
    ) -> Box<dyn PushCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        match &self.inner {
            Inner::Vec(vec) => vec.push_cursor(),
            Inner::File(file) => file.push_cursor(),
            Inner::VecProj(vec) => {
                Box::new(DefaultPushCursor::new(self.factories.project(vec.cursor())))
            }
            Inner::FileProj(file) => Box::new(DefaultPushCursor::new(
                self.factories.project(file.cursor()),
            )),
        }
    }

    fn merge_cursor(
        &self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<GroupFilter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        match &self.inner {
            Inner::Vec(vec) => vec.merge_cursor(key_filter, value_filter),
            Inner::File(file) => file.merge_cursor(key_filter, value_filter),
            Inner::VecProj(vec) => merge_cursor_over(
                self.factories.project(vec.cursor()),
                key_filter,
                value_filter,
            ),
            Inner::FileProj(file) => merge_cursor_over(
                self.factories.project(file.cursor()),
                key_filter,
                value_filter,
            ),
        }
    }

    fn consuming_cursor(
        &mut self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<GroupFilter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        // Destructured so the projection can borrow the factories while the
        // match holds `inner` mutably.
        let Self { factories, inner } = self;
        match inner {
            Inner::Vec(vec) => vec.consuming_cursor(key_filter, value_filter),
            Inner::File(file) => file.consuming_cursor(key_filter, value_filter),
            // A consuming cursor is a `MergeCursor`, and the projection wraps a
            // `Cursor`, so these arms read the batch rather than drain it.
            Inner::VecProj(vec) => {
                merge_cursor_over(factories.project(vec.cursor()), key_filter, value_filter)
            }
            Inner::FileProj(file) => {
                merge_cursor_over(factories.project(file.cursor()), key_filter, value_filter)
            }
        }
    }

    #[inline]
    fn approx_key_count(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.approx_key_count(),
            Inner::Vec(vec) => vec.approx_key_count(),
            Inner::FileProj(file) => file.approx_key_count(),
            Inner::VecProj(vec) => vec.approx_key_count(),
        }
    }

    #[inline]
    fn approx_len(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.approx_len(),
            Inner::Vec(vec) => vec.approx_len(),
            Inner::FileProj(file) => file.approx_len(),
            Inner::VecProj(vec) => vec.approx_len(),
        }
    }

    #[inline]
    fn approximate_byte_size(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.approximate_byte_size(),
            Inner::Vec(vec) => vec.approximate_byte_size(),
            Inner::FileProj(file) => file.approximate_byte_size(),
            Inner::VecProj(vec) => vec.approximate_byte_size(),
        }
    }

    #[inline]
    fn membership_filter_stats(&self) -> FilterStats {
        match &self.inner {
            Inner::File(file) => file.membership_filter_stats(),
            Inner::Vec(vec) => vec.membership_filter_stats(),
            Inner::FileProj(file) => file.membership_filter_stats(),
            Inner::VecProj(vec) => vec.membership_filter_stats(),
        }
    }

    fn membership_filter_kind(&self) -> FilterKind {
        match &self.inner {
            Inner::File(file) => file.membership_filter_kind(),
            Inner::Vec(vec) => vec.membership_filter_kind(),
            Inner::FileProj(file) => file.membership_filter_kind(),
            Inner::VecProj(vec) => vec.membership_filter_kind(),
        }
    }

    fn range_filter_stats(&self) -> FilterStats {
        match &self.inner {
            Inner::File(file) => file.range_filter_stats(),
            Inner::Vec(vec) => vec.range_filter_stats(),
            Inner::FileProj(file) => file.range_filter_stats(),
            Inner::VecProj(vec) => vec.range_filter_stats(),
        }
    }

    #[inline]
    fn location(&self) -> BatchLocation {
        match &self.inner {
            Inner::Vec(vec) => vec.location(),
            Inner::File(file) => file.location(),
            Inner::VecProj(vec) => vec.location(),
            Inner::FileProj(file) => file.location(),
        }
    }

    fn cache_stats(&self) -> CacheStats {
        match &self.inner {
            Inner::Vec(vec) => vec.cache_stats(),
            Inner::File(file) => file.cache_stats(),
            Inner::VecProj(vec) => vec.cache_stats(),
            Inner::FileProj(file) => file.cache_stats(),
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        match &self.inner {
            Inner::File(file) => file.sample_keys(rng, sample_size, sample),
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, sample),
            Inner::FileProj(file) => file.sample_keys(rng, sample_size, sample),
            Inner::VecProj(vec) => vec.sample_keys(rng, sample_size, sample),
        }
    }

    async fn fetch<B>(
        &self,
        keys: &B,
    ) -> Option<Box<dyn CursorFactory<Self::Key, Self::Val, Self::Time, Self::R>>>
    where
        B: BatchReader<Key = Self::Key, Time = ()>,
    {
        match &self.inner {
            Inner::Vec(vec) => vec.fetch(keys).await,
            Inner::File(file) => file.fetch(keys).await,
            Inner::VecProj(_) | Inner::FileProj(_) => None,
        }
    }

    fn keys(&self) -> Option<&DynVec<Self::Key>> {
        match &self.inner {
            Inner::Vec(vec) => vec.keys(),
            Inner::File(file) => file.keys(),
            Inner::VecProj(vec) => vec.keys(),
            Inner::FileProj(file) => file.keys(),
        }
    }
}

impl<K, V, R> Batch for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Timed<T: crate::Timestamp> = FallbackValBatch<K, V, T, R>;
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackIndexedWSetBuilder<K, V, R>;

    fn persisted(&self) -> Option<Self> {
        match &self.inner {
            Inner::Vec(vec) => {
                let mut file = FileIndexedWSetBuilder::with_capacity(
                    &self.factories.plain,
                    vec.approx_key_count(),
                    vec.approx_len(),
                );
                copy_to_builder(&mut file, vec.cursor());
                Some(Self {
                    inner: Inner::File(file.done()),
                    factories: self.factories.clone(),
                })
            }
            Inner::VecProj(vec) => {
                // Spilling reads every record anyway, so it writes what a
                // reader of this batch sees rather than what the inner batch
                // holds: the trailing column goes, and with it the runs that
                // only it distinguished.  The result is an ordinary file batch,
                // smaller than the projected one and needing no stamp.
                let mut file = FileIndexedWSetBuilder::with_capacity(
                    &self.factories.plain,
                    vec.approx_key_count(),
                    vec.approx_len(),
                );
                copy_to_builder(&mut file, self.factories.project(vec.cursor()));
                Some(Self {
                    inner: Inner::File(file.done()),
                    factories: self.factories.clone(),
                })
            }
            Inner::File(_) | Inner::FileProj(_) => None,
        }
    }

    fn file_reader(&self) -> Option<Arc<dyn FileReader>> {
        match &self.inner {
            Inner::Vec(vec) => vec.file_reader(),
            Inner::File(file) => file.file_reader(),
            Inner::VecProj(vec) => vec.file_reader(),
            Inner::FileProj(file) => file.file_reader(),
        }
    }

    fn from_path(factories: &Self::Factories, path: &StoragePath) -> Result<Self, ReaderError> {
        // The file records whether its values carry the trailing column, so the
        // layout is known before the factories that decode it are chosen.
        let stamped = read_metadata(Runtime::buffer_cache, &*Runtime::storage_backend()?, path)?
            .value_stamp
            .is_stamped();

        // A stamped file that these factories cannot describe is left to the
        // plain open, which reports the mismatch.
        let inner = if stamped && factories.has_projection() {
            Inner::FileProj(FileIndexedWSet::from_path(factories.projected(), path)?)
        } else {
            Inner::File(FileIndexedWSet::from_path(&factories.plain, path)?)
        };
        Ok(FallbackIndexedWSet {
            factories: factories.clone(),
            inner,
        })
    }

    fn key_bounds(&self) -> Option<(&Self::Key, &Self::Key)> {
        match &self.inner {
            Inner::File(file) => file.key_bounds(),
            Inner::Vec(vec) => vec.key_bounds(),
            Inner::FileProj(file) => file.key_bounds(),
            Inner::VecProj(vec) => vec.key_bounds(),
        }
    }

    fn negative_weight_count(&self) -> Option<u64> {
        match &self.inner {
            Inner::File(file) => file.negative_weight_count(),
            Inner::Vec(vec) => vec.negative_weight_count(),
            Inner::FileProj(file) => file.negative_weight_count(),
            Inner::VecProj(vec) => vec.negative_weight_count(),
        }
    }

    fn touched_window_count(&self) -> TouchedWindowCount {
        match &self.inner {
            Inner::File(file) => file.touched_window_count(),
            Inner::Vec(vec) => vec.touched_window_count(),
            Inner::FileProj(file) => file.touched_window_count(),
            Inner::VecProj(vec) => vec.touched_window_count(),
        }
    }
}

/// A builder for batches from ordered update tuples.
#[derive(SizeOf)]
pub struct FallbackIndexedWSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackIndexedWSetFactories<K, V, R>,
    inner: BuilderInner<K, V, R>,
}

impl<K, V, R> FallbackIndexedWSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// We ran out of the bytes threshold for `BuilderInner::Threshold`. Spill
    /// to storage as `BuilderInner::File`, writing `vec` as the initial
    /// contents.
    fn spill(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        vec: &VecIndexedWSetBuilder<K, V, R, usize>,
    ) -> BuilderInner<K, V, R> {
        let mut file = FileIndexedWSetBuilder::with_capacity(
            &factories.plain,
            vec.num_keys(),
            vec.num_tuples(),
        );
        vec.copy_to_builder(&mut file);
        BuilderInner::File(file)
    }
}

#[derive(SizeOf)]
#[allow(clippy::large_enum_variant)]
enum BuilderInner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// Memory.
    Vec(VecIndexedWSetBuilder<K, V, R, usize>),

    /// Storage.
    File(FileIndexedWSetBuilder<K, V, R>),

    /// Memory, unless we exceed a maximum size.
    Threshold {
        vec: VecIndexedWSetBuilder<K, V, R, usize>,

        /// Number of bytes so far.
        size: usize,

        /// Threshold at which we spill to storage.
        threshold: usize,
    },
}

impl<K, V, R> BuilderInner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        key_capacity: usize,
        value_capacity: usize,
        build_to: BuildTo,
    ) -> Self {
        match build_to {
            BuildTo::Memory => Self::Vec(Self::new_vec(factories, key_capacity, value_capacity)),
            BuildTo::Storage => Self::File(FileIndexedWSetBuilder::with_capacity(
                &factories.plain,
                key_capacity,
                value_capacity,
            )),
            BuildTo::Threshold(bytes) => Self::Threshold {
                vec: Self::new_vec(factories, key_capacity, value_capacity),
                size: 0,
                threshold: bytes,
            },
        }
    }

    fn new_vec(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        key_capacity: usize,
        value_capacity: usize,
    ) -> VecIndexedWSetBuilder<K, V, R, usize> {
        VecIndexedWSetBuilder::with_capacity(
            &factories.plain.vec_indexed_wset_factory,
            key_capacity,
            value_capacity,
        )
    }
}

impl<K, V, R> Builder<FallbackIndexedWSet<K, V, R>> for FallbackIndexedWSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity_in_location(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        key_capacity: usize,
        value_capacity: usize,
        location: Option<BatchLocation>,
    ) -> Self {
        Self {
            factories: factories.clone(),
            inner: BuilderInner::new(
                factories,
                key_capacity,
                value_capacity,
                BuildTo::for_capacity(key_capacity, value_capacity, location),
            ),
        }
    }

    fn for_merge<'a, B, I>(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        batches: I,
        location: Option<BatchLocation>,
    ) -> Self
    where
        B: Batch<Key = K, Val = V, Time = (), R = R>,
        I: IntoIterator<Item = &'a B> + Clone,
    {
        let key_capacity = batches
            .clone()
            .into_iter()
            .map(|b| b.approx_key_count())
            .sum();
        let value_capacity = batches.clone().into_iter().map(|b| b.approx_len()).sum();
        Self {
            factories: factories.clone(),
            inner: match pick_merge_destination(batches.clone(), location) {
                BatchLocation::Memory => BuilderInner::Vec(VecIndexedWSetBuilder::with_capacity(
                    &factories.plain.vec_indexed_wset_factory,
                    key_capacity,
                    value_capacity,
                )),
                BatchLocation::Storage => BuilderInner::File(FileIndexedWSetBuilder::for_merge(
                    &factories.plain,
                    batches,
                    location,
                )),
            },
        }
    }

    fn push_time_diff(&mut self, time: &(), weight: &R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_time_diff(time, weight),
            BuilderInner::File(file) => file.push_time_diff(time, weight),
            BuilderInner::Threshold {
                vec,
                size,
                threshold: _,
            } => {
                *size += weight.size_of().total_bytes();
                vec.push_time_diff(time, weight);
                // We will check the threshold later in push_val[_mut].
            }
        }
    }

    fn push_val(&mut self, val: &V) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val(val),
            BuilderInner::File(file) => file.push_val(val),
            BuilderInner::Threshold {
                vec,
                size,
                threshold,
            } => {
                *size += val.size_of().total_bytes();
                vec.push_val(val);
                if *size >= *threshold {
                    self.inner = Self::spill(&self.factories, vec);
                }
            }
        }
    }

    fn push_key(&mut self, key: &K) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_key(key),
            BuilderInner::File(file) => file.push_key(key),
            BuilderInner::Threshold {
                vec,
                size,
                threshold,
            } => {
                *size += key.size_of().total_bytes();
                vec.push_key(key);
                if *size >= *threshold {
                    self.inner = Self::spill(&self.factories, vec);
                }
            }
        }
    }

    fn push_time_diff_mut(&mut self, time: &mut (), weight: &mut R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_time_diff_mut(time, weight),
            BuilderInner::File(file) => file.push_time_diff_mut(time, weight),
            BuilderInner::Threshold {
                vec,
                size,
                threshold: _,
            } => {
                *size += weight.size_of().total_bytes();
                vec.push_time_diff_mut(time, weight);
                // We will check the threshold later in push_val[_mut].
            }
        }
    }

    fn push_val_mut(&mut self, val: &mut V) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_mut(val),
            BuilderInner::File(file) => file.push_val_mut(val),
            BuilderInner::Threshold {
                vec,
                size,
                threshold,
            } => {
                *size += val.size_of().total_bytes();
                vec.push_val_mut(val);
                if *size >= *threshold {
                    self.inner = Self::spill(&self.factories, vec);
                }
            }
        }
    }

    fn push_key_mut(&mut self, key: &mut K) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_key_mut(key),
            BuilderInner::File(file) => file.push_key_mut(key),
            BuilderInner::Threshold {
                vec,
                size,
                threshold,
            } => {
                *size += key.size_of().total_bytes();
                vec.push_key_mut(key);
                if *size >= *threshold {
                    self.inner = Self::spill(&self.factories, vec);
                }
            }
        }
    }

    fn push_val_diff(&mut self, val: &V, weight: &R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_diff(val, weight),
            BuilderInner::File(file) => file.push_val_diff(val, weight),
            BuilderInner::Threshold {
                vec,
                size,
                threshold,
            } => {
                *size += (val, weight).size_of().total_bytes();
                vec.push_val_diff(val, weight);
                if *size >= *threshold {
                    self.inner = Self::spill(&self.factories, vec);
                }
            }
        }
    }

    fn push_val_diff_mut(&mut self, val: &mut V, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_diff_mut(val, weight),
            BuilderInner::File(file) => file.push_val_diff_mut(val, weight),
            BuilderInner::Threshold {
                vec,
                size,
                threshold,
            } => {
                *size += val.size_of().total_bytes() + weight.size_of().total_bytes();
                vec.push_val_diff_mut(val, weight);
                if *size >= *threshold {
                    self.inner = Self::spill(&self.factories, vec);
                }
            }
        }
    }

    fn reserve(&mut self, additional: usize) {
        match &mut self.inner {
            BuilderInner::Vec(vec) | BuilderInner::Threshold { vec, .. } => vec.reserve(additional),
            BuilderInner::File(file) => file.reserve(additional),
        }
    }

    fn done(self) -> FallbackIndexedWSet<K, V, R> {
        FallbackIndexedWSet {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done()),
                BuilderInner::Vec(vec) | BuilderInner::Threshold { vec, .. } => {
                    Inner::Vec(vec.done())
                }
            },
        }
    }

    fn num_keys(&self) -> usize {
        match &self.inner {
            BuilderInner::Vec(vec) => vec.num_keys(),
            BuilderInner::File(file) => file.num_keys(),
            BuilderInner::Threshold { vec, .. } => vec.num_keys(),
        }
    }

    fn num_tuples(&self) -> usize {
        match &self.inner {
            BuilderInner::Vec(vec) => vec.num_tuples(),
            BuilderInner::File(file) => file.num_tuples(),
            BuilderInner::Threshold { vec, .. } => vec.num_tuples(),
        }
    }
}

impl<K, V, R> Archive for FallbackIndexedWSet<K, V, R>
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

impl<K, V, R, S> Serialize<S> for FallbackIndexedWSet<K, V, R>
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

impl<K, V, R, D> Deserialize<FallbackIndexedWSet<K, V, R>, D>
    for Archived<FallbackIndexedWSet<K, V, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackIndexedWSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}

impl<K, V, R> Checkpoint for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, Error> {
        Ok(serialize_indexed_wset(self, &mut SerializerInner::new()).into_vec())
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), Error> {
        *self = deserialize_indexed_wset(&self.factories, data);
        Ok(())
    }
}
