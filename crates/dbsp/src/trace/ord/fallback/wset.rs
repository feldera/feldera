use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    circuit::checkpointer::Checkpoint,
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        WeightTrait, WeightTraitTyped,
    },
    storage::{buffer_cache::CacheStats, file::reader::Error as ReaderError},
    trace::{
        cursor::DelegatingCursor,
        deserialize_wset, merge_batches_by_reference,
        ord::{
            file::wset_batch::{FileWSetBuilder, FileWSetMerger},
            merge_batcher::MergeBatcher,
            vec::wset_batch::{VecWSet, VecWSetBuilder, VecWSetFactories, VecWSetMerger},
        },
        serialize_wset, Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories,
        Bounds, BoundsRef, Builder, FileWSet, FileWSetFactories, Filter, MergeCursor, Merger,
        WeightedItem,
    },
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug},
    path::Path,
};
use std::{ops::Neg, path::PathBuf};

use super::utils::{copy_to_builder, pick_merge_destination, GenericMerger};

pub struct FallbackWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    file: FileWSetFactories<K, R>,
    vec: VecWSetFactories<K, R>,
}

impl<K, R> Clone for FallbackWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            vec: self.vec.clone(),
        }
    }
}

impl<K, R> BatchReaderFactories<K, DynUnit, (), R> for FallbackWSetFactories<K, R>
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
            file: FileWSetFactories::new::<KType, VType, RType>(),
            vec: VecWSetFactories::new::<KType, VType, RType>(),
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.file.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.file.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        self.file.val_factory()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.file.weight_factory()
    }
}

impl<K, R> BatchFactories<K, DynUnit, (), R> for FallbackWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.file.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.file.weighted_item_factory()
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.file.weighted_items_factory()
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynUnit, R>> {
        self.file.weighted_vals_factory()
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<()>, R>>> {
        None
    }
}

#[derive(SizeOf)]
pub struct FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackWSetFactories<K, R>,
    inner: Inner<K, R>,
}

impl<K, R> FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    pub fn is_empty(&self) -> bool {
        match &self.inner {
            Inner::Vec(vec) => vec.is_empty(),
            Inner::File(file) => file.is_empty(),
        }
    }
}

#[derive(SizeOf)]
#[allow(clippy::large_enum_variant)]
enum Inner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    Vec(VecWSet<K, R>),
    File(FileWSet<K, R>),
}

impl<K, R> Inner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileWSet<K, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }
    fn as_vec(&self) -> Option<&VecWSet<K, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
        }
    }
}

impl<K, R> Debug for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Vec(vec) => vec.fmt(f),
            Inner::File(file) => file.fmt(f),
        }
    }
}

impl<K, R> Clone for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            inner: match &self.inner {
                Inner::Vec(vec) => Inner::Vec(vec.clone()),
                Inner::File(file) => Inner::File(file.clone()),
            },
        }
    }
}

impl<Other, K, R> PartialEq<Other> for FallbackWSet<K, R>
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

impl<K, R> Eq for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, R> NumEntries for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.num_entries_shallow(),
            Inner::Vec(vec) => vec.num_entries_shallow(),
        }
    }

    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.num_entries_deep(),
            Inner::Vec(vec) => vec.num_entries_deep(),
        }
    }
}

impl<K, R> NegByRef for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
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
            },
        }
    }
}

impl<K, R> Neg for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + ZRingValue + NegByRef + Erase<R>,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        self.neg_by_ref()
    }
}

impl<K, R> AddAssignByRef for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        if !rhs.is_empty() {
            *self = merge_batches_by_reference(&self.factories, [self as &Self, rhs], &None, &None);
        }
    }
}

impl<K, R> AddByRef for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        merge_batches_by_reference(&self.factories, [self, rhs], &None, &None)
    }
}

impl<K, R> BatchReader for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FallbackWSetFactories<K, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = ();
    type R = R;
    type Cursor<'s> = DelegatingCursor<'s, K, DynUnit, (), R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        DelegatingCursor(match &self.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
        })
    }

    fn merge_cursor(
        &self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        match &self.inner {
            Inner::Vec(vec) => vec.merge_cursor(key_filter, value_filter),
            Inner::File(file) => file.merge_cursor(key_filter, value_filter),
        }
    }

    fn consuming_cursor(
        &mut self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        match &mut self.inner {
            Inner::Vec(vec) => vec.consuming_cursor(key_filter, value_filter),
            Inner::File(file) => file.consuming_cursor(key_filter, value_filter),
        }
    }

    #[inline]
    fn key_count(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.key_count(),
            Inner::Vec(vec) => vec.key_count(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.len(),
            Inner::Vec(vec) => vec.len(),
        }
    }

    #[inline]
    fn approximate_byte_size(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.approximate_byte_size(),
            Inner::Vec(vec) => vec.approximate_byte_size(),
        }
    }

    #[inline]
    fn location(&self) -> BatchLocation {
        match &self.inner {
            Inner::Vec(vec) => vec.location(),
            Inner::File(file) => file.location(),
        }
    }

    fn cache_stats(&self) -> CacheStats {
        match &self.inner {
            Inner::Vec(vec) => vec.cache_stats(),
            Inner::File(file) => file.cache_stats(),
        }
    }

    fn bounds(&self) -> BoundsRef<'_, ()> {
        BoundsRef::empty()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        match &self.inner {
            Inner::File(file) => file.sample_keys(rng, sample_size, sample),
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, sample),
        }
    }
}

impl<K, R> Batch for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackWSetBuilder<K, R>;
    type Merger = FallbackWSetMerger<K, R>;

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        FallbackWSetMerger::new_merger(self, other, dst_hint)
    }

    fn persisted(&self) -> Option<Self> {
        match &self.inner {
            Inner::Vec(vec) => {
                let mut file = FileWSetBuilder::with_capacity(&self.factories.file, 0);
                copy_to_builder(&mut file, vec.cursor());
                Some(Self {
                    inner: Inner::File(file.done()),
                    factories: self.factories.clone(),
                })
            }
            Inner::File(_) => None,
        }
    }

    fn checkpoint_path(&self) -> Option<PathBuf> {
        match &self.inner {
            Inner::Vec(vec) => vec.checkpoint_path(),
            Inner::File(file) => file.checkpoint_path(),
        }
    }

    fn from_path(factories: &Self::Factories, path: &Path) -> Result<Self, ReaderError> {
        Ok(Self {
            factories: factories.clone(),
            inner: Inner::File(FileWSet::<K, R>::from_path(&factories.file, path)?),
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FallbackWSetMerger<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackWSetFactories<K, R>,
    inner: MergerInner<K, R>,
}

#[derive(SizeOf)]
enum MergerInner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    AllFile(FileWSetMerger<K, R>),
    AllVec(VecWSetMerger<K, R>),
    ToVec(GenericMerger<K, DynUnit, (), R, VecWSet<K, R>>),
    ToFile(GenericMerger<K, DynUnit, (), R, FileWSet<K, R>>),
}

impl<K, R> Merger<K, DynUnit, (), R, FallbackWSet<K, R>> for FallbackWSetMerger<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_merger(
        batch1: &FallbackWSet<K, R>,
        batch2: &FallbackWSet<K, R>,
        dst_hint: Option<BatchLocation>,
    ) -> Self {
        Self {
            factories: batch1.factories.clone(),
            inner: match (
                pick_merge_destination([batch1, batch2], dst_hint),
                &batch1.inner,
                &batch2.inner,
            ) {
                (BatchLocation::Memory, Inner::Vec(vec1), Inner::Vec(vec2)) => {
                    MergerInner::AllVec(VecWSetMerger::new_merger(vec1, vec2, dst_hint))
                }
                (BatchLocation::Memory, _, _) => MergerInner::ToVec(GenericMerger::new(
                    &batch1.factories.vec,
                    None,
                    batch1,
                    batch2,
                )),
                (BatchLocation::Storage, Inner::File(file1), Inner::File(file2)) => {
                    MergerInner::AllFile(FileWSetMerger::new_merger(file1, file2, dst_hint))
                }
                (BatchLocation::Storage, _, _) => MergerInner::ToFile(GenericMerger::new(
                    &batch1.factories.file,
                    None,
                    batch1,
                    batch2,
                )),
            },
        }
    }

    #[inline]
    fn done(self) -> FallbackWSet<K, R> {
        FallbackWSet {
            factories: self.factories,
            inner: match self.inner {
                MergerInner::AllFile(merger) => Inner::File(merger.done()),
                MergerInner::AllVec(merger) => Inner::Vec(merger.done()),
                MergerInner::ToVec(merger) => Inner::Vec(merger.done()),
                MergerInner::ToFile(merger) => Inner::File(merger.done()),
            },
        }
    }

    fn work(
        &mut self,
        source1: &FallbackWSet<K, R>,
        source2: &FallbackWSet<K, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<DynUnit>>,
        _frontier: &(),
        fuel: &mut isize,
    ) {
        match &mut self.inner {
            MergerInner::AllFile(merger) => merger.work(
                source1.inner.as_file().unwrap(),
                source2.inner.as_file().unwrap(),
                key_filter,
                value_filter,
                &(),
                fuel,
            ),
            MergerInner::AllVec(merger) => merger.work(
                source1.inner.as_vec().unwrap(),
                source2.inner.as_vec().unwrap(),
                key_filter,
                value_filter,
                &(),
                fuel,
            ),
            MergerInner::ToVec(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
            },
            MergerInner::ToFile(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, &(), fuel)
                }
            },
        }
    }
}

/// A builder for batches from ordered update tuples.
#[derive(SizeOf)]
pub struct FallbackWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackWSetFactories<K, R>,
    inner: BuilderInner<K, R>,
}

#[derive(SizeOf)]
#[allow(clippy::large_enum_variant)]
enum BuilderInner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    Vec(VecWSetBuilder<K, R>),
    File(FileWSetBuilder<K, R>),
}

impl<K, R> Builder<FallbackWSet<K, R>> for FallbackWSetBuilder<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(factories: &FallbackWSetFactories<K, R>, capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            inner: BuilderInner::Vec(VecWSetBuilder::with_capacity(&factories.vec, capacity)),
        }
    }

    fn for_merge<'a, B, I>(
        factories: &FallbackWSetFactories<K, R>,
        batches: I,
        location: Option<BatchLocation>,
    ) -> Self
    where
        B: BatchReader,
        I: IntoIterator<Item = &'a B> + Clone,
    {
        let cap = batches.clone().into_iter().map(|b| b.len()).sum();
        Self {
            factories: factories.clone(),
            inner: match pick_merge_destination(batches, location) {
                BatchLocation::Memory => {
                    BuilderInner::Vec(VecWSetBuilder::with_capacity(&factories.vec, cap))
                }
                BatchLocation::Storage => {
                    BuilderInner::File(FileWSetBuilder::with_capacity(&factories.file, cap))
                }
            },
        }
    }

    fn push_time_diff(&mut self, time: &(), weight: &R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_time_diff(time, weight),
            BuilderInner::File(file) => file.push_time_diff(time, weight),
        }
    }

    fn push_val(&mut self, val: &DynUnit) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val(val),
            BuilderInner::File(file) => file.push_val(val),
        }
    }

    fn push_key(&mut self, key: &K) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_key(key),
            BuilderInner::File(file) => file.push_key(key),
        }
    }

    fn push_time_diff_mut(&mut self, time: &mut (), weight: &mut R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_time_diff_mut(time, weight),
            BuilderInner::File(file) => file.push_time_diff_mut(time, weight),
        }
    }

    fn push_val_mut(&mut self, val: &mut DynUnit) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_mut(val),
            BuilderInner::File(file) => file.push_val_mut(val),
        }
    }

    fn push_key_mut(&mut self, key: &mut K) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_key_mut(key),
            BuilderInner::File(file) => file.push_key_mut(key),
        }
    }

    fn push_val_diff(&mut self, val: &DynUnit, weight: &R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_diff(val, weight),
            BuilderInner::File(file) => file.push_val_diff(val, weight),
        }
    }

    fn push_val_diff_mut(&mut self, val: &mut DynUnit, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_diff_mut(val, weight),
            BuilderInner::File(file) => file.push_val_diff_mut(val, weight),
        }
    }

    fn reserve(&mut self, additional: usize) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.reserve(additional),
            BuilderInner::File(file) => file.reserve(additional),
        }
    }

    fn done_with_bounds(self, bounds: Bounds<()>) -> FallbackWSet<K, R> {
        FallbackWSet {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done_with_bounds(bounds)),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done_with_bounds(bounds)),
            },
        }
    }
}

impl<K, R> Archive for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, R, S> Serialize<S> for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, R, D> Deserialize<FallbackWSet<K, R>, D> for Archived<FallbackWSet<K, R>>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackWSet<K, R>, D::Error> {
        unimplemented!();
    }
}

impl<K, R> Checkpoint for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, crate::Error> {
        Ok(serialize_wset(self))
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), crate::Error> {
        self.inner = Inner::Vec(deserialize_wset(&self.factories.vec, data));
        Ok(())
    }
}
