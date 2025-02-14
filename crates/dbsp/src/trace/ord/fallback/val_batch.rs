use std::fmt::{Display, Formatter};
use std::path::{Path, PathBuf};

use crate::storage::buffer_cache::CacheStats;
use crate::trace::cursor::DelegatingCursor;
use crate::trace::ord::file::val_batch::FileValBuilder;
use crate::trace::ord::vec::val_batch::VecValBuilder;
use crate::trace::{BatchLocation, Bounds, BoundsRef, MergeCursor};
use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait,
    },
    storage::file::reader::Error as ReaderError,
    trace::{
        ord::{
            file::val_batch::FileValMerger, merge_batcher::MergeBatcher,
            vec::val_batch::VecValMerger,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, FileValBatch,
        FileValBatchFactories, Filter, Merger, OrdValBatch, OrdValBatchFactories, WeightedItem,
    },
    DBData, DBWeight, NumEntries, Timestamp,
};
use derive_more::Debug;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::utils::{copy_to_builder, pick_merge_destination, GenericMerger};

pub struct FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    file: FileValBatchFactories<K, V, T, R>,
    vec: OrdValBatchFactories<K, V, T, R>,
}

impl<K, V, T, R> Clone for FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            vec: self.vec.clone(),
        }
    }
}

impl<K, V, T, R> BatchReaderFactories<K, V, T, R> for FallbackValBatchFactories<K, V, T, R>
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
            file: FileValBatchFactories::new::<KType, VType, RType>(),
            vec: OrdValBatchFactories::new::<KType, VType, RType>(),
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.file.key_factory()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.file.keys_factory()
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.file.val_factory()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.file.weight_factory()
    }
}

impl<K, V, T, R> BatchFactories<K, V, T, R> for FallbackValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.file.item_factory()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.file.weighted_item_factory()
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.file.weighted_items_factory()
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>> {
        self.file.weighted_vals_factory()
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>> {
        self.file.time_diffs_factory()
    }
}

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(Debug, SizeOf)]
pub struct FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    #[debug(skip)]
    factories: FallbackValBatchFactories<K, V, T, R>,
    inner: Inner<K, V, T, R>,
}

#[derive(Debug, SizeOf)]
enum Inner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    Vec(OrdValBatch<K, V, T, R>),
    File(FileValBatch<K, V, T, R>),
}

impl<K, V, T, R> Inner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileValBatch<K, V, T, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }

    fn as_vec(&self) -> Option<&OrdValBatch<K, V, T, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
        }
    }
}

impl<K, V, T, R> Clone for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
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

impl<K, V, T, R> NumEntries for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_shallow(),
            Inner::File(file) => file.num_entries_shallow(),
        }
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_deep(),
            Inner::File(file) => file.num_entries_deep(),
        }
    }
}

impl<K, V, T, R> Display for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match &self.inner {
            Inner::Vec(vec) => Display::fmt(vec, f),
            Inner::File(file) => Display::fmt(file, f),
        }
    }
}

impl<K, V, T, R> BatchReader for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FallbackValBatchFactories<K, V, T, R>;
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Cursor<'s> = DelegatingCursor<'s, K, V, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

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

    fn key_count(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.key_count(),
            Inner::File(file) => file.key_count(),
        }
    }

    fn len(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.len(),
            Inner::File(file) => file.len(),
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

    fn bounds(&self) -> BoundsRef<'_, T> {
        match &self.inner {
            Inner::Vec(vec) => vec.bounds(),
            Inner::File(file) => file.bounds(),
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<Self::Key>)
    where
        RG: Rng,
        T: PartialEq<()>,
    {
        match &self.inner {
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, output),
            Inner::File(file) => file.sample_keys(rng, sample_size, output),
        }
    }
}

impl<K, V, T, R> Batch for FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackValBuilder<K, V, T, R>;
    type Merger = FallbackValMerger<K, V, T, R>;

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        FallbackValMerger::new_merger(self, other, dst_hint)
    }

    fn persisted(&self) -> Option<Self> {
        match &self.inner {
            Inner::Vec(vec) => {
                let mut file = FileValBuilder::with_capacity(&self.factories.file, 0);
                copy_to_builder(&mut file, vec.cursor());
                Some(Self {
                    inner: Inner::File(file.done_with_bounds(vec.bounds().to_owned())),
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
            inner: Inner::File(FileValBatch::<K, V, T, R>::from_path(
                &factories.file,
                path,
            )?),
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FallbackValMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackValBatchFactories<K, V, T, R>,
    inner: MergerInner<K, V, T, R>,
}

#[derive(SizeOf)]
enum MergerInner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    AllFile(FileValMerger<K, V, T, R>),
    AllVec(VecValMerger<K, V, T, R>),
    ToVec(GenericMerger<K, V, T, R, OrdValBatch<K, V, T, R>>),
    ToFile(GenericMerger<K, V, T, R, FileValBatch<K, V, T, R>>),
}

impl<K, V, T, R> Merger<K, V, T, R, FallbackValBatch<K, V, T, R>> for FallbackValMerger<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(
        batch1: &FallbackValBatch<K, V, T, R>,
        batch2: &FallbackValBatch<K, V, T, R>,
        dst_hint: Option<BatchLocation>,
    ) -> Self {
        FallbackValMerger {
            factories: batch1.factories.clone(),
            inner: match (
                pick_merge_destination([batch1, batch2], dst_hint),
                &batch1.inner,
                &batch2.inner,
            ) {
                (BatchLocation::Memory, Inner::Vec(vec1), Inner::Vec(vec2)) => {
                    MergerInner::AllVec(VecValMerger::new_merger(vec1, vec2, dst_hint))
                }
                (BatchLocation::Memory, _, _) => MergerInner::ToVec(GenericMerger::new(
                    &batch1.factories.vec,
                    Some(batch1.factories.file.timediff_factory),
                    batch1,
                    batch2,
                )),
                (BatchLocation::Storage, Inner::File(file1), Inner::File(file2)) => {
                    MergerInner::AllFile(FileValMerger::new_merger(file1, file2, dst_hint))
                }
                (BatchLocation::Storage, _, _) => MergerInner::ToFile(GenericMerger::new(
                    &batch1.factories.file,
                    Some(batch1.factories.file.timediff_factory),
                    batch1,
                    batch2,
                )),
            },
        }
    }

    fn done(self) -> FallbackValBatch<K, V, T, R> {
        FallbackValBatch {
            factories: self.factories.clone(),
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
        source1: &FallbackValBatch<K, V, T, R>,
        source2: &FallbackValBatch<K, V, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        frontier: &T,
        fuel: &mut isize,
    ) {
        match &mut self.inner {
            MergerInner::AllFile(merger) => merger.work(
                source1.inner.as_file().unwrap(),
                source2.inner.as_file().unwrap(),
                key_filter,
                value_filter,
                frontier,
                fuel,
            ),
            MergerInner::AllVec(merger) => merger.work(
                source1.inner.as_vec().unwrap(),
                source2.inner.as_vec().unwrap(),
                key_filter,
                value_filter,
                frontier,
                fuel,
            ),
            MergerInner::ToVec(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
            },
            MergerInner::ToFile(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, frontier, fuel)
                }
            },
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FallbackValBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackValBatchFactories<K, V, T, R>,
    inner: BuilderInner<K, V, T, R>,
}

#[derive(SizeOf)]
enum BuilderInner<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    Vec(VecValBuilder<K, V, T, R, usize>),
    File(FileValBuilder<K, V, T, R>),
}

impl<K, V, T, R> Builder<FallbackValBatch<K, V, T, R>> for FallbackValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(factories: &FallbackValBatchFactories<K, V, T, R>, capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            inner: BuilderInner::Vec(VecValBuilder::with_capacity(&factories.vec, capacity)),
        }
    }

    fn for_merge<'a, B, I>(
        factories: &FallbackValBatchFactories<K, V, T, R>,
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
                    BuilderInner::Vec(VecValBuilder::with_capacity(&factories.vec, cap))
                }
                BatchLocation::Storage => {
                    BuilderInner::File(FileValBuilder::with_capacity(&factories.file, cap))
                }
            },
        }
    }

    fn push_time_diff(&mut self, time: &T, weight: &R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_time_diff(time, weight),
            BuilderInner::File(file) => file.push_time_diff(time, weight),
        }
    }

    fn push_val(&mut self, val: &V) {
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

    fn push_time_diff_mut(&mut self, time: &mut T, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_time_diff_mut(time, weight),
            BuilderInner::File(file) => file.push_time_diff_mut(time, weight),
        }
    }

    fn push_val_mut(&mut self, val: &mut V) {
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

    fn push_val_diff(&mut self, val: &V, weight: &R)
    where
        T: PartialEq<()>,
    {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_diff(val, weight),
            BuilderInner::File(file) => file.push_val_diff(val, weight),
        }
    }

    fn push_val_diff_mut(&mut self, val: &mut V, weight: &mut R)
    where
        T: PartialEq<()>,
    {
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

    fn done_with_bounds(self, bounds: Bounds<T>) -> FallbackValBatch<K, V, T, R> {
        FallbackValBatch {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done_with_bounds(bounds)),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done_with_bounds(bounds)),
            },
        }
    }
}

impl<K, V, T, R> Archive for FallbackValBatch<K, V, T, R>
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

impl<K, V, T, R, S> Serialize<S> for FallbackValBatch<K, V, T, R>
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

impl<K, V, T, R, D> Deserialize<FallbackValBatch<K, V, T, R>, D>
    for Archived<FallbackValBatch<K, V, T, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackValBatch<K, V, T, R>, D::Error> {
        unimplemented!();
    }
}
