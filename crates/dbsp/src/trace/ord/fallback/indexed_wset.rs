use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    dynamic::{DataTrait, DynVec, Erase, WeightTrait, WeightTraitTyped},
    storage::{buffer_cache::CacheStats, file::reader::Error as ReaderError},
    time::{Antichain, AntichainRef},
    trace::{
        cursor::DelegatingCursor,
        ord::{
            file::indexed_wset_batch::{FileIndexedWSetBuilder, FileIndexedWSetMerger},
            merge_batcher::MergeBatcher,
            vec::indexed_wset_batch::{
                VecIndexedWSet, VecIndexedWSetBuilder, VecIndexedWSetMerger,
            },
        },
        Batch, BatchLocation, BatchReader, Builder, FileIndexedWSet, FileIndexedWSetFactories,
        Filter, Merger,
    },
    DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::path::Path;
use std::{
    fmt::{self, Debug},
    ops::Deref,
};
use std::{ops::Neg, path::PathBuf};

use super::utils::{copy_to_builder, pick_merge_destination, GenericMerger};

pub type FallbackIndexedWSetFactories<K, V, R> = FileIndexedWSetFactories<K, V, R>;

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
}

impl<K, V, R> Inner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileIndexedWSet<K, V, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }
    fn as_vec(&self) -> Option<&VecIndexedWSet<K, V, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
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
        }
    }

    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::File(file) => file.num_entries_deep(),
            Inner::Vec(vec) => vec.num_entries_deep(),
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
            *self = self.merge(rhs, &None, &None);
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
        self.merge(rhs, &None, &None)
    }
}

impl<K, V, R> BatchReader for FallbackIndexedWSet<K, V, R>
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
        })
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

    #[inline]
    fn lower(&self) -> AntichainRef<'_, ()> {
        AntichainRef::new(&[()])
    }

    #[inline]
    fn upper(&self) -> AntichainRef<'_, ()> {
        AntichainRef::empty()
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

impl<K, V, R> Batch for FallbackIndexedWSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackIndexedWSetBuilder<K, V, R>;
    type Merger = FallbackIndexedWSetMerger<K, V, R>;

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        FallbackIndexedWSetMerger::new_merger(self, other, dst_hint)
    }

    fn persisted(&self) -> Option<Self> {
        match &self.inner {
            Inner::Vec(vec) => {
                let mut file = FileIndexedWSetBuilder::with_capacity(&self.factories, 0);
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
        Ok(FallbackIndexedWSet {
            factories: factories.clone(),
            inner: Inner::File(FileIndexedWSet::from_path(factories, path)?),
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FallbackIndexedWSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackIndexedWSetFactories<K, V, R>,
    inner: MergerInner<K, V, R>,
}

#[derive(SizeOf)]
enum MergerInner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    AllFile(FileIndexedWSetMerger<K, V, R>),
    AllVec(VecIndexedWSetMerger<K, V, R>),
    ToVec(GenericMerger<K, V, (), R, VecIndexedWSet<K, V, R>>),
    ToFile(GenericMerger<K, V, (), R, FileIndexedWSet<K, V, R>>),
}

impl<K, V, R> Merger<K, V, (), R, FallbackIndexedWSet<K, V, R>>
    for FallbackIndexedWSetMerger<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_merger(
        batch1: &FallbackIndexedWSet<K, V, R>,
        batch2: &FallbackIndexedWSet<K, V, R>,
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
                    MergerInner::AllVec(VecIndexedWSetMerger::new_merger(vec1, vec2, dst_hint))
                }
                (BatchLocation::Memory, _, _) => MergerInner::ToVec(GenericMerger::new(
                    &batch1.factories.vec_indexed_wset_factory,
                    None,
                    batch1,
                    batch2,
                )),
                (BatchLocation::Storage, Inner::File(file1), Inner::File(file2)) => {
                    MergerInner::AllFile(FileIndexedWSetMerger::new_merger(file1, file2, dst_hint))
                }
                (BatchLocation::Storage, _, _) => {
                    MergerInner::ToFile(GenericMerger::new(&batch1.factories, None, batch1, batch2))
                }
            },
        }
    }

    #[inline]
    fn done(self) -> FallbackIndexedWSet<K, V, R> {
        FallbackIndexedWSet {
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
        source1: &FallbackIndexedWSet<K, V, R>,
        source2: &FallbackIndexedWSet<K, V, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
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

#[derive(SizeOf)]
#[allow(clippy::large_enum_variant)]
enum BuilderInner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    Vec(VecIndexedWSetBuilder<K, V, R, usize>),
    File(FileIndexedWSetBuilder<K, V, R>),
}

impl<K, V, R> Builder<FallbackIndexedWSet<K, V, R>> for FallbackIndexedWSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(factories: &FallbackIndexedWSetFactories<K, V, R>, capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            inner: BuilderInner::Vec(VecIndexedWSetBuilder::with_capacity(
                &factories.vec_indexed_wset_factory,
                capacity,
            )),
        }
    }

    fn for_merge<B, AR>(factories: &FallbackIndexedWSetFactories<K, V, R>, batches: &[AR]) -> Self
    where
        B: BatchReader,
        AR: Deref<Target = B>,
    {
        let cap = batches.iter().map(|b| b.deref().len()).sum();
        Self {
            factories: factories.clone(),
            inner: match pick_merge_destination(batches.iter().map(Deref::deref), None) {
                BatchLocation::Memory => BuilderInner::Vec(VecIndexedWSetBuilder::with_capacity(
                    &factories.vec_indexed_wset_factory,
                    cap,
                )),
                BatchLocation::Storage => {
                    BuilderInner::File(FileIndexedWSetBuilder::with_capacity(factories, cap))
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

    fn push_time_diff_mut(&mut self, time: &mut (), weight: &mut R) {
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

    fn push_val_diff(&mut self, val: &V, weight: &R) {
        match &mut self.inner {
            BuilderInner::Vec(vec) => vec.push_val_diff(val, weight),
            BuilderInner::File(file) => file.push_val_diff(val, weight),
        }
    }

    fn push_val_diff_mut(&mut self, val: &mut V, weight: &mut R) {
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

    fn done_with_bounds(
        self,
        bounds: (Antichain<()>, Antichain<()>),
    ) -> FallbackIndexedWSet<K, V, R> {
        FallbackIndexedWSet {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done_with_bounds(bounds)),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done_with_bounds(bounds)),
            },
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
