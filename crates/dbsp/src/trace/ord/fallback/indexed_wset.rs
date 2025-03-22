use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    dynamic::{DataTrait, DynVec, Erase, WeightTrait, WeightTraitTyped},
    storage::{buffer_cache::CacheStats, file::reader::Error as ReaderError},
    trace::{
        cursor::DelegatingCursor,
        merge_batches_by_reference,
        ord::{
            file::indexed_wset_batch::FileIndexedWSetBuilder,
            merge_batcher::MergeBatcher,
            vec::indexed_wset_batch::{VecIndexedWSet, VecIndexedWSetBuilder},
        },
        Batch, BatchLocation, BatchReader, Builder, FileIndexedWSet, FileIndexedWSetFactories,
        Filter, MergeCursor,
    },
    DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug};
use std::path::Path;
use std::{ops::Neg, path::PathBuf};

use super::utils::{copy_to_builder, pick_merge_destination};

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

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        match &self.inner {
            Inner::File(file) => file.sample_keys(rng, sample_size, sample),
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, sample),
        }
    }

    fn maybe_contains_key(&self, key: &Self::Key) -> bool {
        match &self.inner {
            Inner::Vec(vec) => vec.maybe_contains_key(key),
            Inner::File(file) => file.maybe_contains_key(key),
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

    fn for_merge<'a, B, I>(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
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

    fn done(self) -> FallbackIndexedWSet<K, V, R> {
        FallbackIndexedWSet {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done()),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done()),
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
