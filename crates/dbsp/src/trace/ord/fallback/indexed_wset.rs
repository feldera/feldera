use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    circuit::checkpointer::Checkpoint,
    dynamic::{DataTrait, DynVec, Erase, WeightTrait, WeightTraitTyped},
    storage::{buffer_cache::CacheStats, file::reader::Error as ReaderError},
    trace::{
        cursor::{CursorFactory, DelegatingCursor, PushCursor},
        deserialize_indexed_wset, merge_batches_by_reference,
        ord::{
            fallback::utils::BuildTo,
            file::indexed_wset_batch::FileIndexedWSetBuilder,
            merge_batcher::MergeBatcher,
            vec::indexed_wset_batch::{VecIndexedWSet, VecIndexedWSetBuilder},
        },
        serialize_indexed_wset, Batch, BatchLocation, BatchReader, Builder, FileIndexedWSet,
        FileIndexedWSetFactories, Filter, MergeCursor,
    },
    DBWeight, Error, NumEntries,
};
use feldera_storage::StoragePath;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug};
use std::ops::Neg;

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

    fn push_cursor(
        &self,
    ) -> Box<dyn PushCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_> {
        match &self.inner {
            Inner::Vec(vec) => vec.push_cursor(),
            Inner::File(file) => file.push_cursor(),
        }
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
        }
    }

    fn keys(&self) -> Option<&DynVec<Self::Key>> {
        match &self.inner {
            Inner::Vec(vec) => vec.keys(),
            Inner::File(file) => file.keys(),
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

    fn checkpoint_path(&self) -> Option<StoragePath> {
        match &self.inner {
            Inner::Vec(vec) => vec.checkpoint_path(),
            Inner::File(file) => file.checkpoint_path(),
        }
    }

    fn from_path(factories: &Self::Factories, path: &StoragePath) -> Result<Self, ReaderError> {
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
        let mut file = FileIndexedWSetBuilder::with_capacity(factories, 0);
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
        capacity: usize,
        build_to: BuildTo,
    ) -> Self {
        match build_to {
            BuildTo::Memory => Self::Vec(Self::new_vec(factories, capacity)),
            BuildTo::Storage => {
                Self::File(FileIndexedWSetBuilder::with_capacity(factories, capacity))
            }
            BuildTo::Threshold(bytes) => Self::Threshold {
                vec: Self::new_vec(factories, capacity),
                size: 0,
                threshold: bytes,
            },
        }
    }

    fn new_vec(
        factories: &FallbackIndexedWSetFactories<K, V, R>,
        capacity: usize,
    ) -> VecIndexedWSetBuilder<K, V, R, usize> {
        VecIndexedWSetBuilder::with_capacity(&factories.vec_indexed_wset_factory, capacity)
    }
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
            inner: BuilderInner::new(factories, capacity, BuildTo::for_capacity(capacity)),
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
        Self {
            factories: factories.clone(),
            inner: BuilderInner::new(
                factories,
                batches.clone().into_iter().map(|b| b.len()).sum(),
                pick_merge_destination(batches, location).into(),
            ),
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
        Ok(serialize_indexed_wset(self))
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), Error> {
        *self = deserialize_indexed_wset(&self.factories, data);
        Ok(())
    }
}
