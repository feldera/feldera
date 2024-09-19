use crate::{
    dynamic::{DataTrait, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait},
    storage::file::reader::Error as ReaderError,
    time::AntichainRef,
    trace::{
        cursor::DelegatingCursor,
        ord::{
            file::key_batch::{FileKeyBuilder, FileKeyMerger},
            merge_batcher::MergeBatcher,
            vec::key_batch::{VecKeyBuilder, VecKeyMerger},
            FileKeyBatch, OrdKeyBatch,
        },
        Batch, BatchFactories, BatchLocation, BatchReader, BatchReaderFactories, Builder,
        FileKeyBatchFactories, Filter, Merger, OrdKeyBatchFactories, TimedBuilder, WeightedItem,
    },
    DBData, DBWeight, NumEntries, Timestamp,
};
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug},
    mem::replace,
    path::{Path, PathBuf},
};

use super::utils::{copy_to_builder, pick_merge_destination, BuildTo, GenericMerger};

pub struct FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    file: FileKeyBatchFactories<K, T, R>,
    vec: OrdKeyBatchFactories<K, T, R>,
}

impl<K, T, R> Clone for FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
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

impl<K, T, R> BatchReaderFactories<K, DynUnit, T, R> for FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            file: FileKeyBatchFactories::new::<KType, VType, RType>(),
            vec: OrdKeyBatchFactories::new::<KType, VType, RType>(),
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

impl<K, T, R> BatchFactories<K, DynUnit, T, R> for FallbackKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
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
}

/// A batch of keys with weights and times.
///
/// Each tuple in `FallbackKeyBatch<K, T, R>` has key type `K`, value type `()`,
/// weight type `R`, and time type `R`.
#[derive(SizeOf)]
pub struct FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackKeyBatchFactories<K, T, R>,
    inner: Inner<K, T, R>,
}

#[derive(SizeOf)]
enum Inner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    Vec(OrdKeyBatch<K, T, R>),
    File(FileKeyBatch<K, T, R>),
}

impl<K, T, R> Inner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileKeyBatch<K, T, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }
    fn as_vec(&self) -> Option<&OrdKeyBatch<K, T, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
        }
    }
}

impl<K, T, R> Debug for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Inner::Vec(vec) => vec.fmt(f),
            Inner::File(file) => file.fmt(f),
        }
    }
}

impl<K, T, R> Clone for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
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

impl<K, T, R> NumEntries for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_shallow(),
            Inner::File(file) => file.num_entries_shallow(),
        }
    }

    fn num_entries_deep(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.num_entries_deep(),
            Inner::File(file) => file.num_entries_deep(),
        }
    }
}

impl<K, T, R> BatchReader for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Factories = FallbackKeyBatchFactories<K, T, R>;
    type Key = K;
    type Val = DynUnit;
    type Time = T;
    type R = R;
    type Cursor<'s> = DelegatingCursor<'s, K, DynUnit, T, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        DelegatingCursor(match &self.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
        })
    }

    #[inline]
    fn key_count(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.key_count(),
            Inner::File(file) => file.key_count(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.len(),
            Inner::File(file) => file.len(),
        }
    }

    #[inline]
    fn approximate_byte_size(&self) -> usize {
        match &self.inner {
            Inner::Vec(vec) => vec.approximate_byte_size(),
            Inner::File(file) => file.approximate_byte_size(),
        }
    }

    #[inline]
    fn location(&self) -> BatchLocation {
        match &self.inner {
            Inner::Vec(vec) => vec.location(),
            Inner::File(file) => file.location(),
        }
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        match &self.inner {
            Inner::Vec(vec) => vec.lower(),
            Inner::File(file) => file.lower(),
        }
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        match &self.inner {
            Inner::Vec(vec) => vec.upper(),
            Inner::File(file) => file.upper(),
        }
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        match &mut self.inner {
            Inner::Vec(vec) => vec.truncate_keys_below(lower_bound),
            Inner::File(file) => file.truncate_keys_below(lower_bound),
        }
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, output: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        match &self.inner {
            Inner::Vec(vec) => vec.sample_keys(rng, sample_size, output),
            Inner::File(file) => file.sample_keys(rng, sample_size, output),
        }
    }
}

impl<K, T, R> Batch for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackKeyBuilder<K, T, R>;
    type Merger = FallbackKeyMerger<K, T, R>;

    fn begin_merge(&self, other: &Self, dst_hint: Option<BatchLocation>) -> Self::Merger {
        Self::Merger::new_merger(self, other, dst_hint)
    }

    fn recede_to(&mut self, frontier: &T) {
        match &mut self.inner {
            Inner::Vec(vec) => vec.recede_to(frontier),
            Inner::File(file) => file.recede_to(frontier),
        }
    }

    fn persisted(&self) -> Option<Self> {
        match &self.inner {
            Inner::Vec(vec) => {
                let mut file = FileKeyBuilder::timed_with_capacity(&self.factories.file, 0);
                copy_to_builder(&mut file, vec.cursor());
                Some(Self {
                    inner: Inner::File(
                        file.done_with_bounds(vec.lower().to_owned(), vec.upper().to_owned()),
                    ),
                    factories: self.factories.clone(),
                })
            }
            Inner::File(_) => None,
        }
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        match &self.inner {
            Inner::Vec(vec) => vec.persistent_id(),
            Inner::File(file) => file.persistent_id(),
        }
    }

    fn from_path(factories: &Self::Factories, path: &Path) -> Result<Self, ReaderError> {
        Ok(Self {
            factories: factories.clone(),
            inner: Inner::File(FileKeyBatch::<K, T, R>::from_path(&factories.file, path)?),
        })
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct FallbackKeyMerger<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackKeyBatchFactories<K, T, R>,
    inner: MergerInner<K, T, R>,
}

#[derive(SizeOf)]
enum MergerInner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    AllFile(FileKeyMerger<K, T, R>),
    AllVec(VecKeyMerger<K, T, R>),
    ToVec(GenericMerger<K, DynUnit, T, R, OrdKeyBatch<K, T, R>>),
    ToFile(GenericMerger<K, DynUnit, T, R, FileKeyBatch<K, T, R>>),
}

impl<K, T, R> Merger<K, DynUnit, T, R, FallbackKeyBatch<K, T, R>> for FallbackKeyMerger<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_merger(
        batch1: &FallbackKeyBatch<K, T, R>,
        batch2: &FallbackKeyBatch<K, T, R>,
        dst_hint: Option<BatchLocation>,
    ) -> Self {
        FallbackKeyMerger {
            factories: batch1.factories.clone(),
            inner: match (
                pick_merge_destination(batch1, batch2, dst_hint),
                &batch1.inner,
                &batch2.inner,
            ) {
                (BatchLocation::Memory, Inner::Vec(vec1), Inner::Vec(vec2)) => {
                    MergerInner::AllVec(VecKeyMerger::new_merger(vec1, vec2, dst_hint))
                }
                (BatchLocation::Memory, _, _) => {
                    MergerInner::ToVec(GenericMerger::new(&batch1.factories.vec, batch1, batch2))
                }
                (BatchLocation::Storage, Inner::File(file1), Inner::File(file2)) => {
                    MergerInner::AllFile(FileKeyMerger::new_merger(file1, file2, dst_hint))
                }
                (BatchLocation::Storage, _, _) => {
                    MergerInner::ToFile(GenericMerger::new(&batch1.factories.file, batch1, batch2))
                }
            },
        }
    }

    fn done(self) -> FallbackKeyBatch<K, T, R> {
        FallbackKeyBatch {
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
        source1: &FallbackKeyBatch<K, T, R>,
        source2: &FallbackKeyBatch<K, T, R>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) {
        match &mut self.inner {
            MergerInner::AllFile(merger) => merger.work(
                source1.inner.as_file().unwrap(),
                source2.inner.as_file().unwrap(),
                key_filter,
                value_filter,
                fuel,
            ),
            MergerInner::AllVec(merger) => merger.work(
                source1.inner.as_vec().unwrap(),
                source2.inner.as_vec().unwrap(),
                key_filter,
                value_filter,
                fuel,
            ),
            MergerInner::ToVec(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => merger.work(a, b, key_filter, value_filter, fuel),
            },
            MergerInner::ToFile(merger) => match (&source1.inner, &source2.inner) {
                (Inner::File(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::File(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::File(a), Inner::Vec(b)) => {
                    merger.work(a, b, key_filter, value_filter, fuel)
                }
                (Inner::Vec(a), Inner::Vec(b)) => merger.work(a, b, key_filter, value_filter, fuel),
            },
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct FallbackKeyBuilder<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackKeyBatchFactories<K, T, R>,
    inner: BuilderInner<K, T, R>,
}

#[derive(SizeOf)]
enum BuilderInner<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    /// In-memory.
    Vec(VecKeyBuilder<K, T, R>),

    /// On-storage.
    File(FileKeyBuilder<K, T, R>),

    /// In-memory as long as we don't exceed a maximum threshold size.
    Threshold {
        vec: VecKeyBuilder<K, T, R>,

        /// Bytes left to add until the threshold is exceeded.
        remaining: usize,
    },
}

impl<K, T, R> FallbackKeyBuilder<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    /// We ran out of the bytes threshold for `BuilderInner::Threshold`. Spill
    /// to storage as `BuilderInner::File`, writing `vec` as the initial
    /// contents.
    fn spill(&mut self, vec: OrdKeyBatch<K, T, R>) {
        let mut file = FileKeyBuilder::timed_with_capacity(&self.factories.file, 0);
        copy_to_builder(&mut file, vec.cursor());
        self.inner = BuilderInner::File(file);
    }
}

impl<K, T, R> Builder<FallbackKeyBatch<K, T, R>> for FallbackKeyBuilder<K, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FallbackKeyBatchFactories<K, T, R>, time: T) -> Self {
        Self::with_capacity(factories, time, 0)
    }

    #[inline]
    fn with_capacity(
        factories: &FallbackKeyBatchFactories<K, T, R>,
        time: T,
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            inner: match BuildTo::for_capacity(
                &factories.vec,
                &factories.file,
                time,
                capacity,
                VecKeyBuilder::with_capacity,
                FileKeyBuilder::with_capacity,
            ) {
                BuildTo::Memory(vec) => BuilderInner::Vec(vec),
                BuildTo::Storage(file) => BuilderInner::File(file),
                BuildTo::Threshold(vec, remaining) => BuilderInner::Threshold { vec, remaining },
            },
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut DynPair<DynPair<K, DynUnit>, R>) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push(item),
            BuilderInner::Vec(vec) => vec.push(item),
            BuilderInner::Threshold { vec, remaining } => {
                let size = item.size_of().total_bytes();
                vec.push(item);
                if size > *remaining {
                    let vec = replace(
                        vec,
                        VecKeyBuilder::timed_with_capacity(&self.factories.vec, 0),
                    )
                    .done();
                    self.spill(vec);
                } else {
                    *remaining -= size;
                }
            }
        }
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &DynUnit, weight: &R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_refs(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_refs(key, val, weight),
            BuilderInner::Threshold { vec, remaining } => {
                let size = (key, val, weight).size_of().total_bytes();
                vec.push_refs(key, val, weight);
                if size > *remaining {
                    let vec = replace(
                        vec,
                        VecKeyBuilder::timed_with_capacity(&self.factories.vec, 0),
                    )
                    .done();
                    self.spill(vec);
                } else {
                    *remaining -= size;
                }
            }
        }
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut DynUnit, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_vals(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_vals(key, val, weight),
            BuilderInner::Threshold { vec, remaining } => {
                let size = (key as &K, weight as &R).size_of().total_bytes();
                vec.push_vals(key, val, weight);
                if size > *remaining {
                    let vec = replace(
                        vec,
                        VecKeyBuilder::timed_with_capacity(&self.factories.vec, 0),
                    )
                    .done();
                    self.spill(vec);
                } else {
                    *remaining -= size;
                }
            }
        }
    }

    #[inline(never)]
    fn done(self) -> FallbackKeyBatch<K, T, R> {
        FallbackKeyBatch {
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

impl<K, T, R> Archive for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, T, R, S> Serialize<S> for FallbackKeyBatch<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    S: Serializer + ?Sized,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, T, R, D> Deserialize<FallbackKeyBatch<K, T, R>, D> for Archived<FallbackKeyBatch<K, T, R>>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackKeyBatch<K, T, R>, D::Error> {
        unimplemented!();
    }
}
