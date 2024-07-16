use std::fmt::{Display, Formatter};
use std::mem::replace;
use std::path::PathBuf;

use crate::trace::cursor::DelegatingCursor;
use crate::trace::ord::file::val_batch::FileValBuilder;
use crate::trace::ord::vec::val_batch::VecValBuilder;
use crate::trace::{BatchLocation, TimedBuilder};
use crate::{
    dynamic::{DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait},
    time::AntichainRef,
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
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::utils::{copy_to_builder, BuildTo, GenericMerger, MergeTo};

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
}

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(SizeOf)]
pub struct FallbackValBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: FallbackValBatchFactories<K, V, T, R>,
    inner: Inner<K, V, T, R>,
}

#[derive(SizeOf)]
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

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FallbackValMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        match &mut self.inner {
            Inner::Vec(vec) => vec.recede_to(frontier),
            Inner::File(file) => file.recede_to(frontier),
        }
    }

    fn persistent_id(&self) -> Option<PathBuf> {
        match &self.inner {
            Inner::Vec(vec) => vec.persistent_id(),
            Inner::File(file) => file.persistent_id(),
        }
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
    ) -> Self {
        FallbackValMerger {
            factories: batch1.factories.clone(),
            inner: match (
                MergeTo::from((batch1, batch2)),
                &batch1.inner,
                &batch2.inner,
            ) {
                (MergeTo::Memory, Inner::Vec(vec1), Inner::Vec(vec2)) => {
                    MergerInner::AllVec(VecValMerger::new_merger(vec1, vec2))
                }
                (MergeTo::Memory, _, _) => {
                    MergerInner::ToVec(GenericMerger::new(&batch1.factories.vec, batch1, batch2))
                }
                (MergeTo::Storage, Inner::File(file1), Inner::File(file2)) => {
                    MergerInner::AllFile(FileValMerger::new_merger(file1, file2))
                }
                (MergeTo::Storage, _, _) => {
                    MergerInner::ToFile(GenericMerger::new(&batch1.factories.file, batch1, batch2))
                }
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
    /// In-memory.
    Vec(VecValBuilder<K, V, T, R, usize>),

    /// On-storage.
    File(FileValBuilder<K, V, T, R>),

    /// In-memory as long as we don't exceed a maximum threshold size.
    Threshold {
        vec: VecValBuilder<K, V, T, R, usize>,

        /// Bytes left to add until the threshold is exceeded.
        remaining: usize,
    },
}

impl<K, V, T, R> FallbackValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    /// We ran out of the bytes threshold for `BuilderInner::Threshold`. Spill
    /// to storage as `BuilderInner::File`, writing `vec` as the initial
    /// contents.
    fn spill(&mut self, vec: OrdValBatch<K, V, T, R>) {
        let mut file = FileValBuilder::timed_with_capacity(&self.factories.file, 0);
        copy_to_builder(&mut file, vec.cursor());
        self.inner = BuilderInner::File(file);
    }
}

impl<K, V, T, R> Builder<FallbackValBatch<K, V, T, R>> for FallbackValBuilder<K, V, T, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn new_builder(factories: &FallbackValBatchFactories<K, V, T, R>, time: T) -> Self {
        Self::with_capacity(factories, time, 0)
    }

    fn with_capacity(
        factories: &FallbackValBatchFactories<K, V, T, R>,
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
                VecValBuilder::with_capacity,
                FileValBuilder::with_capacity,
            ) {
                BuildTo::Memory(vec) => BuilderInner::Vec(vec),
                BuildTo::Storage(file) => BuilderInner::File(file),
                BuildTo::Threshold(vec, remaining) => BuilderInner::Threshold { vec, remaining },
            },
        }
    }

    fn reserve(&mut self, _additional: usize) {}

    fn push(&mut self, item: &mut DynPair<DynPair<K, V>, R>) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push(item),
            BuilderInner::Vec(vec) => vec.push(item),
            BuilderInner::Threshold { vec, remaining } => {
                let size = item.size_of().total_bytes();
                vec.push(item);
                if size > *remaining {
                    let vec = replace(
                        vec,
                        VecValBuilder::timed_with_capacity(&self.factories.vec, 0),
                    )
                    .done();
                    self.spill(vec);
                } else {
                    *remaining -= size;
                }
            }
        }
    }

    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_refs(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_refs(key, val, weight),
            BuilderInner::Threshold { vec, remaining } => {
                let size = (key, val, weight).size_of().total_bytes();
                vec.push_refs(key, val, weight);
                if size > *remaining {
                    let vec = replace(
                        vec,
                        VecValBuilder::timed_with_capacity(&self.factories.vec, 0),
                    )
                    .done();
                    self.spill(vec);
                } else {
                    *remaining -= size;
                }
            }
        }
    }

    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_vals(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_vals(key, val, weight),
            BuilderInner::Threshold { vec, remaining } => {
                let size = (key as &K, val as &V, weight as &R).size_of().total_bytes();
                vec.push_vals(key, val, weight);
                if size > *remaining {
                    let vec = replace(
                        vec,
                        VecValBuilder::timed_with_capacity(&self.factories.vec, 0),
                    )
                    .done();
                    self.spill(vec);
                } else {
                    *remaining -= size;
                }
            }
        }
    }

    fn done(self) -> FallbackValBatch<K, V, T, R> {
        FallbackValBatch {
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
