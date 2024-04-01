use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    dynamic::{
        DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait, WeightTraitTyped,
    },
    time::AntichainRef,
    trace::{
        layers::OrdOffset,
        ord::{
            file::indexed_zset_batch::{
                FileIndexedZSetBuilder, FileIndexedZSetCursor, FileIndexedZSetMerger,
            },
            merge_batcher::MergeBatcher,
            vec::indexed_wset_batch::{
                OrdIndexedWSetMerger, VecIndexedWSetBuilder, VecIndexedWSetCursor,
            },
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, FileIndexedZSet,
        FileIndexedZSetFactories, Filter, Merger, OrdIndexedWSet, OrdIndexedWSetFactories,
        WeightedItem,
    },
    DBData, DBWeight, NumEntries, Runtime,
};
use dyn_clone::clone_box;
use rand::Rng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::{self, Debug},
    ops::{Add, AddAssign},
};
use std::{ops::Neg, path::PathBuf};

pub struct FallbackIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    file: FileIndexedZSetFactories<K, V, R>,
    vec: OrdIndexedWSetFactories<K, V, R>,
}

impl<K, V, R> Clone for FallbackIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            file: self.file.clone(),
            vec: self.vec.clone(),
        }
    }
}

impl<K, V, R> BatchReaderFactories<K, V, (), R> for FallbackIndexedZSetFactories<K, V, R>
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
            file: FileIndexedZSetFactories::new::<KType, VType, RType>(),
            vec: OrdIndexedWSetFactories::new::<KType, VType, RType>(),
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

impl<K, V, R> BatchFactories<K, V, (), R> for FallbackIndexedZSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
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

pub struct FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FallbackIndexedZSetFactories<K, V, R>,
    inner: Inner<K, V, R>,
}

#[allow(clippy::large_enum_variant)]
enum Inner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    Vec(OrdIndexedWSet<K, V, R>),
    File(FileIndexedZSet<K, V, R>),
}

impl<K, V, R> Inner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn as_file(&self) -> Option<&FileIndexedZSet<K, V, R>> {
        match self {
            Inner::Vec(_vec) => None,
            Inner::File(file) => Some(file),
        }
    }
    fn as_vec(&self) -> Option<&OrdIndexedWSet<K, V, R>> {
        match self {
            Inner::Vec(vec) => Some(vec),
            Inner::File(_file) => None,
        }
    }
}

impl<K, V, R> Debug for FallbackIndexedZSet<K, V, R>
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

impl<K, V, R> Clone for FallbackIndexedZSet<K, V, R>
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

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
#[cfg(test)]
impl<Other, K, V, R> PartialEq<Other> for FallbackIndexedZSet<K, V, R>
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
impl<K, V, R> Eq for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K, V, R> NumEntries for FallbackIndexedZSet<K, V, R>
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

impl<K, V, R> NegByRef for FallbackIndexedZSet<K, V, R>
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

impl<K, V, R> Neg for FallbackIndexedZSet<K, V, R>
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

impl<K, V, R> Add<Self> for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Output = Self;
    #[inline]

    fn add(self, rhs: Self) -> Self::Output {
        if self.is_empty() {
            rhs
        } else if rhs.is_empty() {
            self
        } else {
            self.merge(&rhs)
        }
    }
}

impl<K, V, R> AddAssign<Self> for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
    }
}

impl<K, V, R> AddAssignByRef for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        if !rhs.is_empty() {
            *self = self.merge(rhs);
        }
    }
}

impl<K, V, R> AddByRef for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        self.merge(rhs)
    }
}

impl<K, V, R> BatchReader for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Factories = FallbackIndexedZSetFactories<K, V, R>;
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = FallbackIndexedZSetCursor<'s, K, V, R>
    where
        V: 's;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FallbackIndexedZSetCursor::new(self)
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
    fn lower(&self) -> AntichainRef<'_, ()> {
        AntichainRef::new(&[()])
    }

    #[inline]
    fn upper(&self) -> AntichainRef<'_, ()> {
        AntichainRef::empty()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        match &mut self.inner {
            Inner::File(file) => file.truncate_keys_below(lower_bound),
            Inner::Vec(vec) => vec.truncate_keys_below(lower_bound),
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
}

impl<K, V, R> Batch for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackIndexedZSetBuilder<K, V, R>;
    type Merger = FallbackIndexedZSetMerger<K, V, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FallbackIndexedZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn dyn_empty(factories: &Self::Factories, time: Self::Time) -> Self {
        Self {
            factories: factories.clone(),
            inner: Inner::Vec(OrdIndexedWSet::dyn_empty(&factories.vec, time)),
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
pub struct FallbackIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FallbackIndexedZSetFactories<K, V, R>,
    inner: MergerInner<K, V, R>,
}

enum MergerInner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    AllFile(FileIndexedZSetMerger<K, V, R>),
    AllVec(OrdIndexedWSetMerger<K, V, R>),
    ToVec(GenericMerger<K, V, R, OrdIndexedWSet<K, V, R>>),
    ToFile(GenericMerger<K, V, R, FileIndexedZSet<K, V, R>>),
}

impl<K, V, R> Merger<K, V, (), R, FallbackIndexedZSet<K, V, R>>
    for FallbackIndexedZSetMerger<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_merger(
        batch1: &FallbackIndexedZSet<K, V, R>,
        batch2: &FallbackIndexedZSet<K, V, R>,
    ) -> Self {
        Self {
            factories: batch1.factories.clone(),
            inner: if batch1.len() + batch2.len() <= Runtime::max_memory_rows() {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::Vec(vec1), Inner::Vec(vec2)) => {
                        MergerInner::AllVec(OrdIndexedWSetMerger::new_merger(vec1, vec2))
                    }
                    _ => MergerInner::ToVec(GenericMerger::new(&batch1.factories.vec)),
                }
            } else {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::File(file1), Inner::File(file2)) => {
                        MergerInner::AllFile(FileIndexedZSetMerger::new_merger(file1, file2))
                    }
                    _ => MergerInner::ToFile(GenericMerger::new(&batch1.factories.file)),
                }
            },
        }
    }

    #[inline]
    fn done(self) -> FallbackIndexedZSet<K, V, R> {
        FallbackIndexedZSet {
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
        source1: &FallbackIndexedZSet<K, V, R>,
        source2: &FallbackIndexedZSet<K, V, R>,
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

impl<K, V, R> SizeOf for FallbackIndexedZSetMerger<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        match &self.inner {
            MergerInner::AllFile(file) => file.size_of_children(context),
            MergerInner::AllVec(vec) => vec.size_of_children(context),
            MergerInner::ToFile(merger) => merger.size_of_children(context),
            MergerInner::ToVec(merger) => merger.size_of_children(context),
        }
    }
}

trait ClonableCursor<'s, K, V, R>: Cursor<K, V, (), R> + Debug
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, V, R> + 's>;
}

impl<'s, K, V, R, C> ClonableCursor<'s, K, V, R> for C
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
    C: Cursor<K, V, (), R> + Debug + Clone + 's,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, V, R> + 's> {
        Box::new(self.clone())
    }
}
/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FallbackIndexedZSetCursor<'s, K, V, R>(Box<dyn ClonableCursor<'s, K, V, R> + 's>)
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized;

impl<'s, K, V, R> Clone for FallbackIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self(self.0.clone_boxed())
    }
}

impl<'s, K, V, R> FallbackIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(zset: &'s FallbackIndexedZSet<K, V, R>) -> Self {
        Self(match &zset.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
        })
    }
}

impl<'s, K, V, R> Cursor<K, V, (), R> for FallbackIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.0.weight_factory()
    }

    fn key(&self) -> &K {
        self.0.key()
    }

    fn val(&self) -> &V {
        self.0.val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        self.0.map_times(logic)
    }

    fn map_times_through(&mut self, upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.0.map_times_through(upper, logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        self.0.map_values(logic)
    }

    fn weight(&mut self) -> &R {
        self.0.weight()
    }

    fn key_valid(&self) -> bool {
        self.0.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.0.val_valid()
    }

    fn step_key(&mut self) {
        self.0.step_key()
    }

    fn step_key_reverse(&mut self) {
        self.0.step_key_reverse()
    }

    fn seek_key(&mut self, key: &K) {
        self.0.seek_key(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.0.seek_key_with(predicate)
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.0.seek_key_with_reverse(predicate)
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.0.seek_key_reverse(key)
    }

    fn step_val(&mut self) {
        self.0.step_val()
    }

    fn seek_val(&mut self, val: &V) {
        self.0.seek_val(val)
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.0.seek_val_with(predicate)
    }

    fn rewind_keys(&mut self) {
        self.0.rewind_keys()
    }

    fn fast_forward_keys(&mut self) {
        self.0.fast_forward_keys()
    }

    fn rewind_vals(&mut self) {
        self.0.rewind_vals()
    }

    fn step_val_reverse(&mut self) {
        self.0.step_val_reverse()
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.0.seek_val_reverse(val)
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.0.seek_val_with_reverse(predicate)
    }

    fn fast_forward_vals(&mut self) {
        self.0.fast_forward_vals()
    }
}

/// A builder for batches from ordered update tuples.
pub struct FallbackIndexedZSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FallbackIndexedZSetFactories<K, V, R>,
    inner: BuilderInner<K, V, R>,
}

#[allow(clippy::large_enum_variant)]
enum BuilderInner<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    File(FileIndexedZSetBuilder<K, V, R>),
    Vec(VecIndexedWSetBuilder<K, V, R, usize>),
}

impl<K, V, R> Builder<FallbackIndexedZSet<K, V, R>> for FallbackIndexedZSetBuilder<K, V, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FallbackIndexedZSetFactories<K, V, R>, time: ()) -> Self {
        Self::with_capacity(factories, time, 0)
    }

    #[inline]
    fn with_capacity(
        factories: &FallbackIndexedZSetFactories<K, V, R>,
        time: (),
        capacity: usize,
    ) -> Self {
        Self {
            factories: factories.clone(),
            inner: if capacity <= Runtime::max_memory_rows() {
                BuilderInner::Vec(VecIndexedWSetBuilder::with_capacity(
                    &factories.vec,
                    time,
                    capacity,
                ))
            } else {
                BuilderInner::File(FileIndexedZSetBuilder::with_capacity(
                    &factories.file,
                    time,
                    capacity,
                ))
            },
        }
    }

    #[inline]
    fn reserve(&mut self, _additional: usize) {}

    #[inline]
    fn push(&mut self, item: &mut DynPair<DynPair<K, V>, R>) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push(item),
            BuilderInner::Vec(vec) => vec.push(item),
        }
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_refs(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_refs(key, val, weight),
        }
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_vals(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_vals(key, val, weight),
        }
    }

    #[inline(never)]
    fn done(self) -> FallbackIndexedZSet<K, V, R> {
        FallbackIndexedZSet {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done()),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done()),
            },
        }
    }
}

impl<K, V, R> SizeOf for FallbackIndexedZSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        match &self.inner {
            BuilderInner::File(file) => file.size_of_children(context),
            BuilderInner::Vec(vec) => vec.size_of_children(context),
        }
    }
}

impl<K, V, R> SizeOf for FallbackIndexedZSet<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        match &self.inner {
            Inner::Vec(vec) => vec.size_of_children(context),
            Inner::File(file) => file.size_of_children(context),
        }
    }
}

impl<K, V, R> Archive for FallbackIndexedZSet<K, V, R>
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

impl<K, V, R, S> Serialize<S> for FallbackIndexedZSet<K, V, R>
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

impl<K, V, R, D> Deserialize<FallbackIndexedZSet<K, V, R>, D>
    for Archived<FallbackIndexedZSet<K, V, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    D: Fallible,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<FallbackIndexedZSet<K, V, R>, D::Error> {
        unimplemented!();
    }
}

enum Position<K>
where
    K: DataTrait + ?Sized,
{
    Start,
    At(Box<K>),
    End,
}

impl<K> Position<K>
where
    K: DataTrait + ?Sized,
{
    fn cursor<'s, B>(&self, source: &'s B) -> B::Cursor<'s>
    where
        B: BatchReader<Key = K>,
    {
        let mut cursor = source.cursor();
        match self {
            Position::Start => (),
            Position::At(key) => cursor.seek_key(key.as_ref()),
            Position::End => {
                cursor.fast_forward_keys();
                cursor.step_key()
            }
        }
        cursor
    }

    fn from_cursor<C, V, R>(cursor: &C) -> Position<K>
    where
        C: Cursor<K, V, (), R>,
        V: ?Sized,
        R: ?Sized,
    {
        if cursor.key_valid() {
            Self::At(clone_box(cursor.key()))
        } else {
            Self::End
        }
    }
}

struct GenericMerger<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = ()>,
{
    builder: O::Builder,
    pos1: Position<K>,
    pos2: Position<K>,
}

trait CursorWeightRef<K, V, T, R>: Cursor<K, V, T, R>
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn weight_ref(&self) -> &R;
}

impl<'s, K, V, R, O> CursorWeightRef<K, V, (), R> for VecIndexedWSetCursor<'s, K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn weight_ref(&self) -> &R {
        self.cursor.child.current_diff()
    }
}

impl<'s, K, V, R> CursorWeightRef<K, V, (), R> for FileIndexedZSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_ref(&self) -> &R {
        self.diff.as_ref()
    }
}

impl<K, V, R, O> GenericMerger<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = ()>,
{
    fn new(factories: &O::Factories) -> Self {
        println!("generic merger");
        Self {
            builder: O::Builder::new_builder(factories, ()),
            pos1: Position::Start,
            pos2: Position::Start,
        }
    }

    fn work<'s, A, B>(
        &mut self,
        source1: &'s A,
        source2: &'s B,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        A: BatchReader<Key = K, Val = V, R = R, Time = ()>,
        B: BatchReader<Key = K, Val = V, R = R, Time = ()>,
        A::Cursor<'s>: CursorWeightRef<K, V, (), R>,
        B::Cursor<'s>: CursorWeightRef<K, V, (), R>,
    {
        let mut cursor1 = self.pos1.cursor(source1);
        let mut cursor2 = self.pos2.cursor(source2);
        let mut diff = source1.factories().weight_factory().default_box();
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => {
                    self.copy_values_if(&mut cursor1, key_filter, value_filter, fuel);
                }
                Ordering::Equal => {
                    if filter(key_filter, cursor1.key()) {
                        self.merge_values(
                            &mut cursor1,
                            &mut cursor2,
                            value_filter,
                            diff.as_mut(),
                            fuel,
                        );
                    } else {
                        *fuel -= 1;
                    }
                    cursor1.step_key();
                    cursor2.step_key();
                }

                Ordering::Greater => {
                    self.copy_values_if(&mut cursor2, key_filter, value_filter, fuel);
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor1, key_filter, value_filter, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_values_if(&mut cursor2, key_filter, value_filter, fuel);
        }
        self.pos1 = Position::from_cursor(&cursor1);
        self.pos2 = Position::from_cursor(&cursor2);
    }

    fn done(self) -> O {
        self.builder.done()
    }

    fn copy_values_if<C>(
        &mut self,
        cursor: &mut C,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) where
        C: CursorWeightRef<K, V, (), R>,
    {
        if filter(key_filter, cursor.key()) {
            while cursor.val_valid() {
                if filter(value_filter, cursor.val()) {
                    self.builder
                        .push_refs(cursor.key(), cursor.val(), cursor.weight_ref());
                }
                cursor.step_val();
                *fuel -= 1;
            }
        } else {
            *fuel -= 1;
        }
        cursor.step_key();
    }

    fn copy_value<C>(&mut self, cursor: &mut C, value_filter: &Option<Filter<V>>)
    where
        C: CursorWeightRef<K, V, (), R>,
    {
        if filter(value_filter, cursor.val()) {
            self.builder
                .push_refs(cursor.key(), cursor.val(), cursor.weight_ref());
        }
        cursor.step_val();
    }

    fn merge_values<C1, C2>(
        &mut self,
        cursor1: &mut C1,
        cursor2: &mut C2,
        value_filter: &Option<Filter<V>>,
        sum: &mut R,
        fuel: &mut isize,
    ) where
        C1: CursorWeightRef<K, V, (), R>,
        C2: CursorWeightRef<K, V, (), R>,
    {
        while cursor1.val_valid() && cursor2.val_valid() {
            let value1 = cursor1.val();
            let value2 = cursor2.val();
            let cmp = value1.cmp(value2);
            match cmp {
                Ordering::Less => {
                    self.copy_value(cursor1, value_filter);
                }
                Ordering::Equal => {
                    if filter(value_filter, value1) {
                        cursor1.weight_ref().add(cursor2.weight_ref(), sum);
                        if !sum.is_zero() {
                            self.builder.push_refs(cursor1.key(), cursor1.val(), sum);
                        }
                    }
                    cursor1.step_val();
                    cursor2.step_val();
                }

                Ordering::Greater => {
                    self.copy_value(cursor2, value_filter);
                }
            }
            *fuel -= 1;
        }

        while cursor1.val_valid() {
            self.copy_value(cursor1, value_filter);
            *fuel -= 1;
        }
        while cursor2.val_valid() {
            self.copy_value(cursor2, value_filter);
            *fuel -= 1;
        }
    }
}

fn filter<T>(f: &Option<Filter<T>>, t: &T) -> bool
where
    T: ?Sized,
{
    f.as_ref().map_or(true, |f| f(t))
}

impl<K, V, R, O> SizeOf for GenericMerger<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = V, R = R, Time = ()>,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.builder.size_of_children(context)
    }
}
