use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    dynamic::{
        DataTrait, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory, WeightTrait,
        WeightTraitTyped,
    },
    time::AntichainRef,
    trace::{
        ord::{
            file::wset_batch::{FileWSetBuilder, FileWSetCursor, FileWSetMerger},
            merge_batcher::MergeBatcher,
            vec::wset_batch::{VecWSetBuilder, VecWSetCursor, VecWSetMerger},
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, FileWSet,
        FileWSetFactories, Filter, Merger, OrdWSet, OrdWSetFactories, WeightedItem,
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

pub struct FallbackWSetFactories<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    file: FileWSetFactories<K, R>,
    vec: OrdWSetFactories<K, R>,
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
            vec: OrdWSetFactories::new::<KType, VType, RType>(),
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
}

pub struct FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FallbackWSetFactories<K, R>,
    inner: Inner<K, R>,
}

#[allow(clippy::large_enum_variant)]
enum Inner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    Vec(OrdWSet<K, R>),
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
    fn as_vec(&self) -> Option<&OrdWSet<K, R>> {
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

// This is `#[cfg(test)]` only because it would be surprisingly expensive in
// production.
#[cfg(test)]
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

#[cfg(test)]
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

impl<K, R> Add<Self> for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
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

impl<K, R> AddAssign<Self> for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        if !rhs.is_empty() {
            *self = self.merge(&rhs);
        }
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
            *self = self.merge(rhs);
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
        self.merge(rhs)
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
    type Cursor<'s> = FallbackWSetCursor<'s, K, R>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        FallbackWSetCursor::new(self)
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

impl<K, R> Batch for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = FallbackWSetBuilder<K, R>;
    type Merger = FallbackWSetMerger<K, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        FallbackWSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn dyn_empty(factories: &Self::Factories, time: Self::Time) -> Self {
        Self {
            factories: factories.clone(),
            inner: Inner::Vec(OrdWSet::dyn_empty(&factories.vec, time)),
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
pub struct FallbackWSetMerger<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FallbackWSetFactories<K, R>,
    inner: MergerInner<K, R>,
}

enum MergerInner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    AllFile(FileWSetMerger<K, R>),
    AllVec(VecWSetMerger<K, R>),
    ToVec(GenericMerger<K, R, OrdWSet<K, R>>),
    ToFile(GenericMerger<K, R, FileWSet<K, R>>),
}

impl<K, R> Merger<K, DynUnit, (), R, FallbackWSet<K, R>> for FallbackWSetMerger<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_merger(batch1: &FallbackWSet<K, R>, batch2: &FallbackWSet<K, R>) -> Self {
        Self {
            factories: batch1.factories.clone(),
            inner: if batch1.len() + batch2.len() <= Runtime::max_memory_rows() {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::Vec(vec1), Inner::Vec(vec2)) => {
                        MergerInner::AllVec(VecWSetMerger::new_merger(vec1, vec2))
                    }
                    _ => MergerInner::ToVec(GenericMerger::new(&batch1.factories.vec)),
                }
            } else {
                match (&batch1.inner, &batch2.inner) {
                    (Inner::File(file1), Inner::File(file2)) => {
                        MergerInner::AllFile(FileWSetMerger::new_merger(file1, file2))
                    }
                    _ => MergerInner::ToFile(GenericMerger::new(&batch1.factories.file)),
                }
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

impl<K, R> SizeOf for FallbackWSetMerger<K, R>
where
    K: DataTrait + ?Sized,
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

trait ClonableCursor<'s, K, R>: Cursor<K, DynUnit, (), R> + Debug
where
    K: ?Sized,
    R: ?Sized,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, R> + 's>;
}

impl<'s, K, R, C> ClonableCursor<'s, K, R> for C
where
    K: ?Sized,
    R: ?Sized,
    C: Cursor<K, DynUnit, (), R> + Debug + Clone + 's,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, R> + 's> {
        Box::new(self.clone())
    }
}
/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct FallbackWSetCursor<'s, K, R>(Box<dyn ClonableCursor<'s, K, R> + 's>)
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized;

impl<'s, K, R> Clone for FallbackWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self(self.0.clone_boxed())
    }
}

impl<'s, K, R> FallbackWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(zset: &'s FallbackWSet<K, R>) -> Self {
        Self(match &zset.inner {
            Inner::Vec(vec) => Box::new(vec.cursor()),
            Inner::File(file) => Box::new(file.cursor()),
        })
    }
}

impl<'s, K, R> Cursor<K, DynUnit, (), R> for FallbackWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.0.weight_factory()
    }

    fn key(&self) -> &K {
        self.0.key()
    }

    fn val(&self) -> &DynUnit {
        self.0.val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        self.0.map_times(logic)
    }

    fn map_times_through(&mut self, upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.0.map_times_through(upper, logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R)) {
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

    fn seek_val(&mut self, val: &DynUnit) {
        self.0.seek_val(val)
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
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

    fn seek_val_reverse(&mut self, val: &DynUnit) {
        self.0.seek_val_reverse(val)
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        self.0.seek_val_with_reverse(predicate)
    }

    fn fast_forward_vals(&mut self) {
        self.0.fast_forward_vals()
    }
}

/// A builder for batches from ordered update tuples.
pub struct FallbackWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    factories: FallbackWSetFactories<K, R>,
    inner: BuilderInner<K, R>,
}

#[allow(clippy::large_enum_variant)]
enum BuilderInner<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    File(FileWSetBuilder<K, R>),
    Vec(VecWSetBuilder<K, R>),
}

impl<K, R> Builder<FallbackWSet<K, R>> for FallbackWSetBuilder<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[inline]
    fn new_builder(factories: &FallbackWSetFactories<K, R>, time: ()) -> Self {
        Self::with_capacity(factories, time, 0)
    }

    #[inline]
    fn with_capacity(factories: &FallbackWSetFactories<K, R>, time: (), capacity: usize) -> Self {
        Self {
            factories: factories.clone(),
            inner: if capacity <= Runtime::max_memory_rows() {
                BuilderInner::Vec(VecWSetBuilder::with_capacity(
                    &factories.vec,
                    time,
                    capacity,
                ))
            } else {
                BuilderInner::File(FileWSetBuilder::with_capacity(
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
    fn push(&mut self, item: &mut DynPair<DynPair<K, DynUnit>, R>) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push(item),
            BuilderInner::Vec(vec) => vec.push(item),
        }
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &DynUnit, weight: &R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_refs(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_refs(key, val, weight),
        }
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut DynUnit, weight: &mut R) {
        match &mut self.inner {
            BuilderInner::File(file) => file.push_vals(key, val, weight),
            BuilderInner::Vec(vec) => vec.push_vals(key, val, weight),
        }
    }

    #[inline(never)]
    fn done(self) -> FallbackWSet<K, R> {
        FallbackWSet {
            factories: self.factories,
            inner: match self.inner {
                BuilderInner::File(file) => Inner::File(file.done()),
                BuilderInner::Vec(vec) => Inner::Vec(vec.done()),
            },
        }
    }
}

impl<K, R> SizeOf for FallbackWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        match &self.inner {
            BuilderInner::File(file) => file.size_of_children(context),
            BuilderInner::Vec(vec) => vec.size_of_children(context),
        }
    }
}

impl<K, R> SizeOf for FallbackWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        match &self.inner {
            Inner::Vec(vec) => vec.size_of_children(context),
            Inner::File(file) => file.size_of_children(context),
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

    fn from_cursor<C, R>(cursor: &C) -> Position<K>
    where
        C: Cursor<K, DynUnit, (), R>,
        R: ?Sized,
    {
        if cursor.key_valid() {
            Self::At(clone_box(cursor.key()))
        } else {
            Self::End
        }
    }
}

struct GenericMerger<K, R, O>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
{
    builder: O::Builder,
    pos1: Position<K>,
    pos2: Position<K>,
}

trait CursorWeightRef<K, T, R>: Cursor<K, DynUnit, T, R>
where
    K: ?Sized,
    R: ?Sized,
{
    fn weight_ref(&self) -> &R;
}

impl<'s, K, R> CursorWeightRef<K, (), R> for VecWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_ref(&self) -> &R {
        self.cursor.current_diff()
    }
}

impl<'s, K, R> CursorWeightRef<K, (), R> for FileWSetCursor<'s, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_ref(&self) -> &R {
        self.diff.as_ref()
    }
}

impl<K, R, O> GenericMerger<K, R, O>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
{
    fn new(factories: &O::Factories) -> Self {
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
        value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) where
        A: BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
        B: BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
        A::Cursor<'s>: CursorWeightRef<K, (), R>,
        B::Cursor<'s>: CursorWeightRef<K, (), R>,
    {
        if !filter(value_filter, &()) {
            return;
        }

        let mut cursor1 = self.pos1.cursor(source1);
        let mut cursor2 = self.pos2.cursor(source2);
        while cursor1.key_valid() && cursor2.key_valid() && *fuel > 0 {
            match cursor1.key().cmp(cursor2.key()) {
                Ordering::Less => {
                    self.copy_value_if(&mut cursor1, key_filter, fuel);
                }
                Ordering::Equal => {
                    self.copy_value_if(&mut cursor1, key_filter, fuel);
                    cursor2.step_key();
                    *fuel -= 1;
                }
                Ordering::Greater => {
                    self.copy_value_if(&mut cursor2, key_filter, fuel);
                }
            }
        }

        while cursor1.key_valid() && *fuel > 0 {
            self.copy_value_if(&mut cursor1, key_filter, fuel);
        }
        while cursor2.key_valid() && *fuel > 0 {
            self.copy_value_if(&mut cursor2, key_filter, fuel);
        }
        self.pos1 = Position::from_cursor(&cursor1);
        self.pos2 = Position::from_cursor(&cursor2);
    }

    fn done(self) -> O {
        self.builder.done()
    }

    fn copy_value_if<C>(&mut self, cursor: &mut C, key_filter: &Option<Filter<K>>, fuel: &mut isize)
    where
        C: CursorWeightRef<K, (), R>,
    {
        if filter(key_filter, cursor.key()) {
            self.builder
                .push_refs(cursor.key(), &(), cursor.weight_ref());
        }
        *fuel -= 1;
        cursor.step_key();
    }
}

fn filter<T>(f: &Option<Filter<T>>, t: &T) -> bool
where
    T: ?Sized,
{
    f.as_ref().map_or(true, |f| f(t))
}

impl<K, R, O> SizeOf for GenericMerger<K, R, O>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: Batch + BatchReader<Key = K, Val = DynUnit, R = R, Time = ()>,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.builder.size_of_children(context)
    }
}
