use crate::{
    algebra::{AddAssignByRef, AddByRef, NegByRef, ZRingValue},
    dynamic::{
        DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, LeanVec, WeightTrait,
        WeightTraitTyped, WithFactory,
    },
    time::{Antichain, AntichainRef},
    trace::{
        cursor::{HasTimeDiffCursor, SingletonTimeDiffCursor},
        layers::{
            Builder as _, Cursor as _, Layer, LayerBuilder, LayerCursor, LayerFactories, Leaf,
            LeafBuilder, LeafFactories, MergeBuilder, OrdOffset, Trie, TupleBuilder,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Deserializer,
        Filter, Merger, Serializer, TimedBuilder, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries,
};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    fmt::{self, Debug, Display},
    ops::Neg,
};

use crate::trace::ord::merge_batcher::MergeBatcher;

pub struct VecIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    layer_factories: LayerFactories<K, LeafFactories<V, R>>,
    item_factory: &'static dyn Factory<DynPair<K, V>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>,
    weighted_item_factory: &'static dyn Factory<DynPair<DynPair<K, V>, R>>,
}

impl<K, V, R> Clone for VecIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            item_factory: self.item_factory,
            weighted_items_factory: self.weighted_items_factory,
            weighted_item_factory: self.weighted_item_factory,
        }
    }
}

impl<K, V, R> BatchReaderFactories<K, V, (), R> for VecIndexedWSetFactories<K, V, R>
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
            layer_factories: LayerFactories::new::<KType>(LeafFactories::new::<VType, RType>()),
            item_factory: WithFactory::<Tup2<KType, VType>>::FACTORY,
            weighted_items_factory:
                WithFactory::<LeanVec<Tup2<Tup2<KType, VType>, RType>>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, VType>, RType>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.layer_factories.key
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.layer_factories.keys
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        self.layer_factories.child.key
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.layer_factories.child.diff
    }
}

impl<K, V, R> BatchFactories<K, V, (), R> for VecIndexedWSetFactories<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.item_factory
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.weighted_items_factory
    }
    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.weighted_item_factory
    }

    // fn weighted_item_factory(&self) -> &'static <Pair<K, V>, R> {
    //     self.weighted_item_factory
    // }

    // fn batch_item_factory(&self) -> &'static BatchItemFactory<K, V, Pair<K, V>,
    // R> {     self.batch_item_factory
    // }

    /*fn item_from<'a, I>(
        &self,
        key: OwnedInitRef<'a, K>,
        val: OwnedInitRef<'a, V>,
        weight: OwnedInitRef<'a, R>,
        item: I,
    ) -> I::Output
    where
        I: MutRef<Self::WeightedItem>,
    {
        let (keyval_ref, weight_ref) = self.weighted_item_factory.split(item);
        let (key_ref, val_ref) = self.item_factory.split(keyval_uninit);
        self.layer_factories.key.write_uninit(key, key_uninit);
        self.layer_factories.child.key.write_uninit(val, val_uninit);
        self.layer_factories
            .child
            .diff
            .write_uninit(weight, weight_uninit);
    }*/
}

type Layers<K, V, R, O> = Layer<K, Leaf<V, R>, O>;

/// An immutable collection of update tuples.
#[derive(SizeOf)]
pub struct VecIndexedWSet<K, V, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    /// Where all the data is.
    #[doc(hidden)]
    pub layer: Layers<K, V, R, O>,
    #[size_of(skip)]
    factories: VecIndexedWSetFactories<K, V, R>,
}

impl<K, V, R, O> PartialEq for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn eq(&self, other: &Self) -> bool {
        self.layer == other.layer
    }
}

impl<K, V, R, O> Eq for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
}

impl<K, V, R, O> Debug for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecIndexedWSet")
            .field("layer", &self.layer)
            .finish()
    }
}

impl<K, V, R, O: OrdOffset> Clone for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
            factories: self.factories.clone(),
        }
    }
}

impl<K, V, R, O> Display for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

/*impl<K, V, R, KV, KVR, O> Default for VecIndexedWSet<K, V, R, KV, KVR, O>
where
    O: OrdOffset,
{
    #[inline]
    fn default() -> Self {
        Self::empty(())
    }
}*/

/*impl<K, V, R, KV, KVR, O> From<Layers<K, V, R, O>> for VecIndexedWSet<K, V, R, KV, KVR, O>
where
    O: OrdOffset,
{
    #[inline]
    fn from(layer: Layers<K, V, R, O>) -> Self {
        Self { layer }
    }
}

impl<K, V, R, O> From<Layers<K, V, R, O>> for Rc<VecIndexedWSet<K, V, R, O>>
where
    K: Ord,
    V: Ord,
    R: Clone,
    O: OrdOffset,
{
    #[inline]
    fn from(layer: Layers<K, V, R, O>) -> Self {
        Rc::new(From::from(layer))
    }
}*/

impl<K, V, R, O> NumEntries for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    const CONST_NUM_ENTRIES: Option<usize> = Layers::<K, V, R, O>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, V, R, O> NegByRef for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    R::Type: DBWeight + ZRingValue + Erase<R>,
    O: OrdOffset,
{
    #[inline]
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            factories: self.factories.clone(),
        }
    }
}

impl<K, V, R, O> Neg for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTraitTyped + ?Sized,
    O: OrdOffset,
    R::Type: DBWeight + ZRingValue,
{
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
            factories: self.factories.clone(),
        }
    }
}

impl<K, V, R, O> AddAssignByRef for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[inline]
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K, V, R, O> AddByRef for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[inline]
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
            factories: self.factories.clone(),
        }
    }
}

impl<K, V, R, O: OrdOffset> Deserialize<VecIndexedWSet<K, V, R, O>, Deserializer> for ()
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<VecIndexedWSet<K, V, R, O>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, V, R, O: OrdOffset> Archive for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K, V, R, O> Serialize<Serializer> for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

/*pub trait Deserializable: Archive<Archived = Self::ArchivedDeser> + Sized {
    type ArchivedDeser: Deserialize<Self, Deserializer>;
}
impl<T: Archive> Deserializable for T
where
    Archived<T>: Deserialize<T, Deserializer>,
{
    type ArchivedDeser = Archived<T>;
*/

impl<K, V, R, O> BatchReader for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Key = K;
    type Val = V;
    type Time = ();
    type R = R;
    type Cursor<'s> = VecIndexedWSetCursor<'s, K, V, R, O>
    where
        V: 's,
        O: 's;
    type Factories = VecIndexedWSetFactories<K, V, R>;
    // type Consumer = VecIndexedWSetConsumer<K, V, R, O>;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        VecIndexedWSetCursor::new(self)
    }

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    /*#[inline]
    fn consumer(self) -> Self::Consumer {
        VecIndexedWSetConsumer {
            consumer: OrderedLayerConsumer::from(self.layer),
        }
    }*/

    #[inline]
    fn key_count(&self) -> usize {
        self.layer.keys()
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
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
        self.layer.truncate_keys_below(lower_bound);
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K, V, R, O> Batch for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = VecIndexedWSetBuilder<K, V, R, O>;
    type Merger = VecIndexedWSetMerger<K, V, R, O>;

    /*fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        Self::from_tuples(
            time,
            keys.into_iter()
                .map(|(k, w)| ((k, From::from(())), w))
                .collect(),
        )
    }*/

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        VecIndexedWSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct VecIndexedWSetMerger<K, V, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <Layers<K, V, R, O> as Trie>::MergeBuilder,
    #[size_of(skip)]
    factories: VecIndexedWSetFactories<K, V, R>,
}

impl<K, V, R, O> Merger<K, V, (), R, VecIndexedWSet<K, V, R, O>>
    for VecIndexedWSetMerger<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[inline]
    fn new_merger(
        batch1: &VecIndexedWSet<K, V, R, O>,
        batch2: &VecIndexedWSet<K, V, R, O>,
    ) -> Self {
        Self {
            lower1: batch1.layer.lower_bound(),
            upper1: batch1.layer.lower_bound() + batch1.layer.keys(),
            lower2: batch2.layer.lower_bound(),
            upper2: batch2.layer.lower_bound() + batch2.layer.keys(),
            result: <<Layers<K, V, R, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                &batch1.layer,
                &batch2.layer,
            ),
            factories: batch1.factories.clone(),
        }
    }

    #[inline]
    fn done(self) -> VecIndexedWSet<K, V, R, O> {
        VecIndexedWSet {
            layer: self.result.done(),
            factories: self.factories,
        }
    }

    fn work(
        &mut self,
        source1: &VecIndexedWSet<K, V, R, O>,
        source2: &VecIndexedWSet<K, V, R, O>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        // Use the more expensive `push_merge_truncate_values_fueled`
        // method if we need to remove truncated values during merging.
        match (key_filter, value_filter) {
            (Some(key_filter), Some(value_filter)) => {
                self.result.push_merge_retain_values_fueled(
                    (&source1.layer, &mut self.lower1, self.upper1),
                    (&source2.layer, &mut self.lower2, self.upper2),
                    key_filter,
                    value_filter,
                    fuel,
                );
            }
            (Some(key_filter), None) => {
                self.result.push_merge_retain_keys_fueled(
                    (&source1.layer, &mut self.lower1, self.upper1),
                    (&source2.layer, &mut self.lower2, self.upper2),
                    key_filter,
                    fuel,
                );
            }
            (None, Some(value_filter)) => {
                self.result.push_merge_retain_values_fueled(
                    (&source1.layer, &mut self.lower1, self.upper1),
                    (&source2.layer, &mut self.lower2, self.upper2),
                    &|_| true,
                    value_filter,
                    fuel,
                );
            }
            (None, None) => {
                self.result.push_merge_fueled(
                    (&source1.layer, &mut self.lower1, self.upper1),
                    (&source2.layer, &mut self.lower2, self.upper2),
                    fuel,
                );
            }
        }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct VecIndexedWSetCursor<'s, K, V, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    pub(crate) cursor: LayerCursor<'s, K, Leaf<V, R>, O>,
}

impl<'s, K, V, R, O> VecIndexedWSetCursor<'s, K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    pub fn new(zset: &'s VecIndexedWSet<K, V, R, O>) -> Self {
        Self {
            cursor: zset.layer.cursor(),
        }
    }

    pub fn new_from(zset: &'s VecIndexedWSet<K, V, R, O>, lower_bound: usize) -> Self {
        Self {
            cursor: zset
                .layer
                .cursor_from(lower_bound, zset.layer.lower_bound() + zset.layer.keys()),
        }
    }
}
impl<'s, K, V, R, O> Clone for VecIndexedWSetCursor<'s, K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, K, V, R, O> Cursor<K, V, (), R> for VecIndexedWSetCursor<'s, K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    // fn key_factory(&self) -> &'static Factory<K> {
    //     self.cursor.storage.factories.key
    // }

    // fn val_factory(&self) -> &'static Factory<V> {
    //     self.cursor.child.storage.factories.key
    // }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.child.storage.factories.diff
    }

    fn key(&self) -> &K {
        self.cursor.item()
    }

    fn val(&self) -> &V {
        self.cursor.child.current_key()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.cursor.child.valid() {
            logic(&(), self.cursor.child.current_diff())
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn weight(&mut self) -> &R {
        debug_assert!(&self.cursor.valid());
        self.cursor.child.current_diff()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.val_valid() {
            logic(self.val(), self.cursor.child.current_diff());
            self.step_val();
        }
    }

    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.child.valid()
    }

    fn step_key(&mut self) {
        self.cursor.step();
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_reverse();
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek(key);
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with(predicate);
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_reverse(key);
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with_reverse(predicate);
    }

    fn step_val(&mut self) {
        self.cursor.child.step();
    }

    fn seek_val(&mut self, val: &V) {
        self.cursor.child.seek(val);
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.cursor.child.seek_key_with(predicate);
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind();
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward();
    }

    fn rewind_vals(&mut self) {
        self.cursor.child.rewind();
    }

    fn step_val_reverse(&mut self) {
        self.cursor.child.step_reverse();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.cursor.child.seek_reverse(val);
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.cursor.child.seek_key_with_reverse(predicate);
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.child.fast_forward();
    }
}

impl<'s, K, V, R> HasTimeDiffCursor<K, V, (), R> for VecIndexedWSetCursor<'s, K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    type TimeDiffCursor<'a> = SingletonTimeDiffCursor<'a, R>
    where
        Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        SingletonTimeDiffCursor::new(self.val_valid().then(|| self.cursor.child.current_diff()))
    }
}

type IndexBuilder<K, V, R, O> = LayerBuilder<K, LeafBuilder<V, R>, O>;

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecIndexedWSetBuilder<K, V, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    builder: IndexBuilder<K, V, R, O>,
    #[size_of(skip)]
    factories: VecIndexedWSetFactories<K, V, R>,
    // weighted_item_factory: &'static dyn Factory<dyn DynPair<K, V>, R>,
    // batch_item_factory: &'static BatchItemFactory<K, V, Pair<K, V>, R>,
}

impl<K, V, R, O> Builder<VecIndexedWSet<K, V, R, O>> for VecIndexedWSetBuilder<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[inline]
    fn new_builder(factories: &VecIndexedWSetFactories<K, V, R>, _time: ()) -> Self {
        Self {
            builder: IndexBuilder::<K, V, R, O>::new(&factories.layer_factories),
            factories: factories.clone(),
        }
    }

    #[inline]
    fn with_capacity(
        factories: &VecIndexedWSetFactories<K, V, R>,
        _time: (),
        capacity: usize,
    ) -> Self {
        Self {
            builder: <IndexBuilder<K, V, R, O> as TupleBuilder>::with_capacity(
                &factories.layer_factories,
                capacity,
            ),
            factories: factories.clone(),
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, kvr: &mut DynPair<DynPair<K, V>, R>) {
        let (kv, r) = kvr.split_mut();
        let (k, v) = kv.split_mut();
        self.builder.push_tuple((k, (v, r)));
    }

    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        self.builder.push_refs((key, (val, weight)))
    }

    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        self.builder.push_tuple((key, (val, weight)))
    }

    #[inline(never)]
    fn done(self) -> VecIndexedWSet<K, V, R, O> {
        VecIndexedWSet {
            layer: self.builder.done(),
            factories: self.factories,
        }
    }
}

impl<K, V, R> TimedBuilder<VecIndexedWSet<K, V, R>> for VecIndexedWSetBuilder<K, V, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn push_time(&mut self, key: &K, val: &V, _time: &(), weight: &R) {
        self.push_refs(key, val, weight);
    }

    fn done_with_bounds(
        self,
        _lower: Antichain<()>,
        _upper: Antichain<()>,
    ) -> VecIndexedWSet<K, V, R> {
        self.done()
    }
}

/*pub struct VecIndexedWSetConsumer<K, V, R, O>
where
    K: 'static,
    V: 'static,
    R: 'static,
    O: OrdOffset,
{
    consumer: OrderedLayerConsumer<K, V, R, O>,
}

impl<K, V, R, O> Consumer<K, V, R, ()> for VecIndexedWSetConsumer<K, V, R, O>
where
    O: OrdOffset,
{
    type ValueConsumer<'a> = VecIndexedWSetValueConsumer<'a, K, V,  R, O>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    fn peek_key(&self) -> &K {
        self.consumer.peek_key()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, consumer) = self.consumer.next_key();
        (key, VecIndexedWSetValueConsumer::new(consumer))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key)
    }
}

pub struct VecIndexedWSetValueConsumer<'a, K, V, R, O>
where
    V: 'static,
    R: 'static,
{
    consumer: OrderedLayerValues<'a, V, R>,
    __type: PhantomData<(K, O)>,
}

impl<'a, K, V, R, O> VecIndexedWSetValueConsumer<'a, K, V, R, O> {
    #[inline]
    const fn new(consumer: OrderedLayerValues<'a, V, R>) -> Self {
        Self {
            consumer,
            __type: PhantomData,
        }
    }
}

impl<'a, K, V, R, O> ValueConsumer<'a, V, R, ()> for VecIndexedWSetValueConsumer<'a, K, V, R, O> {
    fn value_valid(&self) -> bool {
        self.consumer.value_valid()
    }

    fn next_value(&mut self) -> (V, R, ()) {
        self.consumer.next_value()
    }

    fn remaining_values(&self) -> usize {
        self.consumer.remaining_values()
    }
}*/
