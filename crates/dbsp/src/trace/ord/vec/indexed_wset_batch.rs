use crate::{
    algebra::{NegByRef, ZRingValue},
    circuit::checkpointer::Checkpoint,
    dynamic::{
        DataTrait, DynPair, DynVec, DynWeightedPairs, Erase, Factory, LeanVec, WeightTrait,
        WeightTraitTyped, WithFactory,
    },
    trace::{
        cursor::Position,
        deserialize_indexed_wset,
        layers::{
            Cursor as _, Layer, LayerCursor, LayerFactories, Leaf, LeafFactories, OrdOffset, Trie,
        },
        serialize_indexed_wset, Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder,
        Cursor, Deserializer, Filter, MergeCursor, Serializer, VecValBatch, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, Error, NumEntries,
};
use itertools::{EitherOrBoth, Itertools};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug, Display};

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
    weighted_vals_factory: &'static dyn Factory<DynWeightedPairs<V, R>>,
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
            weighted_vals_factory: self.weighted_vals_factory,
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
            weighted_vals_factory: WithFactory::<LeanVec<Tup2<VType, RType>>>::FACTORY,
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

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>> {
        self.weighted_vals_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, V, R>> {
        self.weighted_item_factory
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<crate::dynamic::DynDataTyped<()>, R>>> {
        None
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

impl<K, V, R, O> VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    pub fn from_parts(
        factories: VecIndexedWSetFactories<K, V, R>,
        keys: Box<DynVec<K>>,
        offs: Vec<O>,
        vals: Box<DynVec<V>>,
        diffs: Box<DynVec<R>>,
    ) -> Self {
        Self {
            layer: Layer::from_parts(
                &factories.layer_factories,
                keys,
                offs,
                Leaf::from_parts(&factories.layer_factories.child, vals, diffs),
            ),
            factories,
        }
    }
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
    type Cursor<'s>
        = VecIndexedWSetCursor<'s, K, V, R, O>
    where
        V: 's,
        O: 's;
    type Factories = VecIndexedWSetFactories<K, V, R>;
    // type Consumer = VecIndexedWSetConsumer<K, V, R, O>;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        VecIndexedWSetCursor::new(self)
    }

    fn consuming_cursor(
        &mut self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn crate::trace::MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_>
    {
        if key_filter.is_none() && value_filter.is_none() {
            Box::new(VecIndexedWSetConsumingCursor::new(self))
        } else {
            self.merge_cursor(key_filter, value_filter)
        }
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
    fn approximate_byte_size(&self) -> usize {
        self.size_of().total_bytes()
    }

    fn filter_size(&self) -> usize {
        0
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }

    fn keys(&self) -> Option<&DynVec<Self::Key>> {
        Some(&*self.layer.keys)
    }
}

impl<K, V, R, O> Batch for VecIndexedWSet<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Timed<T: crate::Timestamp> = VecValBatch<K, V, T, R, O>;
    type Batcher = MergeBatcher<Self>;
    type Builder = VecIndexedWSetBuilder<K, V, R, O>;
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
            cursor: zset.layer.cursor_from(lower_bound, zset.layer.keys()),
        }
    }
}
impl<K, V, R, O> Clone for VecIndexedWSetCursor<'_, K, V, R, O>
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

impl<K, V, R, O> Cursor<K, V, (), R> for VecIndexedWSetCursor<'_, K, V, R, O>
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

    fn seek_key_exact(&mut self, key: &K, _hash: Option<u64>) -> bool {
        self.seek_key(key);
        self.key_valid() && self.key().eq(key)
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

    fn position(&self) -> Option<Position> {
        Some(Position {
            total: self.cursor.keys() as u64,
            offset: self.cursor.pos() as u64,
        })
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecIndexedWSetBuilder<K, V, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[size_of(skip)]
    factories: VecIndexedWSetFactories<K, V, R>,
    keys: Box<DynVec<K>>,
    offs: Vec<O>,
    vals: Box<DynVec<V>>,
    diffs: Box<DynVec<R>>,
}

impl<K, V, R, O> VecIndexedWSetBuilder<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn pushed_key(&mut self) {
        self.offs.push(O::from_usize(self.vals.len()));

        debug_assert!(
            {
                let n = self.offs.len();
                self.offs[n - 1] > self.offs[n - 2]
            },
            "every key must have at least one value"
        );

        debug_assert!(
            {
                let n = self.keys.len();
                n == 1 || self.keys[n - 2] < self.keys[n - 1]
            },
            "keys must be strictly monotonically increasing but {:?} >= {:?}",
            &self.keys[self.keys.len() - 2],
            &self.keys[self.keys.len() - 1]
        );
    }

    fn pushed_val(&self) {
        debug_assert_eq!(
            self.vals.len(),
            self.diffs.len(),
            "every value must have exactly one diff"
        );

        debug_assert!(
            {
                let n = self.vals.len();
                let last_n = self.offs.last().unwrap().into_usize();
                let n_vals = n - last_n;
                n_vals < 2 || self.vals[n - 2] < self.vals[n - 1]
            },
            "values for a key must be strictly monotonically increasing but {:?} >= {:?}",
            &self.vals[self.vals.len() - 2],
            &self.vals[self.vals.len() - 1]
        );
    }

    fn pushed_diff(&self) {
        debug_assert_eq!(
            self.vals.len() + 1,
            self.diffs.len(),
            "every value must have exactly one diff"
        );
    }

    /// Copies the contents of this in-progress [Builder] to `dst`.
    ///
    /// This handles all the possible states that this builder can be in (such
    /// as time-diff pairs without a value yet, and values without a key yet)
    /// and reproduces them in `dst`.
    pub fn copy_to_builder<B, BO>(&self, dst: &mut B)
    where
        B: Builder<BO>,
        BO: Batch<Key = K, Val = V, R = R, Time = ()>,
    {
        let mut key_index = 0;
        for (val_diff, val_index) in self
            .vals
            .dyn_iter()
            .zip_longest(self.diffs.dyn_iter())
            .zip(1..)
        {
            match val_diff {
                EitherOrBoth::Both(val, diff) => {
                    dst.push_val_diff(val, diff);
                    if self
                        .offs
                        .get(key_index + 1)
                        .is_some_and(|val_offset| O::from_usize(val_index) >= *val_offset)
                    {
                        dst.push_key(&self.keys[key_index]);
                        key_index += 1;
                    }
                }
                EitherOrBoth::Left(_) => unreachable!(),
                EitherOrBoth::Right(diff) => {
                    dst.push_time_diff(&(), diff);
                }
            }
        }
    }
}

impl<K, V, R, O> Builder<VecIndexedWSet<K, V, R, O>> for VecIndexedWSetBuilder<K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn with_capacity(
        factories: &VecIndexedWSetFactories<K, V, R>,
        key_capacity: usize,
        value_capacity: usize,
    ) -> Self {
        let mut keys = factories.layer_factories.keys.default_box();
        keys.reserve_exact(key_capacity);

        let mut offs = Vec::with_capacity(key_capacity + 1);
        offs.push(O::zero());

        let mut vals = factories.layer_factories.child.keys.default_box();
        vals.reserve_exact(value_capacity);

        let mut diffs = factories.layer_factories.child.diffs.default_box();
        diffs.reserve_exact(value_capacity);
        Self {
            factories: factories.clone(),
            keys,
            offs,
            vals,
            diffs,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.vals.reserve(additional);
        self.diffs.reserve(additional);
    }

    fn push_key(&mut self, key: &K) {
        self.keys.push_ref(key);
        self.pushed_key();
    }

    fn push_key_mut(&mut self, key: &mut K) {
        self.keys.push_val(key);
        self.pushed_key();
    }

    fn push_val(&mut self, val: &V) {
        self.vals.push_ref(val);
        self.pushed_val();
    }

    fn push_val_mut(&mut self, val: &mut V) {
        self.vals.push_val(val);
        self.pushed_val();
    }

    fn push_time_diff(&mut self, _time: &(), weight: &R) {
        debug_assert!(!weight.is_zero());
        self.diffs.push_ref(weight);
        self.pushed_diff();
    }

    fn push_time_diff_mut(&mut self, _time: &mut (), weight: &mut R) {
        debug_assert!(!weight.is_zero());
        self.diffs.push_val(weight);
        self.pushed_diff();
    }

    fn push_val_diff(&mut self, val: &V, weight: &R) {
        debug_assert!(!weight.is_zero());
        self.vals.push_ref(val);
        self.diffs.push_ref(weight);
        self.pushed_val();
    }

    fn push_val_diff_mut(&mut self, val: &mut V, weight: &mut R) {
        debug_assert!(!weight.is_zero());
        self.vals.push_val(val);
        self.diffs.push_val(weight);
        self.pushed_val();
    }

    fn done(self) -> VecIndexedWSet<K, V, R, O> {
        VecIndexedWSet::from_parts(self.factories, self.keys, self.offs, self.vals, self.diffs)
    }

    fn num_keys(&self) -> usize {
        self.keys.len()
    }

    fn num_tuples(&self) -> usize {
        self.diffs.len()
    }
}

/// A cursor for consuming a [VecIndexedWSet].
struct VecIndexedWSetConsumingCursor<'a, K, V, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    wset: &'a mut VecIndexedWSet<K, V, R, O>,
    key_index: usize,
    val_index: usize,
    val_max: usize,
}

impl<'a, K, V, R, O> VecIndexedWSetConsumingCursor<'a, K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn new(wset: &'a mut VecIndexedWSet<K, V, R, O>) -> Self {
        let val_max = wset
            .layer
            .offs
            .get(1)
            .map_or(0, |offset| offset.into_usize());
        Self {
            wset,
            key_index: 0,
            val_index: 0,
            val_max,
        }
    }
}

impl<K, V, R, O> MergeCursor<K, V, (), R> for VecIndexedWSetConsumingCursor<'_, K, V, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn key_valid(&self) -> bool {
        self.key_index < self.wset.layer.keys.len()
    }
    fn val_valid(&self) -> bool {
        self.val_index < self.val_max
    }
    fn key(&self) -> &K {
        self.wset.layer.keys.index(self.key_index)
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid());
        &self.wset.layer.vals.keys[self.val_index]
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        logic(&(), &self.wset.layer.vals.diffs[self.val_index])
    }

    fn weight(&mut self) -> &R {
        &self.wset.layer.vals.diffs[self.val_index]
    }

    fn has_mut(&self) -> bool {
        true
    }

    fn key_mut(&mut self) -> &mut K {
        &mut self.wset.layer.keys[self.key_index]
    }

    fn val_mut(&mut self) -> &mut V {
        &mut self.wset.layer.vals.keys[self.val_index]
    }

    fn weight_mut(&mut self) -> &mut R {
        &mut self.wset.layer.vals.diffs[self.val_index]
    }

    fn step_key(&mut self) {
        self.key_index += 1;
        if self.key_valid() {
            self.val_index = self.wset.layer.offs[self.key_index].into_usize();
            self.val_max = self.wset.layer.offs[self.key_index + 1].into_usize();
        } else {
            self.val_index = 0;
            self.val_max = 0;
        }
    }

    fn step_val(&mut self) {
        self.val_index += 1;
    }
}

impl<K, V, R> Checkpoint for VecIndexedWSet<K, V, R>
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
