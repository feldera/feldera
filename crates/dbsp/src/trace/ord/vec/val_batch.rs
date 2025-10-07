use crate::trace::cursor::Position;
use crate::trace::ord::merge_batcher::MergeBatcher;
use crate::{
    algebra::Lattice,
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WithFactory,
    },
    trace::{
        layers::{
            Cursor as TrieCursor, Layer, LayerCursor, LayerFactories, Leaf, LeafFactories,
            OrdOffset, Trie,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Deserializer,
        Serializer,
    },
    utils::{ConsolidatePairedSlices, Tup2},
    DBData, DBWeight, NumEntries, Timestamp,
};
use feldera_storage::StoragePath;
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug, Display, Formatter};

pub type VecValBatchLayer<K, V, T, R, O> = Layer<K, Layer<V, Leaf<DynDataTyped<T>, R>, O>, O>;

pub struct VecValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    layer_factories: LayerFactories<K, LayerFactories<V, LeafFactories<DynDataTyped<T>, R>>>,
    item_factory: &'static dyn Factory<DynPair<K, V>>,
    consolidate_weights: &'static dyn ConsolidatePairedSlices<DynDataTyped<T>, R>,
    weighted_item_factory: &'static dyn Factory<DynPair<DynPair<K, V>, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>>,
    weighted_vals_factory: &'static dyn Factory<DynWeightedPairs<V, R>>,
    time_diffs_factory: &'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>,
}

impl<K, V, T, R> Clone for VecValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            item_factory: self.item_factory,
            consolidate_weights: self.consolidate_weights,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
            weighted_vals_factory: self.weighted_vals_factory,
            time_diffs_factory: self.time_diffs_factory,
        }
    }
}

unsafe impl<K, V, T, R> Send for VecValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
}

impl<K, V, T, R> BatchReaderFactories<K, V, T, R> for VecValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            layer_factories: LayerFactories::new::<KType>(LayerFactories::new::<VType>(
                LeafFactories::new::<T, RType>(),
            )),
            item_factory: WithFactory::<Tup2<KType, VType>>::FACTORY,
            consolidate_weights: <dyn ConsolidatePairedSlices<_, _>>::factory::<T, RType>(),
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, VType>, RType>>::FACTORY,
            weighted_items_factory:
                WithFactory::<LeanVec<Tup2<Tup2<KType, VType>, RType>>>::FACTORY,
            weighted_vals_factory: WithFactory::<LeanVec<Tup2<VType, RType>>>::FACTORY,
            time_diffs_factory: WithFactory::<LeanVec<Tup2<T, RType>>>::FACTORY,
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
        self.layer_factories.child.child.diff
    }
}

impl<K, V, T, R> BatchFactories<K, V, T, R> for VecValBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    // type BatchItemVTable = BatchItemVTable<K, V, Pair<K, V>, R>;

    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<DynPair<DynPair<K, V>, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        self.weighted_items_factory
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<V, R>> {
        self.weighted_vals_factory
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<T>, R>>> {
        Some(self.time_diffs_factory)
    }

    // fn weighted_item_factory(&self) -> &'static WeightedVTable<Pair<K, V>, R> {
    //     self.weighted_item_factory
    // }

    // fn batch_item_factory(&self) -> &'static BatchItemVTable<K, V, Pair<K, V>, R>
    // {     self.batch_item_factory
    // }

    /*fn item_from<'a, MK, MV, MR>(
        &self,
        key: MK,
        val: MV,
        weight: MR,
        item: Uninit<'_, Self::WeightedItem>,
    ) where
        MK: MaybeOwned<'a, K>,
        MV: MaybeOwned<'a, V>,
        MR: MaybeOwned<'a, R>,
    {
        let (keyval_uninit, weight_uninit) = self.weighted_item_factory.split_uninit(item);
        let (key_uninit, val_uninit) = self.item_factory.split_uninit(keyval_uninit);
        self.layer_factorys.key.write_uninit(key, key_uninit);
        self.layer_factorys.child.key.write_uninit(val, val_uninit);
        self.layer_factorys
            .child
            .child
            .diff
            .write_uninit(weight, weight_uninit);
    }*/
}

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(SizeOf)]
pub struct VecValBatch<K, V, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    #[size_of(skip)]
    factories: VecValBatchFactories<K, V, T, R>,

    // #[size_of(skip)]
    // item_factory: &'static PairVTable<K, V>,
    // #[size_of(skip)]
    // weighted_item_factory: &'static WeightedVTable<Pair<K, V>, R>,
    // #[size_of(skip)]
    // batch_item_factory: &'static BatchItemVTable<K, V, Pair<K, V>, R>,
    /// Where all the dataz is.
    pub layer: VecValBatchLayer<K, V, T, R, O>,
}

unsafe impl<K, V, T, R, O> Send for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
}

impl<K, V, T, R, O> Debug for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecValBatch")
            .field("layer", &self.layer)
            .finish()
    }
}

impl<K, V, T: Lattice, R, O: OrdOffset> Deserialize<VecValBatch<K, V, T, R, O>, Deserializer> for ()
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<VecValBatch<K, V, T, R, O>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, V, T: Lattice, R, O: OrdOffset> Archive for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K, V, T: Lattice, R, O: OrdOffset> Serialize<Serializer> for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, V, T, R, O> Clone for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            factories: self.factories.clone(),
            layer: self.layer.clone(),
        }
    }
}

impl<K, V, T, R, O> NumEntries for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    const CONST_NUM_ENTRIES: Option<usize> = <VecValBatchLayer<K, V, T, R, O>>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, V, T, R, O> Display for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, V, T, R, O> BatchReader for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;
    type Factories = VecValBatchFactories<K, V, T, R>;

    type Cursor<'s>
        = VecValCursor<'s, K, V, T, R, O>
    where
        O: 's;

    // type Consumer = VecValConsumer<K, V, T, R, O>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        VecValCursor {
            cursor: self.layer.cursor(),
        }
    }

    /*fn consumer(self) -> Self::Consumer {
        todo!()
    }*/

    fn key_count(&self) -> usize {
        <VecValBatchLayer<K, V, T, R, O> as Trie>::keys(&self.layer)
    }

    fn len(&self) -> usize {
        <VecValBatchLayer<K, V, T, R, O> as Trie>::tuples(&self.layer)
    }

    fn approximate_byte_size(&self) -> usize {
        self.size_of().total_bytes()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }
}

impl<K, V, T, R, O> Batch for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    type Timed<T2: Timestamp> = VecValBatch<K, V, T2, R, O>;
    type Batcher = MergeBatcher<Self>;
    type Builder = VecValBuilder<K, V, T, R, O>;

    fn checkpoint_path(&self) -> Option<&StoragePath> {
        unimplemented!()
    }

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
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct VecValCursor<'s, K, V, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    cursor: LayerCursor<'s, K, Layer<V, Leaf<DynDataTyped<T>, R>, O>, O>,
}

impl<K, V, T, R, O> Clone for VecValCursor<'_, K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor.clone(),
        }
    }
}

impl<K, V, T, R, O> Cursor<K, V, T, R> for VecValCursor<'_, K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.child.child.storage.factories.diff
    }

    fn key(&self) -> &K {
        self.cursor.item()
    }

    fn val(&self) -> &V {
        self.cursor.child.item()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.cursor.child.child.rewind();
        while self.cursor.child.child.valid() {
            logic(
                self.cursor.child.child.current_key(),
                self.cursor.child.child.current_diff(),
            );
            self.cursor.child.child.step();
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.cursor.child.child.rewind();
        while self.cursor.child.child.valid() {
            if self.cursor.child.child.item().0.less_equal(upper) {
                logic(
                    self.cursor.child.child.current_key(),
                    self.cursor.child.child.current_diff(),
                );
            }
            self.cursor.child.child.step();
        }
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            logic(self.val(), self.cursor.child.child.current_diff());
            self.step_val();
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(&self.cursor.valid());
        self.cursor.child.child.current_diff()
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
    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with_reverse(predicate);
    }
    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_reverse(key);
    }
    fn step_val(&mut self) {
        self.cursor.child.step();
    }
    fn seek_val(&mut self, val: &V) {
        self.cursor.child.seek(val);
    }
    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.cursor.child.seek_with(predicate);
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
        self.cursor.child.seek_with_reverse(predicate);
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.child.fast_forward();
    }

    fn position(&self) -> Option<Position> {
        Some(Position {
            total: TrieCursor::keys(&self.cursor) as u64,
            offset: self.cursor.pos() as u64,
        })
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecValBuilder<K, V, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    #[size_of(skip)]
    factories: VecValBatchFactories<K, V, T, R>,
    keys: Box<DynVec<K>>,
    offs: Vec<O>,
    vals: Box<DynVec<V>>,
    val_offs: Vec<O>,
    times: Box<DynVec<DynDataTyped<T>>>,
    diffs: Box<DynVec<R>>,
}

impl<K, V, T, R, O> VecValBuilder<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn pushed_key(&mut self) {
        let off = O::from_usize(self.vals.len());
        debug_assert!(off > *self.offs.last().unwrap());
        self.offs.push(off);
    }

    fn pushed_val(&mut self) {
        let val_off = O::from_usize(self.times.len());
        debug_assert!(val_off > *self.val_offs.last().unwrap());
        self.val_offs.push(val_off);
    }
}

impl<K, V, T, R, O> Builder<VecValBatch<K, V, T, R, O>> for VecValBuilder<K, V, T, R, O>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn with_capacity(factories: &VecValBatchFactories<K, V, T, R>, capacity: usize) -> Self {
        let mut keys = factories.layer_factories.keys.default_box();
        keys.reserve_exact(capacity);

        let mut offs = Vec::with_capacity(capacity + 1);
        offs.push(O::zero());

        let mut vals = factories.layer_factories.child.keys.default_box();
        vals.reserve_exact(capacity);

        let mut val_offs = Vec::with_capacity(capacity + 1);
        val_offs.push(O::zero());

        let mut times = factories.layer_factories.child.child.keys.default_box();
        times.reserve_exact(capacity);

        let mut diffs = factories.layer_factories.child.child.diffs.default_box();
        diffs.reserve_exact(capacity);
        Self {
            factories: factories.clone(),
            keys,
            offs,
            vals,
            val_offs,
            times,
            diffs,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
        self.offs.reserve(additional);
        self.vals.reserve(additional);
        self.times.reserve(additional);
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

    fn push_time_diff(&mut self, time: &T, weight: &R) {
        debug_assert!(!weight.is_zero());
        self.times.push(time.clone());
        self.diffs.push_ref(weight);
    }

    fn push_time_diff_mut(&mut self, time: &mut T, weight: &mut R) {
        debug_assert!(!weight.is_zero());
        self.times.push(time.clone());
        self.diffs.push_val(weight);
    }

    fn done(self) -> VecValBatch<K, V, T, R, O> {
        VecValBatch {
            layer: Layer::from_parts(
                &self.factories.layer_factories,
                self.keys,
                self.offs,
                Layer::from_parts(
                    &self.factories.layer_factories.child,
                    self.vals,
                    self.val_offs,
                    Leaf::from_parts(
                        &self.factories.layer_factories.child.child,
                        self.times,
                        self.diffs,
                    ),
                ),
            ),
            factories: self.factories,
        }
    }

    fn num_tuples(&self) -> usize {
        self.diffs.len()
    }
}

/*pub struct VecValConsumer<K, V, T, R, O> {
    __type: PhantomData<(K, V, T, R, O)>,
}

impl<K, V, T, R, O> Consumer<K, V, R, T> for VecValConsumer<K, V, T, R, O> {
    type ValueConsumer<'a> = VecValValueConsumer<'a, K, V, T, R, O>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &K {
        todo!()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}

pub struct VecValValueConsumer<'a, K, V, T, R, O> {
    __type: PhantomData<&'a (K, V, T, R, O)>,
}

impl<'a, K, V, T, R, O> ValueConsumer<'a, V, R, T> for VecValValueConsumer<'a, K, V, T, R, O> {
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (V, R, T) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}
*/
