use crate::{
    algebra::{NegByRef, ZRingValue},
    circuit::checkpointer::Checkpoint,
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        LeanVec, WeightTrait, WeightTraitTyped, WithFactory,
    },
    trace::{
        cursor::Position,
        deserialize_wset,
        layers::{Cursor as _, Leaf, LeafCursor, LeafFactories, Trie},
        ord::merge_batcher::MergeBatcher,
        serialize_wset, Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor,
        Deserializer, Filter, MergeCursor, Serializer, VecKeyBatch, WeightedItem,
    },
    utils::Tup2,
    DBData, DBWeight, NumEntries,
};
use itertools::{EitherOrBoth, Itertools};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{self, Debug, Display};

pub struct VecWSetFactories<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    pub layer_factories: LeafFactories<K, R>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
    weighted_vals_factory: &'static dyn Factory<DynWeightedPairs<DynUnit, R>>,
    //pub batch_item_factory: &'static BatchItemFactory<K, (), K, R>,
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for VecWSetFactories<K, R> {
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
            weighted_vals_factory: self.weighted_vals_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> BatchReaderFactories<K, DynUnit, (), R>
    for VecWSetFactories<K, R>
{
    fn new<KType, VType, RType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<DynUnit>,
        RType: DBWeight + Erase<R>,
    {
        Self {
            layer_factories: LeafFactories::new::<KType, RType>(),
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
            weighted_vals_factory: WithFactory::<LeanVec<Tup2<(), RType>>>::FACTORY,
        }
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        self.layer_factories.key
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        self.layer_factories.keys
    }

    fn val_factory(&self) -> &'static dyn Factory<DynUnit> {
        WithFactory::<()>::FACTORY
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.layer_factories.diff
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> BatchFactories<K, DynUnit, (), R>
    for VecWSetFactories<K, R>
{
    //type BatchItemFactory = BatchItemFactory<K, (), K, R>;

    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, DynUnit>> {
        self.item_factory
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<WeightedItem<K, DynUnit, R>> {
        self.weighted_item_factory
    }

    fn weighted_items_factory(
        &self,
    ) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>> {
        self.weighted_items_factory
    }

    fn weighted_vals_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynUnit, R>> {
        self.weighted_vals_factory
    }

    fn time_diffs_factory(
        &self,
    ) -> Option<&'static dyn Factory<DynWeightedPairs<DynDataTyped<()>, R>>> {
        None
    }
}

/// An immutable collection of `(key, weight)` pairs without timing information.
#[derive(SizeOf)]
pub struct VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[doc(hidden)]
    pub layer: Leaf<K, R>,
    #[size_of(skip)]
    factories: VecWSetFactories<K, R>,
    // #[size_of(skip)]
    // weighted_item_factory: &'static WeightedFactory<K, R>,
    // #[size_of(skip)]
    // batch_item_factory: &'static BatchItemFactory<K, (), K, R>,
}

impl<K, R> VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn from_parts(
        factories: VecWSetFactories<K, R>,
        keys: Box<DynVec<K>>,
        diffs: Box<DynVec<R>>,
    ) -> Self {
        Self {
            layer: Leaf::from_parts(&factories.layer_factories, keys, diffs),
            factories,
        }
    }
}

impl<K, R> PartialEq for VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.layer == other.layer
    }
}

impl<K, R> Checkpoint for VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn checkpoint(&self) -> Result<Vec<u8>, crate::Error> {
        Ok(serialize_wset(self))
    }

    fn restore(&mut self, data: &[u8]) -> Result<(), crate::Error> {
        *self = deserialize_wset(&self.factories, data);
        Ok(())
    }
}

impl<K, R> Eq for VecWSet<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Debug for VecWSet<K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecWSet")
            .field("layer", &self.layer)
            .finish()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Clone for VecWSet<K, R> {
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
            factories: self.factories.clone(),
            //weighted_item_factory: self.weighted_item_factory,
            //batch_item_factory: self.batch_item_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> VecWSet<K, R> {
    #[inline]
    pub fn len(&self) -> usize {
        self.layer.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.layer.is_empty()
    }

    /*#[inline]
    pub fn retain(&mut self, retain: &dyn Fn(&K, &R) -> bool) {
        self.layer.retain(retain);
    }*/

    /*
    #[doc(hidden)]
    #[inline]
    pub fn from_columns(mut keys: ErasedVec<K>, mut diffs: ErasedVec<R>) -> Self {
        consolidate_payload_from(&mut keys, &mut diffs, 0);

        Self {
            // Safety: We've ensured that keys and diffs are the same length
            // and are sorted & consolidated
            layer: unsafe { Leaf::from_parts(keys, diffs, 0) },
        }
    }*/
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Deserialize<VecWSet<K, R>, Deserializer>
    for ()
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<VecWSet<K, R>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Archive for VecWSet<K, R> {
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Serialize<Serializer> for VecWSet<K, R> {
    fn serialize(
        &self,
        _serializer: &mut Serializer,
    ) -> Result<Self::Resolver, <Serializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Display for VecWSet<K, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

/*impl<K, R, KR> From<Leaf<K, R, KR>> for VecZSet<K, R, KR> {
    fn from(layer: Leaf<K, R, KR>) -> Self {
        Self { layer }
    }
}

impl<K, R, KR> From<Leaf<K, R, KR>> for Rc<VecZSet<K, R, KR>> {
    fn from(layer: Leaf<K, R, KR>) -> Self {
        Rc::new(From::from(layer))
    }
}*/

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> NumEntries for VecWSet<K, R> {
    const CONST_NUM_ENTRIES: Option<usize> = <Leaf<K, R>>::CONST_NUM_ENTRIES;

    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K: DataTrait + ?Sized, R: WeightTraitTyped + ?Sized> NegByRef for VecWSet<K, R>
where
    R::Type: DBWeight + ZRingValue + Erase<R>,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            factories: self.factories.clone(),
            //weighted_item_factory: self.weighted_item_factory,
            //batch_item_factory: self.batch_item_factory,
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> BatchReader for VecWSet<K, R> {
    type Key = K;
    type Val = DynUnit;
    type Time = ();
    type R = R;
    type Cursor<'s> = VecWSetCursor<'s, K, R>;
    type Factories = VecWSetFactories<K, R>;
    // type Consumer = VecZSetConsumer<K, R>;

    #[inline]
    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        VecWSetCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    fn consuming_cursor(
        &mut self,
        key_filter: Option<Filter<Self::Key>>,
        value_filter: Option<Filter<Self::Val>>,
    ) -> Box<dyn crate::trace::MergeCursor<Self::Key, Self::Val, Self::Time, Self::R> + Send + '_>
    {
        if key_filter.is_none() && value_filter.is_none() {
            Box::new(VecWSetConsumingCursor::new(self))
        } else {
            self.merge_cursor(key_filter, value_filter)
        }
    }

    /*
    #[inline]
    fn consumer(self) -> Self::Consumer {
        VecZSetConsumer {
            consumer: ColumnLayerConsumer::from(self.layer),
        }
    }*/

    #[inline]
    fn key_count(&self) -> usize {
        Trie::keys(&self.layer)
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
    }

    #[inline]
    fn approximate_byte_size(&self) -> usize {
        self.size_of().total_bytes()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        self.layer.sample_keys(rng, sample_size, sample);
    }

    fn keys(&self) -> Option<&DynVec<Self::Key>> {
        Some(&*self.layer.keys)
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Batch for VecWSet<K, R> {
    type Timed<T: crate::Timestamp> = VecKeyBatch<K, T, R>;
    type Batcher = MergeBatcher<Self>;
    type Builder = VecWSetBuilder<K, R>;

    /*fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }*/
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct VecWSetCursor<'s, K: DataTrait + ?Sized, R: WeightTrait + ?Sized> {
    valid: bool,
    pub(crate) cursor: LeafCursor<'s, K, R>,
}

impl<K, R> Clone for VecWSetCursor<'_, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            valid: self.valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> Cursor<K, DynUnit, (), R>
    for VecWSetCursor<'_, K, R>
{
    // fn key_factory(&self) -> &'static Factory<K> {
    //     self.cursor.storage.vtables.key
    // }

    // fn val_factory(&self) -> &'static Factory<()> {
    //     todo!()
    // }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.storage.factories.diff
    }

    fn key(&self) -> &K {
        self.cursor.current_key()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.cursor.valid() {
            logic(&(), self.cursor.current_diff())
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic)
    }

    fn weight(&mut self) -> &R {
        debug_assert!(&self.cursor.valid());
        self.cursor.current_diff()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R)) {
        if self.val_valid() {
            logic(&(), self.cursor.current_diff())
        }
    }

    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }

    fn val_valid(&self) -> bool {
        self.valid
    }

    fn step_key(&mut self) {
        self.cursor.step();
        self.valid = true;
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_reverse();
        self.valid = true;
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek(key);
        self.valid = true;
    }

    fn seek_key_exact(&mut self, key: &K) -> bool {
        self.seek_key(key);
        self.key_valid() && self.key().eq(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with(predicate);
        self.valid = true;
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with_reverse(predicate);
        self.valid = true;
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_reverse(key);
        self.valid = true;
    }

    fn step_val(&mut self) {
        self.valid = false;
    }

    fn seek_val(&mut self, _val: &DynUnit) {}

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind();
        self.valid = true;
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward();
        self.valid = true;
    }

    fn rewind_vals(&mut self) {
        self.valid = true;
    }

    fn step_val_reverse(&mut self) {
        self.valid = false;
    }

    fn seek_val_reverse(&mut self, _val: &DynUnit) {}

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(&()) {
            self.valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.valid = true;
    }

    fn position(&self) -> Option<Position> {
        Some(Position {
            total: self.cursor.keys() as u64,
            offset: self.cursor.position() as u64,
        })
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    #[size_of(skip)]
    factories: VecWSetFactories<K, R>,
    keys: Box<DynVec<K>>,
    val: bool,
    diffs: Box<DynVec<R>>,
}

impl<K, R> VecWSetBuilder<K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn pushed_key(&mut self) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(self.val, "every key must have exactly one value");
            debug_assert_eq!(
                self.keys.len(),
                self.diffs.len(),
                "every key must have exactly one diff"
            );
        }
        self.val = false;

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

    fn pushed_diff(&self) {
        #[cfg(debug_assertions)]
        debug_assert!(!self.val, "every val must have exactly one key");
        debug_assert_eq!(
            self.keys.len() + 1,
            self.diffs.len(),
            "every diff must have exactly one key"
        );
    }

    /// Copies the contents of this in-progress [Builder] to `dst`.
    ///
    /// This handles all the possible states that this builder can be in (such
    /// as a diff without a value, and a value without a key) and reproduces
    /// them in `dst`.
    pub fn copy_to_builder<B, BO>(&self, dst: &mut B)
    where
        B: Builder<BO>,
        BO: Batch<Key = K, Val = DynUnit, R = R, Time = ()>,
    {
        for key_diff in self.keys.dyn_iter().zip_longest(self.diffs.dyn_iter()) {
            match key_diff {
                EitherOrBoth::Both(key, diff) => {
                    dst.push_val_diff(&(), diff);
                    dst.push_key(key);
                }
                EitherOrBoth::Left(_) => unreachable!(),
                EitherOrBoth::Right(diff) => {
                    dst.push_diff(diff);
                    if self.val {
                        dst.push_val(&());
                    }
                }
            }
        }
    }
}

impl<K, R> Builder<VecWSet<K, R>> for VecWSetBuilder<K, R>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn with_capacity(factories: &VecWSetFactories<K, R>, capacity: usize) -> Self {
        let mut keys = factories.layer_factories.keys.default_box();
        keys.reserve_exact(capacity);

        let mut diffs = factories.layer_factories.diffs.default_box();
        diffs.reserve_exact(capacity);
        Self {
            factories: factories.clone(),
            keys,
            val: false,
            diffs,
        }
    }

    fn reserve(&mut self, additional: usize) {
        self.keys.reserve(additional);
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

    fn push_val(&mut self, _val: &DynUnit) {
        #[cfg(debug_assertions)]
        {
            debug_assert!(!self.val);
            debug_assert_eq!(
                self.diffs.len(),
                self.keys.len() + 1,
                "every value must have exactly one diff"
            );
        }

        self.val = true;
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

    fn done(self) -> VecWSet<K, R> {
        debug_assert_eq!(self.keys.len(), self.diffs.len());
        VecWSet {
            layer: Leaf::from_parts(&self.factories.layer_factories, self.keys, self.diffs),
            factories: self.factories,
        }
    }

    fn num_tuples(&self) -> usize {
        self.diffs.len()
    }
}

/// A cursor for consuming a [VecWSet].
struct VecWSetConsumingCursor<'a, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    wset: &'a mut VecWSet<K, R>,
    index: usize,
    val_valid: bool,
    value: Box<DynUnit>,
}

impl<'a, K, R> VecWSetConsumingCursor<'a, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn new(wset: &'a mut VecWSet<K, R>) -> Self {
        let val_valid = !wset.is_empty();
        let value = wset.factories.val_factory().default_box();
        Self {
            wset,
            index: 0,
            val_valid,
            value,
        }
    }
}

impl<K, R> MergeCursor<K, DynUnit, (), R> for VecWSetConsumingCursor<'_, K, R>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn key_valid(&self) -> bool {
        self.index < self.wset.layer.keys.len()
    }
    fn val_valid(&self) -> bool {
        self.val_valid
    }
    fn key(&self) -> &K {
        self.wset.layer.keys.index(self.index)
    }

    fn val(&self) -> &DynUnit {
        ().erase()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        logic(&(), &self.wset.layer.diffs[self.index])
    }

    fn weight(&mut self) -> &R {
        &self.wset.layer.diffs[self.index]
    }

    fn has_mut(&self) -> bool {
        true
    }

    fn key_mut(&mut self) -> &mut K {
        &mut self.wset.layer.keys[self.index]
    }

    fn val_mut(&mut self) -> &mut DynUnit {
        &mut *self.value
    }

    fn weight_mut(&mut self) -> &mut R {
        &mut self.wset.layer.diffs[self.index]
    }

    fn step_key(&mut self) {
        self.index += 1;
        self.val_valid = self.key_valid();
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }
}

/*
#[derive(Debug, SizeOf)]
pub struct VecZSetConsumer<K, R>
where
    K: 'static,
    R: 'static,
{
    consumer: ColumnLayerConsumer<K, R>,
}

impl<K, R> Consumer<K, (), R, ()> for VecZSetConsumer<K, R> {
    type ValueConsumer<'a> = VecZSetValueConsumer<'a, K, R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    fn peek_key(&self) -> &K {
        self.consumer.peek_key()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, values) = self.consumer.next_key();
        (key, VecZSetValueConsumer { values })
    }

    fn seek_key(&mut self, key: &K) {
        self.consumer.seek_key(key);
    }
}

#[derive(Debug)]
pub struct VecZSetValueConsumer<'a, K, R>
where
    K: 'static,
    R: 'static,
{
    values: ColumnLayerValues<'a, K, R>,
}

impl<'a, K, R> ValueConsumer<'a, (), R, ()> for VecZSetValueConsumer<'a, K, R> {
    fn value_valid(&self) -> bool {
        self.values.value_valid()
    }

    fn next_value(&mut self) -> ((), R, ()) {
        self.values.next_value()
    }

    fn remaining_values(&self) -> usize {
        self.values.remaining_values()
    }
}
*/
