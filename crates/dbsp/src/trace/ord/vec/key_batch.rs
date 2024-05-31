use crate::{
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynUnit, DynVec, DynWeightedPairs, Erase, Factory,
        LeanVec, WeightTrait, WithFactory,
    },
    time::{Antichain, AntichainRef},
    trace::{
        cursor::{HasTimeDiffCursor, TimeDiffCursor},
        layers::{
            Builder as _, Cursor as _, Layer, LayerBuilder, LayerCursor, LayerFactories, Leaf,
            LeafBuilder, LeafCursor, LeafFactories, MergeBuilder, OrdOffset, Trie, TupleBuilder,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Deserializer,
        Filter, Merger, Serializer, TimedBuilder, WeightedItem,
    },
    utils::{ConsolidatePairedSlices, Tup2},
    DBData, DBWeight, NumEntries, Timestamp,
};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{
    fmt::{self, Debug, Display},
    ops::DerefMut,
};

use crate::trace::ord::merge_batcher::MergeBatcher;

pub struct VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    layer_factories: LayerFactories<K, LeafFactories<DynDataTyped<T>, R>>,
    consolidate_weights: &'static dyn ConsolidatePairedSlices<DynDataTyped<T>, R>,
    item_factory: &'static dyn Factory<DynPair<K, DynUnit>>,
    weighted_item_factory: &'static dyn Factory<WeightedItem<K, DynUnit, R>>,
    weighted_items_factory: &'static dyn Factory<DynWeightedPairs<DynPair<K, DynUnit>, R>>,
}

unsafe impl<K, T, R> Send for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
}

impl<K, T, R> Clone for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            layer_factories: self.layer_factories.clone(),
            consolidate_weights: self.consolidate_weights,
            item_factory: self.item_factory,
            weighted_item_factory: self.weighted_item_factory,
            weighted_items_factory: self.weighted_items_factory,
        }
    }
}

impl<K, T, R> BatchReaderFactories<K, DynUnit, T, R> for VecKeyBatchFactories<K, T, R>
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
            layer_factories: LayerFactories::new::<KType>(
                <LeafFactories<DynDataTyped<T>, R>>::new::<T, RType>(),
            ),
            consolidate_weights: <dyn ConsolidatePairedSlices<_, _>>::factory::<T, RType>(),
            item_factory: WithFactory::<Tup2<KType, ()>>::FACTORY,
            weighted_item_factory: WithFactory::<Tup2<Tup2<KType, ()>, RType>>::FACTORY,
            weighted_items_factory: WithFactory::<LeanVec<Tup2<Tup2<KType, ()>, RType>>>::FACTORY,
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
        self.layer_factories.child.diff
    }
}

impl<K, R, T> BatchFactories<K, DynUnit, T, R> for VecKeyBatchFactories<K, T, R>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    // type BatchItemFactory = BatchItemFactory<K, (), K, R>;

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
}

pub type VecKeyBatchLayer<K, T, R, O> = Layer<K, Leaf<DynDataTyped<T>, R>, O>;

/// An immutable collection of update tuples, from a contiguous interval of
/// logical times.
#[derive(SizeOf)]
pub struct VecKeyBatch<K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    /// Where all the dataz is.
    pub layer: VecKeyBatchLayer<K, T, R, O>,
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
    #[size_of(skip)]
    factories: VecKeyBatchFactories<K, T, R>,
}

impl<K, T, R, O> Debug for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecKeyBatch")
            .field("layer", &self.layer)
            .field("lower", &self.lower)
            .field("upper", &self.upper)
            .finish()
    }
}

impl<K, T, R, O: OrdOffset> Deserialize<VecKeyBatch<K, T, R, O>, Deserializer> for ()
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn deserialize(
        &self,
        _deserializer: &mut Deserializer,
    ) -> Result<VecKeyBatch<K, T, R, O>, <Deserializer as rkyv::Fallible>::Error> {
        todo!()
    }
}

impl<K, T, R, O> Archive for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        todo!()
    }
}
impl<K, T, R, O: OrdOffset> Serialize<Serializer> for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
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

impl<K, T, R, O> Clone for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            layer: self.layer.clone(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
            factories: self.factories.clone(),
        }
    }
}

impl<K, T, R, O> Display for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
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

impl<K, T, R, O> NumEntries for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    const CONST_NUM_ENTRIES: Option<usize> = <VecKeyBatchLayer<K, T, R, O>>::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, T, R, O> BatchReader for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Key = K;
    type Val = DynUnit;
    type Time = T;
    type R = R;
    type Cursor<'s> = ValKeyCursor<'s, K, T, R, O> where O: 's;
    type Factories = VecKeyBatchFactories<K, T, R>;
    // type Consumer = VecKeyConsumer<K, T, R, O>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        ValKeyCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    /*fn consumer(self) -> Self::Consumer {
        todo!()
    }*/

    fn key_count(&self) -> usize {
        <VecKeyBatchLayer<K, T, R, O> as Trie>::keys(&self.layer)
    }

    fn len(&self) -> usize {
        <VecKeyBatchLayer<K, T, R, O> as Trie>::tuples(&self.layer)
    }

    fn lower(&self) -> AntichainRef<'_, T> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<'_, T> {
        self.upper.as_ref()
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

impl<K, T, R, O> Batch for VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = VecKeyBuilder<K, T, R, O>;
    type Merger = VecKeyMerger<K, T, R, O>;

    fn persistent_id(&self) -> Option<PathBuf> {
        unimplemented!()
    }

    /*fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }*/

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        Self::Merger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            // TODO: Optimize case where self.upper()==self.lower().
            self.do_recede_to(frontier);
        }
    }
}

impl<K, T, R, O> VecKeyBatch<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn do_recede_to(&mut self, frontier: &T) {
        // We will zip through the time leaves, calling advance on each,
        //    then zip through the value layer, sorting and collapsing each,
        //    then zip through the key layer, collapsing each .. ?

        // 1. For each (time, diff) pair, advance the time.
        for time in self.layer.vals.columns_mut().0.deref_mut().as_mut_slice() {
            time.meet_assign(frontier);
        }

        // for time_diff in self.layer.vals.vals.iter_mut() {
        //     time_diff.0 = time_diff.0.advance_by(frontier);
        // }

        // 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
        //    This may leave `val` with an empty range; filtering happens in step 3.
        let mut write_position = 0;
        for i in 0..self.layer.keys.len() {
            // NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be
            // changed.     we will change batch.layer.vals.offs[i] in this
            // iteration, from `write_position`'s     initial value.

            let lower: usize = self.layer.offs[i].into_usize();
            let upper: usize = self.layer.offs[i + 1].into_usize();

            self.layer.offs[i] = O::from_usize(write_position);

            let (times, diffs) = (&mut self.layer.vals.keys, &mut self.layer.vals.diffs);

            // sort the range by the times (ignore the diffs; they will collapse).
            let count = self
                .factories
                .consolidate_weights
                .consolidate_paired_slices(
                    (times.as_mut(), lower, upper),
                    (diffs.as_mut(), lower, upper),
                );

            for index in lower..(lower + count) {
                times.swap(write_position, index);
                diffs.swap(write_position, index);
                write_position += 1;
            }
        }
        self.layer.vals.truncate(write_position);
        self.layer.offs[self.layer.keys.len()] = O::from_usize(write_position);

        // 4. Remove empty keys.
        let mut write_position = 0;
        for i in 0..self.layer.keys.len() {
            let lower: usize = self.layer.offs[i].into_usize();
            let upper: usize = self.layer.offs[i + 1].into_usize();

            if lower < upper {
                self.layer.keys.swap(write_position, i);
                // batch.layer.offs updated via `dedup` below; keeps me sane.
                write_position += 1;
            }
        }
        self.layer.offs.dedup();
        self.layer.keys.truncate(write_position);
        self.layer.offs.truncate(write_position + 1);
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct VecKeyMerger<K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
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
    result: <VecKeyBatchLayer<K, T, R, O> as Trie>::MergeBuilder,
    lower: Antichain<T>,
    upper: Antichain<T>,
    #[size_of(skip)]
    factories: VecKeyBatchFactories<K, T, R>,
}

impl<K, T, R, O> Merger<K, DynUnit, T, R, VecKeyBatch<K, T, R, O>> for VecKeyMerger<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn new_merger(batch1: &VecKeyBatch<K, T, R, O>, batch2: &VecKeyBatch<K, T, R, O>) -> Self {
        // Leonid: we do not require batch bounds to grow monotonically.
        //assert!(batch1.upper() == batch2.lower());

        VecKeyMerger {
            lower1: batch1.layer.lower_bound(),
            upper1: batch1.layer.lower_bound() + batch1.layer.keys(),
            lower2: batch2.layer.lower_bound(),
            upper2: batch2.layer.lower_bound() + batch2.layer.keys(),
            result: <<VecKeyBatchLayer<K, T, R, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            factories: batch1.factories.clone(),
        }
    }

    fn done(self) -> VecKeyBatch<K, T, R, O> {
        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        VecKeyBatch {
            layer: self.result.done(),
            lower: self.lower,
            upper: self.upper,
            factories: self.factories,
        }
    }

    fn work(
        &mut self,
        source1: &VecKeyBatch<K, T, R, O>,
        source2: &VecKeyBatch<K, T, R, O>,
        key_filter: &Option<Filter<K>>,
        _value_filter: &Option<Filter<DynUnit>>,
        fuel: &mut isize,
    ) {
        if let Some(key_filter) = key_filter {
            self.result.push_merge_retain_keys_fueled(
                (&source1.layer, &mut self.lower1, self.upper1),
                (&source2.layer, &mut self.lower2, self.upper2),
                key_filter,
                fuel,
            );
        } else {
            self.result.push_merge_fueled(
                (&source1.layer, &mut self.lower1, self.upper1),
                (&source2.layer, &mut self.lower2, self.upper2),
                fuel,
            );
        }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct ValKeyCursor<'s, K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    valid: bool,
    cursor: LayerCursor<'s, K, Leaf<DynDataTyped<T>, R>, O>,
}

impl<'s, K, T, R, O> Clone for ValKeyCursor<'s, K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    fn clone(&self) -> Self {
        Self {
            valid: self.valid,
            cursor: self.cursor.clone(),
        }
    }
}

impl<'s, K, T, R, O> Cursor<K, DynUnit, T, R> for ValKeyCursor<'s, K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    // fn key_factory(&self) -> &'static Factory<K> {
    //     self.cursor.storage.factories.key
    // }

    // fn val_factory(&self) -> &'static Factory<()> {
    //     todo!()
    // }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.child.storage.factories.diff
    }

    fn key(&self) -> &K {
        self.cursor.item()
    }

    fn val(&self) -> &DynUnit {
        &()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.cursor.child.rewind();
        while self.cursor.child.valid() {
            logic(
                self.cursor.child.current_key(),
                self.cursor.child.current_diff(),
            );
            self.cursor.child.step();
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.cursor.child.rewind();
        while self.cursor.child.valid() {
            if self.cursor.child.item().0.less_equal(upper) {
                logic(
                    self.cursor.child.current_key(),
                    self.cursor.child.current_diff(),
                );
            }
            self.cursor.child.step();
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(&self.cursor.child.valid());
        self.cursor.child.current_diff()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R))
    where
        T: PartialEq<()>,
    {
        if self.val_valid() {
            logic(self.val(), self.cursor.child.current_diff());
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

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with(predicate);
        self.valid = true;
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_with_reverse(predicate);
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
}

pub struct ValKeyTimeDiffCursor<'a, T, R>(LeafCursor<'a, DynDataTyped<T>, R>)
where
    T: Timestamp,
    R: WeightTrait + ?Sized;

impl<'a, T, R> TimeDiffCursor<'a, T, R> for ValKeyTimeDiffCursor<'a, T, R>
where
    T: Timestamp,
    R: WeightTrait + ?Sized,
{
    fn current(&mut self, _tmp: &mut R) -> Option<(&T, &R)> {
        if self.0.valid() {
            Some((self.0.current_key(), self.0.current_diff()))
        } else {
            None
        }
    }
    fn step(&mut self) {
        self.0.step()
    }
}

impl<'s, K, T, R, O> HasTimeDiffCursor<K, DynUnit, T, R> for ValKeyCursor<'s, K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type TimeDiffCursor<'a> = ValKeyTimeDiffCursor<'a, T, R> where Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        ValKeyTimeDiffCursor(self.cursor.values())
    }
}

type RawVecKeyBuilder<K, T, R, O> = LayerBuilder<K, LeafBuilder<DynDataTyped<T>, R>, O>;

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct VecKeyBuilder<K, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    time: T,
    builder: RawVecKeyBuilder<K, T, R, O>,
    #[size_of(skip)]
    factories: VecKeyBatchFactories<K, T, R>,
}

impl<K, T, R, O> TimedBuilder<VecKeyBatch<K, T, R, O>> for VecKeyBuilder<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    /// Pushes a tuple including `time` into the builder.
    ///
    /// A caller that uses this must finalize the batch with
    /// [`Self::done_with_bounds`], supplying correct upper and lower bounds, to
    /// ensure that the final batch's invariants are correct.
    #[inline]
    fn push_time(&mut self, key: &K, _val: &DynUnit, time: &T, weight: &R) {
        self.builder.push_refs((key, (time, weight)));
    }

    /// Finalizes a batch with lower bound `lower` and upper bound `upper`.
    /// This is only necessary if `push_time()` was used; otherwise, use
    /// [`Self::done`] instead.
    #[inline(never)]
    fn done_with_bounds(self, lower: Antichain<T>, upper: Antichain<T>) -> VecKeyBatch<K, T, R, O> {
        VecKeyBatch {
            layer: self.builder.done(),
            lower,
            upper,
            factories: self.factories,
        }
    }
}

impl<K, T, R, O> Builder<VecKeyBatch<K, T, R, O>> for VecKeyBuilder<K, T, R, O>
where
    K: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    #[inline]
    fn new_builder(factories: &VecKeyBatchFactories<K, T, R>, time: T) -> Self {
        Self {
            time,
            builder: <RawVecKeyBuilder<K, T, R, O> as TupleBuilder>::new(
                &factories.layer_factories,
            ),
            factories: factories.clone(),
        }
    }

    #[inline]
    fn with_capacity(factories: &VecKeyBatchFactories<K, T, R>, time: T, cap: usize) -> Self {
        Self {
            time,
            builder: <RawVecKeyBuilder<K, T, R, O> as TupleBuilder>::with_capacity(
                &factories.layer_factories,
                cap,
            ),
            factories: factories.clone(),
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, kr: &mut WeightedItem<K, DynUnit, R>) {
        let (kv, r) = kr.split_mut();
        let k = kv.fst_mut();
        self.builder.push_tuple((k, (&mut self.time.clone(), r)));
    }

    #[inline]
    fn push_refs(&mut self, key: &K, _val: &DynUnit, weight: &R) {
        self.builder.push_refs((key, (&self.time, weight)));
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, _val: &mut DynUnit, weight: &mut R) {
        self.builder
            .push_tuple((key, (&mut self.time.clone(), weight)));
    }

    #[inline(never)]
    fn done(self) -> VecKeyBatch<K, T, R, O> {
        let lower = Antichain::from_elem(self.time.clone());
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        Self::done_with_bounds(self, lower, upper)
    }
}

/*pub struct VecKeyConsumer<K, T, R, O>
where
    K: 'static,
    T: 'static,
    R: 'static,
    O: OrdOffset,
{
    consumer: OrderedLayerConsumer<K, T, R, O>,
}

impl<K, T, R, O> Consumer<K, (), R, T> for VecKeyConsumer<K, T, R, O>
where
    O: OrdOffset,
{
    type ValueConsumer<'a> = VecKeyValueConsumer<'a, K, T, R, O>
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
        (key, VecKeyValueConsumer::new(values))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key);
    }
}

pub struct VecKeyValueConsumer<'a, K, T, R, O>
where
    T: 'static,
    R: 'static,
{
    consumer: OrderedLayerValues<'a, T, R>,
    __type: PhantomData<(K, O)>,
}

impl<'a, K, T, R, O> VecKeyValueConsumer<'a, K, T, R, O> {
    const fn new(consumer: OrderedLayerValues<'a, T, R>) -> Self {
        Self {
            consumer,
            __type: PhantomData,
        }
    }
}

impl<'a, K, T, R, O> ValueConsumer<'a, (), R, T> for VecKeyValueConsumer<'a, K, T, R, O> {
    fn value_valid(&self) -> bool {
        self.consumer.value_valid()
    }

    fn next_value(&mut self) -> ((), R, T) {
        let (time, diff, ()) = self.consumer.next_value();
        ((), diff, time)
    }

    fn remaining_values(&self) -> usize {
        self.consumer.remaining_values()
    }
}
*/
