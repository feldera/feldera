use crate::{
    algebra::Lattice,
    dynamic::{
        DataTrait, DynDataTyped, DynPair, DynVec, DynWeightedPairs, Erase, Factory, LeanVec,
        WeightTrait, WithFactory,
    },
    time::{Antichain, AntichainRef},
    trace::{
        cursor::{HasTimeDiffCursor, TimeDiffCursor},
        layers::{
            Builder as _, Cursor as _, Layer, LayerBuilder, LayerCursor, LayerFactories, Leaf,
            LeafBuilder, LeafCursor, LeafFactories, MergeBuilder, OrdOffset, Trie, TupleBuilder,
        },
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, Deserializer,
        Filter, Merger, Serializer, TimedBuilder,
    },
    utils::{ConsolidatePairedSlices, Tup2},
    DBData, DBWeight, NumEntries, Timestamp,
};
use rand::Rng;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::path::PathBuf;
use std::{
    fmt::{self, Debug, Display, Formatter},
    ops::DerefMut,
};

use crate::trace::ord::merge_batcher::MergeBatcher;

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
            //item_factory: self.item_factory,
            //weighted_item_factory: self.weighted_item_factory,
            //batch_item_factory: self.batch_item_factory,
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
    pub lower: Antichain<T>,
    pub upper: Antichain<T>,
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
            .field("lower", &self.lower)
            .field("upper", &self.upper)
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
            lower: self.lower.clone(),
            upper: self.upper.clone(),
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
            "lower: {:?}, upper: {:?}\nlayer:\n{}",
            self.lower,
            self.upper,
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

    type Cursor<'s> = VecValCursor<'s, K, V, T, R, O>
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

impl<K, V, T, R, O> Batch for VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    type Batcher = MergeBatcher<Self>;
    type Builder = VecValBuilder<K, V, T, R, O>;
    type Merger = VecValMerger<K, V, T, R, O>;

    fn persistent_id(&self) -> Option<PathBuf> {
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

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        VecValMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, frontier: &T) {
        // Nothing to do if the batch is entirely before the frontier.
        if !self.upper().less_equal(frontier) {
            self.do_recede_to(frontier);
        }
    }
}

impl<K, V, T, R, O> VecValBatch<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn do_recede_to(&mut self, frontier: &T) {
        // We have unique ownership of the batch, and can advance times in place.
        // We must still sort, collapse, and remove empty updates.

        // We will zip throught the time leaves, calling advance on each,
        //    then zip through the value layer, sorting and collapsing each,
        //    then zip through the key layer, collapsing each .. ?

        // 1. For each (time, diff) pair, advance the time.
        // Safety: vector is always initialized.  We do not abstract away the time type.
        for time in self
            .layer
            .vals
            .vals
            .columns_mut()
            .0
            .deref_mut()
            .as_mut_slice()
        {
            time.meet_assign(frontier);
        }

        // 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
        //    This may leave `val` with an empty range; filtering happens in step 3.
        let mut write_position = 0;
        for i in 0..self.layer.vals.keys.len() {
            // NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be
            // changed.     we will change batch.layer.vals.offs[i] in this
            // iteration, from `write_position`'s     initial value.

            let lower: usize = self.layer.vals.offs[i].into_usize();
            let upper: usize = self.layer.vals.offs[i + 1].into_usize();

            self.layer.vals.offs[i] = O::from_usize(write_position);

            let (times, diffs) = (
                &mut self.layer.vals.vals.keys,
                &mut self.layer.vals.vals.diffs,
            );

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
        self.layer.vals.vals.truncate(write_position);
        self.layer.vals.offs[self.layer.vals.keys.len()] = O::from_usize(write_position);

        // 3. For each `(key, off)` pair, (values already sorted), filter vals, and
        // rewrite `off`.    This may leave `key` with an empty range. Filtering
        // happens in step 4.
        let mut write_position = 0;
        for i in 0..self.layer.keys.len() {
            // NB: batch.layer.offs[i+1] must remain as is for the next iteration.
            //     instead, we update batch.layer.offs[i]

            let lower: usize = self.layer.offs[i].into_usize();
            let upper: usize = self.layer.offs[i + 1].into_usize();

            self.layer.offs[i] = O::from_usize(write_position);

            // values should already be sorted, but some might now be empty.
            for index in lower..upper {
                let val_lower: usize = self.layer.vals.offs[index].into_usize();
                let val_upper: usize = self.layer.vals.offs[index + 1].into_usize();
                if val_lower < val_upper {
                    self.layer.vals.keys.swap(write_position, index);
                    self.layer.vals.offs[write_position + 1] = self.layer.vals.offs[index + 1];
                    write_position += 1;
                }
            }
            // batch.layer.offs[i+1] = write_position;
        }
        self.layer.vals.keys.truncate(write_position);
        self.layer.vals.offs.truncate(write_position + 1);
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
pub struct VecValMerger<K, V, T, R, O = usize>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <VecValBatchLayer<K, V, T, R, O> as Trie>::MergeBuilder,
    lower: Antichain<T>,
    upper: Antichain<T>,
    #[size_of(skip)]
    factories: VecValBatchFactories<K, V, T, R>,
    // #[size_of(skip)]
    // item_factory: &'static PairVTable<K, V>,
    // #[size_of(skip)]
    // weighted_item_factory: &'static WeightedVTable<Pair<K, V>, R>,
    // #[size_of(skip)]
    // batch_item_factory: &'static BatchItemVTable<K, V, Pair<K, V>, R>,
}

impl<K, V, T, R, O> Merger<K, V, T, R, VecValBatch<K, V, T, R, O>> for VecValMerger<K, V, T, R, O>
where
    Self: SizeOf,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn new_merger(
        batch1: &VecValBatch<K, V, T, R, O>,
        batch2: &VecValBatch<K, V, T, R, O>,
    ) -> Self {
        // Leonid: we do not require batch bounds to grow monotonically.
        // assert!(batch1.upper() == batch2.lower());

        VecValMerger {
            lower1: batch1.layer.lower_bound(),
            upper1: batch1.layer.lower_bound() + batch1.layer.keys(),
            lower2: batch2.layer.lower_bound(),
            upper2: batch2.layer.lower_bound() + batch2.layer.keys(),
            result: <<VecValBatchLayer<K, V, T, R, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            lower: batch1.lower().meet(batch2.lower()),
            upper: batch1.upper().join(batch2.upper()),
            factories: batch1.factories.clone(),
            // item_factory: batch1.item_factory,
            // weighted_item_factory: batch1.weighted_item_factory,
            // batch_item_factory: batch1.batch_item_factory,
        }
    }

    fn done(self) -> VecValBatch<K, V, T, R, O> {
        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        VecValBatch {
            // item_factory: &self.item_factory,
            // weighted_item_factory: &self.weighted_item_factory,
            // batch_item_factory: &self.batch_item_factory,
            factories: self.factories.clone(),
            layer: self.result.done(),
            lower: self.lower,
            upper: self.upper,
        }
    }

    fn work(
        &mut self,
        source1: &VecValBatch<K, V, T, R, O>,
        source2: &VecValBatch<K, V, T, R, O>,
        key_filter: &Option<Filter<K>>,
        value_filter: &Option<Filter<V>>,
        fuel: &mut isize,
    ) {
        match (key_filter, value_filter) {
            (Some(key_filter), Some(value_filter)) => {
                self.result.push_merge_retain_values_fueled(
                    (&source1.layer, &mut self.lower1, self.upper1),
                    (&source2.layer, &mut self.lower2, self.upper2),
                    &|k| key_filter(k),
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

impl<'s, K, V, T, R, O> Clone for VecValCursor<'s, K, V, T, R, O>
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

impl<'s, K, V, T, R, O> Cursor<K, V, T, R> for VecValCursor<'s, K, V, T, R, O>
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
}

pub struct VecValTimeDiffCursor<'a, T, R>(LeafCursor<'a, DynDataTyped<T>, R>)
where
    T: Timestamp,
    R: WeightTrait + ?Sized;

impl<'a, T, R> TimeDiffCursor<'a, T, R> for VecValTimeDiffCursor<'a, T, R>
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

impl<'s, K, V, T, R, O> HasTimeDiffCursor<K, V, T, R> for VecValCursor<'s, K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: Timestamp,
    R: WeightTrait + ?Sized,
    O: OrdOffset,
{
    type TimeDiffCursor<'a> = VecValTimeDiffCursor<'a, T, R> where Self: 'a;

    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_> {
        VecValTimeDiffCursor(self.cursor.child.values())
    }
}

type RawVecValBuilder<K, V, T, R, O> =
    LayerBuilder<K, LayerBuilder<V, LeafBuilder<DynDataTyped<T>, R>, O>, O>;

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
    time: T,
    builder: RawVecValBuilder<K, V, T, R, O>,
    #[size_of(skip)]
    factories: VecValBatchFactories<K, V, T, R>,
    // #[size_of(skip)]
    // item_factory: &'static PairVTable<K, V>,
    // #[size_of(skip)]
    // weighted_item_factory: &'static WeightedVTable<Pair<K, V>, R>,
    // batch_item_factory: &'static BatchItemVTable<K, V, Pair<K, V>, R>,
}

impl<K, V, T, R, O> TimedBuilder<VecValBatch<K, V, T, R, O>> for VecValBuilder<K, V, T, R, O>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    O: OrdOffset,
{
    fn push_time(&mut self, key: &K, val: &V, time: &T, weight: &R) {
        self.builder.push_refs((key, (val, (time, weight))));
    }

    fn done_with_bounds(
        self,
        lower: Antichain<T>,
        upper: Antichain<T>,
    ) -> VecValBatch<K, V, T, R, O> {
        VecValBatch {
            layer: self.builder.done(),
            lower,
            upper,
            factories: self.factories,
        }
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
    #[inline]
    fn new_builder(factories: &VecValBatchFactories<K, V, T, R>, time: T) -> Self {
        Self {
            time,
            builder: RawVecValBuilder::<K, V, T, R, O>::new(&factories.layer_factories),
            factories: factories.clone(),
            // item_factory: factories.item_factory,
            // weighted_item_factory: factories.weighted_item_factory,
            // batch_item_factory: factories.batch_item_factory,
        }
    }

    #[inline]
    fn with_capacity(factories: &VecValBatchFactories<K, V, T, R>, time: T, cap: usize) -> Self {
        Self {
            time,
            builder: <RawVecValBuilder<K, V, T, R, O> as TupleBuilder>::with_capacity(
                &factories.layer_factories,
                cap,
            ),
            factories: factories.clone(),
            // item_factory: factories.item_factory,
            // weighted_item_factory: factories.weighted_item_factory,
            // batch_item_factory: factories.batch_item_factory,
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
        self.builder
            .push_tuple((k, (v, (&mut self.time.clone(), r))));
    }

    #[inline]
    fn push_refs(&mut self, key: &K, val: &V, weight: &R) {
        self.builder.push_refs((key, (val, (&self.time, weight))));
    }

    #[inline]
    fn push_vals(&mut self, key: &mut K, val: &mut V, weight: &mut R) {
        self.builder
            .push_tuple((key, (val, (&mut self.time.clone(), weight))));
    }

    #[inline(never)]
    fn done(self) -> VecValBatch<K, V, T, R, O> {
        let time_next = self.time.advance(0);
        let upper = if time_next <= self.time {
            Antichain::new()
        } else {
            Antichain::from_elem(time_next)
        };
        VecValBatch {
            layer: self.builder.done(),
            lower: Antichain::from_elem(self.time),
            upper,
            factories: self.factories,
            // item_factory: self.item_factory,
            // weighted_item_factory: self.weighted_item_factory,
            // batch_item_factory: self.batch_item_factory,
        }
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
