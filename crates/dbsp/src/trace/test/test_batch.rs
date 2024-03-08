//! Reference implementation of batch and trace traits for use in testing.
//!
//! So far, only methods/traits used in tests have been implemented.
#![allow(clippy::type_complexity)]

use crate::{
    dynamic::{
        pair::DynPair, DataTrait, DowncastTrait, DynVec, DynWeightedPairs, Erase, Factory,
        WeightTrait,
    },
    time::AntichainRef,
    trace::{
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Batcher, Builder, Cursor, Filter,
        Merger, Trace,
    },
    utils::VecExt,
    DBData, DBWeight, NumEntries, Timestamp,
};
use dyn_clone::clone_box;
use rand::{seq::IteratorRandom, thread_rng, Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use rkyv::{ser::Serializer, Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;
use std::{
    cmp::max,
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    fmt::{self, Debug},
    marker::PhantomData,
};

pub struct TestBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    phantom: PhantomData<fn(&K, &V, &T, &R)>,
}

impl<K, V, T, R> Default for TestBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, T, R> TestBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<K, V, T, R> Clone for TestBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<K, V, T, R> BatchReaderFactories<K, V, T, R> for TestBatchFactories<K, V, T, R>
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
        Self::new()
    }

    fn key_factory(&self) -> &'static dyn Factory<K> {
        todo!()
    }

    fn keys_factory(&self) -> &'static dyn Factory<DynVec<K>> {
        todo!()
    }

    fn val_factory(&self) -> &'static dyn Factory<V> {
        todo!()
    }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        todo!()
    }
}

impl<K, V, T, R> BatchFactories<K, V, T, R> for TestBatchFactories<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    //type BatchItemFactory = BatchItemFactory<K, V, Pair<K, V>, R>;

    fn item_factory(&self) -> &'static dyn Factory<DynPair<K, V>> {
        todo!()
    }

    fn weighted_item_factory(&self) -> &'static dyn Factory<DynPair<DynPair<K, V>, R>> {
        todo!()
    }

    fn weighted_items_factory(&self) -> &'static dyn Factory<DynWeightedPairs<DynPair<K, V>, R>> {
        todo!()
    }

    /*fn item_from(
        &self,
        _key: MutRef<'_, K>,
        _val: MutRef<'_, V>,
        _weight: MutRef<'_, R>,
        _item: MutRef<'_, Self::WeightedItem>,
    ) {
        todo!()
    }*/
}

pub fn typed_batch_to_tuples<B>(batch: &B) -> Vec<((B::Key, B::Val, B::Time), B::R)>
where
    B: crate::typed_batch::BatchReader,
{
    batch_to_tuples(batch.inner())
        .iter()
        .map(|((k, v, t), r)| {
            (
                (
                    k.downcast_checked::<B::Key>().clone(),
                    v.downcast_checked::<B::Val>().clone(),
                    t.clone(),
                ),
                r.downcast_checked::<B::R>().clone(),
            )
        })
        .collect::<Vec<_>>()
}

/// Convert any batch into a vector of tuples.
pub fn batch_to_tuples<B>(batch: &B) -> Vec<((Box<B::Key>, Box<B::Val>, B::Time), Box<B::R>)>
where
    B: BatchReader,
{
    let mut result = Vec::new();
    let mut cursor = batch.cursor();

    while cursor.key_valid() {
        while cursor.val_valid() {
            let key = clone_box(cursor.key());
            let val = clone_box(cursor.val());
            cursor.map_times(&mut |t, r| {
                result.push((
                    (clone_box(key.as_ref()), clone_box(val.as_ref()), t.clone()),
                    clone_box(r),
                ))
            });
            cursor.step_val();
        }
        cursor.step_key()
    }

    // Check that cursor iterates over keys, values, and times in order.
    assert!(result.is_sorted_by(|(k1, _), (k2, _)| Some(k1.cmp(k2))));

    <TestBatch<B::Key, B::Val, B::Time, B::R>>::from_data(&result)
        .data
        .into_iter()
        .collect::<Vec<_>>()
}

/// Convert any batch into a vector of tuples.
pub fn batch_to_tuples_reverse_vals<B>(
    batch: &B,
) -> Vec<((Box<B::Key>, Box<B::Val>, B::Time), Box<B::R>)>
where
    B: BatchReader,
{
    let mut result = Vec::new();
    let mut cursor = batch.cursor();

    while cursor.key_valid() {
        cursor.fast_forward_vals();

        while cursor.val_valid() {
            let key = clone_box(cursor.key());
            let val = clone_box(cursor.val());
            cursor.map_times(&mut |t, r| {
                result.push((
                    (clone_box(key.as_ref()), clone_box(val.as_ref()), t.clone()),
                    clone_box(r),
                ))
            });

            cursor.step_val_reverse();
        }
        cursor.step_key()
    }

    <TestBatch<B::Key, B::Val, B::Time, B::R>>::from_data(&result)
        .data
        .into_iter()
        .collect::<Vec<_>>()
}

pub fn batch_to_tuples_reverse_keys<B>(
    batch: &B,
) -> Vec<((Box<B::Key>, Box<B::Val>, B::Time), Box<B::R>)>
where
    B: BatchReader,
{
    let mut result = Vec::new();
    let mut cursor = batch.cursor();

    cursor.fast_forward_keys();

    while cursor.key_valid() {
        while cursor.val_valid() {
            let key = clone_box(cursor.key());
            let val = clone_box(cursor.val());
            cursor.map_times(&mut |t, r| {
                result.push((
                    (clone_box(key.as_ref()), clone_box(val.as_ref()), t.clone()),
                    clone_box(r),
                ))
            });

            cursor.step_val();
        }
        cursor.step_key_reverse()
    }

    <TestBatch<B::Key, B::Val, B::Time, B::R>>::from_data(&result)
        .data
        .into_iter()
        .collect::<Vec<_>>()
}

pub fn batch_to_tuples_reverse_keys_vals<B>(
    batch: &B,
) -> Vec<((Box<B::Key>, Box<B::Val>, B::Time), Box<B::R>)>
where
    B: BatchReader,
{
    let mut result = Vec::new();
    let mut cursor = batch.cursor();

    cursor.fast_forward_keys();

    while cursor.key_valid() {
        cursor.fast_forward_vals();

        while cursor.val_valid() {
            let key = clone_box(cursor.key());
            let val = clone_box(cursor.val());
            cursor.map_times(&mut |t, r| {
                result.push((
                    (clone_box(key.as_ref()), clone_box(val.as_ref()), t.clone()),
                    clone_box(r),
                ))
            });

            cursor.step_val_reverse();
        }
        cursor.step_key_reverse()
    }

    <TestBatch<B::Key, B::Val, B::Time, B::R>>::from_data(&result)
        .data
        .into_iter()
        .collect::<Vec<_>>()
}

/// Panic if `batch1` and `batch2` contain different tuples.
pub fn assert_batch_eq<B1, B2>(batch1: &B1, batch2: &B2)
where
    B1: BatchReader,
    B2: BatchReader<Time = B1::Time, Key = B1::Key, Val = B1::Val, R = B1::R>,
{
    let tuples1 = batch_to_tuples(batch1);
    assert_eq!(tuples1, batch_to_tuples_reverse_vals(batch1));
    assert_eq!(tuples1, batch_to_tuples_reverse_keys(batch1));
    assert_eq!(tuples1, batch_to_tuples_reverse_keys_vals(batch1));

    let tuples2 = batch_to_tuples(batch2);
    assert_eq!(tuples2, batch_to_tuples_reverse_vals(batch2));

    assert_eq!(tuples1, tuples2);
}

/// Panic if `batch1` and `batch2` contain different tuples.
pub fn assert_typed_batch_eq<B1, B2>(batch1: &B1, batch2: &B2)
where
    B1: crate::typed_batch::BatchReader,
    B2: crate::typed_batch::BatchReader<
        Time = B1::Time,
        Key = B1::Key,
        Val = B1::Val,
        R = B1::R,
        Inner = B1::Inner,
    >,
{
    assert_batch_eq(batch1.inner(), batch2.inner())
}

pub fn assert_trace_eq<T1, T2>(trace1: &T1, trace2: &T2)
where
    T1: Trace,
    T2: Trace<Key = T1::Key, Val = T1::Val, Time = T1::Time, R = T1::R>,
{
    let mut tuples1 = batch_to_tuples(trace1);
    assert_eq!(tuples1, batch_to_tuples_reverse_vals(trace1));
    assert_eq!(tuples1, batch_to_tuples_reverse_keys(trace1));
    assert_eq!(tuples1, batch_to_tuples_reverse_keys_vals(trace1));

    if let Some(filter) = trace1.value_filter() {
        tuples1.retain(|((_k, v, _t), _r)| filter(v.as_ref()));
    }

    if let Some(filter) = trace1.key_filter() {
        tuples1.retain(|((k, _v, _t), _r)| filter(k.as_ref()));
    }

    let mut tuples2 = batch_to_tuples(trace2);
    assert_eq!(tuples2, batch_to_tuples_reverse_vals(trace2));

    if let Some(filter) = trace2.value_filter() {
        tuples2.retain(|((_k, v, _t), _r)| filter(v.as_ref()));
    }

    if let Some(filter) = trace2.key_filter() {
        tuples2.retain(|((k, _v, _t), _r)| filter(k.as_ref()));
    }

    assert_eq!(tuples1, tuples2);
}

pub fn assert_batch_cursors_eq<C, B>(mut cursor: C, ref_batch: &B, seed: u64)
where
    B: BatchReader,
    C: Cursor<B::Key, B::Val, B::Time, B::R>,
{
    // Extract all key/value pairs.
    let mut tuples = batch_to_tuples(ref_batch)
        .into_iter()
        .map(|((k, v, _t), _r)| (k, v))
        .collect::<Vec<_>>();

    tuples.dedup();

    // Randomly sample 1/3 of the pairs.
    let sample_len = tuples.len() / 3;
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let mut sample = tuples.into_iter().choose_multiple(&mut rng, sample_len);
    sample.sort();
    sample.dedup();

    let mut sample_map = BTreeMap::<Box<B::Key>, Vec<Box<B::Val>>>::new();

    for (k, v) in sample.into_iter() {
        sample_map.entry(k).or_default().push(v);
    }

    for key in sample_map.keys() {
        cursor.seek_key(key.as_ref());
        assert!(cursor.key_valid());
        assert_eq!(cursor.key(), key.as_ref());

        for val in sample_map.get(key).unwrap().iter() {
            cursor.seek_val(val.as_ref());
            assert!(cursor.val_valid());
            assert_eq!(cursor.val(), val.as_ref());
        }

        cursor.fast_forward_vals();
        assert!(cursor.val_valid());

        for val in sample_map.get(key).unwrap().iter().rev() {
            cursor.seek_val_reverse(val.as_ref());
            assert!(cursor.val_valid());
            assert_eq!(cursor.val(), val.as_ref());
        }
    }

    cursor.fast_forward_keys();

    for key in sample_map.keys().rev() {
        cursor.seek_key_reverse(key.as_ref());
        assert!(cursor.key_valid());
        assert_eq!(cursor.key(), key.as_ref());

        for val in sample_map.get(key).unwrap().iter() {
            cursor.seek_val(val.as_ref());
            assert!(cursor.val_valid());
            assert_eq!(cursor.val(), val.as_ref());
        }

        cursor.fast_forward_vals();
        assert!(cursor.val_valid());

        for val in sample_map.get(key).unwrap().iter().rev() {
            cursor.seek_val_reverse(val.as_ref());
            assert!(cursor.val_valid());
            assert_eq!(cursor.val(), val.as_ref());
        }
    }
}

/// Inefficient but simple batch implementation as a B-tree map.
#[derive(SizeOf)]
pub struct TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
    data: BTreeMap<(Box<K>, Box<V>, T), Box<R>>,
    lower_key_bound: Option<Box<K>>,
    #[size_of(skip)]
    key_filter: Option<Filter<K>>,
    #[size_of(skip)]
    value_filter: Option<Filter<V>>,
}

unsafe impl<K, V, T, R> Send for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
}

impl<K, V, T, R> Debug for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key_filter = if self.key_filter.is_none() {
            "not set".to_string()
        } else {
            "set".to_string()
        };

        let value_filter = if self.value_filter.is_none() {
            "not set".to_string()
        } else {
            "set".to_string()
        };

        f.debug_struct("TestBatch")
            .field("data", &self.data)
            .field("lower_key_bound", &self.lower_key_bound)
            .field("key_filter", &key_filter)
            .field("value_filter", &value_filter)
            .finish()
    }
}

impl<K, V, T, R> Clone for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            data: self
                .data
                .iter()
                .map(|((k, v, t), r)| {
                    (
                        (clone_box(k.as_ref()), clone_box(v.as_ref()), t.clone()),
                        clone_box(r.as_ref()),
                    )
                })
                .collect(),
            lower_key_bound: self.lower_key_bound.as_ref().map(|b| clone_box(b.as_ref())),
            key_filter: self.key_filter.as_ref().cloned(),
            value_filter: self.value_filter.as_ref().cloned(),
        }
    }
}

impl<K, V, T, R> Archive for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<K, V, T, R, S: Serializer + ?Sized> Serialize<S> for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<K, V, T, R, D: Fallible> Deserialize<TestBatch<K, V, T, R>, D>
    for Archived<TestBatch<K, V, T, R>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<TestBatch<K, V, T, R>, D::Error> {
        unimplemented!();
    }
}

impl<K, V, T, R> NumEntries for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.data.len()
    }

    fn num_entries_deep(&self) -> usize {
        self.data.len()
    }
}

impl<K, V, T, R> TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    /// Create batch from sorted or unsorted tuples.
    pub fn from_typed_data<KType, VType, RType>(records: &[((KType, VType, T), RType)]) -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RType: DBData + Erase<R>,
    {
        let mut data: BTreeMap<(Box<K>, Box<V>, T), Box<R>> = BTreeMap::new();

        for ((k, v, t), r) in records.iter() {
            match data.entry((
                Box::new(k.clone()).erase_box(),
                Box::new(v.clone()).erase_box(),
                t.clone(),
            )) {
                Entry::Occupied(mut oe) => oe.get_mut().as_mut().add_assign(r.erase()),
                Entry::Vacant(ve) => {
                    let _ = ve.insert(Box::new(r.clone()).erase_box());
                }
            }
        }

        data.retain(|_, r| !r.is_zero());

        Self {
            data,
            lower_key_bound: None,
            key_filter: None,
            value_filter: None,
        }
    }

    /// Create batch from sorted or unsorted tuples.
    pub fn from_data(records: &[((Box<K>, Box<V>, T), Box<R>)]) -> Self {
        let mut data: BTreeMap<(Box<K>, Box<V>, T), Box<R>> = BTreeMap::new();

        for ((k, v, t), r) in records.iter() {
            match data.entry((clone_box(k.as_ref()), clone_box(v.as_ref()), t.clone())) {
                Entry::Occupied(mut oe) => oe.get_mut().as_mut().add_assign(r),
                Entry::Vacant(ve) => {
                    let _ = ve.insert(clone_box(r.as_ref()));
                }
            }
        }

        data.retain(|_, r| !r.is_zero());

        Self {
            data,
            lower_key_bound: None,
            key_filter: None,
            value_filter: None,
        }
    }
}

// pub struct TestBatchConsumer<K, V, T, R> {
//     _phantom: PhantomData<(K, V, T, R)>,
// }
// pub struct TestBatchValueConsumer<V, T, R> {
//     _phantom: PhantomData<(V, T, R)>,
// }

/*impl<'a, V, T, R> ValueConsumer<'a, V, R, T> for TestBatchValueConsumer<V, T, R>
where
    V: DBData,
    T: Timestamp,
    R: DBWeight,
{
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

impl<K, V, T, R> Consumer<K, V, R, T> for TestBatchConsumer<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: Timestamp,
    R: DBWeight,
{
    type ValueConsumer<'a> = TestBatchValueConsumer<V, T, R>;

    fn key_valid(&self) -> bool {
        todo!()
    }
    fn peek_key(&self) -> &K {
        todo!()
    }
    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }
    /// Advances the cursor to the specified value
    fn seek_key(&mut self, _key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}*/

#[derive(SizeOf, Archive, Serialize, Deserialize)]
pub struct TestBatchBatcher<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Ord,
{
    time: T,
    result: TestBatch<K, V, T, R>,
}

impl<K, V, T, R> Batcher<TestBatch<K, V, T, R>> for TestBatchBatcher<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn new_batcher(factories: &TestBatchFactories<K, V, T, R>, time: T) -> Self {
        Self {
            time,
            result: TestBatch::new(factories, ""),
        }
    }

    fn push_batch(&mut self, batch: &mut Box<DynWeightedPairs<DynPair<K, V>, R>>) {
        for pair in batch.dyn_iter() {
            let (kv, r) = pair.split();
            let (k, v) = kv.split();
            match self
                .result
                .data
                .entry((clone_box(k), clone_box(v), self.time.clone()))
            {
                Entry::Occupied(mut oe) => oe.get_mut().as_mut().add_assign(r),
                Entry::Vacant(ve) => {
                    let _ = ve.insert(clone_box(r));
                }
            }
        }
    }

    fn push_consolidated_batch(&mut self, batch: &mut Box<DynWeightedPairs<DynPair<K, V>, R>>) {
        self.push_batch(batch);
    }

    fn tuples(&self) -> usize {
        self.result.len()
    }

    fn seal(mut self) -> TestBatch<K, V, T, R> {
        self.result.data.retain(|_, r| !r.is_zero());
        self.result
    }
}

#[derive(SizeOf)]
pub struct TestBatchBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    time: T,
    result: TestBatch<K, V, T, R>,
}

impl<K, V, T, R> Builder<TestBatch<K, V, T, R>> for TestBatchBuilder<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn new_builder(factories: &TestBatchFactories<K, V, T, R>, time: T) -> Self {
        Self {
            time,
            result: TestBatch::new(factories, ""),
        }
    }

    fn with_capacity(factories: &TestBatchFactories<K, V, T, R>, time: T, _cap: usize) -> Self {
        Self::new_builder(factories, time)
    }

    fn push(&mut self, element: &mut DynPair<DynPair<K, V>, R>) {
        let pair = &*element;
        let (kv, r) = pair.split();
        let (k, v) = kv.split();

        match self
            .result
            .data
            .entry((clone_box(k), clone_box(v), self.time.clone()))
        {
            Entry::Occupied(mut oe) => oe.get_mut().as_mut().add_assign(r),
            Entry::Vacant(ve) => {
                let _ = ve.insert(clone_box(r));
            }
        }
    }

    fn push_refs(&mut self, k: &K, v: &V, r: &R) {
        match self
            .result
            .data
            .entry((clone_box(k), clone_box(v), self.time.clone()))
        {
            Entry::Occupied(mut oe) => oe.get_mut().as_mut().add_assign(r),
            Entry::Vacant(ve) => {
                let _ = ve.insert(clone_box(r));
            }
        }
    }

    fn push_vals(&mut self, k: &mut K, v: &mut V, r: &mut R) {
        self.push_refs(k, v, r)
    }

    fn reserve(&mut self, _additional: usize) {}

    fn done(mut self) -> TestBatch<K, V, T, R> {
        self.result.data.retain(|_, r| !r.is_zero());
        self.result
    }
}

#[derive(SizeOf)]
pub struct TestBatchMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    result: TestBatch<K, V, T, R>,
}

impl<K, V, T, R> Merger<K, V, T, R, TestBatch<K, V, T, R>> for TestBatchMerger<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn new_merger(source1: &TestBatch<K, V, T, R>, source2: &TestBatch<K, V, T, R>) -> Self {
        let data = source1
            .data
            .iter()
            .chain(source2.data.iter())
            .map(|((k, v, t), r)| {
                (
                    (clone_box(k.as_ref()), clone_box(v.as_ref()), t.clone()),
                    clone_box(r.as_ref()),
                )
            })
            .collect::<Vec<_>>();
        let result = TestBatch::from_data(&data);

        Self { result }
    }

    fn work(
        &mut self,
        _source1: &TestBatch<K, V, T, R>,
        _source2: &TestBatch<K, V, T, R>,
        _key_filter: &Option<Filter<K>>,
        _value_filter: &Option<Filter<V>>,
        _fuel: &mut isize,
    ) {
    }

    fn done(self) -> TestBatch<K, V, T, R> {
        self.result
    }
}

pub struct TestBatchCursor<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    // All tuples in the batch flattened into a vector.
    data: Vec<((Box<K>, Box<V>, T), Box<R>)>,
    // Index that the cursor currently points to.
    index: usize,
    // Set to true after calling `step_val` on the last
    // value for a key.
    val_valid: bool,
}

impl<K, V, T, R> Clone for TestBatchCursor<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            data: self
                .data
                .iter()
                .map(|((k, v, t), r)| {
                    (
                        (clone_box(k.as_ref()), clone_box(v.as_ref()), t.clone()),
                        clone_box(r.as_ref()),
                    )
                })
                .collect(),
            index: self.index,
            val_valid: self.val_valid,
        }
    }
}

impl<K, V, T, R> TestBatchCursor<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    fn new(batch: &TestBatch<K, V, T, R>) -> Self {
        Self {
            data: batch
                .data
                .iter()
                .map(|((k, v, t), r)| {
                    (
                        (clone_box(k.as_ref()), clone_box(v.as_ref()), t.clone()),
                        clone_box(r.as_ref()),
                    )
                })
                .collect::<Vec<_>>(),
            index: 0,
            val_valid: !batch.data.is_empty(),
        }
    }
}

impl<K, V, T, R> Cursor<K, V, T, R> for TestBatchCursor<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    // fn key_factory(&self) -> &'static Factory<K> {
    //     &K::VTABLE
    // }

    // fn val_factory(&self) -> &'static Factory<V> {
    //     &V::VTABLE
    // }

    fn weight_factory(&self) -> &'static dyn Factory<R> {
        todo!()
    }

    fn key_valid(&self) -> bool {
        self.index < self.data.len()
    }

    fn val_valid(&self) -> bool {
        self.key_valid() && self.val_valid
    }

    fn key(&self) -> &K {
        self.data[self.index].0 .0.as_ref()
    }

    fn val(&self) -> &V {
        self.data[self.index].0 .1.as_ref()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        let current_key = clone_box(self.data[self.index].0 .0.as_ref());
        let current_val = clone_box(self.data[self.index].0 .1.as_ref());

        let mut index = self.index;

        while index < self.data.len()
            && self.data[index].0 .0 == current_key
            && self.data[index].0 .1 == current_val
        {
            logic(&self.data[index].0 .2, self.data[index].1.as_ref());
            index += 1;
        }
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            let weight = clone_box(self.weight());
            let val = self.val();
            logic(val, weight.as_ref());
            self.step_val();
        }
    }

    fn map_times_through(&mut self, _upper: &T, _logic: &mut dyn FnMut(&T, &R)) {
        todo!()
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        self.data[self.index].1.as_ref()
    }

    fn step_key(&mut self) {
        let current_key = clone_box(self.data[self.index].0 .0.as_ref());

        while self.index < self.data.len() && &self.data[self.index].0 .0 == &current_key {
            self.index += 1;
        }

        self.val_valid = true;
    }

    fn step_key_reverse(&mut self) {
        todo!()
    }

    fn seek_key(&mut self, key: &K) {
        while self.index < self.data.len() && self.data[self.index].0 .0.as_ref() < key {
            self.index += 1;
        }
    }

    fn seek_key_with(&mut self, _predicate: &dyn Fn(&K) -> bool) {
        todo!()
    }

    fn seek_key_with_reverse(&mut self, _predicate: &dyn Fn(&K) -> bool) {
        todo!()
    }

    fn seek_key_reverse(&mut self, _key: &K) {
        todo!()
    }

    fn step_val(&mut self) {
        let current_key = clone_box(self.data[self.index].0 .0.as_ref());
        let current_val = clone_box(self.data[self.index].0 .1.as_ref());

        while self.index < self.data.len() && &self.data[self.index].0 .1 == &current_val {
            if self.index + 1 < self.data.len() && &self.data[self.index + 1].0 .0 == &current_key {
                self.index += 1;
            } else {
                self.val_valid = false;
                return;
            }
        }
    }

    fn seek_val(&mut self, val: &V) {
        loop {
            if !self.val_valid() || self.val() >= val {
                break;
            }
            self.step_val();
        }
    }

    fn seek_val_with(&mut self, _predicate: &dyn Fn(&V) -> bool) {
        todo!()
    }

    fn rewind_keys(&mut self) {
        self.index = 0;
    }

    fn fast_forward_keys(&mut self) {
        todo!()
    }

    fn rewind_vals(&mut self) {
        todo!()
    }

    fn step_val_reverse(&mut self) {
        let current_key = clone_box(self.data[self.index].0 .0.as_ref());
        let current_val = clone_box(self.data[self.index].0 .1.as_ref());

        while &self.data[self.index].0 .1 == &current_val {
            if self.index > 0 && self.data[self.index - 1].0 .0 == current_key {
                self.index -= 1;
            } else {
                self.val_valid = false;
                return;
            }
        }

        let current_val = clone_box(self.data[self.index].0 .1.as_ref());

        while self.index > 0
            && self.data[self.index - 1].0 .0 == current_key
            && self.data[self.index - 1].0 .1 == current_val
        {
            self.index -= 1;
        }
    }

    fn seek_val_reverse(&mut self, val: &V) {
        loop {
            if !self.val_valid() || self.val() >= val {
                break;
            }
            self.step_val_reverse();
        }
    }

    fn seek_val_with_reverse(&mut self, _predicate: &dyn Fn(&V) -> bool) {
        todo!()
    }

    fn fast_forward_vals(&mut self) {
        let current_key = clone_box(self.data[self.index].0 .0.as_ref());

        while self.index + 1 < self.data.len() && self.data[self.index + 1].0 .0 == current_key {
            self.index += 1;
        }

        let current_val = clone_box(self.data[self.index].0 .1.as_ref());

        while self.index > 0
            && self.data[self.index - 1].0 .0 == current_key
            && self.data[self.index - 1].0 .1 == current_val
        {
            self.index -= 1;
        }

        self.val_valid = true;
    }
}

impl<K, V, T, R> BatchReader for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;
    type Factories = TestBatchFactories<K, V, T, R>;

    type Cursor<'s> = TestBatchCursor<K, V, T, R>;

    // type Consumer = TestBatchConsumer<K, V, T, R>;

    fn factories(&self) -> Self::Factories {
        TestBatchFactories {
            phantom: PhantomData,
        }
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        TestBatchCursor::new(self)
    }

    /*fn consumer(self) -> Self::Consumer {
        todo!()
    }*/

    fn key_count(&self) -> usize {
        self.data
            .keys()
            .map(|(k, _, _)| clone_box(k.as_ref()))
            .collect::<BTreeSet<_>>()
            .len()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn lower(&self) -> AntichainRef<'_, Self::Time> {
        todo!()
    }

    fn upper(&self) -> AntichainRef<'_, Self::Time> {
        todo!()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        let bound = if let Some(bound) = &self.lower_key_bound {
            clone_box(max(bound.as_ref(), lower_bound))
        } else {
            clone_box(lower_bound)
        };

        self.lower_key_bound = Some(clone_box(bound.as_ref()));
        self.data.retain(|(k, _v, _t), _r| k >= &bound);
    }

    fn sample_keys<RG>(&self, _rng: &mut RG, _sample_size: usize, _sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        unimplemented!()
    }
}

impl<K, V, T, R> Batch for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    type Batcher = TestBatchBatcher<K, V, T, R>;
    type Builder = TestBatchBuilder<K, V, T, R>;
    type Merger = TestBatchMerger<K, V, T, R>;

    /*fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self
    where
        Self::Val: From<()>,
    {
        let tuples = keys
            .into_iter()
            .map(|(k, r)| ((k, <Self::Val>::from(())), r))
            .collect::<Vec<_>>();
        Self::from_tuples(time, tuples)
    }*/

    fn recede_to(&mut self, frontier: &Self::Time) {
        let data = self
            .data
            .iter()
            .map(|((k, v, t), r)| {
                (
                    (
                        clone_box(k.as_ref()),
                        clone_box(v.as_ref()),
                        t.meet(frontier),
                    ),
                    clone_box(r.as_ref()),
                )
            })
            .collect::<Vec<_>>();

        self.data = Self::from_data(&data).data;
    }
}

impl<K, V, T, R> Trace for TestBatch<K, V, T, R>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
{
    type Batch = Self;

    fn new<S: AsRef<str>>(_factories: &Self::Factories, _persistent_id: S) -> Self {
        Self {
            data: BTreeMap::new(),
            lower_key_bound: None,
            key_filter: None,
            value_filter: None,
        }
    }

    fn recede_to(&mut self, frontier: &Self::Time) {
        Batch::recede_to(self, frontier);
    }

    fn exert(&mut self, _effort: &mut isize) {}

    fn consolidate(self) -> Option<Self::Batch> {
        if self.data.is_empty() {
            None
        } else {
            Some(self)
        }
    }

    fn insert(&mut self, mut batch: Self::Batch) {
        if let Some(bound) = &self.lower_key_bound {
            batch.truncate_keys_below(bound.as_ref());
        }
        if let Some(filter) = &self.value_filter {
            batch.retain_values(filter.clone());
        }
        if let Some(filter) = &self.key_filter {
            batch.retain_keys(filter.clone());
        }

        self.data = self.merge(&batch).data;
    }

    fn clear_dirty_flag(&mut self) {}

    fn dirty(&self) -> bool {
        todo!()
    }

    fn retain_keys(&mut self, filter: Filter<Self::Key>) {
        self.data.retain(|(k, _v, _t), _r| filter(k.as_ref()));
        self.key_filter = Some(filter);
    }

    fn retain_values(&mut self, filter: Filter<Self::Val>) {
        self.data.retain(|(_k, v, _t), _r| filter(v.as_ref()));
        self.value_filter = Some(filter);
    }

    fn key_filter(&self) -> &Option<Filter<Self::Key>> {
        &self.key_filter
    }

    fn value_filter(&self) -> &Option<Filter<Self::Val>> {
        &self.value_filter
    }
}

/// Test random sampling methods.
///
/// Assumes that `B` is a simple batch, not a trace (i.e., no duplicate keys).
pub fn test_batch_sampling<B>(batch: &B)
where
    B: BatchReader<Time = ()>,
    B::Key: DataTrait,
    B::Val: DataTrait,
    B::R: DataTrait,
{
    let mut sample = batch.factories().keys_factory().default_box();

    let mut all_keys = batch.factories().keys_factory().default_box();
    let mut cursor = batch.cursor();
    while cursor.key_valid() {
        all_keys.push_ref(cursor.key());
        cursor.step_key();
    }
    let all_keys_set: BTreeSet<_> = all_keys.dyn_iter().map(clone_box).collect();

    // Sample size 0 - return empty sample.
    batch.sample_keys(&mut thread_rng(), 0, sample.as_mut());
    assert!(sample.is_empty());
    sample.clear();

    // Sample size == batch size - must return all keys in the batch.
    batch.sample_keys(&mut thread_rng(), batch.key_count(), sample.as_mut());
    assert_eq!(&sample, &all_keys);
    sample.clear();

    // Sample size > batch size - must return all keys in the batch.
    batch.sample_keys(&mut thread_rng(), batch.key_count() << 1, sample.as_mut());
    assert_eq!(&sample, &all_keys);
    sample.clear();

    // Sample size < batch size - return the exact number of keys requested,
    // no duplicates, all returned keys must belong to the batch.
    let sample_size = batch.key_count() >> 1;
    batch.sample_keys(&mut thread_rng(), sample_size, sample.as_mut());
    assert_eq!(sample.len(), sample_size);
    assert!(sample.is_sorted_by(&|k1, k2| k1.cmp(k2)));
    let sample_set = sample.dyn_iter().map(clone_box).collect::<BTreeSet<_>>();
    assert_eq!(sample_set.len(), sample.len());
    for key in sample.dyn_iter() {
        assert!(all_keys_set.contains(key));
    }
    sample.clear();
}

/// Test random sampling methods.
///
/// Similar to `test_batch_sampling`, but allows the sample
/// to contain fewer keys than requested (as keys in a trace
/// can get canceled out).
pub fn test_trace_sampling<T: Trace<Time = ()>>(trace: &T) {
    let mut sample = trace.factories().keys_factory().default_box();

    let batch = TestBatch::<T::Key, T::Val, T::Time, T::R>::from_data(&batch_to_tuples(trace));

    let mut all_keys = trace.factories().keys_factory().default_box();

    let mut cursor = batch.cursor();
    while cursor.key_valid() {
        all_keys.push_ref(cursor.key());
        cursor.step_key();
    }
    let all_keys_set = all_keys.dyn_iter().map(clone_box).collect::<BTreeSet<_>>();

    // Sample size 0 - return empty sample.
    trace.sample_keys(&mut thread_rng(), 0, sample.as_mut());
    assert!(sample.is_empty());
    sample.clear();

    // Sample size == size - must return all keys in the batch.
    trace.sample_keys(&mut thread_rng(), trace.key_count(), sample.as_mut());
    assert_eq!(&sample, &all_keys);
    sample.clear();

    // Sample size > trace size - must return all keys in the trace.
    trace.sample_keys(&mut thread_rng(), trace.key_count() << 1, sample.as_mut());
    assert_eq!(&sample, &all_keys);
    sample.clear();

    // Sample size < trace size - return at most the number of keys requested,
    // no duplicates, all returned keys must belong to the trace.
    let sample_size = trace.key_count() >> 1;
    trace.sample_keys(&mut thread_rng(), sample_size, sample.as_mut());
    assert!(sample.len() <= sample_size);
    assert!(sample.is_sorted_by(&|k1, k2| k1.cmp(k2)));
    let sample_set = sample.dyn_iter().map(clone_box).collect::<BTreeSet<_>>();
    assert_eq!(sample_set.len(), sample.len());

    for key in sample.dyn_iter() {
        assert!(all_keys_set.contains(key));
    }
    sample.clear();
}
