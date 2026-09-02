//! A snapshot of a spine, which can be used to read from the spine without
//! holding a reference to the spine itself.

use std::fmt::Debug;
use std::sync::Arc;

use futures::{StreamExt, stream::FuturesUnordered};
use rand::Rng;
use rkyv::ser::Serializer;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::SpineCursor;
use crate::NumEntries;
use crate::dynamic::{DynVec, Factory};
use crate::storage::file::FilterStats;
use crate::trace::cursor::{CursorFactory, CursorList};
use crate::trace::{
    Batch, BatchReader, BatchReaderFactories, Cursor, Spine, merge_batches,
    sample_keys_from_batches,
};

pub trait WithSnapshot: Sized {
    type Batch: Batch;

    fn into_ro_snapshot(self) -> SpineSnapshot<Self::Batch> {
        self.ro_snapshot()
    }

    /// Returns a read-only, non-merging snapshot of the current trace
    /// state.
    fn ro_snapshot(&self) -> SpineSnapshot<Self::Batch>;
}

pub trait BatchReaderWithSnapshot:
    BatchReader<
        Key = <Self::Batch as BatchReader>::Key,
        Val = <Self::Batch as BatchReader>::Val,
        Time = <Self::Batch as BatchReader>::Time,
        R = <Self::Batch as BatchReader>::R,
    > + WithSnapshot
{
}

impl<B> BatchReaderWithSnapshot for B where
    B: BatchReader<
            Key = <Self::Batch as BatchReader>::Key,
            Val = <Self::Batch as BatchReader>::Val,
            Time = <Self::Batch as BatchReader>::Time,
            R = <Self::Batch as BatchReader>::R,
        > + WithSnapshot
{
}

#[derive(Clone, SizeOf)]
pub struct SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    batches: Vec<Arc<B>>,
    #[size_of(skip)]
    factories: B::Factories,
}

impl<B> WithSnapshot for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    type Batch = B;

    fn into_ro_snapshot(self) -> SpineSnapshot<Self::Batch> {
        self
    }

    fn ro_snapshot(&self) -> SpineSnapshot<B> {
        self.clone()
    }
}

impl<B> WithSnapshot for B
where
    B: Batch,
{
    type Batch = B;
    fn into_ro_snapshot(self) -> SpineSnapshot<B> {
        let factories = self.factories();

        SpineSnapshot {
            batches: vec![Arc::new(self)],
            factories,
        }
    }

    fn ro_snapshot(&self) -> SpineSnapshot<Self::Batch> {
        SpineSnapshot {
            batches: vec![Arc::new(self.clone())],
            factories: self.factories(),
        }
    }
}

impl<B: Batch + Send + Sync> Debug for SpineSnapshot<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpineSnapshot")
            .field("batches", &self.batches)
            .finish()
    }
}

impl<B> SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    pub fn new(factories: B::Factories) -> Self {
        Self {
            batches: Vec::new(),
            factories,
        }
    }

    pub fn with_batches(factories: &B::Factories, batches: Vec<Arc<B>>) -> Self {
        Self {
            batches,
            factories: factories.clone(),
        }
    }

    pub fn extend(&mut self, other: Self) {
        self.batches.extend(other.batches.iter().cloned())
    }

    pub fn extend_with_batches<I>(&mut self, batches: I)
    where
        I: IntoIterator<Item = Arc<B>>,
    {
        self.batches.extend(batches);
    }

    pub fn concat<'a, I>(factories: B::Factories, snapshots: I) -> Self
    where
        I: IntoIterator<Item = &'a Self>,
    {
        Self {
            batches: snapshots
                .into_iter()
                .flat_map(|snapshot| snapshot.batches.iter().cloned())
                .collect::<Vec<_>>(),
            factories,
        }
    }

    pub fn batches(&self) -> &[Arc<B>] {
        &self.batches
    }

    pub fn consolidate(&self) -> B {
        merge_batches(
            &self.factories,
            self.batches().iter().map(|b| b.as_ref().clone()),
            &None,
            &None,
        )
    }

    pub fn into_batches(self) -> Vec<Arc<B>> {
        self.batches
    }
}

impl<B> From<&Spine<B>> for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    fn from(spine: &Spine<B>) -> Self {
        Self {
            batches: spine.merger.get_batches(),
            factories: spine.factories.clone(),
        }
    }
}

impl<B> NumEntries for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.batches.iter().fold(0, |acc, batch| acc + batch.len())
    }

    fn num_entries_deep(&self) -> usize {
        self.num_entries_shallow()
    }
}

impl<B> BatchReader for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    type Factories = B::Factories;
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor<'s> = SpineCursor<B>;

    fn factories(&self) -> Self::Factories {
        self.factories.clone()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        SpineCursor::new_cursor(&self.factories, self.batches.clone())
    }

    fn key_count(&self) -> usize {
        self.batches
            .iter()
            .fold(0, |acc, batch| acc + batch.key_count())
    }

    fn len(&self) -> usize {
        self.batches.iter().fold(0, |acc, batch| acc + batch.len())
    }

    fn approximate_byte_size(&self) -> usize {
        self.batches
            .iter()
            .fold(0, |acc, batch| acc + batch.approximate_byte_size())
    }

    fn membership_filter_stats(&self) -> FilterStats {
        self.batches
            .iter()
            .map(|b| b.membership_filter_stats())
            .sum()
    }

    fn range_filter_stats(&self) -> FilterStats {
        self.batches.iter().map(|b| b.range_filter_stats()).sum()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut DynVec<Self::Key>)
    where
        RG: Rng,
    {
        let batch_refs: Vec<_> = self.batches.iter().map(Arc::as_ref).collect();
        sample_keys_from_batches(&self.factories, &batch_refs, rng, sample_size, sample);
    }

    async fn fetch<K>(
        &self,
        keys: &K,
    ) -> Option<Box<dyn CursorFactory<Self::Key, Self::Val, Self::Time, Self::R>>>
    where
        K: BatchReader<Key = Self::Key, Time = ()>,
    {
        Some(Box::new(
            FetchList::new(self.batches.clone(), keys, self.factories.weight_factory()).await,
        ))
    }
}

pub struct FetchList<B>
where
    B: BatchReader,
{
    weight_factory: &'static dyn Factory<B::R>,
    batches: Vec<Arc<B>>,
    fetched: Vec<Box<dyn CursorFactory<B::Key, B::Val, B::Time, B::R>>>,
}

impl<B> FetchList<B>
where
    B: BatchReader,
{
    pub async fn new<K>(
        inputs: Vec<Arc<B>>,
        keys: &K,
        weight_factory: &'static dyn Factory<B::R>,
    ) -> Self
    where
        K: BatchReader<Key = B::Key, Time = ()>,
    {
        let mut batches = Vec::new();
        let mut fetched = Vec::new();
        let mut futures = inputs
            .into_iter()
            .map(|b| async move { (b.clone(), b.fetch(keys).await) })
            .collect::<FuturesUnordered<_>>();
        while let Some((batch, fetch)) = futures.next().await {
            if let Some(fetch) = fetch {
                fetched.push(fetch);
            } else {
                batches.push(batch);
            }
        }

        Self {
            weight_factory,
            batches,
            fetched,
        }
    }
}

impl<B> CursorFactory<B::Key, B::Val, B::Time, B::R> for FetchList<B>
where
    B: Batch,
{
    fn get_cursor<'a>(&'a self) -> Box<dyn Cursor<B::Key, B::Val, B::Time, B::R> + 'a> {
        let cursors =
            self.fetched
                .iter()
                .map(|hc| hc.get_cursor())
                .chain(self.batches.iter().map(|b| {
                    Box::new(b.cursor()) as Box<dyn Cursor<B::Key, B::Val, B::Time, B::R>>
                }))
                .collect::<Vec<_>>();
        Box::new(CursorList::new(self.weight_factory, cursors))
    }
}

impl<B> Archive for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<B, S: Serializer + ?Sized> Serialize<S> for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<B, D: Fallible> Deserialize<SpineSnapshot<B>, D> for Archived<SpineSnapshot<B>>
where
    B: Batch + Send + Sync,
{
    fn deserialize(&self, _deserializer: &mut D) -> Result<SpineSnapshot<B>, D::Error> {
        unimplemented!();
    }
}

#[cfg(test)]
mod partition_keys_test {
    use crate::{
        ZWeight,
        dynamic::{DowncastTrait, DynData},
        trace::{BatchReader, BatchReaderFactories, Cursor, SpineSnapshot, partition_sample_size},
        typed_batch::{DynOrdIndexedZSet, OrdIndexedZSet},
        utils::Tup2,
    };
    use std::sync::Arc;

    type Batch = DynOrdIndexedZSet<DynData, DynData>;

    /// A snapshot of `batches` batches of `keys_per_batch` keys each, laid out
    /// by `key`, which maps a batch index and an offset within it to a key.
    fn snapshot(
        batches: u64,
        keys_per_batch: u64,
        key: impl Fn(u64, u64) -> u64,
    ) -> SpineSnapshot<Batch> {
        let factories: <Batch as BatchReader>::Factories =
            BatchReaderFactories::new::<u64, u64, ZWeight>();
        let batches = (0..batches)
            .map(|batch| {
                let tuples = (0..keys_per_batch)
                    .map(|i| {
                        let k = key(batch, i);
                        Tup2(Tup2(k, k), 1)
                    })
                    .collect();
                Arc::new(OrdIndexedZSet::<u64, u64>::from_tuples((), tuples).into_inner())
            })
            .collect();

        SpineSnapshot::with_batches(&factories, batches)
    }

    /// Keys of every batch spread over the whole key range, as they are when a
    /// spine groups its batches by arrival time and the keys do not correlate
    /// with arrival.
    fn interleaved(batches: u64) -> impl Fn(u64, u64) -> u64 {
        move |batch, i| i * batches + batch
    }

    /// Partitions `snapshot` and returns the size of each range.
    fn partition(snapshot: &SpineSnapshot<Batch>, partitions: usize) -> Vec<u64> {
        let mut bounds = snapshot.factories().keys_factory().default_box();
        snapshot.partition_keys(partitions, bounds.as_mut());
        assert_eq!(bounds.len(), partitions - 1);

        let mut sizes = vec![0u64; partitions];
        let mut cursor = snapshot.cursor();
        while cursor.key_valid() {
            let key = *cursor.key().downcast_checked::<u64>();
            let range = (0..partitions - 1)
                .find(|&i| key < *bounds.index(i).downcast_checked::<u64>())
                .unwrap_or(partitions - 1);
            sizes[range] += 1;
            cursor.step_key();
        }
        assert_eq!(sizes.iter().sum::<u64>(), snapshot.key_count() as u64);
        sizes
    }

    #[track_caller]
    fn assert_within(sizes: &[u64], multiple_of_an_even_split: u64) {
        let total: u64 = sizes.iter().sum();
        let largest = *sizes.iter().max().unwrap();
        assert!(
            largest * sizes.len() as u64 <= total * multiple_of_an_even_split,
            "largest range holds {largest} of {total} keys: {sizes:?}"
        );
    }

    /// `partition_keys` returns a full set of boundaries, cutting ranges of
    /// comparable size, over a snapshot of many small batches.
    ///
    /// The snapshot holds more batches than the sample holds draws and no batch
    /// reaches a full share of it, so each batch contributes at most one key.
    #[test]
    fn a_snapshot_of_many_small_batches_yields_every_boundary() {
        const BATCHES: u64 = 200;
        const KEYS_PER_BATCH: u64 = 500;
        const PARTITIONS: usize = 12;

        let snapshot = snapshot(BATCHES, KEYS_PER_BATCH, interleaved(BATCHES));
        assert_eq!(snapshot.key_count() as u64, BATCHES * KEYS_PER_BATCH);
        assert!(KEYS_PER_BATCH < snapshot.key_count() as u64 / PARTITIONS.pow(2) as u64);

        assert_within(&partition(&snapshot, PARTITIONS), 3);
    }

    /// A batch large enough for the sample size to come from the accuracy term
    /// rather than the floor still partitions evenly, so the split does not
    /// depend on `partition_keys` happening to fall back to its floor.
    #[test]
    fn a_snapshot_sampled_above_the_floor_partitions_evenly() {
        const BATCHES: u64 = 50;
        const KEYS_PER_BATCH: u64 = 4_000;
        const PARTITIONS: usize = 6;

        let snapshot = snapshot(BATCHES, KEYS_PER_BATCH, interleaved(BATCHES));
        let sample_size = partition_sample_size(snapshot.key_count(), PARTITIONS);
        assert!(
            sample_size > PARTITIONS.pow(2),
            "fixture is meant to be sampled above the floor, drew {sample_size}"
        );

        assert_within(&partition(&snapshot, PARTITIONS), 3);
    }
}
