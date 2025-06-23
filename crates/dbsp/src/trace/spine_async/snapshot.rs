//! A snapshot of a spine, which can be used to read from the spine without
//! holding a reference to the spine itself.

use std::fmt::Debug;
use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use rand::Rng;
use rkyv::ser::Serializer;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::SpineCursor;
use crate::dynamic::{DynVec, Factory};
use crate::trace::cursor::{CursorFactory, CursorList};
use crate::trace::{Batch, BatchReader, BatchReaderFactories, Cursor, Spine};
use crate::NumEntries;

pub trait WithSnapshot<B>
where
    B: Batch,
{
    /// Returns a read-only, non-merging snapshot of the current trace
    /// state.
    fn ro_snapshot(&self) -> SpineSnapshot<B>;
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

impl<B> WithSnapshot<B> for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    fn ro_snapshot(&self) -> SpineSnapshot<B> {
        self.clone()
    }
}

impl<B: Batch + Send + Sync> Debug for SpineSnapshot<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpineSnapshot").finish()
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

    pub fn extend(&mut self, other: Self) {
        self.batches.extend(other.batches.iter().cloned())
    }

    pub fn batches(&self) -> &[Arc<B>] {
        &self.batches
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

    fn sample_keys<RG>(&self, _rng: &mut RG, _sample_size: usize, _sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        // This method probably shouldn't be in the BatchReader
        unimplemented!("Shouldn't be called on a snapshot");
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
