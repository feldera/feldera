//! A snapshot of a spine, which can be used to read from the spine without
//! holding a reference to the spine itself.

use std::fmt::Debug;
use std::sync::Arc;

use rand::Rng;
use rkyv::ser::Serializer;
use rkyv::{Archive, Archived, Deserialize, Fallible, Serialize};
use size_of::SizeOf;

use super::SpineCursor;
use crate::dynamic::DynVec;
use crate::trace::{Batch, BatchReader, Bounds, BoundsRef, Spine};
use crate::NumEntries;

#[derive(Clone, SizeOf)]
pub struct SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    batches: Vec<Arc<B>>,
    #[size_of(skip)]
    bounds: Bounds<B::Time>,
    #[size_of(skip)]
    factories: B::Factories,
}

impl<B: Batch + Send + Sync> Debug for SpineSnapshot<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpineSnapshot").finish()
    }
}

impl<B> From<&Spine<B>> for SpineSnapshot<B>
where
    B: Batch + Send + Sync,
{
    fn from(spine: &Spine<B>) -> Self {
        Self {
            bounds: spine.bounds().to_owned(),
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

    fn bounds(&self) -> BoundsRef<'_, Self::Time> {
        self.bounds.as_ref()
    }

    fn sample_keys<RG>(&self, _rng: &mut RG, _sample_size: usize, _sample: &mut DynVec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        // This method probably shouldn't be in the BatchReader
        unimplemented!("Shouldn't be called on a snapshot");
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
