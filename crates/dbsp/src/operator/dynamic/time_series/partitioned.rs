//! Traits and types that represent partitioned collections.
//!
//! Time series data typically uses two levels of indexing: by entity id (e.g.,
//! user, tenant, session, etc.) and by time.  We refer to the former as
//! partitioning, as it partitions a single collection into multiple
//! indexed collections.  Our basic batch API doesn't directly support
//! two-level indexing, so we emulate it by storing the secondary key
//! with data.  The resulting collection is efficiently searchable
//! first by the partition key and within each partition by the secondary
//! key, e.g., timestamp.

use dyn_clone::clone_box;

use crate::{
    algebra::{IndexedZSet, OrdIndexedZSet},
    dynamic::{DataTrait, DynPair, Factory, WeightTrait},
    trace::{Batch, BatchReader, Cursor},
};
use std::marker::PhantomData;

/// Read interface to collections with two levels of indexing.
///
/// Models a partitioned collection as a `BatchReader` indexed
/// (partitioned) by `BatchReader::Key` and by `K` within each partition.
pub trait PartitionedBatchReader<K: DataTrait + ?Sized, V: DataTrait + ?Sized>:
    BatchReader<Val = DynPair<K, V>, Time = ()>
{
}
impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized, B> PartitionedBatchReader<K, V> for B where
    B: BatchReader<Val = DynPair<K, V>, Time = ()>
{
}

/// Read/write API to partitioned data (see [`PartitionedBatchReader`]).
pub trait PartitionedBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized>:
    Batch<Val = DynPair<K, V>, Time = ()>
{
}
impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized, B> PartitionedBatch<K, V> for B where
    B: Batch<Val = DynPair<K, V>, Time = ()>
{
}

pub trait PartitionedIndexedZSet<K: DataTrait + ?Sized, V: DataTrait + ?Sized>:
    IndexedZSet<Val = DynPair<K, V>>
{
}

impl<K: DataTrait + ?Sized, V: DataTrait + ?Sized, B> PartitionedIndexedZSet<K, V> for B where
    B: IndexedZSet<Val = DynPair<K, V>>
{
}

/// Cursor over a single partition of a partitioned batch.
///
/// Iterates over a single partition of a partitioned collection.
pub struct PartitionCursor<'b, PK, K, V, R, C>
where
    PK: DataTrait + ?Sized,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    cursor: &'b mut C,
    key: Box<K>,
    weight: Box<R>,
    phantom: PhantomData<fn(&PK, &V, &R)>,
}

impl<'b, PK, K, V, R, C> PartitionCursor<'b, PK, K, V, R, C>
where
    C: Cursor<PK, DynPair<K, V>, (), R>,
    PK: DataTrait + ?Sized,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    pub fn new(cursor: &'b mut C) -> Self {
        let key = clone_box(cursor.val().fst());
        let weight = clone_box(cursor.weight());
        Self {
            cursor,
            key,
            weight,
            phantom: PhantomData,
        }
    }
}

impl<'b, C, PK, K, V, R> Cursor<K, V, (), R> for PartitionCursor<'b, PK, K, V, R, C>
where
    C: Cursor<PK, DynPair<K, V>, (), R>,
    PK: DataTrait + ?Sized,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.weight_factory()
    }

    fn key_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid() && self.cursor.val().fst() == self.key.as_ref()
    }

    fn key(&self) -> &K {
        &self.key
    }

    fn val(&self) -> &V {
        self.cursor.val().snd()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        self.cursor.map_times(logic)
    }

    fn map_times_through(&mut self, upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.cursor.map_times_through(upper, logic)
    }

    fn weight(&mut self) -> &R {
        self.cursor.weight()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.cursor.val_valid() {
            if self.cursor.val().fst() == self.key.as_ref() {
                self.cursor.weight().clone_to(&mut self.weight);
                logic(self.cursor.val().snd(), &self.weight);
                self.cursor.step_val();
            } else {
                self.cursor.val().fst().clone_to(&mut self.key);
                break;
            }
        }
    }

    fn step_key(&mut self) {
        while self.cursor.val_valid() {
            if self.cursor.val().fst() == self.key.as_ref() {
                self.cursor.step_val();
            } else {
                self.cursor.val().fst().clone_to(&mut self.key);
                break;
            }
        }
    }

    fn step_key_reverse(&mut self) {
        while self.cursor.val_valid() {
            if self.cursor.val().fst() == self.key.as_ref() {
                self.cursor.step_val_reverse();
            } else {
                self.cursor.val().fst().clone_to(&mut self.key);
                break;
            }
        }
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek_val_with(&|kv| kv.fst() >= key);
        if self.cursor.val_valid() {
            self.cursor.val().fst().clone_to(&mut self.key);
        }
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_val_with(&|kv| predicate(kv.fst()));
        if self.cursor.val_valid() {
            self.cursor.val().fst().clone_to(&mut self.key);
        }
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_val_with_reverse(&|kv| predicate(kv.fst()));
        if self.cursor.val_valid() {
            self.cursor.val().fst().clone_to(&mut self.key);
        }
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_val_with_reverse(&|kv| kv.fst() <= key);
        if self.cursor.val_valid() {
            self.cursor.val().fst().clone_to(&mut *self.key);
        }
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn seek_val(&mut self, _val: &V) {
        unimplemented!()
    }

    fn seek_val_with(&mut self, _predicate: &dyn Fn(&V) -> bool) {
        unimplemented!()
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_vals();
        self.cursor.val().fst().clone_to(&mut self.key);
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward_vals();
        self.cursor.val().fst().clone_to(&mut self.key);
    }

    fn rewind_vals(&mut self) {
        unimplemented!()
    }

    fn step_val_reverse(&mut self) {
        self.cursor.step_val_reverse();
    }

    fn seek_val_reverse(&mut self, _val: &V) {
        unimplemented!()
    }

    fn seek_val_with_reverse(&mut self, _predicate: &dyn Fn(&V) -> bool) {
        unimplemented!()
    }

    fn fast_forward_vals(&mut self) {
        unimplemented!()
    }
}

pub type OrdPartitionedIndexedZSet<PK, TS, V> = OrdIndexedZSet<PK, DynPair<TS, V>>;
