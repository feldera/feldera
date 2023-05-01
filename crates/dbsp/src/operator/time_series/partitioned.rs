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

use crate::{
    algebra::IndexedZSet,
    trace::{Batch, BatchReader, Cursor},
    OrdIndexedZSet,
};
use std::marker::PhantomData;

/// Read interface to collections with two levels of indexing.
///
/// Models a partitioned collection as a `BatchReader` indexed
/// (partitioned) by `BatchReader::Key` and by `K` within each partition.
pub trait PartitionedBatchReader<K, V>: BatchReader<Val = (K, V), Time = ()> {}
impl<K, V, B> PartitionedBatchReader<K, V> for B where B: BatchReader<Val = (K, V), Time = ()> {}

/// Read/write API to partitioned data (see [`PartitionedBatchReader`]).
pub trait PartitionedBatch<K, V>: Batch<Val = (K, V), Time = ()> {}
impl<K, V, B> PartitionedBatch<K, V> for B where B: Batch<Val = (K, V), Time = ()> {}

pub trait PartitionedIndexedZSet<K, V>: IndexedZSet<Val = (K, V)> + Clone + Send {}
impl<K, V, B> PartitionedIndexedZSet<K, V> for B where B: IndexedZSet<Val = (K, V)> + Clone + Send {}

/// Cursor over a single partition of a partitioned batch.
///
/// Iterates over a single partition of a partitioned collection.
pub struct PartitionCursor<'b, PK, K, V, R, C> {
    cursor: &'b mut C,
    key: K,
    phantom: PhantomData<(PK, V, R)>,
}

impl<'b, PK, K, V, R, C> PartitionCursor<'b, PK, K, V, R, C>
where
    C: Cursor<PK, (K, V), (), R>,
    K: Clone,
{
    pub fn new(cursor: &'b mut C) -> Self {
        let key = cursor.val().0.clone();
        Self {
            cursor,
            key,
            phantom: PhantomData,
        }
    }
}

impl<'b, C, PK, K, V, R> Cursor<K, V, (), R> for PartitionCursor<'b, PK, K, V, R, C>
where
    C: Cursor<PK, (K, V), (), R>,
    K: Clone + Eq + Ord,
    V: 'static,
{
    fn key_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid() && self.cursor.val().0 == self.key
    }

    fn key(&self) -> &K {
        &self.key
    }

    fn val(&self) -> &V {
        &self.cursor.val().1
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.cursor.fold_times(init, fold)
    }

    fn fold_times_through<F, U>(&mut self, _upper: &(), init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.cursor.fold_times(init, fold)
    }

    fn weight(&mut self) -> R {
        self.cursor.weight()
    }

    fn step_key(&mut self) {
        while self.cursor.val_valid() {
            if self.cursor.val().0 == self.key {
                self.cursor.step_val();
            } else {
                self.key = self.cursor.val().0.clone();
                break;
            }
        }
    }

    fn step_key_reverse(&mut self) {
        while self.cursor.val_valid() {
            if self.cursor.val().0 == self.key {
                self.cursor.step_val_reverse();
            } else {
                self.key = self.cursor.val().0.clone();
                break;
            }
        }
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek_val_with(|(k, _)| k >= key);
        if self.cursor.val_valid() {
            self.key = self.cursor.val().0.clone();
        }
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.cursor.seek_val_with(|(k, _)| predicate(k));
        if self.cursor.val_valid() {
            self.key = self.cursor.val().0.clone();
        }
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.cursor.seek_val_with_reverse(|(k, _)| predicate(k));
        if self.cursor.val_valid() {
            self.key = self.cursor.val().0.clone();
        }
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_val_with_reverse(|(k, _)| k <= key);
        if self.cursor.val_valid() {
            self.key = self.cursor.val().0.clone();
        }
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn seek_val(&mut self, _val: &V) {
        unimplemented!()
    }

    fn seek_val_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&V) -> bool,
    {
        unimplemented!()
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_vals();
        self.key = self.cursor.val().0.clone();
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward_vals();
        self.key = self.cursor.val().0.clone();
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

    fn seek_val_with_reverse<P>(&mut self, _predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        unimplemented!()
    }

    fn fast_forward_vals(&mut self) {
        unimplemented!()
    }
}

pub type OrdPartitionedIndexedZSet<PK, TS, V, R> = OrdIndexedZSet<PK, (TS, V), R>;
