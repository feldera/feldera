//! Implements the cursor for the persistent trace.
//!
//! The cursor is a wrapper around the RocksDB iterator and some custom logic
//! that ensures it behaves the same as our other cursors.

use std::sync::Arc;

use rkyv::to_bytes;
use rocksdb::{BoundColumnFamily, DBRawIterator};

use super::trace::PersistedValue;
use super::{PersistedKey, Values, ROCKS_DB_INSTANCE};
use crate::algebra::PartialOrder;
use crate::trace::{unaligned_deserialize, Batch, BatchReader, Cursor, Deserializable};

#[derive(PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

/// The cursor for the persistent trace.
pub struct PersistentTraceCursor<'s, B: Batch + 's> {
    /// Iterator of the underlying RocksDB instance.
    db_iter: DBRawIterator<'s>,
    /// Current key and value this will always be Some(x) if the iterator is valid.
    ///
    /// Once we reached the end (or seeked to the end) it will be set to None.
    cur_key: Option<B::Key>,
    /// Current value.
    ///
    /// Once we reached the end (or seeked to the end) it will be set to None.
    cur_val: Option<B::Val>,
    /// Time/weight pairs for the `cur_key`.
    ///
    /// This will always be `Some(x)` if `cur_key` is `Some(y)` or
    /// `None` otherwise.
    cur_diffs: Option<Values<B::Time, B::R>>,

    /// Lower key bound: the cursor must not expose keys below this bound.
    lower_key_bound: &'s Option<B::Key>,
}

impl<'s, B> PersistentTraceCursor<'s, B>
where
    B: Batch + 's,
    <<B as BatchReader>::Key as Deserializable>::ArchivedDeser: Ord,
    <<B as BatchReader>::Val as Deserializable>::ArchivedDeser: Ord,
{
    /// Loads the current key&value and its weights from RocksDB and stores them
    /// in the [`PersistentTraceCursor`] struct.
    ///
    /// In case the iterator is invalid or we encounter a tombstone (a key-value
    /// pair that was deleted but not yet GCed) we reset the values to `None`.
    fn update_current_key_weight(&mut self, direction: Direction) {
        self.cur_key = None;
        self.cur_val = None;
        self.cur_diffs = None;

        match PersistentTraceCursor::<'s, B>::read_key_val_weights(&mut self.db_iter, direction) {
            Some((key, value, diffs)) => {
                if &Some(&key) >= &self.lower_key_bound.as_ref() {
                    self.cur_key = Some(key);
                    self.cur_val = Some(value);
                    self.cur_diffs = Some(diffs);
                }
            }
            None => {
                // We already reset to None, so it also applies to cases where
                // we're below lower_key_bound
            }
        }
    }

    /// Loads the current key and its values from RocksDB.
    ///
    /// # Returns
    /// - The key and its values.
    /// - `None` if the iterator is invalid or we encountered a tombstone (a
    ///   key-value pair that was deleted but not yet GCed)
    #[allow(clippy::type_complexity)]
    fn read_key_val_weights(
        iter: &mut DBRawIterator<'s>,
        direction: Direction,
    ) -> Option<(B::Key, B::Val, Values<B::Time, B::R>)> {
        loop {
            if !iter.valid() {
                //log::trace!("read_key_val_weights iter invalid(), None returned");
                return None;
            }

            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                let rocks_key: PersistedKey<B::Key, B::Val> = unaligned_deserialize(k);
                let (key, value) = rocks_key
                    .try_into()
                    .expect("We never store None as value (just have Option for seeking)");
                let time_weights: PersistedValue<B::Time, B::R> = unaligned_deserialize(v);
                match time_weights {
                    PersistedValue::Values(weights) => {
                        return Some((key, value, weights));
                    }
                    PersistedValue::Tombstone => {
                        // Skip tombstones:
                        // they will be deleted by the compaction filter at a later point in time
                        if direction == Direction::Forward {
                            iter.next();
                        } else {
                            iter.prev();
                        }
                        continue;
                    }
                }
            }
        }
    }
}

impl<'s, B: Batch> PersistentTraceCursor<'s, B>
where
    <<B as BatchReader>::Key as Deserializable>::ArchivedDeser: Ord,
    <<B as BatchReader>::Val as Deserializable>::ArchivedDeser: Ord,
{
    /// Creates a new [`PersistentTraceCursor`], requires to pass a handle to
    /// the column family of the trace.
    pub(super) fn new(cf: &Arc<BoundColumnFamily>, lower_key_bound: &'s Option<B::Key>) -> Self {
        let mut db_iter = ROCKS_DB_INSTANCE.raw_iterator_cf(cf);

        db_iter.seek_to_first();

        let kvw =
            PersistentTraceCursor::<'s, B>::read_key_val_weights(&mut db_iter, Direction::Forward);

        let mut result = match kvw {
            Some((key, value, diffs)) => PersistentTraceCursor {
                db_iter,
                cur_key: Some(key),
                cur_val: Some(value),
                cur_diffs: Some(diffs),
                lower_key_bound,
            },
            None => PersistentTraceCursor {
                db_iter,
                cur_key: None,
                cur_val: None,
                cur_diffs: None,
                lower_key_bound,
            },
        };

        if let Some(bound) = lower_key_bound {
            result.seek_key(bound);
        }

        result
    }
}

impl<'s, B: Batch> Cursor<B::Key, B::Val, B::Time, B::R> for PersistentTraceCursor<'s, B>
where
    <<B as BatchReader>::Key as Deserializable>::ArchivedDeser: Ord,
    <<B as BatchReader>::Val as Deserializable>::ArchivedDeser: Ord,
{
    fn key_valid(&self) -> bool {
        self.cur_key.is_some()
    }

    fn val_valid(&self) -> bool {
        self.cur_val.is_some()
    }

    fn key(&self) -> &B::Key {
        self.cur_key.as_ref().unwrap()
    }

    fn val(&self) -> &B::Val {
        self.cur_val.as_ref().unwrap()
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        if self.key_valid() && self.val_valid() {
            self.cur_diffs
                .as_ref()
                .unwrap()
                .iter()
                .fold(init, |init, (time, val)| fold(init, time, val))
        } else {
            init
        }
    }

    fn fold_times_through<F, U>(&mut self, upper: &B::Time, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        if self.key_valid() && self.val_valid() {
            // Note that `fold_times_through` uses `less_equal` to determine if
            // `fold` should be called on a given `(time, diff)`. However,
            // `cur_diffs` is sorted by `Ord` hence we have to go through all the
            // values to determine how many times `fold` should be called.
            self.cur_diffs
                .as_ref()
                .unwrap()
                .iter()
                .filter(|(time, _)| time.less_equal(upper))
                .fold(init, |init, (time, val)| fold(init, time, val))
        } else {
            init
        }
    }

    fn weight(&mut self) -> B::R
    where
        B::Time: PartialEq<()>,
    {
        // This is super ugly because the RocksDB storage format is generic and
        // needs to support all the different data-structures, but if we can
        // call weight we can basically just access the first elements in the
        // list since we know it will always be length 1...
        self.cur_diffs.as_ref().unwrap()[0].1.clone()
    }

    fn step_key(&mut self) {
        if self.db_iter.valid() {
            debug_assert!(
                self.cur_key.is_some(),
                "db_iter.valid() implies cur_key.is_some()"
            );
            // We seek to the end of the dbsp key (which are k,v pairs):
            let cur_key = self.cur_key.clone().unwrap();
            let persisted_key: PersistedKey<B::Key, B::Val> =
                PersistedKey::EndMarker(cur_key.clone());
            let encoded_key = to_bytes(&persisted_key).expect("Can't encode `key`");
            self.db_iter.seek(encoded_key);
            self.update_current_key_weight(Direction::Forward);
            return;
        } else {
            self.cur_key = None;
            self.cur_val = None;
            self.cur_diffs = None;
        }
    }

    fn step_key_reverse(&mut self) {
        if self.db_iter.valid() {
            self.db_iter.prev();
            self.update_current_key_weight(Direction::Backward);
        } else {
            self.cur_key = None;
            self.cur_val = None;
            self.cur_diffs = None;
        }
    }

    fn seek_key(&mut self, key: &B::Key) {
        if self.cur_key.is_none() {
            // We are at the end of the cursor.
            return;
        }
        if let Some(cur_key) = self.cur_key.as_ref() {
            if cur_key >= key {
                // The rocksdb seek call will start from the beginning, whereas
                // dbsp cursors will start to seek from the current position. We
                // can fix the discrepancy here since we know the batch is
                // ordered: If we're seeking something that's behind us, we just
                // skip the seek call.
                //
                // The semantics of the DRAM cursor are that the value iteration
                // gets set to the beginning of the value list, so we need to
                // reset that still by seeking to the beginning of our current
                // key.
                let persisted_key: PersistedKey<B::Key, B::Val> =
                    PersistedKey::StartMarker(cur_key.clone());
                let encoded_key = to_bytes(&persisted_key).expect("Can't encode `key`");
                self.db_iter.seek(encoded_key);
                assert!(
                    self.db_iter.valid(),
                    "We already know this key is valid because it's the key at current position"
                );
                self.update_current_key_weight(Direction::Forward);
                return;
            }
        }

        let persisted_key: PersistedKey<B::Key, B::Val> = PersistedKey::StartMarker(key.clone());
        let encoded_key = to_bytes(&persisted_key).expect("Can't encode key");
        self.db_iter.seek(encoded_key);
        self.update_current_key_weight(Direction::Forward);
    }

    fn seek_key_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&B::Key) -> bool + Clone,
    {
        unimplemented!()
    }

    fn seek_key_with_reverse<P>(&mut self, _predicate: P)
    where
        P: Fn(&B::Key) -> bool + Clone,
    {
        unimplemented!()
    }

    fn seek_key_reverse(&mut self, key: &B::Key) {
        if self.cur_key.is_none() {
            // We are at the end of the cursor.
            return;
        }
        if let Some(cur_key) = self.cur_key.as_ref() {
            if cur_key <= key {
                // The rocksdb seek call will start from the beginning, whereas
                // dbsp cursors will start to seek from the current position. We
                // can fix the discrepancy here since we know the batch is
                // ordered: If we're seeking something that's behind us, we just
                // skip the seek call:
                self.rewind_vals();
                return;
            }
        }

        let encoded_key = to_bytes(key).expect("Can't encode `key`");
        self.db_iter.seek(encoded_key);
        self.update_current_key_weight(Direction::Backward);
    }

    fn step_val(&mut self) {
        if self.db_iter.valid() {
            let old_key = self.cur_key.clone();
            // If we call .next() we either
            // - got a new value (if the dbsp key is still the same)
            //   we loaded the new value and are done
            // - reach a new key (key changed, but the value will too)
            //   we go back to the prev key, but set value to None
            //   so val_valid() will return false
            // - we reached the end (rocksdb valid() is false now)
            //   same as above, we go back one and set value to None
            self.db_iter.next();
            if self.db_iter.valid() {
                self.update_current_key_weight(Direction::Forward);
                if old_key == self.cur_key {
                    return;
                } else {
                    self.db_iter.prev();
                    self.update_current_key_weight(Direction::Forward);
                    self.cur_val = None;
                    return;
                }
            } else {
                self.cur_val = None;
                return;
            }
        }
    }

    fn step_val_reverse(&mut self) {
        if self.db_iter.valid() {
            let old_key = self.cur_key.clone();
            self.db_iter.prev();
            if self.db_iter.valid() {
                self.update_current_key_weight(Direction::Backward);
                if old_key == self.cur_key {
                    return;
                } else {
                    self.db_iter.next();
                    self.update_current_key_weight(Direction::Forward);
                    self.cur_val = None;
                    return;
                }
            } else {
                self.db_iter.seek_to_first();
                self.cur_val = None;
            }
        } else {
            self.cur_val = None;
        }
    }

    fn seek_val(&mut self, val: &B::Val) {
        if self.val_valid() {
            if self.val() >= val {
                return;
            }

            let key = self.cur_key.as_ref().unwrap().clone();
            let to_seek: PersistedKey<B::Key, B::Val> = PersistedKey::Key(key, val.clone());
            let encoded_key = to_bytes(&to_seek).expect("Can't encode `key`");

            self.db_iter.seek_for_prev(encoded_key);
            self.update_current_key_weight(Direction::Forward);
            if self.cur_val.is_some() && self.cur_val != Some(val.clone()) {
                self.step_val();
            }
        }
    }

    fn seek_val_reverse(&mut self, val: &B::Val) {
        while self.val_valid() && self.val() > val {
            self.step_val_reverse();
        }
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool,
    {
        while self.val_valid() && !predicate(self.val()) {
            self.step_val();
        }
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool,
    {
        while self.val_valid() && !predicate(self.val()) {
            self.step_val_reverse();
        }
    }

    fn rewind_keys(&mut self) {
        self.db_iter.seek_to_first();
        self.update_current_key_weight(Direction::Forward);
    }

    fn fast_forward_keys(&mut self) {
        self.db_iter.seek_to_last();
        self.update_current_key_weight(Direction::Backward);
    }

    fn rewind_vals(&mut self) {
        if self.cur_key.is_some() {
            let key = self.key().clone();
            self.seek_key(&key);
        }
    }

    fn fast_forward_vals(&mut self) {
        self.rewind_vals();
        if self.db_iter.valid() && self.key_valid() && self.val_valid() {
            let key = self.key().clone();
            let last_kv: PersistedKey<B::Key, B::Val> = PersistedKey::EndMarker(key.clone());
            let encoded_key = to_bytes(&last_kv).expect("Can't encode key");
            self.db_iter.seek_for_prev(encoded_key);
            assert!(
                self.db_iter.valid(),
                "fast_forward_vals: We just seeked to a valid key"
            );
            self.update_current_key_weight(Direction::Forward);
        }
    }
}
