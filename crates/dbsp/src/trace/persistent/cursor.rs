//! Implements the cursor for the persistent trace.
//!
//! The cursor is a wrapper around the RocksDB iterator and some custom logic
//! that ensures it behaves the same as our other cursors.

use std::sync::Arc;

use bincode::decode_from_slice;
use rocksdb::{BoundColumnFamily, DBRawIterator};

use super::trace::PersistedValue;
use super::{ReusableEncodeBuffer, Values, BINCODE_CONFIG, ROCKS_DB_INSTANCE};
use crate::algebra::PartialOrder;
use crate::trace::{Batch, Cursor};

#[derive(PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

/// The cursor for the persistent trace.
pub struct PersistentTraceCursor<'s, B: Batch + 's> {
    /// Iterator of the underlying RocksDB instance.
    db_iter: DBRawIterator<'s>,

    /// Current key this will always be Some(key) if the iterator is valid.
    ///
    /// Once we reached the end (or seeked to the end) will be set to None.
    cur_key: Option<B::Key>,
    /// Values for the `cur_key`.
    ///
    /// This will always be `Some(values)` if `cur_key` is something/valid or
    /// `None` otherwise.
    cur_vals: Option<Values<B::Val, B::Time, B::R>>,
    /// Current index of iterator in `cur_vals`.
    ///
    /// Value will be `-1 <= val_idx <= cur_vals.len()`.
    /// `-1` and `cur_vals.len()` represent invalid cursor that rolled
    /// over the left/right end of the vector.
    val_idx: isize,

    /// Lower key bound: the cursor must not expose keys below this bound.
    lower_key_bound: &'s Option<B::Key>,

    /// Temporary storage serializing seeked keys.
    tmp_key: ReusableEncodeBuffer,
}

impl<'s, B> PersistentTraceCursor<'s, B>
where
    B: Batch + 's,
{
    /// Loads the current key and its values from RocksDB and stores them in the
    /// [`PersistentTraceCursor`] struct.
    ///
    /// # Panics
    /// - In case the `db_iter` is invalid.
    fn update_current_key_weight(&mut self, direction: Direction) {
        assert!(self.db_iter.valid());
        let (key, values) =
            PersistentTraceCursor::<'s, B>::read_key_val_weights(&mut self.db_iter, direction);

        if &key < self.lower_key_bound {
            self.cur_key = None;
            self.cur_vals = None;
        } else {
            self.val_idx = 0;
            self.cur_key = key;
            self.cur_vals = values;
        }
    }

    /// Loads the current key and its values from RocksDB.
    ///
    /// # Returns
    /// - The key and its values.
    /// - `None` if the iterator is invalid.
    #[allow(clippy::type_complexity)]
    fn read_key_val_weights(
        iter: &mut DBRawIterator<'s>,
        direction: Direction,
    ) -> (Option<B::Key>, Option<Values<B::Val, B::Time, B::R>>) {
        loop {
            if !iter.valid() {
                return (None, None);
            }

            if let (Some(k), Some(v)) = (iter.key(), iter.value()) {
                let (key, _) =
                    decode_from_slice(k, BINCODE_CONFIG).expect("Can't decode current key");
                let (values, _): (PersistedValue<B::Val, B::Time, B::R>, usize) =
                    decode_from_slice(v, BINCODE_CONFIG).expect("Can't decode current value");
                match values {
                    PersistedValue::Values(vals) => {
                        return (Some(key), Some(vals));
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
            } else {
                // This holds according to the RocksDB C++ API docs
                unreachable!("db_iter.valid() implies Some(key)")
            }
        }
    }
}

impl<'s, B: Batch> PersistentTraceCursor<'s, B> {
    /// Creates a new [`PersistentTraceCursor`], requires to pass a handle to
    /// the column family of the trace.
    pub(super) fn new(cf: &Arc<BoundColumnFamily>, lower_key_bound: &'s Option<B::Key>) -> Self {
        let mut db_iter = ROCKS_DB_INSTANCE.raw_iterator_cf(cf);

        db_iter.seek_to_first();

        let (cur_key, cur_vals) =
            PersistentTraceCursor::<'s, B>::read_key_val_weights(&mut db_iter, Direction::Forward);

        let mut result = PersistentTraceCursor {
            db_iter,
            val_idx: 0,
            cur_key,
            cur_vals,
            lower_key_bound,
            tmp_key: ReusableEncodeBuffer(Vec::new()),
        };

        if let Some(bound) = lower_key_bound {
            result.seek_key(bound);
        }

        result
    }
}

impl<'s, B: Batch> Cursor<B::Key, B::Val, B::Time, B::R> for PersistentTraceCursor<'s, B> {
    fn key_valid(&self) -> bool {
        self.cur_key.is_some()
    }

    fn val_valid(&self) -> bool {
        // A value is valid if `cur_vals` is set and we have not iterated past
        // the length of the current values.
        self.cur_vals.is_some()
            && self.val_idx < self.cur_vals.as_ref().unwrap().len() as isize
            && self.val_idx >= 0
    }

    fn key(&self) -> &B::Key {
        self.cur_key.as_ref().unwrap()
    }

    fn val(&self) -> &B::Val {
        &self.cur_vals.as_ref().unwrap()[self.val_idx as usize].0
    }

    fn fold_times<F, U>(&mut self, init: U, mut fold: F) -> U
    where
        F: FnMut(U, &B::Time, &B::R) -> U,
    {
        if self.key_valid() && self.val_valid() {
            self.cur_vals.as_ref().unwrap()[self.val_idx as usize]
                .1
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
            // `cur_vals` is sorted by `Ord` hence we have to go through all the
            // values to determine how many times `fold` should be called.
            self.cur_vals.as_ref().unwrap()[self.val_idx as usize]
                .1
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
        // list of lists of lists of lists...
        self.cur_vals.as_ref().unwrap()[self.val_idx as usize].1[0]
            .1
            .clone()
    }

    fn step_key(&mut self) {
        if self.db_iter.valid() {
            // Note: RocksDB only allows to call `next` on a `valid` cursor
            self.db_iter.next();

            if self.db_iter.valid() {
                self.update_current_key_weight(Direction::Forward);
            } else {
                self.cur_key = None;
                self.cur_vals = None;
            }
        } else {
            self.cur_key = None;
            self.cur_vals = None;
        }
    }

    fn step_key_reverse(&mut self) {
        if self.db_iter.valid() {
            self.db_iter.prev();

            if self.db_iter.valid() {
                self.update_current_key_weight(Direction::Backward);
            } else {
                self.cur_key = None;
                self.cur_vals = None;
            }
        } else {
            self.cur_key = None;
            self.cur_vals = None;
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
                // skip the seek call:
                self.val_idx = 0;
                return;
            }
        }

        let encoded_key = self.tmp_key.encode(key).expect("Can't encode `key`");
        self.db_iter.seek(encoded_key);
        self.cur_key = Some(key.clone());

        if self.db_iter.valid() {
            self.update_current_key_weight(Direction::Forward);
        } else {
            self.cur_key = None;
            self.cur_vals = None;
            self.val_idx = 0;
        }
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
                self.val_idx = 0;
                return;
            }
        }

        let encoded_key = self.tmp_key.encode(key).expect("Can't encode `key`");
        self.db_iter.seek(encoded_key);
        self.cur_key = Some(key.clone());

        if self.db_iter.valid() {
            self.update_current_key_weight(Direction::Backward);
        } else {
            self.cur_key = None;
            self.cur_vals = None;
            self.val_idx = 0;
        }
    }

    fn step_val(&mut self) {
        self.val_idx += 1;
    }

    fn step_val_reverse(&mut self) {
        self.val_idx -= 1;
    }

    fn seek_val(&mut self, val: &B::Val) {
        while self.val_valid() && self.val() < val {
            self.step_val();
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
        if self.db_iter.valid() {
            self.update_current_key_weight(Direction::Forward);
        } else {
            self.cur_key = None;
            self.cur_vals = None;
            self.val_idx = 0;
        }
    }

    fn fast_forward_keys(&mut self) {
        self.db_iter.seek_to_last();
        if self.db_iter.valid() {
            self.update_current_key_weight(Direction::Backward);
        } else {
            self.cur_key = None;
            self.cur_vals = None;
            self.val_idx = 0;
        }
    }

    fn rewind_vals(&mut self) {
        self.val_idx = 0;
    }

    fn fast_forward_vals(&mut self) {
        if self.cur_vals.is_some() {
            self.val_idx = self.cur_vals.as_ref().unwrap().len() as isize - 1;
        }
    }
}
