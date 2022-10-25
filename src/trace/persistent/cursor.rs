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
    /// Value will be `0 <= val_idx < cur_vals.len()`.
    val_idx: usize,

    /// Temporary storage to hold the last key of the cursor.
    ///
    /// A reference to this is handed out by [`Cursor::last_key`].
    last_key: Option<B::Key>,

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
    fn update_current_key_weight(&mut self) {
        assert!(self.db_iter.valid());
        let (key, values) = PersistentTraceCursor::<'s, B>::read_key_val_weights(&mut self.db_iter);

        self.val_idx = 0;
        self.cur_key = key;
        self.cur_vals = values;
    }

    /// Loads the current key and its values from RocksDB.
    ///
    /// # Returns
    /// - The key and its values.
    /// - `None` if the iterator is invalid.
    #[allow(clippy::type_complexity)]
    fn read_key_val_weights(
        iter: &mut DBRawIterator<'s>,
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
                        iter.next();
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
    pub(super) fn new(cf: &Arc<BoundColumnFamily>) -> Self {
        let mut db_iter = ROCKS_DB_INSTANCE.raw_iterator_cf(cf);
        db_iter.seek_to_first();
        let (cur_key, cur_vals) =
            PersistentTraceCursor::<'s, B>::read_key_val_weights(&mut db_iter);

        PersistentTraceCursor {
            db_iter,
            val_idx: 0,
            cur_key,
            cur_vals,
            last_key: None,
            tmp_key: ReusableEncodeBuffer(Vec::new()),
        }
    }
}

impl<'s, B: Batch> Cursor<'s, B::Key, B::Val, B::Time, B::R> for PersistentTraceCursor<'s, B> {
    #[inline]
    fn key_valid(&self) -> bool {
        self.cur_key.is_some()
    }

    #[inline]
    fn val_valid(&self) -> bool {
        // A value is valid if `cur_vals` is set and we have not iterated past
        // the length of the current values.
        self.cur_vals.is_some() && self.val_idx < self.cur_vals.as_ref().unwrap().len()
    }

    #[inline]
    fn key(&self) -> &B::Key {
        self.cur_key.as_ref().unwrap()
    }

    #[inline]
    fn val(&self) -> &B::Val {
        &self.cur_vals.as_ref().unwrap()[self.val_idx].0
    }

    #[inline]
    fn map_times<L: FnMut(&B::Time, &B::R)>(&mut self, mut logic: L) {
        if self.val_valid() {
            for (time, val) in self.cur_vals.as_ref().unwrap()[self.val_idx].1.iter() {
                logic(time, val);
            }
        }
    }

    fn map_times_through<L: FnMut(&B::Time, &B::R)>(&mut self, mut logic: L, upper: &B::Time) {
        if self.key_valid() && self.val_valid() {
            // Note that `map_times_through` uses `less_equal` to determine if
            // `logic` should be called on a given `(time, diff)`. However,
            // `cur_vals` is sorted by `Ord` hence we have to go through all the
            // values to determine how many times `logic` should be called.
            for (time, val) in self.cur_vals.as_ref().unwrap()[self.val_idx].1.iter() {
                if time.less_equal(upper) {
                    logic(time, val);
                }
            }
        }
    }

    #[inline]
    fn weight(&mut self) -> B::R
    where
        B::Time: PartialEq<()>,
    {
        // This is super ugly because the RocksDB storage format is generic and
        // needs to support all the different data-structures, but if we can
        // call weight we can basically just access the first elements in the
        // list of lists of lists of lists...
        self.cur_vals.as_ref().unwrap()[self.val_idx].1[0].1.clone()
    }

    #[inline]
    fn step_key(&mut self) {
        if self.db_iter.valid() {
            // Note: RocksDB only allows to call `next` on a `valid` cursor
            self.db_iter.next();

            if self.db_iter.valid() {
                self.update_current_key_weight();
            } else {
                self.cur_key = None;
                self.cur_vals = None;
            }
        } else {
            self.cur_key = None;
            self.cur_vals = None;
        }
    }

    /// Returns the last key in the cursor or `None` if the cursor is empty.
    ///
    /// This method should not mutate the cursor position/state. So we seek to
    /// the end and back again, since seeking is generally efficient in RocksDB
    /// due to indexes it shouldn't be a huge problem aside from having to
    /// serialize.
    fn last_key(&mut self) -> Option<&B::Key> {
        self.db_iter.seek_to_last();

        if self.db_iter.valid() {
            let (key, _) = decode_from_slice(self.db_iter.key().unwrap(), BINCODE_CONFIG)
                .expect("Can't decode current key");
            self.last_key = Some(key);

            // Revert iterator
            match &self.cur_key {
                Some(key) => {
                    // TODO: Check if it's better to call db_iter.key() in the
                    // beginning of the function rather than encoding it here...
                    let encoded_key = self.tmp_key.encode(key).expect("Can't encode `key`");
                    self.db_iter.seek(encoded_key);
                }
                None => {
                    self.db_iter.next();
                    debug_assert!(!self.db_iter.valid());
                }
            }
        } else {
            self.last_key = None;
        }

        self.last_key.as_ref()
    }

    #[inline]
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
            self.update_current_key_weight();
        } else {
            self.cur_key = None;
            self.cur_vals = None;
            self.val_idx = 0;
        }
    }

    #[inline]
    fn step_val(&mut self) {
        self.val_idx += 1;
    }

    #[inline]
    fn seek_val(&mut self, val: &B::Val) {
        while self.val_valid() && self.val() < val {
            self.step_val();
        }
    }

    #[inline]
    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&B::Val) -> bool,
    {
        while self.val_valid() && !predicate(self.val()) {
            self.step_val();
        }
    }

    #[inline]
    fn rewind_keys(&mut self) {
        self.db_iter.seek_to_first();
        if self.db_iter.valid() {
            self.update_current_key_weight();
        } else {
            self.cur_key = None;
            self.cur_vals = None;
            self.val_idx = 0;
        }
    }

    #[inline]
    fn rewind_vals(&mut self) {
        self.val_idx = 0;
    }
}
