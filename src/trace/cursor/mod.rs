//! Traits and types for navigating order sequences of
//! `(key, val, time, diff)` tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating
//! ordered collections of tuples of the form `(key, val, time, diff)`.  The
//! tuples are ordered by key, then by value within each key.  Ordering by time
//! is not guaranteed, in particular [`CursorList`](`cursor_list::CursorList`)
//! and [`CursorPair`](`cursor_pair::CursorPair`) cursors can contain
//! out-of-order and duplicate timestamps.
//!
//! The cursor is different from an iterator both because it allows navigation
//! on multiple levels (key and val), but also because it supports efficient
//! seeking (via the `seek_key` and `seek_val` methods).

pub mod cursor_list;
pub mod cursor_pair;

pub use self::cursor_list::CursorList;

/// A cursor for navigating ordered `(key, val, time, diff)` tuples.
pub trait Cursor<'s, K, V, T, R> {
    /// Type the cursor addresses data in.
    type Storage;

    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self) -> bool;

    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values
    /// for this key.
    fn val_valid(&self) -> bool;

    /// A reference to the current key. Panics if invalid.
    fn key(&self) -> &K;

    /// A reference to the current value. Panics if invalid.
    fn val(&self) -> &V;

    /// Returns a reference to the current key, if valid.
    fn get_key(&self) -> Option<&K> {
        if self.key_valid() {
            Some(self.key())
        } else {
            None
        }
    }

    /// Returns a reference to the current value, if valid.
    fn get_val(&self) -> Option<&V> {
        if self.val_valid() {
            Some(self.val())
        } else {
            None
        }
    }

    /// Applies `logic` to each pair of time and difference. Intended for
    /// mutation of the closure's scope.
    fn map_times<L: FnMut(&T, &R)>(&mut self, logic: L);

    /// Returns the weight associated with the current key/value pair.
    ///
    /// This method is only defined for cursors with unit timestamp type
    /// (`T=()`), which contain exactly one weight per key/value pair.  It
    /// is more convenient (and potentially more efficient) than using
    /// [`Self::map_times`] to iterate over a single value.
    ///
    /// # Panics
    ///
    /// Panics if not `self.key_valid() && self.val_valid()`.
    fn weight(&mut self) -> R
    where
        T: PartialEq<()>;

    /// Apply a function to all values associated with the current key.
    fn map_values<L: FnMut(&V, &R)>(&mut self, mut logic: L)
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            let weight = self.weight();
            let val = self.val();
            logic(val, &weight);
            self.step_val();
        }
    }

    /// Advances the cursor to the next key.
    fn step_key(&mut self);

    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, key: &K);

    /// Advances the cursor to the next value.
    fn step_val(&mut self);

    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, val: &V);

    /// Push sorted list of values for the current key
    /// to `vals`.
    fn values<'a>(&mut self, vals: &mut Vec<(&'a V, R)>)
    where
        's: 'a;

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self);

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self);
}

/// Debugging and testing utilities for Cursor.
pub trait CursorDebug<'s, K: Clone, V: Clone, T: Clone, R: Clone>: Cursor<'s, K, V, T, R> {
    /// Rewinds the cursor and outputs its contents to a Vec
    #[allow(clippy::type_complexity)]
    fn to_vec(&mut self) -> Vec<((K, V), Vec<(T, R)>)> {
        let mut out = Vec::new();
        self.rewind_keys();
        self.rewind_vals();
        while self.key_valid() {
            while self.val_valid() {
                let mut kv_out = Vec::new();
                self.map_times(|ts, r| {
                    kv_out.push((ts.clone(), r.clone()));
                });
                out.push(((self.key().clone(), self.val().clone()), kv_out));
                self.step_val();
            }
            self.step_key();
        }
        out
    }
}

impl<'s, C, K: Clone, V: Clone, T: Clone, R: Clone> CursorDebug<'s, K, V, T, R> for C where
    C: Cursor<'s, K, V, T, R>
{
}
