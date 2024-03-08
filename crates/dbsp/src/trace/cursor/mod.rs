//! Traits and types for navigating ordered sequences of `(key, val, time,
//! diff)` tuples.

pub mod cursor_empty;
pub mod cursor_group;
pub mod cursor_list;
pub mod cursor_pair;
mod reverse;

pub use cursor_empty::CursorEmpty;
pub use cursor_group::CursorGroup;
pub use cursor_list::CursorList;
pub use cursor_pair::CursorPair;
pub use reverse::ReverseKeyCursor;

use crate::dynamic::Factory;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Direction {
    Forward,
    Backward,
}

/// A cursor for `(key, val, time, diff)` tuples.
///
/// A cursor navigates an ordered collection of `(key, val, time, diff)` tuples
/// in order by key, then by value within each key.  In the time-diff pairs
/// associated with each key-value pair, the times may not be ordered or unique
/// and, in particular, [`CursorList`](`cursor_list::CursorList`) cursors can
/// contain out-of-order and duplicate timestamps.  Because duplicate times are
/// possible, it's possible to have multiple diffs even when `T = ()`.
///
/// Cursors are not iterators because they allow navigation on multiple levels
/// (by key and value) and because they support efficient seeking (via
/// `seek_key` and `seek_val`).
///
/// # Visiting keys
///
/// A cursor visits keys in forward or reverse order, which is set as a mode.
/// Initially, a cursor is in the forward mode, positioned on the first key (if
/// the collection is non-empty).  A cursor in the forward mode can move and
/// seek forward with, e.g., [`step_key`] and [`seek_key`], but not backward.
/// The direction can be reversed using [`fast_forward_keys`], after which the
/// cursor can move and seek backward only, e.g. with [`step_key_reverse`] and
/// [`seek_key_reverse`].  The client may call [`rewind_keys`] and
/// [`fast_forward_keys`] as many times as necessary to reposition the cursor to
/// the first or last key in the forward or reverse mode, respectively.
///
/// A cursor can have a valid position on a key or an invalid position after the
/// last key (in the forward mode) or before the first key (in the reverse
/// mode).  A cursor for an empty collection of tuples does not have any valid
/// key positions.
///
/// # Visiting values within a key
///
/// A cursor also visits values in a forward or reverse order.  Whenever a
/// cursor moves to a new key, its value mode is reset to forward order and its
/// value position is set to the first value in the key.  This is true even if
/// the cursor is visiting keys in reverse order.  In forward order mode, the
/// cursor can move and seek forward within the values, e.g. with [`step_val`]
/// and [`seek_val`], but not backward.  The value direction may be reversed
/// with [`fast_forward_vals`], after which the cursor may move and seek only
/// backward within the values, e.g. with [`step_val_reverse`] and
/// [`seek_val_reverse`].  The client may call [`rewind_vals`] and
/// [`fast_forward_vals`] as many times as necessary to reposition the cursor to
/// the first or last value in the forward or reverse mode, respectively.
///
/// A cursor with a valid key position can have a valid value position on a
/// value or an invalid value position after the last value (in forward mode) or
/// before the first value (in reverse mode).
///
/// Every key in a nonempty collection has at least one value.
///
/// # Example
///
/// The following is typical code for iterating through all of the key-value
/// pairs navigated by a cursor:
///
/// ```ignore
/// let cursor = ...obtain cursor...;
/// while cursor.key_valid() {
///     while cursor.val_valid() {
///         /// Do something with the current key-value pair.
///         cursor.step_val();
///     }
///     cursor.step_key();
/// }
/// ```
///
/// [`step_key`]: `Self::step_key`
/// [`seek_key`]: `Self::seek_key`
/// [`step_key_reverse`]: `Self::step_key_reverse`
/// [`seek_key_reverse`]: `Self::seek_key_reverse`
/// [`rewind_keys`]: `Self::rewind_keys`
/// [`fast_forward_keys`]: `Self::fast_forward_keys`
/// [`step_val`]: `Self::step_val`
/// [`seek_val`]: `Self::seek_val`
/// [`step_val_reverse`]: `Self::step_val_reverse`
/// [`seek_val_reverse`]: `Self::seek_val_reverse`
/// [`rewind_vals`]: `Self::rewind_vals`
/// [`fast_forward_vals`]: `Self::fast_forward_vals`
pub trait Cursor<K: ?Sized, V: ?Sized, T, R: ?Sized> {
    /*fn key_vtable(&self) -> &'static VTable<K>;
    fn val_vtable(&self) -> &'static VTable<V>;*/

    fn weight_factory(&self) -> &'static dyn Factory<R>;

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
    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R));

    /// Applies `logic` to each pair of time and difference, restricted
    /// to times `t <= upper`.
    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R));

    /// Returns the weight associated with the current key/value pair.  This
    /// concept only makes sense for cursors with unit timestamp type (`T=()`),
    /// since otherwise there is no singular definition of weight.  This method
    /// is more convenient, and may be more efficient, than the equivalent call
    /// to [`Self::map_times`].
    ///
    /// If the current key and value are not valid, behavior is unspecified.
    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>;

    /// Apply a function to all values associated with the current key.
    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>;

    /// Advances the cursor to the next key.
    fn step_key(&mut self);

    /// Moves the cursor to the previous key.
    fn step_key_reverse(&mut self);

    /// Advances the cursor to the specified key.  If `key` itself is not
    /// present, advances to the first key greater than `key`; if there is no
    /// such key, the cursor becomes invalid.
    ///
    /// This has no effect if the cursor is already positioned past `key`, so it
    /// might be desirable to call [`rewind_keys`](Self::rewind_keys) first.
    fn seek_key(&mut self, key: &K);

    /// Advances the cursor to the first key that satisfies `predicate`.
    /// Assumes that `predicate` remains true once it turns true.
    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool);

    /// Move the cursor backward to the first key that satisfies `predicate`.
    /// Assumes that `predicate` remains true once it turns true.
    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool);

    /// Moves the cursor backward to the specified key.  If `key` itself is not
    /// present, moves backward to the first key less than `key`; if there is no
    /// such key, the cursor becomes invalid.
    fn seek_key_reverse(&mut self, key: &K);

    /// Advances the cursor to the next value.
    fn step_val(&mut self);

    /// Moves the cursor to the previous value.
    fn step_val_reverse(&mut self);

    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, val: &V);

    /// Moves the cursor back to the specified value.
    fn seek_val_reverse(&mut self, val: &V);

    /// Move the cursor to the first value (for the current key) that satisfies
    /// `predicate`.  Assumes that `predicate` remains true once it turns true.
    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool);

    /// Move the cursor back to the largest value (for the current key) that
    /// satisfies `predicate`.  Assumes that `predicate` remains true once
    /// it turns true.
    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self);

    /// Moves the cursor to the last key.
    fn fast_forward_keys(&mut self);

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self);

    /// Move the cursor to the last value for the current key.
    fn fast_forward_vals(&mut self);

    /// Reports whether the current `(key, value)` pair is valid.
    /// Returns `false` if the cursor has exhausted all pairs.
    fn keyval_valid(&self) -> bool {
        self.key_valid() && self.val_valid()
    }

    /// Returns current `(key, value)` pair.  Panics if invalid.
    fn keyval(&self) -> (&K, &V) {
        (self.key(), self.val())
    }

    /// Moves the cursor to the next `(key, value)` pair.
    fn step_keyval(&mut self) {
        self.step_val();
        while self.key_valid() && !self.val_valid() {
            self.step_key();
        }
    }

    /// Moves the cursor to the previous `(key, value)` pair.
    fn step_keyval_reverse(&mut self) {
        self.step_val_reverse();
        while self.key_valid() && !self.val_valid() {
            self.step_key_reverse();
            if self.key_valid() {
                self.fast_forward_vals();
            }
        }
    }

    /// Advance the cursor to the specified `(key, value)` pair.
    fn seek_keyval(&mut self, key: &K, val: &V)
    where
        K: PartialEq,
    {
        if self.get_key() != Some(key) {
            self.seek_key(key);
        }

        if self.get_key() == Some(key) {
            self.seek_val(val);
        }

        while self.key_valid() && !self.val_valid() {
            self.step_key();
        }
    }

    /// Moves the cursor back to the specified `(key, value)` pair.
    fn seek_keyval_reverse(&mut self, key: &K, val: &V)
    where
        K: PartialEq,
    {
        if self.get_key() != Some(key) {
            self.seek_key_reverse(key);
            if self.key_valid() {
                self.fast_forward_vals();
            }
        }

        if self.get_key() == Some(key) {
            self.seek_val_reverse(val);
        }

        while self.key_valid() && !self.val_valid() {
            self.step_key_reverse();
            if self.key_valid() {
                self.fast_forward_vals();
            }
        }
    }
}

/*
/// A cursor for taking ownership of ordered `(K, V, R, T)` tuples
pub trait Consumer<K, V, R, T> {
    /// The consumer for the values and diffs associated with a particular key
    type ValueConsumer<'a>: ValueConsumer<'a, V, R, T>
    where
        Self: 'a;

    /// Returns `true` if the current key is valid
    fn key_valid(&self) -> bool;

    /// Returns a reference to the current key
    fn peek_key(&self) -> &K;

    /// Takes ownership of the current key and gets the consumer for its
    /// associated values
    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>);

    /// Advances the cursor to the specified value
    fn seek_key(&mut self, key: &K)
    where
        K: Ord;
}

/// A cursor for taking ownership of the values and diffs associated with a
/// given key
pub trait ValueConsumer<'a, V, R, T> {
    /// Returns `true` if the current value is valid
    fn value_valid(&self) -> bool;

    /// Takes ownership of the current value & diff pair
    // TODO: Maybe this should yield another consumer for `(R, T)` pairs
    fn next_value(&mut self) -> (V, R, T);

    /// Provides the number of remaining values
    fn remaining_values(&self) -> usize;

    // TODO: Seek value method?
}
*/

/*
/// Debugging and testing utilities for Cursor.
pub trait CursorDebug<K: Clone, V: Clone, T: Clone, R: Clone>: Cursor<K, V, T, R> {
    /// Rewinds the cursor and outputs its contents to a Vec
    #[allow(clippy::type_complexity)]
    fn to_vec(&mut self) -> Vec<((K, V), Vec<(T, R)>)> {
        let mut out = Vec::new();
        self.rewind_keys();
        self.rewind_vals();
        while self.key_valid() {
            while self.val_valid() {
                let mut kv_out = Vec::new();
                self.map_times(|ts, r| kv_out.push((ts.clone(), r.clone())));
                out.push(((self.key().clone(), self.val().clone()), kv_out));
                self.step_val();
            }
            self.step_key();
        }
        out
    }

    /// Returns values with time, weights for a given cursor.
    ///
    /// Starts wherever the current cursor is pointing to and walks to the end
    /// of the values for the current key.
    ///
    /// Should only be called with `key_valid() == true`.
    ///
    /// # Panics
    /// - Panics (in debug mode) if the key is not valid.
    fn val_to_vec(&mut self) -> Vec<(V, Vec<(T, R)>)> {
        debug_assert!(self.key_valid());
        let mut vs = Vec::new();
        while self.val_valid() {
            let mut weights = Vec::new();
            self.map_times(|ts, r| {
                weights.push((ts.clone(), r.clone()));
            });

            vs.push((self.val().clone(), weights));
            self.step_val();
        }

        vs
    }
}


impl<C, K: Clone, V: Clone, T: Clone, R: Clone> CursorDebug<K, V, T, R> for C where
    C: Cursor<K, V, T, R>
{
}
*/
