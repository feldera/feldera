//! Traits and types for navigating ordered sequences of `(key, val, time,
//! diff)` tuples.

pub mod cursor_empty;
pub mod cursor_group;
pub mod cursor_list;
pub mod cursor_pair;
mod reverse;

use std::fmt::Debug;

pub use cursor_empty::CursorEmpty;
pub use cursor_group::CursorGroup;
pub use cursor_list::CursorList;
pub use cursor_pair::CursorPair;
pub use reverse::ReverseKeyCursor;
use size_of::SizeOf;

use crate::dynamic::Factory;

use super::BatchReader;

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
/// # Visiting time-diff pairs within a value
///
/// Time-diff pairs are most often visited using [`map_times`] and related
/// methods, which take a closure that is applied to each of the pairs. For
/// cases where there is known to be only one pair, [`weight`] is
/// appropriate. In special cases, the [`TimeDiffCursor`] trait allows iterating
/// through all of the pairs.
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
/// [`map_times`]: Self::map_times
/// [`weight`]: Self::weight
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

/// An object that can produce a cursor bounded by its lifetime.
///
/// [BatchReader::cursor] produces a cursor bounded by the [BatchReader]'s
/// lifetime.  Suppose [BatchReader::cursor] could use an existing [Cursor]
/// implementation, such as [CursorList], but it needs to create some data for
/// the cursor to own.  Then it's rather inconvenient because it's necessary to
/// create a new type to own the data and implement the whole broad [Cursor]
/// trait to forward every call to [CursorList]. Plus, you end up with a
/// self-referencing type, so you need to use [ouroboros].  See `SpineCursor` in
/// the async spine for a good example.
///
/// Suppose we're building a new interface where we want to return a `dyn
/// Cursor`, and some of the implementations would suffer from the problem
/// above.  We can instead return a [CursorFactory].  An implementation of this
/// trait can own the data that it needs to, and then implement
/// [CursorFactory::get_cursor] to create and return a cursor whose lifetime is
/// bounded by the [CursorFactory].  This reduces the boilerplate a great deal
/// (and we don't need [ouroboros], either).
///
/// [BatchReader::cursor] could be retrofitted to this interface, but it's not
/// clear that it's worth it, especially since it forces using `dyn Cursor`.
pub trait CursorFactory<K: ?Sized, V: ?Sized, T, R: ?Sized> {
    fn get_cursor<'a>(&'a self) -> Box<dyn Cursor<K, V, T, R> + 'a>;
}

impl<K: ?Sized, V: ?Sized, T, R: ?Sized, B> CursorFactory<K, V, T, R> for &B
where
    B: BatchReader<Key = K, Val = V, Time = T, R = R>,
{
    fn get_cursor<'a>(&'a self) -> Box<dyn Cursor<K, V, T, R> + 'a> {
        Box::new(self.cursor())
    }
}

/// A wrapper for [BatchReader] that implements [CursorFactory].
pub struct CursorFactoryWrapper<B>(pub B);

impl<K: ?Sized, V: ?Sized, T, R: ?Sized, B> CursorFactory<K, V, T, R> for CursorFactoryWrapper<B>
where
    B: BatchReader<Key = K, Val = V, Time = T, R = R>,
{
    fn get_cursor<'a>(&'a self) -> Box<dyn Cursor<K, V, T, R> + 'a> {
        Box::new(self.0.cursor())
    }
}

/// A cursor that can be cloned as a `dyn Cursor` when it is inside a [`Box`].
///
/// Rust doesn't have a built-in way to clone boxed trait objects.  This
/// provides such a way for boxed [`Cursor`]s.
pub trait ClonableCursor<'s, K, V, T, R>: Cursor<K, V, T, R> + Debug
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, V, T, R> + Send + 's>;
}

impl<'s, K, V, T, R, C> ClonableCursor<'s, K, V, T, R> for C
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
    C: Cursor<K, V, T, R> + Debug + Clone + Send + 's,
{
    fn clone_boxed(&self) -> Box<dyn ClonableCursor<'s, K, V, T, R> + Send + 's> {
        Box::new(self.clone())
    }
}

impl<K, V, T, R, C> Cursor<K, V, T, R> for Box<C>
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
    C: Cursor<K, V, T, R> + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        (**self).weight_factory()
    }

    fn key_valid(&self) -> bool {
        (**self).key_valid()
    }

    fn val_valid(&self) -> bool {
        (**self).val_valid()
    }

    fn key(&self) -> &K {
        (**self).key()
    }

    fn val(&self) -> &V {
        (**self).val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        (**self).map_times(logic)
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        (**self).map_times_through(upper, logic)
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        (**self).weight()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        (**self).map_values(logic)
    }

    fn step_key(&mut self) {
        (**self).step_key()
    }

    fn step_key_reverse(&mut self) {
        (**self).step_key_reverse()
    }

    fn seek_key(&mut self, key: &K) {
        (**self).seek_key(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        (**self).seek_key_with(predicate)
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        (**self).seek_key_with_reverse(predicate)
    }

    fn seek_key_reverse(&mut self, key: &K) {
        (**self).seek_key_reverse(key)
    }

    fn step_val(&mut self) {
        (**self).step_val()
    }

    fn step_val_reverse(&mut self) {
        (**self).step_val_reverse()
    }

    fn seek_val(&mut self, val: &V) {
        (**self).seek_val(val)
    }

    fn seek_val_reverse(&mut self, val: &V) {
        (**self).seek_val_reverse(val)
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        (**self).seek_val_with(predicate)
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        (**self).seek_val_with_reverse(predicate)
    }

    fn rewind_keys(&mut self) {
        (**self).rewind_keys()
    }

    fn fast_forward_keys(&mut self) {
        (**self).fast_forward_keys()
    }

    fn rewind_vals(&mut self) {
        (**self).rewind_vals()
    }

    fn fast_forward_vals(&mut self) {
        (**self).fast_forward_vals()
    }

    fn get_key(&self) -> Option<&K> {
        (**self).get_key()
    }

    fn get_val(&self) -> Option<&V> {
        (**self).get_val()
    }

    fn keyval_valid(&self) -> bool {
        (**self).keyval_valid()
    }

    fn keyval(&self) -> (&K, &V) {
        (**self).keyval()
    }

    fn step_keyval(&mut self) {
        (**self).step_keyval()
    }

    fn step_keyval_reverse(&mut self) {
        (**self).step_keyval_reverse()
    }

    fn seek_keyval(&mut self, key: &K, val: &V)
    where
        K: PartialEq,
    {
        (**self).seek_keyval(key, val)
    }

    fn seek_keyval_reverse(&mut self, key: &K, val: &V)
    where
        K: PartialEq,
    {
        (**self).seek_keyval_reverse(key, val)
    }
}

/// A wrapper around a `dyn Cursor` to allow choice of implementations at runtime.
#[derive(Debug, SizeOf)]
pub struct DelegatingCursor<'s, K, V, T, R>(
    pub Box<dyn ClonableCursor<'s, K, V, T, R> + Send + 's>,
)
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized;

impl<K, V, T, R> Clone for DelegatingCursor<'_, K, V, T, R>
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn clone(&self) -> Self {
        Self(self.0.clone_boxed())
    }
}

impl<K, V, T, R> Cursor<K, V, T, R> for DelegatingCursor<'_, K, V, T, R>
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.0.weight_factory()
    }

    fn key(&self) -> &K {
        self.0.key()
    }

    fn val(&self) -> &V {
        self.0.val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.0.map_times(logic)
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.0.map_times_through(upper, logic)
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        self.0.map_values(logic)
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        self.0.weight()
    }

    fn key_valid(&self) -> bool {
        self.0.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.0.val_valid()
    }

    fn step_key(&mut self) {
        self.0.step_key()
    }

    fn step_key_reverse(&mut self) {
        self.0.step_key_reverse()
    }

    fn seek_key(&mut self, key: &K) {
        self.0.seek_key(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.0.seek_key_with(predicate)
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.0.seek_key_with_reverse(predicate)
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.0.seek_key_reverse(key)
    }

    fn step_val(&mut self) {
        self.0.step_val()
    }

    fn seek_val(&mut self, val: &V) {
        self.0.seek_val(val)
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.0.seek_val_with(predicate)
    }

    fn rewind_keys(&mut self) {
        self.0.rewind_keys()
    }

    fn fast_forward_keys(&mut self) {
        self.0.fast_forward_keys()
    }

    fn rewind_vals(&mut self) {
        self.0.rewind_vals()
    }

    fn step_val_reverse(&mut self) {
        self.0.step_val_reverse()
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.0.seek_val_reverse(val)
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.0.seek_val_with_reverse(predicate)
    }

    fn fast_forward_vals(&mut self) {
        self.0.fast_forward_vals()
    }
}

/// Visits the `(time, diff)` pairs at a [`Cursor`]'s `(key, value)` position.
///
/// Obtained via [`HasTimeDiffCursor`].
///
/// Within time-diff pairs, the times may not be ordered or unique and, in
/// particular, [`CursorList`](`cursor_list::CursorList`) cursors can contain
/// out-of-order and duplicate timestamps.  Because duplicate times are
/// possible, it's possible to have multiple diffs even when `T = ()`.
///
/// That said, most specific kinds of batches do guarantee unique and ordered
/// time-diff pairs, including [`OrdWSet`](crate::trace::ord::OrdWSet),
/// [`OrdIndexedWSet`](crate::trace::ord::OrdIndexedWSet), and similar `WSet`
/// and `ZSet` types and indexed versions.
pub trait TimeDiffCursor<'a, T, R>
where
    R: ?Sized,
{
    /// Returns the current time-diff pair, if there is one, or `None` if the
    /// cursor has been exhausted.
    fn current<'b>(&'b mut self, tmp: &'b mut R) -> Option<(&'b T, &'b R)>;

    /// Advances to the next time-diff pair.
    fn step(&mut self);
}

/// Obtains a [`TimeDiffCursor`] for a [`Cursor`]'s current position.
///
/// Not every cursor type implements this trait.  It's usually better to use
/// [`Cursor::map_times`] and related functions, which are always available.
pub trait HasTimeDiffCursor<K, V, T, R>: Cursor<K, V, T, R>
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    /// The `(time, diff)` cursor type for the [`Cursor`].
    type TimeDiffCursor<'a>: TimeDiffCursor<'a, T, R>
    where
        Self: 'a;

    /// Returns the [`TimeDiffCursor`] for this cursor's key-value position.  If
    /// the current key or value is not valid, the returned cursor will have
    /// length 0.
    fn time_diff_cursor(&self) -> Self::TimeDiffCursor<'_>;
}

/// An implementation of [`TimeDiffCursor`] for simple cases.
pub struct SingletonTimeDiffCursor<'a, R>(Option<&'a R>)
where
    R: ?Sized;

impl<'a, R> SingletonTimeDiffCursor<'a, R>
where
    R: ?Sized,
{
    pub fn new(diff: Option<&'a R>) -> Self {
        Self(diff)
    }
}

impl<'a, R> TimeDiffCursor<'a, (), R> for SingletonTimeDiffCursor<'a, R>
where
    R: ?Sized,
{
    fn current(&mut self, _tmp: &mut R) -> Option<(&(), &R)> {
        self.0.map(|diff| (&(), diff))
    }

    fn step(&mut self) {
        self.0 = None;
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
