use crate::{trace::cursor::Cursor, Timestamp};
use std::marker::PhantomData;

/// A `CursorGroup` iterates over values associated with a single key of a base
/// cursor of type `C: Cursor<K, V, R, T>`.
pub struct CursorGroup<'c, K, T, C> {
    /// Base cursor.
    base: &'c mut C,
    /// The cursor filters out times that are not `<= upper`.
    upper: T,
    val_valid: bool,
    phantom: PhantomData<K>,
}

impl<'c, K, T, C> CursorGroup<'c, K, T, C> {
    /// Creates a cursor over values associated with the current key
    /// of the `base` cursor restricted to times `<= upper`.
    pub fn new<V, R>(base: &'c mut C, upper: T) -> Self
    where
        C: Cursor<K, V, T, R>,
    {
        debug_assert!(base.key_valid());
        Self {
            base,
            upper,
            val_valid: true,
            phantom: PhantomData,
        }
    }
}

impl<'c, K, V, T, R, C> Cursor<V, (), T, R> for CursorGroup<'c, K, T, C>
where
    T: Timestamp,
    C: Cursor<K, V, T, R>,
    K: PartialEq,
{
    fn key_valid(&self) -> bool {
        self.base.val_valid()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn key(&self) -> &V {
        self.base.val()
    }

    fn val(&self) -> &() {
        &()
    }

    fn map_times<L>(&mut self, logic: L)
    where
        L: FnMut(&T, &R),
    {
        self.base.map_times_through(&self.upper, logic);
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        self.base.fold_times_through(&self.upper, init, fold)
    }

    fn map_times_through<L>(&mut self, upper: &T, logic: L)
    where
        L: FnMut(&T, &R),
    {
        self.base.map_times_through(&self.upper.meet(upper), logic)
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        self.base
            .fold_times_through(&self.upper.meet(upper), init, fold)
    }

    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        self.base.weight()
    }

    fn step_key(&mut self) {
        self.base.step_val();
    }

    fn step_key_reverse(&mut self) {
        self.base.step_val_reverse();
    }

    fn seek_key(&mut self, val: &V) {
        self.base.seek_val(val)
    }

    fn seek_key_reverse(&mut self, val: &V) {
        self.base.seek_val_reverse(val)
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }

    fn seek_val(&mut self, _val: &()) {}

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&()) -> bool + Clone,
    {
        if !predicate(&()) {
            self.val_valid = false;
        }
    }

    fn rewind_keys(&mut self) {
        self.base.rewind_vals();
    }

    fn fast_forward_keys(&mut self) {
        self.base.fast_forward_vals();
    }

    fn rewind_vals(&mut self) {
        self.val_valid = true;
    }

    fn step_val_reverse(&mut self) {
        self.val_valid = false;
    }

    fn seek_val_reverse(&mut self, _val: &()) {}

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&()) -> bool + Clone,
    {
        if !predicate(&()) {
            self.val_valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.val_valid = true;
    }
}
