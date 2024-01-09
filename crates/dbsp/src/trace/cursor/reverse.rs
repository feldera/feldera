use crate::trace::Cursor;
use std::marker::PhantomData;

/// Cursor that reverses the direction of keys in the underlying
/// cursor.
///
/// WARNING: This cursor iterates over keys in descending order,
/// and thus will not work correctly with code that assumes
/// monotonically growing keys.
pub struct ReverseKeyCursor<'a, C, K, V, R> {
    cursor: &'a mut C,
    _phantom: PhantomData<(K, V, R)>,
}

impl<'a, C, K, V, R> ReverseKeyCursor<'a, C, K, V, R>
where
    C: Cursor<K, V, (), R>,
{
    pub fn new(cursor: &'a mut C) -> Self {
        cursor.fast_forward_keys();

        Self {
            cursor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, C, K, V, R> Cursor<K, V, (), R> for ReverseKeyCursor<'a, C, K, V, R>
where
    C: Cursor<K, V, (), R>,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &K {
        self.cursor.key()
    }

    fn val(&self) -> &V {
        self.cursor.val()
    }

    fn get_key(&self) -> Option<&K> {
        self.cursor.get_key()
    }

    fn get_val(&self) -> Option<&V> {
        self.cursor.get_val()
    }

    fn map_times<L>(&mut self, logic: L)
    where
        L: FnMut(&(), &R),
    {
        self.cursor.map_times(logic)
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.cursor.fold_times(init, fold)
    }

    fn map_times_through<L>(&mut self, upper: &(), logic: L)
    where
        L: FnMut(&(), &R),
    {
        self.cursor.map_times_through(upper, logic);
    }

    fn fold_times_through<F, U>(&mut self, upper: &(), init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.cursor.fold_times_through(upper, init, fold)
    }

    fn weight(&mut self) -> R {
        self.cursor.weight()
    }

    fn map_values<L: FnMut(&V, &R)>(&mut self, logic: L) {
        self.cursor.map_values(logic)
    }

    fn step_key(&mut self) {
        self.cursor.step_key_reverse()
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_key()
    }

    fn seek_key(&mut self, key: &K)
    where
        K: PartialOrd,
    {
        self.cursor.seek_key_reverse(key)
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.cursor.seek_key_with_reverse(predicate)
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.cursor.seek_key_with(predicate)
    }

    fn seek_key_reverse(&mut self, key: &K)
    where
        K: PartialOrd,
    {
        self.cursor.seek_key(key)
    }

    fn step_val(&mut self) {
        self.cursor.step_val()
    }

    fn step_val_reverse(&mut self) {
        self.cursor.step_val_reverse()
    }

    fn seek_val(&mut self, val: &V) {
        self.cursor.seek_val(val)
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.cursor.seek_val_reverse(val)
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.cursor.seek_val_with(predicate)
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.cursor.seek_val_with_reverse(predicate)
    }

    fn rewind_keys(&mut self) {
        self.cursor.fast_forward_keys()
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.rewind_keys()
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals()
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.fast_forward_vals()
    }
}
