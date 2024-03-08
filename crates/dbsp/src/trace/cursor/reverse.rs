use std::marker::PhantomData;

use crate::dynamic::{Factory, WeightTrait};

use super::Cursor;

/// Cursor that reverses the direction of keys in the underlying
/// cursor.
///
/// WARNING: This cursor iterates over keys in descending order,
/// and thus will not work correctly with code that assumes
/// monotonically growing keys.
pub struct ReverseKeyCursor<'a, C: ?Sized, K: ?Sized, V: ?Sized, R: ?Sized> {
    cursor: &'a mut C,
    _phantom: PhantomData<fn(&K, &V, &R)>,
}

impl<'a, C, K: ?Sized, V: ?Sized, R: ?Sized> ReverseKeyCursor<'a, C, K, V, R>
where
    C: Cursor<K, V, (), R> + ?Sized,
{
    pub fn new(cursor: &'a mut C) -> Self {
        cursor.fast_forward_keys();

        Self {
            cursor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, C, K: ?Sized, V: ?Sized, R: WeightTrait + ?Sized> Cursor<K, V, (), R>
    for ReverseKeyCursor<'a, C, K, V, R>
where
    C: Cursor<K, V, (), R> + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor.weight_factory()
    }

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

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        self.cursor.map_times(logic)
    }

    fn map_times_through(&mut self, upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.cursor.map_times_through(upper, logic);
    }

    fn weight(&mut self) -> &R {
        self.cursor.weight()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        self.cursor.map_values(logic)
    }

    fn step_key(&mut self) {
        self.cursor.step_key_reverse()
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_key()
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek_key_reverse(key)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with_reverse(predicate)
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with(predicate)
    }

    fn seek_key_reverse(&mut self, key: &K) {
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

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.cursor.seek_val_with(predicate)
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
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
