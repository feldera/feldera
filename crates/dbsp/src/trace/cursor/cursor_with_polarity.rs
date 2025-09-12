use std::marker::PhantomData;

use crate::{
    algebra::ZCursor,
    dynamic::{DataTrait, Erase, Factory},
    trace::Cursor,
    DynZWeight, Position, ZWeight,
};

/// Cursor that contains no data.
pub struct CursorWithPolarity<K, V, T, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C: ZCursor<K, V, T>,
{
    cursor: C,
    polarity: ZWeight,
    weight: ZWeight,
    phantom: PhantomData<fn(&K, &V, &T)>,
}

impl<K, V, T, C> CursorWithPolarity<K, V, T, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C: ZCursor<K, V, T>,
{
    pub fn new(cursor: C, polarity: bool) -> Self {
        Self {
            cursor,
            polarity: if polarity { 1 } else { -1 },
            weight: 0,
            phantom: PhantomData,
        }
    }
}

impl<K, V, T, C> Cursor<K, V, T, DynZWeight> for CursorWithPolarity<K, V, T, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C: ZCursor<K, V, T>,
{
    fn weight_factory(&self) -> &'static dyn Factory<DynZWeight> {
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

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &DynZWeight)) {
        self.cursor
            .map_times(&mut |ts, w| logic(ts, (**w * self.polarity).erase()))
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &DynZWeight)) {
        self.cursor
            .map_times_through(upper, &mut |ts, w| logic(ts, (**w * self.polarity).erase()))
    }

    fn weight(&mut self) -> &DynZWeight
    where
        T: PartialEq<()>,
    {
        self.weight = **self.cursor.weight() * self.polarity;
        self.weight.erase()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &DynZWeight))
    where
        T: PartialEq<()>,
    {
        self.cursor.map_values(logic);
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
    }

    fn step_key_reverse(&mut self) {
        self.cursor.step_key_reverse()
    }

    fn seek_key(&mut self, key: &K) {
        self.cursor.seek_key(key);
    }

    fn seek_key_exact(&mut self, key: &K, hash: Option<u64>) -> bool {
        self.cursor.seek_key_exact(key, hash)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with(predicate);
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.cursor.seek_key_with_reverse(predicate);
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.cursor.seek_key_reverse(key);
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn step_val_reverse(&mut self) {
        self.cursor.step_val_reverse();
    }

    fn seek_val(&mut self, val: &V) {
        self.cursor.seek_val(val);
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.cursor.seek_val_reverse(val);
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.cursor.seek_val_with(predicate);
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.cursor.seek_val_with_reverse(predicate);
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
    }

    fn fast_forward_keys(&mut self) {
        self.cursor.fast_forward_keys();
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.fast_forward_vals();
    }

    fn keyval_valid(&self) -> bool {
        self.cursor.keyval_valid()
    }

    fn keyval(&self) -> (&K, &V) {
        self.cursor.keyval()
    }

    fn step_keyval(&mut self) {
        self.cursor.step_keyval();
    }

    fn step_keyval_reverse(&mut self) {
        self.cursor.step_key_reverse();
    }

    fn seek_keyval(&mut self, key: &K, val: &V)
    where
        K: PartialEq,
    {
        self.cursor.seek_keyval(key, val);
    }

    fn seek_keyval_reverse(&mut self, key: &K, val: &V)
    where
        K: PartialEq,
    {
        self.cursor.seek_keyval_reverse(key, val);
    }

    fn position(&self) -> Option<Position> {
        self.cursor.position()
    }
}
