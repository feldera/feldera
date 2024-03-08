use std::marker::PhantomData;

use crate::dynamic::{Factory, WeightTrait};

use super::Cursor;

/// Cursor that contains no data.

pub struct CursorEmpty<K: ?Sized, V: ?Sized, T, R: WeightTrait + ?Sized> {
    weight_factory: &'static dyn Factory<R>,
    phantom: PhantomData<fn(&K, &V, T, &R)>,
}

impl<K: ?Sized, V: ?Sized, T, R: WeightTrait + ?Sized> CursorEmpty<K, V, T, R> {
    pub fn new(weight_factory: &'static dyn Factory<R>) -> Self {
        Self {
            weight_factory,
            phantom: PhantomData,
        }
    }
}

impl<K, V, T, R> Cursor<K, V, T, R> for CursorEmpty<K, V, T, R>
where
    K: ?Sized,
    V: ?Sized,
    T: 'static,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }

    fn key_valid(&self) -> bool {
        false
    }

    fn val_valid(&self) -> bool {
        false
    }

    fn key(&self) -> &K {
        panic!("CursorEmpty::key")
    }

    fn val(&self) -> &V {
        panic!("CursorEmpty::val")
    }

    fn map_times(&mut self, _logic: &mut dyn FnMut(&T, &R)) {}

    fn map_times_through(&mut self, _upper: &T, _logic: &mut dyn FnMut(&T, &R)) {}

    fn map_values(&mut self, _logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        panic!("CursorEmpty::weight")
    }

    fn step_key(&mut self) {
        panic!("CursorEmpty::step_key")
    }

    fn step_key_reverse(&mut self) {
        panic!("")
    }

    fn seek_key(&mut self, _key: &K) {}

    fn seek_key_with(&mut self, _predicate: &dyn Fn(&K) -> bool) {}

    fn seek_key_with_reverse(&mut self, _predicate: &dyn Fn(&K) -> bool) {}

    fn seek_key_reverse(&mut self, _key: &K) {}

    fn step_val(&mut self) {
        panic!("CursorEmpty::step_val")
    }

    fn seek_val(&mut self, _val: &V) {}

    fn seek_val_with(&mut self, _predicate: &dyn Fn(&V) -> bool) {}

    fn rewind_keys(&mut self) {}

    fn fast_forward_keys(&mut self) {}

    fn rewind_vals(&mut self) {}

    fn step_val_reverse(&mut self) {
        panic!("CursorEmpty::step_val_reverse")
    }

    fn seek_val_reverse(&mut self, _val: &V) {}

    fn seek_val_with_reverse(&mut self, _predicate: &dyn Fn(&V) -> bool) {}

    fn fast_forward_vals(&mut self) {}
}
