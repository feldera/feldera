use crate::trace::cursor::Cursor;
use std::marker::PhantomData;

/// Cursor that contains no data.

pub struct CursorEmpty<K, V, T, R> {
    phantom: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R> Default for CursorEmpty<K, V, T, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, T, R> CursorEmpty<K, V, T, R> {
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<K, V, T, R> Cursor<K, V, T, R> for CursorEmpty<K, V, T, R>
where
    K: 'static,
    V: 'static,
    T: 'static,
    R: 'static,
{
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

    fn fold_times<F, U>(&mut self, init: U, _fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        init
    }

    fn fold_times_through<F, U>(&mut self, _upper: &T, init: U, _fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        init
    }

    fn weight(&mut self) -> R {
        panic!("CursorEmpty::weight")
    }

    fn step_key(&mut self) {
        panic!("CursorEmpty::step_key")
    }

    fn step_key_reverse(&mut self) {
        panic!("")
    }

    fn seek_key_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
    }

    fn seek_key_with_reverse<P>(&mut self, _predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
    }

    fn step_val(&mut self) {
        panic!("CursorEmpty::step_val")
    }

    fn seek_val(&mut self, _val: &V) {}

    fn seek_val_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&V) -> bool,
    {
    }

    fn rewind_keys(&mut self) {}

    fn fast_forward_keys(&mut self) {}

    fn rewind_vals(&mut self) {}

    fn step_val_reverse(&mut self) {
        panic!("CursorEmpty::step_val_reverse")
    }

    fn seek_val_reverse(&mut self, _val: &V) {}

    fn seek_val_with_reverse<P>(&mut self, _predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
    }

    fn fast_forward_vals(&mut self) {}
}
