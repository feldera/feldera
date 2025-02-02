use std::marker::PhantomData;

use crate::{
    dynamic::{DataTrait, DynUnit, Erase, Factory, WeightTrait},
    Timestamp,
};

use super::Cursor;

/// A `CursorGroup` iterates over values associated with a single key of a base
/// cursor of type `C: Cursor<K, V, R, T>`.
pub struct CursorGroup<'c, K: DataTrait + ?Sized, T, R: WeightTrait + ?Sized, C> {
    /// Base cursor.
    base: &'c mut C,
    /// The cursor filters out times that are not `<= upper`.
    upper: T,
    val_valid: bool,
    phantom: PhantomData<fn(&K, &R)>,
}

impl<'c, K, T, R, C> CursorGroup<'c, K, T, R, C>
where
    K: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// Creates a cursor over values associated with the current key
    /// of the `base` cursor restricted to times `<= upper`.
    pub fn new<V>(base: &'c mut C, upper: T) -> Self
    where
        C: Cursor<K, V, T, R>,
        V: DataTrait + ?Sized,
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

impl<K, V, T, R, C> Cursor<V, DynUnit, T, R> for CursorGroup<'_, K, T, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: Timestamp,
    C: Cursor<K, V, T, R>,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.base.weight_factory()
    }

    fn key_valid(&self) -> bool {
        self.base.val_valid()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn key(&self) -> &V {
        self.base.val()
    }

    fn val(&self) -> &DynUnit {
        ().erase()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        self.base.map_times_through(&self.upper, logic);
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        self.base.map_times_through(&self.upper.meet(upper), logic)
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        self.base.weight()
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&DynUnit, &R))
    where
        T: PartialEq<()>,
    {
        if self.val_valid() {
            logic(().erase(), self.weight());
            self.step_val();
        }
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

    fn seek_key_exact(&mut self, val: &V) -> bool
    where
        V: PartialEq,
    {
        self.base.seek_val_exact(val)
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.base.seek_val_with(predicate)
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.base.seek_val_with_reverse(predicate)
    }

    fn seek_key_reverse(&mut self, val: &V) {
        self.base.seek_val_reverse(val)
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }

    fn seek_val(&mut self, _val: &DynUnit) {}

    fn seek_val_exact(&mut self, _val: &DynUnit) -> bool
    where
        DynUnit: PartialEq,
    {
        self.val_valid = true;
        true
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(().erase()) {
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

    fn seek_val_reverse(&mut self, _val: &DynUnit) {}

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&DynUnit) -> bool) {
        if !predicate(().erase()) {
            self.val_valid = false;
        }
    }

    fn fast_forward_vals(&mut self) {
        self.val_valid = true;
    }
}
