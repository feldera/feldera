//! A generic cursor implementation merging multiple cursors.

use crate::dynamic::{DataTrait, Factory, WeightTrait};
use dyn_clone::clone_box;
use std::cmp::Ordering;
use std::marker::PhantomData;

use super::{Cursor, Direction};

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and
/// the indices of cursors with the minimum key and minimum value. It performs
/// no clever management of these sets otherwise.
pub struct CursorList<K, V, T, R: WeightTrait, C>
where
    K: ?Sized,
    V: ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, V, T, R>,
{
    // Cache vtables for faster access.
    cursors: Vec<C>,
    current_key: Vec<usize>,
    current_val: Vec<usize>,
    #[cfg(debug_assertions)]
    val_direction: Direction,
    weight: Box<R>,
    weight_factory: &'static dyn Factory<R>,
    __type: PhantomData<fn(&K, &V, &T, &R)>,
}

impl<K, V, T, R, C> Clone for CursorList<K, V, T, R, C>
where
    K: ?Sized,
    V: ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, V, T, R> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            cursors: self.cursors.clone(),
            current_key: self.current_key.clone(),
            current_val: self.current_val.clone(),
            #[cfg(debug_assertions)]
            val_direction: self.val_direction,
            weight: clone_box(&self.weight),
            weight_factory: self.weight_factory,
            __type: PhantomData,
        }
    }
}

impl<K, V, T, R, C> CursorList<K, V, T, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, V, T, R>,
{
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(weight_factory: &'static dyn Factory<R>, cursors: Vec<C>) -> Self {
        let mut result = Self {
            cursors,
            current_key: Vec::new(),
            current_val: Vec::new(),
            #[cfg(debug_assertions)]
            val_direction: Direction::Forward,
            weight: weight_factory.default_box(),
            weight_factory,
            __type: PhantomData,
        };

        result.minimize_keys();
        result
    }

    #[cfg(debug_assertions)]
    fn set_val_direction(&mut self, direction: Direction) {
        self.val_direction = direction;
    }

    #[cfg(not(debug_assertions))]
    fn set_val_direction(&mut self, _direction: Direction) {}

    #[cfg(debug_assertions)]
    fn assert_val_direction(&self, direction: Direction) {
        debug_assert_eq!(self.val_direction, direction);
    }

    #[cfg(not(debug_assertions))]
    fn assert_val_direction(&self, _direction: Direction) {}

    // Initialize current_key with the indices of cursors with the minimum key.
    //
    // This method scans the current keys of each cursor, and tracks the indices
    // of cursors whose key equals the minimum valid key seen so far. As it goes,
    // if it observes an improved key it clears the current list, updates the
    // minimum key, and continues.
    //
    // Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    // in a consistent state as well.
    fn minimize_keys(&mut self) {
        self.assert_val_direction(Direction::Forward);

        self.current_key.clear();

        // Determine the index of the cursor with minimum key.
        // FIXME: add a method to perform this entire computation to `VTable`.
        let mut min_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            if let Some(key) = cursor.get_key() {
                if let Some(min_key_opt) = &mut min_key_opt {
                    if key < min_key_opt {
                        *min_key_opt = key;
                        self.current_key.clear();
                        self.current_key.push(index);
                    } else if &key == min_key_opt {
                        self.current_key.push(index);
                    }
                } else {
                    min_key_opt = Some(key);
                    self.current_key.push(index);
                }
            }
        }

        self.minimize_vals();
    }

    fn maximize_keys(&mut self) {
        self.current_key.clear();

        // Determine the index of the cursor with minimum key.
        // FIXME: add a method to perform this entire computation to `VTable`.
        let mut max_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            if let Some(key) = cursor.get_key() {
                if let Some(max_key_opt) = &mut max_key_opt {
                    match key.cmp(max_key_opt) {
                        Ordering::Greater => {
                            *max_key_opt = key;
                            self.current_key.clear();
                            self.current_key.push(index);
                        }
                        Ordering::Equal => {
                            self.current_key.push(index);
                        }
                        _ => (),
                    }
                } else {
                    max_key_opt = Some(key);
                    self.current_key.push(index);
                }
            }
        }

        self.minimize_vals();
    }

    // Initialize current_val with the indices of minimum key cursors with the
    // minimum value.
    //
    // This method scans the current values of cursor with minimum keys, and tracks
    // the indices of cursors whose value equals the minimum valid value seen so
    // far. As it goes, if it observes an improved value it clears the current
    // list, updates the minimum value, and continues.
    fn minimize_vals(&mut self) {
        self.assert_val_direction(Direction::Forward);

        self.current_val.clear();

        // Determine the index of the cursor with minimum value.
        // FIXME: add a method to perform this entire computation to `VTable`.
        let mut min_val: Option<&V> = None;
        for &index in self.current_key.iter() {
            if let Some(val) = self.cursors[index].get_val() {
                if let Some(min_val) = &mut min_val {
                    match val.cmp(min_val) {
                        Ordering::Less => {
                            *min_val = val;
                            self.current_val.clear();
                            self.current_val.push(index);
                        }
                        Ordering::Equal => self.current_val.push(index),
                        _ => (),
                    }
                } else {
                    min_val = Some(val);
                    self.current_val.push(index);
                }
            }
        }
    }

    fn maximize_vals(&mut self) {
        self.assert_val_direction(Direction::Backward);

        self.current_val.clear();

        // Determine the index of the cursor with maximum value.
        // FIXME: add a method to perform this entire computation to `VTable`.
        let mut max_val: Option<&V> = None;
        for &index in self.current_key.iter() {
            if let Some(val) = self.cursors[index].get_val() {
                match &mut max_val {
                    Some(max_val) => match val.cmp(max_val) {
                        Ordering::Greater => {
                            *max_val = val;
                            self.current_val.clear();
                            self.current_val.push(index);
                        }
                        Ordering::Equal => {
                            self.current_val.push(index);
                        }
                        _ => (),
                    },
                    None => {
                        max_val = Some(val);
                        self.current_val.push(index);
                    }
                }
            }
        }
    }
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> Cursor<K, V, T, R> for CursorList<K, V, T, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }

    fn key_valid(&self) -> bool {
        !self.current_key.is_empty()
    }

    fn val_valid(&self) -> bool {
        !self.current_val.is_empty()
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        debug_assert!(self.cursors[self.current_key[0]].key_valid());
        self.cursors[self.current_key[0]].key()
    }

    fn val(&self) -> &V {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        debug_assert!(self.cursors[self.current_val[0]].val_valid());
        self.cursors[self.current_val[0]].val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        for &index in self.current_val.iter() {
            self.cursors[index].map_times(logic);
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        for &index in self.current_val.iter() {
            self.cursors[index].map_times_through(upper, logic);
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        debug_assert!(self.cursors[self.current_val[0]].val_valid());
        self.weight.as_mut().set_zero();
        for &index in self.current_val.iter() {
            self.cursors[index].map_times(&mut |_, w| self.weight.add_assign(w));
        }
        &self.weight
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            // This will update self.weight, so we can use it below.
            self.weight();
            let val = self.val();
            logic(val, &self.weight);
            self.step_val();
        }
    }

    fn step_key(&mut self) {
        for &index in self.current_key.iter() {
            self.cursors[index].step_key();
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
    }

    fn step_key_reverse(&mut self) {
        for &index in self.current_key.iter() {
            self.cursors[index].step_key_reverse();
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
    }

    fn seek_key(&mut self, key: &K) {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key(key);
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_with(&predicate);
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_with_reverse(&predicate);
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
    }

    fn seek_key_reverse(&mut self, key: &K) {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_reverse(key);
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
    }

    fn step_val(&mut self) {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_val.iter() {
            self.cursors[index].step_val();
        }
        self.minimize_vals();
    }

    fn seek_val(&mut self, val: &V) {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_key.iter() {
            self.cursors[index].seek_val(val);
        }
        self.minimize_vals();
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_key.iter() {
            self.cursors[index].seek_val_with(predicate);
        }
        self.minimize_vals();
    }

    fn rewind_keys(&mut self) {
        for cursor in self.cursors.iter_mut() {
            cursor.rewind_keys();
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
    }

    fn fast_forward_keys(&mut self) {
        for cursor in self.cursors.iter_mut() {
            cursor.fast_forward_keys();
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
    }

    fn rewind_vals(&mut self) {
        for &index in self.current_key.iter() {
            self.cursors[index].rewind_vals();
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_vals();
    }

    fn step_val_reverse(&mut self) {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_val.iter() {
            self.cursors[index].step_val_reverse();
        }
        self.maximize_vals();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_key.iter() {
            self.cursors[index].seek_val_reverse(val);
        }
        self.maximize_vals();
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_key.iter() {
            self.cursors[index].seek_val_with_reverse(predicate);
        }
        self.maximize_vals();
    }

    fn fast_forward_vals(&mut self) {
        for &index in self.current_key.iter() {
            self.cursors[index].fast_forward_vals();
        }

        self.set_val_direction(Direction::Backward);
        self.maximize_vals();
    }
}
