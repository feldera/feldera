//! A generic cursor implementation merging pairs of different cursors.

use std::{cmp::Ordering, marker::PhantomData};

use crate::dynamic::{DataTrait, Factory, WeightTrait};

use super::{Cursor, Direction};

/// A cursor over the combined updates of two different cursors.
///
/// A `CursorPair` wraps two cursors over the same types of updates, and
/// provides navigation through their merged updates.
pub struct CursorPair<'a, K: ?Sized, V: ?Sized, T, R: ?Sized, C1: ?Sized, C2: ?Sized> {
    cursor1: &'a mut C1,
    cursor2: &'a mut C2,
    key_order: Ordering, /* Invalid keys are `Greater` than all other keys when iterating
                         forward, an `Less` than all other keys when iterating backward.
                         `Equal` implies both valid. */
    key_direction: Direction,
    val_order: Ordering, /* Invalid vals are `Greater` than all other vals when iterating
                         forward and `Less` than all other vals when iterating backward.
                         `Equal` implies both valid. */
    val_direction: Direction,
    weight: Box<R>,
    _phantom: PhantomData<fn(&K, &V, &T, &R)>,
}

impl<'a, K, V, T, R, C1, C2> CursorPair<'a, K, V, T, R, C1, C2>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C1: Cursor<K, V, T, R> + ?Sized,
    C2: Cursor<K, V, T, R> + ?Sized,
{
    pub fn new(cursor1: &'a mut C1, cursor2: &'a mut C2) -> Self {
        let key_order = match (cursor1.key_valid(), cursor2.key_valid()) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => cursor1.key().cmp(cursor2.key()),
        };

        let val_order = match (cursor1.val_valid(), cursor2.val_valid()) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => cursor1.val().cmp(cursor2.val()),
        };

        let weight = cursor1.weight_factory().default_box();

        Self {
            cursor1,
            cursor2,
            key_order,
            val_order,
            key_direction: Direction::Forward,
            val_direction: Direction::Forward,
            weight,
            _phantom: PhantomData,
        }
    }

    /// True if current key belongs to `cursor1` only.
    fn current_key1(&self) -> bool {
        self.key_direction == Direction::Forward && self.key_order == Ordering::Less
            || self.key_direction == Direction::Backward && self.key_order == Ordering::Greater
    }

    /// True if current key belongs to `cursor2` only.
    fn current_key2(&self) -> bool {
        self.key_direction == Direction::Forward && self.key_order == Ordering::Greater
            || self.key_direction == Direction::Backward && self.key_order == Ordering::Less
    }

    /// True if current key belongs to both cursors.
    fn current_key12(&self) -> bool {
        self.key_order == Ordering::Equal
    }

    /// True value belongs to `cursor1` only.
    fn current_val1(&self) -> bool {
        self.current_key1()
            || self.current_key12()
                && (self.val_direction == Direction::Forward && self.val_order == Ordering::Less
                    || self.val_direction == Direction::Backward
                        && self.val_order == Ordering::Greater)
    }

    /// True value belongs to `cursor2` only.
    fn current_val2(&self) -> bool {
        self.current_key2()
            || self.current_key12()
                && (self.val_direction == Direction::Forward && self.val_order == Ordering::Greater
                    || self.val_direction == Direction::Backward
                        && self.val_order == Ordering::Less)
    }

    /// True if current value belongs to both cursors.
    fn current_val12(&self) -> bool {
        self.current_key12() && self.val_order == Ordering::Equal
    }

    fn update_key_order_forward(&mut self) {
        debug_assert_eq!(self.key_direction, Direction::Forward);

        self.key_order = match (self.cursor1.key_valid(), self.cursor2.key_valid()) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => {
                let res = self.cursor1.key().cmp(self.cursor2.key());
                if res == Ordering::Equal {
                    self.update_val_order_forward();
                }
                res
            }
        };
    }

    fn update_key_order_reverse(&mut self) {
        debug_assert_eq!(self.key_direction, Direction::Backward);

        self.key_order = match (self.cursor1.key_valid(), self.cursor2.key_valid()) {
            (false, _) => Ordering::Less,
            (_, false) => Ordering::Greater,
            (true, true) => {
                let res = self.cursor1.key().cmp(self.cursor2.key());
                if res == Ordering::Equal {
                    self.update_val_order_forward();
                }
                res
            }
        };
    }

    fn update_val_order_forward(&mut self) {
        debug_assert_eq!(self.val_direction, Direction::Forward);

        self.val_order = match (self.cursor1.val_valid(), self.cursor2.val_valid()) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => self.cursor1.val().cmp(self.cursor2.val()),
        };
    }

    fn update_val_order_reverse(&mut self) {
        debug_assert_eq!(self.val_direction, Direction::Backward);

        self.val_order = match (self.cursor1.val_valid(), self.cursor2.val_valid()) {
            (false, _) => Ordering::Less,
            (_, false) => Ordering::Greater,
            (true, true) => self.cursor1.val().cmp(self.cursor2.val()),
        };
    }
}

impl<'a, K, V, T, R, C1, C2> Cursor<K, V, T, R> for CursorPair<'a, K, V, T, R, C1, C2>
where
    C1: Cursor<K, V, T, R>,
    C2: Cursor<K, V, T, R>,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C1: Cursor<K, V, T, R> + ?Sized,
    C2: Cursor<K, V, T, R> + ?Sized,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.cursor1.weight_factory()
    }

    // validation methods
    fn key_valid(&self) -> bool {
        if self.current_key1() {
            self.cursor1.key_valid()
        } else if self.current_key2() {
            self.cursor2.key_valid()
        } else {
            true
        }
    }

    fn val_valid(&self) -> bool {
        if self.current_val1() {
            self.cursor1.val_valid()
        } else if self.current_val2() {
            self.cursor2.val_valid()
        } else {
            true
        }
    }

    // accessors
    fn key(&self) -> &K {
        if self.current_key1() {
            self.cursor1.key()
        } else {
            self.cursor2.key()
        }
    }

    fn val(&self) -> &V {
        if self.current_val1() {
            self.cursor1.val()
        } else {
            self.cursor2.val()
        }
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        if self.current_val1() || self.current_val12() {
            self.cursor1.map_times(logic);
        }

        if self.current_val2() || self.current_val12() {
            self.cursor2.map_times(logic);
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        if self.current_val1() || self.current_val12() {
            self.cursor1.map_times_through(upper, logic);
        }

        if self.current_val2() || self.current_val12() {
            self.cursor2.map_times_through(upper, logic);
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.val_valid());
        self.weight.set_zero();

        if self.current_val1() || self.current_val12() {
            self.cursor1
                .map_times(&mut |_, w| self.weight.add_assign(w));
        }

        if self.current_val2() || self.current_val12() {
            self.cursor2
                .map_times(&mut |_, w| self.weight.add_assign(w));
        }

        &self.weight
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            // This will store current weight in `self.weight`, so we can use it below.
            self.weight();
            let val = self.val();
            logic(val, &self.weight);
            self.step_val();
        }
    }

    // key methods
    fn step_key(&mut self) {
        debug_assert_eq!(self.key_direction, Direction::Forward);

        if self.key_order != Ordering::Greater {
            self.cursor1.step_key();
        }
        if self.key_order != Ordering::Less {
            self.cursor2.step_key();
        }

        self.update_key_order_forward();
        self.val_direction = Direction::Forward;
    }

    fn step_key_reverse(&mut self) {
        debug_assert_eq!(self.key_direction, Direction::Backward);

        if self.key_order != Ordering::Less {
            self.cursor1.step_key_reverse();
        }
        if self.key_order != Ordering::Greater {
            self.cursor2.step_key_reverse();
        }

        self.update_key_order_reverse();
        self.val_direction = Direction::Forward;
    }

    fn seek_key(&mut self, key: &K) {
        debug_assert_eq!(self.key_direction, Direction::Forward);

        self.cursor1.seek_key(key);
        self.cursor2.seek_key(key);

        self.val_direction = Direction::Forward;
        self.update_key_order_forward();
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        debug_assert_eq!(self.key_direction, Direction::Forward);

        self.cursor1.seek_key_with(predicate);
        self.cursor2.seek_key_with(predicate);

        self.val_direction = Direction::Forward;
        self.update_key_order_forward();
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        debug_assert_eq!(self.key_direction, Direction::Backward);

        self.cursor1.seek_key_with_reverse(predicate);
        self.cursor2.seek_key_with_reverse(predicate);

        self.val_direction = Direction::Forward;
        self.update_key_order_reverse();
    }

    fn seek_key_reverse(&mut self, key: &K) {
        debug_assert_eq!(self.key_direction, Direction::Backward);

        self.cursor1.seek_key_reverse(key);
        self.cursor2.seek_key_reverse(key);

        self.val_direction = Direction::Forward;
        self.update_key_order_reverse();
    }

    // value methods
    fn step_val(&mut self) {
        debug_assert_eq!(self.val_direction, Direction::Forward);

        if self.current_key1() {
            self.cursor1.step_val()
        } else if self.current_key2() {
            self.cursor2.step_val()
        } else {
            if self.val_order != Ordering::Greater {
                self.cursor1.step_val();
            }
            if self.val_order != Ordering::Less {
                self.cursor2.step_val();
            }
            self.update_val_order_forward();
        }
    }

    fn step_val_reverse(&mut self) {
        debug_assert_eq!(self.val_direction, Direction::Backward);

        if self.current_key1() {
            self.cursor1.step_val_reverse()
        } else if self.current_key2() {
            self.cursor2.step_val_reverse()
        } else {
            if self.val_order != Ordering::Less {
                self.cursor1.step_val_reverse();
            }
            if self.val_order != Ordering::Greater {
                self.cursor2.step_val_reverse();
            }
            self.update_val_order_reverse();
        }
    }

    fn seek_val(&mut self, val: &V) {
        debug_assert_eq!(self.val_direction, Direction::Forward);

        if self.current_key1() {
            self.cursor1.seek_val(val);
        } else if self.current_key2() {
            self.cursor2.seek_val(val);
        } else {
            self.cursor1.seek_val(val);
            self.cursor2.seek_val(val);
            self.update_val_order_forward();
        }
    }

    fn seek_val_reverse(&mut self, val: &V) {
        debug_assert_eq!(self.val_direction, Direction::Backward);

        if self.current_key1() {
            self.cursor1.seek_val_reverse(val);
        } else if self.current_key2() {
            self.cursor2.seek_val_reverse(val);
        } else {
            self.cursor1.seek_val_reverse(val);
            self.cursor2.seek_val_reverse(val);
            self.update_val_order_reverse();
        }
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        debug_assert_eq!(self.val_direction, Direction::Forward);

        if self.current_key1() {
            self.cursor1.seek_val_with(predicate);
        } else if self.current_key2() {
            self.cursor2.seek_val_with(predicate);
        } else {
            self.cursor1.seek_val_with(predicate);
            self.cursor2.seek_val_with(predicate);
            self.update_val_order_forward();
        }
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        debug_assert_eq!(self.val_direction, Direction::Backward);

        if self.current_key1() {
            self.cursor1.seek_val_with_reverse(predicate);
        } else if self.current_key2() {
            self.cursor2.seek_val_with_reverse(predicate);
        } else {
            self.cursor1.seek_val_with_reverse(predicate);
            self.cursor2.seek_val_with_reverse(predicate);
            self.update_val_order_reverse();
        }
    }

    // rewinding methods
    fn rewind_keys(&mut self) {
        self.cursor1.rewind_keys();
        self.cursor2.rewind_keys();
        self.key_direction = Direction::Forward;
        self.val_direction = Direction::Forward;
        self.update_key_order_forward();
    }

    fn fast_forward_keys(&mut self) {
        self.cursor1.fast_forward_keys();
        self.cursor2.fast_forward_keys();

        self.key_direction = Direction::Backward;
        self.val_direction = Direction::Forward;
        self.update_key_order_reverse();
    }

    fn rewind_vals(&mut self) {
        self.val_direction = Direction::Forward;

        if self.current_key1() {
            self.cursor1.rewind_vals();
        } else if self.current_key2() {
            self.cursor2.rewind_vals();
        } else {
            self.cursor1.rewind_vals();
            self.cursor2.rewind_vals();
            self.update_val_order_forward();
        }
    }

    fn fast_forward_vals(&mut self) {
        self.val_direction = Direction::Backward;

        if self.current_key1() {
            self.cursor1.fast_forward_vals();
        } else if self.current_key2() {
            self.cursor2.fast_forward_vals();
        } else {
            self.cursor1.fast_forward_vals();
            self.cursor2.fast_forward_vals();
            self.update_val_order_reverse();
        }
    }
}
