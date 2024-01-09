//! A generic cursor implementation merging multiple cursors.

use crate::{
    algebra::{HasZero, MonoidValue},
    trace::cursor::{Cursor, Direction},
};
use std::marker::PhantomData;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and the
/// the indices of cursors with the minimum key and minimum value. It performs
/// no clever management of these sets otherwise.
#[derive(Debug, Clone)]
pub struct CursorList<K, V, T, R, C: Cursor<K, V, T, R>> {
    cursors: Vec<C>,
    current_key: Vec<usize>,
    current_val: Vec<usize>,
    #[cfg(debug_assertions)]
    val_direction: Direction,
    __type: PhantomData<(K, V, T, R)>,
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> CursorList<K, V, T, R, C>
where
    K: Ord,
    V: Ord,
{
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(cursors: Vec<C>) -> Self {
        let mut result = Self {
            cursors,
            current_key: Vec::new(),
            current_val: Vec::new(),
            #[cfg(debug_assertions)]
            val_direction: Direction::Forward,
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
        let mut min_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            let key = cursor.get_key();
            if key.is_some() {
                if min_key_opt.is_none() || key.lt(&min_key_opt) {
                    min_key_opt = key;
                    self.current_key.clear();
                }
                if key.eq(&min_key_opt) {
                    self.current_key.push(index);
                }
            }
        }

        self.minimize_vals();
    }

    fn maximize_keys(&mut self) {
        self.current_key.clear();

        // Determine the index of the cursor with minimum key.
        let mut max_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            let key = cursor.get_key();
            if key.is_some() {
                if max_key_opt.is_none() || key.gt(&max_key_opt) {
                    max_key_opt = key;
                    self.current_key.clear();
                }
                if key.eq(&max_key_opt) {
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
        let mut min_val: Option<&V> = None;
        for &index in self.current_key.iter() {
            let val = self.cursors[index].get_val();
            if val.is_some() {
                if min_val.is_none() || val.lt(&min_val) {
                    min_val = val;
                    self.current_val.clear();
                }
                if val.eq(&min_val) {
                    self.current_val.push(index);
                }
            }
        }
    }

    fn maximize_vals(&mut self) {
        self.assert_val_direction(Direction::Backward);

        self.current_val.clear();

        // Determine the index of the cursor with maximum value.
        let mut max_val: Option<&V> = None;
        for &index in self.current_key.iter() {
            let val = self.cursors[index].get_val();
            if val.is_some() {
                if max_val.is_none() || val.gt(&max_val) {
                    max_val = val;
                    self.current_val.clear();
                }
                if val.eq(&max_val) {
                    self.current_val.push(index);
                }
            }
        }
    }
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> Cursor<K, V, T, R> for CursorList<K, V, T, R, C>
where
    K: Ord,
    V: Ord,
    R: MonoidValue,
{
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

    fn fold_times<F, U>(&mut self, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        for &index in self.current_val.iter() {
            init = self.cursors[index].fold_times(init, &mut fold);
        }

        init
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        for &index in self.current_val.iter() {
            init = self.cursors[index].fold_times_through(upper, init, &mut fold);
        }

        init
    }

    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        debug_assert!(self.cursors[self.current_val[0]].val_valid());
        let mut res: R = HasZero::zero();
        self.map_times(|_, w| res.add_assign_by_ref(w));
        res
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

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_with(&predicate);
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
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

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_key.iter() {
            self.cursors[index].seek_val_with(predicate.clone());
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

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_key.iter() {
            self.cursors[index].seek_val_with_reverse(predicate.clone());
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
