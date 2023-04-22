//! A generic cursor implementation merging multiple cursors.

use crate::{
    algebra::{HasZero, MonoidValue},
    trace::cursor::Cursor,
};
use std::marker::PhantomData;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and the
/// the indices of cursors with the minimum key and minimum value. It performs
/// no clever management of these sets otherwise.
#[derive(Debug)]
pub struct CursorList<K, V, T, R, C: Cursor<K, V, T, R>> {
    cursors: Vec<C>,
    min_key: Vec<usize>,
    min_val: Vec<usize>,
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
            min_key: Vec::new(),
            min_val: Vec::new(),
            __type: PhantomData,
        };

        result.minimize_keys();
        result
    }

    // Initialize min_key with the indices of cursors with the minimum key.
    //
    // This method scans the current keys of each cursor, and tracks the indices
    // of cursors whose key equals the minimum valid key seen so far. As it goes,
    // if it observes an improved key it clears the current list, updates the
    // minimum key, and continues.
    //
    // Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    // in a consistent state as well.
    fn minimize_keys(&mut self) {
        self.min_key.clear();

        // Determine the index of the cursor with minimum key.
        let mut min_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            let key = cursor.get_key();
            if key.is_some() {
                if min_key_opt.is_none() || key.lt(&min_key_opt) {
                    min_key_opt = key;
                    self.min_key.clear();
                }
                if key.eq(&min_key_opt) {
                    self.min_key.push(index);
                }
            }
        }

        self.minimize_vals();
    }

    // Initialize min_val with the indices of minimum key cursors with the minimum
    // value.
    //
    // This method scans the current values of cursor with minimum keys, and tracks
    // the indices of cursors whose value equals the minimum valid value seen so
    // far. As it goes, if it observes an improved value it clears the current
    // list, updates the minimum value, and continues.
    fn minimize_vals(&mut self) {
        self.min_val.clear();

        // Determine the index of the cursor with minimum value.
        let mut min_val: Option<&V> = None;
        for &index in self.min_key.iter() {
            let val = self.cursors[index].get_val();
            if val.is_some() {
                if min_val.is_none() || val.lt(&min_val) {
                    min_val = val;
                    self.min_val.clear();
                }
                if val.eq(&min_val) {
                    self.min_val.push(index);
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
        !self.min_key.is_empty()
    }

    fn val_valid(&self) -> bool {
        !self.min_val.is_empty()
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        debug_assert!(self.cursors[self.min_key[0]].key_valid());
        self.cursors[self.min_key[0]].key()
    }

    fn val(&self) -> &V {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        debug_assert!(self.cursors[self.min_val[0]].val_valid());
        self.cursors[self.min_val[0]].val()
    }

    fn fold_times<F, U>(&mut self, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        for &index in self.min_val.iter() {
            init = self.cursors[index].fold_times(init, &mut fold);
        }

        init
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        for &index in self.min_val.iter() {
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
        debug_assert!(self.cursors[self.min_val[0]].val_valid());
        let mut res: R = HasZero::zero();
        self.map_times(|_, w| res.add_assign_by_ref(w));
        res
    }

    fn step_key(&mut self) {
        for &index in self.min_key.iter() {
            self.cursors[index].step_key();
        }
        self.minimize_keys();
    }

    fn seek_key(&mut self, key: &K) {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key(key);
        }
        self.minimize_keys();
    }

    fn last_key(&mut self) -> Option<&K> {
        self.cursors
            .iter_mut()
            .map(|c| c.last_key())
            .max()
            .unwrap_or(None)
    }

    fn step_val(&mut self) {
        for &index in self.min_val.iter() {
            self.cursors[index].step_val();
        }
        self.minimize_vals();
    }

    fn seek_val(&mut self, val: &V) {
        for &index in self.min_key.iter() {
            self.cursors[index].seek_val(val);
        }
        self.minimize_vals();
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        for &index in self.min_key.iter() {
            self.cursors[index].seek_val_with(predicate.clone());
        }
        self.minimize_vals();
    }

    fn rewind_keys(&mut self) {
        for cursor in self.cursors.iter_mut() {
            cursor.rewind_keys();
        }
        self.minimize_keys();
    }

    fn rewind_vals(&mut self) {
        for &index in self.min_key.iter() {
            self.cursors[index].rewind_vals();
        }
        self.minimize_vals();
    }
}
