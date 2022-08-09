//! A generic cursor implementation merging multiple cursors.

use crate::{
    algebra::{HasZero, MonoidValue},
    trace::{consolidation::consolidate_from, cursor::Cursor},
};
use std::marker::PhantomData;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and the
/// the indices of cursors with the minimum key and minimum value. It performs
/// no clever management of these sets otherwise.
#[derive(Debug)]
pub struct CursorList<'s, K, V, T, R, C: Cursor<'s, K, V, T, R>> {
    _phantom: PhantomData<(K, V, T, R, &'s ())>,
    cursors: Vec<C>,
    min_key: Vec<usize>,
    min_val: Vec<usize>,
}

impl<'s, K, V, T, R, C: Cursor<'s, K, V, T, R>> CursorList<'s, K, V, T, R, C>
where
    K: Ord,
    V: Ord,
{
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(cursors: Vec<C>) -> Self {
        let mut result = CursorList {
            _phantom: PhantomData,
            cursors,
            min_key: Vec::new(),
            min_val: Vec::new(),
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

impl<'s, K, V, T, R, C: Cursor<'s, K, V, T, R>> Cursor<'s, K, V, T, R>
    for CursorList<'s, K, V, T, R, C>
where
    K: Ord,
    V: Ord,
    R: MonoidValue,
{
    type Storage = Vec<C::Storage>;

    // validation methods
    #[inline]
    fn key_valid(&self) -> bool {
        !self.min_key.is_empty()
    }
    #[inline]
    fn val_valid(&self) -> bool {
        !self.min_val.is_empty()
    }

    // accessors
    #[inline]
    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        debug_assert!(self.cursors[self.min_key[0]].key_valid());
        self.cursors[self.min_key[0]].key()
    }
    #[inline]
    fn val(&self) -> &V {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        debug_assert!(self.cursors[self.min_val[0]].val_valid());
        self.cursors[self.min_val[0]].val()
    }
    #[inline]
    fn map_times<L: FnMut(&T, &R)>(&mut self, mut logic: L) {
        for &index in self.min_val.iter() {
            self.cursors[index].map_times(|t, d| logic(t, d));
        }
    }

    #[inline]
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

    // key methods
    #[inline]
    fn step_key(&mut self) {
        for &index in self.min_key.iter() {
            self.cursors[index].step_key();
        }
        self.minimize_keys();
    }
    #[inline]
    fn seek_key(&mut self, key: &K) {
        for cursor in self.cursors.iter_mut() {
            cursor.seek_key(key);
        }
        self.minimize_keys();
    }

    #[inline]
    fn last_key(&mut self) -> Option<&K> {
        self.cursors
            .iter_mut()
            .map(|c| c.last_key())
            .max()
            .unwrap_or(None)
    }

    // value methods
    #[inline]
    fn step_val(&mut self) {
        for &index in self.min_val.iter() {
            self.cursors[index].step_val();
        }
        self.minimize_vals();
    }
    #[inline]
    fn seek_val(&mut self, val: &V) {
        for &index in self.min_key.iter() {
            self.cursors[index].seek_val(val);
        }
        self.minimize_vals();
    }

    fn values<'a>(&mut self, vals: &mut Vec<(&'a V, R)>)
    where
        's: 'a,
    {
        let offset = vals.len();

        for &index in self.min_val.iter() {
            self.cursors[index].values(vals);
        }

        if self.min_val.len() > 1 {
            consolidate_from(vals, offset);
        }
    }

    // rewinding methods
    #[inline]
    fn rewind_keys(&mut self) {
        for cursor in self.cursors.iter_mut() {
            cursor.rewind_keys();
        }
        self.minimize_keys();
    }
    #[inline]
    fn rewind_vals(&mut self) {
        for &index in self.min_key.iter() {
            self.cursors[index].rewind_vals();
        }
        self.minimize_vals();
    }
}
