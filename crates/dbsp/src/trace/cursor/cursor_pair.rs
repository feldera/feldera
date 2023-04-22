//! A generic cursor implementation merging pairs of different cursors.

use std::cmp::{max, Ordering};

use crate::{
    algebra::{HasZero, MonoidValue},
    trace::cursor::Cursor,
};

/// A cursor over the combined updates of two different cursors.
///
/// A `CursorPair` wraps two cursors over the same types of updates, and
/// provides navigation through their merged updates.
pub struct CursorPair<C1, C2> {
    cursor1: C1,
    cursor2: C2,
    key_order: Ordering, /* Invalid keys are `Greater` than all other keys. `Equal` implies both
                          * valid. */
    val_order: Ordering, /* Invalid vals are `Greater` than all other vals. `Equal` implies both
                          * valid. */
}

impl<K, V, T, R, C1, C2> Cursor<K, V, T, R> for CursorPair<C1, C2>
where
    K: Ord,
    V: Ord,
    C1: Cursor<K, V, T, R>,
    C2: Cursor<K, V, T, R>,
    R: MonoidValue,
{
    // validation methods
    fn key_valid(&self) -> bool {
        match self.key_order {
            Ordering::Less => self.cursor1.key_valid(),
            Ordering::Equal => true,
            Ordering::Greater => self.cursor2.key_valid(),
        }
    }

    fn val_valid(&self) -> bool {
        match (self.key_order, self.val_order) {
            (Ordering::Less, _) => self.cursor1.val_valid(),
            (Ordering::Greater, _) => self.cursor2.val_valid(),
            (Ordering::Equal, Ordering::Less) => self.cursor1.val_valid(),
            (Ordering::Equal, Ordering::Greater) => self.cursor2.val_valid(),
            (Ordering::Equal, Ordering::Equal) => true,
        }
    }

    // accessors
    fn key(&self) -> &K {
        match self.key_order {
            Ordering::Less => self.cursor1.key(),
            _ => self.cursor2.key(),
        }
    }

    fn val(&self) -> &V {
        if self.key_order == Ordering::Less
            || (self.key_order == Ordering::Equal && self.val_order != Ordering::Greater)
        {
            self.cursor1.val()
        } else {
            self.cursor2.val()
        }
    }

    fn fold_times<F, U>(&mut self, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        if self.key_order == Ordering::Less
            || (self.key_order == Ordering::Equal && self.val_order != Ordering::Greater)
        {
            init = self.cursor1.fold_times(init, &mut fold);
        }

        if self.key_order == Ordering::Greater
            || (self.key_order == Ordering::Equal && self.val_order != Ordering::Less)
        {
            init = self.cursor2.fold_times(init, fold);
        }

        init
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, mut init: U, mut fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        if self.key_order == Ordering::Less
            || (self.key_order == Ordering::Equal && self.val_order != Ordering::Greater)
        {
            init = self.cursor1.fold_times_through(upper, init, &mut fold);
        }

        if self.key_order == Ordering::Greater
            || (self.key_order == Ordering::Equal && self.val_order != Ordering::Less)
        {
            init = self.cursor2.fold_times_through(upper, init, fold);
        }

        init
    }

    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.val_valid());
        let mut res: R = HasZero::zero();
        self.map_times(|_, w| res.add_assign_by_ref(w));
        res
    }

    // key methods
    fn step_key(&mut self) {
        if self.key_order != Ordering::Greater {
            self.cursor1.step_key();
        }
        if self.key_order != Ordering::Less {
            self.cursor2.step_key();
        }

        self.key_order = match (self.cursor1.key_valid(), self.cursor2.key_valid()) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => self.cursor1.key().cmp(self.cursor2.key()),
        };
    }
    fn seek_key(&mut self, key: &K) {
        self.cursor1.seek_key(key);
        self.cursor2.seek_key(key);

        self.key_order = match (self.cursor1.key_valid(), self.cursor2.key_valid()) {
            (false, _) => Ordering::Greater,
            (_, false) => Ordering::Less,
            (true, true) => self.cursor1.key().cmp(self.cursor2.key()),
        };
    }

    fn last_key(&mut self) -> Option<&K> {
        max(self.cursor1.last_key(), self.cursor2.last_key())
    }

    // value methods
    fn step_val(&mut self) {
        match self.key_order {
            Ordering::Less => self.cursor1.step_val(),
            Ordering::Equal => {
                if self.val_order != Ordering::Greater {
                    self.cursor1.step_val();
                }
                if self.val_order != Ordering::Less {
                    self.cursor2.step_val();
                }
                self.val_order = match (self.cursor1.val_valid(), self.cursor2.val_valid()) {
                    (false, _) => Ordering::Greater,
                    (_, false) => Ordering::Less,
                    (true, true) => self.cursor1.val().cmp(self.cursor2.val()),
                };
            }
            Ordering::Greater => self.cursor2.step_val(),
        }
    }
    fn seek_val(&mut self, val: &V) {
        match self.key_order {
            Ordering::Less => self.cursor1.seek_val(val),
            Ordering::Equal => {
                self.cursor1.seek_val(val);
                self.cursor2.seek_val(val);
                self.val_order = match (self.cursor1.val_valid(), self.cursor2.val_valid()) {
                    (false, _) => Ordering::Greater,
                    (_, false) => Ordering::Less,
                    (true, true) => self.cursor1.val().cmp(self.cursor2.val()),
                };
            }
            Ordering::Greater => self.cursor2.seek_val(val),
        }
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        match self.key_order {
            Ordering::Less => self.cursor1.seek_val_with(predicate),
            Ordering::Equal => {
                self.cursor1.seek_val_with(predicate.clone());
                self.cursor2.seek_val_with(predicate);
                self.val_order = match (self.cursor1.val_valid(), self.cursor2.val_valid()) {
                    (false, _) => Ordering::Greater,
                    (_, false) => Ordering::Less,
                    (true, true) => self.cursor1.val().cmp(self.cursor2.val()),
                };
            }
            Ordering::Greater => self.cursor2.seek_val_with(predicate),
        }
    }

    // rewinding methods
    fn rewind_keys(&mut self) {
        self.cursor1.rewind_keys();
        self.cursor2.rewind_keys();
    }
    fn rewind_vals(&mut self) {
        if self.key_order != Ordering::Greater {
            self.cursor1.rewind_vals();
        }
        if self.key_order != Ordering::Less {
            self.cursor2.rewind_vals();
        }
    }
}
