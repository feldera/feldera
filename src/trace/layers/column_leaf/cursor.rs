use crate::{
    algebra::{AddAssignByRef, HasZero},
    trace::layers::{advance, column_leaf::OrderedColumnLeaf, Cursor},
    utils::assume,
};
use std::fmt::{self, Display};

/// A cursor for walking through an unordered sequence of values.
///
/// This cursor does not support `seek`, though I'm not certain how to expose
/// this.
#[derive(Clone, Debug)]
pub struct OrderedColumnLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pos: usize,
    storage: &'s OrderedColumnLeaf<K, R>,
    bounds: (usize, usize),
}

impl<'s, K, R> OrderedColumnLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    #[inline]
    pub const fn new(
        pos: usize,
        storage: &'s OrderedColumnLeaf<K, R>,
        bounds: (usize, usize),
    ) -> Self {
        Self {
            pos,
            storage,
            bounds,
        }
    }

    #[inline]
    pub(super) const fn storage(&self) -> &'s OrderedColumnLeaf<K, R> {
        self.storage
    }

    #[inline]
    pub(super) const fn bounds(&self) -> (usize, usize) {
        self.bounds
    }
}

impl<'s, K, R> OrderedColumnLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    #[inline]
    pub fn seek_key(&mut self, key: &K) {
        self.pos += advance(&self.storage.keys[self.pos..self.bounds.1], |k| k.lt(key));
    }

    #[inline]
    pub fn current_key(&self) -> &K {
        &self.storage.keys[self.pos]
    }

    #[inline]
    pub fn current_diff(&self) -> &R {
        &self.storage.diffs[self.pos]
    }
}

impl<'s, K, R> Cursor<'s> for OrderedColumnLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    type Key<'k> = (&'k K, &'k R)
    where
        Self: 'k;

    type ValueStorage = ();

    #[inline]
    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    #[inline]
    fn key(&self) -> Self::Key<'s> {
        // Elide extra bounds checking
        unsafe { assume(self.storage.keys.len() == self.storage.diffs.len()) }
        if self.pos >= self.storage.keys.len() {
            cursor_position_oob(self.pos, self.storage.keys.len());
        }

        (&self.storage.keys[self.pos], &self.storage.diffs[self.pos])
    }

    #[inline]
    fn values(&self) {}

    #[inline]
    fn step(&mut self) {
        self.pos += 1;

        if !self.valid() {
            self.pos = self.bounds.1;
        }
    }

    #[inline]
    fn seek<'a>(&mut self, key: Self::Key<'a>)
    where
        's: 'a,
    {
        self.seek_key(key.0);
    }

    #[inline]
    fn last_key(&mut self) -> Option<Self::Key<'s>> {
        if self.bounds.1 > self.bounds.0 {
            Some((
                &self.storage.keys[self.bounds.1 - 1],
                &self.storage.diffs[self.bounds.1 - 1],
            ))
        } else {
            None
        }
    }

    #[inline]
    fn valid(&self) -> bool {
        self.pos < self.bounds.1
    }

    #[inline]
    fn rewind(&mut self) {
        self.pos = self.bounds.0;
    }

    #[inline]
    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
    }
}

impl<'a, K, R> Display for OrderedColumnLeafCursor<'a, K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssignByRef + Clone + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: OrderedColumnLeafCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.key();
            writeln!(f, "{} -> {}", key, val)?;
            cursor.step();
        }

        Ok(())
    }
}

#[cold]
#[inline(never)]
fn cursor_position_oob(position: usize, length: usize) -> ! {
    panic!("the cursor was at the invalid position {position} while the leaf was only {length} elements long")
}
