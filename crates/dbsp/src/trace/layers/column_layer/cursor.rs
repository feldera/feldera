use crate::{
    trace::layers::{advance, column_layer::ColumnLayer, Cursor},
    utils::cursor_position_oob,
    DBData, DBWeight,
};
use std::fmt::{self, Display};

/// A cursor for walking through an [`ColumnLayer`].
#[derive(Debug, Clone)]
pub struct ColumnLayerCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pos: usize,
    storage: &'s ColumnLayer<K, R>,
    bounds: (usize, usize),
}

impl<'s, K, R> ColumnLayerCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pub const fn new(pos: usize, storage: &'s ColumnLayer<K, R>, bounds: (usize, usize)) -> Self {
        Self {
            pos,
            storage,
            bounds,
        }
    }

    pub(super) const fn storage(&self) -> &'s ColumnLayer<K, R> {
        self.storage
    }

    pub(super) const fn bounds(&self) -> (usize, usize) {
        self.bounds
    }

    pub fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        unsafe { self.storage.assume_invariants() }
        self.pos += advance(&self.storage.keys[self.pos..self.bounds.1], predicate);
    }

    pub fn current_key(&self) -> &K {
        &self.storage.keys[self.pos]
    }

    pub fn current_diff(&self) -> &R {
        &self.storage.diffs[self.pos]
    }
}

impl<'s, K, R> Cursor<'s> for ColumnLayerCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    type Key = K;

    type Item<'k> = (&'k K, &'k R)
    where
        Self: 'k;

    type ValueStorage = ();

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    fn item(&self) -> Self::Item<'s> {
        // Elide extra bounds checking
        unsafe { self.storage.assume_invariants() }

        if self.pos >= self.storage.keys.len() {
            cursor_position_oob(self.pos, self.storage.keys.len());
        }

        (&self.storage.keys[self.pos], &self.storage.diffs[self.pos])
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.pos += 1;

        if !self.valid() {
            self.pos = self.bounds.1;
        }
    }

    fn seek(&mut self, key: &Self::Key) {
        unsafe { self.storage.assume_invariants() }
        self.pos += advance(&self.storage.keys[self.pos..self.bounds.1], |k| k.lt(key));
    }

    fn last_item(&mut self) -> Option<Self::Item<'s>> {
        unsafe { self.storage.assume_invariants() }

        if self.bounds.1 > self.bounds.0 {
            Some((
                &self.storage.keys[self.bounds.1 - 1],
                &self.storage.diffs[self.bounds.1 - 1],
            ))
        } else {
            None
        }
    }

    fn valid(&self) -> bool {
        self.pos < self.bounds.1
    }

    fn rewind(&mut self) {
        self.pos = self.bounds.0;
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower;
        self.bounds = (lower, upper);
    }
}

impl<'a, K, R> Display for ColumnLayerCursor<'a, K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: ColumnLayerCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.item();
            writeln!(f, "{key:?} -> {val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}
