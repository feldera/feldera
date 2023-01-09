use crate::{
    trace::layers::{
        advance_erased,
        erased::{ErasedLayer, TypedLayer},
        Cursor,
    },
    utils::cursor_position_oob,
    DBData, DBWeight,
};
use std::{
    fmt::{self, Display},
    marker::PhantomData,
};

/// A cursor for walking through an [`TypedLayer`].
#[derive(Debug, Clone)]
pub struct TypedLayerCursor<'a, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    current: usize,
    storage: &'a ErasedLayer,
    bounds: (usize, usize),
    __type: PhantomData<(K, R)>,
}

impl<'a, K, R> TypedLayerCursor<'a, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pub const fn new(pos: usize, storage: &'a TypedLayer<K, R>, bounds: (usize, usize)) -> Self {
        Self {
            current: pos,
            storage: &storage.layer,
            bounds,
            __type: PhantomData,
        }
    }

    pub(super) const fn storage(&self) -> &'a ErasedLayer {
        self.storage
    }

    pub(super) const fn bounds(&self) -> (usize, usize) {
        self.bounds
    }

    pub fn seek_key(&mut self, key: &K) {
        let key = key as *const K as *const u8;
        self.current += advance_erased(
            self.storage.keys.range(self.current..self.bounds.1),
            self.storage.key_size(),
            |x| unsafe { (self.storage.keys.vtable().common.lt)(x, key) },
        );
    }

    pub fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        self.current += advance_erased(
            self.storage.keys.range(self.current..self.bounds.1),
            self.storage.key_size(),
            |x| unsafe { predicate(&*(x as *const K)) },
        );
    }

    pub fn current_key(&self) -> &'a K
    where
        K: 'static,
    {
        self.storage.keys.index_as(self.current)
    }

    pub fn current_diff(&self) -> &'a R
    where
        R: 'static,
    {
        self.storage.diffs.index_as(self.current)
    }
}

impl<'s, K, R> Cursor<'s> for TypedLayerCursor<'s, K, R>
where
    K: Ord + Clone + 'static,
    R: Clone + 'static,
{
    type Key<'k> = (&'k K, &'k R)
    where
        Self: 'k;

    type ValueStorage = ();

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    fn key(&self) -> Self::Key<'s> {
        if self.current >= self.storage.keys.len() {
            cursor_position_oob(self.current, self.storage.keys.len());
        }

        (self.current_key(), self.current_diff())
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.current += 1;

        if !self.valid() {
            self.current = self.bounds.1;
        }
    }

    fn seek<'a>(&mut self, (key, _): Self::Key<'a>)
    where
        's: 'a,
    {
        self.seek_key(key);
    }

    fn last_key(&mut self) -> Option<Self::Key<'s>> {
        if self.bounds.1 > self.bounds.0 {
            let idx = self.bounds.1 - 1;
            Some((
                self.storage.keys.index_as(idx),
                self.storage.diffs.index_as(idx),
            ))
        } else {
            None
        }
    }

    fn valid(&self) -> bool {
        self.current < self.bounds.1
    }

    fn rewind(&mut self) {
        self.current = self.bounds.0;
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.current = lower;
        self.bounds = (lower, upper);
    }
}

impl<'a, K, R> Display for TypedLayerCursor<'a, K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: TypedLayerCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.key();
            writeln!(f, "{key:?} -> {val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}
