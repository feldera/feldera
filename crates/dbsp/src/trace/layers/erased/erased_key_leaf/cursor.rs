use crate::{
    trace::layers::{
        advance_erased,
        erased::{ErasedKeyLeaf, TypedErasedKeyLeaf},
        Cursor,
    },
    utils::cursor_position_oob,
    DBData, DBWeight,
};
use std::{
    fmt::{self, Display},
    marker::PhantomData,
    ops::Index,
};

/// A cursor for walking through an [`TypedKeyLeaf`].
#[derive(Debug, Clone)]
pub struct TypedKeyLeafCursor<'a, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pos: isize,
    storage: &'a ErasedKeyLeaf<R>,
    bounds: (usize, usize),
    __type: PhantomData<(K, R)>,
}

impl<'a, K, R> TypedKeyLeafCursor<'a, K, R>
where
    K: DBData,
    R: DBWeight,
{
    pub const fn new(
        pos: usize,
        storage: &'a TypedErasedKeyLeaf<K, R>,
        bounds: (usize, usize),
    ) -> Self {
        Self {
            pos: pos as isize,
            storage: &storage.layer,
            bounds,
            __type: PhantomData,
        }
    }

    pub(super) const fn storage(&self) -> &'a ErasedKeyLeaf<R> {
        self.storage
    }

    pub(super) const fn bounds(&self) -> (usize, usize) {
        self.bounds
    }

    pub fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool,
    {
        if self.valid() {
            self.pos += advance_erased(
                self.storage.keys.range(self.pos as usize..self.bounds.1),
                self.storage.key_size(),
                |x| unsafe { predicate(&*(x as *const K)) },
            ) as isize;
        }
    }

    pub fn current_key(&self) -> &'a K
    where
        K: 'static,
    {
        debug_assert!(self.pos >= 0);
        self.storage.keys.index_as(self.pos as usize)
    }

    pub fn current_diff(&self) -> &'a R
    where
        R: 'static,
    {
        debug_assert!(self.pos >= 0);
        self.storage.diffs.index(self.pos as usize)
    }
}

impl<'s, K, R> Cursor<'s> for TypedKeyLeafCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Item<'k> = (&'k K, &'k R)
    where
        Self: 'k;

    type Key = K;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.bounds.1 - self.bounds.0
    }

    fn item(&self) -> Self::Item<'s> {
        if self.pos as usize >= self.storage.keys.len() || self.pos < 0 {
            cursor_position_oob(self.pos, self.storage.keys.len());
        }

        (self.current_key(), self.current_diff())
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.pos += 1;

        if !self.valid() {
            self.pos = self.bounds.1 as isize;
        }
    }

    fn seek(&mut self, key: &Self::Key) {
        let key = key as *const K as *const u8;
        if self.valid() {
            self.pos += advance_erased(
                self.storage.keys.range(self.pos as usize..self.bounds.1),
                self.storage.key_size(),
                |x| unsafe { (self.storage.keys.vtable().common.lt)(x, key) },
            ) as isize;
        }
    }

    fn valid(&self) -> bool {
        self.pos >= self.bounds.0 as isize && self.pos < self.bounds.1 as isize
    }

    fn rewind(&mut self) {
        self.pos = self.bounds.0 as isize;
    }

    fn position(&self) -> usize {
        self.pos as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.pos = lower as isize;
        self.bounds = (lower, upper);
    }

    fn step_reverse(&mut self) {
        self.pos -= 1;

        if self.pos < self.bounds.0 as isize {
            self.pos = self.bounds.0 as isize - 1;
        }
    }

    fn seek_reverse(&mut self, _key: &Self::Key) {
        todo!()
    }

    fn fast_forward(&mut self) {
        self.pos = self.bounds.1 as isize - 1;
    }
}

impl<'a, K, R> Display for TypedKeyLeafCursor<'a, K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: TypedKeyLeafCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.item();
            writeln!(f, "{key:?} -> {val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}
