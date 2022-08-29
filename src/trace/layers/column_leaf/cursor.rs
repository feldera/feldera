use crate::{
    algebra::{AddAssignByRef, HasZero},
    trace::{
        layers::{advance, column_leaf::OrderedColumnLeaf, Cursor},
        Consumer, ValueConsumer,
    },
};
use size_of::SizeOf;
use std::{
    fmt::{self, Display},
    mem::{needs_drop, MaybeUninit},
    ptr,
};

/// A cursor for walking through an [`OrderedColumnLeaf`].
#[derive(Debug, Clone, SizeOf)]
pub struct ColumnLeafCursor<'s, K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    pos: usize,
    storage: &'s OrderedColumnLeaf<K, R>,
    bounds: (usize, usize),
}

impl<'s, K, R> ColumnLeafCursor<'s, K, R>
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

    #[inline]
    pub fn seek_key(&mut self, key: &K) {
        unsafe { self.storage.assume_invariants() }
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

impl<'s, K, R> Cursor<'s> for ColumnLeafCursor<'s, K, R>
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
        unsafe { self.storage.assume_invariants() }

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

impl<'a, K, R> Display for ColumnLeafCursor<'a, K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssignByRef + Clone + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cursor: ColumnLeafCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.key();
            writeln!(f, "{} -> {}", key, val)?;
            cursor.step();
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ColumnLeafConsumer<K, R> {
    // Invariant: `storage.len <= self.position`, if `storage.len ==self.position` the cursor is
    // // exhausted
    position: usize,
    // Invariant: `storage.keys[position..]` and `storage.diffs[position..]` are all valid
    storage: OrderedColumnLeaf<MaybeUninit<K>, MaybeUninit<R>>,
}

impl<K, R> Consumer<K, (), R> for ColumnLeafConsumer<K, R> {
    type ValueConsumer<'a> = ColumnLeafValues<'a, K, R>
    where
        Self: 'a;

    #[inline]
    fn key_valid(&self) -> bool {
        self.position < self.storage.len()
    }

    #[inline]
    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let idx = self.position;
        if idx >= self.storage.len() {
            cursor_position_oob(idx, self.storage.len());
        }

        // We increment position before reading out the key and diff values
        self.position += 1;

        // Copy out the key and diff
        let key = unsafe { self.storage.keys[idx].assume_init_read() };

        (key, ColumnLeafValues::new(self))
    }

    #[inline]
    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        let start_position = self.position;

        // Search for the given key
        let offset = advance(&self.storage.keys[start_position..], |k| unsafe {
            k.assume_init_ref().lt(key)
        });

        // Increment the offset before we drop the elements for panic safety
        self.position += offset;

        unsafe {
            self.storage.assume_invariants();

            // Drop all the skipped keys
            ptr::drop_in_place(
                &mut self.storage.keys[start_position..start_position + offset] as *mut [_]
                    as *mut [K],
            );

            // Drop all the skipped diffs
            ptr::drop_in_place(
                &mut self.storage.keys[start_position..start_position + offset] as *mut [_]
                    as *mut [K],
            );
        }
    }
}

impl<K, R> From<OrderedColumnLeaf<K, R>> for ColumnLeafConsumer<K, R> {
    #[inline]
    fn from(leaf: OrderedColumnLeaf<K, R>) -> Self {
        Self {
            position: 0,
            storage: leaf.into_uninit(),
        }
    }
}

impl<K, R> Drop for ColumnLeafConsumer<K, R> {
    fn drop(&mut self) {
        unsafe {
            // Drop all remaining keys
            ptr::drop_in_place(&mut self.storage.keys[self.position..] as *mut [_] as *mut [K]);

            // Drop all remaining values
            ptr::drop_in_place(&mut self.storage.diffs[self.position..] as *mut [_] as *mut [K]);
        }
    }
}

#[derive(Debug)]
pub struct ColumnLeafValues<'a, K, R> {
    done: bool,
    consumer: &'a mut ColumnLeafConsumer<K, R>,
}

impl<'a, K, R> ColumnLeafValues<'a, K, R> {
    #[inline]
    fn new(consumer: &'a mut ColumnLeafConsumer<K, R>) -> Self {
        Self {
            done: false,
            consumer,
        }
    }
}

impl<'a, K, R> ValueConsumer<'a, (), R> for ColumnLeafValues<'a, K, R> {
    #[inline]
    fn value_valid(&self) -> bool {
        !self.done
    }

    #[inline]
    fn next_value(&mut self) -> ((), R) {
        if self.done {
            value_already_consumed();
        }
        self.done = true;

        // The consumer increments `position` before creating the value consumer
        let idx = self.consumer.position - 1;
        let diff = unsafe { self.consumer.storage.diffs[idx].assume_init_read() };

        ((), diff)
    }
}

impl<'a, K, R> Drop for ColumnLeafValues<'a, K, R> {
    fn drop(&mut self) {
        // If the value consumer was never used, drop the difference value
        if needs_drop::<R>() && !self.done {
            // The consumer increments `position` before creating the value consumer
            let idx = self.consumer.position - 1;
            // Drop the unused difference value
            unsafe { self.consumer.storage.diffs[idx].assume_init_drop() };
        }
    }
}

#[cold]
#[inline(never)]
fn cursor_position_oob(position: usize, length: usize) -> ! {
    panic!("the cursor was at the invalid position {position} while the leaf was only {length} elements long")
}

#[cold]
#[inline(never)]
fn value_already_consumed() -> ! {
    panic!("attempted to consume a value that was already consumed")
}
