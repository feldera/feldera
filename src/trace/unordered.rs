use crate::{
    trace::layers::{Builder, Cursor, MergeBuilder, Trie, TupleBuilder},
    utils::assume,
};
use size_of::SizeOf;

/// A layer of unordered values
#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct UnorderedLeaf<K, R> {
    // Invariant: keys.len == diffs.len
    keys: Vec<K>,
    diffs: Vec<R>,
}

impl<K, R> UnorderedLeaf<K, R> {
    /// Create an empty `UnorderedLeaf`
    pub const fn empty() -> Self {
        Self {
            keys: Vec::new(),
            diffs: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> UnorderedLeaf<K, R> {
        Self {
            keys: Vec::with_capacity(capacity),
            diffs: Vec::with_capacity(capacity),
        }
    }

    /// Get the length of the current leaf
    pub fn len(&self) -> usize {
        unsafe { self.assume_invariants() }
        self.keys.len()
    }

    /// Returns `true` if the current leaf is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn keys(&self) -> &[K] {
        unsafe { self.assume_invariants() }
        &self.keys
    }

    pub fn diffs(&self) -> &[R] {
        unsafe { self.assume_invariants() }
        &self.diffs
    }

    /// Assume the invariants of the current leaf
    ///
    /// # Safety
    ///
    /// Requires that `keys` and `diffs` have the exact same length
    pub(crate) unsafe fn assume_invariants(&self) {
        assume(self.keys.len() == self.diffs.len())
    }
}

impl<K, R> Trie for UnorderedLeaf<K, R>
where
    K: Clone,
    R: Clone,
{
    type Item = (K, R);

    type Cursor<'s> = UnorderedCursor<'s, K, R>
    where
        Self: 's;
    type MergeBuilder = UnorderedMergeBuilder<K, R>;
    type TupleBuilder = UnorderedLeafBuilder<K, R>;

    fn keys(&self) -> usize {
        self.len()
    }

    fn tuples(&self) -> usize {
        self.len()
    }

    fn cursor_from(&self, lower: usize, upper: usize) -> Self::Cursor<'_> {
        UnorderedCursor {
            leaf: self,
            current: lower,
            start: lower,
            end: upper,
        }
    }
}

impl<K, R> Default for UnorderedLeaf<K, R> {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct UnorderedCursor<'a, K, R> {
    leaf: &'a UnorderedLeaf<K, R>,
    current: usize,
    start: usize,
    end: usize,
}

impl<'a, K, R> UnorderedCursor<'a, K, R> {
    const fn remaining(&self) -> usize {
        self.end - self.current
    }
}

impl<'s, K, R> Cursor<'s> for UnorderedCursor<'s, K, R> {
    type Key<'k> = &'k K
    where
        Self: 'k;

    type ValueStorage = ();

    fn keys(&self) -> usize {
        self.end - self.start
    }

    fn key(&self) -> Self::Key<'s> {
        todo!()
    }

    fn values(&self) -> <Self::ValueStorage as Trie>::Cursor<'s> {}

    fn step(&mut self) {
        todo!()
    }

    fn seek<'a>(&mut self, key: Self::Key<'a>)
    where
        's: 'a,
    {
        todo!()
    }

    fn last_key(&mut self) -> Option<Self::Key<'s>> {
        todo!()
    }

    fn valid(&self) -> bool {
        todo!()
    }

    fn rewind(&mut self) {
        todo!()
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        todo!()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct UnorderedMergeBuilder<K, R> {
    leaf: UnorderedLeaf<K, R>,
}

impl<K, R> UnorderedMergeBuilder<K, R> {
    /// Get the length of the current builder
    pub fn len(&self) -> usize {
        self.leaf.len()
    }

    /// Returns `true` if the current builder is empty
    pub fn is_empty(&self) -> bool {
        self.leaf.is_empty()
    }
}

impl<K, R> Builder for UnorderedMergeBuilder<K, R>
where
    K: Clone,
    R: Clone,
{
    type Trie = UnorderedLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        self.len()
    }

    fn done(self) -> Self::Trie {
        self.leaf
    }
}

impl<K, R> MergeBuilder for UnorderedMergeBuilder<K, R>
where
    K: Clone,
    R: Clone,
{
    fn with_capacity(left: &Self::Trie, right: &Self::Trie) -> Self {
        Self {
            leaf: UnorderedLeaf::with_capacity(left.len() + right.len()),
        }
    }

    fn with_key_capacity(capacity: usize) -> Self {
        Self {
            leaf: UnorderedLeaf::with_capacity(capacity),
        }
    }

    fn reserve(&mut self, additional: usize) {
        unsafe { self.leaf.assume_invariants() }
        self.leaf.keys.reserve(additional);
        self.leaf.diffs.reserve(additional);
        unsafe { self.leaf.assume_invariants() }
    }

    fn copy_range(&mut self, other: &Self::Trie, lower: usize, upper: usize) {
        unsafe {
            self.leaf.assume_invariants();
            other.assume_invariants();
        }

        assert!(lower <= other.keys.len() && upper <= other.keys.len());
        self.leaf.keys.extend_from_slice(&other.keys[lower..upper]);
        self.leaf
            .diffs
            .extend_from_slice(&other.diffs[lower..upper]);

        unsafe { self.leaf.assume_invariants() }
    }

    fn push_merge<'a>(
        &'a mut self,
        left: <Self::Trie as Trie>::Cursor<'a>,
        right: <Self::Trie as Trie>::Cursor<'a>,
    ) -> usize {
        // Reserve enough space for both cursor's values
        self.reserve(left.remaining() + right.remaining());

        // Copy the values into the current leaf
        self.copy_range(left.leaf, left.current, left.end);
        self.copy_range(right.leaf, right.current, right.end);

        self.len()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct UnorderedLeafBuilder<K, R> {
    leaf: UnorderedLeaf<K, R>,
}

impl<K, R> UnorderedLeafBuilder<K, R> {
    /// Create a new builder for an [`UnorderedLeaf`]
    pub const fn new() -> Self {
        Self {
            leaf: UnorderedLeaf::empty(),
        }
    }

    /// Get the length of the current builder
    pub fn len(&self) -> usize {
        self.leaf.len()
    }

    /// Returns `true` if the current builder is empty
    pub fn is_empty(&self) -> bool {
        self.leaf.is_empty()
    }
}

impl<K, R> Builder for UnorderedLeafBuilder<K, R>
where
    K: Clone,
    R: Clone,
{
    type Trie = UnorderedLeaf<K, R>;

    fn boundary(&mut self) -> usize {
        self.len()
    }

    fn done(self) -> Self::Trie {
        self.leaf
    }
}

impl<K, R> TupleBuilder for UnorderedLeafBuilder<K, R>
where
    K: Clone,
    R: Clone,
{
    type Item = (K, R);

    fn new() -> Self {
        Self::new()
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            leaf: UnorderedLeaf::with_capacity(capacity),
        }
    }

    fn reserve_tuples(&mut self, additional: usize) {
        self.leaf.keys.reserve(additional);
        self.leaf.diffs.reserve(additional);
    }

    fn tuples(&self) -> usize {
        self.len()
    }

    fn push_tuple(&mut self, (key, diff): (K, R)) {
        unsafe { self.leaf.assume_invariants() }
        self.leaf.keys.push(key);
        self.leaf.diffs.push(diff);
        unsafe { self.leaf.assume_invariants() }
    }
}

impl<K, R> Default for UnorderedLeafBuilder<K, R> {
    fn default() -> Self {
        Self::new()
    }
}
