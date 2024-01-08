//! Efficiently aggregate time series data using radix trees.
//!
//! In time series processing, it is often necessary to aggregate time series
//! data over multiple overlapping time ranges, e.g., for each event in an input
//! stream, we may want to compute an aggregate over the last three months
//! relative to the event time of the input record.  This may require iterating
//! over a large volume of data for each event.  Out-of-order inputs make things
//! worse, as they require re-computing the aggregate multiple times for each
//! record.  Much of this work is redundant, as the contents of the 3-months
//! time window for adjacent events is nearly identical, so it should be
//! possible to reuse most of the computation.  One way to achieve this
//! is to cover the entire timeline with a radix tree, where each node stores
//! the value of the aggregate over the time range determined by its bit prefix.
//!
//! The following diagram shows a radix tree for a time series with 6-bit
//! timestamps for the `+` aggregate.  The root of the tree stores the sum of
//! values across the entire time series. Each edge is labeled by the prefix of
//! the child node it points to.  For instance, the leftmost child of the root
//! stores the aggregate across all events whose timestamps have have `00` in
//! their higher-order bits etc.
//!
//! ```text
//!                                                                    [00]     ┌───────┐  [11]
//!                                  ┌──────────────────────────────────────────┤  58   ├────────────────────────────────────────────┐
//!                                  │                                          └┬─────┬┘                                            │
//!                                  │                                      [01] │     │[10]                                         │
//!                                  │                               ┌───────────┘     └─────────────┐                               │
//!                                  │                               │                               │                               │
//!                       [0000] ┌───┴───┐                       ┌───┴───┐                       ┌───┴───┐                       ┌───┴───┐
//!                      ┌───────┤  11   │                       │  16   ├───────┐       ┌───────┤  15   │                       │  16   ├───────┐
//!                      │       └┬─────┬┘                       └┬─────┬┘       │       │       └┬─────┬┘                       └──┬───┬┘       │
//!                      │  [0001]│     │[001010]         [010111]│     │[0110]  │[0111] │[1000]  │     │[1010]             [110111]│   │[1110]  │[1111]
//!                      │       ┌┘     │                         │     └┐       │       │       ┌┘     │                           │   └┐       │
//!                      │       │      │                         │      │       │       │       │[1001]│                           │    │       │
//!                   ┌──┴──┐ ┌──┴──┐   │                         │   ┌──┴──┐ ┌──┴──┐ ┌──┴──┐ ┌──┴──┐   │                           │ ┌──┴──┐ ┌──┴──┐
//!                   │  5  │ │  4  │   └─┐                       └─┐ │  3  │ │  8  │ │  5  │ │  7  │   └─┐                         │ │  7  │ │  5  │
//!                   └┬──┬─┘ └┬───┬┘     │                         │ └┬──┬─┘ └┬──┬─┘ └┬──┬─┘ └┬──┬─┘     │                         │ └┬──┬─┘ └┬───┬┘
//!                    │  │    │   │      │                         │  │  │    │  │    │  │    │  │       │                         │  │  │    │   │
//!                   ┌┘  │   ┌┘   └┐     │                         │ ┌┘  │   ┌┘  │   ┌┘  │   ┌┘  │       │                         │ ┌┘  │   ┌┘   └┐
//!                   │   │   │     │     │                         │ │   │   │   │   │   │   │   │       │                         │ │   │   │     │
//!                  ┌┴┬─┬┴┬─┬┴┬─┬─┬┴┬─┬─┬┴┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬┴┬┴┬─┬┴┬─┬┴┬─┬┴┬─┬┴┬─┬┴┬─┬┴┬─┬┴┬─┬─┬─┬┴┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬┴┬┴┬─┬┴┬─┬┴┬─┬─┬┴┐
//! time series data:│4│ │1│ │1│ │ │3│ │ │2│ │ │ │ │ │ │ │ │ │ │ │ │5│1│ │2│ │3│ │5│ │3│ │2│ │1│3│3│ │ │ │3│ │ │ │ │ │ │ │ │ │ │ │ │4│5│ │2│ │1│ │x│4│
//!                  └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘
//!        timestamp: 0 1 2 3 4 5 6 7 8 9 10  12  14  16  18  20  22  24  26  28  30  32  34  36  38  40  42  44  46  48  50  52  54  56  58  60  62
//! ```
//!
//! Given this tree, we can compute the aggregate over arbitrary time range of
//! length `n` by visiting `O(log(n))` nodes.  For example to aggregate all
//! values in the time range `[6..30]`, we visit nodes `0001`, `00`, `01`, `10`,
//! `11`, `1111`.
//!
//! The radix tree representation has several nice properties:
//! * Its height is bounded by `TIMESTAMP_BITS/RADIX_BITS`, where
//!   `TIMESTAMP_BITS` is the number of bits used to represent the timestamp,
//!   and `RADIX_BITS` is the number of bits in the radix chosen for the tree,
//!   e.g., `2` in the above example.
//! * The tree is usually much more shallow in practice, as it adapts its depth
//!   by merging nodes with a single child into their parent nodes.
//! * Adding an event only requires updating nodes on the path from the
//!   corresponding leaf to the root.  Updating multiple events with similar
//!   timestamps has lower amortized cost, as they share many common ancestors.
//!
//! We encode radix trees into indexed Z-sets with node prefix as key and node
//! as value.  This way trees and tree updates can be represented as traces and
//! batches.
//!
//! This module implements two operators:
//! * [`tree_aggregate`](`crate::Stream::tree_aggregate`): assembles a time
//!   series stream into a radix tree that covers all values in the stream.
//! * [`partitioned_tree_aggregate`](`crate::Stream::partitioned_tree_aggregate`):
//!   the input stream has two levels of indexing: by logical partition, e.g.,
//!   user id or tenant id, and by time.  The operator outputs a separate tree
//!   per partition.
//!
//! These are low-level operators that are used as building blocks by other time
//! series operators like
//! [`partitioned_rolling_aggregate`](`crate::Stream::partitioned_rolling_aggregate`).
use crate::{
    algebra::{HasOne, HasZero, Semigroup},
    operator::time_series::Range,
    trace::Cursor,
};
use num::PrimInt;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{
    cmp::min,
    collections::{BTreeMap, BTreeSet},
    fmt,
    fmt::{Debug, Display, Formatter, Write},
    hash::Hash,
    mem::size_of,
};

mod partitioned_tree_aggregate;
mod tree_aggregate;
mod updater;

pub use partitioned_tree_aggregate::PartitionedRadixTreeReader;
use updater::radix_tree_update;

// We use constant radix to reduce the need to dynamically allocate a vector of
// child nodes.
const RADIX: usize = 16;

// Number of bits in `RADIX`.
const RADIX_BITS: u32 = RADIX.trailing_zeros();

/// Cursor over a radix tree.
///
/// A radix tree is a set of nodes indexed by each node's unique prefix.
/// Each key in the tree has exactly one value with non-zero weight
/// (specifically, 1).  The natural ordering of `Prefix`s guarantees that the
/// cursor iterates over the tree in pre-order (parent before children),
/// starting from the root node.
pub trait RadixTreeCursor<TS, A, R>: Cursor<Prefix<TS>, TreeNode<TS, A>, (), R>
where
    TS: PrimInt + Debug,
{
    /// Helper function: skip values with zero weights.
    fn skip_zero_weights(&mut self)
    where
        R: HasZero,
    {
        while self.val_valid() && self.weight().is_zero() {
            self.step_val();
        }
    }

    /// Computes aggregate over time range in a radix tree.
    ///
    /// Combine all aggregate values for timestamps in `range`
    /// using semigroup `S` by scanning the tree.
    ///
    /// # Preconditions
    ///
    /// Assumes `self` points to the root of the tree or the
    /// tree is empty and `self.key_valid()` is false.
    ///
    /// # Complexity
    ///
    /// This method visits `O(log(range.to - range.from))` nodes.
    fn aggregate_range<S>(&mut self, range: &Range<TS>) -> Option<A>
    where
        S: Semigroup<A>,
        A: Clone + Debug,
        R: HasZero,
    {
        // Discussion:
        // Starting from the root or the tree every time is
        // wasteful: in practice, this method is invoked for
        // ranges with monotonically increasing left bounds,
        // so we could cache the cursor to the start of the
        // previous range along with all nodes between the root
        // and the cursor, and resume the tree scan from that
        // location.  This requires cloning a bunch of state
        // and adds to complexity.

        // Empty tree.
        if !self.key_valid() {
            return None;
        }

        self.skip_zero_weights();
        if !self.val_valid() {
            return None;
        }
        let node = self.val().clone();
        self.aggregate_range_inner::<S>(&Prefix::full_range(), node, range)
    }

    // This is part of the internal implementation of `aggregate_range`, but private
    // methods in traits are a pain, so we just hide it from rustdoc.
    #[doc(hidden)]
    fn aggregate_range_inner<S>(
        &mut self,
        prefix: &Prefix<TS>,
        node: TreeNode<TS, A>,
        range: &Range<TS>,
    ) -> Option<A>
    where
        S: Semigroup<A>,
        A: Clone + Debug,
        R: HasZero,
    {
        let mut agg = None;

        // The first slot of `node` that overlaps with `range`.
        let start = if range.from < prefix.key {
            0
        } else if prefix.contains(range.from) {
            prefix.slot_of_timestamp(range.from)
        } else {
            RADIX
        };

        // How many slots in `node` overlap with `range`?
        let len = if prefix.contains(range.to) {
            prefix.slot_of_timestamp(range.to) + 1 - start
        } else if prefix.key < range.to {
            RADIX - start
        } else {
            0
        };

        // println!("aggregate_range_inner(prefix: {prefix}, node: {node}, range:
        // {range:x?}), start: {start}, len: {len}");

        // Iterate over slots in [start .. start + len), skipping `None`s
        // (using `flatten`).
        for child in node.children.into_iter().skip(start).take(len).flatten() {
            if child.child_prefix.in_range(range) {
                // The complete child tree is in `range` -- add its aggregate
                // value without descending down the subtree.
                // println!("in range, adding {:?}", child.child_agg);
                agg = Some(if let Some(agg) = agg {
                    S::combine(&agg, &child.child_agg)
                } else {
                    child.child_agg.clone()
                });
            } else if child.child_prefix.contains(range.from)
                || child.child_prefix.contains(range.to)
            {
                // Slot overlaps with range -- descend down the child tree.
                self.seek_key(&child.child_prefix);
                self.skip_zero_weights();
                debug_assert!(self.key_valid());
                debug_assert_eq!(self.key(), &child.child_prefix);

                let child_node = self.val().clone();
                agg = S::combine_opt(
                    &agg,
                    &self.aggregate_range_inner::<S>(&child.child_prefix, child_node, range),
                );
            }
        }

        agg
    }

    /// Produce a semi-human-readable representation of the tree for debugging
    /// purposes.
    fn format_tree<W>(&mut self, writer: &mut W) -> Result<(), fmt::Error>
    where
        TS: Debug,
        A: Debug,
        R: HasZero,
        W: Write,
    {
        while self.key_valid() {
            self.skip_zero_weights();
            if self.val_valid() {
                let indent = self.key().prefix_len as usize / RADIX_BITS as usize;
                writeln!(writer, "{:indent$}[{}] => {}", "", self.key(), self.val())?;
            }
            self.step_key();
        }

        Ok(())
    }

    /// Self-diagnostics: validate that `self` points to a well-formed
    /// radix-tree whose contents is equivalent to `contents`.
    fn validate<S>(&mut self, contents: &BTreeMap<TS, A>)
    where
        R: Eq + HasOne + HasZero + Debug,
        A: Eq + Clone + Debug,
        S: Semigroup<A>,
    {
        let mut contents = contents.clone();

        // Tracks prefixes we expect to encounter in the tree to
        // check there are no dangling child pointers.
        let mut expected_prefixes = BTreeSet::new();
        expected_prefixes.insert(Prefix::full_range());

        while self.key_valid() {
            self.skip_zero_weights();
            if self.val_valid() {
                // Tree should only contain nodes with unit weights.
                assert_eq!(self.weight(), HasOne::one());
                let node_prefix = self.key().clone();
                assert!(expected_prefixes.remove(&node_prefix));
                let node = self.val().clone();
                for (child_idx, child_ptr) in node
                    .children
                    .iter()
                    .enumerate()
                    .filter(|(_, ptr)| ptr.is_some())
                {
                    let child_ptr = child_ptr.as_ref().unwrap();

                    assert!(node_prefix.contains(child_ptr.child_prefix.key));
                    assert!(node_prefix.prefix_len < child_ptr.child_prefix.prefix_len);
                    // Child node is at the right index.
                    assert_eq!(
                        child_idx,
                        node_prefix.slot_of_timestamp(child_ptr.child_prefix.key)
                    );

                    if child_ptr.child_prefix.is_leaf() {
                        // Validate leaf: key must be part of `contents`.
                        let agg = contents.remove(&child_ptr.child_prefix.key).unwrap();
                        assert_eq!(&agg, &child_ptr.child_agg);
                    } else {
                        // Validate intermediate node value.
                        let mut accumulator: Option<A> = None;
                        for (_, key_agg) in contents
                            .iter()
                            .filter(|(&k, _)| child_ptr.child_prefix.contains(k))
                        {
                            match accumulator {
                                None => accumulator = Some(key_agg.clone()),
                                Some(x) => accumulator = Some(S::combine(&x, key_agg)),
                            }
                        }
                        let accumulator = accumulator.unwrap();
                        assert_eq!(&accumulator, &child_ptr.child_agg);
                        expected_prefixes.insert(child_ptr.child_prefix.clone());
                    }
                }

                // We expect at most one value for each key.
                self.step_val();
                self.skip_zero_weights();
                assert!(!self.val_valid());
            };
            self.step_key();
        }

        assert!(contents.is_empty());
        // An empty tree may not contain the top-level node.
        expected_prefixes.remove(&Prefix::full_range());
        assert!(expected_prefixes.is_empty());
    }
}

impl<TS, A, R, C> RadixTreeCursor<TS, A, R> for C
where
    TS: PrimInt + Debug,
    C: Cursor<Prefix<TS>, TreeNode<TS, A>, (), R>,
{
}

/// Describes a range of timestamps that share a common prefix.
#[derive(
    Clone,
    Debug,
    Default,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
pub struct Prefix<TS> {
    /// Prefix bits.
    key: TS,
    /// Prefix length.
    prefix_len: u32,
}

impl<TS> Display for Prefix<TS>
where
    TS: PrimInt + Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        if self.prefix_len == Self::KEY_BITS {
            write!(f, "{:x?}", self.key.to_u128().unwrap())
        } else if self.prefix_len == 0 {
            write!(f, "*")
        } else {
            write!(
                f,
                "{:x?}/{}",
                (self.key >> (Self::KEY_BITS - self.prefix_len) as usize)
                    .to_u128()
                    .unwrap(),
                self.prefix_len
            )
        }
    }
}

impl<TS> Prefix<TS>
where
    TS: PrimInt + Debug,
{
    const KEY_BITS: u32 = (size_of::<TS>() * 8) as u32;

    /// Creates a prefix of `prefix_len` with specified and
    #[cfg(test)]
    fn new(key: TS, prefix_len: u32) -> Self {
        Self { key, prefix_len }
    }

    /// Create a leaf prefix with all bits fixed.
    fn from_timestamp(key: TS) -> Self {
        Self {
            key,
            prefix_len: Self::KEY_BITS,
        }
    }

    /// `true` iff `self` is a leaf prefix, which covers a single fixed
    /// timestamp.
    fn is_leaf(&self) -> bool {
        self.prefix_len == Self::KEY_BITS
    }

    /// Create a prefix of length 0, which covers all possible timestamp values.
    fn full_range() -> Self {
        Self {
            key: TS::zero(),
            prefix_len: 0,
        }
    }

    /// The largest timestamps covered by `self`.
    fn upper(&self) -> TS {
        self.key | !Self::prefix_mask(self.prefix_len)
    }

    fn wildcard_bits(prefix_len: u32) -> usize {
        (Self::KEY_BITS - prefix_len) as usize
    }

    /// Returns bit mask with `prefix_len` higher-order bits set to `1`.
    fn prefix_mask(prefix_len: u32) -> TS {
        if prefix_len == 0 {
            TS::zero()
        } else {
            (TS::max_value() >> Self::wildcard_bits(prefix_len)) << Self::wildcard_bits(prefix_len)
        }
    }

    /// Computes the longest common prefix that covers both `self` and `key`.
    fn longest_common_prefix(&self, key: TS) -> Self {
        let longest_common_len = min((key ^ self.key).leading_zeros(), self.prefix_len);
        let prefix_len = longest_common_len - longest_common_len % RADIX_BITS;

        Self {
            key: key & Self::prefix_mask(prefix_len),
            prefix_len,
        }
    }

    /// `true` iff `self` contains `key`.
    fn contains(&self, key: TS) -> bool {
        //println!("contains prefix_mask: {:x?}", Self::prefix_mask(self.prefix_len));
        (self.key & Self::prefix_mask(self.prefix_len))
            == (key & Self::prefix_mask(self.prefix_len))
    }

    /// Child subtree of `self` that `key` belongs to.
    ///
    /// Precondition: `self` is not a leaf node, `self` contains `key`.
    fn slot_of_timestamp(&self, key: TS) -> usize {
        debug_assert!(self.prefix_len < Self::KEY_BITS);
        debug_assert!(self.contains(key));

        ((key >> Self::wildcard_bits(self.prefix_len + RADIX_BITS)) & TS::from(RADIX - 1).unwrap())
            .to_usize()
            .unwrap()
    }

    /// Child subtree of `self` that contains `other`.
    ///
    /// Precondition: `self` is a prefix of `other`.
    fn slot_of(&self, other: &Self) -> usize {
        debug_assert!(self.prefix_len < other.prefix_len);

        self.slot_of_timestamp(other.key)
    }

    /// Extends `self` with `RADIX_BITS` bits of `slot`.
    fn extend(&self, slot: usize) -> Self {
        debug_assert!(self.prefix_len < Self::KEY_BITS);
        debug_assert!(slot < RADIX);

        let prefix_len = self.prefix_len + RADIX_BITS;

        Self {
            key: self.key | (TS::from(slot).unwrap() << Self::wildcard_bits(prefix_len)),
            prefix_len,
        }
    }

    /// `true` iff `self` is completely contained within range.
    fn in_range(&self, range: &Range<TS>) -> bool {
        range.from <= self.key && range.to >= self.upper()
    }
}

/// Pointer to a child node.
#[derive(
    Clone, Debug, SizeOf, PartialEq, Eq, Hash, PartialOrd, Ord, Archive, Serialize, Deserialize,
)]
pub struct ChildPtr<TS, A> {
    /// Unique prefix of a child subtree, which serves as a pointer
    /// to the child node.  Given this prefix the child node can
    /// be located using `Cursor::seek_key`, unless
    /// `child_prefix.is_leaf()`, in which case we're at the bottom
    /// of the tree.
    child_prefix: Prefix<TS>,
    /// Aggregate over all timestamps covered by the child subtree.
    child_agg: A,
}

impl<TS, A> Display for ChildPtr<TS, A>
where
    TS: PrimInt + Debug,
    A: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[{}->{:?}]", self.child_prefix, self.child_agg)
    }
}

impl<TS, A> ChildPtr<TS, A>
where
    TS: PrimInt + Debug,
{
    fn from_timestamp(key: TS, child_agg: A) -> Self {
        Self {
            child_prefix: Prefix::from_timestamp(key),
            child_agg,
        }
    }

    fn new(child_prefix: Prefix<TS>, child_agg: A) -> Self {
        Self {
            child_prefix,
            child_agg,
        }
    }
}

/// Radix tree node.
#[derive(
    Clone,
    Debug,
    Default,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
pub struct TreeNode<TS, A> {
    /// Array of children.
    // `Option` doesn't introduce space overhead.
    children: [Option<ChildPtr<TS, A>>; RADIX],
}

impl<TS, A> Display for TreeNode<TS, A>
where
    TS: PrimInt + Debug,
    A: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        for child in self.children.iter() {
            match child {
                None => f.write_char('.')?,
                Some(child) => write!(f, "{child}")?,
            }
        }

        Ok(())
    }
}

impl<TS, A> TreeNode<TS, A>
where
    TS: Clone,
    A: Clone,
{
    fn new() -> Self {
        Self {
            children: Default::default(),
        }
    }

    /// Returns mutable reference to a child pointer number `slot`.
    fn slot_mut(&mut self, slot: usize) -> &mut Option<ChildPtr<TS, A>> {
        &mut self.children[slot]
    }

    /// Counts the number of non-empty slots.
    fn occupied_slots(&self) -> usize {
        let mut res = 0;
        for child in self.children.iter() {
            if child.is_some() {
                res += 1;
            }
        }
        res
    }

    /// Index of the first non-empty slot or `None` if all
    /// slots are empty.
    fn first_occupied_slot(&self) -> Option<ChildPtr<TS, A>> {
        for child in self.children.iter() {
            if child.is_some() {
                return child.clone();
            }
        }
        None
    }

    /// Computes aggregate of the entire subtree under `self` as a
    /// sum of aggregates of its children.
    fn aggregate<S>(&self) -> Option<A>
    where
        S: Semigroup<A>,
    {
        self.children.iter().flatten().fold(None, |acc, child| {
            acc.map(|acc| Some(S::combine(&acc, &child.child_agg)))
                .unwrap_or_else(|| Some(child.child_agg.clone()))
        })
    }
}

#[cfg(test)]
pub(super) mod test {
    use super::{ChildPtr, Prefix, RadixTreeCursor, TreeNode, RADIX_BITS};
    use crate::{
        algebra::{DefaultSemigroup, HasZero, Semigroup},
        operator::time_series::Range,
    };
    use num::PrimInt;
    use rkyv::{archived_root, to_bytes, Deserialize, Infallible};
    use std::{collections::BTreeMap, fmt::Debug, iter::once};

    // Checks that `aggregate_range` correctly computes aggregates for all
    // possible ranges.  Enumerates quadratic number of ranges.
    pub(super) fn test_aggregate_range<TS, A, R, C, S>(cursor: &mut C, contents: &BTreeMap<TS, A>)
    where
        C: RadixTreeCursor<TS, A, R>,
        TS: PrimInt + Debug,
        A: Clone + Eq + Debug,
        R: HasZero,
        S: Semigroup<A>,
    {
        let keys: Vec<TS> = once(TS::min_value())
            .chain(contents.keys().cloned())
            .chain(once(TS::max_value()))
            .collect();

        for (i, from) in keys.iter().enumerate() {
            for to in &keys[i..] {
                let expected_agg = contents.range(*from..=*to).fold(None, |acc, (_, v)| {
                    Some(if let Some(acc) = acc {
                        S::combine(&acc, v)
                    } else {
                        v.clone()
                    })
                });

                cursor.rewind_keys();
                let agg = cursor.aggregate_range::<S>(&Range::new(*from, *to));
                assert_eq!(
                    agg, expected_agg,
                    "Aggregating in range {:x?}..{:x?}",
                    *from, *to
                );
            }
        }
    }

    #[test]
    fn test_prefix() {
        type TestPrefix = Prefix<u64>;

        assert_eq!(TestPrefix::from_timestamp(0), Prefix::new(0, 64));
        assert_eq!(
            TestPrefix::from_timestamp(u64::MAX),
            Prefix::new(u64::MAX, 64)
        );
        assert_eq!(TestPrefix::full_range(), Prefix::new(0, 0));
        assert!(!TestPrefix::full_range().is_leaf());
        assert!(TestPrefix::from_timestamp(100).is_leaf());
        assert!(!Prefix::new(100, RADIX_BITS * 2).is_leaf());
        assert_eq!(TestPrefix::prefix_mask(0), 0);
        assert_eq!(TestPrefix::prefix_mask(4), 0xf000_0000_0000_0000);
        assert_eq!(TestPrefix::prefix_mask(32), 0xffff_ffff_0000_0000);
        assert_eq!(TestPrefix::prefix_mask(64), 0xffff_ffff_ffff_ffff);
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0xffff_ffff_ffff_ffff),
            Prefix::new(0xff00_0000_0000_0000u64, 8)
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0xf0ff_ffff_ffff_ffff),
            Prefix::new(0xf000_0000_0000_0000u64, 4)
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0xfcff_ffff_ffff_ffff),
            Prefix::new(0xf000_0000_0000_0000u64, 4)
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).longest_common_prefix(0x00ff_ffff_ffff_ffff),
            Prefix::new(0x0000_0000_0000_0000u64, 0)
        );
        assert_eq!(
            Prefix::new(0xffff_0000_ffff_0000u64, 64).longest_common_prefix(0xffff_0000_ffff_0000),
            Prefix::new(0xffff_0000_ffff_0000u64, 64)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).upper(),
            0xffff_ffff_ffff_ffffu64
        );
        assert_eq!(
            Prefix::new(0xff00_0000_0000_0000u64, 8).upper(),
            0xffff_ffff_ffff_ffffu64
        );
        assert_eq!(
            Prefix::new(0x1234_5678_0000_0000u64, 32).upper(),
            0x1234_5678_ffff_ffffu64
        );
        assert_eq!(
            Prefix::new(0x1234_5678_0000_1111u64, 64).upper(),
            0x1234_5678_0000_1111u64
        );

        assert!(Prefix::new(0xffff_0000_ffff_0000u64, 64).contains(0xffff_0000_ffff_0000));
        assert!(!Prefix::new(0xffff_0000_ffff_0000u64, 64).contains(0xffff_0000_ffff_00ff));
        assert!(Prefix::new(0xffff_ffff_0000_0000u64, 32).contains(0xffff_ffff_ffff_ffff));
        assert!(!Prefix::new(0xffff_ffff_0000_0000u64, 32).contains(0xffff_0000_ffff_ffff));
        assert!(Prefix::new(0x0000_0000_0000_0000u64, 0).contains(0xffff_ffff_ffff_ffff));
        assert!(Prefix::new(0x0000_0000_0000_0000u64, 0).contains(0x0000_0000_0000_0000));

        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0x0000_0000_0000_0001),
            0
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0x1000_0000_0000_0001),
            1
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0xa000_0000_0000_0001),
            0xa
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).slot_of_timestamp(0xf000_0000_0000_0001),
            0xf
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_0000_0001),
            0
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_1000_0001),
            1
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_a000_0001),
            0xa
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).slot_of_timestamp(0xffff_ffff_f000_0001),
            0xf
        );

        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0x0000_0000_0000_0000, 4)),
            0
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0x1000_0000_0000_0000, 8)),
            1
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0xa000_0000_0000_0000, 16)),
            0xa
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0)
                .slot_of(&Prefix::new(0xf000_0000_0000_0000, 32)),
            0xf
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_0000_0000, 36)),
            0
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_1000_0000, 40)),
            1
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_a000_0000, 44)),
            0xa
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32)
                .slot_of(&Prefix::new(0xffff_ffff_f000_0000, 64)),
            0xf
        );

        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(0),
            Prefix::new(0x0000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(1),
            Prefix::new(0x1000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(0xa),
            Prefix::new(0xa000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0x0000_0000_0000_0000u64, 0).extend(0xf),
            Prefix::new(0xf000_0000_0000_0000, 4)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(0),
            Prefix::new(0xffff_ffff_0000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(1),
            Prefix::new(0xffff_ffff_1000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(0xa),
            Prefix::new(0xffff_ffff_a000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 32).extend(0xf),
            Prefix::new(0xffff_ffff_f000_0000, 36)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(0),
            Prefix::new(0xffff_ffff_0000_0000, 64)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(1),
            Prefix::new(0xffff_ffff_0000_0001, 64)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(0xa),
            Prefix::new(0xffff_ffff_0000_000a, 64)
        );
        assert_eq!(
            Prefix::new(0xffff_ffff_0000_0000u64, 60).extend(0xf),
            Prefix::new(0xffff_ffff_0000_000f, 64)
        );
    }

    #[test]
    fn test_tree_node() {
        let mut node = TreeNode::new();
        assert_eq!(node.occupied_slots(), 0);
        assert_eq!(node.first_occupied_slot(), None);
        assert_eq!(node.aggregate::<DefaultSemigroup<_>>(), None);

        *node.slot_mut(1) = Some(ChildPtr::from_timestamp(0x1000_0000_0000_0000u64, 10));
        assert_eq!(node.occupied_slots(), 1);
        assert_eq!(
            node.first_occupied_slot(),
            Some(ChildPtr::from_timestamp(0x1000_0000_0000_0000u64, 10))
        );
        assert_eq!(node.aggregate::<DefaultSemigroup<_>>(), Some(10));

        *node.slot_mut(4) = Some(ChildPtr::from_timestamp(0x4000_0000_0000_0000u64, 40));
        assert_eq!(node.occupied_slots(), 2);
        assert_eq!(
            node.first_occupied_slot(),
            Some(ChildPtr::from_timestamp(0x1000_0000_0000_0000u64, 10))
        );
        assert_eq!(node.aggregate::<DefaultSemigroup<_>>(), Some(50));

        *node.slot_mut(8) = Some(ChildPtr::new(
            Prefix {
                key: 0x8fff_ffff_ffff_ffffu64,
                prefix_len: 4,
            },
            80,
        ));
        assert_eq!(node.occupied_slots(), 3);
        assert_eq!(
            node.first_occupied_slot(),
            Some(ChildPtr::from_timestamp(0x1000_0000_0000_0000u64, 10))
        );
        assert_eq!(node.aggregate::<DefaultSemigroup<_>>(), Some(130));
    }

    #[test]
    fn prefix_decode_encode() {
        type Type = Prefix<u64>;
        for input in [
            Prefix::new(0xffff_ffff_0000_0000u64, 32),
            Prefix::new(0x1234_5678_0000_1111u64, 64),
            Prefix::new(u64::MAX, 64),
        ] {
            let input: Type = input;
            let encoded = to_bytes::<_, 4096>(&input).unwrap();
            let archived = unsafe { archived_root::<Type>(&encoded[..]) };
            let decoded: Type = archived.deserialize(&mut Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn childptr_decode_encode() {
        type Type = ChildPtr<u64, i32>;
        for input in [
            ChildPtr::from_timestamp(u64::MIN, -1),
            ChildPtr::from_timestamp(0x1000_0000_0000_0000u64, 10),
            ChildPtr::from_timestamp(u64::MAX, 3),
        ] {
            let input: Type = input;
            let encoded = to_bytes::<_, 4096>(&input).unwrap();
            let archived = unsafe { archived_root::<Type>(&encoded[..]) };
            let decoded: Type = archived.deserialize(&mut Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn treenode_decode_encode() {
        type Type = TreeNode<u64, i32>;

        let mut input: Type = TreeNode::new();
        *input.slot_mut(1) = Some(ChildPtr::from_timestamp(0x1000_0000_0000_0000u64, 10));

        let encoded = to_bytes::<_, 4096>(&input).unwrap();
        let archived = unsafe { archived_root::<Type>(&encoded[..]) };
        let decoded: Type = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(decoded, input);
    }
}
