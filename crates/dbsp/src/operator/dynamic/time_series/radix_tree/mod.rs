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
    algebra::{HasOne, ZCursor},
    dynamic::{DataTrait, DynOpt, Weight},
    operator::dynamic::{aggregate::AggCombineFunc, time_series::Range},
    DBData, ZWeight,
};
use dyn_clone::clone_box;
use num::PrimInt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fmt::Write,
    ops::Deref,
};

mod partitioned_tree_aggregate;
mod prefix;
mod tree_aggregate;
mod treenode;
mod updater;

use crate::dynamic::{ClonableTrait, DynVec, Erase, Factory, LeanVec, WithFactory};
pub use partitioned_tree_aggregate::{
    FilePartitionedRadixTreeFactories, OrdPartitionedTreeAggregateFactories,
    PartitionedRadixTreeReader,
};
pub use prefix::{DynPrefix, Prefix};
pub use treenode::{
    ChildPtr, DynChildPtr, DynTreeNode, DynTreeNodeUpdate, TreeNode, TreeNodeUpdate,
};
use updater::radix_tree_update;

// We use constant radix to reduce the need to dynamically allocate a vector of
// child nodes.
const RADIX: usize = 16;

// Number of bits in `RADIX`.
const RADIX_BITS: u32 = RADIX.trailing_zeros();

pub struct RadixTreeFactories<TS: 'static, A: DataTrait + ?Sized> {
    opt_aggregate_factory: &'static dyn Factory<DynOpt<A>>,
    child_ptr_factory: &'static dyn Factory<DynChildPtr<TS, A>>,
    node_factory: &'static dyn Factory<DynTreeNode<TS, A>>,
    node_update_factory: &'static dyn Factory<DynTreeNodeUpdate<TS, A>>,
    node_updates_factory: &'static dyn Factory<DynVec<DynTreeNodeUpdate<TS, A>>>,
}

impl<TS, A> RadixTreeFactories<TS, A>
where
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
{
    pub fn new<AType>() -> Self
    where
        AType: DBData + Erase<A>,
    {
        Self {
            opt_aggregate_factory: WithFactory::<Option<AType>>::FACTORY,
            child_ptr_factory: WithFactory::<ChildPtr<TS, AType>>::FACTORY,
            node_factory: WithFactory::<TreeNode<TS, AType>>::FACTORY,
            node_update_factory: WithFactory::<TreeNodeUpdate<TS, AType>>::FACTORY,
            node_updates_factory: WithFactory::<LeanVec<TreeNodeUpdate<TS, AType>>>::FACTORY,
        }
    }
}

impl<TS: 'static, A: DataTrait + ?Sized> Clone for RadixTreeFactories<TS, A> {
    fn clone(&self) -> Self {
        Self {
            opt_aggregate_factory: self.opt_aggregate_factory,
            child_ptr_factory: self.child_ptr_factory,
            node_factory: self.node_factory,
            node_update_factory: self.node_update_factory,
            node_updates_factory: self.node_updates_factory,
        }
    }
}

/// Cursor over a radix tree.
///
/// A radix tree is a set of nodes indexed by each node's unique prefix.
/// Each key in the tree has exactly one value with non-zero weight
/// (specifically, 1).  The natural ordering of `Prefix`s guarantees that the
/// cursor iterates over the tree in pre-order (parent before children),
/// starting from the root node.
pub trait RadixTreeCursor<TS, A>: ZCursor<DynPrefix<TS>, DynTreeNode<TS, A>, ()>
where
    A: DataTrait + ?Sized,
    TS: PrimInt + DBData,
{
    /// Helper function: skip values with zero weights.
    fn skip_zero_weights(&mut self) {
        while self.val_valid() && Weight::is_zero(self.weight()) {
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
    fn aggregate_range(
        &mut self,
        range: &Range<TS>,
        combine: &dyn AggCombineFunc<A>,
        result: &mut DynOpt<A>,
    ) {
        result.set_none();

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
            return;
        }

        self.skip_zero_weights();
        if !self.val_valid() {
            return;
        }
        let node = clone_box(self.val());
        self.aggregate_range_inner(&Prefix::full_range(), node.deref(), range, combine, result)
    }

    // This is part of the internal implementation of `aggregate_range`, but private
    // methods in traits are a pain, so we just hide it from rustdoc.
    #[doc(hidden)]
    fn aggregate_range_inner(
        &mut self,
        prefix: &Prefix<TS>,
        node: &DynTreeNode<TS, A>,
        range: &Range<TS>,
        combine: &dyn AggCombineFunc<A>,
        agg: &mut DynOpt<A>,
    ) {
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

        // Create a box to store child nodes during iteration.
        let mut child_node = clone_box(node);

        // Iterate over slots in [start .. start + len), skipping `None`s
        // (using `flatten`).
        for idx in start..start + len {
            if let Some(child) = node.slot(idx).get() {
                let child_prefix = child.child_prefix();

                if child_prefix.in_range(range) {
                    // The complete child tree is in `range` -- add its aggregate
                    // value without descending down the subtree.
                    // println!("in range, adding {:?}", child.child_agg);
                    if let Some(agg) = agg.get_mut() {
                        combine(agg, child.child_agg());
                    } else {
                        agg.from_ref(child.child_agg());
                    };
                } else if child_prefix.contains(range.from) || child_prefix.contains(range.to) {
                    // Slot overlaps with range -- descend down the child tree.
                    self.seek_key(child_prefix.erase());
                    self.skip_zero_weights();
                    debug_assert!(self.key_valid());
                    debug_assert_eq!(**self.key(), child_prefix);

                    self.val().clone_to(&mut *child_node);
                    self.aggregate_range_inner(&child_prefix, &*child_node, range, combine, agg);
                }
            }
        }
    }

    /// Produce a semi-human-readable representation of the tree for debugging
    /// purposes.
    fn format_tree<W>(&mut self, writer: &mut W) -> Result<(), fmt::Error>
    where
        W: Write,
    {
        while self.key_valid() {
            self.skip_zero_weights();
            if self.val_valid() {
                let indent = self.key().prefix_len as usize / RADIX_BITS as usize;
                writeln!(
                    writer,
                    "{:indent$}[{}] => {}",
                    "",
                    self.key().deref(),
                    self.val()
                )?;
            }
            self.step_key();
        }

        Ok(())
    }

    /// Self-diagnostics: validate that `self` points to a well-formed
    /// radix-tree whose contents is equivalent to `contents`.
    fn validate(&mut self, contents: &BTreeMap<TS, Box<A>>, combine: &dyn Fn(&mut A, &A)) {
        let mut contents_clone = BTreeMap::new();

        for (k, v) in contents.iter() {
            contents_clone.insert(*k, clone_box(v.as_ref()));
        }

        let mut contents = contents_clone;

        // Tracks prefixes we expect to encounter in the tree to
        // check there are no dangling child pointers.
        let mut expected_prefixes = BTreeSet::new();
        expected_prefixes.insert(Prefix::full_range());

        while self.key_valid() {
            self.skip_zero_weights();
            if self.val_valid() {
                // Tree should only contain nodes with unit weights.
                assert_eq!(**self.weight(), ZWeight::one());
                let node_prefix = self.key();
                assert!(expected_prefixes.remove(node_prefix));
                let node = clone_box(self.val());
                for child_idx in 0..RADIX {
                    if let Some(child_ptr) = node.slot(child_idx).get() {
                        assert!(node_prefix.contains(child_ptr.child_prefix().key));
                        assert!(node_prefix.prefix_len < child_ptr.child_prefix().prefix_len);
                        // Child node is at the right index.
                        assert_eq!(
                            child_idx,
                            node_prefix.slot_of_timestamp(child_ptr.child_prefix().key)
                        );

                        if child_ptr.child_prefix().is_leaf() {
                            // Validate leaf: key must be part of `contents`.
                            let agg = contents.remove(&child_ptr.child_prefix().key).unwrap();
                            assert_eq!(&*agg, child_ptr.child_agg());
                        } else {
                            // Validate intermediate node value.
                            let mut accumulator: Option<Box<A>> = None;
                            for (_, key_agg) in contents
                                .iter()
                                .filter(|(&k, _)| child_ptr.child_prefix().contains(k))
                            {
                                match &mut accumulator {
                                    None => accumulator = Some(clone_box(key_agg)),
                                    Some(x) => combine(x, key_agg),
                                }
                            }
                            let accumulator = accumulator.unwrap();
                            assert_eq!(&*accumulator, child_ptr.child_agg());
                            expected_prefixes.insert(child_ptr.child_prefix().clone());
                        }
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

impl<TS, A, C> RadixTreeCursor<TS, A> for C
where
    A: DataTrait + ?Sized,
    TS: PrimInt + DBData,
    C: ZCursor<DynPrefix<TS>, DynTreeNode<TS, A>, ()>,
{
}

#[cfg(test)]
pub(in crate::operator) mod test {

    use super::RadixTreeCursor;
    use crate::{
        algebra::Semigroup,
        dynamic::{DowncastTrait, DynData, Erase},
        operator::dynamic::time_series::Range,
        DBData,
    };
    use num::PrimInt;
    use std::{collections::BTreeMap, iter::once};

    // Checks that `aggregate_range` correctly computes aggregates for all
    // possible ranges.  Enumerates quadratic number of ranges.
    pub(in crate::operator) fn test_aggregate_range<TS, A, C, S>(
        cursor: &mut C,
        contents: &BTreeMap<TS, Box<DynData /* <A> */>>,
    ) where
        C: RadixTreeCursor<TS, DynData /* <A> */>,
        TS: PrimInt + DBData,
        A: DBData,
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
                        S::combine(&acc, v.downcast_checked::<A>())
                    } else {
                        v.downcast_checked::<A>().clone()
                    })
                });

                cursor.rewind_keys();
                let mut agg = None;
                cursor.aggregate_range(
                    &Range::new(*from, *to),
                    &|acc, val| {
                        *acc.downcast_mut_checked::<A>() = S::combine(
                            (acc as &DynData).downcast_checked::<A>(),
                            val.downcast_checked::<A>(),
                        )
                    },
                    agg.erase_mut(),
                );
                assert_eq!(
                    agg, expected_agg,
                    "Aggregating in range {:x?}..{:x?}",
                    *from, *to
                );
            }
        }
    }
}
