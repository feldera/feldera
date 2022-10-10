use super::{ChildPtr, Prefix, RadixTreeCursor, TreeNode, RADIX_BITS};
use crate::{
    algebra::{HasZero, MonoidValue, Semigroup},
    operator::Aggregator,
    trace::{cursor::CursorGroup, Cursor},
};
use num::PrimInt;
use std::{fmt::Debug, marker::PhantomData, mem::size_of};

/// Describes incremental update to a radix tree node.
#[derive(Debug)]
pub(super) struct TreeNodeUpdate<TS, A> {
    /// Prefix that uniquely identifies the node.
    pub prefix: Prefix<TS>,
    /// Old value of the node or `None` if we are creating a new node.
    pub old: Option<TreeNode<TS, A>>,
    /// New value of the node or `None` if we are deleting an existing node.
    pub new: Option<TreeNode<TS, A>>,
}

impl<TS, A> TreeNodeUpdate<TS, A>
where
    TS: Clone,
    A: Clone,
{
    fn from_existing_node(prefix: Prefix<TS>, node: TreeNode<TS, A>) -> Self {
        Self {
            prefix,
            old: Some(node.clone()),
            new: Some(node),
        }
    }

    fn from_new_node(prefix: Prefix<TS>, node: TreeNode<TS, A>) -> Self {
        Self {
            prefix,
            old: None,
            new: Some(node),
        }
    }
}

/// Tree updates stack frame.
///
/// `TreeUpdater` tracks its current position in the tree as a path from
/// the root node.  It pushes a new stack frame when moving down a branch.
#[derive(Clone)]
struct StackFrame {
    /// Index in `TreeUpdater.updates` vector.
    update_index: usize,
    /// Child slot within tree node.
    slot_index: usize,
}

impl StackFrame {
    fn new(update_index: usize, slot_index: usize) -> Self {
        Self {
            update_index,
            slot_index,
        }
    }
}

/// Tree updater object applies updates to monotonically increasing timestamps.
///
/// Tree updater is initialized with a trace cursor that stores the
/// radix tree.  It exposes an API (`update_timestamp()`) to modify
/// values associated with individual timestamps.
///
/// When done with updates, the client calls `finish` to produce a
/// sorted vector of `TreeNodeUpdate`s.  They can then convert these
/// updates into a `Batch` and append it to the tree trace to transform
/// the tree.
///
/// For efficiency, `TreeUpdater` requires that `update_timestamp` is invoked
/// for monotonically increasing timestamps, so that it only needs to scan
/// the underlying cursor once without backtracking.
struct TreeUpdater<'a, TS, A, R, S, TC>
where
    S: Semigroup<A>,
{
    /// Tree cursor.  `TreeUpdater` performs updates of monotonically
    /// increasing timestamps; therefore the cursor only moves forward.
    tree_cursor: TC,
    /// Tracks current location in the tree.
    stack: Vec<StackFrame>,
    /// Accumulated tree updates.
    updates: &'a mut Vec<TreeNodeUpdate<TS, A>>,
    phantom: PhantomData<(R, S)>,
}

impl<'a, 'b, TS, A, R, S, TC> TreeUpdater<'a, TS, A, R, S, TC>
where
    TS: PrimInt + Debug,
    A: Clone + Default + Debug,
    S: Semigroup<A>,
    TC: RadixTreeCursor<'b, TS, A, R>,
    R: HasZero,
{
    /// Create a new `TreeUpdater` backed by `tree_cursor`.
    ///
    /// # Arguments
    ///
    /// * `tree_cursor` must point to the root of the tree.
    /// * `updates` - empty vector of `TreeNodeUpdate`s.  After `finish` call,
    ///   this vector will contain sorted tree updates. We require the client to
    ///   pass the vector for allocation reuse.
    fn new(tree_cursor: TC, updates: &'a mut Vec<TreeNodeUpdate<TS, A>>) -> Self {
        debug_assert!(updates.is_empty());

        let mut res = Self {
            tree_cursor,
            stack: Vec::with_capacity((size_of::<TS>() * 8) / RADIX_BITS as usize),
            updates,
            phantom: PhantomData,
        };

        if res.tree_cursor.key_valid() {
            res.tree_cursor.skip_zero_weights();
            if res.tree_cursor.val_valid() {
                debug_assert_eq!(res.tree_cursor.key(), &Prefix::full_range());
                res.push_existing(Prefix::full_range(), res.tree_cursor.val().clone(), 0)
            } else {
                // No root node -- tree is empty.  Create new root.
                res.push_new(Prefix::full_range(), TreeNode::new(), 0);
            }
        } else {
            // No root node -- tree is empty.  Create new root.
            res.push_new(Prefix::full_range(), TreeNode::new(), 0);
        };
        res
    }

    fn finish(mut self) {
        while !self.stack.is_empty() {
            self.pop();
        }

        // TODO: This can be expensive.  Can we keep `updates` ordered so no sorting is
        // required?  Currently out-of-order updates are added when creating
        // intermediate tree nodes (`push_new`).  I can think of two tricks to
        // reduce the cost of sorting: (1) leave gaps in the updates vector when
        // descending down the tree, (2) experiment with sorting algorithms for
        // almost-sorted vectors, e.g., `dmsort`.
        self.updates
            .sort_unstable_by(|upd1, upd2| upd1.prefix.cmp(&upd2.prefix));
    }

    fn stack_top(&self) -> &StackFrame {
        self.stack.last().unwrap()
    }

    fn stack_top_mut(&mut self) -> &mut StackFrame {
        self.stack.last_mut().unwrap()
    }

    /// Range of times that belong to the current stack frame
    /// (not all of it may be covered by `child_prefix`).
    fn range(&self) -> Prefix<TS> {
        let StackFrame {
            update_index,
            slot_index,
        } = self.stack_top();
        self.updates[*update_index].prefix.extend(*slot_index)
    }

    /// Time range covered by the current tree node.
    fn node_prefix(&self) -> &Prefix<TS> {
        &self.updates[self.stack_top().update_index].prefix
    }

    fn node(&self) -> &TreeNode<TS, A> {
        let update_index = self.stack_top().update_index;

        self.updates[update_index].new.as_ref().unwrap()
    }

    /// Record deletion of the current tree node in update vector.
    fn remove_node(&mut self) {
        let update_index = self.stack_top().update_index;

        self.updates[update_index].new = None;
    }

    /// Immutable reference to the current slot of the current tree node.
    fn slot(&self) -> &Option<ChildPtr<TS, A>> {
        let StackFrame {
            update_index,
            slot_index,
        } = self.stack_top();

        &self.updates[*update_index].new.as_ref().unwrap().children[*slot_index]
    }

    /// Mutable reference to the current slot of the current tree node.
    fn slot_mut(&mut self) -> &mut Option<ChildPtr<TS, A>> {
        let StackFrame {
            update_index,
            slot_index,
        } = self.stack_top().clone();

        &mut self.updates[update_index].new.as_mut().unwrap().children[slot_index]
    }

    /// Prefix of the current slot.
    fn prefix(&self) -> Option<Prefix<TS>> {
        self.slot().as_ref().map(|ptr| ptr.child_prefix.clone())
    }

    /// Finalize updates to the current tree node and move up the tree.
    fn pop(&mut self) {
        //println!("pop: {:?}", self.node());
        let occupied_slots = self.node().occupied_slots();

        if occupied_slots == 0 {
            // Current node is empty -- clear parent slot.
            self.remove_node();
            self.stack.pop();
            if !self.stack.is_empty() {
                *self.slot_mut() = None;
            }
        } else if self.stack.len() == 1 {
            self.stack.pop();
        } else if occupied_slots == 1 && self.stack.len() > 1 {
            // Current node only has one child -- delete
            let child = self.node().first_occupied_slot();
            self.remove_node();
            self.stack.pop();
            *self.slot_mut() = child;
        } else {
            // Compute aggregate over the current tree node, update
            // parent node with it.
            let agg = self.node().aggregate::<S>().unwrap();
            self.stack.pop();
            self.slot_mut().as_mut().unwrap().child_agg = agg;
        }
    }

    /// Create a new child node of the current node, push it to the stack.
    fn push_new(&mut self, prefix: Prefix<TS>, node: TreeNode<TS, A>, slot: usize)
    where
        A: Debug,
    {
        //println!("push_new: {prefix:?} -> ({node:?}, {slot})");
        let frame = StackFrame::new(self.updates.len(), slot);
        self.updates
            .push(TreeNodeUpdate::from_new_node(prefix, node));
        self.stack.push(frame);
    }

    /// Move to an existing child of the current node.
    fn push_existing(&mut self, prefix: Prefix<TS>, node: TreeNode<TS, A>, slot: usize) {
        //println!("push_existing: {prefix:?} -> ({node:?}, {slot})");
        let frame = StackFrame::new(self.updates.len(), slot);
        self.updates
            .push(TreeNodeUpdate::from_existing_node(prefix, node));
        self.stack.push(frame);
    }

    /// Main `TreeUpdate` method: set or update aggregate value for timestamp
    /// `ts`.
    ///
    /// This method must be invoked with monotonically increasing timestamps.
    fn update_timestamp(&mut self, ts: TS, agg: Option<A>) {
        debug_assert!(ts >= self.range().key);

        // println!("update_timestamp({ts:x?})");
        loop {
            if self.range().contains(ts) {
                // println!("in range ({:?})", self.range());

                // Key belongs under the current stack frame.
                match self.prefix() {
                    None => {
                        // println!("create new leaf, agg = {:?}", agg);

                        // No subtree under the current stack frame -- create new leaf.
                        // (unless `agg.is_none()`, in which case there's nothing to do).
                        if let Some(agg) = agg {
                            *self.slot_mut() = Some(ChildPtr::from_timestamp(ts, agg));
                        };
                        return;
                    }
                    Some(prefix) => {
                        if prefix.contains(ts) {
                            if prefix.is_leaf() {
                                // We found `ts` -- update its value.
                                match agg {
                                    None => *self.slot_mut() = None,
                                    Some(agg) => {
                                        *self.slot_mut() = Some(ChildPtr::from_timestamp(ts, agg))
                                    }
                                }
                                return;
                            } else {
                                // There is already a subtree that covers `ts` -- continue descent
                                // to that subtree.
                                // TODO: jump straight to the right timestamp (we can calculate the
                                // exact offset)
                                self.tree_cursor.seek_key(&prefix);
                                debug_assert!(self.tree_cursor.key_valid());
                                debug_assert_eq!(self.tree_cursor.key(), &prefix);
                                self.tree_cursor.skip_zero_weights();
                                debug_assert!(self.tree_cursor.val_valid());
                                let slot = prefix.slot_of_timestamp(ts);
                                self.push_existing(prefix, self.tree_cursor.val().clone(), slot);
                            }
                        } else if agg.is_some() {
                            // A subtree exists, but it doesn't contain `ts` -- replace the subtree
                            // with a new node that is just big enough
                            // to cover `ts` and the old subtree.
                            let new_prefix = prefix.longest_common_prefix(ts);
                            let mut new_node = TreeNode::new();

                            // Append `ts` and the old subtree to `new_node`.
                            let slot = new_prefix.slot_of_timestamp(ts);
                            *new_node.slot_mut(slot) =
                                Some(ChildPtr::from_timestamp(ts, agg.unwrap()));
                            *new_node.slot_mut(new_prefix.slot_of(&prefix)) = self.slot().clone();
                            *self.slot_mut() =
                                Some(ChildPtr::new(new_prefix.clone(), Default::default()));
                            self.push_new(new_prefix, new_node, slot);
                            return;
                        } else {
                            return;
                        }
                    }
                }
            } else {
                // Key is outside the current stack frame.
                if self.node_prefix().contains(ts) {
                    // println!("in node range, moving to slot {}",
                    // self.node_prefix().slot_of_timestamp(ts));

                    // Key belongs to a different slot of the current node -- shift stack frame to
                    // that slot.
                    self.stack_top_mut().slot_index = self.node_prefix().slot_of_timestamp(ts);
                } else {
                    // println!("NOT in node range");

                    // Key is outside the range covered by the current slot -- pop the stack.
                    self.pop();
                }
            }
        }
    }
}

/// Core logic of [`tree_aggregate`](`crate::Stream::tree_aggregate`)
/// and [`partitioned_tree_aggregate`](`crate::Stream::partitioned_tree_aggregate`)
/// operators.
///
/// Given a batch of updates to time series data, computes updates to
/// its radix tree.
///
/// # Arguments
///
/// * `input_delta` - cursor over updates to the time series.  It is only used
///   to identify affected times.
/// * `input` - cursor over the entire contents of the input time series
///   (typically, this is a cursor over the trace of the time series).
/// * `tree` - cursor over the current contents of the radix tree.
/// * `aggregator` - aggregator to reduce time series data.
/// * `output_updates` - empty vector to accumulate tree updates in. When the
///   method returns `output_updates` contains ordered updates that can be used
///   to construct a batch of updates to apply to the tree.
pub(super) fn radix_tree_update<'a, 'b, TS, V, R, Agg, UC, IC, TC, OR>(
    mut input_delta: UC,
    mut input: IC,
    tree: TC,
    aggregator: &Agg,
    output_updates: &'a mut Vec<TreeNodeUpdate<TS, Agg::Output>>,
) where
    TS: PrimInt + Debug,
    R: MonoidValue,
    Agg: Aggregator<V, (), R>,
    Agg::Output: Clone + Default + Eq + Debug,
    UC: Cursor<'b, TS, V, (), R>,
    IC: Cursor<'b, TS, V, (), R>,
    TC: RadixTreeCursor<'b, TS, Agg::Output, OR>,
    OR: MonoidValue,
{
    let mut tree_updater =
        <TreeUpdater<'a, TS, Agg::Output, OR, Agg::Semigroup, TC>>::new(tree, output_updates);

    while input_delta.key_valid() {
        //println!("affected key {:x?}", input_delta.key());

        // Compute new value of aggregate for `input_delta.key()`.
        input.seek_key(input_delta.key());

        let agg = if input.key_valid() && input.key() == input_delta.key() {
            aggregator.aggregate(&mut CursorGroup::new(&mut input, ()))
        } else {
            None
        };

        tree_updater.update_timestamp(*input_delta.key(), agg);

        input_delta.step_key();
    }

    // Pop the stack to generate final updates.
    tree_updater.finish();
    //println!("UPDATES: {:#?}", updates);
}
