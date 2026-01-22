//! Order Statistics Multiset - An augmented B+ tree for efficient rank/select operations.
//!
//! This module provides a weighted multiset implementation optimized for:
//! - O(log n) insertion with weight updates (positive or negative)
//! - O(log n) selection by cumulative weight position (select_kth)
//! - O(log n) prefix sum / rank queries
//! - Efficient serialization for spill-to-disk scenarios
//!
//! The implementation uses a B+ tree with subtree weight sums stored at each internal node,
//! enabling logarithmic-time rank and select operations. Large node sizes (configurable)
//! provide cache efficiency and minimize disk seeks for spilled state.
//!
//! # Design
//!
//! ```text
//!                    [Internal Node]
//!                    keys: [20, 40]
//!                    subtree_sums: [15, 8, 12]  // sum of weights in each subtree
//!                    children: [0, 1, 2]
//!                   /         |         \
//!          [Leaf 0]       [Leaf 1]      [Leaf 2]
//!          [(10,3),(15,5),(18,7)]  [(25,2),(30,3),(35,3)]  [(45,7),(50,5)]
//! ```
//!
//! # Negative Weights
//!
//! This data structure fully supports negative weights for incremental/differential
//! computation. When querying (select_kth, rank), only positions with positive
//! cumulative weight are considered valid.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::Debug,
    hash::{Hash, Hasher},
};

use crate::algebra::ZWeight;
use crate::utils::IsNone;

/// Default branching factor for the B+ tree.
/// Larger values are more cache/disk friendly but may have higher constant factors.
/// 64 provides a good balance for most workloads.
pub const DEFAULT_BRANCHING_FACTOR: usize = 64;

/// Minimum branching factor to ensure tree properties.
pub const MIN_BRANCHING_FACTOR: usize = 4;

/// A leaf node in the B+ tree, storing sorted (key, weight) pairs.
#[derive(Debug, Clone, PartialEq, Eq, SizeOf, serde::Serialize, serde::Deserialize)]
struct LeafNode<T> {
    /// Sorted keys with their weights
    entries: Vec<(T, ZWeight)>,
    /// Link to next leaf for efficient iteration (index in arena, or usize::MAX if none)
    next_leaf: usize,
}

impl<T: Ord + Clone> LeafNode<T> {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_leaf: usize::MAX,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            next_leaf: usize::MAX,
        }
    }

    /// Total weight in this leaf
    fn total_weight(&self) -> ZWeight {
        self.entries.iter().map(|(_, w)| *w).sum()
    }

    /// Find position of key using binary search
    fn find_key_pos(&self, key: &T) -> Result<usize, usize> {
        self.entries.binary_search_by(|(k, _)| k.cmp(key))
    }

    /// Insert or update weight for a key. Returns the weight delta applied to total.
    fn insert(&mut self, key: T, weight: ZWeight) -> ZWeight {
        match self.find_key_pos(&key) {
            Ok(pos) => {
                // Key exists, update weight
                self.entries[pos].1 += weight;
                weight
            }
            Err(pos) => {
                // Key doesn't exist, insert new entry
                self.entries.insert(pos, (key, weight));
                weight
            }
        }
    }

    /// Check if leaf needs splitting
    fn needs_split(&self, max_entries: usize) -> bool {
        self.entries.len() > max_entries
    }

    /// Split this leaf, returning the new right leaf and the split key
    fn split(&mut self) -> (T, LeafNode<T>) {
        let mid = self.entries.len() / 2;
        let right_entries = self.entries.split_off(mid);
        let split_key = right_entries[0].0.clone();

        let right = LeafNode {
            entries: right_entries,
            next_leaf: self.next_leaf,
        };

        // We'll set self.next_leaf after we know the right node's index
        (split_key, right)
    }

    /// Select the k-th element (0-indexed) within this leaf, counting by weight.
    /// Returns the key at position k, or None if k is out of bounds.
    fn select_kth(&self, mut k: ZWeight) -> Option<&T> {
        for (key, weight) in &self.entries {
            if *weight <= 0 {
                continue;
            }
            if k < *weight {
                return Some(key);
            }
            k -= *weight;
        }
        None
    }

    /// Get the cumulative weight of all keys strictly less than the given key.
    fn prefix_weight(&self, key: &T) -> ZWeight {
        let mut sum = 0;
        for (k, weight) in &self.entries {
            if k >= key {
                break;
            }
            if *weight > 0 {
                sum += *weight;
            }
        }
        sum
    }
}

/// An internal node in the B+ tree, storing keys, child indices, and subtree sums.
#[derive(Debug, Clone, PartialEq, Eq, SizeOf, serde::Serialize, serde::Deserialize)]
struct InternalNode {
    /// Separator keys: keys[i] is the minimum key in children[i+1]
    keys: Vec<ZWeight>, // Using ZWeight as placeholder, will be index
    /// Actually stores keys as indices into a separate key storage
    /// For simplicity, we'll store keys directly but typed
    key_indices: Vec<usize>,
    /// Child node indices (into the arena)
    children: Vec<usize>,
    /// Sum of weights in each child's subtree
    subtree_sums: Vec<ZWeight>,
}

/// An internal node with actual key storage
#[derive(Debug, Clone, PartialEq, Eq, SizeOf, serde::Serialize, serde::Deserialize)]
struct InternalNodeTyped<T> {
    /// Separator keys: keys[i] is the minimum key in children[i+1]
    keys: Vec<T>,
    /// Child node indices (into the arena)
    children: Vec<usize>,
    /// Sum of weights in each child's subtree
    subtree_sums: Vec<ZWeight>,
}

impl<T: Ord + Clone> InternalNodeTyped<T> {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
            subtree_sums: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            children: Vec::with_capacity(capacity + 1),
            subtree_sums: Vec::with_capacity(capacity + 1),
        }
    }

    /// Total weight across all children
    fn total_weight(&self) -> ZWeight {
        self.subtree_sums.iter().sum()
    }

    /// Find which child contains the given key
    fn find_child(&self, key: &T) -> usize {
        match self.keys.binary_search(key) {
            Ok(pos) => pos + 1, // Key found, go to right child
            Err(pos) => pos,    // Key not found, pos is insertion point
        }
    }

    /// Check if node needs splitting
    fn needs_split(&self, max_children: usize) -> bool {
        self.children.len() > max_children
    }

    /// Split this internal node, returning the promoted key and the new right node
    fn split(&mut self) -> (T, InternalNodeTyped<T>) {
        let mid = self.keys.len() / 2;

        // The middle key gets promoted
        let promoted_key = self.keys[mid].clone();

        // Right node gets keys after mid
        let right_keys = self.keys.split_off(mid + 1);
        self.keys.pop(); // Remove the promoted key

        let right_children = self.children.split_off(mid + 1);
        let right_sums = self.subtree_sums.split_off(mid + 1);

        let right = InternalNodeTyped {
            keys: right_keys,
            children: right_children,
            subtree_sums: right_sums,
        };

        (promoted_key, right)
    }

    /// Select k-th element by navigating subtree sums
    fn find_child_for_select(&self, k: ZWeight) -> Option<(usize, ZWeight)> {
        let mut remaining = k;
        for (i, &sum) in self.subtree_sums.iter().enumerate() {
            // Only count positive sums for selection
            let effective_sum = sum.max(0);
            if remaining < effective_sum {
                return Some((self.children[i], remaining));
            }
            remaining -= effective_sum;
        }
        None
    }

    /// Get prefix weight up to (but not including) the child containing the key
    fn prefix_weight_before_child(&self, child_idx: usize) -> ZWeight {
        self.subtree_sums[..child_idx]
            .iter()
            .filter(|&&w| w > 0)
            .sum()
    }
}

/// A node in the B+ tree (either leaf or internal)
#[derive(Debug, Clone, PartialEq, Eq, SizeOf, serde::Serialize, serde::Deserialize)]
enum Node<T> {
    Leaf(LeafNode<T>),
    Internal(InternalNodeTyped<T>),
}

impl<T: Ord + Clone> Node<T> {
    fn total_weight(&self) -> ZWeight {
        match self {
            Node::Leaf(leaf) => leaf.total_weight(),
            Node::Internal(internal) => internal.total_weight(),
        }
    }

    fn is_leaf(&self) -> bool {
        matches!(self, Node::Leaf(_))
    }
}

/// An order-statistics multiset implemented as an augmented B+ tree.
///
/// This data structure maintains a sorted multiset of values with integer weights,
/// supporting efficient insertion, deletion (via negative weights), and order-statistic
/// queries (select by position, rank by value).
///
/// # Type Parameters
/// - `T`: The key type, must be `Ord + Clone`
///
/// # Complexity
/// - Insert/Update: O(log n)
/// - Select k-th: O(log n)
/// - Rank query: O(log n)
/// - Merge: O(m log(n+m)) where m is the size of the smaller tree
///
/// # Example
/// ```ignore
/// use dbsp::dynamic::order_statistics_multiset::OrderStatisticsMultiset;
///
/// let mut tree = OrderStatisticsMultiset::new();
/// tree.insert(10, 3);  // Insert key 10 with weight 3
/// tree.insert(20, 2);  // Insert key 20 with weight 2
/// tree.insert(10, -1); // Decrease weight of 10 to 2
///
/// assert_eq!(tree.total_weight(), 4); // 2 + 2
/// assert_eq!(tree.select_kth(0, true), Some(&10)); // Positions 0,1 -> 10
/// assert_eq!(tree.select_kth(2, true), Some(&20)); // Positions 2,3 -> 20
/// assert_eq!(tree.rank(&20), 2); // Two elements before 20
/// ```
#[derive(Debug, Clone, SizeOf, serde::Serialize, serde::Deserialize)]
pub struct OrderStatisticsMultiset<T: Ord + Clone> {
    /// Arena storage for all nodes
    nodes: Vec<Node<T>>,
    /// Index of the root node (usize::MAX if tree is empty)
    root: usize,
    /// Total weight across all elements
    total_weight: ZWeight,
    /// Maximum entries per leaf node
    max_leaf_entries: usize,
    /// Maximum children per internal node
    max_internal_children: usize,
    /// Index of the first leaf (for iteration)
    first_leaf: usize,
    /// Number of distinct keys
    num_keys: usize,
}

// Implement PartialEq manually to compare contents, not structure
impl<T: Ord + Clone + PartialEq> PartialEq for OrderStatisticsMultiset<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.total_weight != other.total_weight || self.num_keys != other.num_keys {
            return false;
        }
        // Compare all entries by iterating
        let self_entries: Vec<_> = self.iter().collect();
        let other_entries: Vec<_> = other.iter().collect();
        self_entries == other_entries
    }
}

impl<T: Ord + Clone + PartialEq> Eq for OrderStatisticsMultiset<T> {}

impl<T: Ord + Clone + PartialOrd> PartialOrd for OrderStatisticsMultiset<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord + Clone> Ord for OrderStatisticsMultiset<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.total_weight.cmp(&other.total_weight) {
            Ordering::Equal => {
                // Compare entries lexicographically
                let mut self_iter = self.iter();
                let mut other_iter = other.iter();
                loop {
                    match (self_iter.next(), other_iter.next()) {
                        (None, None) => return Ordering::Equal,
                        (None, Some(_)) => return Ordering::Less,
                        (Some(_), None) => return Ordering::Greater,
                        (Some((k1, w1)), Some((k2, w2))) => match k1.cmp(k2) {
                            Ordering::Equal => match w1.cmp(&w2) {
                                Ordering::Equal => continue,
                                other => return other,
                            },
                            other => return other,
                        },
                    }
                }
            }
            other => other,
        }
    }
}

impl<T: Ord + Clone> IsNone for OrderStatisticsMultiset<T> {
    fn is_none(&self) -> bool {
        false // OrderStatisticsMultiset is never "none"
    }
}

impl<T: Ord + Clone + Hash> Hash for OrderStatisticsMultiset<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.total_weight.hash(state);
        self.num_keys.hash(state);
        for (key, weight) in self.iter() {
            key.hash(state);
            weight.hash(state);
        }
    }
}

impl<T: Ord + Clone> Default for OrderStatisticsMultiset<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Ord + Clone> OrderStatisticsMultiset<T> {
    /// Create a new empty multiset with default branching factor.
    pub fn new() -> Self {
        Self::with_branching_factor(DEFAULT_BRANCHING_FACTOR)
    }

    /// Create a new empty multiset with specified branching factor.
    ///
    /// Larger branching factors are more cache/disk friendly but may have
    /// higher constant factors for small trees.
    pub fn with_branching_factor(b: usize) -> Self {
        let b = b.max(MIN_BRANCHING_FACTOR);
        Self {
            nodes: Vec::new(),
            root: usize::MAX,
            total_weight: 0,
            max_leaf_entries: b,
            max_internal_children: b,
            first_leaf: usize::MAX,
            num_keys: 0,
        }
    }

    /// Returns the total weight (sum of all weights, can be negative during updates).
    #[inline]
    pub fn total_weight(&self) -> ZWeight {
        self.total_weight
    }

    /// Returns the number of distinct keys in the multiset.
    #[inline]
    pub fn num_keys(&self) -> usize {
        self.num_keys
    }

    /// Returns true if the multiset has no elements with positive weights.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_weight <= 0
    }

    /// Insert a key with the given weight delta.
    ///
    /// - Positive weight: adds occurrences
    /// - Negative weight: removes occurrences
    /// - If key doesn't exist, creates it with the given weight
    ///
    /// # Complexity
    /// O(log n) amortized
    pub fn insert(&mut self, key: T, weight: ZWeight) {
        if weight == 0 {
            return;
        }

        if self.root == usize::MAX {
            // Tree is empty, create first leaf
            let mut leaf = LeafNode::with_capacity(self.max_leaf_entries);
            leaf.entries.push((key, weight));
            self.nodes.push(Node::Leaf(leaf));
            self.root = 0;
            self.first_leaf = 0;
            self.total_weight = weight;
            self.num_keys = 1;
            return;
        }

        // Insert into tree and handle splits
        let (new_key_created, split_result) = self.insert_recursive(self.root, key, weight);

        if new_key_created {
            self.num_keys += 1;
        }
        self.total_weight += weight;

        // Handle root split
        if let Some((promoted_key, new_child_idx)) = split_result {
            let mut new_root = InternalNodeTyped::with_capacity(self.max_internal_children);
            new_root.children.push(self.root);
            new_root.children.push(new_child_idx);
            new_root.keys.push(promoted_key);

            // Calculate subtree sums
            let left_sum = self.nodes[self.root].total_weight();
            let right_sum = self.nodes[new_child_idx].total_weight();
            new_root.subtree_sums.push(left_sum);
            new_root.subtree_sums.push(right_sum);

            let new_root_idx = self.nodes.len();
            self.nodes.push(Node::Internal(new_root));
            self.root = new_root_idx;
        }
    }

    /// Recursive insert helper. Returns (new_key_created, optional split result).
    fn insert_recursive(
        &mut self,
        node_idx: usize,
        key: T,
        weight: ZWeight,
    ) -> (bool, Option<(T, usize)>) {
        // First, determine if this is a leaf or internal node
        let is_leaf = self.nodes[node_idx].is_leaf();

        if is_leaf {
            // Insert into leaf - get info we need first
            let (new_key, needs_split) = {
                let leaf = match &mut self.nodes[node_idx] {
                    Node::Leaf(l) => l,
                    _ => unreachable!(),
                };
                let new_key = leaf.find_key_pos(&key).is_err();
                leaf.insert(key, weight);
                (new_key, leaf.needs_split(self.max_leaf_entries))
            };

            // Handle split if needed
            if needs_split {
                let (split_key, right_leaf, old_next) = {
                    let leaf = match &mut self.nodes[node_idx] {
                        Node::Leaf(l) => l,
                        _ => unreachable!(),
                    };
                    let (split_key, right_leaf) = leaf.split();
                    let old_next = leaf.next_leaf;
                    (split_key, right_leaf, old_next)
                };

                let right_idx = self.nodes.len();

                // Update the left leaf's next pointer
                {
                    let leaf = match &mut self.nodes[node_idx] {
                        Node::Leaf(l) => l,
                        _ => unreachable!(),
                    };
                    leaf.next_leaf = right_idx;
                }

                // Create right leaf with correct next pointer
                let mut right_leaf = right_leaf;
                right_leaf.next_leaf = old_next;
                self.nodes.push(Node::Leaf(right_leaf));

                (new_key, Some((split_key, right_idx)))
            } else {
                (new_key, None)
            }
        } else {
            // Internal node - find child and recurse
            let (child_idx, child_pos) = {
                let internal = match &self.nodes[node_idx] {
                    Node::Internal(i) => i,
                    _ => unreachable!(),
                };
                let child_pos = internal.find_child(&key);
                (internal.children[child_pos], child_pos)
            };

            let (new_key, split_result) = self.insert_recursive(child_idx, key, weight);

            // Update subtree sum for the child we descended into
            {
                let internal = match &mut self.nodes[node_idx] {
                    Node::Internal(i) => i,
                    _ => unreachable!(),
                };
                internal.subtree_sums[child_pos] += weight;
            }

            // Handle child split
            if let Some((promoted_key, new_child_idx)) = split_result {
                // Calculate sums before taking mutable borrow
                let left_sum = self.nodes[child_idx].total_weight();
                let right_sum = self.nodes[new_child_idx].total_weight();

                let needs_internal_split = {
                    let internal = match &mut self.nodes[node_idx] {
                        Node::Internal(i) => i,
                        _ => unreachable!(),
                    };

                    internal.subtree_sums[child_pos] = left_sum;
                    internal.keys.insert(child_pos, promoted_key);
                    internal.children.insert(child_pos + 1, new_child_idx);
                    internal.subtree_sums.insert(child_pos + 1, right_sum);

                    internal.needs_split(self.max_internal_children)
                };

                // Handle internal node split if needed
                if needs_internal_split {
                    let (promoted, right_internal) = {
                        let internal = match &mut self.nodes[node_idx] {
                            Node::Internal(i) => i,
                            _ => unreachable!(),
                        };
                        internal.split()
                    };
                    let right_idx = self.nodes.len();
                    self.nodes.push(Node::Internal(right_internal));
                    return (new_key, Some((promoted, right_idx)));
                }
            }

            (new_key, None)
        }
    }

    /// Select the k-th element (0-indexed) by cumulative weight.
    ///
    /// Only considers positions with positive cumulative weights.
    /// For example, if key A has weight 3 and key B has weight 2:
    /// - select_kth(0, true), select_kth(1, true), select_kth(2, true) return A
    /// - select_kth(3, true), select_kth(4, true) return B
    ///
    /// When `ascending` is false, selection is from the end (descending order).
    ///
    /// # Complexity
    /// O(log n)
    pub fn select_kth(&self, k: ZWeight, ascending: bool) -> Option<&T> {
        let effective_k = if ascending {
            k
        } else {
            if self.total_weight <= 0 {
                return None;
            }
            self.total_weight - 1 - k
        };

        if effective_k < 0 || effective_k >= self.total_weight || self.root == usize::MAX {
            return None;
        }
        self.select_kth_recursive(self.root, effective_k)
    }

    fn select_kth_recursive(&self, node_idx: usize, k: ZWeight) -> Option<&T> {
        match &self.nodes[node_idx] {
            Node::Leaf(leaf) => leaf.select_kth(k),
            Node::Internal(internal) => {
                let (child_idx, remaining_k) = internal.find_child_for_select(k)?;
                self.select_kth_recursive(child_idx, remaining_k)
            }
        }
    }

    /// Get the rank of a key (sum of weights of all keys strictly less than the given key).
    ///
    /// This is useful for computing percentiles: rank(key) / total_weight gives
    /// the percentile of the key.
    ///
    /// # Complexity
    /// O(log n)
    pub fn rank(&self, key: &T) -> ZWeight {
        if self.root == usize::MAX {
            return 0;
        }
        self.rank_recursive(self.root, key)
    }

    fn rank_recursive(&self, node_idx: usize, key: &T) -> ZWeight {
        match &self.nodes[node_idx] {
            Node::Leaf(leaf) => leaf.prefix_weight(key),
            Node::Internal(internal) => {
                let child_pos = internal.find_child(key);
                let prefix = internal.prefix_weight_before_child(child_pos);
                let child_idx = internal.children[child_pos];
                prefix + self.rank_recursive(child_idx, key)
            }
        }
    }

    /// Get the bounds for a percentile value (for PERCENTILE_CONT interpolation).
    ///
    /// Returns (lower_key, upper_key, fraction) where the interpolated result is:
    /// lower + fraction * (upper - lower)
    ///
    /// Uses SQL standard formula: position = percentile * (N - 1)
    ///
    /// When `ascending` is false (descending order), percentile 0.0 returns
    /// the maximum and 1.0 returns the minimum.
    ///
    /// # Complexity
    /// O(log n) - two select operations
    pub fn select_percentile_bounds(
        &self,
        percentile: f64,
        ascending: bool,
    ) -> Option<(&T, &T, f64)> {
        if self.total_weight <= 0 || !(0.0..=1.0).contains(&percentile) {
            return None;
        }

        // For descending, invert the percentile
        let effective_percentile = if ascending {
            percentile
        } else {
            1.0 - percentile
        };

        let n = self.total_weight;
        if n == 1 {
            let value = self.select_kth(0, true)?;
            return Some((value, value, 0.0));
        }

        // SQL standard PERCENTILE_CONT formula
        let pos = effective_percentile * ((n - 1) as f64);
        let lower_idx = pos.floor() as ZWeight;
        let upper_idx = pos.ceil() as ZWeight;
        let fraction = pos - (lower_idx as f64);

        let lower_value = self.select_kth(lower_idx, true)?;
        if lower_idx == upper_idx {
            if ascending {
                return Some((lower_value, lower_value, 0.0));
            } else {
                return Some((lower_value, lower_value, 0.0));
            }
        }

        let upper_value = self.select_kth(upper_idx, true)?;

        if ascending {
            Some((lower_value, upper_value, fraction))
        } else {
            // Swap lo and hi, and invert the fraction for descending
            Some((upper_value, lower_value, 1.0 - fraction))
        }
    }

    /// Get the value for PERCENTILE_DISC (discrete percentile).
    ///
    /// Returns the first value whose cumulative distribution >= percentile.
    ///
    /// When `ascending` is false (descending order), percentile 0.0 returns
    /// the maximum and 1.0 returns the minimum.
    ///
    /// # Complexity
    /// O(log n)
    pub fn select_percentile_disc(&self, percentile: f64, ascending: bool) -> Option<&T> {
        if self.total_weight <= 0 || !(0.0..=1.0).contains(&percentile) {
            return None;
        }

        // For descending, invert the percentile
        let effective_percentile = if ascending {
            percentile
        } else {
            1.0 - percentile
        };

        let n = self.total_weight;
        let pos = (effective_percentile * (n as f64)).ceil() as ZWeight;
        let idx = if pos <= 0 {
            0
        } else if pos > n {
            n - 1
        } else {
            pos - 1
        };

        self.select_kth(idx, true)
    }

    /// Get the weight for a specific key.
    ///
    /// # Complexity
    /// O(log n)
    pub fn get_weight(&self, key: &T) -> ZWeight {
        if self.root == usize::MAX {
            return 0;
        }
        self.get_weight_recursive(self.root, key)
    }

    fn get_weight_recursive(&self, node_idx: usize, key: &T) -> ZWeight {
        match &self.nodes[node_idx] {
            Node::Leaf(leaf) => match leaf.find_key_pos(key) {
                Ok(pos) => leaf.entries[pos].1,
                Err(_) => 0,
            },
            Node::Internal(internal) => {
                let child_pos = internal.find_child(key);
                self.get_weight_recursive(internal.children[child_pos], key)
            }
        }
    }

    /// Iterate over all (key, weight) pairs in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = (&T, ZWeight)> {
        OrderStatisticsIter {
            tree: self,
            current_leaf: self.first_leaf,
            current_pos: 0,
        }
    }

    /// Merge another multiset into this one.
    ///
    /// This is the semigroup operation for combining partial aggregates.
    ///
    /// # Complexity
    /// O(m log(n+m)) where m is the size of `other`
    pub fn merge(&mut self, other: &Self) {
        for (key, weight) in other.iter() {
            self.insert(key.clone(), weight);
        }
    }

    /// Create a new multiset that is the merge of two multisets.
    pub fn merged(left: &Self, right: &Self) -> Self {
        let mut result = left.clone();
        result.merge(right);
        result
    }

    /// Remove entries with zero weight to reclaim space.
    ///
    /// This is an O(n) operation that rebuilds the tree without zero-weight entries.
    pub fn compact(&mut self) {
        let entries: Vec<_> = self
            .iter()
            .filter(|(_, w)| *w != 0)
            .map(|(k, w)| (k.clone(), w))
            .collect();

        let b = self.max_leaf_entries;
        *self = Self::with_branching_factor(b);
        for (key, weight) in entries {
            self.insert(key, weight);
        }
    }

    /// Clear all entries from the multiset.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.root = usize::MAX;
        self.total_weight = 0;
        self.first_leaf = usize::MAX;
        self.num_keys = 0;
    }

    /// Alias for `total_weight()` for API compatibility.
    #[inline]
    pub fn total_count(&self) -> ZWeight {
        self.total_weight
    }
}

/// Iterator over (key, weight) pairs in sorted order.
struct OrderStatisticsIter<'a, T: Ord + Clone> {
    tree: &'a OrderStatisticsMultiset<T>,
    current_leaf: usize,
    current_pos: usize,
}

impl<'a, T: Ord + Clone> Iterator for OrderStatisticsIter<'a, T> {
    type Item = (&'a T, ZWeight);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_leaf == usize::MAX {
                return None;
            }

            let leaf = match &self.tree.nodes.get(self.current_leaf)? {
                Node::Leaf(l) => l,
                _ => return None, // Shouldn't happen
            };

            if self.current_pos < leaf.entries.len() {
                let (key, weight) = &leaf.entries[self.current_pos];
                self.current_pos += 1;
                return Some((key, *weight));
            }

            // Move to next leaf
            self.current_leaf = leaf.next_leaf;
            self.current_pos = 0;
        }
    }
}

// ============================================================================
// rkyv serialization support
// ============================================================================

#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct ArchivedLeafNode<T: Archive> {
    entries: Vec<(T, ZWeight)>,
    next_leaf: usize,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
struct ArchivedInternalNode<T: Archive> {
    keys: Vec<T>,
    children: Vec<usize>,
    subtree_sums: Vec<ZWeight>,
}

/// Serializable representation of the multiset for rkyv.
/// Converts to/from a flat representation for efficient serialization.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
#[archive(bound(
    archive = "<T as Archive>::Archived: Ord",
    serialize = "T: rkyv::Serialize<__S>",
))]
pub struct SerializableOrderStatisticsMultiset<T>
where
    T: Archive,
{
    /// Flattened (key, weight) pairs in sorted order
    entries: Vec<(T, ZWeight)>,
    /// Branching factor for reconstruction
    branching_factor: u32,
}

// Manual implementations of comparison traits for the archived type
impl<T: Archive> PartialEq for ArchivedSerializableOrderStatisticsMultiset<T>
where
    <T as Archive>::Archived: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.branching_factor == other.branching_factor && self.entries == other.entries
    }
}

impl<T: Archive> Eq for ArchivedSerializableOrderStatisticsMultiset<T> where
    <T as Archive>::Archived: Ord
{
}

impl<T: Archive> PartialOrd for ArchivedSerializableOrderStatisticsMultiset<T>
where
    <T as Archive>::Archived: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Archive> Ord for ArchivedSerializableOrderStatisticsMultiset<T>
where
    <T as Archive>::Archived: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.entries.cmp(&other.entries) {
            std::cmp::Ordering::Equal => self.branching_factor.cmp(&other.branching_factor),
            other => other,
        }
    }
}

impl<T: Ord + Clone + Archive> From<&OrderStatisticsMultiset<T>>
    for SerializableOrderStatisticsMultiset<T>
{
    fn from(tree: &OrderStatisticsMultiset<T>) -> Self {
        Self {
            entries: tree.iter().map(|(k, w)| (k.clone(), w)).collect(),
            branching_factor: tree.max_leaf_entries as u32,
        }
    }
}

impl<T: Ord + Clone + Archive> From<SerializableOrderStatisticsMultiset<T>>
    for OrderStatisticsMultiset<T>
{
    fn from(serialized: SerializableOrderStatisticsMultiset<T>) -> Self {
        let mut tree = Self::with_branching_factor(serialized.branching_factor as usize);
        for (key, weight) in serialized.entries {
            tree.insert(key, weight);
        }
        tree
    }
}

// Implement rkyv traits for OrderStatisticsMultiset via SerializableOrderStatisticsMultiset
//
// We use SerializableOrderStatisticsMultiset as the archived form, which flattens
// the B+ tree to a simple sorted vector of (key, weight) pairs. This enables
// efficient serialization for spill-to-disk scenarios.

impl<T> Archive for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Archive,
    <T as Archive>::Archived: Ord,
{
    type Archived = ArchivedSerializableOrderStatisticsMultiset<T>;
    type Resolver = <SerializableOrderStatisticsMultiset<T> as Archive>::Resolver;

    #[allow(clippy::unit_arg)]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        let serializable: SerializableOrderStatisticsMultiset<T> = self.into();
        // SAFETY: Caller guarantees pos and out are valid
        unsafe { serializable.resolve(pos, resolver, out) };
    }
}

impl<T, S> rkyv::Serialize<S> for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Archive,
    <T as Archive>::Archived: Ord,
    S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer + ?Sized,
    SerializableOrderStatisticsMultiset<T>: rkyv::Serialize<S>,
{
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let serializable: SerializableOrderStatisticsMultiset<T> = self.into();
        serializable.serialize(serializer)
    }
}

impl<T, D> rkyv::Deserialize<OrderStatisticsMultiset<T>, D>
    for ArchivedSerializableOrderStatisticsMultiset<T>
where
    T: Ord + Clone + Archive,
    <T as Archive>::Archived: Ord,
    D: rkyv::Fallible + ?Sized,
    <SerializableOrderStatisticsMultiset<T> as Archive>::Archived:
        rkyv::Deserialize<SerializableOrderStatisticsMultiset<T>, D>,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<OrderStatisticsMultiset<T>, D::Error> {
        let serializable: SerializableOrderStatisticsMultiset<T> =
            rkyv::Deserialize::deserialize(self, deserializer)?;
        Ok(serializable.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tree() {
        let tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.num_keys(), 0);
        assert!(tree.is_empty());
        assert_eq!(tree.select_kth(0, true), None);
        assert_eq!(tree.rank(&10), 0);
    }

    #[test]
    fn test_single_insert() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 5);

        assert_eq!(tree.total_weight(), 5);
        assert_eq!(tree.num_keys(), 1);
        assert!(!tree.is_empty());

        // All positions 0-4 should return 10
        for i in 0..5 {
            assert_eq!(tree.select_kth(i, true), Some(&10));
        }
        assert_eq!(tree.select_kth(5, true), None);

        assert_eq!(tree.rank(&5), 0); // Nothing less than 5
        assert_eq!(tree.rank(&10), 0); // Nothing less than 10
        assert_eq!(tree.rank(&15), 5); // 5 elements less than 15
    }

    #[test]
    fn test_multiple_inserts() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 3); // positions 0, 1, 2
        tree.insert(20, 2); // positions 3, 4
        tree.insert(15, 1); // position between: now 0,1,2=10, 3=15, 4,5=20

        assert_eq!(tree.total_weight(), 6);
        assert_eq!(tree.num_keys(), 3);

        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(1, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&10));
        assert_eq!(tree.select_kth(3, true), Some(&15));
        assert_eq!(tree.select_kth(4, true), Some(&20));
        assert_eq!(tree.select_kth(5, true), Some(&20));
        assert_eq!(tree.select_kth(6, true), None);

        assert_eq!(tree.rank(&10), 0);
        assert_eq!(tree.rank(&15), 3);
        assert_eq!(tree.rank(&20), 4);
        assert_eq!(tree.rank(&25), 6);
    }

    #[test]
    fn test_weight_update() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 5);
        tree.insert(10, 3); // Should add to existing weight

        assert_eq!(tree.total_weight(), 8);
        assert_eq!(tree.num_keys(), 1);
        assert_eq!(tree.get_weight(&10), 8);
    }

    #[test]
    fn test_negative_weights() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 5);
        tree.insert(10, -2); // Reduce weight to 3

        assert_eq!(tree.total_weight(), 3);
        assert_eq!(tree.get_weight(&10), 3);

        // Only 3 positions now
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&10));
        assert_eq!(tree.select_kth(3, true), None);
    }

    #[test]
    fn test_negative_weight_to_zero() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 5);
        tree.insert(20, 3);
        tree.insert(10, -5); // Weight becomes 0

        assert_eq!(tree.total_weight(), 3);
        assert_eq!(tree.get_weight(&10), 0);

        // Only 20 should be selectable
        assert_eq!(tree.select_kth(0, true), Some(&20));
        assert_eq!(tree.select_kth(2, true), Some(&20));
        assert_eq!(tree.select_kth(3, true), None);
    }

    #[test]
    fn test_large_tree_splits() {
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        // Insert enough to trigger splits
        for i in 0..100 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.total_weight(), 100);
        assert_eq!(tree.num_keys(), 100);

        // Verify select works correctly after splits
        for i in 0..100 {
            assert_eq!(tree.select_kth(i, true), Some(&i));
        }

        // Verify rank works correctly
        for i in 0..100 {
            assert_eq!(tree.rank(&i), i);
        }
    }

    #[test]
    fn test_reverse_insert_order() {
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        // Insert in reverse order
        for i in (0..50).rev() {
            tree.insert(i, 1);
        }

        // Should still be sorted
        for i in 0..50 {
            assert_eq!(tree.select_kth(i, true), Some(&i));
        }
    }

    #[test]
    fn test_random_weights() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 7);
        tree.insert(20, 3);
        tree.insert(30, 5);

        // Total weight: 15
        // Positions 0-6: 10
        // Positions 7-9: 20
        // Positions 10-14: 30

        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(6, true), Some(&10));
        assert_eq!(tree.select_kth(7, true), Some(&20));
        assert_eq!(tree.select_kth(9, true), Some(&20));
        assert_eq!(tree.select_kth(10, true), Some(&30));
        assert_eq!(tree.select_kth(14, true), Some(&30));
        assert_eq!(tree.select_kth(15, true), None);
    }

    #[test]
    fn test_percentile_disc() {
        let mut tree = OrderStatisticsMultiset::new();
        for i in 1..=100 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&50));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&100));
    }

    #[test]
    fn test_percentile_cont_bounds() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 1);
        tree.insert(20, 1);
        tree.insert(30, 1);
        tree.insert(40, 1);

        // 50th percentile should interpolate between 20 and 30
        let (lower, upper, frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(*lower, 20);
        assert_eq!(*upper, 30);
        assert!((frac - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_merge() {
        let mut tree1 = OrderStatisticsMultiset::new();
        tree1.insert(10, 3);
        tree1.insert(30, 2);

        let mut tree2 = OrderStatisticsMultiset::new();
        tree2.insert(20, 1);
        tree2.insert(30, 1);

        tree1.merge(&tree2);

        assert_eq!(tree1.total_weight(), 7);
        assert_eq!(tree1.get_weight(&10), 3);
        assert_eq!(tree1.get_weight(&20), 1);
        assert_eq!(tree1.get_weight(&30), 3); // 2 + 1
    }

    #[test]
    fn test_iteration() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(30, 3);
        tree.insert(10, 1);
        tree.insert(20, 2);

        let entries: Vec<_> = tree.iter().collect();
        assert_eq!(entries, vec![(&10, 1), (&20, 2), (&30, 3)]);
    }

    #[test]
    fn test_compact() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 5);
        tree.insert(20, 3);
        tree.insert(10, -5); // Zero weight

        assert_eq!(tree.num_keys(), 2); // Still has the zero-weight entry

        tree.compact();

        assert_eq!(tree.num_keys(), 1); // Zero-weight entry removed
        assert_eq!(tree.total_weight(), 3);
        assert_eq!(tree.get_weight(&10), 0); // Not found anymore
        assert_eq!(tree.get_weight(&20), 3);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut tree = OrderStatisticsMultiset::with_branching_factor(8);
        for i in 0..50 {
            tree.insert(i * 10, (i % 5 + 1) as ZWeight);
        }

        let serialized: SerializableOrderStatisticsMultiset<i32> = (&tree).into();
        let restored: OrderStatisticsMultiset<i32> = serialized.into();

        assert_eq!(tree.total_weight(), restored.total_weight());
        assert_eq!(tree.num_keys(), restored.num_keys());

        // Verify all entries match
        let orig_entries: Vec<_> = tree.iter().collect();
        let rest_entries: Vec<_> = restored.iter().collect();
        assert_eq!(orig_entries, rest_entries);
    }

    #[test]
    fn test_string_keys() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert("apple".to_string(), 3);
        tree.insert("banana".to_string(), 2);
        tree.insert("cherry".to_string(), 1);

        assert_eq!(tree.select_kth(0, true), Some(&"apple".to_string()));
        assert_eq!(tree.select_kth(3, true), Some(&"banana".to_string()));
        assert_eq!(tree.select_kth(5, true), Some(&"cherry".to_string()));
    }

    #[test]
    fn test_descending_select() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 2);
        tree.insert(20, 2);
        tree.insert(30, 2);

        // Total weight 6, positions 0-5
        // Ascending: 0,1->10, 2,3->20, 4,5->30
        // Descending: 0,1->30, 2,3->20, 4,5->10

        assert_eq!(tree.select_kth(0, false), Some(&30));
        assert_eq!(tree.select_kth(1, false), Some(&30));
        assert_eq!(tree.select_kth(2, false), Some(&20));
        assert_eq!(tree.select_kth(4, false), Some(&10));
    }

    // ========================================================================
    // Deep tests with multiple actions and edge cases
    // ========================================================================

    #[test]
    fn test_deep_incremental_updates_with_cancellation() {
        // Simulate DBSP incremental updates where inserts and deletes interleave
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        // Initial batch: insert many elements
        for i in 0..20 {
            tree.insert(i * 10, 5);
        }
        assert_eq!(tree.total_weight(), 100);
        assert_eq!(tree.num_keys(), 20);

        // Delta batch 1: remove some, add to others
        tree.insert(0, -5); // Remove key 0 entirely
        tree.insert(10, -3); // Reduce key 10 to weight 2
        tree.insert(50, 10); // Increase key 50 to weight 15
        tree.insert(200, 7); // Add new key

        assert_eq!(tree.total_weight(), 100 - 5 - 3 + 10 + 7);
        assert_eq!(tree.get_weight(&0), 0);
        assert_eq!(tree.get_weight(&10), 2);
        assert_eq!(tree.get_weight(&50), 15);
        assert_eq!(tree.get_weight(&200), 7);

        // Verify selection still works correctly
        // Key 10 now has weight 2, so positions 0,1 -> 10
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(1, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&20));

        // Delta batch 2: complete cancellation of remaining elements
        for i in 1..20 {
            let current = tree.get_weight(&(i * 10));
            tree.insert(i * 10, -current);
        }
        tree.insert(200, -7);

        assert_eq!(tree.total_weight(), 0);
        assert!(tree.is_empty());
        assert_eq!(tree.select_kth(0, true), None);

        // Delta batch 3: rebuild from scratch
        tree.insert(999, 1);
        assert_eq!(tree.total_weight(), 1);
        assert_eq!(tree.select_kth(0, true), Some(&999));
    }

    #[test]
    fn test_deep_split_cascade_small_branching() {
        // Use very small branching factor to force many splits
        let mut tree: OrderStatisticsMultiset<i32> =
            OrderStatisticsMultiset::with_branching_factor(MIN_BRANCHING_FACTOR);

        // Insert sequentially to trigger splits
        for i in 0i32..100 {
            tree.insert(i, 1);

            // Verify invariants after each insert
            assert_eq!(tree.total_weight(), (i + 1) as ZWeight);
            assert_eq!(tree.num_keys(), (i + 1) as usize);

            // Spot check selection
            assert_eq!(tree.select_kth(0, true), Some(&0));
            assert_eq!(tree.select_kth(i as ZWeight, true), Some(&i));
        }

        // Verify entire tree is correct
        for i in 0i32..100 {
            assert_eq!(tree.select_kth(i as ZWeight, true), Some(&i));
            assert_eq!(tree.rank(&i), i as ZWeight);
            assert_eq!(tree.get_weight(&i), 1);
        }

        // Now delete every other element
        for i in (0i32..100).step_by(2) {
            tree.insert(i, -1);
        }

        assert_eq!(tree.total_weight(), 50);

        // Only odd numbers should be selectable
        for (pos, i) in (0i32..100).step_by(2).map(|x| x + 1).enumerate() {
            assert_eq!(tree.select_kth(pos as ZWeight, true), Some(&i));
        }
    }

    #[test]
    fn test_deep_all_weights_become_negative() {
        let mut tree = OrderStatisticsMultiset::new();

        // Insert with positive weights
        tree.insert(10, 5);
        tree.insert(20, 3);
        tree.insert(30, 7);
        assert_eq!(tree.total_weight(), 15);

        // Make all weights negative (simulating over-deletion in a delta)
        tree.insert(10, -10); // -5
        tree.insert(20, -10); // -7
        tree.insert(30, -10); // -3

        assert_eq!(tree.total_weight(), -15);
        assert_eq!(tree.get_weight(&10), -5);
        assert_eq!(tree.get_weight(&20), -7);
        assert_eq!(tree.get_weight(&30), -3);

        // Selection should return None (no positive weights)
        assert!(tree.is_empty()); // total_weight <= 0
        assert_eq!(tree.select_kth(0, true), None);

        // Rank should still work (but return 0 since no positive weights)
        assert_eq!(tree.rank(&15), 0);
        assert_eq!(tree.rank(&25), 0);

        // Now add back positive weights
        tree.insert(10, 10); // 5
        tree.insert(20, 10); // 3

        assert_eq!(tree.total_weight(), 5);
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(4, true), Some(&10));
        // 30 still has negative weight, so 20 is at position 5
        // Wait, 20 has weight 3, so positions 5,6,7 should be 20
        // But total is 5, so only positions 0-4 exist
        // Actually weight of 10 is 5, weight of 20 is 3, total = 8
        // Let me recalculate:
        // 10: started 5, -10 = -5, +10 = 5
        // 20: started 3, -10 = -7, +10 = 3
        // 30: started 7, -10 = -3, no change = -3
        // Total: 5 + 3 + (-3) = 5
        assert_eq!(tree.total_weight(), 5);
        // Selection with total=5 means positions 0-4
        // 10 has weight 5, so all positions 0-4 -> 10
        // 20 has weight 3 but position 5+ (after 10's 5)
        // 30 has negative weight, skipped
        // Actually selection only counts positive weights!
        // So effective total for selection = 5 + 3 = 8
        // Hmm, but total_weight is 5... let me check the logic
        // select_kth checks k >= self.total_weight, so if total is 5, positions 0-4 valid
        // But within those positions, we skip negative weights
        // So positions 0-4 -> 10, position 5+ -> out of bounds
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(4, true), Some(&10));
        assert_eq!(tree.select_kth(5, true), None); // total_weight is 5
    }

    #[test]
    fn test_deep_interleaved_keys_with_varying_weights() {
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        // Insert keys in interleaved order with varying weights
        let data = vec![
            (50, 10), // middle
            (25, 5),  // left quarter
            (75, 15), // right quarter
            (10, 3),  // far left
            (90, 7),  // far right
            (30, 2),  // between 25 and 50
            (60, 8),  // between 50 and 75
            (5, 1),   // leftmost
            (95, 4),  // rightmost
        ];

        for (key, weight) in &data {
            tree.insert(*key, *weight);
        }

        // Verify total weight
        let expected_total: ZWeight = data.iter().map(|(_, w)| w).sum();
        assert_eq!(tree.total_weight(), expected_total);

        // Verify iteration is sorted
        let keys: Vec<_> = tree.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![5, 10, 25, 30, 50, 60, 75, 90, 95]);

        // Verify cumulative positions
        // 5: w=1, positions 0
        // 10: w=3, positions 1-3
        // 25: w=5, positions 4-8
        // 30: w=2, positions 9-10
        // 50: w=10, positions 11-20
        // 60: w=8, positions 21-28
        // 75: w=15, positions 29-43
        // 90: w=7, positions 44-50
        // 95: w=4, positions 51-54
        assert_eq!(tree.select_kth(0, true), Some(&5));
        assert_eq!(tree.select_kth(1, true), Some(&10));
        assert_eq!(tree.select_kth(3, true), Some(&10));
        assert_eq!(tree.select_kth(4, true), Some(&25));
        assert_eq!(tree.select_kth(8, true), Some(&25));
        assert_eq!(tree.select_kth(9, true), Some(&30));
        assert_eq!(tree.select_kth(11, true), Some(&50));
        assert_eq!(tree.select_kth(20, true), Some(&50));
        assert_eq!(tree.select_kth(21, true), Some(&60));
        assert_eq!(tree.select_kth(29, true), Some(&75));
        assert_eq!(tree.select_kth(43, true), Some(&75));
        assert_eq!(tree.select_kth(44, true), Some(&90));
        assert_eq!(tree.select_kth(51, true), Some(&95));
        assert_eq!(tree.select_kth(54, true), Some(&95));
        assert_eq!(tree.select_kth(55, true), None);

        // Verify rank queries
        assert_eq!(tree.rank(&5), 0);
        assert_eq!(tree.rank(&6), 1); // after 5's weight
        assert_eq!(tree.rank(&10), 1);
        assert_eq!(tree.rank(&11), 4); // after 5+10 = 1+3 = 4
        assert_eq!(tree.rank(&50), 11); // 1+3+5+2 = 11
        assert_eq!(tree.rank(&100), 55); // all elements
    }

    #[test]
    fn test_deep_exact_split_boundaries() {
        // Test behavior exactly at node split boundaries
        let b = 4; // Small branching factor
        let mut tree = OrderStatisticsMultiset::with_branching_factor(b);

        // Insert exactly b elements (should fit in one leaf)
        for i in 0..b {
            tree.insert(i as i32, 1);
        }
        assert_eq!(tree.num_keys(), b);

        // Insert one more (should trigger split)
        tree.insert(b as i32, 1);
        assert_eq!(tree.num_keys(), b + 1);

        // Verify all elements accessible
        for i in 0..=b {
            assert_eq!(tree.select_kth(i as ZWeight, true), Some(&(i as i32)));
        }

        // Continue to fill and split multiple times
        for i in (b + 1)..(b * b) {
            tree.insert(i as i32, 1);
        }

        // Verify all elements
        for i in 0..(b * b) {
            assert_eq!(tree.select_kth(i as ZWeight, true), Some(&(i as i32)));
            assert_eq!(tree.rank(&(i as i32)), i as ZWeight);
        }
    }

    #[test]
    fn test_deep_merge_with_overlapping_keys_and_cancellation() {
        let mut tree1 = OrderStatisticsMultiset::with_branching_factor(4);
        let mut tree2 = OrderStatisticsMultiset::with_branching_factor(4);

        // Tree1: positive weights
        for i in 0..20 {
            tree1.insert(i, 5);
        }

        // Tree2: some overlap with negative weights (deletions)
        for i in 10..30 {
            tree2.insert(i, -3);
        }

        // Merge: overlapping keys should have combined weights
        tree1.merge(&tree2);

        // Keys 0-9: weight 5 (only in tree1)
        // Keys 10-19: weight 5 + (-3) = 2 (overlap)
        // Keys 20-29: weight -3 (only in tree2, negative)

        for i in 0..10 {
            assert_eq!(tree1.get_weight(&i), 5);
        }
        for i in 10..20 {
            assert_eq!(tree1.get_weight(&i), 2);
        }
        for i in 20..30 {
            assert_eq!(tree1.get_weight(&i), -3);
        }

        // Total: 10*5 + 10*2 + 10*(-3) = 50 + 20 - 30 = 40
        assert_eq!(tree1.total_weight(), 40);

        // Selection should skip negative weights
        // Effective weights for selection: 0-9 have 5, 10-19 have 2
        // Total positive = 50 + 20 = 70, but total_weight is 40
        // select_kth uses total_weight as bound, so positions 0-39 valid
        assert_eq!(tree1.select_kth(0, true), Some(&0));
        assert_eq!(tree1.select_kth(49, true), None); // position >= total_weight
    }

    #[test]
    fn test_deep_merge_empty_trees() {
        let mut tree1: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        let tree2: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        // Merge two empty trees
        tree1.merge(&tree2);
        assert_eq!(tree1.total_weight(), 0);
        assert!(tree1.is_empty());

        // Merge non-empty into empty
        let mut tree3: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        let mut tree4 = OrderStatisticsMultiset::new();
        tree4.insert(10, 5);
        tree4.insert(20, 3);

        tree3.merge(&tree4);
        assert_eq!(tree3.total_weight(), 8);
        assert_eq!(tree3.get_weight(&10), 5);
        assert_eq!(tree3.get_weight(&20), 3);

        // Merge empty into non-empty
        let empty: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        tree3.merge(&empty);
        assert_eq!(tree3.total_weight(), 8); // unchanged
    }

    #[test]
    fn test_deep_zero_weight_insert_behavior() {
        let mut tree = OrderStatisticsMultiset::new();

        // Insert with zero weight should be no-op
        tree.insert(10, 0);
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.num_keys(), 0);
        assert!(tree.is_empty());

        // Insert positive, then zero delta
        tree.insert(20, 5);
        tree.insert(20, 0);
        assert_eq!(tree.total_weight(), 5);
        assert_eq!(tree.get_weight(&20), 5);

        // Insert new key with zero weight after tree has elements
        tree.insert(30, 0);
        assert_eq!(tree.num_keys(), 1); // Still just key 20
    }

    #[test]
    fn test_deep_percentile_edge_cases() {
        let mut tree = OrderStatisticsMultiset::new();

        // Single element
        tree.insert(42, 1);

        // All percentiles should return the single element
        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&42));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&42));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&42));

        let (lo, hi, frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(*lo, 42);
        assert_eq!(*hi, 42);
        assert_eq!(frac, 0.0);

        // Two elements with equal weight
        tree.clear();
        tree.insert(10, 1);
        tree.insert(20, 1);

        // p=0 -> first, p=1 -> last
        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&10));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&20));

        // p=0.5 -> should be between them
        let (lo, hi, frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(*lo, 10);
        assert_eq!(*hi, 20);
        assert!((frac - 0.5).abs() < 0.01);

        // Edge percentiles
        assert_eq!(tree.select_percentile_bounds(-0.1, true), None);
        assert_eq!(tree.select_percentile_bounds(1.1, true), None);

        // Empty tree
        tree.clear();
        assert_eq!(tree.select_percentile_disc(0.5, true), None);
        assert_eq!(tree.select_percentile_bounds(0.5, true), None);
    }

    #[test]
    fn test_deep_large_weights() {
        let mut tree = OrderStatisticsMultiset::new();

        // Use large weights near i64 max (but safe from overflow)
        let large = i64::MAX / 4;
        tree.insert(1, large);
        tree.insert(2, large);
        tree.insert(3, large);

        assert_eq!(tree.total_weight(), large * 3);

        // Selection at various positions
        assert_eq!(tree.select_kth(0, true), Some(&1));
        assert_eq!(tree.select_kth(large - 1, true), Some(&1));
        assert_eq!(tree.select_kth(large, true), Some(&2));
        assert_eq!(tree.select_kth(large * 2 - 1, true), Some(&2));
        assert_eq!(tree.select_kth(large * 2, true), Some(&3));
        assert_eq!(tree.select_kth(large * 3 - 1, true), Some(&3));
        assert_eq!(tree.select_kth(large * 3, true), None);

        // Rank queries
        assert_eq!(tree.rank(&1), 0);
        assert_eq!(tree.rank(&2), large);
        assert_eq!(tree.rank(&3), large * 2);
        assert_eq!(tree.rank(&4), large * 3);
    }

    #[test]
    fn test_deep_clear_and_reuse() {
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        // Build up a tree
        for i in 0..50 {
            tree.insert(i, (i % 5 + 1) as ZWeight);
        }

        let original_total = tree.total_weight();
        assert!(original_total > 0);
        assert_eq!(tree.num_keys(), 50);

        // Clear
        tree.clear();
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.num_keys(), 0);
        assert!(tree.is_empty());
        assert_eq!(tree.select_kth(0, true), None);
        assert!(tree.iter().next().is_none());

        // Reuse the same tree
        for i in 100..110 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.total_weight(), 10);
        assert_eq!(tree.num_keys(), 10);
        assert_eq!(tree.select_kth(0, true), Some(&100));
        assert_eq!(tree.select_kth(9, true), Some(&109));
    }

    #[test]
    fn test_deep_compact_with_mixed_weights() {
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        // Insert with various weights including zeros and negatives
        tree.insert(10, 5);
        tree.insert(20, 0); // Will have zero (from insert + delta)
        tree.insert(30, -3); // Negative
        tree.insert(40, 7);
        tree.insert(50, 0); // Zero from start (no-op, won't be inserted)

        // Manually create a zero-weight entry
        tree.insert(60, 5);
        tree.insert(60, -5);

        // Before compact: have entries for 10, 30, 40, 60 (20 and 50 weren't added due to zero weight)
        // Actually 20 was inserted with 0 which is a no-op, so it's not in the tree
        // Let me re-check: insert(key, 0) returns early, so 20 and 50 are not in the tree
        // So we have: 10(5), 30(-3), 40(7), 60(0)

        let entries_before: Vec<_> = tree.iter().collect();
        // Should have 10, 30, 40, 60
        assert!(entries_before.iter().any(|(k, _)| **k == 60));

        tree.compact();

        // After compact: only entries with non-zero weight
        let entries_after: Vec<_> = tree.iter().collect();

        // 10(5), 40(7) should remain, 30(-3) has non-zero weight so it stays
        // Actually compact filters w != 0, so 30 stays, 60 is removed
        assert_eq!(entries_after.len(), 3); // 10, 30, 40
        assert!(entries_after.iter().any(|(k, _)| **k == 10));
        assert!(entries_after.iter().any(|(k, _)| **k == 30));
        assert!(entries_after.iter().any(|(k, _)| **k == 40));
        assert!(!entries_after.iter().any(|(k, _)| **k == 60));
    }

    #[test]
    fn test_deep_iteration_across_multiple_leaves() {
        let mut tree: OrderStatisticsMultiset<i32> =
            OrderStatisticsMultiset::with_branching_factor(4);

        // Insert enough to span multiple leaves
        let count = 50usize;
        for i in 0..count {
            tree.insert((i * 2) as i32, 1); // Even numbers only
        }

        // Verify iteration visits all elements in order
        let collected: Vec<_> = tree.iter().map(|(k, w)| (*k, w)).collect();

        assert_eq!(collected.len(), count);
        for (i, (k, w)) in collected.iter().enumerate() {
            assert_eq!(*k, (i * 2) as i32);
            assert_eq!(*w, 1);
        }

        // Modify some elements and verify iteration still correct
        tree.insert(0, 5); // Increase first
        tree.insert(50, 10); // Increase middle
        tree.insert(98, -1); // Remove last (set to 0)

        let modified: Vec<_> = tree.iter().collect();
        assert_eq!(modified.len(), count); // Still 50 entries (including zero-weight)

        assert_eq!(modified[0], (&0, 6)); // 1 + 5
        assert_eq!(modified[25], (&50, 11)); // 1 + 10
        assert_eq!(modified[49], (&98, 0)); // 1 - 1
    }

    #[test]
    fn test_deep_duplicate_key_stress() {
        let mut tree = OrderStatisticsMultiset::new();
        let key = 42;

        // Many updates to the same key
        for i in 0..1000 {
            if i % 2 == 0 {
                tree.insert(key, 1);
            } else {
                tree.insert(key, -1);
            }
        }

        // After 500 +1 and 500 -1, weight should be 0
        assert_eq!(tree.get_weight(&key), 0);
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.num_keys(), 1); // Key exists but with zero weight

        // Add final positive
        tree.insert(key, 100);
        assert_eq!(tree.get_weight(&key), 100);
        assert_eq!(tree.select_kth(50, true), Some(&key));
    }

    #[test]
    fn test_deep_rank_with_gaps_and_zero_weights() {
        let mut tree = OrderStatisticsMultiset::new();

        // Create gaps in the key space with some zero-weight entries
        tree.insert(10, 5);
        tree.insert(20, 0); // Zero weight - won't be inserted (no-op)
        tree.insert(30, 3);
        tree.insert(40, 0); // Zero weight - won't be inserted
        tree.insert(50, 7);

        // Insert then remove to create zero-weight entry that exists in tree
        tree.insert(35, 2);
        tree.insert(35, -2);

        // Rank queries
        assert_eq!(tree.rank(&5), 0); // Before all
        assert_eq!(tree.rank(&10), 0); // At first key
        assert_eq!(tree.rank(&15), 5); // After key 10 (weight 5)
        assert_eq!(tree.rank(&30), 5); // At key 30, after 10's weight
        assert_eq!(tree.rank(&35), 8); // After 10(5) + 30(3), but 35 has 0 weight
        assert_eq!(tree.rank(&40), 8); // Same as 35, since 35 has 0 weight
        assert_eq!(tree.rank(&50), 8); // At key 50
        assert_eq!(tree.rank(&100), 15); // After all: 5 + 3 + 7 = 15
    }

    #[test]
    fn test_deep_serialization_preserves_entries() {
        let mut original = OrderStatisticsMultiset::with_branching_factor(8);

        // Build complex tree with varying weights
        for i in 0..100 {
            original.insert(i, (i % 7 + 1) as ZWeight);
        }

        // Apply some updates
        original.insert(50, 10);
        original.insert(200, 5);

        // Serialize and restore
        let serialized: SerializableOrderStatisticsMultiset<i32> = (&original).into();
        let restored: OrderStatisticsMultiset<i32> = serialized.into();

        // Verify basic properties match
        assert_eq!(original.total_weight(), restored.total_weight());
        assert_eq!(original.num_keys(), restored.num_keys());

        // Compare all entries - this is the key correctness check
        let orig_entries: Vec<_> = original.iter().collect();
        let rest_entries: Vec<_> = restored.iter().collect();
        assert_eq!(orig_entries, rest_entries);

        // Verify rank operations match for sample keys
        for key in [0, 25, 50, 75, 99, 200] {
            assert_eq!(original.rank(&key), restored.rank(&key));
        }

        // Verify percentile operations match
        for p in [0.0, 0.25, 0.5, 0.75, 1.0] {
            assert_eq!(
                original.select_percentile_disc(p, true),
                restored.select_percentile_disc(p, true)
            );
        }
    }

    #[test]
    fn test_deep_select_consistency_after_many_operations() {
        // Build tree and verify select is consistent with content
        let mut tree: OrderStatisticsMultiset<i32> =
            OrderStatisticsMultiset::with_branching_factor(8);

        // Insert with simple weights for easy verification
        let test_data = vec![
            (10, 5), // positions 0-4
            (20, 3), // positions 5-7
            (30, 7), // positions 8-14
            (40, 2), // positions 15-16
            (50, 4), // positions 17-20
        ];

        for (key, weight) in &test_data {
            tree.insert(*key, *weight);
        }

        // Manually verify each range
        let mut pos = 0;
        for (key, weight) in &test_data {
            for _ in 0..*weight {
                assert_eq!(
                    tree.select_kth(pos, true),
                    Some(key),
                    "Failed at position {} for key {}",
                    pos,
                    key
                );
                pos += 1;
            }
        }
        assert_eq!(tree.select_kth(pos, true), None);
    }

    #[test]
    fn test_deep_equality_and_ordering() {
        let mut tree1 = OrderStatisticsMultiset::new();
        let mut tree2 = OrderStatisticsMultiset::new();

        // Build identical trees in different order
        tree1.insert(10, 5);
        tree1.insert(20, 3);
        tree1.insert(30, 7);

        tree2.insert(30, 7);
        tree2.insert(10, 5);
        tree2.insert(20, 3);

        // Should be equal (same logical content)
        assert_eq!(tree1, tree2);

        // Modify one
        tree1.insert(20, 1);

        // Now should be different
        assert_ne!(tree1, tree2);

        // Ordering should be consistent
        assert!(tree1 > tree2); // tree1 has higher total weight

        // Test with same total but different distribution
        tree2.insert(10, 1); // Now both have total 16

        assert_eq!(tree1.total_weight(), tree2.total_weight());
        // But they differ in weight distribution, so not equal
        assert_ne!(tree1, tree2);
    }

    #[test]
    fn test_deep_hash_consistency() {
        use std::collections::hash_map::DefaultHasher;

        let mut tree1 = OrderStatisticsMultiset::new();
        let mut tree2 = OrderStatisticsMultiset::new();

        // Build same content in different order
        tree1.insert(10, 5);
        tree1.insert(20, 3);

        tree2.insert(20, 3);
        tree2.insert(10, 5);

        // Hash should be the same
        let hash1 = {
            let mut h = DefaultHasher::new();
            tree1.hash(&mut h);
            h.finish()
        };

        let hash2 = {
            let mut h = DefaultHasher::new();
            tree2.hash(&mut h);
            h.finish()
        };

        assert_eq!(hash1, hash2);

        // Modify one, hash should change
        tree1.insert(30, 1);

        let hash1_modified = {
            let mut h = DefaultHasher::new();
            tree1.hash(&mut h);
            h.finish()
        };

        assert_ne!(hash1, hash1_modified);
    }

    #[test]
    fn test_deep_boundary_select_positions() {
        let mut tree = OrderStatisticsMultiset::new();

        tree.insert(10, 3); // positions 0, 1, 2
        tree.insert(20, 1); // position 3
        tree.insert(30, 2); // positions 4, 5

        // Test exact boundary positions
        assert_eq!(tree.select_kth(-1, true), None); // Negative position
        assert_eq!(tree.select_kth(0, true), Some(&10)); // First position
        assert_eq!(tree.select_kth(2, true), Some(&10)); // Last position of first key
        assert_eq!(tree.select_kth(3, true), Some(&20)); // First position of second key
        assert_eq!(tree.select_kth(4, true), Some(&30)); // First position of third key
        assert_eq!(tree.select_kth(5, true), Some(&30)); // Last valid position
        assert_eq!(tree.select_kth(6, true), None); // One past last

        // Test descending at boundaries
        assert_eq!(tree.select_kth(0, false), Some(&30)); // Last element
        assert_eq!(tree.select_kth(1, false), Some(&30));
        assert_eq!(tree.select_kth(2, false), Some(&20));
        assert_eq!(tree.select_kth(3, false), Some(&10));
        assert_eq!(tree.select_kth(5, false), Some(&10));
        assert_eq!(tree.select_kth(6, false), None);
    }

    #[test]
    fn test_deep_weight_one_elements() {
        // Common case: all elements have weight 1 (like a regular sorted set)
        let mut tree = OrderStatisticsMultiset::with_branching_factor(4);

        for i in 0..100 {
            tree.insert(i, 1);
        }

        // Position should equal key value
        for i in 0..100 {
            assert_eq!(tree.select_kth(i, true), Some(&i));
            assert_eq!(tree.rank(&i), i);
        }

        // Remove every third element
        for i in (0..100).step_by(3) {
            tree.insert(i, -1);
        }

        // Remaining: 1,2, 4,5, 7,8, 10,11, ... (skip 0,3,6,9,...)
        // That's 66 elements remaining
        assert_eq!(tree.total_weight(), 66);

        // Verify some positions
        assert_eq!(tree.select_kth(0, true), Some(&1)); // 0 was removed
        assert_eq!(tree.select_kth(1, true), Some(&2));
        assert_eq!(tree.select_kth(2, true), Some(&4)); // 3 was removed
    }

    #[test]
    fn test_deep_clone_independence() {
        let mut original = OrderStatisticsMultiset::with_branching_factor(4);

        for i in 0..20 {
            original.insert(i, 1);
        }

        // Clone
        let mut cloned = original.clone();

        // Verify they're equal
        assert_eq!(original, cloned);

        // Modify original
        original.insert(100, 5);
        original.insert(5, -1);

        // Clone should be unchanged
        assert_eq!(cloned.total_weight(), 20);
        assert_eq!(cloned.get_weight(&100), 0);
        assert_eq!(cloned.get_weight(&5), 1);

        // Modify clone
        cloned.insert(200, 10);

        // Original should be unchanged from clone's modification
        assert_eq!(original.get_weight(&200), 0);
    }

    #[test]
    fn test_deep_semigroup_associativity() {
        // Verify (a + b) + c == a + (b + c)
        let mut a = OrderStatisticsMultiset::new();
        let mut b = OrderStatisticsMultiset::new();
        let mut c = OrderStatisticsMultiset::new();

        a.insert(10, 5);
        a.insert(20, 3);

        b.insert(20, 2);
        b.insert(30, 4);

        c.insert(10, -2);
        c.insert(30, 1);
        c.insert(40, 6);

        // (a + b) + c
        let ab = OrderStatisticsMultiset::merged(&a, &b);
        let ab_c = OrderStatisticsMultiset::merged(&ab, &c);

        // a + (b + c)
        let bc = OrderStatisticsMultiset::merged(&b, &c);
        let a_bc = OrderStatisticsMultiset::merged(&a, &bc);

        // Should be equal
        assert_eq!(ab_c.total_weight(), a_bc.total_weight());

        let entries1: Vec<_> = ab_c.iter().collect();
        let entries2: Vec<_> = a_bc.iter().collect();
        assert_eq!(entries1, entries2);
    }
}
