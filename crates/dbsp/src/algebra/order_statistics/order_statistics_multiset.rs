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

use crate::{
    node_storage::NodeStorage,
    storage::file::{Deserializer, Serializer},
};
use rkyv::{Archive, Archived, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::Error;
use crate::algebra::ZWeight;
use crate::circuit::Runtime;
use crate::node_storage::{
    LeafLocation, LeafNodeOps, NodeLocation, NodeStorageConfig, StorableNode,
};
use crate::utils::IsNone;
use feldera_storage::{FileCommitter, StoragePath};

/// Default branching factor for the B+ tree.
/// Larger values are more cache/disk friendly but may have higher constant factors.
/// 64 provides a good balance for most workloads.
pub const DEFAULT_BRANCHING_FACTOR: usize = 64;

/// Minimum branching factor to ensure tree properties.
pub const MIN_BRANCHING_FACTOR: usize = 4;

/// Type alias for OrderStatisticsMultiset storage.
///
/// This provides backward compatibility and convenience for the common case
/// of using NodeStorage with OrderStatisticsMultiset node types.
pub type OsmNodeStorage<T> = NodeStorage<InternalNodeTyped<T>, LeafNode<T>>;

/// A leaf node in the B+ tree, storing sorted (key, weight) pairs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub struct LeafNode<T> {
    /// Sorted keys with their weights
    pub entries: Vec<(T, ZWeight)>,
    /// Link to next leaf for efficient iteration (None if this is the last leaf)
    pub next_leaf: Option<LeafLocation>,
}

impl<T: Ord + Clone> LeafNode<T> {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_leaf: None,
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            next_leaf: None,
        }
    }

    /// Total weight in this leaf
    pub fn total_weight(&self) -> ZWeight {
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

    /// Split this leaf, returning the new right leaf and the split key.
    /// The caller must set self.next_leaf to point to the right leaf after allocating it.
    fn split(&mut self) -> (T, LeafNode<T>) {
        let mid = self.entries.len() / 2;
        let right_entries = self.entries.split_off(mid);
        let split_key = right_entries[0].0.clone();

        let right = LeafNode {
            entries: right_entries,
            next_leaf: self.next_leaf.take(), // Right leaf inherits our next pointer
        };

        // self.next_leaf is now None; caller will set it to point to right leaf
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

/// An internal node with actual key storage
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
pub struct InternalNodeTyped<T> {
    /// Separator keys: keys[i] is the minimum key in children[i+1]
    pub keys: Vec<T>,
    /// Child node locations (can be internal or leaf nodes)
    pub children: Vec<NodeLocation>,
    /// Sum of weights in each child's subtree
    pub subtree_sums: Vec<ZWeight>,
}

impl<T: Ord + Clone> InternalNodeTyped<T> {
    #[allow(dead_code)]
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
    pub fn total_weight(&self) -> ZWeight {
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
    fn find_child_for_select(&self, k: ZWeight) -> Option<(NodeLocation, ZWeight)> {
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

// =============================================================================
// StorableNode implementation for InternalNodeTyped<T>
// =============================================================================

impl<T> StorableNode for InternalNodeTyped<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn estimate_size(&self) -> usize {
        // Estimate: keys vec + children vec + subtree_sums vec + overhead
        let keys_size = self.keys.len() * std::mem::size_of::<T>();
        let children_size = self.children.len() * std::mem::size_of::<NodeLocation>();
        let sums_size = self.subtree_sums.len() * std::mem::size_of::<ZWeight>();
        keys_size + children_size + sums_size + 3 * std::mem::size_of::<Vec<()>>()
    }
}

// =============================================================================
// StorableNode implementation for LeafNode<T>
// =============================================================================

impl<T> StorableNode for LeafNode<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn estimate_size(&self) -> usize {
        // Estimate: entries vec + next_leaf option + overhead
        let entries_size =
            self.entries.len() * (std::mem::size_of::<T>() + std::mem::size_of::<ZWeight>());
        let option_size = std::mem::size_of::<Option<LeafLocation>>();
        entries_size + option_size + std::mem::size_of::<Vec<()>>()
    }
}

impl<T: Ord + Clone> LeafNodeOps for LeafNode<T> {
    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn total_weight(&self) -> ZWeight {
        self.entries.iter().map(|(_, w)| *w).sum()
    }
}

/// A node in the B+ tree (either leaf or internal)
#[derive(Debug, Clone, PartialEq, Eq, SizeOf, serde::Serialize, serde::Deserialize)]
pub enum Node<T> {
    Leaf(LeafNode<T>),
    Internal(InternalNodeTyped<T>),
}

#[allow(dead_code)]
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
/// # Storage
/// Uses `NodeStorage` internally which separates internal nodes (always in memory)
/// from leaf nodes (can be spilled to disk when memory pressure is high).
///
/// # Example
/// ```ignore
/// use dbsp::algebra::OrderStatisticsMultiset;
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
#[derive(Debug, SizeOf)]
pub struct OrderStatisticsMultiset<T: Ord + Clone> {
    /// Node storage (separates internal nodes and leaves, supports disk spilling)
    storage: OsmNodeStorage<T>,
    /// Location of the root node (None if tree is empty)
    root: Option<NodeLocation>,
    /// Total weight across all elements
    total_weight: ZWeight,
    /// Maximum entries per leaf node
    max_leaf_entries: usize,
    /// Maximum children per internal node
    max_internal_children: usize,
    /// Location of the first leaf (for iteration)
    first_leaf: Option<LeafLocation>,
    /// Number of distinct keys
    num_keys: usize,
}

// Implement Clone manually since NodeStorage doesn't implement Clone
impl<T> Clone for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn clone(&self) -> Self {
        // Rebuild from entries (this is O(n) due to bulk load)
        let entries: Vec<_> = self.iter().map(|(k, w)| (k.clone(), w)).collect();
        Self::from_sorted_entries(entries, self.max_leaf_entries)
    }
}

// Implement PartialEq manually to compare contents, not structure
impl<T> PartialEq for OrderStatisticsMultiset<T>
where
    T: Ord
        + Clone
        + Debug
        + SizeOf
        + Send
        + Sync
        + 'static
        + PartialEq
        + Archive
        + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
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

impl<T> Eq for OrderStatisticsMultiset<T>
where
    T: Ord
        + Clone
        + Debug
        + SizeOf
        + Send
        + Sync
        + 'static
        + PartialEq
        + Archive
        + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
}

impl<T> PartialOrd for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
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
    type Inner = OrderStatisticsMultiset<T>;

    fn is_none(&self) -> bool {
        false // OrderStatisticsMultiset is never "none"
    }

    fn unwrap_or_self(&self) -> &Self::Inner {
        self
    }

    fn from_inner(inner: Self::Inner) -> Self {
        inner
    }
}

impl<T> Hash for OrderStatisticsMultiset<T>
where
    T: Ord
        + Clone
        + Debug
        + SizeOf
        + Send
        + Sync
        + 'static
        + Hash
        + Archive
        + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.total_weight.hash(state);
        self.num_keys.hash(state);
        for (key, weight) in self.iter() {
            key.hash(state);
            weight.hash(state);
        }
    }
}

impl<T> Default for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    /// Create a new empty multiset with default branching factor.
    ///
    /// If running inside a DBSP Runtime with storage configured, this will
    /// automatically enable spill-to-disk when memory pressure is high.
    pub fn new() -> Self {
        Self::with_config(DEFAULT_BRANCHING_FACTOR, NodeStorageConfig::from_runtime())
    }

    /// Create a new empty multiset with specified branching factor.
    ///
    /// Larger branching factors are more cache/disk friendly but may have
    /// higher constant factors for small trees.
    ///
    /// If running inside a DBSP Runtime with storage configured, this will
    /// automatically enable spill-to-disk when memory pressure is high.
    pub fn with_branching_factor(b: usize) -> Self {
        Self::with_config(b, NodeStorageConfig::from_runtime())
    }

    /// Create a new empty multiset with specified branching factor and storage config.
    pub fn with_config(b: usize, storage_config: NodeStorageConfig) -> Self {
        let b = b.max(MIN_BRANCHING_FACTOR);
        Self {
            storage: OsmNodeStorage::with_config(storage_config),
            root: None,
            total_weight: 0,
            max_leaf_entries: b,
            max_internal_children: b,
            first_leaf: None,
            num_keys: 0,
        }
    }

    /// Construct a multiset from pre-sorted entries in O(n) time.
    ///
    /// This method builds the B+ tree bottom-up, which is significantly faster
    /// than repeated insertions for bulk loading scenarios like deserialization
    /// or checkpoint restore.
    ///
    /// # Preconditions
    /// - `entries` MUST be sorted by key in ascending order
    /// - Duplicate keys should have their weights combined before calling
    ///
    /// # Complexity
    /// O(n) where n = entries.len()
    ///
    /// # Panics
    /// Debug builds panic if entries are not sorted.
    pub fn from_sorted_entries(entries: Vec<(T, ZWeight)>, branching_factor: usize) -> Self {
        let b = branching_factor.max(MIN_BRANCHING_FACTOR);

        // Handle empty case
        if entries.is_empty() {
            return Self::with_branching_factor(b);
        }

        // Debug assertion for sorted input
        debug_assert!(
            entries.windows(2).all(|w| w[0].0 <= w[1].0),
            "entries must be sorted by key"
        );

        let num_keys = entries.len();
        let total_weight: ZWeight = entries.iter().map(|(_, w)| *w).sum();

        let mut storage = OsmNodeStorage::new();

        // Phase 1: Build leaf nodes
        let mut leaf_locations: Vec<LeafLocation> = Vec::new();
        let mut leaf_weights: Vec<ZWeight> = Vec::new();

        for chunk in entries.chunks(b) {
            let leaf = LeafNode {
                entries: chunk.to_vec(),
                next_leaf: None, // Will be set below
            };
            leaf_weights.push(leaf.total_weight());
            let loc = storage.alloc_leaf(leaf);
            let leaf_loc = loc.as_leaf().expect("alloc_leaf returns Leaf location");
            leaf_locations.push(leaf_loc);
        }

        // Link leaves together
        for i in 0..leaf_locations.len().saturating_sub(1) {
            let current_loc = leaf_locations[i];
            let next_loc = leaf_locations[i + 1];
            storage.get_leaf_mut(current_loc).next_leaf = Some(next_loc);
        }

        let first_leaf = Some(leaf_locations[0]);

        // If there's only one leaf, it becomes the root
        if leaf_locations.len() == 1 {
            return Self {
                storage,
                root: Some(NodeLocation::Leaf(leaf_locations[0])),
                total_weight,
                max_leaf_entries: b,
                max_internal_children: b,
                first_leaf,
                num_keys,
            };
        }

        // Phase 2: Build internal layers bottom-up
        // Level 0 = leaves, Level 1 = first internal nodes, etc.
        let mut current_level_locs: Vec<NodeLocation> =
            leaf_locations.into_iter().map(NodeLocation::Leaf).collect();
        let mut current_level_weights = leaf_weights;
        let mut current_level: u8 = 1; // First internal nodes are at level 1

        while current_level_locs.len() > 1 {
            let mut next_level_locs = Vec::new();
            let mut next_level_weights = Vec::new();

            // Process children in groups of B
            let mut i = 0;
            while i < current_level_locs.len() {
                let chunk_end = (i + b).min(current_level_locs.len());
                let chunk_locs = &current_level_locs[i..chunk_end];
                let chunk_weights = &current_level_weights[i..chunk_end];

                // Build keys: first key of each child except the first
                let mut keys = Vec::with_capacity(chunk_locs.len().saturating_sub(1));
                for &child_loc in &chunk_locs[1..] {
                    let first_key = Self::get_first_key_from_storage(&storage, child_loc);
                    keys.push(first_key);
                }

                let internal = InternalNodeTyped {
                    keys,
                    children: chunk_locs.to_vec(),
                    subtree_sums: chunk_weights.to_vec(),
                };

                let internal_weight = internal.total_weight();
                next_level_weights.push(internal_weight);
                let internal_loc = storage.alloc_internal(internal, current_level);
                next_level_locs.push(internal_loc);

                i = chunk_end;
            }

            current_level_locs = next_level_locs;
            current_level_weights = next_level_weights;
            current_level = current_level.saturating_add(1);
        }

        let root = Some(current_level_locs[0]);

        Self {
            storage,
            root,
            total_weight,
            max_leaf_entries: b,
            max_internal_children: b,
            first_leaf,
            num_keys,
        }
    }

    /// Helper to get the first key from a node (for building separator keys).
    /// This recursively descends to the leftmost leaf to find the minimum key.
    fn get_first_key_from_storage(storage: &OsmNodeStorage<T>, loc: NodeLocation) -> T {
        match loc {
            NodeLocation::Leaf(leaf_loc) => storage.get_leaf(leaf_loc).entries[0].0.clone(),
            NodeLocation::Internal { id, .. } => {
                let internal = storage.get_internal(id);
                Self::get_first_key_from_storage(storage, internal.children[0])
            }
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

        if self.root.is_none() {
            // Tree is empty, create first leaf
            let mut leaf = LeafNode::with_capacity(self.max_leaf_entries);
            leaf.entries.push((key, weight));
            let loc = self.storage.alloc_leaf(leaf);
            let leaf_loc = loc.as_leaf().expect("alloc_leaf returns Leaf location");
            self.root = Some(loc);
            self.first_leaf = Some(leaf_loc);
            self.total_weight = weight;
            self.num_keys = 1;
            return;
        }

        // Insert into tree and handle splits
        let root_loc = self.root.unwrap();
        let (new_key_created, split_result) = self.insert_recursive(root_loc, key, weight);

        if new_key_created {
            self.num_keys += 1;
        }
        self.total_weight += weight;

        // Handle root split
        if let Some((promoted_key, new_child_loc)) = split_result {
            let mut new_root = InternalNodeTyped::with_capacity(self.max_internal_children);
            new_root.children.push(root_loc);
            new_root.children.push(new_child_loc);
            new_root.keys.push(promoted_key);

            // Calculate subtree sums
            let left_sum = self.node_total_weight(root_loc);
            let right_sum = self.node_total_weight(new_child_loc);
            new_root.subtree_sums.push(left_sum);
            new_root.subtree_sums.push(right_sum);

            // New root is one level above the old root
            let new_root_level = root_loc.level().saturating_add(1);
            let new_root_loc = self.storage.alloc_internal(new_root, new_root_level);
            self.root = Some(new_root_loc);
        }
    }

    /// Get the total weight of a node at the given location.
    fn node_total_weight(&self, loc: NodeLocation) -> ZWeight {
        match loc {
            NodeLocation::Leaf(leaf_loc) => self.storage.get_leaf(leaf_loc).total_weight(),
            NodeLocation::Internal { id, .. } => self.storage.get_internal(id).total_weight(),
        }
    }

    /// Recursive insert helper. Returns (new_key_created, optional split result).
    fn insert_recursive(
        &mut self,
        loc: NodeLocation,
        key: T,
        weight: ZWeight,
    ) -> (bool, Option<(T, NodeLocation)>) {
        match loc {
            NodeLocation::Leaf(leaf_loc) => {
                // Insert into leaf
                let leaf = self.storage.get_leaf_mut(leaf_loc);
                let new_key = leaf.find_key_pos(&key).is_err();
                leaf.insert(key, weight);
                let needs_split = leaf.needs_split(self.max_leaf_entries);

                // Handle split if needed
                if needs_split {
                    let leaf = self.storage.get_leaf_mut(leaf_loc);
                    let (split_key, right_leaf) = leaf.split();
                    // right_leaf already has the correct next_leaf (inherited from left)

                    // Allocate the right leaf
                    let right_loc = self.storage.alloc_leaf(right_leaf);
                    let right_leaf_loc = right_loc
                        .as_leaf()
                        .expect("alloc_leaf returns Leaf location");

                    // Update the left leaf's next pointer to the new right leaf
                    self.storage.get_leaf_mut(leaf_loc).next_leaf = Some(right_leaf_loc);

                    (new_key, Some((split_key, right_loc)))
                } else {
                    (new_key, None)
                }
            }
            NodeLocation::Internal {
                id: internal_idx,
                level,
            } => {
                // Internal node - find child and recurse
                let (child_loc, child_pos) = {
                    let internal = self.storage.get_internal(internal_idx);
                    let child_pos = internal.find_child(&key);
                    (internal.children[child_pos], child_pos)
                };

                let (new_key, split_result) = self.insert_recursive(child_loc, key, weight);

                // Update subtree sum for the child we descended into
                {
                    let internal = self.storage.get_internal_mut(internal_idx);
                    internal.subtree_sums[child_pos] += weight;
                }

                // Handle child split
                if let Some((promoted_key, new_child_loc)) = split_result {
                    // Calculate sums
                    let left_sum = self.node_total_weight(child_loc);
                    let right_sum = self.node_total_weight(new_child_loc);

                    let needs_internal_split = {
                        let internal = self.storage.get_internal_mut(internal_idx);

                        internal.subtree_sums[child_pos] = left_sum;
                        internal.keys.insert(child_pos, promoted_key);
                        internal.children.insert(child_pos + 1, new_child_loc);
                        internal.subtree_sums.insert(child_pos + 1, right_sum);

                        internal.needs_split(self.max_internal_children)
                    };

                    // Handle internal node split if needed
                    if needs_internal_split {
                        let internal = self.storage.get_internal_mut(internal_idx);
                        let (promoted, right_internal) = internal.split();
                        // Right sibling has the same level as the original node
                        let right_loc = self.storage.alloc_internal(right_internal, level);
                        return (new_key, Some((promoted, right_loc)));
                    }
                }

                (new_key, None)
            }
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

        if effective_k < 0 || effective_k >= self.total_weight || self.root.is_none() {
            return None;
        }
        self.select_kth_recursive(self.root.unwrap(), effective_k)
    }

    fn select_kth_recursive(&self, loc: NodeLocation, k: ZWeight) -> Option<&T> {
        match loc {
            NodeLocation::Leaf(leaf_loc) => self.storage.get_leaf(leaf_loc).select_kth(k),
            NodeLocation::Internal { id, .. } => {
                let internal = self.storage.get_internal(id);
                let (child_loc, remaining_k) = internal.find_child_for_select(k)?;
                self.select_kth_recursive(child_loc, remaining_k)
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
        match self.root {
            None => 0,
            Some(loc) => self.rank_recursive(loc, key),
        }
    }

    fn rank_recursive(&self, loc: NodeLocation, key: &T) -> ZWeight {
        match loc {
            NodeLocation::Leaf(leaf_loc) => self.storage.get_leaf(leaf_loc).prefix_weight(key),
            NodeLocation::Internal { id, .. } => {
                let internal = self.storage.get_internal(id);
                let child_pos = internal.find_child(key);
                let prefix = internal.prefix_weight_before_child(child_pos);
                let child_loc = internal.children[child_pos];
                prefix + self.rank_recursive(child_loc, key)
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
        match self.root {
            None => 0,
            Some(loc) => self.get_weight_recursive(loc, key),
        }
    }

    fn get_weight_recursive(&self, loc: NodeLocation, key: &T) -> ZWeight {
        match loc {
            NodeLocation::Leaf(leaf_loc) => {
                let leaf = self.storage.get_leaf(leaf_loc);
                match leaf.find_key_pos(key) {
                    Ok(pos) => leaf.entries[pos].1,
                    Err(_) => 0,
                }
            }
            NodeLocation::Internal { id, .. } => {
                let internal = self.storage.get_internal(id);
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

    /// Get a reference to the internal storage.
    /// This is useful for disk spilling operations.
    pub fn storage(&self) -> &OsmNodeStorage<T> {
        &self.storage
    }

    /// Get a mutable reference to the internal storage.
    /// This is useful for disk spilling operations.
    pub fn storage_mut(&mut self) -> &mut OsmNodeStorage<T> {
        &mut self.storage
    }

    /// Reload all evicted leaves from disk into memory.
    ///
    /// This is useful after restoring from a checkpoint when you need
    /// to iterate over all entries (since the iterator requires leaves
    /// to be in memory).
    ///
    /// # Returns
    /// * Ok(count) - Number of leaves reloaded
    /// * Err - If loading fails
    pub fn reload_evicted_leaves(&mut self) -> Result<usize, crate::node_storage::FileFormatError> {
        self.storage.reload_evicted_leaves()
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
        // Use O(n) bulk construction instead of O(n log n) repeated inserts
        *self = Self::from_sorted_entries(entries, b);
    }

    /// Clear all entries from the multiset.
    pub fn clear(&mut self) {
        self.storage.clear();
        self.root = None;
        self.total_weight = 0;
        self.first_leaf = None;
        self.num_keys = 0;
    }

    /// Alias for `total_weight()` for API compatibility.
    #[inline]
    pub fn total_count(&self) -> ZWeight {
        self.total_weight
    }

    // =========================================================================
    // Checkpoint Methods
    // =========================================================================

    /// Collect leaf summaries for checkpoint metadata.
    ///
    /// Returns a summary for each leaf containing:
    /// - `first_key`: First key in the leaf (for building separator keys)
    /// - `weight_sum`: Total weight in the leaf (for building subtree_sums)
    /// - `entry_count`: Number of entries
    ///
    /// This enables O(num_leaves) restore - internal nodes are rebuilt
    /// from summaries without reading leaf contents.
    pub fn collect_leaf_summaries(&self) -> Vec<crate::node_storage::LeafSummary<T>> {
        use crate::node_storage::LeafSummary;

        let mut summaries = Vec::new();
        let mut current_leaf = self.first_leaf;

        while let Some(leaf_loc) = current_leaf {
            let leaf = self.storage.get_leaf(leaf_loc);

            let first_key = leaf.entries.first().map(|(k, _)| k.clone());
            let weight_sum = leaf.total_weight();
            let entry_count = leaf.entries.len();

            summaries.push(LeafSummary::new(first_key, weight_sum, entry_count));
            current_leaf = leaf.next_leaf;
        }

        summaries
    }

    /// Checkpoint the tree's leaves to disk.
    ///
    /// This method:
    /// 1. Flushes all dirty/never-spilled leaves to disk
    /// 2. Collects leaf summaries for O(num_leaves) restore
    /// 3. Moves the spill file to checkpoint location
    /// 4. Returns metadata referencing the checkpoint file
    ///
    /// # Arguments
    ///
    /// - `checkpoint_dir`: Directory for checkpoint files
    /// - `tree_id`: Unique identifier for this tree's checkpoint file
    ///
    /// # Returns
    ///
    /// `CommittedLeafStorage` containing metadata for restore.
    pub fn save_leaves(
        &mut self,
        checkpoint_dir: &std::path::Path,
        tree_id: &str,
    ) -> Result<crate::node_storage::CommittedLeafStorage<T>, crate::node_storage::FileFormatError>
    {
        use crate::node_storage::{CommittedLeafStorage, FileFormatError};

        // Ensure checkpoint directory exists
        std::fs::create_dir_all(checkpoint_dir)
            .map_err(|e| FileFormatError::Io(format!("Failed to create checkpoint dir: {}", e)))?;

        // Collect summaries BEFORE flushing (need access to all leaf data)
        let leaf_summaries = self.collect_leaf_summaries();

        // Flush all leaves to disk
        self.storage.flush_all_leaves_to_disk()?;

        // Get block locations from storage
        let leaf_block_locations: Vec<(usize, u64, u32)> = self
            .storage
            .leaf_block_locations
            .iter()
            .map(|(&id, &(offset, size))| (id, offset, size))
            .collect();

        // Move spill file to checkpoint location
        let dest_path = checkpoint_dir.join(format!("{}.leaves", tree_id));

        if let Some(src_path) = self.storage.take_spill_file()
            && src_path.exists()
        {
            // Try rename first (fast, same filesystem)
            // Fall back to copy if rename fails (cross-filesystem)
            if std::fs::rename(&src_path, &dest_path).is_err() {
                std::fs::copy(&src_path, &dest_path).map_err(|e| {
                    FileFormatError::Io(format!("Failed to copy spill file to checkpoint: {}", e))
                })?;
                // Clean up source after successful copy
                let _ = std::fs::remove_file(&src_path);
            }
        }

        // Compute totals
        let total_entries: usize = leaf_summaries.iter().map(|s| s.entry_count).sum();
        let total_weight: ZWeight = leaf_summaries.iter().map(|s| s.weight_sum).sum();
        let num_leaves = leaf_summaries.len();

        Ok(CommittedLeafStorage {
            spill_file_path: dest_path.to_string_lossy().to_string(),
            leaf_block_locations,
            leaf_summaries,
            total_entries,
            total_weight,
            num_leaves,
        })
    }

    /// Restore from checkpoint - O(num_leaves), not O(num_entries).
    ///
    /// This method:
    /// 1. Points NodeStorage at the checkpoint file (no copy!)
    /// 2. Rebuilds internal nodes from leaf_summaries (no leaf I/O!)
    /// 3. Marks all leaves as evicted (lazy-loaded on demand)
    ///
    /// The checkpoint file becomes the live spill file.
    ///
    /// # Arguments
    ///
    /// - `committed`: Checkpoint metadata from `save_leaves()`
    /// - `branching_factor`: Branching factor for the tree
    /// - `storage_config`: Configuration for the storage backend
    ///
    /// # Returns
    ///
    /// A restored `OrderStatisticsMultiset`.
    pub fn restore_from_committed(
        committed: crate::node_storage::CommittedLeafStorage<T>,
        branching_factor: usize,
        storage_config: crate::node_storage::NodeStorageConfig,
    ) -> Result<Self, Error> {
        use std::path::PathBuf;

        let num_leaves = committed.num_leaves;

        if num_leaves == 0 {
            // Empty tree
            return Ok(Self::with_config(branching_factor, storage_config));
        }

        // Create storage with the checkpoint file as spill file
        let mut storage = OsmNodeStorage::with_config(storage_config);

        // Set up the checkpoint file as our spill file
        let spill_path = PathBuf::from(&committed.spill_file_path);
        storage.mark_all_leaves_evicted(
            num_leaves,
            spill_path,
            committed.leaf_block_locations.clone(),
        );

        // Build internal nodes from leaf summaries
        let (internal_nodes_with_levels, root_loc, first_leaf_loc) =
            Self::build_internal_nodes_from_summaries(&committed.leaf_summaries, branching_factor);

        // Add internal nodes to storage (as clean since they're rebuilt from summaries)
        for (node, level) in internal_nodes_with_levels {
            storage.alloc_internal_clean(node, level);
        }

        // Count total keys from summaries
        let num_keys: usize = committed.leaf_summaries.iter().map(|s| s.entry_count).sum();

        Ok(Self {
            storage,
            root: root_loc,
            total_weight: committed.total_weight,
            max_leaf_entries: branching_factor,
            max_internal_children: branching_factor,
            first_leaf: first_leaf_loc,
            num_keys,
        })
    }

    /// Build internal nodes from leaf summaries.
    ///
    /// Uses first_key for separator keys, weight_sum for subtree_sums.
    /// O(num_leaves) time - no leaf content I/O required.
    ///
    /// # Arguments
    ///
    /// - `summaries`: Leaf summaries from checkpoint
    /// - `branching_factor`: Maximum children per internal node
    ///
    /// # Returns
    ///
    /// - Vector of (internal_node, level) tuples
    /// - Root node location (if any)
    /// - First leaf location (if any)
    fn build_internal_nodes_from_summaries(
        summaries: &[crate::node_storage::LeafSummary<T>],
        branching_factor: usize,
    ) -> (
        Vec<(InternalNodeTyped<T>, u8)>,
        Option<NodeLocation>,
        Option<LeafLocation>,
    ) {
        if summaries.is_empty() {
            return (Vec::new(), None, None);
        }

        let first_leaf = Some(LeafLocation::new(0));

        if summaries.len() == 1 {
            // Single leaf - no internal nodes needed, root is the leaf
            return (
                Vec::new(),
                Some(NodeLocation::Leaf(LeafLocation::new(0))),
                first_leaf,
            );
        }

        let mut internal_nodes: Vec<(InternalNodeTyped<T>, u8)> = Vec::new();

        // Build bottom-up: first level of internal nodes directly points to leaves
        let mut current_level_locations: Vec<NodeLocation> = Vec::new();
        let mut current_level_sums: Vec<ZWeight> = Vec::new();
        let mut current_level_keys: Vec<Option<T>> = Vec::new();

        // Process leaves in groups of `branching_factor`
        for (i, summary) in summaries.iter().enumerate() {
            current_level_locations.push(NodeLocation::Leaf(LeafLocation::new(i)));
            current_level_sums.push(summary.weight_sum);
            current_level_keys.push(summary.first_key.clone());
        }

        // Build internal nodes level by level
        // Level 1 is just above leaves, level 2 above level 1, etc.
        let mut current_level: u8 = 1;
        while current_level_locations.len() > 1 {
            let mut next_level_locations: Vec<NodeLocation> = Vec::new();
            let mut next_level_sums: Vec<ZWeight> = Vec::new();
            let mut next_level_keys: Vec<Option<T>> = Vec::new();

            // Group children into internal nodes
            for chunk_start in (0..current_level_locations.len()).step_by(branching_factor) {
                let chunk_end = (chunk_start + branching_factor).min(current_level_locations.len());

                let children: Vec<_> = current_level_locations[chunk_start..chunk_end].to_vec();
                let subtree_sums: Vec<_> = current_level_sums[chunk_start..chunk_end].to_vec();

                // Separator keys: first key of each child except the first
                let keys: Vec<T> = current_level_keys[chunk_start + 1..chunk_end]
                    .iter()
                    .filter_map(|k| k.clone())
                    .collect();

                let internal_node = InternalNodeTyped {
                    keys,
                    children,
                    subtree_sums: subtree_sums.clone(),
                };

                let node_id = internal_nodes.len();
                internal_nodes.push((internal_node, current_level));

                next_level_locations.push(NodeLocation::Internal {
                    id: node_id,
                    level: current_level,
                });
                next_level_sums.push(subtree_sums.iter().sum());
                next_level_keys.push(current_level_keys[chunk_start].clone());
            }

            current_level_locations = next_level_locations;
            current_level_sums = next_level_sums;
            current_level_keys = next_level_keys;
            current_level = current_level.saturating_add(1);
        }

        // The last remaining location is the root
        let root = current_level_locations.into_iter().next();

        (internal_nodes, root, first_leaf)
    }

    /// Construct from restored components.
    ///
    /// Used internally by `restore()` to create the final tree.
    pub fn from_storage(
        storage: OsmNodeStorage<T>,
        root: Option<NodeLocation>,
        total_weight: ZWeight,
        max_leaf_entries: usize,
        first_leaf: Option<LeafLocation>,
        num_keys: usize,
    ) -> Self {
        Self {
            storage,
            root,
            total_weight,
            max_leaf_entries,
            max_internal_children: max_leaf_entries,
            first_leaf,
            num_keys,
        }
    }
}

/// Iterator over (key, weight) pairs in sorted order.
struct OrderStatisticsIter<'a, T: Ord + Clone> {
    tree: &'a OrderStatisticsMultiset<T>,
    current_leaf: Option<LeafLocation>,
    current_pos: usize,
}

impl<'a, T> Iterator for OrderStatisticsIter<'a, T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    type Item = (&'a T, ZWeight);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let leaf_loc = self.current_leaf?;
            let leaf = self.tree.storage.get_leaf(leaf_loc);

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

// Note: LeafNode and InternalNodeTyped have rkyv derives directly on their struct
// definitions, so we don't need separate ArchivedLeafNode/ArchivedInternalNode types.
// The rkyv derive macro auto-generates ArchivedLeafNode and ArchivedInternalNodeTyped.

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
    deserialize = "T: Archive, <T as Archive>::Archived: rkyv::Deserialize<T, __D>",
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

impl<T> From<&OrderStatisticsMultiset<T>> for SerializableOrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn from(tree: &OrderStatisticsMultiset<T>) -> Self {
        Self {
            entries: tree.iter().map(|(k, w)| (k.clone(), w)).collect(),
            branching_factor: tree.max_leaf_entries as u32,
        }
    }
}

impl<T> From<SerializableOrderStatisticsMultiset<T>> for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
{
    fn from(serialized: SerializableOrderStatisticsMultiset<T>) -> Self {
        // Use O(n) bulk construction instead of O(n log n) repeated inserts
        Self::from_sorted_entries(serialized.entries, serialized.branching_factor as usize)
    }
}

// Implement rkyv traits for OrderStatisticsMultiset via SerializableOrderStatisticsMultiset
//
// We use SerializableOrderStatisticsMultiset as the archived form, which flattens
// the B+ tree to a simple sorted vector of (key, weight) pairs. This enables
// efficient serialization for spill-to-disk scenarios.
//
// Note: The Archive impl has minimal bounds (just T: Archive + Archived<T>: Ord)
// because SerializableOrderStatisticsMultiset requires Ord on the archived type.
// The Serialize and Deserialize impls have full bounds.

impl<T> Archive for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Archive,
    <T as Archive>::Archived: Ord,
{
    type Archived = ArchivedSerializableOrderStatisticsMultiset<T>;
    type Resolver = <SerializableOrderStatisticsMultiset<T> as Archive>::Resolver;

    #[allow(clippy::unit_arg)]
    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        // NOTE: This impl exists to make OrderStatisticsMultiset archivable.
        // The actual conversion happens in Serialize::serialize.
        // We can't access storage.iter() here because we don't have the right bounds.
        // This is only called via serialize() which has the full bounds.
        unreachable!("resolve should not be called directly - use serialize()")
    }
}

impl<T, S> rkyv::Serialize<S> for OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
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
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
    <T as Archive>::Archived: Ord,
    D: rkyv::Fallible + ?Sized,
    ArchivedSerializableOrderStatisticsMultiset<T>:
        rkyv::Deserialize<SerializableOrderStatisticsMultiset<T>, D>,
{
    fn deserialize(&self, deserializer: &mut D) -> Result<OrderStatisticsMultiset<T>, D::Error> {
        let serializable: SerializableOrderStatisticsMultiset<T> =
            rkyv::Deserialize::deserialize(self, deserializer)?;
        Ok(serializable.into())
    }
}

// ============================================================================
// Checkpoint save/restore support
// ============================================================================

/// Helper function to serialize to bytes using rkyv.
fn to_bytes<T>(value: &T) -> Result<feldera_storage::fbuf::FBuf, Error>
where
    T: Archive + for<'a> RkyvSerialize<rkyv::ser::serializers::AllocSerializer<4096>>,
{
    use feldera_storage::fbuf::FBuf;
    use rkyv::ser::Serializer as RkyvSerializer;
    use rkyv::ser::serializers::AllocSerializer;
    use std::io::Error as IoError;

    let mut serializer = AllocSerializer::<4096>::default();
    serializer
        .serialize_value(value)
        .map_err(|e| Error::IO(IoError::other(format!("Failed to serialize: {e}"))))?;
    let bytes = serializer.into_serializer().into_inner();

    // Copy to FBuf which has the required 512-byte alignment
    let mut fbuf = FBuf::with_capacity(bytes.len());
    fbuf.extend_from_slice(&bytes);
    Ok(fbuf)
}

impl<T> OrderStatisticsMultiset<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + RkyvSerialize<Serializer>,
    Archived<T>: RkyvDeserialize<T, Deserializer>,
    <T as Archive>::Archived: Ord,
    SerializableOrderStatisticsMultiset<T>:
        for<'a> rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<4096>>,
    ArchivedSerializableOrderStatisticsMultiset<T>:
        rkyv::Deserialize<SerializableOrderStatisticsMultiset<T>, Deserializer>,
{
    /// Save the multiset state to persistent storage.
    ///
    /// Creates a checkpoint file at `{base}/{persistent_id}_osm.dat` containing
    /// a serialized representation of the multiset.
    ///
    /// # Arguments
    /// * `base` - Base directory for checkpoint files
    /// * `persistent_id` - Unique identifier for this multiset's state
    /// * `files` - Vector to append FileCommitter for atomic checkpoint commit
    ///
    /// # Example
    /// ```ignore
    /// let mut files = Vec::new();
    /// tree.save(&"checkpoint/uuid".into(), "percentile_0", &mut files)?;
    /// // Later, commit all files atomically
    /// for file in files {
    ///     file.commit()?;
    /// }
    /// ```
    pub fn save(
        &self,
        base: &StoragePath,
        persistent_id: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let backend = Runtime::storage_backend()?;

        // Convert to serializable form and serialize
        let serializable: SerializableOrderStatisticsMultiset<T> = self.into();
        let bytes = to_bytes(&serializable)?;

        // Write to checkpoint file
        let path = Self::checkpoint_file(base, persistent_id);
        let committer = backend.write(&path, bytes)?;
        files.push(committer);

        Ok(())
    }

    /// Restore the multiset state from persistent storage.
    ///
    /// Reads the checkpoint file at `{base}/{persistent_id}_osm.dat` and
    /// reconstructs the multiset from the serialized data.
    ///
    /// # Arguments
    /// * `base` - Base directory for checkpoint files
    /// * `persistent_id` - Unique identifier for this multiset's state
    ///
    /// # Example
    /// ```ignore
    /// let mut tree = OrderStatisticsMultiset::new();
    /// tree.restore(&"checkpoint/uuid".into(), "percentile_0")?;
    /// ```
    pub fn restore(&mut self, base: &StoragePath, persistent_id: &str) -> Result<(), Error> {
        let backend = Runtime::storage_backend()?;

        // Read checkpoint file
        let path = Self::checkpoint_file(base, persistent_id);
        let content = backend.read(&path)?;

        // Deserialize using rkyv with DBSP's Deserializer
        // SAFETY: The data was written by save() using rkyv serialization
        let archived =
            unsafe { rkyv::archived_root::<SerializableOrderStatisticsMultiset<T>>(&content) };

        // Use DBSP's Deserializer which wraps SharedDeserializeMap
        // Version 0 since we use AllocSerializer (not file format)
        let mut deserializer = Deserializer::new(0);
        let serializable: SerializableOrderStatisticsMultiset<T> =
            rkyv::Deserialize::deserialize(archived, &mut deserializer).map_err(|e| {
                use std::io::Error as IoError;
                Error::IO(IoError::other(format!(
                    "Failed to deserialize checkpoint: {e:?}"
                )))
            })?;

        // Reconstruct the tree using bulk load for O(n) instead of O(n log n)
        *self = serializable.into();

        Ok(())
    }

    /// Generate the checkpoint file path for this multiset.
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        StoragePath::from(format!("{base}/{persistent_id}_osm.dat"))
    }

    /// Clear the multiset state and reset to empty.
    ///
    /// This is called during state management operations like clearing
    /// before a restore or resetting operator state.
    pub fn clear_state(&mut self) {
        self.clear();
    }
}

#[cfg(test)]
#[path = "order_statistics_multiset_tests.rs"]
mod tests;
