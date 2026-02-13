//! Virtual shard parallel routing for OrderStatisticsZSet batch insertion.
//!
//! When the percentile operator runs with multiple DBSP workers and a single key
//! (no GROUP BY, key = `Tup0`), virtual shard routing distributes tree work across
//! workers using shared memory and barriers.
//!
//! # Architecture
//!
//! The owner publishes a shared tree view and leaf ranges via
//! `VirtualShardState` (lock-free shared memory in `runtime.local_store()`).
//! Each worker persistently owns a contiguous range of leaves and merges
//! incoming entries into its local copies. The 2-barrier protocol:
//!
//! 1. **Publish + Partition** (before barrier 1): Owner publishes tree view + leaf ranges.
//!    Workers partition entries by leaf ownership and deposit into shared slots.
//! 2. **Local Mutation** (between barriers): Workers collect entries, bootstrap
//!    missing leaves from the tree view, merge entries, flush to disk.
//! 3. **Reconcile** (after barrier 2, owner only): Owner integrates worker reports,
//!    registers segments, handles splits, updates subtree sums.
//!
//! Workers read the owner's tree structure in place via a raw pointer
//! (`ReadOnlyTreeView`). The owner does not mutate the tree during routing.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rkyv::{Archive, Serialize as RkyvSerialize};
use tokio::sync::Notify;

use crate::algebra::order_statistics::order_statistics_zset::{
    LeafNode, OsmNodeStorage, OrderStatisticsZSet,
};
use crate::algebra::ZWeight;
use crate::node_storage::{
    CachedLeafSummary, LeafLocation, NodeLocation,
};
use crate::node_storage::serialize_to_bytes;
use crate::storage::backend::{StorageBackend, StoragePath};
use crate::storage::file::Serializer;
use size_of::SizeOf;

use crate::{circuit_cache_key, DBData, Runtime, SchedulerError};

// =============================================================================
// ReadOnlyTreeView: Raw pointer to tree storage for parallel routing
// =============================================================================

/// Read-only pointer to a tree's node storage for parallel routing.
///
/// This is a raw pointer — NOT a copy. Workers traverse the owner's actual
/// internal nodes in memory and read evicted leaves from disk.
///
/// # Safety
///
/// The owner must NOT mutate the tree while any `ReadOnlyTreeView` exists.
/// Internal nodes are always in memory. Workers read them via raw pointer.
/// Evicted leaves are read from segment files via `FileReader` (Send + Sync).
pub(crate) struct ReadOnlyTreeView<V> {
    storage_ptr: *const OsmNodeStorage<V>,
    root: Option<NodeLocation>,
    total_leaves: usize,
    max_leaf_entries: usize,
}

// SAFETY: The owner guarantees no mutation while ReadOnlyTreeView exists.
// Internal nodes are read-only. FileReader::read_block() is Send + Sync.
unsafe impl<V: Send> Send for ReadOnlyTreeView<V> {}
unsafe impl<V: Send> Sync for ReadOnlyTreeView<V> {}

#[allow(dead_code)]
impl<V: DBData> ReadOnlyTreeView<V> {
    /// Create from an existing tree. Just stores a raw pointer.
    fn from_tree(tree: &OrderStatisticsZSet<V>) -> Self {
        ReadOnlyTreeView {
            storage_ptr: tree.storage_ptr(),
            root: tree.root(),
            total_leaves: tree.total_leaves(),
            max_leaf_entries: tree.max_leaf_entries(),
        }
    }

    /// Route a value from root to leaf, returning the target leaf ID.
    ///
    /// Traverses all internal node levels using binary search.
    /// Reads internal nodes via raw pointer (no copy, no allocation).
    ///
    /// Returns `None` if the tree is empty or root is a leaf.
    fn deep_route(&self, val: &V) -> Option<usize> {
        let storage = unsafe { &*self.storage_ptr };
        let mut loc = self.root?;

        // If root is a leaf, there are no internal nodes to route through.
        // Return None so entries go to the owner for sequential insertion.
        if loc.is_leaf() {
            return None;
        }

        loop {
            match loc {
                NodeLocation::Internal { id, .. } => {
                    let node = storage.get_internal(id);
                    let child_idx = node.find_child(val);
                    loc = node.children[child_idx];
                }
                NodeLocation::Leaf(leaf_loc) => {
                    return Some(leaf_loc.id);
                }
            }
        }
    }

    /// Clone a leaf from the tree (loading from disk if evicted).
    fn get_leaf_clone(&self, leaf_id: usize) -> LeafNode<V> {
        let storage = unsafe { &*self.storage_ptr };
        let loc = LeafLocation::new(leaf_id);
        if storage.is_leaf_evicted(loc) {
            storage
                .read_leaf_readonly(leaf_id)
                .expect("evicted leaf must be on disk")
        } else {
            storage.get_leaf(loc).clone()
        }
    }

    /// Get total number of leaves in the tree.
    fn total_leaves(&self) -> usize {
        self.total_leaves
    }

    /// Get max entries per leaf.
    fn max_leaf_entries(&self) -> usize {
        self.max_leaf_entries
    }
}

// =============================================================================
// AsyncBarrier: generation-based async barrier for worker synchronization
// =============================================================================

/// Async-friendly barrier using generation counter + Notify.
///
/// All workers call `wait()` each step. The last to arrive bumps the
/// generation and wakes all waiters. Workers register the Notify future
/// BEFORE checking the generation to avoid TOCTOU races.
struct AsyncBarrier {
    count: AtomicUsize,
    generation: AtomicU64,
    total: usize,
    notify: Notify,
}

impl AsyncBarrier {
    fn new(total: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            total,
            notify: Notify::new(),
        }
    }

    async fn wait(&self) {
        let my_gen = self.generation.load(Ordering::Acquire);
        let prev = self.count.fetch_add(1, Ordering::AcqRel);
        if prev + 1 == self.total {
            // Last to arrive: reset counter, advance generation, wake all.
            self.count.store(0, Ordering::Release);
            self.generation.fetch_add(1, Ordering::Release);
            self.notify.notify_waiters();
        } else {
            // Wait for generation to advance.
            loop {
                let notified = self.notify.notified();
                if self.generation.load(Ordering::Acquire) > my_gen {
                    return;
                }
                notified.await;
            }
        }
    }
}


// =============================================================================
// Merge and Split Logic
// =============================================================================

/// Merge sorted entries into a cloned leaf using two-pointer merge, then split if oversized.
///
/// Both the leaf's entries and the incoming entries must be sorted by value.
///
/// Returns: (modified_leaf, splits, weight_delta, key_count_delta)
/// - `modified_leaf`: The leaf after merging entries
/// - `splits`: Vec of (split_key, split_leaf) for oversized leaves
/// - `weight_delta`: Change in total weight
/// - `key_count_delta`: Change in number of distinct keys
pub(crate) fn merge_and_split<V: DBData>(
    leaf: &LeafNode<V>,
    entries: &[(V, ZWeight)],
    max_leaf_entries: usize,
) -> (LeafNode<V>, Vec<(V, LeafNode<V>)>, ZWeight, i64) {
    // Two-pointer merge of sorted leaf entries and sorted new entries
    let mut merged: Vec<(V, ZWeight)> = Vec::with_capacity(leaf.entries.len() + entries.len());
    let mut weight_delta: ZWeight = 0;

    let mut i = 0; // leaf entries index
    let mut j = 0; // new entries index

    while i < leaf.entries.len() && j < entries.len() {
        match leaf.entries[i].0.cmp(&entries[j].0) {
            std::cmp::Ordering::Less => {
                merged.push(leaf.entries[i].clone());
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(entries[j].clone());
                weight_delta += entries[j].1;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                // Duplicate key: sum weights
                let new_weight = leaf.entries[i].1 + entries[j].1;
                merged.push((leaf.entries[i].0.clone(), new_weight));
                weight_delta += entries[j].1;
                i += 1;
                j += 1;
            }
        }
    }

    // Drain remaining
    while i < leaf.entries.len() {
        merged.push(leaf.entries[i].clone());
        i += 1;
    }
    while j < entries.len() {
        merged.push(entries[j].clone());
        weight_delta += entries[j].1;
        j += 1;
    }

    // Consolidate consecutive duplicate keys from new entries
    let mut consolidated: Vec<(V, ZWeight)> = Vec::with_capacity(merged.len());
    for (val, weight) in merged {
        if let Some(last) = consolidated.last_mut() {
            if last.0 == val {
                last.1 += weight;
                continue;
            }
        }
        consolidated.push((val, weight));
    }
    let original_keys = leaf.entries.len();
    let key_count_delta = consolidated.len() as i64 - original_keys as i64;

    let mut result_leaf = LeafNode {
        entries: consolidated,
        next_leaf: leaf.next_leaf,
    };

    // Split if oversized
    let mut splits = Vec::new();
    while result_leaf.entries.len() > max_leaf_entries {
        let mid = result_leaf.entries.len() / 2;
        let right_entries = result_leaf.entries.split_off(mid);
        let split_key = right_entries[0].0.clone();

        let right_leaf = LeafNode {
            entries: right_entries,
            next_leaf: result_leaf.next_leaf.take(),
        };

        splits.push((split_key, right_leaf));
    }

    (result_leaf, splits, weight_delta, key_count_delta)
}

/// Build a CachedLeafSummary for a leaf.
pub(crate) fn build_cached_summary<V: DBData + Archive + RkyvSerialize<Serializer>>(
    leaf: &LeafNode<V>,
) -> CachedLeafSummary {
    let first_key = leaf.entries.first().map(|(k, _)| k.clone());
    let (first_key_bytes, has_first_key) = if let Some(ref key) = first_key {
        (
            Vec::<u8>::from(serialize_to_bytes(key).unwrap_or_default()),
            true,
        )
    } else {
        (Vec::new(), false)
    };
    CachedLeafSummary {
        first_key_bytes,
        has_first_key,
        weight_sum: leaf.total_weight(),
        entry_count: leaf.entries.len(),
    }
}

// =============================================================================
// Route sorted entries to leaf ranges (batch tree traversal)
// =============================================================================

/// Batch-route sorted entries through the tree, returning leaf ranges.
///
/// Traverses the tree once, partitioning the sorted entry slice at each
/// internal node using `partition_point`. Returns `(leaf_id, start, end)`
/// triples where `entries[start..end]` should be merged into that leaf.
///
/// Ranges are in tree-traversal order (= leaf-id order), and entries within
/// each range are sorted (contiguous slices of the sorted input).
///
/// Returns empty vec if tree has no internal nodes (bootstrap/leaf-only),
/// meaning all entries are unrouted.
///
/// Cost: O(D + nodes_visited) vs O(D × H × log B) for per-entry `deep_route`.
fn route_sorted_entry_ranges<V: DBData>(
    entries: &[(V, ZWeight)],
    view: &ReadOnlyTreeView<V>,
) -> Vec<(usize, usize, usize)> {
    let root = match view.root {
        Some(loc) if !loc.is_leaf() => loc,
        _ => return vec![], // all unrouted (bootstrap)
    };

    let storage = unsafe { &*view.storage_ptr };
    let mut ranges: Vec<(usize, usize, usize)> = Vec::new();

    batch_route_collect_ranges(entries, root, storage, 0, &mut ranges);

    ranges
}

/// Recursive helper: partition sorted entry slice at each internal node.
fn batch_route_collect_ranges<V: DBData>(
    entries: &[(V, ZWeight)],
    loc: NodeLocation,
    storage: &OsmNodeStorage<V>,
    base_offset: usize,
    ranges: &mut Vec<(usize, usize, usize)>,
) {
    if entries.is_empty() {
        return;
    }

    match loc {
        NodeLocation::Leaf(leaf_loc) => {
            ranges.push((leaf_loc.id, base_offset, base_offset + entries.len()));
        }
        NodeLocation::Internal { id, .. } => {
            let node = storage.get_internal(id);
            let mut start = 0;
            for (i, key) in node.keys.iter().enumerate() {
                // partition_point: first index where entry.0 >= key
                let split =
                    entries[start..].partition_point(|(v, _)| v < key) + start;
                if split > start {
                    batch_route_collect_ranges(
                        &entries[start..split],
                        node.children[i],
                        storage,
                        base_offset + start,
                        ranges,
                    );
                }
                start = split;
            }
            if start < entries.len() {
                batch_route_collect_ranges(
                    &entries[start..],
                    *node.children.last().unwrap(),
                    storage,
                    base_offset + start,
                    ranges,
                );
            }
        }
    }
}

/// Map a leaf_id to a worker index using the actual published leaf ranges.
///
/// Map a leaf_id to a worker index using the published leaf ranges.
///
/// Respects cascade-rebalanced ranges that may have shifted
/// leaf boundaries between workers.
fn leaf_owner_from_ranges(leaf_id: usize, ranges: &[(usize, usize)]) -> usize {
    for (w, &(start, end)) in ranges.iter().enumerate() {
        if leaf_id >= start && leaf_id < end {
            return w;
        }
    }
    0 // fallback to owner
}


// =============================================================================
// Cascade Rebalancing
// =============================================================================

/// Compute cascade leaf transfers from entry counts.
///
/// Given `entry_counts[w]` = number of entries processed by worker `w` in the
/// previous step, compute how many leaves each adjacent worker pair should transfer
/// to balance workload.
///
/// Returns `transfers[i]` = (from_worker, to_worker, num_leaves_to_transfer).
/// All workers compute the same result deterministically — no coordination needed.
///
/// The cascade algorithm:
/// 1. Compute target = total / num_workers
/// 2. Compute surplus per worker (positive = excess, negative = deficit)
/// 3. Sweep left-to-right: each pair (i, i+1) transfers min(surplus_i, deficit_{i+1}) leaves
#[allow(dead_code)]
pub(crate) fn compute_cascade_transfers(
    entry_counts: &[usize],
    total_leaves: usize,
) -> Vec<(usize, usize, usize)> {
    let num_workers = entry_counts.len();
    if num_workers <= 1 || total_leaves == 0 {
        return Vec::new();
    }

    let total: usize = entry_counts.iter().sum();
    if total == 0 {
        return Vec::new();
    }

    let target = total / num_workers;
    let mut surplus: Vec<i64> = entry_counts
        .iter()
        .map(|&c| c as i64 - target as i64)
        .collect();

    let mut transfers = Vec::new();

    // Left-to-right cascade
    for i in 0..num_workers - 1 {
        if surplus[i] > 0 && surplus[i + 1] < 0 {
            // Worker i has excess, worker i+1 has deficit
            let transfer_amount = surplus[i].min(-surplus[i + 1]);
            if transfer_amount > 0 {
                // Convert entry count transfer to leaf count (proportional)
                let leaves_to_transfer = ((transfer_amount as usize) * total_leaves / total).max(1);
                transfers.push((i, i + 1, leaves_to_transfer));
                surplus[i] -= transfer_amount;
                surplus[i + 1] += transfer_amount;
            }
        } else if surplus[i] < 0 && surplus[i + 1] > 0 {
            // Worker i+1 has excess, worker i has deficit
            let transfer_amount = surplus[i + 1].min(-surplus[i]);
            if transfer_amount > 0 {
                let leaves_to_transfer = ((transfer_amount as usize) * total_leaves / total).max(1);
                transfers.push((i + 1, i, leaves_to_transfer));
                surplus[i] += transfer_amount;
                surplus[i + 1] -= transfer_amount;
            }
        }
    }

    transfers
}

/// Compute updated leaf ranges after applying cascade transfers.
///
/// Given current ranges `[(start, end), ...]` per worker and a list of transfers,
/// produce new ranges. This is deterministic — all workers get the same result.
#[allow(dead_code)]
pub(crate) fn apply_cascade_to_ranges(
    current_ranges: &[(usize, usize)],
    transfers: &[(usize, usize, usize)],
    total_leaves: usize,
) -> Vec<(usize, usize)> {
    let num_workers = current_ranges.len();
    if num_workers == 0 {
        return Vec::new();
    }

    // Start with current leaf counts per worker
    let mut leaf_counts: Vec<usize> = current_ranges
        .iter()
        .map(|(s, e)| if *e > *s { e - s } else { 0 })
        .collect();

    // Apply transfers
    for &(from, to, count) in transfers {
        let actual = count.min(leaf_counts[from]);
        if actual > 0 {
            leaf_counts[from] -= actual;
            leaf_counts[to] += actual;
        }
    }

    // Rebuild contiguous ranges from leaf counts
    let mut ranges = Vec::with_capacity(num_workers);
    let mut offset = 0;
    for count in &leaf_counts {
        let end = (offset + count).min(total_leaves);
        ranges.push((offset, end));
        offset = end;
    }

    ranges
}

/// Compute evenly-distributed leaf ranges for `num_workers` workers.
#[allow(dead_code)]
fn even_leaf_ranges(total_leaves: usize, num_workers: usize) -> Vec<(usize, usize)> {
    if num_workers == 0 || total_leaves == 0 {
        return vec![(0, 0); num_workers];
    }
    let leaves_per_worker = (total_leaves + num_workers - 1) / num_workers;
    (0..num_workers)
        .map(|w| {
            let start = w * leaves_per_worker;
            let end = ((w + 1) * leaves_per_worker).min(total_leaves);
            (start, end)
        })
        .collect()
}

// =============================================================================
// VirtualShardState: Shared state for virtually-sharded routing
// =============================================================================

/// Bundle of leaves transferred between workers during cascade rebalancing.
pub(crate) struct LeafTransferBundle<V> {
    /// Source worker index.
    pub from_worker: usize,
    /// Transferred leaf data.
    pub transfers: Vec<crate::algebra::order_statistics::worker_leaf_storage::LeafTransferData<V>>,
}

/// Shared state for the virtual sharding protocol.
///
/// All workers in the same runtime share the same `Arc<VirtualShardState>`
/// via `runtime.local_store()`. The owner publishes routing data, workers
/// read it, and workers report mutation results back.
pub(crate) struct VirtualShardState<V: 'static> {
    /// Published step data (set by owner, read by workers).
    step_data: Mutex<Option<Arc<VirtualShardStepData<V>>>>,
    /// Generation counter bumped by owner when data is published.
    generation: AtomicU64,
    /// Notify to wake workers waiting for publication.
    notify: Notify,

    /// Redistribute slots: `redistribute_slots[sender][receiver]` = entries for receiver from sender.
    /// Each worker partitions its own entries and deposits into its row; workers collect from their column.
    redistribute_slots: Vec<Vec<Mutex<Option<Vec<(V, ZWeight)>>>>>,
    /// Worker mutation reports: `report_slots[worker]` = report from that worker.
    /// Set by workers during Local Mutation phase, read by owner.
    report_slots: Vec<Mutex<Option<crate::algebra::order_statistics::worker_leaf_storage::WorkerReport<V>>>>,
    /// Transfer slots for cascade rebalancing: `transfer_slots[receiver]` holds incoming leaf bundles.
    /// Workers deposit outgoing leaves before barrier 1; receivers collect after barrier 1.
    transfer_slots: Vec<Mutex<Vec<LeafTransferBundle<V>>>>,

    /// Per-worker entry counts from previous step (for cascade rebalancing).
    /// Updated by owner during reconciliation.
    prev_entry_counts: Mutex<Vec<usize>>,
    /// Previous step's leaf ranges (for cascade rebalancing).
    /// Updated by owner during publication.
    prev_leaf_ranges: Mutex<Vec<(usize, usize)>>,

    /// Barriers for 2-phase synchronization (reduced from 3).
    /// Barrier 1: After partition+transfer / before local mutation.
    barrier_1: AsyncBarrier,
    /// Barrier 2: After local mutation / before reconcile.
    barrier_2: AsyncBarrier,

    /// Number of workers.
    num_workers: usize,
}

impl<V: 'static> VirtualShardState<V> {
    fn new(num_workers: usize) -> Self {
        let redistribute_slots = (0..num_workers)
            .map(|_| (0..num_workers).map(|_| Mutex::new(None)).collect())
            .collect();
        let report_slots = (0..num_workers).map(|_| Mutex::new(None)).collect();
        let transfer_slots = (0..num_workers).map(|_| Mutex::new(Vec::new())).collect();
        Self {
            step_data: Mutex::new(None),
            generation: AtomicU64::new(0),
            notify: Notify::new(),
            redistribute_slots,
            report_slots,
            transfer_slots,
            prev_entry_counts: Mutex::new(vec![0; num_workers]),
            prev_leaf_ranges: Mutex::new(Vec::new()),
            barrier_1: AsyncBarrier::new(num_workers),
            barrier_2: AsyncBarrier::new(num_workers),
            num_workers,
        }
    }

    /// Publish step data (called by tree owner).
    fn publish(&self, data: VirtualShardStepData<V>) {
        *self.step_data.lock().unwrap() = Some(Arc::new(data));
        self.generation.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Publish empty step data (no owner this step).
    fn publish_none(&self) {
        *self.step_data.lock().unwrap() = None;
        self.generation.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait for a new generation and read the step data.
    async fn wait_and_read(&self, last_generation: u64) -> (u64, Option<Arc<VirtualShardStepData<V>>>) {
        loop {
            let notified = self.notify.notified();
            let current_gen = self.generation.load(Ordering::Acquire);
            if current_gen > last_generation {
                let data = self.step_data.lock().unwrap().clone();
                return (current_gen, data);
            }
            notified.await;
        }
    }
}

/// Data published by the tree owner for the virtual shard protocol.
pub(crate) struct VirtualShardStepData<V> {
    /// Shared read-only view of the owner's tree (internal nodes).
    pub tree_view: Arc<ReadOnlyTreeView<V>>,
    /// Worker index of the tree owner.
    pub owner_index: usize,
    /// Worker leaf-range boundaries: `leaf_ranges[w]` = (first_leaf_id, exclusive_end_leaf_id).
    pub worker_leaf_ranges: Vec<(usize, usize)>,
    /// Previous step's leaf ranges (workers need this for deterministic cascade computation).
    pub prev_leaf_ranges: Vec<(usize, usize)>,
    /// Entry counts from previous step (for cascade rebalancing).
    pub prev_entry_counts: Vec<usize>,
    /// Storage backend for workers to write segment files.
    pub storage_backend: Option<Arc<dyn StorageBackend>>,
    /// Segment path prefix.
    pub segment_path_prefix: String,
    /// Spill directory.
    pub spill_directory: Option<StoragePath>,
    /// Base segment ID: worker W uses `segment_id_base + W`.
    /// Owner pre-allocates N segment IDs during Phase 1.
    pub segment_id_base: u64,
}

// SAFETY: ReadOnlyTreeView is Send+Sync, all other fields are Send+Sync.
unsafe impl<V: Send> Send for VirtualShardStepData<V> {}
unsafe impl<V: Send> Sync for VirtualShardStepData<V> {}

// Cache key for VirtualShardState in runtime.local_store()
circuit_cache_key!(local VirtualShardStateId<V>(usize => Arc<VirtualShardState<V>>));

// =============================================================================
// VirtualShardStepResult: Returned from virtual_shard_step to the operator
// =============================================================================

/// Result of a virtual shard step, returned to the operator for
/// optimizing post-step operations.
pub(crate) struct VirtualShardStepResult {
    /// Whether any leaf splits occurred during reconciliation.
    /// When true, the owner's tree may have dirty split leaves that need flushing.
    pub had_splits: bool,
}

// =============================================================================
// VirtualShardRouting: Per-worker routing state
// =============================================================================

/// Per-worker state for the virtual shard routing protocol.
///
/// Created during operator construction when `Runtime::num_workers() > 1`.
/// Each worker holds its own `VirtualShardRouting` instance with a reference
/// to the shared `VirtualShardState`.
pub(crate) struct VirtualShardRouting<V: DBData>
where
    V: Archive + RkyvSerialize<Serializer>,
{
    /// Shared state across all workers.
    shared: Arc<VirtualShardState<V>>,
    /// Per-worker leaf storage.
    pub leaf_storage: crate::algebra::order_statistics::worker_leaf_storage::WorkerLeafStorage<V>,
    /// Last generation seen from the shared state.
    last_generation: u64,
    /// Number of workers.
    num_workers: usize,
    /// This worker's index.
    worker_index: usize,
    /// Number of entries processed in the previous step (for rebalancing).
    prev_entries_processed: usize,
}

impl<V: DBData> SizeOf for VirtualShardRouting<V>
where
    V: Archive + RkyvSerialize<Serializer>,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.leaf_storage.size_of_children(context);
    }
}

impl<V: DBData> VirtualShardRouting<V>
where
    V: Archive + RkyvSerialize<Serializer>,
{
    /// Create virtual shard routing infrastructure if running with multiple workers.
    ///
    /// Returns `None` for single-threaded execution.
    pub fn new() -> Option<Self> {
        let runtime = Runtime::runtime()?;
        let num_workers = Runtime::num_workers();
        if num_workers <= 1 {
            return None;
        }

        let worker_index = Runtime::worker_index();

        // Get or create the shared state via local_store
        let state_id = runtime.sequence_next();
        let shared = runtime
            .local_store()
            .entry(VirtualShardStateId::<V>::new(state_id))
            .or_insert_with(|| Arc::new(VirtualShardState::new(num_workers)))
            .clone();

        // Create per-worker leaf storage (no storage backend yet — set during step)
        let leaf_storage = crate::algebra::order_statistics::worker_leaf_storage::WorkerLeafStorage::new(
            worker_index,
            None,
            String::new(),
            None,
            64 * 1024 * 1024, // 64MB default spill threshold
        );

        Some(Self {
            shared,
            leaf_storage,
            last_generation: 0,
            num_workers,
            worker_index,
            prev_entries_processed: 0,
        })
    }

    /// Execute one step of the virtual shard protocol.
    ///
    /// ALL workers must call this during every `eval()` call.
    ///
    /// 2-barrier protocol with persistent per-worker leaf ownership:
    ///
    /// **Phase 1** (before barrier 1):
    ///   - Owner publishes tree view + leaf ranges + segment_id_base
    ///   - Workers read publication, configure storage
    ///   - Cascade rebalancing: workers compare old vs new owned range,
    ///     deposit outgoing leaves into transfer_slots[receiver]
    ///   - Workers partition entries → redistribute_slots[my][target]
    ///
    /// **Barrier 1**
    ///
    /// **Phase 2** (between barriers):
    ///   - Collect transferred leaves from transfer_slots[my]
    ///   - Bootstrap: clone from tree_view any leaves in owned range not yet in storage
    ///   - Collect entries from redistribute_slots[sender][my] + sort
    ///   - Local mutation: merge entries into persistent leaves
    ///   - Flush dirty leaves to disk, evict clean leaves
    ///   - Build report: leaf_summaries + segment_info + splits + deltas
    ///
    /// **Barrier 2**
    ///
    /// **Phase 3** (after barrier 2, owner only):
    ///   - Owner reconciliation: register_worker_segment, replace_leaf_with_evicted,
    ///     handle splits, apply_subtree_weight_deltas or recalculate_all_subtree_sums, bulk_load unprocessed
    ///
    /// Returns `VirtualShardStepResult` with metadata about what changed,
    /// enabling the operator to skip redundant flush and percentile selection.
    pub async fn virtual_shard_step(
        &mut self,
        tree: Option<&mut OrderStatisticsZSet<V>>,
        local_entries: Vec<(V, ZWeight)>,
    ) -> Result<VirtualShardStepResult, SchedulerError> {
        let worker_index = self.worker_index;
        let num_workers = self.num_workers;
        let is_owner = tree.is_some();

        // =====================================================================
        // Phase 1: Owner publishes tree view + leaf ranges + segment_id_base
        // =====================================================================

        if is_owner {
            let t = tree.as_ref().unwrap();
            let tree_view = Arc::new(ReadOnlyTreeView::from_tree(t));
            let total_leaves = t.total_leaves();

            // Compute leaf ranges using cascade rebalancing from previous step's entry counts
            let prev_counts = self.shared.prev_entry_counts.lock().unwrap().clone();
            let prev_ranges = self.shared.prev_leaf_ranges.lock().unwrap().clone();

            let worker_leaf_ranges = if !prev_ranges.is_empty() && total_leaves > 0 {
                // Apply cascade rebalancing based on previous entry counts
                let transfers = compute_cascade_transfers(&prev_counts, total_leaves);
                if transfers.is_empty() {
                    // No rebalancing needed — recompute even ranges in case tree grew
                    even_leaf_ranges(total_leaves, num_workers)
                } else {
                    apply_cascade_to_ranges(&prev_ranges, &transfers, total_leaves)
                }
            } else {
                // First step or empty tree: evenly divide leaves
                even_leaf_ranges(total_leaves, num_workers)
            };

            // Store ranges for next step's rebalancing
            *self.shared.prev_leaf_ranges.lock().unwrap() = worker_leaf_ranges.clone();

            let storage_backend = t.storage().get_storage_backend();
            let segment_path_prefix = t.storage().segment_path_prefix().to_string();
            let spill_directory = t.storage().spill_directory().cloned();

            // Pre-allocate segment IDs for all workers.
            // Workers use segment_id_base + worker_index as their segment ID.
            // register_worker_segment() in reconciliation will advance next_segment_id past these.
            let segment_id_base = t.storage().next_segment_id();

            // Publish step data
            self.shared.publish(VirtualShardStepData {
                tree_view,
                owner_index: worker_index,
                worker_leaf_ranges,
                prev_leaf_ranges: prev_ranges,
                prev_entry_counts: prev_counts,
                storage_backend,
                segment_path_prefix,
                spill_directory,
                segment_id_base,
            });
        } else if worker_index == 0 {
            // Worker 0 is coordinator. Publish None so workers don't hang.
            self.shared.publish_none();
        }

        // All workers wait for publication
        let (new_gen, step_data) = self
            .shared
            .wait_and_read(self.last_generation)
            .await;
        self.last_generation = new_gen;

        // Configure worker leaf storage from published step data
        if let Some(ref data) = step_data {
            self.leaf_storage.set_storage_backend(data.storage_backend.clone());
            self.leaf_storage.set_segment_path_prefix(data.segment_path_prefix.clone());
            self.leaf_storage.set_spill_directory(data.spill_directory.clone());

            // Set segment ID for this worker
            self.leaf_storage.set_next_segment_id(data.segment_id_base + worker_index as u64);

            // Phase 1b: Cascade rebalancing — transfer leaves between workers
            let my_new_range = if worker_index < data.worker_leaf_ranges.len() {
                data.worker_leaf_ranges[worker_index]
            } else {
                (0, 0)
            };
            let my_old_range = self.leaf_storage.owned_range();

            // If owned range changed, transfer out-of-range leaves to neighbors
            if my_old_range != my_new_range && self.leaf_storage.is_bootstrapped() {
                let mut outgoing: HashMap<usize, Vec<crate::algebra::order_statistics::worker_leaf_storage::LeafTransferData<V>>> = HashMap::new();

                // Find leaves that are no longer in our range
                let owned_ids: Vec<usize> = self.leaf_storage.leaf_ids().collect();
                for leaf_id in owned_ids {
                    if leaf_id < my_new_range.0 || leaf_id >= my_new_range.1 {
                        // Determine which worker should own this leaf
                        let dest = leaf_owner_from_ranges(leaf_id, &data.worker_leaf_ranges);
                        if dest != worker_index {
                            if let Some(transfer) = self.leaf_storage.take_leaf_for_transfer(leaf_id) {
                                outgoing.entry(dest).or_default().push(transfer);
                            }
                        }
                    }
                }

                // Deposit outgoing leaf bundles into transfer_slots[receiver]
                for (dest, transfers) in outgoing {
                    let bundle = LeafTransferBundle {
                        from_worker: worker_index,
                        transfers,
                    };
                    self.shared.transfer_slots[dest].lock().unwrap().push(bundle);
                }
            }

            // Update owned range
            self.leaf_storage.set_owned_range(my_new_range);

            // Phase 1c: Partition entries → redistribute_slots[my_index][target]
            if data.tree_view.root.is_some() && !local_entries.is_empty() {
                let leaf_ranges = route_sorted_entry_ranges(&local_entries, &data.tree_view);

                if leaf_ranges.is_empty() {
                    // Bootstrap (leaf-only root): all entries to owner
                    *self.shared.redistribute_slots[worker_index][data.owner_index]
                        .lock()
                        .unwrap() = Some(local_entries);
                } else {
                    // Partition entries by leaf ownership using the new ranges
                    let mut per_target: Vec<Vec<(V, ZWeight)>> =
                        (0..num_workers).map(|_| Vec::new()).collect();

                    for &(leaf_id, start, end) in &leaf_ranges {
                        let target = leaf_owner_from_ranges(leaf_id, &data.worker_leaf_ranges);
                        per_target[target].extend_from_slice(&local_entries[start..end]);
                    }

                    for (target, entries) in per_target.into_iter().enumerate() {
                        if !entries.is_empty() {
                            *self.shared.redistribute_slots[worker_index][target]
                                .lock()
                                .unwrap() = Some(entries);
                        }
                    }
                }
            } else {
                // Empty tree or no entries: all entries to owner
                if !local_entries.is_empty() {
                    *self.shared.redistribute_slots[worker_index][data.owner_index]
                        .lock()
                        .unwrap() = Some(local_entries);
                }
            }
        } else {
            // No step data: all entries to worker 0
            if !local_entries.is_empty() {
                *self.shared.redistribute_slots[worker_index][0]
                    .lock()
                    .unwrap() = Some(local_entries);
            }
        }

        // === Barrier 1 ===
        self.shared.barrier_1.wait().await;

        if Runtime::kill_in_progress() {
            return Err(SchedulerError::Killed);
        }

        // =====================================================================
        // Phase 2: Collect transfers + bootstrap + collect entries + local mutation
        // =====================================================================

        // Phase 2a: Collect transferred leaves from transfer_slots[my_index]
        {
            let bundles: Vec<LeafTransferBundle<V>> = self.shared.transfer_slots[worker_index]
                .lock()
                .unwrap()
                .drain(..)
                .collect();
            for bundle in bundles {
                for transfer in bundle.transfers {
                    self.leaf_storage.receive_leaf(transfer);
                }
            }
        }

        // Phase 2b: Bootstrap — clone from tree_view any leaves in owned range not yet in storage
        if let Some(ref data) = step_data {
            let my_range = if worker_index < data.worker_leaf_ranges.len() {
                data.worker_leaf_ranges[worker_index]
            } else {
                (0, 0)
            };

            if my_range.1 > my_range.0 && data.tree_view.root.is_some() {
                for leaf_id in my_range.0..my_range.1 {
                    if !self.leaf_storage.owns_leaf(leaf_id) {
                        // Clone from tree view (handles both in-memory and evicted leaves)
                        let leaf = data.tree_view.get_leaf_clone(leaf_id);
                        self.leaf_storage.insert_leaf(leaf_id, leaf);
                    }
                }
                if !self.leaf_storage.is_bootstrapped() {
                    self.leaf_storage.mark_bootstrapped();
                }
            }
        }

        // Phase 2c: Collect entries from redistribute_slots[sender][my_index] + sort
        let mut my_entries: Vec<(V, ZWeight)> = Vec::new();
        for sender in 0..num_workers {
            if let Some(entries) = self.shared.redistribute_slots[sender][worker_index]
                .lock()
                .unwrap()
                .take()
            {
                my_entries.extend(entries);
            }
        }

        // Sort: each sender's chunk is already sorted (from cursor / tree routing),
        // so my_entries is W sorted runs concatenated. TimSort detects sorted runs
        // and merge-sorts them in O(D log W).
        my_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Phase 2d: Local mutation — merge entries into persistent leaves
        let entries_count = my_entries.len();

        use crate::algebra::order_statistics::worker_leaf_storage::{WorkerReport, WorkerSplitInfo};

        let report = if let Some(ref data) = step_data {
            if !my_entries.is_empty() && data.tree_view.root.is_some() {
                let view = &data.tree_view;
                let max_leaf = view.max_leaf_entries();
                let mut total_weight_delta: ZWeight = 0;
                let mut total_key_count_delta: i64 = 0;
                let mut weight_deltas: Vec<(usize, ZWeight)> = Vec::new();
                let mut key_count_deltas: Vec<(usize, i64)> = Vec::new();
                let mut splits: Vec<WorkerSplitInfo<V>> = Vec::new();
                let mut modified_leaf_ids: Vec<usize> = Vec::new();

                // Route entries to individual leaves within our owned range
                let leaf_ranges_for_entries = route_sorted_entry_ranges(&my_entries, view);

                if leaf_ranges_for_entries.is_empty() {
                    // All unrouted (leaf-only root) — pass back to owner
                    WorkerReport {
                        entries_processed: entries_count,
                        unprocessed_entries: my_entries,
                        ..WorkerReport::empty()
                    }
                } else {
                    for &(leaf_id, range_start, range_end) in &leaf_ranges_for_entries {
                        let entries = &my_entries[range_start..range_end];

                        // Get leaf from persistent storage (reload from disk if evicted),
                        // or clone from tree view if not yet in storage.
                        let original = if self.leaf_storage.owns_leaf(leaf_id) {
                            match self.leaf_storage.get_leaf_reloading(leaf_id) {
                                Ok(leaf) => leaf.clone(),
                                Err(_) => view.get_leaf_clone(leaf_id),
                            }
                        } else {
                            view.get_leaf_clone(leaf_id)
                        };

                        let (modified, leaf_splits, wd, kd) =
                            merge_and_split(&original, entries, max_leaf);

                        total_weight_delta += wd;
                        total_key_count_delta += kd;
                        weight_deltas.push((leaf_id, wd));
                        key_count_deltas.push((leaf_id, kd));

                        // Store modified leaf in persistent storage
                        if self.leaf_storage.owns_leaf(leaf_id) {
                            self.leaf_storage.replace_leaf(leaf_id, modified);
                        } else {
                            self.leaf_storage.insert_leaf(leaf_id, modified);
                        }
                        modified_leaf_ids.push(leaf_id);

                        for (split_key, split_leaf) in leaf_splits {
                            splits.push(WorkerSplitInfo {
                                original_leaf_id: leaf_id,
                                split_key,
                                split_leaf,
                            });
                        }
                    }

                    // Phase 2e: Flush dirty leaves to disk, evict clean, build report
                    let has_storage = self.leaf_storage.has_storage_backend();

                    if has_storage {
                        // Persistent path: flush to disk + lightweight summaries
                        let _ = self.leaf_storage.flush_dirty_to_disk();
                        let _ = self.leaf_storage.evict_clean_leaves();

                        // Build leaf summaries for modified leaves
                        let leaf_summaries: Vec<(usize, CachedLeafSummary)> = modified_leaf_ids
                            .iter()
                            .filter_map(|&leaf_id| {
                                self.leaf_storage.get_slot(leaf_id).map(|slot| {
                                    let summary = match slot {
                                        crate::algebra::order_statistics::worker_leaf_storage::WorkerLeafSlot::Present(leaf) => {
                                            build_cached_summary(leaf)
                                        }
                                        crate::algebra::order_statistics::worker_leaf_storage::WorkerLeafSlot::Evicted(s) => {
                                            s.clone()
                                        }
                                    };
                                    (leaf_id, summary)
                                })
                            })
                            .collect();

                        // Build segment info from the most recently flushed segment
                        let segment_info = self.leaf_storage.segments().last().and_then(|seg| {
                            seg.reader.as_ref().map(|reader| {
                                crate::algebra::order_statistics::worker_leaf_storage::WorkerSegmentReport {
                                    segment_id: seg.id,
                                    path: seg.path.clone(),
                                    reader: reader.clone(),
                                    leaf_index: seg.leaf_index.clone(),
                                    file_size: seg.size_bytes,
                                }
                            })
                        });

                        WorkerReport {
                            weight_deltas,
                            key_count_deltas,
                            splits,
                            total_weight_delta,
                            total_key_count_delta,
                            entries_processed: entries_count,
                            unprocessed_entries: Vec::new(),
                            modified_leaves: Vec::new(),
                            leaf_summaries,
                            segment_info,
                        }
                    } else {
                        // Fallback path: send full leaf data in-memory
                        let modified_leaves: Vec<(usize, LeafNode<V>)> = modified_leaf_ids
                            .iter()
                            .filter_map(|&leaf_id| {
                                if self.leaf_storage.owns_leaf(leaf_id) {
                                    Some((leaf_id, self.leaf_storage.get_leaf(leaf_id).clone()))
                                } else {
                                    None
                                }
                            })
                            .collect();

                        WorkerReport {
                            weight_deltas,
                            key_count_deltas,
                            splits,
                            total_weight_delta,
                            total_key_count_delta,
                            entries_processed: entries_count,
                            unprocessed_entries: Vec::new(),
                            modified_leaves,
                            leaf_summaries: Vec::new(),
                            segment_info: None,
                        }
                    }
                }
            } else if !my_entries.is_empty() {
                // Bootstrap case: tree is empty or no step data with valid root.
                // Pass entries back to owner as unprocessed.
                WorkerReport {
                    entries_processed: 0,
                    unprocessed_entries: my_entries,
                    ..WorkerReport::empty()
                }
            } else {
                WorkerReport::empty()
            }
        } else {
            // No step data at all — pass entries back if any
            if !my_entries.is_empty() {
                WorkerReport {
                    entries_processed: 0,
                    unprocessed_entries: my_entries,
                    ..WorkerReport::empty()
                }
            } else {
                WorkerReport::empty()
            }
        };

        self.prev_entries_processed = entries_count;

        // Publish report
        *self.shared.report_slots[worker_index].lock().unwrap() = Some(report);

        // === Barrier 2 ===
        self.shared.barrier_2.wait().await;

        if Runtime::kill_in_progress() {
            return Err(SchedulerError::Killed);
        }

        // =====================================================================
        // Phase 3: Owner reconciliation
        // =====================================================================

        drop(step_data);

        if let Some(tree) = tree {
            // Read all worker reports
            let mut all_unprocessed: Vec<(V, ZWeight)> = Vec::new();
            let mut total_weight_delta: ZWeight = 0;
            let mut total_key_count_delta: i64 = 0;
            let mut had_splits = false;
            let mut entry_counts = vec![0usize; num_workers];
            // Collect per-leaf weight deltas for incremental subtree sum updates
            let mut leaf_deltas: HashMap<usize, ZWeight> = HashMap::new();

            for w in 0..num_workers {
                if let Some(report) = self.shared.report_slots[w].lock().unwrap().take() {
                    total_weight_delta += report.total_weight_delta;
                    total_key_count_delta += report.total_key_count_delta;
                    entry_counts[w] = report.entries_processed;

                    // Collect per-leaf weight deltas for incremental subtree sum path
                    for &(leaf_id, delta) in &report.weight_deltas {
                        *leaf_deltas.entry(leaf_id).or_insert(0) += delta;
                    }

                    // Collect unprocessed entries from all workers
                    if !report.unprocessed_entries.is_empty() {
                        all_unprocessed.extend(report.unprocessed_entries);
                    }

                    // Persistent path: register worker segment + mark leaves as evicted
                    if let Some(seg) = report.segment_info {
                        tree.storage_mut().register_worker_segment(
                            seg.segment_id,
                            seg.path,
                            seg.reader,
                            seg.leaf_index,
                            seg.file_size,
                        );
                    }

                    for (leaf_id, summary) in report.leaf_summaries {
                        tree.storage_mut().replace_leaf_with_evicted(leaf_id, summary);
                    }

                    // Fallback path: replace leaf data in-memory
                    for (leaf_id, new_leaf) in report.modified_leaves {
                        tree.storage_mut().replace_leaf_data(leaf_id, new_leaf);
                    }

                    // Handle split leaves from workers
                    for split_info in report.splits {
                        had_splits = true;
                        let split_leaf = split_info.split_leaf;
                        let loc = tree.storage_mut().alloc_leaf(split_leaf);
                        let leaf_loc = loc.as_leaf().expect("alloc_leaf returns Leaf");
                        tree.insert_split_child(split_info.split_key, loc);

                        // Update the original leaf's next_leaf pointer.
                        // The original leaf may now be evicted (worker wrote it to disk
                        // and we called replace_leaf_with_evicted above). If so, reload
                        // from disk to fix next_leaf, then mark dirty.
                        let orig_loc = LeafLocation::new(split_info.original_leaf_id);
                        if tree.storage().is_leaf_in_memory(orig_loc) {
                            tree.storage_mut().get_leaf_mut(orig_loc).next_leaf = Some(leaf_loc);
                        } else if tree.storage().is_leaf_evicted(orig_loc) {
                            // Reload from disk (now readable from worker's registered segment)
                            if tree.storage_mut().load_leaf_from_disk(orig_loc).is_ok() {
                                tree.storage_mut().get_leaf_mut(orig_loc).next_leaf = Some(leaf_loc);
                            }
                        }
                    }
                }
            }

            // Store entry counts for next step's cascade rebalancing
            *self.shared.prev_entry_counts.lock().unwrap() = entry_counts;

            // Advance owner's segment ID counter past all worker segment IDs
            // (register_worker_segment already updates next_segment_id)

            // Update totals from processed entries
            if total_weight_delta != 0 {
                tree.update_weight_delta(total_weight_delta);
            }
            if total_key_count_delta != 0 {
                tree.update_key_count_delta(total_key_count_delta);
            }

            // Update subtree sums: use fast delta path when no structural changes,
            // full recalculation when splits occurred (new leaves have placeholder zeros).
            if had_splits {
                tree.recalculate_all_subtree_sums();
            } else if !leaf_deltas.is_empty() {
                tree.apply_subtree_weight_deltas(&leaf_deltas);
            }

            // Handle unprocessed entries (bootstrap or leaf-only root)
            if !all_unprocessed.is_empty() {
                if tree.root().is_none() {
                    // Bootstrap: bulk load all entries into a new tree
                    all_unprocessed.sort_by(|(a, _), (b, _)| a.cmp(b));
                    // Consolidate duplicates
                    let mut consolidated: Vec<(V, ZWeight)> =
                        Vec::with_capacity(all_unprocessed.len());
                    for (val, weight) in all_unprocessed {
                        if let Some(last) = consolidated.last_mut() {
                            if last.0 == val {
                                last.1 += weight;
                                continue;
                            }
                        }
                        consolidated.push((val, weight));
                    }
                    tree.bulk_load_sorted(consolidated);
                } else {
                    // Tree exists but entries couldn't be routed (leaf-only root).
                    // Insert sequentially.
                    for (val, weight) in all_unprocessed {
                        tree.insert(val, weight);
                    }
                }
            }

            return Ok(VirtualShardStepResult {
                had_splits,
            });
        }

        Ok(VirtualShardStepResult {
            had_splits: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::order_statistics::order_statistics_zset::OrderStatisticsZSet;
    use crate::algebra::F64;
    use crate::node_storage::NodeStorageConfig;

    #[test]
    fn test_deep_route_and_routing() {
        // Build a tree large enough to have internal nodes
        let config = NodeStorageConfig::default();
        let mut tree: OrderStatisticsZSet<F64> =
            OrderStatisticsZSet::with_config(64, config);

        // Insert enough values to create internal nodes
        for i in 0..500 {
            tree.insert(F64::new(i as f64), 1);
        }

        // Tree should have internal nodes now
        let root = tree.root();
        assert!(root.is_some());
        assert!(!root.unwrap().is_leaf(), "Tree should have internal nodes with 500 entries");

        // Create a tree view and route entries
        let view = ReadOnlyTreeView::from_tree(&tree);
        assert!(view.total_leaves() > 1);

        // Route known values and verify they map to valid leaf IDs
        for i in 0..500 {
            let val = F64::new(i as f64);
            let leaf_id = view.deep_route(&val);
            assert!(leaf_id.is_some(), "Value {} should route to a leaf", i);
            assert!(
                leaf_id.unwrap() < view.total_leaves(),
                "Leaf ID should be within range"
            );
        }

        // Route new values (not in tree yet) - they should still route to valid leaves
        let leaf_id = view.deep_route(&F64::new(-1.0));
        assert!(leaf_id.is_some());
        let leaf_id = view.deep_route(&F64::new(999.0));
        assert!(leaf_id.is_some());
    }

    #[test]
    fn test_route_sorted_entry_ranges() {
        let config = NodeStorageConfig::default();
        let mut tree: OrderStatisticsZSet<F64> =
            OrderStatisticsZSet::with_config(64, config);

        for i in 0..500 {
            tree.insert(F64::new(i as f64), 1);
        }

        let view = ReadOnlyTreeView::from_tree(&tree);

        // Entries must be sorted for batch routing
        let entries: Vec<(F64, ZWeight)> = (0..100)
            .map(|i| (F64::new(i as f64 * 5.0), 1))
            .collect();

        let ranges = route_sorted_entry_ranges(&entries, &view);

        // All entries should be routed (tree has internal nodes)
        let total_routed: usize = ranges.iter().map(|&(_, s, e)| e - s).sum();
        assert_eq!(total_routed, 100);

        // Ranges should be non-overlapping and cover all entries
        let mut covered = 0;
        for &(leaf_id, start, end) in &ranges {
            assert!(start <= end);
            assert!(leaf_id < view.total_leaves());
            covered += end - start;
        }
        assert_eq!(covered, 100);

        // Ranges should be in leaf-id order (tree traversal order)
        for i in 1..ranges.len() {
            assert!(ranges[i - 1].0 < ranges[i].0);
        }

        // Entries within each range should be sorted
        for &(_, start, end) in &ranges {
            for j in start + 1..end {
                assert!(entries[j - 1].0 <= entries[j].0);
            }
        }
    }

    #[test]
    fn test_route_sorted_entry_ranges_leaf_only() {
        // Tree with only a leaf root — returns empty ranges (all unrouted)
        let config = NodeStorageConfig::default();
        let mut tree: OrderStatisticsZSet<F64> =
            OrderStatisticsZSet::with_config(64, config);

        // Insert just a few values (stays as single leaf)
        for i in 0..5 {
            tree.insert(F64::new(i as f64), 1);
        }

        let view = ReadOnlyTreeView::from_tree(&tree);

        let entries: Vec<(F64, ZWeight)> = (0..10)
            .map(|i| (F64::new(i as f64), 1))
            .collect();

        let ranges = route_sorted_entry_ranges(&entries, &view);

        // All entries should be unrouted (single leaf root)
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_merge_no_split() {
        // Create a leaf with some entries
        let leaf = LeafNode {
            entries: vec![
                (F64::new(10.0), 1),
                (F64::new(20.0), 2),
                (F64::new(30.0), 3),
            ],
            next_leaf: None,
        };

        // Merge new entries that don't cause a split
        let entries = vec![(F64::new(15.0), 1), (F64::new(25.0), 2)];

        let (result, splits, weight_delta, key_delta) = merge_and_split(&leaf, &entries, 64);

        assert!(splits.is_empty());
        assert_eq!(result.entries.len(), 5);
        assert_eq!(weight_delta, 3); // 1 + 2
        assert_eq!(key_delta, 2); // 2 new keys
        // Verify sorted order
        for i in 1..result.entries.len() {
            assert!(result.entries[i - 1].0 < result.entries[i].0);
        }
    }

    #[test]
    fn test_merge_duplicate_keys() {
        let leaf = LeafNode {
            entries: vec![
                (F64::new(10.0), 3),
                (F64::new(20.0), 2),
            ],
            next_leaf: None,
        };

        let entries = vec![(F64::new(10.0), 5), (F64::new(30.0), 1)];

        let (result, splits, weight_delta, key_delta) = merge_and_split(&leaf, &entries, 64);

        assert!(splits.is_empty());
        assert_eq!(result.entries.len(), 3); // 10, 20, 30
        assert_eq!(result.entries[0], (F64::new(10.0), 8)); // 3 + 5
        assert_eq!(weight_delta, 6); // 5 + 1
        assert_eq!(key_delta, 1); // 1 new key (30.0)
    }

    #[test]
    fn test_merge_with_single_split() {
        // Create a leaf near capacity
        let entries: Vec<(F64, ZWeight)> = (0..4)
            .map(|i| (F64::new(i as f64 * 10.0), 1))
            .collect();
        let leaf = LeafNode {
            entries,
            next_leaf: None,
        };

        // Add entries that cause a split (max_leaf_entries = 4)
        let new_entries = vec![
            (F64::new(5.0), 1),
            (F64::new(15.0), 1),
            (F64::new(25.0), 1),
        ];

        let (result, splits, weight_delta, key_delta) = merge_and_split(&leaf, &new_entries, 4);

        // Should have split: total 7 entries, max 4 → split into ~3 and ~4
        assert!(!splits.is_empty());
        // Total entries across result + splits should equal 7
        let total: usize = result.entries.len()
            + splits.iter().map(|(_, l)| l.entries.len()).sum::<usize>();
        assert_eq!(total, 7);
        assert_eq!(weight_delta, 3);
        assert_eq!(key_delta, 3);
    }

    #[test]
    fn test_merge_empty_entries() {
        let leaf = LeafNode {
            entries: vec![
                (F64::new(10.0), 1),
                (F64::new(20.0), 2),
            ],
            next_leaf: Some(LeafLocation::new(42)),
        };

        let (result, splits, weight_delta, key_delta) = merge_and_split(&leaf, &[], 64);

        assert!(splits.is_empty());
        assert_eq!(result.entries.len(), 2);
        assert_eq!(weight_delta, 0);
        assert_eq!(key_delta, 0);
        assert_eq!(result.next_leaf, Some(LeafLocation::new(42)));
    }

    #[test]
    fn test_merge_with_multiple_splits() {
        // Create a small leaf
        let leaf = LeafNode {
            entries: vec![(F64::new(50.0), 1)],
            next_leaf: None,
        };

        // Add many entries causing multiple splits (max_leaf_entries = 3)
        let new_entries: Vec<(F64, ZWeight)> = (0..10)
            .map(|i| (F64::new(i as f64), 1))
            .collect();

        let (result, splits, weight_delta, key_delta) = merge_and_split(&leaf, &new_entries, 3);

        // Total should be 11 entries (1 original + 10 new)
        let total: usize = result.entries.len()
            + splits.iter().map(|(_, l)| l.entries.len()).sum::<usize>();
        assert_eq!(total, 11);
        // Should have multiple splits
        assert!(splits.len() >= 2);
        assert_eq!(weight_delta, 10);
        assert_eq!(key_delta, 10);
    }

    #[test]
    fn test_async_barrier() {
        // Test barrier with a single "worker" (total=1): should pass immediately
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let barrier = AsyncBarrier::new(1);
            barrier.wait().await;
            barrier.wait().await; // Second wait should also work (generation advances)
        });
    }

    #[test]
    fn test_cascade_no_transfer_balanced() {
        // All workers have equal load — no transfers needed
        let counts = vec![100, 100, 100, 100];
        let transfers = compute_cascade_transfers(&counts, 20);
        assert!(transfers.is_empty());
    }

    #[test]
    fn test_cascade_transfer_left_heavy() {
        // Worker 0 has 3x the load of others
        let counts = vec![300, 100, 100, 100];
        let transfers = compute_cascade_transfers(&counts, 20);
        // Worker 0 should transfer to worker 1
        assert!(!transfers.is_empty());
        assert_eq!(transfers[0].0, 0); // from worker 0
        assert_eq!(transfers[0].1, 1); // to worker 1
        assert!(transfers[0].2 > 0); // some leaves
    }

    #[test]
    fn test_cascade_transfer_right_heavy() {
        // Worker 3 has excess, worker 2 has deficit
        let counts = vec![50, 50, 50, 250];
        let transfers = compute_cascade_transfers(&counts, 20);
        assert!(!transfers.is_empty());
        // Worker 3 should transfer to worker 2
        let has_3_to_2 = transfers.iter().any(|&(from, to, _)| from == 3 && to == 2);
        assert!(has_3_to_2);
    }

    #[test]
    fn test_cascade_empty_counts() {
        let counts = vec![0, 0, 0, 0];
        let transfers = compute_cascade_transfers(&counts, 20);
        assert!(transfers.is_empty());
    }

    #[test]
    fn test_cascade_single_worker() {
        let counts = vec![100];
        let transfers = compute_cascade_transfers(&counts, 10);
        assert!(transfers.is_empty());
    }

    #[test]
    fn test_even_leaf_ranges() {
        let ranges = even_leaf_ranges(10, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 3));
        assert_eq!(ranges[1], (3, 6));
        assert_eq!(ranges[2], (6, 9));
        assert_eq!(ranges[3], (9, 10));
    }

    #[test]
    fn test_even_leaf_ranges_exact() {
        let ranges = even_leaf_ranges(8, 4);
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (0, 2));
        assert_eq!(ranges[1], (2, 4));
        assert_eq!(ranges[2], (4, 6));
        assert_eq!(ranges[3], (6, 8));
    }

    #[test]
    fn test_apply_cascade_to_ranges() {
        let initial = vec![(0, 5), (5, 10), (10, 15), (15, 20)];
        // Transfer 2 leaves from worker 0 to worker 1
        let transfers = vec![(0, 1, 2)];
        let result = apply_cascade_to_ranges(&initial, &transfers, 20);
        assert_eq!(result.len(), 4);
        // Worker 0 loses 2, worker 1 gains 2
        assert_eq!(result[0].1 - result[0].0, 3); // 5 - 2 = 3
        assert_eq!(result[1].1 - result[1].0, 7); // 5 + 2 = 7
    }
}
