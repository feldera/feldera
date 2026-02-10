//! Shared-memory parallel routing for OrderStatisticsZSet batch insertion.
//!
//! When the percentile operator runs with multiple DBSP workers and a single key
//! (no GROUP BY, key = `Tup0`), parallel routing distributes tree work across
//! idle workers using shared memory and barriers instead of Exchange.
//!
//! # Architecture
//!
//! The owner publishes a shared tree view and separator keys via
//! `SharedPercentileState` (lock-free shared memory in `runtime.local_store()`).
//! Workers partition their sorted local entries by value range (using binary
//! search on separator keys), then exchange entries through shared-memory slots
//! with barrier synchronization — no Exchange cloning or retry loops.
//!
//! 1. **Publish**: Owner publishes tree view + separator keys via shared memory
//! 2. **Partition**: Workers binary-search entries into per-worker slices
//! 3. **Redistribute**: Shared-memory slots + barrier (replaces Exchange)
//! 4. **Merge**: Workers clone leaves, merge entries, write to disk
//! 5. **Gather**: Shared-memory slots + barrier (replaces Exchange)
//! 6. **Reconcile**: Owner integrates worker results
//!
//! Workers read the owner's tree structure in place via a raw pointer
//! (`ReadOnlyTreeView`). The owner does not mutate the tree during routing.
//!
//! Workers clone their assigned leaves, merge sorted entries using O(K+M)
//! two-pointer merge, handle splits for oversized leaves, and write modified
//! original leaves directly to disk. Split leaves are sent back in-memory
//! since the owner must allocate their leaf IDs.

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
    CachedLeafSummary, LeafLocation, NodeLocation, SegmentId,
};
use crate::node_storage::serialize_to_bytes;
use crate::storage::backend::{FileReader, StorageBackend, StoragePath};
use crate::storage::file::Serializer;
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
// SharedExchangeSlots: shared-memory inter-worker data exchange
// =============================================================================

/// Shared-memory slots replacing Exchange for inter-worker communication.
///
/// Workers write to slots indexed by [sender][receiver], hit a barrier,
/// then read from slots indexed by [sender][my_index]. This eliminates
/// Exchange's CAS retry loops, payload cloning, and notification overhead.
struct SharedExchangeSlots<V> {
    /// Redistribute slots: `slots[sender][receiver]` = entries for receiver from sender.
    redistribute_slots: Vec<Vec<Mutex<Option<Vec<(V, ZWeight)>>>>>,
    /// Gather slots: `slots[sender]` = LeafWriteResult from that worker.
    gather_slots: Vec<Mutex<Option<LeafWriteResult<V>>>>,
    /// Barrier after redistribute writes (ensures all writers done before reads).
    redistribute_barrier: AsyncBarrier,
    /// Barrier after gather writes (ensures all writers done before owner reads).
    gather_barrier: AsyncBarrier,
}

impl<V: Clone + Send + 'static> SharedExchangeSlots<V> {
    fn new(num_workers: usize) -> Self {
        let redistribute_slots = (0..num_workers)
            .map(|_| (0..num_workers).map(|_| Mutex::new(None)).collect())
            .collect();
        let gather_slots = (0..num_workers).map(|_| Mutex::new(None)).collect();
        Self {
            redistribute_slots,
            gather_slots,
            redistribute_barrier: AsyncBarrier::new(num_workers),
            gather_barrier: AsyncBarrier::new(num_workers),
        }
    }
}

// Cache key for SharedExchangeSlots in runtime.local_store()
circuit_cache_key!(local SharedExchangeSlotsId<V>(usize => Arc<SharedExchangeSlots<V>>));

// =============================================================================
// SharedPercentileState: Shared-memory tree view publication
// =============================================================================

/// Data published by the tree owner for workers to read.
pub(crate) struct StepData<V> {
    /// Shared read-only view of the owner's tree.
    pub(crate) tree_view: Arc<ReadOnlyTreeView<V>>,
    /// Separator keys defining worker leaf-range boundaries.
    /// `separator_keys[i]` is the first key of the first leaf owned by worker `i+1`.
    /// Workers binary-search their sorted entries on these to partition by destination.
    pub(crate) separator_keys: Vec<V>,
    /// Worker index of the tree owner.
    pub(crate) owner_index: usize,
    /// Storage backend for workers to write segment files.
    pub(crate) storage_backend: Option<Arc<dyn StorageBackend>>,
    /// Pre-allocated segment IDs for each worker.
    pub(crate) worker_segment_ids: Vec<SegmentId>,
    /// Segment path prefix for generating unique segment file paths.
    pub(crate) segment_path_prefix: String,
    /// Spill directory for segment files.
    pub(crate) spill_directory: Option<StoragePath>,
}

// SAFETY: ReadOnlyTreeView is Send+Sync, all other fields are Send+Sync.
unsafe impl<V: Send> Send for StepData<V> {}
unsafe impl<V: Send> Sync for StepData<V> {}

/// Shared-memory publication mechanism for step data.
///
/// All workers in the same runtime share the same `Arc<SharedPercentileState>`
/// via `runtime.local_store()`. The owner publishes tree view data, and workers
/// wait on a generation counter to read it.
pub(crate) struct SharedPercentileState<V> {
    /// Published step data (set by owner, read by workers).
    step_data: Mutex<Option<Arc<StepData<V>>>>,
    /// Generation counter bumped by owner when data is published.
    generation: AtomicU64,
    /// Notify to wake workers waiting for publication.
    notify: Notify,
}

impl<V> SharedPercentileState<V> {
    fn new() -> Self {
        Self {
            step_data: Mutex::new(None),
            generation: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    /// Publish step data (called by tree owner).
    fn publish(&self, data: StepData<V>) {
        *self.step_data.lock().unwrap() = Some(Arc::new(data));
        self.generation.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Publish empty step data (called by coordinator when no owner exists).
    fn publish_none(&self) {
        *self.step_data.lock().unwrap() = None;
        self.generation.fetch_add(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Wait for a new generation and read the step data.
    async fn wait_and_read(&self, last_generation: u64) -> (u64, Option<Arc<StepData<V>>>) {
        loop {
            // Register the waiter BEFORE checking the condition to avoid
            // TOCTOU race where notification fires between check and wait.
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

// Cache key for SharedPercentileState in runtime.local_store()
circuit_cache_key!(local SharedPercentileStateId<V>(usize => Arc<SharedPercentileState<V>>));

// =============================================================================
// Payload Types (for gather phase)
// =============================================================================

/// Per-leaf disk metadata for leaves written to disk by workers.
#[derive(Clone)]
pub(crate) struct DiskLeafInfo {
    pub(crate) leaf_id: usize,
    pub(crate) cached_summary: CachedLeafSummary,
}

/// Split leaf data sent in-memory from worker to owner.
#[derive(Clone)]
pub(crate) struct SplitLeafInfo<V> {
    /// Which leaf this was split from.
    pub(crate) original_leaf_id: usize,
    /// First key of the split leaf (becomes separator key in parent).
    pub(crate) split_key: V,
    /// The split leaf data (in-memory, owner allocates ID).
    pub(crate) leaf: LeafNode<V>,
}

/// Worker segment file info.
#[derive(Clone)]
pub(crate) struct WorkerSegmentInfo {
    pub(crate) segment_id: SegmentId,
    pub(crate) reader: Arc<dyn FileReader>,
    pub(crate) leaf_index: HashMap<usize, (u64, u32)>,
    pub(crate) file_size: u64,
    pub(crate) path: StoragePath,
}

/// Result of a worker's leaf merge + write operations.
#[derive(Clone)]
pub(crate) struct LeafWriteResult<V> {
    /// Per-leaf disk metadata for leaves written to disk.
    pub(crate) disk_leaves: Vec<DiskLeafInfo>,
    /// Split leaves sent in-memory (owner allocates IDs).
    pub(crate) split_leaves: Vec<SplitLeafInfo<V>>,
    /// Worker's segment file info (None if worker had no leaves to process).
    pub(crate) segment: Option<WorkerSegmentInfo>,
    /// Total weight delta across all processed leaves.
    pub(crate) total_weight_delta: ZWeight,
    /// Total key count delta across all processed leaves.
    pub(crate) total_key_count_delta: i64,
    /// Unrouted entries that deep_route couldn't handle (bootstrap/edge case).
    /// Owner inserts these sequentially after reconciliation.
    pub(crate) unrouted_entries: Vec<(V, ZWeight)>,
}

impl<V> LeafWriteResult<V> {
    fn empty() -> Self {
        Self {
            disk_leaves: vec![],
            split_leaves: vec![],
            segment: None,
            total_weight_delta: 0,
            total_key_count_delta: 0,
            unrouted_entries: vec![],
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

/// Map a leaf_id to a worker index using contiguous range assignment.
fn leaf_owner(leaf_id: usize, total_leaves: usize, num_workers: usize) -> usize {
    if total_leaves == 0 || num_workers == 0 {
        return 0;
    }
    let leaves_per_worker = (total_leaves + num_workers - 1) / num_workers;
    (leaf_id / leaves_per_worker).min(num_workers - 1)
}

// =============================================================================
// Separator key extraction and entry partitioning
// =============================================================================

/// Extract worker boundary separator keys from the tree.
///
/// Returns `num_workers - 1` keys (or fewer if the tree has fewer leaves than
/// workers). `boundaries[i]` is the first key of the first leaf in worker
/// `i+1`'s range. Workers binary-search their sorted entries on these keys
/// to partition entries by destination worker.
///
/// Cost: reads at most `num_workers - 1` leaves (from disk if evicted).
fn extract_worker_boundaries<V: DBData>(
    tree: &OrderStatisticsZSet<V>,
    num_workers: usize,
) -> Vec<V> {
    let total_leaves = tree.total_leaves();
    if total_leaves <= 1 || num_workers <= 1 {
        return vec![];
    }
    let leaves_per_worker = (total_leaves + num_workers - 1) / num_workers;
    let mut boundaries = Vec::with_capacity(num_workers - 1);
    let storage = tree.storage();

    for w in 1..num_workers {
        let boundary_leaf_id = leaves_per_worker * w;
        if boundary_leaf_id >= total_leaves {
            break;
        }
        let loc = LeafLocation::new(boundary_leaf_id);
        let first_key = if storage.is_leaf_evicted(loc) {
            storage
                .read_leaf_readonly(boundary_leaf_id)
                .and_then(|leaf| leaf.entries.first().map(|(k, _)| k.clone()))
        } else {
            storage.get_leaf(loc).entries.first().map(|(k, _)| k.clone())
        };
        if let Some(key) = first_key {
            boundaries.push(key);
        }
    }
    boundaries
}

/// Partition entries into per-worker Vecs using binary search on separator keys.
///
/// Entries must be sorted by value. Returns a Vec of `num_workers` Vecs,
/// where `result[w]` contains entries destined for worker `w`.
///
/// Cost: O(entries × log(num_workers)) — one binary search per entry on
/// at most `num_workers - 1` separator keys, vs O(entries × tree_height)
/// for per-entry deep_route.
fn partition_entries_to_workers<V: Ord + Clone>(
    entries: Vec<(V, ZWeight)>,
    separator_keys: &[V],
    num_workers: usize,
) -> Vec<Vec<(V, ZWeight)>> {
    if separator_keys.is_empty() {
        // No boundaries: all entries go to worker 0
        let mut result = vec![vec![]; num_workers];
        result[0] = entries;
        return result;
    }

    let mut result: Vec<Vec<(V, ZWeight)>> = (0..num_workers).map(|_| Vec::new()).collect();
    for entry in entries {
        // partition_point returns first index where separator > entry.0,
        // which equals the number of separators <= entry.0 = destination worker.
        let worker = separator_keys.partition_point(|sep| sep <= &entry.0);
        let worker = worker.min(num_workers - 1);
        result[worker].push(entry);
    }
    result
}

// =============================================================================
// ParallelRouting: Shared-memory parallel routing for percentile operator
// =============================================================================

/// Shared-memory parallel routing within the percentile operator.
///
/// Created during operator construction when `Runtime::num_workers() > 1`.
/// Uses shared-memory tree view publication + shared-memory slots with
/// barrier synchronization for redistribute/gather phases.
///
/// All workers participate in all barriers during every `eval()` call,
/// even if they have no data.
pub(crate) struct ParallelRouting<V: Ord + Clone + Send + 'static> {
    shared_state: Arc<SharedPercentileState<V>>,
    shared_slots: Arc<SharedExchangeSlots<V>>,
    last_generation: u64,
    num_workers: usize,
    worker_index: usize,
}

impl<V: DBData> ParallelRouting<V>
where
    V: Archive + RkyvSerialize<Serializer>,
{
    /// Create parallel routing infrastructure if running with multiple workers.
    ///
    /// Returns `None` for single-threaded execution.
    pub fn new() -> Option<Self> {
        let runtime = Runtime::runtime()?;
        let num_workers = Runtime::num_workers();
        if num_workers <= 1 {
            return None;
        }

        let worker_index = Runtime::worker_index();

        // Get or create the shared state via local_store (all workers share same Arc)
        let state_id = runtime.sequence_next();
        let shared_state = runtime
            .local_store()
            .entry(SharedPercentileStateId::<V>::new(state_id))
            .or_insert_with(|| Arc::new(SharedPercentileState::new()))
            .clone();

        // Get or create the shared exchange slots
        let slots_id = runtime.sequence_next();
        let shared_slots = runtime
            .local_store()
            .entry(SharedExchangeSlotsId::<V>::new(slots_id))
            .or_insert_with(|| Arc::new(SharedExchangeSlots::new(num_workers)))
            .clone();

        Some(Self {
            shared_state,
            shared_slots,
            last_generation: 0,
            num_workers,
            worker_index,
        })
    }

    /// Execute one round of parallel routing.
    ///
    /// ALL workers must call this method during every `eval()` call.
    /// The owner provides a tree and entries; non-owners provide `None`/empty.
    ///
    /// In the unsharded case (Tup0 key), each worker provides its own
    /// `local_entries` from round-robin distribution, and only the tree owner
    /// provides `tree`.
    pub async fn parallel_step(
        &mut self,
        mut tree: Option<&mut OrderStatisticsZSet<V>>,
        local_entries: Vec<(V, ZWeight)>,
    ) -> Result<(), SchedulerError> {
        let num_workers = self.num_workers;
        let worker_index = self.worker_index;
        let is_owner = tree.is_some();

        // =====================================================================
        // Phase 1: Owner publishes tree view + separator keys (no Exchange)
        // =====================================================================

        if is_owner {
            // Use a single &mut reborrow for both immutable reads and mutable ops.
            // Immutable reads happen first (from_tree, extract_worker_boundaries, etc.)
            // then mutable allocate_segment_id. All return owned values, so no
            // outstanding borrows remain after each statement.
            let t = tree.as_deref_mut().unwrap();

            let tree_view = Arc::new(ReadOnlyTreeView::from_tree(t));
            let separator_keys = extract_worker_boundaries(t, num_workers);
            let storage_backend = t.storage().get_storage_backend();
            let segment_path_prefix = t.storage().segment_path_prefix().to_string();
            let spill_directory = t.storage().spill_directory().cloned();

            // Pre-allocate segment IDs (mutable access — all prior borrows released)
            let worker_segment_ids: Vec<SegmentId> = (0..num_workers)
                .map(|_| t.storage_mut().allocate_segment_id())
                .collect();

            self.shared_state.publish(StepData {
                tree_view,
                separator_keys,
                owner_index: worker_index,
                storage_backend,
                worker_segment_ids,
                segment_path_prefix,
                spill_directory,
            });
        } else if worker_index == 0 {
            // Worker 0 is coordinator. Publish None so workers don't hang.
            self.shared_state.publish_none();
        }

        // All workers wait for publication
        let (new_gen, step_data) = self
            .shared_state
            .wait_and_read(self.last_generation)
            .await;
        self.last_generation = new_gen;

        // =====================================================================
        // Phase 2: Partition entries by worker boundaries (binary search)
        // =====================================================================

        let (per_worker_entries, _owner_index) = if let Some(ref data) = step_data {
            let has_storage = data.storage_backend.is_some();
            if has_storage && !data.separator_keys.is_empty() {
                // Normal case: partition by separator keys
                let parts = partition_entries_to_workers(
                    local_entries,
                    &data.separator_keys,
                    num_workers,
                );
                (parts, data.owner_index)
            } else {
                // No storage or no separator keys (bootstrap): all to owner
                let mut parts = vec![vec![]; num_workers];
                parts[data.owner_index] = local_entries;
                (parts, data.owner_index)
            }
        } else {
            // No step data (multi-key barrier-only): all to worker 0
            let mut parts = vec![vec![]; num_workers];
            parts[0] = local_entries;
            (parts, 0)
        };

        // =====================================================================
        // Phase 3: Redistribute via shared-memory slots + barrier
        // =====================================================================

        // Write per-destination entries to shared slots
        for (dest, entries) in per_worker_entries.into_iter().enumerate() {
            *self.shared_slots.redistribute_slots[worker_index][dest]
                .lock()
                .unwrap() = Some(entries);
        }

        // Wait for all workers to finish writing
        self.shared_slots.redistribute_barrier.wait().await;

        if Runtime::kill_in_progress() {
            return Err(SchedulerError::Killed);
        }

        // Read entries from all senders for this worker
        let mut my_entries: Vec<(V, ZWeight)> = Vec::new();
        for sender in 0..num_workers {
            if let Some(entries) = self.shared_slots.redistribute_slots[sender][worker_index]
                .lock()
                .unwrap()
                .take()
            {
                my_entries.extend(entries);
            }
        }

        // Sort entries: each sender's chunk is already sorted (from cursor),
        // so my_entries is W sorted runs concatenated. Rust's TimSort detects
        // sorted runs and merge-sorts them in O(D log W).
        my_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        // =====================================================================
        // Phase 4: Route to leaf buckets, clone + merge + split + write
        // =====================================================================

        let step_data_ref = step_data.clone();

        let leaf_write_result = if let Some(ref data) = step_data_ref {
            let has_storage = data.storage_backend.is_some();
            if has_storage && !my_entries.is_empty() {
                let view = &data.tree_view;
                let max_leaf = view.max_leaf_entries();

                // Batch-route sorted entries through tree: one traversal
                // partitions the sorted array at each internal node level.
                // O(D + nodes_visited) vs O(D × H × log B) per-entry deep_route.
                let leaf_ranges = route_sorted_entry_ranges(&my_entries, view);

                if leaf_ranges.is_empty() {
                    // Bootstrap/leaf-only tree: all entries are unrouted.
                    LeafWriteResult {
                        unrouted_entries: my_entries,
                        ..LeafWriteResult::empty()
                    }
                } else {
                    let my_segment_id = if worker_index < data.worker_segment_ids.len() {
                        data.worker_segment_ids[worker_index]
                    } else {
                        SegmentId::new(0)
                    };

                    let mut disk_leaves: Vec<DiskLeafInfo> = Vec::new();
                    let mut split_leaves_all: Vec<SplitLeafInfo<V>> = Vec::new();
                    let mut leaves_to_write: Vec<(usize, crate::storage::buffer_cache::FBuf)> =
                        Vec::new();
                    let mut total_weight_delta: ZWeight = 0;
                    let mut total_key_count_delta: i64 = 0;

                    // leaf_ranges is in tree-traversal order (= leaf-id order).
                    // Entries within each range are sorted (contiguous slice of
                    // sorted input). Zero-copy: we reference my_entries by slice.
                    for &(leaf_id, range_start, range_end) in &leaf_ranges {
                        let entries = &my_entries[range_start..range_end];
                        let original_leaf = view.get_leaf_clone(leaf_id);
                        let (modified_leaf, splits, weight_delta, key_delta) =
                            merge_and_split(&original_leaf, entries, max_leaf);

                        total_weight_delta += weight_delta;
                        total_key_count_delta += key_delta;

                        let serialized = serialize_to_bytes(&modified_leaf)
                            .expect("Failed to serialize merged leaf");
                        leaves_to_write.push((leaf_id, serialized));

                        let summary = build_cached_summary(&modified_leaf);
                        disk_leaves.push(DiskLeafInfo {
                            leaf_id,
                            cached_summary: summary,
                        });

                        for (split_key, split_leaf) in splits {
                            split_leaves_all.push(SplitLeafInfo {
                                original_leaf_id: leaf_id,
                                split_key,
                                leaf: split_leaf,
                            });
                        }
                    }

                    // Write leaves to disk
                    let segment_info = if !leaves_to_write.is_empty() {
                        if let Some(ref backend) = data.storage_backend {
                            let filename = format!(
                                "{}segment_{}.dat",
                                data.segment_path_prefix,
                                my_segment_id.value()
                            );
                            let path = if let Some(ref dir) = data.spill_directory {
                                dir.child(filename.as_str())
                            } else {
                                StoragePath::from(filename.as_str())
                            };

                            match crate::node_storage::write_leaves_to_segment(
                                backend,
                                &path,
                                &leaves_to_write,
                            ) {
                                Ok((reader, leaf_index, file_size)) => {
                                    Some(WorkerSegmentInfo {
                                        segment_id: my_segment_id,
                                        reader,
                                        leaf_index,
                                        file_size,
                                        path,
                                    })
                                }
                                Err(_) => None,
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    LeafWriteResult {
                        disk_leaves,
                        split_leaves: split_leaves_all,
                        segment: segment_info,
                        total_weight_delta,
                        total_key_count_delta,
                        unrouted_entries: vec![],
                    }
                }
            } else if !my_entries.is_empty() {
                // No storage or no tree view: treat all as unrouted
                LeafWriteResult {
                    unrouted_entries: my_entries,
                    ..LeafWriteResult::empty()
                }
            } else {
                LeafWriteResult::empty()
            }
        } else {
            // No step data — pass through with any entries as unrouted
            LeafWriteResult {
                unrouted_entries: my_entries,
                ..LeafWriteResult::empty()
            }
        };

        // =====================================================================
        // Phase 5: Gather via shared-memory slots + barrier
        // =====================================================================

        *self.shared_slots.gather_slots[worker_index].lock().unwrap() = Some(leaf_write_result);

        self.shared_slots.gather_barrier.wait().await;

        if Runtime::kill_in_progress() {
            return Err(SchedulerError::Killed);
        }

        // =====================================================================
        // Phase 6: Owner reconciliation
        // =====================================================================

        drop(step_data_ref);
        drop(step_data);

        if let Some(tree) = tree {
            // Read all gather results
            let mut all_results: Vec<LeafWriteResult<V>> = Vec::new();
            let mut all_unrouted: Vec<(V, ZWeight)> = Vec::new();
            for sender in 0..num_workers {
                if let Some(result) = self.shared_slots.gather_slots[sender]
                    .lock()
                    .unwrap()
                    .take()
                {
                    all_unrouted.extend(result.unrouted_entries.clone());
                    all_results.push(result);
                }
            }

            // Reconcile parallel leaf writes (only if there were disk leaves)
            let has_disk_work = all_results.iter().any(|r| !r.disk_leaves.is_empty());
            if has_disk_work {
                tree.reconcile_parallel_writes(all_results);
            }

            // Insert unrouted entries.
            // Bootstrap case: tree is empty and all entries are unrouted because
            // deep_route returns None for empty/leaf-only trees. Each worker's
            // unrouted entries are already sorted (from cursor). Sort-merge them
            // and consolidate duplicates, then bulk-load in O(N).
            if tree.root().is_none() && !all_unrouted.is_empty() {
                // all_unrouted is W sorted runs concatenated — TimSort merges
                // them in O(N log W).
                all_unrouted.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Consolidate consecutive duplicate keys (sum weights).
                let mut consolidated: Vec<(V, ZWeight)> =
                    Vec::with_capacity(all_unrouted.len());
                for (val, weight) in all_unrouted {
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
                // Non-bootstrap: insert remaining unrouted entries sequentially
                for (val, weight) in all_unrouted {
                    tree.insert(val, weight);
                }
            }
        }

        Ok(())
    }
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

    /// Per-worker entry counts from previous step (for cascade rebalancing).
    /// Updated by owner during reconciliation.
    prev_entry_counts: Mutex<Vec<usize>>,
    /// Previous step's leaf ranges (for cascade rebalancing).
    /// Updated by owner during publication.
    prev_leaf_ranges: Mutex<Vec<(usize, usize)>>,

    /// Barriers for 3-phase synchronization.
    barrier_1: AsyncBarrier, // After rebalance / before route
    barrier_2: AsyncBarrier, // After route / before local mutation
    barrier_3: AsyncBarrier, // After local mutation / before reconcile

    /// Number of workers.
    num_workers: usize,
}

impl<V: 'static> VirtualShardState<V> {
    fn new(num_workers: usize) -> Self {
        let redistribute_slots = (0..num_workers)
            .map(|_| (0..num_workers).map(|_| Mutex::new(None)).collect())
            .collect();
        let report_slots = (0..num_workers).map(|_| Mutex::new(None)).collect();
        Self {
            step_data: Mutex::new(None),
            generation: AtomicU64::new(0),
            notify: Notify::new(),
            redistribute_slots,
            report_slots,
            prev_entry_counts: Mutex::new(vec![0; num_workers]),
            prev_leaf_ranges: Mutex::new(Vec::new()),
            barrier_1: AsyncBarrier::new(num_workers),
            barrier_2: AsyncBarrier::new(num_workers),
            barrier_3: AsyncBarrier::new(num_workers),
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
    /// Entry counts from previous step (for cascade rebalancing).
    pub prev_entry_counts: Vec<usize>,
    /// Storage backend for workers to write segment files.
    pub storage_backend: Option<Arc<dyn StorageBackend>>,
    /// Segment path prefix.
    pub segment_path_prefix: String,
    /// Spill directory.
    pub spill_directory: Option<StoragePath>,
}

// SAFETY: ReadOnlyTreeView is Send+Sync, all other fields are Send+Sync.
unsafe impl<V: Send> Send for VirtualShardStepData<V> {}
unsafe impl<V: Send> Sync for VirtualShardStepData<V> {}

// Cache key for VirtualShardState in runtime.local_store()
circuit_cache_key!(local VirtualShardStateId<V>(usize => Arc<VirtualShardState<V>>));

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
    /// The 4-phase protocol:
    /// 1. Publish: owner publishes tree view + leaf ranges
    /// 2. Partition+Redistribute: each worker partitions its own entries in parallel
    ///    via `route_sorted_entry_ranges` + `leaf_owner`, deposits into 2D redistribute
    ///    slots, then collects from its column after barrier
    /// 3. Local Mutation: workers apply entries to owned leaves, report results
    /// 4. Reconcile: owner updates internal nodes from reports
    pub async fn virtual_shard_step(
        &mut self,
        tree: Option<&mut OrderStatisticsZSet<V>>,
        local_entries: Vec<(V, ZWeight)>,
    ) -> Result<(), SchedulerError> {
        let worker_index = self.worker_index;
        let num_workers = self.num_workers;
        let is_owner = tree.is_some();

        // =====================================================================
        // Phase 1: Owner publishes tree view + leaf ranges
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

            // Publish step data
            self.shared.publish(VirtualShardStepData {
                tree_view,
                owner_index: worker_index,
                worker_leaf_ranges,
                prev_entry_counts: prev_counts,
                storage_backend,
                segment_path_prefix,
                spill_directory,
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
        }

        // =====================================================================
        // Phase 2: Each worker partitions its own entries in parallel
        // =====================================================================

        // Each worker routes its sorted local_entries through the tree view
        // and deposits per-target slices into redistribute_slots[my_index][target].
        if let Some(ref data) = step_data {
            if data.tree_view.root.is_some() && !local_entries.is_empty() {
                let total_leaves = data.tree_view.total_leaves;
                let leaf_ranges = route_sorted_entry_ranges(&local_entries, &data.tree_view);

                if leaf_ranges.is_empty() {
                    // Bootstrap (leaf-only root): all entries to owner
                    *self.shared.redistribute_slots[worker_index][data.owner_index]
                        .lock()
                        .unwrap() = Some(local_entries);
                } else {
                    // Partition entries by leaf ownership
                    let mut per_target: Vec<Vec<(V, ZWeight)>> =
                        (0..num_workers).map(|_| Vec::new()).collect();

                    for &(leaf_id, start, end) in &leaf_ranges {
                        let target = leaf_owner(leaf_id, total_leaves, num_workers);
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

        // Each worker collects entries from its column: redistribute_slots[sender][my_index]
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

        // === Barrier 2 ===
        self.shared.barrier_2.wait().await;

        if Runtime::kill_in_progress() {
            return Err(SchedulerError::Killed);
        }

        // =====================================================================
        // Phase 3: Workers perform local mutation on owned leaves
        // =====================================================================

        let entries_count = my_entries.len();

        // Workers merge entries into leaves using the tree view for routing.
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

                        // Clone leaf from tree view and merge entries
                        let original = if self.leaf_storage.owns_leaf(leaf_id) {
                            self.leaf_storage.get_leaf(leaf_id).clone()
                        } else {
                            view.get_leaf_clone(leaf_id)
                        };

                        let (modified, leaf_splits, wd, kd) =
                            merge_and_split(&original, entries, max_leaf);

                        total_weight_delta += wd;
                        total_key_count_delta += kd;
                        weight_deltas.push((leaf_id, wd));
                        key_count_deltas.push((leaf_id, kd));

                        // Store modified leaf in our storage
                        if self.leaf_storage.owns_leaf(leaf_id) {
                            self.leaf_storage.replace_leaf(leaf_id, modified);
                        } else {
                            self.leaf_storage.insert_leaf(leaf_id, modified);
                        }

                        for (split_key, split_leaf) in leaf_splits {
                            splits.push(WorkerSplitInfo {
                                original_leaf_id: leaf_id,
                                split_key,
                                split_leaf,
                            });
                        }
                    }

                    // Collect modified leaves from our local storage for the report.
                    // The owner will replace these in its tree during reconciliation.
                    let modified_leaves: Vec<(usize, LeafNode<V>)> = leaf_ranges_for_entries
                        .iter()
                        .filter_map(|&(leaf_id, _, _)| {
                            if self.leaf_storage.owns_leaf(leaf_id) {
                                Some((leaf_id, self.leaf_storage.get_leaf(leaf_id).clone()))
                            } else {
                                // Leaves we don't own are temporary — remove from our storage
                                self.leaf_storage.remove_leaf(leaf_id).map(|leaf| (leaf_id, leaf))
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

        // === Barrier 3 ===
        self.shared.barrier_3.wait().await;

        if Runtime::kill_in_progress() {
            return Err(SchedulerError::Killed);
        }

        // =====================================================================
        // Phase 4: Owner reconciliation
        // =====================================================================

        drop(step_data);

        if let Some(tree) = tree {
            // Read all worker reports
            let mut all_unprocessed: Vec<(V, ZWeight)> = Vec::new();
            let mut total_weight_delta: ZWeight = 0;
            let mut total_key_count_delta: i64 = 0;
            let mut had_splits = false;
            let mut entry_counts = vec![0usize; num_workers];

            for w in 0..num_workers {
                if let Some(report) = self.shared.report_slots[w].lock().unwrap().take() {
                    total_weight_delta += report.total_weight_delta;
                    total_key_count_delta += report.total_key_count_delta;
                    entry_counts[w] = report.entries_processed;

                    // Collect unprocessed entries from all workers
                    if !report.unprocessed_entries.is_empty() {
                        all_unprocessed.extend(report.unprocessed_entries);
                    }

                    // Apply modified leaves back to the owner's tree
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

                        // Update the original leaf's next_leaf pointer
                        let orig_loc = LeafLocation::new(split_info.original_leaf_id);
                        if tree.storage().is_leaf_in_memory(orig_loc) {
                            tree.storage_mut().get_leaf_mut(orig_loc).next_leaf = Some(leaf_loc);
                        }
                    }
                }
            }

            // Store entry counts for next step's cascade rebalancing
            *self.shared.prev_entry_counts.lock().unwrap() = entry_counts;

            // Update totals from processed entries
            if total_weight_delta != 0 {
                tree.update_weight_delta(total_weight_delta);
            }
            if total_key_count_delta != 0 {
                tree.update_key_count_delta(total_key_count_delta);
            }

            // Recalculate subtree sums after structural changes
            if total_weight_delta != 0 || total_key_count_delta != 0 || had_splits {
                tree.recalculate_all_subtree_sums();
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
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::order_statistics::order_statistics_zset::OrderStatisticsZSet;
    use crate::algebra::F64;
    use crate::node_storage::NodeStorageConfig;

    #[test]
    fn test_leaf_owner_basic() {
        // 10 leaves across 4 workers: leaves_per_worker = ceil(10/4) = 3
        // worker 0: leaves 0-2, worker 1: 3-5, worker 2: 6-8, worker 3: 9
        assert_eq!(leaf_owner(0, 10, 4), 0);
        assert_eq!(leaf_owner(2, 10, 4), 0);
        assert_eq!(leaf_owner(3, 10, 4), 1);
        assert_eq!(leaf_owner(5, 10, 4), 1);
        assert_eq!(leaf_owner(6, 10, 4), 2);
        assert_eq!(leaf_owner(8, 10, 4), 2);
        assert_eq!(leaf_owner(9, 10, 4), 3);
    }

    #[test]
    fn test_leaf_owner_single_worker() {
        // All leaves map to worker 0
        assert_eq!(leaf_owner(0, 100, 1), 0);
        assert_eq!(leaf_owner(50, 100, 1), 0);
        assert_eq!(leaf_owner(99, 100, 1), 0);
    }

    #[test]
    fn test_leaf_owner_edge_cases() {
        assert_eq!(leaf_owner(0, 0, 4), 0);
        assert_eq!(leaf_owner(0, 10, 0), 0);
        // Exact division: 8 leaves across 4 workers
        assert_eq!(leaf_owner(0, 8, 4), 0);
        assert_eq!(leaf_owner(1, 8, 4), 0);
        assert_eq!(leaf_owner(2, 8, 4), 1);
        assert_eq!(leaf_owner(3, 8, 4), 1);
        assert_eq!(leaf_owner(6, 8, 4), 3);
        assert_eq!(leaf_owner(7, 8, 4), 3);
    }

    #[test]
    fn test_leaf_owner_more_workers_than_leaves() {
        // 3 leaves across 8 workers: leaves_per_worker = ceil(3/8) = 1
        assert_eq!(leaf_owner(0, 3, 8), 0);
        assert_eq!(leaf_owner(1, 3, 8), 1);
        assert_eq!(leaf_owner(2, 3, 8), 2);
    }

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
    fn test_extract_worker_boundaries() {
        let config = NodeStorageConfig::default();
        let mut tree: OrderStatisticsZSet<F64> =
            OrderStatisticsZSet::with_config(64, config);

        // Insert enough values to create many leaves
        for i in 0..500 {
            tree.insert(F64::new(i as f64), 1);
        }

        let boundaries = extract_worker_boundaries(&tree, 4);

        // Should have 3 boundaries for 4 workers
        assert!(!boundaries.is_empty());
        assert!(boundaries.len() <= 3);

        // Boundaries should be sorted
        for i in 1..boundaries.len() {
            assert!(boundaries[i - 1] < boundaries[i]);
        }
    }

    #[test]
    fn test_extract_worker_boundaries_small_tree() {
        let config = NodeStorageConfig::default();
        let mut tree: OrderStatisticsZSet<F64> =
            OrderStatisticsZSet::with_config(64, config);

        // Single leaf - no boundaries
        for i in 0..5 {
            tree.insert(F64::new(i as f64), 1);
        }

        let boundaries = extract_worker_boundaries(&tree, 4);
        assert!(boundaries.is_empty());
    }

    #[test]
    fn test_partition_entries_to_workers() {
        let separator_keys = vec![F64::new(25.0), F64::new(50.0), F64::new(75.0)];

        let entries: Vec<(F64, ZWeight)> = (0..100)
            .map(|i| (F64::new(i as f64), 1))
            .collect();

        let partitions = partition_entries_to_workers(entries, &separator_keys, 4);

        assert_eq!(partitions.len(), 4);
        // Worker 0: values < 25.0 → 25 entries (0..25)
        assert_eq!(partitions[0].len(), 25);
        // Worker 1: values 25.0..50.0 → 25 entries
        assert_eq!(partitions[1].len(), 25);
        // Worker 2: values 50.0..75.0 → 25 entries
        assert_eq!(partitions[2].len(), 25);
        // Worker 3: values >= 75.0 → 25 entries
        assert_eq!(partitions[3].len(), 25);

        // Verify partition correctness
        for &(ref v, _) in &partitions[0] {
            assert!(*v < F64::new(25.0));
        }
        for &(ref v, _) in &partitions[1] {
            assert!(*v >= F64::new(25.0) && *v < F64::new(50.0));
        }
    }

    #[test]
    fn test_partition_entries_no_separators() {
        let entries: Vec<(F64, ZWeight)> = (0..50)
            .map(|i| (F64::new(i as f64), 1))
            .collect();

        let partitions = partition_entries_to_workers(entries, &[], 4);

        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].len(), 50); // All to worker 0
        assert_eq!(partitions[1].len(), 0);
        assert_eq!(partitions[2].len(), 0);
        assert_eq!(partitions[3].len(), 0);
    }

    #[test]
    fn test_partition_entries_empty() {
        let separator_keys = vec![F64::new(50.0)];
        let entries: Vec<(F64, ZWeight)> = vec![];

        let partitions = partition_entries_to_workers(entries, &separator_keys, 2);

        assert_eq!(partitions.len(), 2);
        assert!(partitions[0].is_empty());
        assert!(partitions[1].is_empty());
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
