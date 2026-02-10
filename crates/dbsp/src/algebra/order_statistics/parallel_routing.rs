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
// Route entries to leaf buckets (used in Phase 4 per-worker routing)
// =============================================================================

/// Route entries to leaf-id-keyed buckets using the tree view.
///
/// Returns (routed leaf buckets, unrouted entries). Unrouted entries are those
/// where `deep_route` returns `None` (tree is empty or root is a leaf).
/// These must be inserted sequentially by the tree owner.
fn route_entries_to_buckets<V: DBData>(
    entries: Vec<(V, ZWeight)>,
    view: &ReadOnlyTreeView<V>,
) -> (HashMap<usize, Vec<(V, ZWeight)>>, Vec<(V, ZWeight)>) {
    let mut leaf_map: HashMap<usize, Vec<(V, ZWeight)>> = HashMap::new();
    let mut unrouted: Vec<(V, ZWeight)> = Vec::new();
    for (val, weight) in entries {
        if let Some(leaf_id) = view.deep_route(&val) {
            leaf_map.entry(leaf_id).or_default().push((val, weight));
        } else {
            unrouted.push((val, weight));
        }
    }
    (leaf_map, unrouted)
}

/// Map a leaf_id to a worker index using contiguous range assignment.
#[cfg(test)]
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

        // =====================================================================
        // Phase 4: Route to leaf buckets, clone + merge + split + write
        // =====================================================================

        let step_data_ref = step_data.clone();

        let leaf_write_result = if let Some(ref data) = step_data_ref {
            let has_storage = data.storage_backend.is_some();
            if has_storage && !my_entries.is_empty() {
                let view = &data.tree_view;
                let max_leaf = view.max_leaf_entries();

                // Route entries to individual leaf buckets
                let (leaf_map, unrouted) = route_entries_to_buckets(my_entries, view);

                let my_segment_id = if worker_index < data.worker_segment_ids.len() {
                    data.worker_segment_ids[worker_index]
                } else {
                    SegmentId::new(0)
                };

                // Process leaves in ID order
                let mut sorted_leaves: Vec<(usize, Vec<(V, ZWeight)>)> =
                    leaf_map.into_iter().collect();
                sorted_leaves.sort_by_key(|(leaf_id, _)| *leaf_id);

                let mut disk_leaves: Vec<DiskLeafInfo> = Vec::new();
                let mut split_leaves_all: Vec<SplitLeafInfo<V>> = Vec::new();
                let mut leaves_to_write: Vec<(usize, crate::storage::buffer_cache::FBuf)> =
                    Vec::new();
                let mut total_weight_delta: ZWeight = 0;
                let mut total_key_count_delta: i64 = 0;

                for (leaf_id, mut entries) in sorted_leaves {
                    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
                    let original_leaf = view.get_leaf_clone(leaf_id);
                    let (modified_leaf, splits, weight_delta, key_delta) =
                        merge_and_split(&original_leaf, &entries, max_leaf);

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
                            Ok((reader, leaf_index, file_size)) => Some(WorkerSegmentInfo {
                                segment_id: my_segment_id,
                                reader,
                                leaf_index,
                                file_size,
                                path,
                            }),
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
                    unrouted_entries: unrouted,
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

            // Insert unrouted entries sequentially
            for (val, weight) in all_unrouted {
                tree.insert(val, weight);
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
    fn test_route_entries_to_buckets() {
        let config = NodeStorageConfig::default();
        let mut tree: OrderStatisticsZSet<F64> =
            OrderStatisticsZSet::with_config(64, config);

        for i in 0..500 {
            tree.insert(F64::new(i as f64), 1);
        }

        let view = ReadOnlyTreeView::from_tree(&tree);

        let entries: Vec<(F64, ZWeight)> = (0..100)
            .map(|i| (F64::new(i as f64 * 5.0), 1))
            .collect();

        let (buckets, unrouted) = route_entries_to_buckets(entries.clone(), &view);

        // All entries should be routed (tree has internal nodes)
        let total_routed: usize = buckets.values().map(|v| v.len()).sum();
        assert_eq!(total_routed, 100);
        assert!(unrouted.is_empty());

        // All leaf IDs should be valid
        for &leaf_id in buckets.keys() {
            assert!(leaf_id < view.total_leaves());
        }
    }

    #[test]
    fn test_route_entries_to_buckets_leaf_only() {
        // Tree with only a leaf root — deep_route returns None
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

        let (buckets, unrouted) = route_entries_to_buckets(entries, &view);

        // All entries should be unrouted (single leaf root)
        assert!(buckets.is_empty());
        assert_eq!(unrouted.len(), 10);
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
}
