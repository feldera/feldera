//! Exchange-based parallel routing for OrderStatisticsZSet batch insertion.
//!
//! When the percentile operator receives a large batch of entries for a single key,
//! parallel routing distributes the work across idle DBSP workers using the Exchange
//! primitive. Workers help with:
//! - Routing entries from root to leaf (determining which leaf each entry belongs to)
//! - Prefetching evicted leaves from disk
//! - Pre-sorting entries per leaf for cache-friendly insertion
//!
//! # Architecture
//!
//! Three Exchange rounds coordinate the work:
//! 1. **Scatter**: Owner distributes entry chunks + tree view to workers
//! 2. **Redistribute**: Workers exchange routed leaf buckets by leaf range
//! 3. **Gather**: Workers send sorted, prefetched results back to owner
//!
//! Workers read the owner's tree structure in place via a raw pointer
//! (`ReadOnlyTreeView`). The owner does not mutate the tree during routing.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::algebra::order_statistics::order_statistics_zset::{
    LeafNode, OsmNodeStorage, OrderStatisticsZSet,
};
use crate::algebra::ZWeight;
use crate::node_storage::{LeafLocation, NodeLocation};
use crate::operator::communication::Exchange;
use crate::{DBData, Runtime, SchedulerError};

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
pub(crate) struct ReadOnlyTreeView<V: Ord + Clone> {
    storage_ptr: *const OsmNodeStorage<V>,
    root: Option<NodeLocation>,
    total_leaves: usize,
}

// SAFETY: The owner guarantees no mutation while ReadOnlyTreeView exists.
// Internal nodes are read-only. FileReader::read_block() is Send + Sync.
unsafe impl<V: Ord + Clone + Send> Send for ReadOnlyTreeView<V> {}
unsafe impl<V: Ord + Clone + Send> Sync for ReadOnlyTreeView<V> {}

impl<V: DBData> ReadOnlyTreeView<V> {
    /// Create from an existing tree. Just stores a raw pointer.
    fn from_tree(tree: &OrderStatisticsZSet<V>) -> Self {
        ReadOnlyTreeView {
            storage_ptr: tree.storage_ptr(),
            root: tree.root(),
            total_leaves: tree.total_leaves(),
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

    /// Check if a leaf is evicted (needs disk read).
    fn is_leaf_evicted(&self, leaf_id: usize) -> bool {
        let storage = unsafe { &*self.storage_ptr };
        storage.is_leaf_evicted(LeafLocation::new(leaf_id))
    }

    /// Read an evicted leaf from disk without modifying the tree.
    fn read_evicted_leaf(&self, leaf_id: usize) -> Option<LeafNode<V>> {
        let storage = unsafe { &*self.storage_ptr };
        storage.read_leaf_readonly(leaf_id)
    }

    /// Get total number of leaves in the tree.
    fn total_leaves(&self) -> usize {
        self.total_leaves
    }
}

// =============================================================================
// Exchange Payload Types
// =============================================================================

/// Payload for the scatter exchange (owner → workers).
#[derive(Clone)]
pub(crate) struct ScatterPayload<V: Ord + Clone> {
    /// Entry chunks for this worker to route.
    entries: Vec<(V, ZWeight)>,
    /// Shared read-only view of the owner's tree (None for empty payloads).
    tree_view: Option<Arc<ReadOnlyTreeView<V>>>,
    /// Worker index of the tree owner (for gather phase).
    owner_index: usize,
}

/// A bucket of entries destined for a specific leaf.
#[derive(Clone)]
pub(crate) struct LeafBucket<V> {
    leaf_id: usize,
    /// Entries sorted by value within this bucket.
    entries: Vec<(V, ZWeight)>,
    /// Prefetched leaf data from disk (if leaf was evicted).
    prefetched_leaf: Option<LeafNode<V>>,
}

/// Payload for the redistribute exchange (all-to-all leaf bucket exchange).
#[derive(Clone)]
pub(crate) struct RedistributePayload<V> {
    /// Leaf buckets destined for this receiver's leaf range.
    buckets: Vec<LeafBucket<V>>,
}

/// Payload for the gather exchange (workers → owner).
#[derive(Clone)]
pub(crate) struct GatherPayload<V> {
    /// Sorted, prefetched leaf buckets ready for insertion.
    buckets: Vec<LeafBucket<V>>,
}

// =============================================================================
// Route entries to leaf buckets
// =============================================================================

/// Route entries to leaf-id-keyed buckets using the tree view.
fn route_entries_to_buckets<V: DBData>(
    entries: Vec<(V, ZWeight)>,
    view: &ReadOnlyTreeView<V>,
) -> HashMap<usize, Vec<(V, ZWeight)>> {
    let mut leaf_map: HashMap<usize, Vec<(V, ZWeight)>> = HashMap::new();
    for (val, weight) in entries {
        if let Some(leaf_id) = view.deep_route(&val) {
            leaf_map.entry(leaf_id).or_default().push((val, weight));
        }
    }
    leaf_map
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
// ParallelRouting: Exchange infrastructure for percentile operator
// =============================================================================

/// Exchange infrastructure for parallel routing within the percentile operator.
///
/// Created during operator construction when `Runtime::num_workers() > 1`.
/// Uses three `Exchange` instances for scatter/redistribute/gather phases.
///
/// All workers participate in all three exchanges during every `eval()` call,
/// even if they have no data. Empty exchanges have ~6-30 μs overhead.
pub(crate) struct ParallelRouting<V: Ord + Clone + Send + 'static> {
    scatter_exchange: Arc<Exchange<ScatterPayload<V>>>,
    redistribute_exchange: Arc<Exchange<RedistributePayload<V>>>,
    gather_exchange: Arc<Exchange<GatherPayload<V>>>,
    scatter_notify_send: Arc<Notify>,
    scatter_notify_recv: Arc<Notify>,
    redistribute_notify_send: Arc<Notify>,
    redistribute_notify_recv: Arc<Notify>,
    gather_notify_send: Arc<Notify>,
    gather_notify_recv: Arc<Notify>,
    num_workers: usize,
    worker_index: usize,
}

/// Minimum number of entries per key to trigger parallel routing.
const PARALLEL_ROUTING_THRESHOLD: usize = 256;

impl<V: DBData> ParallelRouting<V> {
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

        // Create 3 exchanges with panicking serialize/deserialize
        // (solo-only: local mailboxes pass T directly, no serialization)
        let scatter_exchange = Exchange::with_runtime(
            &runtime,
            runtime.sequence_next(),
            Box::new(|_| panic!("scatter: serialization not supported for local-only exchange")),
            Box::new(|_| panic!("scatter: deserialization not supported for local-only exchange")),
        );

        let redistribute_exchange = Exchange::with_runtime(
            &runtime,
            runtime.sequence_next(),
            Box::new(|_| {
                panic!("redistribute: serialization not supported for local-only exchange")
            }),
            Box::new(|_| {
                panic!("redistribute: deserialization not supported for local-only exchange")
            }),
        );

        let gather_exchange = Exchange::with_runtime(
            &runtime,
            runtime.sequence_next(),
            Box::new(|_| panic!("gather: serialization not supported for local-only exchange")),
            Box::new(|_| panic!("gather: deserialization not supported for local-only exchange")),
        );

        // Create Notify pairs for each exchange
        let scatter_notify_send = Arc::new(Notify::new());
        let scatter_notify_recv = Arc::new(Notify::new());
        let redistribute_notify_send = Arc::new(Notify::new());
        let redistribute_notify_recv = Arc::new(Notify::new());
        let gather_notify_send = Arc::new(Notify::new());
        let gather_notify_recv = Arc::new(Notify::new());

        // Register callbacks
        {
            let ns = scatter_notify_send.clone();
            scatter_exchange.register_sender_callback(worker_index, move || ns.notify_one());
        }
        {
            let nr = scatter_notify_recv.clone();
            scatter_exchange.register_receiver_callback(worker_index, move || nr.notify_one());
        }
        {
            let ns = redistribute_notify_send.clone();
            redistribute_exchange
                .register_sender_callback(worker_index, move || ns.notify_one());
        }
        {
            let nr = redistribute_notify_recv.clone();
            redistribute_exchange
                .register_receiver_callback(worker_index, move || nr.notify_one());
        }
        {
            let ns = gather_notify_send.clone();
            gather_exchange.register_sender_callback(worker_index, move || ns.notify_one());
        }
        {
            let nr = gather_notify_recv.clone();
            gather_exchange.register_receiver_callback(worker_index, move || nr.notify_one());
        }

        Some(Self {
            scatter_exchange,
            redistribute_exchange,
            gather_exchange,
            scatter_notify_send,
            scatter_notify_recv,
            redistribute_notify_send,
            redistribute_notify_recv,
            gather_notify_send,
            gather_notify_recv,
            num_workers,
            worker_index,
        })
    }

    /// Returns the minimum entry count threshold for parallel routing.
    pub fn threshold() -> usize {
        PARALLEL_ROUTING_THRESHOLD
    }

    /// Execute one round of parallel routing.
    ///
    /// ALL workers must call this method during every `eval()` call.
    /// The owner provides a tree and entries; non-owners provide `None`/empty.
    ///
    /// # Arguments
    ///
    /// * `tree` - The owner's tree (Some for owner, None for non-owners)
    /// * `entries` - Entries to route (non-empty for owner, empty for non-owners)
    pub async fn parallel_step(
        &self,
        tree: Option<&mut OrderStatisticsZSet<V>>,
        entries: Vec<(V, ZWeight)>,
    ) -> Result<(), SchedulerError> {
        let num_workers = self.num_workers;
        let worker_index = self.worker_index;
        let has_work = !entries.is_empty() && tree.is_some();

        // =====================================================================
        // Phase 1: Scatter (owner → workers)
        // =====================================================================

        let scatter_payloads: Vec<ScatterPayload<V>> = if has_work {
            let tree_ref = tree.as_ref().unwrap();
            let tree_view = Arc::new(ReadOnlyTreeView::from_tree(*tree_ref));
            let chunk_size = (entries.len() + num_workers - 1) / num_workers;
            (0..num_workers)
                .map(|i| {
                    let start = i * chunk_size;
                    let end = ((i + 1) * chunk_size).min(entries.len());
                    ScatterPayload {
                        entries: if start < entries.len() {
                            entries[start..end].to_vec()
                        } else {
                            vec![]
                        },
                        tree_view: Some(tree_view.clone()),
                        owner_index: worker_index,
                    }
                })
                .collect()
        } else {
            (0..num_workers)
                .map(|_| ScatterPayload {
                    entries: vec![],
                    tree_view: None,
                    owner_index: 0,
                })
                .collect()
        };

        while !self
            .scatter_exchange
            .try_send_all(worker_index, &mut scatter_payloads.iter().cloned())
        {
            if Runtime::kill_in_progress() {
                return Err(SchedulerError::Killed);
            }
            self.scatter_notify_send.notified().await;
        }

        let mut received_entries: Vec<(V, ZWeight)> = Vec::new();
        let mut tree_view: Option<Arc<ReadOnlyTreeView<V>>> = None;
        let mut owner_index: usize = 0;

        while !self
            .scatter_exchange
            .try_receive_all(worker_index, |payload| {
                received_entries.extend(payload.entries);
                if payload.tree_view.is_some() {
                    tree_view = payload.tree_view;
                    owner_index = payload.owner_index;
                }
            })
        {
            if Runtime::kill_in_progress() {
                return Err(SchedulerError::Killed);
            }
            self.scatter_notify_recv.notified().await;
        }

        // =====================================================================
        // Phase 2: Route entries to leaf buckets (parallel)
        // =====================================================================

        let leaf_map = if let Some(ref view) = tree_view {
            route_entries_to_buckets(received_entries, view)
        } else {
            HashMap::new()
        };

        // =====================================================================
        // Phase 3: Redistribute (all-to-all by leaf range)
        // =====================================================================

        let total_leaves = tree_view.as_ref().map_or(0, |v| v.total_leaves());

        let mut outgoing: Vec<Vec<LeafBucket<V>>> = vec![vec![]; num_workers];
        for (leaf_id, entries) in leaf_map {
            let dest = leaf_owner(leaf_id, total_leaves, num_workers);
            outgoing[dest].push(LeafBucket {
                leaf_id,
                entries,
                prefetched_leaf: None,
            });
        }

        let redistribute_payloads: Vec<RedistributePayload<V>> = outgoing
            .into_iter()
            .map(|buckets| RedistributePayload { buckets })
            .collect();

        while !self.redistribute_exchange.try_send_all(
            worker_index,
            &mut redistribute_payloads.iter().cloned(),
        ) {
            if Runtime::kill_in_progress() {
                return Err(SchedulerError::Killed);
            }
            self.redistribute_notify_send.notified().await;
        }

        // Receive and merge buckets from all workers
        let mut my_buckets: HashMap<usize, Vec<(V, ZWeight)>> = HashMap::new();
        while !self
            .redistribute_exchange
            .try_receive_all(worker_index, |payload| {
                for bucket in payload.buckets {
                    my_buckets
                        .entry(bucket.leaf_id)
                        .or_default()
                        .extend(bucket.entries);
                }
            })
        {
            if Runtime::kill_in_progress() {
                return Err(SchedulerError::Killed);
            }
            self.redistribute_notify_recv.notified().await;
        }

        // =====================================================================
        // Phase 4: Sort + Prefetch (per-worker, high locality)
        // =====================================================================

        let mut result_buckets: Vec<LeafBucket<V>> = Vec::new();

        // Process leaves in ID order (contiguous range for this worker)
        let mut sorted_leaves: Vec<(usize, Vec<(V, ZWeight)>)> =
            my_buckets.into_iter().collect();
        sorted_leaves.sort_by_key(|(leaf_id, _)| *leaf_id);

        for (leaf_id, mut entries) in sorted_leaves {
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let prefetched = if let Some(ref view) = tree_view {
                if view.is_leaf_evicted(leaf_id) {
                    view.read_evicted_leaf(leaf_id)
                } else {
                    None
                }
            } else {
                None
            };
            result_buckets.push(LeafBucket {
                leaf_id,
                entries,
                prefetched_leaf: prefetched,
            });
        }

        // =====================================================================
        // Phase 5: Gather (workers → owner)
        // =====================================================================

        let mut gather_payloads: Vec<GatherPayload<V>> = (0..num_workers)
            .map(|_| GatherPayload { buckets: vec![] })
            .collect();
        gather_payloads[owner_index] = GatherPayload {
            buckets: result_buckets,
        };

        while !self
            .gather_exchange
            .try_send_all(worker_index, &mut gather_payloads.iter().cloned())
        {
            if Runtime::kill_in_progress() {
                return Err(SchedulerError::Killed);
            }
            self.gather_notify_send.notified().await;
        }

        let mut all_results: Vec<GatherPayload<V>> = Vec::new();
        while !self
            .gather_exchange
            .try_receive_all(worker_index, |payload| {
                all_results.push(payload);
            })
        {
            if Runtime::kill_in_progress() {
                return Err(SchedulerError::Killed);
            }
            self.gather_notify_recv.notified().await;
        }

        // =====================================================================
        // Phase 6: Owner inserts pre-sorted buckets
        // =====================================================================

        // Drop the tree_view before mutating the tree
        drop(tree_view);

        if let Some(tree) = tree {
            // Collect all buckets from all workers (non-overlapping)
            let mut all_buckets: Vec<LeafBucket<V>> = Vec::new();
            for result in all_results {
                all_buckets.extend(result.buckets);
            }
            // Sort by leaf_id for tree-order access
            all_buckets.sort_by_key(|b| b.leaf_id);

            // Inject prefetched leaves and insert entries
            for bucket in all_buckets {
                if let Some(leaf) = bucket.prefetched_leaf {
                    tree.inject_prefetched_leaf(bucket.leaf_id, leaf);
                }
                // Insert entries (already sorted by value for cache locality)
                for (val, weight) in bucket.entries {
                    tree.insert(val, weight);
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

        let buckets = route_entries_to_buckets(entries.clone(), &view);

        // All entries should be routed
        let total_routed: usize = buckets.values().map(|v| v.len()).sum();
        assert_eq!(total_routed, 100);

        // All leaf IDs should be valid
        for &leaf_id in buckets.keys() {
            assert!(leaf_id < view.total_leaves());
        }
    }
}
