# Virtual Shard Routing for Percentile Operator

## Problem

The percentile operator maintains one `OrderStatisticsZSet` per GROUP BY key. When running with multiple DBSP workers, two cases arise:

1. **Multi-key (GROUP BY)**: `.shard()` distributes keys across workers by hash. Each worker owns different keys, so work distributes naturally. No intra-key parallelism is needed. Workers participate in empty barriers for synchronization only.

2. **Single-key (no GROUP BY, key = `Tup0`)**: All data belongs to one key. A naive `.shard()` would concentrate ALL data on one worker (since `hash(Tup0)` is constant), leaving N−1 workers idle during tree updates. When a large batch arrives, the single owner worker does all routing, disk I/O, and insertion sequentially.

## Solution: Virtual Shard Routing with Persistent Leaf Ownership

The virtual shard routing system distributes leaf-level tree work across workers using a barrier-based 2-phase protocol with shared-memory communication (no Exchange). One logical B+ tree is maintained by a single owner (worker 0), while workers are assigned contiguous ranges of leaves that they **persistently own across steps**.

Skip `.shard()` entirely for Tup0. The tree owner publishes a `ReadOnlyTreeView` and leaf-range assignments via shared memory. Each worker independently routes its own sorted entries through the tree's internal nodes using `route_sorted_entry_ranges()` + `leaf_owner()`, deposits per-target slices into 2D shared-memory redistribute slots, then collects entries from its column after a barrier. Workers merge entries into their persistent leaves, flush to disk independently, and report lightweight metadata back so the owner's tree stays consistent for percentile computation.

- **Bootstrap** (empty tree): Workers pass entries as unprocessed; owner bulk-loads via `bulk_load_sorted()` in O(N). Next step, workers clone their assigned leaves from the newly built tree.
- **Steady-state** (tree has internal nodes): Each worker routes its own sorted entries to target workers via tree traversal and 2D redistribute slots. Workers merge entries into persistent leaves, flush dirty leaves to disk, evict clean leaves from memory, and report summaries + segment info.
- **Cascade rebalancing**: After each step, per-worker entry counts are stored. Before the next step, a deterministic cascade algorithm shifts leaf-range boundaries. Workers physically transfer leaves that fall outside their new range to neighboring workers via `transfer_slots`.

## Architecture

### Data Flow (Tup0, no GROUP BY)

```
Input (round-robin: each worker has ~1/N entries, sorted from cursor)
    │
    ├── Phase 1: Publish + Rebalance + Partition (all workers, parallel)
    │   Owner publishes VirtualShardStepData via VirtualShardState:
    │   tree view, leaf ranges, segment_id_base, storage config,
    │   previous entry counts and leaf ranges.
    │   Leaf ranges computed via cascade rebalancing or even distribution.
    │   All workers wait and read the published data.
    │
    │   Workers compare old vs new owned_range.
    │   Out-of-range leaves transferred via transfer_slots[receiver].
    │
    │   Each worker routes its sorted local entries through the tree's
    │   internal nodes via route_sorted_entry_ranges(), maps each range
    │   to a target worker via leaf_owner(), deposits per-target slices
    │   into redistribute_slots[my_index][target].
    │   Bootstrap (no tree / leaf-only root): all entries to owner.
    │   ═══ Barrier 1 ═══
    │
    ├── Phase 2: Collect + Bootstrap + Mutate + Flush (all workers, parallel)
    │   Collect transferred leaves from transfer_slots[my_index].
    │   Bootstrap: clone from tree_view any leaves in owned range
    │   not yet in worker's storage.
    │   Collect entries from redistribute_slots[sender][my_index],
    │   sort (TimSort merges W sorted runs in O(D log W)).
    │
    │   Local mutation: merge entries into persistent leaves using
    │   get_leaf_reloading() (reload from own disk if evicted) or
    │   clone from tree_view (for leaves not yet in storage).
    │   Handle splits for oversized leaves.
    │
    │   Flush dirty leaves to disk, evict clean leaves from memory.
    │   Build WorkerReport with leaf_summaries + segment_info (persistent)
    │   or modified_leaves (fallback for no-storage-backend).
    │   Bootstrap (no tree): entries returned as unprocessed.
    │   ═══ Barrier 2 ═══
    │
    └── Phase 3: Reconcile (owner only)
        Owner reads all WorkerReports:
        - Persistent path: register_worker_segment() for each segment,
          replace_leaf_with_evicted() for each leaf summary.
        - Fallback path: replace_leaf_data() for each modified leaf.
        - Processes splits: allocate leaf IDs, update internal nodes,
          load_leaf_from_disk() to fix next_leaf chain for evicted leaves.
        - Updates weight/key-count totals from deltas.
        - Recalculates subtree sums.
        - Bootstrap: bulk_load_sorted() from unprocessed entries.
        - Stores entry counts for next step's cascade rebalancing.
```

### Eval Dispatch Logic

The operator dispatches at `eval()` time based on key type and worker count:

```
if K == Tup0 && virtual_shard.is_some():
    eval_unsharded_single_key()    // round-robin input, virtual shard routing
else:
    process_delta()                // sharded multi-key or single worker, sequential
```

The `K == Tup0` check uses `TypeId::of::<K>()` at the monomorphized call site — the compiler eliminates the dead branch entirely. For multi-key queries (GROUP BY), data is pre-sharded by key hash so each worker owns distinct keys — no cross-worker coordination is needed.

## Sharding Strategy

The shard-skip decision is made in `percentile.rs` at the stream API level, before the operator sees the data:

```rust
fn is_tup0_key<K: 'static>() -> bool {
    TypeId::of::<K>() == TypeId::of::<Tup0>()
}

let input = if is_tup0_key::<K>() {
    self.clone()   // skip shard — keep round-robin distribution
} else {
    self.shard()   // multi-key — shard by key hash
};
```

This applies to all three public methods: `percentile()`, `percentile_cont()`, `percentile_disc()`.

**Why skip shard for Tup0**: `shard()` distributes by `hash(key) % num_workers`. Since `Tup0` is a zero-sized type with a constant hash, ALL data would land on one worker — exactly the bottleneck we're trying to avoid. Skipping shard keeps the default round-robin distribution from the input adapter, giving each worker ~1/N entries.

## Shared-Memory Infrastructure

### AsyncBarrier

```rust
struct AsyncBarrier {
    count: AtomicUsize,
    generation: AtomicU64,
    total: usize,
    notify: tokio::sync::Notify,
}
```

Generation-based async barrier. The last worker to arrive resets the counter, bumps the generation, and wakes all waiters. Waiters register the `Notify` future BEFORE checking the generation to prevent TOCTOU races.

### ReadOnlyTreeView

```rust
struct ReadOnlyTreeView<V> {
    storage_ptr: *const OsmNodeStorage<V>,
    root: Option<NodeLocation>,
    total_leaves: usize,
    max_leaf_entries: usize,
}
```

Workers read the owner's actual tree via raw pointer. No internal nodes are copied. Internal nodes are always in memory; evicted leaves are read from segment files via `read_leaf_readonly()` (Send + Sync) without modifying the tree's `LeafSlot` state. Workers clone leaves for bootstrap via `get_leaf_clone()`.

**Safety contract**: The owner must not mutate the tree between creating the view (before publish) and consuming all reports (before reconciliation).

### VirtualShardState (shared across workers)

```rust
struct VirtualShardState<V: 'static> {
    step_data: Mutex<Option<Arc<VirtualShardStepData<V>>>>,
    generation: AtomicU64,
    notify: Notify,
    redistribute_slots: Vec<Vec<Mutex<Option<Vec<(V, ZWeight)>>>>>,  // [sender][receiver] 2D matrix
    report_slots: Vec<Mutex<Option<WorkerReport<V>>>>,               // mutation reports per worker
    transfer_slots: Vec<Mutex<Vec<LeafTransferBundle<V>>>>,          // cascade leaf transfers [receiver]
    prev_entry_counts: Mutex<Vec<usize>>,                            // for cascade rebalancing
    prev_leaf_ranges: Mutex<Vec<(usize, usize)>>,                    // leaf boundaries per worker
    barrier_1: AsyncBarrier,
    barrier_2: AsyncBarrier,
}
```

All workers share the same `Arc<VirtualShardState>` via `runtime.local_store()` using the `circuit_cache_key!` macro. The `redistribute_slots` is an N×N matrix: each worker writes to its row `redistribute_slots[my_index][target]` and reads from its column `redistribute_slots[sender][my_index]`. The `transfer_slots` is a 1D array indexed by receiver: workers deposit `LeafTransferBundle`s for the target worker during cascade rebalancing, and the receiver drains them after barrier 1.

**Publication protocol**:
1. Owner: `step_data.lock().replace(data); generation.fetch_add(1, Release); notify.notify_waiters();`
2. Workers: register `notified()` future, then check `generation.load(Acquire) > last_gen` — if already bumped, read immediately; otherwise `notified.await`.
3. Barrier 1 guarantees all workers finish writing their redistribute and transfer slots before any worker reads.

### VirtualShardStepData (published per step)

```rust
struct VirtualShardStepData<V> {
    tree_view: Arc<ReadOnlyTreeView<V>>,
    owner_index: usize,
    worker_leaf_ranges: Vec<(usize, usize)>,   // (first_leaf_id, exclusive_end) per worker
    prev_leaf_ranges: Vec<(usize, usize)>,     // previous step's ranges (for cascade)
    prev_entry_counts: Vec<usize>,
    storage_backend: Option<Arc<dyn StorageBackend>>,
    segment_path_prefix: String,
    spill_directory: Option<StoragePath>,
    segment_id_base: u64,                      // worker W uses segment_id_base + W
}
```

The `segment_id_base` is the owner's `NodeStorage::next_segment_id()` at publication time. Worker W uses `segment_id_base + W` as its segment ID. After reconciliation, `register_worker_segment()` advances the owner's counter past all worker IDs.

### VirtualShardRouting (per-worker state)

```rust
struct VirtualShardRouting<V: DBData> {
    shared: Arc<VirtualShardState<V>>,
    pub leaf_storage: WorkerLeafStorage<V>,   // per-worker persistent leaf storage
    last_generation: u64,
    num_workers: usize,
    worker_index: usize,
    prev_entries_processed: usize,
}
```

Created during `PercentileOperator::new()` when `num_workers > 1`.

## Persistent WorkerLeafStorage

Per-worker sparse leaf storage for owned leaf ranges (`worker_leaf_storage.rs`). Each worker independently manages its leaves **persistently across steps**:

```rust
struct WorkerLeafStorage<V> {
    leaves: HashMap<usize, WorkerLeafSlot<V>>,    // leaf_id → Present | Evicted
    dirty_leaves: HashSet<usize>,
    segments: Vec<SegmentMetadata>,
    leaf_disk_locations: HashMap<usize, LeafDiskLocation>,
    owned_range: (usize, usize),                   // [start, end) — persistent across steps
    bootstrapped: bool,                             // has worker cloned initial leaves?
    // ... storage backend config
}
```

- `WorkerLeafSlot::Present(LeafNode<V>)` — leaf data in memory
- `WorkerLeafSlot::Evicted(CachedLeafSummary)` — on disk, summary cached

Workers flush dirty leaves to their own segment files and evict clean leaves from memory independently. The `owned_range` tracks which leaves this worker is responsible for; it shifts during cascade rebalancing.

**Lifecycle**: `WorkerLeafStorage` is **persistent across steps** — workers keep their leaves and disk segments. Leaves are only cleared on `restore()` (checkpoint recovery), after which workers re-bootstrap from the restored tree. This avoids redundant cloning of unchanged leaves each step and enables workers to use their own disk I/O independently.

## WorkerReport (Dual-Path Reporting)

Workers report mutation results back to the owner via one of two paths:

```rust
struct WorkerReport<V> {
    weight_deltas: Vec<(usize, ZWeight)>,           // per-leaf weight changes
    key_count_deltas: Vec<(usize, i64)>,             // per-leaf key count changes
    splits: Vec<WorkerSplitInfo<V>>,                 // split leaf data
    total_weight_delta: ZWeight,
    total_key_count_delta: i64,
    entries_processed: usize,                        // for cascade rebalancing
    unprocessed_entries: Vec<(V, ZWeight)>,           // bootstrap: workers can't route
    // --- Persistent path (storage backend available) ---
    leaf_summaries: Vec<(usize, CachedLeafSummary)>, // lightweight metadata per modified leaf
    segment_info: Option<WorkerSegmentReport>,        // worker's flushed segment file
    // --- Fallback path (no storage backend) ---
    modified_leaves: Vec<(usize, LeafNode<V>)>,      // full leaf data
}
```

```rust
struct WorkerSegmentReport {
    segment_id: SegmentId,
    path: StoragePath,
    reader: Arc<dyn FileReader>,
    leaf_index: HashMap<usize, (u64, u32)>,          // leaf_id → (offset, size)
    file_size: u64,
}
```

**Persistent path** (when `storage_backend` is available): Workers flush dirty leaves to disk via `flush_dirty_to_disk()`, build `CachedLeafSummary` for each modified leaf, and report summaries + segment metadata. The owner calls `register_worker_segment()` to register the worker's segment file in its `NodeStorage`, then `replace_leaf_with_evicted()` for each modified leaf. This makes the owner's leaf slots point to the worker's segment files — subsequent `select_percentile` queries on the owner reload these 1–2 leaves from the worker segments on demand.

**Fallback path** (no storage backend, in-memory tests): Workers send full `modified_leaves` data, and the owner calls `replace_leaf_data()` to replace leaf contents in memory. This is the simpler pre-persistent behavior.

### Note on dual-path simplification

> The dual-path reporting exists because in-memory tests run without a storage backend, so `flush_dirty_to_disk()` fails. The simplest way to eliminate the fallback path would be to always provide a storage backend, even in tests — the existing `MemoryBackend` in `storage/backend/memory_impl.rs` is exactly this. If all tests configure a `MemoryBackend`, the persistent path handles everything uniformly: workers flush to memory-backed segment files, report summaries, and the owner registers those segments. The `modified_leaves` field and the fallback branch in both `virtual_shard_step` (Phase 2e) and reconciliation (Phase 3) could then be removed entirely.
>
> The only cost is that test setup would need `RuntimeConfig::storage(Storage::new_default())` or equivalent, which is already the pattern used by checkpoint/restore tests. The benefit is a single code path exercised in both tests and production, reducing maintenance surface and eliminating the risk of the two paths diverging. Until this cleanup, the fallback path is a safe degradation — it just sends more data over shared memory than the persistent path.

## Protocol Phases (Detail)

### Phase 1: Publish + Rebalance + Partition (all workers, then Barrier 1)

The owner computes leaf ranges for each worker and publishes `VirtualShardStepData`. Leaf ranges are computed by the owner:

1. **First step or empty tree**: `even_leaf_ranges()` distributes leaves evenly across workers.
2. **Subsequent steps with previous ranges**: Cascade rebalancing from previous step's entry counts via `compute_cascade_transfers()` + `apply_cascade_to_ranges()`. Falls back to even ranges if no transfers are needed.

The computed ranges are stored in `prev_leaf_ranges` and published in `worker_leaf_ranges`. The `segment_id_base` is read from the owner's `NodeStorage::next_segment_id()`. All workers wait for publication and configure their `WorkerLeafStorage` with storage backend info and segment ID.

**Cascade rebalancing (all workers, parallel)**: Each worker compares its `owned_range()` with the new `worker_leaf_ranges[my_index]`. Leaves outside the new range are removed via `take_leaf_for_transfer()` and deposited into `transfer_slots[receiver]` as `LeafTransferBundle`s. The target worker is determined by `leaf_owner_from_ranges()` using the published `worker_leaf_ranges`. Workers then update their `owned_range`.

**Entry partitioning (all workers, parallel)**: Each worker routes its sorted `local_entries` through the tree's internal nodes via `route_sorted_entry_ranges()`, producing `(leaf_id, start, end)` ranges. Each range is mapped to a target worker via `leaf_owner_from_ranges()` using the published `worker_leaf_ranges`. Per-target entry slices are deposited into `redistribute_slots[my_index][target]`.

When the tree is empty (bootstrap) or has no internal nodes (root is a leaf), no routing is possible — all entries go to `redistribute_slots[my_index][owner_index]`.

### Phase 2: Collect + Bootstrap + Mutate + Flush (all workers, then Barrier 2)

After Barrier 1:

**Collect transfers**: Each worker drains `transfer_slots[my_index]` and calls `receive_leaf()` for each transferred leaf. This handles physical leaf movement during cascade rebalancing.

**Bootstrap**: For each `leaf_id` in the worker's owned range, if `!leaf_storage.owns_leaf(leaf_id)`, the worker clones the leaf from `tree_view.get_leaf_clone(leaf_id)`. This handles: initial bootstrap (first step with a non-empty tree), new split leaves allocated by the owner, and tree growth. After cloning, the worker marks itself as bootstrapped.

**Collect entries**: Each worker reads from its column: `redistribute_slots[sender][my_index]` for all senders, concatenates them, and sorts. Since each sender's entries are already sorted (contiguous slices from tree routing of sorted cursor output), TimSort detects W sorted runs and merges them in O(D log W).

**Local mutation**: Workers merge entries into their persistent leaves. For each routed leaf range:
- If the worker owns the leaf, it uses `get_leaf_reloading()` to get the leaf (reloading from the worker's own disk if evicted).
- If the leaf is not in storage (shouldn't happen after bootstrap, but a safe fallback), clones from `tree_view`.
- Merges entries via two-pointer sorted merge (`merge_and_split()`), handles splits.

**Flush + report**: If a storage backend is available (persistent path): `flush_dirty_to_disk()`, `evict_clean_leaves()`, build `leaf_summaries` via `build_cached_summary()`, and report `segment_info`. If no storage backend (fallback path): clone `modified_leaves` from local storage and report those instead.

**Bootstrap case**: When the tree has no internal nodes, entries are passed back as `unprocessed_entries` in the report.

**Sorted invariant**: ZSet cursors iterate values in sorted order. `collect_entries_for_single_key()` in `percentile_op.rs` produces sorted entries. This invariant enables O(K+M) two-pointer merge at each leaf and O(N) bulk-load during bootstrap.

### Phase 3: Reconcile (owner only)

The owner reads all `WorkerReport`s and:

1. **Register worker segments (persistent path)**: For reports with `segment_info`, calls `register_worker_segment()` to register the worker's segment file. This updates the owner's `leaf_disk_locations` so that future disk reads of those leaves resolve to the worker's segment.
2. **Mark leaves as evicted (persistent path)**: For each `(leaf_id, summary)` in `leaf_summaries`, calls `replace_leaf_with_evicted()`. The owner's leaf slot becomes `Evicted(CachedLeafSummary)` — subsequent percentile select queries reload 1–2 leaves on demand.
3. **Replace leaf data (fallback path)**: For each `(leaf_id, leaf)` in `modified_leaves`, calls `replace_leaf_data()` to update the leaf in memory.
4. **Process splits**: Allocates new leaf IDs via `alloc_leaf()`, calls `insert_split_child()` to update internal nodes. For the original leaf's `next_leaf` pointer: if the leaf is in memory, updates directly; if evicted (from step 2), calls `load_leaf_from_disk(orig_loc)` to reload from the worker's registered segment, fixes `next_leaf`, and marks dirty.
5. **Update totals**: Applies `update_weight_delta()` and `update_key_count_delta()` to the tree.
6. **Update subtree sums**: If splits occurred, full recalculation via `recalculate_all_subtree_sums()`. Otherwise, incremental delta propagation via `apply_subtree_weight_deltas(&leaf_deltas)` — walks internal nodes only, never touches leaf data.
7. **Handle bootstrap**: Sort-merge unprocessed entries from all workers via TimSort — O(N log W). Consolidate consecutive duplicate keys — O(N). Call `bulk_load_sorted()` — O(N). Total: O(N) vs O(N log N) sequential inserts.
8. **Store entry counts**: Per-worker `entries_processed` values are stored for next step's cascade rebalancing.

## Cascade Rebalancing

Deterministic algorithm to redistribute leaf ranges based on workload from the previous step. Computed by the owner during Phase 1 publication; workers execute the physical leaf transfers in Phase 1b.

```rust
fn compute_cascade_transfers(entry_counts: &[usize], total_leaves: usize)
    -> Vec<(usize, usize, usize)>  // (from_worker, to_worker, num_leaves)
```

**Algorithm**:
1. Compute `target = total_entries / num_workers`
2. Compute each worker's surplus = `own_count - target` (positive = excess, negative = deficit)
3. Sweep left-to-right: each pair (i, i+1) transfers `min(surplus_i, deficit_{i+1})` leaves
4. Leaf transfers are proportional to entry-count imbalance

Entry count is the right balancing metric because it predicts computational work (tree navigation + leaf mutation + IO) better than subtree size or leaf count.

```rust
fn even_leaf_ranges(total_leaves: usize, num_workers: usize) -> Vec<(usize, usize)>
```

Computes balanced initial leaf ranges (used on first step before entry counts are available). Returns `Vec<(start, exclusive_end)>` tuples.

```rust
fn apply_cascade_to_ranges(
    current_ranges: &[(usize, usize)],
    transfers: &[(usize, usize, usize)],
    total_leaves: usize,
) -> Vec<(usize, usize)>
```

Applies cascade transfers to produce updated leaf ranges for the next step.

**Physical transfer**: Workers compare `owned_range()` with the new range from `worker_leaf_ranges`. Leaves outside the new range are removed via `take_leaf_for_transfer()` and deposited into `transfer_slots[receiver]` as `LeafTransferBundle`s. The receiving worker calls `receive_leaf()` after barrier 1. Transferred leaves include both in-memory data and disk reader references so the receiver can reload evicted leaves from the sender's segment files.

## Activation and Degradation

| Scenario | Behavior |
|----------|----------|
| `num_workers == 1` | No routing; all inserts sequential |
| Tup0, `num_workers > 1`, tree has internal nodes | Full 2-phase virtual shard protocol with persistent leaves and cascade rebalancing |
| Tup0, `num_workers > 1`, tree is leaf/empty | Bootstrap: all entries unprocessed → owner `bulk_load_sorted()` in O(N); workers bootstrap next step |
| Multi-key, `num_workers > 1` | Sequential per-key insert + empty barriers for synchronization |

The protocol degrades gracefully. The barrier overhead is minimal (~2–10 μs per step for `AsyncBarrier`).

## Lifecycle

| Event | Effect on WorkerLeafStorage |
|-------|----------------------------|
| `eval()` (each step) | Workers merge entries into persistent leaves, flush/evict as needed |
| `clock_end()` | No clearing — workers keep leaves across steps |
| `restore()` | `leaf_storage.clear()` — workers must re-bootstrap from the restored tree |
| Cascade rebalancing | Out-of-range leaves transferred to neighbors; new-range leaves cloned from tree_view |

## Files

| File | Role |
|------|------|
| `algebra/order_statistics/parallel_routing.rs` | `VirtualShardState`, `VirtualShardRouting`, `VirtualShardStepData`, `LeafTransferBundle`, `AsyncBarrier`, `ReadOnlyTreeView`, `compute_cascade_transfers()`, `apply_cascade_to_ranges()`, `even_leaf_ranges()`, `merge_and_split()`, `route_sorted_entry_ranges()`, `batch_route_collect_ranges()`, `leaf_owner()`, `leaf_owner_from_ranges()`, `extract_worker_boundaries()`, `partition_entries_to_workers()` |
| `algebra/order_statistics/worker_leaf_storage.rs` | `WorkerLeafStorage`, `WorkerLeafSlot`, `WorkerReport`, `WorkerSegmentReport`, `WorkerSplitInfo`, `LeafTransferData` |
| `algebra/order_statistics/order_statistics_zset.rs` | `bulk_load_sorted()`, `reconcile_parallel_writes()`, `insert_split_child()`, `recalculate_all_subtree_sums()`, `update_weight_delta()`, `update_key_count_delta()` |
| `operator/percentile.rs` | `is_tup0_key()`, shard-skip logic for Tup0 |
| `operator/dynamic/percentile_op.rs` | `eval_unsharded_single_key()`, `eval_sharded_with_parallel()`, eval dispatch, `virtual_shard` field |
| `node_storage.rs` | `replace_leaf_data()`, `replace_leaf_with_evicted()`, `register_worker_segment()`, `allocate_segment_id()`, `advance_segment_id_past()`, `load_leaf_from_disk()`, `write_leaves_to_segment()`, `serialize_to_bytes()` |
| `node_storage_disk.rs` | `read_leaf_readonly()`, `load_leaf_from_disk()` |

## Known Bottleneck: Owner-Only Phase 3

Profiling with 8 workers on 200K rows shows that while input distribution and leaf mutation are well parallelized, the owner (worker 0) is the single point of contention for memory (193 MiB storage vs 0 on others), output (267 records vs 0), and allocations (11.5K vs 2). All workers spend ~96% of time in the percentile operator — non-owners are mostly blocked at barriers waiting for the owner's Phase 3.

The core issue: after workers finish leaf-level mutation (Phase 2), the owner performs several O(total_leaves) operations sequentially:

1. **`recalculate_all_subtree_sums()`** — full bottom-up walk visiting every leaf and internal node. This is the most expensive part, and it's unnecessary: workers report per-leaf weight deltas in `WorkerReport`, so the owner only needs to propagate delta updates through internal nodes (never touching leaves). The owner should apply `weight_deltas` directly to the `subtree_sums` arrays in internal nodes, walking from each modified leaf's parent up to the root — O(modified_leaves × tree_height) instead of O(total_leaves).

2. **`register_worker_segment()` + `replace_leaf_with_evicted()`** — iterates over all modified leaves per worker report. This is O(modified_leaves) per step and not fundamentally parallelizable, but is lightweight (metadata updates only).

3. **`flush_and_evict()`** on the owner's tree after `virtual_shard_step()` — redundant when the persistent path is active: workers already flushed their modified leaves to disk and the owner received them as evicted summaries. The owner's tree should have very few dirty leaves after reconciliation (only split leaves that were allocated during Phase 3).

4. **`select_percentile_bounds()`** — O(log n) per percentile, potentially loading evicted leaves from disk. This runs only on the owner, producing all output. When percentile values haven't changed (common for large datasets with small deltas), this work could be avoided entirely.

### Implemented Improvements

The following optimizations have been implemented to reduce the Phase 3 bottleneck:

#### 1. Incremental subtree sum updates (✅ implemented)

`apply_subtree_weight_deltas()` on `OrderStatisticsZSet` propagates per-leaf weight deltas through internal nodes without visiting any leaf data. Phase 3 collects `weight_deltas` from all worker reports into a `HashMap<usize, ZWeight>`, then:

- **No splits**: Calls `apply_subtree_weight_deltas(&leaf_deltas)` — recursively walks internal nodes, doing O(1) HashMap lookups at each leaf position. With ~14 internal nodes and ~800 leaves, this visits ~14 nodes + ~800 HashMap lookups (most returning 0). Complexity: O(internal_nodes + total_leaves) but with only HashMap lookups at leaves (no disk I/O, no leaf data reading).
- **Splits occurred**: Falls back to `recalculate_all_subtree_sums()` because newly allocated split leaves have placeholder zeros that need real values.

#### 2. Eliminate redundant owner flush (✅ implemented)

`virtual_shard_step()` returns `VirtualShardStepResult` with a `had_splits` flag. In `eval_unsharded_single_key()`, the post-step `flush_and_evict()` is now gated on `had_splits == true`. Workers already flush their modified leaves during Phase 2; the only dirty leaves on the owner after reconciliation are newly allocated split leaves from Phase 3.

#### 3. Lazy percentile selection (removed — unsound)

This optimization was removed because the cache conditions are insufficient for correctness. Even when `total_weight` is unchanged and the cached boundary leaf isn't modified, entries can move between leaves on different sides of the percentile boundary (e.g., 66K entries shifting from high values to very negative values), causing the k-th element to change without either total_weight or the boundary leaf being modified. Correct detection would require tracking cumulative weight below the boundary — equivalent to redoing the select operation.

#### 6. Worker-reported metrics (✅ implemented)

`PercentileOperator::metadata()` now reports worker-side storage:
- `"worker storage size"` — sum of worker segment file sizes
- `"worker leaves"` — count of leaves owned by this worker
- `"worker evicted leaves"` — count of evicted worker leaves

`SizeOf` is implemented for `VirtualShardRouting` and `WorkerLeafStorage`, and `PercentileOperator::size_of_children()` includes virtual shard state. Since each worker has its own operator instance and the framework merges `Count`/`Bytes` MetaItems across workers, the profiler shows aggregate worker metrics alongside owner tree metrics.

## Future Optimizations

> **Parallel subtree sum propagation** — Instead of having the owner walk the entire internal node tree, partition the subtree sum work by worker leaf ranges. Each worker computes the subtree sum delta for its owned leaf range during Phase 2 (before Barrier 2) using `ReadOnlyTreeView`. Workers report `subtree_delta_path: Vec<(internal_node_id, child_idx, ZWeight)>`, and the owner applies deltas directly. With only ~14 internal nodes total for 200K entries (branching factor 64), the owner-side delta propagation (#1 above) is already very cheap. Implement only if profiling shows Phase 3 subtree sums are still a bottleneck with much larger datasets (millions of entries).

> **Pipeline Phase 3 with next step's Phase 1** — Owner reconciliation (Phase 3) and the next step's entry collection are independent. Double-buffering would let workers start collecting entries for the next step while the owner reconciles. This requires the operator to retain state across `eval()` calls (deferred reconciliation) and emit the previous step's output at the start of the next step — architecturally invasive. With improvements #1 and #2 making Phase 3 very fast (~14 internal nodes + conditional flush), the benefit is uncertain. Implement only if profiling shows Phase 3 wall time is still significant.

> **Batch percentile selection** — With multiple percentiles (e.g. 5 in the spill test), sort them and traverse the tree once rather than doing 5 separate root-to-leaf traversals. This reduces disk reads from potentially 10 leaves (2 per CONT percentile) to ~6-7 with locality. Implement a `select_multiple_percentiles()` method on `OrderStatisticsZSet`.

> **Distributed output emission** — Currently only the owner emits output deltas. For the multi-percentile case, different percentile selections could be farmed out to different workers if the tree's internal node metadata (subtree sums) was replicated or shared. This would require workers to have read access to the tree's internal nodes (already available via `ReadOnlyTreeView`) plus a way to merge their partial outputs. Low priority since percentile selection is O(log n) per query.

## Limitations

- **Solo-only**: Raw pointer sharing requires same address space. Multi-host deployment would need serialized tree snapshots instead of raw pointers.
- **No intra-key parallelism for multi-key**: When multiple distinct keys are sharded across workers, each key's tree is updated sequentially on its owning worker. Only the Tup0 (single-key) case benefits from virtual shard routing.
- **Owner reconciliation is sequential**: Internal node updates (for splits) and subtree sum recalculation run on the owner only. Workers only help with leaf-level mutation. See "Known Bottleneck" section above for analysis and planned improvements.
- **Split leaves need owner allocation**: Split leaves can't be written to disk by workers because they need owner-allocated leaf IDs. Split leaf data is sent in-memory via `WorkerSplitInfo`.
- **Dual-path reporting**: The persistent path (summaries + segment files) and fallback path (in-memory modified_leaves) coexist in `WorkerReport`. See the note above on simplification via `MemoryBackend`.
