# Virtual Shard Routing for Percentile Operator

## Problem

The percentile operator maintains one `OrderStatisticsZSet` per GROUP BY key. When running with multiple DBSP workers, two cases arise:

1. **Multi-key (GROUP BY)**: `.shard()` distributes keys across workers by hash. Each worker owns different keys, so work distributes naturally. No intra-key parallelism is needed. Workers participate in empty barriers for synchronization only.

2. **Single-key (no GROUP BY, key = `Tup0`)**: All data belongs to one key. A naive `.shard()` would concentrate ALL data on one worker (since `hash(Tup0)` is constant), leaving N−1 workers idle during tree updates. When a large batch arrives, the single owner worker does all routing, disk I/O, and insertion sequentially.

## Solution: Virtual Shard Routing

The virtual shard routing system distributes leaf-level tree work across workers using a barrier-based 4-phase protocol with shared-memory communication (no Exchange). One logical B+ tree is maintained by a single owner (worker 0), while workers are assigned contiguous ranges of leaves to mutate in parallel.

Skip `.shard()` entirely for Tup0. The tree owner publishes a `ReadOnlyTreeView` and leaf-range assignments via shared memory. Each worker independently routes its own sorted entries through the tree's internal nodes using `route_sorted_entry_ranges()` + `leaf_owner()`, deposits per-target slices into 2D shared-memory redistribute slots, then collects entries from its column after a barrier. Workers mutate their assigned leaves locally and report modified leaf data back so the owner's tree stays consistent for percentile computation.

- **Bootstrap** (empty tree): Workers pass entries as unprocessed; owner bulk-loads via `bulk_load_sorted()` in O(N).
- **Steady-state** (tree has internal nodes): Each worker routes its own sorted entries to target workers via tree traversal and 2D redistribute slots. Workers merge received entries into cloned leaves, report modifications back to the owner.
- **Cascade rebalancing**: After each step, per-worker entry counts are stored. Before the next step, a deterministic cascade algorithm shifts leaf-range boundaries to balance work across workers.

## Architecture

### Data Flow (Tup0, no GROUP BY)

```
Input (round-robin: each worker has ~1/N entries, sorted from cursor)
    │
    ├── Phase 1: Publish (owner only)
    │   Owner publishes VirtualShardStepData via VirtualShardState:
    │   tree view, leaf ranges, storage config, previous entry counts.
    │   Leaf ranges computed via cascade rebalancing or even distribution.
    │   All workers wait and read the published data.
    │
    ├── Phase 2: Partition + Redistribute (all workers, parallel)
    │   Each worker independently routes its sorted local entries through
    │   the tree's internal nodes via route_sorted_entry_ranges(), maps
    │   each (leaf_id, start, end) range to a target worker via
    │   leaf_owner(), and deposits per-target slices into
    │   redistribute_slots[my_index][target].
    │   Bootstrap (no tree / leaf-only root): all entries to owner.
    │   ═══ Barrier 1 ═══
    │   Each worker collects entries from its column:
    │   redistribute_slots[sender][my_index] for all senders.
    │   Sorts the concatenated result (TimSort merges W sorted runs
    │   in O(D log W)).
    │   ═══ Barrier 2 ═══
    │
    ├── Phase 3: Local Mutation (all workers, parallel)
    │   Each worker processes its collected entries.
    │   Clones owned leaves from tree via ReadOnlyTreeView.
    │   Merges entries via two-pointer sorted merge, handles splits.
    │   Generates WorkerReport: weight_deltas, key_count_deltas,
    │   splits, modified_leaves, entries_processed.
    │   Bootstrap (no tree): entries returned as unprocessed.
    │   ═══ Barrier 3 ═══
    │
    └── Phase 4: Reconcile (owner only)
        Owner reads all WorkerReports:
        - Applies modified leaves back to tree via replace_leaf_data()
        - Processes splits: allocate leaf IDs, update internal nodes
        - Updates weight/key-count totals from deltas
        - Recalculates subtree sums
        - Bootstrap: bulk_load_sorted() from unprocessed entries
        - Stores entry counts for next step's cascade rebalancing
```

### Eval Dispatch Logic

The operator dispatches at `eval()` time based on key type and worker count:

```
if K == Tup0 && num_workers > 1:
    eval_unsharded_single_key()    // round-robin input, virtual shard routing
else if num_workers > 1:
    eval_sharded_with_parallel()   // sharded input, sequential insert, empty barriers
else:
    process_delta()                // single worker, purely sequential
```

The `K == Tup0` check uses `TypeId::of::<K>()` at the monomorphized call site — the compiler eliminates the dead branch entirely.

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

Workers read the owner's actual tree via raw pointer. No internal nodes are copied. Internal nodes are always in memory; evicted leaves are read from segment files via `read_leaf_readonly()` (Send + Sync) without modifying the tree's `LeafSlot` state. Workers clone leaves for merge operations via `get_leaf_clone()`.

**Safety contract**: The owner must not mutate the tree between creating the view (before publish) and consuming all gather results (before reconciliation).

### VirtualShardState (shared across workers)

```rust
struct VirtualShardState<V: 'static> {
    step_data: Mutex<Option<Arc<VirtualShardStepData<V>>>>,
    generation: AtomicU64,
    notify: Notify,
    redistribute_slots: Vec<Vec<Mutex<Option<Vec<(V, ZWeight)>>>>>,  // [sender][receiver] 2D matrix
    report_slots: Vec<Mutex<Option<WorkerReport<V>>>>,               // mutation reports per worker
    prev_entry_counts: Mutex<Vec<usize>>,                            // for cascade rebalancing
    prev_leaf_ranges: Mutex<Vec<(usize, usize)>>,                    // leaf boundaries per worker
    barrier_1: AsyncBarrier,
    barrier_2: AsyncBarrier,
    barrier_3: AsyncBarrier,
    num_workers: usize,
}
```

All workers share the same `Arc<VirtualShardState>` via `runtime.local_store()` using the `circuit_cache_key!` macro. The `redistribute_slots` is an N×N matrix (same pattern as `SharedExchangeSlots` used by the Exchange-based `ParallelRouting`): each worker writes to its row `redistribute_slots[my_index][target]` and reads from its column `redistribute_slots[sender][my_index]`.

**Publication protocol**:
1. Owner: `step_data.lock().replace(data); generation.fetch_add(1, Release); notify.notify_waiters();`
2. Workers: register `notified()` future, then check `generation.load(Acquire) > last_gen` — if already bumped, read immediately; otherwise `notified.await`.
3. Barrier 1 guarantees all workers finish writing their redistribute slots before any worker reads.

### VirtualShardStepData (published per step)

```rust
struct VirtualShardStepData<V> {
    tree_view: Arc<ReadOnlyTreeView<V>>,
    owner_index: usize,
    worker_leaf_ranges: Vec<(usize, usize)>,   // (first_leaf_id, exclusive_end) per worker
    prev_entry_counts: Vec<usize>,
    storage_backend: Option<Arc<dyn StorageBackend>>,
    segment_path_prefix: String,
    spill_directory: Option<StoragePath>,
}
```

### VirtualShardRouting (per-worker state)

```rust
struct VirtualShardRouting<V: DBData> {
    shared: Arc<VirtualShardState<V>>,
    pub leaf_storage: WorkerLeafStorage<V>,   // per-worker sparse leaf storage
    last_generation: u64,
    num_workers: usize,
    worker_index: usize,
    prev_entries_processed: usize,
}
```

Created during `PercentileOperator::new()` when `num_workers > 1`.

## WorkerLeafStorage

Per-worker sparse leaf storage for owned leaf ranges (`worker_leaf_storage.rs`). Each worker independently manages its leaves:

```rust
struct WorkerLeafStorage<V> {
    leaves: HashMap<usize, WorkerLeafSlot<V>>,    // leaf_id → Present | Evicted
    dirty_leaves: HashSet<usize>,
    segments: Vec<SegmentMetadata>,
    leaf_disk_locations: HashMap<usize, LeafDiskLocation>,
    // ... storage backend config
}
```

- `WorkerLeafSlot::Present(LeafNode<V>)` — leaf data in memory
- `WorkerLeafSlot::Evicted(CachedLeafSummary)` — on disk, summary cached

Workers can flush dirty leaves to disk and evict clean leaves independently.

**Lifecycle**: `WorkerLeafStorage` is cleared at `clock_end()` each step. Leaf data is temporary — the owner's tree is the source of truth.

## WorkerReport

Workers report mutation results back to the owner:

```rust
struct WorkerReport<V> {
    weight_deltas: Vec<(usize, ZWeight)>,           // per-leaf weight changes
    key_count_deltas: Vec<(usize, i64)>,             // per-leaf key count changes
    splits: Vec<WorkerSplitInfo<V>>,                 // split leaf data
    total_weight_delta: ZWeight,
    total_key_count_delta: i64,
    entries_processed: usize,                        // for cascade rebalancing
    unprocessed_entries: Vec<(V, ZWeight)>,           // bootstrap: workers can't route
    modified_leaves: Vec<(usize, LeafNode<V>)>,      // leaf data to replace in owner's tree
}
```

The `modified_leaves` field is critical: after workers mutate cloned leaves, the owner replaces its stale leaf data via `NodeStorage::replace_leaf_data()` so the tree is consistent for subsequent percentile computation (select queries).

## Protocol Phases (Detail)

### Phase 1: Publish + Rebalance (owner, then all workers via Barrier 1)

The owner computes leaf ranges for each worker and publishes `VirtualShardStepData`. Leaf ranges are computed by the owner:

1. **First step or empty tree**: `even_leaf_ranges()` distributes leaves evenly across workers.
2. **Subsequent steps with previous ranges**: Cascade rebalancing from previous step's entry counts via `compute_cascade_transfers()` + `apply_cascade_to_ranges()`. Falls back to even ranges if no transfers are needed.

The computed ranges are stored in `prev_leaf_ranges` for next step's rebalancing and published in `worker_leaf_ranges`. All workers wait for publication and configure their `WorkerLeafStorage` with storage backend info, then proceed to Phase 2 partitioning.

**Note**: The published `worker_leaf_ranges` are available to workers but Phase 2 routing currently uses even distribution (`leaf_owner()`: `leaf_id / leaves_per_worker`) rather than the cascade-rebalanced ranges. The cascade ranges will be used for routing once the routing is extended to consult them.

### Phase 2: Partition + Redistribute (all workers, parallel)

Each worker independently routes its own sorted `local_entries` through the tree's internal nodes via `route_sorted_entry_ranges()`, producing `(leaf_id, start, end)` ranges. Each range is mapped to a target worker via `leaf_owner(leaf_id, total_leaves, num_workers)` (even distribution: `leaf_id / leaves_per_worker`). Per-target entry slices are deposited into `redistribute_slots[my_index][target]`.

When the tree is empty (bootstrap) or has no internal nodes (root is a leaf), no routing is possible — all entries go to `redistribute_slots[my_index][owner_index]`.

After Barrier 1, each worker collects entries from its column: `redistribute_slots[sender][my_index]` for all senders, concatenates them, and sorts. Since each sender's entries are already sorted (contiguous slices from tree routing of sorted cursor output), TimSort detects W sorted runs and merges them in O(D log W).

After Barrier 2, the redistribute slots have been consumed and workers proceed to local mutation.

This distributed partitioning eliminates the previous bottleneck where the owner alone collected, sorted, routed, and re-distributed all entries from every worker.

### Phase 3: Local Mutation (all workers, then Barrier 3)

Each worker:
1. Uses the sorted entries collected from its redistribute column after Phase 2
2. Clones owned leaves from the tree via `ReadOnlyTreeView::get_leaf_clone()`
3. Merges entries into cloned leaves via two-pointer sorted merge (`merge_and_split()`)
4. Handles splits for oversized leaves (entries > `max_leaf_entries`)
5. Records modified leaves, weight/key-count deltas, and split info in `WorkerReport`
6. Publishes report to `report_slots[my_index]`

**Bootstrap case**: When the tree has no internal nodes (root is None or a leaf), workers cannot route entries to leaves. Entries are passed back as `unprocessed_entries` in the report. The owner handles them during reconciliation.

**Sorted invariant**: ZSet cursors iterate values in sorted order. `collect_entries_for_single_key()` in `percentile_op.rs` produces sorted entries. This invariant enables O(K+M) two-pointer merge at each leaf (entries are a contiguous sorted slice from routing) and O(N) bulk-load during bootstrap.

### Phase 4: Reconcile (owner only)

The owner reads all `WorkerReport`s and:
1. **Apply modified leaves**: Replaces stale leaf data in the tree via `replace_leaf_data()`. This is essential because the owner's tree still has the pre-mutation leaf data, and accurate leaf data is needed for percentile select queries.
2. **Process splits**: Allocates new leaf IDs, calls `insert_split_child()` to update internal nodes and `next_leaf` chains.
3. **Update totals**: Applies `update_weight_delta()` and `update_key_count_delta()` to the tree.
4. **Recalculate sums**: Single bottom-up pass via `recalculate_all_subtree_sums()`.
5. **Handle bootstrap**: Sort-merge unprocessed entries from all workers via TimSort — O(N log W). Consolidate consecutive duplicate keys — O(N). Call `bulk_load_sorted()` — O(N). Total: O(N) vs O(N log N) sequential inserts.
6. **Store entry counts**: Per-worker `entries_processed` values are stored for next step's cascade rebalancing.

## Cascade Rebalancing

Deterministic algorithm to redistribute leaf ranges based on workload from the previous step. Currently computed by the owner during Phase 1 publication.

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

## Activation and Degradation

| Scenario | Behavior |
|----------|----------|
| `num_workers == 1` | No routing; all inserts sequential |
| Tup0, `num_workers > 1`, tree has internal nodes | Full 4-phase virtual shard protocol with cascade rebalancing |
| Tup0, `num_workers > 1`, tree is leaf/empty | Bootstrap: all entries unprocessed → owner `bulk_load_sorted()` in O(N) |
| Multi-key, `num_workers > 1` | Sequential per-key insert + empty barriers for synchronization |

The protocol degrades gracefully. The barrier overhead is minimal (~2–10 μs per step for `AsyncBarrier`).

## Files

| File | Role |
|------|------|
| `algebra/order_statistics/parallel_routing.rs` | `VirtualShardState`, `VirtualShardRouting`, `VirtualShardStepData`, `AsyncBarrier`, `ReadOnlyTreeView`, `compute_cascade_transfers()`, `apply_cascade_to_ranges()`, `even_leaf_ranges()`, `merge_and_split()`, `route_sorted_entry_ranges()`, `batch_route_collect_ranges()`, `leaf_owner()`, `extract_worker_boundaries()`, `partition_entries_to_workers()` |
| `algebra/order_statistics/worker_leaf_storage.rs` | `WorkerLeafStorage`, `WorkerLeafSlot`, `WorkerReport`, `WorkerSplitInfo`, `LeafTransferData` |
| `algebra/order_statistics/order_statistics_zset.rs` | `bulk_load_sorted()`, `reconcile_parallel_writes()`, `insert_split_child()`, `recalculate_all_subtree_sums()`, `update_weight_delta()`, `update_key_count_delta()` |
| `operator/percentile.rs` | `is_tup0_key()`, shard-skip logic for Tup0 |
| `operator/dynamic/percentile_op.rs` | `eval_unsharded_single_key()`, `eval_sharded_with_parallel()`, eval dispatch, `virtual_shard` field |
| `node_storage.rs` | `replace_leaf_data()`, `replace_leaf_with_evicted()`, `register_worker_segment()`, `write_leaves_to_segment()`, `serialize_to_bytes()` |
| `node_storage_disk.rs` | `read_leaf_readonly()` |

## Limitations

- **Solo-only**: Raw pointer sharing requires same address space. Multi-host deployment would need serialized tree snapshots instead of raw pointers.
- **No intra-key parallelism for multi-key**: When multiple distinct keys are sharded across workers, each key's tree is updated sequentially on its owning worker. Only the Tup0 (single-key) case benefits from virtual shard routing.
- **Owner reconciliation is sequential**: Internal node updates (for splits) and subtree sum recalculation run on the owner only. Workers only help with leaf-level mutation.
- **Modified leaves sent in-memory**: Workers report modified leaf data back to the owner via `WorkerReport.modified_leaves`. For very large leaves this adds memory pressure.
- **Split leaves need owner allocation**: Split leaves can't be written to disk by workers because they need owner-allocated leaf IDs. Split leaf data is sent in-memory.
- **WorkerLeafStorage is ephemeral**: Cleared at `clock_end()` each step. Workers don't persistently own leaves across steps — leaf ownership is logical via ranges, physical data comes from the owner's tree each step.
