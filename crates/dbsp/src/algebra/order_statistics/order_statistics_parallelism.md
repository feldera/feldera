# Parallel Routing for Percentile Operator

## Problem

The percentile operator maintains one `OrderStatisticsZSet` per GROUP BY key. When running with multiple DBSP workers, two cases arise:

1. **Multi-key (GROUP BY)**: `.shard()` distributes keys across workers by hash. Each worker owns different keys, so work distributes naturally. No intra-key parallelism is needed.

2. **Single-key (no GROUP BY, key = `Tup0`)**: All data belongs to one key. A naive `.shard()` would concentrate ALL data on one worker (since `hash(Tup0)` is constant), leaving N−1 workers idle during tree updates. When a large batch arrives, the single owner worker does all routing, disk I/O, and insertion sequentially.

## Solution Overview

The parallel routing system addresses both cases with shared-memory barriers and slot-based inter-worker communication (no Exchange):

### Single-Key (Tup0) — The Primary Optimization Target

Skip `.shard()` entirely. Input stays round-robin distributed across workers (~1/N entries per worker). The tree owner publishes a `ReadOnlyTreeView` and separator keys via shared memory. Workers binary-search their sorted entries to partition by destination worker, exchange entries through shared-memory slots with barrier synchronization, merge entries into cloned leaves, and send results back to the owner.

**Before** (Exchange-based): 2 Exchange barriers per step (CAS retry loops, payload cloning)
**After** (shared-memory): 2 `AsyncBarrier` waits per step (generation counter + Notify, zero cloning)

### Multi-Key (GROUP BY) — Barrier Synchronization Only

`.shard()` distributes keys across workers. Each worker processes its own keys sequentially (no benefit from intra-key parallelism since keys are already distributed). Workers still participate in the 2 barriers with empty payloads to maintain synchronization.

## Architecture

### Single-Key Data Flow (Tup0, no GROUP BY)

```
Input (round-robin: each worker has ~1/N entries)
    │
    ├── Phase 1: Owner publishes tree view + separator keys via SharedPercentileState
    │   (shared memory in runtime.local_store(), no Exchange)
    │
    ├── Phase 2: Each worker partitions its local entries by worker destination
    │   using binary search on separator keys — O(entries × log(num_workers))
    │
    ├── Phase 3 ─── Shared-Memory Redistribute + AsyncBarrier ────────
    │   Workers write entries to slots[sender][receiver], hit barrier,
    │   then read from slots[sender][my_index]. Zero cloning.
    │
    ├── Phase 4: Each worker: route to leaf buckets via deep_route(),
    │   clone leaves, merge entries, split if oversized, write to disk
    │
    ├── Phase 5 ─── Shared-Memory Gather + AsyncBarrier ──────────────
    │   Workers write LeafWriteResult to slots[my_index], hit barrier,
    │   owner reads from all slots. Zero cloning.
    │
    └── Phase 6: Owner reconciles: register segments, set Evicted slots,
        allocate split leaf IDs, update internal nodes, recalc sums.
        Insert any unrouted entries sequentially.
```

### Multi-Key Data Flow (GROUP BY)

```
Input → .shard() (distributed by key hash, each worker has ~1/N keys)
    │
    ├── Each worker: process_delta() inserts entries sequentially
    │   into its own per-key trees
    │
    └── All workers participate in empty Redistribute + Gather barriers
        (barrier synchronization, no data transferred)
```

### Eval Dispatch Logic

The operator dispatches at `eval()` time based on key type and worker count:

```
if K == Tup0 && num_workers > 1:
    eval_unsharded_single_key()    // round-robin input, parallel routing
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

## Shared-Memory Inter-Worker Communication

### SharedExchangeSlots (replaces Exchange)

```rust
struct SharedExchangeSlots<V> {
    redistribute_slots: Vec<Vec<Mutex<Option<Vec<(V, ZWeight)>>>>>,  // [sender][receiver]
    gather_slots: Vec<Mutex<Option<LeafWriteResult<V>>>>,            // [sender]
    redistribute_barrier: AsyncBarrier,
    gather_barrier: AsyncBarrier,
}
```

Workers write to slots indexed by `[my_index][destination]`, hit a barrier, then read from slots indexed by `[sender][my_index]`. The `take()` pattern ensures each payload is consumed exactly once.

**vs Exchange**: Exchange uses CAS retry loops with backoff, mutex-protected mailboxes, and `.iter().cloned()` on payloads (cloning entire `Vec<(V, ZWeight)>` per worker). SharedExchangeSlots uses direct `Mutex<Option<T>>` with `take()` — zero cloning, no retries.

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

### SharedPercentileState (tree view publication)

```rust
struct SharedPercentileState<V> {
    step_data: Mutex<Option<Arc<StepData<V>>>>,
    generation: AtomicU64,
    notify: tokio::sync::Notify,
}

struct StepData<V> {
    tree_view: Arc<ReadOnlyTreeView<V>>,
    separator_keys: Vec<V>,            // NEW: worker boundary keys for binary search
    owner_index: usize,
    storage_backend: Option<Arc<dyn StorageBackend>>,
    worker_segment_ids: Vec<SegmentId>,
    segment_path_prefix: String,
    spill_directory: Option<StoragePath>,
}
```

All workers in the same runtime share the same `Arc<SharedPercentileState>` via `runtime.local_store()` using the `circuit_cache_key!` macro.

**Publication protocol**:
1. Owner: `step_data.lock().replace(data); generation.fetch_add(1, Release); notify.notify_waiters();`
2. Workers: register `notified()` future, then check `generation.load(Acquire) > last_gen` — if already bumped, read immediately; otherwise `notified.await`.
3. The redistribute barrier guarantees all workers finish reading before the owner mutates the tree.

**Coordinator role**: Worker 0 acts as coordinator. When worker 0 is NOT the tree owner (multi-key case where nobody has a tree), it calls `publish_none()` so workers don't hang waiting for step data.

**TOCTOU safety**: The `Notify` future is registered BEFORE checking the generation counter. This prevents a race where the notification fires between the check and the await, which would cause the worker to wait indefinitely.

## Separator Keys and Binary Search Partitioning

### Separator Key Extraction

The owner reads the first key of boundary leaves to define worker value-range partitions:

```rust
fn extract_worker_boundaries(tree, num_workers) -> Vec<V>
```

For W workers and L total leaves, `leaves_per_worker = ceil(L / W)`. Boundary leaf IDs are `leaves_per_worker * w` for `w = 1..W`. Each boundary leaf's first key becomes a separator. Reads at most `W-1` leaves (from disk if evicted).

### Entry Partitioning

```rust
fn partition_entries_to_workers(entries, separator_keys, num_workers) -> Vec<Vec<(V, ZWeight)>>
```

Each entry is assigned to a worker via `partition_point(|sep| sep <= &entry.0)` — a binary search on `W-1` separator keys.

**Cost**: O(entries × log(W)) per worker, vs O(entries × tree_height) for per-entry `deep_route()`. For a tree with height 4 and 7 workers, this is O(entries × 3) vs O(entries × 4), a modest improvement. But the real win is that partitioning happens BEFORE the shared-memory exchange — each worker partitions its own entries in parallel, eliminating the need for all-to-all leaf-bucket redistribution.

## ReadOnlyTreeView

```rust
struct ReadOnlyTreeView<V> {
    storage_ptr: *const OsmNodeStorage<V>,
    root: Option<NodeLocation>,
    total_leaves: usize,
    max_leaf_entries: usize,
}
```

Workers read the owner's actual tree via raw pointer. No internal nodes are copied. Internal nodes are always in memory; evicted leaves are read from segment files via `read_leaf_readonly()` (Send + Sync) without modifying the tree's `LeafSlot` state. Workers clone leaves for merge operations via `get_leaf_clone()`.

**`deep_route()` behavior**: Traverses internal nodes to find the target leaf ID. Returns `None` when the root is a leaf (no internal nodes to route through) or the tree is empty. This causes entries to be treated as "unrouted" and sent to the owner via `LeafWriteResult.unrouted_entries`.

**Safety contract**: The owner must not mutate the tree between creating the view (before publish) and consuming all gather results (before reconciliation).

## ParallelRouting Infrastructure

Created during `PercentileOperator::new()` when `num_workers > 1`. Holds:
- 1 `Arc<SharedPercentileState>` (shared via `runtime.local_store()`)
- 1 `Arc<SharedExchangeSlots>` (shared via `runtime.local_store()`)
- `last_generation` counter for tracking shared state publications
- Worker identity (index, total count)

### Unrouted Entries

When `deep_route()` returns `None` (tree is a leaf, tree is empty, or no storage backend), entries are collected in `LeafWriteResult.unrouted_entries` and propagated back to the owner via the gather phase. The owner inserts these sequentially after reconciliation. This handles:
- **Bootstrap**: Tree starts as a single leaf, grows internal nodes over subsequent steps.
- **No storage backend**: Workers can't write merged leaves to disk, so all entries bypass the parallel merge and go directly to the owner.

## Protocol Phases (Single-Key Path)

### Phase 1: Publish Tree View + Separator Keys (owner only, no barrier)

The tree owner publishes `StepData` containing the `ReadOnlyTreeView`, separator keys, storage backend, pre-allocated segment IDs, and segment path info to `SharedPercentileState`. If no owner exists (multi-key barrier-only path), worker 0 publishes `None`.

### Phase 2: Partition by Separator Keys (all workers, parallel)

Each worker reads the shared separator keys and partitions its local entries using binary search. Entries with value < `separator_keys[0]` go to worker 0, entries with value in `[separator_keys[i-1], separator_keys[i])` go to worker `i`, etc. When no separator keys exist (bootstrap/no storage), all entries go to the owner.

Cost: O(local_entries × log(num_workers)) per worker.

### Phase 3: Redistribute (shared-memory slots + AsyncBarrier)

Each worker writes its per-destination entry Vecs to `redistribute_slots[my_index][dest]`, then all workers hit the `redistribute_barrier`. After the barrier, each worker reads from `redistribute_slots[sender][my_index]` for all senders, consuming entries via `.take()`.

### Phase 4: Route + Clone + Merge + Split + Write (per-worker)

Each worker now has entries destined for its leaf range. It routes entries to individual leaf buckets via `deep_route()` on the shared tree view, then processes each leaf:
1. **Sort** entries within each leaf bucket by value
2. **Clone** each leaf via `get_leaf_clone()` (loads evicted leaves from disk)
3. **Merge** entries into the cloned leaf using O(K+M) two-pointer sorted merge via `merge_and_split()`
4. **Split** oversized leaves (entries > `max_leaf_entries`) into chunks
5. **Write** modified original leaves to a new segment file via `write_leaves_to_segment()`
6. **Build** `CachedLeafSummary` for each disk-written leaf (for Evicted slot without disk I/O)

Entries where `deep_route()` returns `None` are collected as unrouted.

Split leaves are NOT written to disk — they're sent in-memory because the owner must allocate their leaf IDs.

### Phase 5: Gather (shared-memory slots + AsyncBarrier)

Workers write their `LeafWriteResult` to `gather_slots[my_index]`, then all workers hit the `gather_barrier`. The owner reads from all slots after the barrier.

### Phase 6: Owner Reconciliation

The owner reads `LeafWriteResult`s from all workers and calls `reconcile_parallel_writes()`:
1. **Register worker segments**: Add each worker's segment file to the tree's segment list
2. **Set Evicted slots**: Replace in-memory leaves with `LeafSlot::Evicted(CachedLeafSummary)`
3. **Process split leaves**: Allocate leaf IDs, set up `next_leaf` chains, reload original leaf to fix its `next_leaf` pointer
4. **Update internal nodes**: Insert new children for split leaves, handle cascading internal splits
5. **Recalculate subtree sums**: Single bottom-up pass using `CachedLeafSummary.weight_sum` for evicted leaves
6. **Update totals**: Accumulate weight and key count deltas

After reconciliation, any unrouted entries are inserted sequentially into the tree.

## Activation and Degradation

| Scenario | Behavior |
|----------|----------|
| `num_workers == 1` | `ParallelRouting` is `None`; all inserts sequential |
| Tup0, `num_workers > 1`, tree has internal nodes, storage backend exists | Full parallel routing (phases 1–6) |
| Tup0, `num_workers > 1`, tree is leaf/empty | All entries unrouted → owner inserts sequentially (bootstrap) |
| Tup0, `num_workers > 1`, no storage backend | All entries unrouted → owner inserts sequentially (in-memory only) |
| Multi-key, `num_workers > 1` | Sequential per-key insert + empty barriers |

The protocol degrades gracefully. When the tree is small (leaf root) or has no storage backend, entries are collected as unrouted and flow to the owner for sequential insertion. The barrier overhead is minimal (~2–10 μs per step for `AsyncBarrier` vs ~6–30 μs for Exchange).

## Known Bottleneck: Bootstrap (Empty Tree)

The parallel routing cannot distribute work when the tree is empty or has only a leaf root, because there are no internal nodes for `deep_route()` to traverse and no separator keys to partition on. **All entries become unrouted and the owner inserts them one by one.**

This is the dominant cost in workloads that load a large initial batch (e.g., the 200K-row spill test). The first step inserts all rows sequentially on the owner at O(N log N) cost, while N−1 workers sit idle at the barrier. Subsequent steps (with smaller deltas) benefit from parallel routing because the tree now has internal nodes, but they contribute a fraction of total runtime.

### Impact Analysis (200K rows, 7 workers)

```
Phase 1 (initial load): Tree empty → all 200K entries unrouted → owner inserts sequentially
  Cost: O(200K × log(200K)) ≈ 200K × 18 ≈ 3.6M tree operations, fully serial
  Workers 1-6: idle at barrier for ~1.4s

Phases 2-6 (updates/deletes): Tree has internal nodes → parallel routing activates
  Cost: distributed across workers, but each phase ≤ 100K entries
  Total: modest fraction of Phase 1 cost
```

### Proposed Fix: Parallel Sort + Bulk Build

For the bootstrap case (empty/leaf tree), replace sequential insertion with:

1. **Parallel sort**: Each worker sorts its local ~N/W entries — O((N/W) log(N/W)) per worker, runs concurrently
2. **Gather sorted chunks**: Workers send sorted Vecs to owner via shared-memory slots
3. **K-way merge**: Owner merges W sorted chunks into one sorted stream — O(N log W)
4. **Bulk build**: Owner calls `from_sorted_entries()` to construct the tree — O(N)

**Total**: O((N/W) log(N/W)) parallel + O(N log W + N) serial
**vs current**: O(N log N) fully serial

For 200K entries and 7 workers: ~4× improvement (parallel sort at 28K/worker + linear merge + linear build vs 200K sequential inserts with O(log N) each).

Detection: `separator_keys.is_empty()` already identifies the bootstrap case. The fix adds a pre-sort + bulk-build path before falling back to unrouted sequential insertion.

## Files

| File | Role |
|------|------|
| `algebra/order_statistics/parallel_routing.rs` | `SharedPercentileState`, `SharedExchangeSlots`, `AsyncBarrier`, `ReadOnlyTreeView`, `ParallelRouting`, `merge_and_split()`, `extract_worker_boundaries()`, `partition_entries_to_workers()`, routing logic |
| `operator/percentile.rs` | `is_tup0_key()`, shard-skip logic for Tup0 in `percentile()`, `percentile_cont()`, `percentile_disc()` |
| `operator/dynamic/percentile_op.rs` | `eval_unsharded_single_key()`, `eval_sharded_with_parallel()`, `collect_entries_for_single_key()`, eval dispatch |
| `node_storage.rs` | `replace_leaf_with_evicted()`, `register_worker_segment()`, `write_leaves_to_segment()`, `serialize_to_bytes()` |
| `node_storage_disk.rs` | `read_leaf_readonly()` |
| `order_statistics_zset.rs` | `reconcile_parallel_writes()`, `from_sorted_entries()`, `insert_split_child()`, `recalculate_all_subtree_sums()`, accessor methods |

## Limitations

- **Solo-only**: Raw pointer sharing requires same address space. Multi-host Exchange would need serialized tree snapshots instead of raw pointers.
- **No intra-key parallelism for multi-key**: When multiple distinct keys are sharded across workers, the tree for each key is updated sequentially on its owning worker. Only the Tup0 (single-key) case benefits from parallel leaf routing.
- **Storage backend required for parallel merge**: Without a storage backend, workers can't write merged leaves to disk. The protocol degrades to collecting all entries at the owner for sequential insertion. Parallel leaf merge only activates when the tree has both internal nodes and a storage backend.
- **Split leaves in-memory**: Split leaves can't be written to disk by workers because they need owner-allocated leaf IDs. Large splits send data in-memory.
- **Owner reconciliation is sequential**: Internal node updates (for splits) and subtree sum recalculation run on the owner only.
- **Bootstrap is sequential**: The first step with a large batch into an empty tree cannot use parallel routing — see "Known Bottleneck" above.
