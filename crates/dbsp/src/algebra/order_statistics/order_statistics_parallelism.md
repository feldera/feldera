# Parallel Routing for Percentile Operator

## Problem

The percentile operator maintains one `OrderStatisticsZSet` per GROUP BY key. When running with multiple DBSP workers, two cases arise:

1. **Multi-key (GROUP BY)**: `.shard()` distributes keys across workers by hash. Each worker owns different keys, so work distributes naturally. No intra-key parallelism is needed.

2. **Single-key (no GROUP BY, key = `Tup0`)**: All data belongs to one key. A naive `.shard()` would concentrate ALL data on one worker (since `hash(Tup0)` is constant), leaving N−1 workers idle during tree updates. When a large batch arrives, the single owner worker does all routing, disk I/O, and insertion sequentially.

## Solution Overview

The parallel routing system addresses both cases with a shared-memory tree view publication mechanism and two Exchange rounds:

### Single-Key (Tup0) — The Primary Optimization Target

Skip `.shard()` entirely. Input stays round-robin distributed across workers (~1/N entries per worker). The tree owner publishes a `ReadOnlyTreeView` via shared memory. Workers route their local entries to leaf buckets, exchange them by leaf range, merge entries into cloned leaves, and send results back to the owner.

**Before**: 4 Exchange barriers per step (shard + scatter + redistribute + gather)
**After**: 2 Exchange barriers per step (redistribute + gather)

### Multi-Key (GROUP BY) — Barrier Synchronization Only

`.shard()` distributes keys across workers. Each worker processes its own keys sequentially (no benefit from intra-key parallelism since keys are already distributed). Workers still participate in the 2 Exchange barriers with empty payloads to maintain synchronization.

## Architecture

### Single-Key Data Flow (Tup0, no GROUP BY)

```
Input (round-robin: each worker has ~1/N entries)
    │
    ├── Phase 1: Owner publishes tree view via SharedPercentileState
    │   (shared memory in runtime.local_store(), no Exchange)
    │
    ├── Phase 2: Each worker routes its local entries to leaf buckets
    │   via deep_route() on the shared ReadOnlyTreeView
    │
    ├── Phase 3 ─── Redistribute Exchange ──────────────────────────
    │   All-to-all: repartition leaf buckets by contiguous leaf range.
    │   Unrouted entries (bootstrap/no storage) go to owner as sentinel.
    │
    ├── Phase 4: Each worker: clone leaves, merge entries, split if
    │   oversized, write modified leaves to disk as segment files
    │
    ├── Phase 5 ─── Gather Exchange ────────────────────────────────
    │   Workers → Owner: LeafWriteResult (disk metadata + split leaves)
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
    └── All workers participate in empty Redistribute + Gather Exchanges
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

## Shared-Memory Tree View Publication

The scatter Exchange from the previous design is replaced with lock-free shared memory:

### SharedPercentileState

```rust
struct SharedPercentileState<V> {
    step_data: Mutex<Option<Arc<StepData<V>>>>,
    generation: AtomicU64,
    notify: tokio::sync::Notify,
}

struct StepData<V> {
    tree_view: Arc<ReadOnlyTreeView<V>>,
    owner_index: usize,
    storage_backend: Option<Arc<dyn StorageBackend>>,
    worker_segment_ids: Vec<SegmentId>,
    segment_path_prefix: String,
    spill_directory: Option<StoragePath>,
}
```

All workers in the same runtime share the same `Arc<SharedPercentileState>` via `runtime.local_store()` using the `circuit_cache_key!` macro (same pattern as `Exchange::with_runtime`).

**Publication protocol**:
1. Owner: `step_data.lock().replace(data); generation.fetch_add(1, Release); notify.notify_waiters();`
2. Workers: register `notified()` future, then check `generation.load(Acquire) > last_gen` — if already bumped, read immediately; otherwise `notified.await`.
3. The redistribute Exchange barrier guarantees all workers finish reading before the owner mutates the tree.

**Coordinator role**: Worker 0 acts as coordinator. When worker 0 is NOT the tree owner (multi-key case where nobody has a tree), it calls `publish_none()` so workers don't hang waiting for step data.

**TOCTOU safety**: The `Notify` future is registered BEFORE checking the generation counter. This prevents a race where the notification fires between the check and the await, which would cause the worker to wait indefinitely.

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

**`deep_route()` behavior**: Traverses internal nodes to find the target leaf ID. Returns `None` when the root is a leaf (no internal nodes to route through) or the tree is empty. This causes entries to be treated as "unrouted" and sent directly to the owner for sequential insertion.

**Safety contract**: The owner must not mutate the tree between creating the view (before publish) and consuming all gather results (before reconciliation).

## ParallelRouting Infrastructure

Created during `PercentileOperator::new()` when `num_workers > 1`. Holds:
- 1 `Arc<SharedPercentileState>` (shared via `runtime.local_store()`)
- 2 `Exchange` instances (redistribute, gather)
- 4 `Notify` pairs (send + recv for each exchange)
- `last_generation` counter for tracking shared state publications
- Worker identity (index, total count)

Exchange instances use panicking serialize/deserialize closures — data is passed directly via local mailboxes (solo-only, same address space).

## Exchange Payloads

| Type | Direction | Contents |
|------|-----------|----------|
| `RedistributePayload<V>` | All-to-all | `Vec<LeafBucket>` for receiver's leaf range |
| `GatherPayload<V>` | Workers → Owner | `LeafWriteResult` (disk metadata + split leaves) |

The `ScatterPayload` from the previous design is eliminated; tree view data is shared via `SharedPercentileState` instead.

### Unrouted Entries

When `deep_route()` returns `None` (tree is a leaf, tree is empty, or no storage backend), entries are sent to the owner via the redistribute Exchange using a sentinel `leaf_id = usize::MAX`. The owner inserts these sequentially after reconciliation. This handles:
- **Bootstrap**: Tree starts as a single leaf, grows internal nodes over subsequent steps.
- **No storage backend**: Workers can't write merged leaves to disk, so all entries bypass the parallel merge and go directly to the owner.

## Protocol Phases (Single-Key Path)

### Phase 1: Publish Tree View (owner only, no Exchange)

The tree owner publishes `StepData` containing the `ReadOnlyTreeView`, storage backend, pre-allocated segment IDs, and segment path info to `SharedPercentileState`. If no owner exists (multi-key barrier-only path), worker 0 publishes `None`.

### Phase 2: Parallel Deep Routing (all workers)

Each worker reads the shared tree view and routes its local entries. `deep_route()` traverses from root through internal nodes using `find_child()` binary search, returning the target leaf ID. When no storage backend is available, routing is skipped entirely — all entries become unrouted.

Cost: O(local_entries × tree_height) per worker.

### Phase 3: Redistribute (all-to-all Exchange)

After routing, each worker has leaf buckets scattered across the entire tree. The redistribute exchange repartitions so each worker gets a **contiguous range of leaves**:

```
leaf_owner(leaf_id) = (leaf_id / ceil(total_leaves / num_workers)).min(num_workers - 1)
```

Contiguity is critical for I/O locality in the next phase. Unrouted entries are sent to the tree owner as sentinel buckets (`leaf_id = usize::MAX`).

### Phase 4: Clone + Merge + Split + Write (per-worker)

Each worker processes its contiguous leaf range:
1. **Clone** each leaf via `get_leaf_clone()` (loads evicted leaves from disk)
2. **Sort** entries within each leaf bucket by value
3. **Merge** entries into the cloned leaf using O(K+M) two-pointer sorted merge via `merge_and_split()`
4. **Split** oversized leaves (entries > `max_leaf_entries`) into chunks
5. **Write** modified original leaves to a new segment file via `write_leaves_to_segment()`
6. **Build** `CachedLeafSummary` for each disk-written leaf (for Evicted slot without disk I/O)

Split leaves are NOT written to disk — they're sent in-memory because the owner must allocate their leaf IDs.

Because leaf ranges are contiguous, disk reads and writes hit adjacent regions in segment files — sequential I/O rather than random access.

### Phase 5: Gather (workers → owner Exchange)

Workers send their `LeafWriteResult` to the owner. Each leaf appears in exactly one worker's output (redistribution guarantees disjoint ranges).

### Phase 6: Owner Reconciliation

The owner receives `LeafWriteResult`s from all workers and calls `reconcile_parallel_writes()`:
1. **Register worker segments**: Add each worker's segment file to the tree's segment list
2. **Set Evicted slots**: Replace in-memory leaves with `LeafSlot::Evicted(CachedLeafSummary)`
3. **Process split leaves**: Allocate leaf IDs, set up `next_leaf` chains, reload original leaf to fix its `next_leaf` pointer
4. **Update internal nodes**: Insert new children for split leaves, handle cascading internal splits
5. **Recalculate subtree sums**: Single bottom-up pass using `CachedLeafSummary.weight_sum` for evicted leaves
6. **Update totals**: Accumulate weight and key count deltas

After reconciliation, any unrouted entries (sentinel bucket) are inserted sequentially into the tree.

## Activation and Degradation

| Scenario | Behavior |
|----------|----------|
| `num_workers == 1` | `ParallelRouting` is `None`; all inserts sequential |
| Tup0, `num_workers > 1`, tree has internal nodes, storage backend exists | Full parallel routing (phases 1–6) |
| Tup0, `num_workers > 1`, tree is leaf/empty | All entries unrouted → owner inserts sequentially (bootstrap) |
| Tup0, `num_workers > 1`, no storage backend | All entries unrouted → owner inserts sequentially (in-memory only) |
| Multi-key, `num_workers > 1` | Sequential per-key insert + empty Exchange barriers |

The protocol degrades gracefully. When the tree is small (leaf root) or has no storage backend, `deep_route()` returns `None` for all entries, and they flow through the redistribute Exchange as unrouted sentinel buckets to the owner. The Exchange barriers still synchronize all workers; the overhead is ~6–30 μs per step (2 rounds × atomic CAS + mutex on empty mailboxes).

## Files

| File | Role |
|------|------|
| `algebra/order_statistics/parallel_routing.rs` | `SharedPercentileState`, `ReadOnlyTreeView`, `ParallelRouting`, payload types, `merge_and_split()`, routing logic |
| `operator/percentile.rs` | `is_tup0_key()`, shard-skip logic for Tup0 in `percentile()`, `percentile_cont()`, `percentile_disc()` |
| `operator/dynamic/percentile_op.rs` | `eval_unsharded_single_key()`, `eval_sharded_with_parallel()`, `collect_entries_for_single_key()`, eval dispatch |
| `node_storage.rs` | `replace_leaf_with_evicted()`, `register_worker_segment()`, `write_leaves_to_segment()`, `serialize_to_bytes()` |
| `node_storage_disk.rs` | `read_leaf_readonly()` |
| `order_statistics_zset.rs` | `reconcile_parallel_writes()`, `insert_split_child()`, `recalculate_all_subtree_sums()`, accessor methods |

## Limitations

- **Solo-only**: Raw pointer sharing requires same address space. Multi-host Exchange would need serialized tree snapshots instead of raw pointers.
- **No intra-key parallelism for multi-key**: When multiple distinct keys are sharded across workers, the tree for each key is updated sequentially on its owning worker. Only the Tup0 (single-key) case benefits from parallel leaf routing.
- **Storage backend required for parallel merge**: Without a storage backend, workers can't write merged leaves to disk. The protocol degrades to collecting all entries at the owner for sequential insertion. Parallel leaf merge only activates when the tree has both internal nodes and a storage backend.
- **Split leaves in-memory**: Split leaves can't be written to disk by workers because they need owner-allocated leaf IDs. Large splits send data in-memory via Exchange.
- **Owner reconciliation is sequential**: Internal node updates (for splits) and subtree sum recalculation run on the owner only.
