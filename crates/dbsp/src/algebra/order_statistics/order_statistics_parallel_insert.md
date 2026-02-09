# Exchange-Based Parallel Routing for Percentile Operator

## Problem

The percentile operator maintains one `OrderStatisticsZSet` per GROUP BY key. For the no-GROUP-BY case (`K = ()`), all data lands on one worker after `shard()`. The other N−1 workers are idle during tree updates. When a large batch arrives (thousands of inserts), the single owner worker does all routing, disk I/O, and insertion sequentially.

## Solution

Use DBSP's `Exchange` mechanism to distribute batch insertion work across idle workers. Three exchange rounds coordinate routing, prefetching, and sorting across all N workers before the owner performs the final insertion.

### What Workers Help With

1. **Routing**: Traverse internal nodes to determine which leaf each entry belongs to
2. **Prefetching**: Read evicted leaves from disk in parallel (sequential I/O per worker)
3. **Sorting**: Pre-sort entries per leaf for cache-friendly insertion

### What Only the Owner Does

- Mutate the tree (insert entries, handle splits, update subtree sums)

## Architecture

```
eval() on ALL workers (every step):

┌──────────────────────────────────────────────────────────────────┐
│  Owner: extract entries from delta, partition small/large keys   │
│                                                                  │
│  ┌─── Scatter Exchange ──────────────────────────────────────┐   │
│  │  Owner → Workers: entry chunks + ReadOnlyTreeView pointer │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  All workers: deep_route() entries → leaf buckets (parallel)     │
│                                                                  │
│  ┌─── Redistribute Exchange ─────────────────────────────────┐   │
│  │  All-to-all: repartition buckets by contiguous leaf range │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Each worker: sort entries per leaf, prefetch evicted leaves     │
│                                                                  │
│  ┌─── Gather Exchange ───────────────────────────────────────┐   │
│  │  Workers → Owner: sorted, prefetched leaf buckets         │   │
│  └───────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Owner: inject prefetched leaves, insert entries in tree order   │
└──────────────────────────────────────────────────────────────────┘
```

## Key Types

### `ReadOnlyTreeView<V>` — Raw pointer to tree storage

```rust
struct ReadOnlyTreeView<V> {
    storage_ptr: *const OsmNodeStorage<V>,  // Raw pointer, NOT a copy
    root: Option<NodeLocation>,
    total_leaves: usize,
}
```

Workers read the owner's actual tree via raw pointer. No internal nodes are copied. Internal nodes are always in memory; evicted leaves are read from segment files via `FileReader::read_block()` (Send + Sync) without modifying the tree's `LeafSlot` state.

**Safety contract**: The owner must not mutate the tree between creating the view (before scatter) and consuming all gather results (before insertion). All access during phases 1–5 is read-only.

### `ParallelRouting<V>` — Exchange infrastructure

Created during `PercentileOperator::new()` when `num_workers > 1`. Holds:
- 3 `Exchange` instances (scatter, redistribute, gather)
- 6 `Notify` pairs (send + recv for each exchange)
- Worker identity (index, total count)

Exchange instances use panicking serialize/deserialize closures — data is passed directly via local mailboxes (solo-only, same address space).

### Exchange Payloads

| Type | Direction | Contents |
|------|-----------|----------|
| `ScatterPayload<V>` | Owner → Workers | Entry chunks + `Arc<ReadOnlyTreeView>` + owner index |
| `RedistributePayload<V>` | All-to-all | `Vec<LeafBucket>` for receiver's leaf range |
| `GatherPayload<V>` | Workers → Owner | Sorted, prefetched `Vec<LeafBucket>` |

`LeafBucket<V>` contains a leaf ID, sorted entries, and an optional prefetched `LeafNode` (for evicted leaves read from disk).

## Protocol Phases

### Phase 1: Scatter (owner → workers)

The owner splits entries into roughly equal contiguous chunks (one per worker) and broadcasts the `ReadOnlyTreeView` wrapped in `Arc`. Non-owners send empty payloads. All workers participate (Exchange requires symmetric send/receive).

### Phase 2: Parallel Deep Routing

Each worker receives a chunk of entries and the shared tree view. For each entry, `deep_route()` traverses from root through all internal node levels using `find_child()` binary search, returning the target leaf ID. Result: per-worker `HashMap<leaf_id, Vec<(V, ZWeight)>>`.

Cost: O(entries/workers × tree_height) per worker — tree height traversals are distributed.

### Phase 3: Redistribute (all-to-all)

After routing, each worker has leaf buckets scattered across the entire tree. The redistribute exchange repartitions so each worker gets a **contiguous range of leaves**:

```
leaf_owner(leaf_id) = (leaf_id / ceil(total_leaves / num_workers)).min(num_workers - 1)
```

This contiguity is critical for I/O locality in the next phase.

### Phase 4: Sort + Prefetch (per-worker)

Each worker processes its contiguous leaf range:
1. **Sort** entries within each leaf bucket by value (cache-friendly insertion order)
2. **Prefetch** evicted leaves via `read_leaf_readonly()` — reads from segment files without modifying the tree

Because leaf ranges are contiguous, disk reads hit adjacent regions in segment files — sequential I/O rather than random access.

### Phase 5: Gather (workers → owner)

Workers send their sorted, prefetched `LeafBucket`s to the owner. Each leaf appears in exactly one worker's output (redistribution guaranteed disjoint ranges).

### Phase 6: Owner Insertion

The owner receives non-overlapping buckets from all workers, sorted by leaf ID. For each bucket:
1. If a prefetched leaf is present, inject it into the tree's `LeafSlot` (avoids redundant disk read)
2. Insert each entry via `tree.insert(val, weight)` in sorted order

Entries are inserted individually (not batch-merged into leaves). The benefit comes from parallel routing/prefetching, sorted insertion order, and pre-warmed leaf cache.

## Activation Criteria

| Condition | Behavior |
|-----------|----------|
| `num_workers == 1` | `ParallelRouting` is `None`; all inserts sequential |
| `num_workers > 1`, entries < 256 for a key | Sequential insertion for that key |
| `num_workers > 1`, entries >= 256, root is leaf | Sequential (no internal nodes to route through) |
| `num_workers > 1`, entries >= 256, tree has depth | Parallel routing activated for one key per step |

Only **one key per step** uses parallel routing (all workers must participate in the same exchange round). Additional large keys are inserted sequentially.

When no key qualifies for parallel routing, all workers still participate in the exchange with empty payloads. Overhead: ~6–30 μs per step (3 rounds × atomic CAS + mutex on empty mailboxes).

## Files

| File | Role |
|------|------|
| `algebra/order_statistics/parallel_routing.rs` | `ReadOnlyTreeView`, `ParallelRouting`, payload types, routing logic |
| `operator/dynamic/percentile_op.rs` | Integration: `process_delta_with_partition()`, exchange calls in `eval()` |
| `node_storage.rs` | `inject_prefetched_leaf()`, `leaves_len()` |
| `node_storage_disk.rs` | `read_leaf_readonly()` — disk read without modifying tree state |
| `order_statistics_zset.rs` | Accessor methods: `root()`, `total_leaves()`, `storage_ptr()`, `inject_prefetched_leaf()` |

## Future Optimization: `insert_batch_to_leaf()`

The current implementation inserts entries one at a time via `tree.insert()`. A future optimization could add `insert_batch_to_leaf(leaf_id, entries)` that:
1. Navigates once from root to the target leaf — O(height)
2. Merges all pre-sorted entries into the leaf — O(k)
3. Handles splits and propagates up

This would reduce per-leaf cost from O(k × height) to O(height + k), giving O(L × height + N) total instead of O(N × height) for N entries across L leaves. The parallel routing infrastructure is already designed to support this — buckets are sorted by leaf ID and entries within each bucket are sorted by value.

## Limitations

- **Solo-only**: Raw pointer sharing requires same address space. Multi-host Exchange would need serialized tree snapshots instead of raw pointers.
- **One key per exchange round**: Multiple large keys in the same step are not parallelized together.
- **No parallel mutation**: Only the owner mutates the tree. Workers help with read-only routing, I/O, and sorting.
