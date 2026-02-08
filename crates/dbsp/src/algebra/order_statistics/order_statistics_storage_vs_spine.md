# Storage in DBSP: Spine vs NodeStorage

This document explains the relationship between DBSP's Spine storage system and the NodeStorage abstraction for OrderStatisticsZSet.

## DBSP's Incremental Computation Model

DBSP operators follow a consistent pattern for incremental computation:

```
Delta (changes) ──► Operator ──► Output Delta
                        │
                        ▼
                 Persistent State
                 (updated incrementally)
```

Each operator:
1. Maintains its own **persistent state** across circuit steps
2. Receives **deltas** (changes) as input
3. **Updates state incrementally** with the delta (not rebuilds)
4. Produces **output deltas** reflecting state changes

This is the foundation of DBSP's efficiency - operators update state in O(delta) time, not O(total data) time.

## Storage Abstractions by Data Structure

Different data structures need different storage abstractions:

| Data Structure | Storage Abstraction | Used By |
|----------------|---------------------|---------|
| Sorted batches (immutable runs) | **Spine** | Traces, indexed collections |
| B+ trees (mutable, augmented) | **NodeStorage** | OrderStatisticsZSet |

### Spine: Storage for Traces

Spine manages **Traces** - append-only collections of sorted `(Key, Value, Time, Diff)` batches:

```rust
pub trait Trace: BatchReader {
    fn batches(&self) -> &[Batch];
    // Batches are immutable, merged in background
}
```

Spine provides:
- Batch merging (background async)
- Disk spilling for large batches
- Integration with BufferCache for LRU caching
- Checkpoint/restore support

### NodeStorage: Storage for B+ Trees

NodeStorage manages **OrderStatisticsZSet** - mutable B+ trees with augmented subtree sums:

```rust
pub struct OrderStatisticsZSet<T> {
    storage: NodeStorage<T>,  // Manages internal nodes + leaves
    root: Option<NodeLocation>,
    // ...
}
```

NodeStorage provides:
- Separation of internal nodes (pinned) from leaves (spillable)
- Dirty tracking for incremental persistence
- Block-based disk format with checksums
- Integration with BufferCache and Checkpointer

## The Percentile Operator Pattern

The percentile operator follows the standard DBSP pattern:

```
Input Delta ──► PercentileOperator ──► Output Delta
  (K, V, W)            │                (K, percentile)
                       ▼
              Persistent State:
              Map<K, OrderStatisticsZSet<V>>
```

For each `(key, value, weight)` in the input delta:
1. Look up (or create) the tree for `key`
2. Call `tree.insert(value, weight)` - O(log n) operation
3. Compute updated percentile if needed

This is **truly incremental**: O(B log n) per step where B is batch size, not O(n log n) rebuild.

### Why B+ Trees Are Necessary

Percentile computation requires **order statistics queries**:
- `select_kth(k)`: Find the k-th element by weight - O(log n)
- `rank(key)`: Find position of key - O(log n)

These operations require augmented tree structure (subtree weight sums). Sorted batches (Spine) don't support O(log n) rank queries - they'd require O(n) scans.

### Why Previous DBSP Operators Didn't Need Trees

Percentile is the **first DBSP operator requiring rank-based queries**. Previous operators only needed:

| Operation | Query Pattern | Data Structure |
|-----------|---------------|----------------|
| JOIN | "All values for key K" | Trace (key-indexed) |
| DISTINCT | "Have I seen key K?" | Trace (key-indexed) |
| SUM/COUNT/AVG | Accumulate scalar | Simple value |
| GROUP BY | Partition by key | Trace (key-indexed) |

Traces efficiently support **key-based queries** via cursors:
```
Trace query:  "Give me values where key = 'foo'"  → O(log n)
```

But **rank-based queries** require scanning:
```
Rank query:   "Give me the 1000th element"        → O(n) with Trace
                                                  → O(log n) with augmented tree
```

### Other Potential Users of Tree Storage

SQL operations that also need order statistics:

| Operation | Needs Order Statistics? |
|-----------|------------------------|
| `PERCENTILE_CONT` | ✅ Yes |
| `PERCENTILE_DISC` | ✅ Yes |
| `MEDIAN` | ✅ Yes (special case) |
| `NTILE(n)` | ✅ Yes (partition by rank) |
| `CUME_DIST` | ✅ Yes (cumulative distribution) |
| Window `RANK()`, `ROW_NUMBER()` | Potentially |

Rank queries like "find the k-th element" require O(n) scans with Traces but O(log n) with
augmented trees. A generalized tree storage abstraction could support future rank-based operators. (see Phase 8 in `order_statistics_node_storage_plan.md`).

### Why NodeStorage Is Necessary

The B+ trees are **persistent operator state**:
- They accumulate across circuit steps
- Large groups can have millions of entries
- They must survive checkpoints/restarts

NodeStorage provides the same capabilities for trees that Spine provides for batches:

| Capability | Spine (for Traces) | NodeStorage (for Trees) |
|------------|-------------------|------------------------|
| Disk spilling | Large batches spill | Leaf nodes spill |
| Memory management | Via BufferCache | Via BufferCache |
| Checkpointing | Trace::save/restore | save/restore |
| Incremental persistence | Dirty batch tracking | Dirty leaf tracking |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DBSP Circuit                              │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────┐         ┌──────────────────┐         │
│  │ Join Operator    │         │ Percentile Op    │         │
│  │                  │         │                  │         │
│  │ State: Trace     │         │ State: Trees     │         │
│  │   │              │         │   │              │         │
│  │   ▼              │         │   ▼              │         │
│  │ ┌──────┐         │         │ ┌─────────────┐  │         │
│  │ │Spine │         │         │ │NodeStorage  │  │         │
│  │ └──────┘         │         │ └─────────────┘  │         │
│  └──────────────────┘         └──────────────────┘         │
│           │                            │                    │
│           ▼                            ▼                    │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Runtime Infrastructure                  │   │
│  │  • StorageBackend (file I/O)                        │   │
│  │  • BufferCache (unified LRU caching)                │   │
│  │  • Checkpointer (persistence)                       │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Summary

NodeStorage is **not an optimization** but a **necessary storage abstraction** for the percentile operator's persistent state, following the standard DBSP pattern where each operator maintains its own incrementally-updated state.

| Aspect | Spine | NodeStorage |
|--------|-------|-------------|
| Purpose | Storage for Trace batches | Storage for B+ tree nodes |
| Data structure | Immutable sorted runs | Mutable augmented tree |
| Used by | Joins, distincts, etc. | Percentile aggregates |
| Role | Persistent operator state | Persistent operator state |
| Pattern | Standard DBSP | Standard DBSP |
