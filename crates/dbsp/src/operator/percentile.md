# Percentile Operator

Stateful operator for computing SQL `PERCENTILE_CONT` and `PERCENTILE_DISC` incrementally.

## Overview

The `PercentileOperator` maintains per-key `OrderStatisticsMultiset` state across steps, enabling O(log n) incremental updates instead of O(n) per-step rescanning used by the generic aggregate approach.

## Architecture

```
PercentileOperator<K, V>
├── trees: BTreeMap<K, OrderStatisticsMultiset<V>>  // Per-key state
├── prev_output: BTreeMap<K, Option<V>>              // Previous outputs for delta computation
├── percentile: f64                                   // Percentile to compute (0.0-1.0)
├── ascending: bool                                   // Sort order
├── continuous: bool                                  // CONT vs DISC mode
└── storage_config: NodeStorageConfig                 // Spill-to-disk config
```

## Usage

```rust
use dbsp::{Circuit, Stream, typed_batch::OrdIndexedZSet};

// Compute median (50th percentile) for each key
let medians: Stream<_, OrdIndexedZSet<K, Option<V>>> = indexed_zset
    .percentile_cont(0.5, true);

// Compute discrete 90th percentile
let p90: Stream<_, OrdIndexedZSet<K, Option<V>>> = indexed_zset
    .percentile_disc(0.9, true);

// Compute median with stateful API (same as above)
let medians = indexed_zset.median();
```

## Stream Extension Methods

### `percentile_cont(percentile: f64, ascending: bool)`

Computes PERCENTILE_CONT (interpolated percentile) for each key group. For generic types, returns the discrete lower bound at the percentile position.

### `percentile_disc(percentile: f64, ascending: bool)`

Computes PERCENTILE_DISC (discrete percentile) - returns an actual value from the set at the percentile position.

### `median()`

Convenience method equivalent to `percentile_cont(0.5, true)`.

## Delta Processing

The operator:
1. Processes incoming deltas (insertions with weight > 0, deletions with weight < 0)
2. Updates per-key `OrderStatisticsMultiset` trees incrementally
3. Computes new percentile values
4. Emits delta output: `-1` for old value, `+1` for new value (only if changed)

## Implementation Files

- **`operator/dynamic/percentile_op.rs`**: Stateful operator implementation
- **`operator/percentile.rs`**: Stream extension methods

## SQL Compiler Integration

The SQL compiler generates code using `DBSPPercentileOperator`, which calls the stream methods
`percentile_cont()` or `percentile_disc()`. The generated code flow:

1. SQL: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val)`
2. `CalciteToDBSPCompiler` detects percentile aggregates and creates `DBSPPercentileOperator`
3. `ToRustVisitor` generates: `input.map_index(|k,v| ...).percentile_cont(0.5, true)`
4. At runtime, `PercentileOperator` maintains incremental state per key

## Fault Tolerance

The operator implements checkpoint/restore for fault tolerance with O(num_leaves) complexity:

### Checkpoint
- Flushes dirty tree leaves to disk via `save_leaves()`
- Stores `CommittedLeafStorage<V>` metadata (file path, leaf summaries)
- Leaf summaries contain (first_key, weight_sum, entry_count) per leaf

### Restore
- Rebuilds internal nodes from leaf summaries (no leaf I/O required)
- Checkpoint file becomes the live spill file (zero-copy ownership transfer)
- Leaves loaded lazily on demand from disk

This approach is O(num_leaves) instead of O(num_entries), enabling fast recovery for large trees.

## Limitations

- **Numeric interpolation**: Generic implementation returns discrete lower bound

## Related Documentation

- `algebra/order_statistics/order_statistics_multiset.md` - Augmented B+ tree design
- `node_storage.md` - Spill-to-disk storage abstraction
