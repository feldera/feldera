# Percentile Operator

Stateful operator for computing SQL `PERCENTILE_CONT` and `PERCENTILE_DISC` incrementally.

## Overview

The `PercentileOperator` maintains per-key `OrderStatisticsMultiset` state across steps, enabling O(log n) incremental updates instead of O(n) per-step rescanning used by the generic aggregate approach.

## Architecture

```
PercentileOperator<K, V, Mode>
├── trees: BTreeMap<K, OrderStatisticsMultiset<V>>  // Per-key state
├── tree_ids: BTreeMap<K, u64>                       // Unique IDs per tree for checkpoint naming
├── prev_output: BTreeMap<K, Option<V>>              // Previous outputs for delta computation
│                                                     // Keys persist even after tree cleanup
│                                                     // to enable proper NULL retraction
├── percentile: f64                                   // Percentile to compute (0.0-1.0)
├── ascending: bool                                   // Sort order
├── storage_config: NodeStorageConfig                 // Spill-to-disk config
├── next_tree_id: u64                                 // Counter for unique segment path prefixes
├── global_id: GlobalNodeId                           // For checkpoint file naming
└── _mode: PhantomData<Mode>                          // ContMode or DiscMode (zero-sized)
```

`Mode` is a compile-time marker type — either `ContMode` (interpolated) or `DiscMode` (discrete). This avoids a runtime `continuous: bool` flag and lets the two modes have different trait bounds at compile time.

## Interpolate Trait

`PERCENTILE_CONT` requires the `Interpolate` trait for linear interpolation between adjacent values:

```rust
pub trait Interpolate: Clone {
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self;
}
```

Implemented for: `f32`, `f64`, `F32`, `F64`, and `Option<T: Interpolate>`.
Not implemented for integers — the SQL compiler auto-casts ORDER BY values to DOUBLE before calling `percentile_cont`.

## Stream Extension Methods

Two sets of methods are provided:

### Sharded methods (`operator/percentile.rs`)

These call `self.shard()` first to ensure all values for a key land on the same worker in multi-threaded execution. This is the primary API used by the SQL compiler.

```rust
// PERCENTILE_CONT — requires V: Interpolate
fn percentile_cont(&self, persistent_id: Option<&str>, percentile: f64, ascending: bool)
    -> Stream<C, OrdIndexedZSet<K, Option<V>>>

// PERCENTILE_DISC — works with any ordered type
fn percentile_disc(&self, persistent_id: Option<&str>, percentile: f64, ascending: bool)
    -> Stream<C, OrdIndexedZSet<K, Option<V>>>

// Convenience: percentile_cont(None, 0.5, true)
fn median(&self) -> Stream<C, OrdIndexedZSet<K, Option<V>>>
```

### Non-sharded methods (`operator/dynamic/percentile_op.rs`)

These operate directly without sharding — the caller is responsible for ensuring correct partitioning. Used in tests and when the input is already sharded.

```rust
fn percentile_cont_stateful(&self, persistent_id: Option<&str>, percentile: f64, ascending: bool)
    -> Stream<C, OrdIndexedZSet<K, Option<V>>>

fn percentile_disc_stateful(&self, persistent_id: Option<&str>, percentile: f64, ascending: bool)
    -> Stream<C, OrdIndexedZSet<K, Option<V>>>

fn median_stateful(&self, persistent_id: Option<&str>)
    -> Stream<C, OrdIndexedZSet<K, Option<V>>>
```

### Usage example

```rust
use dbsp::{Circuit, Stream, typed_batch::OrdIndexedZSet};

// Sharded (multi-worker safe)
let medians = indexed_zset.percentile_cont(None, 0.5, true);
let p90 = indexed_zset.percentile_disc(Some("p90_op"), 0.9, true);

// Non-sharded (single-worker or pre-sharded input)
let medians = indexed_zset.percentile_cont_stateful(None, 0.5, true);
```

## Delta Processing

The operator processes each step as follows:

1. Iterates the input delta batch key-by-key via cursor
2. Skips NULL values (`value.is_none()`) — SQL percentile functions exclude NULLs
3. Sums weights across timestamps and calls `tree.insert(value, weight)` for non-zero weights
4. If a tree exceeds the flush threshold during ingestion, flushes proactively (inline backpressure)
5. For each changed key, computes the new percentile:
   - **CONT**: `tree.select_percentile_bounds()` → `V::interpolate(lower, upper, fraction)`
   - **DISC**: `tree.select_percentile_disc()` → returns actual value
6. Compares with `prev_output` and emits delta: `-1` for old value, `+1` for new value (only if changed)
7. Cleans up empty trees but retains `prev_output` entries for NULL retraction tracking

### Empty Group NULL Semantics

When a GROUP BY group becomes empty (all values removed), the operator emits `(key, NULL) => +1`.
The `prev_output` map retains the entry `key → None` even after the tree is cleaned up.
When the group later receives new values, the operator correctly emits `(key, NULL) => -1`
followed by `(key, new_value) => +1`, ensuring proper NULL retraction.

### First-time vs subsequent emission

The operator distinguishes "never emitted for this key" from "previously emitted None" using `prev_output.contains_key(&key)`. A key absent from `prev_output` means no prior output — so no retraction is emitted. A key present with value `None` means NULL was previously emitted and must be retracted.

## Lifecycle

- **`clock_end`**: Flushes dirty leaves and evicts clean leaves for all trees via `flush_and_evict()`. By this point all mutations for the step are complete, making eviction safe.
- **`fixedpoint`**: Returns `true` only when `trees` is empty (no state).

## Fault Tolerance

The operator implements checkpoint/restore for fault tolerance.

### Checkpoint

1. Each per-key tree's `OrderStatisticsMultiset` writes its own metadata file and registers segment files via `tree.save(base, tree_pid, files)`. This follows the Spine pattern where the storage layer manages its own persistence.
2. Operator-level metadata (`CommittedPercentileOperator`) is written separately:
   - `tree_ids`: `Vec<(K, u64)>` — which trees exist and their IDs
   - `prev_output`: `Vec<(K, Option<V>)>` — previous output values
   - `percentile`, `ascending`, `branching_factor` — config
   - `next_tree_id` — counter to prevent ID collisions after restore

### Restore

1. Reads `CommittedPercentileOperator` metadata file
2. Restores `next_tree_id` to prevent ID collisions
3. For each `(key, tree_id)`, calls `OrderStatisticsMultiset::restore()` which reads its own NodeStorage metadata and rebuilds internal nodes from leaf summaries
4. Restores `prev_output` for correct delta computation on next step

Complexity is O(num_leaves) per tree, not O(num_entries) — internal nodes are rebuilt from leaf summaries without reading leaf data.

### Checkpoint file naming

- Operator metadata: `{base}/percentile-{persistent_id}.dat`
- Per-tree metadata: delegated to `OrderStatisticsMultiset::save()` with `persistent_id = "{op_persistent_id}_t{tree_id}"`
- Segment files use prefix `t{tree_id}_` to prevent collisions across trees sharing the same StorageBackend namespace

## Limitations

- **Numeric interpolation**: `PERCENTILE_CONT` requires the `Interpolate` trait — only `F32`/`F64` (and their `Option` wrappers). The SQL compiler auto-casts non-DOUBLE ORDER BY values to DOUBLE before calling this operator.
- **PERCENTILE_DISC**: Works with any ordered type — no interpolation constraint.

## Implementation Files

- **`operator/percentile.rs`**: Sharded stream extension methods (primary API)
- **`operator/dynamic/percentile_op.rs`**: `PercentileOperator` struct, `Interpolate` trait, `ContMode`/`DiscMode` markers, non-sharded `_stateful` methods, checkpoint/restore logic
- **`operator/dynamic/percentile_op_tests.rs`**: Unit tests

## SQL Compiler Integration

The SQL compiler generates code using `DBSPPercentileOperator`, which calls the sharded stream methods
`percentile_cont()` or `percentile_disc()`. The generated code flow:

1. SQL: `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val)`
2. `CalciteToDBSPCompiler` detects percentile aggregates and creates `DBSPPercentileOperator`
3. `ToRustVisitor` generates: `input.map_index(|k,v| ...).percentile_cont(Some("op_id"), 0.5, true)`
4. At runtime, `PercentileOperator` maintains incremental state per key

**Note**: `DBSPPercentileOperator` has `valueExtractor` and `postProcessor` closures that require a custom `postorder` in `CircuitRewriter` to be properly transformed through optimization passes (e.g., `ExpandUnsafeCasts`).

## Related Documentation

- `algebra/order_statistics/order_statistics_multiset.md` - Augmented B+ tree design
- `node_storage.md` - Spill-to-disk storage abstraction
