## Overview

Dynamic operator implementations providing runtime-flexible streaming computation. These operators use dynamic dispatch via trait objects to enable SQL compiler integration without excessive monomorphization.

## Operator Summary

### Core Operators

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `filter_map.rs` | Element-wise filter and map transformations | stateless |
| `consolidate.rs` | Merge and deduplicate weighted collections | key lookup |
| `distinct.rs` | Remove duplicate records | key lookup |
| `index.rs` | Create indices for efficient lookups | stateless |
| `output.rs` | Export data from circuits | stateless |
| `input.rs` | Add input streams to circuits | stateless |
| `input_upsert.rs` | Apply insert/update/delete operations | key lookup |
| `upsert.rs` | Convert commands to Z-set updates | key lookup |
| `trace.rs` | Manage persistent state traces | key lookup |
| `accumulate_trace.rs` | Manage accumulated state traces | key lookup |
| `accumulator.rs` | Accumulation with user functions | accumulator |

### Join Operators

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `join.rs` | Hash and indexed joins | key lookup |
| `outer_join.rs` | Left/outer join semantics | key lookup |
| `semijoin.rs` | Filter by presence in another stream | key lookup |
| `join_range.rs` | Range-based joins | key lookup |
| `asof_join.rs` | Temporal as-of joins | key lookup |
| `multijoin.rs` | Multi-way star joins | key lookup |
| `saturate.rs` | Outer join ghost tuple support | key lookup |

### Control Flow Operators

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `recursive.rs` | Fixed-point iteration for recursive queries | key lookup |
| `controlled_filter.rs` | Conditional filtering with control signal | stateless |
| `concat.rs` | Combine multiple input streams | stateless |

### Distribution Operators

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `communication.rs` | Multi-worker data exchange | stateless |
| `balance.rs` | Load balancing across workers | stateless |
| `sample.rs` | Random sampling from streams | accumulator |
| `neighborhood.rs` | Extract rows around anchor point | key lookup |
| `count.rs` | Weighted and distinct counting | accumulator |

### Aggregate Operators (`aggregate/`)

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `aggregator.rs` | Base trait for all aggregators | — |
| `fold.rs` | Generic fold-based aggregation | accumulator |
| `min.rs` | Find minimum value | cursor scan |
| `max.rs` | Find maximum value | cursor scan |
| `average.rs` | Compute average via (sum, count) | accumulator |
| `chain_aggregate.rs` | Incremental aggregate maintenance | accumulator |

### Group Operators (`group/`)

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `lag.rs` | Lag/lead window functions | cursor scan |
| `topk.rs` | Top-K, rank, dense_rank, row_number | cursor scan |

### Time Series Operators (`time_series/`)

| Operator | Purpose | Query Type |
|----------|---------|------------|
| `window.rs` | Time-based windowing | key lookup |
| `waterline.rs` | Progress frontier tracking | accumulator |
| `rolling_aggregate.rs` | Rolling aggregates via radix tree | range query |
| `partitioned.rs` | Two-level indexed collections | key lookup |
| `range.rs` | Time range abstractions | — |
| `radix_tree/` | Prefix-organized tree aggregation | range query |

## Query Types

Operators access data using different query patterns, which determines their performance characteristics and data structure requirements:

| Query Type | Description | Complexity |
|------------|-------------|------------|
| **stateless** | Processes each record independently without maintaining state | O(1) per record |
| **key lookup** | Retrieves or updates values by key in sorted traces/indices | O(log n) per key |
| **cursor scan** | Iterates sequentially through sorted trace until condition met | O(k) where k = position |
| **accumulator** | Maintains running aggregate state (sum, count, etc.) | O(1) per update |
| **range query** | Queries over contiguous key/time ranges using tree structures | O(log n) per range |
| **select k-th** | Finds element at arbitrary position k in weighted multiset | O(log n) per query |

Most operators use **key lookup** or **cursor scan** patterns that work efficiently with standard sorted traces.

## Operator Overview

### Core Operators

**`filter_map.rs`** - Provides element-wise transformations including `filter`, `map`, `flat_map`, and combined `filter_map` operations. Each record passes through independently with its weight preserved; no state is maintained across records.

**`consolidate.rs`** - Merges all batches in a trace into a single consolidated batch. Combines duplicate keys by summing their weights, eliminating zero-weight entries.

**`distinct.rs`** - Removes duplicate records by converting any positive weight to 1 and any negative weight to -1. Maintains incremental state to track which keys are present across steps.

**`index.rs`** - Converts flat Z-sets with `(key, value)` tuple keys into indexed Z-sets where keys and values are separated. The indexed representation enables efficient joins and group-by operations.

**`output.rs`** - Provides `OutputHandle` methods to extract and consolidate results from circuit outputs. Collects data from all workers and merges into a single batch.

**`input.rs`** - Creates input streams with `InputHandle` for feeding data into circuits. Supports both Z-sets and indexed Z-sets with optional waterline tracking.

**`input_upsert.rs`** - Handles upsert (insert/update/delete) semantics for input streams. Converts update commands into proper Z-set deltas with insert (+1) and delete (-1) weights.

**`upsert.rs`** - Converts upsert command streams into Z-set update streams. Tracks current state to emit correct deltas when values change or are deleted.

**`trace.rs`** - Manages persistent state traces that accumulate batches over time. Provides bounded traces with configurable key/value retention and supports checkpoint/restore.

**`accumulate_trace.rs`** - Similar to `trace.rs` but for time-stamped traces used in nested circuits. Maintains temporal history for operators that need to look back in time.

**`accumulator.rs`** - Provides user-defined accumulation over streams. Applies a fold function across batches to maintain running state.

### Join Operators

**`join.rs`** - Implements equi-joins between two indexed Z-sets by matching keys. Supports inner join with incremental updates—only processes changed records, not the entire dataset.

**`outer_join.rs`** - Implements left outer join semantics where unmatched left-side keys produce output with a default (None) right value. Uses ghost tuples from `saturate.rs` to track key presence.

**`semijoin.rs`** - Filters an indexed Z-set to keep only keys that exist in a separate key stream. Efficiently implements EXISTS subqueries in SQL.

**`join_range.rs`** - Performs non-equi joins where each left key matches a contiguous range of right keys. Used for inequality joins like `a.x BETWEEN b.lo AND b.hi`.

**`asof_join.rs`** - Implements temporal as-of joins that match each left record with the most recent right record by timestamp. Commonly used for point-in-time lookups in time-series data.

**`multijoin.rs`** - Provides multi-way star joins where a central fact table joins with multiple dimension tables. More efficient than chaining binary joins for star schemas.

**`saturate.rs`** - Injects ghost tuples `(key, None)` for keys not present in a collection. Auxiliary operator that enables left outer joins by ensuring all keys appear to exist.

### Control Flow Operators

**`recursive.rs`** - Enables fixed-point iteration for recursive SQL queries (WITH RECURSIVE). Repeatedly applies a transformation until the output stabilizes (reaches a fixedpoint).

**`controlled_filter.rs`** - Filters records based on a dynamic threshold signal, returning both passing records and an error stream. Used for implementing constraints and validation with error reporting.

**`concat.rs`** - Combines multiple input streams into a single output stream via accumulate-concat. Supports both additive (union) and subtractive (difference) stream combination.

### Distribution Operators

**`communication.rs`** - Handles data exchange between workers in multi-threaded execution. Includes `gather` (collect to one worker) and `shard` (distribute by key hash) operations.

**`balance.rs`** - Implements load balancing by redistributing data across workers. Uses MaxSAT-based optimization to minimize data movement while achieving balance.

**`sample.rs`** - Computes random samples from streams using reservoir sampling. Supports sampling keys, key-value pairs, and computing quantile sketches.

**`neighborhood.rs`** - Extracts a contiguous range of rows around an anchor point. Returns rows with relative position indices for implementing scrollable result windows.

**`count.rs`** - Provides weighted counting and distinct counting aggregations. Supports both simple count (sum of weights) and count-distinct (number of unique keys).

### Aggregate Operators (`aggregate/`)

**`aggregator.rs`** - Defines the `DynAggregator` trait that all aggregate functions implement. Specifies combine, output, and identity operations for incremental aggregation.

**`fold.rs`** - Generic fold-based aggregation that applies a user function to accumulate values. The foundation for implementing custom aggregate functions.

**`min.rs`** - Computes minimum values using cursor traversal on sorted traces. Scans forward from the beginning until finding the first element with non-zero weight, leveraging the trace's sorted order for O(position) early termination.

**`max.rs`** - Computes maximum values using cursor traversal on sorted traces. Fast-forwards to the end and scans backward until finding the first element with non-zero weight, symmetric to `min.rs` with O(n - position) complexity.

**`average.rs`** - Computes averages by maintaining (sum, count) pairs. The final average is computed as sum/count during output.

**`chain_aggregate.rs`** - Chains multiple aggregators together for computing several aggregates in one pass. Avoids redundant scans when computing multiple aggregates over the same grouping.

### Group Operators (`group/`)

**`lag.rs`** - Implements LAG/LEAD SQL window functions that access values at a fixed offset from the current row. Iterates sequentially from cursor position—O(offset) per row, not O(log n).

**`topk.rs`** - Implements TOP-K selection and ranking functions (ROW_NUMBER, RANK, DENSE_RANK). Iterates through sorted values sequentially to assign ranks—O(K) not O(log n).

### Time Series Operators (`time_series/`)

**`window.rs`** - Filters data to a sliding time window defined by lower and upper bounds. Emits deltas as records enter and exit the window.

**`waterline.rs`** - Computes progress watermarks across a stream by tracking the maximum timestamp seen. Used for determining when time-based windows can be closed.

**`rolling_aggregate.rs`** - Computes aggregates over rolling time-based windows using radix tree acceleration. Supports range queries like "sum of last 24 hours" with O(log n) updates.

**`partitioned.rs`** - Provides two-level indexed collections partitioned by an outer key then inner timestamp. Foundation for time-series operators that group by entity then time.

**`range.rs`** - Defines range types and cursor interfaces for time-based queries. Supports both absolute ranges and relative offsets.

**`radix_tree/`** - Prefix-organized tree structure for efficient range aggregation. Maintains partial aggregates at each tree level for O(log n) range queries.
