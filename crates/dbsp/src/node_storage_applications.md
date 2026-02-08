# NodeStorage Applications: Beyond Percentiles

This document analyzes SQL aggregate functions that could benefit from tree-based storage with node-level spill-to-disk (NodeStorage) compared to Spine-based batch serialization.

## Background: NodeStorage vs Spine Trade-offs

| Aspect | Spine | NodeStorage |
|--------|-------|-------------|
| **Data structure** | Immutable sorted batches | Mutable B+ tree |
| **Spill granularity** | Entire batches | Individual leaf nodes |
| **Update pattern** | Append new batch | In-place node modification |
| **Key-based lookup** | O(log n) via cursor | O(log n) via tree traversal |
| **Rank-based lookup** | O(n) scan across batches | O(log n) via subtree augmentation |
| **Memory when spilled** | Hot batches in cache | Internal nodes pinned, leaves spillable |

**Key insight**: NodeStorage excels when:
1. Queries require **rank/position information** (not just key lookup)
2. Data is **frequently updated** (not append-only)
3. Only a **subset of data** is accessed during queries (hot leaves stay cached)

## Functions Already Covered

See `algebra/order_statistics/order_statistics_sql_functions.md` for detailed analysis of:
- PERCENTILE_CONT, PERCENTILE_DISC, MEDIAN (implemented)
- NTILE, PERCENT_RANK, CUME_DIST, NTH_VALUE (not yet implemented)
- KTH_SMALLEST/LARGEST, CDF (not yet implemented)
- TRIMMED_MEAN, WINSORIZED_MEAN (partial benefit)

## Additional Functions Not Yet in Feldera

### Hypothetical-Set Aggregates (SQL:2003)

These are **aggregate** functions (not window functions) that compute what a ranking function would return for a hypothetical value inserted into the group.

| Function | Semantics | Tree Operation | Complexity |
|----------|-----------|----------------|------------|
| `rank(val) WITHIN GROUP (ORDER BY col)` | Rank with gaps if `val` were inserted | `rank(val) + 1` | O(log n) |
| `dense_rank(val) WITHIN GROUP (ORDER BY col)` | Rank without gaps if `val` were inserted | `dense_rank(val)` | O(log n) |
| `percent_rank(val) WITHIN GROUP (ORDER BY col)` | Relative rank if `val` were inserted | `rank(val) / total` | O(log n) |
| `cume_dist(val) WITHIN GROUP (ORDER BY col)` | Cumulative distribution if `val` were inserted | `(rank(val) + 1) / (total + 1)` | O(log n) |

**Example**:
```sql
-- "If we hired someone at $75,000, what would their salary rank be?"
SELECT rank(75000) WITHIN GROUP (ORDER BY salary) AS hypothetical_rank
FROM employees;
```

**Why Spine cannot do this efficiently**:
- Spine provides key-based cursor positioning, not rank queries
- Finding "how many values are less than X" requires scanning all batches: O(n)

**Why NodeStorage enables this**:
- `rank(value)` is a direct O(log n) operation via subtree weight sums
- The tree answers "count of elements < value" without scanning

**Implementation sketch**:
```rust
fn hypothetical_rank(&self, value: &T) -> i64 {
    self.rank(value) + 1  // 1-based rank
}

fn hypothetical_percent_rank(&self, value: &T) -> f64 {
    let total = self.total_weight();
    if total == 0 { return 0.0; }
    self.rank(value) as f64 / total as f64
}

fn hypothetical_cume_dist(&self, value: &T) -> f64 {
    let total = self.total_weight();
    (self.rank(value) + 1) as f64 / (total + 1) as f64
}
```

---

### Multi-Percentile Arrays

PostgreSQL and other databases support computing multiple percentiles in a single aggregate:

```sql
SELECT percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY value)
FROM measurements;
-- Returns: ARRAY[q1, median, q3]
```

**Why NodeStorage enables this**:
- Same tree, multiple O(log n) queries
- Total: O(k log n) for k percentiles
- With Spine: O(k × n) or O(n + k log n) with sorting

**Related: Interquartile Range (IQR)**
```sql
-- IQR = Q3 - Q1
SELECT percentile_cont(0.75) - percentile_cont(0.25) AS iqr
FROM measurements;
```

Two O(log n) queries on the same tree.

---

### MODE (Most Frequent Value)

**Current status**: Listed in `order_statistics_sql_functions.md` as "not benefiting" from OrderStatisticsZSet.

**However**, MODE could benefit from a **different** NodeStorage-backed tree:

| Approach | Data Structure | Find Mode | Update |
|----------|----------------|-----------|--------|
| Spine-based | Frequency counts in batches | O(n) scan all batches | O(log n) append |
| Frequency-keyed tree | Tree keyed by `(frequency, value)` | O(log n) find max | O(log n) rebalance |

**Frequency-keyed tree design**:
```
Tree keyed by (frequency DESC, value ASC):
  Internal nodes: [max_freq_in_subtree]
  Leaves: [(freq, value), ...]

Operations:
  - Find mode: O(1) - root tracks max frequency
  - Insert value: O(log n) - increment frequency, rebalance
  - Delete value: O(log n) - decrement frequency, rebalance
```

**Why this needs NodeStorage (not Spine)**:
- Frequencies change with every update (mutable, not append-only)
- Need to maintain sorted-by-frequency order for O(log n) max lookup
- Spine's immutable batches would require full rebuilds

**Note**: This requires a different tree structure than OrderStatisticsZSet. The same NodeStorage infrastructure could support it.

---

### General Window RANK/ROW_NUMBER/DENSE_RANK

**Current Feldera status**: Only supported for TopK patterns (via `topk.rs`).

**Limitation**: Cannot compute rank for arbitrary rows, only filter to top-k.

```sql
-- Currently supported (TopK pattern):
SELECT * FROM (
    SELECT *, row_number() OVER (ORDER BY score DESC) AS rn
    FROM players
) WHERE rn <= 10;

-- NOT currently supported (general rank):
SELECT player_id, score,
       rank() OVER (ORDER BY score DESC) AS ranking
FROM players;
-- Returns ALL rows with their ranks
```

#### Detailed Analysis: Can OrderStatisticsZSet Be Used?

**OrderStatisticsZSet data model**:
```rust
// Leaf stores (value, weight) pairs - NOT individual rows
LeafNode { entries: Vec<(T, ZWeight)> }

// Operations:
// - rank(value): sum of weights of all values < value  → O(log n)
// - select_kth(k): find value at cumulative position k → O(log n)
```

| Function | Required Query | OrderStatisticsZSet Support |
|----------|----------------|--------------------------------|
| **RANK()** | Count of rows with smaller values | ✅ `rank(value) + 1` works directly |
| **DENSE_RANK()** | Count of distinct values smaller | ❌ Needs `subtree_key_count` augmentation |
| **ROW_NUMBER()** | Unique position per row | ❌ Cannot distinguish rows with same value |

---

#### RANK(): Works As-Is ✅

**Semantics**: `RANK() = 1 + (count of rows with ORDER BY value < current row's value)`

```
Values:     [1, 2, 2, 3]  (value 2 has weight 2)
RANK():     [1, 2, 2, 4]
```

**Why OrderStatisticsZSet works**:
- `rank(value)` returns sum of weights of all values < `value`
- For value 3: `rank(3) = weight(1) + weight(2) = 1 + 2 = 3`
- Window RANK = `rank(value) + 1 = 4` ✓

**Data flow for RANK()**:
```
1. Original rows in Spine: (partition_key, row_data, order_by_value)
2. Per partition: OrderStatisticsZSet<order_by_value> with weights
3. Output: for each row, compute tree.rank(row.order_by_value) + 1
```

**Complexity**: O(n log n) for n rows (each row does O(log n) rank lookup)

---

#### DENSE_RANK(): Needs Modification ⚠️

**Semantics**: `DENSE_RANK() = 1 + (count of distinct ORDER BY values < current row's value)`

```
Values:        [1, 2, 2, 3]
DENSE_RANK():  [1, 2, 2, 3]  (no gaps)
RANK():        [1, 2, 2, 4]  (gap at 3)
```

**Why OrderStatisticsZSet doesn't work directly**:
- `rank(3)` returns weight sum = 3, not distinct count = 2
- Need count of distinct keys, not sum of weights

**Required modification** - add key count augmentation:
```rust
struct InternalNodeTyped<T> {
    keys: Vec<T>,
    children: Vec<NodeLocation>,
    subtree_sums: Vec<ZWeight>,       // Existing: sum of weights
    subtree_key_counts: Vec<usize>,   // NEW: count of distinct keys
}
```

With this augmentation:
- `dense_rank(value)` = count of distinct keys < value → O(log n)
- Same tree structure, same NodeStorage infrastructure
- Additional O(1) space per internal node entry

---

#### ROW_NUMBER(): Needs Different Structure ❌

**Semantics**: Each row gets unique sequential number 1..n, ties broken arbitrarily

```
Values:        [1, 2, 2, 3]
ROW_NUMBER():  [1, 2, 3, 4]  (both 2s get different numbers)
```

**Why OrderStatisticsZSet cannot support this**:
- Tree stores `(value → weight)`, not individual rows
- For two rows with `value=2`, tree sees `weight=2`, not two separate rows
- Cannot distinguish or order rows with identical ORDER BY values

**Options for ROW_NUMBER()**:

| Option | Structure | Key | Weight | Pros | Cons |
|--------|-----------|-----|--------|------|------|
| **A. Extended key** | OrderStatisticsZSet | `(value, row_id)` | 1 | Same tree ops | Larger keys, always unique |
| **B. Row storage** | New structure | `value` | N/A | Preserves rows | Different data model |
| **C. Post-join** | OSZSet + Spine | `value` | count | Reuses OSZSet | Complex join logic |

**Option A (Extended Key)** is most practical:
```rust
// Instead of: OrderStatisticsZSet<OrderByValue>
// Use:        OrderStatisticsZSet<(OrderByValue, RowId)>

// For rows: [(id=A, val=1), (id=B, val=2), (id=C, val=2), (id=D, val=3)]
// Tree keys: [(1,A), (2,B), (2,C), (3,D)]  all with weight=1
// ROW_NUMBER(id=C) = rank((2,C)) + 1 = 3 ✓
```

This transforms the weighted multiset into an ordered set with compound keys.

**Same NodeStorage infrastructure** works - just different key type.

---

#### Summary: Data Structure Requirements

| Function | OrderStatisticsZSet<T> | Modification Needed |
|----------|---------------------------|---------------------|
| RANK() | ✅ Use as-is | None |
| DENSE_RANK() | ⚠️ Modify | Add `subtree_key_count` augmentation |
| ROW_NUMBER() | ❌ Different key | Use `OrderStatisticsZSet<(T, RowId)>` with weight=1 |

**All three can use the same NodeStorage infrastructure** - the differences are:
1. **RANK**: Standard `(value → weight)` multiset
2. **DENSE_RANK**: Same structure + key count augmentation
3. **ROW_NUMBER**: `((value, row_id) → 1)` ordered set (degenerate multiset)

---

#### Architecture for General Window Ranking

```
┌─────────────────────────────────────────────────────────────────┐
│                     Window Ranking Operator                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Input: Stream<(PartitionKey, RowData, OrderByValue)>           │
│                                                                  │
│  Per-Partition State (NodeStorage-backed):                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  RANK():       OrderStatisticsZSet<OrderByValue>    │    │
│  │                (value → weight)                          │    │
│  ├─────────────────────────────────────────────────────────┤    │
│  │  DENSE_RANK(): OrderStatisticsZSet<OrderByValue>    │    │
│  │                + subtree_key_count augmentation          │    │
│  ├─────────────────────────────────────────────────────────┤    │
│  │  ROW_NUMBER(): OrderStatisticsZSet<(OrderByValue,   │    │
│  │                                         RowId)>         │    │
│  │                (compound_key → 1)                        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  Original Rows: Spine/Trace for row data retrieval              │
│                                                                  │
│  Output: for each (partition, row):                             │
│          rank_value = tree.rank(key) + 1                        │
│          emit (row, rank_value)                                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Why Spine struggles with general ranking**:
- TopK can discard non-top elements (bounded state)
- General ranking must track ALL elements and their positions
- With Spine: computing rank for a specific row requires O(n) scan

**Why NodeStorage enables this**:
- Tree maintains all elements with O(log n) rank queries
- For each row, `rank(value)` gives its position
- Incremental: when values change, only affected ranks update

---

#### Output Strategies for Window Ranking

**The fundamental challenge**: Inserting one row can shift ranks of O(n) existing rows.

```
Before: [10, 20, 30, 40, 50]  →  ranks: [1, 2, 3, 4, 5]
Insert 15:                    →  ranks: [1, 2, 3, 4, 5, 6]
                                         ↑  4 rows changed!
```

Three strategies for handling output, with different trade-offs:

---

##### Option 1: Full Materialization (Current Approach)

Compute and emit ranks for ALL rows after each batch.

```
Per batch:
  1. Update tree: O(B log n)
  2. Iterate all rows, query rank, emit: O(n log n)
```

| Metric | Complexity |
|--------|------------|
| State update | O(B log n) |
| Output | O(n log n) |
| **Total** | **O(n log n)** |

**Pros**: Simple, correct, no new infrastructure needed.
**Cons**: Output cost dominates; doesn't leverage O(log n) queries.

---

##### Option 2: Delta-Aware Output

Only emit rows whose ranks actually changed.

```
Per batch:
  1. Update tree: O(B log n)
  2. For each inserted value V:
     - Identify rows with value > V (ranks shifted)
     - Emit (row, old_rank, new_rank) for affected rows
```

| Metric | Complexity |
|--------|------------|
| State update | O(B log n) |
| Output | O(affected_rows) |
| **Total** | **O(B log n + affected)** |

Where `affected_rows`:
- Worst case: O(n) (insert smallest value)
- Best case: O(1) (insert largest value)
- Average: O(n/2) assuming uniform distribution

**Pros**: Avoids redundant output for unchanged rows.
**Cons**: Still O(n) worst case; requires tracking "affected range."

---

##### Option 3: Join-Based Lookup (On-Demand)

Don't materialize ranks eagerly. Compute only when downstream requests specific rows.

**Why this matters**: Many queries filter window function results:
```sql
SELECT * FROM (
    SELECT *, rank() OVER (ORDER BY score) AS rk FROM players
) WHERE player_id IN (SELECT id FROM active_players)  -- Only need ranks for active players!
```

**Architecture**:
```
┌─────────────────────────────────────────────────────────────┐
│  Rank State: OrderStatisticsZSet                         │
│  (maintained incrementally, ranks NOT materialized)          │
│                         │                                    │
│                         ▼ rank_lookup(value) → O(log n)      │
│  ┌─────────────────┐  ┌──────────────────────────────────┐  │
│  │ Query Stream    │──│ Rank Lookup Join                  │  │
│  │ (filtered rows) │  │ emit (row, tree.rank(val) + 1)   │  │
│  └─────────────────┘  └──────────────────────────────────┘  │
│                                    │                         │
│                         Output: O(Q log n) where Q = query   │
│                                 stream size, not n           │
└─────────────────────────────────────────────────────────────┘
```

| Metric | Complexity |
|--------|------------|
| State update | O(B log n) |
| Output | O(Q log n) where Q = query size |
| **Total** | **O(B log n + Q log n)** |

**Pros**: O(Q log n) when Q << n; ideal for filtered queries.
**Cons**: Requires new infrastructure (see below).

**Limitation: DBSP is push-based, not pull-based**

Unlike Haskell thunks (demand-driven), DBSP operators are triggered by the scheduler:

```rust
// DBSP: scheduler calls eval(), not consumer
pub trait UnaryOperator<I, O>: Operator {
    async fn eval(&mut self, input: &I) -> O;
}
```

| Aspect | Haskell Thunks | DBSP |
|--------|----------------|------|
| Trigger | Consumer demands | Scheduler pushes |
| Flow | Pull (lazy) | Push (eager) |

**Required infrastructure for Option 3**:
1. `RankLookupOperator` holding tree state, accepting query streams
2. Query planner to push filters below window functions
3. SQL pattern recognition: `WHERE rank < k` → use TopK instead

**Current status**: Not implemented. Only Option 1 (full materialization) is possible today.

---

#### Summary: Output Strategy Trade-offs

| Strategy | State Update | Output | Best For |
|----------|--------------|--------|----------|
| **Option 1**: Full materialization | O(B log n) | O(n log n) | Simple cases, small partitions |
| **Option 2**: Delta-aware | O(B log n) | O(affected) | Stable data, few rank changes |
| **Option 3**: Join-based lookup | O(B log n) | O(Q log n) | Filtered queries (Q << n) |

---

## Currently Implemented Functions That Could Benefit

### MAX/MIN with Frequent Deletions

**Current implementation**: Uses Spine-based trace with O(N) space, O(D log M) work.

**Scenario where NodeStorage helps**:
- High-churn workloads with many inserts AND deletes
- Large groups where most data is "cold" (far from current max/min)

**With Spine**:
- Deleting the current max requires finding the new max
- May need to scan multiple batches to find next-largest value
- Batches containing deleted values remain until compaction

**With tree (NodeStorage)**:
- Max/min is always O(log n) or O(1) with caching
- Deletions update tree in-place, O(log n)
- Cold subtrees (far from extrema) spill to disk

**Trade-off**: For append-only or low-deletion workloads, Spine is simpler and sufficient.

---

### ARRAY_AGG with ORDER BY

**Current status**: Listed as "expensive" - O(N) space, O(M) work.

```sql
SELECT customer_id,
       array_agg(order_id ORDER BY order_date) AS orders
FROM orders
GROUP BY customer_id;
```

**With Spine**:
- Accumulates all values in batches
- Final ORDER BY requires sorting or merging sorted batches
- Large arrays may cause memory pressure during materialization

**With tree (NodeStorage)**:
- Maintains sorted order incrementally
- Leaf spilling for large aggregations (million-element arrays)
- Final iteration is O(n) but memory-bounded by leaf cache

**Primary benefit**: Memory management for very large ordered arrays, not algorithmic improvement.

---

### High-Cardinality DISTINCT

```sql
SELECT COUNT(DISTINCT user_id) FROM events;  -- millions of unique users
```

**With Spine**:
- Distinct values stored in trace batches
- Membership test: O(log n) per batch, but may need to check multiple batches
- Memory: all batches for a group may need to be in memory for merging

**With tree (NodeStorage)**:
- Single tree per group with unique values
- Membership test: O(log n) single tree lookup
- Spilling: cold portions of the distinct set spill to disk

**Benefit**: Better memory characteristics for high-cardinality columns (UUIDs, user IDs, etc.)

---

## Summary: NodeStorage Candidates

### High Priority (Not in Feldera, Clear Benefit)

| Function | Why NodeStorage | Complexity |
|----------|-----------------|------------|
| Hypothetical-set aggregates | O(log n) rank queries | O(log n) |
| Multi-percentile arrays | Same tree, multiple queries | O(k log n) |
| General window RANK/ROW_NUMBER | O(log n) per-row rank | O(n) total, O(log n) per query |

### Medium Priority (In Feldera, Could Improve)

| Function | Current Approach | NodeStorage Benefit |
|----------|------------------|---------------------|
| MAX/MIN (high churn) | Spine-based trace | Better deletion handling |
| ARRAY_AGG ORDER BY | Full materialization | Memory-bounded leaf spilling |
| High-cardinality DISTINCT | Spine batches | Single-tree membership, leaf spilling |

### Requires Different Tree Structure

| Function | Required Structure | Notes |
|----------|-------------------|-------|
| MODE | Frequency-keyed tree | Same NodeStorage infra, different tree type |

### No Benefit from NodeStorage

| Function | Why Not |
|----------|---------|
| SUM, COUNT, AVG | Simple scalar accumulator |
| STDDEV, VARIANCE | Online algorithm, O(1) state |
| BIT_AND/OR/XOR | Accumulator pattern |
| EVERY, SOME | Boolean accumulator |
| CORR, COVAR_* | Online algorithm |
| MAD | Requires second tree for deviations |

## References

- [PostgreSQL Aggregate Functions](https://www.postgresql.org/docs/current/functions-aggregate.html) - Ordered-set and hypothetical-set aggregates
- [SQL:2003 Standard](https://www.iso.org/standard/34132.html) - Window function specifications
- `algebra/order_statistics/order_statistics_sql_functions.md` - Percentile family analysis
- `algebra/order_statistics/order_statistics_storage_vs_spine.md` - Storage comparison
- `node_storage.md` - NodeStorage design
