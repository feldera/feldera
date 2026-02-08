# SQL Functions Enabled by OrderStatisticsZSet

## Abstract

OrderStatisticsZSet enables efficient incremental computation for **distribution and positional SQL functions** - a family of aggregate and window functions that require:

1. **Select-by-position**: Find the element at cumulative position k (e.g., "what value is at the 50th percentile?")
2. **Rank queries**: Find the cumulative position of a given value (e.g., "what percentile is value X at?")

These operations are mathematically dual: select maps position → value, while rank maps value → position. Traditional implementations require O(n) linear scans; OrderStatisticsZSet provides O(log n) for both operations through subtree weight augmentation in a B+ tree structure.

### Implementation Tiers

| Tier | Examples | Complexity | Approach |
|------|----------|------------|----------|
| **1. Direct** | PERCENTILE_*, KTH_*, NTH_VALUE, CDF | O(log n) | Single tree operation |
| **2. Composable** | TRIMMED_MEAN, WINSORIZED_MEAN | O(n) | Percentile + filter/aggregate |
| **3. Per-row** | NTILE, PERCENT_RANK, CUME_DIST | O(n) | Output size dominates |
| **4. Not efficient** | MAD, MODE | O(n log n) | Requires different structures |

See [Implementation Hierarchy](#implementation-hierarchy) for detailed analysis and decision tree.

This family includes percentile functions, distribution functions, and bucketing functions - all essential for statistical analysis, reporting, and data quality assessment in analytical SQL workloads.

## Implementation Status

| Function | Status | Query | Batch | Notes |
|----------|--------|-------|-------|-------|
| PERCENTILE_CONT | ✅ Implemented | O(log n) | O(log n) | Single value output |
| PERCENTILE_DISC | ✅ Implemented | O(log n) | O(log n) | Single value output |
| MEDIAN | ✅ Implemented | O(log n) | O(log n) | Via PERCENTILE_CONT(0.5) |
| NTILE | ❌ Not implemented | O(log n) | O(n) | Per-row; O(n) batch via iteration |
| PERCENT_RANK | ❌ Not implemented | O(log n) | O(n) | Per-row window function |
| CUME_DIST | ❌ Not implemented | O(log n) | O(n) | Per-row window function |
| NTH_VALUE | ❌ Not implemented | O(log n) | O(log n) | Single value output |
| KTH_SMALLEST/LARGEST | ❌ Not implemented | O(log n) | O(log n) | Direct select_kth mapping |
| CDF (rank-of-value) | ❌ Not implemented | O(log n) | O(log n) | Inverse of percentile |
| TRIMMED_MEAN | ⚠️ Partial | O(log n)* | O(n) | *Boundaries only; sum requires iteration |
| WINSORIZED_MEAN | ⚠️ Partial | O(log n)* | O(n) | *Boundaries only; sum requires iteration |
| MAD | ❌ No benefit | O(n log n) | O(n log n) | Requires second tree for deviations |

## Implementation Hierarchy

This section categorizes functions by how they can be efficiently implemented, from most to least efficient.

### Tier 1: Direct Percentile Operator (O(log n))

These functions map directly to OrderStatisticsZSet operations with **single-value output**:

| Function | Implementation | Why O(log n) |
|----------|----------------|--------------|
| PERCENTILE_CONT | `select_percentile_bounds()` | Single position lookup |
| PERCENTILE_DISC | `select_percentile_disc()` | Single position lookup |
| MEDIAN | `select_percentile_bounds(0.5)` | Single position lookup |
| KTH_SMALLEST | `select_kth(k-1)` | Single position lookup |
| KTH_LARGEST | `select_kth_desc(k-1)` | Single position lookup |
| NTH_VALUE | `select_kth(n-1)` | Single position lookup |
| CDF (rank-of-value) | `rank(v) + weight(v)` | Single rank lookup |

**Incremental cost:** O(B log n) for batch of B updates + O(log n) query = **O(B log n) total**

**Key property:** Output is a single value, so query cost dominates.

---

### Tier 2: Percentile + Composition with Existing DBSP Operators (O(n))

These functions use percentile for boundaries, then compose with filter/map/aggregate:

#### TRIMMED_MEAN

```
input ─┬─► PERCENTILE_DISC(p) ────► lower ─┐
       │                                    ├─► FILTER ─► AVG
       ├─► PERCENTILE_DISC(1-p) ──► upper ─┘
       │
       └────────────────────────────────────┘
```

```sql
-- Composable SQL pattern
WITH bounds AS (
    SELECT
        PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY val) AS lower,
        PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY val) AS upper
    FROM data
)
SELECT AVG(val) FROM data
WHERE val BETWEEN (SELECT lower FROM bounds) AND (SELECT upper FROM bounds);
```

| Step | Operator | Cost |
|------|----------|------|
| Compute bounds | percentile (×2) | O(log n) |
| Filter by bounds | filter | O(n) |
| Compute average | aggregate | O(n) |
| **Total** | | **O(n)** |

**Incremental behavior:** When bounds are stable (common), filter passes deltas through. When bounds shift, values near boundaries re-evaluate.

#### WINSORIZED_MEAN

```
input ─┬─► PERCENTILE_DISC(p) ────► lower ─┐
       │                                    ├─► MAP(clamp) ─► AVG
       ├─► PERCENTILE_DISC(1-p) ──► upper ─┘
       │
       └────────────────────────────────────┘
```

```sql
WITH bounds AS (
    SELECT
        PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY val) AS lower,
        PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY val) AS upper
    FROM data
)
SELECT AVG(GREATEST(LEAST(val, upper), lower)) FROM data, bounds;
```

Same complexity as TRIMMED_MEAN.

**Why these compose well:**
1. Boundaries are single values (Tier 1 efficiency)
2. Filter/map/aggregate are standard DBSP operators
3. Incremental semantics are well-defined

---

### Tier 3: Per-Row Output Functions (O(n) minimum)

These functions produce **one output value per input row**, making O(n) unavoidable:

| Function | Output | Why O(n) minimum |
|----------|--------|------------------|
| PERCENT_RANK | rank for each row | n output values |
| CUME_DIST | distribution for each row | n output values |
| NTILE | bucket for each row | n output values |

**The fundamental issue:** Even with O(1) rank lookups, emitting n values costs O(n).

**Incremental problem:** Small input changes cause large output changes:
```
Before: [1, 2, 3, 4, 5]  →  ranks: [1, 2, 3, 4, 5]
Insert 0: [0, 1, 2, 3, 4, 5]  →  ranks: [1, 2, 3, 4, 5, 6]
                                        ↑  ALL ranks shifted!
```

When you insert 1 row:
- All rows with larger values have rank += 1
- Total count changes
- PERCENT_RANK = rank/(total-1) changes for **every** row
- Output delta: O(n), not O(1)

**Implementation approaches:**

1. **Batch backfill (all rows):** O(n) via sorted iteration
   ```rust
   // Iterate in order, track cumulative position
   let mut pos = 0;
   for (value, weight) in tree.iter() {
       let percent_rank = pos as f64 / (total - 1) as f64;
       emit(value, percent_rank);
       pos += weight;
   }
   ```

2. **Single-value query:** O(log n) via `rank()`
   ```rust
   let percent_rank = tree.rank(&value) as f64 / (tree.total_weight() - 1) as f64;
   ```

3. **Incremental (batch of B changes):** Output delta is O(n) worst case
   - Not a limitation of the data structure
   - Fundamental to the function's semantics

**Recommendation:** These functions are best suited for:
- Batch/backfill scenarios (compute once for all rows)
- Single-value queries ("what's the percentile of THIS specific value?")
- NOT for incremental per-row tracking (output instability)

---

### Tier 4: Not Efficient with Existing Operators

#### MAD (Median Absolute Deviation)

```
MAD = median(|x_i - median(x)|)
```

**Why composition fails:**

| Step | Operation | Issue |
|------|-----------|-------|
| 1. Compute median(x) | percentile | O(log n) ✓ |
| 2. Compute \|x_i - median\| | map | O(n) ✓ |
| 3. Compute median of deviations | **NEW percentile tree** | O(n log n) ✗ |

The deviations form a **completely different ordered set**:
```
Original: [1, 2, 3, 10, 100]  →  median = 3
Deviations: [2, 1, 0, 7, 97]  →  completely different order!
```

No composition of existing operators avoids building a second tree.

**Total cost:** O(n log n) regardless of approach.

**Alternatives:**
- Approximate MAD via streaming sketches (t-digest)
- Accept O(n log n) cost for exact computation

---

### Summary: Implementation Decision Tree

```
Is output a single value?
├─ YES → Is it a direct position/rank query?
│        ├─ YES → Tier 1: Direct percentile operator, O(log n)
│        └─ NO  → Tier 2: Compose with filter/map/aggregate, O(n)
│
└─ NO (per-row output) →
         Does it require a second ordering?
         ├─ YES → Tier 4: Not efficient, O(n log n)
         └─ NO  → Tier 3: Per-row functions, O(n) minimum
```

| Tier | Functions | Query | Incremental | Notes |
|------|-----------|-------|-------------|-------|
| 1 | PERCENTILE_*, KTH_*, NTH_VALUE, CDF | O(log n) | O(B log n) | Best case |
| 2 | TRIMMED_MEAN, WINSORIZED_MEAN | O(n) | O(n)* | *Stable bounds help |
| 3 | PERCENT_RANK, CUME_DIST, NTILE | O(n) | O(n) | Output size dominates |
| 4 | MAD | O(n log n) | O(n log n) | Requires second tree |

## Functions That Benefit from OrderStatisticsZSet

### PERCENTILE_CONT(fraction) ✅ Implemented

**SQL Standard**: SQL:2016

**Semantics**: Returns a value interpolated between adjacent values at the specified percentile position.

**Example**:
```sql
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees;
```

**Why OrderStatisticsZSet**:
- Requires `select(position)` to find bounding values for interpolation
- Position = fraction × (total_weight - 1)
- Without augmentation: O(n) scan to find position
- With OrderStatisticsZSet: O(log n) via `select_percentile_bounds()`

**Incremental benefit**: When employees are added/removed, only O(log n) tree updates needed; percentile query remains O(log n).

---

### PERCENTILE_DISC(fraction) ✅ Implemented

**SQL Standard**: SQL:2016

**Semantics**: Returns the first value whose cumulative distribution exceeds the specified fraction.

**Example**:
```sql
SELECT PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY response_time) AS p75_latency
FROM requests;
```

**Why OrderStatisticsZSet**:
- Requires `select(ceil(fraction × total_weight) - 1)` for discrete selection
- Same positional access pattern as PERCENTILE_CONT
- O(log n) via `select_percentile_disc()`

---

### MEDIAN ✅ Implemented (via PERCENTILE_CONT)

**SQL Standard**: Not in SQL standard; common extension

**Semantics**: Equivalent to `PERCENTILE_CONT(0.5)`.

**Example**:
```sql
SELECT MEDIAN(price) FROM products;
```

**Why OrderStatisticsZSet**: Same as PERCENTILE_CONT with fraction=0.5.

---

### NTILE(n) ❌ Not Yet Implemented

**SQL Standard**: SQL:2003

**Semantics**: Divides ordered rows into n approximately equal-sized buckets, returning bucket number (1 to n).

**Example**:
```sql
SELECT customer_id, revenue,
       NTILE(4) OVER (ORDER BY revenue) AS quartile
FROM customers;
```

**Why OrderStatisticsZSet**:
- Bucket boundaries at positions: `total_weight × i / n` for i in 1..n
- For each row, need to determine which bucket it falls into
- Requires: `rank(value)` to find row's position, then `position × n / total_weight` for bucket

#### Complexity Analysis

| Scenario | Naive | Optimized |
|----------|-------|-----------|
| Single row query | O(log n) | O(log n) |
| Batch backfill (all rows) | O(n log n) | **O(n)** |
| Incremental (B changes) | O(B log n)* | O(B log n)* |

*See incremental caveat below.

#### Single-Row Query: O(log n)

For querying the bucket of a single value:

```rust
fn ntile(&self, value: &T, num_buckets: usize) -> usize {
    let position = self.rank(value);
    let total = self.total_weight();
    ((position * num_buckets as i64) / total).min(num_buckets as i64 - 1) as usize + 1
}
```

#### Batch Backfill: O(n)

For computing NTILE for ALL rows, avoid O(n log n) by leveraging sorted iteration:

```rust
/// Compute NTILE(num_buckets) for all entries in O(n) time.
/// Returns: Vec<(value, bucket_number)> where bucket_number is 1-based.
fn ntile_all(&self, num_buckets: usize) -> Vec<(&T, usize)> {
    let total = self.total_weight();
    if total <= 0 || num_buckets == 0 {
        return vec![];
    }

    let mut result = Vec::with_capacity(self.num_keys());
    let mut cumulative_pos: i64 = 0;

    // Single O(n) iteration through sorted entries
    for (value, weight) in self.iter() {
        if weight <= 0 { continue; }

        // Bucket assignment based on cumulative position
        let bucket = ((cumulative_pos * num_buckets as i64) / total) as usize + 1;
        result.push((value, bucket.min(num_buckets)));

        cumulative_pos += weight;
    }
    result
}
```

**Key insight**: The tree stores entries in sorted order. By iterating once and tracking cumulative position, we assign buckets without any rank queries.

#### Incremental Caveat

NTILE has complex incremental semantics: adding one row shifts bucket boundaries, potentially changing assignments for *many* existing rows. Consider:

- 100 rows divided into 4 buckets of 25 each
- Add 1 row → now 101 rows, buckets become ~25.25 each
- Rows near bucket boundaries may shift to adjacent buckets

True incremental NTILE requires tracking which rows cross bucket boundaries when totals change. This may require:
1. Recomputing all bucket assignments: O(n) - loses incremental benefit
2. Tracking boundary-adjacent rows: Complex bookkeeping
3. Accepting approximate buckets: Trade correctness for performance

For many use cases, recomputing via `ntile_all()` in O(n) after each batch is acceptable, especially when batch frequency is low relative to query frequency.

---

### PERCENT_RANK() ❌ Not Yet Implemented

**SQL Standard**: SQL:2003

**Semantics**: Returns `(rank - 1) / (count - 1)`, where rank is the row's position among peers.

**Example**:
```sql
SELECT student_id, score,
       PERCENT_RANK() OVER (ORDER BY score) AS percentile
FROM exam_results;
```

**Why OrderStatisticsZSet**:
- Requires `rank(value)` to find position of current value
- Requires `total_weight()` for count (O(1) cached)
- Formula: `(rank(value)) / (total_weight - 1)`
- O(log n) via existing `rank()` operation

**Implementation approach**:
```rust
fn percent_rank(&self, value: &T) -> f64 {
    let total = self.total_weight();
    if total <= 1 { return 0.0; }
    let rank = self.rank(value);
    rank as f64 / (total - 1) as f64
}
```

---

### CUME_DIST() ❌ Not Yet Implemented

**SQL Standard**: SQL:2003

**Semantics**: Returns the cumulative distribution: fraction of rows with values ≤ current value.

**Example**:
```sql
SELECT product_id, sales,
       CUME_DIST() OVER (ORDER BY sales) AS cumulative_pct
FROM products;
```

**Why OrderStatisticsZSet**:
- Requires `rank(value) + weight(value)` for count of rows ≤ value
- Requires `total_weight()` for total count
- Formula: `(rank(value) + weight(value)) / total_weight`
- O(log n) via `rank()` + `get_weight()`

**Implementation approach**:
```rust
fn cume_dist(&self, value: &T) -> f64 {
    let total = self.total_weight();
    if total == 0 { return 0.0; }
    let rank = self.rank(value);
    let weight = self.get_weight(value);
    (rank + weight) as f64 / total as f64
}
```

---

### NTH_VALUE(expr, n) ❌ Not Yet Implemented

**SQL Standard**: SQL:2011

**Semantics**: Returns the value at the nth row of the window frame.

**Example**:
```sql
SELECT date, price,
       NTH_VALUE(price, 3) OVER (ORDER BY date) AS third_price
FROM stock_prices;
```

**Why OrderStatisticsZSet**:
- Requires `select(n - 1)` for 1-based indexing
- Direct positional access without scanning
- O(log n) via existing `select_kth()` operation

**Note**: NTH_VALUE is typically used as a window function with frame bounds. OrderStatisticsZSet benefits the case where the frame is the entire partition.

---

### KTH_SMALLEST / KTH_LARGEST ❌ Not Yet Implemented

**SQL Standard**: Non-standard; vendor extensions (Oracle `KEEP FIRST`, PostgreSQL custom aggregates)

**Semantics**: Returns the k-th smallest (or largest) value in a group.

**Example**:
```sql
-- Hypothetical syntax
SELECT department, KTH_SMALLEST(salary, 3) AS third_lowest_salary
FROM employees
GROUP BY department;
```

**Why OrderStatisticsZSet**:
- Direct mapping to existing `select_kth(k - 1)` for 1-based indexing
- For largest: `select_kth_desc(k - 1)` or `select_kth(total_weight - k)`
- **O(log n)** - this is the primary operation the data structure is designed for

**Implementation approach**:
```rust
fn kth_smallest(&self, k: usize) -> Option<&T> {
    if k == 0 { return None; }
    self.select_kth((k - 1) as i64)  // Convert 1-based to 0-based
}

fn kth_largest(&self, k: usize) -> Option<&T> {
    if k == 0 { return None; }
    self.select_kth_desc((k - 1) as i64)
}
```

**Note**: This is essentially what PERCENTILE_DISC does with `k = ceil(p * n)`. KTH_SMALLEST/LARGEST is the integer-indexed variant.

---

### CDF / Rank-of-Value ❌ Not Yet Implemented

**SQL Standard**: Non-standard; common in statistical SQL extensions

**Semantics**: Given a value, return its position as a fraction of total count (the empirical CDF).

**Example**:
```sql
-- Hypothetical syntax: what percentile is salary=75000?
SELECT CDF(salary, 75000) AS salary_percentile
FROM employees;
-- Returns 0.82 meaning 82% of salaries are <= 75000
```

**Why OrderStatisticsZSet**:
- Direct mapping to existing `rank(value)` operation
- Formula: `(rank(value) + weight(value)) / total_weight`
- **O(log n)** - the dual operation to select-by-position
- Note: This is exactly CUME_DIST computed for an arbitrary probe value, not just values in the dataset

**Implementation approach**:
```rust
fn cdf(&self, value: &T) -> f64 {
    let total = self.total_weight();
    if total == 0 { return 0.0; }
    // rank gives count of values strictly less than `value`
    // For CDF we want P(X <= value), so add weight if value exists
    let rank = self.rank(value);
    let weight = self.get_weight(value).max(0);
    (rank + weight) as f64 / total as f64
}
```

**Inverse relationship**: CDF is the inverse of PERCENTILE_DISC:
- `PERCENTILE_DISC(p)` → value at position p (select)
- `CDF(value)` → position p of value (rank)

---

### TRIMMED_MEAN / TRUNCATED_MEAN ⚠️ Partial Benefit

**SQL Standard**: Non-standard; common in statistical packages

**Semantics**: Compute mean after excluding bottom p% and top p% of values.

**Example**:
```sql
-- Hypothetical syntax: mean excluding bottom/top 10%
SELECT TRIMMED_MEAN(salary, 0.1) AS robust_mean
FROM employees;
```

**Analysis**:

OrderStatisticsZSet provides **partial benefit**:

| Step | Operation | Complexity | Notes |
|------|-----------|------------|-------|
| 1. Find trim boundaries | `select(p*n)`, `select((1-p)*n)` | O(log n) | ✅ Efficient |
| 2. Count trimmed elements | Boundary positions | O(1) | ✅ From step 1 |
| 3. Sum values in range | **Not supported** | O(k) | ❌ Must iterate |
| 4. Compute mean | Division | O(1) | ✅ Trivial |

**The limitation**: OrderStatisticsZSet tracks **weight sums** (counts) per subtree, not **value sums**. Computing the sum of values between two positions requires iterating through those values.

**Possible enhancement**: Add `subtree_value_sum` augmentation alongside `subtree_weight_sum`:

```rust
struct InternalNodeTyped<T: Num> {
    keys: Vec<T>,
    children: Vec<usize>,
    subtree_weight_sums: Vec<ZWeight>,  // Existing: count of elements
    subtree_value_sums: Vec<T>,         // New: sum of values (requires T: Num)
}
```

This would enable O(log n) range-sum queries, making TRIMMED_MEAN fully O(log n). However:
- Requires `T: Num` bound (summable values)
- Increases memory and maintenance overhead
- Only benefits numeric aggregates, not general order statistics

**Current recommendation**: Implement as O(k) where k = number of non-trimmed elements. For small trim percentages (e.g., 5%), k ≈ 0.9n, so this degrades to O(n). For large trim percentages (e.g., 45%), k ≈ 0.1n, which is acceptable.

**Implementation approach** (without value-sum augmentation):
```rust
fn trimmed_mean(&self, trim_fraction: f64) -> Option<f64>
where T: Into<f64> + Clone
{
    let total = self.total_weight();
    if total == 0 { return None; }

    let trim_count = (total as f64 * trim_fraction) as i64;
    let lower_bound = trim_count;
    let upper_bound = total - trim_count;

    if lower_bound >= upper_bound { return None; }

    // O(n) iteration through middle portion
    let mut sum = 0.0;
    let mut count = 0i64;
    let mut pos = 0i64;

    for (value, weight) in self.iter() {
        if weight <= 0 { continue; }
        let end_pos = pos + weight;

        // Calculate overlap with [lower_bound, upper_bound)
        let contrib_start = pos.max(lower_bound);
        let contrib_end = end_pos.min(upper_bound);

        if contrib_start < contrib_end {
            let contrib_weight = contrib_end - contrib_start;
            sum += value.clone().into() * contrib_weight as f64;
            count += contrib_weight;
        }

        pos = end_pos;
        if pos >= upper_bound { break; }  // Early exit
    }

    if count == 0 { None } else { Some(sum / count as f64) }
}
```

---

### WINSORIZED_MEAN ⚠️ Partial Benefit (Same as TRIMMED_MEAN)

**SQL Standard**: Non-standard

**Semantics**: Replace values below p-percentile with the p-percentile value, and values above (1-p)-percentile with that value, then compute mean.

**Why similar to TRIMMED_MEAN**:
- Finding boundary values: O(log n) ✅
- Computing adjusted sum: O(n) without value-sum augmentation ❌

Same enhancement (subtree value sums) would make this O(log n).

---

## Functions That Do NOT Benefit

### ROW_NUMBER, RANK, DENSE_RANK

**Current implementation**: `topk.rs` with specialized TopK operator

**Why not OrderStatisticsZSet**:
- These are typically used with `WHERE rank <= k` (top-k pattern)
- TopK operator only materializes k elements, not full order statistics
- OrderStatisticsZSet would be overkill - maintains full tree when only top-k needed
- TopK is O(n log k) for n insertions, keeping only k elements

**When OrderStatisticsZSet would help**: If you need the rank of *every* row (not just top-k), OrderStatisticsZSet provides O(log n) per row. The current TopK approach doesn't support this efficiently.

---

### MODE

**Semantics**: Returns the most frequently occurring value.

**Why not OrderStatisticsZSet**:
- MODE is frequency-based, not position-based
- Requires tracking "which value has highest weight" - a max-heap problem
- OrderStatisticsZSet tracks cumulative positions, not maximum weights
- Better structure: heap or frequency-indexed tree

---

### MAD (Median Absolute Deviation)

**SQL Standard**: Non-standard; common in statistical packages

**Semantics**: A robust measure of statistical dispersion: `MAD = median(|x_i - median(x)|)`

**Example**:
```sql
-- Hypothetical syntax
SELECT MAD(salary) AS salary_dispersion
FROM employees;
```

**Analysis**:

MAD requires two median computations on **different datasets**:

| Step | Operation | Data Structure | Complexity |
|------|-----------|----------------|------------|
| 1. Compute median of X | Original tree | O(log n) | ✅ |
| 2. Compute |x_i - median| for all x_i | **New values** | O(n) | ❌ |
| 3. Build tree of deviations | New tree | O(n log n) | ❌ |
| 4. Compute median of deviations | Deviation tree | O(log n) | ✅ |

**The fundamental problem**: The absolute deviations `|x_i - median|` form a **completely different ordered set** than the original values. The original tree's structure provides no information about the ordering of deviations.

**Example**:
```
Original values: [1, 2, 3, 10, 100]  →  median = 3
Deviations:      [2, 1, 0, 7, 97]    →  completely different order
```

**Conclusion**: OrderStatisticsZSet cannot help with MAD. The algorithm inherently requires:
1. O(log n) to find median
2. O(n) to compute all deviations
3. O(n log n) to sort/structure deviations (or O(n) with linear-time median)
4. O(log n) or O(n) to find median of deviations

**Total**: O(n log n) or O(n) with specialized algorithms, regardless of data structure.

**Alternative**: For approximate MAD, consider:
- T-digest or Q-digest sketches for approximate percentiles
- Streaming algorithms that maintain running deviation estimates

---

### FIRST_VALUE / LAST_VALUE

**Semantics**: First/last value in ordered window frame.

**Why not OrderStatisticsZSet**:
- Just need min/max, not arbitrary position
- Any sorted structure (BTreeMap, sorted vec) provides O(log n) or O(1) access
- OrderStatisticsZSet's augmentation provides no benefit for min/max

---

### LEAD / LAG

**Semantics**: Access value at fixed row offset from current row.

**Why not OrderStatisticsZSet**:
- Offset-based access relative to current row position
- Requires row iteration context, not absolute position queries
- Typically implemented as streaming operators with bounded buffers

---

### COUNT / SUM / AVG / MIN / MAX

**Semantics**: Standard aggregate functions.

**Why not OrderStatisticsZSet**:
- No positional queries needed
- Simple accumulators suffice
- O(1) incremental updates with basic semigroup operations

---

## Complexity Comparison

| Function | Without OSM | Single Query | Batch | Category |
|----------|-------------|--------------|-------|----------|
| PERCENTILE_CONT | O(n) scan | O(log n) | O(log n) | ✅ Full benefit |
| PERCENTILE_DISC | O(n) scan | O(log n) | O(log n) | ✅ Full benefit |
| KTH_SMALLEST/LARGEST | O(n) scan | O(log n) | O(log n) | ✅ Full benefit |
| NTH_VALUE | O(n) scan | O(log n) | O(log n) | ✅ Full benefit |
| CDF (rank-of-value) | O(n) scan | O(log n) | O(log n) | ✅ Full benefit |
| NTILE | O(n) | O(log n) | O(n) | ⚠️ Per-row output |
| PERCENT_RANK | O(n) scan | O(log n) | O(n) | ⚠️ Per-row output |
| CUME_DIST | O(n) scan | O(log n) | O(n) | ⚠️ Per-row output |
| TRIMMED_MEAN | O(n) | O(n)* | O(n) | ⚠️ Needs value-sum augmentation |
| WINSORIZED_MEAN | O(n) | O(n)* | O(n) | ⚠️ Needs value-sum augmentation |
| MAD | O(n log n) | O(n log n) | O(n log n) | ❌ No benefit |

*O(log n) for boundaries, but O(n) for range sum without additional augmentation.

**Benefit categories**:

1. **Full benefit** (✅): O(log n) query complexity, single value output
   - PERCENTILE_CONT/DISC, KTH_SMALLEST/LARGEST, NTH_VALUE, CDF
   - These are the "sweet spot" for OrderStatisticsZSet

2. **Per-row output** (⚠️): O(log n) per query, but O(n) to compute all rows
   - NTILE, PERCENT_RANK, CUME_DIST
   - Benefit depends on query pattern (single value vs all rows)

3. **Partial benefit** (⚠️): Boundaries O(log n), but sum/mean requires iteration
   - TRIMMED_MEAN, WINSORIZED_MEAN
   - Could be improved with value-sum augmentation

4. **No benefit** (❌): Requires fundamentally different computation
   - MAD (needs second tree for deviations)

**Incremental computation** (batch size B, state size n):
- **Full benefit**: O(B log n) updates + O(log n) query = **O(B log n) total**
- **Per-row**: O(B log n) updates + O(n) for all rows = **O(n) dominated**
- **Partial**: O(B log n) updates + O(n) range sum = **O(n) dominated**

## Implementation Priority

Based on SQL usage patterns, incremental benefit, and implementation complexity:

### Tier 1: Full O(log n) Benefit (High Priority)

1. **KTH_SMALLEST / KTH_LARGEST**
   - Trivial: direct mapping to existing `select_kth()` / `select_kth_desc()`
   - Zero additional code required beyond API exposure
   - Common use case: "third highest salary", "10th percentile value"

2. **CDF / Rank-of-Value**
   - Trivial: direct mapping to existing `rank()` + `get_weight()`
   - Inverse of PERCENTILE_DISC - completes the bidirectional API
   - Use case: "what percentile is this value?"

3. **NTH_VALUE**
   - Direct use of `select_kth(n - 1)`
   - Straightforward window function semantics
   - Common in time-series analysis

### Tier 2: Per-Row Window Functions (Medium Priority)

4. **PERCENT_RANK**
   - O(log n) per query, useful for scoring individual values
   - One-liner using `rank()` and `total_weight()`
   - Note: computing for all rows is O(n)

5. **CUME_DIST**
   - Statistical distribution analysis
   - Requires `rank()` + `get_weight()` combination
   - Same O(n) caveat for full-table computation

6. **NTILE**
   - Common in reporting (quartiles, deciles)
   - O(n) batch algorithm via sorted iteration
   - Complex incremental semantics (boundary shifts)

### Tier 3: Requires Additional Augmentation (Lower Priority)

7. **TRIMMED_MEAN / WINSORIZED_MEAN**
   - Would require `subtree_value_sum` augmentation for O(log n)
   - Currently implementable as O(n) with boundary optimization
   - Consider if there's sufficient demand for robust statistics

### Not Recommended

8. **MAD (Median Absolute Deviation)**
   - Fundamental algorithmic limitation (requires second tree)
   - Better addressed via approximate/streaming algorithms
   - Consider t-digest or similar sketches for approximate MAD

## References

- SQL:2016 Standard - Part 2: Foundation, Section 10.9 (Aggregate functions)
- SQL:2003 Standard - Window function specifications
- PostgreSQL documentation on window functions
- `order_statistics_zset.md` - Implementation details
- `order_statistics_zset.rs` - Source implementation
