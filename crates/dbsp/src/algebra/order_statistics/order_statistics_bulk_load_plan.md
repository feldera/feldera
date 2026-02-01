# OrderStatisticsMultiset Bulk Load Optimization - Implementation Plan

## Overview

This plan implements O(n) bulk construction of `OrderStatisticsMultiset` from sorted entries,
replacing the current O(n log n) approach that uses repeated insertions. This optimization:

1. Improves checkpoint/restore performance from O(n log n) to O(n)
2. Establishes the foundation for node-level file storage (order_statistics_node_storage_plan.md)
3. Enables efficient tree reconstruction from disk-backed node storage

## Current State

### Serialization (O(n)) - Already Optimal
```rust
// order_statistics_multiset.rs:967-973
impl From<&OrderStatisticsMultiset<T>> for SerializableOrderStatisticsMultiset<T> {
    fn from(tree: &OrderStatisticsMultiset<T>) -> Self {
        Self {
            entries: tree.iter().map(|(k, w)| (k.clone(), w)).collect(),  // O(n)
            branching_factor: tree.max_leaf_entries as u32,
        }
    }
}
```

### Deserialization (O(n log n)) - Needs Optimization
```rust
// order_statistics_multiset.rs:976-983
impl From<SerializableOrderStatisticsMultiset<T>> for OrderStatisticsMultiset<T> {
    fn from(serialized: SerializableOrderStatisticsMultiset<T>) -> Self {
        let mut tree = Self::with_branching_factor(serialized.branching_factor as usize);
        for (key, weight) in serialized.entries {
            tree.insert(key, weight);  // O(log n) per entry!
        }
        tree
    }
}
```

## Algorithm: Bottom-Up Bulk Construction

The key insight is that given sorted entries, we can build a perfectly balanced B+ tree
in a single bottom-up pass:

### Phase 1: Build Leaf Layer

```
Input: sorted [(k1,w1), (k2,w2), ..., (kn,wn)]
Branching factor: B (e.g., 64)

Step 1: Partition into leaf nodes
  - Each leaf holds up to B entries
  - Last leaf may have fewer entries (but at least 1)

  Leaf 0: [(k1,w1), ..., (kB,wB)]
  Leaf 1: [(kB+1,wB+1), ..., (k2B,w2B)]
  ...

Step 2: Link leaves together
  - Set next_leaf pointers to form linked list
  - first_leaf = 0
```

### Phase 2: Build Internal Layers (Bottom-Up)

```
For each level from leaves upward:
  - Group B children into one parent
  - Parent's keys = first key of each child (except first child)
  - Parent's subtree_sums = sum of weights in each child

  Continue until only one node remains (the root)
```

### Visual Example (B=3)

```
Input: [(10,1), (20,2), (30,3), (40,4), (50,5), (60,6), (70,7)]

Phase 1: Build leaves (3 entries each)
  Leaf 0: [(10,1), (20,2), (30,3)]  sum=6
  Leaf 1: [(40,4), (50,5), (60,6)]  sum=15
  Leaf 2: [(70,7)]                  sum=7

Phase 2: Build internal layer
  Internal 0:
    keys: [40, 70]           (first key of leaf 1, leaf 2)
    children: [0, 1, 2]      (indices of leaves)
    subtree_sums: [6, 15, 7] (sum of each leaf)

Result: root = Internal 0, first_leaf = 0
```

## Implementation Details

### New Method: `from_sorted_entries`

Add to `impl<T: Ord + Clone> OrderStatisticsMultiset<T>`:

```rust
/// Construct a multiset from pre-sorted entries in O(n) time.
///
/// # Preconditions
/// - `entries` MUST be sorted by key in ascending order
/// - Duplicate keys should have their weights combined before calling
///
/// # Complexity
/// O(n) where n = entries.len()
///
/// # Panics
/// Debug builds panic if entries are not sorted.
pub fn from_sorted_entries(
    entries: Vec<(T, ZWeight)>,
    branching_factor: usize,
) -> Self {
    // ... implementation below
}
```

### Step-by-Step Implementation

```rust
pub fn from_sorted_entries(
    entries: Vec<(T, ZWeight)>,
    branching_factor: usize,
) -> Self {
    let b = branching_factor.max(MIN_BRANCHING_FACTOR);

    // Handle empty case
    if entries.is_empty() {
        return Self::with_branching_factor(b);
    }

    // Debug assertion for sorted input
    debug_assert!(
        entries.windows(2).all(|w| w[0].0 <= w[1].0),
        "entries must be sorted"
    );

    let num_keys = entries.len();
    let total_weight: ZWeight = entries.iter().map(|(_, w)| *w).sum();

    // Phase 1: Build leaf nodes
    let mut nodes: Vec<Node<T>> = Vec::new();
    let mut leaf_indices: Vec<usize> = Vec::new();
    let mut leaf_weights: Vec<ZWeight> = Vec::new();

    for chunk in entries.chunks(b) {
        let leaf_idx = nodes.len();
        leaf_indices.push(leaf_idx);

        let leaf = LeafNode {
            entries: chunk.to_vec(),
            next_leaf: usize::MAX,  // Will be set below
        };
        leaf_weights.push(leaf.total_weight());
        nodes.push(Node::Leaf(leaf));
    }

    // Link leaves together
    for i in 0..leaf_indices.len() - 1 {
        let current_idx = leaf_indices[i];
        let next_idx = leaf_indices[i + 1];
        if let Node::Leaf(leaf) = &mut nodes[current_idx] {
            leaf.next_leaf = next_idx;
        }
    }

    let first_leaf = leaf_indices[0];

    // Phase 2: Build internal layers bottom-up
    let mut current_level_indices = leaf_indices;
    let mut current_level_weights = leaf_weights;

    while current_level_indices.len() > 1 {
        let mut next_level_indices = Vec::new();
        let mut next_level_weights = Vec::new();

        // Process children in groups of B
        let mut i = 0;
        while i < current_level_indices.len() {
            let chunk_end = (i + b).min(current_level_indices.len());
            let chunk_indices = &current_level_indices[i..chunk_end];
            let chunk_weights = &current_level_weights[i..chunk_end];

            // Create internal node
            let internal_idx = nodes.len();
            next_level_indices.push(internal_idx);

            // Build keys: first key of each child except the first
            let mut keys = Vec::with_capacity(chunk_indices.len() - 1);
            for &child_idx in &chunk_indices[1..] {
                let first_key = Self::get_first_key(&nodes, child_idx);
                keys.push(first_key);
            }

            let internal = InternalNodeTyped {
                keys,
                children: chunk_indices.to_vec(),
                subtree_sums: chunk_weights.to_vec(),
            };

            let internal_weight = internal.total_weight();
            next_level_weights.push(internal_weight);
            nodes.push(Node::Internal(internal));

            i = chunk_end;
        }

        current_level_indices = next_level_indices;
        current_level_weights = next_level_weights;
    }

    let root = current_level_indices[0];

    Self {
        nodes,
        root,
        total_weight,
        max_leaf_entries: b,
        max_internal_children: b,
        first_leaf,
        num_keys,
    }
}

/// Helper to get the first key from a node (for building separator keys)
fn get_first_key(nodes: &[Node<T>], node_idx: usize) -> T {
    match &nodes[node_idx] {
        Node::Leaf(leaf) => leaf.entries[0].0.clone(),
        Node::Internal(internal) => Self::get_first_key(nodes, internal.children[0]),
    }
}
```

### Update Deserialization

```rust
impl<T: Ord + Clone + Archive> From<SerializableOrderStatisticsMultiset<T>>
    for OrderStatisticsMultiset<T>
{
    fn from(serialized: SerializableOrderStatisticsMultiset<T>) -> Self {
        // Use O(n) bulk construction instead of O(n log n) repeated inserts
        Self::from_sorted_entries(
            serialized.entries,
            serialized.branching_factor as usize,
        )
    }
}
```

### Update `compact()` Method

The existing `compact()` method also rebuilds via repeated inserts:

```rust
// Current O(n log n) implementation
pub fn compact(&mut self) {
    let entries: Vec<_> = self
        .iter()
        .filter(|(_, w)| *w != 0)
        .map(|(k, w)| (k.clone(), w))
        .collect();

    let b = self.max_leaf_entries;
    *self = Self::with_branching_factor(b);
    for (key, weight) in entries {
        self.insert(key, weight);  // O(log n) each!
    }
}

// New O(n) implementation
pub fn compact(&mut self) {
    let entries: Vec<_> = self
        .iter()
        .filter(|(_, w)| *w != 0)
        .map(|(k, w)| (k.clone(), w))
        .collect();

    let b = self.max_leaf_entries;
    *self = Self::from_sorted_entries(entries, b);
}
```

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_bulk_load_empty() {
    let tree: OrderStatisticsMultiset<i32> =
        OrderStatisticsMultiset::from_sorted_entries(vec![], 64);
    assert!(tree.is_empty());
    assert_eq!(tree.num_keys(), 0);
}

#[test]
fn test_bulk_load_single() {
    let tree = OrderStatisticsMultiset::from_sorted_entries(
        vec![(42, 5)],
        64
    );
    assert_eq!(tree.total_weight(), 5);
    assert_eq!(tree.select_kth(0, true), Some(&42));
}

#[test]
fn test_bulk_load_matches_incremental() {
    // Build tree incrementally
    let mut incremental: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
    for i in 0..1000 {
        incremental.insert(i, (i % 10) as ZWeight + 1);
    }

    // Build tree via bulk load
    let entries: Vec<_> = incremental.iter()
        .map(|(k, w)| (*k, w))
        .collect();
    let bulk = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

    // Verify equality
    assert_eq!(incremental.total_weight(), bulk.total_weight());
    assert_eq!(incremental.num_keys(), bulk.num_keys());

    // Verify select operations match
    for i in 0..incremental.total_weight() {
        assert_eq!(
            incremental.select_kth(i, true),
            bulk.select_kth(i, true),
            "Mismatch at position {i}"
        );
    }
}

#[test]
fn test_bulk_load_various_sizes() {
    for size in [1, 10, 63, 64, 65, 100, 1000, 10000] {
        let entries: Vec<_> = (0..size)
            .map(|i| (i as i32, 1))
            .collect();

        let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

        assert_eq!(tree.num_keys(), size);
        assert_eq!(tree.total_weight(), size as ZWeight);

        // Verify first and last
        assert_eq!(tree.select_kth(0, true), Some(&0));
        assert_eq!(tree.select_kth(size as ZWeight - 1, true), Some(&(size as i32 - 1)));
    }
}

#[test]
fn test_bulk_load_preserves_iteration_order() {
    let entries: Vec<_> = (0..500)
        .map(|i| (i * 2, i as ZWeight + 1))  // Even numbers with varying weights
        .collect();

    let tree = OrderStatisticsMultiset::from_sorted_entries(entries.clone(), 32);

    let collected: Vec<_> = tree.iter().collect();
    assert_eq!(collected.len(), entries.len());

    for ((k1, w1), (k2, w2)) in collected.iter().zip(entries.iter()) {
        assert_eq!(*k1, *k2);
        assert_eq!(*w1, *w2);
    }
}

#[test]
fn test_serialization_roundtrip_uses_bulk_load() {
    use rkyv::{archived_root, to_bytes, Deserialize};

    let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
    for i in 0..1000 {
        tree.insert(i, 1);
    }

    // Serialize
    let bytes = to_bytes::<_, 256>(&tree).unwrap();

    // Deserialize
    let archived = unsafe { archived_root::<OrderStatisticsMultiset<i32>>(&bytes) };
    let restored: OrderStatisticsMultiset<i32> = archived
        .deserialize(&mut rkyv::Infallible)
        .unwrap();

    // Verify
    assert_eq!(tree.total_weight(), restored.total_weight());
    assert_eq!(tree.num_keys(), restored.num_keys());

    for i in 0..tree.total_weight() {
        assert_eq!(tree.select_kth(i, true), restored.select_kth(i, true));
    }
}
```

### Property-Based Tests (proptest)

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_bulk_load_invariants(
        entries in prop::collection::vec((0i32..10000, 1i64..100), 0..1000)
    ) {
        // Sort and deduplicate entries
        let mut sorted_entries: Vec<(i32, i64)> = entries;
        sorted_entries.sort_by_key(|(k, _)| *k);
        sorted_entries.dedup_by_key(|(k, _)| *k);

        let tree = OrderStatisticsMultiset::from_sorted_entries(sorted_entries.clone(), 64);

        // Invariant 1: num_keys matches
        prop_assert_eq!(tree.num_keys(), sorted_entries.len());

        // Invariant 2: total_weight matches
        let expected_weight: i64 = sorted_entries.iter().map(|(_, w)| *w).sum();
        prop_assert_eq!(tree.total_weight(), expected_weight);

        // Invariant 3: iteration yields entries in order
        let collected: Vec<_> = tree.iter().map(|(k, w)| (*k, w)).collect();
        prop_assert_eq!(collected, sorted_entries);

        // Invariant 4: select_kth works correctly
        if !sorted_entries.is_empty() {
            prop_assert_eq!(tree.select_kth(0, true), Some(&sorted_entries[0].0));
        }
    }
}
```

## Performance Validation

### Benchmark

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("OrderStatisticsMultiset construction");

    for size in [100, 1000, 10000, 100000] {
        let entries: Vec<_> = (0..size).map(|i| (i, 1i64)).collect();

        group.bench_with_input(
            BenchmarkId::new("bulk_load", size),
            &entries,
            |b, entries| {
                b.iter(|| {
                    OrderStatisticsMultiset::from_sorted_entries(
                        entries.clone(),
                        64,
                    )
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("repeated_insert", size),
            &entries,
            |b, entries| {
                b.iter(|| {
                    let mut tree = OrderStatisticsMultiset::new();
                    for (k, w) in entries {
                        tree.insert(*k, *w);
                    }
                    tree
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_construction);
criterion_main!(benches);
```

Expected results:
| Size    | Repeated Insert | Bulk Load | Speedup |
|---------|-----------------|-----------|---------|
| 1,000   | ~150µs          | ~15µs     | ~10x    |
| 10,000  | ~2ms            | ~150µs    | ~13x    |
| 100,000 | ~30ms           | ~1.5ms    | ~20x    |

## Edge Cases to Handle

1. **Empty input**: Return empty tree with correct branching factor
2. **Single entry**: Single leaf node, no internal nodes
3. **Exactly B entries**: Single full leaf, no internal nodes
4. **B+1 entries**: Two leaves, one internal node
5. **Entries at level boundaries**: Test sizes like B, B², B³
6. **Negative weights**: Include in total_weight but skip in select
7. **All zero weights**: Tree has entries but is_empty() == true

## Files to Modify

1. `crates/dbsp/src/algebra/order_statistics_multiset.rs`
   - Add `from_sorted_entries()` method
   - Add helper `get_first_key()` method
   - Update `compact()` to use bulk load
   - Update `From<SerializableOrderStatisticsMultiset>` impl
   - Add unit tests

2. `crates/dbsp/src/algebra/order_statistics_multiset.md`
   - Document the bulk load algorithm
   - Add complexity analysis

## Implementation Checklist

- [ ] Implement `from_sorted_entries()` method
- [ ] Implement `get_first_key()` helper
- [ ] Update `From<SerializableOrderStatisticsMultiset>` to use bulk load
- [ ] Update `compact()` to use bulk load
- [ ] Add unit tests for bulk load
- [ ] Add property-based tests
- [ ] Run existing tests to ensure no regressions
- [ ] Add benchmark comparing bulk load vs repeated insert
- [ ] Update documentation

## Future: How This Helps Node-Level Storage

When we implement node-level file backing, bulk load will be used:

1. **Loading cold subtrees**: When a subtree is evicted and later needed,
   its entries can be loaded from disk and reconstructed via bulk load in O(n)

2. **Checkpoint restore**: The current serialization format (sorted entries)
   remains optimal; bulk load makes restoration O(n) instead of O(n log n)

3. **Segment reconstruction**: If we implement segmented trees, each segment
   can be bulk-loaded independently

4. **File format**: The bulk load algorithm naturally produces the optimal
   tree structure for a given dataset, which can be directly written to disk
   as the file format
