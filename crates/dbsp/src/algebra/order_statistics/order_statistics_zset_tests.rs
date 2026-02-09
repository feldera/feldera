#[cfg(test)]
mod tests {
    use crate::algebra::{DEFAULT_BRANCHING_FACTOR, OrderStatisticsZSet, ZWeight};
    use crate::node_storage::NodeStorageConfig;

    fn new_tree<T>() -> OrderStatisticsZSet<T>
    where
        T: crate::DBData,
    {
        OrderStatisticsZSet::with_config(
            DEFAULT_BRANCHING_FACTOR,
            NodeStorageConfig::memory_only(),
        )
    }

    #[test]
    fn test_empty_tree() {
        let mut tree: OrderStatisticsZSet<i32> = new_tree();
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.num_keys(), 0);
        assert!(tree.is_empty());
        assert_eq!(tree.select_kth(0, true), None);
        assert_eq!(tree.rank(&10), 0);
    }

    #[test]
    fn test_single_insert() {
        let mut tree = new_tree();
        tree.insert(10, 5);

        assert_eq!(tree.total_weight(), 5);
        assert_eq!(tree.num_keys(), 1);
        assert!(!tree.is_empty());

        // All positions 0-4 should return 10
        for i in 0..5 {
            assert_eq!(tree.select_kth(i, true), Some(&10));
        }
        assert_eq!(tree.select_kth(5, true), None);

        assert_eq!(tree.rank(&5), 0); // Nothing less than 5
        assert_eq!(tree.rank(&10), 0); // Nothing less than 10
        assert_eq!(tree.rank(&15), 5); // 5 elements less than 15
    }

    #[test]
    fn test_multiple_inserts() {
        let mut tree = new_tree();
        tree.insert(10, 3); // positions 0, 1, 2
        tree.insert(20, 2); // positions 3, 4
        tree.insert(15, 1); // position between: now 0,1,2=10, 3=15, 4,5=20

        assert_eq!(tree.total_weight(), 6);
        assert_eq!(tree.num_keys(), 3);

        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(1, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&10));
        assert_eq!(tree.select_kth(3, true), Some(&15));
        assert_eq!(tree.select_kth(4, true), Some(&20));
        assert_eq!(tree.select_kth(5, true), Some(&20));
        assert_eq!(tree.select_kth(6, true), None);

        assert_eq!(tree.rank(&10), 0);
        assert_eq!(tree.rank(&15), 3);
        assert_eq!(tree.rank(&20), 4);
        assert_eq!(tree.rank(&25), 6);
    }

    #[test]
    fn test_weight_update() {
        let mut tree = new_tree();
        tree.insert(10, 5);
        tree.insert(10, 3); // Should add to existing weight

        assert_eq!(tree.total_weight(), 8);
        assert_eq!(tree.num_keys(), 1);
        assert_eq!(tree.get_weight(&10), 8);
    }

    #[test]
    fn test_negative_weights() {
        let mut tree = new_tree();
        tree.insert(10, 5);
        tree.insert(10, -2); // Reduce weight to 3

        assert_eq!(tree.total_weight(), 3);
        assert_eq!(tree.get_weight(&10), 3);

        // Only 3 positions now
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&10));
        assert_eq!(tree.select_kth(3, true), None);
    }

    #[test]
    fn test_merge() {
        let mut tree1 = new_tree();
        tree1.insert(10, 3);
        tree1.insert(30, 2);

        let mut tree2 = new_tree();
        tree2.insert(20, 1);
        tree2.insert(30, 1);

        tree1.merge(&tree2);

        assert_eq!(tree1.total_weight(), 7);
        assert_eq!(tree1.get_weight(&10), 3);
        assert_eq!(tree1.get_weight(&20), 1);
        assert_eq!(tree1.get_weight(&30), 3); // 2 + 1
    }

    #[test]
    fn test_percentile_disc() {
        let mut tree = new_tree();
        for i in 1..=100 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&50));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&100));
    }

    #[test]
    fn test_percentile_cont_bounds() {
        let mut tree = new_tree();
        tree.insert(10, 1);
        tree.insert(20, 1);
        tree.insert(30, 1);
        tree.insert(40, 1);

        // 50th percentile should interpolate between 20 and 30
        let (lower, upper, frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(lower, 20);
        assert_eq!(upper, 30);
        assert!((frac - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_descending_select() {
        let mut tree = new_tree();
        tree.insert(10, 2);
        tree.insert(20, 2);
        tree.insert(30, 2);

        // Descending: 0,1->30, 2,3->20, 4,5->10
        assert_eq!(tree.select_kth(0, false), Some(&30));
        assert_eq!(tree.select_kth(1, false), Some(&30));
        assert_eq!(tree.select_kth(2, false), Some(&20));
        assert_eq!(tree.select_kth(4, false), Some(&10));
    }

    // ========================================================================
    // Bulk load tests
    // ========================================================================

    #[test]
    fn test_bulk_load_empty() {
        let mut tree: OrderStatisticsZSet<i32> =
            OrderStatisticsZSet::from_sorted_entries(vec![], 64);
        assert!(tree.is_empty());
        assert_eq!(tree.num_keys(), 0);
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.select_kth(0, true), None);
    }

    #[test]
    fn test_bulk_load_single() {
        let mut tree = OrderStatisticsZSet::from_sorted_entries(vec![(42, 5)], 64);
        assert_eq!(tree.total_weight(), 5);
        assert_eq!(tree.num_keys(), 1);
        assert_eq!(tree.select_kth(0, true), Some(&42));
        assert_eq!(tree.select_kth(4, true), Some(&42));
        assert_eq!(tree.select_kth(5, true), None);
    }

    #[test]
    fn test_bulk_load_matches_incremental() {
        // Build tree incrementally
        let mut incremental: OrderStatisticsZSet<i32> = new_tree();
        for i in 0..1000 {
            incremental.insert(i, (i % 10) as ZWeight + 1);
        }

        // Build tree via bulk load
        let entries: Vec<_> = incremental.iter().map(|(k, w)| (*k, w)).collect();
        let mut bulk = OrderStatisticsZSet::from_sorted_entries(entries, 64);

        // Verify equality
        assert_eq!(incremental.total_weight(), bulk.total_weight());
        assert_eq!(incremental.num_keys(), bulk.num_keys());

        // Verify select operations match for a sample of positions
        for i in (0..incremental.total_weight()).step_by(100) {
            assert_eq!(
                incremental.select_kth(i, true),
                bulk.select_kth(i, true),
                "Mismatch at position {i}"
            );
        }

        // Verify iteration produces same results
        let inc_entries: Vec<_> = incremental.iter().collect();
        let bulk_entries: Vec<_> = bulk.iter().collect();
        assert_eq!(inc_entries, bulk_entries);
    }

    #[test]
    fn test_bulk_load_various_sizes() {
        // Test sizes around branching factor boundaries
        for size in [1, 10, 63, 64, 65, 100, 127, 128, 129, 1000, 10000] {
            let entries: Vec<_> = (0..size).map(|i| (i as i32, 1)).collect();

            let mut tree = OrderStatisticsZSet::from_sorted_entries(entries, 64);

            assert_eq!(tree.num_keys(), size, "num_keys mismatch for size {size}");
            assert_eq!(
                tree.total_weight(),
                size as ZWeight,
                "total_weight mismatch for size {size}"
            );

            // Verify first and last
            assert_eq!(
                tree.select_kth(0, true),
                Some(&0),
                "first key for size {size}"
            );
            assert_eq!(
                tree.select_kth(size as ZWeight - 1, true),
                Some(&(size as i32 - 1)),
                "last key for size {size}"
            );
        }
    }

    #[test]
    fn test_bulk_load_preserves_iteration_order() {
        let entries: Vec<_> = (0..500)
            .map(|i| (i * 2, i as ZWeight + 1)) // Even numbers with varying weights
            .collect();

        let tree = OrderStatisticsZSet::from_sorted_entries(entries.clone(), 32);

        let collected: Vec<_> = tree.iter().map(|(k, w)| (*k, w)).collect();
        assert_eq!(collected.len(), entries.len());
        assert_eq!(collected, entries);
    }

    #[test]
    fn test_bulk_load_with_varying_weights() {
        let entries = vec![(10, 3), (20, 2), (30, 5), (40, 1), (50, 4)];
        let mut tree = OrderStatisticsZSet::from_sorted_entries(entries, 64);

        assert_eq!(tree.total_weight(), 15); // 3+2+5+1+4
        assert_eq!(tree.num_keys(), 5);

        // Test select_kth with varying weights
        // Key 10 has weight 3: positions 0, 1, 2
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&10));

        // Key 20 has weight 2: positions 3, 4
        assert_eq!(tree.select_kth(3, true), Some(&20));
        assert_eq!(tree.select_kth(4, true), Some(&20));

        // Key 30 has weight 5: positions 5-9
        assert_eq!(tree.select_kth(5, true), Some(&30));
        assert_eq!(tree.select_kth(9, true), Some(&30));

        // Key 40 has weight 1: position 10
        assert_eq!(tree.select_kth(10, true), Some(&40));

        // Key 50 has weight 4: positions 11-14
        assert_eq!(tree.select_kth(11, true), Some(&50));
        assert_eq!(tree.select_kth(14, true), Some(&50));
    }

    #[test]
    fn test_bulk_load_small_branching_factor() {
        // Use minimum branching factor to force multiple internal levels
        let entries: Vec<_> = (0..100).map(|i| (i, 1)).collect();
        let mut tree = OrderStatisticsZSet::from_sorted_entries(entries, 4);

        assert_eq!(tree.num_keys(), 100);
        assert_eq!(tree.total_weight(), 100);

        // Verify all elements accessible
        for i in 0..100 {
            assert_eq!(tree.select_kth(i, true), Some(&(i as i32)));
        }
    }

    #[test]
    fn test_bulk_load_rank_queries() {
        let entries: Vec<_> = (0..100).map(|i| (i * 10, 1)).collect();
        let mut tree = OrderStatisticsZSet::from_sorted_entries(entries, 64);

        // rank(key) = sum of weights of keys < key
        assert_eq!(tree.rank(&0), 0); // No keys less than 0
        assert_eq!(tree.rank(&10), 1); // One key (0) less than 10
        assert_eq!(tree.rank(&50), 5); // Keys 0,10,20,30,40 less than 50
        assert_eq!(tree.rank(&990), 99); // 99 keys less than 990
        assert_eq!(tree.rank(&1000), 100); // All 100 keys less than 1000
    }

    #[test]
    fn test_compact_uses_bulk_load() {
        let mut tree = new_tree();
        for i in 0..100 {
            tree.insert(i, 1);
        }
        // Add some zero-weight entries
        tree.insert(50, -1); // Now key 50 has weight 0

        let original_weight = tree.total_weight();
        assert_eq!(original_weight, 99); // 100 - 1

        tree.compact();

        // After compact, zero-weight entry should be removed
        assert_eq!(tree.total_weight(), 99);
        assert_eq!(tree.num_keys(), 99); // Key 50 removed

        // Verify structure is still correct
        assert_eq!(tree.select_kth(0, true), Some(&0));
        assert_eq!(tree.select_kth(49, true), Some(&49));
        assert_eq!(tree.select_kth(50, true), Some(&51)); // 50 is gone
    }

    #[test]
    fn test_bulk_load_negative_weights() {
        // Bulk load with some negative weights (for incremental computation)
        let entries = vec![(10, 5), (20, -2), (30, 3)];
        let mut tree = OrderStatisticsZSet::from_sorted_entries(entries, 64);

        assert_eq!(tree.total_weight(), 6); // 5 + (-2) + 3
        assert_eq!(tree.num_keys(), 3);

        // Negative weights are skipped in select
        // Key 10: positions 0-4
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(4, true), Some(&10));
        // Key 20 has negative weight, skipped
        // Key 30: positions 5-7
        assert_eq!(tree.select_kth(5, true), Some(&30));
    }

    #[test]
    fn test_bulk_load_percentile_operations() {
        let entries: Vec<_> = (1..=100).map(|i| (i, 1)).collect();
        let mut tree = OrderStatisticsZSet::from_sorted_entries(entries, 64);

        // PERCENTILE_DISC
        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&50));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&100));

        // PERCENTILE_CONT bounds
        let (lower, upper, _frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(lower, 50);
        assert_eq!(upper, 51);
    }

    // ========================================================================
    // Flush/evict tests
    // ========================================================================

    fn test_config_with_storage(
        threshold_bytes: usize,
    ) -> (
        NodeStorageConfig,
        std::sync::Arc<crate::storage::backend::memory_impl::MemoryBackend>,
    ) {
        use crate::storage::backend::memory_impl::MemoryBackend;
        let backend = std::sync::Arc::new(MemoryBackend::new());
        let config = NodeStorageConfig {
            enable_spill: true,

            spill_threshold_bytes: threshold_bytes,
            target_segment_size: 64 * 1024 * 1024,
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: Some(backend.clone()),
            buffer_cache: None,
        };
        (config, backend)
    }

    #[test]
    fn test_select_kth_after_flush_evict() {
        let (config, _backend) = test_config_with_storage(0);
        let mut tree = OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config);

        // Insert enough data to have multiple leaves
        for i in 0..200 {
            tree.insert(i, 1);
        }

        // Flush and evict
        let (flushed, evicted, _freed) = tree.flush_and_evict().unwrap();
        assert!(flushed > 0, "Should have flushed some leaves");
        assert!(evicted > 0, "Should have evicted some leaves");
        assert!(
            tree.evicted_leaf_count() > 0,
            "Should have evicted leaves tracked"
        );

        // Verify select_kth still works correctly after eviction
        assert_eq!(tree.select_kth(0, true), Some(&0));
        assert_eq!(tree.select_kth(99, true), Some(&99));
        assert_eq!(tree.select_kth(199, true), Some(&199));
        assert_eq!(tree.select_kth(200, true), None);
    }

    #[test]
    fn test_insert_after_flush_evict() {
        let (config, _backend) = test_config_with_storage(0);
        let mut tree = OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config);

        // Insert data
        for i in 0..100 {
            tree.insert(i * 2, 1); // Even numbers 0, 2, 4, ...198
        }

        // Flush and evict
        tree.flush_and_evict().unwrap();
        assert!(tree.evicted_leaf_count() > 0);

        // Insert into evicted tree
        for i in 0..50 {
            tree.insert(i * 2 + 1, 1); // Odd numbers 1, 3, 5, ...99
        }

        // Verify tree integrity
        assert_eq!(tree.total_weight(), 150);
        assert_eq!(tree.num_keys(), 150);
        assert_eq!(tree.select_kth(0, true), Some(&0));
        assert_eq!(tree.select_kth(1, true), Some(&1));
        assert_eq!(tree.select_kth(2, true), Some(&2));
    }

    #[test]
    fn test_flush_and_evict_wrapper() {
        let (config, _backend) = test_config_with_storage(0);
        let mut tree = OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config);

        // Insert data
        for i in 0..100 {
            tree.insert(i, 1);
        }

        assert!(tree.should_flush(), "Should want to flush with threshold 0");

        let (flushed, evicted, freed) = tree.flush_and_evict().unwrap();
        assert!(flushed > 0, "Should flush dirty leaves");
        assert!(evicted > 0, "Should evict clean leaves");
        assert!(freed > 0, "Should free some bytes");
        assert_eq!(tree.evicted_leaf_count(), evicted);
    }

    /// Test reconcile_parallel_writes with split leaves only (no disk writes).
    /// Builds a tree with a small branching factor, then simulates what a worker
    /// would produce: split leaves that need to be inserted.
    #[test]
    fn test_reconcile_with_splits() {
        use crate::algebra::order_statistics::parallel_routing::{
            LeafWriteResult, SplitLeafInfo,
        };
        use crate::algebra::order_statistics::order_statistics_zset::LeafNode;
        use crate::node_storage::LeafLocation;

        // Use small branching factor for easy splitting
        let bf = 4;
        let mut tree: OrderStatisticsZSet<i32> = OrderStatisticsZSet::with_config(
            bf,
            NodeStorageConfig::memory_only(),
        );

        // Insert initial data to build a multi-level tree
        for i in 0..20 {
            tree.insert(i * 10, 1);
        }

        let initial_weight = tree.total_weight();
        let initial_keys = tree.num_keys();

        // Find the first leaf and its next_leaf
        let first_leaf_id = 0; // First leaf is always ID 0
        let first_leaf = tree.storage().get_leaf(LeafLocation::new(first_leaf_id));
        let original_next = first_leaf.next_leaf;

        // Create a split leaf that should be inserted after leaf 0
        // The split key must be > all keys in leaf 0 and < keys in the next leaf
        let split_key = first_leaf.entries.last().unwrap().0 + 5;
        let split_leaf = LeafNode {
            entries: vec![
                (split_key, 2),
                (split_key + 1, 3),
            ],
            next_leaf: original_next, // Inherits original's next_leaf
        };

        let result = LeafWriteResult {
            disk_leaves: Vec::new(),
            split_leaves: vec![SplitLeafInfo {
                original_leaf_id: first_leaf_id,
                split_key,
                leaf: split_leaf,
            }],
            segment: None,
            total_weight_delta: 5, // 2 + 3
            total_key_count_delta: 2,
        };

        tree.reconcile_parallel_writes(vec![result]);

        // Verify totals
        assert_eq!(tree.total_weight(), initial_weight + 5);
        assert_eq!(tree.num_keys(), initial_keys + 2);

        // Verify the new keys are findable via select
        // The split keys should appear in sorted order among all entries
        let all_entries: Vec<(i32, ZWeight)> = tree.iter().map(|(k, w)| (*k, w)).collect();
        assert!(all_entries.contains(&(split_key, 2)));
        assert!(all_entries.contains(&(split_key + 1, 3)));

        // Verify sorted order
        for i in 1..all_entries.len() {
            assert!(all_entries[i - 1].0 < all_entries[i].0,
                "Entries not sorted at position {}: {:?} >= {:?}",
                i, all_entries[i - 1], all_entries[i]);
        }

        // Verify select_kth still works
        assert_eq!(tree.select_kth(0, true), Some(&0));
    }

    /// Comparison test: sequential insert vs reconcile should produce
    /// identical tree contents when inserting the same data.
    #[test]
    fn test_reconcile_vs_sequential() {
        use crate::algebra::order_statistics::parallel_routing::{
            LeafWriteResult, SplitLeafInfo, merge_and_split,
        };
        use crate::node_storage::LeafLocation;

        let bf = 4;

        // Build reference tree with all entries via sequential insert
        let mut sequential_tree: OrderStatisticsZSet<i32> = OrderStatisticsZSet::with_config(
            bf,
            NodeStorageConfig::memory_only(),
        );
        // Phase 1: base entries
        for i in 0..20 {
            sequential_tree.insert(i * 10, 1);
        }
        // Phase 2: additional entries
        let additional: Vec<(i32, ZWeight)> = (0..15).map(|i| (i * 10 + 5, 2)).collect();
        for &(k, w) in &additional {
            sequential_tree.insert(k, w);
        }

        // Build reconcile tree: insert only base, then reconcile the additional
        let mut reconcile_tree: OrderStatisticsZSet<i32> = OrderStatisticsZSet::with_config(
            bf,
            NodeStorageConfig::memory_only(),
        );
        for i in 0..20 {
            reconcile_tree.insert(i * 10, 1);
        }

        // Route additional entries to leaves and merge/split manually
        // (simulating what parallel workers would do)
        let max_leaf_entries = reconcile_tree.max_leaf_entries();
        let mut leaf_buckets: std::collections::HashMap<usize, Vec<(i32, ZWeight)>> =
            std::collections::HashMap::new();

        // Route each entry to the correct leaf
        for &(key, weight) in &additional {
            // Find which leaf this entry belongs to by walking the tree
            let leaf_id = find_leaf_for_key(&reconcile_tree, &key);
            leaf_buckets.entry(leaf_id).or_default().push((key, weight));
        }

        // For each leaf bucket, do merge_and_split
        let mut split_leaves = Vec::new();
        let mut total_weight_delta: ZWeight = 0;
        let mut total_key_count_delta: i64 = 0;

        for (leaf_id, mut entries) in leaf_buckets {
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let leaf = reconcile_tree.storage().get_leaf(LeafLocation::new(leaf_id));
            let (modified, splits, wd, kd) = merge_and_split(leaf, &entries, max_leaf_entries);
            total_weight_delta += wd;
            total_key_count_delta += kd;

            // Replace the leaf in-place with the merged version (simulating what a worker
            // would do: merge entries then write to disk. Since we're in-memory, just replace.)
            *reconcile_tree.storage_mut().get_leaf_mut(LeafLocation::new(leaf_id)) = modified;

            for (split_key, split_leaf) in splits {
                split_leaves.push(SplitLeafInfo {
                    original_leaf_id: leaf_id,
                    split_key,
                    leaf: split_leaf,
                });
            }
        }

        // Construct a LeafWriteResult with only splits (leaves already updated in-place)
        let result = LeafWriteResult {
            disk_leaves: Vec::new(),
            split_leaves,
            segment: None,
            total_weight_delta,
            total_key_count_delta,
        };

        reconcile_tree.reconcile_parallel_writes(vec![result]);

        // Compare: both trees should have identical contents
        let seq_entries: Vec<(i32, ZWeight)> = sequential_tree.iter().map(|(k, w)| (*k, w)).collect();
        let rec_entries: Vec<(i32, ZWeight)> = reconcile_tree.iter().map(|(k, w)| (*k, w)).collect();

        assert_eq!(seq_entries, rec_entries,
            "Sequential and reconcile trees have different contents");
        assert_eq!(sequential_tree.total_weight(), reconcile_tree.total_weight(),
            "Total weights differ");
        assert_eq!(sequential_tree.num_keys(), reconcile_tree.num_keys(),
            "Key counts differ");

        // Verify select_kth works on reconciled tree
        for i in 0..reconcile_tree.total_weight() {
            let seq_val = sequential_tree.select_kth(i, true);
            let rec_val = reconcile_tree.select_kth(i, true);
            assert_eq!(seq_val, rec_val,
                "select_kth({}) differs: seq={:?} rec={:?}", i, seq_val, rec_val);
        }
    }

    /// Helper: find which leaf a key would be routed to.
    fn find_leaf_for_key<T: crate::DBData>(tree: &OrderStatisticsZSet<T>, key: &T) -> usize {
        use crate::node_storage::NodeLocation;

        let mut loc = tree.root().expect("tree has root");
        loop {
            match loc {
                NodeLocation::Internal { id, .. } => {
                    let node = tree.storage().get_internal(id);
                    let child_idx = node.find_child(key);
                    loc = node.children[child_idx];
                }
                NodeLocation::Leaf(leaf_loc) => {
                    return leaf_loc.id;
                }
            }
        }
    }
}
