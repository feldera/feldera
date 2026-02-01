#[cfg(test)]
mod tests {
    use crate::algebra::{OrderStatisticsMultiset, ZWeight};

    #[test]
    fn test_empty_tree() {
        let tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.num_keys(), 0);
        assert!(tree.is_empty());
        assert_eq!(tree.select_kth(0, true), None);
        assert_eq!(tree.rank(&10), 0);
    }

    #[test]
    fn test_single_insert() {
        let mut tree = OrderStatisticsMultiset::new();
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
        let mut tree = OrderStatisticsMultiset::new();
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
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 5);
        tree.insert(10, 3); // Should add to existing weight

        assert_eq!(tree.total_weight(), 8);
        assert_eq!(tree.num_keys(), 1);
        assert_eq!(tree.get_weight(&10), 8);
    }

    #[test]
    fn test_negative_weights() {
        let mut tree = OrderStatisticsMultiset::new();
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
        let mut tree1 = OrderStatisticsMultiset::new();
        tree1.insert(10, 3);
        tree1.insert(30, 2);

        let mut tree2 = OrderStatisticsMultiset::new();
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
        let mut tree = OrderStatisticsMultiset::new();
        for i in 1..=100 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&50));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&100));
    }

    #[test]
    fn test_percentile_cont_bounds() {
        let mut tree = OrderStatisticsMultiset::new();
        tree.insert(10, 1);
        tree.insert(20, 1);
        tree.insert(30, 1);
        tree.insert(40, 1);

        // 50th percentile should interpolate between 20 and 30
        let (lower, upper, frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(*lower, 20);
        assert_eq!(*upper, 30);
        assert!((frac - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_descending_select() {
        let mut tree = OrderStatisticsMultiset::new();
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
        let tree: OrderStatisticsMultiset<i32> =
            OrderStatisticsMultiset::from_sorted_entries(vec![], 64);
        assert!(tree.is_empty());
        assert_eq!(tree.num_keys(), 0);
        assert_eq!(tree.total_weight(), 0);
        assert_eq!(tree.select_kth(0, true), None);
    }

    #[test]
    fn test_bulk_load_single() {
        let tree = OrderStatisticsMultiset::from_sorted_entries(vec![(42, 5)], 64);
        assert_eq!(tree.total_weight(), 5);
        assert_eq!(tree.num_keys(), 1);
        assert_eq!(tree.select_kth(0, true), Some(&42));
        assert_eq!(tree.select_kth(4, true), Some(&42));
        assert_eq!(tree.select_kth(5, true), None);
    }

    #[test]
    fn test_bulk_load_matches_incremental() {
        // Build tree incrementally
        let mut incremental: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        for i in 0..1000 {
            incremental.insert(i, (i % 10) as ZWeight + 1);
        }

        // Build tree via bulk load
        let entries: Vec<_> = incremental.iter().map(|(k, w)| (*k, w)).collect();
        let bulk = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

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

            let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

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

        let tree = OrderStatisticsMultiset::from_sorted_entries(entries.clone(), 32);

        let collected: Vec<_> = tree.iter().map(|(k, w)| (*k, w)).collect();
        assert_eq!(collected.len(), entries.len());
        assert_eq!(collected, entries);
    }

    #[test]
    fn test_bulk_load_with_varying_weights() {
        let entries = vec![(10, 3), (20, 2), (30, 5), (40, 1), (50, 4)];
        let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

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
        let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 4);

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
        let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

        // rank(key) = sum of weights of keys < key
        assert_eq!(tree.rank(&0), 0); // No keys less than 0
        assert_eq!(tree.rank(&10), 1); // One key (0) less than 10
        assert_eq!(tree.rank(&50), 5); // Keys 0,10,20,30,40 less than 50
        assert_eq!(tree.rank(&990), 99); // 99 keys less than 990
        assert_eq!(tree.rank(&1000), 100); // All 100 keys less than 1000
    }

    #[test]
    fn test_compact_uses_bulk_load() {
        let mut tree = OrderStatisticsMultiset::new();
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
        let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

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
        let tree = OrderStatisticsMultiset::from_sorted_entries(entries, 64);

        // PERCENTILE_DISC
        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&50));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&100));

        // PERCENTILE_CONT bounds
        let (lower, upper, _frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(*lower, 50);
        assert_eq!(*upper, 51);
    }
}
