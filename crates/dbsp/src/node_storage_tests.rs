// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        node_storage::CachedLeafNode,
        storage::buffer_cache::{BufferCache, CacheEntry},
    };

    use crate::algebra::order_statistics::order_statistics_multiset::{
        InternalNodeTyped, LeafNode, OsmNodeStorage,
    };
    use crate::node_storage::{
        LeafLocation, NodeLocation, NodeRef, NodeStorage, NodeStorageConfig, ZWeight,
        storage_path_to_fs_path,
    };
    use crate::storage::backend::memory_impl::MemoryBackend;

    /// Helper to create a test config with an in-memory storage backend.
    ///
    /// This is required since flush_dirty_to_disk requires a StorageBackend.
    fn test_config_with_storage(threshold_bytes: usize) -> (NodeStorageConfig, Arc<MemoryBackend>) {
        let backend = Arc::new(MemoryBackend::new());
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
    fn test_storage_empty() {
        let storage: OsmNodeStorage<i32> = OsmNodeStorage::new();
        assert!(storage.is_empty());
        assert_eq!(storage.node_count(), 0);
        assert!(!storage.has_dirty_nodes());
    }

    #[test]
    fn test_alloc_leaf() {
        let mut storage: OsmNodeStorage<i32> = OsmNodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        assert!(!storage.is_empty());
        assert_eq!(storage.node_count(), 1);
        assert!(storage.is_leaf(loc));

        let retrieved = storage.get(loc);
        assert!(retrieved.is_leaf());
        assert_eq!(retrieved.leaf_total_weight(), Some(6));
    }

    #[test]
    fn test_alloc_internal() {
        let mut storage: OsmNodeStorage<i32> = OsmNodeStorage::new();

        // First allocate some leaves to reference
        let leaf1 = LeafNode {
            entries: vec![(10, 5)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(20, 10)],
            next_leaf: None,
        };
        let leaf3 = LeafNode {
            entries: vec![(30, 15)],
            next_leaf: None,
        };
        let loc1 = storage.alloc_leaf(leaf1);
        let loc2 = storage.alloc_leaf(leaf2);
        let loc3 = storage.alloc_leaf(leaf3);

        let internal = InternalNodeTyped {
            keys: vec![20, 30],
            children: vec![loc1, loc2, loc3],
            subtree_sums: vec![5, 10, 15],
        };
        // Level 1: directly above leaves
        let loc = storage.alloc_internal(internal, 1);

        assert!(!storage.is_empty());
        assert_eq!(storage.node_count(), 4); // 3 leaves + 1 internal
        assert!(!storage.is_leaf(loc));

        let retrieved = storage.get(loc);
        assert!(!retrieved.is_leaf());
        // Check internal node subtree sums via direct access
        if let NodeRef::Internal(internal) = retrieved {
            assert_eq!(internal.subtree_sums.iter().sum::<ZWeight>(), 30);
        } else {
            panic!("Expected internal node");
        }
    }

    #[test]
    fn test_get_mut() {
        let mut storage: OsmNodeStorage<i32> = OsmNodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Modify through mutable reference
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let leaf_mut = storage.get_leaf_mut(leaf_loc);
            leaf_mut.entries.push((20, 2));
        }

        // Verify modification
        let retrieved = storage.get(loc);
        assert_eq!(retrieved.leaf_total_weight(), Some(3));
    }

    #[test]
    fn test_clear() {
        let mut storage: OsmNodeStorage<i32> = OsmNodeStorage::new();

        let leaf_loc = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        storage.alloc_internal(
            InternalNodeTyped {
                keys: vec![],
                children: vec![leaf_loc],
                subtree_sums: vec![1],
            },
            1,
        );

        assert!(!storage.is_empty());

        storage.clear();

        assert!(storage.is_empty());
        assert_eq!(storage.node_count(), 0);
        assert!(!storage.has_dirty_nodes());
    }

    #[test]
    fn test_stats() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        let loc1 = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        });
        let loc2 = storage.alloc_leaf(LeafNode {
            entries: vec![(30, 3)],
            next_leaf: None,
        });
        storage.alloc_internal(
            InternalNodeTyped {
                keys: vec![30],
                children: vec![loc1, loc2],
                subtree_sums: vec![3, 3],
            },
            1,
        );

        let stats = storage.stats();
        assert_eq!(stats.internal_node_count, 1);
        assert_eq!(stats.leaf_node_count, 2);
        assert_eq!(stats.total_entries, 3);
    }

    // =========================================================================
    // Phase 3: Dirty Tracking Tests
    // =========================================================================

    #[test]
    fn test_new_leaf_is_dirty() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let _loc = storage.alloc_leaf(leaf);

        // New leaf should be dirty
        assert!(storage.has_dirty_nodes());
        assert_eq!(storage.dirty_leaf_count(), 1);
        assert!(storage.dirty_bytes() > 0);
    }

    #[test]
    fn test_new_internal_is_dirty() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // First allocate leaves to reference
        let loc1 = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 5)],
            next_leaf: None,
        });
        let loc2 = storage.alloc_leaf(LeafNode {
            entries: vec![(20, 10)],
            next_leaf: None,
        });

        let internal = InternalNodeTyped {
            keys: vec![20],
            children: vec![loc1, loc2],
            subtree_sums: vec![5, 10],
        };
        let _loc = storage.alloc_internal(internal, 1);

        // New internal node should be dirty
        assert!(storage.has_dirty_nodes());
        assert_eq!(storage.stats().dirty_internal_count, 1);
    }

    #[test]
    fn test_get_mut_marks_dirty() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Allocate a leaf and mark it clean
        let leaf_loc = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        storage.mark_all_clean();

        // Should start clean
        assert!(!storage.has_dirty_nodes());

        // Modify leaf - should become dirty
        if let NodeLocation::Leaf(loc) = leaf_loc {
            let _leaf = storage.get_leaf_mut(loc);
        }

        assert!(storage.has_dirty_nodes());
        assert_eq!(storage.dirty_leaf_count(), 1);
    }

    #[test]
    fn test_mark_all_clean() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Allocate some nodes (they start dirty)
        let leaf_loc = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        storage.alloc_internal(
            InternalNodeTyped {
                keys: vec![],
                children: vec![leaf_loc],
                subtree_sums: vec![1],
            },
            1,
        );

        assert!(storage.has_dirty_nodes());

        // Mark all clean
        storage.mark_all_clean();

        assert!(!storage.has_dirty_nodes());
        assert_eq!(storage.dirty_leaf_count(), 0);
        assert_eq!(storage.dirty_bytes(), 0);
    }

    #[test]
    fn test_should_flush_threshold() {
        // Config with very small threshold
        let config = NodeStorageConfig::with_threshold(100);
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Initially should not need flush
        assert!(!storage.should_flush());

        // Allocate enough leaves to exceed threshold
        for i in 0..10 {
            storage.alloc_leaf(LeafNode {
                entries: vec![(i, 1), (i + 100, 2), (i + 200, 3)],
                next_leaf: None,
            });
        }

        // Now should exceed threshold
        assert!(storage.should_flush());
        assert!(storage.maybe_flush_dirty_leaves());
    }

    #[test]
    fn test_dirty_bytes_tracking() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        let initial_dirty = storage.dirty_bytes();
        assert_eq!(initial_dirty, 0);

        // Allocate a leaf
        storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        });

        let after_alloc = storage.dirty_bytes();
        assert!(after_alloc > 0);

        // Allocate another leaf
        storage.alloc_leaf(LeafNode {
            entries: vec![(30, 3)],
            next_leaf: None,
        });

        let after_second = storage.dirty_bytes();
        assert!(after_second > after_alloc);
    }

    #[test]
    fn test_leaf_size_estimation() {
        let leaf = LeafNode {
            entries: vec![(1i32, 1), (2, 2), (3, 3)],
            next_leaf: None,
        };

        let size = OsmNodeStorage::<i32>::estimate_leaf_size(&leaf);

        // Should include base size plus entries
        assert!(size > 0);
        assert!(size >= std::mem::size_of::<LeafNode<i32>>());
    }

    #[test]
    fn test_internal_size_estimation() {
        let internal = InternalNodeTyped {
            keys: vec![10i32, 20, 30],
            children: vec![
                NodeLocation::Leaf(LeafLocation::new(0)),
                NodeLocation::Leaf(LeafLocation::new(1)),
                NodeLocation::Leaf(LeafLocation::new(2)),
                NodeLocation::Leaf(LeafLocation::new(3)),
            ],
            subtree_sums: vec![5, 10, 15, 20],
        };

        let size = OsmNodeStorage::<i32>::estimate_internal_size(&internal);

        // Should include base size plus vectors
        assert!(size > 0);
        assert!(size >= std::mem::size_of::<InternalNodeTyped<i32>>());
    }

    #[test]
    fn test_dirty_iterators() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Allocate leaves (dirty)
        let loc1 = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        let loc2 = storage.alloc_leaf(LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        });

        // Allocate internal (dirty)
        storage.alloc_internal(
            InternalNodeTyped {
                keys: vec![20],
                children: vec![loc1, loc2],
                subtree_sums: vec![1, 2],
            },
            1,
        );

        // Check dirty leaf iterator
        let dirty_leaves: Vec<_> = storage.dirty_leaf_ids().collect();
        assert_eq!(dirty_leaves.len(), 2);
        assert!(dirty_leaves.contains(&0));
        assert!(dirty_leaves.contains(&1));

        // Check dirty internal iterator
        let dirty_internals: Vec<_> = storage.dirty_internal_ids().collect();
        assert_eq!(dirty_internals.len(), 1);
        assert!(dirty_internals.contains(&0));
    }

    #[test]
    fn test_memory_only_config() {
        let config = NodeStorageConfig::memory_only();
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Allocate lots of data
        for i in 0..1000 {
            storage.alloc_leaf(LeafNode {
                entries: vec![(i, 1)],
                next_leaf: None,
            });
        }

        // Should never recommend flush (threshold is MAX)
        assert!(!storage.should_flush());
    }

    #[test]
    fn test_update_stats() {
        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });

        // Manually modify a leaf (bypassing dirty tracking for test)
        storage.leaves[0]
            .as_present_mut()
            .unwrap()
            .entries
            .push((20, 2));

        // Stats may be stale
        let old_entries = storage.stats().total_entries;

        // Update stats
        storage.update_stats();

        // Now should reflect the change
        assert_eq!(storage.stats().total_entries, 2);
        assert!(storage.stats().total_entries > old_entries || old_entries == 1);
    }

    // =========================================================================
    // NodeStorage Disk Spilling Tests
    // =========================================================================

    #[test]
    fn test_flush_dirty_to_disk_empty_old_api() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Flushing empty storage should succeed with 0 leaves written
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_flush_dirty_to_disk_basic_old_api() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add some leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(30, 3), (40, 4), (50, 5)],
            next_leaf: None,
        };

        storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Verify dirty state
        assert_eq!(storage.dirty_leaf_count(), 2);
        assert!(storage.dirty_bytes() > 0);

        // Flush to disk
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 2);

        // Verify clean state
        assert_eq!(storage.dirty_leaf_count(), 0);
        assert_eq!(storage.dirty_bytes(), 0);
        assert_eq!(storage.spilled_leaf_count(), 2);
        assert!(storage.is_leaf_spilled(0));
        assert!(storage.is_leaf_spilled(1));

        // Verify stats
        assert_eq!(storage.stats().leaves_written, 2);
    }

    #[test]
    fn test_flush_and_reload() {
        let (config, _backend) = test_config_with_storage(1024);

        let original_entries1 = vec![(100i32, 10i64), (200, 20), (300, 30)];
        let original_entries2 = vec![(400, 40), (500, 50)];

        // Create and flush storage
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        let leaf1 = LeafNode {
            entries: original_entries1.clone(),
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: original_entries2.clone(),
            next_leaf: None,
        };

        let loc1 = storage.alloc_leaf(leaf1);
        let loc2 = storage.alloc_leaf(leaf2);

        // Flush to disk
        storage.flush_dirty_to_disk().unwrap();
        assert_eq!(storage.segment_count(), 1);

        // Evict leaves from memory
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 2);

        // Load leaves back from disk and verify contents
        let leaf_loc1 = loc1.as_leaf().unwrap();
        let loaded1 = storage.load_leaf_from_disk(leaf_loc1).unwrap();
        assert_eq!(loaded1.entries, original_entries1);

        let leaf_loc2 = loc2.as_leaf().unwrap();
        let loaded2 = storage.load_leaf_from_disk(leaf_loc2).unwrap();
        assert_eq!(loaded2.entries, original_entries2);
    }

    #[test]
    fn test_threshold_check_triggers_flush() {
        let dir = tempfile::tempdir().unwrap();

        // Very low threshold to trigger easily (100 bytes threshold)
        let config =
            NodeStorageConfig::with_spill_directory(100, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add a leaf that exceeds threshold
        let leaf = LeafNode {
            entries: vec![
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5),
                (6, 6),
                (7, 7),
                (8, 8),
            ],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        // Should trigger flush
        assert!(storage.should_flush());

        // Flush
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 1);

        // Should no longer need flush
        assert!(!storage.should_flush());
    }

    #[test]
    fn test_backpressure() {
        // Very low threshold to trigger easily (50 bytes)
        let mut storage: OsmNodeStorage<i32> =
            NodeStorage::with_config(NodeStorageConfig::with_threshold(50));

        // Initially no backpressure
        assert!(!storage.should_apply_backpressure());
        assert!(storage.should_relieve_backpressure());
        let (should_apply, ratio) = storage.backpressure_status();
        assert!(!should_apply);
        assert_eq!(ratio, 0.0);

        // Add a leaf that exceeds threshold but not 2x threshold (backpressure high multiplier)
        let leaf = LeafNode {
            entries: vec![(1, 1), (2, 2), (3, 3)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf.clone());

        // Should trigger flush but not backpressure (dirty_bytes ~= 72 bytes, threshold = 50)
        assert!(storage.should_flush());
        assert!(!storage.should_apply_backpressure()); // Not yet 2x threshold
        let (should_apply, ratio) = storage.backpressure_status();
        assert!(!should_apply);
        assert!(ratio > 1.0); // Above threshold

        // Add more data to exceed 2x threshold
        storage.alloc_leaf(leaf.clone());
        storage.alloc_leaf(leaf);

        // Now should trigger backpressure (dirty_bytes ~= 216 bytes, threshold = 50, 2x = 100)
        assert!(storage.should_apply_backpressure());
        assert!(!storage.should_relieve_backpressure());
        let (should_apply, ratio) = storage.backpressure_status();
        assert!(should_apply);
        assert!(ratio >= 2.0); // At least 2x threshold

        // Clear dirty tracking to simulate flush
        storage.mark_all_clean();

        // After flush, backpressure should be relieved
        assert!(!storage.should_apply_backpressure());
        assert!(storage.should_relieve_backpressure());
    }

    #[test]
    fn test_backpressure_disabled_without_spill() {
        // Memory-only config should never trigger backpressure
        let mut storage: OsmNodeStorage<i32> =
            NodeStorage::with_config(NodeStorageConfig::memory_only());

        // Add lots of data
        for i in 0..100 {
            let leaf = LeafNode {
                entries: vec![(i, 1), (i + 1000, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Should never trigger backpressure
        assert!(!storage.should_apply_backpressure());
    }

    #[test]
    fn test_cleanup_segments() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        // Flush
        storage.flush_dirty_to_disk().unwrap();
        assert_eq!(storage.segment_count(), 1);

        // Get segment path for verification - pass the backend for correct path resolution
        let segment_path = storage_path_to_fs_path(
            &storage.segments()[0].path,
            storage.config_ref().storage_backend.as_deref(),
        );
        assert!(segment_path.exists());

        // Cleanup
        storage.cleanup_segments().unwrap();
        assert!(!segment_path.exists());
        assert_eq!(storage.segment_count(), 0);
        assert_eq!(storage.spilled_leaf_count(), 0);
    }

    #[test]
    fn test_incremental_flush() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add first leaf
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush first batch - creates first segment
        let count1 = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count1, 1);
        assert_eq!(storage.dirty_leaf_count(), 0);
        assert_eq!(storage.segment_count(), 1);

        // Add second leaf
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf2);

        // Only new leaf is dirty
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Flush second batch - creates second segment
        let count2 = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count2, 1);
        assert_eq!(storage.segment_count(), 2);

        // Stats show total leaves written
        assert_eq!(storage.stats().leaves_written, 2);
    }

    #[test]
    fn test_load_leaf_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let original_entries = vec![(111i32, 11i64), (222, 22), (333, 33)];

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk
        storage.flush_dirty_to_disk().unwrap();

        // Load from disk (even though it's still in memory, this tests the path)
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.load_leaf_from_disk(leaf_loc).unwrap();
            assert_eq!(loaded.entries, original_entries);
        } else {
            panic!("Expected leaf location");
        }
    }

    // =========================================================================
    // Phase 6: Memory Eviction Tests
    // =========================================================================

    #[test]
    fn test_evict_clean_leaves_basic() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(30, 3), (40, 4), (50, 5)],
            next_leaf: None,
        };

        storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Initially all in memory
        assert_eq!(storage.in_memory_leaf_count(), 2);
        assert_eq!(storage.evicted_leaf_count(), 0);

        // Flush to disk
        storage.flush_dirty_to_disk().unwrap();

        // Still all in memory (flush doesn't evict)
        assert_eq!(storage.in_memory_leaf_count(), 2);
        assert_eq!(storage.evicted_leaf_count(), 0);
        assert_eq!(storage.spilled_leaf_count(), 2);

        // Now evict clean leaves
        let (evicted_count, bytes_freed) = storage.evict_clean_leaves();

        // Both leaves should be evicted
        assert_eq!(evicted_count, 2);
        assert!(bytes_freed > 0);
        assert_eq!(storage.in_memory_leaf_count(), 0);
        assert_eq!(storage.evicted_leaf_count(), 2);
        assert!(storage.is_leaf_evicted(LeafLocation::new(0)));
        assert!(storage.is_leaf_evicted(LeafLocation::new(1)));
    }

    #[test]
    fn test_evict_does_not_affect_dirty_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };

        let loc1 = storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Flush both to disk
        storage.flush_dirty_to_disk().unwrap();

        // Now modify leaf1 (making it dirty again)
        if let NodeLocation::Leaf(leaf_loc) = loc1 {
            let leaf = storage.get_leaf_mut(leaf_loc);
            leaf.entries.push((100, 10));
        }

        // leaf1 is now dirty, leaf2 is clean
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Evict clean leaves
        let (evicted_count, _) = storage.evict_clean_leaves();

        // Only leaf2 should be evicted (leaf1 is dirty)
        assert_eq!(evicted_count, 1);
        assert_eq!(storage.in_memory_leaf_count(), 1);
        assert_eq!(storage.evicted_leaf_count(), 1);
        assert!(!storage.is_leaf_evicted(LeafLocation::new(0))); // Still in memory (dirty)
        assert!(storage.is_leaf_evicted(LeafLocation::new(1))); // Evicted
    }

    #[test]
    fn test_evict_does_not_affect_non_spilled_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add first leaf
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush leaf1 to disk
        storage.flush_dirty_to_disk().unwrap();

        // Add second leaf (not yet spilled)
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf2);

        // leaf1 is spilled and clean, leaf2 is dirty (not spilled)
        assert_eq!(storage.spilled_leaf_count(), 1);
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Evict clean leaves
        let (evicted_count, _) = storage.evict_clean_leaves();

        // Only leaf1 should be evicted
        assert_eq!(evicted_count, 1);
        assert_eq!(storage.in_memory_leaf_count(), 1); // leaf2 still in memory
        assert_eq!(storage.evicted_leaf_count(), 1);
    }

    #[test]
    fn test_reload_evicted_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush and evict
        storage.flush_dirty_to_disk().unwrap();
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 1);

        // Verify it's evicted
        if let NodeLocation::Leaf(leaf_loc) = loc {
            assert!(storage.is_leaf_evicted(leaf_loc));
            assert!(!storage.is_leaf_in_memory(leaf_loc));
        }

        // Reload using get_leaf_reloading
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.get_leaf_reloading(leaf_loc);
            assert_eq!(loaded.entries, original_entries);

            // Now it should be back in memory
            assert!(!storage.is_leaf_evicted(leaf_loc));
            assert!(storage.is_leaf_in_memory(leaf_loc));
        }
    }

    #[test]
    fn test_reload_evicted_leaves_bulk() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add multiple leaves
        for i in 0..5 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Flush and evict all
        storage.flush_dirty_to_disk().unwrap();
        let (evicted, bytes_freed) = storage.evict_clean_leaves();
        assert_eq!(evicted, 5);
        assert!(bytes_freed > 0);
        assert_eq!(storage.in_memory_leaf_count(), 0);
        assert_eq!(storage.evicted_leaf_count(), 5);

        // Reload all evicted leaves
        let reloaded = storage.reload_evicted_leaves().unwrap();
        assert_eq!(reloaded, 5);
        assert_eq!(storage.in_memory_leaf_count(), 5);
        assert_eq!(storage.evicted_leaf_count(), 0);

        // Verify data integrity
        for i in 0..5 {
            let leaf = storage.get_leaf(LeafLocation::new(i));
            assert_eq!(leaf.entries[0].0, (i * 10) as i32);
        }
    }

    #[test]
    fn test_eviction_memory_stats() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        let memory_before = storage.stats().memory_bytes;
        assert!(memory_before > 0);

        // Flush and evict
        storage.flush_dirty_to_disk().unwrap();
        let (_, bytes_freed) = storage.evict_clean_leaves();

        let memory_after = storage.stats().memory_bytes;

        // Memory should be reduced
        assert!(bytes_freed > 0);
        assert!(memory_after < memory_before);
        assert_eq!(memory_before - memory_after, bytes_freed);
    }

    #[test]
    fn test_cache_hit_miss_stats() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaf
        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush and evict
        storage.flush_dirty_to_disk().unwrap();
        storage.evict_clean_leaves();

        // Initial stats
        assert_eq!(storage.stats().cache_hits, 0);
        assert_eq!(storage.stats().cache_misses, 0);

        // First access - should be a cache miss (reload from disk)
        if let NodeLocation::Leaf(leaf_loc) = loc {
            storage.get_leaf_reloading(leaf_loc);
        }
        assert_eq!(storage.stats().cache_misses, 1);

        // Second access - should be a cache hit (in memory now)
        if let NodeLocation::Leaf(leaf_loc) = loc {
            storage.get_leaf_reloading(leaf_loc);
        }
        assert_eq!(storage.stats().cache_hits, 1);
    }

    #[test]
    fn test_no_eviction_without_spill() {
        let mut storage: OsmNodeStorage<i32> =
            NodeStorage::with_config(NodeStorageConfig::memory_only());

        // Add leaves
        for i in 0..10 {
            let leaf = LeafNode {
                entries: vec![(i, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Try to evict - should evict nothing (nothing is spilled)
        let (evicted, bytes_freed) = storage.evict_clean_leaves();
        assert_eq!(evicted, 0);
        assert_eq!(bytes_freed, 0);
        assert_eq!(storage.in_memory_leaf_count(), 10);
    }

    // =========================================================================
    // Phase 7: BufferCache Integration Tests
    // =========================================================================

    #[test]
    fn test_cached_leaf_node_cost() {
        let leaf = LeafNode {
            entries: vec![(10i32, 1i64), (20, 2), (30, 3)],
            next_leaf: None,
        };

        let cached = CachedLeafNode {
            leaf,
            size_bytes: 1024,
        };

        // CacheEntry trait: cost returns size_bytes
        assert_eq!(cached.cost(), 1024);
    }

    #[test]
    fn test_buffer_cache_config_integration() {
        // Create a BufferCache
        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024)); // 1MB cache

        // Create config with BufferCache
        let config = NodeStorageConfig {
            enable_spill: true,

            spill_threshold_bytes: 64 * 1024,      // 64KB
            target_segment_size: 64 * 1024 * 1024, // 64MB
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: None,
            buffer_cache: Some(buffer_cache.clone()),
        };

        // Create storage with config
        let storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Verify config is stored
        assert!(storage.config.buffer_cache.is_some());
    }

    #[test]
    fn test_flush_records_disk_locations() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeStorageConfig::with_spill_directory(
            64 * 1024,
            dir.path().to_string_lossy().as_ref(),
        );

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        for i in 0..5 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Flush to disk
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 5);

        // Disk locations should be recorded
        assert_eq!(storage.leaves_in_segments(), 5);

        // Verify each leaf has a disk location
        for i in 0..5 {
            assert!(storage.leaf_has_segment_location(i));
        }

        // Segments should be created
        assert_eq!(storage.segment_count(), 1);
    }

    #[test]
    fn test_cleanup_clears_disk_locations() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeStorageConfig::with_spill_directory(
            64 * 1024,
            dir.path().to_string_lossy().as_ref(),
        );

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add and flush a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);
        storage.flush_dirty_to_disk().unwrap();

        // Verify locations are recorded
        assert!(storage.leaves_in_segments() > 0);
        assert_eq!(storage.segment_count(), 1);

        // Cleanup
        storage.cleanup_segments().unwrap();

        // All tracking data should be cleared
        assert_eq!(storage.leaves_in_segments(), 0);
        assert_eq!(storage.segment_count(), 0);
    }

    #[test]
    fn test_evict_and_reload_from_segment() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeStorageConfig::with_spill_directory(
            64 * 1024,
            dir.path().to_string_lossy().as_ref(),
        );

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk
        storage.flush_dirty_to_disk().unwrap();

        // Evict the leaf
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 1);

        // Verify evicted
        if let NodeLocation::Leaf(leaf_loc) = loc {
            assert!(storage.is_leaf_evicted(leaf_loc));
        }

        // Reload from segment
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.get_leaf_reloading(leaf_loc);
            assert_eq!(loaded.entries, original_entries);

            // Should be back in memory
            assert!(!storage.is_leaf_evicted(leaf_loc));
        }

        // The leaf should have a segment location
        assert!(storage.leaf_has_segment_location(0));
    }

    #[test]
    fn test_multiple_flushes_create_segments() {
        let dir = tempfile::tempdir().unwrap();
        let config = NodeStorageConfig::with_spill_directory(
            64 * 1024,
            dir.path().to_string_lossy().as_ref(),
        );

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // First batch of leaves
        for i in 0..3 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // First flush - creates first segment
        storage.flush_dirty_to_disk().unwrap();
        assert_eq!(storage.segment_count(), 1);
        assert_eq!(storage.leaves_in_segments(), 3);

        // Add more leaves
        for i in 3..6 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Second flush - creates second segment
        storage.flush_dirty_to_disk().unwrap();
        assert_eq!(storage.segment_count(), 2);
        assert_eq!(storage.leaves_in_segments(), 6);

        // All leaves should have locations
        for i in 0..6 {
            assert!(storage.leaf_has_segment_location(i));
        }
    }

    // =========================================================================
    // Checkpoint-Resume Tests
    // =========================================================================

    #[test]
    fn test_prepare_checkpoint_preserves_runtime_state() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(40, 4), (50, 5)],
            next_leaf: None,
        };
        let loc1 = storage.alloc_leaf(leaf1);
        let loc2 = storage.alloc_leaf(leaf2);

        // Flush to disk first (creates segment)
        storage.flush_dirty_to_disk().unwrap();

        // Verify segment exists
        assert!(storage.segment_count() > 0);

        // Prepare checkpoint - this should NOT invalidate runtime state
        let metadata = storage.prepare_checkpoint().unwrap();

        // Verify runtime state is preserved (segments NOT cleared)
        assert!(
            storage.segment_count() > 0,
            "segments should be preserved after prepare_checkpoint"
        );

        // Verify metadata is correct
        assert_eq!(metadata.num_leaves, 2);
        assert_eq!(metadata.leaf_summaries.len(), 2);
        assert_eq!(metadata.segment_metadata.len(), 1);

        // Verify leaf summaries
        let summary1 = &metadata.leaf_summaries[0];
        assert_eq!(summary1.first_key, Some(10));
        assert_eq!(summary1.weight_sum, 6); // 1 + 2 + 3
        assert_eq!(summary1.entry_count, 3);

        let summary2 = &metadata.leaf_summaries[1];
        assert_eq!(summary2.first_key, Some(40));
        assert_eq!(summary2.weight_sum, 9); // 4 + 5
        assert_eq!(summary2.entry_count, 2);

        // Verify runtime can still read leaves
        let retrieved1 = storage.get(loc1);
        assert!(retrieved1.is_leaf());
        assert_eq!(retrieved1.leaf_total_weight(), Some(6));

        let retrieved2 = storage.get(loc2);
        assert!(retrieved2.is_leaf());
        assert_eq!(retrieved2.leaf_total_weight(), Some(9));
    }

    #[test]
    fn test_prepare_checkpoint_runtime_can_continue_writing() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add initial leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush to disk
        storage.flush_dirty_to_disk().unwrap();

        // Prepare checkpoint
        let metadata1 = storage.prepare_checkpoint().unwrap();
        assert_eq!(metadata1.num_leaves, 1);

        // Runtime continues - add more leaves AFTER checkpoint
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        let loc2 = storage.alloc_leaf(leaf2);

        // Verify new leaf is accessible
        let retrieved = storage.get(loc2);
        assert!(retrieved.is_leaf());
        assert_eq!(retrieved.leaf_total_weight(), Some(2));

        // Flush the new leaf (to existing segment directory)
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 1);

        // Prepare another checkpoint - should include both leaves now
        let metadata2 = storage.prepare_checkpoint().unwrap();
        assert_eq!(metadata2.num_leaves, 2);
    }

    #[test]
    fn test_prepare_checkpoint_with_evicted_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(100, 10), (200, 20)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(300, 30)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Flush and evict
        storage.flush_dirty_to_disk().unwrap();
        let (evicted_count, _) = storage.evict_clean_leaves();
        assert_eq!(evicted_count, 2);

        // Verify leaves are evicted
        assert_eq!(storage.evicted_leaf_count(), 2);

        // Prepare checkpoint - should reload evicted leaves to collect summaries
        let metadata = storage.prepare_checkpoint().unwrap();

        // Verify metadata is correct even with evicted leaves
        assert_eq!(metadata.num_leaves, 2);
        assert_eq!(metadata.leaf_summaries.len(), 2);

        let summary1 = &metadata.leaf_summaries[0];
        assert_eq!(summary1.first_key, Some(100));
        assert_eq!(summary1.weight_sum, 30); // 10 + 20
        assert_eq!(summary1.entry_count, 2);

        let summary2 = &metadata.leaf_summaries[1];
        assert_eq!(summary2.first_key, Some(300));
        assert_eq!(summary2.weight_sum, 30);
        assert_eq!(summary2.entry_count, 1);
    }

    // =========================================================================
    // Segment-Based Storage Tests (Phase 2)
    // =========================================================================

    #[test]
    fn test_segment_storage_initial_state() {
        let storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Initially no segments
        assert_eq!(storage.segment_count(), 0);
        assert!(!storage.has_pending_segment());
        assert_eq!(storage.next_segment_id(), 0);
        assert!(!storage.uses_segment_storage());
        assert_eq!(storage.leaves_in_segments(), 0);
    }

    #[test]
    fn test_flush_dirty_to_disk_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Flushing empty storage should return 0
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 0);
        assert_eq!(storage.segment_count(), 0);
    }

    #[test]
    fn test_flush_dirty_to_disk_single_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);
        assert!(storage.has_dirty_nodes());

        // Flush to segment
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 1);
        assert_eq!(storage.segment_count(), 1);
        assert!(storage.uses_segment_storage());
        assert_eq!(storage.leaves_in_segments(), 1);
        assert!(storage.leaf_has_segment_location(loc.as_leaf().unwrap().id));

        // No more dirty leaves
        assert!(!storage.has_dirty_nodes() || storage.dirty_leaf_count() == 0);
    }

    #[test]
    fn test_flush_dirty_to_disk_multiple_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add multiple leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(100, 10), (200, 20)],
            next_leaf: None,
        };
        let leaf3 = LeafNode {
            entries: vec![(1000, 100)],
            next_leaf: None,
        };
        let loc1 = storage.alloc_leaf(leaf1);
        let loc2 = storage.alloc_leaf(leaf2);
        let loc3 = storage.alloc_leaf(leaf3);

        // Flush all to one segment
        let count = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count, 3);
        assert_eq!(storage.segment_count(), 1);
        assert_eq!(storage.leaves_in_segments(), 3);

        // All leaves should have segment locations
        assert!(storage.leaf_has_segment_location(loc1.as_leaf().unwrap().id));
        assert!(storage.leaf_has_segment_location(loc2.as_leaf().unwrap().id));
        assert!(storage.leaf_has_segment_location(loc3.as_leaf().unwrap().id));
    }

    #[test]
    fn test_multiple_segment_flushes() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // First batch of leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush to first segment
        let count1 = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count1, 1);
        assert_eq!(storage.segment_count(), 1);
        assert_eq!(storage.next_segment_id(), 1);

        // Second batch of leaves
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf2);

        // Flush to second segment
        let count2 = storage.flush_dirty_to_disk().unwrap();
        assert_eq!(count2, 1);
        assert_eq!(storage.segment_count(), 2);
        assert_eq!(storage.next_segment_id(), 2);
        assert_eq!(storage.leaves_in_segments(), 2);

        // Verify segments have different IDs
        let segments = storage.segments();
        assert_eq!(segments[0].id.value(), 0);
        assert_eq!(segments[1].id.value(), 1);
    }

    #[test]
    fn test_load_leaf_from_disk_after_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add and flush a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);
        storage.flush_dirty_to_disk().unwrap();

        // Leaf should still be in memory (not evicted)
        let leaf_ref = storage.get_leaf(loc.as_leaf().unwrap());
        assert_eq!(leaf_ref.entries.len(), 3);

        // Evict the leaf
        storage.evict_clean_leaves();

        // Load from segment
        let leaf_id = loc.as_leaf().unwrap().id;
        let loaded = storage
            .load_leaf_from_disk(LeafLocation::new(leaf_id))
            .unwrap();
        assert_eq!(loaded.entries.len(), 3);
        assert_eq!(loaded.entries[0], (10, 1));
        assert_eq!(loaded.entries[1], (20, 2));
        assert_eq!(loaded.entries[2], (30, 3));
    }

    #[test]
    fn test_load_leaf_from_disk_multiple() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add multiple leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(100, 10), (200, 20)],
            next_leaf: None,
        };
        let loc1 = storage.alloc_leaf(leaf1);
        let loc2 = storage.alloc_leaf(leaf2);

        // Flush to segment
        storage.flush_dirty_to_disk().unwrap();

        // Evict all leaves
        storage.evict_clean_leaves();

        // Load leaf 2 (second leaf)
        let id2 = loc2.as_leaf().unwrap().id;
        let loaded2 = storage.load_leaf_from_disk(LeafLocation::new(id2)).unwrap();
        assert_eq!(loaded2.entries.len(), 2);
        assert_eq!(loaded2.entries[0], (100, 10));

        // Load leaf 1 (first leaf)
        let id1 = loc1.as_leaf().unwrap().id;
        let loaded1 = storage.load_leaf_from_disk(LeafLocation::new(id1)).unwrap();
        assert_eq!(loaded1.entries.len(), 1);
        assert_eq!(loaded1.entries[0], (10, 1));
    }

    #[test]
    fn test_segment_metadata_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(100, 10)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Flush to segment
        storage.flush_dirty_to_disk().unwrap();

        // Check segment metadata
        let segments = storage.segments();
        assert_eq!(segments.len(), 1);

        let seg = &segments[0];
        assert_eq!(seg.id.value(), 0);
        assert_eq!(seg.leaf_count, 2);
        assert!(seg.size_bytes > 0);
        assert!(seg.contains_leaf(0));
        assert!(seg.contains_leaf(1));
        assert!(!seg.contains_leaf(2));

        // Check leaf locations in segment
        let loc0 = seg.get_leaf_location(0);
        assert!(loc0.is_some());
        let loc0 = loc0.unwrap();
        assert_eq!(loc0.segment_id.value(), 0);

        let loc1 = seg.get_leaf_location(1);
        assert!(loc1.is_some());
        let loc1 = loc1.unwrap();
        assert_eq!(loc1.segment_id.value(), 0);
        // Both leaves should have valid offsets (they may or may not be ordered)
        // Just verify they're not at the same offset (different blocks)
        assert!(loc0.offset != loc1.offset || loc0.size != loc1.size);
    }

    // =========================================================================
    // get_leaf_reloading_mut Tests
    // =========================================================================

    #[test]
    fn test_get_leaf_reloading_mut_evicted() {
        let dir = tempfile::tempdir().unwrap();
        let config =
            NodeStorageConfig::with_spill_directory(1024, dir.path().to_string_lossy().as_ref());

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk and evict
        storage.flush_dirty_to_disk().unwrap();
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 1);

        // Verify it's evicted
        if let NodeLocation::Leaf(leaf_loc) = loc {
            assert!(storage.is_leaf_evicted(leaf_loc));

            // Get mutable reference to evicted leaf - should auto-reload
            let leaf_mut = storage.get_leaf_reloading_mut(leaf_loc);
            assert_eq!(leaf_mut.entries, original_entries);

            // Mutate the leaf
            leaf_mut.entries.push((40, 4));

            // Verify mutation stuck
            let leaf_ref = storage.get_leaf(leaf_loc);
            assert_eq!(leaf_ref.entries.len(), 4);
            assert_eq!(leaf_ref.entries[3], (40, 4));

            // Should be back in memory and dirty
            assert!(!storage.is_leaf_evicted(leaf_loc));
            assert!(storage.is_leaf_in_memory(leaf_loc));
        } else {
            panic!("Expected leaf location");
        }
    }
}
