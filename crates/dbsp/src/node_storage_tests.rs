// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rkyv::Deserialize;

    use crate::{
        node_storage::CachedLeafNode,
        storage::buffer_cache::{BufferCache, CacheEntry},
    };

    use crate::algebra::order_statistics::order_statistics_multiset::{
        InternalNodeTyped, LeafNode, OsmNodeStorage,
    };
    use crate::node_storage::{
        DATA_BLOCK_HEADER_SIZE, FILE_HEADER_SIZE, FileFormatError, FileHeader, LeafFile,
        verify_data_block_header,
    };
    use crate::node_storage::{
        LeafLocation, NodeLocation, NodeRef, NodeStorage, NodeStorageConfig, ZWeight,
    };

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
        storage.leaves[0].as_mut().unwrap().entries.push((20, 2));

        // Stats may be stale
        let old_entries = storage.stats().total_entries;

        // Update stats
        storage.update_stats();

        // Now should reflect the change
        assert_eq!(storage.stats().total_entries, 2);
        assert!(storage.stats().total_entries > old_entries || old_entries == 1);
    }

    // =========================================================================
    // Phase 4: LeafFile Disk I/O Tests
    // =========================================================================

    #[test]
    fn test_leaf_file_create_and_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_leaves.osm");

        let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();

        // Write a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let location = leaf_file.write_leaf(0, &leaf).unwrap();

        assert!(location.offset > 0);
        assert!(location.size > 0);
        assert_eq!(leaf_file.num_leaves(), 1);
        assert!(leaf_file.contains(0));
    }

    #[test]
    fn test_leaf_file_header_roundtrip() {
        use std::io::Read as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_header.osm");

        // Create and finalize a leaf file
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: vec![(10, 1)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read raw header bytes
        let mut file = std::fs::File::open(&path).unwrap();
        let mut header_bytes = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header_bytes).unwrap();

        // Manually verify checksum
        let stored_checksum = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
        let computed_checksum = crc32c::crc32c(&header_bytes[4..]);

        eprintln!("Stored checksum: {}", stored_checksum);
        eprintln!("Computed checksum: {}", computed_checksum);
        eprintln!("Magic bytes: {:?}", &header_bytes[4..8]);

        assert_eq!(
            stored_checksum, computed_checksum,
            "Header checksum mismatch"
        );

        // Also verify with from_bytes
        let header = FileHeader::from_bytes(&header_bytes).unwrap();
        assert_eq!(header.num_leaves, 1);
    }

    #[test]
    fn test_leaf_node_rkyv_roundtrip() {
        // Test rkyv serialization/deserialization directly without file I/O
        let original = LeafNode {
            entries: vec![(10i32, 1i64), (20, 2), (30, 3)],
            next_leaf: None,
        };

        // Serialize
        let bytes = rkyv::to_bytes::<_, 4096>(&original).unwrap();
        eprintln!("Serialized {} bytes", bytes.len());

        // Deserialize
        let archived = unsafe { rkyv::archived_root::<LeafNode<i32>>(&bytes) };
        let restored: LeafNode<i32> = archived.deserialize(&mut rkyv::Infallible).unwrap();

        // Verify
        assert_eq!(restored.entries.len(), 3, "entries len mismatch");
        assert_eq!(restored.entries[0], (10, 1));
        assert_eq!(restored.entries[1], (20, 2));
        assert_eq!(restored.entries[2], (30, 3));
        assert_eq!(restored.next_leaf, None);
    }

    #[test]
    fn test_leaf_file_debug_write_read() {
        use std::io::{Read as _, Seek as _};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_debug.osm");

        let original = LeafNode {
            entries: vec![(10i32, 1i64), (20, 2)],
            next_leaf: None,
        };

        // Write using LeafFile
        let location = {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();
            let loc = leaf_file.write_leaf(0, &original).unwrap();
            eprintln!("Wrote leaf at offset {}, size {}", loc.offset, loc.size);
            leaf_file.finalize().unwrap();
            loc
        };

        // Read the raw data block manually
        {
            let mut file = std::fs::File::open(&path).unwrap();

            // Seek to the data block
            file.seek(std::io::SeekFrom::Start(location.offset))
                .unwrap();

            // Read the block
            let mut block = vec![0u8; location.size as usize];
            file.read_exact(&mut block).unwrap();

            eprintln!("Block first 32 bytes: {:?}", &block[0..32.min(block.len())]);

            // Verify header and get data_len
            let (_leaf_id, data_len) = verify_data_block_header(&block).unwrap();
            eprintln!("Extracted data_len from header: {}", data_len);

            // Extract only the actual serialized data (without padding)
            let data = &block[DATA_BLOCK_HEADER_SIZE..DATA_BLOCK_HEADER_SIZE + data_len as usize];
            eprintln!("Data section len: {}", data.len());
            eprintln!("Data first 32 bytes: {:?}", &data[0..32.min(data.len())]);

            // Try to deserialize directly from data
            let archived = unsafe { rkyv::archived_root::<LeafNode<i32>>(data) };
            let restored: LeafNode<i32> = archived.deserialize(&mut rkyv::Infallible).unwrap();

            eprintln!("Restored entries: {:?}", restored.entries);
            assert_eq!(
                restored.entries.len(),
                2,
                "Manual read: entries len mismatch"
            );
        }

        // Read using LeafFile
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();
            let loaded = leaf_file.load_leaf(0).unwrap();
            eprintln!("Loaded via LeafFile entries: {:?}", loaded.entries);
            assert_eq!(
                loaded.entries.len(),
                2,
                "LeafFile read: entries len mismatch"
            );
        }
    }

    #[test]
    fn test_leaf_file_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_leaves.osm");

        // Write
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();

            let leaf1 = LeafNode {
                entries: vec![(10, 1), (20, 2)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf1).unwrap();

            let leaf2 = LeafNode {
                entries: vec![(30, 3), (40, 4), (50, 5)],
                next_leaf: None,
            };
            leaf_file.write_leaf(1, &leaf2).unwrap();

            leaf_file.finalize().unwrap();
        }

        // Read
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();

            assert_eq!(leaf_file.num_leaves(), 2);

            let loaded1 = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded1.entries.len(), 2);
            assert_eq!(loaded1.entries[0], (10, 1));
            assert_eq!(loaded1.entries[1], (20, 2));

            let loaded2 = leaf_file.load_leaf(1).unwrap();
            assert_eq!(loaded2.entries.len(), 3);
            assert_eq!(loaded2.entries[0], (30, 3));
            assert_eq!(loaded2.total_weight(), 12);
        }
    }

    #[test]
    fn test_leaf_file_many_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_many_leaves.osm");

        // Write many leaves
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();

            for i in 0..100 {
                let leaf = LeafNode {
                    entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                    next_leaf: None,
                };
                leaf_file.write_leaf(i as u64, &leaf).unwrap();
            }

            leaf_file.finalize().unwrap();
        }

        // Read and verify
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();

            assert_eq!(leaf_file.num_leaves(), 100);

            // Spot check some leaves
            for i in [0, 25, 50, 75, 99] {
                let loaded = leaf_file.load_leaf(i as u64).unwrap();
                assert_eq!(loaded.entries[0].0, i * 10);
            }
        }
    }

    #[test]
    fn test_leaf_file_string_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_string_leaves.osm");

        // Write
        {
            let mut leaf_file: LeafFile<LeafNode<String>> = LeafFile::create(&path).unwrap();

            let leaf = LeafNode {
                entries: vec![
                    ("apple".to_string(), 5),
                    ("banana".to_string(), 3),
                    ("cherry".to_string(), 7),
                ],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read
        {
            let mut leaf_file: LeafFile<LeafNode<String>> = LeafFile::open(&path).unwrap();

            let loaded = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded.entries.len(), 3);
            assert_eq!(loaded.entries[0].0, "apple");
            assert_eq!(loaded.entries[1].0, "banana");
            assert_eq!(loaded.entries[2].0, "cherry");
            assert_eq!(loaded.total_weight(), 15);
        }
    }

    #[test]
    fn test_leaf_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_empty.osm");

        // Create and finalize empty file
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Open and verify
        {
            let leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();
            assert_eq!(leaf_file.num_leaves(), 0);
            assert!(leaf_file.index().is_empty());
        }
    }

    #[test]
    fn test_leaf_file_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_not_found.osm");

        // Write one leaf
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: vec![(10, 1)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Try to load non-existent leaf
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();
            let result = leaf_file.load_leaf(999);
            assert!(matches!(
                result,
                Err(FileFormatError::BlockNotFound { leaf_id: 999 })
            ));
        }
    }

    #[test]
    fn test_leaf_file_large_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_large_leaf.osm");

        // Create a large leaf (many entries)
        let mut entries = Vec::new();
        for i in 0..1000 {
            entries.push((i, (i % 10 + 1) as ZWeight));
        }

        // Write
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: entries.clone(),
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read and verify
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();
            let loaded = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded.entries.len(), 1000);
            assert_eq!(loaded.entries, entries);
        }
    }

    #[test]
    fn test_leaf_file_negative_weights() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_negative.osm");

        // Write leaf with negative weights
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: vec![(10, 5), (20, -3), (30, 2)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read and verify
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();
            let loaded = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded.entries[1].1, -3);
            assert_eq!(loaded.total_weight(), 4);
        }
    }

    // =========================================================================
    // NodeStorage + LeafFile Integration Tests
    // =========================================================================

    #[test]
    fn test_flush_dirty_to_disk_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_empty_flush.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Flushing empty storage should succeed with 0 leaves written
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_flush_dirty_to_disk_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_flush_basic.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

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
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_flush_reload.osm");

        let original_entries1 = vec![(100i32, 10i64), (200, 20), (300, 30)];
        let original_entries2 = vec![(400, 40), (500, 50)];

        // Create and flush storage
        {
            let mut storage: OsmNodeStorage<i32> =
                NodeStorage::with_config(NodeStorageConfig::with_spill_directory(1024, dir.path()));

            let leaf1 = LeafNode {
                entries: original_entries1.clone(),
                next_leaf: None,
            };
            let leaf2 = LeafNode {
                entries: original_entries2.clone(),
                next_leaf: None,
            };

            storage.alloc_leaf(leaf1);
            storage.alloc_leaf(leaf2);

            storage.flush_dirty_to_disk(Some(&path)).unwrap();

            // Take ownership of the spill file to prevent auto-cleanup on drop
            let _ = storage.take_spill_file();
        }

        // Read back using LeafFile directly
        {
            let mut leaf_file: LeafFile<LeafNode<i32>> = LeafFile::open(&path).unwrap();

            let loaded1 = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded1.entries, original_entries1);

            let loaded2 = leaf_file.load_leaf(1).unwrap();
            assert_eq!(loaded2.entries, original_entries2);
        }
    }

    #[test]
    fn test_threshold_check_triggers_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_threshold.osm");

        // Very low threshold to trigger easily
        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(
            NodeStorageConfig::with_threshold(100), // 100 bytes threshold
        );

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
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
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
    fn test_cleanup_spill_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cleanup.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        // Flush
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert!(path.exists());

        // Cleanup
        storage.cleanup_spill_file().unwrap();
        assert!(!path.exists());
        assert!(storage.spill_file_path().is_none());
        assert_eq!(storage.spilled_leaf_count(), 0);
    }

    #[test]
    fn test_incremental_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_incremental.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add first leaf
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush first batch
        let count1 = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count1, 1);
        assert_eq!(storage.dirty_leaf_count(), 0);

        // Add second leaf
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf2);

        // Only new leaf is dirty
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Flush second batch (this creates a new file - leaves from first flush are lost)
        // In a real system, we'd append or use a different strategy
        let count2 = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count2, 1);

        // Stats show total leaves written
        assert_eq!(storage.stats().leaves_written, 2);
    }

    #[test]
    fn test_load_leaf_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_load.osm");

        let original_entries = vec![(111i32, 11i64), (222, 22), (333, 33)];

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

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
        let path = dir.path().join("test_evict.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

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
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

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
        let path = dir.path().join("test_evict_dirty.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

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
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

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
        let path = dir.path().join("test_evict_nonspilled.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add first leaf
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush leaf1 to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

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
        let path = dir.path().join("test_reload.osm");

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush and evict
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
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
        let path = dir.path().join("test_reload_bulk.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add multiple leaves
        for i in 0..5 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Flush and evict all
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
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
        let path = dir.path().join("test_evict_stats.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        let memory_before = storage.stats().memory_bytes;
        assert!(memory_before > 0);

        // Flush and evict
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
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
        let path = dir.path().join("test_cache_stats.osm");

        let mut storage: OsmNodeStorage<i32> = NodeStorage::new();

        // Add leaf
        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush and evict
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
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
            max_spillable_level: 0,           // Default: only leaves
            spill_threshold_bytes: 64 * 1024, // 64KB
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache.clone()),
        };

        // Create storage with config
        let storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Verify config is stored
        assert!(storage.config.buffer_cache.is_some());
    }

    #[test]
    fn test_flush_records_block_locations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_block_locs.osm");

        // Create BufferCache
        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

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
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count, 5);

        // Block locations should be recorded
        assert_eq!(storage.leaf_block_locations.len(), 5);

        // Verify each leaf has a block location
        for i in 0..5 {
            assert!(storage.leaf_block_locations.contains_key(&i));
            let &(offset, size) = storage.leaf_block_locations.get(&i).unwrap();
            assert!(offset > 0); // After file header
            assert!(size > 0); // Non-empty
        }

        // FileId should be assigned
        assert!(storage.spill_file_id.is_some());
    }

    #[test]
    fn test_cleanup_clears_block_locations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cleanup_locs.osm");

        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // Add and flush a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Verify locations are recorded
        assert!(!storage.leaf_block_locations.is_empty());
        assert!(storage.spill_file_id.is_some());

        // Cleanup
        storage.cleanup_spill_file().unwrap();

        // All tracking data should be cleared
        assert!(storage.leaf_block_locations.is_empty());
        assert!(storage.spill_file_id.is_none());
    }

    #[test]
    fn test_evict_and_reload_with_buffer_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cache_reload.osm");

        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Evict the leaf
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 1);

        // Verify evicted
        if let NodeLocation::Leaf(leaf_loc) = loc {
            assert!(storage.is_leaf_evicted(leaf_loc));
        }

        // Reload - this will insert into BufferCache for future lookups
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.get_leaf_reloading(leaf_loc);
            assert_eq!(loaded.entries, original_entries);

            // Should be back in memory
            assert!(!storage.is_leaf_evicted(leaf_loc));
        }

        // The leaf data was loaded from disk and should have been
        // inserted into the BufferCache (even though we can't verify
        // the cache directly, we can verify it didn't error)
        assert!(storage.leaf_block_locations.contains_key(&0));
    }

    #[test]
    fn test_multiple_flushes_update_block_locations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_multi_flush.osm");

        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);

        // First batch of leaves
        for i in 0..3 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // First flush
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(storage.leaf_block_locations.len(), 3);

        // Add more leaves
        for i in 3..6 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Second flush - existing leaves are skipped (not dirty), new ones written
        // Note: This actually appends to the same file
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(storage.leaf_block_locations.len(), 6);

        // All leaves should have locations
        for i in 0..6 {
            assert!(storage.leaf_block_locations.contains_key(&i));
        }
    }
}
