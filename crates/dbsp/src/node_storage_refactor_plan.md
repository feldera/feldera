# NodeStorage Refactor Plan: Align with Storage Backend Patterns

## Executive Summary

NodeStorage currently uses direct `std::fs` operations with a single mutable spill file. This approach is incompatible with the DBSP storage backend patterns used by Spine, which assumes **immutable files after completion**. This document proposes a segment-based multi-file approach that aligns with established patterns.

---

## Part 1: Single-File vs Multi-File Assessment

### Current NodeStorage Approach

```
┌─────────────────────────────────────────────┐
│           Single Spill File                 │
│  ┌────────┬────────┬────────┬─────────────┐ │
│  │ Leaf 0 │ Leaf 1 │ Leaf 2 │ ... Index   │ │
│  └────────┴────────┴────────┴─────────────┘ │
│  (Appended incrementally, finalized once)   │
└─────────────────────────────────────────────┘
```

**Problems with single mutable file:**

1. **Incompatible with StorageBackend semantics**:
   - `FileWriter::complete()` makes files immutable (see `posixio_impl.rs:262`)
   - Cannot append to a completed file
   - Object storage (S3, GCS) doesn't support append at all

2. **Checkpoint hazards**:
   - What if checkpoint occurs mid-flush?
   - Incomplete file cannot be safely referenced

3. **No concurrent access**:
   - Single file being written prevents concurrent reads
   - Spine batches are independent files for this reason

### Spine's Multi-File Approach

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Batch 1    │  │   Batch 2    │  │   Batch 3    │
│  (immutable) │  │  (immutable) │  │  (immutable) │
└──────────────┘  └──────────────┘  └──────────────┘
        │                 │                 │
        └────────────────────────────────────
                         │
              Checkpoint references all
```

**Why it works:**
- Each batch file is written once, then completed
- Files become read-only immediately
- Checkpoint simply references existing files
- Works with both POSIX and object storage

### Recommended: Segment-Based Multi-File Approach

```
┌─────────────────────────────────────────────────────────┐
│                    NodeStorage                          │
│                                                         │
│  In-Memory:     [Leaf 0] [Leaf 1] [Leaf 2] ... [Leaf N]│
│                     │        │        │                 │
│  Segments:     ┌────┴────┐ ┌─┴────────┴─┐               │
│                │ Seg 0   │ │   Seg 1    │  (immutable)  │
│                │ L0, L1  │ │  L2, L3    │               │
│                └─────────┘ └────────────┘               │
│                                                         │
│  Index:        { leaf_id → (segment_id, block_loc) }    │
└─────────────────────────────────────────────────────────┘
```

**Why segment-based is appropriate for NodeStorage:**

| Consideration | Per-Leaf Files | Single File | Segment Files |
|--------------|----------------|-------------|---------------|
| File overhead | High (header per leaf) | Low | Medium |
| Object storage costs | High (request per leaf) | N/A (incompatible) | Low |
| Checkpoint complexity | High (many file refs) | Low | Medium |
| StorageBackend compatible | Yes | **No** | Yes |
| Memory during flush | Low | Medium | Medium |
| Leaf size (KB range) | Inefficient | - | Efficient |

**Segment sizing strategy:**
- Target segment size: 8-64 MB (configurable)
- Each flush creates one new segment
- Segments are immutable once written
- Similar to LSM-tree SSTable approach

---

## Part 2: Detailed Refactoring Plan

### Phase 1: Type System Alignment

**Goal:** Replace std library types with DBSP storage types.

**Changes:**

1. **Replace `PathBuf` with `StoragePath`** throughout:
   ```rust
   // Before
   spill_file_path: Option<std::path::PathBuf>,

   // After
   use feldera_storage::StoragePath;
   segment_paths: Vec<StoragePath>,
   ```

2. **Replace `Vec<u8>` buffers with `FBuf`**:
   ```rust
   // Before
   let mut block = vec![0u8; block_size];

   // After
   use crate::storage::buffer_cache::FBuf;
   let mut block = FBuf::with_capacity(block_size);
   ```

3. **Update imports**:
   ```rust
   use feldera_storage::{StoragePath, FileWriter, FileReader, FileCommitter};
   use crate::storage::backend::{StorageBackend, BlockLocation};
   use crate::storage::buffer_cache::FBuf;
   ```

### Phase 2: Segment-Based Storage Structure

**Goal:** Replace single spill file with immutable segment files.

**New data structures:**

```rust
/// Identifies a segment file
#[derive(Clone, Debug)]
pub struct SegmentId(u64);

/// Location of a leaf within segment storage
#[derive(Clone, Debug)]
pub struct LeafDiskLocation {
    segment_id: SegmentId,
    block_location: BlockLocation,
}

/// Metadata for a completed segment
#[derive(Clone, Debug)]
pub struct SegmentMetadata {
    id: SegmentId,
    path: StoragePath,
    reader: Arc<dyn FileReader>,
    /// Leaves in this segment: leaf_id -> block_location
    leaf_index: HashMap<usize, BlockLocation>,
    /// Total bytes in segment
    size_bytes: u64,
}

/// Updated NodeStorage fields
pub struct NodeStorage<I, L> {
    // ... existing fields ...

    // Replace single spill file with segments
    /// Completed, immutable segments
    segments: Vec<SegmentMetadata>,

    /// Next segment ID to assign
    next_segment_id: u64,

    /// Maps leaf_id -> segment location
    leaf_locations: HashMap<usize, LeafDiskLocation>,

    /// Pending segment being built (not yet complete)
    /// None if no flush in progress
    pending_segment: Option<PendingSegment>,
}

/// A segment file being written (not yet complete)
struct PendingSegment {
    id: SegmentId,
    writer: Box<dyn FileWriter>,
    leaves_written: Vec<(usize, BlockLocation)>,
    bytes_written: u64,
}
```

### Phase 3: Storage Backend Integration

**Goal:** Use `Runtime::storage_backend()` for all I/O.

**Key changes to flush operation:**

```rust
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps + Archive + RkyvSerialize<Serializer>,
    // ... bounds ...
{
    /// Flush dirty leaves to a new segment file.
    pub fn flush_dirty_to_disk(&mut self) -> Result<usize, Error> {
        if self.dirty_leaves.is_empty() {
            return Ok(0);
        }

        // Get storage backend from Runtime
        let backend = Runtime::storage_backend()?;

        // Create new segment file
        let segment_id = SegmentId(self.next_segment_id);
        self.next_segment_id += 1;

        let segment_path = self.segment_path(&segment_id);
        let mut writer = backend.create_named(&segment_path)?;

        // Write header block
        let header = SegmentHeader::new(segment_id);
        let header_buf = serialize_to_fbuf(&header)?;
        writer.write_block(header_buf)?;

        // Write each dirty leaf as a block
        let mut leaves_written = Vec::new();
        let dirty_ids: Vec<usize> = self.dirty_leaves.iter().copied().collect();

        for leaf_id in &dirty_ids {
            let leaf = self.leaves[*leaf_id].as_present()
                .expect("dirty leaf must be in memory");

            let block = self.serialize_leaf_block(*leaf_id, leaf)?;
            let block_len = block.len();
            writer.write_block(block)?;

            let location = BlockLocation::new(current_offset, block_len)?;
            leaves_written.push((*leaf_id, location));
            current_offset += block_len as u64;
        }

        // Write index block and trailer
        let index_block = self.build_index_block(&leaves_written)?;
        writer.write_block(index_block)?;

        // Complete the file (makes it immutable)
        let reader = writer.complete()?;
        reader.mark_for_checkpoint();

        // Update tracking
        let segment_meta = SegmentMetadata {
            id: segment_id,
            path: segment_path,
            reader,
            leaf_index: leaves_written.iter().cloned().collect(),
            size_bytes: current_offset,
        };

        for (leaf_id, block_loc) in &leaves_written {
            self.leaf_locations.insert(*leaf_id, LeafDiskLocation {
                segment_id,
                block_location: *block_loc,
            });
            self.spilled_leaves.insert(*leaf_id);
        }

        self.segments.push(segment_meta);
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;

        Ok(dirty_ids.len())
    }
}
```

**Key changes to load operation:**

```rust
impl<I, L> NodeStorage<I, L> {
    /// Load a leaf from disk.
    pub fn load_leaf_from_disk(&mut self, loc: LeafLocation) -> Result<&L, Error> {
        let leaf_id = loc.id;

        // Check memory first
        if self.leaves[leaf_id].is_present() {
            self.stats.cache_hits += 1;
            return Ok(self.leaves[leaf_id].as_present().unwrap());
        }

        // Find the segment containing this leaf
        let disk_loc = self.leaf_locations.get(&leaf_id)
            .ok_or_else(|| Error::LeafNotFound(leaf_id))?;

        let segment = self.segments.iter()
            .find(|s| s.id == disk_loc.segment_id)
            .ok_or_else(|| Error::SegmentNotFound(disk_loc.segment_id))?;

        // Read from segment file via FileReader
        let block_data = segment.reader.read_block(disk_loc.block_location)?;

        // Deserialize leaf
        let leaf = self.deserialize_leaf_block(&block_data)?;
        let leaf_size = leaf.estimate_size();

        // Store in memory
        self.leaves[leaf_id] = LeafSlot::Present(leaf);
        self.stats.cache_misses += 1;
        self.stats.memory_bytes += leaf_size;

        Ok(self.leaves[leaf_id].as_present().unwrap())
    }
}
```

### Phase 4: Checkpoint Integration

**Goal:** Integrate with DBSP checkpoint lifecycle.

**Checkpoint metadata structure:**

```rust
/// Committed state for checkpoint
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
pub struct CommittedNodeStorage<K> {
    /// Paths to all segment files (relative to checkpoint base)
    pub segment_paths: Vec<String>,

    /// Per-segment metadata for restore
    pub segment_metadata: Vec<CommittedSegment>,

    /// Leaf summaries for O(num_leaves) restore
    pub leaf_summaries: Vec<LeafSummary<K>>,

    /// Global stats for validation
    pub total_entries: usize,
    pub total_weight: ZWeight,
    pub num_leaves: usize,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
pub struct CommittedSegment {
    /// Segment ID
    pub id: u64,
    /// Leaves in this segment: (leaf_id, offset, size)
    pub leaf_blocks: Vec<(usize, u64, u32)>,
}
```

**Save implementation:**

```rust
impl<I, L> NodeStorage<I, L> {
    pub fn save(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        // 1. Flush any remaining dirty leaves
        if !self.dirty_leaves.is_empty() {
            self.flush_dirty_to_disk()?;
        }

        // 2. Collect file references for checkpoint
        for segment in &self.segments {
            files.push(segment.reader.clone());
        }

        // 3. Build checkpoint metadata
        let committed = CommittedNodeStorage {
            segment_paths: self.segments.iter()
                .map(|s| s.path.to_string())
                .collect(),
            segment_metadata: self.segments.iter()
                .map(|s| CommittedSegment {
                    id: s.id.0,
                    leaf_blocks: s.leaf_index.iter()
                        .map(|(id, loc)| (*id, loc.offset, loc.size as u32))
                        .collect(),
                })
                .collect(),
            leaf_summaries: self.build_leaf_summaries(),
            total_entries: self.stats.total_entries,
            total_weight: self.compute_total_weight(),
            num_leaves: self.leaves.len(),
        };

        // 4. Write checkpoint metadata file
        let backend = Runtime::storage_backend()?;
        let checkpoint_path = base.child(format!("node-storage-{}.dat", persistent_id));
        let content = to_bytes(&committed)?;
        let writer = backend.write(&checkpoint_path, content)?;
        files.push(writer);

        Ok(())
    }
}
```

**Restore implementation:**

```rust
impl<I, L> NodeStorage<I, L> {
    pub fn restore(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
    ) -> Result<(), Error> {
        let backend = Runtime::storage_backend()?;

        // 1. Read checkpoint metadata
        let checkpoint_path = base.child(format!("node-storage-{}.dat", persistent_id));
        let content = backend.read(&checkpoint_path)?;
        let committed: CommittedNodeStorage<L::Key> =
            deserialize_from_bytes(&content)?;

        // 2. Open all segment files
        self.segments.clear();
        for (path_str, seg_meta) in committed.segment_paths.iter()
            .zip(committed.segment_metadata.iter())
        {
            let path: StoragePath = path_str.clone().into();
            let reader = backend.open(&path)?;

            self.segments.push(SegmentMetadata {
                id: SegmentId(seg_meta.id),
                path,
                reader,
                leaf_index: seg_meta.leaf_blocks.iter()
                    .map(|(id, off, sz)| (*id, BlockLocation::new(*off, *sz as usize).unwrap()))
                    .collect(),
                size_bytes: 0, // Not critical for restore
            });
        }

        // 3. Rebuild leaf location index
        self.leaf_locations.clear();
        for segment in &self.segments {
            for (leaf_id, block_loc) in &segment.leaf_index {
                self.leaf_locations.insert(*leaf_id, LeafDiskLocation {
                    segment_id: segment.id,
                    block_location: *block_loc,
                });
            }
        }

        // 4. Mark all leaves as evicted (lazy load)
        self.leaves.clear();
        for summary in committed.leaf_summaries {
            self.leaves.push(LeafSlot::Evicted(CachedLeafSummary::from(summary)));
            self.spilled_leaves.insert(self.leaves.len() - 1);
        }

        self.stats.leaf_node_count = committed.num_leaves;
        self.stats.evicted_leaf_count = committed.num_leaves;
        self.stats.total_entries = committed.total_entries;

        Ok(())
    }
}
```

### Phase 5: Remove LeafFile, Update Config

**Goal:** Remove the custom `LeafFile` struct and simplify configuration.

**Changes:**

1. **Delete `LeafFile<L>` struct** entirely - it duplicates StorageBackend functionality

2. **Simplify `NodeStorageConfig`**:
   ```rust
   pub struct NodeStorageConfig {
       /// Whether to enable disk spilling
       pub enable_spill: bool,

       /// Maximum node level that can be spilled (0 = leaves only)
       pub max_spillable_level: u8,

       /// Target segment size in bytes (default: 64MB)
       pub target_segment_size: usize,

       /// Threshold for triggering flush (bytes of dirty data)
       pub spill_threshold_bytes: usize,
   }

   impl NodeStorageConfig {
       pub fn from_runtime() -> Self {
           // Storage backend is obtained from Runtime when needed,
           // not stored in config
           Self {
               enable_spill: Runtime::storage_backend().is_ok(),
               max_spillable_level: 0,
               target_segment_size: 64 * 1024 * 1024,
               spill_threshold_bytes: Runtime::min_index_storage_bytes()
                   .unwrap_or(64 * 1024 * 1024),
           }
       }
   }
   ```

3. **Remove from config**:
   - `spill_directory: Option<PathBuf>` - use StorageBackend's path
   - `storage_backend: Option<Arc<dyn StorageBackend>>` - get from Runtime
   - `buffer_cache: Option<Arc<BufferCache>>` - get from Runtime

### Phase 6: Segment Compaction (Optional Enhancement)

**Goal:** Merge small segments to reduce file count.

This is an optional optimization that can be implemented later:

```rust
impl<I, L> NodeStorage<I, L> {
    /// Merge multiple small segments into one larger segment.
    pub fn compact_segments(&mut self) -> Result<(), Error> {
        // Find segments below threshold
        let small_segments: Vec<_> = self.segments.iter()
            .filter(|s| s.size_bytes < self.config.target_segment_size / 4)
            .map(|s| s.id)
            .collect();

        if small_segments.len() < 2 {
            return Ok(());
        }

        // Create new merged segment
        // ... similar to flush_dirty_to_disk but reading from existing segments

        // Delete old segments after successful merge
        // ...
    }
}
```

---

## Part 3: Migration Steps

### Step 1: Add New Types (Non-Breaking)
- Add `SegmentId`, `LeafDiskLocation`, `SegmentMetadata` types
- Add new fields to `NodeStorage` with defaults
- Keep old fields for now

### Step 2: Implement Segment-Based Flush
- Implement new `flush_dirty_to_segments()` method
- Keep old `flush_dirty_to_disk()` as deprecated
- Add feature flag to switch between old/new

### Step 3: Implement Segment-Based Load
- Update `load_leaf_from_disk()` to use segments
- Handle both old single-file and new segment formats

### Step 4: Implement Save/Restore
- Add `save()` and `restore()` methods following Trace pattern
- Integrate with checkpoint lifecycle

### Step 5: Remove Old Code
- Remove `LeafFile<L>` struct
- Remove old single-file spill code
- Remove deprecated methods
- Remove `order_statistics_file_format.rs` (no longer needed)

### Step 6: Update Tests
- Update all tests to use new segment-based approach
- Add tests for checkpoint/restore
- Add tests for multi-segment scenarios

---

## Part 4: File Changes Summary

| File | Action |
|------|--------|
| `node_storage.rs` | Major refactor - segment-based storage |
| `order_statistics_file_format.rs` | Delete (no longer needed) |
| `order_statistics_multiset.rs` | Update to use new NodeStorage API |
| `node_storage_tests.rs` | Update tests for new API |

---

## Part 5: Risk Assessment

| Risk | Mitigation |
|------|------------|
| Breaking existing checkpoints | Version checkpoint format, support reading old format |
| Performance regression | Benchmark before/after, tune segment size |
| Complexity increase | Clear documentation, comprehensive tests |
| Object storage latency | Segment files amortize overhead over many leaves |

---

## Appendix: Key StorageBackend Patterns to Follow

From `spine_async.rs`:

```rust
// Getting backend
let backend = Runtime::storage_backend().unwrap();

// Writing checkpoint data
let as_bytes = to_bytes(&committed).expect("...");
backend.write(&Self::checkpoint_file(base, persistent_id), as_bytes)?;

// Reading checkpoint data
let content = Runtime::storage_backend().unwrap().read(&pspine_path)?;

// File references for checkpoint
files.push(batch.file_reader().expect("..."));

// Loading from path
B::from_path(&self.factories.clone(), &batch.clone().into())
```

From `writer.rs`:

```rust
// Creating file
let mut writer = backend.create_named(&path)?;

// Writing blocks
writer.write_block(data)?;

// Completing file (makes immutable)
let reader = writer.complete()?;
reader.commit()?;

// Mark for checkpoint
reader.mark_for_checkpoint();
```
