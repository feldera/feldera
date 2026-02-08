# PercentileOperator Checkpoint/Restore Implementation Plan

## Current State

The `PercentileOperator` maintains per-key `OrderStatisticsZSet` trees that can hold
**billions of entries**. Each tree uses `NodeStorage` which already supports spilling leaf
nodes to disk. Checkpoint/restore is not yet implemented.

## Scope

**This plan is designed for `NodeStorage` with B+ tree structures where domain data
resides in leaf nodes.** This matches `OrderStatisticsZSet`, where all `(value, weight)`
pairs are stored in leaves.

The core principle is: **checkpoint only leaves; reconstruct internal nodes on restore.**

This approach:
- **Keeps checkpoint format independent of `max_spillable_level`** - critical because
  this setting could change between checkpoint and restore (recompilation, different config)
- **Rebuilds internal nodes from leaf summaries** in O(num_leaves) without reading leaf contents
- **Reuses checkpoint file as spill file** - zero re-serialization, zero bulk reads
- **Simplifies checkpoint format** - only one node type to handle

## Goals

1. Fault-tolerant state persistence for `PercentileOperator`
2. Efficient checkpoint size leveraging existing spill files
3. Fast checkpoint/restore for large trees (billions of entries)

## Scale Considerations

**Tree scale:**
- Can hold billions of entries per key
- Multiple keys = potentially trillions of total entries
- Direct serialization would create TB-sized checkpoint files

**NodeStorage architecture (from spine_async.md insight):**
- Internal nodes: <2% of nodes, accessed 100% of time → always in memory
- Leaf nodes: >98% of nodes, accessed sparsely → can spill to disk

**Key insight:** Leaf data may already be on disk in spill files. Re-serializing it
for checkpoint would be wasteful. We should reference existing files like Spine does.

## Data Structure Assumptions

### OrderStatisticsZSet is a B+ Tree

```rust
// Internal nodes: navigation + derived aggregates
struct InternalNodeTyped<T> {
    keys: Vec<T>,                // Separator keys (copies from leaves)
    children: Vec<NodeLocation>, // Child pointers
    subtree_sums: Vec<ZWeight>,  // Derived: sum of weights in each subtree
}

// Leaf nodes: actual data
struct LeafNode<T> {
    entries: Vec<(T, ZWeight)>,  // THE DATA: (value, weight) pairs
    next_leaf: Option<LeafLocation>,
}
```

**Critical property:** All actual data lives in leaf nodes. Internal nodes contain:
- Separator keys: copies of values that also exist in leaves
- Subtree sums: derived/aggregated from leaf data (can be recomputed)

This B+ tree property is why "checkpoint only leaves" works efficiently:
- **Leaves contain all data** → checkpoint only the leaf file
- **Internal nodes are derived** → reconstruct from leaf summaries on restore (O(num_leaves))

### Why Checkpoint Only Leaves?

1. **Config Independence**: `max_spillable_level` could change between checkpoint and restore
   (recompilation, different settings). If we checkpoint internal nodes, the format would
   depend on which nodes were spilled vs in-memory at checkpoint time.

2. **Simplicity**: One node type to serialize (leaves), one reconstruction path on restore.

3. **Efficiency**: With leaf summaries (first_key, weight_sum per leaf), we can rebuild
   internal nodes in O(num_leaves) without reading leaf contents. For 1B entries with
   ~1M leaves, this is ~1000x faster than reading all entries.

### What About max_spillable_level?

NodeStorage supports different spill configurations at runtime:

```rust
/// max_spillable_level controls which nodes can be spilled:
/// - 0: Only leaves can be spilled (default, most efficient)
/// - 1: Leaves and level-1 internal nodes can be spilled
/// - u8::MAX: All nodes can be spilled
```

**The checkpoint approach is independent of `max_spillable_level`:**

| Checkpoint | Always stores | Always reconstructs |
|------------|--------------|---------------------|
| Leaves | Leaf file + index + summaries | Internal nodes from summaries (O(num_leaves)) |

This means:
- Checkpoint at `max_spillable_level = 0` → restore at `max_spillable_level = 2` ✓
- Checkpoint at `max_spillable_level = 2` → restore at `max_spillable_level = 0` ✓

The checkpoint format doesn't change regardless of runtime settings

## Approach: Reference-Based Checkpointing

Follow the Spine pattern: checkpoint stores **references to persisted data files**,
not the data itself.

### How Spine Does It

```rust
// CommittedSpine stores file paths, NOT data
struct CommittedSpine {
    batches: Vec<String>,  // Paths to batch files (data already on disk)
    dirty: bool,
}

fn save(&mut self, ...) {
    // 1. Persist batches to files (if not already)
    let batches = persist_batches(self.batches);

    // 2. Collect file paths
    let ids: Vec<String> = batches.iter()
        .map(|b| b.file_reader().path().to_string())
        .collect();

    // 3. Write small metadata file
    let committed = CommittedSpine { batches: ids, dirty: self.dirty };
    backend.write(&checkpoint_file, to_bytes(&committed)?)?;
}

fn restore(&mut self, ...) {
    let committed: CommittedSpine = deserialize(&content)?;
    for batch_path in committed.batches {
        let batch = B::from_path(&batch_path)?;  // Read from existing file
        self.insert(batch);
    }
}
```

### How PercentileOperator Should Do It

```rust
// Per-tree checkpoint metadata (leaves only - internal nodes reconstructed on restore)
struct CommittedLeafStorage<K> {
    /// Path to the spill file containing all leaf data
    spill_file_path: String,

    /// Index into spill file: leaf_id -> (offset, size)
    leaf_block_locations: Vec<(usize, u64, u32)>,

    /// Per-leaf summaries for O(num_leaves) index reconstruction
    /// Allows rebuilding internal nodes WITHOUT reading leaf contents
    leaf_summaries: Vec<LeafSummary<K>>,

    /// Total entries and weight (for validation)
    total_entries: usize,
    total_weight: ZWeight,
}

/// Summary of a leaf node - enough to rebuild internal nodes without reading leaf contents
struct LeafSummary<K> {
    first_key: K,         // For building separator keys in internal nodes
    weight_sum: ZWeight,  // For building subtree_sums in internal nodes
}

// Operator checkpoint
struct CommittedPercentile<K> {
    /// Per-key tree metadata (references leaf files only)
    trees: Vec<(K, CommittedLeafStorage<K>)>,

    /// Previous outputs for delta computation
    prev_output: Vec<(K, Option<V>)>,
}
```

**Key optimizations:**
1. Internal nodes are NOT stored - rebuilt from `leaf_summaries` in O(num_leaves)
2. Leaf contents are NOT read on restore - checkpoint file used directly as spill file
3. Already-spilled leaves are NOT re-serialized on checkpoint - just move the file

### Checkpoint Size Comparison

| Approach | Checkpoint Contains | Size for 1B entries (~1M leaves) |
|----------|---------------------|----------------------------------|
| Direct serialization | All entries | ~50+ GB |
| Reference-based (this plan) | Leaf file + index + summaries | ~32 MB |

## Implementation Details

### Changes to NodeStorage

Currently, `NodeStorage::drop()` deletes spill files:

```rust
impl<I, L> Drop for NodeStorage<I, L> {
    fn drop(&mut self) {
        if let Some(ref path) = self.spill_file_path {
            std::fs::remove_file(path)?;  // Deletes on drop!
        }
    }
}
```

**Need to add:**

1. **`save_leaves()` method** - Checkpoint leaves to file:
   ```rust
   impl<I, L> NodeStorage<I, L> {
       /// Checkpoint the leaf data, returning metadata that references the spill file.
       ///
       /// This:
       /// 1. Writes only dirty + never-spilled leaves (skips already-spilled clean!)
       /// 2. Moves the spill file to checkpoint location (O(1) rename)
       /// 3. Collects leaf summaries for O(num_leaves) restore
       /// 4. Returns metadata (checkpoint file becomes spill file on restore)
       ///
       /// Already-spilled clean leaves are NOT re-serialized.
       /// Internal nodes are NOT checkpointed - rebuilt from summaries on restore.
       pub fn save_leaves(
           &mut self,
           checkpoint_dir: &StoragePath,
           tree_id: &str,
       ) -> Result<CommittedLeafStorage<L::Key>, Error> {
           // Write leaves not yet on disk (dirty + never-spilled)
           // Already-spilled clean leaves: SKIPPED - no re-serialization!
           self.flush_all_leaves_to_disk()?;

           // Collect leaf summaries BEFORE moving file
           // (need access to leaf data for first_key, weight_sum)
           let leaf_summaries = self.collect_leaf_summaries();

           // Move spill file to checkpoint location - O(1) filesystem rename
           let dest_path = checkpoint_dir.child(format!("{}.leaves", tree_id));
           if let Some(ref src_path) = self.spill_file_path {
               std::fs::rename(src_path, &dest_path)?;
           }

           // Build metadata with summaries for fast restore
           let committed = CommittedLeafStorage {
               spill_file_path: dest_path.to_string(),
               leaf_block_locations: self.leaf_block_locations.clone(),
               leaf_summaries,  // Enables O(num_leaves) restore
               total_entries: self.total_entries(),
               total_weight: self.total_weight(),
           };

           // Clear our reference - file now owned by checkpoint
           // On restore, this same file becomes the new spill file
           self.spill_file_path = None;

           Ok(committed)
       }
   }
   ```

2. **Optimized restore** (uses checkpoint file directly, no bulk reading):
   ```rust
   impl<T: DBData> OrderStatisticsZSet<T> {
       /// Restore from checkpoint - O(num_leaves), not O(num_entries).
       ///
       /// This:
       /// 1. Points NodeStorage at the checkpoint file (no copy!)
       /// 2. Rebuilds internal nodes from leaf_summaries (no leaf I/O!)
       /// 3. Marks all leaves as evicted (lazy-loaded on demand)
       ///
       /// The checkpoint file becomes the live spill file.
       pub fn restore(committed: CommittedLeafStorage<T>) -> Result<Self, Error> {
           let mut storage = NodeStorage::new();

           // Point at checkpoint file - it becomes our spill file
           storage.spill_file_path = Some(committed.spill_file_path.into());
           storage.leaf_block_locations = committed.leaf_block_locations
               .into_iter()
               .map(|(id, off, sz)| (id, (off, sz)))
               .collect();

           // All leaves start evicted - loaded on demand from checkpoint file
           let num_leaves = committed.leaf_summaries.len();
           storage.mark_all_leaves_evicted(num_leaves);

           // Build internal nodes from summaries - O(num_leaves), no leaf I/O
           storage.internal_nodes = Self::build_internal_nodes_from_summaries(
               &committed.leaf_summaries
           );

           Ok(Self {
               storage,
               total_weight: committed.total_weight,
               // ... other fields derived from summaries
           })
       }

       /// Build internal node tree from leaf summaries.
       /// Uses first_key for separators, weight_sum for subtree_sums.
       fn build_internal_nodes_from_summaries(
           summaries: &[LeafSummary<T>]
       ) -> Vec<InternalNode<T>> {
           // Bottom-up construction:
           // 1. Group leaves into parent nodes using first_key as separators
           // 2. Aggregate weight_sum for subtree_sums
           // 3. Recursively build higher levels
           // O(num_leaves) total
           todo!()
       }
   }
   ```

3. **Ownership transfer** - Don't delete files owned by checkpointer:
   ```rust
   impl<I, L> NodeStorage<I, L> {
       /// Take ownership of the spill file path.
       /// After calling this, Drop will not delete the file.
       pub fn take_spill_file(&mut self) -> Option<PathBuf> {
           self.spill_file_path.take()
       }
   }
   ```

### Changes to PercentileOperator

```rust
impl<K, V, Mode> Operator for PercentileOperator<K, V, Mode> {
    fn checkpoint(
        &mut self,
        base: &StoragePath,
        pid: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;
        let checkpoint_dir = base.child(pid);

        // Save each tree's leaves (internal nodes are NOT saved)
        let mut trees = Vec::new();
        for (key, tree) in &mut self.trees {
            let tree_id = format!("tree_{}", trees.len());
            let committed = tree.storage_mut().save_leaves(&checkpoint_dir, &tree_id)?;
            trees.push((key.clone(), committed));
        }

        // Save prev_output
        let prev_output: Vec<_> = self.prev_output.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Write metadata file (small - just leaf index + prev_output)
        let committed = CommittedPercentile { trees, prev_output };
        let as_bytes = to_bytes(&committed)?;
        files.push(
            Runtime::storage_backend()
                .unwrap()
                .write(&checkpoint_dir.child("metadata.dat"), as_bytes)?,
        );

        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
        let pid = require_persistent_id(pid, &self.global_id)?;
        let checkpoint_dir = base.child(pid);

        // Read metadata
        let content = Runtime::storage_backend()
            .unwrap()
            .read(&checkpoint_dir.child("metadata.dat"))?;
        let committed: CommittedPercentile<K> = deserialize(&content)?;

        // Restore trees by reading leaves and reconstructing internal nodes
        self.trees.clear();
        for (key, leaf_storage_meta) in committed.trees {
            // This reads leaves and rebuilds the entire tree via from_sorted_entries()
            let tree = OrderStatisticsZSet::restore(leaf_storage_meta)?;
            self.trees.insert(key, tree);
        }

        // Restore prev_output
        self.prev_output = committed.prev_output.into_iter().collect();

        Ok(())
    }
}
```

### Changes to OrderStatisticsZSet

Need methods for checkpoint/restore:

```rust
impl<T: DBData> OrderStatisticsZSet<T> {
    /// Get mutable access to the underlying NodeStorage (for checkpoint).
    pub fn storage_mut(&mut self) -> &mut OsmNodeStorage<T> {
        &mut self.storage
    }

    /// Collect leaf summaries for checkpoint metadata.
    /// Called after flush_all_leaves_to_disk().
    pub fn collect_leaf_summaries(&self) -> Vec<LeafSummary<T>> {
        self.storage.leaves.iter().map(|leaf| {
            LeafSummary {
                first_key: leaf.entries.first().map(|(k, _)| k.clone()),
                weight_sum: leaf.entries.iter().map(|(_, w)| *w).sum(),
            }
        }).collect()
    }

    /// Restore from checkpoint - O(num_leaves), not O(num_entries).
    ///
    /// Key insight: We rebuild internal nodes from leaf_summaries,
    /// NOT by reading leaf contents. The checkpoint file becomes
    /// the live spill file with lazy-loaded leaves.
    pub fn restore(committed: CommittedLeafStorage<T>) -> Result<Self, Error> {
        // See implementation in "Optimized restore" section above
        // ...
    }

    /// Build internal nodes from leaf summaries.
    /// O(num_leaves) - no leaf content I/O required.
    fn build_internal_nodes_from_summaries(
        summaries: &[LeafSummary<T>]
    ) -> Vec<InternalNode<T>> {
        // Bottom-up: group leaves, use first_key for separators,
        // aggregate weight_sum for subtree_sums
        // ...
    }

    /// Construct from restored NodeStorage.
    pub fn from_storage(storage: OsmNodeStorage<T>, total_weight: ZWeight) -> Self {
        Self {
            storage,
            total_weight,
            // root derived from storage.internal_nodes
            // ...
        }
    }
}
```

## File Layout

```
<checkpoint_base>/<persistent_id>/
├── metadata.dat              # Small: leaf indices + prev_output (NO internal nodes)
├── tree_0.leaves             # Leaf data for key 0
├── tree_1.leaves             # Leaf data for key 1
├── tree_2.leaves             # ...
└── ...
```

**metadata.dat contains:**
- Leaf block locations (index into .leaves files)
- Leaf summaries (first_key, weight_sum per leaf) for O(num_leaves) index reconstruction
- Total entries and weights per tree (for validation)
- prev_output map
- **Note: NO internal nodes** - they're reconstructed from summaries on restore

**.leaves files contain:**
- Raw leaf node data: `Vec<(T, ZWeight)>` entries
- Same format as current spill files
- Sorted order is preserved (leaves form a linked list in B+ tree)

**Checkpoint metadata size for 1B entries (~1M leaves):**
- ~1M leaves × 16 bytes per leaf location ≈ 16 MB
- ~1M leaves × 16 bytes per leaf summary (8-byte key + 8-byte weight) ≈ 16 MB
- Plus prev_output entries (small)
- Total: ~32 MB (vs ~50+ GB for direct serialization)

## Checkpoint Flow

```
checkpoint():
  for each tree:
    1. flush_all_leaves_to_disk()      # Write dirty/unspilled leaves
    2. Copy spill file to checkpoint storage (e.g., S3)
    3. Collect leaf metadata:
       - leaf_block_locations (already tracked by NodeStorage)
       - leaf_summaries (first_key, weight_sum per leaf)

  Write metadata.dat (small file with leaf indices + summaries + prev_output)

  # Runtime resumes immediately with its local spill file
```

### Runtime Resume After Checkpoint

The pipeline stops briefly during checkpoint, then **resumes immediately**:

1. **Independent copies**: Checkpoint storage receives a copy; runtime keeps its local spill file
2. **In-place overwrites**: Runtime modifies leaves by overwriting blocks at existing offsets
3. **No shared state**: Checkpoint and runtime are completely independent

This design uses full-file copy because the primary checkpoint storage (S3) doesn't support
partial updates. The copy happens from local disk to checkpoint storage, so runtime can
continue with its local file unchanged.

### Leaf Categories at Checkpoint Time

Leaves fall into three categories:

| Category | Location | Action on Checkpoint |
|----------|----------|---------------------|
| Already spilled (clean) | On disk, unchanged | **No write** - already in spill file |
| Dirty | Modified since flush | **Must write** - overwrite in spill file |
| Never spilled | In memory only | **Must write** - write to spill file |

**Flush cost = O(dirty + never-spilled leaves)**
**Copy cost = O(total file size)** - full file copied to checkpoint storage

### Edge Case: Small Trees (No Spill File Yet)

If a tree is small and hasn't triggered spilling:
- No spill file exists yet
- All leaves are in memory (category: "never spilled")

**Solution:** `flush_all_leaves_to_disk()` handles this:
```rust
pub fn flush_all_leaves_to_disk(&mut self) -> Result<(), Error> {
    // Create spill file if none exists
    if self.spill_file_path.is_none() {
        self.create_spill_file()?;
    }

    // Write leaves that aren't already on disk
    for leaf_id in 0..self.leaves.len() {
        if !self.is_leaf_on_disk(leaf_id) {
            self.write_leaf_to_disk(leaf_id)?;
        }
        // Already-spilled clean leaves: skip (no re-serialization!)
    }

    // Collect leaf summaries for index reconstruction on restore
    self.collect_leaf_summaries();

    Ok(())
}
```

This ensures:
- Small trees work (creates file if needed)
- Large trees are efficient (skips already-spilled leaves)
- No re-serialization of data already on disk

## Restore Flow

```
restore():
  Read metadata.dat

  for each tree:
    1. Point NodeStorage at the checkpoint .leaves file (don't read contents!)
    2. Restore leaf_block_locations from checkpoint
    3. Mark all leaves as "evicted" (on disk, lazy-loaded on demand)
    4. Rebuild internal nodes from leaf_summaries:
       - Separator keys from first_key of each leaf
       - Subtree sums aggregated from weight_sum of each leaf
       - O(num_leaves) time, NOT O(num_entries)

  Result: Fully reconstructed tree, checkpoint file IS the spill file
```

### Restore is O(num_leaves), Not O(num_entries)

The key optimization: we rebuild internal nodes from `leaf_summaries` without
reading leaf contents. For 1B entries with ~1M leaves, this is ~1000x faster.

```rust
fn restore_optimized(committed: CommittedLeafStorage<K>) -> OrderStatisticsZSet<K> {
    let mut storage = NodeStorage::new();

    // 1. Point at checkpoint file - DON'T copy or read it
    storage.spill_file_path = Some(committed.spill_file_path.into());
    storage.leaf_block_locations = committed.leaf_block_locations;

    // 2. All leaves are cold (on disk) - will be lazy-loaded on access
    storage.mark_all_leaves_evicted();

    // 3. Build internal nodes from summaries - O(num_leaves)
    storage.internal_nodes = build_internal_nodes_from_summaries(
        &committed.leaf_summaries
    );

    OrderStatisticsZSet::from_storage(storage, committed.total_weight)
}
```

### Post-Restore: Checkpoint File Becomes Spill File

After restore, the checkpoint file IS the live spill file:
- Leaf access → read from checkpoint file at recorded offset
- Same format, same offsets, zero transformation
- If a leaf is modified → overwrite at existing offset (in-place)
- Next checkpoint → copy file to checkpoint storage, continue with local file

**Restore requires no bulk reading of leaf data - only metadata + lazy loading.**

## Benefits

1. **Fast flush**: Skip already-spilled leaves, only write dirty + never-spilled
2. **Immediate resume**: Runtime continues with local spill file right after checkpoint
3. **In-place updates**: Modified leaves overwrite existing blocks (no file growth from modifications)
4. **Fast restore**: O(num_leaves) index rebuild, not O(num_entries) data read
5. **Zero-copy restore**: Checkpoint file becomes live spill file directly
6. **Config-independent format**: Same checkpoint format regardless of `max_spillable_level`
7. **Checkpoint independence**: Each checkpoint is self-contained, no shared state with runtime

## Required Changes Summary

| Component | Change |
|-----------|--------|
| `NodeStorage` | Add `checkpoint_to()`, `flush_all_leaves_to_disk()` methods |
| `NodeStorage` | Add `mark_all_leaves_evicted()`, `is_leaf_on_disk()` methods |
| `NodeStorage` | Handle edge case: create spill file if none exists |
| `NodeStorage` | Support in-place block overwrites for modified leaves |
| `OrderStatisticsZSet` | Add `storage_mut()`, `restore()`, `collect_leaf_summaries()` methods |
| `OrderStatisticsZSet` | Add `build_internal_nodes_from_summaries()`, `from_storage()` methods |
| `PercentileOperator` | Implement `checkpoint()`, `restore()` using above |
| Spill file lifecycle | Checkpoint gets copy; runtime keeps local file; restore reuses checkpoint as spill file |

### NodeStorage Methods Detail

```rust
impl<I, L> NodeStorage<I, L> {
    /// Write leaves not yet on disk (dirty + never-spilled).
    /// Skips already-spilled clean leaves - no re-serialization!
    pub fn flush_all_leaves_to_disk(&mut self) -> Result<(), Error>;

    /// Check if a leaf is already on disk (spilled and not dirty).
    pub fn is_leaf_on_disk(&self, leaf_id: usize) -> bool;

    /// Checkpoint: flush unwritten leaves, move file, return metadata.
    pub fn save_leaves(&mut self, checkpoint_dir: &StoragePath, id: &str)
        -> Result<CommittedLeafStorage<L::Key>, Error>;

    /// Mark all leaves as evicted (on disk, not in memory).
    /// Used during restore to indicate leaves should be lazy-loaded.
    pub fn mark_all_leaves_evicted(&mut self, num_leaves: usize);

    /// Take ownership of spill file path.
    pub fn take_spill_file(&mut self) -> Option<PathBuf>;
}
```

### OrderStatisticsZSet Methods Detail

```rust
impl<T: DBData> OrderStatisticsZSet<T> {
    /// Collect leaf summaries for checkpoint (first_key, weight_sum per leaf).
    pub fn collect_leaf_summaries(&self) -> Vec<LeafSummary<T>>;

    /// Restore from checkpoint - O(num_leaves), checkpoint file becomes spill file.
    pub fn restore(committed: CommittedLeafStorage<T>) -> Result<Self, Error>;

    /// Build internal nodes from summaries without reading leaf contents.
    fn build_internal_nodes_from_summaries(summaries: &[LeafSummary<T>]) -> Vec<InternalNode<T>>;

    /// Construct from restored NodeStorage.
    pub fn from_storage(storage: OsmNodeStorage<T>, total_weight: ZWeight) -> Self;
}
```

## Testing Strategy

1. **Unit tests for NodeStorage checkpoint:**
   - `flush_all_leaves_to_disk()` with various states:
     - All in memory (never spilled) → all written
     - Partially spilled → only unspilled written
     - Fully spilled (clean) → nothing written (verify no re-serialization)
     - Mix of dirty + clean + never-spilled → correct subset written
   - Verify `is_leaf_on_disk()` correctly identifies leaf states
   - Verify leaf_summaries collected correctly

2. **Unit tests for OrderStatisticsZSet restore:**
   - `build_internal_nodes_from_summaries()` produces correct tree structure
   - Verify separator keys match first_key from summaries
   - Verify subtree_sums match aggregated weight_sum
   - Verify restored tree gives same percentile results as original

3. **Integration tests for checkpoint file reuse:**
   - Checkpoint → restore → verify checkpoint file is now spill file
   - After restore, access leaves → verify loaded from checkpoint file
   - After restore, modify tree → verify new data appended correctly
   - Checkpoint again → verify cycle works

4. **Integration tests for PercentileOperator:**
   - Checkpoint/restore with multiple keys
   - Verify percentile computation correct after restore
   - Verify incremental updates work after restore
   - Test with different `max_spillable_level` settings at checkpoint vs restore

5. **Performance tests:**
   - Checkpoint with large tree (mostly spilled) → verify O(dirty + unspilled)
   - Restore with large tree → verify O(num_leaves), not O(num_entries)
   - Verify no bulk leaf reads during restore (mock/instrument file I/O)
   - Measure checkpoint metadata size (~32 MB for 1B entries)

## Comparison with Alternatives

| Approach | Checkpoint Size | Checkpoint Time | Restore Time | Complexity |
|----------|-----------------|-----------------|--------------|------------|
| Direct serialization | O(all data) | O(all data) | O(all data) | Low |
| **Optimized (this plan)** | O(num_leaves) | O(dirty + unspilled) | O(num_leaves) | Medium |
| Always-persistent tree | O(metadata only) | O(1) | O(1) | High |

**Direct serialization:** Serialize all tree data to checkpoint file. Simple but
creates huge checkpoints for large trees. Impractical for billions of entries.

**Optimized leaves-only (this plan):**
- Checkpoint: Flush dirty + never-spilled leaves, copy file to checkpoint storage
- Resume: Runtime continues with local spill file, in-place overwrites for modifications
- Restore: Rebuild internal nodes from leaf_summaries, checkpoint file becomes spill file
- No bulk reading of leaf contents on restore
- Checkpoint format independent of `max_spillable_level`

**Always-persistent tree:** Tree is always backed by persistent storage (like a
database B+ tree). Checkpoint is just a metadata snapshot since data is already
durable. Would require significant architectural changes to NodeStorage - essentially
making it a persistent data structure rather than a memory-with-spill structure.
Not worth the complexity for the current use case.

### Performance Summary

For a steady-state system with 1B entries (~1M leaves), mostly already spilled:

| Operation | Cost | Example |
|-----------|------|---------|
| Checkpoint (flush) | O(dirty + unspilled) | ~1000 leaves = fast |
| Checkpoint (file copy) | O(file size) | Copy to S3 |
| Checkpoint metadata | O(num_leaves) | ~32 MB write |
| Resume (runtime) | O(1) | Immediate - local file unchanged |
| Restore (index rebuild) | O(num_leaves) | ~1M leaves = fast |
| Restore (leaf I/O) | O(0) | Zero - file used directly |
| First leaf access | O(1) | Read from checkpoint file |

The file copy to checkpoint storage (S3) is the main checkpoint cost. This is acceptable
because checkpoints are infrequent and S3 upload bandwidth is typically good.
