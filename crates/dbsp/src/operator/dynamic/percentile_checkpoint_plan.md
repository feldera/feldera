# PercentileOperator Checkpoint/Restore Implementation Plan

## Current State

The `PercentileOperator` maintains per-key `OrderStatisticsMultiset` trees that can hold
**billions of entries**. Each tree uses `NodeStorage` which already supports spilling leaf
nodes to disk. Checkpoint/restore is not yet implemented.

## Scope

**This plan is designed for `NodeStorage` with B+ tree structures where domain data
resides in leaf nodes.** This matches `OrderStatisticsMultiset`, where all `(value, weight)`
pairs are stored in leaves.

The core principle is: **don't re-serialize data that's already on disk**. Checkpoint
references existing spill files rather than duplicating data.

This works regardless of `max_spillable_level`:
- **`max_spillable_level = 0` (default):** Only leaves spill. Checkpoint serializes
  internal nodes (~2%) in metadata, references leaf spill file.
- **`max_spillable_level > 0`:** Internal nodes can also spill. Checkpoint references
  both internal and leaf data in spill file(s), metadata is even smaller.

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

### OrderStatisticsMultiset is a B+ Tree

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

This B+ tree property is why the reference-based checkpoint works efficiently:
- Checkpoint the leaves (where all data lives) → reference spill files
- Serialize internal nodes (small, derived) → include in metadata

### NodeStorage Spill Levels

NodeStorage supports different spill configurations:

```rust
/// max_spillable_level controls which nodes can be spilled:
/// - 0: Only leaves can be spilled (default, most efficient)
/// - 1: Leaves and level-1 internal nodes can be spilled
/// - u8::MAX: All nodes can be spilled
```

**The checkpoint approach works for any `max_spillable_level`:**

| Spill Level | What's on Disk | What's in Metadata |
|-------------|----------------|-------------------|
| 0 (default) | Leaves only | Internal nodes + leaf index |
| 1+ | Leaves + some internal nodes | Root path + node indices |

The principle is the same: reference data already on disk, serialize only what's in memory.
With higher spill levels, more data is already on disk, so metadata gets smaller.

### Generalization

The approach generalizes to any `NodeStorage`-backed tree:
1. Spilled nodes are already serialized to files → reference them
2. In-memory nodes are serialized in checkpoint metadata
3. The higher the spill level, the smaller the metadata

For OrderStatisticsMultiset:
- With `max_spillable_level = 0`: ~2% in metadata (internal nodes), ~98% referenced (leaves)
- With higher levels: even less in metadata, more referenced

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
// Per-tree checkpoint metadata
struct CommittedNodeStorage<I> {
    /// Path to the spill file containing all leaf data
    spill_file_path: String,

    /// All internal nodes - always in memory, small (<2% of tree)
    internal_nodes: Vec<I>,

    /// Index into spill file: leaf_id -> (offset, size)
    leaf_block_locations: Vec<(usize, u64, u32)>,

    /// Tree structure metadata
    root_location: Option<NodeLocation>,
    leaf_count: usize,
    total_entries: usize,
    total_weight: ZWeight,
}

// Operator checkpoint
struct CommittedPercentile<K, I> {
    /// Per-key tree metadata (references spill files, doesn't contain leaf data)
    trees: Vec<(K, CommittedNodeStorage<I>)>,

    /// Previous outputs for delta computation
    prev_output: Vec<(K, Option<V>)>,
}
```

### Checkpoint Size Comparison

| Approach | Checkpoint Contains | Size for 1B entries |
|----------|---------------------|---------------------|
| Direct serialization | All entries | ~50+ GB |
| Reference-based | Internal nodes + index | ~100 MB |

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

1. **`save()` method** - Persist tree and return metadata:
   ```rust
   impl<I, L> NodeStorage<I, L> {
       /// Checkpoint the storage, returning metadata that references the spill file.
       ///
       /// This:
       /// 1. Flushes all dirty leaves to the spill file
       /// 2. Moves the spill file to a checkpoint-managed location
       /// 3. Returns metadata with the file path and internal nodes
       /// 4. Clears the internal spill_file_path (checkpointer owns the file now)
       pub fn save(
           &mut self,
           checkpoint_dir: &StoragePath,
           tree_id: &str,
       ) -> Result<CommittedNodeStorage<I>, Error> {
           // Flush any remaining dirty leaves
           self.flush_dirty_to_disk()?;

           // Move/rename spill file to checkpoint location
           let dest_path = checkpoint_dir.child(format!("{}.leaves", tree_id));
           if let Some(ref src_path) = self.spill_file_path {
               std::fs::rename(src_path, &dest_path)?;
           }

           // Build metadata
           let committed = CommittedNodeStorage {
               spill_file_path: dest_path.to_string(),
               internal_nodes: self.internal_nodes.clone(),
               leaf_block_locations: self.leaf_block_locations.iter()
                   .map(|(id, (off, sz))| (*id, *off, *sz))
                   .collect(),
               root_location: /* ... */,
               // ...
           };

           // Clear our reference - checkpointer owns the file now
           self.spill_file_path = None;

           Ok(committed)
       }
   }
   ```

2. **`restore()` method** - Rebuild from checkpoint:
   ```rust
   impl<I, L> NodeStorage<I, L> {
       /// Restore storage from a checkpoint.
       ///
       /// This points the storage at an existing spill file without copying data.
       pub fn restore(committed: CommittedNodeStorage<I>) -> Result<Self, Error> {
           let mut storage = Self::new();

           // Restore internal nodes (small, always in memory)
           storage.internal_nodes = committed.internal_nodes;

           // Point at existing spill file
           storage.spill_file_path = Some(committed.spill_file_path.into());
           storage.leaf_block_locations = committed.leaf_block_locations
               .into_iter()
               .map(|(id, off, sz)| (id, (off, sz)))
               .collect();

           // All leaves start as evicted (loaded on demand)
           storage.evicted_leaves = (0..committed.leaf_count).collect();
           storage.spilled_leaves = storage.evicted_leaves.clone();

           Ok(storage)
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

        // Save each tree's NodeStorage
        let mut trees = Vec::new();
        for (key, tree) in &mut self.trees {
            let tree_id = format!("tree_{}", trees.len());
            let committed = tree.storage_mut().save(&checkpoint_dir, &tree_id)?;
            trees.push((key.clone(), committed));
        }

        // Save prev_output
        let prev_output: Vec<_> = self.prev_output.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Write metadata file (small - just internal nodes + index)
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
        let committed: CommittedPercentile<K, _> = deserialize(&content)?;

        // Restore trees from their spill files
        self.trees.clear();
        for (key, node_storage_meta) in committed.trees {
            let storage = NodeStorage::restore(node_storage_meta)?;
            let tree = OrderStatisticsMultiset::from_storage(storage);
            self.trees.insert(key, tree);
        }

        // Restore prev_output
        self.prev_output = committed.prev_output.into_iter().collect();

        Ok(())
    }
}
```

### Changes to OrderStatisticsMultiset

Need methods to access/set internal storage:

```rust
impl<T> OrderStatisticsMultiset<T> {
    /// Get mutable access to the underlying NodeStorage.
    pub fn storage_mut(&mut self) -> &mut OsmNodeStorage<T> {
        &mut self.storage
    }

    /// Construct from existing NodeStorage (for restore).
    pub fn from_storage(storage: OsmNodeStorage<T>, /* metadata */) -> Self {
        Self {
            storage,
            root: /* from metadata */,
            total_weight: /* from metadata */,
            // ...
        }
    }
}
```

## File Layout

```
<checkpoint_base>/<persistent_id>/
├── metadata.dat              # Small: internal nodes + indices + prev_output
├── tree_0.leaves             # Spill file for key 0 (leaf data)
├── tree_1.leaves             # Spill file for key 1 (leaf data)
├── tree_2.leaves             # ...
└── ...
```

**metadata.dat contains:**
- Internal nodes for all trees (~2% of tree data)
- Leaf block locations (index into .leaves files)
- Tree structure metadata
- prev_output map

**.leaves files contain:**
- Raw leaf node data (already serialized by NodeStorage)
- Same format as current spill files

## Checkpoint Flow

```
checkpoint():
  for each tree:
    1. flush_all_to_disk()             # Force ALL leaves to disk (even if not spilled yet)
    2. Move spill file to checkpoint/  # Take ownership
    3. Collect metadata (internal nodes, index)

  Write metadata.dat (small file)
```

### Edge Case: Small Trees (No Spill File Yet)

If a tree is small and hasn't triggered spilling:
- No spill file exists yet
- All leaves are in memory

**Solution:** `flush_all_to_disk()` must handle this case:
```rust
pub fn flush_all_to_disk(&mut self, path: Option<&Path>) -> Result<(), Error> {
    // If no spill file exists, create one
    if self.spill_file_path.is_none() {
        self.create_spill_file(path)?;
    }

    // Flush ALL leaves, not just dirty ones
    for leaf_id in 0..self.leaves.len() {
        if !self.spilled_leaves.contains(&leaf_id) {
            self.flush_leaf_to_disk(leaf_id)?;
        }
    }

    Ok(())
}
```

This ensures checkpoint works regardless of whether spilling has occurred.

## Restore Flow

```
restore():
  Read metadata.dat

  for each tree:
    1. Create NodeStorage pointing at .leaves file
    2. Set evicted_leaves = all leaves (lazy loading)
    3. Restore internal nodes from metadata

  Leaves loaded on-demand when accessed
```

## Benefits

1. **Fast checkpoint**: Only serialize internal nodes (~2% of data) + small index
2. **No data duplication**: Leaf data already on disk, just reference it
3. **Fast restore**: No deserialization of leaf data, load on demand
4. **Bounded memory**: Checkpoint doesn't load all data into memory

## Required Changes Summary

| Component | Change |
|-----------|--------|
| `NodeStorage` | Add `save()`, `restore()`, `flush_all_to_disk()` methods |
| `NodeStorage` | Handle edge case: create spill file if none exists |
| `OrderStatisticsMultiset` | Add `storage_mut()`, `from_storage()` methods |
| `PercentileOperator` | Implement `checkpoint()`, `restore()` using above |
| Spill file lifecycle | Checkpoint takes ownership; Drop doesn't delete checkpoint files |

### NodeStorage Methods Detail

```rust
impl<I, L> NodeStorage<I, L> {
    /// Force all leaves to disk, creating spill file if needed.
    /// Unlike flush_dirty_to_disk(), this writes ALL leaves.
    pub fn flush_all_to_disk(&mut self, path: Option<&Path>) -> Result<usize, Error>;

    /// Checkpoint: flush all, move file, return metadata.
    pub fn save(&mut self, checkpoint_dir: &StoragePath, id: &str)
        -> Result<CommittedNodeStorage<I>, Error>;

    /// Restore: point at existing spill file, lazy-load leaves.
    pub fn restore(committed: CommittedNodeStorage<I>) -> Result<Self, Error>;
}
```

## Testing Strategy

1. **Unit tests for NodeStorage save/restore:**
   - Save/restore with various spill states
   - Verify lazy loading works after restore

2. **Integration tests for PercentileOperator:**
   - Checkpoint/restore with multiple keys
   - Verify percentile computation correct after restore
   - Verify incremental updates work after restore

3. **Scale tests:**
   - Checkpoint with millions of entries
   - Measure checkpoint file size (should be small)
   - Measure checkpoint/restore time

## Timeline Estimate

1. NodeStorage save/restore methods: 4-6 hours
2. OrderStatisticsMultiset integration: 2-3 hours
3. PercentileOperator checkpoint/restore: 3-4 hours
4. Testing: 4-6 hours

**Total: ~2-3 days**

## Comparison with Alternatives

| Approach | Checkpoint Size | Checkpoint Time | Complexity |
|----------|-----------------|-----------------|------------|
| Direct serialization | O(all data) | O(all data) | Low |
| **Reference-based (this plan)** | O(internal nodes) | O(flush dirty) | Medium |
| Always-persistent tree | O(metadata only) | O(1) | High |

**Direct serialization:** Serialize all tree data to checkpoint file. Simple but
creates huge checkpoints for large trees. Impractical for billions of entries.

**Reference-based (this plan):** Flush leaves to spill files, checkpoint references
the files. Internal nodes (~2%) serialized in metadata. Good balance of efficiency
and complexity.

**Always-persistent tree:** Tree is always backed by persistent storage (like a
database B+ tree). Checkpoint is just a metadata snapshot since data is already
durable. Would require significant architectural changes to NodeStorage - essentially
making it a persistent data structure rather than a memory-with-spill structure.
Not worth the complexity for the current use case.
