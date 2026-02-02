# PercentileOperator Checkpoint/Restore Implementation Plan

## Current State

The `PercentileOperator` maintains per-key `OrderStatisticsMultiset` trees in memory. Checkpoint/restore is not yet implemented due to serialization complexity.

## Goals

1. Fault-tolerant state persistence for `PercentileOperator`
2. Efficient incremental checkpointing (only dirty trees)
3. Consistent recovery on restart

## Architecture

### State to Checkpoint

```rust
pub struct PercentileOperator<K, V> {
    trees: BTreeMap<K, OrderStatisticsMultiset<V>>,  // Main state
    prev_output: BTreeMap<K, Option<V>>,              // Previous outputs
    percentile: f64,                                   // Config (immutable)
    ascending: bool,                                   // Config (immutable)
    continuous: bool,                                  // Config (immutable)
}
```

### Checkpoint Structure

```
<base>/<persistent_id>/
├── metadata.json           # Operator metadata and key list
├── prev_output.bin         # Serialized prev_output map
└── trees/
    ├── key_0.bin           # OrderStatisticsMultiset for key 0
    ├── key_1.bin           # OrderStatisticsMultiset for key 1
    └── ...
```

## Implementation Steps

### Step 1: Add Serialization Bounds

Update type bounds to require serialization:

```rust
impl<K, V> PercentileOperator<K, V>
where
    K: DBData + rkyv::Serialize<...>,
    V: DBData + rkyv::Serialize<...>,
    <K as Archive>::Archived: Ord + Deserialize<K, ...>,
    <V as Archive>::Archived: Ord + Deserialize<V, ...>,
```

**Challenge**: `DBData` doesn't inherently include rkyv serialization bounds. Need to either:
- Add serialization bounds to stream extension methods
- Use a separate trait for serializable DBData
- Create wrapper types

### Step 2: Implement OrderStatisticsMultiset Serialization

The tree stores nodes as:
```rust
pub struct OrderStatisticsMultiset<T> {
    root: Option<NodeId>,
    branching_factor: usize,
    storage: NodeStorage<InternalNode<T>, LeafNode<T>>,
    total_count: ZWeight,
}
```

**Serialization approach**:
1. Serialize tree as sorted (value, count) pairs
2. On restore, rebuild tree via bulk_load
3. This is simpler than serializing internal node structure

```rust
impl<T: DBData + Serialize<...>> OrderStatisticsMultiset<T> {
    pub fn to_pairs(&self) -> Vec<(T, ZWeight)> {
        // Iterate tree in order, collect (value, count) pairs
    }

    pub fn from_pairs(pairs: Vec<(T, ZWeight)>) -> Self {
        let mut tree = Self::new();
        tree.bulk_load(pairs);
        tree
    }
}
```

### Step 3: Implement Operator::checkpoint

```rust
fn checkpoint(
    &mut self,
    base: &StoragePath,
    pid: Option<&str>,
    files: &mut Vec<Arc<dyn FileCommitter>>,
) -> Result<(), Error> {
    let pid = require_persistent_id(pid, &self.global_id)?;
    let backend = Runtime::storage_backend().unwrap();

    // Create checkpoint directory
    let checkpoint_dir = base.join(&format!("{pid}"));

    // 1. Serialize metadata (key list)
    let key_list: Vec<K> = self.trees.keys().cloned().collect();
    let metadata = PercentileCheckpoint {
        version: 1,
        percentile: self.percentile,
        ascending: self.ascending,
        continuous: self.continuous,
        keys: key_list,
    };
    let metadata_file = checkpoint_dir.join("metadata.json");
    let committer = backend.write_json(&metadata_file, &metadata)?;
    files.push(committer);

    // 2. Serialize prev_output
    let prev_output_file = checkpoint_dir.join("prev_output.bin");
    let committer = serialize_rkyv(&prev_output_file, &self.prev_output)?;
    files.push(committer);

    // 3. Serialize each tree
    for (key, tree) in &self.trees {
        let key_hash = hash_key(key);
        let tree_file = checkpoint_dir.join(&format!("trees/{key_hash}.bin"));
        let pairs = tree.to_pairs();
        let committer = serialize_rkyv(&tree_file, &pairs)?;
        files.push(committer);
    }

    Ok(())
}
```

### Step 4: Implement Operator::restore

```rust
fn restore(&mut self, base: &StoragePath, pid: Option<&str>) -> Result<(), Error> {
    let pid = require_persistent_id(pid, &self.global_id)?;
    let backend = Runtime::storage_backend().unwrap();

    let checkpoint_dir = base.join(&format!("{pid}"));

    // 1. Load metadata
    let metadata_file = checkpoint_dir.join("metadata.json");
    let metadata: PercentileCheckpoint<K> = backend.read_json(&metadata_file)?;

    // Verify config matches
    if metadata.percentile != self.percentile
        || metadata.ascending != self.ascending
        || metadata.continuous != self.continuous {
        return Err(Error::ConfigMismatch);
    }

    // 2. Load prev_output
    let prev_output_file = checkpoint_dir.join("prev_output.bin");
    self.prev_output = deserialize_rkyv(&prev_output_file)?;

    // 3. Load trees
    self.trees.clear();
    for key in metadata.keys {
        let key_hash = hash_key(&key);
        let tree_file = checkpoint_dir.join(&format!("trees/{key_hash}.bin"));
        let pairs: Vec<(V, ZWeight)> = deserialize_rkyv(&tree_file)?;
        let tree = OrderStatisticsMultiset::from_pairs(pairs);
        self.trees.insert(key, tree);
    }

    Ok(())
}
```

### Step 5: Add Init Method

```rust
fn init(&mut self, global_id: &GlobalNodeId) {
    self.global_id = global_id.clone();
}
```

## Helper Types

```rust
#[derive(Serialize, Deserialize)]
struct PercentileCheckpoint<K> {
    version: u32,
    percentile: f64,
    ascending: bool,
    continuous: bool,
    keys: Vec<K>,
}
```

## Testing Strategy

### Unit Tests

1. **Basic checkpoint/restore**: Create state, checkpoint, restore, verify equality
2. **Incremental after restore**: Restore state, apply deltas, verify correctness
3. **Empty state**: Checkpoint/restore empty operator
4. **Large state**: Checkpoint/restore with many keys and large trees

### Integration Tests

1. **Circuit restart**: Full circuit checkpoint and restart
2. **Worker failure**: Multi-worker checkpoint with simulated failure

## Alternative Approaches Considered

### 1. NodeStorage-based Checkpointing
Leverage existing `NodeStorage` spill files as checkpoints. Rejected because:
- Spill files are for memory management, not fault tolerance
- Spill files are cleaned up on Drop
- Would require significant NodeStorage changes

### 2. Trace-based Storage
Store trees in a DBSP Trace. Rejected because:
- Traces are optimized for different access patterns
- Would add significant complexity
- Trees need different serialization than batches

### 3. Per-key Separate Files
Each key's tree in its own file (current proposal). Pros:
- Simple implementation
- Parallel checkpoint possible
- Only dirty trees need rewriting

## Dependencies

1. `OrderStatisticsMultiset::to_pairs()` and `from_pairs()` methods
2. `require_persistent_id` helper function
3. rkyv serialization bounds on K and V

## Open Questions

1. **Incremental checkpointing**: Should we track dirty keys to only checkpoint changed trees?
2. **Key hashing**: How to generate stable filenames from arbitrary key types?
3. **Cleanup**: When should old checkpoint files be deleted?

## Timeline Estimate

1. Add `to_pairs`/`from_pairs` to OrderStatisticsMultiset: 1-2 hours
2. Add serialization bounds and init method: 1 hour
3. Implement checkpoint method: 2-3 hours
4. Implement restore method: 2-3 hours
5. Unit tests: 2-3 hours
6. Integration tests: 2-3 hours

**Total**: ~12-16 hours of development
