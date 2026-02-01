# Spine Async: Persistence and Spill-to-Disk Architecture

This document describes the persistence and spill-to-disk behavior of the `Spine` data structure in DBSP, intended as a reference for implementing similar capabilities in other data structures like `OrderStatisticsMultiset`.

## Overview

The `Spine` is DBSP's primary trace data structure for maintaining incremental state. It consists of a collection of immutable batches that are merged asynchronously in the background. The key innovation is the **two-tier storage model** where batches can exist either in memory or on disk, with transparent switching between the two based on size thresholds.

## Core Architecture

### Two-Tier Storage Model

```
┌─────────────────────────────────────────────────────────────┐
│                         Spine                                │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                    AsyncMerger                           ││
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐       ││
│  │  │ Level 0 │ │ Level 1 │ │ Level 2 │ │   ...   │       ││
│  │  │ <10K    │ │ <100K   │ │ <1M     │ │         │       ││
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘       ││
│  │       │           │           │           │             ││
│  │  ┌────▼────┐ ┌────▼────┐ ┌────▼────┐ ┌────▼────┐       ││
│  │  │ Batches │ │ Batches │ │ Batches │ │ Batches │       ││
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘       ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘

Each Batch can be:
┌─────────────────────┐    ┌─────────────────────┐
│   VecBatch (RAM)    │ or │  FileBatch (Disk)   │
│                     │    │                     │
│  - Fast access      │    │  - Buffered I/O     │
│  - Limited by RAM   │    │  - Virtually        │
│  - No persistence   │    │    unlimited size   │
└─────────────────────┘    │  - Checkpointable   │
                           └─────────────────────┘
```

### BatchLocation Enum

Every batch reports its storage location:

```rust
pub enum BatchLocation {
    Memory,   // Data is in RAM (VecBatch, VecWSet, etc.)
    Storage,  // Data is on disk (FileBatch, FileWSet, etc.)
}
```

This allows the system to make informed decisions about data placement.

## Spill-to-Disk Decision Logic

### Configuration Thresholds

Two runtime thresholds control when data spills to disk:

1. **`min_step_storage_bytes`**: Threshold for step operations (building batches during computation)
2. **`min_index_storage_bytes`**: Threshold for index operations (merging batches)

These are accessed via `Runtime::min_step_storage_bytes()` and `Runtime::min_index_storage_bytes()`.

### Decision Functions

**`pick_merge_destination()`** (from `fallback/utils.rs`):

```rust
pub fn pick_merge_destination<'a, B, I>(
    batches: I,
    dst_hint: Option<BatchLocation>,
) -> BatchLocation
where
    B: BatchReader,
    I: IntoIterator<Item = &'a B>,
{
    // 1. Respect explicit hints
    if let Some(location) = dst_hint {
        return location;
    }

    // 2. Check threshold
    match Runtime::min_index_storage_bytes().unwrap_or(usize::MAX) {
        0 => BatchLocation::Storage,           // Always storage
        usize::MAX => BatchLocation::Memory,   // Storage disabled
        min_storage_bytes => {
            // Sum batch sizes until threshold is reached
            let mut size = 0;
            for b in batches {
                size += b.approximate_byte_size();
                if size >= min_storage_bytes {
                    return BatchLocation::Storage;
                }
            }
            BatchLocation::Memory
        }
    }
}
```

**`BuildTo::for_capacity()`**:

```rust
pub fn for_capacity(key_capacity: usize, value_capacity: usize) -> Self {
    match Runtime::min_step_storage_bytes().unwrap_or(usize::MAX) {
        usize::MAX => Self::Memory,  // Storage disabled

        min_step_storage_bytes
            if (key_capacity + value_capacity) * 32 >= min_step_storage_bytes =>
        {
            Self::Storage  // Estimate suggests large batch
        }

        min_step_storage_bytes => {
            Self::Threshold(min_step_storage_bytes)  // Start in memory, spill if needed
        }
    }
}
```

### Threshold-Based Building

The `BuildTo::Threshold(bytes)` variant enables **lazy spilling**:

```rust
enum BuilderInner<K, R> {
    Vec(VecWSetBuilder<K, R>),      // Always in memory
    File(FileWSetBuilder<K, R>),    // Always on disk
    Threshold {
        vec: VecWSetBuilder<K, R>,  // Start in memory
        size: usize,                 // Accumulated size
        threshold: usize,            // Spill threshold
    },
}
```

On each push operation:
1. Add to the in-memory `vec`
2. Increment `size` by the element's size
3. If `size >= threshold`, **spill**:
   - Create a `FileWSetBuilder`
   - Copy all accumulated data to it
   - Replace `Threshold { vec, .. }` with `File(file_builder)`

This ensures small batches stay in memory while large ones automatically move to disk.

## File Storage Architecture

### File Format Overview

File batches use a columnar B-tree-like format:

```
┌────────────────────────────────────────────────────┐
│                    File Layout                      │
├────────────────────────────────────────────────────┤
│  Data Block 0    │  Keys + Values (serialized)     │
│  Data Block 1    │  Keys + Values (serialized)     │
│  ...             │                                  │
│  Data Block N    │  Keys + Values (serialized)     │
├────────────────────────────────────────────────────┤
│  Index Block 0   │  Child pointers + row counts    │
│  ...             │                                  │
├────────────────────────────────────────────────────┤
│  Filter Block    │  Bloom filter for key existence │
├────────────────────────────────────────────────────┤
│  File Trailer    │  Column roots, metadata         │
└────────────────────────────────────────────────────┘
```

### Block Structure

**Data Blocks** contain:
- Serialized key-value pairs (using rkyv for zero-copy)
- Value map (offsets to each item)
- Optional row groups for hierarchical access
- Compression (Snappy) and CRC32C checksum

**Index Blocks** contain:
- Child block locations (offset, size)
- Row counts per child subtree
- Separator keys for binary search

**Filter Block**:
- Bloom filter for probabilistic key existence checks
- Reduces unnecessary I/O for missing keys

### Buffer Cache Integration

The `BufferCache` provides a global LRU cache for file blocks:

```rust
pub struct BufferCache {
    // Thread-safe, lock-free cache
    // Key: (FileId, BlockOffset)
    // Value: Arc<dyn CacheEntry>
}

// Cache access pattern in DataBlock::new():
let (access, entry) = match cache.get(&file_handle, location) {
    Some(entry) => (CacheAccess::Hit, entry),
    None => {
        let block = file.read_block(location)?;
        let entry = Self::from_raw_with_cache(block, node, cache, file_id);
        (CacheAccess::Miss, entry)
    }
};
file.stats.record(access, start.elapsed(), location);
```

Key characteristics:
- **LRU eviction**: Least recently used blocks are evicted first
- **Reference counting**: `Arc<dyn CacheEntry>` allows blocks to stay alive while in use
- **Statistics tracking**: Hit/miss rates, access times for monitoring
- **Cost-based**: Each entry reports its memory cost for eviction decisions

### CacheEntry Trait

```rust
pub trait CacheEntry: Send + Sync + 'static {
    fn cost(&self) -> usize;  // Memory cost for eviction decisions
}

// Example for DataBlock:
impl<K, A> CacheEntry for DataBlock<K, A> {
    fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.capacity()
    }
}
```

## Asynchronous Merging

### Level-Based Organization

Batches are assigned to levels based on their size (roughly log10):

```rust
fn size_to_level(len: usize) -> usize {
    match len {
        0..=9999 => 0,
        10_000..=99_999 => 1,
        100_000..=999_999 => 2,
        1_000_000..=9_999_999 => 3,
        // ... up to level 8
    }
}
```

### Merge Slot Structure

Each level maintains a `Slot`:

```rust
struct Slot<B> {
    merging_batches: Option<Vec<Arc<B>>>,  // Currently being merged
    loose_batches: VecDeque<Arc<B>>,        // Awaiting merge
    // Statistics...
}
```

### Merge Initiation

Merges are started when a level has enough loose batches:

```rust
const MERGE_COUNTS: [RangeInclusive<usize>; MAX_LEVELS] = [
    8..=64,   // Level 0: merge 8-64 batches
    8..=64,   // Level 1
    3..=64,   // Level 2
    3..=64,   // Level 3
    // ...
];

fn try_start_merge(&mut self, level: usize) -> Option<Vec<Arc<B>>> {
    let merge_counts = &MERGE_COUNTS[level];
    if self.merging_batches.is_none()
       && self.loose_batches.len() >= *merge_counts.start()
    {
        let n = min(*merge_counts.end(), self.loose_batches.len());
        let batches = self.loose_batches.drain(..n).collect();
        self.merging_batches = Some(batches.clone());
        Some(batches)
    } else {
        None
    }
}
```

### Backpressure

The merger applies backpressure when too many batches accumulate:

```rust
const HIGH_THRESHOLD: usize = 128;
const LOWER_THRESHOLD: usize = 127;

fn should_apply_backpressure(&self) -> bool {
    self.slots.iter()
        .map(|s| s.loose_batches.len())
        .sum::<usize>() >= HIGH_THRESHOLD
}

// In add_batch():
if state.should_apply_backpressure() {
    let start = Instant::now();
    // Block until merging catches up
    let state = self.no_backpressure.wait(state).unwrap();
    state.spine_stats.backpressure_wait += start.elapsed();
}
```

## Checkpointing and Persistence

### Save Operation

```rust
fn save(&mut self, base: &StoragePath, persistent_id: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>) -> Result<(), Error> {

    // 1. Pause new merges, get batch lists
    let (not_merging, merging) = self.merger.pause_new_merges();

    // 2. Persist all batches to disk (if not already)
    let not_merging = persist_batches(not_merging);
    let merging = persist_batches(merging);

    // 3. Resume merging with persisted batches
    self.merger.resume(not_merging.iter().cloned());

    // 4. Collect file paths
    let ids = not_merging.iter().chain(merging.iter())
        .map(|batch| batch.file_reader().path().to_string())
        .collect();

    // 5. Write checkpoint metadata
    let committed = CommittedSpine { batches: ids, dirty: self.dirty };
    backend.write(&checkpoint_file, to_bytes(&committed)?)?;
}

fn persist_batches<B: Batch>(batches: Vec<Arc<B>>) -> Vec<Arc<B>> {
    batches.into_iter().map(|batch| {
        if let Some(persisted) = batch.persisted() {
            Arc::new(persisted)  // Was in memory, now on disk
        } else {
            batch  // Already on disk
        }
    }).collect()
}
```

### Restore Operation

```rust
fn restore(&mut self, base: &StoragePath, persistent_id: &str) -> Result<(), Error> {
    let content = backend.read(&checkpoint_file)?;
    let committed: CommittedSpine = deserialize(&content)?;

    self.dirty = committed.dirty;
    for batch_path in committed.batches {
        let batch = B::from_path(&self.factories, &batch_path.into())?;
        self.insert(batch);
    }
}
```

### Batch Persistence Interface

```rust
pub trait Batch: BatchReader {
    /// If this batch is in memory, return a persisted (on-disk) version.
    /// If already on disk, return None.
    fn persisted(&self) -> Option<Self>;

    /// Get the file reader if this batch is on disk.
    fn file_reader(&self) -> Option<Arc<dyn FileReader>>;

    /// Load a batch from a file path.
    fn from_path(factories: &Self::Factories, path: &StoragePath) -> Result<Self, Error>;
}
```

## Design Patterns for New Data Structures

### Applying to OrderStatisticsMultiset

The OrderStatisticsMultiset B+ tree can adopt similar patterns:

#### 1. Two-Tier Node Storage

```rust
enum Node<T> {
    InMemory(InMemoryNode<T>),
    OnDisk(DiskNodeRef<T>),  // Contains file offset, loaded on demand
}

struct DiskNodeRef<T> {
    file: Arc<Reader>,
    offset: u64,
    cached: Option<Arc<InMemoryNode<T>>>,  // Cached after load
}
```

#### 2. Hot/Cold Node Classification

Unlike batches where the entire thing is memory or disk, B+ trees can have:
- **Hot nodes**: Root and upper internal nodes (always in memory)
- **Warm nodes**: Frequently accessed leaves (cached via buffer cache)
- **Cold nodes**: Rarely accessed leaves (loaded on demand)

```rust
struct OrderStatisticsMultiset<T> {
    // Always in memory: root and top k levels
    hot_nodes: Vec<InternalNode<T>>,

    // On-disk with caching
    file: Option<Arc<Reader>>,
    buffer_cache: Arc<BufferCache>,

    // Metadata
    total_weight: ZWeight,
    num_keys: usize,
}
```

#### 3. Lazy Node Loading

```rust
fn get_child(&self, node_idx: usize, child_idx: usize) -> Arc<Node<T>> {
    let node = &self.nodes[node_idx];
    match &node.children[child_idx] {
        NodeRef::Loaded(arc) => arc.clone(),
        NodeRef::OnDisk { offset, size } => {
            // Check buffer cache first
            if let Some(cached) = self.buffer_cache.get(&self.file, *offset) {
                return cached;
            }
            // Load from disk
            let block = self.file.read_block(*offset)?;
            let node = Arc::new(deserialize_node(block));
            self.buffer_cache.insert(self.file.id(), *offset, node.clone());
            node
        }
    }
}
```

#### 4. Partial Persistence

For checkpointing:
- Persist only modified ("dirty") nodes
- Track which nodes have changed since last checkpoint
- On restore, load root and upper levels; lazy-load leaves

```rust
struct Node<T> {
    // ... existing fields ...
    dirty: bool,  // Modified since last checkpoint
    disk_offset: Option<u64>,  // Location on disk if persisted
}

fn persist_node(&mut self, node_idx: usize) -> u64 {
    let node = &mut self.nodes[node_idx];
    if !node.dirty && node.disk_offset.is_some() {
        return node.disk_offset.unwrap();  // Already persisted
    }

    // Recursively persist children first
    for child_idx in &mut node.children {
        if let NodeRef::Loaded(child) = child_idx {
            let offset = self.persist_node(*child);
            *child_idx = NodeRef::OnDisk { offset, size };
        }
    }

    // Write this node
    let offset = self.writer.write_node(node)?;
    node.dirty = false;
    node.disk_offset = Some(offset);
    offset
}
```

#### 5. Spill Threshold for Construction

During bulk loading or heavy updates:

```rust
impl OrderStatisticsMultiset<T> {
    fn insert(&mut self, key: T, weight: ZWeight) {
        // Normal insert logic...

        self.memory_size += key.size_of() + size_of::<ZWeight>();

        // Check if we should spill cold nodes to disk
        if self.memory_size > self.spill_threshold {
            self.spill_cold_leaves();
        }
    }

    fn spill_cold_leaves(&mut self) {
        // Find leaves that haven't been accessed recently
        // Write them to disk
        // Replace with OnDisk references
    }
}
```

### Integration with Buffer Cache

Reuse DBSP's existing `BufferCache`:

```rust
use crate::storage::buffer_cache::{BufferCache, CacheEntry};

impl<T: DataTrait> CacheEntry for LeafNode<T> {
    fn cost(&self) -> usize {
        self.entries.capacity() * (size_of::<T>() + size_of::<ZWeight>())
    }
}

// Access pattern:
fn access_leaf(&self, offset: u64) -> Arc<LeafNode<T>> {
    let cache = Runtime::buffer_cache();
    match cache.get(&self.file, BlockLocation::from_offset(offset)) {
        Some(entry) => entry.downcast().unwrap(),
        None => {
            let leaf = self.load_leaf_from_disk(offset)?;
            cache.insert(self.file.id(), offset, leaf.clone());
            leaf
        }
    }
}
```

### Serialization Strategy

Use rkyv for zero-copy serialization, matching DBSP's approach:

```rust
#[derive(Archive, Serialize, Deserialize)]
struct SerializedLeaf<T> {
    entries: Vec<(T, ZWeight)>,
    next_leaf: u64,  // File offset, not index
}

#[derive(Archive, Serialize, Deserialize)]
struct SerializedInternal<T> {
    keys: Vec<T>,
    child_offsets: Vec<u64>,  // File offsets
    subtree_sums: Vec<ZWeight>,
}

// File layout:
// [Leaf 0] [Leaf 1] ... [Leaf N] [Internal 0] ... [Internal M] [Root] [Trailer]
```

## Key Takeaways for OrderStatisticsMultiset

1. **Start Simple**: Begin with in-memory-only, add disk spilling as optimization
2. **Use Existing Infrastructure**: Leverage `BufferCache`, `Runtime` thresholds, `StorageBackend`
3. **B+ Tree is Already Disk-Friendly**: Large nodes, sequential access patterns
4. **Hot/Cold Separation**: Keep root + upper levels in memory, spill deep leaves
5. **Lazy Loading**: Only load nodes when accessed, cache for reuse
6. **Dirty Tracking**: Only persist modified nodes during checkpointing
7. **Threshold-Based Decisions**: Use runtime configuration for spill thresholds

## Related Files

- `crates/dbsp/src/trace/spine_async.rs`: Main spine implementation
- `crates/dbsp/src/trace/ord/fallback/`: Two-tier batch implementations
- `crates/dbsp/src/storage/buffer_cache/`: LRU block cache
- `crates/dbsp/src/storage/file/reader.rs`: File format reading
- `crates/dbsp/src/storage/file/writer.rs`: File format writing
- `crates/dbsp/src/algebra/order_statistics_multiset.rs`: Target for spill-to-disk
