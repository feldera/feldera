# OrderStatisticsMultiset Node-Level Storage - Implementation Plan

## Overview

This plan implements file-backed storage for `OrderStatisticsMultiset` nodes, enabling
spill-to-disk for large percentile aggregation state. The design keeps internal nodes
pinned in memory while allowing leaf nodes to be evicted to disk via LRU policy.

## Prerequisites

- **Bulk Load Optimization (Option 2)** must be implemented first
  - Required for O(n) reconstruction of evicted subtrees
  - Provides foundation for file format design

## Existing Infrastructure to Integrate With

This implementation will leverage existing DBSP components rather than building from scratch:

### 1. Storage Backend (`crates/dbsp/src/storage/backend.rs`)

The `StorageBackend` trait provides file I/O abstraction:

```rust
// From feldera_storage
pub trait StorageBackend: Send + Sync {
    fn create(&self, path: &StoragePath) -> Result<Box<dyn FileWriter>, StorageError>;
    fn open(&self, path: &StoragePath) -> Result<Arc<dyn FileReader>, StorageError>;
    fn read(&self, path: &StoragePath) -> Result<FBuf, StorageError>;
    fn write(&self, path: &StoragePath, data: FBuf) -> Result<(), StorageError>;
    // ...
}
```

**Usage**: Access via `Runtime::storage_backend()`. Used by `Spine`, `Z1Operator`, etc.

```rust
// Pattern from spine_async.rs:1512
let backend = Runtime::storage_backend().unwrap();
backend.write(&checkpoint_path, as_bytes)?;
```

### 2. Buffer Cache (`crates/dbsp/src/storage/buffer_cache/cache.rs`)

LRU cache implementation with cost-based eviction:

```rust
pub struct BufferCache { /* ... */ }

pub trait CacheEntry: Any + Send + Sync {
    fn cost(&self) -> usize;  // Memory cost for eviction decisions
}

impl BufferCache {
    fn get(&mut self, key: CacheKey) -> Option<Arc<dyn CacheEntry>>;
    fn insert(&mut self, key: CacheKey, value: Arc<dyn CacheEntry>);
    fn delete_file(&mut self, file_id: FileId);  // Evict all entries for a file
}
```

**Key insight**: The existing cache uses `(FileId, offset)` as keys and supports
file-granularity deletion. We can adapt this for leaf nodes.

### 3. Runtime Configuration (`crates/dbsp/src/circuit/runtime.rs`)

Storage thresholds and backend access:

```rust
impl Runtime {
    pub fn storage_backend() -> Option<Arc<dyn StorageBackend>>;
    pub fn min_storage_bytes() -> Option<usize>;      // Threshold for step output
    pub fn min_index_storage_bytes() -> Option<usize>; // Threshold for index storage
    pub fn file_writer_parameters() -> FileWriterParameters;
}
```

**Usage pattern** (from `fallback/utils.rs:94`):
```rust
match Runtime::min_index_storage_bytes().unwrap_or(usize::MAX) {
    0 => BatchLocation::Storage,
    usize::MAX => BatchLocation::Memory,
    threshold => { /* check size against threshold */ }
}
```

### 4. File Format Infrastructure (`crates/dbsp/src/storage/file/`)

Existing layer file format with rkyv serialization:

- **`format.rs`**: Block-based file format with headers, compression, checksums
- **`writer.rs`**: `FileWriter` for creating files
- **`reader.rs`**: `FileReader` with block-level caching
- **`item.rs`**: `Item<K, A>` serialization patterns

**Key structures**:
```rust
// Block-based format with 512-byte alignment
pub const DATA_BLOCK_MAGIC: [u8; 4] = *b"LFDB";
pub const INDEX_BLOCK_MAGIC: [u8; 4] = *b"LFIB";
pub const FILE_TRAILER_BLOCK_MAGIC: [u8; 4] = *b"LFFT";
```

### 5. Checkpoint/Restore (`crates/dbsp/src/circuit/checkpointer.rs`)

The `Checkpointer` manages persistent state:

```rust
pub struct Checkpointer {
    backend: Arc<dyn StorageBackend>,
    checkpoint_list: VecDeque<CheckpointMetadata>,
}
```

**Integration pattern** (from `Trace` trait in `trace.rs:284-293`):
```rust
pub trait Trace: BatchReader {
    fn save(
        &mut self,
        base: &StoragePath,
        pid: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error>;

    fn restore(&mut self, base: &StoragePath, pid: &str) -> Result<(), Error>;
}
```

### 6. Serialization (`rkyv` patterns)

Consistent serialization patterns across DBSP:

```rust
// From spine_async.rs:1514
let as_bytes = to_bytes(&committed).expect("Serializing should work.");
backend.write(&checkpoint_path, as_bytes)?;

// From spine_async.rs:1533
let content = Runtime::storage_backend().unwrap().read(&path)?;
let archived = unsafe { rkyv::archived_root::<CommittedSpine>(&content) };
let restored = archived.deserialize(&mut SharedDeserializeMap::new())?;
```

### 7. FBuf (`crates/storage/src/fbuf.rs`)

Buffer type for storage I/O:

```rust
pub struct FBuf { /* aligned buffer for storage I/O */ }
pub struct FBufSerializer { /* rkyv serializer into FBuf */ }
```

### Integration Summary Table

| Component | Location | What We'll Use |
|-----------|----------|----------------|
| `StorageBackend` | `storage/backend.rs` | File I/O for leaf storage |
| `BufferCache` | `storage/buffer_cache/cache.rs` | LRU eviction, adapt for leaves |
| `Runtime` | `circuit/runtime.rs` | `storage_backend()`, thresholds |
| `Checkpointer` | `circuit/checkpointer.rs` | Pattern for save/restore |
| `FileWriter/Reader` | `storage/file/` | Block-based file format |
| `FBuf` | `feldera_storage` | Aligned I/O buffers |
| `rkyv` | - | Serialization format |

## Design Rationale: Why "Pin Internals, LRU Leaves"

### Node Count Analysis

For a tree with N keys and branching factor B:

| Level | Node Count | % of Total | Access Frequency |
|-------|------------|------------|------------------|
| Root (0) | 1 | negligible | Every operation |
| Level 1 | ≤B | negligible | Every operation |
| Level 2 | ≤B² | <0.1% | Most operations |
| ... | ... | ... | ... |
| Leaves | N/B | ~98%+ | Sparse (by key range) |

**Example with B=64, N=10M keys:**
- Leaves: ~156,250 nodes (~98.5%)
- Internal: ~2,480 nodes (~1.5%)
- Memory for internals: ~2,480 × ~2KB ≈ 5MB (always fits in memory)

### Access Patterns

```
Query: select_kth(k) or rank(key)

  [Root]          <- Always accessed
    |
  [Internal]      <- Always accessed (on path)
   /    \
[Int] [Internal]  <- One accessed per query
       /   \
   [Leaf] [Leaf]  <- One accessed per query
```

Every query:
1. Traverses root (100% hit rate)
2. Traverses O(log_B N) internal nodes on path
3. Accesses exactly ONE leaf

**Conclusion**: Internal nodes have near-100% reuse; leaves have sparse access.

### Eviction Strategy Decision

| Strategy | Pros | Cons |
|----------|------|------|
| Breadth-first (root first) | None | Catastrophic - root needed for every op |
| Depth-first (leaves first) | Matches access pattern | Need to track levels |
| Pure LRU | Simple | May evict hot internal nodes |
| **Pin internals + LRU leaves** | Optimal for B+ tree | Slightly more complex |

**Chosen: Pin all internal nodes, LRU-evict leaves only**

This is simple and near-optimal because:
- Internal nodes are <2% of nodes but accessed constantly
- Leaves are >98% of nodes but accessed sparsely
- No need for complex weighted-LRU or level tracking

## Architecture

### High-Level Structure

```
┌─────────────────────────────────────────────────────────┐
│              OrderStatisticsMultiset<T>                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │              NodeStorage<T>                      │   │
│  │  ┌─────────────────┐  ┌─────────────────────┐   │   │
│  │  │ InternalNodes   │  │    LeafCache        │   │   │
│  │  │ (always pinned) │  │  (adapts existing   │   │   │
│  │  │                 │  │   BufferCache)      │   │   │
│  │  │ Vec<Internal>   │  │                     │   │   │
│  │  └─────────────────┘  └────────┬────────────┘   │   │
│  │                                │                 │   │
│  │                       ┌────────▼────────────┐   │   │
│  │                       │   LeafFileStore     │   │   │
│  │                       │  (uses Storage-     │   │   │
│  │                       │   Backend + rkyv)   │   │   │
│  │                       └─────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│                              │                          │
│              ┌───────────────┴───────────────┐         │
│              ▼                               ▼         │
│  ┌─────────────────────┐    ┌─────────────────────┐   │
│  │ Runtime::storage_   │    │ Checkpointer        │   │
│  │ backend()           │    │ (save/restore)      │   │
│  └─────────────────────┘    └─────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### Integration with Existing Components

```
                    OrderStatisticsMultiset
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
    ┌───────────────┐ ┌───────────────┐ ┌───────────────┐
    │ BufferCache   │ │ StorageBackend│ │ Checkpointer  │
    │ (LRU logic)   │ │ (file I/O)    │ │ (lifecycle)   │
    │               │ │               │ │               │
    │ Existing:     │ │ Existing:     │ │ Existing:     │
    │ CacheEntry    │ │ FileWriter    │ │ save/restore  │
    │ CacheKey      │ │ FileReader    │ │ patterns      │
    │ cost-based    │ │ FBuf          │ │               │
    │ eviction      │ │ StoragePath   │ │               │
    └───────────────┘ └───────────────┘ └───────────────┘
```

### Core Types

```rust
use crate::storage::backend::{StorageBackend, StoragePath, FileReader, FileWriter};
use crate::storage::buffer_cache::{BufferCache, CacheEntry, CacheStats};
use crate::circuit::runtime::Runtime;
use feldera_storage::FBuf;

/// Storage configuration - integrates with Runtime settings
pub struct NodeStorageConfig {
    /// Maximum memory for leaf cache (bytes)
    /// Default: from Runtime::min_index_storage_bytes() or 64MB
    pub max_leaf_cache_bytes: usize,

    /// Whether to enable disk spilling
    /// Default: Runtime::storage_backend().is_some()
    pub enable_spill: bool,

    /// Base path for spill files (uses StoragePath from feldera_storage)
    /// Default: derived from Runtime storage backend
    pub spill_base: Option<StoragePath>,
}

impl NodeStorageConfig {
    /// Create config from Runtime settings (like fallback/utils.rs pattern)
    pub fn from_runtime() -> Self {
        Self {
            max_leaf_cache_bytes: Runtime::min_index_storage_bytes()
                .unwrap_or(64 * 1024 * 1024),
            enable_spill: Runtime::storage_backend().is_some(),
            spill_base: None,  // Will use default from backend
        }
    }
}

/// Node location tracking
#[derive(Clone, Copy, Debug)]
enum NodeLocation {
    /// Node is in the pinned internal nodes vector at given index
    Internal(usize),
    /// Node is in the leaf cache (may be memory or disk)
    Leaf(LeafId),
}

/// Unique identifier for a leaf node (similar to FileId in storage backend)
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
struct LeafId(u64);

/// The main storage abstraction
pub struct NodeStorage<T: Ord + Clone> {
    /// Configuration
    config: NodeStorageConfig,

    /// Internal nodes - always in memory
    /// Index 0 = root (if tree has internal nodes)
    internal_nodes: Vec<InternalNodeTyped<T>>,

    /// Leaf node cache with LRU eviction
    /// Adapts the existing BufferCache pattern
    leaf_cache: LeafCache<T>,

    /// Mapping from old arena indices to new locations
    /// (needed during migration from Vec<Node<T>>)
    index_map: Vec<NodeLocation>,

    /// Statistics (compatible with CacheStats from buffer_cache)
    stats: StorageStats,
}

/// LRU cache for leaf nodes
/// Adapts patterns from crates/dbsp/src/storage/buffer_cache/cache.rs
struct LeafCache<T: Ord + Clone> {
    /// In-memory leaves with LRU tracking
    /// Uses same LRU pattern as BufferCache (serial numbers + BTreeMap)
    memory: BTreeMap<LeafId, LeafCacheEntry<T>>,

    /// LRU tracking: serial -> LeafId (same pattern as BufferCache)
    lru: BTreeMap<u64, LeafId>,

    /// Next serial number for LRU ordering
    next_serial: u64,

    /// Current memory usage in bytes
    memory_bytes: usize,

    /// Maximum memory usage (from config)
    max_bytes: usize,

    /// File store for evicted leaves (uses StorageBackend)
    file_store: Option<LeafFileStore<T>>,

    /// Next leaf ID to assign
    next_id: LeafId,
}

/// Cache entry for a leaf (implements pattern from CacheEntry trait)
struct LeafCacheEntry<T> {
    leaf: LeafNode<T>,
    serial: u64,  // For LRU tracking
    dirty: bool,  // Needs write-back on eviction
}

impl<T: Ord + Clone> LeafCacheEntry<T> {
    /// Cost function (same pattern as CacheEntry::cost())
    fn cost(&self) -> usize {
        std::mem::size_of::<LeafNode<T>>()
            + self.leaf.entries.capacity() * std::mem::size_of::<(T, ZWeight)>()
    }
}

/// File-based storage for evicted leaves
/// Uses StorageBackend and rkyv serialization (same as Spine)
struct LeafFileStore<T: Ord + Clone + Archive> {
    /// Storage backend (from Runtime::storage_backend())
    backend: Arc<dyn StorageBackend>,

    /// Base path for leaf files
    base_path: StoragePath,

    /// Index: LeafId -> file path
    /// Each leaf stored as separate small file (simpler than block management)
    /// Alternative: single file with offset index (like layer file format)
    index: HashMap<LeafId, StoragePath>,

    /// Phantom for T
    _phantom: PhantomData<T>,
}
```

### Node Storage Operations

```rust
impl<T: Ord + Clone> NodeStorage<T> {
    /// Create new storage with configuration
    pub fn new(config: NodeStorageConfig) -> Self;

    /// Create from existing arena (migration path)
    pub fn from_arena(
        nodes: Vec<Node<T>>,
        root: usize,
        config: NodeStorageConfig,
    ) -> (Self, NodeLocation);

    /// Get a node by location (may trigger disk read)
    pub fn get(&mut self, loc: NodeLocation) -> NodeRef<'_, T>;

    /// Get a node mutably (may trigger disk read, marks dirty)
    pub fn get_mut(&mut self, loc: NodeLocation) -> NodeRefMut<'_, T>;

    /// Allocate a new leaf node
    pub fn alloc_leaf(&mut self, leaf: LeafNode<T>) -> NodeLocation;

    /// Allocate a new internal node
    pub fn alloc_internal(&mut self, internal: InternalNodeTyped<T>) -> NodeLocation;

    /// Update internal node's subtree sum for a child
    pub fn update_subtree_sum(
        &mut self,
        internal_loc: NodeLocation,
        child_idx: usize,
        delta: ZWeight,
    );

    /// Flush all dirty nodes to disk
    pub fn flush(&mut self) -> Result<(), Error>;

    /// Get storage statistics
    pub fn stats(&self) -> &StorageStats;
}
```

### Leaf Cache Operations

```rust
impl<T: Ord + Clone> LeafCache<T> {
    /// Get leaf, loading from disk if needed
    fn get(&mut self, id: LeafId) -> &LeafNode<T> {
        if let Some(leaf) = self.memory.get(&id) {
            // Move to back (most recent)
            self.memory.to_back(&id);
            return leaf;
        }

        // Load from disk
        let leaf = self.file_store.as_mut()
            .expect("leaf not in memory but no file store")
            .load(id);

        self.insert_with_eviction(id, leaf);
        self.memory.get(&id).unwrap()
    }

    /// Get leaf mutably
    fn get_mut(&mut self, id: LeafId) -> &mut LeafNode<T> {
        // Similar to get(), but marks as dirty
        // ...
    }

    /// Insert leaf, evicting LRU entries if needed
    fn insert_with_eviction(&mut self, id: LeafId, leaf: LeafNode<T>) {
        let leaf_size = Self::estimate_size(&leaf);

        // Evict until we have space
        while self.memory_bytes + leaf_size > self.max_bytes
              && !self.memory.is_empty()
        {
            self.evict_lru();
        }

        self.memory.insert(id, leaf);
        self.memory_bytes += leaf_size;
    }

    /// Evict least recently used leaf to disk
    fn evict_lru(&mut self) {
        if let Some((id, leaf)) = self.memory.pop_front() {
            let leaf_size = Self::estimate_size(&leaf);
            self.memory_bytes -= leaf_size;

            if let Some(store) = &mut self.file_store {
                store.store(id, &leaf);
            }
        }
    }

    /// Estimate memory size of a leaf
    fn estimate_size(leaf: &LeafNode<T>) -> usize {
        std::mem::size_of::<LeafNode<T>>()
            + leaf.entries.capacity() * std::mem::size_of::<(T, ZWeight)>()
    }
}
```

## File Format

### Leaf File Structure

```
┌─────────────────────────────────────────┐
│ Header (64 bytes)                       │
│   magic: [u8; 8] = "OSMLEAF\0"         │
│   version: u32 = 1                      │
│   branching_factor: u32                 │
│   num_leaves: u64                       │
│   index_offset: u64                     │
│   reserved: [u8; 32]                    │
├─────────────────────────────────────────┤
│ Leaf Data Section                       │
│   ┌─────────────────────────────────┐   │
│   │ Leaf 0: rkyv serialized bytes   │   │
│   ├─────────────────────────────────┤   │
│   │ Leaf 1: rkyv serialized bytes   │   │
│   ├─────────────────────────────────┤   │
│   │ ...                             │   │
│   └─────────────────────────────────┘   │
├─────────────────────────────────────────┤
│ Index Section (at index_offset)         │
│   Array of (LeafId, offset, length)     │
│   Sorted by LeafId for binary search    │
└─────────────────────────────────────────┘
```

### Serialization

Uses rkyv patterns consistent with existing DBSP code (spine_async.rs, z1.rs, etc.):

```rust
use crate::storage::file::to_bytes;  // From storage/file.rs
use rkyv::{archived_root, Deserialize};
use rkyv::de::deserializers::SharedDeserializeMap;

impl<T: Ord + Clone + Archive> LeafFileStore<T>
where
    T::Archived: Deserialize<T, SharedDeserializeMap>,
    <T as Archive>::Archived: Ord,
{
    /// Store leaf to disk (pattern from spine_async.rs:1514)
    fn store(&mut self, id: LeafId, leaf: &LeafNode<T>) -> Result<(), Error> {
        // Use same serialization as Spine checkpoints
        let bytes = to_bytes(leaf).expect("Serializing LeafNode should work.");

        // Write via StorageBackend (same pattern as spine_async.rs:1515)
        let path = self.leaf_path(id);
        self.backend.write(&path, bytes)?;

        // Track in index
        self.index.insert(id, path);
        Ok(())
    }

    /// Load leaf from disk (pattern from spine_async.rs:1532-1534)
    fn load(&mut self, id: LeafId) -> Result<LeafNode<T>, Error> {
        let path = self.index.get(&id)
            .ok_or_else(|| Error::Runtime("leaf not in file store".into()))?;

        // Read via StorageBackend
        let content = self.backend.read(path)?;

        // Deserialize (same pattern as spine_async.rs:1533)
        let archived = unsafe { archived_root::<LeafNode<T>>(&content) };
        let leaf = archived
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("deserialization failed");

        Ok(leaf)
    }

    /// Generate path for a leaf (follows DBSP naming conventions)
    fn leaf_path(&self, id: LeafId) -> StoragePath {
        self.base_path.child(format!("leaf-{}.dat", id.0))
    }
}
```

**Alternative: Single-file storage with block index**

For higher performance with many leaves, could use layer file format pattern:

```rust
/// More efficient for many small leaves - uses block-based format
/// Similar to storage/file/format.rs
struct BlockBasedLeafStore<T> {
    /// Single file containing all leaves
    file: Arc<dyn FileReader>,
    writer: Option<Box<dyn FileWriter>>,

    /// Block index: LeafId -> BlockLocation
    /// Uses existing BlockLocation from storage/backend.rs
    index: HashMap<LeafId, BlockLocation>,

    /// Integrates with existing BufferCache for block caching
    cache: Arc<BufferCache>,
}
```

This matches the layer file infrastructure but adds complexity. Start with
simple per-leaf files, optimize to block-based if needed.
```

## Integration with OrderStatisticsMultiset

### Modified Structure

```rust
pub struct OrderStatisticsMultiset<T: Ord + Clone> {
    /// Node storage (replaces Vec<Node<T>>)
    storage: NodeStorage<T>,

    /// Location of root node (replaces usize index)
    root: Option<NodeLocation>,

    /// Total weight across all elements
    total_weight: ZWeight,

    /// Maximum entries per leaf node
    max_leaf_entries: usize,

    /// Maximum children per internal node
    max_internal_children: usize,

    /// Location of first leaf (for iteration)
    first_leaf: Option<NodeLocation>,

    /// Number of distinct keys
    num_keys: usize,
}
```

### Modified Operations

#### Insert

```rust
pub fn insert(&mut self, key: T, weight: ZWeight) {
    if weight == 0 {
        return;
    }

    match self.root {
        None => {
            // Create first leaf
            let mut leaf = LeafNode::with_capacity(self.max_leaf_entries);
            leaf.entries.push((key, weight));
            let loc = self.storage.alloc_leaf(leaf);
            self.root = Some(loc);
            self.first_leaf = Some(loc);
            self.total_weight = weight;
            self.num_keys = 1;
        }
        Some(root_loc) => {
            let (new_key_created, split_result) =
                self.insert_recursive(root_loc, key, weight);

            if new_key_created {
                self.num_keys += 1;
            }
            self.total_weight += weight;

            // Handle root split (similar to before, but with NodeLocation)
            if let Some((promoted_key, new_child_loc)) = split_result {
                // ... create new root internal node
            }
        }
    }
}
```

#### Select

```rust
pub fn select_kth(&mut self, k: ZWeight, ascending: bool) -> Option<&T> {
    // Note: &mut self because get() may trigger disk I/O
    let root_loc = self.root?;

    let effective_k = if ascending {
        k
    } else {
        self.total_weight.checked_sub(k + 1)?
    };

    if effective_k < 0 || effective_k >= self.total_weight {
        return None;
    }

    self.select_recursive(root_loc, effective_k)
}

fn select_recursive(&mut self, loc: NodeLocation, k: ZWeight) -> Option<&T> {
    match loc {
        NodeLocation::Internal(idx) => {
            let internal = &self.storage.internal_nodes[idx];
            let (child_loc, remaining) = internal.find_child_for_select(k)?;
            self.select_recursive(child_loc, remaining)
        }
        NodeLocation::Leaf(id) => {
            let leaf = self.storage.leaf_cache.get(id);
            leaf.select_kth(k)
        }
    }
}
```

### Checkpoint/Restore

Follows the `Trace::save`/`Trace::restore` pattern from `crates/dbsp/src/trace.rs:284-293`:

```rust
use crate::storage::file::to_bytes;
use feldera_storage::{FileCommitter, StoragePath};

/// Checkpoint data structure (follows CommittedSpine pattern from spine_async.rs)
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
struct CommittedOrderStatisticsMultiset<T: Archive> {
    total_weight: ZWeight,
    num_keys: usize,
    max_leaf_entries: usize,
    max_internal_children: usize,
    /// Internal nodes serialized directly (small, always fit)
    internal_nodes: Vec<InternalNodeTyped<T>>,
    /// Root location
    root_location: Option<NodeLocation>,
    /// First leaf location
    first_leaf: Option<NodeLocation>,
    /// Paths to leaf files (for FileCommitter tracking)
    leaf_files: Vec<String>,
    /// Dirty flag (same as Spine)
    dirty: bool,
}

impl<T: Ord + Clone + Archive> OrderStatisticsMultiset<T>
where
    <T as Archive>::Archived: Ord + Deserialize<T, SharedDeserializeMap>,
{
    /// Save to checkpoint (follows Trace::save pattern from spine_async.rs:1466-1527)
    pub fn save(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        // 1. Flush all dirty leaves to disk
        self.storage.flush()?;

        // 2. Collect leaf file paths for FileCommitter tracking
        let leaf_files: Vec<String> = self.storage.leaf_cache
            .file_store.as_ref()
            .map(|store| store.index.values().map(|p| p.to_string()).collect())
            .unwrap_or_default();

        // 3. Add leaf files to committer (same pattern as spine_async.rs:1499-1510)
        for path in &leaf_files {
            if let Some(store) = &self.storage.leaf_cache.file_store {
                let reader = store.backend.open(&path.clone().into())?;
                files.push(reader);
            }
        }

        // 4. Serialize checkpoint metadata
        let committed = CommittedOrderStatisticsMultiset {
            total_weight: self.total_weight,
            num_keys: self.num_keys,
            max_leaf_entries: self.max_leaf_entries,
            max_internal_children: self.max_internal_children,
            internal_nodes: self.storage.internal_nodes.clone(),
            root_location: self.root,
            first_leaf: self.first_leaf,
            leaf_files,
            dirty: self.dirty,
        };

        // 5. Write checkpoint file via StorageBackend
        let backend = Runtime::storage_backend().unwrap();
        let checkpoint_path = Self::checkpoint_file(base, persistent_id);
        let as_bytes = to_bytes(&committed).expect("Serializing checkpoint should work.");
        backend.write(&checkpoint_path, as_bytes)?;

        Ok(())
    }

    /// Restore from checkpoint (follows Trace::restore pattern)
    pub fn restore(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
    ) -> Result<(), Error> {
        let checkpoint_path = Self::checkpoint_file(base, persistent_id);

        // Read via StorageBackend (same pattern as spine_async.rs:1532)
        let content = Runtime::storage_backend().unwrap().read(&checkpoint_path)?;
        let archived = unsafe { archived_root::<CommittedOrderStatisticsMultiset<T>>(&content) };
        let committed: CommittedOrderStatisticsMultiset<T> = archived
            .deserialize(&mut SharedDeserializeMap::new())?;

        // Restore state
        self.total_weight = committed.total_weight;
        self.num_keys = committed.num_keys;
        self.max_leaf_entries = committed.max_leaf_entries;
        self.max_internal_children = committed.max_internal_children;
        self.dirty = committed.dirty;

        // Restore storage with leaf file references
        self.storage = NodeStorage::restore_from_checkpoint(
            committed.internal_nodes,
            committed.leaf_files,
            NodeStorageConfig::from_runtime(),
        )?;

        self.root = committed.root_location;
        self.first_leaf = committed.first_leaf;

        Ok(())
    }

    /// Generate checkpoint file path (follows DBSP conventions)
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        base.child(format!("osm-{}.dat", persistent_id))
    }
}
```

## Migration Path

### Phase 1: Bulk Load (Prerequisite)
- Implement `from_sorted_entries()`
- All existing code continues to work

### Phase 2: Storage Abstraction
1. Introduce `NodeStorage` trait/struct
2. Default implementation wraps `Vec<Node<T>>` (no behavior change)
3. All tests pass

### Phase 3: Leaf Cache
1. Implement `LeafCache` with in-memory only
2. Migrate leaf storage to use cache
3. Internal nodes stay in `Vec`
4. All tests pass

### Phase 4: Disk Spilling
1. Implement `LeafFileStore`
2. Connect to `LeafCache` eviction
3. Add configuration for memory limits
4. Integration tests with large datasets

### Phase 5: Integration
1. Connect to Runtime storage settings
2. Checkpoint/restore support
3. Metrics and observability
4. Documentation

## Configuration Integration

Uses the same Runtime configuration patterns as `fallback/utils.rs` and `spine_async.rs`:

```rust
use crate::Runtime;
use crate::storage::backend::StoragePath;

impl<T: Ord + Clone> OrderStatisticsMultiset<T> {
    /// Create with automatic storage configuration from Runtime
    /// (Same pattern as fallback batches that auto-detect storage settings)
    pub fn new() -> Self {
        let config = NodeStorageConfig::from_runtime();
        Self::with_config(DEFAULT_BRANCHING_FACTOR, config)
    }

    /// Create with explicit configuration
    pub fn with_config(branching_factor: usize, config: NodeStorageConfig) -> Self {
        Self {
            storage: NodeStorage::new(config),
            root: None,
            total_weight: 0,
            max_leaf_entries: branching_factor,
            max_internal_children: branching_factor,
            first_leaf: None,
            num_keys: 0,
        }
    }
}

impl NodeStorageConfig {
    /// Create config from Runtime settings
    /// Follows pattern from fallback/utils.rs:94 (pick_merge_destination)
    pub fn from_runtime() -> Self {
        // Check if storage is enabled (same check as spine_async.rs:1512)
        let storage_available = Runtime::storage_backend().is_some();

        // Use same threshold logic as fallback batches
        let threshold = Runtime::min_index_storage_bytes().unwrap_or(usize::MAX);

        Self {
            enable_spill: storage_available && threshold != usize::MAX,
            max_leaf_cache_bytes: match threshold {
                // If threshold is 0, always spill -> use small cache
                0 => 16 * 1024 * 1024,  // 16MB
                // If threshold is MAX, never spill -> large cache (doesn't matter)
                usize::MAX => usize::MAX,
                // Otherwise, cache up to threshold before spilling
                t => t,
            },
            spill_base: None,  // Will use Runtime::storage_backend() paths
        }
    }

    /// Create config that never spills (for testing or small datasets)
    pub fn memory_only() -> Self {
        Self {
            enable_spill: false,
            max_leaf_cache_bytes: usize::MAX,
            spill_base: None,
        }
    }

    /// Create config that always spills (for testing disk path)
    pub fn always_spill(base: StoragePath) -> Self {
        Self {
            enable_spill: true,
            max_leaf_cache_bytes: 0,  // Immediately spill
            spill_base: Some(base),
        }
    }
}
```

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_leaf_cache_eviction() {
    let config = NodeStorageConfig {
        max_leaf_cache_bytes: 1024, // Very small for testing
        enable_spill: true,
        spill_dir: Some(tempdir().path().to_owned()),
    };

    let mut cache = LeafCache::new(config);

    // Insert leaves until eviction happens
    for i in 0..100 {
        let leaf = LeafNode {
            entries: vec![(i, 1)],
            next_leaf: usize::MAX,
        };
        cache.insert(LeafId(i), leaf);
    }

    // Verify some leaves were evicted
    assert!(cache.file_store.as_ref().unwrap().index.len() > 0);

    // Verify all leaves still accessible
    for i in 0..100 {
        let leaf = cache.get(LeafId(i));
        assert_eq!(leaf.entries[0].0, i);
    }
}

#[test]
fn test_storage_large_tree() {
    let config = NodeStorageConfig {
        max_leaf_cache_bytes: 1024 * 1024, // 1MB
        enable_spill: true,
        spill_dir: Some(tempdir().path().to_owned()),
    };

    let mut tree = OrderStatisticsMultiset::with_config(config);

    // Insert 1M entries
    for i in 0..1_000_000 {
        tree.insert(i, 1);
    }

    // Verify correctness
    assert_eq!(tree.total_weight(), 1_000_000);
    assert_eq!(tree.select_kth(0, true), Some(&0));
    assert_eq!(tree.select_kth(999_999, true), Some(&999_999));

    // Check that spilling occurred
    assert!(tree.storage.stats().leaves_evicted > 0);
}

#[test]
fn test_checkpoint_restore_with_spill() {
    let dir = tempdir();
    let config = NodeStorageConfig {
        max_leaf_cache_bytes: 1024,
        enable_spill: true,
        spill_dir: Some(dir.path().to_owned()),
    };

    // Create and populate tree
    let mut tree = OrderStatisticsMultiset::with_config(config.clone());
    for i in 0..10_000 {
        tree.insert(i, 1);
    }

    // Checkpoint
    let checkpoint_path = dir.path().join("checkpoint");
    tree.checkpoint(&checkpoint_path).unwrap();

    // Restore
    let restored = OrderStatisticsMultiset::restore(
        &checkpoint_path,
        config
    ).unwrap();

    // Verify
    assert_eq!(tree.total_weight(), restored.total_weight());
    for i in 0..100 {
        assert_eq!(
            tree.select_kth(i * 100, true),
            restored.select_kth(i * 100, true)
        );
    }
}
```

### Benchmarks

```rust
fn bench_with_spill(c: &mut Criterion) {
    let mut group = c.benchmark_group("OrderStatisticsMultiset spill");

    for (cache_mb, total_entries) in [(1, 100_000), (10, 1_000_000), (100, 10_000_000)] {
        let config = NodeStorageConfig {
            max_leaf_cache_bytes: cache_mb * 1024 * 1024,
            enable_spill: true,
            spill_dir: Some(tempdir().path().to_owned()),
        };

        group.bench_function(
            format!("insert_{}entries_{}mb_cache", total_entries, cache_mb),
            |b| {
                b.iter(|| {
                    let mut tree = OrderStatisticsMultiset::with_config(config.clone());
                    for i in 0..total_entries {
                        tree.insert(i, 1);
                    }
                })
            },
        );

        // Setup tree for query benchmarks
        let mut tree = OrderStatisticsMultiset::with_config(config.clone());
        for i in 0..total_entries {
            tree.insert(i, 1);
        }

        group.bench_function(
            format!("select_{}entries_{}mb_cache", total_entries, cache_mb),
            |b| {
                let mut rng = rand::thread_rng();
                b.iter(|| {
                    let k = rng.gen_range(0..total_entries as i64);
                    tree.select_kth(k, true)
                })
            },
        );
    }

    group.finish();
}
```

## Memory Budget Analysis

### Scenario: 10M keys, B=64, 8-byte keys

| Component | Count | Size Each | Total |
|-----------|-------|-----------|-------|
| Internal nodes | ~2,480 | ~2KB | ~5MB |
| Leaf nodes (all) | ~156,250 | ~1KB | ~156MB |
| With 64MB cache | - | - | ~69MB total |

### Eviction Behavior

With 64MB leaf cache and 156MB total leaves:
- ~41% of leaves fit in cache
- Worst case: every query misses cache → 1 disk read
- With locality: hot leaves stay cached, cold leaves on disk

### Tuning Recommendations

| Workload | Cache Size | Rationale |
|----------|------------|-----------|
| Small (<1M keys) | Disable spill | Everything fits in memory |
| Medium (1-10M keys) | 64-256MB | Good hit rate for typical access |
| Large (>10M keys) | 256MB-1GB | Balance memory vs I/O |
| Memory-constrained | 16-64MB | Accept higher I/O for lower memory |

## Implementation Checklist

### Phase 1: Bulk Load (prerequisite)
- [ ] Implement `from_sorted_entries()`
- [ ] Update deserialization to use bulk load
- [ ] Update `compact()` to use bulk load
- [ ] Tests and benchmarks

### Phase 2: Storage Abstraction
- [ ] Define `NodeLocation` enum
- [ ] Define `NodeStorage` struct with `NodeStorageConfig`
- [ ] Implement wrapper over `Vec<Node<T>>` (no behavior change)
- [ ] Update `OrderStatisticsMultiset` to use `NodeStorage`
- [ ] All existing tests pass (no regressions)

### Phase 3: Leaf Cache (integrate with existing buffer_cache patterns)
- [ ] Implement `LeafCache` struct using LRU pattern from `buffer_cache/cache.rs`
- [ ] Implement `LeafCacheEntry` with `cost()` method (like `CacheEntry` trait)
- [ ] Use `BTreeMap` + serial number pattern from `CacheInner`
- [ ] Size estimation for leaves
- [ ] Eviction logic (memory-only first)
- [ ] Tests for cache behavior

### Phase 4: Disk Spilling (integrate with StorageBackend)
- [ ] Implement `LeafFileStore` using `Runtime::storage_backend()`
- [ ] Use `StoragePath` for file naming
- [ ] Use rkyv serialization (same pattern as `spine_async.rs`)
- [ ] Connect cache eviction to file store
- [ ] Implement load from file via `backend.read()`
- [ ] Tests with disk I/O

### Phase 5: Integration with DBSP Infrastructure
- [ ] Connect to `Runtime::min_index_storage_bytes()` for thresholds
- [ ] Implement `save()` following `Trace::save` pattern
- [ ] Implement `restore()` following `Trace::restore` pattern
- [ ] Add `FileCommitter` tracking for checkpoint files
- [ ] Implement `StorageStats` compatible with `CacheStats`
- [ ] Documentation
- [ ] Integration tests with `Checkpointer`
- [ ] Benchmarks comparing memory-only vs spilling

## Files to Create/Modify

### New Files
- `crates/dbsp/src/algebra/order_statistics_storage.rs` - NodeStorage implementation
- `crates/dbsp/src/algebra/order_statistics_leaf_cache.rs` - LeafCache + LeafFileStore
- `crates/dbsp/src/algebra/order_statistics_file_format.rs` - File format definitions (optional, if block-based)

### Modified Files
- `crates/dbsp/src/algebra/order_statistics_multiset.rs` - Use NodeStorage
- `crates/dbsp/src/algebra.rs` - Export new modules

### Existing Files to Reference/Import From

| File | What We'll Use |
|------|----------------|
| `crates/dbsp/src/storage/backend.rs` | `StorageBackend`, `StoragePath`, `FileReader`, `FileWriter` |
| `crates/dbsp/src/storage/buffer_cache/cache.rs` | LRU pattern (serial numbers, BTreeMap), `CacheEntry` trait |
| `crates/dbsp/src/storage/file.rs` | `to_bytes()` helper for rkyv serialization |
| `crates/dbsp/src/circuit/runtime.rs` | `Runtime::storage_backend()`, `Runtime::min_index_storage_bytes()` |
| `crates/dbsp/src/trace.rs` | `Trace::save`/`restore` signature pattern |
| `crates/dbsp/src/trace/spine_async.rs` | Reference implementation for checkpoint patterns |
| `crates/dbsp/src/trace/ord/fallback/utils.rs` | `pick_merge_destination()` pattern for threshold decisions |
| `crates/storage/src/fbuf.rs` | `FBuf` for aligned I/O buffers |

## Open Questions

1. **Concurrent access**: Should `NodeStorage` support concurrent reads?
   - Current: Single-threaded per tree (matches existing design)
   - Future: Could add RwLock for read-heavy workloads
   - Reference: `BufferCache` uses `Mutex<CacheInner>` for thread-safety

2. **Compaction**: How to handle file fragmentation over time?
   - Option A: Background compaction thread (like `AsyncMerger` in spine_async.rs)
   - Option B: Compact on checkpoint (simple, follows checkpoint/restore pattern)
   - Option C: Accept fragmentation (simplest, start here)
   - Reference: Spine uses background merge thread for similar compaction

3. **Memory accounting**: How to integrate with DBSP memory budgets?
   - Reference: `StorageBackend::usage()` tracks storage usage atomically
   - Reference: `CacheStats` tracks cache hit/miss rates
   - Can expose `StorageStats` through same patterns

4. **Shared storage**: Should multiple trees share a file store?
   - Pro: Better space utilization
   - Con: Complexity, coordination overhead
   - Recommendation: One file store per tree (simpler)
   - Reference: Each `Spine` has its own `AsyncMerger` (not shared)

5. **Block-based vs file-per-leaf storage**:
   - File-per-leaf: Simpler, uses `backend.read()`/`backend.write()` directly
   - Block-based: More efficient, could reuse layer file format from `storage/file/format.rs`
   - Recommendation: Start with file-per-leaf, optimize if needed
   - Reference: Layer files use 512-byte blocks with index/data separation
