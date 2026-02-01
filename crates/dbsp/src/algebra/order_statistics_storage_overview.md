# OrderStatisticsMultiset Storage Implementation

## Overview

This document describes the disk-backed storage implementation for `OrderStatisticsMultiset`, an augmented B+ tree used for O(log n) rank/select queries in percentile aggregation. The implementation supports level-based disk spilling with configurable policies, integrating with DBSP's storage infrastructure.

## Architecture

### Design Rationale

The storage design exploits B+ tree access patterns and supports level-based spilling:

**Default behavior (max_spillable_level = 0):**

| Node Type | % of Nodes | Access Pattern | Strategy |
|-----------|------------|----------------|----------|
| Internal | ~2% | Every operation | Always pinned in memory |
| Leaves | ~98% | Sparse by key range | Spill to disk, LRU cache |

For a tree with 10M keys (B=64): internal nodes use ~5MB (always fits), leaves use ~156MB (spilled as needed).

**Level-based spilling:**

Nodes are organized by level (0 = leaves, 1 = parents of leaves, etc.). The `max_spillable_level` config controls which levels can be spilled:
- `max_spillable_level = 0`: Only leaves can be spilled (default, most efficient)
- `max_spillable_level = 1`: Leaves and their direct parents can be spilled
- `max_spillable_level = u8::MAX`: All nodes can be spilled (aggressive memory savings)

Higher values allow more aggressive memory reclamation but may cause cascading disk reads.

### Component Structure

```
┌─────────────────────────────────────────────────────────┐
│              OrderStatisticsMultiset<T>                 │
│  ┌─────────────────────────────────────────────────┐   │
│  │              NodeStorage<T>                      │   │
│  │  ┌─────────────────┐  ┌─────────────────────┐   │   │
│  │  │ InternalNodes   │  │    Leaves           │   │   │
│  │  │ (always pinned) │  │  Vec<Option<Leaf>>  │   │   │
│  │  │ Vec<Internal>   │  │  (evictable)        │   │   │
│  │  └─────────────────┘  └────────┬────────────┘   │   │
│  │                                │                 │   │
│  │                       ┌────────▼────────────┐   │   │
│  │                       │     LeafFile        │   │   │
│  │                       │  (block-based I/O)  │   │   │
│  │                       └─────────────────────┘   │   │
│  └─────────────────────────────────────────────────┘   │
│                              │                          │
│              ┌───────────────┴───────────────┐         │
│              ▼                               ▼         │
│  ┌─────────────────────┐    ┌─────────────────────┐   │
│  │   BufferCache       │    │   StorageBackend    │   │
│  │   (LRU caching)     │    │   (file I/O)        │   │
│  └─────────────────────┘    └─────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

## Core Types

### StorableNode Trait

Unified trait for all tree nodes that can be stored in NodeStorage:

```rust
/// Unified trait for tree nodes that can be stored in NodeStorage.
/// All nodes must be serializable to support:
/// - Checkpointing (save/restore of tree state)
/// - Level-based disk spilling (configurable via `max_spillable_level`)
pub trait StorableNode:
    Clone + Debug + SizeOf + Send + Sync + 'static
    + Archive + for<'a> RkyvSerialize<AllocSerializer<4096>>
where
    Self::Archived: RkyvDeserialize<Self, rkyv::Infallible>,
{
    fn estimate_size(&self) -> usize;
}
```

Default implementations:
- `InternalNodeTyped<T>` implements `StorableNode`
- `LeafNode<T>` implements `StorableNode`

This unified trait enables:
- Consistent size estimation for memory accounting
- Full serialization support for both checkpointing and disk spilling
- Level-based spilling where any level of nodes can be spilled (configured via `max_spillable_level`)
- Future extensibility for custom node types

### NodeLocation

Strongly-typed node location tracking with level information:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeLocation {
    Internal { id: usize, level: u8 },  // Index into internal nodes vector + level
    Leaf(LeafLocation),                  // Leaf location (level = 0)
}

impl NodeLocation {
    /// Returns the level of this node.
    /// Level 0 = leaves, Level 1 = parents of leaves, etc.
    pub fn level(&self) -> u8;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LeafLocation {
    pub id: usize,        // Unique leaf identifier
}
```

### NodeStorageConfig

Configuration for storage behavior:

```rust
pub struct NodeStorageConfig {
    /// Enable disk spilling
    pub enable_spill: bool,

    /// Maximum node level that can be spilled to disk.
    /// - Level 0 = leaf nodes only (default, most efficient)
    /// - Level 1 = leaves + their direct parents
    /// - Level N = all nodes up to N levels from leaves
    pub max_spillable_level: u8,

    /// Bytes of dirty data before triggering flush
    pub spill_threshold_bytes: usize,

    /// Directory for spill files (optional)
    pub spill_directory: Option<PathBuf>,

    /// Storage backend for file I/O
    pub storage_backend: Option<Arc<dyn StorageBackend>>,

    /// Buffer cache for evicted leaf caching
    pub buffer_cache: Option<Arc<BufferCache>>,
}

impl NodeStorageConfig {
    /// Auto-configure from DBSP Runtime settings
    pub fn from_runtime() -> Self;

    /// Memory-only configuration (no spilling)
    pub fn memory_only() -> Self;

    /// Configure with specific threshold
    pub fn with_threshold(bytes: usize) -> Self;

    /// Check if a node at the given level can be spilled
    pub fn can_spill_level(&self, level: u8) -> bool {
        self.enable_spill && level <= self.max_spillable_level
    }
}
```

### NodeStorage<T>

Main storage abstraction separating internal nodes from leaves:

```rust
pub struct NodeStorage<T> {
    // Configuration
    config: NodeStorageConfig,

    // Internal nodes (always in memory)
    internal_nodes: Vec<InternalNodeWithMeta<T>>,

    // Leaf nodes (Some = in memory, None = evicted)
    leaves: Vec<Option<LeafNode<T>>>,

    // Dirty tracking
    dirty_leaves: HashSet<usize>,
    dirty_bytes: usize,

    // Eviction tracking
    evicted_leaves: HashSet<usize>,
    spilled_leaves: HashSet<usize>,

    // Disk storage
    spill_file_path: Option<PathBuf>,
    spill_file_id: Option<FileId>,
    leaf_block_locations: HashMap<usize, (u64, u32)>,

    // Statistics
    stats: StorageStats,
}
```

## Node Access API

### Allocation

```rust
impl<T> NodeStorage<T> {
    /// Allocate new leaf (marked dirty)
    pub fn alloc_leaf(&mut self, leaf: LeafNode<T>) -> NodeLocation;

    /// Allocate new internal node (marked dirty)
    pub fn alloc_internal(&mut self, node: InternalNodeTyped<T>) -> NodeLocation;
}
```

### Read Access

```rust
impl<T> NodeStorage<T> {
    /// Get node reference (immutable)
    pub fn get(&self, loc: NodeLocation) -> NodeRef<'_, T>;

    /// Get leaf directly
    pub fn get_leaf(&self, loc: LeafLocation) -> &LeafNode<T>;

    /// Get leaf, reloading from disk if evicted
    pub fn get_leaf_reloading(&mut self, loc: LeafLocation) -> &LeafNode<T>;

    /// Get internal node directly
    pub fn get_internal(&self, idx: usize) -> &InternalNodeTyped<T>;
}
```

### Write Access

```rust
impl<T> NodeStorage<T> {
    /// Get mutable node reference (marks dirty)
    pub fn get_mut(&mut self, loc: NodeLocation) -> NodeRefMut<'_, T>;

    /// Get mutable leaf (marks dirty)
    pub fn get_leaf_mut(&mut self, loc: LeafLocation) -> &mut LeafNode<T>;

    /// Get mutable internal node (marks dirty)
    pub fn get_internal_mut(&mut self, idx: usize) -> &mut InternalNodeTyped<T>;
}
```

## Dirty Tracking

Copy-on-write semantics for incremental persistence:

```rust
impl<T> NodeStorage<T> {
    /// Check if any nodes are dirty
    pub fn has_dirty_nodes(&self) -> bool;

    /// Count of dirty leaves
    pub fn dirty_leaf_count(&self) -> usize;

    /// Total bytes of dirty data
    pub fn dirty_bytes(&self) -> usize;

    /// Check if threshold exceeded
    pub fn should_flush(&self) -> bool;

    /// Mark all nodes as clean (after persistence)
    pub fn mark_all_clean(&mut self);
}
```

## Disk Spilling

### Flushing to Disk

```rust
impl<T> NodeStorage<T> {
    /// Flush dirty leaves to disk file
    /// Returns number of leaves written
    pub fn flush_dirty_to_disk(
        &mut self,
        path: Option<&Path>,
    ) -> Result<usize, FileFormatError>;

    /// Get current spill file path
    pub fn spill_file_path(&self) -> Option<&Path>;

    /// Check if leaf has been spilled
    pub fn is_leaf_spilled(&self, id: usize) -> bool;

    /// Count of spilled leaves
    pub fn spilled_leaf_count(&self) -> usize;
}
```

### Loading from Disk

```rust
impl<T> NodeStorage<T> {
    /// Load leaf from disk (checks BufferCache first)
    pub fn load_leaf_from_disk(
        &mut self,
        loc: LeafLocation,
    ) -> Result<&LeafNode<T>, FileFormatError>;

    /// Reload all spilled leaves into memory
    pub fn reload_spilled_leaves(&mut self) -> Result<usize, FileFormatError>;
}
```

## Memory Eviction

Leaves can be evicted from memory after flushing to disk:

```rust
impl<T> NodeStorage<T> {
    /// Evict clean, spilled leaves from memory
    /// Returns (count evicted, bytes freed)
    pub fn evict_clean_leaves(&mut self) -> (usize, usize);

    /// Check if leaf is evicted
    pub fn is_leaf_evicted(&self, loc: LeafLocation) -> bool;

    /// Check if leaf is in memory
    pub fn is_leaf_in_memory(&self, loc: LeafLocation) -> bool;

    /// Count of evicted leaves
    pub fn evicted_leaf_count(&self) -> usize;

    /// Count of in-memory leaves
    pub fn in_memory_leaf_count(&self) -> usize;

    /// Reload all evicted leaves
    pub fn reload_evicted_leaves(&mut self) -> Result<usize, FileFormatError>;
}
```

**Eviction Rules:**
- Only clean leaves can be evicted (dirty leaves must stay in memory)
- Only spilled leaves can be evicted (must have disk copy)
- Evicted leaves auto-reload on access via `get_leaf_reloading()`

## BufferCache Integration

Evicted leaves are cached in DBSP's global BufferCache for faster reload:

```rust
/// Wrapper for caching leaves in BufferCache
pub struct CachedLeafNode<T> {
    pub leaf: LeafNode<T>,
    pub size_bytes: usize,
}

impl<T> CacheEntry for CachedLeafNode<T> {
    fn cost(&self) -> usize {
        self.size_bytes
    }
}
```

**Cache Flow:**
1. `flush_dirty_to_disk()` records FileId and block locations
2. `load_leaf_from_disk()` checks BufferCache first (using FileId + offset as key)
3. On cache miss, loads from disk and inserts into BufferCache
4. `cleanup_spill_file()` evicts all entries for the file

## Backpressure

Prevents unbounded memory growth during heavy writes:

```rust
impl<T> NodeStorage<T> {
    /// Returns true when dirty_bytes >= 2x threshold
    pub fn should_apply_backpressure(&self) -> bool;

    /// Returns true when dirty_bytes < threshold
    pub fn should_relieve_backpressure(&self) -> bool;

    /// Returns (should_apply, ratio) for monitoring
    pub fn backpressure_status(&self) -> (bool, f64);
}
```

## Cleanup

```rust
impl<T> NodeStorage<T> {
    /// Delete spill file and clear tracking state
    /// Warning: evicted leaves will be lost!
    pub fn cleanup_spill_file(&mut self) -> Result<(), std::io::Error>;

    /// Clear all storage
    pub fn clear(&mut self);
}
```

## File Format

Block-based format with 512-byte alignment and CRC32C checksums:

```
┌─────────────────────────────────────────────────────┐
│ File Header (512 bytes)                             │
│   checksum: u32 (CRC32C of bytes [4..512])         │
│   magic: "OSML" (Order Statistics Multiset Leaf)   │
│   version: u32 (= 1)                                │
│   num_leaves: u64                                   │
│   index_offset: u64                                 │
│   total_entries: u64                                │
│   total_weight: i64                                 │
│   reserved: [u8; 468]                               │
├─────────────────────────────────────────────────────┤
│ Data Blocks (512-byte aligned)                      │
│   [checksum:4][magic:4][leaf_id:8][data_len:8]     │
│   [rkyv serialized LeafNode<T>]                    │
│   [padding to 512-byte boundary]                   │
├─────────────────────────────────────────────────────┤
│ Index Block (at index_offset)                       │
│   checksum + magic "OSMI" + num_entries            │
│   Array of (leaf_id, offset, size) entries         │
└─────────────────────────────────────────────────────┘
```

## LeafFile API

```rust
pub struct LeafFile<T> {
    path: PathBuf,
    file: Option<File>,
    index: HashMap<u64, FileBlockLocation>,
    header: FileHeader,
    // ...
}

impl<T> LeafFile<T> {
    /// Create new file for writing
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, FileFormatError>;

    /// Open existing file for reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, FileFormatError>;

    /// Write leaf to file
    pub fn write_leaf(
        &mut self,
        leaf_id: u64,
        leaf: &LeafNode<T>,
    ) -> Result<FileBlockLocation, FileFormatError>;

    /// Load leaf by ID
    pub fn load_leaf(&mut self, leaf_id: u64) -> Result<LeafNode<T>, FileFormatError>;

    /// Write index and finalize file
    pub fn finalize(&mut self) -> Result<(), FileFormatError>;

    /// Check if leaf exists
    pub fn contains(&self, leaf_id: u64) -> bool;

    /// Number of leaves in file
    pub fn num_leaves(&self) -> u64;
}
```

## Statistics

```rust
#[derive(Default, Clone, Debug)]
pub struct StorageStats {
    pub internal_node_count: usize,
    pub leaf_node_count: usize,
    pub total_entries: usize,
    pub memory_bytes: usize,
    pub dirty_internal_count: usize,
    pub dirty_leaf_count: usize,
    pub dirty_bytes: usize,
    pub evicted_leaf_count: usize,
    pub evicted_bytes: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub disk_writes: u64,
}
```

## OrderStatisticsMultiset Integration

The tree uses NodeStorage internally:

```rust
pub struct OrderStatisticsMultiset<T> {
    storage: NodeStorage<T>,
    root: Option<NodeLocation>,
    first_leaf: Option<LeafLocation>,
    total_weight: ZWeight,
    num_keys: usize,
    // ...
}
```

Key type changes from arena-based storage:
- `InternalNodeTyped.children: Vec<NodeLocation>` (was `Vec<usize>`)
- `LeafNode.next_leaf: Option<LeafLocation>` (was `usize`)

### Checkpoint/Restore

```rust
impl<T> OrderStatisticsMultiset<T> {
    /// Save tree state to checkpoint
    pub fn save(
        &self,
        base: &StoragePath,
        persistent_id: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error>;

    /// Restore tree from checkpoint
    pub fn restore(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
    ) -> Result<(), Error>;
}
```

Uses rkyv serialization and O(n) bulk reconstruction via `from_sorted_entries()`.

## Usage Example

```rust
use dbsp::algebra::{OrderStatisticsMultiset, NodeStorageConfig};

// Create with auto-configuration from Runtime
let mut tree = OrderStatisticsMultiset::<i32>::new();

// Or with explicit configuration
let config = NodeStorageConfig::with_threshold(64 * 1024 * 1024);
let mut tree = OrderStatisticsMultiset::with_storage_config(64, config);

// Insert elements
for i in 0..1_000_000 {
    tree.insert(i, 1);
}

// Check backpressure and flush if needed
if tree.storage().should_apply_backpressure() {
    tree.storage_mut().flush_dirty_to_disk(None)?;
    tree.storage_mut().evict_clean_leaves();
}

// Query operations (auto-reload evicted leaves)
let median = tree.select_kth(500_000, true);
let rank = tree.rank(&500_000);
```

## Test Coverage

79 tests covering:
- NodeStorage basic operations
- Dirty tracking and threshold checks
- LeafFile read/write roundtrips
- Large leaves, many leaves, string keys
- Negative weights, empty files
- NodeStorage + LeafFile integration
- Memory eviction and reload
- BufferCache integration

## Files

| File | Contents |
|------|----------|
| `order_statistics_storage.rs` | `NodeStorage`, `LeafFile`, `CachedLeafNode` |
| `order_statistics_file_format.rs` | Block format constants, `FileHeader`, `IndexEntry` |
| `order_statistics_multiset.rs` | `OrderStatisticsMultiset`, `LeafNode`, `InternalNodeTyped` |
