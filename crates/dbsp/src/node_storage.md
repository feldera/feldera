# Generic Node Storage for B+ Tree Structures

## Overview

This document describes the generic disk-backed storage implementation for **B+ tree-like data structures**. The `NodeStorage<I, L>` abstraction separates internal nodes from leaf nodes and supports level-based disk spilling with configurable policies, integrating with DBSP's storage infrastructure.

The storage is designed for trees where **data is stored exclusively in leaves**, while internal nodes serve only as navigation/index structures. This is the classic B+ tree property, distinct from B-trees where internal nodes also contain data.

The primary use case is `OrderStatisticsMultiset`, an augmented B+ tree used for O(log n) rank/select queries in percentile aggregation. The generic design enables reuse for other B+ tree variants like interval trees or segment trees.

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
│  │        OsmNodeStorage<T> (type alias)           │   │
│  │    = NodeStorage<InternalNodeTyped<T>,          │   │
│  │                   LeafNode<T>>                  │   │
│  │  ┌─────────────────┐  ┌─────────────────────┐   │   │
│  │  │ InternalNodes   │  │    Leaves           │   │   │
│  │  │ (always pinned) │  │  Vec<LeafSlot<L>>   │   │   │
│  │  │ Vec<I>          │  │  (evictable)        │   │   │
│  │  └─────────────────┘  └────────┬────────────┘   │   │
│  │                                │                 │   │
│  │                       ┌────────▼────────────┐   │   │
│  │                       │     LeafFile<L>     │   │   │
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

## Core Traits

### StorableNode Trait

Base trait for all tree nodes that can be stored in NodeStorage:

```rust
/// Base trait for tree nodes that can be stored in NodeStorage.
///
/// This trait defines the minimal interface for tree nodes. Serialization
/// bounds are added separately where needed (e.g., for disk spilling) to
/// keep the base trait simple.
pub trait StorableNode:
    Clone + Debug + SizeOf + Send + Sync + 'static
{
    /// Estimate the memory size of this node.
    fn estimate_size(&self) -> usize;
}
```

Default implementations:
- `InternalNodeTyped<T>` implements `StorableNode`
- `LeafNode<T>` implements `StorableNode`

### LeafNodeOps Trait

Trait for leaf-specific operations required by the storage layer:

```rust
/// Operations specific to leaf nodes.
///
/// This trait defines leaf-specific behavior that the generic storage
/// layer needs to interact with leaves.
pub trait LeafNodeOps {
    /// Key type stored in the leaf
    type Key;

    /// Compute total weight of all entries in this leaf
    fn total_weight(&self) -> ZWeight;

    /// Number of entries in this leaf
    fn entry_count(&self) -> usize;

    /// Iterate over entries as (key, weight) pairs
    fn entries(&self) -> impl Iterator<Item = (&Self::Key, ZWeight)>;
}
```

## Core Types

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

### LeafSlot<L>

Enum representing a leaf's state in memory:

```rust
/// Represents the state of a leaf slot in NodeStorage.
///
/// Each leaf is either present in memory or evicted to disk with
/// cached summary information for checkpoint support.
pub enum LeafSlot<L> {
    /// Leaf is in memory
    Present(L),
    /// Leaf is evicted to disk, with cached summary for checkpoints
    Evicted(CachedLeafSummary),
}

/// Cached summary of an evicted leaf.
///
/// Stores enough information to generate LeafSummary for checkpoints
/// without reloading the leaf from disk.
pub struct CachedLeafSummary {
    pub first_key_bytes: Vec<u8>,  // rkyv-serialized first key
    pub has_first_key: bool,       // false if leaf was empty
    pub weight_sum: ZWeight,
    pub entry_count: usize,
}

impl<L> LeafSlot<L> {
    pub fn is_present(&self) -> bool;
    pub fn is_evicted(&self) -> bool;
    pub fn as_present(&self) -> Option<&L>;
    pub fn as_present_mut(&mut self) -> Option<&mut L>;
    pub fn as_evicted(&self) -> Option<&CachedLeafSummary>;
}
```

### NodeStorage<I, L>

Generic storage abstraction separating internal nodes from leaves:

```rust
/// Generic node-level storage for tree data structures.
///
/// `NodeStorage<I, L>` is generic over:
/// - `I: StorableNode` - internal node type
/// - `L: StorableNode + LeafNodeOps` - leaf node type
///
/// This follows the Spine-Trace pattern from DBSP where storage is
/// decoupled from the specific data structure using it.
pub struct NodeStorage<I, L> {
    // Configuration
    config: NodeStorageConfig,

    // Internal nodes (always in memory)
    internal_nodes: Vec<NodeWithMeta<I>>,

    // Leaf nodes with state tracking
    // - Present(L): leaf is in memory
    // - Evicted(summary): leaf is on disk, summary cached for checkpoints
    leaves: Vec<LeafSlot<L>>,

    // Dirty tracking
    dirty_leaves: HashSet<usize>,
    dirty_bytes: usize,

    // Spill tracking (leaves written to disk)
    spilled_leaves: HashSet<usize>,

    // Disk storage
    spill_file_path: Option<PathBuf>,
    spill_file_id: Option<FileId>,
    leaf_block_locations: HashMap<usize, (u64, u32)>,

    // Statistics (evicted_leaf_count maintained incrementally)
    stats: StorageStats,
}

/// Convenience type alias for OrderStatisticsMultiset storage.
pub type OsmNodeStorage<T> = NodeStorage<InternalNodeTyped<T>, LeafNode<T>>;
```

## Node Access API

### Allocation

```rust
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps,
{
    /// Allocate new leaf (marked dirty)
    pub fn alloc_leaf(&mut self, leaf: L) -> NodeLocation;

    /// Allocate new internal node at given level (marked dirty)
    pub fn alloc_internal(&mut self, node: I, level: u8) -> NodeLocation;
}
```

### Read Access

```rust
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps,
{
    /// Get node reference (immutable)
    pub fn get(&self, loc: NodeLocation) -> NodeRef<'_, I, L>;

    /// Get leaf directly
    pub fn get_leaf(&self, loc: LeafLocation) -> &L;

    /// Get leaf, reloading from disk if evicted
    pub fn get_leaf_reloading(&mut self, loc: LeafLocation) -> &L;

    /// Get internal node directly
    pub fn get_internal(&self, idx: usize) -> &I;
}
```

### Write Access

```rust
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps,
{
    /// Get mutable node reference (marks dirty)
    pub fn get_mut(&mut self, loc: NodeLocation) -> NodeRefMut<'_, I, L>;

    /// Get mutable leaf (marks dirty)
    pub fn get_leaf_mut(&mut self, loc: LeafLocation) -> &mut L;

    /// Get mutable internal node (marks dirty)
    pub fn get_internal_mut(&mut self, idx: usize) -> &mut I;
}
```

### Node References

Generic reference types for accessing nodes:

```rust
/// Immutable reference to a node.
pub enum NodeRef<'a, I, L> {
    Internal(&'a I),
    Leaf(&'a L),
}

impl<'a, I, L> NodeRef<'a, I, L> {
    pub fn is_leaf(&self) -> bool;
}

impl<'a, I, L: LeafNodeOps> NodeRef<'a, I, L> {
    /// Get total weight (only for leaf nodes)
    pub fn leaf_total_weight(&self) -> Option<ZWeight>;
}

/// Mutable reference to a node.
pub enum NodeRefMut<'a, I, L> {
    Internal(&'a mut I),
    Leaf(&'a mut L),
}
```

## Dirty Tracking

Copy-on-write semantics for incremental persistence:

```rust
impl<I, L> NodeStorage<I, L> {
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
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps + Archive + RkyvSerialize<Serializer>,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
{
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
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps + Archive + RkyvSerialize<Serializer>,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
{
    /// Load leaf from disk (checks BufferCache first)
    pub fn load_leaf_from_disk(
        &mut self,
        loc: LeafLocation,
    ) -> Result<&L, FileFormatError>;

    /// Reload all spilled leaves into memory
    pub fn reload_spilled_leaves(&mut self) -> Result<usize, FileFormatError>;
}
```

## Memory Eviction

Leaves can be evicted from memory after flushing to disk. **Flush and eviction are decoupled**:
- `flush_dirty_to_disk()` writes leaves to disk and marks them clean (durability)
- `evict_clean_leaves()` removes clean, spilled leaves from memory (memory management)

This separation allows flushing without eviction, which is important for checkpoints to keep hot leaves in memory for immediate runtime resumption.

```rust
impl<I, L> NodeStorage<I, L> {
    /// Evict clean, spilled leaves from memory.
    /// Captures summary (first_key, weight_sum, entry_count) at eviction time
    /// for checkpoint support without reloading.
    /// Returns (count evicted, bytes freed)
    pub fn evict_clean_leaves(&mut self) -> (usize, usize);

    /// Check if leaf is evicted
    pub fn is_leaf_evicted(&self, loc: LeafLocation) -> bool;

    /// Check if leaf is in memory
    pub fn is_leaf_in_memory(&self, loc: LeafLocation) -> bool;

    /// Count of evicted leaves (O(1) - maintained incrementally)
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
- Summary is captured at eviction time for checkpoint support

## BufferCache Integration

Evicted leaves are cached in DBSP's global BufferCache for faster reload:

```rust
/// Wrapper for caching leaves in BufferCache
pub struct CachedLeafNode<L> {
    pub leaf: L,
    pub size_bytes: usize,
}

impl<L> CacheEntry for CachedLeafNode<L> {
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
impl<I, L> NodeStorage<I, L> {
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
impl<I, L> NodeStorage<I, L> {
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
│   [rkyv serialized leaf node]                      │
│   [padding to 512-byte boundary]                   │
├─────────────────────────────────────────────────────┤
│ Index Block (at index_offset)                       │
│   checksum + magic "OSMI" + num_entries            │
│   Array of (leaf_id, offset, size) entries         │
└─────────────────────────────────────────────────────┘
```

## LeafFile API

```rust
pub struct LeafFile<L> {
    path: PathBuf,
    file: Option<File>,
    index: HashMap<u64, FileBlockLocation>,
    header: FileHeader,
    // ...
}

impl<L> LeafFile<L>
where
    L: StorableNode + Archive + RkyvSerialize<Serializer>,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
{
    /// Create new file for writing
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, FileFormatError>;

    /// Open existing file for reading
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, FileFormatError>;

    /// Write leaf to file
    pub fn write_leaf(
        &mut self,
        leaf_id: u64,
        leaf: &L,
    ) -> Result<FileBlockLocation, FileFormatError>;

    /// Load leaf by ID
    pub fn load_leaf(&mut self, leaf_id: u64) -> Result<L, FileFormatError>;

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
    pub evicted_leaf_count: usize,  // Maintained incrementally (O(1) updates)
    pub evicted_bytes: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub leaves_written: u64,
}
```

**Performance Note**: `evicted_leaf_count` is maintained incrementally by evict/reload methods rather than computed via O(n) scans. This avoids performance overhead on frequent mutations (alloc_leaf, get_internal_mut, etc.).

## OrderStatisticsMultiset Integration

The tree uses `OsmNodeStorage<T>` internally:

```rust
pub struct OrderStatisticsMultiset<T> {
    storage: OsmNodeStorage<T>,  // = NodeStorage<InternalNodeTyped<T>, LeafNode<T>>
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

NodeStorage supports efficient O(num_leaves) checkpoint/restore for fault tolerance.

#### Checkpoint Data Structures

```rust
/// Summary of a leaf node - enough to rebuild internal nodes without reading leaf contents.
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
pub struct LeafSummary<K> {
    pub first_key: Option<K>,  // First key in leaf (for separator keys)
    pub weight_sum: ZWeight,   // Sum of weights (for subtree_sums)
    pub entry_count: usize,    // Number of entries
}

/// Committed (checkpoint) state for NodeStorage leaves.
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
pub struct CommittedLeafStorage<K> {
    pub spill_file_path: String,                    // Path to leaf data file
    pub leaf_block_locations: Vec<(usize, u64, u32)>, // (leaf_id, offset, size)
    pub leaf_summaries: Vec<LeafSummary<K>>,        // Per-leaf summaries
    pub total_entries: usize,
    pub total_weight: ZWeight,
    pub num_leaves: usize,
}
```

#### Checkpoint Methods

```rust
impl<I, L> NodeStorage<I, L> {
    /// Prepare storage for checkpoint.
    /// Flushes all dirty leaves and collects summaries for all leaves.
    /// Uses cached summaries for evicted leaves (no disk reload needed).
    /// Returns CommittedLeafStorage containing all checkpoint metadata.
    pub fn prepare_checkpoint<P: AsRef<Path>>(
        &mut self,
        path: Option<P>,
    ) -> Result<CommittedLeafStorage<L::Key>, FileFormatError>;

    /// Flush all leaves to disk for checkpoint.
    pub fn flush_all_leaves_to_disk<P: AsRef<Path>>(
        &mut self,
        path: Option<P>,
    ) -> Result<usize, FileFormatError>;

    /// Take ownership of spill file path (prevents cleanup on drop).
    pub fn take_spill_file(&mut self) -> Option<PathBuf>;

    /// Mark all leaves as evicted (for restore from checkpoint file).
    /// Summaries are stored in LeafSlot::Evicted for checkpoint support.
    pub fn mark_all_leaves_evicted(
        &mut self,
        num_leaves: usize,
        spill_path: PathBuf,
        block_locations: Vec<(usize, u64, u32)>,
        summaries: Vec<LeafSummary<L::Key>>,
    );

    /// Allocate internal node as clean (for restore).
    pub fn alloc_internal_clean(&mut self, node: I, level: u8) -> NodeLocation;
}
```

#### Checkpoint Flow

1. **Checkpoint**:
   - Call `prepare_checkpoint()` which flushes all dirty leaves and collects summaries
   - For in-memory leaves (`LeafSlot::Present`): compute summary directly from leaf
   - For evicted leaves (`LeafSlot::Evicted`): use cached summary (no disk reload needed)
   - Copy spill file to checkpoint storage → serialize `CommittedLeafStorage`
2. **Resume**: Runtime continues immediately with its local spill file (independent from checkpoint)
3. **Restore**: Deserialize metadata → call `mark_all_leaves_evicted()` with summaries → rebuild internal nodes from summaries → checkpoint file becomes spill file

This is O(num_leaves) instead of O(num_entries), enabling fast checkpoint/restore for large trees. The cached summaries in `LeafSlot::Evicted` ensure checkpoints don't require reloading evicted leaves.

#### Runtime Resume After Checkpoint

The pipeline stops briefly during checkpoint, then **resumes immediately**. Key design:

1. **Independent copies**: Checkpoint receives a full copy of the spill file; runtime keeps its local copy
2. **In-place overwrites**: Runtime modifies leaves by overwriting existing blocks (no append-only requirement)
3. **No shared state**: Checkpoint and runtime are completely independent after checkpoint completes

```
CHECKPOINT TIME:
  1. Flush all dirty leaves to local spill_file
  2. Copy spill_file to checkpoint storage (e.g., S3)
  3. Collect metadata (summaries, block locations)
  4. Runtime continues with local spill_file unchanged

RUNTIME AFTER CHECKPOINT:
  - Reads/writes to local spill_file as normal
  - Modified leaves overwrite their existing blocks
  - No coordination with checkpoint needed
```

This approach prioritizes simplicity and correctness. The primary checkpoint storage (S3) doesn't support partial updates, so full-file copy is the natural fit. For local filesystem deployments, reflink-based copy-on-write could reduce I/O.

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

## Extensibility

The generic design enables using `NodeStorage<I, L>` for other **B+ tree variants** where data resides in leaves:

```rust
// Example: Interval tree storage (B+ tree variant)
type IntervalInternalNode = ...;
type IntervalLeafNode = ...;  // Must implement LeafNodeOps
type IntervalNodeStorage = NodeStorage<IntervalInternalNode, IntervalLeafNode>;

// Example: Segment tree storage (B+ tree variant)
type SegmentInternalNode = ...;
type SegmentLeafNode = ...;  // Must implement LeafNodeOps
type SegmentNodeStorage = NodeStorage<SegmentInternalNode, SegmentLeafNode>;
```

Requirements:
- Internal node type must implement `StorableNode`
- Leaf node type must implement `StorableNode + LeafNodeOps`
- For disk spilling, leaf type must also implement rkyv `Archive`, `Serialize`, and `Deserialize`
- **The tree must follow B+ tree semantics**: data entries stored only in leaves, internal nodes for navigation only

## Test Coverage

55+ tests covering:
- NodeStorage basic operations
- Dirty tracking and threshold checks
- LeafFile read/write roundtrips
- Large leaves, many leaves, string keys
- Negative weights, empty files
- NodeStorage + LeafFile integration
- Memory eviction and reload
- BufferCache integration
- Checkpoint prepare/restore with evicted leaves
- LeafSlot state transitions

## Files

| File | Contents |
|------|----------|
| `node_storage.rs` | `NodeStorage<I,L>`, `LeafFile<L>`, `CachedLeafNode<L>`, `OsmNodeStorage<T>` |
| `order_statistics_file_format.rs` | Block format constants, `FileHeader`, `IndexEntry` |
| `order_statistics_multiset.rs` | `OrderStatisticsMultiset<T>`, `LeafNode<T>`, `InternalNodeTyped<T>` |

## Design Notes: B+ Tree Assumption and LeafNodeOps

### Why LeafNodeOps?

The `LeafNodeOps` trait exists because `NodeStorage` assumes **B+ tree semantics** where:

1. **Leaves contain data**: All actual key-value entries (with weights) are stored in leaf nodes
2. **Internal nodes are indexes**: Internal nodes contain only routing keys, child pointers, and aggregate metadata (like subtree sums)

This is distinct from classic B-trees where internal nodes also store data entries.

### What LeafNodeOps Provides

The `LeafNodeOps` trait enables:
- **Statistics collection**: `entry_count()` and `total_weight()` for dirty bytes tracking and memory accounting
- **Weight queries**: The `NodeRef::leaf_total_weight()` accessor for querying leaf statistics

### Level-Based Spilling vs Node Semantics

The `max_spillable_level` configuration unifies **which levels can be evicted to disk**, but does not change the fundamental data model:
- Level 0 (leaves) contains the actual data entries
- Level 1+ (internal nodes) contains navigation/index information

Even when internal nodes can be spilled (`max_spillable_level > 0`), they remain structurally different from leaves - they don't have "entries" or "weights" in the same sense.

### Limitations

This design does **not** support:
- **B-trees** where internal nodes store data entries
- **Trees with queryable internal node data** beyond navigation metadata

For such structures, you would need either:
1. An `InternalNodeOps` trait mirroring `LeafNodeOps`
2. A unified `NodeOps` trait that both node types implement

The current design prioritizes simplicity for the common B+ tree case (order-statistics trees, interval trees, segment trees) over full generality.

## Scalability

### Single-File Design for Large Datasets

NodeStorage uses a single spill file containing all leaves. This scales well:

| Leaves | Index Size | File Size (typical) | Notes |
|--------|------------|---------------------|-------|
| 1K | 20 KB | ~1 MB | Fits in memory, spilling optional |
| 1M | 20 MB | ~1 GB | Normal operation |
| 100M | 2 GB | ~100 GB | Large dataset |
| 1B | 20 GB | ~1 TB | Index may need streaming access |

**Index in memory**: The in-memory index (`leaf_block_locations`) uses ~20 bytes per leaf.
For 1M leaves this is 20 MB; for 1B leaves this is 20 GB. Most deployments will have
far fewer leaves (B+ tree with branching factor 64 means 1B entries ≈ 16M leaves).

**Filesystem requirements**: The single-file approach requires a filesystem supporting:
- Large files (>2GB): ext4, XFS, NTFS, and all modern filesystems support this
- Efficient random access: SSDs recommended for large files with random read patterns
- Sufficient inodes: Not a concern (single file vs millions of small files)

### When Single-File May Not Scale

For extremely large deployments (billions of leaves, not entries), consider:
- Partitioning data across multiple trees (by key range)
- Using a different storage backend with native partitioning

The current design targets the common case where trees have millions of leaves (billions
of entries), not billions of leaves. For reference, 1B entries with B=64 yields ~16M leaves.
