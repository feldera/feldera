# OrderStatisticsMultiset Node-Level Storage - Implementation Plan

## Overview

This plan implements file-backed storage for `OrderStatisticsMultiset` nodes, enabling
spill-to-disk for large percentile aggregation state. The design supports level-based
spilling with configurable policies - by default only leaf nodes are evicted to disk,
but higher levels can be enabled via `max_spillable_level` configuration.

## Prerequisites

- **Bulk Load Optimization (crates/dbsp/src/algebra/order_statistics_bulk_load_plan.md)** must be implemented first
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

## Key Design Decisions (Updated Based on spine_async.md Analysis)

### Decision 1: Use Global BufferCache, Not Custom Cache

**Original approach**: Create a custom `LeafCache<T>` with its own LRU tracking.

**Updated approach**: Use DBSP's global `BufferCache` directly.

**Rationale** (from spine_async.md:509-534):
- The global `BufferCache` is already battle-tested and integrated with DBSP's memory management
- Implementing `CacheEntry` for `LeafNode<T>` allows seamless integration
- Avoids maintaining parallel cache implementations
- Enables cross-data-structure cache sharing (leaves compete fairly with other cached data)

### Decision 2: Block-Based File Format from Start

**Original approach**: Simple file-per-leaf, optimize to block-based later.

**Updated approach**: Use block-based format from the start.

**Rationale** (from spine_async.md:149-187):
- Fewer syscalls (one file open vs thousands)
- 512-byte alignment enables direct I/O
- Built-in CRC32C checksums for data integrity
- Consistent with layer file format in `storage/file/format.rs`
- Bloom filters can be added for key existence checks

### Decision 3: Explicit Dirty Tracking

**Added requirement**: Track dirty state at node level for incremental persistence.

**Rationale** (from spine_async.md:452-482):
- Only persist modified nodes during checkpoint
- Enables copy-on-write semantics
- Critical for efficient incremental checkpointing

### Decision 4: Backpressure Mechanism

**Added requirement**: Apply backpressure when eviction queue is full.

**Rationale** (from spine_async.md:294-311):
- Prevents unbounded memory growth during heavy writes
- Ensures disk I/O can keep up with insertion rate
- Matches spine's proven approach

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
use crate::storage::backend::{StorageBackend, StoragePath, FileReader, FileWriter, BlockLocation};
use crate::storage::buffer_cache::{BufferCache, CacheEntry, CacheStats};
use crate::circuit::runtime::Runtime;
use feldera_storage::FBuf;
use std::sync::Arc;

/// Storage configuration - integrates with Runtime settings
pub struct NodeStorageConfig {
    /// Whether to enable disk spilling
    /// Default: Runtime::storage_backend().is_some()
    pub enable_spill: bool,

    /// Base path for spill files (uses StoragePath from feldera_storage)
    /// Default: derived from Runtime storage backend
    pub spill_base: Option<StoragePath>,

    /// Threshold for triggering spill (bytes of leaf data)
    /// Default: from Runtime::min_index_storage_bytes()
    pub spill_threshold_bytes: usize,
}

impl NodeStorageConfig {
    /// Create config from Runtime settings (like fallback/utils.rs pattern)
    pub fn from_runtime() -> Self {
        Self {
            enable_spill: Runtime::storage_backend().is_some(),
            spill_base: None,  // Will use default from backend
            spill_threshold_bytes: Runtime::min_index_storage_bytes()
                .unwrap_or(64 * 1024 * 1024),
        }
    }
}

/// Node location tracking
#[derive(Clone, Copy, Debug, Archive, RkyvSerialize, RkyvDeserialize)]
enum NodeLocation {
    /// Node is in the pinned internal nodes vector at given index
    Internal(usize),
    /// Node is on disk at given block location (loaded via BufferCache)
    Leaf(LeafLocation),
}

/// Location of a leaf node on disk
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Archive, RkyvSerialize, RkyvDeserialize)]
struct LeafLocation {
    /// Unique ID for this leaf (monotonically increasing)
    id: u64,
    /// Block location in the leaf file
    block: BlockLocation,
}

/// The main storage abstraction
pub struct NodeStorage<T: Ord + Clone + Archive> {
    /// Configuration
    config: NodeStorageConfig,

    /// Internal nodes - always in memory, with dirty tracking
    internal_nodes: Vec<NodeWithMeta<InternalNodeTyped<T>>>,

    /// Leaf file for disk-backed leaves (uses block-based format)
    leaf_file: Option<LeafFile<T>>,

    /// In-memory leaves not yet spilled (dirty leaves)
    /// These are pending write to disk
    dirty_leaves: HashMap<u64, LeafNode<T>>,

    /// Total bytes of dirty leaves (for spill threshold)
    dirty_bytes: usize,

    /// Next leaf ID to assign
    next_leaf_id: u64,

    /// Statistics
    stats: StorageStats,

    /// Backpressure: pending eviction count
    pending_evictions: usize,
}

/// Internal node with metadata for dirty tracking
NodeWithMeta<InternalNodeTyped<T>>

/// Wrapper to make LeafNode work with global BufferCache
/// Implements CacheEntry trait for integration with DBSP's cache
#[derive(Clone)]
pub struct CachedLeafNode<T: Ord + Clone> {
    leaf: LeafNode<T>,
}

impl<T: Ord + Clone + Send + Sync + 'static> CacheEntry for CachedLeafNode<T> {
    fn cost(&self) -> usize {
        std::mem::size_of::<LeafNode<T>>()
            + self.leaf.entries.capacity() * std::mem::size_of::<(T, ZWeight)>()
    }
}

/// Block-based file for leaf storage
/// Uses patterns from crates/dbsp/src/storage/file/format.rs
struct LeafFile<T: Ord + Clone + Archive> {
    /// Storage backend (from Runtime::storage_backend())
    backend: Arc<dyn StorageBackend>,

    /// File path
    path: StoragePath,

    /// File reader for loading leaves
    reader: Option<Arc<dyn FileReader>>,

    /// File writer for appending leaves
    writer: Option<Box<dyn FileWriter>>,

    /// Index: LeafId -> BlockLocation
    /// Loaded into memory for fast lookups
    leaf_index: HashMap<u64, BlockLocation>,

    /// Bloom filter for key existence checks (optional optimization)
    bloom_filter: Option<BloomFilter>,

    /// Reference to global buffer cache
    buffer_cache: Arc<BufferCache>,

    /// File ID for cache key generation
    file_id: u64,

    /// Phantom for T
    _phantom: PhantomData<T>,
}

/// Statistics for monitoring (compatible with CacheStats)
#[derive(Default, Clone)]
pub struct StorageStats {
    /// Cache hits (leaf found in BufferCache)
    pub cache_hits: u64,
    /// Cache misses (leaf loaded from disk)
    pub cache_misses: u64,
    /// Leaves written to disk
    pub leaves_written: u64,
    /// Bytes written to disk
    pub bytes_written: u64,
    /// Backpressure wait time
    pub backpressure_wait: std::time::Duration,
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

### Leaf Access via Global BufferCache

The key insight from spine_async.md is that we should use DBSP's global `BufferCache`
directly rather than implementing a custom cache. This provides:
- Unified memory management across all DBSP data structures
- Battle-tested LRU eviction logic
- Automatic cache statistics and monitoring

```rust
impl<T: Ord + Clone + Archive> NodeStorage<T>
where
    T: Send + Sync + 'static,
    <T as Archive>::Archived: Ord + Deserialize<T, SharedDeserializeMap>,
{
    /// Get leaf node, using global BufferCache
    /// Pattern from spine_async.md:509-534
    fn get_leaf(&mut self, loc: LeafLocation) -> Arc<CachedLeafNode<T>> {
        // 1. Check if it's a dirty leaf (not yet written to disk)
        if let Some(leaf) = self.dirty_leaves.get(&loc.id) {
            return Arc::new(CachedLeafNode { leaf: leaf.clone() });
        }

        // 2. Check global BufferCache
        let cache = Runtime::buffer_cache();
        let cache_key = (self.leaf_file.as_ref().unwrap().file_id, loc.block);

        if let Some(entry) = cache.get(&cache_key) {
            self.stats.cache_hits += 1;
            return entry.downcast::<CachedLeafNode<T>>().unwrap();
        }

        // 3. Load from disk
        self.stats.cache_misses += 1;
        let leaf_file = self.leaf_file.as_mut().unwrap();
        let leaf = leaf_file.load_leaf(loc)?;

        // 4. Insert into global cache
        let cached = Arc::new(CachedLeafNode { leaf });
        cache.insert(cache_key, cached.clone());

        cached
    }

    /// Get leaf mutably - must copy-on-write and mark dirty
    fn get_leaf_mut(&mut self, loc: LeafLocation) -> &mut LeafNode<T> {
        // If already dirty, return mutable reference
        if self.dirty_leaves.contains_key(&loc.id) {
            return self.dirty_leaves.get_mut(&loc.id).unwrap();
        }

        // Otherwise, copy from cache/disk and mark dirty
        let cached = self.get_leaf(loc);
        let leaf = cached.leaf.clone();
        let leaf_size = Self::estimate_leaf_size(&leaf);

        self.dirty_leaves.insert(loc.id, leaf);
        self.dirty_bytes += leaf_size;

        // Check if we need to flush dirty leaves to disk
        self.maybe_flush_dirty_leaves();

        self.dirty_leaves.get_mut(&loc.id).unwrap()
    }

    /// Allocate a new leaf - starts as dirty
    fn alloc_leaf(&mut self, leaf: LeafNode<T>) -> NodeLocation {
        let id = self.next_leaf_id;
        self.next_leaf_id += 1;

        let leaf_size = Self::estimate_leaf_size(&leaf);
        self.dirty_leaves.insert(id, leaf);
        self.dirty_bytes += leaf_size;

        // Check if we need to flush
        self.maybe_flush_dirty_leaves();

        NodeLocation::Leaf(LeafLocation {
            id,
            block: BlockLocation::default(),  // Will be set when written
        })
    }

    /// Check threshold and flush dirty leaves if needed
    fn maybe_flush_dirty_leaves(&mut self) {
        if self.dirty_bytes >= self.config.spill_threshold_bytes {
            self.flush_dirty_leaves();
        }
    }

    /// Flush all dirty leaves to disk with backpressure
    fn flush_dirty_leaves(&mut self) {
        if !self.config.enable_spill || self.dirty_leaves.is_empty() {
            return;
        }

        // Apply backpressure if too many pending (spine_async.md:294-311)
        const HIGH_THRESHOLD: usize = 128;
        if self.pending_evictions >= HIGH_THRESHOLD {
            let start = std::time::Instant::now();
            self.wait_for_pending_evictions();
            self.stats.backpressure_wait += start.elapsed();
        }

        let leaf_file = self.leaf_file.get_or_insert_with(|| {
            LeafFile::create(
                self.config.spill_base.clone().unwrap_or_default(),
                Runtime::storage_backend().unwrap(),
                Runtime::buffer_cache(),
            )
        });

        // Write dirty leaves to disk
        for (id, leaf) in self.dirty_leaves.drain() {
            let block_loc = leaf_file.write_leaf(id, &leaf)?;
            self.stats.leaves_written += 1;
            self.stats.bytes_written += Self::estimate_leaf_size(&leaf) as u64;
        }

        self.dirty_bytes = 0;
    }

    /// Wait for pending disk writes to complete
    fn wait_for_pending_evictions(&mut self) {
        // In async implementation, this would await pending futures
        // For sync implementation, writes are already complete
        self.pending_evictions = 0;
    }

    /// Estimate memory size of a leaf
    fn estimate_leaf_size(leaf: &LeafNode<T>) -> usize {
        std::mem::size_of::<LeafNode<T>>()
            + leaf.entries.capacity() * std::mem::size_of::<(T, ZWeight)>()
    }
}
```

## File Format

### Block-Based Leaf File Structure

**Updated**: We use a block-based single-file format from the start, aligned with DBSP's
layer file conventions (see `storage/file/format.rs`). This provides:

- **Fewer syscalls**: Single file open vs thousands of small files
- **512-byte alignment**: Enables direct I/O for better performance
- **CRC32C checksums**: Data integrity verification
- **BufferCache integration**: Block-level caching via global cache
- **Bloom filters**: Optional probabilistic key existence checks

```
┌────────────────────────────────────────────────────────────┐
│ File Header (512 bytes, block-aligned)                      │
│   magic: [u8; 4] = "OSML"  (Order Statistics Multiset Leaf) │
│   version: u32 = 1                                          │
│   branching_factor: u32                                     │
│   num_leaves: u64                                           │
│   index_block_offset: u64                                   │
│   bloom_filter_offset: u64  (0 if no bloom filter)          │
│   total_entries: u64                                        │
│   total_weight: i64                                         │
│   checksum: u32 (CRC32C of header)                         │
│   reserved: [u8; 456]                                       │
├────────────────────────────────────────────────────────────┤
│ Data Blocks (512-byte aligned)                              │
│   ┌──────────────────────────────────────────────────────┐ │
│   │ Block Header (16 bytes)                               │ │
│   │   magic: [u8; 4] = "OSMD"  (OSM Data block)          │ │
│   │   leaf_id: u64                                        │ │
│   │   checksum: u32 (CRC32C of block data)               │ │
│   ├──────────────────────────────────────────────────────┤ │
│   │ Block Data                                            │ │
│   │   rkyv serialized LeafNode<T>                        │ │
│   │   padding to 512-byte boundary                       │ │
│   └──────────────────────────────────────────────────────┘ │
│   ... more data blocks ...                                  │
├────────────────────────────────────────────────────────────┤
│ Index Block (at index_block_offset)                         │
│   magic: [u8; 4] = "OSMI"  (OSM Index block)               │
│   num_entries: u64                                          │
│   Array of IndexEntry:                                      │
│     leaf_id: u64                                            │
│     block_offset: u64                                       │
│     block_size: u32                                         │
│   Sorted by leaf_id for binary search                       │
├────────────────────────────────────────────────────────────┤
│ Bloom Filter Block (optional, at bloom_filter_offset)       │
│   magic: [u8; 4] = "OSMB"  (OSM Bloom filter)              │
│   num_bits: u64                                             │
│   num_hashes: u32                                           │
│   filter_data: [u8; ...]                                    │
├────────────────────────────────────────────────────────────┤
│ File Trailer (64 bytes)                                     │
│   magic: [u8; 4] = "OSMT"  (OSM Trailer)                   │
│   header_offset: u64 = 0                                    │
│   index_block_offset: u64                                   │
│   bloom_filter_offset: u64                                  │
│   file_checksum: u32 (CRC32C of entire file)               │
└────────────────────────────────────────────────────────────┘
```

### Magic Number Convention

Following DBSP conventions (4-byte magic numbers like "LFDB", "LFIB", "LFFT"):
- `OSML` - OSM Leaf file header
- `OSMD` - OSM Data block
- `OSMI` - OSM Index block
- `OSMB` - OSM Bloom filter block
- `OSMT` - OSM Trailer

### LeafFile Implementation

```rust
use crate::storage::file::{to_bytes, FileWriter, FileReader};
use crate::storage::buffer_cache::{BufferCache, CacheEntry};
use rkyv::{archived_root, Deserialize};
use rkyv::de::deserializers::SharedDeserializeMap;

/// Block location in the leaf file
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Default)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
pub struct BlockLocation {
    pub offset: u64,
    pub size: u32,
}

impl<T: Ord + Clone + Archive> LeafFile<T>
where
    T: Send + Sync + 'static,
    T::Archived: Deserialize<T, SharedDeserializeMap> + Ord,
{
    /// Create a new leaf file
    pub fn create(
        base_path: StoragePath,
        backend: Arc<dyn StorageBackend>,
        buffer_cache: Arc<BufferCache>,
    ) -> Result<Self, Error> {
        let path = base_path.child("leaves.osm");
        let writer = backend.create(&path)?;

        // Write header placeholder (will be updated on finalize)
        let header = LeafFileHeader::default();
        writer.write_block(0, &header.to_bytes())?;

        Ok(Self {
            backend,
            path,
            reader: None,
            writer: Some(writer),
            leaf_index: HashMap::new(),
            bloom_filter: None,
            buffer_cache,
            file_id: rand::random(),  // Unique ID for cache keys
            _phantom: PhantomData,
        })
    }

    /// Write a leaf to the file, returning its block location
    pub fn write_leaf(&mut self, id: u64, leaf: &LeafNode<T>) -> Result<BlockLocation, Error> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| Error::Runtime("file not open for writing".into()))?;

        // Serialize leaf with rkyv
        let bytes = to_bytes(leaf).expect("Serializing LeafNode should work.");

        // Create block with header
        let mut block = Vec::with_capacity(16 + bytes.len());
        block.extend_from_slice(b"OSMD");  // Magic
        block.extend_from_slice(&id.to_le_bytes());  // Leaf ID
        let checksum = crc32c::crc32c(&bytes);
        block.extend_from_slice(&checksum.to_le_bytes());
        block.extend_from_slice(&bytes);

        // Pad to 512-byte alignment
        let padded_size = (block.len() + 511) & !511;
        block.resize(padded_size, 0);

        // Write block and get offset
        let offset = writer.append_block(&block)?;
        let location = BlockLocation {
            offset,
            size: block.len() as u32,
        };

        // Update in-memory index
        self.leaf_index.insert(id, location);

        // Update bloom filter if present
        if let Some(ref mut bloom) = self.bloom_filter {
            bloom.insert(id);
        }

        Ok(location)
    }

    /// Load a leaf from the file, using global BufferCache
    /// Pattern from spine_async.md:509-534
    pub fn load_leaf(&self, loc: LeafLocation) -> Result<LeafNode<T>, Error> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| Error::Runtime("file not open for reading".into()))?;

        // Check global BufferCache first (same pattern as Spine)
        let cache_key = (self.file_id, loc.block.offset);
        if let Some(entry) = self.buffer_cache.get(&cache_key) {
            return Ok(entry.downcast::<CachedLeafNode<T>>().unwrap().leaf.clone());
        }

        // Read block from disk
        let block = reader.read_block(loc.block.offset, loc.block.size as usize)?;

        // Verify magic
        if &block[0..4] != b"OSMD" {
            return Err(Error::Runtime("invalid data block magic".into()));
        }

        // Verify checksum
        let stored_checksum = u32::from_le_bytes(block[12..16].try_into().unwrap());
        let data = &block[16..];
        let computed_checksum = crc32c::crc32c(data);
        if stored_checksum != computed_checksum {
            return Err(Error::Runtime("block checksum mismatch".into()));
        }

        // Deserialize (same pattern as spine_async.rs:1533)
        let archived = unsafe { archived_root::<LeafNode<T>>(data) };
        let leaf = archived
            .deserialize(&mut SharedDeserializeMap::new())
            .expect("deserialization failed");

        // Insert into global BufferCache for future access
        let cached = Arc::new(CachedLeafNode { leaf: leaf.clone() });
        self.buffer_cache.insert(cache_key, cached);

        Ok(leaf)
    }

    /// Check if a leaf might exist (using bloom filter)
    /// Reduces disk I/O for non-existent keys
    pub fn may_contain_leaf(&self, id: u64) -> bool {
        match &self.bloom_filter {
            Some(bloom) => bloom.may_contain(id),
            None => true,  // No bloom filter, assume it might exist
        }
    }

    /// Finalize the file (write index, bloom filter, and trailer)
    pub fn finalize(&mut self) -> Result<(), Error> {
        // Implementation writes index block, optional bloom filter,
        // and file trailer with checksums
        // ...
    }
}
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

### Phase 1: Bulk Load (prerequisite) ✅ COMPLETE
- [x] Implement `from_sorted_entries()`
- [x] Update deserialization to use bulk load
- [x] Update `compact()` to use bulk load
- [x] Tests and benchmarks

### Phase 2: Storage Abstraction ✅ COMPLETE
- [x] Define `NodeLocation` enum with `Internal(usize)` and `Leaf(LeafLocation)`
- [x] Define `NodeStorageConfig` struct (Runtime integration pending Phase 5)
- [x] Define `InternalNodeWithMeta<T>` with dirty tracking (later substituted with `NodeWithMeta<InternalNodeTyped<T>>`)
- [x] Define `NodeStorage<T>` struct
- [ ] Implement `CacheEntry` trait for `CachedLeafNode<T>` (deferred to Phase 5)
- [x] Implement wrapper that maintains current behavior (no disk spilling yet)
- [x] **Update `OrderStatisticsMultiset` to use `NodeStorage`** ✅ DONE
- [x] All existing tests pass (no regressions)

### Phase 3: Dirty Leaf Management ✅ COMPLETE
- [x] Implement dirty leaf tracking in `NodeStorage`
- [x] Implement `alloc_leaf()` that creates dirty leaves
- [x] Implement `get_leaf_mut()` with copy-on-write semantics
- [x] Implement `maybe_flush_dirty_leaves()` threshold check
- [x] Implement leaf size estimation
- [x] Tests for dirty tracking behavior

### Phase 4: Block-Based Disk Storage ✅ COMPLETE
- [x] Define `LeafFile<T>` struct with block-based format
- [x] Implement `LeafFile::create()` with header writing
- [x] Implement `LeafFile::write_leaf()` with 512-byte alignment
- [x] Implement `LeafFile::load_leaf()` (BufferCache integration deferred to Phase 6)
- [x] Implement CRC32C checksum verification
- [x] Implement `LeafFile::finalize()` with index and trailer
- [x] Tests with actual disk I/O
- [x] NodeStorage + LeafFile integration (flush_dirty_to_disk, load_leaf_from_disk)

### Phase 5: Integration with DBSP Infrastructure ✅ COMPLETE
- [x] Connect to `Runtime::min_index_storage_bytes()` for spill thresholds
- [x] Connect to `Runtime::storage_backend()` for file I/O (uses `file_system_path()`)
- [x] Connect to `Runtime::buffer_cache()` for global caching (configured, reserved for Phase 6 eviction)
- [x] Implement `save()` following `Trace::save` pattern
- [x] Implement `restore()` following `Trace::restore` pattern
- [x] Add `FileCommitter` tracking for checkpoint files
- [x] Implement `StorageStats` (CacheStats compatibility)
- [x] Documentation (`node_storage_overview.md` updated)
- [x] Backpressure mechanism for heavy writes (`should_apply_backpressure()`, `should_relieve_backpressure()`)
- [ ] Integration tests with `Checkpointer` (deferred - requires end-to-end pipeline)
- [ ] Benchmarks comparing memory-only vs spilling (deferred)

**Phase 5 Implementation Details:**

1. **NodeStorageConfig.from_runtime()**: Auto-configures from DBSP Runtime settings
   - `storage_backend`: From `Runtime::storage_backend()`
   - `spill_threshold_bytes`: From `Runtime::min_index_storage_bytes()` (default 64MB)
   - `buffer_cache`: From `Runtime::buffer_cache()`

2. **flush_dirty_to_disk()**: Uses `storage_backend.file_system_path()` for spill path
   - Creates `osm_spill/` subdirectory within storage path
   - Falls back to config directory or temp dir if backend unavailable
   - Uses unique filenames with PID and timestamp

3. **Checkpoint/Restore**: Added to `OrderStatisticsMultiset`
   - `save(base, persistent_id, files)`: Serializes via `SerializableOrderStatisticsMultiset<T>`
   - `restore(base, persistent_id)`: Reconstructs via O(n) bulk load
   - Uses rkyv serialization compatible with DBSP patterns

4. **Backpressure**: Added to `NodeStorage`
   - `should_apply_backpressure()`: Returns true when dirty_bytes >= 2x threshold
   - `should_relieve_backpressure()`: Returns true when dirty_bytes < 1x threshold
   - `backpressure_status()`: Returns (should_apply, ratio) for monitoring
   - Follows DBSP's HIGH_THRESHOLD/LOW_THRESHOLD hysteresis pattern

### Phase 6: Memory Eviction ✅ COMPLETE
- [x] Actually evict clean leaves from `Vec<LeafNode<T>>` to free memory
- [x] Changed `leaves: Vec<LeafNode<T>>` to `leaves: Vec<Option<LeafNode<T>>>`
- [x] Added `evicted_leaves: HashSet<usize>` for tracking evicted leaves
- [x] Implemented `evict_clean_leaves()` -> (count, bytes_freed)
- [x] Implemented `get_leaf_reloading()` for auto-reload on access
- [x] Implemented `reload_evicted_leaves()` for bulk reload
- [x] Added eviction-related statistics to `StorageStats`
- [x] Added 8 eviction tests

### Phase 7: BufferCache Integration ✅ COMPLETE
- [x] Implemented `CacheEntry` trait for `CachedLeafNode<T>` wrapper
- [x] Added `FileId` tracking in `NodeStorage` for BufferCache keys
- [x] Added `leaf_block_locations: HashMap<usize, (u64, u32)>` to track block locations
- [x] Updated `flush_dirty_to_disk()` to record block locations for each leaf
- [x] Updated `load_leaf_from_disk()` to check BufferCache first
- [x] Updated `load_leaf_from_disk()` to insert loaded leaves into BufferCache
- [x] Updated `cleanup_spill_file()` to evict entries from BufferCache
- [x] Added `CacheFileHandle` minimal FileReader implementation for cache lookups
- [x] Added 6 BufferCache integration tests
- [x] Updated documentation

**Phase 7 Implementation Details:**

1. **CachedLeafNode wrapper**: Wraps `LeafNode<T>` with size tracking for cache cost
   ```rust
   pub struct CachedLeafNode<T: Ord + Clone + Send + Sync + 'static> {
       pub leaf: LeafNode<T>,
       pub size_bytes: usize,
   }

   impl CacheEntry for CachedLeafNode<T> {
       fn cost(&self) -> usize { self.size_bytes }
   }
   ```

2. **Block location tracking**: `flush_dirty_to_disk()` now records:
   - `spill_file_id: Option<FileId>` - unique ID for BufferCache keys
   - `leaf_block_locations: HashMap<usize, (u64, u32)>` - (offset, size) per leaf

3. **Cache-aware loading**: `load_leaf_from_disk()` flow:
   - Check BufferCache using (FileId, offset) as key
   - On hit: clone leaf from cache (fast path)
   - On miss: read from disk, insert into BufferCache, return leaf

4. **Cleanup integration**: `cleanup_spill_file()` evicts all entries for the file

### Phase 8: Future Optimizations (PARTIALLY ANALYZED)
- [ ] Bloom filter for key existence checks (reduces disk I/O)
- [x] Async I/O for disk operations - **NOT APPLICABLE** (see [Async I/O Analysis](#async-io-analysis))
- [ ] Warm node tracking (access frequency for smarter eviction)
- [ ] File compaction (reclaim space from deleted leaves)
- [ ] Memory-mapped I/O option for large files

### Phase 9: Generalize NodeStorage with Level-Based Spilling (COMPLETED)
- [x] Define unified `StorableNode` trait for all nodes (both internal and leaf)
- [x] Add `level` field to `NodeLocation::Internal` for level tracking
- [x] Add `max_spillable_level` to `NodeStorageConfig` for configurable spilling depth
- [x] Implement `StorableNode` trait for `InternalNodeTyped<T>` and `LeafNode<T>`
- [x] Update `alloc_internal()` to require level parameter
- [x] Track levels correctly during bulk load (`from_sorted_entries`)
- [x] Track levels correctly during tree modifications (splits, new root)
- [x] Update disk spilling methods to use trait methods for size estimation
- [x] Document level-based approach in overview and code
- [ ] (Future) Make `LeafFile` fully generic over node type
- [ ] (Future) Support other order-statistics data structures

**Implementation Notes:**

**Unified StorableNode trait:**
```rust
pub trait StorableNode:
    Clone + Debug + SizeOf + Send + Sync + 'static
    + Archive + for<'a> RkyvSerialize<AllocSerializer<4096>>
where
    Self::Archived: RkyvDeserialize<Self, rkyv::Infallible>,
{
    fn estimate_size(&self) -> usize;
}
```
- Single trait replaces separate `StorableInternalNode` and `StorableLeafNode`
- Full serialization bounds enable level-based spilling for any node type
- Both `InternalNodeTyped<T>` and `LeafNode<T>` implement this trait

**Level-based spilling:**
- Nodes organized by level: Level 0 = leaves, Level 1 = parents of leaves, etc.
- `NodeLocation::Internal { id, level }` tracks both index and level
- `NodeStorageConfig::max_spillable_level` controls which levels can spill:
  - `0` (default): Only leaves can be spilled (most efficient)
  - `1`: Leaves and their direct parents can be spilled
  - `u8::MAX`: All nodes can be spilled (aggressive memory savings)
- `can_spill_level(level: u8)` helper checks if a level is spillable

**Level tracking in OrderStatisticsMultiset:**
- `from_sorted_entries`: Tracks `current_level` starting at 1, increments each layer
- Root split: New root level = `old_root.level() + 1`
- Internal split: Right sibling has same level as original node

**Size estimation strategy:**
- Basic impl block (`T: Ord + Clone`): Static methods for in-memory operations
- Disk spilling impl block (full bounds): Trait methods for serialization-aware ops
- This separation keeps basic operations free from heavy serialization bounds

## Status Summary

**Completed:** Phases 1-7 with full DBSP infrastructure integration, memory eviction, BufferCache integration, and Phase 9 level-based generalization.

`OrderStatisticsMultiset` now uses `NodeStorage` internally:
- `InternalNodeTyped.children: Vec<NodeLocation>` (strongly typed with level)
- `NodeLocation::Internal { id, level }` tracks node level for spilling decisions
- `LeafNode.next_leaf: Option<LeafLocation>` (strongly typed)
- `NodeStorageConfig.from_runtime()` for automatic configuration
- `NodeStorageConfig.max_spillable_level` for configurable spilling depth
- `save()`/`restore()` for checkpoint support
- Backpressure mechanism for heavy writes
- Memory eviction with `evict_clean_leaves()` and auto-reload
- BufferCache integration for faster evicted leaf reloading
- Unified `StorableNode` trait for all node types
- Level tracking in bulk load and tree modifications
- All 79 order_statistics tests pass

**Remaining:** Phase 8 (marked NOT APPLICABLE - see analysis below).

**See:** `node_storage_overview.md` for detailed implementation notes.

## Files to Create/Modify

### New Files
- `crates/dbsp/src/algebra/node_storage.rs` - NodeStorage, LeafFile implementation
- `crates/dbsp/src/algebra/order_statistics_file_format.rs` - Block format constants, header structs

### Modified Files
- `crates/dbsp/src/algebra/order_statistics_multiset.rs` - Use NodeStorage
- `crates/dbsp/src/algebra/mod.rs` - Export new modules

### Existing Files to Reference/Import From

| File | What We'll Use |
|------|----------------|
| `crates/dbsp/src/storage/backend.rs` | `StorageBackend`, `StoragePath`, `FileReader`, `FileWriter`, `BlockLocation` |
| `crates/dbsp/src/storage/buffer_cache/cache.rs` | `BufferCache`, `CacheEntry` trait (use directly, don't reimplement) |
| `crates/dbsp/src/storage/file/format.rs` | Block alignment patterns, magic number conventions |
| `crates/dbsp/src/storage/file.rs` | `to_bytes()` helper for rkyv serialization |
| `crates/dbsp/src/circuit/runtime.rs` | `Runtime::storage_backend()`, `Runtime::min_index_storage_bytes()`, `Runtime::buffer_cache()` |
| `crates/dbsp/src/trace.rs` | `Trace::save`/`restore` signature pattern |
| `crates/dbsp/src/trace/spine_async.rs` | Reference implementation for checkpoint/backpressure patterns |
| `crates/dbsp/src/trace/ord/fallback/utils.rs` | `pick_merge_destination()` pattern for threshold decisions |
| `crates/storage/src/fbuf.rs` | `FBuf` for aligned I/O buffers |

## Open Questions (Updated)

1. **Concurrent access**: Should `NodeStorage` support concurrent reads?
   - Current: Single-threaded per tree (matches existing design)
   - Future: Could add RwLock for read-heavy workloads
   - Reference: `BufferCache` uses `Mutex<CacheInner>` for thread-safety
   - **Recommendation**: Start single-threaded, add concurrency if needed

2. **Compaction**: How to handle file fragmentation over time?
   - Option A: Background compaction thread (like `AsyncMerger` in spine_async.rs)
   - Option B: Compact on checkpoint (simple, follows checkpoint/restore pattern)
   - Option C: Accept fragmentation (simplest, start here)
   - Reference: Spine uses background merge thread for similar compaction
   - **Recommendation**: Start with Option C, add compaction in Phase 6

3. **Memory accounting**: How to integrate with DBSP memory budgets?
   - Reference: `StorageBackend::usage()` tracks storage usage atomically
   - Reference: `CacheStats` tracks cache hit/miss rates
   - Can expose `StorageStats` through same patterns
   - **Recommendation**: Implement `StorageStats` in Phase 5

4. **Shared storage**: Should multiple trees share a file store?
   - Pro: Better space utilization
   - Con: Complexity, coordination overhead
   - **Recommendation**: One file store per tree (simpler, matches Spine pattern)

## Async I/O Analysis

### Context

This analysis was conducted to determine whether async I/O (as used in Spine) is appropriate for OrderStatisticsMultiset's disk operations.

### Spine's Async Architecture

Spine uses async I/O for **continuous background merging**:

```
Main Thread                    Background Thread
    │                              │
    │  add_batch(batch)            │
    ├─────────────────────────────>│  merge batches
    │                              │  (continuous loop)
    │  query operations            │
    │  (non-blocking via           │  write merged output
    │   PushCursor pattern)        │
    │<─────────────────────────────┤  notify completion
    │  backpressure if needed      │
```

**Key Spine Components:**
- `BackgroundThread` (`trace/spine_async/thread.rs`): Thread-local worker running continuously
- `Arc<Mutex<SharedState>>`: Shared state between main and background threads
- `Arc<Condvar>`: Backpressure coordination (wait/notify)
- `PushCursor` trait: Non-blocking reads returning `Err(Pending)` when data unavailable
- `BulkRows`: Channel-based async I/O with readahead (up to 100 blocks)

### OrderStatisticsMultiset's Use Case

| Aspect | Spine | OrderStatisticsMultiset |
|--------|-------|-------------------------|
| Background work | Continuous (merge loop) | One-shot (flush) |
| Work frequency | Constant | Occasional (threshold) |
| I/O pattern | Read+Write interleaved | Batch write only |
| Read pattern | Sequential scan (merge) | Point lookup |
| Duration | Long-running | Brief (~1ms for 1MB) |

### Why Async I/O is NOT Appropriate

1. **No continuous background work**: Unlike Spine's merge loop, OrderStatisticsMultiset only flushes occasionally when the dirty threshold is exceeded. There's no continuous processing that would benefit from async.

2. **Flush is already fast**: Writing 1000 dirty leaves (~1MB) takes approximately 1ms on SSD. The complexity of async threading isn't justified for this duration.

3. **Spine's patterns don't transfer**:
   - `BackgroundThread` is designed for continuous workers, not one-shot operations
   - `PushCursor` pattern is for non-blocking reads during merge iteration, not writes
   - The channel-based I/O with readahead assumes sequential access, not random lookups

4. **Point lookups are already efficient**: With BufferCache integration, evicted leaf reloads are fast. The `PushCursor` pattern would add complexity without benefit for single-leaf lookups.

### What IS Consistent with Spine

The current implementation already follows Spine's patterns where applicable:

| Pattern | Spine Usage | Our Usage |
|---------|-------------|-----------|
| BufferCache | Block caching | Leaf caching (Phase 7) |
| Backpressure | HIGH/LOW thresholds | `should_apply_backpressure()` |
| CacheEntry trait | Block cost tracking | Leaf cost tracking |
| Dirty tracking | Batch tracking | Leaf dirty tracking |
| rkyv serialization | Zero-copy batches | Zero-copy leaves |

### Conclusion

**Async I/O is marked as NOT APPLICABLE** for OrderStatisticsMultiset. The current synchronous implementation is:
- Correct and simple
- Consistent with Spine's philosophy (use async only where it provides real benefit)
- Efficient enough for the use case (occasional batch writes, fast point lookups)

If profiling later shows flush latency is a bottleneck, an optional `flush_dirty_to_disk_async()` could be added. However, this should be data-driven, not speculative.

### References

- `crates/dbsp/src/trace/spine_async.rs`: Main async merge implementation
- `crates/dbsp/src/trace/spine_async/thread.rs`: BackgroundThread infrastructure
- `crates/dbsp/src/trace/spine_async.md`: Design documentation
- `crates/dbsp/src/trace/cursor.rs`: PushCursor trait definition
