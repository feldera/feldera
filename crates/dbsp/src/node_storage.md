# Generic Node Storage for B+ Tree Structures

## Overview

This document describes the generic disk-backed storage implementation for **B+ tree-like data structures**. The `NodeStorage<I, L>` abstraction separates internal nodes from leaf nodes and supports level-based disk spilling with configurable policies, integrating with DBSP's storage infrastructure.

The storage is designed for trees where **data is stored exclusively in leaves**, while internal nodes serve only as navigation/index structures. This is the classic B+ tree property, distinct from B-trees where internal nodes also contain data.

The primary use case is `OrderStatisticsMultiset`, an augmented B+ tree used for O(log n) rank/select queries in percentile aggregation. The generic design enables reuse for other B+ tree variants like interval trees or segment trees.

## Architecture

### Design Rationale

The storage design exploits B+ tree access patterns and supports level-based spilling:

| Node Type | % of Nodes | Access Pattern | Strategy |
|-----------|------------|----------------|----------|
| Internal | ~2% | Every operation | Always pinned in memory |
| Leaves | ~98% | Sparse by key range | Spill to disk, reload on demand |

For a tree with 10M keys (B=64): internal nodes use ~5MB (always fits), leaves use ~156MB (spilled as needed).

Only leaves are spilled and evicted. Internal nodes are always pinned in memory.

### Component Structure

```
+--------------------------------------------------------------+
|              OrderStatisticsMultiset<T>                       |
|  +--------------------------------------------------------+  |
|  |        OsmNodeStorage<T> (type alias)                  |  |
|  |    = NodeStorage<InternalNodeTyped<T>, LeafNode<T>>    |  |
|  |  +------------------+  +----------------------------+  |  |
|  |  | Internal Nodes   |  |   Leaves                   |  |  |
|  |  | (always pinned)  |  |  Vec<LeafSlot<L>>          |  |  |
|  |  | Vec<NodeWithMeta> |  |  (Present or Evicted)      |  |  |
|  |  +------------------+  +-------------+--------------+  |  |
|  |                                      |                 |  |
|  |                           +----------v-----------+     |  |
|  |                           |  Segment Files       |     |  |
|  |                           |  (immutable, per-    |     |  |
|  |                           |   flush, block I/O)  |     |  |
|  |                           +----------+-----------+     |  |
|  +--------------------------------------|----------------+  |
|                                         |                   |
|              +-------------+------------+----------+        |
|              v             v                       v        |
|  +----------------+  +------------------+  +-------------+  |
|  | StorageBackend |  |  FileReader/     |  | BufferCache |  |
|  | (create/open)  |  |  FileCommitter   |  | (LRU cache) |  |
|  +----------------+  +------------------+  +-------------+  |
+--------------------------------------------------------------+
```

### Storage Model: Immutable Segments

NodeStorage uses **immutable segment files**, following the same pattern as DBSP's spine_async trace storage:

1. Each `flush_dirty_to_disk()` creates a **new immutable segment file** with a unique ID
2. Segment files are created via `StorageBackend::create_named()` and become read-only after `writer.complete()`
3. Each segment has a `FileReader` that provides read access and controls file lifecycle
4. Multiple segments can coexist; a leaf's location is tracked via `leaf_disk_locations` map

This is fundamentally different from a single mutable spill file. Immutable segments enable:
- **Checkpoint without copy**: segments are shared between runtime and checkpoint via `mark_for_checkpoint()`
- **Atomic commit**: segment FileReaders implement `FileCommitter` for checkpoint atomicity
- **Clean Drop behavior**: files created via `create_named()` are deleted on drop unless marked for checkpoint; files opened via `backend.open()` (restored segments) are never deleted

## Core Traits

### StorableNode Trait

Base trait for all tree nodes that can be stored in NodeStorage:

```rust
pub trait StorableNode: Clone + Debug + SizeOf + Send + Sync + 'static {
    fn estimate_size(&self) -> usize;
}
```

### LeafNodeOps Trait

Trait for leaf-specific operations required by the storage layer:

```rust
pub trait LeafNodeOps {
    type Key: Clone;

    fn entry_count(&self) -> usize;
    fn total_weight(&self) -> ZWeight;
    fn first_key(&self) -> Option<Self::Key>;
}
```

`first_key()` is used during checkpoint to build `LeafSummary` metadata, enabling O(num_leaves) internal node reconstruction on restore without reading leaf contents.

## Core Types

### NodeLocation

Strongly-typed node location tracking with level information:

```rust
pub enum NodeLocation {
    Internal { id: usize, level: u8 },
    Leaf(LeafLocation),
}

pub struct LeafLocation {
    pub id: usize,
}
```

### NodeStorageConfig

```rust
pub struct NodeStorageConfig {
    pub enable_spill: bool,
    pub spill_threshold_bytes: usize,
    pub target_segment_size: usize,
    pub spill_directory: Option<StoragePath>,
    pub segment_path_prefix: String,
    pub storage_backend: Option<Arc<dyn StorageBackend>>,
    pub buffer_cache: Option<Arc<BufferCache>>,
}
```

Key fields:
- `target_segment_size`: Controls when to start a new segment vs appending to an existing one (default: 64MB)
- `segment_path_prefix`: Ensures file name uniqueness when multiple NodeStorage instances share a backend. Segment files are named `{prefix}segment_{id}.dat`. For example, prefix `"t42_"` produces `t42_segment_0.dat`.
- `storage_backend`: Required for all disk I/O. Obtained from DBSP Runtime in production, or created directly for tests.

Constructors:
- `NodeStorageConfig::from_runtime()` - Auto-configure from DBSP Runtime settings
- `NodeStorageConfig::memory_only()` - No spilling (for testing or small datasets)
- `NodeStorageConfig::with_threshold(bytes)` - Custom spill threshold
- `NodeStorageConfig::with_spill_directory(bytes, dir)` - Creates a PosixBackend for the directory

### Segment Types

```rust
/// Unique identifier for a segment file.
pub struct SegmentId(pub u64);

/// Location of a leaf within segment-based storage.
pub struct LeafDiskLocation {
    pub segment_id: SegmentId,
    pub offset: u64,
    pub size: u32,
}

/// Metadata for a completed (immutable) segment file.
pub struct SegmentMetadata {
    pub id: SegmentId,
    pub path: StoragePath,
    pub leaf_index: HashMap<usize, (u64, u32)>,
    pub size_bytes: u64,
    pub leaf_count: usize,
    pub reader: Option<Arc<dyn FileReader>>,
}
```

The `reader` field is the key link to the StorageBackend lifecycle:
- For segments created via `flush_dirty_to_disk()`: reader comes from `writer.complete()`
- For segments restored via `restore()`: reader comes from `backend.open()`

### LeafSlot

```rust
pub enum LeafSlot<L> {
    Present(L),
    Evicted(CachedLeafSummary),
}

pub struct CachedLeafSummary {
    pub first_key_bytes: Vec<u8>,
    pub has_first_key: bool,
    pub weight_sum: ZWeight,
    pub entry_count: usize,
}
```

The cached summary enables checkpoint metadata generation without reloading evicted leaves from disk.

### NodeStorage

```rust
pub struct NodeStorage<I, L> {
    pub config: NodeStorageConfig,
    internal_nodes: Vec<NodeWithMeta<I>>,
    leaves: Vec<LeafSlot<L>>,
    dirty_leaves: HashSet<usize>,
    dirty_bytes: usize,
    stats: StorageStats,
    segments: Vec<SegmentMetadata>,
    next_segment_id: u64,
    leaf_disk_locations: HashMap<usize, LeafDiskLocation>,
    pending_segment: Option<PendingSegment>,
}

pub type OsmNodeStorage<T> = NodeStorage<InternalNodeTyped<T>, LeafNode<T>>;
```

## Node Access API

### Allocation

```rust
/// Allocate new leaf (marked dirty)
pub fn alloc_leaf(&mut self, leaf: L) -> NodeLocation;

/// Allocate new internal node at given level (marked dirty)
pub fn alloc_internal(&mut self, node: I, level: u8) -> NodeLocation;

/// Allocate internal node as clean (for restore)
pub fn alloc_internal_clean(&mut self, node: I, level: u8) -> NodeLocation;
```

### Read Access

```rust
pub fn get(&self, loc: NodeLocation) -> NodeRef<'_, I, L>;
pub fn get_leaf(&self, loc: LeafLocation) -> &L;
pub fn get_leaf_reloading(&mut self, loc: LeafLocation) -> &L;
pub fn get_internal(&self, idx: usize) -> &I;
```

`get_leaf()` panics if the leaf is evicted. Use `get_leaf_reloading()` when leaves may be evicted - it transparently reloads from disk.

### Write Access

```rust
pub fn get_mut(&mut self, loc: NodeLocation) -> NodeRefMut<'_, I, L>;
pub fn get_leaf_mut(&mut self, loc: LeafLocation) -> &mut L;
pub fn get_leaf_reloading_mut(&mut self, loc: LeafLocation) -> &mut L;
pub fn get_internal_mut(&mut self, idx: usize) -> &mut I;
```

All write accessors mark the node as dirty (copy-on-write semantics).

`get_leaf_mut()` panics if the leaf is evicted. Use `get_leaf_reloading_mut()` when eviction is enabled — it transparently reloads from disk before returning a mutable reference and marking the leaf dirty.

## Dirty Tracking

```rust
pub fn has_dirty_nodes(&self) -> bool;
pub fn dirty_leaf_count(&self) -> usize;
pub fn dirty_bytes(&self) -> usize;
pub fn should_flush(&self) -> bool;
pub fn mark_all_clean(&mut self);
```

## Disk Spilling

### Flushing to Disk

```rust
/// Flush dirty leaves to a new immutable segment file.
pub fn flush_dirty_to_disk(&mut self) -> Result<usize, FileFormatError>;

/// Flush ALL leaves (dirty + never-spilled) to disk.
/// Used internally by prepare_checkpoint().
pub fn flush_all_leaves_to_disk(&mut self) -> Result<usize, FileFormatError>;
```

Each flush creates a new segment file via `StorageBackend::create_named()`. The segment lifecycle:
1. Create file with `backend.create_named(&path)`
2. Write serialized leaf blocks with checksums
3. Write index block at end of file
4. Finalize with `writer.complete()` → returns `FileReader`
5. `FileReader` is stored in `SegmentMetadata.reader`

The FileReader **does NOT** have `mark_for_checkpoint()` called at flush time. This means the file will be deleted on drop unless `save()` is called later. This is correct behavior: segments that were never checkpointed should be cleaned up.

### Loading from Disk

```rust
pub fn load_leaf_from_disk(&mut self, loc: LeafLocation) -> Result<&L, FileFormatError>;
pub fn reload_evicted_leaves(&mut self) -> Result<usize, FileFormatError>;
```

Loading uses the `FileReader` stored in `SegmentMetadata` to read blocks via `reader.read_block()`.

## Memory Eviction

Flush and eviction are decoupled:
- `flush_dirty_to_disk()` writes leaves to disk and marks them clean (durability)
- `evict_clean_leaves()` removes clean, spilled leaves from memory (memory management)

```rust
pub fn evict_clean_leaves(&mut self) -> (usize, usize);
pub fn is_leaf_evicted(&self, loc: LeafLocation) -> bool;
pub fn is_leaf_in_memory(&self, loc: LeafLocation) -> bool;
pub fn evicted_leaf_count(&self) -> usize;
pub fn reload_evicted_leaves(&mut self) -> Result<usize, FileFormatError>;
```

**Eviction rules:**
- Only clean leaves can be evicted (dirty leaves must stay in memory)
- Only spilled leaves can be evicted (must have a disk copy in some segment)
- Summary is captured at eviction time (stored in `LeafSlot::Evicted`)
- Evicted leaves auto-reload via `get_leaf_reloading()` (read) or `get_leaf_reloading_mut()` (write)

## Backpressure

```rust
pub fn should_apply_backpressure(&self) -> bool; // dirty_bytes >= 2x threshold
pub fn should_relieve_backpressure(&self) -> bool; // dirty_bytes < threshold
pub fn backpressure_status(&self) -> (bool, f64);
```

## Checkpoint / Restore

NodeStorage integrates with DBSP's checkpoint system following the **spine_async pattern**: reference-based checkpointing via `FileCommitter` registration and `StorageBackend::open()` for restore.

### Checkpoint Data Structures

```rust
/// Per-leaf summary - enough to rebuild internal nodes without reading leaf contents.
pub struct LeafSummary<K> {
    pub first_key: Option<K>,
    pub weight_sum: ZWeight,
    pub entry_count: usize,
}

/// Committed checkpoint metadata referencing segment files.
pub struct CommittedSegmentStorage<K> {
    pub segment_paths: Vec<String>,
    pub segment_metadata: Vec<CommittedSegment>,
    pub leaf_summaries: Vec<LeafSummary<K>>,
    pub total_entries: usize,
    pub total_weight: ZWeight,
    pub num_leaves: usize,
    pub segment_path_prefix: String,
}

pub struct CommittedSegment {
    pub id: u64,
    pub leaf_blocks: Vec<(usize, u64, u32)>,  // (leaf_id, offset, size)
    pub size_bytes: u64,
}
```

`CommittedSegmentStorage` stores **paths as strings** (same pattern as `CommittedSpine` in spine_async which stores `batches: Vec<String>`). The actual data remains in segment files; the metadata just references them. The `segment_path_prefix` preserves the unique prefix used for segment file naming, ensuring restored instances recover their correct prefix and avoid file name collisions.

```rust
/// Information returned by NodeStorage::restore() for the caller to rebuild
/// higher-level data structures (e.g., internal nodes) without reading leaf contents.
pub struct NodeStorageRestoreInfo<K> {
    pub leaf_summaries: Vec<LeafSummary<K>>,
    pub total_weight: ZWeight,
    pub total_entries: usize,
    pub num_leaves: usize,
}
```

### Checkpoint Methods

```rust
/// Save for checkpoint: flush dirty leaves, serialize metadata to its own file,
/// and register segment FileReaders for atomic commit.
/// Writes metadata to {base}/nodestorage-{persistent_id}.dat via backend.write().
pub fn save(
    &mut self,
    base: &StoragePath,
    persistent_id: &str,
    files: &mut Vec<Arc<dyn FileCommitter>>,
) -> Result<(), FileFormatError>;

/// Restore from checkpoint: read metadata file, open segment files,
/// create evicted leaf slots. Returns info needed to rebuild higher-level structures.
pub fn restore(
    &mut self,
    base: &StoragePath,
    persistent_id: &str,
) -> Result<NodeStorageRestoreInfo<L::Key>, FileFormatError>;
```

Internal helpers (private):
- `prepare_checkpoint()` — flushes dirty leaves, collects `CommittedSegmentStorage` metadata
- `restore_from_committed()` — opens segment files, creates evicted leaf slots, restores `segment_path_prefix`

### Checkpoint Flow (following spine_async pattern)

NodeStorage writes and reads its own metadata file, matching the Spine pattern where `Spine::save()` calls `to_bytes()` + `backend.write()` internally.

**Save (`save(base, persistent_id, files)`):**
```
1. prepare_checkpoint()
   a. flush_all_leaves_to_disk()  -- ensures all leaves are in segment files
   b. Collect LeafSummary for each leaf (Present: compute directly, Evicted: use cache)
   c. Build CommittedSegmentStorage metadata (including segment_path_prefix)

2. For each segment's FileReader:
   a. reader.mark_for_checkpoint()  -- prevents file deletion on drop
   b. files.push(reader.clone())    -- registers for atomic commit

3. Serialize CommittedSegmentStorage via serialize_to_bytes()
4. Write to {base}/nodestorage-{persistent_id}.dat via backend.write()
   (backend.write() auto-calls mark_for_checkpoint(); result dropped — matches Spine)
```

**Restore (`restore(base, persistent_id)`):**
```
1. Read {base}/nodestorage-{persistent_id}.dat via backend.read()
2. Deserialize CommittedSegmentStorage via rkyv (archived_root + deserialize)
3. restore_from_committed(&committed):
   a. Restore segment_path_prefix from committed metadata
   b. For each segment path: backend.open(&path) -> FileReader
   c. Build SegmentMetadata, populate leaf_disk_locations index
   d. For each leaf summary: create LeafSlot::Evicted with cached summary
4. Return NodeStorageRestoreInfo (leaf_summaries, total_weight, total_entries, num_leaves)
5. Leaves are lazily loaded on demand via load_leaf_from_disk()
```

**Runtime resume after checkpoint:**
After `save()`, the runtime continues operating normally. Segment files are shared between the runtime (via in-memory `SegmentMetadata.reader`) and the checkpoint (via the `files` vec). `mark_for_checkpoint()` ensures the files survive even if the runtime drops its reference first.

### Drop Behavior

```rust
impl<I, L> Drop for NodeStorage<I, L> {
    fn drop(&mut self) {
        self.segments.clear(); // Drops FileReaders, triggering cleanup
    }
}
```

File cleanup is entirely driven by the `FileReader` lifecycle:

| Segment origin | `mark_for_checkpoint()` called? | Behavior on drop |
|---------------|-------------------------------|-----------------|
| `flush_dirty_to_disk()` (via `create_named` + `complete`) | No | File **deleted** |
| `flush_dirty_to_disk()`, then `save()` | Yes (by `save()`) | File **kept** |
| `restore()` (via `backend.open`) | N/A | File **never deleted** |

This matches the spine_async pattern where:
- Batch files from `persisted()` are deleted on drop unless `file_reader()` marks them for checkpoint
- Batch files from `from_path()` (restore) are never deleted

### Checkpoint File

NodeStorage writes its metadata to `{base}/nodestorage-{persistent_id}.dat`. This file contains the rkyv-serialized `CommittedSegmentStorage<K>` struct. The file is written via `backend.write()` which auto-calls `mark_for_checkpoint()`, ensuring the file persists across the checkpoint lifecycle without being pushed to the `files` vec. This matches the Spine pattern exactly.

### Comparison with spine_async

| Aspect | spine_async (Spine) | NodeStorage |
|--------|-------------------|-------------|
| Save signature | `save(&mut self, base, id, files)` | `save(&mut self, base, id, files)` |
| File registration | `files.push(batch.file_reader())` | `files.push(reader.clone())` |
| mark_for_checkpoint | Called inside `file_reader()` | Called explicitly in `save()` loop |
| Metadata format | `CommittedSpine { batches: Vec<String> }` | `CommittedSegmentStorage { segment_paths: Vec<String>, ... }` |
| Metadata serialization | `to_bytes()` + `backend.write()` | `serialize_to_bytes()` + `backend.write()` (same pattern) |
| Restore | `restore(&mut self, base, id)` | `restore(&mut self, base, id)` |
| Restore file opening | `B::from_path()` -> `backend.open()` | `backend.open()` directly |
| Drop cleanup | FileReader `DeleteOnDrop` flag | Same via segment FileReaders |

NodeStorage now follows the Spine pattern exactly: it writes/reads its own metadata file rather than returning metadata for the caller to serialize.

## Segment File Format

Block-based format with 512-byte alignment and CRC32C checksums:

```
+-----------------------------------------------------+
| File Header (512 bytes)                              |
|   checksum: u32 (CRC32C of bytes [4..512])          |
|   magic: "OSML" (Order Statistics Multiset Leaf)     |
|   version: u32 (= 1)                                |
|   num_leaves: u64                                    |
|   index_offset: u64                                  |
|   total_entries: u64                                 |
|   total_weight: i64                                  |
|   reserved: [u8; 468]                                |
+-----------------------------------------------------+
| Data Blocks (512-byte aligned)                       |
|   [checksum:4][magic:4][leaf_id:8][data_len:8]       |
|   [rkyv serialized leaf node]                        |
|   [padding to 512-byte boundary]                     |
+-----------------------------------------------------+
| Index Block (at index_offset)                        |
|   checksum + magic "OSMI" + num_entries              |
|   Array of (leaf_id, offset, size) entries           |
+-----------------------------------------------------+
```

## OrderStatisticsMultiset Integration

```rust
pub struct OrderStatisticsMultiset<T> {
    storage: OsmNodeStorage<T>,
    root: Option<NodeLocation>,
    first_leaf: Option<LeafLocation>,
    total_weight: ZWeight,
    num_keys: usize,
    max_leaf_entries: usize,
    max_internal_children: usize,
}
```

### Eviction-Aware Query Methods

All query methods that access leaf data take `&mut self` and use `get_leaf_reloading()` / `get_leaf_reloading_mut()` to transparently handle evicted leaves:

```rust
pub fn select_kth(&mut self, k: ZWeight, ascending: bool) -> Option<&T>;
pub fn select_percentile_bounds(&mut self, percentile: f64, ascending: bool) -> Option<(T, T, f64)>;
pub fn select_percentile_disc(&mut self, percentile: f64, ascending: bool) -> Option<&T>;
pub fn rank(&mut self, key: &T) -> ZWeight;
pub fn get_weight(&mut self, key: &T) -> ZWeight;
```

`select_percentile_bounds` returns **owned** values `(T, T, f64)` rather than references to avoid overlapping mutable borrows from two internal `select_kth` calls. Clone cost is negligible for the numeric types used in practice (F32/F64).

Methods that access only cached fields (`is_empty()`, `total_weight()`, `num_keys()`) remain `&self`.

The `iter()` method also remains `&self` — callers must call `reload_evicted_leaves()` first if leaves may be evicted.

### Flush/Evict Wrapper Methods

Convenience wrappers over the underlying `NodeStorage` flush/evict API:

```rust
pub fn should_flush(&self) -> bool;
pub fn flush_and_evict(&mut self) -> Result<(usize, usize, usize), FileFormatError>;
pub fn evicted_leaf_count(&self) -> usize;
pub fn reload_evicted_leaves(&mut self) -> Result<usize, FileFormatError>;
```

`flush_and_evict()` calls `flush_dirty_to_disk()` then `evict_clean_leaves()`, returning `(leaves_flushed, leaves_evicted, bytes_freed)`.

### Checkpoint/Restore API

```rust
/// Save for checkpoint: delegates to NodeStorage::save(), which writes its own
/// metadata file and registers segment FileReaders for atomic commit.
pub fn save(
    &mut self,
    base: &StoragePath,
    persistent_id: &str,
    files: &mut Vec<Arc<dyn FileCommitter>>,
) -> Result<(), Error>;

/// Restore from checkpoint: creates NodeStorage, delegates to NodeStorage::restore()
/// which reads its own metadata file, then rebuilds internal nodes from summaries.
pub fn restore(
    base: &StoragePath,
    persistent_id: &str,
    branching_factor: usize,
    storage_config: NodeStorageConfig,
) -> Result<Self, Error>;
```

The restore process is O(num_leaves), not O(num_entries):
1. `NodeStorage::restore()` reads its metadata file, opens segment files, creates evicted leaf slots
2. `build_internal_nodes_from_summaries()` rebuilds the internal node tree from `LeafSummary` data
3. No leaf content is read - leaves are lazily loaded on demand

### PercentileOperator Integration

`PercentileOperator` maintains a `BTreeMap<K, OrderStatisticsMultiset<V>>` and a `BTreeMap<K, u64>` mapping keys to tree IDs. It implements the full `Operator` lifecycle including `clock_end()` for flush/evict and `checkpoint()` / `restore()` for fault tolerance.

**Runtime flush/evict lifecycle:**

The operator triggers flush/evict at two points:

1. **Inline backpressure during `eval()`**: After processing all values for each key, checks `tree.should_flush()` and flushes proactively. This bounds memory during pathological cases where a single `eval()` processes millions of inserts.

2. **`clock_end()`**: After all operators finish eval for the step, flushes and evicts all trees that exceed the spill threshold. By `clock_end()`, no more reads happen until the next step, making eviction safe. Follows existing DBSP lifecycle patterns (analogous to Spine's proactive spilling).

```rust
fn clock_end(&mut self, _scope: Scope) {
    for tree in self.trees.values_mut() {
        if tree.should_flush() {
            let _ = tree.flush_and_evict();
        }
    }
}
```

**Operator metadata reporting:**

`metadata()` reports `evicted_leaves` count alongside `num_keys`, `total_entries`, `percentile_pct`, and `ascending` for monitoring spill activity.

**Checkpoint:**
```rust
fn checkpoint(&mut self, base, persistent_id, files) {
    // Each tree's OSM writes its own metadata file + registers segment files
    for (key, tree) in self.trees.iter_mut() {
        let tree_id = self.tree_ids[key];
        let tree_pid = format!("{}_t{}", persistent_id, tree_id);
        tree.save(base, &tree_pid, files)?;
    }
    // Write operator-level metadata only (tree_ids, prev_output, config, next_tree_id)
    // via backend.write() — matches Spine pattern
}
```

**Restore:**
```rust
fn restore(&mut self, base, persistent_id) {
    // Read operator-level metadata
    // Restore next_tree_id (prevents ID collisions after restore)
    for (key, tree_id) in archived_tree_ids {
        let tree_pid = format!("{}_t{}", persistent_id, tree_id);
        let tree = OrderStatisticsMultiset::restore(
            base, &tree_pid, branching_factor, config
        )?;
        self.trees.insert(key, tree);
        self.tree_ids.insert(key, tree_id);
    }
}
```

**Operator metadata (`CommittedPercentileOperator`)** contains only:
- `tree_ids: Vec<(K, u64)>` — key-to-tree-ID mapping
- `prev_output: Vec<(K, Option<V>)>` — previous output for delta computation
- `percentile`, `ascending`, `branching_factor` — configuration
- `next_tree_id: u64` — counter for assigning unique IDs to new trees

Each tree gets a unique `segment_path_prefix` (e.g., `"t0_"`, `"t1_"`) derived from its tree ID via `next_tree_id` counter. The prefix is persisted in `CommittedSegmentStorage.segment_path_prefix` and restored automatically by `NodeStorage::restore()`.

**Architecture alignment with Spine/Z1Trace:**
```
PercentileOperator::checkpoint()        (analogous to Z1Trace)
├── for each tree:
│   └── tree.save(base, tree_pid, files)           // OSM delegates
│       └── storage.save(base, tree_pid, files)     // NodeStorage writes:
│           ├── [segment files registered with files]
│           └── nodestorage-{tree_pid}.dat          // metadata file
└── writes percentile-{op_id}.dat                   // operator metadata only
```

## Statistics

```rust
pub struct StorageStats {
    pub internal_node_count: usize,
    pub leaf_node_count: usize,
    pub total_entries: usize,
    pub memory_bytes: usize,
    pub dirty_internal_count: usize,
    pub dirty_leaf_count: usize,
    pub dirty_bytes: usize,
    pub evicted_leaf_count: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub leaves_written: u64,
}
```

`evicted_leaf_count` is maintained incrementally by evict/reload methods rather than computed via O(n) scans.

## Extensibility

The generic design enables using `NodeStorage<I, L>` for other **B+ tree variants** where data resides in leaves:

```rust
type IntervalNodeStorage = NodeStorage<IntervalInternalNode, IntervalLeafNode>;
type SegmentNodeStorage = NodeStorage<SegmentInternalNode, SegmentLeafNode>;
```

Requirements:
- Internal node type must implement `StorableNode`
- Leaf node type must implement `StorableNode + LeafNodeOps`
- For disk spilling, leaf type must also implement rkyv `Archive`, `Serialize`, and `Deserialize`
- **The tree must follow B+ tree semantics**: data entries stored only in leaves

## Files

| File | Contents |
|------|----------|
| `node_storage.rs` | Core types, traits, config, in-memory operations. Includes `node_storage_disk.rs` via `include!()` |
| `node_storage_disk.rs` | Disk I/O: flush, load, evict, checkpoint (save/restore), file format, Drop impl |
| `node_storage_tests.rs` | 55+ tests covering all operations |
| `order_statistics_multiset.rs` | `OrderStatisticsMultiset<T>`, `LeafNode<T>`, `InternalNodeTyped<T>` |

## Test Coverage

55+ tests covering:
- NodeStorage basic operations
- Dirty tracking and threshold checks
- Segment file read/write roundtrips
- Large leaves, many leaves, string keys
- Negative weights, empty files
- NodeStorage + segment integration
- Memory eviction and reload (`get_leaf_reloading`, `get_leaf_reloading_mut`)
- BufferCache integration
- Checkpoint save/restore with evicted leaves
- LeafSlot state transitions
- FileReader lifecycle (drop cleanup)
- OSM flush/evict wrappers (`should_flush`, `flush_and_evict`, `evicted_leaf_count`)
- OSM query correctness after flush/evict (`select_kth`, `insert` on evicted trees)
- PercentileOperator `clock_end` triggering flush/evict with correct post-eviction queries
