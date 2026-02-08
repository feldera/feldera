//! Generic node-level storage abstraction for tree data structures.
//!
//! This module provides a storage layer that separates internal nodes from leaf nodes,
//! with support for level-based disk spilling. Following the Spine-Trace pattern from
//! DBSP, `NodeStorage<I, L>` is generic over internal node type `I` and leaf node type `L`,
//! allowing reuse for different tree structures.
//!
//! # Design
//!
//! The key insight from spine_async.md is that internal nodes are <2% of nodes but
//! accessed 100% of the time, while leaves are >98% of nodes but accessed sparsely.
//! Therefore we:
//! - Pin all internal nodes in memory (with dirty tracking)
//! - Track dirty leaves for efficient incremental persistence
//! - Evict clean leaves from memory after flushing to disk
//! - Reload evicted leaves on demand from disk
//!
//! # Generic Design
//!
//! `NodeStorage<I, L>` requires:
//! - `I: StorableNode` - internal node type
//! - `L: StorableNode + LeafNodeOps` - leaf node type with statistics operations
//!
//! This enables using the same storage implementation for different tree types:
//! - Order-statistics trees (percentile aggregation)
//! - Interval trees
//! - Segment trees
//! - Any other tree structure with internal/leaf node separation
//!
//! # Memory Eviction
//!
//! After flushing dirty leaves to disk with `flush_dirty_to_disk()`, call
//! `evict_clean_leaves()` to actually free memory. Evicted leaves are stored as
//! `None` in the leaves vector, and will be automatically reloaded from disk
//! when accessed via `get_leaf()` or `get_leaf_mut()`.
//!
//! # Runtime Integration
//!
//! When running inside a DBSP Runtime, the storage configuration can be automatically
//! derived from Runtime settings using `NodeStorageConfig::from_runtime()`. This
//! integrates with:
//! - `Runtime::min_index_storage_bytes()` for spill threshold
//! - `Runtime::storage_backend()` for file I/O
//! - `Runtime::buffer_cache()` for LRU caching (reserved for future optimization)
//!
//! See `order_statistics_node_storage_plan.md` for the full design.

use crate::storage::buffer_cache::{BufferCache, CacheEntry, FBuf, FBufSerializer};
use crate::storage::file::{Deserializer, Serializer};
use rkyv::{
    Archive, Archived, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    ser::serializers::{
        AllocScratch, CompositeSerializer, FallbackScratch, HeapScratch, SharedSerializeMap,
    },
};
use size_of::SizeOf;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use crate::algebra::ZWeight;
use crate::circuit::Runtime;
use crate::storage::backend::posixio_impl::PosixBackend;
use crate::storage::backend::{BlockLocation, FileReader, StorageBackend, StoragePath};
use feldera_storage::FileCommitter;
use feldera_types::config::{FileBackendConfig, StorageCacheConfig};

// =============================================================================
// Node Traits
// =============================================================================

/// Base trait for tree nodes that can be stored in NodeStorage.
///
/// `StorableNode` defines the basic interface for tree nodes. Serialization
/// bounds for disk spilling are added separately on impl blocks that need I/O.
///
/// # Spilling
///
/// Only leaf nodes (level 0) are spilled to disk and evicted from memory.
/// Internal nodes are always pinned in memory.
///
/// # Implementation
///
/// ```ignore
/// #[derive(Clone, Debug, SizeOf, Archive, RkyvSerialize, RkyvDeserialize)]
/// struct MyNode {
///     data: Vec<i64>,
/// }
///
/// impl StorableNode for MyNode {
///     fn estimate_size(&self) -> usize {
///         std::mem::size_of::<Self>() + self.data.capacity() * std::mem::size_of::<i64>()
///     }
/// }
/// ```
pub trait StorableNode: Clone + Debug + SizeOf + Send + Sync + 'static {
    /// Estimate the memory size of this node in bytes.
    ///
    /// Should include struct size plus heap allocations (Vec capacity, etc.).
    /// Used for memory accounting, dirty tracking, and backpressure decisions.
    fn estimate_size(&self) -> usize;
}

/// Additional operations required for leaf nodes.
///
/// Leaf nodes need to provide statistics about their contents for
/// `NodeStorage` to track entry counts and total weights accurately.
///
/// # Implementation
///
/// ```ignore
/// impl LeafNodeOps for MyLeafNode {
///     type Key = i32;
///
///     fn entry_count(&self) -> usize {
///         self.entries.len()
///     }
///
///     fn total_weight(&self) -> ZWeight {
///         self.entries.iter().map(|(_, w)| *w).sum()
///     }
///
///     fn first_key(&self) -> Option<Self::Key> {
///         self.entries.first().map(|(k, _)| k.clone())
///     }
/// }
/// ```
pub trait LeafNodeOps {
    /// The key type stored in this leaf.
    type Key: Clone;

    /// Number of entries in this leaf node.
    fn entry_count(&self) -> usize;

    /// Total weight (sum of all entry weights) in this leaf node.
    fn total_weight(&self) -> ZWeight;

    /// Get the first key in this leaf (for building separator keys during restore).
    ///
    /// Returns `None` if the leaf is empty.
    fn first_key(&self) -> Option<Self::Key>;
}

// =============================================================================
// Configuration
// =============================================================================

/// Storage configuration for NodeStorage.
///
/// Controls when and which nodes should be flushed to disk.
/// Can be created from DBSP Runtime settings using `from_runtime()`.
#[derive(Clone)]
pub struct NodeStorageConfig {
    /// Whether to enable disk spilling
    pub enable_spill: bool,

    /// Threshold for triggering flush of dirty nodes (bytes)
    /// When dirty_bytes exceeds this, `should_flush()` returns true
    pub spill_threshold_bytes: usize,

    /// Target segment file size in bytes (default: 64MB).
    ///
    /// Each flush creates a new segment file. This controls when to start
    /// a new segment vs appending to an existing one. Larger segments reduce
    /// file count but increase checkpoint/restore overhead.
    pub target_segment_size: usize,

    /// Directory for spill files (if enable_spill is true)
    /// If None, uses storage backend's default path or system temp directory
    pub spill_directory: Option<StoragePath>,

    /// Prefix for segment file names to ensure uniqueness.
    ///
    /// When multiple NodeStorage instances share the same backend/directory,
    /// each instance must have a unique prefix to prevent file name collisions.
    /// Segment files are named `{prefix}segment_{id}.dat`.
    ///
    /// If empty (default), segment files are named `segment_{id}.dat`.
    /// Set this to e.g. `"tree_42_"` to produce `tree_42_segment_0.dat`.
    pub segment_path_prefix: String,

    /// Storage backend for file I/O (if running in a Runtime with storage)
    /// If None, uses direct file I/O
    pub storage_backend: Option<Arc<dyn StorageBackend>>,

    /// Buffer cache for caching deserialized nodes (if running in a Runtime)
    /// Evicted nodes are cached here for faster reloading.
    pub buffer_cache: Option<Arc<BufferCache>>,
}

impl Debug for NodeStorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeStorageConfig")
            .field("enable_spill", &self.enable_spill)
            .field("spill_threshold_bytes", &self.spill_threshold_bytes)
            .field("target_segment_size", &self.target_segment_size)
            .field("spill_directory", &self.spill_directory)
            .field("segment_path_prefix", &self.segment_path_prefix)
            .field(
                "storage_backend",
                &self.storage_backend.as_ref().map(|_| "<StorageBackend>"),
            )
            .field(
                "buffer_cache",
                &self.buffer_cache.as_ref().map(|_| "<BufferCache>"),
            )
            .finish()
    }
}

impl Default for NodeStorageConfig {
    fn default() -> Self {
        Self {
            enable_spill: false,
            spill_threshold_bytes: 64 * 1024 * 1024, // 64MB
            target_segment_size: 64 * 1024 * 1024,   // 64MB
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: None,
            buffer_cache: None,
        }
    }
}

impl NodeStorageConfig {
    /// Create config that never spills (for testing or small datasets)
    pub fn memory_only() -> Self {
        Self {
            enable_spill: false,
            spill_threshold_bytes: usize::MAX,
            target_segment_size: 64 * 1024 * 1024,
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: None,
            buffer_cache: None,
        }
    }

    /// Create config with specific threshold (for testing)
    pub fn with_threshold(threshold_bytes: usize) -> Self {
        Self {
            enable_spill: true,
            spill_threshold_bytes: threshold_bytes,
            target_segment_size: 64 * 1024 * 1024,
            spill_directory: None,
            segment_path_prefix: String::new(),
            storage_backend: None,
            buffer_cache: None,
        }
    }

    /// Create config with specific threshold and directory.
    ///
    /// This creates a `PosixBackend` for the specified directory, enabling
    /// file-based storage. Primarily used for testing; production code should
    /// use `from_runtime()` to get the backend from the Runtime.
    pub fn with_spill_directory(threshold_bytes: usize, dir: &str) -> Self {
        // Create a PosixBackend for the directory
        let backend = Arc::new(PosixBackend::new(
            std::path::Path::new(dir),
            StorageCacheConfig::default(),
            &FileBackendConfig::default(),
        ));
        let spill_dir = StoragePath::from(dir);
        Self {
            enable_spill: true,
            spill_threshold_bytes: threshold_bytes,
            target_segment_size: 64 * 1024 * 1024,
            spill_directory: Some(spill_dir),
            segment_path_prefix: String::new(),
            storage_backend: Some(backend),
            buffer_cache: None,
        }
    }

    /// Create config from DBSP Runtime settings.
    ///
    /// This integrates with the Runtime's storage configuration:
    /// - Uses `Runtime::min_index_storage_bytes()` for the spill threshold
    /// - Uses `Runtime::storage_backend()` for file I/O
    /// - Uses `Runtime::buffer_cache()` for caching deserialized leaves
    ///
    /// If the Runtime doesn't have storage configured, returns a memory-only config.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = NodeStorageConfig::from_runtime();
    /// let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(config);
    /// ```
    pub fn from_runtime() -> Self {
        // Check if we're in a runtime context
        if Runtime::runtime().is_none() {
            // No runtime - use memory-only mode (for unit tests)
            return Self::memory_only();
        }

        // Check if storage is available in the Runtime
        let storage_backend = Runtime::storage_backend().ok();

        if storage_backend.is_none() {
            // No storage configured - use memory-only mode
            return Self::memory_only();
        }

        // Get the spill threshold from Runtime settings
        // min_index_storage_bytes is for persistent data (like percentile state)
        let spill_threshold_bytes = Runtime::min_index_storage_bytes().unwrap_or(64 * 1024 * 1024); // Default 64MB if not set

        // Get the buffer cache for caching deserialized leaves
        let buffer_cache = Some(Runtime::buffer_cache());

        Self {
            enable_spill: true,
            spill_threshold_bytes,
            target_segment_size: 64 * 1024 * 1024, // 64MB
            spill_directory: None,                 // Will use storage_backend instead
            segment_path_prefix: String::new(),
            storage_backend,
            buffer_cache,
        }
    }

    /// Check if this config uses a storage backend (vs direct file I/O)
    pub fn has_storage_backend(&self) -> bool {
        self.storage_backend.is_some()
    }

    /// Check if this config has a buffer cache configured
    ///
    /// When true, evicted leaves can be cached in the global BufferCache
    /// for faster reloading.
    pub fn has_buffer_cache(&self) -> bool {
        self.buffer_cache.is_some()
    }
}

// =============================================================================
// BufferCache Integration (Phase 7)
// =============================================================================

/// Wrapper for caching deserialized leaves in the BufferCache.
///
/// This implements `CacheEntry` so that leaves can be stored in the global
/// BufferCache alongside other DBSP cached data.
///
/// # Type Parameters
///
/// - `L`: The leaf node type
pub struct CachedLeafNode<L> {
    /// The deserialized leaf node
    pub leaf: L,
    /// Estimated size in bytes (for cache cost accounting)
    pub size_bytes: usize,
}

impl<L: Send + Sync + 'static> CacheEntry for CachedLeafNode<L> {
    fn cost(&self) -> usize {
        self.size_bytes
    }
}

// =============================================================================
// Node Location Types
// =============================================================================

/// Location of a node in storage with level tracking.
///
/// Nodes are organized by level:
/// - Level 0: Leaf nodes (no children)
/// - Level 1: Direct parents of leaves
/// - Level 2+: Higher internal nodes
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    SizeOf,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive(check_bytes)]
pub enum NodeLocation {
    /// Internal node at given index with its level (level >= 1)
    Internal { id: usize, level: u8 },
    /// Leaf node (always level 0)
    Leaf(LeafLocation),
}

impl NodeLocation {
    /// Returns true if this is a leaf location.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeLocation::Leaf(_))
    }

    /// Returns the level of this node (0 for leaves, >= 1 for internal nodes).
    #[inline]
    pub fn level(&self) -> u8 {
        match self {
            NodeLocation::Internal { level, .. } => *level,
            NodeLocation::Leaf(_) => 0,
        }
    }

    /// Returns the leaf location if this is a leaf, None otherwise.
    #[inline]
    pub fn as_leaf(&self) -> Option<LeafLocation> {
        match self {
            NodeLocation::Leaf(loc) => Some(*loc),
            _ => None,
        }
    }

    /// Returns the internal index if this is an internal node, None otherwise.
    #[inline]
    pub fn as_internal(&self) -> Option<usize> {
        match self {
            NodeLocation::Internal { id, .. } => Some(*id),
            _ => None,
        }
    }
}

/// Location of a leaf node.
///
/// For Phase 3, this wraps an index into the leaf vector with dirty tracking.
/// In Phase 4+, this will include disk block location for spilled leaves.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    SizeOf,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
    serde::Serialize,
    serde::Deserialize,
)]
#[archive(check_bytes)]
pub struct LeafLocation {
    /// Unique ID for this leaf (index in Phases 2-3, monotonic ID in Phase 4+)
    pub id: usize,
}

impl LeafLocation {
    /// Create a new leaf location with the given ID.
    #[inline]
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

// =============================================================================
// Storage Statistics
// =============================================================================

/// Statistics for monitoring storage behavior.
///
/// Compatible with DBSP's CacheStats patterns.
#[derive(Debug, Clone, Default, SizeOf)]
pub struct StorageStats {
    /// Number of internal nodes
    pub internal_node_count: usize,
    /// Number of leaf nodes (total, including evicted)
    pub leaf_node_count: usize,
    /// Total entries across all leaves (including evicted).
    /// This count is set during allocation and bulk stats updates,
    /// and represents the total logical entry count regardless of eviction state.
    pub total_entries: usize,
    /// Estimated memory usage in bytes
    pub memory_bytes: usize,

    // Dirty tracking stats (Phase 3)
    /// Number of dirty internal nodes
    pub dirty_internal_count: usize,
    /// Number of dirty leaf nodes
    pub dirty_leaf_count: usize,
    /// Bytes of dirty leaf data
    pub dirty_bytes: usize,

    // Disk spilling stats (Phase 4+)
    /// Cache hits (leaf found in memory)
    pub cache_hits: u64,
    /// Cache misses (leaf loaded from disk)
    pub cache_misses: u64,
    /// Leaves written to disk
    pub leaves_written: u64,

    // Eviction stats (Phase 6)
    /// Number of leaves currently evicted from memory
    pub evicted_leaf_count: usize,
}

// =============================================================================
// Checkpoint Data Structures
// =============================================================================

/// Summary of a leaf node - enough to rebuild internal nodes without reading leaf contents.
///
/// This enables O(num_leaves) restore instead of O(num_entries):
/// - `first_key`: Used to build separator keys in internal nodes
/// - `weight_sum`: Used to build subtree_sums in internal nodes
///
/// # Serialization
///
/// LeafSummary is serialized as part of checkpoint metadata. The key type `K`
/// must support rkyv serialization.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive(check_bytes)]
pub struct LeafSummary<K> {
    /// First key in the leaf (for building separator keys in internal nodes)
    pub first_key: Option<K>,
    /// Sum of weights in the leaf (for building subtree_sums in internal nodes)
    pub weight_sum: ZWeight,
    /// Number of entries in the leaf
    pub entry_count: usize,
}

impl<K> LeafSummary<K> {
    /// Create a new leaf summary.
    pub fn new(first_key: Option<K>, weight_sum: ZWeight, entry_count: usize) -> Self {
        Self {
            first_key,
            weight_sum,
            entry_count,
        }
    }
}

/// Committed (checkpoint) state for NodeStorage leaves.
///
/// This struct stores metadata that references a leaf file on disk, enabling:
/// - O(num_leaves) restore (internal nodes rebuilt from summaries)
/// - Zero-copy restore (checkpoint file becomes the live spill file)
/// - No re-serialization of already-spilled leaves
///
/// # File Layout
///
/// The checkpoint consists of:
/// - A `.leaves` file containing all leaf data (block-based format)
/// - This metadata stored in the main checkpoint file
///
/// # Restore Process
///
/// On restore:
/// 1. Point NodeStorage at the checkpoint `.leaves` file (no copy!)
/// 2. Rebuild internal nodes from `leaf_summaries` (no leaf I/O!)
/// 3. Mark all leaves as evicted (lazy-loaded on demand)
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive(check_bytes)]
pub struct CommittedLeafStorage<K> {
    /// Path to the spill file containing all leaf data
    pub spill_file_path: String,

    /// Index into spill file: Vec of (leaf_id, offset, size)
    /// Sorted by leaf_id for binary search
    pub leaf_block_locations: Vec<(usize, u64, u32)>,

    /// Per-leaf summaries for O(num_leaves) index reconstruction
    /// Allows rebuilding internal nodes WITHOUT reading leaf contents
    pub leaf_summaries: Vec<LeafSummary<K>>,

    /// Total number of entries across all leaves (for validation)
    pub total_entries: usize,

    /// Total weight across all leaves (for validation)
    pub total_weight: ZWeight,

    /// Number of leaves
    pub num_leaves: usize,
}

impl<K> Default for CommittedLeafStorage<K> {
    fn default() -> Self {
        Self {
            spill_file_path: String::new(),
            leaf_block_locations: Vec::new(),
            leaf_summaries: Vec::new(),
            total_entries: 0,
            total_weight: 0,
            num_leaves: 0,
        }
    }
}

// =============================================================================
// Segment-Based Storage Types (Phase 2)
// =============================================================================

/// Unique identifier for a segment file.
///
/// Segments are immutable files containing serialized leaf data. Once a segment
/// is written and completed, it cannot be modified. This aligns with the
/// StorageBackend semantics where files become read-only after `complete()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, SizeOf)]
pub struct SegmentId(pub u64);

impl SegmentId {
    /// Create a new segment ID.
    #[inline]
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the numeric value of this segment ID.
    #[inline]
    pub fn value(&self) -> u64 {
        self.0
    }
}

/// Location of a leaf within segment-based storage.
///
/// This maps a leaf to its physical location in a segment file, enabling
/// efficient random access without scanning the entire file.
#[derive(Debug, Clone, Copy, SizeOf)]
pub struct LeafDiskLocation {
    /// The segment containing this leaf
    pub segment_id: SegmentId,
    /// Byte offset within the segment file
    pub offset: u64,
    /// Size of the serialized leaf in bytes
    pub size: u32,
}

impl LeafDiskLocation {
    /// Create a new leaf disk location.
    pub fn new(segment_id: SegmentId, offset: u64, size: u32) -> Self {
        Self {
            segment_id,
            offset,
            size,
        }
    }

    /// Convert to a BlockLocation (for StorageBackend compatibility).
    pub fn to_block_location(&self) -> BlockLocation {
        BlockLocation::new(self.offset, self.size as usize).expect("Invalid block location")
    }
}

/// Metadata for a completed (immutable) segment file.
///
/// Once a segment is completed, this metadata allows efficient access to
/// individual leaves without re-reading the segment index from disk.
pub struct SegmentMetadata {
    /// Unique identifier for this segment
    pub id: SegmentId,
    /// Path to the segment file
    pub path: StoragePath,
    /// Index mapping leaf_id -> (offset, size) within this segment
    pub leaf_index: std::collections::HashMap<usize, (u64, u32)>,
    /// Total bytes in the segment file
    pub size_bytes: u64,
    /// Number of leaves in this segment
    pub leaf_count: usize,
    /// File reader for this segment (when using StorageBackend)
    ///
    /// This is Some when the segment was created via StorageBackend,
    /// allowing efficient reads without reopening the file.
    pub reader: Option<Arc<dyn FileReader>>,
}

impl Debug for SegmentMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentMetadata")
            .field("id", &self.id)
            .field("path", &self.path)
            .field("leaf_index", &self.leaf_index)
            .field("size_bytes", &self.size_bytes)
            .field("leaf_count", &self.leaf_count)
            .field("reader", &self.reader.as_ref().map(|_| "<FileReader>"))
            .finish()
    }
}

impl SegmentMetadata {
    /// Create metadata for a new segment.
    pub fn new(id: SegmentId, path: StoragePath) -> Self {
        Self {
            id,
            path,
            leaf_index: std::collections::HashMap::new(),
            size_bytes: 0,
            leaf_count: 0,
            reader: None,
        }
    }

    /// Create metadata with an associated file reader.
    pub fn with_reader(id: SegmentId, path: StoragePath, reader: Arc<dyn FileReader>) -> Self {
        Self {
            id,
            path,
            leaf_index: std::collections::HashMap::new(),
            size_bytes: 0,
            leaf_count: 0,
            reader: Some(reader),
        }
    }

    /// Check if this segment contains a specific leaf.
    #[inline]
    pub fn contains_leaf(&self, leaf_id: usize) -> bool {
        self.leaf_index.contains_key(&leaf_id)
    }

    /// Get the location of a leaf within this segment.
    #[inline]
    pub fn get_leaf_location(&self, leaf_id: usize) -> Option<LeafDiskLocation> {
        self.leaf_index
            .get(&leaf_id)
            .map(|(offset, size)| LeafDiskLocation::new(self.id, *offset, *size))
    }
}

/// A segment file currently being written (not yet completed).
///
/// This represents the mutable state of a segment during the flush operation.
/// Once all leaves are written, the segment is finalized and becomes a
/// `SegmentMetadata`.
#[derive(Debug)]
pub struct PendingSegment {
    /// Unique identifier for this segment
    pub id: SegmentId,
    /// Path to the segment file being written
    pub path: StoragePath,
    /// Filesystem path for direct I/O (temporary until full StorageBackend migration)
    pub fs_path: std::path::PathBuf,
    /// Leaves written so far: (leaf_id, offset, size)
    pub leaves_written: Vec<(usize, u64, u32)>,
    /// Current write offset in the file
    pub write_offset: u64,
    /// Total bytes written
    pub bytes_written: u64,
}

impl PendingSegment {
    /// Create a new pending segment.
    pub fn new(id: SegmentId, path: StoragePath, fs_path: std::path::PathBuf) -> Self {
        Self {
            id,
            path,
            fs_path,
            leaves_written: Vec::new(),
            write_offset: 0,
            bytes_written: 0,
        }
    }

    /// Record that a leaf was written to this segment.
    pub fn record_leaf(&mut self, leaf_id: usize, offset: u64, size: u32) {
        self.leaves_written.push((leaf_id, offset, size));
        self.bytes_written += size as u64;
    }

    /// Finalize this segment into immutable metadata.
    pub fn finalize(self) -> SegmentMetadata {
        let mut meta = SegmentMetadata::new(self.id, self.path);
        meta.size_bytes = self.bytes_written;
        meta.leaf_count = self.leaves_written.len();
        for (leaf_id, offset, size) in self.leaves_written {
            meta.leaf_index.insert(leaf_id, (offset, size));
        }
        meta
    }
}

/// Committed state for segment-based checkpoint.
///
/// This replaces `CommittedLeafStorage` for segment-based storage. It contains
/// all information needed to restore NodeStorage state without re-reading
/// leaf contents.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive(check_bytes)]
pub struct CommittedSegmentStorage<K> {
    /// Paths to all segment files (relative to checkpoint base)
    pub segment_paths: Vec<String>,

    /// Per-segment metadata: segment_id, list of (leaf_id, offset, size)
    pub segment_metadata: Vec<CommittedSegment>,

    /// Per-leaf summaries for O(num_leaves) index reconstruction
    pub leaf_summaries: Vec<LeafSummary<K>>,

    /// Total number of entries across all leaves (for validation)
    pub total_entries: usize,

    /// Total weight across all leaves (for validation)
    pub total_weight: ZWeight,

    /// Number of leaves
    pub num_leaves: usize,

    /// Segment path prefix for this storage instance.
    /// Restored on checkpoint restore to prevent file name collisions
    /// when multiple trees share the same StorageBackend namespace.
    pub segment_path_prefix: String,
}

impl<K> Default for CommittedSegmentStorage<K> {
    fn default() -> Self {
        Self {
            segment_paths: Vec::new(),
            segment_metadata: Vec::new(),
            leaf_summaries: Vec::new(),
            total_entries: 0,
            total_weight: 0,
            num_leaves: 0,
            segment_path_prefix: String::new(),
        }
    }
}

/// Metadata for a single segment in a checkpoint.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct CommittedSegment {
    /// Segment ID
    pub id: u64,
    /// Leaves in this segment: (leaf_id, offset, size)
    pub leaf_blocks: Vec<(usize, u64, u32)>,
    /// Total size of the segment file
    pub size_bytes: u64,
}

/// Information returned by NodeStorage::restore() for the caller to rebuild
/// internal tree structure (e.g., OSM rebuilds internal nodes from this).
#[derive(Debug)]
pub struct NodeStorageRestoreInfo<K> {
    /// Per-leaf summaries for rebuilding internal nodes
    pub leaf_summaries: Vec<LeafSummary<K>>,
    /// Total weight across all leaves
    pub total_weight: ZWeight,
    /// Total number of entries across all leaves
    pub total_entries: usize,
    /// Number of leaves
    pub num_leaves: usize,
}

// =============================================================================
// Internal Node with Metadata
// =============================================================================

/// Node wrapper with metadata for dirty tracking.
///
/// Wraps any node type with additional fields for incremental persistence.
/// Used for both internal nodes and (potentially) spillable internal nodes.
#[derive(Debug, Clone, SizeOf)]
pub struct NodeWithMeta<N> {
    /// The actual node data
    pub node: N,
    /// True if modified since last checkpoint/flush
    dirty: bool,
}

impl<N> NodeWithMeta<N> {
    /// Create a new node wrapper (starts dirty since it's new)
    pub fn new(node: N) -> Self {
        Self { node, dirty: true }
    }

    /// Check if this node is dirty
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark this node as dirty
    #[inline]
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Mark this node as clean (after persistence)
    #[inline]
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }
}

// =============================================================================
// Node Storage
// =============================================================================

/// Generic storage abstraction for tree nodes.
///
/// Provides separation of internal nodes (always in memory) from leaf nodes
/// (which can be spilled to disk and evicted from memory). Following the
/// Spine-Trace pattern, this is generic over node types to support different
/// tree structures.
///
/// # Type Parameters
/// - `I`: Internal node type, must implement `StorableNode`
/// - `L`: Leaf node type, must implement `StorableNode + LeafNodeOps`
///
/// # Disk Spilling and Memory Eviction
///
/// When `config.enable_spill` is true, dirty leaves can be flushed to disk
/// via `flush_dirty_to_disk()`. After flushing, call `evict_clean_leaves()`
/// to actually free memory. Evicted leaves are set to `None` and will be
/// automatically reloaded from disk when accessed.
///
/// # Example
///
/// ```ignore
/// // After accumulating dirty data
/// if storage.should_flush() {
///     storage.flush_dirty_to_disk(None)?;  // Write to disk
///     let evicted = storage.evict_clean_leaves();  // Free memory
///     println!("Evicted {} leaves, freed {} bytes", evicted.0, evicted.1);
/// }
/// ```
#[derive(Debug, SizeOf)]
pub struct NodeStorage<I, L> {
    /// Configuration
    #[size_of(skip)]
    pub config: NodeStorageConfig,

    /// Internal nodes - always in memory, with dirty tracking
    internal_nodes: Vec<NodeWithMeta<I>>,

    /// Leaf nodes - Present if in memory, Evicted if on disk only
    leaves: Vec<LeafSlot<L>>,

    /// Set of dirty leaf indices (for efficient tracking)
    #[size_of(skip)]
    dirty_leaves: HashSet<usize>,

    /// Total bytes of dirty leaves (for spill threshold)
    dirty_bytes: usize,

    /// Statistics
    stats: StorageStats,

    // === Segment-based storage ===
    /// Completed, immutable segment files
    #[size_of(skip)]
    segments: Vec<SegmentMetadata>,

    /// Next segment ID to assign
    next_segment_id: u64,

    /// Maps leaf_id -> segment location for efficient lookup.
    /// If a leaf is in this map, it has been written to disk.
    #[size_of(skip)]
    leaf_disk_locations: std::collections::HashMap<usize, LeafDiskLocation>,

    /// Segment currently being written (not yet completed)
    /// None if no flush in progress
    #[size_of(skip)]
    pending_segment: Option<PendingSegment>,
}

/// State of a leaf slot in NodeStorage.
///
/// Leaves can be either present in memory or evicted to disk.
/// When evicted, we cache the summary to avoid reloading for checkpoint.
#[derive(Debug, Clone)]
pub enum LeafSlot<L> {
    /// Leaf is present in memory
    Present(L),
    /// Leaf was evicted to disk; summary cached for checkpoint
    Evicted(CachedLeafSummary),
}

impl<L> LeafSlot<L> {
    /// Returns true if the leaf is present in memory.
    #[inline]
    pub fn is_present(&self) -> bool {
        matches!(self, LeafSlot::Present(_))
    }

    /// Returns true if the leaf is evicted to disk.
    #[inline]
    pub fn is_evicted(&self) -> bool {
        matches!(self, LeafSlot::Evicted(_))
    }

    /// Returns a reference to the leaf if present in memory.
    #[inline]
    pub fn as_present(&self) -> Option<&L> {
        match self {
            LeafSlot::Present(leaf) => Some(leaf),
            LeafSlot::Evicted(_) => None,
        }
    }

    /// Returns a mutable reference to the leaf if present in memory.
    #[inline]
    pub fn as_present_mut(&mut self) -> Option<&mut L> {
        match self {
            LeafSlot::Present(leaf) => Some(leaf),
            LeafSlot::Evicted(_) => None,
        }
    }

    /// Returns the cached summary if evicted.
    #[inline]
    pub fn as_evicted(&self) -> Option<&CachedLeafSummary> {
        match self {
            LeafSlot::Present(_) => None,
            LeafSlot::Evicted(summary) => Some(summary),
        }
    }
}

/// Cached leaf summary data stored without generic key type.
///
/// The first_key is stored as serialized bytes to avoid type parameters
/// at the struct level. This is reconstructed into `LeafSummary<K>` when needed.
#[derive(Debug, Clone)]
pub struct CachedLeafSummary {
    /// Serialized first key (rkyv format), or empty if leaf was empty
    pub first_key_bytes: Vec<u8>,
    /// Whether first_key_bytes represents Some(key) or None
    pub has_first_key: bool,
    /// Sum of weights in the leaf
    pub weight_sum: ZWeight,
    /// Number of entries in the leaf
    pub entry_count: usize,
}

// =============================================================================
// Basic NodeStorage methods (minimal bounds)
// =============================================================================

impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps,
{
    /// Create new empty storage with default configuration.
    pub fn new() -> Self {
        Self::with_config(NodeStorageConfig::default())
    }

    /// Create new empty storage with specified configuration.
    pub fn with_config(config: NodeStorageConfig) -> Self {
        Self {
            config,
            internal_nodes: Vec::new(),
            leaves: Vec::new(),
            dirty_leaves: HashSet::new(),
            dirty_bytes: 0,
            stats: StorageStats::default(),
            segments: Vec::new(),
            next_segment_id: 0,
            leaf_disk_locations: std::collections::HashMap::new(),
            pending_segment: None,
        }
    }

    /// Check if storage is empty (no nodes).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.internal_nodes.is_empty() && self.leaves.is_empty()
    }

    /// Get the total number of nodes.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.internal_nodes.len() + self.leaves.len()
    }

    /// Get storage statistics.
    pub fn stats(&self) -> &StorageStats {
        &self.stats
    }

    /// Get mutable access to stats (for updating memory_bytes etc.)
    pub fn stats_mut(&mut self) -> &mut StorageStats {
        &mut self.stats
    }

    /// Get the current configuration (immutable access).
    pub fn config_ref(&self) -> &NodeStorageConfig {
        &self.config
    }

    // =========================================================================
    // Segment-Based Storage Accessors
    // =========================================================================

    /// Get the number of completed segments.
    #[inline]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Get all completed segments.
    #[inline]
    pub fn segments(&self) -> &[SegmentMetadata] {
        &self.segments
    }

    /// Check if there's a pending segment being written.
    #[inline]
    pub fn has_pending_segment(&self) -> bool {
        self.pending_segment.is_some()
    }

    /// Get the pending segment (if any).
    #[inline]
    pub fn pending_segment(&self) -> Option<&PendingSegment> {
        self.pending_segment.as_ref()
    }

    /// Get the next segment ID that will be assigned.
    #[inline]
    pub fn next_segment_id(&self) -> u64 {
        self.next_segment_id
    }

    /// Check if a leaf has a known segment location.
    #[inline]
    pub fn leaf_has_segment_location(&self, leaf_id: usize) -> bool {
        self.leaf_disk_locations.contains_key(&leaf_id)
    }

    /// Get the segment location for a leaf (if known).
    #[inline]
    pub fn get_leaf_segment_location(&self, leaf_id: usize) -> Option<&LeafDiskLocation> {
        self.leaf_disk_locations.get(&leaf_id)
    }

    /// Get the total number of leaves stored in segments.
    pub fn leaves_in_segments(&self) -> usize {
        self.leaf_disk_locations.len()
    }

    /// Check if segment-based storage is being used.
    ///
    /// Returns true if there are any completed segments or a pending segment.
    #[inline]
    pub fn uses_segment_storage(&self) -> bool {
        !self.segments.is_empty() || self.pending_segment.is_some()
    }

    /// Generate a path for a new segment file.
    ///
    /// The path is based on the configured spill directory and the segment ID.
    pub fn generate_segment_path(&self, segment_id: SegmentId) -> StoragePath {
        let prefix = &self.config.segment_path_prefix;
        let filename = format!("{}segment_{}.dat", prefix, segment_id.value());
        if let Some(ref dir) = self.config.spill_directory {
            dir.child(filename.as_str())
        } else {
            StoragePath::from(filename.as_str())
        }
    }

    // =========================================================================
    // Size Estimation
    // =========================================================================

    /// Estimate the memory size of a leaf node in bytes.
    ///
    /// Uses the `StorableNode::estimate_size()` trait method.
    #[inline]
    pub fn estimate_leaf_size(leaf: &L) -> usize {
        leaf.estimate_size()
    }

    /// Estimate the memory size of an internal node in bytes.
    ///
    /// Uses the `StorableNode::estimate_size()` trait method.
    #[inline]
    pub fn estimate_internal_size(internal: &I) -> usize {
        internal.estimate_size()
    }

    // =========================================================================
    // Dirty Tracking
    // =========================================================================

    /// Check if any nodes are dirty.
    #[inline]
    pub fn has_dirty_nodes(&self) -> bool {
        !self.dirty_leaves.is_empty() || self.internal_nodes.iter().any(|n| n.is_dirty())
    }

    /// Get the number of dirty leaves.
    #[inline]
    pub fn dirty_leaf_count(&self) -> usize {
        self.dirty_leaves.len()
    }

    /// Get the total bytes of dirty leaf data.
    #[inline]
    pub fn dirty_bytes(&self) -> usize {
        self.dirty_bytes
    }

    /// Check if dirty bytes exceed the spill threshold.
    ///
    /// In Phase 4+, this would trigger actual disk spilling.
    /// In Phase 3, this just returns whether threshold is exceeded.
    #[inline]
    pub fn should_flush(&self) -> bool {
        self.config.enable_spill && self.dirty_bytes >= self.config.spill_threshold_bytes
    }

    /// Check threshold and indicate if flush is needed.
    ///
    /// This is the Phase 3 equivalent of `maybe_flush_dirty_leaves()`.
    /// Returns true if dirty bytes exceed threshold.
    /// Actual flushing to disk is implemented in Phase 4.
    pub fn maybe_flush_dirty_leaves(&self) -> bool {
        self.should_flush()
    }

    // =========================================================================
    // Backpressure
    // =========================================================================

    /// High threshold multiplier for backpressure.
    /// When dirty data exceeds this multiple of spill_threshold_bytes, backpressure is applied.
    const BACKPRESSURE_HIGH_MULTIPLIER: usize = 2;

    /// Low threshold multiplier for relieving backpressure.
    /// When dirty data drops below this multiple of spill_threshold_bytes, backpressure is relieved.
    const BACKPRESSURE_LOW_MULTIPLIER: usize = 1;

    /// Check if backpressure should be applied.
    ///
    /// Returns true when dirty data has accumulated to a level that would
    /// cause memory pressure. The caller should flush dirty data to disk
    /// before continuing to add more data.
    ///
    /// This follows the DBSP pattern of HIGH_THRESHOLD / LOW_THRESHOLD for
    /// hysteresis to prevent thrashing.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if storage.should_apply_backpressure() {
    ///     // Flush dirty leaves to disk before adding more
    ///     storage.flush_dirty_to_disk(None)?;
    /// }
    /// ```
    pub fn should_apply_backpressure(&self) -> bool {
        if !self.config.enable_spill {
            return false;
        }
        self.dirty_bytes >= self.config.spill_threshold_bytes * Self::BACKPRESSURE_HIGH_MULTIPLIER
    }

    /// Check if backpressure can be relieved.
    ///
    /// Returns true when dirty data has dropped below the threshold, indicating
    /// that normal operation can resume without memory pressure concerns.
    pub fn should_relieve_backpressure(&self) -> bool {
        self.dirty_bytes < self.config.spill_threshold_bytes * Self::BACKPRESSURE_LOW_MULTIPLIER
    }

    /// Get backpressure status for monitoring/debugging.
    ///
    /// Returns (should_apply, current_ratio) where ratio is dirty_bytes / threshold.
    pub fn backpressure_status(&self) -> (bool, f64) {
        let ratio = if self.config.spill_threshold_bytes > 0 {
            self.dirty_bytes as f64 / self.config.spill_threshold_bytes as f64
        } else {
            0.0
        };
        (self.should_apply_backpressure(), ratio)
    }

    /// Mark all nodes as clean (after checkpoint/persistence).
    ///
    /// Called after successfully persisting state to disk.
    pub fn mark_all_clean(&mut self) {
        for internal in &mut self.internal_nodes {
            internal.mark_clean();
        }
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.update_dirty_stats();
    }

    /// Update dirty-related statistics.
    ///
    /// Note: evicted_leaf_count is updated directly by evict/reload methods,
    /// not here, to avoid O(n) scans on every mutation.
    fn update_dirty_stats(&mut self) {
        self.stats.dirty_internal_count =
            self.internal_nodes.iter().filter(|n| n.is_dirty()).count();
        self.stats.dirty_leaf_count = self.dirty_leaves.len();
        self.stats.dirty_bytes = self.dirty_bytes;
    }

    // =========================================================================
    // Node Allocation
    // =========================================================================

    /// Allocate a new leaf node, returning its location.
    ///
    /// New leaves are automatically marked as dirty.
    pub fn alloc_leaf(&mut self, leaf: L) -> NodeLocation {
        let id = self.leaves.len();
        let leaf_size = Self::estimate_leaf_size(&leaf);

        self.stats.leaf_node_count += 1;
        self.stats.total_entries += leaf.entry_count();
        self.stats.memory_bytes += leaf_size;

        // Mark as dirty (new node)
        self.dirty_leaves.insert(id);
        self.dirty_bytes += leaf_size;

        self.leaves.push(LeafSlot::Present(leaf));
        self.update_dirty_stats();

        NodeLocation::Leaf(LeafLocation { id })
    }

    /// Allocate a new internal node, returning its location.
    ///
    /// New internal nodes are automatically marked as dirty.
    ///
    /// # Arguments
    /// * `internal` - The internal node to allocate
    /// * `level` - The level of this node (1 = parent of leaves, 2 = grandparent, etc.)
    pub fn alloc_internal(&mut self, internal: I, level: u8) -> NodeLocation {
        debug_assert!(level >= 1, "Internal nodes must have level >= 1");
        let id = self.internal_nodes.len();
        let node_size = Self::estimate_internal_size(&internal);

        self.stats.internal_node_count += 1;
        self.stats.memory_bytes += node_size;

        // Wrap with metadata (starts dirty)
        self.internal_nodes.push(NodeWithMeta::new(internal));
        self.update_dirty_stats();

        NodeLocation::Internal { id, level }
    }

    /// Allocate a new internal node as clean (for restore purposes).
    ///
    /// Used during restore when internal nodes are rebuilt from summaries
    /// and should not be marked as needing persistence.
    ///
    /// # Arguments
    /// * `internal` - The internal node to allocate
    /// * `level` - The level of this node (1 = parent of leaves, 2 = grandparent, etc.)
    pub fn alloc_internal_clean(&mut self, internal: I, level: u8) -> NodeLocation {
        debug_assert!(level >= 1, "Internal nodes must have level >= 1");
        let id = self.internal_nodes.len();
        let node_size = Self::estimate_internal_size(&internal);

        self.stats.internal_node_count += 1;
        self.stats.memory_bytes += node_size;

        // Wrap with metadata but immediately mark clean
        let mut node_with_meta = NodeWithMeta::new(internal);
        node_with_meta.mark_clean();
        self.internal_nodes.push(node_with_meta);

        NodeLocation::Internal { id, level }
    }

    // =========================================================================
    // Node Access (Read)
    // =========================================================================

    /// Get a reference to a node by location.
    ///
    /// # Panics
    /// Panics if the location is invalid or if the leaf is evicted.
    /// Use `get_leaf()` for leaves that may be evicted (it auto-reloads).
    pub fn get(&self, loc: NodeLocation) -> NodeRef<'_, I, L> {
        match loc {
            NodeLocation::Internal { id, .. } => NodeRef::Internal(&self.internal_nodes[id].node),
            NodeLocation::Leaf(leaf_loc) => {
                let leaf = self.leaves[leaf_loc.id]
                    .as_present()
                    .expect("Leaf is evicted - use get_leaf() for auto-reload");
                NodeRef::Leaf(leaf)
            }
        }
    }

    /// Get a reference to a leaf by location.
    ///
    /// If the leaf is evicted, it will be automatically reloaded from disk.
    /// Note: This requires `&mut self` because it may need to reload from disk.
    ///
    /// # Panics
    /// Panics if the location is invalid or if reload fails.
    #[inline]
    pub fn get_leaf(&self, loc: LeafLocation) -> &L {
        self.leaves[loc.id]
            .as_present()
            .expect("Leaf is evicted - use get_leaf_reloading() for auto-reload")
    }

    /// Check if a leaf is currently in memory (not evicted).
    #[inline]
    pub fn is_leaf_in_memory(&self, loc: LeafLocation) -> bool {
        loc.id < self.leaves.len() && self.leaves[loc.id].is_present()
    }

    /// Check if a leaf is evicted (on disk only, not in memory).
    #[inline]
    pub fn is_leaf_evicted(&self, loc: LeafLocation) -> bool {
        loc.id < self.leaves.len() && self.leaves[loc.id].is_evicted()
    }

    /// Get a reference to an internal node by index.
    ///
    /// # Panics
    /// Panics if the index is invalid.
    #[inline]
    pub fn get_internal(&self, idx: usize) -> &I {
        &self.internal_nodes[idx].node
    }

    /// Check if a location refers to a leaf.
    #[inline]
    pub fn is_leaf(&self, loc: NodeLocation) -> bool {
        matches!(loc, NodeLocation::Leaf(_))
    }

    // =========================================================================
    // Node Access (Write) - Copy-on-Write Semantics
    // =========================================================================

    /// Get a mutable reference to a node by location.
    ///
    /// This marks the node as dirty (copy-on-write semantics for persistence).
    ///
    /// # Panics
    /// Panics if the location is invalid or if the leaf is evicted.
    pub fn get_mut(&mut self, loc: NodeLocation) -> NodeRefMut<'_, I, L> {
        match loc {
            NodeLocation::Internal { id, .. } => {
                self.internal_nodes[id].mark_dirty();
                self.update_dirty_stats();
                NodeRefMut::Internal(&mut self.internal_nodes[id].node)
            }
            NodeLocation::Leaf(leaf_loc) => {
                self.mark_leaf_dirty(leaf_loc.id);
                let leaf = self.leaves[leaf_loc.id]
                    .as_present_mut()
                    .expect("Leaf is evicted - cannot get mutable reference");
                NodeRefMut::Leaf(leaf)
            }
        }
    }

    /// Get a mutable reference to a leaf by location.
    ///
    /// This marks the leaf as dirty (copy-on-write semantics).
    ///
    /// # Panics
    /// Panics if the location is invalid or if the leaf is evicted.
    #[inline]
    pub fn get_leaf_mut(&mut self, loc: LeafLocation) -> &mut L {
        self.mark_leaf_dirty(loc.id);
        self.leaves[loc.id]
            .as_present_mut()
            .expect("Leaf is evicted - cannot get mutable reference")
    }

    /// Get a mutable reference to an internal node by index.
    ///
    /// This marks the node as dirty.
    ///
    /// # Panics
    /// Panics if the index is invalid.
    #[inline]
    pub fn get_internal_mut(&mut self, idx: usize) -> &mut I {
        self.internal_nodes[idx].mark_dirty();
        self.update_dirty_stats();
        &mut self.internal_nodes[idx].node
    }

    /// Mark a leaf as dirty by index.
    ///
    /// If the leaf wasn't already dirty, adds its size to dirty_bytes.
    ///
    /// # Panics
    /// Panics if the leaf is evicted.
    fn mark_leaf_dirty(&mut self, id: usize) {
        if !self.dirty_leaves.contains(&id) {
            let leaf = self.leaves[id]
                .as_present()
                .expect("Cannot mark evicted leaf as dirty");
            let leaf_size = Self::estimate_leaf_size(leaf);
            self.dirty_leaves.insert(id);
            self.dirty_bytes += leaf_size;
            self.update_dirty_stats();
        }
    }

    // =========================================================================
    // Storage Management
    // =========================================================================

    /// Clear all nodes from storage.
    ///
    /// Note: This does not delete any segment files on disk. Use `cleanup_segments()`
    /// if you want to remove disk files.
    pub fn clear(&mut self) {
        self.internal_nodes.clear();
        self.leaves.clear();
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.stats = StorageStats::default();
        self.leaf_disk_locations.clear();
        // Don't clear segments - files may still exist on disk
    }

    /// Update statistics after bulk modifications.
    pub fn update_stats(&mut self) {
        self.stats.internal_node_count = self.internal_nodes.len();
        self.stats.leaf_node_count = self.leaves.len();
        self.stats.total_entries = self
            .leaves
            .iter()
            .filter_map(|l| l.as_present())
            .map(|l| l.entry_count())
            .sum();

        // Recalculate memory usage (only count in-memory leaves)
        let internal_bytes: usize = self
            .internal_nodes
            .iter()
            .map(|n| Self::estimate_internal_size(&n.node))
            .sum();
        let leaf_bytes: usize = self
            .leaves
            .iter()
            .filter_map(|l| l.as_present())
            .map(Self::estimate_leaf_size)
            .sum();
        self.stats.memory_bytes = internal_bytes + leaf_bytes;

        self.update_dirty_stats();
    }

    /// Get the number of leaves currently evicted (on disk only).
    #[inline]
    pub fn evicted_leaf_count(&self) -> usize {
        self.stats.evicted_leaf_count
    }

    /// Get the number of leaves currently in memory.
    /// Warning! O(n) operation.
    #[inline]
    pub fn in_memory_leaf_count(&self) -> usize {
        self.leaves.iter().filter(|l| l.is_present()).count()
    }

    // =========================================================================
    // Dirty Node Iteration
    // =========================================================================

    /// Get iterator over dirty leaf indices.
    pub fn dirty_leaf_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.dirty_leaves.iter().copied()
    }

    /// Get iterator over dirty internal node indices.
    pub fn dirty_internal_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.internal_nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.is_dirty())
            .map(|(i, _)| i)
    }
}

impl<I, L> Default for NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps,
{
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Node References
// =============================================================================

/// Immutable reference to a node.
///
/// Generic over internal node type `I` and leaf node type `L`.
#[derive(Debug)]
pub enum NodeRef<'a, I, L> {
    Internal(&'a I),
    Leaf(&'a L),
}

impl<'a, I, L> NodeRef<'a, I, L> {
    /// Check if this is a leaf node.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeRef::Leaf(_))
    }
}

impl<'a, I, L: LeafNodeOps> NodeRef<'a, I, L> {
    /// Get the total weight of this node's subtree (only for leaf nodes).
    pub fn leaf_total_weight(&self) -> Option<ZWeight> {
        match self {
            NodeRef::Leaf(leaf) => Some(leaf.total_weight()),
            _ => None,
        }
    }
}

/// Mutable reference to a node.
///
/// Generic over internal node type `I` and leaf node type `L`.
#[derive(Debug)]
pub enum NodeRefMut<'a, I, L> {
    Internal(&'a mut I),
    Leaf(&'a mut L),
}

impl<'a, I, L> NodeRefMut<'a, I, L> {
    /// Check if this is a leaf node.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeRefMut::Leaf(_))
    }
}

include!("node_storage_disk.rs");

#[cfg(test)]
#[path = "node_storage_tests.rs"]
mod tests;
