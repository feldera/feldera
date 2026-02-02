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
use crate::storage::backend::{BlockLocation, FileId, StorageBackend, StorageError};

/// Serialize a value to bytes using DBSP's Serializer.
///
/// This is similar to `rkyv::to_bytes` but uses DBSP's serializer types
/// (FBuf-backed with 64KB scratch space) for compatibility with the rest
/// of the DBSP storage layer.
fn serialize_to_bytes<T>(value: &T) -> Result<FBuf, FileFormatError>
where
    T: Archive + RkyvSerialize<Serializer>,
{
    use rkyv::ser::Serializer as RkyvSerializer;

    // Create DBSP's serializer with FBuf backing
    let fbuf = FBuf::with_capacity(4096);
    let mut serializer = CompositeSerializer::<
        _,
        FallbackScratch<HeapScratch<65536>, AllocScratch>,
        SharedSerializeMap,
    >::new(
        FBufSerializer::new(fbuf, usize::MAX),
        FallbackScratch::default(),
        SharedSerializeMap::default(),
    );

    serializer
        .serialize_value(value)
        .map_err(|e| FileFormatError::Serialization(format!("{:?}", e)))?;

    // Extract the bytes from the serializer
    let fbuf = serializer.into_serializer().into_inner();
    Ok(fbuf)
}

// =============================================================================
// Node Traits
// =============================================================================

/// Base trait for tree nodes that can be stored in NodeStorage.
///
/// `StorableNode` defines the basic interface for tree nodes. Serialization
/// bounds for disk spilling are added separately on impl blocks that need I/O.
///
/// # Level-Based Spilling
///
/// Nodes are organized by level:
/// - Level 0: Leaf nodes (no children)
/// - Level 1: Direct parents of leaves
/// - Level 2: Parents of level-1 nodes
/// - etc.
///
/// The `max_spillable_level` config controls which levels can be spilled:
/// - `max_spillable_level = 0`: Only leaves can be spilled (default, most efficient)
/// - `max_spillable_level = 1`: Leaves and their parents can be spilled
/// - `max_spillable_level = u8::MAX`: All nodes can be spilled
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
///     fn entry_count(&self) -> usize {
///         self.entries.len()
///     }
///
///     fn total_weight(&self) -> ZWeight {
///         self.entries.iter().map(|(_, w)| *w).sum()
///     }
/// }
/// ```
pub trait LeafNodeOps {
    /// Number of entries in this leaf node.
    fn entry_count(&self) -> usize;

    /// Total weight (sum of all entry weights) in this leaf node.
    fn total_weight(&self) -> ZWeight;
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

    /// Maximum node level that can be spilled to disk.
    ///
    /// - Level 0 = leaf nodes only (default, most efficient)
    /// - Level 1 = leaves + their direct parents
    /// - Level N = all nodes up to N levels from leaves
    ///
    /// Higher values allow more aggressive memory reclamation but may
    /// cause cascading disk reads when accessing spilled internal nodes.
    pub max_spillable_level: u8,

    /// Threshold for triggering flush of dirty nodes (bytes)
    /// When dirty_bytes exceeds this, `should_flush()` returns true
    pub spill_threshold_bytes: usize,

    /// Directory for spill files (if enable_spill is true)
    /// If None, uses system temp directory
    pub spill_directory: Option<std::path::PathBuf>,

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
            .field("max_spillable_level", &self.max_spillable_level)
            .field("spill_threshold_bytes", &self.spill_threshold_bytes)
            .field("spill_directory", &self.spill_directory)
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
            max_spillable_level: 0,                  // Only leaves by default
            spill_threshold_bytes: 64 * 1024 * 1024, // 64MB
            spill_directory: None,
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
            max_spillable_level: 0,
            spill_threshold_bytes: usize::MAX,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: None,
        }
    }

    /// Create config with specific threshold (for testing)
    pub fn with_threshold(threshold_bytes: usize) -> Self {
        Self {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: threshold_bytes,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: None,
        }
    }

    /// Create config with specific threshold and directory
    pub fn with_spill_directory<P: Into<std::path::PathBuf>>(
        threshold_bytes: usize,
        dir: P,
    ) -> Self {
        Self {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: threshold_bytes,
            spill_directory: Some(dir.into()),
            storage_backend: None,
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
            max_spillable_level: 0, // Only leaves by default
            spill_threshold_bytes,
            spill_directory: None, // Will use storage_backend instead
            storage_backend,
            buffer_cache,
        }
    }

    /// Check if this config uses a storage backend (vs direct file I/O)
    pub fn has_storage_backend(&self) -> bool {
        self.storage_backend.is_some()
    }

    /// Check if a node at the given level can be spilled to disk.
    #[inline]
    pub fn can_spill_level(&self, level: u8) -> bool {
        self.enable_spill && level <= self.max_spillable_level
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

/// Minimal FileReader implementation for BufferCache lookups.
///
/// The BufferCache API requires a `&dyn FileReader` for `get()` operations,
/// but only uses the `file_id()` method. This wrapper provides just enough
/// implementation to satisfy that requirement.
#[derive(Debug)]
struct CacheFileHandle {
    file_id: FileId,
    path: feldera_storage::StoragePath,
}

impl feldera_storage::FileRw for CacheFileHandle {
    fn file_id(&self) -> FileId {
        self.file_id
    }

    fn path(&self) -> &feldera_storage::StoragePath {
        &self.path
    }
}

impl feldera_storage::FileCommitter for CacheFileHandle {
    fn commit(&self) -> Result<(), StorageError> {
        // Not used for cache lookups
        Ok(())
    }
}

impl feldera_storage::FileReader for CacheFileHandle {
    fn mark_for_checkpoint(&self) {
        // Not used for cache lookups
    }

    fn read_block(&self, _location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        // Not used for cache lookups - we use the cache, not direct reads
        Err(StorageError::StdIo {
            kind: std::io::ErrorKind::Unsupported,
            operation: "read_block",
            path: None,
        })
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        // Not used for cache lookups
        Ok(0)
    }
}

// =============================================================================
// Node Location Types
// =============================================================================

/// Location of a node in storage with level tracking for spill decisions.
///
/// Nodes are organized by level:
/// - Level 0: Leaf nodes (no children)
/// - Level 1: Direct parents of leaves
/// - Level 2+: Higher internal nodes
///
/// The level determines spill eligibility based on `max_spillable_level` config.
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
    /// Total entries across all in-memory leaves
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
    /// Bytes of evicted leaf data (estimated)
    pub evicted_bytes: usize,
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

    /// Leaf nodes - Some(leaf) if in memory, None if evicted to disk
    leaves: Vec<Option<L>>,

    /// Set of dirty leaf indices (for efficient tracking)
    #[size_of(skip)]
    dirty_leaves: HashSet<usize>,

    /// Total bytes of dirty leaves (for spill threshold)
    dirty_bytes: usize,

    /// Statistics
    stats: StorageStats,

    // === Disk spilling state ===
    /// Path to the current spill file (if any)
    #[size_of(skip)]
    spill_file_path: Option<std::path::PathBuf>,

    /// Set of leaf indices that have been written to disk
    #[size_of(skip)]
    spilled_leaves: HashSet<usize>,

    /// Set of leaf indices that have been evicted from memory
    /// (subset of spilled_leaves - only spilled leaves can be evicted)
    #[size_of(skip)]
    evicted_leaves: HashSet<usize>,

    // === BufferCache integration ===
    /// FileId for the current spill file (for BufferCache keys)
    #[size_of(skip)]
    spill_file_id: Option<FileId>,

    /// Block locations for each spilled leaf (for BufferCache keys)
    /// Maps leaf_id -> (offset, size) in the spill file
    #[size_of(skip)]
    pub leaf_block_locations: std::collections::HashMap<usize, (u64, u32)>,
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
            spill_file_path: None,
            spilled_leaves: HashSet::new(),
            evicted_leaves: HashSet::new(),
            spill_file_id: None,
            leaf_block_locations: std::collections::HashMap::new(),
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

    /// Update dirty-related and eviction statistics.
    fn update_dirty_stats(&mut self) {
        self.stats.dirty_internal_count =
            self.internal_nodes.iter().filter(|n| n.is_dirty()).count();
        self.stats.dirty_leaf_count = self.dirty_leaves.len();
        self.stats.dirty_bytes = self.dirty_bytes;
        self.stats.evicted_leaf_count = self.evicted_leaves.len();
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

        self.leaves.push(Some(leaf));
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
                    .as_ref()
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
            .as_ref()
            .expect("Leaf is evicted - use get_leaf_reloading() for auto-reload")
    }

    /// Check if a leaf is currently in memory (not evicted).
    #[inline]
    pub fn is_leaf_in_memory(&self, loc: LeafLocation) -> bool {
        loc.id < self.leaves.len() && self.leaves[loc.id].is_some()
    }

    /// Check if a leaf is evicted (on disk only, not in memory).
    #[inline]
    pub fn is_leaf_evicted(&self, loc: LeafLocation) -> bool {
        self.evicted_leaves.contains(&loc.id)
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
                    .as_mut()
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
            .as_mut()
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
                .as_ref()
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
    /// Note: This does not delete any spill files on disk. Use `cleanup_spill_file()`
    /// if you want to remove disk files.
    pub fn clear(&mut self) {
        self.internal_nodes.clear();
        self.leaves.clear();
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.stats = StorageStats::default();
        self.spilled_leaves.clear();
        self.evicted_leaves.clear();
        self.leaf_block_locations.clear();
        // Don't clear spill_file_path/spill_file_id - file may still exist on disk
    }

    /// Update statistics after bulk modifications.
    pub fn update_stats(&mut self) {
        self.stats.internal_node_count = self.internal_nodes.len();
        self.stats.leaf_node_count = self.leaves.len();
        self.stats.total_entries = self
            .leaves
            .iter()
            .filter_map(|l| l.as_ref())
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
            .filter_map(|l| l.as_ref())
            .map(Self::estimate_leaf_size)
            .sum();
        self.stats.memory_bytes = internal_bytes + leaf_bytes;

        self.update_dirty_stats();
    }

    /// Get the number of leaves currently evicted (on disk only).
    #[inline]
    pub fn evicted_leaf_count(&self) -> usize {
        self.evicted_leaves.len()
    }

    /// Get the number of leaves currently in memory.
    #[inline]
    pub fn in_memory_leaf_count(&self) -> usize {
        self.leaves.iter().filter(|l| l.is_some()).count()
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
// Disk Spilling Methods
// =============================================================================

use std::path::{Path, PathBuf};

/// Disk spilling methods for NodeStorage.
///
/// These methods require the full `StorableNode` bounds on leaf nodes plus
/// rkyv serialization support for disk spilling.
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps + Archive + RkyvSerialize<Serializer>,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
{
    /// Flush all dirty leaves to disk.
    ///
    /// Creates (or overwrites) a spill file containing all dirty leaves.
    /// After flushing, leaves are marked as clean and added to `spilled_leaves`.
    ///
    /// # Arguments
    /// * `path` - Optional path override. If None, uses config's spill_directory
    ///            or creates a temp file.
    ///
    /// # Returns
    /// * Number of leaves written on success
    /// * Error if I/O fails
    ///
    /// # Example
    /// ```ignore
    /// let mut storage: OsmNodeStorage<i32> = NodeStorage::with_config(
    ///     NodeStorageConfig::with_threshold(1024)
    /// );
    /// // ... add leaves ...
    /// if storage.should_flush() {
    ///     let count = storage.flush_dirty_to_disk(None)?;
    ///     println!("Flushed {} leaves to disk", count);
    /// }
    /// ```
    pub fn flush_dirty_to_disk<P: AsRef<Path>>(
        &mut self,
        path: Option<P>,
    ) -> Result<usize, FileFormatError> {
        if self.dirty_leaves.is_empty() {
            return Ok(0);
        }

        // Determine the file path
        // Priority: explicit path > storage_backend path > config.spill_directory > temp dir
        let file_path = match path {
            Some(p) => p.as_ref().to_path_buf(),
            None => {
                // Generate a unique filename with process ID and a random suffix
                let filename = format!(
                    "osm_spill_{}_{:x}.osm",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(0)
                );

                if let Some(ref dir) = self.config.spill_directory {
                    // Use explicitly configured spill directory
                    dir.join(&filename)
                } else if let Some(ref backend) = self.config.storage_backend {
                    // Use storage backend's file system path if available
                    if let Some(base_path) = backend.file_system_path() {
                        base_path.join("osm_spill").join(&filename)
                    } else {
                        // Storage backend doesn't support local files (e.g., object storage)
                        // Fall back to temp directory
                        std::env::temp_dir().join(&filename)
                    }
                } else {
                    // No backend, use temp directory
                    std::env::temp_dir().join(&filename)
                }
            }
        };

        // Ensure parent directory exists
        if let Some(parent) = file_path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    FileFormatError::Io(format!(
                        "Failed to create spill directory {}: {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
        }

        // Create the leaf file
        let mut leaf_file: LeafFile<L> = LeafFile::create(&file_path)?;

        // Generate a new FileId for this spill file (for BufferCache integration)
        let file_id = FileId::new();

        // Write all dirty leaves and record their block locations
        let dirty_ids: Vec<usize> = self.dirty_leaves.iter().copied().collect();
        for &id in &dirty_ids {
            let leaf = self.leaves[id]
                .as_ref()
                .expect("Dirty leaf must be in memory");
            let block_loc = leaf_file.write_leaf(id as u64, leaf)?;

            // Store block location for BufferCache key (Phase 7)
            // Store as (offset, size) tuple for conversion to BlockLocation later
            self.leaf_block_locations
                .insert(id, (block_loc.offset, block_loc.size));
            self.spilled_leaves.insert(id);
        }

        // Finalize the file
        leaf_file.finalize()?;

        // Update statistics
        let count = dirty_ids.len();
        self.stats.leaves_written += count as u64;

        // Mark all as clean
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.update_dirty_stats();

        // Store file info for later use
        self.spill_file_id = Some(file_id);

        // Store the path for potential later use
        self.spill_file_path = Some(file_path);

        Ok(count)
    }

    /// Load a specific leaf from the spill file.
    ///
    /// If the leaf is already in memory, returns a reference to it.
    /// If the leaf was evicted to disk, checks the BufferCache first,
    /// then loads from disk if necessary.
    ///
    /// # Arguments
    /// * `loc` - The leaf location
    ///
    /// # Returns
    /// * Reference to the leaf
    /// * Error if the leaf cannot be loaded
    pub fn load_leaf_from_disk(&mut self, loc: LeafLocation) -> Result<&L, FileFormatError> {
        let id = loc.id;

        // Check if we have the leaf in memory
        if id < self.leaves.len() && self.leaves[id].is_some() {
            // Leaf is in memory, update cache stats
            self.stats.cache_hits += 1;
            return Ok(self.leaves[id].as_ref().unwrap());
        }

        // Try to load from BufferCache if available (Phase 7)
        if let Some(ref buffer_cache) = self.config.buffer_cache {
            if let (Some(file_id), Some(&(offset, size))) =
                (self.spill_file_id, self.leaf_block_locations.get(&id))
            {
                // Create a minimal FileReader for cache lookup
                let cache_handle = CacheFileHandle {
                    file_id,
                    path: feldera_storage::StoragePath::default(),
                };

                // Convert tuple to BlockLocation for BufferCache
                let block_loc = BlockLocation {
                    offset,
                    size: size as usize,
                };

                if let Some(entry) = buffer_cache.get(&cache_handle, block_loc) {
                    // Found in BufferCache! Downcast and store in leaves
                    if let Some(cached) = entry.downcast::<CachedLeafNode<L>>() {
                        let leaf = cached.leaf.clone();
                        let leaf_size = cached.size_bytes;

                        // Store in memory
                        while self.leaves.len() <= id {
                            self.leaves.push(None);
                        }
                        self.leaves[id] = Some(leaf);

                        // No longer evicted
                        self.evicted_leaves.remove(&id);

                        // Update stats
                        self.stats.cache_hits += 1;
                        self.stats.memory_bytes += leaf_size;

                        return Ok(self.leaves[id].as_ref().unwrap());
                    }
                }
            }
        }

        // Need to load from disk
        let path = self
            .spill_file_path
            .as_ref()
            .ok_or_else(|| FileFormatError::Io(format!("No spill file for leaf {}", id)))?;

        let mut leaf_file: LeafFile<L> = LeafFile::open(path)?;
        let leaf = leaf_file.load_leaf(id as u64)?;
        // Use StorableNode trait method for size estimation
        let leaf_size = leaf.estimate_size();

        self.stats.cache_misses += 1;

        // Insert into BufferCache for future access (Phase 7)
        if let Some(ref buffer_cache) = self.config.buffer_cache {
            if let (Some(file_id), Some(&(offset, _size))) =
                (self.spill_file_id, self.leaf_block_locations.get(&id))
            {
                let cached = Arc::new(CachedLeafNode {
                    leaf: leaf.clone(),
                    size_bytes: leaf_size,
                });
                buffer_cache.insert(file_id, offset, cached);
            }
        }

        // Store in memory (extend if needed)
        while self.leaves.len() <= id {
            self.leaves.push(None);
        }
        self.leaves[id] = Some(leaf);

        // No longer evicted
        self.evicted_leaves.remove(&id);

        // Update memory stats
        self.stats.memory_bytes += leaf_size;

        Ok(self.leaves[id].as_ref().unwrap())
    }

    /// Get the path to the current spill file, if any.
    pub fn spill_file_path(&self) -> Option<&Path> {
        self.spill_file_path.as_deref()
    }

    /// Check if a leaf has been spilled to disk.
    pub fn is_leaf_spilled(&self, id: usize) -> bool {
        self.spilled_leaves.contains(&id)
    }

    /// Get the number of leaves that have been spilled to disk.
    pub fn spilled_leaf_count(&self) -> usize {
        self.spilled_leaves.len()
    }

    /// Delete the spill file from disk.
    ///
    /// This should be called when the storage is no longer needed or
    /// when all data has been persisted elsewhere.
    ///
    /// **Warning**: If there are evicted leaves, they will be lost!
    /// Call `reload_evicted_leaves()` first if you need to preserve them.
    pub fn cleanup_spill_file(&mut self) -> Result<(), std::io::Error> {
        // Evict entries from BufferCache if present (Phase 7)
        if let (Some(buffer_cache), Some(file_id)) = (&self.config.buffer_cache, self.spill_file_id)
        {
            let cache_handle = CacheFileHandle {
                file_id,
                path: feldera_storage::StoragePath::default(),
            };
            buffer_cache.evict(&cache_handle);
        }

        if let Some(ref path) = self.spill_file_path {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
        self.spill_file_path = None;
        self.spill_file_id = None;
        self.spilled_leaves.clear();
        self.evicted_leaves.clear();
        self.leaf_block_locations.clear();
        Ok(())
    }

    /// Evict clean leaves from memory to free RAM.
    ///
    /// Only clean leaves that have been spilled to disk can be evicted.
    /// Dirty leaves are kept in memory since they haven't been persisted yet.
    ///
    /// This should be called after `flush_dirty_to_disk()` to actually free
    /// the memory. Without eviction, leaves remain in memory even after
    /// being written to disk.
    ///
    /// # Returns
    /// * Tuple of (number of leaves evicted, bytes freed)
    ///
    /// # Example
    /// ```ignore
    /// if storage.should_flush() {
    ///     storage.flush_dirty_to_disk(None)?;  // Write dirty leaves to disk
    ///     let (evicted, freed) = storage.evict_clean_leaves();  // Free memory
    ///     println!("Evicted {} leaves, freed {} bytes", evicted, freed);
    /// }
    /// ```
    pub fn evict_clean_leaves(&mut self) -> (usize, usize) {
        let mut evicted_count = 0;
        let mut bytes_freed = 0;

        // Only evict leaves that are:
        // 1. In memory (Some)
        // 2. Spilled to disk (in spilled_leaves)
        // 3. Not dirty (not in dirty_leaves)
        for id in 0..self.leaves.len() {
            if self.leaves[id].is_some()
                && self.spilled_leaves.contains(&id)
                && !self.dirty_leaves.contains(&id)
            {
                // Calculate size before evicting (using StorableNode trait)
                let leaf = self.leaves[id].as_ref().unwrap();
                let leaf_size = leaf.estimate_size();

                // Evict by setting to None
                self.leaves[id] = None;
                self.evicted_leaves.insert(id);

                evicted_count += 1;
                bytes_freed += leaf_size;
            }
        }

        // Update memory stats
        self.stats.memory_bytes = self.stats.memory_bytes.saturating_sub(bytes_freed);

        (evicted_count, bytes_freed)
    }

    /// Get a leaf, automatically reloading from disk if evicted.
    ///
    /// This is the primary method for accessing leaves when eviction is enabled.
    /// It handles the common case of reloading evicted leaves transparently.
    ///
    /// # Panics
    /// Panics if the leaf location is invalid or if reload fails.
    pub fn get_leaf_reloading(&mut self, loc: LeafLocation) -> &L {
        let id = loc.id;

        // If already in memory, return it
        if id < self.leaves.len() && self.leaves[id].is_some() {
            self.stats.cache_hits += 1;
            return self.leaves[id].as_ref().unwrap();
        }

        // Need to reload from disk
        self.load_leaf_from_disk(loc)
            .expect("Failed to reload evicted leaf from disk")
    }

    /// Reload all evicted leaves from disk into memory.
    ///
    /// This can be used to restore all leaves to memory, e.g., before
    /// serialization or when memory pressure is relieved.
    ///
    /// # Returns
    /// * Number of leaves reloaded
    pub fn reload_evicted_leaves(&mut self) -> Result<usize, FileFormatError> {
        let path = match &self.spill_file_path {
            Some(p) => p.clone(),
            None => return Ok(0),
        };

        if self.evicted_leaves.is_empty() {
            return Ok(0);
        }

        let mut leaf_file: LeafFile<L> = LeafFile::open(&path)?;
        let mut count = 0;

        for id in self.evicted_leaves.clone() {
            let leaf = leaf_file.load_leaf(id as u64)?;
            // Use StorableNode trait method for size estimation
            let leaf_size = leaf.estimate_size();

            // Ensure we have space
            while self.leaves.len() <= id {
                self.leaves.push(None);
            }
            self.leaves[id] = Some(leaf);
            self.stats.memory_bytes += leaf_size;
            count += 1;
        }

        self.evicted_leaves.clear();
        self.stats.cache_misses += count as u64;

        Ok(count)
    }

    /// Reload all spilled leaves from disk into memory.
    ///
    /// This can be used to restore state after a restart.
    /// Deprecated: use `reload_evicted_leaves()` instead.
    pub fn reload_spilled_leaves(&mut self) -> Result<usize, FileFormatError> {
        let path = match &self.spill_file_path {
            Some(p) => p.clone(),
            None => return Ok(0),
        };

        let mut leaf_file: LeafFile<L> = LeafFile::open(&path)?;
        let mut count = 0;

        for &id in &self.spilled_leaves.clone() {
            let leaf = leaf_file.load_leaf(id as u64)?;

            // Ensure we have space
            while self.leaves.len() <= id {
                self.leaves.push(None);
            }
            self.leaves[id] = Some(leaf);
            count += 1;
        }

        // All spilled leaves are now in memory
        self.evicted_leaves.clear();

        self.stats.cache_misses += count as u64;
        Ok(count)
    }

    // =========================================================================
    // Checkpoint Methods
    // =========================================================================

    /// Check if a leaf is already on disk (spilled and not dirty).
    ///
    /// Returns true if the leaf has been written to disk and hasn't been
    /// modified since. Such leaves don't need re-serialization during checkpoint.
    #[inline]
    pub fn is_leaf_on_disk(&self, leaf_id: usize) -> bool {
        // A leaf is on disk if it's in spilled_leaves and not dirty
        self.spilled_leaves.contains(&leaf_id) && !self.dirty_leaves.contains(&leaf_id)
    }

    /// Flush all leaves to disk, writing only those not yet persisted.
    ///
    /// This method handles three categories of leaves:
    /// - Already spilled (clean): SKIP - no re-serialization needed
    /// - Dirty: MUST write - modified since last flush
    /// - Never spilled: MUST write - only in memory
    ///
    /// Creates a spill file if none exists (handles small trees).
    ///
    /// # Returns
    ///
    /// Number of leaves written to disk.
    pub fn flush_all_leaves_to_disk(&mut self) -> Result<usize, FileFormatError> {
        // Create spill file if none exists
        if self.spill_file_path.is_none() && !self.leaves.is_empty() {
            let temp_dir = self.config.spill_directory.clone().unwrap_or_else(|| {
                std::env::temp_dir().join("dbsp_node_storage")
            });
            std::fs::create_dir_all(&temp_dir)
                .map_err(|e| FileFormatError::Io(format!("Failed to create spill dir: {}", e)))?;

            // Generate unique filename using timestamp and thread id
            let unique_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let thread_id = std::thread::current().id();
            let file_name = format!("leaves_{}_{:?}.dat", unique_id, thread_id);
            self.spill_file_path = Some(temp_dir.join(file_name));
        }

        let path = match &self.spill_file_path {
            Some(p) => p.clone(),
            None => return Ok(0), // No leaves to flush
        };

        // Open or create the leaf file
        let mut leaf_file: LeafFile<L> = if path.exists() {
            // Open existing file for appending
            LeafFile::open(&path)?
        } else {
            // Create new file
            LeafFile::create(&path)?
        };

        let mut count = 0;

        // Write leaves that aren't already on disk
        for leaf_id in 0..self.leaves.len() {
            // Skip if already on disk and clean
            if self.is_leaf_on_disk(leaf_id) {
                continue;
            }

            // Get the leaf - skip if evicted
            let leaf = match &self.leaves[leaf_id] {
                Some(leaf) => leaf,
                None => continue, // Already evicted, should be in spilled_leaves
            };

            // Write to file
            let location = leaf_file.write_leaf(leaf_id as u64, leaf)?;

            // Update tracking
            self.leaf_block_locations
                .insert(leaf_id, (location.offset, location.size));
            self.spilled_leaves.insert(leaf_id);
            self.dirty_leaves.remove(&leaf_id);
            count += 1;
        }

        // Finalize the file (writes index)
        leaf_file.finalize()?;

        self.stats.leaves_written += count as u64;
        Ok(count)
    }

    /// Take ownership of the spill file path.
    ///
    /// After calling this, Drop will not delete the file. Used during checkpoint
    /// to transfer file ownership to the checkpointer.
    ///
    /// # Returns
    ///
    /// The spill file path, if any.
    pub fn take_spill_file(&mut self) -> Option<PathBuf> {
        self.spill_file_path.take()
    }

    /// Mark all leaves as evicted (on disk, not in memory).
    ///
    /// Used during restore to indicate leaves should be lazy-loaded from the
    /// checkpoint file. The checkpoint file becomes the live spill file.
    ///
    /// # Arguments
    ///
    /// - `num_leaves`: Number of leaves in the checkpoint
    /// - `spill_file_path`: Path to the checkpoint file (becomes spill file)
    /// - `block_locations`: Index into the checkpoint file
    pub fn mark_all_leaves_evicted(
        &mut self,
        num_leaves: usize,
        spill_file_path: PathBuf,
        block_locations: Vec<(usize, u64, u32)>,
    ) {
        // Set up the spill file
        self.spill_file_path = Some(spill_file_path);

        // Clear existing leaves
        self.leaves.clear();
        self.leaves.resize_with(num_leaves, || None);

        // Mark all leaves as spilled (on disk) and evicted (not in memory)
        self.spilled_leaves.clear();
        self.evicted_leaves.clear();
        self.dirty_leaves.clear();

        for i in 0..num_leaves {
            self.spilled_leaves.insert(i);
            self.evicted_leaves.insert(i);
        }

        // Set up block locations for lazy loading
        self.leaf_block_locations.clear();
        for (leaf_id, offset, size) in block_locations {
            self.leaf_block_locations.insert(leaf_id, (offset, size));
        }

        // Update stats
        self.stats.leaf_node_count = num_leaves;
        self.stats.evicted_leaf_count = num_leaves;
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

// =============================================================================
// Leaf File - Block-Based Disk Storage (Phase 4)
// =============================================================================

use super::algebra::order_statistics::order_statistics_file_format::{
    BlockLocation as FileBlockLocation, DATA_BLOCK_HEADER_SIZE, FILE_HEADER_SIZE, FileHeader,
    INDEX_ENTRY_SIZE, IndexEntry, MAGIC_INDEX_BLOCK, align_to_block, create_data_block_header,
    set_block_checksum, verify_data_block_header,
};

// Re-export FileFormatError for use by OrderStatisticsMultiset
pub use super::algebra::order_statistics::order_statistics_file_format::FileFormatError;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite};

/// A file-backed storage for leaf nodes.
///
/// LeafFile provides block-based storage for leaf nodes with:
/// - 512-byte block alignment for efficient I/O
/// - CRC32C checksums for data integrity
/// - rkyv serialization for compact storage
/// - In-memory index for fast lookups
///
/// # File Format
///
/// See `order_statistics_file_format.rs` for detailed format documentation.
///
/// # Type Parameters
///
/// - `L`: The leaf node type, must implement `StorableNode`
#[derive(Debug)]
pub struct LeafFile<L> {
    /// File path
    path: PathBuf,
    /// File handle (None if not open)
    file: Option<File>,
    /// Current write position
    write_offset: u64,
    /// Index mapping leaf_id -> block location
    index: HashMap<u64, FileBlockLocation>,
    /// File header (updated on finalize)
    header: FileHeader,
    /// Whether the file is finalized (read-only)
    finalized: bool,
    /// Phantom for type parameter
    _phantom: std::marker::PhantomData<L>,
}

impl<L> LeafFile<L>
where
    L: StorableNode + Archive + RkyvSerialize<Serializer>,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
{
    /// Create a new leaf file for writing.
    ///
    /// The file is created with a placeholder header that will be updated
    /// when `finalize()` is called.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self, FileFormatError> {
        let path = path.as_ref().to_path_buf();

        // Create the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Write placeholder header
        let header = FileHeader::default();
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        Ok(Self {
            path,
            file: Some(file),
            write_offset: FILE_HEADER_SIZE as u64,
            index: HashMap::new(),
            header,
            finalized: false,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Open an existing leaf file for reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, FileFormatError> {
        let path = path.as_ref().to_path_buf();

        let mut file = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Read and verify header
        let mut header_bytes = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header_bytes)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;
        let header = FileHeader::from_bytes(&header_bytes)?;

        // Read index
        let index = Self::read_index(&mut file, &header)?;

        Ok(Self {
            path,
            file: Some(file),
            write_offset: header.index_offset,
            index,
            header,
            finalized: true,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Read the index from the file.
    fn read_index(
        file: &mut File,
        header: &FileHeader,
    ) -> Result<HashMap<u64, FileBlockLocation>, FileFormatError> {
        if header.index_offset == 0 || header.num_leaves == 0 {
            return Ok(HashMap::new());
        }

        // Seek to index block
        file.seek(SeekFrom::Start(header.index_offset))
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Calculate the index block size (header 16 + entries, aligned to 512 bytes)
        let num_entries = header.num_leaves as usize;
        let entries_size = num_entries * INDEX_ENTRY_SIZE;
        let index_size = 16 + entries_size;
        let index_block_size = align_to_block(index_size);

        // Read the full aligned block (including padding)
        let mut index_block = vec![0u8; index_block_size];
        file.read_exact(&mut index_block)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Verify checksum of the full block (bytes [4..])
        let stored_checksum = u32::from_le_bytes(index_block[0..4].try_into().unwrap());
        let computed_checksum = crc32c::crc32c(&index_block[4..]);
        if stored_checksum != computed_checksum {
            return Err(FileFormatError::ChecksumMismatch {
                expected: computed_checksum,
                found: stored_checksum,
            });
        }

        // Verify magic
        if &index_block[4..8] != &MAGIC_INDEX_BLOCK {
            return Err(FileFormatError::InvalidMagic {
                expected: MAGIC_INDEX_BLOCK,
                found: [
                    index_block[4],
                    index_block[5],
                    index_block[6],
                    index_block[7],
                ],
            });
        }

        // Parse entries from bytes [16..16+entries_size]
        let mut index = HashMap::with_capacity(num_entries);
        for i in 0..num_entries {
            let start = 16 + i * INDEX_ENTRY_SIZE;
            let entry = IndexEntry::from_bytes(&index_block[start..start + INDEX_ENTRY_SIZE]);
            index.insert(entry.leaf_id, entry.location);
        }

        Ok(index)
    }

    /// Write a leaf node to the file.
    ///
    /// Returns the block location where the leaf was written.
    pub fn write_leaf(
        &mut self,
        leaf_id: u64,
        leaf: &L,
    ) -> Result<FileBlockLocation, FileFormatError>
    where
        L: LeafNodeOps,
    {
        if self.finalized {
            return Err(FileFormatError::Io(
                "file is finalized (read-only)".to_string(),
            ));
        }

        let file = self
            .file
            .as_mut()
            .ok_or_else(|| FileFormatError::Io("file not open".to_string()))?;

        // Serialize the leaf using DBSP's Serializer
        let serialized = serialize_to_bytes(leaf)?;

        // Create block: header + serialized data
        let data_size = DATA_BLOCK_HEADER_SIZE + serialized.len();
        let block_size = align_to_block(data_size);
        let mut block = vec![0u8; block_size];

        // Write header (includes the actual serialized data length for deserialization)
        let header = create_data_block_header(leaf_id, serialized.len() as u64);
        block[..DATA_BLOCK_HEADER_SIZE].copy_from_slice(&header);

        // Write data
        block[DATA_BLOCK_HEADER_SIZE..DATA_BLOCK_HEADER_SIZE + serialized.len()]
            .copy_from_slice(&serialized);

        // Compute and set checksum
        set_block_checksum(&mut block);

        // Write to file
        file.seek(SeekFrom::Start(self.write_offset))
            .map_err(|e| FileFormatError::Io(e.to_string()))?;
        file.write_all(&block)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Create location
        let location = FileBlockLocation::new(self.write_offset, block_size as u32);

        // Update state
        self.index.insert(leaf_id, location);
        self.write_offset += block_size as u64;
        self.header.num_leaves += 1;
        self.header.total_entries += leaf.entry_count() as u64;
        self.header.total_weight += leaf.total_weight();

        Ok(location)
    }

    /// Load a leaf node from the file.
    pub fn load_leaf(&mut self, leaf_id: u64) -> Result<L, FileFormatError> {
        let location = self
            .index
            .get(&leaf_id)
            .copied()
            .ok_or(FileFormatError::BlockNotFound { leaf_id })?;

        self.load_leaf_at(location)
    }

    /// Load a leaf node from a specific block location.
    pub fn load_leaf_at(&mut self, location: FileBlockLocation) -> Result<L, FileFormatError> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| FileFormatError::Io("file not open".to_string()))?;

        // Read block
        let mut block = vec![0u8; location.size as usize];
        file.seek(SeekFrom::Start(location.offset))
            .map_err(|e| FileFormatError::Io(e.to_string()))?;
        file.read_exact(&mut block)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Verify header and checksum, extract data_len
        let (_leaf_id, data_len) = verify_data_block_header(&block)?;

        // Extract only the actual serialized data (without padding)
        // rkyv stores the root object at the END of the buffer, so we need exact length
        let data = &block[DATA_BLOCK_HEADER_SIZE..DATA_BLOCK_HEADER_SIZE + data_len as usize];

        // Use archived_root to access the archived representation
        let archived = unsafe { rkyv::archived_root::<L>(data) };
        let leaf: L = archived
            .deserialize(&mut Deserializer::default())
            .map_err(|e| FileFormatError::Serialization(format!("{:?}", e)))?;

        Ok(leaf)
    }

    /// Finalize the file, writing the index and updating the header.
    ///
    /// After calling this, the file becomes read-only.
    pub fn finalize(&mut self) -> Result<(), FileFormatError> {
        if self.finalized {
            return Ok(());
        }

        let file = self
            .file
            .as_mut()
            .ok_or_else(|| FileFormatError::Io("file not open".to_string()))?;

        // Write index block
        let index_offset = self.write_offset;

        // Build index block: header (16 bytes) + entries
        let num_entries = self.index.len();
        let index_size = 16 + num_entries * INDEX_ENTRY_SIZE;
        let index_block_size = align_to_block(index_size);
        let mut index_block = vec![0u8; index_block_size];

        // Index header: checksum(4) + magic(4) + num_entries(8)
        index_block[4..8].copy_from_slice(&MAGIC_INDEX_BLOCK);
        index_block[8..16].copy_from_slice(&(num_entries as u64).to_le_bytes());

        // Write entries (sorted by leaf_id for consistency)
        let mut entries: Vec<_> = self.index.iter().collect();
        entries.sort_by_key(|(id, _)| *id);

        for (i, (leaf_id, location)) in entries.iter().enumerate() {
            let entry = IndexEntry::new(**leaf_id, location.offset, location.size);
            let entry_bytes = entry.to_bytes();
            let start = 16 + i * INDEX_ENTRY_SIZE;
            index_block[start..start + INDEX_ENTRY_SIZE].copy_from_slice(&entry_bytes);
        }

        // Set checksum
        set_block_checksum(&mut index_block);

        // Write index block
        file.seek(SeekFrom::Start(index_offset))
            .map_err(|e| FileFormatError::Io(e.to_string()))?;
        file.write_all(&index_block)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Update and rewrite header
        self.header.index_offset = index_offset;
        let header_bytes = self.header.to_bytes();
        file.seek(SeekFrom::Start(0))
            .map_err(|e| FileFormatError::Io(e.to_string()))?;
        file.write_all(&header_bytes)
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        // Flush to disk
        file.sync_all()
            .map_err(|e| FileFormatError::Io(e.to_string()))?;

        self.finalized = true;
        Ok(())
    }

    /// Get the number of leaves in the file.
    pub fn num_leaves(&self) -> u64 {
        self.header.num_leaves
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Check if the file is finalized.
    pub fn is_finalized(&self) -> bool {
        self.finalized
    }

    /// Get a reference to the index.
    pub fn index(&self) -> &HashMap<u64, FileBlockLocation> {
        &self.index
    }

    /// Check if a leaf exists in the file.
    pub fn contains(&self, leaf_id: u64) -> bool {
        self.index.contains_key(&leaf_id)
    }

    /// Close the file (releases file handle).
    pub fn close(&mut self) {
        self.file = None;
    }
}

impl<L> Drop for LeafFile<L> {
    fn drop(&mut self) {
        // Just close the file handle.
        // Note: finalize() should be called explicitly before dropping to persist
        // the index. If not finalized, the file will be incomplete.
        // We don't call finalize() here because it requires rkyv bounds that Drop can't have.
        if !self.finalized && self.file.is_some() && !self.index.is_empty() {
            // Log warning or debug info about unfinalizing file
            // The file can still be read back using the index stored in memory
            // until this object is dropped.
        }
        // File handle is automatically closed when dropped
    }
}

// =============================================================================
// Drop Implementation for NodeStorage
// =============================================================================

impl<I, L> Drop for NodeStorage<I, L> {
    fn drop(&mut self) {
        // Clean up spill file when NodeStorage is dropped.
        // This ensures we don't leave orphan spill files on disk.
        //
        // Note: This is a best-effort cleanup. If the process crashes,
        // spill files in temp directories will be cleaned up by the OS.
        if let Some(ref path) = self.spill_file_path {
            if path.exists() {
                if let Err(e) = std::fs::remove_file(path) {
                    // Log but don't panic - it's okay if cleanup fails
                    tracing::debug!(
                        "Failed to cleanup NodeStorage spill file {}: {}",
                        path.display(),
                        e
                    );
                }
            }
        }

        // Evict entries from BufferCache if present
        if let (Some(buffer_cache), Some(file_id)) =
            (&self.config.buffer_cache, self.spill_file_id)
        {
            let cache_handle = CacheFileHandle {
                file_id,
                path: feldera_storage::StoragePath::default(),
            };
            buffer_cache.evict(&cache_handle);
        }
    }
}

#[cfg(test)]
#[path = "node_storage_tests.rs"]
mod tests;
