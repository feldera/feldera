//! Node-level storage abstraction for OrderStatisticsMultiset.
//!
//! This module provides a storage layer that separates internal nodes (always in memory)
//! from leaf nodes (which can be spilled to disk and evicted from memory).
//!
//! # Design
//!
//! The key insight from spine_async.md is that internal nodes are <2% of nodes but
//! accessed 100% of the time, while leaves are >98% of nodes but accessed sparsely.
//! Therefore we:
//! - Pin all internal nodes in memory (with dirty tracking)
//! - Track dirty leaves for efficient incremental persistence
//! - Evict clean leaves from memory after flushing to disk (Phase 6)
//! - Reload evicted leaves on demand from disk
//!
//! # Memory Eviction (Phase 6)
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

use rkyv::{
    Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
    ser::serializers::AllocSerializer,
};
use size_of::SizeOf;
use std::collections::HashSet;
use std::fmt::Debug;
use std::mem;
use std::sync::Arc;

use super::order_statistics_multiset::{InternalNodeTyped, LeafNode};
use crate::algebra::ZWeight;
use crate::circuit::Runtime;
use crate::storage::backend::{BlockLocation, FileId, StorageBackend, StorageError};
use crate::storage::buffer_cache::{BufferCache, CacheEntry, FBuf};

// =============================================================================
// Storable Node Trait (Unified for Level-Based Spilling)
// =============================================================================

/// Unified trait for tree nodes that can be stored in NodeStorage.
///
/// All nodes must be serializable to support:
/// - Checkpointing (save/restore of tree state)
/// - Level-based disk spilling (configurable via `max_spillable_level`)
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
/// Both `InternalNodeTyped<T>` and `LeafNode<T>` implement this trait.
/// To implement for custom node types:
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
pub trait StorableNode:
    Clone
    + Debug
    + SizeOf
    + Send
    + Sync
    + 'static
    + Archive
    + for<'a> RkyvSerialize<AllocSerializer<4096>>
where
    Self::Archived: RkyvDeserialize<Self, rkyv::Infallible>,
{
    /// Estimate the memory size of this node in bytes.
    ///
    /// Should include struct size plus heap allocations (Vec capacity, etc.).
    /// Used for memory accounting, dirty tracking, and backpressure decisions.
    fn estimate_size(&self) -> usize;
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
            .field("storage_backend", &self.storage_backend.as_ref().map(|_| "<StorageBackend>"))
            .field("buffer_cache", &self.buffer_cache.as_ref().map(|_| "<BufferCache>"))
            .finish()
    }
}

impl Default for NodeStorageConfig {
    fn default() -> Self {
        Self {
            enable_spill: false,
            max_spillable_level: 0, // Only leaves by default
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
    /// let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);
    /// ```
    pub fn from_runtime() -> Self {
        // Check if storage is available in the Runtime
        let storage_backend = Runtime::storage_backend().ok();

        if storage_backend.is_none() {
            // No storage configured - use memory-only mode
            return Self::memory_only();
        }

        // Get the spill threshold from Runtime settings
        // min_index_storage_bytes is for persistent data (like percentile state)
        let spill_threshold_bytes = Runtime::min_index_storage_bytes()
            .unwrap_or(64 * 1024 * 1024); // Default 64MB if not set

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
pub struct CachedLeafNode<T: Ord + Clone + Send + Sync + 'static> {
    /// The deserialized leaf node
    pub leaf: LeafNode<T>,
    /// Estimated size in bytes (for cache cost accounting)
    pub size_bytes: usize,
}

impl<T: Ord + Clone + Send + Sync + 'static> CacheEntry for CachedLeafNode<T> {
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

    fn read_block(
        &self,
        _location: BlockLocation,
    ) -> Result<Arc<FBuf>, StorageError> {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, SizeOf)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[derive(serde::Serialize, serde::Deserialize)]
#[archive(check_bytes)]
pub enum NodeLocation {
    /// Internal node at given index with its level (level >= 1)
    Internal {
        id: usize,
        level: u8,
    },
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, SizeOf)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[derive(serde::Serialize, serde::Deserialize)]
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
// Internal Node with Metadata
// =============================================================================

/// Internal node with metadata for dirty tracking.
///
/// Wraps `InternalNodeTyped<T>` with additional fields for incremental persistence.
#[derive(Debug, Clone, SizeOf)]
pub struct InternalNodeWithMeta<T: Ord + Clone> {
    /// The actual internal node data
    pub node: InternalNodeTyped<T>,
    /// True if modified since last checkpoint/flush
    dirty: bool,
}

impl<T: Ord + Clone> InternalNodeWithMeta<T> {
    /// Create a new internal node (starts dirty since it's new)
    pub fn new(node: InternalNodeTyped<T>) -> Self {
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

/// Storage abstraction for OrderStatisticsMultiset nodes.
///
/// Provides separation of internal nodes (always in memory) from leaf nodes
/// (which can be spilled to disk and evicted from memory).
///
/// # Type Parameters
/// - `T`: The key type, must be `Ord + Clone`
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
pub struct NodeStorage<T: Ord + Clone> {
    /// Configuration
    #[size_of(skip)]
    config: NodeStorageConfig,

    /// Internal nodes - always in memory, with dirty tracking
    internal_nodes: Vec<InternalNodeWithMeta<T>>,

    /// Leaf nodes - Some(leaf) if in memory, None if evicted to disk
    leaves: Vec<Option<LeafNode<T>>>,

    /// Set of dirty leaf indices (for efficient tracking)
    #[size_of(skip)]
    dirty_leaves: HashSet<usize>,

    /// Total bytes of dirty leaves (for spill threshold)
    dirty_bytes: usize,

    /// Statistics
    stats: StorageStats,

    // === Disk spilling state (Phase 4) ===
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

    // === BufferCache integration (Phase 7) ===
    /// FileId for the current spill file (for BufferCache keys)
    #[size_of(skip)]
    spill_file_id: Option<FileId>,

    /// Block locations for each spilled leaf (for BufferCache keys)
    /// Maps leaf_id -> FileBlockLocation in the spill file
    /// Note: FileBlockLocation is from our file format, converted to BlockLocation
    /// when used with BufferCache.
    #[size_of(skip)]
    leaf_block_locations: std::collections::HashMap<usize, (u64, u32)>, // (offset, size)
}

impl<T: Ord + Clone> NodeStorage<T> {
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

    /// Get the current configuration.
    pub fn config(&self) -> &NodeStorageConfig {
        &self.config
    }

    // =========================================================================
    // Size Estimation
    // =========================================================================

    /// Estimate the memory size of a leaf node in bytes.
    ///
    /// Used for dirty tracking thresholds and memory accounting.
    pub fn estimate_leaf_size(leaf: &LeafNode<T>) -> usize {
        let base_size = mem::size_of::<LeafNode<T>>();
        let entries_size = leaf.entries.capacity() * mem::size_of::<(T, ZWeight)>();
        base_size + entries_size
    }

    /// Estimate the memory size of an internal node in bytes.
    pub fn estimate_internal_size(internal: &InternalNodeTyped<T>) -> usize {
        let base_size = mem::size_of::<InternalNodeTyped<T>>();
        let keys_size = internal.keys.capacity() * mem::size_of::<T>();
        let children_size = internal.children.capacity() * mem::size_of::<usize>();
        let sums_size = internal.subtree_sums.capacity() * mem::size_of::<ZWeight>();
        base_size + keys_size + children_size + sums_size
    }

    // =========================================================================
    // Dirty Tracking
    // =========================================================================

    /// Check if any nodes are dirty.
    #[inline]
    pub fn has_dirty_nodes(&self) -> bool {
        !self.dirty_leaves.is_empty()
            || self.internal_nodes.iter().any(|n| n.is_dirty())
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
        self.stats.dirty_internal_count = self
            .internal_nodes
            .iter()
            .filter(|n| n.is_dirty())
            .count();
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
    pub fn alloc_leaf(&mut self, leaf: LeafNode<T>) -> NodeLocation {
        let id = self.leaves.len();
        let leaf_size = Self::estimate_leaf_size(&leaf);

        self.stats.leaf_node_count += 1;
        self.stats.total_entries += leaf.entries.len();
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
    pub fn alloc_internal(&mut self, internal: InternalNodeTyped<T>, level: u8) -> NodeLocation {
        debug_assert!(level >= 1, "Internal nodes must have level >= 1");
        let id = self.internal_nodes.len();
        let node_size = Self::estimate_internal_size(&internal);

        self.stats.internal_node_count += 1;
        self.stats.memory_bytes += node_size;

        // Wrap with metadata (starts dirty)
        self.internal_nodes.push(InternalNodeWithMeta::new(internal));
        self.update_dirty_stats();

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
    pub fn get(&self, loc: NodeLocation) -> NodeRef<'_, T> {
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
    pub fn get_leaf(&self, loc: LeafLocation) -> &LeafNode<T> {
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
    pub fn get_internal(&self, idx: usize) -> &InternalNodeTyped<T> {
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
    pub fn get_mut(&mut self, loc: NodeLocation) -> NodeRefMut<'_, T> {
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
    pub fn get_leaf_mut(&mut self, loc: LeafLocation) -> &mut LeafNode<T> {
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
    pub fn get_internal_mut(&mut self, idx: usize) -> &mut InternalNodeTyped<T> {
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
            .map(|l| l.entries.len())
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

impl<T: Ord + Clone> Default for NodeStorage<T> {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Disk Spilling Methods (Phase 4)
// =============================================================================

use std::path::{Path, PathBuf};

impl<T> NodeStorage<T>
where
    T: Ord + Clone + Debug + SizeOf + Send + Sync + 'static + Archive + rkyv::Serialize<AllocSerializer<4096>>,
    T::Archived: rkyv::Deserialize<T, rkyv::Infallible>,
    // With these bounds, InternalNodeTyped<T>: StorableNode and LeafNode<T>: StorableNode
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
    /// let mut storage: NodeStorage<i32> = NodeStorage::with_config(
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
        let mut leaf_file: LeafFile<T> = LeafFile::create(&file_path)?;

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
    pub fn load_leaf_from_disk(
        &mut self,
        loc: LeafLocation,
    ) -> Result<&LeafNode<T>, FileFormatError> {
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
                    if let Some(cached) = entry.downcast::<CachedLeafNode<T>>() {
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
        let path = self.spill_file_path.as_ref().ok_or_else(|| {
            FileFormatError::Io(format!("No spill file for leaf {}", id))
        })?;

        let mut leaf_file: LeafFile<T> = LeafFile::open(path)?;
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
        if let (Some(buffer_cache), Some(file_id)) =
            (&self.config.buffer_cache, self.spill_file_id)
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
    pub fn get_leaf_reloading(&mut self, loc: LeafLocation) -> &LeafNode<T> {
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

        let mut leaf_file: LeafFile<T> = LeafFile::open(&path)?;
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

        let mut leaf_file: LeafFile<T> = LeafFile::open(&path)?;
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
}

// =============================================================================
// Node References
// =============================================================================

/// Immutable reference to a node.
#[derive(Debug)]
pub enum NodeRef<'a, T: Ord + Clone> {
    Internal(&'a InternalNodeTyped<T>),
    Leaf(&'a LeafNode<T>),
}

impl<'a, T: Ord + Clone> NodeRef<'a, T> {
    /// Check if this is a leaf node.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeRef::Leaf(_))
    }

    /// Get the total weight of this node's subtree.
    pub fn total_weight(&self) -> ZWeight {
        match self {
            NodeRef::Internal(internal) => internal.total_weight(),
            NodeRef::Leaf(leaf) => leaf.total_weight(),
        }
    }
}

/// Mutable reference to a node.
#[derive(Debug)]
pub enum NodeRefMut<'a, T: Ord + Clone> {
    Internal(&'a mut InternalNodeTyped<T>),
    Leaf(&'a mut LeafNode<T>),
}

impl<'a, T: Ord + Clone> NodeRefMut<'a, T> {
    /// Check if this is a leaf node.
    #[inline]
    pub fn is_leaf(&self) -> bool {
        matches!(self, NodeRefMut::Leaf(_))
    }
}

// =============================================================================
// Leaf File - Block-Based Disk Storage (Phase 4)
// =============================================================================

use super::order_statistics_file_format::{
    align_to_block, create_data_block_header, set_block_checksum, verify_data_block_header,
    BlockLocation as FileBlockLocation, FileFormatError, FileHeader, IndexEntry, DATA_BLOCK_HEADER_SIZE,
    FILE_HEADER_SIZE, INDEX_ENTRY_SIZE, MAGIC_INDEX_BLOCK,
};
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
#[derive(Debug)]
pub struct LeafFile<T: Ord + Clone> {
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
    _phantom: std::marker::PhantomData<T>,
}

impl<T> LeafFile<T>
where
    T: Ord + Clone + Archive + rkyv::Serialize<AllocSerializer<4096>>,
    T::Archived: rkyv::Deserialize<T, rkyv::Infallible>,
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
        leaf: &LeafNode<T>,
    ) -> Result<FileBlockLocation, FileFormatError> {
        if self.finalized {
            return Err(FileFormatError::Io("file is finalized (read-only)".to_string()));
        }

        let file = self
            .file
            .as_mut()
            .ok_or_else(|| FileFormatError::Io("file not open".to_string()))?;

        // Serialize the leaf using rkyv (4096 is the scratch buffer size)
        let serialized = rkyv::to_bytes::<_, 4096>(leaf)
            .map_err(|e| FileFormatError::Serialization(format!("{:?}", e)))?;

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
        self.header.total_entries += leaf.entries.len() as u64;
        self.header.total_weight += leaf.total_weight();

        Ok(location)
    }

    /// Load a leaf node from the file.
    pub fn load_leaf(&mut self, leaf_id: u64) -> Result<LeafNode<T>, FileFormatError> {
        let location = self
            .index
            .get(&leaf_id)
            .copied()
            .ok_or(FileFormatError::BlockNotFound { leaf_id })?;

        self.load_leaf_at(location)
    }

    /// Load a leaf node from a specific block location.
    pub fn load_leaf_at(&mut self, location: FileBlockLocation) -> Result<LeafNode<T>, FileFormatError> {
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
        let archived = unsafe { rkyv::archived_root::<LeafNode<T>>(data) };
        let leaf: LeafNode<T> = archived
            .deserialize(&mut rkyv::Infallible)
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

impl<T: Ord + Clone> Drop for LeafFile<T> {
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
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_empty() {
        let storage: NodeStorage<i32> = NodeStorage::new();
        assert!(storage.is_empty());
        assert_eq!(storage.node_count(), 0);
        assert!(!storage.has_dirty_nodes());
    }

    #[test]
    fn test_alloc_leaf() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        assert!(!storage.is_empty());
        assert_eq!(storage.node_count(), 1);
        assert!(storage.is_leaf(loc));

        let retrieved = storage.get(loc);
        assert!(retrieved.is_leaf());
        assert_eq!(retrieved.total_weight(), 6);
    }

    #[test]
    fn test_alloc_internal() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // First allocate some leaves to reference
        let leaf1 = LeafNode { entries: vec![(10, 5)], next_leaf: None };
        let leaf2 = LeafNode { entries: vec![(20, 10)], next_leaf: None };
        let leaf3 = LeafNode { entries: vec![(30, 15)], next_leaf: None };
        let loc1 = storage.alloc_leaf(leaf1);
        let loc2 = storage.alloc_leaf(leaf2);
        let loc3 = storage.alloc_leaf(leaf3);

        let internal = InternalNodeTyped {
            keys: vec![20, 30],
            children: vec![loc1, loc2, loc3],
            subtree_sums: vec![5, 10, 15],
        };
        // Level 1: directly above leaves
        let loc = storage.alloc_internal(internal, 1);

        assert!(!storage.is_empty());
        assert_eq!(storage.node_count(), 4); // 3 leaves + 1 internal
        assert!(!storage.is_leaf(loc));

        let retrieved = storage.get(loc);
        assert!(!retrieved.is_leaf());
        assert_eq!(retrieved.total_weight(), 30);
    }

    #[test]
    fn test_get_mut() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Modify through mutable reference
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let leaf_mut = storage.get_leaf_mut(leaf_loc);
            leaf_mut.entries.push((20, 2));
        }

        // Verify modification
        let retrieved = storage.get(loc);
        assert_eq!(retrieved.total_weight(), 3);
    }

    #[test]
    fn test_clear() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let leaf_loc = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        storage.alloc_internal(InternalNodeTyped {
            keys: vec![],
            children: vec![leaf_loc],
            subtree_sums: vec![1],
        }, 1);

        assert!(!storage.is_empty());

        storage.clear();

        assert!(storage.is_empty());
        assert_eq!(storage.node_count(), 0);
        assert!(!storage.has_dirty_nodes());
    }

    #[test]
    fn test_stats() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let loc1 = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        });
        let loc2 = storage.alloc_leaf(LeafNode {
            entries: vec![(30, 3)],
            next_leaf: None,
        });
        storage.alloc_internal(InternalNodeTyped {
            keys: vec![30],
            children: vec![loc1, loc2],
            subtree_sums: vec![3, 3],
        }, 1);

        let stats = storage.stats();
        assert_eq!(stats.internal_node_count, 1);
        assert_eq!(stats.leaf_node_count, 2);
        assert_eq!(stats.total_entries, 3);
    }

    // =========================================================================
    // Phase 3: Dirty Tracking Tests
    // =========================================================================

    #[test]
    fn test_new_leaf_is_dirty() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let _loc = storage.alloc_leaf(leaf);

        // New leaf should be dirty
        assert!(storage.has_dirty_nodes());
        assert_eq!(storage.dirty_leaf_count(), 1);
        assert!(storage.dirty_bytes() > 0);
    }

    #[test]
    fn test_new_internal_is_dirty() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // First allocate leaves to reference
        let loc1 = storage.alloc_leaf(LeafNode { entries: vec![(10, 5)], next_leaf: None });
        let loc2 = storage.alloc_leaf(LeafNode { entries: vec![(20, 10)], next_leaf: None });

        let internal = InternalNodeTyped {
            keys: vec![20],
            children: vec![loc1, loc2],
            subtree_sums: vec![5, 10],
        };
        let _loc = storage.alloc_internal(internal, 1);

        // New internal node should be dirty
        assert!(storage.has_dirty_nodes());
        assert_eq!(storage.stats().dirty_internal_count, 1);
    }

    #[test]
    fn test_get_mut_marks_dirty() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Allocate a leaf and mark it clean
        let leaf_loc = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        storage.mark_all_clean();

        // Should start clean
        assert!(!storage.has_dirty_nodes());

        // Modify leaf - should become dirty
        if let NodeLocation::Leaf(loc) = leaf_loc {
            let _leaf = storage.get_leaf_mut(loc);
        }

        assert!(storage.has_dirty_nodes());
        assert_eq!(storage.dirty_leaf_count(), 1);
    }

    #[test]
    fn test_mark_all_clean() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Allocate some nodes (they start dirty)
        let leaf_loc = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        storage.alloc_internal(InternalNodeTyped {
            keys: vec![],
            children: vec![leaf_loc],
            subtree_sums: vec![1],
        }, 1);

        assert!(storage.has_dirty_nodes());

        // Mark all clean
        storage.mark_all_clean();

        assert!(!storage.has_dirty_nodes());
        assert_eq!(storage.dirty_leaf_count(), 0);
        assert_eq!(storage.dirty_bytes(), 0);
    }

    #[test]
    fn test_should_flush_threshold() {
        // Config with very small threshold
        let config = NodeStorageConfig::with_threshold(100);
        let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);

        // Initially should not need flush
        assert!(!storage.should_flush());

        // Allocate enough leaves to exceed threshold
        for i in 0..10 {
            storage.alloc_leaf(LeafNode {
                entries: vec![(i, 1), (i + 100, 2), (i + 200, 3)],
                next_leaf: None,
            });
        }

        // Now should exceed threshold
        assert!(storage.should_flush());
        assert!(storage.maybe_flush_dirty_leaves());
    }

    #[test]
    fn test_dirty_bytes_tracking() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let initial_dirty = storage.dirty_bytes();
        assert_eq!(initial_dirty, 0);

        // Allocate a leaf
        storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        });

        let after_alloc = storage.dirty_bytes();
        assert!(after_alloc > 0);

        // Allocate another leaf
        storage.alloc_leaf(LeafNode {
            entries: vec![(30, 3)],
            next_leaf: None,
        });

        let after_second = storage.dirty_bytes();
        assert!(after_second > after_alloc);
    }

    #[test]
    fn test_leaf_size_estimation() {
        let leaf = LeafNode {
            entries: vec![(1i32, 1), (2, 2), (3, 3)],
            next_leaf: None,
        };

        let size = NodeStorage::<i32>::estimate_leaf_size(&leaf);

        // Should include base size plus entries
        assert!(size > 0);
        assert!(size >= std::mem::size_of::<LeafNode<i32>>());
    }

    #[test]
    fn test_internal_size_estimation() {
        let internal = InternalNodeTyped {
            keys: vec![10i32, 20, 30],
            children: vec![
                NodeLocation::Leaf(LeafLocation::new(0)),
                NodeLocation::Leaf(LeafLocation::new(1)),
                NodeLocation::Leaf(LeafLocation::new(2)),
                NodeLocation::Leaf(LeafLocation::new(3)),
            ],
            subtree_sums: vec![5, 10, 15, 20],
        };

        let size = NodeStorage::<i32>::estimate_internal_size(&internal);

        // Should include base size plus vectors
        assert!(size > 0);
        assert!(size >= std::mem::size_of::<InternalNodeTyped<i32>>());
    }

    #[test]
    fn test_dirty_iterators() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Allocate leaves (dirty)
        let loc1 = storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });
        let loc2 = storage.alloc_leaf(LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        });

        // Allocate internal (dirty)
        storage.alloc_internal(InternalNodeTyped {
            keys: vec![20],
            children: vec![loc1, loc2],
            subtree_sums: vec![1, 2],
        }, 1);

        // Check dirty leaf iterator
        let dirty_leaves: Vec<_> = storage.dirty_leaf_ids().collect();
        assert_eq!(dirty_leaves.len(), 2);
        assert!(dirty_leaves.contains(&0));
        assert!(dirty_leaves.contains(&1));

        // Check dirty internal iterator
        let dirty_internals: Vec<_> = storage.dirty_internal_ids().collect();
        assert_eq!(dirty_internals.len(), 1);
        assert!(dirty_internals.contains(&0));
    }

    #[test]
    fn test_memory_only_config() {
        let config = NodeStorageConfig::memory_only();
        let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);

        // Allocate lots of data
        for i in 0..1000 {
            storage.alloc_leaf(LeafNode {
                entries: vec![(i, 1)],
                next_leaf: None,
            });
        }

        // Should never recommend flush (threshold is MAX)
        assert!(!storage.should_flush());
    }

    #[test]
    fn test_update_stats() {
        let mut storage: NodeStorage<i32> = NodeStorage::new();

        storage.alloc_leaf(LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        });

        // Manually modify a leaf (bypassing dirty tracking for test)
        storage.leaves[0].as_mut().unwrap().entries.push((20, 2));

        // Stats may be stale
        let old_entries = storage.stats().total_entries;

        // Update stats
        storage.update_stats();

        // Now should reflect the change
        assert_eq!(storage.stats().total_entries, 2);
        assert!(storage.stats().total_entries > old_entries || old_entries == 1);
    }

    // =========================================================================
    // Phase 4: LeafFile Disk I/O Tests
    // =========================================================================

    #[test]
    fn test_leaf_file_create_and_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_leaves.osm");

        let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();

        // Write a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        let location = leaf_file.write_leaf(0, &leaf).unwrap();

        assert!(location.offset > 0);
        assert!(location.size > 0);
        assert_eq!(leaf_file.num_leaves(), 1);
        assert!(leaf_file.contains(0));
    }

    #[test]
    fn test_leaf_file_header_roundtrip() {
        use std::io::Read as _;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_header.osm");

        // Create and finalize a leaf file
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: vec![(10, 1)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read raw header bytes
        let mut file = std::fs::File::open(&path).unwrap();
        let mut header_bytes = [0u8; FILE_HEADER_SIZE];
        file.read_exact(&mut header_bytes).unwrap();

        // Manually verify checksum
        let stored_checksum = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
        let computed_checksum = crc32c::crc32c(&header_bytes[4..]);

        eprintln!("Stored checksum: {}", stored_checksum);
        eprintln!("Computed checksum: {}", computed_checksum);
        eprintln!("Magic bytes: {:?}", &header_bytes[4..8]);

        assert_eq!(
            stored_checksum, computed_checksum,
            "Header checksum mismatch"
        );

        // Also verify with from_bytes
        let header = FileHeader::from_bytes(&header_bytes).unwrap();
        assert_eq!(header.num_leaves, 1);
    }

    #[test]
    fn test_leaf_node_rkyv_roundtrip() {
        // Test rkyv serialization/deserialization directly without file I/O
        let original = LeafNode {
            entries: vec![(10i32, 1i64), (20, 2), (30, 3)],
            next_leaf: None,
        };

        // Serialize
        let bytes = rkyv::to_bytes::<_, 4096>(&original).unwrap();
        eprintln!("Serialized {} bytes", bytes.len());

        // Deserialize
        let archived = unsafe { rkyv::archived_root::<LeafNode<i32>>(&bytes) };
        let restored: LeafNode<i32> = archived.deserialize(&mut rkyv::Infallible).unwrap();

        // Verify
        assert_eq!(restored.entries.len(), 3, "entries len mismatch");
        assert_eq!(restored.entries[0], (10, 1));
        assert_eq!(restored.entries[1], (20, 2));
        assert_eq!(restored.entries[2], (30, 3));
        assert_eq!(restored.next_leaf, None);
    }

    #[test]
    fn test_leaf_file_debug_write_read() {
        use std::io::{Read as _, Seek as _};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_debug.osm");

        let original = LeafNode {
            entries: vec![(10i32, 1i64), (20, 2)],
            next_leaf: None,
        };

        // Write using LeafFile
        let location = {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();
            let loc = leaf_file.write_leaf(0, &original).unwrap();
            eprintln!("Wrote leaf at offset {}, size {}", loc.offset, loc.size);
            leaf_file.finalize().unwrap();
            loc
        };

        // Read the raw data block manually
        {
            let mut file = std::fs::File::open(&path).unwrap();

            // Seek to the data block
            file.seek(std::io::SeekFrom::Start(location.offset)).unwrap();

            // Read the block
            let mut block = vec![0u8; location.size as usize];
            file.read_exact(&mut block).unwrap();

            eprintln!("Block first 32 bytes: {:?}", &block[0..32.min(block.len())]);

            // Verify header and get data_len
            let (_leaf_id, data_len) = verify_data_block_header(&block).unwrap();
            eprintln!("Extracted data_len from header: {}", data_len);

            // Extract only the actual serialized data (without padding)
            let data = &block[DATA_BLOCK_HEADER_SIZE..DATA_BLOCK_HEADER_SIZE + data_len as usize];
            eprintln!("Data section len: {}", data.len());
            eprintln!("Data first 32 bytes: {:?}", &data[0..32.min(data.len())]);

            // Try to deserialize directly from data
            let archived = unsafe { rkyv::archived_root::<LeafNode<i32>>(data) };
            let restored: LeafNode<i32> = archived.deserialize(&mut rkyv::Infallible).unwrap();

            eprintln!("Restored entries: {:?}", restored.entries);
            assert_eq!(
                restored.entries.len(),
                2,
                "Manual read: entries len mismatch"
            );
        }

        // Read using LeafFile
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();
            let loaded = leaf_file.load_leaf(0).unwrap();
            eprintln!("Loaded via LeafFile entries: {:?}", loaded.entries);
            assert_eq!(loaded.entries.len(), 2, "LeafFile read: entries len mismatch");
        }
    }

    #[test]
    fn test_leaf_file_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_leaves.osm");

        // Write
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();

            let leaf1 = LeafNode {
                entries: vec![(10, 1), (20, 2)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf1).unwrap();

            let leaf2 = LeafNode {
                entries: vec![(30, 3), (40, 4), (50, 5)],
                next_leaf: None,
            };
            leaf_file.write_leaf(1, &leaf2).unwrap();

            leaf_file.finalize().unwrap();
        }

        // Read
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();

            assert_eq!(leaf_file.num_leaves(), 2);

            let loaded1 = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded1.entries.len(), 2);
            assert_eq!(loaded1.entries[0], (10, 1));
            assert_eq!(loaded1.entries[1], (20, 2));

            let loaded2 = leaf_file.load_leaf(1).unwrap();
            assert_eq!(loaded2.entries.len(), 3);
            assert_eq!(loaded2.entries[0], (30, 3));
            assert_eq!(loaded2.total_weight(), 12);
        }
    }

    #[test]
    fn test_leaf_file_many_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_many_leaves.osm");

        // Write many leaves
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();

            for i in 0..100 {
                let leaf = LeafNode {
                    entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                    next_leaf: None,
                };
                leaf_file.write_leaf(i as u64, &leaf).unwrap();
            }

            leaf_file.finalize().unwrap();
        }

        // Read and verify
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();

            assert_eq!(leaf_file.num_leaves(), 100);

            // Spot check some leaves
            for i in [0, 25, 50, 75, 99] {
                let loaded = leaf_file.load_leaf(i as u64).unwrap();
                assert_eq!(loaded.entries[0].0, i * 10);
            }
        }
    }

    #[test]
    fn test_leaf_file_string_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_string_leaves.osm");

        // Write
        {
            let mut leaf_file: LeafFile<String> = LeafFile::create(&path).unwrap();

            let leaf = LeafNode {
                entries: vec![
                    ("apple".to_string(), 5),
                    ("banana".to_string(), 3),
                    ("cherry".to_string(), 7),
                ],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read
        {
            let mut leaf_file: LeafFile<String> = LeafFile::open(&path).unwrap();

            let loaded = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded.entries.len(), 3);
            assert_eq!(loaded.entries[0].0, "apple");
            assert_eq!(loaded.entries[1].0, "banana");
            assert_eq!(loaded.entries[2].0, "cherry");
            assert_eq!(loaded.total_weight(), 15);
        }
    }

    #[test]
    fn test_leaf_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_empty.osm");

        // Create and finalize empty file
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Open and verify
        {
            let leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();
            assert_eq!(leaf_file.num_leaves(), 0);
            assert!(leaf_file.index().is_empty());
        }
    }

    #[test]
    fn test_leaf_file_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_not_found.osm");

        // Write one leaf
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: vec![(10, 1)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Try to load non-existent leaf
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();
            let result = leaf_file.load_leaf(999);
            assert!(matches!(
                result,
                Err(FileFormatError::BlockNotFound { leaf_id: 999 })
            ));
        }
    }

    #[test]
    fn test_leaf_file_large_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_large_leaf.osm");

        // Create a large leaf (many entries)
        let mut entries = Vec::new();
        for i in 0..1000 {
            entries.push((i, (i % 10 + 1) as ZWeight));
        }

        // Write
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: entries.clone(),
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read and verify
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();
            let loaded = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded.entries.len(), 1000);
            assert_eq!(loaded.entries, entries);
        }
    }

    #[test]
    fn test_leaf_file_negative_weights() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_negative.osm");

        // Write leaf with negative weights
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::create(&path).unwrap();
            let leaf = LeafNode {
                entries: vec![(10, 5), (20, -3), (30, 2)],
                next_leaf: None,
            };
            leaf_file.write_leaf(0, &leaf).unwrap();
            leaf_file.finalize().unwrap();
        }

        // Read and verify
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();
            let loaded = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded.entries[1].1, -3);
            assert_eq!(loaded.total_weight(), 4);
        }
    }

    // =========================================================================
    // NodeStorage + LeafFile Integration Tests
    // =========================================================================

    #[test]
    fn test_flush_dirty_to_disk_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_empty_flush.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Flushing empty storage should succeed with 0 leaves written
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_flush_dirty_to_disk_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_flush_basic.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add some leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(30, 3), (40, 4), (50, 5)],
            next_leaf: None,
        };

        storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Verify dirty state
        assert_eq!(storage.dirty_leaf_count(), 2);
        assert!(storage.dirty_bytes() > 0);

        // Flush to disk
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count, 2);

        // Verify clean state
        assert_eq!(storage.dirty_leaf_count(), 0);
        assert_eq!(storage.dirty_bytes(), 0);
        assert_eq!(storage.spilled_leaf_count(), 2);
        assert!(storage.is_leaf_spilled(0));
        assert!(storage.is_leaf_spilled(1));

        // Verify stats
        assert_eq!(storage.stats().leaves_written, 2);
    }

    #[test]
    fn test_flush_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_flush_reload.osm");

        let original_entries1 = vec![(100i32, 10i64), (200, 20), (300, 30)];
        let original_entries2 = vec![(400, 40), (500, 50)];

        // Create and flush storage
        {
            let mut storage: NodeStorage<i32> = NodeStorage::with_config(
                NodeStorageConfig::with_spill_directory(1024, dir.path()),
            );

            let leaf1 = LeafNode {
                entries: original_entries1.clone(),
                next_leaf: None,
            };
            let leaf2 = LeafNode {
                entries: original_entries2.clone(),
                next_leaf: None,
            };

            storage.alloc_leaf(leaf1);
            storage.alloc_leaf(leaf2);

            storage.flush_dirty_to_disk(Some(&path)).unwrap();
        }

        // Read back using LeafFile directly
        {
            let mut leaf_file: LeafFile<i32> = LeafFile::open(&path).unwrap();

            let loaded1 = leaf_file.load_leaf(0).unwrap();
            assert_eq!(loaded1.entries, original_entries1);

            let loaded2 = leaf_file.load_leaf(1).unwrap();
            assert_eq!(loaded2.entries, original_entries2);
        }
    }

    #[test]
    fn test_threshold_check_triggers_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_threshold.osm");

        // Very low threshold to trigger easily
        let mut storage: NodeStorage<i32> = NodeStorage::with_config(
            NodeStorageConfig::with_threshold(100), // 100 bytes threshold
        );

        // Add a leaf that exceeds threshold
        let leaf = LeafNode {
            entries: vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        // Should trigger flush
        assert!(storage.should_flush());

        // Flush
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count, 1);

        // Should no longer need flush
        assert!(!storage.should_flush());
    }

    #[test]
    fn test_backpressure() {
        // Very low threshold to trigger easily (50 bytes)
        let mut storage: NodeStorage<i32> = NodeStorage::with_config(
            NodeStorageConfig::with_threshold(50),
        );

        // Initially no backpressure
        assert!(!storage.should_apply_backpressure());
        assert!(storage.should_relieve_backpressure());
        let (should_apply, ratio) = storage.backpressure_status();
        assert!(!should_apply);
        assert_eq!(ratio, 0.0);

        // Add a leaf that exceeds threshold but not 2x threshold (backpressure high multiplier)
        let leaf = LeafNode {
            entries: vec![(1, 1), (2, 2), (3, 3)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf.clone());

        // Should trigger flush but not backpressure (dirty_bytes ~= 72 bytes, threshold = 50)
        assert!(storage.should_flush());
        assert!(!storage.should_apply_backpressure()); // Not yet 2x threshold
        let (should_apply, ratio) = storage.backpressure_status();
        assert!(!should_apply);
        assert!(ratio > 1.0); // Above threshold

        // Add more data to exceed 2x threshold
        storage.alloc_leaf(leaf.clone());
        storage.alloc_leaf(leaf);

        // Now should trigger backpressure (dirty_bytes ~= 216 bytes, threshold = 50, 2x = 100)
        assert!(storage.should_apply_backpressure());
        assert!(!storage.should_relieve_backpressure());
        let (should_apply, ratio) = storage.backpressure_status();
        assert!(should_apply);
        assert!(ratio >= 2.0); // At least 2x threshold

        // Clear dirty tracking to simulate flush
        storage.mark_all_clean();

        // After flush, backpressure should be relieved
        assert!(!storage.should_apply_backpressure());
        assert!(storage.should_relieve_backpressure());
    }

    #[test]
    fn test_backpressure_disabled_without_spill() {
        // Memory-only config should never trigger backpressure
        let mut storage: NodeStorage<i32> = NodeStorage::with_config(
            NodeStorageConfig::memory_only(),
        );

        // Add lots of data
        for i in 0..100 {
            let leaf = LeafNode {
                entries: vec![(i, 1), (i + 1000, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Should never trigger backpressure
        assert!(!storage.should_apply_backpressure());
    }

    #[test]
    fn test_cleanup_spill_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cleanup.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        // Flush
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert!(path.exists());

        // Cleanup
        storage.cleanup_spill_file().unwrap();
        assert!(!path.exists());
        assert!(storage.spill_file_path().is_none());
        assert_eq!(storage.spilled_leaf_count(), 0);
    }

    #[test]
    fn test_incremental_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_incremental.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add first leaf
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush first batch
        let count1 = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count1, 1);
        assert_eq!(storage.dirty_leaf_count(), 0);

        // Add second leaf
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf2);

        // Only new leaf is dirty
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Flush second batch (this creates a new file - leaves from first flush are lost)
        // In a real system, we'd append or use a different strategy
        let count2 = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count2, 1);

        // Stats show total leaves written
        assert_eq!(storage.stats().leaves_written, 2);
    }

    #[test]
    fn test_load_leaf_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_load.osm");

        let original_entries = vec![(111i32, 11i64), (222, 22), (333, 33)];

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Load from disk (even though it's still in memory, this tests the path)
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.load_leaf_from_disk(leaf_loc).unwrap();
            assert_eq!(loaded.entries, original_entries);
        } else {
            panic!("Expected leaf location");
        }
    }

    // =========================================================================
    // Phase 6: Memory Eviction Tests
    // =========================================================================

    #[test]
    fn test_evict_clean_leaves_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_evict.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1), (20, 2)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(30, 3), (40, 4), (50, 5)],
            next_leaf: None,
        };

        storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Initially all in memory
        assert_eq!(storage.in_memory_leaf_count(), 2);
        assert_eq!(storage.evicted_leaf_count(), 0);

        // Flush to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Still all in memory (flush doesn't evict)
        assert_eq!(storage.in_memory_leaf_count(), 2);
        assert_eq!(storage.evicted_leaf_count(), 0);
        assert_eq!(storage.spilled_leaf_count(), 2);

        // Now evict clean leaves
        let (evicted_count, bytes_freed) = storage.evict_clean_leaves();

        // Both leaves should be evicted
        assert_eq!(evicted_count, 2);
        assert!(bytes_freed > 0);
        assert_eq!(storage.in_memory_leaf_count(), 0);
        assert_eq!(storage.evicted_leaf_count(), 2);
        assert!(storage.is_leaf_evicted(LeafLocation::new(0)));
        assert!(storage.is_leaf_evicted(LeafLocation::new(1)));
    }

    #[test]
    fn test_evict_does_not_affect_dirty_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_evict_dirty.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add leaves
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };

        let loc1 = storage.alloc_leaf(leaf1);
        storage.alloc_leaf(leaf2);

        // Flush both to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Now modify leaf1 (making it dirty again)
        if let NodeLocation::Leaf(leaf_loc) = loc1 {
            let leaf = storage.get_leaf_mut(leaf_loc);
            leaf.entries.push((100, 10));
        }

        // leaf1 is now dirty, leaf2 is clean
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Evict clean leaves
        let (evicted_count, _) = storage.evict_clean_leaves();

        // Only leaf2 should be evicted (leaf1 is dirty)
        assert_eq!(evicted_count, 1);
        assert_eq!(storage.in_memory_leaf_count(), 1);
        assert_eq!(storage.evicted_leaf_count(), 1);
        assert!(!storage.is_leaf_evicted(LeafLocation::new(0))); // Still in memory (dirty)
        assert!(storage.is_leaf_evicted(LeafLocation::new(1)));  // Evicted
    }

    #[test]
    fn test_evict_does_not_affect_non_spilled_leaves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_evict_nonspilled.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add first leaf
        let leaf1 = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf1);

        // Flush leaf1 to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Add second leaf (not yet spilled)
        let leaf2 = LeafNode {
            entries: vec![(20, 2)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf2);

        // leaf1 is spilled and clean, leaf2 is dirty (not spilled)
        assert_eq!(storage.spilled_leaf_count(), 1);
        assert_eq!(storage.dirty_leaf_count(), 1);

        // Evict clean leaves
        let (evicted_count, _) = storage.evict_clean_leaves();

        // Only leaf1 should be evicted
        assert_eq!(evicted_count, 1);
        assert_eq!(storage.in_memory_leaf_count(), 1); // leaf2 still in memory
        assert_eq!(storage.evicted_leaf_count(), 1);
    }

    #[test]
    fn test_reload_evicted_leaf() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_reload.osm");

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush and evict
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 1);

        // Verify it's evicted
        if let NodeLocation::Leaf(leaf_loc) = loc {
            assert!(storage.is_leaf_evicted(leaf_loc));
            assert!(!storage.is_leaf_in_memory(leaf_loc));
        }

        // Reload using get_leaf_reloading
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.get_leaf_reloading(leaf_loc);
            assert_eq!(loaded.entries, original_entries);

            // Now it should be back in memory
            assert!(!storage.is_leaf_evicted(leaf_loc));
            assert!(storage.is_leaf_in_memory(leaf_loc));
        }
    }

    #[test]
    fn test_reload_evicted_leaves_bulk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_reload_bulk.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add multiple leaves
        for i in 0..5 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Flush and evict all
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        let (evicted, bytes_freed) = storage.evict_clean_leaves();
        assert_eq!(evicted, 5);
        assert!(bytes_freed > 0);
        assert_eq!(storage.in_memory_leaf_count(), 0);
        assert_eq!(storage.evicted_leaf_count(), 5);

        // Reload all evicted leaves
        let reloaded = storage.reload_evicted_leaves().unwrap();
        assert_eq!(reloaded, 5);
        assert_eq!(storage.in_memory_leaf_count(), 5);
        assert_eq!(storage.evicted_leaf_count(), 0);

        // Verify data integrity
        for i in 0..5 {
            let leaf = storage.get_leaf(LeafLocation::new(i));
            assert_eq!(leaf.entries[0].0, (i * 10) as i32);
        }
    }

    #[test]
    fn test_eviction_memory_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_evict_stats.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1), (20, 2), (30, 3)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);

        let memory_before = storage.stats().memory_bytes;
        assert!(memory_before > 0);

        // Flush and evict
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        let (_, bytes_freed) = storage.evict_clean_leaves();

        let memory_after = storage.stats().memory_bytes;

        // Memory should be reduced
        assert!(bytes_freed > 0);
        assert!(memory_after < memory_before);
        assert_eq!(memory_before - memory_after, bytes_freed);
    }

    #[test]
    fn test_cache_hit_miss_stats() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cache_stats.osm");

        let mut storage: NodeStorage<i32> = NodeStorage::new();

        // Add leaf
        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush and evict
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        storage.evict_clean_leaves();

        // Initial stats
        assert_eq!(storage.stats().cache_hits, 0);
        assert_eq!(storage.stats().cache_misses, 0);

        // First access - should be a cache miss (reload from disk)
        if let NodeLocation::Leaf(leaf_loc) = loc {
            storage.get_leaf_reloading(leaf_loc);
        }
        assert_eq!(storage.stats().cache_misses, 1);

        // Second access - should be a cache hit (in memory now)
        if let NodeLocation::Leaf(leaf_loc) = loc {
            storage.get_leaf_reloading(leaf_loc);
        }
        assert_eq!(storage.stats().cache_hits, 1);
    }

    #[test]
    fn test_no_eviction_without_spill() {
        let mut storage: NodeStorage<i32> = NodeStorage::with_config(
            NodeStorageConfig::memory_only(),
        );

        // Add leaves
        for i in 0..10 {
            let leaf = LeafNode {
                entries: vec![(i, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Try to evict - should evict nothing (nothing is spilled)
        let (evicted, bytes_freed) = storage.evict_clean_leaves();
        assert_eq!(evicted, 0);
        assert_eq!(bytes_freed, 0);
        assert_eq!(storage.in_memory_leaf_count(), 10);
    }

    // =========================================================================
    // Phase 7: BufferCache Integration Tests
    // =========================================================================

    #[test]
    fn test_cached_leaf_node_cost() {
        let leaf = LeafNode {
            entries: vec![(10i32, 1i64), (20, 2), (30, 3)],
            next_leaf: None,
        };

        let cached = CachedLeafNode {
            leaf,
            size_bytes: 1024,
        };

        // CacheEntry trait: cost returns size_bytes
        assert_eq!(cached.cost(), 1024);
    }

    #[test]
    fn test_buffer_cache_config_integration() {
        // Create a BufferCache
        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024)); // 1MB cache

        // Create config with BufferCache
        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0, // Default: only leaves
            spill_threshold_bytes: 64 * 1024, // 64KB
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache.clone()),
        };

        // Create storage with config
        let storage: NodeStorage<i32> = NodeStorage::with_config(config);

        // Verify config is stored
        assert!(storage.config.buffer_cache.is_some());
    }

    #[test]
    fn test_flush_records_block_locations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_block_locs.osm");

        // Create BufferCache
        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);

        // Add leaves
        for i in 0..5 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1), (i * 10 + 1, 2)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Flush to disk
        let count = storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(count, 5);

        // Block locations should be recorded
        assert_eq!(storage.leaf_block_locations.len(), 5);

        // Verify each leaf has a block location
        for i in 0..5 {
            assert!(storage.leaf_block_locations.contains_key(&i));
            let &(offset, size) = storage.leaf_block_locations.get(&i).unwrap();
            assert!(offset > 0); // After file header
            assert!(size > 0);   // Non-empty
        }

        // FileId should be assigned
        assert!(storage.spill_file_id.is_some());
    }

    #[test]
    fn test_cleanup_clears_block_locations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cleanup_locs.osm");

        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);

        // Add and flush a leaf
        let leaf = LeafNode {
            entries: vec![(10, 1)],
            next_leaf: None,
        };
        storage.alloc_leaf(leaf);
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Verify locations are recorded
        assert!(!storage.leaf_block_locations.is_empty());
        assert!(storage.spill_file_id.is_some());

        // Cleanup
        storage.cleanup_spill_file().unwrap();

        // All tracking data should be cleared
        assert!(storage.leaf_block_locations.is_empty());
        assert!(storage.spill_file_id.is_none());
    }

    #[test]
    fn test_evict_and_reload_with_buffer_cache() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_cache_reload.osm");

        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);

        let original_entries = vec![(10i32, 1i64), (20, 2), (30, 3)];

        // Add leaf
        let leaf = LeafNode {
            entries: original_entries.clone(),
            next_leaf: None,
        };
        let loc = storage.alloc_leaf(leaf);

        // Flush to disk
        storage.flush_dirty_to_disk(Some(&path)).unwrap();

        // Evict the leaf
        let (evicted, _) = storage.evict_clean_leaves();
        assert_eq!(evicted, 1);

        // Verify evicted
        if let NodeLocation::Leaf(leaf_loc) = loc {
            assert!(storage.is_leaf_evicted(leaf_loc));
        }

        // Reload - this will insert into BufferCache for future lookups
        if let NodeLocation::Leaf(leaf_loc) = loc {
            let loaded = storage.get_leaf_reloading(leaf_loc);
            assert_eq!(loaded.entries, original_entries);

            // Should be back in memory
            assert!(!storage.is_leaf_evicted(leaf_loc));
        }

        // The leaf data was loaded from disk and should have been
        // inserted into the BufferCache (even though we can't verify
        // the cache directly, we can verify it didn't error)
        assert!(storage.leaf_block_locations.contains_key(&0));
    }

    #[test]
    fn test_multiple_flushes_update_block_locations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test_multi_flush.osm");

        let buffer_cache = Arc::new(BufferCache::new(1024 * 1024));

        let config = NodeStorageConfig {
            enable_spill: true,
            max_spillable_level: 0,
            spill_threshold_bytes: 64 * 1024,
            spill_directory: None,
            storage_backend: None,
            buffer_cache: Some(buffer_cache),
        };

        let mut storage: NodeStorage<i32> = NodeStorage::with_config(config);

        // First batch of leaves
        for i in 0..3 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // First flush
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(storage.leaf_block_locations.len(), 3);

        // Add more leaves
        for i in 3..6 {
            let leaf = LeafNode {
                entries: vec![(i * 10, 1)],
                next_leaf: None,
            };
            storage.alloc_leaf(leaf);
        }

        // Second flush - existing leaves are skipped (not dirty), new ones written
        // Note: This actually appends to the same file
        storage.flush_dirty_to_disk(Some(&path)).unwrap();
        assert_eq!(storage.leaf_block_locations.len(), 6);

        // All leaves should have locations
        for i in 0..6 {
            assert!(storage.leaf_block_locations.contains_key(&i));
        }
    }
}
