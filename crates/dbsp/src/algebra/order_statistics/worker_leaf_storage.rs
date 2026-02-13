//! Per-worker sparse leaf storage for virtually-sharded B+ tree.
//!
//! Each DBSP worker persistently owns a contiguous range of leaves from the
//! shared logical tree. Workers manage their own flush/evict cycles
//! independently, writing to per-worker segment files.
//!
//! # Architecture
//!
//! - Worker 0 ("owner") holds the `TreeStructure`: internal nodes, root, metadata
//! - Each worker holds a `WorkerLeafStorage<V>`: a sparse set of leaves it owns
//! - Workers read the owner's internal nodes via `ReadOnlyTreeView` (raw pointer)
//! - Leaf ownership is persistent across steps; boundaries shift via cascade rebalancing
//!
//! # Disk I/O
//!
//! Each worker has its own segment files. Flush and evict are independent:
//! - `flush_dirty_to_disk()` writes dirty leaves to a new segment file
//! - `evict_clean_leaves()` frees memory for leaves already on disk
//! - Workers reuse `write_leaves_to_segment()` from `node_storage_disk.rs`

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rkyv::{Archive, Serialize as RkyvSerialize};

use crate::algebra::order_statistics::order_statistics_zset::LeafNode;
use crate::algebra::ZWeight;
use crate::node_storage::{
    CachedLeafSummary, LeafDiskLocation, LeafLocation, SegmentId, SegmentMetadata,
};
use crate::node_storage::serialize_to_bytes;
use crate::storage::backend::{FileReader, StorageBackend, StoragePath};
use crate::storage::file::Serializer;
use size_of::SizeOf;

use crate::DBData;

use super::parallel_routing::build_cached_summary;

// =============================================================================
// LeafSlot: Present or Evicted state per leaf
// =============================================================================

/// State of a leaf in worker storage: in memory or evicted to disk.
#[derive(Debug, Clone)]
pub(crate) enum WorkerLeafSlot<V> {
    /// Leaf data is in memory.
    Present(LeafNode<V>),
    /// Leaf was evicted to disk; summary cached for subtree-sum queries.
    Evicted(CachedLeafSummary),
}

impl<V> WorkerLeafSlot<V> {
    #[inline]
    pub fn is_present(&self) -> bool {
        matches!(self, WorkerLeafSlot::Present(_))
    }

    #[inline]
    pub fn is_evicted(&self) -> bool {
        matches!(self, WorkerLeafSlot::Evicted(_))
    }

    #[inline]
    pub fn as_present(&self) -> Option<&LeafNode<V>> {
        match self {
            WorkerLeafSlot::Present(leaf) => Some(leaf),
            WorkerLeafSlot::Evicted(_) => None,
        }
    }

    #[inline]
    pub fn as_present_mut(&mut self) -> Option<&mut LeafNode<V>> {
        match self {
            WorkerLeafSlot::Present(leaf) => Some(leaf),
            WorkerLeafSlot::Evicted(_) => None,
        }
    }

    /// Get the cached weight (works for both present and evicted leaves).
    pub fn weight(&self) -> ZWeight
    where
        V: Ord + Clone,
    {
        match self {
            WorkerLeafSlot::Present(leaf) => leaf.total_weight(),
            WorkerLeafSlot::Evicted(summary) => summary.weight_sum,
        }
    }

    /// Get entry count (works for both present and evicted leaves).
    pub fn entry_count(&self) -> usize
    where
        V: Ord + Clone,
    {
        match self {
            WorkerLeafSlot::Present(leaf) => leaf.entries.len(),
            WorkerLeafSlot::Evicted(summary) => summary.entry_count,
        }
    }
}

// =============================================================================
// WorkerLeafStorage: Sparse per-worker leaf storage
// =============================================================================

/// Sparse per-worker leaf storage for the virtual sharding protocol.
///
/// Holds a subset of the global tree's leaves (identified by their global leaf
/// IDs). Each worker manages its own segment files for flush/evict.
///
/// Global leaf IDs are allocated by the owner; this storage maps them to
/// in-memory or on-disk state.
pub(crate) struct WorkerLeafStorage<V: Ord + Clone> {
    /// Worker identity.
    worker_id: usize,

    /// Sparse leaf storage: global_leaf_id → slot.
    leaves: HashMap<usize, WorkerLeafSlot<V>>,

    /// Set of dirty leaf IDs (modified since last flush).
    dirty_leaves: HashSet<usize>,

    /// Total bytes of dirty leaf data (for spill threshold).
    dirty_bytes: usize,

    /// Completed segment files owned by this worker.
    segments: Vec<SegmentMetadata>,

    /// Next segment ID counter (unique per worker, assigned by owner).
    next_segment_id: u64,

    /// Maps leaf_id → disk location for evicted leaves.
    leaf_disk_locations: HashMap<usize, LeafDiskLocation>,

    /// Storage backend for file I/O.
    storage_backend: Option<Arc<dyn StorageBackend>>,

    /// Segment file path prefix (e.g., "t0_w1_" for tree 0, worker 1).
    segment_path_prefix: String,

    /// Spill directory for segment files.
    spill_directory: Option<StoragePath>,

    /// Spill threshold in bytes (flush when dirty_bytes >= this).
    spill_threshold_bytes: usize,

    /// Current owned leaf range: [start, end) — persistent across steps.
    /// Updated during cascade rebalancing.
    owned_range: (usize, usize),

    /// Whether this worker has bootstrapped (cloned initial leaves from owner).
    bootstrapped: bool,
}

impl<V: DBData> WorkerLeafStorage<V>
where
    V: Archive + RkyvSerialize<Serializer>,
{
    /// Create empty worker leaf storage.
    pub fn new(
        worker_id: usize,
        storage_backend: Option<Arc<dyn StorageBackend>>,
        segment_path_prefix: String,
        spill_directory: Option<StoragePath>,
        spill_threshold_bytes: usize,
    ) -> Self {
        Self {
            worker_id,
            leaves: HashMap::new(),
            dirty_leaves: HashSet::new(),
            dirty_bytes: 0,
            segments: Vec::new(),
            next_segment_id: 0,
            leaf_disk_locations: HashMap::new(),
            storage_backend,
            segment_path_prefix,
            spill_directory,
            spill_threshold_bytes,
            owned_range: (0, 0),
            bootstrapped: false,
        }
    }

    // =========================================================================
    // Leaf Access
    // =========================================================================

    /// Get the number of owned leaves.
    #[inline]
    pub fn num_leaves(&self) -> usize {
        self.leaves.len()
    }

    /// Check if this worker owns a given leaf.
    #[inline]
    pub fn owns_leaf(&self, leaf_id: usize) -> bool {
        self.leaves.contains_key(&leaf_id)
    }

    /// Get an immutable reference to a leaf (must be present, not evicted).
    ///
    /// # Panics
    /// Panics if the leaf is not owned or is evicted.
    pub fn get_leaf(&self, leaf_id: usize) -> &LeafNode<V> {
        self.leaves[&leaf_id]
            .as_present()
            .expect("leaf must be present in memory")
    }

    /// Get a mutable reference to a leaf, marking it dirty.
    ///
    /// # Panics
    /// Panics if the leaf is not owned or is evicted.
    pub fn get_leaf_mut(&mut self, leaf_id: usize) -> &mut LeafNode<V> {
        self.mark_dirty(leaf_id);
        self.leaves
            .get_mut(&leaf_id)
            .expect("leaf must exist")
            .as_present_mut()
            .expect("leaf must be present in memory")
    }

    /// Get the slot for a leaf (present or evicted).
    pub fn get_slot(&self, leaf_id: usize) -> Option<&WorkerLeafSlot<V>> {
        self.leaves.get(&leaf_id)
    }

    /// Get the weight of a leaf (works for both present and evicted).
    pub fn leaf_weight(&self, leaf_id: usize) -> ZWeight {
        self.leaves
            .get(&leaf_id)
            .map(|slot| slot.weight())
            .unwrap_or(0)
    }

    /// Check if a leaf is currently in memory (not evicted).
    pub fn is_leaf_present(&self, leaf_id: usize) -> bool {
        self.leaves
            .get(&leaf_id)
            .map(|s| s.is_present())
            .unwrap_or(false)
    }

    /// Iterate over all owned leaf IDs.
    pub fn leaf_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.leaves.keys().copied()
    }

    // =========================================================================
    // Leaf Mutation
    // =========================================================================

    /// Insert a leaf into this worker's storage.
    ///
    /// The leaf is marked dirty immediately.
    pub fn insert_leaf(&mut self, leaf_id: usize, leaf: LeafNode<V>) {
        let size = estimate_leaf_size(&leaf);
        self.dirty_leaves.insert(leaf_id);
        self.dirty_bytes += size;
        self.leaves
            .insert(leaf_id, WorkerLeafSlot::Present(leaf));
    }

    /// Remove a leaf from this worker's storage (for transfers).
    ///
    /// Returns the leaf if it was present and in memory.
    pub fn remove_leaf(&mut self, leaf_id: usize) -> Option<LeafNode<V>> {
        if let Some(slot) = self.leaves.remove(&leaf_id) {
            self.dirty_leaves.remove(&leaf_id);
            self.leaf_disk_locations.remove(&leaf_id);
            match slot {
                WorkerLeafSlot::Present(leaf) => {
                    let size = estimate_leaf_size(&leaf);
                    self.dirty_bytes = self.dirty_bytes.saturating_sub(size);
                    Some(leaf)
                }
                WorkerLeafSlot::Evicted(_) => None,
            }
        } else {
            None
        }
    }

    /// Replace a leaf in-place, marking it dirty.
    pub fn replace_leaf(&mut self, leaf_id: usize, leaf: LeafNode<V>) {
        let size = estimate_leaf_size(&leaf);
        self.dirty_leaves.insert(leaf_id);
        self.dirty_bytes += size;
        self.leaves
            .insert(leaf_id, WorkerLeafSlot::Present(leaf));
    }

    /// Mark a leaf as dirty.
    fn mark_dirty(&mut self, leaf_id: usize) {
        if !self.dirty_leaves.contains(&leaf_id) {
            if let Some(slot) = self.leaves.get(&leaf_id) {
                if let Some(leaf) = slot.as_present() {
                    let size = estimate_leaf_size(leaf);
                    self.dirty_leaves.insert(leaf_id);
                    self.dirty_bytes += size;
                }
            }
        }
    }

    // =========================================================================
    // Flush / Evict (independent per worker)
    // =========================================================================

    /// Check if dirty data exceeds the spill threshold.
    pub fn should_flush(&self) -> bool {
        self.storage_backend.is_some() && self.dirty_bytes >= self.spill_threshold_bytes
    }

    /// Flush dirty leaves to a new segment file.
    ///
    /// Returns the number of leaves flushed, or an error.
    pub fn flush_dirty_to_disk(&mut self) -> Result<usize, String> {
        let backend = match &self.storage_backend {
            Some(b) => b.clone(),
            None => return Err("No storage backend configured".to_string()),
        };

        if self.dirty_leaves.is_empty() {
            return Ok(0);
        }

        // Collect dirty leaves sorted by ID for deterministic output.
        let mut dirty_ids: Vec<usize> = self.dirty_leaves.iter().copied().collect();
        dirty_ids.sort();

        // Serialize dirty leaves.
        let mut leaf_blocks: Vec<(usize, crate::storage::buffer_cache::FBuf)> = Vec::new();
        for &leaf_id in &dirty_ids {
            if let Some(WorkerLeafSlot::Present(leaf)) = self.leaves.get(&leaf_id) {
                let serialized = serialize_to_bytes(leaf)
                    .map_err(|e| format!("Failed to serialize leaf {}: {:?}", leaf_id, e))?;
                leaf_blocks.push((leaf_id, serialized));
            }
        }

        if leaf_blocks.is_empty() {
            return Ok(0);
        }

        // Write to segment file.
        let seg_id = SegmentId::new(self.next_segment_id);
        self.next_segment_id += 1;

        let filename = format!(
            "{}segment_{}.dat",
            self.segment_path_prefix,
            seg_id.value()
        );
        let path = if let Some(ref dir) = self.spill_directory {
            dir.child(filename.as_str())
        } else {
            StoragePath::from(filename.as_str())
        };

        let (reader, leaf_index, file_size) =
            crate::node_storage::write_leaves_to_segment(&backend, &path, &leaf_blocks)
                .map_err(|e| format!("Failed to write segment: {:?}", e))?;

        // Update disk locations.
        for (&leaf_id, &(offset, size)) in &leaf_index {
            self.leaf_disk_locations.insert(
                leaf_id,
                LeafDiskLocation::new(seg_id, offset, size),
            );
        }

        // Store segment metadata.
        let mut segment = SegmentMetadata::with_reader(seg_id, path, reader);
        segment.leaf_index = leaf_index;
        segment.size_bytes = file_size;
        segment.leaf_count = leaf_blocks.len();
        self.segments.push(segment);

        let flushed = leaf_blocks.len();

        // Clear dirty tracking.
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;

        Ok(flushed)
    }

    /// Evict clean leaves from memory (leaves that have been flushed to disk).
    ///
    /// Returns (evicted_count, bytes_freed).
    pub fn evict_clean_leaves(&mut self) -> (usize, usize) {
        let mut evicted = 0;
        let mut bytes_freed = 0;

        let ids_to_check: Vec<usize> = self
            .leaves
            .iter()
            .filter_map(|(&id, slot)| {
                if slot.is_present()
                    && self.leaf_disk_locations.contains_key(&id)
                    && !self.dirty_leaves.contains(&id)
                {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();

        for leaf_id in ids_to_check {
            if let Some(WorkerLeafSlot::Present(leaf)) = self.leaves.get(&leaf_id) {
                let size = estimate_leaf_size(leaf);
                let summary = build_cached_summary(leaf);
                self.leaves
                    .insert(leaf_id, WorkerLeafSlot::Evicted(summary));
                bytes_freed += size;
                evicted += 1;
            }
        }

        (evicted, bytes_freed)
    }

    /// Flush dirty leaves and evict clean leaves.
    pub fn flush_and_evict(&mut self) -> Result<(usize, usize, usize), String> {
        let flushed = self.flush_dirty_to_disk()?;
        let (evicted, freed) = self.evict_clean_leaves();
        Ok((flushed, evicted, freed))
    }

    // =========================================================================
    // Leaf Transfer (for cascade rebalancing)
    // =========================================================================

    /// Transfer a leaf to another worker.
    ///
    /// Returns a `LeafTransferData` containing the leaf data and optional
    /// disk reader reference, or None if the leaf is not owned.
    pub fn take_leaf_for_transfer(&mut self, leaf_id: usize) -> Option<LeafTransferData<V>> {
        let slot = self.leaves.remove(&leaf_id)?;
        self.dirty_leaves.remove(&leaf_id);
        let disk_loc = self.leaf_disk_locations.remove(&leaf_id);

        // Find the segment reader if the leaf is on disk.
        let disk_reader = disk_loc.and_then(|loc| {
            self.segments
                .iter()
                .find(|s| s.id == loc.segment_id)
                .and_then(|s| s.reader.clone())
                .map(|reader| (reader, loc))
        });

        match slot {
            WorkerLeafSlot::Present(leaf) => {
                let size = estimate_leaf_size(&leaf);
                self.dirty_bytes = self.dirty_bytes.saturating_sub(size);
                Some(LeafTransferData {
                    leaf_id,
                    leaf: Some(leaf),
                    disk_reader,
                })
            }
            WorkerLeafSlot::Evicted(_summary) => Some(LeafTransferData {
                leaf_id,
                leaf: None,
                disk_reader: disk_reader.or_else(|| {
                    // Leaf is evicted but we still have summary — transfer just the summary.
                    // Receiving worker will need to read from disk via the shared reader.
                    None
                }),
            }),
        }
    }

    /// Receive a transferred leaf from another worker.
    pub fn receive_leaf(&mut self, transfer: LeafTransferData<V>) {
        let leaf_id = transfer.leaf_id;

        if let Some(leaf) = transfer.leaf {
            // Leaf was in memory — insert as dirty (new owner needs to flush).
            self.insert_leaf(leaf_id, leaf);
        } else if let Some((reader, disk_loc)) = transfer.disk_reader {
            // Leaf is on disk — register the disk location.
            // Create a segment entry if we don't have one for this segment yet.
            let seg_id = disk_loc.segment_id;
            if !self.segments.iter().any(|s| s.id == seg_id) {
                // Create a minimal segment entry referencing the transferred reader.
                let mut seg = SegmentMetadata::with_reader(
                    seg_id,
                    StoragePath::from("transferred"),
                    reader,
                );
                seg.leaf_index
                    .insert(leaf_id, (disk_loc.offset, disk_loc.size));
                seg.leaf_count = 1;
                self.segments.push(seg);
            } else {
                // Add to existing segment's leaf index.
                if let Some(seg) = self.segments.iter_mut().find(|s| s.id == seg_id) {
                    seg.leaf_index
                        .insert(leaf_id, (disk_loc.offset, disk_loc.size));
                    seg.leaf_count += 1;
                }
            }

            // Store disk location and evicted summary.
            self.leaf_disk_locations.insert(leaf_id, disk_loc);
            let summary = CachedLeafSummary {
                first_key_bytes: Vec::new(),
                has_first_key: false,
                weight_sum: 0,
                entry_count: 0,
            };
            self.leaves
                .insert(leaf_id, WorkerLeafSlot::Evicted(summary));
        }
    }

    // =========================================================================
    // Disk Read (reload evicted leaves)
    // =========================================================================

    /// Reload an evicted leaf from disk into memory.
    ///
    /// Returns the leaf if successfully loaded, or an error.
    pub fn reload_leaf(&mut self, leaf_id: usize) -> Result<&LeafNode<V>, String>
    where
        V: Archive,
        <V as Archive>::Archived: rkyv::Deserialize<V, crate::storage::file::Deserializer>,
    {
        // Already present?
        if self
            .leaves
            .get(&leaf_id)
            .map(|s| s.is_present())
            .unwrap_or(false)
        {
            return Ok(self.leaves[&leaf_id].as_present().unwrap());
        }

        let disk_loc = self
            .leaf_disk_locations
            .get(&leaf_id)
            .ok_or_else(|| format!("Leaf {} has no disk location", leaf_id))?;

        let segment = self
            .segments
            .iter()
            .find(|s| s.id == disk_loc.segment_id)
            .ok_or_else(|| {
                format!(
                    "Segment {:?} not found for leaf {}",
                    disk_loc.segment_id, leaf_id
                )
            })?;

        let reader = segment
            .reader
            .as_ref()
            .ok_or_else(|| format!("No reader for segment {:?}", segment.id))?;

        let block_loc = disk_loc.to_block_location();
        let block = reader
            .read_block(block_loc)
            .map_err(|e| format!("Failed to read block: {:?}", e))?;

        // Parse data block: skip 24-byte header (checksum + magic + leaf_id + data_len)
        let data_block_header_size = 24;
        if block.len() < data_block_header_size {
            return Err("Block too small for header".to_string());
        }

        let data_len =
            u64::from_le_bytes(block[16..24].try_into().unwrap()) as usize;
        if block.len() < data_block_header_size + data_len {
            return Err("Block too small for data".to_string());
        }

        let data = &block[data_block_header_size..data_block_header_size + data_len];
        let archived = unsafe { rkyv::archived_root::<LeafNode<V>>(data) };
        let mut deserializer = crate::storage::file::Deserializer::new(0);
        let leaf: LeafNode<V> = rkyv::Deserialize::deserialize(archived, &mut deserializer)
            .map_err(|e| format!("Failed to deserialize leaf: {:?}", e))?;

        self.leaves
            .insert(leaf_id, WorkerLeafSlot::Present(leaf));

        Ok(self.leaves[&leaf_id].as_present().unwrap())
    }

    /// Get a leaf, reloading from disk if evicted.
    pub fn get_leaf_reloading(&mut self, leaf_id: usize) -> Result<&LeafNode<V>, String>
    where
        V: Archive,
        <V as Archive>::Archived: rkyv::Deserialize<V, crate::storage::file::Deserializer>,
    {
        if self.is_leaf_present(leaf_id) {
            Ok(self.leaves[&leaf_id].as_present().unwrap())
        } else {
            self.reload_leaf(leaf_id)
        }
    }

    // =========================================================================
    // Bootstrap: receive initial leaf distribution from owner
    // =========================================================================

    /// Receive a batch of leaves during bootstrap (initial distribution).
    ///
    /// The owner distributes its leaves to workers based on contiguous ranges.
    /// Leaves arrive as in-memory data and are inserted as dirty.
    pub fn bootstrap_receive_leaves(&mut self, leaves: Vec<(usize, LeafNode<V>)>) {
        for (leaf_id, leaf) in leaves {
            self.insert_leaf(leaf_id, leaf);
        }
    }

    // =========================================================================
    // Segment Management
    // =========================================================================

    /// Set the next segment ID (assigned by owner during coordination).
    pub fn set_next_segment_id(&mut self, id: u64) {
        self.next_segment_id = id;
    }

    /// Get segment metadata (for checkpoint).
    pub fn segments(&self) -> &[SegmentMetadata] {
        &self.segments
    }

    /// Get all dirty leaf IDs.
    pub fn dirty_leaf_ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.dirty_leaves.iter().copied()
    }

    /// Get dirty bytes.
    pub fn dirty_bytes(&self) -> usize {
        self.dirty_bytes
    }

    /// Check if a storage backend is configured.
    pub fn has_storage_backend(&self) -> bool {
        self.storage_backend.is_some()
    }

    // =========================================================================
    // Owned Range Tracking
    // =========================================================================

    /// Set the owned leaf range [start, end).
    pub fn set_owned_range(&mut self, range: (usize, usize)) {
        self.owned_range = range;
    }

    /// Get the current owned leaf range [start, end).
    pub fn owned_range(&self) -> (usize, usize) {
        self.owned_range
    }

    /// Check if this worker has bootstrapped (cloned initial leaves from owner).
    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped
    }

    /// Mark this worker as bootstrapped.
    pub fn mark_bootstrapped(&mut self) {
        self.bootstrapped = true;
    }

    /// Update storage backend configuration.
    pub fn set_storage_backend(&mut self, backend: Option<Arc<dyn StorageBackend>>) {
        self.storage_backend = backend;
    }

    /// Update segment path prefix.
    pub fn set_segment_path_prefix(&mut self, prefix: String) {
        self.segment_path_prefix = prefix;
    }

    /// Update spill directory.
    pub fn set_spill_directory(&mut self, dir: Option<StoragePath>) {
        self.spill_directory = dir;
    }

    /// Clear all state (for reset/restore).
    pub fn clear(&mut self) {
        self.leaves.clear();
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.leaf_disk_locations.clear();
        self.owned_range = (0, 0);
        self.bootstrapped = false;
        // Don't clear segments — files may still exist on disk.
    }

    // =========================================================================
    // Metric Accessors
    // =========================================================================

    /// Total size of all segment files on disk (bytes).
    pub fn storage_size(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes).sum()
    }

    /// Number of leaves currently evicted to disk.
    pub fn evicted_leaf_count(&self) -> usize {
        self.leaves.values().filter(|s| s.is_evicted()).count()
    }
}

impl<V: DBData> SizeOf for WorkerLeafStorage<V>
where
    V: Archive + RkyvSerialize<Serializer>,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        // Estimate leaf data size
        for slot in self.leaves.values() {
            match slot {
                WorkerLeafSlot::Present(leaf) => {
                    // Each entry is (V, ZWeight)
                    let entry_size = std::mem::size_of::<V>() + std::mem::size_of::<ZWeight>();
                    context.add(leaf.entries.len() * entry_size);
                }
                WorkerLeafSlot::Evicted(_) => {
                    // Summary is small, just the struct size
                    context.add(std::mem::size_of::<CachedLeafSummary>());
                }
            }
        }
        // HashMap overhead
        context.add(self.leaves.capacity() * std::mem::size_of::<(usize, WorkerLeafSlot<V>)>());
        context.add(self.dirty_leaves.capacity() * std::mem::size_of::<usize>());
        context.add(self.leaf_disk_locations.capacity() * std::mem::size_of::<(usize, LeafDiskLocation)>());
    }
}

// =============================================================================
// LeafTransferData: serializable leaf transfer between workers
// =============================================================================

/// Data transferred when a leaf changes ownership between adjacent workers.
///
/// Contains the leaf data (if in memory) and/or a disk reader reference.
/// The receiving worker integrates this into its `WorkerLeafStorage`.
pub(crate) struct LeafTransferData<V> {
    /// Global leaf ID.
    pub leaf_id: usize,
    /// Leaf data if it was in memory (None if evicted).
    pub leaf: Option<LeafNode<V>>,
    /// Disk reader + location if the leaf is on disk.
    pub disk_reader: Option<(Arc<dyn FileReader>, LeafDiskLocation)>,
}

// =============================================================================
// WorkerReport: mutation results reported back to owner
// =============================================================================

/// Report from a worker after processing its assigned entries.
///
/// The owner uses this to update internal node structure.
/// Supports two modes:
/// - **Persistent path**: `leaf_summaries` + `segment_info` (lightweight metadata, leaves on disk)
/// - **Fallback path**: `modified_leaves` (full leaf data, for no-storage-backend / in-memory tests)
#[derive(Clone)]
pub(crate) struct WorkerReport<V> {
    /// Per-leaf weight deltas: (leaf_id, weight_delta).
    pub weight_deltas: Vec<(usize, ZWeight)>,
    /// Per-leaf key count deltas: (leaf_id, key_count_delta).
    pub key_count_deltas: Vec<(usize, i64)>,
    /// Split leaves: (original_leaf_id, split_key, split_leaf_data).
    pub splits: Vec<WorkerSplitInfo<V>>,
    /// Total weight delta across all processed leaves.
    pub total_weight_delta: ZWeight,
    /// Total key count delta across all processed leaves.
    pub total_key_count_delta: i64,
    /// Number of entries processed by this worker.
    pub entries_processed: usize,
    /// Entries that could not be processed (e.g., bootstrap with no tree).
    /// Returned to the owner for sequential handling.
    pub unprocessed_entries: Vec<(V, ZWeight)>,
    /// Modified leaves: (leaf_id, new_leaf_data).
    /// Fallback for no-storage-backend: owner replaces these in its tree.
    pub modified_leaves: Vec<(usize, LeafNode<V>)>,
    /// Lightweight leaf summaries for the persistent path.
    /// Owner calls `replace_leaf_with_evicted()` per summary.
    pub leaf_summaries: Vec<(usize, CachedLeafSummary)>,
    /// Worker segment file info for the persistent path.
    /// Owner calls `register_worker_segment()` with this data.
    pub segment_info: Option<WorkerSegmentReport>,
}

impl<V> WorkerReport<V> {
    pub fn empty() -> Self {
        Self {
            weight_deltas: Vec::new(),
            key_count_deltas: Vec::new(),
            splits: Vec::new(),
            total_weight_delta: 0,
            total_key_count_delta: 0,
            entries_processed: 0,
            unprocessed_entries: Vec::new(),
            modified_leaves: Vec::new(),
            leaf_summaries: Vec::new(),
            segment_info: None,
        }
    }
}

/// Segment file metadata reported by a worker to the owner.
///
/// The owner uses this to register the worker's segment file and update
/// its `leaf_disk_locations` so that future reads of those leaves resolve
/// to the worker's segment.
#[derive(Clone)]
pub(crate) struct WorkerSegmentReport {
    /// Segment ID allocated by the owner for this worker.
    pub segment_id: SegmentId,
    /// Path of the segment file on disk.
    pub path: StoragePath,
    /// Reader for the segment file.
    pub reader: Arc<dyn FileReader>,
    /// Leaf index: leaf_id → (offset, size) within the segment file.
    pub leaf_index: HashMap<usize, (u64, u32)>,
    /// Total size of the segment file in bytes.
    pub file_size: u64,
}

/// Information about a leaf split performed by a worker.
#[derive(Clone)]
pub(crate) struct WorkerSplitInfo<V> {
    /// The original leaf that was split.
    pub original_leaf_id: usize,
    /// The separator key (first key of the new right leaf).
    pub split_key: V,
    /// The new right leaf data.
    pub split_leaf: LeafNode<V>,
}

// =============================================================================
// Helper functions
// =============================================================================

/// Estimate the memory size of a leaf node.
fn estimate_leaf_size<V>(leaf: &LeafNode<V>) -> usize {
    let entry_size = std::mem::size_of::<V>() + std::mem::size_of::<ZWeight>();
    let entries_bytes = leaf.entries.len() * entry_size;
    let overhead = std::mem::size_of::<Vec<(V, ZWeight)>>() + std::mem::size_of::<Option<LeafLocation>>();
    entries_bytes + overhead
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algebra::F64;

    fn make_leaf(entries: Vec<(F64, ZWeight)>) -> LeafNode<F64> {
        LeafNode {
            entries,
            next_leaf: None,
        }
    }

    #[test]
    fn test_basic_insert_and_get() {
        let mut storage: WorkerLeafStorage<F64> =
            WorkerLeafStorage::new(0, None, String::new(), None, usize::MAX);

        let leaf = make_leaf(vec![(F64::new(1.0), 1), (F64::new(2.0), 2)]);
        storage.insert_leaf(10, leaf);

        assert!(storage.owns_leaf(10));
        assert!(!storage.owns_leaf(11));
        assert_eq!(storage.num_leaves(), 1);

        let leaf = storage.get_leaf(10);
        assert_eq!(leaf.entries.len(), 2);
        assert_eq!(leaf.total_weight(), 3);
    }

    #[test]
    fn test_remove_leaf() {
        let mut storage: WorkerLeafStorage<F64> =
            WorkerLeafStorage::new(0, None, String::new(), None, usize::MAX);

        let leaf = make_leaf(vec![(F64::new(1.0), 1)]);
        storage.insert_leaf(5, leaf);

        assert!(storage.owns_leaf(5));
        let removed = storage.remove_leaf(5);
        assert!(removed.is_some());
        assert!(!storage.owns_leaf(5));
        assert_eq!(storage.num_leaves(), 0);
    }

    #[test]
    fn test_leaf_transfer() {
        let mut src: WorkerLeafStorage<F64> =
            WorkerLeafStorage::new(0, None, String::new(), None, usize::MAX);
        let mut dst: WorkerLeafStorage<F64> =
            WorkerLeafStorage::new(1, None, String::new(), None, usize::MAX);

        let leaf = make_leaf(vec![(F64::new(10.0), 3), (F64::new(20.0), 5)]);
        src.insert_leaf(42, leaf);

        // Transfer from src to dst.
        let transfer = src.take_leaf_for_transfer(42).unwrap();
        assert!(!src.owns_leaf(42));
        assert!(transfer.leaf.is_some());

        dst.receive_leaf(transfer);
        assert!(dst.owns_leaf(42));

        let leaf = dst.get_leaf(42);
        assert_eq!(leaf.entries.len(), 2);
        assert_eq!(leaf.total_weight(), 8);
    }

    #[test]
    fn test_worker_report_empty() {
        let report: WorkerReport<F64> = WorkerReport::empty();
        assert_eq!(report.total_weight_delta, 0);
        assert_eq!(report.entries_processed, 0);
        assert!(report.splits.is_empty());
    }

    #[test]
    fn test_dirty_tracking() {
        let mut storage: WorkerLeafStorage<F64> =
            WorkerLeafStorage::new(0, None, String::new(), None, 1024);

        // Insert marks dirty.
        let leaf = make_leaf(vec![(F64::new(1.0), 1)]);
        storage.insert_leaf(0, leaf);
        assert!(storage.dirty_bytes > 0);

        // Without backend, should_flush is always false.
        assert!(!storage.should_flush());
    }

    #[test]
    fn test_replace_leaf() {
        let mut storage: WorkerLeafStorage<F64> =
            WorkerLeafStorage::new(0, None, String::new(), None, usize::MAX);

        let leaf1 = make_leaf(vec![(F64::new(1.0), 1)]);
        storage.insert_leaf(0, leaf1);
        assert_eq!(storage.get_leaf(0).entries.len(), 1);

        let leaf2 = make_leaf(vec![(F64::new(1.0), 1), (F64::new(2.0), 2), (F64::new(3.0), 3)]);
        storage.replace_leaf(0, leaf2);
        assert_eq!(storage.get_leaf(0).entries.len(), 3);
        assert_eq!(storage.get_leaf(0).total_weight(), 6);
    }
}
