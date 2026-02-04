// Disk I/O operations, file format, and Drop implementation for NodeStorage.
//
// This file is included into node_storage.rs via include!() and shares
// the same module scope and imports. It contains:
// - Serialization utilities (serialize_to_bytes, storage_path_to_fs_path)
// - Disk spilling impl block (flush, load, evict, checkpoint methods)
// - File format constants, types, and utilities
// - Drop implementation for segment cleanup

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

/// Convert a `StoragePath` to a filesystem `PathBuf`.
///
/// This is a temporary helper for Phase 1 of the storage backend migration.
/// It converts the logical `StoragePath` to a filesystem path by using the
/// path's string representation. In later phases, all I/O will go through
/// the `StorageBackend` and this helper will be removed.
///
/// If a storage backend is provided and has a filesystem path, the `StoragePath`
/// is resolved relative to that base. Otherwise, the path string is used directly.
///
/// Note: `StoragePath` (from object_store) strips leading "/" from paths, so
/// absolute paths like "/tmp/foo" become "tmp/foo". This helper attempts to
/// restore absolute paths on Unix systems by checking if the path exists
/// with a leading "/" prefix.
pub fn storage_path_to_fs_path(
    sp: &StoragePath,
    backend: Option<&dyn StorageBackend>,
) -> std::path::PathBuf {
    if let Some(backend) = backend {
        if let Some(base) = backend.file_system_path() {
            return base.join(sp.as_ref());
        }
    }

    // StoragePath strips leading "/", so we need to restore it for absolute paths.
    // Try the path as-is first, then try with leading "/" on Unix.
    let path_str = sp.as_ref();
    let relative_path = std::path::PathBuf::from(path_str);

    // If the relative path exists, use it
    if relative_path.exists() {
        return relative_path;
    }

    // On Unix, try with leading "/" (StoragePath strips leading slashes)
    #[cfg(unix)]
    {
        let absolute_path = std::path::PathBuf::from(format!("/{}", path_str));
        if absolute_path.parent().map(|p| p.exists()).unwrap_or(false) {
            return absolute_path;
        }
    }

    // Fall back to relative path
    relative_path
}

// =============================================================================
// Disk Spilling Methods
// =============================================================================

/// Disk spilling methods for NodeStorage.
///
/// These methods require the full `StorableNode` bounds on leaf nodes plus
/// rkyv serialization support for disk spilling.
impl<I, L> NodeStorage<I, L>
where
    I: StorableNode,
    L: StorableNode + LeafNodeOps + Archive + RkyvSerialize<Serializer>,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
    L::Key: Archive + RkyvSerialize<Serializer>,
    Archived<L::Key>: RkyvDeserialize<L::Key, Deserializer>,
{
    /// Flush all dirty leaves to disk as an immutable segment file.
    ///
    /// Each flush creates a new immutable segment file with a unique ID. This aligns
    /// with StorageBackend semantics where files become read-only after completion.
    ///
    /// # Segment Lifecycle
    ///
    /// 1. Create a new segment file with unique ID
    /// 2. Write all dirty leaves as serialized blocks
    /// 3. Write segment index at end of file
    /// 4. Complete/finalize the file (becomes immutable)
    /// 5. Update in-memory tracking structures
    ///
    /// # Returns
    ///
    /// Number of leaves written to the new segment.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if storage.should_flush() {
    ///     let count = storage.flush_dirty_to_disk()?;
    ///     println!("Created segment with {} leaves", count);
    /// }
    /// ```
    pub fn flush_dirty_to_disk(&mut self) -> Result<usize, FileFormatError> {
        if self.dirty_leaves.is_empty() {
            return Ok(0);
        }

        // Get storage backend - required for all I/O
        let backend = self.config.storage_backend.clone()
            .or_else(|| Runtime::storage_backend().ok())
            .ok_or_else(|| FileFormatError::Io(
                "No storage backend available. Tests must run within a Runtime with storage configured.".to_string()
            ))?;

        // Allocate new segment ID
        let segment_id = SegmentId::new(self.next_segment_id);
        self.next_segment_id += 1;

        // Generate path for the segment file
        let storage_path = self.generate_segment_path(segment_id);

        // Create file writer
        let mut writer = backend.create_named(&storage_path)
            .map_err(|e| FileFormatError::Io(format!("Failed to create segment file: {}", e)))?;

        // Collect dirty leaf IDs (sorted for deterministic output)
        let mut dirty_ids: Vec<usize> = self.dirty_leaves.iter().copied().collect();
        dirty_ids.sort();

        // Track statistics for header
        let mut total_entries: u64 = 0;
        let mut total_weight: i64 = 0;
        for &id in &dirty_ids {
            if let Some(leaf) = self.leaves.get(id).and_then(|slot| slot.as_present()) {
                total_entries += leaf.entry_count() as u64;
                total_weight += leaf.total_weight();
            }
        }

        // Write placeholder header (will be updated at end)
        // For now just write zeros - we'll write the final file in a single pass
        // by collecting all blocks first
        let mut all_blocks: Vec<FBuf> = Vec::new();
        let mut leaf_index: std::collections::HashMap<usize, (u64, u32)> = std::collections::HashMap::new();

        // Reserve space for header (will be written last)
        let header_block = FBuf::with_capacity(FILE_HEADER_SIZE);
        let mut header_buf = header_block;
        header_buf.resize(FILE_HEADER_SIZE, 0);
        all_blocks.push(header_buf);

        let mut current_offset = FILE_HEADER_SIZE as u64;

        // Write each dirty leaf as a data block
        for &id in &dirty_ids {
            let leaf = match self.leaves.get(id).and_then(|slot| slot.as_present()) {
                Some(leaf) => leaf,
                None => continue, // Skip if leaf is not present
            };

            // Serialize the leaf
            let serialized = serialize_to_bytes(leaf)?;

            // Create data block: header + serialized data, padded to 512 bytes
            let data_size = DATA_BLOCK_HEADER_SIZE + serialized.len();
            let block_size = align_to_block(data_size);

            let mut block = FBuf::with_capacity(block_size);
            block.resize(block_size, 0);

            // Write data block header
            let header = create_data_block_header(id as u64, serialized.len() as u64);
            block[..DATA_BLOCK_HEADER_SIZE].copy_from_slice(&header);

            // Write serialized data
            block[DATA_BLOCK_HEADER_SIZE..DATA_BLOCK_HEADER_SIZE + serialized.len()]
                .copy_from_slice(&serialized);

            // Compute and set checksum
            set_block_checksum(&mut block);

            // Record location in index
            leaf_index.insert(id, (current_offset, block_size as u32));
            current_offset += block_size as u64;

            all_blocks.push(block);
        }

        // Create index block
        let index_offset = current_offset;
        let num_entries = dirty_ids.len();
        let entries_size = num_entries * INDEX_ENTRY_SIZE;
        let index_content_size = 16 + entries_size; // 4 checksum + 4 magic + 8 num_entries + entries
        let index_block_size = align_to_block(index_content_size);

        let mut index_block = FBuf::with_capacity(index_block_size);
        index_block.resize(index_block_size, 0);

        // Write index block header (skip checksum at [0..4])
        index_block[4..8].copy_from_slice(&MAGIC_INDEX_BLOCK);
        index_block[8..16].copy_from_slice(&(num_entries as u64).to_le_bytes());

        // Write index entries
        for (i, &id) in dirty_ids.iter().enumerate() {
            if let Some(&(offset, size)) = leaf_index.get(&id) {
                let start = 16 + i * INDEX_ENTRY_SIZE;
                let entry = IndexEntry {
                    leaf_id: id as u64,
                    location: FileBlockLocation { offset, size },
                };
                let entry_bytes = entry.to_bytes();
                index_block[start..start + INDEX_ENTRY_SIZE].copy_from_slice(&entry_bytes);
            }
        }

        // Compute and set checksum for index block
        set_block_checksum(&mut index_block);
        all_blocks.push(index_block);

        // Now update the header block with actual values
        let header = FileHeader {
            num_leaves: num_entries as u64,
            index_offset,
            total_entries,
            total_weight,
        };
        let header_bytes = header.to_bytes();
        all_blocks[0][..FILE_HEADER_SIZE].copy_from_slice(&header_bytes);

        // Write all blocks to the file
        for block in all_blocks {
            writer.write_block(block)
                .map_err(|e| FileFormatError::Io(format!("Failed to write block: {}", e)))?;
        }

        // Complete the file and get a reader.
        // Note: We do NOT call mark_for_checkpoint() here. That is done
        // in save() when the segment is registered for checkpoint commit.
        // Until then, the FileReader's drop will delete the file if needed.
        let reader = writer.complete()
            .map_err(|e| FileFormatError::Io(format!("Failed to complete segment file: {}", e)))?;

        // Create segment metadata with the reader
        let segment_meta = SegmentMetadata {
            id: segment_id,
            path: storage_path,
            leaf_index,
            size_bytes: index_offset + index_block_size as u64,
            leaf_count: num_entries,
            reader: Some(reader),
        };

        // Update leaf_disk_locations for efficient lookup
        for (&leaf_id, &(offset, size)) in &segment_meta.leaf_index {
            self.leaf_disk_locations.insert(
                leaf_id,
                LeafDiskLocation::new(segment_id, offset, size),
            );
        }

        // Store the completed segment
        self.segments.push(segment_meta);

        // Update statistics
        let count = dirty_ids.len();
        self.stats.leaves_written += count as u64;

        // Mark all as clean
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.update_dirty_stats();

        Ok(count)
    }

    /// Load a leaf from disk storage.
    ///
    /// If the leaf is in memory, returns it directly. Otherwise, finds the
    /// segment containing the leaf and loads it from disk using the FileReader
    /// stored in SegmentMetadata.
    ///
    /// # Returns
    ///
    /// Reference to the loaded leaf.
    pub fn load_leaf_from_disk(&mut self, loc: LeafLocation) -> Result<&L, FileFormatError> {
        let id = loc.id;

        // Check if already in memory
        if id < self.leaves.len() && self.leaves[id].is_present() {
            self.stats.cache_hits += 1;
            return Ok(self.leaves[id].as_present().unwrap());
        }

        // Find segment location
        let disk_loc = self
            .leaf_disk_locations
            .get(&id)
            .copied()
            .ok_or_else(|| {
                FileFormatError::Io(format!(
                    "Leaf {} not found in segment storage",
                    id
                ))
            })?;

        // Find the segment containing this leaf
        let segment = self
            .segments
            .iter()
            .find(|s| s.id == disk_loc.segment_id)
            .ok_or_else(|| {
                FileFormatError::Io(format!(
                    "Segment {:?} not found for leaf {}",
                    disk_loc.segment_id, id
                ))
            })?;

        // Get the FileReader from the segment (required)
        let reader = segment.reader.as_ref().ok_or_else(|| {
            FileFormatError::Io(format!(
                "Segment {:?} has no FileReader. Cannot load leaf {}.",
                disk_loc.segment_id, id
            ))
        })?;

        // Read the data block from disk
        let block_data = reader.read_block(BlockLocation {
            offset: disk_loc.offset,
            size: disk_loc.size as usize,
        }).map_err(|e| FileFormatError::Io(format!("Failed to read leaf block: {}", e)))?;

        // Verify and parse the data block
        let (read_leaf_id, data_len) = verify_data_block_header(&block_data)?;
        if read_leaf_id != id as u64 {
            return Err(FileFormatError::Io(format!(
                "Leaf ID mismatch: expected {}, found {}",
                id, read_leaf_id
            )));
        }

        // Extract and deserialize the leaf data
        let data_start = DATA_BLOCK_HEADER_SIZE;
        let data_end = data_start + data_len as usize;
        if data_end > block_data.len() {
            return Err(FileFormatError::Io(format!(
                "Data extends beyond block: data_end={}, block_len={}",
                data_end, block_data.len()
            )));
        }

        let leaf_data = &block_data[data_start..data_end];
        let leaf = deserialize_leaf::<L>(leaf_data)?;
        let leaf_size = leaf.estimate_size();

        // Track if we're reloading an evicted leaf
        let was_evicted = id < self.leaves.len() && self.leaves[id].is_evicted();

        // Store in memory
        if id < self.leaves.len() {
            self.leaves[id] = LeafSlot::Present(leaf);
        } else {
            return Err(FileFormatError::Io(format!(
                "Leaf slot {} does not exist",
                id
            )));
        }

        // Update stats
        self.stats.cache_misses += 1;
        self.stats.memory_bytes += leaf_size;
        if was_evicted {
            self.stats.evicted_leaf_count = self.stats.evicted_leaf_count.saturating_sub(1);
        }

        Ok(self.leaves[id].as_present().unwrap())
    }

    /// Check if a leaf has been written to disk (in any segment).
    pub fn is_leaf_spilled(&self, id: usize) -> bool {
        self.leaf_disk_locations.contains_key(&id)
    }

    /// Get the number of leaves that have been written to disk.
    pub fn spilled_leaf_count(&self) -> usize {
        self.leaf_disk_locations.len()
    }

    /// Get disk locations for all spilled leaves.
    ///
    /// Returns an iterator over (leaf_id, segment_id, offset, size) tuples for each leaf
    /// that has been written to disk. Used for checkpoint metadata.
    pub fn get_leaf_disk_locations(
        &self,
    ) -> impl Iterator<Item = (usize, u64, u64, u32)> + '_ {
        self.leaf_disk_locations
            .iter()
            .map(|(&id, loc)| (id, loc.segment_id.0, loc.offset, loc.size))
    }

    /// Delete all segment files from disk.
    ///
    /// This should be called when the storage is no longer needed or
    /// when all data has been persisted elsewhere.
    ///
    /// **Warning**: If there are evicted leaves, they will be lost!
    /// Call `reload_evicted_leaves()` first if you need to preserve them.
    pub fn cleanup_segments(&mut self) -> Result<(), std::io::Error> {
        // Delete all segment files
        for segment in &self.segments {
            let fs_path = storage_path_to_fs_path(
                &segment.path,
                self.config.storage_backend.as_deref(),
            );
            if fs_path.exists() {
                std::fs::remove_file(&fs_path)?;
            }
        }

        // Clear segment tracking
        self.segments.clear();
        self.leaf_disk_locations.clear();
        self.pending_segment = None;

        Ok(())
    }

    /// Evict clean leaves from memory to free RAM.
    ///
    /// Only clean leaves that have been written to disk can be evicted.
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
    ///     storage.flush_dirty_to_disk()?;  // Write dirty leaves to disk
    ///     let (evicted, freed) = storage.evict_clean_leaves();  // Free memory
    ///     println!("Evicted {} leaves, freed {} bytes", evicted, freed);
    /// }
    /// ```
    pub fn evict_clean_leaves(&mut self) -> (usize, usize) {
        let mut evicted_count = 0;
        let mut bytes_freed = 0;

        // Only evict leaves that are:
        // 1. In memory (Present)
        // 2. Written to disk (in leaf_disk_locations)
        // 3. Not dirty (not in dirty_leaves)
        for id in 0..self.leaves.len() {
            if self.leaves[id].is_present()
                && self.leaf_disk_locations.contains_key(&id)
                && !self.dirty_leaves.contains(&id)
            {
                // Calculate size and capture summary before evicting
                let leaf = self.leaves[id].as_present().unwrap();
                let leaf_size = leaf.estimate_size();

                // Capture summary for checkpoint support
                let first_key = leaf.first_key();
                let (first_key_bytes, has_first_key) = if let Some(ref key) = first_key {
                    (
                        Vec::<u8>::from(serialize_to_bytes(key).unwrap_or_default()),
                        true,
                    )
                } else {
                    (Vec::new(), false)
                };
                let summary = CachedLeafSummary {
                    first_key_bytes,
                    has_first_key,
                    weight_sum: leaf.total_weight(),
                    entry_count: leaf.entry_count(),
                };

                // Evict by replacing Present with Evicted
                self.leaves[id] = LeafSlot::Evicted(summary);

                evicted_count += 1;
                bytes_freed += leaf_size;
            }
        }

        // Update stats
        self.stats.memory_bytes = self.stats.memory_bytes.saturating_sub(bytes_freed);
        self.stats.evicted_leaf_count += evicted_count;

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
        if id < self.leaves.len() && self.leaves[id].is_present() {
            self.stats.cache_hits += 1;
            return self.leaves[id].as_present().unwrap();
        }

        // Need to reload from disk
        self.load_leaf_from_disk(loc)
            .expect("Failed to reload evicted leaf from disk")
    }

    /// Get a mutable reference to a leaf, auto-reloading from disk if evicted.
    ///
    /// This is the primary method for mutating leaves when eviction is enabled.
    /// If the leaf is evicted, it is reloaded from disk first, then marked dirty.
    ///
    /// # Panics
    /// Panics if the leaf location is invalid or if reload fails.
    pub fn get_leaf_reloading_mut(&mut self, loc: LeafLocation) -> &mut L {
        let id = loc.id;

        if self.leaves[id].is_evicted() {
            self.load_leaf_from_disk(loc)
                .expect("Failed to reload evicted leaf for mutation");
        }

        self.mark_leaf_dirty(id);
        self.leaves[id]
            .as_present_mut()
            .expect("Leaf should be present after reload")
    }

    /// Reload all evicted leaves from disk into memory.
    ///
    /// This can be used to restore all leaves to memory, e.g., before
    /// serialization or when memory pressure is relieved.
    ///
    /// # Returns
    /// * Number of leaves reloaded
    pub fn reload_evicted_leaves(&mut self) -> Result<usize, FileFormatError> {
        // No segments means no leaves to reload
        if self.segments.is_empty() {
            return Ok(0);
        }

        // Collect IDs of evicted leaves
        let evicted_ids: Vec<usize> = self
            .leaves
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.is_evicted())
            .map(|(id, _)| id)
            .collect();

        if evicted_ids.is_empty() {
            return Ok(0);
        }

        let mut count = 0;

        // Reload each evicted leaf using load_leaf_from_disk
        for id in evicted_ids {
            let loc = LeafLocation::new(id);
            self.load_leaf_from_disk(loc)?;
            count += 1;
        }

        Ok(count)
    }

    // =========================================================================
    // Checkpoint Methods
    // =========================================================================

    /// Check if a leaf is already on disk (written and not dirty).
    ///
    /// Returns true if the leaf has been written to disk and hasn't been
    /// modified since. Such leaves don't need re-serialization during checkpoint.
    #[inline]
    pub fn is_leaf_on_disk(&self, leaf_id: usize) -> bool {
        // A leaf is on disk if it's in leaf_disk_locations and not dirty
        self.leaf_disk_locations.contains_key(&leaf_id) && !self.dirty_leaves.contains(&leaf_id)
    }

    /// Flush all leaves to disk as a new segment, writing only those not yet persisted.
    ///
    /// This method handles three categories of leaves:
    /// - Already on disk (clean): SKIP - no re-serialization needed
    /// - Dirty: MUST write - modified since last flush
    /// - Never spilled: MUST write - only in memory
    ///
    /// Creates a new segment file for the leaves that need to be written.
    ///
    /// # Returns
    ///
    /// Number of leaves written to disk.
    pub fn flush_all_leaves_to_disk(&mut self) -> Result<usize, FileFormatError> {
        // Collect leaves that need to be written
        let mut leaves_to_write: Vec<usize> = Vec::new();
        for leaf_id in 0..self.leaves.len() {
            // Skip if already on disk and clean
            if self.is_leaf_on_disk(leaf_id) {
                continue;
            }

            // Skip if evicted (already on disk)
            if self.leaves[leaf_id].is_evicted() {
                continue;
            }

            leaves_to_write.push(leaf_id);
        }

        if leaves_to_write.is_empty() {
            return Ok(0);
        }

        // Mark all these leaves as dirty so flush_dirty_to_disk() will process them
        for &leaf_id in &leaves_to_write {
            if !self.dirty_leaves.contains(&leaf_id) {
                let leaf = self.leaves[leaf_id].as_present().unwrap();
                let leaf_size = Self::estimate_leaf_size(leaf);
                self.dirty_leaves.insert(leaf_id);
                self.dirty_bytes += leaf_size;
            }
        }

        // Use the segment-based flush
        self.flush_dirty_to_disk()
    }

    /// Prepare segment-based checkpoint metadata without invalidating runtime state.
    ///
    /// This method:
    /// 1. Flushes all dirty leaves to segment files
    /// 2. Collects leaf summaries for O(num_leaves) restore
    /// 3. Returns metadata referencing the segment files
    /// 4. **Does NOT invalidate segments** - runtime continues operating
    ///
    /// After calling this method, the caller should copy the segment files
    /// (returned paths in metadata) to checkpoint storage. The runtime
    /// can continue operating with its local segment files.
    ///
    /// # Returns
    ///
    /// `CommittedSegmentStorage` containing:
    /// - Paths to all segment files (caller should copy these to checkpoint storage)
    /// - Per-segment metadata with leaf locations
    /// - Leaf summaries for O(num_leaves) restore
    ///
    /// # Errors
    ///
    /// Returns an error if flushing leaves to disk fails.
    pub fn prepare_checkpoint(&mut self) -> Result<CommittedSegmentStorage<L::Key>, FileFormatError>
    where
        Archived<L::Key>: RkyvDeserialize<L::Key, Deserializer>,
    {
        // 1. Flush all dirty leaves to ensure everything is on disk
        self.flush_all_leaves_to_disk()?;

        // 2. Collect leaf summaries from LeafSlot:
        //    - Present(leaf): compute summary directly from leaf (it's in memory)
        //    - Evicted(summary): use the cached summary (captured at eviction time)
        let mut leaf_summaries = Vec::with_capacity(self.leaves.len());
        let mut total_entries = 0usize;
        let mut total_weight = 0i64;

        for slot in &self.leaves {
            let summary = match slot {
                LeafSlot::Present(leaf) => {
                    // Leaf is in memory - compute summary directly
                    LeafSummary::new(leaf.first_key(), leaf.total_weight(), leaf.entry_count())
                }
                LeafSlot::Evicted(cached) => {
                    // Leaf is evicted - reconstruct summary from cached data
                    let first_key =
                        if cached.has_first_key && !cached.first_key_bytes.is_empty() {
                            // SAFETY: first_key_bytes was serialized by serialize_to_bytes() in
                            // evict_clean_leaves() or mark_all_leaves_evicted() using the same
                            // rkyv serializer. The bytes are guaranteed to be a valid archived
                            // representation of L::Key. We use unsafe archived_root instead of
                            // check_archived_root for performance since we trust our own serialization.
                            let archived =
                                unsafe { rkyv::archived_root::<L::Key>(&cached.first_key_bytes) };
                            // Using .ok() here is intentional: if deserialization fails (which
                            // shouldn't happen with valid serialized data), we treat it as if
                            // the leaf had no first key. This is acceptable because:
                            // 1. The weight_sum and entry_count are still valid for restore
                            // 2. A missing first_key only affects separator key reconstruction
                            // 3. Failing the entire checkpoint for one corrupt key is too harsh
                            archived.deserialize(&mut Deserializer::default()).ok()
                        } else {
                            None
                        };
                    LeafSummary::new(first_key, cached.weight_sum, cached.entry_count)
                }
            };
            total_entries += summary.entry_count;
            total_weight += summary.weight_sum;
            leaf_summaries.push(summary);
        }

        // 3. Collect segment paths and metadata
        let segment_paths: Vec<String> = self
            .segments
            .iter()
            .map(|s| s.path.to_string())
            .collect();

        let segment_metadata: Vec<CommittedSegment> = self
            .segments
            .iter()
            .map(|s| CommittedSegment {
                id: s.id.0,
                leaf_blocks: s.leaf_index.iter()
                    .map(|(&id, &(off, sz))| (id, off, sz))
                    .collect(),
                size_bytes: s.size_bytes,
            })
            .collect();

        // 4. Build metadata (does NOT invalidate segments)
        Ok(CommittedSegmentStorage {
            segment_paths,
            segment_metadata,
            leaf_summaries,
            total_entries,
            total_weight,
            num_leaves: self.leaves.len(),
            segment_path_prefix: self.config.segment_path_prefix.clone(),
        })
    }

    /// Generate the checkpoint metadata file path for this NodeStorage instance.
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        StoragePath::from(format!("{base}/nodestorage-{persistent_id}.dat"))
    }

    /// Save NodeStorage state for checkpoint, following the spine_async pattern.
    ///
    /// This method:
    /// 1. Flushes all dirty/never-spilled leaves to segment files
    /// 2. Marks segment FileReaders for checkpoint (prevents deletion on drop)
    /// 3. Registers segment FileReaders with `files` for atomic commit
    /// 4. Serializes and writes its own metadata file
    ///
    /// Like Spine, NodeStorage writes its own metadata file via `backend.write()`,
    /// which auto-calls `mark_for_checkpoint()`. The metadata file is NOT pushed
    /// to `files` — it persists through the backend's checkpoint lifecycle.
    ///
    /// # Arguments
    /// * `base` - Base storage path for checkpoint files
    /// * `persistent_id` - Unique identifier for this storage instance
    /// * `files` - Vector to append FileCommitters for atomic checkpoint commit
    pub fn save(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), FileFormatError>
    where
        Archived<L::Key>: RkyvDeserialize<L::Key, Deserializer>,
    {
        // Build checkpoint metadata (flushes dirty leaves internally)
        let metadata = self.prepare_checkpoint()?;

        // Register all segment FileReaders for atomic checkpoint commit.
        // This follows the spine_async pattern where all data files are
        // registered with the checkpoint for atomic commit.
        for segment in &self.segments {
            if let Some(ref reader) = segment.reader {
                reader.mark_for_checkpoint();
                files.push(reader.clone());
            }
        }

        // Serialize and write metadata file via backend.write().
        // Like Spine, the write result is dropped — backend.write() auto-calls
        // mark_for_checkpoint() so the file persists.
        let bytes = serialize_to_bytes(&metadata)?;
        let backend = self.config.storage_backend.clone()
            .or_else(|| Runtime::storage_backend().ok())
            .ok_or_else(|| FileFormatError::Io(
                "No storage backend available for save".to_string()
            ))?;
        let _file_reader = backend.write(
            &Self::checkpoint_file(base, persistent_id),
            bytes,
        ).map_err(|e| FileFormatError::Io(format!("Failed to write NodeStorage metadata: {}", e)))?;

        Ok(())
    }

    /// Restore NodeStorage state from its own metadata file.
    ///
    /// This method:
    /// 1. Reads the metadata file written by `save()`
    /// 2. Deserializes `CommittedSegmentStorage`
    /// 3. Calls internal `restore_from_committed()` to rebuild state
    /// 4. Returns info needed by the caller to rebuild internal tree nodes
    ///
    /// # Arguments
    /// * `base` - Base storage path for checkpoint files
    /// * `persistent_id` - Unique identifier for this storage instance
    ///
    /// # Returns
    /// `NodeStorageRestoreInfo` with leaf summaries and totals for internal node rebuild.
    pub fn restore(
        &mut self,
        base: &StoragePath,
        persistent_id: &str,
    ) -> Result<NodeStorageRestoreInfo<L::Key>, FileFormatError>
    where
        Archived<L::Key>: RkyvDeserialize<L::Key, Deserializer>,
    {
        let backend = self.config.storage_backend.clone()
            .or_else(|| Runtime::storage_backend().ok())
            .ok_or_else(|| FileFormatError::Io(
                "No storage backend available for restore".to_string()
            ))?;

        let path = Self::checkpoint_file(base, persistent_id);
        let content = backend.read(&path)
            .map_err(|e| FileFormatError::Io(format!("Failed to read NodeStorage metadata: {}", e)))?;

        // Deserialize CommittedSegmentStorage using rkyv
        let archived = unsafe {
            rkyv::archived_root::<CommittedSegmentStorage<L::Key>>(&content)
        };
        let mut deserializer = crate::storage::file::Deserializer::new(0);
        let committed: CommittedSegmentStorage<L::Key> =
            rkyv::Deserialize::deserialize(archived, &mut deserializer)
                .map_err(|e| FileFormatError::Deserialization(format!("{:?}", e)))?;

        // Extract info before consuming committed
        let info = NodeStorageRestoreInfo {
            leaf_summaries: committed.leaf_summaries.clone(),
            total_weight: committed.total_weight,
            total_entries: committed.total_entries,
            num_leaves: committed.num_leaves,
        };

        // Restore internal state from committed metadata
        self.restore_from_committed(&committed)?;

        Ok(info)
    }

    /// Restore NodeStorage state from committed checkpoint metadata.
    ///
    /// This method:
    /// 1. Opens segment files via StorageBackend (getting FileReaders)
    /// 2. Sets up segments with FileReaders for leaf loading
    /// 3. Creates evicted leaf slots from summaries
    /// 4. Builds leaf_disk_locations for efficient lookup
    ///
    /// After calling this, leaves are lazily loaded on demand via
    /// `load_leaf_from_disk()` or `get_leaf_reloading()`.
    ///
    /// This follows the spine_async restore pattern where segment files
    /// are opened via `StorageBackend::open()`, which returns FileReaders
    /// that never delete files on drop (they belong to the checkpoint).
    ///
    /// # Arguments
    /// * `committed` - Checkpoint metadata from `save()`
    ///
    /// # Errors
    /// Returns an error if segment files cannot be opened via StorageBackend.
    pub fn restore_from_committed(
        &mut self,
        committed: &CommittedSegmentStorage<L::Key>,
    ) -> Result<(), FileFormatError> {
        // Get storage backend for opening segment files
        let backend = self.config.storage_backend.clone()
            .or_else(|| Runtime::storage_backend().ok())
            .ok_or_else(|| FileFormatError::Io(
                "No storage backend available for restore".to_string()
            ))?;

        // Clear existing state
        self.leaves.clear();
        self.segments.clear();
        self.leaf_disk_locations.clear();
        self.dirty_leaves.clear();
        self.dirty_bytes = 0;
        self.pending_segment = None;

        // Restore segment_path_prefix (Bug B fix: ensures restored trees
        // keep their unique prefix for segment file names)
        self.config.segment_path_prefix = committed.segment_path_prefix.clone();

        // Create Evicted slots for each leaf with cached summary
        for summary in &committed.leaf_summaries {
            let (first_key_bytes, has_first_key) = if let Some(ref key) = summary.first_key {
                (
                    Vec::<u8>::from(serialize_to_bytes(key).unwrap_or_default()),
                    true,
                )
            } else {
                (Vec::new(), false)
            };
            let cached = CachedLeafSummary {
                first_key_bytes,
                has_first_key,
                weight_sum: summary.weight_sum,
                entry_count: summary.entry_count,
            };
            self.leaves.push(LeafSlot::Evicted(cached));
        }

        // Open segment files via StorageBackend and build metadata.
        // Using backend.open() returns FileReaders that never delete files
        // on drop, which is correct for restored checkpoint segments.
        for (path_str, meta) in committed.segment_paths.iter()
            .zip(committed.segment_metadata.iter())
        {
            let path = StoragePath::from(path_str.as_str());
            let segment_id = SegmentId::new(meta.id);

            // Open the segment file via StorageBackend to get a FileReader
            let reader = backend.open(&path)
                .map_err(|e| FileFormatError::Io(format!(
                    "Failed to open segment file '{}': {}", path_str, e
                )))?;

            // Build segment metadata with reader
            let mut segment = SegmentMetadata::with_reader(segment_id, path, reader);
            segment.size_bytes = meta.size_bytes;
            segment.leaf_count = meta.leaf_blocks.len();

            for &(leaf_id, offset, size) in &meta.leaf_blocks {
                segment.leaf_index.insert(leaf_id, (offset, size));
                self.leaf_disk_locations.insert(
                    leaf_id,
                    LeafDiskLocation::new(segment_id, offset, size),
                );
            }

            self.segments.push(segment);

            // Track the next segment ID
            if meta.id >= self.next_segment_id {
                self.next_segment_id = meta.id + 1;
            }
        }

        // Update stats
        let num_leaves = committed.num_leaves;
        self.stats.leaf_node_count = num_leaves;
        self.stats.evicted_leaf_count = num_leaves;
        self.stats.total_entries = committed.total_entries;

        Ok(())
    }

    /// Mark all leaves as evicted (on disk, not in memory) for segment-based restore.
    ///
    /// **Note**: This method creates segments WITHOUT FileReaders. Use
    /// `restore_from_committed()` instead for proper StorageBackend integration,
    /// which opens segments via `backend.open()` to get FileReaders for lazy
    /// leaf loading.
    ///
    /// # Arguments
    ///
    /// - `segment_paths`: Paths to segment files (relative to checkpoint base)
    /// - `segment_metadata`: Per-segment metadata with leaf locations
    /// - `summaries`: Leaf summaries from checkpoint (stored in LeafSlot::Evicted)
    pub fn mark_all_leaves_evicted(
        &mut self,
        segment_paths: Vec<StoragePath>,
        segment_metadata: Vec<CommittedSegment>,
        summaries: Vec<LeafSummary<L::Key>>,
    ) {
        // Clear existing state
        self.leaves.clear();
        self.segments.clear();
        self.leaf_disk_locations.clear();
        self.dirty_leaves.clear();
        self.pending_segment = None;

        // Create Evicted slots for each leaf with cached summary
        for summary in &summaries {
            let (first_key_bytes, has_first_key) = if let Some(ref key) = summary.first_key {
                (
                    Vec::<u8>::from(serialize_to_bytes(key).unwrap_or_default()),
                    true,
                )
            } else {
                (Vec::new(), false)
            };
            let cached = CachedLeafSummary {
                first_key_bytes,
                has_first_key,
                weight_sum: summary.weight_sum,
                entry_count: summary.entry_count,
            };
            self.leaves.push(LeafSlot::Evicted(cached));
        }

        // Restore segment metadata and build leaf_disk_locations
        for (path, meta) in segment_paths.into_iter().zip(segment_metadata.into_iter()) {
            let segment_id = SegmentId::new(meta.id);

            // Build the segment metadata
            let mut segment = SegmentMetadata::new(segment_id, path);
            segment.size_bytes = meta.size_bytes;
            segment.leaf_count = meta.leaf_blocks.len();

            for (leaf_id, offset, size) in meta.leaf_blocks {
                segment.leaf_index.insert(leaf_id, (offset, size));
                self.leaf_disk_locations.insert(
                    leaf_id,
                    LeafDiskLocation::new(segment_id, offset, size),
                );
            }

            self.segments.push(segment);

            // Track the next segment ID
            if meta.id >= self.next_segment_id {
                self.next_segment_id = meta.id + 1;
            }
        }

        // Update stats
        let num_leaves = summaries.len();
        self.stats.leaf_node_count = num_leaves;
        self.stats.evicted_leaf_count = num_leaves;
    }
}

// =============================================================================
// Leaf File - Block-Based Disk Storage
// =============================================================================
//
// This section implements block-based storage for leaf nodes with:
// - 512-byte block alignment for Direct I/O compatibility
// - CRC32C checksums for data integrity
// - rkyv serialization for compact storage
// - In-memory index for fast lookups

// =============================================================================
// File Format Constants
// =============================================================================

/// Block alignment for all disk I/O (512 bytes).
/// Matches `FBuf::ALIGNMENT` in the DBSP storage layer.
pub const BLOCK_ALIGNMENT: usize = 512;

/// File header size (one block).
pub const FILE_HEADER_SIZE: usize = BLOCK_ALIGNMENT;

/// Data block header size (checksum + magic + leaf_id + data_len).
/// Layout: [checksum:4][magic:4][leaf_id:8][data_len:8] = 24 bytes
pub const DATA_BLOCK_HEADER_SIZE: usize = 4 + 4 + 8 + 8;

/// Index entry size (leaf_id + offset + size).
const INDEX_ENTRY_SIZE: usize = 8 + 8 + 4; // 20 bytes

/// File format version.
const FORMAT_VERSION: u32 = 1;

/// Magic number for file header: "OSML" (Order Statistics Multiset Leaf file)
const MAGIC_FILE_HEADER: [u8; 4] = *b"OSML";

/// Magic number for data blocks: "OSMD" (OSM Data block)
const MAGIC_DATA_BLOCK: [u8; 4] = *b"OSMD";

/// Magic number for index block: "OSMI" (OSM Index block)
const MAGIC_INDEX_BLOCK: [u8; 4] = *b"OSMI";

// =============================================================================
// File Format Types
// =============================================================================

/// Location of a block within a leaf file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct FileBlockLocation {
    /// Byte offset from start of file
    pub offset: u64,
    /// Size of the block in bytes (always 512-byte aligned)
    pub size: u32,
}

impl FileBlockLocation {
    /// Create a new block location.
    pub fn new(offset: u64, size: u32) -> Self {
        Self { offset, size }
    }
}

/// File header structure (512 bytes).
#[derive(Clone, Debug, Default)]
pub struct FileHeader {
    /// Number of leaves stored in this file
    pub num_leaves: u64,
    /// Byte offset of the index block
    pub index_offset: u64,
    /// Total entries across all leaves
    pub total_entries: u64,
    /// Total weight across all leaves
    pub total_weight: i64,
}

impl FileHeader {
    /// Serialize the header to a 512-byte block with checksum.
    pub fn to_bytes(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut block = [0u8; FILE_HEADER_SIZE];

        // Leave bytes [0..4] for checksum (computed at end)
        block[4..8].copy_from_slice(&MAGIC_FILE_HEADER);
        block[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        block[12..20].copy_from_slice(&self.num_leaves.to_le_bytes());
        block[20..28].copy_from_slice(&self.index_offset.to_le_bytes());
        block[28..36].copy_from_slice(&self.total_entries.to_le_bytes());
        block[36..44].copy_from_slice(&self.total_weight.to_le_bytes());

        // Compute checksum of bytes [4..512]
        let checksum = crc32c::crc32c(&block[4..]);
        block[0..4].copy_from_slice(&checksum.to_le_bytes());

        block
    }

    /// Parse a header from a 512-byte block, verifying the checksum.
    pub fn from_bytes(block: &[u8; FILE_HEADER_SIZE]) -> Result<Self, FileFormatError> {
        // Verify checksum
        let stored_checksum = u32::from_le_bytes(block[0..4].try_into().unwrap());
        let computed_checksum = crc32c::crc32c(&block[4..]);
        if stored_checksum != computed_checksum {
            return Err(FileFormatError::ChecksumMismatch {
                expected: computed_checksum,
                found: stored_checksum,
            });
        }

        // Verify magic
        if &block[4..8] != &MAGIC_FILE_HEADER {
            return Err(FileFormatError::InvalidMagic {
                expected: MAGIC_FILE_HEADER,
                found: [block[4], block[5], block[6], block[7]],
            });
        }

        // Verify version
        let version = u32::from_le_bytes(block[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(FileFormatError::UnsupportedVersion {
                expected: FORMAT_VERSION,
                found: version,
            });
        }

        Ok(Self {
            num_leaves: u64::from_le_bytes(block[12..20].try_into().unwrap()),
            index_offset: u64::from_le_bytes(block[20..28].try_into().unwrap()),
            total_entries: u64::from_le_bytes(block[28..36].try_into().unwrap()),
            total_weight: i64::from_le_bytes(block[36..44].try_into().unwrap()),
        })
    }
}

/// Entry in the leaf index.
#[derive(Clone, Copy, Debug, Default)]
struct IndexEntry {
    /// Unique leaf ID
    leaf_id: u64,
    /// Location of the data block
    location: FileBlockLocation,
}

// These methods may be used in future checkpoint recovery code.
#[allow(dead_code)]
impl IndexEntry {
    /// Create a new index entry.
    fn new(leaf_id: u64, offset: u64, size: u32) -> Self {
        Self {
            leaf_id,
            location: FileBlockLocation::new(offset, size),
        }
    }

    /// Serialize to bytes (20 bytes).
    fn to_bytes(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut bytes = [0u8; INDEX_ENTRY_SIZE];
        bytes[0..8].copy_from_slice(&self.leaf_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.location.offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.location.size.to_le_bytes());
        bytes
    }

    /// Parse from bytes.
    fn from_bytes(bytes: &[u8]) -> Self {
        let leaf_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let size = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        Self::new(leaf_id, offset, size)
    }
}

/// Errors that can occur during file format operations.
#[derive(Debug, Clone)]
pub enum FileFormatError {
    /// CRC32C checksum verification failed
    ChecksumMismatch { expected: u32, found: u32 },
    /// Invalid magic number
    InvalidMagic { expected: [u8; 4], found: [u8; 4] },
    /// Unsupported file format version
    UnsupportedVersion { expected: u32, found: u32 },
    /// I/O error
    Io(String),
    /// Serialization error
    Serialization(String),
    /// Deserialization error
    Deserialization(String),
    /// Block not found in index
    BlockNotFound { leaf_id: u64 },
}

impl std::fmt::Display for FileFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormatError::ChecksumMismatch { expected, found } => {
                write!(
                    f,
                    "checksum mismatch: expected {:#x}, found {:#x}",
                    expected, found
                )
            }
            FileFormatError::InvalidMagic { expected, found } => {
                write!(
                    f,
                    "invalid magic: expected {:?}, found {:?}",
                    expected, found
                )
            }
            FileFormatError::UnsupportedVersion { expected, found } => {
                write!(
                    f,
                    "unsupported version: expected {}, found {}",
                    expected, found
                )
            }
            FileFormatError::Io(msg) => write!(f, "I/O error: {}", msg),
            FileFormatError::Serialization(msg) => write!(f, "serialization error: {}", msg),
            FileFormatError::Deserialization(msg) => write!(f, "deserialization error: {}", msg),
            FileFormatError::BlockNotFound { leaf_id } => {
                write!(f, "block not found for leaf_id {}", leaf_id)
            }
        }
    }
}

impl std::error::Error for FileFormatError {}

// =============================================================================
// File Format Utility Functions
// =============================================================================

/// Round a size up to the next 512-byte boundary.
#[inline]
fn align_to_block(size: usize) -> usize {
    (size + BLOCK_ALIGNMENT - 1) & !(BLOCK_ALIGNMENT - 1)
}

/// Create a data block header.
fn create_data_block_header(leaf_id: u64, data_len: u64) -> [u8; DATA_BLOCK_HEADER_SIZE] {
    let mut header = [0u8; DATA_BLOCK_HEADER_SIZE];
    // bytes [0..4] reserved for checksum (computed later over entire block)
    header[4..8].copy_from_slice(&MAGIC_DATA_BLOCK);
    header[8..16].copy_from_slice(&leaf_id.to_le_bytes());
    header[16..24].copy_from_slice(&data_len.to_le_bytes());
    header
}

/// Verify a data block header and extract the leaf_id and data_len.
pub fn verify_data_block_header(block: &[u8]) -> Result<(u64, u64), FileFormatError> {
    if block.len() < DATA_BLOCK_HEADER_SIZE {
        return Err(FileFormatError::Io("block too small".to_string()));
    }

    // Verify checksum
    let stored_checksum = u32::from_le_bytes(block[0..4].try_into().unwrap());
    let computed_checksum = crc32c::crc32c(&block[4..]);
    if stored_checksum != computed_checksum {
        return Err(FileFormatError::ChecksumMismatch {
            expected: computed_checksum,
            found: stored_checksum,
        });
    }

    // Verify magic
    if &block[4..8] != &MAGIC_DATA_BLOCK {
        return Err(FileFormatError::InvalidMagic {
            expected: MAGIC_DATA_BLOCK,
            found: [block[4], block[5], block[6], block[7]],
        });
    }

    let leaf_id = u64::from_le_bytes(block[8..16].try_into().unwrap());
    let data_len = u64::from_le_bytes(block[16..24].try_into().unwrap());
    Ok((leaf_id, data_len))
}

/// Set the checksum in a block (first 4 bytes).
fn set_block_checksum(block: &mut [u8]) {
    let checksum = crc32c::crc32c(&block[4..]);
    block[0..4].copy_from_slice(&checksum.to_le_bytes());
}

/// Deserialize a leaf node from rkyv-serialized bytes.
///
/// The data should be the serialized leaf content (without block header/checksum).
fn deserialize_leaf<L>(data: &[u8]) -> Result<L, FileFormatError>
where
    L: Archive,
    Archived<L>: RkyvDeserialize<L, Deserializer>,
{
    // SAFETY: The data was serialized by serialize_to_bytes() using the same rkyv
    // serializer, and we verified the block's CRC32C checksum in the caller.
    // The checksum ensures data integrity, and the serialization format is
    // guaranteed to match. We use unsafe archived_root instead of
    // check_archived_root for performance since the checksum already validates
    // the data hasn't been corrupted.
    let archived = unsafe { rkyv::archived_root::<L>(data) };
    let leaf: L = archived
        .deserialize(&mut Deserializer::default())
        .map_err(|e| FileFormatError::Serialization(format!("{:?}", e)))?;
    Ok(leaf)
}

// =============================================================================
// Drop Implementation for NodeStorage
// =============================================================================

impl<I, L> Drop for NodeStorage<I, L> {
    fn drop(&mut self) {
        // Segment file cleanup is handled by the FileReader's drop behavior:
        //
        // - Files created via create_named()+complete(): deleted on drop UNLESS
        //   mark_for_checkpoint() was called (checkpoint files survive)
        // - Files opened via backend.open(): NEVER deleted on drop (restored
        //   segments from checkpoints survive)
        //
        // We clear the segments vector to drop the FileReaders, which triggers
        // the appropriate cleanup based on each file's lifecycle state.
        self.segments.clear();
    }
}
