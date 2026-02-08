//! Stateful percentile operator with incremental updates.
//!
//! This module provides a dedicated `PercentileOperator` that maintains
//! `OrderStatisticsZSet` state per key across steps, enabling O(log n)
//! incremental updates instead of re-scanning the entire input trace each step.
//!
//! # Architecture
//!
//! Unlike the aggregate-based percentile implementation which re-scans the entire
//! input each step, this operator:
//!
//! 1. Maintains per-key `OrderStatisticsZSet` state across steps
//! 2. Applies delta changes incrementally (O(log n) per change)
//! 3. Supports checkpoint/restore for fault tolerance
//! 4. Uses spill-to-disk for large trees via `NodeStorage`
//!
//! # Usage
//!
//! ```ignore
//! let percentiles = input_stream.percentile_cont_stateful(
//!     Some("my_percentile_op"),
//!     0.5,   // percentile
//!     true,  // ascending
//! );
//! ```

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Neg;
use std::sync::Arc;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::{
    Circuit, DBData, Error, Runtime, Stream, ZWeight,
    algebra::{
        AddAssignByRef, DEFAULT_BRANCHING_FACTOR, F32, F64, HasOne, HasZero,
        OrderStatisticsZSet,
    },
    circuit::{
        GlobalNodeId, OwnershipPreference, Scope,
        metadata::{MetaItem, OperatorMeta},
        operator_traits::{Operator, UnaryOperator},
    },
    dynamic::DowncastTrait,
    node_storage::NodeStorageConfig,
    storage::file::{Deserializer, to_bytes},
    trace::{BatchReader, Cursor},
    typed_batch::{BatchReader as TypedBatchReader, OrdIndexedZSet, TypedBatch},
    utils::{IsNone, Tup2},
};
use feldera_storage::{FileCommitter, StoragePath};

use super::super::require_persistent_id;

/// Trait for types that support linear interpolation.
///
/// Used by PERCENTILE_CONT to interpolate between adjacent values.
///
/// # SQL Standard Behavior
///
/// According to SQL standard, PERCENTILE_CONT:
/// - Only operates on numeric types (INTEGER, DECIMAL, FLOAT, DOUBLE, etc.)
/// - Always returns DOUBLE PRECISION, even for integer inputs
/// - Performs linear interpolation between adjacent values
///
/// For integer columns, the SQL compiler should cast to DOUBLE before
/// computing PERCENTILE_CONT. This trait is only implemented for
/// floating-point types to enforce this requirement.
///
/// For non-numeric types (strings, dates, etc.), use PERCENTILE_DISC instead,
/// which returns an actual value from the set without interpolation.
pub trait Interpolate: Clone {
    /// Compute linear interpolation: lower + fraction * (upper - lower)
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self;
}

// Implement for primitive floating-point types only.
// Integer types should NOT implement Interpolate because SQL standard
// requires PERCENTILE_CONT to return DOUBLE, not the original integer type.
impl Interpolate for f64 {
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self {
        lower + fraction * (upper - lower)
    }
}

impl Interpolate for f32 {
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self {
        lower + (fraction as f32) * (upper - lower)
    }
}

// Implement for F64 and F32 wrappers (DBSP's ordered float types)
impl Interpolate for F64 {
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self {
        let l = lower.into_inner();
        let u = upper.into_inner();
        F64::new(l + fraction * (u - l))
    }
}

impl Interpolate for F32 {
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self {
        let l = lower.into_inner() as f64;
        let u = upper.into_inner() as f64;
        F32::new((l + fraction * (u - l)) as f32)
    }
}

// Implement for Option<T> where T is Interpolate.
// This handles nullable columns: Option<F64>, Option<F32>, etc.
impl<T: Interpolate> Interpolate for Option<T> {
    fn interpolate(lower: &Self, upper: &Self, fraction: f64) -> Self {
        match (lower, upper) {
            (Some(l), Some(u)) => Some(T::interpolate(l, u, fraction)),
            (Some(l), None) => Some(l.clone()),
            (None, Some(u)) => Some(u.clone()),
            (None, None) => None,
        }
    }
}

// =============================================================================
// Marker types for PERCENTILE_CONT vs PERCENTILE_DISC modes
// =============================================================================

/// Marker type for PERCENTILE_CONT (continuous/interpolated) mode.
///
/// PERCENTILE_CONT performs linear interpolation between adjacent values.
/// According to SQL standard, it only works with numeric types and always
/// returns DOUBLE PRECISION.
#[derive(Clone, Copy, Debug, Default)]
pub struct ContMode;

/// Marker type for PERCENTILE_DISC (discrete) mode.
///
/// PERCENTILE_DISC returns an actual value from the set without interpolation.
/// It works with any ordered type (numeric, string, date, etc.) and returns
/// the same type as the input.
#[derive(Clone, Copy, Debug, Default)]
pub struct DiscMode;

// =============================================================================
// Checkpoint Data Structures
// =============================================================================

/// Committed (checkpoint) state for the PercentileOperator.
///
/// The operator only stores its own metadata here. Each tree's NodeStorage
/// writes/reads its own metadata file (like Spine), so we only need
/// `tree_ids` to know which trees to restore, not embedded storage metadata.
///
/// # Benefits
///
/// - Follows Spine pattern: storage layer manages its own persistence
/// - Checkpoint size: operator-level metadata is tiny (keys + prev_output + config)
/// - `next_tree_id` is saved/restored to prevent ID collisions
/// - segment_path_prefix is saved in NodeStorage's own metadata
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive(check_bytes)]
struct CommittedPercentileOperator<K, V>
where
    K: Archive,
    V: Archive,
{
    /// Per-key tree IDs (NodeStorage writes its own metadata files)
    tree_ids: Vec<(K, u64)>,
    /// Previous output values for delta computation
    prev_output: Vec<(K, Option<V>)>,
    /// Configuration
    percentile: u64, // f64 bits as u64 for rkyv compatibility
    ascending: bool,
    /// Branching factor for tree reconstruction
    branching_factor: u32,
    /// Next tree ID counter (`next_tree_id` is saved/restored to prevent ID collisions)
    next_tree_id: u64,
}

// =============================================================================
// Percentile Operator
// =============================================================================

/// A stateful operator that computes percentiles incrementally.
///
/// This operator maintains `OrderStatisticsZSet` state per key across steps,
/// enabling O(log n) incremental updates instead of O(n) per-step rescanning.
///
/// # Type Parameters
///
/// - `K`: Key type for grouping
/// - `V`: Value type being aggregated
/// - `Mode`: Either `ContMode` (interpolated) or `DiscMode` (discrete)
///
/// # SQL Standard Behavior
///
/// - **PERCENTILE_CONT** (`ContMode`): Requires floating-point types (F32, F64).
///   For integer columns, the SQL compiler should cast to DOUBLE before computing.
///   Returns an interpolated value that may not exist in the original data.
///
/// - **PERCENTILE_DISC** (`DiscMode`): Works with any ordered type (numeric,
///   string, date, etc.). Returns an actual value from the set.
pub struct PercentileOperator<K, V, Mode>
where
    K: DBData,
    V: DBData,
{
    /// Per-key order statistics trees
    trees: BTreeMap<K, OrderStatisticsZSet<V>>,

    /// Per-key tree IDs for checkpoint file naming.
    /// Maps each key to a unique tree_id used to derive persistent_id
    /// for each tree's NodeStorage metadata file.
    tree_ids: BTreeMap<K, u64>,

    /// Previous output values for computing deltas
    prev_output: BTreeMap<K, Option<V>>,

    /// Percentile to compute (0.0 to 1.0)
    percentile: f64,

    /// Sort order (true = ascending)
    ascending: bool,

    /// Storage config for spill-to-disk
    storage_config: NodeStorageConfig,

    /// Counter for generating unique segment path prefixes per tree.
    /// Each tree gets a unique prefix like "t0_", "t1_", etc. to prevent
    /// segment file name collisions when multiple trees share the same
    /// StorageBackend namespace.
    next_tree_id: u64,

    /// Global node ID for checkpoint file naming
    global_id: GlobalNodeId,

    /// Marker for CONT vs DISC mode (zero-sized, compile-time only)
    _mode: std::marker::PhantomData<Mode>,
}

impl<K, V> PercentileOperator<K, V, ContMode>
where
    K: DBData,
    V: DBData,
{
    /// Create a new PERCENTILE_CONT operator.
    ///
    /// PERCENTILE_CONT performs linear interpolation between adjacent values.
    /// It should only be used with floating-point types (F32, F64, Option<F32>, Option<F64>).
    /// For integer columns, the SQL compiler should cast to DOUBLE before calling this.
    pub fn new_cont(percentile: f64, ascending: bool) -> Self {
        Self {
            trees: BTreeMap::new(),
            tree_ids: BTreeMap::new(),
            prev_output: BTreeMap::new(),
            percentile,
            ascending,
            storage_config: NodeStorageConfig::from_runtime(),
            next_tree_id: 0,
            global_id: GlobalNodeId::root(),
            _mode: std::marker::PhantomData,
        }
    }
}

impl<K, V> PercentileOperator<K, V, DiscMode>
where
    K: DBData,
    V: DBData,
{
    /// Create a new PERCENTILE_DISC operator.
    ///
    /// PERCENTILE_DISC returns an actual value from the set without interpolation.
    /// It works with any ordered type: numeric, string, date, timestamp, etc.
    pub fn new_disc(percentile: f64, ascending: bool) -> Self {
        Self {
            trees: BTreeMap::new(),
            tree_ids: BTreeMap::new(),
            prev_output: BTreeMap::new(),
            percentile,
            ascending,
            storage_config: NodeStorageConfig::from_runtime(),
            next_tree_id: 0,
            global_id: GlobalNodeId::root(),
            _mode: std::marker::PhantomData,
        }
    }
}

impl<K, V, Mode> PercentileOperator<K, V, Mode>
where
    K: DBData,
    V: DBData,
    <K as Archive>::Archived: Ord,
    <V as Archive>::Archived: Ord,
{
    /// Generate the checkpoint file path for this operator.
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        StoragePath::from(format!("{base}/percentile-{persistent_id}.dat"))
    }
}

impl<K, V, Mode> Operator for PercentileOperator<K, V, Mode>
where
    K: DBData,
    V: DBData,
    Mode: Send + 'static,
    <K as Archive>::Archived: Ord,
    <V as Archive>::Archived: Ord,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("PercentileOperator")
    }

    fn init(&mut self, global_id: &GlobalNodeId) {
        self.global_id = global_id.clone();
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let num_keys = self.trees.len();
        let total_entries: usize = self.trees.values().map(|t| t.num_keys()).sum();
        let total_evicted: usize = self.trees.values().map(|t| t.evicted_leaf_count()).sum();

        meta.extend([
            (Cow::Borrowed("num_keys"), MetaItem::Int(num_keys)),
            (Cow::Borrowed("total_entries"), MetaItem::Int(total_entries)),
            (
                Cow::Borrowed("percentile_pct"),
                MetaItem::Int((self.percentile * 100.0) as usize),
            ),
            (Cow::Borrowed("ascending"), MetaItem::Bool(self.ascending)),
            (
                Cow::Borrowed("evicted_leaves"),
                MetaItem::Count(total_evicted),
            ),
        ]);
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Not at fixedpoint if we have any state
        self.trees.is_empty()
    }

    fn clock_end(&mut self, _scope: Scope) {
        // Flush dirty leaves to disk and evict clean leaves for all trees.
        // By clock_end(), all mutations are complete for this step, making
        // eviction safe. Follows existing DBSP lifecycle patterns.
        for tree in self.trees.values_mut() {
            if tree.should_flush() {
                if let Err(e) = tree.flush_and_evict() {
                    tracing::warn!("PercentileOperator: flush_and_evict failed: {}", e);
                }
            }
        }
    }

    fn checkpoint(
        &mut self,
        base: &StoragePath,
        persistent_id: Option<&str>,
        files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        // Each tree's OSM writes its own metadata file + registers segment files.
        // This follows the Spine pattern: storage layer manages its own persistence.
        for (key, tree) in self.trees.iter_mut() {
            let tree_id = self.tree_ids[key];
            let tree_pid = format!("{}_t{}", persistent_id, tree_id);
            tree.save(base, &tree_pid, files)?;
        }

        // Write operator-level metadata (small — just keys, prev_output, config).
        // Like Spine, metadata is written via backend.write() which auto-calls
        // mark_for_checkpoint(). Not pushed to `files`.
        let committed = CommittedPercentileOperator {
            tree_ids: self
                .tree_ids
                .iter()
                .map(|(k, id)| (k.clone(), *id))
                .collect(),
            prev_output: self
                .prev_output
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            percentile: self.percentile.to_bits(),
            ascending: self.ascending,
            branching_factor: DEFAULT_BRANCHING_FACTOR as u32,
            next_tree_id: self.next_tree_id,
        };

        let bytes = to_bytes(&committed).expect("Serializing checkpoint should work");
        let backend = self
            .storage_config
            .storage_backend
            .clone()
            .or_else(|| Runtime::storage_backend().ok())
            .expect("No storage backend available for checkpoint");
        let _file = backend.write(&Self::checkpoint_file(base, persistent_id), bytes)?;

        Ok(())
    }

    fn restore(&mut self, base: &StoragePath, persistent_id: Option<&str>) -> Result<(), Error> {
        let persistent_id = require_persistent_id(persistent_id, &self.global_id)?;

        // Read operator metadata file
        let path = Self::checkpoint_file(base, persistent_id);
        let backend = self
            .storage_config
            .storage_backend
            .clone()
            .or_else(|| Runtime::storage_backend().ok())
            .expect("No storage backend available for restore");
        let content = backend.read(&path)?;

        // Deserialize using rkyv
        let archived =
            unsafe { rkyv::archived_root::<CommittedPercentileOperator<K, V>>(&content) };

        // Use DBSP's Deserializer
        let mut deserializer = Deserializer::new(0);

        let branching_factor = archived.branching_factor as usize;

        // restore next_tree_id to prevent ID collisions
        self.next_tree_id = archived.next_tree_id;

        // Restore each tree via OSM::restore (NodeStorage reads its own metadata).
        self.trees.clear();
        self.tree_ids.clear();
        for archived_entry in archived.tree_ids.iter() {
            let key: K = rkyv::Deserialize::deserialize(&archived_entry.0, &mut deserializer)
                .map_err(|e| {
                    use std::io::{Error as IoError, ErrorKind};
                    Error::IO(IoError::new(
                        ErrorKind::Other,
                        format!("Failed to deserialize key: {e:?}"),
                    ))
                })?;
            let tree_id: u64 = archived_entry.1.into();

            let tree_pid = format!("{}_t{}", persistent_id, tree_id);
            let mut config = self.storage_config.clone();
            config.segment_path_prefix = format!("t{}_", tree_id);

            // OSM::restore delegates to NodeStorage::restore which reads its own metadata.
            // Bug B fix: NodeStorage restores segment_path_prefix from its saved state.
            let tree = OrderStatisticsZSet::restore(base, &tree_pid, branching_factor, config)?;
            self.trees.insert(key.clone(), tree);
            self.tree_ids.insert(key, tree_id);
        }

        // Restore prev_output
        self.prev_output.clear();
        for archived_entry in archived.prev_output.iter() {
            let key: K = rkyv::Deserialize::deserialize(&archived_entry.0, &mut deserializer)
                .map_err(|e| {
                    use std::io::{Error as IoError, ErrorKind};
                    Error::IO(IoError::new(
                        ErrorKind::Other,
                        format!("Failed to deserialize prev_output key: {e:?}"),
                    ))
                })?;

            let value: Option<V> =
                rkyv::Deserialize::deserialize(&archived_entry.1, &mut deserializer).map_err(
                    |e| {
                        use std::io::{Error as IoError, ErrorKind};
                        Error::IO(IoError::new(
                            ErrorKind::Other,
                            format!("Failed to deserialize prev_output value: {e:?}"),
                        ))
                    },
                )?;

            self.prev_output.insert(key, value);
        }

        // Restore config
        self.percentile = f64::from_bits(archived.percentile);
        self.ascending = archived.ascending;

        Ok(())
    }

    fn clear_state(&mut self) -> Result<(), Error> {
        self.trees.clear();
        self.tree_ids.clear();
        self.prev_output.clear();
        Ok(())
    }
}

// =============================================================================
// UnaryOperator implementation for PERCENTILE_CONT (requires Interpolate)
// =============================================================================

impl<K, V> UnaryOperator<OrdIndexedZSet<K, V>, OrdIndexedZSet<K, Option<V>>>
    for PercentileOperator<K, V, ContMode>
where
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone + Interpolate,
    <V as Archive>::Archived: Ord,
{
    async fn eval(&mut self, delta: &OrdIndexedZSet<K, V>) -> OrdIndexedZSet<K, Option<V>> {
        // Track changed keys
        let mut changed_keys: Vec<K> = Vec::new();

        // Process each key in the delta using the inner dynamic batch
        let inner = delta.inner();
        let mut cursor = inner.cursor();

        while cursor.key_valid() {
            // Downcast the key from dynamic type
            let key: K = unsafe { cursor.key().downcast::<K>().clone() };

            // Get or create tree for this key, assigning a unique segment
            // path prefix to prevent file name collisions across trees.
            let next_tree_id = &mut self.next_tree_id;
            let tree_ids = &mut self.tree_ids;
            let storage_config = &self.storage_config;
            let tree = self.trees.entry(key.clone()).or_insert_with(|| {
                let tree_id = *next_tree_id;
                let mut config = storage_config.clone();
                config.segment_path_prefix = format!("t{}_", tree_id);
                *next_tree_id += 1;
                tree_ids.insert(key.clone(), tree_id);
                OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config)
            });

            // Process all values for this key
            while cursor.val_valid() {
                // Downcast the value
                let value: V = unsafe { cursor.val().downcast::<V>().clone() };

                // Skip NULL values - SQL PERCENTILE_CONT excludes NULLs
                if value.is_none() {
                    cursor.step_val();
                    continue;
                }

                // Sum up the weight across all timestamps (for ZSets, time is ())
                let mut weight: ZWeight = HasZero::zero();
                cursor.map_times(&mut |_, w| {
                    weight.add_assign_by_ref(unsafe { w.downcast() });
                });

                // Insert handles both positive (insert) and negative (delete) weights
                if !weight.is_zero() {
                    tree.insert(value, weight);
                }

                cursor.step_val();
            }

            // Inline backpressure: flush proactively if this tree exceeded threshold
            if tree.should_flush() {
                let _ = tree.flush_and_evict();
            }

            changed_keys.push(key);
            cursor.step_key();
        }

        // Build output as Vec<Tup2<Tup2<K, Option<V>>, ZWeight>>
        let mut tuples: Vec<Tup2<Tup2<K, Option<V>>, ZWeight>> = Vec::new();

        for key in changed_keys {
            // Compute new percentile value for this key using interpolation
            let new_value = self.trees.get_mut(&key).and_then(|tree| {
                if tree.is_empty() {
                    None
                } else {
                    // PERCENTILE_CONT: interpolate between adjacent values
                    tree.select_percentile_bounds(self.percentile, self.ascending)
                        .map(|(lower, upper, fraction)| V::interpolate(&lower, &upper, fraction))
                }
            });
            let had_prev = self.prev_output.contains_key(&key);
            let prev_value = self.prev_output.get(&key).cloned().flatten();

            // Only emit if value changed (using had_prev to distinguish
            // "never emitted" from "emitted None")
            if new_value != prev_value {
                // Retract old output if we previously emitted something
                if had_prev {
                    tuples.push(Tup2(
                        Tup2(key.clone(), prev_value.clone()),
                        ZWeight::one().neg(),
                    ));
                }

                // Emit insertion of new value (or NULL if group became empty
                // and we had a previous value to retract)
                if new_value.is_some() || had_prev {
                    tuples.push(Tup2(Tup2(key.clone(), new_value.clone()), ZWeight::one()));
                }

                // Update prev_output
                self.prev_output.insert(key.clone(), new_value);
            }

            // Clean up empty trees but keep prev_output for NULL retraction tracking
            if let Some(tree) = self.trees.get(&key) {
                if tree.is_empty() {
                    self.trees.remove(&key);
                    self.tree_ids.remove(&key);
                }
            }
        }

        // Use TypedBatch::from_tuples to create the output
        TypedBatch::from_tuples((), tuples)
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

// =============================================================================
// UnaryOperator implementation for PERCENTILE_DISC (no Interpolate required)
// =============================================================================

impl<K, V> UnaryOperator<OrdIndexedZSet<K, V>, OrdIndexedZSet<K, Option<V>>>
    for PercentileOperator<K, V, DiscMode>
where
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone, // No Interpolate bound - works with any ordered type
    <V as Archive>::Archived: Ord,
{
    async fn eval(&mut self, delta: &OrdIndexedZSet<K, V>) -> OrdIndexedZSet<K, Option<V>> {
        // Track changed keys
        let mut changed_keys: Vec<K> = Vec::new();

        // Process each key in the delta using the inner dynamic batch
        let inner = delta.inner();
        let mut cursor = inner.cursor();

        while cursor.key_valid() {
            // Downcast the key from dynamic type
            let key: K = unsafe { cursor.key().downcast::<K>().clone() };

            // Get or create tree for this key, assigning a unique segment
            // path prefix to prevent file name collisions across trees.
            let next_tree_id = &mut self.next_tree_id;
            let tree_ids = &mut self.tree_ids;
            let storage_config = &self.storage_config;
            let tree = self.trees.entry(key.clone()).or_insert_with(|| {
                let tree_id = *next_tree_id;
                let mut config = storage_config.clone();
                config.segment_path_prefix = format!("t{}_", tree_id);
                *next_tree_id += 1;
                tree_ids.insert(key.clone(), tree_id);
                OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config)
            });

            // Process all values for this key
            while cursor.val_valid() {
                // Downcast the value
                let value: V = unsafe { cursor.val().downcast::<V>().clone() };

                // Skip NULL values - SQL PERCENTILE_DISC excludes NULLs
                if value.is_none() {
                    cursor.step_val();
                    continue;
                }

                // Sum up the weight across all timestamps (for ZSets, time is ())
                let mut weight: ZWeight = HasZero::zero();
                cursor.map_times(&mut |_, w| {
                    weight.add_assign_by_ref(unsafe { w.downcast() });
                });

                // Insert handles both positive (insert) and negative (delete) weights
                if !weight.is_zero() {
                    tree.insert(value, weight);
                }

                cursor.step_val();
            }

            // Inline backpressure: flush proactively if this tree exceeded threshold
            if tree.should_flush() {
                let _ = tree.flush_and_evict();
            }

            changed_keys.push(key);
            cursor.step_key();
        }

        // Build output as Vec<Tup2<Tup2<K, Option<V>>, ZWeight>>
        let mut tuples: Vec<Tup2<Tup2<K, Option<V>>, ZWeight>> = Vec::new();

        for key in changed_keys {
            // Compute new percentile value for this key (discrete, no interpolation)
            let new_value = self.trees.get_mut(&key).and_then(|tree| {
                if tree.is_empty() {
                    None
                } else {
                    // PERCENTILE_DISC: return actual value from the set
                    tree.select_percentile_disc(self.percentile, self.ascending)
                        .cloned()
                }
            });
            let had_prev = self.prev_output.contains_key(&key);
            let prev_value = self.prev_output.get(&key).cloned().flatten();

            // Only emit if value changed (using had_prev to distinguish
            // "never emitted" from "emitted None")
            if new_value != prev_value {
                // Retract old output if we previously emitted something
                if had_prev {
                    tuples.push(Tup2(
                        Tup2(key.clone(), prev_value.clone()),
                        ZWeight::one().neg(),
                    ));
                }

                // Emit insertion of new value (or NULL if group became empty
                // and we had a previous value to retract)
                if new_value.is_some() || had_prev {
                    tuples.push(Tup2(Tup2(key.clone(), new_value.clone()), ZWeight::one()));
                }

                // Update prev_output
                self.prev_output.insert(key.clone(), new_value);
            }

            // Clean up empty trees but keep prev_output for NULL retraction tracking
            if let Some(tree) = self.trees.get(&key) {
                if tree.is_empty() {
                    self.trees.remove(&key);
                    self.tree_ids.remove(&key);
                }
            }
        }

        // Use TypedBatch::from_tuples to create the output
        TypedBatch::from_tuples((), tuples)
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

// =============================================================================
// Stream Extension Methods for PERCENTILE_CONT (requires Interpolate)
// =============================================================================

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone + Interpolate,
    <V as Archive>::Archived: Ord,
{
    /// Compute PERCENTILE_CONT incrementally with persistent state.
    ///
    /// Unlike the aggregate-based percentile, this operator maintains
    /// `OrderStatisticsZSet` state per key across steps for O(log n)
    /// incremental updates instead of O(n) per-step rescanning.
    ///
    /// # SQL Standard
    ///
    /// PERCENTILE_CONT only works with numeric floating-point types (F32, F64).
    /// For integer columns, the SQL compiler should cast to DOUBLE before calling.
    /// Returns an interpolated value that may not exist in the original data.
    ///
    /// # Arguments
    ///
    /// - `persistent_id`: Optional identifier for checkpoint/restore
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    ///
    /// # Example
    ///
    /// ```ignore
    /// let medians = indexed_zset.percentile_cont_stateful(
    ///     Some("median_op"),
    ///     0.5,
    ///     true,
    /// );
    /// ```
    #[track_caller]
    pub fn percentile_cont_stateful(
        &self,
        persistent_id: Option<&str>,
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        let operator = PercentileOperator::<K, V, ContMode>::new_cont(percentile, ascending);
        self.circuit()
            .add_unary_operator(operator, self)
            .set_persistent_id(persistent_id)
    }

    /// Compute median (50th percentile) incrementally with persistent state.
    ///
    /// This is a convenience method equivalent to
    /// `percentile_cont_stateful(persistent_id, 0.5, true)`.
    #[track_caller]
    pub fn median_stateful(
        &self,
        persistent_id: Option<&str>,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        self.percentile_cont_stateful(persistent_id, 0.5, true)
    }
}

// =============================================================================
// Stream Extension Methods for PERCENTILE_DISC (works with any ordered type)
// =============================================================================

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone, // No Interpolate bound - works with any ordered type
    <V as Archive>::Archived: Ord,
{
    /// Compute PERCENTILE_DISC incrementally with persistent state.
    ///
    /// Unlike the aggregate-based percentile, this operator maintains
    /// `OrderStatisticsZSet` state per key across steps for O(log n)
    /// incremental updates.
    ///
    /// # SQL Standard
    ///
    /// PERCENTILE_DISC works with any ordered type (numeric, string, date, etc.)
    /// and returns an actual value from the set without interpolation.
    ///
    /// # Arguments
    ///
    /// - `persistent_id`: Optional identifier for checkpoint/restore
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    #[track_caller]
    pub fn percentile_disc_stateful(
        &self,
        persistent_id: Option<&str>,
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        let operator = PercentileOperator::<K, V, DiscMode>::new_disc(percentile, ascending);
        self.circuit()
            .add_unary_operator(operator, self)
            .set_persistent_id(persistent_id)
    }
}

#[cfg(test)]
#[path = "percentile_op_tests.rs"]
mod tests;
