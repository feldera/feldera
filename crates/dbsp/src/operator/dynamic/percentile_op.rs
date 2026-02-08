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
//! # Multi-percentile support
//!
//! A single operator can compute multiple percentiles from the same tree,
//! avoiding redundant tree maintenance when the same WITHIN GROUP clause
//! is shared across multiple PERCENTILE_CONT or PERCENTILE_DISC calls.
//!
//! # Usage
//!
//! ```ignore
//! let percentiles = input_stream.percentile_cont(
//!     Some("my_percentile_op"),
//!     &[0.25, 0.5, 0.75],
//!     true,  // ascending
//!     |results| Tup3(results[0].clone(), results[1].clone(), results[2].clone()),
//! );
//! ```

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Neg;
use std::sync::Arc;

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;

use crate::{
    Circuit, DBData, Error, Runtime, Stream, ZWeight,
    algebra::{
        AddAssignByRef, DEFAULT_BRANCHING_FACTOR, F32, F64, HasOne, HasZero,
        OrderStatisticsZSet,
    },
    circuit::{
        GlobalNodeId, OwnershipPreference, Scope,
        metadata::{
            BatchSizeStats, MetaItem, OperatorMeta,
            INPUT_BATCHES_LABEL, OUTPUT_BATCHES_LABEL, NUM_ENTRIES_LABEL,
            USED_BYTES_LABEL, NUM_ALLOCATIONS_LABEL, SHARED_BYTES_LABEL,
        },
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
#[derive(Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive(check_bytes)]
struct CommittedPercentileOperator<K, O>
where
    K: Archive,
    O: Archive,
{
    /// Per-key tree IDs (NodeStorage writes its own metadata files)
    tree_ids: Vec<(K, u64)>,
    /// Previous output values for delta computation
    prev_output: Vec<(K, O)>,
    /// Configuration — percentile values stored as u64 bits for rkyv compatibility
    percentiles: Vec<u64>,
    ascending: bool,
    /// Branching factor for tree reconstruction
    branching_factor: u32,
    /// Next tree ID counter (`next_tree_id` is saved/restored to prevent ID collisions)
    next_tree_id: u64,
}

// =============================================================================
// Percentile Operator
// =============================================================================

/// A stateful operator that computes one or more percentiles incrementally.
///
/// This operator maintains `OrderStatisticsZSet` state per key across steps,
/// enabling O(log n) incremental updates instead of O(n) per-step rescanning.
/// When multiple percentiles share the same ORDER BY column and direction,
/// they can be computed from a single shared tree, eliminating redundant
/// tree maintenance.
///
/// # Type Parameters
///
/// - `K`: Key type for grouping
/// - `V`: Value type being aggregated (stored in the tree)
/// - `O`: Output value type (e.g., `Option<V>` for single, `Tup3<Option<V>, ...>` for multi)
/// - `Mode`: Either `ContMode` (interpolated) or `DiscMode` (discrete)
/// - `F`: Closure type `Fn(&[Option<V>]) -> O` that builds the output from computed percentile results
pub struct PercentileOperator<K, V, O, Mode, F>
where
    K: DBData,
    V: DBData,
    O: DBData,
{
    /// Per-key order statistics trees
    trees: BTreeMap<K, OrderStatisticsZSet<V>>,

    /// Per-key tree IDs for checkpoint file naming.
    tree_ids: BTreeMap<K, u64>,

    /// Previous output values for computing deltas
    prev_output: BTreeMap<K, O>,

    /// Percentile values to compute (each 0.0 to 1.0)
    percentiles: Vec<f64>,

    /// Sort order (true = ascending)
    ascending: bool,

    /// Closure that builds the output value from a slice of computed percentile results.
    /// Each element in the slice corresponds to one percentile value.
    build_output: F,

    /// Storage config for spill-to-disk
    storage_config: NodeStorageConfig,

    /// Counter for generating unique segment path prefixes per tree.
    next_tree_id: u64,

    /// Global node ID for checkpoint file naming
    global_id: GlobalNodeId,

    /// Input batch size statistics for profiling
    input_batch_stats: BatchSizeStats,

    /// Output batch size statistics for profiling
    output_batch_stats: BatchSizeStats,

    /// Marker for CONT vs DISC mode (zero-sized, compile-time only)
    _mode: std::marker::PhantomData<Mode>,
}

impl<K, V, O, Mode, F> SizeOf for PercentileOperator<K, V, O, Mode, F>
where
    K: DBData,
    V: DBData,
    O: DBData,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        self.trees.size_of_children(context);
        self.tree_ids.size_of_children(context);
        self.prev_output.size_of_children(context);
        self.percentiles.size_of_children(context);
    }
}

impl<K, V, O, F> PercentileOperator<K, V, O, ContMode, F>
where
    K: DBData,
    V: DBData,
    O: DBData,
    F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
{
    /// Create a new PERCENTILE_CONT operator computing one or more percentiles.
    pub fn new_cont(percentiles: Vec<f64>, ascending: bool, build_output: F) -> Self {
        Self {
            trees: BTreeMap::new(),
            tree_ids: BTreeMap::new(),
            prev_output: BTreeMap::new(),
            percentiles,
            ascending,
            build_output,
            storage_config: NodeStorageConfig::from_runtime(),
            next_tree_id: 0,
            global_id: GlobalNodeId::root(),
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: BatchSizeStats::new(),
            _mode: std::marker::PhantomData,
        }
    }
}

impl<K, V, O, F> PercentileOperator<K, V, O, DiscMode, F>
where
    K: DBData,
    V: DBData,
    O: DBData,
    F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
{
    /// Create a new PERCENTILE_DISC operator computing one or more percentiles.
    pub fn new_disc(percentiles: Vec<f64>, ascending: bool, build_output: F) -> Self {
        Self {
            trees: BTreeMap::new(),
            tree_ids: BTreeMap::new(),
            prev_output: BTreeMap::new(),
            percentiles,
            ascending,
            build_output,
            storage_config: NodeStorageConfig::from_runtime(),
            next_tree_id: 0,
            global_id: GlobalNodeId::root(),
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: BatchSizeStats::new(),
            _mode: std::marker::PhantomData,
        }
    }
}

impl<K, V, O, Mode, F> PercentileOperator<K, V, O, Mode, F>
where
    K: DBData,
    V: DBData,
    O: DBData,
    <K as Archive>::Archived: Ord,
    <O as Archive>::Archived: Ord,
{
    /// Generate the checkpoint file path for this operator.
    fn checkpoint_file(base: &StoragePath, persistent_id: &str) -> StoragePath {
        StoragePath::from(format!("{base}/percentile-{persistent_id}.dat"))
    }
}

impl<K, V, O, Mode, F> Operator for PercentileOperator<K, V, O, Mode, F>
where
    K: DBData,
    V: DBData,
    O: DBData,
    Mode: Send + 'static,
    F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
    <K as Archive>::Archived: Ord,
    <V as Archive>::Archived: Ord,
    <O as Archive>::Archived: Ord,
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
        let storage_size: u64 = self.trees.values()
            .flat_map(|t| t.storage().segments())
            .map(|seg| seg.size_bytes)
            .sum();
        let bytes = self.size_of();

        meta.extend(metadata! {
            NUM_ENTRIES_LABEL => MetaItem::Count(total_entries),
            INPUT_BATCHES_LABEL => self.input_batch_stats.metadata(),
            OUTPUT_BATCHES_LABEL => self.output_batch_stats.metadata(),
            USED_BYTES_LABEL => MetaItem::bytes(bytes.used_bytes()),
            NUM_ALLOCATIONS_LABEL => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_BYTES_LABEL => MetaItem::bytes(bytes.shared_bytes()),
            "storage size" => MetaItem::bytes(storage_size as usize),
            "num keys" => MetaItem::Int(num_keys),
            "evicted leaves" => MetaItem::Count(total_evicted),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Not at fixedpoint if we have any state
        self.trees.is_empty()
    }

    fn clock_end(&mut self, _scope: Scope) {
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

        for (key, tree) in self.trees.iter_mut() {
            let tree_id = self.tree_ids[key];
            let tree_pid = format!("{}_t{}", persistent_id, tree_id);
            tree.save(base, &tree_pid, files)?;
        }

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
            percentiles: self.percentiles.iter().map(|p| p.to_bits()).collect(),
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

        let path = Self::checkpoint_file(base, persistent_id);
        let backend = self
            .storage_config
            .storage_backend
            .clone()
            .or_else(|| Runtime::storage_backend().ok())
            .expect("No storage backend available for restore");
        let content = backend.read(&path)?;

        let archived =
            unsafe { rkyv::archived_root::<CommittedPercentileOperator<K, O>>(&content) };

        let mut deserializer = Deserializer::new(0);

        let branching_factor = archived.branching_factor as usize;

        self.next_tree_id = archived.next_tree_id;

        // Restore each tree
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

            let value: O =
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
        self.percentiles = archived.percentiles.iter().map(|p| f64::from_bits((*p).into())).collect();
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
// Helper: process input delta (shared between ContMode and DiscMode)
// =============================================================================

/// Process the input delta batch, inserting/deleting values into per-key trees.
/// Returns the list of keys that had changes.
fn process_delta<K, V>(
    delta: &OrdIndexedZSet<K, V>,
    trees: &mut BTreeMap<K, OrderStatisticsZSet<V>>,
    tree_ids: &mut BTreeMap<K, u64>,
    next_tree_id: &mut u64,
    storage_config: &NodeStorageConfig,
) -> Vec<K>
where
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone,
    <V as Archive>::Archived: Ord,
{
    let mut changed_keys: Vec<K> = Vec::new();

    let inner = delta.inner();
    let mut cursor = inner.cursor();

    while cursor.key_valid() {
        let key: K = unsafe { cursor.key().downcast::<K>().clone() };

        let nti = &mut *next_tree_id;
        let tids = &mut *tree_ids;
        let sc = &*storage_config;
        let tree = trees.entry(key.clone()).or_insert_with(|| {
            let tree_id = *nti;
            let mut config = sc.clone();
            config.segment_path_prefix = format!("t{}_", tree_id);
            *nti += 1;
            tids.insert(key.clone(), tree_id);
            OrderStatisticsZSet::with_config(DEFAULT_BRANCHING_FACTOR, config)
        });

        while cursor.val_valid() {
            let value: V = unsafe { cursor.val().downcast::<V>().clone() };

            if value.is_none() {
                cursor.step_val();
                continue;
            }

            let mut weight: ZWeight = HasZero::zero();
            cursor.map_times(&mut |_, w| {
                weight.add_assign_by_ref(unsafe { w.downcast() });
            });

            if !weight.is_zero() {
                tree.insert(value, weight);
            }

            cursor.step_val();
        }

        if tree.should_flush() {
            let _ = tree.flush_and_evict();
        }

        changed_keys.push(key);
        cursor.step_key();
    }

    changed_keys
}

/// Emit output deltas by comparing new output with previous output for each changed key.
fn emit_deltas<K, O>(
    changed_keys: Vec<K>,
    new_outputs: Vec<Option<O>>,
    prev_output: &mut BTreeMap<K, O>,
    trees: &mut BTreeMap<K, OrderStatisticsZSet<impl DBData>>,
    tree_ids: &mut BTreeMap<K, u64>,
) -> Vec<Tup2<Tup2<K, O>, ZWeight>>
where
    K: DBData,
    O: DBData,
{
    let mut tuples: Vec<Tup2<Tup2<K, O>, ZWeight>> = Vec::new();

    for (key, new_output) in changed_keys.into_iter().zip(new_outputs) {
        let had_prev = prev_output.contains_key(&key);
        let prev_val = prev_output.get(&key).cloned();

        let changed = match (&new_output, &prev_val) {
            (Some(new), Some(old)) => new != old,
            (None, None) if !had_prev => false,
            _ => true,
        };

        if changed {
            // Retract old output if we previously emitted something
            if let Some(old) = &prev_val {
                tuples.push(Tup2(
                    Tup2(key.clone(), old.clone()),
                    ZWeight::one().neg(),
                ));
            }

            // Emit insertion of new value
            if let Some(new) = &new_output {
                tuples.push(Tup2(Tup2(key.clone(), new.clone()), ZWeight::one()));
            }

            // Update prev_output
            match new_output {
                Some(val) => { prev_output.insert(key.clone(), val); }
                None => { prev_output.remove(&key); }
            }
        }

        // Clean up empty trees but keep prev_output for NULL retraction tracking
        if let Some(tree) = trees.get(&key) {
            if tree.is_empty() {
                trees.remove(&key);
                tree_ids.remove(&key);
            }
        }
    }

    tuples
}

// =============================================================================
// UnaryOperator implementation for PERCENTILE_CONT (requires Interpolate)
// =============================================================================

impl<K, V, O, F> UnaryOperator<OrdIndexedZSet<K, V>, OrdIndexedZSet<K, O>>
    for PercentileOperator<K, V, O, ContMode, F>
where
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone + Interpolate,
    <V as Archive>::Archived: Ord,
    O: DBData,
    <O as Archive>::Archived: Ord,
    F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
{
    async fn eval(&mut self, delta: &OrdIndexedZSet<K, V>) -> OrdIndexedZSet<K, O> {
        self.input_batch_stats.add_batch(delta.inner().len());

        let changed_keys = process_delta(
            delta,
            &mut self.trees,
            &mut self.tree_ids,
            &mut self.next_tree_id,
            &self.storage_config,
        );

        // Compute new outputs for each changed key
        let num_percentiles = self.percentiles.len();
        let new_outputs: Vec<Option<O>> = changed_keys.iter().map(|key| {
            let tree_empty = self.trees.get(key).map_or(true, |t| t.is_empty());
            if tree_empty {
                // Tree is empty: emit a NULL output if we previously emitted something,
                // so that downstream sees the retraction of old value + insertion of NULL row.
                if self.prev_output.contains_key(key) {
                    let null_results: Vec<Option<V>> = vec![None; num_percentiles];
                    Some((self.build_output)(&null_results))
                } else {
                    None
                }
            } else {
                let tree = self.trees.get_mut(key).unwrap();
                let results: Vec<Option<V>> = self.percentiles.iter().map(|p| {
                    tree.select_percentile_bounds(*p, self.ascending)
                        .map(|(lower, upper, fraction)| V::interpolate(&lower, &upper, fraction))
                }).collect();
                Some((self.build_output)(&results))
            }
        }).collect();

        let tuples = emit_deltas(
            changed_keys,
            new_outputs,
            &mut self.prev_output,
            &mut self.trees,
            &mut self.tree_ids,
        );

        let result: OrdIndexedZSet<K, O> = TypedBatch::from_tuples((), tuples);
        self.output_batch_stats.add_batch(result.inner().len());
        result
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

// =============================================================================
// UnaryOperator implementation for PERCENTILE_DISC (no Interpolate required)
// =============================================================================

impl<K, V, O, F> UnaryOperator<OrdIndexedZSet<K, V>, OrdIndexedZSet<K, O>>
    for PercentileOperator<K, V, O, DiscMode, F>
where
    K: DBData,
    <K as crate::storage::file::Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone,
    <V as Archive>::Archived: Ord,
    O: DBData,
    <O as Archive>::Archived: Ord,
    F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
{
    async fn eval(&mut self, delta: &OrdIndexedZSet<K, V>) -> OrdIndexedZSet<K, O> {
        self.input_batch_stats.add_batch(delta.inner().len());

        let changed_keys = process_delta(
            delta,
            &mut self.trees,
            &mut self.tree_ids,
            &mut self.next_tree_id,
            &self.storage_config,
        );

        // Compute new outputs for each changed key
        let num_percentiles = self.percentiles.len();
        let new_outputs: Vec<Option<O>> = changed_keys.iter().map(|key| {
            let tree_empty = self.trees.get(key).map_or(true, |t| t.is_empty());
            if tree_empty {
                // Tree is empty: emit a NULL output if we previously emitted something,
                // so that downstream sees the retraction of old value + insertion of NULL row.
                if self.prev_output.contains_key(key) {
                    let null_results: Vec<Option<V>> = vec![None; num_percentiles];
                    Some((self.build_output)(&null_results))
                } else {
                    None
                }
            } else {
                let tree = self.trees.get_mut(key).unwrap();
                let results: Vec<Option<V>> = self.percentiles.iter().map(|p| {
                    tree.select_percentile_disc(*p, self.ascending).cloned()
                }).collect();
                Some((self.build_output)(&results))
            }
        }).collect();

        let tuples = emit_deltas(
            changed_keys,
            new_outputs,
            &mut self.prev_output,
            &mut self.trees,
            &mut self.tree_ids,
        );

        let result: OrdIndexedZSet<K, O> = TypedBatch::from_tuples((), tuples);
        self.output_batch_stats.add_batch(result.inner().len());
        result
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
    /// Compute one or more PERCENTILE_CONT values incrementally with persistent state.
    ///
    /// # Arguments
    ///
    /// - `persistent_id`: Optional identifier for checkpoint/restore
    /// - `percentiles`: Slice of values between 0.0 and 1.0
    /// - `ascending`: If true, sorts values in ascending order
    /// - `build_output`: Closure converting `&[Option<V>]` → `O`
    #[track_caller]
    pub fn percentile_cont_stateful<O, F>(
        &self,
        persistent_id: Option<&str>,
        percentiles: &[f64],
        ascending: bool,
        build_output: F,
    ) -> Stream<C, OrdIndexedZSet<K, O>>
    where
        O: DBData,
        <O as Archive>::Archived: Ord,
        F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
    {
        let operator = PercentileOperator::<K, V, O, ContMode, F>::new_cont(
            percentiles.to_vec(), ascending, build_output,
        );
        self.circuit()
            .add_unary_operator(operator, self)
            .set_persistent_id(persistent_id)
    }

    /// Compute median (50th percentile) incrementally with persistent state.
    #[track_caller]
    pub fn median_stateful(
        &self,
        persistent_id: Option<&str>,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        self.percentile_cont_stateful(persistent_id, &[0.5], true, |results| results[0].clone())
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
    V: DBData + IsNone,
    <V as Archive>::Archived: Ord,
{
    /// Compute one or more PERCENTILE_DISC values incrementally with persistent state.
    ///
    /// # Arguments
    ///
    /// - `persistent_id`: Optional identifier for checkpoint/restore
    /// - `percentiles`: Slice of values between 0.0 and 1.0
    /// - `ascending`: If true, sorts values in ascending order
    /// - `build_output`: Closure converting `&[Option<V>]` → `O`
    #[track_caller]
    pub fn percentile_disc_stateful<O, F>(
        &self,
        persistent_id: Option<&str>,
        percentiles: &[f64],
        ascending: bool,
        build_output: F,
    ) -> Stream<C, OrdIndexedZSet<K, O>>
    where
        O: DBData,
        <O as Archive>::Archived: Ord,
        F: Fn(&[Option<V>]) -> O + Clone + Send + 'static,
    {
        let operator = PercentileOperator::<K, V, O, DiscMode, F>::new_disc(
            percentiles.to_vec(), ascending, build_output,
        );
        self.circuit()
            .add_unary_operator(operator, self)
            .set_persistent_id(persistent_id)
    }
}

#[cfg(test)]
#[path = "percentile_op_tests.rs"]
mod tests;
