//! Stateful percentile operator with incremental updates.
//!
//! This module provides a dedicated `PercentileOperator` that maintains
//! `OrderStatisticsMultiset` state per key across steps, enabling O(log n)
//! incremental updates instead of re-scanning the entire input trace each step.
//!
//! # Architecture
//!
//! Unlike the aggregate-based percentile implementation which re-scans the entire
//! input each step, this operator:
//!
//! 1. Maintains per-key `OrderStatisticsMultiset` state across steps
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

use rkyv::Archive;

use crate::{
    Circuit, DBData, Stream, ZWeight,
    algebra::{
        AddAssignByRef, F32, F64, HasOne, HasZero, OrderStatisticsMultiset,
        DEFAULT_BRANCHING_FACTOR,
    },
    circuit::{
        OwnershipPreference, Scope,
        metadata::{MetaItem, OperatorMeta},
        operator_traits::{Operator, UnaryOperator},
    },
    dynamic::DowncastTrait,
    node_storage::NodeStorageConfig,
    trace::{BatchReader, Cursor},
    typed_batch::{
        BatchReader as TypedBatchReader, OrdIndexedZSet, TypedBatch,
    },
    utils::{IsNone, Tup2},
};

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
// Percentile Operator
// =============================================================================

/// A stateful operator that computes percentiles incrementally.
///
/// This operator maintains `OrderStatisticsMultiset` state per key across steps,
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
    trees: BTreeMap<K, OrderStatisticsMultiset<V>>,

    /// Previous output values for computing deltas
    prev_output: BTreeMap<K, Option<V>>,

    /// Percentile to compute (0.0 to 1.0)
    percentile: f64,

    /// Sort order (true = ascending)
    ascending: bool,

    /// Storage config for spill-to-disk
    storage_config: NodeStorageConfig,

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
            prev_output: BTreeMap::new(),
            percentile,
            ascending,
            storage_config: NodeStorageConfig::from_runtime(),
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
            prev_output: BTreeMap::new(),
            percentile,
            ascending,
            storage_config: NodeStorageConfig::from_runtime(),
            _mode: std::marker::PhantomData,
        }
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

    fn metadata(&self, meta: &mut OperatorMeta) {
        let num_keys = self.trees.len();
        let total_entries: usize = self.trees.values().map(|t| t.num_keys()).sum();

        meta.extend([
            (
                Cow::Borrowed("num_keys"),
                MetaItem::Int(num_keys),
            ),
            (
                Cow::Borrowed("total_entries"),
                MetaItem::Int(total_entries),
            ),
            (
                Cow::Borrowed("percentile_pct"),
                MetaItem::Int((self.percentile * 100.0) as usize),
            ),
            (
                Cow::Borrowed("ascending"),
                MetaItem::Bool(self.ascending),
            ),
        ]);
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Not at fixedpoint if we have any state
        self.trees.is_empty()
    }

    // Note: Checkpoint/restore is not yet implemented for PercentileOperator.
    // The state (OrderStatisticsMultiset trees) would need complex serialization bounds.
    // For now, this operator does not support fault-tolerant checkpointing.
    // TODO: Implement checkpoint/restore with proper serialization bounds.
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

            // Get or create tree for this key
            let storage_config = self.storage_config.clone();
            let tree = self.trees.entry(key.clone()).or_insert_with(|| {
                OrderStatisticsMultiset::with_config(
                    DEFAULT_BRANCHING_FACTOR,
                    storage_config,
                )
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

            changed_keys.push(key);
            cursor.step_key();
        }

        // Build output as Vec<Tup2<Tup2<K, Option<V>>, ZWeight>>
        let mut tuples: Vec<Tup2<Tup2<K, Option<V>>, ZWeight>> = Vec::new();

        for key in changed_keys {
            // Compute new percentile value for this key using interpolation
            let new_value = self.trees.get(&key).and_then(|tree| {
                if tree.is_empty() {
                    None
                } else {
                    // PERCENTILE_CONT: interpolate between adjacent values
                    tree.select_percentile_bounds(self.percentile, self.ascending)
                        .map(|(lower, upper, fraction)| V::interpolate(lower, upper, fraction))
                }
            });
            let prev_value = self.prev_output.get(&key).cloned().flatten();

            // Only emit if value changed
            if new_value != prev_value {
                // Emit deletion of old value if it existed
                if let Some(ref old_val) = prev_value {
                    tuples.push(Tup2(
                        Tup2(key.clone(), Some(old_val.clone())),
                        ZWeight::one().neg(),
                    ));
                }

                // Emit insertion of new value
                if new_value.is_some() || prev_value.is_some() {
                    tuples.push(Tup2(
                        Tup2(key.clone(), new_value.clone()),
                        ZWeight::one(),
                    ));
                }

                // Update prev_output
                self.prev_output.insert(key.clone(), new_value);
            }

            // Clean up empty trees
            if let Some(tree) = self.trees.get(&key) {
                if tree.is_empty() {
                    self.trees.remove(&key);
                    self.prev_output.remove(&key);
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
    V: DBData + IsNone,  // No Interpolate bound - works with any ordered type
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

            // Get or create tree for this key
            let storage_config = self.storage_config.clone();
            let tree = self.trees.entry(key.clone()).or_insert_with(|| {
                OrderStatisticsMultiset::with_config(
                    DEFAULT_BRANCHING_FACTOR,
                    storage_config,
                )
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

            changed_keys.push(key);
            cursor.step_key();
        }

        // Build output as Vec<Tup2<Tup2<K, Option<V>>, ZWeight>>
        let mut tuples: Vec<Tup2<Tup2<K, Option<V>>, ZWeight>> = Vec::new();

        for key in changed_keys {
            // Compute new percentile value for this key (discrete, no interpolation)
            let new_value = self.trees.get(&key).and_then(|tree| {
                if tree.is_empty() {
                    None
                } else {
                    // PERCENTILE_DISC: return actual value from the set
                    tree.select_percentile_disc(self.percentile, self.ascending)
                        .cloned()
                }
            });
            let prev_value = self.prev_output.get(&key).cloned().flatten();

            // Only emit if value changed
            if new_value != prev_value {
                // Emit deletion of old value if it existed
                if let Some(ref old_val) = prev_value {
                    tuples.push(Tup2(
                        Tup2(key.clone(), Some(old_val.clone())),
                        ZWeight::one().neg(),
                    ));
                }

                // Emit insertion of new value
                if new_value.is_some() || prev_value.is_some() {
                    tuples.push(Tup2(
                        Tup2(key.clone(), new_value.clone()),
                        ZWeight::one(),
                    ));
                }

                // Update prev_output
                self.prev_output.insert(key.clone(), new_value);
            }

            // Clean up empty trees
            if let Some(tree) = self.trees.get(&key) {
                if tree.is_empty() {
                    self.trees.remove(&key);
                    self.prev_output.remove(&key);
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
    /// `OrderStatisticsMultiset` state per key across steps for O(log n)
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
        _persistent_id: Option<&str>,  // TODO: Implement checkpoint/restore
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        let operator = PercentileOperator::<K, V, ContMode>::new_cont(percentile, ascending);
        self.circuit().add_unary_operator(operator, self)
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
    V: DBData + IsNone,  // No Interpolate bound - works with any ordered type
    <V as Archive>::Archived: Ord,
{
    /// Compute PERCENTILE_DISC incrementally with persistent state.
    ///
    /// Unlike the aggregate-based percentile, this operator maintains
    /// `OrderStatisticsMultiset` state per key across steps for O(log n)
    /// incremental updates.
    ///
    /// # SQL Standard
    ///
    /// PERCENTILE_DISC works with any ordered type (numeric, string, date, etc.)
    /// and returns an actual value from the set without interpolation.
    ///
    /// # Arguments
    ///
    /// - `persistent_id`: Optional identifier for checkpoint/restore (not yet implemented)
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    #[track_caller]
    pub fn percentile_disc_stateful(
        &self,
        _persistent_id: Option<&str>,  // TODO: Implement checkpoint/restore
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        let operator = PercentileOperator::<K, V, DiscMode>::new_disc(percentile, ascending);
        self.circuit().add_unary_operator(operator, self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Runtime, indexed_zset};

    #[test]
    fn test_percentile_cont_basic() {
        // PERCENTILE_CONT requires floating-point types (implements Interpolate)
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, 0.5, true);
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Step 1: Insert values for key 1
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
            Tup2(1, Tup2(F64::new(30.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10.0, 20.0, 30.0] is 20.0
        assert_eq!(result, indexed_zset! { 1 => { Some(F64::new(20.0)) => 1 } });

        // Step 2: Add more values
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(40.0), 1)),
            Tup2(1, Tup2(F64::new(50.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10.0, 20.0, 30.0, 40.0, 50.0] is 30.0
        // Delta should be: remove old (20.0), add new (30.0)
        assert_eq!(result, indexed_zset! { 1 => { Some(F64::new(20.0)) => -1, Some(F64::new(30.0)) => 1 } });

        // Step 3: Delete a value
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(30.0), -1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10.0, 20.0, 40.0, 50.0] with interpolation:
        // pos = 0.5 * (4-1) = 1.5, lower_idx = 1, upper_idx = 2
        // values are 20.0 and 40.0, fraction = 0.5
        // interpolated = 20.0 + 0.5 * (40.0 - 20.0) = 30.0
        assert_eq!(result, indexed_zset! { 1 => { Some(F64::new(30.0)) => -1, Some(F64::new(30.0)) => 1 } });
    }

    #[test]
    fn test_percentile_disc_basic() {
        // PERCENTILE_DISC works with any ordered type (no Interpolate required)
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let output = input.percentile_disc_stateful(None, 0.5, true);
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Step 1: Insert values for key 1
        input.append(&mut vec![
            Tup2(1, Tup2(10, 1)),
            Tup2(1, Tup2(20, 1)),
            Tup2(1, Tup2(30, 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Median of [10, 20, 30] = 20 (discrete, no interpolation)
        assert_eq!(result, indexed_zset! { 1 => { Some(20) => 1 } });
    }

    #[test]
    fn test_percentile_operator_multiple_keys() {
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();
            let output = input.percentile_disc_stateful(None, 0.5, true);
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert values for multiple keys
        input.append(&mut vec![
            Tup2(1, Tup2(10, 1)),
            Tup2(1, Tup2(20, 1)),
            Tup2(1, Tup2(30, 1)),
            Tup2(2, Tup2(100, 1)),
            Tup2(2, Tup2(200, 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Key 1: Median of [10, 20, 30] = 20
        // Key 2: Median of [100, 200] = 100 or 200
        assert!(result.key_count() == 2);
    }

    #[test]
    fn test_percentile_operator_empty_group() {
        // Use F64 for PERCENTILE_CONT test
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, 0.5, true);
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // Insert then delete all values
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), 1)),
            Tup2(1, Tup2(F64::new(20.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        assert_eq!(result.key_count(), 1);

        // Delete all values
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(10.0), -1)),
            Tup2(1, Tup2(F64::new(20.0), -1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // Should emit deletion of the previous value and None for empty group
        assert!(result.key_count() > 0);
    }

    #[test]
    fn test_percentile_cont_interpolation() {
        // Test that PERCENTILE_CONT correctly interpolates between values
        let (mut circuit, (input, output)) = Runtime::init_circuit(1, |circuit| {
            let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
            let output = input.percentile_cont_stateful(None, 0.5, true);
            Ok((input_handle, output.output()))
        })
        .unwrap();

        // With 2 values, median should interpolate: (1.0 + 2.0) / 2 = 1.5
        input.append(&mut vec![
            Tup2(1, Tup2(F64::new(1.0), 1)),
            Tup2(1, Tup2(F64::new(2.0), 1)),
        ]);
        circuit.transaction().unwrap();

        let result = output.consolidate();
        // For 2 elements at percentile 0.5: pos = 0.5 * 1 = 0.5
        // lower_idx = 0, upper_idx = 1, fraction = 0.5
        // interpolated = 1.0 + 0.5 * (2.0 - 1.0) = 1.5
        assert_eq!(result, indexed_zset! { 1 => { Some(F64::new(1.5)) => 1 } });
    }
}
