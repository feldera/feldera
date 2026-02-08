//! Stream methods for percentile aggregation.
//!
//! This module provides convenient extension methods for computing percentiles
//! on indexed Z-sets using a stateful incremental operator.
//!
//! The percentile methods use a dedicated `PercentileOperator` that maintains
//! `OrderStatisticsZSet` state per key across steps, enabling O(log n)
//! incremental updates per change instead of O(n) per-step rescanning.

use crate::{
    Circuit, DBData, Stream,
    operator::dynamic::percentile_op::{ContMode, DiscMode, Interpolate, PercentileOperator},
    storage::file::Deserializable,
    typed_batch::OrdIndexedZSet,
    utils::IsNone,
};
use rkyv::Archive;

// =============================================================================
// PERCENTILE_CONT methods (requires Interpolate - floating-point types only)
// =============================================================================

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DBData,
    <K as Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone + Interpolate,
    <V as Archive>::Archived: Ord,
{
    /// Compute one or more PERCENTILE_CONT values for each group with linear interpolation.
    ///
    /// Returns the linearly interpolated values at the specified percentile positions.
    /// NULL values in the input are automatically excluded from the calculation.
    ///
    /// This method automatically shards the input by key to ensure all values for
    /// a key are processed by the same worker in multi-threaded execution.
    ///
    /// # Arguments
    /// - `persistent_id`: Optional identifier for checkpoint/restore
    /// - `percentiles`: Slice of values between 0.0 and 1.0
    /// - `ascending`: If true, sorts values in ascending order
    /// - `build_output`: Closure converting `&[Option<V>]` → `O`
    #[track_caller]
    pub fn percentile_cont<O, F>(
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
        // Shard input by key to ensure all values for a key are on the same worker
        let sharded = self.shard();
        let operator = PercentileOperator::<K, V, O, ContMode, F>::new_cont(
            percentiles.to_vec(), ascending, build_output,
        );
        sharded
            .circuit()
            .add_unary_operator(operator, &sharded)
            .set_persistent_id(persistent_id)
    }

    /// Compute the median (50th percentile) for each group.
    #[track_caller]
    pub fn median(&self) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        self.percentile_cont(None, &[0.5], true, |results| results[0].clone())
    }
}

// =============================================================================
// PERCENTILE_DISC methods (works with any ordered type)
// =============================================================================

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DBData,
    <K as Deserializable>::ArchivedDeser: Ord,
    V: DBData + IsNone, // No Interpolate bound - works with any ordered type
    <V as Archive>::Archived: Ord,
{
    /// Compute one or more PERCENTILE_DISC values for each group.
    ///
    /// Returns actual values from the set (no interpolation).
    /// NULL values in the input are automatically excluded from the calculation.
    ///
    /// This method automatically shards the input by key to ensure all values for
    /// a key are processed by the same worker in multi-threaded execution.
    ///
    /// # Arguments
    /// - `persistent_id`: Optional identifier for checkpoint/restore
    /// - `percentiles`: Slice of values between 0.0 and 1.0
    /// - `ascending`: If true, sorts values in ascending order
    /// - `build_output`: Closure converting `&[Option<V>]` → `O`
    #[track_caller]
    pub fn percentile_disc<O, F>(
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
        // Shard input by key to ensure all values for a key are on the same worker
        let sharded = self.shard();
        let operator = PercentileOperator::<K, V, O, DiscMode, F>::new_disc(
            percentiles.to_vec(), ascending, build_output,
        );
        sharded
            .circuit()
            .add_unary_operator(operator, &sharded)
            .set_persistent_id(persistent_id)
    }
}
