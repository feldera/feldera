//! Stream methods for percentile aggregation.
//!
//! This module provides convenient extension methods for computing percentiles
//! on indexed Z-sets using a stateful incremental operator.
//!
//! # Implementation
//!
//! The percentile methods use a dedicated `PercentileOperator` that maintains
//! `OrderStatisticsMultiset` state per key across steps. This enables O(log n)
//! incremental updates per change instead of O(n) per-step rescanning.
//!
//! The operator:
//! - Maintains per-key order statistics trees across steps
//! - Applies delta changes incrementally
//! - Supports checkpoint/restore for fault tolerance
//! - Uses spill-to-disk for large trees via `NodeStorage`

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
    /// Compute PERCENTILE_CONT for each group with linear interpolation.
    ///
    /// Returns the linearly interpolated value at the specified percentile position.
    /// For example, PERCENTILE_CONT(0.5) over [1.0, 2.0] returns 1.5 (the interpolated median).
    ///
    /// NULL values in the input are automatically excluded from the calculation.
    ///
    /// This method uses a stateful operator that maintains `OrderStatisticsMultiset`
    /// state per key, enabling O(log n) incremental updates instead of O(n)
    /// per-step rescanning.
    ///
    /// # SQL Standard
    ///
    /// PERCENTILE_CONT only works with numeric types and always returns DOUBLE PRECISION.
    /// For integer columns, the SQL compiler should cast to DOUBLE before calling.
    /// This method only accepts floating-point types (F32, F64, Option<F32>, Option<F64>).
    ///
    /// **Note**: This operator automatically shards the input by key to ensure
    /// all values for a key are processed by the same worker in multi-threaded
    /// execution.
    ///
    /// # Arguments
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    ///
    /// # Example
    /// ```ignore
    /// // Compute the median (50th percentile) for each key
    /// let medians = indexed_zset.percentile_cont(None, 0.5, true);
    /// ```
    #[track_caller]
    pub fn percentile_cont(
        &self,
        persistent_id: Option<&str>,
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        // Shard input by key to ensure all values for a key are on the same worker
        let sharded = self.shard();
        let operator = PercentileOperator::<K, V, ContMode>::new_cont(percentile, ascending);
        sharded
            .circuit()
            .add_unary_operator(operator, &sharded)
            .set_persistent_id(persistent_id)
    }

    /// Compute the median (50th percentile) for each group.
    ///
    /// This is a convenience method equivalent to `percentile_cont(None, 0.5, true)`.
    ///
    /// # Example
    /// ```ignore
    /// let medians = indexed_zset.median();
    /// ```
    #[track_caller]
    pub fn median(&self) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        self.percentile_cont(None, 0.5, true)
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
    /// Compute PERCENTILE_DISC for each group.
    ///
    /// Returns an actual value from the set (no interpolation).
    /// PERCENTILE_DISC finds the first value whose cumulative distribution
    /// is greater than or equal to the specified percentile.
    ///
    /// NULL values in the input are automatically excluded from the calculation.
    ///
    /// This method uses a stateful operator that maintains `OrderStatisticsMultiset`
    /// state per key, enabling O(log n) incremental updates instead of O(n)
    /// per-step rescanning.
    ///
    /// # SQL Standard
    ///
    /// PERCENTILE_DISC works with any ordered type (numeric, string, date, timestamp, etc.)
    /// and returns an actual value from the set without interpolation.
    ///
    /// **Note**: This operator automatically shards the input by key to ensure
    /// all values for a key are processed by the same worker in multi-threaded
    /// execution.
    ///
    /// # Arguments
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    ///
    /// # Example
    /// ```ignore
    /// // Compute the discrete median for each key
    /// let medians = indexed_zset.percentile_disc(None, 0.5, true);
    /// ```
    #[track_caller]
    pub fn percentile_disc(
        &self,
        persistent_id: Option<&str>,
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        // Shard input by key to ensure all values for a key are on the same worker
        let sharded = self.shard();
        let operator = PercentileOperator::<K, V, DiscMode>::new_disc(percentile, ascending);
        sharded
            .circuit()
            .add_unary_operator(operator, &sharded)
            .set_persistent_id(persistent_id)
    }
}
