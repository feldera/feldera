//! Stream methods for percentile aggregation.
//!
//! This module provides convenient extension methods for computing percentiles
//! on indexed Z-sets using DBSP's incremental aggregation framework.

use crate::{
    Circuit, DBData, DynZWeight, Stream, ZWeight,
    circuit::WithClock,
    dynamic::DynData,
    operator::dynamic::aggregate::{
        DynAggregatorImpl, IncAggregateFactories, PercentileCont, PercentileDisc,
    },
    storage::file::Deserializable,
    typed_batch::OrdIndexedZSet,
};

impl<C, K, V> Stream<C, OrdIndexedZSet<K, V>>
where
    C: Circuit,
    K: DBData,
    <K as Deserializable>::ArchivedDeser: Ord,
    V: DBData,
{
    /// Compute PERCENTILE_CONT for each group.
    ///
    /// Returns the interpolated value at the specified percentile position.
    /// For the generic implementation, this returns the discrete value at
    /// the percentile position (no numeric interpolation). Use the sqllib
    /// functions for proper numeric interpolation that handles type-specific
    /// behavior (e.g., integers returning f64).
    ///
    /// # Arguments
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    ///
    /// # Example
    /// ```ignore
    /// // Compute the median (50th percentile) for each key
    /// let medians = indexed_zset.percentile_cont(0.5, true);
    /// ```
    #[track_caller]
    pub fn percentile_cont(
        &self,
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        let factories = IncAggregateFactories::new::<K, V, ZWeight, Option<V>>();
        let aggregator = PercentileCont::<V>::new(percentile, ascending);
        let dyn_aggregator = DynAggregatorImpl::<
            DynData,
            V,
            <C as WithClock>::Time,
            DynZWeight,
            ZWeight,
            _,
            DynData,
            DynData,
        >::new(aggregator);

        self.inner()
            .dyn_aggregate(None, &factories, &dyn_aggregator)
            .typed()
    }

    /// Compute PERCENTILE_DISC for each group.
    ///
    /// Returns an actual value from the set (no interpolation).
    /// PERCENTILE_DISC finds the first value whose cumulative distribution
    /// is greater than or equal to the specified percentile.
    ///
    /// # Arguments
    /// - `percentile`: Value between 0.0 and 1.0 specifying the percentile
    /// - `ascending`: If true, sorts values in ascending order
    ///
    /// # Example
    /// ```ignore
    /// // Compute the discrete median for each key
    /// let medians = indexed_zset.percentile_disc(0.5, true);
    /// ```
    #[track_caller]
    pub fn percentile_disc(
        &self,
        percentile: f64,
        ascending: bool,
    ) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        let factories = IncAggregateFactories::new::<K, V, ZWeight, Option<V>>();
        let aggregator = PercentileDisc::<V>::new(percentile, ascending);
        let dyn_aggregator = DynAggregatorImpl::<
            DynData,
            V,
            <C as WithClock>::Time,
            DynZWeight,
            ZWeight,
            _,
            DynData,
            DynData,
        >::new(aggregator);

        self.inner()
            .dyn_aggregate(None, &factories, &dyn_aggregator)
            .typed()
    }

    /// Compute the median (50th percentile) for each group.
    ///
    /// This is a convenience method equivalent to `percentile_cont(0.5, true)`.
    ///
    /// # Example
    /// ```ignore
    /// let medians = indexed_zset.median();
    /// ```
    #[track_caller]
    pub fn median(&self) -> Stream<C, OrdIndexedZSet<K, Option<V>>> {
        self.percentile_cont(0.5, true)
    }
}
