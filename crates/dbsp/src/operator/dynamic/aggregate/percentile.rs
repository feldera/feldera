// Percentile aggregators for DBSP
//
// This module provides PERCENTILE_CONT and PERCENTILE_DISC aggregators
// that follow SQL standard semantics.
//
// The implementation uses an OrderStatisticsMultiset - an augmented B+ tree
// that supports:
// - O(log n) insertion with positive/negative weights (for incremental computation)
// - O(log n) deletion via negative weights
// - O(log n) k-th element selection via subtree weight sums
// - Proper multiset semantics (duplicate values allowed with counts)

use std::marker::PhantomData;

use crate::{
    DBData, Timestamp,
    algebra::{AddAssignByRef, HasZero, OrderStatisticsMultiset, Semigroup, ZWeight},
    dynamic::{DataTrait, DynUnit, Erase, WeightTrait},
    operator::Aggregator,
    trace::Cursor,
    utils::Tup2,
};

/// Semigroup implementation for OrderStatisticsMultiset.
#[derive(Clone, Debug, Default)]
pub struct OrderStatisticsMultisetSemigroup<T>(PhantomData<T>);

impl<T> Semigroup<OrderStatisticsMultiset<T>> for OrderStatisticsMultisetSemigroup<T>
where
    T: DBData,
{
    fn combine(
        left: &OrderStatisticsMultiset<T>,
        right: &OrderStatisticsMultiset<T>,
    ) -> OrderStatisticsMultiset<T> {
        OrderStatisticsMultiset::merged(left, right)
    }
}

/// Semigroup for SQL PERCENTILE aggregates.
///
/// Combines `(Option<P>, OrderStatisticsMultiset<V>)` tuples where:
/// - P is the percentile value (same across all groups, first Some wins)
/// - V is the data type being aggregated
///
/// This semigroup is used by the SQL compiler for percentile computations.
#[derive(Clone, Debug, Default)]
pub struct PercentileSemigroup<T>(PhantomData<T>);

impl<P, V> Semigroup<Tup2<Option<P>, OrderStatisticsMultiset<V>>>
    for PercentileSemigroup<Tup2<Option<P>, OrderStatisticsMultiset<V>>>
where
    P: Clone,
    V: DBData,
{
    fn combine(
        left: &Tup2<Option<P>, OrderStatisticsMultiset<V>>,
        right: &Tup2<Option<P>, OrderStatisticsMultiset<V>>,
    ) -> Tup2<Option<P>, OrderStatisticsMultiset<V>> {
        // Take the first Some percentile value (they should all be the same within a group)
        let percentile = match (&left.0, &right.0) {
            (Some(p), _) => Some(p.clone()),
            (None, Some(p)) => Some(p.clone()),
            (None, None) => None,
        };

        // Merge the trees
        let tree = OrderStatisticsMultiset::merged(&left.1, &right.1);

        Tup2::new(percentile, tree)
    }
}

/// An [aggregator](`crate::operator::Aggregator`) that computes PERCENTILE_CONT.
///
/// PERCENTILE_CONT returns a value interpolated between two adjacent values
/// in the ordered set. The result type is always f64 for numeric types.
///
/// # Type Parameters
/// - `V`: The value type being aggregated
#[derive(Clone)]
pub struct PercentileCont<V> {
    /// The percentile to compute (0.0 to 1.0)
    percentile: f64,
    /// Whether to sort in ascending order
    ascending: bool,
    phantom: PhantomData<V>,
}

impl<V> PercentileCont<V> {
    /// Create a new PERCENTILE_CONT aggregator.
    ///
    /// # Arguments
    /// - `percentile`: The percentile to compute (0.0 to 1.0)
    /// - `ascending`: Sort order (true = ASC, false = DESC)
    pub fn new(percentile: f64, ascending: bool) -> Self {
        Self {
            percentile,
            ascending,
            phantom: PhantomData,
        }
    }
}

impl<V, T> Aggregator<V, T, ZWeight> for PercentileCont<V>
where
    V: DBData,
    T: Timestamp,
{
    type Accumulator = OrderStatisticsMultiset<V>;
    type Semigroup = OrderStatisticsMultisetSemigroup<V>;
    type Output = Option<V>;

    fn aggregate<VTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<VTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Accumulator>
    where
        VTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        V: Erase<VTrait>,
        ZWeight: Erase<RTrait>,
    {
        let mut tree = OrderStatisticsMultiset::new();
        let mut non_empty = false;

        while cursor.key_valid() {
            let mut weight: ZWeight = HasZero::zero();
            cursor.map_times(&mut |_, w| {
                weight.add_assign_by_ref(unsafe { w.downcast() });
            });

            if !weight.is_zero() {
                non_empty = true;
                let value: &V = unsafe { cursor.key().downcast() };
                tree.insert(value.clone(), weight);
            }

            cursor.step_key();
        }

        non_empty.then_some(tree)
    }

    fn finalize(&self, tree: Self::Accumulator) -> Self::Output {
        // For generic types, we just return the discrete value at the percentile position
        // Interpolation is only meaningful for numeric types and must be handled
        // by a post-processing step or specialized implementation
        let (lower, _upper, _fraction) =
            tree.select_percentile_bounds(self.percentile, self.ascending)?;
        Some(lower.clone())
    }
}

/// An [aggregator](`crate::operator::Aggregator`) that computes PERCENTILE_DISC.
///
/// PERCENTILE_DISC returns an actual value from the ordered set (no interpolation).
#[derive(Clone)]
pub struct PercentileDisc<V> {
    /// The percentile to compute (0.0 to 1.0)
    percentile: f64,
    /// Whether to sort in ascending order
    ascending: bool,
    phantom: PhantomData<V>,
}

impl<V> PercentileDisc<V> {
    /// Create a new PERCENTILE_DISC aggregator.
    pub fn new(percentile: f64, ascending: bool) -> Self {
        Self {
            percentile,
            ascending,
            phantom: PhantomData,
        }
    }
}

impl<V, T> Aggregator<V, T, ZWeight> for PercentileDisc<V>
where
    V: DBData,
    T: Timestamp,
{
    type Accumulator = OrderStatisticsMultiset<V>;
    type Semigroup = OrderStatisticsMultisetSemigroup<V>;
    type Output = Option<V>;

    fn aggregate<VTrait, RTrait>(
        &self,
        cursor: &mut dyn Cursor<VTrait, DynUnit, T, RTrait>,
    ) -> Option<Self::Accumulator>
    where
        VTrait: DataTrait + ?Sized,
        RTrait: WeightTrait + ?Sized,
        V: Erase<VTrait>,
        ZWeight: Erase<RTrait>,
    {
        let mut tree = OrderStatisticsMultiset::new();
        let mut non_empty = false;

        while cursor.key_valid() {
            let mut weight: ZWeight = HasZero::zero();
            cursor.map_times(&mut |_, w| {
                weight.add_assign_by_ref(unsafe { w.downcast() });
            });

            if !weight.is_zero() {
                non_empty = true;
                let value: &V = unsafe { cursor.key().downcast() };
                tree.insert(value.clone(), weight);
            }

            cursor.step_key();
        }

        non_empty.then_some(tree)
    }

    fn finalize(&self, tree: Self::Accumulator) -> Self::Output {
        tree.select_percentile_disc(self.percentile, self.ascending)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        tree.insert(10, 1);
        tree.insert(20, 1);
        tree.insert(30, 1);
        tree.insert(40, 1);
        tree.insert(50, 1);

        assert_eq!(tree.total_count(), 5);

        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(1, true), Some(&20));
        assert_eq!(tree.select_kth(2, true), Some(&30));
        assert_eq!(tree.select_kth(3, true), Some(&40));
        assert_eq!(tree.select_kth(4, true), Some(&50));
        assert_eq!(tree.select_kth(5, true), None);
    }

    #[test]
    fn test_duplicates() {
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        tree.insert(10, 3);
        tree.insert(20, 2);

        assert_eq!(tree.total_count(), 5);

        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(1, true), Some(&10));
        assert_eq!(tree.select_kth(2, true), Some(&10));
        assert_eq!(tree.select_kth(3, true), Some(&20));
        assert_eq!(tree.select_kth(4, true), Some(&20));
    }

    #[test]
    fn test_deletion() {
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        tree.insert(10, 1);
        tree.insert(20, 1);
        tree.insert(30, 1);

        assert_eq!(tree.total_count(), 3);

        tree.insert(20, -1);

        assert_eq!(tree.total_count(), 2);
        assert_eq!(tree.select_kth(0, true), Some(&10));
        assert_eq!(tree.select_kth(1, true), Some(&30));
    }

    #[test]
    fn test_percentile_cont_bounds() {
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        for i in 1..=5 {
            tree.insert(i, 1);
        }

        let (lower, upper, frac) = tree.select_percentile_bounds(0.5, true).unwrap();
        assert_eq!(*lower, 3);
        assert_eq!(*upper, 3);
        assert_eq!(frac, 0.0);

        let (lower, upper, frac) = tree.select_percentile_bounds(0.25, true).unwrap();
        assert_eq!(*lower, 2);
        assert_eq!(*upper, 2);
        assert_eq!(frac, 0.0);
    }

    #[test]
    fn test_percentile_disc() {
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        for i in 1..=5 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&3));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&5));
    }

    #[test]
    fn test_merge() {
        let mut tree1: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        tree1.insert(1, 1);
        tree1.insert(3, 1);
        tree1.insert(5, 1);

        let mut tree2: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();
        tree2.insert(2, 1);
        tree2.insert(4, 1);

        let merged = OrderStatisticsMultiset::merged(&tree1, &tree2);
        assert_eq!(merged.total_count(), 5);
        assert_eq!(merged.select_kth(0, true), Some(&1));
        assert_eq!(merged.select_kth(1, true), Some(&2));
        assert_eq!(merged.select_kth(2, true), Some(&3));
        assert_eq!(merged.select_kth(3, true), Some(&4));
        assert_eq!(merged.select_kth(4, true), Some(&5));
    }

    #[test]
    fn test_negative_numbers() {
        // Test that negative numbers are correctly ordered
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        // Insert values in the order they appear in the failing test
        tree.insert(-10, 1);
        tree.insert(0, 1);
        tree.insert(10, 1);
        tree.insert(20, 1);
        tree.insert(30, 1);
        tree.insert(100, 1);
        tree.insert(200, 1);

        assert_eq!(tree.total_count(), 7);

        // Verify ordering: -10 should be first, 200 should be last
        assert_eq!(tree.select_kth(0, true), Some(&-10));
        assert_eq!(tree.select_kth(1, true), Some(&0));
        assert_eq!(tree.select_kth(2, true), Some(&10));
        assert_eq!(tree.select_kth(6, true), Some(&200));

        // Test extreme percentiles
        let (lower, upper, frac) = tree.select_percentile_bounds(0.0, true).unwrap();
        assert_eq!(*lower, -10);
        assert_eq!(*upper, -10);
        assert_eq!(frac, 0.0);

        let (lower, upper, frac) = tree.select_percentile_bounds(1.0, true).unwrap();
        assert_eq!(*lower, 200);
        assert_eq!(*upper, 200);
        assert_eq!(frac, 0.0);

        // Test discrete percentiles at extremes
        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&-10));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&200));
    }

    #[test]
    fn test_f64_negative_numbers() {
        use crate::algebra::F64;

        // Test with F64 values (the actual type used in SQL DOUBLE)
        let mut tree: OrderStatisticsMultiset<F64> = OrderStatisticsMultiset::new();

        tree.insert(F64::new(-10.0), 1);
        tree.insert(F64::new(0.0), 1);
        tree.insert(F64::new(10.0), 1);
        tree.insert(F64::new(20.0), 1);
        tree.insert(F64::new(30.0), 1);
        tree.insert(F64::new(100.0), 1);
        tree.insert(F64::new(200.0), 1);

        assert_eq!(tree.total_count(), 7);

        // Verify ordering: -10.0 should be first, 200.0 should be last
        assert_eq!(tree.select_kth(0, true), Some(&F64::new(-10.0)));
        assert_eq!(tree.select_kth(1, true), Some(&F64::new(0.0)));
        assert_eq!(tree.select_kth(2, true), Some(&F64::new(10.0)));
        assert_eq!(tree.select_kth(6, true), Some(&F64::new(200.0)));

        // Test extreme percentiles
        let (lower, upper, frac) = tree.select_percentile_bounds(0.0, true).unwrap();
        assert_eq!(*lower, F64::new(-10.0));
        assert_eq!(*upper, F64::new(-10.0));
        assert_eq!(frac, 0.0);

        let (lower, upper, frac) = tree.select_percentile_bounds(1.0, true).unwrap();
        assert_eq!(*lower, F64::new(200.0));
        assert_eq!(*upper, F64::new(200.0));
        assert_eq!(frac, 0.0);

        // Test discrete percentiles at extremes
        assert_eq!(
            tree.select_percentile_disc(0.0, true),
            Some(&F64::new(-10.0))
        );
        assert_eq!(
            tree.select_percentile_disc(1.0, true),
            Some(&F64::new(200.0))
        );
    }

    #[test]
    fn test_incremental_insert_duplicates() {
        use crate::algebra::F64;

        // This test simulates the failing Java test scenario:
        // Step 1: Insert -10, 0, 10, 20, 30, 100, 200
        // Step 2: Insert 10, 10 (duplicates)
        // Expected: Tree should contain all 9 values, with min=-10 and max=200

        let mut tree: OrderStatisticsMultiset<F64> = OrderStatisticsMultiset::new();

        // Step 1: Initial insert
        tree.insert(F64::new(-10.0), 1);
        tree.insert(F64::new(0.0), 1);
        tree.insert(F64::new(10.0), 1);
        tree.insert(F64::new(20.0), 1);
        tree.insert(F64::new(30.0), 1);
        tree.insert(F64::new(100.0), 1);
        tree.insert(F64::new(200.0), 1);

        assert_eq!(tree.total_count(), 7);

        // Step 2: Insert duplicates (simulating incremental update)
        tree.insert(F64::new(10.0), 1);
        tree.insert(F64::new(10.0), 1);

        assert_eq!(tree.total_count(), 9);

        // Verify min and max are still correct
        assert_eq!(tree.select_kth(0, true), Some(&F64::new(-10.0)));
        assert_eq!(tree.select_kth(8, true), Some(&F64::new(200.0)));

        // Test extreme percentiles (should still be -10 and 200)
        let (lower, _, _) = tree.select_percentile_bounds(0.0, true).unwrap();
        assert_eq!(*lower, F64::new(-10.0));

        let (lower, _, _) = tree.select_percentile_bounds(1.0, true).unwrap();
        assert_eq!(*lower, F64::new(200.0));

        // Test discrete percentiles at extremes
        assert_eq!(
            tree.select_percentile_disc(0.0, true),
            Some(&F64::new(-10.0))
        );
        assert_eq!(
            tree.select_percentile_disc(1.0, true),
            Some(&F64::new(200.0))
        );
    }

    #[test]
    fn test_semigroup_merge_with_duplicates() {
        use crate::algebra::F64;

        // Simulate two batches being combined via Semigroup::combine
        // Tree 1: Values from step 1
        let mut tree1: OrderStatisticsMultiset<F64> = OrderStatisticsMultiset::new();
        tree1.insert(F64::new(-10.0), 1);
        tree1.insert(F64::new(0.0), 1);
        tree1.insert(F64::new(10.0), 1);
        tree1.insert(F64::new(20.0), 1);
        tree1.insert(F64::new(30.0), 1);
        tree1.insert(F64::new(100.0), 1);
        tree1.insert(F64::new(200.0), 1);

        // Tree 2: Values from step 2 (duplicates of 10)
        let mut tree2: OrderStatisticsMultiset<F64> = OrderStatisticsMultiset::new();
        tree2.insert(F64::new(10.0), 1);
        tree2.insert(F64::new(10.0), 1);

        // Merge the trees (this is what Semigroup::combine does)
        let merged = OrderStatisticsMultiset::merged(&tree1, &tree2);

        assert_eq!(merged.total_count(), 9);

        // Verify the merged tree has correct min/max
        assert_eq!(merged.select_kth(0, true), Some(&F64::new(-10.0)));
        assert_eq!(merged.select_kth(8, true), Some(&F64::new(200.0)));

        // Test extreme percentiles
        let (lower, _, _) = merged.select_percentile_bounds(0.0, true).unwrap();
        assert_eq!(*lower, F64::new(-10.0));

        let (lower, _, _) = merged.select_percentile_bounds(1.0, true).unwrap();
        assert_eq!(*lower, F64::new(200.0));
    }

    #[test]
    fn test_descending_order() {
        let mut tree: OrderStatisticsMultiset<i32> = OrderStatisticsMultiset::new();

        for i in 1..=5 {
            tree.insert(i, 1);
        }

        // Descending: 0th should be max, 4th should be min
        assert_eq!(tree.select_kth(0, false), Some(&5));
        assert_eq!(tree.select_kth(4, false), Some(&1));

        // Descending percentiles
        assert_eq!(tree.select_percentile_disc(0.0, false), Some(&5));
        assert_eq!(tree.select_percentile_disc(1.0, false), Some(&1));
    }
}
