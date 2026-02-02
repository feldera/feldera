// Percentile semigroups for DBSP
//
// This module provides semigroups used by the SQL compiler for percentile aggregation.
// The actual percentile computation is now handled by the stateful PercentileOperator
// in `operator/dynamic/percentile_op.rs`.

use std::marker::PhantomData;

use crate::{
    DBData,
    algebra::{OrderStatisticsMultiset, Semigroup},
    utils::Tup2,
};

/// Semigroup implementation for OrderStatisticsMultiset.
///
/// Used by the SQL compiler for combining partial aggregates.
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
}
