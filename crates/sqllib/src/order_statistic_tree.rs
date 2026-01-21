// Order Statistics Multiset for SQL percentile functions
//
// This module re-exports the core OrderStatisticTree from dbsp and provides
// sqllib-specific semigroups for the Fold-based aggregate pattern.

use dbsp::algebra::Semigroup;
use std::fmt::Debug;
use std::marker::PhantomData;

// Re-export the core types from dbsp
pub use dbsp::operator::{OrderStatisticTree, OrderStatisticTreeSemigroup};

/// Semigroup for the percentile accumulator type: (Option<percentile>, OrderStatisticTree<T>)
///
/// The percentile value should be the same across all groups, so we take the first Some value.
/// This is used by the SQL compiler's Fold-based percentile aggregation pattern.
#[derive(Clone)]
#[doc(hidden)]
pub struct PercentileTreeSemigroup<T>(PhantomData<T>);

impl<P, V> Semigroup<crate::Tup2<Option<P>, OrderStatisticTree<V>>>
    for PercentileTreeSemigroup<crate::Tup2<Option<P>, OrderStatisticTree<V>>>
where
    P: Clone + Debug + Default + Send + Sync + 'static,
    V: dbsp::DBData,
{
    fn combine(
        left: &crate::Tup2<Option<P>, OrderStatisticTree<V>>,
        right: &crate::Tup2<Option<P>, OrderStatisticTree<V>>,
    ) -> crate::Tup2<Option<P>, OrderStatisticTree<V>> {
        // Take the first Some percentile value (they should all be the same)
        let percentile = match (&left.0, &right.0) {
            (Some(p), _) => Some(p.clone()),
            (None, Some(p)) => Some(p.clone()),
            (None, None) => None,
        };

        // Merge the trees
        let tree = OrderStatisticTree::merged(&left.1, &right.1);

        crate::Tup2::new(percentile, tree)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dbsp::algebra::ZWeight;

    #[test]
    fn test_basic_operations() {
        let mut tree: OrderStatisticTree<i32> = OrderStatisticTree::new();

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
        let mut tree: OrderStatisticTree<i32> = OrderStatisticTree::new();

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
        let mut tree: OrderStatisticTree<i32> = OrderStatisticTree::new();

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
        let mut tree: OrderStatisticTree<i32> = OrderStatisticTree::new();

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
        let mut tree: OrderStatisticTree<i32> = OrderStatisticTree::new();

        for i in 1..=5 {
            tree.insert(i, 1);
        }

        assert_eq!(tree.select_percentile_disc(0.0, true), Some(&1));
        assert_eq!(tree.select_percentile_disc(0.5, true), Some(&3));
        assert_eq!(tree.select_percentile_disc(1.0, true), Some(&5));
    }

    #[test]
    fn test_merge() {
        let mut tree1: OrderStatisticTree<i32> = OrderStatisticTree::new();
        tree1.insert(1, 1);
        tree1.insert(3, 1);
        tree1.insert(5, 1);

        let mut tree2: OrderStatisticTree<i32> = OrderStatisticTree::new();
        tree2.insert(2, 1);
        tree2.insert(4, 1);

        let merged = OrderStatisticTree::merged(&tree1, &tree2);
        assert_eq!(merged.total_count(), 5);
        assert_eq!(merged.select_kth(0, true), Some(&1));
        assert_eq!(merged.select_kth(1, true), Some(&2));
        assert_eq!(merged.select_kth(2, true), Some(&3));
        assert_eq!(merged.select_kth(3, true), Some(&4));
        assert_eq!(merged.select_kth(4, true), Some(&5));
    }
}
