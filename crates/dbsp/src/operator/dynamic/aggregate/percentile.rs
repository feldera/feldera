// Percentile aggregators for DBSP
//
// This module provides PERCENTILE_CONT and PERCENTILE_DISC aggregators
// that follow SQL standard semantics.
//
// The implementation uses an OrderStatisticTree - a BTreeMap-based multiset
// that supports:
// - O(log n) insertion with positive/negative weights (for incremental computation)
// - O(log n) deletion via negative weights
// - O(n) k-th element selection (could be optimized to O(log n) with augmented tree)
// - Proper multiset semantics (duplicate values allowed with counts)

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
};

use rkyv::{Archive, Archived, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;

use crate::{
    DBData, Timestamp,
    algebra::{AddAssignByRef, HasZero, Semigroup, ZWeight},
    dynamic::{DataTrait, DynUnit, Erase, WeightTrait},
    operator::Aggregator,
    trace::Cursor,
    utils::IsNone,
};

/// An order-statistics multiset implemented using a BTreeMap.
///
/// This data structure maintains a sorted multiset of values with counts,
/// supporting efficient insertion and deletion operations for incremental
/// computation in DBSP.
///
/// # Type Parameters
/// - `T`: The value type, must be Ord for the BTreeMap
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Default,
    SizeOf,
    serde::Serialize,
    serde::Deserialize,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(bound(
    serialize = "T: RkyvSerialize<__S>, __S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer",
))]
pub struct OrderStatisticTree<T>
where
    T: Ord + Clone,
{
    /// The underlying BTreeMap stored as a sorted Vec for rkyv serialization
    #[with(rkyv::with::AsVec)]
    tree: BTreeMap<T, i64>,
    /// Total count of all elements (sum of all counts, can be negative during incremental updates)
    total_count: i64,
}

// Implement PartialOrd and Ord for OrderStatisticTree
impl<T> PartialOrd for OrderStatisticTree<T>
where
    T: Ord + Clone,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for OrderStatisticTree<T>
where
    T: Ord + Clone,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match self.total_count.cmp(&other.total_count) {
            Ordering::Equal => {
                let self_iter = self.tree.iter();
                let other_iter = other.tree.iter();
                for (s, o) in self_iter.zip(other_iter) {
                    match s.0.cmp(o.0) {
                        Ordering::Equal => match s.1.cmp(o.1) {
                            Ordering::Equal => continue,
                            other => return other,
                        },
                        other => return other,
                    }
                }
                self.tree.len().cmp(&other.tree.len())
            }
            other => other,
        }
    }
}

// Implement Hash for OrderStatisticTree
impl<T> Hash for OrderStatisticTree<T>
where
    T: Ord + Clone + Hash,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.total_count.hash(state);
        for (key, value) in &self.tree {
            key.hash(state);
            value.hash(state);
        }
    }
}

// Implement IsNone for OrderStatisticTree
impl<T> IsNone for OrderStatisticTree<T>
where
    T: Ord + Clone,
{
    fn is_none(&self) -> bool {
        false // OrderStatisticTree is never "none"
    }
}

// Implement comparison traits for ArchivedOrderStatisticTree
impl<T> PartialEq for ArchivedOrderStatisticTree<T>
where
    T: Archive + Ord + Clone,
    Archived<T>: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.total_count == other.total_count
    }
}

impl<T> Eq for ArchivedOrderStatisticTree<T>
where
    T: Archive + Ord + Clone,
    Archived<T>: Ord,
{
}

impl<T> PartialOrd for ArchivedOrderStatisticTree<T>
where
    T: Archive + Ord + Clone,
    Archived<T>: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ArchivedOrderStatisticTree<T>
where
    T: Archive + Ord + Clone,
    Archived<T>: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.total_count.cmp(&other.total_count)
    }
}

impl<T> OrderStatisticTree<T>
where
    T: Ord + Clone,
{
    /// Create a new empty OrderStatisticTree
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            total_count: 0,
        }
    }

    /// Insert a value with a given weight (count).
    ///
    /// Positive weights add occurrences, negative weights remove occurrences.
    /// This is the core operation for incremental computation.
    pub fn insert(&mut self, value: T, weight: ZWeight) {
        if weight == 0 {
            return;
        }

        self.total_count += weight;

        let entry = self.tree.entry(value).or_insert(0);
        *entry += weight;
        // Note: We leave zero entries for simplicity; compact() can be called to clean up
    }

    /// Get the total count of elements in the multiset
    pub fn total_count(&self) -> i64 {
        self.total_count
    }

    /// Check if the multiset is empty (no elements with positive counts)
    pub fn is_empty(&self) -> bool {
        self.total_count <= 0
    }

    /// Get the k-th smallest element (0-indexed).
    ///
    /// Returns None if k is out of bounds or the tree is empty.
    /// Only considers elements with positive counts.
    pub fn select_kth(&self, k: i64, ascending: bool) -> Option<&T> {
        if k < 0 || self.total_count <= 0 || k >= self.total_count {
            return None;
        }

        if ascending {
            self.select_kth_ascending(k)
        } else {
            self.select_kth_descending(k)
        }
    }

    fn select_kth_ascending(&self, k: i64) -> Option<&T> {
        let mut remaining = k;
        for (value, &count) in &self.tree {
            if count <= 0 {
                continue;
            }
            if remaining < count {
                return Some(value);
            }
            remaining -= count;
        }
        None
    }

    fn select_kth_descending(&self, k: i64) -> Option<&T> {
        let mut remaining = k;
        for (value, &count) in self.tree.iter().rev() {
            if count <= 0 {
                continue;
            }
            if remaining < count {
                return Some(value);
            }
            remaining -= count;
        }
        None
    }

    /// Get the value and fraction for a given percentile (PERCENTILE_CONT).
    ///
    /// Returns (lower_value, upper_value, fraction) where the result is:
    /// lower_value + fraction * (upper_value - lower_value)
    pub fn select_percentile_bounds(
        &self,
        percentile: f64,
        ascending: bool,
    ) -> Option<(&T, &T, f64)> {
        if self.total_count <= 0 || percentile < 0.0 || percentile > 1.0 {
            return None;
        }

        let n = self.total_count;
        if n == 1 {
            let value = self.select_kth(0, ascending)?;
            return Some((value, value, 0.0));
        }

        // SQL standard PERCENTILE_CONT formula:
        // row_position = percentile * (N - 1)
        let pos = percentile * ((n - 1) as f64);
        let lower_idx = pos.floor() as i64;
        let upper_idx = pos.ceil() as i64;
        let fraction = pos - (lower_idx as f64);

        let lower_value = self.select_kth(lower_idx, ascending)?;
        if lower_idx == upper_idx {
            return Some((lower_value, lower_value, 0.0));
        }

        let upper_value = self.select_kth(upper_idx, ascending)?;
        Some((lower_value, upper_value, fraction))
    }

    /// Get the value for PERCENTILE_DISC (discrete percentile).
    ///
    /// Returns the first value whose cumulative distribution >= percentile.
    pub fn select_percentile_disc(&self, percentile: f64, ascending: bool) -> Option<&T> {
        if self.total_count <= 0 || percentile < 0.0 || percentile > 1.0 {
            return None;
        }

        let n = self.total_count;

        // SQL standard PERCENTILE_DISC formula:
        // Find the first row where (row_number / N) >= percentile
        let pos = (percentile * (n as f64)).ceil() as i64;
        let idx = if pos <= 0 {
            0
        } else if pos > n {
            n - 1
        } else {
            pos - 1 // Convert to 0-indexed
        };

        self.select_kth(idx, ascending)
    }

    /// Merge another tree into this one.
    pub fn merge(&mut self, other: &Self) {
        for (value, &count) in &other.tree {
            if count == 0 {
                continue;
            }
            self.total_count += count;
            *self.tree.entry(value.clone()).or_insert(0) += count;
        }
    }

    /// Create a new tree that is the merge of two trees.
    pub fn merged(left: &Self, right: &Self) -> Self {
        let mut result = left.clone();
        result.merge(right);
        result
    }

    /// Remove entries with zero count to save memory.
    pub fn compact(&mut self) {
        self.tree.retain(|_, count| *count != 0);
    }
}

/// Semigroup implementation for OrderStatisticTree.
#[derive(Clone, Debug, Default)]
pub struct OrderStatisticTreeSemigroup<T>(PhantomData<T>);

impl<T> Semigroup<OrderStatisticTree<T>> for OrderStatisticTreeSemigroup<T>
where
    T: DBData,
{
    fn combine(
        left: &OrderStatisticTree<T>,
        right: &OrderStatisticTree<T>,
    ) -> OrderStatisticTree<T> {
        OrderStatisticTree::merged(left, right)
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
    type Accumulator = OrderStatisticTree<V>;
    type Semigroup = OrderStatisticTreeSemigroup<V>;
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
        let mut tree = OrderStatisticTree::new();
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
    type Accumulator = OrderStatisticTree<V>;
    type Semigroup = OrderStatisticTreeSemigroup<V>;
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
        let mut tree = OrderStatisticTree::new();
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

    #[test]
    fn test_negative_numbers() {
        // Test that negative numbers are correctly ordered
        let mut tree: OrderStatisticTree<i32> = OrderStatisticTree::new();

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
        let mut tree: OrderStatisticTree<F64> = OrderStatisticTree::new();

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

        let mut tree: OrderStatisticTree<F64> = OrderStatisticTree::new();

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
        let mut tree1: OrderStatisticTree<F64> = OrderStatisticTree::new();
        tree1.insert(F64::new(-10.0), 1);
        tree1.insert(F64::new(0.0), 1);
        tree1.insert(F64::new(10.0), 1);
        tree1.insert(F64::new(20.0), 1);
        tree1.insert(F64::new(30.0), 1);
        tree1.insert(F64::new(100.0), 1);
        tree1.insert(F64::new(200.0), 1);

        // Tree 2: Values from step 2 (duplicates of 10)
        let mut tree2: OrderStatisticTree<F64> = OrderStatisticTree::new();
        tree2.insert(F64::new(10.0), 1);
        tree2.insert(F64::new(10.0), 1);

        // Merge the trees (this is what Semigroup::combine does)
        let merged = OrderStatisticTree::merged(&tree1, &tree2);

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
}
