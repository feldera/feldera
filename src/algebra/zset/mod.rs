#[cfg(test)]
pub(crate) mod tests;

use crate::algebra::{
    finite_map::{FiniteHashMap, FiniteMap, KeyProperties, MapBuilder},
    AddAssignByRef, ZRingValue,
};

/// The Z-set trait.
///
/// A Z-set is a set where each element has a weight.
/// Weights belong to some ring.
///
/// `Data` - Type of values stored in Z-set
/// `Weight` - Type of weights.  Must be a value from a Ring.
pub trait ZSet<Data, Weight>: FiniteMap<Data, Weight>
where
    Data: KeyProperties,
    Weight: ZRingValue,
{
    // type KeyIterator: Iterator<Ite m= Data>;

    /// Returns a Z-set that contains all elements with positive weights from `self` with weights
    /// set to 1.
    fn distinct(&self) -> Self;

    /// Like `distinct` but optimized to operate on an owned value.
    fn distinct_owned(self) -> Self;
}

/// An implementation of ZSets using [`FiniteHashMap`]s
pub type ZSetHashMap<Data, Weight> = FiniteHashMap<Data, Weight>;

/// Build a Z-set from an iterator, giving each item a weight of 1.
impl<Data, Weight> FromIterator<Data> for ZSetHashMap<Data, Weight>
where
    Data: KeyProperties,
    Weight: ZRingValue,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Data>,
    {
        Self::from_iter(iter.into_iter().map(|d| (d, Weight::one())))
    }
}

impl<Data, Weight> ZSet<Data, Weight> for ZSetHashMap<Data, Weight>
where
    Data: KeyProperties,
    Weight: ZRingValue,
{
    fn distinct(&self) -> Self {
        let mut result = Self::new();

        for (key, value) in &self.value {
            if value.ge0() {
                result.increment(key, Weight::one());
            }
        }

        result
    }

    fn distinct_owned(self) -> Self {
        let mut result = Self::new();

        for (key, value) in self.value.into_iter() {
            if value.ge0() {
                result.increment_owned(key, Weight::one());
            }
        }

        result
    }
}

/// An indexed Z-set maps arbitrary keys to Z-set values.
pub trait IndexedZSet<Key, Value, Weight>: FiniteMap<Key, Self::ZSet>
where
    Key: KeyProperties,
    Value: KeyProperties,
    Weight: ZRingValue,
{
    type ZSet: ZSet<Value, Weight>;
}

pub type IndexedZSetHashMap<Key, Value, Weight> = FiniteHashMap<Key, ZSetHashMap<Value, Weight>>;

impl<Key, Value, Weight> IndexedZSet<Key, Value, Weight> for IndexedZSetHashMap<Key, Value, Weight>
where
    Key: KeyProperties,
    Value: KeyProperties,
    Weight: ZRingValue,
{
    type ZSet = ZSetHashMap<Value, Weight>;
}
