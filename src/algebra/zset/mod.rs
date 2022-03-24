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

    /// Multiply each value in the other Z-set by `weight`
    /// and add it to this set.  This is similar to `mul_add_assign`, but
    /// not the same, since `mul_add_assign` multiplies `self`, whereas
    /// this trait multiplies `other`.
    fn add_assign_weighted(&mut self, weight: &Weight, other: &Self);

    /// Returns a Z-set that contains all elements with positive weights from
    /// `self` with weights set to 1.
    fn distinct(&self) -> Self;

    /// Like `distinct` but optimized to operate on an owned value.
    fn distinct_owned(self) -> Self;

    /// Given a Z-set 'set' partition it using a 'partitioner'
    /// function which is applied independently to each tuple.
    /// This consumes the Z-set.
    fn partition<Key, F, I>(self, partitioner: F) -> I
    where
        Key: KeyProperties,
        F: FnMut(&Data) -> Key,
        I: IndexedZSet<Key, Data, Weight>;
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
    fn add_assign_weighted(&mut self, weight: &Weight, other: &Self) {
        if weight.is_zero() {
            return;
        }

        for (key, value) in &other.value {
            let new_weight = value.mul_by_ref(weight);
            self.increment(key, new_weight);
        }
    }

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

    fn partition<KeyType, F, I>(self, mut partitioner: F) -> I
    where
        KeyType: KeyProperties,
        F: FnMut(&Data) -> KeyType,
        I: IndexedZSet<KeyType, Data, Weight>,
    {
        let mut result = I::empty();
        for (t, w) in self {
            let k = partitioner(&t);
            result.update(&k, |zs| zs.increment_owned(t, w));
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

    /// Add all the data in all partitions into a single zset.
    fn sum(&self) -> Self::ZSet
    where
        for<'a> &'a Self: IntoIterator<Item = (&'a Key, &'a Self::ZSet)>,
    {
        self.into_iter()
            .fold(Self::ZSet::empty(), |mut set, (_, values)| {
                set.add_assign_by_ref(values);
                set
            })
    }
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
