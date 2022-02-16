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

    /// Given a Z-set 'set' partition it using a 'partitioner'
    /// function which is applied independently to each tuple.
    /// This consumes the Z-set.
    fn partition<Key, F>(self, partitioner: F) -> IndexedZSetMap<Key, Data, Weight>
    where
        Key: KeyProperties,
        F: FnMut(&Data) -> Key;

    /// Cartesian product between this zset and `other`.
    /// Every data value in this set paired with every
    /// data value in `other` and the merger is applied.
    /// The result has a weight equal to the product of the
    /// data weights, and everything is summed into a [`ZSetHashMap`].
    // TODO: this should return a trait, and not an implementation.
    fn cartesian<Data2, ZS2, Data3, F>(&self, other: &ZS2, merger: F) -> ZSetHashMap<Data3, Weight>
    where
        Data2: KeyProperties,
        Data3: KeyProperties,
        F: FnMut(&Data, &Data2) -> Data3,
        ZS2: ZSet<Data2, Weight>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a Data2, &'a Weight)>;

    /// Join two sets.  `K` is the type of keys used to perform the join.
    /// `left_key` is the function that computes the key for each tuple of self.
    /// `right_key` is the function that computes the key for each tuple of
    /// `other`. `merger` is the function that merges elements that have the
    /// same key.
    fn join<K, KF, KF2, Data2, ZS2, Data3, F>(
        &self,
        other: &ZS2,
        left_key: KF,
        right_key: KF2,
        merger: F,
    ) -> ZSetHashMap<Data3, Weight>
    where
        K: KeyProperties,
        Data2: KeyProperties,
        Data3: KeyProperties,
        KF: FnMut(&Data) -> K,
        KF2: FnMut(&Data2) -> K,
        F: FnMut(&Data, &Data2) -> Data3,
        ZS2: ZSet<Data2, Weight>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a Data2, &'a Weight)>;
}

/// An implementation of ZSets using [`FiniteHashMap`]s
pub type ZSetHashMap<Data, Weight> = FiniteHashMap<Data, Weight>;

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

    fn partition<KeyType, F>(self, mut partitioner: F) -> IndexedZSetMap<KeyType, Data, Weight>
    where
        KeyType: KeyProperties,
        F: FnMut(&Data) -> KeyType,
    {
        let mut result = FiniteHashMap::new();
        for (t, w) in self {
            let k = partitioner(&t);
            let zs = ZSetHashMap::singleton(t, w);
            result.increment(&k, zs);
        }

        result
    }

    fn cartesian<Data2, ZS2, Data3, F>(
        &self,
        other: &ZS2,
        mut merger: F,
    ) -> ZSetHashMap<Data3, Weight>
    where
        Data2: KeyProperties,
        Data3: KeyProperties,
        F: FnMut(&Data, &Data2) -> Data3,
        ZS2: ZSet<Data2, Weight>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a Data2, &'a Weight)>,
    {
        let mut result = ZSetHashMap::new();
        for (k, v) in self {
            for (k2, v2) in other {
                let data = merger(k, k2);
                let weight = v.mul_by_ref(v2);
                result.increment(&data, weight)
            }
        }

        result
    }

    fn join<K, KF, KF2, Data2, ZS2, Data3, F>(
        &self,
        other: &ZS2,
        left_key: KF,
        right_key: KF2,
        mut merger: F,
    ) -> ZSetHashMap<Data3, Weight>
    where
        K: KeyProperties,
        Data2: KeyProperties,
        Data3: KeyProperties,
        KF: FnMut(&Data) -> K,
        KF2: FnMut(&Data2) -> K,
        F: FnMut(&Data, &Data2) -> Data3,
        ZS2: ZSet<Data2, Weight>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a Data2, &'a Weight)>,
    {
        let combiner = |left: &ZSetHashMap<Data, Weight>, right: &ZSetHashMap<Data2, Weight>| {
            left.cartesian::<Data2, ZSetHashMap<Data2, Weight>, Data3, _>(right, &mut merger)
        };

        let left = self.clone().partition(left_key);
        let right = other.clone().partition(right_key);

        left.match_keys::<
            ZSetHashMap<Data2, Weight>,
            ZSetHashMap<Data3, Weight>,
            IndexedZSetMap<K, Data2,Weight>,
            _,
        >(&right, combiner)
        .sum()
    }
}

type IndexedZSetMap<Key, Data, Weight> = FiniteHashMap<Key, ZSetHashMap<Data, Weight>>;

/// An indexed Z-set is a structure that maps arbitrary keys to Z-set values
impl<Key, Data, Weight> IndexedZSetMap<Key, Data, Weight>
where
    Data: KeyProperties,
    Key: KeyProperties,
    Weight: ZRingValue,
    for<'a> &'a Self: IntoIterator<Item = (&'a Key, &'a ZSetHashMap<Data, Weight>)>,
{
    /// Add all the data in all partitions into a single zset.
    pub fn sum(&self) -> ZSetHashMap<Data, Weight> {
        self.into_iter()
            .fold(ZSetHashMap::new(), |mut set, (_, values)| {
                set.add_assign_by_ref(values);
                set
            })
    }
}
