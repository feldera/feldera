/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module implements ZSets.
//! A ZSet maps arbitrary keys (which only need to support equality and hashing)
//! to weights.  The weights must implement the ZRingValue trait.

use super::{
    finite_map::{FiniteHashMap, FiniteMap, KeyProperties},
    AddAssignByRef, HasZero, ZRingValue,
};
#[cfg(test)]
use super::{AddByRef, CheckedI64, NegByRef};
#[cfg(test)]
use std::cmp::Ordering;
#[cfg(test)]
use std::fmt::{Display, Error, Formatter};

////////////////////////////////////////////////////////
/// Z-set trait.
///
/// A Z-set is a set where each element has a weight.
/// Weights belong to some ring.
///
/// `DataType` - Type of values stored in Z-set
/// `WeightType` - Type of weights.  Must be a value from a Ring.
pub trait ZSet<DataType, WeightType>: FiniteMap<DataType, WeightType>
where
    DataType: KeyProperties,
    WeightType: ZRingValue,
{
    //type KeyIterator: Iterator<Item=DataType>;

    /// Multiply each value in the other Z-set by `weight`
    /// and add it to this set.  This is similar to `mul_add_assign`, but
    /// not the same, since `mul_add_assign` multiplies `self`, whereas
    /// this trait multiplies `other`.
    fn add_assign_weighted(&mut self, weight: &WeightType, other: &Self);

    /// Returns a Z-set that contains all elements with positive weights from `self`
    /// with weights set to 1.
    fn distinct(&self) -> Self;

    /// Given a Z-set 'set' partition it using a 'partitioner'
    /// function which is applied independently to each tuple.
    /// This consumes the Z-set.
    fn partition<KeyType, F>(
        self,
        partitioner: &F,
    ) -> IndexedZSetMap<KeyType, DataType, WeightType>
    where
        KeyType: KeyProperties,
        F: Fn(&DataType) -> KeyType;

    /// Cartesian product between this zset and `other`.
    /// Every data value in this set paired with every
    /// data value in `other` and the merger is applied.  The result has a weight
    /// equal to the product of the data weights, and everything
    /// is summed into a ZSetHashMap.
    //  TODO: this should return a trait, and not an implementation.
    fn cartesian<DataType2, ZS2, DataType3, F>(
        &self,
        other: &ZS2,
        merger: &F,
    ) -> ZSetHashMap<DataType3, WeightType>
    where
        DataType2: KeyProperties,
        DataType3: KeyProperties,
        F: Fn(&DataType, &DataType2) -> DataType3,
        ZS2: ZSet<DataType2, WeightType>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a DataType2, &'a WeightType)>;

    /// Join two sets.  `K` is the type of keys used to perform the join.
    /// `left_key` is the function that computes the key for each tuple of self.
    /// `right_key` is the function that computes the key for each tuple of `other`.
    /// `merger` is the function that merges elements that have the same key.
    fn join<K, KF, KF2, DataType2, ZS2, DataType3, F>(
        &self,
        other: &ZS2,
        left_key: &KF,
        right_key: &KF2,
        merger: F,
    ) -> ZSetHashMap<DataType3, WeightType>
    where
        K: KeyProperties,
        DataType2: KeyProperties,
        DataType3: KeyProperties,
        KF: Fn(&DataType) -> K,
        KF2: Fn(&DataType2) -> K,
        F: Fn(&DataType, &DataType2) -> DataType3,
        ZS2: ZSet<DataType2, WeightType>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a DataType2, &'a WeightType)>;
}

///////////////////////////////////////////////////////
/// Implementation of ZSets in terms of HashMaps
pub type ZSetHashMap<DataType, WeightType> = FiniteHashMap<DataType, WeightType>;

impl<DataType, WeightType> ZSet<DataType, WeightType> for ZSetHashMap<DataType, WeightType>
where
    DataType: KeyProperties,
    WeightType: ZRingValue,
{
    fn add_assign_weighted(&mut self, weight: &WeightType, other: &Self) {
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
                result.increment(key, WeightType::one());
            }
        }
        result
    }

    fn partition<KeyType, F>(self, partitioner: &F) -> IndexedZSetMap<KeyType, DataType, WeightType>
    where
        KeyType: KeyProperties,
        F: Fn(&DataType) -> KeyType,
    {
        let mut result = FiniteHashMap::<KeyType, ZSetHashMap<DataType, WeightType>>::new();
        for (t, w) in self {
            let k = partitioner(&t);
            let zs = ZSetHashMap::<DataType, WeightType>::singleton(t, w);
            result.increment(&k, zs);
        }
        result
    }

    fn cartesian<DataType2, ZS2, DataType3, F>(
        &self,
        other: &ZS2,
        merger: &F,
    ) -> ZSetHashMap<DataType3, WeightType>
    where
        DataType2: KeyProperties,
        DataType3: KeyProperties,
        F: Fn(&DataType, &DataType2) -> DataType3,
        ZS2: ZSet<DataType2, WeightType>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a DataType2, &'a WeightType)>,
    {
        let mut result = ZSetHashMap::<DataType3, WeightType>::new();
        for (k, v) in self {
            for (k2, v2) in other {
                let data = merger(k, k2);
                let weight = v.mul_by_ref(v2);
                result.increment(&data, weight)
            }
        }
        result
    }

    fn join<K, KF, KF2, DataType2, ZS2, DataType3, F>(
        &self,
        other: &ZS2,
        left_key: &KF,
        right_key: &KF2,
        merger: F,
    ) -> ZSetHashMap<DataType3, WeightType>
    where
        K: KeyProperties,
        DataType2: KeyProperties,
        DataType3: KeyProperties,
        KF: Fn(&DataType) -> K,
        KF2: Fn(&DataType2) -> K,
        F: Fn(&DataType, &DataType2) -> DataType3,
        ZS2: ZSet<DataType2, WeightType>,
        for<'a> &'a ZS2: IntoIterator<Item = (&'a DataType2, &'a WeightType)>,
    {
        let combiner = move |left: &ZSetHashMap<DataType, WeightType>,
                             right: &ZSetHashMap<DataType2, WeightType>| {
            left.cartesian::<DataType2, ZSetHashMap<DataType2, WeightType>, DataType3, _>(
                right, &merger,
            )
        };
        let li: IndexedZSetMap<K, DataType, WeightType> = self.clone().partition(left_key);
        let ri: IndexedZSetMap<K, DataType2, WeightType> = other.clone().partition(right_key);
        li.match_keys::<ZSetHashMap<DataType2, WeightType>,
            ZSetHashMap<DataType3, WeightType>,
            IndexedZSetMap<K, DataType2,WeightType>,_>(&ri, &combiner).sum()
    }
}

#[test]
fn zset_integer_tests() {
    let mut z = ZSetHashMap::<i64, i64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!("{}", z.to_string());
    assert_eq!(0, z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, i64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<i64, i64>::new();
    assert_eq!(z, z2);

    z.increment(&0, 1);
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());
    assert_eq!(1, z.lookup(&0));
    assert_eq!(0, z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, i64>::zero());
    assert_eq!(false, z.is_zero());

    z.increment(&2, 0);
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());

    z.increment(&1, -1);
    assert_eq!(2, z.support_size());
    assert_eq!("{0=>1,1=>-1}", z.to_string());

    z.increment(&-1, 1);
    assert_eq!(3, z.support_size());
    assert_eq!("{-1=>1,0=>1,1=>-1}", z.to_string());

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!("{-1=>1,0=>1}", d.to_string());

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!("{-1=>-1,0=>-1,1=>1}", d.to_string());
    assert_ne!(d, z);

    z.increment(&1, 1);
    assert_eq!(2, z.support_size());
    assert_eq!("{-1=>1,0=>1}", z.to_string());

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>2,0=>2}", z2.to_string());

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>3,0=>3}", z2.to_string());
}

#[test]
fn checked_zset_integer_weights_tests() {
    let mut z = ZSetHashMap::<i64, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!("{}", z.to_string());
    assert_eq!(CheckedI64::from(0), z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<i64, CheckedI64>::new();
    assert_eq!(z, z2);

    z.increment(&0, CheckedI64::from(1));
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());
    assert_eq!(CheckedI64::from(1), z.lookup(&0));
    assert_eq!(CheckedI64::from(0), z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert_eq!(false, z.is_zero());

    z.increment(&2, CheckedI64::from(0));
    assert_eq!(1, z.support_size());
    assert_eq!("{0=>1}", z.to_string());

    z.increment(&1, CheckedI64::from(-1));
    assert_eq!(2, z.support_size());
    assert_eq!("{0=>1,1=>-1}", z.to_string());

    z.increment(&-1, CheckedI64::from(1));
    assert_eq!(3, z.support_size());
    assert_eq!("{-1=>1,0=>1,1=>-1}", z.to_string());

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!("{-1=>1,0=>1}", d.to_string());

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!("{-1=>-1,0=>-1,1=>1}", d.to_string());
    assert_ne!(d, z);

    z.increment(&1, CheckedI64::from(1));
    assert_eq!(2, z.support_size());
    assert_eq!("{-1=>1,0=>1}", z.to_string());

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>2,0=>2}", z2.to_string());

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{-1=>3,0=>3}", z2.to_string());
}

#[cfg(test)]
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct TestTuple {
    left: i64,
    right: i64,
}

#[cfg(test)]
impl TestTuple {
    fn new(left: i64, right: i64) -> Self {
        Self { left, right }
    }
}

#[cfg(test)]
impl Display for TestTuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "({},{})", self.left, self.right)
    }
}

#[cfg(test)]
impl PartialOrd for TestTuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let lo = self.left.cmp(&other.left);
        if lo != Ordering::Equal {
            return Some(lo);
        }
        Some(self.right.cmp(&other.right))
    }
}

#[cfg(test)]
impl Ord for TestTuple {
    fn cmp(&self, other: &Self) -> Ordering {
        let lo = self.left.cmp(&other.left);
        if lo != Ordering::Equal {
            return lo;
        }
        self.right.cmp(&other.right)
    }
}

#[test]
fn zset_tuple_tests() {
    let mut z = ZSetHashMap::<TestTuple, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!("{}", z.to_string());
    assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 0))); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<TestTuple, CheckedI64>::new();
    assert_eq!(z, z2);

    z.increment(&TestTuple::new(0, 0), CheckedI64::from(1));
    assert_eq!(1, z.support_size());
    assert_eq!("{(0,0)=>1}", z.to_string());
    assert_eq!(CheckedI64::from(1), z.lookup(&TestTuple::new(0, 0)));
    assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 1)));
    assert_ne!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
    assert_eq!(false, z.is_zero());

    z.increment(&TestTuple::new(2, 0), CheckedI64::from(0));
    assert_eq!(1, z.support_size());
    assert_eq!("{(0,0)=>1}", z.to_string());

    z.increment(&TestTuple::new(1, 0), CheckedI64::from(-1));
    assert_eq!(2, z.support_size());
    assert_eq!("{(0,0)=>1,(1,0)=>-1}", z.to_string());

    z.increment(&TestTuple::new(-1, 0), CheckedI64::from(1));
    assert_eq!(3, z.support_size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1,(1,0)=>-1}", z.to_string());

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1}", d.to_string());

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!("{(-1,0)=>-1,(0,0)=>-1,(1,0)=>1}", d.to_string());
    assert_ne!(d, z);

    z.increment(&TestTuple::new(1, 0), CheckedI64::from(1));
    assert_eq!(2, z.support_size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1}", z.to_string());

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    let z2str = z2.to_string();
    assert_eq!("{(-1,0)=>2,(0,0)=>2}", z2str);

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!("{(-1,0)=>3,(0,0)=>3}", z2.to_string());

    let prod = z2.cartesian(&z2, &|t, t2| {
        TestTuple::new(t.left + t2.left, t.right - t2.right)
    });
    assert_eq!("{(-2,0)=>9,(-1,0)=>18,(0,0)=>9}", prod.to_string());

    let j = z2.join(&z2, &|s| s.left, &|s| s.left, |a, _| a.clone());
    assert_eq!("{(-1,0)=>9,(0,0)=>9}", j.to_string());
}

type IndexedZSetMap<KeyType, DataType, WeightType> =
    FiniteHashMap<KeyType, ZSetHashMap<DataType, WeightType>>;

/// An indexed Z-set is a structure that maps arbitrary keys of type KeyType
/// to Z-set values
impl<KeyType, DataType, WeightType> IndexedZSetMap<KeyType, DataType, WeightType>
where
    DataType: KeyProperties,
    KeyType: KeyProperties,
    WeightType: ZRingValue,
    for<'a> &'a Self: IntoIterator<Item = (&'a KeyType, &'a ZSetHashMap<DataType, WeightType>)>,
{
    /// Add all the data in all partitions into a single zset.
    pub fn sum(&self) -> ZSetHashMap<DataType, WeightType> {
        let mut result = ZSetHashMap::<DataType, WeightType>::zero();
        self.into_iter()
            // fold does not seem to work with add_assign.
            .for_each(|(_, v)| result.add_assign_by_ref(v));
        result
    }
}

#[test]
pub fn indexed_zset_tests() {
    let mut z = ZSetHashMap::<TestTuple, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    z.increment(&TestTuple::new(0, 0), CheckedI64::from(1));
    z.increment(&TestTuple::new(2, 0), CheckedI64::from(2));
    z.increment(&TestTuple::new(1, 0), CheckedI64::from(-1));
    z.increment(&TestTuple::new(-1, 0), CheckedI64::from(1));
    let ps = z.clone().partition(&|t: &TestTuple| t.left.abs() % 2);
    let s = ps.to_string();
    assert_eq!("{0=>{(0,0)=>1,(2,0)=>2},1=>{(-1,0)=>1,(1,0)=>-1}}", s);
    let z2 = ps.sum();
    assert_eq!(z, z2);
}
