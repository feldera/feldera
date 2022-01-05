/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module implements ZSets.
//! A ZSet maps arbitrary keys (which only need to support equality and hashing)
//! to weights.  The weights must implement the ZRingValue trait.

use super::*;
use finite_map::*;
#[cfg(test)]
use std::cmp::Ordering;
use std::hash::Hash;

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
    DataType: Clone + Hash + Eq + 'static,
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
}

///////////////////////////////////////////////////////
/// Implementation of ZSets in terms of HashMaps
type ZSetHashMap<DataType, WeightType> = FiniteHashMap<DataType, WeightType>;

impl<DataType, WeightType> ZSet<DataType, WeightType> for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    fn distinct(&self) -> Self {
        let mut result = Self::new();
        for (key, value) in &self.value {
            if value.ge0() {
                result.insert(key, &WeightType::one());
            }
        }
        result
    }
    fn add_assign_weighted(&mut self, weight: &WeightType, other: &Self) {
        if WeightType::is_zero(weight) {
            return;
        }
        for (key, value) in &other.value {
            let new_weight = value.clone().mul(weight.clone());
            self.insert(key, &new_weight);
        }
    }
}

#[test]
fn zset_integer_tests() {
    let mut z = ZSetHashMap::<i64, i64>::with_capacity(5);
    assert_eq!(0, z.size());
    assert_eq!("{}", to_string(&z));
    assert_eq!(0, z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, i64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<i64, i64>::new();
    assert_eq!(z, z2);

    z.insert(&0, &1);
    assert_eq!(1, z.size());
    assert_eq!("{0=>1}", to_string(&z));
    assert_eq!(1, z.lookup(&0));
    assert_eq!(0, z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, i64>::zero());
    assert_eq!(false, z.is_zero());

    z.insert(&2, &0);
    assert_eq!(1, z.size());
    assert_eq!("{0=>1}", to_string(&z));

    z.insert(&1, &-1);
    assert_eq!(2, z.size());
    assert_eq!("{0=>1,1=>-1}", to_string(&z));

    z.insert(&-1, &1);
    assert_eq!(3, z.size());
    assert_eq!("{-1=>1,0=>1,1=>-1}", to_string(&z));

    let d = z.distinct();
    assert_eq!(2, d.size());
    assert_eq!("{-1=>1,0=>1}", to_string(&d));

    let d = z.clone().neg();
    assert_eq!(3, d.size());
    assert_eq!("{-1=>-1,0=>-1,1=>1}", to_string(&d));
    assert_ne!(d, z);

    z.insert(&1, &1);
    assert_eq!(2, z.size());
    assert_eq!("{-1=>1,0=>1}", to_string(&z));

    let mut z2 = z.clone().add(z.clone());
    assert_eq!(2, z2.size());
    assert_eq!("{-1=>2,0=>2}", to_string(&z2));

    z2.add_assign(z.clone());
    assert_eq!(2, z2.size());
    assert_eq!("{-1=>3,0=>3}", to_string(&z2));
}

#[test]
fn checked_zset_integer_weights_tests() {
    let mut z = ZSetHashMap::<i64, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.size());
    assert_eq!("{}", to_string(&z));
    assert_eq!(CheckedI64::from(0), z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<i64, CheckedI64>::new();
    assert_eq!(z, z2);

    z.insert(&0, &CheckedI64::from(1));
    assert_eq!(1, z.size());
    assert_eq!("{0=>1}", to_string(&z));
    assert_eq!(CheckedI64::from(1), z.lookup(&0));
    assert_eq!(CheckedI64::from(0), z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert_eq!(false, z.is_zero());

    z.insert(&2, &CheckedI64::from(0));
    assert_eq!(1, z.size());
    assert_eq!("{0=>1}", to_string(&z));

    z.insert(&1, &CheckedI64::from(-1));
    assert_eq!(2, z.size());
    assert_eq!("{0=>1,1=>-1}", to_string(&z));

    z.insert(&-1, &CheckedI64::from(1));
    assert_eq!(3, z.size());
    assert_eq!("{-1=>1,0=>1,1=>-1}", to_string(&z));

    let d = z.distinct();
    assert_eq!(2, d.size());
    assert_eq!("{-1=>1,0=>1}", to_string(&d));

    let d = z.clone().neg();
    assert_eq!(3, d.size());
    assert_eq!("{-1=>-1,0=>-1,1=>1}", to_string(&d));
    assert_ne!(d, z);

    z.insert(&1, &CheckedI64::from(1));
    assert_eq!(2, z.size());
    assert_eq!("{-1=>1,0=>1}", to_string(&z));

    let mut z2 = z.clone().add(z.clone());
    assert_eq!(2, z2.size());
    assert_eq!("{-1=>2,0=>2}", to_string(&z2));

    z2.add_assign(z.clone());
    assert_eq!(2, z2.size());
    assert_eq!("{-1=>3,0=>3}", to_string(&z2));
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
    assert_eq!(0, z.size());
    assert_eq!("{}", to_string(&z));
    assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 0))); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
    assert!(z.is_zero());
    let z2 = ZSetHashMap::<TestTuple, CheckedI64>::new();
    assert_eq!(z, z2);

    z.insert(&TestTuple::new(0, 0), &CheckedI64::from(1));
    assert_eq!(1, z.size());
    assert_eq!("{(0,0)=>1}", to_string(&z));
    assert_eq!(CheckedI64::from(1), z.lookup(&TestTuple::new(0, 0)));
    assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 1)));
    assert_ne!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
    assert_eq!(false, z.is_zero());

    z.insert(&TestTuple::new(2, 0), &CheckedI64::from(0));
    assert_eq!(1, z.size());
    assert_eq!("{(0,0)=>1}", to_string(&z));

    z.insert(&TestTuple::new(1, 0), &CheckedI64::from(-1));
    assert_eq!(2, z.size());
    assert_eq!("{(0,0)=>1,(1,0)=>-1}", to_string(&z));

    z.insert(&TestTuple::new(-1, 0), &CheckedI64::from(1));
    assert_eq!(3, z.size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1,(1,0)=>-1}", to_string(&z));

    let d = z.distinct();
    assert_eq!(2, d.size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1}", to_string(&d));

    let d = z.clone().neg();
    assert_eq!(3, d.size());
    assert_eq!("{(-1,0)=>-1,(0,0)=>-1,(1,0)=>1}", to_string(&d));
    assert_ne!(d, z);

    z.insert(&TestTuple::new(1, 0), &CheckedI64::from(1));
    assert_eq!(2, z.size());
    assert_eq!("{(-1,0)=>1,(0,0)=>1}", to_string(&z));

    let mut z2 = z.clone().add(z.clone());
    assert_eq!(2, z2.size());
    let z2str = to_string(&z2);
    assert_eq!("{(-1,0)=>2,(0,0)=>2}", z2str);

    z2.add_assign(z.clone());
    assert_eq!(2, z2.size());
    assert_eq!("{(-1,0)=>3,(0,0)=>3}", to_string(&z2));
}

type IndexedZSetMap<KeyType, DataType, WeightType> =
    FiniteHashMap<KeyType, ZSetHashMap<DataType, WeightType>>;

/// An indexed Z-set is a structure that maps arbitrary keys of type KeyType
/// to Z-set values
impl<KeyType, DataType, WeightType> IndexedZSetMap<KeyType, DataType, WeightType>
where
    DataType: Clone + 'static + Hash + Eq + Display,
    KeyType: Clone + 'static + Hash + Eq + Display,
    WeightType: ZRingValue,
{
    /// Given a Z-set 'set' partition it using a 'partitioned'
    /// function which is applied independently to each tuple.
    pub fn partition<ZS>(set: ZS, partitioner: fn(&DataType) -> KeyType) -> Self
    where
        ZS: ZSet<DataType, WeightType>,
    {
        let mut result =
            FiniteHashMap::<KeyType, ZSetHashMap<DataType, WeightType>>::with_capacity(set.size());
        for t in set.support() {
            let k = partitioner(t);
            let w = set.lookup(t);
            let zs = ZSetHashMap::<DataType, WeightType>::singleton(t, &w);
            result.insert(&k, &zs);
        }
        result
    }
}

#[test]
pub fn indexed_zset_tests() {
    let mut z = ZSetHashMap::<TestTuple, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.size());
    z.insert(&TestTuple::new(0, 0), &CheckedI64::from(1));
    z.insert(&TestTuple::new(2, 0), &CheckedI64::from(2));
    z.insert(&TestTuple::new(1, 0), &CheckedI64::from(-1));
    z.insert(&TestTuple::new(-1, 0), &CheckedI64::from(1));
    let ps = IndexedZSetMap::partition(z, |t: &TestTuple| t.left.abs() % 2);
    let s = to_string(&ps);
    assert_eq!("{0=>{(0,0)=>1,(2,0)=>2},1=>{(-1,0)=>1,(1,0)=>-1}}", s);
}
