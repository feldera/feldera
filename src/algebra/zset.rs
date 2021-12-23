/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module implements ZSets.
//! A ZSet maps arbitrary keys (which only need to support equality and hashing)
//! to weights.  The weights must implement the ZRingValue trait.

use super::*;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{hash_map, HashMap};
use std::hash::Hash;
use string_builder::Builder;

////////////////////////////////////////////////////////
/// Z-set trait.
///
/// A Z-set is a set where each element has a weight.
/// Weights belong to some ring.
///
/// `DataType` - Type of values stored in Z-set
///
/// `WeightType` - Type of weights.  Must be a value from a Ring.
pub trait ZSet<DataType, WeightType>: GroupValue
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    //type KeyIterator: Iterator<Item=DataType>;

    /// Find the weight associated to the specified key
    fn lookup(&self, key: &DataType) -> WeightType;
    /// Multiply each value in the other Z-set by `weight`
    /// and add it to this set.  This is similar to `mul_add_assign`, but
    /// not the same, since `mul_add_assign` multiplies `self`, whereas
    /// this trait multiplies `other`.
    fn add_assign_weighted(&mut self, weight: &WeightType, other: &Self);
    // FIXME: the return type is wrong, the result should be a more abstract iterator.
    fn support(&self) -> hash_map::Keys<'_, DataType, WeightType>;
    /// Returns a Z-set that contains all elements with positive weights from `self`
    /// with weights set to 1.
    fn distinct(&self) -> Self;
    /// Add one value `key` to the zset with the specified `weight`
    fn insert(&mut self, key: DataType, weight: &WeightType);
    /// The size of the support
    fn size(&self) -> usize;
}

trait ZSetPrinter<DataType, WeightType, ZS, C>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
    ZS: ZSet<DataType, WeightType>,
    C: FnMut(&DataType, &DataType) -> Ordering,
{
    /// Write a string representation of set `z` into an internal builder.
    /// The `elem_compare` function is used to sort keys to get a deterministic output.
    fn serialize(&mut self, z: &ZS, elem_compare: C);
}

///////////////////////////////////////////////////////
/// Implementation of ZSets in terms of HashMaps
#[derive(Debug, Clone)]
struct ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    map: HashMap<DataType, WeightType>,
}

impl<DataType, WeightType> ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    /// Allocate an empty ZSetHashMap
    fn new() -> Self {
        ZSetHashMap::default()
    }
    /// Allocate an empty ZSetHashMap that is expected to hold 'size' values.
    pub fn with_capacity(size: usize) -> Self {
        ZSetHashMap::<DataType, WeightType> {
            map: HashMap::with_capacity(size),
        }
    }
}

impl<DataType, WeightType> Default for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    fn default() -> Self {
        ZSetHashMap::<DataType, WeightType> {
            map: HashMap::default(),
        }
    }
}

impl<DataType, WeightType> PartialEq for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    fn eq(&self, other: &Self) -> bool {
        self.map.eq(&other.map)
    }
}

impl<DataType, WeightType> Eq for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
}

impl<DataType, WeightType> Neg for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    type Output = Self;
    fn neg(self) -> Self::Output {
        let mut result = ZSetHashMap::<DataType, WeightType>::with_capacity(self.size());
        for (key, value) in self.map {
            result.map.insert(key, value.neg());
        }
        result
    }
}

impl<DataType, WeightType> Add for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    type Output = Self;
    fn add(mut self, value: Self) -> Self {
        self.add_assign_weighted(&WeightType::one(), &value);
        self
    }
}

impl<DataType, WeightType> AddAssign for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    fn add_assign(&mut self, value: Self) {
        self.add_assign_weighted(&WeightType::one(), &value);
    }
}

impl<DataType, WeightType> Zero for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    fn is_zero(&self) -> bool {
        self.size() == 0
    }
    fn zero() -> Self {
        ZSetHashMap::new()
    }
}

impl<DataType, WeightType> ZSet<DataType, WeightType> for ZSetHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: ZRingValue,
{
    /// Look up the weight of the 'key'.  
    fn lookup(&self, key: &DataType) -> WeightType {
        let val = self.map.get(key);
        match val {
            Some(weight) => weight.clone(),
            None => WeightType::zero(),
        }
    }
    fn distinct(&self) -> Self {
        let mut result = Self::new();
        for (key, value) in &self.map {
            if value.ge0() {
                result.insert(key.clone(), value);
            }
        }
        result
    }
    fn add_assign_weighted(&mut self, weight: &WeightType, other: &Self) {
        if WeightType::is_zero(weight) {
            return;
        }
        for (key, value) in &other.map {
            let new_weight = value.clone().mul(weight.clone());
            self.insert(key.clone(), &new_weight);
        }
    }
    fn support<'a>(&self) -> hash_map::Keys<'_, DataType, WeightType> {
        self.map.keys()
    }
    fn size(&self) -> usize {
        self.map.len()
    }
    fn insert(&mut self, key: DataType, weight: &WeightType) {
        if weight.is_zero() {
            return;
        }
        let value = self.map.entry(key);
        match value {
            Entry::Vacant(e) => {
                e.insert(weight.clone());
            }
            Entry::Occupied(mut e) => {
                let w = e.get().clone().add(weight.clone());
                if WeightType::is_zero(&w) {
                    e.remove_entry();
                } else {
                    e.insert(w);
                };
            }
        };
    }
}

struct Printer {
    builder: Builder,
}

/// This class knows how to convert a ZSet to a string.
/// It does this by using a comparator which can sort the elements of
/// the ZSet in a canonical order.  This makes testing much simpler
/// since printing ZSets using this class gives a deterministic result.
/// This also makes the requirements on ZSet lesser, since we can
/// have ZSets that do not support Cmp on keys.
impl<DataType, WeightType, ZS, C> ZSetPrinter<DataType, WeightType, ZS, C> for Printer
where
    DataType: Clone + Hash + Eq + 'static + Display,
    WeightType: ZRingValue + Display,
    ZS: ZSet<DataType, WeightType>,
    C: FnMut(&DataType, &DataType) -> Ordering,
{
    /// Write the ZSet string representation into the internal builder.
    /// 'elem_compare' is a function that can compare two ZSet keys.
    /// It is used to sort the Zset keys prior to writing into the builder.
    fn serialize(&mut self, z: &ZS, elem_compare: C) {
        let mut vec: Vec<DataType> = z.support().cloned().collect();
        vec.sort_by(elem_compare);
        self.builder.append("{");

        let mut first = true;
        for k in vec {
            if !first {
                self.builder.append(",");
            } else {
                first = false;
            }
            let val = z.lookup(&k);
            let kf = format!("{}", k);
            self.builder.append(kf);
            self.builder.append("=>");
            let vf = format!("{}", val);
            self.builder.append(vf);
        }
        self.builder.append("}");
    }
}

#[cfg(test)]
impl Printer {
    pub fn new() -> Printer {
        Printer {
            builder: Builder::new(10),
        }
    }
    pub fn to_string(self) -> String {
        self.builder.string().unwrap()
    }
    pub fn serialize_set<DataType, WeightType, ZS>(&mut self, z: &ZS)
    where
        DataType: Clone + 'static + Hash + Display + Ord,
        WeightType: ZRingValue + Display,
        ZS: ZSet<DataType, WeightType>,
    {
        self.serialize(z, DataType::cmp);
    }
}

#[cfg(test)]
mod zset_tests {
    use super::*;

    // zset to string
    fn to_string<DataType, WeightType, ZS>(z: &ZS) -> String
    where
        DataType: Clone + 'static + Hash + Display + Ord,
        WeightType: ZRingValue + Display,
        ZS: ZSet<DataType, WeightType>,
    {
        let mut pr = Printer::new();
        pr.serialize_set(z);
        pr.to_string()
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

        z.insert(0, &1);
        assert_eq!(1, z.size());
        assert_eq!("{0=>1}", to_string(&z));
        assert_eq!(1, z.lookup(&0));
        assert_eq!(0, z.lookup(&1));
        assert_ne!(z, ZSetHashMap::<i64, i64>::zero());
        assert_eq!(false, z.is_zero());

        z.insert(2, &0);
        assert_eq!(1, z.size());
        assert_eq!("{0=>1}", to_string(&z));

        z.insert(1, &-1);
        assert_eq!(2, z.size());
        assert_eq!("{0=>1,1=>-1}", to_string(&z));

        z.insert(-1, &1);
        assert_eq!(3, z.size());
        assert_eq!("{-1=>1,0=>1,1=>-1}", to_string(&z));

        let d = z.distinct();
        assert_eq!(2, d.size());
        assert_eq!("{-1=>1,0=>1}", to_string(&d));

        let d = z.clone().neg();
        assert_eq!(3, d.size());
        assert_eq!("{-1=>-1,0=>-1,1=>1}", to_string(&d));
        assert_ne!(d, z);

        z.insert(1, &1);
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

        z.insert(0, &CheckedI64::from(1));
        assert_eq!(1, z.size());
        assert_eq!("{0=>1}", to_string(&z));
        assert_eq!(CheckedI64::from(1), z.lookup(&0));
        assert_eq!(CheckedI64::from(0), z.lookup(&1));
        assert_ne!(z, ZSetHashMap::<i64, CheckedI64>::zero());
        assert_eq!(false, z.is_zero());

        z.insert(2, &CheckedI64::from(0));
        assert_eq!(1, z.size());
        assert_eq!("{0=>1}", to_string(&z));

        z.insert(1, &CheckedI64::from(-1));
        assert_eq!(2, z.size());
        assert_eq!("{0=>1,1=>-1}", to_string(&z));

        z.insert(-1, &CheckedI64::from(1));
        assert_eq!(3, z.size());
        assert_eq!("{-1=>1,0=>1,1=>-1}", to_string(&z));

        let d = z.distinct();
        assert_eq!(2, d.size());
        assert_eq!("{-1=>1,0=>1}", to_string(&d));

        let d = z.clone().neg();
        assert_eq!(3, d.size());
        assert_eq!("{-1=>-1,0=>-1,1=>1}", to_string(&d));
        assert_ne!(d, z);

        z.insert(1, &CheckedI64::from(1));
        assert_eq!(2, z.size());
        assert_eq!("{-1=>1,0=>1}", to_string(&z));

        let mut z2 = z.clone().add(z.clone());
        assert_eq!(2, z2.size());
        assert_eq!("{-1=>2,0=>2}", to_string(&z2));

        z2.add_assign(z.clone());
        assert_eq!(2, z2.size());
        assert_eq!("{-1=>3,0=>3}", to_string(&z2));
    }

    #[derive(Debug, Clone, Hash, Eq, PartialEq)]
    struct TestTuple {
        left: i64,
        right: i64,
    }

    impl TestTuple {
        fn new(left: i64, right: i64) -> Self {
            Self { left, right }
        }
    }

    impl Display for TestTuple {
        fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
            write!(f, "({},{})", self.left, self.right)
        }
    }

    impl PartialOrd for TestTuple {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            let lo = self.left.cmp(&other.left);
            if lo != Ordering::Equal {
                return Some(lo);
            }
            Some(self.right.cmp(&other.right))
        }
    }

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

        z.insert(TestTuple::new(0, 0), &CheckedI64::from(1));
        assert_eq!(1, z.size());
        assert_eq!("{(0,0)=>1}", to_string(&z));
        assert_eq!(CheckedI64::from(1), z.lookup(&TestTuple::new(0, 0)));
        assert_eq!(CheckedI64::from(0), z.lookup(&TestTuple::new(0, 1)));
        assert_ne!(z, ZSetHashMap::<TestTuple, CheckedI64>::zero());
        assert_eq!(false, z.is_zero());

        z.insert(TestTuple::new(2, 0), &CheckedI64::from(0));
        assert_eq!(1, z.size());
        assert_eq!("{(0,0)=>1}", to_string(&z));

        z.insert(TestTuple::new(1, 0), &CheckedI64::from(-1));
        assert_eq!(2, z.size());
        assert_eq!("{(0,0)=>1,(1,0)=>-1}", to_string(&z));

        z.insert(TestTuple::new(-1, 0), &CheckedI64::from(1));
        assert_eq!(3, z.size());
        assert_eq!("{(-1,0)=>1,(0,0)=>1,(1,0)=>-1}", to_string(&z));

        let d = z.distinct();
        assert_eq!(2, d.size());
        assert_eq!("{(-1,0)=>1,(0,0)=>1}", to_string(&d));

        let d = z.clone().neg();
        assert_eq!(3, d.size());
        assert_eq!("{(-1,0)=>-1,(0,0)=>-1,(1,0)=>1}", to_string(&d));
        assert_ne!(d, z);

        z.insert(TestTuple::new(1, 0), &CheckedI64::from(1));
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
}
