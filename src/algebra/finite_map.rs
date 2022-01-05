/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) 2021 VMware, Inc
*/

//! This module implements groups that are finite maps
//! from values to a group.

use super::GroupValue;
use num::Zero;
use std::collections::hash_map::Entry;
use std::collections::{hash_map, HashMap};
use std::fmt::{Display, Formatter, Result};
use std::hash::Hash;
use std::ops::{Add, AddAssign, Neg};

////////////////////////////////////////////////////////
/// Finite map trait.
///
/// A finite map maps arbitrary values (comparable for equality)
/// to values in a group.
///
/// `DataType` - Type of values stored in finite map.
///
/// `ResultType` - Type of results.
pub trait FiniteMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    /// Find the value associated to the specified key
    fn lookup(&self, key: &DataType) -> ResultType;
    /// Return the set of values that are mapped to non-zero values.
    // FIXME: the return type is wrong, the result should be a more abstract iterator.
    fn support(&self) -> hash_map::Keys<'_, DataType, ResultType>;
    /// The size of the support
    fn size(&self) -> usize;
    /// Add one value `key` to the zset with the specified `weight`
    fn insert(&mut self, key: &DataType, weight: &ResultType);
    /// Create a map containing a singleton value.
    fn singleton(key: &DataType, value: &ResultType) -> Self;
}

/// A FiniteMap that has GroupValue as codomain is itself a group.
pub trait FiniteMapGroupValue<DataType, ResultType>:
    FiniteMap<DataType, ResultType> + GroupValue
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
}

#[derive(Debug, Clone)]
pub struct FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
{
    // Unfortunately I cannot just implement these traits for
    // HashMap since they conflict with some existing traits.
    // We maintain the invariant that the keys (and only these keys)
    // that have non-zero weights are in this map.
    pub value: HashMap<DataType, ResultType>,
}

impl<DataType, ResultType> FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    /// Allocate an empty FiniteHashMap
    pub fn new() -> Self {
        FiniteHashMap::default()
    }
    /// Allocate an empty FiniteHashMap that is expected to hold 'size' values.
    pub fn with_capacity(size: usize) -> Self {
        FiniteHashMap::<DataType, ResultType> {
            value: HashMap::with_capacity(size),
        }
    }
}

impl<DataType, ResultType> FiniteMap<DataType, ResultType> for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    fn singleton(key: &DataType, value: &ResultType) -> FiniteHashMap<DataType, ResultType> {
        let mut result = FiniteHashMap::with_capacity(1);
        result.insert(key, value);
        result
    }
    fn lookup(&self, key: &DataType) -> ResultType {
        let val = self.value.get(key);
        match val {
            Some(weight) => weight.clone(),
            None => ResultType::zero(),
        }
    }
    fn support<'a>(&self) -> hash_map::Keys<'_, DataType, ResultType> {
        self.value.keys()
    }
    fn size(&self) -> usize {
        self.value.len()
    }
    fn insert(&mut self, key: &DataType, weight: &ResultType) {
        if weight.is_zero() {
            return;
        }
        let value = self.value.entry(key.clone());
        match value {
            Entry::Vacant(e) => {
                e.insert(weight.clone());
            }
            Entry::Occupied(mut e) => {
                let w = e.get().clone().add(weight.clone());
                if ResultType::is_zero(&w) {
                    e.remove_entry();
                } else {
                    e.insert(w);
                };
            }
        };
    }
}

impl<DataType, ResultType> Default for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
{
    fn default() -> Self {
        FiniteHashMap::<DataType, ResultType> {
            value: HashMap::default(),
        }
    }
}

impl<DataType, ResultType> Add for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    type Output = Self;

    fn add(self, other: Self) -> Self {
        let mut result = self;
        for (k, v) in other.value {
            let entry = result.value.entry(k);
            match entry {
                Entry::Vacant(e) => {
                    e.insert(v.clone());
                }
                Entry::Occupied(mut e) => {
                    let w = e.get().clone().add(v.clone());
                    if ResultType::is_zero(&w) {
                        e.remove_entry();
                    } else {
                        e.insert(w);
                    };
                }
            }
        }
        result
    }
}

impl<DataType, ResultType> AddAssign for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    fn add_assign(&mut self, other: Self) {
        for (k, v) in other.value {
            let entry = self.value.entry(k);
            match entry {
                Entry::Vacant(e) => {
                    e.insert(v.clone());
                }
                Entry::Occupied(mut e) => {
                    let w = e.get().clone().add(v.clone());
                    if ResultType::is_zero(&w) {
                        e.remove_entry();
                    } else {
                        e.insert(w);
                    };
                }
            }
        }
    }
}

impl<DataType, ResultType> Zero for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    fn zero() -> Self {
        FiniteHashMap::default()
    }
    fn is_zero(&self) -> bool {
        self.value.is_empty()
    }
}

impl<DataType, ResultType> Neg for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
    type Output = Self;

    fn neg(self) -> Self {
        let mut result = self;
        for val in result.value.values_mut() {
            *val = -val.clone();
        }
        result
    }
}

impl<DataType, WeightType> PartialEq for FiniteHashMap<DataType, WeightType>
where
    DataType: Clone + Hash + Eq + 'static,
    WeightType: GroupValue,
{
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl<DataType, ResultType> Eq for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static,
    ResultType: GroupValue,
{
}

/// This class knows how to display a FiniteMap to a string, but only
/// if the map keys support comparison
impl<DataType, ResultType> Display for FiniteHashMap<DataType, ResultType>
where
    DataType: Clone + Hash + Eq + 'static + Display + Ord,
    ResultType: GroupValue + Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let mut vec: Vec<DataType> = self.support().cloned().collect();
        vec.sort_by(DataType::cmp);
        write!(f, "{{")?;

        let mut first = true;
        for k in vec {
            if !first {
                write!(f, ",")?;
            } else {
                first = false;
            }
            let val = self.lookup(&k);
            write!(f, "{}", k)?;
            write!(f, "=>")?;
            write!(f, "{}", val)?;
        }
        write!(f, "}}")
    }
}

// zset to string
pub fn to_string<DataType, ResultType>(z: &FiniteHashMap<DataType, ResultType>) -> String
where
    DataType: Clone + 'static + Hash + Display + Ord,
    ResultType: GroupValue + Display,
{
    format!("{}", &z)
}

#[test]
fn hashmap_tests() {
    let mut z = FiniteHashMap::<i64, i64>::with_capacity(5);
    assert_eq!(0, z.size());
    assert_eq!("{}", to_string(&z));
    assert_eq!(0, z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, FiniteHashMap::<i64, i64>::zero());
    assert!(z.is_zero());
    let z2 = FiniteHashMap::<i64, i64>::new();
    assert_eq!(z, z2);

    z.insert(&0, &1);
    assert_eq!(1, z.size());
    assert_eq!("{0=>1}", to_string(&z));
    assert_eq!(1, z.lookup(&0));
    assert_eq!(0, z.lookup(&1));
    assert_ne!(z, FiniteHashMap::<i64, i64>::zero());
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
