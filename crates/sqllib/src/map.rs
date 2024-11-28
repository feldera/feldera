//! Functions for manipulating maps

use crate::{insert_or_keep_largest, Weight};
use dbsp::utils::Tup2;
use std::collections::BTreeMap;

#[doc(hidden)]
pub fn map_agg<K, V>(
    accumulator: &mut BTreeMap<K, V>,
    value: Tup2<K, V>,
    weight: Weight,
) -> BTreeMap<K, V>
where
    K: Clone + Ord,
    V: Clone + Ord,
{
    if weight < 0 {
        panic!("Negative weight {:?}", weight);
    }
    let k = value.0;
    let v = value.1;
    insert_or_keep_largest(accumulator, &k, &v);
    accumulator.clone()
}

#[doc(hidden)]
pub fn map_aggN<K, V>(
    accumulator: &mut Option<BTreeMap<K, V>>,
    value: Tup2<K, V>,
    weight: Weight,
) -> Option<BTreeMap<K, V>>
where
    K: Clone + Ord,
    V: Clone + Ord,
{
    accumulator
        .as_mut()
        .map(|accumulator| map_agg(accumulator, value, weight))
}

/////////////////////////////////////////

// 8 versions of map_index, depending on
// nullability of map
// nullability of map value
// nullability of index

#[doc(hidden)]
pub fn map_index___<I, T>(value: &BTreeMap<I, T>, map_index: I) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    value.get(&map_index).cloned()
}

#[doc(hidden)]
pub fn map_index__N<I, T>(value: &BTreeMap<I, T>, map_index: Option<I>) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    let map_index = map_index?;
    map_index___(value, map_index)
}

#[doc(hidden)]
pub fn map_index_N_<I, T>(value: &BTreeMap<I, Option<T>>, map_index: I) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    match value.get(&map_index) {
        None => None,
        Some(result) => result.clone(),
    }
}

#[doc(hidden)]
pub fn map_index_NN<I, T>(value: &BTreeMap<I, Option<T>>, map_index: Option<I>) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    let map_index = map_index?;
    map_index_N_(value, map_index)
}

#[doc(hidden)]
pub fn map_indexN__<I, T>(value: &Option<BTreeMap<I, T>>, map_index: I) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    match value {
        None => None,
        Some(value) => map_index___(value, map_index),
    }
}

#[doc(hidden)]
pub fn map_indexN_N<I, T>(value: &Option<BTreeMap<I, T>>, map_index: Option<I>) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    let map_index = map_index?;
    match value {
        None => None,
        Some(value) => map_index___(value, map_index),
    }
}

#[doc(hidden)]
pub fn map_indexNN_<I, T>(value: &Option<BTreeMap<I, Option<T>>>, map_index: I) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    match value {
        None => None,
        Some(value) => map_index_N_(value, map_index),
    }
}

#[doc(hidden)]
pub fn map_indexNNN<I, T>(value: &Option<BTreeMap<I, Option<T>>>, map_index: Option<I>) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    let map_index = map_index?;
    match value {
        None => None,
        Some(value) => map_index_N_(value, map_index),
    }
}

#[doc(hidden)]
pub fn cardinalityMap<I, T>(value: &BTreeMap<I, T>) -> i32 {
    value.len() as i32
}

#[doc(hidden)]
pub fn cardinalityMapN<I, T>(value: &Option<BTreeMap<I, T>>) -> Option<i32> {
    value.as_ref().map(|map| cardinalityMap(map))
}

#[doc(hidden)]
pub fn map_contains_key__<I, T>(value: &BTreeMap<I, T>, key: I) -> bool
where
    I: Ord,
    T: Clone,
{
    value.contains_key(&key)
}

#[doc(hidden)]
pub fn map_contains_keyN_<I, T>(value: &Option<BTreeMap<I, T>>, key: I) -> Option<bool>
where
    I: Ord,
    T: Clone,
{
    value.as_ref().map(|map| map_contains_key__(map, key))
}

#[doc(hidden)]
pub fn map_contains_keyNN<I, T>(value: &Option<BTreeMap<I, T>>, key: Option<I>) -> Option<bool>
where
    I: Ord,
    T: Clone,
{
    let key = key?;
    map_contains_keyN_(value, key)
}

#[doc(hidden)]
pub fn map_contains_key_N<I, T>(value: &BTreeMap<I, T>, key: Option<I>) -> Option<bool>
where
    I: Ord,
    T: Clone,
{
    let key = key?;
    Some(map_contains_key__(value, key))
}
