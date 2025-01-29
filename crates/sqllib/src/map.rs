//! Functions for manipulating maps

use crate::{ConcatSemigroup, Semigroup, Weight};
use dbsp::utils::Tup2;
use std::collections::BTreeMap;
use std::sync::Arc;

pub type Map<K, V> = Arc<BTreeMap<K, V>>;

#[doc(hidden)]
fn insert_or_keep_largest<K, V>(map: &mut BTreeMap<K, V>, key: &K, value: &V)
where
    K: Ord + Clone,
    V: Ord + Clone,
{
    map.entry(key.clone())
        .and_modify(|e| {
            if value > e {
                *e = value.clone();
            }
        })
        .or_insert(value.clone());
}

#[doc(hidden)]
impl<K, V> Semigroup<BTreeMap<K, V>> for ConcatSemigroup<BTreeMap<K, V>>
where
    K: Clone + Ord,
    V: Clone + Ord,
{
    #[doc(hidden)]
    fn combine(left: &BTreeMap<K, V>, right: &BTreeMap<K, V>) -> BTreeMap<K, V> {
        let mut result: BTreeMap<K, V> = left.clone();
        for (k, v) in right {
            insert_or_keep_largest(&mut result, k, v);
        }
        result
    }
}

#[doc(hidden)]
impl<K, V> Semigroup<Option<BTreeMap<K, V>>> for ConcatSemigroup<Option<BTreeMap<K, V>>>
where
    K: Clone + Ord,
    V: Clone + Ord,
{
    #[doc(hidden)]
    fn combine(
        left: &Option<BTreeMap<K, V>>,
        right: &Option<BTreeMap<K, V>>,
    ) -> Option<BTreeMap<K, V>> {
        match (left, right) {
            (None, _) => right.clone(),
            (_, None) => left.clone(),
            (Some(left), Some(right)) => {
                Some(ConcatSemigroup::<BTreeMap<K, V>>::combine(left, right))
            }
        }
    }
}

#[doc(hidden)]
pub fn map_map__<K, V0, V1, F>(map: &Map<K, V0>, f: F) -> Map<K, V1>
where
    K: Ord + Clone,
    F: Fn(&V0) -> V1,
{
    let result: BTreeMap<K, V1> = (**map)
        .iter()
        .map(move |(key, value)| (key.clone(), f(value)))
        .collect();
    result.into()
}

#[doc(hidden)]
pub fn map_mapN_<K, V0, V1, F>(map: &Option<Map<K, V0>>, f: F) -> Option<Map<K, V1>>
where
    K: Ord + Clone,
    F: Fn(&V0) -> V1,
{
    map.as_ref().map(|map| map_map__(map, f))
}

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
pub fn map_index___<I, T>(value: &Map<I, T>, map_index: I) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    value.get(&map_index).cloned()
}

#[doc(hidden)]
pub fn map_index__N<I, T>(value: &Map<I, T>, map_index: Option<I>) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    let map_index = map_index?;
    map_index___(value, map_index)
}

#[doc(hidden)]
pub fn map_index_N_<I, T>(value: &Map<I, Option<T>>, map_index: I) -> Option<T>
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
pub fn map_index_NN<I, T>(value: &Map<I, Option<T>>, map_index: Option<I>) -> Option<T>
where
    I: Ord,
    T: Clone,
{
    let map_index = map_index?;
    map_index_N_(value, map_index)
}

#[doc(hidden)]
pub fn map_indexN__<I, T>(value: &Option<Map<I, T>>, map_index: I) -> Option<T>
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
pub fn map_indexN_N<I, T>(value: &Option<Map<I, T>>, map_index: Option<I>) -> Option<T>
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
pub fn map_indexNN_<I, T>(value: &Option<Map<I, Option<T>>>, map_index: I) -> Option<T>
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
pub fn map_indexNNN<I, T>(value: &Option<Map<I, Option<T>>>, map_index: Option<I>) -> Option<T>
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
pub fn cardinalityMap<I, T>(value: &Map<I, T>) -> i32 {
    value.len() as i32
}

#[doc(hidden)]
pub fn cardinalityMapN<I, T>(value: &Option<Map<I, T>>) -> Option<i32> {
    value.as_ref().map(|map| cardinalityMap(map))
}

#[doc(hidden)]
pub fn map_contains_key__<I, T>(value: &Map<I, T>, key: I) -> bool
where
    I: Ord,
    T: Clone,
{
    value.contains_key(&key)
}

#[doc(hidden)]
pub fn map_contains_keyN_<I, T>(value: &Option<Map<I, T>>, key: I) -> Option<bool>
where
    I: Ord,
    T: Clone,
{
    value.as_ref().map(|map| map_contains_key__(map, key))
}

#[doc(hidden)]
pub fn map_contains_keyNN<I, T>(value: &Option<Map<I, T>>, key: Option<I>) -> Option<bool>
where
    I: Ord,
    T: Clone,
{
    let key = key?;
    map_contains_keyN_(value, key)
}

#[doc(hidden)]
pub fn map_contains_key_N<I, T>(value: &Map<I, T>, key: Option<I>) -> Option<bool>
where
    I: Ord,
    T: Clone,
{
    let key = key?;
    Some(map_contains_key__(value, key))
}
