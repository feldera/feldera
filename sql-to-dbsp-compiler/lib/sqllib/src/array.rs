// Array operations

use crate::{some_function2, some_generic_function2, Weight};
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::Index;

pub fn element<T>(array: Vec<T>) -> Option<T>
where
    T: Clone,
{
    if array.is_empty() {
        None
    } else if array.len() == 1 {
        Some(array[0].clone())
    } else {
        panic!("'ELEMENT()' called on array that does not have exactly 1 element");
    }
}

pub fn elementN<T>(array: Option<Vec<T>>) -> Option<T>
where
    T: Clone,
{
    let array = array?;
    element(array)
}

pub fn cardinality<T>(value: Vec<T>) -> i32 {
    value.len() as i32
}

pub fn cardinalityN<T>(value: Option<Vec<T>>) -> Option<i32> {
    let value = value?;
    Some(value.len() as i32)
}

// 8 versions of index, depending on
// nullability of vector
// nullability of vector element
// nullability of index

pub fn index___<T>(value: Vec<T>, index: usize) -> Option<T>
where
    T: Clone,
{
    if index >= value.len() {
        None
    } else {
        Some(value.index(index).clone())
    }
}

pub fn index___N<T>(value: Vec<T>, index: Option<usize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    index___(value, index)
}

pub fn index__N_<T>(value: Vec<Option<T>>, index: usize) -> Option<T>
where
    T: Clone,
{
    if index >= value.len() {
        None
    } else {
        value.index(index).clone()
    }
}

pub fn index__N_N<T>(value: Vec<Option<T>>, index: Option<usize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    if index >= value.len() {
        None
    } else {
        value.index(index).clone()
    }
}

pub fn index_N__<T>(value: Option<Vec<T>>, index: usize) -> Option<T>
where
    T: Clone,
{
    match value {
        None => None,
        Some(value) => index___(value, index),
    }
}

pub fn index_N__N<T>(value: Option<Vec<T>>, index: Option<usize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    match value {
        None => None,
        Some(value) => index___(value, index),
    }
}

pub fn index_N_N_<T>(value: Option<Vec<Option<T>>>, index: usize) -> Option<T>
where
    T: Clone,
{
    match value {
        None => None,
        Some(value) => index__N_(value, index),
    }
}

pub fn index_N_N_N<T>(value: Option<Vec<Option<T>>>, index: Option<usize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    match value {
        None => None,
        Some(value) => index__N_(value, index),
    }
}

pub fn array<T>() -> Vec<T> {
    vec![]
}

pub fn limit<T>(vector: &[T], limit: usize) -> Vec<T>
where
    T: Clone,
{
    vector[0..limit].to_vec()
}

pub fn map<T, S, F>(vector: &[T], func: F) -> Vec<S>
where
    F: FnMut(&T) -> S,
{
    vector.iter().map(func).collect()
}

pub fn array_append<T>(mut vector: Vec<T>, value: T) -> Vec<T> {
    vector.push(value);
    vector
}

pub fn array_appendN<T>(vector: Option<Vec<T>>, value: T) -> Option<Vec<T>> {
    Some(array_append(vector?, value))
}

pub fn array_repeat<T>(element: T, count: i32) -> Vec<T>
where
    T: Clone,
{
    std::iter::repeat(element)
        .take(usize::try_from(count).unwrap_or(0))
        .collect()
}

pub fn array_repeatN<T>(element: T, count: Option<i32>) -> Option<Vec<T>>
where
    T: Clone,
{
    Some(array_repeat(element, count?))
}

pub fn array_remove__<T>(mut vector: Vec<T>, element: T) -> Vec<T>
where
    T: Eq,
{
    vector.retain(|v| v != &element);
    vector
}

some_generic_function2!(array_remove, T, Vec<T>, T, Eq, Vec<T>);

pub fn array_position__<T>(vector: Vec<T>, element: T) -> i64
where
    T: Eq,
{
    vector
        .into_iter()
        .position(|x| x == element)
        .map(|v| v + 1)
        .unwrap_or(0) as i64
}

some_generic_function2!(array_position, T, Vec<T>, T, Eq, i64);

pub fn array_reverse_<T>(vector: Vec<T>) -> Vec<T> {
    vector.into_iter().rev().collect()
}

pub fn array_reverseN<T>(vector: Option<Vec<T>>) -> Option<Vec<T>> {
    Some(array_reverse_(vector?))
}

pub fn sort_array<T>(mut vector: Vec<T>, ascending: bool) -> Vec<T>
where
    T: Ord,
{
    if ascending {
        vector.sort()
    } else {
        vector.sort_by(|a, b| b.cmp(a))
    };

    vector
}

pub fn sort_arrayN<T>(vector: Option<Vec<T>>, ascending: bool) -> Option<Vec<T>>
where
    T: Ord,
{
    Some(sort_array(vector?, ascending))
}

pub fn array_max__<T>(vector: Vec<T>) -> Option<T>
where
    T: Ord,
{
    vector.into_iter().max()
}

pub fn array_maxN_<T>(vector: Option<Vec<T>>) -> Option<T>
where
    T: Ord,
{
    array_max__(vector?)
}

pub fn array_max_N<T>(vector: Vec<Option<T>>) -> Option<T>
where
    T: Ord,
{
    vector.into_iter().flatten().max()
}

pub fn array_maxNN<T>(vector: Option<Vec<Option<T>>>) -> Option<T>
where
    T: Ord,
{
    array_max_N(vector?)
}

pub fn array_min__<T>(vector: Vec<T>) -> Option<T>
where
    T: Ord,
{
    vector.into_iter().min()
}

pub fn array_minN_<T>(vector: Option<Vec<T>>) -> Option<T>
where
    T: Ord,
{
    array_min__(vector?)
}

pub fn array_min_N<T>(vector: Vec<Option<T>>) -> Option<T>
where
    T: Ord,
{
    vector.into_iter().flatten().min()
}

pub fn array_minNN<T>(vector: Option<Vec<Option<T>>>) -> Option<T>
where
    T: Ord,
{
    array_min_N(vector?)
}

pub fn array_compact_<T>(vector: Vec<Option<T>>) -> Vec<T> {
    vector.into_iter().flatten().collect()
}

pub fn array_compact_N<T>(vector: Option<Vec<Option<T>>>) -> Option<Vec<T>> {
    Some(array_compact_(vector?))
}

pub fn array_prepend<T>(mut vector: Vec<T>, value: T) -> Vec<T> {
    vector.insert(0, value);
    vector
}

pub fn array_prependN<T>(vector: Option<Vec<T>>, value: T) -> Option<Vec<T>> {
    Some(array_prepend(vector?, value))
}

pub fn array_contains__<T>(vector: Vec<T>, element: T) -> bool
where
    T: Eq,
{
    vector.contains(&element)
}

some_generic_function2!(array_contains, T, Vec<T>, T, Eq, bool);

pub fn array_distinct<T>(mut vector: Vec<T>) -> Vec<T>
where
    T: Eq + Hash + Clone,
{
    let mut hset: HashSet<T> = HashSet::new();
    vector.retain(|v| hset.insert(v.clone()));
    vector
}

pub fn array_distinctN<T>(vector: Option<Vec<T>>) -> Option<Vec<T>>
where
    T: Eq + Hash + Clone,
{
    Some(array_distinct(vector?))
}

pub fn sequence__(start: i32, end: i32) -> Vec<i32> {
    (start..=end).collect()
}

some_function2!(sequence, i32, i32, Vec<i32>);

// translated from the Calcite implementation to match the behavior
pub fn arrays_overlapNvec_Nvec_<T>(first: Vec<Option<T>>, second: Vec<Option<T>>) -> Option<bool>
where
    T: Eq + Hash,
{
    if first.len() > second.len() {
        return arrays_overlapNvec_Nvec_(second, first);
    }

    let (smaller, bigger) = (first, second);
    let mut has_null = false;

    if !smaller.is_empty() && !bigger.is_empty() {
        let mut shset: HashSet<Option<T>> = HashSet::from_iter(smaller);
        has_null = shset.remove(&None);

        for element in bigger {
            if element.is_none() {
                has_null = true;
            } else if shset.contains(&element) {
                return Some(true);
            }
        }
    }
    if has_null {
        None
    } else {
        Some(true)
    }
}

pub fn arrays_overlapNvec_NvecN<T>(
    first: Vec<Option<T>>,
    second: Option<Vec<Option<T>>>,
) -> Option<bool>
where
    T: Eq + Hash,
{
    arrays_overlapNvec_Nvec_(first, second?)
}

pub fn arrays_overlapNvecNNvecN<T>(
    first: Option<Vec<Option<T>>>,
    second: Option<Vec<Option<T>>>,
) -> Option<bool>
where
    T: Eq + Hash,
{
    arrays_overlapNvec_Nvec_(first?, second?)
}

pub fn arrays_overlapNvecNNvec_<T>(
    first: Option<Vec<Option<T>>>,
    second: Vec<Option<T>>,
) -> Option<bool>
where
    T: Eq + Hash,
{
    arrays_overlapNvec_Nvec_(first?, second)
}

pub fn arrays_overlap_vec__vec_<T>(first: Vec<T>, second: Vec<T>) -> bool
where
    T: Eq + Hash,
{
    let first: HashSet<T> = HashSet::from_iter(first);
    let second: HashSet<T> = HashSet::from_iter(second);

    first.intersection(&second).count() != 0
}

pub fn arrays_overlap_vecN_vecN<T>(first: Option<Vec<T>>, second: Option<Vec<T>>) -> Option<bool>
where
    T: Eq + Hash,
{
    Some(arrays_overlap_vec__vec_(first?, second?))
}

pub fn arrays_overlap_vec__vecN<T>(first: Vec<T>, second: Option<Vec<T>>) -> Option<bool>
where
    T: Eq + Hash,
{
    Some(arrays_overlap_vec__vec_(first, second?))
}

pub fn arrays_overlap_vecN_vec_<T>(first: Option<Vec<T>>, second: Vec<T>) -> Option<bool>
where
    T: Eq + Hash,
{
    Some(arrays_overlap_vec__vec_(first?, second))
}

pub fn array_agg<T>(accumulator: &mut Vec<T>, value: T, weight: Weight, distinct: bool) -> Vec<T>
where
    T: Clone,
{
    if weight < 0 {
        panic!("Negative weight {:?}", weight);
    }
    if distinct {
        accumulator.push(value.clone())
    } else {
        for _i in 0..weight {
            accumulator.push(value.clone())
        }
    }
    accumulator.to_vec()
}

pub fn array_agg_opt<T>(
    accumulator: &mut Vec<Option<T>>,
    value: Option<T>,
    weight: Weight,
    distinct: bool,
    ignore_nulls: bool,
) -> Vec<Option<T>>
where
    T: Clone,
{
    if ignore_nulls && value.is_none() {
        accumulator.to_vec()
    } else {
        array_agg(accumulator, value, weight, distinct)
    }
}
