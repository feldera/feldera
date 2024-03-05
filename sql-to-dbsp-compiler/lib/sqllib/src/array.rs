// Array operations

use crate::some_generic_function2;
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

pub fn index__<T>(value: &Vec<T>, index: usize) -> T
where
    T: Clone,
{
    (*value.index(index)).clone()
}

pub fn index__N<T>(value: &Vec<T>, index: Option<usize>) -> T
where
    T: Clone,
{
    index__(value, index.unwrap())
}

pub fn index_N_<T>(value: &Option<Vec<T>>, index: usize) -> Option<T>
where
    T: Clone,
{
    value.as_ref().map(|value| index__(value, index))
}

pub fn index_N_N<T>(value: &Option<Vec<T>>, index: Option<usize>) -> Option<T>
where
    T: Clone,
{
    value.as_ref().map(|value| index__N(value, index))
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
