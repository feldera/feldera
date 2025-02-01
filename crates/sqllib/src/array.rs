// Array operations

use crate::{some_function2, some_generic_function2, ConcatSemigroup, Semigroup, Weight};
use itertools::Itertools;
use std::{collections::HashSet, fmt::Debug, hash::Hash, ops::Index, sync::Arc};

pub type Array<T> = Arc<Vec<T>>;

/// Convert a Rust Vec to a SQL Array
pub fn to_array<T>(data: Vec<T>) -> Array<T> {
    Arc::new(data)
}

#[doc(hidden)]
pub fn to_arrayN<T>(data: Option<Vec<T>>) -> Option<Array<T>> {
    data.map(|data| to_array(data))
}

/// Convert a SQL array to a Rust Vec
pub fn to_vec<T>(data: Array<T>) -> Vec<T>
where
    T: Clone,
{
    (*data).clone()
}

#[doc(hidden)]
impl<V> Semigroup<Vec<V>> for ConcatSemigroup<Vec<V>>
where
    V: Clone + Ord,
{
    #[doc(hidden)]
    fn combine(left: &Vec<V>, right: &Vec<V>) -> Vec<V> {
        left.iter().merge(right).cloned().collect()
    }
}

#[doc(hidden)]
impl<V> Semigroup<Option<Vec<V>>> for ConcatSemigroup<Option<Vec<V>>>
where
    V: Clone + Ord,
{
    #[doc(hidden)]
    fn combine(left: &Option<Vec<V>>, right: &Option<Vec<V>>) -> Option<Vec<V>> {
        match (left, right) {
            (None, _) => right.clone(),
            (_, None) => left.clone(),
            (Some(left), Some(right)) => Some(left.iter().merge(right).cloned().collect()),
        }
    }
}

#[doc(hidden)]
pub fn array_map__<T, S, F>(vec: Array<T>, f: F) -> Array<S>
where
    F: Fn(&T) -> S,
{
    (*vec).iter().map(f).collect::<Vec<S>>().into()
}

#[doc(hidden)]
pub fn array_mapN_<T, S, F>(vec: Option<Array<T>>, f: F) -> Option<Array<S>>
where
    F: Fn(&T) -> S,
{
    vec.as_ref().map(|vec| array_map__(vec.clone(), f))
}

#[doc(hidden)]
pub fn element__<T>(array: Array<T>) -> Option<T>
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

#[doc(hidden)]
pub fn elementN_<T>(array: Option<Array<T>>) -> Option<T>
where
    T: Clone,
{
    let array = array?;
    element__(array)
}

#[doc(hidden)]
pub fn element_N<T>(array: Array<Option<T>>) -> Option<T>
where
    T: Clone,
{
    if array.is_empty() {
        None
    } else if array.len() == 1 {
        array[0].clone()
    } else {
        panic!("'ELEMENT()' called on array that does not have exactly 1 element");
    }
}

#[doc(hidden)]
pub fn elementNN<T>(array: Option<Array<Option<T>>>) -> Option<T>
where
    T: Clone,
{
    let array = array?;
    element_N(array)
}

#[doc(hidden)]
pub fn cardinalityVec<T>(value: Array<T>) -> i32 {
    value.len() as i32
}

#[doc(hidden)]
pub fn cardinalityVecN<T>(value: Option<Array<T>>) -> Option<i32> {
    value.as_ref().map(|value| value.len() as i32)
}

// 8 versions of index, depending on
// nullability of vector
// nullability of vector element
// nullability of index

#[doc(hidden)]
pub fn index___<T>(value: &Array<T>, index: isize) -> Option<T>
where
    T: Clone,
{
    if index < 0 {
        return None;
    };
    let index: usize = index as usize;
    if index >= value.len() {
        None
    } else {
        Some(value.index(index).clone())
    }
}

#[doc(hidden)]
pub fn index__N<T>(value: &Array<T>, index: Option<isize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    index___(value, index)
}

#[doc(hidden)]
pub fn index_N_<T>(value: &Array<Option<T>>, index: isize) -> Option<T>
where
    T: Clone,
{
    if index < 0 {
        return None;
    };
    let index: usize = index as usize;
    if index >= value.len() {
        None
    } else {
        value.index(index).clone()
    }
}

#[doc(hidden)]
pub fn index_NN<T>(value: &Array<Option<T>>, index: Option<isize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    if index < 0 {
        return None;
    };
    let index: usize = index as usize;
    if index >= value.len() {
        None
    } else {
        value.index(index).clone()
    }
}

#[doc(hidden)]
pub fn indexN__<T>(value: &Option<Array<T>>, index: isize) -> Option<T>
where
    T: Clone,
{
    match value {
        None => None,
        Some(value) => index___(value, index),
    }
}

#[doc(hidden)]
pub fn indexN_N<T>(value: &Option<Array<T>>, index: Option<isize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    match value {
        None => None,
        Some(value) => index___(value, index),
    }
}

#[doc(hidden)]
pub fn indexNN_<T>(value: &Option<Array<Option<T>>>, index: isize) -> Option<T>
where
    T: Clone,
{
    match value {
        None => None,
        Some(value) => index_N_(value, index),
    }
}

#[doc(hidden)]
pub fn indexNNN<T>(value: &Option<Array<Option<T>>>, index: Option<isize>) -> Option<T>
where
    T: Clone,
{
    let index = index?;
    match value {
        None => None,
        Some(value) => index_N_(value, index),
    }
}

#[doc(hidden)]
pub fn array<T>() -> Array<T> {
    vec![].into()
}

#[doc(hidden)]
pub fn limit<T>(vector: Array<T>, limit: usize) -> Array<T>
where
    T: Clone,
{
    let limit = std::cmp::min(limit, (*vector).len());
    (**vector)[0..limit].to_vec().into()
}

#[doc(hidden)]
pub fn map<T, S, F>(vector: Array<T>, func: F) -> Array<S>
where
    F: FnMut(&T) -> S,
{
    (*vector).iter().map(func).collect::<Vec<S>>().into()
}

#[doc(hidden)]
pub fn array_append<T>(vector: Array<T>, value: T) -> Array<T>
where
    T: Clone,
{
    let mut result = Arc::unwrap_or_clone(vector);
    result.push(value);
    result.into()
}

#[doc(hidden)]
pub fn array_appendN<T>(vector: Option<Array<T>>, value: T) -> Option<Array<T>>
where
    T: Clone,
{
    Some(array_append(vector?, value))
}

#[doc(hidden)]
pub fn array_repeat__<T>(element: T, count: i32) -> Array<T>
where
    T: Clone,
{
    std::iter::repeat(element)
        .take(usize::try_from(count).unwrap_or(0))
        .collect::<Vec<T>>()
        .into()
}

#[doc(hidden)]
pub fn array_repeatN_<T>(element: Option<T>, count: i32) -> Option<Array<Option<T>>>
where
    T: Clone,
{
    Some(array_repeat__(element, count))
}

#[doc(hidden)]
pub fn array_repeat_N<T>(element: T, count: Option<i32>) -> Option<Array<T>>
where
    T: Clone,
{
    Some(array_repeat__(element, count?))
}

#[doc(hidden)]
pub fn array_repeatNN<T>(element: Option<T>, count: Option<i32>) -> Option<Array<Option<T>>>
where
    T: Clone,
{
    Some(array_repeat__(element, count?))
}

#[doc(hidden)]
pub fn array_remove__<T>(vector: Array<T>, element: T) -> Array<T>
where
    T: Eq + Clone,
{
    let mut vec = Arc::unwrap_or_clone(vector);
    vec.retain(|v| v != &element);
    vec.into()
}

some_generic_function2!(array_remove, T, Array<T>, T, Eq + Clone, Array<T>);

#[doc(hidden)]
pub fn array_position__<T>(vector: Array<T>, element: T) -> i64
where
    T: Eq,
{
    (*vector)
        .iter()
        .position(|x| *x == element)
        .map(|v| v + 1)
        .unwrap_or(0) as i64
}

some_generic_function2!(array_position, T, Array<T>, T, Eq, i64);

#[doc(hidden)]
pub fn array_reverse_<T>(vector: Array<T>) -> Array<T>
where
    T: Clone,
{
    (**vector).iter().rev().cloned().collect::<Vec<T>>().into()
}

#[doc(hidden)]
pub fn array_reverseN<T>(vector: Option<Array<T>>) -> Option<Array<T>>
where
    T: Clone,
{
    Some(array_reverse_(vector?))
}

#[doc(hidden)]
pub fn sort_array<T>(vector: Array<T>, ascending: bool) -> Array<T>
where
    T: Ord + Clone,
{
    let mut data = (*vector).clone();
    if ascending {
        data.sort()
    } else {
        data.sort_by(|a, b| b.cmp(a))
    };
    data.into()
}

#[doc(hidden)]
pub fn sort_arrayN<T>(vector: Option<Array<T>>, ascending: bool) -> Option<Array<T>>
where
    T: Ord + Clone,
{
    Some(sort_array(vector?, ascending))
}

#[doc(hidden)]
pub fn array_max__<T>(vector: Array<T>) -> Option<T>
where
    T: Ord + Clone,
{
    (*vector).iter().cloned().max()
}

#[doc(hidden)]
pub fn array_maxN_<T>(vector: Option<Array<T>>) -> Option<T>
where
    T: Ord + Clone,
{
    array_max__(vector?)
}

#[doc(hidden)]
pub fn array_max_N<T>(vector: Array<Option<T>>) -> Option<T>
where
    T: Ord + Clone,
{
    (*vector).iter().flatten().cloned().max()
}

#[doc(hidden)]
pub fn array_maxNN<T>(vector: Option<Array<Option<T>>>) -> Option<T>
where
    T: Ord + Clone,
{
    array_max_N(vector?)
}

#[doc(hidden)]
pub fn array_min__<T>(vector: Array<T>) -> Option<T>
where
    T: Ord + Clone,
{
    (*vector).iter().cloned().min()
}

#[doc(hidden)]
pub fn array_minN_<T>(vector: Option<Array<T>>) -> Option<T>
where
    T: Ord + Clone,
{
    array_min__(vector?)
}

#[doc(hidden)]
pub fn array_min_N<T>(vector: Array<Option<T>>) -> Option<T>
where
    T: Ord + Clone,
{
    (*vector).iter().flatten().cloned().min()
}

#[doc(hidden)]
pub fn array_minNN<T>(vector: Option<Array<Option<T>>>) -> Option<T>
where
    T: Ord + Clone,
{
    array_min_N(vector?)
}

#[doc(hidden)]
pub fn array_compact_<T>(vector: Array<Option<T>>) -> Array<T>
where
    T: Clone,
{
    (*vector)
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<T>>()
        .into()
}

#[doc(hidden)]
pub fn array_compact_N<T>(vector: Option<Array<Option<T>>>) -> Option<Array<T>>
where
    T: Clone,
{
    Some(array_compact_(vector?))
}

#[doc(hidden)]
pub fn array_prepend<T>(vector: Array<T>, value: T) -> Array<T>
where
    T: Clone,
{
    let mut data = (*vector).clone();
    data.insert(0, value);
    data.into()
}

#[doc(hidden)]
pub fn array_prependN<T>(vector: Option<Array<T>>, value: T) -> Option<Array<T>>
where
    T: Clone,
{
    Some(array_prepend(vector?, value))
}

#[doc(hidden)]
pub fn array_contains__<T>(vector: Array<T>, element: T) -> bool
where
    T: Eq,
{
    vector.contains(&element)
}

some_generic_function2!(array_contains, T, Array<T>, T, Eq, bool);

#[doc(hidden)]
pub fn array_distinct<T>(vector: Array<T>) -> Array<T>
where
    T: Eq + Hash + Clone,
{
    let mut hset: HashSet<T> = HashSet::new();
    let data = (*vector)
        .iter()
        .filter(|v| hset.insert((*v).clone()))
        .cloned()
        .collect::<Vec<T>>();
    data.into()
}

#[doc(hidden)]
pub fn array_distinctN<T>(vector: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Hash + Clone,
{
    Some(array_distinct(vector?))
}

#[doc(hidden)]
pub fn sequence__(start: i32, end: i32) -> Array<i32> {
    (start..=end).collect::<Vec<i32>>().into()
}

some_function2!(sequence, i32, i32, Array<i32>);

#[doc(hidden)]
pub fn arrays_overlap__<T>(first: Array<T>, second: Array<T>) -> bool
where
    T: Eq + Hash,
{
    if first.len() > second.len() {
        return arrays_overlap__(second, first);
    }

    let (smaller, bigger) = (first, second);

    if !smaller.is_empty() && !bigger.is_empty() {
        let shset: HashSet<&T> = HashSet::from_iter((*smaller).iter());

        for element in (*bigger).iter() {
            if shset.contains(&element) {
                return true;
            }
        }
    }
    false
}

#[doc(hidden)]
pub fn arrays_overlapN_<T>(first: Option<Array<T>>, second: Array<T>) -> Option<bool>
where
    T: Eq + Hash,
{
    let first = first?;
    Some(arrays_overlap__(first, second))
}

#[doc(hidden)]
pub fn arrays_overlap_N<T>(first: Array<T>, second: Option<Array<T>>) -> Option<bool>
where
    T: Eq + Hash,
{
    let second = second?;
    Some(arrays_overlap__(first, second))
}

#[doc(hidden)]
pub fn arrays_overlapNN<T>(first: Option<Array<T>>, second: Option<Array<T>>) -> Option<bool>
where
    T: Eq + Hash,
{
    let first = first?;
    let second = second?;
    Some(arrays_overlap__(first, second))
}

#[doc(hidden)]
pub fn array_agg<T>(accumulator: &mut Vec<T>, value: T, weight: Weight, distinct: bool, keep: bool)
where
    T: Clone,
{
    if weight < 0 {
        panic!("Negative weight {:?}", weight);
    }
    if distinct && keep {
        accumulator.push(value.clone())
    } else if keep {
        for _i in 0..weight {
            accumulator.push(value.clone())
        }
    }
}

#[doc(hidden)]
pub fn array_aggN<T>(
    accumulator: &mut Option<Vec<T>>,
    value: T,
    weight: Weight,
    distinct: bool,
    keep: bool,
) where
    T: Clone,
{
    if let Some(accumulator) = accumulator.as_mut() {
        array_agg(accumulator, value, weight, distinct, keep)
    }
}

#[doc(hidden)]
pub fn array_agg_opt<T>(
    accumulator: &mut Vec<Option<T>>,
    value: Option<T>,
    weight: Weight,
    distinct: bool,
    keep: bool,
    ignore_nulls: bool,
) where
    T: Clone,
{
    if !ignore_nulls || value.is_some() {
        array_agg(accumulator, value, weight, distinct, keep);
    }
}

#[doc(hidden)]
pub fn array_agg_optN<T>(
    accumulator: &mut Option<Vec<Option<T>>>,
    value: Option<T>,
    weight: Weight,
    distinct: bool,
    keep: bool,
    ignore_nulls: bool,
) where
    T: Clone,
{
    if let Some(accumulator) = accumulator.as_mut() {
        array_agg_opt(accumulator, value, weight, distinct, keep, ignore_nulls);
    }
}

#[doc(hidden)]
pub fn array_concat__<T>(left: Array<T>, right: Array<T>) -> Array<T>
where
    T: Clone,
{
    let mut result = Vec::with_capacity(left.len() + right.len());
    result.extend((*left).clone());
    result.extend((*right).clone());
    result.into()
}

#[doc(hidden)]
pub fn array_concatN_<T>(left: Option<Array<T>>, right: Array<T>) -> Option<Array<T>>
where
    T: Clone,
{
    let left = left?;
    Some(array_concat__(left, right))
}

#[doc(hidden)]
pub fn array_concat_N<T>(left: Array<T>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Clone,
{
    let right = right?;
    Some(array_concat__(left, right))
}

#[doc(hidden)]
pub fn array_concatNN<T>(left: Option<Array<T>>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Clone,
{
    let left = left?;
    let right = right?;
    Some(array_concat__(left, right))
}

fn to_set<T>(v: &[T]) -> HashSet<T>
where
    T: Eq + Clone + Hash + Ord,
{
    v.iter().cloned().collect()
}

#[doc(hidden)]
pub fn array_except__<T>(left: Array<T>, right: Array<T>) -> Array<T>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = to_set(&left);
    let right = to_set(&right);
    let result = left.difference(&right);
    let mut result = result.cloned().collect::<Vec<T>>();
    result.sort();
    result.into()
}

#[doc(hidden)]
pub fn array_exceptN_<T>(left: Option<Array<T>>, right: Array<T>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = left?;
    Some(array_except__(left, right))
}

#[doc(hidden)]
pub fn array_except_N<T>(left: Array<T>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let right = right?;
    Some(array_except__(left, right))
}

#[doc(hidden)]
pub fn array_exceptNN<T>(left: Option<Array<T>>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = left?;
    let right = right?;
    Some(array_except__(left, right))
}

#[doc(hidden)]
pub fn array_union__<T>(left: Array<T>, right: Array<T>) -> Array<T>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = to_set(&left);
    let right = to_set(&right);
    let result = left.union(&right);
    let mut result = result.cloned().collect::<Vec<T>>();
    result.sort();
    result.into()
}

#[doc(hidden)]
pub fn array_unionN_<T>(left: Option<Array<T>>, right: Array<T>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = left?;
    Some(array_union__(left, right))
}

#[doc(hidden)]
pub fn array_union_N<T>(left: Array<T>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let right = right?;
    Some(array_union__(left, right))
}

#[doc(hidden)]
pub fn array_unionNN<T>(left: Option<Array<T>>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = left?;
    let right = right?;
    Some(array_union__(left, right))
}

#[doc(hidden)]
pub fn array_intersect__<T>(left: Array<T>, right: Array<T>) -> Array<T>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = to_set(&left);
    let right = to_set(&right);
    let result = left.intersection(&right);
    let mut result = result.cloned().collect::<Vec<T>>();
    result.sort();
    result.into()
}

#[doc(hidden)]
pub fn array_intersectN_<T>(left: Option<Array<T>>, right: Array<T>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = left?;
    Some(array_intersect__(left, right))
}

#[doc(hidden)]
pub fn array_intersect_N<T>(left: Array<T>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let right = right?;
    Some(array_intersect__(left, right))
}

#[doc(hidden)]
pub fn array_intersectNN<T>(left: Option<Array<T>>, right: Option<Array<T>>) -> Option<Array<T>>
where
    T: Eq + Clone + Hash + Ord,
{
    let left = left?;
    let right = right?;
    Some(array_intersect__(left, right))
}

// There are only 8 variants of array_insert, since the
// compiler enforces the element type is always nullable.
// The standard macros we have don't work for this function.

// The result type must always be Array<Option<T>>
// The suffix has 4 symbols
// N_N_
// ^array
//  ^pos
//   ^value
//    ^array element type

#[doc(hidden)]
pub fn array_insert__N_<T>(array: Array<T>, pos: i32, value: Option<T>) -> Array<Option<T>>
where
    T: Clone + Debug,
{
    let array: Array<Option<T>> = array
        .iter()
        .map(|x| Some(x.clone()))
        .collect::<Vec<Option<T>>>()
        .into();
    array_insert__NN(array, pos, value)
}

#[doc(hidden)]
pub fn array_insert_NN_<T>(
    array: Array<T>,
    pos: Option<i32>,
    value: Option<T>,
) -> Option<Array<Option<T>>>
where
    T: Clone + Debug,
{
    let pos = pos?;
    Some(array_insert__N_(array, pos, value))
}

#[doc(hidden)]
#[allow(clippy::needless_range_loop)]
pub fn array_insert__NN<T>(array: Array<Option<T>>, pos: i32, value: Option<T>) -> Array<Option<T>>
where
    T: Clone + Debug,
{
    const MAX_ARRAY_LENGTH: usize = (i32::MAX) as usize - 15;
    let mut abs = num::abs(pos) as usize;

    if pos == 0 {
        panic!("Index of 0 for 'array_insert");
    }
    if abs > MAX_ARRAY_LENGTH {
        panic!("Index {} too large for 'array_index'", pos);
    }

    let len = array.len();
    if pos <= 0 {
        if abs <= len {
            // Insert inside array
            abs = len - abs + 2;
        } else {
            // extend array and insert at the beginning
            let mut result: Vec<Option<T>> = Vec::with_capacity(abs + 1);
            result.push(value);
            for _index in 0..(abs - len - 1) {
                result.push(None);
            }
            result.extend(array.iter().cloned());
            return result.into();
        }
    } else if abs > len {
        // extend the array and insert at end
        let mut result = Vec::<Option<T>>::with_capacity(abs + 1);
        result.extend(array.iter().cloned());
        for _index in len..(abs - 1) {
            result.push(None);
        }
        result.push(value);
        return result.into();
    }

    let mut result = Vec::<Option<T>>::with_capacity(len + 1);
    result.extend_from_slice(&array[..(abs - 1)]);
    result.push(value);
    result.extend_from_slice(&array[(abs - 1)..len]);
    result.into()
}

#[doc(hidden)]
pub fn array_insert_NNN<T>(
    array: Array<Option<T>>,
    pos: Option<i32>,
    value: Option<T>,
) -> Option<Array<Option<T>>>
where
    T: Clone + Debug,
{
    let pos = pos?;
    Some(array_insert__NN(array, pos, value))
}

#[doc(hidden)]
pub fn array_insertN_N_<T>(
    array: Option<Array<T>>,
    pos: i32,
    value: Option<T>,
) -> Option<Array<Option<T>>>
where
    T: Clone + Debug,
{
    let array = array?;
    Some(array_insert__N_(array, pos, value))
}

#[doc(hidden)]
pub fn array_insertNNN_<T>(
    array: Option<Array<T>>,
    pos: Option<i32>,
    value: Option<T>,
) -> Option<Array<Option<T>>>
where
    T: Clone + Debug,
{
    let array = array?;
    let pos = pos?;
    Some(array_insert__N_(array, pos, value))
}

#[doc(hidden)]
pub fn array_insertN_NN<T>(
    array: Option<Array<Option<T>>>,
    pos: i32,
    value: Option<T>,
) -> Option<Array<Option<T>>>
where
    T: Clone + Debug,
{
    let array = array?;
    Some(array_insert__NN(array, pos, value))
}

#[doc(hidden)]
pub fn array_insertNNNN<T>(
    array: Option<Array<Option<T>>>,
    pos: Option<i32>,
    value: Option<T>,
) -> Option<Array<Option<T>>>
where
    T: Clone + Debug,
{
    let array = array?;
    let pos = pos?;
    Some(array_insert__NN(array, pos, value))
}
