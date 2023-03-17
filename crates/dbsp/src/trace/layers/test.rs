//! Test various implementations of `trait Trie`.

use super::{
    column_layer::ColumnLayer, ordered::OrderedLayer, ordered_leaf::OrderedLeaf, Builder, Cursor,
    Trie, TupleBuilder,
};
use crate::{algebra::HasZero, trace::consolidation::consolidate, DBData, DBWeight};
use proptest::{collection::vec, prelude::*};
use std::collections::BTreeMap;

// Unordered vectors of tuples used as test inputs.
type Tuples1<T, R> = Vec<(T, R)>;
type Tuples2<K, T, R> = Vec<((K, T), R)>;
type Tuples3<K, V, T, R> = Vec<((K, V, T), R)>;

// An equivalent representation of tries as nested maps.
type Map1<T, R> = BTreeMap<T, R>;
type Map2<K, T, R> = BTreeMap<K, Map1<T, R>>;
type Map3<K, V, T, R> = BTreeMap<K, Map2<V, T, R>>;

// Generate random input data.
fn tuples1(max_key: i32, max_val: i32, max_len: usize) -> BoxedStrategy<Tuples1<i32, i32>> {
    vec((0..max_key, -max_val..max_val), 0..max_len).boxed()
}

fn tuples2(
    max_key: i32,
    max_t: i32,
    max_r: i32,
    max_len: usize,
) -> BoxedStrategy<Tuples2<i32, i32, i32>> {
    vec(((0..max_key, 0..max_t), -max_r..max_r), 0..max_len).boxed()
}

fn tuples3(
    max_key: i32,
    max_val: i32,
    max_t: i32,
    max_r: i32,
    max_len: usize,
) -> BoxedStrategy<Tuples3<i32, i32, i32, i32>> {
    vec(
        ((0..max_key, 0..max_val, 0..max_t), -max_r..max_r),
        0..max_len,
    )
    .boxed()
}

// Generate nested map representation of the trie to use as a reference.
fn tuples_to_map1<T, R>(tuples: &Tuples1<T, R>) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    for (k, v) in tuples.iter() {
        result
            .entry(k.clone())
            .or_insert_with(HasZero::zero)
            .add_assign_by_ref(v);
    }

    // Prune zero weights.
    result.retain(|_, v| !v.is_zero());
    result
}

fn tuples_to_map2<K, T, R>(tuples: &Tuples2<K, T, R>) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    for ((k, t), r) in tuples.iter() {
        result
            .entry(k.clone())
            .or_insert_with(BTreeMap::new)
            .entry(t.clone())
            .or_insert_with(HasZero::zero)
            .add_assign_by_ref(r);
    }

    for vals in result.values_mut() {
        vals.retain(|_, v| !v.is_zero());
    }
    result.retain(|_, vals| !vals.is_empty());
    result
}

fn tuples_to_map3<K, V, T, R>(tuples: &Tuples3<K, V, T, R>) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map3<K, V, T, R> = BTreeMap::new();

    for ((k, v, t), r) in tuples.iter() {
        result
            .entry(k.clone())
            .or_insert_with(BTreeMap::new)
            .entry(v.clone())
            .or_insert_with(BTreeMap::new)
            .entry(t.clone())
            .or_insert_with(HasZero::zero)
            .add_assign_by_ref(r);
    }

    for vals in result.values_mut() {
        for weights in vals.values_mut() {
            weights.retain(|_, w| !w.is_zero());
        }
        vals.retain(|_, weights| !weights.is_empty());
    }
    result.retain(|_, vals| !vals.is_empty());
    result
}

// Generate a layer-based representation of the trie.
fn tuples_to_trie1<T, R, Tr>(tuples: &Tuples1<T, R>) -> Tr
where
    Tr: Trie<Item = (T, R)> + std::fmt::Debug,
    Tr::TupleBuilder: std::fmt::Debug,
    T: DBData,
    R: DBWeight,
{
    let mut tuples = tuples.clone();
    consolidate(&mut tuples);
    let mut builder = <Tr::TupleBuilder>::with_capacity(tuples.len());
    builder.extend_tuples(tuples.iter().cloned());
    builder.done()
}

fn tuples_to_trie2<K, T, R, Tr>(tuples: &Tuples2<K, T, R>) -> Tr
where
    Tr: Trie<Item = (K, (T, R))> + std::fmt::Debug,
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut tuples = tuples.clone();
    consolidate(&mut tuples);

    let mut builder = <Tr::TupleBuilder>::with_capacity(tuples.len());
    builder.extend_tuples(tuples.iter().cloned().map(|((k, t), r)| (k, (t, r))));
    builder.done()
}

fn tuples_to_trie3<K, V, T, R, Tr>(tuples: &Tuples3<K, V, T, R>) -> Tr
where
    Tr: Trie<Item = (K, (V, (T, R)))> + std::fmt::Debug,
    Tr::TupleBuilder: std::fmt::Debug,
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut tuples = tuples.clone();
    consolidate(&mut tuples);

    let mut builder = <Tr::TupleBuilder>::with_capacity(tuples.len());
    builder.extend_tuples(
        tuples
            .iter()
            .cloned()
            .map(|((k, v, t), r)| (k, (v, (t, r)))),
    );
    builder.done()
}

// The following functions convert various layer-based representations of tries
// to map-based representation for equivalence checking.  There's a lot of
// duplicated code here.  Generic implementations are likely possible, but
// non-trivial as it is not easy to unify `(&K, &V)` and `&(K, V)` keys types
// used by different leaf implementations.
fn ordered_leaf_to_map1<T, R>(trie: &OrderedLeaf<T, R>) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        let (t, r) = Cursor::key(&cursor);
        result.insert(t.clone(), r.clone());
        cursor.step();
    }

    result
}

fn column_layer_to_map1<T, R>(trie: &ColumnLayer<T, R>) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        let (t, r) = Cursor::key(&cursor);
        result.insert(t.clone(), r.clone());
        cursor.step();
    }

    result
}

fn ordered_column_layer_to_map2<K, T, R>(
    trie: &OrderedLayer<K, ColumnLayer<T, R>, usize>,
) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        result.insert(cursor.key().clone(), BTreeMap::new());

        let mut leaf_cursor = cursor.values();

        while leaf_cursor.valid() {
            let (t, r) = leaf_cursor.key();

            let entry = result.get_mut(cursor.key()).unwrap();
            entry.insert(t.clone(), r.clone());

            leaf_cursor.step();
        }
        cursor.step();
    }

    result
}

fn ordered_leaf_layer_to_map2<K, T, R>(
    trie: &OrderedLayer<K, OrderedLeaf<T, R>, usize>,
) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        result.insert(cursor.key().clone(), BTreeMap::new());

        let mut leaf_cursor = cursor.values();

        while leaf_cursor.valid() {
            let (t, r) = leaf_cursor.key();

            let entry = result.get_mut(cursor.key()).unwrap();
            entry.insert(t.clone(), r.clone());

            leaf_cursor.step();
        }
        cursor.step();
    }

    result
}

fn ordered_column_layer_to_map3<K, V, T, R>(
    trie: &OrderedLayer<K, OrderedLayer<V, ColumnLayer<T, R>, usize>, usize>,
) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map3<K, V, T, R> = BTreeMap::new();

    let mut cursor1 = trie.cursor();

    while cursor1.valid() {
        let k = cursor1.key();

        result.insert(k.clone(), BTreeMap::new());

        let mut cursor2 = cursor1.values();

        while cursor2.valid() {
            let v = cursor2.key();

            result
                .get_mut(k)
                .unwrap()
                .insert(v.clone(), BTreeMap::new());

            let mut cursor3 = cursor2.values();
            while cursor3.valid() {
                let (t, r) = cursor3.key();
                result
                    .get_mut(k)
                    .unwrap()
                    .get_mut(v)
                    .unwrap()
                    .insert(t.clone(), r.clone());
                cursor3.step();
            }
            cursor2.step();
        }
        cursor1.step();
    }

    result
}

fn ordered_leaf_layer_to_map3<K, V, T, R>(
    trie: &OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>, usize>, usize>,
) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map3<K, V, T, R> = BTreeMap::new();

    let mut cursor1 = trie.cursor();

    while cursor1.valid() {
        let k = cursor1.key();

        result.insert(k.clone(), BTreeMap::new());

        let mut cursor2 = cursor1.values();

        while cursor2.valid() {
            let v = cursor2.key();

            result
                .get_mut(k)
                .unwrap()
                .insert(v.clone(), BTreeMap::new());

            let mut cursor3 = cursor2.values();
            while cursor3.valid() {
                let (t, r) = cursor3.key();
                result
                    .get_mut(k)
                    .unwrap()
                    .get_mut(v)
                    .unwrap()
                    .insert(t.clone(), r.clone());
                cursor3.step();
            }
            cursor2.step();
        }
        cursor1.step();
    }

    result
}

// Merge map-based implementations of tries.
fn merge_map1<T, R>(left: &Map1<T, R>, right: &Map1<T, R>) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight,
{
    let mut result = left.clone();
    for (k, v) in right.iter() {
        result
            .entry(k.clone())
            .or_insert_with(|| HasZero::zero())
            .add_assign_by_ref(v);
    }

    result.retain(|_, v| !v.is_zero());
    result
}

fn merge_map2<K, T, R>(left: &Map2<K, T, R>, right: &Map2<K, T, R>) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result = left.clone();
    for (k, vals) in right.iter() {
        let entry = result.entry(k.clone()).or_insert_with(BTreeMap::new);
        for (t, v) in vals.iter() {
            entry
                .entry(t.clone())
                .or_insert_with(HasZero::zero)
                .add_assign_by_ref(v);
        }
    }

    for vals in result.values_mut() {
        vals.retain(|_, v| !v.is_zero());
    }
    result.retain(|_, vals| !vals.is_empty());

    result
}

fn merge_map3<K, V, T, R>(left: &Map3<K, V, T, R>, right: &Map3<K, V, T, R>) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result = left.clone();
    for (k, vals) in right.iter() {
        let entry = result.entry(k.clone()).or_insert_with(BTreeMap::new);
        for (v, times) in vals.iter() {
            let entry = entry.entry(v.clone()).or_insert_with(BTreeMap::new);
            for (t, r) in times.iter() {
                entry
                    .entry(t.clone())
                    .or_insert_with(HasZero::zero)
                    .add_assign_by_ref(r);
            }
        }
    }

    for vals in result.values_mut() {
        for times in vals.values_mut() {
            times.retain(|_, r| !r.is_zero())
        }
        vals.retain(|_, times| !times.is_empty());
    }
    result.retain(|_, vals| !vals.is_empty());

    result
}

// Map-based implementations for `truncate_below`.
fn truncate_map1<T, R>(map: &Map1<T, R>, lower_bound: usize) -> Map1<T, R>
where
    T: DBData,
    R: DBData,
{
    map.iter()
        .skip(lower_bound)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn truncate_map2<K, T, R>(map: &Map2<K, T, R>, lower_bound: usize) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBData,
{
    map.iter()
        .skip(lower_bound)
        .map(|(k, vals)| (k.clone(), vals.clone()))
        .collect()
}

fn truncate_map3<K, V, T, R>(map: &Map3<K, V, T, R>, lower_bound: usize) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBData,
{
    map.iter()
        .skip(lower_bound)
        .map(|(k, vals)| (k.clone(), vals.clone()))
        .collect()
}

// Map-based implementations for `truncate_below`.
fn retain_map1<T, R, F>(map: &mut Map1<T, R>, retain: F)
where
    T: DBData,
    R: DBData,
    F: Fn(&T, &R) -> bool,
{
    map.retain(|k, v| retain(k, v));
}

// Check that layer- and map-based representations are equivalent
// by converting `Tr` to `Map` using `convert` closure.
fn assert_eq_trie_map1<T, R, Tr, F>(trie: &Tr, map: &Map1<T, R>, convert: F)
where
    T: DBData,
    R: DBData,
    F: Fn(&Tr) -> Map1<T, R>,
{
    assert_eq!(map, &convert(trie));
}

fn assert_eq_trie_map2<K, T, R, Tr, F>(trie: &Tr, map: &Map2<K, T, R>, convert: F)
where
    K: DBData,
    T: DBData,
    R: DBData,
    F: Fn(&Tr) -> Map2<K, T, R>,
    Tr: std::fmt::Debug,
{
    assert_eq!(map, &convert(trie));
}

fn assert_eq_trie_map3<K, V, T, R, Tr, F>(trie: &Tr, map: &Map3<K, V, T, R>, convert: F)
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBData,
    F: Fn(&Tr) -> Map3<K, V, T, R>,
    Tr: std::fmt::Debug,
{
    assert_eq!(map, &convert(trie));
}

fn test_trie1<T, R, Tr, F>(left: &Tuples1<T, R>, right: &Tuples1<T, R>, trie_to_map: &F)
where
    T: DBData,
    R: DBWeight,
    Tr: Trie<Item = (T, R)> + std::fmt::Debug,
    Tr::TupleBuilder: std::fmt::Debug,
    F: Fn(&Tr) -> Map1<T, R>,
{
    // Check that map- and layer-based representations of
    // input datasets are identical.
    let left_map = tuples_to_map1(&left);
    let right_map = tuples_to_map1(&right);

    let left_trie = tuples_to_trie1::<_, _, Tr>(&left);
    let right_trie = tuples_to_trie1::<_, _, Tr>(&right);

    assert_eq_trie_map1(&left_trie, &left_map, trie_to_map);
    assert_eq_trie_map1(&right_trie, &right_map, trie_to_map);

    // Merge produces identical results.
    let mut merged_trie = left_trie.merge(&right_trie);
    let merged_map = merge_map1(&left_map, &right_map);
    assert_eq_trie_map1(&merged_trie, &merged_map, trie_to_map);

    // Truncate tries at the start, middle, and end.
    for lower_bound in [0, merged_trie.keys() >> 1, merged_trie.keys()] {
        merged_trie.truncate_below(lower_bound);
        assert_eq_trie_map1(
            &merged_trie,
            &truncate_map1(&merged_map, lower_bound),
            trie_to_map,
        );
    }
}

fn test_trie2<K, T, R, Tr, F>(left: &Tuples2<K, T, R>, right: &Tuples2<K, T, R>, trie_to_map: &F)
where
    K: DBData,
    T: DBData,
    R: DBWeight,
    Tr: Trie<Item = (K, (T, R))> + std::fmt::Debug,
    Tr::TupleBuilder: std::fmt::Debug,
    F: Fn(&Tr) -> Map2<K, T, R>,
{
    let left_map = tuples_to_map2(&left);
    let right_map = tuples_to_map2(&right);

    let left_trie = tuples_to_trie2::<_, _, _, Tr>(&left);
    let right_trie = tuples_to_trie2::<_, _, _, Tr>(&right);

    assert_eq_trie_map2(&left_trie, &left_map, trie_to_map);

    assert_eq_trie_map2(&right_trie, &right_map, trie_to_map);

    let mut merged_trie = left_trie.merge(&right_trie);
    let merged_map = merge_map2(&left_map, &right_map);
    assert_eq_trie_map2(&merged_trie, &merged_map, trie_to_map);

    for lower_bound in [0, merged_trie.keys() >> 1, merged_trie.keys()] {
        merged_trie.truncate_below(lower_bound);
        assert_eq_trie_map2(
            &merged_trie,
            &truncate_map2(&merged_map, lower_bound),
            trie_to_map,
        );
    }
}

fn test_trie3<K, V, T, R, Tr, F>(
    left: &Tuples3<K, V, T, R>,
    right: &Tuples3<K, V, T, R>,
    trie_to_map: &F,
) where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
    Tr: Trie<Item = (K, (V, (T, R)))> + std::fmt::Debug,
    Tr::TupleBuilder: std::fmt::Debug,
    F: Fn(&Tr) -> Map3<K, V, T, R>,
{
    let left_map = tuples_to_map3(&left);
    let right_map = tuples_to_map3(&right);

    let left_trie = tuples_to_trie3::<_, _, _, _, Tr>(&left);
    let right_trie = tuples_to_trie3::<_, _, _, _, Tr>(&right);

    assert_eq_trie_map3(&left_trie, &left_map, trie_to_map);

    assert_eq_trie_map3(&right_trie, &right_map, trie_to_map);

    let mut merged_trie = left_trie.merge(&right_trie);
    let merged_map = merge_map3(&left_map, &right_map);
    assert_eq_trie_map3(&merged_trie, &merged_map, trie_to_map);

    for lower_bound in [0, merged_trie.keys() >> 1, merged_trie.keys()] {
        merged_trie.truncate_below(lower_bound);
        assert_eq_trie_map3(
            &merged_trie,
            &truncate_map3(&merged_map, lower_bound),
            trie_to_map,
        );
    }
}

proptest! {
    #[test]
    fn test_column_layer_retain(tuples in tuples1(100, 3, 5000)) {
        let mut map = tuples_to_map1(&tuples);
        let mut trie = tuples_to_trie1::<_, _, ColumnLayer<_, _>>(&tuples);

        for key in 0..100 {
            trie.retain(|k, _v| k != &key);
            trie.truncate_below(2);

            retain_map1(&mut map, |k, _v| k != &key);
            map = truncate_map1(&mut map, 2);

            assert_eq_trie_map1(&trie, &map, column_layer_to_map1);
        }
    }

    #[test]
    fn test_leaf_layers(left in tuples1(10, 3, 5000), right in tuples1(10, 3, 5000)) {
        test_trie1::<_, _, OrderedLeaf<_, _>, _>(&left, &right, &ordered_leaf_to_map1);
        test_trie1::<_, _, ColumnLayer<_, _>, _>(&left, &right, &column_layer_to_map1);
    }

    #[test]
    fn test_nested_layers(left in tuples2(10, 10, 2, 5000), right in tuples2(10, 5, 2, 5000)) {
        test_trie2::<_, _, _, OrderedLayer<_, ColumnLayer<_, _>, usize>, _>(&left, &right, &ordered_column_layer_to_map2);
        test_trie2::<_, _, _, OrderedLayer<_, OrderedLeaf<_, _>, usize>, _>(&left, &right, &ordered_leaf_layer_to_map2);
    }

    #[test]
    fn test_twice_nested_layers(left in tuples3(10, 5, 5, 2, 5000), right in tuples3(10, 5, 5, 2, 5000)) {
        test_trie3::<_, _, _, _, OrderedLayer<_, OrderedLayer<_, OrderedLeaf<_, _>, usize>, usize>, _>(&left, &right, &ordered_leaf_layer_to_map3);
        test_trie3::<_, _, _, _, OrderedLayer<_, OrderedLayer<_, ColumnLayer<_, _>, usize>, usize>, _>(&left, &right, &ordered_column_layer_to_map3);
    }
}
