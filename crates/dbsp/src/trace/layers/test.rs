//! Test various implementations of `trait Trie`.

use crate::{
    algebra::{HasZero, ZRingValue},
    dynamic::{DowncastTrait, DynData, DynWeight, Erase, LeanVec},
    trace::layers::file::{
        column_layer::{FileColumnLayer, FileLeafFactories},
        ordered::{FileOrderedLayer, FileOrderedLayerFactories},
    },
    utils::consolidate_pairs,
    DBData, DBWeight,
};
use proptest::{collection::vec, prelude::*};
use std::collections::BTreeMap;

use super::{
    layer::LayerFactories, leaf::LeafFactories, Builder, Cursor, Layer, Leaf, Trie, TupleBuilder,
};

// Unordered vectors of tuples used as test inputs.
pub(crate) type Tuples1<T, R> = Vec<(T, R)>;
pub(crate) type Tuples2<K, T, R> = Vec<((K, T), R)>;
pub(crate) type Tuples3<K, V, T, R> = Vec<((K, V, T), R)>;

// An equivalent representation of tries as nested maps.
pub(crate) type Map1<T, R> = BTreeMap<T, R>;
pub(crate) type Map2<K, T, R> = BTreeMap<K, Map1<T, R>>;
pub(crate) type Map3<K, V, T, R> = BTreeMap<K, Map2<V, T, R>>;

// Generate random input data.
pub(crate) fn tuples1(
    max_key: i32,
    max_val: i32,
    max_len: usize,
) -> BoxedStrategy<Tuples1<i32, i32>> {
    vec((0..max_key, -max_val..max_val), 0..max_len).boxed()
}

pub(crate) fn tuples2(
    max_key: i32,
    max_t: i32,
    max_r: i32,
    max_len: usize,
) -> BoxedStrategy<Tuples2<i32, i32, i32>> {
    vec(((0..max_key, 0..max_t), -max_r..max_r), 0..max_len).boxed()
}

pub(crate) fn tuples3(
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
pub(crate) fn tuples_to_map1<T, R>(tuples: &Tuples1<T, R>) -> Map1<T, R>
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

pub(crate) fn tuples_to_map2<K, T, R>(tuples: &Tuples2<K, T, R>) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    for ((k, t), r) in tuples.iter() {
        result
            .entry(k.clone())
            .or_default()
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

pub(crate) fn tuples_to_map3<K, V, T, R>(tuples: &Tuples3<K, V, T, R>) -> Map3<K, V, T, R>
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
            .or_default()
            .entry(v.clone())
            .or_default()
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

// Merge map-based implementations of tries.
pub(crate) fn merge_map1<T, R>(left: &Map1<T, R>, right: &Map1<T, R>) -> Map1<T, R>
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

pub(crate) fn merge_map2<K, T, R>(left: &Map2<K, T, R>, right: &Map2<K, T, R>) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result = left.clone();
    for (k, vals) in right.iter() {
        let entry = result.entry(k.clone()).or_default();
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

pub(crate) fn merge_map3<K, V, T, R>(
    left: &Map3<K, V, T, R>,
    right: &Map3<K, V, T, R>,
) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result = left.clone();
    for (k, vals) in right.iter() {
        let entry = result.entry(k.clone()).or_default();
        for (v, times) in vals.iter() {
            let entry = entry.entry(v.clone()).or_default();
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
pub(crate) fn truncate_map1<T, R>(map: &Map1<T, R>, lower_bound: usize) -> Map1<T, R>
where
    T: DBData,
    R: DBData,
{
    map.iter()
        .skip(lower_bound)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub(crate) fn truncate_map2<K, T, R>(map: &Map2<K, T, R>, lower_bound: usize) -> Map2<K, T, R>
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

pub(crate) fn truncate_map3<K, V, T, R>(
    map: &Map3<K, V, T, R>,
    lower_bound: usize,
) -> Map3<K, V, T, R>
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
/*fn retain_map1<T, R, F>(map: &mut Map1<T, R>, retain: F)
where
    T: DBData,
    R: DBData,
    F: Fn(&T, &R) -> bool,
{
    map.retain(|k, v| retain(k, v));
}

 */

type Trie1 /* <T, R> */ = Leaf<DynData, DynWeight>;
type Trie2 /* <K, T, R> */ = Layer<DynData, Trie1>;
type Trie3 /* <K, V, T, R> */ = Layer<DynData, Trie2>;

type FileTrie1 /* <T, R> */ = FileColumnLayer<DynData, DynWeight>;
type FileTrie2 /* <K, V, R> */ = FileOrderedLayer<DynData, DynData, DynWeight>;

// Generate a layer-based representation of the trie.
fn tuples_to_trie1<T: DBData, R: DBWeight + ZRingValue, L: Trie>(
    factories: &L::Factories,
    tuples: &Tuples1<T, R>,
) -> L
where
    T: DBData,
    R: DBWeight + ZRingValue,
    L: for<'a> Trie<Item<'a> = (&'a mut DynData, &'a mut DynWeight)>,
{
    let mut tuples = LeanVec::from(tuples.clone());
    consolidate_pairs(&mut tuples);

    let mut builder =
        <<L as Trie>::TupleBuilder as TupleBuilder>::with_capacity(factories, tuples.len());

    for (mut t, mut r) in tuples.as_slice().iter().cloned() {
        builder.push_tuple((t.erase_mut(), r.erase_mut()));
    }
    builder.done()
}

fn tuples_to_trie2<K, T, R, L>(factories: &L::Factories, tuples: &Tuples2<K, T, R>) -> L
where
    K: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
    L: for<'a> Trie<Item<'a> = (&'a mut DynData, (&'a mut DynData, &'a mut DynWeight))>,
{
    let mut tuples = LeanVec::from(tuples.clone());
    consolidate_pairs(&mut tuples);

    let mut builder = <<L/*<K, T, R>*/ as Trie>::TupleBuilder as TupleBuilder>::with_capacity(
        factories,
        tuples.len(),
    );

    for ((mut k, mut t), mut r) in tuples.as_slice().iter().cloned() {
        builder.push_tuple((k.erase_mut(), (t.erase_mut(), r.erase_mut())))
    }
    builder.done()
}

fn tuples_to_trie3<K, V, T, R>(tuples: &Tuples3<K, V, T, R>) -> Trie3
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut tuples = LeanVec::from(tuples.clone());
    consolidate_pairs(&mut tuples);

    let mut builder = <<Trie3 as Trie>::TupleBuilder as TupleBuilder>::with_capacity(
        &LayerFactories::new::<K>(LayerFactories::new::<V>(LeafFactories::new::<T, R>())),
        tuples.len(),
    );

    for ((mut k, mut v, mut t), mut r) in tuples.as_slice().iter().cloned() {
        builder.push_tuple((
            k.erase_mut(),
            (v.erase_mut(), (t.erase_mut(), r.erase_mut())),
        ))
    }

    builder.done()
}

// The following functions convert various layer-based representations of tries
// to map-based representation for equivalence checking.  There's a lot of
// duplicated code here.  Generic implementations are likely possible, but
// non-trivial as it is not easy to unify `(&K, &V)` and `&(K, V)` keys types
// used by different leaf implementations.
fn vec_leaf_to_map1<T, R>(trie: &Trie1) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        let (t, r) = Cursor::item(&cursor);
        result.insert(
            t.downcast_checked::<T>().clone(),
            r.downcast_checked::<R>().clone(),
        );
        cursor.step();
    }

    result
}

fn vec_leaf_to_map1_reverse<T, R>(trie: &Trie1) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();
    cursor.fast_forward();

    while cursor.valid() {
        let (t, r) = Cursor::item(&cursor);
        result.insert(
            t.downcast_checked::<T>().clone(),
            r.downcast_checked::<R>().clone(),
        );
        cursor.step_reverse();
    }

    result
}

fn file_leaf_to_map1<T, R>(trie: &FileTrie1) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        let (t, r) = Cursor::item(&cursor);
        result.insert(
            t.downcast_checked::<T>().clone(),
            r.downcast_checked::<R>().clone(),
        );
        cursor.step();
    }

    result
}

fn file_leaf_to_map1_reverse<T, R>(trie: &FileTrie1) -> Map1<T, R>
where
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map1<T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();
    cursor.fast_forward();

    while cursor.valid() {
        let (t, r) = Cursor::item(&cursor);
        result.insert(
            t.downcast_checked::<T>().clone(),
            r.downcast_checked::<R>().clone(),
        );
        cursor.step_reverse();
    }

    result
}

fn vec_trie2_to_map2<K, T, R>(trie: &Trie2) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        result.insert(
            cursor.item().downcast_checked::<K>().clone(),
            BTreeMap::new(),
        );

        let mut leaf_cursor = cursor.values();

        while leaf_cursor.valid() {
            let (t, r) = leaf_cursor.item();

            let entry = result.get_mut(cursor.item().downcast_checked()).unwrap();
            entry.insert(
                t.downcast_checked::<T>().clone(),
                r.downcast_checked::<R>().clone(),
            );

            leaf_cursor.step();
        }
        cursor.step();
    }

    result
}

fn file_trie2_to_map2<K, T, R>(trie: &FileTrie2) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();

    while cursor.valid() {
        result.insert(
            cursor.item().downcast_checked::<K>().clone(),
            BTreeMap::new(),
        );

        let mut leaf_cursor = cursor.values();

        while leaf_cursor.valid() {
            let (t, r) = leaf_cursor.item();

            let entry = result.get_mut(cursor.item().downcast_checked()).unwrap();
            entry.insert(
                t.downcast_checked::<T>().clone(),
                r.downcast_checked::<R>().clone(),
            );

            leaf_cursor.step();
        }
        cursor.step();
    }

    result
}

fn vec_trie2_to_map2_reverse<K, T, R>(trie: &Trie2) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();
    cursor.fast_forward();

    while cursor.valid() {
        result.insert(
            cursor.item().downcast_checked::<K>().clone(),
            BTreeMap::new(),
        );

        let mut leaf_cursor = cursor.values();
        leaf_cursor.fast_forward();

        while leaf_cursor.valid() {
            let (t, r) = leaf_cursor.item();

            let entry = result.get_mut(cursor.item().downcast_checked()).unwrap();
            entry.insert(
                t.downcast_checked::<T>().clone(),
                r.downcast_checked::<R>().clone(),
            );

            leaf_cursor.step_reverse();
        }
        cursor.step_reverse();
    }

    result
}

fn file_trie2_to_map2_reverse<K, T, R>(trie: &FileTrie2) -> Map2<K, T, R>
where
    K: DBData,
    T: DBData,
    R: DBWeight,
{
    let mut result: Map2<K, T, R> = BTreeMap::new();

    let mut cursor = trie.cursor();
    cursor.fast_forward();

    while cursor.valid() {
        result.insert(
            cursor.item().downcast_checked::<K>().clone(),
            BTreeMap::new(),
        );

        let mut leaf_cursor = cursor.values();
        leaf_cursor.fast_forward();

        while leaf_cursor.valid() {
            let (t, r) = leaf_cursor.item();

            let entry = result.get_mut(cursor.item().downcast_checked()).unwrap();
            entry.insert(
                t.downcast_checked::<T>().clone(),
                r.downcast_checked::<R>().clone(),
            );

            leaf_cursor.step_reverse();
        }
        cursor.step_reverse();
    }

    result
}

fn trie3_to_map3<K, V, T, R>(trie: &Trie3) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map3<K, V, T, R> = BTreeMap::new();

    let mut cursor1 = trie.cursor();

    while cursor1.valid() {
        let k = cursor1.item().downcast_checked::<K>();

        result.insert(k.clone(), BTreeMap::new());

        let mut cursor2 = cursor1.values();

        while cursor2.valid() {
            let v = cursor2.item().downcast_checked::<V>();

            result
                .get_mut(k)
                .unwrap()
                .insert(v.clone(), BTreeMap::new());

            let mut cursor3 = cursor2.values();
            while cursor3.valid() {
                let (t, r) = cursor3.item();
                result.get_mut(k).unwrap().get_mut(v).unwrap().insert(
                    t.downcast_checked::<T>().clone(),
                    r.downcast_checked::<R>().clone(),
                );
                cursor3.step();
            }
            cursor2.step();
        }
        cursor1.step();
    }

    result
}

fn trie3_to_map3_reverse<K, V, T, R>(trie: &Trie3) -> Map3<K, V, T, R>
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
{
    let mut result: Map3<K, V, T, R> = BTreeMap::new();

    let mut cursor1 = trie.cursor();
    cursor1.fast_forward();

    while cursor1.valid() {
        let k = cursor1.item().downcast_checked::<K>();

        result.insert(k.clone(), BTreeMap::new());

        let mut cursor2 = cursor1.values();
        cursor2.fast_forward();

        while cursor2.valid() {
            let v = cursor2.item().downcast_checked::<V>();

            result
                .get_mut(k)
                .unwrap()
                .insert(v.clone(), BTreeMap::new());

            let mut cursor3 = cursor2.values();
            cursor3.fast_forward();

            while cursor3.valid() {
                let (t, r) = cursor3.item();
                result.get_mut(k).unwrap().get_mut(v).unwrap().insert(
                    t.downcast_checked::<T>().clone(),
                    r.downcast_checked::<R>().clone(),
                );
                cursor3.step_reverse();
            }
            cursor2.step_reverse();
        }
        cursor1.step_reverse();
    }

    result
}

// Check that layer- and map-based representations are equivalent
// by converting `Tr` to `Map` using `convert` closure.
fn assert_eq_trie_map1<T, R, L, F>(trie: &L, map: &Map1<T, R>, convert: F)
where
    T: DBData,
    R: DBWeight,
    L: for<'a> Trie<Item<'a> = (&'a mut DynData, &'a mut DynWeight)>,
    F: Fn(&L) -> Map1<T, R>,
{
    assert_eq!(map, &convert(trie));
}

fn assert_eq_trie_map2<K, T, R, L, F>(trie: &L, map: &Map2<K, T, R>, convert: F)
where
    K: DBData,
    T: DBData,
    R: DBWeight,
    F: Fn(&L) -> Map2<K, T, R>,
    L: for<'a> Trie<Item<'a> = (&'a mut DynData, (&'a mut DynData, &'a mut DynWeight))>,
{
    let map2 = &convert(trie);
    assert_eq!(map, map2);
}

fn assert_eq_trie_map3<K, V, T, R, F>(trie: &Trie3, map: &Map3<K, V, T, R>, convert: F)
where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight,
    F: Fn(&Trie3) -> Map3<K, V, T, R>,
{
    assert_eq!(map, &convert(trie));
}

fn test_trie1<T, R, F, L>(
    factories: &L::Factories,
    left: &Tuples1<T, R>,
    right: &Tuples1<T, R>,
    trie_to_map: &F,
) where
    T: DBData,
    R: DBWeight + ZRingValue,
    F: Fn(&L) -> Map1<T, R>,
    L: for<'a> Trie<Item<'a> = (&'a mut DynData, &'a mut DynWeight)>,
{
    // Check that map- and layer-based representations of
    // input datasets are identical.
    let left_map = tuples_to_map1(left);
    let right_map = tuples_to_map1(right);

    let left_trie = tuples_to_trie1::<_, _, L>(factories, left);
    let right_trie = tuples_to_trie1::<_, _, L>(factories, right);

    assert_eq_trie_map1(&left_trie, &left_map, trie_to_map);
    assert_eq_trie_map1(&right_trie, &right_map, trie_to_map);

    // Merge produces identical results.
    let mut merged_trie = left_trie.merge(&right_trie);
    let merged_map = merge_map1(&left_map, &right_map);
    assert_eq_trie_map1(&merged_trie, &merged_map, trie_to_map);

    // Truncate tries at the start, middle, and end.
    for lower_bound in [0, merged_trie.keys() >> 1, merged_trie.keys()] {
        println!("lower_bound: {lower_bound}");
        merged_trie.truncate_below(lower_bound);
        assert_eq_trie_map1(
            &merged_trie,
            &truncate_map1(&merged_map, lower_bound),
            trie_to_map,
        );
    }
}

fn test_trie2<K, T, R, F, L>(
    factories: &L::Factories,
    left: &Tuples2<K, T, R>,
    right: &Tuples2<K, T, R>,
    trie_to_map: &F,
) where
    K: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
    F: Fn(&L) -> Map2<K, T, R>,
    L: for<'a> Trie<Item<'a> = (&'a mut DynData, (&'a mut DynData, &'a mut DynWeight))>,
{
    let left_map = tuples_to_map2(left);
    let right_map = tuples_to_map2(right);

    let left_trie = tuples_to_trie2(factories, left);
    let right_trie = tuples_to_trie2(factories, right);

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

fn test_trie3<K, V, T, R, F>(
    left: &Tuples3<K, V, T, R>,
    right: &Tuples3<K, V, T, R>,
    trie_to_map: &F,
) where
    K: DBData,
    V: DBData,
    T: DBData,
    R: DBWeight + ZRingValue,
    F: Fn(&Trie3) -> Map3<K, V, T, R>,
{
    let left_map = tuples_to_map3(left);
    let right_map = tuples_to_map3(right);

    let left_trie = tuples_to_trie3::<_, _, _, _>(left);
    let right_trie = tuples_to_trie3::<_, _, _, _>(right);

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
    /*
    #[test]
    fn test_column_layer_retain(tuples in tuples1(100, 3, 5000)) {
        let mut map = tuples_to_map1(&tuples);
        let mut trie = tuples_to_trie1::<_, _, ColumnLayer<_, _>>(&tuples);

        for key in 0..100 {
            trie.retain(|k, _v| k != &key);
            trie.truncate_below(2);

            retain_map1(&mut map, |k, _v| k != &key);
            map = truncate_map1(&map, 2);

            assert_eq_trie_map1(&trie, &map, column_layer_to_map1);
        }
    }
    */

    #[test]
    fn test_leaf_layers(left in tuples1(10, 3, 5000), right in tuples1(10, 3, 5000)) {
        test_trie1::<_, _, _, Trie1>(&LeafFactories::new::<i32, i32>(), &left, &right, &vec_leaf_to_map1);
        test_trie1::<_, _, _, Trie1>(&LeafFactories::new::<i32, i32>(), &left, &right, &vec_leaf_to_map1_reverse);
    }

    #[test]
    fn test_file_leaf_layers(left in tuples1(10, 3, 5000), right in tuples1(10, 3, 5000)) {
        test_trie1::<_, _, _, FileTrie1>(&FileLeafFactories::new::<i32, i32>(), &left, &right, &file_leaf_to_map1);
        test_trie1::<_, _, _, FileTrie1>(&FileLeafFactories::new::<i32, i32>(), &left, &right, &file_leaf_to_map1_reverse);
    }


    #[test]
    fn test_nested_layers(left in tuples2(10, 10, 2, 5000), right in tuples2(10, 5, 2, 5000)) {
        test_trie2(&LayerFactories::new::<i32>(LeafFactories::new::<i32, i32>()), &left, &right, &vec_trie2_to_map2);
        test_trie2(&LayerFactories::new::<i32>(LeafFactories::new::<i32, i32>()), &left, &right, &vec_trie2_to_map2_reverse);
    }

    #[test]
    fn test_file_nested_layers(left in tuples2(10, 10, 2, 5000), right in tuples2(10, 5, 2, 5000)) {
        test_trie2(&FileOrderedLayerFactories::new::<i32, i32,i32>(), &left, &right, &file_trie2_to_map2);
        test_trie2(&FileOrderedLayerFactories::new::<i32, i32,i32>(), &left, &right, &file_trie2_to_map2_reverse);
    }

    #[test]
    fn test_twice_nested_layers(left in tuples3(10, 5, 5, 2, 5000), right in tuples3(10, 5, 5, 2, 5000)) {
        test_trie3(&left, &right, &trie3_to_map3);
        test_trie3(&left, &right, &trie3_to_map3_reverse);
    }
}
