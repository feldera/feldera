use crate::algebra::{AddAssignByRef, AddByRef, FiniteHashMap, FiniteMap, HasZero, NegByRef};
use std::{
    iter,
    ops::{Add, Neg},
};

type Map = FiniteHashMap<i64, i64>;

#[test]
fn hashmap_tests() {
    let mut z = Map::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!(finite_map! {}, z);
    assert_eq!(0, z.lookup(&0)); // not present -> 0
    assert_eq!(z, Map::zero());
    assert!(z.is_zero());

    let z2 = Map::new();
    assert_eq!(z, z2);

    let z3 = Map::singleton(3, 4);
    assert_eq!(finite_map! { 3 => 4 }, z3);

    z.increment(&0, 1);
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { 0 => 1 }, z);
    assert_eq!(1, z.lookup(&0));
    assert_eq!(0, z.lookup(&1));
    assert_ne!(z, Map::zero());
    assert!(!z.is_zero());

    z.increment(&2, 0);
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { 0 => 1 }, z);

    z.increment(&1, -1);
    assert_eq!(2, z.support_size());
    assert_eq!(finite_map! { 0 => 1, 1 => -1 }, z);

    z.increment(&-1, 1);
    assert_eq!(3, z.support_size());
    assert_eq!(finite_map! { -1 => 1, 0 => 1, 1 => -1 }, z);

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!(finite_map! { -1 => -1, 0 => -1, 1 => 1 }, d);
    assert_ne!(d, z);

    let d = z.clone().neg();
    assert_eq!(3, d.support_size());
    assert_eq!(finite_map! { -1 => -1, 0 => -1, 1 => 1 }, d);
    assert_ne!(d, z);

    let i: Map = d.clone().into_iter().collect();
    assert_eq!(i, d);

    z.increment(&1, 1);
    assert_eq!(2, z.support_size());
    assert_eq!(finite_map! { -1 => 1, 0 => 1 }, z);

    let mut z2 = z.add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(finite_map! { -1 => 2, 0 => 2 }, z2);

    let z2_owned = z.clone().add(z.clone());
    assert_eq!(2, z2_owned.support_size());
    assert_eq!(finite_map! { -1 => 2, 0 => 2 }, z2_owned);

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(finite_map! { -1 => 3, 0 => 3 }, z2);

    let z3 = z2.map(&|_x| 0);
    assert_eq!(1, z3.support_size());
    assert_eq!(finite_map! { 0 => 6 }, z3);

    let z4 = z2.filter(|&x| x >= 0);
    assert_eq!(1, z4.support_size());
    assert_eq!(finite_map! { 0 => 3 }, z4);

    z2.increment(&4, 2);
    let z5 = z2.flat_map(|x| iter::once(*x));
    assert_eq!(&z5, &z2);
    let z5 = z2.flat_map(|x| {
        if *x > 0 {
            (0..*x).into_iter().collect::<Vec<i64>>().into_iter()
        } else {
            iter::once(*x).collect::<Vec<i64>>().into_iter()
        }
    });
    assert_eq!(finite_map! { -1 => 3, 0 => 3, 4 => 2 }, z2);
    assert_eq!(finite_map! { -1 => 3, 0 => 5, 1 => 2, 2 => 2, 3 => 2 }, z5);

    let z6 = z2.match_keys(&z5, |w, w2| w + w2);
    assert_eq!(finite_map! { -1 => 6, 0 => 8 }, z6);
}
