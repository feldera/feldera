use crate::algebra::{
    finite_map::{FiniteMap, MapBuilder},
    zset::{IndexedZSet, ZSet, ZSetHashMap},
    AddAssignByRef, AddByRef, CheckedInt, HasZero, NegByRef,
};

type CheckedI64 = CheckedInt<i64>;

#[test]
fn zset_integer_tests() {
    let mut z = ZSetHashMap::<i64, i64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!(finite_map! {}, z);
    assert_eq!(0, z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, i64>::zero());
    assert!(z.is_zero());

    let z2 = ZSetHashMap::<i64, i64>::new();
    assert_eq!(z, z2);

    z.increment(&0, 1);
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { 0 => 1 }, z);
    assert_eq!(1, z.lookup(&0));
    assert_eq!(0, z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, i64>::zero());
    assert!(!z.is_zero());

    z.increment_owned(2, 0);
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { 0 => 1 }, z);

    z.increment(&1, -1);
    assert_eq!(2, z.support_size());
    assert_eq!(finite_map! { 0 => 1, 1 => -1 }, z);

    z.increment_owned(-1, 1);
    assert_eq!(3, z.support_size());
    assert_eq!(finite_map! { -1 => 1, 0 => 1, 1 => -1 }, z);

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!(finite_map! { -1 => 1, 0 => 1 }, d);

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!(finite_map! { -1 => -1, 0 => -1, 1 => 1 }, d);
    assert_ne!(d, z);

    z.increment(&1, 1);
    assert_eq!(2, z.support_size());
    assert_eq!(finite_map! { -1 => 1, 0 => 1 }, z);

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(finite_map! { -1 => 2, 0 => 2 }, z2);

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(finite_map! { -1 => 3, 0 => 3 }, z2);
}

#[test]
fn checked_zset_integer_weights_tests() {
    let mut z = ZSetHashMap::<i64, CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!(finite_map! {}, z);
    assert_eq!(CheckedI64::new(0), z.lookup(&0)); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert!(z.is_zero());

    let z2 = ZSetHashMap::<i64, CheckedI64>::new();
    assert_eq!(z, z2);

    z.increment_owned(0, CheckedI64::new(1));
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { 0 => CheckedI64::new(1) }, z);
    assert_eq!(CheckedI64::new(1), z.lookup(&0));
    assert_eq!(CheckedI64::new(0), z.lookup(&1));
    assert_ne!(z, ZSetHashMap::<i64, CheckedI64>::zero());
    assert!(!z.is_zero());

    z.increment(&2, CheckedI64::new(0));
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { 0 => CheckedI64::new(1) }, z);

    z.increment_owned(1, CheckedI64::new(-1));
    assert_eq!(2, z.support_size());
    assert_eq!(
        finite_map! {
            0 => CheckedI64::new(1),
            1 => CheckedI64::new(-1),
        },
        z,
    );

    z.increment(&-1, CheckedI64::from(1));
    assert_eq!(3, z.support_size());
    assert_eq!(
        finite_map! {
            -1 => CheckedI64::new(1),
            0 => CheckedI64::new(1),
            1 => CheckedI64::new(-1),
        },
        z,
    );

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!(
        finite_map! {
            -1 => CheckedI64::new(1),
            0 => CheckedI64::new(1),
        },
        d,
    );

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!(
        finite_map! {
            -1 => CheckedI64::new(-1),
            0 => CheckedI64::new(-1),
            1 => CheckedI64::new(1),
        },
        d,
    );
    assert_ne!(d, z);

    z.increment_owned(1, CheckedI64::new(1));
    assert_eq!(2, z.support_size());
    assert_eq!(
        finite_map! {
            -1 => CheckedI64::new(1),
            0 => CheckedI64::new(1),
        },
        z,
    );

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(
        finite_map! {
            -1 => CheckedI64::new(2),
            0 => CheckedI64::new(2),
        },
        z2,
    );

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(
        finite_map! {
            -1 => CheckedI64::new(3),
            0 => CheckedI64::new(3),
        },
        z2,
    );
}

#[test]
fn zset_tuple_tests() {
    let mut z = ZSetHashMap::<(i64, i64), CheckedI64>::with_capacity(5);
    assert_eq!(0, z.support_size());
    assert_eq!(finite_map! {}, z);
    assert_eq!(CheckedI64::new(0), z.lookup(&(0, 0))); // not present -> weight 0
    assert_eq!(z, ZSetHashMap::<(i64, i64), CheckedI64>::zero());
    assert!(z.is_zero());

    let z2 = ZSetHashMap::<(i64, i64), CheckedI64>::new();
    assert_eq!(z, z2);

    z.increment_owned((0, 0), CheckedI64::new(1));
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { (0, 0) => CheckedI64::new(1) }, z,);
    assert_eq!(CheckedI64::from(1), z.lookup(&(0, 0)));
    assert_eq!(CheckedI64::from(0), z.lookup(&(0, 1)));
    assert_ne!(z, ZSetHashMap::<(i64, i64), CheckedI64>::zero());
    assert!(!z.is_zero());

    z.increment(&(2, 0), CheckedI64::new(0));
    assert_eq!(1, z.support_size());
    assert_eq!(finite_map! { (0, 0) => CheckedI64::new(1) }, z,);

    z.increment_owned((1, 0), CheckedI64::new(-1));
    assert_eq!(2, z.support_size());
    assert_eq!(
        finite_map! {
            (0, 0) => CheckedI64::new(1),
            (1, 0) => CheckedI64::new(-1),
        },
        z,
    );

    z.increment(&(-1, 0), CheckedI64::new(1));
    assert_eq!(3, z.support_size());
    assert_eq!(
        finite_map! {
            (-1, 0) => CheckedI64::new(1),
            (0, 0) => CheckedI64::new(1),
            (1, 0) => CheckedI64::new(-1),
        },
        z,
    );

    let d = z.distinct();
    assert_eq!(2, d.support_size());
    assert_eq!(
        finite_map! {
            (-1, 0) => CheckedI64::new(1),
            (0, 0) => CheckedI64::new(1),
        },
        d,
    );

    let d = z.neg_by_ref();
    assert_eq!(3, d.support_size());
    assert_eq!(
        finite_map! {
            (-1, 0) => CheckedI64::new(-1),
            (0, 0) => CheckedI64::new(-1),
            (1, 0) => CheckedI64::new(1),
        },
        d,
    );
    assert_ne!(d, z);

    z.increment_owned((1, 0), CheckedI64::new(1));
    assert_eq!(2, z.support_size());
    assert_eq!(
        finite_map! {
            (-1, 0) => CheckedI64::new(1),
            (0, 0) => CheckedI64::new(1),
        },
        z,
    );

    let mut z2 = z.clone().add_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(
        finite_map! {
            (-1, 0) => CheckedI64::new(2),
            (0, 0) => CheckedI64::new(2),
        },
        z2,
    );

    z2.add_assign_by_ref(&z);
    assert_eq!(2, z2.support_size());
    assert_eq!(
        finite_map! {
            (-1, 0) => CheckedI64::new(3),
            (0, 0) => CheckedI64::new(3),
        },
        z2
    );
}

#[test]
pub fn indexed_zset_tests() {
    let mut set = ZSetHashMap::<(i64, i64), CheckedI64>::with_capacity(5);
    assert_eq!(0, set.support_size());
    set.increment(&(0, 0), CheckedI64::new(1));
    set.increment_owned((2, 0), CheckedI64::new(2));
    set.increment(&(1, 0), CheckedI64::new(-1));
    set.increment_owned((-1, 0), CheckedI64::new(1));

    let mut partitioned = set.clone().partition(|(left, _)| left.abs() % 2);
    assert_eq!(
        finite_map! {
            0 => finite_map! {
                (0, 0) => CheckedI64::new(1),
                (2, 0) => CheckedI64::new(2),
            },
            1 => finite_map! {
                (-1, 0) => CheckedI64::new(1),
                (1, 0) => CheckedI64::new(-1),
            },
        },
        partitioned,
    );

    let z2 = partitioned.sum();
    assert_eq!(set, z2);

    set.increment(&(0, 0), CheckedI64::new(-1));
    partitioned.update_owned(0, |z| z.increment_owned((0, 0), CheckedI64::new(-1)));

    set.increment(&(2, 0), CheckedI64::new(-1));
    partitioned.update_owned(1, |z| z.increment_owned((2, 0), CheckedI64::new(-1)));

    set.increment(&(8, 0), CheckedI64::new(1));
    partitioned.update_owned(4, |z| z.increment_owned((8, 0), CheckedI64::new(1)));

    let z3 = partitioned.sum();
    assert_eq!(set, z3);
}
