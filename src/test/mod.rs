#![cfg(test)]

use crate::{
    algebra::{AddByRef, MapBuilder, ZSetHashMap},
    circuit::{operator_traits::SourceOperator, Root},
    finite_map,
    operator::Generator,
};
use std::{cell::RefCell, ops::Deref, rc::Rc};

fn make_generator() -> impl SourceOperator<ZSetHashMap<i64, i64>> {
    let mut z = ZSetHashMap::new();
    let mut count = 0i64;

    Generator::new(move || {
        count += 1;
        let result = z.clone();
        z.increment(&count, 1i64);
        result
    })
}

// Apply functions to a stream
#[test]
fn map() {
    let actual_data = Rc::new(RefCell::new(Vec::new()));
    let actual_data_clone = actual_data.clone();
    let root = Root::build(|circuit| {
        circuit
            .add_source(make_generator())
            .apply(|map| map.add_by_ref(map))
            .inspect(move |map| actual_data.borrow_mut().push(map.clone()));
    })
    .unwrap();

    for _ in 0..3 {
        root.step().unwrap()
    }

    let expected = vec![
        finite_map! {},
        finite_map! { 1 => 2 },
        finite_map! { 1 => 2, 2 => 2 },
    ];
    assert_eq!(&expected, actual_data_clone.borrow().deref());
}

fn make_tuple_generator() -> impl SourceOperator<ZSetHashMap<(i64, i64), i64>> {
    let mut z = ZSetHashMap::new();
    let mut count = 0;

    Generator::new(move || {
        count += 1;
        let result = z.clone();
        z.increment(&(count, count + 1), 1i64);
        result
    })
}

// Test a map on a relation containing tuples
#[test]
fn tuple_relation_test() {
    let actual_data = Rc::new(RefCell::new(Vec::new()));
    let actual_data_clone = actual_data.clone();
    let root = Root::build(|circuit| {
        circuit
            .add_source(make_tuple_generator())
            .apply(|map| map.add_by_ref(map))
            .inspect(move |map| actual_data.borrow_mut().push(map.clone()));
    })
    .unwrap();

    for _ in 0..3 {
        root.step().unwrap()
    }

    let expected = vec![
        finite_map! {},
        finite_map! { (1, 2) => 2 },
        finite_map! {
            (1, 2) => 2,
            (2, 3) => 2,
        },
    ];
    assert_eq!(&expected, actual_data_clone.borrow().deref());
}

// Test a filter on a relation containing tuples
/*
#[test]
fn tuple_filter_test() {
    let actual_data = Rc::new(RefCell::new(Vec::new()));
    let actual_data_clone = actual_data.clone();
    let root = Root::build(|circuit| {
        circuit
            .add_source(make_tuple_generator())
            .filter_keys::<_, _, ZSetHashMap<_, _>, _>(|(left, _)| left % 2 == 0)
            .inspect(move |map| actual_data.borrow_mut().push(map.clone()));
    })
    .unwrap();

    for _ in 0..3 {
        root.step().unwrap()
    }

    let expected = vec![finite_map! {}, finite_map! {}, finite_map! { (2, 3) => 1 }];
    assert_eq!(&expected, actual_data_clone.borrow().deref());
}
*/
