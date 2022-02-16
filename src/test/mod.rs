#![cfg(test)]

use crate::{
    algebra::{AddByRef, FiniteHashMap, HasZero, MapBuilder, ZSet, ZSetHashMap},
    circuit::{operator_traits::SourceOperator, Root},
    finite_map,
    operator::{Apply2, Generator, Z1},
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

// Test a join on a relation containing tuples
#[test]
fn join_test() {
    let actual_data = Rc::new(RefCell::new(Vec::new()));
    let actual_data_clone = actual_data.clone();
    let root = Root::build(|circuit| {
        let source0 = circuit.add_source(make_tuple_generator());
        let source1 = circuit.add_source(make_tuple_generator());

        let join = |z0: &ZSetHashMap<(i64, i64), i64>, z1: &ZSetHashMap<(i64, i64), i64>| {
            z0.join(z1, |&(a, _)| a, |&(b, _)| b + 1, |&(a, _), &(b, _)| (a, b))
        };

        let join_op = Apply2::new(
            move |left: &FiniteHashMap<(i64, i64), i64>, right: &FiniteHashMap<(i64, i64), i64>| {
                join(left, right)
            },
        );

        circuit
            .add_binary_operator(join_op, &source0, &source1)
            .unwrap()
            .inspect(move |map| actual_data.borrow_mut().push(map.clone()));
    })
    .unwrap();

    for _ in 0..3 {
        root.step().unwrap()
    }

    let expected = vec![finite_map! {}, finite_map! {}, finite_map! { (2, 1) => 1 }];
    assert_eq!(&expected, actual_data_clone.borrow().deref());
}

// Test transitive closure
#[test]
fn transitive_closure() {
    let actual_data = Rc::new(RefCell::new(Vec::new()));
    let actual_data_clone = actual_data.clone();
    let root = Root::build(|circuit| {
        let mut z = ZSetHashMap::<(i64, i64), i64>::new();
        let mut t = 0;

        let gen = Generator::new(move || {
            let result = z.clone();
            if t == 0 {
                z.increment(&(1, 2), 1);
                z.increment(&(2, 3), 1);
            } else if t == 1 {
                z.increment(&(3, 4), 1);
            } else if t == 2 {
                z.increment(&(2, 3), -1);
            }
            t += 1;

            result
        });

        let source0 = circuit.add_source(gen);
        let circuit_output = circuit
            .iterate_with_condition(|child| {
                let i_output = source0.delta0(child).integrate();
                let (z1_output, z1_feedback) =
                    child.add_feedback_with_export(Z1::new(ZSetHashMap::<(i64, i64), i64>::new()));

                let add_output = i_output.plus(&z1_output.local);
                let join_func =
                    |z0: &ZSetHashMap<(i64, i64), i64>, z1: &ZSetHashMap<(i64, i64), i64>| {
                        z0.join(
                            z1,
                            |&(_, right)| right,
                            |&(left, _)| left,
                            |&(left, _), &(_, right)| (left, right),
                        )
                    };

                let join_op = Apply2::new(
                    move |left: &ZSetHashMap<(i64, i64), i64>,
                          right: &ZSetHashMap<(i64, i64), i64>| {
                        join_func(left, right)
                    },
                );

                let join_output = child
                    .add_binary_operator(join_op, &add_output, &i_output)
                    .unwrap();

                let distinct_output = join_output.plus(&i_output).apply(ZSet::distinct);
                z1_feedback.connect(&distinct_output);
                let differentiator_output = distinct_output.differentiate();
                let condition = differentiator_output.condition(HasZero::is_zero);
                Ok((condition, z1_output.export))
            })
            .unwrap();

        circuit_output.inspect(move |map| actual_data.borrow_mut().push(map.clone()));
    })
    .unwrap();

    for _ in 0..4 {
        root.step().unwrap()
    }

    let expected = vec![
        finite_map! {},
        finite_map! {
            (1, 2) => 1,
            (1, 3) => 1,
            (2, 3) => 1,
        },
        finite_map! {
            (1, 2) => 1,
            (1, 3) => 1,
            (1, 4) => 1,
            (2, 3) => 1,
            (2, 4) => 1,
            (3, 4) => 1,
        },
        finite_map! {
            (1, 2) => 1,
            (3, 4) => 1,
        },
    ];
    assert_eq!(&expected, actual_data_clone.borrow().deref());
}
