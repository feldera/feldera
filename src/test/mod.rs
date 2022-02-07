/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) $CURRENT_YEAR VMware, Inc
*/

// Tests for circuits operating on z-sets

#[cfg(test)]
mod tests {
    use crate::{
        algebra::{
            finite_map::{FiniteHashMap, FiniteMap},
            zset::{tests::TestTuple, ZSet, ZSetHashMap},
            AddByRef, HasZero,
        },
        circuit::{
            operator::{Apply2, Generator, NestedSource, Z1},
            operator_traits::SourceOperator,
            *,
        },
    };
    use std::{cell::RefCell, ops::Deref, rc::Rc};

    fn make_generator() -> impl SourceOperator<ZSetHashMap<i64, i64>> {
        let mut z = ZSetHashMap::<i64, i64>::new();
        let mut count = 0i64;
        Generator::new(move || {
            count = count + 1;
            let result = z.clone();
            z.increment(&count, 1i64);
            result
        })
    }

    // Circuit that converts input data to strings
    #[test]
    fn stringify() {
        let actual_data = Rc::new(RefCell::new(Vec::<String>::new()));
        let actual_data_clone = actual_data.clone();
        let root = Root::build(|circuit| {
            let output = circuit
                .add_source(make_generator())
                .apply(ToString::to_string);
            output.inspect(move |s: &String| actual_data.borrow_mut().push(s.clone()));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap()
        }
        let expected_output = vec!["{}", "{1=>1}", "{1=>1,2=>1}"];
        assert_eq!(&expected_output, actual_data_clone.borrow().deref());
    }

    // Test zset map function - aka select
    #[test]
    fn map() {
        let actual_data = Rc::new(RefCell::new(Vec::<String>::new()));
        let actual_data_clone = actual_data.clone();
        let root = Root::build(|circuit| {
            circuit
                .add_source(make_generator())
                .apply(|z: &FiniteHashMap<i64, i64>| z.add_by_ref(&z.clone()))
                .apply(ToString::to_string)
                .inspect(move |s: &String| actual_data.borrow_mut().push(s.clone()));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap()
        }
        let expected_output = vec!["{}", "{1=>2}", "{1=>2,2=>2}"];
        assert_eq!(&expected_output, actual_data_clone.borrow().deref());
    }

    fn make_tuple_generator() -> impl SourceOperator<ZSetHashMap<TestTuple, i64>> {
        let mut z = ZSetHashMap::<TestTuple, i64>::new();
        let mut count = 0;
        Generator::new(move || {
            count = count + 1;
            let result = z.clone();
            z.increment(&TestTuple::new(count, count + 1), 1i64);
            result
        })
    }

    // Test a map on a relation containing tuples
    #[test]
    fn tuple_relation_test() {
        let actual_data = Rc::new(RefCell::new(Vec::<String>::new()));
        let actual_data_clone = actual_data.clone();
        let root = Root::build(|circuit| {
            circuit
                .add_source(make_tuple_generator())
                .apply(|z: &FiniteHashMap<TestTuple, i64>| z.add_by_ref(&z.clone()))
                .apply(ToString::to_string)
                .inspect(move |s: &String| actual_data.borrow_mut().push(s.clone()));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap()
        }
        let expected_output = vec!["{}", "{(1,2)=>2}", "{(1,2)=>2,(2,3)=>2}"];
        assert_eq!(&expected_output, actual_data_clone.borrow().deref());
    }

    // Test a filter on a relation containing tuples
    #[test]
    fn tuple_filter_test() {
        let actual_data = Rc::new(RefCell::new(Vec::<String>::new()));
        let actual_data_clone = actual_data.clone();
        let root = Root::build(|circuit| {
            circuit
                .add_source(make_tuple_generator())
                .filter_keys::<_, _, ZSetHashMap<_, _>, _>(|tt: &TestTuple| tt.left % 2 == 0)
                .apply(ToString::to_string)
                .inspect(move |s: &String| actual_data.borrow_mut().push(s.clone()));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap()
        }
        let expected_output = vec!["{}", "{}", "{(2,3)=>1}"];
        assert_eq!(&expected_output, actual_data_clone.borrow().deref());
    }

    // Test a join on a relation containing tuples
    #[test]
    fn join_test() {
        let actual_data = Rc::new(RefCell::new(Vec::<String>::new()));
        let actual_data_clone = actual_data.clone();
        let root = Root::build(|circuit| {
            let source0 = circuit.add_source(make_tuple_generator());
            let source1 = circuit.add_source(make_tuple_generator());
            let join = |z0: &ZSetHashMap<TestTuple, i64>, z1: &ZSetHashMap<TestTuple, i64>| {
                z0.join(z1, &|l| l.left, &|r| r.left + 1, |l, r| {
                    TestTuple::new(l.left, r.left)
                })
            };
            let join_op = Apply2::new(
                move |left: &FiniteHashMap<TestTuple, i64>,
                      right: &FiniteHashMap<TestTuple, i64>| join(left, right),
            );
            circuit
                .add_binary_operator(join_op, &source0, &source1)
                .unwrap()
                .apply(ToString::to_string)
                .inspect(move |s: &String| actual_data.borrow_mut().push(s.clone()));
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap()
        }
        let expected_output = vec!["{}", "{}", "{(2,1)=>1}"];
        assert_eq!(&expected_output, actual_data_clone.borrow().deref());
    }

    // Test transitive closure
    #[test]
    fn transitive_closure() {
        let actual_data = Rc::new(RefCell::new(Vec::<String>::new()));
        let actual_data_clone = actual_data.clone();
        let root = Root::build(|circuit| {
            let mut z = ZSetHashMap::<TestTuple, i64>::new();
            let mut t = 0;
            let gen = Generator::new(move || {
                let result = z.clone();
                if t == 0 {
                    z.increment(&TestTuple::new(1, 2), 1);
                    z.increment(&TestTuple::new(2, 3), 1);
                } else if t == 1 {
                    z.increment(&TestTuple::new(3, 4), 1);
                } else if t == 2 {
                    z.increment(&TestTuple::new(2, 3), -1);
                }
                t = t + 1;
                result
            });
            let source0 = circuit.add_source(gen);
            let circuit_output = circuit
                .iterate(|child| {
                    let ns = NestedSource::new(
                        source0,
                        child,
                        move |z: &mut ZSetHashMap<TestTuple, i64>| *z = z.clone(),
                    );
                    let i_output = child.add_source(ns).integrate();
                    let (z1_output, z1_feedback) =
                        child.add_feedback(Z1::new(ZSetHashMap::<TestTuple, i64>::new()));
                    let add_output = i_output.plus(&z1_output);
                    let join_func =
                        |z0: &ZSetHashMap<TestTuple, i64>, z1: &ZSetHashMap<TestTuple, i64>| {
                            z0.join(z1, &|l| l.right, &|r| r.left, |l, r| {
                                TestTuple::new(l.left, r.right)
                            })
                        };
                    let join_op = Apply2::new(
                        move |left: &ZSetHashMap<TestTuple, i64>,
                              right: &ZSetHashMap<TestTuple, i64>| {
                            join_func(left, right)
                        },
                    );
                    let join_output = child
                        .add_binary_operator(join_op, &add_output, &i_output)
                        .unwrap();
                    let distinct_output = join_output.plus(&i_output).apply(ZSet::distinct);
                    z1_feedback.connect(&distinct_output);
                    let differentiator_output = distinct_output.differentiate();
                    let termination_test = move || {
                        // TODO: safe termination protocol
                        unsafe { differentiator_output.take() }.is_zero()
                    };
                    Ok((termination_test, distinct_output.leave()))
                })
                .unwrap();

            circuit_output
                .apply(ToString::to_string)
                .inspect(move |s: &String| actual_data.borrow_mut().push(s.clone()));
        })
        .unwrap();

        for _ in 0..4 {
            root.step().unwrap()
        }
        let expected_output = vec![
            "{}",
            "{(1,2)=>1,(1,3)=>1,(2,3)=>1}",
            "{(1,2)=>1,(1,3)=>1,(1,4)=>1,(2,3)=>1,(2,4)=>1,(3,4)=>1}",
            "{(1,2)=>1,(3,4)=>1}",
        ];
        assert_eq!(&expected_output, actual_data_clone.borrow().deref());
    }
}
