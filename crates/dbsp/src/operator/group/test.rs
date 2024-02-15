#![allow(clippy::type_complexity)]

use std::cmp::Ordering;

use crate::utils::{Tup3, Tup4};
use crate::{
    algebra::ZRingValue,
    indexed_zset,
    operator::CmpFunc,
    trace::{
        cursor::Cursor,
        test_batch::{assert_batch_eq, TestBatch},
        BatchReader, Trace,
    },
    utils::Tup2,
    CollectionHandle, DBData, DBWeight, OrdIndexedZSet, OutputHandle, RootCircuit, Runtime,
};
use anyhow::Result as AnyResult;
use proptest::{collection::vec, prelude::*};

fn input_trace(
    max_key: i32,
    max_val: i32,
    max_batch_size: usize,
    max_batches: usize,
) -> impl Strategy<Value = Vec<Vec<(i32, i32, i32)>>> {
    vec(
        vec((0..max_key, 0..max_val, -1..2), 0..max_batch_size),
        0..max_batches,
    )
}

impl<K, V, R> TestBatch<K, V, (), R>
where
    K: DBData,
    V: DBData,
    R: DBWeight + ZRingValue,
{
    fn topk_asc(&self, k: usize) -> TestBatch<K, V, (), R> {
        let mut result = Vec::new();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            let mut count = 0;
            while cursor.val_valid() && count < k {
                let w = cursor.weight();
                result.push(((cursor.key().clone(), cursor.val().clone(), ()), w));
                count += 1;
                cursor.step_val();
            }
            cursor.step_key();
        }

        TestBatch::from_data(&result)
    }

    fn topk_desc(&self, k: usize) -> TestBatch<K, V, (), R> {
        let mut result = Vec::new();

        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut count = 0;

            cursor.fast_forward_vals();
            while cursor.val_valid() && count < k {
                let w = cursor.weight();
                result.push(((cursor.key().clone(), cursor.val().clone(), ()), w));
                count += 1;
                cursor.step_val_reverse();
            }
            cursor.step_key();
        }

        TestBatch::from_data(&result)
    }

    fn lag(&self, lag: usize) -> TestBatch<K, Tup2<V, Option<V>>, (), R> {
        let mut result = Vec::new();
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut vals = Vec::new();

            while cursor.val_valid() {
                let w = cursor.weight();
                vals.push((cursor.val().clone(), w.neg()));
                cursor.step_val();
            }

            for i in 0..vals.len() {
                let (v, w) = vals[i].clone();
                let old_v = if i >= lag {
                    Some(vals[i - lag].0.clone())
                } else {
                    None
                };
                result.push(((cursor.key().clone(), Tup2(v, old_v), ()), w.neg()));
            }

            cursor.step_key();
        }

        TestBatch::from_data(&result)
    }

    fn lead(&self, lag: usize) -> TestBatch<K, Tup2<V, Option<V>>, (), R> {
        let mut result = Vec::new();
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut vals = Vec::new();

            while cursor.val_valid() {
                let w = cursor.weight();
                vals.push((cursor.val().clone(), w.neg()));
                cursor.step_val();
            }

            for i in 0..vals.len() {
                let (v, w) = vals[i].clone();
                let old_v = if vals.len() - i > lag {
                    Some(vals[i + lag].0.clone())
                } else {
                    None
                };
                result.push(((cursor.key().clone(), Tup2(v, old_v), ()), w.neg()));
            }

            cursor.step_key();
        }

        TestBatch::from_data(&result)
    }
}

fn topk_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, Tup2<i32, i32>>,
    OutputHandle<OrdIndexedZSet<i32, i32, i32>>,
    OutputHandle<OrdIndexedZSet<i32, i32, i32>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

    let topk_asc_handle = input_stream.topk_asc(5).integrate().output();
    let topk_desc_handle = input_stream.topk_desc(5).integrate().output();

    Ok((input_handle, topk_asc_handle, topk_desc_handle))
}

fn topk_custom_ord_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, Tup2<Tup3<String, i32, i32>, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup3<String, i32, i32>, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup4<i64, String, i32, i32>, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup4<i64, String, i32, i32>, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup4<i64, String, i32, i32>, i32>>,
)> {
    let (input_stream, input_handle) =
        circuit.add_input_indexed_zset::<i32, Tup3<String, i32, i32>, i32>();

    // Sort by the 2nd column ascending and by the 3rd column descending.
    struct AscDesc;

    impl CmpFunc<Tup3<String, i32, i32>> for AscDesc {
        fn cmp(
            left: &Tup3<String, i32, i32>,
            right: &Tup3<String, i32, i32>,
        ) -> std::cmp::Ordering {
            let ord = left.1.cmp(&right.1);
            let res = if ord == Ordering::Equal {
                let ord = right.2.cmp(&left.2);
                if ord == Ordering::Equal {
                    left.0.cmp(&right.0)
                } else {
                    ord
                }
            } else {
                ord
            };
            res
        }
    }

    let topk_handle = input_stream
        .topk_custom_order::<AscDesc>(5)
        .integrate()
        .output();

    let topk_rank_handle = input_stream
        .topk_rank_custom_order::<AscDesc, _, _, _>(
            5,
            |Tup3(_, x1, y1), Tup3(_, x2, y2)| x1 == x2 && y1 == y2,
            |rank, Tup3(s, x, y)| Tup4(rank, s.clone(), *x, *y),
        )
        .integrate()
        .output();

    let topk_dense_rank_handle = input_stream
        .topk_dense_rank_custom_order::<AscDesc, _, _, _>(
            5,
            |Tup3(_, x1, y1), Tup3(_, x2, y2)| x1 == x2 && y1 == y2,
            |rank, Tup3(s, x, y)| Tup4(rank, s.clone(), *x, *y),
        )
        .integrate()
        .output();

    let topk_row_number_handle = input_stream
        .topk_row_number_custom_order::<AscDesc, _, _>(5, |rank, Tup3(s, x, y)| {
            Tup4(rank, s.clone(), *x, *y)
        })
        .integrate()
        .output();

    Ok((
        input_handle,
        topk_handle,
        topk_rank_handle,
        topk_dense_rank_handle,
        topk_row_number_handle,
    ))
}

fn lag_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, Tup2<i32, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup2<i32, Option<i32>>, i32>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

    let lag_handle = input_stream.lag(3, |v| v.cloned()).integrate().output();

    Ok((input_handle, lag_handle))
}

fn lead_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, Tup2<i32, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup2<i32, Option<i32>>, i32>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

    let lead_handle = input_stream.lead(3, |v| v.cloned()).integrate().output();

    Ok((input_handle, lead_handle))
}

#[test]
fn test_topk_custom_ord() {
    let (
        mut dbsp,
        (
            input_handle,
            topk_handle,
            topk_rank_handle,
            topk_dense_rank_handle,
            topk_row_number_handle,
        ),
    ) = Runtime::init_circuit(4, topk_custom_ord_test_circuit).unwrap();

    let trace = vec![
        vec![
            (1, Tup3("foo".to_string(), 10, 100), 1),
            (1, Tup3("foo".to_string(), 9, 99), 1),
            (1, Tup3("foo".to_string(), 8, 98), 1),
            (1, Tup3("foo".to_string(), 10, 90), 1),
            (1, Tup3("foo".to_string(), 9, 98), 1),
            (1, Tup3("foo".to_string(), 8, 97), 1),
        ],
        vec![
            (1, Tup3("foo".to_string(), 10, 80), 1),
            (1, Tup3("foo".to_string(), 9, 97), 1),
            (1, Tup3("foo".to_string(), 8, 96), 1),
            (1, Tup3("foo".to_string(), 10, 79), 1),
            (1, Tup3("foo".to_string(), 9, 96), 1),
            (1, Tup3("foo".to_string(), 8, 95), 1),
        ],
        vec![
            (1, Tup3("foo".to_string(), 9, 99), -1),
            (1, Tup3("foo".to_string(), 8, 98), -1),
            (1, Tup3("foo".to_string(), 9, 98), -1),
            (1, Tup3("foo".to_string(), 8, 97), -1),
        ],
        // Two values with the same rank
        vec![(1, Tup3("bar".to_string(), 8, 96), 1)],
        vec![(1, Tup3("foo".to_string(), 7, 96), 1)],
        // >5 elements with the same rank.
        vec![
            (1, Tup3("baz".to_string(), 8, 96), 1),
            (1, Tup3("buzz".to_string(), 8, 96), 1),
            (1, Tup3("foobar".to_string(), 8, 96), 1),
            (1, Tup3("fubar".to_string(), 8, 96), 1),
        ],
        // non-unit weights
        vec![
            (1, Tup3("foo".to_string(), 7, 96), 1),
            (1, Tup3("baz".to_string(), 8, 96), 1),
        ],
    ];
    let mut expected_output = vec![indexed_zset! {
        1 => {{Tup3("foo".to_string(), 8, 98)} => 1, {Tup3("foo".to_string(), 8, 97)} => 1, {Tup3("foo".to_string(), 9, 99)} => 1, {Tup3("foo".to_string(), 9, 98)} => 1, {Tup3("foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup3("foo".to_string(), 8, 98)} => 1, {Tup3("foo".to_string(), 8, 97)} => 1, {Tup3("foo".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 95)} => 1, {Tup3("foo".to_string(), 9, 99)} => 1},
    },
    indexed_zset! {
        1 => {{Tup3("foo".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 95)} => 1, {Tup3("foo".to_string(), 9, 97)} => 1, {Tup3("foo".to_string(), 9, 96)} => 1, {Tup3("foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup3("bar".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 95)} => 1, {Tup3("foo".to_string(), 9, 97)} => 1, {Tup3("foo".to_string(), 9, 96)} => 1}
    },
    indexed_zset! {
        1 => {{Tup3("foo".to_string(), 7, 96)} => 1, {Tup3("bar".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 95)} => 1, {Tup3("foo".to_string(), 9, 97)} => 1},
    },
    indexed_zset! {
        1 => {{Tup3("foo".to_string(), 7, 96)} => 1, {Tup3("bar".to_string(), 8, 96)} => 1, {Tup3("baz".to_string(), 8, 96)} => 1, {Tup3("buzz".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup3("foo".to_string(), 7, 96)} => 2, {Tup3("bar".to_string(), 8, 96)} => 1, {Tup3("baz".to_string(), 8, 96)} => 2, {Tup3("buzz".to_string(), 8, 96)} => 1, {Tup3("foo".to_string(), 8, 96)} => 1},
    }]
    .into_iter();

    let mut expected_ranked_output = vec![indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 98)} => 1, {Tup4(2, "foo".to_string(), 8, 97)} => 1, {Tup4(3, "foo".to_string(), 9, 99)} => 1, {Tup4(4, "foo".to_string(), 9, 98)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 98)} => 1, {Tup4(2, "foo".to_string(), 8, 97)} => 1, {Tup4(3, "foo".to_string(), 8, 96)} => 1, {Tup4(4, "foo".to_string(), 8, 95)} => 1, {Tup4(5, "foo".to_string(), 9, 99)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 95)} => 1, {Tup4(3, "foo".to_string(), 9, 97)} => 1, {Tup4(4, "foo".to_string(), 9, 96)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "bar".to_string(), 8, 96)} => 1, {Tup4(1, "foo".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 95)} => 1, {Tup4(4, "foo".to_string(), 9, 97)} => 1, {Tup4(5, "foo".to_string(), 9, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 96)} => 1, {Tup4(4, "foo".to_string(), 8, 95)} => 1, {Tup4(5, "foo".to_string(), 9, 97)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(2, "baz".to_string(), 8, 96)} => 1, {Tup4(2, "buzz".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foobar".to_string(), 8, 96)} => 1, {Tup4(2, "fubar".to_string(), 8, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 2, {Tup4(3, "bar".to_string(), 8, 96)} => 1, {Tup4(3, "baz".to_string(), 8, 96)} => 2, {Tup4(3, "buzz".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 96)} => 1, {Tup4(3, "foobar".to_string(), 8, 96)} => 1, {Tup4(3, "fubar".to_string(), 8, 96)} => 1},
    }]
    .into_iter();

    let mut expected_dense_ranked_output = vec![indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 98)} => 1, {Tup4(2, "foo".to_string(), 8, 97)} => 1, {Tup4(3, "foo".to_string(), 9, 99)} => 1, {Tup4(4, "foo".to_string(), 9, 98)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 98)} => 1, {Tup4(2, "foo".to_string(), 8, 97)} => 1, {Tup4(3, "foo".to_string(), 8, 96)} => 1, {Tup4(4, "foo".to_string(), 8, 95)} => 1, {Tup4(5, "foo".to_string(), 9, 99)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 95)} => 1, {Tup4(3, "foo".to_string(), 9, 97)} => 1, {Tup4(4, "foo".to_string(), 9, 96)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "bar".to_string(), 8, 96)} => 1, {Tup4(1, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 95)} => 1, {Tup4(3, "foo".to_string(), 9, 97)} => 1, {Tup4(4, "foo".to_string(), 9, 96)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 95)} => 1, {Tup4(4, "foo".to_string(), 9, 97)} => 1, {Tup4(5, "foo".to_string(), 9, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(2, "baz".to_string(), 8, 96)} => 1, {Tup4(2, "buzz".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foobar".to_string(), 8, 96)} => 1, {Tup4(2, "fubar".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 95)} => 1, {Tup4(4, "foo".to_string(), 9, 97)} => 1, {Tup4(5, "foo".to_string(), 9, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 2, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(2, "baz".to_string(), 8, 96)} => 2, {Tup4(2, "buzz".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foobar".to_string(), 8, 96)} => 1, {Tup4(2, "fubar".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 95)} => 1, {Tup4(4, "foo".to_string(), 9, 97)} => 1, {Tup4(5, "foo".to_string(), 9, 96)} => 1},
    }]
    .into_iter();

    let mut expected_row_number_output = vec![indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 98)} => 1, {Tup4(2, "foo".to_string(), 8, 97)} => 1, {Tup4(3, "foo".to_string(), 9, 99)} => 1, {Tup4(4, "foo".to_string(), 9, 98)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 98)} => 1, {Tup4(2, "foo".to_string(), 8, 97)} => 1, {Tup4(3, "foo".to_string(), 8, 96)} => 1, {Tup4(4, "foo".to_string(), 8, 95)} => 1, {Tup4(5, "foo".to_string(), 9, 99)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 95)} => 1, {Tup4(3, "foo".to_string(), 9, 97)} => 1, {Tup4(4, "foo".to_string(), 9, 96)} => 1, {Tup4(5, "foo".to_string(), 10, 100)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "bar".to_string(), 8, 96)} => 1, {Tup4(2, "foo".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 95)} => 1, {Tup4(4, "foo".to_string(), 9, 97)} => 1, {Tup4(5, "foo".to_string(), 9, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(3, "foo".to_string(), 8, 96)} => 1, {Tup4(4, "foo".to_string(), 8, 95)} => 1, {Tup4(5, "foo".to_string(), 9, 97)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "bar".to_string(), 8, 96)} => 1, {Tup4(3, "baz".to_string(), 8, 96)} => 1, {Tup4(4, "buzz".to_string(), 8, 96)} => 1, {Tup4(5, "foo".to_string(), 8, 96)} => 1},
    },
    indexed_zset! {
        1 => {{Tup4(1, "foo".to_string(), 7, 96)} => 1, {Tup4(2, "foo".to_string(), 7, 96)} => 1, {Tup4(3, "bar".to_string(), 8, 96)} => 1, {Tup4(4, "baz".to_string(), 8, 96)} => 1, {Tup4(5, "baz".to_string(), 8, 96)} => 1},
    }]
    .into_iter();

    for batch in trace.into_iter() {
        for (k, v, r) in batch.into_iter() {
            input_handle.push(k, Tup2(v, r));
        }
        dbsp.step().unwrap();

        let topk_result = topk_handle.consolidate();

        assert_batch_eq(&topk_result, &expected_output.next().unwrap());

        let topk_rank_result = topk_rank_handle.consolidate();

        assert_batch_eq(&topk_rank_result, &expected_ranked_output.next().unwrap());

        let topk_dense_rank_result = topk_dense_rank_handle.consolidate();

        assert_batch_eq(
            &topk_dense_rank_result,
            &expected_dense_ranked_output.next().unwrap(),
        );

        let topk_row_number_result = topk_row_number_handle.consolidate();

        assert_batch_eq(
            &topk_row_number_result,
            &expected_row_number_output.next().unwrap(),
        );
    }
}

proptest! {
    #[test]
    fn test_topk(trace in input_trace(5, 1_000, 200, 20)) {
        let (mut dbsp, (input_handle, topk_asc_handle, topk_desc_handle)) = Runtime::init_circuit(4, topk_test_circuit).unwrap();

        let mut ref_trace = TestBatch::new(None, "");

        for batch in trace.into_iter() {
            let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

            let ref_batch = TestBatch::from_data(&records);
            ref_trace.insert(ref_batch);

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, Tup2(v, r));
            }
            dbsp.step().unwrap();

            let topk_asc_result = topk_asc_handle.consolidate();
            let topk_desc_result = topk_desc_handle.consolidate();

            let ref_topk_asc = ref_trace.topk_asc(5);
            let ref_topk_desc = ref_trace.topk_desc(5);

            assert_batch_eq(&topk_asc_result, &ref_topk_asc);
            assert_batch_eq(&topk_desc_result, &ref_topk_desc);
        }
    }

    #[test]
    fn test_lag(trace in input_trace(5, 100, 200, 20)) {
        let (mut dbsp, (input_handle, lag_handle)) = Runtime::init_circuit(4, lag_test_circuit).unwrap();

        let mut ref_trace = TestBatch::new(None, "");

        for batch in trace.into_iter() {
            let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

            let ref_batch = TestBatch::from_data(&records);
            ref_trace.insert(ref_batch);

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, Tup2(v, r));
            }
            dbsp.step().unwrap();

            let lag_result = lag_handle.consolidate();
            let ref_lag = ref_trace.lag(3);

            assert_batch_eq(&lag_result, &ref_lag);
        }
    }

    #[test]
    fn test_lead(trace in input_trace(5, 100, 200, 20)) {
        let (mut dbsp, (input_handle, lead_handle)) = Runtime::init_circuit(4, lead_test_circuit).unwrap();

        let mut ref_trace = TestBatch::new(None, "");

        for batch in trace.into_iter() {
            let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

            let ref_batch = TestBatch::from_data(&records);
            ref_trace.insert(ref_batch);

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, Tup2(v, r));
            }
            dbsp.step().unwrap();

            let lead_result = lead_handle.consolidate();
            let ref_lead = ref_trace.lead(3);

            assert_batch_eq(&lead_result, &ref_lead);
        }
    }
}
