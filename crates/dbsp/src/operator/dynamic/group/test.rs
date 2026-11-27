#![allow(clippy::type_complexity)]

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, btree_map::Entry},
    ops::RangeInclusive,
    sync::Arc,
    time::Duration,
};

use super::rank::test::{
    OrderByFirstTupleField, multiset_to_indexed_zset, reference_dense_rank_custom_order,
    reference_rank_custom_order, reference_row_number_custom_order,
};
use crate::utils::test::CIRCUIT_CASES;
use crate::{
    DBData, DBSPHandle, DynZWeight, RootCircuit, Runtime, ZWeight,
    circuit::{
        CircuitConfig, GlobalNodeId,
        metadata::{MetaItem, STATE_RECORDS_COUNT},
    },
    dynamic::{DowncastTrait, DynData, DynPair},
    indexed_zset,
    operator::{CmpFunc, IndexedZSetHandle, InputHandle, OutputHandle},
    trace::{
        Cursor, SpineSnapshot as DynSpineSnapshot, Trace, TraceRole,
        test::test_batch::{TestBatch, TestBatchFactories, assert_batch_eq, assert_typed_batch_eq},
    },
    typed_batch::{
        BatchReader, DynBatchReader, DynOrdIndexedZSet, IndexedZSetReader, OrdIndexedZSet,
        SpineSnapshot, TypedBatch,
    },
    utils::{Tup2, Tup3, Tup4},
};
use anyhow::Result as AnyResult;
use feldera_storage::tokio::TOKIO;
use proptest::{collection::vec, prelude::*};

fn input_trace(
    max_key: i32,
    max_val: i32,
    max_batch_size: usize,
    max_batches: usize,
) -> impl Strategy<Value = Vec<Vec<(i32, i32, ZWeight)>>> {
    vec(
        vec((0..max_key, 0..max_val, -1..2i64), 0..max_batch_size),
        0..max_batches,
    )
}

impl TestBatch<DynData, DynData, (), DynZWeight> {
    fn topk_asc<K, V>(&self, k: usize) -> Self
    where
        K: DBData,
        V: DBData,
    {
        let mut result: Vec<((K, V, ()), ZWeight)> = Vec::new();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            let mut count = 0;
            while cursor.val_valid() && count < k {
                let w = *cursor.weight().downcast_checked::<ZWeight>();
                result.push((
                    (
                        cursor.key().downcast_checked::<K>().clone(),
                        cursor.val().downcast_checked::<V>().clone(),
                        (),
                    ),
                    w,
                ));
                count += 1;
                cursor.step_val();
            }
            cursor.step_key();
        }

        TestBatch::from_typed_data(result.as_slice())
    }

    fn topk_desc<K, V>(&self, k: usize) -> Self
    where
        K: DBData,
        V: DBData,
    {
        let mut result: Vec<((K, V, ()), ZWeight)> = Vec::new();

        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut count = 0;

            cursor.fast_forward_vals();
            while cursor.val_valid() && count < k {
                let w = *cursor.weight().downcast_checked::<ZWeight>();
                result.push((
                    (
                        cursor.key().downcast_checked::<K>().clone(),
                        cursor.val().downcast_checked::<V>().clone(),
                        (),
                    ),
                    w,
                ));
                count += 1;
                cursor.step_val_reverse();
            }
            cursor.step_key();
        }

        TestBatch::from_typed_data(result.as_slice())
    }

    fn lag<K, V>(
        &self,
        lag: usize,
    ) -> TestBatch<DynData, DynPair<DynData, DynData> /* <(V, Option<V>)> */, (), DynZWeight>
    where
        K: DBData,
        V: DBData,
    {
        let mut result = Vec::new();
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut vals = Vec::new();

            while cursor.val_valid() {
                // Note: we skip weights <= 0.
                let w = *cursor.weight().downcast_checked::<ZWeight>();
                for _i in 0..w {
                    vals.push(cursor.val().downcast_checked::<V>().clone());
                }
                cursor.step_val();
            }

            for i in 0..vals.len() {
                let v = vals[i].clone();
                let old_v = if i >= lag {
                    Some(vals[i - lag].clone())
                } else {
                    None
                };
                result.push((
                    (
                        cursor.key().downcast_checked::<K>().clone(),
                        Tup2(v, old_v),
                        (),
                    ),
                    1,
                ));
            }

            cursor.step_key();
        }

        TestBatch::from_typed_data(result.as_slice())
    }

    fn lead<K, V>(
        &self,
        lag: usize,
    ) -> TestBatch<DynData, DynPair<DynData, DynData> /* <(V, Option<V>)> */, (), DynZWeight>
    where
        K: DBData,
        V: DBData,
    {
        let mut result = Vec::new();
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let mut vals = Vec::new();

            while cursor.val_valid() {
                let w = *cursor.weight().downcast_checked::<ZWeight>();
                // Note: we skip weights <= 0.
                for _i in 0..w {
                    vals.push(cursor.val().downcast_checked::<V>().clone());
                }
                cursor.step_val();
            }

            for i in 0..vals.len() {
                let v = vals[i].clone();
                let old_v = if vals.len() - i > lag {
                    Some(vals[i + lag].clone())
                } else {
                    None
                };
                result.push((
                    (
                        cursor.key().downcast_checked::<K>().clone(),
                        Tup2(v, old_v),
                        (),
                    ),
                    1,
                ));
            }

            cursor.step_key();
        }

        TestBatch::from_typed_data(result.as_slice())
    }
}

fn topk_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    IndexedZSetHandle<i32, i32>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<i32, i32>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<i32, i32>>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();

    let topk_asc_handle = input_stream
        .topk_asc(5)
        .accumulate_integrate()
        .accumulate_output();
    let topk_desc_handle = input_stream
        .topk_desc(5)
        .accumulate_integrate()
        .accumulate_output();

    Ok((input_handle, topk_asc_handle, topk_desc_handle))
}

fn topk_custom_ord_test_circuit(
    circuit: &mut RootCircuit,
    k: usize,
) -> AnyResult<(
    IndexedZSetHandle<i32, Tup3<String, i32, i32>>,
    OutputHandle<OrdIndexedZSet<i32, Tup3<String, i32, i32>>>,
    OutputHandle<OrdIndexedZSet<i32, Tup4<i64, String, i32, i32>>>,
    OutputHandle<OrdIndexedZSet<i32, Tup4<i64, String, i32, i32>>>,
    OutputHandle<OrdIndexedZSet<i32, Tup4<i64, String, i32, i32>>>,
)> {
    let (input_stream, input_handle) =
        circuit.add_input_indexed_zset::<i32, Tup3<String, i32, i32>>();

    // Sort by the 2nd column ascending and by the 3rd column descending.
    struct AscDesc;

    impl CmpFunc<Tup3<String, i32, i32>> for AscDesc {
        fn cmp(
            left: &Tup3<String, i32, i32>,
            right: &Tup3<String, i32, i32>,
        ) -> std::cmp::Ordering {
            let ord = left.1.cmp(&right.1);

            if ord == Ordering::Equal {
                let ord = right.2.cmp(&left.2);
                if ord == Ordering::Equal {
                    left.0.cmp(&right.0)
                } else {
                    ord
                }
            } else {
                ord
            }
        }
    }

    let topk_handle = input_stream
        .topk_custom_order::<AscDesc>(k)
        .integrate()
        .output();

    let topk_rank_handle = input_stream
        .topk_rank_custom_order::<AscDesc, _, _, _>(
            k,
            |Tup3(_, x1, y1), Tup3(_, x2, y2)| x1 == x2 && y1 == y2,
            |rank, Tup3(s, x, y)| Tup4(rank, s.clone(), *x, *y),
        )
        .integrate()
        .output();

    let topk_dense_rank_handle = input_stream
        .topk_dense_rank_custom_order::<AscDesc, _, _, _>(
            k,
            |Tup3(_, x1, y1), Tup3(_, x2, y2)| x1 == x2 && y1 == y2,
            |rank, Tup3(s, x, y)| Tup4(rank, s.clone(), *x, *y),
        )
        .integrate()
        .output();

    let topk_row_number_handle = input_stream
        .topk_row_number_custom_order::<AscDesc, _, _>(k, |rank, Tup3(s, x, y)| {
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
    IndexedZSetHandle<i32, i32>,
    OutputHandle<
        TypedBatch<
            i32,
            Tup2<i32, Option<i32>>,
            ZWeight,
            DynSpineSnapshot<
                DynOrdIndexedZSet<
                    DynData, /* <i32> */
                    DynPair<DynData /* <i32> */, DynData /* <Option<i32>> */>,
                >,
            >,
        >,
    >,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();

    let lag_handle = input_stream
        .lag(3, |v| v.cloned())
        .accumulate_integrate()
        .accumulate_output();

    Ok((input_handle, lag_handle))
}

fn lead_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    IndexedZSetHandle<i32, i32>,
    OutputHandle<
        TypedBatch<
            i32,
            Tup2<i32, Option<i32>>,
            ZWeight,
            DynSpineSnapshot<
                DynOrdIndexedZSet<
                    DynData, /* <i32> */
                    DynPair<DynData /* <i32> */, DynData /* <Option<i32>> */>,
                >,
            >,
        >,
    >,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();

    let lead_handle = input_stream
        .lag(-3, |v| v.cloned())
        .accumulate_integrate()
        .accumulate_output();

    Ok((input_handle, lead_handle))
}

fn lag_custom_order_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    IndexedZSetHandle<i32, Tup2<i32, String>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<i32, Tup3<i32, String, Option<Tup2<i32, String>>>>>>,
)> {
    struct AscDesc;

    impl CmpFunc<Tup2<i32, String>> for AscDesc {
        fn cmp(left: &Tup2<i32, String>, right: &Tup2<i32, String>) -> std::cmp::Ordering {
            let ord = left.0.cmp(&right.0);

            if ord == Ordering::Equal {
                right.1.cmp(&left.1)
            } else {
                ord
            }
        }
    }

    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, Tup2<i32, String>>();

    let lag_handle = input_stream
        .lag_custom_order::<_, _, _, AscDesc, _>(
            3,
            |v| v.cloned(),
            |v, vl| Tup3(v.0, v.1.clone(), vl.clone()),
        )
        .accumulate_integrate()
        .accumulate_output();

    Ok((input_handle, lag_handle))
}

fn lead_test(trace: Vec<Vec<(i32, i32, ZWeight)>>, transaction: bool) {
    let (mut dbsp, (input_handle, lead_handle)) = Runtime::init_circuit(
        CircuitConfig::from(4).with_splitter_chunk_size_records(2),
        lead_test_circuit,
    )
    .unwrap();

    let mut ref_trace = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    if transaction {
        dbsp.start_transaction().unwrap();
        for batch in trace.into_iter() {
            let records = batch
                .iter()
                .map(|(k, v, r)| ((*k, *v, ()), *r))
                .collect::<Vec<_>>();

            let ref_batch = TestBatch::from_typed_data(&records);
            TOKIO.block_on(ref_trace.insert(ref_batch));

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.step().unwrap();
        }

        dbsp.commit_transaction().unwrap();

        let lead_result = lead_handle.concat().consolidate();
        let ref_lead = ref_trace.lead::<i32, i32>(3);

        assert_batch_eq(lead_result.inner(), &ref_lead);
    } else {
        for batch in trace.into_iter() {
            let records = batch
                .iter()
                .map(|(k, v, r)| ((*k, *v, ()), *r))
                .collect::<Vec<_>>();

            let ref_batch = TestBatch::from_typed_data(&records);
            TOKIO.block_on(ref_trace.insert(ref_batch));

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.transaction().unwrap();

            let lead_result = lead_handle.concat().consolidate();
            let ref_lead = ref_trace.lead::<i32, i32>(3);

            assert_batch_eq(lead_result.inner(), &ref_lead);
        }
    }
}

fn lag_test(trace: Vec<Vec<(i32, i32, ZWeight)>>, transaction: bool) {
    let (mut dbsp, (input_handle, lag_handle)) = Runtime::init_circuit(
        CircuitConfig::from(4).with_splitter_chunk_size_records(2),
        lag_test_circuit,
    )
    .unwrap();

    let mut ref_trace = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    if transaction {
        dbsp.start_transaction().unwrap();

        for batch in trace.into_iter() {
            let records = batch
                .iter()
                .map(|(k, v, r)| ((*k, *v, ()), *r))
                .collect::<Vec<_>>();

            let ref_batch = TestBatch::from_typed_data(&records);
            TOKIO.block_on(ref_trace.insert(ref_batch));

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.step().unwrap();
        }

        dbsp.commit_transaction().unwrap();

        let lag_result = lag_handle.concat().consolidate();
        let ref_lag = ref_trace.lag::<i32, i32>(3);

        assert_batch_eq(lag_result.inner(), &ref_lag);
    } else {
        for batch in trace.into_iter() {
            let records = batch
                .iter()
                .map(|(k, v, r)| ((*k, *v, ()), *r))
                .collect::<Vec<_>>();

            let ref_batch = TestBatch::from_typed_data(&records);
            TOKIO.block_on(ref_trace.insert(ref_batch));

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.transaction().unwrap();

            let lag_result = SpineSnapshot::<TypedBatch<i32, Tup2<i32, Option<i32>>, _, _>>::concat(
                &lag_handle.take_from_all(),
            );
            let ref_lag = ref_trace.lag::<i32, i32>(3);

            assert_batch_eq(lag_result.inner(), &ref_lag);
        }
    }
}

#[test]
fn test_lead_regressions() {
    let trace = vec![vec![(0, 0, 1), (0, 73, -1), (0, 1, 1)], vec![(0, 0, 1)]];

    lead_test(trace, false);
}

#[test]
fn test_lag_regressions() {
    let traces = vec![
        vec![vec![(2, 64, -1), (2, 0, 1)]],
        vec![vec![(0, 0, -1)]],
        vec![vec![(4, 0, 4), (4, 4, 1)]],
        vec![vec![(4, 0, 4), (4, 69, 2)]],
        vec![
            vec![(0, 87, 1)],
            vec![(0, 84, 1), (0, 87, 1), (0, 88, 1)],
            vec![(0, 0, -1)],
        ],
    ];

    for trace in traces {
        lag_test(trace, false);
    }
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
    ) = Runtime::init_circuit(4, |circuit| topk_custom_ord_test_circuit(circuit, 5)).unwrap();

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
        // A row of weight 2 leads the second peer group. Four rows precede
        // the group, so it ranks 5 and stays in the output.
        vec![
            (1, Tup3("foo".to_string(), 7, 96), 2),
            (1, Tup3("bar".to_string(), 8, 96), 1),
        ],
    ];
    let mut expected_output = vec![indexed_zset! {
        1 => {Tup3("foo".to_string(), 8, 98) => 1, Tup3("foo".to_string(), 8, 97) => 1, Tup3("foo".to_string(), 9, 99) => 1, Tup3("foo".to_string(), 9, 98) => 1, Tup3("foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup3("foo".to_string(), 8, 98) => 1, Tup3("foo".to_string(), 8, 97) => 1, Tup3("foo".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 95) => 1, Tup3("foo".to_string(), 9, 99) => 1},
    },
    indexed_zset! {
        1 => {Tup3("foo".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 95) => 1, Tup3("foo".to_string(), 9, 97) => 1, Tup3("foo".to_string(), 9, 96) => 1, Tup3("foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup3("bar".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 95) => 1, Tup3("foo".to_string(), 9, 97) => 1, Tup3("foo".to_string(), 9, 96) => 1}
    },
    indexed_zset! {
        1 => {Tup3("foo".to_string(), 7, 96) => 1, Tup3("bar".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 95) => 1, Tup3("foo".to_string(), 9, 97) => 1},
    },
    indexed_zset! {
        1 => {Tup3("foo".to_string(), 7, 96) => 1, Tup3("bar".to_string(), 8, 96) => 1, Tup3("baz".to_string(), 8, 96) => 1, Tup3("buzz".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup3("foo".to_string(), 7, 96) => 2, Tup3("bar".to_string(), 8, 96) => 1, Tup3("baz".to_string(), 8, 96) => 2, Tup3("buzz".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup3("foo".to_string(), 7, 96) => 4, Tup3("bar".to_string(), 8, 96) => 2, Tup3("baz".to_string(), 8, 96) => 2, Tup3("buzz".to_string(), 8, 96) => 1, Tup3("foo".to_string(), 8, 96) => 1},
    }]
    .into_iter();

    let mut expected_ranked_output = vec![indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 98) => 1, Tup4(2, "foo".to_string(), 8, 97) => 1, Tup4(3, "foo".to_string(), 9, 99) => 1, Tup4(4, "foo".to_string(), 9, 98) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 98) => 1, Tup4(2, "foo".to_string(), 8, 97) => 1, Tup4(3, "foo".to_string(), 8, 96) => 1, Tup4(4, "foo".to_string(), 8, 95) => 1, Tup4(5, "foo".to_string(), 9, 99) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 95) => 1, Tup4(3, "foo".to_string(), 9, 97) => 1, Tup4(4, "foo".to_string(), 9, 96) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "bar".to_string(), 8, 96) => 1, Tup4(1, "foo".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 95) => 1, Tup4(4, "foo".to_string(), 9, 97) => 1, Tup4(5, "foo".to_string(), 9, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(4, "foo".to_string(), 8, 95) => 1, Tup4(5, "foo".to_string(), 9, 97) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(2, "baz".to_string(), 8, 96) => 1, Tup4(2, "buzz".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(2, "foobar".to_string(), 8, 96) => 1, Tup4(2, "fubar".to_string(), 8, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 2, Tup4(3, "bar".to_string(), 8, 96) => 1, Tup4(3, "baz".to_string(), 8, 96) => 2, Tup4(3, "buzz".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 96) => 1, Tup4(3, "foobar".to_string(), 8, 96) => 1, Tup4(3, "fubar".to_string(), 8, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 4, Tup4(5, "bar".to_string(), 8, 96) => 2, Tup4(5, "baz".to_string(), 8, 96) => 2, Tup4(5, "buzz".to_string(), 8, 96) => 1, Tup4(5, "foo".to_string(), 8, 96) => 1, Tup4(5, "foobar".to_string(), 8, 96) => 1, Tup4(5, "fubar".to_string(), 8, 96) => 1},
    }]
    .into_iter();

    let mut expected_dense_ranked_output = vec![indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 98) => 1, Tup4(2, "foo".to_string(), 8, 97) => 1, Tup4(3, "foo".to_string(), 9, 99) => 1, Tup4(4, "foo".to_string(), 9, 98) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 98) => 1, Tup4(2, "foo".to_string(), 8, 97) => 1, Tup4(3, "foo".to_string(), 8, 96) => 1, Tup4(4, "foo".to_string(), 8, 95) => 1, Tup4(5, "foo".to_string(), 9, 99) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 95) => 1, Tup4(3, "foo".to_string(), 9, 97) => 1, Tup4(4, "foo".to_string(), 9, 96) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "bar".to_string(), 8, 96) => 1, Tup4(1, "foo".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 95) => 1, Tup4(3, "foo".to_string(), 9, 97) => 1, Tup4(4, "foo".to_string(), 9, 96) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 95) => 1, Tup4(4, "foo".to_string(), 9, 97) => 1, Tup4(5, "foo".to_string(), 9, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(2, "baz".to_string(), 8, 96) => 1, Tup4(2, "buzz".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(2, "foobar".to_string(), 8, 96) => 1, Tup4(2, "fubar".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 95) => 1, Tup4(4, "foo".to_string(), 9, 97) => 1, Tup4(5, "foo".to_string(), 9, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 2, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(2, "baz".to_string(), 8, 96) => 2, Tup4(2, "buzz".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(2, "foobar".to_string(), 8, 96) => 1, Tup4(2, "fubar".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 95) => 1, Tup4(4, "foo".to_string(), 9, 97) => 1, Tup4(5, "foo".to_string(), 9, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 4, Tup4(2, "bar".to_string(), 8, 96) => 2, Tup4(2, "baz".to_string(), 8, 96) => 2, Tup4(2, "buzz".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(2, "foobar".to_string(), 8, 96) => 1, Tup4(2, "fubar".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 95) => 1, Tup4(4, "foo".to_string(), 9, 97) => 1, Tup4(5, "foo".to_string(), 9, 96) => 1},
    }]
    .into_iter();

    let mut expected_row_number_output = vec![indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 98) => 1, Tup4(2, "foo".to_string(), 8, 97) => 1, Tup4(3, "foo".to_string(), 9, 99) => 1, Tup4(4, "foo".to_string(), 9, 98) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 98) => 1, Tup4(2, "foo".to_string(), 8, 97) => 1, Tup4(3, "foo".to_string(), 8, 96) => 1, Tup4(4, "foo".to_string(), 8, 95) => 1, Tup4(5, "foo".to_string(), 9, 99) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 95) => 1, Tup4(3, "foo".to_string(), 9, 97) => 1, Tup4(4, "foo".to_string(), 9, 96) => 1, Tup4(5, "foo".to_string(), 10, 100) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "bar".to_string(), 8, 96) => 1, Tup4(2, "foo".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 95) => 1, Tup4(4, "foo".to_string(), 9, 97) => 1, Tup4(5, "foo".to_string(), 9, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(3, "foo".to_string(), 8, 96) => 1, Tup4(4, "foo".to_string(), 8, 95) => 1, Tup4(5, "foo".to_string(), 9, 97) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "bar".to_string(), 8, 96) => 1, Tup4(3, "baz".to_string(), 8, 96) => 1, Tup4(4, "buzz".to_string(), 8, 96) => 1, Tup4(5, "foo".to_string(), 8, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "foo".to_string(), 7, 96) => 1, Tup4(3, "bar".to_string(), 8, 96) => 1, Tup4(4, "baz".to_string(), 8, 96) => 1, Tup4(5, "baz".to_string(), 8, 96) => 1},
    },
    indexed_zset! {
        1 => {Tup4(1, "foo".to_string(), 7, 96) => 1, Tup4(2, "foo".to_string(), 7, 96) => 1, Tup4(3, "foo".to_string(), 7, 96) => 1, Tup4(4, "foo".to_string(), 7, 96) => 1, Tup4(5, "bar".to_string(), 8, 96) => 1},
    }]
    .into_iter();

    for batch in trace.into_iter() {
        for (k, v, r) in batch.into_iter() {
            input_handle.push(k, (v, r));
        }
        dbsp.transaction().unwrap();

        let topk_result = topk_handle.consolidate();

        assert_typed_batch_eq(&topk_result, &expected_output.next().unwrap());

        let topk_rank_result = topk_rank_handle.consolidate();

        assert_typed_batch_eq(&topk_rank_result, &expected_ranked_output.next().unwrap());

        let topk_dense_rank_result = topk_dense_rank_handle.consolidate();

        assert_typed_batch_eq(
            &topk_dense_rank_result,
            &expected_dense_ranked_output.next().unwrap(),
        );

        let topk_row_number_result = topk_row_number_handle.consolidate();

        assert_typed_batch_eq(
            &topk_row_number_result,
            &expected_row_number_output.next().unwrap(),
        );
    }
}

#[test]
fn test_topk_custom_ord_zero() {
    let (
        mut dbsp,
        (
            input_handle,
            topk_handle,
            topk_rank_handle,
            topk_dense_rank_handle,
            topk_row_number_handle,
        ),
    ) = Runtime::init_circuit(4, |circuit| topk_custom_ord_test_circuit(circuit, 0)).unwrap();

    input_handle.push(1, (Tup3("foo".to_string(), 10, 100), 1));
    input_handle.push(1, (Tup3("foo".to_string(), 9, 99), 2));
    dbsp.transaction().unwrap();

    // Ranks and row numbers start at 1, so no row qualifies.
    assert!(topk_handle.consolidate().is_empty());
    assert!(topk_rank_handle.consolidate().is_empty());
    assert!(topk_dense_rank_handle.consolidate().is_empty());
    assert!(topk_row_number_handle.consolidate().is_empty());
}

#[test]
fn test_lag_custom_ord_small_step() {
    test_lag_custom_ord(false)
}

#[test]
fn test_lag_custom_ord_big_step() {
    test_lag_custom_ord(true)
}

fn test_lag_custom_ord(transaction: bool) {
    let (mut dbsp, (input_handle, lag_handle)) = Runtime::init_circuit(
        CircuitConfig::from(4).with_splitter_chunk_size_records(2),
        lag_custom_order_test_circuit,
    )
    .unwrap();

    let trace = [
        vec![
            (1, Tup2(1, "f".to_string()), 1),
            (1, Tup2(1, "e".to_string()), 1),
            (1, Tup2(1, "d".to_string()), 1),
            (1, Tup2(1, "c".to_string()), 1),
            (1, Tup2(1, "b".to_string()), 1),
            (1, Tup2(1, "a".to_string()), 1),
            (1, Tup2(2, "d".to_string()), 1),
            (1, Tup2(2, "c".to_string()), 1),
            (1, Tup2(2, "b".to_string()), 1),
            (1, Tup2(2, "a".to_string()), 1),
        ],
        vec![
            (1, Tup2(2, "e".to_string()), 1),
            (1, Tup2(2, "d".to_string()), 1),
            (1, Tup2(3, "d".to_string()), 1),
            (1, Tup2(3, "c".to_string()), 1),
            (1, Tup2(3, "b".to_string()), 1),
            (1, Tup2(3, "a".to_string()), 1),
        ],
        vec![
            (1, Tup2(1, "f".to_string()), -1),
            (1, Tup2(1, "d".to_string()), -1),
            (1, Tup2(1, "b".to_string()), -1),
            (1, Tup2(2, "d".to_string()), -1),
            (1, Tup2(2, "b".to_string()), -1),
        ],
    ];
    let expected_output: Vec<OrdIndexedZSet<i32, Tup3<i32, String, Option<Tup2<i32, String>>>>> = vec![
        indexed_zset! {
            1i32 => { Tup3(1i32, "f".to_string(), None) => 1
                    , Tup3(1i32, "e".to_string(), None) => 1
                    , Tup3(1i32, "d".to_string(), None) => 1
                    , Tup3(1i32, "c".to_string(), Some(Tup2(1, "f".to_string()))) => 1
                    , Tup3(1i32, "b".to_string(), Some(Tup2(1, "e".to_string()))) => 1
                    , Tup3(1i32, "a".to_string(), Some(Tup2(1, "d".to_string()))) => 1
                    , Tup3(2i32, "d".to_string(), Some(Tup2(1, "c".to_string()))) => 1
                    , Tup3(2i32, "c".to_string(), Some(Tup2(1, "b".to_string()))) => 1
                    , Tup3(2i32, "b".to_string(), Some(Tup2(1, "a".to_string()))) => 1
                    , Tup3(2i32, "a".to_string(), Some(Tup2(2, "d".to_string()))) => 1
            }
        },
        indexed_zset! {
            1i32 => { Tup3(1i32, "f".to_string(), None) => 1
                , Tup3(1i32, "e".to_string(), None) => 1
                , Tup3(1i32, "d".to_string(), None) => 1
                , Tup3(1i32, "c".to_string(), Some(Tup2(1, "f".to_string()))) => 1
                , Tup3(1i32, "b".to_string(), Some(Tup2(1, "e".to_string()))) => 1
                , Tup3(1i32, "a".to_string(), Some(Tup2(1, "d".to_string()))) => 1
                , Tup3(2i32, "e".to_string(), Some(Tup2(1, "c".to_string()))) => 1
                , Tup3(2i32, "d".to_string(), Some(Tup2(1, "b".to_string()))) => 1
                , Tup3(2i32, "d".to_string(), Some(Tup2(1, "a".to_string()))) => 1
                , Tup3(2i32, "c".to_string(), Some(Tup2(2, "e".to_string()))) => 1
                , Tup3(2i32, "b".to_string(), Some(Tup2(2, "d".to_string()))) => 1
                , Tup3(2i32, "a".to_string(), Some(Tup2(2, "d".to_string()))) => 1
                , Tup3(3i32, "d".to_string(), Some(Tup2(2, "c".to_string()))) => 1
                , Tup3(3i32, "c".to_string(), Some(Tup2(2, "b".to_string()))) => 1
                , Tup3(3i32, "b".to_string(), Some(Tup2(2, "a".to_string()))) => 1
                , Tup3(3i32, "a".to_string(), Some(Tup2(3, "d".to_string()))) => 1
            }
        },
        indexed_zset! {
            1i32 => { Tup3(1i32, "e".to_string(), None) => 1
                , Tup3(1i32, "c".to_string(), None) => 1
                , Tup3(1i32, "a".to_string(), None) => 1
                , Tup3(2i32, "e".to_string(), Some(Tup2(1, "e".to_string()))) => 1
                , Tup3(2i32, "d".to_string(), Some(Tup2(1, "c".to_string()))) => 1
                , Tup3(2i32, "c".to_string(), Some(Tup2(1, "a".to_string()))) => 1
                , Tup3(2i32, "a".to_string(), Some(Tup2(2, "e".to_string()))) => 1
                , Tup3(3i32, "d".to_string(), Some(Tup2(2, "d".to_string()))) => 1
                , Tup3(3i32, "c".to_string(), Some(Tup2(2, "c".to_string()))) => 1
                , Tup3(3i32, "b".to_string(), Some(Tup2(2, "a".to_string()))) => 1
                , Tup3(3i32, "a".to_string(), Some(Tup2(3, "d".to_string()))) => 1
            }
        },
    ];

    if transaction {
        dbsp.start_transaction().unwrap();

        for i in trace {
            println!("step");
            let batch = i.clone();
            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.step().unwrap();
        }

        dbsp.commit_transaction().unwrap();

        let topk_result = lag_handle.concat().consolidate();

        assert_typed_batch_eq(&topk_result, &expected_output.last().unwrap().clone());
    } else {
        for i in 0..trace.len() {
            println!("step");
            let batch = trace[i].clone();
            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.transaction().unwrap();

            let topk_result = lag_handle.concat().consolidate();

            assert_typed_batch_eq(&topk_result, &expected_output[i]);
        }
    }
}

fn test_topk(trace: Vec<Vec<(i32, i32, ZWeight)>>, transaction: bool) {
    let (mut dbsp, (input_handle, topk_asc_handle, topk_desc_handle)) = Runtime::init_circuit(
        CircuitConfig::from(4).with_splitter_chunk_size_records(2),
        topk_test_circuit,
    )
    .unwrap();

    let mut ref_trace = TestBatch::new(
        &TestBatchFactories::new(),
        Arc::new(String::from("Test")),
        TraceRole::Integral,
    );

    if transaction {
        dbsp.start_transaction().unwrap();

        for batch in trace.into_iter() {
            let records = batch
                .iter()
                .map(|(k, v, r)| ((*k, *v, ()), *r))
                .collect::<Vec<_>>();

            let ref_batch = TestBatch::from_typed_data(&records);
            TOKIO.block_on(ref_trace.insert(ref_batch));

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.step().unwrap();
        }

        dbsp.commit_transaction().unwrap();

        let topk_asc_result = topk_asc_handle.concat().consolidate();
        let topk_desc_result = topk_desc_handle.concat().consolidate();

        let ref_topk_asc = ref_trace.topk_asc::<i32, i32>(5);
        let ref_topk_desc = ref_trace.topk_desc::<i32, i32>(5);

        assert_batch_eq(topk_asc_result.inner(), &ref_topk_asc);
        assert_batch_eq(topk_desc_result.inner(), &ref_topk_desc);
    } else {
        for batch in trace.into_iter() {
            let records = batch
                .iter()
                .map(|(k, v, r)| ((*k, *v, ()), *r))
                .collect::<Vec<_>>();

            let ref_batch = TestBatch::from_typed_data(&records);
            TOKIO.block_on(ref_trace.insert(ref_batch));

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.transaction().unwrap();

            let topk_asc_result = topk_asc_handle.concat().consolidate();
            let topk_desc_result = topk_desc_handle.concat().consolidate();

            let ref_topk_asc = ref_trace.topk_asc::<i32, i32>(5);
            let ref_topk_desc = ref_trace.topk_desc::<i32, i32>(5);

            assert_batch_eq(topk_asc_result.inner(), &ref_topk_asc);
            assert_batch_eq(topk_desc_result.inner(), &ref_topk_desc);
        }
    }
}

/// Strategy for batches of updates to the input of `topk_rank_test_circuit`.
///
/// Each update is `(key, Tup2(ranked, tie_break), weight)`, and a negative
/// weight deletes copies of a row. The ranges are small, so rows often tie on
/// the ranked field and often repeat whole, which gives them weights above 1.
fn topk_rank_trace() -> impl Strategy<Value = Vec<Vec<(u64, Tup2<u64, u64>, ZWeight)>>> {
    vec(
        vec(
            (0..3u64, 0..4u64, 0..3u64, -2..=3i64).prop_map(|(key, ranked, tie_break, weight)| {
                (key, Tup2(ranked, tie_break), weight)
            }),
            0..10,
        ),
        1..20,
    )
}

/// Builds a circuit that computes top-k RANK, DENSE_RANK, and ROW_NUMBER of
/// the same input.
///
/// Rows are ranked by `Tup2::0`, and `Tup2::1` orders the rows of a peer group.
/// Each output row is `Tup3(ranked, tie_break, rank)`.
///
/// # Arguments
///
/// * `circuit` - The circuit to build.
/// * `k` - The number of ranks to keep (row numbers for ROW_NUMBER).
///
/// # Returns
///
/// The input handle and the integrated outputs of RANK, DENSE_RANK, and
/// ROW_NUMBER, in that order.
fn topk_rank_test_circuit(
    circuit: &mut RootCircuit,
    k: usize,
) -> AnyResult<(
    IndexedZSetHandle<u64, Tup2<u64, u64>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, i64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, i64>>>>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, i64>>>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, Tup2<u64, u64>>();

    let rank_handle = input_stream
        .topk_rank_custom_order::<OrderByFirstTupleField, _, _, _>(
            k,
            |left, right| left.0 == right.0,
            |rank, value| Tup3(value.0, value.1, rank),
        )
        .accumulate_integrate()
        .accumulate_output();
    let dense_rank_handle = input_stream
        .topk_dense_rank_custom_order::<OrderByFirstTupleField, _, _, _>(
            k,
            |left, right| left.0 == right.0,
            |rank, value| Tup3(value.0, value.1, rank),
        )
        .accumulate_integrate()
        .accumulate_output();
    let row_number_handle = input_stream
        .topk_row_number_custom_order::<OrderByFirstTupleField, _, _>(k, |row_number, value| {
            Tup3(value.0, value.1, row_number)
        })
        .accumulate_integrate()
        .accumulate_output();

    Ok((
        input_handle,
        rank_handle,
        dense_rank_handle,
        row_number_handle,
    ))
}

/// Pushes `batch` to `input_handle` and applies it to `multiset`.
///
/// An update never deletes more copies of a row than `multiset` holds, so the
/// input stays a valid table, with no negative weights.
///
/// # Arguments
///
/// * `input_handle` - The input of the circuit.
/// * `multiset` - The contents of the input, which the function updates.
/// * `batch` - The updates to apply.
fn push_clamped(
    input_handle: &IndexedZSetHandle<u64, Tup2<u64, u64>>,
    multiset: &mut HashMap<(u64, Tup2<u64, u64>), ZWeight>,
    batch: Vec<(u64, Tup2<u64, u64>, ZWeight)>,
) {
    for (key, value, weight) in batch {
        let current = multiset.entry((key, value)).or_insert(0);
        let weight = weight.max(-*current);
        if weight != 0 {
            *current += weight;
            input_handle.push(key, (value, weight));
        }
    }
}

/// Returns the rows of `ranked` whose rank, the last field, is at most `k`.
fn rank_at_most(
    ranked: OrdIndexedZSet<u64, Tup3<u64, u64, i64>>,
    k: usize,
) -> OrdIndexedZSet<u64, Tup3<u64, u64, i64>> {
    let tuples = ranked
        .iter()
        .filter(|(_, Tup3(_, _, rank), _)| *rank <= k as i64)
        .map(|(key, value, weight)| Tup2(Tup2(key, value), weight))
        .collect();
    OrdIndexedZSet::from_tuples((), tuples)
}

/// Checks top-k RANK, DENSE_RANK, and ROW_NUMBER on `trace` against the
/// reference models in `rank::test`, cut at rank `k`.
///
/// # Arguments
///
/// * `k` - The number of ranks to keep (row numbers for ROW_NUMBER).
/// * `trace` - Batches of updates to the input.
/// * `transaction` - Whether to run all batches in one transaction and check
///   the outputs at commit, instead of one transaction per batch with a check
///   after each.
fn test_topk_rank(k: usize, trace: Vec<Vec<(u64, Tup2<u64, u64>, ZWeight)>>, transaction: bool) {
    let (mut dbsp, (input_handle, rank_handle, dense_rank_handle, row_number_handle)) =
        Runtime::init_circuit(
            CircuitConfig::from(4).with_splitter_chunk_size_records(2),
            move |circuit| topk_rank_test_circuit(circuit, k),
        )
        .unwrap();

    let check = |multiset: &HashMap<(u64, Tup2<u64, u64>), ZWeight>| {
        let input = multiset_to_indexed_zset(multiset);

        let expected_rank = rank_at_most(
            reference_rank_custom_order::<_, _, OrderByFirstTupleField, _, _, _>(
                &input,
                |left, right| left.0.cmp(&right.0),
                |rank, value| Tup3(value.0, value.1, rank),
            ),
            k,
        );
        assert_typed_batch_eq(&rank_handle.concat().consolidate(), &expected_rank);

        let expected_dense_rank = rank_at_most(
            reference_dense_rank_custom_order::<_, _, OrderByFirstTupleField, _, _, _>(
                &input,
                |left, right| left.0.cmp(&right.0),
                |rank, value| Tup3(value.0, value.1, rank),
            ),
            k,
        );
        assert_typed_batch_eq(
            &dense_rank_handle.concat().consolidate(),
            &expected_dense_rank,
        );

        let expected_row_number = rank_at_most(
            reference_row_number_custom_order::<_, _, OrderByFirstTupleField, _, _>(
                &input,
                |row_number, value| Tup3(value.0, value.1, row_number),
            ),
            k,
        );
        assert_typed_batch_eq(
            &row_number_handle.concat().consolidate(),
            &expected_row_number,
        );
    };

    let mut multiset = HashMap::new();
    if transaction {
        dbsp.start_transaction().unwrap();
        for batch in trace {
            push_clamped(&input_handle, &mut multiset, batch);
            dbsp.step().unwrap();
        }
        dbsp.commit_transaction().unwrap();
        check(&multiset);
    } else {
        for batch in trace {
            push_clamped(&input_handle, &mut multiset, batch);
            dbsp.transaction().unwrap();
            check(&multiset);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    #[test]
    fn test_topk_small_step(trace in input_trace(5, 1_000, 200, 20)) {
        test_topk(trace, false)
    }

    #[test]
    fn test_topk_big_step(trace in input_trace(5, 1_000, 200, 20)) {
        test_topk(trace, true)
    }

    #[test]
    fn test_topk_rank_small_step(k in 0..=6usize, trace in topk_rank_trace()) {
        test_topk_rank(k, trace, false)
    }

    #[test]
    fn test_topk_rank_big_step(k in 0..=6usize, trace in topk_rank_trace()) {
        test_topk_rank(k, trace, true)
    }

    #[test]
    fn test_lag_small_step(trace in input_trace(5, 100, 200, 20)) {
        lag_test(trace, false)
    }

    #[test]
    fn test_lag_big_step(trace in input_trace(5, 100, 200, 20)) {
        lag_test(trace, true)
    }

    #[test]
    fn test_lead_small_step(trace in input_trace(5, 100, 200, 20)) {
        lead_test(trace, false)
    }

    #[test]
    fn test_lead_big_step(trace in input_trace(5, 100, 200, 20)) {
        lead_test(trace, true)
    }

}

/// A row of the tests of `lag_custom_order_with_waterline`: `(ts, payload)`.
type WaterlineRow = Tup2<u64, u8>;

/// An output record of the tests of `lag_custom_order_with_waterline`: a row
/// and the row at the offset from it.
type WaterlineRecord = Tup2<WaterlineRow, Option<WaterlineRow>>;

/// Orders rows by ascending `ts`, then by descending payload, so that the
/// rows below the waterline come first in each group and the order differs
/// from the natural order of the rows.
struct TsAscPayloadDesc;

impl CmpFunc<WaterlineRow> for TsAscPayloadDesc {
    fn cmp(left: &WaterlineRow, right: &WaterlineRow) -> Ordering {
        left.0.cmp(&right.0).then_with(|| right.1.cmp(&left.1))
    }
}

/// A change to the input of a test of `lag_custom_order_with_waterline`.
#[derive(Clone, Debug)]
struct WaterlineChange {
    key: i32,
    row: WaterlineRow,
    /// 1 inserts a copy of `row`, -1 deletes one.
    weight: ZWeight,
}

impl WaterlineChange {
    fn insert(key: i32, ts: u64, payload: u8) -> Self {
        Self {
            key,
            row: Tup2(ts, payload),
            weight: 1,
        }
    }

    fn delete(key: i32, ts: u64, payload: u8) -> Self {
        Self {
            key,
            row: Tup2(ts, payload),
            weight: -1,
        }
    }
}

/// A transaction of a test of `lag_custom_order_with_waterline`.
#[derive(Clone, Debug)]
struct WaterlineTransaction {
    /// The waterline during the transaction.  Every row that the
    /// transaction changes is at or above it.
    waterline: u64,
    /// The input changes of each step of the transaction.
    steps: Vec<Vec<WaterlineChange>>,
}

impl WaterlineTransaction {
    /// Returns a transaction of one step.
    fn new(waterline: u64, changes: Vec<WaterlineChange>) -> Self {
        Self {
            waterline,
            steps: vec![changes],
        }
    }
}

/// Adds `weight` to the weight of `item` in `multiset`, and removes the item
/// when its weight becomes zero.
fn add_weight<T: Ord>(multiset: &mut BTreeMap<T, ZWeight>, item: T, weight: ZWeight) {
    match multiset.entry(item) {
        Entry::Vacant(entry) => {
            if weight != 0 {
                entry.insert(weight);
            }
        }
        Entry::Occupied(mut entry) => {
            *entry.get_mut() += weight;
            if *entry.get() == 0 {
                entry.remove();
            }
        }
    }
}

/// Builds a circuit with a LAG operator over the order [`TsAscPayloadDesc`]
/// that garbage-collects its state below a waterline set by the test.
///
/// # Arguments
///
/// * `circuit` - the circuit to build.
/// * `offset` - the offset of the operator.
///
/// # Returns
///
/// The handles of the input, of the waterline, and of the output of the
/// operator.
fn lag_with_waterline_circuit(
    circuit: &mut RootCircuit,
    offset: isize,
) -> AnyResult<(
    IndexedZSetHandle<i32, WaterlineRow>,
    InputHandle<u64>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<i32, WaterlineRecord>>>,
)> {
    let (input, input_handle) = circuit.add_input_indexed_zset::<i32, WaterlineRow>();
    let (waterline, waterline_handle) = circuit.add_input_stream::<u64>();

    // The input stream yields 0 at each step when the test does not set it.
    let mut largest = 0;
    let waterline = waterline
        .apply_mut(move |waterline| {
            largest = largest.max(*waterline);
            largest
        })
        .typed_box::<DynData>();

    let output_handle = input
        .lag_custom_order_with_waterline::<_, _, _, TsAscPayloadDesc, _, _, _>(
            &waterline,
            |row: &WaterlineRow, waterline: &u64| row.0 >= *waterline,
            offset,
            |row| row.cloned(),
            |row, lagged| Tup2(*row, *lagged),
        )
        .accumulate_output();

    Ok((input_handle, waterline_handle, output_handle))
}

/// Computes the output of a LAG operator over the order [`TsAscPayloadDesc`].
///
/// # Arguments
///
/// * `input` - the weight of each `(key, row)` pair of the input.
/// * `offset` - the offset of the operator.
///
/// # Returns
///
/// The weight of each `(key, record)` pair of the output.
fn reference_lag_with_waterline(
    input: &BTreeMap<(i32, WaterlineRow), ZWeight>,
    offset: isize,
) -> BTreeMap<(i32, WaterlineRecord), ZWeight> {
    let mut groups: BTreeMap<i32, Vec<WaterlineRow>> = BTreeMap::new();
    for ((key, row), weight) in input {
        let group = groups.entry(*key).or_default();
        for _ in 0..*weight {
            group.push(*row);
        }
    }

    let mut output = BTreeMap::new();
    for (key, mut rows) in groups {
        rows.sort_by(TsAscPayloadDesc::cmp);
        for (index, row) in rows.iter().enumerate() {
            let lagged = (index as isize)
                .checked_sub(offset)
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| rows.get(index))
                .cloned();
            add_weight(&mut output, (key, Tup2(*row, lagged)), 1);
        }
    }
    output
}

/// Counts the records that remain in a group-wise collection after garbage
/// collection.
///
/// # Arguments
///
/// * `records` - the group of each record and whether the record is at or
///   above the waterline.
/// * `below` - the number of records below the waterline that each group
///   keeps.
///
/// # Returns
///
/// The number of records at or above the waterline, plus up to `below`
/// other records of each group.
fn count_retained(records: impl Iterator<Item = (i32, bool)>, below: usize) -> usize {
    let mut groups: BTreeMap<i32, (usize, usize)> = BTreeMap::new();
    for (key, above) in records {
        let (above_count, below_count) = groups.entry(key).or_default();
        if above {
            *above_count += 1;
        } else {
            *below_count += 1;
        }
    }
    groups
        .values()
        .map(|(above_count, below_count)| above_count + below_count.min(&below))
        .sum()
}

/// Counts the records of the rows that hold the last `n` positions below the
/// waterline in each group of the output of a LEAD operator.
///
/// A row with weight `w` holds `w` consecutive positions of its group.  A
/// change at the waterline can retract every record of these rows.
///
/// # Arguments
///
/// * `input` - the input of the operator.
/// * `output` - the output of the operator.
/// * `n` - the number of positions.
/// * `waterline` - the waterline.
///
/// # Returns
///
/// The number of records whose row holds one of the last `n` positions
/// below `waterline`, in the order [`TsAscPayloadDesc`], of its group.
fn count_records_of_last_positions(
    input: &BTreeMap<(i32, WaterlineRow), ZWeight>,
    output: &BTreeMap<(i32, WaterlineRecord), ZWeight>,
    n: usize,
    waterline: u64,
) -> usize {
    let mut groups: BTreeMap<i32, Vec<(WaterlineRow, ZWeight)>> = BTreeMap::new();
    for ((key, row), weight) in input {
        if row.0 < waterline {
            groups.entry(*key).or_default().push((*row, *weight));
        }
    }

    let mut last_rows = BTreeSet::new();
    for (key, mut rows) in groups {
        rows.sort_by(|(left, _), (right, _)| TsAscPayloadDesc::cmp(left, right));
        let mut positions = 0;
        for (row, weight) in rows.into_iter().rev() {
            if positions >= n as ZWeight {
                break;
            }
            last_rows.insert((key, row));
            positions += weight;
        }
    }

    output
        .keys()
        .filter(|(key, record)| last_rows.contains(&(*key, record.0)))
        .count()
}

/// Bounds the number of records that the integrals of a LAG operator with a
/// waterline hold after garbage collection.
///
/// Let `n` be the absolute value of `offset`.  Each group of the input
/// integral holds the rows at or above the waterline and the last `n` rows
/// below it.  The rows below the waterline never change, so the count is
/// exact.
///
/// Each group of the output integral holds the records of the rows at or
/// above the waterline and, for LEAD, at most `2 * n` records below it,
/// among them every record of the rows that hold the last `n` positions
/// below it.  Those records can still change.  When a merge counts a record
/// that the operator retracts later, it drops an older record that it would
/// keep now, so a group may hold fewer than `2 * n` records below the
/// waterline.
///
/// # Arguments
///
/// * `input` - the input of the operator.
/// * `output` - the output of the operator.
/// * `offset` - the offset of the operator.
/// * `waterline` - the waterline.
///
/// # Returns
///
/// The least and the greatest number of records in the input and the output
/// integrals.
fn retained_lag_state(
    input: &BTreeMap<(i32, WaterlineRow), ZWeight>,
    output: &BTreeMap<(i32, WaterlineRecord), ZWeight>,
    offset: isize,
    waterline: u64,
) -> RangeInclusive<usize> {
    let n = offset.unsigned_abs();
    let input_records = count_retained(
        input.keys().map(|(key, row)| (*key, row.0 >= waterline)),
        if offset == 0 { 0 } else { n },
    );
    let output_records = |below| {
        count_retained(
            output
                .keys()
                .map(|(key, record)| (*key, record.0.0 >= waterline)),
            below,
        )
    };

    let output_above = output_records(0);
    if offset < 0 {
        let least = input_records
            + output_above
            + count_records_of_last_positions(input, output, n, waterline);
        let most = input_records + output_records(2 * n);
        least..=most
    } else {
        let exact = input_records + output_above;
        exact..=exact
    }
}

/// Compacts every trace of `dbsp`, which applies garbage collection to every
/// trace with more than one batch.
///
/// # Returns
///
/// The number of records in the traces of the circuit.
fn compact_traces(dbsp: &mut DBSPHandle) -> usize {
    dbsp.start_compaction().unwrap();
    dbsp.wait_for_compaction(Duration::from_secs(60)).unwrap();

    // The root node sums the records of the operators, so it is left out.
    let root = GlobalNodeId::root();
    dbsp.retrieve_profile()
        .unwrap()
        .worker_profiles
        .iter()
        .flat_map(|profile| profile.attribute_profile(&STATE_RECORDS_COUNT))
        .filter(|(id, _)| *id != root)
        .map(|(_, records)| match records {
            MetaItem::Count(records) => records,
            records => panic!("unexpected number of records: {records:?}"),
        })
        .sum()
}

/// Runs `plan` through `lag_custom_order_with_waterline` and checks that
/// garbage collection keeps every record that the operator reads.
///
/// After each transaction, the test compares the output with
/// [`reference_lag_with_waterline`], which sees the whole input, and
/// compacts the traces, so that the next transaction reads traces that
/// garbage collection has pruned.  At the end, it holds the waterline for
/// two more transactions: one installs its filter, and the other inserts a
/// row in each group at the waterline, so that compaction merges every
/// batch under that filter.  Then the integrals must hold as many records as
/// [`retained_lag_state`] allows.
///
/// # Arguments
///
/// * `offset` - the offset of the operator.
/// * `workers` - the number of workers of the circuit.
/// * `plan` - the transactions to run, with waterlines that never decrease.
fn lag_with_waterline_test(offset: isize, workers: usize, plan: Vec<WaterlineTransaction>) {
    let (mut dbsp, (input_handle, waterline_handle, output_handle)) = Runtime::init_circuit(
        CircuitConfig::from(workers).with_splitter_chunk_size_records(2),
        move |circuit| lag_with_waterline_circuit(circuit, offset),
    )
    .unwrap();

    let final_waterline = plan.last().map_or(0, |transaction| transaction.waterline);
    let settle = [
        WaterlineTransaction::new(final_waterline, Vec::new()),
        WaterlineTransaction::new(
            final_waterline,
            (0..3)
                .map(|key| WaterlineChange::insert(key, final_waterline, 0))
                .collect(),
        ),
    ];

    let mut input = BTreeMap::new();
    let mut output = BTreeMap::new();
    let mut waterline = 0;
    for transaction in plan.iter().chain(settle.iter()) {
        assert!(
            transaction.waterline >= waterline,
            "the waterline decreases to {}",
            transaction.waterline
        );
        waterline = transaction.waterline;

        dbsp.start_transaction().unwrap();
        for step in &transaction.steps {
            for change in step {
                assert!(
                    change.row.0 >= waterline,
                    "{change:?} is below the waterline {waterline}"
                );
                input_handle.push(change.key, (change.row, change.weight));
                add_weight(&mut input, (change.key, change.row), change.weight);
            }
            waterline_handle.set_for_all(waterline);
            dbsp.step().unwrap();
        }
        dbsp.commit_transaction().unwrap();
        assert!(
            input.values().all(|weight| *weight > 0),
            "the plan deletes a row that the input does not hold"
        );

        for (key, record, weight) in output_handle.concat().consolidate().iter() {
            add_weight(&mut output, (key, record), weight);
        }
        assert_eq!(
            output,
            reference_lag_with_waterline(&input, offset),
            "wrong output after {transaction:?}"
        );

        compact_traces(&mut dbsp);
    }

    let records = compact_traces(&mut dbsp);
    let expected = retained_lag_state(&input, &output, offset, waterline);
    assert!(
        expected.contains(&records),
        "the integrals hold {records} records instead of {expected:?}"
    );
}

/// A change of a generated plan, before [`resolve_waterline_plan`] turns it
/// into a [`WaterlineChange`]: whether it deletes a row, the key, the
/// timestamp above the waterline, the payload, and the row to delete.
type GeneratedChange = (bool, i32, u64, u8, prop::sample::Index);

/// Turns generated transactions into a plan for [`lag_with_waterline_test`].
///
/// Computes the waterline of each transaction from the advances, and
/// replaces each deletion with a deletion of a copy of a row at or above the
/// waterline; a deletion without such a row is dropped.
///
/// # Arguments
///
/// * `transactions` - the advance of the waterline and the changes of each
///   step of each transaction.
///
/// # Returns
///
/// The plan.
fn resolve_waterline_plan(
    transactions: Vec<(u64, Vec<Vec<GeneratedChange>>)>,
) -> Vec<WaterlineTransaction> {
    let mut input = BTreeMap::new();
    let mut waterline = 0;
    transactions
        .into_iter()
        .map(|(advance, steps)| {
            waterline += advance;
            let steps = steps
                .into_iter()
                .map(|changes| {
                    changes
                        .into_iter()
                        .filter_map(|(delete, key, above, payload, index)| {
                            let change = if delete {
                                let live = input
                                    .keys()
                                    .filter(|(_, row): &&(i32, WaterlineRow)| row.0 >= waterline)
                                    .cloned()
                                    .collect::<Vec<_>>();
                                if live.is_empty() {
                                    return None;
                                }
                                let (key, row) = *index.get(&live);
                                WaterlineChange::delete(key, row.0, row.1)
                            } else {
                                WaterlineChange::insert(key, waterline + above, payload)
                            };
                            add_weight(&mut input, (change.key, change.row), change.weight);
                            Some(change)
                        })
                        .collect()
                })
                .collect();
            WaterlineTransaction { waterline, steps }
        })
        .collect()
}

/// Generates plans for [`lag_with_waterline_test`].
///
/// The waterline advances by up to 3 per transaction.  A change inserts a row
/// with a timestamp from the waterline to 4 above it, most often at the
/// waterline, the oldest timestamp it admits, or deletes a copy of a row at
/// or above the waterline.  The payloads take 3 values, so that groups hold
/// copies of a row and rows with the same timestamp.
fn lag_with_waterline_plan() -> impl Strategy<Value = Vec<WaterlineTransaction>> {
    let change = (
        prop::bool::weighted(0.3),
        0..3i32,
        prop_oneof![2 => Just(0u64), 1 => 0..5u64],
        0..3u8,
        any::<prop::sample::Index>(),
    );
    let transaction = (
        prop_oneof![1 => Just(0u64), 2 => 1..4u64],
        vec(vec(change, 0..6), 1..3),
    );
    vec(transaction, 1..12).prop_map(resolve_waterline_plan)
}

/// LEAD(1) over two copies of a row, which have different leads: the output
/// integral must keep both records of the row below the waterline, since
/// the operator retracts both when a row at the waterline changes the lead
/// of the second copy.
#[test]
fn test_lead_with_waterline_copies() {
    lag_with_waterline_test(
        -1,
        1,
        vec![
            WaterlineTransaction::new(
                0,
                vec![
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 1, 0),
                ],
            ),
            // Installs the filter of waterline 2 ...
            WaterlineTransaction::new(2, Vec::new()),
            // ... which the merges of the batches of this transaction apply.
            WaterlineTransaction::new(2, vec![WaterlineChange::insert(0, 5, 0)]),
            WaterlineTransaction::new(2, vec![WaterlineChange::insert(0, 2, 0)]),
        ],
    );
}

/// LEAD(1) where a row at the waterline changes the lead of the last row
/// below it: the input integral must keep that row to compute its new
/// output.
#[test]
fn test_lead_with_waterline_last_row_below() {
    lag_with_waterline_test(
        -1,
        1,
        vec![
            WaterlineTransaction::new(
                0,
                vec![
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 2, 0),
                ],
            ),
            WaterlineTransaction::new(3, Vec::new()),
            WaterlineTransaction::new(3, vec![WaterlineChange::insert(0, 7, 0)]),
            WaterlineTransaction::new(3, vec![WaterlineChange::insert(0, 3, 0)]),
        ],
    );
}

/// LEAD(2) where the two copies of the last row below the waterline come to
/// have the same lead.  Before that, the merge under waterline 5 counts both
/// of their records among the last four, and drops the records of the first
/// three rows; afterwards, the group holds three records below the waterline
/// instead of four.
#[test]
fn test_lead_with_waterline_merged_records() {
    lag_with_waterline_test(
        -2,
        1,
        vec![
            WaterlineTransaction::new(
                0,
                vec![
                    WaterlineChange::insert(0, 0, 1),
                    WaterlineChange::insert(0, 0, 0),
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 2, 0),
                    WaterlineChange::insert(0, 3, 0),
                    WaterlineChange::insert(0, 4, 0),
                    WaterlineChange::insert(0, 4, 0),
                ],
            ),
            WaterlineTransaction::new(5, Vec::new()),
            WaterlineTransaction::new(5, vec![WaterlineChange::insert(0, 5, 0)]),
            WaterlineTransaction::new(5, vec![WaterlineChange::insert(0, 5, 0)]),
        ],
    );
}

/// LEAD(2) where a row at the waterline makes the operator retract the most
/// records below it, `2 * n`: the three records of row 1, whose three copies
/// lead to 1, 2, and 11, and the record of row 2.  The output integral must
/// keep all four.  This is the example in the documentation of
/// `LagRetention`.
#[test]
fn test_lead_with_waterline_most_records() {
    lag_with_waterline_test(
        -2,
        1,
        vec![
            WaterlineTransaction::new(
                0,
                vec![
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 2, 0),
                ],
            ),
            // Installs the filter of waterline 10 ...
            WaterlineTransaction::new(10, Vec::new()),
            // ... which the merges of the batches of this transaction apply.
            WaterlineTransaction::new(
                10,
                vec![
                    WaterlineChange::insert(0, 11, 0),
                    WaterlineChange::insert(0, 12, 0),
                ],
            ),
            WaterlineTransaction::new(10, vec![WaterlineChange::insert(0, 10, 0)]),
        ],
    );
}

/// LAG(2) where a transaction deletes a row at the waterline and inserts
/// another: the outputs of the rows that follow lag the last two rows below
/// the waterline.
#[test]
fn test_lag_with_waterline_boundary() {
    lag_with_waterline_test(
        2,
        1,
        vec![
            WaterlineTransaction::new(
                0,
                vec![
                    WaterlineChange::insert(0, 1, 0),
                    WaterlineChange::insert(0, 2, 0),
                    WaterlineChange::insert(0, 3, 0),
                    WaterlineChange::insert(0, 4, 0),
                ],
            ),
            WaterlineTransaction::new(4, Vec::new()),
            WaterlineTransaction::new(4, vec![WaterlineChange::insert(0, 6, 0)]),
            WaterlineTransaction::new(
                4,
                vec![
                    WaterlineChange::delete(0, 4, 0),
                    WaterlineChange::insert(0, 4, 1),
                ],
            ),
        ],
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    #[test]
    fn test_lag_with_waterline(n in 1..=3isize, plan in lag_with_waterline_plan()) {
        lag_with_waterline_test(n, 1, plan)
    }

    #[test]
    fn test_lead_with_waterline(n in 1..=3isize, plan in lag_with_waterline_plan()) {
        lag_with_waterline_test(-n, 1, plan)
    }

    #[test]
    fn test_lag_with_waterline_workers(offset in -3..=3isize, plan in lag_with_waterline_plan()) {
        lag_with_waterline_test(offset, 4, plan)
    }
}
