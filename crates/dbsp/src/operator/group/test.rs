use crate::{
    algebra::ZRingValue,
    trace::{
        cursor::Cursor,
        test_batch::{assert_batch_eq, TestBatch},
        BatchReader, Trace,
    },
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

    fn lag(&self, lag: usize) -> TestBatch<K, (V, Option<V>), (), R> {
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
                result.push(((cursor.key().clone(), (v, old_v), ()), w.neg()));
            }

            cursor.step_key();
        }

        TestBatch::from_data(&result)
    }

    fn lead(&self, lag: usize) -> TestBatch<K, (V, Option<V>), (), R> {
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
                result.push(((cursor.key().clone(), (v, old_v), ()), w.neg()));
            }

            cursor.step_key();
        }

        TestBatch::from_data(&result)
    }
}

fn topk_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, (i32, i32)>,
    OutputHandle<OrdIndexedZSet<i32, i32, i32>>,
    OutputHandle<OrdIndexedZSet<i32, i32, i32>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

    let topk_asc_handle = input_stream.topk_asc(5).integrate().output();
    let topk_desc_handle = input_stream.topk_desc(5).integrate().output();

    Ok((input_handle, topk_asc_handle, topk_desc_handle))
}

fn lag_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, (i32, i32)>,
    OutputHandle<OrdIndexedZSet<i32, (i32, Option<i32>), i32>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

    let lag_handle = input_stream.lag(3, |v| v.cloned()).integrate().output();

    Ok((input_handle, lag_handle))
}

fn lead_test_circuit(
    circuit: &mut RootCircuit,
) -> AnyResult<(
    CollectionHandle<i32, (i32, i32)>,
    OutputHandle<OrdIndexedZSet<i32, (i32, Option<i32>), i32>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

    let lead_handle = input_stream.lead(3, |v| v.cloned()).integrate().output();

    Ok((input_handle, lead_handle))
}

proptest! {
    #[test]
    fn test_topk(trace in input_trace(5, 1_000, 200, 20)) {
        let (mut dbsp, (input_handle, topk_asc_handle, topk_desc_handle)) = Runtime::init_circuit(4, topk_test_circuit).unwrap();

        let mut ref_trace = TestBatch::new();

        for batch in trace.into_iter() {
            let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

            let ref_batch = TestBatch::from_data(&records);
            ref_trace.insert(ref_batch);

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
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

        let mut ref_trace = TestBatch::new();

        for batch in trace.into_iter() {
            let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

            let ref_batch = TestBatch::from_data(&records);
            ref_trace.insert(ref_batch);

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
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

        let mut ref_trace = TestBatch::new();

        for batch in trace.into_iter() {
            let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

            let ref_batch = TestBatch::from_data(&records);
            ref_trace.insert(ref_batch);

            for (k, v, r) in batch.into_iter() {
                input_handle.push(k, (v, r));
            }
            dbsp.step().unwrap();

            let lead_result = lead_handle.consolidate();
            let ref_lead = ref_trace.lead(3);

            assert_batch_eq(&lead_result, &ref_lead);
        }
    }
}
