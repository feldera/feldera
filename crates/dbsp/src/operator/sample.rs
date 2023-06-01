//! Compute random samples of data.

use crate::{
    algebra::{HasOne, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Scope,
    },
    trace::{Batch, BatchReader, Builder},
    Circuit, DBData, DBWeight, OrdZSet, RootCircuit, Stream,
};
use rand::thread_rng;
use std::{borrow::Cow, cmp::min, marker::PhantomData};

// Prevent gigantic memory allocations by bounding sample size.
pub const MAX_SAMPLE_SIZE: usize = 10_000_000;

// TODO: Operator to randomly sample `(K, V)` pairs.  This is
// a little more tricky and also more expensive to implement than
// sampling keys, as at the low level we need to pick random
// values (`V`) from `OrderedLayer` and map the back to key (`K`)
// by doing a lookup in the offset array.

impl<B> Stream<RootCircuit, B>
where
    B: BatchReader<Time = ()> + Clone,
    B::R: ZRingValue,
{
    /// Generates a uniform random sample of keys from `self`.
    ///
    /// At every clock tick, computes a random sample of the input batch
    /// using [`BatchReader::sample_keys`].  The `sample_size` stream
    /// specifies the size of the sample to compute (use `0` when no sample
    /// is needed at the current clock cycle to make sure the operator
    /// doesn't waste CPU cycles).
    ///
    /// Maximal supported sample size is [`MAX_SAMPLE_SIZE`].  If the operator
    /// receives a larger `sample_size` value, it treats it as
    /// `MAX_SAMPLE_SIZE`.
    ///
    /// Outputs a Z-set containing randomly sampled keys.  Each key is output
    /// with weight `1` regardless of its weight or the number of associated
    /// values in the input batch.
    ///
    /// This is not an incremental operator.  It samples the input
    /// batch received at the current clock cycle and not the integral
    /// of the input stream.  Prefix the call to `sample_keys()` with
    /// `integrate_trace()` to sample the integral of the input.
    ///
    /// WARNING: This operator (by definition) returns non-deterministic
    /// outputs.  As such it may not play well with most other DBSP operators
    /// and must be used with care.
    pub fn stream_sample_keys(
        &self,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, OrdZSet<B::Key, B::R>> {
        self.circuit().region("sample_keys", || {
            let stream = self.try_sharded_version();

            // Sample each worker.
            let local_output =
                self.circuit()
                    .add_binary_operator(SampleKeys::new(), &stream, sample_size);

            // Sample results from all workers.
            self.circuit().add_binary_operator(
                SampleKeys::new(),
                &local_output.gather(0),
                sample_size,
            )
        })
    }
}

struct SampleKeys<T>
where
    T: BatchReader,
{
    _phantom: PhantomData<T>,
}

impl<T> SampleKeys<T>
where
    T: BatchReader,
{
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for SampleKeys<T>
where
    T: BatchReader + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("SampleKeys")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, usize, OrdZSet<T::Key, T::R>> for SampleKeys<T>
where
    T: BatchReader<Time = ()>,
    T::Key: DBData,
    T::Val: DBData,
    T::R: DBWeight + ZRingValue,
{
    fn eval(&mut self, input_trace: &T, &sample_size: &usize) -> OrdZSet<T::Key, T::R> {
        let sample_size = min(sample_size, MAX_SAMPLE_SIZE);

        if sample_size != 0 {
            let mut sample = Vec::with_capacity(sample_size);
            input_trace.sample_keys(&mut thread_rng(), sample_size, &mut sample);

            let mut builder = <<OrdZSet<_, _> as Batch>::Builder>::with_capacity((), sample.len());
            for key in sample.into_iter() {
                builder.push((key, HasOne::one()));
            }
            builder.done()
        } else {
            <OrdZSet<_, _>>::empty(())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        trace::{
            cursor::Cursor,
            test_batch::{batch_to_tuples, TestBatch},
            BatchReader, Trace,
        },
        CollectionHandle, InputHandle, OrdZSet, OutputHandle, RootCircuit, Runtime,
    };
    use anyhow::Result as AnyResult;
    use std::collections::BTreeSet;

    fn test_circuit(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(
        InputHandle<usize>,
        CollectionHandle<i32, (i32, i32)>,
        OutputHandle<OrdZSet<i32, i32>>,
    )> {
        let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
        let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

        let sample_handle = input_stream
            .shard()
            .integrate_trace()
            .stream_sample_keys(&sample_size_stream)
            .output();

        Ok((sample_size_handle, input_handle, sample_handle))
    }

    use proptest::{collection::vec, prelude::*};

    fn input_trace(
        max_key: i32,
        max_val: i32,
        max_batch_size: usize,
        max_batches: usize,
    ) -> impl Strategy<Value = Vec<(Vec<(i32, i32, i32)>, usize)>> {
        vec(
            (
                vec((0..max_key, 0..max_val, -1..2), 0..max_batch_size),
                (0..max_key as usize),
            ),
            0..max_batches,
        )
    }

    proptest! {
        #[test]
        fn sample_keys_proptest(trace in input_trace(100, 5, 200, 20)) {

            let (mut dbsp, (sample_size_handle, input_handle, output_handle)) =
                Runtime::init_circuit(4, test_circuit).unwrap();

            let mut ref_trace = TestBatch::new(None);

            for (batch, sample_size) in trace.into_iter() {

                let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

                let ref_batch = TestBatch::from_data(&records);
                ref_trace.insert(ref_batch);

                for (k, v, r) in batch.into_iter() {
                    input_handle.push(k, (v, r));
                }

                sample_size_handle.set_for_all(sample_size);

                dbsp.step().unwrap();

                let output = output_handle.consolidate();

                // Validation.

                // Collect all keys in the trace.
                let batch = TestBatch::from_data(&batch_to_tuples(&ref_trace));

                let mut all_keys = Vec::new();
                let mut cursor = batch.cursor();
                while cursor.key_valid() {
                    all_keys.push(cursor.key().clone());
                    cursor.step_key();
                }
                let all_keys_set = all_keys.iter().cloned().collect::<BTreeSet<_>>();

                println!("all_keys: {all_keys_set:?}");
                println!("output: {output:?}");
                assert!(output.key_count() <= all_keys_set.len());
                assert!(output.key_count() <= sample_size);

                let mut cursor = output.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key()));
                    cursor.step_key();
                }
            }
        }
    }
}
