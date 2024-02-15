//! Compute random samples of data.

use crate::{
    algebra::{HasOne, HasZero, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Scope,
    },
    trace::{ord::VecZSet, Batch, BatchReader, Builder, Cursor},
    utils::Tup2,
    Circuit, DBData, DBWeight, RootCircuit, Stream,
};
use rand::thread_rng;
use std::{borrow::Cow, cmp::min, marker::PhantomData};

// Prevent gigantic memory allocations by bounding sample size.
pub const MAX_SAMPLE_SIZE: usize = 10_000_000;
pub const MAX_QUANTILES: usize = 1_000;

pub const fn default_quantiles() -> u32 {
    100
}

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
    /// of the input stream.  Prefix the call to `stream_sample_keys()` with
    /// `integrate_trace()` to sample the integral of the input.
    ///
    /// WARNING: This operator (by definition) returns non-deterministic
    /// outputs.  As such it may not play well with most other DBSP operators
    /// and must be used with care.
    pub fn stream_sample_keys(
        &self,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<B::Key, B::R>> {
        self.circuit().region("stream_sample_keys", || {
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

    /// Generates a uniform random sample of (key,value) pairs from `self`,
    /// assuming that `self` contains exactly one value per key.
    ///
    /// Equivalent to `self.map(|(k, v)| (k, v)).stream_sample_keys()`,
    /// but is more efficient.
    #[allow(clippy::type_complexity)]
    pub fn stream_sample_unique_key_vals(
        &self,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<Tup2<B::Key, B::Val>, B::R>> {
        self.circuit().region("stream_sample_unique_key_vals", || {
            let stream = self.try_sharded_version();

            // Sample each worker using `SampleUniqueKeyVals`.
            let local_output = self.circuit().add_binary_operator(
                SampleUniqueKeyVals::new(),
                &stream,
                sample_size,
            );

            // Sample results from all workers using `SampleKeys`.
            self.circuit().add_binary_operator(
                SampleKeys::new(),
                &local_output.gather(0),
                sample_size,
            )
        })
    }

    /// Generates a subset of keys that partition the set of all keys in `self`
    /// into `num_quantiles + 1` approximately equal-size quantiles.
    ///
    /// Internally, this operator uses the
    /// [`stream_sample_keys`](`Self::stream_sample_keys`) operator to compute a
    /// uniforn random sample of size `num_quantiles ^ 2` and then picks
    /// every `num_quantile`'s element of the sample.
    ///
    /// Maximal supported `num_quantiles` value is [`MAX_QUANTILES`].  If the
    /// operator receives a larger `num_quantiles` value, it treats it as
    /// `MAX_QUANTILES`.
    ///
    /// Outputs a Z-set containing `<=num_quantiles` keys.  Each key is output
    /// with weight `1` regardless of its weight or the number of associated
    /// values in the input batch.
    ///
    /// This is not an incremental operator.  It samples the input
    /// batch received at the current clock cycle and not the integral
    /// of the input stream.  Prefix the call to `stream_key_quantiles()` with
    /// `integrate_trace()` to sample the integral of the input.
    ///
    /// WARNING: This operator returns non-deterministic outputs, i.e.,
    /// feeding the same input twice can produce different outputs.  As such it
    /// may not play well with most other DBSP operators and must be used with
    /// care.
    pub fn stream_key_quantiles(
        &self,
        num_quantiles: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<B::Key, B::R>> {
        let sample_size = num_quantiles.apply(|num| num * num);

        self.stream_sample_keys(&sample_size).apply2_owned(
            num_quantiles,
            |sample, num_quantiles| {
                let num_quantiles = min(*num_quantiles, MAX_QUANTILES);

                let sample_size = sample.key_count();

                if sample_size <= num_quantiles {
                    sample
                } else {
                    let mut builder =
                        <<VecZSet<_, _> as Batch>::Builder>::with_capacity((), num_quantiles);
                    for i in 0..num_quantiles {
                        let key = sample.layer.keys()[(i * sample_size) / num_quantiles].clone();
                        builder.push((key, HasOne::one()));
                    }
                    builder.done()
                }
            },
        )
    }

    /// Generates a subset of (key, value) pairs that partition the set of all
    /// tuples in `self` `num_quantiles + 1` approximately equal-size quantiles,
    /// assuming that `self` contains exactly one value per key.
    ///
    /// Equivalent to `self.map(|(k, v)| (k,
    /// v)).stream_unique_key_val_quantiles()`, but is more efficient.
    #[allow(clippy::type_complexity)]
    pub fn stream_unique_key_val_quantiles(
        &self,
        num_quantiles: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<Tup2<B::Key, B::Val>, B::R>> {
        let sample_size = num_quantiles.apply(|num| num * num);

        self.stream_sample_unique_key_vals(&sample_size)
            .apply2_owned(num_quantiles, |sample, num_quantiles| {
                let num_quantiles = min(*num_quantiles, MAX_QUANTILES);

                let sample_size = sample.key_count();

                if sample_size <= num_quantiles {
                    sample
                } else {
                    let mut builder =
                        <<VecZSet<_, _> as Batch>::Builder>::with_capacity((), num_quantiles);
                    for i in 0..num_quantiles {
                        let key = sample.layer.keys()[(i * sample_size) / num_quantiles].clone();
                        builder.push((key, HasOne::one()));
                    }
                    builder.done()
                }
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

impl<T> BinaryOperator<T, usize, VecZSet<T::Key, T::R>> for SampleKeys<T>
where
    T: BatchReader<Time = ()>,
    T::Key: DBData,
    T::Val: DBData,
    T::R: DBWeight + ZRingValue,
{
    fn eval(&mut self, input_trace: &T, &sample_size: &usize) -> VecZSet<T::Key, T::R> {
        let sample_size = min(sample_size, MAX_SAMPLE_SIZE);

        if sample_size != 0 {
            let mut sample = Vec::with_capacity(sample_size);
            input_trace.sample_keys(&mut thread_rng(), sample_size, &mut sample);

            let mut builder = <<VecZSet<_, _> as Batch>::Builder>::with_capacity((), sample.len());
            for key in sample.into_iter() {
                builder.push((key, HasOne::one()));
            }
            builder.done()
        } else {
            <VecZSet<_, _>>::empty(())
        }
    }
}

struct SampleUniqueKeyVals<T>
where
    T: BatchReader,
{
    _phantom: PhantomData<T>,
}

impl<T> SampleUniqueKeyVals<T>
where
    T: BatchReader,
{
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for SampleUniqueKeyVals<T>
where
    T: BatchReader + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("SampleUniqueKeyVals")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, usize, VecZSet<Tup2<T::Key, T::Val>, T::R>> for SampleUniqueKeyVals<T>
where
    T: BatchReader<Time = ()>,
    T::Key: DBData,
    T::Val: DBData,
    T::R: DBWeight + ZRingValue,
{
    fn eval(
        &mut self,
        input_trace: &T,
        &sample_size: &usize,
    ) -> VecZSet<Tup2<T::Key, T::Val>, T::R> {
        let sample_size = min(sample_size, MAX_SAMPLE_SIZE);

        if sample_size != 0 {
            let mut sample = Vec::with_capacity(sample_size);
            input_trace.sample_keys(&mut thread_rng(), sample_size, &mut sample);

            let mut cursor = input_trace.cursor();
            let mut sample_with_vals = Vec::with_capacity(sample_size);

            for key in sample.into_iter() {
                cursor.seek_key(&key);
                debug_assert!(cursor.key_valid());
                debug_assert_eq!(cursor.key(), &key);

                while cursor.val_valid() {
                    if !cursor.weight().is_zero() {
                        sample_with_vals.push(Tup2(key, cursor.val().clone()));
                        break;
                    }
                    cursor.step_val();
                }
            }

            let mut builder =
                <<VecZSet<_, _> as Batch>::Builder>::with_capacity((), sample_with_vals.len());
            for key in sample_with_vals.into_iter() {
                builder.push((key, HasOne::one()));
            }
            builder.done()
        } else {
            <VecZSet<_, _>>::empty(())
        }
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
mod test {
    use crate::{
        operator::input::Update,
        trace::{
            cursor::Cursor,
            ord::VecZSet,
            test_batch::{batch_to_tuples, TestBatch},
            BatchReader, Trace,
        },
        utils::Tup2,
        CollectionHandle, InputHandle, OrdIndexedZSet, OutputHandle, RootCircuit, Runtime,
        UpsertHandle,
    };
    use anyhow::Result as AnyResult;
    use proptest::{collection::vec, prelude::*};
    use std::{cmp::Ordering, collections::BTreeSet};

    fn test_circuit(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(
        InputHandle<usize>,
        CollectionHandle<i32, Tup2<i32, i32>>,
        OutputHandle<VecZSet<i32, i32>>,
        OutputHandle<VecZSet<i32, i32>>,
    )> {
        let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
        let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32, i32>();

        let sample_handle = input_stream
            .shard()
            .integrate_trace()
            .stream_sample_keys(&sample_size_stream)
            .output();

        let quantile_handle = input_stream
            .shard()
            .integrate_trace()
            .stream_key_quantiles(&sample_size_stream)
            .output();

        Ok((
            sample_size_handle,
            input_handle,
            sample_handle,
            quantile_handle,
        ))
    }

    fn test_unique_key_circuit(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(
        InputHandle<usize>,
        UpsertHandle<i32, Update<i32, i32>>,
        OutputHandle<VecZSet<Tup2<i32, i32>, i32>>,
        OutputHandle<VecZSet<Tup2<i32, i32>, i32>>,
        OutputHandle<OrdIndexedZSet<i32, i32, i32>>,
    )> {
        let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
        let (input_stream, input_handle) =
            circuit.add_input_map::<i32, i32, i32, i32>(Box::new(|v, u| *v = u));

        let sample_handle = input_stream
            .shard()
            .integrate_trace()
            .stream_sample_unique_key_vals(&sample_size_stream)
            .output();

        let quantile_handle = input_stream
            .shard()
            .integrate_trace()
            .stream_unique_key_val_quantiles(&sample_size_stream)
            .output();

        let trace_handle = input_stream.integrate().output();

        Ok((
            sample_size_handle,
            input_handle,
            sample_handle,
            quantile_handle,
            trace_handle,
        ))
    }

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

            let (mut dbsp, (sample_size_handle, input_handle, output_sample_handle, output_quantile_handle)) =
                Runtime::init_circuit(4, test_circuit).unwrap();

            let mut ref_trace = TestBatch::new(None, "");

            for (batch, sample_size) in trace.into_iter() {

                let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

                let ref_batch = TestBatch::from_data(&records);
                ref_trace.insert(ref_batch);

                for (k, v, r) in batch.into_iter() {
                    input_handle.push(k, Tup2(v, r));
                }

                sample_size_handle.set_for_all(sample_size);

                dbsp.step().unwrap();

                let output_sample = output_sample_handle.consolidate();
                let output_quantile = output_quantile_handle.consolidate();

                // Validation.

                // Collect all keys in the trace.
                let batch = TestBatch::from_data(&batch_to_tuples(&ref_trace));

                let mut all_keys = Vec::new();
                let mut cursor = batch.cursor();
                while cursor.key_valid() {
                    all_keys.push(*cursor.key());
                    cursor.step_key();
                }
                let all_keys_set = all_keys.iter().cloned().collect::<BTreeSet<_>>();

                assert!(output_sample.key_count() <= all_keys_set.len());
                assert!(output_sample.key_count() <= sample_size);

                let mut cursor = output_sample.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key()));
                    cursor.step_key();
                }

                assert!(output_quantile.key_count() <= all_keys_set.len());
                assert!(output_quantile.key_count() <= sample_size);

                let mut cursor = output_quantile.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key()));
                    cursor.step_key();
                }
            }
        }

        #[test]
        fn sample_unique_keys_proptest(trace in input_trace(100, 5, 200, 20)) {

            let (mut dbsp, (sample_size_handle, input_handle, output_sample_handle, output_quantile_handle, output_trace_handle)) =
                Runtime::init_circuit(4, test_unique_key_circuit).unwrap();

            for (batch, sample_size) in trace.into_iter() {

                for (k, v, r) in batch.into_iter() {
                    match r.cmp(&0) {
                        Ordering::Greater => {
                            input_handle.push(k, Update::Insert(v));
                        }
                        Ordering::Less => {
                            input_handle.push(k, Update::Delete);
                        }
                        _ => (),
                    }
                }

                sample_size_handle.set_for_all(sample_size);

                dbsp.step().unwrap();

                let output_sample = output_sample_handle.consolidate();
                let output_quantile = output_quantile_handle.consolidate();
                let output_trace = output_trace_handle.consolidate();

                // Validation.

                // Collect all keys in the trace.
                let batch = TestBatch::from_data(&batch_to_tuples(&output_trace));

                let mut all_keys = Vec::new();
                let mut cursor = batch.cursor();
                while cursor.key_valid() {
                    all_keys.push(Tup2(*cursor.key(), *cursor.val()));
                    cursor.step_key();
                }
                let all_keys_set = all_keys.iter().cloned().collect::<BTreeSet<_>>();

                assert!(output_sample.key_count() <= all_keys_set.len());
                assert!(output_sample.key_count() <= sample_size);

                let mut cursor = output_sample.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key()));
                    cursor.step_key();
                }

                assert!(output_quantile.key_count() <= all_keys_set.len());
                assert!(output_quantile.key_count() <= sample_size);

                let mut cursor = output_quantile.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key()));
                    cursor.step_key();
                }
            }
        }

    }
}
