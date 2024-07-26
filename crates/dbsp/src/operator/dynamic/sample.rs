//! Compute random samples of data.

use crate::{
    algebra::{
        zset::{VecZSet, VecZSetFactories},
        HasOne, IndexedZSetReader,
    },
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Scope,
    },
    dynamic::{DynPair, Erase},
    trace::{Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor},
    utils::Tup2,
    Circuit, DBData, RootCircuit, Stream, ZWeight,
};
use minitrace::trace;
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

pub struct StreamSampleUniqueKeyValsFactories<B: IndexedZSetReader> {
    input_factories: B::Factories,
    output_factories: VecZSetFactories<DynPair<B::Key, B::Val>>,
}

impl<B: IndexedZSetReader> Clone for StreamSampleUniqueKeyValsFactories<B> {
    fn clone(&self) -> Self {
        Self {
            input_factories: self.input_factories.clone(),
            output_factories: self.output_factories.clone(),
        }
    }
}

impl<B> StreamSampleUniqueKeyValsFactories<B>
where
    B: IndexedZSetReader,
{
    pub fn new<KType, VType>() -> Self
    where
        KType: DBData + Erase<B::Key>,
        VType: DBData + Erase<B::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<Tup2<KType, VType>, (), ZWeight>(),
        }
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSetReader + Clone,
{
    /// See [`Stream::stream_sample_keys`].
    pub fn dyn_stream_sample_keys(
        &self,
        output_factories: &VecZSetFactories<B::Key>,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<B::Key>> {
        self.circuit().region("stream_sample_keys", || {
            let stream = self.try_sharded_version();

            // Sample each worker.
            let local_output = self.circuit().add_binary_operator(
                SampleKeys::new(output_factories),
                &stream,
                sample_size,
            );

            // Sample results from all workers.
            self.circuit().add_binary_operator(
                SampleKeys::new(output_factories),
                &local_output.dyn_gather(output_factories, 0),
                sample_size,
            )
        })
    }

    /// See [`Stream::stream_sample_unique_key_vals`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_stream_sample_unique_key_vals(
        &self,
        factories: &StreamSampleUniqueKeyValsFactories<B>,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<DynPair<B::Key, B::Val>>> {
        self.circuit().region("stream_sample_unique_key_vals", || {
            let stream = self.try_sharded_version();

            // Sample each worker using `SampleUniqueKeyVals`.
            let local_output = self.circuit().add_binary_operator(
                SampleUniqueKeyVals::new(&factories.input_factories, &factories.output_factories),
                &stream,
                sample_size,
            );

            // Sample results from all workers using `SampleKeys`.
            self.circuit().add_binary_operator(
                SampleKeys::new(&factories.output_factories),
                &local_output.dyn_gather(&factories.output_factories, 0),
                sample_size,
            )
        })
    }

    /// See [`Stream::stream_key_quantiles`].
    pub fn dyn_stream_key_quantiles(
        &self,
        output_factories: &VecZSetFactories<B::Key>,
        num_quantiles: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<B::Key>> {
        let sample_size = num_quantiles.apply(|num| num * num);

        let output_factories = output_factories.clone();

        self.dyn_stream_sample_keys(&output_factories, &sample_size)
            .apply2_owned(num_quantiles, move |sample, num_quantiles| {
                let num_quantiles = min(*num_quantiles, MAX_QUANTILES);

                let sample_size = sample.key_count();

                if sample_size <= num_quantiles {
                    sample
                } else {
                    let mut builder = <<VecZSet<_> as Batch>::Builder>::with_capacity(
                        &output_factories,
                        (),
                        num_quantiles,
                    );
                    for i in 0..num_quantiles {
                        let key = &sample.layer.keys[(i * sample_size) / num_quantiles];
                        builder.push_refs(key, ().erase(), ZWeight::one().erase());
                    }
                    builder.done()
                }
            })
    }

    /// See [`Stream::stream_unique_key_val_quantiles`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_stream_unique_key_val_quantiles(
        &self,
        factories: &StreamSampleUniqueKeyValsFactories<B>,
        num_quantiles: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, VecZSet<DynPair<B::Key, B::Val>>> {
        let sample_size = num_quantiles.apply(|num| num * num);

        let factories = factories.clone();

        self.dyn_stream_sample_unique_key_vals(&factories, &sample_size)
            .apply2_owned(num_quantiles, move |sample, num_quantiles| {
                let num_quantiles = min(*num_quantiles, MAX_QUANTILES);

                let sample_size = sample.key_count();

                if sample_size <= num_quantiles {
                    sample
                } else {
                    let mut builder = <<VecZSet<_> as Batch>::Builder>::with_capacity(
                        &factories.output_factories,
                        (),
                        num_quantiles,
                    );
                    for i in 0..num_quantiles {
                        let key = &sample.layer.keys[(i * sample_size) / num_quantiles];
                        builder.push_refs(key, ().erase(), ZWeight::one().erase());
                    }
                    builder.done()
                }
            })
    }
}

struct SampleKeys<T>
where
    T: IndexedZSetReader,
{
    output_factories: VecZSetFactories<T::Key>,
    _phantom: PhantomData<T>,
}

impl<T> SampleKeys<T>
where
    T: IndexedZSetReader,
{
    fn new(output_factories: &VecZSetFactories<T::Key>) -> Self {
        Self {
            output_factories: output_factories.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for SampleKeys<T>
where
    T: IndexedZSetReader + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("SampleKeys")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, usize, VecZSet<T::Key>> for SampleKeys<T>
where
    T: IndexedZSetReader,
{
    #[trace]
    fn eval(&mut self, input_trace: &T, &sample_size: &usize) -> VecZSet<T::Key> {
        let sample_size = min(sample_size, MAX_SAMPLE_SIZE);

        if sample_size != 0 {
            let mut sample = self.output_factories.keys_factory().default_box();
            sample.reserve(sample_size);

            input_trace.sample_keys(&mut thread_rng(), sample_size, sample.as_mut());

            let mut builder = <<VecZSet<_> as Batch>::Builder>::with_capacity(
                &self.output_factories,
                (),
                sample.len(),
            );
            for key in sample.dyn_iter_mut() {
                builder.push_vals(key, ().erase_mut(), ZWeight::one().erase_mut());
            }
            builder.done()
        } else {
            <VecZSet<_>>::dyn_empty(&self.output_factories)
        }
    }
}

struct SampleUniqueKeyVals<T>
where
    T: IndexedZSetReader,
{
    input_factories: T::Factories,
    output_factories: VecZSetFactories<DynPair<T::Key, T::Val>>,
    _phantom: PhantomData<T>,
}

impl<T> SampleUniqueKeyVals<T>
where
    T: IndexedZSetReader,
{
    fn new(
        input_factories: &T::Factories,
        output_factories: &VecZSetFactories<DynPair<T::Key, T::Val>>,
    ) -> Self {
        Self {
            input_factories: input_factories.clone(),
            output_factories: output_factories.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Operator for SampleUniqueKeyVals<T>
where
    T: IndexedZSetReader,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("SampleUniqueKeyVals")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T> BinaryOperator<T, usize, VecZSet<DynPair<T::Key, T::Val>>> for SampleUniqueKeyVals<T>
where
    T: IndexedZSetReader,
{
    #[trace]
    fn eval(&mut self, input_trace: &T, &sample_size: &usize) -> VecZSet<DynPair<T::Key, T::Val>> {
        let sample_size = min(sample_size, MAX_SAMPLE_SIZE);

        if sample_size != 0 {
            let mut sample = self.input_factories.keys_factory().default_box();
            sample.reserve(sample_size);

            let mut item = self.input_factories.item_factory().default_box();

            input_trace.sample_keys(&mut thread_rng(), sample_size, sample.as_mut());

            let mut cursor = input_trace.cursor();

            let mut builder = <<VecZSet<_> as Batch>::Builder>::with_capacity(
                &self.output_factories,
                (),
                sample_size,
            );

            for key in sample.dyn_iter_mut() {
                cursor.seek_key(key);
                debug_assert!(cursor.key_valid());
                debug_assert_eq!(cursor.key(), key);

                while cursor.val_valid() {
                    if !cursor.weight().is_zero() {
                        item.from_refs(key, cursor.val());
                        builder.push_refs(
                            item.as_mut(),
                            ().erase_mut(),
                            ZWeight::one().erase_mut(),
                        );
                        break;
                    }
                    cursor.step_val();
                }
            }

            builder.done()
        } else {
            <VecZSet<_>>::dyn_empty(&self.output_factories)
        }
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
mod test {
    use crate::{
        dynamic::{DowncastTrait, DynData, DynPair},
        operator::{IndexedZSetHandle, InputHandle, MapHandle, OutputHandle, Update},
        trace::{
            test::test_batch::{batch_to_tuples, TestBatch, TestBatchFactories},
            Cursor, Trace,
        },
        typed_batch::{
            BatchReader, DynBatchReader, DynVecZSet, OrdIndexedZSet, TypedBatch, VecZSet,
        },
        utils::Tup2,
        DynZWeight, RootCircuit, Runtime, ZWeight,
    };
    use anyhow::Result as AnyResult;
    use proptest::{collection::vec, prelude::*};
    use std::{cmp::Ordering, collections::BTreeSet};

    fn test_circuit(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(
        InputHandle<usize>,
        IndexedZSetHandle<i32, i32>,
        OutputHandle<VecZSet<i32>>,
        OutputHandle<VecZSet<i32>>,
    )> {
        let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
        let (input_stream, input_handle) = circuit.add_input_indexed_zset::<i32, i32>();

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
        MapHandle<i32, i32, i32>,
        OutputHandle<
            TypedBatch<
                Tup2<i32, i32>,
                (),
                ZWeight,
                DynVecZSet<DynPair<DynData /* <i32> */, DynData /* <i32> */>>,
            >,
        >,
        OutputHandle<
            TypedBatch<
                Tup2<i32, i32>,
                (),
                ZWeight,
                DynVecZSet<DynPair<DynData /* <i32> */, DynData /* <i32> */>>,
            >,
        >,
        OutputHandle<OrdIndexedZSet<i32, i32>>,
    )> {
        let (sample_size_stream, sample_size_handle) = circuit.add_input_stream::<usize>();
        let (input_stream, input_handle) =
            circuit.add_input_map::<i32, i32, i32, _>(|v, u| *v = *u);

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
    ) -> impl Strategy<Value = Vec<(Vec<(i32, i32, ZWeight)>, usize)>> {
        vec(
            (
                vec((0..max_key, 0..max_val, -1..2i64), 0..max_batch_size),
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

            let mut ref_trace: TestBatch<DynData/*<i32>*/, DynData/*<i32>*/, (), DynZWeight> = TestBatch::new(&TestBatchFactories::new());

            for (batch, sample_size) in trace.into_iter() {

                let records = batch.iter().map(|(k, v, r)| ((*k, *v, ()), *r)).collect::<Vec<_>>();

                let ref_batch = TestBatch::from_typed_data(&records);
                ref_trace.insert(ref_batch);

                for (k, v, r) in batch.into_iter() {
                    input_handle.push(k, (v, r));
                }

                sample_size_handle.set_for_all(sample_size);

                dbsp.step().unwrap();

                let output_sample = output_sample_handle.consolidate();
                let output_quantile = output_quantile_handle.consolidate();

                // Validation.

                // Collect all keys in the trace.
                let batch: TestBatch<DynData/*<i32>*/, DynData/*<i32>*/, (), DynZWeight> = TestBatch::from_data(&batch_to_tuples(&ref_trace));

                let mut all_keys = Vec::new();
                let mut cursor = batch.cursor();
                while cursor.key_valid() {
                    all_keys.push(*cursor.key().downcast_checked::<i32>());
                    cursor.step_key();
                }
                let all_keys_set = all_keys.iter().cloned().collect::<BTreeSet<_>>();

                assert!(output_sample.key_count() <= all_keys_set.len());
                assert!(output_sample.key_count() <= sample_size);

                let mut cursor = output_sample.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key().downcast_checked()));
                    cursor.step_key();
                }

                assert!(output_quantile.key_count() <= all_keys_set.len());
                assert!(output_quantile.key_count() <= sample_size);

                let mut cursor = output_quantile.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key().downcast_checked()));
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
                let batch: TestBatch<DynData/*<i32>*/, DynData/*<i32>*/, (), DynZWeight> = TestBatch::from_data(&batch_to_tuples(output_trace.inner()));

                let mut all_keys = Vec::new();
                let mut cursor = batch.cursor();
                while cursor.key_valid() {
                    all_keys.push(Tup2(*cursor.key().downcast_checked::<i32>(), *cursor.val().downcast_checked::<i32>()));
                    cursor.step_key();
                }
                let all_keys_set = all_keys.iter().cloned().collect::<BTreeSet<_>>();

                assert!(output_sample.key_count() <= all_keys_set.len());
                assert!(output_sample.key_count() <= sample_size);

                let mut cursor = output_sample.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key().downcast_checked()));
                    cursor.step_key();
                }

                assert!(output_quantile.key_count() <= all_keys_set.len());
                assert!(output_quantile.key_count() <= sample_size);

                let mut cursor = output_quantile.cursor();
                while cursor.key_valid() {
                    assert!(all_keys_set.contains(cursor.key().downcast_checked()));
                    cursor.step_key();
                }
            }
        }

    }
}
