use crate::{
    algebra::{IndexedZSet, ZBatch},
    circuit::{
        metadata::{
            BatchSizeStats, MetaItem, OperatorMeta, INPUT_BATCHES_LABEL, OUTPUT_BATCHES_LABEL,
        },
        operator_traits::Operator,
        splitter_output_chunk_size,
    },
    dynamic::Erase,
    operator::{
        async_stream_operators::{StreamingNaryOperator, StreamingNaryWrapper},
        dynamic::{MonoIndexedZSet, MonoIndexedZSetFactories, MonoZSet, MonoZSetFactories},
    },
    trace::{
        cursor::{CursorList, CursorWithPolarity},
        BatchReader, BatchReaderFactories, Builder, Cursor, Spine, SpineSnapshot, WithSnapshot,
    },
    Circuit, NestedCircuit, Position, RootCircuit, Stream,
};
use async_stream::stream;
use std::{borrow::Cow, cell::RefCell, marker::PhantomData};

impl RootCircuit {
    /// See [`RootCircuit::accumulate_concat_indexed_zsets`].
    pub fn dyn_accumulate_concat_indexed_zsets(
        &self,
        factories: &MonoIndexedZSetFactories,
        streams: &[(Stream<Self, MonoIndexedZSet>, bool)],
    ) -> Stream<Self, MonoIndexedZSet> {
        dyn_accumulate_concat(factories, streams.iter().cloned())
    }

    /// See [`RootCircuit::accumulate_concat_zsets`].
    pub fn dyn_accumulate_concat_zsets(
        &self,
        factories: &MonoZSetFactories,
        streams: &[(Stream<Self, MonoZSet>, bool)],
    ) -> Stream<Self, MonoZSet> {
        dyn_accumulate_concat(factories, streams.iter().cloned())
    }
}

impl NestedCircuit {
    /// See [`NestedCircuit::accumulate_concat_indexed_zsets`].
    pub fn dyn_acumulate_concat_indexed_zsets(
        &self,
        factories: &MonoIndexedZSetFactories,
        streams: &[(Stream<Self, MonoIndexedZSet>, bool)],
    ) -> Stream<Self, MonoIndexedZSet> {
        dyn_accumulate_concat(factories, streams.iter().cloned())
    }

    /// See [`NestedCircuit::accumulate_concat_zsets`].
    pub fn dyn_accumulate_concat_zsets(
        &self,
        factories: &MonoZSetFactories,
        streams: &[(Stream<Self, MonoZSet>, bool)],
    ) -> Stream<Self, MonoZSet> {
        dyn_accumulate_concat(factories, streams.iter().cloned())
    }
}

pub fn dyn_accumulate_concat<C, Z, I>(input_factories: &Z::Factories, streams: I) -> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet,
    I: IntoIterator<Item = (Stream<C, Z>, bool)>,
{
    let (streams, polarities): (Vec<_>, Vec<_>) = streams.into_iter().unzip();

    let streams: Vec<Stream<C, Z>> = streams
        .into_iter()
        .map(|stream| stream.try_sharded_version())
        .collect();
    assert!(!streams.is_empty());

    let sharded = streams.iter().all(|stream| stream.is_sharded());

    let accumulated_streams = streams
        .iter()
        .map(|stream| stream.dyn_accumulate(input_factories))
        .collect::<Vec<_>>();

    let circuit = streams[0].circuit();

    let result = circuit.add_nary_operator(
        StreamingNaryWrapper::new(AccumulateConcatZSets::new(input_factories, &polarities)),
        &accumulated_streams,
    );

    if sharded {
        result.mark_sharded();
    }

    result
}

struct AccumulateConcatZSets<Z>
where
    Z: ZBatch,
{
    factories: Z::Factories,
    snapshots: RefCell<Vec<Option<SpineSnapshot<Z>>>>,
    input_batch_stats: RefCell<Vec<BatchSizeStats>>,
    output_batch_stats: RefCell<BatchSizeStats>,
    polarity: Vec<bool>,
    flush: RefCell<bool>,
    phantom: PhantomData<fn(&Z)>,
}

impl<Z> AccumulateConcatZSets<Z>
where
    Z: ZBatch,
{
    pub fn new(factories: &Z::Factories, polarity: &[bool]) -> Self {
        Self {
            factories: factories.clone(),
            snapshots: RefCell::new(Vec::new()),
            input_batch_stats: RefCell::new(Vec::new()),
            output_batch_stats: RefCell::new(BatchSizeStats::new()),
            polarity: polarity.to_vec(),
            flush: RefCell::new(false),
            phantom: PhantomData,
        }
    }
}

impl<Z> Operator for AccumulateConcatZSets<Z>
where
    Z: IndexedZSet,
{
    fn name(&self) -> std::borrow::Cow<'static, str> {
        Cow::Borrowed("AccumulateConcat")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_LABEL => MetaItem::Array(self.input_batch_stats.borrow().iter().map(|stats| stats.metadata()).collect()),
            OUTPUT_BATCHES_LABEL => self.output_batch_stats.borrow().metadata(),
        });
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        true
    }

    fn flush(&mut self) {
        *self.flush.borrow_mut() = true;
    }
}

impl<Z: IndexedZSet> StreamingNaryOperator<Option<Spine<Z>>, Z> for AccumulateConcatZSets<Z> {
    fn eval<'a, Iter>(
        self: std::rc::Rc<Self>,
        inputs: Iter,
    ) -> impl futures::Stream<Item = (Z, bool, Option<Position>)> + 'static
    where
        Iter: Iterator<Item = Cow<'a, Option<Spine<Z>>>>,
    {
        let inputs = inputs.into_iter().collect::<Vec<_>>();
        assert_eq!(inputs.len(), self.polarity.len());

        let mut snapshots = self.snapshots.borrow_mut();

        snapshots.resize(inputs.len(), None);

        // Latch final output of each input stream.
        for (i, input) in inputs.iter().enumerate() {
            let snapshot = input.as_ref().as_ref().map(|spine| spine.ro_snapshot());

            if snapshot.is_some() && snapshots[i].is_some() {
                panic!("received input {i} twice");
            }

            if let Some(snapshot) = snapshot {
                let mut input_batch_stats = self.input_batch_stats.borrow_mut();
                input_batch_stats.resize_with(inputs.len(), BatchSizeStats::new);
                input_batch_stats[i].add_batch(snapshot.len());

                snapshots[i] = Some(snapshot);
            }
        }
        drop(snapshots);

        let flush = *self.flush.borrow();
        *self.flush.borrow_mut() = false;
        let factories = self.factories.clone();

        let snapshots = if flush {
            self.snapshots
                .borrow_mut()
                .iter_mut()
                .enumerate()
                .map(|(i, snapshot)| {
                    if snapshot.is_none() {
                        panic!("input {i} is empty on flush");
                    }
                    snapshot.take().unwrap()
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        stream! {
            if !flush {
                yield (Z::dyn_empty(&factories), true, None);
                return;
            }

            let chunk_size = splitter_output_chunk_size();
            let cursors = snapshots.into_iter().enumerate().map(|(i, snapshot)| CursorWithPolarity::new(snapshot.cursor(), self.polarity[i])).collect::<Vec<_>>();

            let mut builder = Z::Builder::with_capacity(&factories, chunk_size);
            let mut cursor = CursorList::new(factories.weight_factory(), cursors);

            let mut has_val = false;

            while cursor.key_valid() {
                while cursor.val_valid() {
                    let w = **cursor.weight();
                    builder.push_val_diff(cursor.val(), w.erase());
                    has_val=true;

                    if builder.num_tuples() >= chunk_size {
                        builder.push_key(cursor.key());
                        has_val = false;

                        let result = builder.done();
                        self.output_batch_stats.borrow_mut().add_batch(result.len());
                        yield (result, false, cursor.position());
                        builder = Z::Builder::with_capacity(&factories, chunk_size);
                    }
                    cursor.step_val();
                }
                if has_val{
                    builder.push_key(cursor.key());
                }
                cursor.step_key();
            }

            let result = builder.done();
            self.output_batch_stats.borrow_mut().add_batch(result.len());

            yield (result, true, cursor.position())
        }
    }
}

#[cfg(test)]
mod test {

    use proptest::{collection::vec, prelude::*};

    use crate::{algebra::NegByRef, circuit::CircuitConfig, utils::Tup2, DBData, Runtime, ZWeight};

    fn test_zset<K: DBData>(inputs: Vec<(Vec<Vec<Tup2<K, ZWeight>>>, bool)>, transaction: bool) {
        let (mut inputs, polarities): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();

        let (mut dbsp, (input_handles, output_handle, expected_output_handle)) =
            Runtime::init_circuit(
                CircuitConfig::from(4).with_splitter_chunk_size_records(2),
                move |circuit| {
                    let mut streams = Vec::new();
                    let mut input_handles = Vec::new();

                    for polarity in polarities.iter() {
                        let (stream, handle) = circuit.add_input_zset::<K>();
                        streams.push((stream, *polarity));
                        input_handles.push(handle)
                    }

                    let output_handle = circuit
                        .accumulate_concat_zsets(&streams)
                        .accumulate_output();

                    let streams_with_polarities = streams
                        .iter()
                        .map(|(stream, polarity)| {
                            if *polarity {
                                stream.clone()
                            } else {
                                stream.apply(|batch| batch.neg_by_ref())
                            }
                        })
                        .collect::<Vec<_>>();

                    let expected_output_handle = streams_with_polarities[0]
                        .sum(streams_with_polarities[1..].iter())
                        .accumulate_output();

                    Ok((input_handles, output_handle, expected_output_handle))
                },
            )
            .unwrap();

        if transaction {
            dbsp.start_transaction().unwrap();
        }

        for step in 0..inputs[0].len() {
            for i in 0..input_handles.len() {
                input_handles[i].append(&mut inputs[i][step]);
            }
            if !transaction {
                dbsp.transaction().unwrap();
                assert_eq!(
                    output_handle.concat().consolidate(),
                    expected_output_handle.concat().consolidate()
                )
            } else {
                dbsp.step().unwrap();
            }
        }

        if transaction {
            dbsp.commit_transaction().unwrap();
            assert_eq!(
                output_handle.concat().consolidate(),
                expected_output_handle.concat().consolidate()
            )
        }
    }

    fn test_indexed_zset<K: DBData, V: DBData>(
        inputs: Vec<(Vec<Vec<Tup2<K, Tup2<V, ZWeight>>>>, bool)>,
        transaction: bool,
    ) {
        let (mut inputs, polarities): (Vec<_>, Vec<_>) = inputs.into_iter().unzip();

        let (mut dbsp, (input_handles, output_handle, expected_output_handle)) =
            Runtime::init_circuit(
                CircuitConfig::from(4).with_splitter_chunk_size_records(2),
                move |circuit| {
                    let mut streams = Vec::new();
                    let mut input_handles = Vec::new();

                    for polarity in polarities.iter() {
                        let (stream, handle) = circuit.add_input_indexed_zset::<K, V>();
                        streams.push((stream, *polarity));
                        input_handles.push(handle)
                    }

                    let output_handle = circuit
                        .accumulate_concat_indexed_zsets(&streams)
                        .accumulate_output();

                    let streams_with_polarities = streams
                        .iter()
                        .map(|(stream, polarity)| {
                            if *polarity {
                                stream.clone()
                            } else {
                                stream.apply(|batch| batch.neg_by_ref())
                            }
                        })
                        .collect::<Vec<_>>();

                    let expected_output_handle = streams_with_polarities[0]
                        .sum(streams_with_polarities[1..].iter())
                        .accumulate_output();

                    Ok((input_handles, output_handle, expected_output_handle))
                },
            )
            .unwrap();

        if transaction {
            dbsp.start_transaction().unwrap();
        }

        for step in 0..inputs[0].len() {
            for i in 0..input_handles.len() {
                input_handles[i].append(&mut inputs[i][step]);
            }
            if !transaction {
                dbsp.transaction().unwrap();
                assert_eq!(
                    output_handle.concat().consolidate(),
                    expected_output_handle.concat().consolidate()
                )
            } else {
                dbsp.step().unwrap();
            }
        }

        if transaction {
            dbsp.commit_transaction().unwrap();
            assert_eq!(
                output_handle.concat().consolidate(),
                expected_output_handle.concat().consolidate()
            )
        }
    }

    fn generate_test_zset(
        max_key: i32,
        max_weight: ZWeight,
        max_tuples: usize,
    ) -> BoxedStrategy<Vec<Tup2<i32, ZWeight>>> {
        vec(
            (0..max_key, -max_weight..max_weight).prop_map(|(x, y)| Tup2(x, y)),
            0..max_tuples,
        )
        .boxed()
    }

    fn generate_test_zsets(
        max_key: i32,
        max_weight: ZWeight,
        max_tuples: usize,
    ) -> BoxedStrategy<(
        Vec<Vec<Tup2<i32, ZWeight>>>,
        Vec<Vec<Tup2<i32, ZWeight>>>,
        Vec<Vec<Tup2<i32, ZWeight>>>,
    )> {
        (
            vec(generate_test_zset(max_key, max_weight, max_tuples), 10),
            vec(generate_test_zset(max_key, max_weight, max_tuples), 10),
            vec(generate_test_zset(max_key, max_weight, max_tuples), 10),
        )
            .boxed()
    }

    fn generate_test_indexed_zset(
        max_key: i32,
        max_val: i32,
        max_weight: ZWeight,
        max_tuples: usize,
    ) -> BoxedStrategy<Vec<Tup2<i32, Tup2<i32, ZWeight>>>> {
        vec(
            (0..max_key, 0..max_val, -max_weight..max_weight)
                .prop_map(|(x, y, z)| Tup2(x, Tup2(y, z))),
            0..max_tuples,
        )
        .boxed()
    }

    fn generate_test_indexed_zsets(
        max_key: i32,
        max_val: i32,
        max_weight: ZWeight,
        max_tuples: usize,
    ) -> BoxedStrategy<(
        Vec<Vec<Tup2<i32, Tup2<i32, ZWeight>>>>,
        Vec<Vec<Tup2<i32, Tup2<i32, ZWeight>>>>,
        Vec<Vec<Tup2<i32, Tup2<i32, ZWeight>>>>,
    )> {
        (
            vec(
                generate_test_indexed_zset(max_key, max_val, max_weight, max_tuples),
                10,
            ),
            vec(
                generate_test_indexed_zset(max_key, max_val, max_weight, max_tuples),
                10,
            ),
            vec(
                generate_test_indexed_zset(max_key, max_val, max_weight, max_tuples),
                10,
            ),
        )
            .boxed()
    }

    proptest! {
        #[test]
        fn proptest_concat_zset_big_step(inputs in generate_test_zsets(10, 3, 100)) {
            let (inputs1, inputs2, inputs3) = inputs;
            test_zset(vec![(inputs1, true), (inputs2, false), (inputs3, true)], true);
        }

        #[test]
        fn proptest_concat_zset_small_step(inputs in generate_test_zsets(10, 3, 100)) {
            let (inputs1, inputs2, inputs3) = inputs;
            test_zset(vec![(inputs1, true), (inputs2, false), (inputs3, true)], false);
        }

        #[test]
        fn proptest_concat_indexed_zset_big_step(inputs in generate_test_indexed_zsets(10, 5, 3, 100)) {
            let (inputs1, inputs2, inputs3) = inputs;
            test_indexed_zset(vec![(inputs1, true), (inputs2, false), (inputs3, true)], true);
        }

        #[test]
        fn proptest_concat_indexed_zset_small_step(inputs in generate_test_indexed_zsets(10, 5, 3, 100)) {
            let (inputs1, inputs2, inputs3) = inputs;
            test_indexed_zset(vec![(inputs1, true), (inputs2, false), (inputs3, true)], false);
        }
    }
}
