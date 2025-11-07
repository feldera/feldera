use std::panic::Location;

use crate::{
    algebra::{IndexedZSet, OrdIndexedZSet},
    dynamic::{DataTrait, DynData, DynUnit},
    operator::{
        async_stream_operators::StreamingBinaryWrapper,
        dynamic::{
            join::{JoinFactories, JoinTrace, TraceJoinFuncs},
            saturate::SaturateFactories,
            MonoIndexedZSet, MonoZSet,
        },
    },
    Circuit, RootCircuit, Stream,
};

impl Stream<RootCircuit, MonoIndexedZSet> {
    #[track_caller]
    pub fn dyn_left_join_mono(
        &self,
        factories: &JoinFactories<MonoIndexedZSet, MonoIndexedZSet, (), MonoZSet>,
        other: &Stream<RootCircuit, MonoIndexedZSet>,
        join_funcs: TraceJoinFuncs<DynData, DynData, DynData, DynData, DynUnit>,
    ) -> Stream<RootCircuit, MonoZSet> {
        self.dyn_left_join(factories, other, join_funcs)
    }

    #[track_caller]
    pub fn dyn_left_join_index_mono(
        &self,
        factories: &JoinFactories<MonoIndexedZSet, MonoIndexedZSet, (), MonoIndexedZSet>,
        other: &Stream<RootCircuit, MonoIndexedZSet>,
        join_funcs: TraceJoinFuncs<DynData, DynData, DynData, DynData, DynData>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_left_join(factories, other, join_funcs)
    }
}

impl<K, V1> Stream<RootCircuit, OrdIndexedZSet<K, V1>>
where
    K: DataTrait + ?Sized,
    V1: DataTrait + ?Sized,
{
    /// Left-join `self` with `other`.
    ///
    /// For any key on the left that is not present on the right, passes the default
    /// value of `V2` to the join function. This assumes that the underlying concrete
    /// type if `Option<T>` and that the `other` stream does not contain any `None` values.
    #[track_caller]
    pub fn dyn_left_join<V2, Z>(
        &self,
        factories: &JoinFactories<OrdIndexedZSet<K, V1>, OrdIndexedZSet<K, V2>, (), Z>,
        other: &Stream<RootCircuit, OrdIndexedZSet<K, V2>>,
        join_funcs: TraceJoinFuncs<K, V1, V2, Z::Key, Z::Val>,
    ) -> Stream<RootCircuit, Z>
    where
        V2: DataTrait + ?Sized,
        Z: IndexedZSet,
    {
        // The implementation is identical to `join`, except these two changes that
        // are necessary to implement the left join as an inner join:
        // - we saturate the `other` stream
        // - we set the `SATURATE` parameter to `true` for the first join operator.

        let right_saturated_factories = SaturateFactories {
            batch_factories: factories.right_factories.clone(),
            trace_factories: factories.right_trace_factories.clone(),
        };

        self.circuit().region("left_join", || {
            let left = self.dyn_shard(&factories.left_factories);
            let right = other.dyn_shard(&factories.right_factories);
            let right_saturated = right
                //.inspect(|s| println!("right: {:?}", s))
                .dyn_saturate(&right_saturated_factories);
            //.inspect(|s| println!("right_saturated: {:?}", s));

            let left_trace = left
                .dyn_accumulate_trace(&factories.left_trace_factories, &factories.left_factories);
            let right_trace = right
                .dyn_accumulate_trace(&factories.right_trace_factories, &factories.right_factories);

            let left = self.circuit().add_binary_operator(
                StreamingBinaryWrapper::new(JoinTrace::<_, _, _, _, _, true>::new(
                    &factories.right_trace_factories,
                    &factories.output_factories,
                    factories.timed_item_factory,
                    factories.timed_items_factory,
                    join_funcs.left,
                    Location::caller(),
                    self.circuit().clone(),
                )),
                &left.dyn_accumulate(&factories.left_factories),
                &right_trace,
            );

            let right = self.circuit().add_binary_operator(
                StreamingBinaryWrapper::new(JoinTrace::<_, _, _, _, _, false>::new(
                    &factories.left_trace_factories,
                    &factories.output_factories,
                    factories.timed_item_factory,
                    factories.timed_items_factory,
                    join_funcs.right,
                    Location::caller(),
                    self.circuit().clone(),
                )),
                &right_saturated,
                &left_trace.accumulate_delay_trace(),
            );

            left.plus(&right)
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{
        algebra::DefaultSemigroup,
        circuit::CircuitConfig,
        operator::{dynamic::join::test::generate_join_test_data, Fold},
        utils::Tup2,
        DBData, OrdIndexedZSet, OrdZSet, RootCircuit, Runtime, Stream, ZWeight,
    };
    use proptest::prelude::*;

    impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
    where
        K: DBData,
        V: DBData,
    {
        #[track_caller]
        pub fn reference_left_join<F, V2, OV>(
            &self,
            other: &Stream<RootCircuit, OrdIndexedZSet<K, Option<V2>>>,
            join: F,
        ) -> Stream<RootCircuit, OrdZSet<OV>>
        where
            V2: DBData,
            OV: DBData,
            F: Fn(&K, &V, &Option<V2>) -> OV + Clone + 'static,
        {
            let inner = self.join(other, join.clone());

            let outer = self.plus(
                &self
                    .join_index(
                        &other.aggregate(<Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                            1,
                            |sum: &mut i64, _v: &Option<V2>, _w| *sum = 1,
                        )),
                        |k, v, _| Some((k.clone(), v.clone())),
                    )
                    .neg(),
            );

            let outer = outer.map(move |(k, v)| join(k, v, &None));

            inner.plus(&outer)
        }

        #[track_caller]
        pub fn reference_left_join_index<F, V2, OK, OV, It>(
            &self,
            other: &Stream<RootCircuit, OrdIndexedZSet<K, Option<V2>>>,
            join: F,
        ) -> Stream<RootCircuit, OrdIndexedZSet<OK, OV>>
        where
            V2: DBData,
            OK: DBData,
            OV: DBData,
            F: Fn(&K, &V, &Option<V2>) -> It + Clone + 'static,
            It: IntoIterator<Item = (OK, OV)> + 'static,
        {
            let inner = self.join_index(other, join.clone());

            let outer = self.plus(
                &self
                    .join_index(
                        &other.aggregate(<Fold<_, _, DefaultSemigroup<_>, _, _>>::new(
                            1,
                            |sum: &mut i64, _v: &Option<V2>, _w| *sum = 1,
                        )),
                        |k, v, _| Some((k.clone(), v.clone())),
                    )
                    .neg(),
            );

            let outer = outer.flat_map_index(move |(k, v)| join(k, v, &None));

            inner.plus(&outer)
        }
    }

    fn proptest_left_join<K: DBData, V: DBData, OV: DBData>(
        mut left_inputs: Vec<Vec<Tup2<K, Tup2<V, ZWeight>>>>,
        right_inputs: Vec<Vec<Tup2<K, Tup2<V, ZWeight>>>>,
        f: impl Fn(&K, &V, &Option<V>) -> OV + Clone + Send + 'static,
        transaction: bool,
    ) {
        let (mut dbsp, (left_handle, right_handle, output_handle, expected_output_handle)) =
            Runtime::init_circuit(
                CircuitConfig::from(4).with_splitter_chunk_size_records(2),
                move |circuit| {
                    let (left_input, left_handle) = circuit.add_input_indexed_zset::<K, V>();
                    let (right_input, right_handle) =
                        circuit.add_input_indexed_zset::<K, Option<V>>();

                    let output_handle = left_input
                        .left_join(&right_input, f.clone())
                        .accumulate_output();

                    let f = f.clone();

                    let expected_output_handle = left_input
                        .reference_left_join(&right_input, f.clone())
                        .accumulate_output();

                    Ok((
                        left_handle,
                        right_handle,
                        output_handle,
                        expected_output_handle,
                    ))
                },
            )
            .unwrap();

        if transaction {
            dbsp.start_transaction().unwrap();
        }

        let mut right_inputs: Vec<Vec<Tup2<K, Tup2<Option<V>, ZWeight>>>> = right_inputs
            .into_iter()
            .map(|input| {
                input
                    .into_iter()
                    .map(|Tup2(k, Tup2(v, w))| Tup2(k, Tup2(Some(v), w)))
                    .collect()
            })
            .collect();

        for step in 0..left_inputs.len() {
            // println!(
            //     "step: left: {:?} right: {:?}",
            //     left_inputs[step], right_inputs[step]
            // );

            left_handle.append(&mut left_inputs[step]);
            right_handle.append(&mut right_inputs[step]);

            if !transaction {
                dbsp.transaction().unwrap();
                let output = output_handle.concat().consolidate();
                let expected_output = expected_output_handle.concat().consolidate();
                // println!(
                //     "output: {:?} expected_output: {:?}",
                //     output, expected_output
                // );
                assert_eq!(output, expected_output)
            } else {
                dbsp.step().unwrap();
            }
        }

        if transaction {
            dbsp.commit_transaction().unwrap();
            let output = output_handle.concat().consolidate();

            assert_eq!(output, expected_output_handle.concat().consolidate())
        }
    }

    fn proptest_left_join_index<K: DBData, V: DBData, OK: DBData, OV: DBData, It>(
        mut left_inputs: Vec<Vec<Tup2<K, Tup2<V, ZWeight>>>>,
        right_inputs: Vec<Vec<Tup2<K, Tup2<V, ZWeight>>>>,
        f: impl Fn(&K, &V, &Option<V>) -> It + Clone + Send + 'static,
        transaction: bool,
    ) where
        It: IntoIterator<Item = (OK, OV)> + 'static,
    {
        let (mut dbsp, (left_handle, right_handle, output_handle, expected_output_handle)) =
            Runtime::init_circuit(
                CircuitConfig::from(4).with_splitter_chunk_size_records(2),
                move |circuit| {
                    let (left_input, left_handle) = circuit.add_input_indexed_zset::<K, V>();
                    let (right_input, right_handle) =
                        circuit.add_input_indexed_zset::<K, Option<V>>();

                    let output_handle = left_input
                        .left_join_index(&right_input, f.clone())
                        .accumulate_output();

                    let f = f.clone();

                    let expected_output_handle = left_input
                        .reference_left_join_index(&right_input, f.clone())
                        .accumulate_output();

                    Ok((
                        left_handle,
                        right_handle,
                        output_handle,
                        expected_output_handle,
                    ))
                },
            )
            .unwrap();

        if transaction {
            dbsp.start_transaction().unwrap();
        }

        let mut right_inputs: Vec<Vec<Tup2<K, Tup2<Option<V>, ZWeight>>>> = right_inputs
            .into_iter()
            .map(|input| {
                input
                    .into_iter()
                    .map(|Tup2(k, Tup2(v, w))| Tup2(k, Tup2(Some(v), w)))
                    .collect()
            })
            .collect();

        for step in 0..left_inputs.len() {
            // println!(
            //     "step: left: {:?} right: {:?}",
            //     left_inputs[step], right_inputs[step]
            // );

            left_handle.append(&mut left_inputs[step]);
            right_handle.append(&mut right_inputs[step]);

            if !transaction {
                dbsp.transaction().unwrap();
                let output = output_handle.concat().consolidate();
                let expected_output = expected_output_handle.concat().consolidate();
                // println!(
                //     "output: {:?} expected_output: {:?}",
                //     output, expected_output
                // );
                assert_eq!(output, expected_output)
            } else {
                dbsp.step().unwrap();
            }
        }

        if transaction {
            dbsp.commit_transaction().unwrap();
            let output = output_handle.concat().consolidate();

            assert_eq!(output, expected_output_handle.concat().consolidate())
        }
    }

    proptest! {
        #[test]
        fn proptest_left_join_big_step(inputs in generate_join_test_data(10, 5, 3, 50)) {
            proptest_left_join(inputs.0, inputs.1, |_k, v1, v2| Tup2(*v1, *v2), true);
        }

        #[test]
        fn proptest_left_join_small_step(inputs in generate_join_test_data(10, 5, 3, 50)) {
            proptest_left_join(inputs.0, inputs.1, |_k, v1, v2| Tup2(*v1, *v2), false);
        }

        #[test]
        fn proptest_left_join_index_big_step(inputs in generate_join_test_data(10, 5, 1, 50)) {
            proptest_left_join_index(inputs.0, inputs.1, |k, v1, v2| vec![(*k, Tup2(*v1, *v2))], true);
        }

        #[test]
        fn proptest_left_join_index_small_step(inputs in generate_join_test_data(10, 5, 1, 50)) {
            proptest_left_join_index(inputs.0, inputs.1, |k, v1, v2| vec![(*k, Tup2(*v1, *v2))], false);
        }
    }
}
