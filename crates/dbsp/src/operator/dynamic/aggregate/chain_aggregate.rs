use std::{borrow::Cow, cmp::Ordering, marker::PhantomData};

use dyn_clone::clone_box;
use minitrace::trace;

use crate::{
    algebra::{IndexedZSet, ZBatchReader},
    circuit::operator_traits::{BinaryOperator, Operator},
    dynamic::{ClonableTrait, DynData, Erase},
    operator::dynamic::{
        accumulate_trace::AccumulateTraceFeedback, trace::TraceBounds, MonoIndexedZSet,
    },
    trace::{BatchReader, BatchReaderFactories, Builder, Cursor, Spine},
    Circuit, RootCircuit, Scope, Stream, ZWeight,
};

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_chain_aggregate_mono(
        &self,
        persistent_id: Option<&str>,
        input_factories: &<MonoIndexedZSet as BatchReader>::Factories,
        output_factories: &<MonoIndexedZSet as BatchReader>::Factories,
        finit: Box<dyn Fn(&mut DynData, &DynData, ZWeight)>,
        fupdate: Box<dyn Fn(&mut DynData, &DynData, ZWeight)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_chain_aggregate(
            persistent_id,
            input_factories,
            output_factories,
            finit,
            fupdate,
        )
    }
}

impl<Z> Stream<RootCircuit, Z>
where
    Z: IndexedZSet,
{
    /// See [Stream::chain_aggregate].
    pub fn dyn_chain_aggregate<OZ>(
        &self,
        persistent_id: Option<&str>,
        input_factories: &Z::Factories,
        output_factories: &OZ::Factories,
        finit: Box<dyn Fn(&mut OZ::Val, &Z::Val, ZWeight)>,
        fupdate: Box<dyn Fn(&mut OZ::Val, &Z::Val, ZWeight)>,
    ) -> Stream<RootCircuit, OZ>
    where
        OZ: IndexedZSet<Key = Z::Key>,
    {
        // ```
        //  stream ┌──────────────┐   output   ┌──────────────────┐
        // ───────►│ChainAggregate├────────────┤UntimedTraceAppend├───┐
        //         └──────────────┘            └──────────────────┘   │
        //                ▲                          ▲                │
        //                │        delayed_trace   ┌─┴──┐             │
        //                └────────────────────────┤Z^-1│◄────────────┘
        //                                         └────┘
        // ```

        let circuit = self.circuit();
        let stream = self.dyn_shard(input_factories);

        let bounds = TraceBounds::unbounded();

        let feedback = circuit.add_accumulate_integrate_trace_feedback::<Spine<OZ>>(
            persistent_id,
            output_factories,
            bounds,
        );

        let output = circuit
            .add_binary_operator(
                ChainAggregate::new(output_factories, finit, fupdate),
                &stream.dyn_accumulate(input_factories),
                &feedback.delayed_trace,
            )
            .mark_sharded();

        feedback.connect(&output, output_factories);

        output
    }
}

struct ChainAggregate<Z, OZ>
where
    Z: ZBatchReader<Time = ()>,
    OZ: IndexedZSet,
{
    output_factories: OZ::Factories,
    finit: Box<dyn Fn(&mut OZ::Val, &Z::Val, ZWeight)>,
    fupdate: Box<dyn Fn(&mut OZ::Val, &Z::Val, ZWeight)>,
    _phantom: PhantomData<(Z, OZ)>,
}

impl<Z, OZ> ChainAggregate<Z, OZ>
where
    Z: ZBatchReader<Time = ()>,
    OZ: IndexedZSet,
{
    fn new(
        output_factories: &OZ::Factories,
        finit: Box<dyn Fn(&mut OZ::Val, &Z::Val, ZWeight)>,
        fupdate: Box<dyn Fn(&mut OZ::Val, &Z::Val, ZWeight)>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            finit,
            fupdate,
            _phantom: PhantomData,
        }
    }
}

impl<Z, OZ> Operator for ChainAggregate<Z, OZ>
where
    Z: ZBatchReader<Time = ()>,
    OZ: IndexedZSet,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ChainAggregate")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<Z, OZ> BinaryOperator<Option<Z>, Spine<OZ>, OZ> for ChainAggregate<Z, OZ>
where
    Z: ZBatchReader<Time = ()>,
    OZ: IndexedZSet<Key = Z::Key>,
{
    #[trace]
    async fn eval(&mut self, delta: &Option<Z>, output_trace: &Spine<OZ>) -> OZ {
        let Some(delta) = delta else {
            return OZ::dyn_empty(&self.output_factories);
        };

        let mut delta_cursor = delta.cursor();
        let mut output_trace_cursor = output_trace.cursor();

        let mut builder = OZ::Builder::with_capacity(&self.output_factories, delta.len());

        let mut old = self.output_factories.val_factory().default_box();
        let mut new = self.output_factories.val_factory().default_box();

        while delta_cursor.key_valid() {
            let mut key = clone_box(delta_cursor.key());
            let mut retract = false;

            // Read the current value of the aggregate, be careful to skip entries with weight 0.
            if output_trace_cursor.seek_key_exact(&key) {
                debug_assert!(
                    output_trace_cursor.val_valid() && **output_trace_cursor.weight() != 0
                );

                output_trace_cursor.val().clone_to(&mut old);
                retract = true;
            };

            // If there is an existing value, start from it, otherwise start from finit().
            if retract {
                old.clone_to(&mut new);
            } else {
                let w = **delta_cursor.weight();
                (self.finit)(&mut new, delta_cursor.val(), w);
                delta_cursor.step_val();
            }

            // Iterate over new values.
            while delta_cursor.val_valid() {
                let w = **delta_cursor.weight();
                (self.fupdate)(&mut new, delta_cursor.val(), w);
                delta_cursor.step_val();
            }

            // Apply retraction and insertion in correct order.
            if retract {
                match new.cmp(&old) {
                    Ordering::Less => {
                        builder.push_val_diff_mut(&mut new, (1 as ZWeight).erase_mut());
                        builder.push_val_diff_mut(&mut old, (-1 as ZWeight).erase_mut());
                        builder.push_key_mut(&mut key);
                    }
                    Ordering::Greater => {
                        builder.push_val_diff_mut(&mut old, (-1 as ZWeight).erase_mut());
                        builder.push_val_diff_mut(&mut new, (1 as ZWeight).erase_mut());
                        builder.push_key_mut(&mut key);
                    }
                    _ => (),
                }
                // do nothing if new == old.
            } else {
                builder.push_val_diff_mut(&mut new, 1.erase_mut());
                builder.push_key_mut(&mut key);
            }

            delta_cursor.step_key();
        }
        builder.done()
    }
}

#[cfg(test)]
mod test {
    use std::cmp::{max, min};

    use crate::{
        circuit::CircuitConfig, operator::Min, typed_batch::SpineSnapshot, utils::Tup2, zset,
        OrdIndexedZSet, OutputHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
    };
    use proptest::{collection, prelude::*};

    fn min_test_circuit(
        circuit: &mut RootCircuit,
    ) -> (
        ZSetHandle<Tup2<u64, i32>>,
        OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, String>>>,
        OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, String>>>,
    ) {
        let (input, input_handle) = circuit.add_input_zset::<Tup2<u64, i32>>();

        let input_indexed = input.map_index(|Tup2(x, y)| (*x, *y));
        let aggregate = input_indexed
            .aggregate(Min)
            .map_index(|(x, y)| (*x, y.to_string()))
            .accumulate_output();
        let delta_aggregate = input_indexed
            .chain_aggregate(|v, _w| *v, |acc, i, _w| min(acc, *i))
            .map_index(|(x, y)| (*x, y.to_string()))
            .accumulate_output();

        (input_handle, aggregate, delta_aggregate)
    }

    pub fn test_input(
        num_keys: u64,
        max_val: i32,
        max_tuples: usize,
    ) -> impl Strategy<Value = Vec<Tup2<Tup2<u64, i32>, ZWeight>>> {
        collection::vec(
            (
                (0..num_keys, -max_val..max_val).prop_map(|(x, y)| Tup2(x, y)),
                1..=2i64,
            )
                .prop_map(|(x, y)| Tup2(x, y)),
            0..max_tuples,
        )
    }

    pub fn test_inputs() -> impl Strategy<Value = Vec<Vec<Tup2<Tup2<u64, i32>, ZWeight>>>> {
        collection::vec(test_input(20, 1000, 30), 0..10)
    }

    proptest! {
        #[test]
        fn chain_aggregate_min_test(inputs in test_inputs()) {
            let (mut dbsp, (input, aggregate, delta_aggregate)) =
                Runtime::init_circuit(CircuitConfig::with_workers(4), |circuit| {
                    Ok(min_test_circuit(circuit))
                })
                .unwrap();

            for mut batch in inputs.into_iter() {
                input.append(&mut batch);
                dbsp.transaction().unwrap();
                assert_eq!(SpineSnapshot::<OrdIndexedZSet<u64, String>>::concat(&aggregate.take_from_all()).iter().collect::<Vec<_>>(), SpineSnapshot::<OrdIndexedZSet<u64, String>>::concat(&delta_aggregate.take_from_all()).iter().collect::<Vec<_>>());
            }
        }
    }

    /// NOW() clock implemented as a chain aggregate, as in the SQL compiler.
    /// This design is robust against clock going back, clock input missing in some steps (e.g., in the middle of a transaction),
    /// and multiple clock updates in a single step.
    #[test]
    fn clock_test() {
        let (mut dbsp, (clock_handle, busy_input_handle, now_handle)) = Runtime::init_circuit(
            CircuitConfig::with_workers(4).with_splitter_chunk_size_records(1),
            |circuit| {
                let (clock_stream, clock_handle) = circuit.add_input_zset::<u64>();

                // Feeding >1 values to this stream will force transaction commit to take multiple steps.
                let (busy_stream, busy_input_handle) = circuit.add_input_zset::<u64>();
                busy_stream.map_index(|x| (*x, *x)).distinct();

                let now = clock_stream
                    .map_index(|x| ((), *x))
                    .chain_aggregate(|v, _w| *v, |acc, i, _w| max(acc, *i));

                let now_handle = now.map(|(_, x)| *x).accumulate_output();
                Ok((clock_handle, busy_input_handle, now_handle))
            },
        )
        .unwrap();

        // Feed initial clock value.
        clock_handle.append(&mut vec![Tup2(10u64, 1)]);
        dbsp.transaction().unwrap();
        assert_eq!(now_handle.concat().consolidate(), zset! { 10 => 1 });

        // Clock goes back - keep the old value.
        clock_handle.append(&mut vec![Tup2(5u64, 1)]);
        dbsp.transaction().unwrap();
        assert_eq!(now_handle.concat().consolidate(), zset! {});

        // Multiple clock updates - only the bigger value has effect.
        clock_handle.append(&mut vec![Tup2(15u64, 1), Tup2(20u64, 1)]);
        busy_input_handle.append(&mut (0..100).map(|i| Tup2(i, 1)).collect::<Vec<_>>());

        dbsp.transaction().unwrap();
        assert_eq!(
            now_handle.concat().consolidate(),
            zset! { 10 => -1, 20 => 1 }
        );

        // Miss a clock tick
        dbsp.transaction().unwrap();
        assert_eq!(now_handle.concat().consolidate(), zset! {});

        // Transaction
        dbsp.start_transaction().unwrap();
        busy_input_handle.append(&mut (100..200).map(|i| Tup2(i, 1)).collect::<Vec<_>>());

        for i in 20..30 {
            clock_handle.append(&mut vec![Tup2(i as u64, 1)]);
            dbsp.step().unwrap();
            assert_eq!(now_handle.concat().consolidate(), zset! {});
        }

        dbsp.commit_transaction().unwrap();
        assert_eq!(now_handle.concat().consolidate(), zset! {20 => -1, 29 => 1});

        dbsp.kill().unwrap();
    }
}
