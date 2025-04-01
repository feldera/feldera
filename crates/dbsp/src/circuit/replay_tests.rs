use feldera_types::config::StorageConfig;

use crate::{
    utils::Tup2, DBData, OrdZSet, OutputHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use super::{dbsp_handle::Mode, CircuitConfig, CircuitStorageConfig};

trait TestDataType {
    type InputHandles: Send + 'static;
    type OutputHandles: Send + 'static;
    type Chunk: Clone;
    type ZSet: Debug + PartialEq + Eq;

    fn push_inputs(chunks: Self::Chunk, handles: &Self::InputHandles);
    fn read_outputs(handles: &Self::OutputHandles) -> Self::ZSet;
}

macro_rules! impl_test_data {
    ($tname:ident, $($name:tt: $t:ident),*) => {
        struct $tname<$($t: DBData),*> {
            phantom: PhantomData<($($t),*)>,
        }

        impl<$($t: DBData),*> $tname<$($t),*> {
            fn new() -> Self {
                Self {
                    phantom: PhantomData,
                }
            }
        }

        impl<$($t),*> TestDataType for $tname<$($t),*>
        where
            $($t: DBData),*
        {
            type InputHandles = ($(ZSetHandle<$t>),*);
            type OutputHandles = ($(OutputHandle<OrdZSet<$t>>),*);
            type Chunk = ($(Vec<Tup2<$t, ZWeight>>),*);
            type ZSet = ($(OrdZSet<$t>),*);

            fn push_inputs(mut chunks: Self::Chunk, handles: &Self::InputHandles) {
                $(
                    handles.$name.append(&mut chunks.$name);
                )*
            }

            fn read_outputs(handles: &Self::OutputHandles) -> Self::ZSet {
                ($(handles.$name.consolidate()),*)
            }
        }
    };
}

//impl_test_data!(TestData1, 0: T1);
impl_test_data!(TestData2, 0: T1, 1: T2);
impl_test_data!(TestData3, 0: T1, 1: T2, 2: T3);

struct TestData1<T1: DBData> {
    phantom: PhantomData<T1>,
}

impl<T1: DBData> TestData1<T1> {
    fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

impl<T1> TestDataType for TestData1<T1>
where
    T1: DBData,
{
    type InputHandles = ZSetHandle<T1>;
    type OutputHandles = OutputHandle<OrdZSet<T1>>;
    type Chunk = Vec<Tup2<T1, ZWeight>>;
    type ZSet = OrdZSet<T1>;

    fn push_inputs(mut chunks: Self::Chunk, handles: &Self::InputHandles) {
        handles.append(&mut chunks);
    }

    fn read_outputs(handles: &Self::OutputHandles) -> Self::ZSet {
        handles.consolidate()
    }
}

type CircuitFn<I1, I2, O1, O2> = Arc<
    dyn Fn(
            &mut RootCircuit,
        ) -> (
            <I1 as TestDataType>::InputHandles,
            <I2 as TestDataType>::InputHandles,
            <O1 as TestDataType>::OutputHandles,
            <O2 as TestDataType>::OutputHandles,
        ) + Send
        + Sync,
>;

fn test_replay<I1, I2, I3, O1, O2, O3>(
    circuit_constructor1: &CircuitFn<I1, I2, O1, O2>,
    circuit_constructor2: &CircuitFn<I2, I3, O2, O3>,
    inputs1: Vec<(I1::Chunk, I2::Chunk)>,
    inputs2: Vec<(I2::Chunk, I3::Chunk)>,
) where
    I1: TestDataType,
    I2: TestDataType,
    I3: TestDataType,
    O1: TestDataType,
    O2: TestDataType,
    O3: TestDataType,
{
    let mut circuit_config = CircuitConfig::with_workers(4).with_mode(Mode::Persistent);
    let path = tempfile::tempdir().unwrap().into_path();

    circuit_config.storage = Some(CircuitStorageConfig {
        config: StorageConfig {
            path: path.to_string_lossy().into_owned(),
            cache: Default::default(),
        },
        options: Default::default(),
        init_checkpoint: None,
    });

    // Create both reference circuits, feed I1 and I2 to circuit1; feed I2 and I3 to circuit2.
    let mut reference_output1 = Vec::new();
    let mut reference_output2 = Vec::new();
    let mut reference_output2_2 = Vec::new();
    let mut reference_output3 = Vec::new();

    {
        let circuit_constructor1_clone = circuit_constructor1.clone();
        let (mut circuit, (input_handles1, input_handles2, output_handles1, output_handles2)) =
            Runtime::init_circuit(&circuit_config, move |circuit| {
                Ok(circuit_constructor1_clone(circuit))
            })
            .unwrap();

        for (data1, data2) in inputs1.iter() {
            I1::push_inputs(data1.clone(), &input_handles1);
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.step().unwrap();

            reference_output1.push(O1::read_outputs(&output_handles1));
            reference_output2.push(O2::read_outputs(&output_handles2));
        }

        for (data2, _data3) in inputs2.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.step().unwrap();

            reference_output2.push(O2::read_outputs(&output_handles2));
        }

        circuit.kill().unwrap();

        let circuit_constructor2_clone = circuit_constructor2.clone();
        let (mut circuit, (input_handles2, input_handles3, output_handles2, output_handles3)) =
            Runtime::init_circuit(&circuit_config, move |circuit| {
                Ok(circuit_constructor2_clone(circuit))
            })
            .unwrap();

        for (_data1, data2) in inputs1.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.step().unwrap();

            reference_output2_2.push(O2::read_outputs(&output_handles2));
        }

        for (data2, data3) in inputs2.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);
            I3::push_inputs(data3.clone(), &input_handles3);

            circuit.step().unwrap();

            reference_output2_2.push(O2::read_outputs(&output_handles2));
            reference_output3.push(O3::read_outputs(&output_handles3));
        }

        circuit.kill().unwrap();

        // The common part of the two circuits must return identical results.
        assert_eq!(reference_output2, reference_output2_2);
    }

    let mut actual_output1 = Vec::new();
    let mut actual_output2 = Vec::new();
    let mut actual_output3 = Vec::new();

    let checkpoint = {
        // Create the first circuit.
        let circuit_constructor1_clone = circuit_constructor1.clone();

        let (mut circuit, (input_handles1, input_handles2, output_handles1, output_handles2)) =
            Runtime::init_circuit(&circuit_config, move |circuit| {
                Ok(circuit_constructor1_clone(circuit))
            })
            .unwrap();

        // Feed inputs1.
        for (data1, data2) in inputs1.iter() {
            I1::push_inputs(data1.clone(), &input_handles1);
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.step().unwrap();

            actual_output1.push(O1::read_outputs(&output_handles1));
            actual_output2.push(O2::read_outputs(&output_handles2));
        }

        // Checkpoint.
        let checkpoint = circuit.commit().unwrap();
        circuit.kill();
        checkpoint
    };

    {
        // Restart the second circuit from the checkpoint.
        let mut circuit_config = circuit_config.clone();
        circuit_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid.clone());

        let circuit_constructor2_clone = circuit_constructor2.clone();

        let (mut circuit, (input_handles2, input_handles3, output_handles2, output_handles3)) =
            Runtime::init_circuit(&circuit_config, move |circuit| {
                Ok(circuit_constructor2_clone(circuit))
            })
            .unwrap();

        // Feed inputs2.
        for (data2, data3) in inputs2.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);
            I3::push_inputs(data3.clone(), &input_handles3);

            circuit.step().unwrap();

            actual_output2.push(O2::read_outputs(&output_handles2));
            actual_output3.push(O3::read_outputs(&output_handles3));
        }

        circuit.kill();
    }

    // Compare the outputs.
}

// Linear circuit without integrals where the old and the new circuits are disjoint.
// No state to checkpoint or replay.
fn linear_circuit1(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, (), OutputHandle<OrdZSet<u64>>, ()) {
    let (input_stream, input_handle) = circuit.add_input_zset();
}

fn linear_circuit2(
    circuit: &mut RootCircuit,
) -> ((), ZSetHandle<u64>, (), OutputHandle<OrdZSet<u64>>) {
    todo!()
}

// Linear circuit with materialized inputs.

// Aggregate

// Join

// Recursion
