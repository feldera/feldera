use feldera_types::config::StorageConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{
    utils::Tup2, DBData, OrdZSet, OutputHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use super::{dbsp_handle::Mode, CircuitConfig, CircuitStorageConfig};

fn init_logging() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .unwrap(),
        )
        .try_init();
}
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

impl_test_data!(TestData2, 0: T1, 1: T2);
impl_test_data!(TestData3, 0: T1, 1: T2, 2: T3);

struct TestData1<T1: DBData> {
    phantom: PhantomData<T1>,
}

impl TestDataType for () {
    type InputHandles = ();
    type OutputHandles = ();
    type Chunk = ();
    type ZSet = ();

    fn push_inputs(mut _chunks: Self::Chunk, _handles: &Self::InputHandles) {}

    fn read_outputs(_handles: &Self::OutputHandles) -> Self::ZSet {}
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
    circuit_constructor1: CircuitFn<I1, I2, O1, O2>,
    circuit_constructor2: CircuitFn<I2, I3, O2, O3>,
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
    init_logging();

    let mut circuit_config = CircuitConfig::with_workers(1).with_mode(Mode::Persistent);
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
        circuit.kill().unwrap();
        checkpoint
    };

    assert_eq!(reference_output1, actual_output1);

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

        circuit.kill().unwrap();
    }

    // Compare the outputs.
    assert_eq!(reference_output2, actual_output2);
    assert_eq!(reference_output3, actual_output3);
}

fn sequence(from: u64, to: u64) -> Vec<Vec<Tup2<u64, ZWeight>>> {
    (from..to).map(|x| vec![Tup2(x, 1)]).collect()
}

// Linear circuit without integrals where the old and the new circuits are disjoint.
// No state to checkpoint or replay.
fn linear_circuit1(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, (), OutputHandle<OrdZSet<u64>>, ()) {
    let (input_stream, input_handle) = circuit.add_input_zset::<u64>();
    let output_handle = input_stream
        .map(|x| x % 1000)
        .output_persistent(Some("output1"));

    (input_handle, (), output_handle, ())
}

fn linear_circuit2(
    circuit: &mut RootCircuit,
) -> ((), ZSetHandle<u64>, (), OutputHandle<OrdZSet<u64>>) {
    let (input_stream, input_handle) = circuit.add_input_zset::<u64>();
    let output_handle = input_stream
        .map(|x| x >> 1)
        .output_persistent(Some("output2"));

    ((), input_handle, (), output_handle)
}

#[test]
fn test_linear_circuit() {
    test_replay::<TestData1<u64>, (), TestData1<u64>, TestData1<u64>, (), TestData1<u64>>(
        Arc::new(linear_circuit1),
        Arc::new(linear_circuit2),
        sequence(0, 2).into_iter().map(|x| (x, ())).collect(),
        sequence(3, 5).into_iter().map(|x| ((), x)).collect(),
    );
}

// Linear circuit with materialized inputs:
//
// Pipeline 1:
//
// ---> input1 (materialized) --> map --> output1
// ---> input2 (materialized) --> map --> output2
//
// Pipeline 2:
// ---> input2 (materialized) --> map --> output2
// ---> input3 (materialized) --> map --> output3
//
// where input2 -> output2 is common between the two pipelines.
// No replay is needed on restart because output2 doesn't need backfill.

fn linear_circuit_materialized_inputs1(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    ZSetHandle<u64>,
    OutputHandle<OrdZSet<u64>>,
    OutputHandle<OrdZSet<u64>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    // These integrals will be used for replay input streams.
    input_stream1.integrate_trace();
    input_stream2.integrate_trace();

    let output_handle1 = input_stream1
        .map(|x| x % 1000)
        .output_persistent(Some("output1"));

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .output_persistent(Some("output2"));

    (input_handle1, input_handle2, output_handle1, output_handle2)
}

fn linear_circuit_materialized_inputs2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    ZSetHandle<u64>,
    OutputHandle<OrdZSet<u64>>,
    OutputHandle<OrdZSet<u64>>,
) {
    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    input_stream2.integrate_trace();
    input_stream3.integrate_trace();

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .output_persistent(Some("output2"));

    let output_handle3 = input_stream3
        .map(|x| x >> 1)
        .output_persistent(Some("output3"));

    (input_handle2, input_handle3, output_handle2, output_handle3)
}

#[test]
fn test_linear_circuit_materialized_inputs() {
    test_replay::<
        TestData1<u64>,
        TestData1<u64>,
        TestData1<u64>,
        TestData1<u64>,
        TestData1<u64>,
        TestData1<u64>,
    >(
        Arc::new(linear_circuit_materialized_inputs1),
        Arc::new(linear_circuit_materialized_inputs2),
        std::iter::zip(sequence(0, 2), sequence(0, 2)).collect(),
        std::iter::zip(sequence(3, 5), sequence(3, 5)).collect(),
    );
}

// Linear circuit with materialized inputs:
//
// Pipeline 1:
//
// ---> input1 (materialized) --> map --> output1
// ---> input2 (materialized) --> map --> output2
//
// Pipeline 2:
// ---> input2 (materialized) --> map --> output2_2
// ---> input3 (materialized) --> map --> output3
//
// where input2 is common between the two pipelines, but output2_2 has a different persistent id
// than output2 and therefore requires backfill.

fn linear_circuit_materialized_inputs1_2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    ZSetHandle<u64>,
    (OutputHandle<OrdZSet<u64>>, OutputHandle<OrdZSet<u64>>),
    (),
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    // These integrals will be used for replay input streams.
    input_stream1.integrate_trace();
    input_stream2.integrate_trace();

    let output_handle1 = input_stream1
        .map(|x| x % 1000)
        .output_persistent(Some("output1"));

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .output_persistent(Some("output2"));

    (
        input_handle1,
        input_handle2,
        (output_handle1, output_handle2),
        (),
    )
}

fn linear_circuit_materialized_inputs2_2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    ZSetHandle<u64>,
    (),
    (OutputHandle<OrdZSet<u64>>, OutputHandle<OrdZSet<u64>>),
) {
    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    input_stream2.integrate_trace();
    input_stream3.integrate_trace();

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .output_persistent(Some("output2_2"));

    let output_handle3 = input_stream3
        .map(|x| x >> 1)
        .output_persistent(Some("output3"));

    (
        input_handle2,
        input_handle3,
        (),
        (output_handle2, output_handle3),
    )
}

#[test]
fn test_linear_circuit_materialized_inputs_with_backfill() {
    test_replay::<
        TestData1<u64>,
        TestData1<u64>,
        TestData1<u64>,
        TestData2<u64, u64>,
        (),
        TestData2<u64, u64>,
    >(
        Arc::new(linear_circuit_materialized_inputs1_2),
        Arc::new(linear_circuit_materialized_inputs2_2),
        std::iter::zip(sequence(0, 2), sequence(0, 2)).collect(),
        std::iter::zip(sequence(3, 5), sequence(3, 5)).collect(),
    );
}

// Aggregate

// Join

// Recursion
