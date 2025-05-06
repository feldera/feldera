use feldera_types::config::StorageConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{
    operator::{
        time_series::{RelOffset, RelRange},
        Max, Min,
    },
    utils::{Tup2, Tup3},
    CmpFunc, DBData, OrdZSet, OutputHandle, RootCircuit, Runtime, Stream, ZSetHandle, ZWeight,
};
use std::{fmt::Debug, iter::repeat, marker::PhantomData, sync::Arc};

use super::{dbsp_handle::Mode, CircuitConfig, CircuitStorageConfig};

fn init_logging() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("debug"))
                .unwrap(),
        )
        .try_init();
}

/// A trait that defines 0, 1, or multiple input or output streams.
trait TestDataType {
    /// Input handles.
    type InputHandles: Send + 'static;

    /// Output handles.
    type OutputHandles: Send + 'static;

    /// Chunk of input data.
    type Chunk: Clone;

    /// Tuple of output ZSets.
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

/// Main test function, instantiated for each test case below.
///
/// Creates, runs, and checkpoints circuit 1:
///
/// ```text
///                      ┌─────┐
///    I1                │     │      O1
/// ────────────────────►│     ├────────────────►
///                      │     │
///          ┌──────────►│     │
///          │           └─────┘
///          │           ┌─────┐
///          │           │     │
///    I2    │           │     │      O2
/// ─────────┴──────────►│     ├────────────────►
///                      │     │
///                      └─────┘
/// ```
/// Then modifies the circuit into circuit 2, restarts it from the checkpoint
/// and feeds more data:
///
/// ```text
///                      ┌─────┐
///                      │     │
///    I2                │     │      O2
/// ─────────┬──────────►│     ├────────────────►
///          │           │     │
///          │           └─────┘
///          │           ┌─────┐
///          └──────────►│     │
///                      │     │      O3
///    I3                │     ├────────────────►
/// ────────────────────►│     │
///                      └─────┘
///```
///
/// The common part of the two circuits must return identical results.
fn test_replay<I1, I2, I3, O1, O2, O3>(
    circuit_constructor1: CircuitFn<I1, I2, O1, O2>,
    circuit_constructor2: CircuitFn<I2, I3, O2, O3>,
    inputs1: Vec<I1::Chunk>,
    inputs2_1: Vec<I2::Chunk>,
    inputs2_2: Vec<I2::Chunk>,
    inputs3: Vec<I3::Chunk>,
) where
    I1: TestDataType,
    I2: TestDataType,
    I3: TestDataType,
    O1: TestDataType,
    O2: TestDataType,
    O3: TestDataType,
{
    assert_eq!(inputs1.len(), inputs2_1.len());
    assert_eq!(inputs2_2.len(), inputs3.len());

    init_logging();

    let mut circuit_config = CircuitConfig::with_workers(4).with_mode(Mode::Persistent);
    let path = tempfile::tempdir().unwrap().into_path();
    println!("Running replay_test in {}", path.display());

    let config = StorageConfig {
        path: path.to_string_lossy().into_owned(),
        cache: Default::default(),
    };
    let options = Default::default();

    circuit_config.storage = Some(CircuitStorageConfig::for_config(config, options).unwrap());

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

        for (data1, data2) in std::iter::zip(&inputs1, &inputs2_1) {
            I1::push_inputs(data1.clone(), &input_handles1);
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.step().unwrap();

            reference_output1.push(O1::read_outputs(&output_handles1));
            reference_output2.push(O2::read_outputs(&output_handles2));
        }

        for data2 in inputs2_2.iter() {
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

        for data2 in inputs2_1.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.step().unwrap();

            reference_output2_2.push(O2::read_outputs(&output_handles2));
        }

        for (data2, data3) in std::iter::zip(&inputs2_2, &inputs3) {
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

        // Feed inputs1, inputs2_1.
        for (data1, data2) in std::iter::zip(inputs1, inputs2_1) {
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
        println!("Restarting circuit from checkpoint {}", checkpoint.uuid);

        // Restart the second circuit from the checkpoint.
        let mut circuit_config = circuit_config.clone();
        circuit_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);

        let circuit_constructor2_clone = circuit_constructor2.clone();

        let (mut circuit, (input_handles2, input_handles3, output_handles2, output_handles3)) =
            Runtime::init_circuit(&circuit_config, move |circuit| {
                Ok(circuit_constructor2_clone(circuit))
            })
            .unwrap();

        while circuit.bootstrap_in_progress() {
            circuit.step().unwrap();
        }
        println!("Replay finished");

        // Feed inputs2_2, inputs2.
        for (data2, data3) in std::iter::zip(&inputs2_2, &inputs3) {
            I2::push_inputs(data2.clone(), &input_handles2);
            I3::push_inputs(data3.clone(), &input_handles3);

            circuit.step().unwrap();

            actual_output2.push(O2::read_outputs(&output_handles2));
            actual_output3.push(O3::read_outputs(&output_handles3));
        }

        circuit.kill().unwrap();
    }

    // Compare the outputs.
    println!("Reference output1: {:?}", reference_output1);
    println!("Reference output3: {:?}", reference_output3);

    println!("Actual output1: {:?}", actual_output1);
    println!("Actual output3: {:?}", actual_output3);
    assert_eq!(reference_output2, actual_output2);
    assert_eq!(reference_output3, actual_output3);
}

fn sequence(from: u64, to: u64) -> Vec<Vec<Tup2<u64, ZWeight>>> {
    (from..to).map(|x| vec![Tup2(x, 1)]).collect()
}

fn chain(from: u64, to: u64) -> Vec<Vec<Tup2<Tup2<u64, u64>, ZWeight>>> {
    (from..to).map(|x| vec![Tup2(Tup2(x, x + 1), 1)]).collect()
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
        sequence(0, 2),
        repeat(()).take(2).collect(),
        repeat(()).take(2).collect(),
        sequence(3, 5),
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

    // These integrals will be used to replay input streams.
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
        sequence(0, 2),
        sequence(0, 2),
        sequence(3, 5),
        sequence(3, 5),
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
        sequence(0, 2),
        sequence(0, 2),
        sequence(3, 5),
        sequence(3, 5),
    );
}

// Circuit with joins:
//
// Pipeline 1:
//
// ---> input1 ---> join --> output1
//                   ^
// ---> input2 ------|
//
// Pipeline 2 (adds another join):
//
// ---> input1 ---> join --> output1
//                   ^
// ---> input2 ------|
//                   v
// ---> input3 ---> join --> output2
//
fn join_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (),
    OutputHandle<OrdZSet<u64>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    // These integrals will be used for replay input streams.
    input_stream1.integrate_trace();
    input_stream2.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));
    let input_stream2_indexed = input_stream2
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream2_indexed"));

    let output_handle1 = input_stream1_indexed
        .join(&input_stream2_indexed, |key, _v1, _v2| *key)
        .output_persistent(Some("output1"));

    ((), (input_handle1, input_handle2), (), output_handle1)
}

fn join_circuit2(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    ZSetHandle<u64>,
    OutputHandle<OrdZSet<u64>>,
    OutputHandle<OrdZSet<u64>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    // These integrals will be used for replay input streams.
    input_stream1.integrate_trace();
    input_stream2.integrate_trace();
    input_stream3.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));
    let input_stream2_indexed = input_stream2
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream2_indexed"));
    let input_stream3_indexed = input_stream3
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream3_indexed"));

    let output_handle1 = input_stream1_indexed
        .join(&input_stream2_indexed, |key, _v1, _v2| *key)
        .output_persistent(Some("output1"));

    let output_handle2 = input_stream2_indexed
        .join(&input_stream3_indexed, |key, _v1, _v2| *key)
        .output_persistent(Some("output2"));

    (
        (input_handle1, input_handle2),
        input_handle3,
        output_handle1,
        output_handle2,
    )
}

#[test]
fn test_join_circuit() {
    test_replay::<(), TestData2<u64, u64>, TestData1<u64>, (), TestData1<u64>, TestData1<u64>>(
        Arc::new(join_circuit1),
        Arc::new(join_circuit2),
        repeat(()).take(2).collect(),
        std::iter::zip(sequence(0, 2), sequence(2, 4)).collect(),
        std::iter::zip(sequence(2, 4), sequence(0, 2)).collect(),
        sequence(1, 3),
    );
}

// Circuit with aggregates:
//
// Pipeline 1:
//
// ---> input1 ---> aggregate1 --> output1
//
// Pipeline 2: adds two more aggregates, one chained after the existing aggregate, and one over the input stream:
//
//                      /--------> output1
// ---> input1 ---> aggregate1 --> aggregate2 -> output2
//        |-------> aggregate3 --> output3
//
fn aggregate_circuit1(
    circuit: &mut RootCircuit,
) -> ((), ZSetHandle<u64>, (), OutputHandle<OrdZSet<u64>>) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (x % 2, *x))
        .set_persistent_id(Some("input_stream1_indexed"));

    let aggregate1 = input_stream1_indexed
        .aggregate_persistent(Some("aggregate1"), Max)
        .set_persistent_id(Some("aggregate1"));

    let aggregate1_flat = aggregate1
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("aggregate1_flat"));

    let output_handle1 = aggregate1_flat.output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn aggregate_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<OrdZSet<u64>>,
    (OutputHandle<OrdZSet<u64>>, OutputHandle<OrdZSet<u64>>),
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (x % 2, *x))
        .set_persistent_id(Some("input_stream1_indexed"));

    let aggregate1 = input_stream1_indexed
        .aggregate_persistent(Some("aggregate1"), Max)
        .set_persistent_id(Some("aggregate1"));

    let aggregate1_flat = aggregate1
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("aggregate1_flat"));

    let aggregate2 = aggregate1
        .aggregate_persistent(Some("aggregate2"), Min)
        .set_persistent_id(Some("aggregate2"));

    let aggregate2_flat = aggregate2
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("aggregate2_flat"));

    let aggregate3 = input_stream1_indexed
        .aggregate_persistent(Some("aggregate3"), Min)
        .set_persistent_id(Some("aggregate3"));

    let aggregate3_flat = aggregate3
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("aggregate3_flat"));

    let output_handle1 = aggregate1_flat.output_persistent(Some("output1"));
    let output_handle2 = aggregate2_flat.output_persistent(Some("output2"));
    let output_handle3 = aggregate3_flat.output_persistent(Some("output3"));

    (
        input_handle1,
        (),
        output_handle1,
        (output_handle2, output_handle3),
    )
}

#[test]
fn test_aggregate_circuit() {
    test_replay::<(), TestData1<u64>, (), (), TestData1<u64>, TestData2<u64, u64>>(
        Arc::new(aggregate_circuit1),
        Arc::new(aggregate_circuit2),
        repeat(()).take(5).collect(),
        sequence(0, 5),
        sequence(5, 10),
        repeat(()).take(5).collect(),
    );
}

// Circuit with recursion1:
//
// Pipeline 1:
//
// ---> input1 ---> recusrive subcircuit --> output1
//
// Pipeline 2: Connect a new output to the recursive subcircuit. This should require
// clearing the state of the cubcircuit, since it cannot be used to backfill
//
// ---> input1 ---> recusrive subcircuit --> output1
//                                      \--> output2
//
fn recursive_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    ZSetHandle<Tup2<u64, u64>>,
    (),
    OutputHandle<OrdZSet<Tup2<u64, u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<Tup2<u64, u64>>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let paths = circuit
        .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
            let edges = input_stream1.delta0(child);

            let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
            let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

            Ok(edges.plus(&paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to))))
        })
        .unwrap();

    let output_handle1 = paths.output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn recursive_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<Tup2<u64, u64>>,
    (),
    OutputHandle<OrdZSet<Tup2<u64, u64>>>,
    OutputHandle<OrdZSet<Tup2<u64, u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<Tup2<u64, u64>>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let paths = circuit
        .recursive(|child, paths: Stream<_, OrdZSet<Tup2<u64, u64>>>| {
            let edges = input_stream1.delta0(child);

            let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
            let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

            Ok(edges.plus(&paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to))))
        })
        .unwrap();

    let output_handle1 = paths.output_persistent(Some("output1"));
    let output_handle2 = paths.output_persistent(Some("output2"));

    (input_handle1, (), output_handle1, output_handle2)
}

#[test]
fn test_recursive_circuit1() {
    test_replay::<
        (),
        TestData1<Tup2<u64, u64>>,
        (),
        (),
        TestData1<Tup2<u64, u64>>,
        TestData1<Tup2<u64, u64>>,
    >(
        Arc::new(recursive_circuit1),
        Arc::new(recursive_circuit2),
        repeat(()).take(5).collect(),
        chain(0, 5),
        chain(5, 10),
        repeat(()).take(5).collect(),
    );
}

// Circuit with lag:
//
// Pipeline 1:
//
// ---> input1 ---> lag1 --> output1
//
// Pipeline 2: adds two more lag operators, one chained after the existing lag, and one over the input stream:
//
//                      /--------> output1
// ---> input1 ---> lag1 --> lag2 -> output2
//        |-------> lag3 --> output3
//

struct Asc<T>(PhantomData<T>);

impl<T: DBData> CmpFunc<T> for Asc<T> {
    fn cmp(left: &T, right: &T) -> std::cmp::Ordering {
        left.cmp(right)
    }
}

fn lag_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    ZSetHandle<u64>,
    (),
    OutputHandle<OrdZSet<Tup2<u64, u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (x % 3, *x))
        .set_persistent_id(Some("input_stream1_indexed"));

    let lag1 = input_stream1_indexed
        .lag_custom_order_persistent::<_, _, _, Asc<_>, _>(
            Some("lag1"),
            1,
            |x| x.cloned().unwrap_or(0),
            |x, y| Tup2(*x, *y),
        )
        .set_persistent_id(Some("lag1"));

    let lag1_flat = lag1.map(|(_k, v)| *v).set_persistent_id(Some("lag1_flat"));

    let output_handle1 = lag1_flat.output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn lag_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<OrdZSet<Tup2<u64, u64>>>,
    (
        OutputHandle<OrdZSet<Tup3<u64, u64, u64>>>,
        OutputHandle<OrdZSet<Tup2<u64, u64>>>,
    ),
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (x % 3, *x))
        .set_persistent_id(Some("input_stream1_indexed"));

    let lag1 = input_stream1_indexed
        .lag_custom_order_persistent::<_, _, _, Asc<_>, _>(
            Some("lag1"),
            1,
            |x| x.cloned().unwrap_or(0),
            |x, y| Tup2(*x, *y),
        )
        .set_persistent_id(Some("lag1"));

    let lag1_flat = lag1.map(|(_k, v)| *v).set_persistent_id(Some("lag1_flat"));

    let lag2 = lag1
        .lag_custom_order_persistent::<_, _, _, Asc<_>, _>(
            Some("lag2"),
            1,
            |x| x.cloned().unwrap_or(Tup2(0, 0)).1,
            |Tup2(x, y), z| Tup3(*x, *y, *z),
        )
        .set_persistent_id(Some("lag2"));

    let lag2_flat = lag2.map(|(_k, v)| *v).set_persistent_id(Some("lag2_flat"));

    let lag3 = input_stream1_indexed
        .lag_custom_order_persistent::<_, _, _, Asc<_>, _>(
            Some("lag3"),
            1,
            |x| x.cloned().unwrap_or(0),
            |x, y| Tup2(*x, *y),
        )
        .set_persistent_id(Some("lag3"));

    let lag3_flat = lag3.map(|(_k, v)| *v).set_persistent_id(Some("lag3_flat"));

    let output_handle1 = lag1_flat.output_persistent(Some("output1"));
    let output_handle2 = lag2_flat.output_persistent(Some("output2"));
    let output_handle3 = lag3_flat.output_persistent(Some("output3"));

    (
        input_handle1,
        (),
        output_handle1,
        (output_handle2, output_handle3),
    )
}

#[test]
fn test_lag_circuit() {
    test_replay::<
        (),
        TestData1<u64>,
        (),
        (),
        TestData1<Tup2<u64, u64>>,
        TestData2<Tup3<u64, u64, u64>, Tup2<u64, u64>>,
    >(
        Arc::new(lag_circuit1),
        Arc::new(lag_circuit2),
        repeat(()).take(10).collect(),
        sequence(0, 10),
        sequence(10, 20),
        repeat(()).take(10).collect(),
    );
}

// Circuit with rolling aggregates:
//
// Pipeline 1:
//           /--------> rolling1 -> output1
// ---> input1 ---> rolling2 --> output2
//
// Pipeline 2:
//                      /--------> output2
// ---> input1 ---> rolling2 --> rolling3 -> output3
//        |-------> rolling4 --> output4
//

fn rolling_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    ZSetHandle<u64>,
    OutputHandle<OrdZSet<Tup2<u64, Option<u64>>>>,
    OutputHandle<OrdZSet<Tup2<u64, Option<u64>>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));

    let rolling1 = input_stream1_indexed
        .partitioned_rolling_aggregate_persistent(
            Some("rolling1"),
            |x| (x % 3, *x),
            Min,
            RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
        )
        .set_persistent_id(Some("rolling1"));

    let rolling1_flat = rolling1
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("rolling1_flat"));

    let output_handle1 = rolling1_flat.output_persistent(Some("output1"));

    let rolling2 = input_stream1_indexed
        .partitioned_rolling_aggregate_persistent(
            Some("rolling2"),
            |x| (x % 5, *x),
            Min,
            RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
        )
        .set_persistent_id(Some("rolling2"));

    let rolling2_flat = rolling2
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("rolling2_flat"));

    let output_handle2 = rolling2_flat.output_persistent(Some("output2"));

    ((), input_handle1, output_handle1, output_handle2)
}

fn rolling_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<OrdZSet<Tup2<u64, Option<u64>>>>,
    (
        OutputHandle<OrdZSet<Tup2<u64, Option<u64>>>>,
        OutputHandle<OrdZSet<Tup2<u64, Option<u64>>>>,
    ),
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));

    let rolling2 = input_stream1_indexed
        .partitioned_rolling_aggregate_persistent(
            Some("rolling2"),
            |x| (x % 5, *x),
            Min,
            RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
        )
        .set_persistent_id(Some("rolling2"));

    let rolling2_flat = rolling2
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("rolling2_flat"));

    let output_handle2 = rolling2_flat.output_persistent(Some("output2"));

    let rolling3 = rolling2_flat
        .map_index(|Tup2(x, y)| (*x, Tup2(*x, *y)))
        .partitioned_rolling_aggregate_persistent(
            Some("rolling3"),
            |x| (x.0 % 7, x.0),
            Min,
            RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
        )
        .set_persistent_id(Some("rolling3"));

    let rolling3_flat = rolling3
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("rolling3_flat"));

    let output_handle3 = rolling3_flat.output_persistent(Some("output3"));

    let rolling4 = input_stream1_indexed
        .partitioned_rolling_aggregate_persistent(
            Some("rolling4"),
            |x| (x % 2, *x),
            Min,
            RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
        )
        .set_persistent_id(Some("rolling4"));

    let rolling4_flat = rolling4
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .set_persistent_id(Some("rolling4_flat"));

    let output_handle4 = rolling4_flat.output_persistent(Some("output4"));

    (
        input_handle1,
        (),
        output_handle2,
        (output_handle3, output_handle4),
    )
}

#[test]
fn test_rolling_circuit() {
    test_replay::<
        (),
        TestData1<u64>,
        (),
        TestData1<Tup2<u64, Option<u64>>>,
        TestData1<Tup2<u64, Option<u64>>>,
        TestData2<Tup2<u64, Option<u64>>, Tup2<u64, Option<u64>>>,
    >(
        Arc::new(rolling_circuit1),
        Arc::new(rolling_circuit2),
        repeat(()).take(20).collect(),
        sequence(0, 20),
        sequence(20, 40),
        repeat(()).take(20).collect(),
    );
}
