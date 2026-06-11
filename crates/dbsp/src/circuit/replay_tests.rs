use feldera_types::config::StorageConfig;

use crate::{
    CmpFunc, DBData, IndexedZSetHandle, OrdIndexedZSet, OrdZSet, OutputHandle, RootCircuit,
    Runtime, Stream, ZSetHandle, ZWeight,
    circuit::dbsp_handle::CircuitStorageConfig,
    default_hash, indexed_zset,
    operator::{
        Max, Min,
        time_series::{RelOffset, RelRange},
    },
    typed_batch::SpineSnapshot,
    utils::{Tup2, Tup3, Tup4, test::init_test_logger},
    zset,
};
use std::{cmp::Ordering, fmt::Debug, marker::PhantomData, path::PathBuf, sync::Arc};

use super::{CircuitConfig, dbsp_handle::Mode};

const NUM_WORKERS: usize = 4;

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
            type OutputHandles = ($(OutputHandle<SpineSnapshot<OrdZSet<$t>>>),*);
            type Chunk = ($(Vec<Tup2<$t, ZWeight>>),*);
            type ZSet = ($(OrdZSet<$t>),*);

            fn push_inputs(mut chunks: Self::Chunk, handles: &Self::InputHandles) {
                $(
                    handles.$name.append(&mut chunks.$name);
                )*
            }

            fn read_outputs(handles: &Self::OutputHandles) -> Self::ZSet {
                ($(SpineSnapshot::<OrdZSet<$t>>::concat(&handles.$name.take_from_all()).consolidate()),*)
            }
        }
    };
}

impl_test_data!(TestData2, 0: T1, 1: T2);
impl_test_data!(TestData3, 0: T1, 1: T2, 2: T3);
impl_test_data!(TestData4, 0: T1, 1: T2, 2: T3, 3: T4);

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
    type OutputHandles = OutputHandle<SpineSnapshot<OrdZSet<T1>>>;
    type Chunk = Vec<Tup2<T1, ZWeight>>;
    type ZSet = OrdZSet<T1>;

    fn push_inputs(mut chunks: Self::Chunk, handles: &Self::InputHandles) {
        handles.append(&mut chunks);
    }

    fn read_outputs(handles: &Self::OutputHandles) -> Self::ZSet {
        SpineSnapshot::<OrdZSet<T1>>::concat(&handles.take_from_all()).consolidate()
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

fn circuit_config(path: &PathBuf) -> CircuitConfig {
    CircuitConfig::with_workers(NUM_WORKERS)
        .with_splitter_chunk_size_records(2)
        .with_mode(Mode::Persistent)
        .with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: Default::default(),
                },
                Default::default(),
            )
            .unwrap(),
        ))
}

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

    init_test_logger();

    let path = tempfile::tempdir().unwrap().keep();
    println!("Running replay_test in {}", path.display());

    // Create both reference circuits, feed I1 and I2 to circuit1; feed I2 and I3 to circuit2.
    let mut reference_output1 = Vec::new();
    let mut reference_output2 = Vec::new();
    let mut reference_output2_2 = Vec::new();
    let mut reference_output3 = Vec::new();

    {
        println!("Running first circuit to get reference output");
        let circuit_constructor1_clone = circuit_constructor1.clone();
        let (mut circuit, (input_handles1, input_handles2, output_handles1, output_handles2)) =
            Runtime::init_circuit(circuit_config(&path), move |circuit| {
                Ok(circuit_constructor1_clone(circuit))
            })
            .unwrap();

        for (data1, data2) in std::iter::zip(&inputs1, &inputs2_1) {
            I1::push_inputs(data1.clone(), &input_handles1);
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.transaction().unwrap();

            reference_output1.push(O1::read_outputs(&output_handles1));
            reference_output2.push(O2::read_outputs(&output_handles2));
        }

        for data2 in inputs2_2.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.transaction().unwrap();

            reference_output2.push(O2::read_outputs(&output_handles2));
        }

        circuit.kill().unwrap();

        println!("Running second circuit to get reference output");
        let circuit_constructor2_clone = circuit_constructor2.clone();
        let (mut circuit, (input_handles2, input_handles3, output_handles2, output_handles3)) =
            Runtime::init_circuit(circuit_config(&path), move |circuit| {
                Ok(circuit_constructor2_clone(circuit))
            })
            .unwrap();

        for data2 in inputs2_1.iter() {
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.transaction().unwrap();

            reference_output2_2.push(O2::read_outputs(&output_handles2));
        }

        for (data2, data3) in std::iter::zip(&inputs2_2, &inputs3) {
            I2::push_inputs(data2.clone(), &input_handles2);
            I3::push_inputs(data3.clone(), &input_handles3);

            circuit.transaction().unwrap();

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
        println!("Running first circuit to create checkpoint");

        // Create the first circuit.
        let circuit_constructor1_clone = circuit_constructor1.clone();

        let (mut circuit, (input_handles1, input_handles2, output_handles1, output_handles2)) =
            Runtime::init_circuit(circuit_config(&path), move |circuit| {
                Ok(circuit_constructor1_clone(circuit))
            })
            .unwrap();

        // Feed inputs1, inputs2_1.
        for (data1, data2) in std::iter::zip(inputs1, inputs2_1) {
            I1::push_inputs(data1.clone(), &input_handles1);
            I2::push_inputs(data2.clone(), &input_handles2);

            circuit.transaction().unwrap();

            actual_output1.push(O1::read_outputs(&output_handles1));
            actual_output2.push(O2::read_outputs(&output_handles2));
        }

        // Checkpoint.
        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    assert_eq!(reference_output1, actual_output1);

    {
        println!("Restarting circuit from checkpoint {}", checkpoint.uuid);

        // Restart the second circuit from the checkpoint.
        let mut circuit_config = circuit_config(&path);
        circuit_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);

        let circuit_constructor2_clone = circuit_constructor2.clone();

        let (mut circuit, (input_handles2, input_handles3, output_handles2, output_handles3)) =
            Runtime::init_circuit(circuit_config, move |circuit| {
                Ok(circuit_constructor2_clone(circuit))
            })
            .unwrap();

        while circuit.bootstrap_in_progress() {
            circuit.transaction().unwrap();
        }
        println!("Replay finished");

        // Feed inputs2_2, inputs2.
        for (data2, data3) in std::iter::zip(&inputs2_2, &inputs3) {
            I2::push_inputs(data2.clone(), &input_handles2);
            I3::push_inputs(data3.clone(), &input_handles3);

            circuit.transaction().unwrap();

            actual_output2.push(O2::read_outputs(&output_handles2));
            actual_output3.push(O3::read_outputs(&output_handles3));
        }

        println!("Additional transactions completed");

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
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    (),
) {
    let (input_stream, input_handle) = circuit.add_input_zset::<u64>();
    let output_handle = input_stream
        .map(|x| x % 1000)
        .accumulate_output_persistent(Some("output1"));

    (input_handle, (), output_handle, ())
}

fn linear_circuit2(
    circuit: &mut RootCircuit,
) -> (
    (),
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input_stream, input_handle) = circuit.add_input_zset::<u64>();
    let output_handle = input_stream
        .map(|x| x >> 1)
        .accumulate_output_persistent(Some("output2"));

    ((), input_handle, (), output_handle)
}

#[test]
fn test_linear_circuit() {
    test_replay::<TestData1<u64>, (), TestData1<u64>, TestData1<u64>, (), TestData1<u64>>(
        Arc::new(linear_circuit1),
        Arc::new(linear_circuit2),
        sequence(0, 2),
        std::iter::repeat_n((), 2).collect(),
        std::iter::repeat_n((), 2).collect(),
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
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
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
        .accumulate_output_persistent(Some("output1"));

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("output2"));

    (input_handle1, input_handle2, output_handle1, output_handle2)
}

fn linear_circuit_materialized_inputs2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    input_stream2.integrate_trace();
    input_stream3.integrate_trace();

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("output2"));

    let output_handle3 = input_stream3
        .map(|x| x >> 1)
        .accumulate_output_persistent(Some("output3"));

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
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
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
        .accumulate_output_persistent(Some("output1"));

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("output2"));

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
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
) {
    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    input_stream2.integrate_trace();
    input_stream3.integrate_trace();

    let output_handle2 = input_stream2
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("output2_2"));

    let output_handle3 = input_stream3
        .map(|x| x >> 1)
        .accumulate_output_persistent(Some("output3"));

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
//        |----------------------|
//        |                      v
//        |              |----->join--->
//        |              |
// ---> input1 ---> join --> output1
//                   ^
// ---> input2 ------|
//
// Pipeline 2 (adds another join):
//
//        |----------------------|
//        |                      v
//        |              |----->join--->
//        |              |
// ---> input1 ---> join --> output1
//                   ^    |
// ---> input2 ------|    |------------>join-->output2
//                   v    |
// ---> input3 ---> join --
//
fn join_circuit1(
    balancing: bool,
    circuit: &mut RootCircuit,
) -> (
    (),
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    // Note: we don't need to manually create integrals of the input streams, as they can be replayed from the
    // integrals maintained internally by the join operator.

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));
    let input_stream2_indexed = input_stream2
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream2_indexed"));

    let (_j1_indexed, output_handle1) = if balancing {
        let j1 = input_stream1_indexed
            .join_balanced_inner(&input_stream2_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j1"));

        let j1_indexed = j1
            .map_index(|x| (*x, *x))
            .set_persistent_id(Some("j1-indexed"));
        j1_indexed.join_balanced_inner(&input_stream1_indexed, |key, _v1, _v2| *key);

        (j1_indexed, j1.accumulate_output_persistent(Some("output1")))
    } else {
        let j1 = input_stream1_indexed
            .join(&input_stream2_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j1"));

        let j1_indexed = j1
            .map_index(|x| (*x, *x))
            .set_persistent_id(Some("j1-indexed"));
        j1_indexed.join(&input_stream1_indexed, |key, _v1, _v2| *key);

        (j1_indexed, j1.accumulate_output_persistent(Some("output1")))
    };

    ((), (input_handle1, input_handle2), (), output_handle1)
}

fn join_circuit2(
    balancing: bool,
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    // // These integrals will be used for replay input streams.
    // input_stream1.integrate_trace();
    // input_stream2.integrate_trace();
    // input_stream3.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));
    let input_stream2_indexed = input_stream2
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream2_indexed"));
    let input_stream3_indexed = input_stream3
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream3_indexed"));

    let (j1_indexed, output_handle1) = if balancing {
        let j1 = input_stream1_indexed
            .join_balanced_inner(&input_stream2_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j1"));

        let j1_indexed = j1
            .map_index(|x| (*x, *x))
            .set_persistent_id(Some("j1-indexed"));

        j1_indexed.join_balanced_inner(&input_stream1_indexed, |key, _v1, _v2| *key);

        (j1_indexed, j1.accumulate_output_persistent(Some("output1")))
    } else {
        let j1 = input_stream1_indexed
            .join(&input_stream2_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j1"));

        let j1_indexed = j1
            .map_index(|x| (*x, *x))
            .set_persistent_id(Some("j1-indexed"));

        j1_indexed.join(&input_stream1_indexed, |key, _v1, _v2| *key);

        (j1_indexed, j1.accumulate_output_persistent(Some("output1")))
    };

    let output_handle2 = if balancing {
        let j2 = input_stream2_indexed
            .join_balanced_inner(&input_stream3_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j2"));

        let j2_indexed = j2
            .map_index(|x| (*x, *x))
            .set_persistent_id(Some("j2-indexed"));

        let j3 = j2_indexed
            .join_balanced_inner(&j1_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j3"));

        j3.accumulate_output_persistent(Some("output2"))
    } else {
        let j2 = input_stream2_indexed
            .join(&input_stream3_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j2"));

        let j2_indexed = j2
            .map_index(|x| (*x, *x))
            .set_persistent_id(Some("j2-indexed"));

        let j3 = j2_indexed
            .join(&j1_indexed, |key, _v1, _v2| *key)
            .set_persistent_id(Some("j3"));

        j3.accumulate_output_persistent(Some("output2"))
    };

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
        Arc::new(|circuit| join_circuit1(false, circuit)),
        Arc::new(|circuit| join_circuit2(false, circuit)),
        std::iter::repeat_n((), 2).collect(),
        std::iter::zip(sequence(0, 2), sequence(2, 4)).collect(),
        std::iter::zip(sequence(2, 4), sequence(0, 2)).collect(),
        sequence(1, 3),
    );
}

#[test]
fn test_balanced_join_circuit() {
    test_replay::<(), TestData2<u64, u64>, TestData1<u64>, (), TestData1<u64>, TestData1<u64>>(
        Arc::new(|circuit| join_circuit1(true, circuit)),
        Arc::new(|circuit| join_circuit2(true, circuit)),
        std::iter::repeat_n((), 2).collect(),
        std::iter::zip(sequence(0, 2), sequence(2, 4)).collect(),
        std::iter::zip(sequence(2, 4), sequence(0, 2)).collect(),
        sequence(1, 3),
    );
}

// Test adaptive joins+bootstrapping:
//
// This test modifies the join cluster by adding an extra join to it.
// Depending on the pre-commit partitioning policies, the circuit should be able
// to either bootstrap the new join only or the entire cluster.
//
// Pipeline 1:
//
// ---> input1 ---> join --> output1
//                   ^
// ---> input2 ------|
//
//
// ---> input3 ---> join --> output2
//                   ^
// ---> input4 ------|
//
//
// Pipeline 2:
//
// ---> input1 ---> join --> output1
//                   ^
// ---> input2 ------|
//                   v
//         |-------> join --> output3
//         |
//         |
// ---> input3 ---> join --> output2
//                   ^
// ---> input4 ------|
fn balancer_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    (
        ZSetHandle<u64>,
        ZSetHandle<u64>,
        ZSetHandle<u64>,
        ZSetHandle<u64>,
    ),
    (),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    let (input_stream4, input_handle4) = circuit.add_input_zset::<u64>();
    input_stream4.set_persistent_id(Some("input4"));

    // These integrals will be used for replay input streams.
    input_stream1.integrate_trace();
    input_stream2.integrate_trace();
    input_stream3.integrate_trace();
    input_stream4.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));
    let input_stream2_indexed = input_stream2
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream2_indexed"));
    let input_stream3_indexed = input_stream3
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream3_indexed"));
    let input_stream4_indexed = input_stream4
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream4_indexed"));

    let output_handle1 = input_stream1_indexed
        .join_balanced_inner(&input_stream2_indexed, |key, _v1, _v2| *key)
        .accumulate_output_persistent(Some("output1"));

    let output_handle2 = input_stream3_indexed
        .join_balanced_inner(&input_stream4_indexed, |key, _v1, _v2| *key)
        .accumulate_output_persistent(Some("output2"));

    (
        (),
        (input_handle1, input_handle2, input_handle3, input_handle4),
        (),
        (output_handle1, output_handle2),
    )
}

fn balancer_circuit2(
    circuit: &mut RootCircuit,
) -> (
    (
        ZSetHandle<u64>,
        ZSetHandle<u64>,
        ZSetHandle<u64>,
        ZSetHandle<u64>,
    ),
    (),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let (input_stream2, input_handle2) = circuit.add_input_zset::<u64>();
    input_stream2.set_persistent_id(Some("input2"));

    let (input_stream3, input_handle3) = circuit.add_input_zset::<u64>();
    input_stream3.set_persistent_id(Some("input3"));

    let (input_stream4, input_handle4) = circuit.add_input_zset::<u64>();
    input_stream4.set_persistent_id(Some("input4"));

    // These integrals will be used for replay input streams.
    input_stream1.integrate_trace();
    input_stream2.integrate_trace();
    input_stream3.integrate_trace();
    input_stream4.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream1_indexed"));
    let input_stream2_indexed = input_stream2
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream2_indexed"));
    let input_stream3_indexed = input_stream3
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream3_indexed"));
    let input_stream4_indexed = input_stream4
        .map_index(|x| (*x, *x))
        .set_persistent_id(Some("input_stream4_indexed"));

    let output_handle1 = input_stream1_indexed
        .join_balanced_inner(&input_stream2_indexed, |key, _v1, _v2| *key)
        .accumulate_output_persistent(Some("output1"));

    let output_handle2 = input_stream3_indexed
        .join_balanced_inner(&input_stream4_indexed, |key, _v1, _v2| *key)
        .accumulate_output_persistent(Some("output2"));

    let output_handle3 = input_stream2_indexed
        .join_balanced_inner(&input_stream3_indexed, |key, _v1, _v2| *key)
        .accumulate_output_persistent(Some("output3"));

    (
        (input_handle1, input_handle2, input_handle3, input_handle4),
        (),
        (output_handle1, output_handle2),
        output_handle3,
    )
}

/// Keep inputs perfectly balanced.
///
/// All collections should be partitioned using PartitioningPolicy::Shard.
/// The cluster should be able to bootstrap the new join only.
#[test]
fn test_balancer1() {
    test_replay::<(), TestData4<u64, u64, u64, u64>, (), (), TestData2<u64, u64>, TestData1<u64>>(
        Arc::new(|circuit| balancer_circuit1(circuit)),
        Arc::new(|circuit| balancer_circuit2(circuit)),
        vec![(); 100],
        itertools::izip!(
            sequence(0, 100),
            sequence(0, 100),
            sequence(0, 100),
            sequence(0, 100),
        )
        .collect(),
        itertools::izip!(
            sequence(100, 200),
            sequence(100, 200),
            sequence(100, 200),
            sequence(100, 200)
        )
        .collect(),
        vec![(); 100],
    );
}

/// Keep inputs skewed, so that both sides of the new join PartitioningPolicy::Broadcast,
/// so the cluster cannot be bootstrapped without backfilling the entire cluster.
#[test]
fn test_balancer2() {
    let skewed_sequence_small1 = (0..1000)
        .filter(|x| default_hash(&x) % NUM_WORKERS as u64 == 0)
        .map(|x| vec![Tup2(x, 1)])
        .collect::<Vec<_>>();

    let skewed_sequence_large1 = (0..1000)
        .filter(|x| default_hash(&x) % NUM_WORKERS as u64 == 0)
        .map(|x| vec![Tup2(x, 100)])
        .collect::<Vec<_>>();

    let skewed_sequence_small2 = (1000..2000)
        .filter(|x| default_hash(&x) % NUM_WORKERS as u64 == 0)
        .map(|x| vec![Tup2(x, 1)])
        .collect::<Vec<_>>();

    let skewed_sequence_large2 = (1000..2000)
        .filter(|x| default_hash(&x) % NUM_WORKERS as u64 == 0)
        .map(|x| vec![Tup2(x, 100)])
        .collect::<Vec<_>>();

    test_replay::<(), TestData4<u64, u64, u64, u64>, (), (), TestData2<u64, u64>, TestData1<u64>>(
        Arc::new(|circuit| balancer_circuit1(circuit)),
        Arc::new(|circuit| balancer_circuit2(circuit)),
        vec![(); skewed_sequence_small1.len()],
        itertools::izip!(
            skewed_sequence_large1.clone(),
            skewed_sequence_small1.clone(),
            skewed_sequence_small1.clone(),
            skewed_sequence_large1.clone(),
        )
        .collect(),
        itertools::izip!(
            skewed_sequence_large2.clone(),
            skewed_sequence_small2.clone(),
            skewed_sequence_small2.clone(),
            skewed_sequence_large2.clone(),
        )
        .collect(),
        vec![(); skewed_sequence_small2.len()],
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
) -> (
    (),
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
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

    let output_handle1 = aggregate1_flat.accumulate_output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn aggregate_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
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

    let output_handle1 = aggregate1_flat.accumulate_output_persistent(Some("output1"));
    let output_handle2 = aggregate2_flat.accumulate_output_persistent(Some("output2"));
    let output_handle3 = aggregate3_flat.accumulate_output_persistent(Some("output3"));

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
        std::iter::repeat_n((), 5).collect(),
        sequence(0, 5),
        sequence(5, 10),
        std::iter::repeat_n((), 5).collect(),
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
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
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

    let output_handle1 = paths.accumulate_output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn recursive_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<Tup2<u64, u64>>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
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

    let output_handle1 = paths.accumulate_output_persistent(Some("output1"));
    let output_handle2 = paths.accumulate_output_persistent(Some("output2"));

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
        std::iter::repeat_n((), 5).collect(),
        chain(0, 5),
        chain(5, 10),
        std::iter::repeat_n((), 5).collect(),
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

/// Total order for `Tup2` values: by first `u64`, then second (used with `rank_custom_order`).
struct RankValOrd(PhantomData<()>);

impl CmpFunc<Tup2<u64, u64>> for RankValOrd {
    fn cmp(left: &Tup2<u64, u64>, right: &Tup2<u64, u64>) -> Ordering {
        left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
    }
}

fn lag_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
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

    let output_handle1 = lag1_flat.accumulate_output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn lag_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    (
        OutputHandle<SpineSnapshot<OrdZSet<Tup3<u64, u64, u64>>>>,
        OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
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

    let output_handle1 = lag1_flat.accumulate_output_persistent(Some("output1"));
    let output_handle2 = lag2_flat.accumulate_output_persistent(Some("output2"));
    let output_handle3 = lag3_flat.accumulate_output_persistent(Some("output3"));

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
        std::iter::repeat_n((), 10).collect(),
        sequence(0, 10),
        sequence(10, 20),
        std::iter::repeat_n((), 10).collect(),
    );
}

// Circuit with rank:
//
// The interesting part here is that the input to dense_rank is replayed from the internal state of the rank operator.
//
// Pipeline 1:
//
// ---> input1 ---> rank --> output1
//
// Pipeline 2: adds a dense_rank:
//
//                      /--------> output1
// ---> input1 ---> rank-------> dense_rank --> output2
//

struct RankValOrd3(PhantomData<()>);

impl CmpFunc<Tup3<u64, u64, i64>> for RankValOrd3 {
    fn cmp(left: &Tup3<u64, u64, i64>, right: &Tup3<u64, u64, i64>) -> Ordering {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1).then_with(|| left.2.cmp(&right.2)))
    }
}

fn rank_circuit1(
    circuit: &mut RootCircuit,
) -> (
    (),
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, i64>>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    let input_stream1_indexed = input_stream1
        .map_index(|x| (x % 3, Tup2(*x, *x % 5)))
        .set_persistent_id(Some("input_stream1_indexed"));

    let rank1 = input_stream1_indexed
        .rank_custom_order_persistent::<RankValOrd, u64, _, _, _, _>(
            Some("rank1"),
            |v| v.0,
            |a, b| a.0.cmp(b),
            |rank, t| Tup3(t.0, t.1, rank),
        )
        .set_persistent_id(Some("rank1"));

    let rank1_flat = rank1
        .map(|(k, t)| Tup4(*k, t.0, t.1, t.2))
        .set_persistent_id(Some("rank1_flat"));

    let output_handle1 = rank1_flat.accumulate_output_persistent(Some("output1"));

    ((), input_handle1, (), output_handle1)
}

fn rank_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, i64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, i64>>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let input_stream1_indexed = input_stream1
        .map_index(|x| (x % 3, Tup2(*x, *x % 5)))
        .set_persistent_id(Some("input_stream1_indexed"));

    let rank1 = input_stream1_indexed
        .rank_custom_order_persistent::<RankValOrd, u64, _, _, _, _>(
            Some("rank1"),
            |v| v.0,
            |a, b| a.0.cmp(b),
            |rank, t| Tup3(t.0, t.1, rank),
        )
        .set_persistent_id(Some("rank1"));

    let rank1_flat = rank1
        .map(|(k, t)| Tup4(*k, t.0, t.1, t.2))
        .set_persistent_id(Some("rank1_flat"));

    let dense1 = rank1
        .dense_rank_custom_order_persistent::<RankValOrd3, u64, _, _, _, _>(
            Some("dense1"),
            |v| v.0,
            |a, b| a.0.cmp(b),
            |rank, t| Tup3(t.0, t.1, rank),
        )
        .set_persistent_id(Some("dense1"));

    let dense1_flat = dense1
        .map(|(k, t)| Tup4(*k, t.0, t.1, t.2))
        .set_persistent_id(Some("dense1_flat"));

    let output_handle1 = rank1_flat.accumulate_output_persistent(Some("output1"));
    let output_handle2 = dense1_flat.accumulate_output_persistent(Some("output2"));

    (input_handle1, (), output_handle1, output_handle2)
}

#[test]
fn test_rank_circuit() {
    test_replay::<
        (),
        TestData1<u64>,
        (),
        (),
        TestData1<Tup4<u64, u64, u64, i64>>,
        TestData1<Tup4<u64, u64, u64, i64>>,
    >(
        Arc::new(rank_circuit1),
        Arc::new(rank_circuit2),
        std::iter::repeat_n((), 10).collect(),
        sequence(0, 10),
        sequence(10, 20),
        std::iter::repeat_n((), 10).collect(),
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
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
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

    let output_handle1 = rolling1_flat.accumulate_output_persistent(Some("output1"));

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

    let output_handle2 = rolling2_flat.accumulate_output_persistent(Some("output2"));

    ((), input_handle1, output_handle1, output_handle2)
}

fn rolling_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    (),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
    (
        OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
        OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
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

    let output_handle2 = rolling2_flat.accumulate_output_persistent(Some("output2"));

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

    let output_handle3 = rolling3_flat.accumulate_output_persistent(Some("output3"));

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

    let output_handle4 = rolling4_flat.accumulate_output_persistent(Some("output4"));

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
        std::iter::repeat_n((), 20).collect(),
        sequence(0, 20),
        sequence(20, 40),
        std::iter::repeat_n((), 20).collect(),
    );
}

// Regression test:
//
// Pipeline 1:
// ---> input_map
//
// Pipeline 2:
// ---> input_map ---> aggregate --> output
//
// The second pipeline should replay the input from the input_map operator.
// A bug prevented this from happening, because the integral built by the
// aggregate operator was used to replay instead.

#[test]
fn regression1() {
    init_test_logger();

    let path = tempfile::tempdir().unwrap().keep();

    let (mut circuit1, input_handle1) =
        Runtime::init_circuit(circuit_config(&path), move |circuit| {
            let (input_stream, input_handle) = circuit
                .add_input_map_persistent::<u64, u64, u64, _>(Some("input_map"), |v, u| *v = *u);
            input_stream.set_persistent_id(Some("input_map"));
            Ok(input_handle)
        })
        .unwrap();

    input_handle1.push(0, crate::operator::Update::Insert(0));

    circuit1.transaction().unwrap();

    // Checkpoint.
    let checkpoint = circuit1.checkpoint().run().unwrap();
    circuit1.kill().unwrap();

    // Restart the second circuit from the checkpoint.
    let mut circuit_config = circuit_config(&path);
    circuit_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);

    let (mut circuit2, (_input_handle2, output_handle2)) =
        Runtime::init_circuit(circuit_config, move |circuit| {
            let (input_stream, input_handle) = circuit
                .add_input_map_persistent::<u64, u64, u64, _>(Some("input_map"), |v, u| *v = *u);
            input_stream.set_persistent_id(Some("input_map"));

            let aggregate = input_stream
                .aggregate_persistent(Some("aggregate1"), Max)
                .set_persistent_id(Some("aggregate1"));

            let output_handle = aggregate
                .accumulate_trace()
                .apply(|trace| trace.ro_snapshot().consolidate())
                .output_persistent(Some("output"));
            Ok((input_handle, output_handle))
        })
        .unwrap();

    while circuit2.bootstrap_in_progress() {
        circuit2.transaction().unwrap();
    }
    println!("Replay finished");

    let actual_output = &output_handle2.concat().consolidate();

    // The bug causes the output to be empty.
    assert_eq!(actual_output, &indexed_zset!(0 => {0 => 1}));
}

/// Unit test for the replay behavior of Z1Trace and AccumulateZ1Trace operators.
/// Operators must correctly replay their contents during bootstrap as one atomic transaction.

#[derive(Clone, Copy, Debug)]
enum ReplayTraceKind {
    IntegrateTrace,
    AccumulateTrace,
}

type IndexedReplayBatch = Vec<Tup2<u64, Tup2<u64, ZWeight>>>;

fn add_replay_trace(
    stream: &Stream<RootCircuit, OrdIndexedZSet<u64, u64>>,
    trace_kind: ReplayTraceKind,
) {
    match trace_kind {
        ReplayTraceKind::IntegrateTrace => {
            stream.integrate_trace();
        }
        ReplayTraceKind::AccumulateTrace => {
            stream.accumulate_trace();
        }
    }
}

fn transactional_bootstrap_circuit1(
    circuit: &mut RootCircuit,
    trace_kind: ReplayTraceKind,
) -> IndexedZSetHandle<u64, u64> {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    input_stream.set_persistent_id(Some("input"));
    add_replay_trace(&input_stream, trace_kind);
    input_handle
}

fn transactional_bootstrap_circuit2(
    circuit: &mut RootCircuit,
    trace_kind: ReplayTraceKind,
) -> (
    IndexedZSetHandle<u64, u64>,
    OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, u64>>>,
) {
    let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, u64>();
    input_stream.set_persistent_id(Some("input"));
    add_replay_trace(&input_stream, trace_kind);

    let output_handle = input_stream.accumulate_output_persistent(Some("output"));

    (input_handle, output_handle)
}

fn replay_batch_to_indexed_zset(batches: &[IndexedReplayBatch]) -> OrdIndexedZSet<u64, u64> {
    OrdIndexedZSet::from_tuples(
        (),
        batches
            .iter()
            .flatten()
            .map(|Tup2(key, Tup2(value, weight))| Tup2(Tup2(*key, *value), *weight))
            .collect(),
    )
}

fn run_transactional_bootstrap_test(
    trace_kind: ReplayTraceKind,
    batches: Vec<IndexedReplayBatch>,
    expect_multistep_replay: bool,
) {
    init_test_logger();

    let path = tempfile::tempdir().unwrap().keep();
    let expected = replay_batch_to_indexed_zset(&batches);

    let checkpoint = {
        let (mut circuit, input_handle) =
            Runtime::init_circuit(circuit_config(&path), move |circuit| {
                Ok(transactional_bootstrap_circuit1(circuit, trace_kind))
            })
            .unwrap();

        for mut batch in batches.clone() {
            input_handle.append(&mut batch);
            circuit.transaction().unwrap();
        }

        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    let mut circuit_config = circuit_config(&path);
    circuit_config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);

    let (mut circuit, (_input_handle, output_handle)) =
        Runtime::init_circuit(circuit_config, move |circuit| {
            Ok(transactional_bootstrap_circuit2(circuit, trace_kind))
        })
        .unwrap();

    assert_eq!(output_handle.num_nonempty_mailboxes(), 0);

    if circuit.bootstrap_in_progress() {
        circuit.start_transaction().unwrap();
        circuit.start_commit_transaction().unwrap();

        let mut incomplete_commit_steps = 0;
        loop {
            let commit_complete = circuit.step().unwrap();
            if commit_complete {
                break;
            }

            incomplete_commit_steps += 1;
        }

        if expect_multistep_replay {
            assert!(
                incomplete_commit_steps > 0,
                "{trace_kind:?} replay finished in a single commit step despite the splitter chunk size"
            );
        }
    }

    assert!(!circuit.bootstrap_in_progress());
    assert_eq!(output_handle.concat().consolidate(), expected);

    circuit.kill().unwrap();
}

fn transactional_bootstrap_cases() -> Vec<(Vec<IndexedReplayBatch>, bool)> {
    vec![
        (vec![], false),
        (vec![vec![Tup2(1, Tup2(10, 1))]], false),
        (
            vec![vec![Tup2(1, Tup2(10, 1)), Tup2(1, Tup2(11, 1))]],
            false,
        ),
        (
            vec![
                vec![
                    Tup2(1, Tup2(10, 1)),
                    Tup2(1, Tup2(11, 1)),
                    Tup2(2, Tup2(20, 1)),
                ],
                vec![
                    Tup2(1, Tup2(11, -1)),
                    Tup2(1, Tup2(12, 1)),
                    Tup2(4, Tup2(40, 2)),
                    Tup2(5, Tup2(50, 2)),
                    Tup2(6, Tup2(50, 2)),
                    Tup2(7, Tup2(50, 2)),
                    Tup2(8, Tup2(50, 2)),
                    Tup2(9, Tup2(50, 2)),
                ],
            ],
            true,
        ),
    ]
}

#[test]
fn test_integrate_trace_bootstrap_is_transactional() {
    for (batches, expect_multistep_replay) in transactional_bootstrap_cases() {
        run_transactional_bootstrap_test(
            ReplayTraceKind::IntegrateTrace,
            batches,
            expect_multistep_replay,
        );
    }
}

#[test]
fn test_accumulate_trace_bootstrap_is_transactional() {
    for (batches, expect_multistep_replay) in transactional_bootstrap_cases() {
        run_transactional_bootstrap_test(
            ReplayTraceKind::AccumulateTrace,
            batches,
            expect_multistep_replay,
        );
    }
}

// Concurrent bootstrapping, phase 1 (background backfill).
//
// Pipeline 1:
// ---> input1 (materialized) --> map --> output1
//
// Pipeline 2:
// ---> input1 (materialized) --> map --> output1
//                            \-> map --> output2 (new view)
//
// Pipeline 2 restarts from pipeline 1's checkpoint with
// `start_concurrent_bootstrap`: the old view (output1) must keep producing
// per-transaction deltas while the bootstrap circuit replays input1's
// integral for the new view in the background.

fn concurrent_circuit1(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    // This integral will be used to replay the input stream.
    input_stream1.integrate_trace();

    let output_handle1 = input_stream1
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("output1"));

    (input_handle1, output_handle1)
}

#[allow(clippy::type_complexity)]
fn concurrent_circuit2(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input_stream1, input_handle1) = circuit.add_input_zset::<u64>();
    input_stream1.set_persistent_id(Some("input1"));

    input_stream1.integrate_trace();

    let output_handle1 = input_stream1
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("output1"));

    // The new view is stateful (`distinct` integrates its output), so its
    // post-cutover deltas depend on the state computed during
    // bootstrapping: a correct cutover suppresses re-insertions of elements
    // already seen before it.  `distinct`'s internal integrals derive their
    // persistent ids from their input streams, so both streams need ids.
    let mapped = input_stream1.map(|x| x % 7);
    mapped.set_persistent_id(Some("view2_input"));
    let distinct = mapped.distinct();
    distinct.set_persistent_id(Some("view2"));
    let output_handle2 = distinct.accumulate_output_persistent(Some("output2"));

    (input_handle1, output_handle1, output_handle2)
}

#[test]
fn test_concurrent_bootstrap_old_views_live() {
    init_test_logger();

    let path = tempfile::tempdir().unwrap().keep();
    println!(
        "Running test_concurrent_bootstrap_old_views_live in {}",
        path.display()
    );

    fn read_output(handle: &OutputHandle<SpineSnapshot<OrdZSet<u64>>>) -> OrdZSet<u64> {
        SpineSnapshot::<OrdZSet<u64>>::concat(&handle.take_from_all()).consolidate()
    }

    // Run pipeline 1 and checkpoint it.
    let checkpoint = {
        let (mut circuit, (input, output1)) =
            Runtime::init_circuit(circuit_config(&path), |circuit| {
                Ok(concurrent_circuit1(circuit))
            })
            .unwrap();

        for x in 0..10u64 {
            input.push(x, 1);
            circuit.transaction().unwrap();
            assert_eq!(read_output(&output1), zset! { x + 5 => 1 });
        }

        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    // Restart as pipeline 2 with a concurrent bootstrap.
    let (mut circuit, (input, output1, output2)) =
        Runtime::init_circuit(circuit_config(&path), |circuit| {
            Ok(concurrent_circuit2(circuit))
        })
        .unwrap();

    let outcome = circuit
        .start_concurrent_bootstrap(checkpoint.uuid.to_string().into())
        .unwrap();
    let crate::circuit::ConcurrentRestoreOutcome::Concurrent(info) = outcome else {
        panic!("the new view must bootstrap concurrently, got {outcome:?}");
    };
    assert!(!info.replay_sources.is_empty());

    // The old view keeps producing per-transaction deltas while the
    // bootstrap circuit replays one chunk per pump.  With a splitter chunk
    // size of 2 records and 10 checkpointed records spread across
    // NUM_WORKERS workers, the replay needs several pumps, so the loop below
    // genuinely interleaves the two circuits.
    let mut bootstrap_done = false;
    let mut pumps = 0;
    for x in 10..20u64 {
        input.push(x, 1);
        circuit.transaction().unwrap();
        assert_eq!(read_output(&output1), zset! { x + 5 => 1 });

        if !bootstrap_done {
            bootstrap_done = circuit.step_bootstrap_circuit().unwrap();
            pumps += 1;
        }
    }
    println!("bootstrap complete after {pumps} pumps: {bootstrap_done}");

    // Finish the replay if the main loop outpaced it.
    while !bootstrap_done {
        bootstrap_done = circuit.step_bootstrap_circuit().unwrap();
        pumps += 1;
    }
    assert!(pumps > 1, "expected a multi-pump replay, got {pumps}");

    // The new view produces nothing before cutover: its operators are
    // excluded from the main circuit's schedule.
    assert_eq!(read_output(&output2), zset! {});

    // The old view continues to work after the background replay completes.
    for x in 20..25u64 {
        input.push(x, 1);
        circuit.transaction().unwrap();
        assert_eq!(read_output(&output1), zset! { x + 5 => 1 });
    }

    // Cutting over before the synchronization transaction is rejected.
    assert!(circuit.complete_concurrent_bootstrap().is_err());

    // Synchronization: replay the recorded changes (x = 10..25) into the
    // bootstrap circuit.  The main circuit processes no transactions until
    // the cutover.
    circuit.sync_concurrent_bootstrap().unwrap();
    assert!(circuit.sync_concurrent_bootstrap().is_err());
    while !circuit.step_bootstrap_circuit().unwrap() {}

    // Cutover: install the bootstrapped state into the main circuit and
    // reactivate it fully.
    circuit.complete_concurrent_bootstrap().unwrap();
    assert!(circuit.destroy_bootstrap_circuit().is_err());

    // The new view is now live.  `25 % 7 == 4`, and `4` was produced by
    // both the checkpointed inputs (`4`) and the recorded ones (`11`, `18`),
    // so a correct cutover must suppress it; `distinct` re-emits it only if
    // the bootstrapped state was lost.
    input.push(25, 1);
    circuit.transaction().unwrap();
    assert_eq!(read_output(&output1), zset! { 30 => 1 });
    assert_eq!(read_output(&output2), zset! {});

    // An element never seen before (`5 % 7 == 5` was seen; use a value with
    // residue 6 that only appeared via inputs 6, 13, 20 — also seen; every
    // residue 0..=6 was covered by 0..25, so push weight -1 retractions to
    // observe state instead): retracting one of three copies of residue 4
    // must produce no delta, retracting all three must retract it.
    for x in [4u64, 11, 18] {
        input.push(x, -1);
        circuit.transaction().unwrap();
        assert_eq!(read_output(&output2), zset! {});
    }
    input.push(25, -1);
    circuit.transaction().unwrap();
    assert_eq!(read_output(&output2), zset! { 4 => -1 });

    circuit.kill().unwrap();
}

// ---------------------------------------------------------------------------
// Concurrent bootstrapping across circuit topologies.
//
// Each test runs the same protocol through `run_concurrent_bootstrap_test`:
//
// 1. Reference: run circuit 2 (old views + new views) from scratch over all
//    input phases, recording every per-transaction output delta.
// 2. Run circuit 1 over phase-1 inputs, checkpoint, kill.
// 3. Restart as circuit 2 with `start_concurrent_bootstrap` and check the
//    expected outcome:
//    * `Concurrent`: feed phase-2 inputs, asserting after every transaction
//      that the old views' deltas match the reference and the new views stay
//      silent, while pumping the bootstrap circuit (the small splitter chunk
//      size guarantees a genuinely interleaved multi-pump replay); then run
//      the synchronization transaction and the cutover.
//    * `FellBack`: assert the refusal reason, then drive the classic
//      (stop-the-world) bootstrap to completion.
//    * `UpToDate`: nothing to bootstrap.
// 4. Feed phase-3 inputs, asserting every view's per-transaction delta.
// 5. Feed phase-4 inputs -- by convention the retraction of every record fed
//    in phases 1-3 -- asserting every view's per-transaction delta.  The
//    retraction deltas of stateful views depend on their accumulated state,
//    so any record lost or duplicated by the checkpoint, the backfill, the
//    synchronization, or the state transfer shifts them and fails the test.
// ---------------------------------------------------------------------------

/// Expected result of `start_concurrent_bootstrap` in a topology test.
#[derive(Clone, Copy)]
enum ExpectedOutcome {
    /// The bootstrap proceeds concurrently.
    Concurrent,
    /// The engine falls back to a classic bootstrap; the reason must contain
    /// the given substring.
    FellBack(&'static str),
    /// The checkpoint matches the circuit.
    UpToDate,
}

type ConcCircuit1Fn<I, OO> = Arc<
    dyn Fn(
            &mut RootCircuit,
        ) -> (
            <I as TestDataType>::InputHandles,
            <OO as TestDataType>::OutputHandles,
        ) + Send
        + Sync,
>;

type ConcCircuit2Fn<I, OO, ON> = Arc<
    dyn Fn(
            &mut RootCircuit,
        ) -> (
            <I as TestDataType>::InputHandles,
            <OO as TestDataType>::OutputHandles,
            <ON as TestDataType>::OutputHandles,
        ) + Send
        + Sync,
>;

fn circuit_config_with_workers(path: &PathBuf, workers: usize) -> CircuitConfig {
    CircuitConfig::with_workers(workers)
        .with_splitter_chunk_size_records(2)
        .with_mode(Mode::Persistent)
        .with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path: path.to_string_lossy().into_owned(),
                    cache: Default::default(),
                },
                Default::default(),
            )
            .unwrap(),
        ))
}

#[allow(clippy::too_many_arguments)]
fn run_concurrent_bootstrap_test<I, OO, ON>(
    workers: usize,
    configure: fn(CircuitConfig) -> CircuitConfig,
    circuit1: ConcCircuit1Fn<I, OO>,
    circuit2: ConcCircuit2Fn<I, OO, ON>,
    inputs1: Vec<I::Chunk>,
    inputs2: Vec<I::Chunk>,
    inputs3: Vec<I::Chunk>,
    inputs4: Vec<I::Chunk>,
    expected: ExpectedOutcome,
    // Invoked with the bootstrap info in the `Concurrent` case; lets tests
    // assert WHERE the replay boundary landed (e.g., at an operator's own
    // output integral vs at the integrals upstream of it).
    info_check: fn(&crate::circuit::circuit_builder::BootstrapInfo),
) where
    I: TestDataType,
    OO: TestDataType,
    ON: TestDataType,
{
    init_test_logger();

    let path = tempfile::tempdir().unwrap().keep();
    println!("Running concurrent bootstrap test in {}", path.display());

    let all_inputs: Vec<I::Chunk> = inputs1
        .iter()
        .chain(inputs2.iter())
        .chain(inputs3.iter())
        .chain(inputs4.iter())
        .cloned()
        .collect();

    // Reference: circuit 2 from scratch over all phases.
    let mut ref_old = Vec::new();
    let mut ref_new = Vec::new();
    {
        let circuit2 = circuit2.clone();
        let (mut circuit, (inputs, old_outputs, new_outputs)) = Runtime::init_circuit(
            configure(circuit_config_with_workers(&path, workers)),
            move |circuit| Ok(circuit2(circuit)),
        )
        .unwrap();

        for chunk in all_inputs.iter() {
            I::push_inputs(chunk.clone(), &inputs);
            circuit.transaction().unwrap();
            ref_old.push(OO::read_outputs(&old_outputs));
            ref_new.push(ON::read_outputs(&new_outputs));
        }
        circuit.kill().unwrap();
    }

    // Phase 1: run circuit 1 and checkpoint it.  Adding views does not
    // change the old views' outputs, so the reference applies here too.
    let checkpoint = {
        let circuit1 = circuit1.clone();
        let (mut circuit, (inputs, old_outputs)) = Runtime::init_circuit(
            configure(circuit_config_with_workers(&path, workers)),
            move |circuit| Ok(circuit1(circuit)),
        )
        .unwrap();

        for (i, chunk) in inputs1.iter().enumerate() {
            I::push_inputs(chunk.clone(), &inputs);
            circuit.transaction().unwrap();
            assert_eq!(
                OO::read_outputs(&old_outputs),
                ref_old[i],
                "phase 1, transaction {i}"
            );
        }

        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    // Restart as circuit 2 with a concurrent bootstrap.
    let circuit2 = circuit2.clone();
    let (mut circuit, (inputs, old_outputs, new_outputs)) = Runtime::init_circuit(
        configure(circuit_config_with_workers(&path, workers)),
        move |circuit| Ok(circuit2(circuit)),
    )
    .unwrap();

    // The value of "no output": mailboxes are empty before any transaction.
    let empty_new = ON::read_outputs(&new_outputs);

    let outcome = circuit
        .start_concurrent_bootstrap(checkpoint.uuid.to_string().into())
        .unwrap();

    let mut offset = inputs1.len();

    match expected {
        ExpectedOutcome::Concurrent => {
            let crate::circuit::ConcurrentRestoreOutcome::Concurrent(info) = outcome else {
                panic!("expected a concurrent bootstrap, got {outcome:?}");
            };
            assert!(!info.replay_sources.is_empty());
            info_check(&info);

            // Phase 2: old views live and correct per transaction; new views
            // silent; bootstrap pumped in between.
            let mut bootstrap_done = false;
            let mut pumps = 0;
            for (i, chunk) in inputs2.iter().enumerate() {
                I::push_inputs(chunk.clone(), &inputs);
                circuit.transaction().unwrap();
                assert_eq!(
                    OO::read_outputs(&old_outputs),
                    ref_old[offset + i],
                    "phase 2, transaction {i}: old views diverged during the background replay"
                );
                assert_eq!(
                    ON::read_outputs(&new_outputs),
                    empty_new,
                    "phase 2, transaction {i}: new views must be silent before cutover"
                );
                if !bootstrap_done {
                    bootstrap_done = circuit.step_bootstrap_circuit().unwrap();
                    pumps += 1;
                }
            }
            while !bootstrap_done {
                bootstrap_done = circuit.step_bootstrap_circuit().unwrap();
                pumps += 1;
            }
            assert!(
                pumps > 1,
                "expected a multi-pump background replay, got {pumps}"
            );

            // Synchronization and cutover.
            circuit.sync_concurrent_bootstrap().unwrap();
            while !circuit.step_bootstrap_circuit().unwrap() {}
            circuit.complete_concurrent_bootstrap().unwrap();

            offset += inputs2.len();
        }
        ExpectedOutcome::FellBack(reason_substr) => {
            let crate::circuit::ConcurrentRestoreOutcome::FellBack { reason, .. } = &outcome else {
                panic!("expected a fallback, got {outcome:?}");
            };
            assert!(
                reason.contains(reason_substr),
                "fallback reason {reason:?} does not mention {reason_substr:?}"
            );

            // Classic bootstrap: stop-the-world replay driven by empty
            // transactions.
            assert!(circuit.bootstrap_in_progress());
            while circuit.bootstrap_in_progress() {
                circuit.transaction().unwrap();
            }
            // Discard the backfill deltas the replay emitted to the outputs.
            let _ = OO::read_outputs(&old_outputs);
            let _ = ON::read_outputs(&new_outputs);

            // Phase 2 runs after the bootstrap; all views are live.
            for (i, chunk) in inputs2.iter().enumerate() {
                I::push_inputs(chunk.clone(), &inputs);
                circuit.transaction().unwrap();
                assert_eq!(OO::read_outputs(&old_outputs), ref_old[offset + i]);
                assert_eq!(ON::read_outputs(&new_outputs), ref_new[offset + i]);
            }
            offset += inputs2.len();
        }
        ExpectedOutcome::UpToDate => {
            assert!(
                matches!(outcome, crate::circuit::ConcurrentRestoreOutcome::UpToDate),
                "expected UpToDate, got {outcome:?}"
            );
            for (i, chunk) in inputs2.iter().enumerate() {
                I::push_inputs(chunk.clone(), &inputs);
                circuit.transaction().unwrap();
                assert_eq!(OO::read_outputs(&old_outputs), ref_old[offset + i]);
                assert_eq!(ON::read_outputs(&new_outputs), ref_new[offset + i]);
            }
            offset += inputs2.len();
        }
    }

    // Phases 3 and 4: every view live; phase 4 (the full retraction) probes
    // the accumulated state of every stateful operator.
    for (i, chunk) in inputs3.iter().chain(inputs4.iter()).enumerate() {
        I::push_inputs(chunk.clone(), &inputs);
        circuit.transaction().unwrap();
        assert_eq!(
            OO::read_outputs(&old_outputs),
            ref_old[offset + i],
            "post-cutover transaction {i}: old views"
        );
        assert_eq!(
            ON::read_outputs(&new_outputs),
            ref_new[offset + i],
            "post-cutover transaction {i}: new views"
        );
    }

    circuit.kill().unwrap();
}

/// One single-record transaction per value in `from..to`, weight `w`.
fn weighted_sequence(from: u64, to: u64, w: ZWeight) -> Vec<Vec<Tup2<u64, ZWeight>>> {
    (from..to).map(|x| vec![Tup2(x, w)]).collect()
}

/// The retraction of every chunk in `phases`, one transaction per chunk.
fn negate_chunks(phases: &[&[Vec<Tup2<u64, ZWeight>>]]) -> Vec<Vec<Tup2<u64, ZWeight>>> {
    phases
        .iter()
        .flat_map(|chunks| chunks.iter())
        .map(|chunk| chunk.iter().map(|Tup2(x, w)| Tup2(*x, -w)).collect())
        .collect()
}

// --- Topology: two views over one materialized table; stateless new view. ---
//
// The new view's only checkpointable node is its output marker, so the
// excluded set is minimal and the cutover transfers (almost) nothing; the
// interesting property is that the live circuit's copy of the new view's
// operators can run against live inputs with their outputs dropped, without
// disturbing anything.

fn topo_shared_input_old(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_shared_input_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));
    let out_new = input
        .map(|x| x * 3)
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_stateless_new_view() {
    let p1 = weighted_sequence(0, 16, 1);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_shared_input_old),
        Arc::new(topo_shared_input_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: boundary at an INTERIOR materialized stream. ---
//
// The old view materializes a distinct; the new (stateful, aggregating) view
// consumes the distinct's output.  The replay boundary is the interior
// distinct stream, not the input: the recorder records the distinct's output
// deltas, and the old view's operators -- including the shared distinct --
// keep running live throughout.

fn topo_interior_boundary_old(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let d_in = input.map(|x| x % 31);
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d"));
    d.integrate_trace();
    let out_old = d.accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_interior_boundary_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, ZWeight>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let d_in = input.map(|x| x % 31);
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d"));
    d.integrate_trace();
    let out_old = d.accumulate_output_persistent(Some("out_old"));

    // New view: count of distinct elements per residue class -- a stateful
    // aggregate whose deltas depend on the accumulated group contents.
    let d_by_key = d.map_index(|x| (x % 5, *x));
    d_by_key.set_persistent_id(Some("d_by_key"));
    let counts = d_by_key.weighted_count_persistent(Some("counts_agg"));
    counts.set_persistent_id(Some("counts"));
    let out_new = counts
        .map(|(k, c)| Tup2(*k, *c))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_interior_boundary_aggregate() {
    let p1 = weighted_sequence(0, 20, 1);
    let p2 = weighted_sequence(20, 30, 1);
    let p3 = weighted_sequence(30, 36, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, ZWeight>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_interior_boundary_old),
        Arc::new(topo_interior_boundary_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: unmaterialized operators shared by BOTH circuit copies. ---
//
// The shared map chain has no replay source of its own, so the backward walk
// pulls it into the replay circuit: the same operators run live in the main
// circuit (serving the old view) and replaying in the bootstrap circuit
// (rebuilding the new view's distinct), at different logical positions in
// the data.

fn topo_shared_chain_old(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let m = input.map(|x| x.wrapping_mul(2) % 97);
    m.set_persistent_id(Some("m"));
    let out_old = m
        .map(|x| x + 1)
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_shared_chain_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let m = input.map(|x| x.wrapping_mul(2) % 97);
    m.set_persistent_id(Some("m"));
    let out_old = m
        .map(|x| x + 1)
        .accumulate_output_persistent(Some("out_old"));
    let d = m.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_shared_unmaterialized_chain() {
    let p1 = weighted_sequence(0, 20, 1);
    let p2 = weighted_sequence(20, 28, 1);
    let p3 = weighted_sequence(28, 34, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_shared_chain_old),
        Arc::new(topo_shared_chain_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: old and new views share one STATEFUL operator. ---
//
// The old view outputs a distinct directly; the new view extends the same
// (cached, hence physically shared) distinct.  Depending on where the walk
// finds a replay source, the distinct either becomes the boundary or is
// rebuilt inside the bootstrap circuit while its twin serves the old view
// live; the test asserts correctness in either case.

fn topo_shared_distinct_old(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let d_in = input.map(|x| x % 41);
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d"));
    let out_old = d.accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_shared_distinct_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let d_in = input.map(|x| x % 41);
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d"));
    let out_old = d.accumulate_output_persistent(Some("out_old"));
    let out_new = d
        .map(|x| x + 1000)
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_shared_stateful_operator() {
    let p1 = weighted_sequence(0, 20, 1);
    let p2 = weighted_sequence(20, 30, 1);
    let p3 = weighted_sequence(30, 36, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_shared_distinct_old),
        Arc::new(topo_shared_distinct_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: new JOIN view over two materialized tables. ---
//
// Two boundary streams, two recorders, and transferable join state: with
// default settings the multi-worker join uses the sharded-exchange path,
// whose state moves between circuit copies.  With adaptive joins enabled the
// join uses the rebalancing machinery instead, whose state cannot move: the
// same topology must then refuse up front -- before any backfill work -- and
// fall back to a classic bootstrap that completes correctly.

fn topo_join_old(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));
    ((h1, h2), (out1, out2))
}

#[allow(clippy::type_complexity)]
fn topo_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    let out_new = joined.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), (out1, out2), out_new)
}

#[allow(clippy::type_complexity)]
fn topo_join_balanced_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join_balanced_inner(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    let out_new = joined.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), (out1, out2), out_new)
}

fn join_phase(
    r1: std::ops::Range<u64>,
    r2: std::ops::Range<u64>,
) -> Vec<(Vec<Tup2<u64, ZWeight>>, Vec<Tup2<u64, ZWeight>>)> {
    r1.zip(r2)
        .map(|(a, b)| (vec![Tup2(a, 1)], vec![Tup2(b, 1)]))
        .collect()
}

fn negate_join_chunks(
    phases: &[&[(Vec<Tup2<u64, ZWeight>>, Vec<Tup2<u64, ZWeight>>)]],
) -> Vec<(Vec<Tup2<u64, ZWeight>>, Vec<Tup2<u64, ZWeight>>)> {
    phases
        .iter()
        .flat_map(|chunks| chunks.iter())
        .map(|(c1, c2)| {
            (
                c1.iter().map(|Tup2(x, w)| Tup2(*x, -w)).collect(),
                c2.iter().map(|Tup2(x, w)| Tup2(*x, -w)).collect(),
            )
        })
        .collect()
}

#[test]
fn test_concurrent_join_view_single_worker() {
    let p1 = join_phase(0..12, 100..112);
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData2<u64, u64>,
        TestData1<Tup2<u64, u64>>,
    >(
        1,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

#[test]
fn test_concurrent_join_view_multiworker() {
    let p1 = join_phase(0..12, 100..112);
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData2<u64, u64>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

#[test]
fn test_concurrent_balanced_join_falls_back() {
    let p1 = join_phase(0..12, 100..112);
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData2<u64, u64>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_join_balanced_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::FellBack("cannot be transferred"),
        |_| {},
    );
}

// --- Topology: OLD multi-worker join stays live while a new view bootstraps. ---
//
// The rebalancing operators live outside the backfilled cone, so the
// concurrent bootstrap must proceed, and the join -- the most
// coordination-heavy machinery in the engine -- must keep producing correct
// deltas while the bootstrap circuit (which contains an unscheduled clone of
// it) replays in the background.

#[allow(clippy::type_complexity)]
fn topo_old_join_old(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    let out_old = joined.accumulate_output_persistent(Some("out_old"));
    ((h1, h2), out_old)
}

#[allow(clippy::type_complexity)]
fn topo_old_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    let out_old = joined.accumulate_output_persistent(Some("out_old"));

    let d_in = t1.map(|x| x % 41);
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), out_old, out_new)
}

#[test]
fn test_concurrent_old_join_stays_live() {
    let p1 = join_phase(0..16, 100..116);
    let p2 = join_phase(16..24, 116..124);
    let p3 = join_phase(24..28, 124..128);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData2<u64, u64>, TestData1<Tup2<u64, u64>>, TestData1<u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_old_join_old),
        Arc::new(topo_old_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: linear aggregate with deletions in every phase. ---
//
// Sums are sensitive to lost or duplicated records in both directions; the
// deletions interleaved into each phase exercise negative weights through
// the checkpoint, the recorded window, and the post-cutover stream.

fn topo_aggregate_old(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_aggregate_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let sums = by_key.aggregate_linear_persistent(Some("sums_agg"), |v| *v as i64);
    sums.set_persistent_id(Some("sums"));
    let out_new = sums
        .map(|(k, s)| Tup2(*k, *s))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_aggregate_with_deletions() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(3, -1), Tup2(7, -1)]);
    let mut p2 = weighted_sequence(16, 24, 1);
    p2.push(vec![Tup2(18, -1), Tup2(4, -1)]);
    let mut p3 = weighted_sequence(24, 28, 1);
    p3.push(vec![Tup2(25, -1)]);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, i64>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_aggregate_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: TWO new views sharing a new stateful operator. ---
//
// Both backfill cones run through one new distinct: the cones overlap in
// need_backfill itself, and the cutover must transfer the shared operator's
// state exactly once (the swap is per node, not per view).

#[allow(clippy::type_complexity)]
fn topo_two_new_views_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let nd_in = input.map(|x| x % 23);
    nd_in.set_persistent_id(Some("nd_in"));
    let nd = nd_in.distinct();
    nd.set_persistent_id(Some("nd"));
    let out_new1 = nd.accumulate_output_persistent(Some("out_new1"));
    let out_new2 = nd
        .map(|x| x * 10)
        .accumulate_output_persistent(Some("out_new2"));
    (ih, out_old, (out_new1, out_new2))
}

#[test]
fn test_concurrent_two_new_views_share_operator() {
    let p1 = weighted_sequence(0, 20, 1);
    let p2 = weighted_sequence(20, 30, 1);
    let p3 = weighted_sequence(30, 36, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData2<u64, u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_two_new_views_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Topology: a view is REMOVED while another is added. ---
//
// The dropped view's checkpoint files become orphans; the surviving old view
// and the new view must behave exactly as if the dropped view never existed.

fn topo_removed_view_old(
    circuit: &mut RootCircuit,
) -> (ZSetHandle<u64>, OutputHandle<SpineSnapshot<OrdZSet<u64>>>) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));
    // A second view that circuit 2 drops.  Its output handle is discarded;
    // the view still runs and checkpoints.
    let dropped_in = input.map(|x| x % 13);
    dropped_in.set_persistent_id(Some("dropped_in"));
    let dropped = dropped_in.distinct();
    dropped.set_persistent_id(Some("dropped"));
    let _ = dropped.accumulate_output_persistent(Some("out_dropped"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_removed_view_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));
    let d_in = input.map(|x| x % 19);
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_removed_view() {
    let p1 = weighted_sequence(0, 16, 1);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_removed_view_old),
        Arc::new(topo_removed_view_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Unchanged circuit: nothing to bootstrap. ---

#[allow(clippy::type_complexity)]
fn topo_unchanged_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    (),
) {
    let (ih, out_old) = topo_shared_input_old(circuit);
    (ih, out_old, ())
}

#[test]
fn test_concurrent_up_to_date() {
    let p1 = weighted_sequence(0, 16, 1);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, ()>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_shared_input_old),
        Arc::new(topo_unchanged_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::UpToDate,
        |_| {},
    );
}

// --- Fallback: new view over an UNMATERIALIZED table. ---
//
// The backward walk reaches the raw input node (no replay source), so the
// concurrent bootstrap must refuse and fall back; the classic bootstrap then
// completes, the old view stays correct, and the new view computes over
// post-restart data only (an unmaterialized table's history is gone).

#[test]
fn test_concurrent_unmaterialized_table_falls_back() {
    init_test_logger();
    let path = tempfile::tempdir().unwrap().keep();

    let old_constructor = |circuit: &mut RootCircuit| {
        let (input, ih) = circuit.add_input_zset::<u64>();
        input.set_persistent_id(Some("t1"));
        // NOT materialized: no integrate_trace().
        let out_old = input
            .map(|x| x + 5)
            .accumulate_output_persistent(Some("out_old"));
        Ok((ih, out_old))
    };
    let new_constructor = |circuit: &mut RootCircuit| {
        let (input, ih) = circuit.add_input_zset::<u64>();
        input.set_persistent_id(Some("t1"));
        let out_old = input
            .map(|x| x + 5)
            .accumulate_output_persistent(Some("out_old"));
        let d_in = input.map(|x| x % 19);
        d_in.set_persistent_id(Some("d_in"));
        let d = d_in.distinct();
        d.set_persistent_id(Some("d_new"));
        let out_new = d.accumulate_output_persistent(Some("out_new"));
        Ok((ih, out_old, out_new))
    };

    fn read(handle: &OutputHandle<SpineSnapshot<OrdZSet<u64>>>) -> OrdZSet<u64> {
        SpineSnapshot::<OrdZSet<u64>>::concat(&handle.take_from_all()).consolidate()
    }

    let checkpoint = {
        let (mut circuit, (input, out_old)) =
            Runtime::init_circuit(circuit_config(&path), old_constructor).unwrap();
        for x in 0..8u64 {
            input.push(x, 1);
            circuit.transaction().unwrap();
            assert_eq!(read(&out_old), zset! { x + 5 => 1 });
        }
        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    let (mut circuit, (input, out_old, out_new)) =
        Runtime::init_circuit(circuit_config(&path), new_constructor).unwrap();
    let outcome = circuit
        .start_concurrent_bootstrap(checkpoint.uuid.to_string().into())
        .unwrap();
    let crate::circuit::ConcurrentRestoreOutcome::FellBack { reason, .. } = &outcome else {
        panic!("expected fallback, got {outcome:?}");
    };
    assert!(
        reason.contains("no replay source"),
        "unexpected reason: {reason}"
    );

    while circuit.bootstrap_in_progress() {
        circuit.transaction().unwrap();
    }
    let _ = read(&out_old);
    let _ = read(&out_new);

    // Old view live; new view sees post-restart data only.
    input.push(100, 1);
    circuit.transaction().unwrap();
    assert_eq!(read(&out_old), zset! { 105 => 1 });
    assert_eq!(read(&out_new), zset! { 100 % 19 => 1 });

    // A value whose residue was seen before the restart is NOT suppressed:
    // the unmaterialized history is gone.
    input.push(3, 1);
    circuit.transaction().unwrap();
    assert_eq!(read(&out_old), zset! { 8 => 1 });
    assert_eq!(read(&out_new), zset! { 3 => 1 });

    circuit.kill().unwrap();
}

// --- Abort: destroy the bootstrap circuit instead of cutting over. ---

#[test]
fn test_concurrent_bootstrap_abort() {
    init_test_logger();
    let path = tempfile::tempdir().unwrap().keep();

    fn read(handle: &OutputHandle<SpineSnapshot<OrdZSet<u64>>>) -> OrdZSet<u64> {
        SpineSnapshot::<OrdZSet<u64>>::concat(&handle.take_from_all()).consolidate()
    }

    let checkpoint = {
        let (mut circuit, (input, out_old)) =
            Runtime::init_circuit(circuit_config(&path), |c| Ok(topo_shared_distinct_old(c)))
                .unwrap();
        for x in 0..12u64 {
            input.push(x, 1);
            circuit.transaction().unwrap();
            let _ = read(&out_old);
        }
        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    let (mut circuit, (input, out_old, out_new)) =
        Runtime::init_circuit(circuit_config(&path), |c| Ok(topo_shared_distinct_new(c))).unwrap();
    let outcome = circuit
        .start_concurrent_bootstrap(checkpoint.uuid.to_string().into())
        .unwrap();
    assert!(matches!(
        outcome,
        crate::circuit::ConcurrentRestoreOutcome::Concurrent(_)
    ));

    // Run the backfill to completion, with the old view live throughout.
    // The number of pumps varies with scheduling, so feed a bounded number
    // of fresh values (whose residues cannot collide with earlier ones) and
    // finish pumping without input.
    let mut done = false;
    for x in 12..20u64 {
        input.push(x, 1);
        circuit.transaction().unwrap();
        assert_eq!(read(&out_old), zset! { x % 41 => 1 });
        if !done {
            done = circuit.step_bootstrap_circuit().unwrap();
        }
    }
    while !done {
        done = circuit.step_bootstrap_circuit().unwrap();
    }

    // Abort instead of synchronizing.
    circuit.destroy_bootstrap_circuit().unwrap();

    // The old view keeps working; the new view stays silent; checkpoints are
    // refused (they would record the backfilled nodes as legitimately
    // empty).
    // 1023 % 41 == 39, a residue no earlier input produced (only values
    // 0..20 were inserted), so the distinct emits it.
    input.push(1023, 1);
    circuit.transaction().unwrap();
    assert_eq!(read(&out_old), zset! { 1023 % 41 => 1 });
    assert_eq!(read(&out_new), zset! {});
    assert!(circuit.checkpoint().run().is_err());

    circuit.kill().unwrap();
}

// --- Upsert-backed (map) input table. ---
//
// Map inputs go through the input_upsert machinery, whose integrals register
// their own replay sources and recorders; this covers that attachment path
// end to end, including updates applied through the patch function while the
// recorder is live.

#[test]
fn test_concurrent_bootstrap_map_input() {
    use crate::operator::input::Update;

    init_test_logger();
    let path = tempfile::tempdir().unwrap().keep();

    fn read(handle: &OutputHandle<SpineSnapshot<OrdZSet<u64>>>) -> OrdZSet<u64> {
        SpineSnapshot::<OrdZSet<u64>>::concat(&handle.take_from_all()).consolidate()
    }

    let old_constructor = |circuit: &mut RootCircuit| {
        let (input, ih) =
            circuit.add_input_map_persistent::<u64, u64, u64, _>(Some("t1"), |v, u| *v = *u);
        input.set_persistent_id(Some("t1"));
        input.integrate_trace();
        let out_old = input
            .map(|(_k, v)| *v + 5)
            .accumulate_output_persistent(Some("out_old"));
        Ok((ih, out_old))
    };
    let new_constructor = |circuit: &mut RootCircuit| {
        let (input, ih) =
            circuit.add_input_map_persistent::<u64, u64, u64, _>(Some("t1"), |v, u| *v = *u);
        input.set_persistent_id(Some("t1"));
        input.integrate_trace();
        let out_old = input
            .map(|(_k, v)| *v + 5)
            .accumulate_output_persistent(Some("out_old"));
        let d_in = input.map(|(_k, v)| *v % 7);
        d_in.set_persistent_id(Some("d_in"));
        let d = d_in.distinct();
        d.set_persistent_id(Some("d_new"));
        let out_new = d.accumulate_output_persistent(Some("out_new"));
        Ok((ih, out_old, out_new))
    };

    let checkpoint = {
        let (mut circuit, (input, out_old)) =
            Runtime::init_circuit(circuit_config(&path), old_constructor).unwrap();
        for x in 0..12u64 {
            input.push(x, Update::Insert(x));
            circuit.transaction().unwrap();
            assert_eq!(read(&out_old), zset! { x + 5 => 1 });
        }
        let checkpoint = circuit.checkpoint().run().unwrap();
        circuit.kill().unwrap();
        checkpoint
    };

    let (mut circuit, (input, out_old, out_new)) =
        Runtime::init_circuit(circuit_config(&path), new_constructor).unwrap();
    let outcome = circuit
        .start_concurrent_bootstrap(checkpoint.uuid.to_string().into())
        .unwrap();
    let crate::circuit::ConcurrentRestoreOutcome::Concurrent(_) = outcome else {
        panic!("expected concurrent bootstrap, got {outcome:?}");
    };

    // Old view live during the backfill; record map changes (including a
    // deletion) for the synchronization transaction.
    let mut done = false;
    for x in 12..18u64 {
        input.push(x, Update::Insert(x));
        circuit.transaction().unwrap();
        assert_eq!(read(&out_old), zset! { x + 5 => 1 });
        assert_eq!(read(&out_new), zset! {});
        if !done {
            done = circuit.step_bootstrap_circuit().unwrap();
        }
    }
    input.push(13, Update::Delete); // delete key 13 (value 13)
    circuit.transaction().unwrap();
    assert_eq!(read(&out_old), zset! { 18 => -1 });
    while !done {
        done = circuit.step_bootstrap_circuit().unwrap();
    }

    circuit.sync_concurrent_bootstrap().unwrap();
    while !circuit.step_bootstrap_circuit().unwrap() {}
    circuit.complete_concurrent_bootstrap().unwrap();

    // Residue 4 is contributed by the surviving values 4, 11, and 18.
    // Re-inserting another residue-4 value must be suppressed.
    input.push(18, Update::Insert(18));
    circuit.transaction().unwrap();
    assert_eq!(read(&out_old), zset! { 23 => 1 });
    assert_eq!(read(&out_new), zset! {});

    // Updating key 11 through the PATCH function: value 11 -> 25.  The old
    // view sees the value change; the new view is unaffected (25 % 7 == 4
    // replaces 11 % 7 == 4).
    input.push(11, Update::Update(25));
    circuit.transaction().unwrap();
    assert_eq!(read(&out_old), zset! { 16 => -1, 30 => 1 });
    assert_eq!(read(&out_new), zset! {});

    // Deleting the residue-4 contributors one per transaction retracts the
    // residue exactly when the last one disappears: any contributor lost by
    // the checkpoint, the replay, or the synchronization shifts which
    // deletion fires the retraction.
    input.push(4, Update::Delete);
    circuit.transaction().unwrap();
    assert_eq!(read(&out_new), zset! {});
    input.push(18, Update::Delete);
    circuit.transaction().unwrap();
    assert_eq!(read(&out_new), zset! {});
    input.push(11, Update::Delete); // value 25, the last residue-4 contributor
    circuit.transaction().unwrap();
    assert_eq!(read(&out_new), zset! { 4 => -1 });

    circuit.kill().unwrap();
}

// ---------------------------------------------------------------------------
// Operator-coverage round: each test bootstraps a new view built from one
// operator family over materialized inputs, through the standard harness
// (reference equivalence per transaction + full-retraction state probe).
// ---------------------------------------------------------------------------

// --- Non-linear aggregate (Min). ---

#[allow(clippy::type_complexity)]
fn topo_min_aggregate_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let mins = by_key.aggregate_persistent(Some("min_agg"), Min);
    mins.set_persistent_id(Some("mins"));
    let out_new = mins
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_min_aggregate() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(3, -1), Tup2(5, -1)]);
    let mut p2 = weighted_sequence(16, 24, 1);
    p2.push(vec![Tup2(6, -1)]);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, u64>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_min_aggregate_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Chain aggregate (append-only running minimum). ---
//
// Chain aggregates assume monotone inputs, so this test runs without
// deletions and without the retraction probe.

#[allow(clippy::type_complexity)]
fn topo_chain_aggregate_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let chain =
        by_key.chain_aggregate_persistent(Some("chain_min"), |v, _w| *v, |acc, v, _w| acc.min(*v));
    chain.set_persistent_id(Some("chain"));
    let out_new = chain
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_chain_aggregate() {
    let p1 = weighted_sequence(0, 16, 1);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, u64>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_chain_aggregate_new),
        p1,
        p2,
        p3,
        Vec::new(),
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Top-K. ---

#[allow(clippy::type_complexity)]
fn topo_topk_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let topk = by_key.topk_asc_persistent(Some("topk"), 2);
    topk.set_persistent_id(Some("topk_out"));
    let out_new = topk
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_topk() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(2, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, u64>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_topk_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Rank. ---

#[allow(clippy::type_complexity)]
fn topo_rank_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let indexed = input.map_index(|x| (x % 3, Tup2(*x, *x % 5)));
    indexed.set_persistent_id(Some("indexed"));
    let ranked = indexed.rank_custom_order_persistent::<RankValOrd, u64, _, _, _, _>(
        Some("rank"),
        |v| v.0,
        |a, b| a.0.cmp(b),
        |rank, t| Tup3(t.0, t.1, rank),
    );
    ranked.set_persistent_id(Some("ranked"));
    let out_new = ranked
        .map(|(k, t)| Tup4(*k, t.0, t.1, t.2))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_rank() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(7, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<u64>,
        TestData1<Tup4<u64, u64, u64, i64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_rank_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Lag. ---

#[allow(clippy::type_complexity)]
fn topo_lag_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let indexed = input.map_index(|x| (x % 3, *x));
    indexed.set_persistent_id(Some("indexed"));
    let lagged = indexed.lag_custom_order_persistent::<_, _, _, Asc<_>, _>(
        Some("lag"),
        1,
        |x| x.cloned().unwrap_or(0),
        |x, y| Tup2(*x, *y),
    );
    lagged.set_persistent_id(Some("lagged"));
    let out_new = lagged
        .map(|(_k, v)| *v)
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_lag() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(9, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, u64>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_lag_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Partitioned rolling aggregate. ---

#[allow(clippy::type_complexity)]
fn topo_rolling_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let indexed = input.map_index(|x| (*x, *x));
    indexed.set_persistent_id(Some("indexed"));
    let rolling = indexed.partitioned_rolling_aggregate_persistent(
        Some("rolling"),
        |x| (x % 3, *x),
        Min,
        RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
    );
    rolling.set_persistent_id(Some("rolling_out"));
    let out_new = rolling
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_rolling_aggregate() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(8, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<u64>,
        TestData1<Tup2<u64, Option<u64>>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_rolling_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Outer join (covers left- and right-join legs). ---

#[allow(clippy::type_complexity)]
fn topo_outer_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<Tup3<u64, u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.outer_join(
        &t2i,
        |k, a, b| Tup3(*k, *a, *b),
        |k, a| Tup3(*k, *a, u64::MAX),
        |k, b| Tup3(*k, u64::MAX, *b),
    );
    joined.set_persistent_id(Some("joined"));
    let out_new = joined.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), (out1, out2), out_new)
}

#[test]
fn test_concurrent_outer_join() {
    // Unbalanced table sizes leave unmatched rows on both sides, exercising
    // the left and right legs of the outer join.
    let p1 = join_phase(0..12, 100..106)
        .into_iter()
        .chain(join_phase(200..206, 110..116))
        .collect::<Vec<_>>();
    let p2 = join_phase(12..18, 116..122);
    let p3 = join_phase(18..22, 122..126);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData2<u64, u64>,
        TestData1<Tup3<u64, u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_outer_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- As-of join. ---

#[allow(clippy::type_complexity)]
fn topo_asof_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<Tup3<u64, u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));

    // The value doubles as the timestamp: each left row joins the most
    // recent right row (by value) in its key group.
    let t1i = t1.map_index(|x| (x % 4, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 4, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.asof_join::<u64, _, _, _, u64, Tup3<u64, u64, u64>>(
        &t2i,
        |k, v1, v2| Tup3(*k, *v1, v2.copied().unwrap_or(u64::MAX)),
        |v| *v,
        |v| *v,
    );
    joined.set_persistent_id(Some("joined"));
    let out_new = joined.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), (out1, out2), out_new)
}

#[test]
fn test_concurrent_asof_join() {
    let p1 = join_phase(0..12, 100..112);
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData2<u64, u64>,
        TestData1<Tup3<u64, u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_asof_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Star join (three-way, via the root-circuit Match operator). ---

crate::define_star_join!(3);

#[allow(clippy::type_complexity)]
fn topo_three_tables_old(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let (t3, h3) = circuit.add_input_zset::<u64>();
    t3.set_persistent_id(Some("t3"));
    t3.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));
    let out3 = t3.map(|x| x + 3).accumulate_output_persistent(Some("out3"));
    ((h1, h2, h3), (out1, out2, out3))
}

#[allow(clippy::type_complexity)]
fn topo_star_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let (t3, h3) = circuit.add_input_zset::<u64>();
    t3.set_persistent_id(Some("t3"));
    t3.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));
    let out3 = t3.map(|x| x + 3).accumulate_output_persistent(Some("out3"));

    let t1m = t1.map_index(|x| (x % 4, Some(*x)));
    t1m.set_persistent_id(Some("t1m"));
    let t2m = t2.map_index(|x| (x % 4, Some(*x)));
    t2m.set_persistent_id(Some("t2m"));
    let t3m = t3.map_index(|x| (x % 4, Some(*x)));
    t3m.set_persistent_id(Some("t3m"));

    let star = star_join3(&t1m, (&t2m, false), (&t3m, false), |k, v1, v2, v3| {
        Tup4(
            *k,
            v1.unwrap_or(u64::MAX),
            v2.unwrap_or(u64::MAX),
            v3.unwrap_or(u64::MAX),
        )
    });
    star.set_persistent_id(Some("star"));
    let out_new = star.accumulate_output_persistent(Some("out_new"));
    ((h1, h2, h3), (out1, out2, out3), out_new)
}

fn star_phase(
    r1: std::ops::Range<u64>,
    r2: std::ops::Range<u64>,
    r3: std::ops::Range<u64>,
) -> Vec<(
    Vec<Tup2<u64, ZWeight>>,
    Vec<Tup2<u64, ZWeight>>,
    Vec<Tup2<u64, ZWeight>>,
)> {
    r1.zip(r2.zip(r3))
        .map(|(a, (b, c))| (vec![Tup2(a, 1)], vec![Tup2(b, 1)], vec![Tup2(c, 1)]))
        .collect()
}

#[allow(clippy::type_complexity)]
fn negate_star_chunks(
    phases: &[&[(
        Vec<Tup2<u64, ZWeight>>,
        Vec<Tup2<u64, ZWeight>>,
        Vec<Tup2<u64, ZWeight>>,
    )]],
) -> Vec<(
    Vec<Tup2<u64, ZWeight>>,
    Vec<Tup2<u64, ZWeight>>,
    Vec<Tup2<u64, ZWeight>>,
)> {
    phases
        .iter()
        .flat_map(|chunks| chunks.iter())
        .map(|(c1, c2, c3)| {
            (
                c1.iter().map(|Tup2(x, w)| Tup2(*x, -w)).collect(),
                c2.iter().map(|Tup2(x, w)| Tup2(*x, -w)).collect(),
                c3.iter().map(|Tup2(x, w)| Tup2(*x, -w)).collect(),
            )
        })
        .collect()
}

#[test]
fn test_concurrent_star_join() {
    let p1 = star_phase(0..12, 100..112, 200..212);
    let p2 = star_phase(12..18, 112..118, 212..218);
    let p3 = star_phase(18..22, 118..122, 218..222);
    let p4 = negate_star_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData3<u64, u64, u64>,
        TestData3<u64, u64, u64>,
        TestData1<Tup4<u64, u64, u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_three_tables_old),
        Arc::new(topo_star_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Window with bounds derived from a materialized table. ---
//
// The window's upper bound is the waterline (least upper bound) of a second
// materialized table, so the backward walk from the new view runs through
// the waterline machinery to that table's integral and concurrent
// bootstrapping is possible.  The cutover must transfer the Window
// operator's (lo, hi) state and the waterline's Z^-1 alongside the integrals
// for the post-cutover deltas to match the reference.

#[allow(clippy::type_complexity)]
fn topo_window_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
) {
    use crate::typed_batch::TypedBox;

    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));

    // The window admits keys of t1 in [0, waterline(t2) + 1).
    let t2i = t2.map_index(|x| (*x, ()));
    t2i.set_persistent_id(Some("t2i"));
    let wl = t2i.waterline_persistent(Some("wl"), || 0u64, |k, _v| *k, |a, b| *a.max(b));
    let bounds = wl.apply(|hi| (TypedBox::new(0u64), TypedBox::new(**hi + 1)));

    let win_in = t1.map_index(|x| (*x, ()));
    win_in.set_persistent_id(Some("win_in"));
    let windowed = win_in.window((true, false), &bounds);
    windowed.set_persistent_id(Some("windowed"));
    let out_new = windowed
        .map(|(k, _)| *k)
        .accumulate_output_persistent(Some("out_new"));
    ((h1, h2), (out1, out2), out_new)
}

#[test]
fn test_concurrent_window() {
    // t2's values trail t1's, so the window admits each t1 key a few
    // transactions after its insertion -- before, during, and after the
    // bootstrap.
    let p1 = join_phase(0..12, 0..12);
    let p2 = join_phase(12..18, 12..18);
    let p3 = join_phase(18..22, 18..22);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData2<u64, u64>, TestData2<u64, u64>, TestData1<u64>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_window_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Linear aggregate with post-processing (the SQL compiler's AVG shape). ---
//
// The accumulator is a (sum, count) pair and the post-processing step
// divides; the result for every group depends on the full accumulated state,
// so a record lost or duplicated anywhere in the bootstrap shifts the
// averages.

#[allow(clippy::type_complexity)]
fn topo_avg_aggregate_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let out_old = input
        .map(|x| x + 5)
        .accumulate_output_persistent(Some("out_old"));

    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let avgs = by_key.aggregate_linear_postprocess_persistent(
        Some("avg_agg"),
        |v| Tup2(*v as i64, 1i64),
        |Tup2(sum, count)| if count == 0 { 0 } else { sum / count },
    );
    avgs.set_persistent_id(Some("avgs"));
    let out_new = avgs
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_avg_aggregate() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(3, -1), Tup2(10, -1)]);
    let mut p2 = weighted_sequence(16, 24, 1);
    p2.push(vec![Tup2(17, -1)]);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<TestData1<u64>, TestData1<u64>, TestData1<Tup2<u64, i64>>>(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_aggregate_old),
        Arc::new(topo_avg_aggregate_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// --- Left join. ---
//
// The right side is saturated (Option-valued), so unmatched left rows
// produce output with `None`; the unbalanced table sizes keep a population
// of unmatched rows on the left throughout all phases.

#[allow(clippy::type_complexity)]
fn topo_left_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    (
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
        OutputHandle<SpineSnapshot<OrdZSet<u64>>>,
    ),
    OutputHandle<SpineSnapshot<OrdZSet<Tup3<u64, u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();
    let out1 = t1.map(|x| x + 1).accumulate_output_persistent(Some("out1"));
    let out2 = t2.map(|x| x + 2).accumulate_output_persistent(Some("out2"));

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, Some(*x)));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.left_join(&t2i, |k, a, b| Tup3(*k, *a, b.unwrap_or(u64::MAX)));
    joined.set_persistent_id(Some("joined"));
    let out_new = joined.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), (out1, out2), out_new)
}

#[test]
fn test_concurrent_left_join() {
    // The left table is twice the size of the right one, so half its keys
    // have no match and exercise the None leg.
    let p1 = join_phase(0..12, 100..106)
        .into_iter()
        .chain(join_phase(300..306, 106..112))
        .collect::<Vec<_>>();
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData2<u64, u64>,
        TestData1<Tup3<u64, u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_join_old),
        Arc::new(topo_left_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |_| {},
    );
}

// ============================================================================
// New views DOWNSTREAM of a pre-existing operator.
//
// The tests above add each operator type to the NEW circuit and bootstrap it
// from upstream integrals.  The tests below flip the roles: the operator is
// part of the OLD circuit (present in the checkpoint), and the new view
// consumes its output.  Two distinct behaviors are possible, depending on
// whether the operator's output stream has a replay source:
//
// 1. The operator maintains an integral of its output and registers it as a
//    replay source.  Every stateful unary operator does: aggregates retract
//    previous outputs through an upsert whose trace registers a replay
//    source; group operators (top-k, rank, lag), rolling aggregates, and
//    chain aggregates keep an output trace that registers one.  A join's
//    output gains a replay source only through an explicit
//    `integrate_trace()` (the materialized-join test).  The backward walk
//    stops at that integral: the new view backfills from the operator's own
//    output, and the operator runs only in the main circuit, where a
//    recorder captures its output deltas.
//
// 2. The operator's output has no replay source -- in practice joins (and
//    stateless transforms), whose outputs are not integrated.  The walk
//    passes through the operator to the integrals upstream of it, and the
//    operator is re-evaluated as part of the backfill: it runs in BOTH
//    copies -- live in the main circuit and replaying in the bootstrap
//    circuit -- with its bootstrap-copy state cleared and rebuilt (then
//    discarded at cutover in favor of the live copy's state).
//
// Which case applies is asserted through `BootstrapInfo::need_backfill`
// (`participate_in_backfill` minus the replay sources): in case 2 the
// operator's persistent id appears in it; in case 1 it must not.
// ============================================================================

/// True if any persistent id in `info.need_backfill` contains `fragment`.
fn backfill_pids_contain(
    info: &crate::circuit::circuit_builder::BootstrapInfo,
    fragment: &str,
) -> bool {
    info.need_backfill
        .values()
        .flatten()
        .any(|pid| pid.contains(fragment))
}

// --- Downstream of a linear aggregate (case 1: internal output integral). ---

fn topo_downstream_of_aggregate_old(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let sums = by_key.aggregate_linear_persistent(Some("sums_agg"), |v| *v as i64);
    sums.set_persistent_id(Some("sums"));
    let out_old = sums
        .map(|(k, s)| Tup2(*k, *s))
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_aggregate_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, i64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let sums = by_key.aggregate_linear_persistent(Some("sums_agg"), |v| *v as i64);
    sums.set_persistent_id(Some("sums"));
    let out_old = sums
        .map(|(k, s)| Tup2(*k, *s))
        .accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing aggregate.
    let d_in = sums.map(|(k, s)| Tup2(*k % 2, *s));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_aggregate() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(3, -1), Tup2(7, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<Tup2<u64, i64>>,
        TestData1<Tup2<u64, i64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_aggregate_old),
        Arc::new(topo_downstream_of_aggregate_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |info| {
            // The aggregate's own output integral is the replay source; the
            // aggregate and everything upstream of it stay out of the
            // backfill.
            assert_eq!(info.replay_sources.len(), 1);
            assert!(!backfill_pids_contain(info, "sums"));
            assert!(!backfill_pids_contain(info, "by_key"));
        },
    );
}

// --- Downstream of rank (case 1: rank registers its output delta). ---

#[allow(clippy::type_complexity)]
fn topo_downstream_of_rank_old(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let indexed = input.map_index(|x| (x % 3, Tup2(*x, *x % 5)));
    indexed.set_persistent_id(Some("indexed"));
    let ranked = indexed.rank_custom_order_persistent::<RankValOrd, u64, _, _, _, _>(
        Some("rank"),
        |v| v.0,
        |a, b| a.0.cmp(b),
        |rank, t| Tup3(t.0, t.1, rank),
    );
    ranked.set_persistent_id(Some("ranked"));
    let out_old = ranked
        .map(|(k, t)| Tup4(*k, t.0, t.1, t.2))
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_rank_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup4<u64, u64, u64, i64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, i64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let indexed = input.map_index(|x| (x % 3, Tup2(*x, *x % 5)));
    indexed.set_persistent_id(Some("indexed"));
    let ranked = indexed.rank_custom_order_persistent::<RankValOrd, u64, _, _, _, _>(
        Some("rank"),
        |v| v.0,
        |a, b| a.0.cmp(b),
        |rank, t| Tup3(t.0, t.1, rank),
    );
    ranked.set_persistent_id(Some("ranked"));
    let out_old = ranked
        .map(|(k, t)| Tup4(*k, t.0, t.1, t.2))
        .accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing rank.
    let d_in = ranked.map(|(k, t)| Tup2(*k, t.2));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_rank() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(7, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<Tup4<u64, u64, u64, i64>>,
        TestData1<Tup2<u64, i64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_rank_old),
        Arc::new(topo_downstream_of_rank_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |info| {
            // Rank registers a replay source for its internal output delta,
            // which the public output projects through a stateless
            // `map_index`: the rank state replays from its own trace (the
            // walk does not reach the input side), and only the projection
            // node re-runs during the backfill.
            assert_eq!(info.replay_sources.len(), 1);
            assert!(!backfill_pids_contain(info, "indexed"));
            assert!(backfill_pids_contain(info, "ranked"));
        },
    );
}

// --- Downstream of a join whose output is materialized (case 1). ---

#[allow(clippy::type_complexity)]
fn topo_downstream_of_materialized_join_old(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    joined.integrate_trace();
    let out_old = joined.accumulate_output_persistent(Some("out_old"));
    ((h1, h2), out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_materialized_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    joined.integrate_trace();
    let out_old = joined.accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing, materialized join.
    let d_in = joined.map(|t| Tup2(t.0 % 16, t.1 % 16));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_materialized_join() {
    let p1 = join_phase(0..12, 100..112);
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData1<Tup2<u64, u64>>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_materialized_join_old),
        Arc::new(topo_downstream_of_materialized_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |info| {
            // The explicit integral of the join's output is the replay
            // source; the join and its input integrals stay out of the
            // backfill.
            assert_eq!(info.replay_sources.len(), 1);
            assert!(!backfill_pids_contain(info, "joined"));
            assert!(!backfill_pids_contain(info, "t1i"));
            assert!(!backfill_pids_contain(info, "t2i"));
        },
    );
}

// --- Downstream of a join whose output is NOT materialized (case 2). ---

#[allow(clippy::type_complexity)]
fn topo_downstream_of_unmaterialized_join_old(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    let out_old = joined.accumulate_output_persistent(Some("out_old"));
    ((h1, h2), out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_unmaterialized_join_new(
    circuit: &mut RootCircuit,
) -> (
    (ZSetHandle<u64>, ZSetHandle<u64>),
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (t1, h1) = circuit.add_input_zset::<u64>();
    t1.set_persistent_id(Some("t1"));
    t1.integrate_trace();
    let (t2, h2) = circuit.add_input_zset::<u64>();
    t2.set_persistent_id(Some("t2"));
    t2.integrate_trace();

    let t1i = t1.map_index(|x| (x % 8, *x));
    t1i.set_persistent_id(Some("t1i"));
    let t2i = t2.map_index(|x| (x % 8, *x));
    t2i.set_persistent_id(Some("t2i"));
    let joined = t1i.join(&t2i, |_k, a, b| Tup2(*a, *b));
    joined.set_persistent_id(Some("joined"));
    let out_old = joined.accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing, unmaterialized join.
    let d_in = joined.map(|t| Tup2(t.0 % 16, t.1 % 16));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    ((h1, h2), out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_unmaterialized_join() {
    let p1 = join_phase(0..12, 100..112);
    let p2 = join_phase(12..18, 112..118);
    let p3 = join_phase(18..22, 118..122);
    let p4 = negate_join_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData2<u64, u64>,
        TestData1<Tup2<u64, u64>>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_unmaterialized_join_old),
        Arc::new(topo_downstream_of_unmaterialized_join_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |info| {
            // The join's output has no replay source, so the walk continues
            // to the join's two input integrals, and the join itself is
            // re-evaluated as part of the backfill.
            assert_eq!(info.replay_sources.len(), 2);
            assert!(backfill_pids_contain(info, "joined"));
        },
    );
}

// --- Downstream of a partitioned rolling aggregate (case 1: its output
// trace registers a replay source). ---

#[allow(clippy::type_complexity)]
fn topo_downstream_of_rolling_old(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let indexed = input.map_index(|x| (*x, *x));
    indexed.set_persistent_id(Some("indexed"));
    let rolling = indexed.partitioned_rolling_aggregate_persistent(
        Some("rolling"),
        |x| (x % 3, *x),
        Min,
        RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
    );
    rolling.set_persistent_id(Some("rolling_out"));
    let out_old = rolling
        .map_index(|(k, v)| (*k, *v))
        .map(|(_k, v)| *v)
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_rolling_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, Option<u64>>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let indexed = input.map_index(|x| (*x, *x));
    indexed.set_persistent_id(Some("indexed"));
    let rolling = indexed.partitioned_rolling_aggregate_persistent(
        Some("rolling"),
        |x| (x % 3, *x),
        Min,
        RelRange::new(RelOffset::Before(10), RelOffset::After(0)),
    );
    rolling.set_persistent_id(Some("rolling_out"));
    let flat = rolling.map_index(|(k, v)| (*k, *v)).map(|(_k, v)| *v);
    flat.set_persistent_id(Some("flat"));
    let out_old = flat.accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing rolling aggregate.
    let d_in = flat.map(|t| Tup2(t.0 % 7, t.1.unwrap_or(0)));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_rolling() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(8, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<Tup2<u64, Option<u64>>>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_rolling_old),
        Arc::new(topo_downstream_of_rolling_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |info| {
            // The rolling aggregate's output trace is the replay source; the
            // operator and everything upstream of it stay out of the
            // backfill.
            assert_eq!(info.replay_sources.len(), 1);
            assert!(!backfill_pids_contain(info, "rolling"));
            assert!(!backfill_pids_contain(info, "indexed"));
        },
    );
}

// --- Downstream of top-k (case 1: like all group operators, top-k keeps an
// output trace that registers a replay source). ---

#[allow(clippy::type_complexity)]
fn topo_downstream_of_topk_old(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let topk = by_key.topk_asc_persistent(Some("topk"), 2);
    topk.set_persistent_id(Some("topk_out"));
    let out_old = topk
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_topk_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let topk = by_key.topk_asc_persistent(Some("topk"), 2);
    topk.set_persistent_id(Some("topk_out"));
    let out_old = topk
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing top-k.
    let d_in = topk.map(|(k, v)| Tup2(*k, *v % 9));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_topk() {
    let mut p1 = weighted_sequence(0, 16, 1);
    p1.push(vec![Tup2(2, -1)]);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    let p4 = negate_chunks(&[&p1, &p2, &p3]);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<Tup2<u64, u64>>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_topk_old),
        Arc::new(topo_downstream_of_topk_new),
        p1,
        p2,
        p3,
        p4,
        ExpectedOutcome::Concurrent,
        |info| {
            // Top-k's output trace is the replay source; the operator and
            // everything upstream of it stay out of the backfill.
            assert_eq!(info.replay_sources.len(), 1);
            assert!(!backfill_pids_contain(info, "topk"));
            assert!(!backfill_pids_contain(info, "by_key"));
        },
    );
}

// --- Downstream of a chain aggregate (case 1: its output trace registers a
// replay source; append-only input). ---

#[allow(clippy::type_complexity)]
fn topo_downstream_of_chain_old(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let chain =
        by_key.chain_aggregate_persistent(Some("chain_min"), |v, _w| *v, |acc, v, _w| acc.min(*v));
    chain.set_persistent_id(Some("chain"));
    let out_old = chain
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_old"));
    (ih, out_old)
}

#[allow(clippy::type_complexity)]
fn topo_downstream_of_chain_new(
    circuit: &mut RootCircuit,
) -> (
    ZSetHandle<u64>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
    OutputHandle<SpineSnapshot<OrdZSet<Tup2<u64, u64>>>>,
) {
    let (input, ih) = circuit.add_input_zset::<u64>();
    input.set_persistent_id(Some("t1"));
    input.integrate_trace();
    let by_key = input.map_index(|x| (x % 5, *x));
    by_key.set_persistent_id(Some("by_key"));
    let chain =
        by_key.chain_aggregate_persistent(Some("chain_min"), |v, _w| *v, |acc, v, _w| acc.min(*v));
    chain.set_persistent_id(Some("chain"));
    let out_old = chain
        .map(|(k, v)| Tup2(*k, *v))
        .accumulate_output_persistent(Some("out_old"));

    // New view downstream of the pre-existing chain aggregate.
    let d_in = chain.map(|(k, v)| Tup2(*k, *v % 3));
    d_in.set_persistent_id(Some("d_in"));
    let d = d_in.distinct();
    d.set_persistent_id(Some("d_new"));
    let out_new = d.accumulate_output_persistent(Some("out_new"));
    (ih, out_old, out_new)
}

#[test]
fn test_concurrent_downstream_of_chain_aggregate() {
    // Chain aggregates require append-only input: no retraction phase.
    let p1 = weighted_sequence(0, 16, 1);
    let p2 = weighted_sequence(16, 24, 1);
    let p3 = weighted_sequence(24, 28, 1);
    run_concurrent_bootstrap_test::<
        TestData1<u64>,
        TestData1<Tup2<u64, u64>>,
        TestData1<Tup2<u64, u64>>,
    >(
        NUM_WORKERS,
        |config| config,
        Arc::new(topo_downstream_of_chain_old),
        Arc::new(topo_downstream_of_chain_new),
        p1,
        p2,
        p3,
        Vec::new(),
        ExpectedOutcome::Concurrent,
        |info| {
            // The chain aggregate's output trace is the replay source; the
            // operator and everything upstream of it stay out of the
            // backfill.
            assert_eq!(info.replay_sources.len(), 1);
            assert!(!backfill_pids_contain(info, "chain"));
            assert!(!backfill_pids_contain(info, "by_key"));
        },
    );
}
