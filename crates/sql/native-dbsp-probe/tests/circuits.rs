use dbsp::{
    Circuit, OrdZSet, Runtime,
    operator::{ConstantGenerator, IncrementalConstantGenerator},
    typed_batch::IndexedZSetReader,
    utils::Tup2,
};
use feldera_native_dbsp_probe::{Projection, Row};
use feldera_native_probe::{Arithmetic, Error, Expr, Program};

fn projection() -> Projection {
    Projection::compile(
        3,
        &Program::new(
            3,
            vec![
                Expr::Column(1),
                Expr::Column(2),
                Expr::Arithmetic {
                    op: Arithmetic::Add,
                    left: 0,
                    right: 1,
                },
            ],
        )
        .unwrap(),
    )
    .unwrap()
}

fn zset(rows: Vec<(Row, i64)>) -> OrdZSet<Row> {
    OrdZSet::from_keys(
        (),
        rows.into_iter()
            .map(|(row, weight)| Tup2(row, weight))
            .collect(),
    )
}

#[test]
fn generic_negation_sum_and_sink_accept_erased_batches() {
    let (mut runtime, (input, erased_output, erased_accumulated, output)) =
        Runtime::init_circuit(1, |circuit| {
            let (stream, input) = circuit.add_input_zset::<i64>();
            let stream = stream.inner();
            let negated = stream.neg();
            let sum = negated.sum([&negated, &stream]);
            let factories = dbsp::trace::BatchReaderFactories::new::<i64, (), i64>();
            Ok((
                input,
                sum.output(),
                sum.dyn_accumulate_output_persistent(&factories, None),
                sum.typed::<OrdZSet<i64>>().accumulate_output(),
            ))
        })
        .unwrap();

    input.append(&mut vec![Tup2(7, 2), Tup2(9, -3)]);
    runtime.transaction().unwrap();
    let expected = dbsp::zset!(7_i64 => -2, 9_i64 => 3);
    assert_eq!(feldera_sqllib::read_output_spine(&output), expected);
    assert_eq!(
        erased_accumulated
            .take_from_worker(0)
            .unwrap()
            .consolidate(),
        *expected,
    );
    assert_eq!(
        erased_output.take_from_worker(0).unwrap(),
        expected.into_inner()
    );
    runtime.kill().unwrap();
}

#[test]
fn erased_accumulated_output_publishes_the_complete_transaction() {
    use dbsp::{
        algebra::OrdZSet as ErasedZSet,
        dynamic::DynData,
        trace::{BatchReader, BatchReaderFactories, SpineSnapshot},
    };

    let (mut runtime, (input, output)) = Runtime::init_circuit(2, |circuit| {
        let (stream, input) = circuit.add_input_zset::<i64>();
        let factories = BatchReaderFactories::new::<i64, (), i64>();
        let output = stream
            .inner()
            .dyn_accumulate_output_persistent(&factories, Some("output"));
        Ok((input, output))
    })
    .unwrap();
    let factories = <ErasedZSet<DynData> as BatchReader>::Factories::new::<i64, (), i64>();

    for weight in [1, -1] {
        input.append(&mut vec![Tup2(7, weight * 2), Tup2(9, weight * -3)]);
        runtime.transaction().unwrap();
        let outputs = output.take_from_all();
        assert_eq!(outputs.len(), 2);
        let actual = SpineSnapshot::concat(factories.clone(), outputs.iter()).consolidate();
        assert_eq!(
            actual,
            dbsp::zset!(7_i64 => weight * 2, 9_i64 => weight * -3).into_inner()
        );
    }
    runtime.kill().unwrap();
}

#[test]
fn constants_can_produce_erased_batches_with_explicit_empty_values() {
    let expected = dbsp::zset!(7_i64 => 2, 9_i64 => -3).into_inner();
    let empty = OrdZSet::<i64>::default().into_inner();
    let (mut runtime, (constant, incremental)) = Runtime::init_circuit(2, |circuit| {
        let value = dbsp::zset!(7_i64 => 2, 9_i64 => -3).into_inner();
        let empty = OrdZSet::<i64>::default().into_inner();
        let constant = circuit
            .add_source(ConstantGenerator::with_empty(value.clone(), {
                let empty = empty.clone();
                move || empty.clone()
            }))
            .output();
        let incremental = circuit
            .add_source(IncrementalConstantGenerator::with_empty(value, move || {
                empty.clone()
            }))
            .output();
        Ok((constant, incremental))
    })
    .unwrap();

    for first in [true, false] {
        runtime.transaction().unwrap();
        assert_eq!(constant.take_from_worker(0).unwrap(), expected);
        assert_eq!(constant.take_from_worker(1).unwrap(), empty);
        assert_eq!(
            incremental.take_from_worker(0).as_ref(),
            Some(if first { &expected } else { &empty })
        );
        assert_eq!(incremental.take_from_worker(1).unwrap(), empty);
    }
    runtime.kill().unwrap();
}

#[test]
fn constant_and_sink_checkpoint_interchanges_between_batch_representations() {
    use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, Mode};
    use feldera_types::config::{StorageConfig, StorageOptions};

    let directory = tempfile::tempdir().unwrap();
    let config = |checkpoint| {
        let mut storage = CircuitStorageConfig::for_config(
            StorageConfig {
                path: directory.path().to_string_lossy().into_owned(),
                cache: Default::default(),
            },
            StorageOptions::default(),
        )
        .unwrap();
        storage.init_checkpoint = checkpoint;
        CircuitConfig::with_workers(2)
            .with_mode(Mode::Persistent)
            .with_storage(Some(storage))
    };
    let classic = |circuit: &mut dbsp::RootCircuit| {
        let value = dbsp::zset!(7_i64 => 2, 9_i64 => -3);
        let stream = circuit.add_source(IncrementalConstantGenerator::new(value));
        stream.set_persistent_id(Some("constant"));
        Ok(stream.accumulate_output_persistent(Some("output")))
    };
    let erased = |circuit: &mut dbsp::RootCircuit| {
        let value = dbsp::zset!(7_i64 => 2, 9_i64 => -3).into_inner();
        let empty = OrdZSet::<i64>::default().into_inner();
        let stream = circuit
            .add_source(IncrementalConstantGenerator::with_empty(value, move || {
                empty.clone()
            }));
        stream.set_persistent_id(Some("constant"));
        let factories = dbsp::trace::BatchReaderFactories::new::<i64, (), i64>();
        Ok(stream.dyn_accumulate_output_persistent(&factories, Some("output")))
    };

    let (mut runtime, output) = Runtime::init_circuit(config(None), classic).unwrap();
    runtime.transaction().unwrap();
    assert_eq!(
        feldera_sqllib::read_output_spine(&output),
        dbsp::zset!(7_i64 => 2, 9_i64 => -3)
    );
    let checkpoint = runtime.checkpoint().run().unwrap();
    runtime.kill().unwrap();

    let (mut runtime, output) =
        Runtime::init_circuit(config(Some(checkpoint.uuid)), erased).unwrap();
    runtime.transaction().unwrap();
    let empty = OrdZSet::<i64>::default();
    let snapshots = output.take_from_all();
    assert_eq!(snapshots.len(), 2);
    for snapshot in snapshots {
        assert_eq!(snapshot.consolidate(), *empty);
    }
    let checkpoint = runtime.checkpoint().run().unwrap();
    runtime.kill().unwrap();

    let (mut runtime, output) =
        Runtime::init_circuit(config(Some(checkpoint.uuid)), classic).unwrap();
    runtime.transaction().unwrap();
    assert_eq!(feldera_sqllib::read_output_spine(&output), empty);
    runtime.kill().unwrap();
}

#[test]
fn native_batch_preserves_nulls_signed_weights_and_runtime_row_width() {
    fn accepts_dynamic_data(_: &dyn dbsp::dynamic::Data) {}
    accepts_dynamic_data(&vec![Some(1i64), None]);
    let projection = projection();
    let rows = vec![
        (vec![Some(1), Some(2), Some(3)], 2),
        (vec![Some(2), None, Some(4)], -1),
    ];
    assert_eq!(
        projection.evaluate(&zset(rows)).unwrap(),
        zset(vec![
            (vec![Some(1), Some(2), Some(3), Some(5)], 2),
            (vec![Some(2), None, Some(4), None], -1)
        ])
    );
    assert!(
        projection
            .evaluate(&zset(vec![(vec![Some(1)], 1)]))
            .is_err()
    );
    assert_eq!(
        projection
            .evaluate(&zset(vec![(vec![Some(1), Some(i64::MAX), Some(1)], 1)]))
            .unwrap_err(),
        Error::Overflow {
            row: 0,
            op: Arithmetic::Add,
            left: i64::MAX,
            right: 1
        }
    );
}

#[test]
fn batch_projection_feeds_incremental_join_and_sum_with_retractions() {
    let (mut circuit, (left, right, joined, summed)) = Runtime::init_circuit(1, move |circuit| {
        let projection = projection();
        let (left, left_handle) = circuit.add_input_zset::<Row>();
        let (right, right_handle) = circuit.add_input_zset::<Row>();
        // An unexpected native failure fails this test circuit immediately.
        // Production integration still needs transactional error propagation.
        let projected =
            left.apply(move |batch| projection.evaluate(batch).expect("native batch evaluation"));
        let keyed = projected
            .filter(|row| row[0].is_some())
            .map_index(|row| (row[0].unwrap(), row[3]));
        let other = right
            .filter(|row| row[0].is_some())
            .map_index(|row| (row[0].unwrap(), row[1]));
        let joined = keyed
            .join(&other, |key, left, right| vec![Some(*key), *left, *right])
            .output();
        // This tests DBSP linear sums, which omit zero totals; SQL SUM needs
        // a separate group-presence/NULL contract before C7 can be claimed.
        let summed = keyed.aggregate_linear(|value| value.unwrap_or(0)).output();
        Ok((left_handle, right_handle, joined, summed))
    })
    .unwrap();
    left.append(&mut vec![
        Tup2(vec![Some(1), Some(2), Some(3)], 2),
        Tup2(vec![Some(2), None, Some(7)], 1),
        Tup2(vec![None, Some(8), Some(9)], 1),
    ]);
    right.append(&mut vec![
        Tup2(vec![Some(1), Some(10)], 3),
        Tup2(vec![Some(2), Some(20)], 1),
    ]);
    circuit.transaction().unwrap();
    assert_eq!(
        joined.consolidate(),
        zset(vec![
            (vec![Some(1), Some(5), Some(10)], 6),
            (vec![Some(2), None, Some(20)], 1)
        ])
    );
    let sums: Vec<_> = summed.consolidate().iter().collect();
    assert_eq!(sums, vec![(1, 10, 1)]);
    left.append(&mut vec![
        Tup2(vec![Some(1), Some(2), Some(3)], -1),
        Tup2(vec![Some(2), None, Some(7)], -1),
    ]);
    circuit.transaction().unwrap();
    assert_eq!(
        joined.consolidate(),
        zset(vec![
            (vec![Some(1), Some(5), Some(10)], -3),
            (vec![Some(2), None, Some(20)], -1)
        ])
    );
    assert_eq!(
        summed.consolidate().iter().collect::<Vec<_>>(),
        vec![(1, 5, 1), (1, 10, -1)]
    );
    right.append(&mut vec![Tup2(vec![Some(1), Some(10)], -3)]);
    circuit.transaction().unwrap();
    assert_eq!(
        joined.consolidate(),
        zset(vec![(vec![Some(1), Some(5), Some(10)], -3)])
    );
    assert!(summed.consolidate().iter().collect::<Vec<_>>().is_empty());
    circuit.kill().unwrap();
}

#[test]
fn native_chunks_preserve_the_last_row_and_global_overflow_position() {
    let projection = projection();
    let mut rows: Vec<_> = (0..65_537)
        .map(|key| (vec![Some(key), Some(1), Some(2)], 1))
        .collect();
    let output = projection.evaluate(&zset(rows.clone())).unwrap();
    let projected: Vec<_> = output.iter().collect();
    assert_eq!(projected.len(), rows.len());
    assert_eq!(
        projected.last().unwrap(),
        &(vec![Some(65_536), Some(1), Some(2), Some(3)], (), 1)
    );
    rows.last_mut().unwrap().0 = vec![Some(65_536), Some(i64::MAX), Some(1)];
    assert_eq!(
        projection.evaluate(&zset(rows)).unwrap_err(),
        Error::Overflow {
            row: 65_536,
            op: Arithmetic::Add,
            left: i64::MAX,
            right: 1,
        }
    );
}
