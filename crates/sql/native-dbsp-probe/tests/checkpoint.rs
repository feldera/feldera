//! Checkpoint lifecycle for the prototype's existing BIGINT/Vec representation.
//! This does not compare generated SQL tuple archives with a native row encoder.

use std::path::Path;

use dbsp::{
    DBSPHandle, OrdZSet, OutputHandle, Runtime, ZSetHandle,
    circuit::{CircuitConfig, CircuitStorageConfig},
    typed_batch::IndexedZSetReader,
    utils::Tup2,
};
use feldera_native_dbsp_probe::{Projection, Row};
use feldera_native_probe::{Arithmetic, Expr, Program};
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    size::ConfigSize,
};

struct JoinCircuit {
    runtime: DBSPHandle,
    left: ZSetHandle<Row>,
    right: ZSetHandle<Row>,
    output: OutputHandle<OrdZSet<Row>>,
}

impl JoinCircuit {
    fn new(config: CircuitConfig, native: bool) -> Self {
        let (runtime, (left, right, output)) = Runtime::init_circuit(config, move |circuit| {
            let projection = native.then(|| {
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
            });
            let (left, left_handle) = circuit.add_input_zset::<Row>();
            let (right, right_handle) = circuit.add_input_zset::<Row>();
            // Keep operator construction identical: only expression evaluation
            // changes across restore. Rust owns the state representation in
            // both versions; archive-format equivalence is a separate N2 test.
            let projected = left.apply(move |batch| match &projection {
                Some(projection) => projection.evaluate(batch).unwrap(),
                None => rust_projection(batch),
            });
            let keyed = projected
                .filter(|row| row[0].is_some())
                .map_index(|row| (row[0].unwrap(), row[3]));
            let other = right
                .filter(|row| row[0].is_some())
                .map_index(|row| (row[0].unwrap(), row[1]));
            let output = keyed
                .join(&other, |key, left, right| vec![Some(*key), *left, *right])
                .output();
            Ok((left_handle, right_handle, output))
        })
        .unwrap();
        Self {
            runtime,
            left,
            right,
            output,
        }
    }

    fn step(
        &mut self,
        mut left: Vec<Tup2<Row, i64>>,
        mut right: Vec<Tup2<Row, i64>>,
    ) -> OrdZSet<Row> {
        self.left.append(&mut left);
        self.right.append(&mut right);
        self.runtime.transaction().unwrap();
        self.output.consolidate()
    }

    fn stop(self) {
        self.runtime.kill().unwrap();
    }
}

fn rust_projection(input: &OrdZSet<Row>) -> OrdZSet<Row> {
    OrdZSet::from_keys(
        (),
        input
            .iter()
            .map(|(mut row, (), weight)| {
                assert_eq!(row.len(), 3);
                let sum = match (row[1], row[2]) {
                    (Some(left), Some(right)) => Some(left.checked_add(right).unwrap()),
                    _ => None,
                };
                row.push(sum);
                Tup2(row, weight)
            })
            .collect(),
    )
}

fn storage(path: &Path) -> CircuitConfig {
    CircuitConfig::with_workers(1).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: path.to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                min_spill_size: Some(ConfigSize::ZERO),
                min_step_spill_size: Some(ConfigSize::ZERO),
                ..StorageOptions::default()
            },
        )
        .unwrap(),
    ))
}

#[test]
fn native_projection_restores_rust_evaluated_join_state_and_retractions() {
    let reference_directory = tempfile::tempdir().unwrap();
    let checkpoint_directory = tempfile::tempdir().unwrap();
    let mut reference = JoinCircuit::new(storage(reference_directory.path()), false);
    let mut original = JoinCircuit::new(storage(checkpoint_directory.path()), false);
    let initial_left = vec![
        Tup2(vec![Some(1), Some(2), Some(3)], 2),
        Tup2(vec![Some(2), None, Some(7)], 1),
        Tup2(vec![None, Some(8), Some(9)], 1),
    ];
    let initial_right = vec![
        Tup2(vec![Some(1), Some(10)], 3),
        Tup2(vec![Some(2), Some(20)], 1),
    ];
    let expected = OrdZSet::from_keys(
        (),
        vec![
            Tup2(vec![Some(1), Some(5), Some(10)], 6),
            Tup2(vec![Some(2), None, Some(20)], 1),
        ],
    );
    assert_eq!(
        reference.step(initial_left.clone(), initial_right.clone()),
        expected
    );
    assert_eq!(original.step(initial_left, initial_right), expected);
    let checkpoint = original.runtime.checkpoint().run().unwrap();
    original.stop();

    let mut config = storage(checkpoint_directory.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let mut restored = JoinCircuit::new(config, true);
    let steps = [
        (vec![], vec![], vec![]),
        (
            vec![],
            vec![Tup2(vec![Some(1), Some(10)], -1)],
            vec![Tup2(vec![Some(1), Some(5), Some(10)], -2)],
        ),
        (
            vec![
                Tup2(vec![Some(1), Some(2), Some(3)], -1),
                Tup2(vec![Some(3), Some(4), Some(6)], 1),
            ],
            vec![Tup2(vec![Some(3), Some(30)], 2)],
            vec![
                Tup2(vec![Some(1), Some(5), Some(10)], -2),
                Tup2(vec![Some(3), Some(10), Some(30)], 2),
            ],
        ),
        (
            vec![Tup2(vec![Some(1), Some(2), Some(3)], -1)],
            vec![Tup2(vec![Some(3), Some(30)], -1)],
            vec![
                Tup2(vec![Some(1), Some(5), Some(10)], -2),
                Tup2(vec![Some(3), Some(10), Some(30)], -1),
            ],
        ),
        (
            vec![],
            vec![Tup2(vec![Some(2), Some(20)], -1)],
            vec![Tup2(vec![Some(2), None, Some(20)], -1)],
        ),
    ];
    for (index, (left, right, expected)) in steps.into_iter().enumerate() {
        let expected = OrdZSet::from_keys((), expected);
        assert_eq!(
            reference.step(left.clone(), right.clone()),
            expected,
            "Rust step {index}"
        );
        assert_eq!(
            restored.step(left, right),
            expected,
            "restored native step {index}"
        );
    }
    restored.stop();
    reference.stop();
}
