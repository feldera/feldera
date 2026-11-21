//! Checkpoint compatibility for the nullable SUM accumulator used as a DBSP weight.
#![cfg(target_os = "linux")]

use dbsp::{
    DBSPHandle, OrdZSet, OutputHandle, Runtime, ZSetHandle,
    algebra::MulByRef,
    circuit::CircuitConfig,
    utils::{Tup2, Tup3},
};
use std::{path::PathBuf, sync::Arc};

#[allow(dead_code)]
#[path = "support/checkpoint.rs"]
mod checkpoint;
mod native_weights;
use checkpoint::{retain_directory, storage};
use native_weights::{NativeCount, NativeSum};

type Sum = Tup3<i64, i64, i64>;
type Row = Tup2<i32, Option<i64>>;
type Change = Tup2<Row, i64>;

struct SumCircuit {
    runtime: DBSPHandle,
    input: ZSetHandle<Row>,
    output: OutputHandle<OrdZSet<Row>>,
}

impl SumCircuit {
    fn start<A>(config: CircuitConfig) -> Self
    where
        A: dbsp::DBWeight + From<Sum> + Into<Sum> + MulByRef<i64, Output = A>,
    {
        let (runtime, (input, output)) = Runtime::init_circuit(config, |circuit| {
            let (source, input) = circuit.add_input_zset::<Row>();
            let output = source
                .map_index(|Tup2(key, value)| (*key, *value))
                .aggregate_linear_postprocess_persistent(
                    Some("nullable-sum"),
                    |value| A::from(Tup3(value.unwrap_or(0), i64::from(value.is_some()), 1)),
                    |value: A| {
                        let Tup3(sum, count, _) = value.into();
                        (count != 0).then_some(sum)
                    },
                )
                .map(|(k, v)| Tup2(*k, *v))
                .output();
            Ok((input, output))
        })
        .unwrap();
        Self {
            runtime,
            input,
            output,
        }
    }

    fn step(&mut self, mut input: Vec<Change>) -> OrdZSet<Row> {
        self.input.append(&mut input);
        self.runtime.transaction().unwrap();
        self.output.consolidate()
    }

    fn stop(self) {
        self.runtime.kill().unwrap();
    }
}

fn changes(values: &[(i32, Option<i64>, i64)]) -> Vec<Change> {
    values
        .iter()
        .map(|&(key, value, weight)| Tup2(Tup2(key, value), weight))
        .collect()
}

#[test]
fn wide_count_weight_matches_rkyv_bytes() {
    let weight = Tup2(Tup2(i128::from(i64::MAX) + 1, -19), Tup2(8, 1));
    let expected = rkyv::to_bytes::<_, 256>(&weight).unwrap();
    let actual = rkyv::to_bytes::<_, 256>(&NativeCount::from(weight)).unwrap();
    assert_eq!(actual.as_slice(), expected.as_slice());
}

#[test]
fn nullable_sum_checkpoint_restores_native_accumulator_weights() {
    let evidence = std::env::var_os("P0_NATIVE_SUM_STATE_EVIDENCE").map(PathBuf::from);
    if let Some(path) = &evidence {
        std::fs::create_dir(path).unwrap();
    }
    let storage_dir = tempfile::tempdir().unwrap();
    let oracle_dir = tempfile::tempdir().unwrap();
    let mut original = SumCircuit::start::<Sum>(storage(storage_dir.path()));
    let mut oracle = SumCircuit::start::<Sum>(storage(oracle_dir.path()));
    let initial = changes(&[(1, Some(4), 2), (1, None, 1), (2, None, 1), (3, Some(0), 1)]);
    let expected = OrdZSet::from_keys(
        (),
        changes(&[(1, Some(8), 1), (2, None, 1), (3, Some(0), 1)]),
    );
    assert_eq!(original.step(initial.clone()), expected);
    assert_eq!(oracle.step(initial), expected);
    let fingerprint = original.runtime.fingerprint();
    let checkpoint = original.runtime.checkpoint().run().unwrap();
    original.stop();
    if let Some(path) = &evidence {
        retain_directory(storage_dir.path(), &path.join("rust-checkpoint"));
    }
    assert_eq!(NativeSum::archive_counts().0, 0);

    let owner = NativeSum::encoder();
    let first_library = Arc::downgrade(&owner);
    let mut config = storage(storage_dir.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let mut native = SumCircuit::start::<NativeSum>(config);
    assert_eq!(native.runtime.fingerprint(), fingerprint);
    for (delta, output) in [
        (changes(&[]), changes(&[])),
        (
            changes(&[(1, Some(4), -1)]),
            changes(&[(1, Some(8), -1), (1, Some(4), 1)]),
        ),
        (
            changes(&[(1, Some(4), -1), (2, Some(0), 1)]),
            changes(&[
                (1, Some(4), -1),
                (1, None, 1),
                (2, None, -1),
                (2, Some(0), 1),
            ]),
        ),
        (
            changes(&[(1, None, -1), (3, Some(0), -1)]),
            changes(&[(1, None, -1), (3, Some(0), -1)]),
        ),
    ] {
        let expected = OrdZSet::from_keys((), output);
        assert_eq!(oracle.step(delta.clone()), expected);
        assert_eq!(native.step(delta), expected);
    }
    let native_checkpoint = native.runtime.checkpoint().run().unwrap();
    let (writes, reads) = NativeSum::archive_counts();
    assert!(
        writes > 0 && reads > 0,
        "native weights were not exercised: {writes} writes, {reads} reads"
    );
    native.stop();
    drop(owner);
    assert!(first_library.upgrade().is_none());
    if let Some(path) = &evidence {
        retain_directory(storage_dir.path(), &path.join("native-checkpoint"));
    }

    let mut config = storage(storage_dir.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(native_checkpoint.uuid);
    let owner = NativeSum::encoder();
    let fresh_library = Arc::downgrade(&owner);
    let mut resumed = SumCircuit::start::<NativeSum>(config);
    assert_eq!(resumed.runtime.fingerprint(), fingerprint);
    let delta = changes(&[(2, Some(0), -1)]);
    let expected = OrdZSet::from_keys((), changes(&[(2, Some(0), -1), (2, None, 1)]));
    assert_eq!(oracle.step(delta.clone()), expected);
    assert_eq!(resumed.step(delta), expected);
    resumed.stop();
    oracle.stop();
    drop(owner);
    assert!(fresh_library.upgrade().is_none());
    let relinked_reads = NativeSum::archive_counts().1 - reads;
    assert!(relinked_reads > 0);
    println!(
        "native accumulator weights: {writes} writes, {reads} reads, {relinked_reads} reads after relinking, five post-restore transactions"
    );
}

#[test]
fn native_count_restores_persistent_state() {
    use dbsp::{circuit::Mode, utils::Tup7};
    use feldera_native_dbsp_probe::{CountDistinctStateIds, build_count_distinct_persistent};
    use feldera_sqllib::{F64, SqlString};

    let directory = tempfile::tempdir().unwrap();
    let config = || {
        CircuitConfig::with_workers(4)
            .with_mode(Mode::Persistent)
            .with_storage(storage(directory.path()).storage)
    };
    let start = |config| {
        Runtime::init_circuit(config, |circuit| {
            let ids = CountDistinctStateIds {
                per_key: "count-per-key",
                global: "count-global",
                empty_group: "count-empty",
                errors: "count-errors",
                output: "count-output",
                error_output: "count-error-output",
            };
            Ok(build_count_distinct_persistent(circuit, 0, 10, ids)?)
        })
        .unwrap()
    };
    let row = |key| {
        Tup7(
            key,
            F64::new(0.0),
            false,
            SqlString::from(""),
            None,
            None,
            None,
        )
    };
    let (mut runtime, ports) = start(config());
    ports.input.append(&mut vec![
        Tup2(row(11), 2),
        Tup2(row(12), 1),
        Tup2(row(0), 1),
    ]);
    runtime.transaction().unwrap();
    assert_eq!(
        ports.output.consolidate(),
        OrdZSet::from_keys((), vec![Tup2(Tup2(3, 2), 1)]),
    );
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    let checkpoint = runtime.checkpoint().run().unwrap();
    let fingerprint = runtime.fingerprint();
    runtime.kill().unwrap();
    drop(ports);

    let mut resume = config();
    resume.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);
    let (mut runtime, ports) = start(resume);
    assert_eq!(runtime.fingerprint(), fingerprint);
    ports.input.append(&mut vec![Tup2(row(11), -2)]);
    runtime.transaction().unwrap();
    assert_eq!(
        ports.output.consolidate(),
        OrdZSet::from_keys((), vec![Tup2(Tup2(3, 2), -1), Tup2(Tup2(1, 1), 1)]),
    );
    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
    runtime.kill().unwrap();
}
