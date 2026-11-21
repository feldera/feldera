//! Checkpoint interchange with the frozen classic aggregate circuits.

use super::{
    SqlDecimal, SqlString, circuit0, circuit0_native, circuit1, circuit1_native, input0, input5,
    native_weights::{FixedWeight, NativeWeight},
};
use dbsp::{
    DBData, DBSPHandle, OrdZSet, OutputHandle, ZSetHandle,
    circuit::{CircuitConfig, Mode},
    typed_batch::SpineSnapshot,
    utils::{Tup1, Tup2, Tup3, Tup7},
};
use feldera_sqllib::{F64, append_to_collection_handle, read_output_spine};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[allow(dead_code)]
#[path = "checkpoint_support.rs"]
mod checkpoint;
use checkpoint::{retain_directory, storage};

type Input = Tup7<i32, F64, bool, SqlString, Option<i32>, Option<SqlDecimal<6, 2>>, Option<u32>>;
type ErrorRow = Tup3<SqlString, SqlString, SqlString>;
type Output<R> = OutputHandle<SpineSnapshot<OrdZSet<R>>>;
type Handles<R> = (ZSetHandle<Input>, Output<R>, Output<ErrorRow>);
type Builder<R> = fn(CircuitConfig) -> Result<(DBSPHandle, Handles<R>), dbsp::Error>;

#[test]
fn frozen_sum_checkpoint_interchange() {
    verify::<Tup3<i64, i64, i64>, _>(
        "sum",
        circuit0,
        circuit0_native,
        input0().0,
        input5().0,
        Tup1(Some(1)),
        Tup1(None),
    );
}

#[test]
fn frozen_count_checkpoint_interchange() {
    // The frozen empty-input test does not exercise the accumulator trace.
    // These two keys have distinct IDs and one arrives with multiplicity two.
    let row = |id| {
        Tup7::new(
            id,
            F64::new(0.0),
            false,
            SqlString::from(""),
            None,
            None,
            None,
        )
    };
    verify::<Tup2<Tup2<i128, i64>, Tup2<i64, i64>>, _>(
        "count",
        circuit1,
        circuit1_native,
        OrdZSet::from_keys((), vec![Tup2(row(11_000), 2), Tup2(row(12_000), 1)]),
        OrdZSet::from_keys((), vec![Tup2(row(11_000), -2), Tup2(row(12_000), -1)]),
        Tup2(3, 2),
        Tup2(0, 0),
    );
}

fn step<R: DBData>(
    runtime: &mut DBSPHandle,
    (input, data, errors): &Handles<R>,
    update: &OrdZSet<Input>,
    expected: &OrdZSet<R>,
) {
    append_to_collection_handle(update, input);
    runtime.transaction().unwrap();
    assert_eq!(&read_output_spine(data), expected);
    assert_eq!(read_output_spine(errors), OrdZSet::from_keys((), vec![]));
}

fn verify<A: FixedWeight, R: DBData>(
    name: &str,
    classic: Builder<R>,
    build_native: Builder<R>,
    initial: OrdZSet<Input>,
    retractions: OrdZSet<Input>,
    present: R,
    empty: R,
) {
    let workers = std::env::var("P0_FROZEN_AGGREGATE_WORKERS").map_or(1, |value| {
        value
            .parse::<usize>()
            .expect("worker count must be an integer")
    });
    assert!(workers > 0);
    let storage_config = |path: &Path| {
        CircuitConfig::with_workers(workers)
            .with_mode(Mode::Persistent)
            .with_storage(storage(path).storage)
    };
    let evidence = std::env::var_os("P0_FROZEN_AGGREGATE_STATE_EVIDENCE")
        .map(|root| PathBuf::from(root).join(name));
    if let Some(path) = &evidence {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::create_dir(path).unwrap();
    }
    let state = tempfile::tempdir().unwrap();
    let oracle_dir = tempfile::tempdir().unwrap();
    let (mut original, original_handles) = classic(storage_config(state.path())).unwrap();
    let (mut oracle, oracle_handles) = classic(storage_config(oracle_dir.path())).unwrap();
    let initial_output = OrdZSet::from_keys((), vec![Tup2(present.clone(), 1)]);
    step(&mut original, &original_handles, &initial, &initial_output);
    step(&mut oracle, &oracle_handles, &initial, &initial_output);

    let fingerprint = original.fingerprint();
    let classic_snapshot = original.checkpoint().run().unwrap();
    original.kill().unwrap();
    drop(original_handles);
    if let Some(path) = &evidence {
        retain_directory(state.path(), &path.join("classic"));
    }
    assert_eq!(NativeWeight::<A>::archive_counts(), (0, 0));

    let owner = NativeWeight::<A>::encoder();
    let previous_library = Arc::downgrade(&owner);
    let mut config = storage_config(state.path());
    config.storage.as_mut().unwrap().init_checkpoint = Some(classic_snapshot.uuid);
    let (mut native, native_handles) = build_native(config).unwrap();
    assert_eq!(native.fingerprint(), fingerprint);
    let retract = vec![Tup2(empty.clone(), 1), Tup2(present.clone(), -1)];
    let reinsert = vec![Tup2(empty, -1), Tup2(present, 1)];
    let no_updates = OrdZSet::from_keys((), vec![]);
    for (bag, expected) in [
        (&no_updates, vec![]),
        (&retractions, retract.clone()),
        (&initial, reinsert),
    ] {
        let expected = OrdZSet::from_keys((), expected);
        step(&mut native, &native_handles, bag, &expected);
        step(&mut oracle, &oracle_handles, bag, &expected);
    }

    let native_snapshot = native.checkpoint().run().unwrap();
    let (writes, reads) = NativeWeight::<A>::archive_counts();
    assert!(writes > 0 && reads > 0, "{writes} writes, {reads} reads");
    native.kill().unwrap();
    drop((native_handles, owner));
    assert!(previous_library.upgrade().is_none());
    if let Some(path) = &evidence {
        retain_directory(state.path(), &path.join("native"));
    }

    let restore_config = || {
        let mut config = storage_config(state.path());
        config.storage.as_mut().unwrap().init_checkpoint = Some(native_snapshot.uuid);
        config
    };
    let expected = OrdZSet::from_keys((), retract);
    let (mut relinked, relinked_handles) = build_native(restore_config()).unwrap();
    assert_eq!(relinked.fingerprint(), fingerprint);
    step(&mut relinked, &relinked_handles, &retractions, &expected);
    step(&mut oracle, &oracle_handles, &retractions, &expected);
    relinked.kill().unwrap();
    drop(relinked_handles);
    oracle.kill().unwrap();
    let relinked_reads = NativeWeight::<A>::archive_counts().1 - reads;
    assert!(relinked_reads > 0);

    let (mut restored, restored_handles) = classic(restore_config()).unwrap();
    assert_eq!(restored.fingerprint(), fingerprint);
    step(&mut restored, &restored_handles, &retractions, &expected);
    restored.kill().unwrap();
    println!(
        "{name}: workers={workers} fingerprint={fingerprint:x} archive_writes={writes} archive_reads={reads} relinked_reads={relinked_reads} reverse_restore=passed"
    );
}
