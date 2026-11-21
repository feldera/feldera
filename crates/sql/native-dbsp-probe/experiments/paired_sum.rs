//! Paired runtime measurement of the frozen nullable SUM accumulator.

use super::{circuit0, circuit0_native, native_weights::NativeSum};
use dbsp::{
    OrdZSet,
    circuit::CircuitConfig,
    utils::{Tup1, Tup2, Tup7},
};
use feldera_sqllib::{F64, SqlString, read_output_spine};
use std::{env, path::PathBuf, time::Instant};

#[allow(dead_code)]
#[path = "checkpoint_support.rs"]
mod checkpoint;

fn setting(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing benchmark setting {name}"))
}

fn sum_delta(previous: Option<i32>, next: Option<i32>) -> OrdZSet<Tup1<Option<i32>>> {
    OrdZSet::from_keys((), vec![Tup2(Tup1(previous), -1), Tup2(Tup1(next), 1)])
}

#[test]
#[ignore = "explicit benchmark run"]
fn paired_sum_runtime_sample() {
    let engine = setting("P0_NATIVE_BENCH_ENGINE");
    let batch: i32 = setting("P0_NATIVE_BENCH_ROWS").parse().unwrap();
    let steps: usize = setting("P0_NATIVE_BENCH_STEPS").parse().unwrap();
    let state = PathBuf::from(setting("P0_NATIVE_BENCH_STATE"));
    let workers = env::var("P0_NATIVE_BENCH_WORKERS").map_or(1, |value| {
        value
            .parse::<usize>()
            .expect("worker count must be an integer")
    });
    assert!((1..=65_536).contains(&batch));
    assert!(steps > 0 && steps.is_multiple_of(2));
    assert!(workers > 0);

    let mut inserted = Vec::new();
    let mut retracted = Vec::new();
    for index in 0..batch {
        let value = (index % 2 == 0).then_some(2);
        let row = Tup7(
            index + 1,
            F64::new(0.0),
            false,
            SqlString::from(""),
            value,
            None,
            None,
        );
        inserted.push(Tup2(row.clone(), 1));
        retracted.push(Tup2(row, -1));
    }
    let sum = Some(2 * ((batch + 1) / 2));
    let updates = [inserted, retracted];
    let expected = [sum_delta(None, sum), sum_delta(sum, None)];
    let empty_errors = OrdZSet::from_keys((), vec![]);

    let config =
        CircuitConfig::with_workers(workers).with_storage(checkpoint::storage(&state).storage);
    let start = Instant::now();
    // Keep the encoder alive through the sample, including any state spill.
    let _encoder = match engine.as_str() {
        "classic" => None,
        "native" => Some(NativeSum::encoder()),
        _ => panic!("expected classic or native benchmark mode"),
    };
    let (mut runtime, (input, output, errors)) = match engine.as_str() {
        "classic" => circuit0(config),
        "native" => circuit0_native(config),
        _ => unreachable!(),
    }
    .unwrap();
    runtime.transaction().unwrap();
    assert_eq!(
        read_output_spine(&output),
        OrdZSet::from_keys((), vec![Tup2(Tup1(None), 1)])
    );
    assert_eq!(read_output_spine(&errors), empty_errors);
    let init_ns = start.elapsed().as_nanos();

    const WARMUP_STEPS: usize = 6;
    let mut elapsed_ns = 0;
    for index in 0..(WARMUP_STEPS + steps) {
        let phase = index % 2;
        let mut delta = updates[phase].clone();
        let start = Instant::now();
        input.append(&mut delta);
        runtime.transaction().unwrap();
        let actual = read_output_spine(&output);
        let error_output = read_output_spine(&errors);
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(actual, expected[phase]);
        assert_eq!(error_output, empty_errors);
        if index >= WARMUP_STEPS {
            elapsed_ns += elapsed;
        }
    }
    let start = Instant::now();
    runtime.checkpoint().run().unwrap();
    let checkpoint_ns = start.elapsed().as_nanos();
    if engine == "native" {
        let (writes, reads) = NativeSum::archive_counts();
        assert!(
            writes > 0 && reads > 0,
            "{writes} archive writes, {reads} archive reads"
        );
    }
    println!(
        "engine={engine} rows={batch} steps={steps} init_ns={init_ns} elapsed_ns={elapsed_ns} checkpoint_ns={checkpoint_ns} workers={workers}"
    );
    runtime.kill().unwrap();
}
