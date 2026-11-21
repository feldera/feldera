//! Benchmark the generated decimal rolling circuit and its native archive adapter.

use super::{circuit0, circuit0_native, native_decimal};
use dbsp::{
    OrdZSet,
    circuit::{CircuitConfig, CircuitStorageConfig, Mode, StepSize},
    utils::{Tup2, Tup7},
};
use feldera_sqllib::{F64, SqlDecimal, SqlString, read_output_spine};
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    size::ConfigSize,
};
use std::{env, sync::atomic::Ordering::Relaxed, time::Instant};

const SEED_ROWS: i32 = 2048;

type Row = Tup7<i32, F64, bool, SqlString, Option<i32>, Option<SqlDecimal<6, 2>>, Option<u32>>;

fn input(start: i32, end: i32, weight: i64) -> Vec<Tup2<Row, i64>> {
    (start..end)
        .map(|index| {
            Tup2(
                Tup7(
                    index,
                    F64::from(0.0),
                    false,
                    SqlString::from_ref(""),
                    None,
                    Some(SqlDecimal::new(i128::from(index), 0).unwrap()),
                    None,
                ),
                weight,
            )
        })
        .collect()
}

fn output(start: i32, end: i32, weight: i64) -> OrdZSet<Tup2<i32, i64>> {
    // The ordered keys are distinct and consecutive. Each row's inclusive
    // rolling count is one more than its zero-based input key.
    OrdZSet::from_keys(
        (),
        (start..end)
            .map(|index| Tup2(Tup2(index, i64::from(index) + 1), weight))
            .collect(),
    )
}

#[test]
#[ignore = "explicit benchmark run"]
fn paired_rolling_runtime_sample() {
    let engine = env::var("P0_NATIVE_BENCH_ENGINE").unwrap();
    let rows: i32 = env::var("P0_NATIVE_BENCH_ROWS").unwrap().parse().unwrap();
    let steps: usize = env::var("P0_NATIVE_BENCH_STEPS").unwrap().parse().unwrap();
    let workers = env::var("P0_NATIVE_BENCH_WORKERS")
        .unwrap()
        .parse()
        .unwrap();
    let path = env::var("P0_NATIVE_BENCH_STATE").unwrap();
    assert!(rows > 0 && rows <= 2048 && steps > 0 && steps % 2 == 0);
    let config = CircuitConfig::with_workers(workers)
        .with_mode(Mode::Persistent)
        .with_step_size(StepSize::FullSteps)
        .with_storage(Some(
            CircuitStorageConfig::for_config(
                StorageConfig {
                    path,
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions {
                    min_spill_size: Some(ConfigSize::ZERO),
                    min_step_spill_size: Some(ConfigSize::ZERO),
                    ..StorageOptions::default()
                },
            )
            .unwrap(),
        ));
    let start = Instant::now();
    let encoder = if engine == "native" {
        Some(native_decimal::encoder())
    } else {
        assert_eq!(engine, "classic");
        None
    };
    let (mut runtime, (handle, data, errors)) = if encoder.is_some() {
        circuit0_native(config).unwrap()
    } else {
        circuit0(config).unwrap()
    };
    let init_ns = start.elapsed().as_nanos();
    handle.append(&mut input(0, SEED_ROWS, 1));
    runtime.transaction().unwrap();
    assert_eq!(read_output_spine(&data), output(0, SEED_ROWS, 1));
    let delta = [
        input(SEED_ROWS, SEED_ROWS + rows, 1),
        input(SEED_ROWS, SEED_ROWS + rows, -1),
    ];
    let expected = [
        output(SEED_ROWS, SEED_ROWS + rows, 1),
        output(SEED_ROWS, SEED_ROWS + rows, -1),
    ];
    let mut transaction_ns = Vec::with_capacity(steps);
    for step in 0..(6 + steps) {
        let phase = step % 2;
        let mut delta = delta[phase].clone();
        let start = Instant::now();
        handle.append(&mut delta);
        runtime.transaction().unwrap();
        let actual = read_output_spine(&data);
        let actual_errors = read_output_spine(&errors);
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(actual, expected[phase]);
        assert!(actual_errors.is_empty());
        if step >= 6 {
            transaction_ns.push(elapsed);
        }
    }
    let start = Instant::now();
    runtime.checkpoint().run().unwrap();
    let checkpoint_ns = start.elapsed().as_nanos();
    runtime.kill().unwrap();
    if encoder.is_some() {
        assert!(native_decimal::WRITES.load(Relaxed) > 0);
        assert!(native_decimal::READS.load(Relaxed) > 0);
    }
    let elapsed_ns: u128 = transaction_ns.iter().sum();
    println!(
        "measured_step_ns={}",
        serde_json::to_string(&transaction_ns).unwrap()
    );
    println!(
        "engine={engine} rows={rows} steps={steps} init_ns={init_ns} elapsed_ns={elapsed_ns} checkpoint_ns={checkpoint_ns}"
    );
}
