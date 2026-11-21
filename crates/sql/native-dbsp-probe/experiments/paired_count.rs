//! Paired runtime measurement using the frozen incremental COUNT DISTINCT circuit.

use super::circuit1;
use dbsp::{
    circuit::{CircuitConfig, CircuitStorageConfig, StepSize},
    utils::{Tup2, Tup7},
    DBSPHandle, OrdZSet, Runtime, ZSetHandle,
};
use feldera_native_dbsp_probe::{build_count_distinct, IntegerCaseRow};
use feldera_sqllib::{read_output_spine, SqlString, F64};
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    size::ConfigSize,
};
use std::{env, time::Instant};

type Counts = OrdZSet<Tup2<i64, i64>>;
type Input = Vec<Tup2<IntegerCaseRow, i64>>;

fn rows(start: i32, count: usize, weight: i64) -> Input {
    (0..count as i32)
        .map(|offset| {
            Tup2(
                Tup7(
                    start + offset,
                    F64::from(0.0),
                    false,
                    SqlString::from_ref(""),
                    None,
                    None,
                    None,
                ),
                weight,
            )
        })
        .collect()
}

fn count_delta(old: Option<(i64, i64)>, new: (i64, i64)) -> Counts {
    let mut rows = vec![Tup2(Tup2(new.0, new.1), 1)];
    if let Some((total, distinct)) = old {
        rows.push(Tup2(Tup2(total, distinct), -1));
    }
    OrdZSet::from_keys((), rows)
}

fn step(
    runtime: &mut DBSPHandle,
    input: &ZSetHandle<IntegerCaseRow>,
    read: &mut impl FnMut() -> Counts,
    mut delta: Input,
) -> Counts {
    input.append(&mut delta);
    runtime.transaction().unwrap();
    read()
}

#[test]
#[ignore = "explicit benchmark run"]
fn paired_count_runtime_sample() {
    let engine = env::var("P0_NATIVE_BENCH_ENGINE").unwrap();
    let batch: usize = env::var("P0_NATIVE_BENCH_ROWS").unwrap().parse().unwrap();
    let steps: usize = env::var("P0_NATIVE_BENCH_STEPS").unwrap().parse().unwrap();
    let path = env::var("P0_NATIVE_BENCH_STATE").unwrap();
    let workers = env::var("P0_NATIVE_BENCH_WORKERS").map_or(1, |value| {
        value
            .parse::<usize>()
            .expect("worker count must be an integer")
    });
    assert!(steps > 0 && steps % 2 == 0);
    assert!(workers > 0);
    const BASE: i64 = 16_384;
    const FIRST_VALID_KEY: i32 = 20_000;
    let seed = rows(FIRST_VALID_KEY, BASE as usize, 1);
    // Half of each update changes an existing weight. The other half adds
    // or removes new distinct keys. Each two-step cycle returns to the seed.
    let mut insert = rows(FIRST_VALID_KEY, batch / 2, 1);
    insert.extend(rows(FIRST_VALID_KEY + BASE as i32, batch - batch / 2, 1));
    let delete = insert
        .iter()
        .map(|Tup2(row, _)| Tup2(row.clone(), -1))
        .collect();
    let high = (BASE + batch as i64, BASE + (batch - batch / 2) as i64);
    let low = (BASE, BASE);
    let deltas = [insert, delete];
    let expected = [count_delta(Some(low), high), count_delta(Some(high), low)];

    let config = CircuitConfig::with_workers(workers)
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
    let (mut runtime, input, mut read): (_, _, Box<dyn FnMut() -> Counts>) = match engine.as_str() {
        "classic" => {
            let (runtime, (input, output, errors)) = circuit1(config).unwrap();
            (
                runtime,
                input,
                Box::new(move || {
                    assert_eq!(read_output_spine(&errors), OrdZSet::from_keys((), vec![]));
                    read_output_spine(&output)
                }),
            )
        }
        "native" => {
            let (runtime, ports) = Runtime::init_circuit(config, |circuit| {
                Ok(build_count_distinct(circuit, 0, 10_000)?)
            })
            .unwrap();
            (
                runtime,
                ports.input,
                Box::new(move || {
                    assert_eq!(ports.errors.consolidate(), OrdZSet::from_keys((), vec![]));
                    ports.output.consolidate()
                }),
            )
        }
        _ => panic!("expected classic or native benchmark mode"),
    };
    let init_ns = start.elapsed().as_nanos();
    assert_eq!(
        step(&mut runtime, &input, &mut read, seed),
        count_delta(None, low)
    );
    let mut elapsed_ns = 0;
    for index in 0..(6 + steps) {
        let phase = index % 2;
        let delta = deltas[phase].clone();
        let start = Instant::now();
        let output = step(&mut runtime, &input, &mut read, delta);
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(output, expected[phase]);
        if index >= 6 {
            elapsed_ns += elapsed;
        }
    }
    let start = Instant::now();
    runtime.checkpoint().run().unwrap();
    let checkpoint_ns = start.elapsed().as_nanos();
    runtime.kill().unwrap();
    println!("engine={engine} rows={batch} steps={steps} init_ns={init_ns} elapsed_ns={elapsed_ns} checkpoint_ns={checkpoint_ns}");
}
