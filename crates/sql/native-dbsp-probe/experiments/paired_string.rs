//! Measure the archive adapter on the persistent nullable VARCHAR join.

use super::{Join, Row, native};
use dbsp::{
    OrdZSet,
    circuit::{CircuitConfig, CircuitStorageConfig, Mode, StepSize},
    utils::{Tup1, Tup2},
};
use feldera_sqllib::SqlString;
use feldera_types::{
    config::{StorageCacheConfig, StorageConfig, StorageOptions},
    size::ConfigSize,
};
use std::{
    env,
    time::{Duration, Instant},
};

const SEED_ROWS: u32 = 2048;

fn keys(count: u32) -> Vec<Row> {
    (0..count)
        .map(|key| {
            let text = if key.is_multiple_of(2) {
                format!("k{key:08}")
            } else {
                format!("persistent-string-key-{key:08}-é\0🐍")
            };
            Tup1(Some(SqlString::from_ref(&text)))
        })
        .collect()
}

fn weighted(keys: &[Row], weight: i64) -> Vec<Tup2<Row, i64>> {
    keys.iter().map(|key| Tup2(key.clone(), weight)).collect()
}

#[test]
#[ignore = "explicit benchmark run"]
fn persistent_varchar_join_sample() {
    let engine = env::var("P0_NATIVE_BENCH_ENGINE").unwrap();
    let rows: usize = env::var("P0_NATIVE_BENCH_ROWS").unwrap().parse().unwrap();
    let steps: usize = env::var("P0_NATIVE_BENCH_STEPS").unwrap().parse().unwrap();
    let workers = env::var("P0_NATIVE_BENCH_WORKERS")
        .unwrap()
        .parse()
        .unwrap();
    let path = env::var("P0_NATIVE_BENCH_STATE").unwrap();
    assert!(rows > 0 && rows <= 2048 && steps > 0 && steps.is_multiple_of(2));
    let storage = CircuitStorageConfig::for_config(
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
    .unwrap();
    let config = CircuitConfig::with_workers(workers)
        .with_mode(Mode::Persistent)
        .with_step_size(StepSize::FullSteps)
        .with_storage(Some(storage));
    let start = Instant::now();
    let encoder = if engine == "native" {
        Some(native::encoder())
    } else {
        assert_eq!(engine, "classic");
        None
    };
    let mut join = if encoder.is_some() {
        // Use the same generated writer without contended observation counters.
        Join::start::<native::MeasuredStringKey>(config)
    } else {
        Join::start::<Row>(config)
    };
    let init_ns = start.elapsed().as_nanos();

    let mut keys = keys(SEED_ROWS + rows as u32);
    keys.push(Tup1(None));
    let sample_keys = &keys[SEED_ROWS as usize..keys.len() - 1];
    let mut seeded = weighted(&keys[..SEED_ROWS as usize], 1);
    seeded.push(Tup2(Tup1(None), 1));
    let expected = OrdZSet::from_keys(
        (),
        seeded
            .iter()
            .map(|Tup2(key, _)| Tup2(key.clone(), 2))
            .collect(),
    );
    assert_eq!(join.step(seeded, weighted(&keys, 2)), expected);

    let deltas = [weighted(sample_keys, 1), weighted(sample_keys, -1)];
    let expected = [
        OrdZSet::from_keys((), weighted(sample_keys, 2)),
        OrdZSet::from_keys((), weighted(sample_keys, -2)),
    ];
    let mut transaction_ns = Vec::with_capacity(steps);
    for step in 0..(6 + steps) {
        let phase = step % 2;
        let delta = deltas[phase].clone();
        let start = Instant::now();
        let actual = join.step(delta, Vec::new());
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(actual, expected[phase]);
        if step >= 6 {
            transaction_ns.push(elapsed);
        }
    }
    let start = Instant::now();
    join.runtime.start_compaction().unwrap();
    join.runtime
        .wait_for_compaction(Duration::from_secs(10))
        .unwrap();
    let compaction_ns = start.elapsed().as_nanos();
    let start = Instant::now();
    join.runtime.checkpoint().run().unwrap();
    let checkpoint_ns = start.elapsed().as_nanos();
    join.runtime.kill().unwrap();

    let elapsed_ns: u128 = transaction_ns.iter().sum();
    println!(
        "measured_step_ns={}",
        serde_json::to_string(&transaction_ns).unwrap()
    );
    println!(
        "engine={engine} rows={rows} steps={steps} init_ns={init_ns} elapsed_ns={elapsed_ns} checkpoint_ns={checkpoint_ns} compaction_ns={compaction_ns}"
    );
}
