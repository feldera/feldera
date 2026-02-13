//! Benchmark tests for the percentile operator parallelism.
//!
//! These tests generate high-throughput data directly into the DBSP circuit
//! (no HTTP/SDK overhead) using a sliding window pattern to stress worker
//! workload rebalancing with spill-to-disk enabled.
//!
//! Run explicitly (they are `#[ignore]`d):
//! ```bash
//! cargo test -p dbsp test_percentile_bench_tup0 -- --ignored --nocapture
//! cargo test -p dbsp test_percentile_bench_keyed -- --ignored --nocapture
//! ```
//!
//! For profiling with Samply:
//! ```bash
//! cargo test -p dbsp test_percentile_bench_tup0 --no-run -- --ignored
//! samply record target/debug/deps/dbsp-HASH test_percentile_bench_tup0 --ignored --nocapture
//! ```

use std::time::Instant;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::{
    algebra::F64,
    circuit::CircuitConfig,
    Runtime,
    utils::{Tup0, Tup2},
};

// ---------------------------------------------------------------------------
// Configuration constants
// ---------------------------------------------------------------------------

const NUM_WORKERS: usize = 8;
const TOTAL_RECORDS: usize = 2_000_000;
const BATCH_SIZE: usize = 10_000;
const DELETE_RATIO: f64 = 0.10;
const VALUE_RANGE: f64 = 1_000_000.0;
const WINDOW_WIDTH_FRACTION: f64 = 1.0 / 3.0;
const SWEEP_STEPS: usize = 200;
const PERCENTILES: &[f64] = &[0.25, 0.50, 0.75, 0.90, 0.99];

const NUM_GROUPS: i32 = 4;

// ---------------------------------------------------------------------------
// Sliding window helpers
// ---------------------------------------------------------------------------

/// Compute the sliding window center for a given progress value.
///
/// The center oscillates sinusoidally across the value range:
///   center = mid + amplitude * sin(2π * progress)
///
/// where `amplitude = mid - window_half` ensures the window stays in bounds.
fn compute_window_center(progress: f64) -> f64 {
    let mid = VALUE_RANGE / 2.0;
    let window_half = VALUE_RANGE * WINDOW_WIDTH_FRACTION / 2.0;
    let amplitude = mid - window_half;
    mid + amplitude * (2.0 * std::f64::consts::PI * progress).sin()
}

// ---------------------------------------------------------------------------
// Test: Tup0 (no GROUP BY) — VirtualShardRouting
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_percentile_bench_tup0() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = CircuitConfig::with_workers(NUM_WORKERS)
        .with_temporary_storage(tempdir.path());

    let (mut circuit, (input, _output)) = Runtime::init_circuit(config, |circuit| {
        let (input, input_handle) = circuit.add_input_indexed_zset::<Tup0, F64>();
        let output = input.percentile_cont_stateful(
            None,
            PERCENTILES,
            true,
            |results| {
                Tup2(
                    Tup2(results[0].clone(), results[1].clone()),
                    Tup2(
                        Tup2(results[2].clone(), results[3].clone()),
                        results[4].clone(),
                    ),
                )
            },
        );
        Ok((input_handle, output.output()))
    })
    .unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(42);
    let mut live_entries: Vec<f64> = Vec::new();
    let total_steps = TOTAL_RECORDS / BATCH_SIZE;

    eprintln!(
        "=== Tup0 benchmark: {} records, batch_size={}, workers={}, sweep_steps={} ===",
        TOTAL_RECORDS, BATCH_SIZE, NUM_WORKERS, SWEEP_STEPS
    );

    let start = Instant::now();
    for step in 0..total_steps {
        let progress = step as f64 / SWEEP_STEPS as f64;
        let center = compute_window_center(progress);
        let window_half = VALUE_RANGE * WINDOW_WIDTH_FRACTION / 2.0;
        let lo = (center - window_half).max(0.0);
        let hi = (center + window_half).min(VALUE_RANGE);

        let num_inserts = (BATCH_SIZE as f64 * (1.0 - DELETE_RATIO)) as usize;
        let num_deletes = BATCH_SIZE - num_inserts;

        let mut batch: Vec<Tup2<Tup0, Tup2<F64, i64>>> = Vec::with_capacity(BATCH_SIZE);

        // Inserts
        for _ in 0..num_inserts {
            let val = rng.gen_range(lo..=hi);
            batch.push(Tup2(Tup0(), Tup2(F64::new(val), 1)));
            live_entries.push(val);
        }

        // Deletes (random from live pool)
        let actual_deletes = num_deletes.min(live_entries.len());
        for _ in 0..actual_deletes {
            let idx = rng.gen_range(0..live_entries.len());
            let val = live_entries.swap_remove(idx);
            batch.push(Tup2(Tup0(), Tup2(F64::new(val), -1)));
        }

        input.append(&mut batch);
        circuit.transaction().unwrap();

        if step % 50 == 0 {
            let elapsed = start.elapsed();
            let records_so_far = (step + 1) * BATCH_SIZE;
            eprintln!(
                "Step {}/{}: {} records, {:.1}s, {:.0} records/s, live_entries: {}",
                step,
                total_steps,
                records_so_far,
                elapsed.as_secs_f64(),
                records_so_far as f64 / elapsed.as_secs_f64(),
                live_entries.len()
            );
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "Completed {} records in {:.1}s ({:.0} records/s)",
        TOTAL_RECORDS,
        elapsed.as_secs_f64(),
        TOTAL_RECORDS as f64 / elapsed.as_secs_f64()
    );

    circuit.kill().unwrap();
}

// ---------------------------------------------------------------------------
// Test: Keyed (with GROUP BY) — ParallelRouting
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_percentile_bench_keyed() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = CircuitConfig::with_workers(NUM_WORKERS)
        .with_temporary_storage(tempdir.path());

    let (mut circuit, (input, _output)) = Runtime::init_circuit(config, |circuit| {
        let (input, input_handle) = circuit.add_input_indexed_zset::<i32, F64>();
        let output = input.percentile_cont_stateful(
            None,
            PERCENTILES,
            true,
            |results| {
                Tup2(
                    Tup2(results[0].clone(), results[1].clone()),
                    Tup2(
                        Tup2(results[2].clone(), results[3].clone()),
                        results[4].clone(),
                    ),
                )
            },
        );
        Ok((input_handle, output.output()))
    })
    .unwrap();

    let mut rng = ChaCha8Rng::seed_from_u64(42);
    // Per-group live entries
    let mut live_entries: Vec<Vec<f64>> = (0..NUM_GROUPS).map(|_| Vec::new()).collect();
    let total_steps = TOTAL_RECORDS / BATCH_SIZE;

    eprintln!(
        "=== Keyed benchmark: {} records, batch_size={}, workers={}, groups={}, sweep_steps={} ===",
        TOTAL_RECORDS, BATCH_SIZE, NUM_WORKERS, NUM_GROUPS, SWEEP_STEPS
    );

    let start = Instant::now();
    for step in 0..total_steps {
        let num_inserts = (BATCH_SIZE as f64 * (1.0 - DELETE_RATIO)) as usize;
        let num_deletes = BATCH_SIZE - num_inserts;
        let window_half = VALUE_RANGE * WINDOW_WIDTH_FRACTION / 2.0;

        let mut batch: Vec<Tup2<i32, Tup2<F64, i64>>> = Vec::with_capacity(BATCH_SIZE);

        // Inserts: distribute evenly across groups with phase offset
        let inserts_per_group = num_inserts / NUM_GROUPS as usize;
        for group_id in 0..NUM_GROUPS {
            // Each group has a phase offset for diversity
            let phase_offset = group_id as f64 / NUM_GROUPS as f64;
            let progress = step as f64 / SWEEP_STEPS as f64 + phase_offset;
            let center = compute_window_center(progress);
            let lo = (center - window_half).max(0.0);
            let hi = (center + window_half).min(VALUE_RANGE);

            for _ in 0..inserts_per_group {
                let val = rng.gen_range(lo..=hi);
                batch.push(Tup2(group_id, Tup2(F64::new(val), 1)));
                live_entries[group_id as usize].push(val);
            }
        }

        // Deletes: distribute across groups proportionally
        let deletes_per_group = num_deletes / NUM_GROUPS as usize;
        for group_id in 0..NUM_GROUPS {
            let pool = &mut live_entries[group_id as usize];
            let actual = deletes_per_group.min(pool.len());
            for _ in 0..actual {
                let idx = rng.gen_range(0..pool.len());
                let val = pool.swap_remove(idx);
                batch.push(Tup2(group_id, Tup2(F64::new(val), -1)));
            }
        }

        input.append(&mut batch);
        circuit.transaction().unwrap();

        if step % 50 == 0 {
            let elapsed = start.elapsed();
            let records_so_far = (step + 1) * BATCH_SIZE;
            let total_live: usize = live_entries.iter().map(|v| v.len()).sum();
            eprintln!(
                "Step {}/{}: {} records, {:.1}s, {:.0} records/s, live_entries: {} ({} per group avg)",
                step,
                total_steps,
                records_so_far,
                elapsed.as_secs_f64(),
                records_so_far as f64 / elapsed.as_secs_f64(),
                total_live,
                total_live / NUM_GROUPS as usize
            );
        }
    }

    let elapsed = start.elapsed();
    eprintln!(
        "Completed {} records in {:.1}s ({:.0} records/s)",
        TOTAL_RECORDS,
        elapsed.as_secs_f64(),
        TOTAL_RECORDS as f64 / elapsed.as_secs_f64()
    );

    circuit.kill().unwrap();
}
