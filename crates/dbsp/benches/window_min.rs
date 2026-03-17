//! Benchmark that simulates a pathological scenario where a window operator is followed by an aggregate
//! that computes min over the timestamp column. As the window operator evicts tuples on the left side of
//! the window, 0-weight tuples accumulate in the trace, slowing down the skip_zero_weight_vals_forward operation.
//! Effectively the O(1) min computation becomes O(n), where n is the number of zero-weight tuples in the trace.

use anyhow::{Context, Result, anyhow};
use dbsp::circuit::CircuitConfig;
use dbsp::operator::Min;
use dbsp::utils::Tup2;
use dbsp::{Runtime, TypedBox, ZWeight, circuit::DevTweaks};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::time::{Duration, Instant};

const WORKERS: usize = 1;
const TOTAL_RECORDS: u64 = 10_000_000;
const BATCH_SIZE: usize = 10_000;
const VALUE_SPACE: u64 = 100;
const CLOCK_STEP: u64 = 10_000;
const SEED: u64 = 0xD15E_A5E5_1A5A_B1E0;
const PROGRESS_EVERY_BATCHES: usize = 50;
const PROGRESS_EVERY_CLOCK_STEPS: usize = 50;
const PROFILE_OUTPUT_DIR: &str = "window_min_file_storage.profile";
const NEGATIVE_WEIGHT_MULTIPLIERS: &[u16] = &[0, 1, 2, 4, 6, 8];

struct RunSummary {
    negative_weight_multiplier: u16,
    total_step_duration_secs: f64,
    overall_throughput_records_per_sec: f64,
    wall_clock_secs: f64,
}

fn main() -> Result<()> {
    validate_constants()?;
    let mut summaries = Vec::with_capacity(NEGATIVE_WEIGHT_MULTIPLIERS.len());
    for &negative_weight_multiplier in NEGATIVE_WEIGHT_MULTIPLIERS {
        summaries.push(run_workload(negative_weight_multiplier)?);
    }
    print_summary_table(&summaries);
    Ok(())
}

fn run_workload(negative_weight_multiplier: u16) -> Result<RunSummary> {
    let mut config = CircuitConfig::from(WORKERS);
    config.dev_tweaks = DevTweaks {
        negative_weight_multiplier,
        ..DevTweaks::default()
    };

    let total_batches = TOTAL_RECORDS / BATCH_SIZE as u64;
    println!(
        "Running window_min_file_storage benchmark with {WORKERS} worker, file backend, {TOTAL_RECORDS} records ({} batches of {BATCH_SIZE})",
        total_batches
    );
    println!("negative_weight_multiplier={negative_weight_multiplier}");

    let (mut dbsp, (clock_handle, data_handle)) = Runtime::init_circuit(config, |circuit| {
        let (clock, clock_handle) = circuit.add_input_stream::<u64>();
        let (data, data_handle) = circuit.add_input_indexed_zset::<u64, u64>();

        let bounds =
            clock.apply(|ts| (TypedBox::new(ts.saturating_add(1)), TypedBox::new(u64::MAX)));

        data.window((true, true), &bounds)
            .map_index(|(ts, value)| (*value, *ts))
            .aggregate(Min)
            .inspect(|_| {});

        Ok((clock_handle, data_handle))
    })
    .context("failed to initialize DBSP circuit")?;

    dbsp.enable_cpu_profiler()
        .context("failed to enable CPU profiler")?;

    let mut rng = ChaCha8Rng::seed_from_u64(SEED);
    let benchmark_start = Instant::now();
    let ingest_start = Instant::now();
    let mut ingest_step_duration = Duration::ZERO;
    let mut clock_step_duration = Duration::ZERO;
    let mut total_step_duration = Duration::ZERO;

    println!("pass 1: ingesting data with fixed clock=0");
    for batch_idx in 0..total_batches {
        let batch_start_ts = batch_idx * BATCH_SIZE as u64;
        let mut batch = Vec::with_capacity(BATCH_SIZE);

        for i in 0..BATCH_SIZE {
            let ts = batch_start_ts + i as u64;
            let value = rng.gen_range(0..VALUE_SPACE);
            batch.push(Tup2(ts, Tup2(value, 1 as ZWeight)));
        }

        data_handle.append(&mut batch);
        clock_handle.set_for_all(0);

        let step_start = Instant::now();
        dbsp.transaction().context("DBSP transaction failed")?;
        let step_elapsed = step_start.elapsed();
        ingest_step_duration += step_elapsed;
        total_step_duration += step_elapsed;

        let processed_batches = batch_idx as usize + 1;
        if processed_batches.is_multiple_of(PROGRESS_EVERY_BATCHES)
            || processed_batches == total_batches as usize
        {
            let processed_records = processed_batches as u64 * BATCH_SIZE as u64;
            let elapsed = benchmark_start.elapsed().as_secs_f64();
            let percent = (processed_batches as f64 / total_batches as f64) * 100.0;
            println!(
                "pass1 progress: {processed_batches}/{total_batches} batches ({processed_records}/{TOTAL_RECORDS} records, {percent:.2}%), wall_clock_secs={elapsed:.3}"
            );
        }
    }

    let ingest_wall_clock = ingest_start.elapsed();
    println!("pass 2: advancing clock by {CLOCK_STEP}");
    let mut clock_value = 0u64;
    let final_clock = TOTAL_RECORDS.saturating_add(CLOCK_STEP);
    let mut clock_steps = 0usize;

    while clock_value <= final_clock {
        clock_handle.set_for_all(clock_value);

        let step_start = Instant::now();
        dbsp.transaction().context("DBSP transaction failed")?;
        let step_elapsed = step_start.elapsed();
        clock_step_duration += step_elapsed;
        total_step_duration += step_elapsed;

        clock_steps += 1;
        if clock_steps.is_multiple_of(PROGRESS_EVERY_CLOCK_STEPS) || clock_value == final_clock {
            let elapsed = benchmark_start.elapsed().as_secs_f64();
            let percent = if final_clock == 0 {
                100.0
            } else {
                (clock_value as f64 / final_clock as f64) * 100.0
            };
            println!(
                "pass2 progress: step={clock_steps}, clock={clock_value}/{final_clock} ({percent:.2}%), wall_clock_secs={elapsed:.3}"
            );
        }

        clock_value = clock_value.saturating_add(CLOCK_STEP);
    }

    let profile = dbsp
        .retrieve_profile()
        .context("failed to retrieve runtime profile")?;

    let profile_dump_path = dbsp
        .dump_profile(PROFILE_OUTPUT_DIR)
        .context("failed to dump graph profile")?;

    let total_storage_size = profile
        .total_storage_size()
        .map_err(|err| anyhow!("failed to compute total storage size from profile: {err:?}"))?;

    dbsp.kill()
        .map_err(|_| anyhow!("failed to stop DBSP runtime"))?;

    let wall_clock = benchmark_start.elapsed();
    let ingest_step_seconds = ingest_step_duration.as_secs_f64();
    let clock_step_seconds = clock_step_duration.as_secs_f64();
    let step_seconds = total_step_duration.as_secs_f64();
    let ingest_throughput = TOTAL_RECORDS as f64 / ingest_step_seconds;
    let overall_throughput = TOTAL_RECORDS as f64 / step_seconds;

    println!("ingest_wall_clock_secs={}", ingest_wall_clock.as_secs_f64());
    println!("ingest_step_duration_secs={ingest_step_seconds}");
    println!("clock_step_duration_secs={clock_step_seconds}");
    println!("total_step_duration_secs={step_seconds}");
    println!("ingest_throughput_records_per_sec={ingest_throughput}");
    println!("overall_throughput_records_per_sec={overall_throughput}");
    println!("wall_clock_secs={}", wall_clock.as_secs_f64());
    println!("total_storage_size={total_storage_size}");
    println!("profile_dump_path={}", profile_dump_path.display());

    Ok(RunSummary {
        negative_weight_multiplier,
        total_step_duration_secs: step_seconds,
        overall_throughput_records_per_sec: overall_throughput,
        wall_clock_secs: wall_clock.as_secs_f64(),
    })
}

fn print_summary_table(summaries: &[RunSummary]) {
    println!();
    println!("## window_min summary");
    println!(
        "| negative_weight_multiplier | total_step_duration_secs | overall_throughput_records_per_sec | wall_clock_secs |"
    );
    println!("|---:|---:|---:|---:|");
    for summary in summaries {
        println!(
            "| {} | {:.9} | {:.9} | {:.9} |",
            summary.negative_weight_multiplier,
            summary.total_step_duration_secs,
            summary.overall_throughput_records_per_sec,
            summary.wall_clock_secs
        );
    }
}

fn validate_constants() -> Result<()> {
    if WORKERS != 1 {
        return Err(anyhow!("WORKERS must be 1"));
    }
    if BATCH_SIZE == 0 {
        return Err(anyhow!("BATCH_SIZE must be > 0"));
    }
    if TOTAL_RECORDS == 0 || !TOTAL_RECORDS.is_multiple_of(BATCH_SIZE as u64) {
        return Err(anyhow!(
            "TOTAL_RECORDS must be > 0 and divisible by BATCH_SIZE"
        ));
    }
    if VALUE_SPACE == 0 {
        return Err(anyhow!("VALUE_SPACE must be > 0"));
    }
    if CLOCK_STEP == 0 {
        return Err(anyhow!("CLOCK_STEP must be > 0"));
    }
    Ok(())
}
