use anyhow::{Context, Result, anyhow};
use crossbeam::channel::{Sender, bounded};
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig};
use dbsp::{
    Runtime,
    mimalloc::MiMalloc,
    operator::{MapHandle, Update},
    utils::{Tup2, Tup5},
};
use feldera_types::config::{StorageCacheConfig, StorageConfig, StorageOptions};
use rand::{Rng, RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::{
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tempfile::tempdir;

const BATCH_SIZE: usize = 10_000;
const PROGRESS_EVERY_BATCHES: usize = 100;
const WORKERS: usize = 2;
const DATAGEN_THREADS: usize = 8;
const TOTAL_RECORDS: u64 = 70_000_000;
const KEY_SPACE: u64 = 100_000_000;
const SEED: u64 = 0;
const MAX_IN_FLIGHT_BATCHES: usize = 64;

type Value = Tup5<u64, u64, u64, u64, u64>;
type BatchRecord = Tup2<u64, Update<Value, Value>>;
type Batch = Vec<BatchRecord>;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    validate_constants()?;
    let temp = tempdir().context("failed to create temp directory for storage backend")?;
    let config = CircuitConfig::with_workers(WORKERS).with_storage(Some(
        CircuitStorageConfig::for_config(
            StorageConfig {
                path: temp.path().to_string_lossy().into_owned(),
                cache: StorageCacheConfig::default(),
            },
            StorageOptions {
                min_storage_bytes: None,
                min_step_storage_bytes: None,
                ..StorageOptions::default()
            },
        )
        .context("failed to configure POSIX storage backend")?,
    ));

    let total_batches = (TOTAL_RECORDS / BATCH_SIZE as u64) as usize;
    println!(
        "Running input_map_ingest benchmark with {} workers, {} datagen threads, {} records ({} batches of {})",
        WORKERS, DATAGEN_THREADS, TOTAL_RECORDS, total_batches, BATCH_SIZE
    );

    let (mut dbsp, mut input_handle) = Runtime::init_circuit(config, |circuit| {
        let (stream, handle): (_, MapHandle<u64, Value, Value>) =
            circuit.add_input_map::<u64, Value, Value, _>(|v, u| *v = *u);

        stream.inspect(|_| {});
        Ok(handle)
    })
    .context("failed to initialize DBSP circuit")?;

    let (sender, receiver) = bounded::<Batch>(MAX_IN_FLIGHT_BATCHES);
    let datagen_handles = spawn_datagen_threads(total_batches, sender);

    let mut total_step_duration = Duration::ZERO;
    let benchmark_start = Instant::now();
    for batch_idx in 0..total_batches {
        let mut batch = receiver
            .recv()
            .map_err(|_| anyhow!("datagen channel closed before sending all batches"))?;
        input_handle.append(&mut batch);

        let step_start = Instant::now();
        dbsp.transaction().context("DBSP transaction/step failed")?;
        total_step_duration += step_start.elapsed();

        let processed_batches = batch_idx + 1;
        if processed_batches % PROGRESS_EVERY_BATCHES == 0 || processed_batches == total_batches {
            let processed_records = processed_batches as u64 * BATCH_SIZE as u64;
            let elapsed = benchmark_start.elapsed().as_secs_f64();
            let percent = (processed_batches as f64 / total_batches as f64) * 100.0;
            println!(
                "progress: {processed_batches}/{total_batches} batches ({processed_records}/{TOTAL_RECORDS} records, {percent:.2}%), wall_clock_secs={elapsed:.3}"
            );
        }
    }

    let profile_path = dbsp.dump_profile("profile").unwrap();
    println!("profile dumped to {profile_path:?}");

    for handle in datagen_handles {
        handle
            .join()
            .map_err(|_| anyhow!("datagen thread panicked"))?
            .context("datagen thread failed")?;
    }

    dbsp.kill()
        .map_err(|_| anyhow!("failed to stop DBSP runtime"))?;

    let wall_clock = benchmark_start.elapsed();
    let step_seconds = total_step_duration.as_secs_f64();
    let throughput = TOTAL_RECORDS as f64 / step_seconds;

    println!("total_step_duration_secs={step_seconds}");
    println!("throughput_records_per_sec={throughput}");
    println!("wall_clock_secs={}", wall_clock.as_secs_f64());

    Ok(())
}

fn validate_constants() -> Result<()> {
    if WORKERS == 0 {
        return Err(anyhow!("WORKERS must be > 0"));
    }
    if DATAGEN_THREADS == 0 {
        return Err(anyhow!("DATAGEN_THREADS must be > 0"));
    }
    if KEY_SPACE == 0 {
        return Err(anyhow!("KEY_SPACE must be > 0"));
    }
    if TOTAL_RECORDS == 0 || !TOTAL_RECORDS.is_multiple_of(BATCH_SIZE as u64) {
        return Err(anyhow!(
            "TOTAL_RECORDS must be > 0 and divisible by {BATCH_SIZE}"
        ));
    }
    if MAX_IN_FLIGHT_BATCHES == 0 {
        return Err(anyhow!("MAX_IN_FLIGHT_BATCHES must be > 0"));
    }
    Ok(())
}

fn spawn_datagen_threads(
    total_batches: usize,
    sender: Sender<Batch>,
) -> Vec<JoinHandle<Result<()>>> {
    let mut handles = Vec::with_capacity(DATAGEN_THREADS);
    let batches_per_thread = total_batches / DATAGEN_THREADS;
    let extra_batches = total_batches % DATAGEN_THREADS;

    for thread_id in 0..DATAGEN_THREADS {
        let thread_sender = sender.clone();
        let batches_for_thread = batches_per_thread + usize::from(thread_id < extra_batches);
        let thread_seed = SEED.wrapping_add((thread_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));

        handles.push(thread::spawn(move || {
            datagen_thread(thread_sender, batches_for_thread, KEY_SPACE, thread_seed)
        }));
    }

    drop(sender);
    handles
}

fn datagen_thread(sender: Sender<Batch>, batches: usize, key_space: u64, seed: u64) -> Result<()> {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    for _ in 0..batches {
        let mut batch = Vec::with_capacity(BATCH_SIZE);
        for _ in 0..BATCH_SIZE {
            let key = rng.gen_range(0..key_space);
            let value = Tup5(
                rng.next_u64(),
                rng.next_u64(),
                rng.next_u64(),
                rng.next_u64(),
                rng.next_u64(),
            );
            batch.push(Tup2(key, Update::Insert(value)));
        }

        sender
            .send(batch)
            .map_err(|_| anyhow!("receiver dropped while datagen was sending batches"))?;
    }

    Ok(())
}
