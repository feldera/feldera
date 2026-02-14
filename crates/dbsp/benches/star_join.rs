//! Benchmark comparing star join operator vs chain of binary joins.
//!
//! Generates 5 streams of `OrdIndexedZSet<u64, Tup5<u64,u64,u64,u64,u64>>` and compares:
//! - Run 1: Chain of 4 join_index + 1 join (binary joins)
//! - Run 2: inner_star_join5 (star join)
//!
//! Output: `OrdZSet<Tup5<Tup5<...>, Tup5<...>, Tup5<...>, Tup5<...>, Tup5<...>>>` (25 u64s)

use dbsp::{
    NumEntries, Runtime,
    circuit::{
        CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
    },
    define_inner_star_join,
    mimalloc::MiMalloc,
    utils::{Tup2, Tup3, Tup4, Tup5},
};
use feldera_types::config::{FileBackendConfig, StorageBackendConfig};
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use size_of::HumanBytes;
use std::time::Instant;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

type Val = Tup5<u64, u64, u64, u64, u64>;

const SEED: u64 = 0x1234_5678_9abc_def0;

define_inner_star_join!(5);

fn generate_batch(
    rng: &mut ChaCha8Rng,
    num_keys: usize,
    key_range: u64,
    vals_per_key: usize,
) -> Vec<Tup2<u64, Tup2<Val, i64>>> {
    let mut data = Vec::with_capacity(num_keys * vals_per_key);
    for _ in 0..num_keys {
        let key = rng.gen_range(0..key_range);
        for _ in 0..vals_per_key {
            let val = Tup5(
                rand::Rng::r#gen(rng),
                rand::Rng::r#gen(rng),
                rand::Rng::r#gen(rng),
                rand::Rng::r#gen(rng),
                rand::Rng::r#gen(rng),
            );
            data.push(Tup2(key, Tup2(val, 1)));
        }
    }
    data
}

fn generate_5_batches(
    rng: &mut ChaCha8Rng,
    num_keys_per_batch: usize,
    key_range: u64,
    vals_per_key: usize,
) -> [Vec<Tup2<u64, Tup2<Val, i64>>>; 5] {
    let mut data = [Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    for i in 0..5 {
        data[i] = generate_batch(rng, num_keys_per_batch, key_range, vals_per_key);
    }
    data
}

fn circuit_config_with_posix_storage(
    workers: usize,
    storage_path: &std::path::Path,
) -> CircuitConfig {
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: storage_path.to_string_lossy().into_owned(),
            cache: StorageCacheConfig::default(),
        },
        StorageOptions {
            backend: StorageBackendConfig::File(Box::new(FileBackendConfig::default())),
            min_storage_bytes: Some(0),
            ..StorageOptions::default()
        },
    )
    .expect("POSIX storage config");

    CircuitConfig::from(workers).with_storage(storage)
}

fn run_binary_joins(
    data: Vec<[Vec<Tup2<u64, Tup2<Val, i64>>>; 5]>,
) -> (std::time::Duration, usize, HumanBytes) {
    let mut total_output_len = 0usize;
    let mut total_storage_size = HumanBytes::new(0);

    let tempdir = tempfile::tempdir().expect("temp dir for POSIX storage");
    let circuit_config = circuit_config_with_posix_storage(NUM_WORKERS, tempdir.path());

    let elapsed = std::thread::scope(|_| {
        let (mut dbsp, (h1, h2, h3, h4, h5, output_handle)) =
            Runtime::init_circuit(circuit_config, move |circuit| {
                let (s1, h1) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s2, h2) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s3, h3) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s4, h4) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s5, h5) = circuit.add_input_indexed_zset::<u64, Val>();

                // Chain: join_index 4 times, then join to produce OrdZSet<OutputVal>
                let out = s1
                    .join_index(&s2, |k, v1, v2| {
                        std::iter::once((*k, Tup2(v1.clone(), v2.clone())))
                    })
                    .join_index(&s3, |k, t, v3| {
                        std::iter::once((*k, Tup3(t.0.clone(), t.1.clone(), v3.clone())))
                    })
                    .join_index(&s4, |k, t, v4| {
                        std::iter::once((
                            *k,
                            Tup4(t.0.clone(), t.1.clone(), t.2.clone(), v4.clone()),
                        ))
                    })
                    .join(&s5, |_k, t, v5| {
                        Tup5(
                            t.0.clone(),
                            t.1.clone(),
                            t.2.clone(),
                            t.3.clone(),
                            v5.clone(),
                        )
                    });

                let output_handle = out.accumulate_output();

                Ok((h1, h2, h3, h4, h5, output_handle))
            })
            .unwrap();

        let start = Instant::now();
        for (i, mut batches) in data.into_iter().enumerate() {
            println!("step: {}", i);
            h1.append(&mut batches[0]);
            h2.append(&mut batches[1]);
            h3.append(&mut batches[2]);
            h4.append(&mut batches[3]);
            h5.append(&mut batches[4]);
            dbsp.transaction().unwrap();
        }
        let elapsed = start.elapsed();

        let output = output_handle.concat().consolidate();
        total_output_len = output.num_entries_shallow();

        let profile = dbsp.retrieve_profile().unwrap();
        total_storage_size = profile.total_storage_size().unwrap();

        elapsed
    });

    (elapsed, total_output_len, total_storage_size)
}

fn run_star_join(
    data: Vec<[Vec<Tup2<u64, Tup2<Val, i64>>>; 5]>,
) -> (std::time::Duration, usize, HumanBytes) {
    let mut total_output_len = 0usize;
    let mut total_storage_size = HumanBytes::new(0);

    let tempdir = tempfile::tempdir().expect("temp dir for POSIX storage");
    let circuit_config = circuit_config_with_posix_storage(NUM_WORKERS, tempdir.path());

    let elapsed = std::thread::scope(|_| {
        let (mut dbsp, (h1, h2, h3, h4, h5, output_handle)) =
            Runtime::init_circuit(circuit_config, move |circuit| {
                let (s1, h1) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s2, h2) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s3, h3) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s4, h4) = circuit.add_input_indexed_zset::<u64, Val>();
                let (s5, h5) = circuit.add_input_indexed_zset::<u64, Val>();

                // inner_star_join5: join all 5 streams at once
                let out = inner_star_join5(&s1, &s2, &s3, &s4, &s5, |_k, v1, v2, v3, v4, v5| {
                    Tup5(v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone())
                });

                let output_handle = out.accumulate_output();

                Ok((h1, h2, h3, h4, h5, output_handle))
            })
            .unwrap();

        let start = Instant::now();
        for (i, mut batches) in data.into_iter().enumerate() {
            // println!("step: {}", i);
            h1.append(&mut batches[0]);
            h2.append(&mut batches[1]);
            h3.append(&mut batches[2]);
            h4.append(&mut batches[3]);
            h5.append(&mut batches[4]);
            dbsp.transaction().unwrap();
        }
        let elapsed = start.elapsed();

        let output = output_handle.concat().consolidate();
        total_output_len = output.num_entries_shallow();

        let profile = dbsp.retrieve_profile().unwrap();
        //println!("  Profile: {:?}", profile);
        total_storage_size = profile.total_storage_size().unwrap();

        elapsed
    });

    (elapsed, total_output_len, total_storage_size)
}

const NUM_KEYS_PER_BATCH: usize = 10000;
const VALS_PER_KEY: usize = 1;
const NUM_WORKERS: usize = 4;

const NUM_DENSE_STEPS: usize = 10;
const DENSE_KEY_RANGE: u64 = NUM_KEYS_PER_BATCH as u64 * 2;

const NUM_SPARSE_STEPS: usize = 100;
const SPARSE_KEY_RANGE: u64 = 1000000;

fn run_benchmark(data: Vec<[Vec<Tup2<u64, Tup2<Val, i64>>>; 5]>) {
    println!("\n--- Binary joins (chain of join_index + join) ---");
    let (binary_elapsed, binary_output_len, binary_storage) = run_binary_joins(data.clone());
    println!(
        "  Time: {:?} ({} output tuples)",
        binary_elapsed, binary_output_len
    );
    println!("  Total storage size: {}", binary_storage);
    println!("  Per step: {:?}", binary_elapsed / data.len() as u32);

    println!("\n--- Star join (inner_star_join5) ---");
    let (star_elapsed, star_output_len, star_storage) = run_star_join(data.clone());
    println!(
        "  Time: {:?} ({} output tuples)",
        star_elapsed, star_output_len
    );
    println!("  Total storage size: {}", star_storage);
    println!("  Per step: {:?}", star_elapsed / data.len() as u32);

    println!("\n--- Summary ---");
    assert_eq!(
        binary_output_len, star_output_len,
        "Output counts should match"
    );
    let speedup = binary_elapsed.as_secs_f64() / star_elapsed.as_secs_f64();
    println!(
        "  Star join is {:.2}x {} than binary joins",
        speedup,
        if speedup > 1.0 { "faster" } else { "slower" }
    );
}

fn main() {
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);

    println!(
        "Generating data: {} keys/batch (key range: {}) × {} vals/key × 5 streams × {} steps",
        NUM_KEYS_PER_BATCH, SPARSE_KEY_RANGE, VALS_PER_KEY, NUM_SPARSE_STEPS
    );

    let mut data = Vec::with_capacity(NUM_SPARSE_STEPS);
    for _ in 0..NUM_SPARSE_STEPS {
        data.push(generate_5_batches(
            &mut rng,
            NUM_KEYS_PER_BATCH,
            SPARSE_KEY_RANGE,
            VALS_PER_KEY,
        ));
    }

    run_benchmark(data);

    println!(
        "Generating data: {} keys/batch (key range: {}) × {} vals/key × 5 streams × {} steps",
        NUM_KEYS_PER_BATCH, DENSE_KEY_RANGE, VALS_PER_KEY, NUM_DENSE_STEPS
    );

    let mut data = Vec::with_capacity(NUM_DENSE_STEPS);
    for _ in 0..NUM_DENSE_STEPS {
        data.push(generate_5_batches(
            &mut rng,
            NUM_KEYS_PER_BATCH,
            DENSE_KEY_RANGE,
            VALS_PER_KEY,
        ));
    }

    run_benchmark(data);
}
