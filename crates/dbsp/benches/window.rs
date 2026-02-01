use dbsp::{
    Runtime,
    circuit::{
        CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
    },
    mimalloc::MiMalloc,
    typed_batch::TypedBox,
    utils::Tup2,
};
use feldera_types::config::{FileBackendConfig, StorageBackendConfig};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::time::Instant;
use tempfile::tempdir;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

/// Number of DBSP steps in the benchmark.
const STEPS: usize = 100_000;

/// The number of tuples generated per step.
const TUPLES_PER_STEP: usize = 500;

/// The range of values for randomly generated keys 0..KEY_RANGE.
const KEY_RANGE: u64 = 20_000_000;

/// The size of the window in the benchmark.
const WINDOW_SIZE: u64 = 10_000_000;

/// How much to move the start of the window per step.
/// NOTE: I see read amplification even with window step 0.
const WINDOW_STEP: u64 = /* 100 */ 0;

/// The length of the randomly generated value of type String.
const VALUE_LEN: usize = 100;

fn random_ascii_string(rng: &mut StdRng) -> String {
    let mut bytes = vec![0u8; VALUE_LEN];
    for byte in &mut bytes {
        *byte = rng.gen_range(b'a'..=b'z');
    }
    unsafe { String::from_utf8_unchecked(bytes) }
}

fn main() {
    let mut rng = StdRng::seed_from_u64(1);
    let mut bounds = Vec::with_capacity(STEPS);
    let mut inputs = Vec::with_capacity(STEPS);

    // Note input data must fit in memory.
    println!("Generating input data...");
    for step in 0..STEPS {
        let start = step as u64 * WINDOW_STEP;
        bounds.push((start, start + WINDOW_SIZE));

        let mut tuples = Vec::with_capacity(TUPLES_PER_STEP);
        for _ in 0..TUPLES_PER_STEP {
            let key = rng.gen_range(0..KEY_RANGE);
            let value = random_ascii_string(&mut rng);
            tuples.push(Tup2(key, Tup2(value, 1i64)));
        }
        inputs.push(tuples);
    }

    for (step, tuples) in inputs.iter().take(2).enumerate() {
        println!("sample step {step}:");
        for sample in tuples.iter().take(3) {
            println!("{sample:?}");
        }
    }

    let temp = tempdir().expect("Failed to create temp dir for storage");
    let storage_config = StorageConfig {
        path: temp.path().to_string_lossy().into_owned(),
        cache: StorageCacheConfig::default(),
    };
    let mut storage_options = StorageOptions::default();
    storage_options.backend = StorageBackendConfig::File(Box::new(FileBackendConfig::default()));
    //storage_options.min_storage_bytes = Some(0);
    let storage = CircuitStorageConfig::for_config(storage_config, storage_options).unwrap();
    let config = CircuitConfig::with_workers(8).with_storage(storage);

    let (mut circuit, (bounds_handle, input_handle, output_handle)) =
        Runtime::init_circuit(config, |circuit| {
            let (bounds, bounds_handle) = circuit.add_input_stream::<(u64, u64)>();
            let (input, input_handle) = circuit.add_input_indexed_zset::<u64, String>();

            let bounds = bounds.apply(|(start, end)| (TypedBox::new(*start), TypedBox::new(*end)));
            let output_handle = input.window((true, false), &bounds).accumulate_output();

            Ok((bounds_handle, input_handle, output_handle))
        })
        .unwrap();
    circuit.enable_cpu_profiler().unwrap();

    println!("Running circuit...");
    let start_time = Instant::now();
    for step in 0..STEPS {
        //println!("step {step}");
        bounds_handle.set_for_all(bounds[step]);
        let mut tuples = inputs[step].clone();
        input_handle.append(&mut tuples);
        circuit.transaction().unwrap();
        let _ = output_handle.take_from_all();
    }
    println!("runtime: {:?}", start_time.elapsed());
    let profile_dir = std::env::current_dir().unwrap().join("window-profile");
    let profile_path = circuit.dump_profile(&profile_dir).unwrap();
    println!("profile: {}", profile_path.display());
}
