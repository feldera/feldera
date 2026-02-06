use std::env;
use std::path::Path;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

use crossbeam::channel::{TryRecvError, TrySendError, bounded};
use dbsp::DBData;
use dbsp::DynZWeight;
use dbsp::Runtime;
use dbsp::ZWeight;
use dbsp::circuit::metadata::OperatorMeta;
use dbsp::circuit::{
    CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
};
use dbsp::dynamic::{DynData, DynUnit, DynWeightedPairs, Erase, LeanVec, pair::DynPair};
use dbsp::trace::ord::{FallbackWSet, FallbackWSetFactories};
use dbsp::trace::{Batch, BatchFactories, BatchReaderFactories, Batcher, MergerType, Spine, Trace};
use dbsp::utils::{Tup1, Tup2};
use feldera_sqllib::SqlString;

type DynPairs = Box<DynWeightedPairs<DynPair<DynData, DynUnit>, DynZWeight>>;

#[derive(Debug)]
struct WorkerResult {
    records: u64,
    bytes: u64,
    elapsed: Duration,
}

const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8, 12, 16, 20, 24];
const DEFAULT_INPUT_SIZES: &[usize] = &[8, 16, 32, 64, 128, 256, 512, 1024];
const DEFAULT_BATCH_SIZE: usize = 10_000;
const DEFAULT_DURATION_SECS: u64 = 120;
const PRODUCERS_PER_CONSUMER: usize = 8;
const MAX_BUFFERED_RECORDS: usize = 500_000_000;
const MERGER_TYPE: MergerType = MergerType::ListMerger;
const INPUT_TYPE_STR: &str = "str";
const INPUT_TYPE_U64: &str = "u64";

#[derive(Copy, Clone, Debug)]
enum InputType {
    Str,
    U64,
}

#[test]
#[ignore]
fn spine_ingest_tput_file_storage() {
    let threads_list = parse_csv_env("THREADS", DEFAULT_THREADS);
    let input_type = parse_input_type();
    let input_sizes = match input_type {
        InputType::U64 => vec![8],
        InputType::Str => parse_csv_env("INPUT_SIZES", DEFAULT_INPUT_SIZES),
    };
    let batch_sizes = parse_batch_sizes();
    let duration_secs = parse_env_u64("DURATION", DEFAULT_DURATION_SECS);
    let storage_path = env::var("STORAGE_PATH")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut csv_lines = Vec::new();
    for &threads in &threads_list {
        for &input_size in &input_sizes {
            for &batch_size in &batch_sizes {
                eprintln!(
                    "starting experiment: input_type={:?}, threads={threads}, input_size={input_size}, batch_size={batch_size}, duration_s={duration_secs}",
                    input_type
                );
                let line = match input_type {
                    InputType::Str => run_one_case::<Tup1<SqlString>>(
                        threads,
                        input_size,
                        duration_secs,
                        batch_size,
                        storage_path.as_deref(),
                        generate_batch_str,
                    ),
                    InputType::U64 => run_one_case::<Tup1<u64>>(
                        threads,
                        input_size,
                        duration_secs,
                        batch_size,
                        storage_path.as_deref(),
                        generate_batch_u64,
                    ),
                };
                csv_lines.push(line);
            }
        }
    }

    println!("threads,input_size,records,batch_size,bytes,elapsed_s,bytes_per_sec,records_per_sec");
    for line in csv_lines {
        println!("{line}");
    }
}

fn run_one_case<K>(
    threads: usize,
    input_size: usize,
    duration_secs: u64,
    batch_size: usize,
    storage_path: Option<&str>,
    generate_batch: fn(usize, usize, &mut u64) -> BatchPayload,
) -> String
where
    K: DBData + Erase<DynData>,
{
    assert!(threads > 0, "threads must be positive");
    assert!(duration_secs > 0, "duration must be positive");
    assert!(input_size > 0, "input size must be positive");
    assert!(batch_size > 0, "batch_size must be positive");
    let barrier = std::sync::Arc::new(Barrier::new(threads));

    let (storage_path, _tempdir) = match storage_path {
        Some(path) => (prepare_storage_path(path), None),
        None => {
            let tempdir = tempfile::tempdir().expect("tempdir");
            (tempdir.path().to_string_lossy().into_owned(), Some(tempdir))
        }
    };
    let storage = CircuitStorageConfig::for_config(
        StorageConfig {
            path: storage_path,
            cache: StorageCacheConfig::default(),
        },
        StorageOptions::default(),
    )
    .expect("storage config");

    let mut config = CircuitConfig::with_workers(threads).with_storage(storage);
    config.dev_tweaks.merger = MERGER_TYPE;

    let (tx, rx) = mpsc::channel::<WorkerResult>();
    let duration = Duration::from_secs(duration_secs);

    let runtime = Runtime::run(config, {
        let barrier = barrier.clone();
        let tx = tx.clone();
        move |_parker| {
            let worker_index = Runtime::local_worker_offset();
            let stop = Arc::new(AtomicBool::new(false));
            let capacity_batches = std::cmp::max(1, MAX_BUFFERED_RECORDS / batch_size);
            let (batch_tx, batch_rx) = bounded::<BatchPayload>(capacity_batches);

            let mut producers = Vec::with_capacity(PRODUCERS_PER_CONSUMER);
            for producer_idx in 0..PRODUCERS_PER_CONSUMER {
                let stop = stop.clone();
                let batch_tx = batch_tx.clone();
                let log_queue = worker_index == 0 && producer_idx == 0;
                let log_sender = batch_tx.clone();
                let mut rng_state = (worker_index as u64)
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    .wrapping_add(producer_idx as u64 + 1);
                let thread_name = format!("b-producer-{worker_index}-{producer_idx}");
                producers.push(
                    std::thread::Builder::new()
                        .name(thread_name)
                        .spawn(move || {
                    let mut next_log = Instant::now() + Duration::from_secs(10);
                    loop {
                        if stop.load(Ordering::Acquire) {
                            break;
                        }
                        if log_queue && Instant::now() >= next_log {
                            let batches = log_sender.len();
                            let records = batches * batch_size;
                            eprintln!("buffered_records={records}");
                            next_log += Duration::from_secs(10);
                        }
                        let mut payload = generate_batch(batch_size, input_size, &mut rng_state);
                        loop {
                            if stop.load(Ordering::Acquire) {
                                return;
                            }
                            match batch_tx.try_send(payload) {
                                Ok(()) => break,
                                Err(TrySendError::Full(p)) => {
                                    payload = p;
                                    //std::thread::yield_now();
                                }
                                Err(TrySendError::Disconnected(_)) => return,
                            }
                        }
                    }
                        })
                        .expect("spawn b-producer thread"),
                );
            }

            let factories = FallbackWSetFactories::<DynData, DynZWeight>::new::<K, (), ZWeight>();
            let mut spine: Spine<FallbackWSet<DynData, DynZWeight>> = Spine::new(&factories);

            // Wait until the producers have filled the queue at least once.
            while batch_rx.len() < capacity_batches {
                std::thread::yield_now();
            }
            if worker_index == 0 {
                eprintln!("prefill complete (queue full: {capacity_batches} batches)");
            }

            barrier.wait();
            let start = Instant::now();
            let end_time = start + duration;
            let mut records: u64 = 0;
            let mut bytes: u64 = 0;
            let mut pairs = factories.weighted_items_factory().default_box();

            loop {
                let now = Instant::now();
                if now >= end_time {
                    break;
                }
                match batch_rx.try_recv() {
                    Ok(mut payload) => {
                        let mut batcher =
                            <FallbackWSet<_, _> as Batch>::Batcher::new_batcher(&factories, ());
                        pairs.from_pairs(payload.tuples.as_mut());
                        batcher.push_batch(&mut pairs);
                        let batch = batcher.seal();

                        spine.insert(batch);
                        records += payload.len as u64;
                        bytes += payload.len as u64 * input_size as u64;
                    }
                    Err(TryRecvError::Empty) => {
                        eprintln!("warning: empty buffer, increase producers or pre-buffering");
                        //unreachable!("should not be emtpy");
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            let mut operator_metadata: OperatorMeta = OperatorMeta::new();
            spine.metadata(&mut operator_metadata);
            eprintln!("{:#?}", operator_metadata);
            let elapsed = start.elapsed();

            stop.store(true, Ordering::Release);
            drop(batch_rx);
            for producer in producers {
                let _ = producer.join();
            }

            tx.send(WorkerResult {
                records,
                bytes,
                elapsed,
            })
            .expect("send worker result");
        }
    })
    .expect("runtime");

    drop(tx);

    let mut results = Vec::with_capacity(threads);
    for _ in 0..threads {
        results.push(rx.recv().expect("worker result"));
    }
    runtime.join().expect("runtime join");

    let total_bytes: u64 = results.iter().map(|r| r.bytes).sum();
    let total_records: u64 = results.iter().map(|r| r.records).sum();
    let elapsed = results
        .iter()
        .map(|r| r.elapsed)
        .max()
        .unwrap_or_else(|| Duration::from_secs(0));

    let elapsed_s = elapsed.as_secs_f64();
    let bytes_per_sec = if elapsed_s > 0.0 {
        total_bytes as f64 / elapsed_s
    } else {
        0.0
    };
    let records_per_sec = if elapsed_s > 0.0 {
        total_records as f64 / elapsed_s
    } else {
        0.0
    };

    format!(
        "{threads},{input_size},{total_records},{batch_size},{total_bytes},{elapsed_s:.6},{bytes_per_sec:.2},{records_per_sec:.2}"
    )
}

struct BatchPayload {
    tuples: DynPairs,
    len: usize,
}

fn generate_batch_str(batch_size: usize, input_size: usize, rng: &mut u64) -> BatchPayload {
    let mut tuples = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        let s = random_ascii_string(input_size, rng);
        let key = Tup1(SqlString::from(s));
        tuples.push(Tup2(Tup2(key, ()), 1));
    }
    BatchPayload {
        tuples: Box::new(LeanVec::from(tuples)).erase_box(),
        len: batch_size,
    }
}

fn generate_batch_u64(batch_size: usize, _input_size: usize, rng: &mut u64) -> BatchPayload {
    let mut tuples = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        let key = Tup1(*rng);
        tuples.push(Tup2(Tup2(key, ()), 1));
    }
    BatchPayload {
        tuples: Box::new(LeanVec::from(tuples)).erase_box(),
        len: batch_size,
    }
}

fn random_ascii_string(len: usize, state: &mut u64) -> String {
    let mut bytes = vec![0u8; len];
    for b in &mut bytes {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *b = b'a' + ((*state >> 24) as u8 % 26);
    }
    String::from_utf8(bytes).expect("valid ascii")
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(value) => value.parse().unwrap_or(default),
        Err(_) => default,
    }
}

fn parse_input_type() -> InputType {
    match env::var("INPUT_TYPE") {
        Ok(value) => {
            let value = value.trim().to_ascii_lowercase();
            if value == INPUT_TYPE_U64 {
                InputType::U64
            } else if value == INPUT_TYPE_STR {
                InputType::Str
            } else {
                panic!(
                    "Invalid INPUT_TYPE '{value}', expected '{INPUT_TYPE_STR}' or '{INPUT_TYPE_U64}'"
                );
            }
        }
        Err(_) => InputType::Str,
    }
}

fn prepare_storage_path(path: &str) -> String {
    let trimmed = path.trim();
    assert!(!trimmed.is_empty(), "STORAGE_PATH must not be empty");
    let storage_path = Path::new(trimmed);
    assert!(
        storage_path != Path::new("/"),
        "refusing to remove root directory"
    );
    assert!(
        storage_path != Path::new("."),
        "refusing to remove current directory"
    );

    if storage_path.exists() {
        std::fs::remove_dir_all(storage_path).expect("clear storage dir");
    }
    std::fs::create_dir_all(storage_path).expect("create storage dir");
    storage_path.to_string_lossy().into_owned()
}

fn parse_batch_sizes() -> Vec<usize> {
    parse_csv_env("BATCH_SIZES", &[DEFAULT_BATCH_SIZE])
}

fn parse_csv_env(name: &str, defaults: &[usize]) -> Vec<usize> {
    let Ok(value) = env::var(name) else {
        return defaults.to_vec();
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return defaults.to_vec();
    }
    trimmed
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| {
            let parsed = item
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("Invalid {name} entry: {item}"));
            if parsed == 0 {
                panic!("{name} entries must be positive: {item}");
            }
            parsed
        })
        .collect()
}
