use std::env;
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::time::{Duration, Instant};

use crossbeam::channel::{TryRecvError, TrySendError, bounded};
use dbsp::utils::Tup2;

const DEFAULT_THREADS: &[usize] = &[1, 2, 4, 8, 12, 16, 20, 24];
const DEFAULT_BATCH_SIZE: usize = 10_000;
const DEFAULT_DURATION_SECS: u64 = 120;
const PRODUCERS_PER_CONSUMER: usize = 8;
const MAX_BUFFERED_RECORDS: usize = 500_000_000;

#[derive(Debug)]
struct WorkerResult {
    records: u64,
    bytes: u64,
    elapsed: Duration,
}

struct BatchPayload {
    tuples: Vec<Tup2<Tup2<u64, ()>, u64>>,
    len: usize,
}

#[test]
#[ignore]
fn u64_sort_tput() {
    let threads_list = parse_csv_env("THREADS", DEFAULT_THREADS);
    let batch_sizes = parse_batch_sizes();
    let duration_secs = parse_env_u64("DURATION", DEFAULT_DURATION_SECS);

    let mut csv_lines = Vec::new();
    for &threads in &threads_list {
        for &batch_size in &batch_sizes {
            eprintln!(
                "starting experiment: threads={threads}, batch_size={batch_size}, duration_s={duration_secs}"
            );
            let line = run_one_case(threads, duration_secs, batch_size);
            csv_lines.push(line);
        }
    }

    println!("threads,records,batch_size,bytes,elapsed_s,bytes_per_sec,records_per_sec");
    for line in csv_lines {
        println!("{line}");
    }
}

fn run_one_case(threads: usize, duration_secs: u64, batch_size: usize) -> String {
    assert!(threads > 0, "threads must be positive");
    assert!(duration_secs > 0, "duration must be positive");
    assert!(batch_size > 0, "batch_size must be positive");

    let barrier = Arc::new(Barrier::new(threads));
    let duration = Duration::from_secs(duration_secs);
    let (tx, rx) = mpsc::channel::<WorkerResult>();
    let mut handles = Vec::with_capacity(threads);

    for worker_index in 0..threads {
        let barrier = barrier.clone();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
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
                producers.push(std::thread::spawn(move || {
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
                        let mut payload = generate_batch_u64(batch_size, &mut rng_state);
                        loop {
                            if stop.load(Ordering::Acquire) {
                                return;
                            }
                            match batch_tx.try_send(payload) {
                                Ok(()) => break,
                                Err(TrySendError::Full(p)) => {
                                    payload = p;
                                    std::thread::yield_now();
                                }
                                Err(TrySendError::Disconnected(_)) => return,
                            }
                        }
                    }
                }));
            }

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

            loop {
                let now = Instant::now();
                if now >= end_time {
                    break;
                }
                match batch_rx.try_recv() {
                    Ok(mut payload) => {
                        payload.tuples.sort_unstable();
                        records += payload.len as u64;
                        bytes += payload.len as u64 * 8;
                        std::mem::forget(payload);
                    }
                    Err(TryRecvError::Empty) => {
                        eprintln!("warning: empty buffer, increase producers or pre-buffering");
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
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
        }));
    }

    drop(tx);

    let mut results = Vec::with_capacity(threads);
    for _ in 0..threads {
        results.push(rx.recv().expect("worker result"));
    }
    for handle in handles {
        let _ = handle.join();
    }

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
        "{threads},{total_records},{batch_size},{total_bytes},{elapsed_s:.6},{bytes_per_sec:.2},{records_per_sec:.2}"
    )
}

fn generate_batch_u64(batch_size: usize, rng: &mut u64) -> BatchPayload {
    let mut tuples = Vec::with_capacity(batch_size);
    for _ in 0..batch_size {
        *rng ^= *rng << 13;
        *rng ^= *rng >> 7;
        *rng ^= *rng << 17;
        tuples.push(Tup2(Tup2(*rng, ()), 1u64));
    }
    BatchPayload {
        tuples,
        len: batch_size,
    }
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    match env::var(name) {
        Ok(value) => value.parse().unwrap_or(default),
        Err(_) => default,
    }
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
