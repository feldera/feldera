//! A simple CLI app to benchmark different storage backends/scenarios.
//!
//! An example invocation:
//!
//! ```shell
//! cargo run --release --bin bench --features metrics-exporter-tcp -- --cache --threads 2 --total-size 4294967296 --path /path/to/disk
//! ```
//!
//! Run `metrics-observer` in another terminal to see the metrics.
//!
//! There are still some issues with this benchmark to make it useful:
//! - Threads indicate they're done writing but are still writing, potentially
//!   async code is just wrong/needs join.

#![allow(async_fn_in_trait)]

use async_lock::Barrier;
use std::fs::create_dir_all;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use monoio::{FusionDriver, RuntimeBuilder};

#[cfg(feature = "glommio")]
use feldera_storage::backend::glommio_impl::GlommioBackend;

use feldera_storage::backend::monoio_impl::MonoioBackend;
use feldera_storage::backend::{AtomicIncrementOnlyI64, StorageControl, StorageRead, StorageWrite};
use feldera_storage::buffer_cache::FBuf;

#[derive(Debug, Clone, Default)]
struct ThreadBenchResult {
    read_time: Duration,
    write_time: Duration,
}

#[derive(Debug, Clone, Default)]
struct BenchResult {
    times: Vec<ThreadBenchResult>,
}

fn mean(data: &[f64]) -> Option<f64> {
    let sum = data.iter().sum::<f64>();
    let count = data.len();

    match count {
        positive if positive > 0 => Some(sum / count as f64),
        _ => None,
    }
}

fn std_deviation(data: &[f64]) -> Option<f64> {
    match (mean(data), data.len()) {
        (Some(data_mean), count) if count > 0 => {
            let variance = data
                .iter()
                .map(|value| {
                    let diff = data_mean - *value;

                    diff * diff
                })
                .sum::<f64>()
                / count as f64;

            Some(variance.sqrt())
        }
        _ => None,
    }
}

impl BenchResult {
    fn validate(&self) -> Result<(), String> {
        if self.times.is_empty() {
            return Err("No results found.".to_string());
        }
        assert!(!self.times.is_empty());

        if self.read_time_std() >= 2.0 {
            return Err("Read times are not stable.".to_string());
        }
        if self.write_time_std() >= 5.0 {
            return Err("Write times are not stable.".to_string());
        }
        Ok(())
    }

    fn read_time_std(&self) -> f64 {
        std_deviation(
            &self
                .times
                .iter()
                .map(|t| t.read_time.as_secs_f64())
                .collect::<Vec<f64>>(),
        )
        .unwrap()
    }

    fn write_time_std(&self) -> f64 {
        std_deviation(
            &self
                .times
                .iter()
                .map(|t| t.write_time.as_secs_f64())
                .collect::<Vec<f64>>(),
        )
        .unwrap()
    }

    fn read_time_mean(&self) -> f64 {
        mean(
            &self
                .times
                .iter()
                .map(|t| t.read_time.as_secs_f64())
                .collect::<Vec<f64>>(),
        )
        .unwrap()
    }

    fn write_time_mean(&self) -> f64 {
        mean(
            &self
                .times
                .iter()
                .map(|t| t.write_time.as_secs_f64())
                .collect::<Vec<f64>>(),
        )
        .unwrap()
    }

    fn display(&self, args: Args) {
        let read_time = self.read_time_mean();
        let write_time = self.write_time_mean();
        const ONE_MIB: f64 = 1024f64 * 1024f64;

        if !args.csv {
            println!(
                "read: {} MiB/s (mean: {}s, std: {}s)",
                ((args.per_thread_file_size * args.threads) as f64 / ONE_MIB) / read_time,
                read_time,
                self.read_time_std()
            );
            println!(
                "write: {} MiB/s (mean: {}s, std: {}s)",
                ((args.per_thread_file_size * args.threads) as f64 / ONE_MIB) / write_time,
                write_time,
                self.write_time_std()
            );
        } else {
            println!(
                "backend,cache,per_thread_file_size,threads,buffer_size,read_time,read_time_std,write_time,write_time_std",
            );
            println!(
                "{:?},{:?},{},{},{},{},{},{},{}",
                args.backend,
                args.cache,
                args.per_thread_file_size,
                args.threads,
                args.buffer_size,
                read_time,
                self.read_time_std(),
                write_time,
                self.write_time_std(),
            )
        }
    }
}

#[derive(Debug, Clone)]
enum Backend {
    #[cfg(feature = "glommio")]
    Glommio,
    Monoio,
}

impl From<String> for Backend {
    fn from(s: String) -> Self {
        match s.as_str() {
            #[cfg(feature = "glommio")]
            "Glommio" => Backend::Glommio,
            "Monoio" => Backend::Monoio,
            _ => panic!("invalid backend"),
        }
    }
}

/// Simple program to benchmark files.
/// Spawns multiple threads, each thread writes one file sequentially
/// and then reads it back.
///
/// The program prints the write and read throughput.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to a file or directory
    #[clap(short, long, default_value = "/tmp/feldera-storage")]
    path: std::path::PathBuf,

    /// Which backend to use.
    #[clap(long, default_value = "Monoio")]
    backend: Backend,

    /// Number of threads to use
    #[clap(long, default_value = "1")]
    threads: usize,

    /// Buffer size
    #[clap(long, default_value = "4096")]
    buffer_size: usize,

    /// Size that is to be written (per-thread)
    #[clap(long, default_value = "1073741824")]
    per_thread_file_size: usize,

    /// Verify file-operations are performed correctly.
    #[clap(long, default_value = "false")]
    verify: bool,

    /// Adds a buffer cache with given bytes of capacity.
    #[clap(long)]
    cache: Option<usize>,

    /// Print data as CSV.
    #[clap(long, default_value = "false")]
    csv: bool,
}

fn allocate_buffer(sz: usize) -> FBuf {
    FBuf::with_capacity(sz)
}

async fn benchmark<T: StorageControl + StorageWrite + StorageRead>(
    backend: T,
    barrier: Arc<Barrier>,
) -> ThreadBenchResult {
    let args = Args::parse();
    let file = backend.create().await.unwrap();

    barrier.wait_blocking();
    let start_write = Instant::now();
    for i in 0..args.per_thread_file_size / args.buffer_size {
        let mut wb = allocate_buffer(args.buffer_size);
        wb.resize(args.buffer_size, 0xff);

        debug_assert!(i * args.buffer_size < args.per_thread_file_size);
        debug_assert!(wb.len() == args.buffer_size);
        backend
            .write_block(&file, (i * args.buffer_size) as u64, wb)
            .await
            .expect("write failed");
    }
    let (ih, _path) = backend.complete(file).await.expect("complete failed");
    let write_time = start_write.elapsed();

    barrier.wait_blocking();
    let start_read = Instant::now();
    for i in 0..args.per_thread_file_size / args.buffer_size {
        let rr = backend
            .read_block(&ih, (i * args.buffer_size) as u64, args.buffer_size)
            .await
            .expect("read failed");
        if args.verify {
            assert_eq!(rr.len(), args.buffer_size);
            assert_eq!(
                rr.iter().as_slice(),
                vec![0xffu8; args.buffer_size].as_slice()
            );
        }
    }
    let read_time = start_read.elapsed();

    backend.delete(ih).await.expect("delete failed");
    ThreadBenchResult {
        write_time,
        read_time,
    }
}

#[cfg(feature = "glommio")]
fn glommio_main(args: Args) -> BenchResult {
    use glommio::{
        timer::Timer, DefaultStallDetectionHandler, LocalExecutorPoolBuilder, PoolPlacement,
    };

    let mut br = BenchResult::default();
    let counter: Arc<AtomicIncrementOnlyI64> = Default::default();
    let barrier = Arc::new(Barrier::new(args.threads));

    LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(args.threads))
        .ring_depth(4096)
        .spin_before_park(Duration::from_millis(10))
        .detect_stalls(Some(Box::new(|| Box::new(DefaultStallDetectionHandler {}))))
        .on_all_shards(|| async move {
            let barrier = barrier.clone();
            let counter = counter.clone();
            let backend = GlommioBackend::new(args.path.clone(), counter);
            Timer::new(Duration::from_millis(100)).await;
            benchmark(backend, barrier).await
        })
        .expect("failed to spawn local executors")
        .join_all()
        .into_iter()
        .map(|r| r.unwrap_or_else(|_e| panic!("unable to get result from benchmark thread")))
        .for_each(|tres| br.times.push(tres.clone()));

    br
}

fn monoio_main(args: Args) -> BenchResult {
    let counter: Arc<AtomicIncrementOnlyI64> = Default::default();
    let barrier = Arc::new(Barrier::new(args.threads));
    // spawn n-1 threads
    let threads: Vec<_> = (1..args.threads)
        .map(|_| {
            let args = args.clone();
            let barrier = barrier.clone();
            let counter = counter.clone();
            thread::spawn(move || {
                let barrier = barrier.clone();
                let monoio_backend = MonoioBackend::new(args.path.clone(), counter);
                let mut rt = RuntimeBuilder::<FusionDriver>::new()
                    .enable_timer()
                    .with_entries(4096)
                    .build()
                    .expect("Failed building the Runtime");
                rt.block_on(benchmark(monoio_backend, barrier))
            })
        })
        .collect();

    // Run on main thread
    let monoio_backend = MonoioBackend::new(args.path.clone(), counter);
    let mut rt = RuntimeBuilder::<FusionDriver>::new()
        .enable_timer()
        .with_entries(4096)
        .build()
        .expect("Failed building the Runtime");

    let mut br = BenchResult::default();
    let main_res = rt.block_on(benchmark(monoio_backend, barrier));
    br.times.push(main_res);

    // Wait for other n-1 threads
    threads.into_iter().for_each(|t| {
        let tres = t.join().expect("thread panicked");
        br.times.push(tres);
    });

    br
}

fn main() {
    let args = Args::parse();
    assert!(args.per_thread_file_size > 0);
    assert!(args.buffer_size > 0);
    assert!(args.per_thread_file_size >= args.buffer_size);
    assert!(args.threads > 0);
    if !args.path.exists() {
        create_dir_all(&args.path).expect("failed to create directory");
    }

    #[cfg(feature = "metrics-exporter-tcp")]
    {
        let builder = metrics_exporter_tcp::TcpBuilder::new();
        builder.install().expect("failed to install TCP exporter");
    }

    let br = match args.backend {
        #[cfg(feature = "glommio")]
        Backend::Glommio => glommio_main(args.clone()),
        Backend::Monoio => monoio_main(args.clone()),
    };

    br.display(args.clone());
    if !args.csv {
        if let Err(e) = br.validate() {
            println!("Result validation failed: {}", e);
            std::process::exit(1);
        }
    }
}
