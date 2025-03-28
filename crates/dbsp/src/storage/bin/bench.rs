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
//! - Threads indicate they're done writing but are still writing.

use libc::timespec;
use std::fs::create_dir_all;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;

use feldera_storage::backend::posixio_impl::PosixBackend;
use feldera_storage::backend::{AtomicIncrementOnlyI64, Storage};
use feldera_storage::buffer_cache::FBuf;

#[derive(Debug, Clone, Default)]
struct ThreadBenchResult {
    read_time: Duration,
    write_time: Duration,
    cpu_time: Duration,
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

    fn cpu_time_mean(&self) -> f64 {
        mean(
            &self
                .times
                .iter()
                .map(|t| t.cpu_time.as_secs_f64())
                .collect::<Vec<f64>>(),
        )
        .unwrap()
    }

    fn display(&self, args: Args) {
        let read_time = self.read_time_mean();
        let write_time = self.write_time_mean();
        let cpu_time = self.cpu_time_mean();
        const ONE_MIB: f64 = 1024f64 * 1024f64;

        if !args.csv {
            if !args.write_only {
                println!(
                    "read: {} MiB/s (mean: {}s, std: {}s)",
                    ((args.per_thread_file_size * args.threads) as f64 / ONE_MIB) / read_time,
                    read_time,
                    self.read_time_std()
                );
            }
            println!(
                "write: {} MiB/s (mean: {}s, std: {}s)",
                ((args.per_thread_file_size * args.threads) as f64 / ONE_MIB) / write_time,
                write_time,
                self.write_time_std()
            );
            println!("cpu: {}s (mean))", cpu_time,);
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
    Posix,
}

impl From<String> for Backend {
    fn from(s: String) -> Self {
        match s.as_str() {
            "Posix" => Backend::Posix,
            _ => panic!("invalid backend"),
        }
    }
}

/// Simple program to benchmark files.
///
/// Spawns multiple threads, each thread writes one file sequentially
/// and then reads it back.
///
/// The program prints read and write throughput, and the CPU time used by the
/// benchmark threads, which includes system and user time for those threads
/// (but not for other user or kernel threads spawned by them for I/O, if any).
#[derive(Parser, Debug, Clone)]
#[command(author, version)]
struct Args {
    /// Path to a file or directory
    #[clap(short, long, default_value = "/tmp/feldera-storage")]
    path: std::path::PathBuf,

    /// Which backend to use.
    #[clap(long, default_value = "Posix")]
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

    /// Write without reading back?
    #[clap(long, default_value = "false")]
    write_only: bool,

    /// Print data as CSV.
    #[clap(long, default_value = "false")]
    csv: bool,
}

fn allocate_buffer(sz: usize) -> FBuf {
    FBuf::with_capacity(sz)
}

/// Returns the amount of CPU time (user + system) used by the current thread.
///
/// It was difficult to determine that the result includes both user and system
/// time, so for future reference, see [the original commit] that added support,
/// which includes:
///
/// ```patch
/// +static inline unsigned long thread_ticks(task_t *p) {
/// +       return p->utime + current->stime;
/// +}
/// ```
///
/// [the original commit]: https://git.kernel.org/pub/scm/linux/kernel/git/tglx/history.git/commit/?id=bb82e8a53042a91688fd819d0c475a1c9a2b982a
fn thread_cpu_time() -> Duration {
    let mut tp = timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut tp as *mut timespec) };
    Duration::new(tp.tv_sec as u64, tp.tv_nsec as u32)
}

fn benchmark<T: Storage>(backend: &T, barrier: Arc<Barrier>) -> ThreadBenchResult {
    let args = Args::parse();
    let file = backend.create().unwrap();

    barrier.wait();
    let start_write = Instant::now();
    for i in 0..args.per_thread_file_size / args.buffer_size {
        let mut wb = allocate_buffer(args.buffer_size);
        wb.resize(args.buffer_size, 0xff);

        debug_assert!(i * args.buffer_size < args.per_thread_file_size);
        debug_assert!(wb.len() == args.buffer_size);
        backend
            .write_block(&file, (i * args.buffer_size) as u64, wb)
            .expect("write failed");
    }
    let (ih, _path) = backend.complete(file).expect("complete failed");
    let write_time = start_write.elapsed();

    barrier.wait();
    let start_read = Instant::now();
    if !args.write_only {
        for i in 0..args.per_thread_file_size / args.buffer_size {
            let rr = backend
                .read_block(&ih, (i * args.buffer_size) as u64, args.buffer_size)
                .expect("read failed");
            if args.verify {
                assert_eq!(rr.len(), args.buffer_size);
                assert_eq!(
                    rr.iter().as_slice(),
                    vec![0xffu8; args.buffer_size].as_slice()
                );
            }
        }
    }
    let read_time = start_read.elapsed();

    backend.delete(ih).expect("delete failed");
    ThreadBenchResult {
        write_time,
        read_time,
        cpu_time: thread_cpu_time(),
    }
}

fn posixio_main(args: Args) -> BenchResult {
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
                let posixio_backend = PosixBackend::new(args.path.clone(), counter);
                benchmark(&posixio_backend, barrier)
            })
        })
        .collect();

    // Run on main thread
    let posixio_backend = PosixBackend::new(args.path.clone(), counter);

    let mut br = BenchResult::default();
    let main_res = benchmark(&posixio_backend, barrier);
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

    let br = match args.backend {
        Backend::Posix => posixio_main(args.clone()),
    };

    br.display(args.clone());
    if !args.csv {
        if let Err(e) = br.validate() {
            println!("Result validation failed: {}", e);
            std::process::exit(1);
        }
    }
}
