//! A simple CLI app to benchmark different storage backends/scenarios.

#![allow(async_fn_in_trait)]

use std::cmp::max;
use std::fs::create_dir_all;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use monoio::{FusionDriver, RuntimeBuilder};

#[cfg(feature = "glommio")]
use feldera_storage::backend::glommio_impl::GlommioBackend;

use feldera_storage::backend::monoio_impl::MonoioBackend;
use feldera_storage::backend::{StorageControl, StorageRead, StorageWrite};
use feldera_storage::buffer_cache::{BufferCache, FBuf};

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
    total_size: usize,

    /// Verify file-operations are performed correctly.
    #[clap(long, default_value = "false")]
    verify: bool,

    /// Add the buffer-cache.
    #[clap(long, default_value = "false")]
    cache: bool,
}

fn allocate_buffer(sz: usize) -> FBuf {
    FBuf::with_capacity(sz)
}

async fn benchmark<T: StorageControl + StorageWrite + StorageRead>(
    backend: T,
) -> (Duration, Duration) {
    let args = Args::parse();
    let file = backend.create().await.unwrap();

    let mut wbuffers = Vec::with_capacity(args.total_size / args.buffer_size);
    for _i in 0..args.total_size / args.buffer_size {
        let mut buf = allocate_buffer(args.buffer_size);
        buf.resize(args.buffer_size, 0);
        wbuffers.push(buf);
    }

    let start_write = Instant::now();
    for i in 0..args.total_size / args.buffer_size {
        let wb = wbuffers.pop().unwrap();
        debug_assert!(i * args.buffer_size < args.total_size);
        debug_assert!(wb.len() == args.buffer_size);
        backend
            .write_block(&file, (i * args.buffer_size) as u64, wb)
            .await
            .expect("write failed");
    }
    let write_time = start_write.elapsed();

    let ih = backend.complete(file).await.expect("complete failed");

    let start_read = Instant::now();
    let mut ops = vec![];
    for i in 0..args.total_size / args.buffer_size {
        ops.push(backend.read_block(&ih, (i * args.buffer_size) as u64, args.buffer_size));
    }
    for (i, op) in ops.into_iter().enumerate() {
        let rr = op.await.unwrap_or_else(|_| panic!("read {i} failed"));

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
    (write_time, read_time)
}

#[cfg(feature = "glommio")]
fn glommio_main(args: Args) -> (Duration, Duration) {
    use glommio::{
        timer::Timer, DefaultStallDetectionHandler, LocalExecutorPoolBuilder, PoolPlacement,
    };

    let mut write_time = Duration::ZERO;
    let mut read_time = Duration::ZERO;

    LocalExecutorPoolBuilder::new(PoolPlacement::Unbound(args.threads))
        .ring_depth(4096)
        .spin_before_park(Duration::from_millis(10))
        .detect_stalls(Some(Box::new(|| Box::new(DefaultStallDetectionHandler {}))))
        .on_all_shards(|| async move {
            let backend = GlommioBackend::new(args.path.clone());
            Timer::new(Duration::from_millis(100)).await;
            benchmark(backend).await
        })
        .expect("failed to spawn local executors")
        .join_all()
        .into_iter()
        .map(|r| match r {
            Ok(r) => r,
            Err(_e) => {
                panic!("unable to get result from benchmark thread")
            }
        })
        .for_each(|(w, r)| {
            // TODO: ideally we should determine if we have a high std-dev here and abort
            // (for now this must be done by hand by inspecting the individual thread times)
            write_time = max(write_time, w);
            read_time = max(read_time, r);
        });

    (write_time, read_time)
}

fn monoio_main(args: Args) -> (Duration, Duration) {
    // spawn n-1 threads
    let threads: Vec<_> = (1..args.threads)
        .map(|_| {
            let args = args.clone();
            thread::spawn(move || {
                let monoio_backend = MonoioBackend::new(args.path.clone());
                if args.cache {
                    RuntimeBuilder::<FusionDriver>::new()
                        .enable_timer()
                        .with_entries(32768)
                        .build()
                        .expect("Failed building the Runtime")
                        .block_on(benchmark(BufferCache::with_backend(monoio_backend)))
                } else {
                    RuntimeBuilder::<FusionDriver>::new()
                        .enable_timer()
                        .with_entries(32768)
                        .build()
                        .expect("Failed building the Runtime")
                        .block_on(benchmark(monoio_backend))
                }
            })
        })
        .collect();

    // Run on main thread
    let monoio_backend = MonoioBackend::new(args.path.clone());
    let (mut write_time, mut read_time) = if args.cache {
        RuntimeBuilder::<FusionDriver>::new()
            .enable_timer()
            .with_entries(32768)
            .build()
            .expect("Failed building the Runtime")
            .block_on(benchmark(BufferCache::with_backend(monoio_backend)))
    } else {
        RuntimeBuilder::<FusionDriver>::new()
            .enable_timer()
            .with_entries(32768)
            .build()
            .expect("Failed building the Runtime")
            .block_on(benchmark(monoio_backend))
    };

    // Wait for other n-1 threads
    threads.into_iter().for_each(|t| {
        let (r, w) = t.join().expect("thread panicked");
        write_time = max(write_time, w);
        read_time = max(read_time, r);
    });

    (write_time, read_time)
}

fn main() {
    let args = Args::parse();
    assert!(args.total_size > 0);
    assert!(args.buffer_size > 0);
    assert!(args.total_size >= args.buffer_size);
    assert!(args.threads > 0);
    if !args.path.exists() {
        create_dir_all(&args.path).expect("failed to create directory");
    }

    #[cfg(feature = "metrics-exporter-tcp")]
    {
        let builder = metrics_exporter_tcp::TcpBuilder::new();
        builder.install().expect("failed to install TCP exporter");
    }

    let (write_time, read_time) = match args.backend {
        #[cfg(feature = "glommio")]
        Backend::Glommio => glommio_main(args.clone()),
        Backend::Monoio => monoio_main(args.clone()),
    };

    println!(
        "write: {} MiB/s",
        ((args.total_size * args.threads) as f64 / (1024f64 * 1024f64)) / write_time.as_secs_f64()
    );
    println!(
        "read: {} MiB/s",
        ((args.total_size * args.threads) as f64 / (1024f64 * 1024f64)) / read_time.as_secs_f64()
    );
}
