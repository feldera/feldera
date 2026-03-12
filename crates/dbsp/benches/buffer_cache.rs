//! Throughput comparison between `feldera-buffer-cache`'s weighted LRU and
//! sharded S3-FIFO caches.
//!
//! `cargo bench -p dbsp --bench buffer_cache -- --write-ratios 0,10 --capacity-mib 1024 --strategy s3-fifo,lru --max-duration 30`
use clap::{Parser, ValueEnum};
use dbsp::mimalloc::MiMalloc;
use feldera_buffer_cache::{CacheEntry, LruCache, S3FifoCache};
use rand::rngs::ThreadRng;
use rand::{Rng, thread_rng};
use rand_distr::{Distribution, Zipf};
use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

const MIB: usize = 1024 * 1024;
const DEFAULT_THROUGHPUT_OPS_PER_RATIO: usize = 20_000_000;
const BLOCK_ALIGNMENT: u64 = 512;
const SHARDED_CACHE_SHARDS: usize = 256;
const SIZE_CLASSES: [usize; 6] = [
    4 * 1024,
    8 * 1024,
    16 * 1024,
    32 * 1024,
    64 * 1024,
    128 * 1024,
];

#[derive(Parser, Debug, Clone)]
#[command(name = "buffer_cache")]
#[command(about = "Multi-thread throughput scaling: weighted LRU vs sharded S3-FIFO")]
struct Args {
    /// Total cache capacity in MiB.
    #[arg(long, default_value_t = 256)]
    capacity_mib: usize,

    /// Key universe size.
    #[arg(long, default_value_t = 10_000_000)]
    key_space: usize,

    /// Operations per thread per write-ratio point.
    #[arg(long, default_value_t = DEFAULT_THROUGHPUT_OPS_PER_RATIO)]
    ops_per_ratio: usize,

    /// Comma-separated write ratios in percent.
    #[arg(long, default_value = "1,10,20,30,40,50")]
    write_ratios: String,

    /// Zipf skew parameter `a` for key-access distribution.
    #[arg(long, default_value_t = 1.1)]
    zipf_a: f64,

    /// Comma-separated thread counts.
    #[arg(long, default_value = "1,2,4,8")]
    threads: String,

    /// Comma-separated strategies to run: s3-fifo,lru.
    #[arg(long, value_delimiter = ',', default_value = "s3-fifo,lru")]
    strategy: Vec<Strategy>,

    /// Optional max duration (seconds) per run. If reached, stop early and report partial stats.
    #[arg(long)]
    max_duration: Option<f64>,

    #[doc(hidden)]
    #[arg(long = "bench", hide = true)]
    __bench: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Strategy {
    S3Fifo,
    Lru,
}

#[derive(Clone, Copy)]
struct Op {
    offset: u64,
    charge: usize,
    write: bool,
}

#[derive(Clone, Copy)]
struct Prefill {
    offset: u64,
    charge: usize,
}

#[derive(Clone)]
struct CacheValue(Arc<Vec<u8>>);

impl CacheValue {
    fn new(charge: usize) -> Self {
        Self(Arc::new(vec![0u8; charge]))
    }
}

impl CacheEntry for CacheValue {
    fn cost(&self) -> usize {
        self.0.len()
    }
}

#[derive(Default, Clone, Copy)]
struct RunStats {
    ops_per_sec: f64,
    total_reads: f64,
    total_writes: f64,
    hit_rate: f64,
    final_charge: usize,
}

trait BenchCache: Send + Sync + 'static {
    const NAME: &'static str;

    fn with_capacity(capacity_bytes: usize) -> Self;

    fn insert(&self, key: u64, value: CacheValue);

    fn get(&self, key: &u64) -> Option<CacheValue>;

    fn total_charge(&self) -> usize;
}

type S3FifoBenchCache = S3FifoCache<u64, CacheValue>;
type LruBenchCache = LruCache<u64, CacheValue>;

impl BenchCache for S3FifoBenchCache {
    const NAME: &'static str = "s3-fifo";

    fn with_capacity(capacity_bytes: usize) -> Self {
        Self::with_shards(capacity_bytes, SHARDED_CACHE_SHARDS)
    }

    fn insert(&self, key: u64, value: CacheValue) {
        self.insert(key, value);
    }

    fn get(&self, key: &u64) -> Option<CacheValue> {
        self.get(key)
    }

    fn total_charge(&self) -> usize {
        self.total_charge()
    }
}

impl BenchCache for LruBenchCache {
    const NAME: &'static str = "lru";

    fn with_capacity(capacity_bytes: usize) -> Self {
        Self::new(capacity_bytes)
    }

    fn insert(&self, key: u64, value: CacheValue) {
        self.insert(key, value);
    }

    fn get(&self, key: &u64) -> Option<CacheValue> {
        self.get(key)
    }

    fn total_charge(&self) -> usize {
        self.total_charge()
    }
}

fn sample_zipf_class(rng: &mut ThreadRng, zipf: &Zipf<f64>) -> usize {
    let rank = zipf.sample(rng) as usize;
    rank.saturating_sub(1).min(SIZE_CLASSES.len() - 1)
}

fn key_offset(key_idx: usize) -> u64 {
    key_idx as u64 * BLOCK_ALIGNMENT
}

fn parse_u8_csv(s: &str) -> Vec<u8> {
    let mut out: Vec<u8> = s
        .split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<u8>().expect("invalid u8 in CSV"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_usize_csv(s: &str) -> Vec<usize> {
    let mut out: Vec<usize> = s
        .split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<usize>().expect("invalid usize in CSV"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

fn build_universe(key_space: usize) -> Vec<usize> {
    let zipf = Zipf::new(SIZE_CLASSES.len() as u64, 1.2_f64).expect("valid zipf params");
    let mut rng = thread_rng();
    let mut charge_by_key = vec![0usize; key_space];

    for charge in &mut charge_by_key {
        let class = sample_zipf_class(&mut rng, &zipf);
        *charge = SIZE_CLASSES[class];
    }

    charge_by_key
}

fn build_prefill(charge_by_key: &[usize], capacity_bytes: usize) -> Vec<Prefill> {
    let mut prefill = Vec::new();
    let mut total = 0usize;
    for (key_idx, &charge) in charge_by_key.iter().enumerate() {
        prefill.push(Prefill {
            offset: key_offset(key_idx),
            charge,
        });
        total = total.saturating_add(charge);
        if total >= capacity_bytes {
            break;
        }
    }
    prefill
}

fn build_thread_traces(
    write_ratio: u8,
    ops_per_thread: usize,
    charge_by_key: &[usize],
    zipf_a: f64,
    threads: usize,
) -> Vec<Vec<Op>> {
    let key_space = charge_by_key.len();
    let zipf = Zipf::new(key_space as u64, zipf_a).expect("valid zipf params");

    (0..threads)
        .map(|_| {
            let mut rng = thread_rng();
            let mut trace = Vec::with_capacity(ops_per_thread);
            for _ in 0..ops_per_thread {
                let key_idx = (zipf.sample(&mut rng) as usize)
                    .saturating_sub(1)
                    .min(key_space.saturating_sub(1));
                trace.push(Op {
                    offset: key_offset(key_idx),
                    charge: charge_by_key[key_idx],
                    write: rng.gen_ratio(write_ratio as u32, 100),
                });
            }
            trace
        })
        .collect()
}

fn run_cache_mt<C>(
    prefill: &[Prefill],
    traces: &[Vec<Op>],
    capacity_bytes: usize,
    max_duration: Option<Duration>,
) -> RunStats
where
    C: BenchCache,
{
    let cache = Arc::new(C::with_capacity(capacity_bytes));
    let workers = traces.len();
    let prefill_done = Barrier::new(workers + 1);
    let ready = Barrier::new(workers + 1);
    let start_gate = Barrier::new(workers + 1);
    let run_start: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);

        for (worker_idx, trace) in traces.iter().enumerate() {
            let cache = cache.clone();
            let prefill_done = &prefill_done;
            let ready = &ready;
            let start_gate = &start_gate;
            let run_start = run_start.clone();
            let prefill_start = worker_idx * prefill.len() / workers;
            let prefill_end = (worker_idx + 1) * prefill.len() / workers;

            handles.push(scope.spawn(move || {
                for p in &prefill[prefill_start..prefill_end] {
                    cache.insert(p.offset, CacheValue::new(p.charge));
                }

                prefill_done.wait();
                ready.wait();
                start_gate.wait();
                let run_start = *run_start.get().expect("run start must be initialized");

                let mut ops = 0usize;
                let mut writes = 0usize;
                let mut reads = 0usize;
                let mut hits = 0usize;
                for op in trace {
                    if let Some(limit) = max_duration
                        && run_start.elapsed() >= limit
                    {
                        break;
                    }
                    ops += 1;
                    if op.write {
                        writes += 1;
                        cache.insert(op.offset, CacheValue::new(op.charge));
                    } else {
                        reads += 1;
                        if cache.get(&op.offset).is_some() {
                            hits += 1;
                        }
                    }
                }
                (ops, reads, writes, hits)
            }));
        }

        prefill_done.wait();
        ready.wait();
        let start = Instant::now();
        run_start
            .set(start)
            .expect("run start should only be set once");
        start_gate.wait();

        let mut total_ops = 0usize;
        let mut total_reads = 0usize;
        let mut total_writes = 0usize;
        let mut total_hits = 0usize;
        for handle in handles {
            let (ops, reads, writes, hits) = handle
                .join()
                .unwrap_or_else(|_| panic!("{} thread panicked", C::NAME));
            total_ops += ops;
            total_reads += reads;
            total_writes += writes;
            total_hits += hits;
        }

        let elapsed = start.elapsed().as_secs_f64();
        RunStats {
            ops_per_sec: total_ops as f64 / elapsed,
            total_reads: total_reads as f64,
            total_writes: total_writes as f64,
            hit_rate: if total_reads == 0 {
                0.0
            } else {
                total_hits as f64 / total_reads as f64
            },
            final_charge: cache.total_charge(),
        }
    })
}

fn main() {
    let args = Args::parse();
    let capacity_bytes = args.capacity_mib.saturating_mul(MIB);
    let write_ratios = parse_u8_csv(&args.write_ratios);
    let thread_counts = parse_usize_csv(&args.threads);
    let run_s3_fifo = args.strategy.contains(&Strategy::S3Fifo);
    let run_lru = args.strategy.contains(&Strategy::Lru);

    assert!(args.key_space > 0, "key-space must be > 0");
    assert!(!write_ratios.is_empty(), "write-ratios cannot be empty");
    assert!(!thread_counts.is_empty(), "threads cannot be empty");
    assert!(
        run_s3_fifo || run_lru,
        "strategy cannot be empty; choose one or more of s3-fifo,lru"
    );
    assert!(args.ops_per_ratio > 0, "ops-per-ratio must be > 0");
    assert!(args.zipf_a >= 0.0, "zipf-a must be >= 0");
    if let Some(max_duration) = args.max_duration {
        assert!(
            max_duration.is_finite() && max_duration > 0.0,
            "max-duration must be finite and > 0"
        );
    }
    for &ratio in &write_ratios {
        assert!(ratio <= 100, "write ratio must be in [0,100]");
    }
    for &threads in &thread_counts {
        assert!(threads > 0, "thread count must be > 0");
    }

    let charge_by_key = build_universe(args.key_space);
    let prefill = build_prefill(&charge_by_key, capacity_bytes);
    let max_duration = args.max_duration.map(Duration::from_secs_f64);
    let max_duration_str = args
        .max_duration
        .map_or_else(|| "none".to_string(), |value| format!("{value:.3}"));

    println!(
        "capacity_mib={}, key_space={}, base_ops_per_ratio={}, zipf_a={}, max_duration_s={}\n",
        args.capacity_mib, args.key_space, args.ops_per_ratio, args.zipf_a, max_duration_str
    );
    println!(
        "os,zipf_a,write_ratio,threads,ops_per_thread,total_ops,total_reads,total_writes,s3_fifo_ops_s,lru_ops_s,s3_fifo_hit,lru_hit,s3_fifo_charge,lru_charge,s3_fifo_scaling,lru_scaling,s3_fifo_vs_lru"
    );

    for &ratio in &write_ratios {
        let mut s3_fifo_base = None::<f64>;
        let mut lru_base = None::<f64>;

        for &threads in &thread_counts {
            let ops_per_thread = ((args.ops_per_ratio as f64) * (threads as f64) * 0.5)
                .round()
                .max(1.0) as usize;
            let total_ops = ops_per_thread.saturating_mul(threads);
            let traces =
                build_thread_traces(ratio, ops_per_thread, &charge_by_key, args.zipf_a, threads);
            let s3_fifo = run_s3_fifo.then(|| {
                run_cache_mt::<S3FifoBenchCache>(&prefill, &traces, capacity_bytes, max_duration)
            });
            let lru = run_lru.then(|| {
                run_cache_mt::<LruBenchCache>(&prefill, &traces, capacity_bytes, max_duration)
            });

            if max_duration.is_none() {
                let mut observed = Vec::new();
                if let Some(stats) = s3_fifo {
                    observed.push(("s3-fifo", stats));
                }
                if let Some(stats) = lru {
                    observed.push(("lru", stats));
                }
                if let Some((base_name, base_stats)) = observed.first().copied() {
                    for (name, stats) in observed.iter().skip(1).copied() {
                        assert!(
                            base_stats.total_reads == stats.total_reads
                                && base_stats.total_writes == stats.total_writes,
                            "read/write totals diverged between {} and {} at write_ratio={}, threads={}: {}(reads={}, writes={}) {}(reads={}, writes={})",
                            base_name,
                            name,
                            ratio,
                            threads,
                            base_name,
                            base_stats.total_reads,
                            base_stats.total_writes,
                            name,
                            stats.total_reads,
                            stats.total_writes
                        );
                    }
                }
            }

            let s3_fifo_base_value =
                s3_fifo.map(|stats| *s3_fifo_base.get_or_insert(stats.ops_per_sec));
            let lru_base_value = lru.map(|stats| *lru_base.get_or_insert(stats.ops_per_sec));

            let s3_fifo_scaling = match (s3_fifo, s3_fifo_base_value) {
                (Some(stats), Some(base)) if base > 0.0 => stats.ops_per_sec / base,
                (Some(_), _) => 0.0,
                (None, _) => f64::NAN,
            };
            let lru_scaling = match (lru, lru_base_value) {
                (Some(stats), Some(base)) if base > 0.0 => stats.ops_per_sec / base,
                (Some(_), _) => 0.0,
                (None, _) => f64::NAN,
            };
            let s3_fifo_vs_lru = match (s3_fifo, lru) {
                (Some(s3_fifo), Some(lru)) if lru.ops_per_sec > 0.0 => {
                    s3_fifo.ops_per_sec / lru.ops_per_sec
                }
                (Some(_), Some(_)) => 0.0,
                _ => f64::NAN,
            };
            let total_reads = s3_fifo
                .or(lru)
                .map(|stats| stats.total_reads)
                .unwrap_or(f64::NAN);
            let total_writes = s3_fifo
                .or(lru)
                .map(|stats| stats.total_writes)
                .unwrap_or(f64::NAN);
            let s3_fifo_charge = s3_fifo
                .map(|stats| stats.final_charge.to_string())
                .unwrap_or_else(|| "NaN".to_string());
            let lru_charge = lru
                .map(|stats| stats.final_charge.to_string())
                .unwrap_or_else(|| "NaN".to_string());

            println!(
                "{},{:.3},{},{},{},{},{:.0},{:.0},{:.0},{:.0},{:.3},{:.3},{},{},{:.3},{:.3},{:.3}",
                std::env::consts::OS,
                args.zipf_a,
                ratio,
                threads,
                ops_per_thread,
                total_ops,
                total_reads,
                total_writes,
                s3_fifo.map_or(f64::NAN, |stats| stats.ops_per_sec),
                lru.map_or(f64::NAN, |stats| stats.ops_per_sec),
                s3_fifo.map_or(f64::NAN, |stats| stats.hit_rate),
                lru.map_or(f64::NAN, |stats| stats.hit_rate),
                s3_fifo_charge,
                lru_charge,
                s3_fifo_scaling,
                lru_scaling,
                s3_fifo_vs_lru
            );
        }
    }
}
