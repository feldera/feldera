//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.

use anyhow::{anyhow, Result};
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::circuit::{CircuitConfig, StorageCacheConfig, StorageConfig};
use dbsp::storage::backend::metrics::{
    BUFFER_CACHE_HIT, BUFFER_CACHE_MISS, FILES_CREATED, READS_SUCCESS, TOTAL_BYTES_READ,
    TOTAL_BYTES_WRITTEN, WRITES_SUCCESS,
};
use dbsp::storage::backend::tempdir_for_thread;
use dbsp::utils::Tup2;
use dbsp::{
    mimalloc::{AllocStats, MiMalloc},
    DBSPHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
};
use dbsp_nexmark::{
    config::Config as NexmarkConfig,
    model::Event,
    queries::{Query, ALL_QUERIES},
    NexmarkSource,
};
use env_logger::Env;
use indicatif::{ProgressBar, ProgressStyle};
use metrics::{Key, SharedString, Unit};
use metrics_util::{
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
    CompositeKey, MetricKind,
};
use num_format::{Locale, ToFormattedString};
use serde::Serialize;
use serde_with::{serde_as, DurationSecondsWithFrac};
use size_of::HumanBytes;
use std::{
    collections::HashMap,
    fs::OpenOptions,
    ops::Sub,
    path::Path,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

type MetricsSnapshot = HashMap<CompositeKey, (Option<Unit>, Option<SharedString>, DebugValue)>;

/// Currently just the elapsed time, but later add CPU and Mem.
#[serde_as]
#[derive(Default, Serialize)]
struct NexmarkResult {
    name: String,
    num_cores: usize,
    num_events: u64,
    #[serde_as(as = "DurationSecondsWithFrac<String>")]
    elapsed: Duration,
    before_stats: AllocStats,
    after_stats: AllocStats,
    #[serde(skip_serializing)]
    before_metrics: MetricsSnapshot,
    #[serde(skip_serializing)]
    after_metrics: MetricsSnapshot,
}

struct InputStats {
    num_events: u64,
}

enum StepCompleted {
    Dbsp,
    Source(usize),
}

fn spawn_dbsp_consumer(
    query: &str,
    profile_path: Option<&str>,
    mut dbsp: DBSPHandle,
    step_do_rx: mpsc::Receiver<()>,
    step_done_tx: mpsc::SyncSender<StepCompleted>,
) -> JoinHandle<()> {
    let query = query.to_string();
    let profile_path = profile_path.map(ToString::to_string);

    thread::Builder::new()
        .name("benchmark_consumer".into())
        .spawn(move || {
            if profile_path.is_some() {
                dbsp.enable_cpu_profiler().unwrap();
            }
            while let Ok(()) = step_do_rx.recv() {
                dbsp.step().unwrap();
                step_done_tx.send(StepCompleted::Dbsp).unwrap();
            }
            if let Some(profile_path) = profile_path {
                dbsp.dump_profile(<String as AsRef<Path>>::as_ref(&profile_path).join(query))
                    .unwrap();
            }
        })
        .unwrap()
}

fn spawn_source_producer(
    nexmark_config: NexmarkConfig,
    input_handle: ZSetHandle<Event>,
    step_do_rx: mpsc::Receiver<()>,
    step_done_tx: mpsc::SyncSender<StepCompleted>,
    source_exhausted_tx: mpsc::SyncSender<InputStats>,
) {
    thread::Builder::new()
        .name("benchmark producer".into())
        .spawn(move || {
            let batch_size = nexmark_config.input_batch_size;
            let mut source = NexmarkSource::new(nexmark_config.generator_options);
            let mut num_events: u64 = 0;

            // Start iterating by loading up the first batch of input ready for processing,
            // then waiting for further instructions.
            let last_batch_count = loop {
                let mut events: Vec<Tup2<Event, ZWeight>> = Vec::with_capacity(batch_size);
                let mut batch_count = 0;
                for event in &mut source {
                    events.push(Tup2(event, 1));
                    batch_count += 1;
                    if batch_count == batch_size {
                        break;
                    }
                }
                input_handle.append(&mut events);
                num_events += batch_count as u64;

                step_done_tx
                    .send(StepCompleted::Source(batch_count))
                    .unwrap();
                // If we're unable to fetch a full batch, then we're done.
                if batch_count < batch_size {
                    break batch_count;
                }
                step_do_rx.recv().unwrap();
            };

            source_exhausted_tx.send(InputStats { num_events }).unwrap();
            step_done_tx
                .send(StepCompleted::Source(last_batch_count))
                .unwrap();
        })
        .unwrap();
}

fn coordinate_input_and_steps(
    progress: bool,
    expected_num_events: u64,
    dbsp_step_tx: mpsc::SyncSender<()>,
    source_step_tx: mpsc::SyncSender<()>,
    step_done_rx: mpsc::Receiver<StepCompleted>,
    source_exhausted_rx: mpsc::Receiver<InputStats>,
    dbsp_join_handle: JoinHandle<()>,
) -> Result<InputStats> {
    // The producer should have already loaded up the first batch ready for
    // consumption before we start the loop.
    let progress_bar = if progress {
        ProgressBar::new(expected_num_events)
    } else {
        ProgressBar::hidden()
    };
    progress_bar.set_style(
        ProgressStyle::with_template(
            "{human_pos} / {human_len} [{wide_bar}] {percent:.2} % {per_sec:.2} {eta}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    if let Ok(StepCompleted::Dbsp) = step_done_rx.recv() {
        return Err(anyhow!("Expected initial source step, got DBSP step"));
    }

    // Continue until the source is exhausted.
    loop {
        if let Ok(input_stats) = source_exhausted_rx.try_recv() {
            // Wait for the processing to complete. We explicitly do one more step
            // to ensure the last input is processed, before dropping the dbsp_step_tx
            // half of the channel to ensure the dbsp thread terminates.
            dbsp_step_tx.send(())?;
            step_done_rx.recv()?;
            drop(dbsp_step_tx);
            dbsp_join_handle
                .join()
                .expect("DBSP consumer thread panicked");
            progress_bar.finish_with_message("Done");
            return Ok(input_stats);
        }

        // Trigger the step and the input of the next batch.
        dbsp_step_tx.send(())?;
        source_step_tx.send(())?;

        // Ensure both the dbsp and source finish before continuing.
        for _ in 0..2 {
            if let Ok(StepCompleted::Source(num_events)) = step_done_rx.recv() {
                progress_bar.inc(num_events as u64);
            }
        }
    }
}

fn create_ascii_table(config: &NexmarkConfig) -> AsciiTable {
    // Reported metrics (per query) for the benchmark.
    let mut result_columns = vec![
        "Query",
        "#Events",
        "Cores",
        "Elapsed",
        "Cores * Elapsed",
        "Throughput/Cores",
        "Total Usr CPU",
        "Total Sys CPU",
        "Current RSS",
        "Peak RSS",
        "Current Commit",
        "Peak Commit",
        "Page Faults",
    ];
    let mut max_width = 200;

    if config.min_storage_rows != usize::MAX {
        result_columns.extend_from_slice(&[
            "# Files",
            "# Writes",
            "# Reads",
            "Avg Write Size",
            "Avg Read Size",
            "Total Writes",
            "Total Reads",
            "Cache Hit",
            "Cache Miss",
        ]);
        max_width += 50;
    }

    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(max_width);

    for (idx, column_name) in result_columns.into_iter().enumerate() {
        ascii_table.column(idx).set_header(column_name);
    }

    ascii_table
}

fn run_query(config: &NexmarkConfig, snapshotter: &Snapshotter, query: Query) -> NexmarkResult {
    let name = format!("{query:?}");
    println!(
        "Starting {name} bench of {} events...",
        config.generator_options.max_events
    );

    let num_cores = config.cpu_cores;
    let expected_num_events = config.generator_options.max_events;
    let circuit_config = CircuitConfig {
        storage: Some(StorageConfig {
            path: tempdir_for_thread().to_string_lossy().to_string(),
            cache: if config.feldera_cache {
                StorageCacheConfig::FelderaCache
            } else {
                StorageCacheConfig::PageCache
            },
        }),
        min_storage_rows: config.min_storage_rows,
        ..CircuitConfig::with_workers(num_cores)
    };
    let (dbsp, input_handle) =
        Runtime::init_circuit(circuit_config, move |circuit: &mut RootCircuit| {
            let (stream, input_handle) = circuit.add_input_zset::<Event>();

            query.query(circuit, stream);
            Ok(input_handle)
        })
        .unwrap();

    // Create a channel for the coordinating thread to determine whether the
    // producer or consumer step is completed first.
    let (step_done_tx, step_done_rx) = mpsc::sync_channel(2);

    // Start the DBSP runtime processing steps only when it receives a message to do
    // so. The DBSP processing happens in its own thread where the resource usage
    // calculation can also happen.
    let (dbsp_step_tx, dbsp_step_rx) = mpsc::sync_channel(1);
    let dbsp_join_handle = spawn_dbsp_consumer(
        &name,
        config.profile_path.as_deref(),
        dbsp,
        dbsp_step_rx,
        step_done_tx.clone(),
    );

    // Start the generator inputting the specified number of batches to the circuit
    // whenever it receives a message.
    let (source_step_tx, source_step_rx): (mpsc::SyncSender<()>, mpsc::Receiver<()>) =
        mpsc::sync_channel(1);
    let (source_exhausted_tx, source_exhausted_rx) = mpsc::sync_channel(1);
    spawn_source_producer(
        config.clone(),
        input_handle,
        source_step_rx,
        step_done_tx,
        source_exhausted_tx,
    );

    ALLOC.reset_stats();
    let before_stats = ALLOC.stats();
    let before_metrics = snapshotter.snapshot();
    let start = Instant::now();

    let input_stats = coordinate_input_and_steps(
        config.progress,
        expected_num_events,
        dbsp_step_tx,
        source_step_tx,
        step_done_rx,
        source_exhausted_rx,
        dbsp_join_handle,
    )
    .unwrap();

    let elapsed = start.elapsed();
    let after_stats = ALLOC.stats();
    let after_metrics = snapshotter.snapshot();

    // Return the user/system CPU overhead from the generator/input thread.
    NexmarkResult {
        name,
        num_cores,
        before_stats,
        after_stats,
        before_metrics: before_metrics.into_hashmap(),
        after_metrics: after_metrics.into_hashmap(),
        elapsed,
        num_events: input_stats.num_events,
    }
}

fn run_queries(nexmark_config: &NexmarkConfig, snapshotter: &Snapshotter) -> Vec<NexmarkResult> {
    let queries_to_run = if nexmark_config.query.is_empty() {
        ALL_QUERIES.as_slice()
    } else {
        &nexmark_config.query
    };
    let mut results = Vec::new();
    for &query in queries_to_run {
        let result = run_query(nexmark_config, &snapshotter, query);
        results.push(result);
    }
    results
}

// TODO(absoludity): Some tools mentioned at
// https://nnethercote.github.io/perf-book/benchmarking.html but as had been
// said earlier, most are more suited to micro-benchmarking.  I assume that our
// best option for comparable benchmarks will be to try to do exactly what the
// Java implementation does: core(s) * time [see Run
// Nexmark](https://github.com/nexmark/nexmark#run-nexmark).  Right now, just
// grab elapsed time for each query run.  See
// https://github.com/matklad/t-cmd/blob/master/src/main.rs Also CpuMonitor.java
// in nexmark (binary that uses procfs to get cpu usage ever 100ms?)

fn parse_counter(metrics: &MetricsSnapshot, name: &'static str) -> u64 {
    if let Some((_, _, DebugValue::Counter(value))) = metrics.get(&CompositeKey::new(
        MetricKind::Counter,
        Key::from_static_name(name),
    )) {
        *value
    } else {
        0
    }
}

struct Metrics {
    files_created: u64,
    writes_success: u64,
    reads_success: u64,
    total_bytes_written: u64,
    total_bytes_read: u64,
    buffer_cache_hit: u64,
    buffer_cache_miss: u64,
}

impl From<&MetricsSnapshot> for Metrics {
    fn from(source: &MetricsSnapshot) -> Self {
        Self {
            files_created: parse_counter(source, FILES_CREATED),
            writes_success: parse_counter(source, WRITES_SUCCESS),
            reads_success: parse_counter(source, READS_SUCCESS),
            total_bytes_written: parse_counter(source, TOTAL_BYTES_WRITTEN),
            total_bytes_read: parse_counter(source, TOTAL_BYTES_READ),
            buffer_cache_hit: parse_counter(source, BUFFER_CACHE_HIT),
            buffer_cache_miss: parse_counter(source, BUFFER_CACHE_MISS),
        }
    }
}

fn div(num: u64, denom: u64) -> u64 {
    num.checked_div(denom).unwrap_or_default()
}

struct MetricsDiff {
    n_created: u64,
    n_writes: u64,
    n_reads: u64,
    avg_wblock: u64,
    avg_rblock: u64,
    total_bytes_written: u64,
    total_bytes_read: u64,
    cache_miss: u64,
    cache_hit: u64,
}

impl Sub<&Metrics> for &Metrics {
    type Output = MetricsDiff;

    fn sub(self, rhs: &Metrics) -> Self::Output {
        let lhs = self;

        let n_writes = lhs.writes_success - rhs.writes_success;
        let n_reads = lhs.reads_success - rhs.reads_success;
        let wbytes_diff = lhs.total_bytes_written - rhs.total_bytes_written;
        let rbytes_diff = lhs.total_bytes_read - rhs.total_bytes_read;

        MetricsDiff {
            n_created: lhs.files_created - rhs.files_created,
            n_writes,
            n_reads,
            avg_wblock: div(wbytes_diff, n_writes),
            avg_rblock: div(rbytes_diff, n_reads),
            total_bytes_written: lhs.total_bytes_written - rhs.total_bytes_written,
            total_bytes_read: lhs.total_bytes_read - rhs.total_bytes_read,
            cache_miss: lhs.buffer_cache_miss - rhs.buffer_cache_miss,
            cache_hit: lhs.buffer_cache_hit - rhs.buffer_cache_hit,
        }
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let nexmark_config = NexmarkConfig::parse();
    let cpu_cores = nexmark_config.cpu_cores;

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().unwrap();
    let results = run_queries(&nexmark_config, &snapshotter);

    let ascii_table = create_ascii_table(&nexmark_config);
    ascii_table.print(results.iter().map(|result| {
        let (before, after) = (result.before_stats, result.after_stats);

        let before_metrics: Metrics = (&result.before_metrics).into();
        let after_metrics: Metrics = (&result.after_metrics).into();
        let diff = &after_metrics - &before_metrics;

        let mut row = vec![
            result.name.clone(),
            format!("{}", result.num_events.to_formatted_string(&Locale::en)),
            format!("{cpu_cores}"),
            format!("{:#.3?}", result.elapsed),
            format!("{:#.3?}", result.elapsed * cpu_cores as u32),
            format!(
                "{0:.3} K/s",
                result.num_events as f32 / result.elapsed.as_secs_f32() / cpu_cores as f32 / 1000.0,
            ),
            format!(
                "{:#.3?}",
                Duration::from_millis((after.user_ms - before.user_ms) as u64),
            ),
            format!(
                "{:#.3?}",
                Duration::from_millis((after.system_ms - before.system_ms) as u64),
            ),
            format!("{}", HumanBytes::from(after.current_rss)),
            format!("{}", HumanBytes::from(after.peak_rss)),
            format!("{}", HumanBytes::from(after.current_commit)),
            format!("{}", HumanBytes::from(after.peak_commit)),
            format!("{}", after.page_faults - before.page_faults),
        ];
        if nexmark_config.min_storage_rows != usize::MAX {
            row.extend_from_slice(&[
                format!("{}", diff.n_created),
                format!("{}", diff.n_writes),
                format!("{}", diff.n_reads),
                format!("{}", HumanBytes::from(diff.avg_wblock)),
                format!("{}", HumanBytes::from(diff.avg_rblock)),
                format!("{}", HumanBytes::from(diff.total_bytes_written)),
                format!("{}", HumanBytes::from(diff.total_bytes_read)),
                format!("{}", diff.cache_hit),
                format!("{}", diff.cache_miss),
            ])
        }
        row
    }));

    if let Some(csv_file) = nexmark_config.output_csv {
        let results_file_already_exists = Path::new(&csv_file).is_file();
        let file = OpenOptions::new()
            .write(true)
            .append(results_file_already_exists)
            .create(!results_file_already_exists)
            .open(&csv_file)
            .expect("failed to open results csv file for writing");
        let mut csv_writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(file);
        if !results_file_already_exists {
            csv_writer.write_record([
                "name",
                "num_cores",
                "num_events",
                "elapsed",
                "allocstats_before_elapsed_ms",
                "allocstats_before_user_ms",
                "allocstats_before_system_ms",
                "allocstats_before_current_rss",
                "allocstats_before_peak_rss",
                "allocstats_before_current_commit",
                "allocstats_before_peak_commit",
                "allocstats_before_page_faults",
                "allocstats_after_elapsed_ms",
                "allocstats_after_user_ms",
                "allocstats_after_system_ms",
                "allocstats_after_current_rss",
                "allocstats_after_peak_rss",
                "allocstats_after_current_commit",
                "allocstats_after_peak_commit",
                "allocstats_after_page_faults",
            ])?;
        }

        for result in results.into_iter() {
            csv_writer.serialize(result)?;
        }
    }

    Ok(())
}
