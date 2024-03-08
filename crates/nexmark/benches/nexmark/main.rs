//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.

#[macro_use]
mod run_queries;

use anyhow::{anyhow, Result};
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::utils::Tup2;
use dbsp::{
    mimalloc::{AllocStats, MiMalloc},
    utils::Tup3,
    DBSPHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
};
use dbsp_nexmark::{
    config::{Config as NexmarkConfig, Query as NexmarkQuery},
    model::Event,
    queries::{
        q0, q1, q12, q13, q13_side_input, q14, q15, q16, q17, q18, q19, q2, q20, q21, q22, q3, q4,
        q5, q6, q7, q8, q9,
    },
    NexmarkSource,
};
use indicatif::{ProgressBar, ProgressStyle};
use num_format::{Locale, ToFormattedString};
use serde::Serialize;
use serde_with::{serde_as, DurationSecondsWithFrac};
use size_of::HumanBytes;
use std::{
    fs::OpenOptions,
    path::Path,
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

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
            let mut source = NexmarkSource::new(nexmark_config);
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
    expected_num_events: u64,
    dbsp_step_tx: mpsc::SyncSender<()>,
    source_step_tx: mpsc::SyncSender<()>,
    step_done_rx: mpsc::Receiver<StepCompleted>,
    source_exhausted_rx: mpsc::Receiver<InputStats>,
    dbsp_join_handle: JoinHandle<()>,
) -> Result<InputStats> {
    // The producer should have already loaded up the first batch ready for
    // consumption before we start the loop.
    // let progress_bar = ProgressBar::new(expected_num_events);
    let progress_bar = ProgressBar::new(expected_num_events);
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

fn create_ascii_table() -> AsciiTable {
    /// Reported metrics (per query) for the benchmark.
    const RESULT_COLUMNS: [&str; 13] = [
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

    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(200);

    for (idx, column_name) in RESULT_COLUMNS.into_iter().enumerate() {
        ascii_table.column(idx).set_header(column_name);
    }

    ascii_table
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

fn main() -> Result<()> {
    let nexmark_config = NexmarkConfig::parse();
    let max_events = nexmark_config.max_events;
    let queries_to_run = nexmark_config.query.clone();
    let cpu_cores = nexmark_config.cpu_cores;

    let results = run_queries!(
        nexmark_config,
        max_events,
        queries_to_run,
        queries => {
            q0,
            q1,
            q2,
            q3,
            q4,
            q5,
            q6,
            q7,
            q8,
            q9,
            q12,
            q13,
            q14,
            q15,
            q16,
            q17,
            q18,
            q19,
            q20,
            q21,
            q22,
        }
    );

    let ascii_table = create_ascii_table();
    ascii_table.print(results.iter().map(|result| {
        let (before, after) = (result.before_stats, result.after_stats);

        vec![
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
        ]
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
