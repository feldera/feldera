//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]

#[macro_use]
mod run_queries;
#[path = "../mimalloc.rs"]
mod mimalloc;

use anyhow::{anyhow, Result};
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::{
    nexmark::{
        config::{Config as NexmarkConfig, Query as NexmarkQuery},
        model::Event,
        queries::{
            q0, q1, q12, q13, q13_side_input, q14, q15, q16, q17, q18, q19, q2, q20, q21, q22, q3,
            q4, q5, q6, q7, q8, q9,
        },
        NexmarkSource,
    },
    trace::ord::OrdZSet,
    Circuit, CollectionHandle, DBSPHandle, Runtime,
};
use indicatif::{ProgressBar, ProgressStyle};
use mimalloc::{AllocStats, MiMalloc};
use num_format::{Locale, ToFormattedString};
use size_of::HumanBytes;
use std::{
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

// TODO: Ideally these macros would be in a separate `lib.rs` in this benchmark
// crate, but benchmark binaries don't appear to work like that (in that, I
// haven't yet found a way to import from a `lib.rs` in the same directory as
// the benchmark's `main.rs`)

/// Currently just the elapsed time, but later add CPU and Mem.
#[derive(Default)]
struct NexmarkResult {
    name: String,
    num_events: u64,
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
    mut dbsp: DBSPHandle,
    step_do_rx: mpsc::Receiver<()>,
    step_done_tx: mpsc::SyncSender<StepCompleted>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("benchmark_consumer".into())
        .spawn(move || {
            while let Ok(()) = step_do_rx.recv() {
                dbsp.step().unwrap();
                step_done_tx.send(StepCompleted::Dbsp).unwrap();
            }
        })
        .unwrap()
}

fn spawn_source_producer(
    nexmark_config: NexmarkConfig,
    mut input_handle: CollectionHandle<Event, isize>,
    step_do_rx: mpsc::Receiver<()>,
    step_done_tx: mpsc::SyncSender<StepCompleted>,
    source_exhausted_tx: mpsc::SyncSender<InputStats>,
) {
    thread::Builder::new()
        .name("benchmark producer".into())
        .spawn(move || {
            let batch_size = nexmark_config.input_batch_size;
            let mut source = NexmarkSource::<isize, OrdZSet<Event, isize>>::new(nexmark_config);
            let mut num_events: u64 = 0;

            // Start iterating by loading up the first batch of input ready for processing,
            // then waiting for further instructions.
            let last_batch_count = loop {
                let mut events: Vec<(Event, isize)> = Vec::with_capacity(batch_size);
                let mut batch_count = 0;
                for event in &mut source {
                    events.push((event, 1));
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
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(200);

    let columns = [
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
    for (idx, column_name) in columns.into_iter().enumerate() {
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
    ascii_table.print(results.into_iter().map(|result| {
        let (before, after) = (result.before_stats, result.after_stats);

        vec![
            result.name,
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

    Ok(())
}
