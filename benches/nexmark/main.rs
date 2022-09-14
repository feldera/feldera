//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]

#[cfg(unix)]
use libc::{getrusage, rusage, timeval};
#[cfg(unix)]
use std::{io::Error, mem::MaybeUninit};
use std::{
    sync::mpsc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::{
    nexmark::{
        config::Config as NexmarkConfig,
        model::Event,
        queries::{q0, q1, q13, q13_side_input, q14, q15, q2, q3, q4, q5, q6, q7, q8, q9},
        NexmarkSource,
    },
    trace::ord::OrdZSet,
    Circuit, CollectionHandle, DBSPHandle, Runtime,
};
use indicatif::{ProgressBar, ProgressStyle};
use num_format::{Locale, ToFormattedString};

// TODO: Ideally these macros would be in a separate `lib.rs` in this benchmark
// crate, but benchmark binaries don't appear to work like that (in that, I
// haven't yet found a way to import from a `lib.rs` in the same directory as
// the benchmark's `main.rs`)

/// Returns a closure for a circuit with the nexmark source that returns
/// the input handle.
macro_rules! nexmark_circuit {
    ( "q13", $q:expr ) => {
        |circuit: &mut Circuit<()>| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();
            let (side_stream, mut side_input_handle) =
                circuit.add_input_zset::<(usize, String, u64), isize>();

            let output = $q(stream, side_stream);

            output.inspect(move |_zs| ());

            // Ensure the side-input is loaded here so we can return a single input
            // handle like the other queries.
            side_input_handle.append(&mut q13_side_input());

            input_handle
        }
    };
    ( $q_name:expr, $q:expr ) => {
        |circuit: &mut Circuit<()>| {
            let (stream, input_handle) = circuit.add_input_zset::<Event, isize>();

            let output = $q(stream);

            output.inspect(move |_zs| ());

            input_handle
        }
    };
}

/// Currently just the elapsed time, but later add CPU and Mem.
#[derive(Default)]
struct NexmarkResult {
    name: String,
    num_events: u64,
    elapsed: Duration,
    total_usr_cpu: Duration,
    total_sys_cpu: Duration,
    max_rss: Option<u64>,
}

struct InputStats {
    num_events: u64,
}

enum StepCompleted {
    DBSP,
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
                step_done_tx.send(StepCompleted::DBSP).unwrap();
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
                while let Some(event) = source.next() {
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

    if let Ok(StepCompleted::DBSP) = step_done_rx.recv() {
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

macro_rules! run_query {
    ( $q_name:tt, $q:expr, $nexmark_config:expr) => {{
        // let circuit_closure = nexmark_circuit!($q_name, $q);
        let circuit_closure = nexmark_circuit!($q_name, $q);

        let num_cores = $nexmark_config.cpu_cores;
        let expected_num_events = $nexmark_config.max_events;
        let (dbsp, input_handle) = Runtime::init_circuit(num_cores, circuit_closure).unwrap();

        // Create a channel for the coordinating thread to determine whether the
        // producer or consumer step is completed first.
        let (step_done_tx, step_done_rx) = mpsc::sync_channel(2);

        // Start the DBSP runtime processing steps only when it receives a message to do
        // so. The DBSP processing happens in its own thread where the resource usage
        // calculation can also happen.
        let (dbsp_step_tx, dbsp_step_rx) = mpsc::sync_channel(1);
        let dbsp_join_handle = spawn_dbsp_consumer(dbsp, dbsp_step_rx, step_done_tx.clone());

        // Start the generator inputting the specified number of batches to the circuit
        // whenever it receives a message.
        let (source_step_tx, source_step_rx): (mpsc::SyncSender<()>, mpsc::Receiver<()>) =
            mpsc::sync_channel(1);
        let (source_exhausted_tx, source_exhausted_rx) = mpsc::sync_channel(1);
        spawn_source_producer(
            $nexmark_config,
            input_handle,
            source_step_rx,
            step_done_tx,
            source_exhausted_tx,
        );

        let input_stats = coordinate_input_and_steps(
            expected_num_events,
            dbsp_step_tx,
            source_step_tx,
            step_done_rx,
            source_exhausted_rx,
            dbsp_join_handle,
        )
        .unwrap();

        // Return the user/system CPU overhead from the generator/input thread.
        NexmarkResult {
            num_events: input_stats.num_events,
            ..NexmarkResult::default()
        }
    }};
}

macro_rules! run_queries {
    ( $nexmark_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:tt, $q:expr) ),+ ) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();
        // We have no way (currently) of finding the max memory usage for each
        // subsequent query as the value is for the process. So only the first
        // query will have a value.
        let mut query_count = 0;
        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name.to_string()) {
            query_count += 1;
            println!("Starting {} bench of {} events...", $q_name, $max_events);

            let start = Instant::now();
            let (before_usr_cpu, before_sys_cpu, before_max_rss) = unsafe { rusage(libc::RUSAGE_SELF) };

            let thread_nexmark_config = $nexmark_config.clone();
            let result = run_query!($q_name, $q, thread_nexmark_config);
            let (after_usr_cpu, after_sys_cpu, after_max_rss) = unsafe { rusage(libc::RUSAGE_SELF) };
            results.push(NexmarkResult {
                name: $q_name.to_string(),
                total_usr_cpu: after_usr_cpu - before_usr_cpu,
                total_sys_cpu: after_sys_cpu - before_sys_cpu,
                max_rss: match query_count { 1 => Some(after_max_rss - before_max_rss), _ => None},
                elapsed: start.elapsed(),
                ..result
            });
        }
        )+
        results
    }};
}

fn create_ascii_table() -> AsciiTable {
    let mut ascii_table = AsciiTable::default();
    ascii_table.set_max_width(200);
    ascii_table.column(0).set_header("Query");
    ascii_table.column(1).set_header("#Events");
    ascii_table.column(2).set_header("Cores");
    ascii_table.column(3).set_header("Elapsed");
    ascii_table.column(4).set_header("Cores * Elapsed");
    ascii_table.column(5).set_header("Throughput/Cores");
    ascii_table.column(6).set_header("Total Usr CPU");
    ascii_table.column(7).set_header("Total Sys CPU");
    ascii_table.column(8).set_header("Max RSS(Kb)");
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

// TODO: Implement for non-unix platforms (mainly removing libc perf stuff)
#[cfg(not(unix))]
fn main() -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn main() -> Result<()> {
    let nexmark_config = NexmarkConfig::parse();
    let max_events = nexmark_config.max_events;
    let queries_to_run = nexmark_config.query.clone();
    let cpu_cores = nexmark_config.cpu_cores;

    let results = run_queries!(
        nexmark_config,
        max_events,
        queries_to_run,
        ("q0", q0),
        ("q1", q1),
        ("q2", q2),
        ("q3", q3),
        ("q4", q4),
        ("q5", q5),
        ("q6", q6),
        ("q7", q7),
        ("q8", q8),
        ("q9", q9),
        ("q13", q13),
        ("q14", q14),
        ("q15", q15)
    );

    let ascii_table = create_ascii_table();
    ascii_table.print(results.into_iter().map(|r| {
        vec![
            r.name,
            format!("{}", r.num_events.to_formatted_string(&Locale::en)),
            format!("{cpu_cores}"),
            format!("{0:.3}s", r.elapsed.as_secs_f32()),
            format!("{0:.3}s", cpu_cores as f32 * r.elapsed.as_secs_f32()),
            format!(
                "{0:.3} K/s",
                r.num_events as f32 / r.elapsed.as_secs_f32() / cpu_cores as f32 / 1000.0
            ),
            format!("{0:.3}s", (r.total_usr_cpu).as_secs_f32()),
            format!("{0:.3}s", (r.total_sys_cpu).as_secs_f32()),
            format!(
                "{}",
                if let Some(max_rss) = r.max_rss {
                    max_rss.to_formatted_string(&Locale::en)
                } else {
                    "N/A".to_string()
                }
            ),
        ]
    }));

    Ok(())
}

#[cfg(unix)]
fn duration_for_timeval(tv: timeval) -> Duration {
    Duration::new(tv.tv_sec as u64, tv.tv_usec as u32 * 1_000)
}

/// Returns the user CPU, system CPU and maxrss (in Kb) for the current process.
#[cfg(unix)]
pub unsafe fn rusage(target: i32) -> (Duration, Duration, u64) {
    let mut ru: MaybeUninit<rusage> = MaybeUninit::uninit();
    let err_code = getrusage(target, ru.as_mut_ptr());
    if err_code != 0 {
        panic!("getrusage returned {}", Error::last_os_error());
    }
    let ru = ru.assume_init();
    (
        duration_for_timeval(ru.ru_utime),
        duration_for_timeval(ru.ru_stime),
        ru.ru_maxrss as u64,
    )
}

/// Define for non-unix so that bench can still run reporting on time etc.
#[cfg(not(unix))]
pub unsafe fn rusage(_target: i32) -> (Duration, Duration, u64) {
    (Duration::from_millis(0), Duration::from_millis(0), 0)
}
