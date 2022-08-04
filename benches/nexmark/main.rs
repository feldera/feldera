//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]

#[cfg(unix)]
use libc::{getrusage, rusage, timeval, RUSAGE_THREAD};
use std::{
    io::Error,
    mem::MaybeUninit,
    sync::{mpsc, mpsc::TryRecvError},
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::{
    nexmark::{
        config::Config as NexmarkConfig,
        generator::config::Config as GeneratorConfig,
        model::Event,
        queries::{q0, q1, q2, q3, q4},
        NexmarkSource,
    },
    trace::{ord::OrdZSet, BatchReader},
    Circuit,
};
use num_format::{Locale, ToFormattedString};
use rand::prelude::ThreadRng;

// TODO: Ideally these macros would be in a separate `lib.rs` in this benchmark
// crate, but benchmark binaries don't appear to work like that (in that, I
// haven't yet found a way to import from a `lib.rs` in the same directory as
// the benchmark's `main.rs`)

/// Returns a closure for a circuit with the nexmark source that sets
/// `max_events_reached` once no more data is available.
macro_rules! nexmark_circuit {
    ( $q:expr, $generator_config:expr, $output_fixedpoint_tx:expr ) => {
        |circuit: &mut Circuit<()>| {
            // We need the source to signal when it has reached its fixedpoint.
            let (fixedpoint_tx, fixedpoint_rx) = mpsc::channel();
            let mut num_events_generated = 0;

            let source = NexmarkSource::<ThreadRng, isize, OrdZSet<Event, isize>>::new(
                $generator_config,
                fixedpoint_tx,
            );
            let input = circuit.add_source(source);

            let output = $q(input);

            output.inspect(move |zs: &OrdZSet<_, _>| {
                // Turns out we can't get an exact count of events by accumulating the lengths
                // of the `OrdZSet` since duplicate bids are not that difficult to
                // produce in the generator (0-3 per 1000), which get merged to a
                // single Item in the `OrdZSet` with an adjusted weight. Nor does
                // the number of output events even correspond with the input events
                // necessarily. Nor can we rely on empty sets as an indicator that
                // it's finished (since they'll be returned in queries that don't
                // emit data often). So, for simplicity, the source sends a message
                // on it's fixedpoint_sync channel when it reaches its fixed point, which we can
                // read here and forward to the test harness.
                match fixedpoint_rx.try_recv() {
                    Ok(num_events) => {
                        num_events_generated = num_events;
                    }
                    Err(TryRecvError::Empty) => (),
                    _ => panic!("unexpected result from fixedpoint_sync channel"),
                };
                // The source may reach its fixed point for inputs before they have been
                // processed.
                if num_events_generated > 0 && zs.len() == 0 {
                    $output_fixedpoint_tx.send(num_events_generated).unwrap();
                }
            });
        }
    };
}

/// Currently just the elapsed time, but later add CPU and Mem.
struct NexmarkResult {
    name: String,
    num_events: u64,
    elapsed: Duration,
    usr_cpu: Duration,
    sys_cpu: Duration,
    max_rss: u64,
}

macro_rules! run_query {
    ( $q_name:expr, $q:expr, $generator_config:expr, $max_events:expr, $result_tx:expr ) => {{
        // Until we have the I/O API to control the running of circuits,
        // use a channel to signal when the test is finished (all events emitted
        // by the generator), sending back the number of events processed.
        let (fixedpoint_tx, fixedpoint_rx): (mpsc::Sender<u64>, mpsc::Receiver<u64>) =
            mpsc::channel();

        let circuit = nexmark_circuit!($q, $generator_config, fixedpoint_tx);

        let root = Circuit::build(circuit).unwrap().0;

        let num_events_generated;
        let start = Instant::now();
        loop {
            match fixedpoint_rx.try_recv() {
                Ok(num_events) => {
                    num_events_generated = num_events;
                    break;
                }
                Err(TryRecvError::Empty) => root.step().unwrap(),
                _ => panic!("unexpected result from fixedpoint sync channel"),
            }
        }

        let (usr_cpu, sys_cpu, max_rss) = unsafe { rusage_thread() };

        $result_tx
            .send(NexmarkResult {
                name: $q_name.to_string(),
                num_events: num_events_generated,
                elapsed: start.elapsed(),
                sys_cpu,
                usr_cpu,
                max_rss,
            })
            .unwrap();
    }};
}

macro_rules! run_queries {
    ( $generator_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:expr, $q:expr) ),+ ) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();

        // Run each query in a separate thread so we can measure the resource
        // usage of the thread in isolation. We'll communicate the resource usage
        // for collection via a channel to accumulate here.
        let (result_tx, result_rx): (mpsc::Sender<NexmarkResult>, mpsc::Receiver<NexmarkResult>) =
            mpsc::channel();

        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name.to_string()) {
            println!("Starting {} bench of {} events...", $q_name, $max_events);
            let thread_result_tx = result_tx.clone();
            let thread_generator_config = $generator_config.clone();
            thread::spawn(move || {
                run_query!($q_name, $q, thread_generator_config, $max_events, thread_result_tx);
            });
            // Wait for the thread to finish then collect the result.
            results.push(result_rx.recv().unwrap());
        }
        )+
        results
    }};
}

fn create_ascii_table() -> AsciiTable {
    let mut ascii_table = AsciiTable::default();
    ascii_table.column(0).set_header("Query");
    ascii_table.column(1).set_header("#Events");
    ascii_table.column(2).set_header("Cores");
    ascii_table.column(3).set_header("Elapsed(s)");
    // Redundant until we use more than one core.
    // ascii_table.column(4).set_header("Cores * Time(s)");
    ascii_table.column(4).set_header("Throughput/Cores");
    ascii_table.column(5).set_header("User CPU(s)");
    ascii_table.column(6).set_header("System CPU(s)");
    ascii_table.column(7).set_header("Max RSS(Kb)");
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
    let generator_config = GeneratorConfig::new(nexmark_config, 0, 0, 0);

    let results = run_queries!(
        generator_config,
        max_events,
        queries_to_run,
        ("q0", q0),
        ("q1", q1),
        ("q2", q2),
        ("q3", q3),
        ("q4", q4)
    );

    let ascii_table = create_ascii_table();
    ascii_table.print(results.into_iter().map(|r| {
        vec![
            r.name,
            format!("{}", r.num_events.to_formatted_string(&Locale::en)),
            String::from("1"),
            format!("{0:.3}", r.elapsed.as_secs_f32()),
            format!(
                "{0:.3} K/s",
                r.num_events as f32 / r.elapsed.as_secs_f32() / 1000.0
            ),
            format!("{0:.3}", r.usr_cpu.as_secs_f32()),
            format!("{0:.3}", r.sys_cpu.as_secs_f32()),
            format!("{}", r.max_rss.to_formatted_string(&Locale::en)),
        ]
    }));

    Ok(())
}

#[cfg(unix)]
fn duration_for_timeval(tv: timeval) -> Duration {
    Duration::new(tv.tv_sec as u64, tv.tv_usec as u32 * 1_000)
}

/// Returns the user CPU, system CPU and maxrss (in Kb) for the current thread.
#[cfg(unix)]
pub unsafe fn rusage_thread() -> (Duration, Duration, u64) {
    let mut ru: MaybeUninit<rusage> = MaybeUninit::uninit();
    let err_code = getrusage(RUSAGE_THREAD, ru.as_mut_ptr());
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
