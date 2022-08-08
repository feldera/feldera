//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]
use std::{
    sync::{mpsc, mpsc::TryRecvError},
    time::{Duration, Instant},
};

use anyhow::Result;
use ascii_table::AsciiTable;
use clap::Parser;
use dbsp::{
    circuit::{Circuit, Root},
    nexmark::{
        config::Config as NexmarkConfig,
        generator::config::Config as GeneratorConfig,
        model::Event,
        queries::{q0, q1, q2, q3, q4},
        NexmarkSource,
    },
    trace::{ord::OrdZSet, BatchReader},
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
#[derive(Debug)]
struct NexmarkResult {
    name: String,
    num_events: u64,
    elapsed: Duration,
}

macro_rules! run_query {
    ( $q_name:expr, $q:expr, $generator_config:expr, $max_events:expr ) => {{
        // Until we have the I/O API to control the running of circuits,
        // use an empty channel to signal when the test is finished (all events emitted
        // by the generator).
        let (fixedpoint_tx, fixedpoint_rx): (mpsc::Sender<u64>, mpsc::Receiver<u64>) =
            mpsc::channel();

        let circuit = nexmark_circuit!($q, $generator_config, fixedpoint_tx);

        let root = Root::build(circuit).unwrap();

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
        NexmarkResult {
            name: $q_name.to_string(),
            num_events: num_events_generated,
            elapsed: start.elapsed(),
        }
    }};
}

macro_rules! run_queries {
    ( $generator_config:expr, $max_events:expr, $queries_to_run:expr, $( ($q_name:expr, $q:expr) ),+ ) => {{
        let mut results: Vec<NexmarkResult> = Vec::new();
        $(
        if $queries_to_run.len() == 0 || $queries_to_run.contains(&$q_name.to_string()) {
            println!("Starting {} bench of {} events...", $q_name, $max_events);

            results.push(run_query!($q_name, $q, $generator_config.clone(), $max_events));
        }
        )+
        results
    }};
}

fn create_ascii_table() -> AsciiTable {
    let mut ascii_table = AsciiTable::default();
    ascii_table.column(0).set_header("Nexmark Query");
    ascii_table.column(1).set_header("Events Num");
    ascii_table.column(2).set_header("Cores");
    ascii_table.column(3).set_header("Time(s)");
    ascii_table.column(4).set_header("Cores * Time(s)");
    ascii_table.column(5).set_header("Throughput/Cores");
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
            format!("{0:.3}", r.elapsed.as_secs_f32()),
            format!(
                "{0:.3} K/s",
                r.num_events as f32 / r.elapsed.as_secs_f32() / 1000.0
            ),
        ]
    }));

    Ok(())
}
