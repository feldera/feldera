//! Nexmark benchmarks for DBSP
//!
//! CLI for running Nexmark benchmarks with DBSP.
#![feature(is_some_with)]
use anyhow::Result;
use clap::Parser;
use dbsp::{
    circuit::Root,
    nexmark::{
        config::Config as NexmarkConfig, generator::config::Config as GeneratorConfig,
        model::Event, queries::q1, NexmarkSource,
    },
    profile::CPUProfiler,
    trace::{ord::OrdZSet, BatchReader},
};
use rand::prelude::ThreadRng;

// TODO: Enable running specific tests
fn main() -> Result<()> {
    let nexmark_config = NexmarkConfig::parse();
    let generator_config = GeneratorConfig::new(nexmark_config, 0, 0, 0, 0);

    let root = Root::build(|circuit| {
        // TODO(absoludity): CPUProfiler is not currently used in any of the
        // other benchmarks, only commented out. Not yet sure whether it will be
        // helpful either. Some tools mentioned at
        // https://nnethercote.github.io/perf-book/benchmarking.html but as had
        // been said earlier, most are more suited to micro-benchmarking.  I
        // assume that our best option for comparable benchmarks will be to try
        // to do exactly what the Java implementation does: core(s) * time [see
        // Run Nexmark](https://github.com/nexmark/nexmark#run-nexmark).  Right
        // now, just grab simple timestamp to do a duration for 100_000_000 events with
        // a source generating 1M events/sec as per the Nexmark results.
        let start_time = std::time::SystemTime::now();

        let source =
            NexmarkSource::<ThreadRng, isize, OrdZSet<Event, isize>>::new(generator_config);
        let input = circuit.add_source(source);

        let output = q1(input);
        let mut events_processed: u64 = 0;

        output.inspect(move |zs: &OrdZSet<_, _>| {
            events_processed += zs.len() as u64;
            if events_processed >= 100_000_000 {
                println!(
                    "{} events processed in {}ms",
                    events_processed,
                    std::time::SystemTime::now()
                        .duration_since(start_time)
                        .unwrap()
                        .as_millis()
                );
                // TODO: send signal through channel to stop iterating once
                // sufficient data for test is processed?
            }
        });
    })
    .unwrap();

    for _ in 0..100_000 {
        root.step().unwrap();
    }
    Ok(())
}
