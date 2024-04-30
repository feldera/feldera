use anyhow::Result;

use rand::rngs::{mock::StepRng, SmallRng};

use dbsp::mimalloc::MiMalloc;
use dbsp_nexmark::{
    config::GeneratorOptions,
    generator::{config::Config, NexmarkGenerator},
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

use rand::SeedableRng;

macro_rules! with_rng {
    ($num_event_generators:expr, $rng_name:expr, $rng:expr, $reps:expr, $show_intermediate:expr) => {{
        let count = 1_000_000;
        let reps = $reps;
        let num_event_generators = $num_event_generators;
        let rng_name = $rng_name;
        let rng = $rng;
        let config = Config {
            options: GeneratorOptions {
                num_event_generators,
                ..GeneratorOptions::default()
            },
            ..Config::default()
        };
        let mut generator = NexmarkGenerator::new(config, rng, 0);

        println!("rng = {rng_name}");

        let mut hist = hdrhist::HDRHist::new();

        if $show_intermediate {
            println!("throughput (hz), every {count} samples:");
        }
        for _ in 0..reps {
            let start = std::time::Instant::now();
            let mut prev_e = std::time::Instant::now();
            for _ in 0..count {
                generator.next_event().unwrap();
                let end_e = std::time::Instant::now();
                hist.add_value((end_e - prev_e).as_nanos() as u64);
                prev_e = end_e;
            }
            let end = std::time::Instant::now();
            if $show_intermediate {
                print!("{:.2} ", count as f64 / (end - start).as_secs_f64());
            }
        }
        if $show_intermediate {
            println!();
        }

        let total_count = count * reps;
        println!("ccdf of latencies (ns) for {total_count} samples:");
        println!("{}", hist.summary_string());
        println!();
    }};
}

macro_rules! just_rng {
    ($rng_name:expr, $rng:expr) => {{
        use rand::Rng;

        let count = 10_000_000;
        let rng_name = $rng_name;
        let mut rng = $rng;

        println!("(just the rng) rng = {rng_name}");

        println!("throughput (hz) every {count} samples:");
        for _ in 0..10 {
            let start = std::time::Instant::now();
            for _ in 0..count {
                rng.gen_range(0..1024);
            }
            let end = std::time::Instant::now();
            print!("{:.2} ", count as f64 / (end - start).as_secs_f64());
        }
        println!();
        println!();
    }};
}

fn main() -> Result<()> {
    just_rng!("StepRng", StepRng::new(0, 1));
    just_rng!("SmallRng", SmallRng::from_entropy());
    just_rng!("ThreadRng", rand::thread_rng());

    {
        let count = 1_000_000;
        let reps = 10;

        println!("(just the time calls)");

        let mut hist = hdrhist::HDRHist::new();

        println!("throughput (hz), every {count} samples:");
        for _ in 0..reps {
            let start = std::time::Instant::now();
            let mut prev_e = std::time::Instant::now();
            for _ in 0..count {
                let end_e = std::time::Instant::now();
                hist.add_value((end_e - prev_e).as_nanos() as u64);
                prev_e = end_e;
            }
            let end = std::time::Instant::now();
            print!("{:.2} ", count as f64 / (end - start).as_secs_f64());
        }
        println!();

        let total_count = count * reps;
        println!("ccdf of latencies (ns) for {total_count} samples:");
        println!("{}", hist.summary_string());
        println!();
    }

    for num_event_generators in [1, 3].into_iter() {
        println!("== num_event_generators = {num_event_generators} ==");
        with_rng!(
            num_event_generators,
            "StepRng",
            StepRng::new(0, 1),
            10,
            true
        );
        with_rng!(
            num_event_generators,
            "SmallRng",
            SmallRng::from_entropy(),
            10,
            true
        );
        with_rng!(
            num_event_generators,
            "ThreadRng",
            rand::thread_rng(),
            10,
            true
        );
    }

    println!("== StepRng gen_range, batches of 100 ==");
    {
        use rand::Rng;
        let count = 1_000_000;
        let inner_count = 100;

        let mut rng = StepRng::new(0, 1);
        let mut hist = hdrhist::HDRHist::new();

        let mut prev_e = std::time::Instant::now();
        for _ in 0..count {
            for _ in 0..inner_count {
                rng.gen_range(0..1024);
            }
            let end_e = std::time::Instant::now();
            hist.add_value((end_e - prev_e).as_nanos() as u64);
            prev_e = end_e;
        }

        println!("ccdf of latencies (ns) for {count} batches of {inner_count} samples:");
        println!("{}", hist.summary_string());
        println!();
    }

    // println!("== next_event with SmallRng, long experiment ==");
    // with_rng!(1, "SmallRng", SmallRng::from_entropy(), 1_000_000, false);

    Ok(())
}
