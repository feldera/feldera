#[path = "../mimalloc.rs"]
mod mimalloc;

use anyhow::Result;

use rand::rngs::mock::StepRng;

use dbsp::nexmark::{
    config::Config as NexmarkConfig,
    generator::{config::Config, NexmarkGenerator},
};

use mimalloc::MiMalloc;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let count = 2_000_000;

    for num_event_generators in 1..4 {
        for _ in 0..10 {
            let config = Config {
                nexmark_config: NexmarkConfig {
                    num_event_generators,
                    ..NexmarkConfig::default()
                },
                ..Config::default()
            };
            let mut generator = NexmarkGenerator::new(config, StepRng::new(0, 1), 0);

            print!("num_event_generators = {num_event_generators}: ");

            for _ in 0..5 {
                let start = std::time::Instant::now();
                for _ in 0..count {
                    generator.next_event().unwrap();
                }
                let end = std::time::Instant::now();
                print!("{:.2} ", count as f64 / (end - start).as_secs_f64());
            }

            println!("hz");
        }
    }

    Ok(())
}
