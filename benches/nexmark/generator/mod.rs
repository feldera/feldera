//! Generators for the models usd in the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink generator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator).

use self::config::Config;
use rand::Rng;

mod auctions;
mod config;
mod people;
mod price;
mod strings;

pub struct NexmarkGenerator<R: Rng> {
    config: Config,
    rng: R,
}
