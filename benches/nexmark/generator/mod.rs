//! Generators for the models usd in the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink generator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator).

use self::config::Config;
use cached::SizedCache;
use rand::Rng;

mod auctions;
mod bids;
mod config;
mod people;
mod price;
mod strings;

pub struct NexmarkGenerator<R: Rng> {
    config: Config,
    rng: R,
    bid_channel_cache: SizedCache<usize, (String, String)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bids::CHANNELS_NUMBER;
    use rand::rngs::mock::StepRng;

    pub fn make_test_generator() -> NexmarkGenerator<StepRng> {
        NexmarkGenerator {
            config: Config::default(),
            rng: StepRng::new(0, 1),
            bid_channel_cache: SizedCache::with_size(CHANNELS_NUMBER),
        }
    }
}
