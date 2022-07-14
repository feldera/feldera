//! Generates prices for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PriceGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PriceGenerator.java).

use super::NexmarkGenerator;
use rand::Rng;

impl<R: Rng> NexmarkGenerator<R> {
    pub fn next_price(&mut self) -> usize {
        (10.0_f32.powf(self.rng.gen_range(0.0..1.0) * 6.0) * 100.0).ceil() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::config::Config;
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_next_price() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 1),
            config: Config::default(),
        };

        let p = ng.next_price();

        assert_eq!(p, 10_usize.pow(0) * 100);
    }
}
