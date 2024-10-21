//! Generates prices for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PriceGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PriceGenerator.java).

use super::GeneratorContext;
use rand::Rng;

impl<R: Rng> GeneratorContext<'_, R> {
    pub fn next_price(&mut self) -> u64 {
        (10.0_f32.powf(self.rng.gen_range(0.0..1.0) * 6.0) * 100.0).ceil() as u64
    }
}

#[cfg(test)]
mod tests {
    use crate::generator::GeneratorContext;

    use super::super::tests::make_test_generator;

    #[test]
    fn test_next_price() {
        let (core, mut rng) = make_test_generator();
        let mut gc = GeneratorContext::new(&core, &mut rng);

        let p = gc.next_price();

        assert_eq!(p, 10_u64.pow(0) * 100);
    }
}
