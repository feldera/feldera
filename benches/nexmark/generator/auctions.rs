//! Generates people for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/AuctionGenerator.java).

use super::NexmarkGenerator;
use anyhow::Result;
use rand::Rng;
use std::{
    cmp,
    time::{Duration, SystemTime},
};

impl<R: Rng> NexmarkGenerator<R> {
    fn next_auction_length_ms(
        &mut self,
        event_count_so_far: usize,
        timestamp: SystemTime,
    ) -> Result<Duration> {
        // What's our current event number?
        let current_event_number = self.config.next_adjusted_event_number(event_count_so_far);
        // How many events until we've generated num_in_flight_actions?
        // E.g. with defaults, this is 100 * 50 / 3 = 1666 total events (bids, people,
        // auctions)
        let num_events_for_auctions = (self.config.nexmark_config.num_in_flight_auctions
            * self.config.nexmark_config.total_proportion())
            / self.config.nexmark_config.auction_proportion;
        // When will the auction num_in_flight_auctions beyond now be generated?
        // E.g. with defaults, timestamp for the event 1666 from now
        // (corresponding to 100 auctions from now).
        let future_auction = self
            .config
            .timestamp_for_event((current_event_number + num_events_for_auctions) as u64);
        // Choose a length with average horizon.
        let horizon = future_auction.duration_since(timestamp)?;

        let next_duration_ms: u128 =
            1 + self.rng.gen_range(0..cmp::max(horizon.as_millis() * 2, 1));

        Ok(Duration::from_millis(next_duration_ms.try_into()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::config::Config;
    use rand::rngs::mock::StepRng;

    #[test]
    fn test_next_auction_length_ms() {
        let mut ng = NexmarkGenerator {
            rng: StepRng::new(0, 5),
            config: Config::default(),
        };

        let len_ms = ng
            .next_auction_length_ms(0, SystemTime::UNIX_EPOCH)
            .unwrap();

        // Since StepRng always returns zero, can only test the lower bound here.
        assert_eq!(len_ms.as_millis(), 1);
    }
}
