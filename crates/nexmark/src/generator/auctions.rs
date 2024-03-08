//! Generates auctions for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/AuctionGenerator.java).

use super::{
    super::model::Auction,
    config::{FIRST_AUCTION_ID, FIRST_CATEGORY_ID, FIRST_PERSON_ID},
    NexmarkGenerator,
};
use anyhow::Result;
use rand::Rng;
use std::{
    cmp,
    mem::{size_of, size_of_val},
};

/// Keep the number of categories small so the example queries will find results
/// even with a small batch of events.
const NUM_CATEGORIES: u64 = 5;

/// Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are
/// 1 over these values.
const HOT_SELLER_RATIO: usize = 100;

impl<R: Rng> NexmarkGenerator<R> {
    /// Generate and return a random auction with the next available id.
    pub fn next_auction(
        &mut self,
        events_count_so_far: u64,
        event_id: u64,
        timestamp: u64,
    ) -> Result<Auction> {
        let id = self.last_base0_auction_id(event_id) + FIRST_AUCTION_ID;

        // Here P(auction will be for a hot seller) = 1 - 1/hot_sellers_ratio.
        let seller = match self
            .rng
            .gen_range(0..self.config.nexmark_config.hot_sellers_ratio)
        {
            0 => self.next_base0_person_id(event_id),
            _ => {
                // Choose the first person in the batch of last hot_sellers_ratio people.
                (self.last_base0_person_id(event_id) / HOT_SELLER_RATIO as u64)
                    * HOT_SELLER_RATIO as u64
            }
        } + FIRST_PERSON_ID;

        let category = FIRST_CATEGORY_ID + self.rng.gen_range(0..NUM_CATEGORIES);
        let initial_bid = self.next_price();

        let next_length_ms: u64 = self.next_auction_length_ms(events_count_so_far, timestamp);

        let item_name = self.next_string(20);
        let description = self.next_string(100);
        let reserve = initial_bid + self.next_price();

        // Not sure why original Java implementation doesn't include date_time, but
        // following the same.
        let current_size = size_of::<u64>()
            + size_of_val(item_name.as_str())
            + size_of_val(description.as_str())
            // (initial_bid, reserve, category)
            + size_of::<usize>() * 3
            // seller, expires
            + size_of::<u64>() * 2;
        Ok(Auction {
            id,
            item_name,
            description,
            initial_bid,
            reserve,
            date_time: timestamp,
            expires: timestamp + next_length_ms,
            seller,
            category,
            extra: self.next_extra(
                current_size,
                self.config.nexmark_config.avg_auction_byte_size,
            ),
        })
    }

    /// Return the last valid auction id (ignoring FIRST_AUCTION_ID). Will be
    /// the current auction id if due to generate an auction.
    pub fn last_base0_auction_id(&self, event_id: u64) -> u64 {
        let mut epoch = event_id / self.config.nexmark_config.total_proportion() as u64;
        let mut offset = event_id % self.config.nexmark_config.total_proportion() as u64;
        let person_proportion = self.config.nexmark_config.person_proportion as u64;
        let auction_proportion = self.config.nexmark_config.auction_proportion as u64;

        if offset < person_proportion {
            // About to generate a person.
            // Go back to the last auction in the last epoch.
            epoch = match epoch.checked_sub(1) {
                Some(e) => e,
                None => return 0,
            };
            offset = auction_proportion - 1;
        } else if offset >= (person_proportion + auction_proportion) {
            // About to generate a bid.
            // Go back to the last auction generated in this epoch.
            offset = auction_proportion - 1;
        } else {
            // About to generate an auction.
            offset -= person_proportion;
        }
        epoch * auction_proportion + offset
    }

    /// Return a random auction id (base 0).
    pub fn next_base0_auction_id(&mut self, next_event_id: u64) -> u64 {
        // Choose a random auction for any of those which are likely to still be in
        // flight, plus a few 'leads'.
        // Note that ideally we'd track non-expired auctions exactly, but that state
        // is difficult to split.
        let min_auction = self
            .last_base0_auction_id(next_event_id)
            .saturating_sub(self.config.nexmark_config.num_in_flight_auctions as u64);
        let max_auction = self.last_base0_auction_id(next_event_id);
        min_auction + self.rng.gen_range(0..(max_auction - min_auction + 1))
    }

    /// Return a random time delay, in milliseconds, for length of auctions.
    fn next_auction_length_ms(&mut self, event_count_so_far: u64, timestamp: u64) -> u64 {
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
            .timestamp_for_event(current_event_number + num_events_for_auctions as u64);
        // Choose a length with average horizon.
        let horizon = future_auction.saturating_sub(timestamp);

        1 + self.rng.gen_range(0..cmp::max(horizon * 2, 1))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::make_test_generator;
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_next_auction() {
        let mut ng = make_test_generator();

        let auction = ng.next_auction(0, 0, 0).unwrap();

        // Note: due to usize differences on windows, need to calculate the
        // size explicitly:
        let current_size = size_of::<u64>()
            + 3
            + 3
            // (initial_bid, reserve, category)
            + size_of::<usize>() * 3
            // seller, expires
            + size_of::<u64>() * 2;
        let delta = ((500 - current_size) as f32 * 0.2).round() as usize;
        let expected_size = 500 - delta - current_size;

        assert_eq!(
            Auction {
                id: FIRST_AUCTION_ID,
                item_name: String::from("AAA"),
                description: String::from("AAA"),
                initial_bid: 100,
                reserve: 200,
                date_time: 0,
                expires: 1,
                seller: 1000,
                category: 10,
                extra: (0..expected_size).map(|_| "A").collect::<String>(),
            },
            auction
        );
    }

    #[rstest]
    // By default an epoch is 50 events and event 0 is a person, events 1, 2 and 3
    // are auctions, then 4-49 are bids.
    // Epoch 0 emits the (zero-based) auctions 0, 1 and 2.
    #[case(0, 0)]
    #[case(1, 0)]
    #[case(2, 1)]
    #[case(3, 2)]
    #[case(23, 2)]
    #[case(49, 2)]
    // Epoch 1: When we're about to generate a person, we return the last auction from
    // the previous epoch.
    #[case(50, 2)] // About to generate a person again
    #[case(50 + 1, 3)]
    #[case(50 + 2, 4)]
    #[case(50 + 3, 5)]
    #[case(50 + 23, 5)]
    // After the 1st person is generated in the 33rd epoch, we have 99 auctions.
    #[case(50*33 + 1, 99)]
    fn test_last_base0_auction_id(#[case] event_id: u64, #[case] expected_id: u64) {
        let ng = make_test_generator();

        let last_auction_id = ng.last_base0_auction_id(event_id);

        assert_eq!(last_auction_id, expected_id);
    }

    #[rstest]
    // Since the default number of inflight auctions is 100, we need to get above an event
    // id of 50*33 events (ie. 33 epochs, since there are 3 auction events per epoch)
    #[case(2, 0)]
    #[case(50 * 33 + 1, 0)] // last_base0_auction_id is 33*3 + 1 - 1 = 99
    #[case(50 * 33 + 2, 0)] // last_base0_auction_id is 33*3 + 2 - 1 = 100
    #[case(50 * 33 + 3, 1)] // last_base0_auction_id is 33*3 + 3 - 1 = 101
    #[case(50 * 34, 1)] // last_base0_auction_id is 34*3 + 0 - 1 = 101
    #[case(50 * 34 + 1, 2)] // last_base0_auction_id is 34*3 + 1 - 1 = 102
    #[case(50 * 34 + 2, 3)] // last_base0_auction_id is 34*3 + 2 - 1 = 103
    #[case(50 * 34 + 3, 4)] // last_base0_auction_id is 34*3 + 3 - 1 = 104
    #[case(50 * 34 + 49, 4)] // last_base0_auction_id is still 104 (all extra are bids)
    #[case(50 * 35, 4)] // last_base0_auction_id is 35*3 + 0 - 1 = 104
    #[case(50 * 35 + 1, 5)] // last_base0_auction_id is 35*3 + 1 - 1 = 105
    fn test_next_base0_auction_id(#[case] next_event_id: u64, #[case] expected_id: u64) {
        let mut ng = make_test_generator();

        let next_auction_id = ng.next_base0_auction_id(next_event_id);

        assert_eq!(next_auction_id, expected_id);
    }

    #[test]
    fn test_next_auction_length() {
        let mut ng = make_test_generator();

        let len_ms = ng.next_auction_length_ms(0, 0);

        // Since StepRng always returns zero, can only test the lower bound here.
        assert_eq!(len_ms, 1);
    }
}
