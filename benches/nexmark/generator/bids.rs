//! Generates bids for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/BidGenerator.java).
use super::config::{FIRST_AUCTION_ID, FIRST_PERSON_ID};
use super::strings::next_string;
use super::NexmarkGenerator;
use crate::model::Bid;
use cached::Cached;
use rand::Rng;
use std::time::{Duration, SystemTime};

/// Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are
/// 1 over these values.
const HOT_AUCTON_RATIO: usize = 100;
const HOT_BIDDER_RATIO: usize = 100;
const HOT_CHANNELS_RATIO: usize = 100;

pub const CHANNELS_NUMBER: usize = 10_000;

const HOT_CHANNELS: [&str; 4] = ["Google", "Facebook", "Baidu", "Apple"];
const HOT_URLS: [&str; 4] = [
    "https://www.nexmark.com/googl/item.htm?query=1",
    "https://www.nexmark.com/meta/item.htm?query=1",
    "https://www.nexmark.com/bidu/item.htm?query=1",
    "https://www.nexmark.com/aapl/item.htm?query=1",
];
const BASE_URL_PATH_LENGTH: usize = 5;

impl<R: Rng> NexmarkGenerator<R> {
    fn get_new_channel_instance(&mut self, channel_number: usize) -> (String, String) {
        // Manually check the cache. Note: using a manual SizedCache because the
        // `cached` library doesn't allow using the proc_macro `cached` with
        // `self`.
        self.bid_channel_cache
            .cache_get_or_set_with(channel_number, || {
                let mut url = get_base_url(&mut self.rng);
                // Just following the Java implementation: 1 in 10 chance that
                // the URL is returned as is, otherwise a channel_id query param is
                // added to the URL. Also following the Java implementation
                // which uses `Integer.reverse` to get a deterministic channel_id.
                url = match self.rng.gen_range(0..10) {
                    9 => url,
                    _ => format!("{}&channel_id={}", url, channel_number.reverse_bits()),
                };

                (format!("channel-{}", channel_number), url)
            })
            .clone()
    }
}

fn get_base_url<R: Rng>(rng: &mut R) -> String {
    format!(
        "https://www.nexmark.com/{}/item.htm?query=1",
        next_string(rng, BASE_URL_PATH_LENGTH)
    )
}

impl<R: Rng> NexmarkGenerator<R> {
    fn next_bid(&mut self, event_id: u64, timestamp: u64) -> Bid {
        let auction = match self
            .rng
            .gen_range(0..self.config.nexmark_config.hot_auction_ratio)
        {
            0 => self.next_base0_auction_id(event_id),
            _ => {
                // Choose the first auction in the batch of last HOT_AUCTION_RATIO auctions.
                (self.last_base0_auction_id(event_id) / HOT_AUCTON_RATIO as u64)
                    * HOT_AUCTON_RATIO as u64
            }
        } + FIRST_AUCTION_ID as u64;

        let bidder = match self
            .rng
            .gen_range(0..self.config.nexmark_config.hot_bidders_ratio)
        {
            0 => self.next_base0_person_id(event_id),
            _ => {
                // Choose the second person (so hot bidders and hot sellers don't collide) in
                // the batch of last HOT_BIDDER_RATIO people.
                (self.last_base0_person_id(event_id) / HOT_BIDDER_RATIO as u64)
                    * HOT_BIDDER_RATIO as u64
                    + 1
            }
        } + FIRST_PERSON_ID as u64;

        let price = self.next_price();

        // NOTE: Shifted the start and finish of the range here simply so that when
        // testing with the StepRng, we can test the deterministic case where the first
        // hot channel (Google) is used.
        let channel_number = self.rng.gen_range(0..CHANNELS_NUMBER);
        let (channel, url) = match self.rng.gen_range(1..=HOT_CHANNELS_RATIO) {
            HOT_CHANNELS_RATIO => self.get_new_channel_instance(channel_number),
            _ => {
                let hot_index = self.rng.gen_range(0..HOT_CHANNELS.len());
                (HOT_CHANNELS[hot_index].into(), HOT_URLS[hot_index].into())
            }
        };

        Bid {
            auction,
            bidder,
            price,
            channel,
            url,
            date_time: SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp),
            extra: String::new(),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::generator::tests::make_test_generator;
    use rand::rngs::mock::StepRng;
    use rstest::rstest;

    #[rstest]
    #[case(0, 1_000, 1_000)]
    #[case(50*34 + 3, 1_004, 1_000)]
    #[case(50*1500, 5_399, 1_501)]
    fn test_next_bid(
        #[case] event_id: u64,
        #[case] expected_auction_id: u64,
        #[case] expected_bidder_id: u64,
    ) {
        let mut ng = make_test_generator();

        let bid = ng.next_bid(event_id, 1_000_000_000_000);

        assert_eq!(
            Bid {
                auction: expected_auction_id,
                bidder: expected_bidder_id,
                price: 100,
                channel: "Google".into(),
                url: "https://www.nexmark.com/googl/item.htm?query=1".into(),
                date_time: SystemTime::UNIX_EPOCH + Duration::from_millis(1_000_000_000_000),
                extra: String::new(),
            },
            bid
        );
    }

    #[test]
    fn test_get_base_url() {
        let mut rng = StepRng::new(0, 1);
        assert_eq!(
            get_base_url(&mut rng),
            String::from("https://www.nexmark.com/AAA/item.htm?query=1")
        );
    }

    #[test]
    fn test_get_new_channel_instance() {
        let mut ng = make_test_generator();

        let (channel_name, channel_url) = ng.get_new_channel_instance(1234);

        assert_eq!(channel_name, "channel-1234");
        assert_eq!(
            channel_url,
            "https://www.nexmark.com/AAA/item.htm?query=1&channel_id=5413326752099336192"
        );
    }

    #[test]
    fn test_get_new_channel_instance_cached() {
        let mut ng = make_test_generator();
        ng.bid_channel_cache
            .cache_set(1234, ("Google".into(), "https://google.example.com".into()));

        let (channel_name, channel_url) = ng.get_new_channel_instance(1234);

        assert_eq!(channel_name, "Google");
        assert_eq!(channel_url, "https://google.example.com");
    }
}
