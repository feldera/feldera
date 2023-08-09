//! Generates bids for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/BidGenerator.java).
use super::NexmarkGenerator;
use super::{
    super::model::Bid,
    config::{FIRST_AUCTION_ID, FIRST_PERSON_ID},
    strings::next_string,
};
use dbsp::{algebra::ArcStr, arcstr_format, arcstr_literal};
use cached::Cached;
use rand::Rng;
use std::mem::size_of;

/// Fraction of people/auctions which may be 'hot' sellers/bidders/auctions are
/// 1 over these values.
const HOT_AUCTON_RATIO: usize = 100;
const HOT_BIDDER_RATIO: usize = 100;
const HOT_CHANNELS_RATIO: usize = 100;

pub const CHANNELS_NUMBER: u32 = 10_000;

static HOT_CHANNELS: [ArcStr; 4] = [
    arcstr_literal!("Google"),
    arcstr_literal!("Facebook"),
    arcstr_literal!("Baidu"),
    arcstr_literal!("Apple"),
];
static HOT_URLS: [ArcStr; 4] = [
    arcstr_literal!("https://www.nexmark.com/googl/item.htm?query=1"),
    arcstr_literal!("https://www.nexmark.com/meta/item.htm?query=1"),
    arcstr_literal!("https://www.nexmark.com/bidu/item.htm?query=1"),
    arcstr_literal!("https://www.nexmark.com/aapl/item.htm?query=1"),
];

const BASE_URL_PATH_LENGTH: usize = 5;

impl<R: Rng> NexmarkGenerator<R> {
    fn get_new_channel_instance(&mut self, channel_number: u32) -> (ArcStr, ArcStr) {
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
                    _ => format!("{url}&channel_id={}", channel_number.reverse_bits()).into(),
                };

                (format!("channel-{channel_number}").into(), url)
            })
            .clone()
    }

    pub fn next_bid(&mut self, event_id: u64, timestamp: u64) -> Bid {
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
                (HOT_CHANNELS[hot_index].clone(), HOT_URLS[hot_index].clone())
            }
        };
        // Original Java implementation calculates the size of the bid as
        // 8 * 4 - which can only be the auction, bidder, price and date_time (only 4
        // numbers on the record). Not sure why the channel and url (Strings)
        // are not included, but following suit.
        let current_size = size_of::<u64>() * 3 + size_of::<usize>();

        Bid {
            auction,
            bidder,
            price,
            channel,
            url,
            date_time: timestamp,
            extra: self.next_extra(current_size, self.config.nexmark_config.avg_bid_byte_size),
        }
    }
}

fn get_base_url<R: Rng>(rng: &mut R) -> ArcStr {
    arcstr_format!(
        "https://www.nexmark.com/{}/item.htm?query=1",
        next_string(rng, BASE_URL_PATH_LENGTH),
    )
}

#[cfg(test)]
pub mod tests {
    use super::super::tests::make_test_generator;
    use super::*;
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

        // Note: due to usize differences on windows, need to calculate the
        // size explicitly:
        let current_size = size_of::<u64>() * 3 + size_of::<usize>();
        let delta = ((100 - current_size) as f32 * 0.2).round() as usize;
        let expected_size = 100 - delta - current_size;

        assert_eq!(
            Bid {
                auction: expected_auction_id,
                bidder: expected_bidder_id,
                price: 100,
                channel: arcstr_literal!("Google"),
                url: arcstr_literal!("https://www.nexmark.com/googl/item.htm?query=1"),
                date_time: 1_000_000_000_000,
                extra: "A".repeat(expected_size).into(),
            },
            bid
        );
    }

    #[test]
    fn test_get_base_url() {
        let mut rng = StepRng::new(0, 1);
        assert_eq!(
            get_base_url(&mut rng),
            "https://www.nexmark.com/AAA/item.htm?query=1",
        );
    }

    #[test]
    fn test_get_new_channel_instance() {
        let mut ng = make_test_generator();

        let (channel_name, channel_url) = ng.get_new_channel_instance(1234);

        assert_eq!(channel_name, "channel-1234");
        assert_eq!(
            channel_url,
            "https://www.nexmark.com/AAA/item.htm?query=1&channel_id=1260388352",
        );
    }

    #[test]
    fn test_get_new_channel_instance_cached() {
        let mut ng = make_test_generator();
        ng.bid_channel_cache.cache_set(
            1234,
            (
                arcstr_literal!("Google"),
                arcstr_literal!("https://google.example.com"),
            ),
        );

        let (channel_name, channel_url) = ng.get_new_channel_instance(1234);

        assert_eq!(channel_name, "Google");
        assert_eq!(channel_url, "https://google.example.com");
    }
}
