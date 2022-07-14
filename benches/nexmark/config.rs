//! Configuration options for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink Configuration API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java)
//! and the specific [Nexmark Flink Generator config](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java).
use clap::Parser;

// Number of yet-to-be-created people and auction ids allowed.
pub const PERSON_ID_LEAD: usize = 10;

/// A Nexmark streaming data source generator
///
/// Based on the Java/Flink generator found in the [Nexmark repository](https://github.com/nexmark/nexmark).
#[derive(Parser, Debug)]
#[clap(author, version, about)]
pub struct Config {
    /// Specify the proportion of events that will be new auctions.
    #[clap(long, default_value = "3", env = "NEXMARK_AUCTION_PROPORTION")]
    pub auction_proportion: usize,

    /// Specify the proportion of events that will be new bids.
    #[clap(long, default_value = "46", env = "NEXMARK_BID_PROPORTION")]
    pub bid_proportion: usize,

    /// Initial overall event rate (per second).
    #[clap(long, default_value = "10000", env = "NEXMARK_FIRST_EVENT_RATE")]
    pub first_event_rate: usize,

    /// Ration of auctions for 'hot' sellers compared to all other people.
    #[clap(long, default_value = "4", env = "NEXMARK_HOT_SELLERS_RATIO")]
    pub hot_sellers_ratio: usize,

    /// Maximum number of people to consider as active for placing auctions or
    /// bids.
    #[clap(long, default_value = "1000", env = "NEXMARK_NUM_ACTIVE_PEOPLE")]
    pub num_active_people: usize,

    /// Number of event generators to use. Each generates events in its own
    /// timeline.
    #[clap(long, default_value = "1", env = "NEXMARK_NUM_EVENT_GENERATORS")]
    pub num_event_generators: usize,

    /// Average number of auctions which should be inflight at any time, per
    /// generator.
    #[clap(long, default_value = "100", env = "NEXMARK_NUM_IN_FLIGHT_AUCTIONS")]
    pub num_in_flight_auctions: usize,

    /// Number of events in out-of-order groups. 1 implies no out-of-order
    /// events. 1000 implies every 1000 events per generator are emitted in
    /// pseudo-random order.
    #[clap(long, default_value = "1", env = "NEXMARK_OUT_OF_ORDER_GROUP_SIZE")]
    pub out_of_order_group_size: usize,

    /// Specify the proportion of events that will be new people.
    #[clap(long, default_value = "1", env = "NEXMARK_PERSON_PROPORTION")]
    pub person_proportion: usize,
}

/// Implementation of config methods based on the Java implementation at
/// [NexmarkConfig.java](https://github.com/nexmark/nexmark/blob/master/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java).
impl Config {
    pub fn total_proportion(&self) -> usize {
        self.person_proportion + self.auction_proportion + self.bid_proportion
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            auction_proportion: 3,
            bid_proportion: 46,
            first_event_rate: 10_000,
            hot_sellers_ratio: 4,
            num_active_people: 1000,
            num_event_generators: 1,
            num_in_flight_auctions: 100,
            out_of_order_group_size: 1,
            person_proportion: 1,
        }
    }
}
