//! Configuration options for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink Configuration API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java)
//! and the specific [Nexmark Flink Generator config](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java).
use clap::Parser;

// We start the ids at specific values to help ensure the queries find a match
// even on small synthesized dataset sizes.
pub const FIRST_PERSON_ID: usize = 1000;

// Number of yet-to-be-created people and auction ids allowed.
pub const PERSON_ID_LEAD: usize = 10;

/// Maximum number of people to consider as active for placing auctions or bids.
pub const NUM_ACTIVE_PEOPLE: usize = 1000;

/// A Nexmark streaming data source generator
///
/// Based on the Java/Flink generator found in the [Nexmark repository](https://github.com/nexmark/nexmark).
#[derive(Parser, Debug)]
#[clap(author, version, about)]
pub struct Config {
    #[clap(
        long = "person-proportion",
        default_value = "1",
        env = "NEXMARK_PERSON_PROPORTION",
        help = "Specify the proportion of events that will be new people"
    )]
    pub person_proportion: usize,

    #[clap(
        long = "auction-proportion",
        default_value = "3",
        env = "NEXMARK_AUCTION_PROPORTION",
        help = "Specify the proportion of events that will be new auctions"
    )]
    pub auction_proportion: usize,

    #[clap(
        long = "bit-proportion",
        default_value = "46",
        env = "NEXMARK_BID_PROPORTION",
        help = "Specify the proportion of events that will be new bids"
    )]
    pub bid_proportion: usize,
}

impl Config {
    pub fn total_proportion(&self) -> usize {
        self.person_proportion + self.auction_proportion + self.bid_proportion
    }
}
