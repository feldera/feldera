//! Configuration options for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink Configuration API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/NexmarkConfiguration.java)
//! and the specific [Nexmark Flink Generator config](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java).

use clap::{Args, Parser};

pub use crate::queries::Query;

// Number of yet-to-be-created people and auction ids allowed.
pub const PERSON_ID_LEAD: usize = 10;

/// A Nexmark streaming data source generator
///
/// Based on the Java/Flink generator found in the [Nexmark repository](https://github.com/nexmark/nexmark).
#[derive(Clone, Debug, Parser)]
#[clap(author, version, about)]
pub struct Config {
    // Cargo passes any `--bench nexmark` (for example) through to our main
    // as an arg, so just ensure it's a valid arg option for now.
    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    pub __bench: bool,

    #[clap(flatten)]
    pub generator_options: GeneratorOptions,

    /// Number of CPU cores to be available.
    #[clap(long, default_value = "2", env = "NEXMARK_CPU_CORES")]
    pub cpu_cores: usize,

    /// Dump DBSP profiles for all executed queries to the specified directory.
    #[clap(long, env = "NEXMARK_PROFILE_PATH")]
    pub profile_path: Option<String>,

    /// Queries to run, all by default.
    #[clap(long, env = "NEXMARK_QUERIES", value_enum)]
    pub query: Vec<Query>,

    /// DBSP-specific configuration options.
    /// The size of the batches to be inputted to DBSP per step.
    #[clap(long, default_value = "40000", env = "DBSP_INPUT_BATCH_SIZE")]
    pub input_batch_size: usize,

    /// Store results in a csv file in addition to printing on the command-line.
    #[clap(long = "csv", env = "DBSP_RESULTS_AS_CSV")]
    pub output_csv: Option<String>,

    /// Disable progress bar.
    #[clap(long = "no-progress", default_value_t = true, action = clap::ArgAction::SetFalse)]
    pub progress: bool,

    /// Enable storage.
    #[clap(long)]
    pub storage: bool,
}

/// Properties of the generated input events.
#[derive(Clone, Debug, Args)]
pub struct GeneratorOptions {
    /// The size of the buffer (channel) to use in the Nexmark Source.
    #[clap(long, default_value = "10000", env = "NEXMARK_SOURCE_BUFFER_SIZE")]
    pub source_buffer_size: usize,

    /// Specify the proportion of events that will be new auctions.
    #[clap(long, default_value = "3", env = "NEXMARK_AUCTION_PROPORTION")]
    pub auction_proportion: usize,

    /// Average idealized size of a 'new auction' event, in bytes.
    #[clap(long, default_value = "500", env = "NEXMARK_AVG_AUCTION_BYTE_SIZE")]
    pub avg_auction_byte_size: usize,

    /// Average idealized size of a 'bid event, in bytes.
    #[clap(long, default_value = "100", env = "NEXMARK_AVG_BID_BYTE_SIZE")]
    pub avg_bid_byte_size: usize,

    /// Average idealized size of a 'new person' event, in bytes.
    #[clap(long, default_value = "200", env = "NEXMARK_AVG_PERSON_BYTE_SIZE")]
    pub avg_person_byte_size: usize,

    /// Specify the proportion of events that will be new bids.
    #[clap(long, default_value = "46", env = "NEXMARK_BID_PROPORTION")]
    pub bid_proportion: usize,

    /// Initial overall event rate (per second).
    #[clap(long, default_value = "10000000", env = "NEXMARK_FIRST_EVENT_RATE")]
    pub first_event_rate: usize,

    /// Ratio of bids to 'hot' auctions compared to all other auctions.
    #[clap(long, default_value = "2", env = "NEXMARK_HOT_AUCTION_RATIO")]
    pub hot_auction_ratio: usize,

    /// Ratio of bids for 'hot' bidders compared to all other people.
    #[clap(long, default_value = "4", env = "NEXMARK_HOT_BIDDERS_RATIO")]
    pub hot_bidders_ratio: usize,

    /// Ration of auctions for 'hot' sellers compared to all other people.
    #[clap(long, default_value = "4", env = "NEXMARK_HOT_SELLERS_RATIO")]
    pub hot_sellers_ratio: usize,

    /// Max number of events to be generated. 0 is unlimited.
    #[clap(long, default_value = "100000000", env = "NEXMARK_MAX_EVENTS")]
    pub max_events: u64,

    /// Maximum number of people to consider as active for placing auctions or
    /// bids.
    #[clap(long, default_value = "1000", env = "NEXMARK_NUM_ACTIVE_PEOPLE")]
    pub num_active_people: usize,

    /// Number of event generators to use. Each generates events in its own
    /// timeline.
    #[clap(long, default_value = "2", env = "NEXMARK_NUM_EVENT_GENERATORS")]
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
impl GeneratorOptions {
    pub fn total_proportion(&self) -> usize {
        self.person_proportion + self.auction_proportion + self.bid_proportion
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            __bench: true,
            generator_options: GeneratorOptions::default(),
            cpu_cores: 2,
            profile_path: None,
            query: Vec::new(),
            input_batch_size: 40_000,
            output_csv: None,
            progress: true,
            storage: false,
        }
    }
}

impl Default for GeneratorOptions {
    fn default() -> Self {
        Self {
            auction_proportion: 3,
            avg_auction_byte_size: 500,
            avg_bid_byte_size: 100,
            avg_person_byte_size: 200,
            bid_proportion: 46,
            first_event_rate: 10_000_000,
            hot_auction_ratio: 2,
            hot_bidders_ratio: 4,
            hot_sellers_ratio: 4,
            max_events: 100_000_000,
            num_active_people: 1000,
            num_event_generators: 2,
            num_in_flight_auctions: 100,
            out_of_order_group_size: 1,
            person_proportion: 1,
            source_buffer_size: 10_000,
        }
    }
}
