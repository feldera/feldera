use super::super::config::Config as NexmarkConfig;

// We start the ids at specific values to help ensure the queries find a match
// even on small synthesized dataset sizes.
pub const FIRST_PERSON_ID: usize = 1000;
pub const FIRST_AUCTION_ID: usize = 1000;
pub const FIRST_CATEGORY_ID: usize = 10;

/// The generator config is a combination of the CLI configuration and the
/// options specific to this generator instantiation.
pub struct Config {
    pub nexmark_config: NexmarkConfig,

    /// Time for first event (ms since epoch).
    pub base_time: u64,

    /// Event id of first event to be generated. Event ids are unique over all
    /// generators, and are used as a seed to generate each event's data.
    pub first_event_id: u64,

    /// Maximum number of events to generate.
    pub max_events: u64,

    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp
    pub first_event_number: usize,

    /// Delay between events, in microseconds. If the array has more than one
    /// entry then the rate is changed every {@link #stepLengthSec}, and wraps
    /// around.
    pub inter_event_delay_us: [usize; 5],
}

/// Implementation of config methods based on the Java implementation at
/// [GeneratorConfig.java](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java)
impl Config {
    pub fn new(
        nexmark_config: NexmarkConfig,
        base_time: u64,
        first_event_id: u64,
        max_events_or_zero: u64,
        first_event_number: usize,
    ) -> Config {
        let inter_event_delay =
            1_000_000 / nexmark_config.first_event_rate * nexmark_config.num_event_generators;

        // Original Java implementation says:
        // "Scale maximum down to avoid overflow in getEstimatedSizeBytes."
        // but including to ensure similar behavior.
        let max_events = match max_events_or_zero {
            0 => {
                let max_average = *[
                    nexmark_config.avg_person_byte_size,
                    nexmark_config.avg_auction_byte_size,
                    nexmark_config.avg_bid_byte_size,
                ]
                .iter()
                .max()
                .unwrap();
                u64::MAX / (nexmark_config.total_proportion() as u64 * max_average as u64)
            }
            _ => max_events_or_zero,
        };
        Config {
            nexmark_config,
            base_time,
            first_event_id,
            max_events,
            first_event_number,
            inter_event_delay_us: [inter_event_delay, 0, 0, 0, 0],
        }
    }

    /// Return the next event number for a generator which has so far emitted
    /// `num_events`.
    pub fn next_event_number(&self, num_events: u64) -> u64 {
        self.first_event_number as u64 + num_events
    }

    /// Return the next event number for a generator which has so far emitted
    /// `num_events`, but adjusted to account for `out_of_order_group_size`.
    pub fn next_adjusted_event_number(&self, num_events: u64) -> u64 {
        let n = self.nexmark_config.out_of_order_group_size as u64;
        let event_number = self.next_event_number(num_events);
        let base = (event_number / n) * n;
        let offset = (event_number * 953) % n;
        base + offset
    }

    /// Return the event number whose event time will be a suitable watermark
    /// for a generator which has so far emitted `num_events`.
    pub fn next_event_number_for_watermark(&self, num_events: u64) -> u64 {
        let n = self.nexmark_config.out_of_order_group_size as u64;
        let event_number = self.next_event_number(num_events);
        (event_number / n) * n
    }

    // What timestamp should the event with `eventNumber` have for this
    // generator?
    pub fn timestamp_for_event(&self, event_number: u64) -> u64 {
        self.base_time + self.inter_event_delay_us[0] as u64 * event_number / 1000
    }
}

impl Default for Config {
    fn default() -> Self {
        // TODO(absoludity): In the Java implementation, both the firstEventID
        // and the firstEventNumber are set to 1, as shown in:
        //
        // https://github.com/nexmark/nexmark/blob/54974ef36a0d01ef8ebc0b4ba39cfc50136af0f6/nexmark-flink/src/main/java/com/github/nexmark/flink/source/NexmarkTableSourceFactory.java#L50
        //
        // but AFAICT (and according to my tests) this means that the first
        // event id is 2 so the first event is *not* the first person, but
        // rather an auction with id 1001, that refers to a seller, a person,
        // with the id 1000, that was never generated. I need to triple-check
        // the Java output before creating an issue against their repo, but for
        // now I'm using defaults of 0 for both, which results in the expected
        // events (first event is a person with id 1000, etc.).
        Config::new(NexmarkConfig::default(), 0, 0, 0, 0)
    }
}

#[cfg(test)]
pub mod tests {
    use super::super::super::config::Config as NexmarkConfig;
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_next_event_number() {
        assert_eq!(Config::default().next_event_number(4), 4);
    }

    #[rstest]
    #[case(0, 0)]
    #[case(1, 1)]
    #[case(2, 2)]
    #[case(199, 199)]
    fn test_next_adjusted_event_number_default(#[case] num_events: u64, #[case] expected: u64) {
        assert_eq!(
            Config::default().next_adjusted_event_number(num_events),
            expected
        );
    }

    // When the out-of-order-group-size is 3, each group of three numbers will
    // have a pseudo-random order, but only from within the same group of three.
    #[rstest]
    #[case(0, 0)]
    #[case(1, 2)]
    #[case(2, 1)]
    #[case(3, 3)]
    #[case(4, 5)]
    #[case(5, 4)]
    fn test_next_adjusted_event_number_custom_group_size_3(
        #[case] num_events: u64,
        #[case] expected: u64,
    ) {
        // Seems to be issues in the Java implementation?!
        let config = Config {
            nexmark_config: NexmarkConfig {
                out_of_order_group_size: 3,
                ..NexmarkConfig::default()
            },
            ..Config::default()
        };
        assert_eq!(config.next_adjusted_event_number(num_events), expected);
    }

    // With the default first event rate of 10_000 events per second and one
    // generator, there is 1_000_000 µs/s / 10_000 events/s = 100µs / event, so
    // the timestamp increases by 100µs, ie. 0.1ms for each event.
    #[rstest]
    #[case(10, 1)]
    #[case(20, 2)]
    #[case(50, 5)]
    fn test_timestamp_for_event(#[case] event_number: u64, #[case] expected: u64) {
        assert_eq!(
            Config::default().timestamp_for_event(event_number),
            expected,
        );
    }
}
