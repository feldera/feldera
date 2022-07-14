use super::super::config::Config as NexmarkConfig;
use std::time::{Duration, SystemTime};

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
    pub base_time: SystemTime,

    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp"
    pub first_event_number: usize,

    /// Delay between events, in microseconds. If the array has more than one
    /// entry then the rate is changed every {@link #stepLengthSec}, and wraps
    /// around.
    pub inter_event_delay_us: [usize; 5],
}

/// Implementation of config methods based on the Java implementation at
/// [GeneratorConfig.java](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java)
impl Config {
    pub fn new(nexmark_config: NexmarkConfig, base_time: u64, first_event_number: usize) -> Config {
        let inter_event_delay =
            1_000_000 / nexmark_config.first_event_rate * nexmark_config.num_event_generators;

        Config {
            nexmark_config,
            base_time: SystemTime::UNIX_EPOCH + Duration::from_millis(base_time),
            first_event_number,
            inter_event_delay_us: [inter_event_delay, 0, 0, 0, 0],
        }
    }

    // Return the next event number for a generator which has so far emitted
    // `num_events`.
    pub fn next_event_number(&self, num_events: u64) -> u64 {
        self.first_event_number as u64 + num_events
    }

    // Return the next event number for a generator which has so far emitted
    // `num_events`, but adjusted to account for `out_of_order_group_size`.
    pub fn next_adjusted_event_number(&self, num_events: u64) -> u64 {
        let n = self.nexmark_config.out_of_order_group_size as u64;
        let event_number = self.next_event_number(num_events);
        let base = (event_number / n) * n;
        let offset = (event_number * 953) % n;
        base + offset
    }

    // What timestamp should the event with `eventNumber` have for this
    // generator?
    pub fn timestamp_for_event(&self, event_number: u64) -> SystemTime {
        return self.base_time
            + Duration::from_micros(self.inter_event_delay_us[0] as u64 * event_number);
    }
}

impl Default for Config {
    fn default() -> Self {
        Config::new(NexmarkConfig::default(), 0, 1)
    }
}
#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::config::Config as NexmarkConfig;
    use rstest::rstest;

    #[test]
    fn test_next_event_number() {
        assert_eq!(Config::default().next_event_number(4), 5);
    }

    #[rstest]
    #[case(0, 1)]
    #[case(1, 2)]
    #[case(2, 3)]
    #[case(199, 200)]
    fn test_next_adjusted_event_number_default(#[case] num_events: u64, #[case] expected: u64) {
        assert_eq!(
            Config::default().next_adjusted_event_number(num_events),
            expected
        );
    }

    // When the out-of-order-group-size is 3, each group of three numbers will
    // have a pseudo-random order, but only from within the same group of three.
    #[rstest]
    #[case(0, 2)]
    #[case(1, 1)]
    #[case(2, 3)]
    #[case(3, 5)]
    #[case(4, 4)]
    #[case(5, 6)]
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
    // the timestamp increases by 100µs for each event.
    #[rstest]
    #[case(1, SystemTime::UNIX_EPOCH + Duration::from_micros(100*1))]
    #[case(2, SystemTime::UNIX_EPOCH + Duration::from_micros(100*2))]
    #[case(5, SystemTime::UNIX_EPOCH + Duration::from_micros(100*5))]
    fn test_timestamp_for_event(#[case] event_number: u64, #[case] expected: SystemTime) {
        assert_eq!(
            Config::default().timestamp_for_event(event_number),
            expected,
        );
    }
}
