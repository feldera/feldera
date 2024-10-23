use crate::config::GeneratorOptions;

// We start the ids at specific values to help ensure the queries find a match
// even on small synthesized dataset sizes.
pub const FIRST_PERSON_ID: u64 = 1000;
pub const FIRST_AUCTION_ID: u64 = 1000;
pub const FIRST_CATEGORY_ID: u64 = 10;

/// The generator config is a combination of the CLI configuration and the
/// options specific to this generator instantiation.
#[derive(Clone)]
pub struct Config {
    pub options: GeneratorOptions,

    /// Time for first event (ms since epoch).
    pub base_time: u64,

    /// Maximum number of events to generate.
    pub max_events: u64,

    /// Generators running in parallel time may share the same event number, and
    /// the event number is used to determine the event timestamp
    /// TODO: Cannot yet make sense of the above (original) comment. Generators
    /// running in parallel may share the same event space, but should still be
    /// generating different event numbers (using `first_event_number +
    /// events_count_so_far*num_generators`, for example).
    pub first_event_number: usize,
}

/// Implementation of config methods based on the Java implementation at
/// [GeneratorConfig.java](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/GeneratorConfig.java)
impl Config {
    pub fn new(options: GeneratorOptions, base_time: u64, first_event_number: usize) -> Config {
        // Original Java implementation says:
        // "Scale maximum down to avoid overflow in getEstimatedSizeBytes."
        // but including to ensure similar behavior.
        let max_events = match options.max_events {
            0 => {
                let max_average = *[
                    options.avg_person_byte_size,
                    options.avg_auction_byte_size,
                    options.avg_bid_byte_size,
                ]
                .iter()
                .max()
                .unwrap();
                u64::MAX / (options.total_proportion() as u64 * max_average as u64)
            }
            _ => options.max_events,
        };
        Config {
            options,
            base_time,
            max_events,
            first_event_number,
        }
    }

    /// Return the next event number for a generator which has so far emitted
    /// `num_events`.
    pub fn next_event_number(&self, num_events: u64) -> u64 {
        self.first_event_number as u64 + num_events * self.options.num_event_generators as u64
    }

    // What timestamp should the event with `eventNumber` have for this
    // generator?
    pub fn timestamp_for_event(&self, event_number: u64) -> u64 {
        self.base_time + self.options.event_interval as u64 * event_number
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
        Config::new(GeneratorOptions::default(), 0, 0)
    }
}

#[cfg(test)]
pub mod tests {
    use crate::config::GeneratorOptions;

    use super::*;
    use rstest::rstest;
    use std::iter::zip;

    #[rstest]
    #[case::single_generator(1, 0, vec![0, 1, 2])]
    #[case::first_of_two_generators(2, 0, vec![0, 2, 4])]
    #[case::second_of_two_generators(2, 1, vec![1, 3, 5])]
    #[case::third_of_five_generators(5, 2, vec![2, 7, 12])]
    fn test_next_event_number(
        #[case] num_event_generators: usize,
        #[case] first_event_number: usize,
        #[case] expected_next_event_numbers: Vec<u64>,
    ) {
        let config = Config {
            options: GeneratorOptions {
                num_event_generators,
                ..GeneratorOptions::default()
            },
            first_event_number,
            ..Config::default()
        };
        for (event_num, expected) in zip(
            0..expected_next_event_numbers.len(),
            expected_next_event_numbers.into_iter(),
        ) {
            assert_eq!(config.next_event_number(event_num as u64), expected);
        }
    }

    #[rstest]
    #[case(0, 0)]
    #[case(1, 1)]
    #[case(2, 2)]
    #[case(199, 199)]
    fn test_next_adjusted_event_number_single_generator(
        #[case] num_events: u64,
        #[case] expected: u64,
    ) {
        assert_eq!(
            Config::new(
                GeneratorOptions {
                    num_event_generators: 1,
                    ..GeneratorOptions::default()
                },
                0,
                0
            )
            .next_event_number(num_events),
            expected
        );
    }

    #[rstest]
    #[case(0, 0)]
    #[case(1, 2)]
    #[case(2, 4)]
    #[case(199, 398)]
    fn test_next_adjusted_event_number_default(#[case] num_events: u64, #[case] expected: u64) {
        // The default config has 2 generators, so the 0th generator emits
        // events 0, 2, 4 etc.
        assert_eq!(Config::default().next_event_number(num_events), expected);
    }

    // The default event interval is 10 ms.
    #[rstest]
    #[case(1, 10)]
    #[case(2, 20)]
    #[case(5, 50)]
    fn test_timestamp_for_event_single_generator(#[case] event_number: u64, #[case] expected: u64) {
        assert_eq!(
            Config::default().timestamp_for_event(event_number),
            expected,
        );
    }
}
