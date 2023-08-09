//! Generates strings which are used for different field in other model objects.
//!
//! API based on the equivalent [Nexmark Flink StringsGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/StringsGenerator.java).

use super::NexmarkGenerator;
use dbsp::{algebra::ArcStr, arcstr_literal};
use rand::{distributions::Alphanumeric, distributions::DistString, Rng};

const MIN_STRING_LENGTH: usize = 3;

/// Return a random string of up to `max_length`.
pub(super) fn next_string<R: Rng>(rng: &mut R, max_length: usize) -> ArcStr {
    let len = rng.gen_range(MIN_STRING_LENGTH..=max_length);
    ArcStr::from(Alphanumeric.sample_string(rng, len))
}

/// Return a random string such that the current_size + string length is on
/// average the desired average size.
fn next_extra<R: Rng>(rng: &mut R, current_size: usize, desired_average_size: usize) -> ArcStr {
    if current_size > desired_average_size {
        return arcstr_literal!("");
    }

    let avg_extra_size = desired_average_size - current_size;
    let delta = (avg_extra_size as f32 * 0.2).round() as usize;
    if delta == 0 {
        return arcstr_literal!("");
    }

    let desired_size =
        rng.gen_range((avg_extra_size.saturating_sub(delta))..=(avg_extra_size + delta));
    ArcStr::from(Alphanumeric.sample_string(rng, desired_size))
}

impl<R: Rng> NexmarkGenerator<R> {
    /// Return a random string of up to `max_length`.
    ///
    /// Note: The original java implementation selects from lower-case letters
    /// only adds a special spacer char with a 1 in 13 chance (' ' by default)
    /// If both are necessary, we can update to a less optimized version, but
    /// otherwise it's simpler to use the Alphanumeric distribution.
    pub fn next_string(&mut self, max_length: usize) -> ArcStr {
        next_string(&mut self.rng, max_length)
    }

    /// Return a random string such that the current_size + string length is on
    /// average the desired average size.
    pub fn next_extra(&mut self, current_size: usize, desired_average_size: usize) -> ArcStr {
        next_extra(&mut self.rng, current_size, desired_average_size)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::make_test_generator;
    use rstest::rstest;

    #[test]
    fn next_string_length() {
        let mut ng = make_test_generator();

        let s = ng.next_string(5);

        assert_eq!(s, "AAA");
    }

    #[rstest]
    // Difference of 100, delta of 20 (0.2 * 100), so gen_range(80..=120)
    #[case::current_significantly_smaller_than_desired(100, 200, 80)]
    // Difference of 9, delta of 2 (0.2 * 9 = 1.8) so gen_range(8..=12)
    #[case::current_significantly_smaller_than_desired_round_up(6, 15, 7)]
    // Difference of 7, delta of 1 (0.2 * 7 = 1.4) so gen_range(6..=8)
    #[case::current_significantly_smaller_than_desired_round_down(8, 15, 6)]
    #[case::current_marginally_smaller_than_desired(14, 15, 0)]
    #[case::current_bigger_than_desired(20, 15, 0)]
    #[case::current_is_equal_to_desired(15, 15, 0)]
    #[case::current_greater_than_desired(20, 15, 0)]
    fn test_next_extra(
        #[case] current_size: usize,
        #[case] desired_average_size: usize,
        #[case] expected_size: usize,
    ) {
        let mut ng = make_test_generator();

        let s = ng.next_extra(current_size, desired_average_size);

        assert_eq!(s.len(), expected_size);
    }
}
