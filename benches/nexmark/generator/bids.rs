//! Generates bids for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/BidGenerator.java).
use super::strings::next_string;
use super::NexmarkGenerator;
use cached::Cached;
use rand::Rng;

pub const CHANNELS_NUMBER: usize = 10_000;

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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::generator::tests::make_test_generator;
    use rand::rngs::mock::StepRng;

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
