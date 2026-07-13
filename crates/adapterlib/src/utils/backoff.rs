//! Exponential backoff with jitter, shared by connectors that retry
//! intermittent object-store and metadata failures.

use rand::Rng;
use std::cmp::min;
use std::time::Duration;

/// Calculate exponential backoff delay for retrying an operation.
///
/// Starts at 0.5s, doubles each retry, caps at 32s, plus uniform jitter up to 25% of that delay
/// (capped at the 32s ceiling) to reduce synchronized retries across connectors.
pub fn calculate_backoff_delay(retry_count: u32) -> Duration {
    let base_delay_ms: u64 = 500; // 0.5 seconds
    let max_delay_ms: u64 = 32_000; // 32 seconds
    let delay_ms = min(
        base_delay_ms.checked_shl(retry_count).unwrap_or(u64::MAX),
        max_delay_ms,
    );
    let jitter_span = (delay_ms / 4).max(1);
    let jitter_ms = rand::thread_rng().gen_range(0..jitter_span);
    Duration::from_millis(min(delay_ms + jitter_ms, max_delay_ms))
}

#[cfg(test)]
mod tests {
    use super::calculate_backoff_delay;
    use std::time::Duration;

    #[test]
    fn backoff_grows_and_caps() {
        // Delay lower bound doubles with each retry until the 32s cap.
        // Upper bound adds up to 25% jitter, itself capped at 32s.
        for (retry, min_ms) in [(0u32, 500u64), (1, 1000), (2, 2000), (6, 32000)] {
            let d = calculate_backoff_delay(retry);
            assert!(d >= Duration::from_millis(min_ms), "retry {retry}: {d:?}");
            assert!(d <= Duration::from_millis(32_000), "retry {retry}: {d:?}");
        }
    }

    #[test]
    fn backoff_saturates_on_large_retry_count() {
        // `checked_shl` overflow must not panic; it saturates to the cap.
        let d = calculate_backoff_delay(1000);
        assert!(d <= Duration::from_millis(32_000));
    }
}
