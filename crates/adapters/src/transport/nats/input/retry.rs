//! Retry policy for the NATS input connector.
//!
//! Every reconnect attempt costs the JetStream cluster a new TCP connection, a
//! `STREAM.INFO` request, and a `CONSUMER.CREATE` request. Creating a consumer
//! at a checkpointed `ByStartSequence` position makes the server compute the
//! consumer's pending count over the stream, which is expensive on large
//! streams. Retrying at a fixed short interval therefore turns a stalled
//! cluster into a self-sustaining consumer-creation storm: the more the
//! cluster struggles, the more consumers every connector asks it to create.
//!
//! The policy below replaces the fixed interval with capped exponential
//! backoff plus jitter, and optionally gives up after a bounded number of
//! consecutive failures so that operators, not the connector, decide when to
//! resume hammering a sick cluster.

use rand::Rng;
use std::time::Duration;

/// Capped exponential backoff with jitter and an optional attempt limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RetryPolicy {
    /// Delay before the first retry. Doubles on every consecutive failure.
    initial: Duration,
    /// Upper bound on the delay between retries.
    max: Duration,
    /// Give up after this many consecutive failed attempts. `None` retries forever.
    max_attempts: Option<u32>,
}

impl RetryPolicy {
    pub(super) fn new(initial: Duration, max: Duration, max_attempts: Option<u32>) -> Self {
        Self {
            initial,
            max: max.max(initial),
            max_attempts,
        }
    }

    /// Nominal (jitter-free) delay before retry number `attempt`, where the
    /// first retry is attempt `1`.
    ///
    /// Grows as `initial * 2^(attempt - 1)` and saturates at `max`.
    pub(super) fn nominal_delay(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1);
        self.initial
            .checked_mul(1u32.checked_shl(exponent).unwrap_or(u32::MAX))
            .unwrap_or(self.max)
            .min(self.max)
    }

    /// Delay before retry number `attempt`: the nominal delay lengthened by a
    /// random amount of up to a quarter, without exceeding `max`.
    ///
    /// Jitter only ever lengthens the delay, so the configured initial
    /// interval is a true minimum and `max` a true maximum. Connectors that
    /// failed at the same moment (for example, every connector in a pipeline
    /// after a NATS node restart) drift apart because the random extra
    /// accumulates over successive retries, even once the delay reaches `max`.
    pub(super) fn delay(&self, attempt: u32) -> Duration {
        let nominal = self.nominal_delay(attempt);
        let ceiling = nominal.saturating_add(nominal / 4).min(self.max);
        if ceiling <= nominal {
            return nominal;
        }
        rand::thread_rng().gen_range(nominal..=ceiling)
    }

    /// Whether `attempt` consecutive failures exceed the configured limit.
    pub(super) fn exhausted(&self, attempt: u32) -> bool {
        self.max_attempts.is_some_and(|limit| attempt > limit)
    }

    pub(super) fn max_attempts(&self) -> Option<u32> {
        self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::RetryPolicy;
    use std::time::Duration;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn nominal_delay_doubles_then_caps() {
        let policy = RetryPolicy::new(secs(5), secs(300), None);
        assert_eq!(policy.nominal_delay(0), secs(5));
        assert_eq!(policy.nominal_delay(1), secs(5));
        assert_eq!(policy.nominal_delay(2), secs(10));
        assert_eq!(policy.nominal_delay(3), secs(20));
        assert_eq!(policy.nominal_delay(7), secs(300));
        assert_eq!(policy.nominal_delay(8), secs(300));
    }

    #[test]
    fn nominal_delay_saturates_on_huge_attempt_counts() {
        let policy = RetryPolicy::new(secs(5), secs(300), None);
        assert_eq!(policy.nominal_delay(u32::MAX), secs(300));
        assert_eq!(policy.nominal_delay(64), secs(300));
    }

    #[test]
    fn max_below_initial_is_raised_to_initial() {
        let policy = RetryPolicy::new(secs(10), secs(1), None);
        assert_eq!(policy.nominal_delay(1), secs(10));
        assert_eq!(policy.nominal_delay(5), secs(10));
    }

    #[test]
    fn jittered_delay_never_shortens_nominal_nor_exceeds_max() {
        let policy = RetryPolicy::new(secs(4), secs(64), None);
        for attempt in 1..=10 {
            let nominal = policy.nominal_delay(attempt);
            for _ in 0..200 {
                let delay = policy.delay(attempt);
                assert!(
                    delay >= nominal,
                    "attempt {attempt}: {delay:?} < {nominal:?}"
                );
                assert!(
                    delay <= nominal + nominal / 4,
                    "attempt {attempt}: {delay:?} > 1.25 * {nominal:?}"
                );
                assert!(delay <= secs(64), "attempt {attempt}: {delay:?} > max");
            }
        }
    }

    #[test]
    fn jittered_delay_below_max_varies() {
        let policy = RetryPolicy::new(secs(4), secs(64), None);
        let delays: std::collections::HashSet<_> = (0..50).map(|_| policy.delay(1)).collect();
        assert!(
            delays.len() > 1,
            "jitter produced a constant delay: {delays:?}"
        );
    }

    #[test]
    fn jittered_delay_at_max_is_max() {
        let policy = RetryPolicy::new(secs(4), secs(64), None);
        assert_eq!(policy.delay(20), secs(64));
    }

    #[test]
    fn jittered_delay_of_zero_nominal_is_zero() {
        let policy = RetryPolicy::new(Duration::ZERO, Duration::ZERO, None);
        assert_eq!(policy.delay(1), Duration::ZERO);
    }

    #[test]
    fn exhaustion_respects_limit() {
        let unbounded = RetryPolicy::new(secs(1), secs(1), None);
        assert!(!unbounded.exhausted(u32::MAX));

        let bounded = RetryPolicy::new(secs(1), secs(1), Some(3));
        assert!(!bounded.exhausted(1));
        assert!(!bounded.exhausted(3));
        assert!(bounded.exhausted(4));
    }
}
