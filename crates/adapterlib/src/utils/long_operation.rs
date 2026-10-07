//! Warnings about operations that take a long time.

use std::time::{Duration, Instant};

/// Tracks an operation that might take a long time, to warn about it at
/// increasing intervals: first after a threshold, then each time the elapsed
/// time doubles.
pub struct LongOperationWarning {
    start: Instant,
    warn_threshold: Duration,
}

impl LongOperationWarning {
    /// Starts tracking an operation, which should first be warned about after
    /// `warn_threshold`.
    pub fn new(warn_threshold: Duration) -> Self {
        Self::new_at(Instant::now(), warn_threshold)
    }

    /// Like [Self::new], for an operation that started at `start`.
    pub fn new_at(start: Instant, warn_threshold: Duration) -> Self {
        Self {
            start,
            warn_threshold,
        }
    }

    /// Calls `warn` with the elapsed time if it is time for a warning, and
    /// then doubles the time until the next warning.
    pub fn check(&mut self, warn: impl FnOnce(Duration)) {
        self.check_at(Instant::now(), warn)
    }

    /// Like [Self::check], with `now` as the current time.
    pub fn check_at(&mut self, now: Instant, warn: impl FnOnce(Duration)) {
        let elapsed = now.saturating_duration_since(self.start);
        if elapsed >= self.warn_threshold {
            warn(elapsed);
            self.warn_threshold *= 2;
        }
    }

    /// Returns the time of the next warning.
    pub fn next_warning(&self) -> Instant {
        self.start + self.warn_threshold
    }

    /// Returns the time since the operation started.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::LongOperationWarning;

    /// Each warning comes after twice as long as the one before.
    #[test]
    fn warnings_back_off() {
        let threshold = Duration::from_secs(60);
        let start = Instant::now();
        let mut operation = LongOperationWarning::new_at(start, threshold);
        let mut warnings = Vec::new();
        let mut check = |operation: &mut LongOperationWarning, at: Duration| {
            operation.check_at(start + at, |elapsed| warnings.push(elapsed));
        };

        check(&mut operation, Duration::ZERO);
        check(&mut operation, threshold - Duration::from_nanos(1));
        check(&mut operation, threshold);
        // Not again until twice the threshold.
        check(&mut operation, threshold);
        check(&mut operation, threshold * 2 - Duration::from_nanos(1));
        check(&mut operation, threshold * 2);
        assert_eq!(warnings, [threshold, threshold * 2]);
        assert_eq!(operation.next_warning(), start + threshold * 4);
    }
}
