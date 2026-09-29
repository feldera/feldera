//! Prometheus metrics for the NATS input connector.
//!
//! These exist so that a reconnect storm is visible from the pipeline side:
//! a rising `consumers_created_total` with a flat `records` count is the
//! signature of the connector repeatedly recreating consumers on a struggling
//! cluster.

use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
use std::sync::atomic::{AtomicU64, Ordering};

/// Retry state reported by [`NatsInputMetrics::retry_state`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub(super) enum RetryState {
    /// Reading, paused, or not started: no retry in progress.
    Healthy = 0,
    /// Waiting for the next retry attempt.
    Retrying = 1,
    /// Stopped after a fatal error (including retry exhaustion).
    Stopped = 2,
}

#[derive(Debug, Default)]
pub(super) struct NatsInputMetrics {
    consumers_created: AtomicU64,
    consumers_deleted: AtomicU64,
    retries: AtomicU64,
    consecutive_failures: AtomicU64,
    retry_state: AtomicU64,
}

impl NatsInputMetrics {
    pub(super) fn consumer_created(&self) {
        self.consumers_created.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn consumer_deleted(&self) {
        self.consumers_deleted.fetch_add(1, Ordering::Relaxed);
    }

    /// Records one retry attempt and the number of consecutive failures so far.
    pub(super) fn retry_attempt(&self, consecutive_failures: u32) {
        self.retries.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures
            .store(u64::from(consecutive_failures), Ordering::Relaxed);
    }

    pub(super) fn set_retry_state(&self, state: RetryState) {
        self.retry_state.store(state as u64, Ordering::Relaxed);
        if state == RetryState::Healthy {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    pub(super) fn snapshot(&self) -> NatsInputMetricsSnapshot {
        NatsInputMetricsSnapshot {
            consumers_created: self.consumers_created.load(Ordering::Relaxed),
            consumers_deleted: self.consumers_deleted.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            consecutive_failures: self.consecutive_failures.load(Ordering::Relaxed),
            retry_state: self.retry_state.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct NatsInputMetricsSnapshot {
    pub consumers_created: u64,
    pub consumers_deleted: u64,
    pub retries: u64,
    pub consecutive_failures: u64,
    pub retry_state: u64,
}

impl ConnectorMetrics for NatsInputMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        vec![
            (
                "input_connector_nats_consumers_created_total",
                "Number of JetStream consumers this connector has created (initial start, resume, replay, and every retry).",
                ValueType::Counter,
                self.consumers_created.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_nats_consumers_deleted_total",
                "Number of JetStream consumers this connector has explicitly deleted on pause, stop, error, or replay completion.",
                ValueType::Counter,
                self.consumers_deleted.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_nats_retries_total",
                "Number of reconnect attempts made while in retry mode.",
                ValueType::Counter,
                self.retries.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_nats_consecutive_failures",
                "Consecutive failed reconnect attempts in the current retry episode (0 when healthy).",
                ValueType::Gauge,
                self.consecutive_failures.load(Ordering::Relaxed) as f64,
            ),
            (
                "input_connector_nats_retry_state",
                "Retry state: 0=healthy/paused, 1=retrying, 2=stopped after a fatal error.",
                ValueType::Gauge,
                self.retry_state.load(Ordering::Relaxed) as f64,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::{NatsInputMetrics, RetryState};
    use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};

    #[test]
    fn counters_and_gauges_track_events() {
        let metrics = NatsInputMetrics::default();
        metrics.consumer_created();
        metrics.consumer_created();
        metrics.consumer_deleted();
        metrics.set_retry_state(RetryState::Retrying);
        metrics.retry_attempt(3);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.consumers_created, 2);
        assert_eq!(snapshot.consumers_deleted, 1);
        assert_eq!(snapshot.retries, 1);
        assert_eq!(snapshot.consecutive_failures, 3);
        assert_eq!(snapshot.retry_state, RetryState::Retrying as u64);

        metrics.set_retry_state(RetryState::Healthy);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.consecutive_failures, 0);
        assert_eq!(snapshot.retry_state, RetryState::Healthy as u64);
    }

    #[test]
    fn exported_metric_names_are_prometheus_safe() {
        let metrics = NatsInputMetrics::default();
        let exported = metrics.metrics();
        assert_eq!(exported.len(), 5);
        for (name, help, value_type, _) in exported {
            assert!(
                name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "{name} is not a valid Prometheus metric name"
            );
            assert!(name.starts_with("input_connector_nats_"));
            assert!(!help.is_empty());
            if name.ends_with("_total") {
                assert!(matches!(value_type, ValueType::Counter), "{name}");
            } else {
                assert!(matches!(value_type, ValueType::Gauge), "{name}");
            }
        }
    }
}
