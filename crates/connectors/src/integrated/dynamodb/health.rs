//! Health of the DynamoDB sink while its chunk writers retry.
//!
//! Every worker thread of one endpoint spawns chunk writers on the shared
//! Tokio runtime. They all report to one [`RetryHealth`], which publishes the
//! endpoint's health through a [`HealthPublisher`] and keeps the count of
//! chunks in a retry loop as a gauge.
//!
//! | Event                          | Count | Published                              |
//! |--------------------------------|-------|----------------------------------------|
//! | A chunk schedules a retry      | +1 on its first retry | unhealthy, with the cause |
//! | A chunk succeeds               | -1 if counted | healthy, if no chunk retries |
//! | A chunk gives up               | -1 if counted | unhealthy, with the failure |
//! | A chunk is dropped (shutdown)  | -1 if counted | nothing                      |
//!
//! A count of zero alone is not health: the last chunk may have given up. A
//! later success clears that failure. Publication runs under the state lock,
//! so two chunks cannot publish stale states out of order.

use std::sync::{Arc, Mutex, Weak};

use feldera_types::adapter_stats::ConnectorHealth;

use super::metrics::DynamoDBOutputMetrics;
use crate::controller::{ControllerInner, EndpointId};

/// Where [`RetryHealth`] sends the endpoint's health.
pub(crate) trait HealthPublisher: Send + Sync {
    fn publish(&self, health: ConnectorHealth);
}

/// The production publisher: the controller's health for the endpoint.
pub(crate) struct ControllerHealthPublisher {
    pub(crate) controller: Weak<ControllerInner>,
    pub(crate) endpoint_id: EndpointId,
}

impl HealthPublisher for ControllerHealthPublisher {
    fn publish(&self, health: ConnectorHealth) {
        if let Some(controller) = self.controller.upgrade() {
            controller.update_output_connector_health(self.endpoint_id, health);
        }
    }
}

/// A publisher that drops every report, for workers that run without a controller.
#[cfg(any(test, feature = "testing"))]
pub(crate) struct NoHealthPublisher;

#[cfg(any(test, feature = "testing"))]
impl HealthPublisher for NoHealthPublisher {
    fn publish(&self, _health: ConnectorHealth) {}
}

#[derive(Default)]
struct State {
    /// Chunks in a retry loop right now.
    retrying_chunks: usize,
    /// Why the last chunk gave up, until a later chunk succeeds.
    failure: Option<String>,
    /// The last report: `None` before the first one, then `Some(None)` for
    /// healthy and `Some(Some(description))` for unhealthy.
    published: Option<Option<String>>,
}

pub(crate) struct RetryHealth {
    state: Mutex<State>,
    publisher: Box<dyn HealthPublisher>,
    metrics: Arc<DynamoDBOutputMetrics>,
}

impl RetryHealth {
    pub(crate) fn new(
        publisher: Box<dyn HealthPublisher>,
        metrics: Arc<DynamoDBOutputMetrics>,
    ) -> Self {
        Self {
            state: Mutex::new(State::default()),
            publisher,
            metrics,
        }
    }

    /// The accounting of one chunk. Drop it to release the chunk's retry
    /// count without a report; call `succeeded` or `failed` to report.
    pub(crate) fn chunk(&self) -> ChunkGuard<'_> {
        ChunkGuard {
            health: self,
            counted: false,
            done: false,
        }
    }

    /// Chunks in a retry loop right now.
    #[cfg(test)]
    pub(crate) fn retrying_chunks(&self) -> usize {
        self.state.lock().unwrap().retrying_chunks
    }

    /// Publishes `health` unless it is what was last published.
    fn publish(&self, state: &mut State, health: Option<String>) {
        if state.published.as_ref() == Some(&health) {
            return;
        }
        self.publisher.publish(match &health {
            Some(description) => ConnectorHealth::unhealthy(description),
            None => ConnectorHealth::healthy(),
        });
        state.published = Some(health);
    }
}

/// One chunk's contribution to [`RetryHealth`]. See [`RetryHealth::chunk`].
pub(crate) struct ChunkGuard<'a> {
    health: &'a RetryHealth,
    counted: bool,
    done: bool,
}

impl ChunkGuard<'_> {
    /// The chunk waits for a retry because of `cause`.
    pub(crate) fn retrying(&mut self, cause: &str) {
        let mut state = self.health.state.lock().unwrap();
        if !self.counted {
            self.counted = true;
            state.retrying_chunks += 1;
            self.health
                .metrics
                .set_retrying_chunks(state.retrying_chunks as u64);
        }
        self.health.publish(&mut state, Some(cause.to_string()));
    }

    /// The chunk was written.
    pub(crate) fn succeeded(mut self) {
        self.done = true;
        let mut state = self.health.state.lock().unwrap();
        self.release(&mut state);
        state.failure = None;
        if state.retrying_chunks == 0 {
            self.health.publish(&mut state, None);
        }
    }

    /// The chunk gave up, and its items are lost.
    pub(crate) fn failed(mut self, description: &str) {
        self.done = true;
        let mut state = self.health.state.lock().unwrap();
        self.release(&mut state);
        state.failure = Some(description.to_string());
        self.health
            .publish(&mut state, Some(description.to_string()));
    }

    fn release(&mut self, state: &mut State) {
        if self.counted {
            self.counted = false;
            state.retrying_chunks -= 1;
            self.health
                .metrics
                .set_retrying_chunks(state.retrying_chunks as u64);
        }
    }
}

impl Drop for ChunkGuard<'_> {
    fn drop(&mut self) {
        // A dropped chunk (the pipeline shut down, or its task was cancelled)
        // leaves the retry loop, but its destination did not recover.
        if !self.done && self.counted {
            let mut state = self.health.state.lock().unwrap();
            self.release(&mut state);
        }
    }
}

#[cfg(any(test, feature = "testing"))]
pub(crate) mod test {
    use super::*;
    use feldera_types::adapter_stats::ConnectorHealthStatus;

    /// Records every report as `Ok(())` for healthy or `Err(description)`.
    #[derive(Clone, Default)]
    pub(crate) struct RecordingPublisher {
        pub(crate) reports: Arc<Mutex<Vec<Result<(), String>>>>,
    }

    impl HealthPublisher for RecordingPublisher {
        fn publish(&self, health: ConnectorHealth) {
            self.reports.lock().unwrap().push(match health.status {
                ConnectorHealthStatus::Healthy => Ok(()),
                ConnectorHealthStatus::Unhealthy => Err(health.description.unwrap_or_default()),
            });
        }
    }

    impl RecordingPublisher {
        pub(crate) fn reports(&self) -> Vec<Result<(), String>> {
            self.reports.lock().unwrap().clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test::RecordingPublisher;
    use super::*;
    use feldera_adapterlib::metrics::ConnectorMetrics;

    fn health() -> (RetryHealth, RecordingPublisher, Arc<DynamoDBOutputMetrics>) {
        let publisher = RecordingPublisher::default();
        let metrics = Arc::new(DynamoDBOutputMetrics::default());
        (
            RetryHealth::new(Box::new(publisher.clone()), metrics.clone()),
            publisher,
            metrics,
        )
    }

    fn gauge(metrics: &DynamoDBOutputMetrics) -> f64 {
        metrics
            .metrics()
            .into_iter()
            .find(|(name, ..)| *name == "dynamodb_output_retrying_chunks")
            .map(|(.., value)| value)
            .unwrap()
    }

    #[test]
    fn a_retry_is_unhealthy_and_a_success_is_healthy() {
        let (health, publisher, metrics) = health();
        let mut chunk = health.chunk();
        chunk.retrying("throttled");
        chunk.retrying("throttled");
        chunk.retrying("still throttled");
        assert_eq!(health.retrying_chunks(), 1);
        assert_eq!(gauge(&metrics), 1.0);
        chunk.succeeded();
        assert_eq!(health.retrying_chunks(), 0);
        assert_eq!(gauge(&metrics), 0.0);
        // Identical reports are published once.
        assert_eq!(
            publisher.reports(),
            vec![
                Err("throttled".into()),
                Err("still throttled".into()),
                Ok(())
            ]
        );
    }

    #[test]
    fn a_success_without_retries_is_healthy_once() {
        let (health, publisher, _) = health();
        health.chunk().succeeded();
        health.chunk().succeeded();
        assert_eq!(publisher.reports(), vec![Ok(())]);
    }

    #[test]
    fn a_failure_is_unhealthy_until_a_later_success() {
        let (health, publisher, _) = health();
        let mut chunk = health.chunk();
        chunk.retrying("throttled");
        chunk.failed("gave up");
        assert_eq!(health.retrying_chunks(), 0);
        assert_eq!(
            publisher.reports(),
            vec![Err("throttled".into()), Err("gave up".into())]
        );
        health.chunk().succeeded();
        assert_eq!(publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn a_first_attempt_failure_is_unhealthy_without_a_count() {
        let (health, publisher, metrics) = health();
        health.chunk().failed("not retryable");
        assert_eq!(health.retrying_chunks(), 0);
        assert_eq!(gauge(&metrics), 0.0);
        assert_eq!(publisher.reports(), vec![Err("not retryable".into())]);
    }

    #[test]
    fn a_success_cannot_hide_another_chunks_retry_loop() {
        let (health, publisher, _) = health();
        let mut slow = health.chunk();
        slow.retrying("throttled");
        health.chunk().succeeded();
        assert_eq!(publisher.reports(), vec![Err("throttled".into())]);
        slow.succeeded();
        assert_eq!(publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn a_failure_while_another_chunk_retries_stays_unhealthy_until_it_succeeds() {
        let (health, publisher, metrics) = health();
        let mut slow = health.chunk();
        slow.retrying("slow throttled");
        let mut doomed = health.chunk();
        doomed.retrying("doomed throttled");
        assert_eq!(gauge(&metrics), 2.0);
        doomed.failed("doomed gave up");
        assert_eq!(gauge(&metrics), 1.0);
        assert_eq!(
            publisher.reports().last(),
            Some(&Err("doomed gave up".into()))
        );
        slow.succeeded();
        assert_eq!(gauge(&metrics), 0.0);
        assert_eq!(publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn a_dropped_chunk_releases_its_count_without_a_report() {
        let (health, publisher, metrics) = health();
        let mut other = health.chunk();
        other.retrying("throttled");
        {
            let mut dropped = health.chunk();
            dropped.retrying("throttled too");
            assert_eq!(gauge(&metrics), 2.0);
        }
        assert_eq!(gauge(&metrics), 1.0);
        assert_eq!(health.retrying_chunks(), 1);
        assert_eq!(
            publisher.reports(),
            vec![Err("throttled".into()), Err("throttled too".into())]
        );
        drop(other);
        assert_eq!(gauge(&metrics), 0.0);
        // Shutdown does not establish that the destination recovered.
        assert_eq!(publisher.reports().len(), 2);
    }

    #[test]
    fn completion_order_under_the_lock_decides_the_final_state() {
        let (health, publisher, _) = health();
        let (mut a, mut b) = (health.chunk(), health.chunk());
        a.retrying("a");
        b.retrying("b");
        a.succeeded();
        b.failed("b gave up");
        assert_eq!(publisher.reports().last(), Some(&Err("b gave up".into())));

        let (mut a, mut b) = (health.chunk(), health.chunk());
        a.retrying("a");
        b.retrying("b");
        b.failed("b gave up");
        a.succeeded();
        assert_eq!(publisher.reports().last(), Some(&Ok(())));
    }
}
