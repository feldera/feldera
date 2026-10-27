//! Tests of the non fault tolerant Kafka sink against a librdkafka mock
//! cluster, which needs no broker and can take one down and up.

use super::output::KafkaOutputEndpoint;
use crate::{OutputEndpoint, OutputEndpointCallbacks};
use anyhow::{Error as AnyError, Result as AnyResult};
use feldera_adapterlib::metrics::ConnectorMetrics;
use feldera_adapterlib::test::wait;
use feldera_types::adapter_stats::{ConnectorHealth, ConnectorHealthStatus};
use feldera_types::transport::kafka::KafkaOutputConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// An error an endpoint reported: whether it was fatal, its tag, its text.
pub(crate) type RecordedError = (bool, Option<&'static str>, String);

/// Records what the sink reports to the controller.
#[derive(Default)]
pub(crate) struct RecordingCallbacks {
    health: Mutex<Vec<ConnectorHealth>>,
    errors: Mutex<Vec<RecordedError>>,
    metrics: Mutex<Option<Arc<dyn ConnectorMetrics>>>,
}

impl RecordingCallbacks {
    pub(crate) fn health_status(&self) -> Option<ConnectorHealthStatus> {
        self.health
            .lock()
            .unwrap()
            .last()
            .map(|health| health.status.clone())
    }

    /// Every health report so far, oldest first.
    pub(crate) fn health_history(&self) -> Vec<ConnectorHealth> {
        self.health.lock().unwrap().clone()
    }

    pub(crate) fn errors(&self) -> Vec<RecordedError> {
        self.errors.lock().unwrap().clone()
    }

    pub(crate) fn metric(&self, name: &str) -> f64 {
        let metrics = self.metrics.lock().unwrap();
        let metrics = metrics.as_ref().expect("metrics registered in connect");
        metrics
            .metrics()
            .into_iter()
            .find(|(metric, ..)| *metric == name)
            .unwrap_or_else(|| panic!("no metric {name}"))
            .3
    }
}

impl OutputEndpointCallbacks for RecordingCallbacks {
    fn error(&self, fatal: bool, error: AnyError, tag: Option<&'static str>) {
        self.errors
            .lock()
            .unwrap()
            .push((fatal, tag, error.to_string()));
    }

    fn update_connector_health(&self, health: ConnectorHealth) {
        self.health.lock().unwrap().push(health);
    }

    fn set_custom_metrics(&self, metrics: Arc<dyn ConnectorMetrics>) {
        *self.metrics.lock().unwrap() = Some(metrics);
    }
}

/// Keep the producer alive through delivery assertions and release blocked
/// writers even when an assertion unwinds the test.
struct OutputWorker {
    shutdown: CancellationToken,
    release: mpsc::Sender<()>,
    handle: Option<thread::JoinHandle<AnyResult<()>>>,
}

impl Drop for OutputWorker {
    fn drop(&mut self) {
        self.shutdown.cancel();
        let _ = self.release.send(());
        let result = self.handle.take().unwrap().join();
        if !thread::panicking() {
            result
                .expect("output worker panicked")
                .expect("output worker failed");
        }
    }
}

pub(super) fn endpoint(
    cluster: &MockCluster<'_, rdkafka::producer::DefaultProducerContext>,
    options: serde_json::Value,
) -> (KafkaOutputEndpoint, Arc<RecordingCallbacks>) {
    let mut config = json!({
        "topic": "test_topic",
        "bootstrap.servers": cluster.bootstrap_servers(),
    });
    config
        .as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    let mut endpoint = KafkaOutputEndpoint::new(
        serde_json::from_value(config).unwrap(),
        "test_endpoint",
        CancellationToken::new(),
    )
    .unwrap();
    let callbacks = Arc::new(RecordingCallbacks::default());
    endpoint.connect(callbacks.clone()).unwrap();
    (endpoint, callbacks)
}

#[test]
fn output_timeout_alias_reaches_native_configuration() {
    for option in ["message.timeout.ms", "delivery.timeout.ms"] {
        for value in ["0", "30000"] {
            let mut config: KafkaOutputConfig = serde_json::from_value(json!({
                "topic": "test_topic",
                option: value,
            }))
            .unwrap();
            config.validate().unwrap();
            let mut native = ClientConfig::new();
            for (key, value) in config.kafka_options {
                native.set(key, value);
            }
            assert_eq!(
                native
                    .create_native_config()
                    .unwrap()
                    .get("message.timeout.ms")
                    .unwrap(),
                value
            );
        }
    }
}

/// A broker outage stalls the sink instead of losing messages, and the sink
/// says so.
///
/// With the broker down, the producer queue fills to its bound and the next
/// push blocks. The queue gauge shows the backlog and the connector reports
/// itself unhealthy. Once the broker is back, every message is delivered,
/// none is dropped, and the connector reports itself healthy again.
#[test]
fn a_broker_outage_stalls_the_sink_and_loses_nothing() {
    const QUEUE_BOUND: usize = 10;
    const MESSAGES: usize = 3 * QUEUE_BOUND;

    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("test_topic", 1, 1).unwrap();

    let config: KafkaOutputConfig = serde_json::from_value(json!({
        "topic": "test_topic",
        "bootstrap.servers": cluster.bootstrap_servers(),
        "queue.buffering.max.messages": QUEUE_BOUND.to_string(),
        "statistics.interval.ms": "100",
    }))
    .unwrap();
    let shutdown = CancellationToken::new();
    let mut endpoint = KafkaOutputEndpoint::new(config, "test_endpoint", shutdown.clone()).unwrap();
    let callbacks = Arc::new(RecordingCallbacks::default());
    endpoint.connect(callbacks.clone()).unwrap();

    cluster.broker_down(1).unwrap();

    // Push from a thread of its own, since the push after the queue bound
    // blocks until the broker is back.
    let pushed = Arc::new(AtomicUsize::new(0));
    let (release, released) = mpsc::channel();
    let _pusher = OutputWorker {
        shutdown,
        release,
        handle: Some({
            let pushed = pushed.clone();
            thread::spawn(move || {
                for i in 0..MESSAGES {
                    endpoint.push_buffer(format!("message {i}").as_bytes())?;
                    pushed.fetch_add(1, Ordering::Release);
                }
                let _ = released.recv();
                Ok(())
            })
        }),
    };

    // The queue fills to its bound and the sink stalls on the next message.
    // The stall is reported once the send has retried for a while.
    wait(
        || {
            callbacks
                .health_status()
                .is_some_and(|s| matches!(s, ConnectorHealthStatus::Unhealthy))
        },
        60_000,
    )
    .expect("the stalled sink must report itself unhealthy");
    assert_eq!(pushed.load(Ordering::Acquire), QUEUE_BOUND);
    assert_eq!(
        callbacks.metric("kafka_output_queue_messages"),
        QUEUE_BOUND as f64
    );
    assert_eq!(
        callbacks.metric("kafka_output_queue_max_messages"),
        QUEUE_BOUND as f64
    );
    assert_eq!(
        callbacks.metric("kafka_output_delivered_messages_total"),
        0.0
    );
    assert!(callbacks.metric("kafka_output_queue_full_wait_seconds_total") > 0.0);
    let description = callbacks
        .health
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .description
        .clone();
    assert!(
        description
            .as_deref()
            .is_some_and(|d| d.contains("queue") && d.contains("test_topic")),
        "{description:?}"
    );

    // The broker comes back: the queue drains, the pusher finishes, and every
    // message reaches the broker.
    cluster.broker_up(1).unwrap();
    wait(|| pushed.load(Ordering::Acquire) == MESSAGES, 60_000)
        .expect("the pusher must finish after broker recovery");
    assert_eq!(pushed.load(Ordering::Acquire), MESSAGES);
    wait(
        || callbacks.metric("kafka_output_delivered_messages_total") == MESSAGES as f64,
        60_000,
    )
    .expect("every message must be delivered once the broker is back");
    wait(
        || {
            callbacks
                .health_status()
                .is_some_and(|s| matches!(s, ConnectorHealthStatus::Healthy))
        },
        10_000,
    )
    .expect("the sink must report itself healthy again");
    assert_eq!(callbacks.metric("kafka_output_delivery_errors_total"), 0.0);
    assert!(callbacks.metric("kafka_output_queue_full_wait_seconds_total") > 0.0);
    assert!(callbacks.errors().is_empty(), "{:?}", callbacks.errors());
    // Let a statistics callback report the drained queue.
    wait(
        || callbacks.metric("kafka_output_queue_messages") == 0.0,
        10_000,
    )
    .expect("the queue gauge must show the drained queue");

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", "verify-output")
        .create()
        .unwrap();
    let mut partitions = TopicPartitionList::new();
    partitions
        .add_partition_offset("test_topic", 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&partitions).unwrap();
    let mut received = Vec::new();
    wait(
        || {
            if let Some(message) = consumer.poll(Duration::from_millis(100)) {
                received.push(
                    message
                        .unwrap()
                        .payload_view::<str>()
                        .unwrap()
                        .unwrap()
                        .to_owned(),
                );
            }
            received.len() == MESSAGES
        },
        60_000,
    )
    .expect("every payload must be readable from the broker");
    assert_eq!(
        received,
        (0..MESSAGES)
            .map(|i| format!("message {i}"))
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_quiet_sink_detects_stalled_delivery_without_statistics() {
    for statistics in ["0", "100"] {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("test_topic", 1, 1).unwrap();
        let (mut endpoint, callbacks) = endpoint(
            &cluster,
            json!({
                "queue.buffering.max.messages": "10",
                "statistics.interval.ms": statistics,
            }),
        );
        cluster.broker_down(1).unwrap();
        endpoint.push_buffer(b"one pending message").unwrap();
        wait(
            || {
                matches!(
                    callbacks.health_status(),
                    Some(ConnectorHealthStatus::Unhealthy)
                )
            },
            30_000,
        )
        .expect("a pending delivery must turn unhealthy without filling the queue or more pushes");
        assert_eq!(callbacks.metric("kafka_output_delivery_errors_total"), 0.0);
        cluster.broker_up(1).unwrap();
        wait(
            || callbacks.metric("kafka_output_delivered_messages_total") == 1.0,
            60_000,
        )
        .expect("the pending record must be delivered after recovery");
        wait(
            || {
                matches!(
                    callbacks.health_status(),
                    Some(ConnectorHealthStatus::Healthy)
                )
            },
            10_000,
        )
        .expect("delivery must restore health");
    }
}
