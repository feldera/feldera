//! Metrics shared by the two Kafka sinks.

use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Metrics of the sink, exported per endpoint through the pipeline's
/// `/metrics` endpoint.
///
/// The queue gauges and the broker counters come from the librdkafka
/// statistics callback, so they refresh every `statistics.interval.ms`. The
/// delivery counters and the wait counter are live.
#[derive(Default)]
pub(crate) struct KafkaOutputMetrics {
    /// Messages in the producer queue.
    pub(crate) queue_messages: AtomicU64,
    /// Bytes in the producer queue.
    pub(crate) queue_bytes: AtomicU64,
    /// `queue.buffering.max.messages`.
    pub(crate) queue_max_messages: AtomicU64,
    /// `queue.buffering.max.kbytes`, in bytes.
    pub(crate) queue_max_bytes: AtomicU64,
    /// Produce requests retried, summed over brokers.
    pub(crate) produce_retries: AtomicU64,
    /// Produce requests that timed out, summed over brokers.
    pub(crate) request_timeouts: AtomicU64,
    /// Messages the broker acknowledged.
    pub(crate) delivered_messages: AtomicU64,
    /// Messages librdkafka gave up on.
    pub(crate) delivery_errors: AtomicU64,
    /// Time `push_buffer` and `push_key` spent waiting on a full queue.
    pub(crate) queue_full_wait_micros: AtomicU64,
    /// Last observed native counters, retained when a broker disappears.
    broker_counters: Mutex<BTreeMap<String, (u64, u64)>>,
}

impl KafkaOutputMetrics {
    /// A `kafka_send` wait callback: adds a wait on a full producer queue to
    /// the counter as the wait grows.
    pub(crate) fn count_queue_full_wait(&self) -> impl FnMut(Duration) + '_ {
        let mut reported = 0;
        move |waited| {
            let micros = waited.as_micros() as u64;
            self.queue_full_wait_micros
                .fetch_add(micros - reported, Ordering::Relaxed);
            reported = micros;
        }
    }

    pub(crate) fn update_from_statistics(&self, statistics: &rdkafka::Statistics) {
        self.queue_messages
            .store(statistics.msg_cnt, Ordering::Relaxed);
        self.queue_bytes
            .store(statistics.msg_size, Ordering::Relaxed);
        self.queue_max_messages
            .store(statistics.msg_max, Ordering::Relaxed);
        self.queue_max_bytes
            .store(statistics.msg_size_max, Ordering::Relaxed);
        let mut previous = self.broker_counters.lock().unwrap();
        for (name, broker) in &statistics.brokers {
            let counters = (broker.txretries, broker.req_timeouts);
            let old = previous.insert(name.clone(), counters).unwrap_or_default();
            // A recreated broker can reset its native counters. Count its new
            // observations without decreasing the endpoint's totals.
            self.produce_retries.fetch_add(
                counters.0.checked_sub(old.0).unwrap_or(counters.0),
                Ordering::Relaxed,
            );
            self.request_timeouts.fetch_add(
                counters.1.checked_sub(old.1).unwrap_or(counters.1),
                Ordering::Relaxed,
            );
        }
    }
}

impl ConnectorMetrics for KafkaOutputMetrics {
    fn metrics(&self) -> Vec<(&'static str, &'static str, ValueType, f64)> {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed) as f64;
        vec![
            (
                "kafka_output_queue_messages",
                "Messages in the producer queue, waiting for the broker. Refreshed every `statistics.interval.ms`.",
                ValueType::Gauge,
                load(&self.queue_messages),
            ),
            (
                "kafka_output_queue_bytes",
                "Bytes in the producer queue, waiting for the broker. Refreshed every `statistics.interval.ms`.",
                ValueType::Gauge,
                load(&self.queue_bytes),
            ),
            (
                "kafka_output_queue_max_messages",
                "Capacity of the producer queue in messages (`queue.buffering.max.messages`). The connector blocks when the queue is full.",
                ValueType::Gauge,
                load(&self.queue_max_messages),
            ),
            (
                "kafka_output_queue_max_bytes",
                "Capacity of the producer queue in bytes (`queue.buffering.max.kbytes`). The connector blocks when the queue is full.",
                ValueType::Gauge,
                load(&self.queue_max_bytes),
            ),
            (
                "kafka_output_produce_retries_total",
                "Broker requests retried, including metadata and produce requests.",
                ValueType::Counter,
                load(&self.produce_retries),
            ),
            (
                "kafka_output_request_timeouts_total",
                "Broker requests that timed out.",
                ValueType::Counter,
                load(&self.request_timeouts),
            ),
            (
                "kafka_output_delivered_messages_total",
                "Messages the broker acknowledged.",
                ValueType::Counter,
                load(&self.delivered_messages),
            ),
            (
                "kafka_output_delivery_errors_total",
                "Messages librdkafka failed to deliver, including explicit timeout or retry limits and permanent errors.",
                ValueType::Counter,
                load(&self.delivery_errors),
            ),
            (
                "kafka_output_queue_full_wait_seconds_total",
                "Time the connector spent waiting for room in a full producer queue.",
                ValueType::Counter,
                load(&self.queue_full_wait_micros) / 1_000_000.0,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::statistics::{Broker, Statistics};

    #[test]
    fn broker_counter_totals_survive_disappearance_and_reset() {
        let metrics = KafkaOutputMetrics::default();
        let mut statistics = Statistics::default();
        statistics.brokers.insert(
            "broker".to_owned(),
            Broker {
                txretries: 7,
                req_timeouts: 3,
                ..Default::default()
            },
        );
        metrics.update_from_statistics(&statistics);
        metrics.update_from_statistics(&Statistics::default());
        metrics.update_from_statistics(&statistics);
        assert_eq!(metrics.produce_retries.load(Ordering::Relaxed), 7);
        assert_eq!(metrics.request_timeouts.load(Ordering::Relaxed), 3);
        let broker = statistics.brokers.get_mut("broker").unwrap();
        broker.txretries = 2;
        broker.req_timeouts = 1;
        metrics.update_from_statistics(&statistics);
        assert_eq!(metrics.produce_retries.load(Ordering::Relaxed), 9);
        assert_eq!(metrics.request_timeouts.load(Ordering::Relaxed), 4);
    }
}
