use crate::transport::kafka::{
    DeferredLogging, OauthbearerAuth, PemToLocation, buffered_bytes, build_headers,
    generate_oauthbearer_token, kafka_send, rdkafka_loglevel_from, resolve_oauthbearer_auth,
};
use crate::util::MemoryUseReporter;
use crate::{OutputEndpoint, OutputEndpointCallbacks};
use anyhow::{Error as AnyError, Result as AnyResult, anyhow, bail};
use feldera_adapterlib::metrics::{ConnectorMetrics, ValueType};
use feldera_storage::tokio::TOKIO;
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::transport::kafka::KafkaOutputConfig;
use rdkafka::client::OAuthToken;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::{
    ClientConfig, ClientContext,
    config::FromClientConfigAndContext,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use tracing::span::EnteredSpan;
use tracing::{debug, info_span};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

/// The sink is stalled, and unhealthy, when a message has waited this long
/// for delivery and no delivery completed in the same time. Age alone is not
/// a stall: a sink limited by broker throughput can hold a large queue for
/// longer than this while it delivers.
const DELIVERY_STALL_TIMEOUT: Duration = Duration::from_secs(10);
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(100);

/// Metrics of the sink, exported per endpoint through the pipeline's
/// `/metrics` endpoint.
///
/// The queue gauges and the broker counters come from the librdkafka
/// statistics callback, so they refresh every `statistics.interval.ms`. The
/// delivery counters and the wait counter are live.
#[derive(Default)]
pub(crate) struct KafkaOutputMetrics {
    /// Messages in the producer queue.
    queue_messages: AtomicU64,
    /// Bytes in the producer queue.
    queue_bytes: AtomicU64,
    /// `queue.buffering.max.messages`.
    queue_max_messages: AtomicU64,
    /// `queue.buffering.max.kbytes`, in bytes.
    queue_max_bytes: AtomicU64,
    /// Produce requests retried, summed over brokers.
    produce_retries: AtomicU64,
    /// Produce requests that timed out, summed over brokers.
    request_timeouts: AtomicU64,
    /// Messages the broker acknowledged.
    delivered_messages: AtomicU64,
    /// Messages librdkafka gave up on.
    delivery_errors: AtomicU64,
    /// Time `push_buffer` and `push_key` spent waiting on a full queue.
    queue_full_wait_micros: AtomicU64,
    /// Last observed native counters, retained when a broker disappears.
    broker_counters: Mutex<BTreeMap<String, (u64, u64)>>,
}

impl KafkaOutputMetrics {
    fn update_from_statistics(&self, statistics: &rdkafka::Statistics) {
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

/// Pending deliveries include the record currently waiting for queue space.
/// IDs preserve enqueue order even when acknowledgements arrive out of order.
#[derive(Default)]
struct DeliveryState {
    next_id: usize,
    pending: BTreeMap<usize, Instant>,
    /// When the last delivery succeeded.
    last_delivery: Option<Instant>,
    failure: Option<String>,
    fatal_error: Option<String>,
    unhealthy: bool,
}

/// Producer context object used to handle async delivery notifications from
/// Kafka.
struct KafkaOutputContext {
    /// Channel to the controller, set by `connect`.
    callbacks: RwLock<Option<Arc<dyn OutputEndpointCallbacks>>>,

    metrics: Arc<KafkaOutputMetrics>,

    /// Also serializes health publication across send, delivery, and timer.
    delivery_state: Mutex<DeliveryState>,

    deferred_logging: DeferredLogging,

    oauthbearer_config: OauthbearerAuth,

    memory_use_reporter: Mutex<MemoryUseReporter>,

    topic: String,
}

impl KafkaOutputContext {
    fn new(kafka_config: &KafkaOutputConfig) -> AnyResult<Self> {
        let oauthbearer_config = resolve_oauthbearer_auth(
            &kafka_config.kafka_options,
            kafka_config.oauth_provider,
            kafka_config.region.clone(),
        )?;

        Ok(Self {
            oauthbearer_config,
            callbacks: RwLock::new(None),
            metrics: Arc::new(KafkaOutputMetrics::default()),
            delivery_state: Mutex::new(DeliveryState::default()),
            deferred_logging: DeferredLogging::new(),
            topic: kafka_config.topic.clone(),
            memory_use_reporter: Mutex::new(MemoryUseReporter::new("buffers", 1024 * 1024)),
        })
    }

    fn start_delivery(&self) -> usize {
        let mut state = self.delivery_state.lock().unwrap();
        let id = state.next_id;
        state.next_id = id.checked_add(1).expect("Kafka delivery ID overflow");
        state.pending.insert(id, Instant::now());
        id
    }

    fn complete_delivery(&self, id: usize, error: Option<&KafkaError>) {
        let mut state = self.delivery_state.lock().unwrap();
        state.pending.remove(&id);
        match error {
            None => {
                self.metrics
                    .delivered_messages
                    .fetch_add(1, Ordering::Relaxed);
                state.last_delivery = Some(Instant::now());
                state.failure = None;
            }
            Some(error)
                if matches!(
                    error.rdkafka_error_code(),
                    Some(RDKafkaErrorCode::PurgeQueue | RDKafkaErrorCode::PurgeInflight)
                ) =>
            {
                // Only dropping the producer purges the queue, at pipeline
                // shutdown. The queue can hold 100k records then; one error
                // callback each would flood the log and the status with the
                // one fact that the pipeline is stopping.
                self.metrics.delivery_errors.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "a record queued for topic '{}' was purged at shutdown: {error}",
                    self.topic
                );
                return;
            }
            Some(error) => {
                self.metrics.delivery_errors.fetch_add(1, Ordering::Relaxed);
                if let Some(cb) = self.callbacks.read().unwrap().as_ref() {
                    cb.error(
                        false,
                        AnyError::new(error.clone()),
                        Some("kafka_nonft_delivery"),
                    );
                }
                state.failure = Some(format!(
                    "delivery to topic '{}' failed: {error}; check broker availability, message limits, and delivery timeout settings",
                    self.topic
                ));
            }
        }
        self.update_health(&mut state, Instant::now());
    }

    fn check_health(&self) {
        self.update_health(&mut self.delivery_state.lock().unwrap(), Instant::now());
    }

    /// Keep the state lock through publication: otherwise a timer can publish
    /// an obsolete unhealthy status after a delivery has restored health.
    fn update_health(&self, state: &mut DeliveryState, now: Instant) {
        let progressing = state
            .last_delivery
            .is_some_and(|last| now.duration_since(last) < DELIVERY_STALL_TIMEOUT);
        let overdue = state.pending.first_key_value().and_then(|(_, started)| {
            let age = now.duration_since(*started);
            (age >= DELIVERY_STALL_TIMEOUT && !progressing).then(|| {
                format!(
                    "output queued for topic '{}' has waited {age:.0?} for delivery and no delivery completed in the last {DELIVERY_STALL_TIMEOUT:?}; check broker availability",
                    self.topic
                )
            })
        });
        let description = state
            .fatal_error
            .as_ref()
            .or(state.failure.as_ref())
            .or(overdue.as_ref());
        let unhealthy = description.is_some();
        if state.unhealthy != unhealthy
            && let Some(cb) = self.callbacks.read().unwrap().as_ref()
        {
            cb.update_connector_health(match description {
                Some(description) => ConnectorHealth::unhealthy(description),
                None => ConnectorHealth::healthy(),
            });
            state.unhealthy = unhealthy;
        }
    }
}

impl ClientContext for KafkaOutputContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if matches!(
            error.rdkafka_error_code(),
            Some(RDKafkaErrorCode::AllBrokersDown | RDKafkaErrorCode::BrokerTransportFailure)
        ) {
            // librdkafka reconnects without losing the queued messages. The
            // delivery-age monitor reports a sustained outage; an output
            // error would incorrectly imply that records were lost.
            debug!("Kafka producer reconnecting: {error}; {reason}");
            return;
        }
        let fatal = match error.rdkafka_error_code() {
            Some(code) => code == RDKafkaErrorCode::Fatal,
            _ => false,
        };

        if fatal {
            let mut state = self.delivery_state.lock().unwrap();
            state.fatal_error = Some(format!(
                "Kafka producer for topic '{}' failed: {error}; {reason}; fix the broker or configuration error and restart the pipeline",
                self.topic
            ));
            self.update_health(&mut state, Instant::now());
        }

        if let Some(cb) = self.callbacks.read().unwrap().as_ref() {
            cb.error(
                fatal,
                anyhow!("Kafka producer error: {error}; Reason: {reason}"),
                Some("kafka_nonft_producer"),
            );
        }
    }

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        generate_oauthbearer_token(&self.oauthbearer_config)
    }

    fn stats(&self, statistics: rdkafka::Statistics) {
        let _guard = span(&self.topic);
        self.metrics.update_from_statistics(&statistics);
        self.memory_use_reporter
            .lock()
            .unwrap()
            .update(buffered_bytes(&statistics));
    }
}

impl ProducerContext for KafkaOutputContext {
    type DeliveryOpaque = usize;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        self.complete_delivery(
            delivery_opaque,
            delivery_result.as_ref().err().map(|(error, _)| error),
        );
    }
}

pub struct KafkaOutputEndpoint {
    /// Abort the lightweight monitor before destroying the producer.
    _health_task: AbortOnDropHandle<()>,
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    config: KafkaOutputConfig,
    headers: OwnedHeaders,
    max_message_size: usize,
    /// `ControllerInner::shutdown_token`, which ends a wait on a full
    /// producer queue.  See [kafka_send].
    shutdown: CancellationToken,
}

fn span(topic: &str) -> EnteredSpan {
    info_span!("kafka_output", ft = false, topic = topic).entered()
}

impl KafkaOutputEndpoint {
    pub fn new(
        mut config: KafkaOutputConfig,
        endpoint_name: &str,
        shutdown: CancellationToken,
    ) -> AnyResult<Self> {
        let _guard = span(&config.topic);
        // Create Kafka producer configuration.
        config.validate()?;
        debug!("Starting Kafka output endpoint: {config:?}");

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, value);
        }

        client_config.pem_to_location(endpoint_name)?;

        let headers = build_headers(&config.headers);

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Context object to intercept message delivery events.
        let context = KafkaOutputContext::new(&config)?;

        let message_max_bytes = client_config
            .get("message.max.bytes")
            .map(|s| s.parse::<usize>().unwrap_or(DEFAULT_MAX_MESSAGE_SIZE))
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        if message_max_bytes <= MAX_MESSAGE_OVERHEAD {
            bail!(
                "Invalid setting 'message.max.bytes={message_max_bytes}'. 'message.max.bytes' must be greated than {MAX_MESSAGE_OVERHEAD}"
            );
        }

        let max_message_size = message_max_bytes - MAX_MESSAGE_OVERHEAD;
        debug!(
            "Configured max message size: {max_message_size} ('message.max.bytes={message_max_bytes}')"
        );

        // Create Kafka producer.
        let kafka_producer = ThreadedProducer::from_config_and_context(&client_config, context)
            .map_err(|e| anyhow!("error creating Kafka producer: {e}"))?;

        let context = Arc::downgrade(kafka_producer.context());
        let stopping = shutdown.clone();
        let health_task = TOKIO.spawn(async move {
            let mut interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);
            loop {
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => break,
                    _ = interval.tick() => {
                        let Some(context) = context.upgrade() else { break };
                        context.check_health();
                    }
                }
            }
        });

        Ok(Self {
            _health_task: AbortOnDropHandle::new(health_task),
            kafka_producer,
            config,
            headers,
            max_message_size,
            shutdown,
        })
    }
}

impl KafkaOutputEndpoint {
    /// Enqueues `record`, waiting out a full producer queue, and reports the
    /// wait to the metrics and the health status.
    fn send<T1, T2>(&self, record: BaseRecord<T1, T2, ()>) -> AnyResult<()>
    where
        T1: rdkafka::message::ToBytes + ?Sized,
        T2: rdkafka::message::ToBytes + ?Sized,
    {
        let context = self.kafka_producer.context();
        let id = context.start_delivery();
        let record = BaseRecord {
            topic: record.topic,
            partition: record.partition,
            payload: record.payload,
            key: record.key,
            timestamp: record.timestamp,
            headers: record.headers,
            delivery_opaque: id,
        };
        let mut reported_wait = 0;
        let result = kafka_send(
            &self.kafka_producer,
            &self.config.topic,
            record,
            &self.shutdown,
            &mut |waited| {
                let micros = waited.as_micros() as u64;
                context
                    .metrics
                    .queue_full_wait_micros
                    .fetch_add(micros - reported_wait, Ordering::Relaxed);
                reported_wait = micros;
            },
        );
        if let Err(error) = &result {
            let mut state = context.delivery_state.lock().unwrap();
            state.pending.remove(&id);
            if !self.shutdown.is_cancelled() {
                state.failure = Some(format!(
                    "could not enqueue output for topic '{}': {error}; check broker configuration and message limits",
                    self.config.topic
                ));
                context.update_health(&mut state, Instant::now());
            }
        }
        result.map(|_| ())
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn connect(&mut self, callbacks: Arc<dyn OutputEndpointCallbacks>) -> AnyResult<()> {
        // Retrieve metadata for our producer.  This makes first contact with
        // the broker, which allows us to limit the time for initialization to
        // make sure that the configuration is correct.  After this, Kafka will
        // retry indefinitely.
        //
        // We don't actually care about the metadata.
        let _guard = span(&self.config.topic);
        self.kafka_producer
            .context()
            .deferred_logging
            .with_deferred_logging(|| {
                self.kafka_producer.client().fetch_metadata(
                    Some(&self.config.topic),
                    Duration::from(self.config.initialization_timeout()),
                )
            })
            .map_err(|e| {
                anyhow!(
                    "error retrieving metadata for topic '{}': {e}",
                    self.config.topic
                )
            })?;
        let context = self.kafka_producer.context();
        callbacks.set_custom_metrics(context.metrics.clone());
        *context.callbacks.write().unwrap() = Some(callbacks);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.max_message_size
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        let _guard = span(&self.config.topic);
        let record = <BaseRecord<(), [u8], ()>>::to(&self.config.topic)
            .payload(buffer)
            .headers(self.headers.clone());
        self.send(record)
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = span(&self.config.topic);
        let mut record = <BaseRecord<[u8], [u8], ()>>::to(&self.config.topic);

        if let Some(key) = key {
            record = record.key(key);
        }

        if let Some(val) = val {
            record = record.payload(val);
        }

        let mut all_headers = self.headers.clone();

        for (key, value) in headers {
            all_headers = all_headers.insert(Header { key, value: *value });
        }

        record = record.headers(all_headers);
        self.send(record)
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }

    fn memory(&self) -> usize {
        let context = self.kafka_producer.context();
        // Include the timestamps and B-tree node overhead for pending sends.
        let tracking = context.delivery_state.lock().unwrap().pending.len()
            * (std::mem::size_of::<(usize, Instant)>() + 4 * std::mem::size_of::<usize>());
        tracking + context.memory_use_reporter.lock().unwrap().current()
    }
}

#[cfg(test)]
mod tests {
    use super::super::test::{RecordingCallbacks, endpoint};
    use super::*;
    use feldera_adapterlib::test::wait;
    use feldera_types::adapter_stats::ConnectorHealthStatus;
    use rdkafka::mocking::MockCluster;
    use rdkafka::statistics::{Broker, Statistics};
    use serde_json::json;

    fn context() -> (KafkaOutputContext, Arc<RecordingCallbacks>) {
        let config = serde_json::from_value(serde_json::json!({"topic": "test"})).unwrap();
        let context = KafkaOutputContext::new(&config).unwrap();
        let callbacks = Arc::new(RecordingCallbacks::default());
        *context.callbacks.write().unwrap() = Some(callbacks.clone());
        (context, callbacks)
    }

    /// An old pending message makes the sink unhealthy only while deliveries
    /// have stopped: a slow broker that keeps acknowledging is not a stall.
    #[test]
    fn a_stall_is_an_old_pending_message_without_recent_deliveries() {
        let (context, callbacks) = context();
        let old = context.start_delivery();
        context
            .delivery_state
            .lock()
            .unwrap()
            .pending
            .insert(old, Instant::now() - DELIVERY_STALL_TIMEOUT);
        let recent = context.start_delivery();
        context.check_health();
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Unhealthy)
        ));
        // Different partitions can acknowledge out of enqueue order: a
        // delivery is progress, even with the old message still pending.
        context.complete_delivery(recent, None);
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Healthy)
        ));
        // Deliveries stop again while the old message still waits.
        context.delivery_state.lock().unwrap().last_delivery =
            Some(Instant::now() - DELIVERY_STALL_TIMEOUT);
        context.check_health();
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Unhealthy)
        ));
        context.complete_delivery(old, None);
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Healthy)
        ));
    }

    /// A lost broker connection is logged, not reported as an output error:
    /// librdkafka reconnects and the queued records are not lost.
    #[test]
    fn a_broker_disconnection_is_not_an_output_error() {
        let (context, callbacks) = context();
        context.error(
            KafkaError::Global(RDKafkaErrorCode::AllBrokersDown),
            "all brokers down",
        );
        context.error(
            KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure),
            "connection reset",
        );
        assert!(callbacks.errors().is_empty(), "{:?}", callbacks.errors());
        assert!(callbacks.health_status().is_none());
        context.error(KafkaError::Global(RDKafkaErrorCode::Fatal), "fatal");
        assert_eq!(callbacks.errors().len(), 1);
    }

    /// Records purged when the producer is dropped at shutdown count as
    /// delivery errors but raise no error callback and no health report.
    #[test]
    fn a_purge_at_shutdown_is_counted_but_not_reported() {
        let (context, callbacks) = context();
        let id = context.start_delivery();
        context.complete_delivery(
            id,
            Some(&KafkaError::MessageProduction(RDKafkaErrorCode::PurgeQueue)),
        );
        assert_eq!(context.metrics.delivery_errors.load(Ordering::Relaxed), 1);
        assert!(callbacks.errors().is_empty(), "{:?}", callbacks.errors());
        assert!(callbacks.health_status().is_none());
        assert!(context.delivery_state.lock().unwrap().pending.is_empty());
    }

    /// A record that librdkafka refuses to enqueue, here for its size, makes
    /// the sink unhealthy and leaves no pending entry behind; the next
    /// delivered record restores health.
    #[test]
    fn a_refused_record_reports_unhealthy_and_leaks_no_pending_entry() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("test_topic", 1, 1).unwrap();
        let (mut endpoint, callbacks) = endpoint(&cluster, json!({}));
        let error = endpoint
            .push_buffer(&vec![b'x'; DEFAULT_MAX_MESSAGE_SIZE + 1])
            .unwrap_err();
        assert!(error.to_string().contains("test_topic"), "{error}");
        let status = callbacks.health_history();
        let last = status.last().expect("the refusal is reported");
        assert!(matches!(last.status, ConnectorHealthStatus::Unhealthy));
        assert!(
            last.description
                .as_deref()
                .is_some_and(|d| d.contains("could not enqueue")),
            "{last:?}"
        );
        assert!(
            endpoint
                .kafka_producer
                .context()
                .delivery_state
                .lock()
                .unwrap()
                .pending
                .is_empty()
        );
        endpoint.push_buffer(b"fits").unwrap();
        wait(
            || {
                matches!(
                    callbacks.health_status(),
                    Some(ConnectorHealthStatus::Healthy)
                )
            },
            30_000,
        )
        .expect("a delivered record restores health");
    }

    #[test]
    fn a_fatal_error_cannot_be_cleared_by_delivery() {
        let (context, callbacks) = context();
        let id = context.start_delivery();
        context.error(
            KafkaError::Global(RDKafkaErrorCode::Fatal),
            "test fatal error",
        );
        context.complete_delivery(id, None);
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Unhealthy)
        ));
    }

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
    #[test]
    fn enqueue_after_delivery_timeout_does_not_restore_health() {
        let cluster = MockCluster::new(1).unwrap();
        cluster.create_topic("test_topic", 1, 1).unwrap();
        let (mut endpoint, callbacks) = endpoint(
            &cluster,
            json!({
                "queue.buffering.max.messages": "1",
                "message.timeout.ms": "1000",
                "statistics.interval.ms": "0",
            }),
        );
        cluster.broker_down(1).unwrap();
        endpoint.push_buffer(b"will expire").unwrap();
        // This push waits until the first delivery fails and frees its queue slot.
        endpoint.push_buffer(b"also expires").unwrap();
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Unhealthy)
        ));
        wait(
            || callbacks.metric("kafka_output_delivery_errors_total") == 2.0,
            15_000,
        )
        .expect("the explicit timeout must still apply");
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Unhealthy)
        ));
        cluster.broker_up(1).unwrap();
        // Use this producer's connection and metadata: probing with another
        // client does not ensure this producer has reconnected before its short
        // delivery deadline starts.
        endpoint
            .kafka_producer
            .client()
            .fetch_metadata(Some("test_topic"), Duration::from_secs(10))
            .unwrap();
        // The reconnect can still outlast the 1 s deadline; retry until one
        // record is delivered.
        wait(
            || {
                let errors = callbacks.metric("kafka_output_delivery_errors_total");
                endpoint.push_buffer(b"recovered").unwrap();
                wait(
                    || {
                        callbacks.metric("kafka_output_delivered_messages_total") > 0.0
                            || callbacks.metric("kafka_output_delivery_errors_total") > errors
                    },
                    15_000,
                )
                .unwrap();
                callbacks.metric("kafka_output_delivered_messages_total") > 0.0
            },
            90_000,
        )
        .expect("a record must be delivered after the broker is back");
        assert!(matches!(
            callbacks.health_status(),
            Some(ConnectorHealthStatus::Healthy)
        ));
    }
}
