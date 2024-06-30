use crate::transport::kafka::{build_headers, rdkafka_loglevel_from, DeferredLogging};
use crate::transport::secret_resolver::MaybeSecret;
use crate::{AsyncErrorCallback, OutputEndpoint};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use pipeline_types::secret_ref::MaybeSecretRef;
use pipeline_types::transport::kafka::KafkaOutputConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::{
    config::FromClientConfigAndContext,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    ClientConfig, ClientContext,
};
use std::{sync::RwLock, time::Duration};
use tracing::debug;

const OUTPUT_POLLING_INTERVAL: Duration = Duration::from_millis(100);

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

/// Producer context object used to handle async delivery notifications from
/// Kafka.
struct KafkaOutputContext {
    /// Used to unpark the endpoint thread waiting for the number of in-flight
    /// messages to drop below `max_inflight_messages`.
    unparker: Unparker,

    /// Callback to notify the controller about delivery failure.
    async_error_callback: RwLock<Option<AsyncErrorCallback>>,

    deferred_logging: DeferredLogging,
}

impl KafkaOutputContext {
    fn new(unparker: Unparker) -> Self {
        Self {
            unparker,
            async_error_callback: RwLock::new(None),
            deferred_logging: DeferredLogging::new(),
        }
    }
}

impl ClientContext for KafkaOutputContext {
    fn error(&self, error: KafkaError, reason: &str) {
        let fatal = match error.rdkafka_error_code() {
            Some(code) => code == RDKafkaErrorCode::Fatal,
            _ => false,
        };

        // eprintln!("Kafka error: {error}, fatal: {fatal}, reason: {reason}");

        if let Some(cb) = self.async_error_callback.read().unwrap().as_ref() {
            cb(fatal, anyhow!(reason.to_string()));
        }
    }

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }
}

impl ProducerContext for KafkaOutputContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        if let Err((error, _message)) = delivery_result {
            if let Some(cb) = self.async_error_callback.read().unwrap().as_ref() {
                cb(false, AnyError::new(error.clone()));
            }
        }

        // There is no harm in unparking the endpoint thread unconditionally,
        // regardless of whether it's actually parked or not.
        self.unparker.unpark();
    }
}

pub struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    config: KafkaOutputConfig,
    headers: OwnedHeaders,
    parker: Parker,
    max_message_size: usize,
}

impl KafkaOutputEndpoint {
    pub fn new(mut config: KafkaOutputConfig) -> AnyResult<Self> {
        // Create Kafka producer configuration.
        config.validate()?;
        debug!("Starting Kafka output endpoint: {config:?}");

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            // If it is a secret reference, resolve it to the actual secret string
            match MaybeSecret::new_using_default_directory(
                MaybeSecretRef::new_using_pattern_match(value.clone()),
            )? {
                MaybeSecret::String(simple_string) => {
                    client_config.set(key, simple_string);
                }
                MaybeSecret::Secret(secret_string) => {
                    client_config.set(key, secret_string);
                }
            }
        }

        let headers = build_headers(&config.headers);

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        let parker = Parker::new();

        // Context object to intercept message delivery events.
        let context = KafkaOutputContext::new(parker.unparker().clone());

        let message_max_bytes = client_config
            .get("message.max.bytes")
            .map(|s| s.parse::<usize>().unwrap_or(DEFAULT_MAX_MESSAGE_SIZE))
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        if message_max_bytes <= MAX_MESSAGE_OVERHEAD {
            bail!("Invalid setting 'message.max.bytes={message_max_bytes}'. 'message.max.bytes' must be greated than {MAX_MESSAGE_OVERHEAD}");
        }

        let max_message_size = message_max_bytes - MAX_MESSAGE_OVERHEAD;
        debug!("Configured max message size: {max_message_size} ('message.max.bytes={message_max_bytes}')");

        // Create Kafka producer.
        let kafka_producer = ThreadedProducer::from_config_and_context(&client_config, context)?;

        Ok(Self {
            kafka_producer,
            config,
            headers,
            parker,
            max_message_size,
        })
    }

    fn wait_for_in_flight_acks(&self) {
        // Wait for the number of unacknowledged messages to drop
        // below `max_inflight_messages`.
        while self.kafka_producer.in_flight_count() as i64
            > self.config.max_inflight_messages as i64
        {
            // FIXME: It appears that the delivery callback can be invoked before the
            // in-flight counter is decremented, in which case we may never get
            // unparked and may need to poll the in-flight counter.  This
            // shouldn't cause performance issues in practice, but
            // it would still be nice to have a more reliable way to wake up the endpoint
            // thread _after_ the in-flight counter has been decremented.
            self.parker.park_timeout(OUTPUT_POLLING_INTERVAL);
        }
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        // Retrieve metadata for our producer.  This makes first contact with
        // the broker, which allows us to limit the time for initialization to
        // make sure that the configuration is correct.  After this, Kafka will
        // retry indefinitely.
        //
        // We don't actually care about the metadata.
        self.kafka_producer
            .context()
            .deferred_logging
            .with_deferred_logging(|| {
                self.kafka_producer.client().fetch_metadata(
                    Some(&self.config.topic),
                    Duration::from_secs(self.config.initialization_timeout_secs as u64),
                )
            })?;
        *self
            .kafka_producer
            .context()
            .async_error_callback
            .write()
            .unwrap() = Some(async_error_callback);
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.max_message_size
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        self.wait_for_in_flight_acks();

        let record = <BaseRecord<(), [u8], ()>>::to(&self.config.topic)
            .payload(buffer)
            .headers(self.headers.clone());
        self.kafka_producer
            .send(record)
            .map_err(|(err, _record)| err)?;
        Ok(())
    }

    fn push_key(&mut self, key: &[u8], val: &[u8]) -> AnyResult<()> {
        self.wait_for_in_flight_acks();

        let record = <BaseRecord<[u8], [u8], ()>>::to(&self.config.topic)
            .key(key)
            .payload(val)
            .headers(self.headers.clone());
        self.kafka_producer
            .send(record)
            .map_err(|(err, _record)| err)?;
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}
