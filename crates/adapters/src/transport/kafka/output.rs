use super::{default_redpanda_server, KafkaLogLevel};
use crate::{AsyncErrorCallback, OutputEndpoint, OutputEndpointConfig, OutputTransport};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::{
    queue::ArrayQueue,
    sync::{Parker, Unparker},
};
use log::{debug, error};
use rdkafka::{
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    ClientConfig, ClientContext, Statistics,
};
use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    sync::RwLock,
    time::{Duration, Instant},
};
use utoipa::{
    openapi::{
        schema::{KnownFormat, Schema},
        ObjectBuilder, RefOr, SchemaFormat, SchemaType,
    },
    ToSchema,
};

const OUTPUT_POLLING_INTERVAL: Duration = Duration::from_millis(100);

const ERROR_BUFFER_SIZE: usize = 1000;

/// [`OutputTransport`] implementation that writes data to a Kafka topic.
///
/// This output transport is only available if the crate is configured with
/// `with-kafka` feature.
///
/// The output transport factory gives this transport the name `kafka`.
pub struct KafkaOutputTransport;

impl OutputTransport for KafkaOutputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("kafka")
    }

    /// Creates a new [`OutputEndpoint`] fpor writing to a Kafka topic,
    /// interpreting `config` as a [`KafkaOutputConfig`].
    ///
    /// See [`OutputTransport::new_endpoint()`] for more information.
    fn new_endpoint(
        &self,
        _name: &str,
        config: &OutputEndpointConfig,
    ) -> AnyResult<Box<dyn OutputEndpoint>> {
        let config = KafkaOutputConfig::deserialize(&config.connector_config.transport.config)?;
        let ep = KafkaOutputEndpoint::new(config)?;

        Ok(Box::new(ep))
    }
}

const fn default_max_inflight_messages() -> u32 {
    1000
}

const fn default_initialization_timeout_secs() -> u32 {
    10
}

/// Configuration for writing data to a Kafka topic with [`OutputTransport`].
#[derive(Deserialize, Debug)]
pub struct KafkaOutputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka producer.
    #[serde(flatten)]
    pub kafka_options: BTreeMap<String, String>,

    /// Topic to write to.
    pub topic: String,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    pub log_level: Option<KafkaLogLevel>,

    /// Maximum number of unacknowledged messages buffered by the Kafka
    /// producer.
    ///
    /// Kafka producer buffers outgoing messages until it receives an
    /// acknowledgement from the broker.  This configuration parameter
    /// bounds the number of unacknowledged messages.  When the number of
    /// unacknowledged messages reaches this limit, sending of a new message
    /// blocks until additional acknowledgements arrive from the broker.
    ///
    /// Defaults to 1000.
    #[serde(default = "default_max_inflight_messages")]
    pub max_inflight_messages: u32,

    /// Maximum timeout in seconds to wait for the endpoint to connect to
    /// a Kafka broker.
    ///
    /// Defaults to 10.
    #[serde(default = "default_initialization_timeout_secs")]
    pub initialization_timeout_secs: u32,
}

impl KafkaOutputConfig {
    /// Set `option` to `val`, if missing.
    fn set_option_if_missing(&mut self, option: &str, val: &str) {
        self.kafka_options
            .entry(option.to_string())
            .or_insert_with(|| val.to_string());
    }

    /// Validate configuration, set default option values required by this
    /// adapter.
    fn validate(&mut self) -> AnyResult<()> {
        self.set_option_if_missing("bootstrap.servers", &default_redpanda_server());
        Ok(())
    }
}

// The auto-derived implementation gets confused by the flattened
// `kafka_options` field.
impl<'s> ToSchema<'s> for KafkaOutputConfig {
    fn schema() -> (&'s str, RefOr<Schema>) {
        (
            "KafkaOutputConfig",
            ObjectBuilder::new()
                .property(
                    "topic",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                )
                .required("topic")
                .property(
                    "log_level",
                    KafkaLogLevel::schema().1
                )
                .property(
                    "max_inflight_messages",
                    ObjectBuilder::new()
                        .schema_type(SchemaType::Integer)
                        .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32)))
                        .description(Some(r#"Maximum number of unacknowledged messages buffered by the Kafka producer.

Kafka producer buffers outgoing messages until it receives an
acknowledgement from the broker.  This configuration parameter
bounds the number of unacknowledged messages.  When the number of
unacknowledged messages reaches this limit, sending of a new message
blocks until additional acknowledgements arrive from the broker.

Defaults to 1000."#)),
                )
                .additional_properties(Some(
                        ObjectBuilder::new()
                        .schema_type(SchemaType::String)
                        .description(Some(r#"Options passed directly to `rdkafka`.

See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
used to configure the Kafka producer."#))))
                .into(),
        )
    }
}

/// Producer context object used to handle async delivery notifications from
/// Kafka.
struct KafkaOutputContext {
    /// Used to unpark the endpoint thread waiting for the number of in-flight
    /// messages to drop below `max_inflight_messages`.
    unparker: Unparker,

    /// Callback to notify the controller about delivery failure.
    async_error_callback: RwLock<Option<AsyncErrorCallback>>,

    /// Errors encountered during initialization, i.e., before an error
    /// callback is connected to the context.
    errors: ArrayQueue<(bool, String)>,

    /// The latest snapshot of Kafka producer statistics obtained
    /// via the `stats` callback.
    stats: RwLock<Option<Statistics>>,
}

impl KafkaOutputContext {
    fn new(unparker: Unparker) -> Self {
        Self {
            unparker,
            async_error_callback: RwLock::new(None),
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
            stats: RwLock::new(None),
        }
    }

    fn push_error(&self, fatal: bool, reason: &str) {
        // `force_push` makes the queue operate as a circular buffer.
        self.errors.force_push((fatal, reason.to_string()));
    }

    fn pop_error(&self) -> Option<(bool, String)> {
        self.errors.pop()
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
        } else {
            self.push_error(fatal, reason);
        }
    }

    fn stats(&self, statistics: Statistics) {
        *self.stats.write().unwrap() = Some(statistics);
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

struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    config: KafkaOutputConfig,
    parker: Parker,
}

impl KafkaOutputEndpoint {
    fn new(mut config: KafkaOutputConfig) -> AnyResult<Self> {
        // Create Kafka producer configuration.
        config.validate()?;
        debug!("Starting Kafka output endpoint: {config:?}");

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, value);
        }

        // This is needed to activate the `KafkaOutputContext::stats` callback.
        // We currently only use the stats during initialization, but we
        // may want to surface it in the future as part of endpoint
        // statistics.  It doesn't seem possible to adjust this parameter at
        // runtime, so we pick a value that shouldn't slow down the initialization
        // too much while also not causing significant runtime overhead.
        client_config.set("statistics.interval.ms", "1000");

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(RDKafkaLogLevel::from(log_level));
        }

        let parker = Parker::new();

        // Context object to intercept message delivery events.
        let context = KafkaOutputContext::new(parker.unparker().clone());

        // Create Kafka producer.
        let kafka_producer = ThreadedProducer::from_config_and_context(&client_config, context)?;

        Ok(Self {
            kafka_producer,
            config,
            parker,
        })
    }

    /// Determines from producer stats whether the producer can be
    /// considered successfully initialized.
    ///
    /// This is not a well-defined notion in Kafka, but as a good-enough
    /// approximation, we check that the producer is connected to at least
    /// one broker whose status is "UP".  Together with the
    /// `KafkaOutputContext::error` callback this should at least detect
    /// invalid broker address type of issue.
    fn status_ok(stats: &Statistics) -> bool {
        stats.brokers.values().any(|broker| broker.state == "UP")
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn connect(&self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        let start = Instant::now();
        loop {
            // Treat all errors as fatal during initialization.
            if let Some((_fatal, error)) = self.kafka_producer.context().pop_error() {
                bail!(error);
            }

            if let Some(stats) = self.kafka_producer.context().stats.read().unwrap().as_ref() {
                if Self::status_ok(stats) {
                    // debug!("Kafka output endpoint initialization complete. Stats: {stats:?}");
                    break;
                }
            }

            if start.elapsed() > Duration::from_secs(self.config.initialization_timeout_secs as u64)
            {
                if let Some(stats) = self.kafka_producer.context().stats.read().unwrap().as_ref() {
                    error!("Timeout initializing Kafka producer. Producer stats: {stats:?}");
                }
                bail!(
                    "failed to initialize Kafka producer, giving up after {}s",
                    self.config.initialization_timeout_secs
                );
            }
        }
        *self
            .kafka_producer
            .context()
            .async_error_callback
            .write()
            .unwrap() = Some(async_error_callback);
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
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

        let record = <BaseRecord<(), [u8], ()>>::to(&self.config.topic).payload(buffer);
        self.kafka_producer
            .send(record)
            .map_err(|(err, _record)| err)?;
        Ok(())
    }
}
