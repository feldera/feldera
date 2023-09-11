use super::{DeferredLogging, KafkaLogLevel};
use crate::transport::kafka::rdkafka_loglevel_from;
use crate::transport::secret_resolver::MaybeSecret;
use crate::{AsyncErrorCallback, OutputEndpoint, OutputEndpointConfig, OutputTransport};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use log::debug;
use pipeline_types::secret_ref::MaybeSecretRef;
use pipeline_types::transport::kafka::default_redpanda_server;
use rdkafka::{
    config::FromClientConfigAndContext,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    ClientConfig, ClientContext,
};
use serde::Deserialize;
use std::{borrow::Cow, collections::BTreeMap, sync::RwLock, time::Duration};
use utoipa::{
    openapi::{
        schema::{KnownFormat, Schema},
        ObjectBuilder, RefOr, SchemaFormat, SchemaType,
    },
    ToSchema,
};

const OUTPUT_POLLING_INTERVAL: Duration = Duration::from_millis(100);

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

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
    fn new_endpoint(&self, config: &OutputEndpointConfig) -> AnyResult<Box<dyn OutputEndpoint>> {
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

struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    config: KafkaOutputConfig,
    parker: Parker,
    max_message_size: usize,
}

impl KafkaOutputEndpoint {
    fn new(mut config: KafkaOutputConfig) -> AnyResult<Self> {
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
            parker,
            max_message_size,
        })
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

    fn is_durable(&self) -> bool {
        false
    }
}
