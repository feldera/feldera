use super::KafkaLogLevel;
use crate::{OutputEndpoint, OutputTransport};
use anyhow::{Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use rdkafka::{
    config::{FromClientConfigAndContext, RDKafkaLogLevel},
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    ClientConfig, ClientContext,
};
use serde::Deserialize;
use serde_yaml::Value as YamlValue;
use std::{borrow::Cow, collections::BTreeMap};

/// `OutputTransport` implementation that writes to a Kafka topic.
pub struct KafkaOutputTransport;

impl OutputTransport for KafkaOutputTransport {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("kafka")
    }

    fn new_endpoint(
        &self,
        config: &YamlValue,
        async_error_callback: Box<dyn Fn(AnyError) + Send + Sync>,
    ) -> AnyResult<Box<dyn OutputEndpoint>> {
        let config = KafkaOutputConfig::deserialize(config)?;
        let ep = KafkaOutputEndpoint::new(config, async_error_callback)?;

        Ok(Box::new(ep))
    }
}

const fn default_max_inflight_messages() -> u32 {
    1000
}

/// Output endpoint configuration.
#[derive(Deserialize)]
pub struct KafkaOutputConfig {
    /// Options passed directly to `rdkafka`.
    ///
    /// See [`librdkafka` options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    /// used to configure the Kafka producer.
    #[serde(flatten)]
    kafka_options: BTreeMap<String, String>,

    /// Topic to write to.
    topic: String,

    /// The log level of the client.
    ///
    /// If not specified, the log level will be calculated based on the global
    /// log level of the `log` crate.
    log_level: Option<KafkaLogLevel>,

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
    max_inflight_messages: u32,
}

/// Producer context object used to handle async delivery notifications from
/// Kafka.
struct KafkaOutputContext {
    /// Used to unpark the endpoint thread waiting for the number of in-flight
    /// messages to drop below `max_inflight_messages`.
    unparker: Unparker,

    /// Callback to notify the controller about delivery failure.
    async_error_callback: Box<dyn Fn(AnyError) + Send + Sync>,
}

impl KafkaOutputContext {
    fn new(unparker: Unparker, async_error_callback: Box<dyn Fn(AnyError) + Send + Sync>) -> Self {
        Self {
            unparker,
            async_error_callback,
        }
    }
}

impl ClientContext for KafkaOutputContext {}

impl ProducerContext for KafkaOutputContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        if let Err((error, _message)) = delivery_result {
            (self.async_error_callback)(AnyError::new(error.clone()));
        }

        // There is no harm in unparking the the endpoint thread unconditionally,
        // regardless of whether it's actually parked or not.
        self.unparker.unpark();
    }
}

struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    topic: String,
    max_inflight_messages: u32,
    parker: Parker,
}

impl KafkaOutputEndpoint {
    fn new(
        config: KafkaOutputConfig,
        async_error_callback: Box<dyn Fn(AnyError) + Send + Sync>,
    ) -> AnyResult<Self> {
        // Create Kafka consumer configuration.
        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, value);
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(RDKafkaLogLevel::from(log_level));
        }

        let parker = Parker::new();

        // Context object to intercept message delivery events.
        let context = KafkaOutputContext::new(parker.unparker().clone(), async_error_callback);

        // Create Kafka producer.
        let kafka_producer = ThreadedProducer::from_config_and_context(&client_config, context)?;

        Ok(Self {
            kafka_producer,
            topic: config.topic,
            max_inflight_messages: config.max_inflight_messages,
            parker,
        })
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        // Wait for the number of unacknowledged messages to drop
        // below `max_inflight_messages`.
        while self.kafka_producer.in_flight_count() as i64 > self.max_inflight_messages as i64 {
            self.parker.park();
        }

        let record = <BaseRecord<(), [u8], ()>>::to(&self.topic).payload(buffer);
        self.kafka_producer
            .send(record)
            .map_err(|(err, _record)| err)?;
        Ok(())
    }
}
