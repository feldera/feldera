use crate::transport::kafka::{
    build_headers, kafka_send, rdkafka_loglevel_from, DeferredLogging, PemToLocation,
};
use crate::{AsyncErrorCallback, OutputEndpoint};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use aws_msk_iam_sasl_signer::generate_auth_token;
use feldera_types::transport::kafka::KafkaOutputConfig;
use rdkafka::client::OAuthToken;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::{
    config::FromClientConfigAndContext,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    ClientConfig, ClientContext,
};
use std::error::Error;
use std::{sync::RwLock, time::Duration};
use tracing::span::EnteredSpan;
use tracing::{debug, info_span};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

/// Producer context object used to handle async delivery notifications from
/// Kafka.
struct KafkaOutputContext {
    /// Callback to notify the controller about delivery failure.
    async_error_callback: RwLock<Option<AsyncErrorCallback>>,

    deferred_logging: DeferredLogging,

    oauthbearer: bool,
}

impl KafkaOutputContext {
    fn new(oauthbearer: bool) -> Self {
        Self {
            oauthbearer,
            async_error_callback: RwLock::new(None),
            deferred_logging: DeferredLogging::new(),
        }
    }
}

impl ClientContext for KafkaOutputContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        let fatal = match error.rdkafka_error_code() {
            Some(code) => code == RDKafkaErrorCode::Fatal,
            _ => false,
        };

        // eprintln!("Kafka error: {error}, fatal: {fatal}, reason: {reason}");

        if let Some(cb) = self.async_error_callback.read().unwrap().as_ref() {
            cb(
                fatal,
                anyhow!("Kafka producer error: {error}; Reason: {reason}"),
            );
        }
    }

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        // TODO: Currently, OAUTHBEARER only works with AWS MSK.
        if self.oauthbearer {
            let region = {
                let region = std::env::var("AWS_REGION").ok();
                let default = std::env::var("AWS_DEFAULT_REGION").ok();
                aws_types::region::Region::new(
                    region.or(default).unwrap_or("us-east-1".to_string()),
                )
            };
            let (token, expiration_time_ms) =
                { futures::executor::block_on(async { generate_auth_token(region).await }) }?;

            return Ok(OAuthToken {
                token,
                principal_name: "".to_string(),
                lifetime_ms: expiration_time_ms,
            });
        }

        Ok(OAuthToken {
            token: "".to_string(),
            principal_name: "".to_string(),
            lifetime_ms: i64::MAX,
        })
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
    }
}

pub struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<KafkaOutputContext>,
    config: KafkaOutputConfig,
    headers: OwnedHeaders,
    max_message_size: usize,
}

fn span(config: &KafkaOutputConfig) -> EnteredSpan {
    info_span!("kafka_output", ft = false, topic = config.topic.clone()).entered()
}

impl KafkaOutputEndpoint {
    pub fn new(mut config: KafkaOutputConfig, endpoint_name: &str) -> AnyResult<Self> {
        let _guard = span(&config);
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
        let context = KafkaOutputContext::new(
            config
                .kafka_options
                .get("sasl.mechanism")
                .is_some_and(|s| s.to_uppercase() == "OAUTHBEARER"),
        );

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
        let kafka_producer = ThreadedProducer::from_config_and_context(&client_config, context)
            .map_err(|e| anyhow!("error creating Kafka producer: {e}"))?;

        Ok(Self {
            kafka_producer,
            config,
            headers,
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
        let _guard = span(&self.config);
        self.kafka_producer
            .context()
            .deferred_logging
            .with_deferred_logging(|| {
                self.kafka_producer.client().fetch_metadata(
                    Some(&self.config.topic),
                    Duration::from_secs(self.config.initialization_timeout_secs as u64),
                )
            })
            .map_err(|e| {
                anyhow!(
                    "error retrieving metadata for topic '{}': {e}",
                    &self.config.topic
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
        let _guard = span(&self.config);
        let record = <BaseRecord<(), [u8], ()>>::to(&self.config.topic)
            .payload(buffer)
            .headers(self.headers.clone());
        kafka_send(&self.kafka_producer, &self.config.topic, record)
    }

    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        let _guard = span(&self.config);
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
        kafka_send(&self.kafka_producer, &self.config.topic, record)
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test::{init_test_logger, test_circuit, TestStruct},
        Controller,
    };
    use feldera_types::config::PipelineConfig;
    use tracing::info;

    #[test]
    fn test_kafka_output_errors() {
        init_test_logger();

        info!("test_kafka_output_errors: Test invalid Kafka broker address");

        let config_str = r#"
name: test
workers: 4
inputs:
outputs:
    test_output:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                bootstrap.servers: localhost:11111
                topic: end_to_end_test_output_topic
        format:
            name: csv
"#;

        info!("test_kafka_output_errors: Creating circuit");

        info!("test_kafka_output_errors: Starting controller");
        let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();

        match Controller::with_config(
            |workers| {
                Ok(test_circuit::<TestStruct>(
                    workers,
                    &TestStruct::schema(),
                    &[None],
                ))
            },
            &config,
            Box::new(|e| panic!("error: {e}")),
        ) {
            Ok(_) => panic!("expected an error"),
            Err(e) => info!("test_kafka_output_errors: error: {e}"),
        }
    }
}
