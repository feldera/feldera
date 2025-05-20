use crate::transport::kafka::{
    build_headers, generate_oauthbearer_token, kafka_send, validate_aws_msk_region,
    MemoryUseReporter,
};
use crate::{
    transport::{kafka::DeferredLogging, Step},
    AsyncErrorCallback, OutputEndpoint,
};
use anyhow::{anyhow, bail, Context, Error as AnyError, Result as AnyResult};
use feldera_types::transport::kafka::KafkaOutputConfig;
use rdkafka::client::OAuthToken;
use rdkafka::message::OwnedHeaders;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::BaseConsumer,
    error::KafkaError,
    producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer},
    types::RDKafkaErrorCode,
    ClientConfig, ClientContext, Message,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Mutex;
use std::{cmp::max, sync::RwLock, time::Duration};
use tracing::span::EnteredSpan;
use tracing::{debug, info, info_span, warn};

use super::{count_partitions_in_topic, CommonConfig, Ctp, DataConsumerContext};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_000_000;

/// Max metadata overhead added by Kafka to each message.  Useful payload size
/// plus this overhead must not exceed `message.max.bytes`.
// This value was established empirically.
const MAX_MESSAGE_OVERHEAD: usize = 64;

/// State of the `KafkaOutputEndpoint`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum State {
    /// Just created.
    New,

    /// `connect` has been called.
    Connected,

    /// `batch_start_step()` has been called.  The next call to `push_buffer()`
    /// will write at position `.0`.
    BatchOpen(OutputPosition),

    /// `batch_end` has been called for step `.0`.
    BatchClosed(Step),
}

/// A position in the output partition.
///
/// This is stored as the Kafka message key.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct OutputPosition {
    /// The step number.
    step: Step,

    /// An index within the step.  The first message output in a step has
    /// substep 0, the second has substep 1, and so on.
    ///
    /// We don't have an a priori need to store the substep number in the Kafka
    /// message key, but the substep number allows us to have a unique key for
    /// every message.  That is valuable because Kafka can be configured to
    /// deduplicate messages based on key and we do not want to lose data in
    /// that case.
    substep: u64,
}

impl OutputPosition {
    fn from_message<M>(msg: &M) -> AnyResult<OutputPosition>
    where
        M: Message,
    {
        Ok(serde_json::from_slice(msg.key().unwrap_or(&[]))?)
    }
}

pub struct KafkaOutputEndpoint {
    kafka_producer: ThreadedProducer<DataProducerContext>,
    topic: String,
    headers: OwnedHeaders,
    next_partition: usize,
    n_partitions: usize,
    max_message_size: usize,
    next_step: Step,
    state: State,
}

pub fn span(topic: &str) -> EnteredSpan {
    info_span!("kafka_output", ft = true, topic = String::from(topic)).entered()
}

impl KafkaOutputEndpoint {
    pub fn new(config: KafkaOutputConfig) -> AnyResult<Self> {
        let _guard = span(&config.topic);
        let ft = config.clone().fault_tolerance.unwrap_or_default();
        let mut common = CommonConfig::new(
            &config.kafka_options,
            &ft.consumer_options,
            &ft.producer_options,
            config.log_level,
        )?;
        common
            .producer_config
            .set("transactional.id", &config.topic);

        let message_max_bytes = common
            .producer_config
            .get("message.max.bytes")
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_MESSAGE_SIZE);
        if message_max_bytes <= MAX_MESSAGE_OVERHEAD {
            bail!("Invalid setting 'message.max.bytes={message_max_bytes}'. 'message.max.bytes' must be greater than {MAX_MESSAGE_OVERHEAD}");
        }

        let max_message_size = message_max_bytes - MAX_MESSAGE_OVERHEAD;
        debug!("Configured max message size: {max_message_size} ('message.max.bytes={message_max_bytes}')");

        // Initialize our producer.
        //
        // This makes first contact with the broker and gives up after a
        // timeout.  After this, Kafka will retry indefinitely, but limiting the
        // time for initialization is useful to make sure that the configuration
        // is correct.
        //
        // Since we initialize transactions, this has the effect of achieving
        // mutual exclusion with other instances of ourselves and any other
        // producers cooperating with us by using the same `transactional.id`.
        let context = DataProducerContext::new(&config)?;
        let kafka_producer =
            ThreadedProducer::from_config_and_context(&common.producer_config, context)?;
        kafka_producer
            .context()
            .deferred_logging
            .with_deferred_logging(|| {
                kafka_producer.init_transactions(Duration::from_secs(
                    config.initialization_timeout_secs.into(),
                ))
            })?;

        // Read the number of partitions and the next step number.  We do this
        // after initializing transactions to avoid a race.
        let (n_partitions, next_step) =
            Self::read_next_step(&common.seekable_consumer_config, &config)?;

        Ok(Self {
            kafka_producer,
            topic: config.topic.clone(),
            headers: build_headers(&config.headers),
            n_partitions,
            next_partition: 0,
            max_message_size,
            next_step,
            state: State::New,
        })
    }

    /// Reads the tail of `topic` using `seekable_consumer_config`. Returns the
    /// number of partitions in `topic` and the step number for the next step to
    /// be written.
    fn read_next_step(
        seekable_consumer_config: &ClientConfig,
        kafka_config: &KafkaOutputConfig,
    ) -> AnyResult<(usize, Step)> {
        let topic = &kafka_config.topic;
        let context = DataConsumerContext::new(|error| warn!("{error}"), kafka_config)?;
        let consumer = BaseConsumer::from_config_and_context(seekable_consumer_config, context)?;
        let n_partitions = count_partitions_in_topic(&consumer, topic)?;
        let mut next_step = 0;
        for partition in 0..n_partitions {
            let ctp = Ctp::new(&consumer, topic, partition as i32);
            let watermarks = ctp.fetch_watermarks(None)?;
            if !watermarks.is_empty() {
                if let Some(msg) = ctp.read_last_message(&watermarks)? {
                    let key = OutputPosition::from_message(&msg).with_context(|| {
                        format!(
                            "message at offset {} in {ctp} should have step and substep as key",
                            msg.offset()
                        )
                    })?;
                    next_step = max(next_step, key.step + 1);
                }
            } else if watermarks != (0..0) {
                // The partition is empty, but it has nonzero watermarks:
                //
                // - If it once had some content, which is now all deleted or expired, we can't
                //   continue because we need to know about at least the most recent step.
                //
                // - Maybe it has always been empty of real content, but a producer once started
                //   a transaction and either aborted it or didn't write anything, and then the
                //   segment was compacted.  We could have a heuristic for that by checking for
                //   a relatively small `high` value, e.g. <1000.
                //
                // For now, just warn.
                warn!(
                    "{ctp} is empty but has nonzero high watermark {}",
                    watermarks.end
                );
            };
        }
        Ok((n_partitions, next_step))
    }
}

impl OutputEndpoint for KafkaOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        debug_assert_eq!(self.state, State::New);
        let _guard = span(&self.topic);
        self.state = State::Connected;

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
        let _guard = span(&self.topic);
        let State::BatchOpen(OutputPosition { step, substep }) = self.state else {
            unreachable!(
                "state should be BatchOpen (not {:?}) in `push_buffer()`",
                self.state
            )
        };
        self.state = State::BatchOpen(OutputPosition {
            step,
            substep: substep + 1,
        });

        if step >= self.next_step {
            let key = OutputPosition { step, substep };
            let key = serde_json::to_string(&key).unwrap();
            let record = BaseRecord::to(&self.topic)
                .key(&key)
                .partition(self.next_partition as i32)
                .payload(buffer)
                .headers(self.headers.clone());
            kafka_send(&self.kafka_producer, &self.topic, record)?;

            self.next_partition += 1;
            if self.next_partition >= self.n_partitions {
                self.next_partition = 0;
            }
        }
        Ok(())
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        todo!()
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        let _guard = span(&self.topic);
        let State::BatchOpen(position) = self.state else {
            unreachable!(
                "state should be BatchOpen (not {:?}) in `batch_end()`",
                self.state
            )
        };
        self.state = State::BatchClosed(position.step);

        if position.step >= self.next_step {
            self.kafka_producer.commit_transaction(None)?;
            self.next_step = position.step + 1;
        }
        Ok(())
    }

    fn batch_start(&mut self, step: Step) -> AnyResult<()> {
        let _guard = span(&self.topic);
        let first_step = match self.state {
            State::New => unreachable!("connect() should be called first"),
            State::Connected => true,
            State::BatchClosed(closed_step) => {
                if step <= closed_step {
                    unreachable!(
                        "step numbers should increase, not go from {closed_step} to {step}"
                    );
                };
                false
            }
            State::BatchOpen(_) => {
                unreachable!("batch_end() should be called before next batch_start_step()")
            }
        };

        if step >= self.next_step {
            if step > self.next_step {
                warn!("skipping from step {} to {step}", self.next_step);
            }
            self.kafka_producer.begin_transaction()?;
        } else if first_step {
            info!(
                "dropping steps {step}..{} that were already output in a previous run",
                self.next_step
            );
        }
        self.state = State::BatchOpen(OutputPosition { step, substep: 0 });
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

struct DataProducerContext {
    /// Callback to notify the controller about delivery failure.
    async_error_callback: RwLock<Option<AsyncErrorCallback>>,

    deferred_logging: DeferredLogging,

    oauthbearer_config: HashMap<String, String>,

    memory_use_reporter: Mutex<MemoryUseReporter>,

    topic: String,
}

impl DataProducerContext {
    fn new(kafka_config: &KafkaOutputConfig) -> AnyResult<Self> {
        let mut oauthbearer_config = HashMap::new();
        if let Some(region) =
            validate_aws_msk_region(&kafka_config.kafka_options, kafka_config.region.clone())?
        {
            oauthbearer_config.insert("region".to_owned(), region);
        };

        Ok(Self {
            async_error_callback: RwLock::new(None),
            deferred_logging: DeferredLogging::new(),
            oauthbearer_config,
            topic: kafka_config.topic.clone(),
            memory_use_reporter: Mutex::new(MemoryUseReporter::new()),
        })
    }
}

impl ClientContext for DataProducerContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        if let Some(cb) = self.async_error_callback.read().unwrap().as_ref() {
            let fatal = error
                .rdkafka_error_code()
                .is_some_and(|code| code == RDKafkaErrorCode::Fatal);
            cb(fatal, anyhow!(reason.to_string()));
        } else {
            warn!("{error}");
        }
    }

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        generate_oauthbearer_token(&self.oauthbearer_config)
    }

    fn stats(&self, statistics: rdkafka::Statistics) {
        let _guard = span(&self.topic);
        self.memory_use_reporter.lock().unwrap().update(&statistics);
    }
}

impl ProducerContext for DataProducerContext {
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
