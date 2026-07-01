//! RabbitMQ AMQP 1.0 input connector (`fe2o3-amqp` receiver).
//!
//! Attaches a receiver to `/queues/{name}`. When an `offset` is configured the
//! queue is treated as a stream: the `rabbitmq:stream-offset-spec` filter
//! selects the start position and per-message `x-stream-offset` annotations
//! drive exactly-once checkpoint/replay. Without an offset it is an
//! at-least-once queue consumer.

use super::connection::ConnectionParams;
use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand},
};
use anyhow::{Error as AnyError, Result as AnyResult, anyhow};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use fe2o3_amqp::types::messaging::{Body, Data, Source};
use fe2o3_amqp::types::primitives::{Symbol, Value};
use fe2o3_amqp::{Delivery, Receiver, Session};
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_types::transport::rabbitmq::{RabbitmqInputConfig, RabbitmqOffsetConfig};
use feldera_types::{config::FtModel, program_schema::Relation};
use serde::{Deserialize, Serialize};
use serde_amqp::described::Described;
use serde_amqp::descriptor::Descriptor;
use serde_json::Value as JsonValue;
use std::hash::Hasher;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, trace};
use xxhash_rust::xxh3::Xxh3Default;

const STREAM_OFFSET_FILTER: &str = "rabbitmq:stream-offset-spec";

/// Checkpoint/resume metadata: stream offsets `[start, end)` (exclusive end).
#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    offsets: std::ops::Range<u64>,
}

impl Metadata {
    fn from_resume_info(resume_info: Option<JsonValue>) -> Result<Self, AnyError> {
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(Self { offsets: 0..0 }))
    }
}

struct ReaderHandle {
    cancel: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

pub struct RabbitmqInputEndpoint {
    config: Arc<RabbitmqInputConfig>,
}

impl RabbitmqInputEndpoint {
    pub fn new(config: RabbitmqInputConfig) -> AnyResult<Self> {
        config
            .validate()
            .map_err(|e| anyhow!("Invalid RabbitMQ input configuration: {e}"))?;
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for RabbitmqInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        // Only streams (offset-based) support exactly-once replay.
        self.config.is_stream().then_some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for RabbitmqInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let resume_info = Metadata::from_resume_info(resume_info)?;
        info!("RabbitMQ resume info: {:?}", resume_info);
        Ok(Box::new(RabbitmqReader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
            &schema.name.name(),
        )?))
    }
}

struct RabbitmqReader {
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl RabbitmqReader {
    fn new(
        config: Arc<RabbitmqInputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        table_name: &str,
    ) -> AnyResult<Self> {
        let span = info_span!(
            "rabbitmq_input",
            table = %table_name,
            host = %config.host,
            queue = %config.queue,
        );
        let (command_sender, command_receiver) = unbounded_channel();
        TOKIO.spawn(
            worker_task(config, resume_info, consumer, parser, command_receiver).instrument(span),
        );
        Ok(Self { command_sender })
    }
}

impl InputReader for RabbitmqReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}

async fn worker_task(
    config: Arc<RabbitmqInputConfig>,
    resume_info: Metadata,
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    command_receiver: UnboundedReceiver<InputReaderCommand>,
) -> AnyResult<()> {
    let mut reader: Option<ReaderHandle> = None;
    let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));
    let next_offset = Arc::new(AtomicU64::new(resume_info.offsets.end));
    let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

    while let Some((metadata, ())) = command_receiver.recv_replay().await? {
        if metadata.offsets.is_empty() {
            consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
            continue;
        }
        consumer.error(
            false,
            anyhow!(
                "RabbitMQ replay not implemented for range {:?}",
                metadata.offsets
            ),
            Some("rabbitmq-input"),
        );
    }

    loop {
        let command = command_receiver.recv().await?;

        match command {
            InputReaderCommand::Replay { .. } => {
                unreachable!("replay commands must appear at the beginning")
            }
            InputReaderCommand::Extend => {
                if reader.is_some() {
                    continue;
                }
                let resume_offset = next_offset.load(Ordering::Acquire);
                reader = Some(spawn_reader(
                    config.clone(),
                    resume_offset,
                    next_offset.clone(),
                    queue.clone(),
                    parser.fork(),
                    consumer.clone(),
                ));
            }
            InputReaderCommand::Pause => {
                if let Some(h) = reader.take() {
                    h.cancel.cancel();
                    let _ = h.join.await;
                }
            }
            InputReaderCommand::Queue { .. } => {
                let (buffer_size, hasher, batches) = queue.flush_with_aux();
                let offset_range = match (batches.first(), batches.last()) {
                    (Some((_, first)), Some((_, last))) => *first..*last + 1,
                    _ => {
                        let pos = next_offset.load(Ordering::Acquire);
                        pos..pos
                    }
                };
                if buffer_size.records > 0 {
                    debug!("Queued {:?} ({offset_range:?})", buffer_size);
                }
                let metadata_json = serde_json::to_value(Metadata {
                    offsets: offset_range,
                })?;
                let timestamp = batches.last().map(|(ts, _)| *ts).unwrap_or_else(Utc::now);
                let hash = hasher.map(|h| h.finish()).unwrap_or(0);
                consumer.extended(
                    buffer_size,
                    Some(Resume::Replay {
                        hash,
                        seek: metadata_json.clone(),
                        replay: rmpv::Value::Nil,
                    }),
                    vec![Watermark::new(timestamp, Some(metadata_json))],
                );
            }
            InputReaderCommand::Disconnect => break,
        }
    }

    if let Some(h) = reader.take() {
        h.cancel.cancel();
        let _ = h.join.await;
    }
    Ok(())
}

/// Build the `rabbitmq:stream-offset-spec` filter value. On resume we always
/// start from the stored offset; otherwise we use the configured start policy.
fn offset_spec_value(offset: &RabbitmqOffsetConfig, resume_offset: u64) -> Value {
    if resume_offset > 0 {
        return Value::Ulong(resume_offset);
    }
    match offset {
        RabbitmqOffsetConfig::Policy { policy } => Value::Symbol(Symbol::from(policy.as_str())),
        RabbitmqOffsetConfig::Offset { offset } => Value::Ulong(*offset),
        RabbitmqOffsetConfig::Timestamp { timestamp } => {
            let millis = chrono::DateTime::parse_from_rfc3339(timestamp)
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0);
            Value::Timestamp(serde_amqp::primitives::Timestamp::from_milliseconds(millis))
        }
    }
}

fn stream_offset_of(message: &fe2o3_amqp::types::messaging::Message<Body<Value>>) -> Option<u64> {
    let annotations = message.message_annotations.as_ref()?;
    let key = fe2o3_amqp::types::messaging::annotations::OwnedKey::Symbol(Symbol::from(
        "x-stream-offset",
    ));
    match annotations.0.get(&key)? {
        Value::Ulong(v) => Some(*v),
        Value::Long(v) => u64::try_from(*v).ok(),
        Value::Uint(v) => Some(*v as u64),
        Value::Int(v) => u64::try_from(*v).ok(),
        _ => None,
    }
}

fn spawn_reader(
    config: Arc<RabbitmqInputConfig>,
    resume_offset: u64,
    next_offset: Arc<AtomicU64>,
    input_queue: Arc<InputQueue<u64>>,
    mut parser: Box<dyn Parser>,
    consumer: Box<dyn InputConsumer>,
) -> ReaderHandle {
    let cancel = CancellationToken::new();
    let child = cancel.child_token();
    let join = tokio::spawn(async move {
        if let Err(e) = run_reader(
            &config,
            resume_offset,
            &next_offset,
            &input_queue,
            &mut parser,
            child,
        )
        .await
        {
            consumer.error(
                true,
                anyhow!("RabbitMQ reader failed: {e:#}"),
                Some("rabbitmq-input"),
            );
        }
    });
    ReaderHandle { cancel, join }
}

async fn run_reader(
    config: &RabbitmqInputConfig,
    resume_offset: u64,
    next_offset: &AtomicU64,
    input_queue: &InputQueue<u64>,
    parser: &mut Box<dyn Parser>,
    cancel: CancellationToken,
) -> AnyResult<()> {
    let params = ConnectionParams {
        host: &config.host,
        port: config.port,
        vhost: &config.vhost,
        username: &config.username,
        password: &config.password,
        tls: config.tls,
        tls_ca_pem: config.tls_ca_pem.as_deref(),
        container_id: format!("feldera-rabbitmq-input-{}", process::id()),
    };
    let mut connection = params.open().await?;
    let mut session = Session::begin(&mut connection)
        .await
        .map_err(|e| anyhow!("begin AMQP session: {e}"))?;

    let address = format!("/queues/{}", config.queue);
    let link_name = config
        .consumer_name
        .clone()
        .unwrap_or_else(|| format!("feldera-rabbitmq-input-{}", config.queue));

    let mut receiver = if let Some(offset) = &config.offset {
        let value = offset_spec_value(offset, resume_offset);
        let source = Source::builder()
            .address(address.as_str())
            .add_to_filter(
                Symbol::from(STREAM_OFFSET_FILTER),
                Some(Described {
                    descriptor: Descriptor::Name(Symbol::from(STREAM_OFFSET_FILTER)),
                    value,
                }),
            )
            .build();
        Receiver::builder()
            .name(link_name)
            .source(source)
            .attach(&mut session)
            .await
            .map_err(|e| anyhow!("attach stream receiver to '{}': {e}", config.queue))?
    } else {
        Receiver::attach(&mut session, link_name, address.as_str())
            .await
            .map_err(|e| anyhow!("attach receiver to '{}': {e}", config.queue))?
    };

    // Monotonic fallback offset for non-stream queues (no x-stream-offset).
    let mut fallback_offset = resume_offset;

    loop {
        select! {
            _ = cancel.cancelled() => break,
            item = receiver.recv::<Body<Value>>() => {
                let delivery: Delivery<Body<Value>> = item
                    .map_err(|e| anyhow!("receive from '{}': {e}", config.queue))?;
                let offset = stream_offset_of(delivery.message()).unwrap_or(fallback_offset);
                fallback_offset = offset + 1;
                next_offset.store(offset + 1, Ordering::Release);
                if let Body::Data(batch) = &delivery.message().body {
                    for Data(bytes) in batch.iter() {
                        input_queue.push_with_aux(
                            parser.parse(bytes.as_ref(), None),
                            Utc::now(),
                            offset,
                        );
                        trace!("RabbitMQ message at offset {offset}");
                    }
                }
                receiver
                    .accept(&delivery)
                    .await
                    .map_err(|e| anyhow!("settle delivery: {e}"))?;
            }
        }
    }

    let _ = receiver.close().await;
    let _ = session.end().await;
    let _ = connection.close().await;
    Ok(())
}
