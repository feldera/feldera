//! NATS JetStream input adapter with exactly-once fault tolerance.
//!
//! This adapter reads from a NATS JetStream using an **ordered pull consumer**,
//! which provides strict message ordering with automatic recreation on failures.
//! Combined with feldera message tracking, we achieve exactly-once semantics.
//!
//! # Ordered Pull Consumer
//!
//! We use `jetstream::consumer::pull::OrderedConfig` which provides:
//! - **Strict ordering**: Messages delivered in exact stream order
//! - **No acknowledgments**: Uses `AckPolicy::None` (tracked via sequences instead)
//! - **Automatic recreation**: On gap detection, heartbeat loss, or deletion
//! - **Ephemeral & single-replica**: Always in-memory, no durability overhead
//!
//! The ordered consumer automatically detects sequence gaps and recreates itself,
//! resuming from the last processed position. This complements our exactly-once
//! logic: we track sequences externally for checkpointing while the ordered
//! consumer ensures no gaps in the message stream.
//!
//! # Authentication
//!
//! Currently only credentials-based authentication (`.creds` files or inline strings)
//! is implemented. Additional authentication methods are defined in the configuration
//! schema but not yet implemented:
//! - TODO: JWT authentication
//! - TODO: NKey authentication
//! - TODO: Token authentication
//! - TODO: Username/password authentication
//!
//! See `config_utils::translate_connect_options` for implementation details.

mod config_utils;
#[cfg(test)]
mod test;

use crate::{
    transport::{InputQueue, InputReaderCommand},
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
};
use anyhow::{anyhow, Context, Error as AnyError, Result as AnyResult};
use async_nats::{
    self,
    jetstream::{self, consumer as nats_consumer},
};

use chrono::Utc;
use config_utils::{translate_connect_options, translate_consumer_options};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::nats::{self as cfg, NatsInputConfig},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cmp;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::{
    select,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, info_span, Instrument};
use xxhash_rust::xxh3::Xxh3Default;

type NatsConsumerConfig = nats_consumer::pull::OrderedConfig;
type NatsConsumer = nats_consumer::Consumer<NatsConsumerConfig>;

/// Checkpoint/resume metadata
///
/// The sequence_numbers is a range `[start, end)` where:
/// - `start` = first message sequence in batch
/// - `end - 1` = last message in batch
/// - `end` = next message to consume (exclusive)
///
/// - `[0, 0)`: No messages processed and no checkpoint yet, start from beginning
/// - `[6, 6)`: (empty) All messages up to #6 processed, resume from #6
/// - `[6, 10)`: Batch contained messages #6-9, resume from #10
#[derive(Debug, Serialize, Deserialize)]
struct Metadata {
    sequence_numbers: std::ops::Range<u64>,
}

impl Metadata {
    fn from_resume_info(resume_info: Option<JsonValue>) -> Result<Self, AnyError> {
        // If None JsonValue create Metadata value 0..0, meaning "start from beginning"
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(Self {
                sequence_numbers: 0..0,
            }))
    }
}

pub struct NatsInputEndpoint {
    config: Arc<NatsInputConfig>,
}

impl NatsInputEndpoint {
    pub fn new(config: NatsInputConfig) -> Result<Self, AnyError> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for NatsInputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for NatsInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let resume_info = Metadata::from_resume_info(resume_info)?;
        info!("Resume info: {:?}", resume_info);

        Ok(Box::new(NatsReader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
        )?))
    }
}

struct NatsReader {
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl NatsReader {
    fn new(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let span = info_span!("nats_input");
        let (command_sender, command_receiver) = unbounded_channel();
        let nats_connection = TOKIO
            .block_on(Self::connect_nats(&config.connection_config).instrument(span.clone()))?;

        let consumer_clone = consumer.clone();
        TOKIO.spawn(async move {
            Self::worker_task(
                config,
                resume_info,
                jetstream::new(nats_connection),
                consumer_clone,
                parser,
                command_receiver,
            )
            .instrument(span)
            .await
            .unwrap_or_else(|e| consumer.error(true, e, Some("nats-input")));
        });

        Ok(Self { command_sender })
    }

    async fn connect_nats(
        connection_config: &cfg::ConnectOptions,
    ) -> Result<async_nats::Client, AnyError> {
        let connect_options = translate_connect_options(connection_config).await?;

        let client = connect_options
            .connect(&connection_config.server_url)
            .await?;

        Ok(client)
    }

    async fn worker_task(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        jetstream: jetstream::Context,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut canceller: Option<Canceller> = None;
        let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));
        let next_sequence = Arc::new(AtomicU64::new(resume_info.sequence_numbers.end));
        let nats_consumer_config = translate_consumer_options(&config.consumer_config);

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

        // Handle replay commands
        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Attempt to replay: {:?}", metadata);
            if !metadata.sequence_numbers.is_empty() {
                let first_message_sequence = metadata.sequence_numbers.start;

                let nats_consumer = create_nats_consumer(
                    &jetstream,
                    &nats_consumer_config,
                    &config.stream_name,
                    first_message_sequence,
                )
                .await?;

                // Since range is exclusive, last message to reply is (end-1).
                let last_message_sequence = metadata.sequence_numbers.end - 1;
                let (hasher, buffer_size) = consume_nats_messages_until(
                    nats_consumer,
                    last_message_sequence,
                    consumer.clone(),
                    parser.fork(),
                )
                .await
                .with_context(|| format!("While attempting to replay sequences {first_message_sequence}..{last_message_sequence}"))?;

                consumer.replayed(buffer_size, hasher.finish());

                next_sequence.store(last_message_sequence + 1, Ordering::Release);
            } else {
                consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
            }
        }

        loop {
            let command = command_receiver.recv().await?;
            match command {
                command @ InputReaderCommand::Replay { .. } => {
                    unreachable!("{command:?} must be at the beginning of the command stream")
                }
                InputReaderCommand::Queue { .. } => {
                    let (buffer_size, hasher, batches) = queue.flush_with_aux();
                    let sequence_number_range = match (batches.first(), batches.last()) {
                        (Some((_, first)), Some((_, last))) => *first..*last + 1,
                        _ => {
                            // If no batches were queued, create an empty range [pos, pos).
                            let pos = next_sequence.load(Ordering::Acquire);
                            pos..pos
                        }
                    };
                    info!(
                        "Queued {:?} records ({sequence_number_range:?})",
                        buffer_size
                    );
                    let metadata_json = serde_json::to_value(&Metadata {
                        sequence_numbers: sequence_number_range,
                    })?;
                    let timestamp = batches.last().map(|(ts, _)| *ts).unwrap_or_else(Utc::now);
                    let hash = hasher.map(|h| h.finish()).unwrap_or(0);
                    let resume = Resume::Replay {
                        hash,
                        seek: metadata_json.clone(),
                        replay: rmpv::Value::Nil,
                    };

                    consumer.extended(
                        buffer_size,
                        Some(resume),
                        vec![Watermark::new(timestamp, Some(metadata_json))],
                    );
                }
                InputReaderCommand::Pause => {
                    if let Some(canceller) = canceller.take() {
                        canceller.cancel_and_join().await;
                    }
                }
                InputReaderCommand::Extend => {
                    info!("Extend from {:?}", next_sequence.load(Ordering::Acquire));
                    if canceller.is_none() {
                        let nats_consumer = create_nats_consumer(
                            &jetstream,
                            &nats_consumer_config,
                            &config.stream_name,
                            next_sequence.load(Ordering::Acquire),
                        )
                        .await?;

                        canceller = Some(
                            spawn_nats_reader(
                                nats_consumer,
                                next_sequence.clone(),
                                queue.clone(),
                                consumer.clone(),
                                parser.fork(),
                            )
                            .await?,
                        );
                    }
                }
                InputReaderCommand::Disconnect => break,
            }
        }
        if let Some(canceller) = canceller.take() {
            canceller.cancel_and_join().await;
        }
        Ok(())
    }
}

async fn create_nats_consumer(
    jetstream: &jetstream::Context,
    consumer_config: &NatsConsumerConfig,
    stream_name: &str,
    message_start_sequence: u64,
) -> AnyResult<NatsConsumer> {
    let mut consumer_config = consumer_config.clone();

    // For 0, use the deliver policy configured by the user.
    // For >0, override with ByStartSequence to resume from a checkpoint position.
    if message_start_sequence > 0 {
        consumer_config.deliver_policy = jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: message_start_sequence,
        };
    }

    Ok(jetstream
        .create_consumer_strict_on_stream(consumer_config, stream_name)
        .await?)
}

async fn consume_nats_messages_until(
    nats_consumer: NatsConsumer,
    last_message_sequence: u64,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
) -> AnyResult<(Xxh3Default, BufferSize)> {
    let mut nats_messages = nats_consumer.messages().await?;

    let mut hasher = Xxh3Default::new();
    let mut buffer_size = BufferSize::default();
    loop {
        let Some(result) = nats_messages.next().await else {
            return Err(anyhow!("Unexpected end of NATS stream"));
        };
        match result {
            Ok(message) => {
                let info = match message.info() {
                    Ok(info) => info,
                    Err(error) => {
                        consumer.error(
                            false,
                            anyhow!("Failed to get NATS message info: {error}"),
                            Some("nats-input"),
                        );
                        continue;
                    }
                };
                let data = &message.payload;
                let (buffer, errors) = parser.parse(data, &None);
                consumer.parse_errors(errors);
                if let Some(mut buffer) = buffer {
                    buffer.hash(&mut hasher);
                    buffer.flush();
                }
                let amt = BufferSize {
                    records: 1,
                    bytes: data.len(),
                };
                consumer.buffered(amt);
                buffer_size += amt;
                info!("Got message #{}", info.stream_sequence);

                match info.stream_sequence.cmp(&last_message_sequence) {
                    cmp::Ordering::Less => (),     // Still more messages to consume
                    cmp::Ordering::Equal => break, // This was the final message we wanted
                    cmp::Ordering::Greater => {
                        return Err(anyhow!("Received unexpected message with offset {}; maybe the requested messages have been deleted?", info.stream_sequence));
                    }
                }
            }
            Err(error) => consumer.error(false, anyhow!("NATS error: {error}"), Some("nats-input")),
        }
    }

    Ok((hasher, buffer_size))
}

/// Spawns a background task that continuously reads from an ordered consumer
/// and queues parsed messages.
///
/// Messages are tagged with their stream sequence number for checkpoint tracking.
/// The ordered consumer ensures no gaps occur; if one is detected, it automatically
/// recreates itself and resumes from the last known position.
async fn spawn_nats_reader(
    nats_consumer: NatsConsumer,
    next_sequence: Arc<AtomicU64>,
    queue: Arc<InputQueue<u64>>,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
) -> AnyResult<Canceller> {
    let mut nats_messages = nats_consumer.messages().await?;

    let cancel_token = CancellationToken::new();
    let join_handle = tokio::spawn({
        let cancel_token_copy = cancel_token.clone();
        async move {
            loop {
                select! {
                    _ = cancel_token_copy.cancelled() => {
                        break;
                    }
                    result = nats_messages.next() => {
                        let Some(result) = result else {
                            consumer.error(true, anyhow!("Unexpected end of NATS stream"), Some("nats-input"));
                            return;
                        };
                        match result {
                            Ok(message) => {
                                let info = match message.info() {
                                    Ok(info) => info,
                                    Err(error) => {
                                        consumer.error(false, anyhow!("Failed to get NATS message info: {error}"), Some("nats-input"));
                                        continue;
                                    }
                                };
                                info!("Got message #{}", info.stream_sequence);
                                // Store the *next* sequence to process for resume tracking.
                                // This is the checkpoint position if we need to restart.
                                next_sequence.store(info.stream_sequence + 1, Ordering::Release);
                                let data = &message.payload;
                                queue.push_with_aux(parser.parse(data, &None), Utc::now(), info.stream_sequence);
                            }
                            Err(error) => {
                                consumer.error(false, anyhow!("NATS error: {error}"), Some("nats-input"));
                            }
                        }
                    }
                }
            }
        }
    });

    Ok(Canceller {
        cancel_token,
        join_handle,
    })
}

/// Used to instruct a task to shut down, and wait for it to end.
struct Canceller {
    cancel_token: CancellationToken,
    join_handle: JoinHandle<()>,
}

impl Canceller {
    async fn cancel_and_join(self) {
        self.cancel_token.cancel();
        let _ = self.join_handle.await;
    }
}

impl InputReader for NatsReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}
