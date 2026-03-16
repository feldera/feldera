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
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand},
};
use anyhow::{Context, Error as AnyError, Result as AnyResult, anyhow};
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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span, trace};
use xxhash_rust::xxh3::Xxh3Default;

type NatsConsumerConfig = nats_consumer::pull::OrderedConfig;
type NatsConsumer = nats_consumer::Consumer<NatsConsumerConfig>;

/// Connection and identification context for a NATS stream.
#[derive(Clone)]
struct StreamContext {
    connection_config: cfg::ConnectOptions,
    stream_name: String,
    inactivity_timeout: Duration,
}

/// Shared mutable state for the reader task.
#[derive(Clone)]
struct ReaderState {
    next_sequence: Arc<AtomicU64>,
    queue: Arc<InputQueue<u64>>,
}

/// Consumer and parser pair for message processing.
struct MessagePipeline {
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderLifecycleState {
    Running,
    ErrorRetrying,
    Paused,
    Stopped,
}

enum ConnectorError {
    Retryable(AnyError),
    Fatal(AnyError),
}

impl std::fmt::Debug for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(e) => write!(f, "Retryable({e:#})"),
            Self::Fatal(e) => write!(f, "Fatal({e:#})"),
        }
    }
}

impl ConnectorError {
    fn with_context(self, context: impl std::fmt::Display + Send + Sync + 'static) -> Self {
        match self {
            Self::Retryable(error) => Self::Retryable(error.context(context)),
            Self::Fatal(error) => Self::Fatal(error.context(context)),
        }
    }
}

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
        if config.inactivity_timeout_secs == 0 {
            return Err(anyhow!(
                "Invalid NATS input configuration: inactivity_timeout_secs must be at least 1 second"
            ));
        }
        if config.retry_interval_secs == 0 {
            return Err(anyhow!(
                "Invalid NATS input configuration: retry_interval_secs must be at least 1 second"
            ));
        }
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
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let resume_info = Metadata::from_resume_info(resume_info)?;
        info!("Resume info: {:?}", resume_info);

        Ok(Box::new(NatsReader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
            &schema.name.name(),
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
        table_name: &str,
    ) -> AnyResult<Self> {
        let span = info_span!(
            "nats_input",
            table = %table_name,
            server_url = %config.connection_config.server_url,
            stream_name = %config.stream_name,
            // Note: this is consumer_name from config, not the created name with unique suffix.
            consumer_name = config.consumer_config.name.as_deref().unwrap_or(""),
            consumer_description = config.consumer_config.description.as_deref().unwrap_or(""),
            filter_subjects = ?config.consumer_config.filter_subjects,
        );
        let (command_sender, command_receiver) = unbounded_channel();

        let consumer_clone = consumer.clone();
        TOKIO.spawn(async move {
            Self::worker_task(
                config,
                resume_info,
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
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to NATS server at {}",
                    connection_config.server_url
                )
            })?;

        Ok(client)
    }

    /// Verifies that the specified stream exists on the JetStream server.
    ///
    /// This provides early validation during initialization.
    /// If the stream doesn't exist, we fail fast with a clear error
    /// instead of timing out later during consumer creation.
    async fn verify_stream_exists(
        jetstream: &jetstream::Context,
        stream_name: &str,
    ) -> Result<(), AnyError> {
        let _ = fetch_stream_state(jetstream, stream_name).await?;
        Ok(())
    }

    async fn verify_server_and_stream_health(
        connection_config: &cfg::ConnectOptions,
        stream_name: &str,
    ) -> Result<(), AnyError> {
        Self::initialize_jetstream(connection_config, stream_name)
            .await
            .context("health check failed")?;
        Ok(())
    }

    async fn initialize_jetstream(
        connection_config: &cfg::ConnectOptions,
        stream_name: &str,
    ) -> AnyResult<jetstream::Context> {
        let init_deadline = Duration::from_secs(
            connection_config.connection_timeout_secs + connection_config.request_timeout_secs,
        );
        tokio::time::timeout(init_deadline, async {
            let client = Self::connect_nats(connection_config).await?;
            let js = jetstream::new(client);
            Self::verify_stream_exists(&js, stream_name).await?;
            Ok::<_, AnyError>(js)
        })
        .await
        .map_err(|_| anyhow!("NATS initialization timed out after {init_deadline:?}"))?
    }

    async fn try_start_stream_reader(
        nats_consumer_config: &NatsConsumerConfig,
        stream_ctx: &StreamContext,
        state: &ReaderState,
        io: MessagePipeline,
        reader_error_sender: UnboundedSender<ConnectorError>,
    ) -> Result<Canceller, ConnectorError> {
        let jetstream =
            Self::initialize_jetstream(&stream_ctx.connection_config, &stream_ctx.stream_name)
                .await
                .with_context(|| {
                    format!(
                        "NATS initialization failed for stream '{}' at server '{}' \
                    (connection_timeout={}s, request_timeout={}s)",
                        stream_ctx.stream_name,
                        stream_ctx.connection_config.server_url,
                        stream_ctx.connection_config.connection_timeout_secs,
                        stream_ctx.connection_config.request_timeout_secs,
                    )
                })
                .map_err(ConnectorError::Retryable)?;

        validate_resume_position(
            &jetstream,
            &stream_ctx.stream_name,
            state.next_sequence.load(Ordering::Acquire),
        )
        .await?;

        let nats_consumer = create_nats_consumer(
            &jetstream,
            nats_consumer_config,
            &stream_ctx.stream_name,
            state.next_sequence.load(Ordering::Acquire),
        )
        .await
        .map_err(ConnectorError::Retryable)?;

        spawn_nats_reader(
            jetstream,
            nats_consumer,
            state.clone(),
            stream_ctx.clone(),
            io,
            reader_error_sender,
        )
        .await
        .map_err(ConnectorError::Retryable)
    }

    async fn worker_task(
        config: Arc<NatsInputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut state = ReaderLifecycleState::Paused;
        let mut canceller: Option<Canceller> = None;
        let mut next_retry_at: Option<tokio::time::Instant> = None;
        let mut retry_count: u32 = 0;
        let (mut reader_error_sender, mut reader_error_receiver) =
            unbounded_channel::<ConnectorError>();
        let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));
        let next_sequence = Arc::new(AtomicU64::new(resume_info.sequence_numbers.end));
        let nats_consumer_config = translate_consumer_options(&config.consumer_config);
        let inactivity_timeout = Duration::from_secs(config.inactivity_timeout_secs);
        let retry_interval = Duration::from_secs(config.retry_interval_secs);
        let stream_ctx = StreamContext {
            connection_config: config.connection_config.clone(),
            stream_name: config.stream_name.clone(),
            inactivity_timeout,
        };
        let live_state = ReaderState {
            next_sequence: next_sequence.clone(),
            queue: queue.clone(),
        };

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

        // Handle replay commands
        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Attempt to replay: {:?}", metadata);
            if metadata.sequence_numbers.is_empty() {
                consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
                continue;
            }

            let first_message_sequence = metadata.sequence_numbers.start;
            // Since range is exclusive, last message to replay is (end - 1).
            let last_message_sequence = metadata.sequence_numbers.end - 1;

            'replay_attempt: loop {
                let replay_result: Result<(Xxh3Default, BufferSize), ConnectorError> = async {
                    let jetstream = Self::initialize_jetstream(&stream_ctx.connection_config, &stream_ctx.stream_name)
                        .await
                        .with_context(|| {
                            format!(
                                "NATS replay initialization failed for stream '{}' at server '{}'",
                                stream_ctx.stream_name,
                                stream_ctx.connection_config.server_url,
                            )
                        })
                        .map_err(ConnectorError::Retryable)?;

                    validate_replay_range(&jetstream, &stream_ctx.stream_name, &metadata.sequence_numbers)
                        .await?;

                    let nats_consumer = create_nats_consumer(
                        &jetstream,
                        &nats_consumer_config,
                        &stream_ctx.stream_name,
                        first_message_sequence,
                    )
                    .await
                    .map_err(ConnectorError::Retryable)?;

                    consume_nats_messages_until(
                        &jetstream,
                        nats_consumer,
                        last_message_sequence,
                        &stream_ctx,
                        MessagePipeline {
                            consumer: consumer.clone(),
                            parser: parser.fork(),
                        },
                    )
                    .await
                    .map_err(|error| {
                        error.with_context(format!(
                            "While attempting to replay sequences {first_message_sequence}..{last_message_sequence}"
                        ))
                    })
                }
                .await;

                match replay_result {
                    Ok((hasher, buffer_size)) => {
                        consumer.replayed(buffer_size, hasher.finish());
                        next_sequence.store(last_message_sequence + 1, Ordering::Release);
                        break;
                    }
                    Err(ConnectorError::Retryable(error)) => {
                        consumer.error(
                            false,
                            anyhow!(
                                "NATS replay entered ERROR state, retrying in {:?}: {error:#}",
                                retry_interval
                            ),
                            Some("nats-input"),
                        );
                        if let Some(command) = command_receiver.try_recv()? {
                            match command {
                                InputReaderCommand::Disconnect => return Ok(()),
                                InputReaderCommand::Replay { .. } => {
                                    // Keep this command buffered and continue retrying
                                    // the current replay range until it completes.
                                    command_receiver.put_back(command);
                                }
                                other => {
                                    // Honor control commands while replay is retrying.
                                    command_receiver.put_back(other);
                                    break 'replay_attempt;
                                }
                            }
                        }

                        tokio::time::sleep(retry_interval).await;
                    }
                    Err(ConnectorError::Fatal(error)) => {
                        consumer.error(true, error, Some("nats-input"));
                        return Ok(());
                    }
                }
            }
        }

        loop {
            let command = match state {
                ReaderLifecycleState::Running => {
                    select! {
                        maybe_error = reader_error_receiver.recv() => {
                            match maybe_error {
                                Some(error) => {
                                    if let Some(canceller) = canceller.take() {
                                        canceller.cancel_and_join().await;
                                    }
                                    // Drain any additional errors queued before cancellation
                                    // to prevent stale errors from affecting the next reader.
                                    while reader_error_receiver.try_recv().is_ok() {}
                                    match error.with_context("NATS reader task failed") {
                                        ConnectorError::Retryable(error) => {
                                            state = ReaderLifecycleState::ErrorRetrying;
                                            retry_count = 1;
                                            next_retry_at = Some(tokio::time::Instant::now() + retry_interval);
                                            consumer.error(false, anyhow!("NATS input entered ERROR state, will retry in {retry_interval:?}: {error:#}"), Some("nats-input"));
                                        }
                                        ConnectorError::Fatal(error) => {
                                            state = ReaderLifecycleState::Stopped;
                                            next_retry_at = None;
                                            consumer.error(true, error, Some("nats-input"));
                                        }
                                    }
                                }
                                None => {
                                    // Error channel closed unexpectedly (reader task
                                    // dropped the sender without sending an error,
                                    // e.g. due to a panic). Treat as retryable.
                                    if let Some(canceller) = canceller.take() {
                                        canceller.cancel_and_join().await;
                                    }
                                    state = ReaderLifecycleState::ErrorRetrying;
                                    retry_count = 1;
                                    next_retry_at = Some(tokio::time::Instant::now() + retry_interval);
                                    consumer.error(false, anyhow!("NATS reader task exited unexpectedly without reporting an error, will retry in {retry_interval:?}"), Some("nats-input"));
                                }
                            }
                            continue;
                        }
                        command = command_receiver.recv() => command?,
                    }
                }
                ReaderLifecycleState::ErrorRetrying => {
                    select! {
                        maybe_error = reader_error_receiver.recv() => {
                            match maybe_error {
                                Some(ConnectorError::Retryable(_)) => {
                                    next_retry_at = Some(tokio::time::Instant::now() + retry_interval);
                                }
                                Some(ConnectorError::Fatal(error)) => {
                                    consumer.error(true, error, Some("nats-input"));
                                    state = ReaderLifecycleState::Stopped;
                                    next_retry_at = None;
                                }
                                None => {
                                    // Error channel closed (all senders dropped).
                                    // We are already in ErrorRetrying so just reset
                                    // the channel and let the retry timer recreate
                                    // the reader with a fresh sender.
                                    let (new_sender, new_receiver) = unbounded_channel();
                                    reader_error_sender = new_sender;
                                    reader_error_receiver = new_receiver;
                                }
                            }
                            continue;
                        }
                        _ = tokio::time::sleep_until(next_retry_at.expect("retry deadline should be set")) => {
                            match Self::try_start_stream_reader(
                                &nats_consumer_config,
                                &stream_ctx,
                                &live_state,
                                MessagePipeline {
                                    consumer: consumer.clone(),
                                    parser: parser.fork(),
                                },
                                reader_error_sender.clone(),
                            ).await {
                                Ok(new_canceller) => {
                                    info!("NATS input recovered after {retry_count} retries, resuming from {:?}", next_sequence.load(Ordering::Acquire));
                                    // Drain stale errors from the previous (failed) reader
                                    // before entering Running with a fresh reader.
                                    while reader_error_receiver.try_recv().is_ok() {}
                                    canceller = Some(new_canceller);
                                    state = ReaderLifecycleState::Running;
                                    retry_count = 0;
                                    next_retry_at = None;
                                }
                                Err(ConnectorError::Retryable(error)) => {
                                    retry_count += 1;
                                    consumer.error(
                                        false,
                                        anyhow!("NATS input retry #{retry_count} failed, next attempt in {retry_interval:?}: {}", error.root_cause()),
                                        Some("nats-input"),
                                    );
                                    next_retry_at = Some(tokio::time::Instant::now() + retry_interval);
                                }
                                Err(ConnectorError::Fatal(error)) => {
                                    consumer.error(
                                        true,
                                        anyhow!("NATS input encountered a non-retryable startup error: {error:#}"),
                                        Some("nats-input"),
                                    );
                                    state = ReaderLifecycleState::Stopped;
                                    next_retry_at = None;
                                }
                            }
                            continue;
                        }
                        command = command_receiver.recv() => command?,
                    }
                }
                ReaderLifecycleState::Paused => command_receiver.recv().await?,
                ReaderLifecycleState::Stopped => break,
            };

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
                    if buffer_size.records > 0 {
                        debug!(
                            "Queued {:?} records ({sequence_number_range:?})",
                            buffer_size
                        );
                    } else {
                        trace!("Queued 0 records ({sequence_number_range:?})");
                    }
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
                    // Drain errors sent by the reader before/during cancellation
                    // so they don't leak into the next Extend cycle.
                    while reader_error_receiver.try_recv().is_ok() {}
                    state = ReaderLifecycleState::Paused;
                    retry_count = 0;
                    next_retry_at = None;
                }
                InputReaderCommand::Extend => {
                    info!("Extend from {:?}", next_sequence.load(Ordering::Acquire));
                    if matches!(state, ReaderLifecycleState::Running) {
                        continue;
                    }
                    // Drain stale errors from a previous reader so they don't
                    // immediately trigger ErrorRetrying for the new reader.
                    while reader_error_receiver.try_recv().is_ok() {}
                    match Self::try_start_stream_reader(
                        &nats_consumer_config,
                        &stream_ctx,
                        &live_state,
                        MessagePipeline {
                            consumer: consumer.clone(),
                            parser: parser.fork(),
                        },
                        reader_error_sender.clone(),
                    )
                    .await
                    {
                        Ok(new_canceller) => {
                            state = ReaderLifecycleState::Running;
                            retry_count = 0;
                            canceller = Some(new_canceller);
                            next_retry_at = None;
                        }
                        Err(ConnectorError::Retryable(error)) => {
                            state = ReaderLifecycleState::ErrorRetrying;
                            retry_count = 1;
                            next_retry_at = Some(tokio::time::Instant::now() + retry_interval);
                            consumer.error(false, anyhow!("NATS input entered ERROR state, will retry in {retry_interval:?}: {error:#}"), Some("nats-input"));
                        }
                        Err(ConnectorError::Fatal(error)) => {
                            consumer.error(
                                true,
                                anyhow!("NATS input encountered a non-retryable startup error: {error:#}"),
                                Some("nats-input"),
                            );
                            state = ReaderLifecycleState::Stopped;
                            next_retry_at = None;
                        }
                    }
                }
                InputReaderCommand::Disconnect => {
                    state = ReaderLifecycleState::Stopped;
                    next_retry_at = None;
                }
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

    // Add a unique suffix to named consumers.
    // If consumer is unnamed, NATS automatically generates a random name.
    //
    // This fixes "consumer already exists" errors that occurred with rapid
    // pipeline restarts/replays before the previous consumer expires (inactive_threshold).
    consumer_config.name = consumer_config
        .name
        .map(|n| format!("{n}_{}", uuid::Uuid::now_v7()));

    jetstream
        .create_consumer_strict_on_stream(consumer_config.clone(), stream_name)
        .await
        .with_context(|| {
            format!(
                "Failed to create consumer on stream '{}' (start_sequence={}, deliver_policy={:?}, filter_subjects={:?})",
                stream_name,
                message_start_sequence,
                consumer_config.deliver_policy,
                consumer_config.filter_subjects,
            )
        })
}

/// Runs a health check after an inactivity timeout fires.
///
/// Returns `Ok(())` if the server and stream are healthy (caller should
/// continue its loop), or an error describing the stall and the failed check.
///
/// If the primary probe fails but the fallback reconnect succeeds, we still
/// return `Ok(())`. This is safe because the ordered consumer will detect the
/// lost heartbeats and automatically recreate its subscription on the
/// reconnected client, so messages will resume without intervention.
async fn check_inactivity_health(
    jetstream: &jetstream::Context,
    stream_ctx: &StreamContext,
    context: &str,
) -> Result<(), AnyError> {
    // First try a cheap probe against the existing JetStream context.
    // This avoids reconnect churn on quiet-but-healthy streams.
    if let Err(primary_error) = fetch_stream_state(jetstream, &stream_ctx.stream_name).await {
        // If the in-place probe fails, run a full reconnect + stream lookup as
        // a fallback check before surfacing a fatal error.
        NatsReader::verify_server_and_stream_health(
            &stream_ctx.connection_config,
            &stream_ctx.stream_name,
        )
        .await
        .map_err(|fallback_error| {
            anyhow!(
                "NATS {context} stalled for {:?}. Health check failed: {}; reconnect failed: {}",
                stream_ctx.inactivity_timeout,
                primary_error.root_cause(),
                fallback_error.root_cause(),
            )
        })?;
    }

    Ok(())
}

async fn consume_nats_messages_until(
    jetstream: &jetstream::Context,
    nats_consumer: NatsConsumer,
    last_message_sequence: u64,
    stream_ctx: &StreamContext,
    io: MessagePipeline,
) -> Result<(Xxh3Default, BufferSize), ConnectorError> {
    let MessagePipeline {
        consumer,
        mut parser,
    } = io;
    let mut nats_messages = nats_consumer
        .messages()
        .await
        .map_err(|error| ConnectorError::Retryable(error.into()))?;

    let mut hasher = Xxh3Default::new();
    let mut buffer_size = BufferSize::default();
    loop {
        let next_result =
            tokio::time::timeout(stream_ctx.inactivity_timeout, nats_messages.next()).await;
        match next_result {
            // Outer timeout fired while waiting for the next stream item.
            Err(_timeout_elapsed) => {
                check_inactivity_health(jetstream, stream_ctx, "replay")
                    .await
                    .map_err(ConnectorError::Retryable)?;
                continue;
            }
            Ok(None) => {
                return Err(ConnectorError::Retryable(anyhow!(
                    "Unexpected end of NATS stream"
                )));
            }
            Ok(Some(Err(error))) => {
                return Err(ConnectorError::Retryable(anyhow!("NATS error: {error}")));
            }
            Ok(Some(Ok(message))) => {
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
                let (buffer, errors) = parser.parse(data, None);
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
                debug!("Replay got message #{}", info.stream_sequence);

                match info.stream_sequence.cmp(&last_message_sequence) {
                    cmp::Ordering::Less => (),     // Still more messages to consume
                    cmp::Ordering::Equal => break, // This was the final message we wanted
                    cmp::Ordering::Greater => {
                        return Err(ConnectorError::Fatal(anyhow!(
                            "Received unexpected message with offset {}; maybe the requested messages have been deleted?",
                            info.stream_sequence
                        )));
                    }
                }
            }
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
    jetstream: jetstream::Context,
    nats_consumer: NatsConsumer,
    state: ReaderState,
    stream_ctx: StreamContext,
    io: MessagePipeline,
    reader_error_sender: UnboundedSender<ConnectorError>,
) -> AnyResult<Canceller> {
    let MessagePipeline {
        consumer,
        mut parser,
    } = io;
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
                    result = tokio::time::timeout(stream_ctx.inactivity_timeout, nats_messages.next()) => {
                        match result {
                            // Outer timeout fired while waiting for the next stream item.
                            Err(_timeout_elapsed) => {
                                if let Err(error) = check_inactivity_health(
                                    &jetstream,
                                    &stream_ctx,
                                    "input",
                                ).await {
                                    let _ = reader_error_sender.send(ConnectorError::Retryable(error));
                                    return;
                                }
                            }
                            Ok(None) => {
                                let _ = reader_error_sender.send(ConnectorError::Retryable(anyhow!("Unexpected end of NATS stream")));
                                return;
                            }
                            Ok(Some(Err(error))) => {
                                let _ = reader_error_sender.send(ConnectorError::Retryable(anyhow!("NATS message stream error: {error}")));
                                return;
                            }
                            Ok(Some(Ok(message))) => {
                                let info = match message.info() {
                                    Ok(info) => info,
                                    Err(error) => {
                                        consumer.error(false, anyhow!("Failed to get NATS message info: {error}"), Some("nats-input"));
                                        continue;
                                    }
                                };
                                trace!("Got message #{}", info.stream_sequence);
                                // Store the checkpoint position (the next sequence).
                                state.next_sequence.store(info.stream_sequence + 1, Ordering::Release);
                                let data = &message.payload;
                                state.queue.push_with_aux(parser.parse(data, None), Utc::now(), info.stream_sequence);
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

struct StreamState {
    messages: u64,
    first_sequence: u64,
    last_sequence: u64,
}

async fn fetch_stream_state(
    jetstream: &jetstream::Context,
    stream_name: &str,
) -> AnyResult<StreamState> {
    let stream = jetstream
        .get_stream(stream_name)
        .await
        .with_context(|| format!("Failed to get stream '{stream_name}'"))?;
    let stream_info = stream.cached_info();

    Ok(StreamState {
        messages: stream_info.state.messages,
        first_sequence: stream_info.state.first_sequence,
        last_sequence: stream_info.state.last_sequence,
    })
}

async fn validate_replay_range(
    jetstream: &jetstream::Context,
    stream_name: &str,
    requested_range: &std::ops::Range<u64>,
) -> Result<(), ConnectorError> {
    validate_sequence_bounds(
        jetstream,
        stream_name,
        SequenceValidationMode::Replay {
            requested_range: requested_range.clone(),
        },
    )
    .await
}

async fn validate_resume_position(
    jetstream: &jetstream::Context,
    stream_name: &str,
    resume_cursor: u64,
) -> Result<(), ConnectorError> {
    validate_sequence_bounds(
        jetstream,
        stream_name,
        SequenceValidationMode::Resume { resume_cursor },
    )
    .await
}

enum SequenceValidationMode {
    Replay {
        requested_range: std::ops::Range<u64>,
    },
    Resume {
        resume_cursor: u64,
    },
}

async fn validate_sequence_bounds(
    jetstream: &jetstream::Context,
    stream_name: &str,
    mode: SequenceValidationMode,
) -> Result<(), ConnectorError> {
    match &mode {
        SequenceValidationMode::Replay { requested_range } if requested_range.is_empty() => {
            return Ok(());
        }
        SequenceValidationMode::Resume { resume_cursor: 0 } => {
            // Fresh starts use `0` and should always be allowed.
            return Ok(());
        }
        _ => {}
    }

    // Fetching stream state is an I/O operation that can fail transiently
    // (e.g., timeout, temporary network issues). These should be retryable.
    let stream_state = fetch_stream_state(jetstream, stream_name)
        .await
        .map_err(ConnectorError::Retryable)?;
    let available_first = stream_state.first_sequence;
    let available_last = stream_state.last_sequence;

    // Logical validation errors (data out of bounds, stream empty) are fatal
    // because retrying won't change the outcome.
    match mode {
        SequenceValidationMode::Replay { requested_range } => {
            if stream_state.messages == 0 {
                return Err(ConnectorError::Fatal(anyhow!(
                    "Replay requested sequences {:?} from stream '{stream_name}', but the stream is empty",
                    requested_range
                )));
            }

            let requested_first = requested_range.start;
            let requested_last = requested_range.end - 1;

            if requested_first < available_first || requested_first > available_last {
                return Err(ConnectorError::Fatal(anyhow!(
                    "Replay start sequence {requested_first} is outside available stream range [{available_first}, {available_last}] for stream '{stream_name}'"
                )));
            }

            if requested_last > available_last {
                return Err(ConnectorError::Fatal(anyhow!(
                    "Replay end sequence {requested_last} exceeds available stream tail {available_last} for stream '{stream_name}'"
                )));
            }
        }
        SequenceValidationMode::Resume { resume_cursor } => {
            if stream_state.messages == 0 {
                return Err(ConnectorError::Fatal(anyhow!(
                    "Resume sequence {resume_cursor} is invalid for stream '{stream_name}': stream is empty"
                )));
            }

            let valid_upper = available_last.saturating_add(1);

            if resume_cursor < available_first {
                return Err(ConnectorError::Fatal(anyhow!(
                    "Resume sequence {resume_cursor} is before earliest available sequence {available_first} for stream '{stream_name}'"
                )));
            }

            if resume_cursor > valid_upper {
                return Err(ConnectorError::Fatal(anyhow!(
                    "Resume sequence {resume_cursor} is after valid upper bound {valid_upper} for stream '{stream_name}'"
                )));
            }
        }
    }

    Ok(())
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
