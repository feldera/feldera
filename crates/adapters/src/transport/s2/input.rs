use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand},
};
use anyhow::{Context, Error as AnyError, Result as AnyResult, anyhow};
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::s2::{S2InputConfig, S2StartFrom},
};
use futures::StreamExt;
use s2_sdk::{
    S2,
    types::{
        AccountEndpoint, BasinEndpoint, ReadFrom, ReadInput, ReadStart, RetryConfig, S2Config,
        S2Endpoints,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::hash::Hasher;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span};
use xxhash_rust::xxh3::Xxh3Default;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub(crate) seq_num_range: std::ops::Range<u64>,
}

impl Metadata {
    pub(crate) fn from_resume_info(resume_info: Option<JsonValue>) -> Result<Self, AnyError> {
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(Self {
                seq_num_range: 0..0,
            }))
    }
}

pub struct S2InputEndpoint {
    config: Arc<S2InputConfig>,
}

impl S2InputEndpoint {
    pub fn new(config: S2InputConfig) -> Result<Self, AnyError> {
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl InputEndpoint for S2InputEndpoint {
    fn fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }
}

impl TransportInputEndpoint for S2InputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let resume_info = Metadata::from_resume_info(resume_info)?;
        info!("Resume info: {:?}", resume_info);
        Ok(Box::new(S2Reader::new(
            self.config.clone(),
            resume_info,
            consumer,
            parser,
            &schema.name.name(),
        )?))
    }
}

fn make_read_input(start_seq: u64) -> ReadInput {
    ReadInput::new().with_start(ReadStart::new().with_from(ReadFrom::SeqNum(start_seq)))
}

fn config_to_read_input(config: &S2InputConfig) -> ReadInput {
    let start = match &config.start_from {
        S2StartFrom::SeqNum(n) => ReadStart::new().with_from(ReadFrom::SeqNum(*n)),
        S2StartFrom::Timestamp(ts) => ReadStart::new().with_from(ReadFrom::Timestamp(*ts)),
        S2StartFrom::TailOffset(n) => ReadStart::new().with_from(ReadFrom::TailOffset(*n)),
        S2StartFrom::Beginning => ReadStart::new().with_from(ReadFrom::SeqNum(0)),
        S2StartFrom::Tail => ReadStart::new().with_from(ReadFrom::TailOffset(0)),
    };
    ReadInput::new().with_start(start)
}

fn read_input_for_position(config: &S2InputConfig, next_seq: u64) -> ReadInput {
    if next_seq > 0 {
        make_read_input(next_seq)
    } else {
        config_to_read_input(config)
    }
}

struct S2Reader {
    command_sender: UnboundedSender<InputReaderCommand>,
}

impl S2Reader {
    fn new(
        config: Arc<S2InputConfig>,
        resume_info: Metadata,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        table_name: &str,
    ) -> AnyResult<Self> {
        let span = info_span!(
            "s2_input",
            table = %table_name,
            basin = %config.basin,
            stream = %config.stream,
        );
        let (command_sender, command_receiver) = unbounded_channel();

        let s2_stream = TOKIO
            .block_on(
                async {
                    // Configure retry policy: more attempts with longer delays than SDK defaults
                    let retry_config = RetryConfig::new()
                        .with_max_attempts(NonZeroU32::new(10).unwrap())
                        .with_min_base_delay(Duration::from_millis(100))
                        .with_max_base_delay(Duration::from_secs(10));

                    let mut s2_config = S2Config::new(config.auth_token.clone())
                        .with_connection_timeout(Duration::from_secs(10))
                        .with_request_timeout(Duration::from_secs(30))
                        .with_retry(retry_config);

                    if let Some(ref endpoint) = config.endpoint {
                        let endpoints = S2Endpoints::new(
                            AccountEndpoint::new(endpoint)?,
                            BasinEndpoint::new(endpoint)?,
                        )?;
                        s2_config = s2_config.with_endpoints(endpoints);
                    }

                    let client = S2::new(s2_config)?;
                    let basin = client.basin(config.basin.parse().map_err(|e| anyhow!("{e}"))?);
                    Ok::<_, AnyError>(
                        basin.stream(config.stream.parse().map_err(|e| anyhow!("{e}"))?),
                    )
                }
                .instrument(span.clone()),
            )
            .map_err(|e| {
                error!(basin = %config.basin, stream = %config.stream, "S2 init failed: {e:#}");
                e.context(format!(
                    "S2 initialization failed for stream '{}' in basin '{}'",
                    config.stream, config.basin,
                ))
            })?;

        let consumer_clone = consumer.clone();
        TOKIO.spawn(async move {
            Self::worker_task(
                config,
                resume_info,
                s2_stream,
                consumer_clone,
                parser,
                command_receiver,
            )
            .instrument(span)
            .await
            .unwrap_or_else(|e| consumer.error(true, e, Some("s2-input")));
        });

        Ok(Self { command_sender })
    }

    async fn worker_task(
        config: Arc<S2InputConfig>,
        resume_info: Metadata,
        s2_stream: s2_sdk::S2Stream,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> Result<(), AnyError> {
        let mut canceller: Option<Canceller> = None;
        let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));
        let next_seq = Arc::new(AtomicU64::new(resume_info.seq_num_range.end));
        let s2_stream = Arc::new(s2_stream);

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

        // Handle replay commands
        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Replay: {:?}", metadata);
            if !metadata.seq_num_range.is_empty() {
                let first = metadata.seq_num_range.start;
                let last = metadata.seq_num_range.end - 1;

                let read_input = make_read_input(first);
                let mut session = s2_stream.read_session(read_input).await.with_context(|| {
                    format!("Failed to create read session for replay from {first}")
                })?;

                let mut hasher = Xxh3Default::new();
                let mut buffer_size = BufferSize::default();
                let mut replay_parser = parser.fork();
                let mut done = false;

                while let Some(result) = session.next().await {
                    let batch = result.map_err(|e| anyhow!("S2 read error during replay: {e}"))?;
                    for record in &batch.records {
                        if record.seq_num > last {
                            done = true;
                            break;
                        }
                        let data = &record.body;
                        let (buffer, errors) = replay_parser.parse(data, None);
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
                        if record.seq_num == last {
                            done = true;
                            break;
                        }
                    }
                    if done {
                        break;
                    }
                }

                consumer.replayed(buffer_size, hasher.finish());
                next_seq.store(last + 1, Ordering::Release);
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
                    let seq_range = match (batches.first(), batches.last()) {
                        (Some((_, first)), Some((_, last))) => *first..*last + 1,
                        _ => {
                            let pos = next_seq.load(Ordering::Acquire);
                            pos..pos
                        }
                    };
                    info!("Queued {:?} records ({seq_range:?})", buffer_size);
                    let metadata_json = serde_json::to_value(&Metadata {
                        seq_num_range: seq_range,
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
                    if let Some(c) = canceller.take() {
                        c.cancel_and_join().await;
                    }
                }
                InputReaderCommand::Extend => {
                    let cur_seq = next_seq.load(Ordering::Acquire);
                    info!("Extend from {cur_seq}");
                    if canceller.is_none() {
                        canceller = Some(spawn_s2_reader(
                            s2_stream.clone(),
                            config.clone(),
                            next_seq.clone(),
                            queue.clone(),
                            consumer.clone(),
                            parser.fork(),
                        ));
                    }
                }
                InputReaderCommand::Disconnect => break,
            }
        }
        if let Some(c) = canceller.take() {
            c.cancel_and_join().await;
        }
        Ok(())
    }
}

fn spawn_s2_reader(
    s2_stream: Arc<s2_sdk::S2Stream>,
    config: Arc<S2InputConfig>,
    next_seq: Arc<AtomicU64>,
    queue: Arc<InputQueue<u64>>,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
) -> Canceller {
    let cancel_token = CancellationToken::new();
    let join_handle = tokio::spawn({
        let cancel_token_copy = cancel_token.clone();
        async move {
            let start_seq = next_seq.load(Ordering::Acquire);
            let read_input = read_input_for_position(&config, start_seq);

            // Create read session. The SDK will automatically retry transient failures
            // according to the configured RetryConfig (10 attempts, 100ms-10s delays).
            let mut session = match s2_stream.read_session(read_input).await {
                Ok(session) => session,
                Err(error) => {
                    // SDK has exhausted all retries. This is a fatal error.
                    consumer.error(
                        true,
                        anyhow!("Failed to create S2 read session after retries: {error}"),
                        Some("s2-input"),
                    );
                    error!(
                        next_seq = start_seq,
                        "S2 session setup failed after SDK retries: {error}"
                    );
                    return;
                }
            };

            // Read loop. The SDK's read_session automatically handles:
            // - Reconnection on transient failures
            // - Heartbeat timeout detection (20 seconds)
            // - Automatic retry with exponential backoff
            loop {
                select! {
                    _ = cancel_token_copy.cancelled() => {
                        info!("S2 reader cancelled");
                        break;
                    }
                    result = session.next() => {
                        match result {
                            Some(Ok(batch)) => {
                                // Successfully received a batch
                                for record in &batch.records {
                                    info!("Got record #{}", record.seq_num);
                                    next_seq.store(record.seq_num + 1, Ordering::Release);
                                    let data = &record.body;
                                    queue.push_with_aux(parser.parse(data, None), Utc::now(), record.seq_num);
                                }
                            }
                            Some(Err(error)) => {
                                // SDK has exhausted all retries for this error.
                                // This is a fatal error (e.g., auth failure, non-retryable error).
                                consumer.error(
                                    true,
                                    anyhow!("S2 stream error after SDK retries: {error}"),
                                    Some("s2-input"),
                                );
                                error!("S2 stream error after SDK retries: {error}");
                                break;
                            }
                            None => {
                                // Stream ended normally (reached end of data or end condition)
                                info!("S2 read session ended normally");
                                break;
                            }
                        }
                    }
                }
            }
        }
    });

    Canceller {
        cancel_token,
        join_handle,
    }
}

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

impl InputReader for S2Reader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
    }

    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}
