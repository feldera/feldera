use crate::{
    InputConsumer, InputEndpoint, InputReader, Parser, TransportInputEndpoint,
    transport::{InputQueue, InputReaderCommand},
};
use anyhow::{Error as AnyError, Result as AnyResult, anyhow};
use async_trait::async_trait;
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{InputCommandReceiver, Resume, Watermark};
use feldera_types::{
    config::FtModel,
    program_schema::Relation,
    transport::s2::{S2InputConfig, S2StartFrom},
};
use futures::{Stream, StreamExt};
use s2_sdk::{
    S2, S2Stream,
    types::{
        AccountEndpoint, AppendConditionFailed, BasinEndpoint, ReadFrom, ReadInput, ReadLimits,
        ReadStart, ReadStop, RetryConfig, S2Config, S2Endpoints, S2Error,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::hash::Hasher;
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::Duration;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info, info_span, trace};
use xxhash_rust::xxh3::Xxh3Default;

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub(crate) seq_num_range: std::ops::Range<u64>,
    #[serde(default)]
    pub(crate) position_resolved: bool,
}

impl Metadata {
    pub(crate) fn from_resume_info(resume_info: Option<JsonValue>) -> Result<Self, AnyError> {
        Ok(resume_info
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or(Self {
                seq_num_range: 0..0,
                position_resolved: false,
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

#[cfg(test)]
pub(crate) fn make_replay_read_input(seq_num_range: &std::ops::Range<u64>) -> ReadInput {
    make_replay_read_input_from(seq_num_range.start, seq_num_range.end)
}

fn make_replay_read_input_from(start: u64, end: u64) -> ReadInput {
    let count = usize::try_from(end - start).unwrap_or(usize::MAX);
    make_read_input(start)
        .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(count)))
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

fn read_input_for_position(
    config: &S2InputConfig,
    next_seq: u64,
    position_resolved: bool,
) -> ReadInput {
    if position_resolved {
        make_read_input(next_seq)
    } else {
        config_to_read_input(config)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum S2ErrorKind {
    Retryable,
    Fatal,
}

fn classify_s2_error(error: &S2Error) -> S2ErrorKind {
    match error {
        S2Error::MalformedAccessToken(_) | S2Error::Validation(_) | S2Error::ReadUnwritten(_) => {
            S2ErrorKind::Fatal
        }
        S2Error::AppendConditionFailed(AppendConditionFailed::FencingTokenMismatch(_))
        | S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(_)) => {
            S2ErrorKind::Fatal
        }
        S2Error::Server(response) => classify_s2_server_code(&response.code),
        S2Error::Client(message) => classify_s2_client_message(message),
    }
}

fn classify_s2_server_code(code: &str) -> S2ErrorKind {
    match code {
        "request_timeout"
        | "rate_limited"
        | "other"
        | "storage"
        | "hot_server"
        | "unavailable"
        | "upstream_timeout"
        | "transaction_conflict" => S2ErrorKind::Retryable,
        _ => S2ErrorKind::Fatal,
    }
}

fn classify_s2_client_message(message: &str) -> S2ErrorKind {
    if message == "heartbeat timeout"
        || message == "timeout"
        || message.starts_with("connect:")
        || message.starts_with("connection closed early:")
        || message.starts_with("request canceled:")
        || message.starts_with("unexpected eof:")
        || message.starts_with("connection reset:")
        || message.starts_with("connection aborted:")
        || message.starts_with("connection refused:")
    {
        S2ErrorKind::Retryable
    } else {
        S2ErrorKind::Fatal
    }
}

#[derive(Debug, Clone)]
struct S2Position {
    seq_num: u64,
}

#[derive(Debug, Clone)]
struct S2Record {
    seq_num: u64,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct S2Batch {
    records: Vec<S2Record>,
    tail: Option<S2Position>,
}

type S2ReadSession = Pin<Box<dyn Send + Stream<Item = Result<S2Batch, S2Error>>>>;

#[derive(Clone)]
struct LiveReaderShared {
    next_seq: Arc<AtomicU64>,
    position_resolved: Arc<AtomicBool>,
    queue: Arc<InputQueue<u64>>,
    ingest_lock: Arc<StdMutex<()>>,
}

#[async_trait]
trait S2StreamClient: Send + Sync {
    async fn check_tail(&self) -> Result<S2Position, S2Error>;
    async fn read_session(&self, input: ReadInput) -> Result<S2ReadSession, S2Error>;
}

#[async_trait]
impl S2StreamClient for S2Stream {
    async fn check_tail(&self) -> Result<S2Position, S2Error> {
        let tail = S2Stream::check_tail(self).await?;
        Ok(S2Position {
            seq_num: tail.seq_num,
        })
    }

    async fn read_session(&self, input: ReadInput) -> Result<S2ReadSession, S2Error> {
        let session = S2Stream::read_session(self, input).await?;
        Ok(Box::pin(session.map(|result| {
            result.map(|batch| S2Batch {
                records: batch
                    .records
                    .into_iter()
                    .map(|record| S2Record {
                        seq_num: record.seq_num,
                        body: record.body.to_vec(),
                    })
                    .collect(),
                tail: batch.tail.map(|tail| S2Position {
                    seq_num: tail.seq_num,
                }),
            })
        })))
    }
}

#[derive(Debug, Clone, Copy)]
struct BackoffConfig {
    initial: Duration,
    max: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: RECONNECT_INITIAL_BACKOFF,
            max: RECONNECT_MAX_BACKOFF,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BackoffState {
    config: BackoffConfig,
    current: Duration,
}

impl BackoffState {
    fn new(config: BackoffConfig) -> Self {
        Self {
            config,
            current: config.initial,
        }
    }

    fn reset(&mut self) {
        self.current = self.config.initial;
    }

    fn next_deadline(&mut self) -> Instant {
        let delay = self.current;
        self.current = (self.current * 2).min(self.config.max);
        Instant::now() + delay
    }
}

#[derive(Debug)]
struct ClassifiedError {
    kind: S2ErrorKind,
    error: AnyError,
    made_progress: bool,
}

impl ClassifiedError {
    fn new(context: impl std::fmt::Display, error: S2Error) -> Self {
        let kind = classify_s2_error(&error);
        Self {
            kind,
            error: anyhow!("{context}: {error}"),
            made_progress: false,
        }
    }

    fn retryable(context: impl std::fmt::Display) -> Self {
        Self {
            kind: S2ErrorKind::Retryable,
            error: anyhow!(context.to_string()),
            made_progress: false,
        }
    }

    fn fatal(context: impl std::fmt::Display) -> Self {
        Self {
            kind: S2ErrorKind::Fatal,
            error: anyhow!(context.to_string()),
            made_progress: false,
        }
    }

    fn after_progress(mut self, made_progress: bool) -> Self {
        self.made_progress = made_progress;
        self
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
                    // Configure retry policy: more attempts with longer delays than SDK defaults.
                    // The connector owns the lifecycle after the SDK exhausts these finite retries.
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
                Arc::new(s2_stream),
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
        s2_stream: Arc<dyn S2StreamClient>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> Result<(), AnyError> {
        Self::worker_task_with_backoff(
            config,
            resume_info,
            s2_stream,
            consumer,
            parser,
            command_receiver,
            BackoffConfig::default(),
        )
        .await
    }

    async fn worker_task_with_backoff(
        config: Arc<S2InputConfig>,
        resume_info: Metadata,
        s2_stream: Arc<dyn S2StreamClient>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
        backoff_config: BackoffConfig,
    ) -> Result<(), AnyError> {
        let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));
        let next_seq = Arc::new(AtomicU64::new(resume_info.seq_num_range.end));
        let position_resolved = Arc::new(AtomicBool::new(resume_info.position_resolved));
        let ingest_lock = Arc::new(StdMutex::new(()));
        let live_shared = LiveReaderShared {
            next_seq: next_seq.clone(),
            position_resolved: position_resolved.clone(),
            queue: queue.clone(),
            ingest_lock: ingest_lock.clone(),
        };

        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);

        // Handle replay commands before normal operation. Replays retry transient S2 failures
        // from the absolute sequence number that remains to be replayed, while preserving the
        // cumulative parser/hash/buffer state collected so far.
        while let Some((metadata, ())) = command_receiver.recv_replay().await? {
            info!("Replay: {:?}", metadata);
            match replay_checkpoint(
                s2_stream.clone(),
                consumer.clone(),
                parser.fork(),
                &mut command_receiver,
                &metadata,
                backoff_config,
            )
            .await?
            {
                ReplayOutcome::Completed => {
                    next_seq.store(metadata.seq_num_range.end, Ordering::Release);
                    // Preserve the checkpoint's resolved flag rather than forcing it true.
                    // An empty checkpoint taken before the start position was resolved
                    // (e.g. a Tail start that had not yet anchored) must stay unresolved so
                    // the live reader reapplies the configured start_from on resume instead
                    // of reading from absolute sequence 0.
                    position_resolved.store(metadata.position_resolved, Ordering::Release);
                }
                ReplayOutcome::Disconnected => return Ok(()),
            }
        }

        let (reader_error_sender, mut reader_error_receiver) =
            unbounded_channel::<ClassifiedError>();
        let mut canceller: Option<Canceller> = None;
        let mut state = ReaderLifecycleState::Paused;
        let mut backoff = BackoffState::new(backoff_config);
        let mut next_retry_at: Option<Instant> = None;

        loop {
            match state {
                ReaderLifecycleState::Paused => {
                    let command = command_receiver.recv().await?;
                    match command {
                        command @ InputReaderCommand::Replay { .. } => {
                            unreachable!(
                                "{command:?} must be at the beginning of the command stream"
                            )
                        }
                        InputReaderCommand::Queue { .. } => {
                            flush_queue(
                                &queue,
                                &next_seq,
                                &position_resolved,
                                &ingest_lock,
                                consumer.as_ref(),
                            )?;
                        }
                        InputReaderCommand::Pause => {}
                        InputReaderCommand::Extend => {
                            drain_reader_errors(&mut reader_error_receiver);
                            match resolve_tail(s2_stream.clone(), &live_shared, &config).await {
                                Ok(()) => {
                                    canceller = Some(spawn_live_reader(
                                        s2_stream.clone(),
                                        config.clone(),
                                        live_shared.clone(),
                                        parser.fork(),
                                        reader_error_sender.clone(),
                                    ));
                                    backoff.reset();
                                    state = ReaderLifecycleState::Running;
                                }
                                Err(e) => {
                                    apply_start_error(
                                        e,
                                        consumer.as_ref(),
                                        &mut state,
                                        &mut backoff,
                                        &mut next_retry_at,
                                    );
                                }
                            }
                        }
                        InputReaderCommand::Disconnect => break,
                    }
                }
                ReaderLifecycleState::Running => {
                    select! {
                        biased;
                        maybe_error = reader_error_receiver.recv() => {
                            if let Some(c) = canceller.take() {
                                c.cancel_and_join().await;
                            }
                            let error = maybe_error.unwrap_or_else(|| ClassifiedError::retryable("S2 reader task stopped without reporting an error"));
                            drain_reader_errors(&mut reader_error_receiver);
                            match error.kind {
                                S2ErrorKind::Retryable => {
                                    if error.made_progress {
                                        backoff.reset();
                                    }
                                    consumer.error(false, error.error, Some("s2-input"));
                                    next_retry_at = Some(backoff.next_deadline());
                                    state = ReaderLifecycleState::ErrorRetrying;
                                }
                                S2ErrorKind::Fatal => {
                                    consumer.error(true, error.error, Some("s2-input"));
                                    state = ReaderLifecycleState::Stopped;
                                }
                            }
                        }
                        command = command_receiver.recv() => {
                            match command? {
                                command @ InputReaderCommand::Replay { .. } => {
                                    unreachable!("{command:?} must be at the beginning of the command stream")
                                }
                                InputReaderCommand::Queue { .. } => {
                                    flush_queue(
                                    &queue,
                                    &next_seq,
                                    &position_resolved,
                                    &ingest_lock,
                                    consumer.as_ref(),
                                )?;
                                }
                                InputReaderCommand::Pause => {
                                    if let Some(c) = canceller.take() {
                                        c.cancel_and_join().await;
                                    }
                                    drain_reader_errors(&mut reader_error_receiver);
                                    backoff.reset();
                                    state = ReaderLifecycleState::Paused;
                                }
                                InputReaderCommand::Extend => {}
                                InputReaderCommand::Disconnect => break,
                            }
                        }
                    }
                }
                ReaderLifecycleState::ErrorRetrying => {
                    let retry_at = next_retry_at.get_or_insert_with(|| backoff.next_deadline());
                    select! {
                        command = command_receiver.recv() => {
                            match command? {
                                command @ InputReaderCommand::Replay { .. } => {
                                    unreachable!("{command:?} must be at the beginning of the command stream")
                                }
                                InputReaderCommand::Queue { .. } => {
                                    flush_queue(
                                    &queue,
                                    &next_seq,
                                    &position_resolved,
                                    &ingest_lock,
                                    consumer.as_ref(),
                                )?;
                                }
                                InputReaderCommand::Pause => {
                                    if let Some(c) = canceller.take() {
                                        c.cancel_and_join().await;
                                    }
                                    drain_reader_errors(&mut reader_error_receiver);
                                    next_retry_at = None;
                                    backoff.reset();
                                    state = ReaderLifecycleState::Paused;
                                }
                                InputReaderCommand::Extend => {}
                                InputReaderCommand::Disconnect => break,
                            }
                        }
                        _ = sleep_until(*retry_at) => {
                            next_retry_at = None;
                            drain_reader_errors(&mut reader_error_receiver);
                            match resolve_tail(s2_stream.clone(), &live_shared, &config).await {
                                Ok(()) => {
                                    canceller = Some(spawn_live_reader(
                                        s2_stream.clone(),
                                        config.clone(),
                                        live_shared.clone(),
                                        parser.fork(),
                                        reader_error_sender.clone(),
                                    ));
                                    backoff.reset();
                                    state = ReaderLifecycleState::Running;
                                }
                                Err(e) => {
                                    apply_start_error(
                                        e,
                                        consumer.as_ref(),
                                        &mut state,
                                        &mut backoff,
                                        &mut next_retry_at,
                                    );
                                }
                            }
                        }
                    }
                }
                ReaderLifecycleState::Stopped => break,
            }
        }

        if let Some(c) = canceller.take() {
            c.cancel_and_join().await;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderLifecycleState {
    Running,
    ErrorRetrying,
    Paused,
    Stopped,
}

fn drain_reader_errors(reader_error_receiver: &mut UnboundedReceiver<ClassifiedError>) {
    while reader_error_receiver.try_recv().is_ok() {}
}

fn lock_ingest(ingest_lock: &StdMutex<()>) -> StdMutexGuard<'_, ()> {
    ingest_lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn flush_queue(
    queue: &InputQueue<u64>,
    next_seq: &AtomicU64,
    position_resolved: &AtomicBool,
    ingest_lock: &StdMutex<()>,
    consumer: &dyn InputConsumer,
) -> Result<(), AnyError> {
    let _guard = lock_ingest(ingest_lock);
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
        position_resolved: position_resolved.load(Ordering::Acquire),
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
    Ok(())
}

/// Resolves the S2 tail position when the start position has not yet been anchored.
///
/// This runs in the worker (not the spawned reader) so that a checkpoint `Queue`
/// cannot observe an unresolved, zero-anchored position. Once the tail is known,
/// the live reader always resumes from the absolute sequence number, including
/// sequence 0.
async fn resolve_tail(
    s2_stream: Arc<dyn S2StreamClient>,
    shared: &LiveReaderShared,
    config: &S2InputConfig,
) -> Result<(), ClassifiedError> {
    if shared.position_resolved.load(Ordering::Acquire)
        || !matches!(config.start_from, S2StartFrom::Tail)
    {
        return Ok(());
    }
    let tail = s2_stream
        .check_tail()
        .await
        .map_err(|e| ClassifiedError::new("failed to resolve S2 tail position", e))?;
    shared.next_seq.store(tail.seq_num, Ordering::Release);
    shared.position_resolved.store(true, Ordering::Release);
    Ok(())
}

/// Applies a tail/session-setup error in the worker, updating lifecycle state.
fn apply_start_error(
    error: ClassifiedError,
    consumer: &dyn InputConsumer,
    state: &mut ReaderLifecycleState,
    backoff: &mut BackoffState,
    next_retry_at: &mut Option<Instant>,
) {
    match error.kind {
        S2ErrorKind::Retryable => {
            consumer.error(false, error.error, Some("s2-input"));
            *next_retry_at = Some(backoff.next_deadline());
            *state = ReaderLifecycleState::ErrorRetrying;
        }
        S2ErrorKind::Fatal => {
            consumer.error(true, error.error, Some("s2-input"));
            *state = ReaderLifecycleState::Stopped;
        }
    }
}

fn spawn_live_reader(
    s2_stream: Arc<dyn S2StreamClient>,
    config: Arc<S2InputConfig>,
    shared: LiveReaderShared,
    mut parser: Box<dyn Parser>,
    reader_error_sender: UnboundedSender<ClassifiedError>,
) -> Canceller {
    let cancel_token = CancellationToken::new();
    let join_handle = tokio::spawn({
        let cancel_token_copy = cancel_token.clone();
        async move {
            let start_seq = shared.next_seq.load(Ordering::Acquire);
            let resolved = shared.position_resolved.load(Ordering::Acquire);
            let read_input = read_input_for_position(&config, start_seq, resolved);
            let mut session = select! {
                _ = cancel_token_copy.cancelled() => return,
                result = s2_stream.read_session(read_input) => match result {
                    Ok(session) => session,
                    Err(error) => {
                        let _ = reader_error_sender.send(ClassifiedError::new(
                            format!("failed to create S2 read session from {start_seq}"),
                            error,
                        ));
                        return;
                    }
                }
            };

            let mut made_progress = false;
            loop {
                select! {
                    _ = cancel_token_copy.cancelled() => {
                        info!("S2 reader cancelled");
                        break;
                    }
                    result = session.next() => {
                        match result {
                            Some(Ok(batch)) => {
                                made_progress = true;
                                if let Err(error) = process_live_batch(
                                    batch,
                                    &shared.next_seq,
                                    &shared.position_resolved,
                                    &shared.queue,
                                    &shared.ingest_lock,
                                    parser.as_mut(),
                                ) {
                                    let _ = reader_error_sender.send(error.after_progress(made_progress));
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                let _ = reader_error_sender.send(ClassifiedError::new("S2 stream error after SDK retries", error).after_progress(made_progress));
                                break;
                            }
                            None => {
                                let _ = reader_error_sender.send(ClassifiedError::retryable("S2 read session ended; reconnecting").after_progress(made_progress));
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

fn process_live_batch(
    batch: S2Batch,
    next_seq: &AtomicU64,
    position_resolved: &AtomicBool,
    queue: &InputQueue<u64>,
    ingest_lock: &StdMutex<()>,
    parser: &mut dyn Parser,
) -> Result<(), ClassifiedError> {
    let _guard = lock_ingest(ingest_lock);
    let mut expected_seq = next_seq.load(Ordering::Acquire);
    let mut resolved = position_resolved.load(Ordering::Acquire);

    for record in &batch.records {
        trace!("Got record #{}", record.seq_num);
        if resolved && record.seq_num != expected_seq {
            return Err(ClassifiedError::fatal(format!(
                "S2 live read expected sequence number {expected_seq}, but received {}",
                record.seq_num
            )));
        }
        if !resolved {
            resolved = true;
            position_resolved.store(true, Ordering::Release);
        }
        expected_seq = record.seq_num + 1;
        next_seq.store(expected_seq, Ordering::Release);
        queue.push_with_aux(parser.parse(&record.body, None), Utc::now(), record.seq_num);
    }

    if let Some(tail) = &batch.tail {
        if resolved && tail.seq_num != expected_seq {
            return Err(ClassifiedError::fatal(format!(
                "S2 live read tail sequence number {} does not match expected sequence number {expected_seq}",
                tail.seq_num
            )));
        }
        next_seq.store(tail.seq_num, Ordering::Release);
        position_resolved.store(true, Ordering::Release);
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplayOutcome {
    Completed,
    Disconnected,
}

async fn replay_checkpoint(
    s2_stream: Arc<dyn S2StreamClient>,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
    command_receiver: &mut InputCommandReceiver<Metadata, ()>,
    metadata: &Metadata,
    backoff_config: BackoffConfig,
) -> Result<ReplayOutcome, AnyError> {
    let first = metadata.seq_num_range.start;
    let end = metadata.seq_num_range.end;
    if first == end {
        consumer.replayed(BufferSize::default(), Xxh3Default::new().finish());
        return Ok(ReplayOutcome::Completed);
    }

    let mut hasher = Xxh3Default::new();
    let mut buffer_size = BufferSize::default();
    let mut expected_seq = first;
    let mut backoff = BackoffState::new(backoff_config);

    while expected_seq < end {
        let read_input = make_replay_read_input_from(expected_seq, end);
        let mut session = match s2_stream.read_session(read_input).await {
            Ok(session) => session,
            Err(error) => {
                let classified = ClassifiedError::new(
                    format!("failed to create S2 read session for replay from {expected_seq}"),
                    error,
                );
                match classified.kind {
                    S2ErrorKind::Retryable => {
                        consumer.error(false, classified.error, Some("s2-input"));
                        if wait_replay_backoff(&mut backoff, command_receiver).await?
                            == ReplayOutcome::Disconnected
                        {
                            return Ok(ReplayOutcome::Disconnected);
                        }
                        continue;
                    }
                    S2ErrorKind::Fatal => return Err(classified.error),
                }
            }
        };

        loop {
            match session.next().await {
                Some(Ok(batch)) => {
                    backoff.reset();
                    for record in &batch.records {
                        if record.seq_num != expected_seq {
                            return Err(anyhow!(
                                "S2 replay expected sequence number {expected_seq}, but received {}",
                                record.seq_num
                            ));
                        }
                        let data = &record.body;
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
                        expected_seq += 1;
                    }
                    if expected_seq == end {
                        consumer.replayed(buffer_size, hasher.finish());
                        return Ok(ReplayOutcome::Completed);
                    }
                }
                Some(Err(error)) => {
                    let classified = ClassifiedError::new(
                        format!("S2 read error during replay from {expected_seq}"),
                        error,
                    );
                    match classified.kind {
                        S2ErrorKind::Retryable => {
                            consumer.error(false, classified.error, Some("s2-input"));
                            if wait_replay_backoff(&mut backoff, command_receiver).await?
                                == ReplayOutcome::Disconnected
                            {
                                return Ok(ReplayOutcome::Disconnected);
                            }
                            break;
                        }
                        S2ErrorKind::Fatal => return Err(classified.error),
                    }
                }
                None => {
                    return Err(anyhow!(
                        "S2 replay ended at sequence number {expected_seq}, before expected end {end}; the checkpointed records may have been trimmed"
                    ));
                }
            }
        }
    }

    consumer.replayed(buffer_size, hasher.finish());
    Ok(ReplayOutcome::Completed)
}

async fn wait_replay_backoff(
    backoff: &mut BackoffState,
    command_receiver: &mut InputCommandReceiver<Metadata, ()>,
) -> Result<ReplayOutcome, AnyError> {
    let retry_at = backoff.next_deadline();
    // Hold any non-Disconnect control command locally (instead of put_back) so the
    // backoff timer still elapses. Otherwise a buffered command would be re-received
    // immediately on the next retry, busy-looping read_session against S2.
    // `put_back` is single-slot, so a second pending command stays in the channel and
    // is handled on the following backoff iteration; no command is lost.
    let mut held: Option<InputReaderCommand> = None;
    let outcome = loop {
        select! {
            _ = sleep_until(retry_at) => break ReplayOutcome::Completed,
            command = command_receiver.recv() => match command? {
                InputReaderCommand::Disconnect => break ReplayOutcome::Disconnected,
                other if held.is_none() => held = Some(other),
                other => {
                    command_receiver.put_back(other);
                    break ReplayOutcome::Completed;
                }
            },
        }
    };
    if let Some(command) = held {
        command_receiver.put_back(command);
    }
    Ok(outcome)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{InputBuffer, Splitter};
    use dbsp::operator::StagedBuffers;
    use feldera_adapterlib::ConnectorMetadata;
    use feldera_adapterlib::format::ParseError;
    use feldera_types::adapter_stats::ConnectorHealth;
    use feldera_types::config::FtModel;
    use futures::stream;
    use rmpv::Value as RmpValue;
    use s2_sdk::types::{FencingToken, ValidationError};
    use std::collections::VecDeque;
    use std::sync::{Mutex, MutexGuard};
    use tokio::time::{Duration, timeout};

    #[test]
    fn classifies_server_codes_exactly() {
        for code in [
            "request_timeout",
            "rate_limited",
            "other",
            "storage",
            "hot_server",
            "unavailable",
            "upstream_timeout",
            "transaction_conflict",
        ] {
            assert_eq!(
                classify_s2_server_code(code),
                S2ErrorKind::Retryable,
                "{code}"
            );
        }
        for code in ["", "not_found", "permission_denied", "rate_limited_extra"] {
            assert_eq!(classify_s2_server_code(code), S2ErrorKind::Fatal, "{code}");
        }
    }

    #[test]
    fn classifies_client_messages_exactly() {
        for message in [
            "heartbeat timeout",
            "timeout",
            "connect: dns",
            "connection closed early: eof",
            "request canceled: dropped",
            "unexpected eof: body",
            "connection reset: reset",
            "connection aborted: aborted",
            "connection refused: refused",
        ] {
            assert_eq!(
                classify_s2_client_message(message),
                S2ErrorKind::Retryable,
                "{message}"
            );
        }
        for message in ["", "heart beat timeout", "timeout: later", "unknown"] {
            assert_eq!(
                classify_s2_client_message(message),
                S2ErrorKind::Fatal,
                "{message}"
            );
        }
    }

    #[test]
    fn classifies_constructible_s2_error_variants() {
        assert_eq!(
            classify_s2_error(&S2Error::Client("timeout".to_string())),
            S2ErrorKind::Retryable
        );
        assert_eq!(
            classify_s2_error(&S2Error::Client("unknown".to_string())),
            S2ErrorKind::Fatal
        );
        assert_eq!(
            classify_s2_error(&S2Error::MalformedAccessToken("bad".to_string())),
            S2ErrorKind::Fatal
        );
        assert_eq!(
            classify_s2_error(&S2Error::Validation(ValidationError("bad".to_string()))),
            S2ErrorKind::Fatal
        );
        assert_eq!(
            classify_s2_error(&S2Error::AppendConditionFailed(
                AppendConditionFailed::SeqNumMismatch(1)
            )),
            S2ErrorKind::Fatal
        );
        assert_eq!(
            classify_s2_error(&S2Error::AppendConditionFailed(
                AppendConditionFailed::FencingTokenMismatch(
                    "token".parse::<FencingToken>().unwrap()
                )
            )),
            S2ErrorKind::Fatal
        );
    }

    #[test]
    fn resolved_zero_uses_absolute_sequence_zero() {
        let config = basic_config(S2StartFrom::Tail);
        let input = read_input_for_position(&config, 0, true);
        assert!(matches!(input.start.from, ReadFrom::SeqNum(0)));

        let input = read_input_for_position(&config, 0, false);
        assert!(matches!(input.start.from, ReadFrom::TailOffset(0)));
    }

    #[tokio::test]
    async fn empty_unresolved_checkpoint_reuses_configured_start_from() {
        // A checkpoint taken before the start position was resolved must not force the
        // live reader to read from absolute sequence 0; the configured start_from still
        // applies on resume.
        let stream = Arc::new(FakeS2Stream::new(vec![FakeAction::Session(Ok(
            FakeSession::Pending,
        ))]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let handle = spawn_worker(
            stream.clone(),
            consumer,
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: false,
            },
            S2StartFrom::SeqNum(5),
        );

        sender
            .send(InputReaderCommand::Replay {
                metadata: serde_json::to_value(Metadata {
                    seq_num_range: 0..0,
                    position_resolved: false,
                })
                .unwrap(),
                data: RmpValue::Nil,
            })
            .unwrap();
        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| stream.read_inputs().len() == 1).await;
        assert!(matches!(
            stream.read_inputs()[0].start.from,
            ReadFrom::SeqNum(5)
        ));
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn resolved_tail_is_anchored_before_first_queue_checkpoint() {
        // The tail must be resolved (and the position anchored) before a Queue can emit a
        // replayable checkpoint, so an empty checkpoint carries the resolved sequence
        // rather than an unresolved zero.
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Tail(Ok(S2Position { seq_num: 7 })),
            FakeAction::Session(Ok(FakeSession::Pending)),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let handle = spawn_worker(
            stream.clone(),
            consumer.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: false,
            },
            S2StartFrom::Tail,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| stream.read_inputs().len() == 1).await;
        sender
            .send(InputReaderCommand::Queue {
                checkpoint_requested: false,
            })
            .unwrap();
        wait_for(|| !consumer.extended().is_empty()).await;
        assert_eq!(consumer.extended()[0].seq_num_range, 7..7);
        assert!(consumer.extended()[0].position_resolved);
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn tail_transient_recovers_and_starts_from_resolved_zero() {
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Tail(Err(retryable_error())),
            FakeAction::Tail(Ok(S2Position { seq_num: 0 })),
            FakeAction::Session(Ok(FakeSession::Pending)),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let handle = spawn_worker(
            stream.clone(),
            consumer.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: false,
            },
            S2StartFrom::Tail,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| consumer.error_count() >= 1).await;
        wait_for(|| stream.read_inputs().len() == 1).await;
        assert_eq!(consumer.errors()[0].0, false);
        assert!(matches!(
            stream.read_inputs()[0].start.from,
            ReadFrom::SeqNum(0)
        ));
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn live_setup_and_midstream_transients_recover() {
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Session(Err(retryable_error())),
            FakeAction::Session(Ok(FakeSession::Events(vec![
                Ok(batch(vec![(0, b"a")], None)),
                Err(retryable_error()),
            ]))),
            FakeAction::Session(Ok(FakeSession::Events(vec![Ok(batch(
                vec![(1, b"b")],
                None,
            ))]))),
            FakeAction::Session(Ok(FakeSession::Pending)),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let parser = RecordingParser::new();
        let handle = spawn_worker_with_parser(
            stream,
            consumer.clone(),
            parser.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: true,
            },
            S2StartFrom::Beginning,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| parser.data() == b"ab".to_vec()).await;
        assert!(consumer.errors().iter().any(|(fatal, _)| !fatal));
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn live_none_reconnects() {
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Session(Ok(FakeSession::Events(vec![Ok(batch(
                vec![(0, b"a")],
                None,
            ))]))),
            FakeAction::Session(Ok(FakeSession::Events(vec![Ok(batch(
                vec![(1, b"b")],
                None,
            ))]))),
            FakeAction::Session(Ok(FakeSession::Pending)),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let parser = RecordingParser::new();
        let handle = spawn_worker_with_parser(
            stream,
            consumer,
            parser.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: true,
            },
            S2StartFrom::Beginning,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| parser.data() == b"ab".to_vec()).await;
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn queue_responds_while_retrying() {
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Session(Ok(FakeSession::Events(vec![
                Ok(batch(vec![(0, b"a")], None)),
                Err(retryable_error()),
            ]))),
            FakeAction::Session(Ok(FakeSession::Pending)),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let handle = spawn_worker(
            stream,
            consumer.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: true,
            },
            S2StartFrom::Beginning,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| consumer.error_count() >= 1).await;
        sender
            .send(InputReaderCommand::Queue {
                checkpoint_requested: false,
            })
            .unwrap();
        wait_for(|| !consumer.extended().is_empty()).await;
        assert_eq!(consumer.extended()[0].seq_num_range, 0..1);
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pause_cancels_retry_backoff_and_extend_restarts() {
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Session(Err(retryable_error())),
            FakeAction::Session(Ok(FakeSession::Pending)),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let handle = spawn_worker(
            stream.clone(),
            consumer,
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: true,
            },
            S2StartFrom::Beginning,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| stream.session_attempts() == 1).await;
        sender.send(InputReaderCommand::Pause).unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(stream.session_attempts(), 1);
        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| stream.session_attempts() == 2).await;
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn fatal_setup_error_stops() {
        let stream = Arc::new(FakeS2Stream::new(vec![FakeAction::Session(Err(
            fatal_error(),
        ))]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let handle = spawn_worker(
            stream,
            consumer.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: true,
            },
            S2StartFrom::Beginning,
        );

        sender.send(InputReaderCommand::Extend).unwrap();
        wait_for(|| consumer.error_count() == 1).await;
        assert!(consumer.errors()[0].0);
        handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn live_gap_duplicate_or_tail_mismatch_is_fatal() {
        for (records, tail) in [
            (vec![(1, b"gap" as &[u8])], None),
            (vec![(0, b"a" as &[u8]), (0, b"dup" as &[u8])], None),
            (vec![(0, b"a" as &[u8])], Some(0)),
            (Vec::new(), Some(1)),
        ] {
            let stream = Arc::new(FakeS2Stream::new(vec![FakeAction::Session(Ok(
                FakeSession::Events(vec![Ok(batch(records, tail))]),
            ))]));
            let (sender, receiver) = unbounded_channel();
            let consumer = RecordingConsumer::new();
            consumer.allow_errors();
            let handle = spawn_worker(
                stream,
                consumer.clone(),
                receiver,
                Metadata {
                    seq_num_range: 0..0,
                    position_resolved: true,
                },
                S2StartFrom::Beginning,
            );
            sender.send(InputReaderCommand::Extend).unwrap();
            wait_for(|| consumer.error_count() == 1).await;
            assert!(consumer.errors()[0].0);
            handle.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn replay_partial_transient_resumes_without_duplicates() {
        let stream = Arc::new(FakeS2Stream::new(vec![
            FakeAction::Session(Ok(FakeSession::Events(vec![
                Ok(batch(vec![(0, b"a")], None)),
                Err(retryable_error()),
            ]))),
            FakeAction::Session(Ok(FakeSession::Events(vec![Ok(batch(
                vec![(1, b"b"), (2, b"c")],
                None,
            ))]))),
        ]));
        let (sender, receiver) = unbounded_channel();
        let consumer = RecordingConsumer::new();
        consumer.allow_errors();
        let parser = RecordingParser::new();
        let handle = spawn_worker_with_parser(
            stream.clone(),
            consumer.clone(),
            parser.clone(),
            receiver,
            Metadata {
                seq_num_range: 0..0,
                position_resolved: false,
            },
            S2StartFrom::Beginning,
        );

        sender
            .send(InputReaderCommand::Replay {
                metadata: serde_json::to_value(Metadata {
                    seq_num_range: 0..3,
                    position_resolved: true,
                })
                .unwrap(),
                data: RmpValue::Nil,
            })
            .unwrap();
        wait_for(|| consumer.replayed_count() == 1).await;
        sender.send(InputReaderCommand::Disconnect).unwrap();
        handle.await.unwrap().unwrap();
        assert_eq!(parser.data(), b"abc".to_vec());
        let inputs = stream.read_inputs();
        assert!(matches!(inputs[0].start.from, ReadFrom::SeqNum(0)));
        assert_eq!(inputs[0].stop.limits.count, Some(3));
        assert!(matches!(inputs[1].start.from, ReadFrom::SeqNum(1)));
        assert_eq!(inputs[1].stop.limits.count, Some(2));
    }

    #[tokio::test]
    async fn replay_gap_short_and_read_unwritten_are_fatal() {
        let cases = vec![
            (
                vec![FakeAction::Session(Ok(FakeSession::Events(vec![Ok(
                    batch(vec![(1, b"gap")], None),
                )])))],
                1,
            ),
            (
                vec![FakeAction::Session(Ok(FakeSession::Events(vec![])))],
                1,
            ),
            (
                vec![FakeAction::Session(Ok(FakeSession::Events(vec![Ok(
                    batch(vec![(0, b"short")], None),
                )])))],
                2,
            ),
            (vec![FakeAction::Session(Err(fatal_error()))], 1),
        ];
        for (case_index, (actions, replay_end)) in cases.into_iter().enumerate() {
            let stream = Arc::new(FakeS2Stream::new(actions));
            let (sender, receiver) = unbounded_channel();
            let consumer = RecordingConsumer::new();
            consumer.allow_errors();
            let handle = spawn_worker(
                stream,
                consumer.clone(),
                receiver,
                Metadata {
                    seq_num_range: 0..0,
                    position_resolved: false,
                },
                S2StartFrom::Beginning,
            );
            sender
                .send(InputReaderCommand::Replay {
                    metadata: serde_json::to_value(Metadata {
                        seq_num_range: 0..replay_end,
                        position_resolved: true,
                    })
                    .unwrap(),
                    data: RmpValue::Nil,
                })
                .unwrap();
            timeout(Duration::from_secs(1), handle)
                .await
                .unwrap_or_else(|_| panic!("replay fatal case {case_index} timed out"))
                .unwrap()
                .unwrap_err();
        }
    }

    fn spawn_worker(
        stream: Arc<FakeS2Stream>,
        consumer: RecordingConsumer,
        receiver: UnboundedReceiver<InputReaderCommand>,
        metadata: Metadata,
        start_from: S2StartFrom,
    ) -> JoinHandle<Result<(), AnyError>> {
        spawn_worker_with_parser(
            stream,
            consumer,
            RecordingParser::new(),
            receiver,
            metadata,
            start_from,
        )
    }

    fn spawn_worker_with_parser(
        stream: Arc<FakeS2Stream>,
        consumer: RecordingConsumer,
        parser: RecordingParser,
        receiver: UnboundedReceiver<InputReaderCommand>,
        metadata: Metadata,
        start_from: S2StartFrom,
    ) -> JoinHandle<Result<(), AnyError>> {
        tokio::spawn(S2Reader::worker_task_with_backoff(
            Arc::new(basic_config(start_from)),
            metadata,
            stream,
            Box::new(consumer),
            Box::new(parser),
            receiver,
            BackoffConfig {
                initial: Duration::from_millis(1),
                max: Duration::from_millis(2),
            },
        ))
    }

    fn basic_config(start_from: S2StartFrom) -> S2InputConfig {
        S2InputConfig {
            basin: "basin".to_string(),
            stream: "stream".to_string(),
            auth_token: "token".to_string(),
            endpoint: None,
            start_from,
        }
    }

    fn retryable_error() -> S2Error {
        S2Error::Client("timeout".to_string())
    }

    fn fatal_error() -> S2Error {
        S2Error::Client("unknown".to_string())
    }

    fn batch(records: Vec<(u64, &[u8])>, tail: Option<u64>) -> S2Batch {
        S2Batch {
            records: records
                .into_iter()
                .map(|(seq_num, body)| S2Record {
                    seq_num,
                    body: body.to_vec(),
                })
                .collect(),
            tail: tail.map(|seq_num| S2Position { seq_num }),
        }
    }

    async fn wait_for(mut condition: impl FnMut() -> bool) {
        timeout(Duration::from_secs(1), async move {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("timed out waiting for condition");
    }

    enum FakeAction {
        Tail(Result<S2Position, S2Error>),
        Session(Result<FakeSession, S2Error>),
    }

    enum FakeSession {
        Events(Vec<Result<S2Batch, S2Error>>),
        Pending,
    }

    struct FakeS2Stream {
        actions: Mutex<VecDeque<FakeAction>>,
        read_inputs: Mutex<Vec<ReadInput>>,
    }

    impl FakeS2Stream {
        fn new(actions: Vec<FakeAction>) -> Self {
            Self {
                actions: Mutex::new(actions.into()),
                read_inputs: Mutex::new(Vec::new()),
            }
        }

        fn read_inputs(&self) -> Vec<ReadInput> {
            self.read_inputs.lock().unwrap().clone()
        }

        fn session_attempts(&self) -> usize {
            self.read_inputs.lock().unwrap().len()
        }

        fn next_action(&self) -> FakeAction {
            self.actions
                .lock()
                .unwrap()
                .pop_front()
                .expect("missing fake S2 action")
        }
    }

    #[async_trait]
    impl S2StreamClient for FakeS2Stream {
        async fn check_tail(&self) -> Result<S2Position, S2Error> {
            match self.next_action() {
                FakeAction::Tail(result) => result,
                FakeAction::Session(_) => panic!("expected tail action"),
            }
        }

        async fn read_session(&self, input: ReadInput) -> Result<S2ReadSession, S2Error> {
            self.read_inputs.lock().unwrap().push(input);
            match self.next_action() {
                FakeAction::Session(Ok(FakeSession::Events(events))) => {
                    Ok(Box::pin(stream::iter(events)))
                }
                FakeAction::Session(Ok(FakeSession::Pending)) => Ok(Box::pin(stream::pending())),
                FakeAction::Session(Err(error)) => Err(error),
                FakeAction::Tail(_) => panic!("expected session action"),
            }
        }
    }

    #[derive(Clone)]
    struct RecordingConsumer(Arc<Mutex<ConsumerState>>);

    #[derive(Default)]
    struct ConsumerState {
        errors: Vec<(bool, String)>,
        extended: Vec<Metadata>,
        replayed: usize,
        allow_errors: bool,
    }

    impl RecordingConsumer {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(ConsumerState::default())))
        }

        fn state(&self) -> MutexGuard<'_, ConsumerState> {
            self.0.lock().unwrap()
        }

        fn allow_errors(&self) {
            self.state().allow_errors = true;
        }

        fn errors(&self) -> Vec<(bool, String)> {
            self.state().errors.clone()
        }

        fn error_count(&self) -> usize {
            self.state().errors.len()
        }

        fn extended(&self) -> Vec<Metadata> {
            self.state()
                .extended
                .iter()
                .map(|m| Metadata {
                    seq_num_range: m.seq_num_range.clone(),
                    position_resolved: m.position_resolved,
                })
                .collect()
        }

        fn replayed_count(&self) -> usize {
            self.state().replayed
        }
    }

    impl InputConsumer for RecordingConsumer {
        fn max_batch_size(&self) -> usize {
            usize::MAX
        }
        fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
            Some(FtModel::ExactlyOnce)
        }
        fn parse_errors(&self, _errors: Vec<ParseError>) {}
        fn buffered(&self, _amt: BufferSize) {}
        fn replayed(&self, _num_records: BufferSize, _hash: u64) {
            self.state().replayed += 1;
        }
        fn request_step(&self) {}
        fn extended(&self, _amt: BufferSize, _resume: Option<Resume>, watermarks: Vec<Watermark>) {
            let metadata = watermarks.into_iter().find_map(|w| w.metadata).unwrap();
            self.state()
                .extended
                .push(serde_json::from_value(metadata).unwrap());
        }
        fn eoi(&self) {}
        fn start_transaction(&self, _label: Option<&str>) {}
        fn commit_transaction(&self) {}
        fn error(&self, fatal: bool, error: AnyError, _tag: Option<&'static str>) {
            let mut state = self.state();
            assert!(state.allow_errors, "unexpected error: {error}");
            state.errors.push((fatal, error.to_string()));
        }
        fn update_connector_health(&self, _health: ConnectorHealth) {}
        fn completion_watcher(
            &self,
        ) -> Option<tokio::sync::watch::Receiver<feldera_types::coordination::Completion>> {
            None
        }
    }

    #[derive(Clone)]
    struct RecordingParser(Arc<Mutex<Vec<u8>>>);

    impl RecordingParser {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Vec::new())))
        }
        fn data(&self) -> Vec<u8> {
            self.0.lock().unwrap().clone()
        }
    }

    impl Parser for RecordingParser {
        fn parse(
            &mut self,
            data: &[u8],
            _metadata: Option<ConnectorMetadata>,
        ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
            self.0.lock().unwrap().extend_from_slice(data);
            (None, Vec::new())
        }
        fn stage(&self, _buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
            panic!("stage not used")
        }
        fn splitter(&self) -> Box<dyn Splitter> {
            panic!("splitter not used")
        }
        fn fork(&self) -> Box<dyn Parser> {
            Box::new(self.clone())
        }
    }
}
