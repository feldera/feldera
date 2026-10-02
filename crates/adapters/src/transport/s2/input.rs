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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex as StdMutex, MutexGuard as StdMutexGuard};
use std::time::Duration;
use tokio::{
    select,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::JoinHandle,
    time::{Instant, sleep_until},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, info, info_span, trace};
use xxhash_rust::xxh3::Xxh3Default;

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Checkpoint/resume metadata persisted by the S2 input connector.
///
/// `seq_num_range` is the half-open `[start, end)` range of S2 sequence numbers
/// covered by a checkpoint step; `end` is the position to resume from. For an
/// empty (no-record) checkpoint it is `pos..pos`. `position_resolved` records
/// whether the start position has been anchored to an absolute S2 sequence number
/// (by consuming a record or resolving a tail) — once true, the connector always
/// resumes from `end` as an absolute sequence rather than recomputing the
/// configured `start_from`. It defaults to `false` so checkpoints written before
/// this field existed still deserialize and re-anchor on resume.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct S2CheckpointMetadata {
    pub(crate) seq_num_range: std::ops::Range<u64>,
    #[serde(default)]
    pub(crate) position_resolved: bool,
}

impl S2CheckpointMetadata {
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
        let resume_info = S2CheckpointMetadata::from_resume_info(resume_info)?;
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
    let from = match &config.start_from {
        S2StartFrom::SeqNum(n) => ReadFrom::SeqNum(*n),
        S2StartFrom::Timestamp(ts) => ReadFrom::Timestamp(*ts),
        S2StartFrom::TailOffset(n) => ReadFrom::TailOffset(*n),
        S2StartFrom::Beginning => ReadFrom::SeqNum(0),
        S2StartFrom::Tail => ReadFrom::TailOffset(0),
    };
    ReadInput::new().with_start(ReadStart::new().with_from(from))
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

/// Classifies a public `S2Error::Client(String)` after the SDK has exhausted its
/// own retries. The SDK exposes client-transport errors only as a formatted
/// string, so this matches the exact messages/prefixes that `s2-sdk` 0.31.10
/// produces (see `ClientError` in `s2-sdk/src/api.rs`). An unknown message is
/// treated as fatal rather than retrying blindly: if a future SDK rewords a
/// message, the connector fails closed instead of looping forever. Update this
/// list (and the `classifies_client_messages_exactly` test) when bumping s2-sdk.
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

/// State shared between the worker and the live reader task.
///
/// Wrapped in a single `Arc` and passed by shared reference, rather than a
/// struct of individual `Arc`s. `position_resolved` is an atomic flag read
/// lock-free by the reader; `ingest` guards the data that must stay consistent
/// between ingestion and checkpointing (see [`IngestState`]).
struct LiveReaderShared {
    position_resolved: AtomicBool,
    ingest: StdMutex<IngestState>,
}

/// The next S2 sequence number and the input queue, guarded together so that a
/// checkpoint flush (`Queue`) can never observe a `next_seq` advanced past a
/// record that has not yet been queued. This is the invariant that keeps
/// exactly-once checkpoints honest.
struct IngestState {
    next_seq: u64,
    queue: InputQueue<u64>,
}

impl LiveReaderShared {
    fn new(position_resolved: bool, next_seq: u64, queue: InputQueue<u64>) -> Self {
        Self {
            position_resolved: AtomicBool::new(position_resolved),
            ingest: StdMutex::new(IngestState { next_seq, queue }),
        }
    }

    fn lock_ingest(&self) -> StdMutexGuard<'_, IngestState> {
        self.ingest
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
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

    fn with_progress(mut self, made_progress: bool) -> Self {
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
        resume_info: S2CheckpointMetadata,
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
        resume_info: S2CheckpointMetadata,
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
        resume_info: S2CheckpointMetadata,
        s2_stream: Arc<dyn S2StreamClient>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
        backoff_config: BackoffConfig,
    ) -> Result<(), AnyError> {
        let live_shared = Arc::new(LiveReaderShared::new(
            resume_info.position_resolved,
            resume_info.seq_num_range.end,
            InputQueue::<u64>::new(consumer.clone()),
        ));

        let mut command_receiver =
            InputCommandReceiver::<S2CheckpointMetadata, ()>::new(command_receiver);

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
                    {
                        let mut ingest = live_shared.lock_ingest();
                        ingest.next_seq = metadata.seq_num_range.end;
                    }
                    // Preserve the checkpoint's resolved flag rather than forcing it true.
                    // An empty checkpoint taken before the start position was resolved
                    // (e.g. a Tail start that had not yet anchored) must stay unresolved so
                    // the live reader reapplies the configured start_from on resume instead
                    // of reading from absolute sequence 0.
                    live_shared
                        .position_resolved
                        .store(metadata.position_resolved, Ordering::Release);
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
                            flush_queue(&live_shared, consumer.as_ref())?;
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
                            // Collect every queued reader error rather than acting on only the
                            // first and discarding the rest. If any of them is fatal, treat the
                            // batch as fatal so a terminal failure is not masked by an earlier
                            // retryable one. S2 normally surfaces a single terminal error per
                            // session, so this is mostly defensive.
                            let mut errors: Vec<ClassifiedError> = maybe_error.into_iter().collect();
                            while let Ok(e) = reader_error_receiver.try_recv() {
                                errors.push(e);
                            }
                            let error = if errors.is_empty() {
                                ClassifiedError::retryable(
                                    "S2 reader task stopped without reporting an error",
                                )
                            } else if errors.iter().any(|e| e.kind == S2ErrorKind::Fatal) {
                                errors
                                    .into_iter()
                                    .find(|e| e.kind == S2ErrorKind::Fatal)
                                    .expect("a fatal error was present")
                            } else {
                                errors.into_iter().next().expect("non-empty")
                            };
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
                                    flush_queue(&live_shared, consumer.as_ref())?;
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
                                    flush_queue(&live_shared, consumer.as_ref())?;
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
                                // No-op: this state is only reachable from Running (on a
                                // reader error), so a restart is already pending via the
                                // retry timer; an Extend here would just duplicate it.
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

fn flush_queue(shared: &LiveReaderShared, consumer: &dyn InputConsumer) -> Result<(), AnyError> {
    let ingest = shared.lock_ingest();
    let (buffer_size, hasher, batches) = ingest.queue.flush_with_aux();
    let seq_range = match (batches.first(), batches.last()) {
        (Some((_, first)), Some((_, last))) => *first..*last + 1,
        // No records were queued; checkpoint the current resume position as a
        // zero-length range. hash is 0 because there is nothing to verify on
        // replay of an empty range.
        _ => {
            let pos = ingest.next_seq;
            pos..pos
        }
    };
    drop(ingest);
    debug!("Queued {:?} records ({seq_range:?})", buffer_size);
    let metadata_json = serde_json::to_value(&S2CheckpointMetadata {
        seq_num_range: seq_range,
        position_resolved: shared.position_resolved.load(Ordering::Acquire),
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
    {
        let mut ingest = shared.lock_ingest();
        ingest.next_seq = tail.seq_num;
    }
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
    shared: Arc<LiveReaderShared>,
    mut parser: Box<dyn Parser>,
    reader_error_sender: UnboundedSender<ClassifiedError>,
) -> Canceller {
    let cancel_token = CancellationToken::new();
    let join_handle = tokio::spawn({
        let cancel_token_copy = cancel_token.clone();
        async move {
            let start_seq = shared.lock_ingest().next_seq;
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
                                    &shared,
                                    parser.as_mut(),
                                ) {
                                    let _ = reader_error_sender.send(error.with_progress(made_progress));
                                    break;
                                }
                            }
                            Some(Err(error)) => {
                                let _ = reader_error_sender.send(ClassifiedError::new("S2 stream error after SDK retries", error).with_progress(made_progress));
                                break;
                            }
                            None => {
                                let _ = reader_error_sender.send(ClassifiedError::retryable("S2 read session ended; reconnecting").with_progress(made_progress));
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
    shared: &LiveReaderShared,
    parser: &mut dyn Parser,
) -> Result<(), ClassifiedError> {
    let mut ingest = shared.lock_ingest();
    let mut expected_seq = ingest.next_seq;
    let mut resolved = shared.position_resolved.load(Ordering::Acquire);

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
            shared.position_resolved.store(true, Ordering::Release);
        }
        expected_seq = record.seq_num + 1;
        ingest.next_seq = expected_seq;
        ingest
            .queue
            .push_with_aux(parser.parse(&record.body, None), Utc::now(), record.seq_num);
    }

    if let Some(tail) = &batch.tail {
        if resolved && tail.seq_num != expected_seq {
            return Err(ClassifiedError::fatal(format!(
                "S2 live read tail sequence number {} does not match expected sequence number {expected_seq}",
                tail.seq_num
            )));
        }
        ingest.next_seq = tail.seq_num;
        shared.position_resolved.store(true, Ordering::Release);
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
    command_receiver: &mut InputCommandReceiver<S2CheckpointMetadata, ()>,
    metadata: &S2CheckpointMetadata,
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
    command_receiver: &mut InputCommandReceiver<S2CheckpointMetadata, ()>,
) -> Result<ReplayOutcome, AnyError> {
    // Check for control commands *before* sleeping. Disconnect stops replay
    // immediately; any other command is buffered back for the worker to handle
    // once this backoff elapses (it cannot be serviced mid-replay). We take at
    // most one command per call — `put_back` is single-slot, so taking a second
    // before draining the first would assert-fail. The sleep then always elapses,
    // so a buffered command cannot busy-loop `read_session` against S2.
    if let Some(command) = command_receiver.try_recv()? {
        match command {
            InputReaderCommand::Disconnect => return Ok(ReplayOutcome::Disconnected),
            other => command_receiver.put_back(other),
        }
    }
    sleep_until(backoff.next_deadline()).await;
    Ok(ReplayOutcome::Completed)
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
mod tests;
