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
            AppendConditionFailed::FencingTokenMismatch("token".parse::<FencingToken>().unwrap())
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
        S2CheckpointMetadata {
            seq_num_range: 0..0,
            position_resolved: false,
        },
        S2StartFrom::SeqNum(5),
    );

    sender
        .send(InputReaderCommand::Replay {
            metadata: serde_json::to_value(S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
        S2CheckpointMetadata {
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
            S2CheckpointMetadata {
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
        S2CheckpointMetadata {
            seq_num_range: 0..0,
            position_resolved: false,
        },
        S2StartFrom::Beginning,
    );

    sender
        .send(InputReaderCommand::Replay {
            metadata: serde_json::to_value(S2CheckpointMetadata {
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
            S2CheckpointMetadata {
                seq_num_range: 0..0,
                position_resolved: false,
            },
            S2StartFrom::Beginning,
        );
        sender
            .send(InputReaderCommand::Replay {
                metadata: serde_json::to_value(S2CheckpointMetadata {
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
    metadata: S2CheckpointMetadata,
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
    metadata: S2CheckpointMetadata,
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
    extended: Vec<S2CheckpointMetadata>,
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

    fn extended(&self) -> Vec<S2CheckpointMetadata> {
        self.state()
            .extended
            .iter()
            .map(|m| S2CheckpointMetadata {
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
