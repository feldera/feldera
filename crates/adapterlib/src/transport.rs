use anyhow::{Error as AnyError, Result as AnyResult};
use chrono::{DateTime, Utc};
use dyn_clone::DynClone;
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::program_schema::Relation;
use feldera_types::transaction::TransactionId;
use rmpv::{Value as RmpValue, ext::Error as RmpDecodeError};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use std::collections::VecDeque;
use std::fmt::Display;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;
use xxhash_rust::xxh3::Xxh3Default;

use crate::PipelineState;
use crate::catalog::InputCollectionHandle;
use crate::format::{BufferSize, InputBuffer, ParseError, Parser};
use crate::metrics::ConnectorMetrics;
use crate::utils::long_operation::LongOperationWarning;

/// Step number for fault-tolerant circuits.
///
/// The step number increases by 1 each time the circuit runs; that is, it
/// tracks the global clock for the outermost circuit.  The first step is
/// numbered zero.
///
/// A [fault-tolerant](crate#fault-tolerance) output transport divides output
/// into steps numbered sequentially.  If a given step is written multiple
/// times, the endpoint must discard the later writes.
pub type Step = u64;

/// A configured input endpoint.
pub trait InputEndpoint: Send {
    /// This endpoint's level of fault tolerance, if any:
    ///
    /// - An endpoint that returns `None` does not support suspend and resume or
    ///   any kind of fault tolerance and has no further constraints.
    ///
    /// - An endpoint that returns `Some(FtModel::AtLeastOnce)` can support
    ///   suspend and resume and at-least-once fault tolerance.  Such an
    ///   endpoint must pass `Some(Resume::*)` to [InputConsumer::extended] for
    ///   at least some steps (see [Resume] for details).
    ///
    /// - An endpoint that returns `Some(FtModel::ExactlyOnce)` can support
    ///   suspend and resume, at-least-once fault tolerance, and exactly once
    ///   fault tolerance.  Such an endpoint must pass `Some(Resume::Replay
    ///   {..})` to [InputConsumer::extended] for every step (see [Resume] for
    ///   details).
    fn fault_tolerance(&self) -> Option<FtModel>;
}

pub trait TransportInputEndpoint: InputEndpoint {
    /// Creates a new input endpoint. The endpoint should use `parser` to parse
    /// data into records. Returns an [`InputReader`] for reading the endpoint's
    /// data.  The endpoint will use `consumer` to report its progress.
    ///
    /// The `resume_info` parameter is used when resuming the pipeline from a
    /// checkpoint. It contains resume metadata that the endpoint returned via the
    /// [`InputConsumer::extended`] function before suspending. When specified,
    /// it tells a fault-tolerant input reader to seek past the data already read
    /// in the step whose metadata is given by the value.
    ///
    /// The reader is initially paused.
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>>;
}

#[doc(hidden)]
pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
        resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>>;
}

/// Commands for an [InputReader] to execute.
///
/// # Transitions
///
/// The following diagram shows the possible order in which the controller can
/// issue commands to [InputReader]s:
///
/// ```text
///   ┌─⯇─ (start) ─⯈──┐
///   │      │         │
///   │      │  ┌───┐  │
///   │      ▼  ▼   │  │
///   ├─⯇─ Replay ──┘  │
///   │      │         │
///   │      ▼         │
///   ├─⯇─ Extend⯇─────┤
///   │      │         │
///   │      │ ┌───┐   │
///   │      ▼ ▼   │   │
///   ├─⯇─ Queue ──┘   │
///   │      │         │
///   │      ▼         │
///   ├─⯇─ Pause ─⯈────┘
///   │      │
///   │      ▼
///   └───⯈Disconnect
///          │
///          ▼
///        (end)
/// ```
///
/// # Stalls
///
/// When the controller issues a [InputReaderCommand::Replay] or
/// [InputReaderCommand::Queue] command to an input adapter, it waits for the
/// input adapter to respond to them.  Until it receives a reply, the next step
/// cannot proceed. An input adapter that does not respond to one of these
/// commands will stall the entire pipeline.  However, the controller also uses
/// [InputReader::is_closed] to detect that an input adapter has died due to an
/// error or reaching end-of-input, so input adapters for which it is difficult
/// to handle errors gracefully can report that they have died using
/// `is_closed`, if necessary, as described in more detail below.
///
/// ## End-of-input handling
///
/// If an input adapter reaches the end of its input, and it isn't implemented
/// to wait for and pass along further input, then it should:
///
/// - Make sure that it has already indicated that it has buffered all of its
///   data, via [InputConsumer::buffered].
///
/// - Call [InputConsumer::eoi] to indicate that it has reached end of input.
///
/// - Respond to [InputReaderCommand::Queue] until it has queued all of its
///   input and has none left.
///
/// - Optionally, at this point, it may exit and start returning `true` from
///   `InputReader::is_closed`.
///
/// ## Error handling
///
/// If an input adapter encounters a fatal error that keeps it from continuing
/// to obtain input, then it should report the error via [InputConsumer::error]
/// with `true` for `fatal`.  Afterward, it may exit and start returning `true`
/// from `InputReader::is_closed`.
///
/// ## Additional requirement
///
/// An input adapter should ensure that, if it flushes any records to the
/// circuit in response to [InputReaderCommand::Replay] or
/// [InputReaderCommand::Queue], then it finishes up and responds to the
/// consumer using [InputConsumer::replayed] or [InputConsumer::extended],
/// respectively.  If it instead dies mid-way, then the controller will not
/// record the step properly and fault tolerance replay will be incorrect.
#[derive(Debug)]
pub enum InputReaderCommand {
    /// Tells the input reader to replay the step described by `metadata` and
    /// `data` by reading and flushing buffers for the data in the step, and
    /// then [InputConsumer::replayed] to signal completion.
    ///
    /// The input reader should report the data that it queues to
    /// [InputConsumer::buffered] as it does the replay.
    ///
    /// The input reader doesn't have to process other commands while it does
    /// the replay.
    ///
    /// # Constraints
    ///
    /// Only fault-tolerant input readers need to accept this. It will be issued
    /// zero or more times, before any other command.
    Replay { metadata: JsonValue, data: RmpValue },

    /// Tells the input reader to accept further input. The first time it
    /// receives this command, the reader should start from the resume point
    /// passed as `resume_info` when the endpoint was opened, if any, and
    /// otherwise from the beginning of input.
    ///
    /// The input reader should report the data that it queues to
    /// [InputConsumer::buffered] as it queues it.
    ///
    /// # Constraints
    ///
    /// The controller will not call this function:
    ///
    /// - Twice on a given reader without an intervening
    ///   [InputReaderCommand::Pause].
    ///
    /// - If it requested a replay (with [InputReaderCommand::Replay]) and the reader
    ///   hasn't yet reported that the replay is complete.
    Extend,

    /// Tells the input reader to stop reading more input.
    ///
    /// The controller uses this to limit the number of buffered records and to
    /// respond to user requests to pause the pipeline.
    ///
    /// # Constraints
    ///
    /// The controller issues this only after a paired
    /// [InputReaderCommand::Extend].
    Pause,

    /// Tells the input reader to flush input buffers to the circuit.
    ///
    /// The input reader can call [InputConsumer::max_batch_size] to find out
    /// how many records it should flush. When it's done, it must call
    /// [InputConsumer::extended] to report it.
    ///
    /// The `checkpoint_requested` flag indicates that the controller is trying
    /// to checkpoint or suspend the pipeline. This serves as a hint to the reader
    /// to try to clear the checkpoint barrier by returning [Resume::Seek] or
    /// [Resume::Replay] if possible. For instance, if the reader has multiple
    /// buffers queued, it can choose to stop flushing them after reaching the first
    /// buffer that corresponds to a seekable position in the input stream.
    ///
    /// # Constraints
    ///
    /// The controller won't issue this command before it first issues [InputReaderCommand::Extend].
    Queue { checkpoint_requested: bool },

    /// Tells the reader it's going to be dropped soon and should clean up.
    ///
    /// The reader can continue to queue some data buffers afterward if that's
    /// the easiest implementation.
    ///
    /// # Constraints
    ///
    /// The controller calls this only once and won't call any other functions
    /// for a given reader after it calls this one.
    Disconnect,
}

impl InputReaderCommand {
    /// Returns this command translated to a [NonFtInputReaderCommand], or
    /// `None` if that is not possible (because this command is only for
    /// fault-tolerant endpoints).
    pub fn as_nonft(&self) -> Option<NonFtInputReaderCommand> {
        match self {
            InputReaderCommand::Replay { .. } => None,
            InputReaderCommand::Queue { .. } => Some(NonFtInputReaderCommand::Queue),
            InputReaderCommand::Extend => {
                Some(NonFtInputReaderCommand::Transition(PipelineState::Running))
            }
            InputReaderCommand::Pause => {
                Some(NonFtInputReaderCommand::Transition(PipelineState::Paused))
            }
            InputReaderCommand::Disconnect => Some(NonFtInputReaderCommand::Transition(
                PipelineState::Terminated,
            )),
        }
    }
}

/// A subset of [InputReaderCommand] that only includes the commands for
/// non-fault-tolerant connectors.
#[derive(Debug)]
pub enum NonFtInputReaderCommand {
    /// Equivalent to [InputReaderCommand::Queue].
    Queue,

    /// Equivalencies:
    ///
    /// - `Transition(PipelineState::Paused)`: [InputReaderCommand::Pause].
    ///
    /// - `Transition(PipelineState::Running)`: [InputReaderCommand::Extend].
    ///
    /// - `Transition(PipelineState::Terminated)`: [InputReaderCommand::Disconnect].
    Transition(PipelineState),
}

#[doc(hidden)]
pub struct InputQueueEntry<A, B> {
    /// Data buffer to push to the circuit.
    buffer: Option<B>,

    /// Time when data in this buffer was received from the transport endpoint.
    /// It is used to track the processing latency in different stages of the pipeline.
    timestamp: DateTime<Utc>,

    /// Start a transaction with the given label before pushing the buffer to the circuit
    /// unless a transaction is already in progress.
    start_transaction: Option<Option<String>>,

    /// Commit the transaction after pushing the buffer to the circuit if there is a transaction in progress.
    commit_transaction: bool,

    /// Auxiliary data associated with the buffer.
    aux: A,
}

impl<A, B> InputQueueEntry<A, B> {
    #[doc(hidden)]
    pub fn new_with_aux(timestamp: DateTime<Utc>, aux: A) -> Self {
        Self {
            buffer: None,
            timestamp,
            start_transaction: None,
            commit_transaction: false,
            aux,
        }
    }

    #[doc(hidden)]
    pub fn with_buffer(self, buffer: Option<B>) -> Self {
        Self { buffer, ..self }
    }

    /// Start a transaction with the given label before pushing the buffer to the circuit
    /// unless a transaction is already in progress.
    pub fn with_start_transaction(self, start_transaction: Option<Option<String>>) -> Self {
        Self {
            start_transaction,
            ..self
        }
    }

    /// Commit the transaction after pushing the buffer to the circuit if there is a transaction in progress.
    pub fn with_commit_transaction(self, commit_transaction: bool) -> Self {
        Self {
            commit_transaction,
            ..self
        }
    }
}

/// A thread-safe queue for collecting and flushing input buffers.
///
/// Commonly used by `InputReader` implementations for staging buffers from
/// worker threads.
pub struct InputQueue<A = (), B = Box<dyn InputBuffer>> {
    #[allow(clippy::type_complexity)]
    pub queue: Mutex<VecDeque<InputQueueEntry<A, B>>>,
    pub consumer: Box<dyn InputConsumer>,
    pub transaction_in_progress: AtomicBool,

    /// A transaction boundary that the connector requested and the pipeline
    /// has not reached yet.  The queue flushes nothing until it does.
    pending_boundary: Mutex<Pending>,
}

/// How long a connector may wait for a transaction boundary before
/// [InputQueue] reports it.  It reports it again each time the wait doubles.
const BOUNDARY_WARNING_THRESHOLD: Duration = Duration::from_secs(60);

#[cfg(test)]
thread_local! {
    /// The time that [now] returns on this thread, if a test set one.
    static FAKE_NOW: std::cell::Cell<Option<Instant>> = const { std::cell::Cell::new(None) };
}

/// Returns the current time, which a test can replace with [FAKE_NOW] so that
/// it does not depend on how fast it runs.
fn now() -> Instant {
    #[cfg(test)]
    if let Some(now) = FAKE_NOW.with(|now| now.get()) {
        return now;
    }
    Instant::now()
}

/// A pending transaction boundary, with how long the connector has waited.
struct Pending {
    boundary: PendingBoundary,
    wait: LongOperationWarning,
}

impl Pending {
    fn new(boundary: PendingBoundary) -> Self {
        Self {
            boundary,
            wait: LongOperationWarning::new_at(now(), BOUNDARY_WARNING_THRESHOLD),
        }
    }
}

/// A transaction boundary that the pipeline has not reached yet (see
/// [InputConsumer::open_transaction]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum PendingBoundary {
    /// No boundary is pending.
    #[default]
    None,

    /// The connector requested a transaction, which the pipeline has not
    /// opened yet.
    Open,

    /// The connector committed transaction `id`, which the pipeline has not
    /// closed yet.
    Close(TransactionId),
}

impl<A, B: InputBuffer> InputQueue<A, B> {
    pub fn new(consumer: Box<dyn InputConsumer>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            consumer,
            transaction_in_progress: AtomicBool::new(false),
            pending_boundary: Mutex::new(Pending::new(PendingBoundary::None)),
        }
    }

    fn set_pending_boundary(&self, boundary: PendingBoundary) {
        *self.pending_boundary.lock().unwrap() = Pending::new(boundary);
    }

    /// Returns true if the connector must not flush now, because the pipeline
    /// has not reached the transaction boundary that the connector requested.
    ///
    /// The wait ends without anything here doing more, because:
    ///
    /// - The connector's request is already with the pipeline: the queue
    ///   passes a request to the consumer before it starts to wait.  A
    ///   multihost host reports its connectors' requests to the coordinator.
    ///
    /// - The coordinator opens a transaction whenever a host reports a request,
    ///   and commits it once no host reports one, between steps and also while
    ///   the pipeline is paused.
    ///
    /// - The records that wait still count as buffered, so the host requests
    ///   another step, which flushes them once the boundary is reached.
    ///
    /// - The coordinator commits a transaction with steps that flush no input,
    ///   so no records can land in a transaction that is committing.
    ///
    fn awaiting_boundary(&self) -> bool {
        let mut pending = self.pending_boundary.lock().unwrap();
        if pending.boundary == PendingBoundary::None {
            return false;
        }
        let reached = match (pending.boundary, self.consumer.open_transaction()) {
            (PendingBoundary::None, _) | (_, None) => true,
            (PendingBoundary::Open, Some(open)) => open.is_some(),
            // A transaction that opens right after the commit is a different
            // transaction, so its records may land in it.
            (PendingBoundary::Close(id), Some(open)) => open != Some(id),
        };
        if reached {
            *pending = Pending::new(PendingBoundary::None);
        }
        !reached
    }

    /// If the connector has input that waits for a transaction boundary, and
    /// has waited longer than [BOUNDARY_WARNING_THRESHOLD], reports it to the
    /// consumer as a non-fatal error, and again each time the wait doubles.
    ///
    /// A connector with nothing queued holds nothing back, so a long wait is
    /// normal: for example, a connector that commits its part of a
    /// transaction early waits for the transaction's other connectors to
    /// finish theirs.
    ///
    /// The caller must not hold the lock on `queue`.
    fn report_long_wait(&self) {
        if self.queue.lock().unwrap().is_empty() {
            return;
        }
        let mut pending = self.pending_boundary.lock().unwrap();
        let boundary = pending.boundary;
        if boundary == PendingBoundary::None {
            return;
        }
        pending.wait.check_at(now(), |elapsed| {
            let waiting_for = match boundary {
                PendingBoundary::Close(id) => format!("to commit transaction {id}"),
                _ => "to open the transaction that it requested".to_string(),
            };
            self.consumer.error(
                false,
                anyhow::anyhow!(
                    "this connector has waited {} seconds for the pipeline {waiting_for}, and it reads no more input until then",
                    elapsed.as_secs()
                ),
                Some("transaction_boundary"),
            );
        });
    }

    pub fn push_entry(&self, entry: InputQueueEntry<A, B>, errors: Vec<ParseError>) {
        self.consumer.parse_errors(errors);
        let len = entry
            .buffer
            .as_ref()
            .map_or(BufferSize::empty(), |buffer| buffer.len());

        let mut queue = self.queue.lock().unwrap();
        queue.push_back(entry);
        self.consumer.buffered(len);

        // The endpoint pushed an empty buffer. This likely indicates that the accompanying aux data
        // needs to be processed by the endpoint after preceding buffers have been flushed. However,
        // since we didn't report any buffered records, the controller may never perform another step,
        // so we nudge it to do it.
        if len.records == 0 {
            self.consumer.request_step();
        }
    }

    /// Appends `buffer`, to the queue, and associates it with `aux`.  Reports
    /// to the controller that `errors` have occurred during parsing.
    pub fn push_with_aux(
        &self,
        (buffer, errors): (Option<B>, Vec<ParseError>),
        timestamp: DateTime<Utc>,
        aux: A,
    ) {
        let entry = InputQueueEntry::new_with_aux(timestamp, aux).with_buffer(buffer);

        self.push_entry(entry, errors);
    }

    /// Runs `release` on the auxiliary data of every queued entry, leaving the
    /// entries themselves queued.
    ///
    /// A reader that is shutting down calls this to release whatever the
    /// auxiliary data owns, so that a producer waiting on a queued entry is
    /// told rather than left waiting.
    ///
    /// The entries stay queued because their records were reported to the
    /// consumer by [`Self::push_entry`] and only a flush credits them back
    /// through [`InputConsumer::extended`]. Discarding them would leave the
    /// endpoint charged with records no step can consume, and the controller
    /// keeps stepping for as long as an endpoint it polls reports records
    /// buffered.
    ///
    /// `release` runs under the queue lock, so it must not queue anything.
    pub fn release_aux(&self, mut release: impl FnMut(&mut A)) {
        for entry in self.queue.lock().unwrap().iter_mut() {
            release(&mut entry.aux);
        }
    }

    /// Flushes a batch of records to the circuit and returns the auxiliary data
    /// that was associated with those records.
    ///
    /// This always flushes whole buffers to the circuit (with `flush`),
    /// since auxiliary data is associated with a whole buffer rather than with
    /// individual records. If the auxiliary data type `A` is `()`, then
    /// [InputQueue<()>::flush] avoids that and so is a better choice.
    #[allow(clippy::type_complexity)]
    pub fn flush_with_aux(&self) -> (BufferSize, Option<Xxh3Default>, Vec<(DateTime<Utc>, A)>) {
        self.flush_with_aux_until(&|_| false)
    }

    /// Flushes a batch of records to the circuit and returns the auxiliary data
    /// that was associated with those records.
    ///
    /// Stops after flushing at least `max_batch_size` records or after flushing a
    /// buffer whose auxiliary data satisfies the `stop_at` predicate, whichever
    /// happens first.
    ///
    /// This always flushes whole buffers to the circuit (with `flush`),
    /// since auxiliary data is associated with a whole buffer rather than with
    /// individual records. If the auxiliary data type `A` is `()`, then
    /// [InputQueue<()>::flush] avoids that and so is a better choice.
    #[allow(clippy::type_complexity)]
    pub fn flush_with_aux_until(
        &self,
        stop_at: &dyn Fn(&A) -> bool,
    ) -> (BufferSize, Option<Xxh3Default>, Vec<(DateTime<Utc>, A)>) {
        let mut total = BufferSize::empty();
        let mut hasher = self.consumer.hasher();
        let n = self.consumer.max_batch_size();
        let mut consumed_aux = Vec::new();

        let mut stop = false;

        while !stop && total.records < n && !self.awaiting_boundary() {
            let Some(InputQueueEntry {
                buffer,
                timestamp,
                aux,
                start_transaction,
                commit_transaction,
            }) = self.queue.lock().unwrap().pop_front()
            else {
                break;
            };

            if let Some(label) = start_transaction {
                self.start_transaction(label.as_deref());
                if self.awaiting_boundary() {
                    // Keep the entry, without its request, for when the
                    // transaction opens.
                    self.queue.lock().unwrap().push_front(InputQueueEntry {
                        buffer,
                        timestamp,
                        aux,
                        start_transaction: None,
                        commit_transaction,
                    });
                    stop = true;
                    break;
                }
            }

            if let Some(mut buffer) = buffer {
                total += buffer.len();
                if let Some(hasher) = hasher.as_mut() {
                    buffer.hash(hasher);
                }
                buffer.flush();
            }

            stop = stop_at(&aux);
            consumed_aux.push((timestamp, aux));

            if commit_transaction && self.commit_transaction() {
                break;
            }
        }

        // Process any entries with aux data only.
        let mut queue = self.queue.lock().unwrap();
        while !stop
            && !self.awaiting_boundary()
            && queue
                .front()
                .is_some_and(|InputQueueEntry { buffer, .. }| buffer.is_none())
        {
            let Some(InputQueueEntry {
                timestamp,
                aux,
                start_transaction,
                commit_transaction,
                ..
            }) = queue.pop_front()
            else {
                break;
            };

            if let Some(label) = start_transaction {
                self.start_transaction(label.as_deref());
                if self.awaiting_boundary() {
                    queue.push_front(InputQueueEntry {
                        buffer: None,
                        timestamp,
                        aux,
                        start_transaction: None,
                        commit_transaction,
                    });
                    break;
                }
            }

            stop = stop_at(&aux);
            consumed_aux.push((timestamp, aux));

            if commit_transaction && self.commit_transaction() {
                break;
            }
        }
        drop(queue);
        self.report_long_wait();

        (total, hasher, consumed_aux)
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn start_transaction(&self, label: Option<&str>) -> bool {
        if self
            .transaction_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.consumer.start_transaction(label);
            self.set_pending_boundary(PendingBoundary::Open);
            true
        } else {
            false
        }
    }

    fn commit_transaction(&self) -> bool {
        if self
            .transaction_in_progress
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.consumer.commit_transaction();
            self.pending_close();
            true
        } else {
            false
        }
    }

    /// Records that the connector committed the transaction that is open now,
    /// if any.
    fn pending_close(&self) {
        self.set_pending_boundary(match self.consumer.open_transaction() {
            Some(Some(id)) => PendingBoundary::Close(id),
            _ => PendingBoundary::None,
        });
    }
}

impl InputQueue<(), Box<dyn InputBuffer>> {
    /// Appends `buffer`, if nonempty,` to the queue.  Reports to the controller
    /// that `errors` occurred during parsing.
    pub fn push(
        &self,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        timestamp: DateTime<Utc>,
    ) {
        self.push_with_aux((buffer, errors), timestamp, ())
    }

    /// Flushes a batch of records to the circuit and reports to the consumer
    /// that it was done.
    ///
    /// Only non-fault-tolerant input adapters can use this.
    pub fn queue(&self) {
        let mut total = BufferSize::empty();
        let n = self.consumer.max_batch_size();
        let mut consumed = Vec::new();

        while total.records < n && !self.awaiting_boundary() {
            let Some(InputQueueEntry {
                buffer,
                timestamp,
                start_transaction,
                commit_transaction,
                ..
            }) = self.queue.lock().unwrap().pop_front()
            else {
                break;
            };

            if let Some(label) = start_transaction {
                self.start_transaction(label.as_deref());
                if self.awaiting_boundary() {
                    // Keep the entry, without its request, for when the
                    // transaction opens.
                    self.queue.lock().unwrap().push_front(InputQueueEntry {
                        buffer,
                        timestamp,
                        start_transaction: None,
                        commit_transaction,
                        aux: (),
                    });
                    break;
                }
            }

            if let Some(mut buffer) = buffer {
                let mut taken = buffer.take_some(n - total.records);
                total += taken.len();
                consumed.push(Watermark::new(timestamp, None));
                taken.flush();
                drop(taken);
                if !buffer.is_empty() {
                    self.queue.lock().unwrap().push_front(InputQueueEntry {
                        buffer: Some(buffer),
                        timestamp,
                        start_transaction: None,
                        commit_transaction,
                        aux: (),
                    });
                    break;
                }
            }

            if commit_transaction {
                self.commit_transaction();
                break;
            }
        }
        self.report_long_wait();
        self.consumer.extended(total, None, consumed);
    }
}

/// Reads data from an endpoint.
///
/// Use [`TransportInputEndpoint::open`] to obtain an [`InputReader`].
pub trait InputReader: Send + Sync {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync>;

    /// Requests the input reader to execute `command`.
    fn request(&self, command: InputReaderCommand);

    /// Returns true if the endpoint is closed, meaning that it has already
    /// acted on all of the commands that it ever will. A closed endpoint can be
    /// one that came to the end of its input (and is not waiting for more to
    /// arrive) or one that encountered a fatal error and cannot continue.
    ///
    /// An endpoint is often implemented in terms of a channel to a thread. In
    /// such a case, this can be implemented in terms of `is_closed` on the
    /// channel's sender.
    fn is_closed(&self) -> bool;

    fn replay(&self, metadata: JsonValue, data: RmpValue) {
        self.request(InputReaderCommand::Replay { metadata, data });
    }

    fn extend(&self) {
        self.request(InputReaderCommand::Extend);
    }

    fn pause(&self) {
        self.request(InputReaderCommand::Pause);
    }

    fn queue(&self, checkpoint_requested: bool) {
        self.request(InputReaderCommand::Queue {
            checkpoint_requested,
        });
    }

    fn disconnect(&self) {
        self.request(InputReaderCommand::Disconnect);
    }

    /// Returns the approximate amount of memory used by the connector's
    /// underlying implementation.  For the Kafka connectors, for example, this
    /// is the amount of memory used by librdkafka.  Not all connectors use a
    /// substantial amount of memory, so the default implementation returns 0.
    fn memory(&self) -> usize {
        0
    }
}

/// Position in an input stream, including the timestamp when the data was ingested
/// from the transport endpoint and transport-specific metadata such as delta table
/// version or Kafka partition offsets.
#[derive(Clone, Debug)]
pub struct Watermark {
    pub timestamp: DateTime<Utc>,
    pub metadata: Option<JsonValue>,
}

impl Watermark {
    pub fn new(timestamp: DateTime<Utc>, metadata: Option<JsonValue>) -> Self {
        Self {
            timestamp,
            metadata,
        }
    }
}

/// Input stream consumer.
///
/// A transport endpoint pushes binary data downstream via an instance of this
/// trait.
pub trait InputConsumer: Send + Sync + DynClone {
    /// Returns the maximum number of records that an `InputReader` should queue
    /// in response to a [InputReaderCommand::Queue] command.
    ///
    /// Nothing keeps the endpoint from queuing more than this if necessary (for
    /// example, if for the sake of lateness it needs to group more than this
    /// number of records together).
    fn max_batch_size(&self) -> usize;

    /// Returns the level of fault tolerance that the pipeline supports, if any.
    ///
    /// An endpoint only needs to implement `min(endpoint_ft, pipeline_ft)`
    /// fault tolerance, where `endpoint_ft` is what the endpoint returns from
    /// `InputEndpoint::fault_tolerance` and `pipeline_ft` is what this function
    /// returns.  For example, if an input adapter supports
    /// `Some(FtModel::ExactlyOnce)`, but the pipeline's fault tolerance level
    /// is `None`, then the input adapter can simply pass `None` as `resume` to
    /// [InputConsumer::extended].  This optimization is, probably, worthwhile
    /// only to input adapters that log a copy of all of their data, instead of
    /// just metadata.
    fn pipeline_fault_tolerance(&self) -> Option<FtModel>;

    /// Returns a hasher, if the fault tolerance model calls for hashing, and
    /// `None` otherwise.
    ///
    /// This is just a convenience method.  Connectors can do hashing any way
    /// they like, as long as they do it the same way for new data and for
    /// replays.
    fn hasher(&self) -> Option<Xxh3Default> {
        match self.pipeline_fault_tolerance() {
            Some(FtModel::ExactlyOnce) => Some(Xxh3Default::new()),
            _ => None,
        }
    }

    /// Reports `errors` as parse errors.
    fn parse_errors(&self, errors: Vec<ParseError>);

    /// Reports that the input adapter has internally buffered `amt` records and
    /// bytes.
    ///
    /// Fault-tolerant input adapters should report buffered data during replay
    /// as well as in normal operation.
    fn buffered(&self, amt: BufferSize);

    /// Reports that the input adapter has completed flushing `amt` data to the
    /// circuit, that hash to `hash`, in response to an
    /// [InputReaderCommand::Replay] request.
    ///
    /// Only a fault-tolerant input adapter will invoke this.
    fn replayed(&self, amt: BufferSize, hash: u64);

    /// Reports that the input adapter has completed flushing `amt` data to the
    /// circuit, that hash to `hash`, in response to an
    /// [InputReaderCommand::Queue] request.
    ///
    /// If the step is one that the input adapter can restart after, or replay,
    /// then it should supply that as `resume` (see [Resume] for details).
    fn extended(&self, amt: BufferSize, resume: Option<Resume>, watermarks: Vec<Watermark>);

    /// Reports that the endpoint has reached end of input and that no more data
    /// will be received from the endpoint.
    ///
    /// If the endpoint has already indicated that it has buffered records then
    /// the controller will request them in future [InputReaderCommand::Queue]
    /// messages. The endpoint must not make further calls to
    /// [InputConsumer::buffered] or [InputConsumer::parse_errors].
    fn eoi(&self);

    /// Request the controller to schedule a step even if the connector hasn't queued
    /// any records.
    fn request_step(&self);

    /// The connector is initiating a transaction. `label` is an optional label for
    /// the transaction for debugging purposes.
    ///
    /// This function can be invoked in response to a `Queue` command.
    ///
    /// Any updates pushed by the connector after this function is invoked will be part
    /// of the transaction.
    ///
    /// The connector _must_ perform a matching call to `commit_transaction` to commit the
    /// transaction.
    ///
    /// Multiple connectors can initiate a transaction concurrently, in which case their
    /// updates will be combined into a single transaction. The transaction will be committed
    /// when all the connectors have committed it.
    fn start_transaction(&self, label: Option<&str>);

    /// The connector is committing a transaction started by a previous `start_transaction` call.
    ///
    /// This function can be invoked in response to a `Queue` command after pushing all updates that
    /// belong to the the transaction, immediately before calling `extended`. The connector cannot
    /// queue any more updates after this function is invoked, until the next `Queue` command.
    fn commit_transaction(&self);

    /// Reports which transaction, if any, the records that the connector
    /// flushes now land in.  [InputQueue] uses this to keep each of a
    /// connector's records on the side of a transaction boundary that the
    /// connector requested.
    ///
    /// The answer differs between single-host and multihost pipelines:
    ///
    /// - A single-host pipeline acts on
    ///   [`start_transaction`](Self::start_transaction) and
    ///   [`commit_transaction`](Self::commit_transaction) at once, so records
    ///   that the connector flushes after a request already land on the
    ///   requested side of the boundary.  This returns `None`, and
    ///   [InputQueue] never holds records back.
    ///
    /// - A host of a multihost pipeline only reports its connectors' requests
    ///   to the coordinator, which opens and commits the transaction on every
    ///   host later, between steps.  Records flushed before then would land on
    ///   the wrong side of the boundary.  This returns `Some(Some(id))` while
    ///   transaction `id` is open and `Some(None)` while no transaction is
    ///   open.  After a request, [InputQueue] holds the connector's records
    ///   until the coordinator has acted on it: after a start, until a
    ///   transaction is open; after a commit of transaction `id`, until `id`
    ///   is no longer open.
    ///
    /// The default implementation returns `None`, which is right for a
    /// single-host pipeline.
    fn open_transaction(&self) -> Option<Option<TransactionId>> {
        None
    }

    /// Register connector-specific metrics for Prometheus export.
    ///
    /// A connector may call this once during [`TransportInputEndpoint::open`]
    /// to provide an [`Arc<dyn ConnectorMetrics>`] whose [`ConnectorMetrics::metrics`]
    /// will be polled on every scrape.  The default implementation is a no-op,
    /// so connectors that have no custom metrics need not override it.
    fn set_custom_metrics(&self, _metrics: Arc<dyn ConnectorMetrics>) {}

    /// Returns a watch receiver that fires each time pipeline step processing
    /// completes.  The value is the count of fully-processed steps
    /// (`total_completed_steps`): a record ingested in step `n` is done when
    /// the value exceeds `n`.
    ///
    /// Input adapters can use this to defer acknowledgment (e.g. a CDC
    /// connector holding a replication slot) until their data has been
    /// processed by the circuit and all output connectors.
    ///
    /// Returns `None` if the consumer does not support completion tracking.
    fn completion_watcher(&self) -> Option<tokio::sync::watch::Receiver<Completion>>;

    /// Returns a watch receiver that fires each time a durable checkpoint
    /// completes.  The value is the count of checkpointed steps: a record
    /// ingested in step `n` is durably stored when the value exceeds `n`.
    ///
    /// Input adapters that require at-least-once delivery stronger than step
    /// completion (e.g. a CDC connector that must not advance its replication
    /// slot past the last checkpoint) can wait on this rather than on
    /// [`completion_watcher`][Self::completion_watcher].
    ///
    /// Returns `Some` only when fault tolerance is enabled for the pipeline
    /// (which implies storage is configured and checkpoints are scheduled).
    /// Returns `None` otherwise, in which case the connector should fall back
    /// to [`completion_watcher`][Self::completion_watcher].
    fn checkpoint_watcher(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        None
    }

    /// The step the controller is building, which is the step that data
    /// flushed in answer to the current [`InputReaderCommand::Queue`] lands in.
    ///
    /// An adapter that defers acknowledgment needs this rather than
    /// `total_completed_steps`, which is a minimum over the output connectors
    /// and so only a lower bound on the step being fed: a lagging output makes
    /// it name a step whose checkpoint predates the rows.
    ///
    /// A consumer that returns `Some` from [`completion_watcher`] or
    /// [`checkpoint_watcher`] must return `Some` here while handling a `Queue`
    /// command. Those two say that acknowledgment can be deferred, and this
    /// says what it waits for; an adapter offered the first two without this
    /// cannot tell which step holds its rows, and may refuse to run rather
    /// than acknowledge them against the wrong one.
    ///
    /// The value is meaningful only while handling a `Queue` command. Returns
    /// `None` if the consumer does not track steps.
    ///
    /// [`completion_watcher`]: Self::completion_watcher
    /// [`checkpoint_watcher`]: Self::checkpoint_watcher
    fn current_step(&self) -> Option<Step> {
        None
    }

    /// Endpoint failed.
    ///
    /// Reports that the endpoint failed and that it will not queue any more
    /// data.
    ///
    /// Optional tag that can be used for additional context
    /// e.g. for rate limiting
    fn error(&self, fatal: bool, error: AnyError, tag: Option<&'static str>);

    /// Updates the health status of the connector.
    fn update_connector_health(&self, health: ConnectorHealth);
}

/// Information needed to restart after or replay input.
///
/// Feldera supports a few ways to checkpoint and resume a pipeline.  These
/// operations in turn require support from the pipeline's input adapters:
///
/// 1. To support suspend and resume, or at-least-once fault tolerance, the
///    input adapter must indicate, per step, how to restart from just after
///    that step, by passing `Some(Resume::*)` to [InputConsumer::extended].
///
///    Such input adapters might have steps for which seeking would be
///    impractical.  Such an input adapter may skip over those steps by passing
///    `Some(Resume::Barrier)` instead; the controller will not try to
///    checkpoint after them.
///
/// 2. To additionally support exactly once fault tolerance, the input adapter
///    must indicate, per step, both how to restart after the step and how to
///    replay exactly that step, by passing `Some(Resume::Replay { .. })` to
///    [InputConsumer::extended].
///
///    An input adapter that supports fault tolerance may not skip steps; that
///    is, it must supply `Some(Resume::Replay { .. })` for every step.
#[derive(Clone, Debug)]
pub enum Resume {
    /// The input adapter does not support resuming after this step.
    Barrier,

    /// The input adapter can resume just after this step, but it can't replay
    /// the step exactly.
    Seek {
        /// Metadata needed for the controller to restart the input adapter from
        /// just after this input step.
        seek: JsonValue,
    },

    /// The input adapter can replay this step exactly, or resume just after the
    /// step.
    ///
    /// Input adapters can use `seek` and `replay` in different combinations:
    ///
    /// - Some kinds of input adapters, for example the ones for files, or for
    ///   Kafka, can reread the input data that they used before.  These will
    ///   ordinarily just use `seek`, filling it with a pointer just past the
    ///   end of the data to be read. (Ordinarily, it would already know where
    ///   the start is from the previous step, so the start pointer isn't
    ///   usually needed.)
    ///
    ///   These input adapters can just set `replay` to [RmpValue::Nil].
    ///
    /// - Other kinds of input connectors can't seek back and reread the input
    ///   data that they used before. The best example is the HTTP input
    ///   connector, because which can't ask whatever client connected before to
    ///   repeat the same exact data that it input before.  These input
    ///   connectors have to save all the input data for replay, by putting into
    ///   the `replay` field.
    ///
    ///   These input adapters can just set `seek` to [JsonValue::Null].
    ///
    ///   (In theory, any input connector could substitute data for metadata,
    ///   but if the data can simply be reread using the metadata, we usually
    ///   consider that better because it saves time and space saving all the
    ///   data when in most cases it will never be reread.)
    Replay {
        /// Metadata needed for the controller to restart the input adapter from
        /// just after this input step.
        seek: JsonValue,

        /// The data needed for the controller to replay exactly this input step
        /// using [InputReaderCommand::Replay].
        replay: RmpValue,

        /// Hash of the input records in this step, for verification on replay.
        ///
        /// The input adapter can compute this in any way convenient to it, as
        /// long as it does so the same way for reading data initially and on
        /// replay.  On replay, the controller checks that the replayed value
        /// matches the original one and fails the circuit if it differs.
        hash: u64,
    },
}

impl Resume {
    pub fn is_barrier(&self) -> bool {
        matches!(self, Self::Barrier)
    }

    /// Returns the `seek` value, if any, in this [Resume].
    pub fn seek(&self) -> Option<&JsonValue> {
        match self {
            Resume::Barrier => None,
            Resume::Seek { seek } | Resume::Replay { seek, .. } => Some(seek),
        }
    }

    /// Consumes this [Resume] and returns just the `seek` value, if any.
    pub fn into_seek(self) -> Option<JsonValue> {
        match self {
            Resume::Barrier => None,
            Resume::Seek { seek } | Resume::Replay { seek, .. } => Some(seek),
        }
    }

    /// Returns the maximum fault tolerance level that this [Resume] can
    /// support.
    pub fn fault_tolerance(&self) -> FtModel {
        match self {
            &Resume::Barrier | Resume::Seek { .. } => FtModel::AtLeastOnce,
            Resume::Replay { .. } => FtModel::ExactlyOnce,
        }
    }

    /// If `hash` is provided, returns `Resume::Replay` with its hash value and
    /// `seek`; otherwise, returns `Resume::Seek` with `seek`.
    ///
    /// This is convenient for endpoints that only need to use metadata to
    /// support journaling. [InputConsumer::hasher] can be a convenient way to
    /// get a hasher.
    pub fn new_metadata_only(seek: JsonValue, hash: Option<u64>) -> Self {
        match hash {
            Some(hash) => Self::Replay {
                seek,
                replay: RmpValue::Nil,
                hash,
            },
            None => Self::Seek { seek },
        }
    }

    /// If `hash` is provided, returns `Resume::Replay` with its hash value and
    /// whatever `replay` returns; otherwise, returns `Resume::Seek`.
    ///
    /// This is convenient for endpoints that support journaling by journaling
    /// all the data (and that don't need to journal any metadata).
    /// [InputConsumer::hasher] can be a convenient way to get a hasher.
    pub fn new_data_only<F>(replay: F, hash: Option<u64>) -> Self
    where
        F: FnOnce() -> RmpValue,
    {
        let seek = JsonValue::Null;
        match hash {
            Some(hash) => Self::Replay {
                seek,
                replay: replay(),
                hash,
            },
            None => Self::Seek { seek },
        }
    }
}

dyn_clone::clone_trait_object!(InputConsumer);

/// Helper function to parse resume info passed to [`InputConsumer::extended`].
pub fn parse_resume_info<M>(metadata: &JsonValue) -> AnyResult<M>
where
    M: DeserializeOwned,
{
    serde_json_path_to_error::from_value::<M>(metadata.clone())
            .map_err(|e| anyhow::anyhow!("unable to parse checkpointed connector state (checkpointed state: {metadata}; parse error: {e})"))
}

#[doc(hidden)]
pub type AsyncErrorCallback = Box<dyn Fn(bool, AnyError, Option<&'static str>) + Send + Sync>;

/// Command handler API exposed by connectors.
///
/// Connectors can support arbitrary connector-specific commands that can be
/// invoked via the `/command` endpoint. These commands take and return arbitrary
/// JSON values.
///
/// This API is not part of trait `Output[Input]Endpoint` because it can be invoked
/// from any thread, and requires `Send + Sync`, while the `OutputEndpoint` API is
/// not `Sync` and is meant to be called from the controller thread only.
///
/// The idea is that connectors that support custom commands create separate command
/// handler objects that implement this trait and are returned by
/// `OutputEndpoint::command_handler`.
pub trait CommandHandler: Send + Sync {
    /// Handle a command specified by the JSON objest.
    ///
    /// Fails if the connector does not support the command, the command is invalid,
    /// or command execution fails.
    fn command(&self, command: serde_json::Value) -> AnyResult<serde_json::Value>;
}

/// Distinguishes a full-materialized-view snapshot from an incremental delta
/// when pushed to an output connector.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OutputBatchType {
    Delta,
    Snapshot,
}

/// A configured output transport endpoint.
///
/// Output endpoints come in two flavors:
///
/// * A [fault-tolerant](crate#fault-tolerance) endpoint accepts output that has
///   been divided into numbered steps.  If it is given output associated with a
///   step number that has already been output, then it discards the duplicate.
///   It must also keep data written to the output transport from becoming
///   visible to downstream readers until `batch_end` is called.  (This works
///   for output to Kafka, which supports transactional output.  If it is
///   difficult for some future fault-tolerant output endpoint, then the API
///   could be adjusted to support writing output only after it can become
///   immediately visible.)
///
/// * A non-fault-tolerant endpoint does not have a concept of steps and ignores
///   them.
///
/// # Every wait must end when the pipeline goes down
///
/// Every method here runs on the controller's output thread for the endpoint.
/// That thread is an auxiliary thread of the DBSP runtime, and tearing the
/// pipeline down joins it.  So an unbounded wait in any of them - a retry loop,
/// a full queue, an acknowledgement from a client, a lock held by something
/// else - hangs the teardown, and the teardown can be the very thing that would
/// have released the wait.
///
/// An endpoint that waits on anything outside the pipeline therefore takes a
/// `CancellationToken` from `ControllerInner::shutdown_token`, and either polls
/// it between attempts or awaits it alongside the operation with
/// `CancellationToken::run_until_cancelled`.  The Delta, Kafka, DynamoDB and
/// HTTP sinks are worked examples.  `Encoder` implementations inherit the rule,
/// since they run on the same thread.
///
/// A bounded wait is not an exception worth making: "bounded" here has to mean
/// short enough to sit inside a shutdown, and a retry budget measured in
/// minutes does not.
pub trait OutputEndpoint: Send {
    fn command_handler(&self) -> Option<Arc<dyn CommandHandler>> {
        None
    }

    /// Finishes establishing the connection to the output endpoint.
    ///
    /// If the endpoint encounters any errors during output, now or later, it
    /// invokes `async_error_callback` to notify the client about asynchronous
    /// errors, i.e., errors that happen outside the context of the
    /// [`OutputEndpoint::push_buffer`] method. For instance, a reliable message
    /// bus like Kafka may notify the endpoint about a failure to deliver a
    /// previously sent message via an async callback. If the endpoint is unable
    /// to handle this error, it must forward it to the client via the
    /// `async_error_callback`.  The first argument of the callback is a flag
    /// that indicates a fatal error that the endpoint cannot recover from.
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()>;

    /// Maximum buffer size that this transport can transmit.
    /// The encoder should not generate buffers exceeding this size.
    fn max_buffer_size_bytes(&self) -> usize;

    /// Notifies the output endpoint that data subsequently written by
    /// `push_buffer` belong to the given `step`.
    ///
    /// A [fault-tolerant](crate#fault-tolerance) endpoint has additional
    /// requirements:
    ///
    /// 1. If data for the given step has been written before, the endpoint
    ///    should discard it.
    ///
    /// 2. The output batch must not be made visible to downstream readers
    ///    before the next call to `batch_end`.
    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        Ok(())
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()>;

    /// Output a message consisting of a key/value pair, with optional headers.
    ///
    /// This API is implemented by Kafka and other transports that transmit
    /// messages consisting of key and value fields and is invoked by
    /// Kafka-specific data formats that rely on this message structure,
    /// e.g., Debezium. If a given transport does not implement this API, it
    /// should return an error.
    ///
    /// `headers` contains a list of key/optional_value pairs to be appended
    /// to Kafka message headers.
    fn push_key(
        &mut self,
        key: Option<&[u8]>,
        val: Option<&[u8]>,
        headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()>;

    /// Notifies the output endpoint that output for the current step is
    /// complete.
    ///
    /// A fault-tolerant output endpoint may now make the output batch visible
    /// to readers.
    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())
    }

    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    fn is_fault_tolerant(&self) -> bool;

    /// Returns the approximate amount of memory used by the connector's
    /// underlying implementation.  For the Kafka connectors, for example, this
    /// is the amount of memory used by librdkafka.  Not all connectors use a
    /// substantial amount of memory, so the default implementation returns 0.
    fn memory(&self) -> usize {
        0
    }
}

/// An [UnboundedReceiver] wrapper for [InputReaderCommand] for fault-tolerant connectors.
///
/// A fault-tolerant connector wants to receive, in order:
///
/// - Zero or more [InputReaderCommand::Replay]s.
///
/// - Zero or more other commands.
///
/// This helps with that.
// This is used by Kafka and Nexmark but both of those are optional.
pub struct InputCommandReceiver<M, D> {
    receiver: UnboundedReceiver<InputReaderCommand>,
    buffer: Option<InputReaderCommand>,
    _phantom: PhantomData<(M, D)>,
}

/// Error type returned by some [InputCommandReceiver] methods.
///
/// We could just use `anyhow` and that would probably be just as good though.
#[derive(Debug)]
pub enum InputCommandReceiverError {
    Disconnected,
    JsonDecodeError(serde_json_path_to_error::Error),
    RmpDecodeError(RmpDecodeError),
}

impl std::error::Error for InputCommandReceiverError {}

impl Display for InputCommandReceiverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputCommandReceiverError::Disconnected => write!(f, "sender disconnected"),
            InputCommandReceiverError::RmpDecodeError(e) => e.fmt(f),
            InputCommandReceiverError::JsonDecodeError(e) => e.fmt(f),
        }
    }
}

impl From<RmpDecodeError> for InputCommandReceiverError {
    fn from(value: RmpDecodeError) -> Self {
        Self::RmpDecodeError(value)
    }
}

impl From<serde_json_path_to_error::Error> for InputCommandReceiverError {
    fn from(value: serde_json_path_to_error::Error) -> Self {
        Self::JsonDecodeError(value)
    }
}

// This is used by Kafka and Nexmark but both of those are optional.
impl<M, D> InputCommandReceiver<M, D> {
    pub fn new(receiver: UnboundedReceiver<InputReaderCommand>) -> Self {
        Self {
            receiver,
            buffer: None,
            _phantom: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn blocking_recv_replay(&mut self) -> Result<Option<(M, D)>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
        D: for<'a> Deserialize<'a>,
    {
        let command = self.blocking_recv()?;
        self.take_replay(command)
    }

    #[doc(hidden)]
    pub async fn recv_replay(&mut self) -> Result<Option<(M, D)>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
        D: for<'a> Deserialize<'a>,
    {
        let command = self.recv().await?;
        self.take_replay(command)
    }

    fn take_replay(
        &mut self,
        command: InputReaderCommand,
    ) -> Result<Option<(M, D)>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
        D: for<'a> Deserialize<'a>,
    {
        match command {
            InputReaderCommand::Replay { metadata, data } => Ok(Some((
                serde_json_path_to_error::from_value::<M>(metadata)?,
                rmpv::ext::from_value::<D>(data)?,
            ))),
            other => {
                self.put_back(other);
                Ok(None)
            }
        }
    }

    #[doc(hidden)]
    pub async fn recv(&mut self) -> Result<InputReaderCommand, InputCommandReceiverError> {
        match self.buffer.take() {
            Some(value) => Ok(value),
            None => self
                .receiver
                .recv()
                .await
                .ok_or(InputCommandReceiverError::Disconnected),
        }
    }

    #[doc(hidden)]
    pub fn blocking_recv(&mut self) -> Result<InputReaderCommand, InputCommandReceiverError> {
        match self.buffer.take() {
            Some(value) => Ok(value),
            None => self
                .receiver
                .blocking_recv()
                .ok_or(InputCommandReceiverError::Disconnected),
        }
    }

    #[doc(hidden)]
    pub fn try_recv(&mut self) -> Result<Option<InputReaderCommand>, InputCommandReceiverError> {
        if let Some(command) = self.buffer.take() {
            Ok(Some(command))
        } else {
            match self.receiver.try_recv() {
                Ok(command) => Ok(Some(command)),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => Err(InputCommandReceiverError::Disconnected),
            }
        }
    }

    #[doc(hidden)]
    pub fn put_back(&mut self, value: InputReaderCommand) {
        assert!(self.buffer.is_none());
        self.buffer = Some(value);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::sync::mpsc::unbounded_channel;

    /// A fault-tolerant reader must not abandon an acknowledged replay step
    /// just because a control command arrives while the replay is in progress
    /// (see the `InputReaderCommand::Replay` docs: "The input reader doesn't
    /// have to process other commands while it does the replay."). The command
    /// receiver's single-slot buffer is what lets a reader park a control
    /// command and keep retrying the in-flight replay until it completes.
    ///
    /// This test drives the exact command interleaving the NATS input hit:
    /// a Replay, then a Pause arriving mid-replay. The Pause must stay
    /// buffered behind the replay (try_recv/put_back), never interrupt it, and
    /// only surface after the replay completes.
    #[test]
    fn control_command_does_not_interrupt_inflight_replay() {
        let (sender, receiver) = unbounded_channel();
        let mut commands = InputCommandReceiver::<serde_json::Value, rmpv::Value>::new(receiver);

        // A replay step, then a control command arriving while it is in flight.
        sender
            .send(InputReaderCommand::Replay {
                metadata: serde_json::json!({}),
                data: rmpv::Value::Nil,
            })
            .unwrap();
        sender.send(InputReaderCommand::Pause).unwrap();

        // The reader is mid-replay and polls for a *control* command without
        // consuming the replay. It must see the Replay first (FIFO), buffer it
        // back, and not yet observe the Pause.
        let first = commands.try_recv().unwrap().expect("a command is queued");
        assert!(matches!(first, InputReaderCommand::Replay { .. }));
        commands.put_back(first);

        // Polling again returns the buffered Replay, not the Pause — the
        // control command stays behind the in-flight replay.
        let again = commands.try_recv().unwrap().expect("buffered command");
        assert!(matches!(again, InputReaderCommand::Replay { .. }));
        commands.put_back(again);

        // Once the replay completes, the buffered command is drained and the
        // Pause is finally delivered.
        let replay = commands.try_recv().unwrap().unwrap();
        assert!(matches!(replay, InputReaderCommand::Replay { .. }));
        let pause = commands.try_recv().unwrap().unwrap();
        assert!(matches!(pause, InputReaderCommand::Pause));
    }
}

/// Tests that a connector's records land inside the transaction that it
/// requests, when the pipeline opens and closes transactions later than the
/// connector asks, as a multihost pipeline does.
#[cfg(test)]
mod transaction_boundary_tests {
    use std::hash::Hasher;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use anyhow::Error as AnyError;
    use chrono::Utc;
    use feldera_types::adapter_stats::ConnectorHealth;
    use feldera_types::config::FtModel;
    use feldera_types::coordination::Completion;

    use feldera_types::transaction::TransactionId;

    use super::{InputConsumer, InputQueue, InputQueueEntry, Resume, Watermark};
    use crate::format::{BufferSize, InputBuffer, ParseError};

    /// What the pipeline does with the connector's requests.
    #[derive(Default)]
    struct PipelineState {
        /// The transaction that the pipeline has open, if any.  Only the test
        /// changes this, as the coordinator would, after the connector asks.
        open: Option<TransactionId>,

        /// True if the pipeline acts on the connector's requests at once, as a
        /// single-host pipeline does.
        immediate: bool,

        /// Each record that the connector flushed, with whether a
        /// transaction was open when it did.
        flushed: Vec<(u32, bool)>,

        /// Errors that the connector reported.
        errors: Vec<String>,

        /// The number of requests to start a transaction.
        starts: usize,
    }

    /// A consumer that, like a host of a multihost pipeline, only records the
    /// connector's transaction requests.
    #[derive(Clone, Default)]
    struct Pipeline(Arc<Mutex<PipelineState>>);

    impl Pipeline {
        fn set_open(&self, open: Option<TransactionId>) {
            self.0.lock().unwrap().open = open;
        }

        /// Returns the records flushed so far, and forgets them.
        fn take_flushed(&self) -> Vec<(u32, bool)> {
            std::mem::take(&mut self.0.lock().unwrap().flushed)
        }

        fn starts(&self) -> usize {
            self.0.lock().unwrap().starts
        }
    }

    impl InputConsumer for Pipeline {
        fn max_batch_size(&self) -> usize {
            usize::MAX
        }
        fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
            None
        }
        fn parse_errors(&self, _errors: Vec<ParseError>) {}
        fn buffered(&self, _amt: BufferSize) {}
        fn replayed(&self, _amt: BufferSize, _hash: u64) {}
        fn extended(&self, _amt: BufferSize, _resume: Option<Resume>, _watermarks: Vec<Watermark>) {
        }
        fn eoi(&self) {}
        fn request_step(&self) {}
        fn start_transaction(&self, _label: Option<&str>) {
            let mut state = self.0.lock().unwrap();
            state.starts += 1;
            if state.immediate {
                state.open = Some(1);
            }
        }
        fn commit_transaction(&self) {
            let mut state = self.0.lock().unwrap();
            if state.immediate {
                state.open = None;
            }
        }
        fn open_transaction(&self) -> Option<Option<TransactionId>> {
            let state = self.0.lock().unwrap();
            (!state.immediate).then_some(state.open)
        }
        fn completion_watcher(&self) -> Option<tokio::sync::watch::Receiver<Completion>> {
            None
        }
        fn error(&self, _fatal: bool, error: AnyError, _tag: Option<&'static str>) {
            self.0.lock().unwrap().errors.push(error.to_string());
        }
        fn update_connector_health(&self, _health: ConnectorHealth) {}
    }

    /// Records with consecutive IDs.
    struct Records {
        ids: Vec<u32>,
        pipeline: Pipeline,
    }

    impl InputBuffer for Records {
        fn flush(&mut self) {
            let mut state = self.pipeline.0.lock().unwrap();
            let open = state.open.is_some();
            state
                .flushed
                .extend(self.ids.drain(..).map(|id| (id, open)));
        }
        fn len(&self) -> BufferSize {
            BufferSize {
                records: self.ids.len(),
                bytes: self.ids.len(),
            }
        }
        fn hash(&self, _hasher: &mut dyn Hasher) {}
        fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
            let n = n.min(self.ids.len());
            (n > 0).then(|| {
                Box::new(Records {
                    ids: self.ids.drain(..n).collect(),
                    pipeline: self.pipeline.clone(),
                }) as Box<dyn InputBuffer>
            })
        }
    }

    /// A queue entry with records `ids`.
    fn records(pipeline: &Pipeline, ids: &[u32]) -> InputQueueEntry<(), Box<dyn InputBuffer>> {
        InputQueueEntry::new_with_aux(Utc::now(), ()).with_buffer(Some(Box::new(Records {
            ids: ids.to_vec(),
            pipeline: pipeline.clone(),
        })
            as Box<dyn InputBuffer>))
    }

    /// A queue entry with no records.
    fn marker() -> InputQueueEntry<(), Box<dyn InputBuffer>> {
        InputQueueEntry::new_with_aux(Utc::now(), ())
    }

    /// The two ways that a connector flushes its queue.
    #[derive(Clone, Copy, Debug)]
    enum Flush {
        WithAux,
        Queue,
    }

    impl Flush {
        fn flush(self, queue: &InputQueue) {
            match self {
                Flush::WithAux => {
                    queue.flush_with_aux();
                }
                Flush::Queue => queue.queue(),
            }
        }
    }

    const FLUSHES: [Flush; 2] = [Flush::WithAux, Flush::Queue];

    /// Records after a request to start a transaction wait until the
    /// transaction is open.
    #[test]
    fn records_wait_for_the_transaction_to_open() {
        for flush in FLUSHES {
            let pipeline = Pipeline::default();
            let queue = InputQueue::new(Box::new(pipeline.clone()));
            queue.push_entry(marker().with_start_transaction(Some(None)), Vec::new());
            queue.push_entry(records(&pipeline, &[1, 2]), Vec::new());

            // The pipeline has not opened the transaction yet.
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [], "{flush:?}");

            pipeline.set_open(Some(1));
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(1, true), (2, true)], "{flush:?}");
        }
    }

    /// Records after a request to commit a transaction wait until the
    /// transaction is closed.
    #[test]
    fn records_wait_for_the_transaction_to_close() {
        for flush in FLUSHES {
            let pipeline = Pipeline::default();
            pipeline.set_open(Some(1));
            let queue = InputQueue::new(Box::new(pipeline.clone()));
            queue.push_entry(
                records(&pipeline, &[1]).with_start_transaction(Some(None)),
                Vec::new(),
            );
            queue.push_entry(marker().with_commit_transaction(true), Vec::new());
            queue.push_entry(records(&pipeline, &[2]), Vec::new());

            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(1, true)], "{flush:?}");

            // The pipeline has not committed the transaction yet.
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [], "{flush:?}");

            pipeline.set_open(None);
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(2, false)], "{flush:?}");
        }
    }

    /// A transaction that opens right after the connector's commit is a
    /// different transaction, so the connector's next records may land in it.
    #[test]
    fn records_may_land_in_the_next_transaction() {
        for flush in FLUSHES {
            let pipeline = Pipeline::default();
            pipeline.set_open(Some(1));
            let queue = InputQueue::new(Box::new(pipeline.clone()));
            queue.push_entry(
                records(&pipeline, &[1]).with_start_transaction(Some(None)),
                Vec::new(),
            );
            queue.push_entry(marker().with_commit_transaction(true), Vec::new());
            queue.push_entry(records(&pipeline, &[2]), Vec::new());
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(1, true)], "{flush:?}");

            pipeline.set_open(Some(2));
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(2, true)], "{flush:?}");
        }
    }

    /// A connector that commits a transaction and then starts another, while
    /// the pipeline keeps the first one open for other participants, does not
    /// request the second one until the first one closes.  A coordinator
    /// ignores a request from a connector that already joined and committed
    /// the open transaction, so that the transaction can commit.
    #[test]
    fn next_request_waits_for_the_transaction_to_close() {
        for flush in FLUSHES {
            let pipeline = Pipeline::default();
            pipeline.set_open(Some(1));
            let queue = InputQueue::new(Box::new(pipeline.clone()));
            for id in [1, 2] {
                queue.push_entry(
                    records(&pipeline, &[id]).with_start_transaction(Some(None)),
                    Vec::new(),
                );
                queue.push_entry(marker().with_commit_transaction(true), Vec::new());
            }

            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(1, true)], "{flush:?}");
            assert_eq!(pipeline.starts(), 1, "{flush:?}");

            // Another participant keeps the transaction open.
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [], "{flush:?}");
            assert_eq!(pipeline.starts(), 1, "{flush:?}");

            // Once it closes, the connector requests the next transaction and
            // waits for it to open.
            pipeline.set_open(None);
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [], "{flush:?}");
            assert_eq!(pipeline.starts(), 2, "{flush:?}");

            pipeline.set_open(Some(2));
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(2, true)], "{flush:?}");
        }
    }

    /// Makes the queue's clock read `start + elapsed` on this thread.
    fn set_clock(start: Instant, elapsed: Duration) {
        super::FAKE_NOW.with(|now| now.set(Some(start + elapsed)));
    }

    /// A connector that waits a long time for a boundary reports it.
    #[test]
    fn long_waits_are_reported() {
        let threshold = super::BOUNDARY_WARNING_THRESHOLD;
        let start = Instant::now();
        set_clock(start, Duration::ZERO);
        let pipeline = Pipeline::default();
        let queue = InputQueue::new(Box::new(pipeline.clone()));
        queue.push_entry(marker().with_start_transaction(Some(None)), Vec::new());
        queue.flush_with_aux();
        set_clock(start, threshold - Duration::from_nanos(1));
        queue.flush_with_aux();
        assert_eq!(pipeline.0.lock().unwrap().errors.len(), 0);

        set_clock(start, threshold);
        queue.flush_with_aux();
        let errors = std::mem::take(&mut pipeline.0.lock().unwrap().errors);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("to open the transaction"), "{errors:?}");

        // Not again until the wait doubles.
        set_clock(start, threshold * 2 - Duration::from_nanos(1));
        queue.flush_with_aux();
        assert_eq!(pipeline.0.lock().unwrap().errors.len(), 0);
        set_clock(start, threshold * 2);
        queue.flush_with_aux();
        assert_eq!(pipeline.0.lock().unwrap().errors.len(), 1);
    }

    /// A connector that waits for a boundary but holds nothing back does not
    /// report the wait.
    #[test]
    fn waits_without_input_are_not_reported() {
        let start = Instant::now();
        set_clock(start, Duration::ZERO);
        let pipeline = Pipeline::default();
        pipeline.set_open(Some(1));
        let queue = InputQueue::new(Box::new(pipeline.clone()));
        queue.push_entry(
            records(&pipeline, &[1])
                .with_start_transaction(Some(None))
                .with_commit_transaction(true),
            Vec::new(),
        );
        queue.flush_with_aux();
        assert_eq!(pipeline.take_flushed(), [(1, true)]);

        // The transaction stays open, but nothing waits for it to close.
        set_clock(start, super::BOUNDARY_WARNING_THRESHOLD);
        queue.flush_with_aux();
        assert_eq!(pipeline.0.lock().unwrap().errors.len(), 0);
    }

    /// A pipeline that acts on requests at once never holds records back.
    #[test]
    fn immediate_pipeline_never_waits() {
        for flush in FLUSHES {
            let pipeline = Pipeline::default();
            pipeline.0.lock().unwrap().immediate = true;
            let queue = InputQueue::new(Box::new(pipeline.clone()));
            queue.push_entry(
                records(&pipeline, &[1]).with_start_transaction(Some(None)),
                Vec::new(),
            );
            queue.push_entry(records(&pipeline, &[2]), Vec::new());
            queue.push_entry(marker().with_commit_transaction(true), Vec::new());
            queue.push_entry(records(&pipeline, &[3]), Vec::new());
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(1, true), (2, true)], "{flush:?}");
            flush.flush(&queue);
            assert_eq!(pipeline.take_flushed(), [(3, false)], "{flush:?}");
        }
    }
}

/// A model-based test of transaction boundaries in a pipeline that, like a
/// multihost pipeline, learns of a connector's requests only after a delay.
///
/// The model has one host, which runs the real [InputQueue], and a
/// coordinator, which sees the host's requests after a random delay, opens a
/// transaction when it sees one, and commits it when it sees none.  A random
/// sequence of actions interleaves the connector, the queue, and the
/// coordinator, and then a fair drain runs every action until nothing is left.
#[cfg(test)]
mod transaction_boundary_model_tests {
    use std::collections::{BTreeMap, BTreeSet, VecDeque};
    use std::hash::Hasher;
    use std::sync::{Arc, Mutex};

    use anyhow::Error as AnyError;
    use chrono::Utc;
    use feldera_types::adapter_stats::ConnectorHealth;
    use feldera_types::config::FtModel;
    use feldera_types::coordination::Completion;
    use feldera_types::transaction::TransactionId;
    use proptest::prelude::*;

    use super::{InputConsumer, InputQueue, InputQueueEntry, Resume, Watermark};
    use crate::format::{BufferSize, InputBuffer, ParseError};

    #[derive(Default)]
    struct World {
        /// Changes to the host's request that the coordinator has not seen yet.
        updates: VecDeque<bool>,

        /// Whether the coordinator believes that the host requests a
        /// transaction.
        seen_request: bool,

        /// The transaction that the coordinator has open on the host.
        open: Option<TransactionId>,

        /// The last transaction ID that the coordinator used.
        last_id: TransactionId,

        /// The number of records that a flush may take.
        max_batch_size: usize,

        /// Each flushed record, with the transaction it landed in.
        flushed: Vec<(u32, Option<TransactionId>)>,
    }

    #[derive(Clone, Default)]
    struct Host(Arc<Mutex<World>>);

    impl Host {
        fn world(&self) -> std::sync::MutexGuard<'_, World> {
            self.0.lock().unwrap()
        }

        /// The coordinator sees the oldest change to the host's request.
        fn deliver(&self) -> bool {
            let mut world = self.world();
            match world.updates.pop_front() {
                Some(request) => {
                    world.seen_request = request;
                    true
                }
                None => false,
            }
        }

        /// The coordinator opens or commits a transaction, if it should.
        fn coordinate(&self) -> bool {
            let mut world = self.world();
            match (world.open, world.seen_request) {
                (None, true) => {
                    world.last_id += 1;
                    world.open = Some(world.last_id);
                    true
                }
                (Some(_), false) => {
                    world.open = None;
                    true
                }
                _ => false,
            }
        }
    }

    impl InputConsumer for Host {
        fn max_batch_size(&self) -> usize {
            self.world().max_batch_size
        }
        fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
            None
        }
        fn parse_errors(&self, _errors: Vec<ParseError>) {}
        fn buffered(&self, _amt: BufferSize) {}
        fn replayed(&self, _amt: BufferSize, _hash: u64) {}
        fn extended(&self, _amt: BufferSize, _resume: Option<Resume>, _watermarks: Vec<Watermark>) {
        }
        fn eoi(&self) {}
        fn request_step(&self) {}
        fn start_transaction(&self, _label: Option<&str>) {
            self.world().updates.push_back(true);
        }
        fn commit_transaction(&self) {
            self.world().updates.push_back(false);
        }
        fn open_transaction(&self) -> Option<Option<TransactionId>> {
            Some(self.world().open)
        }
        fn completion_watcher(&self) -> Option<tokio::sync::watch::Receiver<Completion>> {
            None
        }
        fn error(&self, _fatal: bool, _error: AnyError, _tag: Option<&'static str>) {}
        fn update_connector_health(&self, _health: ConnectorHealth) {}
    }

    struct Records {
        ids: Vec<u32>,
        host: Host,
    }

    impl InputBuffer for Records {
        fn flush(&mut self) {
            let mut world = self.host.world();
            let open = world.open;
            world
                .flushed
                .extend(self.ids.drain(..).map(|id| (id, open)));
        }
        fn len(&self) -> BufferSize {
            BufferSize {
                records: self.ids.len(),
                bytes: self.ids.len(),
            }
        }
        fn hash(&self, _hasher: &mut dyn Hasher) {}
        fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
            let n = n.min(self.ids.len());
            (n > 0).then(|| {
                Box::new(Records {
                    ids: self.ids.drain(..n).collect(),
                    host: self.host.clone(),
                }) as Box<dyn InputBuffer>
            })
        }
    }

    /// One queue entry that the connector pushes.
    #[derive(Clone, Debug)]
    struct Entry {
        n_records: u32,
        start: bool,
        commit: bool,
    }

    /// Returns a strategy for a connector's entries: plain records, and
    /// transactions whose start and commit entries may or may not have records.
    fn entries() -> impl Strategy<Value = Vec<Entry>> {
        let plain = (1u32..3).prop_map(|n_records| {
            vec![Entry {
                n_records,
                start: false,
                commit: false,
            }]
        });
        let transaction = (0u32..3, prop::collection::vec(1u32..3, 0..3), 0u32..3).prop_map(
            |(first, middle, last)| {
                let mut entries = vec![Entry {
                    n_records: first,
                    start: true,
                    commit: false,
                }];
                entries.extend(middle.into_iter().map(|n_records| Entry {
                    n_records,
                    start: false,
                    commit: false,
                }));
                entries.push(Entry {
                    n_records: last,
                    start: false,
                    commit: true,
                });
                entries
            },
        );
        prop::collection::vec(prop_oneof![plain, transaction], 0..6)
            .prop_map(|segments| segments.into_iter().flatten().collect())
    }

    #[derive(Clone, Copy, Debug)]
    enum Action {
        Push,
        FlushWithAux,
        Queue,
        Deliver,
        Coordinate,
    }

    fn action() -> impl Strategy<Value = Action> {
        prop_oneof![
            Just(Action::Push),
            Just(Action::FlushWithAux),
            Just(Action::Queue),
            Just(Action::Deliver),
            Just(Action::Coordinate),
        ]
    }

    /// Runs the model.  `queue_flush` selects [InputQueue::queue] instead of
    /// [InputQueue::flush_with_aux] for flushes in the drain.
    fn run(entries: Vec<Entry>, actions: Vec<Action>, max_batch_size: usize, queue_flush: bool) {
        let host = Host::default();
        host.world().max_batch_size = max_batch_size;
        let queue = InputQueue::new(Box::new(host.clone()));

        // Assign record IDs, and remember which connector transaction, if
        // any, each record belongs to.
        let mut next_id = 0;
        let mut owner = BTreeMap::new();
        let mut transaction = None;
        let mut n_transactions = 0;
        let mut pending = VecDeque::new();
        for entry in &entries {
            if entry.start {
                transaction = Some(n_transactions);
                n_transactions += 1;
            }
            let ids = (next_id..next_id + entry.n_records).collect::<Vec<_>>();
            next_id += entry.n_records;
            for id in &ids {
                owner.insert(*id, transaction);
            }
            if entry.commit {
                transaction = None;
            }
            pending.push_back((entry.clone(), ids));
        }
        let push = |pending: &mut VecDeque<(Entry, Vec<u32>)>| {
            let Some((entry, ids)) = pending.pop_front() else {
                return false;
            };
            let buffer = (!ids.is_empty()).then(|| {
                Box::new(Records {
                    ids,
                    host: host.clone(),
                }) as Box<dyn InputBuffer>
            });
            queue.push_entry(
                InputQueueEntry::new_with_aux(Utc::now(), ())
                    .with_buffer(buffer)
                    .with_start_transaction(entry.start.then_some(None))
                    .with_commit_transaction(entry.commit),
                Vec::new(),
            );
            true
        };
        let flush = |queue_flush: bool| {
            let before = host.world().flushed.len();
            if queue_flush {
                queue.queue();
            } else {
                queue.flush_with_aux();
            }
            host.world().flushed.len() > before
        };

        for action in actions {
            match action {
                Action::Push => {
                    push(&mut pending);
                }
                Action::FlushWithAux => {
                    flush(false);
                }
                Action::Queue => {
                    flush(true);
                }
                Action::Deliver => {
                    host.deliver();
                }
                Action::Coordinate => {
                    host.coordinate();
                }
            }
        }

        // Drain fairly.  Every round makes progress until nothing is left,
        // so a round that makes none ends the drain.
        for _ in 0..10_000 {
            let mut progress = push(&mut pending);
            progress |= flush(queue_flush);
            while host.deliver() {
                progress = true;
            }
            progress |= host.coordinate();
            progress |= !queue.is_empty();
            if !progress {
                break;
            }
        }

        let world = host.world();
        // Liveness: everything got through.
        assert!(queue.is_empty(), "records stuck in the queue");
        assert_eq!(world.flushed.len(), next_id as usize, "records lost");

        // Safety: each connector transaction lands in exactly one pipeline
        // transaction, of its own, and plain records land in none of them.
        let mut landed = BTreeMap::<usize, BTreeSet<Option<TransactionId>>>::new();
        let mut plain = BTreeSet::new();
        for (id, open) in &world.flushed {
            match owner[id] {
                Some(transaction) => {
                    landed.entry(transaction).or_default().insert(*open);
                }
                None => {
                    plain.insert(*open);
                }
            }
        }
        let mut used = BTreeSet::new();
        for (transaction, opens) in &landed {
            assert_eq!(
                opens.len(),
                1,
                "connector transaction {transaction} landed in {opens:?}"
            );
            let open = *opens.first().unwrap();
            assert!(
                open.is_some(),
                "connector transaction {transaction} landed outside"
            );
            assert!(
                used.insert(open),
                "two connector transactions shared {open:?}"
            );
        }
        assert!(
            plain.is_disjoint(&used),
            "plain records landed in a connector transaction"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        #[test]
        fn boundaries_hold_and_input_drains(
            entries in entries(),
            actions in prop::collection::vec(action(), 0..60),
            max_batch_size in 1usize..5,
            queue_flush: bool,
        ) {
            run(entries, actions, max_batch_size, queue_flush);
        }
    }
}
