use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Mutex;

use anyhow::{Error as AnyError, Result as AnyResult};
use dyn_clone::DynClone;
use feldera_types::program_schema::Relation;
use rmpv::{ext::Error as RmpDecodeError, Value as RmpValue};
use serde::Deserialize;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::UnboundedReceiver;
use xxhash_rust::xxh3::Xxh3Default;

use crate::catalog::InputCollectionHandle;
use crate::format::{InputBuffer, ParseError, Parser};
use crate::PipelineState;

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
///
pub trait InputEndpoint: Send {
    /// Whether this endpoint is [fault tolerant](crate#fault-tolerance).
    ///
    /// A fault-tolerant endpoint must support [InputReaderCommand::Seek] and
    /// [InputReaderCommand::Replay].
    fn is_fault_tolerant(&self) -> bool;
}

pub trait TransportInputEndpoint: InputEndpoint {
    /// Creates a new input endpoint. The endpoint should use `parser` to parse
    /// data into records. Returns an [`InputReader`] for reading the endpoint's
    /// data.  The endpoint will use `consumer` to report its progress.
    ///
    /// The reader is initially paused.
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>>;
}

pub trait IntegratedInputEndpoint: InputEndpoint {
    fn open(
        self: Box<Self>,
        input_handle: &InputCollectionHandle,
    ) -> AnyResult<Box<dyn InputReader>>;
}

/// Commands for an [InputReader] to execute.
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
    /// Tells a fault-tolerant input reader to seek past the data already read
    /// in the step whose metadata is given by the value.
    ///
    /// # Contraints
    ///
    /// Only fault-tolerant input readers need to accept this. If it is given,
    /// it will be issued only once and before any of the other commands.
    Seek(RmpValue),

    /// Tells the input reader to replay the step described in `metadata` by
    /// reading and flushing buffers for the data in the step, and then
    /// [InputConsumer::replayed] to signal completion.
    ///
    /// The input reader should report the data that it queues to
    /// [InputConsumer::buffered] as it does the replay.
    ///
    /// The input reader doesn't have to process other commands while it does
    /// the replay.
    ///
    /// # Contraints
    ///
    /// Only fault-tolerant input readers need to accept this. It will be issued
    /// zero or more times, after [InputReaderCommand::Seek] but before any
    /// other commands.
    Replay(RmpValue),

    /// Tells the input reader to accept further input. The first time it
    /// receives this command, the reader should start from:
    ///
    /// - If [InputReaderCommand::Seek] or [InputReaderCommand::Replay] was
    ///   previously issued, then just beyond the end of the data from the last
    ///   call.
    ///
    /// - Otherwise, from the beginning of the input.
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
    /// # Constraints
    ///
    /// The controller won't issue this command before it first issues [InputReaderCommand::Extend].
    Queue,

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
            InputReaderCommand::Seek(_) | InputReaderCommand::Replay(_) => None,
            InputReaderCommand::Queue => Some(NonFtInputReaderCommand::Queue),
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

/// A thread-safe queue for collecting and flushing input buffers.
///
/// Commonly used by `InputReader` implementations for staging buffers from
/// worker threads.
pub struct InputQueue<A = ()> {
    pub queue: Mutex<VecDeque<(Box<dyn InputBuffer>, A)>>,
    pub consumer: Box<dyn InputConsumer>,
}

impl<A> InputQueue<A> {
    pub fn new(consumer: Box<dyn InputConsumer>) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            consumer,
        }
    }

    /// Appends `buffer`, if nonempty, to the queue, and associates it with
    /// `aux`.  Reports to the controller that `num_bytes` have been received
    /// and at least partially parsed, and that `errors` have occurred during
    /// parsing.
    ///
    /// If `buffer` has no records, then this discards `aux`, even if `buffer`
    /// is non-`None`.
    pub fn push_with_aux(
        &self,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        num_bytes: usize,
        aux: A,
    ) {
        self.consumer.parse_errors(errors);
        match buffer {
            Some(buffer) if !buffer.is_empty() => {
                let num_records = buffer.len();

                let mut queue = self.queue.lock().unwrap();
                queue.push_back((buffer, aux));
                self.consumer.buffered(num_records, num_bytes);
            }
            _ => self.consumer.buffered(0, num_bytes),
        }
    }

    /// Flushes a batch of records to the circuit and returns the auxiliary data
    /// that was associated with those records.
    ///
    /// This always flushes whole buffers to the circuit (with `flush`),
    /// since auxiliary data is associated with a whole buffer rather than with
    /// individual records. If the auxiliary data type `A` is `()`, then
    /// [InputQueue<()>::flush] avoids that and so is a better choice.
    pub fn flush_with_aux(&self) -> (usize, u64, Vec<A>) {
        let mut total = 0;
        let mut hasher = self
            .consumer
            .is_pipeline_fault_tolerant()
            .then(Xxh3Default::new);
        let n = self.consumer.max_batch_size();
        let mut consumed_aux = Vec::new();
        while total < n {
            let Some((mut buffer, aux)) = self.queue.lock().unwrap().pop_front() else {
                break;
            };
            total += buffer.len();
            if let Some(hasher) = hasher.as_mut() {
                buffer.hash(hasher);
            }
            buffer.flush();
            consumed_aux.push(aux);
        }
        (
            total,
            hasher.map_or(0, |hasher| hasher.finish()),
            consumed_aux,
        )
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl InputQueue<()> {
    /// Appends `buffer`, if nonempty,` to the queue.  Reports to the controller
    /// that `num_bytes` have been received and at least partially parsed, and
    /// that `errors` have occurred during parsing.
    pub fn push(
        &self,
        (buffer, errors): (Option<Box<dyn InputBuffer>>, Vec<ParseError>),
        num_bytes: usize,
    ) {
        self.push_with_aux((buffer, errors), num_bytes, ())
    }

    /// Flushes a batch of records to the circuit and reports to the consumer
    /// that it was done.
    pub fn queue(&self) {
        let mut total = 0;
        let n = self.consumer.max_batch_size();
        while total < n {
            let Some((mut buffer, ())) = self.queue.lock().unwrap().pop_front() else {
                break;
            };
            let mut taken = buffer.take_some(n - total);
            total += taken.len();
            taken.flush();
            drop(taken);
            if !buffer.is_empty() {
                self.queue.lock().unwrap().push_front((buffer, ()));
                break;
            }
        }
        self.consumer.extended(total, 0, RmpValue::Nil);
    }
}

/// Reads data from an endpoint.
///
/// Use [`TransportInputEndpoint::open`] to obtain an [`InputReader`].
pub trait InputReader: Send + Sync {
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

    fn seek(&self, metadata: RmpValue) {
        self.request(InputReaderCommand::Seek(metadata));
    }

    fn replay(&self, metadata: RmpValue) {
        self.request(InputReaderCommand::Replay(metadata));
    }

    fn extend(&self) {
        self.request(InputReaderCommand::Extend);
    }

    fn pause(&self) {
        self.request(InputReaderCommand::Pause);
    }

    fn queue(&self) {
        self.request(InputReaderCommand::Queue);
    }

    fn disconnect(&self) {
        self.request(InputReaderCommand::Disconnect);
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

    /// Returns whether the input endpoint is running within a fault-tolerant
    /// pipeline. If the endpoint is one where fault tolerance is expensive,
    /// then a `false` return value can allow it to skip that expense.
    fn is_pipeline_fault_tolerant(&self) -> bool;

    /// Reports `errors` as parse errors.
    fn parse_errors(&self, errors: Vec<ParseError>);

    /// Reports that the input adapter has internally buffered `num_records`
    /// records comprising `num_bytes` bytes of input data.
    ///
    /// Fault-tolerant input adapters should report buffered data during replay
    /// as well as in normal operation.
    fn buffered(&self, num_records: usize, num_bytes: usize);

    /// Reports that the input adapter has completed flushing `num_records`
    /// records to the circuit, that hash to `hash`, in response to an
    /// [InputReaderCommand::Replay] request.
    ///
    /// Only a fault-tolerant input adapter will invoke this.
    fn replayed(&self, num_records: usize, hash: u64);

    /// Reports that the input adapter has completed flushing `num_records`
    /// records to the circuit, that hash to `hash`, in response to an
    /// [InputReaderCommand::Queue] request.
    ///
    /// If [InputConsumer::is_pipeline_fault_tolerant] returns false, then the
    /// value of `hash` doesn't matter.
    fn extended(&self, num_records: usize, hash: u64, metadata: RmpValue);

    /// Reports that the endpoint has reached end of input and that no more data
    /// will be received from the endpoint.
    ///
    /// If the endpoint has already indicated that it has buffered records then
    /// the controller will request them in future [InputReaderCommand::Queue]
    /// messages. The endpoint must not make further calls to
    /// [InputConsumer::buffered] or [InputConsumer::parse_errors].
    fn eoi(&self);

    /// Endpoint failed.
    ///
    /// Reports that the endpoint failed and that it will not queue any more
    /// data.
    fn error(&self, fatal: bool, error: AnyError);
}

dyn_clone::clone_trait_object!(InputConsumer);

pub type AsyncErrorCallback = Box<dyn Fn(bool, AnyError) + Send + Sync>;

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
pub trait OutputEndpoint: Send {
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
    fn batch_start(&mut self, _step: Step) -> AnyResult<()> {
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
}

/// An [UnboundedReceiver] wrapper for [InputReaderCommand] for fault-tolerant connectors.
///
/// A fault-tolerant connector wants to receive, in order:
///
/// - Zero or one [InputReaderCommand::Seek]s.
///
/// - Zero or more [InputReaderCommand::Replay]s.
///
/// - Zero or more other commands.
///
/// This helps with that.
// This is used by Kafka and Nexmark but both of those are optional.
pub struct InputCommandReceiver<T> {
    receiver: UnboundedReceiver<InputReaderCommand>,
    buffer: Option<InputReaderCommand>,
    _phantom: PhantomData<T>,
}

/// Error type returned by some [InputCommandReceiver] methods.
///
/// We could just use `anyhow` and that would probably be just as good though.
#[derive(Debug)]
pub enum InputCommandReceiverError {
    Disconnected,
    DecodeError(RmpDecodeError),
}

impl std::error::Error for InputCommandReceiverError {}

impl Display for InputCommandReceiverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputCommandReceiverError::Disconnected => write!(f, "sender disconnected"),
            InputCommandReceiverError::DecodeError(e) => e.fmt(f),
        }
    }
}

impl From<RmpDecodeError> for InputCommandReceiverError {
    fn from(value: RmpDecodeError) -> Self {
        Self::DecodeError(value)
    }
}

// This is used by Kafka and Nexmark but both of those are optional.
impl<T> InputCommandReceiver<T> {
    pub fn new(receiver: UnboundedReceiver<InputReaderCommand>) -> Self {
        Self {
            receiver,
            buffer: None,
            _phantom: PhantomData,
        }
    }

    pub fn blocking_recv_seek(&mut self) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        let command = self.blocking_recv()?;
        self.take_seek(command)
    }

    pub async fn recv_seek(&mut self) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        let command = self.recv().await?;
        self.take_seek(command)
    }

    fn take_seek(
        &mut self,
        command: InputReaderCommand,
    ) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        debug_assert!(self.buffer.is_none());
        match command {
            InputReaderCommand::Seek(metadata) => Ok(Some(rmpv::ext::from_value::<T>(metadata)?)),
            InputReaderCommand::Disconnect => Err(InputCommandReceiverError::Disconnected),
            other => {
                self.put_back(other);
                Ok(None)
            }
        }
    }

    pub fn blocking_recv_replay(&mut self) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        let command = self.blocking_recv()?;
        self.take_replay(command)
    }

    pub async fn recv_replay(&mut self) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        let command = self.recv().await?;
        self.take_replay(command)
    }

    fn take_replay(
        &mut self,
        command: InputReaderCommand,
    ) -> Result<Option<T>, InputCommandReceiverError>
    where
        T: for<'a> Deserialize<'a>,
    {
        match command {
            InputReaderCommand::Seek(_) => unreachable!(),
            InputReaderCommand::Replay(metadata) => Ok(Some(rmpv::ext::from_value::<T>(metadata)?)),
            other => {
                self.put_back(other);
                Ok(None)
            }
        }
    }

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

    pub fn blocking_recv(&mut self) -> Result<InputReaderCommand, InputCommandReceiverError> {
        match self.buffer.take() {
            Some(value) => Ok(value),
            None => self
                .receiver
                .blocking_recv()
                .ok_or(InputCommandReceiverError::Disconnected),
        }
    }

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

    pub fn put_back(&mut self, value: InputReaderCommand) {
        assert!(self.buffer.is_none());
        self.buffer = Some(value);
    }
}
