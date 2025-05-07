use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Mutex;

use anyhow::{Error as AnyError, Result as AnyResult};
use dyn_clone::DynClone;
use feldera_types::config::FtModel;
use feldera_types::program_schema::Relation;
use rmpv::{ext::Error as RmpDecodeError, Value as RmpValue};
use serde::Deserialize;
use serde_json::{Error as JsonError, Value as JsonValue};
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
/// # Transitions
///
/// The following diagram shows the possible order in which the controller can
/// issue commands to [InputReader]s:
///
/// ```text
///   ┌─⯇─ (start) ─⯈──┐
///   │      │         │
///   │      ▼         │
///   ├─⯇─ Seek ─⯈─────│
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
    /// Tells a fault-tolerant input reader to seek past the data already read
    /// in the step whose metadata is given by the value.
    ///
    /// # Constraints
    ///
    /// Only fault-tolerant input readers need to accept this. If it is given,
    /// it will be issued only once and before any of the other commands.
    Seek(JsonValue),

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
    /// zero or more times, after [InputReaderCommand::Seek] but before any
    /// other commands.
    Replay { metadata: JsonValue, data: RmpValue },

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
            InputReaderCommand::Seek(_) | InputReaderCommand::Replay { .. } => None,
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

/// A thread-safe queue for collecting and flushing input buffers.
///
/// Commonly used by `InputReader` implementations for staging buffers from
/// worker threads.
pub struct InputQueue<A = ()> {
    #[allow(clippy::type_complexity)]
    pub queue: Mutex<VecDeque<(Option<Box<dyn InputBuffer>>, A)>>,
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
        let num_records = buffer.len();

        let mut queue = self.queue.lock().unwrap();
        queue.push_back((buffer, aux));
        self.consumer.buffered(num_records, num_bytes);
    }

    /// Flushes a batch of records to the circuit and returns the auxiliary data
    /// that was associated with those records.
    ///
    /// This always flushes whole buffers to the circuit (with `flush`),
    /// since auxiliary data is associated with a whole buffer rather than with
    /// individual records. If the auxiliary data type `A` is `()`, then
    /// [InputQueue<()>::flush] avoids that and so is a better choice.
    pub fn flush_with_aux(&self) -> (usize, Option<Xxh3Default>, Vec<A>) {
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
    pub fn flush_with_aux_until(
        &self,
        stop_at: &dyn Fn(&A) -> bool,
    ) -> (usize, Option<Xxh3Default>, Vec<A>) {
        let mut total = 0;
        let mut hasher = self.consumer.hasher();
        let n = self.consumer.max_batch_size();
        let mut consumed_aux = Vec::new();
        while total < n {
            let Some((buffer, aux)) = self.queue.lock().unwrap().pop_front() else {
                break;
            };

            if let Some(mut buffer) = buffer {
                total += buffer.len();
                if let Some(hasher) = hasher.as_mut() {
                    buffer.hash(hasher);
                }
                buffer.flush();
            }

            let stop = stop_at(&aux);
            consumed_aux.push(aux);

            if stop {
                break;
            }
        }
        (total, hasher, consumed_aux)
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
    ///
    /// Only non-fault-tolerant input adapters can use this.
    pub fn queue(&self) {
        let mut total = 0;
        let n = self.consumer.max_batch_size();
        while total < n {
            let Some((buffer, ())) = self.queue.lock().unwrap().pop_front() else {
                break;
            };

            if let Some(mut buffer) = buffer {
                let mut taken = buffer.take_some(n - total);
                total += taken.len();
                taken.flush();
                drop(taken);
                if !buffer.is_empty() {
                    self.queue.lock().unwrap().push_front((Some(buffer), ()));
                    break;
                }
            }
        }
        self.consumer.extended(total, None);
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

    fn seek(&self, metadata: JsonValue) {
        self.request(InputReaderCommand::Seek(metadata));
    }

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
    fn hasher(&self) -> Option<Xxh3Default> {
        match self.pipeline_fault_tolerance() {
            Some(FtModel::ExactlyOnce) => Some(Xxh3Default::new()),
            _ => None,
        }
    }

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
    /// If the step is one that the input adapter can restart after, or replay,
    /// then it should supply that as `resume` (see [Resume] for details).
    fn extended(&self, num_records: usize, resume: Option<Resume>);

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
        /// just after this input step using [InputReaderCommand::Seek].
        seek: JsonValue,
    },

    /// The input adapter can replay this step exactly, or resume just after the
    /// step.
    Replay {
        /// Metadata needed for the controller to restart the input adapter from
        /// just after this input step using [InputReaderCommand::Seek].
        seek: JsonValue,

        /// The data needed for the controller to replay exactly this input step
        /// using [InputReaderCommand::Replay].
        replay: RmpValue,

        /// Hash of the input records in this step, for verification on replay.
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

    /// If `hasher` is provided, returns `Resume::Replay` with its hash value
    /// and `seek`; otherwise, returns `Resume::Seek` with `seek`.
    ///
    /// This is convenient for endpoints that only need to use metadata to
    /// support journaling.  Use [InputConsumer::hasher] to get the hasher.
    pub fn new_metadata_only(seek: JsonValue, hasher: Option<Xxh3Default>) -> Self {
        match hasher {
            Some(hasher) => Self::Replay {
                seek,
                replay: RmpValue::Nil,
                hash: hasher.finish(),
            },
            None => Self::Seek { seek },
        }
    }

    /// If `hasher` is provided, returns `Resume::Replay` with its hash value
    /// and whatever `replay` returns; otherwise, returns `Resume::Seek`.
    ///
    /// This is convenient for endpoints that support journaling by journaling
    /// all the data (and that don't need to journal any metadata).  Use
    /// [InputConsumer::hasher] to get the hasher.
    pub fn new_data_only<F>(replay: F, hasher: Option<Xxh3Default>) -> Self
    where
        F: FnOnce() -> RmpValue,
    {
        let seek = JsonValue::Null;
        match hasher {
            Some(hasher) => Self::Replay {
                seek,
                replay: replay(),
                hash: hasher.finish(),
            },
            None => Self::Seek { seek },
        }
    }
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
    JsonDecodeError(JsonError),
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

impl From<JsonError> for InputCommandReceiverError {
    fn from(value: JsonError) -> Self {
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

    pub fn blocking_recv_seek(&mut self) -> Result<Option<M>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
    {
        let command = self.blocking_recv()?;
        self.take_seek(command)
    }

    pub async fn recv_seek(&mut self) -> Result<Option<M>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
    {
        let command = self.recv().await?;
        self.take_seek(command)
    }

    fn take_seek(
        &mut self,
        command: InputReaderCommand,
    ) -> Result<Option<M>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
    {
        debug_assert!(self.buffer.is_none());
        match command {
            InputReaderCommand::Seek(metadata) => Ok(Some(serde_json::from_value::<M>(metadata)?)),
            InputReaderCommand::Disconnect => Err(InputCommandReceiverError::Disconnected),
            other => {
                self.put_back(other);
                Ok(None)
            }
        }
    }

    pub fn blocking_recv_replay(&mut self) -> Result<Option<(M, D)>, InputCommandReceiverError>
    where
        M: for<'a> Deserialize<'a>,
        D: for<'a> Deserialize<'a>,
    {
        let command = self.blocking_recv()?;
        self.take_replay(command)
    }

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
            InputReaderCommand::Seek(_) => unreachable!(),
            InputReaderCommand::Replay { metadata, data } => Ok(Some((
                serde_json::from_value::<M>(metadata)?,
                rmpv::ext::from_value::<D>(data)?,
            ))),
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
