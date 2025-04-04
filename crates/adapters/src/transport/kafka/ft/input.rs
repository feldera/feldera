use crate::transport::kafka::ft::count_partitions_in_topic;
use crate::transport::secret_resolver::resolve_secret;
use crate::transport::InputCommandReceiver;
use crate::{
    transport::{
        kafka::{rdkafka_loglevel_from, refine_kafka_error, DeferredLogging},
        InputReader,
    },
    InputConsumer, TransportInputEndpoint,
};
use crate::{InputBuffer, ParseError, Parser};
use anyhow::{anyhow, bail, Error as AnyError, Result as AnyResult};
use crossbeam::queue::ArrayQueue;
use crossbeam::sync::{Parker, Unparker};
use feldera_adapterlib::transport::{InputEndpoint, InputReaderCommand};
use feldera_types::program_schema::Relation;
use feldera_types::transport::kafka::{KafkaInputConfig, KafkaStartFromConfig};
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::message::BorrowedMessage;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use std::hash::Hasher;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::thread::{self, JoinHandle, Thread};
use std::{
    iter,
    sync::{Arc, Mutex, Weak},
    thread::spawn,
    time::Duration,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::span::EnteredSpan;
use tracing::{debug, info_span};
use xxhash_rust::xxh3::Xxh3Default;

/// Poll timeout must be low, as it bounds the amount of time it takes to resume the connector.
const POLL_TIMEOUT: Duration = Duration::from_millis(5);

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

// Size of the circular buffer used to pass errors from ClientContext
// to the worker thread.
const ERROR_BUFFER_SIZE: usize = 1000;

pub struct KafkaFtInputEndpoint {
    config: Arc<KafkaInputConfig>,
}

impl KafkaFtInputEndpoint {
    pub fn new(mut config: KafkaInputConfig) -> AnyResult<KafkaFtInputEndpoint> {
        config.validate()?;
        Ok(KafkaFtInputEndpoint {
            config: Arc::new(config),
        })
    }
}

struct KafkaFtInputReader {
    _inner: Arc<KafkaFtInputReaderInner>,
    command_sender: UnboundedSender<InputReaderCommand>,
    poller_thread: Thread,
}

struct KafkaFtInputContext {
    // We keep a weak reference to the endpoint to avoid a reference cycle:
    // endpoint->BaseConsumer->context->endpoint.
    endpoint: Mutex<Weak<KafkaFtInputReaderInner>>,

    deferred_logging: DeferredLogging,
}

impl KafkaFtInputContext {
    fn new() -> Self {
        Self {
            endpoint: Mutex::new(Weak::new()),
            deferred_logging: DeferredLogging::new(),
        }
    }
}

impl ClientContext for KafkaFtInputContext {
    fn error(&self, error: KafkaError, reason: &str) {
        // eprintln!("Kafka error: {error}");
        if let Some(endpoint) = self.endpoint.lock().unwrap().upgrade() {
            endpoint.push_error(error, reason);
        }
    }

    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.deferred_logging.log(level, fac, log_message);
    }
}

impl ConsumerContext for KafkaFtInputContext {}

struct KafkaFtHasher(Vec<Xxh3Default>);

impl KafkaFtHasher {
    fn new(n_partitions: usize) -> Self {
        Self(
            iter::repeat_with(Xxh3Default::new)
                .take(n_partitions)
                .collect(),
        )
    }

    fn add<B>(&mut self, partition: usize, buffer: &B)
    where
        B: InputBuffer,
    {
        buffer.hash(&mut self.0[partition]);
    }

    fn finish(&self) -> u64 {
        let mut h = Xxh3Default::new();
        for (partition, hasher) in self.0.iter().enumerate() {
            h.write_usize(partition);
            h.write_u64(hasher.finish());
        }
        h.finish()
    }
}

struct KafkaFtInputReaderInner {
    kafka_consumer: Arc<BaseConsumer<KafkaFtInputContext>>,
    errors: ArrayQueue<(KafkaError, String)>,
}

impl KafkaFtInputReaderInner {
    #[allow(clippy::borrowed_box)]
    fn poller_thread(
        &self,
        config: Arc<KafkaInputConfig>,
        consumer: &Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        n_partitions: usize,
        command_receiver: UnboundedReceiver<InputReaderCommand>,
    ) -> AnyResult<()> {
        let topic = &config.topic;

        // Start reading the partitions either at the resume point or at the
        // beginning.
        let mut command_receiver = InputCommandReceiver::<Metadata, ()>::new(command_receiver);
        let initial_offsets = match command_receiver.blocking_recv_seek()? {
            Some(metadata) => metadata
                .parse(n_partitions)?
                .into_iter()
                .map(|range| Offset::Offset(range.end))
                .collect::<Vec<_>>(),
            None => match &config.start_from {
                KafkaStartFromConfig::Earliest => {
                    iter::repeat_n(Offset::Beginning, n_partitions).collect()
                }
                KafkaStartFromConfig::Latest => iter::repeat_n(Offset::End, n_partitions).collect(),
                KafkaStartFromConfig::Offsets(offsets) => {
                    if n_partitions != offsets.len() {
                        bail!("Topic {topic} has {n_partitions} partitions but configuration specifies {} offsets.", offsets.len());
                    }
                    for (offset, partition) in offsets.iter().copied().zip(0..) {
                        let (low, high) = self
                            .kafka_consumer
                            .fetch_watermarks(topic, partition, METADATA_TIMEOUT)
                            .map_err(|e| {
                                anyhow!("error fetching metadata for partition '{partition}': {e}",)
                            })?;

                        // Return an error if the specified offset doesn't exist.
                        if !(low..=high).contains(&offset) {
                            bail!("configuration error: provided offset '{offset}' not currently in partition '{partition}'");
                        }
                    }

                    offsets.iter().copied().map(Offset::Offset).collect()
                }
            },
        };
        let next_offsets = initial_offsets
            .iter()
            .map(|o| match o {
                Offset::Offset(offset) => *offset,
                _ => 0,
            })
            .collect::<Vec<_>>();

        let mut assignment = TopicPartitionList::new();
        for (partition, offset) in (0..).zip(initial_offsets) {
            assignment
                .add_partition_offset(topic, partition, offset)
                .map_err(|error| self.refine_error(error).1)?;
        }
        self.kafka_consumer
            .assign(&assignment)
            .map_err(|error| self.refine_error(error).1)?;

        // Prepare to launch threads.
        let exit = Arc::new(AtomicBool::new(false));
        let n_threads = config.poller_threads().min(n_partitions);
        let mut threads = (0..n_threads)
            .map(|_| RecvThread {
                exit: exit.clone(),
                main_thread: thread::current(),
                parker: Parker::new(),
                base_consumer: self.kafka_consumer.clone(),
                consumer: consumer.clone(),
                parser: parser.fork(),
                receivers: Vec::with_capacity(n_partitions.div_ceil(n_threads)),
            })
            .collect::<Vec<_>>();

        // Split every partition away as its own separate queue.
        let mut receivers = Vec::with_capacity(n_partitions);
        for ((partition, thread), next_offset) in (0..n_partitions as i32)
            .zip((0..n_threads).cycle())
            .zip(next_offsets)
        {
            let thread = &mut threads[thread];
            let mut queue = self
                .kafka_consumer
                .split_partition_queue(topic, partition)
                .ok_or_else(|| anyhow!("could not split queue for partition {partition}"))?;

            queue.set_nonempty_callback({
                let unparker = thread.parker.unparker().clone();
                move || unparker.unpark()
            });

            let receiver = Arc::new(PartitionReceiver::new(partition, queue, next_offset));
            receivers.push(receiver.clone());
            thread.receivers.push(receiver);
        }

        // Poll the main consumer queue until it is empty.
        //
        // We have to do this because there's a race between assigning topics
        // and splitting the partition queues.  But (presumably), after we
        // process initial messages that might go to the main queue, the rest
        // should go to the split queues.
        while let Some(message) = self.kafka_consumer.poll(Duration::ZERO) {
            match message {
                Err(KafkaError::PartitionEOF(p)) if (0..n_partitions as i32).contains(&p) => {
                    receivers[p as usize].eof.store(true, Ordering::Relaxed);
                }
                Err(e) => {
                    let (fatal, e) = self.refine_error(e);
                    consumer.error(fatal, e);
                    if fatal {
                        return Ok(());
                    }
                }
                Ok(message) => {
                    let partition = message.partition() as usize;
                    let receiver = receivers.get_mut(partition).ok_or_else(|| {
                        anyhow!("received message for nonexistent partition {partition}")
                    })?;
                    receiver.handle_kafka_message(
                        &self.kafka_consumer,
                        &**consumer,
                        &mut *parser,
                        Ok(message),
                    );
                }
            }
        }

        // Launch threads.
        let threads = threads
            .into_iter()
            .map(RecvThreadHandle::new)
            .collect::<Vec<_>>();

        // Then replay as many steps as requested.
        while let Some((metadata, ())) = command_receiver.blocking_recv_replay()? {
            let metadata = metadata.parse(n_partitions)?;
            let mut incomplete_partitions = HashSet::new();
            for (partition, (offsets, receiver)) in
                metadata.iter().zip(receivers.iter_mut()).enumerate()
            {
                if !offsets.is_empty() {
                    receiver.set_max_offset(offsets.end - 1);
                    incomplete_partitions.insert(partition);
                }
            }
            for thread in &threads {
                thread.unparker.unpark();
            }
            let mut total_records = 0;
            let mut hasher = KafkaFtHasher::new(n_partitions);
            loop {
                // Process messages for all partitions.
                for (partition, receiver) in receivers.iter().enumerate() {
                    let max = receiver.max_offset();
                    while let Some(mut msg) = receiver.read(max) {
                        consumer.parse_errors(msg.errors);
                        total_records += msg.buffer.len();
                        hasher.add(partition, &msg.buffer);
                        msg.buffer.flush();
                        if msg.offset == max {
                            incomplete_partitions.remove(&partition);
                        }
                    }
                }
                if incomplete_partitions.is_empty() {
                    break;
                }

                // Poll the Kafka consumer. We don't want messages from it but it's obligatory.
                match self.kafka_consumer.poll(Duration::ZERO) {
                    Some(Err(e)) => {
                        let (fatal, e) = self.refine_error(e);
                        consumer.error(fatal, e);
                        if fatal {
                            return Ok(());
                        }
                    }
                    Some(Ok(message)) => {
                        bail!("All partitions are split but the main consumer received a message anyway: {message:?}");
                    }
                    None => (),
                }

                if receivers.iter().any(|receiver| receiver.fatal_error()) {
                    return Ok(());
                }

                thread::park_timeout(POLL_TIMEOUT);
            }
            consumer.replayed(total_records, hasher.finish());
        }

        // We're done replaying.

        let mut running = false;
        let mut kafka_paused = false;
        loop {
            let was_running = running;
            while let Some(command) = command_receiver.try_recv()? {
                match command {
                    command @ InputReaderCommand::Seek(_)
                    | command @ InputReaderCommand::Replay { .. } => {
                        unreachable!("{command:?} must be at the beginning of the command stream")
                    }
                    InputReaderCommand::Extend => running = true,
                    InputReaderCommand::Pause => running = false,
                    InputReaderCommand::Queue => {
                        let mut total = 0;
                        let mut hasher = KafkaFtHasher::new(n_partitions);
                        let mut offsets = receivers
                            .iter()
                            .map(|r| {
                                let next_offset = r.next_offset();
                                next_offset..next_offset
                            })
                            .collect::<Vec<_>>();
                        while total < consumer.max_batch_size() {
                            let mut empty = true;
                            for (partition, (receiver, range)) in
                                receivers.iter().zip(offsets.iter_mut()).enumerate()
                            {
                                if let Some(mut msg) = receiver.read(i64::MAX) {
                                    consumer.parse_errors(msg.errors);
                                    total += msg.buffer.len();
                                    hasher.add(partition, &msg.buffer);
                                    msg.buffer.flush();
                                    empty = false;

                                    if range.is_empty() {
                                        *range = msg.offset..msg.offset + 1;
                                    } else {
                                        range.end = msg.offset + 1;
                                    }
                                }
                            }
                            if empty {
                                break;
                            }
                        }
                        consumer.extended(
                            total,
                            hasher.finish(),
                            serde_json::to_value(&Metadata { offsets }).unwrap(),
                            rmpv::Value::Nil,
                        );
                    }
                    InputReaderCommand::Disconnect => return Ok(()),
                }
            }

            if !running {
                if !kafka_paused {
                    self.pause_partitions()
                        .map_err(|error| self.refine_error(error).1)?;
                    for receiver in receivers.iter() {
                        receiver.set_max_offset(0);
                    }
                    kafka_paused = true;
                }
            } else {
                if kafka_paused {
                    self.resume_partitions()
                        .map_err(|error| self.refine_error(error).1)?;
                    kafka_paused = false;
                }
                if !was_running {
                    for receiver in receivers.iter() {
                        receiver.set_max_offset(i64::MAX);
                    }
                    for thread in &threads {
                        thread.unparker.unpark();
                    }
                }
            }

            // Keep polling even while the consumer is paused as `BaseConsumer`
            // processes control messages (including rebalancing and errors)
            // within the polling thread.
            match self.kafka_consumer.poll(Duration::ZERO) {
                None => (),
                Some(Err(e)) => {
                    let (fatal, e) = self.refine_error(e);
                    consumer.error(fatal, e);
                    if fatal {
                        return Ok(());
                    }
                }
                Some(Ok(message)) => {
                    bail!("All partitions are split but the main consumer received a message anyway: {message:?}");
                }
            }

            while let Some((error, reason)) = self.pop_error() {
                let (fatal, _e) = self.refine_error(error);
                // `reason` contains a human-readable description of the
                // error.
                consumer.error(fatal, anyhow!(reason));
                if fatal {
                    return Ok(());
                }
            }

            if receivers.iter().any(|r| r.fatal_error()) {
                return Ok(());
            }
            if receivers.iter().all(|r| r.eof()) {
                consumer.eoi();
                return Ok(());
            }

            thread::park_timeout(Duration::from_secs(1));
        }
    }

    fn push_error(&self, error: KafkaError, reason: &str) {
        // `force_push` makes the queue operate as a circular buffer.
        self.errors.force_push((error, reason.to_string()));
    }

    fn pop_error(&self) -> Option<(KafkaError, String)> {
        self.errors.pop()
    }

    /// Pause all partitions assigned to the consumer.
    fn pause_partitions(&self) -> KafkaResult<()> {
        self.kafka_consumer
            .pause(&self.kafka_consumer.assignment()?)
    }

    /// Resume all partitions assigned to the consumer.
    fn resume_partitions(&self) -> KafkaResult<()> {
        self.kafka_consumer
            .resume(&self.kafka_consumer.assignment()?)
    }

    fn refine_error(&self, e: KafkaError) -> (bool, AnyError) {
        refine_kafka_error(self.kafka_consumer.client(), e)
    }
}

fn span(config: &KafkaInputConfig) -> EnteredSpan {
    info_span!("kafka_input", topic = config.topic).entered()
}

impl KafkaFtInputReader {
    fn new(
        config: &Arc<KafkaInputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        let _guard = span(config);

        // Create Kafka consumer configuration.
        debug!("Starting Kafka input endpoint: {:?}", config);

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            client_config.set(key, resolve_secret(value)?);
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Create Kafka consumer and count the number of partitions in the topic.
        //
        // This has the desirable side effect of ensuring that we can reach the
        // broker and failing with an error if we cannot.
        let context = KafkaFtInputContext::new();
        let kafka_consumer = BaseConsumer::from_config_and_context(&client_config, context)?;
        let partition_count = count_partitions_in_topic(&kafka_consumer, &config.topic)?;

        let inner = Arc::new(KafkaFtInputReaderInner {
            kafka_consumer: Arc::new(kafka_consumer),
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
        });
        *inner.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&inner);

        let (command_sender, command_receiver) = unbounded_channel();
        let poller_handle = spawn({
            let endpoint = inner.clone();
            let config = config.clone();
            move || {
                let _guard = span(&config);
                if let Err(e) = endpoint.poller_thread(
                    config,
                    &consumer,
                    parser,
                    partition_count,
                    command_receiver,
                ) {
                    consumer.error(true, e);
                }
            }
        });
        let poller_thread = poller_handle.thread().clone();
        Ok(KafkaFtInputReader {
            _inner: inner,
            command_sender,
            poller_thread,
        })
    }
}

impl InputEndpoint for KafkaFtInputEndpoint {
    fn is_fault_tolerant(&self) -> bool {
        true
    }
}

impl TransportInputEndpoint for KafkaFtInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(KafkaFtInputReader::new(
            &self.config,
            consumer,
            parser,
        )?))
    }
}

impl InputReader for KafkaFtInputReader {
    fn request(&self, command: InputReaderCommand) {
        let _ = self.command_sender.send(command);
        self.poller_thread.unpark();
    }
    fn is_closed(&self) -> bool {
        self.command_sender.is_closed()
    }
}

impl Drop for KafkaFtInputReader {
    fn drop(&mut self) {
        self.request(InputReaderCommand::Disconnect);
    }
}

#[derive(Serialize, Deserialize)]
pub(super) struct Metadata {
    /// Per-partition ranges of offsets.
    pub offsets: Vec<Range<i64>>,
}

impl Metadata {
    fn parse(self, n_partitions: usize) -> AnyResult<Vec<Range<i64>>> {
        let offsets = self.offsets;
        if offsets.len() != n_partitions {
            bail!("topic has {n_partitions} partitions but metadata for replay has offsets for {} partitions", offsets.len());
        }
        Ok(offsets)
    }
}

struct Msg {
    offset: i64,
    buffer: Option<Box<dyn InputBuffer>>,
    errors: Vec<ParseError>,
}

impl PartialEq for Msg {
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl Eq for Msg {}

impl PartialOrd for Msg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Msg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.offset.cmp(&other.offset)
    }
}

struct PartitionReceiver {
    partition: i32,
    queue: PartitionQueue<KafkaFtInputContext>,

    /// The maximum message offset that we want to receive.
    ///
    /// If we are replaying, then this is the offset of the final message to be
    /// replayed.
    ///
    /// If we are not replaying, then this is `i64::MAX`.
    max_offset: AtomicI64,

    /// The minimum message offset that we could receive next in this partition
    /// and offset.
    ///
    /// We process messages in order, so when `messages` is non-empty, this is
    /// the largest offset (key) in `messages` plus 1.
    next_offset: AtomicI64,

    /// Parsed messages and errors.
    messages: Mutex<BTreeSet<Msg>>,

    eof: AtomicBool,
    fatal_error: AtomicBool,
}

impl PartitionReceiver {
    pub fn new(
        partition: i32,
        queue: PartitionQueue<KafkaFtInputContext>,
        next_offset: i64,
    ) -> Self {
        Self {
            partition,
            queue,
            max_offset: AtomicI64::new(0),
            next_offset: AtomicI64::new(next_offset),
            messages: Mutex::new(BTreeSet::new()),
            eof: AtomicBool::new(false),
            fatal_error: AtomicBool::new(false),
        }
    }
    pub fn read(&self, max: i64) -> Option<Msg> {
        let mut messages = self.messages.lock().unwrap();
        match messages.first() {
            Some(msg) if msg.offset <= max => messages.pop_first(),
            _ => None,
        }
    }

    pub fn fatal_error(&self) -> bool {
        self.fatal_error.load(Ordering::Relaxed)
    }

    pub fn eof(&self) -> bool {
        self.eof.load(Ordering::Relaxed)
    }

    fn set_max_offset(&self, max: i64) {
        self.max_offset.store(max, Ordering::Relaxed);
    }

    pub fn max_offset(&self) -> i64 {
        self.max_offset.load(Ordering::Relaxed)
    }

    fn next_offset(&self) -> i64 {
        self.next_offset.load(Ordering::Relaxed)
    }

    fn handle_kafka_message(
        &self,
        base_consumer: &BaseConsumer<KafkaFtInputContext>,
        consumer: &dyn InputConsumer,
        parser: &mut dyn Parser,
        message: KafkaResult<BorrowedMessage<'_>>,
    ) {
        match message {
            Err(KafkaError::PartitionEOF(_)) => {
                self.eof.store(true, Ordering::Relaxed);
            }
            Err(e) => {
                let (fatal, e) = refine_kafka_error(base_consumer.client(), e);
                consumer.error(fatal, e);
                if fatal {
                    self.fatal_error.store(true, Ordering::Relaxed);
                }
            }
            Ok(message) => {
                let offset = message.offset();
                if offset >= self.next_offset() {
                    self.next_offset.store(offset + 1, Ordering::Relaxed);

                    let payload = message.payload().unwrap_or(&[]);
                    let (buffer, errors) = parser.parse(payload);
                    let len = buffer.len();
                    self.messages.lock().unwrap().insert(Msg {
                        offset,
                        buffer,
                        errors,
                    });
                    consumer.buffered(len, payload.len());
                } else {
                    tracing::error!(
                        "Received message in partition {} at out-of-order offset {offset}",
                        self.partition
                    );
                }
            }
        }
    }

    fn run(
        &self,
        base_consumer: &BaseConsumer<KafkaFtInputContext>,
        consumer: &dyn InputConsumer,
        parser: &mut dyn Parser,
    ) -> bool {
        let next_offset = self.next_offset();
        let max_offset = self.max_offset();
        if next_offset > max_offset {
            return false;
        }

        match self.queue.poll(Duration::ZERO) {
            Some(message) => {
                self.handle_kafka_message(base_consumer, consumer, parser, message);
                true
            }
            None => false,
        }
    }
}

struct RecvThreadHandle {
    exit: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
    unparker: Unparker,
}

impl RecvThreadHandle {
    fn new(mut thread: RecvThread) -> RecvThreadHandle {
        Self {
            exit: thread.exit.clone(),
            unparker: thread.parker.unparker().clone(),
            join_handle: {
                Some(spawn(move || {
                    //let _guard = span(&endpoint.config);
                    thread.run();
                }))
            },
        }
    }
}

impl Drop for RecvThreadHandle {
    /// When we're dropped, make the thread exit.
    ///
    /// *Careful*: `exit` is shared with all the helper threads, so if one gets
    /// dropped, the others will exit too. Currently this is OK because we
    /// always drop all of them together. It is in fact desirable because if
    /// they had separate `should_exit` flags then we'd have to block up to
    /// `POLL_TIMEOUT` per thread whereas since it is shared we will only block
    /// that long once.
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);

        let _ = self.join_handle.take().map(|handle| {
            self.unparker.unpark();
            handle.join()
        });
    }
}
struct RecvThread {
    exit: Arc<AtomicBool>,
    main_thread: Thread,
    parker: Parker,
    base_consumer: Arc<BaseConsumer<KafkaFtInputContext>>,
    consumer: Box<dyn InputConsumer>,
    parser: Box<dyn Parser>,
    receivers: Vec<Arc<PartitionReceiver>>,
}

impl RecvThread {
    fn run(&mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            let mut did_work = false;
            for receiver in &self.receivers {
                if receiver.run(&self.base_consumer, &*self.consumer, &mut *self.parser) {
                    did_work = true;
                }
            }
            if did_work {
                self.main_thread.unpark();
            } else {
                self.parker.park_timeout(Duration::from_secs(1));
            }
        }
    }
}
