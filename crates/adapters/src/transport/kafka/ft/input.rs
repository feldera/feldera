use crate::transport::kafka::ft::count_partitions_in_topic;
use crate::transport::{InputEndpoint, InputReaderCommand};
use crate::{
    transport::{
        kafka::{rdkafka_loglevel_from, refine_kafka_error, DeferredLogging},
        secret_resolver::MaybeSecret,
        InputReader,
    },
    InputConsumer, TransportInputEndpoint,
};
use crate::{InputBuffer, ParseError, Parser};
use anyhow::{anyhow, Error as AnyError, Result as AnyResult};
use crossbeam::queue::ArrayQueue;
use feldera_types::program_schema::Relation;
use feldera_types::{secret_ref::MaybeSecretRef, transport::kafka::KafkaInputConfig};
use indexmap::IndexSet;
use log::debug;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::{
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext},
    error::{KafkaError, KafkaResult},
    ClientConfig, ClientContext, Message,
};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::mpsc::{channel, Receiver, RecvError, Sender, TryRecvError};
use std::thread::Thread;
use std::{
    cmp::max,
    collections::HashSet,
    iter, mem,
    sync::{Arc, Mutex, Weak},
    thread::spawn,
    time::Duration,
};

/// Poll timeout must be low, as it bounds the amount of time it takes to resume the connector.
const POLL_TIMEOUT: Duration = Duration::from_millis(5);

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
    command_sender: Sender<InputReaderCommand>,
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

struct KafkaFtInputReaderInner {
    kafka_consumer: BaseConsumer<KafkaFtInputContext>,
    errors: ArrayQueue<(KafkaError, String)>,
}

impl KafkaFtInputReaderInner {
    #[allow(clippy::borrowed_box)]
    fn poller_thread(
        &self,
        consumer: &Box<dyn InputConsumer>,
        mut parser: Box<dyn Parser>,
        topics: IndexSet<String>,
        partition_counts: Vec<usize>,
        command_receiver: Receiver<InputReaderCommand>,
    ) -> AnyResult<()> {
        // Start reading the partitions either at the resume point or at the
        // beginning.
        let mut assignment = TopicPartitionList::new();
        let mut buffered_messages: Vec<Vec<MessageBuffer>> = Vec::with_capacity(topics.len());
        let mut command_receiver = BufferedReceiver::new(command_receiver);
        match command_receiver.recv()? {
            InputReaderCommand::Seek(metadata) => {
                let metadata = serde_json::from_value::<Metadata>(metadata)?;
                let offsets = metadata.parse(&topics, &partition_counts)?;
                for (topic, partitions) in topics.iter().zip(offsets.into_iter()) {
                    let mut buffered_partition = Vec::with_capacity(partitions.len());
                    for (partition, offsets) in partitions.into_iter().enumerate() {
                        assignment
                            .add_partition_offset(
                                topic.as_str(),
                                partition as i32,
                                Offset::Offset(offsets.end),
                            )
                            .map_err(|error| self.refine_error(error).1)?;
                        buffered_partition.push(MessageBuffer::new(offsets.end));
                    }
                    buffered_messages.push(buffered_partition);
                }
            }
            InputReaderCommand::Disconnect => return Ok(()),
            other => {
                command_receiver.put_back(other);
                for (topic, n_partitions) in topics.iter().zip(partition_counts.iter()) {
                    for partition in 0..*n_partitions {
                        assignment
                            .add_partition_offset(
                                topic.as_str(),
                                partition as i32,
                                Offset::Beginning,
                            )
                            .map_err(|error| self.refine_error(error).1)?;
                    }
                    buffered_messages.push(
                        iter::repeat_with(|| MessageBuffer::new(0))
                            .take(*n_partitions)
                            .collect(),
                    );
                }
            }
        };
        self.kafka_consumer
            .assign(&assignment)
            .map_err(|error| self.refine_error(error).1)?;

        // Then replay as many steps as requested.
        loop {
            match command_receiver.recv()? {
                InputReaderCommand::Seek(_) => {
                    unreachable!("Seek must be the first input reader command")
                }
                InputReaderCommand::Disconnect => return Ok(()),
                InputReaderCommand::Replay(metadata) => {
                    let metadata = serde_json::from_value::<Metadata>(metadata)?;
                    let metadata = metadata.parse(&topics, &partition_counts)?;
                    let mut replayer = MetadataReplayer::new(&metadata);
                    let mut total_records = 0;
                    for (topic, partitions) in metadata.iter().enumerate() {
                        for (partition, offsets) in partitions.iter().enumerate() {
                            let buf = &mut buffered_messages[topic][partition];
                            buf.next_offset = max(buf.next_offset, offsets.end);

                            for (offset, mut msg) in buf.split_before(offsets.end) {
                                match replayer.received_offset(topic, partition, offset)? {
                                    ReplayAction::Replay => {
                                        consumer.parse_errors(msg.errors);
                                        total_records += msg.buffer.len();
                                        msg.buffer.flush_all()
                                    }
                                    ReplayAction::Defer => unreachable!(
                                        "`replay_messages` was split so that this couldn't happen."
                                    ),
                                };
                            }
                        }
                    }
                    while !replayer.is_complete() {
                        match self.kafka_consumer.poll(POLL_TIMEOUT) {
                            Some(Err(e)) => {
                                let (fatal, e) = self.refine_error(e);
                                consumer.error(fatal, e);
                                if fatal {
                                    return Ok(());
                                }
                            }
                            Some(Ok(message)) => {
                                let payload = message.payload().unwrap_or(&[]);
                                let (mut buffer, errors) = parser.parse(payload);
                                consumer.buffered(buffer.len(), payload.len());
                                let topic = Self::lookup_topic(&topics, message.topic())?;
                                let partition = message.partition() as usize;
                                match replayer.received_offset(
                                    topic,
                                    partition,
                                    message.offset(),
                                )? {
                                    ReplayAction::Replay => {
                                        consumer.parse_errors(errors);
                                        total_records += buffer.len();
                                        buffer.flush_all();
                                    }
                                    ReplayAction::Defer => {
                                        // Message is after the step we're replaying.
                                        let buf = &mut buffered_messages[topic][partition];
                                        buf.next_offset =
                                            max(buf.next_offset, message.offset() + 1);
                                        buf.messages
                                            .insert(message.offset(), Msg { buffer, errors });
                                    }
                                }
                            }
                            None => (),
                        }
                    }
                    consumer.replayed(total_records);
                }
                other => {
                    command_receiver.put_back(other);
                    break;
                }
            }
        }

        let mut running = false;
        let mut kafka_paused = false;
        loop {
            loop {
                match command_receiver.try_recv() {
                    Ok(command @ InputReaderCommand::Seek(_))
                    | Ok(command @ InputReaderCommand::Replay(_)) => {
                        unreachable!("{command:?} must be at the beginning of the command stream")
                    }
                    Ok(InputReaderCommand::Extend) => running = true,
                    Ok(InputReaderCommand::Pause) => running = false,
                    Ok(InputReaderCommand::Queue) => {
                        let mut total = 0;
                        let mut ranges = partition_counts
                            .iter()
                            .map(|n_partitions| iter::repeat(None).take(*n_partitions).collect())
                            .collect::<Vec<Vec<Option<Range<i64>>>>>();
                        while total < consumer.max_batch_size() {
                            let mut empty = true;
                            for (topic, partitions) in buffered_messages.iter_mut().enumerate() {
                                for (partition, buf) in partitions.iter_mut().enumerate() {
                                    if let Some((offset, mut msg)) = buf.messages.pop_first() {
                                        consumer.parse_errors(msg.errors);
                                        total += msg.buffer.flush_all();
                                        empty = false;

                                        let range = &mut ranges[topic][partition];
                                        match range {
                                            Some(range) => range.end = offset + 1,
                                            None => *range = Some(offset..offset + 1),
                                        }
                                    }
                                }
                            }
                            if empty {
                                break;
                            }
                        }
                        let offsets = topics
                            .iter()
                            .cloned()
                            .zip(ranges.into_iter().enumerate().map(|(topic, partitions)| {
                                partitions
                                    .into_iter()
                                    .enumerate()
                                    .map(|(partition, offsets)| {
                                        offsets.unwrap_or({
                                            let buf = &buffered_messages[topic][partition];
                                            buf.next_offset..buf.next_offset
                                        })
                                    })
                                    .collect::<Vec<_>>()
                            }))
                            .collect();
                        consumer
                            .extended(total, serde_json::to_value(&Metadata { offsets }).unwrap());
                    }
                    Ok(InputReaderCommand::Disconnect) | Err(TryRecvError::Disconnected) => {
                        return Ok(())
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }

            if !running && !kafka_paused {
                self.pause_partitions()
                    .map_err(|error| self.refine_error(error).1)?;
                kafka_paused = true;
            } else if running && kafka_paused {
                self.resume_partitions()
                    .map_err(|error| self.refine_error(error).1)?;
                kafka_paused = false;
            }

            // Keep polling even while the consumer is paused as `BaseConsumer`
            // processes control messages (including rebalancing and errors)
            // within the polling thread.
            match self.kafka_consumer.poll(POLL_TIMEOUT) {
                None => (),
                Some(Err(e)) => {
                    // println!("poll returned error");
                    let (fatal, e) = self.refine_error(e);
                    consumer.error(fatal, e);
                    if fatal {
                        return Ok(());
                    }
                }
                Some(Ok(message)) => {
                    let payload = message.payload().unwrap_or(&[]);
                    let (buffer, errors) = parser.parse(payload);
                    consumer.buffered(buffer.len(), payload.len());
                    let topic = Self::lookup_topic(&topics, message.topic())?;
                    let partition = message.partition() as usize;
                    let buf = buffered_messages[topic].get_mut(partition).ok_or_else(|| {
                        anyhow!("received message for nonexistent partition {partition}")
                    })?;
                    buf.next_offset = max(buf.next_offset, message.offset() + 1);
                    buf.messages
                        .insert(message.offset(), Msg { buffer, errors });
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
        }
    }

    fn lookup_topic(topics: &IndexSet<String>, topic: &str) -> AnyResult<usize> {
        topics.get_index_of(topic).ok_or_else(|| {
            anyhow!("received message for topic {topic:?}, which is not a configured topic")
        })
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

impl KafkaFtInputReader {
    fn new(
        config: &Arc<KafkaInputConfig>,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> AnyResult<Self> {
        // Create Kafka consumer configuration.
        debug!("Starting Kafka input endpoint: {:?}", config);

        let mut client_config = ClientConfig::new();

        for (key, value) in config.kafka_options.iter() {
            // If it is a secret reference, resolve it to the actual secret string
            match MaybeSecret::new_using_default_directory(
                MaybeSecretRef::new_using_pattern_match(value.clone()),
            )? {
                MaybeSecret::String(simple_string) => {
                    client_config.set(key, simple_string);
                }
                MaybeSecret::Secret(secret_string) => {
                    client_config.set(key, secret_string);
                }
            }
        }

        if let Some(log_level) = config.log_level {
            client_config.set_log_level(rdkafka_loglevel_from(log_level));
        }

        // Create Kafka consumer and count the number of partitions in each topic.
        //
        // This has the desirable side effect of ensuring that we can reach the
        // broker and failing with an error if we cannot.
        let context = KafkaFtInputContext::new();
        let kafka_consumer = BaseConsumer::from_config_and_context(&client_config, context)?;
        let topics = config.topics.iter().cloned().collect::<IndexSet<_>>();
        let mut partition_counts = Vec::with_capacity(topics.len());
        for topic in topics.iter() {
            partition_counts.push(count_partitions_in_topic(&kafka_consumer, topic)?);
        }

        let inner = Arc::new(KafkaFtInputReaderInner {
            kafka_consumer,
            errors: ArrayQueue::new(ERROR_BUFFER_SIZE),
        });
        *inner.kafka_consumer.context().endpoint.lock().unwrap() = Arc::downgrade(&inner);

        let (command_sender, command_receiver) = channel();
        let poller_handle = spawn({
            let endpoint = inner.clone();
            move || {
                if let Err(e) = endpoint.poller_thread(
                    &consumer,
                    parser,
                    topics,
                    partition_counts,
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
}

impl Drop for KafkaFtInputReader {
    fn drop(&mut self) {
        self.request(InputReaderCommand::Disconnect);
    }
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    /// Maps from a topic name to per-partition ranges of offsets.
    offsets: HashMap<String, Vec<Range<i64>>>,
}

impl Metadata {
    fn parse(
        mut self,
        topics: &IndexSet<String>,
        partition_counts: &[usize],
    ) -> AnyResult<Vec<Vec<Range<i64>>>> {
        let mut offsets = Vec::with_capacity(topics.len());
        for (topic_index, topic) in topics.iter().enumerate() {
            let Some(partitions) = self.offsets.remove(topic) else {
                return Err(anyhow!(
                    "metadata for replay lacks partition info for topic {topic:?}"
                ));
            };
            if partitions.len() != partition_counts[topic_index] {
                return Err(anyhow!("topic {topic:?} has {} partitions but metadata for replay has offsets for {} partitions",
                                   partition_counts[topic_index], partitions.len()));
            }
            offsets.push(partitions);
        }
        if let Some(extra) = self.offsets.keys().next() {
            return Err(anyhow!("{} extra topic(s) including {extra:?} are not configured but it appears in metadata for replay",
                               self.offsets.len()));
        }
        Ok(offsets)
    }
}

enum ReplayAction {
    Replay,
    Defer,
}

struct MetadataReplayer<'a> {
    incomplete_partitions: HashSet<(usize, usize)>,
    metadata: &'a Vec<Vec<Range<i64>>>,
}

impl<'a> MetadataReplayer<'a> {
    fn new(metadata: &'a Vec<Vec<Range<i64>>>) -> Self {
        let mut incomplete_partitions = HashSet::new();
        for (topic, partitions) in metadata.iter().enumerate() {
            for (partition, offsets) in partitions.iter().enumerate() {
                if !offsets.is_empty() {
                    incomplete_partitions.insert((topic, partition));
                }
            }
        }
        Self {
            incomplete_partitions,
            metadata,
        }
    }
    fn is_complete(&self) -> bool {
        self.incomplete_partitions.is_empty()
    }
    fn received_offset(
        &mut self,
        topic: usize,
        partition: usize,
        offset: i64,
    ) -> AnyResult<ReplayAction> {
        let offsets = self.metadata[topic]
            .get(partition)
            .ok_or_else(|| anyhow!("received message for nonexistent partition {partition}"))?;
        if offset < offsets.start {
            Err(anyhow!(
                "Received message in partition {partition} at out of order offset {offset}",
            ))
        } else if offset < offsets.end {
            if offset + 1 == offsets.end {
                self.incomplete_partitions.remove(&(topic, partition));
            }
            Ok(ReplayAction::Replay)
        } else {
            if self.incomplete_partitions.contains(&(topic, partition)) {
                return Err(anyhow!("Received message after the replay window before the last message in the replay window (partition {partition}, message offset {offset}, replay window {offsets:?}"));
            }
            Ok(ReplayAction::Defer)
        }
    }
}

/// Wraps [Receiver] with a one-message buffer, to allow received messages to be
/// put back and received again later.
struct BufferedReceiver<T> {
    receiver: Receiver<T>,
    buffer: Option<T>,
}

impl<T> BufferedReceiver<T> {
    fn new(receiver: Receiver<T>) -> Self {
        Self {
            receiver,
            buffer: None,
        }
    }

    fn recv(&mut self) -> Result<T, RecvError> {
        match self.buffer.take() {
            Some(value) => Ok(value),
            None => self.receiver.recv(),
        }
    }

    fn try_recv(&mut self) -> Result<T, TryRecvError> {
        match self.buffer.take() {
            Some(value) => Ok(value),
            None => self.receiver.try_recv(),
        }
    }

    fn put_back(&mut self, value: T) {
        assert!(self.buffer.is_none());
        self.buffer = Some(value);
    }
}

/// Parsed messages in a particular topic and partition that are not yet ready
/// to be flushed to the circuit.
struct MessageBuffer {
    /// The minimum message offset that we could receive next in this partition
    /// and offset.
    ///
    /// We process messages in order, so when `messages` is non-empty, this is
    /// the largest offset (key) in `messages` plus 1.
    next_offset: i64,

    /// Messages (and errors) that are not yet ready to flush.
    messages: BTreeMap<i64, Msg>,
}

impl MessageBuffer {
    fn new(next_offset: i64) -> Self {
        Self {
            next_offset,
            messages: BTreeMap::new(),
        }
    }

    fn split_before(&mut self, offset: i64) -> BTreeMap<i64, Msg> {
        let mut messages = self.messages.split_off(&offset);
        mem::swap(&mut self.messages, &mut messages);
        messages
    }
}

struct Msg {
    buffer: Option<Box<dyn InputBuffer>>,
    errors: Vec<ParseError>,
}
