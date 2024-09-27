use super::{count_partitions_in_topic, Ctp, DataConsumerContext, ErrorHandler, POLL_TIMEOUT};
use crate::transport::kafka::ft::check_fatal_errors;
use crate::transport::{InputEndpoint, InputQueue, InputReader, Step};
use crate::{InputConsumer, Parser, TransportInputEndpoint};
use anyhow::{anyhow, bail, Context, Error as AnyError, Result as AnyResult};
use crossbeam::sync::{Parker, Unparker};
use feldera_types::program_schema::Relation;
use feldera_types::transport::kafka::KafkaInputConfig;
use futures::executor::block_on;
use log::{debug, error, info, warn};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
use rdkafka::config::{FromClientConfig, FromClientConfigAndContext};
use rdkafka::consumer::base_consumer::PartitionQueue;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::producer::{BaseRecord, DeliveryResult, ProducerContext, ThreadedProducer};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::{consumer::Consumer, producer::Producer, Message, Offset};
use rdkafka::{ClientContext, TopicPartitionList};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize, Serializer};
use std::cmp::{max, Ordering};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::thread::{self};
use std::time::Duration;
use std::{
    error::Error,
    fmt::Result as FmtResult,
    ops::Range,
    sync::{Arc, Mutex},
    thread::{Builder, JoinHandle},
};

use super::CommonConfig;

/// Validated version of [`KafkaInputConfig`], converted into a convenient form
/// for internal use.
#[derive(Clone)]
struct Config {
    /// Common with output configuration.
    common: CommonConfig,

    /// Topics to be read.
    data_topics: Vec<String>,

    /// The index topic suffix.
    index_suffix: String,

    /// Create index topics if they are missing?
    create_missing_index: bool,

    /// Maximum number of bytes in a step.
    max_step_bytes: usize,

    /// Maximum number of messages in a step.
    max_step_messages: i64,
}

impl TryFrom<KafkaInputConfig> for Config {
    type Error = AnyError;

    fn try_from(source: KafkaInputConfig) -> Result<Self, Self::Error> {
        let ft = source.fault_tolerance.as_ref().unwrap();
        let max_step_bytes = ft.max_step_bytes.unwrap_or(u64::MAX).max(1);
        let max_step_messages = ft.max_step_messages.unwrap_or(u64::MAX).max(1);

        let common = CommonConfig::new(
            &source.kafka_options,
            &ft.consumer_options,
            &ft.producer_options,
            source.log_level,
        )?;
        let index_suffix = match ft.index_suffix {
            Some(ref s) => s.clone(),
            None => "_input-index".into(),
        };

        if source.topics.is_empty() {
            bail!("`data_topics` must name at least one data topic.");
        }

        Ok(Config {
            common,
            data_topics: source.topics.clone(),
            index_suffix,
            create_missing_index: ft.create_missing_index.unwrap_or(true),
            max_step_bytes: max_step_bytes.try_into().unwrap_or(usize::MAX),
            max_step_messages: max_step_messages.try_into().unwrap_or(i64::MAX),
        })
    }
}

/// Fault-tolerant Kafka input endpoint.
pub struct Endpoint(Arc<Config>);

impl Endpoint {
    pub fn new(config: KafkaInputConfig) -> AnyResult<Self> {
        Ok(Self(Arc::new(config.try_into()?)))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum OkAction {
    Run(Step),
    Pause,
}

/// Request made of the `WorkerThread`:
///
/// - `Ok(action)`: Run or pause.
///
/// - `Err(ExitRequest)`: All done, please exit.
///
/// Representing an exit request as `Err` allows it to be implemented via `?`.
type Action = Result<OkAction, ExitRequest>;

/// Error type to represent that the worker thread should exit.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct ExitRequest;
impl Display for ExitRequest {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "ExitRequest")
    }
}
impl Error for ExitRequest {}

/// A reader for fault-tolerant Kafka input.
///
/// Code outside this module accesses this as `dyn InputReader`.
struct Reader {
    action: Arc<Mutex<Action>>,
    complete_step: Arc<Mutex<Option<Step>>>,
    unparker: Unparker,
    join_handle: Option<JoinHandle<()>>,
    queue: Arc<InputQueue>,
}

impl Reader {
    fn set_action(&self, new_action: Action) {
        let mut action = self.action.lock().unwrap();
        if *action != new_action {
            *action = new_action;
            self.unparker.unpark();
        }
    }
}

impl InputReader for Reader {
    fn start(&self, step: Step) -> AnyResult<()> {
        self.set_action(Ok(OkAction::Run(step)));
        Ok(())
    }

    fn pause(&self) -> AnyResult<()> {
        self.set_action(Ok(OkAction::Pause));
        Ok(())
    }

    fn complete(&self, new_step: Step) {
        let mut complete_step = self.complete_step.lock().unwrap();
        match *complete_step {
            Some(step) if new_step <= step => (),
            _ => {
                *complete_step = Some(new_step);
                self.unparker.unpark();
            }
        }
    }

    fn disconnect(&self) {
        self.set_action(Err(ExitRequest));
    }

    fn flush(&self, n: usize) -> usize {
        self.queue.flush(n)
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.set_action(Err(ExitRequest));
        if let Some(join_handle) = self.join_handle.take() {
            // As a corner case, `Reader` might get dropped from a callback
            // executed from the worker thread.  We must not join ourselves
            // because that can cause a panic.
            if join_handle.thread().id() != thread::current().id() {
                let _ = join_handle.join();
            }
        }
    }
}

impl Reader {
    fn new(
        config: &Arc<Config>,
        start_step: Step,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
    ) -> Self {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        let action = Arc::new(Mutex::new(Ok(OkAction::Pause)));
        let complete_step = Arc::new(Mutex::new(None));
        let queue = Arc::new(InputQueue::new(consumer.clone()));
        let join_handle = {
            let queue = queue.clone();
            Some(WorkerThread::spawn(
                &action,
                &complete_step,
                start_step,
                parker,
                config,
                consumer,
                parser,
                queue,
            ))
        };
        Reader {
            action,
            complete_step,
            unparker,
            join_handle,
            queue,
        }
    }
}

struct PollerThread {
    join_handle: Option<JoinHandle<()>>,
    exit_request: Arc<AtomicBool>,
}

impl PollerThread {
    fn new<C: ConsumerContext + 'static>(
        consumer: &Arc<BaseConsumer<C>>,
        receiver: &Arc<Box<dyn InputConsumer>>,
    ) -> Self {
        let exit_request = Arc::new(AtomicBool::new(false));
        let join_handle = Some(
            Builder::new()
                .name("poller(kafka-ft)".into())
                .spawn({
                    let consumer = consumer.clone();
                    let receiver = receiver.clone();
                    let exit_request = exit_request.clone();
                    move || Self::run(consumer, receiver, exit_request)
                })
                .unwrap(),
        );
        Self {
            join_handle,
            exit_request,
        }
    }

    fn run<C: ConsumerContext>(
        consumer: Arc<BaseConsumer<C>>,
        receiver: Arc<Box<dyn InputConsumer>>,
        exit_request: Arc<AtomicBool>,
    ) {
        while !exit_request.load(AtomicOrdering::Acquire) {
            match consumer.poll(POLL_TIMEOUT).transpose() {
                Ok(None) => (),
                Ok(Some(_)) => {
                    // All of the subscribed partitions should have be broken
                    // off into `PartitionQueue`s.
                    warn!("ignoring message received from consumer despite `split_partition_queue`")
                }
                Err(error) => {
                    let fatal = error
                        .rdkafka_error_code()
                        .is_some_and(|code| code == RDKafkaErrorCode::Fatal);
                    if !fatal {
                        receiver.error(false, error.into())
                    } else {
                        // The client will detect this later.
                    }
                }
            }
        }
    }
}

impl Drop for PollerThread {
    fn drop(&mut self) {
        self.exit_request.store(true, AtomicOrdering::Release);
        let _ = self.join_handle.take().unwrap().join();
    }
}

struct WorkerThread {
    action: Arc<Mutex<Action>>,
    complete_step: Arc<Mutex<Option<Step>>>,
    start_step: Step,
    parker: Parker,
    config: Arc<Config>,
    receiver: Arc<Box<dyn InputConsumer>>,
    parser: Box<dyn Parser>,
    queue: Arc<InputQueue>,
}

impl WorkerThread {
    #[allow(clippy::too_many_arguments)]
    fn spawn(
        action: &Arc<Mutex<Action>>,
        complete_step: &Arc<Mutex<Option<Step>>>,
        start_step: Step,
        parker: Parker,
        config: &Arc<Config>,
        receiver: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        queue: Arc<InputQueue>,
    ) -> JoinHandle<()> {
        let receiver = Arc::new(receiver);
        let worker_thread = Self {
            action: action.clone(),
            complete_step: complete_step.clone(),
            start_step,
            parker,
            config: config.clone(),
            receiver: receiver.clone(),
            parser,
            queue,
        };
        Builder::new()
            .name(format!(
                // Use a very short prefix because Linux truncates thread names
                // after 15 bytes.
                "dkw-{}",
                config.data_topics.first().unwrap_or(&"".into())
            ))
            .spawn(move || {
                if let Err(error) = worker_thread.run() {
                    if error.downcast_ref::<ExitRequest>().is_some() {
                        // Normal termination because of a requested exit.
                    } else {
                        error!("Fault-tolerant Kafka input endpoint failed due to: {error:#}");
                        receiver.error(true, error);
                    }
                }
            })
            .unwrap()
    }

    /// Reads and returns the currently requested `Action`.
    fn action(&self) -> Action {
        *self.action.lock().unwrap()
    }

    /// Wait for `reader.start()` to be called.
    fn wait_for_pipeline_start(&self, step: Step) -> AnyResult<()> {
        loop {
            if let OkAction::Run(up_to_step) = self.action()? {
                if step <= up_to_step {
                    return Ok(());
                }
            }
            self.parker.park_timeout(POLL_TIMEOUT);
        }
    }

    fn is_completion_requested(&self, step: Step) -> bool {
        match *self.complete_step.lock().unwrap() {
            Some(complete_step) => step <= complete_step,
            None => false,
        }
    }

    fn run(mut self) -> AnyResult<()> {
        self.wait_for_pipeline_start(self.start_step)?;

        // Read the index for each partition.
        let mut topics = Vec::with_capacity(self.config.data_topics.len());
        let index_suffix = &self.config.index_suffix;
        let mut assignment = TopicPartitionList::new();
        for data_topic in &self.config.data_topics {
            let index_topic = format!("{data_topic}{index_suffix}");
            let index = IndexReader::new(data_topic, &self.config, |error| {
                self.receiver.error(false, error)
            })
            .with_context(|| format!("Failed to read index topic {index_topic}"))?;
            let positions = index.find_step(self.start_step).with_context(|| {
                format!("Failed to find step {} in {index_topic}", self.start_step)
            })?;
            let index_partitions = index.into_partitions();

            for (index, p) in positions.iter().enumerate() {
                let index = index as i32;
                assignment.add_partition_offset(
                    data_topic,
                    index,
                    Offset::Offset(p.data_offset),
                )?;
                assignment.add_partition_offset(
                    &index_topic,
                    index,
                    Offset::Offset(p.index_offset),
                )?;
            }
            topics.push((data_topic, index_topic, positions, index_partitions));
        }
        let n_partitions = topics.iter().map(|x| x.2.len()).sum();

        // Create a consumer.
        let receiver_clone = self.receiver.clone();
        let consumer = Arc::new(
            BaseConsumer::from_config_and_context(
                &self.config.common.data_consumer_config,
                DataConsumerContext::new(move |error| receiver_clone.error(false, error)),
            )
            .context("Failed to create consumer")?,
        );

        let consumer_eh = ErrorHandler::new(consumer.client());

        consumer
            .assign(&assignment)
            .context("Failed to assign topics and partitions")?;

        let mut partitions = Vec::with_capacity(n_partitions);
        let mut data_queues = Vec::with_capacity(n_partitions);
        for (data_topic, index_topic, positions, index_partitions) in topics.iter() {
            for (index, p) in index_partitions.iter().enumerate() {
                let index = index as i32;
                let index_ctp = Ctp::new(&consumer, index_topic, index);
                let index_queue = index_ctp.split_partition_queue()?;
                let data_ctp = Ctp::new(&consumer, data_topic, index);
                let mut data_queue = data_ctp.split_partition_queue()?;
                let unparker = self.parker.unparker().clone();
                data_queue.set_nonempty_callback(move || unparker.unpark());
                data_queues.push(data_queue);
                partitions.push(Partition {
                    index,
                    index_ctp,
                    index_queue,
                    data_ctp,
                    next_offset: positions[index as usize].data_offset,
                    start_offset: None,
                    steps: p.steps.clone(),
                });
            }
        }
        let _poller_thread = PollerThread::new(&consumer, &self.receiver);

        // Create a Kafka producer for adding to the index topic.
        debug!("creating Kafka producer for index topics");
        let expected_deliveries = Arc::new(Mutex::new(HashMap::new()));
        let producer = ThreadedProducer::from_config_and_context(
            &self.config.common.producer_config,
            IndexProducerContext::new(&self.receiver, &expected_deliveries),
        )
        .context("creating Kafka producer for index topics failed")?;

        // Fetch the watermarks for the topic we're going to produce to, and
        // then throw away the information because we don't care.  librdkafka
        // needs this information, and it will fetch it for itself eventually,
        // but it likes to wait 1 full second before asking for it and fetching
        // it immediately avoids that delay.
        for (_data_topic, index_topic, positions, _index_partitions) in topics.iter() {
            for partition in 0..positions.len() {
                let producer_ctp = Ctp::new(&producer, index_topic, partition as i32);
                ErrorHandler::new(producer.client())
                    .retry_errors(
                        || producer_ctp.fetch_watermarks(POLL_TIMEOUT),
                        producer.client(),
                        || self.action(),
                    )
                    .with_context(|| format!("Fetching watermark for {producer_ctp} failed"))?;
            }
        }

        // Each loop iteration produces one step.
        let mut next_partition = 0;
        let mut saved_message = None;
        for step in self.start_step.. {
            self.receiver.start_step(step);
            self.wait_for_pipeline_start(step)?;
            check_fatal_errors(consumer.client()).context("Consumer reported fatal error")?;
            check_fatal_errors(producer.client()).context("Producer reported fatal error")?;

            // Get all of the messages already written to the step from any
            // partitions that already have them.
            let mut n_prewritten = 0;
            for p in &mut partitions {
                if step >= p.steps.end {
                    continue;
                }
                n_prewritten += 1;

                // Read the `IndexEntry` that tells us where to get the step's data.
                let index_message = consumer_eh
                    .read_partition_queue(&consumer, &p.index_queue, || self.action())
                    .with_context(|| format!("Failed to read {} partition queue", p.index_ctp))?;
                let payload = index_message.payload().unwrap_or(&[]);
                let index_entry: IndexEntry =
                    serde_json::from_slice(payload).with_context(|| {
                        format!(
                            "Failed to parse index entry in message at offset {} in {}",
                            index_message.offset(),
                            p.index_ctp
                        )
                    })?;
                if let Some(error) = {
                    if index_entry.step != step {
                        Some(format!("IndexEntry should be for step {step}"))
                    } else if p.next_offset > index_entry.data_offsets.start {
                        Some(format!("data offset less than {}", p.next_offset))
                    } else {
                        None
                    }
                } {
                    bail!(
                        "bad IndexEntry at {} offset {} ({error}): {index_entry:?}",
                        p.index_ctp,
                        index_message.offset()
                    );
                }

                // Read the step's data.
                while p.next_offset < index_entry.data_offsets.end {
                    let data_message = consumer_eh
                        .read_partition_queue(&consumer, &data_queues[p.index as usize], || {
                            self.action()
                        })
                        .with_context(|| {
                            format!("Failed to read {} partition queue.", p.data_ctp)
                        })?;
                    if data_message.offset() < index_entry.data_offsets.start {
                        continue;
                    }
                    if let Some(payload) = data_message.payload() {
                        self.queue.push(payload.len(), self.parser.parse(payload));
                    }
                    p.next_offset = data_message.offset() + 1;
                }
            }

            if n_prewritten == 0 {
                // Add messages to the step.
                let mut lack_of_progress = 0;

                let mut n_messages = 0;
                let mut n_bytes = 0;
                'assemble: while !self.is_completion_requested(step)
                    && n_messages < self.config.max_step_messages
                    && n_bytes < self.config.max_step_bytes
                {
                    let p = &mut partitions[next_partition];

                    self.wait_for_pipeline_start(step)?;
                    check_fatal_errors(consumer.client())
                        .context("Consumer reported fatal error")?;
                    check_fatal_errors(producer.client())
                        .context("Producer reported fatal error")?;

                    // If there's a saved message, take it, otherwise try to
                    // fetch a message.  Because we resume where we left off,
                    // any saved message is for `next_partition`.
                    let data_message = if let Some(message) = saved_message.take() {
                        Ok(Some(message))
                    } else {
                        data_queues[next_partition]
                            .poll(Duration::ZERO)
                            .transpose()
                            .with_context(|| {
                                format!("Failed to read {} partition queue.", p.data_ctp)
                            })
                    };

                    match data_message? {
                        Some(data_message)
                            if n_messages == 0
                                || n_bytes + data_message.payload_len()
                                    <= self.config.max_step_bytes =>
                        {
                            // We got a message that fits within the byte
                            // limit (or there are no messages in the current
                            // step).

                            if let Some(payload) = data_message.payload() {
                                self.queue.push(payload.len(), self.parser.parse(payload));
                            }
                            n_messages += 1;
                            n_bytes += data_message.payload_len();

                            p.start_offset.get_or_insert(data_message.offset());
                            p.next_offset = data_message.offset() + 1;
                            lack_of_progress = 0;
                        }
                        Some(data_message) => {
                            // We got a message but it would overflow the byte
                            // limit for the step.  Save it for the next step.
                            //
                            // We don't advance `next_partition` so that we'll
                            // resume in the same place.
                            saved_message = Some(data_message);
                            break 'assemble;
                        }
                        None => lack_of_progress += 1,
                    }

                    next_partition += 1;
                    if next_partition >= n_partitions {
                        next_partition = 0;
                    }

                    if lack_of_progress >= n_partitions {
                        // Wait for at least one of these to happen:
                        //   - A message to arrive in one of the data partitions.
                        //   - A completion request for this step.
                        //   - A request to exit.
                        self.parker.park();
                        lack_of_progress = 0;
                    }
                }
            } else {
                // At least some of the partitions for this step were written to
                // the index in a previous run.  In the common case, all of the
                // partitions were written; a partial write only happens on a
                // crash.
                //
                // We have to keep the partitions that were written.  We could
                // add to the other partitions, if any, but that would slightly
                // complicate the loop above and there's little value in it.
            }

            // Complete the step.
            if n_prewritten < n_partitions {
                // Prepare to count deliveries so we can later report that the
                // step has committed.
                expected_deliveries
                    .lock()
                    .unwrap()
                    .insert(step, n_partitions - n_prewritten);

                // Write an index entry in each partition that doesn't already
                // have one.
                for p in &mut partitions {
                    if step >= p.steps.end {
                        let offsets = p.start_offset.unwrap_or(p.next_offset)..p.next_offset;
                        p.start_offset = None;

                        // The errors that writing to a partition produces are ones that we can
                        // reasonably treat as fatal, whether Kafka considers them fatal or not.
                        let producer_ctp = Ctp::new(&producer, p.index_ctp.topic, p.index);
                        IndexEntry::new(step, offsets)
                            .write(&producer_ctp)
                            .with_context(|| {
                                format!("Failed to write {producer_ctp} index entry.")
                            })?;
                    }
                }
            } else {
                // This is a fully pre-existing step that has already committed.
                self.receiver.committed(step);
            }
        }
        unreachable!()
    }
}

struct Partition<'a, F>
where
    F: Fn(AnyError) + Send + Sync,
{
    index: i32,
    index_ctp: Ctp<'a, Arc<BaseConsumer<DataConsumerContext<F>>>, DataConsumerContext<F>>,
    index_queue: PartitionQueue<DataConsumerContext<F>>,
    data_ctp: Ctp<'a, Arc<BaseConsumer<DataConsumerContext<F>>>, DataConsumerContext<F>>,
    next_offset: i64,
    start_offset: Option<i64>,
    steps: Range<Step>,
}

impl InputEndpoint for Endpoint {
    fn steps(&self) -> AnyResult<Range<Step>> {
        let mut steps = None;
        for data_topic in &self.0.data_topics {
            let Range {
                start: new_start,
                end: new_end,
            } = IndexReader::new(data_topic, &self.0, |error| warn!("{error:#}"))
                .with_context(|| format!("Failed to read index for {data_topic}"))?
                .steps();
            steps = Some(steps.map_or(
                new_start..new_end,
                |Range {
                     start: old_start,
                     end: old_end,
                 }| { max(old_start, new_start)..max(old_end, new_end) },
            ));
        }
        Ok(steps.unwrap())
    }

    fn is_fault_tolerant(&self) -> bool {
        true
    }

    fn expire(&self, _step: Step) {}
}

impl TransportInputEndpoint for Endpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        start_step: Step,
        _schema: Relation,
    ) -> AnyResult<Box<dyn InputReader>> {
        Ok(Box::new(Reader::new(&self.0, start_step, consumer, parser)))
    }
}

struct IndexProducerContext {
    consumer: Arc<Box<dyn InputConsumer>>,

    /// Maps from a step number to the number of messages that remain to be
    /// delivered before that step is considered committed.
    expected_deliveries: Arc<Mutex<HashMap<Step, usize>>>,
}

impl IndexProducerContext {
    fn new(
        consumer: &Arc<Box<dyn InputConsumer>>,
        expected_deliveries: &Arc<Mutex<HashMap<Step, usize>>>,
    ) -> Self {
        Self {
            consumer: consumer.clone(),
            expected_deliveries: expected_deliveries.clone(),
        }
    }
}

impl ClientContext for IndexProducerContext {
    fn error(&self, error: KafkaError, reason: &str) {
        let fatal = error
            .rdkafka_error_code()
            .is_some_and(|code| code == RDKafkaErrorCode::Fatal);
        if !fatal {
            self.consumer.error(false, anyhow!(reason.to_string()));
        } else {
            // The caller will detect this later and bail out with it as its
            // final action.
        }
    }
}

impl ProducerContext for IndexProducerContext {
    type DeliveryOpaque = Box<Step>;
    fn delivery(&self, delivery_result: &DeliveryResult, step: Self::DeliveryOpaque) {
        match delivery_result {
            Ok(_) => {
                if let Entry::Occupied(mut o) =
                    self.expected_deliveries.lock().unwrap().entry(*step)
                {
                    let count = o.get_mut();
                    if *count > 1 {
                        *count -= 1;
                        return;
                    } else {
                        o.remove();
                    }
                } else {
                    return;
                }

                // We do this after the above to avoid holding the lock on
                // `expected_deliveries`.
                self.consumer.committed(*step);
            }
            Err(_) => {
                // If there's an error, we don't want to ever report that the
                // step committed, and we don't want to leak an entry in
                // `expected_deliveries` either.  This has both effects.
                self.expected_deliveries.lock().unwrap().remove(&*step);
            }
        }
    }
}

/// Type of the records stored in a index partition, to associate a DBSP step
/// number with the corresponding range of event offsets in the data partition.
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, Hash)]
struct IndexEntry {
    step: Step,
    #[serde(serialize_with = "serialize_range")]
    data_offsets: Range<i64>,
}

impl IndexEntry {
    // Create and return a new `IndexEntry`.
    fn new(step: Step, data_offsets: Range<i64>) -> Self {
        Self { step, data_offsets }
    }

    /// Reads and returns an `IndexEntry` from `ctp` at Kafka event offset
    /// `offset`.
    fn read<C: ClientContext + ConsumerContext>(
        ctp: &Ctp<BaseConsumer<C>, C>,
        offset: i64,
    ) -> AnyResult<IndexEntry> {
        Self::from_message(&ctp.read_at_offset(offset)?).with_context(|| {
            format!("message at offset {offset} in {ctp} should be valid IndexEntry")
        })
    }

    /// Writes this `IndexEntry` to `ctp`.
    fn write<C: ClientContext + ProducerContext<DeliveryOpaque = Box<u64>>>(
        &self,
        ctp: &Ctp<ThreadedProducer<C>, C>,
    ) -> KafkaResult<()> {
        let json = serde_json::to_string(self).unwrap();
        let key = self.step.to_string();
        let record = BaseRecord::with_opaque_to(ctp.topic, Box::new(self.step))
            .partition(ctp.partition)
            .payload(&json)
            .key(&key);
        ctp.client.send(record).map_err(|(error, _record)| error)
    }

    fn from_message<M>(msg: &M) -> AnyResult<IndexEntry>
    where
        M: Message,
    {
        Ok(serde_json::from_slice(msg.payload().unwrap_or(&[]))?)
    }
}

/// Serializes `r` as `[start, end]` rather than as `["start": start, "end":
/// end]`.
fn serialize_range<S: Serializer>(r: &Range<i64>, serializer: S) -> Result<S::Ok, S::Error> {
    let mut tup = serializer.serialize_tuple(2)?;
    tup.serialize_element(&r.start)?;
    tup.serialize_element(&r.end)?;
    tup.end()
}

#[cfg(test)]
#[test]
fn test_index_entry() {
    let data = r#"
{
    "step": 123,
    "data_offsets": [12, 23]
}
"#;
    assert_eq!(
        serde_json::from_str::<IndexEntry>(data).unwrap(),
        IndexEntry::new(123, 12..23)
    );
}

/// Index reader for fault-tolerant Kafka input and output.
///
/// The key to repeatability for Kafka input and output is to ensure that events
/// in the data topic are always divided into steps in the same way.  We do this
/// by tracking the division into steps in a separate "index" topic that
/// consists of [`IndexEntry`] events, each of which maps a range of Kafka
/// events in the data topic to a step number.
///
/// `IndexReader` provides an interface for reading the index topic.
struct IndexReader<E>
where
    E: Fn(AnyError) + Send + Sync + Clone,
{
    consumer: BaseConsumer<DataConsumerContext<E>>,
    index_topic: String,

    partitions: Vec<IndexPartition>,
}

struct IndexPartition {
    /// The range of valid Kafka event offsets in the index topic.
    watermarks: Range<i64>,

    /// The range of valid step numbers.
    steps: Range<Step>,

    /// The Kafka event offset in the data topic at which the next step's data
    /// will begin.  (The next step will be numbered `steps.end`.)
    next_step_start: i64,
}

impl<E> IndexReader<E>
where
    E: Fn(AnyError) + Send + Sync + Clone,
{
    /// Creates `topic` with `n_partitions` partitions using the admin client
    /// from `config`, and then blocks until the topic creation is complete.
    fn create_topic(topic: &str, n_partitions: i32, config: &Config) -> AnyResult<()> {
        let admin_client = AdminClient::from_config(&config.common.admin_config)?;
        let new_topic = NewTopic::new(
            topic,
            n_partitions,
            rdkafka::admin::TopicReplication::Fixed(-1),
        );
        block_on(admin_client.create_topics(&[new_topic], &AdminOptions::new()))?;

        Ok(())
    }

    fn count_or_create_topic<C: ConsumerContext>(
        consumer: &impl Consumer<C>,
        config: &Config,
        topic: &str,
        n_partitions: i32,
    ) -> AnyResult<usize> {
        match count_partitions_in_topic(consumer, topic) {
            Ok(n) => Ok(n),
            Err(error) => {
                if let Some(kafka_error) = error.downcast_ref::<KafkaError>() {
                    let code = kafka_error.rdkafka_error_code();
                    if code == Some(RDKafkaErrorCode::UnknownTopicOrPartition) {
                        info!("Index topic {topic} does not yet exist, creating with {n_partitions} partitions");
                        Self::create_topic(topic, n_partitions, config)?;
                        return Ok(n_partitions as usize);
                    }
                }
                Err(error)
            }
        }
    }

    /// Creates a new `IndexReader` for the index topic corresponding to
    /// `data_topic` using configuration `config`.  Reports transient errors via
    /// `error_cb`.
    ///
    /// This creates a "seekable consumer" that consumes 100% CPU in a separate
    /// `librdkafka`-owned thread.  This means that it's important to drop the
    /// `IndexReader` when it's no longer in active use.  (It would make sense
    /// for `IndexReader` to pause this consumer, eliminating the CPU drain,
    /// when we're not using it, but in some cases resuming the consumer causes
    /// a multi-second delay.)
    fn new(data_topic: &str, config: &Config, error_cb: E) -> AnyResult<Self> {
        let index_topic = format!("{data_topic}{}", config.index_suffix);
        let context = DataConsumerContext::new(error_cb.clone());
        let consumer =
            BaseConsumer::from_config_and_context(&config.common.seekable_consumer_config, context)
                .context("Failed to open seekable consumer")?;

        let n_partitions = consumer
            .context()
            .deferred_logging
            .with_deferred_logging(|| count_partitions_in_topic(&consumer, data_topic))?;
        let index_partitions = if config.create_missing_index {
            Self::count_or_create_topic(&consumer, config, &index_topic, n_partitions as i32)?
        } else {
            count_partitions_in_topic(&consumer, &index_topic)?
        };
        if n_partitions != index_partitions {
            bail!("{data_topic} and {index_topic} should have the same number of partitions, but {data_topic} has {n_partitions} and {index_topic} has {index_partitions}");
        }
        if n_partitions == 0 {
            bail!("{data_topic} has 0 partitions but it should have at least 1");
        }

        let mut partitions = Vec::with_capacity(n_partitions);
        for partition in 0..n_partitions as i32 {
            let ctp = Ctp::new(&consumer, &index_topic, partition);

            let watermarks = ctp
                .fetch_watermarks_patiently()
                .with_context(|| format!("Failed to fetch watermarks for {ctp}"))?;

            let (steps, next_step_start) = if !watermarks.is_empty() {
                let first = IndexEntry::read(&ctp, watermarks.start)?;
                let Some(last) = ctp.read_last_message(&watermarks)? else {
                    bail!("{ctp} was not previously empty but its last message cannot be read");
                };
                let last = IndexEntry::from_message(&last)?;
                (first.step..(last.step + 1), last.data_offsets.end)
            } else {
                if watermarks.end > 0 {
                    // The partition is empty, but it has nonzero watermarks:
                    //
                    // - If it once had some content, which is now all deleted or expired, we can't
                    //   continue because we need to know about at least the most recent step.
                    //
                    // - Maybe it has always been empty of real content, but a producer once started
                    //   a transaction and either aborted it or didn't write anything, and then the
                    //   segment was compacted.  We could have a heuristic for that by checking for
                    //   a relatively small `high` value, e.g. <1000.
                    //
                    // For now, just warn.
                    warn!(
                        "{ctp} is empty but it has nonzero high watermark {}",
                        watermarks.end
                    );
                }
                (0..0, 0)
            };
            debug!("index {ctp} has steps {steps:?} and watermarks {watermarks:?}");
            partitions.push(IndexPartition {
                watermarks,
                steps,
                next_step_start,
            });
        }
        Ok(Self {
            consumer,
            index_topic,
            partitions,
        })
    }

    /// Searches the index topic for `step` and returns the step's position in
    /// every partition.  If `step` is beyond the current end of a partition,
    /// returns a position just past the end; if `step` is before the current
    /// beginning of a partition, returns an error.
    fn find_step(&self, step: Step) -> AnyResult<Vec<StepPosition>> {
        let mut steps = Vec::with_capacity(self.partitions.len());
        for (index, partition) in self.partitions.iter().enumerate() {
            if step >= partition.steps.end {
                steps.push(StepPosition {
                    index_offset: partition.watermarks.end,
                    data_offset: partition.next_step_start,
                });
            } else if step >= partition.steps.start {
                let ctp = Ctp::new(&self.consumer, &self.index_topic, index as i32);
                let mut events = partition.watermarks.clone();
                loop {
                    assert!(!events.is_empty());
                    let offset = events.start + (events.end - events.start) / 2;
                    let entry = IndexEntry::read(&ctp, offset)?;
                    events = match step.cmp(&entry.step) {
                        Ordering::Equal => {
                            steps.push(StepPosition {
                                index_offset: offset,
                                data_offset: entry.data_offsets.start,
                            });
                            break;
                        }
                        Ordering::Greater => offset + 1..events.end,
                        Ordering::Less => events.start..offset,
                    };
                }
            } else {
                bail!("can't read step {} because it has already expired in partition {index} (the first unexpired step in that partition is {})",
                step, partition.steps.start);
            }
        }
        Ok(steps)
    }

    /// Returns the range of steps that at least partially exist.
    fn steps(&self) -> Range<Step> {
        let start = self.partitions.iter().map(|p| p.steps.start).max().unwrap();
        let end = self.partitions.iter().map(|p| p.steps.end).max().unwrap();
        start..end
    }

    fn into_partitions(self) -> Vec<IndexPartition> {
        self.partitions
    }
}

struct StepPosition {
    /// The Kafka event offset of `step` in the index topic.
    index_offset: i64,

    /// The Kafka event offset of the first message in `step` in the data topic.
    data_offset: i64,
}
