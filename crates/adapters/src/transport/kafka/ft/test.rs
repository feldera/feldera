use crate::format::{Splitter, Sponge};
use crate::test::wait;
use crate::transport::kafka::ft::input::Metadata;
use crate::transport::{input_transport_config_to_endpoint, output_transport_config_to_endpoint};
use crate::{
    test::{
        kafka::{KafkaResources, TestProducer},
        test_circuit, TestStruct,
    },
    Controller, InputConsumer, ParseError, PipelineConfig,
};
use crate::{InputBuffer, InputReader, Parser, TransportInputEndpoint};
use anyhow::Error as AnyError;
use crossbeam::sync::{Parker, Unparker};
use csv::ReaderBuilder as CsvReaderBuilder;
use feldera_types::program_schema::Relation;
use rmpv::Value as RmpValue;
use serde_json::Value as JsonValue;
use std::fs::create_dir;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::atomic::AtomicUsize;
use std::thread::sleep;
use std::{
    mem,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

fn init_test_logger() {
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_test_writer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("debug"))
                .unwrap(),
        )
        .try_init();
}

#[test]
#[ignore]
fn test_kafka_output_errors() {
    init_test_logger();

    info!("test_kafka_output_errors: Test invalid Kafka broker address");

    let config_str = r#"
name: test
workers: 4
inputs:
outputs:
    test_output:
        stream: test_output1
        transport:
            name: kafka_output
            config:
                bootstrap.servers: localhost:11111
                topic: ft_end_to_end_test_output_topic
                start_from: earliest
        format:
            name: csv
"#;

    info!("test_kafka_output_errors: Creating circuit");

    info!("test_kafka_output_errors: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();

    match Controller::with_config(
        |workers| Ok(test_circuit::<TestStruct>(workers, &TestStruct::schema())),
        &config,
        Box::new(|e| panic!("error: {e}")),
    ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("test_kafka_output_errors: error: {e}"),
    }
}

fn create_reader(
    topic: &str,
) -> (
    Box<dyn TransportInputEndpoint>,
    DummyInputReceiver,
    Box<dyn InputReader>,
) {
    let config_str = format!(
        r#"
name: kafka_input
config:
    topic: {topic}
    log_level: debug
    start_from: earliest
"#
    );

    let endpoint =
        input_transport_config_to_endpoint(serde_yaml::from_str(&config_str).unwrap(), "", true)
            .unwrap()
            .unwrap();
    assert!(endpoint.is_fault_tolerant());

    let receiver = DummyInputReceiver::new();
    let reader = endpoint
        .open(
            receiver.consumer(),
            Box::new(DummyParser::new(&receiver)),
            Relation::empty(),
        )
        .unwrap();

    (endpoint, receiver, reader)
}

#[test]
fn single_input() {
    test_input("single_input_ft", &[10]);
}

#[test]
fn multiple_input() {
    test_input("multiple_input_ft", &[10, 20, 30, 40, 50]);
}

#[test]
fn empty_input() {
    test_input("empty_input_ft", &[10, 0, 20, 0, 50, 0]);
}

#[test]
fn empty_initial_input() {
    test_input("empty_initial_input_ft", &[0, 10, 0, 20, 0, 50]);
}

#[test]
fn big_input() {
    test_input("big_input", &[100, 1000, 10000]);
}

fn test_input(topic: &str, batch_sizes: &[u32]) {
    init_test_logger();

    let mut _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);

    let (_endpoint, receiver, reader) = create_reader(topic);
    reader.extend();

    fn metadata(batch: &Range<u32>) -> JsonValue {
        #[allow(clippy::single_range_in_vec_init)]
        let metadata = Metadata {
            offsets: vec![batch.start as i64..batch.end as i64],
        };
        serde_json::to_value(metadata).unwrap()
    }

    let n_batches = batch_sizes.len();
    let mut batches: Vec<Range<u32>> = Vec::with_capacity(n_batches);
    for batch_size in batch_sizes {
        let start = batches.last().map_or(0, |b| b.end);
        batches.push(start..start + batch_size);
    }

    // First, write batches of the specified sizes.
    let producer = TestProducer::new();
    for batch in &batches {
        let batch_size = batch.len();
        println!();

        // Write a batch to the topic and wait for the adapter to read it.
        println!("producing {batch_size} messages {batch:?} to {topic}");
        let input_batch = batch
            .clone()
            .map(|id| vec![TestStruct::for_id(id)])
            .collect::<Vec<_>>();
        producer.send_to_topic(&input_batch, topic);

        println!("waiting for connector to buffer the {batch_size} messages {batch:?}.");
        receiver.expect_buffering(batch.len());

        // Tell the adapter to queue the batch and wait for it to do it.
        println!("queuing and expecting {batch_size} records");
        reader.queue();
        receiver.expect(vec![ConsumerCall::Extended {
            num_records: batch.len(),
            metadata: metadata(batch),
        }]);

        // Make sure that the flushed batches were what we expected.
        println!("checking flushed batches against expectation");
        receiver.expect_flushed(batch);
    }
    drop(_endpoint);
    drop(receiver);
    drop(reader);

    // Then, execute all possible sequences that seek to a starting position,
    // replay zero or more batches, and then extend through the rest of the
    // data.
    for seek in 0..n_batches {
        for replay in 0..n_batches - seek {
            println!();
            println!("seeking to {seek}, replaying {replay} batches, and then reading the rest");
            let (_endpoint, receiver, reader) = create_reader(topic);
            receiver.inner.drop_buffered.store(true, Ordering::Release);

            if seek > 0 {
                println!("- seek to {seek}");
                reader.seek(metadata(&batches[seek - 1]));
            }
            for batch in &batches[seek..seek + replay] {
                println!("- replaying {batch:?}");
                reader.replay(metadata(batch), RmpValue::Nil);
                println!("expecting {} records", batch.len());
                receiver.expect(vec![ConsumerCall::Replayed {
                    num_records: batch.len(),
                }]);
                println!("checking {} flushed records {batch:?}", batch.len());
                receiver.expect_flushed(batch);
            }

            let final_start = if seek + replay > 0 {
                batches[seek + replay - 1].end
            } else {
                0
            };
            let final_end = batches.last().unwrap().end;
            let final_batch = final_start..final_end;
            println!("- reading the rest ({final_batch:?})");
            reader.extend();
            receiver.expect_n_buffers((final_batch.end - batches[seek].start) as usize);
            reader.queue();
            receiver.expect(vec![ConsumerCall::Extended {
                num_records: final_batch.len(),
                metadata: metadata(&final_batch),
            }]);
            receiver.expect_flushed(&final_batch);
        }
    }
}

#[derive(Debug, PartialEq)]
enum ConsumerCall {
    ParseErrors,
    Buffered {
        num_records: usize,
        num_bytes: usize,
    },
    Replayed {
        num_records: usize,
    },
    Extended {
        num_records: usize,
        metadata: JsonValue,
    },
    Error(bool),
    Eoi,
}

struct DummyParser(Arc<DummyInputReceiverInner>);

impl DummyParser {
    fn new(receiver: &DummyInputReceiver) -> Self {
        Self(receiver.inner.clone())
    }
}

impl Parser for DummyParser {
    fn parse(&mut self, data: &[u8]) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        (
            Some(Box::new(DummyInputBuffer {
                receiver: self.0.clone(),
                data: Some(String::from_utf8_lossy(data).into_owned()),
            })),
            Vec::new(),
        )
    }

    fn splitter(&self) -> Box<dyn Splitter> {
        Box::new(Sponge)
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self(self.0.clone()))
    }
}

struct DummyInputBuffer {
    receiver: Arc<DummyInputReceiverInner>,
    data: Option<String>,
}

impl InputBuffer for DummyInputBuffer {
    fn flush(&mut self) {
        if let Some(s) = self.data.take() {
            info!("flushing {:?}", s);
            self.receiver.flushed.lock().unwrap().push(s);
        }
    }

    fn len(&self) -> usize {
        self.data.as_ref().map_or(0, |_| 1)
    }

    fn hash(&self, _hasher: &mut dyn Hasher) {}

    fn take_some(&mut self, _n: usize) -> Option<Box<dyn InputBuffer>> {
        self.data.take().map(|data| {
            Box::new(Self {
                receiver: self.receiver.clone(),
                data: Some(data),
            }) as Box<dyn InputBuffer>
        })
    }
}

struct DummyInputReceiver {
    inner: Arc<DummyInputReceiverInner>,
    parker: Parker,
}

struct DummyInputReceiverInner {
    unparker: Unparker,
    n_buffered: AtomicUsize,
    drop_buffered: AtomicBool,
    calls: Mutex<Vec<ConsumerCall>>,
    flushed: Mutex<Vec<String>>,
}

impl DummyInputReceiver {
    pub fn new() -> Self {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        Self {
            inner: Arc::new(DummyInputReceiverInner {
                unparker,
                n_buffered: AtomicUsize::new(0),
                drop_buffered: AtomicBool::new(false),
                calls: Mutex::new(Vec::new()),
                flushed: Mutex::new(Vec::new()),
            }),
            parker,
        }
    }

    pub fn expect_n_buffers(&self, n: usize) {
        let start = Instant::now();
        loop {
            let received = self.inner.n_buffered.load(Ordering::Acquire);
            if received == n {
                return;
            }
            assert!(received < n);

            if start.elapsed() >= Duration::from_secs(10) {
                panic!("only buffered {received} out of {n} expected");
            }
            self.parker.park_timeout(Duration::from_millis(100));
        }
    }

    pub fn expect_buffering(&self, n: usize) {
        if n == 0 {
            return;
        }

        let mut received = 0;
        let start = Instant::now();
        loop {
            let mut current = self.inner.calls.lock().unwrap();
            for call in current.drain(..) {
                match call {
                    ConsumerCall::Buffered {
                        num_records,
                        num_bytes: _,
                    } => {
                        assert!(received + num_records <= n);
                        received += num_records;
                        if received == n {
                            return;
                        }
                    }
                    _ => panic!("expected ConsumerCall::Buffered, received {call:?}"),
                }
            }
            drop(current);

            if start.elapsed() >= Duration::from_secs(10) {
                panic!("only buffered {received} out of {n} expected");
            }
            self.parker.park_timeout(Duration::from_millis(100));
        }
    }

    pub fn expect_flushed(&self, batches: &Range<u32>) {
        let actual_flushed = mem::take(&mut *self.inner.flushed.lock().unwrap());
        let expect_flushed = batches
            .clone()
            .map(|i| format!("{i},false,,\n"))
            .collect::<Vec<_>>();
        assert_eq!(actual_flushed, expect_flushed);
    }

    /// Wait until the input consumer receives `expected`. Panics if it receives
    /// something else or if it doesn't receive it within a reasonable amount of
    /// time.  It is not an error for the consumer to receive more following
    /// `expected`; any data received afterward is left for later calls to
    /// check.
    #[track_caller]
    pub fn expect(&self, expected: Vec<ConsumerCall>) {
        let mut last_change = Instant::now();
        let mut last_len = 0;
        loop {
            let mut current = self.inner.calls.lock().unwrap();
            if current.len() >= expected.len()
                || Instant::now().duration_since(last_change) > Duration::from_secs(10)
            {
                let len = current.len().min(expected.len());
                let actual: Vec<_> = current.drain(0..len).collect();

                // Without this, sometimes we get SIGSEGV in librdkafka.
                drop(current);

                assert_eq!(expected, actual);
                return;
            }
            if current.len() != last_len {
                last_len = current.len();
                last_change = Instant::now();
            }
            drop(current);

            self.parker.park_timeout(Duration::from_millis(100));
        }
    }

    pub fn consumer(&self) -> Box<dyn InputConsumer> {
        Box::new(DummyInputConsumer(self.inner.clone()))
    }
}

#[derive(Clone)]
struct DummyInputConsumer(Arc<DummyInputReceiverInner>);

impl DummyInputConsumer {
    fn called(&self, call: ConsumerCall) {
        info!("{call:?}");
        self.0.calls.lock().unwrap().push(call);
        self.0.unparker.unpark();
    }
}

impl InputConsumer for DummyInputConsumer {
    fn max_batch_size(&self) -> usize {
        usize::MAX
    }

    fn is_pipeline_fault_tolerant(&self) -> bool {
        true
    }

    fn parse_errors(&self, errors: Vec<ParseError>) {
        if !errors.is_empty() {
            for error in errors {
                info!("parse error: {error}");
            }
            self.called(ConsumerCall::ParseErrors);
        }
    }

    fn buffered(&self, num_records: usize, num_bytes: usize) {
        let call = ConsumerCall::Buffered {
            num_records,
            num_bytes,
        };
        info!("{call:?}");
        if !self.0.drop_buffered.load(Ordering::Acquire) {
            self.0.calls.lock().unwrap().push(call);
            self.0.unparker.unpark();
        } else {
            self.0.n_buffered.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn replayed(&self, num_records: usize, _hash: u64) {
        self.called(ConsumerCall::Replayed { num_records });
    }

    fn extended(&self, num_records: usize, _hash: u64, metadata: JsonValue, _data: RmpValue) {
        self.called(ConsumerCall::Extended {
            num_records,
            metadata,
        });
    }

    fn error(&self, fatal: bool, error: AnyError) {
        info!("error: {error}");
        self.called(ConsumerCall::Error(fatal));
    }

    fn eoi(&self) {
        self.called(ConsumerCall::Eoi);
    }
}

#[test]
fn output_test() {
    kafka_output_test(
        "ft_kafka_end_to_end_csv_large",
        "csv",
        "",
        1000000,
        Vec::new(),
    );
}

fn kafka_output_test(
    test_name: &str,
    _format: &str,
    _format_config: &str,
    _message_max_bytes: usize,
    _data: Vec<Vec<TestStruct>>,
) {
    init_test_logger();
    let output_topic = format!("ft_{test_name}_output_topic");

    // Create topics.
    let _kafka_resources = KafkaResources::create_topics(&[(&output_topic, 1)]);

    let config_str = format!(
        r#"
name: kafka_output
config:
    topic: {output_topic}
"#
    );

    let mut endpoint =
        output_transport_config_to_endpoint(serde_yaml::from_str(&config_str).unwrap(), "", true)
            .unwrap()
            .unwrap();
    assert!(endpoint.is_fault_tolerant());
    endpoint
        .connect(Box::new(|fatal, error| info!("({fatal:?}, {error:?})")))
        .unwrap();
    for step in 0..5 {
        endpoint.batch_start(step).unwrap();
        endpoint
            .push_buffer(format!("string{step}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
}

fn _test() {
    let config_str = r#"
name: kafka_output
config:
    topic: my_topic
"#;

    let mut endpoint =
        output_transport_config_to_endpoint(serde_yaml::from_str(config_str).unwrap(), "", true)
            .unwrap()
            .unwrap();
    assert!(endpoint.is_fault_tolerant());
    endpoint
        .connect(Box::new(|fatal, error| info!("({fatal:?}, {error:?})")))
        .unwrap();
    for step in 0..5 {
        endpoint.batch_start(step).unwrap();
        endpoint
            .push_buffer(format!("string{step}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
}

/*
fn test_ft_kafka_input(data: Vec<Vec<TestStruct>>, topic1: &str, topic2: &str) {
    init_test_logger();

    let topic1 = &format!("ft_{topic1}");
    let index_topic1 = &format!("{topic1}_input-index");
    let topic2 = &format!("ft_{topic2}");
    let index_topic2 = &format!("{topic2}_input-index");
    let mut _kafka_resources = KafkaResources::create_topics(&[
        (topic1, 1),
        (index_topic1, 0),
        (topic2, 2),
        (index_topic2, 0),
    ]);

    info!("proptest_kafka_input: Test: Specify invalid Kafka broker address");

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: kafka_input
    config:
        bootstrap.servers: localhost:11111
        topics: ["{topic1}", "{topic2}"]
        log_level: debug
format:
    name: csv
"#
    );

    let (reader, consumer, parser, _input_handle) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    consumer.on_error(Some(Box::new(|_, _| {})));
    reader.start(0).unwrap();
    wait(|| consumer.state().endpoint_error.is_some(), 60000).unwrap();
    info!(
        "proptest_kafka_input: Error: {}",
        consumer.state().endpoint_error.as_ref().unwrap()
    );

    info!("proptest_kafka_input: Test: Specify invalid Kafka topic name");

    let config_str = r#"
stream: test_input
transport:
    name: kafka_input
    config:
        topics: ["this_topic_does_not_exist"]
        log_level: debug
format:
    name: csv
"#;

    let (reader, consumer, _input_handle) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(config_str).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    consumer.on_error(Some(Box::new(|_, _| {})));
    reader.start(0).unwrap();
    wait(|| consumer.state().endpoint_error.is_some(), 60000).unwrap();
    info!(
        "proptest_kafka_input: Error: {}",
        consumer.state().endpoint_error.as_ref().unwrap()
    );

    let config_str = format!(
        r#"
stream: test_input
transport:
    name: kafka_input
    config:
        topics: [{topic1}, {topic2}]
        log_level: debug
format:
    name: csv
"#
    );

    info!("proptest_kafka_input: Building input pipeline");

    let (endpoint, _consumer, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_yaml::from_str(&config_str).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    consumer.on_error(Some(Box::new(|fatal, error| {
        // It's normal for Kafka to emit errors, but not fatal ones.
        if fatal {
            panic!();
        } else {
            info!("{error}")
        }
    })));

    endpoint.start(0).unwrap();

    let producer = TestProducer::new();

    info!("proptest_kafka_input: Test: Receive from a topic with a single partition");

    // Send data to a topic with a single partition;
    // Make sure all records arrive in the original order.
    producer.send_to_topic(&data, topic1);

    wait_for_output_ordered(&zset, &data);
    zset.reset();

    info!("proptest_kafka_input: Test: Receive from a topic with multiple partitions");

    // Send data to a topic with multiple partitions.
    // Make sure all records are delivered, but not necessarily in the original
    // order.
    producer.send_to_topic(&data, topic2);

    wait_for_output_unordered(&zset, &data);
    zset.reset();

    info!("proptest_kafka_input: Test: pause/resume");
    //println!("records before pause: {}", zset.state().flushed.len());

    // Paused endpoint shouldn't receive any data.
    endpoint.pause().unwrap();
    sleep(Duration::from_millis(1000));

    producer.send_to_topic(&data, topic1);
    sleep(Duration::from_millis(1000));
    assert_eq!(zset.state().flushed.len(), 0);

    // Receive everything after unpause.
    endpoint.start(0).unwrap();
    wait_for_output_unordered(&zset, &data);

    zset.reset();

    info!("proptest_kafka_input: Test: Disconnect");
    // Disconnected endpoint should not receive any data.
    endpoint.disconnect();
    sleep(Duration::from_millis(1000));

    producer.send_to_topic(&data, topic1);
    sleep(Duration::from_millis(1000));
    assert_eq!(zset.state().flushed.len(), 0);
}

/// If Kafka tests are going to fail because the server is not running or
/// not functioning properly, it's good to fail quickly without printing a
/// thousand records as part of the failure.
#[test]
fn kafka_input_trivial() {
    test_ft_kafka_input(Vec::new(), "trivial_test_topic1", "trivial_test_topic2");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    #[test]
    fn proptest_kafka_input(data in generate_test_batches(0, 100, 1000)) {
        test_ft_kafka_input(data, "input_test_topic1", "input_test_topic2");
    }

    #[test]
    fn proptest_kafka_end_to_end_csv_large(data in generate_test_batches(0, 30, 1000)) {
        ft_kafka_end_to_end_test("proptest_kafka_end_to_end_csv_large", "csv", "", 1000000, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_csv_small(data in generate_test_batches(0, 30, 1000)) {
        ft_kafka_end_to_end_test("proptest_kafka_end_to_end_csv_small", "csv", "", 1500, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_json_small(data in generate_test_batches(0, 30, 1000)) {
        ft_kafka_end_to_end_test("proptest_kafka_end_to_end_json_small", "json", "", 2048, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_json_array_small(data in generate_test_batches(0, 30, 1000)) {
        ft_kafka_end_to_end_test("proptest_kafka_end_to_end_json_array_small", "json", "array: true", 5000, data);
    }
}
*/

#[derive(Clone)]
struct FtTestRound {
    n_records: usize,
    do_checkpoint: bool,
}

impl FtTestRound {
    fn with_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: true,
        }
    }
    fn without_checkpoint(n_records: usize) -> Self {
        Self {
            n_records,
            do_checkpoint: false,
        }
    }
}

/// Runs a basic test of fault tolerance.
///
/// The test proceeds in multiple rounds. For each element of `rounds`, the
/// test writes `n_records` records to the input file, and starts the
/// pipeline and waits for it to process the data.  If `do_checkpoint` is
/// true, it creates a new checkpoint. Then it stops the checkpoint, checks
/// that the output is as expected, and goes on to the next round.
fn test_ft(topic: &str, rounds: &[FtTestRound]) {
    init_test_logger();

    let mut _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    sleep(Duration::from_secs(1));
    let producer = TestProducer::new();
    let tempdir = TempDir::new().unwrap();

    // This allows the temporary directory to be deleted when we finish.  If
    // you want to keep it for inspection instead, comment out the following
    // line and then remove the comment markers on the two lines after that.
    let tempdir_path = tempdir.path();
    //let tempdir_path = tempdir.into_path();
    //println!("{}", tempdir_path.display());

    let storage_dir = tempdir_path.join("storage");
    create_dir(&storage_dir).unwrap();
    let output_path = tempdir_path.join("output.csv");

    let config_str = format!(
        r#"
name: test
workers: 4
storage_config:
    path: {storage_dir:?}
storage: true
fault_tolerance: {{}}
clock_resolution_usecs: null
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: kafka_input
            config:
                topic: {topic}
                start_from: earliest
                log_level: debug
        format:
            name: csv
outputs:
    test_output1:
        stream: test_output1
        transport:
            name: file_output
            config:
                path: {output_path:?}
        format:
            name: csv
            config:
        "#
    );

    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    // Number of records written to the input.
    let mut total_records = 0usize;

    // Number of input records included in the latest checkpoint (always <=
    // total_records).
    let mut checkpointed_records = 0usize;

    for (
        round,
        FtTestRound {
            n_records,
            do_checkpoint,
        },
    ) in rounds.iter().cloned().enumerate()
    {
        println!(
            "--- round {round}: add {n_records} records, {} --- ",
            if do_checkpoint {
                "and checkpoint"
            } else {
                "no checkpoint"
            }
        );

        // Write records to the input topic.
        println!(
            "Writing records {total_records}..{}",
            total_records + n_records
        );
        if n_records > 0 {
            let input_batch = (total_records..total_records + n_records)
                .map(|id| vec![TestStruct::for_id(id as u32)])
                .collect::<Vec<_>>();
            producer.send_to_topic(&input_batch, topic);

            total_records += n_records;
        }

        // Start pipeline.
        println!("start pipeline");
        let controller = Controller::with_config(
            |circuit_config| Ok(test_circuit::<TestStruct>(circuit_config, &[])),
            &config,
            Box::new(|e| panic!("error: {e}")),
        )
        .unwrap();
        controller.start();

        // Wait for the records that are not in the checkpoint to be
        // processed or replayed.
        let expect_n = total_records - checkpointed_records;
        println!(
            "wait for {} records {checkpointed_records}..{total_records}",
            expect_n
        );
        let mut last_n = 0;
        wait(
            || {
                let n = controller
                    .status()
                    .output_status()
                    .get(&0)
                    .unwrap()
                    .transmitted_records() as usize;
                if n > last_n {
                    println!("received {n} records of {expect_n}");
                    last_n = n;
                }
                n >= expect_n
            },
            10_000,
        )
        .unwrap();

        // No more records should arrive, but give the controller some time
        // to send some more in case there's a bug.
        sleep(Duration::from_millis(100));

        // Then verify that the number is as expected.
        assert_eq!(
            controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records(),
            expect_n as u64
        );

        // Checkpoint, if requested.
        if do_checkpoint {
            println!("checkpoint");
            controller.checkpoint().unwrap();
        }

        // Stop controller.
        println!("stop controller");
        controller.stop().unwrap();

        // Read output and compare. Our output adapter, which is not FT,
        // truncates the output file to length 0 each time. Therefore, the
        // output file should contain all the records in
        // `checkpointed_records..total_records`.
        let mut actual = CsvReaderBuilder::new()
            .has_headers(false)
            .from_path(&output_path)
            .unwrap()
            .deserialize::<(TestStruct, i32)>()
            .map(|res| {
                let (val, weight) = res.unwrap();
                assert_eq!(weight, 1);
                val
            })
            .collect::<Vec<_>>();
        actual.sort();

        assert_eq!(actual.len(), expect_n);
        for (record, expect_record) in actual
            .into_iter()
            .zip((checkpointed_records..).map(|id| TestStruct::for_id(id as u32)))
        {
            assert_eq!(record, expect_record);
        }

        if do_checkpoint {
            checkpointed_records = total_records;
        }
        println!();
    }
}

#[test]
fn ft_with_checkpoints() {
    test_ft(
        "ft_with_checkpoints",
        &[
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ],
    );
}

#[test]
fn ft_without_checkpoints() {
    test_ft(
        "ft_without_checkpoints",
        &[
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
        ],
    );
}

#[test]
fn ft_alternating() {
    test_ft(
        "ft_alternating",
        &[
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
        ],
    );
}

#[test]
fn ft_initially_zero_without_checkpoint() {
    test_ft(
        "ft_initially_zero_without_checkpoint",
        &[
            FtTestRound::without_checkpoint(0),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(0),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ],
    );
}

#[test]
fn ft_initially_zero_with_checkpoint() {
    test_ft(
        "ft_initially_zero_with_checkpoint",
        &[
            FtTestRound::with_checkpoint(0),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::without_checkpoint(0),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
            FtTestRound::without_checkpoint(2500),
            FtTestRound::with_checkpoint(2500),
        ],
    );
}
