use crate::{
    test::{
        generate_test_batches,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        mock_input_pipeline, test_circuit, wait, MockDeZSet, TestStruct, DEFAULT_TIMEOUT_MS,
    },
    transport::Step,
    Controller, InputConsumer, InputTransport, OutputTransport, ParseError, PipelineConfig,
};
use anyhow::Error as AnyError;
use crossbeam::sync::{Parker, Unparker};
use env_logger::Env;
use log::info;
use proptest::prelude::*;
use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::sleep,
    time::{Duration, Instant},
};
use uuid::Uuid;

/// Wait to receive all records in `data` in the same order.
fn wait_for_output_ordered(zset: &MockDeZSet<TestStruct>, data: &[Vec<TestStruct>]) {
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || zset.state().flushed.len() == num_records,
        DEFAULT_TIMEOUT_MS,
    );

    for (i, val) in data.iter().flat_map(|data| data.iter()).enumerate() {
        assert_eq!(&zset.state().flushed[i].0, val);
    }
}

/// Wait to receive all records in `data` in some order.
fn wait_for_output_unordered(zset: &MockDeZSet<TestStruct>, data: &[Vec<TestStruct>]) {
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || zset.state().flushed.len() == num_records,
        DEFAULT_TIMEOUT_MS,
    );

    let mut data_sorted = data
        .iter()
        .flat_map(|data| data.clone().into_iter())
        .collect::<Vec<_>>();
    data_sorted.sort();

    let mut zset_sorted = zset
        .state()
        .flushed
        .iter()
        .map(|(val, polarity)| {
            assert!(polarity);
            val.clone()
        })
        .collect::<Vec<_>>();
    zset_sorted.sort();

    assert_eq!(zset_sorted, data_sorted);
}

fn init_test_logger() {
    let _ = env_logger::Builder::from_env(Env::default().default_filter_or("debug"))
        .is_test(true)
        .format(move |buf, record| {
            let t = chrono::Utc::now();
            let t = format!("{}", t.format("%Y-%m-%d %H:%M:%S%.6f"));
            writeln!(
                buf,
                "{t} {} {}",
                buf.default_styled_level(record.level()),
                record.args()
            )
        })
        .try_init();
}

#[test]
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
            name: durable_kafka
            config:
                kafka_options:
                    bootstrap.servers: localhost:11111
                topic: durable_end_to_end_test_output_topic
                max_inflight_messages: 0
        format:
            name: csv
"#;

    info!("test_kafka_output_errors: Creating circuit");

    info!("test_kafka_output_errors: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(config_str).unwrap();

    match Controller::with_config(
        |workers| Ok(test_circuit(workers)),
        &config,
        Box::new(|e| panic!("error: {e}")),
    ) {
        Ok(_) => panic!("expected an error"),
        Err(e) => info!("test_kafka_output_errors: error: {e}"),
    }
}

fn durable_kafka_end_to_end_test(
    test_name: &str,
    format: &str,
    format_config: &str,
    message_max_bytes: usize,
    data: Vec<Vec<TestStruct>>,
) {
    init_test_logger();
    let uuid = Uuid::new_v4();
    let input_topic = format!("{test_name}_input_topic_{uuid}");
    let input_index_topic = format!("{input_topic}_input-index");
    let output_topic = format!("{test_name}_output_topic_{uuid}");

    // Create topics.
    let mut _kafka_resources = KafkaResources::create_topics(&[
        (&input_topic, 1),
        (&input_index_topic, 0),
        (&output_topic, 1),
    ]);

    // Create controller.

    let config_str = format!(
        r#"
name: test
workers: 4
inputs:
    test_input1:
        stream: test_input1
        transport:
            name: durable_kafka
            config:
                topics: ["{input_topic}"]
                log_level: debug
        format:
            name: csv
outputs:
    test_output2:
        stream: test_output1
        transport:
            name: durable_kafka
            config:
                topic: {output_topic}
                max_inflight_messages: 0
                message.max.bytes: "{message_max_bytes}"
        format:
            name: {format}
            config:
                {format_config}
"#
    );

    info!("{test_name}: Creating circuit. Config {config_str}");

    info!("{test_name}: Starting controller");
    let config: PipelineConfig = serde_yaml::from_str(&config_str).unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let test_name_clone = test_name.to_string();

    let controller = Controller::with_config(
        |workers| Ok(test_circuit(workers)),
        &config,
        Box::new(move |e| if running_clone.load(Ordering::Acquire) {
            panic!("{test_name_clone}: error: {e}")
        } else {
            info!("{test_name_clone}: error during shutdown (likely caused by Kafka topics being deleted): {e}")
        }),
    )
        .unwrap();
    assert!(controller.is_fault_tolerant());

    let buffer_consumer = BufferConsumer::new(&output_topic, format, format_config);

    info!("{test_name}: Sending inputs");
    let producer = TestProducer::new();
    producer.send_to_topic(&data, &input_topic);

    info!("{test_name}: Starting controller");
    // Start controller.
    controller.start();

    // Wait for output buffer to contain all of `data`.

    info!("{test_name}: Waiting for output");
    buffer_consumer.wait_for_output_unordered(&data);

    drop(buffer_consumer);

    controller.stop().unwrap();

    // Endpoint threads might still be running (`controller.stop()` doesn't wait
    // for them to terminate).  Once `KafkaResources` is dropped, these threads
    // may start throwing errors due to deleted Kafka topics.  Make sure these
    // errors don't cause panics.
    running.store(false, Ordering::Release);
}

/// Test a topic that's empty and won't get any data.
#[test]
fn test_empty_input() {
    init_test_logger();

    let mut _kafka_resources =
        KafkaResources::create_topics(&[("empty", 1), ("empty_input-index", 0)]);

    let transport = <dyn InputTransport>::get_transport("durable_kafka").unwrap();

    let config_str = r#"
topics: [empty]
log_level: debug
"#
    .to_string();

    let endpoint = transport
        .new_endpoint(&serde_yaml::from_str(&config_str).unwrap())
        .unwrap();
    assert!(endpoint.is_durable());

    info!("checking initial steps");
    assert_eq!(endpoint.steps().unwrap(), 0..0);

    // Initially there are no steps.  Reading step 0 should block because
    // nothing is writing data.
    info!("trying to read read step 0 (should time out)");
    let receiver = DummyInputReceiver::new();
    let reader = endpoint.open(receiver.consumer(), 0).unwrap();
    reader.start(Step::MAX).unwrap();
    receiver.expect(vec![ConsumerCall::StartStep(0)]);

    // Five times, try to commit
    for step in 0..=4 {
        info!("committing and reading step {step}",);
        reader.commit(step);
        receiver.wait_to_settle(step);
        receiver.expect(vec![ConsumerCall::StartStep(step + 1)]);
        receiver.wait_to_settle(step);
        assert_eq!(endpoint.steps().unwrap(), 0..(step + 1));
    }

    // Try to read multiple steps beyond the last available.
    let mut step = 4;
    for n in 2..10 {
        let commits = (step + 1)..=(step + n);
        step = *commits.end();
        info!("committing up to step {step}");
        reader.commit(step);
        for step in commits {
            receiver.expect(vec![ConsumerCall::StartStep(step + 1)])
        }
        receiver.wait_to_settle(step);
        assert_eq!(endpoint.steps().unwrap(), 0..(step + 1));
    }
    receiver.expect_eof();

    // Now open the same endpoint again and all the steps should be immediately
    // available.
    let receiver2 = DummyInputReceiver::new();
    let reader2 = endpoint.open(receiver2.consumer(), 0).unwrap();
    reader2.start(step + 1).unwrap();
    for step in 0..=step {
        receiver2.expect(vec![ConsumerCall::StartStep(step)])
    }
    receiver2.wait_to_settle(step);
    receiver2.expect(vec![ConsumerCall::StartStep(step + 1)]);
    receiver2.expect_eof();
}

#[test]
fn test_input() {
    init_test_logger();

    let topic = "durability";
    let index_topic = &format!("{topic}_input-index");
    let mut _kafka_resources = KafkaResources::create_topics(&[(topic, 1), (index_topic, 0)]);

    let config_str = format!(
        r#"
topics: [{topic}]
log_level: debug
max_step_messages: 5
"#
    );

    let transport = <dyn InputTransport>::get_transport("durable_kafka").unwrap();

    let endpoint = transport
        .new_endpoint(&serde_yaml::from_str(&config_str).unwrap())
        .unwrap();
    assert!(endpoint.is_durable());

    info!("checking initial steps");
    assert_eq!(endpoint.steps().unwrap(), 0..0);

    info!("trying to read read step 0 (should time out)");
    let receiver = DummyInputReceiver::new();
    let reader = endpoint.open(receiver.consumer(), 0).unwrap();
    reader.start(0).unwrap();
    receiver.expect(vec![ConsumerCall::StartStep(0)]);

    fn test(id: u32) -> TestStruct {
        TestStruct {
            id,
            b: false,
            i: None,
            s: "".into(),
        }
    }

    info!("now write some data");
    let producer = TestProducer::new();
    producer.send_to_topic(&[vec![test(0)]], topic);

    info!("now we should get that data in step 0");
    receiver.expect(vec![ConsumerCall::InputChunk("0,false,,\n".into())]);

    info!("commit step 0");
    reader.commit(0);
    receiver.expect(vec![ConsumerCall::StartStep(1)]);
    receiver.wait_to_settle(0);
    assert_eq!(endpoint.steps().unwrap(), 0..1);

    info!("we shouldn't get more data yet because we didn't ask for step 1 yet");
    let producer = TestProducer::new();
    producer.send_to_topic(&[vec![test(1), test(2)]], topic);
    receiver.expect_eof();

    info!("ask for more steps and we should get that data");
    reader.start(10).unwrap();
    receiver.expect(vec![ConsumerCall::InputChunk(
        "1,false,,\n2,false,,\n".into(),
    )]);

    info!("commit step 1");
    reader.commit(1);
    receiver.expect(vec![ConsumerCall::StartStep(2)]);
    receiver.wait_to_settle(1);
    assert_eq!(endpoint.steps().unwrap(), 0..2);

    info!("writing 4 messages (with max_step_messages=5) should not force a step");
    for i in 3..=6 {
        producer.send_to_topic(&[vec![test(i)]], topic);
        receiver.expect(vec![ConsumerCall::InputChunk(format!("{i},false,,\n"))]);
    }
    receiver.expect_eof();

    info!("writing a fifth message should force a step");
    producer.send_to_topic(&[vec![test(7)]], topic);
    receiver.expect(vec![ConsumerCall::InputChunk("7,false,,\n".into())]);
    receiver.expect(vec![ConsumerCall::StartStep(3)]);
    receiver.wait_to_settle(2);
    assert_eq!(endpoint.steps().unwrap(), 0..3);

    receiver.expect_eof();
}

#[derive(Debug, Eq, PartialEq)]
enum ConsumerCall {
    StartStep(Step),
    InputFragment(String),
    InputChunk(String),
    Error(bool),
    Eoi,
}

struct DummyInputReceiver {
    inner: Arc<DummyInputReceiverInner>,
    parker: Parker,
}

struct DummyInputReceiverInner {
    unparker: Unparker,
    calls: Mutex<Vec<ConsumerCall>>,
    settled: Mutex<Option<Step>>,
}

impl DummyInputReceiver {
    pub fn new() -> Self {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        Self {
            inner: Arc::new(DummyInputReceiverInner {
                unparker,
                calls: Mutex::new(Vec::new()),
                settled: Mutex::new(None),
            }),
            parker,
        }
    }

    /// Wait some time for the input consumer to report that `settled` was
    /// called.  However, we don't expect it to have been called, so we panic
    /// with an error if it has.
    ///
    /// The waiting time here is arbitrary, since we expect that we could wait
    /// forever.
    #[track_caller]
    pub fn expect_eof(&self) {
        sleep(Duration::from_millis(100));

        let actual: Vec<_> = self.inner.calls.lock().unwrap().drain(..).collect();
        assert_eq!(Vec::<ConsumerCall>::new(), actual);
    }

    /// Wait until the input consumer receives `expected`. Panics if it receives
    /// something else or if it doesn't receive it within a reasonable amount of
    /// time.  It is not an error for the consumer receives more following
    /// `expected`; any calls afterward is left for later calls to check.
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

    pub fn wait_to_settle(&self, step: Step) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(Instant::now() < deadline);
            if let Some(settled) = *self.inner.settled.lock().unwrap() {
                if settled >= step {
                    return;
                }
            }
            self.parker.park_deadline(deadline);
        }
    }

    pub fn consumer(&self) -> Box<dyn InputConsumer> {
        Box::new(DummyInputConsumer(self.inner.clone()))
    }
}

struct DummyInputConsumer(Arc<DummyInputReceiverInner>);

impl DummyInputConsumer {
    fn called(&mut self, call: ConsumerCall) {
        info!("{call:?}");
        self.0.calls.lock().unwrap().push(call);
        self.0.unparker.unpark();
    }
}

impl InputConsumer for DummyInputConsumer {
    fn start_step(&mut self, step: Step) {
        self.called(ConsumerCall::StartStep(step));
    }
    fn input_fragment(&mut self, data: &[u8]) -> Vec<ParseError> {
        self.called(ConsumerCall::InputFragment(
            String::from_utf8(data.into()).unwrap(),
        ));
        vec![]
    }
    fn input_chunk(&mut self, data: &[u8]) -> Vec<ParseError> {
        self.called(ConsumerCall::InputChunk(
            String::from_utf8(data.into()).unwrap(),
        ));
        vec![]
    }
    fn error(&mut self, fatal: bool, error: AnyError) {
        info!("error: {error}");
        self.called(ConsumerCall::Error(fatal));
    }
    fn eoi(&mut self) -> Vec<ParseError> {
        self.called(ConsumerCall::Eoi);
        vec![]
    }
    fn fork(&self) -> Box<dyn InputConsumer> {
        unreachable!()
    }
    fn settled(&mut self, step: Step) {
        info!("step {step} settled");
        let mut settled = self.0.settled.lock().unwrap();
        if let Some(settled) = *settled {
            assert_eq!(settled + 1, step);
        }
        *settled = Some(step);
        self.0.unparker.unpark();
    }
}

#[test]
fn output_test() {
    kafka_output_test(
        "durable_kafka_end_to_end_csv_large",
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
    let output_topic = format!("durable_{test_name}_output_topic");

    // Create topics.
    let _kafka_resources = KafkaResources::create_topics(&[(&output_topic, 1)]);

    let transport = <dyn OutputTransport>::get_transport("durable_kafka").unwrap();

    let config_str = format!(
        r#"
stream: test_output1
transport:
    name: durable_kafka
    config:
        topic: {output_topic}
format:
    name: csv
"#
    );

    let mut endpoint = transport
        .new_endpoint(&serde_yaml::from_str(&config_str).unwrap())
        .unwrap();
    assert!(endpoint.is_durable());
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
    let transport = <dyn OutputTransport>::get_transport("durable_kafka").unwrap();

    let config_str = r#"
stream: my_test_stream
transport:
    name: durable_kafka
    config:
        topic: my_topic
format:
    name: csv
"#;

    let mut endpoint = transport
        .new_endpoint(&serde_yaml::from_str(&config_str).unwrap())
        .unwrap();
    assert!(endpoint.is_durable());
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

fn test_durable_kafka_input(data: Vec<Vec<TestStruct>>, topic1: &str, topic2: &str) {
    init_test_logger();

    let topic1 = &format!("durable_{topic1}");
    let index_topic1 = &format!("{topic1}_input-index");
    let topic2 = &format!("durable_{topic2}");
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
    name: durable_kafka
    config:
        kafka_options:
            bootstrap.servers: localhost:11111
        topics: ["{topic1}", "{topic2}"]
        log_level: debug
format:
    name: csv
"#
    );

    let (reader, consumer, _input_handle) =
        mock_input_pipeline::<TestStruct>(serde_yaml::from_str(&config_str).unwrap()).unwrap();
    consumer.on_error(Some(Box::new(|_| {})));
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
    name: durable_kafka
    config:
        topics: ["this_topic_does_not_exist"]
        log_level: debug
format:
    name: csv
"#;

    let (reader, consumer, _input_handle) =
        mock_input_pipeline::<TestStruct>(serde_yaml::from_str(config_str).unwrap()).unwrap();
    consumer.on_error(Some(Box::new(|_| {})));
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
    name: durable_kafka
    config:
        topics: [{topic1}, {topic2}]
        log_level: debug
format:
    name: csv
"#
    );

    info!("proptest_kafka_input: Building input pipeline");

    let (endpoint, _consumer, zset) =
        mock_input_pipeline::<TestStruct>(serde_yaml::from_str(&config_str).unwrap()).unwrap();

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
    test_durable_kafka_input(Vec::new(), "trivial_test_topic1", "trivial_test_topic2");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2))]

    #[test]
    fn proptest_kafka_input(data in generate_test_batches(0, 100, 1000)) {
        test_durable_kafka_input(data, "input_test_topic1", "input_test_topic2");
    }

    #[test]
    fn proptest_kafka_end_to_end_csv_large(data in generate_test_batches(0, 30, 1000)) {
        durable_kafka_end_to_end_test("proptest_kafka_end_to_end_csv_large", "csv", "", 1000000, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_csv_small(data in generate_test_batches(0, 30, 1000)) {
        durable_kafka_end_to_end_test("proptest_kafka_end_to_end_csv_small", "csv", "", 1500, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_json_small(data in generate_test_batches(0, 30, 1000)) {
        durable_kafka_end_to_end_test("proptest_kafka_end_to_end_json_small", "json", "", 2048, data);
    }

    #[test]
    fn proptest_kafka_end_to_end_json_array_small(data in generate_test_batches(0, 30, 1000)) {
        durable_kafka_end_to_end_test("proptest_kafka_end_to_end_json_array_small", "json", "array: true", 5000, data);
    }
}
