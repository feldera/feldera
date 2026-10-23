//! Tests of the fault-tolerant Kafka transport that reach no engine: the readers, with
//! dummy parsers and consumers, and the writers, against the broker; unit tests of
//! partition selection; and typed checks of the Kafka metadata on the static engine's record
//! layer. The connector tests in `transport::kafka::tests` cover the transport with an
//! engine.

// A reader's offsets are ranges, so one range is one partition's offsets, not a mistake.
#![allow(clippy::single_range_in_vec_init)]

use crate::format::{Splitter, SpongeSplitter};
use crate::test::wait;
use crate::transport::kafka::ft::input::{BACKPRESSURE, Metadata};
use crate::transport::{input_transport_config_to_endpoint, output_transport_config_to_endpoint};
use crate::{InputBuffer, InputReader, Parser, TransportInputEndpoint};
use crate::{
    InputConsumer, ParseError,
    test::{
        KafkaRawMetadata, TestStruct,
        data::TestStructMetadata,
        kafka::{KafkaResources, TestProducer, csv_messages},
        mock_input_pipeline, suites, wait_for_output_count,
    },
};
use anyhow::Error as AnyError;
use crossbeam::sync::{Parker, Unparker};
use csv::WriterBuilder as CsvWriterBuilder;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::catalog::StagedBuffers;
use feldera_adapterlib::format::{BufferSize, flatten_nested};
use feldera_adapterlib::transport::{OutputBatchType, Resume, Watermark};
use feldera_sqllib::{ByteArray, SqlString, Variant};
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::config::{FtModel, InputEndpointConfig};
use feldera_types::program_schema::Relation;
use feldera_types::secret_resolver::default_secrets_directory;
use feldera_types::transport::kafka::{
    KafkaInputConfig, KafkaStartFromConfig, default_redpanda_server,
};
use proptest::prelude::*;
use rand::thread_rng;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Headers;
use rdkafka::producer::BaseRecord;
use rdkafka::{Message, Timestamp};
use rmpv::Value as RmpValue;
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::iter::repeat_with;
use std::ops::Range;
use std::sync::atomic::AtomicUsize;
use std::thread::sleep;
use std::{
    mem,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

fn create_reader(
    topic: &str,
    resume_info: Option<JsonValue>,
    synchronize_partitions: bool,
) -> (
    Box<dyn TransportInputEndpoint>,
    DummyInputReceiver,
    Box<dyn InputReader>,
) {
    create_reader_config(topic, resume_info, synchronize_partitions, None)
}

/// Like [`create_reader`], but with an optional `header_filter` added to the
/// connector configuration.
fn create_reader_config(
    topic: &str,
    resume_info: Option<JsonValue>,
    synchronize_partitions: bool,
    header_filter: Option<JsonValue>,
) -> (
    Box<dyn TransportInputEndpoint>,
    DummyInputReceiver,
    Box<dyn InputReader>,
) {
    let mut inner_config = json!({
        "topic": topic,
        "log_level": "debug",
        "start_from": "earliest",
        "synchronize_partitions": synchronize_partitions,
    });
    if let Some(header_filter) = header_filter {
        inner_config
            .as_object_mut()
            .unwrap()
            .insert("header_filter".to_string(), header_filter);
    }

    let config = serde_json::from_value(json!({
      "name": "kafka_input",
      "config": inner_config,
    }))
    .unwrap();

    let endpoint =
        input_transport_config_to_endpoint(&config, "", default_secrets_directory(), None)
            .unwrap()
            .unwrap();
    assert!(endpoint.fault_tolerance() == Some(FtModel::ExactlyOnce));

    let receiver = DummyInputReceiver::new();
    let reader = endpoint
        .open(
            receiver.consumer(),
            Box::new(DummyParser::new(&receiver)),
            Relation::empty(),
            resume_info,
        )
        .unwrap();

    (endpoint, receiver, reader)
}

/// Drain and return the payloads flushed to `receiver` so far, in order.
fn take_flushed(receiver: &DummyInputReceiver) -> Vec<String> {
    mem::take(&mut *receiver.inner.flushed.lock().unwrap())
}

/// Build the `headers` argument for [`TestProducer::send_message`] from
/// `(key, value)` pairs, where a `None` value denotes a header with a null
/// value.
fn headers(pairs: &[(&str, Option<&[u8]>)]) -> Option<BTreeMap<String, Option<Vec<u8>>>> {
    Some(
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.map(|v| v.to_vec())))
            .collect(),
    )
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

#[test]
fn test_synchronization() {
    init_test_logger();

    const TOPIC: &str = "synchronization";
    const N_PARTITIONS: usize = 32;
    const N_MESSAGES: usize = 4096;
    let mut _kafka_resources = KafkaResources::create_topics(&[(TOPIC, N_PARTITIONS as i32)]);

    let (_endpoint, receiver, reader) = create_reader(TOPIC, None, true);
    reader.extend();

    // This is the number of records we expect to read when we produce
    // `N_MESSAGES` records, since we can read all the records except that we
    // end up blocked when one of the partitions is exhausted.
    let n = N_MESSAGES - N_PARTITIONS + 1;

    // Produce N_MESSAGES messages into random partitions[*], with timestamps
    // that match their record IDs.
    //
    // [*] The last N_PARTITIONS of the messages go to each of the partitions in
    // order so that we have a sentinel at the end of each partition.  If we
    // distributed all of them randomly then we'd end up reading an
    // unpredictable number of messages from the consumer because (probably)
    // we'd consume all the messages from one of the partitions and then we
    // wouldn't be able to read any of the remaining ones.
    let producer = TestProducer::new();
    let now = Timestamp::now().to_millis().unwrap();
    let mut rng = thread_rng();
    let mut expect_lens = vec![0; N_PARTITIONS];
    for (id, partition) in repeat_with(|| rng.gen_range(0..N_PARTITIONS))
        .take(N_MESSAGES - N_PARTITIONS)
        .chain(0..N_PARTITIONS)
        .enumerate()
    {
        if id < n {
            expect_lens[partition] += 1;
        }

        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        writer.serialize(TestStruct::for_id(id as u32)).unwrap();
        writer.flush().unwrap();
        let bytes = writer.into_inner().unwrap();

        let record = <BaseRecord<(), [u8], ()>>::to(TOPIC)
            .payload(&bytes)
            .timestamp(now + id as i64)
            .partition(partition as i32);
        producer.producer.send(record).unwrap();
    }

    println!("waiting for connector to buffer {n} messages.");
    receiver.expect_buffering(n);

    // Tell the adapter to queue the batch and wait for it to do it, then check
    // that the connector successfully merged all the records on the basis of
    // timestamp.
    println!("queuing and expecting {n} records",);
    reader.queue(false);
    let metadata = Metadata {
        offsets: expect_lens.into_iter().map(|len| 0..len).collect(),
        shard: None,
        partitions: None,
    };
    receiver.expect(vec![ConsumerCall::Extended {
        num_records: n,
        metadata: serde_json::to_value(&metadata).unwrap(),
    }]);
}

#[test]
fn test_synchronization_backpressure() {
    fn produce_record(
        producer: &TestProducer,
        topic: &str,
        partition: i32,
        id: u32,
        timestamp: i64,
    ) {
        let mut writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        writer.serialize(TestStruct::for_id(id)).unwrap();
        writer.flush().unwrap();
        let bytes = writer.into_inner().unwrap();

        let record = <BaseRecord<(), [u8], ()>>::to(topic)
            .payload(&bytes)
            .timestamp(timestamp)
            .partition(partition);
        producer.producer.send(record).unwrap();
    }

    init_test_logger();

    const TOPIC: &str = "synchronization_backpressure";
    const N_PARTITIONS: usize = 2;
    let mut _kafka_resources = KafkaResources::create_topics(&[(TOPIC, N_PARTITIONS as i32)]);

    let (_endpoint, receiver, reader) = create_reader(TOPIC, None, true);
    reader.extend();

    // Produce 2500 messages into partition 0, with timestamps that match their
    // record IDs, and 1 message into partition 1 with record ID 5000 and timestamp 2000.
    let producer = TestProducer::new();
    let now = Timestamp::now().to_millis().unwrap();
    for id in 0..2500 {
        produce_record(&producer, TOPIC, 0, id, now + id as i64);
    }
    produce_record(&producer, TOPIC, 1, 5000, now + 2000);

    // 2002 messages should get buffered (partition 0 ids 0..=2000, partition 1
    // record id 5000).
    let expect = 2002;
    println!("waiting for connector to buffer {expect} messages.");
    receiver.expect_buffering(expect);

    // Tell the adapter to queue the batch and wait for it to do it, then check
    // that the connector successfully merged all the records on the basis of
    // timestamp.
    println!("queuing and expecting {expect} records",);
    reader.queue(false);
    let metadata = Metadata {
        offsets: [2001, 1].into_iter().map(|len| 0..len).collect(),
        shard: None,
        partitions: None,
    };
    receiver.expect(vec![ConsumerCall::Extended {
        num_records: expect,
        metadata: serde_json::to_value(&metadata).unwrap(),
    }]);

    // There are about 500 more records to read in partition 0, but none of them
    // can actually be buffered because they are waiting for something to show
    // up in partition 1.  However, they should not get stuck in backpressure
    // because there's a 1000-record minimum for that.
    //
    // It's hard to wait for something not to happen, so just give it a few
    // seconds.
    sleep(Duration::from_secs(5));
    let partition0 = (String::from(TOPIC), 0);
    assert!(!BACKPRESSURE.lock().unwrap().contains(&partition0));

    // Produce another 1000 messages into partition 0, with timestamps that
    // match their record IDs.
    for id in 2500..3500 {
        produce_record(&producer, TOPIC, 0, id, now + id as i64);
    }

    // Now there are 1500 records to read in partition 0 but they're stuck
    // because we still have nothing in partition 1.  This should exert backpressure.
    wait(|| BACKPRESSURE.lock().unwrap().contains(&partition0), 10000).unwrap();

    // Produce one record into partition 1 that is beyond all the records in
    // partition 0.
    produce_record(&producer, TOPIC, 1, 5000, now + 5000);

    // 1499 messages should get buffered (partition 0 ids 2001..3500).
    let expect = 1499;
    println!("waiting for connector to buffer {expect} messages.");
    receiver.expect_buffering(expect);

    // Tell the adapter to queue the batch and wait for it to do it, then check
    // that the connector successfully read all the records from partition 0.
    println!("queuing and expecting {expect} records",);
    reader.queue(false);
    let metadata = Metadata {
        offsets: [2001..3500, 1..1].into_iter().collect(),
        shard: None,
        partitions: None,
    };
    receiver.expect(vec![ConsumerCall::Extended {
        num_records: expect,
        metadata: serde_json::to_value(&metadata).unwrap(),
    }]);

    // Backpressure was fixed.
    assert!(!BACKPRESSURE.lock().unwrap().contains(&partition0));
}

fn test_input(topic: &str, batch_sizes: &[u32]) {
    init_test_logger();

    let mut _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);

    let (_endpoint, receiver, reader) = create_reader(topic, None, false);
    reader.extend();

    fn metadata(batch: &Range<u32>) -> JsonValue {
        #[allow(clippy::single_range_in_vec_init)]
        let metadata = Metadata {
            offsets: vec![batch.start as i64..batch.end as i64],
            shard: None,
            partitions: None,
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
        producer.send_to_topic(&csv_messages(&input_batch), topic);

        println!("waiting for connector to buffer the {batch_size} messages {batch:?}.");
        receiver.expect_buffering(batch.len());

        // Tell the adapter to queue the batch and wait for it to do it.
        println!("queuing and expecting {batch_size} records");
        reader.queue(false);
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
            let resume_info = if seek > 0 {
                println!("- seek to {seek}");
                Some(metadata(&batches[seek - 1]))
            } else {
                None
            };
            let (_endpoint, receiver, reader) = create_reader(topic, resume_info, false);
            receiver.inner.drop_buffered.store(true, Ordering::Release);

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
            reader.queue(false);
            receiver.expect(vec![ConsumerCall::Extended {
                num_records: final_batch.len(),
                metadata: metadata(&final_batch),
            }]);
            receiver.expect_flushed(&final_batch);
        }
    }
}

#[test]
fn test_input_tombstone() {
    init_test_logger();

    let topic = "kafka_tombstone_ft";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);

    let (endpoint, receiver, reader) = create_reader(topic, None, false);
    reader.extend();

    let producer = TestProducer::new();
    producer.send_message(b"m0", topic, None);
    producer.send_tombstone(b"deleted-key", topic);
    producer.send_message(b"", topic, None);
    producer.send_message(b"m3", topic, None);

    receiver.expect_buffering(3);
    reader.queue(false);

    let metadata = Metadata {
        offsets: vec![0..4],
        shard: None,
        partitions: None,
    };
    receiver.expect(vec![ConsumerCall::Extended {
        num_records: 3,
        metadata: serde_json::to_value(&metadata).unwrap(),
    }]);
    assert_eq!(take_flushed(&receiver), vec!["m0", "", "m3"]);

    drop(endpoint);
    drop(receiver);
    drop(reader);

    let (_endpoint, receiver, reader) = create_reader(topic, None, false);
    receiver.inner.drop_buffered.store(true, Ordering::Release);
    reader.replay(serde_json::to_value(&metadata).unwrap(), RmpValue::Nil);
    receiver.expect(vec![ConsumerCall::Replayed { num_records: 3 }]);
    assert_eq!(take_flushed(&receiver), vec!["m0", "", "m3"]);
}

/// A configured header filter drops non-matching messages before parsing, and
/// the drop decision is deterministic across replay: a fresh reader replaying
/// the checkpointed offset range reproduces exactly the admitted records.
///
/// To confirm this test catches a regression, make `header_filter_admits`
/// always return `true`; the connector then buffers all 6 messages instead of
/// 3 and both the live and the replay expectations fail.
#[test]
fn test_input_header_filter() {
    init_test_logger();

    let topic = "kafka_header_filter_ft";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);

    // Admit only messages whose `keep` header equals `yes`.
    let filter = json!({"header": {"name": "keep", "pattern": "yes"}});

    let (endpoint, receiver, reader) =
        create_reader_config(topic, None, false, Some(filter.clone()));
    reader.extend();

    // Offsets 0..6.  `keep=yes` at 0, 2, 4 are admitted; 1, 3 (`keep=no`) and 5
    // (no header) are dropped.  The trailing drop at offset 5 lies beyond the
    // recorded range and must never be replayed.
    let producer = TestProducer::new();
    producer.send_message(b"m0", topic, headers(&[("keep", Some(b"yes"))]));
    producer.send_message(b"m1", topic, headers(&[("keep", Some(b"no"))]));
    producer.send_message(b"m2", topic, headers(&[("keep", Some(b"yes"))]));
    producer.send_message(b"m3", topic, headers(&[("keep", Some(b"no"))]));
    producer.send_message(b"m4", topic, headers(&[("keep", Some(b"yes"))]));
    producer.send_message(b"m5", topic, None);

    // Only the 3 admitted messages are buffered.
    receiver.expect_buffering(3);
    reader.queue(false);

    // The offset range spans the first through the last admitted offset (0..5);
    // dropped offsets inside and beyond the range do not change it.
    let metadata = Metadata {
        offsets: vec![0..5],
        shard: None,
        partitions: None,
    };
    receiver.expect(vec![ConsumerCall::Extended {
        num_records: 3,
        metadata: serde_json::to_value(&metadata).unwrap(),
    }]);
    assert_eq!(take_flushed(&receiver), vec!["m0", "m2", "m4"]);

    drop(endpoint);
    drop(receiver);
    drop(reader);

    // Replay the recorded range with the same filter.  The filter is re-applied
    // during replay, so exactly the admitted records reappear, in order.
    let (_endpoint, receiver, reader) = create_reader_config(topic, None, false, Some(filter));
    receiver.inner.drop_buffered.store(true, Ordering::Release);
    reader.replay(serde_json::to_value(&metadata).unwrap(), RmpValue::Nil);
    receiver.expect(vec![ConsumerCall::Replayed { num_records: 3 }]);
    assert_eq!(take_flushed(&receiver), vec!["m0", "m2", "m4"]);
}

/// A boolean header filter (`and`/`or`/`not`) drops the right messages in the
/// real connector, exercising leading, middle, and trailing drops, and its
/// decisions are re-applied identically on replay.  The filter reads headers
/// directly, so it works even though `include_headers` is not set.
#[test]
fn test_input_header_filter_boolean() {
    init_test_logger();

    let topic = "kafka_header_filter_boolean_ft";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);

    // Admit iff (env is prod or staging) and not (skip == drop).
    let filter = json!({
        "and": [
            {"or": [
                {"header": {"name": "env", "pattern": "prod"}},
                {"header": {"name": "env", "pattern": "staging"}}
            ]},
            {"not": {"header": {"name": "skip", "pattern": "drop"}}}
        ]
    });

    let (endpoint, receiver, reader) =
        create_reader_config(topic, None, false, Some(filter.clone()));
    reader.extend();

    let producer = TestProducer::new();
    // 0: env=dev               -> drop (fails `or`); leading drop
    // 1: env=prod              -> admit
    // 2: env=staging, skip=drop -> drop (fails `not`); middle drop
    // 3: env=staging           -> admit
    // 4: env=prod, skip=drop   -> drop (fails `not`); trailing drop
    producer.send_message(b"m0", topic, headers(&[("env", Some(b"dev"))]));
    producer.send_message(b"m1", topic, headers(&[("env", Some(b"prod"))]));
    producer.send_message(
        b"m2",
        topic,
        headers(&[("env", Some(b"staging")), ("skip", Some(b"drop"))]),
    );
    producer.send_message(b"m3", topic, headers(&[("env", Some(b"staging"))]));
    producer.send_message(
        b"m4",
        topic,
        headers(&[("env", Some(b"prod")), ("skip", Some(b"drop"))]),
    );

    // Admitted: offsets 1 and 3.
    receiver.expect_buffering(2);
    reader.queue(false);

    // The range starts at the first admitted offset (1) and ends after the last
    // (3), skipping the leading drop at offset 0.
    let metadata = Metadata {
        offsets: vec![1..4],
        shard: None,
        partitions: None,
    };
    receiver.expect(vec![ConsumerCall::Extended {
        num_records: 2,
        metadata: serde_json::to_value(&metadata).unwrap(),
    }]);
    assert_eq!(take_flushed(&receiver), vec!["m1", "m3"]);

    drop(endpoint);
    drop(receiver);
    drop(reader);

    // Replaying the recorded range reproduces the admitted records.
    let (_endpoint, receiver, reader) = create_reader_config(topic, None, false, Some(filter));
    receiver.inner.drop_buffered.store(true, Ordering::Release);
    reader.replay(serde_json::to_value(&metadata).unwrap(), RmpValue::Nil);
    receiver.expect(vec![ConsumerCall::Replayed { num_records: 2 }]);
    assert_eq!(take_flushed(&receiver), vec!["m1", "m3"]);
}

#[derive(Debug, PartialEq)]
enum ConsumerCall {
    ParseErrors,
    Buffered(BufferSize),
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

struct DummyStagedBuffers {
    data: Vec<String>,
    handle: Arc<DummyInputReceiverInner>,
}

impl StagedBuffers for DummyStagedBuffers {
    fn flush(&mut self) -> anyhow::Result<()> {
        info!("flushing {} staged buffers", self.data.len());
        self.handle.flushed.lock().unwrap().append(&mut self.data);
        Ok(())
    }
}

struct DummyParser(Arc<DummyInputReceiverInner>);

impl DummyParser {
    fn new(receiver: &DummyInputReceiver) -> Self {
        Self(receiver.inner.clone())
    }
}

impl Parser for DummyParser {
    fn parse(
        &mut self,
        data: &[u8],
        _metadata: Option<ConnectorMetadata>,
    ) -> (Option<Box<dyn InputBuffer>>, Vec<ParseError>) {
        (
            Some(Box::new(DummyInputBuffer {
                receiver: self.0.clone(),
                data: Some(String::from_utf8_lossy(data).into_owned()),
            })),
            Vec::new(),
        )
    }

    fn splitter(&self) -> Box<dyn Splitter> {
        Box::new(SpongeSplitter)
    }

    fn fork(&self) -> Box<dyn Parser> {
        Box::new(Self(self.0.clone()))
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(DummyStagedBuffers {
            data: flatten_nested::<DummyInputBuffer>(buffers)
                .into_iter()
                .filter_map(|buffer| buffer.data)
                .collect(),
            handle: self.0.clone(),
        })
    }
}

struct DummyInputBuffer {
    receiver: Arc<DummyInputReceiverInner>,
    data: Option<String>,
}

impl InputBuffer for DummyInputBuffer {
    fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(s) = self.data.take() {
            info!("flushing {:?}", s);
            self.receiver.flushed.lock().unwrap().push(s);
        }
        Ok(())
    }

    fn len(&self) -> BufferSize {
        self.data
            .as_ref()
            .map_or(BufferSize::empty(), |_| BufferSize {
                records: 1,
                bytes: 0,
            })
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

            if start.elapsed() >= Duration::from_secs(60) {
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
                    ConsumerCall::Buffered(BufferSize { records, bytes: _ }) => {
                        assert!(received + records <= n);
                        received += records;
                        if received == n {
                            return;
                        }
                    }
                    _ => panic!("expected ConsumerCall::Buffered, received {call:?}"),
                }
            }
            drop(current);

            if start.elapsed() >= Duration::from_secs(60) {
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

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        Some(FtModel::ExactlyOnce)
    }

    fn parse_errors(&self, errors: Vec<ParseError>) {
        if !errors.is_empty() {
            for error in errors {
                info!("parse error: {error}");
            }
            self.called(ConsumerCall::ParseErrors);
        }
    }

    fn buffered(&self, amt: BufferSize) {
        let call = ConsumerCall::Buffered(amt);
        info!("{call:?}");
        if !self.0.drop_buffered.load(Ordering::Acquire) {
            self.0.calls.lock().unwrap().push(call);
            self.0.unparker.unpark();
        } else {
            self.0.n_buffered.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn replayed(&self, amt: BufferSize, _hash: u64) {
        self.called(ConsumerCall::Replayed {
            num_records: amt.records,
        });
    }

    fn extended(&self, amt: BufferSize, resume: Option<Resume>, _watermark: Vec<Watermark>) {
        self.called(ConsumerCall::Extended {
            num_records: amt.records,
            metadata: resume
                .map(|resume| resume.into_seek().unwrap())
                .unwrap_or_default(),
        });
    }

    fn completion_watcher(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<feldera_types::coordination::Completion>> {
        None
    }

    fn error(&self, fatal: bool, error: AnyError, _tag: Option<&'static str>) {
        info!("error: {error}");
        // Record only fatal errors. Non-fatal errors, such as a momentary
        // broker disconnection, can arrive at any time, and recording them
        // would break the strict call sequence that `expect()` checks.
        if fatal {
            self.called(ConsumerCall::Error(fatal));
        }
    }

    fn request_step(&self) {}

    fn eoi(&self) {
        self.called(ConsumerCall::Eoi);
    }

    fn start_transaction(&self, _label: Option<&str>) {}

    fn commit_transaction(&self) {}

    fn update_connector_health(&self, _health: ConnectorHealth) {}
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

    let config = serde_json::from_value(json!({
      "name": "kafka_output",
      "config": {
        "topic": output_topic
      }
    }))
    .unwrap();

    let mut endpoint = output_transport_config_to_endpoint(
        &config,
        "",
        true,
        default_secrets_directory(),
        CancellationToken::new(),
    )
    .unwrap()
    .unwrap();
    assert!(endpoint.is_fault_tolerant());
    endpoint
        .connect(Box::new(|fatal, error, tag| {
            info!("({fatal:?}, {error:?}, {tag:?})")
        }))
        .unwrap();
    for step in 0..5 {
        endpoint.batch_start(step, OutputBatchType::Delta).unwrap();
        endpoint
            .push_buffer(format!("string{step}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
}

/// Verifies that a fault-tolerant Kafka output connector attaches the headers
/// from its configuration, plus any per-message headers, to every message.
#[test]
fn output_headers_test() {
    init_test_logger();
    let output_topic = "ft_output_headers_test_output_topic";

    // Create topic.
    let _kafka_resources = KafkaResources::create_topics(&[(output_topic, 1)]);

    let config = serde_json::from_value(json!({
      "name": "kafka_output",
      "config": {
        "topic": output_topic,
        "headers": [
          {
            "key": "header1",
            "value": "foobar"
          },
          {
            "key": "header2",
            "value": [1, 2, 3, 4, 5]
          }
        ]
      }
    }))
    .unwrap();

    let mut endpoint = output_transport_config_to_endpoint(
        &config,
        "",
        true,
        default_secrets_directory(),
        CancellationToken::new(),
    )
    .unwrap()
    .unwrap();
    assert!(endpoint.is_fault_tolerant());
    endpoint
        .connect(Box::new(|fatal, error, tag| {
            info!("({fatal:?}, {error:?}, {tag:?})")
        }))
        .unwrap();

    // `push_buffer` writes just the configured headers.
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint.push_buffer(b"from push_buffer").unwrap();
    endpoint.batch_end().unwrap();

    // `push_key` adds per-message headers on top of the configured ones.  A
    // `None` value is a header with a null value.
    endpoint.batch_start(1, OutputBatchType::Delta).unwrap();
    endpoint
        .push_key(
            None,
            Some(b"from push_key".as_slice()),
            &[("header3", Some(b"baz".as_slice())), ("header4", None)],
        )
        .unwrap();
    endpoint.batch_end().unwrap();

    // An exactly-once connector deduplicates output by Kafka message key, so
    // it cannot also write a caller-supplied key.
    endpoint.batch_start(2, OutputBatchType::Delta).unwrap();
    let error = endpoint
        .push_key(Some(b"key".as_slice()), Some(b"value".as_slice()), &[])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not support key-value pairs"),
        "unexpected error: {error}"
    );
    endpoint.batch_end().unwrap();

    let configured = [
        ("header1".to_string(), Some(b"foobar".to_vec())),
        ("header2".to_string(), Some(vec![1u8, 2, 3, 4, 5])),
    ];
    let per_message = [
        ("header3".to_string(), Some(b"baz".to_vec())),
        ("header4".to_string(), None),
    ];
    assert_eq!(
        read_messages(output_topic, 2),
        vec![
            (b"from push_buffer".to_vec(), configured.to_vec()),
            (
                b"from push_key".to_vec(),
                [configured.as_slice(), per_message.as_slice()].concat()
            ),
        ]
    );
}

/// A Kafka message header, where a `None` value is a null value.
type OutputHeader = (String, Option<Vec<u8>>);

/// A Kafka message's payload and headers.
type OutputMessage = (Vec<u8>, Vec<OutputHeader>);

/// Reads the first `n` messages in `topic`, returning each message's payload
/// and headers in the order that they were produced.
fn read_messages(topic: &str, n: usize) -> Vec<OutputMessage> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", default_redpanda_server())
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("group.id", format!("{topic}_reader"))
        // Fault-tolerant output connectors write inside transactions.
        .set("isolation.level", "read_committed")
        .create::<BaseConsumer>()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut messages = Vec::new();
    while messages.len() < n {
        assert!(
            Instant::now() < deadline,
            "read only {} of {n} messages in {topic} before timing out",
            messages.len()
        );
        let Some(message) = consumer.poll(Duration::from_millis(100)) else {
            continue;
        };
        let message = message.unwrap();
        let headers = match message.headers() {
            Some(headers) => (0..headers.count())
                .map(|i| {
                    let header = headers.get(i);
                    (header.key.to_string(), header.value.map(|v| v.to_vec()))
                })
                .collect(),
            None => Vec::new(),
        };
        messages.push((message.payload().unwrap_or_default().to_vec(), headers));
    }
    messages
}

fn _test() {
    let config = serde_json::from_value(json!({
      "name": "kafka_output",
      "config": {
        "topic": "my_topic"
      }
    }))
    .unwrap();

    let mut endpoint = output_transport_config_to_endpoint(
        &config,
        "",
        true,
        default_secrets_directory(),
        CancellationToken::new(),
    )
    .unwrap()
    .unwrap();
    assert!(endpoint.is_fault_tolerant());
    endpoint
        .connect(Box::new(|fatal, error, tag| {
            info!("({fatal:?}, {error:?}, {tag:?})")
        }))
        .unwrap();
    for step in 0..5 {
        endpoint.batch_start(step, OutputBatchType::Delta).unwrap();
        endpoint
            .push_buffer(format!("string{step}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
}

// Typed VARIANT checks stay with the static engine: the shared suites see a VARIANT as JSON text.

/// Sends the two messages of the metadata tests to `topic`: `first` with a header that holds a
/// value and a header without one, then `second` without headers.
fn send_metadata_messages(topic: &str, first: &[u8], second: &[u8]) {
    let producer = TestProducer::new();
    producer.send_message(
        first,
        topic,
        Some(BTreeMap::from([
            ("header1".to_string(), Some(b"foobar".to_vec())),
            ("header2".to_string(), None),
        ])),
    );
    producer.send_message(second, topic, None);
}

/// The `kafka_headers` of the messages that [`send_metadata_messages`] sends: a header value
/// is binary, and a header without a value is the VARIANT null.
fn expected_kafka_headers() -> Vec<Variant> {
    vec![
        Variant::Map(Arc::new(BTreeMap::from([
            (
                Variant::String(SqlString::from("header1")),
                Variant::Binary(ByteArray::new(b"foobar")),
            ),
            (
                Variant::String(SqlString::from("header2")),
                Variant::VariantNull,
            ),
        ]))),
        Variant::Map(Arc::new(BTreeMap::new())),
    ]
}

/// Returns the configuration of a Kafka input endpoint that reads `topic` with every Kafka
/// attribute, in `format`.
fn metadata_input_config(topic: &str, format: JsonValue) -> InputEndpointConfig {
    serde_json::from_value(json!({
        "stream": "test_input",
        "transport": {
            "name": "kafka_input",
            "config": {
                "topic": topic,
                "log_level": "debug",
                "auto.offset.reset": "earliest",
                "include_headers": true,
                "include_topic": true,
                "include_timestamp": true,
                "include_partition": true,
                "include_offset": true
            }
        },
        "format": format
    }))
    .unwrap()
}

#[test]
fn test_kafka_metadata_json() {
    init_test_logger();

    let topic = "kafka_metadata_json_typed_test_topic";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    let config = metadata_input_config(
        topic,
        json!({ "name": "json", "config": { "update_format": "raw" } }),
    );
    let (endpoint, _, _, zset) =
        mock_input_pipeline::<TestStructMetadata, TestStructMetadata>(config, Relation::empty())
            .unwrap();
    endpoint.extend();

    send_metadata_messages(topic, b"{\"i\": 0}", b"{\"i\": 1}");
    let received = wait_for_output_count(&zset, 2, || endpoint.queue(false));
    let headers: Vec<Variant> = received
        .into_iter()
        .map(|record| record.kafka_headers)
        .collect();
    assert_eq!(headers, expected_kafka_headers());
}

#[test]
fn test_kafka_metadata_raw() {
    init_test_logger();

    let topic = "kafka_metadata_raw_typed_test_topic";
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    let config = metadata_input_config(
        topic,
        json!({ "name": "raw", "config": { "mode": "blob", "column_name": "data" } }),
    );
    let (endpoint, _, _, zset) = mock_input_pipeline::<KafkaRawMetadata, KafkaRawMetadata>(
        config,
        suites::kafka::raw_metadata_relation(),
    )
    .unwrap();
    endpoint.extend();

    send_metadata_messages(topic, b"foo", b"bar");
    let received = wait_for_output_count(&zset, 2, || endpoint.queue(false));
    let headers: Vec<Variant> = received
        .into_iter()
        .map(|record| record.kafka_headers)
        .collect();
    assert_eq!(headers, expected_kafka_headers());
}

/// Tests for distributed connectors, which divide a topic's partitions among
/// the hosts of a multihost pipeline.
mod distributed {
    use std::collections::BTreeSet;

    use super::*;
    use crate::transport::kafka::KafkaFtInputEndpoint;
    use crate::transport::kafka::ft::input::select_partitions;
    use feldera_types::coordination::{InputDistribution, InputShard};
    use rdkafka::producer::Producer;

    fn shard(host: usize, n_hosts: usize, home: usize) -> InputShard {
        InputShard::new(host, n_hosts, InputDistribution { home }).unwrap()
    }

    fn config(extra: JsonValue) -> KafkaInputConfig {
        let mut config = json!({
            "topic": "t",
            "bootstrap.servers": "localhost:9092",
        });
        config
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(config).unwrap()
    }

    #[test]
    fn select_all_partitions_of_topic() {
        // Three hosts, home host 1: partition p goes to host (p + 1) % 3.
        let selected = |host| {
            select_partitions(config(json!({})), 5, &shard(host, 3, 1))
                .unwrap()
                .partitions
                .unwrap()
        };
        assert_eq!(selected(0), [2]);
        assert_eq!(selected(1), [0, 3]);
        assert_eq!(selected(2), [1, 4]);
    }

    #[test]
    fn select_from_explicit_partitions_and_offsets() {
        let config = config(json!({
            "partitions": [7, 2, 4],
            "start_from": {"offsets": [70, 20, 40]},
        }));
        let selected = select_partitions(config.clone(), 10, &shard(0, 2, 0)).unwrap();
        assert_eq!(selected.partitions.unwrap(), [2, 7]);
        assert_eq!(
            selected.start_from,
            KafkaStartFromConfig::Offsets(vec![20, 70])
        );

        let selected = select_partitions(config, 10, &shard(1, 2, 0)).unwrap();
        assert_eq!(selected.partitions.unwrap(), [4]);
        assert_eq!(selected.start_from, KafkaStartFromConfig::Offsets(vec![40]));
    }

    /// An explicit list of partitions that are not consecutive divides
    /// evenly, because the hosts divide it by position.
    #[test]
    fn explicit_partitions_divide_evenly() {
        let config = config(json!({"partitions": [0, 2, 4, 6]}));
        let selected = |host| {
            select_partitions(config.clone(), 8, &shard(host, 2, 0))
                .unwrap()
                .partitions
                .unwrap()
        };
        assert_eq!(selected(0), [0, 4]);
        assert_eq!(selected(1), [2, 6]);
    }

    /// Resume metadata is positional in a host's partition list, so a
    /// partition that the topic gains must go after the host's existing ones.
    #[test]
    fn new_partitions_go_last() {
        for host in 0..3 {
            let before = select_partitions(config(json!({})), 4, &shard(host, 3, 2))
                .unwrap()
                .partitions
                .unwrap();
            let after = select_partitions(config(json!({})), 9, &shard(host, 3, 2))
                .unwrap()
                .partitions
                .unwrap();
            assert_eq!(after[..before.len()], before[..]);
        }
    }

    #[test]
    fn mismatched_offsets_are_rejected() {
        let config = config(json!({"start_from": {"offsets": [1, 2]}}));
        assert!(select_partitions(config, 3, &shard(0, 2, 0)).is_err());
    }

    #[test]
    fn synchronize_partitions_is_rejected() {
        let sync = config(json!({"synchronize_partitions": true}));
        assert!(KafkaFtInputEndpoint::new(sync.clone(), Some(InputShard::ALL)).is_err());
        assert!(KafkaFtInputEndpoint::new(sync, None).is_ok());
    }

    fn create_reader(
        topic: &str,
        shard: InputShard,
        start_from: JsonValue,
    ) -> (
        Box<dyn TransportInputEndpoint>,
        DummyInputReceiver,
        Box<dyn InputReader>,
    ) {
        create_resumed_reader(topic, shard, start_from, None)
    }

    fn create_resumed_reader(
        topic: &str,
        shard: InputShard,
        start_from: JsonValue,
        resume_info: Option<&Metadata>,
    ) -> (
        Box<dyn TransportInputEndpoint>,
        DummyInputReceiver,
        Box<dyn InputReader>,
    ) {
        let config = serde_json::from_value(json!({
          "name": "kafka_input",
          "config": {
              "topic": topic,
              "log_level": "debug",
              "start_from": start_from,
          },
        }))
        .unwrap();
        let endpoint = input_transport_config_to_endpoint(
            &config,
            "",
            default_secrets_directory(),
            Some(shard),
        )
        .unwrap()
        .unwrap();
        let receiver = DummyInputReceiver::new();
        let reader = endpoint
            .open(
                receiver.consumer(),
                Box::new(DummyParser::new(&receiver)),
                Relation::empty(),
                resume_info.map(|metadata| serde_json::to_value(metadata).unwrap()),
            )
            .unwrap();
        (endpoint, receiver, reader)
    }

    /// Produces `n` records into `partition`, with IDs `first..first + n`.
    fn produce(producer: &TestProducer, topic: &str, partition: i32, first: u32, n: u32) {
        for id in first..first + n {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::new());
            writer.serialize(TestStruct::for_id(id)).unwrap();
            let bytes = writer.into_inner().unwrap();
            let record = <BaseRecord<(), [u8], ()>>::to(topic)
                .payload(&bytes)
                .partition(partition);
            producer.producer.send(record).unwrap();
        }
        producer.producer.flush(Duration::from_secs(10)).unwrap();
    }

    /// Three hosts together read every record of a topic exactly once, and
    /// each one reads only its own partitions.
    #[test]
    fn hosts_divide_partitions() {
        init_test_logger();
        const TOPIC: &str = "distributed_partitions";
        let _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 5)]);

        // Partition `p` gets `p + 1` records, with IDs that start at `10 * p`.
        let producer = TestProducer::new();
        for partition in 0..5 {
            produce(
                &producer,
                TOPIC,
                partition,
                10 * partition as u32,
                partition as u32 + 1,
            );
        }

        // With home host 1, host 0 reads partition 2, host 1 reads partitions
        // 0 and 3, and host 2 reads partitions 1 and 4.
        let mut all_records = BTreeSet::new();
        for (host, partitions) in [(0, vec![2]), (1, vec![0, 3]), (2, vec![1, 4])] {
            let (_endpoint, receiver, reader) =
                create_reader(TOPIC, shard(host, 3, 1), json!("earliest"));
            reader.extend();
            let n: usize = partitions.iter().map(|p| p + 1).sum();
            receiver.expect_buffering(n);
            reader.queue(false);
            let metadata = Metadata {
                offsets: partitions.iter().map(|&p| 0..p as i64 + 1).collect(),
                shard: Some(shard(host, 3, 1)),
                partitions: Some(partitions.iter().map(|&p| p as i32).collect()),
            };
            receiver.expect(vec![ConsumerCall::Extended {
                num_records: n,
                metadata: serde_json::to_value(&metadata).unwrap(),
            }]);
            for record in take_flushed(&receiver) {
                let id: u32 = record.split(',').next().unwrap().parse().unwrap();
                assert!(
                    partitions.contains(&(id as usize / 10)),
                    "host {host} read record {id} from a partition it does not own"
                );
                assert!(all_records.insert(id), "record {id} read twice");
            }
        }
        let expected = (0..5u32)
            .flat_map(|p| 10 * p..10 * p + p + 1)
            .collect::<BTreeSet<_>>();
        assert_eq!(all_records, expected);
    }

    /// A host resumes only from metadata that it wrote while reading the same
    /// shard, because the offsets in the metadata are positional in the
    /// shard's partitions.
    #[test]
    // A shard with one partition has one range of offsets.
    #[allow(clippy::single_range_in_vec_init)]
    fn resume_requires_the_same_shard() {
        init_test_logger();
        const TOPIC: &str = "distributed_resume_shard";
        let _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 2)]);
        let producer = TestProducer::new();
        produce(&producer, TOPIC, 0, 0, 3);

        // Host 0 of 2 with home host 0 reads partition 0.  It read 1 record
        // before the checkpoint, so 2 remain.
        let ours = shard(0, 2, 0);
        // An older checkpoint, which does not name its partitions.
        let checkpoint = |shard| Metadata {
            offsets: vec![0..1],
            shard: Some(shard),
            partitions: None,
        };
        let (_endpoint, receiver, reader) =
            create_resumed_reader(TOPIC, ours, json!("earliest"), Some(&checkpoint(ours)));
        reader.extend();
        receiver.expect_buffering(2);
        reader.queue(false);
        receiver.expect(vec![ConsumerCall::Extended {
            num_records: 2,
            metadata: serde_json::to_value(Metadata {
                offsets: vec![1..3],
                shard: Some(ours),
                partitions: Some(vec![0]),
            })
            .unwrap(),
        }]);

        // With home host 1, host 0 would read partition 1 instead, so it must
        // not apply the checkpoint's offsets.
        let (_endpoint, receiver, _reader) = create_resumed_reader(
            TOPIC,
            ours,
            json!("earliest"),
            Some(&checkpoint(shard(0, 2, 1))),
        );
        receiver.expect(vec![ConsumerCall::Error(true)]);

        // A host that reads all of the partitions must not either.
        let (_endpoint, receiver, _reader) = super::create_reader(
            TOPIC,
            Some(serde_json::to_value(checkpoint(ours)).unwrap()),
            false,
        );
        receiver.expect(vec![ConsumerCall::Error(true)]);
    }

    /// A host that owns no partitions reads nothing, and ends its input once
    /// it runs, so that it does not hold back the connector's merged
    /// frontier.  Its reader keeps running, so that the controller keeps the
    /// endpoint.  This holds for every way to choose where to start reading.
    #[test]
    fn host_without_partitions_ends_input() {
        init_test_logger();
        const TOPIC: &str = "distributed_idle_host";
        let _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 2)]);
        let producer = TestProducer::new();
        produce(&producer, TOPIC, 0, 0, 3);
        produce(&producer, TOPIC, 1, 3, 3);

        // With 3 hosts and home host 0, partitions 0 and 1 go to hosts 0 and
        // 1, so host 2 has none.
        for start_from in [
            json!("earliest"),
            json!("latest"),
            json!({"timestamp": 0}),
            json!({"offsets": [1, 2]}),
        ] {
            println!("start_from: {start_from}");
            let (_endpoint, receiver, reader) = create_reader(TOPIC, shard(2, 3, 0), start_from);

            // While paused, the host does not end its input.
            reader.pause();
            sleep(Duration::from_secs(1));
            assert_eq!(*receiver.inner.calls.lock().unwrap(), vec![]);

            reader.extend();
            receiver.expect(vec![ConsumerCall::Eoi]);
            assert_eq!(take_flushed(&receiver), Vec::<String>::new());

            // The reader neither exits nor ends its input again.
            sleep(Duration::from_secs(2));
            assert!(!reader.is_closed());
            assert_eq!(*receiver.inner.calls.lock().unwrap(), vec![]);
        }
    }
}

/// Tests for the end of input, with `enable.partition.eof`.
mod end_of_input {
    use super::*;
    use rdkafka::producer::Producer;

    fn produce(producer: &TestProducer, topic: &str, ids: Range<u32>) {
        for id in ids {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::new());
            writer.serialize(TestStruct::for_id(id)).unwrap();
            let bytes = writer.into_inner().unwrap();
            let record = <BaseRecord<(), [u8], ()>>::to(topic)
                .payload(&bytes)
                .partition(0);
            producer.producer.send(record).unwrap();
        }
        producer.producer.flush(Duration::from_secs(10)).unwrap();
    }

    /// Once the reader reaches the end of every partition, it ends its input
    /// and reads nothing more, even if more records arrive, but it keeps
    /// running, so that the controller keeps the endpoint.
    #[test]
    fn reads_nothing_after_end_of_input() {
        init_test_logger();
        const TOPIC: &str = "end_of_input";
        let _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 1)]);
        let producer = TestProducer::new();
        produce(&producer, TOPIC, 0..3);

        let config = serde_json::from_value(json!({
          "name": "kafka_input",
          "config": {
              "topic": TOPIC,
              "log_level": "debug",
              "start_from": "earliest",
              "enable.partition.eof": "true",
          },
        }))
        .unwrap();
        let endpoint =
            input_transport_config_to_endpoint(&config, "", default_secrets_directory(), None)
                .unwrap()
                .unwrap();
        let receiver = DummyInputReceiver::new();
        let reader = endpoint
            .open(
                receiver.consumer(),
                Box::new(DummyParser::new(&receiver)),
                Relation::empty(),
                None,
            )
            .unwrap();
        reader.extend();
        let ended = || {
            receiver
                .inner
                .calls
                .lock()
                .unwrap()
                .contains(&ConsumerCall::Eoi)
        };
        let buffered = || {
            receiver
                .inner
                .calls
                .lock()
                .unwrap()
                .iter()
                .map(|call| match call {
                    ConsumerCall::Buffered(amt) => amt.records,
                    _ => 0,
                })
                .sum::<usize>()
        };
        wait(ended, 60_000).expect("timed out reaching the end of input");
        assert_eq!(buffered(), 3);

        produce(&producer, TOPIC, 3..6);
        sleep(Duration::from_secs(3));
        assert_eq!(buffered(), 3);
        assert!(!reader.is_closed());
    }
}

/// Tests for a `partitions` list that is not in ascending order.
///
/// The reader keeps its partitions in ascending order internally, so it must
/// not match them with the list by position.
mod unsorted_partitions {
    use super::*;
    use crate::transport::kafka::ft::input::sort_partitions;
    use rdkafka::producer::Producer;

    fn config(extra: JsonValue) -> KafkaInputConfig {
        let mut config = json!({"topic": "t", "bootstrap.servers": "localhost:9092"});
        config
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(config).unwrap()
    }

    #[test]
    fn sort_keeps_offsets_with_their_partitions() {
        let sorted = sort_partitions(config(json!({
            "partitions": [7, 2, 4],
            "start_from": {"offsets": [70, 20, 40]},
        })));
        assert_eq!(sorted.partitions.unwrap(), [2, 4, 7]);
        assert_eq!(
            sorted.start_from,
            KafkaStartFromConfig::Offsets(vec![20, 40, 70])
        );

        let sorted = sort_partitions(config(json!({"partitions": [3, 1, 2]})));
        assert_eq!(sorted.partitions.unwrap(), [1, 2, 3]);

        // Without a list, there is nothing to sort.
        assert_eq!(sort_partitions(config(json!({}))).partitions, None);

        // Mismatched offsets keep their order, so that the reader can report
        // the mismatch as the user wrote it.
        let sorted = sort_partitions(config(json!({
            "partitions": [7, 2],
            "start_from": {"offsets": [70, 20, 40]},
        })));
        assert_eq!(sorted.partitions.unwrap(), [7, 2]);
        assert_eq!(
            sorted.start_from,
            KafkaStartFromConfig::Offsets(vec![70, 20, 40])
        );
    }

    fn create_reader(
        topic: &str,
        extra_config: JsonValue,
        resume_info: Option<JsonValue>,
    ) -> (
        Box<dyn TransportInputEndpoint>,
        DummyInputReceiver,
        Box<dyn InputReader>,
    ) {
        let mut inner_config = json!({
            "topic": topic,
            "log_level": "debug",
            "start_from": "earliest",
        });
        inner_config
            .as_object_mut()
            .unwrap()
            .extend(extra_config.as_object().unwrap().clone());
        let config = serde_json::from_value(json!({
          "name": "kafka_input",
          "config": inner_config,
        }))
        .unwrap();
        let endpoint =
            input_transport_config_to_endpoint(&config, "", default_secrets_directory(), None)
                .unwrap()
                .unwrap();
        let receiver = DummyInputReceiver::new();
        let reader = endpoint
            .open(
                receiver.consumer(),
                Box::new(DummyParser::new(&receiver)),
                Relation::empty(),
                resume_info,
            )
            .unwrap();
        (endpoint, receiver, reader)
    }

    /// Produces records with IDs `ids` into `partition`, each with its ID as
    /// its timestamp offset from `base`.
    fn produce(producer: &TestProducer, topic: &str, partition: i32, base: i64, ids: Range<u32>) {
        for id in ids {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::new());
            writer.serialize(TestStruct::for_id(id)).unwrap();
            let bytes = writer.into_inner().unwrap();
            let record = <BaseRecord<(), [u8], ()>>::to(topic)
                .payload(&bytes)
                .timestamp(base + id as i64)
                .partition(partition);
            producer.producer.send(record).unwrap();
        }
        producer.producer.flush(Duration::from_secs(10)).unwrap();
    }

    /// Returns the IDs of the records flushed to `receiver` so far.
    fn flushed_ids(receiver: &DummyInputReceiver) -> Vec<u32> {
        take_flushed(receiver)
            .iter()
            .map(|record| record.split(',').next().unwrap().parse().unwrap())
            .collect()
    }

    /// With synchronized partitions, the reader must read the partition with
    /// the earliest record, not the partition at the same position in the
    /// `partitions` list.
    #[test]
    fn synchronization() {
        init_test_logger();
        const TOPIC: &str = "unsorted_partitions_synchronization";
        let _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 3)]);

        // Partition `p` gets records `10 * p` and `10 * p + 1`, so partition 0
        // has the earliest records.  After it reads both of them, partition 0
        // is empty, so synchronization stops.
        let producer = TestProducer::new();
        let base = Timestamp::now().to_millis().unwrap();
        for partition in 0..3 {
            let first = 10 * partition as u32;
            produce(&producer, TOPIC, partition, base, first..first + 2);
        }

        let (_endpoint, receiver, reader) = create_reader(
            TOPIC,
            json!({"partitions": [2, 0, 1], "synchronize_partitions": true}),
            None,
        );
        reader.extend();
        receiver.expect_buffering(2);
        reader.queue(false);
        let metadata = Metadata {
            offsets: vec![0..2, 0..0, 0..0],
            shard: None,
            partitions: None,
        };
        receiver.expect(vec![ConsumerCall::Extended {
            num_records: 2,
            metadata: serde_json::to_value(&metadata).unwrap(),
        }]);
        assert_eq!(flushed_ids(&receiver), [0, 1]);
    }

    /// Resuming from a checkpoint must give each partition its own offset.
    #[test]
    fn resume() {
        init_test_logger();
        const TOPIC: &str = "unsorted_partitions_resume";
        let _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 2)]);
        let producer = TestProducer::new();
        let base = Timestamp::now().to_millis().unwrap();
        produce(&producer, TOPIC, 0, base, 0..5);
        produce(&producer, TOPIC, 1, base, 10..15);

        // The checkpoint read 1 record from partition 0 and all 5 records
        // from partition 1, so 4 records remain, all from partition 0.
        let resume_info = Metadata {
            offsets: vec![0..1, 0..5],
            shard: None,
            partitions: None,
        };
        let (_endpoint, receiver, reader) = create_reader(
            TOPIC,
            json!({"partitions": [1, 0]}),
            Some(serde_json::to_value(&resume_info).unwrap()),
        );
        reader.extend();
        receiver.expect_buffering(4);
        reader.queue(false);
        let metadata = Metadata {
            offsets: vec![1..5, 5..5],
            shard: None,
            partitions: None,
        };
        receiver.expect(vec![ConsumerCall::Extended {
            num_records: 4,
            metadata: serde_json::to_value(&metadata).unwrap(),
        }]);
        assert_eq!(flushed_ids(&receiver), [1, 2, 3, 4]);
    }
}
