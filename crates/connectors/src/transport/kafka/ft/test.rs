//! Tests of the fault-tolerant Kafka transport that reach no engine: the readers, with
//! dummy parsers and consumers, and the writers, against the broker; and unit tests of
//! partition selection. The connector tests in `transport::kafka::tests` cover the transport
//! with an engine.

// A reader's offsets are ranges, so one range is one partition's offsets, not a mistake.
#![allow(clippy::single_range_in_vec_init)]

use crate::{
    InputBuffer, InputConsumer, OutputEndpoint, ParseError, Parser,
    format::{Splitter, SpongeSplitter},
    test::{
        engine_test, input_pipeline,
        kafka::{BufferConsumer, KafkaResources, TestProducer},
        pipeline::{PipelineTestEngine, TestCircuit},
        published_schema, read, relations, render, rows,
        suites::json_parser,
        wait,
    },
    transport::output_transport_config_to_endpoint,
};
#[cfg(test)]
use crate::{
    InputReader, TransportInputEndpoint,
    test::kafka::csv_messages,
    transport::{
        input_transport_config_to_endpoint,
        kafka::ft::input::{BACKPRESSURE, Metadata},
    },
};
use anyhow::Error as AnyError;
use crossbeam::sync::{Parker, Unparker};
#[cfg(test)]
use csv::WriterBuilder as CsvWriterBuilder;
use feldera_adapterlib::{
    ConnectorMetadata,
    catalog::StagedBuffers,
    format::{BufferSize, flatten_nested},
    test::{
        Capability, ConnectorTestEngine, InputOptions, MockOutputCallbacks, Updates, wait_for_rows,
    },
    transport::{OutputBatchType, Resume, Watermark},
};
use feldera_types::{
    adapter_stats::ConnectorHealth,
    config::{
        ConnectorConfig, FormatConfig, FtModel, InputEndpointConfig, PipelineConfig,
        TransportConfig,
    },
    program_schema::{ColumnType, Field, Relation},
    secret_resolver::default_secrets_directory,
    transport::kafka::{
        KafkaInputConfig, KafkaLogLevel, KafkaStartFromConfig, default_redpanda_server,
    },
};
use proptest::prelude::*;
#[cfg(test)]
use rand::thread_rng;
#[cfg(test)]
use rdkafka::{
    ClientConfig,
    consumer::{BaseConsumer, Consumer},
    producer::{BaseRecord, Producer},
};
use rdkafka::{
    Message, Offset, Timestamp, TopicPartitionList,
    message::{BorrowedMessage, Header, Headers},
};
#[cfg(test)]
use rmpv::Value as RmpValue;
use serde_json::{Value as JsonValue, json};
#[cfg(test)]
use std::iter::repeat_with;
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::create_dir,
    hash::Hasher,
    mem,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::sleep,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// A record of the CSV messages that these tests send.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Default, Clone, serde::Serialize)]
struct CsvRecord {
    id: u32,
    b: bool,
    i: Option<i64>,
    s: String,
}

impl CsvRecord {
    /// Returns the record with `id` and default values in the other columns.
    #[cfg_attr(not(test), allow(dead_code))]
    fn for_id(id: u32) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }
}

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

#[cfg(test)]
fn create_reader(
    topic: &str,
    resume_info: Option<JsonValue>,
    synchronize_partitions: bool,
) -> (
    Box<dyn TransportInputEndpoint>,
    DummyInputReceiver,
    Box<dyn InputReader>,
) {
    create_reader_config(topic, resume_info, synchronize_partitions, json!({}))
}

/// Like [`create_reader`], plus `extra_config` in the connector configuration.
#[cfg(test)]
fn create_reader_config(
    topic: &str,
    resume_info: Option<JsonValue>,
    synchronize_partitions: bool,
    extra_config: JsonValue,
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
    for (key, value) in extra_config.as_object().unwrap() {
        inner_config
            .as_object_mut()
            .unwrap()
            .insert(key.clone(), value.clone());
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
#[cfg(test)]
fn take_flushed(receiver: &DummyInputReceiver) -> Vec<String> {
    mem::take(&mut *receiver.inner.flushed.lock().unwrap())
}

/// Build the `headers` argument for [`TestProducer::send_message`] from
/// `(key, value)` pairs, where a `None` value denotes a header with a null
/// value.
#[cfg(test)]
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
        writer.serialize(CsvRecord::for_id(id as u32)).unwrap();
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
        writer.serialize(CsvRecord::for_id(id)).unwrap();
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

/// Prefetch for a partition that the reader does not drain stays near 4 MB.
#[test]
fn test_prefetch_limit() {
    init_test_logger();

    const TOPIC: &str = "prefetch_limit";
    const MIB: usize = 1024 * 1024;
    let mut _kafka_resources = KafkaResources::create_topics(&[(TOPIC, 2)]);

    // Empty partition 1 stops synchronized partition 0 at 1000 messages, like a stuck parser.
    let (_endpoint, _receiver, reader) =
        create_reader_config(TOPIC, None, true, json!({"statistics.interval.ms": "1000"}));
    reader.extend();

    // 32 MiB: without the limit, librdkafka prefetches all of it.
    let producer = TestProducer::new();
    let payload = vec![b'x'; 16 * 1024];
    for _ in 0..2048 {
        let record = <BaseRecord<(), [u8], ()>>::to(TOPIC)
            .payload(&payload)
            .partition(0);
        producer.producer.send(record).unwrap();
    }
    producer.producer.flush(Duration::from_secs(30)).unwrap();

    wait(|| reader.memory() > 2 * MIB, 20000).expect("no prefetch measured");

    // Watch a few more 1 s reports in case prefetch keeps growing.
    let mut peak = 0;
    for _ in 0..30 {
        peak = peak.max(reader.memory());
        sleep(Duration::from_millis(100));
    }
    // 4 MB limit plus up to 1 MiB from one fetch.
    assert!(peak < 8 * MIB, "prefetch {peak} bytes exceeds the limit");
}

/// librdkafka accepts the defaults with options that interact with the fetch size.
#[test]
fn test_prefetch_defaults_accepted() {
    for options in [
        vec![],
        vec![("message.max.bytes", "104857600")],
        vec![("message.max.bytes", "0x6400000")],
        vec![("message.max.bytes", " 104857600")],
        vec![
            ("queued.max.messages.kbytes", "1024"),
            ("receive.message.max.bytes", "10000000"),
        ],
    ] {
        let options = options
            .into_iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let mut config = KafkaInputConfig::default(options, "t");
        config.validate().unwrap();
        let mut client_config = ClientConfig::new();
        for (key, value) in &config.kafka_options {
            client_config.set(key, value);
        }
        if let Err(error) = client_config.create::<BaseConsumer>() {
            panic!("{:?}: {error}", config.kafka_options);
        }
    }
}

#[cfg(test)]
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
            .map(|id| vec![CsvRecord::for_id(id)])
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
        create_reader_config(topic, None, false, json!({"header_filter": filter.clone()}));
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
    let (_endpoint, receiver, reader) =
        create_reader_config(topic, None, false, json!({"header_filter": filter}));
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
        create_reader_config(topic, None, false, json!({"header_filter": filter.clone()}));
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
    let (_endpoint, receiver, reader) =
        create_reader_config(topic, None, false, json!({"header_filter": filter}));
    receiver.inner.drop_buffered.store(true, Ordering::Release);
    reader.replay(serde_json::to_value(&metadata).unwrap(), RmpValue::Nil);
    receiver.expect(vec![ConsumerCall::Replayed { num_records: 2 }]);
    assert_eq!(take_flushed(&receiver), vec!["m1", "m3"]);
}

#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
struct DummyParser(Arc<DummyInputReceiverInner>);

impl DummyParser {
    #[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
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

#[cfg_attr(not(test), allow(dead_code))]
struct DummyInputReceiver {
    inner: Arc<DummyInputReceiverInner>,
    parker: Parker,
}

#[cfg_attr(not(test), allow(dead_code))]
struct DummyInputReceiverInner {
    unparker: Unparker,
    n_buffered: AtomicUsize,
    drop_buffered: AtomicBool,
    calls: Mutex<Vec<ConsumerCall>>,
    flushed: Mutex<Vec<String>>,
}

impl DummyInputReceiver {
    #[cfg_attr(not(test), allow(dead_code))]
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

    #[cfg_attr(not(test), allow(dead_code))]
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

    #[cfg_attr(not(test), allow(dead_code))]
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

    #[cfg_attr(not(test), allow(dead_code))]
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
    #[cfg_attr(not(test), allow(dead_code))]
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn consumer(&self) -> Box<dyn InputConsumer> {
        Box::new(DummyInputConsumer(self.inner.clone()))
    }
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
struct DummyInputConsumer(Arc<DummyInputReceiverInner>);

impl DummyInputConsumer {
    #[cfg_attr(not(test), allow(dead_code))]
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

#[cfg(test)]
fn kafka_output_test(
    test_name: &str,
    _format: &str,
    _format_config: &str,
    _message_max_bytes: usize,
    _data: Vec<Vec<CsvRecord>>,
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
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();
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
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();

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

    // With a caller-supplied key, the position header goes first, ahead of
    // the configured and per-message headers.
    endpoint.batch_start(2, OutputBatchType::Delta).unwrap();
    endpoint
        .push_key(
            Some(b"key".as_slice()),
            Some(b"from keyed push_key".as_slice()),
            &[("header3", Some(b"baz".as_slice())), ("header4", None)],
        )
        .unwrap();
    endpoint.batch_end().unwrap();

    let configured = [
        ("header1".to_string(), Some(b"foobar".to_vec())),
        ("header2".to_string(), Some(vec![1u8, 2, 3, 4, 5])),
    ];
    let per_message = [
        ("header3".to_string(), Some(b"baz".to_vec())),
        ("header4".to_string(), None),
    ];
    let position = [(
        super::output::POSITION_HEADER.to_string(),
        Some(br#"{"transaction":2,"substep":0}"#.to_vec()),
    )];
    assert_eq!(
        read_messages(output_topic, 3),
        vec![
            (b"from push_buffer".to_vec(), configured.to_vec()),
            (
                b"from push_key".to_vec(),
                [configured.as_slice(), per_message.as_slice()].concat()
            ),
            (
                b"from keyed push_key".to_vec(),
                [
                    position.as_slice(),
                    configured.as_slice(),
                    per_message.as_slice()
                ]
                .concat()
            ),
        ]
    );
}

/// A keyed Kafka message's partition, key, payload, and position header value.
type KeyedMessage = (i32, Vec<u8>, Vec<u8>, Vec<u8>);

/// Polls `consumer` for a keyed output message.
fn read_keyed_message(consumer: &BaseConsumer) -> Option<KeyedMessage> {
    let message = consumer.poll(Duration::from_millis(100))?.unwrap();
    let headers = message
        .headers()
        .expect("keyed messages should have headers");
    let position = headers
        .iter()
        .find(|header| header.key == super::output::POSITION_HEADER)
        .expect("keyed messages should have a position header")
        .value
        .expect("position header should have a value")
        .to_vec();
    Some((
        message.partition(),
        message.key().unwrap().to_vec(),
        message.payload().unwrap().to_vec(),
        position,
    ))
}

/// Verifies that a fault-tolerant Kafka output connector supports
/// caller-supplied keys: the key is written as-is, the position travels in a
/// header, and records with the same key land in the same partition, so that
/// a consumer of a subset of partitions sees them in order (issue #7355).
#[test]
fn output_keyed_test() {
    init_test_logger();
    let output_topic = "ft_output_keyed_test_output_topic";

    // Create topic with multiple partitions.
    let _kafka_resources = KafkaResources::create_topics(&[(output_topic, 2)]);

    let config = serde_json::from_value(json!({
      "name": "kafka_output",
      "config": { "topic": output_topic }
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
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();

    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint
        .push_key(Some(b"key-a".as_slice()), Some(b"1".as_slice()), &[])
        .unwrap();
    endpoint
        .push_key(Some(b"key-b".as_slice()), Some(b"2".as_slice()), &[])
        .unwrap();
    endpoint
        .push_key(Some(b"key-a".as_slice()), Some(b"3".as_slice()), &[])
        .unwrap();
    endpoint.batch_end().unwrap();

    let consumer = ClientConfig::new()
        .set("bootstrap.servers", default_redpanda_server())
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("group.id", format!("{output_topic}_reader"))
        // Fault-tolerant output connectors write inside transactions.
        .set("isolation.level", "read_committed")
        .create::<BaseConsumer>()
        .unwrap();
    consumer.subscribe(&[output_topic]).unwrap();

    let mut messages: Vec<KeyedMessage> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while messages.len() < 3 {
        assert!(
            Instant::now() < deadline,
            "read only {} of 3 messages in {output_topic} before timing out",
            messages.len()
        );
        let Some(message) = read_keyed_message(&consumer) else {
            continue;
        };
        messages.push(message);
    }

    // The caller-supplied key is written as-is, and messages with the same
    // key land in the same partition, in the order they were pushed.  The
    // order across partitions is arbitrary, so we only check within
    // partitions.
    assert!(
        messages
            .iter()
            .all(|message| message.1 == b"key-a".to_vec() || message.1 == b"key-b".to_vec()),
        "unexpected key: {messages:?}"
    );
    let key_a_messages: Vec<_> = messages
        .iter()
        .filter(|message| message.1 == b"key-a".to_vec())
        .collect();
    let key_b_messages: Vec<_> = messages
        .iter()
        .filter(|message| message.1 == b"key-b".to_vec())
        .collect();
    assert_eq!(key_a_messages.len(), 2, "in {messages:?}");
    assert_eq!(key_b_messages.len(), 1, "in {messages:?}");
    assert_eq!(
        key_a_messages[0].0, key_a_messages[1].0,
        "messages with the same key should land in the same partition: {messages:?}"
    );
    assert_eq!(
        key_a_messages
            .iter()
            .map(|(_, _, payload, _)| payload.clone())
            .collect::<Vec<_>>(),
        vec![b"1".to_vec(), b"3".to_vec()],
        "messages with the same key should stay in order: {messages:?}"
    );

    // The position header lets recovery pick up where this run left off.
    for (_, _, _, position) in &messages {
        let position: serde_json::Value = serde_json::from_slice(position).unwrap();
        assert_eq!(position["transaction"], json!(0), "in {messages:?}");
    }

    // A restarted endpoint recovers the transaction number from the position
    // headers, so it drops the already-written transaction 0 and only writes
    // transaction 1.
    let mut endpoint = output_transport_config_to_endpoint(
        &config,
        "",
        true,
        default_secrets_directory(),
        CancellationToken::new(),
    )
    .unwrap()
    .unwrap();
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();
    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    endpoint
        .push_key(Some(b"key-a".as_slice()), Some(b"dropped".as_slice()), &[])
        .unwrap();
    endpoint.batch_end().unwrap();
    endpoint.batch_start(1, OutputBatchType::Delta).unwrap();
    endpoint
        .push_key(Some(b"key-a".as_slice()), Some(b"4".as_slice()), &[])
        .unwrap();
    endpoint.batch_end().unwrap();

    // The consumer is already positioned after the first three messages, so
    // this reads the one new message from transaction 1.
    let mut new_messages: Vec<KeyedMessage> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(60);
    while new_messages.is_empty() {
        assert!(
            Instant::now() < deadline,
            "no new messages in {output_topic} before timing out"
        );
        let Some(message) = read_keyed_message(&consumer) else {
            continue;
        };
        new_messages.push(message);
    }
    // The first message read must be transaction 1's record; if the replayed
    // transaction 0 record ("dropped") had been written, it would come first.
    assert_eq!(new_messages[0].2, b"4".to_vec(), "in {new_messages:?}");
    let position: serde_json::Value = serde_json::from_slice(&new_messages[0].3).unwrap();
    assert_eq!(position["transaction"], json!(1), "in {new_messages:?}");
}

/// Verifies that a fault-tolerant Kafka output connector refuses keyed
/// messages that recovery could not rely on: tombstones, which compaction
/// eventually removes, and messages too large for the broker.  A refused
/// message writes nothing and takes no substep, so the next message in the
/// same transaction gets substep 0.
#[test]
fn output_keyed_refused_test() {
    init_test_logger();
    let output_topic = "ft_output_keyed_refused_test_output_topic";

    let _kafka_resources = KafkaResources::create_topics(&[(output_topic, 1)]);

    let config = serde_json::from_value(json!({
      "name": "kafka_output",
      "config": {
        "topic": output_topic,
        "message.max.bytes": "1000"
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
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();

    endpoint.batch_start(0, OutputBatchType::Delta).unwrap();
    let error = endpoint
        .push_key(Some(b"key".as_slice()), None, &[])
        .unwrap_err();
    assert!(
        error.to_string().contains("tombstones"),
        "unexpected error: {error}"
    );
    let error = endpoint
        .push_key(Some(b"key".as_slice()), Some(&[0u8; 1000]), &[])
        .unwrap_err();
    assert!(
        error.to_string().contains("exceeds the maximum"),
        "unexpected error: {error}"
    );
    // Headers count toward the limit even when key and value are small.
    let error = endpoint
        .push_key(
            Some(b"key".as_slice()),
            Some(b"small".as_slice()),
            &[("big", Some([0u8; 1000].as_slice()))],
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("exceeds the maximum"),
        "unexpected error: {error}"
    );
    endpoint
        .push_key(Some(b"key".as_slice()), Some(b"kept".as_slice()), &[])
        .unwrap();
    endpoint.batch_end().unwrap();

    let consumer = ClientConfig::new()
        .set("bootstrap.servers", default_redpanda_server())
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("group.id", format!("{output_topic}_reader"))
        .set("isolation.level", "read_committed")
        .create::<BaseConsumer>()
        .unwrap();
    consumer.subscribe(&[output_topic]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let message = loop {
        assert!(
            Instant::now() < deadline,
            "no message in {output_topic} before timing out"
        );
        if let Some(message) = read_keyed_message(&consumer) {
            break message;
        }
    };
    assert_eq!(message.2, b"kept".to_vec());
    let position: serde_json::Value = serde_json::from_slice(&message.3).unwrap();
    assert_eq!(position, json!({"transaction": 0, "substep": 0}));

    // Nothing else was written.
    let extra = (0..20).find_map(|_| read_keyed_message(&consumer));
    assert_eq!(extra, None, "unexpected extra message");
}

/// Returns the payloads of the committed messages that survive in partition
/// 0 of `topic`, reading from the start every time, so that it reflects what
/// compaction has removed so far.
fn surviving_payloads(topic: &str) -> Vec<String> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", default_redpanda_server())
        .set("group.id", format!("{topic}_scan"))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        .create::<BaseConsumer>()
        .unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment
        .add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&assignment).unwrap();

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut payloads = Vec::new();
    loop {
        assert!(Instant::now() < deadline, "timed out scanning {topic}");
        match consumer.poll(Duration::from_millis(100)) {
            None => continue,
            Some(Ok(message)) => payloads
                .push(String::from_utf8(message.payload().unwrap_or_default().to_vec()).unwrap()),
            Some(Err(rdkafka::error::KafkaError::PartitionEOF(_))) => return payloads,
            Some(Err(error)) => panic!("error scanning {topic}: {error}"),
        }
    }
}

/// Waits until the message with payload `removed` no longer survives in
/// partition 0 of `topic`, which shows that compaction ran, and returns the
/// payloads that do survive.  Panics if it is still there after `timeout`:
/// a compaction test whose topic was never compacted proves nothing.
fn wait_for_compaction(topic: &str, removed: &str, timeout: Duration) -> Vec<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let payloads = surviving_payloads(topic);
        if !payloads.iter().any(|payload| payload == removed) {
            info!("payloads surviving compaction in {topic}: {payloads:?}");
            return payloads;
        }
        assert!(
            Instant::now() < deadline,
            "compaction did not remove {removed:?} from {topic} within {timeout:?}; the broker's compaction settings may be too slow for this test: {payloads:?}"
        );
        sleep(Duration::from_secs(2));
    }
}

/// Creates a compacted topic whose segments roll after a message or two, so
/// that the broker's cleaner compacts closed segments as soon as it runs.
fn create_compacted_topic(topic: &str) -> KafkaResources {
    KafkaResources::create_topics_with_config(
        &[(topic, 1)],
        &[
            ("cleanup.policy", "compact"),
            // Redpanda never makes a partition ready with segments smaller
            // than one record batch, such as 1 byte.
            ("segment.bytes", "1024"),
            ("min.cleanable.dirty.ratio", "0.01"),
            ("min.compaction.lag.ms", "0"),
        ],
    )
}

/// Creates and connects a fault-tolerant Kafka output endpoint for `topic`.
fn new_ft_output_endpoint(topic: &str) -> Box<dyn OutputEndpoint> {
    let config = serde_json::from_value(json!({
      "name": "kafka_output",
      "config": { "topic": topic }
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
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();
    endpoint
}

/// Reads the committed payloads in `topic` that start with `prefix`, waiting
/// until at least one arrives and then for any stragglers, so that an extra
/// replayed transaction is caught too.
fn read_payloads_with_prefix(topic: &str, prefix: &str) -> Vec<String> {
    let consumer = ClientConfig::new()
        .set("bootstrap.servers", default_redpanda_server())
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("group.id", format!("{topic}_reader"))
        .set("isolation.level", "read_committed")
        .create::<BaseConsumer>()
        .unwrap();
    consumer.subscribe(&[topic]).unwrap();
    let mut payloads = Vec::new();
    let mut idle_polls = 0;
    let deadline = Instant::now() + Duration::from_secs(60);
    while payloads.is_empty() || idle_polls < 20 {
        assert!(
            Instant::now() < deadline,
            "no payload starting with {prefix:?} appeared in {topic}"
        );
        match consumer.poll(Duration::from_millis(100)) {
            None => idle_polls += 1,
            Some(message) => {
                let payload =
                    String::from_utf8(message.unwrap().payload().unwrap().to_vec()).unwrap();
                if payload.starts_with(prefix) {
                    payloads.push(payload);
                }
                idle_polls = 0;
            }
        }
    }
    payloads
}

/// Verifies that keyed exactly-once output recovers correctly from a
/// compacted topic.  Keyed messages repeat keys, so compaction removes older
/// messages for each key, but it keeps the newest message for each key, and
/// the newest message in the partition is the newest for its own key, so
/// recovery still reads the last position.
#[test]
fn output_keyed_compacted_test() {
    init_test_logger();
    let output_topic = "ft_output_keyed_compacted_test_output_topic";
    let _kafka_resources = create_compacted_topic(output_topic);

    // Write `key-a` in transactions 0, 1 and 2, so that compaction can remove
    // the first two, then `key-b` in transaction 3, so that `key-a`'s last
    // message is in a closed segment too.
    let mut endpoint = new_ft_output_endpoint(output_topic);
    for (transaction, key) in [(0, "key-a"), (1, "key-a"), (2, "key-a"), (3, "key-b")] {
        endpoint
            .batch_start(transaction, OutputBatchType::Delta)
            .unwrap();
        endpoint
            .push_key(
                Some(key.as_bytes()),
                Some(format!("{key} in transaction {transaction}").as_bytes()),
                &[],
            )
            .unwrap();
        endpoint.batch_end().unwrap();
    }
    drop(endpoint);

    // Compaction removes `key-a`'s older messages and keeps its newest.
    let survivors = wait_for_compaction(
        output_topic,
        "key-a in transaction 1",
        Duration::from_secs(300),
    );
    assert!(
        !survivors.contains(&"key-a in transaction 0".to_string()),
        "{survivors:?}"
    );
    assert!(
        survivors.contains(&"key-a in transaction 2".to_string()),
        "{survivors:?}"
    );

    // Replay transactions 0 through 3, then write transaction 4.  Only
    // transaction 4 should appear, which means recovery read transaction 3
    // even after compaction.
    let mut endpoint = new_ft_output_endpoint(output_topic);
    for transaction in 0..5 {
        endpoint
            .batch_start(transaction, OutputBatchType::Delta)
            .unwrap();
        endpoint
            .push_key(
                Some(b"key-c".as_slice()),
                Some(format!("key-c in transaction {transaction}").as_bytes()),
                &[],
            )
            .unwrap();
        endpoint.batch_end().unwrap();
    }
    assert_eq!(
        read_payloads_with_prefix(output_topic, "key-c"),
        vec!["key-c in transaction 4"]
    );
}

/// Verifies that keyless exactly-once output still recovers correctly from a
/// compacted topic: every keyless message has a unique key, so compaction
/// removes none of them.  This is how the connector behaved before keyed
/// output was supported, and it must not change.
#[test]
fn output_keyless_compacted_test() {
    init_test_logger();
    let output_topic = "ft_output_keyless_compacted_test_output_topic";
    let _kafka_resources = create_compacted_topic(output_topic);

    // Two keyed messages with the same key come first, as a control:
    // compaction removing the older one shows that the cleaner compacted this
    // partition.  They are written by the connector, so that every message in
    // the topic carries a position that recovery can read.
    let mut endpoint = new_ft_output_endpoint(output_topic);
    for (transaction, payload) in [(0, "control 1"), (1, "control 2")] {
        endpoint
            .batch_start(transaction, OutputBatchType::Delta)
            .unwrap();
        endpoint
            .push_key(Some(b"control"), Some(payload.as_bytes()), &[])
            .unwrap();
        endpoint.batch_end().unwrap();
    }
    for transaction in 2..6 {
        endpoint
            .batch_start(transaction, OutputBatchType::Delta)
            .unwrap();
        endpoint
            .push_buffer(format!("first run, transaction {transaction}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
    drop(endpoint);

    // Compaction removes the first control message and none of the four
    // keyless messages, because each has its own key.
    let survivors = wait_for_compaction(output_topic, "control 1", Duration::from_secs(300));
    for transaction in 2..6 {
        let payload = format!("first run, transaction {transaction}");
        assert!(
            survivors.contains(&payload),
            "{payload:?} not in {survivors:?}"
        );
    }

    // Replay transactions 0 through 5, then write transaction 6.  Only
    // transaction 6 should appear, which means recovery read transaction 5
    // after compaction.
    let mut endpoint = new_ft_output_endpoint(output_topic);
    for transaction in 0..7 {
        endpoint
            .batch_start(transaction, OutputBatchType::Delta)
            .unwrap();
        endpoint
            .push_buffer(format!("second run, transaction {transaction}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
    assert_eq!(
        read_payloads_with_prefix(output_topic, "second run"),
        vec!["second run, transaction 6"]
    );
}

/// A Kafka message header, where a `None` value is a null value.
#[cfg_attr(not(test), allow(dead_code))]
type OutputHeader = (String, Option<Vec<u8>>);

/// A Kafka message's payload and headers.
#[cfg_attr(not(test), allow(dead_code))]
type OutputMessage = (Vec<u8>, Vec<OutputHeader>);

/// Reads the first `n` messages in `topic`, returning each message's payload
/// and headers in the order that they were produced.
#[cfg(test)]
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
    endpoint.connect(Arc::new(MockOutputCallbacks)).unwrap();
    for step in 0..5 {
        endpoint.batch_start(step, OutputBatchType::Delta).unwrap();
        endpoint
            .push_buffer(format!("string{step}").as_bytes())
            .unwrap();
        endpoint.batch_end().unwrap();
    }
}

/// The table, and view, of most cases.
pub fn relation() -> Relation {
    relations::basic("kafka_rows")
}

/// A table whose columns other than `data` take their values from the Kafka connector
/// attributes of the same names, as [`raw_metadata_options`] declares.
pub fn raw_metadata_relation() -> Relation {
    Relation::new(
        "kafka_raw_metadata".into(),
        vec![
            Field::new("data".into(), ColumnType::varchar(false)),
            Field::new("kafka_headers".into(), ColumnType::variant(false)),
            Field::new("kafka_topic".into(), ColumnType::varchar(false)),
            Field::new("kafka_timestamp".into(), ColumnType::timestamp(false)),
            Field::new("kafka_partition".into(), ColumnType::int(false)),
            Field::new("kafka_offset".into(), ColumnType::bigint(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The input options of [`raw_metadata_relation`]: each Kafka column's DEFAULT reads the
/// attribute of the same name.
pub fn raw_metadata_options() -> InputOptions {
    json_parser::metadata_options()
}

/// Returns rows of [`relation`]: `(id, b, i, s)`, each inserted once.
fn rows(rows: &[(i64, bool, Option<i64>, &str)]) -> Updates {
    let rows: Vec<String> = rows
        .iter()
        .map(|(id, b, i, s)| json!({"id": id, "b": b, "i": i, "s": s}).to_string())
        .collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    Updates::from_json(published_schema(&relation()), &rows).unwrap()
}

/// Returns the rows of [`relation`] with `ids`, each with `b` false, `i` NULL, and `s`
/// empty.
fn for_ids(ids: Range<usize>) -> Updates {
    let rows: Vec<(i64, bool, Option<i64>, &str)> =
        ids.map(|id| (id as i64, false, None, "")).collect();
    self::rows(&rows)
}

/// Returns each batch of `batches` as one message of CSV records.
fn csv(batches: &[Updates]) -> Vec<Vec<u8>> {
    batches
        .iter()
        .map(|batch| render::csv(&relation(), batch.rows()))
        .collect()
}

/// Returns the rows of `batches`, in order.
fn concat(batches: &[Updates]) -> Updates {
    Updates::concat(published_schema(&relation()), batches).unwrap()
}

/// Returns each row of `batches` as a batch of its own.
fn singletons(batches: &[Updates]) -> Vec<Updates> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.len() as u32).map(move |index| batch.take(&[index], vec![1]).unwrap())
        })
        .collect()
}

/// Waits for the rows of `expected` to arrive in `collection`, in any order, calling `flush`.
fn check_unordered(
    collection: &dyn feldera_adapterlib::test::TestCollection,
    expected: &Updates,
    flush: impl FnMut(),
) {
    let updates = wait_for_rows(collection, expected.len(), flush);
    assert!(updates.weights().iter().all(|weight| *weight == 1));
    assert_eq!(updates.zset().unwrap(), expected.zset().unwrap());
}

/// A pipeline whose output goes to `topic` of a broker that does not exist does not start: the
/// output endpoint fails to fetch the topic's metadata.
fn check_output_errors(
    engine: &dyn PipelineTestEngine,
    topic: &str,
    inputs: JsonValue,
    start_from: bool,
) {
    init_test_logger();
    let mut transport = json!({
        "bootstrap.servers": "localhost:11111",
        "topic": topic,
    });
    if start_from {
        transport["start_from"] = json!("earliest");
    }
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": inputs,
        "outputs": {
            "test_output": {
                "stream": "test_output1",
                "transport": { "name": "kafka_output", "config": transport },
                "format": { "name": "csv" }
            }
        }
    }))
    .unwrap();
    match engine.controller(
        &TestCircuit::identity(relation()),
        &config,
        Box::new(|error, _| panic!("error: {error}")),
    ) {
        Ok(_) => panic!("the pipeline started"),
        Err(error) => {
            let expected = format!(
                "FATAL error on output endpoint 'test_output': error retrieving metadata for topic '{topic}': Meta data fetch error: "
            );
            assert!(
                error.to_string().starts_with(&expected),
                "unexpected error: {error}"
            );
        }
    }
}

/// A fault-tolerant output connector to a broker that does not exist does not start.
#[engine_test(ignore)]
pub fn output_errors_fault_tolerant(engine: &dyn PipelineTestEngine) {
    check_output_errors(
        engine,
        "ft_end_to_end_test_output_topic",
        JsonValue::Null,
        true,
    );
}

/// An output connector to a broker that does not exist does not start.
#[engine_test]
pub fn output_errors(engine: &dyn PipelineTestEngine) {
    check_output_errors(engine, "end_to_end_test_output_topic", json!({}), false);
}

/// One round of [`check_fault_tolerance`].
#[derive(Clone)]
struct FtRound {
    /// Records to write before the round starts the pipeline.
    n_records: usize,
    /// The pipeline must not deliver the records.
    expect_failure: bool,
    /// Checkpoint at the end of the round.
    do_checkpoint: bool,
    /// Delete the topic and create it empty after the round.
    delete_topics_afterward: bool,
    /// Add a partition to the topic after the round.
    add_partition_afterward: bool,
}

impl FtRound {
    fn new(n_records: usize, do_checkpoint: bool) -> Self {
        Self {
            n_records,
            expect_failure: false,
            do_checkpoint,
            delete_topics_afterward: false,
            add_partition_afterward: false,
        }
    }

    fn with_checkpoint(n_records: usize) -> Self {
        Self::new(n_records, true)
    }

    fn without_checkpoint(n_records: usize) -> Self {
        Self::new(n_records, false)
    }

    fn with_deleting_topics_afterward(self) -> Self {
        Self {
            delete_topics_afterward: true,
            ..self
        }
    }

    fn with_failure_expected(self) -> Self {
        Self {
            expect_failure: true,
            ..self
        }
    }

    fn with_adding_partition_afterward(self) -> Self {
        Self {
            add_partition_afterward: true,
            ..self
        }
    }
}

/// Checks fault tolerance in `rounds`. Each round writes its records to `topic`, starts the
/// pipeline, waits for it to deliver the records that the latest checkpoint does not hold,
/// checkpoints if the round asks, stops, and checks the output, which a connector without
/// fault tolerance writes anew on each start.
fn check_fault_tolerance(
    engine: &dyn PipelineTestEngine,
    topic: &str,
    rounds: &[FtRound],
    resume_earliest_if_data_expires: bool,
) {
    init_test_logger();
    let mut n_partitions = 1;
    let mut kafka_resources = KafkaResources::create_topics(&[(topic, n_partitions as i32)]);
    sleep(Duration::from_secs(1));
    let producer = TestProducer::new();
    let tempdir = TempDir::new().unwrap();
    let storage_dir = tempdir.path().join("storage");
    create_dir(&storage_dir).unwrap();
    let output_path = tempdir.path().join("output.csv");

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "storage_config": { "path": storage_dir },
        "storage": true,
        "fault_tolerance": {},
        "clock_resolution_usecs": null,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "kafka_input",
                    "config": {
                        "topic": topic,
                        "start_from": "earliest",
                        "log_level": "debug",
                        "resume_earliest_if_data_expires": resume_earliest_if_data_expires,
                    }
                },
                "format": { "name": "csv" }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": { "name": "file_output", "config": { "path": output_path } },
                "format": { "name": "csv", "config": {} }
            }
        }
    }))
    .unwrap();
    let circuit = TestCircuit::Identity {
        relation: relation(),
        persistent_output_ids: vec![Some("output".to_string())],
        input_properties: Vec::new(),
        materialized: true,
        persistent_ids: true,
        table_version: 0,
        attribute_defaults: BTreeMap::new(),
    };

    // Records written to the topic, and records that the latest checkpoint holds.
    let mut total_records = 0usize;
    let mut checkpointed_records = 0usize;

    for (round, step) in rounds.iter().cloned().enumerate() {
        info!(
            "round {round}: {} records, checkpoint {}, failure expected {}",
            step.n_records, step.do_checkpoint, step.expect_failure
        );
        if step.n_records > 0 {
            let messages = csv(&singletons(&[for_ids(
                total_records..total_records + step.n_records,
            )]));
            producer.send_to_topic(&messages, topic);
            total_records += step.n_records;
        }

        let controller = engine
            .controller(
                &circuit,
                &config,
                Box::new(move |error, _| panic!("error: {error}")),
            )
            .unwrap();
        controller.start();

        // Wait for the records that the checkpoint lacks to be processed or replayed.
        let transmitted = || {
            controller
                .status()
                .output_status()
                .get(&0)
                .unwrap()
                .transmitted_records() as usize
        };
        let result = wait(|| transmitted() >= total_records, 10_000);
        if step.expect_failure {
            result.unwrap_err();
            return;
        }
        result.unwrap();

        // No more records arrive, but give a bug the time to send some.
        sleep(Duration::from_millis(100));
        assert_eq!(transmitted(), total_records);

        if step.do_checkpoint {
            controller.checkpoint().unwrap();
        }
        controller.stop().unwrap();

        // The output connector is not fault-tolerant and truncates its file on each start, so
        // the file holds the records past the checkpoint.
        let output = read::csv_updates(&relation(), &std::fs::read(&output_path).unwrap()).unwrap();
        assert!(output.weights().iter().all(|weight| *weight == 1));
        assert_eq!(
            output.zset().unwrap(),
            for_ids(checkpointed_records..total_records).zset().unwrap()
        );

        if step.do_checkpoint {
            checkpointed_records = total_records;
        }
        if step.delete_topics_afterward {
            drop(kafka_resources);
            kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
        }
        if step.add_partition_afterward {
            n_partitions += 1;
            kafka_resources.add_partition(topic, n_partitions);
        }
    }
}

/// Restarts after checkpoints deliver each record once.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_with_checkpoints(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_with_checkpoints",
        &[
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500),
        ],
        false,
    );
}

/// A topic that disappears and comes back empty after a checkpoint stops a restart from
/// resuming.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_with_topic_expiration(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_with_topic_expiration",
        &[
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500).with_deleting_topics_afterward(),
            FtRound::with_checkpoint(2500).with_failure_expected(),
        ],
        false,
    );
}

/// With `resume_earliest_if_data_expires`, a restart resumes from the start of a topic that
/// disappeared and came back empty.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_with_topic_expiration_and_resume_earliest(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_with_topic_expiration_and_resume_earliest",
        &[
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500).with_deleting_topics_afterward(),
            FtRound::with_checkpoint(2500),
        ],
        true,
    );
}

/// The same as [`ft_with_topic_expiration`], on another topic.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_with_tolerance_topic_expiration(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_with_tolerance_for_topic_expiration",
        &[
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500).with_deleting_topics_afterward(),
            FtRound::with_checkpoint(2500).with_failure_expected(),
        ],
        false,
    );
}

/// Restarts without checkpoints replay every record.
#[engine_test(requires(Storage, FaultTolerance), services(Kafka))]
pub fn ft_without_checkpoints(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_without_checkpoints",
        &[
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(2500),
        ],
        false,
    );
}

/// Rounds with and without checkpoints alternate, starting with a checkpoint.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_alternating(engine: &dyn PipelineTestEngine) {
    let rounds: Vec<FtRound> = (0..10)
        .map(|round| FtRound::new(2500, round % 2 == 0))
        .collect();
    check_fault_tolerance(engine, "ft_alternating", &rounds, false);
}

/// The first round has no records and no checkpoint.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_initially_zero_without_checkpoint(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_initially_zero_without_checkpoint",
        &[
            FtRound::without_checkpoint(0),
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(0),
            FtRound::with_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::with_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::with_checkpoint(2500),
        ],
        false,
    );
}

/// The first round has no records and a checkpoint.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_initially_zero_with_checkpoint(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_initially_zero_with_checkpoint",
        &[
            FtRound::with_checkpoint(0),
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(0),
            FtRound::with_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::with_checkpoint(2500),
            FtRound::without_checkpoint(2500),
            FtRound::with_checkpoint(2500),
        ],
        false,
    );
}

/// Partitions added to the topic between restarts are read too.
#[engine_test(requires(Storage, Checkpoints, FaultTolerance), services(Kafka))]
pub fn ft_with_adding_partitions(engine: &dyn PipelineTestEngine) {
    check_fault_tolerance(
        engine,
        "ft_with_adding_partitions",
        &[
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(2500).with_adding_partition_afterward(),
            FtRound::without_checkpoint(2500),
            FtRound::without_checkpoint(2500).with_adding_partition_afterward(),
            FtRound::without_checkpoint(2500),
            FtRound::with_checkpoint(2500),
            FtRound::with_checkpoint(2500).with_adding_partition_afterward(),
            FtRound::with_checkpoint(2500),
        ],
        false,
    );
}

/// Returns the configuration of a Kafka input endpoint of `topic` that starts from
/// `start_from` and reads `partitions`, if any, with CSV records.
fn start_from_config(
    topic: &str,
    start_from: KafkaStartFromConfig,
    partitions: Option<Vec<i32>>,
) -> InputEndpointConfig {
    let mut kafka_options = BTreeMap::new();
    let auto_offset_reset = match start_from {
        KafkaStartFromConfig::Earliest => Some("earliest"),
        KafkaStartFromConfig::Latest => Some("latest"),
        KafkaStartFromConfig::Offsets(_) | KafkaStartFromConfig::Timestamp(_) => None,
    };
    if let Some(auto_offset_reset) = auto_offset_reset {
        kafka_options.insert("auto.offset.reset".into(), auto_offset_reset.into());
    }
    kafka_options.insert("bootstrap.servers".into(), default_redpanda_server());
    kafka_options.insert("group.id".into(), "test-client".into());
    InputEndpointConfig::new(
        "test_input",
        ConnectorConfig::new(
            TransportConfig::KafkaInput(KafkaInputConfig {
                log_level: Some(KafkaLogLevel::Debug),
                start_from,
                partitions,
                ..KafkaInputConfig::default(kafka_options, topic)
            }),
            Some(FormatConfig {
                name: Cow::from("csv"),
                config: json!({}),
            }),
        ),
    )
}

/// Writes `data`, one CSV message per batch, each followed by an empty message, to a topic of
/// `partitions` partitions, before or after the endpoint starts, as `start_from` needs; checks
/// that the endpoint reads the records past where it starts, or reports the error of a start
/// that does not exist; then checks that a disconnected endpoint reads nothing.
fn check_start_from(
    engine: &dyn ConnectorTestEngine,
    data: Vec<Updates>,
    topic: &str,
    partitions: i32,
    start_from: KafkaStartFromConfig,
) {
    let _kafka = KafkaResources::create_topics(&[(topic, partitions)]);
    let producer = TestProducer::new();
    let send = |batches: &[Updates]| {
        producer.send_to_topic(&csv(batches), topic);
        producer.send_string("", topic);
    };
    let (expected, send_before, mut send_after) = match &start_from {
        KafkaStartFromConfig::Earliest => (Some(data.as_slice()), Some(data.as_slice()), None),
        KafkaStartFromConfig::Latest => (Some(data.as_slice()), None, Some(data.as_slice())),
        KafkaStartFromConfig::Offsets(offsets) => {
            let offset = offsets[0] as usize;
            (data.get(offset..), data.get(..offset), data.get(offset..))
        }
        KafkaStartFromConfig::Timestamp(_) => {
            let half = data.len() / 2;
            (
                Some(&data[half..]),
                Some(&data[..half]),
                Some(&data[half..]),
            )
        }
    };
    if let Some(send_before) = send_before {
        send(send_before);
    }
    let start_from = match start_from {
        // Start from a time between the two halves.
        KafkaStartFromConfig::Timestamp(_) => {
            sleep(Duration::from_secs(2));
            let timestamp = KafkaStartFromConfig::Timestamp(Timestamp::now().to_millis().unwrap());
            sleep(Duration::from_secs(2));
            if let Some(send_after) = send_after.take() {
                send(send_after);
            }
            timestamp
        }
        other => other,
    };

    let pipeline = input_pipeline(
        engine,
        &relation(),
        &InputOptions::default(),
        start_from_config(topic, start_from, None),
    )
    .unwrap();
    if expected.is_none() {
        pipeline.consumer.on_error(Some(Box::new(|_, _| ())));
    }
    pipeline.reader.extend();
    if let Some(send_after) = send_after {
        send(send_after);
    }

    let flush = || pipeline.reader.queue(false);
    match expected {
        Some(expected) => check_unordered(pipeline.collection.as_ref(), &concat(expected), flush),
        None => {
            sleep(Duration::from_millis(1000));
            let error = pipeline
                .consumer
                .get_error()
                .expect("the connector reports an error");
            panic!("{error}");
        }
    }
    pipeline.collection.reset();

    // A disconnected endpoint reads nothing.
    pipeline.reader.disconnect();
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(pipeline.collection.len(), 0);
}

/// Two batches of rows.
fn testdata() -> Vec<Updates> {
    vec![
        rows(&[(0, true, Some(0), "0")]),
        rows(&[
            (1, true, Some(1), "1"),
            (2, true, Some(2), "2"),
            (3, true, Some(3), "3"),
        ]),
    ]
}

/// Fifty batches of one row.
fn testdata2() -> Vec<Updates> {
    (0..50)
        .map(|id| rows(&[(id, id >= 25, Some(id), &id.to_string())]))
        .collect()
}

/// An endpoint that starts from an offset reads the messages from that offset.
#[engine_test(services(Kafka))]
pub fn input_offset(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        testdata(),
        "test_kafka_input_offset",
        1,
        KafkaStartFromConfig::Offsets(vec![1]),
    );
}

/// An endpoint that starts from a time reads the messages after that time.
#[engine_test(services(Kafka))]
pub fn input_timestamp(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        testdata2(),
        "test_kafka_input_timestamp",
        1,
        KafkaStartFromConfig::Timestamp(0),
    );
}

/// An endpoint that starts from an offset past the end of the partition reports it.
#[engine_test(
    should_panic = "provided offset '3' not currently in partition '0'",
    services(Kafka)
)]
pub fn input_offset_doesnt_exist(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        Vec::new(),
        "test_kafka_input_offset_doesnt_exist",
        1,
        KafkaStartFromConfig::Offsets(vec![3]),
    );
}

/// The same as [`input_offset_doesnt_exist`], with three partitions.
#[engine_test(
    should_panic = "provided offset '3' not currently in partition '0'",
    services(Kafka)
)]
pub fn input_offset_doesnt_exist_2(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        Vec::new(),
        "test_kafka_input_offset_doesnt_exist_2",
        3,
        KafkaStartFromConfig::Offsets(vec![3, 4, 5]),
    );
}

/// An endpoint that starts from the earliest message reads every message.
#[engine_test(services(Kafka))]
pub fn input_offset_earliest(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        testdata(),
        "test_kafka_input_offset_earliest",
        1,
        KafkaStartFromConfig::Earliest,
    );
}

/// The same as [`input_offset_earliest`], with two partitions.
#[engine_test(services(Kafka))]
pub fn input_offset_earliest_2(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        testdata(),
        "test_kafka_input_offset_earliest_2",
        2,
        KafkaStartFromConfig::Earliest,
    );
}

/// An endpoint that starts from the latest message reads the messages sent after it starts.
#[engine_test(services(Kafka))]
pub fn input_offset_latest(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        testdata(),
        "test_kafka_input_offset_latest",
        1,
        KafkaStartFromConfig::Latest,
    );
}

/// The same as [`input_offset_latest`], with two partitions.
#[engine_test(services(Kafka))]
pub fn input_offset_latest_2(engine: &dyn ConnectorTestEngine) {
    check_start_from(
        engine,
        testdata(),
        "test_kafka_input_offset_latest_2",
        2,
        KafkaStartFromConfig::Latest,
    );
}

/// Writes `data`, the batch of each of `partitions` in turn, one message per row, and `extra`
/// to the other partitions, to a topic of `n_partitions` partitions; checks that an endpoint
/// of `partitions` that starts from `start_from` reads its rows past where it starts and
/// none of the others; then checks that a disconnected endpoint reads nothing.
fn check_partitions(
    engine: &dyn ConnectorTestEngine,
    topic: &str,
    n_partitions: i32,
    partitions: Vec<i32>,
    start_from: KafkaStartFromConfig,
    data: Vec<Updates>,
    extra: Vec<Updates>,
) {
    let _kafka = KafkaResources::create_topics(&[(topic, n_partitions)]);
    let producer = TestProducer::new();
    let expected: Vec<Updates> = match &start_from {
        KafkaStartFromConfig::Earliest | KafkaStartFromConfig::Latest => data.clone(),
        KafkaStartFromConfig::Offsets(offsets) => data
            .iter()
            .zip(offsets)
            .map(|(batch, offset)| {
                let indices: Vec<u32> = (*offset as u32..batch.len() as u32).collect();
                let weights = vec![1; indices.len()];
                batch.take(&indices, weights).unwrap()
            })
            .collect(),
        KafkaStartFromConfig::Timestamp(_) => unimplemented!(),
    };
    let load = || {
        for (batch, partition) in data.iter().zip(&partitions) {
            for message in csv(&singletons(std::slice::from_ref(batch))) {
                producer.send_to_topic_partition(&[message], topic, *partition);
                producer.send_string_partition("", topic, *partition);
            }
        }
        for (partition, batch) in (0..n_partitions)
            .filter(|partition| !partitions.contains(partition))
            .zip(&extra)
        {
            for message in csv(&singletons(std::slice::from_ref(batch))) {
                producer.send_to_topic_partition(&[message], topic, partition);
                producer.send_string_partition("", topic, partition);
            }
        }
    };
    if start_from != KafkaStartFromConfig::Latest {
        load();
    }
    let pipeline = input_pipeline(
        engine,
        &relation(),
        &InputOptions::default(),
        start_from_config(topic, start_from.clone(), Some(partitions.clone())),
    )
    .unwrap();
    pipeline.reader.extend();
    if start_from == KafkaStartFromConfig::Latest {
        load();
    }

    let flush = || pipeline.reader.queue(false);
    check_unordered(pipeline.collection.as_ref(), &concat(&expected), flush);
    pipeline.collection.reset();

    pipeline.reader.disconnect();
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(pipeline.collection.len(), 0);
}

/// One row of each of three partitions.
fn partition_data() -> Vec<Updates> {
    vec![
        rows(&[(0, false, Some(0), "0")]),
        rows(&[(10, true, Some(10), "10")]),
        rows(&[(20, false, Some(20), "20")]),
    ]
}

/// One row of each partition that the endpoint does not read.
fn partition_extra() -> Vec<Updates> {
    vec![
        rows(&[(100, false, Some(100), "100-extra")]),
        rows(&[(200, true, Some(200), "200-extra")]),
        rows(&[(300, false, Some(300), "300-extra")]),
    ]
}

/// An endpoint of some partitions that starts from the latest message reads the messages of
/// those partitions sent after it starts.
#[engine_test(services(Kafka))]
pub fn input_partitions_latest(engine: &dyn ConnectorTestEngine) {
    check_partitions(
        engine,
        "test_input_partitions0",
        5,
        vec![1, 2, 4],
        KafkaStartFromConfig::Latest,
        partition_data(),
        partition_extra(),
    );
}

/// An endpoint of some partitions that starts from the earliest message reads every message
/// of those partitions.
#[engine_test(services(Kafka))]
pub fn input_partitions_earliest(engine: &dyn ConnectorTestEngine) {
    check_partitions(
        engine,
        "test_input_partitions1",
        5,
        vec![1, 2, 4],
        KafkaStartFromConfig::Earliest,
        partition_data(),
        partition_extra(),
    );
}

/// An endpoint of some partitions that starts from an offset in each reads the messages of
/// those partitions from those offsets.
#[engine_test(services(Kafka))]
pub fn input_partitions_offsets(engine: &dyn ConnectorTestEngine) {
    check_partitions(
        engine,
        "test_input_partitions2",
        5,
        vec![2, 4],
        KafkaStartFromConfig::Offsets(vec![1, 1]),
        vec![
            rows(&[
                (0, false, Some(0), "0"),
                (1, false, Some(1), "1"),
                (2, false, Some(2), "2"),
            ]),
            rows(&[
                (10, true, Some(10), "10"),
                (11, true, Some(11), "11"),
                (12, true, Some(12), "12"),
                (13, true, Some(13), "13"),
            ]),
            rows(&[
                (20, false, Some(20), "20"),
                (21, false, Some(21), "21"),
                (22, false, Some(22), "22"),
                (23, false, Some(23), "23"),
                (24, false, Some(24), "24"),
            ]),
        ],
        partition_extra(),
    );
}

/// Checks that an endpoint to a broker that does not exist, or of a topic that does not
/// exist, does not open; then that an endpoint with `poller_threads` pollers reads `data`,
/// in order with one poller; then that a disconnected endpoint reads nothing.
fn check_input(
    engine: &dyn ConnectorTestEngine,
    data: Vec<Updates>,
    topic: &str,
    poller_threads: usize,
) {
    init_test_logger();
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    let open = |transport: JsonValue, max_batch_size: Option<usize>| {
        let mut config = json!({
            "stream": "test_input",
            "transport": { "name": "kafka_input", "config": transport },
            "format": { "name": "csv" }
        });
        if let Some(max_batch_size) = max_batch_size {
            config["max_batch_size"] = json!(max_batch_size);
        }
        input_pipeline(
            engine,
            &relation(),
            &InputOptions::default(),
            serde_json::from_value(config).unwrap(),
        )
    };

    assert!(
        open(
            json!({
                "topic": topic,
                "log_level": "debug",
                "bootstrap.servers": "localhost:11111",
                "auto.offset.reset": "earliest"
            }),
            None
        )
        .is_err(),
        "an endpoint to a broker that does not exist opened"
    );
    assert!(
        open(
            json!({
                "topic": "this_topic_does_not_exist",
                "log_level": "debug",
                "auto.offset.reset": "earliest"
            }),
            None
        )
        .is_err(),
        "an endpoint of a topic that does not exist opened"
    );

    // `auto.offset.reset: earliest` makes the consumer see every message, even those sent
    // before the group rebalances.
    let pipeline = open(
        json!({
            "topic": topic,
            "log_level": "debug",
            "poller_threads": poller_threads,
            "auto.offset.reset": "earliest"
        }),
        Some(10_000_000),
    )
    .unwrap();
    pipeline.reader.extend();
    let producer = TestProducer::new();
    producer.send_to_topic(&csv(&data), topic);

    let expected = concat(&data);
    let flush = || pipeline.reader.queue(false);
    if poller_threads == 1 {
        // One poller keeps the order of the messages.
        let updates = wait_for_rows(pipeline.collection.as_ref(), expected.len(), flush);
        assert_eq!(updates, expected);
    } else {
        check_unordered(pipeline.collection.as_ref(), &expected, flush);
    }
    pipeline.collection.reset();

    pipeline.reader.disconnect();
    sleep(Duration::from_millis(1000));
    flush();
    producer.send_to_topic(&csv(&data), topic);
    sleep(Duration::from_millis(1000));
    flush();
    assert_eq!(pipeline.collection.len(), 0);
}

/// If the broker is down, the Kafka cases fail here first, and fail fast.
#[engine_test(services(Kafka))]
pub fn input_trivial(engine: &dyn ConnectorTestEngine) {
    check_input(engine, Vec::new(), "trivial_test_topic", 1);
}

/// The same as [`input_trivial`], with three pollers.
#[engine_test(services(Kafka))]
pub fn input_trivial_threaded(engine: &dyn ConnectorTestEngine) {
    check_input(engine, Vec::new(), "threaded_trivial_test_topic1", 3);
}

/// Random batches arrive in order through one poller.
#[engine_test(services(Kafka))]
pub fn random_input(engine: &dyn ConnectorTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=100, 1000))| {
        check_input(engine, data, "input_test_topic1", 1);
    });
}

/// Random batches arrive through three pollers.
#[engine_test(services(Kafka))]
pub fn random_input_threaded(engine: &dyn ConnectorTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=100, 1000))| {
        check_input(engine, data, "threaded_test_topic1", 3);
    });
}

/// An output connector with a buffer sends records in groups of the buffer's size, and sends
/// the rest when the buffer's time runs out; every message carries the configured headers.
#[engine_test(services(Kafka))]
pub fn buffer(engine: &dyn PipelineTestEngine) {
    init_test_logger();
    let input_topic = "buffer_test_input_topic";
    let output_topic = "buffer_test_output_topic";
    // `records` is a multiple of `buffer_size`.
    let records = 1_000;
    let buffer_size = 100;
    // Much longer than the pipeline takes to process `buffer_size` records.
    let buffer_timeout_ms = 10_000;
    let _kafka_resources = KafkaResources::create_topics(&[(input_topic, 1), (output_topic, 1)]);

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "kafka_input",
                    "config": {
                        "auto.offset.reset": "earliest",
                        "group.instance.id": "buffer_test",
                        "topics": [input_topic],
                        "log_level": "debug"
                    }
                },
                "format": { "name": "csv" }
            }
        },
        "outputs": {
            "test_output2": {
                "stream": "test_output1",
                "transport": {
                    "name": "kafka_output",
                    "config": {
                        "topic": output_topic,
                        "message.max.bytes": "1000000",
                        "headers": [
                            { "key": "header1", "value": "foobar" },
                            { "key": "header2", "value": [1, 2, 3, 4, 5] }
                        ]
                    }
                },
                "format": { "name": "csv", "config": {} },
                "enable_output_buffer": true,
                "max_output_buffer_size_records": buffer_size,
                "max_output_buffer_time_millis": buffer_timeout_ms
            }
        }
    }))
    .unwrap();

    // Once the topics are gone, endpoint threads may still report errors, which must not
    // fail the case.
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let controller = engine
        .controller(
            &TestCircuit::identity(relation()),
            &config,
            Box::new(move |error, _| {
                if running_clone.load(Ordering::Acquire) {
                    panic!("buffer: error: {error}")
                } else {
                    info!("buffer: error after the topics are gone: {error}")
                }
            }),
        )
        .unwrap();

    let check_headers = Box::new(|message: &BorrowedMessage| {
        let headers = message.headers().unwrap();
        assert_eq!(headers.count(), 2);
        assert_eq!(
            headers.try_get(0).unwrap(),
            Header {
                key: "header1",
                value: Some(b"foobar".as_slice())
            }
        );
        assert_eq!(
            headers.try_get(1).unwrap(),
            Header {
                key: "header2",
                value: Some([1u8, 2, 3, 4, 5].as_slice())
            }
        );
    });
    let consumer = BufferConsumer::new(
        engine,
        &relation(),
        output_topic,
        "csv",
        json!({}),
        Some(check_headers),
    );
    let producer = TestProducer::new();
    controller.start();

    // Send one record more than `records`, which stays in the buffer until the timeout.
    let mut buffered = Vec::new();
    for id in 0..=records {
        let row = rows(&[(id, false, None, "foo")]);
        producer.send_to_topic(&csv(std::slice::from_ref(&row)), input_topic);
        buffered.push(row);
        if buffered.len() >= buffer_size {
            consumer.wait_for_output_unordered(&std::mem::take(&mut buffered));
            consumer.clear();
        } else {
            assert_eq!(consumer.len(), 0);
        }
    }
    consumer.wait_for_output_unordered(&std::mem::take(&mut buffered));
    consumer.clear();
    drop(consumer);
    controller.stop().unwrap();
    running.store(false, Ordering::Release);
}

/// Checks a pipeline that reads CSV `data` from an input topic and writes it in `format`
/// with `format_config` to an output topic, in messages of at most `message_max_bytes`.
fn check_end_to_end(
    engine: &dyn PipelineTestEngine,
    test_name: &str,
    format: &str,
    format_config: JsonValue,
    message_max_bytes: usize,
    data: Vec<Updates>,
) {
    init_test_logger();
    let input_topic = format!("{test_name}_input_topic");
    let output_topic = format!("{test_name}_output_topic");
    let _kafka_resources = KafkaResources::create_topics(&[(&input_topic, 1), (&output_topic, 1)]);

    // `auto.offset.reset: earliest` makes the consumer see every message, even those sent
    // before the group rebalances.
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "kafka_input",
                    "config": {
                        "auto.offset.reset": "earliest",
                        "group.instance.id": test_name,
                        "topics": [input_topic],
                        "log_level": "debug"
                    }
                },
                "format": { "name": "csv" }
            }
        },
        "outputs": {
            "test_output2": {
                "stream": "test_output1",
                "transport": {
                    "name": "kafka_output",
                    "config": {
                        "topic": output_topic,
                        "message.max.bytes": message_max_bytes.to_string(),
                    }
                },
                "format": { "name": format, "config": format_config }
            }
        }
    }))
    .unwrap();

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let name = test_name.to_string();
    let controller = engine
        .controller(
            &TestCircuit::identity(relation()),
            &config,
            Box::new(move |error, _| {
                if running_clone.load(Ordering::Acquire) {
                    panic!("{name}: error: {error}")
                } else {
                    info!("{name}: error after the topics are gone: {error}")
                }
            }),
        )
        .unwrap();
    let consumer = BufferConsumer::new(
        engine,
        &relation(),
        &output_topic,
        format,
        format_config,
        None,
    );
    // Dropping the producer discards the messages it has not delivered yet, so it lives until
    // the output arrives.
    let producer = TestProducer::new();
    producer.send_to_topic(&csv(&data), &input_topic);
    controller.start();
    consumer.wait_for_output_unordered(&data);
    drop(consumer);
    controller.stop().unwrap();
    running.store(false, Ordering::Release);
}

/// Random batches go through a pipeline as CSV, in messages up to 1,000,000 bytes.
#[engine_test(services(Kafka))]
pub fn random_end_to_end_csv_large(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_csv_large", "csv", json!({}), 1_000_000, data);
    });
}

/// Random batches go through a pipeline as CSV, in messages up to 1,500 bytes.
#[engine_test(services(Kafka))]
pub fn random_end_to_end_csv_small(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_csv_small", "csv", json!({}), 1500, data);
    });
}

/// Random batches go through a pipeline as JSON, in messages up to 2,048 bytes.
#[engine_test(services(Kafka))]
pub fn random_end_to_end_json_small(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_json_small", "json", json!({}), 2048, data);
    });
}

/// Random batches go through a pipeline as JSON arrays, in messages up to 5,000 bytes.
#[engine_test(services(Kafka))]
pub fn random_end_to_end_json_array_small(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_json_array_small", "json", json!({"array": true}), 5000, data);
    });
}

/// Returns `updates` without its column `kafka_timestamp`, whose value is the time of
/// delivery.
fn without_timestamp(updates: &Updates) -> Updates {
    let schema = updates.rows().schema();
    let columns: Vec<usize> = (0..schema.fields().len())
        .filter(|index| schema.field(*index).name() != "kafka_timestamp")
        .collect();
    Updates::new(
        updates.rows().project(&columns).unwrap(),
        updates.weights().to_vec(),
    )
    .unwrap()
}

/// Checks that an endpoint with `format` that includes every Kafka attribute fills the
/// columns of `relation` that read them, for a message with two headers, one of them without
/// a value, and a message without headers, whose payloads are `payloads`.
fn check_metadata(
    engine: &dyn ConnectorTestEngine,
    topic: &str,
    relation: &Relation,
    options: &InputOptions,
    format: JsonValue,
    payloads: [&[u8]; 2],
    expected: [&str; 2],
) {
    init_test_logger();
    let _kafka_resources = KafkaResources::create_topics(&[(topic, 1)]);
    let config = serde_json::from_value(json!({
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
    .unwrap();
    let pipeline = input_pipeline(engine, relation, options, config).unwrap();
    pipeline.reader.extend();

    let producer = TestProducer::new();
    producer.send_message(
        payloads[0],
        topic,
        Some(BTreeMap::from([
            ("header1".to_string(), Some(b"foobar".to_vec())),
            ("header2".to_string(), None),
        ])),
    );
    producer.send_message(payloads[1], topic, None);

    let updates = wait_for_rows(pipeline.collection.as_ref(), 2, || {
        pipeline.reader.queue(false)
    });
    let expected = Updates::from_json(
        published_schema(relation),
        &[(1, expected[0]), (1, expected[1])],
    )
    .unwrap();
    assert_eq!(without_timestamp(&updates), without_timestamp(&expected));
}

/// The JSON text of the headers of the first message of [`check_metadata`]: a VARIANT map
/// whose binary value is an array of bytes on an engine with
/// [`Capability::VariantBinaryAsArray`], and base64 text on any other.
fn headers_json(engine: &dyn ConnectorTestEngine) -> String {
    let foobar = if engine.has(Capability::VariantBinaryAsArray) {
        "[102,111,111,98,97,114]"
    } else {
        r#""Zm9vYmFy""#
    };
    format!(r#"{{"header1":{foobar},"header2":null}}"#)
}

/// JSON records receive the Kafka attributes, with the headers as a VARIANT map in which a
/// header without a value is NULL.
#[engine_test(services(Kafka))]
pub fn metadata_json(engine: &dyn ConnectorTestEngine) {
    let topic = "kafka_metadata_json_test_topic";
    let row = |i: i64, headers: &str, offset: i64| {
        json!({
            "i": i,
            "kafka_headers": headers,
            "kafka_topic": topic,
            "kafka_timestamp": "1970-01-01T00:00:00",
            "kafka_partition": 0,
            "kafka_offset": offset,
        })
        .to_string()
    };
    let (row0, row1) = (row(0, &headers_json(engine), 0), row(1, "{}", 1));
    check_metadata(
        engine,
        topic,
        &json_parser::metadata_relation(),
        &json_parser::metadata_options(),
        json!({ "name": "json", "config": { "update_format": "raw" } }),
        [br#"{"i": 0}"#, br#"{"i": 1}"#],
        [&row0, &row1],
    );
}

/// Raw messages receive the Kafka attributes.
#[engine_test(requires(RawFormat), services(Kafka))]
pub fn metadata_raw(engine: &dyn ConnectorTestEngine) {
    let topic = "kafka_metadata_raw_test_topic";
    let row = |data: &str, headers: &str, offset: i64| {
        json!({
            "data": data,
            "kafka_headers": headers,
            "kafka_topic": topic,
            "kafka_timestamp": "1970-01-01T00:00:00",
            "kafka_partition": 0,
            "kafka_offset": offset,
        })
        .to_string()
    };
    let (row0, row1) = (row("foo", &headers_json(engine), 0), row("bar", "{}", 1));
    check_metadata(
        engine,
        topic,
        &raw_metadata_relation(),
        &raw_metadata_options(),
        json!({ "name": "raw", "config": { "mode": "blob", "column_name": "data" } }),
        [b"foo", b"bar"],
        [&row0, &row1],
    );
}

/// Tests for distributed connectors, which divide a topic's partitions among
/// the hosts of a multihost pipeline.
#[cfg(test)]
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
            writer.serialize(CsvRecord::for_id(id)).unwrap();
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
#[cfg(test)]
mod end_of_input {
    use super::*;
    use rdkafka::producer::Producer;

    fn produce(producer: &TestProducer, topic: &str, ids: Range<u32>) {
        for id in ids {
            let mut writer = CsvWriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::new());
            writer.serialize(CsvRecord::for_id(id)).unwrap();
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
#[cfg(test)]
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
            writer.serialize(CsvRecord::for_id(id)).unwrap();
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
