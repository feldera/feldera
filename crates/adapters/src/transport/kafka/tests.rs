//! The Kafka connectors: input from where the configuration starts, from chosen partitions,
//! with one or several pollers, and with connector metadata; fault-tolerant input across
//! restarts, topic expiration, and new partitions; output with a buffer and to a broker that
//! does not exist; and pipelines from an input topic to an output topic.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::create_dir;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, Updates, wait, wait_for_rows};
use feldera_types::config::{
    ConnectorConfig, FormatConfig, InputEndpointConfig, PipelineConfig, TransportConfig,
};
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::transport::kafka::{
    KafkaInputConfig, KafkaLogLevel, KafkaStartFromConfig, default_redpanda_server,
};
use proptest::prelude::*;
use rdkafka::message::{BorrowedMessage, Header, Headers};
use rdkafka::{Message, Timestamp};
use serde_json::{Value as JsonValue, json};
use tempfile::TempDir;
use tracing::info;

use crate::test::kafka::{BufferConsumer, KafkaResources, TestProducer};
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::suites::json_parser;
use crate::test::{
    init_test_logger, input_pipeline, published_schema, read, relations, render, rows,
};

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
pub fn output_errors_fault_tolerant(engine: &dyn PipelineTestEngine) {
    check_output_errors(
        engine,
        "ft_end_to_end_test_output_topic",
        JsonValue::Null,
        true,
    );
}

/// An output connector to a broker that does not exist does not start.
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
pub fn ft_alternating(engine: &dyn PipelineTestEngine) {
    let rounds: Vec<FtRound> = (0..10)
        .map(|round| FtRound::new(2500, round % 2 == 0))
        .collect();
    check_fault_tolerance(engine, "ft_alternating", &rounds, false);
}

/// The first round has no records and no checkpoint.
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
pub fn input_trivial(engine: &dyn ConnectorTestEngine) {
    check_input(engine, Vec::new(), "trivial_test_topic", 1);
}

/// The same as [`input_trivial`], with three pollers.
pub fn input_trivial_threaded(engine: &dyn ConnectorTestEngine) {
    check_input(engine, Vec::new(), "threaded_trivial_test_topic1", 3);
}

/// Random batches arrive in order through one poller.
pub fn random_input(engine: &dyn ConnectorTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=100, 1000))| {
        check_input(engine, data, "input_test_topic1", 1);
    });
}

/// Random batches arrive through three pollers.
pub fn random_input_threaded(engine: &dyn ConnectorTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=100, 1000))| {
        check_input(engine, data, "threaded_test_topic1", 3);
    });
}

/// An output connector with a buffer sends records in groups of the buffer's size, and sends
/// the rest when the buffer's time runs out; every message carries the configured headers.
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
pub fn random_end_to_end_csv_large(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_csv_large", "csv", json!({}), 1_000_000, data);
    });
}

/// Random batches go through a pipeline as CSV, in messages up to 1,500 bytes.
pub fn random_end_to_end_csv_small(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_csv_small", "csv", json!({}), 1500, data);
    });
}

/// Random batches go through a pipeline as JSON, in messages up to 2,048 bytes.
pub fn random_end_to_end_json_small(engine: &dyn PipelineTestEngine) {
    proptest!(ProptestConfig::with_cases(2), |(data in rows::batches(&relation(), "id", 0..=30, 1000))| {
        check_end_to_end(engine, "proptest_kafka_end_to_end_json_small", "json", json!({}), 2048, data);
    });
}

/// Random batches go through a pipeline as JSON arrays, in messages up to 5,000 bytes.
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

/// JSON records receive the Kafka attributes, with the headers as a VARIANT map in which a
/// header without a value is NULL.
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
    let (row0, row1) = (
        row(
            0,
            r#"{"header1":[102,111,111,98,97,114],"header2":null}"#,
            0,
        ),
        row(1, "{}", 1),
    );
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
    let (row0, row1) = (
        row(
            "foo",
            r#"{"header1":[102,111,111,98,97,114],"header2":null}"#,
            0,
        ),
        row("bar", "{}", 1),
    );
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
