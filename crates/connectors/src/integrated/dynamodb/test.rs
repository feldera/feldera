//! Tests of the DynamoDB output connector that reach no engine: its metrics, the conversion of
//! items, and the retry loop. The `dynamodb` connector suite covers the connector with an
//! engine.

#[cfg(test)]
use super::health::NoHealthPublisher;
#[cfg(test)]
use super::helpers::write_transact_chunk;
use super::{
    health::RetryHealth,
    helpers::{RetryPolicy, make_client, to_transact_item},
    metrics::DynamoDBOutputMetrics,
    output::{DynamoDBOutputEndpoint, DynamoDBWorker},
};
use crate::{
    Controller,
    catalog::{RecordFormat, SerBatchReader},
    controller::EndpointId,
    format::Encoder,
    test::{
        engine_test,
        pipeline::{PipelineTestEngine, TestCircuit},
        published_schema, read, relations,
    },
};
use aws_sdk_dynamodb::{
    Client,
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
};
use feldera_adapterlib::{
    metrics::ConnectorMetrics,
    test::{ConnectorTestEngine, Updates, wait},
    transport::OutputBatchType,
};
use feldera_storage::tokio::TOKIO;
use feldera_types::{
    config::PipelineConfig,
    program_schema::{ColumnType, Field, Relation, SqlIdentifier},
    size::ConfigSize,
    transport::dynamodb::{DynamoDBWriteMode, DynamoDBWriterConfig},
};
use rand::{Rng, distributions::Alphanumeric};
use serde_json::{Value, json};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::{
    collections::{BTreeMap, HashMap},
    io::Write,
    sync::{Arc, Weak, atomic::Ordering},
    thread::sleep,
    time::Duration,
};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

pub fn key_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("test_idx", false),
        fields: vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("sort".into(), ColumnType::varchar(false)),
        ],
        materialized: false,
        properties: BTreeMap::new(),
        primary_key: None,
    }
}

pub fn value_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("test_view", false),
        fields: vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("sort".into(), ColumnType::varchar(false)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::bigint(true)),
            Field::new("s".into(), ColumnType::varchar(false)),
        ],
        materialized: true,
        properties: BTreeMap::new(),
        primary_key: Some(vec!["id".into(), "sort".into()]),
    }
}

pub fn all_types_key_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("all_types_idx", false),
        fields: vec![Field::new("id".into(), ColumnType::int(false))],
        materialized: false,
        properties: BTreeMap::new(),
        primary_key: None,
    }
}

pub fn all_types_value_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("all_types_view", false),
        fields: vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("boolean_".into(), ColumnType::boolean(false)),
            Field::new("tinyint_".into(), ColumnType::tinyint(false)),
            Field::new("smallint_".into(), ColumnType::smallint(false)),
            Field::new("int_".into(), ColumnType::int(false)),
            Field::new("bigint_".into(), ColumnType::bigint(false)),
            Field::new("decimal_".into(), ColumnType::decimal(38, 10, false)),
            Field::new("float_".into(), ColumnType::real(false)),
            Field::new("double_".into(), ColumnType::double(false)),
            Field::new("varchar_".into(), ColumnType::varchar(false)),
            Field::new("time_".into(), ColumnType::time(false)),
            Field::new("date_".into(), ColumnType::date(false)),
            Field::new("timestamp_".into(), ColumnType::timestamp(false)),
            Field::new("variant_".into(), ColumnType::variant(false)),
            Field::new("uuid_".into(), ColumnType::uuid(false)),
            Field::new("varbinary_".into(), ColumnType::varbinary(false)),
            Field::new(
                "struct_".into(),
                ColumnType::structure(false, &relations::basic_columns()),
            ),
            Field::new(
                "string_array_".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new(
                "struct_array_".into(),
                ColumnType::array(
                    false,
                    ColumnType::structure(false, &relations::basic_columns()),
                ),
            ),
            Field::new(
                "map_".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::structure(false, &relations::basic_columns()),
                ),
            ),
            Field::new("nullable_".into(), ColumnType::bigint(true)),
        ],
        materialized: true,
        properties: BTreeMap::new(),
        primary_key: Some(vec!["id".into()]),
    }
}

fn config(batch_size: usize) -> DynamoDBWriterConfig {
    DynamoDBWriterConfig {
        table: "test_table".to_string(),
        region: "us-east-1".to_string(),
        endpoint_url: None,
        aws_access_key_id: None,
        aws_secret_access_key: None,
        batch_size: Some(batch_size),
        write_mode: DynamoDBWriteMode::Batch,
        max_buffer_size: ConfigSize::from_mib(1),
        max_concurrent_requests: 16,
        threads: 1,
        max_retries: Some(10),
        put_condition_expression: None,
        delete_condition_expression: None,
    }
}

fn dynamodb_region() -> String {
    std::env::var("DYNAMODB_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

fn endpoint_config(
    table: String,
    endpoint_url: Option<String>,
    threads: usize,
) -> DynamoDBWriterConfig {
    let use_static_dummy_credentials = endpoint_url.is_some();
    DynamoDBWriterConfig {
        table,
        region: dynamodb_region(),
        endpoint_url,
        aws_access_key_id: use_static_dummy_credentials.then(|| "dummy".to_string()),
        aws_secret_access_key: use_static_dummy_credentials.then(|| "dummy".to_string()),
        batch_size: None,
        write_mode: DynamoDBWriteMode::Batch,
        max_buffer_size: ConfigSize::from_mib(1),
        max_concurrent_requests: 16,
        threads,
        max_retries: Some(10),
        put_condition_expression: None,
        delete_condition_expression: None,
    }
}

/// Creates a client pointed at a non-existent endpoint. Writes will fail,
/// but encoding tests never await the spawned write tasks.
pub(super) fn dummy_client() -> Client {
    make_client(&DynamoDBWriterConfig {
        endpoint_url: Some("http://localhost:0".to_string()),
        aws_access_key_id: Some("dummy".to_string()),
        aws_secret_access_key: Some("dummy".to_string()),
        ..config(25)
    })
}

fn worker(batch_size: usize) -> DynamoDBWorker {
    DynamoDBWorker::with_client(
        dummy_client(),
        "",
        &config(batch_size),
        &key_relation(),
        &value_relation(),
    )
}

fn worker_with_max_buffer_size(batch_size: usize, max_buffer_size_bytes: usize) -> DynamoDBWorker {
    let mut config = config(batch_size);
    config.max_buffer_size = ConfigSize::from_bytes(max_buffer_size_bytes as u64);
    DynamoDBWorker::with_client(
        dummy_client(),
        "",
        &config,
        &key_relation(),
        &value_relation(),
    )
}

fn worker_with_config(config: DynamoDBWriterConfig) -> DynamoDBWorker {
    DynamoDBWorker::with_client(
        dummy_client(),
        "",
        &config,
        &key_relation(),
        &value_relation(),
    )
}

#[cfg(test)]
pub(super) fn empty_endpoint_with_counter(
    records_written: Arc<std::sync::atomic::AtomicU64>,
) -> DynamoDBOutputEndpoint {
    DynamoDBOutputEndpoint {
        endpoint_id: EndpointId::default(),
        endpoint_name: "test_dynamodb_endpoint".to_string(),
        config: config(25),
        controller: Weak::new(),
        handles: Vec::new(),
        records_written,
        bytes_written: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        rows_since_last_log: 0,
        bytes_since_last_log: 0,
        retries_since_last_log: 0,
        batches_since_last_log: 0,
        last_throughput_log: std::time::Instant::now(),
    }
}

pub(super) fn dynamodb_metric(metrics: &DynamoDBOutputMetrics, name: &str) -> f64 {
    metrics
        .metrics()
        .into_iter()
        .find_map(|(metric_name, _, _, value)| (metric_name == name).then_some(value))
        .unwrap_or_else(|| panic!("missing DynamoDB metric {name}"))
}

/// Returns `rows` of `relation`, each with its weight, as an output batch of the index `key`.
fn indexed_batch(
    engine: &dyn ConnectorTestEngine,
    key: &Relation,
    relation: &Relation,
    rows: Vec<(Value, i64)>,
) -> Arc<dyn SerBatchReader> {
    let rows: Vec<(i64, String)> = rows
        .into_iter()
        .map(|(row, weight)| (weight, read::arrow_form(relation, row).unwrap().to_string()))
        .collect();
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    let updates = Updates::from_json(published_schema(relation), &rows).unwrap();
    engine
        .indexed_output_batch(key, relation, updates.rows(), updates.weights())
        .unwrap()
}

/// Returns `rows` of [`value_relation`], each with its weight, as an output batch of its index.
fn build_batch(
    engine: &dyn ConnectorTestEngine,
    rows: Vec<(Value, i64)>,
) -> Arc<dyn SerBatchReader> {
    indexed_batch(engine, &key_relation(), &value_relation(), rows)
}

/// Returns `rows` of [`all_types_value_relation`], each with its weight, as an output batch of
/// its index.
fn build_all_types_batch(
    engine: &dyn ConnectorTestEngine,
    rows: Vec<(Value, i64)>,
) -> Arc<dyn SerBatchReader> {
    indexed_batch(
        engine,
        &all_types_key_relation(),
        &all_types_value_relation(),
        rows,
    )
}

/// Returns the row of [`value_relation`] with these columns, in the default JSON flavor.
fn record(id: i32, sort: &str, i: Option<i64>, s: &str) -> Value {
    json!({"id": id, "sort": sort, "b": id % 2 == 0, "i": i, "s": s})
}

/// Returns a row of [`all_types_value_relation`] with a value in every column but
/// `nullable_`, in the default JSON flavor.
fn all_types_record() -> Value {
    json!({
        "id": 42,
        "boolean_": true,
        "tinyint_": -8,
        "smallint_": 16,
        "int_": 32,
        "bigint_": 64,
        "decimal_": 12.345,
        "float_": 1.5,
        "double_": 2.25,
        "varchar_": "hello",
        "time_": "01:02:03.000456",
        "date_": "2024-05-25",
        "timestamp_": "2024-05-25 01:02:03.000456",
        "variant_": "variant-value",
        "uuid_": "550e8400-e29b-41d4-a716-446655440000",
        "varbinary_": [1, 2, 3, 4],
        "struct_": {"id": 5, "b": false, "i": 50, "s": "inside-struct"},
        "string_array_": ["a", "b"],
        "struct_array_": [{"id": 6, "b": true, "i": null, "s": "inside-array"}],
        "map_": {"nested": {"id": 7, "b": true, "i": 70, "s": "inside-map"}},
        "nullable_": null,
    })
}

fn all_types_worker() -> DynamoDBWorker {
    DynamoDBWorker::with_client(
        dummy_client(),
        "",
        &config(25),
        &all_types_key_relation(),
        &all_types_value_relation(),
    )
}

fn encode_batch(worker: &mut DynamoDBWorker, batch: &Arc<dyn SerBatchReader>) {
    let mut cursor = batch.cursor(RecordFormat::DynamoDB).unwrap();
    worker.batch_start_inner();
    worker.encode_cursor(&mut *cursor).unwrap();
    worker.batch_end_inner().unwrap();
}

fn stage_batch(worker: &mut DynamoDBWorker, batch: &Arc<dyn SerBatchReader>) {
    let mut cursor = batch.cursor(RecordFormat::DynamoDB).unwrap();
    worker.batch_start_inner();
    worker.encode_cursor(&mut *cursor).unwrap();
}

fn encode_endpoint_batch(endpoint: &mut DynamoDBOutputEndpoint, batch: &Arc<dyn SerBatchReader>) {
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    endpoint.consumer().batch_end();
}

pub(super) fn attr_s<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
    item.get(field).unwrap().as_s().unwrap()
}

pub(super) fn attr_n<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
    item.get(field).unwrap().as_n().unwrap()
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_insert_as_put_item(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    let batch = build_batch(engine, vec![(record(1, "a", Some(10), "inserted"), 1)]);
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();
    assert_eq!(attr_n(item, "id"), "1");
    assert_eq!(attr_s(item, "sort"), "a");
    assert_eq!(item.get("b").unwrap().as_bool().unwrap(), &false);
    assert_eq!(attr_n(item, "i"), "10");
    assert_eq!(attr_s(item, "s"), "inserted");
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_delete_as_delete_item_key_only(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    let batch = build_batch(engine, vec![(record(2, "b", Some(20), "deleted"), -1)]);
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].put_request().is_none());
    let key = worker.pending[0].delete_request().unwrap().key();
    assert_eq!(key.len(), 2);
    assert_eq!(attr_n(key, "id"), "2");
    assert_eq!(attr_s(key, "sort"), "b");
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_upsert_as_put_item_with_new_value(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    let batch = build_batch(
        engine,
        vec![
            (record(3, "c", Some(30), "old"), -1),
            (record(3, "c", None, "new"), 1),
        ],
    );
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();
    assert_eq!(attr_n(item, "id"), "3");
    assert_eq!(attr_s(item, "sort"), "c");
    assert_eq!(item.get("i").unwrap().as_null().unwrap(), &true);
    assert_eq!(attr_s(item, "s"), "new");
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_stages_until_batch_end(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(100);
    let batch = build_batch(
        engine,
        (0..5)
            .map(|id| (record(id, "chunk", Some(id as i64), "r"), 1))
            .collect(),
    );

    stage_batch(&mut worker, &batch);
    // pending holds all records until flush is called
    assert_eq!(worker.pending.len(), 5);

    let pending = worker.pending.clone();
    worker.flush().unwrap();
    // flush hands the staged records off to the write task, draining `pending`.
    assert!(worker.pending.is_empty());

    let mut ids = pending
        .iter()
        .map(|r| {
            r.put_request()
                .unwrap()
                .item()
                .get("id")
                .unwrap()
                .as_n()
                .unwrap()
                .parse::<i32>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, vec![0, 1, 2, 3, 4]);
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_flushes_when_buffer_limit_is_reached(engine: &dyn ConnectorTestEngine) {
    // max_buffer_size_bytes=1 forces a flush after the first record.
    let mut worker = worker_with_max_buffer_size(25, 1);
    let batch = build_batch(engine, vec![(record(9, "early", Some(90), "flushed"), 1)]);
    stage_batch(&mut worker, &batch);
    // pending is empty because the record was flushed mid-encode.
    // (`records_written` can't be checked here: the dummy client's writes fail,
    // and the counter only advances on a successful write. End-to-end counting
    // is covered by the `dynamodb_progress_counter_*` integration tests.)
    assert!(worker.pending.is_empty());
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_flushes_when_record_threshold_is_reached(engine: &dyn ConnectorTestEngine) {
    // threshold = batch_size(2) × max_concurrent_requests(2) = 4
    let mut config = config(2);
    config.max_concurrent_requests = 2;
    config.max_buffer_size = ConfigSize::MAX;
    let mut worker = worker_with_config(config);
    let batch = build_batch(
        engine,
        (0..4)
            .map(|id| (record(id, "threshold", Some(id as i64), "r"), 1))
            .collect(),
    );
    stage_batch(&mut worker, &batch);
    // Reaching the record threshold drains `pending` via a mid-encode flush.
    assert!(worker.pending.is_empty());
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_pending_never_buffers_whole_batch(engine: &dyn ConnectorTestEngine) {
    // `push_request` flushes every `batch_size` records, so a batch far larger
    // than `batch_size` is never buffered whole in `pending`: only the
    // sub-`batch_size` remainder stays staged until `batch_end`.
    const BATCH_SIZE: usize = 10;
    const RECORDS: i32 = 25;

    let mut config = config(BATCH_SIZE);
    // Enough permits that no flush blocks, and `usize::MAX` bytes so the
    // count-based flush is exercised in isolation from the byte-based one.
    config.max_concurrent_requests = 64;
    config.max_buffer_size = ConfigSize::MAX;
    let mut worker = worker_with_config(config);

    let batch = build_batch(
        engine,
        (0..RECORDS)
            .map(|id| (record(id, "bulk", Some(id as i64), "r"), 1))
            .collect(),
    );
    stage_batch(&mut worker, &batch);

    // The 25 records flush at 10 and 20; only the trailing 5 remain staged, so
    // `pending` holds the remainder rather than the entire batch.
    assert_eq!(worker.pending.len(), RECORDS as usize % BATCH_SIZE);
    assert!(worker.pending.len() < BATCH_SIZE);
    assert!(worker.pending.len() < RECORDS as usize);
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_empty_batch_single_thread(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    encode_batch(&mut worker, &build_batch(engine, Vec::new()));
    assert!(worker.pending.is_empty());
    assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_empty_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    let batch = build_batch(engine, Vec::new());
    for _ in 0..4 {
        let mut worker = worker(25);
        encode_batch(&mut worker, &batch);
        assert!(worker.pending.is_empty());
        assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
    }
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_multiple_batches_insert_upsert_delete(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);

    stage_batch(
        &mut worker,
        &build_batch(
            engine,
            vec![(record(11, "sequence", Some(110), "insert"), 1)],
        ),
    );
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].put_request().is_some());

    stage_batch(
        &mut worker,
        &build_batch(
            engine,
            vec![
                (record(11, "sequence", Some(110), "insert"), -1),
                (record(11, "sequence", Some(111), "upsert"), 1),
            ],
        ),
    );
    assert_eq!(worker.pending.len(), 1);
    assert_eq!(
        attr_s(worker.pending[0].put_request().unwrap().item(), "s"),
        "upsert"
    );

    stage_batch(
        &mut worker,
        &build_batch(
            engine,
            vec![(record(11, "sequence", Some(111), "upsert"), -1)],
        ),
    );
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].delete_request().is_some());
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_non_unique_key_is_skipped_without_failing_batch(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    let batch = build_batch(
        engine,
        vec![
            (record(1, "duplicate", Some(10), "first"), 1),
            (record(1, "duplicate", Some(11), "second"), 1),
            (record(2, "unique", Some(20), "kept"), 1),
        ],
    );

    stage_batch(&mut worker, &batch);

    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();
    assert_eq!(attr_n(item, "id"), "2");
    assert_eq!(attr_s(item, "sort"), "unique");
    assert_eq!(attr_s(item, "s"), "kept");
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_non_unique_key_updates_connector_metric(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    let batch = build_batch(
        engine,
        vec![
            (record(1, "duplicate", Some(10), "first"), 1),
            (record(1, "duplicate", Some(11), "second"), 1),
            (record(2, "unique", Some(20), "kept"), 1),
        ],
    );

    stage_batch(&mut worker, &batch);

    assert_eq!(
        dynamodb_metric(
            &worker.metrics,
            "dynamodb_output_duplicate_keys_skipped_total"
        ),
        1.0
    );
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_drops_oversized_item_keeping_the_rest(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    // The middle record's `s` attribute alone exceeds the 400 KB item-size
    // limit; the other two are ordinary.
    let oversized_value = "x".repeat(410 * 1024);
    let batch = build_batch(
        engine,
        vec![
            (record(1, "a", Some(10), "one"), 1),
            (record(2, "b", Some(20), &oversized_value), 1),
            (record(3, "c", Some(30), "three"), 1),
        ],
    );

    stage_batch(&mut worker, &batch);

    // Only the two normal records are buffered for writing.
    assert_eq!(worker.pending.len(), 2);
    assert_eq!(
        attr_n(worker.pending[0].put_request().unwrap().item(), "id"),
        "1"
    );
    assert_eq!(
        attr_n(worker.pending[1].put_request().unwrap().item(), "id"),
        "3"
    );
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_oversized_item_updates_connector_metric(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    let oversized_value = "x".repeat(410 * 1024);
    let batch = build_batch(
        engine,
        vec![(record(1, "a", Some(10), &oversized_value), 1)],
    );

    stage_batch(&mut worker, &batch);

    assert!(worker.pending.is_empty());
    assert_eq!(
        dynamodb_metric(
            &worker.metrics,
            "dynamodb_output_oversized_items_dropped_total"
        ),
        1.0
    );
}

/// A write that never succeeds must surface as an error rather than being
/// silently swallowed. This drives the full concurrency path against an
/// unreachable endpoint: `batch_end_inner` flushes (spawning a write task on the
/// `JoinSet`), then blocks draining it and aggregates the task's error. With
/// `max_retries = 0` the worker does not retry, so the failure surfaces promptly.
#[engine_test(requires(DynamoDbItems))]
pub fn worker_surfaces_write_failure_at_batch_end(engine: &dyn ConnectorTestEngine) {
    let mut config = config(25);
    config.max_retries = Some(0);
    let mut worker = DynamoDBWorker::with_client(
        dummy_client(),
        "test",
        &config,
        &key_relation(),
        &value_relation(),
    );

    let batch = build_batch(engine, vec![(record(1, "a", Some(10), "one"), 1)]);
    let reader = batch;
    let mut cursor = reader.cursor(RecordFormat::DynamoDB).unwrap();
    worker.batch_start_inner();
    worker.encode_cursor(&mut *cursor).unwrap();

    let error = worker
        .batch_end_inner()
        .expect_err("write to an unreachable endpoint must fail");
    assert!(
        format!("{error:#}").contains("DynamoDB writes failed"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn connector_metrics_accumulate_transact_write_failures() {
    let metrics = DynamoDBOutputMetrics::default();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_transact_write_failures_total"),
        0.0
    );

    metrics.record_transact_write_failure();
    metrics.record_transact_write_failure();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_transact_write_failures_total"),
        2.0
    );
}

#[test]
fn connector_metrics_accumulate_failed_items() {
    let metrics = DynamoDBOutputMetrics::default();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_failed_items_total"),
        0.0
    );

    metrics.record_failed_items(3);
    metrics.record_failed_items(2);

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_failed_items_total"),
        5.0
    );
}

#[test]
fn connector_metrics_accumulate_throttled_items() {
    let metrics = DynamoDBOutputMetrics::default();

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_throttled_items_total"),
        0.0
    );

    metrics.record_throttled_items(10);
    metrics.record_throttled_items(5);

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_throttled_items_total"),
        15.0
    );
}

#[test]
fn connector_metrics_record_semaphore_wait_histogram() {
    let metrics = DynamoDBOutputMetrics::default();

    metrics.record_semaphore_wait(Duration::from_micros(400));
    metrics.record_semaphore_wait(Duration::from_micros(600));

    let histograms = metrics.histograms();
    let wait = histograms
        .iter()
        .find(|h| h.name == "dynamodb_output_semaphore_wait_microseconds")
        .expect("missing semaphore-wait histogram");

    // 400us + 600us recorded; the snapshot sum is in microseconds.
    assert_eq!(wait.snapshot.sum(), 1000);

    let count: u64 = wait
        .snapshot
        .iter_buckets()
        .map(|bucket| bucket.count)
        .sum();
    assert_eq!(count, 2);
}

#[test]
fn connector_metrics_record_write_call_latency_histogram() {
    let metrics = DynamoDBOutputMetrics::default();

    metrics.record_write_call_latency(Duration::from_micros(1_500));
    metrics.record_write_call_latency(Duration::from_micros(2_500));

    let histograms = metrics.histograms();
    let latency = histograms
        .iter()
        .find(|h| h.name == "dynamodb_output_write_call_latency_microseconds")
        .expect("missing write-call latency histogram");

    // 1,500us + 2,500us recorded; the snapshot sum is in microseconds.
    assert_eq!(latency.snapshot.sum(), 4_000);

    let count: u64 = latency
        .snapshot
        .iter_buckets()
        .map(|bucket| bucket.count)
        .sum();
    assert_eq!(count, 2);
}

#[test]
fn encoder_progress_counter_resets_at_batch_start_and_end() {
    let records_written = Arc::new(AtomicU64::new(99));
    let mut endpoint = empty_endpoint_with_counter(records_written.clone());

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written.load(Ordering::Relaxed), 0);

    records_written.store(88, Ordering::Relaxed);
    endpoint.consumer().batch_end();
    assert_eq!(records_written.load(Ordering::Relaxed), 0);
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_all_supported_sql_types(engine: &dyn ConnectorTestEngine) {
    let mut worker = all_types_worker();
    let batch = build_all_types_batch(engine, vec![(all_types_record(), 1)]);
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();

    assert_eq!(attr_n(item, "id"), "42");
    assert_eq!(item.get("boolean_").unwrap().as_bool().unwrap(), &true);
    assert_eq!(attr_n(item, "tinyint_"), "-8");
    assert_eq!(attr_n(item, "smallint_"), "16");
    assert_eq!(attr_n(item, "int_"), "32");
    assert_eq!(attr_n(item, "bigint_"), "64");
    assert_eq!(attr_n(item, "decimal_"), "12.345");
    assert_eq!(attr_n(item, "float_"), "1.5");
    assert_eq!(attr_n(item, "double_"), "2.25");
    assert_eq!(attr_s(item, "varchar_"), "hello");
    assert!(item.get("time_").unwrap().is_s());
    assert!(item.get("date_").unwrap().is_s());
    assert!(item.get("timestamp_").unwrap().is_s());
    assert_eq!(attr_s(item, "variant_"), "variant-value");
    assert_eq!(
        attr_s(item, "uuid_"),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert!(item.get("varbinary_").unwrap().is_b());
    assert!(item.get("struct_").unwrap().is_m());
    assert!(item.get("string_array_").unwrap().is_l());
    assert!(item.get("struct_array_").unwrap().is_l());
    assert!(item.get("map_").unwrap().is_m());
    assert_eq!(item.get("nullable_").unwrap().as_null().unwrap(), &true);
}

/// Returns the attribute that the worker writes for the VARIANT `variant`, a value in the
/// default JSON flavor.
fn encoded_variant_attribute(engine: &dyn ConnectorTestEngine, variant: Value) -> AttributeValue {
    let mut record = all_types_record();
    record["variant_"] = variant;

    let mut worker = all_types_worker();
    let batch = build_all_types_batch(engine, vec![(record, 1)]);
    stage_batch(&mut worker, &batch);
    worker.pending[0]
        .put_request()
        .unwrap()
        .item()
        .get("variant_")
        .unwrap()
        .clone()
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_variant_decimal_serializes_as_number(engine: &dyn ConnectorTestEngine) {
    let variant = encoded_variant_attribute(engine, json!(123.45));

    assert_eq!(variant.as_n().unwrap(), "123.45");
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_variant_map_preserves_private_number_key(engine: &dyn ConnectorTestEngine) {
    let variant = encoded_variant_attribute(
        engine,
        json!({"$serde_json::private::Number": "not-a-number-token", "other": 1}),
    );
    let variant = variant.as_m().unwrap();

    assert_eq!(
        variant
            .get("$serde_json::private::Number")
            .unwrap()
            .as_s()
            .unwrap(),
        "not-a-number-token"
    );
    assert_eq!(variant.get("other").unwrap().as_n().unwrap(), "1");
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_variant_list_recursively_serializes_numbers(engine: &dyn ConnectorTestEngine) {
    let variant = encoded_variant_attribute(engine, json!([123.45, {"nested": 6.789}]));
    let values = variant.as_l().unwrap();

    assert_eq!(values[0].as_n().unwrap(), "123.45");
    assert_eq!(
        values[1]
            .as_m()
            .unwrap()
            .get("nested")
            .unwrap()
            .as_n()
            .unwrap(),
        "6.789"
    );
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_split_cursor_partitions_encode_disjoint_key_ranges(
    engine: &dyn ConnectorTestEngine,
) {
    let batch = build_batch(
        engine,
        (0..10)
            .map(|id| (record(id, "split", Some(id as i64), "r"), 1))
            .collect(),
    );

    let mut all_requests = Vec::new();
    for range in batch.split(3, RecordFormat::DynamoDB).unwrap() {
        let mut worker = worker(100);
        let mut cursor = range.cursor().unwrap();
        worker.batch_start_inner();
        worker.encode_cursor(cursor.as_mut()).unwrap();
        all_requests.extend(worker.pending.iter().cloned());
    }

    let mut ids = all_requests
        .iter()
        .map(|request| {
            request
                .put_request()
                .unwrap()
                .item()
                .get("id")
                .unwrap()
                .as_n()
                .unwrap()
                .parse::<i32>()
                .unwrap()
        })
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, (0..10).collect::<Vec<_>>());
}

#[engine_test(requires(DynamoDbItems))]
pub fn encoder_composite_key_delete_excludes_non_key_fields(engine: &dyn ConnectorTestEngine) {
    // A delete request must carry exactly the two key columns. Value-only
    // fields ("b", "i", "s") must not appear; including them would be rejected
    // by DynamoDB as an invalid key.
    let mut worker = worker(25);
    let batch = build_batch(
        engine,
        vec![(record(5, "alpha", Some(50), "composite"), -1)],
    );
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].put_request().is_none());
    let key = worker.pending[0].delete_request().unwrap().key();
    assert_eq!(
        key.len(),
        2,
        "delete key should contain exactly the two composite key fields"
    );
    assert_eq!(attr_n(key, "id"), "5");
    assert_eq!(attr_s(key, "sort"), "alpha");
    assert!(!key.contains_key("b"));
    assert!(!key.contains_key("i"));
    assert!(!key.contains_key("s"));
}

#[test]
fn encoder_json_to_attribute_value_encodes_nested_values() {
    let item = HashMap::from([
        ("n".to_string(), AttributeValue::N("1.25".to_string())),
        ("s".to_string(), AttributeValue::S("x".to_string())),
        ("b".to_string(), AttributeValue::Bool(true)),
        ("null".to_string(), AttributeValue::Null(true)),
        (
            "list".to_string(),
            AttributeValue::L(vec![
                AttributeValue::N("1".to_string()),
                AttributeValue::S("two".to_string()),
            ]),
        ),
        (
            "map".to_string(),
            AttributeValue::M(HashMap::from([(
                "nested".to_string(),
                AttributeValue::Bool(false),
            )])),
        ),
    ]);

    assert_eq!(attr_n(&item, "n"), "1.25");
    assert_eq!(attr_s(&item, "s"), "x");
    assert_eq!(item.get("b").unwrap().as_bool().unwrap(), &true);
    assert_eq!(item.get("null").unwrap().as_null().unwrap(), &true);
    assert!(item.get("list").unwrap().is_l());
    assert!(item.get("map").unwrap().is_m());
}

#[test]
fn to_transact_item_attaches_conditions_per_operation() {
    use aws_sdk_dynamodb::types::{DeleteRequest, PutRequest, WriteRequest};

    let item = HashMap::from([("id".to_string(), AttributeValue::N("1".to_string()))]);
    let put = WriteRequest::builder()
        .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
        .build();
    let delete = WriteRequest::builder()
        .delete_request(
            DeleteRequest::builder()
                .set_key(Some(HashMap::from([(
                    "id".to_string(),
                    AttributeValue::N("1".to_string()),
                )])))
                .build()
                .unwrap(),
        )
        .build();

    // A put carries the put condition; a delete carries the delete condition.
    let transact_put =
        to_transact_item("t", &put, Some("attribute_not_exists(id)"), Some("bogus")).unwrap();
    assert_eq!(
        transact_put.put().unwrap().condition_expression(),
        Some("attribute_not_exists(id)")
    );

    let transact_delete =
        to_transact_item("t", &delete, Some("bogus"), Some("attribute_exists(id)")).unwrap();
    assert_eq!(
        transact_delete.delete().unwrap().condition_expression(),
        Some("attribute_exists(id)")
    );

    // No condition attached when none is configured.
    let unconditional = to_transact_item("t", &put, None, None).unwrap();
    assert_eq!(unconditional.put().unwrap().condition_expression(), None);
}

pub(super) fn dynamodb_endpoint_url() -> Option<String> {
    const DEFAULT_DYNAMODB_ENDPOINT: &str = "http://127.0.0.1:8000";

    Some(
        std::env::var("DYNAMODB_ENDPOINT")
            .ok()
            .filter(|endpoint| !endpoint.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_DYNAMODB_ENDPOINT.to_string()),
    )
}

pub(super) fn dynamodb_client(endpoint_url: Option<&str>) -> Client {
    make_client(&endpoint_config(
        "unused".to_string(),
        endpoint_url.map(str::to_string),
        1,
    ))
}

pub(super) fn wait_for_dynamodb(client: &Client) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if TOKIO
            .block_on(client.list_tables().send())
            .map(|_| ())
            .is_ok()
        {
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for DynamoDB endpoint"
        );
        sleep(Duration::from_millis(250));
    }
}

pub(super) fn test_table_name(suffix: &str) -> String {
    // A random suffix keeps concurrently running tests that share `suffix` (for
    // example the single- and multi-threaded variants) on separate tables.
    let random: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(12)
        .map(char::from)
        .collect();
    format!("feldera_dynamodb_{suffix}_{random}")
}

fn create_table(client: &Client, table: &str, sort_key: bool) {
    let mut create = client
        .create_table()
        .table_name(table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest);

    if sort_key {
        create = create
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("sort")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .unwrap(),
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("sort")
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            );
    }

    TOKIO.block_on(create.send()).unwrap();
}

fn delete_table(client: &Client, table: &str) {
    let _ = TOKIO.block_on(client.delete_table().table_name(table).send());
}

/// Creates a DynamoDB table and deletes it when dropped, so a test that panics
/// mid-way still cleans up after itself rather than leaking the table.
pub(super) struct TempDynamoTable {
    client: Client,
    name: String,
}

impl TempDynamoTable {
    pub(super) fn new(client: &Client, name: &str, sort_key: bool) -> Self {
        create_table(client, name, sort_key);
        TempDynamoTable {
            client: client.clone(),
            name: name.to_string(),
        }
    }
}

impl Drop for TempDynamoTable {
    fn drop(&mut self) {
        delete_table(&self.client, &self.name);
    }
}

fn scan_table(client: &Client, table: &str) -> Vec<HashMap<String, AttributeValue>> {
    let mut rows = TOKIO
        .block_on(client.scan().table_name(table).send())
        .unwrap()
        .items()
        .to_vec();
    rows.sort_by_key(|row| {
        row.get("id")
            .unwrap()
            .as_n()
            .unwrap()
            .parse::<i32>()
            .unwrap()
    });
    rows
}

pub(super) fn dynamodb_endpoint(
    threads: usize,
    table: &str,
    endpoint_url: Option<&str>,
) -> DynamoDBOutputEndpoint {
    let config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), threads);
    DynamoDBOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
    )
    .unwrap()
}

fn write_raw_json_input(records: &[Value]) -> NamedTempFile {
    let mut input_file = NamedTempFile::new().unwrap();
    for record in records {
        input_file
            .as_file_mut()
            .write_all(&serde_json::to_vec(record).unwrap())
            .unwrap();
        input_file.write_all(b"\n").unwrap();
    }
    input_file
}

fn dynamodb_buffered_output_pipeline(
    engine: &dyn PipelineTestEngine,
    table: &str,
    endpoint_url: Option<&str>,
    input_file: &NamedTempFile,
    max_output_buffer_size_records: usize,
    max_output_buffer_time_millis: u64,
) -> Controller {
    let output_config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), 1);
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": input_file.path() }
                },
                "format": {
                    "name": "json",
                    "config": { "update_format": "raw" }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "index": "idx1",
                "enable_output_buffer": true,
                "max_output_buffer_size_records": max_output_buffer_size_records,
                "max_output_buffer_time_millis": max_output_buffer_time_millis,
                "transport": {
                    "name": "dynamodb_output",
                    "config": output_config
                }
            }
        }
    }))
    .unwrap();

    let circuit = TestCircuit::Keyed {
        key: key_relation(),
        relation: value_relation(),
        persistent_output_ids: vec![None],
        aliased: false,
    };
    engine
        .controller(
            &circuit,
            &config,
            Box::new(move |e, _| panic!("dynamodb output buffer test: error: {e}")),
        )
        .unwrap()
}

fn dynamodb_all_types_endpoint(table: &str, endpoint_url: Option<&str>) -> DynamoDBOutputEndpoint {
    let config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), 2);
    DynamoDBOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(all_types_key_relation()),
        &all_types_value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
    )
    .unwrap()
}

fn expected_all_types_item(engine: &dyn ConnectorTestEngine) -> HashMap<String, AttributeValue> {
    let mut worker = all_types_worker();
    let batch = build_all_types_batch(engine, vec![(all_types_record(), 1)]);
    stage_batch(&mut worker, &batch);
    worker.pending[0].put_request().unwrap().item().clone()
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_insert(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("insert");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(2, "b", Some(20), "two"), 1),
            ],
        ),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    assert_eq!(attr_n(&rows[0], "id"), "1");
    assert_eq!(attr_s(&rows[0], "s"), "one");
    assert_eq!(attr_n(&rows[1], "id"), "2");
    assert_eq!(attr_s(&rows[1], "s"), "two");
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_upsert(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("upsert");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(engine, vec![(record(2, "b", Some(20), "two"), 1)]),
    );
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(2, "b", Some(20), "two"), -1),
                (record(2, "b", None, "two-updated"), 1),
            ],
        ),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(attr_n(&rows[0], "id"), "2");
    assert_eq!(attr_s(&rows[0], "sort"), "b");
    assert_eq!(rows[0].get("i").unwrap().as_null().unwrap(), &true);
    assert_eq!(attr_s(&rows[0], "s"), "two-updated");
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_delete(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("delete");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(3, "c", Some(30), "three"), 1),
            ],
        ),
    );
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(engine, vec![(record(1, "a", Some(10), "one"), -1)]),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(attr_n(&rows[0], "id"), "3");
    assert_eq!(attr_s(&rows[0], "sort"), "c");
    assert_eq!(attr_s(&rows[0], "s"), "three");
}

/// Builds a single-threaded transactional endpoint carrying the given condition
/// expressions, retaining `metrics` so the test can inspect the suppressed-write
/// count. A single worker keeps every item in one transaction, exercising the
/// discard-and-retry path when part of that transaction fails its condition.
fn dynamodb_conditional_endpoint(
    table: &str,
    endpoint_url: Option<&str>,
    put_condition: Option<&str>,
    delete_condition: Option<&str>,
    metrics: Arc<DynamoDBOutputMetrics>,
) -> DynamoDBOutputEndpoint {
    let mut config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), 1);
    config.write_mode = DynamoDBWriteMode::Transactional;
    config.batch_size = None;
    config.put_condition_expression = put_condition.map(str::to_string);
    config.delete_condition_expression = delete_condition.map(str::to_string);
    DynamoDBOutputEndpoint::new_with_metrics(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
        metrics,
    )
    .unwrap()
}

/// Writes an item directly, standing in for a row left by an earlier pipeline run.
fn seed_item(client: &Client, table: &str, id: i32, sort: &str, marker: &str) {
    TOKIO
        .block_on(
            client
                .put_item()
                .table_name(table)
                .item("id", AttributeValue::N(id.to_string()))
                .item("sort", AttributeValue::S(sort.to_string()))
                .item("s", AttributeValue::S(marker.to_string()))
                .send(),
        )
        .unwrap();
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_conditional_put_skips_existing_row(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("conditional_put_skip");
    let _table_guard = TempDynamoTable::new(&client, &table, true);

    // A row already present, as if written before a pipeline restart.
    seed_item(&client, &table, 1, "a", "original");

    let metrics = Arc::new(DynamoDBOutputMetrics::default());
    let mut endpoint = dynamodb_conditional_endpoint(
        &table,
        endpoint_url.as_deref(),
        Some("attribute_not_exists(id)"),
        None,
        metrics.clone(),
    );

    // One transaction: re-emit the existing key with a new value (must be
    // suppressed) alongside a genuinely new key (must land). The whole
    // transaction is first cancelled by the existing key; the connector drops
    // that item and retries the rest.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "reprocessed"), 1),
                (record(2, "b", Some(20), "fresh"), 1),
            ],
        ),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    // The existing row was left untouched.
    assert_eq!(attr_n(&rows[0], "id"), "1");
    assert_eq!(attr_s(&rows[0], "s"), "original");
    // The new row was written.
    assert_eq!(attr_n(&rows[1], "id"), "2");
    assert_eq!(attr_s(&rows[1], "s"), "fresh");

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_condition_check_failures_total"),
        1.0
    );
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_conditional_put_writes_all_new_rows(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("conditional_put_all_new");
    let _table_guard = TempDynamoTable::new(&client, &table, true);

    let metrics = Arc::new(DynamoDBOutputMetrics::default());
    let mut endpoint = dynamodb_conditional_endpoint(
        &table,
        endpoint_url.as_deref(),
        Some("attribute_not_exists(id)"),
        None,
        metrics.clone(),
    );

    // No key exists yet, so the condition holds for every put.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(2, "b", Some(20), "two"), 1),
            ],
        ),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    assert_eq!(attr_s(&rows[0], "s"), "one");
    assert_eq!(attr_s(&rows[1], "s"), "two");
    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_condition_check_failures_total"),
        0.0
    );
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_conditional_delete_skips_absent_row(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("conditional_delete");
    let _table_guard = TempDynamoTable::new(&client, &table, true);

    seed_item(&client, &table, 1, "a", "original");

    let metrics = Arc::new(DynamoDBOutputMetrics::default());
    let mut endpoint = dynamodb_conditional_endpoint(
        &table,
        endpoint_url.as_deref(),
        None,
        Some("attribute_exists(id)"),
        metrics.clone(),
    );

    // Delete the existing key (condition holds) and a key that was never written
    // (condition fails, delete suppressed).
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "original"), -1),
                (record(2, "b", Some(20), "absent"), -1),
            ],
        ),
    );

    assert!(scan_table(&client, &table).is_empty());
    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_condition_check_failures_total"),
        1.0
    );
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_all_supported_sql_types_round_trip(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("all_types");
    let _table_guard = TempDynamoTable::new(&client, &table, false);
    let mut endpoint = dynamodb_all_types_endpoint(&table, endpoint_url.as_deref());
    let batch = build_all_types_batch(engine, vec![(all_types_record(), 1)]);

    encode_endpoint_batch(&mut endpoint, &batch);

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], expected_all_types_item(engine,));
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_output_buffer_flushes_by_size(engine: &dyn PipelineTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("output_buffer_size");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let input_file = write_raw_json_input(&[
        record(1, "a", Some(10), "one"),
        record(2, "b", Some(20), "two"),
        record(3, "c", Some(30), "three"),
    ]);
    let controller = dynamodb_buffered_output_pipeline(
        engine,
        &table,
        endpoint_url.as_deref(),
        &input_file,
        3,
        60_000,
    );

    controller.start();
    wait(|| scan_table(&client, &table).len() == 3, 60_000)
        .expect("timeout waiting for size-triggered output buffer flush");
    controller.stop().unwrap();
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_output_buffer_flushes_by_time(engine: &dyn PipelineTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("output_buffer_time");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let input_file = write_raw_json_input(&[
        record(1, "a", Some(10), "one"),
        record(2, "b", Some(20), "two"),
    ]);
    let controller = dynamodb_buffered_output_pipeline(
        engine,
        &table,
        endpoint_url.as_deref(),
        &input_file,
        1_000,
        200,
    );

    controller.start();
    wait(|| scan_table(&client, &table).len() == 2, 60_000)
        .expect("timeout waiting for time-triggered output buffer flush");
    controller.stop().unwrap();
}

/// Encodes a batch holding two normal-sized records and one record that exceeds
/// DynamoDB's 400 KB item-size limit, then verifies that the oversized record is
/// dropped while the normal records are written. The three records share a
/// single write chunk, so this also confirms the oversized record does not
/// poison the chunk it travels in: it is never sent and therefore never
/// retried.
fn check_oversized_item_dropped(engine: &dyn ConnectorTestEngine, write_mode: DynamoDBWriteMode) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("oversized");
    let _table_guard = TempDynamoTable::new(&client, &table, true);

    let mut config = endpoint_config(table.clone(), endpoint_url.clone(), 1);
    config.write_mode = write_mode;

    let metrics = Arc::new(DynamoDBOutputMetrics::default());
    let mut endpoint = DynamoDBOutputEndpoint::new_with_metrics(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
        metrics.clone(),
    )
    .unwrap();

    // The middle record's `s` attribute alone is larger than the 400 KB limit;
    // the other two are ordinary.
    let oversized_value = "x".repeat(410 * 1024);
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(2, "b", Some(20), &oversized_value), 1),
                (record(3, "c", Some(30), "three"), 1),
            ],
        ),
    );

    // The two normal records land; the oversized record is dropped.
    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    assert_eq!(attr_n(&rows[0], "id"), "1");
    assert_eq!(attr_s(&rows[0], "s"), "one");
    assert_eq!(attr_n(&rows[1], "id"), "3");
    assert_eq!(attr_s(&rows[1], "s"), "three");

    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_oversized_items_dropped_total"),
        1.0,
    );

    // The oversized record was dropped before it was sent, so it was never
    // retried and nothing failed downstream.
    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_failed_items_total"),
        0.0,
    );
    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_transact_write_failures_total"),
        0.0,
    );
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_oversized_item_dropped_batch_mode(engine: &dyn ConnectorTestEngine) {
    check_oversized_item_dropped(engine, DynamoDBWriteMode::Batch);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_oversized_item_dropped_transactional_mode(engine: &dyn ConnectorTestEngine) {
    check_oversized_item_dropped(engine, DynamoDBWriteMode::Transactional);
}

pub(super) fn records_written(endpoint: &DynamoDBOutputEndpoint) -> u64 {
    endpoint.records_written.load(Ordering::Relaxed)
}

fn dynamodb_endpoint_flush_every(
    max_buffer_size_bytes: usize,
    threads: usize,
    table: &str,
    endpoint_url: Option<&str>,
) -> DynamoDBOutputEndpoint {
    let mut config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), threads);
    config.max_buffer_size = ConfigSize::from_bytes(max_buffer_size_bytes as u64);
    DynamoDBOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
    )
    .unwrap()
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_empty_batch(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("empty");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    encode_endpoint_batch(&mut endpoint, &build_batch(engine, vec![]));

    assert_eq!(scan_table(&client, &table).len(), 0);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_multiple_batches(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("multi");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    // Batch 1: insert two records.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(2, "b", Some(20), "two"), 1),
            ],
        ),
    );
    assert_eq!(scan_table(&client, &table).len(), 2);

    // Batch 2: upsert record 1.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "a", Some(10), "one"), -1),
                (record(1, "a", Some(99), "one-updated"), 1),
            ],
        ),
    );
    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    assert_eq!(attr_n(&rows[0], "i"), "99");
    assert_eq!(attr_s(&rows[0], "s"), "one-updated");

    // Batch 3: delete record 2.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(engine, vec![(record(2, "b", Some(20), "two"), -1)]),
    );
    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(attr_n(&rows[0], "id"), "1");
}

fn backpressure_writes_all_records(engine: &dyn ConnectorTestEngine, threads: usize) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("backpressure");
    let _table_guard = TempDynamoTable::new(&client, &table, true);

    let mut config = endpoint_config(table.clone(), endpoint_url.clone(), threads);
    config.max_buffer_size = ConfigSize::from_bytes(1); // flush after every record
    config.max_concurrent_requests = 1; // a single in-flight write per worker
    let mut endpoint = DynamoDBOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
    )
    .unwrap();

    let num_records = 200i32;
    let batch = build_batch(
        engine,
        (0..num_records)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );
    encode_endpoint_batch(&mut endpoint, &batch);

    assert_eq!(scan_table(&client, &table).len(), num_records as usize);
    // Counter resets to 0 at the end of every batch.
    assert_eq!(records_written(&endpoint), 0);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_backpressure_single_thread(engine: &dyn ConnectorTestEngine) {
    backpressure_writes_all_records(engine, 1);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_backpressure_multi_thread(engine: &dyn ConnectorTestEngine) {
    backpressure_writes_all_records(engine, 4);
}

fn progress_basic(engine: &dyn ConnectorTestEngine, threads: usize) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_basic");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(threads, &table, endpoint_url.as_deref());
    let batch = build_batch(
        engine,
        (0..100i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );

    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written(&endpoint), 0);
    endpoint.encode(batch.clone()).unwrap();
    endpoint.consumer().batch_end();
    // Counter resets to 0 at the end of every batch.
    assert_eq!(records_written(&endpoint), 0);

    assert_eq!(scan_table(&client, &table).len(), 100);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_progress_counter_single_thread(engine: &dyn ConnectorTestEngine) {
    progress_basic(engine, 1);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_progress_counter_multi_thread(engine: &dyn ConnectorTestEngine) {
    progress_basic(engine, 3);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_progress_counter_empty_batch(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_empty");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(build_batch(engine, vec![])).unwrap();
    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

fn progress_advances_mid_batch(engine: &dyn ConnectorTestEngine, threads: usize) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_mid");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    // max_buffer_size_bytes=1 forces a flush after every single encoded record.
    let mut endpoint = dynamodb_endpoint_flush_every(1, threads, &table, endpoint_url.as_deref());
    let num_records = 50usize;
    let batch = build_batch(
        engine,
        (0..num_records as i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();

    // With per-record flushes, the counter must be > 0 before batch_end.
    let mid_count = records_written(&endpoint);
    assert!(
        mid_count > 0,
        "expected records_written > 0 mid-batch, got {mid_count}"
    );
    assert!(
        mid_count <= num_records as u64,
        "records_written {mid_count} exceeds total {num_records}"
    );

    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);

    assert_eq!(scan_table(&client, &table).len(), num_records);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_progress_counter_advances_mid_batch_single_thread(
    engine: &dyn ConnectorTestEngine,
) {
    progress_advances_mid_batch(engine, 1);
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_progress_counter_advances_mid_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    progress_advances_mid_batch(engine, 3);
}

#[test]
fn dynamodb_progress_counter_batch_start_resets() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_reset");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    // Simulate a stale counter left over from a previous batch.
    endpoint.records_written.store(99, Ordering::Relaxed);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written(&endpoint), 0);

    // Close cleanly so the worker does not leak an open state.
    endpoint.consumer().batch_end();
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_composite_key_same_hash_different_sort(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("composite");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(
            engine,
            vec![
                (record(1, "first", Some(10), "row-one"), 1),
                (record(1, "second", Some(20), "row-two"), 1),
            ],
        ),
    );

    assert_eq!(
        scan_table(&client, &table).len(),
        2,
        "both rows with id=1 should be stored separately by sort key"
    );

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(engine, vec![(record(1, "first", Some(10), "row-one"), -1)]),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(
        rows.len(),
        1,
        "deleting sort=first should leave sort=second intact"
    );
    assert_eq!(attr_n(&rows[0], "id"), "1");
    assert_eq!(attr_s(&rows[0], "sort"), "second");
    assert_eq!(attr_s(&rows[0], "s"), "row-two");
}

#[engine_test(requires(DynamoDbItems), services(DynamoDbLocal))]
pub fn dynamodb_progress_counter_multiple_batches(engine: &dyn ConnectorTestEngine) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_multi");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    let batch1 = build_batch(
        engine,
        (0..50i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );
    encode_endpoint_batch(&mut endpoint, &batch1);
    assert_eq!(records_written(&endpoint), 0);

    let batch2 = build_batch(
        engine,
        (50..80i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );
    encode_endpoint_batch(&mut endpoint, &batch2);
    assert_eq!(records_written(&endpoint), 0);

    assert_eq!(scan_table(&client, &table).len(), 80);
}

/// A shutdown ends `write_transact_chunk`'s unbounded retry loop.
///
/// `max_retries: None` makes the loop unbounded, and
/// `DynamoDBWorker::drain_pending_writes` blocks the controller's output
/// thread on these tasks, a DBSP aux thread that `RuntimeHandle::kill` joins.
///
/// Nothing listens at the client's endpoint, so every attempt fails and the
/// loop cannot end on its own.
#[test]
fn a_shutdown_ends_an_unbounded_transact_retry_loop() {
    use aws_sdk_dynamodb::types::{PutRequest, WriteRequest};

    let item = HashMap::from([("id".to_string(), AttributeValue::N("1".to_string()))]);
    let put = WriteRequest::builder()
        .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
        .build();
    let items = vec![to_transact_item("test_table", &put, None, None).unwrap()];

    let shutdown = CancellationToken::new();
    let client = dummy_client();

    // Write on a thread of its own, so that a regression fails this test
    // rather than hanging the whole run.
    let writing_shutdown = shutdown.clone();
    let (done, write_returned) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let metrics = Arc::new(DynamoDBOutputMetrics::default());
        let health = RetryHealth::new(Box::new(NoHealthPublisher), metrics.clone());
        let result = TOKIO.block_on(write_transact_chunk(
            client,
            "test_endpoint".to_string(),
            items,
            &RetryPolicy::new(None),
            &writing_shutdown,
            &metrics,
            &health,
        ));
        let _ = done.send(result.err());
    });

    // Long enough for the first attempt to fail and the retry to back off.
    sleep(Duration::from_millis(200));
    shutdown.cancel();

    let error = write_returned
        .recv_timeout(Duration::from_secs(30))
        .expect("a shutdown must end the retry loop")
        .expect("a chunk that gave up must report an error");
    writer.join().unwrap();

    assert!(
        error.to_string().contains("shutting down"),
        "unexpected error: {error}"
    );
}

// ---------------------------------------------------------------------------
// A mock DynamoDB that answers as the test says, for the retry loops and the
// health they report. DynamoDB Local cannot throttle on demand.
// ---------------------------------------------------------------------------

// The retry-loop tests of `cfg(test)` use most of it; the connector suites use the rest.
#[cfg_attr(not(test), allow(dead_code))]
mod mock_dynamodb {
    use super::*;
    use crate::integrated::dynamodb::health::test::RecordingPublisher;
    use crate::integrated::dynamodb::helpers::{
        RetryDelay, TransactChunkOutcome, client_config, write_batch_chunk, write_transact_chunk,
    };
    use aws_credential_types::provider::error::CredentialsError;
    use aws_credential_types::provider::future::ProvideCredentials as ProvideCredentialsFuture;
    use aws_sdk_dynamodb::config::retry::RetryConfig;
    use aws_sdk_dynamodb::config::{Credentials, IdentityCache, ProvideCredentials};
    use aws_sdk_dynamodb::types::{PutRequest, TransactWriteItem, WriteRequest};
    use std::sync::atomic::AtomicUsize;
    use std::sync::mpsc;
    use tokio::sync::Mutex as AsyncMutex;
    use wiremock::matchers::{body_string_contains, header};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    pub(super) const BATCH_TARGET: &str = "DynamoDB_20120810.BatchWriteItem";
    pub(super) const TRANSACT_TARGET: &str = "DynamoDB_20120810.TransactWriteItems";
    const JSON: &str = "application/x-amz-json-1.0";
    const RESOURCE: &str = "arn:aws:dynamodb:us-east-1:1:table/t";

    pub(super) fn server() -> MockServer {
        TOKIO.block_on(MockServer::start())
    }

    /// A mock for one operation. Mount it in order: the first mounted mock
    /// with responses left answers.
    pub(super) fn on(target: &str) -> wiremock::MockBuilder {
        Mock::given(header("x-amz-target", target))
    }

    fn error_body(code: &str, message: &str, extra: Value) -> Value {
        let mut body = json!({
            "__type": format!("com.amazonaws.dynamodb.v20120810#{code}"),
            "message": message,
        });
        if let Value::Object(extra) = extra {
            body.as_object_mut().unwrap().extend(extra);
        }
        body
    }

    pub(super) fn error(code: &str, message: &str) -> ResponseTemplate {
        ResponseTemplate::new(400)
            .insert_header("content-type", JSON)
            .set_body_json(error_body(code, message, json!({})))
    }

    pub(super) fn throttled() -> ResponseTemplate {
        ResponseTemplate::new(400)
            .insert_header("content-type", JSON)
            .insert_header("x-amzn-requestid", "req-throttled")
            .set_body_json(error_body(
                "ThrottlingException",
                "Throughput exceeds the current capacity of your table or index.",
                json!({"throttlingReasons": [
                    {"reason": "TableWriteKeyRangeThroughputExceeded", "resource": RESOURCE}
                ]}),
            ))
    }

    pub(super) fn ok() -> ResponseTemplate {
        ResponseTemplate::new(200)
            .insert_header("content-type", JSON)
            .set_body_json(json!({"UnprocessedItems": {}}))
    }

    /// A `BatchWriteItem` response that returns the items with these ids unprocessed.
    pub(super) fn unprocessed(ids: &[i32]) -> ResponseTemplate {
        let items = ids
            .iter()
            .map(|id| json!({"PutRequest": {"Item": item_json(*id)}}))
            .collect::<Vec<_>>();
        ResponseTemplate::new(200)
            .insert_header("content-type", JSON)
            .set_body_json(json!({"UnprocessedItems": {"tbl": items}}))
    }

    /// A `TransactWriteItems` cancellation with one reason per item. `None` is
    /// an item with no error of its own, as DynamoDB reports it.
    pub(super) fn cancelled(codes: &[&str]) -> ResponseTemplate {
        let reasons = codes
            .iter()
            .map(|code| json!({"Code": code, "Message": format!("{code} message")}))
            .collect::<Vec<_>>();
        ResponseTemplate::new(400)
            .insert_header("content-type", JSON)
            .set_body_json(error_body(
                "TransactionCanceledException",
                "Transaction cancelled, please refer cancellation reasons for specific reasons",
                json!({"CancellationReasons": reasons}),
            ))
    }

    fn item_json(id: i32) -> Value {
        json!({"id": {"N": id.to_string()}})
    }

    pub(super) fn put(id: i32) -> WriteRequest {
        let item = HashMap::from([("id".to_string(), AttributeValue::N(id.to_string()))]);
        WriteRequest::builder()
            .put_request(PutRequest::builder().set_item(Some(item)).build().unwrap())
            .build()
    }

    pub(super) fn puts(ids: &[i32]) -> Vec<WriteRequest> {
        ids.iter().map(|id| put(*id)).collect()
    }

    pub(super) fn transact_puts(ids: &[i32]) -> Vec<TransactWriteItem> {
        ids.iter()
            .map(|id| to_transact_item("tbl", &put(*id), None, None).unwrap())
            .collect()
    }

    pub(super) fn mock_config(server: &MockServer) -> DynamoDBWriterConfig {
        DynamoDBWriterConfig {
            table: "tbl".to_string(),
            endpoint_url: Some(server.uri()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            ..config(25)
        }
    }

    /// A client whose SDK makes `sdk_attempts` attempts per request, so a
    /// test counts the connector's own retries. `None` keeps the SDK default.
    pub(super) fn client(server: &MockServer, sdk_attempts: Option<u32>) -> Client {
        let mut builder = client_config(&mock_config(server));
        if let Some(attempts) = sdk_attempts {
            builder = builder.retry_config(RetryConfig::standard().with_max_attempts(attempts));
        }
        Client::from_conf(builder.build())
    }

    pub(super) fn single_attempt_client(server: &MockServer) -> Client {
        client(server, Some(1))
    }

    /// The requests the server received for `target`, in order.
    pub(super) fn requests(server: &MockServer, target: &str) -> Vec<Request> {
        TOKIO
            .block_on(server.received_requests())
            .unwrap_or_default()
            .into_iter()
            .filter(|request| {
                request
                    .headers
                    .get("x-amz-target")
                    .is_some_and(|value| value == target)
            })
            .collect()
    }

    /// The write items a request carries: `RequestItems` of a batch, or
    /// `TransactItems` of a transaction, which leaves the fresh
    /// `ClientRequestToken` of each request out of the comparison.
    pub(super) fn write_items(request: &Request) -> Value {
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        body.get("RequestItems")
            .or_else(|| body.get("TransactItems"))
            .cloned()
            .unwrap()
    }

    pub(super) fn item_count(request: &Request) -> usize {
        match write_items(request) {
            Value::Object(tables) => tables
                .values()
                .map(|items| items.as_array().unwrap().len())
                .sum(),
            Value::Array(items) => items.len(),
            _ => unreachable!(),
        }
    }

    /// A delay that waits at once.
    pub(super) fn no_delay() -> RetryDelay {
        Arc::new(|_| Box::pin(async {}))
    }

    /// A delay that reports each retry it enters on `entered` and waits for
    /// one `()` on the release channel, so a test holds a chunk in its retry
    /// loop and observes it there.
    pub(super) struct Gate {
        pub(super) entered: mpsc::Receiver<usize>,
        release: tokio::sync::mpsc::UnboundedSender<()>,
    }

    impl Gate {
        pub(super) fn new() -> (RetryDelay, Gate) {
            let (entered_tx, entered) = mpsc::channel();
            let (release, release_rx) = tokio::sync::mpsc::unbounded_channel();
            let release_rx = Arc::new(AsyncMutex::new(release_rx));
            let delay: RetryDelay = Arc::new(move |retry| {
                let entered_tx = entered_tx.clone();
                let release_rx = release_rx.clone();
                Box::pin(async move {
                    let _ = entered_tx.send(retry);
                    release_rx.lock().await.recv().await;
                })
            });
            (delay, Gate { entered, release })
        }

        /// Waits for the chunk to enter its next retry, and returns its number.
        pub(super) fn entered(&self) -> usize {
            self.entered
                .recv_timeout(Duration::from_secs(30))
                .expect("the chunk did not enter a retry")
        }

        pub(super) fn release(&self) {
            self.release.send(()).unwrap();
        }

        pub(super) fn no_retry_entered(&self) {
            assert!(
                self.entered.try_recv().is_err(),
                "the chunk entered a retry"
            );
        }
    }

    /// The shared state a chunk writer needs, with a publisher that records.
    pub(super) struct Harness {
        pub(super) metrics: Arc<DynamoDBOutputMetrics>,
        pub(super) health: Arc<RetryHealth>,
        pub(super) publisher: RecordingPublisher,
        pub(super) shutdown: CancellationToken,
    }

    impl Harness {
        pub(super) fn new() -> Self {
            let metrics = Arc::new(DynamoDBOutputMetrics::default());
            let publisher = RecordingPublisher::default();
            let health = Arc::new(RetryHealth::new(
                Box::new(publisher.clone()),
                metrics.clone(),
            ));
            Self {
                metrics,
                health,
                publisher,
                shutdown: CancellationToken::new(),
            }
        }

        pub(super) fn metric(&self, name: &str) -> f64 {
            dynamodb_metric(&self.metrics, name)
        }

        /// Runs a batch chunk on the shared runtime. The handle resolves when
        /// the chunk is written, dropped, or given up on.
        pub(super) fn write_batch(
            &self,
            client: Client,
            requests: Vec<WriteRequest>,
            policy: RetryPolicy,
        ) -> tokio::task::JoinHandle<anyhow::Result<u64>> {
            let (metrics, health, shutdown) = (
                self.metrics.clone(),
                self.health.clone(),
                self.shutdown.clone(),
            );
            TOKIO.spawn(async move {
                write_batch_chunk(
                    client,
                    "test_endpoint".to_string(),
                    "tbl".to_string(),
                    requests,
                    &policy,
                    &shutdown,
                    &metrics,
                    &health,
                )
                .await
            })
        }

        pub(super) fn write_transact(
            &self,
            client: Client,
            items: Vec<TransactWriteItem>,
            policy: RetryPolicy,
        ) -> tokio::task::JoinHandle<anyhow::Result<TransactChunkOutcome>> {
            let (metrics, health, shutdown) = (
                self.metrics.clone(),
                self.health.clone(),
                self.shutdown.clone(),
            );
            TOKIO.spawn(async move {
                write_transact_chunk(
                    client,
                    "test_endpoint".to_string(),
                    items,
                    &policy,
                    &shutdown,
                    &metrics,
                    &health,
                )
                .await
            })
        }
    }

    /// Waits for a chunk writer, with a bound so a regression fails the test
    /// rather than hanging the run.
    pub(super) fn finish<T>(handle: tokio::task::JoinHandle<T>) -> T {
        TOKIO
            .block_on(async { tokio::time::timeout(Duration::from_secs(60), handle).await })
            .expect("the chunk writer did not finish")
            .expect("the chunk writer panicked")
    }

    /// A credential provider that fails as the test says before it answers.
    pub(super) struct FlakyProvider {
        /// What each call does, in order; later calls succeed.
        failures: Vec<Failure>,
        pub(super) calls: Arc<AtomicUsize>,
    }

    #[derive(Clone, Copy)]
    pub(super) enum Failure {
        ProviderError,
        InvalidConfiguration,
        /// Never answers, so the identity cache's load timeout fires.
        Hang,
    }

    impl FlakyProvider {
        pub(super) fn new(failures: Vec<Failure>) -> Self {
            Self {
                failures,
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl std::fmt::Debug for FlakyProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("FlakyProvider")
        }
    }

    impl ProvideCredentials for FlakyProvider {
        fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFuture<'a>
        where
            Self: 'a,
        {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let failure = self.failures.get(call).copied();
            ProvideCredentialsFuture::new(async move {
                match failure {
                    Some(Failure::ProviderError) => {
                        Err(CredentialsError::provider_error("STS is down"))
                    }
                    Some(Failure::InvalidConfiguration) => Err(
                        CredentialsError::invalid_configuration("the role ARN is malformed"),
                    ),
                    Some(Failure::Hang) => std::future::pending().await,
                    None => Ok(Credentials::new("dummy", "dummy", None, None, "flaky")),
                }
            })
        }
    }

    /// A single-attempt client whose credentials come from `provider`, with a
    /// short identity-cache load timeout.
    pub(super) fn client_with_provider(server: &MockServer, provider: FlakyProvider) -> Client {
        Client::from_conf(
            aws_sdk_dynamodb::Config::builder()
                .region(aws_sdk_dynamodb::config::Region::new("us-east-1"))
                .endpoint_url(server.uri())
                .credentials_provider(provider)
                .identity_cache(
                    IdentityCache::lazy()
                        .load_timeout(Duration::from_millis(200))
                        .build(),
                )
                .retry_config(RetryConfig::standard().with_max_attempts(1))
                .build(),
        )
    }

    /// Mounts `response` for requests of `target` whose body names item `id`.
    pub(super) fn on_item(target: &str, id: i32) -> wiremock::MockBuilder {
        on(target).and(body_string_contains(format!("\"N\":\"{id}\"")))
    }
}

/// The retry loops against the mock DynamoDB: what they resend, what they
/// count, when they give up, and the health they report.
#[cfg(test)]
mod retry_tests {
    use super::mock_dynamodb::{requests as received, server as mock_server, *};
    use super::*;
    use std::sync::atomic::Ordering;

    fn policy(max_retries: Option<u8>) -> RetryPolicy {
        RetryPolicy::with_delay(max_retries, no_delay())
    }

    fn mount(server: &wiremock::MockServer, mock: wiremock::Mock) {
        TOKIO.block_on(mock.mount(server));
    }

    fn mount_sequence(
        server: &wiremock::MockServer,
        target: &str,
        responses: Vec<wiremock::ResponseTemplate>,
    ) {
        for response in responses {
            mount(server, on(target).respond_with(response).up_to_n_times(1));
        }
    }

    /// Always answer `response` after the mounted sequence.
    fn mount_rest(
        server: &wiremock::MockServer,
        target: &str,
        response: wiremock::ResponseTemplate,
    ) {
        mount(server, on(target).respond_with(response));
    }

    #[test]
    fn a_throttled_batch_chunk_is_retried_until_it_lands() {
        let server = mock_server();
        mount_sequence(&server, BATCH_TARGET, vec![throttled(), throttled()]);
        mount_rest(&server, BATCH_TARGET, ok());
        let harness = Harness::new();

        let retries = finish(harness.write_batch(
            single_attempt_client(&server),
            puts(&[1, 2]),
            policy(Some(10)),
        ))
        .unwrap();

        assert_eq!(retries, 2);
        let requests = received(&server, BATCH_TARGET);
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|request| write_items(request) == write_items(&requests[0])),
            "a retry must resend the same items"
        );
        assert_eq!(harness.metric("dynamodb_output_retries_total"), 2.0);
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 0.0);
        assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 0.0);
        let reports = harness.publisher.reports();
        assert_eq!(reports.len(), 3, "{reports:?}");
        assert!(
            reports[0]
                .as_ref()
                .is_err_and(|cause| cause.contains("retry 1 of 10")
                    && cause.contains("TableWriteKeyRangeThroughputExceeded")
                    && cause.contains("req-throttled")),
            "{reports:?}"
        );
        assert!(
            reports[1]
                .as_ref()
                .is_err_and(|cause| cause.contains("retry 2 of 10")),
            "{reports:?}"
        );
        assert_eq!(reports[2], Ok(()));
    }

    #[test]
    fn a_throttled_transaction_is_retried_until_it_lands() {
        let server = mock_server();
        mount_sequence(&server, TRANSACT_TARGET, vec![throttled(), throttled()]);
        mount_rest(&server, TRANSACT_TARGET, ok());
        let harness = Harness::new();

        let outcome = finish(harness.write_transact(
            single_attempt_client(&server),
            transact_puts(&[1, 2]),
            policy(None),
        ))
        .unwrap();

        assert_eq!(outcome.retries, 2);
        assert_eq!(outcome.suppressed, 0);
        let requests = received(&server, TRANSACT_TARGET);
        assert_eq!(requests.len(), 3);
        assert!(
            requests
                .iter()
                .all(|request| write_items(request) == write_items(&requests[0]))
        );
        assert_eq!(harness.metric("dynamodb_output_retries_total"), 2.0);
        assert_eq!(
            harness.metric("dynamodb_output_transact_write_failures_total"),
            2.0
        );
        assert_eq!(harness.publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn the_sdk_retries_and_the_connector_retries_stack() {
        let server = mock_server();
        // More throttles than the SDK's attempts, so the connector must retry.
        mount(
            &server,
            on(BATCH_TARGET).respond_with(throttled()).up_to_n_times(4),
        );
        mount_rest(&server, BATCH_TARGET, ok());
        let harness = Harness::new();

        let retries =
            finish(harness.write_batch(client(&server, None), puts(&[1]), policy(Some(10))))
                .unwrap();

        assert!(retries >= 1, "{retries}");
        assert!(received(&server, BATCH_TARGET).len() >= 5);
        assert_eq!(harness.publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn the_retry_budget_is_exact() {
        for (max_retries, expected_requests) in [(0u8, 1usize), (2, 3)] {
            let server = mock_server();
            mount_rest(&server, BATCH_TARGET, throttled());
            let harness = Harness::new();
            let error = finish(harness.write_batch(
                single_attempt_client(&server),
                puts(&[1, 2, 3]),
                policy(Some(max_retries)),
            ))
            .unwrap_err()
            .to_string();
            assert_eq!(received(&server, BATCH_TARGET).len(), expected_requests);
            assert!(
                error.contains(&format!(
                    "dropped 3 item(s) after {max_retries} BatchWriteItem retries"
                )) && error.contains("max_retries")
                    && error.contains("ThrottlingException"),
                "{error}"
            );
            assert_eq!(
                harness.metric("dynamodb_output_retries_total"),
                max_retries as f64
            );
            assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 3.0);
            assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 0.0);
            assert_eq!(harness.publisher.reports().last(), Some(&Err(error)));

            let server = mock_server();
            mount_rest(&server, TRANSACT_TARGET, throttled());
            let harness = Harness::new();
            let error = finish(harness.write_transact(
                single_attempt_client(&server),
                transact_puts(&[1, 2, 3]),
                policy(Some(max_retries)),
            ))
            .unwrap_err()
            .to_string();
            assert_eq!(received(&server, TRANSACT_TARGET).len(), expected_requests);
            assert!(error.contains("TransactWriteItems retries"), "{error}");
            assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 3.0);
        }
    }

    #[test]
    fn a_permanent_error_fails_the_chunk_at_once() {
        for (target, code) in [
            (BATCH_TARGET, "ValidationException"),
            (BATCH_TARGET, "AccessDeniedException"),
            (TRANSACT_TARGET, "ResourceNotFoundException"),
        ] {
            let server = mock_server();
            mount_rest(&server, target, error(code, "no"));
            let harness = Harness::new();
            let (delay, gate) = Gate::new();
            let policy = RetryPolicy::with_delay(None, delay);
            let error = if target == BATCH_TARGET {
                finish(harness.write_batch(single_attempt_client(&server), puts(&[1, 2]), policy))
                    .unwrap_err()
            } else {
                finish(harness.write_transact(
                    single_attempt_client(&server),
                    transact_puts(&[1, 2]),
                    policy,
                ))
                .unwrap_err()
            }
            .to_string();
            assert_eq!(received(&server, target).len(), 1, "{code}");
            gate.no_retry_entered();
            assert!(
                error.contains("a retry cannot fix") && error.contains(code),
                "{error}"
            );
            assert_eq!(harness.metric("dynamodb_output_retries_total"), 0.0);
            assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 2.0);
            assert_eq!(harness.publisher.reports(), vec![Err(error)]);
        }
    }

    #[test]
    fn credential_failures_are_retried_until_the_provider_answers() {
        for target in [BATCH_TARGET, TRANSACT_TARGET] {
            for failures in [
                vec![Failure::ProviderError, Failure::ProviderError],
                vec![Failure::Hang, Failure::Hang],
            ] {
                let server = mock_server();
                mount_rest(&server, target, ok());
                let harness = Harness::new();
                let provider = FlakyProvider::new(failures);
                let calls = provider.calls.clone();
                let client = client_with_provider(&server, provider);
                let retries = if target == BATCH_TARGET {
                    finish(harness.write_batch(client, puts(&[1]), policy(Some(5)))).unwrap()
                } else {
                    finish(harness.write_transact(client, transact_puts(&[1]), policy(Some(5))))
                        .unwrap()
                        .retries
                };
                assert_eq!(retries, 2);
                assert_eq!(calls.load(Ordering::SeqCst), 3);
                // Credential resolution failed before any write reached the server.
                assert_eq!(received(&server, target).len(), 1);
                assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 0.0);
                let reports = harness.publisher.reports();
                assert!(reports.len() >= 2 && reports[0].is_err(), "{reports:?}");
                assert_eq!(reports.last(), Some(&Ok(())));
            }
        }
    }

    #[test]
    fn an_invalid_credentials_configuration_fails_at_once() {
        let server = mock_server();
        mount_rest(&server, BATCH_TARGET, ok());
        let harness = Harness::new();
        let provider = FlakyProvider::new(vec![Failure::InvalidConfiguration; 3]);
        let calls = provider.calls.clone();
        let client = client_with_provider(&server, provider);

        let error = finish(harness.write_batch(client, puts(&[1]), policy(None)))
            .unwrap_err()
            .to_string();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(received(&server, BATCH_TARGET).is_empty());
        assert!(
            error.contains("a retry cannot fix") && error.contains("the role ARN is malformed"),
            "{error}"
        );
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 1.0);
    }

    #[test]
    fn an_item_collection_over_its_size_limit_is_retried_within_the_budget() {
        let limit = || {
            error(
                "ItemCollectionSizeLimitExceededException",
                "Collection size exceeded.",
            )
        };

        // As a whole-request error, in batch mode.
        let server = mock_server();
        mount_sequence(&server, BATCH_TARGET, vec![limit()]);
        mount_rest(&server, BATCH_TARGET, ok());
        let harness = Harness::new();
        let retries = finish(harness.write_batch(
            single_attempt_client(&server),
            puts(&[1, 2]),
            policy(Some(3)),
        ))
        .unwrap();
        assert_eq!(retries, 1);
        assert_eq!(received(&server, BATCH_TARGET).len(), 2);

        // As a per-item cancellation reason, in transactional mode.
        let server = mock_server();
        mount_sequence(
            &server,
            TRANSACT_TARGET,
            vec![cancelled(&["None", "ItemCollectionSizeLimitExceeded"])],
        );
        mount_rest(&server, TRANSACT_TARGET, ok());
        let harness = Harness::new();
        let outcome = finish(harness.write_transact(
            single_attempt_client(&server),
            transact_puts(&[1, 2]),
            policy(Some(3)),
        ))
        .unwrap();
        assert_eq!(outcome.retries, 1);
        let requests = received(&server, TRANSACT_TARGET);
        assert_eq!(requests.len(), 2);
        assert_eq!(item_count(&requests[1]), 2, "both items are retried");
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 0.0);

        // Exhaustion names the limit and counts the outstanding items.
        let server = mock_server();
        mount_rest(&server, TRANSACT_TARGET, limit());
        let harness = Harness::new();
        let error = finish(harness.write_transact(
            single_attempt_client(&server),
            transact_puts(&[1, 2]),
            policy(Some(1)),
        ))
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("ItemCollectionSizeLimitExceededException"),
            "{error}"
        );
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 2.0);
    }

    #[test]
    fn a_partial_success_then_an_error_resends_only_the_rest() {
        let server = mock_server();
        mount_sequence(&server, BATCH_TARGET, vec![unprocessed(&[2]), throttled()]);
        mount_rest(&server, BATCH_TARGET, ok());
        let harness = Harness::new();

        let retries = finish(harness.write_batch(
            single_attempt_client(&server),
            puts(&[1, 2]),
            policy(Some(2)),
        ))
        .unwrap();

        assert_eq!(retries, 2, "one budget for unprocessed items and errors");
        let requests = received(&server, BATCH_TARGET);
        assert_eq!(requests.len(), 3);
        assert_eq!(item_count(&requests[0]), 2);
        assert_eq!(item_count(&requests[1]), 1);
        assert_eq!(write_items(&requests[2]), write_items(&requests[1]));
        assert_eq!(harness.metric("dynamodb_output_throttled_items_total"), 1.0);

        // The same with one retry less: the budget runs out on the error.
        let server = mock_server();
        mount_sequence(&server, BATCH_TARGET, vec![unprocessed(&[2]), throttled()]);
        mount_rest(&server, BATCH_TARGET, ok());
        let harness = Harness::new();
        let error = finish(harness.write_batch(
            single_attempt_client(&server),
            puts(&[1, 2]),
            policy(Some(1)),
        ))
        .unwrap_err()
        .to_string();
        assert!(error.contains("dropped 1 item(s)"), "{error}");
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 1.0);
        assert_eq!(harness.metric("dynamodb_output_retries_total"), 1.0);
    }

    #[test]
    fn a_partial_success_then_exhaustion_drops_only_the_rest() {
        let server = mock_server();
        mount_sequence(&server, BATCH_TARGET, vec![unprocessed(&[3])]);
        mount_rest(&server, BATCH_TARGET, unprocessed(&[3]));
        let harness = Harness::new();

        let error = finish(harness.write_batch(
            single_attempt_client(&server),
            puts(&[1, 2, 3]),
            policy(Some(2)),
        ))
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("dropped 1 item(s) after 2 BatchWriteItem retries"),
            "{error}"
        );
        assert!(error.contains("1 of 1 item(s) unprocessed"), "{error}");
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 1.0);
        assert_eq!(harness.metric("dynamodb_output_retries_total"), 2.0);
        assert_eq!(harness.metric("dynamodb_output_throttled_items_total"), 3.0);
    }

    #[test]
    fn a_cancelled_transaction_with_mixed_reasons_retries_the_survivors() {
        let server = mock_server();
        mount_sequence(
            &server,
            TRANSACT_TARGET,
            vec![cancelled(&[
                "ConditionalCheckFailed",
                "TransactionConflict",
                "None",
            ])],
        );
        mount_rest(&server, TRANSACT_TARGET, ok());
        let harness = Harness::new();

        let outcome = finish(harness.write_transact(
            single_attempt_client(&server),
            transact_puts(&[1, 2, 3]),
            policy(Some(3)),
        ))
        .unwrap();

        assert_eq!(outcome.retries, 1);
        assert_eq!(outcome.suppressed, 1);
        let requests = received(&server, TRANSACT_TARGET);
        assert_eq!(requests.len(), 2);
        assert_eq!(item_count(&requests[1]), 2);
        assert_eq!(
            harness.metric("dynamodb_output_condition_check_failures_total"),
            1.0
        );
        assert_eq!(harness.metric("dynamodb_output_retries_total"), 1.0);
        assert!(
            harness.publisher.reports()[0]
                .as_ref()
                .is_err_and(|cause| cause.contains("transient item error")),
            "{:?}",
            harness.publisher.reports()
        );
    }

    #[test]
    fn cancelled_transaction_variants_follow_the_policy() {
        // A permanent item error drops the whole transaction at once.
        let server = mock_server();
        mount_rest(
            &server,
            TRANSACT_TARGET,
            cancelled(&["ValidationError", "None"]),
        );
        let harness = Harness::new();
        let error = finish(harness.write_transact(
            single_attempt_client(&server),
            transact_puts(&[1, 2]),
            policy(None),
        ))
        .unwrap_err()
        .to_string();
        assert_eq!(received(&server, TRANSACT_TARGET).len(), 1);
        assert!(
            error.contains("[ValidationError]") && error.contains("dropped 2 item(s)"),
            "{error}"
        );
        assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 2.0);

        // Condition failures alone resubmit the rest at once: no retry, no health report.
        let server = mock_server();
        mount_sequence(
            &server,
            TRANSACT_TARGET,
            vec![cancelled(&["ConditionalCheckFailed", "None"])],
        );
        mount_rest(&server, TRANSACT_TARGET, ok());
        let harness = Harness::new();
        let (delay, gate) = Gate::new();
        let outcome = finish(harness.write_transact(
            single_attempt_client(&server),
            transact_puts(&[1, 2]),
            RetryPolicy::with_delay(Some(0), delay),
        ))
        .unwrap();
        assert_eq!((outcome.retries, outcome.suppressed), (0, 1));
        gate.no_retry_entered();
        let requests = received(&server, TRANSACT_TARGET);
        assert_eq!(requests.len(), 2);
        assert_eq!(item_count(&requests[1]), 1);
        assert_eq!(harness.metric("dynamodb_output_retries_total"), 0.0);
        assert_eq!(
            harness.metric("dynamodb_output_transact_write_failures_total"),
            0.0
        );
        assert_eq!(harness.publisher.reports(), vec![Ok(())]);

        // Missing reasons, a mismatched count, and all-`None` reasons retry
        // the whole request with backoff and the budget.
        for (name, response) in [
            ("missing", cancelled(&[])),
            ("mismatched", cancelled(&["None"])),
            ("all none", cancelled(&["None", "None"])),
        ] {
            let server = mock_server();
            mount_sequence(&server, TRANSACT_TARGET, vec![response]);
            mount_rest(&server, TRANSACT_TARGET, ok());
            let harness = Harness::new();
            let outcome = finish(harness.write_transact(
                single_attempt_client(&server),
                transact_puts(&[1, 2]),
                policy(Some(1)),
            ))
            .unwrap();
            assert_eq!(outcome.retries, 1, "{name}");
            let requests = received(&server, TRANSACT_TARGET);
            assert_eq!(requests.len(), 2, "{name}");
            assert_eq!(
                write_items(&requests[1]),
                write_items(&requests[0]),
                "{name}"
            );
            assert_eq!(
                harness.metric("dynamodb_output_transact_write_failures_total"),
                1.0,
                "{name}"
            );
            let reports = harness.publisher.reports();
            assert!(
                reports[0]
                    .as_ref()
                    .is_err_and(|cause| cause.contains("transaction cancelled")),
                "{name}: {reports:?}"
            );

            // The same cancellation forever exhausts a bounded budget.
            let server = mock_server();
            mount_rest(&server, TRANSACT_TARGET, cancelled(&["None", "None"]));
            let harness = Harness::new();
            let error = finish(harness.write_transact(
                single_attempt_client(&server),
                transact_puts(&[1, 2]),
                policy(Some(1)),
            ))
            .unwrap_err()
            .to_string();
            assert!(
                error.contains("after 1 TransactWriteItems retries"),
                "{error}"
            );
        }
    }

    #[test]
    fn concurrent_retries_keep_the_endpoint_unhealthy_until_the_last_one_lands() {
        let server = mock_server();
        for id in [1, 2] {
            mount(
                &server,
                on_item(BATCH_TARGET, id)
                    .respond_with(throttled())
                    .up_to_n_times(1),
            );
            mount(&server, on_item(BATCH_TARGET, id).respond_with(ok()));
        }
        let harness = Harness::new();
        let (delay_a, gate_a) = Gate::new();
        let (delay_b, gate_b) = Gate::new();
        let a = harness.write_batch(
            single_attempt_client(&server),
            puts(&[1]),
            RetryPolicy::with_delay(None, delay_a),
        );
        let b = harness.write_batch(
            single_attempt_client(&server),
            puts(&[2]),
            RetryPolicy::with_delay(None, delay_b),
        );
        assert_eq!(gate_a.entered(), 1);
        assert_eq!(gate_b.entered(), 1);
        assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 2.0);
        assert!(harness.publisher.reports().iter().all(Result::is_err));

        gate_b.release();
        finish(b).unwrap();
        assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 1.0);
        assert!(
            harness.publisher.reports().last().unwrap().is_err(),
            "a success cannot hide a retry loop"
        );

        gate_a.release();
        finish(a).unwrap();
        assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 0.0);
        assert_eq!(harness.publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn a_failure_beside_a_retry_loop_stays_unhealthy_until_the_loop_lands() {
        let server = mock_server();
        mount(
            &server,
            on_item(BATCH_TARGET, 1)
                .respond_with(throttled())
                .up_to_n_times(1),
        );
        mount(&server, on_item(BATCH_TARGET, 1).respond_with(ok()));
        mount(&server, on_item(BATCH_TARGET, 2).respond_with(throttled()));
        let harness = Harness::new();
        let (delay, gate) = Gate::new();
        let slow = harness.write_batch(
            single_attempt_client(&server),
            puts(&[1]),
            RetryPolicy::with_delay(None, delay),
        );
        assert_eq!(gate.entered(), 1);

        let doomed = finish(harness.write_batch(
            single_attempt_client(&server),
            puts(&[2]),
            policy(Some(0)),
        ));
        let error = doomed.unwrap_err().to_string();
        assert_eq!(harness.publisher.reports().last(), Some(&Err(error)));
        assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 1.0);

        gate.release();
        finish(slow).unwrap();
        assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 0.0);
        assert_eq!(harness.publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn a_terminal_failure_stays_unhealthy_until_a_later_success() {
        let server = mock_server();
        mount(&server, on_item(BATCH_TARGET, 1).respond_with(throttled()));
        mount(&server, on_item(BATCH_TARGET, 2).respond_with(ok()));
        let harness = Harness::new();

        finish(harness.write_batch(single_attempt_client(&server), puts(&[1]), policy(Some(1))))
            .unwrap_err();
        let reports = harness.publisher.reports();
        assert!(reports.last().unwrap().is_err(), "{reports:?}");
        sleep(Duration::from_millis(50));
        assert_eq!(
            harness.publisher.reports(),
            reports,
            "idle: nothing restores health"
        );

        finish(harness.write_batch(single_attempt_client(&server), puts(&[2]), policy(Some(1))))
            .unwrap();
        assert_eq!(harness.publisher.reports().last(), Some(&Ok(())));
    }

    #[test]
    fn unbounded_retries_end_on_shutdown_without_a_recovery_report() {
        for target in [BATCH_TARGET, TRANSACT_TARGET] {
            let server = mock_server();
            mount_rest(&server, target, throttled());
            let harness = Harness::new();
            let (delay, gate) = Gate::new();
            let policy = RetryPolicy::with_delay(None, delay);
            let handle: tokio::task::JoinHandle<anyhow::Result<()>> = if target == BATCH_TARGET {
                let chunk = harness.write_batch(single_attempt_client(&server), puts(&[1]), policy);
                TOKIO.spawn(async move { chunk.await.unwrap().map(|_| ()) })
            } else {
                let chunk = harness.write_transact(
                    single_attempt_client(&server),
                    transact_puts(&[1]),
                    policy,
                );
                TOKIO.spawn(async move { chunk.await.unwrap().map(|_| ()) })
            };

            // Past the default budget of ten, and the retries count as they start.
            for retry in 1..=12 {
                assert_eq!(gate.entered(), retry, "{target}");
                assert_eq!(
                    harness.metric("dynamodb_output_retries_total"),
                    (retry - 1) as f64
                );
                assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 1.0);
                gate.release();
            }
            assert_eq!(gate.entered(), 13);
            assert_eq!(harness.metric("dynamodb_output_retries_total"), 12.0);
            assert_eq!(received(&server, target).len(), 13);

            harness.shutdown.cancel();
            let error = finish(handle).unwrap_err().to_string();
            assert!(error.contains("shutting down"), "{target}: {error}");
            assert_eq!(harness.metric("dynamodb_output_retrying_chunks"), 0.0);
            assert_eq!(harness.metric("dynamodb_output_failed_items_total"), 0.0);
            assert!(
                harness.publisher.reports().iter().all(Result::is_err),
                "{target}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The endpoint and the controller against the mock DynamoDB: a retried write
// of every SQL type, health in the controller's statistics during an outage,
// and a controller stop during an outage that never ends.
// ---------------------------------------------------------------------------

/// A retried write resends the complete item, every supported SQL type and
/// the null included, and the endpoint reports its health on the way.
#[engine_test(requires(DynamoDbItems))]
pub fn dynamodb_retried_write_resends_every_sql_type(engine: &dyn ConnectorTestEngine) {
    use crate::integrated::dynamodb::health::test::RecordingPublisher;
    use mock_dynamodb::*;

    let server = server();
    // The endpoint's client keeps the SDK's three attempts: the fourth request
    // is the connector's retry.
    TOKIO.block_on(
        on(BATCH_TARGET)
            .respond_with(throttled())
            .up_to_n_times(3)
            .mount(&server),
    );
    TOKIO.block_on(on(BATCH_TARGET).respond_with(ok()).mount(&server));
    let publisher = RecordingPublisher::default();
    let metrics = Arc::new(DynamoDBOutputMetrics::default());
    let mut endpoint = DynamoDBOutputEndpoint::new_with_options(
        EndpointId::default(),
        "test_endpoint",
        &mock_config(&server),
        &Some(all_types_key_relation()),
        &all_types_value_relation(),
        Weak::new(),
        CancellationToken::new(),
        true,
        metrics.clone(),
        RetryPolicy::with_delay(Some(3), no_delay()),
        Box::new(publisher.clone()),
    )
    .unwrap();
    let batch = build_all_types_batch(engine, vec![(all_types_record(), 1)]);

    encode_endpoint_batch(&mut endpoint, &batch);

    let requests = requests(&server, BATCH_TARGET);
    assert_eq!(requests.len(), 4);
    assert!(
        requests
            .iter()
            .all(|request| write_items(request) == write_items(&requests[0]))
    );
    let item = &write_items(&requests[3])["tbl"][0]["PutRequest"]["Item"];
    let mut columns = item
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    columns.sort();
    let mut expected = all_types_value_relation()
        .fields
        .iter()
        .map(|field| field.name.name().to_string())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(columns, expected);
    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_retries_total"),
        1.0
    );
    assert_eq!(
        dynamodb_metric(&metrics, "dynamodb_output_failed_items_total"),
        0.0
    );
    let reports = publisher.reports();
    assert!(reports.len() == 2 && reports[0].is_err(), "{reports:?}");
    assert_eq!(reports[1], Ok(()));
}

/// A pipeline that reads `input_file` and writes to the mock DynamoDB at
/// `server_uri` with one worker. Errors go to `errors` instead of a panic.
fn mock_dynamodb_pipeline(
    engine: &dyn PipelineTestEngine,
    server_uri: &str,
    write_mode: DynamoDBWriteMode,
    max_retries: Option<u8>,
    max_concurrent_requests: usize,
    input_file: &NamedTempFile,
    errors: Arc<std::sync::Mutex<Vec<String>>>,
) -> Controller {
    let output_config = DynamoDBWriterConfig {
        write_mode,
        max_retries,
        max_concurrent_requests,
        batch_size: Some(10),
        ..endpoint_config("tbl".to_string(), Some(server_uri.to_string()), 1)
    };
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": input_file.path() }
                },
                "format": {
                    "name": "json",
                    "config": { "update_format": "raw" }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "index": "idx1",
                "transport": {
                    "name": "dynamodb_output",
                    "config": output_config
                }
            }
        }
    }))
    .unwrap();

    let circuit = TestCircuit::Keyed {
        key: key_relation(),
        relation: value_relation(),
        persistent_output_ids: vec![None],
        aliased: false,
    };
    engine
        .controller(
            &circuit,
            &config,
            Box::new(move |e, _| errors.lock().unwrap().push(e.to_string())),
        )
        .unwrap()
}

/// The health of `test_output1` as the controller's statistics show it:
/// `None` before the first report, then whether it is healthy.
fn output_health(controller: &Controller) -> Option<bool> {
    use feldera_types::adapter_stats::ConnectorHealthStatus;
    controller
        .status()
        .output_status()
        .values()
        .find(|status| status.endpoint_name == "test_output1")
        .and_then(|status| status.health.lock().unwrap().clone())
        .map(|health| matches!(health.status, ConnectorHealthStatus::Healthy))
}

fn output_metrics(controller: &Controller) -> Arc<dyn ConnectorMetrics> {
    controller
        .status()
        .output_status()
        .values()
        .find(|status| status.endpoint_name == "test_output1")
        .and_then(|status| status.custom_metrics.clone())
        .expect("the DynamoDB sink registers its metrics")
}

fn connector_metric(metrics: &dyn ConnectorMetrics, name: &str) -> f64 {
    metrics
        .metrics()
        .into_iter()
        .find(|(metric, ..)| *metric == name)
        .map(|(.., value)| value)
        .unwrap()
}

/// The controller shows the sink unhealthy while DynamoDB throttles it, and
/// healthy again once the writes land, with the SDK's own retries in place.
#[engine_test(requires(DynamoDbItems))]
pub fn dynamodb_output_reports_health_through_a_throttling_outage(engine: &dyn PipelineTestEngine) {
    use mock_dynamodb::*;

    let server = server();
    // More throttles than two SDK calls of three attempts: the connector
    // retries at least twice, and stays unhealthy through the SDK's backoff.
    TOKIO.block_on(
        on(BATCH_TARGET)
            .respond_with(throttled())
            .up_to_n_times(7)
            .mount(&server),
    );
    TOKIO.block_on(on(BATCH_TARGET).respond_with(ok()).mount(&server));
    let input_file = write_raw_json_input(&[
        record(1, "a", Some(10), "one"),
        record(2, "b", None, "two"),
        record(3, "c", Some(30), "three"),
    ]);
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let controller = mock_dynamodb_pipeline(
        engine,
        &server.uri(),
        DynamoDBWriteMode::Batch,
        None,
        16,
        &input_file,
        errors.clone(),
    );

    controller.start();
    wait(|| output_health(&controller) == Some(false), 60_000)
        .expect("the sink never reported itself unhealthy");
    assert_eq!(
        connector_metric(
            &*output_metrics(&controller),
            "dynamodb_output_retrying_chunks"
        ),
        1.0
    );
    wait(|| output_health(&controller) == Some(true), 120_000).expect("the sink never recovered");

    let requests = requests(&server, BATCH_TARGET);
    assert!(requests.len() >= 8, "{}", requests.len());
    assert!(requests.iter().all(|request| item_count(request) == 3));
    let metrics = output_metrics(&controller);
    assert!(connector_metric(&*metrics, "dynamodb_output_retries_total") >= 2.0);
    assert_eq!(
        connector_metric(&*metrics, "dynamodb_output_failed_items_total"),
        0.0
    );
    assert_eq!(
        connector_metric(&*metrics, "dynamodb_output_retrying_chunks"),
        0.0
    );
    controller.stop().unwrap();
    assert!(
        errors.lock().unwrap().is_empty(),
        "{:?}",
        errors.lock().unwrap()
    );
}

/// `Controller::stop` ends an outage that never ends: a chunk in an unbounded
/// retry loop, the worker blocked behind it on its write slot, and the output
/// thread blocked behind the worker.
fn stop_during_an_unbounded_outage(engine: &dyn PipelineTestEngine, write_mode: DynamoDBWriteMode) {
    use mock_dynamodb::*;

    let target = match write_mode {
        DynamoDBWriteMode::Batch => BATCH_TARGET,
        DynamoDBWriteMode::Transactional => TRANSACT_TARGET,
    };
    let server = server();
    TOKIO.block_on(on(target).respond_with(throttled()).mount(&server));
    // Three chunks of ten for one write slot: the first retries forever, the
    // worker waits for the slot with the second, and the output thread waits
    // for the worker.
    let records = (0..30)
        .map(|id| record(id, "x", Some(id as i64), "r"))
        .collect::<Vec<_>>();
    let input_file = write_raw_json_input(&records);
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let controller = mock_dynamodb_pipeline(
        engine,
        &server.uri(),
        write_mode,
        None,
        1,
        &input_file,
        errors.clone(),
    );

    controller.start();
    wait(|| output_health(&controller) == Some(false), 60_000)
        .expect("the sink never reported itself unhealthy");
    // Past the SDK's three attempts: the connector itself retried.
    wait(|| requests(&server, target).len() >= 4, 60_000).expect("the connector never retried");
    let metrics = output_metrics(&controller);
    assert_eq!(
        connector_metric(&*metrics, "dynamodb_output_write_chunks_total"),
        1.0,
        "the worker is blocked on its write slot"
    );
    assert_eq!(
        connector_metric(&*metrics, "dynamodb_output_retrying_chunks"),
        1.0
    );

    // Stop on another thread, so that a regression fails this test rather
    // than hanging the run.
    let stopper = std::thread::spawn(move || controller.stop());
    wait(|| stopper.is_finished(), 60_000).expect("the controller did not stop");
    stopper.join().unwrap().unwrap();
    wait(
        || connector_metric(&*metrics, "dynamodb_output_retrying_chunks") == 0.0,
        10_000,
    )
    .expect("the retry loop did not end");
    assert_eq!(
        connector_metric(&*metrics, "dynamodb_output_failed_items_total"),
        0.0
    );
}

#[engine_test(requires(DynamoDbItems))]
pub fn dynamodb_output_stops_during_an_unbounded_batch_outage(engine: &dyn PipelineTestEngine) {
    stop_during_an_unbounded_outage(engine, DynamoDBWriteMode::Batch);
}

#[engine_test(requires(DynamoDbItems))]
pub fn dynamodb_output_stops_during_an_unbounded_transactional_outage(
    engine: &dyn PipelineTestEngine,
) {
    stop_during_an_unbounded_outage(engine, DynamoDBWriteMode::Transactional);
}
