//! The DynamoDB output connector: how the worker encodes output batches as DynamoDB items,
//! buffers them, and reports oversized items, duplicate keys, and write failures; and, against
//! DynamoDB Local, the items that a table holds after inserts, upserts, deletes, conditional
//! writes, and buffered pipelines.

use feldera_types::size::ConfigSize;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::thread::sleep;
use std::time::Duration;

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use feldera_adapterlib::metrics::ConnectorMetrics;
use feldera_adapterlib::test::{ConnectorTestEngine, Updates, wait};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_storage::tokio::TOKIO;
use feldera_types::config::PipelineConfig;
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::transport::dynamodb::{DynamoDBWriteMode, DynamoDBWriterConfig};
use rand::Rng;
use rand::distributions::Alphanumeric;
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

use super::helpers::make_client;
use super::metrics::DynamoDBOutputMetrics;
use super::output::{DynamoDBOutputEndpoint, DynamoDBWorker};
use crate::Controller;
use crate::catalog::{RecordFormat, SerBatchReader};
use crate::controller::EndpointId;
use crate::format::Encoder;
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::{published_schema, read, relations};

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

pub fn encoder_empty_batch_single_thread(engine: &dyn ConnectorTestEngine) {
    let mut worker = worker(25);
    encode_batch(&mut worker, &build_batch(engine, Vec::new()));
    assert!(worker.pending.is_empty());
    assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
}

pub fn encoder_empty_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    let batch = build_batch(engine, Vec::new());
    for _ in 0..4 {
        let mut worker = worker(25);
        encode_batch(&mut worker, &batch);
        assert!(worker.pending.is_empty());
        assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
    }
}

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

pub fn encoder_variant_decimal_serializes_as_number(engine: &dyn ConnectorTestEngine) {
    let variant = encoded_variant_attribute(engine, json!(123.45));

    assert_eq!(variant.as_n().unwrap(), "123.45");
}

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

pub fn dynamodb_oversized_item_dropped_batch_mode(engine: &dyn ConnectorTestEngine) {
    check_oversized_item_dropped(engine, DynamoDBWriteMode::Batch);
}

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

pub fn dynamodb_backpressure_single_thread(engine: &dyn ConnectorTestEngine) {
    backpressure_writes_all_records(engine, 1);
}

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

pub fn dynamodb_progress_counter_single_thread(engine: &dyn ConnectorTestEngine) {
    progress_basic(engine, 1);
}

pub fn dynamodb_progress_counter_multi_thread(engine: &dyn ConnectorTestEngine) {
    progress_basic(engine, 3);
}

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

pub fn dynamodb_progress_counter_advances_mid_batch_single_thread(
    engine: &dyn ConnectorTestEngine,
) {
    progress_advances_mid_batch(engine, 1);
}

pub fn dynamodb_progress_counter_advances_mid_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    progress_advances_mid_batch(engine, 3);
}

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
