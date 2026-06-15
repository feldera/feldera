use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::sleep;
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use dbsp::OrdIndexedZSet;
use dbsp::circuit::tokio::TOKIO;
use dbsp::utils::Tup2;
use feldera_adapterlib::catalog::SerBatch;
use feldera_adapterlib::metrics::ConnectorMetrics;
use feldera_adapterlib::transport::OutputBatchType;
use feldera_macros::IsNone;
use feldera_sqllib::{
    ByteArray, Date, F32, F64, SqlDecimal, SqlString, Time, Timestamp, Uuid, Variant,
};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::transport::dynamodb::{DynamoDBWriteMode, DynamoDBWriterConfig};
use feldera_types::{
    deserialize_table_record, deserialize_without_context, serialize_struct, serialize_table_record,
};
use rand::Rng;
use rand::distributions::Alphanumeric;
use size_of::SizeOf;

use crate::catalog::RecordFormat;
use crate::controller::EndpointId;
use crate::format::Encoder;
use crate::static_compile::seroutput::SerBatchImpl;
use crate::test::TestStruct;

use super::helpers::make_client;
use super::metrics::DynamoDBOutputMetrics;
use super::output::{DynamoDBOutputEndpoint, DynamoDBWorker};

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestRecord {
    id: i32,
    sort: String,
    b: bool,
    i: Option<i64>,
    s: String,
}

deserialize_without_context!(TestRecord);

serialize_struct!(TestRecord()[5]{
    id["id"]: i32,
    sort["sort"]: String,
    b["b"]: bool,
    i["i"]: Option<i64>,
    s["s"]: String
});

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct TestKey {
    id: i32,
    sort: String,
}

deserialize_without_context!(TestKey);

serialize_struct!(TestKey()[2]{
    id["id"]: i32,
    sort["sort"]: String
});

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct AllTypesRecord {
    id: i32,
    boolean_: bool,
    tinyint_: i8,
    smallint_: i16,
    int_: i32,
    bigint_: i64,
    decimal_: SqlDecimal<38, 10>,
    float_: F32,
    double_: F64,
    varchar_: SqlString,
    time_: Time,
    date_: Date,
    timestamp_: Timestamp,
    variant_: Variant,
    uuid_: Uuid,
    varbinary_: ByteArray,
    struct_: TestStruct,
    string_array_: Vec<SqlString>,
    struct_array_: Vec<TestStruct>,
    map_: BTreeMap<SqlString, TestStruct>,
    nullable_: Option<i64>,
}

serialize_table_record!(AllTypesRecord[21]{
    id["id"]: i32,
    boolean_["boolean_"]: bool,
    tinyint_["tinyint_"]: i8,
    smallint_["smallint_"]: i16,
    int_["int_"]: i32,
    bigint_["bigint_"]: i64,
    decimal_["decimal_"]: SqlDecimal,
    float_["float_"]: F32,
    double_["double_"]: F64,
    varchar_["varchar_"]: SqlString,
    time_["time_"]: Time,
    date_["date_"]: Date,
    timestamp_["timestamp_"]: Timestamp,
    variant_["variant_"]: Variant,
    uuid_["uuid_"]: Uuid,
    varbinary_["varbinary_"]: ByteArray,
    struct_["struct_"]: TestStruct,
    string_array_["string_array_"]: Vec<SqlString>,
    struct_array_["struct_array_"]: Vec<TestStruct>,
    map_["map_"]: BTreeMap<SqlString, TestStruct>,
    nullable_["nullable_"]: Option<i64>
});

deserialize_table_record!(AllTypesRecord["AllTypesRecord", Variant, 21] {
    (id, "id", false, i32, |_| None),
    (boolean_, "boolean_", false, bool, |_| None),
    (tinyint_, "tinyint_", false, i8, |_| None),
    (smallint_, "smallint_", false, i16, |_| None),
    (int_, "int_", false, i32, |_| None),
    (bigint_, "bigint_", false, i64, |_| None),
    (decimal_, "decimal_", false, SqlDecimal<38, 10>, |_| None),
    (float_, "float_", false, F32, |_| None),
    (double_, "double_", false, F64, |_| None),
    (varchar_, "varchar_", false, SqlString, |_| None),
    (time_, "time_", false, Time, |_| None),
    (date_, "date_", false, Date, |_| None),
    (timestamp_, "timestamp_", false, Timestamp, |_| None),
    (variant_, "variant_", false, Variant, |_| None),
    (uuid_, "uuid_", false, Uuid, |_| None),
    (varbinary_, "varbinary_", false, ByteArray, |_| None),
    (struct_, "struct_", false, TestStruct, |_| None),
    (string_array_, "string_array_", false, Vec<SqlString>, |_| None),
    (struct_array_, "struct_array_", false, Vec<TestStruct>, |_| None),
    (map_, "map_", false, BTreeMap<SqlString, TestStruct>, |_| None),
    (nullable_, "nullable_", true, Option<i64>, |_| None)
});

#[derive(
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    Hash,
    SizeOf,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    IsNone,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct AllTypesKey {
    id: i32,
}

deserialize_without_context!(AllTypesKey);

serialize_struct!(AllTypesKey()[1]{
    id["id"]: i32
});

fn key_relation() -> Relation {
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

fn value_relation() -> Relation {
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

fn all_types_key_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("all_types_idx", false),
        fields: vec![Field::new("id".into(), ColumnType::int(false))],
        materialized: false,
        properties: BTreeMap::new(),
        primary_key: None,
    }
}

fn all_types_value_relation() -> Relation {
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
                ColumnType::structure(false, &TestStruct::schema()),
            ),
            Field::new(
                "string_array_".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new(
                "struct_array_".into(),
                ColumnType::array(false, ColumnType::structure(false, &TestStruct::schema())),
            ),
            Field::new(
                "map_".into(),
                ColumnType::map(
                    false,
                    ColumnType::varchar(false),
                    ColumnType::structure(false, &TestStruct::schema()),
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
        max_buffer_size_bytes: 1024 * 1024,
        max_concurrent_requests: 16,
        threads: 1,
        allow_cross_step_write_overlap: false,
        max_retries: Some(10),
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
        max_buffer_size_bytes: 1024 * 1024,
        max_concurrent_requests: 16,
        threads,
        allow_cross_step_write_overlap: false,
        max_retries: Some(10),
    }
}

/// Creates a client pointed at a non-existent endpoint. Writes will fail,
/// but encoding tests never await the spawned write tasks.
fn dummy_client() -> Client {
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
    config.max_buffer_size_bytes = max_buffer_size_bytes;
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

fn empty_endpoint_with_counter(records_written: Arc<AtomicU64>) -> DynamoDBOutputEndpoint {
    DynamoDBOutputEndpoint {
        endpoint_id: EndpointId::default(),
        endpoint_name: "test_dynamodb_endpoint".to_string(),
        config: config(25),
        controller: Weak::new(),
        handles: Vec::new(),
        records_written,
        bytes_written: Arc::new(AtomicU64::new(0)),
        rows_since_last_log: 0,
        bytes_since_last_log: 0,
        retries_since_last_log: 0,
        batches_since_last_log: 0,
        last_throughput_log: Instant::now(),
    }
}

fn dynamodb_metric(metrics: &DynamoDBOutputMetrics, name: &str) -> f64 {
    metrics
        .metrics()
        .into_iter()
        .find_map(|(metric_name, _, _, value)| (metric_name == name).then_some(value))
        .unwrap_or_else(|| panic!("missing DynamoDB metric {name}"))
}

fn build_batch(tuples: Vec<(TestRecord, i64)>) -> Arc<dyn SerBatch> {
    let tuples: Vec<_> = tuples
        .into_iter()
        .map(|(record, weight)| {
            Tup2(
                Tup2(
                    TestKey {
                        id: record.id,
                        sort: record.sort.clone(),
                    },
                    record,
                ),
                weight,
            )
        })
        .collect();
    let zset = OrdIndexedZSet::from_tuples((), tuples);
    Arc::new(SerBatchImpl::<_, TestKey, TestRecord>::new(zset))
}

fn build_all_types_batch(tuples: Vec<(AllTypesRecord, i64)>) -> Arc<dyn SerBatch> {
    let tuples: Vec<_> = tuples
        .into_iter()
        .map(|(record, weight)| Tup2(Tup2(AllTypesKey { id: record.id }, record), weight))
        .collect();
    let zset = OrdIndexedZSet::from_tuples((), tuples);
    Arc::new(SerBatchImpl::<_, AllTypesKey, AllTypesRecord>::new(zset))
}

fn record(id: i32, sort: &str, i: Option<i64>, s: &str) -> TestRecord {
    TestRecord {
        id,
        sort: sort.to_string(),
        b: id % 2 == 0,
        i,
        s: s.to_string(),
    }
}

fn all_types_record() -> AllTypesRecord {
    let mut map = BTreeMap::new();
    map.insert(
        SqlString::from_ref("nested"),
        TestStruct {
            id: 7,
            b: true,
            i: Some(70),
            s: "inside-map".to_string(),
        },
    );

    AllTypesRecord {
        id: 42,
        boolean_: true,
        tinyint_: -8,
        smallint_: 16,
        int_: 32,
        bigint_: 64,
        decimal_: SqlDecimal::<38, 10>::new(12345, 3).unwrap(),
        float_: F32::new(1.5),
        double_: F64::new(2.25),
        varchar_: SqlString::from_ref("hello"),
        time_: Time::from_time(chrono::NaiveTime::from_hms_micro_opt(1, 2, 3, 456).unwrap()),
        date_: Date::from_date(chrono::NaiveDate::from_ymd_opt(2024, 5, 25).unwrap()),
        timestamp_: Timestamp::from_naiveDateTime(
            chrono::NaiveDate::from_ymd_opt(2024, 5, 25)
                .unwrap()
                .and_hms_micro_opt(1, 2, 3, 456)
                .unwrap(),
        ),
        variant_: Variant::String(SqlString::from_ref("variant-value")),
        uuid_: uuid::uuid!("550e8400-e29b-41d4-a716-446655440000").into(),
        varbinary_: ByteArray::from_vec(vec![1, 2, 3, 4]),
        struct_: TestStruct {
            id: 5,
            b: false,
            i: Some(50),
            s: "inside-struct".to_string(),
        },
        string_array_: vec![SqlString::from_ref("a"), SqlString::from_ref("b")],
        struct_array_: vec![TestStruct {
            id: 6,
            b: true,
            i: None,
            s: "inside-array".to_string(),
        }],
        map_: map,
        nullable_: None,
    }
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

fn encode_batch(worker: &mut DynamoDBWorker, batch: &Arc<dyn SerBatch>) {
    let reader = batch.clone().arc_as_batch_reader();
    let mut cursor = reader.cursor(RecordFormat::DynamoDB).unwrap();
    worker.batch_start_inner();
    worker.encode_cursor(&mut *cursor).unwrap();
    worker.batch_end_inner().unwrap();
}

fn stage_batch(worker: &mut DynamoDBWorker, batch: &Arc<dyn SerBatch>) {
    let reader = batch.clone().arc_as_batch_reader();
    let mut cursor = reader.cursor(RecordFormat::DynamoDB).unwrap();
    worker.batch_start_inner();
    worker.encode_cursor(&mut *cursor).unwrap();
}

fn encode_endpoint_batch(endpoint: &mut DynamoDBOutputEndpoint, batch: &Arc<dyn SerBatch>) {
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint
        .encode(batch.clone().arc_as_batch_reader())
        .unwrap();
    endpoint.consumer().batch_end();
}

fn attr_s<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
    item.get(field).unwrap().as_s().unwrap()
}

fn attr_n<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
    item.get(field).unwrap().as_n().unwrap()
}

#[test]
fn encoder_insert_as_put_item() {
    let mut worker = worker(25);
    let batch = build_batch(vec![(record(1, "a", Some(10), "inserted"), 1)]);
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();
    assert_eq!(attr_n(item, "id"), "1");
    assert_eq!(attr_s(item, "sort"), "a");
    assert_eq!(item.get("b").unwrap().as_bool().unwrap(), &false);
    assert_eq!(attr_n(item, "i"), "10");
    assert_eq!(attr_s(item, "s"), "inserted");
}

#[test]
fn encoder_delete_as_delete_item_key_only() {
    let mut worker = worker(25);
    let batch = build_batch(vec![(record(2, "b", Some(20), "deleted"), -1)]);
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].put_request().is_none());
    let key = worker.pending[0].delete_request().unwrap().key();
    assert_eq!(key.len(), 2);
    assert_eq!(attr_n(key, "id"), "2");
    assert_eq!(attr_s(key, "sort"), "b");
}

#[test]
fn encoder_upsert_as_put_item_with_new_value() {
    let mut worker = worker(25);
    let batch = build_batch(vec![
        (record(3, "c", Some(30), "old"), -1),
        (record(3, "c", None, "new"), 1),
    ]);
    stage_batch(&mut worker, &batch);
    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();
    assert_eq!(attr_n(item, "id"), "3");
    assert_eq!(attr_s(item, "sort"), "c");
    assert_eq!(item.get("i").unwrap().as_null().unwrap(), &true);
    assert_eq!(attr_s(item, "s"), "new");
}

#[test]
fn encoder_stages_until_batch_end() {
    let mut worker = worker(100);
    let batch = build_batch(
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

#[test]
fn encoder_flushes_when_buffer_limit_is_reached() {
    // max_buffer_size_bytes=1 forces a flush after the first record.
    let mut worker = worker_with_max_buffer_size(25, 1);
    let batch = build_batch(vec![(record(9, "early", Some(90), "flushed"), 1)]);
    stage_batch(&mut worker, &batch);
    // pending is empty because the record was flushed mid-encode.
    // (`records_written` can't be checked here: the dummy client's writes fail,
    // and the counter only advances on a successful write. End-to-end counting
    // is covered by the `dynamodb_progress_counter_*` integration tests.)
    assert!(worker.pending.is_empty());
}

#[test]
fn encoder_flushes_when_record_threshold_is_reached() {
    // threshold = batch_size(2) × max_concurrent_requests(2) = 4
    let mut config = config(2);
    config.max_concurrent_requests = 2;
    config.max_buffer_size_bytes = usize::MAX;
    let mut worker = worker_with_config(config);
    let batch = build_batch(
        (0..4)
            .map(|id| (record(id, "threshold", Some(id as i64), "r"), 1))
            .collect(),
    );
    stage_batch(&mut worker, &batch);
    // Reaching the record threshold drains `pending` via a mid-encode flush.
    assert!(worker.pending.is_empty());
}

#[test]
fn encoder_empty_batch_single_thread() {
    let mut worker = worker(25);
    encode_batch(&mut worker, &build_batch(Vec::new()));
    assert!(worker.pending.is_empty());
    assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
}

#[test]
fn encoder_empty_batch_multi_thread() {
    let batch = build_batch(Vec::new());
    for _ in 0..4 {
        let mut worker = worker(25);
        encode_batch(&mut worker, &batch);
        assert!(worker.pending.is_empty());
        assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn encoder_multiple_batches_insert_upsert_delete() {
    let mut worker = worker(25);

    stage_batch(
        &mut worker,
        &build_batch(vec![(record(11, "sequence", Some(110), "insert"), 1)]),
    );
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].put_request().is_some());

    stage_batch(
        &mut worker,
        &build_batch(vec![
            (record(11, "sequence", Some(110), "insert"), -1),
            (record(11, "sequence", Some(111), "upsert"), 1),
        ]),
    );
    assert_eq!(worker.pending.len(), 1);
    assert_eq!(
        attr_s(worker.pending[0].put_request().unwrap().item(), "s"),
        "upsert"
    );

    stage_batch(
        &mut worker,
        &build_batch(vec![(record(11, "sequence", Some(111), "upsert"), -1)]),
    );
    assert_eq!(worker.pending.len(), 1);
    assert!(worker.pending[0].delete_request().is_some());
}

#[test]
fn encoder_non_unique_key_is_skipped_without_failing_batch() {
    let mut worker = worker(25);
    let batch = build_batch(vec![
        (record(1, "duplicate", Some(10), "first"), 1),
        (record(1, "duplicate", Some(11), "second"), 1),
        (record(2, "unique", Some(20), "kept"), 1),
    ]);

    stage_batch(&mut worker, &batch);

    assert_eq!(worker.pending.len(), 1);
    let item = worker.pending[0].put_request().unwrap().item();
    assert_eq!(attr_n(item, "id"), "2");
    assert_eq!(attr_s(item, "sort"), "unique");
    assert_eq!(attr_s(item, "s"), "kept");
}

#[test]
fn encoder_non_unique_key_updates_connector_metric() {
    let mut worker = worker(25);
    let batch = build_batch(vec![
        (record(1, "duplicate", Some(10), "first"), 1),
        (record(1, "duplicate", Some(11), "second"), 1),
        (record(2, "unique", Some(20), "kept"), 1),
    ]);

    stage_batch(&mut worker, &batch);

    assert_eq!(
        dynamodb_metric(
            &worker.metrics,
            "dynamodb_output_duplicate_keys_skipped_total"
        ),
        1.0
    );
}

#[test]
fn encoder_drops_oversized_item_keeping_the_rest() {
    let mut worker = worker(25);
    // The middle record's `s` attribute alone exceeds the 400 KB item-size
    // limit; the other two are ordinary.
    let oversized_value = "x".repeat(410 * 1024);
    let batch = build_batch(vec![
        (record(1, "a", Some(10), "one"), 1),
        (record(2, "b", Some(20), &oversized_value), 1),
        (record(3, "c", Some(30), "three"), 1),
    ]);

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

#[test]
fn encoder_oversized_item_updates_connector_metric() {
    let mut worker = worker(25);
    let oversized_value = "x".repeat(410 * 1024);
    let batch = build_batch(vec![(record(1, "a", Some(10), &oversized_value), 1)]);

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
#[test]
fn worker_surfaces_write_failure_at_batch_end() {
    let mut config = config(25);
    config.max_retries = Some(0);
    let mut worker = DynamoDBWorker::with_client(
        dummy_client(),
        "test",
        &config,
        &key_relation(),
        &value_relation(),
    );

    let batch = build_batch(vec![(record(1, "a", Some(10), "one"), 1)]);
    let reader = batch.arc_as_batch_reader();
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

#[test]
fn encoder_all_supported_sql_types() {
    let mut worker = all_types_worker();
    let batch = build_all_types_batch(vec![(all_types_record(), 1)]);
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

fn encoded_variant_attribute(variant: Variant) -> AttributeValue {
    let mut record = all_types_record();
    record.variant_ = variant;

    let mut worker = all_types_worker();
    let batch = build_all_types_batch(vec![(record, 1)]);
    stage_batch(&mut worker, &batch);
    worker.pending[0]
        .put_request()
        .unwrap()
        .item()
        .get("variant_")
        .unwrap()
        .clone()
}

#[test]
fn encoder_variant_decimal_serializes_as_number() {
    let variant = encoded_variant_attribute(Variant::SqlDecimal((12345, 2)));

    assert_eq!(variant.as_n().unwrap(), "123.45");
}

#[test]
fn encoder_variant_map_preserves_private_number_key() {
    let variant = encoded_variant_attribute(Variant::Map(Arc::new(BTreeMap::from([
        (
            Variant::String(SqlString::from_ref("$serde_json::private::Number")),
            Variant::String(SqlString::from_ref("not-a-number-token")),
        ),
        (
            Variant::String(SqlString::from_ref("other")),
            Variant::Int(1),
        ),
    ]))));
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

#[test]
fn encoder_variant_list_recursively_serializes_numbers() {
    let variant = encoded_variant_attribute(Variant::Array(Arc::new(vec![
        Variant::SqlDecimal((12345, 2)),
        Variant::Map(Arc::new(BTreeMap::from([(
            Variant::String(SqlString::from_ref("nested")),
            Variant::SqlDecimal((6789, 3)),
        )]))),
    ])));
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

#[test]
fn encoder_split_cursor_partitions_encode_disjoint_key_ranges() {
    use feldera_adapterlib::catalog::SplitCursorBuilder;

    let batch = build_batch(
        (0..10)
            .map(|id| (record(id, "split", Some(id as i64), "r"), 1))
            .collect(),
    );
    let reader = batch.arc_as_batch_reader();
    let mut bounds = reader.keys_factory().default_box();
    reader.partition_keys(3, &mut *bounds);

    let mut all_requests = Vec::new();
    for i in 0..=bounds.len() {
        let Some(cursor_builder) =
            SplitCursorBuilder::from_bounds(reader.clone(), &*bounds, i, RecordFormat::DynamoDB)
        else {
            continue;
        };
        let mut worker = worker(100);
        let mut cursor = cursor_builder.build();
        worker.batch_start_inner();
        worker.encode_cursor(&mut cursor).unwrap();
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

#[test]
fn encoder_composite_key_delete_excludes_non_key_fields() {
    // A delete request must carry exactly the two key columns. Value-only
    // fields ("b", "i", "s") must not appear; including them would be rejected
    // by DynamoDB as an invalid key.
    let mut worker = worker(25);
    let batch = build_batch(vec![(record(5, "alpha", Some(50), "composite"), -1)]);
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

fn dynamodb_endpoint_url() -> Option<String> {
    const DEFAULT_DYNAMODB_ENDPOINT: &str = "http://127.0.0.1:8000";

    Some(
        std::env::var("DYNAMODB_ENDPOINT")
            .ok()
            .filter(|endpoint| !endpoint.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_DYNAMODB_ENDPOINT.to_string()),
    )
}

fn dynamodb_client(endpoint_url: Option<&str>) -> Client {
    make_client(&endpoint_config(
        "unused".to_string(),
        endpoint_url.map(str::to_string),
        1,
    ))
}

fn wait_for_dynamodb(client: &Client) {
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

fn test_table_name(suffix: &str) -> String {
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
struct TempDynamoTable {
    client: Client,
    name: String,
}

impl TempDynamoTable {
    fn new(client: &Client, name: &str, sort_key: bool) -> Self {
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

fn dynamodb_endpoint(
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
        true,
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
        true,
    )
    .unwrap()
}

fn expected_all_types_item() -> HashMap<String, AttributeValue> {
    let mut worker = all_types_worker();
    let batch = build_all_types_batch(vec![(all_types_record(), 1)]);
    stage_batch(&mut worker, &batch);
    worker.pending[0].put_request().unwrap().item().clone()
}

#[test]
fn dynamodb_insert() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("insert");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(1, "a", Some(10), "one"), 1),
            (record(2, "b", Some(20), "two"), 1),
        ]),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    assert_eq!(attr_n(&rows[0], "id"), "1");
    assert_eq!(attr_s(&rows[0], "s"), "one");
    assert_eq!(attr_n(&rows[1], "id"), "2");
    assert_eq!(attr_s(&rows[1], "s"), "two");
}

#[test]
fn dynamodb_upsert() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("upsert");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![(record(2, "b", Some(20), "two"), 1)]),
    );
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(2, "b", Some(20), "two"), -1),
            (record(2, "b", None, "two-updated"), 1),
        ]),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(attr_n(&rows[0], "id"), "2");
    assert_eq!(attr_s(&rows[0], "sort"), "b");
    assert_eq!(rows[0].get("i").unwrap().as_null().unwrap(), &true);
    assert_eq!(attr_s(&rows[0], "s"), "two-updated");
}

#[test]
fn dynamodb_delete() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("delete");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(1, "a", Some(10), "one"), 1),
            (record(3, "c", Some(30), "three"), 1),
        ]),
    );
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![(record(1, "a", Some(10), "one"), -1)]),
    );

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(attr_n(&rows[0], "id"), "3");
    assert_eq!(attr_s(&rows[0], "sort"), "c");
    assert_eq!(attr_s(&rows[0], "s"), "three");
}

#[test]
fn dynamodb_all_supported_sql_types_round_trip() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("all_types");
    let _table_guard = TempDynamoTable::new(&client, &table, false);
    let mut endpoint = dynamodb_all_types_endpoint(&table, endpoint_url.as_deref());
    let batch = build_all_types_batch(vec![(all_types_record(), 1)]);

    encode_endpoint_batch(&mut endpoint, &batch);

    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], expected_all_types_item());
}

/// Encodes a batch holding two normal-sized records and one record that exceeds
/// DynamoDB's 400 KB item-size limit, then verifies that the oversized record is
/// dropped while the normal records are written. The three records share a
/// single write chunk, so this also confirms the oversized record does not
/// poison the chunk it travels in: it is never sent and therefore never
/// retried.
fn check_oversized_item_dropped(write_mode: DynamoDBWriteMode) {
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
        true,
        metrics.clone(),
    )
    .unwrap();

    // The middle record's `s` attribute alone is larger than the 400 KB limit;
    // the other two are ordinary.
    let oversized_value = "x".repeat(410 * 1024);
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(1, "a", Some(10), "one"), 1),
            (record(2, "b", Some(20), &oversized_value), 1),
            (record(3, "c", Some(30), "three"), 1),
        ]),
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

#[test]
fn dynamodb_oversized_item_dropped_batch_mode() {
    check_oversized_item_dropped(DynamoDBWriteMode::Batch);
}

#[test]
fn dynamodb_oversized_item_dropped_transactional_mode() {
    check_oversized_item_dropped(DynamoDBWriteMode::Transactional);
}

fn records_written(endpoint: &DynamoDBOutputEndpoint) -> u64 {
    endpoint.records_written.load(Ordering::Relaxed)
}

fn dynamodb_endpoint_flush_every(
    max_buffer_size_bytes: usize,
    threads: usize,
    table: &str,
    endpoint_url: Option<&str>,
) -> DynamoDBOutputEndpoint {
    let mut config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), threads);
    config.max_buffer_size_bytes = max_buffer_size_bytes;
    DynamoDBOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        true,
    )
    .unwrap()
}

// Integration test: an empty batch completes without error and writes nothing.
#[test]
fn dynamodb_empty_batch() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("empty");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    encode_endpoint_batch(&mut endpoint, &build_batch(vec![]));

    assert_eq!(scan_table(&client, &table).len(), 0);
}

// Integration test: insert → upsert → delete across multiple successive batches.
#[test]
fn dynamodb_multiple_batches() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("multi");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

    // Batch 1: insert two records.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(1, "a", Some(10), "one"), 1),
            (record(2, "b", Some(20), "two"), 1),
        ]),
    );
    assert_eq!(scan_table(&client, &table).len(), 2);

    // Batch 2: upsert record 1.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(1, "a", Some(10), "one"), -1),
            (record(1, "a", Some(99), "one-updated"), 1),
        ]),
    );
    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 2);
    assert_eq!(attr_n(&rows[0], "i"), "99");
    assert_eq!(attr_s(&rows[0], "s"), "one-updated");

    // Batch 3: delete record 2.
    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![(record(2, "b", Some(20), "two"), -1)]),
    );
    let rows = scan_table(&client, &table);
    assert_eq!(rows.len(), 1);
    assert_eq!(attr_n(&rows[0], "id"), "1");
}

// Integration test: with write concurrency capped at a single in-flight request
// and a flush forced after every record, all records still land. This stresses
// the semaphore + JoinSet backpressure path: each flush must block on the lone
// permit and drain the previous in-flight write before spawning the next.
fn backpressure_writes_all_records(threads: usize) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("backpressure");
    let _table_guard = TempDynamoTable::new(&client, &table, true);

    let mut config = endpoint_config(table.clone(), endpoint_url.clone(), threads);
    config.max_buffer_size_bytes = 1; // flush after every record
    config.max_concurrent_requests = 1; // a single in-flight write per worker
    let mut endpoint = DynamoDBOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &config,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        true,
    )
    .unwrap();

    let num_records = 200i32;
    let batch = build_batch(
        (0..num_records)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );
    encode_endpoint_batch(&mut endpoint, &batch);

    assert_eq!(scan_table(&client, &table).len(), num_records as usize);
    // Counter resets to 0 at the end of every batch.
    assert_eq!(records_written(&endpoint), 0);
}

#[test]
fn dynamodb_backpressure_single_thread() {
    backpressure_writes_all_records(1);
}

#[test]
fn dynamodb_backpressure_multi_thread() {
    backpressure_writes_all_records(4);
}

// Progress counter: starts at zero, and resets to zero after every batch.
fn progress_basic(threads: usize) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_basic");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(threads, &table, endpoint_url.as_deref());
    let batch = build_batch(
        (0..100i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );

    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written(&endpoint), 0);
    endpoint
        .encode(batch.clone().arc_as_batch_reader())
        .unwrap();
    endpoint.consumer().batch_end();
    // Counter resets to 0 at the end of every batch.
    assert_eq!(records_written(&endpoint), 0);

    assert_eq!(scan_table(&client, &table).len(), 100);
}

#[test]
fn dynamodb_progress_counter_single_thread() {
    progress_basic(1);
}

#[test]
fn dynamodb_progress_counter_multi_thread() {
    progress_basic(3);
}

// Progress counter: an empty batch leaves the counter at zero throughout.
#[test]
fn dynamodb_progress_counter_empty_batch() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_empty");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint
        .encode(build_batch(vec![]).arc_as_batch_reader())
        .unwrap();
    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

// Progress counter: advances during encode when mid-batch flushes occur.
fn progress_advances_mid_batch(threads: usize) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_mid");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    // max_buffer_size_bytes=1 forces a flush after every single encoded record.
    let mut endpoint = dynamodb_endpoint_flush_every(1, threads, &table, endpoint_url.as_deref());
    let num_records = 50usize;
    let batch = build_batch(
        (0..num_records as i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint
        .encode(batch.clone().arc_as_batch_reader())
        .unwrap();

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

#[test]
fn dynamodb_progress_counter_advances_mid_batch_single_thread() {
    progress_advances_mid_batch(1);
}

#[test]
fn dynamodb_progress_counter_advances_mid_batch_multi_thread() {
    progress_advances_mid_batch(3);
}

// Progress counter: batch_start resets a stale counter from a prior batch.
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

// Integration test: two rows sharing the same hash key are stored as separate rows
// (disambiguated by the sort key), and deleting one leaves the other untouched.
#[test]
fn dynamodb_composite_key_same_hash_different_sort() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("composite");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![
            (record(1, "first", Some(10), "row-one"), 1),
            (record(1, "second", Some(20), "row-two"), 1),
        ]),
    );

    assert_eq!(
        scan_table(&client, &table).len(),
        2,
        "both rows with id=1 should be stored separately by sort key"
    );

    encode_endpoint_batch(
        &mut endpoint,
        &build_batch(vec![(record(1, "first", Some(10), "row-one"), -1)]),
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

// Progress counter: resets to zero between successive batches.
#[test]
fn dynamodb_progress_counter_multiple_batches() {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    wait_for_dynamodb(&client);

    let table = test_table_name("progress_multi");
    let _table_guard = TempDynamoTable::new(&client, &table, true);
    let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

    let batch1 = build_batch(
        (0..50i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );
    encode_endpoint_batch(&mut endpoint, &batch1);
    assert_eq!(records_written(&endpoint), 0);

    let batch2 = build_batch(
        (50..80i32)
            .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
            .collect(),
    );
    encode_endpoint_batch(&mut endpoint, &batch2);
    assert_eq!(records_written(&endpoint), 0);

    assert_eq!(scan_table(&client, &table).len(), 80);
}
