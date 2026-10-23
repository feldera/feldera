//! The Postgres output connector on output batches: the rows that it writes to a table through
//! inserts, updates, and deletes, on one worker thread or several, with the extra columns that
//! a command sets, and the progress counter along the way.
//!
//! This module also holds the fixtures that the output module's own tests share with it.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use chrono::NaiveDateTime;
use feldera_adapterlib::test::{ConnectorTestEngine, Updates};
use feldera_adapterlib::transport::{OutputBatchType, OutputEndpoint};
use feldera_types::program_schema::{ColumnType, Field, Relation, SqlIdentifier};
use feldera_types::size::ConfigSize;
use feldera_types::transport::postgres::{
    PostgresTlsConfig, PostgresWriteMode, PostgresWriterConfig,
};
use postgres::NoTls;
use serde_json::{Value, json};

use super::PostgresOutputEndpoint;
use crate::catalog::SerBatchReader;
use crate::controller::EndpointId;
use crate::format::Encoder;
use crate::test::{published_schema, read};

pub fn key_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("postgres_idx", false),
        fields: vec![Field::new("id".into(), ColumnType::int(false))],
        materialized: false,
        properties: BTreeMap::new(),
        primary_key: None,
    }
}

pub fn value_relation() -> Relation {
    Relation {
        name: SqlIdentifier::new("postgres_view", false),
        fields: vec![
            Field::new("id".into(), ColumnType::int(false)),
            Field::new("b".into(), ColumnType::boolean(false)),
            Field::new("i".into(), ColumnType::bigint(true)),
            Field::new("s".into(), ColumnType::varchar(false)),
        ],
        materialized: true,
        properties: BTreeMap::new(),
        primary_key: Some(vec!["id".into()]),
    }
}

pub(super) fn postgres_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
}

pub(super) fn pg_client() -> postgres::Client {
    postgres::Client::connect(&postgres_url(), NoTls).expect("failed to connect to postgres")
}

pub(super) const TEST_TABLE: &str = "test_parallel_pg_output";

pub(super) fn setup_table(client: &mut postgres::Client) {
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS "{TEST_TABLE}" (
                    id int PRIMARY KEY,
                    b bool NOT NULL,
                    i int8,
                    s varchar NOT NULL
                )"#
            ),
            &[],
        )
        .expect("failed to create test table");
    client
        .execute(&format!(r#"TRUNCATE "{TEST_TABLE}""#), &[])
        .expect("failed to truncate test table");
}

fn setup_table_with_extra_columns(client: &mut postgres::Client) {
    client
        .execute(&format!(r#"DROP TABLE IF EXISTS "{TEST_TABLE}""#), &[])
        .expect("failed to drop test table");

    client
        .execute(
            &format!(
                r#"
                CREATE TABLE "{TEST_TABLE}" (
                    id int PRIMARY KEY,
                    b bool NOT NULL,
                    i int8,
                    s varchar NOT NULL,
                    "EXTRA.COLUMN1" varchar,
                    extra_column2 timestamp,
                    extra_column3 bigint,
                    extra_column4 json
                )"#
            ),
            &[],
        )
        .expect("failed to create test table");
}

pub(super) fn make_config_with_extra_columns(
    threads: usize,
    extra_columns: Vec<String>,
) -> PostgresWriterConfig {
    PostgresWriterConfig {
        uri: postgres_url(),
        table: TEST_TABLE.to_string(),
        tls: PostgresTlsConfig::default(),
        max_records_in_buffer: None,
        max_buffer_size: ConfigSize::from_mib(1),
        on_conflict_do_nothing: false,
        mode: PostgresWriteMode::Materialized,
        cdc_op_column: "__feldera_op".to_owned(),
        cdc_ts_column: "__feldera_ts".to_owned(),
        threads,
        extra_columns,
    }
}

pub(super) fn make_endpoint_with_extra_columns(
    threads: usize,
    extra_columns: Vec<String>,
) -> PostgresOutputEndpoint {
    PostgresOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &make_config_with_extra_columns(threads, extra_columns),
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        true,
    )
    .expect("failed to create endpoint")
}

pub(super) fn make_endpoint(threads: usize) -> PostgresOutputEndpoint {
    make_endpoint_with_extra_columns(threads, Vec::new())
}

/// Returns `rows` of [`value_relation`] in the default JSON flavor, each with its weight, as an
/// output batch of its index.
fn build_batch(
    engine: &dyn ConnectorTestEngine,
    rows: Vec<(Value, i64)>,
) -> Arc<dyn SerBatchReader> {
    let relation = value_relation();
    let rows: Vec<(i64, String)> = rows
        .into_iter()
        .map(|(row, weight)| {
            (
                weight,
                read::arrow_form(&relation, row).unwrap().to_string(),
            )
        })
        .collect();
    let rows: Vec<(i64, &str)> = rows
        .iter()
        .map(|(weight, row)| (*weight, row.as_str()))
        .collect();
    let updates = Updates::from_json(published_schema(&relation), &rows).unwrap();
    engine
        .indexed_output_batch(
            &key_relation(),
            &relation,
            updates.rows(),
            updates.weights(),
        )
        .unwrap()
}

fn build_insert_batch(
    engine: &dyn ConnectorTestEngine,
    records: &[Value],
) -> Arc<dyn SerBatchReader> {
    build_batch(engine, records.iter().map(|r| (r.clone(), 1)).collect())
}

fn build_delete_batch(
    engine: &dyn ConnectorTestEngine,
    records: &[Value],
) -> Arc<dyn SerBatchReader> {
    build_batch(engine, records.iter().map(|r| (r.clone(), -1)).collect())
}

/// Build an upsert batch: weight -1 for the old value, +1 for the new.
fn build_upsert_batch(
    engine: &dyn ConnectorTestEngine,
    updates: &[(Value, Value)],
) -> Arc<dyn SerBatchReader> {
    let mut rows = Vec::new();
    for (old, new) in updates {
        assert_eq!(id(old), id(new));
        rows.push((old.clone(), -1));
        rows.push((new.clone(), 1));
    }
    build_batch(engine, rows)
}

fn encode_batch(endpoint: &mut PostgresOutputEndpoint, batch: &Arc<dyn SerBatchReader>) {
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    endpoint.consumer().batch_end();
}

fn query_all(client: &mut postgres::Client) -> Vec<Value> {
    let rows = client
        .query(
            &format!(r#"SELECT id, b, i, s FROM "{TEST_TABLE}" ORDER BY id"#),
            &[],
        )
        .expect("failed to query");
    rows.into_iter()
        .map(|row| {
            record(
                row.get(0),
                row.get(1),
                row.get(2),
                row.get::<_, String>(3).as_str(),
            )
        })
        .collect()
}

/// Returns the rows of the table, each with the `json` column `extra_column4` beside it: `None`
/// for SQL NULL, so that SQL NULL and a JSON null stay apart.
fn query_all_with_extra_columns(client: &mut postgres::Client) -> Vec<(Value, Option<Value>)> {
    let rows = client
        .query(
            &format!(r#"SELECT id, b, i, s, "EXTRA.COLUMN1", extra_column2, extra_column3, extra_column4 FROM "{TEST_TABLE}" ORDER BY id"#),
            &[],
        )
        .expect("failed to query");
    rows.into_iter()
        .map(|row| {
            let mut record = record(
                row.get(0),
                row.get(1),
                row.get(2),
                row.get::<_, String>(3).as_str(),
            );
            record["EXTRA.COLUMN1"] = json!(row.get::<_, Option<String>>(4));
            record["extra_column2"] = json!(
                row.get::<_, Option<NaiveDateTime>>(5)
                    .map(|t| t.to_string())
            );
            record["extra_column3"] = json!(row.get::<_, Option<i64>>(6));
            (record, row.get::<_, Option<Value>>(7))
        })
        .collect()
}

/// Returns the row of [`value_relation`] with these columns, in the default JSON flavor.
fn record(id: i32, b: bool, i: Option<i64>, s: &str) -> Value {
    json!({ "id": id, "b": b, "i": i, "s": s })
}

/// Returns the `id` of `row`, a row of [`value_relation`].
fn id(row: &Value) -> i32 {
    row["id"].as_i64().unwrap() as i32
}

/// Returns the row of [`value_relation`] that the cases number `i`.
fn numbered_record(i: i32) -> Value {
    record(
        i,
        i % 2 == 0,
        if i % 3 == 0 {
            None
        } else {
            Some(i as i64 * 10)
        },
        &format!("record_{i}"),
    )
}

/// Returns `record` with the extra columns that the connector writes beside the columns of the
/// view, in the form [`query_all_with_extra_columns`] reads them back.
fn with_extra_columns(
    record: &Value,
    column1: Option<&str>,
    column2: Option<&str>,
    column3: Option<i64>,
    column4: Option<Value>,
) -> (Value, Option<Value>) {
    let mut record = record.clone();
    record["EXTRA.COLUMN1"] = json!(column1);
    record["extra_column2"] = json!(column2);
    record["extra_column3"] = json!(column3);
    (record, column4)
}

fn make_records(n: usize) -> Vec<Value> {
    (0..n as i32).map(numbered_record).collect()
}

fn insert_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let mut client = pg_client();
    setup_table(&mut client);

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads);

    encode_batch(&mut endpoint, &batch);

    let got = query_all(&mut client);
    assert_eq!(got, records);
}

pub fn insert_single_thread(engine: &dyn ConnectorTestEngine) {
    insert_test(engine, 1);
}

pub fn insert_multi_thread(engine: &dyn ConnectorTestEngine) {
    insert_test(engine, 4);
}

fn upsert_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let mut client = pg_client();
    setup_table(&mut client);

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads);

    encode_batch(&mut endpoint, &batch);

    // Update even-id records
    let updates: Vec<_> = records
        .iter()
        .filter(|r| id(r) % 2 == 0)
        .map(|r| {
            let mut new = r.clone();
            new["s"] = Value::from(format!("updated_{}", id(r)));
            (r.clone(), new)
        })
        .collect();
    let upsert_batch = build_upsert_batch(engine, &updates);

    encode_batch(&mut endpoint, &upsert_batch);

    let got = query_all(&mut client);
    let expected: Vec<_> = records
        .iter()
        .map(|r| {
            if id(r) % 2 == 0 {
                let mut new = r.clone();
                new["s"] = Value::from(format!("updated_{}", id(r)));
                new
            } else {
                r.clone()
            }
        })
        .collect();
    assert_eq!(got, expected);
}

pub fn extra_columns_test(engine: &dyn ConnectorTestEngine) {
    let mut client = pg_client();
    setup_table_with_extra_columns(&mut client);

    let records = make_records(100);
    let batch1 = build_insert_batch(engine, &records[..25]);
    let batch2 = build_insert_batch(engine, &records[25..50]);
    let batch3 = build_insert_batch(engine, &records[50..75]);
    let batch4 = build_insert_batch(engine, &records[75..100]);
    let mut endpoint = make_endpoint_with_extra_columns(
        2,
        vec![
            "EXTRA.COLUMN1".to_string(),
            "extra_column2".to_string(),
            "extra_column3".to_string(),
            "extra_column4".to_string(),
        ],
    );

    encode_batch(&mut endpoint, &batch1);

    endpoint
        .command_handler()
        .unwrap()
        .command(serde_json::json!({
            "set_extra_columns": {
                "EXTRA.COLUMN1": "foo1",
                "extra_column2": "2026-04-23 14:30:00.123456",
                "extra_column3": 100,
                "extra_column4": { "foo": "bar" },
            }
        }))
        .unwrap();

    encode_batch(&mut endpoint, &batch2);

    endpoint
        .command_handler()
        .unwrap()
        .command(serde_json::json!({
            "set_extra_columns": {
                "EXTRA.COLUMN1": "foo2",
                "extra_column2": "2026-04-23T15:30:00",
                "extra_column3": 200,
                "extra_column4": ["foo", "bar"],
            }
        }))
        .unwrap();

    encode_batch(&mut endpoint, &batch3);

    endpoint
        .command_handler()
        .unwrap()
        .command(serde_json::json!({
            "set_extra_columns": {
                "EXTRA.COLUMN1": null,
                "extra_column2": null,
                "extra_column3": null,
                "extra_column4": null,
            }
        }))
        .unwrap();

    encode_batch(&mut endpoint, &batch4);

    let got = query_all_with_extra_columns(&mut client);
    let expected1: Vec<_> = records[..25]
        .iter()
        .map(|r| with_extra_columns(r, None, None, None, None))
        .collect();
    let expected2: Vec<_> = records[25..50]
        .iter()
        .map(|r| {
            with_extra_columns(
                r,
                Some("foo1"),
                Some("2026-04-23 14:30:00.123456"),
                Some(100),
                Some(json!({ "foo": "bar" })),
            )
        })
        .collect();
    let expected3: Vec<_> = records[50..75]
        .iter()
        .map(|r| {
            with_extra_columns(
                r,
                Some("foo2"),
                Some("2026-04-23 15:30:00"),
                Some(200),
                Some(json!(["foo", "bar"])),
            )
        })
        .collect();
    let expected4: Vec<_> = records[75..100]
        .iter()
        .map(|r| with_extra_columns(r, None, None, None, None))
        .collect();
    let expected: Vec<_> = expected1
        .into_iter()
        .chain(expected2)
        .chain(expected3)
        .chain(expected4)
        .collect();
    assert_eq!(got, expected);

    // Update records
    let updates: Vec<_> = records
        .iter()
        .map(|r| {
            let mut new = r.clone();
            new["s"] = Value::from(format!("updated_{}", id(r)));
            (r.clone(), new)
        })
        .collect();
    let upsert_batch = build_upsert_batch(engine, &updates);

    endpoint
        .command_handler()
        .unwrap()
        .command(serde_json::json!({
            "set_extra_columns": {
                "EXTRA.COLUMN1": "foo3",
                "extra_column2": "2026-04-23T16:30:00",
                "extra_column3": 300,
                "extra_column4": "foo",
            }
        }))
        .unwrap();

    encode_batch(&mut endpoint, &upsert_batch);

    let got = query_all_with_extra_columns(&mut client);
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(r, _)| {
            let (mut new, column4) = with_extra_columns(
                &r,
                Some("foo3"),
                Some("2026-04-23 16:30:00"),
                Some(300),
                Some(json!("foo")),
            );
            new["s"] = Value::from(format!("updated_{}", id(&r)));
            (new, column4)
        })
        .collect();
    assert_eq!(got, expected);
}

pub fn upsert_single_thread(engine: &dyn ConnectorTestEngine) {
    upsert_test(engine, 1);
}

pub fn upsert_multi_thread(engine: &dyn ConnectorTestEngine) {
    upsert_test(engine, 4);
}

fn delete_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let mut client = pg_client();
    setup_table(&mut client);

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads);

    encode_batch(&mut endpoint, &batch);

    // Delete odd-id records
    let to_delete: Vec<_> = records.iter().filter(|r| id(r) % 2 == 1).cloned().collect();
    let delete_batch = build_delete_batch(engine, &to_delete);

    encode_batch(&mut endpoint, &delete_batch);

    let got = query_all(&mut client);
    let expected: Vec<_> = records.into_iter().filter(|r| id(r) % 2 == 0).collect();
    assert_eq!(got, expected);
}

pub fn delete_single_thread(engine: &dyn ConnectorTestEngine) {
    delete_test(engine, 1);
}

pub fn delete_multi_thread(engine: &dyn ConnectorTestEngine) {
    delete_test(engine, 4);
}

fn empty_batch_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let mut client = pg_client();
    setup_table(&mut client);

    let batch = build_insert_batch(engine, &[]);
    let mut endpoint = make_endpoint(threads);

    encode_batch(&mut endpoint, &batch);

    let got = query_all(&mut client);
    assert!(got.is_empty());
}

pub fn empty_batch_single_thread(engine: &dyn ConnectorTestEngine) {
    empty_batch_test(engine, 1);
}

pub fn empty_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    empty_batch_test(engine, 4);
}

pub fn single_record_multi_thread(engine: &dyn ConnectorTestEngine) {
    let mut client = pg_client();
    setup_table(&mut client);

    let records = make_records(1);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(4);

    encode_batch(&mut endpoint, &batch);

    let got = query_all(&mut client);
    assert_eq!(got, records);
}

fn multiple_batches_test(engine: &dyn ConnectorTestEngine, threads: usize) {
    let mut client = pg_client();
    setup_table(&mut client);

    let mut endpoint = make_endpoint(threads);

    // Batch 1: insert 50 records
    let records1 = make_records(50);
    let batch1 = build_insert_batch(engine, &records1);
    encode_batch(&mut endpoint, &batch1);

    let got = query_all(&mut client);
    assert_eq!(got, records1);

    // Batch 2: insert 50 more records (ids 50..100)
    let records2: Vec<_> = (50..100).map(numbered_record).collect();
    let batch2 = build_insert_batch(engine, &records2);
    encode_batch(&mut endpoint, &batch2);

    let got = query_all(&mut client);
    let mut all_records = records1;
    all_records.extend(records2);
    assert_eq!(got, all_records);

    // Batch 3: update records 0..25
    let updates: Vec<_> = all_records[..25]
        .iter()
        .map(|r| {
            let mut new = r.clone();
            new["s"] = Value::from(format!("batch3_{}", id(r)));
            (r.clone(), new)
        })
        .collect();
    let batch3 = build_upsert_batch(engine, &updates);
    encode_batch(&mut endpoint, &batch3);

    let got = query_all(&mut client);
    let expected: Vec<_> = all_records
        .iter()
        .map(|r| {
            if id(r) < 25 {
                let mut new = r.clone();
                new["s"] = Value::from(format!("batch3_{}", id(r)));
                new
            } else {
                r.clone()
            }
        })
        .collect();
    assert_eq!(got, expected);

    // Batch 4: delete records 75..100
    let to_delete: Vec<_> = expected[75..].to_vec();
    let batch4 = build_delete_batch(engine, &to_delete);
    encode_batch(&mut endpoint, &batch4);

    let got = query_all(&mut client);
    assert_eq!(got.len(), 75);
    assert_eq!(got, expected[..75]);
}

pub fn multiple_batches_single_thread(engine: &dyn ConnectorTestEngine) {
    multiple_batches_test(engine, 1);
}

pub fn multiple_batches_multi_thread(engine: &dyn ConnectorTestEngine) {
    multiple_batches_test(engine, 4);
}

pub(super) fn records_written(endpoint: &PostgresOutputEndpoint) -> u64 {
    endpoint.records_written.load(Ordering::Relaxed)
}

/// Build a config that forces frequent in-batch flushes by capping the
/// number of records buffered between `execute()` calls.  This lets
/// tests observe the progress counter advance mid-batch instead of
/// only at commit time.
fn make_endpoint_flush_every(threads: usize, max_records: usize) -> PostgresOutputEndpoint {
    let mut cfg = make_config_with_extra_columns(threads, Vec::new());
    cfg.max_records_in_buffer = Some(max_records);
    PostgresOutputEndpoint::new(
        EndpointId::default(),
        "test_endpoint",
        &cfg,
        &Some(key_relation()),
        &value_relation(),
        Weak::new(),
        true,
    )
    .expect("failed to create endpoint")
}

fn progress_basic(engine: &dyn ConnectorTestEngine, threads: usize) {
    let mut client = pg_client();
    setup_table(&mut client);

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);
    let mut endpoint = make_endpoint(threads);

    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    assert_eq!(records_written(&endpoint), 0);
    endpoint.encode(batch.clone()).unwrap();
    endpoint.consumer().batch_end();
    // Counter resets to 0 at the end of every batch.
    assert_eq!(records_written(&endpoint), 0);

    // Sanity check: data actually landed in postgres.
    let got = query_all(&mut client);
    assert_eq!(got, records);
}

pub fn progress_counter_single_thread(engine: &dyn ConnectorTestEngine) {
    progress_basic(engine, 1);
}

pub fn progress_counter_multi_thread(engine: &dyn ConnectorTestEngine) {
    progress_basic(engine, 4);
}

pub fn progress_counter_empty_batch(engine: &dyn ConnectorTestEngine) {
    let mut client = pg_client();
    setup_table(&mut client);

    let batch = build_insert_batch(engine, &[]);
    let mut endpoint = make_endpoint(1);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    assert_eq!(records_written(&endpoint), 0);
    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_advances_mid_batch_single_thread(engine: &dyn ConnectorTestEngine) {
    let mut client = pg_client();
    setup_table(&mut client);

    let records = make_records(50);
    let batch = build_insert_batch(engine, &records);
    // Force flushes during encode so the counter advances mid-batch.
    let mut endpoint = make_endpoint_flush_every(1, 10);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();
    // After encode, completed flushes are visible in the counter.
    // With max_records_in_buffer=10 and 50 records, at least 4 flushes
    // (40 records) have run; the trailing partial buffer is flushed
    // during batch_end.
    assert!(
        records_written(&endpoint) >= 40,
        "expected mid-batch progress, got {}",
        records_written(&endpoint)
    );

    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_multiple_batches(engine: &dyn ConnectorTestEngine) {
    let mut client = pg_client();
    setup_table(&mut client);

    let mut endpoint = make_endpoint(1);

    // Batch 1: 50 records.
    let records1 = make_records(50);
    let batch1 = build_insert_batch(engine, &records1);
    encode_batch(&mut endpoint, &batch1);
    assert_eq!(records_written(&endpoint), 0);

    // Batch 2: 30 more records (ids 50..80).
    let records2: Vec<_> = (50..80)
        .map(|i| record(i, i % 2 == 0, Some(i as i64), &format!("record_{i}")))
        .collect();
    let batch2 = build_insert_batch(engine, &records2);
    encode_batch(&mut endpoint, &batch2);
    assert_eq!(records_written(&endpoint), 0);
}

pub fn progress_counter_advances_mid_batch_multi_thread(engine: &dyn ConnectorTestEngine) {
    let mut client = pg_client();
    setup_table(&mut client);

    // 4 workers, force flush every 5 records so each worker flushes
    // multiple times.  With 100 records spread across 4 workers, every
    // worker has ~25 records; 5 flushes of 5 records each.
    let mut endpoint = make_endpoint_flush_every(4, 5);

    let records = make_records(100);
    let batch = build_insert_batch(engine, &records);

    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint.encode(batch.clone()).unwrap();

    // After encode, completed flushes from all 4 workers are visible.
    // We can't predict the exact number (depends on how the trailing
    // partial buffer per worker shakes out), but it must be > 0 and
    // <= 100.
    let mid = records_written(&endpoint);
    assert!(mid > 0, "expected mid-batch progress, got 0");
    assert!(mid <= 100, "counter exceeds total records: {mid}");

    endpoint.consumer().batch_end();
    assert_eq!(records_written(&endpoint), 0);

    // Final state in postgres matches all input records.
    let got = query_all(&mut client);
    assert_eq!(got, records);
}
