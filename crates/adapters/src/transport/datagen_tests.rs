//! The datagen input connector: generation plans, the strategies of each column type, NULLs,
//! transactions, and invalid plans.

use crate::test::engine_test;
use std::collections::BTreeMap;
use std::env;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use feldera_adapterlib::test::{ConnectorTestEngine, InputOptions, default_timeout_ms, wait};
use feldera_types::config::InputEndpointConfig;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use serde_json::{Value, json};

use crate::test::{InputPipeline, input_pipeline, relations, render};

/// A table of assorted types.
pub fn assorted_relation() -> Relation {
    relations::assorted("datagen_assorted")
}

/// A table of a DOUBLE and DECIMAL columns of three scales.
pub fn real_relation() -> Relation {
    Relation::new(
        "datagen_real".into(),
        vec![
            Field::new("double".into(), ColumnType::double(false)),
            Field::new("dec".into(), ColumnType::decimal(28, 0, false)),
            Field::new("dec_scale".into(), ColumnType::decimal(10, 10, false)),
            Field::new("dec_w_scale".into(), ColumnType::decimal(2, 1, false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// A table of one VARBINARY column.
pub fn bytes_relation() -> Relation {
    Relation::new(
        "datagen_bytes".into(),
        vec![Field::new("bs".into(), ColumnType::varbinary(false))],
        false,
        BTreeMap::new(),
    )
}

/// A table of a TIMESTAMP, a DATE, and a TIME column with the case-sensitive name `t`.
pub fn time_relation() -> Relation {
    Relation::new(
        "datagen_time".into(),
        vec![
            Field::new("ts".into(), ColumnType::timestamp(false)),
            Field::new("dt".into(), ColumnType::date(false)),
            Field::new("\"t\"".into(), ColumnType::time(false)),
        ],
        false,
        BTreeMap::new(),
    )
}

/// Returns the configuration of a datagen endpoint with the transport configuration
/// `transport`.
fn config(transport: Value) -> InputEndpointConfig {
    serde_json::from_value(json!({
        "stream": "test_input",
        "transport": { "name": "datagen", "config": transport }
    }))
    .unwrap()
}

/// Opens a datagen endpoint with `transport` into a fresh table of `relation`, and starts it.
fn generator(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    transport: Value,
) -> InputPipeline {
    let pipeline = input_pipeline(
        engine,
        relation,
        &InputOptions::default(),
        config(transport),
    )
    .unwrap();
    pipeline.reader.extend();
    pipeline
}

/// Opens a datagen endpoint with one plan of `plan` into a fresh table of `relation`, runs it
/// to the end, and returns the rows that it generates, in order, as JSON objects in the
/// default JSON flavor.
fn generate(engine: &dyn ConnectorTestEngine, relation: &Relation, plan: Value) -> Vec<Value> {
    let pipeline = generator(engine, relation, json!({ "plan": [plan] }));
    generated(&pipeline, relation)
}

/// Waits for the generator of `pipeline` to finish, flushes what it generated, and returns
/// the rows, in order, as JSON objects in the default JSON flavor.
fn generated(pipeline: &InputPipeline, relation: &Relation) -> Vec<Value> {
    wait(|| pipeline.consumer.state().eoi, default_timeout_ms()).unwrap();
    pipeline.reader.queue(false);
    wait(
        || pipeline.consumer.state().n_extended > 0,
        default_timeout_ms(),
    )
    .unwrap();
    let updates = pipeline.collection.updates().unwrap();
    assert!(
        updates.weights().iter().all(|weight| *weight == 1),
        "the generator inserts rows only: {updates:?}"
    );
    (0..updates.len())
        .map(|index| render::json_object(relation, updates.rows(), index))
        .collect()
}

/// Returns `value`, a decimal as the default JSON flavor writes it, as a float.
fn decimal(value: &Value) -> f64 {
    value.as_str().unwrap().parse().unwrap()
}

/// Returns `value`, a byte string as the default JSON flavor writes it.
fn bytes(value: &Value) -> Vec<u64> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|byte| byte.as_u64().unwrap())
        .collect()
}

/// A generator with a transaction size starts a transaction with its first step and commits
/// it once the transaction is full.
#[engine_test]
pub fn transaction(engine: &dyn ConnectorTestEngine) {
    let pipeline = generator(
        engine,
        &assorted_relation(),
        json!({
            "plan": [{ "limit": 1000, "rate": 100, "fields": {} }],
            "transaction_size": 2000
        }),
    );

    // Two seconds in, a step finds a transaction in progress.
    sleep(Duration::from_secs(2));
    pipeline.reader.queue(false);
    wait(
        || pipeline.consumer.state().n_extended > 0,
        default_timeout_ms(),
    )
    .unwrap();
    assert!(pipeline.consumer.state().transaction_in_progress);

    // Ten seconds later, the next step finds it still open.
    sleep(Duration::from_secs(10));
    assert_eq!(pipeline.consumer.state().n_extended, 1);
    pipeline.reader.queue(false);
    wait(
        || pipeline.consumer.state().n_extended > 1,
        default_timeout_ms(),
    )
    .unwrap();
    assert!(pipeline.consumer.state().transaction_in_progress);

    generated(&pipeline, &assorted_relation());
}

/// At the end of input, a generator answers a `Queue` that comes right after an `Extend`.
///
/// The worker's wake-ups coalesce, so `Extend` then `Queue`, as the controller sends them when
/// it resumes inputs, reach the worker as one wake-up. A worker that handled one command per
/// wake-up would leave the `Queue` unanswered, and the controller's step would wait forever.
#[engine_test]
pub fn extend_and_queue_at_end_of_input(engine: &dyn ConnectorTestEngine) {
    let relation = assorted_relation();
    let pipeline = generator(
        engine,
        &relation,
        json!({ "plan": [{ "limit": 10, "fields": {} }] }),
    );
    generated(&pipeline, &relation);
    assert!(pipeline.consumer.state().eoi);
    // Let the worker park.
    sleep(Duration::from_millis(200));
    let baseline = pipeline.consumer.state().n_extended;

    pipeline.reader.extend();
    pipeline.reader.queue(false);

    wait(|| pipeline.consumer.state().n_extended > baseline, 10_000)
        .expect("the generator answers a Queue right after an Extend at the end of input");
}

/// By default, an integer column counts up from 0, and a boolean column alternates.
#[engine_test]
pub fn limit_increment(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({ "limit": 10, "fields": {} }),
    );
    assert_eq!(rows.len(), 10);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["id"], json!(index));
        assert_eq!(row["b"], json!(index % 2 == 1));
        assert_eq!(row["es"]["a"], json!(index % 2 == 1));
    }
}

/// An incrementing column with a scale steps by the scale and wraps within its range.
#[engine_test]
pub fn scaled_range_increment(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({
            "limit": 10,
            "fields": { "id": { "strategy": "increment", "range": [10, 20], "scale": 3 } }
        }),
    );
    assert_eq!(rows.len(), 10);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["id"], json!(10 + (10 + 3 * index) % 10));
    }
}

/// An incrementing DOUBLE column steps by its scale from the start of its range.
#[engine_test]
pub fn scaled_range_increment_reals(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &real_relation(),
        json!({
            "limit": 5,
            "fields": { "double": { "strategy": "increment", "range": [1.1, 10.1], "scale": 1 } }
        }),
    );
    assert_eq!(rows.len(), 5);
    for (index, row) in rows.iter().enumerate() {
        let double = row["double"].as_f64().unwrap();
        assert!((double - (1.1 + index as f64)).abs() < 1e-5, "{double}");
    }
}

/// Incrementing DECIMAL columns stay within the precision and scale of their types, and
/// within a range or a set of values.
#[engine_test]
pub fn decimal_increment(engine: &dyn ConnectorTestEngine) {
    let relation = real_relation();
    let rows = generate(
        engine,
        &relation,
        json!({
            "limit": 20,
            "fields": { "dec": { "strategy": "increment" }, "dec_w_scale": { "strategy": "increment" } }
        }),
    );
    assert_eq!(rows.len(), 20);
    for row in &rows {
        assert!((0.0..20.0).contains(&decimal(&row["dec"])), "{row}");
        assert!((0.0..0.9999).contains(&decimal(&row["dec_scale"])), "{row}");
        assert!((0.0..10.0).contains(&decimal(&row["dec_w_scale"])), "{row}");
    }

    let rows = generate(
        engine,
        &relation,
        json!({
            "limit": 20,
            "fields": {
                "dec": { "strategy": "increment", "range": [1, 5] },
                "dec_w_scale": { "values": [0.1, 1, 9.9] }
            }
        }),
    );
    assert_eq!(rows.len(), 20);
    for row in &rows {
        assert!((1.0..5.0).contains(&decimal(&row["dec"])), "{row}");
        assert!(
            [0.1, 1.0, 9.9].contains(&decimal(&row["dec_w_scale"])),
            "{row}"
        );
    }
}

/// Uniform DECIMAL columns stay within a set of values or a range.
#[engine_test]
pub fn decimal_uniform(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &real_relation(),
        json!({
            "limit": 1000,
            "fields": {
                "dec_scale": { "strategy": "uniform", "range": [0.5, 0.6] },
                "dec_w_scale": { "strategy": "uniform" },
                "dec": { "strategy": "uniform", "values": [0.1, 1, 2] }
            }
        }),
    );
    assert_eq!(rows.len(), 1000);
    for row in &rows {
        // DECIMAL(28, 0) holds 0.1 as 0.
        assert!([0.0, 1.0, 2.0].contains(&decimal(&row["dec"])), "{row}");
        assert!((0.5..0.6).contains(&decimal(&row["dec_scale"])), "{row}");
        assert!((-9.9..9.9).contains(&decimal(&row["dec_w_scale"])), "{row}");
    }
}

/// Zipf DECIMAL columns stay within a set of values or a range.
#[engine_test]
pub fn decimal_zipf(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &real_relation(),
        json!({
            "limit": 1000,
            "fields": {
                "dec_scale": { "strategy": "zipf", "range": [0, 1] },
                "dec_w_scale": { "strategy": "zipf" },
                "dec": { "strategy": "zipf", "values": [0.1, 1, 2] }
            }
        }),
    );
    assert_eq!(rows.len(), 1000);
    for row in &rows {
        assert!([0.0, 1.0, 2.0].contains(&decimal(&row["dec"])), "{row}");
        // DECIMAL(10, 10) cannot hold 1.
        assert!((0.0..1.0).contains(&decimal(&row["dec_scale"])), "{row}");
        assert!((0.0..9.9).contains(&decimal(&row["dec_w_scale"])), "{row}");
    }
}

/// A VARBINARY column with a length range holds bytes that count up from 0.
#[engine_test]
pub fn array_increment(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &bytes_relation(),
        json!({ "limit": 2, "fields": { "bs": { "range": [10, 11] } } }),
    );
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(bytes(&row["bs"]), (0..10).collect::<Vec<u64>>());
    }
}

/// A uniform integer column stays within its range.
#[engine_test]
pub fn uniform_range(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({ "limit": 1, "fields": { "id": { "strategy": "uniform", "range": [10, 20] } } }),
    );
    assert_eq!(rows.len(), 1);
    let id = rows[0]["id"].as_i64().unwrap();
    assert!((10..20).contains(&id), "{id}");
}

/// A column with a list of values takes them in turn.
#[engine_test]
pub fn values(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({ "limit": 4, "fields": { "id": { "values": [99, 100, 101] } } }),
    );
    let ids: Vec<i64> = rows.iter().map(|row| row["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [99, 100, 101, 99]);
}

/// Opens a generator of one row of `relation` whose column `column` takes `values` with each
/// strategy, and checks that each generator reports that a value has the wrong type.
fn check_invalid_values(
    engine: &dyn ConnectorTestEngine,
    relation: &Relation,
    column: &str,
    values: Value,
) {
    for strategy in ["increment", "uniform", "zipf"] {
        let pipeline = input_pipeline(
            engine,
            relation,
            &InputOptions::default(),
            config(json!({
                "plan": [{
                    "limit": 4,
                    "fields": { column: { "values": values, "strategy": strategy } }
                }]
            })),
        )
        .unwrap();
        let errors = Arc::new(Mutex::new(Vec::new()));
        let errors_clone = errors.clone();
        pipeline
            .consumer
            .on_error(Some(Box::new(move |_fatal, error| {
                errors_clone.lock().unwrap().push(error.to_string())
            })));
        pipeline.reader.extend();
        wait(|| !errors.lock().unwrap().is_empty(), default_timeout_ms()).unwrap();
        let error = errors.lock().unwrap()[0].clone();
        assert!(
            error.contains("Invalid type found in `values`"),
            "{strategy}: {error}"
        );
        pipeline.reader.disconnect();
    }
}

/// Strings in the values of an integer column are an error.
#[engine_test]
pub fn invalid_integer_values(engine: &dyn ConnectorTestEngine) {
    check_invalid_values(engine, &assorted_relation(), "id", json!(["a", "b"]));
}

/// Numbers in the values of a VARCHAR column are an error.
#[engine_test]
pub fn invalid_string_values(engine: &dyn ConnectorTestEngine) {
    check_invalid_values(engine, &assorted_relation(), "nAmE", json!([1, 2]));
}

/// A boolean in the values of a TIMESTAMP column is an error.
#[engine_test]
pub fn invalid_timestamp_values(engine: &dyn ConnectorTestEngine) {
    check_invalid_values(engine, &assorted_relation(), "ts", json!([true]));
}

/// A number in the values of a TIME column is an error.
#[engine_test]
pub fn invalid_time_values(engine: &dyn ConnectorTestEngine) {
    check_invalid_values(engine, &time_relation(), "t", json!([1]));
}

/// A number in the values of a DATE column is an error.
#[engine_test]
pub fn invalid_date_values(engine: &dyn ConnectorTestEngine) {
    check_invalid_values(engine, &assorted_relation(), "dt", json!([1]));
}

/// A column with `null_percentage` 100 is always NULL.
#[engine_test]
pub fn null(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({ "limit": 10, "fields": { "nAmE": { "null_percentage": 100 } } }),
    );
    assert_eq!(rows.len(), 10);
    assert!(rows.iter().all(|row| row["nAmE"].is_null()));
}

/// A column with `null_percentage` 50 is sometimes NULL and sometimes not.
#[engine_test]
pub fn null_percentage(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({ "limit": 100, "fields": { "nAmE": { "null_percentage": 50 } } }),
    );
    assert_eq!(rows.len(), 100);
    assert!(rows.iter().any(|row| row["nAmE"].is_null()));
    assert!(rows.iter().any(|row| !row["nAmE"].is_null()));
}

/// The `word` strategy makes a VARCHAR that is not NULL.
#[engine_test]
pub fn string_generators(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &assorted_relation(),
        json!({ "limit": 2, "fields": { "nAmE": { "strategy": "word" } } }),
    );
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row["nAmE"].is_string()));
}

/// A VARBINARY column with a list of values takes them in turn.
#[engine_test]
pub fn byte_array_with_values(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &bytes_relation(),
        json!({
            "limit": 3,
            "fields": { "bs": { "range": [1, 5], "values": [[1, 2], [1, 2, 3]] } }
        }),
    );
    let values: Vec<Vec<u64>> = rows.iter().map(|row| bytes(&row["bs"])).collect();
    assert_eq!(values, [vec![1, 2], vec![1, 2, 3], vec![1, 2]]);
}

/// A VARBINARY column with a length range and a byte range grows by one byte per row, and
/// wraps.
#[engine_test]
pub fn byte_array_with_increment(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &bytes_relation(),
        json!({
            "limit": 5,
            "fields": { "bs": { "range": [0, 3], "value": { "range": [0, 2] } } }
        }),
    );
    let values: Vec<Vec<u64>> = rows.iter().map(|row| bytes(&row["bs"])).collect();
    assert_eq!(values[..4], [vec![], vec![0], vec![0, 1], vec![]]);
}

/// The bytes of a VARBINARY column follow the strategy and range of `value`.
#[engine_test]
pub fn byte_array_with_value(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &bytes_relation(),
        json!({
            "limit": 2,
            "fields": {
                "bs": { "range": [1, 2], "value": { "range": [128, 255], "strategy": "uniform" } }
            }
        }),
    );
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert!(bytes(&row["bs"])[0] >= 128, "{row}");
    }
}

/// By default, a TIMESTAMP column counts milliseconds, a DATE column days, and a TIME column
/// milliseconds.
#[engine_test]
pub fn time_types(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &time_relation(),
        json!({ "limit": 2, "fields": {} }),
    );
    assert_eq!(
        rows,
        [
            json!({"ts": "1970-01-01 00:00:00", "dt": "1970-01-01", "t": "00:00:00"}),
            json!({"ts": "1970-01-01 00:00:00.001", "dt": "1970-01-02", "t": "00:00:00.001"}),
        ]
    );
}

/// Temporal columns with integer ranges count up within them, and wrap.
#[engine_test]
pub fn time_types_with_integer_range(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &time_relation(),
        json!({
            "limit": 3,
            "fields": {
                "ts": { "range": [1724803200000u64, 1724803200002u64] },
                "dt": { "range": [19963, 19965] },
                "t": { "range": [5, 7] }
            }
        }),
    );
    let first = json!({"ts": "2024-08-28 00:00:00", "dt": "2024-08-28", "t": "00:00:00.005"});
    assert_eq!(
        rows,
        [
            first.clone(),
            json!({"ts": "2024-08-28 00:00:00.001", "dt": "2024-08-29", "t": "00:00:00.006"}),
            first,
        ]
    );
}

/// Uniform temporal columns stay within their ranges.
#[engine_test]
pub fn uniform_dates_times_timestamps(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &time_relation(),
        json!({
            "limit": 100,
            "fields": {
                "ts": { "strategy": "uniform", "range": ["2024-10-11T11:04:00Z", "2024-10-11T11:05:02Z"] },
                "dt": { "strategy": "uniform", "range": [19963, 19965] },
                "t": { "strategy": "uniform", "range": [5, 7] }
            }
        }),
    );
    assert_eq!(rows.len(), 100);
    for row in &rows {
        let ts = row["ts"].as_str().unwrap();
        assert!(
            ("2024-10-11 11:04:00"..="2024-10-11 11:05:01.999999").contains(&ts),
            "{row}"
        );
        let dt = row["dt"].as_str().unwrap();
        assert!(["2024-08-28", "2024-08-29"].contains(&dt), "{row}");
        let t = row["t"].as_str().unwrap();
        assert!(
            ("00:00:00.005"..="00:00:00.006999999").contains(&t),
            "{row}"
        );
    }
}

/// Temporal columns with string ranges count up within them by their scales, and wrap.
#[engine_test]
pub fn time_types_with_string_range(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &time_relation(),
        json!({
            "limit": 3,
            "fields": {
                "ts": { "range": ["2024-08-28T00:00:00Z", "2024-08-28T00:00:02Z"], "scale": 1000 },
                "dt": { "range": ["2024-08-28", "2024-08-30"] },
                "t": { "range": ["00:00:05", "00:00:07"], "scale": 1000 }
            }
        }),
    );
    let first = json!({"ts": "2024-08-28 00:00:00", "dt": "2024-08-28", "t": "00:00:05"});
    assert_eq!(
        rows,
        [
            first.clone(),
            json!({"ts": "2024-08-28 00:00:01", "dt": "2024-08-29", "t": "00:00:06"}),
            first,
        ]
    );
}

/// A plan that names a case-sensitive column in another case names no column, and the
/// endpoint does not open.
#[engine_test]
pub fn case_sensitivity(engine: &dyn ConnectorTestEngine) {
    let result = input_pipeline(
        engine,
        &time_relation(),
        &InputOptions::default(),
        config(json!({ "plan": [{ "limit": 2, "fields": { "T": {} } }] })),
    );
    assert!(result.is_err());
}

/// A plan names case-insensitive columns in any case.
#[engine_test]
pub fn case_insensitivity(engine: &dyn ConnectorTestEngine) {
    let rows = generate(
        engine,
        &time_relation(),
        json!({
            "limit": 1,
            "fields": {
                "TS": { "values": ["1970-01-01T00:00:00Z"] },
                "dT": { "values": ["1970-01-02"] },
                "t": { "values": ["00:00:01"] }
            }
        }),
    );
    assert_eq!(
        rows,
        [json!({"ts": "1970-01-01 00:00:00", "dt": "1970-01-02", "t": "00:00:01"})]
    );
}

/// Prints the rate at which `WORKERS` workers (default 1) generate `RECORDS` rows (default
/// 5,000,000).
#[engine_test(ignore = "a benchmark")]
pub fn throughput(engine: &dyn ConnectorTestEngine) {
    let records: usize = env::var("RECORDS")
        .ok()
        .and_then(|records| records.parse().ok())
        .unwrap_or(5_000_000);
    let workers: usize = env::var("WORKERS")
        .ok()
        .and_then(|workers| workers.parse().ok())
        .unwrap_or(1);
    let pipeline = generator(
        engine,
        &time_relation(),
        json!({
            "plan": [{
                "limit": records,
                "fields": { "ts": { "range": ["1970-01-01T00:00:00Z", "1980-01-01T00:00:00Z"] } }
            }],
            "workers": workers
        }),
    );
    // The rate covers generating the rows and flushing them into the table.
    let start = std::time::Instant::now();
    wait(|| pipeline.consumer.state().eoi, default_timeout_ms()).unwrap();
    pipeline.reader.queue(false);
    wait(
        || pipeline.consumer.state().n_extended > 0,
        default_timeout_ms(),
    )
    .unwrap();
    let rate = records as f64 / start.elapsed().as_secs_f64();
    println!("Throughput({workers}, {records}): {rate:.2} records/s");
}
