use super::{
    Admission, CopyBarrier, FelderaDestination, ParkedResults, count_snapshot_buffers, take_acks, *,
};
use crate::{
    PipelineState, RecordFormat,
    test::{MockInputConsumer, engine_test, relations},
    transport::InputQueue,
};
use anyhow::Error as AnyError;
use chrono::Utc;
#[cfg(test)]
use chrono::{FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone};
#[cfg(test)]
use etl::data::{Date, PgNumeric, PgTime, PgTimeTz, Timestamp};
use etl::{schema::PgLsn, store::TableState};
use feldera_adapterlib::{
    format::{BufferSize, ParseError},
    test::{ConnectorTestEngine, InputOptions},
};
#[cfg(test)]
use feldera_sqllib::Timestamp as SqlTimestamp;
use feldera_types::{
    adapter_stats::ConnectorHealth, format::json::JsonFlavor, program_schema::Relation,
};
#[cfg(test)]
use feldera_types::{
    serde_with_context::{DeserializeWithContext, SqlSerdeConfig},
    size::ConfigSize,
};
use serde_json::json;
#[cfg(test)]
use std::str::FromStr;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::watch::channel;

#[test]
fn setup_hint_only_for_permission_or_missing_etl_helper_errors() {
    for run_source_migrations in [false, true] {
        for diagnostic in [
            "permission denied for schema etl",
            "permission denied for table public.orders",
            "must be superuser to create an event trigger",
            "must be a superuser to create an event trigger",
            "must be owner of function emit_schema_change_messages",
            "function etl.describe_table_schema(integer) does not exist",
            "function etl.describe_table_identity(oid) does not exist",
        ] {
            let error = with_setup_hint(anyhow!(diagnostic), run_source_migrations).to_string();
            assert!(error.starts_with(diagnostic), "{error}");
            assert!(
                error.contains(&format!(
                    "With run_source_migrations={run_source_migrations}"
                )),
                "{error}"
            );
            assert!(error.contains("#running-as-a-non-superuser"), "{error}");
            // A missing SELECT names the table, so the runtime hint must
            // point at the published tables and not only at etl's objects.
            assert_eq!(
                error.contains("SELECT on every table in the publication"),
                !run_source_migrations,
                "{error}"
            );
        }
        for diagnostic in [
            "connection refused",
            "password authentication failed",
            "relation public.orders does not exist",
            "function public.unrelated(oid) does not exist",
            "Postgres CDC source table is missing required Feldera columns",
        ] {
            assert_eq!(
                with_setup_hint(anyhow!(diagnostic), run_source_migrations).to_string(),
                diagnostic
            );
        }
    }
}

#[test]
fn setup_hint_startup_diagnostic_is_displayed_once() {
    const DIAGNOSTIC: &str = "permission denied for schema etl";
    const CONTEXT: &str = "failed to start etl pipeline";
    for run_source_migrations in [false, true] {
        let e = etl_error!(
            ErrorKind::SourceQueryFailed,
            "Failed to run ETL source migrations"
        )
        .with_source(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            DIAGNOSTIC,
        ));
        // Match pipeline.start()'s error path, including the etl source
        // and the startup context that already embeds its diagnostic.
        let message = format!("{CONTEXT}: {e}");
        let error = with_setup_hint(anyhow!(e).context(message), run_source_migrations);
        let controller_error = ControllerError::input_transport_error("cdc", true, error);
        let displayed = controller_error.to_string();
        assert_eq!(displayed.matches(DIAGNOSTIC).count(), 1, "{displayed}");
        assert_eq!(displayed.matches(CONTEXT).count(), 1, "{displayed}");
        assert_eq!(displayed.matches("Hint:").count(), 1, "{displayed}");

        let serialized = serde_json::to_value(&controller_error).unwrap();
        let serialized_error = serialized["error"].as_str().unwrap();
        assert_eq!(
            serialized_error.matches(DIAGNOSTIC).count(),
            1,
            "{serialized_error}"
        );
        assert_eq!(
            serialized_error.matches(CONTEXT).count(),
            1,
            "{serialized_error}"
        );
        assert_eq!(
            serialized_error.matches("Hint:").count(),
            1,
            "{serialized_error}"
        );
    }
}

#[test]
fn setup_hint_covers_replication_and_avoids_duplicate_periods() {
    for diagnostic in [
        "permission denied to start WAL sender",
        "permission denied to start WAL sender.",
        "permission denied to start WAL sender.\n",
    ] {
        let error = with_setup_hint(anyhow!(diagnostic), false).to_string();
        assert!(error.contains("WAL sender. Hint:"), "{error}");
        assert!(
            error.contains("runtime role also needs the REPLICATION attribute"),
            "{error}"
        );
    }
    let error = with_setup_hint(anyhow!("permission denied for schema etl"), true).to_string();
    assert!(
        error.contains("connect as a superuser to install etl's source objects."),
        "{error}"
    );
    assert!(
        error.contains("To use a non-superuser role instead, have an administrator install the source objects and apply the required runtime grants"),
        "{error}"
    );
    assert!(!error.contains("in either case"), "{error}");
}

/// The startup rollback recognizes both errors an older connector left
/// behind at a stop: the one it recorded itself when Feldera stopped it
/// mid-batch, and the one etl records for a deferred result it dropped.
#[test]
fn startup_rollback_recognizes_the_stop_errors_of_older_connectors() {
    let terminated = etl_error!(
        ErrorKind::DestinationError,
        LEGACY_STOP_ERRORS[0],
        "state channel closed"
    );
    assert!(is_legacy_stop_error(&terminated.to_string()));
    let dropped = etl_error!(
        ErrorKind::DestinationError,
        LEGACY_STOP_ERRORS[1],
        "table sync worker drained during shutdown"
    );
    assert!(is_legacy_stop_error(&dropped.to_string()));
    // A rollback that fired on an unrelated destination error would replay
    // writes etl already acknowledged, so the matcher must stay narrow.
    assert!(!is_legacy_stop_error("connection refused"));
    assert!(!is_legacy_stop_error(
        "Table copy durability barrier did not confirm durability"
    ));
}

// -----------------------------------------------------------------------
// cell_to_json unit tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// array_cell_to_json unit tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// row_to_json unit tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// stable_connection_identity unit tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Copy barrier unit tests
// -----------------------------------------------------------------------
//
// These drive the production [`CopyBarrier`] through the calls the
// destination and the reader make: `note_copy_batch`, `note_buffer_queued`
// and `hold_terminal` for the destination, `note_buffers_flushed` for the
// reader. etl keeps the constructor of a `WriteTableRowsResult` private, so
// the terminal barrier is answered here by setting a flag, exactly as the
// destination answers it by sending `Durable`.

// -----------------------------------------------------------------------
// parse_pg_uri unit tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Target table matching / column resolution tests
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Acknowledgment against an unnamed step
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// A flush that races the completion watcher's stop
// -----------------------------------------------------------------------

#[test]
fn test_cell_null() {
    assert_eq!(cell_to_json(&Cell::Null), Value::Null);
}

#[test]
fn test_cell_bool() {
    assert_eq!(cell_to_json(&Cell::Bool(true)), json!(true));
    assert_eq!(cell_to_json(&Cell::Bool(false)), json!(false));
}

#[test]
fn test_cell_string() {
    assert_eq!(cell_to_json(&Cell::String("hello".into())), json!("hello"));
    assert_eq!(cell_to_json(&Cell::String("".into())), json!(""));
    // Unicode
    assert_eq!(
        cell_to_json(&Cell::String("caf\u{00e9}".into())),
        json!("caf\u{00e9}")
    );
}

#[test]
fn test_cell_integers() {
    assert_eq!(cell_to_json(&Cell::I16(42)), json!(42));
    assert_eq!(cell_to_json(&Cell::I16(-1)), json!(-1));
    assert_eq!(cell_to_json(&Cell::I32(100_000)), json!(100_000));
    assert_eq!(
        cell_to_json(&Cell::U32(4_000_000_000)),
        json!(4_000_000_000u64)
    );
    assert_eq!(cell_to_json(&Cell::I64(i64::MAX)), json!(i64::MAX));
    assert_eq!(cell_to_json(&Cell::I64(i64::MIN)), json!(i64::MIN));
}

#[test]
fn test_cell_f32() {
    assert_eq!(cell_to_json(&Cell::F32(3.5)), json!(3.5f32));
    // NaN and infinity produce null
    assert_eq!(cell_to_json(&Cell::F32(f32::NAN)), Value::Null);
    assert_eq!(cell_to_json(&Cell::F32(f32::INFINITY)), Value::Null);
    assert_eq!(cell_to_json(&Cell::F32(f32::NEG_INFINITY)), Value::Null);
}

#[test]
fn test_cell_f64() {
    assert_eq!(cell_to_json(&Cell::F64(2.25)), json!(2.25f64));
    assert_eq!(cell_to_json(&Cell::F64(f64::NAN)), Value::Null);
    assert_eq!(cell_to_json(&Cell::F64(f64::INFINITY)), Value::Null);
    assert_eq!(cell_to_json(&Cell::F64(f64::NEG_INFINITY)), Value::Null);
}

#[test]
fn test_cell_numeric() {
    let n = PgNumeric::from_str("123.456").unwrap();
    let v = cell_to_json(&Cell::Numeric(n));
    assert_eq!(v, json!("123.456"));
}

#[test]
fn test_cell_date() {
    let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let v = cell_to_json(&Cell::Date(Date::Value(d)));
    assert_eq!(v, json!("2024-06-15"));
}

#[test]
fn test_cell_time() {
    let t = NaiveTime::from_hms_opt(14, 30, 0).unwrap();
    let v = cell_to_json(&Cell::Time(PgTime::Value(t)));
    assert_eq!(v, json!("14:30:00"));
}

#[test]
fn test_cell_timetz() {
    let t = PgTimeTz::new(
        NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
        FixedOffset::east_opt(2 * 3600).unwrap(),
    );
    let v = cell_to_json(&Cell::TimeTz(t));
    assert_eq!(v, json!("14:30:00+02"));
}

#[test]
fn test_cell_timestamp() {
    let dt = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
    );
    let v = cell_to_json(&Cell::Timestamp(Timestamp::Value(dt)));
    assert_eq!(v, json!("2024-01-01T12:00:00"));
}

/// `%.f` prints 0, 3, 6 or 9 fraction digits, the fewest that lose
/// nothing. Feldera's parser for the connector's JSON flavor must take
/// all four and keep the microseconds, which is all a Postgres timestamp
/// carries.
#[test]
fn timestamp_fractions_round_trip_through_the_parser() {
    let config = SqlSerdeConfig::from(JsonFlavor::Datagen);
    let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
    for (nanos, fraction) in [
        (0, ""),
        (123_000_000, ".123"),
        (123_456_000, ".123456"),
        (123_456_789, ".123456789"),
    ] {
        let dt = NaiveDateTime::new(date, NaiveTime::from_hms_nano_opt(12, 0, 0, nanos).unwrap());
        let json = cell_to_json(&Cell::Timestamp(Timestamp::Value(dt)));
        assert_eq!(json, json!(format!("2024-01-01T12:00:00{fraction}")));

        let text = json.to_string();
        let parsed =
            <SqlTimestamp as DeserializeWithContext<SqlSerdeConfig, ()>>::deserialize_with_context(
                &mut serde_json::Deserializer::from_str(&text),
                &config,
            )
            .unwrap_or_else(|e| panic!("Feldera rejects {text}: {e}"));
        assert_eq!(parsed.microseconds(), dt.and_utc().timestamp_micros());
    }
}

#[test]
fn test_cell_timestamptz() {
    let dt = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
    let v = cell_to_json(&Cell::TimestampTz(Timestamp::Value(dt)));
    // RFC 3339 format
    assert_eq!(v, json!("2024-01-01T12:00:00+00:00"));
}

#[test]
fn test_cell_temporal_special_values_keep_postgres_spelling() {
    assert_eq!(
        cell_to_json(&Cell::Date(Date::PosInfinity)),
        json!("infinity")
    );
    assert_eq!(
        cell_to_json(&Cell::Date(Date::NegInfinity)),
        json!("-infinity")
    );
    assert_eq!(
        cell_to_json(&Cell::Time(PgTime::EndOfDay)),
        json!("24:00:00")
    );
    assert_eq!(
        cell_to_json(&Cell::Timestamp(Timestamp::PosInfinity)),
        json!("infinity")
    );
    assert_eq!(
        cell_to_json(&Cell::TimestampTz(Timestamp::NegInfinity)),
        json!("-infinity")
    );
    let arr = ArrayCell::Timestamp(vec![Some(Timestamp::NegInfinity), None]);
    assert_eq!(array_cell_to_json(&arr), json!(["-infinity", null]));
}

#[test]
fn test_cell_uuid() {
    let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let v = cell_to_json(&Cell::Uuid(u));
    assert_eq!(v, json!("550e8400-e29b-41d4-a716-446655440000"));
}

#[test]
fn test_cell_json() {
    let j = json!({"key": "value", "num": 42});
    let v = cell_to_json(&Cell::Json(j.clone()));
    assert_eq!(v, j);
}

#[test]
fn test_cell_bytes() {
    let v = cell_to_json(&Cell::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    assert_eq!(v, json!([0xde, 0xad, 0xbe, 0xef]));
}

#[test]
fn test_cell_bytes_empty() {
    let v = cell_to_json(&Cell::Bytes(vec![]));
    assert_eq!(v, json!([]));
}

#[test]
fn test_array_bool() {
    let arr = ArrayCell::Bool(vec![Some(true), Some(false), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([true, false, null]));
}

#[test]
fn test_array_string() {
    let arr = ArrayCell::String(vec![Some("a".into()), None, Some("b".into())]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["a", null, "b"]));
}

#[test]
fn test_array_i16() {
    let arr = ArrayCell::I16(vec![Some(1), Some(-2), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([1, -2, null]));
}

#[test]
fn test_array_i32() {
    let arr = ArrayCell::I32(vec![Some(100), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([100, null]));
}

#[test]
fn test_array_i64() {
    let arr = ArrayCell::I64(vec![Some(i64::MAX), None, Some(0)]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([i64::MAX, null, 0]));
}

#[test]
fn test_array_f32_with_special() {
    let arr = ArrayCell::F32(vec![Some(1.5), None, Some(f32::NAN), Some(f32::INFINITY)]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([1.5f32, null, null, null]));
}

#[test]
fn test_array_f64_with_special() {
    let arr = ArrayCell::F64(vec![Some(2.5), Some(f64::NEG_INFINITY), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([2.5f64, null, null]));
}

#[test]
fn test_array_numeric() {
    let n = PgNumeric::from_str("99.99").unwrap();
    let arr = ArrayCell::Numeric(vec![Some(n), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["99.99", null]));
}

#[test]
fn test_array_date() {
    let d = NaiveDate::from_ymd_opt(2024, 12, 25).unwrap();
    let arr = ArrayCell::Date(vec![Some(Date::Value(d)), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["2024-12-25", null]));
}

#[test]
fn test_array_time() {
    let t = NaiveTime::from_hms_opt(8, 30, 0).unwrap();
    let arr = ArrayCell::Time(vec![Some(PgTime::Value(t)), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["08:30:00", null]));
}

#[test]
fn test_array_timetz() {
    let t = PgTimeTz::new(
        NaiveTime::from_hms_opt(8, 30, 0).unwrap(),
        FixedOffset::west_opt(5 * 3600).unwrap(),
    );
    let arr = ArrayCell::TimeTz(vec![Some(t), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["08:30:00-05", null]));
}

#[test]
fn test_array_timestamp() {
    let dt = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let arr = ArrayCell::Timestamp(vec![Some(Timestamp::Value(dt)), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["2024-06-01T00:00:00", null]));
}

#[test]
fn test_array_timestamptz() {
    let dt = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
    let arr = ArrayCell::TimestampTz(vec![Some(Timestamp::Value(dt)), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["2024-06-01T00:00:00+00:00", null]));
}

#[test]
fn test_array_uuid() {
    let u = uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
    let arr = ArrayCell::Uuid(vec![Some(u), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!(["12345678-1234-1234-1234-123456789abc", null]));
}

#[test]
fn test_array_json() {
    let j = json!({"a": 1});
    let arr = ArrayCell::Json(vec![Some(j.clone()), None]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([{"a": 1}, null]));
}

#[test]
fn test_array_bytes() {
    let arr = ArrayCell::Bytes(vec![Some(vec![0xca, 0xfe]), None, Some(vec![])]);
    let v = array_cell_to_json(&arr);
    assert_eq!(v, json!([[0xca, 0xfe], null, []]));
}

#[test]
fn test_row_to_json_basic() {
    let cells = vec![Cell::I32(1), Cell::String("hello".into()), Cell::Bool(true)];
    let cols = vec!["id".into(), "name".into(), "active".into()];
    let v = row_to_json(&cells, &cols, "cdc_in");
    assert_eq!(v, json!({"id": 1, "name": "hello", "active": true}));
}

#[test]
fn test_row_to_json_with_null() {
    let cells = vec![Cell::I32(42), Cell::Null];
    let cols = vec!["id".into(), "value".into()];
    let v = row_to_json(&cells, &cols, "cdc_in");
    assert_eq!(v, json!({"id": 42, "value": null}));
}

#[test]
fn test_row_to_json_more_cells_than_columns() {
    // Extra cells get auto-generated column names
    let cells = vec![Cell::I32(1), Cell::I32(2), Cell::I32(3)];
    let cols = vec!["a".into(), "b".into()];
    let v = row_to_json(&cells, &cols, "cdc_in");
    assert_eq!(v, json!({"a": 1, "b": 2, "col_2": 3}));
}

#[test]
fn test_row_to_json_all_types() {
    let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
    let t = NaiveTime::from_hms_opt(10, 30, 0).unwrap();
    let ts = NaiveDateTime::new(d, t);
    let tstz = Utc.with_ymd_and_hms(2024, 3, 15, 10, 30, 0).unwrap();
    let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let n = PgNumeric::from_str("123.45").unwrap();

    let cells = vec![
        Cell::Bool(true),
        Cell::I16(16),
        Cell::I32(32),
        Cell::U32(4_000_000_000),
        Cell::I64(64),
        Cell::F32(1.5),
        Cell::F64(2.5),
        Cell::Numeric(n),
        Cell::String("text".into()),
        Cell::Date(Date::Value(d)),
        Cell::Time(PgTime::Value(t)),
        Cell::Timestamp(Timestamp::Value(ts)),
        Cell::TimestampTz(Timestamp::Value(tstz)),
        Cell::Uuid(u),
        Cell::Json(json!({"key": "val"})),
        Cell::Bytes(vec![0xab, 0xcd]),
        Cell::Null,
        Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), None])),
    ];
    let cols: Vec<String> = vec![
        "bool_col",
        "i16_col",
        "i32_col",
        "u32_col",
        "i64_col",
        "f32_col",
        "f64_col",
        "numeric_col",
        "text_col",
        "date_col",
        "time_col",
        "ts_col",
        "tstz_col",
        "uuid_col",
        "json_col",
        "bytes_col",
        "null_col",
        "arr_col",
    ]
    .into_iter()
    .map(String::from)
    .collect();

    let v = row_to_json(&cells, &cols, "cdc_in");
    assert_eq!(v["bool_col"], json!(true));
    assert_eq!(v["i16_col"], json!(16));
    assert_eq!(v["i32_col"], json!(32));
    assert_eq!(v["u32_col"], json!(4_000_000_000u64));
    assert_eq!(v["i64_col"], json!(64));
    assert_eq!(v["f32_col"], json!(1.5f32));
    assert_eq!(v["f64_col"], json!(2.5f64));
    assert_eq!(v["numeric_col"], json!("123.45"));
    assert_eq!(v["text_col"], json!("text"));
    assert_eq!(v["date_col"], json!("2024-03-15"));
    assert_eq!(v["time_col"], json!("10:30:00"));
    assert_eq!(v["ts_col"], json!("2024-03-15T10:30:00"));
    assert_eq!(v["tstz_col"], json!("2024-03-15T10:30:00+00:00"));
    assert_eq!(v["uuid_col"], json!("550e8400-e29b-41d4-a716-446655440000"));
    assert_eq!(v["json_col"], json!({"key": "val"}));
    assert_eq!(v["bytes_col"], json!([0xab, 0xcd]));
    assert_eq!(v["null_col"], Value::Null);
    assert_eq!(v["arr_col"], json!([1, 2, null]));
}

#[test]
fn test_stable_identity_excludes_password() {
    let id1 = stable_connection_identity(
        "postgres://user:oldpass@localhost:5432/db",
        "pub",
        "public.tbl",
    );
    let id2 = stable_connection_identity(
        "postgres://user:newpass@localhost:5432/db",
        "pub",
        "public.tbl",
    );
    assert_eq!(
        id1, id2,
        "rotating the password should not change the stable identity"
    );
}

#[test]
fn test_stable_identity_excludes_username() {
    let id1 = stable_connection_identity("postgres://alice@localhost:5432/db", "pub", "public.tbl");
    let id2 = stable_connection_identity("postgres://bob@localhost:5432/db", "pub", "public.tbl");
    assert_eq!(id1, id2);
}

#[test]
fn test_stable_identity_changes_with_host() {
    let id1 = stable_connection_identity("postgres://u:p@host1:5432/db", "pub", "public.tbl");
    let id2 = stable_connection_identity("postgres://u:p@host2:5432/db", "pub", "public.tbl");
    assert_ne!(id1, id2);
}

#[test]
fn test_stable_identity_changes_with_publication() {
    let id1 = stable_connection_identity("postgres://u:p@host:5432/db", "pub1", "public.tbl");
    let id2 = stable_connection_identity("postgres://u:p@host:5432/db", "pub2", "public.tbl");
    assert_ne!(id1, id2);
}

#[test]
fn test_stable_identity_invalid_uri_falls_back() {
    // Parse failure falls back to using the raw URI — identity is still
    // deterministic, just less robust to URI format variations.
    let id = stable_connection_identity("not a valid uri", "pub", "tbl");
    assert!(id.contains("not a valid uri"));
}

/// Acknowledgments of one write, waiting for `flush_step` to become
/// durable, answered by raising `answered`.
#[cfg(test)]
fn held(flush_step: Step, answered: &Arc<AtomicBool>) -> HeldAcks {
    HeldAcks {
        flush_step,
        acks: vec![terminal(answered)],
    }
}

/// The table whose input stream the destination writes to, with the columns of
/// [`relations::basic`].
pub fn relation() -> Relation {
    relations::basic("postgres_cdc_destination")
}

/// Answer a terminal barrier by raising `answered`.
pub(super) fn terminal(answered: &Arc<AtomicBool>) -> Box<dyn FnOnce() + Send> {
    let answered = Arc::clone(answered);
    Box::new(move || answered.store(true, Ordering::Release))
}

/// Whether the barrier has been answered.
pub(super) fn answered(flag: &Arc<AtomicBool>) -> bool {
    flag.load(Ordering::Acquire)
}

#[test]
fn a_terminal_barrier_waits_for_the_last_snapshot_buffer() {
    let barrier = CopyBarrier::new();
    assert!(
        !barrier.note_copy_batch(),
        "the first copy finds the barrier already up"
    );
    barrier.note_buffer_queued();
    barrier.note_buffer_queued();

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        !barrier.hold_terminal(terminal(&flag)),
        "the barrier stays up, so the reader's own flush reports it"
    );
    assert!(!answered(&flag), "two buffers are still queued");

    assert!(
        barrier.note_buffers_flushed(1),
        "the barrier is held while a buffer is left"
    );
    assert!(!answered(&flag));
    assert!(barrier.copy_open());

    assert!(
        !barrier.note_buffers_flushed(1),
        "the last buffer answers the barrier"
    );
    assert!(answered(&flag));
    assert!(
        barrier.copy_open(),
        "the barrier stays up until etl is seen to record the copy"
    );
    assert!(barrier.note_sync_state_observed(&sync_done(500)));
    assert!(!barrier.copy_open());
    assert_eq!(barrier.sync_done_lsn(), Some(500));
}

/// etl's `sync_done` state at `lsn`.
pub(super) fn sync_done(lsn: u64) -> TableState {
    TableState::SyncDone {
        lsn: PgLsn::from(lsn),
        table_decoding_state: None,
    }
}

#[test]
fn the_barrier_stays_up_until_etl_records_the_copy() {
    let barrier = CopyBarrier::new();
    barrier.note_copy_batch();
    let flag = Arc::new(AtomicBool::new(false));
    assert!(barrier.hold_terminal(terminal(&flag)));
    assert!(barrier.copy_open(), "etl has not recorded the copy yet");
    // States on the way to the record do not lower it.
    assert!(!barrier.note_sync_state_observed(&TableState::FinishedCopy));
    assert!(barrier.copy_open());
    // `ready` counts as recorded too, and needs no LSN.
    assert!(barrier.note_sync_state_observed(&TableState::Ready));
    assert!(!barrier.copy_open());
    assert_eq!(barrier.sync_done_lsn(), None);
    // Seen again outside a wait, the state lowers nothing.
    assert!(!barrier.note_sync_state_observed(&sync_done(7)));
    assert_eq!(barrier.sync_done_lsn(), Some(7));
}

#[test]
fn a_terminal_barrier_with_nothing_queued_is_answered_at_once() {
    // What an empty source table, or one whose rows the circuit already
    // holds, looks like from here.
    let barrier = CopyBarrier::new();
    barrier.note_copy_batch();

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        barrier.hold_terminal(terminal(&flag)),
        "the barrier came down away from the reader, which must be stepped \
         for it to report the copy done"
    );
    assert!(answered(&flag));
    assert!(barrier.note_sync_state_observed(&sync_done(1)));
    assert!(!barrier.copy_open());
}

#[test]
fn a_copy_that_starts_over_raises_the_barrier_again() {
    let barrier = CopyBarrier::new();
    barrier.note_copy_batch();
    barrier.note_buffer_queued();
    let first = Arc::new(AtomicBool::new(false));
    assert!(!barrier.hold_terminal(terminal(&first)));
    barrier.note_buffers_flushed(1);
    assert!(answered(&first));
    assert!(barrier.note_sync_state_observed(&sync_done(100)));
    assert!(!barrier.copy_open());

    // etl abandons the copy it recorded and reads the table again.
    assert!(
        barrier.note_copy_batch(),
        "a batch that arrives after a copy closed starts a new one"
    );
    assert!(barrier.copy_open());

    barrier.note_buffer_queued();
    let second = Arc::new(AtomicBool::new(false));
    assert!(!barrier.hold_terminal(terminal(&second)));
    assert!(!answered(&second), "the second copy waits on its own rows");
    barrier.note_buffers_flushed(1);
    assert!(answered(&second));
    assert!(barrier.note_sync_state_observed(&sync_done(200)));
    assert!(!barrier.copy_open());
    assert_eq!(barrier.sync_done_lsn(), Some(200));
}

#[test]
fn a_terminal_barrier_waits_for_an_abandoned_attempts_buffers_too() {
    let barrier = CopyBarrier::new();
    barrier.note_copy_batch();
    barrier.note_buffer_queued();
    // etl abandons the attempt without a terminal barrier and starts
    // over; the abandoned buffer still sits in the queue.
    assert!(!barrier.note_copy_batch(), "the barrier never came down");
    barrier.note_buffer_queued();

    let flag = Arc::new(AtomicBool::new(false));
    assert!(!barrier.hold_terminal(terminal(&flag)));
    assert!(
        barrier.note_buffers_flushed(1),
        "one attempt's buffer flushed, the other's still queued"
    );
    assert!(!answered(&flag));
    assert!(!barrier.note_buffers_flushed(1));
    assert!(answered(&flag), "both attempts' buffers are in the circuit");
    assert!(barrier.note_sync_state_observed(&TableState::Ready));
    assert!(!barrier.copy_open());
}

#[test]
fn a_stop_takes_a_held_terminal_barrier_away_unanswered() {
    let barrier = CopyBarrier::new();
    barrier.note_copy_batch();
    barrier.note_buffer_queued();
    let flag = Arc::new(AtomicBool::new(false));
    assert!(!barrier.hold_terminal(terminal(&flag)));

    let taken = barrier.take_terminal();
    assert!(
        taken.is_some(),
        "the stop takes the held barrier, to park it"
    );
    assert!(!answered(&flag), "a stop answers no terminal barrier");
    assert!(barrier.copy_open(), "the copy stays open across a stop");

    // The reader may still flush the buffer the barrier waited for, after
    // the stop took the barrier. The copy is not finished, so nothing may
    // record it as such.
    assert!(
        !barrier.note_buffers_flushed(1),
        "no barrier is held once the stop took it"
    );
    assert!(!answered(&flag));
    assert!(
        barrier.copy_open(),
        "a flush after a stop does not record the copy as finished"
    );
}

#[test]
fn a_stop_with_no_terminal_barrier_held_changes_nothing() {
    let barrier = CopyBarrier::new();
    barrier.note_copy_batch();
    // A stop before any copy barrier arrived, and a second stop, which
    // `Drop` makes after `shutdown_etl_pipeline` already ran.
    assert!(barrier.take_terminal().is_none());
    assert!(barrier.take_terminal().is_none());
    assert!(barrier.copy_open());

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        barrier.hold_terminal(terminal(&flag)),
        "an empty copy is answered at once"
    );
    assert!(answered(&flag));
}

/// A panic on the worker task must not turn the bounded runtime shutdown
/// into a hang: dropping a runtime during the unwind waits without bound
/// for the work it hosts, and a blocking task that never returns stands
/// in for an etl worker stuck past the stop grace.
///
/// The blocking task stays stuck until the test releases it, so a
/// regression hangs without end and the generous timeout below cannot
/// mistake a slow CI host for a hang.
#[test]
fn a_panic_on_the_worker_task_still_shuts_the_runtime_down_in_time() {
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let thread = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let stuck = runtime.spawn_blocking(move || {
            let _ = started_tx.send(());
            // Returns once the test drops `release_tx`.
            let _ = release_rx.recv();
        });
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            block_on_then_shut_down(runtime, async move {
                // Shut down only once the blocking task occupies its
                // thread; a queued task is dropped and tests nothing.
                started_rx.await.unwrap();
                // Unlike `panic!`, this skips the panic hook, whose
                // backtrace can take seconds to resolve on CI.
                std::panic::resume_unwind(Box::new("worker task failed"));
            })
        }));
        drop(stuck);
        let _ = done_tx.send(outcome.is_err());
    });

    let panicked = done_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("the wrapper thread hung on the runtime after a panic");
    drop(release_tx);
    assert!(panicked, "the panic is resumed once the runtime is gone");
    thread.join().unwrap();
}

#[test]
fn another_tables_copy_is_no_business_of_the_barrier() {
    let columns = vec!["id".to_string()];
    // A table this connector does not read, rows and terminal write alike.
    assert!(matches!(
        classify_copy_write(None, true),
        CopyWrite::OtherTable
    ));
    assert!(matches!(
        classify_copy_write(None, false),
        CopyWrite::OtherTable
    ));
    // The source table: rows carry a batch id, the terminal barrier does
    // not.
    assert!(matches!(
        classify_copy_write(Some(columns.clone()), true),
        CopyWrite::TargetBatch(names) if names == columns
    ));
    assert!(matches!(
        classify_copy_write(Some(columns), false),
        CopyWrite::TargetBarrier
    ));
}

#[cfg(test)]
fn parse_uri(uri: &str) -> AnyResult<PgConnectionConfig> {
    parse_pg_uri(uri, &PostgresTlsConfig::default(), "test")
}

#[test]
fn test_parse_pg_uri_basic() {
    let config = parse_uri("postgres://user:pass@localhost:5432/mydb").unwrap();
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 5432);
    assert_eq!(config.username, "user");
    assert!(config.password.is_some());
    assert_eq!(config.name, "mydb");
}

#[test]
fn test_parse_pg_uri_default_port() {
    let config = parse_uri("postgres://user:pass@host.example.com/testdb").unwrap();
    assert_eq!(config.port, 5432);
    assert_eq!(config.host, "host.example.com");
}

#[test]
fn test_parse_pg_uri_no_password() {
    let config = parse_uri("postgres://user@localhost/mydb").unwrap();
    assert!(config.password.is_none());
}

#[test]
fn test_parse_pg_uri_missing_username() {
    let result = parse_uri("postgres://localhost/mydb");
    assert!(result.is_err());
}

#[test]
fn test_parse_pg_uri_missing_database() {
    let result = parse_uri("postgres://user:pass@localhost");
    assert!(result.is_err());
}

#[test]
fn test_parse_pg_uri_custom_port() {
    let config = parse_uri("postgres://user:pass@db.host:15432/mydb").unwrap();
    assert_eq!(config.port, 15432);
}

#[test]
fn test_parse_pg_uri_invalid_scheme() {
    let result = parse_uri("not_a_uri");
    assert!(result.is_err());
}

#[test]
fn a_source_table_outside_the_publication_is_named_with_the_alternatives() {
    let tables = |names: &[(&str, &str)]| -> Vec<(String, String)> {
        names
            .iter()
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .collect()
    };
    assert_eq!(
        source_table_missing_from("orders", "p", &tables(&[("public", "orders")])),
        None
    );
    assert_eq!(
        source_table_missing_from("sales.orders", "p", &tables(&[("sales", "orders")])),
        None
    );
    // The breaking change's most likely victim: a bare name for a table
    // outside `public`.
    let problem = source_table_missing_from(
        "orders",
        "p",
        &tables(&[("sales", "orders"), ("archive", "orders")]),
    )
    .expect("a bare name no longer matches a table outside public");
    assert!(
        problem.contains("'orders' is not in publication 'p'"),
        "{problem}"
    );
    assert!(
        problem.contains("sales.orders, archive.orders"),
        "{problem}"
    );
    assert!(problem.contains("schema 'public'"), "{problem}");
    let empty = source_table_missing_from("orders", "p", &[]).unwrap();
    assert!(empty.contains("carries no tables"), "{empty}");
}

#[test]
fn unqualified_source_table_means_public() {
    assert!(is_target_table("orders", "public", "orders"));
    assert!(!is_target_table("orders", "public", "users"));
    // A publication can carry the same table name in several schemas, and
    // a bare name must not pull in every one of them.
    assert!(!is_target_table("orders", "sales", "orders"));
    assert!(!is_target_table("orders", "archive", "orders"));
}

#[test]
fn qualified_source_table_matches_its_schema_only() {
    assert!(is_target_table("public.orders", "public", "orders"));
    assert!(!is_target_table("other.orders", "public", "orders"));
    assert!(is_target_table("sales.orders", "sales", "orders"));
    assert!(!is_target_table("sales.orders", "archive", "orders"));
}

#[test]
fn quoted_source_table_matches_its_schema_only() {
    assert!(is_target_table("\"public\".\"orders\"", "public", "orders"));
    assert!(is_target_table("\"sales\".\"orders\"", "sales", "orders"));
    assert!(!is_target_table("\"other\".\"orders\"", "public", "orders"));
}

/// Mirrors `validate_columns`: every non-nullable Feldera column must exist
/// (by name) in the Postgres source table.
#[cfg(test)]
fn missing_required<'a>(pg_columns: &[&str], feldera_required: &[&'a str]) -> Vec<&'a str> {
    let pg: BTreeSet<&str> = pg_columns.iter().copied().collect();
    feldera_required
        .iter()
        .copied()
        .filter(|c| !pg.contains(c))
        .collect()
}

#[test]
fn test_required_columns_present() {
    // All required present, different order -> valid.
    assert!(missing_required(&["name", "id"], &["id", "name"]).is_empty());
    // Extra Postgres column (extra) -> still valid.
    assert!(missing_required(&["id", "name", "extra"], &["id", "name"]).is_empty());
    // No required columns (all Feldera columns nullable) -> always valid.
    assert!(missing_required(&["id"], &[]).is_empty());
    // A required column absent from Postgres -> reported missing.
    assert_eq!(missing_required(&["id"], &["id", "name"]), vec!["name"]);
    // Renamed columns (source id,name vs required c0,c1) -> both missing.
    assert_eq!(
        missing_required(&["id", "name"], &["c0", "c1"]),
        vec!["c0", "c1"]
    );
}

/// A destination with a live queue and a running pipeline, sharing `copy`.
fn destination(engine: &dyn ConnectorTestEngine, copy: &Arc<CopyBarrier>) -> FelderaDestination {
    destination_with(engine, copy, MockInputConsumer::new(), false)
}

/// A destination sharing `consumer` with its queue, so a test can set the
/// batch size the reader's flush obeys, and deferring its answers to etl
/// when `defer_acks` says so.
fn destination_with(
    engine: &dyn ConnectorTestEngine,
    copy: &Arc<CopyBarrier>,
    consumer: MockInputConsumer,
    defer_acks: bool,
) -> FelderaDestination {
    destination_stopping(
        engine,
        copy,
        consumer,
        defer_acks,
        &Arc::new(AtomicBool::new(false)),
    )
}

/// A destination that shares `stopping` with a reader shutting etl down.
fn destination_stopping(
    engine: &dyn ConnectorTestEngine,
    copy: &Arc<CopyBarrier>,
    consumer: MockInputConsumer,
    defer_acks: bool,
    stopping: &Arc<AtomicBool>,
) -> FelderaDestination {
    let stream = engine
        .input_collection(&relation(), &InputOptions::default())
        .unwrap()
        .input_handle()
        .handle
        .configure_deserializer(RecordFormat::Json(JsonFlavor::Datagen))
        .unwrap();
    let (_state_tx, pipeline_state_rx) = channel(PipelineState::Running);
    FelderaDestination {
        input_stream: Arc::new(Mutex::new(stream)),
        queue: Arc::new(InputQueue::new(Box::new(consumer.clone()))),
        source_table: "public.t".to_string(),
        endpoint_name: "cdc_in".to_string(),
        feldera_required_columns: Vec::new(),
        defer_acks,
        pipeline_state_rx,
        copy: Arc::clone(copy),
        consumer: Box::new(consumer),
        stopping: Arc::clone(stopping),
        high_water: Arc::new(Mutex::new(HighWater::default())),
    }
}

/// One row of streamed events, serialized through the destination's own
/// deserializer as `write_events` does, so the buffer holds a record and
/// counts against the reader's batch size.
fn event_buffer(
    destination: &FelderaDestination,
    id: u32,
) -> (
    Option<Box<dyn feldera_adapterlib::format::InputBuffer>>,
    Vec<ParseError>,
) {
    let mut stream = destination.input_stream.lock().unwrap();
    let json = json!({ "id": id, "b": false, "i": null, "s": "" }).to_string();
    stream.insert(json.as_bytes(), &None).unwrap();
    (stream.take_all(), Vec::new())
}

#[engine_test]
pub fn a_queued_snapshot_buffer_holds_the_terminal_barrier_until_the_reader_flushes_it(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let destination = destination(engine, &copy);
    destination.push_snapshot_buffer((None, Vec::new()), Utc::now());
    destination.push_snapshot_buffer((None, Vec::new()), Utc::now());

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        !copy.hold_terminal(terminal(&flag)),
        "two buffers sit in the queue, so the barrier is held"
    );
    assert!(!answered(&flag));

    // The reader's flush, as the Queue handler performs it.
    let (_, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(count_snapshot_buffers(&flushed), 2);
    assert!(!copy.note_buffers_flushed(count_snapshot_buffers(&flushed)));
    assert!(
        take_acks(flushed).is_empty(),
        "the copy answers etl through the barrier, never per entry"
    );
    assert!(answered(&flag), "the flush answered etl");
    assert!(copy.note_sync_state_observed(&sync_done(3)));
    assert!(!copy.copy_open());
}

// -----------------------------------------------------------------------
// Streamed write acknowledgment (#7122)
// -----------------------------------------------------------------------
//
// A write is answered only once every row of it is in the circuit. These
// drive the production destination and the queue the reader flushes, and
// cross the two through `take_acks`, as the Queue handler does.
//
// To validate them, restore the architecture the fix replaced: have
// `push_event_buffer` drop the answer into one shared list and `take_acks`
// drain that list instead of reading the entries its flush returned. Both
// tests then go red, the first because the opening flush answers a write
// whose rows are still queued, the second because the answer of a write
// that arrived mid-handler goes out with the flush that preceded it.

#[engine_test]
pub fn a_streamed_write_is_answered_only_when_its_last_buffer_reaches_the_circuit(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let consumer = MockInputConsumer::new();
    // One record per step, so the write spans three flushes. This is the
    // `max_batch_size` split of #7122, without depending on its default.
    consumer.set_max_batch_size(1);
    let destination = destination_with(engine, &copy, consumer, true);

    let flag = Arc::new(AtomicBool::new(false));
    for id in 0..2 {
        let buffer = event_buffer(&destination, id);
        destination.push_event_buffer(buffer, Utc::now(), None);
    }
    let last = event_buffer(&destination, 2);
    destination.push_event_buffer(last, Utc::now(), Some(terminal(&flag)));

    for flush in 1..=2 {
        let (_, _, flushed) = destination.queue.flush_with_aux();
        let taken = flushed.len();
        assert!(
            take_acks(flushed).is_empty(),
            "flush {flush} left rows of the write in the queue, so it must not \
         answer etl for it"
        );
        assert!(!answered(&flag));
        assert_eq!(
            taken, 1,
            "flush {flush} took more than its one buffer, so the batch size \
         stopped splitting the write and the test proves nothing"
        );
    }

    let (_, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(flushed.len(), 1);
    let mut acks = take_acks(flushed);
    assert_eq!(acks.len(), 1, "the last buffer carries the answer");
    acks.pop().unwrap()();
    assert!(answered(&flag), "every row of the write is in the circuit");
}

#[engine_test]
pub fn a_write_that_arrives_while_the_handler_runs_is_not_answered_by_it(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let destination = destination_with(engine, &copy, MockInputConsumer::new(), true);

    let buffer = event_buffer(&destination, 0);
    destination.push_event_buffer(buffer, Utc::now(), None);

    // The handler's flush.
    let (_, _, flushed) = destination.queue.flush_with_aux();

    // etl hands a write over here, after the flush and before the handler
    // answers: the second race of #7122. The old handler took every stored
    // sender at this point, stamping this write with a step that holds no
    // row of it.
    let flag = Arc::new(AtomicBool::new(false));
    let buffer = event_buffer(&destination, 1);
    destination.push_event_buffer(buffer, Utc::now(), Some(terminal(&flag)));

    // The handler's answers, drawn from what its own flush returned.
    assert!(
        take_acks(flushed).is_empty(),
        "the write's rows are still in the queue"
    );
    assert!(!answered(&flag));

    let (_, _, flushed) = destination.queue.flush_with_aux();
    let mut acks = take_acks(flushed);
    assert_eq!(acks.len(), 1, "the flush that takes its rows answers it");
    acks.pop().unwrap()();
    assert!(answered(&flag));
}

#[engine_test]
pub fn a_write_that_starts_once_the_connector_stops_is_turned_away_without_error(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let stopping = Arc::new(AtomicBool::new(false));
    let destination =
        destination_stopping(engine, &copy, MockInputConsumer::new(), true, &stopping);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    assert_eq!(
        runtime.block_on(destination.wait_unpaused()),
        Admission::Admit
    );

    // etl may still issue a write after the stop raised the flag. Refusing
    // it would make etl record a table error; turning it away leaves etl
    // to abandon it, and nothing reaches the queue.
    stopping.store(true, Ordering::SeqCst);
    assert_eq!(
        runtime.block_on(destination.wait_unpaused()),
        Admission::Stopping
    );
    let (size, _, _) = destination.queue.flush_with_aux();
    assert_eq!(size.records, 0, "nothing reached the queue");
}

/// An acknowledgment that answers into a oneshot, so a test can tell an
/// answered write from a parked one.
#[cfg(test)]
fn oneshot_ack() -> (Ack, tokio::sync::oneshot::Receiver<()>) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    (Box::new(move || tx.send(()).unwrap()), rx)
}

#[test]
fn a_flush_racing_the_stop_answers_the_durable_acks_and_parks_the_rest() {
    use tokio::sync::oneshot::error::TryRecvError;

    // The checkpoint frontier stands at 5: steps below 5 are durable.
    let (_frontier_tx, frontier_rx) = tokio::sync::watch::channel(5u64);
    let watcher_rx = Mutex::new(Some(WatcherReceiver::Strict(frontier_rx)));
    let parked = ParkedResults::default();

    let (durable, mut durable_rx) = oneshot_ack();
    answer_durable_after_stop(&watcher_rx, &parked, 4, vec![durable]);
    assert!(durable_rx.try_recv().is_ok());

    // A write the frontier has not passed is parked: unanswered while the
    // connector lives, closed once it drops.
    let (pending, mut pending_rx) = oneshot_ack();
    answer_durable_after_stop(&watcher_rx, &parked, 5, vec![pending]);
    assert!(matches!(pending_rx.try_recv(), Err(TryRecvError::Empty)));
    drop(parked);
    assert!(matches!(pending_rx.try_recv(), Err(TryRecvError::Closed)));
}

#[test]
fn the_completion_watcher_answers_the_durable_acks_and_parks_the_rest_on_stop() {
    use tokio::sync::oneshot::error::TryRecvError;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_frontier_tx, frontier_rx) = tokio::sync::watch::channel(5u64);
    let (pending_tx, pending_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let parked = Arc::new(ParkedResults::default());
    let (durable, mut durable_rx) = oneshot_ack();
    let (pending, mut pending_ack_rx) = oneshot_ack();

    pending_tx
        .send(HeldAcks {
            flush_step: 4,
            acks: vec![durable],
        })
        .unwrap();
    pending_tx
        .send(HeldAcks {
            flush_step: 9,
            acks: vec![pending],
        })
        .unwrap();
    shutdown_tx.send(()).unwrap();

    runtime.block_on(completion_watcher_task(
        WatcherReceiver::Strict(frontier_rx),
        pending_rx,
        shutdown_rx,
        Arc::clone(&parked),
        "test".to_string(),
    ));

    assert!(durable_rx.try_recv().is_ok());
    assert!(matches!(
        pending_ack_rx.try_recv(),
        Err(TryRecvError::Empty)
    ));
    drop(parked);
    assert!(matches!(
        pending_ack_rx.try_recv(),
        Err(TryRecvError::Closed)
    ));
}

#[test]
fn a_parked_result_keeps_its_sender_alive_until_the_connector_drops() {
    use tokio::sync::oneshot::error::TryRecvError;

    let parked = ParkedResults::default();
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
    parked.park(tx);

    // etl sees a parked result as a write still pending, never as an
    // error: the sender lives on, unanswered.
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

    drop(parked);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Closed)));
}

#[engine_test]
pub fn a_queued_snapshot_buffer_stays_charged_to_the_copy_barrier_until_flushed(
    engine: &dyn ConnectorTestEngine,
) {
    let copy = Arc::new(CopyBarrier::new());
    let destination = destination_stopping(
        engine,
        &copy,
        MockInputConsumer::new(),
        true,
        &Arc::new(AtomicBool::new(false)),
    );

    let buffer = event_buffer(&destination, 0);
    destination.push_snapshot_buffer(buffer, Utc::now());

    // The reader flushes the buffer, and the barrier counts it, which is
    // what lets a terminal barrier be answered afterwards.
    let (_, _, flushed) = destination.queue.flush_with_aux();
    assert_eq!(count_snapshot_buffers(&flushed), 1);
    copy.note_buffers_flushed(count_snapshot_buffers(&flushed));

    let flag = Arc::new(AtomicBool::new(false));
    assert!(
        copy.hold_terminal(terminal(&flag)),
        "no snapshot buffer is left pending, so a terminal barrier answers at once"
    );
    assert!(answered(&flag));
}

#[test]
fn a_stop_answers_the_writes_the_frontier_has_already_passed() {
    let (durable, waiting_still, in_channel) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let mut pending = vec![held(3, &durable), held(9, &waiting_still)];
    // An answer the reader sent but the watcher had not yet received is as
    // durable as one already waiting.
    let (tx, mut rx) = mpsc::unbounded_channel();
    tx.send(held(4, &in_channel)).unwrap();

    answer_durable_on_stop(&mut pending, &mut rx, 5);

    assert!(answered(&durable), "step 3 is durable at frontier 5");
    assert!(answered(&in_channel), "step 4 is durable at frontier 5");
    assert!(
        !answered(&waiting_still),
        "step 9 is not durable, so this write must fail rather than be answered"
    );
    assert_eq!(
        pending.len(),
        1,
        "only the write that is not durable is left"
    );
}

#[test]
fn every_way_a_streamed_write_can_end_is_accounted_for() {
    // (buffers already queued, rows still held back, a step to wait on).
    let expected = [
        ((0, false, false), WriteEnding::AnswerUnqueued),
        ((0, false, true), WriteEnding::AnswerUnqueued),
        ((0, true, false), WriteEnding::QueueTailAndAnswer),
        ((0, true, true), WriteEnding::QueueClosingAck),
        ((2, false, false), WriteEnding::AnswerUnqueued),
        // The write's rows landed exactly on the 2 MiB mark, so the closing
        // entry carries none. It must still be queued, or the answer would
        // ride whichever flush ran next instead of the one holding its
        // rows, which is #7122.
        ((2, false, true), WriteEnding::QueueClosingAck),
        ((2, true, false), WriteEnding::QueueTailAndAnswer),
        ((2, true, true), WriteEnding::QueueClosingAck),
    ];
    for ((queued, tail_rows, defer_acks), ending) in expected {
        assert_eq!(
            write_ending(queued, tail_rows, defer_acks),
            ending,
            "queued={queued} tail_rows={tail_rows} defer_acks={defer_acks}"
        );
    }
}

#[test]
fn an_answer_waits_for_the_frontier_to_pass_the_step_that_holds_its_rows() {
    let (early, late) = (
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(false)),
    );
    let mut waiting = vec![held(3, &early), held(5, &late)];

    // A frontier of n means steps 0..n-1 are durable, so a write flushed
    // during step n waits for n + 1.
    fire_completed(&mut waiting, 3);
    assert!(!answered(&early), "step 3 is not durable yet");
    fire_completed(&mut waiting, 4);
    assert!(answered(&early));
    assert!(!answered(&late));
    fire_completed(&mut waiting, 5);
    assert!(!answered(&late), "step 5 is not durable yet");
    fire_completed(&mut waiting, 6);
    assert!(answered(&late));
    assert!(waiting.is_empty());
}

#[test]
fn a_stalled_table_is_fatal_only_before_etl_takes_it_up() {
    // Unresolved or still `init`: etl never picked the table up.
    assert_eq!(stall_verdict(None), StallVerdict::NeverPickedUp);
    assert_eq!(
        stall_verdict(Some(&TableState::Init)),
        StallVerdict::NeverPickedUp
    );
    // Inside the copy, etl may be waiting on the source for as long as a
    // transaction runs there; that must not fail the pipeline.
    assert_eq!(
        stall_verdict(Some(&TableState::DataSync)),
        StallVerdict::WaitingOnSource
    );
    assert_eq!(
        stall_verdict(Some(&TableState::FinishedCopy)),
        StallVerdict::WaitingOnSource
    );
}

/// An etl table error as the store hands it back.
#[cfg(test)]
fn errored(reason: &str, retry_policy: TableRetryPolicy) -> TableState {
    TableState::Errored {
        reason: reason.to_string(),
        solution: None,
        retry_policy,
        source_err: etl_error!(ErrorKind::DestinationError, "test fixture"),
    }
}

#[test]
fn startup_rolls_back_only_what_a_restart_makes_moot() {
    // An older connector's stop left this behind; see `LEGACY_STOP_ERRORS`.
    let shutdown = errored(LEGACY_STOP_ERRORS[0], TableRetryPolicy::ManualRetry);
    assert_eq!(
        reconcile_decision(&shutdown, false, ResumedCopy::NoCheckpoint),
        Reconcile::RollBack
    );
    let timed = errored(
        "connection refused",
        TableRetryPolicy::TimedRetry {
            next_retry: Utc::now(),
        },
    );
    assert_eq!(
        reconcile_decision(&timed, false, ResumedCopy::NoCheckpoint),
        Reconcile::RollBack
    );
    // An error only etl or the user can clear stays put; the monitor
    // reports it once the pipeline runs.
    let manual = errored("unsupported schema change", TableRetryPolicy::ManualRetry);
    assert_eq!(
        reconcile_decision(&manual, true, ResumedCopy::NoCheckpoint),
        Reconcile::CopyOutstanding
    );
    for state in [
        TableState::Init,
        TableState::DataSync,
        TableState::FinishedCopy,
    ] {
        assert_eq!(
            reconcile_decision(&state, true, ResumedCopy::NoCheckpoint),
            Reconcile::CopyOutstanding,
            "{state:?}"
        );
    }
    assert_eq!(
        reconcile_decision(&TableState::Ready, true, ResumedCopy::NoCheckpoint),
        Reconcile::RedoCopy
    );
    assert_eq!(
        reconcile_decision(&TableState::Ready, false, ResumedCopy::NoCheckpoint),
        Reconcile::SyncCompleted
    );
}

#[test]
fn a_copy_completed_after_the_checkpoint_is_read_again() {
    // The checkpoint recorded the copy etl holds: nothing to do.
    assert_eq!(
        reconcile_decision(&sync_done(200), false, ResumedCopy::CheckpointAt(200)),
        Reconcile::SyncCompleted
    );
    // etl completed a newer copy than the one the checkpoint knows.
    assert_eq!(
        reconcile_decision(&sync_done(200), false, ResumedCopy::CheckpointAt(100)),
        Reconcile::RedoNewerCopy
    );
    // A checkpoint written before the connector recorded copy positions
    // predates every copy etl completed.
    assert_eq!(
        reconcile_decision(&sync_done(200), false, ResumedCopy::CheckpointWithoutLsn),
        Reconcile::RedoNewerCopy
    );
    // A start with no checkpoint has nothing for the copy to be newer
    // than. Under fault tolerance `redo_snapshot` reads the table again
    // because the circuit starts empty; without it the connector streams
    // from the slot and leaves the table's earlier rows out of Feldera,
    // which is what the connector documents. Reading the table again here
    // would repeat every row of it on every restart of such a pipeline.
    assert_eq!(
        reconcile_decision(&sync_done(200), false, ResumedCopy::NoCheckpoint),
        Reconcile::SyncCompleted
    );
    assert_eq!(
        reconcile_decision(&sync_done(200), true, ResumedCopy::NoCheckpoint),
        Reconcile::RedoCopy
    );
    // `Ready` implies a later checkpoint that holds the copy.
    assert_eq!(
        reconcile_decision(&TableState::Ready, false, ResumedCopy::CheckpointAt(100)),
        Reconcile::SyncCompleted
    );
    // No checkpoint at all under fault tolerance outranks the LSN check.
    assert_eq!(
        reconcile_decision(&sync_done(200), true, ResumedCopy::CheckpointAt(200)),
        Reconcile::RedoCopy
    );
}

#[test]
fn a_recreated_table_resolves_to_the_id_etl_replicates_now() {
    let (old, new) = (TableId(10), TableId(20));
    assert_eq!(current_table_id(vec![]), None);
    assert_eq!(current_table_id(vec![(old, false), (new, true)]), Some(new));
    // etl keeps state only for the table it replicates, even when that
    // is the older id.
    assert_eq!(current_table_id(vec![(new, false), (old, true)]), Some(old));
    // No state anywhere: the newest id, which etl is about to copy.
    assert_eq!(
        current_table_id(vec![(new, false), (old, false), (old, false)]),
        Some(new)
    );
}

#[test]
fn a_table_waiting_its_turn_is_not_a_stall() {
    let (target, other) = (TableId(1), TableId(2));
    let states = |v: Vec<(TableId, TableState)>| -> BTreeMap<TableId, TableState> {
        v.into_iter().collect()
    };
    // Only the source table, still `init`: etl has nothing else to do.
    assert!(!nothing_to_report(
        PipelineState::Running,
        &states(vec![(target, TableState::Init)]),
        Some(target)
    ));
    // Another table still to copy: the source table waits its turn.
    assert!(nothing_to_report(
        PipelineState::Running,
        &states(vec![
            (target, TableState::Init),
            (other, TableState::DataSync)
        ]),
        Some(target)
    ));
    // An errored other table does not excuse the stall.
    assert!(!nothing_to_report(
        PipelineState::Running,
        &states(vec![
            (target, TableState::Init),
            (other, errored("boom", TableRetryPolicy::ManualRetry)),
        ]),
        Some(target)
    ));
    // A completed sync is meant to be quiet; a paused pipeline holds
    // everything where it stands.
    assert!(nothing_to_report(
        PipelineState::Running,
        &states(vec![(target, TableState::Ready)]),
        Some(target)
    ));
    assert!(nothing_to_report(
        PipelineState::Paused,
        &states(vec![(target, TableState::Init)]),
        Some(target)
    ));
}

#[test]
fn a_flush_alone_moves_the_progress_clock() {
    let copy = CopyBarrier::new();
    let progress = |copy: &CopyBarrier| TableProgress {
        table_id: None,
        state: None,
        snapshot_buffers_queued: copy.snapshot_buffers_queued(),
        snapshot_buffers_flushed: copy.snapshot_buffers_flushed(),
    };
    copy.note_buffer_queued();
    let terminal_held = progress(&copy);
    // etl has handed over its last batch: nothing more is queued, and the
    // circuit drains what is. That must not read as a stall.
    copy.note_buffers_flushed(1);
    assert_ne!(terminal_held, progress(&copy));
}

#[test]
fn the_reader_reports_a_barrier_while_the_copy_is_open() {
    assert!(matches!(resume_for(true, 7, Some(9)), Resume::Barrier));
    match resume_for(false, 7, Some(9)) {
        Resume::Seek { seek } => {
            assert_eq!(seek, json!({ "pipeline_id": 7, "copy_sync_lsn": 9 }))
        }
        _ => panic!("a finished copy resumes from a seek"),
    }
    match resume_for(false, 7, None) {
        Resume::Seek { seek } => {
            assert_eq!(seek, json!({ "pipeline_id": 7, "copy_sync_lsn": null }))
        }
        _ => panic!("a finished copy resumes from a seek"),
    }
}

#[test]
fn the_monitor_records_only_growing_sync_lsns() {
    let barrier = CopyBarrier::new();
    assert_eq!(barrier.sync_done_lsn(), None);
    barrier.note_sync_done_lsn(300);
    barrier.note_sync_done_lsn(200);
    assert_eq!(barrier.sync_done_lsn(), Some(300));
}

/// A consumer that reports completed steps but never says which step it is
/// feeding.
///
/// The controller returns `Some` from both `completion_watcher` and
/// `current_step`. This one stands for a future consumer that offers the
/// first and forgets the second, the shape
/// [`PostgresCdcInputInner::acknowledge_after_step`] refuses to
/// acknowledge against (#7122).
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
struct StepBlindConsumer {
    mock: MockInputConsumer,
    /// Keeps the completion channel open for the receivers handed out.
    completions: Arc<Sender<Completion>>,
    /// The last error the endpoint reported: whether it was fatal, and its
    /// text. Recorded here rather than delegated, because the mock panics
    /// on an error no callback claims.
    error: Arc<Mutex<Option<(bool, String)>>>,
}

impl StepBlindConsumer {
    #[cfg_attr(not(test), allow(dead_code))]
    fn new() -> Self {
        let (completions, _rx) = channel(Completion::default());
        Self {
            mock: MockInputConsumer::new(),
            completions: Arc::new(completions),
            error: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn reported_error(&self) -> Option<(bool, String)> {
        self.error.lock().unwrap().clone()
    }
}

impl InputConsumer for StepBlindConsumer {
    fn max_batch_size(&self) -> usize {
        self.mock.max_batch_size()
    }

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        self.mock.pipeline_fault_tolerance()
    }

    fn parse_errors(&self, errors: Vec<ParseError>) {
        self.mock.parse_errors(errors)
    }

    fn buffered(&self, amt: BufferSize) {
        self.mock.buffered(amt)
    }

    fn replayed(&self, amt: BufferSize, hash: u64) {
        self.mock.replayed(amt, hash)
    }

    fn extended(&self, amt: BufferSize, resume: Option<Resume>, watermarks: Vec<Watermark>) {
        self.mock.extended(amt, resume, watermarks)
    }

    fn eoi(&self) {
        self.mock.eoi()
    }

    fn request_step(&self) {
        self.mock.request_step()
    }

    fn start_transaction(&self, label: Option<&str>) {
        self.mock.start_transaction(label)
    }

    fn commit_transaction(&self) {
        self.mock.commit_transaction()
    }

    fn update_connector_health(&self, health: ConnectorHealth) {
        self.mock.update_connector_health(health)
    }

    /// Step completion is tracked, so the connector may defer its answers.
    fn completion_watcher(&self) -> Option<Receiver<Completion>> {
        Some(self.completions.subscribe())
    }

    fn error(&self, fatal: bool, error: AnyError, _tag: Option<&'static str>) {
        *self.error.lock().unwrap() = Some((fatal, error.to_string()));
    }

    // `current_step` keeps the trait default, `None`: this consumer never
    // says which step it feeds, which is what the tests below exercise.
}

/// The configuration the reader needs to exist. Construction opens no
/// connection, so the URI only has to parse.
#[cfg(test)]
/// A pipeline configuration with nothing but a name, for the endpoint constructor.
fn test_pipeline_config() -> feldera_types::config::PipelineConfig {
    feldera_types::config::PipelineConfig {
        global: Default::default(),
        multihost: None,
        name: Some("test".to_string()),
        given_name: None,
        storage_config: None,
        secrets_dir: None,
        inputs: Default::default(),
        outputs: Default::default(),
        program_ir: None,
    }
}

fn cdc_config() -> PostgresCdcReaderConfig {
    PostgresCdcReaderConfig {
        uri: "postgres://user@localhost:5432/db".to_string(),
        publication: "pub".to_string(),
        source_table: "public.t".to_string(),
        run_source_migrations: true,
        tls: PostgresTlsConfig::default(),
        batch: PostgresCdcBatchConfig::default(),
        memory_backpressure: PostgresCdcMemoryBackpressureConfig::default(),
    }
}

#[test]
fn a_uri_that_does_not_parse_fails_the_endpoint_at_creation() {
    let mut config = cdc_config();
    config.uri = "postgres://localhost:5432/db".to_string();
    let error = PostgresCdcInputEndpoint::new(
        "cdc_in",
        &config,
        &test_pipeline_config(),
        &feldera_types::circuit::Layout::new_solo(1),
        Arc::default(),
        Box::new(MockInputConsumer::new()),
    )
    .err()
    .expect("a URI without a username does not reach the reader");
    let message = error.to_string();
    assert!(message.contains("missing username in URI"), "{message}");
}

#[test]
fn a_pipeline_that_hides_the_step_it_feeds_fails_the_endpoint() {
    let consumer = StepBlindConsumer::new();
    let inner = PostgresCdcInputInner::new(
        "cdc_in",
        cdc_config(),
        PublicationKey::per_table(&cdc_config()),
        Box::new(consumer.clone()),
        Arc::default(),
    );

    let flag = Arc::new(AtomicBool::new(false));
    inner.acknowledge_after_step(vec![terminal(&flag)]);

    let (fatal, message) = consumer
        .reported_error()
        .expect("the connector cannot tell which step holds the rows, so it must fail");
    assert!(fatal, "the connector cannot go on acknowledging blindly");
    assert!(
        message.contains("not the step it is feeding"),
        "the error names what the pipeline withheld: {message}"
    );
    assert!(
        !answered(&flag),
        "answering etl here would report rows as stored against a step that may not \
         hold them (#7122)"
    );
}

#[test]
fn a_pipeline_that_tracks_no_steps_is_answered_by_the_flush() {
    let inner = PostgresCdcInputInner::new(
        "cdc_in",
        cdc_config(),
        PublicationKey::per_table(&cdc_config()),
        Box::new(MockInputConsumer::new()),
        Arc::default(),
    );

    let flag = Arc::new(AtomicBool::new(false));
    inner.acknowledge_after_step(vec![terminal(&flag)]);

    assert!(
        answered(&flag),
        "nothing tracks steps, so no step is worth waiting for and the flush answers \
         etl itself"
    );
}

/// A consumer that reports completed steps and names the step it is feeding,
/// standing for a correct controller. The completion frontier is controlled
/// by the `Sender<Completion>` it holds, so a test can put it ahead of or
/// behind the step the consumer feeds.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone)]
struct StepAwareConsumer {
    mock: MockInputConsumer,
    /// Keeps the completion channel open and controls the frontier.
    completions: Arc<Sender<Completion>>,
    /// The step `current_step` reports, the step the flush feeds.
    step: Step,
}

impl StepAwareConsumer {
    #[cfg_attr(not(test), allow(dead_code))]
    fn new(step: Step, frontier: Step) -> Self {
        let (completions, _rx) = channel(Completion {
            total_completed_steps: frontier,
        });
        Self {
            mock: MockInputConsumer::new(),
            completions: Arc::new(completions),
            step,
        }
    }
}

impl InputConsumer for StepAwareConsumer {
    fn max_batch_size(&self) -> usize {
        self.mock.max_batch_size()
    }

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        self.mock.pipeline_fault_tolerance()
    }

    fn parse_errors(&self, errors: Vec<ParseError>) {
        self.mock.parse_errors(errors)
    }

    fn buffered(&self, amt: BufferSize) {
        self.mock.buffered(amt)
    }

    fn replayed(&self, amt: BufferSize, hash: u64) {
        self.mock.replayed(amt, hash)
    }

    fn extended(&self, amt: BufferSize, resume: Option<Resume>, watermarks: Vec<Watermark>) {
        self.mock.extended(amt, resume, watermarks)
    }

    fn eoi(&self) {
        self.mock.eoi()
    }

    fn request_step(&self) {
        self.mock.request_step()
    }

    fn start_transaction(&self, label: Option<&str>) {
        self.mock.start_transaction(label)
    }

    fn commit_transaction(&self) {
        self.mock.commit_transaction()
    }

    fn update_connector_health(&self, health: ConnectorHealth) {
        self.mock.update_connector_health(health)
    }

    fn completion_watcher(&self) -> Option<Receiver<Completion>> {
        Some(self.completions.subscribe())
    }

    fn current_step(&self) -> Option<Step> {
        Some(self.step)
    }

    fn error(&self, fatal: bool, error: AnyError, tag: Option<&'static str>) {
        self.mock.error(fatal, error, tag)
    }
}

#[test]
fn a_flush_that_races_the_watcher_stop_answers_the_durable_writes() {
    // The frontier is past the step the consumer feeds, so the write is
    // durable.
    let inner = PostgresCdcInputInner::new(
        "cdc_in",
        cdc_config(),
        PublicationKey::per_table(&cdc_config()),
        Box::new(StepAwareConsumer::new(3, 5)),
        Arc::default(),
    );

    // Close the completion channel as the watcher does when it stops, so the
    // next send is refused and the reader must answer on its own.
    inner
        .completion_task_rx
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .close();

    let flag = Arc::new(AtomicBool::new(false));
    inner.acknowledge_after_step(vec![terminal(&flag)]);

    assert!(
        answered(&flag),
        "the frontier has passed the step the flush fed, so the write is durable and the \
         stop must answer it rather than cost etl a re-read of the source table"
    );
}

#[test]
fn a_flush_that_races_the_watcher_stop_drops_the_writes_it_has_not_reached() {
    // The frontier has not reached the step the consumer feeds.
    let inner = PostgresCdcInputInner::new(
        "cdc_in",
        cdc_config(),
        PublicationKey::per_table(&cdc_config()),
        Box::new(StepAwareConsumer::new(7, 5)),
        Arc::default(),
    );

    inner
        .completion_task_rx
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .close();

    let flag = Arc::new(AtomicBool::new(false));
    inner.acknowledge_after_step(vec![terminal(&flag)]);

    assert!(
        !answered(&flag),
        "the frontier has not passed the step the flush fed, so the write is not durable and \
         its answer drops, which etl records as a re-read on the next start"
    );
}

#[test]
fn test_batch_config_from_maps_all_fields() {
    let config = PostgresCdcBatchConfig {
        max_fill_ms: 5_000,
        memory_budget_ratio: 0.3,
        max_size: ConfigSize::from_bytes(1024),
    };

    let etl_config = batch_config_from(&config);

    assert_eq!(etl_config.max_fill_ms, 5_000);
    assert_eq!(etl_config.memory_budget_ratio, 0.3);
    assert_eq!(etl_config.max_bytes, 1024);
}

#[test]
fn test_batch_config_from_default_matches_connector_default() {
    let etl_config = batch_config_from(&PostgresCdcBatchConfig::default());

    assert_eq!(etl_config.max_fill_ms, BatchConfig::default().max_fill_ms);
    assert_eq!(
        etl_config.memory_budget_ratio,
        BatchConfig::default().memory_budget_ratio
    );
    assert_eq!(etl_config.max_bytes, BatchConfig::default().max_bytes);
}

#[test]
fn test_memory_backpressure_config_from_maps_all_fields() {
    let config = PostgresCdcMemoryBackpressureConfig {
        activate_threshold: 0.6,
        resume_threshold: 0.4,
    };

    let etl_config = memory_backpressure_config_from(&config);

    assert_eq!(etl_config.activate_threshold, 0.6);
    assert_eq!(etl_config.resume_threshold, 0.4);
}

#[test]
fn test_memory_backpressure_config_from_default_matches_connector_default() {
    let etl_config =
        memory_backpressure_config_from(&PostgresCdcMemoryBackpressureConfig::default());

    assert_eq!(
        etl_config.activate_threshold,
        MemoryBackpressureConfig::default().activate_threshold
    );
    assert_eq!(
        etl_config.resume_threshold,
        MemoryBackpressureConfig::default().resume_threshold
    );
}

// -----------------------------------------------------------------------
// Shared pipeline: ack fan-in and shared settings
// -----------------------------------------------------------------------

#[test]
fn a_fan_in_answers_once_the_last_share_fires() {
    let flag = Arc::new(AtomicBool::new(false));
    let fan_in = AckFanIn::new(terminal(&flag), 2);
    let first = fan_in.share();
    let second = fan_in.share();
    first();
    assert!(!answered(&flag), "one member still owes its share");
    second();
    assert!(answered(&flag), "the last share answers etl");
}

#[test]
fn a_fan_in_without_shares_answers_at_once() {
    let flag = Arc::new(AtomicBool::new(false));
    let _fan_in = AckFanIn::new(terminal(&flag), 0);
    assert!(
        answered(&flag),
        "a write with rows for no member is durable at once"
    );
}

#[test]
fn shared_settings_name_the_first_field_that_differs() {
    let base = cdc_config();
    assert_eq!(shared_settings_differ(&base, &base.clone()), None);

    let mut other_table = base.clone();
    other_table.source_table = "public.other".to_string();
    assert_eq!(
        shared_settings_differ(&base, &other_table),
        None,
        "the table is what members differ in"
    );

    let mut batch = base.clone();
    batch.batch.max_fill_ms += 1;
    assert_eq!(shared_settings_differ(&base, &batch), Some("batch"));

    let mut migrations = base.clone();
    migrations.run_source_migrations = !base.run_source_migrations;
    assert_eq!(
        shared_settings_differ(&base, &migrations),
        Some("run_source_migrations")
    );

    let mut user = base.clone();
    user.uri = base.uri.replace("postgres://", "postgres://someone_else@");
    assert_eq!(shared_settings_differ(&base, &user), Some("uri user"));
}

#[test]
fn the_high_water_mark_drops_what_was_pushed_before() {
    let mut mark = HighWater::default();
    assert!(mark.admit((5, 0)), "a fresh member drops nothing");
    assert!(!mark.admit((5, 0)), "the same event again is a replay");
    assert!(!mark.admit((4, 9)), "an earlier transaction is a replay");
    assert!(
        mark.admit((5, 1)),
        "the second half of a transaction split over two writes is new"
    );
    assert!(mark.admit((6, 0)));
    assert!(!mark.admit((6, 0)));
    mark.reset();
    assert!(mark.admit((6, 0)), "after a new copy the stream is new");
}

#[test]
fn the_copy_workers_fit_the_free_slots() {
    // 10 slots, none in use: etl's default of 4 copy workers.
    assert_eq!(copy_worker_budget(10, 0), Some(4));
    // 4 slots, none in use: the apply slot and 3 copy slots.
    assert_eq!(copy_worker_budget(4, 0), Some(3));
    // 4 slots, 2 in use: the apply slot and 1 copy slot.
    assert_eq!(copy_worker_budget(4, 2), Some(1));
    // 4 slots, 3 in use: the apply slot alone; the copy still gets a worker,
    // and a copy slot that cannot be created is a table error.
    assert_eq!(copy_worker_budget(4, 3), Some(1));
    // Not even the apply slot fits.
    assert_eq!(copy_worker_budget(4, 4), None);
    assert_eq!(copy_worker_budget(4, 5), None);
}
