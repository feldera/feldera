use crate::{
    Controller,
    controller::MAX_CONNECTOR_ERROR_LEN,
    integrated::postgres::test::pg::{TempPgTable, TempTestStruct, pg_connect, test_circuit},
    test::{
        engine_test,
        pipeline::{PipelineTestEngine, TestCircuit},
        published_schema, read, relations,
    },
};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, SubsecRound};
use feldera_adapterlib::test::{ZSet, wait};
use feldera_types::{
    config::PipelineConfig,
    program_schema::{ColumnType, Field, Relation},
    transport::postgres::{PostgresTlsConfig, PostgresWriteMode},
};
use postgres::Row;
use rand::Rng;
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    io::Write,
    ops::RangeInclusive,
    path::Path,
    sync::{Arc, Mutex},
};
use tempfile::NamedTempFile;

/// The table that most cases write: a column of each type that the Postgres output connector
/// writes, with the index [`key_relation`] on `bigint_`.
pub fn relation() -> Relation {
    let basic_struct = || ColumnType::structure(false, &relations::basic_columns());
    Relation::new(
        "postgres".into(),
        vec![
            Field::new("boolean_".into(), ColumnType::boolean(false)),
            Field::new("tinyint_".into(), ColumnType::tinyint(false)),
            Field::new("smallint_".into(), ColumnType::smallint(false)),
            Field::new("int_".into(), ColumnType::int(false)),
            Field::new("bigint_".into(), ColumnType::bigint(false)),
            Field::new("decimal_".into(), ColumnType::decimal(10, 3, false)),
            Field::new("float_".into(), ColumnType::real(false)),
            Field::new("double_".into(), ColumnType::double(false)),
            Field::new("varchar_".into(), ColumnType::varchar(false)),
            Field::new("time_".into(), ColumnType::time(false)),
            Field::new("date_".into(), ColumnType::date(false)),
            Field::new("timestamp_".into(), ColumnType::timestamp(false)),
            Field::new("variant_".into(), ColumnType::variant(false)),
            Field::new("uuid_".into(), ColumnType::uuid(false)),
            Field::new("varbinary_".into(), ColumnType::varbinary(false)),
            Field::new("struct_".into(), basic_struct()),
            Field::new(
                "string_array_".into(),
                ColumnType::array(false, ColumnType::varchar(false)),
            ),
            Field::new(
                "struct_array_".into(),
                ColumnType::array(false, basic_struct()),
            ),
            Field::new(
                "map_".into(),
                ColumnType::map(false, ColumnType::varchar(false), basic_struct()),
            ),
        ],
        false,
        BTreeMap::new(),
    )
}

/// The index of [`relation`] on `bigint_`.
pub fn key_relation() -> Relation {
    Relation::new(
        "postgres_key".into(),
        vec![Field::new("bigint_".into(), ColumnType::bigint(false))],
        false,
        BTreeMap::new(),
    )
}

/// A table with the columns of [`relations::basic`], which the input connector reads.
pub fn basic_relation() -> Relation {
    relations::basic("postgres_basic")
}

/// The index of [`basic_relation`] on `id`.
pub fn basic_key_relation() -> Relation {
    relations::basic_key("postgres_basic_key")
}

/// Returns the row of [`relation`] with `bigint_`, the string `variant` as its VARIANT, and the
/// default value of every other column, in the default JSON flavor.
fn default_record(bigint_: i64) -> Value {
    json!({
        "boolean_": false,
        "tinyint_": 0,
        "smallint_": 0,
        "int_": 0,
        "bigint_": bigint_,
        "decimal_": "0.000",
        "float_": 0.0,
        "double_": 0.0,
        "varchar_": "",
        "time_": "00:00:00",
        "date_": "1970-01-01",
        "timestamp_": "1970-01-01 00:00:00",
        "variant_": "variant",
        "uuid_": "00000000-0000-0000-0000-000000000000",
        "varbinary_": [],
        "struct_": { "id": 0, "b": false, "i": null, "s": "" },
        "string_array_": [],
        "struct_array_": [],
        "map_": {},
    })
}

/// Returns the circuit whose table `test_input1` has the columns of [`relation`] and no primary
/// key, whose view `test_output1` holds its rows, and whose index `idx` on the view is keyed by
/// `bigint_`.
fn indexed_view() -> TestCircuit {
    TestCircuit::IndexedView {
        key: key_relation(),
        relation: relation(),
        index: "idx".to_string(),
    }
}

/// Returns a random row of [`relation`] in the default JSON flavor: the time columns hold the
/// current time, and the other columns values that Postgres stores exactly.
fn random_record(rng: &mut impl Rng) -> Value {
    let now = chrono::Utc::now().naive_utc();
    let mut map = Map::new();
    map.insert(rng.r#gen::<u32>().to_string(), random_basic_record(rng));
    json!({
        "boolean_": rng.r#gen::<bool>(),
        "tinyint_": rng.r#gen::<i8>(),
        "smallint_": rng.r#gen::<i16>(),
        "int_": rng.r#gen::<i32>(),
        "bigint_": rng.r#gen::<i64>(),
        "decimal_": decimal_text(rng.gen_range(-100..100)),
        "float_": (rng.r#gen::<f32>() * 1000.0).trunc() / 1000.0,
        "double_": (rng.r#gen::<f64>() * 1000.0).trunc() / 1000.0,
        "varchar_": rng.r#gen::<u32>().to_string(),
        "time_": now.time().round_subsecs(5).format("%H:%M:%S%.f").to_string(),
        "date_": now.date().format("%Y-%m-%d").to_string(),
        "timestamp_": now.trunc_subsecs(6).format("%Y-%m-%d %H:%M:%S%.f").to_string(),
        "variant_": rng.r#gen::<u32>().to_string(),
        "uuid_": uuid::Uuid::new_v4().to_string(),
        "varbinary_": [rng.gen_range(0..127u8)],
        "struct_": random_basic_record(rng),
        "string_array_": [],
        "struct_array_": [random_basic_record(rng)],
        "map_": map,
    })
}

/// Returns a random row of [`relations::basic`] in the default JSON flavor.
fn random_basic_record(rng: &mut impl Rng) -> Value {
    json!({
        "id": rng.gen_range(0..i32::MAX as u32),
        "b": rng.r#gen::<bool>(),
        "i": rng.r#gen::<Option<i64>>(),
        "s": rng.r#gen::<u32>().to_string(),
    })
}

/// Returns `thousandths` as decimal text with three digits after the point.
fn decimal_text(thousandths: i64) -> String {
    let sign = if thousandths < 0 { "-" } else { "" };
    let magnitude = thousandths.unsigned_abs();
    format!("{sign}{}.{:03}", magnitude / 1000, magnitude % 1000)
}

/// Returns `count` random rows of [`relation`].
fn random_records(count: usize) -> Vec<Value> {
    let mut rng = rand::thread_rng();
    (0..count).map(|_| random_record(&mut rng)).collect()
}

/// Returns `rows`, rows of `relation` in the default JSON flavor, as a Z-set.
fn rows_zset(relation: &Relation, rows: &[Value]) -> ZSet {
    let rows: Vec<String> = rows
        .iter()
        .map(|row| read::arrow_form(relation, row.clone()).unwrap().to_string())
        .collect();
    let rows: Vec<(i64, &str)> = rows.iter().map(|row| (1, row.as_str())).collect();
    feldera_adapterlib::test::Updates::from_json(published_schema(relation), &rows)
        .unwrap()
        .zset()
        .unwrap()
}

/// Returns the `bigint_` of `row`, a row of [`relation`].
fn bigint(row: &Value) -> i64 {
    row["bigint_"].as_i64().unwrap()
}

impl TempTestStruct {
    /// Returns the value as a row of [`relations::basic`] in the default JSON flavor.
    fn record(self) -> Value {
        json!({ "id": self.id.unwrap(), "b": self.b.unwrap(), "i": self.i, "s": self.s.unwrap() })
    }
}

/// Returns `row`, a row of a table that [`TempPgTable`] created, as a row of [`relation`] in
/// the default JSON flavor.
fn row_record(row: &Row) -> Value {
    json!({
        "boolean_": row.get::<_, bool>("boolean_"),
        "tinyint_": row.get::<_, i16>("tinyint_"),
        "smallint_": row.get::<_, i16>("smallint_"),
        "int_": row.get::<_, i32>("int_"),
        "bigint_": row.get::<_, i64>("bigint_"),
        "decimal_": row.get::<_, String>("decimal_str"),
        "float_": row.get::<_, f32>("float_"),
        "double_": row.get::<_, f64>("double_"),
        "varchar_": row.get::<_, String>("varchar_"),
        "time_": row.get::<_, NaiveTime>("time_").format("%H:%M:%S%.f").to_string(),
        "date_": row.get::<_, NaiveDate>("date_").format("%Y-%m-%d").to_string(),
        "timestamp_": row
            .get::<_, NaiveDateTime>("timestamp_")
            .format("%Y-%m-%d %H:%M:%S%.f")
            .to_string(),
        "variant_": row.get::<_, Value>("variant_"),
        "uuid_": row.get::<_, uuid::Uuid>("uuid_").to_string(),
        "varbinary_": row.get::<_, Vec<u8>>("varbinary_"),
        "struct_": row.get::<_, TempTestStruct>("struct_").record(),
        "string_array_": row.get::<_, Vec<String>>("string_array_"),
        "struct_array_": row
            .get::<_, Vec<TempTestStruct>>("struct_array_")
            .into_iter()
            .map(TempTestStruct::record)
            .collect::<Vec<_>>(),
        "map_": row.get::<_, Value>("map_"),
    })
}

/// Returns `rows`, read from a table that [`TempPgTable`] created, as rows of [`relation`] in
/// the default JSON flavor.
fn records_of(rows: Vec<Row>) -> Vec<Value> {
    rows.iter().map(row_record).collect()
}

/// Returns `rows`, read from a table that a connector in cdc mode wrote, as rows of
/// [`relation`] in the default JSON flavor, each with its `__feldera_op`.
fn cdc_records_of(rows: Vec<Row>) -> Vec<(Value, String)> {
    rows.iter()
        .map(|row| (row_record(row), row.get::<_, String>("__feldera_op")))
        .collect()
}

/// Returns the rows of `records`, without their operations.
fn rows_only(records: &[(Value, String)]) -> Vec<Value> {
    records.iter().map(|(row, _)| row.clone()).collect()
}

/// Returns the wall-clock time in microseconds since the epoch, the unit of `__feldera_ts`.
fn now_micros() -> i64 {
    chrono::Utc::now().timestamp_micros()
}

/// Checks that each of `rows`, read from a table that a connector in cdc mode wrote, carries a
/// `__feldera_ts` within the time window of the run that wrote it. `run_of` returns that
/// window, in microseconds since the epoch, for the row and its `__feldera_op`.
fn check_cdc_times(rows: Vec<Row>, run_of: impl Fn(&Value, &str) -> RangeInclusive<i64>) {
    for row in &rows {
        let record = row_record(row);
        let op: String = row.get("__feldera_op");
        let ts: i64 = row.get("__feldera_ts");
        let run = run_of(&record, &op);
        assert!(
            run.contains(&ts),
            "the '{op}' row {record} has __feldera_ts {ts}, outside its run {run:?}"
        );
    }
}

/// Whether `got` and `expected`, rows of [`relation`] in the default JSON flavor, hold the
/// same rows, as SQL compares them.
fn same_rows(got: &[Value], expected: &[Value]) -> bool {
    rows_zset(&relation(), got) == rows_zset(&relation(), expected)
}

pub(super) fn postgres_url() -> String {
    std::env::var("POSTGRES_URL")
        .unwrap_or("postgres://postgres:password@localhost:5432".to_string())
}

pub(super) fn unique_pg_name(prefix: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let max_prefix_len = 63 - suffix.len() - 1;
    let prefix = &prefix[..prefix.len().min(max_prefix_len)];

    format!("{prefix}_{suffix}")
}

/// Returns the SSL connection URL and TLS config if the SSL environment
/// variables are set (`POSTGRES_SSL_URL`, `POSTGRES_SSL_CA_LOCATION`,
/// `POSTGRES_SSL_CLIENT_LOCATION`, `POSTGRES_SSL_CLIENT_KEY_LOCATION`).
pub(super) fn postgres_ssl_config() -> (String, PostgresTlsConfig) {
    let url = std::env::var("POSTGRES_SSL_URL")
        .unwrap_or("postgres://postgres:secret@localhost:5432/postgres".to_string());
    let ssl_ca_pem = std::env::var("POSTGRES_SSL_CA_PEM").ok();
    let ssl_ca_location = std::env::var("POSTGRES_SSL_CA_LOCATION").ok();
    let ssl_client_pem = std::env::var("POSTGRES_SSL_CLIENT_CERT_PEM").ok();
    let ssl_client_location = std::env::var("POSTGRES_SSL_CLIENT_LOCATION").ok();
    let ssl_client_key = std::env::var("POSTGRES_SSL_CLIENT_KEY_PEM").ok();
    let ssl_client_key_location = std::env::var("POSTGRES_SSL_CLIENT_KEY_LOCATION").ok();
    let ssl_certificate_chain_location =
        std::env::var("POSTGRES_SSL_CERTIFICATE_CHAIN_LOCATION").ok();
    let verify_hostname = std::env::var("POSTGRES_SSL_VERIFY_HOSTNAME")
        .ok()
        .and_then(|s| s.parse::<bool>().ok());

    (
        url,
        PostgresTlsConfig {
            ssl_ca_location,
            ssl_ca_pem,
            ssl_client_location,
            ssl_client_key_location,
            ssl_client_pem,
            ssl_client_key,
            ssl_certificate_chain_location,
            verify_hostname,
        },
    )
}

/// Returns the SSL connection URL and TLS config supported by the CDC connector.
pub(super) fn postgres_cdc_ssl_config() -> (String, PostgresTlsConfig) {
    let (url, tls) = postgres_ssl_config();
    (
        url,
        PostgresTlsConfig {
            ssl_ca_location: tls.ssl_ca_location,
            ssl_ca_pem: tls.ssl_ca_pem,
            ..Default::default()
        },
    )
}

fn test_pg_on_conflict(
    engine: &dyn PipelineTestEngine,
    on_conflict_do_nothing: bool,
    mode: PostgresWriteMode,
) {
    let table_name = unique_pg_name("test_pg_on_conflict");
    let url = postgres_url();
    let max_buffer_size_bytes = 1024;
    let timeout_ms = 120_000;

    let data: Vec<Value> = random_records(10000);
    let mut insert_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        insert_file.as_file_mut().write_all(&row_bytes).unwrap();
        insert_file.write_all(b"\n").unwrap();
    }

    let upsert_data = data
        .clone()
        .into_iter()
        .map(|mut d| {
            d["varchar_"] = Value::from("updated");
            d
        })
        .collect::<Vec<_>>();

    let mut upsert_file = NamedTempFile::new().unwrap();

    for new in upsert_data.iter() {
        let row_bytes = serde_json::to_vec(&new).unwrap();
        upsert_file.as_file_mut().write_all(&row_bytes).unwrap();
    }

    let url_clone = url.clone();

    let get_config = |in_file: &Path| {
        serde_json::from_value(json!({
            "name": "test",
            "workers": 4,
            "inputs": {
                "ins": {
                    "stream": "test_input1",
                    "transport": {
                        "name": "file_input",
                        "config": {
                            "path": in_file,
                        },
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "raw",
                            "array": false
                        }
                    }
                }
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "postgres_output",
                        "config": {
                            "uri": url_clone,
                            "table": &table_name,
                            "max_buffer_size_bytes": max_buffer_size_bytes,
                            "on_conflict_do_nothing": on_conflict_do_nothing,
                            "mode": match mode {
                                PostgresWriteMode::Materialized => "materialized",
                                PostgresWriteMode::Cdc => "cdc",
                            }
                        }
                    },
                    "index": "idx"
                }
            }
        }))
        .unwrap()
    };

    let mut table = TempPgTable::new(
        &table_name,
        url,
        matches!(mode, PostgresWriteMode::Materialized),
        &None,
    );

    let (controller, err_receiver) = test_circuit(engine, get_config(insert_file.path()));

    controller.start();

    wait(
        || match mode {
            PostgresWriteMode::Materialized => {
                let rows = table.query();

                let got = records_of(rows);

                same_rows(&got, &data) || !err_receiver.is_empty()
            }
            PostgresWriteMode::Cdc => {
                let rows = table.query();

                let got = cdc_records_of(rows);

                same_rows(&rows_only(&got), &data) && got.iter().all(|(_, op)| op == "i")
                    || !err_receiver.is_empty()
            }
        },
        timeout_ms,
    )
    .expect("timeout: failed to insert data into postgres");

    controller.stop().unwrap();

    let (controller, err_receiver) = test_circuit(engine, get_config(upsert_file.path()));

    controller.start();

    let expected = if on_conflict_do_nothing && matches!(mode, PostgresWriteMode::Materialized) {
        data
    } else {
        upsert_data
    };

    match mode {
        PostgresWriteMode::Materialized => {
            wait(
                || {
                    let rows = table.query();

                    let got = records_of(rows);

                    same_rows(&got, &expected) || !err_receiver.is_empty()
                },
                timeout_ms,
            )
            .expect("timeout: failed to update data into postgres");
        }
        PostgresWriteMode::Cdc => {
            wait(
                || {
                    let rows = table.query();

                    let got = cdc_records_of(rows);

                    let got: Vec<_> = got
                        .iter()
                        .filter(|(r, _)| r["varchar_"] == "updated")
                        .cloned()
                        .collect();

                    same_rows(&rows_only(&got), &expected) && got.iter().all(|(_, op)| op == "i")
                        || !err_receiver.is_empty()
                },
                timeout_ms,
            )
            .expect("timeout: failed to update data into postgres");
        }
    }

    controller.stop().unwrap();
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn on_conflict_do_update(engine: &dyn PipelineTestEngine) {
    test_pg_on_conflict(engine, false, PostgresWriteMode::Materialized);
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn on_conflict_do_nothing(engine: &dyn PipelineTestEngine) {
    test_pg_on_conflict(engine, true, PostgresWriteMode::Materialized);
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn insert0(engine: &dyn PipelineTestEngine) {
    pg_insert0(engine, PostgresWriteMode::Materialized);
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn insert0_cdc(engine: &dyn PipelineTestEngine) {
    pg_insert0(engine, PostgresWriteMode::Cdc);
}

fn pg_insert0(engine: &dyn PipelineTestEngine, mode: PostgresWriteMode) {
    let table_name = unique_pg_name("test_pg_insert");
    let url = postgres_url();
    let max_buffer_size_bytes = 1024;
    let timeout_ms = 120_000;

    // On average 1 record is about 590 bytes, so 2000 records produce a buffer
    // of over 1 MiB
    let data: Vec<Value> = random_records(10000);

    let mut temp_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        let inner = &row_bytes;
        temp_file.as_file_mut().write_all(inner).unwrap();
        temp_file.write_all(b"\n").unwrap();
    }

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": temp_file.path(),
              "byte_size_buffer": 2097152
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "raw",
              "array": false
            }
          }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "postgres_output",
            "config": {
              "uri": url,
              "table": &table_name,
              "max_buffer_size_bytes": max_buffer_size_bytes,
              "mode": match mode {
                  PostgresWriteMode::Materialized => "materialized",
                  PostgresWriteMode::Cdc => "cdc",
              }
            }
          },
          "index": "idx"
        }
      }
    }))
    .unwrap();

    let mut table = TempPgTable::new(
        &table_name,
        url,
        matches!(mode, PostgresWriteMode::Materialized),
        &None,
    );

    let start = now_micros();
    let (controller, err_receiver) = test_circuit(engine, config);

    controller.start();

    wait(
        || match mode {
            PostgresWriteMode::Materialized => {
                let rows = table.query();

                let got = records_of(rows);

                same_rows(&got, &data) || !err_receiver.is_empty()
            }
            PostgresWriteMode::Cdc => {
                let rows = table.query();

                let got = cdc_records_of(rows);

                same_rows(&rows_only(&got), &data) && got.iter().all(|(_, op)| op == "i")
                    || !err_receiver.is_empty()
            }
        },
        timeout_ms,
    )
    .expect("timeout: failed to insert data into postgres");

    if matches!(mode, PostgresWriteMode::Cdc) {
        let rows = table.query();
        let run = start..=now_micros();
        check_cdc_times(rows, |_, _| run.clone());
    }
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn reconnect_does_not_lose_the_batch(engine: &dyn PipelineTestEngine) {
    pg_reconnect_test(engine, "test_pg_reconnect", 1, 1);
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn reconnect_does_not_lose_the_batch_multi_thread(engine: &dyn PipelineTestEngine) {
    pg_reconnect_test(engine, "test_pg_reconnect_mt", 3, 1);
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn reconnect_survives_repeated_drops(engine: &dyn PipelineTestEngine) {
    pg_reconnect_test(engine, "test_pg_reconnect_repeat", 1, 3);
}

/// Makes the connector's database refuse new connections, and lets them through
/// again when dropped.
///
/// `ALTER DATABASE ... ALLOW_CONNECTIONS false` turns away even a superuser, so
/// it stands in for a server that is restarting or under maintenance.
///
/// It turns away every connection to the database, not only the connector's,
/// and the tests here share one database. So a test that closes the database
/// fails any test that happens to be connecting, which is why every test that
/// reaches postgres is `#[serial]`.
struct RefusedConnections {
    admin: postgres::Client,
    database: String,
    refusing: bool,
}

impl RefusedConnections {
    /// Opens the connection this needs up front, so that starting to refuse is a
    /// single statement once the moment arrives.
    fn prepare(url: &str) -> Self {
        let database: String = pg_connect(url, &None)
            .query_one("SELECT current_database()", &[])
            .expect("failed to read the database name")
            .get(0);

        // ALTER DATABASE cannot run from a session connected to that database.
        let admin = pg_connect(&Self::other_database_url(url), &None);

        Self {
            admin,
            database,
            refusing: false,
        }
    }

    fn refuse(&mut self) {
        let refuse = Self::allow_connections(&self.database, false);
        self.admin
            .execute(&refuse, &[])
            .expect("failed to make the database refuse connections");
        self.refusing = true;
    }

    fn other_database_url(url: &str) -> String {
        match url::Url::parse(url) {
            Ok(mut parsed) => {
                parsed.set_path("/template1");
                parsed.to_string()
            }
            Err(_) => format!("{url}/template1"),
        }
    }

    fn allow_connections(database: &str, allow: bool) -> String {
        format!(r#"ALTER DATABASE "{database}" WITH ALLOW_CONNECTIONS {allow}"#)
    }
}

/// Restoring on drop matters: leaving the database closed would fail every later test.
impl Drop for RefusedConnections {
    fn drop(&mut self) {
        if !self.refusing {
            return;
        }
        let restore = Self::allow_connections(&self.database, true);
        self.admin
            .execute(&restore, &[])
            .expect("failed to let connections through again");
    }
}

/// A database that refuses the reconnect must be waited out, not given up on.
///
/// This is the one path in the connector that dropping a backend does not reach:
/// there the reconnect succeeds on the first try, so `retry_connecting_with_backoff` never
/// enters its error branch. Refusing connections for a few seconds forces that
/// branch, and the batch still has to arrive once they are let through.
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn reconnect_waits_out_a_refusing_database(engine: &dyn PipelineTestEngine) {
    const RECORDS: usize = 3000;

    let table_name = unique_pg_name("test_pg_refused");
    let url = postgres_url();
    let verify_url = url.clone();

    let mut data: Vec<Value> = random_records(RECORDS);
    for (i, datum) in data.iter_mut().enumerate() {
        datum["bigint_"] = Value::from(i as i64);
    }
    let temp_file = write_records_to_temp_file(&data);
    let config = pg_output_test_config(temp_file.path(), &url, &table_name, 1, Some(RECORDS));

    let _table = TempPgTable::new(&table_name, url, true, &None);
    let (controller, _err_receiver) = test_circuit(engine, config);

    // Every connection the test needs has to exist before the database starts
    // turning connections away.
    let mut watcher = pg_connect(&verify_url, &None);
    let mut refused = RefusedConnections::prepare(&verify_url);

    // Close the database before the batch starts rather than once it is under
    // way. The connector connected and prepared while the controller was built,
    // and closing the database leaves an established connection alone, so this
    // costs it nothing until it has to reconnect. What it buys is that the drop
    // below no longer has to land in the stretch between the batch opening and
    // `refuse()` returning, which is a matter of milliseconds.
    refused.refuse();
    controller.start();
    assert!(
        poll_and_terminate_batch_backends(&mut watcher, &table_name),
        "never dropped the connector's connection while the database was refusing them, \
         so this test proves nothing"
    );

    // Hold the refusal until the loop has failed twice, so that it outlasts the
    // first backoff steps of one and two seconds instead of assuming they fit
    // in a sleep.
    wait(|| pg_conn_retry_failures(&controller) >= 2, 60_000)
        .expect("the reconnect loop did not fail twice, so it was never exercised");
    drop(refused);

    wait(
        || pg_total_processed_input_records(&controller) >= RECORDS as u64,
        180_000,
    )
    .expect("timeout: connector did not finish handling the input");

    let (records, _bytes) = pg_transmitted_totals(&controller);
    let fatal_error = {
        let status = controller.status();
        let outputs = status.output_status();
        outputs
            .values()
            .next()
            .expect("output endpoint")
            .fatal_error()
    };
    let _ = controller.stop();

    assert_eq!(
        pg_row_count(&verify_url, &table_name),
        RECORDS as u64,
        "rows were lost while the database refused connections"
    );
    assert_eq!(
        records, RECORDS as u64,
        "reported {records} of {RECORDS} rows as transmitted"
    );
    assert_eq!(
        fatal_error, None,
        "the connector recovered, yet the endpoint is marked as fatally failed"
    );
}

/// The pipeline must be able to stop while postgres is unreachable.
///
/// A worker waiting out a database it cannot reach sits in a backoff loop, and
/// the endpoint's drop joins that thread, so anything that keeps the loop from
/// noticing the shutdown hangs the pipeline rather than merely stalling its
/// output.
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn stops_while_the_database_is_unreachable(engine: &dyn PipelineTestEngine) {
    const RECORDS: usize = 3000;
    const STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

    let table_name = unique_pg_name("test_pg_stop_unreachable");
    let url = postgres_url();
    let verify_url = url.clone();

    let mut data: Vec<Value> = random_records(RECORDS);
    for (i, datum) in data.iter_mut().enumerate() {
        datum["bigint_"] = Value::from(i as i64);
    }
    let temp_file = write_records_to_temp_file(&data);
    let config = pg_output_test_config(temp_file.path(), &url, &table_name, 1, Some(RECORDS));

    let _table = TempPgTable::new(&table_name, url, true, &None);
    let (controller, _err_receiver) = test_circuit(engine, config);

    // Both connections have to exist before the database starts turning them
    // away. `refused` lets connections through again when it drops, including
    // while this test unwinds, which releases the worker either way.
    let mut watcher = pg_connect(&verify_url, &None);
    let mut refused = RefusedConnections::prepare(&verify_url);

    // Close the database before the batch starts, as
    // `test_pg_reconnect_waits_out_a_refusing_database` explains.
    refused.refuse();
    controller.start();
    assert!(
        poll_and_terminate_batch_backends(&mut watcher, &table_name),
        "never dropped the connector's connection"
    );

    // The worker is in the backoff loop once a reconnect of its own has failed,
    // which is the state to ask for a stop from.
    wait(|| pg_conn_retry_failures(&controller) >= 1, 60_000)
        .expect("the worker never entered the reconnect backoff loop");

    let (stopped_sender, stopped) = crossbeam::channel::bounded(1);
    std::thread::spawn(move || {
        let _ = controller.stop();
        let _ = stopped_sender.send(());
    });

    // Wait with a deadline rather than blocking: a regression here would
    // otherwise hang the whole test run instead of failing this one test.
    assert!(
        stopped.recv_timeout(STOP_TIMEOUT).is_ok(),
        "the pipeline did not stop within {STOP_TIMEOUT:?} while postgres was unreachable"
    );
}

/// The `transmitted_records` counter still reports every row of a batch that commits.
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn transmitted_records_counts_committed_rows(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_commit_count");
    let url = postgres_url();
    let verify_url = url.clone();

    let data: Vec<Value> = random_records(1000);
    let temp_file = write_records_to_temp_file(&data);
    let config = pg_output_test_config(temp_file.path(), &url, &table_name, 1, None);

    let _table = TempPgTable::new(&table_name, url, true, &None);

    let (controller, err_receiver) = test_circuit(engine, config);
    controller.start();

    wait(
        || pg_transmitted_totals(&controller).0 >= data.len() as u64 || !err_receiver.is_empty(),
        60_000,
    )
    .expect("timeout: connector did not report the committed rows");

    let (records, bytes) = pg_transmitted_totals(&controller);
    let _ = controller.stop();

    assert!(err_receiver.is_empty(), "connector reported an error");
    assert_eq!(
        records,
        data.len() as u64,
        "reported {records} of {} rows as transmitted",
        data.len()
    );
    assert!(bytes > 0, "reported 0 bytes for {records} rows");
    assert_eq!(
        pg_row_count(&verify_url, &table_name),
        records,
        "the counter and the table disagree"
    );
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn insert(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_insert");
    let url = postgres_url();
    let max_records_in_buffer = 1000;

    // On average 1 record is about 590 bytes, so 2000 records produce a buffer
    // of over 1 MiB
    let data: Vec<Value> = random_records(10000);

    let mut temp_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        let inner = &row_bytes;
        temp_file.as_file_mut().write_all(inner).unwrap();
        temp_file.write_all(b"\n").unwrap();
    }

    let config = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "ins": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_file.path(),
                        "byte_size_buffer": 2097152
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                        "array": false
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "postgres_output",
                    "config": {
                        "uri": url,
                      "table": &table_name,
                        "max_records_in_buffer": max_records_in_buffer
                    }
                },
                "index": "idx"
            }
        }
    }))
    .unwrap();

    let mut table = TempPgTable::new(&table_name, url, true, &None);

    let (controller, err_receiver) = test_circuit(engine, config);

    controller.start();

    wait(
        || {
            let rows = table.query();

            let got = records_of(rows);

            same_rows(&got, &data) || !err_receiver.is_empty()
        },
        40_000,
    )
    .expect("timeout: failed to insert data into postgres");
}

// A NOT NULL DEFAULT column (served_at) omitted from the Feldera schema must be
// populated from its default, not NULL (#6694).
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn insert_omitted_default_column(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_default");
    let url = postgres_url();
    let verify_url = url.clone();
    let timeout_ms = 60_000;

    let data: Vec<Value> = random_records(100);

    let mut temp_file = NamedTempFile::new().unwrap();
    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        temp_file.as_file_mut().write_all(&row_bytes).unwrap();
        temp_file.write_all(b"\n").unwrap();
    }

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": { "name": "file_input", "config": { "path": temp_file.path() } },
          "format": { "name": "json", "config": { "update_format": "raw", "array": false } }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": { "name": "postgres_output", "config": { "uri": url, "table": &table_name } },
          "index": "idx"
        }
      }
    }))
    .unwrap();

    // The Feldera view (`relation`) has no served_at column.
    let mut table = TempPgTable::new_with_extra_columns(
        &table_name,
        url,
        true,
        &None,
        "served_at TIMESTAMP DEFAULT now() NOT NULL",
    );

    let (controller, err_receiver) = test_circuit(engine, config);
    controller.start();

    wait(
        || {
            let got = records_of(table.query());
            same_rows(&got, &data) || !err_receiver.is_empty()
        },
        timeout_ms,
    )
    .expect("timeout: failed to insert data into postgres");

    assert!(err_receiver.is_empty(), "connector reported an error");

    // Every row's served_at was filled by the DEFAULT rather than left NULL.
    let mut client = pg_connect(&verify_url, &None);
    let rows = client
        .query(&format!("SELECT served_at FROM {table_name}"), &[])
        .expect("failed to query table");
    assert_eq!(rows.len(), data.len());
    assert!(
        rows.iter().all(|r| r
            .get::<_, Option<chrono::NaiveDateTime>>("served_at")
            .is_some()),
        "served_at should be populated by its DEFAULT"
    );
}

/// Bytes reported by the `while executing ... statement for N record(s) (M
/// bytes)` context that the connector attaches to a failed statement.
fn payload_bytes_in_error(message: &str) -> Option<usize> {
    message
        .split(" record(s) (")
        .nth(1)?
        .split(" bytes)")
        .next()?
        .parse()
        .ok()
}

/// A statement Postgres rejects must not carry the whole batch into the error
/// message.
///
/// The payload of one statement is as large as `max_buffer_size` (1 MiB by
/// default) and an endpoint retains up to `MAX_CONNECTOR_ERRORS` messages per
/// tag, so quoting the payload in full grew `/stats` and
/// `/output_endpoints/{name}/stats` past the size limit the pipeline manager
/// applies while proxying them, failing the whole request.
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn error_message_is_bounded(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_errmsg");
    let url = postgres_url();

    // One record is about 590 bytes, so 10000 records fill the connector's
    // default 1 MiB buffer several times over.
    let data: Vec<Value> = random_records(10000);

    let mut temp_file = NamedTempFile::new().unwrap();
    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        temp_file.as_file_mut().write_all(&row_bytes).unwrap();
        temp_file.write_all(b"\n").unwrap();
    }

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      // Step on large batches so that the connector fills its 1 MiB buffer,
      // instead of flushing a handful of records at a time.
      "min_batch_size_records": 5000,
      "max_buffering_delay_usecs": 5000000,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": { "name": "file_input", "config": { "path": temp_file.path(), "byte_size_buffer": 8388608 } },
          "format": { "name": "json", "config": { "update_format": "raw", "array": false } }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": { "name": "postgres_output", "config": { "uri": url, "table": &table_name } },
          "index": "idx"
        }
      }
    }))
    .unwrap();

    // `freshness_timestamp` is NOT NULL, has no DEFAULT, and is absent from the
    // Feldera view, so every INSERT the connector issues violates the
    // constraint.
    let _table = TempPgTable::new_with_extra_columns(
        &table_name,
        url,
        true,
        &None,
        "freshness_timestamp TIMESTAMP NOT NULL",
    );

    let (controller, _err_receiver) = test_circuit(engine, config);
    controller.start();

    wait(
        || {
            controller
                .status()
                .output_status()
                .values()
                .any(|endpoint| {
                    endpoint
                        .metrics
                        .num_transport_errors
                        .load(std::sync::atomic::Ordering::Relaxed)
                        > 0
                })
        },
        60_000,
    )
    .expect("timeout: connector reported no transport error");

    // Snapshot the errors and shut the pipeline down before asserting, so that a
    // failing assertion does not race the table teardown.
    let (errors, fatal_error) = {
        let status = controller.status();
        let outputs = status.output_status();
        let endpoint = outputs.values().next().expect("output endpoint");
        (
            endpoint.transport_errors.lock().unwrap().to_api_type(),
            endpoint.fatal_error(),
        )
    };
    let _ = controller.stop();

    assert!(!errors.is_empty(), "no transport error was recorded");

    for error in &errors {
        assert!(
            !error.message.contains("bytes elided"),
            "connector produced a {}-byte message that the status layer had to \
             truncate: {}",
            error.message.len(),
            error.message
        );
        assert!(
            error.message.len() <= MAX_CONNECTOR_ERROR_LEN,
            "error message is {} bytes",
            error.message.len()
        );
    }

    // Truncating must keep the diagnosis, which Postgres reports at the end of
    // the error chain: the SQLSTATE, the server's message, and the DETAIL
    // naming the offending row.
    let diagnosed = errors.iter().find(|e| e.message.contains("E23502"));
    let diagnosed = diagnosed
        .unwrap_or_else(|| panic!("no error reports the SQLSTATE: {errors:#?}"))
        .message
        .as_str();
    assert!(
        diagnosed.contains("violates not-null constraint"),
        "error drops the server's message: {diagnosed}"
    );
    assert!(
        diagnosed.contains("DETAIL: Failing row contains"),
        "error drops the server's DETAIL: {diagnosed}"
    );

    // At least one failed statement has
    // to have carried a payload far larger than the message describing it.
    let largest_payload = errors
        .iter()
        .filter_map(|e| payload_bytes_in_error(&e.message))
        .max()
        .expect("no error reports its payload size");
    assert!(
        largest_payload > 64 * 1024,
        "the connector never buffered a large batch (largest was {largest_payload} bytes), \
         so this test would pass without truncation"
    );

    let fatal_error = fatal_error.expect("endpoint reported no fatal error");
    assert!(
        fatal_error.len() <= MAX_CONNECTOR_ERROR_LEN,
        "fatal_error is {} bytes",
        fatal_error.len()
    );
}

/// Serializes `data` as the newline-delimited JSON into a temp file the file input reads.
fn write_records_to_temp_file(data: &[Value]) -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().unwrap();
    for datum in data {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        temp_file.as_file_mut().write_all(&row_bytes).unwrap();
        temp_file.write_all(b"\n").unwrap();
    }
    temp_file
}

/// Pipeline that feeds the records in `path` to a postgres output connector
/// writing to `table_name` through `threads` connections.
///
/// `min_batch_size_records`, when set, makes the pipeline ingest that
/// many records in one large batch.
fn pg_output_test_config(
    path: &Path,
    url: &str,
    table_name: &str,
    threads: usize,
    min_batch_size_records: Option<usize>,
) -> PipelineConfig {
    let mut config = json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": { "name": "file_input", "config": { "path": path } },
          "format": { "name": "json", "config": { "update_format": "raw", "array": false } }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": { "name": "postgres_output", "config": { "uri": url, "table": table_name, "threads": threads } },
          "index": "idx"
        }
      }
    });

    if let Some(records) = min_batch_size_records {
        config["min_batch_size_records"] = json!(records);
        config["max_buffering_delay_usecs"] = json!(5_000_000);
        config["inputs"]["ins"]["transport"]["config"]["byte_size_buffer"] = json!(8_388_608);
    }

    serde_json::from_value(config).unwrap()
}

fn pg_transmitted_totals(controller: &Controller) -> (u64, u64) {
    let status = controller.status();
    let outputs = status.output_status();
    let endpoint = outputs.values().next().expect("output endpoint");
    (
        endpoint
            .metrics
            .transmitted_records
            .load(std::sync::atomic::Ordering::Relaxed),
        endpoint
            .metrics
            .transmitted_bytes
            .load(std::sync::atomic::Ordering::Relaxed),
    )
}

fn pg_total_processed_input_records(controller: &Controller) -> u64 {
    let status = controller.status();
    let outputs = status.output_status();
    let endpoint = outputs.values().next().expect("output endpoint");
    endpoint
        .metrics
        .total_processed_input_records
        .load(std::sync::atomic::Ordering::Relaxed)
}

/// How many failures the reconnect loop has reported.
///
/// Only `retry_connecting_with_backoff` reports under this tag, so a non-zero
/// count means a worker lost its connection and is waiting out a database it
/// cannot reach. That is a state the tests below can wait for, unlike the
/// sleeps they used to spend guessing at it.
fn pg_conn_retry_failures(controller: &Controller) -> usize {
    let status = controller.status();
    let outputs = status.output_status();
    let endpoint = outputs.values().next().expect("output endpoint");
    let errors = endpoint.transport_errors.lock().unwrap().to_api_type();
    errors
        .iter()
        .filter(|e| e.tag.as_deref() == Some("pg_conn_retry"))
        .count()
}

fn pg_row_count(url: &str, table_name: &str) -> u64 {
    let mut client = pg_connect(url, &None);
    let count: i64 = client
        .query_one(&format!("SELECT count(*) FROM {table_name}"), &[])
        .expect("failed to count rows")
        .get(0);
    count as u64
}

/// `transmitted_records` counts what Postgres committed, not what the connector
/// buffered.
///
/// A failing statement aborts the whole transaction, and Postgres answers the
/// following COMMIT with ROLLBACK instead of an error, so a commit that reports
/// success proves nothing on its own.
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn transmitted_records_excludes_rolled_back_rows(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_rollback_count");
    let url = postgres_url();
    let verify_url = url.clone();

    let data: Vec<Value> = random_records(1000);
    let temp_file = write_records_to_temp_file(&data);
    let config = pg_output_test_config(temp_file.path(), &url, &table_name, 1, None);

    // `freshness_timestamp` is NOT NULL, has no DEFAULT, and is absent from the
    // Feldera view, so every INSERT the connector issues aborts its transaction
    // and the batch writes nothing.
    let _table = TempPgTable::new_with_extra_columns(
        &table_name,
        url,
        true,
        &None,
        "freshness_timestamp TIMESTAMP NOT NULL",
    );

    let (controller, _err_receiver) = test_circuit(engine, config);
    controller.start();

    // Wait until the connector has finished handling every record. `total_processed_input_records`
    // reports what the connector handled whether or not the write succeeded, so
    // it advances even though nothing reaches the table, which keeps the
    // assertions below from passing against counters that have yet to move.
    wait(
        || pg_total_processed_input_records(&controller) >= data.len() as u64,
        60_000,
    )
    .expect("timeout: connector did not finish handling the input");

    let (records, bytes) = pg_transmitted_totals(&controller);
    let _ = controller.stop();

    assert_eq!(
        records, 0,
        "reported {records} records as transmitted, but every transaction was rolled back"
    );
    assert_eq!(
        bytes, 0,
        "reported {bytes} bytes as transmitted, but every transaction was rolled back"
    );
    assert_eq!(
        pg_row_count(&verify_url, &table_name),
        0,
        "the table should have rejected every row"
    );
}

/// Records reported by the `... statement for N record(s)` context that the
/// connector attaches to a failed statement.
fn records_in_error(message: &str) -> Option<usize> {
    message
        .split(" statement for ")
        .nth(1)?
        .split(" record(s)")
        .next()?
        .parse()
        .ok()
}

/// A transaction that fails partway discards the statements that already
/// succeeded, so none of the batch counts as transmitted.
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn transmitted_records_excludes_rows_lost_to_a_later_failure(engine: &dyn PipelineTestEngine) {
    const RECORDS: usize = 3000;
    const REJECTED: &str = "__reject__";

    let table_name = unique_pg_name("test_pg_partial_rollback");
    let url = postgres_url();
    let verify_url = url.clone();

    // Enough records to fill the connector's 1 MiB buffer more than once, so the
    // batch issues several statements to postgres. Keys ascend and the cursor walks them in
    // order, which puts the rejected record in the last statement and leaves
    // every statement before it valid.
    let mut data: Vec<Value> = random_records(RECORDS);
    for (i, datum) in data.iter_mut().enumerate() {
        datum["bigint_"] = Value::from(i as i64);
    }
    data.last_mut().unwrap()["varchar_"] = Value::from(REJECTED);

    let temp_file = write_records_to_temp_file(&data);

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      // Step once over the whole input, so it becomes a single transaction.
      "min_batch_size_records": RECORDS,
      "max_buffering_delay_usecs": 5000000,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": { "name": "file_input", "config": { "path": temp_file.path(), "byte_size_buffer": 8388608 } },
          "format": { "name": "json", "config": { "update_format": "raw", "array": false } }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": { "name": "postgres_output", "config": { "uri": url, "table": &table_name } },
          "index": "idx"
        }
      }
    }))
    .unwrap();

    let _table = TempPgTable::new_with_extra_columns(
        &table_name,
        url,
        true,
        &None,
        &format!("CHECK (varchar_ <> '{REJECTED}')"),
    );

    let (controller, _err_receiver) = test_circuit(engine, config);
    controller.start();

    wait(
        || pg_total_processed_input_records(&controller) >= RECORDS as u64,
        60_000,
    )
    .expect("timeout: connector did not finish handling the input");

    let (records, bytes) = pg_transmitted_totals(&controller);
    let errors = {
        let status = controller.status();
        let outputs = status.output_status();
        outputs
            .values()
            .next()
            .expect("output endpoint")
            .transport_errors
            .lock()
            .unwrap()
            .to_api_type()
    };
    let _ = controller.stop();

    // Guard against the test going vacuous: the rejected record has to arrive in
    // a statement that carries only part of the batch, so that an earlier
    // statement succeeded and had rows to lose.
    let failed_records = errors
        .iter()
        .filter_map(|e| records_in_error(&e.message))
        .min()
        .expect("no statement failure was reported");
    assert!(
        failed_records < RECORDS,
        "the whole batch went out as one statement of {failed_records} records, so no earlier \
         statement succeeded and this test would pass without the commit guard"
    );

    assert_eq!(
        records, 0,
        "reported {records} records as transmitted, but the transaction was rolled back"
    );
    assert_eq!(
        bytes, 0,
        "reported {bytes} bytes as transmitted, but the transaction was rolled back"
    );
    assert_eq!(
        pg_row_count(&verify_url, &table_name),
        0,
        "the aborted transaction should have left the table empty"
    );
}

/// Selects the backends that the connector under test is writing a batch
/// through, which is where the tests below inject their faults. Callers bind
/// `$1` to the pattern from [`statements_on`].
///
/// Postgres serves each connection from a process of its own, a backend, lists
/// one row per backend in `pg_stat_activity`, and terminates one on
/// `pg_terminate_backend(pid)`. That is the whole mechanism: a thread of the
/// test connects as an ordinary client and terminates the connector's backends,
/// which the connector sees as its connection dropping, taking with it the
/// transaction it was writing the batch into. The backends to terminate are the
/// ones connected to this database (`datname`), inside a transaction
/// (`xact_start`), whose latest statement is one of the connector's writes,
/// which all call `jsonb_populate_recordset` on the table under test;
/// `pg_backend_pid` excludes the injecting thread's own backend.
///
/// Matching the table keeps the fault inside the test that injects it. These
/// tests share one database, so a predicate that named only the connector's
/// statements would match another test's connector just as readily: that fails
/// a test which asked for no fault, and counts here as a fault this test landed,
/// which is worse, since the count is how these tests know their fault arrived.
///
/// `xact_start` narrows the match to a backend with a statement in flight or a
/// transaction open. It does not tell writing a statement apart from preparing
/// one: preparing puts the statement's text through the parser, which reports it
/// in `query` under a transaction of its own.
///
/// The window in which terminating a backend lands mid-batch runs from the
/// connector's first write statement until its transaction ends. It covers the
/// whole batch rather than the instant a statement is running, because Postgres
/// keeps reporting the last statement in `query` while the backend sits idle
/// inside its transaction. Terminating a backend before the window opens costs
/// the connector no more than an idle connection, or statements it prepares
/// again; terminating one after the window closes finds the batch already
/// committed. Neither exercises the reconnect path under test.
///
/// The window is measured in milliseconds, so every caller arms its terminate
/// loop before the batch starts and lets that loop do the finding. Watching for
/// the window and acting on what it saw takes a second round trip, which the
/// batch can commit inside of.
const BATCH_BACKENDS: &str = "SELECT pid FROM pg_stat_activity \
     WHERE pid <> pg_backend_pid() AND datname = current_database() \
       AND xact_start IS NOT NULL \
       AND query ILIKE '%jsonb_populate_recordset%' \
       AND query LIKE $1";

/// The pattern that matches the statements the connector writes to `table`,
/// whose name it quotes in every one of them.
fn statements_on(table: &str) -> String {
    format!("%\"{table}\"%")
}

/// Terminates every backend that the connector under test has inside a batch
/// transaction, and reports whether it found one to terminate.
///
/// Finding and terminating are one statement on purpose. The window is a matter
/// of milliseconds, so checking first and terminating in a second round trip
/// leaves a gap the batch can commit in.
fn terminate_batch_backends(client: &mut postgres::Client, table: &str) -> bool {
    let terminated: i64 = client
        .query_one(
            &format!(
                "SELECT count(*) FROM \
                 (SELECT pg_terminate_backend(pid) FROM ({BATCH_BACKENDS}) backends) terminated"
            ),
            &[&statements_on(table)],
        )
        .map(|row| row.get(0))
        .unwrap_or(0);
    terminated > 0
}

/// Polls for up to eight seconds until it terminates a backend of the connector
/// writing to `table` inside a batch transaction.
fn poll_and_terminate_batch_backends(client: &mut postgres::Client, table: &str) -> bool {
    for _ in 0..40_000 {
        if terminate_batch_backends(client, table) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_micros(200));
    }
    false
}

/// Drops the connector's connections mid-batch, `rounds` times over, and reports
/// how many rounds landed.
///
/// A round lands when it terminates a backend inside the window. A round that
/// misses ends the loop; the caller asserts on the count, so a test whose fault
/// never landed fails instead of passing.
fn terminate_connector_backends(url: &str, table: &str, rounds: usize) -> usize {
    let mut client = pg_connect(url, &None);
    let mut landed = 0;

    for _ in 0..rounds {
        if !poll_and_terminate_batch_backends(&mut client, table) {
            break;
        }
        landed += 1;
    }

    landed
}

/// Losing the connection in the middle of a batch must not lose the batch.
///
/// Reconnecting is not enough on its own: the dropped connection rolled its
/// transaction back, and the payloads that carried those rows were drained from
/// the buffers as they flushed, so the batch has to be encoded again from the
/// cursor the worker retained.
///
/// `threads` is the number of connections the connector writes through, each
/// with a transaction and a partition of the batch of its own. `kills` is how
/// many connection drops to inject, one round after another: any drop past the
/// first lands while the connector is writing the batch again.
fn pg_reconnect_test(engine: &dyn PipelineTestEngine, name: &str, threads: usize, kills: usize) {
    // Enough records per connection to fill the connector's 1 MiB buffer more
    // than once, so that statements have already written rows into the
    // transaction by the time the connection drops.
    //
    // Scale with `threads`, because the batch is split between them: sizing the
    // whole batch instead left each connection with less than a bufferful, so it
    // wrote its partition in a single statement and committed. That shrank the
    // window from 79ms to 15ms, narrow enough to miss on CI.
    let records = 3000 * threads;

    let table_name = unique_pg_name(name);
    let url = postgres_url();
    let verify_url = url.clone();

    let mut data: Vec<Value> = random_records(records);
    for (i, datum) in data.iter_mut().enumerate() {
        datum["bigint_"] = Value::from(i as i64);
    }
    let temp_file = write_records_to_temp_file(&data);

    let config = pg_output_test_config(temp_file.path(), &url, &table_name, threads, Some(records));

    let _table = TempPgTable::new(&table_name, url, true, &None);

    // Build the pipeline before arming the killer: the connector connects and
    // prepares its statements while the controller is being constructed.
    let (controller, _err_receiver) = test_circuit(engine, config);

    // The killer is the fault injector: a thread that terminates the connector's
    // backends over SQL from a connection of its own, as `BATCH_BACKENDS` describes.
    let killer_url = verify_url.clone();
    let killer_table = table_name.clone();
    let killer =
        std::thread::spawn(move || terminate_connector_backends(&killer_url, &killer_table, kills));

    controller.start();
    let landed = killer.join().expect("killer thread panicked");

    // The endpoint reports the batch as processed whether or not it wrote it, so
    // this settles the test in both outcomes.
    wait(
        || pg_total_processed_input_records(&controller) >= records as u64,
        120_000,
    )
    .expect("timeout: connector did not finish handling the input");

    let (transmitted, _bytes) = pg_transmitted_totals(&controller);
    let fatal_error = {
        let status = controller.status();
        let outputs = status.output_status();
        outputs
            .values()
            .next()
            .expect("output endpoint")
            .fatal_error()
    };
    let _ = controller.stop();

    assert_eq!(
        landed, kills,
        "landed {landed} of {kills} connection drops, so this test proves less than it should"
    );
    assert_eq!(
        pg_row_count(&verify_url, &table_name),
        records as u64,
        "rows were lost when the connection dropped"
    );
    assert_eq!(
        transmitted, records as u64,
        "reported {transmitted} of {records} rows as transmitted"
    );

    assert_eq!(
        fatal_error, None,
        "the connector recovered, yet the endpoint is marked as fatally failed"
    );
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn upsert0(engine: &dyn PipelineTestEngine) {
    pg_upsert0(engine, PostgresWriteMode::Materialized);
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn upsert0_cdc(engine: &dyn PipelineTestEngine) {
    pg_upsert0(engine, PostgresWriteMode::Cdc);
}

fn pg_upsert0(engine: &dyn PipelineTestEngine, mode: PostgresWriteMode) {
    let table_name = unique_pg_name("test_pg_upsert");
    let url = postgres_url();
    let max_buffer_size_bytes = 1024;
    let timeout_ms = 120_000;

    let data: Vec<Value> = random_records(10000);
    let mut insert_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        insert_file.as_file_mut().write_all(&row_bytes).unwrap();
        insert_file.write_all(b"\n").unwrap();
    }

    let upsert_data = data
        .clone()
        .into_iter()
        .map(|mut d| {
            d["varchar_"] = Value::from("updated");
            d
        })
        .collect::<Vec<_>>();

    let mut upsert_file = NamedTempFile::new().unwrap();

    for old in data.iter() {
        let row_bytes = serde_json::to_vec(&old).unwrap();
        upsert_file
            .as_file_mut()
            .write_all(br#"{"delete": "#)
            .unwrap();
        upsert_file.as_file_mut().write_all(&row_bytes).unwrap();
        upsert_file.write_all(b"}\n").unwrap();
    }

    for new in upsert_data.iter() {
        let row_bytes = serde_json::to_vec(&new).unwrap();
        upsert_file
            .as_file_mut()
            .write_all(br#"{"insert": "#)
            .unwrap();
        upsert_file.as_file_mut().write_all(&row_bytes).unwrap();
        upsert_file.write_all(b"}\n").unwrap();
    }

    let config = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "ins": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": insert_file.path(),
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                        "array": false
                    }
                }
            },
            "ups": {
                "paused": true,
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": upsert_file.path(),
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete",
                        "array": false
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "postgres_output",
                    "config": {
                        "uri": url,
                        "table": &table_name,
                        "max_buffer_size_bytes": max_buffer_size_bytes,
                        "mode": match mode {
                            PostgresWriteMode::Materialized => "materialized",
                            PostgresWriteMode::Cdc => "cdc",
                        }

                    }
                },
                "index": "idx"
            }
        }
    }))
    .unwrap();

    let mut table = TempPgTable::new(
        &table_name,
        url,
        matches!(mode, PostgresWriteMode::Materialized),
        &None,
    );

    let start = now_micros();
    let (controller, err_receiver) = test_circuit(engine, config);

    controller.start();

    wait(
        || match mode {
            PostgresWriteMode::Materialized => {
                let rows = table.query();

                let got = records_of(rows);

                same_rows(&got, &data) || !err_receiver.is_empty()
            }
            PostgresWriteMode::Cdc => {
                let rows = table.query();

                let got = cdc_records_of(rows);

                same_rows(&rows_only(&got), &data) && got.iter().all(|(_, op)| op == "i")
                    || !err_receiver.is_empty()
            }
        },
        timeout_ms,
    )
    .expect("timeout: failed to insert data into postgres");

    let run1 = start..=now_micros();
    if matches!(mode, PostgresWriteMode::Cdc) {
        check_cdc_times(table.query(), |_, _| run1.clone());
    }

    let start = now_micros();
    controller
        .start_input_endpoint("ups")
        .expect("failed to start upsert input file connector");

    wait(
        || match mode {
            PostgresWriteMode::Materialized => {
                let rows = table.query();

                let got = records_of(rows);

                same_rows(&got, &upsert_data) || !err_receiver.is_empty()
            }
            PostgresWriteMode::Cdc => {
                let rows = table.query();

                let got = cdc_records_of(rows);

                let deletes = got
                    .iter()
                    .filter(|(_, op)| op == "d")
                    .cloned()
                    .collect::<Vec<_>>();

                let updates = got
                    .iter()
                    .filter(|(r, op)| r["varchar_"] == "updated" && op == "i")
                    .cloned()
                    .collect::<Vec<_>>();

                same_rows(&rows_only(&deletes), &data)
                    && same_rows(&rows_only(&updates), &upsert_data)
                    || !err_receiver.is_empty()
            }
        },
        timeout_ms,
    )
    .expect("timeout: failed to update data into postgres");

    if matches!(mode, PostgresWriteMode::Cdc) {
        // The first run inserts the original rows; the second deletes them and inserts the
        // updated ones.
        let rows = table.query();
        let run2 = start..=now_micros();
        check_cdc_times(rows, |record, op| {
            if op == "d" || record["varchar_"] == "updated" {
                run2.clone()
            } else {
                run1.clone()
            }
        });
    }
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn upsert(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_upsert");
    let url = postgres_url();
    let max_records_in_buffer = 1000;

    let data: Vec<Value> = random_records(10000);
    let mut insert_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        insert_file.as_file_mut().write_all(&row_bytes).unwrap();
        insert_file.write_all(b"\n").unwrap();
    }

    let upsert_data = data
        .clone()
        .into_iter()
        .map(|mut d| {
            d["varchar_"] = Value::from("updated");
            d
        })
        .collect::<Vec<_>>();

    let mut upsert_file = NamedTempFile::new().unwrap();

    for old in data.iter() {
        let row_bytes = serde_json::to_vec(&old).unwrap();
        upsert_file
            .as_file_mut()
            .write_all(br#"{"delete": "#)
            .unwrap();
        upsert_file.as_file_mut().write_all(&row_bytes).unwrap();
        upsert_file.write_all(b"}\n").unwrap();
    }

    for new in upsert_data.iter() {
        let row_bytes = serde_json::to_vec(&new).unwrap();
        upsert_file
            .as_file_mut()
            .write_all(br#"{"insert": "#)
            .unwrap();
        upsert_file.as_file_mut().write_all(&row_bytes).unwrap();
        upsert_file.write_all(b"}\n").unwrap();
    }

    let config = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
        "inputs": {
            "ins": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": insert_file.path(),
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "raw",
                        "array": false
                    }
                }
            },
            "ups": {
                "paused": true,
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": upsert_file.path(),
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete",
                        "array": false
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "postgres_output",
                    "config": {
                        "uri": url,
                      "table": &table_name,
                        "max_records_in_buffer": max_records_in_buffer
                    }
                },
                "index": "idx"
            }
        }
    }))
    .unwrap();

    let mut table = TempPgTable::new(&table_name, url, true, &None);

    let (controller, err_receiver) = test_circuit(engine, config);

    controller.start();

    wait(
        || {
            let rows = table.query();

            let got = records_of(rows);

            same_rows(&got, &data) || !err_receiver.is_empty()
        },
        40_000,
    )
    .expect("timeout: failed to insert data into postgres");

    controller
        .start_input_endpoint("ups")
        .expect("failed to start upsert input file connector");

    wait(
        || {
            let rows = table.query();

            let got = records_of(rows);

            same_rows(&got, &upsert_data) || !err_receiver.is_empty()
        },
        40_000,
    )
    .expect("timeout: failed to update data into postgres");
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn delete(engine: &dyn PipelineTestEngine) {
    pg_delete(engine, PostgresWriteMode::Materialized)
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn delete_cdc(engine: &dyn PipelineTestEngine) {
    pg_delete(engine, PostgresWriteMode::Cdc)
}

fn pg_delete(engine: &dyn PipelineTestEngine, mode: PostgresWriteMode) {
    let table_name = unique_pg_name("test_pg_delete");
    let url = postgres_url();

    let data: Vec<Value> = random_records(10000);
    let mut insert_file = NamedTempFile::new().unwrap();

    for datum in data.iter() {
        let row_bytes = serde_json::to_vec(&datum).unwrap();
        insert_file.as_file_mut().write_all(&row_bytes).unwrap();
        insert_file.write_all(b"\n").unwrap();
    }

    let mut delete_file = NamedTempFile::new().unwrap();

    for old in data.iter() {
        let row_bytes = serde_json::to_vec(&old).unwrap();
        delete_file
            .as_file_mut()
            .write_all(br#"{"delete": "#)
            .unwrap();
        delete_file.as_file_mut().write_all(&row_bytes).unwrap();
        delete_file.write_all(b"}\n").unwrap();
    }

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": insert_file.path()
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "raw",
              "array": false
            }
          }
        },
        "dels": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": delete_file.path()
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete",
              "array": false
            }
          }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "postgres_output",
            "config": {
              "uri": url,
              "table": &table_name,
              "mode": match mode {
                  PostgresWriteMode::Materialized => "materialized",
                  PostgresWriteMode::Cdc => "cdc",
              }
            }
          },
          "index": "idx"
        }
      }
    }))
    .unwrap();

    let mut table = TempPgTable::new(
        &table_name,
        url,
        matches!(mode, PostgresWriteMode::Materialized),
        &None,
    );

    let start = now_micros();
    let (controller, err_receiver) = test_circuit(engine, config);

    controller.start();

    wait(
        || match mode {
            PostgresWriteMode::Materialized => {
                let rows = table.query();

                let got = records_of(rows);

                same_rows(&got, &data) || !err_receiver.is_empty()
            }
            PostgresWriteMode::Cdc => {
                let rows = table.query();

                let got = cdc_records_of(rows);

                same_rows(&rows_only(&got), &data) && got.iter().all(|(_, op)| op == "i")
                    || !err_receiver.is_empty()
            }
        },
        40_000,
    )
    .expect("timeout: failed to insert data into postgres");

    let run1 = start..=now_micros();
    if matches!(mode, PostgresWriteMode::Cdc) {
        check_cdc_times(table.query(), |_, _| run1.clone());
    }

    let start = now_micros();
    controller
        .start_input_endpoint("dels")
        .expect("failed to start delete input file connector");

    wait(
        || match mode {
            PostgresWriteMode::Materialized => {
                let rows = table.query();

                let got = records_of(rows);

                got.is_empty() || !err_receiver.is_empty()
            }
            PostgresWriteMode::Cdc => {
                let rows = table.query();

                let got = cdc_records_of(rows);

                let got = got
                    .iter()
                    .filter(|(_, op)| op != "i")
                    .cloned()
                    .collect::<Vec<_>>();

                // Comparing through the relation parses each column, the decimals and maps too.
                same_rows(&rows_only(&got), &data) && got.iter().all(|(_, op)| op == "d")
                    || !err_receiver.is_empty()
            }
        },
        40_000,
    )
    .expect("timeout: failed to delete data from postgres");

    if matches!(mode, PostgresWriteMode::Cdc) {
        let rows = table.query();
        let run2 = start..=now_micros();
        check_cdc_times(rows, |_, op| {
            if op == "i" {
                run1.clone()
            } else {
                run2.clone()
            }
        });
    }
}

fn pg_simple(engine: &dyn PipelineTestEngine, url: String, tls: Option<PostgresTlsConfig>) {
    let table_name = unique_pg_name("simple_test");

    let mut client = pg_connect(&url, &tls);
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS {table_name} (
    id int primary key,
    b bool not null,
    i int8,
    s varchar not null
)"#
            ),
            &[],
        )
        .expect("failed to create test table in postgres");

    client
        .execute(&format!("TRUNCATE {table_name}"), &[])
        .unwrap();

    let input_file = NamedTempFile::new().unwrap();
    let data = vec![
        json!({ "id": 1, "b": true, "i": null, "s": "test" }),
        json!({ "id": 2, "b": false, "i": 1, "s": "test" }),
    ];

    input_file
        .as_file()
        .write_all(&serde_json::to_vec(&data).unwrap())
        .unwrap();

    let upsert_file = NamedTempFile::new().unwrap();
    let upsert_data: Vec<serde_json::Value> = data
        .clone()
        .into_iter()
        .flat_map(|d| {
            let updated = {
                let mut n = d.clone();
                n["s"] = Value::from("updated");
                n
            };
            [
                json!({
                    "delete": d,
                }),
                json!({
                    "insert": updated,
                }),
            ]
        })
        .collect();

    upsert_file
        .as_file()
        .write_all(&serde_json::to_vec(&upsert_data).unwrap())
        .unwrap();

    let delete_file = NamedTempFile::new().unwrap();
    delete_file
        .as_file()
        .write_all(
            &serde_json::to_vec(
                &data
                    .clone()
                    .into_iter()
                    .map(|d| json!({"delete": d}))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap();

    let idx = "v1_idx";

    let mut pg_output_config = json!({
        "uri": url,
        "table": &table_name,
    });
    if let Some(ref tls) = tls {
        let tls_json = serde_json::to_value(tls).unwrap();
        pg_output_config
            .as_object_mut()
            .unwrap()
            .extend(tls_json.as_object().unwrap().clone());
    }

    let config = serde_json::from_value(json!({
      "name": "test",
      "workers": 4,
      "inputs": {
        "ins": {
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": input_file.path(),
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "raw",
              "array": true
            }
          }
        },
        "ups": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": upsert_file.path()
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete",
              "array": true
            }
          }
        },
        "del": {
          "paused": true,
          "stream": "test_input1",
          "transport": {
            "name": "file_input",
            "config": {
              "path": delete_file.path()
            }
          },
          "format": {
            "name": "json",
            "config": {
              "update_format": "insert_delete",
              "array": true
            }
          }
        }
      },
      "outputs": {
        "test_output1": {
          "stream": "test_output1",
          "transport": {
            "name": "postgres_output",
            "config": pg_output_config,
          },
          "index": idx
        }
      }
    }))
    .unwrap();

    // An index on the view, which holds the rows of the table as they arrive.
    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = engine
        .controller(
            &TestCircuit::IndexedView {
                key: basic_key_relation(),
                relation: basic_relation(),
                index: idx.to_string(),
            },
            &config,
            Box::new(move |e, _| {
                let msg = format!("postgres_output_test: error: {e}");
                println!("{msg}");
                err_sender.send(msg).unwrap()
            }),
        )
        .unwrap();

    controller.start();

    wait(
        ||
            {
                let rows = client
                    .query(
                        &format!("SELECT * FROM {table_name} ORDER BY id"),
                        &[],
                    )
                    .unwrap();
                let got: Vec<Value> = rows
                    .into_iter()
                    .map(|row| {
                        let id: i32 = row.get(0);
                        let b: bool = row.get(1);
                        let i: Option<i64> = row.get(2);
                        let s: String = row.get(3);

                        json!({ "id": id, "b": b, "i": i, "s": s })
                    })
                    .collect();

                got == data
            }
            || !err_receiver.is_empty(),
        1_000,
    )
    .expect("timeout: inserting records into postgres failed");

    controller.start_input_endpoint("ups").unwrap();

    wait(
        ||
            {
                client
                    .query(&format!("SELECT * FROM {table_name}"), &[])
                    .unwrap()
                    .iter()
                    .all(|r| {
                        let s: String = r.get("s");
                        s.as_str() == "updated"
                    })
            }
            || !err_receiver.is_empty(),
        1_000,
    )
    .expect("timeout");

    controller.start_input_endpoint("del").unwrap();

    wait(
        || {
            client
                .query(&format!("SELECT * FROM {table_name}"), &[])
                .unwrap()
                .is_empty()
                || !err_receiver.is_empty()
        },
        1_000,
    )
    .expect("timedout: failed to delete records");

    client
        .execute(&format!("DROP TABLE {table_name}"), &[])
        .unwrap();
}

#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn simple(engine: &dyn PipelineTestEngine) {
    pg_simple(engine, postgres_url(), None);
}

/// Same as `test_pg_simple` but over a TLS connection.
#[engine_test(serial, requires(PostgresRendering), services(PostgresTls))]
pub fn simple_tls(engine: &dyn PipelineTestEngine) {
    let (url, tls) = postgres_ssl_config();
    pg_simple(engine, url, Some(tls));
}

/// Test the postgres *input* connector over a TLS connection.
///
/// Inserts rows into TLS-enabled postgres directly, then reads them back
/// through the `postgres_input` transport and writes them to a file.
#[engine_test(serial, services(PostgresTls))]
pub fn input_tls(engine: &dyn PipelineTestEngine) {
    let (url, tls) = postgres_ssl_config();
    let table_name = unique_pg_name("test_pg_input_tls");

    // -- seed the table via the sync client --
    let mut client = pg_connect(&url, &Some(tls.clone()));
    client
        .execute(
            &format!(
                r#"CREATE TABLE IF NOT EXISTS {table_name} (
    id int primary key,
    b bool not null,
    i int8,
    s varchar not null
)"#
            ),
            &[],
        )
        .expect("failed to create test table");
    client
        .execute(&format!("TRUNCATE {table_name}"), &[])
        .unwrap();
    client
        .execute(
            &format!("INSERT INTO {table_name} VALUES (1, true, NULL, 'hello')"),
            &[],
        )
        .unwrap();
    client
        .execute(
            &format!("INSERT INTO {table_name} VALUES (2, false, 42, 'world')"),
            &[],
        )
        .unwrap();

    // -- set up the pipeline: postgres_input -> file_output --
    let output_file = NamedTempFile::new().unwrap();
    let output_path = output_file.path().to_owned();

    let mut pg_input_config = json!({
        "uri": url,
        "query": format!("SELECT * FROM {table_name} ORDER BY id"),
    });
    let tls_json = serde_json::to_value(&tls).unwrap();
    pg_input_config
        .as_object_mut()
        .unwrap()
        .extend(tls_json.as_object().unwrap().clone());

    let config = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "inputs": {
            "pg_in": {
                "stream": "test_input1",
                "transport": {
                    "name": "postgres_input",
                    "config": pg_input_config,
                },
            },
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path,
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete",
                        "array": false,
                    }
                }
            }
        }
    }))
    .unwrap();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = engine
        .controller(
            &TestCircuit::identity(basic_relation()),
            &config,
            Box::new(move |e, _| {
                let msg = format!("test_pg_input_tls: error: {e}");
                println!("{msg}");
                err_sender.send(msg).unwrap()
            }),
        )
        .unwrap();

    controller.start();

    // Wait for the output file to contain the expected data.
    wait(
        || {
            let content = std::fs::read_to_string(&output_path).unwrap_or_default();
            let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
            lines.len() >= 2 || !err_receiver.is_empty()
        },
        10_000,
    )
    .expect("timeout: postgres input TLS test did not produce output");

    assert!(err_receiver.is_empty(), "unexpected errors in pipeline");

    let content = std::fs::read_to_string(&output_path).unwrap();
    let rows: Vec<serde_json::Value> = content
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["insert"]["id"], 1);
    assert_eq!(rows[0]["insert"]["b"], true);
    assert!(rows[0]["insert"]["i"].is_null());
    assert_eq!(rows[0]["insert"]["s"], "hello");
    assert_eq!(rows[1]["insert"]["id"], 2);
    assert_eq!(rows[1]["insert"]["b"], false);
    assert_eq!(rows[1]["insert"]["i"], 42);
    assert_eq!(rows[1]["insert"]["s"], "world");

    controller.stop().unwrap();

    client
        .execute(&format!("DROP TABLE {table_name}"), &[])
        .unwrap();
}

/// A runtime failure of the connector's query must be reported to the
/// controller as a fatal input error.
///
/// Regression test: initialization succeeds because the connection to
/// PostgreSQL can be established, so by the time the query executes, the
/// init-status channel has been consumed. The query error used to be sent on
/// that channel and silently dropped, so the connector terminated without
/// notifying the controller and the pipeline stalled forever.
#[engine_test(serial, services(Postgres))]
pub fn input_query_error(engine: &dyn PipelineTestEngine) {
    let url = postgres_url();

    let output_file = NamedTempFile::new().unwrap();
    let output_path = output_file.path().to_owned();

    let config = serde_json::from_value(json!({
        "name": "test",
        "workers": 1,
        "inputs": {
            "pg_in": {
                "stream": "test_input1",
                "transport": {
                    "name": "postgres_input",
                    "config": {
                        "uri": url,
                        "query": "SELECT * FROM feldera_missing_table_regression",
                    },
                },
            },
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": {
                        "path": output_path,
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete",
                        "array": false,
                    }
                }
            }
        }
    }))
    .unwrap();

    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = engine
        .controller(
            &TestCircuit::identity(basic_relation()),
            &config,
            Box::new(move |e, _| {
                err_sender
                    .send(format!("test_pg_input_query_error: {e}"))
                    .unwrap()
            }),
        )
        .unwrap();

    controller.start();

    wait(|| !err_receiver.is_empty(), 10_000)
        .expect("timeout: postgres input query error was not reported to the controller");

    let error = err_receiver.recv().unwrap();
    assert!(
        error.contains("error executing query"),
        "unexpected error: {error}"
    );

    controller.stop().unwrap();
}

/// A unique-key constraint violation in an indexed postgres output must skip
/// only the offending key, not the rest of the batch, and must report one error
/// per non-unique key.
///
/// The connector is configured with `index: idx`, so every key is expected to
/// map to a single value.  We feed a single batch in which most keys are unique
/// but two keys (`bigint_` 100 and 200) carry two distinct values each.  The
/// connector must (a) write every well-formed record and (b) report a
/// uniqueness-violation error for each of the two offending keys while leaving
/// their records out of the table.
///
/// Output buffering is sized so the whole input collapses into one output
/// batch; this guarantees that both values of each non-unique key are encoded
/// together (otherwise the conflict would not be observable within a single
/// batch).
#[engine_test(serial, requires(PostgresRendering), services(Postgres))]
pub fn non_unique_keys_skipped(engine: &dyn PipelineTestEngine) {
    let table_name = unique_pg_name("test_pg_non_unique");
    let url = postgres_url();

    let make_record = default_record;

    // Ten well-formed keys, one value each.
    let unique: Vec<Value> = (0..10).map(make_record).collect();

    // Two non-unique keys, each with two distinct values in the same batch.
    let make_conflict = |bigint_: i64| -> [Value; 2] {
        let first = make_record(bigint_);
        let mut second = first.clone();
        second["int_"] = Value::from((first["int_"].as_i64().unwrap() as i32).wrapping_add(1));
        assert_eq!(bigint(&first), bigint(&second));
        assert_ne!(first, second);
        [first, second]
    };
    let conflict_a = make_conflict(100);
    let conflict_b = make_conflict(200);

    let mut input_file = NamedTempFile::new().unwrap();
    for record in unique
        .iter()
        .chain(conflict_a.iter())
        .chain(conflict_b.iter())
    {
        let row_bytes = serde_json::to_vec(&record).unwrap();
        input_file.as_file_mut().write_all(&row_bytes).unwrap();
        input_file.write_all(b"\n").unwrap();
    }

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test",
        "workers": 4,
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
                "index": "idx",
                "enable_output_buffer": true,
                "max_output_buffer_size_records": 1_000_000,
                "max_output_buffer_time_millis": 2_000,
                "transport": {
                    "name": "postgres_output",
                    "config": {
                        "uri": url.clone(),
                        "table": &table_name
                    }
                }
            }
        }
    }))
    .unwrap();

    // The connector uses INSERT ... ON CONFLICT DO UPDATE, which requires a
    // UNIQUE constraint.  The Feldera-level check in `indexed_operation_type`
    // filters out conflicting keys before they reach postgres.
    let mut table = TempPgTable::new(&table_name, url, true, &None);

    let errors = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
    let errors_clone = errors.clone();

    let controller = engine
        .controller(
            &indexed_view(),
            &config,
            Box::new(move |e, tag| {
                errors_clone.lock().unwrap().push((e.to_string(), tag));
            }),
        )
        .unwrap();

    controller.start();

    // Wait until exactly the ten unique records have been committed to postgres.
    wait(|| table.query().len() == unique.len(), 60_000)
        .expect("timeout waiting for the well-formed records to reach postgres");

    controller.stop().unwrap();

    // (a) Exactly the well-formed records are written; the non-unique keys are
    //     dropped entirely.
    let written = records_of(table.query());
    assert!(
        same_rows(&written, &unique),
        "non-unique keys must be skipped while every unique key is preserved"
    );
    assert!(
        written.iter().all(|r| bigint(r) != 100 && bigint(r) != 200),
        "no record for a non-unique key should appear in the output"
    );

    // (b) One uniqueness-violation error is reported per non-unique key.
    let errors = errors.lock().unwrap();
    let violations: Vec<&(String, Option<String>)> = errors
        .iter()
        .filter(|(_, tag)| {
            tag.as_deref()
                .is_some_and(|t| t.contains("pg_uniqueness_violation"))
        })
        .collect();
    assert_eq!(
        violations.len(),
        2,
        "expected one uniqueness-violation error per non-unique key, got: {errors:?}"
    );
    assert!(
        violations.iter().any(|(msg, _)| msg.contains("100")),
        "a uniqueness-violation error should name key 100: {violations:?}"
    );
    assert!(
        violations.iter().any(|(msg, _)| msg.contains("200")),
        "a uniqueness-violation error should name key 200: {violations:?}"
    );
}

// Postgres CDC input connector tests that reach no engine. The `postgres_cdc`
// connector suite holds the pipeline tests.

#[cfg(feature = "with-postgres-cdc")]
pub mod cdc_scenarios;

#[cfg(feature = "with-postgres-cdc")]
pub mod cdc_privileges;

#[cfg(feature = "with-postgres-cdc")]
pub mod cdc_tests {
    use crate::{
        Controller,
        integrated::postgres::test::{
            pg::pg_connect, postgres_cdc_ssl_config, postgres_url, unique_pg_name,
        },
        test::{
            engine_test,
            pipeline::{PipelineTestEngine, TestCircuit},
            relations, wait,
        },
    };
    use feldera_types::{
        config::PipelineConfig,
        program_schema::{ColumnType, Field, Relation},
        transport::postgres::PostgresTlsConfig,
    };
    use serde_json::json;
    #[cfg(test)]
    use serial_test::serial;
    use std::{
        collections::BTreeMap,
        path::Path,
        time::{Duration, Instant},
    };
    use tempfile::NamedTempFile;

    /// Helper: creates a table, publication, and sets REPLICA IDENTITY FULL.
    /// Returns a connected client for further DML operations.
    /// On drop, cleans up the publication and table.
    pub(super) struct CdcTestTable {
        pub(super) client: postgres::Client,
        pub(super) table_name: String,
        pub(super) publication_name: String,
        pub(super) url: String,
    }

    impl CdcTestTable {
        pub(super) fn new_simple(table_name: &str, publication_name: &str, url: &str) -> Self {
            Self::new_simple_with_tls(table_name, publication_name, url, None)
        }

        fn new_simple_with_tls(
            table_name: &str,
            publication_name: &str,
            url: &str,
            tls: Option<&PostgresTlsConfig>,
        ) -> Self {
            let tls = tls.cloned();
            let mut client = pg_connect(url, &tls);

            // Clean up any leftover objects from previous runs.
            let _ = client.execute(
                &format!("DROP PUBLICATION IF EXISTS {publication_name}"),
                &[],
            );
            let _ = client.execute(&format!("DROP TABLE IF EXISTS {table_name}"), &[]);

            client
                .execute(
                    &format!(
                        r#"CREATE TABLE {table_name} (
id   INTEGER PRIMARY KEY,
b    BOOLEAN NOT NULL,
i    BIGINT,
s    VARCHAR NOT NULL
)"#
                    ),
                    &[],
                )
                .expect("failed to create CDC test table");

            client
                .execute(
                    &format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL"),
                    &[],
                )
                .expect("failed to set REPLICA IDENTITY FULL");

            client
                .execute(
                    &format!("CREATE PUBLICATION {publication_name} FOR TABLE {table_name}"),
                    &[],
                )
                .expect("failed to create publication");

            CdcTestTable {
                client,
                table_name: table_name.to_string(),
                publication_name: publication_name.to_string(),
                url: url.to_string(),
            }
        }

        /// Creates a table with many Postgres types for data type coverage testing.
        pub(super) fn new_all_types(table_name: &str, publication_name: &str, url: &str) -> Self {
            let mut client = pg_connect(url, &None);

            let _ = client.execute(
                &format!("DROP PUBLICATION IF EXISTS {publication_name}"),
                &[],
            );
            let _ = client.execute(&format!("DROP TABLE IF EXISTS {table_name}"), &[]);

            client
                .execute(
                    &format!(
                        r#"CREATE TABLE {table_name} (
id              SERIAL PRIMARY KEY,
col_text        TEXT,
col_integer     INTEGER,
col_bigint      BIGINT,
col_boolean     BOOLEAN,
col_real        REAL,
col_double      DOUBLE PRECISION,
col_date        DATE,
col_time        TIME,
col_timetz      TIMETZ,
col_timetz_array TIMETZ[],
col_timestamp   TIMESTAMP,
col_timestamptz TIMESTAMPTZ,
col_uuid        UUID,
col_jsonb       JSONB,
col_bytea       BYTEA,
col_numeric     NUMERIC(10,2),
col_smallint    SMALLINT,
col_int_array   INTEGER[]
)"#
                    ),
                    &[],
                )
                .expect("failed to create CDC all-types test table");

            client
                .execute(
                    &format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL"),
                    &[],
                )
                .expect("failed to set REPLICA IDENTITY FULL");

            client
                .execute(
                    &format!("CREATE PUBLICATION {publication_name} FOR TABLE {table_name}"),
                    &[],
                )
                .expect("failed to create publication");

            CdcTestTable {
                client,
                table_name: table_name.to_string(),
                publication_name: publication_name.to_string(),
                url: url.to_string(),
            }
        }

        pub(super) fn execute(&mut self, query: &str) {
            self.client
                .execute(query, &[])
                .unwrap_or_else(|e| panic!("failed to execute '{query}': {e}"));
        }
    }

    impl Drop for CdcTestTable {
        fn drop(&mut self) {
            self.drop_replication_slots();
            let _ = self.client.execute(
                &format!("DROP PUBLICATION IF EXISTS {}", self.publication_name),
                &[],
            );
            let _ = self
                .client
                .execute(&format!("DROP TABLE IF EXISTS {}", self.table_name), &[]);
        }
    }

    impl CdcTestTable {
        /// Drop the replication slots etl created for this pipeline.
        ///
        /// etl names slots after the *pipeline ID* (e.g.
        /// `supabase_etl_apply_<id>` and `supabase_etl_table_sync_<id>_<oid>`),
        /// not the publication, so we reconstruct that ID exactly as the
        /// connector does. The slot limit is small (4 by default on
        /// `debezium/postgres`); leaking even one slot per test exhausts it
        /// within a couple of tests, so cleanup must be reliable.
        fn drop_replication_slots(&mut self) {
            let connector_url = cdc_connector_url(&self.url);
            let source_table = format!("public.{}", self.table_name);
            let pipeline_id = crate::integrated::postgres::cdc_input::pipeline_id(
                &connector_url,
                &self.publication_name,
                &source_table,
            )
            .to_string();

            // The pipeline ID appears as a `_`-delimited token in every slot
            // name etl creates, so an exact-token match avoids dropping an
            // unrelated slot whose ID merely shares these digits.
            let owns_slot = |name: &str| name.split('_').any(|token| token == pipeline_id);

            for _attempt in 0..20 {
                let slots: Vec<(String, Option<i32>)> = self
                    .client
                    .query(
                        "SELECT slot_name, active_pid FROM pg_replication_slots",
                        &[],
                    )
                    .unwrap_or_default()
                    .into_iter()
                    .map(|r| {
                        (
                            r.get::<_, String>("slot_name"),
                            r.get::<_, Option<i32>>("active_pid"),
                        )
                    })
                    .filter(|(name, _)| owns_slot(name))
                    .collect();

                if slots.is_empty() {
                    return;
                }

                for (slot, active_pid) in &slots {
                    if let Some(pid) = active_pid {
                        let _ = self
                            .client
                            .execute(&format!("SELECT pg_terminate_backend({pid})"), &[]);
                    }
                    let _ = self
                        .client
                        .execute(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[]);
                }

                // Any slots left over had a backend that has not detached yet;
                // wait for it to clear, then retry.
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
        }
    }

    pub(super) fn cdc_connector_url(url: &str) -> String {
        let Ok(mut url) = url::Url::parse(url) else {
            return url.to_string();
        };

        if url.path().trim_start_matches('/').is_empty() {
            let database = if url.username().is_empty() {
                "postgres".to_string()
            } else {
                url.username().to_string()
            };
            url.set_path(&database);
        }

        url.to_string()
    }

    /// Build a test circuit that reads from CDC input and writes to a file output.
    /// The input schema matches the "simple" table (id, b, i, s).
    fn cdc_simple_test_circuit(
        engine: &dyn PipelineTestEngine,
        url: &str,
        publication: &str,
        source_table: &str,
        output_path: &Path,
    ) -> (Controller, crossbeam::channel::Receiver<String>) {
        cdc_simple_test_circuit_with_tls(engine, url, publication, source_table, output_path, None)
    }

    fn cdc_simple_test_circuit_with_tls(
        engine: &dyn PipelineTestEngine,
        url: &str,
        publication: &str,
        source_table: &str,
        output_path: &Path,
        tls: Option<&PostgresTlsConfig>,
    ) -> (Controller, crossbeam::channel::Receiver<String>) {
        try_cdc_simple_test_circuit_with_tls(
            engine,
            url,
            publication,
            source_table,
            output_path,
            tls,
        )
        .unwrap()
    }

    /// Like `cdc_simple_test_circuit_with_tls`, for tests that expect the
    /// connector to refuse its configuration while the controller is built.
    fn try_cdc_simple_test_circuit_with_tls(
        engine: &dyn PipelineTestEngine,
        url: &str,
        publication: &str,
        source_table: &str,
        output_path: &Path,
        tls: Option<&PostgresTlsConfig>,
    ) -> Result<(Controller, crossbeam::channel::Receiver<String>), crate::ControllerError> {
        if tls.is_some() {
            crate::ensure_default_crypto_provider();
        }

        let url = cdc_connector_url(url);
        let mut postgres_cdc_config = json!({
            "uri": url,
            "publication": publication,
            "source_table": source_table,
        });
        if let Some(tls) = tls {
            let tls_json = serde_json::to_value(tls).unwrap();
            postgres_cdc_config
                .as_object_mut()
                .unwrap()
                .extend(tls_json.as_object().unwrap().clone());
        }

        build_simple_cdc_test_circuit(engine, postgres_cdc_config, output_path)
    }

    /// Build a CDC test circuit from a ready-made `postgres_cdc_input` config.
    ///
    /// Shared by the plain and the de-elevated-role scenarios: the input schema
    /// matches the "simple" table (id, b, i, s) and the output is a JSON file.
    pub(super) fn build_simple_cdc_test_circuit(
        engine: &dyn PipelineTestEngine,
        postgres_cdc_config: serde_json::Value,
        output_path: &Path,
    ) -> Result<(Controller, crossbeam::channel::Receiver<String>), crate::ControllerError> {
        let config = serde_json::from_value(json!({
            "name": "cdc_test",
            "workers": 1,
            "inputs": {
                "cdc_in": {
                    "stream": "test_input1",
                    "transport": {
                        "name": "postgres_cdc_input",
                        "config": postgres_cdc_config,
                    },
                },
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path,
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "insert_delete",
                            "array": false,
                        }
                    }
                }
            }
        }))
        .unwrap();

        let (err_sender, err_receiver) = crossbeam::channel::unbounded();
        let controller = engine.controller(
            &TestCircuit::identity(cdc_relation()),
            &config,
            Box::new(move |e, _| {
                let msg = format!("cdc_test: error: {e}");
                println!("{msg}");

                let _ = err_sender.send(msg);
            }),
        )?;

        Ok((controller, err_receiver))
    }

    /// The table that most cases read into: the columns of [`relations::basic`], which the
    /// source tables of the cases have.
    pub fn cdc_relation() -> Relation {
        relations::basic("postgres_cdc")
    }

    /// The table of the all-types cases: a column for each Postgres type that the connector reads.
    pub fn cdc_all_types_relation() -> Relation {
        Relation::new(
            "postgres_cdc_all_types".into(),
            vec![
                Field::new("id".into(), ColumnType::int(false)),
                Field::new("col_text".into(), ColumnType::varchar(true)),
                Field::new("col_integer".into(), ColumnType::int(true)),
                Field::new("col_bigint".into(), ColumnType::bigint(true)),
                Field::new("col_boolean".into(), ColumnType::boolean(true)),
                Field::new("col_real".into(), ColumnType::real(true)),
                Field::new("col_double".into(), ColumnType::double(true)),
                Field::new("col_date".into(), ColumnType::varchar(true)),
                Field::new("col_time".into(), ColumnType::varchar(true)),
                // Feldera has no TIME WITH TIME ZONE, so TIMETZ arrives as
                // Postgres text with the offset.
                Field::new("col_timetz".into(), ColumnType::varchar(true)),
                Field::new(
                    "col_timetz_array".into(),
                    ColumnType::array(true, ColumnType::varchar(true)),
                ),
                Field::new("col_timestamp".into(), ColumnType::varchar(true)),
                Field::new("col_timestamptz".into(), ColumnType::varchar(true)),
                Field::new("col_uuid".into(), ColumnType::varchar(true)),
                Field::new("col_jsonb".into(), ColumnType::variant(true)),
                Field::new("col_bytea".into(), ColumnType::varbinary(true)),
                Field::new("col_numeric".into(), ColumnType::varchar(true)),
                Field::new("col_smallint".into(), ColumnType::smallint(true)),
                Field::new(
                    "col_int_array".into(),
                    ColumnType::array(true, ColumnType::int(true)),
                ),
            ],
            false,
            BTreeMap::new(),
        )
    }

    fn cdc_all_types_test_circuit(
        engine: &dyn PipelineTestEngine,
        url: &str,
        publication: &str,
        source_table: &str,
        output_path: &Path,
    ) -> (Controller, crossbeam::channel::Receiver<String>) {
        let url = cdc_connector_url(url);
        let config = serde_json::from_value(json!({
            "name": "cdc_all_types_test",
            "workers": 1,
            "inputs": {
                "cdc_in": {
                    "stream": "test_input1",
                    "transport": {
                        "name": "postgres_cdc_input",
                        "config": {
                            "uri": url,
                            "publication": publication,
                            "source_table": source_table,
                        },
                    },
                },
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": {
                            "path": output_path,
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "insert_delete",
                            "array": false,
                        }
                    }
                }
            }
        }))
        .unwrap();

        let (err_sender, err_receiver) = crossbeam::channel::unbounded();
        let controller = engine
            .controller(
                &TestCircuit::identity(cdc_all_types_relation()),
                &config,
                Box::new(move |e, _| {
                    let msg = format!("cdc_all_types_test: error: {e}");
                    println!("{msg}");
                    let _ = err_sender.send(msg);
                }),
            )
            .unwrap();

        (controller, err_receiver)
    }

    /// Like `cdc_simple_test_circuit` but with fault tolerance enabled,
    /// file storage at `storage_dir`, and a 3600-second checkpoint interval so
    /// that no automatic checkpoint fires during the test.  With fault
    /// tolerance enabled the connector uses strict mode: the replication slot
    /// only advances after a durable checkpoint.
    pub(super) fn cdc_ft_test_circuit(
        engine: &dyn PipelineTestEngine,
        url: &str,
        publication: &str,
        source_table: &str,
        storage_dir: &Path,
        output_path: &Path,
    ) -> (Controller, crossbeam::channel::Receiver<String>) {
        cdc_ft_test_circuit_for(
            engine,
            url,
            publication,
            source_table,
            storage_dir,
            output_path,
            &cdc_relation(),
            CircuitShape::default(),
        )
    }

    /// How a fault-tolerant test circuit runs: how many workers it has, and
    /// how many records its reader takes per step, which a test that needs a
    /// write to reach the circuit over several steps sets to one.
    #[derive(Clone, Copy)]
    pub(super) struct CircuitShape {
        pub workers: usize,
        pub max_batch_size: Option<u64>,
    }

    impl Default for CircuitShape {
        fn default() -> Self {
            Self {
                workers: 1,
                max_batch_size: None,
            }
        }
    }

    /// Fault-tolerant CDC circuit for record type `T` with `schema`, in the
    /// given `shape`. Checkpoints only when the test asks for one (interval
    /// set to one hour).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn cdc_ft_test_circuit_for(
        engine: &dyn PipelineTestEngine,
        url: &str,
        publication: &str,
        source_table: &str,
        storage_dir: &Path,
        output_path: &Path,
        relation: &Relation,
        shape: CircuitShape,
    ) -> (Controller, crossbeam::channel::Receiver<String>) {
        let url = cdc_connector_url(url);
        let config: PipelineConfig = serde_json::from_value(json!({
            "name": "cdc_ft_test",
            "workers": shape.workers,
            "storage_config": { "path": storage_dir },
            "storage": true,
            "fault_tolerance": { "model": "at_least_once", "checkpoint_interval_secs": 3600 },
            "inputs": {
                "cdc_in": {
                    "stream": "test_input1",
                    "max_batch_size": shape.max_batch_size,
                    "transport": {
                        "name": "postgres_cdc_input",
                        "config": {
                            "uri": url,
                            "publication": publication,
                            "source_table": source_table,
                        },
                    },
                },
            },
            "outputs": {
                "test_output1": {
                    "stream": "test_output1",
                    "transport": {
                        "name": "file_output",
                        "config": { "path": output_path },
                    },
                    "format": {
                        "name": "json",
                        "config": { "update_format": "insert_delete", "array": false },
                    },
                },
            },
        }))
        .unwrap();

        let (err_sender, err_receiver) = crossbeam::channel::unbounded();
        let controller = engine
            .controller(
                &TestCircuit::Identity {
                    relation: relation.clone(),
                    persistent_output_ids: vec![Some("output".to_string())],
                    input_properties: Vec::new(),
                    materialized: true,
                    persistent_ids: true,
                    table_version: 0,
                    attribute_defaults: BTreeMap::new(),
                },
                &config,
                Box::new(move |e, _| {
                    let msg = format!("cdc_ft_test: error: {e}");
                    println!("{msg}");
                    let _ = err_sender.send(msg);
                }),
            )
            .unwrap();

        (controller, err_receiver)
    }

    /// etl states in which a table's initial sync is complete: etl keeps the
    /// table's data on a restart and streams changes instead of copying it
    /// again. `sync_done` is the durable catchup handoff; `ready` follows only
    /// once a later streamed write advances etl's replication checkpoint past
    /// the handoff, so a table nobody writes to stays in `sync_done`.
    pub(super) const ETL_SYNC_COMPLETED_STATES: [&str; 2] = ["sync_done", "ready"];

    /// Wait until etl has completed the initial sync of a table of this
    /// connector's pipeline, which is when a stop no longer makes the next
    /// start read the table again. The publications these callers use hold
    /// one table, the source table.
    pub(super) fn wait_for_etl_sync_completed(table: &mut CdcTestTable) {
        wait(
            || {
                etl_table_states(table)
                    .iter()
                    .any(|s| ETL_SYNC_COMPLETED_STATES.contains(&s.as_str()))
            },
            60_000,
        )
        .unwrap_or_else(|_| {
            panic!(
                "timeout waiting for etl to complete the table sync (one of \
             {ETL_SYNC_COMPLETED_STATES:?}); current states: {:?}",
                etl_table_states(table)
            )
        });
    }

    /// Block until part of the initial copy of `table` has reached the
    /// circuit, and report how much of it has if etl is still copying.
    ///
    /// `copied_so_far` returns the number of copied records the circuit has
    /// taken from the connector; `row_count` is the size of the table.
    ///
    /// etl writes `init` for every published table at pipeline startup, before
    /// any table-sync worker runs, so `init` says nothing about a running
    /// copy. It writes `data_sync` immediately before it creates the copy
    /// slot, so a `data_sync` state that has already delivered records places
    /// the caller inside the copy, with the copy slot created.
    ///
    /// Returns `None` when the copy outran the measurement: etl had left
    /// `data_sync`, or every row was already in the circuit. How fast the copy
    /// runs is a property of the runner, not of the connector, so the caller
    /// retries with a larger table instead of failing.
    pub(super) fn wait_for_etl_copy_in_progress(
        table: &mut CdcTestTable,
        copied_so_far: impl Fn() -> u64,
        row_count: u64,
        what: &str,
    ) -> Option<u64> {
        // Stop on any state past `init`, so a copy that etl ran to completion
        // between two polls reports `None` instead of waiting out the timeout.
        wait(
            || etl_table_states(table).iter().any(|s| s != "init"),
            60_000,
        )
        .unwrap_or_else(|_| {
            panic!(
                "timeout waiting for etl to start the copy for {what}; etl states: {:?}",
                etl_table_states(table)
            )
        });
        let started = std::time::Instant::now();
        wait(|| copied_so_far() > 0, 60_000)
            .unwrap_or_else(|_| panic!("timeout waiting for the first copied rows of {what}"));
        // Measure before re-reading the state, so the state decides whether
        // the measurement describes a copy that was still running.
        let copied = copied_so_far();
        let states = etl_table_states(table);
        // The margin the callers' row counts rest on is visible only here.
        println!(
            "{what}: {copied} copied records in the circuit {:?} after etl entered the copy, \
         etl states: {states:?}",
            started.elapsed()
        );
        let mid_copy = states.iter().any(|s| s == "data_sync") && copied < row_count;
        mid_copy.then_some(copied)
    }

    /// Wait until every replication slot of `table`'s connector is inactive,
    /// which is when a restarted connector can reattach to them. A fixed delay
    /// here is a timing race: the walsender releases the slot some time after
    /// the controller has stopped.
    pub(super) fn wait_for_slots_released(table: &mut CdcTestTable) {
        let pipeline_id = crate::integrated::postgres::cdc_input::pipeline_id(
            &cdc_connector_url(&table.url),
            &table.publication_name,
            &format!("public.{}", table.table_name),
        )
        .to_string();
        let active_slots = |table: &mut CdcTestTable| -> Vec<String> {
            table
                .client
                .query(
                    "SELECT slot_name::text FROM pg_replication_slots WHERE active",
                    &[],
                )
                .unwrap_or_else(|e| panic!("querying pg_replication_slots failed: {e}"))
                .iter()
                .map(|r| r.get::<_, String>(0))
                .filter(|name| name.split('_').any(|token| token == pipeline_id))
                .collect()
        };
        wait(|| active_slots(table).is_empty(), 60_000).unwrap_or_else(|_| {
            panic!(
                "timeout: replication slots still active after stop: {:?}",
                active_slots(table)
            )
        });
    }

    /// Current `state` of every table etl tracks for this connector's pipeline.
    ///
    /// Reuses the table's client: `wait_for_etl_sync_completed` polls this every 10 ms.
    /// A failed query panics rather than reading as "no tables", which would let
    /// the `errored`-state assertions pass vacuously.
    pub(super) fn etl_table_states(table: &mut CdcTestTable) -> Vec<String> {
        let pipeline_id = crate::integrated::postgres::cdc_input::pipeline_id(
            &cdc_connector_url(&table.url),
            &table.publication_name,
            &format!("public.{}", table.table_name),
        ) as i64;
        table
            .client
            .query(
                "SELECT state::text FROM etl.replication_state WHERE pipeline_id = $1 AND is_current",
                &[&pipeline_id],
            )
            .unwrap_or_else(|e| panic!("querying etl.replication_state failed: {e}"))
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// Helper: read output file lines as JSON values.
    pub(super) fn read_output_json(path: &Path) -> Vec<serde_json::Value> {
        let content = std::fs::read_to_string(path).unwrap_or_default();
        content
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// Helper: count the number of "insert" entries in the output.
    fn count_inserts(rows: &[serde_json::Value]) -> usize {
        rows.iter().filter(|r| r.get("insert").is_some()).count()
    }

    /// Helper: count the number of "delete" entries in the output.
    fn count_deletes(rows: &[serde_json::Value]) -> usize {
        rows.iter().filter(|r| r.get("delete").is_some()).count()
    }

    fn has_insert_id(rows: &[serde_json::Value], id: i64) -> bool {
        rows.iter().any(|r| {
            r.get("insert")
                .and_then(|v| v.get("id"))
                .and_then(|v| v.as_i64())
                == Some(id)
        })
    }

    /// Tests that the CDC connector picks up rows inserted into a Postgres table
    /// via the initial snapshot and/or logical replication stream.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege.
    #[engine_test(serial, ignore, services(Postgres))]
    pub fn basic_insert(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_basic_insert");
        let publication = unique_pg_name("cdc_pub_basic_insert");

        // Pre-insert some rows before starting the pipeline (tests snapshot).
        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, NULL, 'hello')"
        ));
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (2, false, 42, 'world')"
        ));

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
        );

        controller.start();

        // Wait for the snapshot data to appear in the output.
        wait(
            || {
                let rows = read_output_json(&output_path);
                count_inserts(&rows) >= 2 || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC basic insert test did not receive snapshot rows");

        assert!(err_receiver.is_empty(), "unexpected errors in CDC pipeline");

        // Now insert more rows while the pipeline is running (tests replication).
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (3, true, 100, 'streaming')"
        ));

        wait(
            || {
                let rows = read_output_json(&output_path);
                count_inserts(&rows) >= 3 || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC basic insert test did not receive streamed row");

        let rows = read_output_json(&output_path);
        assert!(count_inserts(&rows) >= 3);

        // Verify the content of the rows
        let inserts: Vec<&serde_json::Value> =
            rows.iter().filter_map(|r| r.get("insert")).collect();

        // Check that we have rows with ids 1, 2, 3
        let ids: Vec<i64> = inserts
            .iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_i64()))
            .collect();
        assert!(ids.contains(&1), "missing row with id=1");
        assert!(ids.contains(&2), "missing row with id=2");
        assert!(ids.contains(&3), "missing row with id=3");

        controller.stop().unwrap();
    }

    /// Tests CDC replication of all common Postgres data types.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege.
    #[engine_test(serial, ignore, services(Postgres))]
    pub fn all_data_types(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_all_types");
        let publication = unique_pg_name("cdc_pub_all_types");

        let mut table = CdcTestTable::new_all_types(&table_name, &publication, &url);

        // Insert a row with all types populated.
        table.execute(&format!(
            r#"INSERT INTO {table_name} (
            id, col_text, col_integer, col_bigint, col_boolean,
            col_real, col_double, col_date, col_time, col_timetz, col_timetz_array,
            col_timestamp, col_timestamptz, col_uuid, col_jsonb,
            col_bytea, col_numeric, col_smallint, col_int_array
        ) VALUES (
            1, 'hello world', 42, 9876543210, true,
            3.5, 2.25, '2024-06-15', '14:30:00',
            '14:30:00+02', ARRAY['08:30:00-05'::timetz, NULL],
            '2024-01-01 12:00:00', '2024-01-01 12:00:00+00', '550e8400-e29b-41d4-a716-446655440000',
            '{{"key": "value", "nested": {{"a": 1}}}}',
            E'\\xDEADBEEF', 12345.67, 7, ARRAY[1, 2, 3]
        )"#
        ));

        // Insert a row with NULLs for nullable columns.
        table.execute(&format!(r#"INSERT INTO {table_name} (id) VALUES (2)"#));

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_all_types_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
        );

        controller.start();

        // Wait for both rows to appear.
        wait(
            || {
                let rows = read_output_json(&output_path);
                count_inserts(&rows) >= 2 || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC all-types test did not receive rows");

        assert!(
            err_receiver.is_empty(),
            "unexpected errors in CDC all-types pipeline"
        );

        let rows = read_output_json(&output_path);
        let inserts: Vec<&serde_json::Value> =
            rows.iter().filter_map(|r| r.get("insert")).collect();

        // Find the fully-populated row (id=1).
        let row1 = inserts
            .iter()
            .find(|r| r.get("id").and_then(|v| v.as_i64()) == Some(1))
            .expect("missing row with id=1");

        // Verify types are present and correctly encoded.
        assert_eq!(row1["col_text"], json!("hello world"));
        assert_eq!(row1["col_integer"], json!(42));
        assert_eq!(row1["col_bigint"], json!(9876543210i64));
        assert_eq!(row1["col_boolean"], json!(true));
        // REAL travels as f32, whose spacing at 3.5 is 2^-22 (about 2.4e-7),
        // so 1e-6 forgives a few rounding steps and nothing more. The same
        // tolerance guards this column in test_all_types_replayed_after_restart.
        assert!((row1["col_real"].as_f64().unwrap() - 3.5).abs() < 1e-6);
        assert!((row1["col_double"].as_f64().unwrap() - 2.25).abs() < 1e-9);
        // Date, time, timestamp are encoded as strings
        assert!(
            row1["col_date"].as_str().unwrap().contains("2024-06-15"),
            "col_date mismatch: {:?}",
            row1["col_date"]
        );
        assert!(
            row1["col_time"].as_str().unwrap().contains("14:30"),
            "col_time mismatch: {:?}",
            row1["col_time"]
        );
        // TIMETZ keeps the offset it was inserted with: Postgres stores the
        // offset with the time and never normalizes it to the session zone.
        assert_eq!(row1["col_timetz"], json!("14:30:00+02"));
        assert_eq!(row1["col_timetz_array"], json!(["08:30:00-05", null]));
        assert!(
            row1["col_timestamp"]
                .as_str()
                .unwrap()
                .contains("2024-01-01"),
            "col_timestamp mismatch: {:?}",
            row1["col_timestamp"]
        );
        assert!(
            row1["col_timestamptz"].as_str().unwrap().contains("2024"),
            "col_timestamptz mismatch: {:?}",
            row1["col_timestamptz"]
        );
        // UUID
        assert_eq!(
            row1["col_uuid"].as_str().unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        // JSONB - should be passed through as JSON
        assert!(row1["col_jsonb"].is_object() || row1["col_jsonb"].is_string());
        // BYTEA is encoded as a JSON byte array.
        assert_eq!(row1["col_bytea"], json!([0xde, 0xad, 0xbe, 0xef]));
        // NUMERIC - encoded as string to preserve precision
        assert!(
            row1["col_numeric"].as_str().is_some(),
            "col_numeric should be a string: {:?}",
            row1["col_numeric"]
        );
        assert_eq!(row1["col_smallint"], json!(7));
        // Integer array
        assert!(row1["col_int_array"].is_array());
        let arr = row1["col_int_array"].as_array().unwrap();
        assert_eq!(arr.len(), 3);

        // Find the NULL row (id=2).
        let row2 = inserts
            .iter()
            .find(|r| r.get("id").and_then(|v| v.as_i64()) == Some(2))
            .expect("missing row with id=2");

        assert!(row2["col_text"].is_null());
        assert!(row2["col_integer"].is_null());
        assert!(row2["col_bigint"].is_null());
        assert!(row2["col_boolean"].is_null());
        assert!(row2["col_timetz"].is_null());
        assert!(row2["col_timetz_array"].is_null());
        assert!(row2["col_uuid"].is_null());
        assert!(row2["col_jsonb"].is_null());
        assert!(row2["col_bytea"].is_null());

        controller.stop().unwrap();
    }

    /// Tests that the CDC connector correctly captures UPDATE and DELETE operations.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege, REPLICA IDENTITY FULL.
    #[engine_test(serial, ignore, services(Postgres))]
    pub fn update_delete(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_upd_del");
        let publication = unique_pg_name("cdc_pub_upd_del");

        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
        );

        controller.start();

        // Give the pipeline a moment to start and establish the replication connection.
        std::thread::sleep(std::time::Duration::from_secs(2));

        // Phase 1: Insert rows.
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, 10, 'alpha')"
        ));
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (2, false, 20, 'beta')"
        ));

        wait(
            || {
                let rows = read_output_json(&output_path);
                count_inserts(&rows) >= 2 || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC update/delete test did not receive initial inserts");

        // Phase 2: Update a row.
        table.execute(&format!(
            "UPDATE {table_name} SET s = 'alpha_updated', i = 11 WHERE id = 1"
        ));

        // An UPDATE with REPLICA IDENTITY FULL should produce a delete of the old
        // row and an insert of the new row in Feldera's insert/delete model.
        wait(
            || {
                let rows = read_output_json(&output_path);
                // We should see at least one delete (the old row) and one more insert
                // (the new row) beyond the initial 2 inserts.
                let ins = count_inserts(&rows);
                let dels = count_deletes(&rows);
                (ins >= 3 && dels >= 1) || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC update/delete test did not receive update events");

        // Phase 3: Delete a row.
        table.execute(&format!("DELETE FROM {table_name} WHERE id = 2"));

        wait(
            || {
                let rows = read_output_json(&output_path);
                let dels = count_deletes(&rows);
                dels >= 2 || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC update/delete test did not receive delete event");

        assert!(
            err_receiver.is_empty(),
            "unexpected errors in CDC update/delete pipeline"
        );

        let rows = read_output_json(&output_path);

        // Verify the delete for id=2 is present.
        let deletes: Vec<&serde_json::Value> =
            rows.iter().filter_map(|r| r.get("delete")).collect();
        let deleted_ids: Vec<i64> = deletes
            .iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_i64()))
            .collect();
        assert!(
            deleted_ids.contains(&2),
            "missing delete for id=2, got deletes: {:?}",
            deleted_ids
        );

        // Verify the updated row (id=1, s='alpha_updated') is present in inserts.
        let inserts: Vec<&serde_json::Value> =
            rows.iter().filter_map(|r| r.get("insert")).collect();
        let updated = inserts.iter().find(|r| {
            r.get("id").and_then(|v| v.as_i64()) == Some(1)
                && r.get("s").and_then(|v| v.as_str()) == Some("alpha_updated")
        });
        assert!(
            updated.is_some(),
            "missing updated row (id=1, s='alpha_updated') in inserts"
        );

        controller.stop().unwrap();
    }

    /// Tests that a paused CDC input endpoint stops delivering new rows and
    /// resumes delivery after the endpoint is started again.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege.
    #[engine_test(serial, ignore, services(Postgres))]
    pub fn pause_unpause(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_pause_unpause");
        let publication = unique_pg_name("cdc_pub_pause_unpause");

        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
        );

        controller.start();

        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, 10, 'before_pause')"
        ));

        wait(
            || has_insert_id(&read_output_json(&output_path), 1) || !err_receiver.is_empty(),
            60_000,
        )
        .expect("timeout: CDC pause/unpause test did not receive initial row");
        assert!(
            err_receiver.is_empty(),
            "unexpected error before pause: {:?}",
            err_receiver.try_recv()
        );

        controller.pause_input_endpoint("cdc_in").unwrap();
        wait(
            || {
                controller
                    .is_input_endpoint_paused("cdc_in")
                    .unwrap_or(false)
            },
            10_000,
        )
        .expect("timeout: CDC input endpoint did not report paused");

        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (2, false, 20, 'while_paused')"
        ));

        std::thread::sleep(std::time::Duration::from_secs(2));
        let rows = read_output_json(&output_path);
        assert!(
            !has_insert_id(&rows, 2),
            "paused CDC input delivered row id=2 before unpause: {rows:?}"
        );
        assert!(
            err_receiver.is_empty(),
            "unexpected error while paused: {:?}",
            err_receiver.try_recv()
        );

        controller.start_input_endpoint("cdc_in").unwrap();
        wait(
            || {
                !controller
                    .is_input_endpoint_paused("cdc_in")
                    .unwrap_or(true)
            },
            10_000,
        )
        .expect("timeout: CDC input endpoint did not report unpaused");

        wait(
            || has_insert_id(&read_output_json(&output_path), 2) || !err_receiver.is_empty(),
            60_000,
        )
        .expect("timeout: CDC pause/unpause test did not receive row after unpause");
        assert!(
            err_receiver.is_empty(),
            "unexpected error after unpause: {:?}",
            err_receiver.try_recv()
        );

        controller.stop().unwrap();
    }

    /// Same as `test_cdc_basic_insert` but over a TLS connection.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege, and
    /// `POSTGRES_SSL_URL`/`POSTGRES_SSL_CA_LOCATION` or `POSTGRES_SSL_CA_PEM`.
    #[engine_test(serial, ignore, services(PostgresTls))]
    pub fn basic_insert_tls(engine: &dyn PipelineTestEngine) {
        let (url, tls) = postgres_cdc_ssl_config();
        let table_name = unique_pg_name("cdc_test_basic_insert_tls");
        let publication = unique_pg_name("cdc_pub_basic_insert_tls");

        let mut table =
            CdcTestTable::new_simple_with_tls(&table_name, &publication, &url, Some(&tls));

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_simple_test_circuit_with_tls(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
            Some(&tls),
        );

        controller.start();

        std::thread::sleep(std::time::Duration::from_secs(2));

        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, 100, 'streaming')"
        ));

        wait(
            || {
                let rows = read_output_json(&output_path);
                count_inserts(&rows) >= 1 || !err_receiver.is_empty()
            },
            60_000,
        )
        .expect("timeout: CDC TLS basic insert test did not receive streamed row");

        assert!(
            err_receiver.is_empty(),
            "unexpected errors in CDC TLS pipeline"
        );

        let rows = read_output_json(&output_path);
        let inserts: Vec<&serde_json::Value> =
            rows.iter().filter_map(|r| r.get("insert")).collect();
        assert!(
            inserts.iter().any(|r| {
                r.get("id").and_then(|v| v.as_i64()) == Some(1)
                    && r.get("s").and_then(|v| v.as_str()) == Some("streaming")
            }),
            "missing streamed TLS row in inserts: {inserts:?}"
        );

        controller.stop().unwrap();
    }

    /// Tests that compatible source schema changes continue to replicate:
    /// adding an extra Postgres column and dropping `i`, which is nullable in
    /// the Feldera schema.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege.
    #[engine_test(serial, ignore, services(Postgres))]
    pub fn compatible_schema_changes(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_compatible_schema");
        let publication = unique_pg_name("cdc_pub_compatible_schema");

        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
        );

        controller.start();

        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, 10, 'alpha')"
        ));
        wait(
            || has_insert_id(&read_output_json(&output_path), 1) || !err_receiver.is_empty(),
            60_000,
        )
        .expect("timeout: did not receive the initial insert");
        assert!(
            err_receiver.is_empty(),
            "unexpected error before the schema change: {:?}",
            err_receiver.try_recv()
        );

        table.execute(&format!("ALTER TABLE {table_name} ADD COLUMN extra TEXT"));
        table.execute(&format!(
            "INSERT INTO {table_name} (id, b, i, s, extra) \
         VALUES (2, false, 20, 'with_extra', 'ignored')"
        ));

        wait(
            || has_insert_id(&read_output_json(&output_path), 2) || !err_receiver.is_empty(),
            60_000,
        )
        .expect("timeout: did not receive row after adding an extra source column");
        assert!(
            err_receiver.is_empty(),
            "unexpected error after add-column schema change: {:?}",
            err_receiver.try_recv()
        );

        table.execute(&format!("ALTER TABLE {table_name} DROP COLUMN i"));
        table.execute(&format!(
            "INSERT INTO {table_name} (id, b, s, extra) \
         VALUES (3, true, 'without_nullable_i', 'ignored')"
        ));

        wait(
            || has_insert_id(&read_output_json(&output_path), 3) || !err_receiver.is_empty(),
            60_000,
        )
        .expect("timeout: did not receive row after dropping nullable source column");
        assert!(
            err_receiver.is_empty(),
            "unexpected error after dropping nullable column: {:?}",
            err_receiver.try_recv()
        );

        let rows = read_output_json(&output_path);
        assert!(has_insert_id(&rows, 1), "missing pre-schema-change row");
        assert!(
            has_insert_id(&rows, 2),
            "missing row after add-column change"
        );
        assert!(
            has_insert_id(&rows, 3),
            "missing row after drop-column change"
        );

        controller.stop().unwrap();
    }

    /// An unqualified `source_table` means `public`, so a pipeline that named
    /// a table outside `public` by its bare name no longer matches anything.
    /// Such a pipeline must fail at start with the publication's table list,
    /// instead of ingesting nothing until the stall monitor speaks.
    #[engine_test(serial, services(Postgres))]
    pub fn source_table_not_in_publication_fails_at_start(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_not_published");
        let publication = unique_pg_name("cdc_pub_not_published");
        let _table = CdcTestTable::new_simple(&table_name, &publication, &url);
        let output_file = NamedTempFile::new().unwrap();

        // The connector checks the publication while it opens, so the refusal
        // comes out of the controller's constructor.
        let err = match try_cdc_simple_test_circuit_with_tls(
            engine,
            &url,
            &publication,
            "no_such_table",
            output_file.path(),
            None,
        ) {
            Err(err) => err.to_string(),
            Ok((controller, _)) => {
                let _ = controller.stop();
                panic!("a source table outside the publication started a pipeline");
            }
        };
        assert!(
            err.contains("'no_such_table' is not in publication")
                && err.contains(&format!("public.{table_name}")),
            "unexpected error: {err}"
        );
    }

    /// Tests that dropping the source primary-key column (`id`), which is a
    /// required Feldera column, is surfaced as a fatal connector error.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege.
    #[engine_test(serial, ignore, services(Postgres))]
    pub fn drop_primary_key_column_rejected(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_drop_pk_col");
        let publication = unique_pg_name("cdc_pub_drop_pk_col");

        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);

        let output_file = NamedTempFile::new().unwrap();
        let output_path = output_file.path().to_owned();

        let (controller, err_receiver) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path,
        );

        controller.start();

        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, 10, 'alpha')"
        ));
        wait(
            || has_insert_id(&read_output_json(&output_path), 1) || !err_receiver.is_empty(),
            60_000,
        )
        .expect("timeout: did not receive the initial insert");
        assert!(
            err_receiver.is_empty(),
            "unexpected error before dropping primary key column: {:?}",
            err_receiver.try_recv()
        );

        table.execute(&format!("ALTER TABLE {table_name} DROP COLUMN id CASCADE"));
        table.execute(&format!(
            "INSERT INTO {table_name} (b, i, s) VALUES (false, 20, 'missing_id')"
        ));

        wait(|| !err_receiver.is_empty(), 60_000)
            .expect("timeout: dropped primary-key column was not reported as an error");

        let err = err_receiver.try_recv().unwrap_or_default();
        assert!(
            err.to_lowercase().contains("missing required")
                || err.to_lowercase().contains("missing")
                || err.to_lowercase().contains("schema")
                || err.to_lowercase().contains("replication error"),
            "error should report the missing required primary-key column, got: {err}"
        );

        let _ = controller.stop();
    }

    /// Stop the pipeline right after a row streamed in behind the initial
    /// copy, and check that the replication slots are released at once.
    ///
    /// etl buffers streamed rows for up to its batch fill time before it hands
    /// them over. Until etl discarded such a buffer at shutdown, the worker
    /// holding it waited the fill time out with its slot active, and under CI
    /// load the wait grew past the release timeout (#7170, #7201). Whether a
    /// stop lands on a buffered row is a matter of timing, so the test stops
    /// several pipelines. Every release must stay within the connector's
    /// bound, and no stop may reach the grace that closes etl's connections
    /// by force: that fallback releases the slots too, but late, and a
    /// release is prompt only when etl stopped on its own.
    #[engine_test(serial, services(Postgres))]
    pub fn stop_with_buffered_rows_releases_slots_promptly(engine: &dyn PipelineTestEngine) {
        use crate::integrated::postgres::cdc_input::{
            ETL_STOP_GRACE, RUNTIME_SHUTDOWN_TIMEOUT, STOPPED_PAST_GRACE, WATCHER_SHUTDOWN_TIMEOUT,
            pipeline_id,
        };

        // The connector's bound on a stop, with room for PostgreSQL to notice
        // a closed connection.
        let release_bound = WATCHER_SHUTDOWN_TIMEOUT
            + ETL_STOP_GRACE
            + RUNTIME_SHUTDOWN_TIMEOUT
            + Duration::from_secs(2);

        let url = postgres_url();
        for _ in 0..10 {
            let table_name = unique_pg_name("cdc_test_prompt_release");
            let publication = unique_pg_name("cdc_pub_prompt_release");
            let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);
            table.execute(&format!(
                "INSERT INTO {table_name} VALUES (1, true, NULL, 'first')"
            ));
            let output_file = NamedTempFile::new().unwrap();
            let output_path = output_file.path().to_owned();
            let (controller, err_receiver) = cdc_simple_test_circuit(
                engine,
                &url,
                &publication,
                &format!("public.{table_name}"),
                &output_path,
            );
            controller.start();
            wait(
                || count_inserts(&read_output_json(&output_path)) >= 1 || !err_receiver.is_empty(),
                60_000,
            )
            .expect("timeout: the copied row did not arrive");
            // Commit a row as the copy lands, so that it streams in while etl
            // is still handing the table over between its workers.
            table.execute(&format!(
                "INSERT INTO {table_name} VALUES (2, false, 42, 'second')"
            ));
            wait(
                || count_inserts(&read_output_json(&output_path)) >= 2 || !err_receiver.is_empty(),
                60_000,
            )
            .expect("timeout: the streamed row did not arrive");
            assert!(err_receiver.is_empty(), "unexpected errors before stop");

            controller.stop().unwrap();
            let stopped = Instant::now();
            wait_for_slots_released(&mut table);
            let release = stopped.elapsed();
            assert!(
                release < release_bound,
                "replication slots released {release:?} after stop"
            );
            let pipeline = pipeline_id(
                &cdc_connector_url(&url),
                &publication,
                &format!("public.{table_name}"),
            );
            assert!(
                !STOPPED_PAST_GRACE.lock().unwrap().contains(&pipeline),
                "the stop ran past the grace: etl held its slots for more than {ETL_STOP_GRACE:?} \
             and the connector closed its connections by force"
            );
        }
    }

    /// Shutting the connector's runtime down closes the PostgreSQL connections
    /// it hosts. That is what releases a replication slot when an etl worker
    /// outlives the stop grace, so a backend held by a hosted connection must
    /// be gone once the runtime is.
    #[test]
    #[serial]
    fn test_cdc_runtime_shutdown_closes_hosted_connections() {
        let url = postgres_url();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let backend_pid: i32 = runtime.block_on(async {
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("failed to connect");
            tokio::spawn(connection);
            let pid = client
                .query_one("SELECT pg_backend_pid()", &[])
                .await
                .unwrap()
                .get(0);
            // A query that does not return stands in for an etl worker that
            // does not stop.
            tokio::spawn(async move {
                let _ = client.execute("SELECT pg_sleep(600)", &[]).await;
            });
            // Let the hosted task send its query before the runtime goes.
            tokio::task::yield_now().await;
            pid
        });
        runtime.shutdown_timeout(crate::integrated::postgres::cdc_input::RUNTIME_SHUTDOWN_TIMEOUT);

        let mut observer = pg_connect(&url, &None);
        wait(
            || {
                observer
                    .query(
                        "SELECT 1 FROM pg_stat_activity WHERE pid = $1",
                        &[&backend_pid],
                    )
                    .unwrap()
                    .is_empty()
            },
            5_000,
        )
        .expect("the backend of a hosted connection outlived the runtime");
    }

    #[engine_test(serial, services(Postgres))]
    pub fn restart_resumes_from_slot(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_restart");
        let publication = unique_pg_name("cdc_pub_restart");

        // Insert initial rows.
        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, NULL, 'first')"
        ));

        // --- First pipeline run ---
        let output_file_1 = NamedTempFile::new().unwrap();
        let output_path_1 = output_file_1.path().to_owned();

        let (controller_1, err_receiver_1) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path_1,
        );

        controller_1.start();

        // Wait for the snapshot row to appear.
        wait(
            || {
                let rows = read_output_json(&output_path_1);
                count_inserts(&rows) >= 1 || !err_receiver_1.is_empty()
            },
            60_000,
        )
        .expect("timeout: first run did not receive snapshot row");

        assert!(
            err_receiver_1.is_empty(),
            "unexpected errors in first pipeline run"
        );

        // Insert a second row while the pipeline is running.
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (2, false, 42, 'second')"
        ));

        wait(
            || {
                let rows = read_output_json(&output_path_1);
                count_inserts(&rows) >= 2 || !err_receiver_1.is_empty()
            },
            60_000,
        )
        .expect("timeout: first run did not receive streamed row");

        // Stop the first pipeline.
        controller_1.stop().unwrap();
        wait_for_slots_released(&mut table);

        // --- Second pipeline run (restart) ---
        let output_file_2 = NamedTempFile::new().unwrap();
        let output_path_2 = output_file_2.path().to_owned();

        let (controller_2, err_receiver_2) = cdc_simple_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            &output_path_2,
        );

        controller_2.start();

        // Insert a third row in the restarted pipeline.
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (3, true, 99, 'third')"
        ));

        // Wait for the third row to appear.
        wait(
            || {
                let rows = read_output_json(&output_path_2);
                count_inserts(&rows) >= 1 || !err_receiver_2.is_empty()
            },
            60_000,
        )
        .expect("timeout: second run did not receive any rows");

        assert!(
            err_receiver_2.is_empty(),
            "unexpected errors in second pipeline run"
        );

        let rows = read_output_json(&output_path_2);
        let inserts: Vec<&serde_json::Value> =
            rows.iter().filter_map(|r| r.get("insert")).collect();

        let ids: Vec<i64> = inserts
            .iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_i64()))
            .collect();

        // The third row (id=3) must be present — it was inserted after restart.
        assert!(
            ids.contains(&3),
            "missing row with id=3 after restart, got ids: {ids:?}"
        );

        // Row id=1 should NOT be re-snapshotted in the second run's output.
        // If PostgresStore is working correctly, etl resumes from the slot
        // position and skips the snapshot phase.
        //
        // Note: some replay of recent rows (id=2) is acceptable under
        // at-least-once semantics, but a full re-snapshot (id=1 appearing)
        // indicates the stored state was not preserved.
        assert!(
            !ids.contains(&1),
            "row id=1 was re-snapshotted after restart — PostgresStore state \
         was not preserved. Got ids: {ids:?}"
        );

        controller_2.stop().unwrap();
    }

    /// With fault tolerance enabled, a pipeline stopped before any checkpoint
    /// must deliver the initial snapshot again when restarted on fresh
    /// storage, which holds none of the rows. Rows 1 and 2 are snapshot rows
    /// that never came through the replication slot, so holding the slot
    /// alone cannot bring them back; scenario 3 in `cdc_scenarios` covers
    /// slot replay.
    ///
    /// Requires: wal_level=logical, user with REPLICATION privilege.
    #[engine_test(serial, requires(Storage, FaultTolerance), services(Postgres))]
    pub fn ft_mode_holds_slot(engine: &dyn PipelineTestEngine) {
        let url = postgres_url();
        let table_name = unique_pg_name("cdc_test_strict_hold");
        let publication = unique_pg_name("cdc_pub_strict_hold");

        let mut table = CdcTestTable::new_simple(&table_name, &publication, &url);
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (1, true, NULL, 'first')"
        ));
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (2, false, 42, 'second')"
        ));

        // --- Run 1: FT mode (strict), no checkpoint fires (interval = 3600 s) ---
        let storage = tempfile::tempdir().unwrap();
        let out_1 = NamedTempFile::new().unwrap();
        let (ctrl_1, errs_1) = cdc_ft_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            storage.path(),
            out_1.path(),
        );
        ctrl_1.start();

        wait(
            || count_inserts(&read_output_json(out_1.path())) >= 2 || !errs_1.is_empty(),
            60_000,
        )
        .expect("timeout: run 1 did not receive snapshot rows");
        assert!(
            errs_1.is_empty(),
            "unexpected errors in run 1: {:?}",
            errs_1.try_recv()
        );

        // Stop without a checkpoint, but only once etl has persisted the copy
        // as complete. Stopping earlier leaves etl in `data_sync` or
        // `finished_copy`, and it redoes the copy on restart, which would
        // hide a lost snapshot.
        wait_for_etl_sync_completed(&mut table);
        ctrl_1.stop().unwrap();
        wait_for_slots_released(&mut table);

        // --- Run 2: fresh output, same slot (same url/publication/source_table) ---
        let out_2 = NamedTempFile::new().unwrap();
        let storage_2 = tempfile::tempdir().unwrap();
        let (ctrl_2, errs_2) = cdc_ft_test_circuit(
            engine,
            &url,
            &publication,
            &format!("public.{table_name}"),
            storage_2.path(),
            out_2.path(),
        );
        ctrl_2.start();

        // Insert a new row to confirm replication is flowing.
        table.execute(&format!(
            "INSERT INTO {table_name} VALUES (3, true, 99, 'third')"
        ));

        wait(
            || count_inserts(&read_output_json(out_2.path())) >= 3 || !errs_2.is_empty(),
            60_000,
        )
        .expect("timeout: run 2 did not receive expected rows");
        assert!(
            errs_2.is_empty(),
            "unexpected errors in run 2: {:?}",
            errs_2.try_recv()
        );

        let rows = read_output_json(out_2.path());
        let ids: Vec<i64> = rows
            .iter()
            .filter_map(|r| {
                r.get("insert")
                    .and_then(|v| v.get("id"))
                    .and_then(|v| v.as_i64())
            })
            .collect();

        // Run 1 never checkpointed, so run 2 must deliver the snapshot again.
        assert!(
            ids.contains(&1),
            "row 1 must be redelivered (no checkpoint); ids={ids:?}"
        );
        assert!(
            ids.contains(&2),
            "row 2 must be redelivered (no checkpoint); ids={ids:?}"
        );
        assert!(ids.contains(&3), "row 3 (new) must appear; ids={ids:?}");

        ctrl_2.stop().unwrap();
    }
}

#[cfg(any(test, feature = "testing"))]
pub mod pg {

    use feldera_types::{config::PipelineConfig, transport::postgres::PostgresTlsConfig};
    use postgres::{NoTls, Row};

    use crate::{
        Controller,
        integrated::postgres::{test::indexed_view, tls::make_tls_connector},
        test::pipeline::PipelineTestEngine,
    };

    /// The composite type `test_struct` that the tables of the cases use for their struct columns.
    #[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
    #[postgres(name = "test_struct")]
    pub(crate) struct TempTestStruct {
        pub(crate) id: Option<i32>,
        pub(crate) i: Option<i64>,
        pub(crate) b: Option<bool>,
        pub(crate) s: Option<String>,
    }

    pub(super) struct TempPgTable {
        pub(crate) client: postgres::Client,
        pub(crate) name: String,
    }

    /// Connects a sync postgres client, with TLS if config is provided.
    pub(super) fn pg_connect(uri: &str, tls: &Option<PostgresTlsConfig>) -> postgres::Client {
        if let Some(tls) = tls {
            // The binary installs the process-level provider at startup;
            // tests must do it themselves.
            let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            let connector = make_tls_connector(tls, "test")
                .expect("failed to build TLS connector")
                .expect("TLS config should produce a connector");
            postgres::Client::connect(uri, connector).expect("failed to connect with TLS")
        } else {
            postgres::Client::connect(uri, NoTls).expect("failed to connect")
        }
    }

    impl TempPgTable {
        pub(crate) fn new(
            name: &str,
            uri: String,
            pk: bool,
            tls: &Option<PostgresTlsConfig>,
        ) -> Self {
            Self::new_with_extra_columns(name, uri, pk, tls, "")
        }

        /// Same as `new`, but appends `extra_columns` to the table: column
        /// definitions absent from the Feldera schema, e.g.
        /// `"served_at TIMESTAMP DEFAULT now() NOT NULL"`.
        pub(crate) fn new_with_extra_columns(
            name: &str,
            uri: String,
            pk: bool,
            tls: &Option<PostgresTlsConfig>,
            extra_columns: &str,
        ) -> Self {
            let mut client = pg_connect(&uri, tls);

            let pk = if pk { "PRIMARY KEY" } else { "" };
            let extra_columns = if extra_columns.is_empty() {
                String::new()
            } else {
                format!(",\n    {extra_columns}")
            };

            client
                .execute(
                    r#"DO $$
BEGIN
CREATE TYPE test_struct AS (
    id INTEGER,
    b BOOL,
    i BIGINT,
    s VARCHAR
);
EXCEPTION
WHEN duplicate_object THEN NULL;
END $$"#,
                    &[],
                )
                .expect("failed to create type test_struct");

            client
                .execute(
                    &format!(
                        r#"
CREATE TABLE {name} (
boolean_      BOOLEAN,
tinyint_      SMALLINT,
smallint_     SMALLINT,
int_          INTEGER,
bigint_       BIGINT {pk},
decimal_      DECIMAL,
float_        REAL,
double_       DOUBLE PRECISION,
varchar_      TEXT,
time_         TIME,
date_         DATE,
timestamp_    TIMESTAMP,
variant_      JSONB,
uuid_         UUID,
varbinary_    BYTEA,
string_array_ VARCHAR ARRAY,
struct_       test_struct,
struct_array_ test_struct ARRAY,
map_          JSONB,
__feldera_op  CHAR,
__feldera_ts  BIGINT
{extra_columns}
)"#
                    ),
                    &[],
                )
                .unwrap_or_else(|_| panic!("failed to create table {name}"));

            TempPgTable {
                client,
                name: name.to_owned(),
            }
        }

        pub fn query(&mut self) -> Vec<Row> {
            self.client
                .query(
                    &format!(
                        "SELECT *, CAST(decimal_ AS VARCHAR) AS decimal_str FROM {}",
                        self.name
                    ),
                    &[],
                )
                .expect("failed to query table")
        }
    }

    impl Drop for TempPgTable {
        fn drop(&mut self) {
            self.client
                .execute(&format!("DROP TABLE {}", self.name), &[])
                .unwrap_or_else(|_| panic!("failed to drop table {}", self.name));
        }
    }

    /// Builds the pipeline of [`indexed_view`] with `config`, and returns it with a channel of the
    /// errors that it reports.
    pub(crate) fn test_circuit(
        engine: &dyn PipelineTestEngine,
        config: PipelineConfig,
    ) -> (Controller, crossbeam::channel::Receiver<String>) {
        let (err_sender, err_receiver) = crossbeam::channel::unbounded();
        let controller = engine
            .controller(
                &indexed_view(),
                &config,
                Box::new(move |e, _| {
                    let msg = format!("postgres_output_test: error: {}", e);
                    println!("{msg}");
                    err_sender.send(msg).unwrap()
                }),
            )
            .unwrap();
        (controller, err_receiver)
    }
}
