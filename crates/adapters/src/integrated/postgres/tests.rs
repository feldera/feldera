//! The Postgres connectors in pipelines: the output connector writing a view through its index
//! in materialized and cdc mode, with conflicts, column defaults, errors, and reconnects, and
//! the input connector reading a table, with and without TLS.

use crate::test::engine_test;
use std::collections::BTreeMap;
use std::io::Write;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, SubsecRound};
use feldera_adapterlib::test::{ZSet, wait};
use feldera_types::config::PipelineConfig;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::transport::postgres::{PostgresTlsConfig, PostgresWriteMode};
use postgres::{NoTls, Row};
use rand::Rng;
use serde_json::{Map, Value, json};
use tempfile::NamedTempFile;

use super::tls::make_tls_connector;
use crate::Controller;
use crate::controller::MAX_CONNECTOR_ERROR_LEN;
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::{published_schema, read, relations};

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

/// Builds the pipeline of [`indexed_view`] with `config`, and returns it with a channel of the
/// errors that it reports.
fn test_circuit(
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

/// The composite type `test_struct` that the tables of the cases use for their struct columns.
#[derive(Debug, postgres_types::ToSql, postgres_types::FromSql)]
#[postgres(name = "test_struct")]
struct TempTestStruct {
    id: Option<i32>,
    i: Option<i64>,
    b: Option<bool>,
    s: Option<String>,
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

pub(super) struct TempPgTable {
    client: postgres::Client,
    name: String,
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
    fn new(name: &str, uri: String, pk: bool, tls: &Option<PostgresTlsConfig>) -> Self {
        Self::new_with_extra_columns(name, uri, pk, tls, "")
    }

    /// Same as `new`, but appends `extra_columns` to the table: column
    /// definitions absent from the Feldera schema, e.g.
    /// `"served_at TIMESTAMP DEFAULT now() NOT NULL"`.
    fn new_with_extra_columns(
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
