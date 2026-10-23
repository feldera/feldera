//! The Postgres CDC input connector in pipelines: snapshots, streamed inserts, updates, and
//! deletes, pauses, schema changes, and restarts, with and without fault tolerance.
//!
//! The cases need a Postgres server with `wal_level=logical` at `POSTGRES_URL` and a user with
//! the `REPLICATION` privilege.
//!
//! This module also holds the fixtures that the CDC scenario and privilege tests share with it.

use crate::test::engine_test;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use feldera_adapterlib::test::wait;
use feldera_types::config::PipelineConfig;
use feldera_types::program_schema::{ColumnType, Field, Relation};
use feldera_types::transport::postgres::PostgresTlsConfig;
use serde_json::json;
use tempfile::NamedTempFile;

use super::tests::{pg_connect, postgres_ssl_config, postgres_url, unique_pg_name};
use crate::Controller;
use crate::test::pipeline::{PipelineTestEngine, TestCircuit};
use crate::test::relations;

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
    try_cdc_simple_test_circuit_with_tls(engine, url, publication, source_table, output_path, tls)
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
                attribute_defaults: Vec::new(),
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
    let inserts: Vec<&serde_json::Value> = rows.iter().filter_map(|r| r.get("insert")).collect();

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

    let mut table = CdcTestTable::new_simple_with_tls(&table_name, &publication, &url, Some(&tls));

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
    let inserts: Vec<&serde_json::Value> = rows.iter().filter_map(|r| r.get("insert")).collect();
    assert!(
        inserts.iter().any(|r| {
            r.get("id").and_then(|v| v.as_i64()) == Some(1)
                && r.get("s").and_then(|v| v.as_str()) == Some("streaming")
        }),
        "missing streamed TLS row in inserts: {inserts:?}"
    );

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
    let inserts: Vec<&serde_json::Value> = rows.iter().filter_map(|r| r.get("insert")).collect();

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
    let deletes: Vec<&serde_json::Value> = rows.iter().filter_map(|r| r.get("delete")).collect();
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
    let inserts: Vec<&serde_json::Value> = rows.iter().filter_map(|r| r.get("insert")).collect();
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
    let inserts: Vec<&serde_json::Value> = rows.iter().filter_map(|r| r.get("insert")).collect();

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
