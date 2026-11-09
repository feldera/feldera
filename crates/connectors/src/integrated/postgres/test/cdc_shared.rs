//! Connectors of one publication that share one replication slot
//! (`dev_tweaks.shared_cdc_slot`, #7413).

use crate::{
    Controller, ControllerError,
    integrated::postgres::test::{
        cdc_tests::{
            CdcTestTable, cdc_connector_url, cdc_relation, drop_slots_of_pipeline_id,
            read_output_json,
        },
        postgres_url, unique_pg_name,
    },
    test::{
        engine_test,
        pipeline::{PipelineTestEngine, TestCircuit},
    },
};
use crossbeam::channel::Receiver;
use feldera_adapterlib::test::wait;
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path};
use tempfile::NamedTempFile;

const WAIT_MS: u64 = 60_000;

/// `PipelineConfig::name` of the test pipelines; part of the shared slot's id.
const PIPELINE_NAME: &str = "cdc_shared_test";

/// Two tables in one publication: `a` owns the publication and `b` is added
/// to it. `b` keeps a publication of its own, which nothing reads.
struct TwoTables {
    a: CdcTestTable,
    b: CdcTestTable,
}

impl TwoTables {
    fn new(prefix: &str) -> Self {
        let url = postgres_url();
        let a = CdcTestTable::new_simple(
            &unique_pg_name(&format!("{prefix}_a")),
            &unique_pg_name(&format!("{prefix}_pub")),
            &url,
        );
        let mut b = CdcTestTable::new_simple(
            &unique_pg_name(&format!("{prefix}_b")),
            &unique_pg_name(&format!("{prefix}_pub_b")),
            &url,
        );
        b.execute(&format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            a.publication_name, b.table_name
        ));
        Self { a, b }
    }

    fn publication(&self) -> &str {
        &self.a.publication_name
    }

    /// The id of the slot the two connectors share.
    fn shared_slot_id(&self) -> String {
        crate::integrated::postgres::cdc::shared_pipeline_id(
            &cdc_connector_url(&self.a.url),
            self.publication(),
            PIPELINE_NAME,
        )
        .to_string()
    }

    /// The id of the slot `table` gets without sharing.
    fn per_table_slot_id(&self, table: &CdcTestTable) -> String {
        crate::integrated::postgres::cdc::pipeline_id(
            &cdc_connector_url(&self.a.url),
            self.publication(),
            &format!("public.{}", table.table_name),
        )
        .to_string()
    }

    /// Connector configuration for `table`, with `extra` settings.
    fn connector(&self, table: &CdcTestTable, extra: Value) -> Value {
        let mut config = json!({
            "uri": cdc_connector_url(&self.a.url),
            "publication": self.publication(),
            "source_table": format!("public.{}", table.table_name),
        });
        config
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().cloned().unwrap_or_default());
        config
    }

    /// A pipeline with connectors `cdc_a` and `cdc_b` on the two tables,
    /// writing to `out_a` and `out_b`.
    fn pipeline(&self, shared: bool, b_extra: Value, out_a: &Path, out_b: &Path) -> Value {
        let output = |path: &Path, stream: &str| {
            json!({
                "stream": stream,
                "transport": { "name": "file_output", "config": { "path": path } },
                "format": {
                    "name": "json",
                    "config": { "update_format": "insert_delete", "array": false },
                },
            })
        };
        json!({
            "name": PIPELINE_NAME,
            "workers": 1,
            "dev_tweaks": { "shared_cdc_slot": shared },
            "inputs": {
                "cdc_a": {
                    "stream": "test_input1",
                    "transport": { "name": "postgres_cdc_input", "config": self.connector(&self.a, json!({})) },
                },
                "cdc_b": {
                    "stream": "test_input2",
                    "transport": { "name": "postgres_cdc_input", "config": self.connector(&self.b, b_extra) },
                },
            },
            "outputs": {
                "test_output1": output(out_a, "test_output1"),
                "test_output2": output(out_b, "test_output2"),
            },
        })
    }
}

impl Drop for TwoTables {
    fn drop(&mut self) {
        // The tables drop the slots of their own publications; the slots of
        // the shared publication are this struct's to drop.
        let shared = self.shared_slot_id();
        let b = self.per_table_slot_id(&self.b);
        drop_slots_of_pipeline_id(&mut self.a.client, &shared);
        drop_slots_of_pipeline_id(&mut self.a.client, &b);
    }
}

/// Starts a pipeline from `config`. Connector errors arrive on the receiver.
fn start(
    engine: &dyn PipelineTestEngine,
    config: Value,
) -> Result<(Controller, Receiver<String>), ControllerError> {
    let config = serde_json::from_value(config).unwrap();
    let (err_sender, err_receiver) = crossbeam::channel::unbounded();
    let controller = engine.controller(
        &TestCircuit::Identity {
            relation: cdc_relation(),
            persistent_output_ids: vec![None, None],
            input_properties: Vec::new(),
            materialized: true,
            persistent_ids: true,
            table_version: 0,
            attribute_defaults: BTreeMap::new(),
        },
        &config,
        Box::new(move |e, _| {
            let _ = err_sender.send(format!("{e}"));
        }),
    )?;
    controller.start();
    Ok((controller, err_receiver))
}

/// Bulk insert ids `from..=to` in one statement.
fn insert_range(table: &mut CdcTestTable, from: i64, to: i64) {
    table.execute(&format!(
        "INSERT INTO {} SELECT g, g % 2 = 0, g * 10, 'row-' || g FROM generate_series({from}, {to}) g",
        table.table_name
    ));
}

/// Ids of the inserts in the output at `path`, in order.
fn inserted_ids(path: &Path) -> Vec<i64> {
    read_output_json(path)
        .iter()
        .filter_map(|row| row.get("insert")?.get("id")?.as_i64())
        .collect()
}

/// Wait until the output at `path` holds at least `n` inserts. Panics on a
/// connector error or a timeout.
fn wait_for_inserts(path: &Path, n: usize, errors: &Receiver<String>, what: &str) {
    wait(
        || inserted_ids(path).len() >= n || !errors.is_empty(),
        WAIT_MS,
    )
    .unwrap_or_else(|_| {
        panic!(
            "timeout waiting for {what}: expected {n} inserts, got {:?}",
            inserted_ids(path)
        )
    });
    if let Ok(e) = errors.try_recv() {
        panic!("connector error while waiting for {what}: {e}");
    }
}

/// The apply slots etl holds, by name.
fn apply_slots(table: &mut CdcTestTable) -> Vec<String> {
    table
        .client
        .query(
            "SELECT slot_name::text FROM pg_replication_slots WHERE slot_name LIKE 'supabase_etl_apply_%' ORDER BY 1",
            &[],
        )
        .unwrap_or_else(|e| panic!("querying pg_replication_slots failed: {e}"))
        .iter()
        .map(|r| r.get::<_, String>(0))
        .collect()
}

/// The slots among `slots` whose name carries the pipeline id `token`.
fn slots_with_token<'a>(slots: &'a [String], token: &str) -> Vec<&'a String> {
    slots
        .iter()
        .filter(|name| name.split('_').any(|t| t == token))
        .collect()
}

/// Two connectors on one publication hold one apply slot between them, and
/// each gets the snapshot and the streamed rows of its own table.
#[engine_test(serial, services(Postgres))]
pub fn two_tables_share_one_slot(engine: &dyn PipelineTestEngine) {
    let mut tables = TwoTables::new("cdc_shared");
    insert_range(&mut tables.a, 1, 3);
    insert_range(&mut tables.b, 101, 103);
    let out_a = NamedTempFile::new().unwrap();
    let out_b = NamedTempFile::new().unwrap();

    let (controller, errors) = start(
        engine,
        tables.pipeline(true, json!({}), out_a.path(), out_b.path()),
    )
    .unwrap();
    wait_for_inserts(out_a.path(), 3, &errors, "snapshot of a");
    wait_for_inserts(out_b.path(), 3, &errors, "snapshot of b");

    insert_range(&mut tables.a, 4, 4);
    insert_range(&mut tables.b, 104, 104);
    wait_for_inserts(out_a.path(), 4, &errors, "streamed row of a");
    wait_for_inserts(out_b.path(), 4, &errors, "streamed row of b");

    let slots = apply_slots(&mut tables.a);
    assert_eq!(
        slots_with_token(&slots, &tables.shared_slot_id()).len(),
        1,
        "one apply slot for the publication: {slots:?}"
    );
    for table in [&tables.a, &tables.b] {
        assert!(
            slots_with_token(&slots, &tables.per_table_slot_id(table)).is_empty(),
            "no slot per table: {slots:?}"
        );
    }

    controller.stop().unwrap();
    let mut a = inserted_ids(out_a.path());
    let mut b = inserted_ids(out_b.path());
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, vec![1, 2, 3, 4], "table a arrives exactly once");
    assert_eq!(b, vec![101, 102, 103, 104], "table b arrives exactly once");
}

/// The settings that configure the whole etl pipeline must agree between
/// the connectors of one publication.
#[engine_test(serial, services(Postgres))]
pub fn members_with_different_settings_are_rejected(engine: &dyn PipelineTestEngine) {
    let tables = TwoTables::new("cdc_shared_cfg");
    let out_a = NamedTempFile::new().unwrap();
    let out_b = NamedTempFile::new().unwrap();

    let error = start(
        engine,
        tables.pipeline(
            true,
            json!({ "batch": { "max_fill_ms": 123 } }),
            out_a.path(),
            out_b.path(),
        ),
    )
    .err()
    .expect("a member with other batch settings is refused")
    .to_string();
    for needle in [
        "cdc_a",
        "cdc_b",
        "different 'batch' settings",
        "same settings",
    ] {
        assert!(error.contains(needle), "{needle:?} not in: {error}");
    }
}

/// Without the setting, each connector keeps a slot of its own.
#[engine_test(serial, services(Postgres))]
pub fn without_the_setting_each_table_keeps_its_slot(engine: &dyn PipelineTestEngine) {
    let mut tables = TwoTables::new("cdc_shared_off");
    insert_range(&mut tables.a, 1, 2);
    insert_range(&mut tables.b, 101, 102);
    let out_a = NamedTempFile::new().unwrap();
    let out_b = NamedTempFile::new().unwrap();

    let (controller, errors) = start(
        engine,
        tables.pipeline(false, json!({}), out_a.path(), out_b.path()),
    )
    .unwrap();
    wait_for_inserts(out_a.path(), 2, &errors, "snapshot of a");
    wait_for_inserts(out_b.path(), 2, &errors, "snapshot of b");

    let slots = apply_slots(&mut tables.a);
    assert!(
        slots_with_token(&slots, &tables.shared_slot_id()).is_empty(),
        "no shared slot: {slots:?}"
    );
    for table in [&tables.a, &tables.b] {
        assert_eq!(
            slots_with_token(&slots, &tables.per_table_slot_id(table)).len(),
            1,
            "one slot per table: {slots:?}"
        );
    }
    controller.stop().unwrap();
}
