//! Connectors of one publication that share one replication slot
//! (`dev_tweaks.shared_cdc_slot`, #7413).

use crate::{
    Controller, ControllerError,
    integrated::postgres::{
        cdc::{ETL_STARTS, RESTART_GRACE_MS},
        test::{
            cdc_tests::{
                CdcTestTable, cdc_connector_url, cdc_relation, drop_slots_of_pipeline_id,
                read_output_json,
            },
            postgres_url, unique_pg_name,
        },
    },
    test::{
        engine_test,
        pipeline::{PipelineTestEngine, TestCircuit},
    },
};
use crossbeam::channel::Receiver;
use feldera_adapterlib::test::wait;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};
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
        // Tests assert on restarts, so none waits for the grace.
        RESTART_GRACE_MS.store(0, Ordering::Relaxed);
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

    /// The id of the etl pipeline the two connectors share.
    fn shared_pipeline_id(&self) -> u64 {
        crate::integrated::postgres::cdc::shared_pipeline_id(
            &cdc_connector_url(&self.a.url),
            self.publication(),
            PIPELINE_NAME,
        )
    }

    /// The id of the slot the two connectors share.
    fn shared_slot_id(&self) -> String {
        self.shared_pipeline_id().to_string()
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

    /// How many times etl started for the shared pipeline.
    fn etl_starts(&self) -> usize {
        ETL_STARTS
            .lock()
            .unwrap()
            .get(&self.shared_pipeline_id())
            .copied()
            .unwrap_or(0)
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

    /// The input endpoint `cdc_<name>` on `table`, feeding `test_input<n>`.
    fn input(&self, table: &CdcTestTable, n: usize, extra: Value) -> Value {
        json!({
            "stream": format!("test_input{n}"),
            "transport": { "name": "postgres_cdc_input", "config": self.connector(table, extra) },
        })
    }

    /// A pipeline with connectors `cdc_a` on `a` and, when `with_b`, `cdc_b`
    /// on `b`, writing to `out_a` and `out_b`.
    fn pipeline(&self, shared: bool, with_b: Option<Value>, out_a: &Path, out_b: &Path) -> Value {
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
        let mut inputs = json!({ "cdc_a": self.input(&self.a, 1, json!({})) });
        if let Some(b_extra) = with_b {
            inputs["cdc_b"] = self.input(&self.b, 2, b_extra);
        }
        json!({
            "name": PIPELINE_NAME,
            "workers": 1,
            "dev_tweaks": { "shared_cdc_slot": shared },
            "inputs": inputs,
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
        tables.pipeline(true, Some(json!({})), out_a.path(), out_b.path()),
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
    assert_eq!(tables.etl_starts(), 1, "a stop starts etl for nobody");
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
            Some(json!({ "batch": { "max_fill_ms": 123 } })),
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
        tables.pipeline(false, Some(json!({})), out_a.path(), out_b.path()),
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

/// A connector that connects while etl runs for another table makes etl
/// start again with both, gets its snapshot, and the first table streams on
/// with every row exactly once.
#[engine_test(serial, services(Postgres))]
pub fn a_late_connector_joins_the_running_publication(engine: &dyn PipelineTestEngine) {
    let mut tables = TwoTables::new("cdc_shared_join");
    insert_range(&mut tables.a, 1, 3);
    insert_range(&mut tables.b, 101, 103);
    let out_a = NamedTempFile::new().unwrap();
    let out_b = NamedTempFile::new().unwrap();

    let (controller, errors) = start(
        engine,
        tables.pipeline(true, None, out_a.path(), out_b.path()),
    )
    .unwrap();
    wait_for_inserts(out_a.path(), 3, &errors, "snapshot of a");
    assert_eq!(tables.etl_starts(), 1);

    let input_b = serde_json::from_value(tables.input(&tables.b, 2, json!({}))).unwrap();
    controller.connect_input("cdc_b", &input_b, None).unwrap();
    wait_for_inserts(out_b.path(), 3, &errors, "snapshot of b after joining");
    assert_eq!(
        tables.etl_starts(),
        2,
        "etl started again with the new member"
    );

    insert_range(&mut tables.a, 4, 5);
    insert_range(&mut tables.b, 104, 105);
    wait_for_inserts(out_a.path(), 5, &errors, "streamed rows of a");
    wait_for_inserts(out_b.path(), 5, &errors, "streamed rows of b");

    let slots = apply_slots(&mut tables.a);
    assert_eq!(
        slots_with_token(&slots, &tables.shared_slot_id()).len(),
        1,
        "still one apply slot: {slots:?}"
    );

    controller.stop().unwrap();
    let mut a = inserted_ids(out_a.path());
    let mut b = inserted_ids(out_b.path());
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, vec![1, 2, 3, 4, 5], "table a arrives exactly once");
    assert_eq!(
        b,
        vec![101, 102, 103, 104, 105],
        "table b arrives exactly once"
    );
}

/// etl's stored state of `table` in the shared pipeline, if any.
fn shared_etl_state(tables: &mut TwoTables, table: &str) -> Option<String> {
    let pipeline_id = tables.shared_pipeline_id() as i64;
    tables
        .a
        .client
        .query(
            "SELECT s.state::text FROM etl.replication_state s JOIN pg_class c \
             ON c.oid::text = s.table_id::text \
             WHERE s.pipeline_id = $1 AND s.is_current AND c.relname = $2",
            &[&pipeline_id, &table],
        )
        .unwrap_or_else(|e| panic!("querying etl.replication_state failed: {e}"))
        .first()
        .map(|r| r.get(0))
}

/// Bulk insert ids `from..=to` with a `pad_bytes` long string, so the copy
/// of the table takes several writes and some time.
fn insert_padded_rows(table: &mut CdcTestTable, from: i64, to: i64, pad_bytes: usize) {
    table.execute(&format!(
        "INSERT INTO {} SELECT g, g % 2 = 0, g * 10, repeat('x', {pad_bytes}) \
         FROM generate_series({from}, {to}) g",
        table.table_name
    ));
}

/// A disconnected connector leaves the publication; etl starts again once
/// for the other connector, which streams on with every row exactly once.
///
/// The disconnect lands while etl is still copying the other table, with
/// some of its rows already in the circuit. etl copies a table whose copy
/// was under way again from scratch when it starts, so the restart has to
/// wait for the copy.
#[engine_test(serial, services(Postgres))]
pub fn a_disconnected_member_leaves_the_others_streaming(engine: &dyn PipelineTestEngine) {
    const ROWS: i64 = 400;
    let mut tables = TwoTables::new("cdc_shared_leave");
    insert_padded_rows(&mut tables.a, 1, ROWS, 50_000);
    insert_range(&mut tables.b, 101, 103);
    let out_a = NamedTempFile::new().unwrap();
    let out_b = NamedTempFile::new().unwrap();

    let (controller, errors) = start(
        engine,
        tables.pipeline(true, Some(json!({})), out_a.path(), out_b.path()),
    )
    .unwrap();
    let a_name = tables.a.table_name.clone();
    wait(
        || {
            !inserted_ids(out_a.path()).is_empty()
                && shared_etl_state(&mut tables, &a_name).as_deref() == Some("data_sync")
        },
        WAIT_MS,
    )
    .expect("rows of a reach the circuit while etl still copies a");

    let id_b = controller.input_endpoint_id_by_name("cdc_b").unwrap();
    controller.disconnect_input(&id_b);
    wait(|| tables.etl_starts() == 2, WAIT_MS).expect("etl starts again for the member that stays");

    insert_range(&mut tables.a, ROWS + 1, ROWS + 3);
    insert_range(&mut tables.b, 104, 106);
    wait_for_inserts(
        out_a.path(),
        ROWS as usize + 3,
        &errors,
        "the copy and the streamed rows of a after the leave",
    );

    // Rows of `b` reach no Feldera table any more: give them time to arrive
    // wrongly, then check that they did not.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        inserted_ids(out_b.path()).len() <= 3,
        "b got nothing past its snapshot"
    );
    assert_eq!(tables.etl_starts(), 2, "one restart for the leave");

    controller.stop().unwrap();
    let mut a = inserted_ids(out_a.path());
    a.sort_unstable();
    assert_eq!(
        a,
        (1..=ROWS + 3).collect::<Vec<_>>(),
        "table a arrives exactly once"
    );
}
