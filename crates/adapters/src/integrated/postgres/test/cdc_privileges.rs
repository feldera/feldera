//! Source-migration and runtime-grant regressions for #7142.
//!
//! Each test owns a database and two roles. In particular, the test that removes
//! a migration history entry must never use the suite's shared `etl` schema.
//! `POSTGRES_URL` must connect as an administrator to a server with
//! `wal_level=logical`; the fixtures create databases, roles, and event triggers.

use super::cdc_tests::{
    CdcTestTable, build_simple_cdc_test_circuit, cdc_connector_url, read_output_json,
    wait_for_etl_sync_completed, wait_for_slots_released,
};
use super::*;

const SOURCE_MIGRATION: i64 = 20260724120000;

struct Database {
    admin: postgres::Client,
    name: String,
    role: String,
    owner: String,
    url: String,
    runtime_url: String,
}

impl Database {
    fn new() -> Self {
        let admin_url = cdc_connector_url(&postgres_url());
        let mut admin = pg::pg_connect(&admin_url, &None);
        let name = unique_pg_name("cdc_privileges_db");
        // Names contain only a fixed prefix and a UUID, so they are SQL identifiers.
        admin
            .batch_execute(&format!("CREATE DATABASE {name} TEMPLATE template0"))
            .unwrap();
        let mut url = url::Url::parse(&admin_url).unwrap();
        url.set_path(&name);
        let mut db = Self {
            admin,
            name,
            role: unique_pg_name("cdc_runtime"),
            owner: unique_pg_name("cdc_owner"),
            url: url.to_string(),
            runtime_url: String::new(),
        };
        db.admin
            .batch_execute(&format!(
                "CREATE ROLE {} LOGIN PASSWORD 'cdc_test_password' REPLICATION NOSUPERUSER;
                 CREATE ROLE {} NOLOGIN NOSUPERUSER;
                 REVOKE ALL ON DATABASE {} FROM PUBLIC;
                 GRANT CONNECT, CREATE ON DATABASE {} TO {};",
                db.role, db.owner, db.name, db.name, db.role,
            ))
            .unwrap();
        url.set_username(&db.role).unwrap();
        url.set_password(Some("cdc_test_password")).unwrap();
        db.runtime_url = url.to_string();

        let mut client = pg::pg_connect(&db.url, &None);
        client
            .batch_execute(&format!(
                "REVOKE ALL ON SCHEMA public FROM PUBLIC;
                 GRANT USAGE ON SCHEMA public TO {}, {};",
                db.role, db.owner,
            ))
            .unwrap();

        // Exercise the default on a genuinely fresh database, including a copy
        // and a streamed write, before installing any runtime grants on etl.
        let mut table = db.table();
        let run = Run::new(config(&db.url, &table, None));
        run.wait_row("insert", 1, None);
        wait_for_etl_sync_completed(&mut table);
        table.execute(&format!(
            "INSERT INTO {} VALUES (2, false, 20, 'bootstrap')",
            table.table_name,
        ));
        run.wait_row("insert", 2, None);
        run.stop(&mut table);
        drop(table);

        // Match the documented grants. The migration log is only read; identity
        // columns do not need a separate sequence grant. Pending DDL stays an
        // administrator's responsibility since the runtime role owns no objects.
        client
            .batch_execute(&format!(
                "GRANT USAGE, CREATE ON SCHEMA etl TO {role};
                 GRANT SELECT ON etl._sqlx_migrations TO {role};
                 GRANT SELECT, INSERT, UPDATE, DELETE ON
                    etl.replication_state, etl.replication_progress,
                    etl.table_schemas, etl.table_columns,
                    etl.destination_tables_metadata TO {role};
                 GRANT EXECUTE ON FUNCTION etl.describe_table_schema(oid) TO {role};
                 GRANT EXECUTE ON FUNCTION etl.describe_table_identity(oid) TO {role};",
                role = db.role,
            ))
            .unwrap();
        db
    }

    fn table(&self) -> CdcTestTable {
        let mut table = CdcTestTable::new_simple(
            &unique_pg_name("cdc_privileges_table"),
            &unique_pg_name("cdc_privileges_pub"),
            &self.url,
        );
        table
            .client
            .batch_execute(&format!(
                "ALTER TABLE {table} OWNER TO {owner};
             GRANT SELECT ON {table} TO {role};
             INSERT INTO {table} VALUES (1, true, 10, 'snapshot');",
                table = table.table_name,
                owner = self.owner,
                role = self.role,
            ))
            .unwrap();
        table
    }

    fn assert_trigger(&self, client: &mut postgres::Client) {
        let row = client
            .query_one(
                "SELECT e.evtenabled::text, r.rolsuper, f.rolsuper, p.prosecdef
             FROM pg_event_trigger e
             JOIN pg_roles r ON r.oid = e.evtowner
             JOIN pg_proc p ON p.oid = e.evtfoid
             JOIN pg_roles f ON f.oid = p.proowner
             WHERE e.evtname = 'supabase_etl_ddl_message_trigger'",
                &[],
            )
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "O");
        assert!(row.get::<_, bool>(1));
        assert!(row.get::<_, bool>(2));
        assert!(row.get::<_, bool>(3));
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Stop any remaining connections before dropping logical slots. FORCE
        // alone cannot drop a database with active logical replication slots.
        let cleanup = (|| -> anyhow::Result<()> {
            self.admin.query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                &[&self.name],
            )?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let slots = self.admin.query(
                    "SELECT slot_name::text FROM pg_replication_slots WHERE database = $1 AND active",
                    &[&self.name],
                )?;
                if slots.is_empty() {
                    break;
                }
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "timed out after 60 s waiting for replication slots in database {}: {:?}",
                    self.name,
                    slots
                        .iter()
                        .map(|row| row.get::<_, String>(0))
                        .collect::<Vec<_>>()
                );
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            self.admin.query(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1",
                &[&self.name],
            )?;
            self.admin
                .batch_execute(&format!("DROP DATABASE {} WITH (FORCE)", self.name))?;
            // Dropping the database removes its grants before dropping roles.
            self.admin.batch_execute(&format!(
                "DROP ROLE IF EXISTS {}; DROP ROLE IF EXISTS {};",
                self.role, self.owner,
            ))?;
            Ok(())
        })();
        if let Err(error) = cleanup {
            if std::thread::panicking() {
                eprintln!("CDC privilege fixture cleanup failed: {error:?}");
            } else {
                panic!("CDC privilege fixture cleanup failed: {error:?}");
            }
        }
    }
}

fn config(url: &str, table: &CdcTestTable, migrations: Option<bool>) -> serde_json::Value {
    let mut config = json!({
        "uri": url,
        "publication": table.publication_name,
        "source_table": format!("public.{}", table.table_name),
    });
    if let Some(enabled) = migrations {
        config["run_source_migrations"] = json!(enabled);
    }
    config
}

struct Run {
    controller: Option<Controller>,
    errors: crossbeam::channel::Receiver<String>,
    output: NamedTempFile,
}

impl Run {
    fn new(config: serde_json::Value) -> Self {
        let output = NamedTempFile::new().unwrap();
        let (controller, errors) = build_simple_cdc_test_circuit(config, output.path()).unwrap();
        controller.start();
        Self {
            controller: Some(controller),
            errors,
            output,
        }
    }

    fn wait_row(&self, operation: &str, id: i64, text: Option<&str>) {
        wait(
            || {
                !self.errors.is_empty()
                    || read_output_json(self.output.path()).iter().any(|row| {
                        row.get(operation).is_some_and(|row| {
                            row["id"] == id && text.is_none_or(|text| row["s"] == text)
                        })
                    })
            },
            60_000,
        )
        .expect("timed out waiting for CDC row");
        assert!(
            self.errors.is_empty(),
            "CDC error: {:?}",
            self.errors.try_recv()
        );
    }

    fn stop(mut self, table: &mut CdcTestTable) {
        self.controller.take().unwrap().stop().unwrap();
        wait_for_slots_released(table);
        assert!(
            self.errors.is_empty(),
            "CDC error: {:?}",
            self.errors.try_recv()
        );
    }

    fn stop_after_error(mut self, table: &mut CdcTestTable) {
        self.controller.take().unwrap().stop().unwrap();
        wait_for_slots_released(table);
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        if let Some(controller) = self.controller.take() {
            let _ = controller.stop();
        }
    }
}

fn startup_error(config: serde_json::Value) -> String {
    let output = NamedTempFile::new().unwrap();
    match build_simple_cdc_test_circuit(config, output.path()) {
        Err(error) => error.to_string(),
        Ok((controller, _)) => {
            let _ = controller.stop();
            panic!("connector unexpectedly started");
        }
    }
}

#[test]
#[serial]
fn test_cdc_privileges_snapshot_stream_restart_and_ddl() {
    let db = Database::new();
    let mut table = db.table();
    let mut runtime = pg::pg_connect(&db.runtime_url, &None);
    let row = runtime
        .query_one(
            "SELECT rolsuper, rolreplication, has_table_privilege($1, 'INSERT')
         FROM pg_roles WHERE rolname = current_user",
            &[&table.table_name],
        )
        .unwrap();
    assert!(!row.get::<_, bool>(0));
    assert!(row.get::<_, bool>(1));
    assert!(!row.get::<_, bool>(2));
    drop(runtime);

    let run = Run::new(config(&db.runtime_url, &table, Some(false)));
    run.wait_row("insert", 1, None);
    wait_for_etl_sync_completed(&mut table);
    table.execute(&format!(
        "INSERT INTO {} VALUES (2, false, 20, 'new')",
        table.table_name
    ));
    run.wait_row("insert", 2, Some("new"));
    table.execute(&format!(
        "UPDATE {} SET s = 'updated' WHERE id = 2",
        table.table_name
    ));
    run.wait_row("delete", 2, Some("new"));
    run.wait_row("insert", 2, Some("updated"));
    table.execute(&format!("DELETE FROM {} WHERE id = 2", table.table_name));
    run.wait_row("delete", 2, Some("updated"));

    // The source owner has no etl grants. The administrator-owned definer
    // function must still emit schema changes on its behalf.
    table
        .client
        .batch_execute(&format!(
            "SET ROLE {}; ALTER TABLE {} DROP COLUMN i; RESET ROLE;
         INSERT INTO {} (id, b, s) VALUES (3, true, 'after_ddl');",
            db.owner, table.table_name, table.table_name,
        ))
        .unwrap();
    run.wait_row("insert", 3, Some("after_ddl"));
    db.assert_trigger(&mut table.client);
    run.stop(&mut table);

    let slots = table.client.query(
        "SELECT slot_name::text FROM pg_replication_slots WHERE database = current_database() ORDER BY slot_name", &[],
    ).unwrap().iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>();
    assert!(!slots.is_empty());
    let run = Run::new(config(&db.runtime_url, &table, Some(false)));
    table.execute(&format!(
        "INSERT INTO {} (id, b, s) VALUES (4, true, 'restart')",
        table.table_name
    ));
    run.wait_row("insert", 4, None);
    assert!(
        !read_output_json(run.output.path())
            .iter()
            .any(|r| r["insert"]["id"] == 1),
        "restart repeated the initial snapshot"
    );
    let resumed_slots = table.client.query(
        "SELECT slot_name::text FROM pg_replication_slots WHERE database = current_database() ORDER BY slot_name", &[],
    ).unwrap().iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>();
    assert_eq!(slots, resumed_slots);

    // An incompatible change still becomes a connector error with migrations off.
    table
        .client
        .batch_execute(&format!(
            "SET ROLE {}; ALTER TABLE {} DROP COLUMN s; RESET ROLE;",
            db.owner, table.table_name,
        ))
        .unwrap();
    // Column validation runs when the destination receives rows in the new
    // schema, just as in the existing incompatible-schema regression test.
    table.execute(&format!(
        "INSERT INTO {} (id, b) VALUES (5, true)",
        table.table_name,
    ));
    wait(|| !run.errors.is_empty(), 60_000).expect("missing required column was not reported");
    let error = run.errors.try_recv().unwrap();
    assert!(
        error.contains("missing required Feldera columns")
            && error
                .contains("non-nullable Feldera columns absent from the Postgres table: [\"s\"]"),
        "unexpected error: {error}"
    );
    run.stop_after_error(&mut table);
}

#[test]
#[serial]
fn test_cdc_privileges_switch_credentials_resumes() {
    let db = Database::new();
    let mut table = db.table();
    let run = Run::new(config(&db.url, &table, None));
    run.wait_row("insert", 1, None);
    wait_for_etl_sync_completed(&mut table);
    table.execute(&format!(
        "INSERT INTO {} VALUES (2, false, 20, 'before_switch')",
        table.table_name
    ));
    run.wait_row("insert", 2, None);
    run.stop(&mut table);
    let slot = table.client.query_one(
        "SELECT slot_name::text FROM pg_replication_slots WHERE database = current_database() AND slot_name LIKE 'supabase_etl_apply_%'", &[],
    ).unwrap().get::<_, String>(0);

    let run = Run::new(config(&db.runtime_url, &table, Some(false)));
    table.execute(&format!(
        "INSERT INTO {} VALUES (3, true, 30, 'after_switch')",
        table.table_name
    ));
    run.wait_row("insert", 3, None);
    assert!(
        !read_output_json(run.output.path())
            .iter()
            .any(|r| r["insert"]["id"] == 1),
        "switching credentials repeated the snapshot"
    );
    let active: bool = table
        .client
        .query_one(
            "SELECT active FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .unwrap()
        .get(0);
    assert!(active, "the administrator's slot was not reused");
    run.stop(&mut table);
}

#[test]
#[serial]
fn test_cdc_privileges_source_migration_switch() {
    let db = Database::new();
    let mut table = db.table();
    assert_eq!(
        table
            .client
            .execute(
                "DELETE FROM etl._sqlx_migrations WHERE version = $1",
                &[&SOURCE_MIGRATION],
            )
            .unwrap(),
        1
    );
    for enabled in [Some(true), None] {
        let error = startup_error(config(&db.runtime_url, &table, enabled));
        // The hint also mentions superuser privileges; check the underlying
        // PostgreSQL failure independently of the advice appended to it.
        let diagnostic = error.split(". Hint:").next().unwrap();
        assert!(
            diagnostic.contains("source migrations")
                && (diagnostic.contains("superuser")
                    || diagnostic.contains("owner")
                    || diagnostic.contains("permission denied")),
            "unexpected error: {error}"
        );
        assert_setup_hint(&error, true);
        assert!(error.contains("connect as a superuser"), "{error}");
        assert!(
            error.contains("before setting run_source_migrations=false"),
            "{error}"
        );
        db.assert_trigger(&mut table.client);
    }
    let run = Run::new(config(&db.runtime_url, &table, Some(false)));
    run.wait_row("insert", 1, None);
    wait_for_etl_sync_completed(&mut table);
    table.execute(&format!(
        "INSERT INTO {} VALUES (2, false, 20, 'stream')",
        table.table_name
    ));
    run.wait_row("insert", 2, None);
    let count: i64 = table
        .client
        .query_one(
            "SELECT count(*) FROM etl._sqlx_migrations WHERE version = $1",
            &[&SOURCE_MIGRATION],
        )
        .unwrap()
        .get(0);
    assert_eq!(count, 0, "disabled source migration was recorded");
    db.assert_trigger(&mut table.client);
    run.stop(&mut table);
}

#[test]
#[serial]
fn test_cdc_privileges_missing_setup_errors() {
    let db = Database::new();
    let mut table = db.table();
    for (object, privilege) in [
        (format!("DATABASE {}", db.name), "CREATE"),
        ("SCHEMA etl".to_string(), "CREATE"),
        ("TABLE etl._sqlx_migrations".to_string(), "SELECT"),
    ] {
        table.execute(&format!("REVOKE {privilege} ON {object} FROM {}", db.role));
        let error = startup_error(config(&db.runtime_url, &table, Some(false)));
        assert!(
            error.contains("PostgresStore") && error.contains("permission denied"),
            "unexpected error without {privilege} on {object}: {error}"
        );
        assert_setup_hint(&error, false);
        assert!(error.contains("state-table privileges"), "{error}");
        table.execute(&format!("GRANT {privilege} ON {object} TO {}", db.role));
    }

    // Schema helper failures arise in the copy worker, after startup has returned.
    for missing in [false, true] {
        let mut source = db.table();
        if missing {
            table.execute("DROP FUNCTION etl.describe_table_schema(oid)");
        } else {
            table.execute(&format!(
                "REVOKE EXECUTE ON FUNCTION etl.describe_table_schema(oid) FROM {}",
                db.role,
            ));
        }
        let run = Run::new(config(&db.runtime_url, &source, Some(false)));
        wait(|| !run.errors.is_empty(), 60_000).expect("schema helper failure was not reported");
        let error = run.errors.try_recv().unwrap();
        assert!(
            error.contains("describe_table_schema"),
            "unexpected error: {error}"
        );
        assert!(
            error.contains(if missing {
                "does not exist"
            } else {
                "permission denied"
            }),
            "unexpected error: {error}"
        );
        assert_setup_hint(&error, false);
        assert!(
            error.contains("install or update etl's source objects"),
            "{error}"
        );
        assert!(error.contains("grant EXECUTE"), "{error}");
        run.stop_after_error(&mut source);
    }
}

fn assert_setup_hint(error: &str, run_source_migrations: bool) {
    assert!(
        error.contains(&format!("With run_source_migrations={run_source_migrations}"))
            && error.contains("State-store migrations still run")
            && error.contains("Running as a non-superuser")
            && error.contains(
                "https://docs.feldera.com/connectors/sources/postgresql-cdc#running-as-a-non-superuser"
            ),
        "missing setup guidance: {error}"
    );
}
