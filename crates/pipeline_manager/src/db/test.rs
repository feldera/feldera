use super::{
    storage::Storage, AttachedConnector, ConnectorDescr, ConnectorId, DBError, PipelineId,
    PipelineRevision, PipelineStatus, ProgramDescr, ProgramId, ProgramStatus, ProjectDB, Revision,
    Version,
};
use super::{ApiPermission, Pipeline, PipelineDescr, PipelineRuntimeState, ProgramSchema};
use crate::auth::{self, TenantId, TenantRecord};
use crate::db::{Relation, ServiceDescr, ServiceId};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use openssl::sha::{self};
use pipeline_types::config::{
    ConnectorConfig, MysqlConfig, ResourceConfig, RuntimeConfig, ServiceConfig,
};
use pretty_assertions::assert_eq;
use proptest::test_runner::{Config, TestRunner};
use proptest::{bool, prelude::*};
use proptest_derive::Arbitrary;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::SystemTime;
use std::vec;
use tokio::sync::Mutex;
use uuid::Uuid;

type DBResult<T> = Result<T, DBError>;

struct DbHandle {
    db: ProjectDB,
    #[cfg(feature = "pg-embed")]
    _temp_dir: tempfile::TempDir,
    #[cfg(not(feature = "pg-embed"))]
    config: tokio_postgres::Config,
}

impl Drop for DbHandle {
    #[cfg(feature = "pg-embed")]
    fn drop(&mut self) {
        // We drop `pg` before the temp dir gets deleted (which will
        // shutdown postgres). Otherwise postgres log an error that the
        // directory is already gone during shutdown which could be
        // confusing for a developer.
        if let Some(pg) = self.db.pg_inst.as_mut() {
            let _r = async {
                pg.stop_db().await.unwrap();
            };
        }
    }

    #[cfg(not(feature = "pg-embed"))]
    fn drop(&mut self) {
        let _r = async {
            let db_name = self.config.get_dbname().unwrap_or("");

            // This command cannot be executed while connected to the target
            // database. Thus we make a new connection.
            let mut config = self.config.clone();
            config.dbname("");
            let (client, conn) = config.connect(tokio_postgres::NoTls).await.unwrap();
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    eprintln!("connection error: {}", e);
                }
            });

            client
                .execute(format!("DROP DATABASE {} FORCE", db_name).as_str(), &[])
                .await
                .unwrap();
        };
    }
}

#[cfg(test)]
impl Version {
    fn increment(&self) -> Self {
        Self(self.0 + 1)
    }
}

#[cfg(feature = "pg-embed")]
async fn test_setup() -> DbHandle {
    let (conn, _temp_dir) = setup_pg().await;

    DbHandle {
        db: conn,
        _temp_dir,
    }
}

#[cfg(feature = "pg-embed")]
pub(crate) async fn setup_pg() -> (ProjectDB, tempfile::TempDir) {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU16, Ordering};

    // Find a free port to use for running the test database.
    let port = {
        /// This is a fallback method counter for port selection in case binding
        /// to port 0 on localhost fails (to select a random, open
        /// port).
        static DB_PORT_COUNTER: AtomicU16 = AtomicU16::new(5555);

        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("Failed to bind to port 0");
        listener
            .local_addr()
            .map(|l| l.port())
            .unwrap_or(DB_PORT_COUNTER.fetch_add(1, Ordering::Relaxed))
    };

    let _temp_dir = tempfile::tempdir().unwrap();
    let temp_path = _temp_dir.path();
    let pg = crate::db::pg_setup::install(temp_path.into(), false, Some(port))
        .await
        .unwrap();
    let db_uri = pg.db_uri.clone();
    let conn = ProjectDB::connect_inner(&db_uri, Some(pg)).await.unwrap();
    conn.run_migrations().await.unwrap();
    (conn, _temp_dir)
}

#[cfg(not(feature = "pg-embed"))]
async fn test_setup() -> DbHandle {
    let (conn, config) = setup_pg().await;
    DbHandle { db: conn, config }
}

#[cfg(not(feature = "pg-embed"))]
pub(crate) async fn setup_pg() -> (ProjectDB, tokio_postgres::Config) {
    use pg_client_config::load_config;

    let mut config = load_config(None).unwrap();

    // Workaround for https://github.com/3liz/pg-event-server/issues/1.
    if let Ok(pguser) = std::env::var("PGUSER") {
        config.user(&pguser);
    };

    let test_db = format!("test_{}", rand::thread_rng().gen::<u32>());
    let (client, conn) = config
        .connect(tokio_postgres::NoTls)
        .await
        .expect("Failure connecting to test PG instance");
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
        .execute(format!("CREATE DATABASE {}", test_db).as_str(), &[])
        .await
        .expect("Failure in test setup");
    drop(client);

    log::debug!("tests connecting to: {config:#?}");

    config.dbname(&test_db);
    let conn = ProjectDB::with_config(config.clone()).await.unwrap();
    conn.run_migrations().await.unwrap();

    (conn, config)
}

pub fn test_connector_config() -> ConnectorConfig {
    ConnectorConfig::from_yaml_str(
        r#"
transport:
    name: kafka
    config:
        auto.offset.reset: "earliest"
        group.instance.id: "group0"
        topics: [test_input1]
format:
    name: csv"#,
    )
}

#[tokio::test]
async fn program_creation() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let res = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "ignored",
        )
        .await
        .unwrap();
    let rows = handle.db.list_programs(tenant_id, false).await.unwrap();
    let expected = ProgramDescr {
        program_id: res.0,
        name: "test1".to_string(),
        description: "program desc".to_string(),
        version: res.1,
        status: ProgramStatus::None,
        schema: None,
        code: None,
    };
    let actual = rows.get(0).unwrap();
    assert_eq!(1, rows.len());
    assert_eq!(&expected, actual);

    let rows = handle.db.list_programs(tenant_id, true).await.unwrap();
    let expected = ProgramDescr {
        program_id: res.0,
        name: "test1".to_string(),
        description: "program desc".to_string(),
        version: res.1,
        status: ProgramStatus::None,
        schema: None,
        code: Some("ignored".to_string()),
    };
    let actual = rows.get(0).unwrap();
    assert_eq!(1, rows.len());
    assert_eq!(&expected, actual);
}

#[tokio::test]
async fn duplicate_program() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let _ = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "ignored",
        )
        .await;
    let res = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "ignored",
        )
        .await
        .expect_err("Expecting unique violation");
    let expected = DBError::DuplicateName;
    assert_eq!(format!("{}", res), format!("{}", expected));
}

#[tokio::test]
async fn program_code() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let (program_id, _) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let result = handle
        .db
        .get_program_by_id(tenant_id, program_id, true)
        .await
        .unwrap();
    assert_eq!("test1", result.name);
    assert_eq!("program desc", result.description);
    assert_eq!(
        "create table t1(c1 integer);".to_owned(),
        result.code.unwrap()
    );
}

#[tokio::test]
async fn update_program() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let (program_id, _) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let _ = handle
        .db
        .update_program(
            tenant_id,
            program_id,
            "test2",
            "different desc",
            &Some("create table t2(c2 integer);".to_string()),
        )
        .await;
    let descr = handle
        .db
        .get_program_by_id(tenant_id, program_id, true)
        .await
        .unwrap();
    assert_eq!("test2", descr.name);
    assert_eq!("different desc", descr.description);
    assert_eq!("create table t2(c2 integer);", descr.code.unwrap());

    let _ = handle
        .db
        .update_program(
            tenant_id,
            program_id,
            "updated_test1",
            "some new description",
            &None,
        )
        .await;
    let results = handle.db.list_programs(tenant_id, false).await.unwrap();
    assert_eq!(1, results.len());
    let row = results.get(0).unwrap();
    assert_eq!("updated_test1", row.name);
    assert_eq!("some new description", row.description);
}

#[tokio::test]
async fn program_queries() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let (program_id, _) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_if_exists(tenant_id, program_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle
        .db
        .lookup_program(tenant_id, "test1", false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle
        .db
        .lookup_program(tenant_id, "test2", false)
        .await
        .unwrap();
    assert!(desc.is_none());
}

#[tokio::test]
async fn program_config() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let connector_id = handle
        .db
        .new_connector(
            tenant_id,
            Uuid::now_v7(),
            "a",
            "b",
            &test_connector_config(),
        )
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_id,
        relation_name: "".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    let _ = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            None,
            "1",
            "2",
            &rc,
            &Some(vec![ac.clone()]),
        )
        .await
        .unwrap();
    let res = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(1, res.len());
    let config = res.get(0).unwrap();
    assert_eq!("1", config.descriptor.name);
    assert_eq!("2", config.descriptor.description);
    assert_eq!(rc, config.descriptor.config);
    assert_eq!(None, config.descriptor.program_id);
    let connectors = &config.descriptor.attached_connectors;
    assert_eq!(1, connectors.len());
    let ac_ret = connectors.get(0).unwrap().clone();
    assert_eq!(ac, ac_ret);
}

#[tokio::test]
async fn project_pending() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let (uid1, v1) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "project desc",
            "ignored",
        )
        .await
        .unwrap();
    let (uid2, v2) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test2",
            "project desc",
            "ignored",
        )
        .await
        .unwrap();

    handle
        .db
        .set_program_for_compilation(tenant_id, uid2, v2, ProgramStatus::Pending)
        .await
        .unwrap();
    handle
        .db
        .set_program_for_compilation(tenant_id, uid1, v1, ProgramStatus::Pending)
        .await
        .unwrap();
    let (_, id, _version) = handle.db.next_job().await.unwrap().unwrap();
    assert_eq!(id, uid2);
    let (_, id, _version) = handle.db.next_job().await.unwrap().unwrap();
    assert_eq!(id, uid2);
    // Maybe next job should set the status to something else
    // so it won't get picked up twice?
}

#[tokio::test]
async fn update_status() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let (program_id, _) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_if_exists(tenant_id, program_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ProgramStatus::None, desc.status);
    handle
        .db
        .set_program_for_compilation(
            tenant_id,
            program_id,
            desc.version,
            ProgramStatus::CompilingRust,
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_if_exists(tenant_id, program_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ProgramStatus::CompilingRust, desc.status);
}

#[tokio::test]
async fn duplicate_attached_conn_name() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let connector_id = handle
        .db
        .new_connector(
            tenant_id,
            Uuid::now_v7(),
            "a",
            "b",
            &test_connector_config(),
        )
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_id,
        relation_name: "".to_string(),
    };
    let ac2 = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_id,
        relation_name: "".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    let _ = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            None,
            "1",
            "2",
            &rc,
            &Some(vec![ac, ac2]),
        )
        .await
        .expect_err("duplicate attached connector name");
}

#[tokio::test]
async fn save_api_key() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    // Attempt several key generations and validations
    for _ in 1..10 {
        let api_key = auth::generate_api_key();
        handle
            .db
            .store_api_key_hash(
                tenant_id,
                api_key.clone(),
                vec![ApiPermission::Read, ApiPermission::Write],
            )
            .await
            .unwrap();
        let scopes = handle.db.validate_api_key(api_key.clone()).await.unwrap();
        assert_eq!(tenant_id, scopes.0);
        assert_eq!(&ApiPermission::Read, scopes.1.get(0).unwrap());
        assert_eq!(&ApiPermission::Write, scopes.1.get(1).unwrap());

        let api_key_2 = auth::generate_api_key();
        let err = handle.db.validate_api_key(api_key_2).await.unwrap_err();
        assert!(matches!(err, DBError::InvalidKey));
    }
}

/// A Function that commits twice and checks the second time errors, returns
/// revision of first commit.
async fn commit_check(handle: &DbHandle, tenant_id: TenantId, pipeline_id: PipelineId) -> Revision {
    let new_revision_id = Uuid::now_v7();
    let r = handle
        .db
        .create_pipeline_revision(new_revision_id, tenant_id, pipeline_id)
        .await
        .unwrap();
    assert_eq!(r.0, new_revision_id);

    // We get an error the 2nd time since nothing changed
    let e = handle
        .db
        .create_pipeline_revision(new_revision_id, tenant_id, pipeline_id)
        .await
        .unwrap_err();
    match e {
        DBError::RevisionNotChanged => r,
        e => {
            // We should get a RevisionNotChanged error here since we
            // didn't change anything inbetween
            panic!("unexpected error trying to create revision 2nd time: {}", e)
        }
    }
}

#[tokio::test]
async fn create_tenant() {
    let handle = test_setup().await;
    let tenant_id_1 = handle
        .db
        .get_or_create_tenant_id("x".to_string(), "y".to_string())
        .await
        .unwrap();
    let tenant_id_2 = handle
        .db
        .get_or_create_tenant_id("x".to_string(), "y".to_string())
        .await
        .unwrap();
    // Concurrent attempts to create should also return the same tenant_id
    let tenant_id_3 = handle
        .db
        .create_tenant_if_not_exists(Uuid::now_v7(), "x".to_string(), "y".to_string())
        .await
        .unwrap();
    let tenant_id_4 = handle
        .db
        .get_or_create_tenant_id("x".to_string(), "y".to_string())
        .await
        .unwrap();
    assert_eq!(tenant_id_1, tenant_id_2);
    assert_eq!(tenant_id_2, tenant_id_3);
    assert_eq!(tenant_id_3, tenant_id_4);
}

#[tokio::test]
async fn versioning_no_change_no_connectors() {
    let _r = env_logger::try_init();

    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;

    let (program_id, _) = handle
        .db
        .new_program(tenant_id, Uuid::now_v7(), "", "", "")
        .await
        .unwrap();
    handle
        .db
        .set_program_status_guarded(tenant_id, program_id, Version(1), ProgramStatus::Success)
        .await
        .unwrap();
    let rc = RuntimeConfig::from_yaml("");
    let (pipeline_id, _version) = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            Some(program_id),
            "",
            "",
            &rc,
            &None,
        )
        .await
        .unwrap();
    let _r = handle
        .db
        .set_program_schema(
            tenant_id,
            program_id,
            ProgramSchema {
                inputs: vec![],
                outputs: vec![],
            },
        )
        .await
        .unwrap();
    let _r1: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
}

#[tokio::test]
async fn versioning() {
    let _r = env_logger::try_init();

    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;

    let (program_id, _) = handle
        .db
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "test1",
            "program desc",
            "only schema matters--this isn't compiled",
        )
        .await
        .unwrap();
    handle
        .db
        .set_program_status_guarded(tenant_id, program_id, Version(1), ProgramStatus::Success)
        .await
        .unwrap();
    handle
        .db
        .set_program_schema(
            tenant_id,
            program_id,
            ProgramSchema {
                inputs: vec![Relation {
                    name: "t1".into(),
                    fields: vec![],
                }],
                outputs: vec![Relation {
                    name: "v1".into(),
                    fields: vec![],
                }],
            },
        )
        .await
        .unwrap();
    let config1 = test_connector_config();
    let config2 = ConnectorConfig {
        max_buffered_records: config1.clone().max_buffered_records + 5,
        ..config1.clone()
    };
    let connector_id1: ConnectorId = handle
        .db
        .new_connector(tenant_id, Uuid::now_v7(), "a", "b", &config1)
        .await
        .unwrap();
    let mut ac1 = AttachedConnector {
        name: "ac1".to_string(),
        is_input: true,
        connector_id: connector_id1,
        relation_name: "t1".to_string(),
    };
    let connector_id2 = handle
        .db
        .new_connector(tenant_id, Uuid::now_v7(), "d", "e", &config2)
        .await
        .unwrap();
    let mut ac2 = AttachedConnector {
        name: "ac2".to_string(),
        is_input: false,
        connector_id: connector_id2,
        relation_name: "v1".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    let (pipeline_id, _version) = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            Some(program_id),
            "1",
            "2",
            &rc,
            &Some(vec![ac1.clone(), ac2.clone()]),
        )
        .await
        .unwrap();
    let r1: Revision = commit_check(&handle, tenant_id, pipeline_id).await;

    // If we change the program we need to adjust the connectors before we can
    // commit again:
    let new_version = handle
        .db
        .update_program(
            tenant_id,
            program_id,
            "test1",
            "program desc",
            &Some("only schema matters--this isn't compiled2".to_string()),
        )
        .await
        .unwrap();
    handle
        .db
        .set_program_status_guarded(tenant_id, program_id, Version(2), ProgramStatus::Success)
        .await
        .unwrap();
    handle
        .db
        .set_program_schema(
            tenant_id,
            program_id,
            ProgramSchema {
                inputs: vec![
                    Relation {
                        name: "t1".into(),
                        fields: vec![],
                    },
                    Relation {
                        name: "t2".into(),
                        fields: vec![],
                    },
                ],
                outputs: vec![Relation {
                    name: "v1".into(),
                    fields: vec![],
                }],
            },
        )
        .await
        .unwrap();
    let r2: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r1, r2, "we got a new revision");

    handle
        .db
        .set_program_status_guarded(tenant_id, program_id, new_version, ProgramStatus::Success)
        .await
        .unwrap();
    handle
        .db
        .set_program_schema(
            tenant_id,
            program_id,
            ProgramSchema {
                inputs: vec![Relation {
                    name: "tnew1".into(),
                    fields: vec![],
                }],
                outputs: vec![Relation {
                    name: "vnew1".into(),
                    fields: vec![],
                }],
            },
        )
        .await
        .unwrap();
    // This doesn't work because the connectors reference (now invalid) tables:
    assert!(handle
        .db
        .create_pipeline_revision(Uuid::now_v7(), tenant_id, pipeline_id)
        .await
        .is_err());
    ac1.is_input = true;
    ac1.relation_name = "tnew1".into();
    let gp_config = RuntimeConfig {
        workers: 1,
        cpu_profiler: true,
        min_batch_size_records: 0,
        max_buffering_delay_usecs: 0,
        resources: ResourceConfig::default(),
    };
    handle
        .db
        .update_pipeline(
            tenant_id,
            pipeline_id,
            Some(program_id),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone(), ac2.clone()]),
        )
        .await
        .unwrap();
    // This doesn't work because the ac2 still references a wrong table
    assert!(handle
        .db
        .create_pipeline_revision(Uuid::now_v7(), tenant_id, pipeline_id)
        .await
        .is_err());
    // Let's fix ac2
    ac2.is_input = false;
    ac2.relation_name = "vnew1".into();
    handle
        .db
        .update_pipeline(
            tenant_id,
            pipeline_id,
            Some(program_id),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone(), ac2.clone()]),
        )
        .await
        .unwrap();

    // Now we can commit again
    let r3 = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r2, r3, "we got a new revision");

    // If we change the connector we can commit again:
    let config3 = ConnectorConfig {
        max_buffered_records: config2.max_buffered_records + 5,
        ..config2
    };
    handle
        .db
        .update_connector(tenant_id, connector_id1, "a", "b", &Some(config3))
        .await
        .unwrap();
    let r4 = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r3, r4, "we got a new revision");

    // If we change the attached connectors we can commit again:
    ac1.name = "xxx".into();
    handle
        .db
        .update_pipeline(
            tenant_id,
            pipeline_id,
            Some(program_id),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone(), ac2.clone()]),
        )
        .await
        .unwrap();
    let r5: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r4, r5, "we got a new revision");

    // If we remove an ac that's also a change:
    handle
        .db
        .update_pipeline(
            tenant_id,
            pipeline_id,
            Some(program_id),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone()]),
        )
        .await
        .unwrap();
    let r6 = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r5, r6, "we got a new revision");

    // And if we change the pipeline config itself that's a change:
    handle
        .db
        .update_pipeline(
            tenant_id,
            pipeline_id,
            Some(program_id),
            "1",
            "2",
            &Some(RuntimeConfig {
                workers: gp_config.workers + 1,
                ..gp_config
            }),
            &Some(vec![ac1.clone()]),
        )
        .await
        .unwrap();
    let r6 = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r5, r6, "we got a new revision");
}

/// Generate uuids but limits the the randomess to the first bits.
///
/// This ensures that we have a good chance of generating a uuid that is already
/// in the database -- useful for testing error conditions.
pub(crate) fn limited_uuid() -> impl Strategy<Value = Uuid> {
    vec![any::<u8>()].prop_map(|mut bytes| {
        // prepend a bunch of zero bytes so the buffer is big enough for
        // building a uuid
        bytes.resize(16, 0);
        // restrict the any::<u8> (0..255) to 1..4 this enforces more
        // interesting scenarios for testing (and we start at 1 because shaving
        bytes[0] &= 0b11;
        // a uuid of 0 is invalid and postgres will treat it as NULL
        bytes[0] |= 0b1;
        Uuid::from_bytes(
            bytes
                .as_slice()
                .try_into()
                .expect("slice with incorrect length"),
        )
    })
}

/// Generate different connector types
/// TODO: should we generate more configuration variants?
pub(crate) fn limited_connector() -> impl Strategy<Value = ConnectorConfig> {
    any::<u8>().prop_map(|byte| {
        ConnectorConfig::from_yaml_str(
            format!(
                "
                transport:
                    name: kafka
                    config:
                        auto.offset.reset: \"earliest\"
                        group.instance.id: \"group0\"
                        topics: [test_input{byte}]
                format:
                    name: csv"
            )
            .as_str(),
        )
    })
}

/// Generate different connector types
/// TODO: should we generate more configuration variants?
pub(crate) fn limited_option_connector() -> impl Strategy<Value = Option<ConnectorConfig>> {
    any::<Option<u8>>().prop_map(|byte| {
        byte.map(|b| {
            ConnectorConfig::from_yaml_str(
                format!(
                    "
                transport:
                    name: kafka
                    config:
                        auto.offset.reset: \"earliest\"
                        group.instance.id: \"group0\"
                        topics: [test_input{b}]
                format:
                    name: csv"
                )
                .as_str(),
            )
        })
    })
}

/// Generate different service types
pub(crate) fn limited_service_config() -> impl Strategy<Value = ServiceConfig> {
    any::<u8>().prop_map(|byte| {
        ServiceConfig::Mysql(MysqlConfig {
            hostname: format!("example{byte}"),
            port: "1234".to_string(),
            user: "example".to_string(),
            password: "something".to_string(),
        })
    })
}

/// Generate different service types
pub(crate) fn limited_option_service_config() -> impl Strategy<Value = Option<ServiceConfig>> {
    any::<Option<u8>>().prop_map(|byte| {
        byte.map(|b| {
            ServiceConfig::Mysql(MysqlConfig {
                hostname: format!("example{b}"),
                port: "1234".to_string(),
                user: "example".to_string(),
                password: "something".to_string(),
            })
        })
    })
}

/// Generate different resource configurations
pub(crate) fn runtime_config() -> impl Strategy<Value = RuntimeConfig> {
    any::<(
        u16,
        bool,
        u64,
        u64,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
    )>()
    .prop_map(|config| RuntimeConfig {
        workers: config.0,
        cpu_profiler: config.1,
        min_batch_size_records: config.2,
        max_buffering_delay_usecs: config.3,
        resources: ResourceConfig {
            cpu_cores_min: config.4,
            cpu_cores_max: config.5,
            memory_mb_min: config.6,
            memory_mb_max: config.7,
            storage_mb_max: config.8,
        },
    })
}

/// Generate different resource configurations
pub(crate) fn option_runtime_config() -> impl Strategy<Value = Option<RuntimeConfig>> {
    any::<
        Option<(
            u16,
            bool,
            u64,
            u64,
            Option<u64>,
            Option<u64>,
            Option<u64>,
            Option<u64>,
            Option<u64>,
        )>,
    >()
    .prop_map(|c| {
        c.map(|config| RuntimeConfig {
            workers: config.0,
            cpu_profiler: config.1,
            min_batch_size_records: config.2,
            max_buffering_delay_usecs: config.3,
            resources: ResourceConfig {
                cpu_cores_min: config.4,
                cpu_cores_max: config.5,
                memory_mb_min: config.6,
                memory_mb_max: config.7,
                storage_mb_max: config.8,
            },
        })
    })
}

/// Actions we can do on the Storage trait.
#[derive(Debug, Clone, Arbitrary)]
enum StorageAction {
    ListPrograms(TenantId, bool),
    NewProgram(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        String,
    ),
    UpdateProgram(TenantId, ProgramId, String, String, Option<String>),
    GetProgramIfExists(TenantId, ProgramId, bool),
    LookupProgram(TenantId, String, bool),
    SetProgramForCompilation(TenantId, ProgramId, Version, ProgramStatus),
    SetProgramStatusGuarded(TenantId, ProgramId, Version, ProgramStatus),
    SetProgramSchema(TenantId, ProgramId, ProgramSchema),
    DeleteProgram(TenantId, ProgramId),
    AllPrograms,
    NextJob,
    NewPipeline(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        Option<ProgramId>,
        String,
        String,
        // TODO: Somehow, deriving Arbitrary for GlobalPipelineConfig isn't visible
        // to the Arbitrary trait implementation here.
        // We'll prepare the struct ourselves from its constituent parts
        #[proptest(strategy = "runtime_config()")] RuntimeConfig,
        Option<Vec<AttachedConnector>>,
    ),
    UpdatePipeline(
        TenantId,
        PipelineId,
        Option<ProgramId>,
        String,
        String,
        #[proptest(strategy = "option_runtime_config()")] Option<RuntimeConfig>,
        Option<Vec<AttachedConnector>>,
    ),
    UpdatePipelineRuntimeState(TenantId, PipelineId, PipelineRuntimeState),
    SetPipelineDesiredStatus(TenantId, PipelineId, PipelineStatus),
    DeletePipeline(TenantId, PipelineId),
    GetPipelineById(TenantId, PipelineId),
    GetPipelineByName(TenantId, String),
    GetPipelineDescrById(TenantId, PipelineId),
    GetPipelineDescrByName(TenantId, String),
    GetPipelineRuntimeState(TenantId, PipelineId),
    ListPipelines(TenantId),
    NewConnector(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        // TODO: Should be ConnectorConfig.
        #[proptest(strategy = "limited_connector()")] ConnectorConfig,
    ),
    ListConnectors(TenantId),
    GetConnectorById(TenantId, ConnectorId),
    GetConnectorByName(TenantId, String),
    UpdateConnector(
        TenantId,
        ConnectorId,
        String,
        String,
        #[proptest(strategy = "limited_option_connector()")] Option<ConnectorConfig>,
    ),
    DeleteConnector(TenantId, ConnectorId),
    NewService(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        #[proptest(strategy = "limited_service_config()")] ServiceConfig,
    ),
    ListServices(TenantId),
    GetServiceById(TenantId, ServiceId),
    GetServiceByName(TenantId, String),
    UpdateService(
        TenantId,
        ServiceId,
        String,
        #[proptest(strategy = "limited_option_service_config()")] Option<ServiceConfig>,
    ),
    DeleteService(TenantId, ServiceId),
    StoreApiKeyHash(TenantId, String, Vec<ApiPermission>),
    ValidateApiKey(TenantId, String),
    CreatePipelineRevision(
        #[proptest(strategy = "limited_uuid()")] Uuid,
        TenantId,
        PipelineId,
    ),
    GetCommittedPipeline(TenantId, PipelineId),
}

fn check_responses<T: Debug + PartialEq>(step: usize, model: DBResult<T>, impl_: DBResult<T>) {
    match (model, impl_) {
        (Ok(mr), Ok(ir)) => assert_eq!(
            mr, ir,
            "mismatch detected with model (left) and right (impl)"
        ),
        (Err(me), Ok(ir)) => {
            panic!("Step({step}): model returned error: {me:?}, but impl returned result: {ir:?}");
        }
        (Ok(mr), Err(ie)) => {
            panic!("Step({step}): model returned result: {mr:?}, but impl returned error: {ie:?}");
        }
        (Err(me), Err(ie)) => {
            assert_eq!(
                me.to_string(),
                ie.to_string(),
                "Step({step}): Error return mismatch"
            );
        }
    }
}

// Compare everything except the `created` field which gets set inside the DB..
fn compare_pipeline(step: usize, model: DBResult<Pipeline>, impl_: DBResult<Pipeline>) {
    match (model, impl_) {
        (Ok(mut mr), Ok(ir)) => {
            mr.state.created = ir.state.created;
            mr.state.status_since = ir.state.status_since;
            assert_eq!(
                mr, ir,
                "mismatch detected with model (left) and right (impl)"
            );
        }
        (Err(me), Ok(ir)) => {
            panic!("Step({step}): model returned error: {me:?}, but impl returned result: {ir:?}");
        }
        (Ok(mr), Err(ie)) => {
            panic!("Step({step}): model returned result: {mr:?}, but impl returned error: {ie:?}");
        }
        (Err(me), Err(ie)) => {
            assert_eq!(
                me.to_string(),
                ie.to_string(),
                "Step({step}): Error return mismatch"
            );
        }
    }
}

fn compare_pipeline_runtime_state(
    step: usize,
    model: DBResult<PipelineRuntimeState>,
    impl_: DBResult<PipelineRuntimeState>,
) {
    match (model, impl_) {
        (Ok(mut mr), Ok(ir)) => {
            mr.created = ir.created;
            mr.status_since = ir.status_since;
            assert_eq!(
                mr, ir,
                "mismatch detected with model (left) and right (impl)"
            );
        }
        (Err(me), Ok(ir)) => {
            panic!("Step({step}): model returned error: {me:?}, but impl returned result: {ir:?}");
        }
        (Ok(mr), Err(ie)) => {
            panic!("Step({step}): model returned result: {mr:?}, but impl returned error: {ie:?}");
        }
        (Err(me), Err(ie)) => {
            assert_eq!(
                me.to_string(),
                ie.to_string(),
                "Step({step}): Error return mismatch"
            );
        }
    }
}

/// Compare everything except the `created` field which gets set inside the DB.
fn compare_pipelines(mut model_response: Vec<Pipeline>, mut impl_response: Vec<Pipeline>) {
    assert_eq!(
        model_response
            .iter_mut()
            .map(|p| {
                p.state.created =
                    DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::MIN, Utc);
                p.state.status_since =
                    DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::MIN, Utc);
            })
            .collect::<Vec<_>>(),
        impl_response
            .iter_mut()
            .map(|p| {
                p.state.created =
                    DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::MIN, Utc);
                p.state.status_since =
                    DateTime::<Utc>::from_naive_utc_and_offset(NaiveDateTime::MIN, Utc);
            })
            .collect::<Vec<_>>()
    );
}

async fn create_tenants_if_not_exists(
    model: &Mutex<DbModel>,
    handle: &DbHandle,
    tenant_id: TenantId,
) -> DBResult<()> {
    let mut m = model.lock().await;
    if !m.tenants.contains_key(&tenant_id) {
        let rec = TenantRecord {
            id: tenant_id,
            tenant: Uuid::now_v7().to_string(),
            provider: Uuid::now_v7().to_string(),
        };
        m.tenants.insert(tenant_id, rec.clone());
        handle
            .db
            .pool
            .get()
            .await
            .unwrap()
            .execute(
                "INSERT INTO tenant VALUES ($1, $2, $3)",
                &[&rec.id.0, &rec.tenant, &rec.provider],
            )
            .await?;
    }
    Ok(())
}

/// Compare the database storage implementation with our model using in-memory
/// data-structures. Ideally, the two behave the same.
#[test]
fn db_impl_behaves_like_model() {
    let _r = env_logger::try_init();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.block_on(async { test_setup().await });

    // We use the lower-level proptest API `TestRunner` here because if we use
    // `proptest!` it was very difficult to get drop() called on `handle` (I
    // tried putting it in Tokio OnceCell, std OnceCell, and static_init with
    // Finaly); none of those options called drop on handle when the program
    // ended which meant the DB would not shut-down after this test.
    let mut config = Config::default();
    config.max_shrink_iters = u32::MAX;
    config.source_file = Some("src/db/test.rs");
    let mut runner = TestRunner::new(config);
    let res = runner
        .run(
            &prop::collection::vec(any::<StorageAction>(), 0..256),
            |actions| {
                let model = Mutex::new(DbModel::default());
                runtime.block_on(async {
                    // We empty all tables in the database before each test
                    // (with TRUNCATE TABLE). We also reset the sequence ids
                    // (with RESTART IDENTITY)
                    handle
                        .db
                        .pool.get().await.unwrap()
                        .execute("DO $$ DECLARE r RECORD;
                            BEGIN
                                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname =current_schema()) LOOP
                                EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' RESTART IDENTITY CASCADE';
                                END LOOP;
                            END $$;",
                            &[],
                        )
                        .await
                        .unwrap();

                    for (i, action) in actions.into_iter().enumerate() {
                        match action {
                            StorageAction::ListPrograms(tenant_id, with_code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_programs(tenant_id, with_code).await;
                                let impl_response = handle.db.list_programs(tenant_id, with_code).await;
                                if model_response.is_ok() && impl_response.is_ok() {
                                    let m = model_response.unwrap();
                                    let mut i = impl_response.unwrap();
                                    // Impl does not guarantee order of rows returned by SELECT
                                    i.sort_by(|a, b| a.program_id.cmp(&b.program_id));
                                    assert_eq!(m, i);
                                } else {
                                    check_responses(i, model_response, impl_response);
                                }
                            }
                            StorageAction::NewProgram(tenant_id, id, name, description, code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_program(tenant_id, id, &name, &description, &code).await;
                                let impl_response =
                                    handle.db.new_program(tenant_id, id, &name, &description, &code).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateProgram(tenant_id, program_id, name, description, code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model
                                    .update_program(tenant_id, program_id, &name, &description, &code)
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_program(tenant_id, program_id, &name, &description, &code)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetProgramIfExists(tenant_id,program_id, with_code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_program_if_exists(tenant_id, program_id, with_code).await;
                                let impl_response =
                                    handle.db.get_program_if_exists(tenant_id, program_id, with_code).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::LookupProgram(tenant_id, name, with_code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.lookup_program(tenant_id, &name, with_code).await;
                                let impl_response = handle.db.lookup_program(tenant_id, &name, with_code).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProgramForCompilation(tenant_id, program_id, version, status) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.set_program_for_compilation(tenant_id, program_id, version, status.clone()).await;
                                let impl_response =
                                    handle.db.set_program_for_compilation(tenant_id, program_id, version, status.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProgramStatusGuarded(tenant_id, program_id, version, status) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model
                                    .set_program_status_guarded(tenant_id, program_id, version, status.clone())
                                    .await;
                                let impl_response = handle
                                    .db
                                    .set_program_status_guarded(tenant_id, program_id, version, status.clone())
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProgramSchema(tenant_id,program_id, schema) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.set_program_schema(tenant_id, program_id, schema.clone()).await;
                                let impl_response =
                                    handle.db.set_program_schema(tenant_id, program_id, schema).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteProgram(tenant_id, program_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_program(tenant_id, program_id).await;
                                let impl_response = handle.db.delete_program(tenant_id, program_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::AllPrograms => {
                                let mut model_response = model.all_programs().await.unwrap();
                                let mut impl_response = handle.db.all_programs().await.unwrap();
                                model_response.sort_by(|a, b| a.1.program_id.cmp(&b.1.program_id));
                                impl_response.sort_by(|a, b| a.1.program_id.cmp(&b.1.program_id));
                                check_responses(i, Ok(model_response), Ok(impl_response));
                            }
                            StorageAction::NextJob => {
                                let model_response = model.next_job().await;
                                let impl_response = handle.db.next_job().await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineById(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_by_id(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_by_id(tenant_id, pipeline_id).await;
                                compare_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineByName(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_by_name(tenant_id, name.clone()).await;
                                let impl_response = handle.db.get_pipeline_by_name(tenant_id, name).await;
                                compare_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineDescrById(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_descr_by_id(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_descr_by_id(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineDescrByName(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_descr_by_name(tenant_id, name.clone()).await;
                                let impl_response = handle.db.get_pipeline_descr_by_name(tenant_id, name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineRuntimeState(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_runtime_state(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_runtime_state(tenant_id, pipeline_id).await;
                                compare_pipeline_runtime_state(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelines(tenant_id,) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_pipelines(tenant_id, ).await.unwrap();
                                let mut impl_response = handle.db.list_pipelines(tenant_id, ).await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.descriptor.pipeline_id.cmp(&b.descriptor.pipeline_id));
                                compare_pipelines(model_response, impl_response);
                            }
                            StorageAction::NewPipeline(tenant_id, id, program_id, name, description, config, connectors) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_pipeline(tenant_id, id, program_id, &name, &description, &config, &connectors.clone()).await;
                                let impl_response =
                                    handle.db.new_pipeline(tenant_id, id, program_id, &name, &description, &config, &connectors).await;
                                    check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdatePipeline(tenant_id,pipeline_id, program_id, name, description, config, connectors) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model
                                    .update_pipeline(tenant_id, pipeline_id, program_id, &name, &description, &config, &connectors.clone())
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_pipeline(tenant_id, pipeline_id, program_id, &name, &description, &config, &connectors)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdatePipelineRuntimeState(tenant_id, pipeline_id, state) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.update_pipeline_runtime_state(tenant_id, pipeline_id, &state).await;
                                let impl_response = handle.db.update_pipeline_runtime_state(tenant_id, pipeline_id, &state).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetPipelineDesiredStatus(tenant_id, pipeline_id, status) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.set_pipeline_desired_status(tenant_id, pipeline_id, status).await;
                                let impl_response = handle.db.set_pipeline_desired_status(tenant_id, pipeline_id, status).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeletePipeline(tenant_id,pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_pipeline(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.delete_pipeline(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListConnectors(tenant_id,) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_connectors(tenant_id).await.unwrap();
                                let mut impl_response = handle.db.list_connectors(tenant_id).await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.connector_id.cmp(&b.connector_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::NewConnector(tenant_id, id, name, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_connector(tenant_id, id, &name, &description, &config).await;
                                let impl_response =
                                    handle.db.new_connector(tenant_id, id, &name, &description, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetConnectorById(tenant_id,connector_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_connector_by_id(tenant_id, connector_id).await;
                                let impl_response = handle.db.get_connector_by_id(tenant_id, connector_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetConnectorByName(tenant_id,name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_connector_by_name(tenant_id, name.clone()).await;
                                let impl_response = handle.db.get_connector_by_name(tenant_id, name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateConnector(tenant_id,connector_id, name, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.update_connector(tenant_id, connector_id, &name, &description, &config).await;
                                let impl_response =
                                    handle.db.update_connector(tenant_id, connector_id, &name, &description, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteConnector(tenant_id,connector_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_connector(tenant_id, connector_id).await;
                                let impl_response = handle.db.delete_connector(tenant_id, connector_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::StoreApiKeyHash(tenant_id,key, permissions) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.store_api_key_hash(tenant_id, key.clone(), permissions.clone()).await;
                                let impl_response = handle.db.store_api_key_hash(tenant_id, key.clone(), permissions.clone()).await;
                                check_responses(i, model_response, impl_response);
                            },
                            StorageAction::ValidateApiKey(tenant_id,key) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.validate_api_key(key.clone()).await;
                                let impl_response = handle.db.validate_api_key(key.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::CreatePipelineRevision(new_revision_id, tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.create_pipeline_revision(new_revision_id, tenant_id, pipeline_id).await;
                                let impl_response = handle.db.create_pipeline_revision(new_revision_id, tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetCommittedPipeline(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_last_committed_pipeline_revision(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_last_committed_pipeline_revision(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NewService(tenant_id, id, name, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_service(tenant_id, id, &name, &description, &config).await;
                                let impl_response =
                                    handle.db.new_service(tenant_id, id, &name, &description, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListServices(tenant_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_services(tenant_id).await.unwrap();
                                let mut impl_response = handle.db.list_services(tenant_id).await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.service_id.cmp(&b.service_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::GetServiceById(tenant_id, service_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_service_by_id(tenant_id, service_id).await;
                                let impl_response = handle.db.get_service_by_id(tenant_id, service_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetServiceByName(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_service_by_name(tenant_id, name.clone()).await;
                                let impl_response = handle.db.get_service_by_name(tenant_id, name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateService(tenant_id, service_id, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.update_service(tenant_id, service_id, &description, &config).await;
                                let impl_response =
                                    handle.db.update_service(tenant_id, service_id, &description, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteService(tenant_id, service_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_service(tenant_id, service_id).await;
                                let impl_response = handle.db.delete_service(tenant_id, service_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                        }
                    }
                });
                Ok(())
            },
        );

    if let Err(e) = res {
        panic!("{e:#}");
    }
}

/// The program data
type ProgramData = (ProgramDescr, SystemTime);

/// Our model of the database (uses btrees for tables).
#[derive(Debug, Default)]
struct DbModel {
    // `programs` Format is: (program, code, created)
    pub programs: BTreeMap<(TenantId, ProgramId), ProgramData>,
    pub pipelines: BTreeMap<(TenantId, PipelineId), Pipeline>,
    pub history: BTreeMap<(TenantId, PipelineId), PipelineRevision>,
    pub api_keys: BTreeMap<String, (TenantId, Vec<ApiPermission>)>,
    pub connectors: BTreeMap<(TenantId, ConnectorId), ConnectorDescr>,
    pub services: BTreeMap<(TenantId, ServiceId), ServiceDescr>,
    pub tenants: BTreeMap<TenantId, TenantRecord>,
}

#[async_trait]
impl Storage for Mutex<DbModel> {
    async fn list_programs(
        &self,
        tenant_id: TenantId,
        with_code: bool,
    ) -> DBResult<Vec<ProgramDescr>> {
        let s = self.lock().await;
        Ok(s.programs
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1 .0.clone())
            .map(|p| ProgramDescr {
                code: if with_code { p.code.clone() } else { None },
                ..p
            })
            .collect())
    }

    async fn new_program(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> DBResult<(super::ProgramId, super::Version)> {
        let mut s = self.lock().await;
        if s.programs.keys().any(|k| k.1 == ProgramId(id)) {
            return Err(DBError::unique_key_violation("program_pkey"));
        }
        if s.programs
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1 .0.clone())
            .any(|p| p.name == program_name)
        {
            return Err(DBError::DuplicateName);
        }

        let program_id = ProgramId(id);
        let version = Version(1);

        s.programs.insert(
            (tenant_id, program_id),
            (
                ProgramDescr {
                    program_id,
                    name: program_name.to_owned(),
                    description: program_description.to_owned(),
                    status: ProgramStatus::None,
                    schema: None,
                    version,
                    code: Some(program_code.to_owned()),
                },
                SystemTime::now(),
            ),
        );

        Ok((program_id, version))
    }

    async fn update_program(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
        program_name: &str,
        program_description: &str,
        program_code: &Option<String>,
    ) -> DBResult<super::Version> {
        let mut s = self.lock().await;
        if !s.programs.contains_key(&(tenant_id, program_id)) {
            return Err(DBError::UnknownProgram { program_id });
        }

        if s.programs
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1 .0.clone())
            .any(|p| p.name == program_name && p.program_id != program_id)
        {
            return Err(DBError::DuplicateName);
        }

        s.programs
            .get_mut(&(tenant_id, program_id))
            .map(|(p, _e)| {
                let cur_code = p.code.clone().unwrap();
                p.name = program_name.to_owned();
                p.description = program_description.to_owned();
                if let Some(code) = program_code {
                    if *code != cur_code {
                        p.code = program_code.to_owned();
                        p.version.0 += 1;
                        p.schema = None;
                        p.status = ProgramStatus::None;
                    }
                }
                p.version
            })
            .ok_or(DBError::UnknownProgram { program_id })
    }

    async fn get_program_if_exists(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
        with_code: bool,
    ) -> DBResult<Option<ProgramDescr>> {
        let s = self.lock().await;
        Ok(s.programs
            .get(&(tenant_id, program_id))
            .map(|(p, _)| p.clone())
            .map(|p| ProgramDescr {
                code: if with_code { p.code.clone() } else { None },
                ..p
            }))
    }

    async fn lookup_program(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        with_code: bool,
    ) -> DBResult<Option<ProgramDescr>> {
        let s = self.lock().await;
        Ok(s.programs
            .iter()
            .filter(|k: &(&(TenantId, ProgramId), &ProgramData)| k.0 .0 == tenant_id)
            .map(|k| k.1 .0.clone())
            .find(|p| p.name == program_name)
            .map(|p| ProgramDescr {
                code: if with_code { p.code.clone() } else { None },
                ..p
            }))
    }

    async fn set_program_for_compilation(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
        expected_version: super::Version,
        status: ProgramStatus,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        let _r = s.programs.get_mut(&(tenant_id, program_id)).map(|(p, t)| {
            if p.version == expected_version {
                p.status = status;
                *t = SystemTime::now();
                p.schema = None;
            }
        });

        Ok(())
    }

    async fn set_program_status_guarded(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
        expected_version: super::Version,
        status: ProgramStatus,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        s.programs
            .get_mut(&(tenant_id, program_id))
            .map(|(p, t)| {
                if p.version == expected_version {
                    p.status = status;
                    *t = SystemTime::now();
                }
            })
            .ok_or(DBError::UnknownProgram { program_id })
    }

    async fn set_program_schema(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
        schema: ProgramSchema,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        let _r = s.programs.get_mut(&(tenant_id, program_id)).map(|(p, _)| {
            p.schema = Some(schema);
        });

        Ok(())
    }

    async fn delete_program(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        // Foreign key delete:
        let found = s
            .pipelines
            .iter()
            .filter(|&c| c.0 .0 == tenant_id)
            .any(|c| c.1.descriptor.program_id == Some(program_id));

        if found {
            Err(DBError::ProgramInUseByPipeline { program_id })
        } else {
            s.programs
                .remove(&(tenant_id, program_id))
                .map(|_| ())
                .ok_or(DBError::UnknownProgram { program_id })?;

            Ok(())
        }
    }

    async fn all_programs(&self) -> DBResult<Vec<(TenantId, ProgramDescr)>> {
        let s = self.lock().await;
        Ok(s.programs
            .iter()
            .map(|k| {
                (
                    k.0 .0,
                    ProgramDescr {
                        code: None,
                        ..k.1 .0.clone()
                    },
                )
            })
            .collect())
    }

    async fn all_pipelines(&self) -> DBResult<Vec<(TenantId, PipelineId)>> {
        let s = self.lock().await;
        Ok(s.pipelines.iter().map(|k| (k.0 .0, k.0 .1)).collect())
    }

    async fn next_job(
        &self,
    ) -> DBResult<Option<(super::TenantId, super::ProgramId, super::Version)>> {
        let s = self.lock().await;
        let mut values: Vec<(&(TenantId, ProgramId), &ProgramData)> =
            Vec::from_iter(s.programs.iter());
        values.sort_by(|(_, t1), (_, t2)| t1.1.cmp(&t2.1));

        values
            .iter()
            .find(|(_, v)| v.0.status == ProgramStatus::Pending)
            .map(|(k, v)| Ok(Some((k.0, v.0.program_id, v.0.version))))
            .unwrap_or(Ok(None))
    }

    async fn create_pipeline_revision(
        &self,
        new_revision_id: Uuid,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> DBResult<Revision> {
        let mut s = self.lock().await;

        let pipeline = s
            .pipelines
            .get(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        if let Some(program_id) = pipeline.descriptor.program_id {
            let program_data = s
                .programs
                .get(&(tenant_id, program_id))
                .ok_or(DBError::UnknownProgram { program_id })?;
            let connectors = s
                .connectors
                .values()
                .filter(|c| {
                    pipeline
                        .descriptor
                        .attached_connectors
                        .iter()
                        .any(|ac| ac.connector_id == c.connector_id)
                })
                .cloned()
                .collect::<Vec<ConnectorDescr>>();

            let pipeline = pipeline.descriptor.clone();
            let connectors = connectors.clone();
            let program_data = program_data.clone();

            // Gives an answer if the relevant configuration state for the
            // pipeline has changed since our last commit.
            fn detect_changes(
                cur_pipeline: &PipelineDescr,
                cur_sql: &str,
                cur_connectors: &Vec<ConnectorDescr>,
                prev: &PipelineRevision,
            ) -> bool {
                cur_sql != &prev.program.code.clone().unwrap()
                    || cur_pipeline.config != prev.pipeline.config
                    || cur_pipeline
                        .attached_connectors
                        .iter()
                        .map(|ac| (&ac.name, ac.is_input, &ac.relation_name))
                        .ne(prev
                            .pipeline
                            .attached_connectors
                            .iter()
                            .map(|ach| (&ach.name, ach.is_input, &ach.relation_name)))
                    || cur_connectors
                        .iter()
                        .map(|c| &c.config)
                        .ne(prev.connectors.iter().map(|c| &c.config))
            }

            let prev = s.history.get(&(tenant_id, pipeline_id));
            let (has_changed, next_revision) = match prev {
                Some(prev) => (
                    detect_changes(
                        &pipeline,
                        &program_data.0.code.clone().unwrap(),
                        &connectors,
                        prev,
                    ),
                    prev.revision,
                ),
                None => (true, Revision(new_revision_id)),
            };

            if has_changed {
                PipelineRevision::validate(&pipeline, &connectors, &program_data.0)?;
                s.history.insert(
                    (tenant_id, pipeline_id),
                    PipelineRevision::new(
                        next_revision,
                        pipeline,
                        connectors,
                        program_data.0.clone(),
                    ),
                );
                Ok(next_revision)
            } else {
                Err(DBError::RevisionNotChanged)
            }
        } else {
            return Err(DBError::ProgramNotSet);
        }
    }

    async fn get_last_committed_pipeline_revision(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> DBResult<PipelineRevision> {
        let s = self.lock().await;
        let _p = s
            .pipelines
            .get(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;
        let history = s
            .history
            .get(&(tenant_id, pipeline_id))
            .ok_or(DBError::NoRevisionAvailable { pipeline_id })?;
        Ok(history.clone())
    }

    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        program_id: Option<super::ProgramId>,
        pipeline_name: &str,
        pipeline_description: &str,
        config: &RuntimeConfig,
        // TODO: not clear why connectors is an option here
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> DBResult<(super::PipelineId, super::Version)> {
        let mut s = self.lock().await;
        let db_connectors = s.connectors.clone();

        // UUIDs are global
        if s.pipelines.keys().any(|k| k.1 == PipelineId(id)) {
            return Err(DBError::unique_key_violation("pipeline_pkey"));
        }
        // UNIQUE constraint on name
        if let Some(_) = s
            .pipelines
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1)
            .find(|c| c.descriptor.name == pipeline_name)
        {
            return Err(DBError::DuplicateName.into());
        }
        // Model the foreign key constraint on `program_id`
        if let Some(program_id) = program_id {
            if !s.programs.contains_key(&(tenant_id, program_id)) {
                return Err(DBError::UnknownProgram { program_id });
            }
        }

        let mut new_acs: Vec<AttachedConnector> = vec![];
        if let Some(connectors) = connectors {
            for ac in connectors {
                // Check that all attached connectors point to a valid
                // connector_id
                if !db_connectors.contains_key(&(tenant_id, ac.connector_id)) {
                    return Err(DBError::UnknownConnector {
                        connector_id: ac.connector_id,
                    });
                }
                if new_acs.iter().any(|nac| nac.name == ac.name) {
                    return Err(DBError::DuplicateName);
                }

                new_acs.push(ac.clone());
            }
        }

        let pipeline_id = PipelineId(id);
        let version = Version(1);
        s.pipelines.insert(
            (tenant_id, pipeline_id),
            Pipeline {
                descriptor: PipelineDescr {
                    pipeline_id,
                    program_id,
                    name: pipeline_name.to_owned(),
                    description: pipeline_description.to_owned(),
                    config: config.clone(),
                    attached_connectors: new_acs,
                    version: Version(1),
                },
                state: PipelineRuntimeState {
                    location: "".to_string(),
                    desired_status: PipelineStatus::Shutdown,
                    current_status: PipelineStatus::Shutdown,
                    status_since: Utc::now(),
                    error: None,
                    created: Utc::now(),
                },
            },
        );

        Ok((pipeline_id, version))
    }

    async fn update_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_id: Option<ProgramId>,
        pipeline_name: &str,
        pipeline_description: &str,
        config: &Option<RuntimeConfig>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> DBResult<Version> {
        let mut s = self.lock().await;
        let db_connectors = s.connectors.clone();
        let db_programs = s.programs.clone();

        // pipeline must exist
        s.pipelines
            .get_mut(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        let mut new_acs: Vec<AttachedConnector> = vec![];
        if let Some(connectors) = connectors {
            for ac in connectors {
                // Check that all attached connectors point to a valid
                // connector_id
                if !db_connectors.contains_key(&(tenant_id, ac.connector_id)) {
                    return Err(DBError::UnknownConnector {
                        connector_id: ac.connector_id,
                    });
                }
                if new_acs.iter().any(|nac| nac.name == ac.name) {
                    return Err(DBError::DuplicateName);
                }

                new_acs.push(ac.clone());
            }
        } else {
            new_acs = s
                .pipelines
                .get(&(tenant_id, pipeline_id))
                .ok_or(DBError::UnknownPipeline { pipeline_id })?
                .descriptor
                .attached_connectors
                .clone();
        }

        // UNIQUE constraint on name
        if let Some(c) = s
            .pipelines
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1)
            .find(|c| c.descriptor.name == pipeline_name)
        {
            if c.descriptor.pipeline_id != pipeline_id {
                return Err(DBError::DuplicateName.into());
            }
        }

        // Check program exists foreign key constraint
        if let Some(program_id) = program_id {
            if !db_programs.contains_key(&(tenant_id, program_id)) {
                return Err(DBError::UnknownProgram { program_id });
            }
        }

        let c = &mut s
            .pipelines
            .get_mut(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?
            .descriptor;

        // Foreign key constraint on `program_id`
        if let Some(program_id) = program_id {
            if db_programs.contains_key(&(tenant_id, program_id)) {
                c.program_id = Some(program_id);
            } else {
                return Err(DBError::UnknownProgram { program_id });
            }
        } else {
            c.program_id = None;
        }

        c.attached_connectors = new_acs;
        c.name = pipeline_name.to_owned();
        c.description = pipeline_description.to_owned();
        c.version = c.version.increment();
        c.config = config.clone().unwrap_or(c.config.clone());
        Ok(c.version)
    }

    async fn delete_config(
        &self,
        tenant_id: TenantId,
        pipeline_id: super::PipelineId,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        s.pipelines
            .remove(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        Ok(())
    }

    async fn attached_connector_is_input(
        &self,
        _tenant_id: TenantId,
        _pipeline_id: PipelineId,
        _name: &str,
    ) -> DBResult<bool> {
        todo!()
    }

    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_id: super::PipelineId,
    ) -> DBResult<bool> {
        let mut s = self.lock().await;
        let _r = s.history.remove(&(tenant_id, pipeline_id));
        // TODO: Our APIs sometimes are not consistent we return a bool here but
        // other calls fail silently on delete/lookups
        Ok(s.pipelines
            .remove(&(tenant_id, pipeline_id))
            .map(|_| true)
            .unwrap_or(false))
    }

    async fn get_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: super::PipelineId,
    ) -> DBResult<super::Pipeline> {
        let s = self.lock().await;
        s.pipelines
            .get(&(tenant_id, pipeline_id))
            .cloned()
            .ok_or(DBError::UnknownPipeline { pipeline_id })
    }

    async fn get_pipeline_descr_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: super::PipelineId,
    ) -> DBResult<super::PipelineDescr> {
        self.get_pipeline_by_id(tenant_id, pipeline_id)
            .await
            .map(|p| p.descriptor)
    }

    async fn get_pipeline_runtime_state(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRuntimeState, DBError> {
        self.get_pipeline_by_id(tenant_id, pipeline_id)
            .await
            .map(|p| p.state)
    }

    async fn update_pipeline_runtime_state(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        state: &PipelineRuntimeState,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;

        let pipeline = s
            .pipelines
            .get_mut(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        pipeline.state.location = state.location.clone();
        pipeline.state.current_status = state.current_status;
        pipeline.state.status_since = state.status_since;
        pipeline.state.error = state.error.clone();
        pipeline.state.created = state.created;

        Ok(())
    }

    async fn set_pipeline_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        desired_status: PipelineStatus,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;

        s.pipelines
            .get_mut(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?
            .state
            .desired_status = desired_status;

        Ok(())
    }

    async fn get_pipeline_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> DBResult<super::Pipeline> {
        let s = self.lock().await;
        s.pipelines
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|p| p.descriptor.name == name)
            .ok_or(DBError::UnknownName { name })
    }

    async fn get_pipeline_descr_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> DBResult<super::PipelineDescr> {
        self.get_pipeline_by_name(tenant_id, name)
            .await
            .map(|p| p.descriptor)
    }

    async fn list_pipelines(&self, tenant_id: TenantId) -> DBResult<Vec<super::Pipeline>> {
        Ok(self
            .lock()
            .await
            .pipelines
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .collect())
    }

    async fn new_connector(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ConnectorConfig,
    ) -> DBResult<super::ConnectorId> {
        let mut s = self.lock().await;
        if s.connectors.keys().any(|k| k.1 == ConnectorId(id)) {
            return Err(DBError::unique_key_violation("connector_pkey"));
        }
        if s.connectors
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .any(|c| c.name == name)
        {
            return Err(DBError::DuplicateName.into());
        }

        let connector_id = super::ConnectorId(id);
        s.connectors.insert(
            (tenant_id, connector_id),
            ConnectorDescr {
                connector_id,
                name: name.to_owned(),
                description: description.to_owned(),
                config: config.to_owned(),
            },
        );
        Ok(connector_id)
    }

    async fn list_connectors(&self, tenant_id: TenantId) -> DBResult<Vec<ConnectorDescr>> {
        let s = self.lock().await;
        Ok(s.connectors
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .collect())
    }

    async fn get_connector_by_id(
        &self,
        tenant_id: TenantId,
        connector_id: super::ConnectorId,
    ) -> DBResult<ConnectorDescr> {
        let s = self.lock().await;
        s.connectors
            .get(&(tenant_id, connector_id))
            .cloned()
            .ok_or(DBError::UnknownConnector { connector_id })
    }

    async fn get_connector_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> DBResult<ConnectorDescr> {
        let s = self.lock().await;
        s.connectors
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|c| c.name == name)
            .ok_or(DBError::UnknownName { name })
    }

    async fn update_connector(
        &self,
        tenant_id: TenantId,
        connector_id: super::ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<ConnectorConfig>,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        // `connector_id` needs to exist
        if s.connectors.get(&(tenant_id, connector_id)).is_none() {
            return Err(DBError::UnknownConnector { connector_id }.into());
        }
        // UNIQUE constraint on name
        if let Some(c) = s
            .connectors
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1)
            .find(|c| c.name == connector_name)
            .cloned()
        {
            if c.connector_id != connector_id {
                return Err(DBError::DuplicateName.into());
            }
        }

        let c = s
            .connectors
            .get_mut(&(tenant_id, connector_id))
            .ok_or(DBError::UnknownConnector { connector_id })?;
        c.name = connector_name.to_owned();
        c.description = description.to_owned();
        if let Some(config) = config {
            c.config = config.clone();
        }
        Ok(())
    }

    async fn delete_connector(
        &self,
        tenant_id: TenantId,
        connector_id: super::ConnectorId,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        s.connectors
            .remove(&(tenant_id, connector_id))
            .ok_or(DBError::UnknownConnector { connector_id })?;
        s.pipelines.values_mut().for_each(|c| {
            c.descriptor
                .attached_connectors
                .retain(|c| c.connector_id != connector_id);
        });
        Ok(())
    }

    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        key: String,
        permissions: Vec<ApiPermission>,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        if let btree_map::Entry::Vacant(e) = s.api_keys.entry(hash) {
            e.insert((tenant_id, permissions));
            Ok(())
        } else {
            Err(DBError::duplicate_key())
        }
    }

    async fn validate_api_key(&self, key: String) -> DBResult<(TenantId, Vec<ApiPermission>)> {
        let s = self.lock().await;
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let record = s.api_keys.get(&hash);
        match record {
            Some(record) => Ok((record.0, record.1.clone())),
            None => Err(DBError::InvalidKey),
        }
    }

    async fn get_or_create_tenant_id(
        &self,
        _tenant_name: String,
        _provider: String,
    ) -> DBResult<TenantId> {
        todo!("For model-based tests, we generate the TenantID using proptest, as opposed to generating a claim that we then get or create an ID for");
    }

    async fn create_tenant_if_not_exists(
        &self,
        _tenant_id: Uuid,
        _tenant_name: String,
        _provider: String,
    ) -> DBResult<TenantId> {
        todo!("For model-based tests, we generate the TenantID using proptest, as opposed to generating a claim that we then get or create an ID for");
    }

    /// Record information about a compiler binary
    async fn create_compiled_binary_ref(
        &self,
        _program_id: ProgramId,
        _version: Version,
        _url: String,
    ) -> Result<(), DBError> {
        todo!("Unimplemented");
    }

    async fn get_compiled_binary_ref(
        &self,
        _program_id: ProgramId,
        _version: Version,
    ) -> Result<Option<String>, DBError> {
        todo!("Unimplemented");
    }

    async fn delete_compiled_binary_ref(
        &self,
        _program_id: ProgramId,
        _version: Version,
    ) -> Result<(), DBError> {
        todo!("Unimplemented");
    }

    async fn check_connection(&self) -> Result<(), DBError> {
        todo!("Unimplemented");
    }

    async fn new_service(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        description: &str,
        config: &ServiceConfig,
    ) -> Result<ServiceId, DBError> {
        let mut s = self.lock().await;
        if s.services.keys().any(|k| k.1 == ServiceId(id)) {
            return Err(DBError::unique_key_violation("service_pkey"));
        }
        if s.services
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .any(|c| c.name == name)
        {
            return Err(DBError::DuplicateName.into());
        }

        let service_id = super::ServiceId(id);
        s.services.insert(
            (tenant_id, service_id),
            ServiceDescr {
                service_id,
                name: name.to_owned(),
                description: description.to_owned(),
                config: config.to_owned(),
            },
        );
        Ok(service_id)
    }

    async fn list_services(&self, tenant_id: TenantId) -> Result<Vec<ServiceDescr>, DBError> {
        let s = self.lock().await;
        Ok(s.services
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .collect())
    }

    async fn get_service_by_id(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
    ) -> Result<ServiceDescr, DBError> {
        let s = self.lock().await;
        s.services
            .get(&(tenant_id, service_id))
            .cloned()
            .ok_or(DBError::UnknownService { service_id })
    }

    async fn get_service_by_name(
        &self,
        tenant_id: TenantId,
        name: String,
    ) -> Result<ServiceDescr, DBError> {
        let s = self.lock().await;
        s.services
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|c| c.name == name)
            .ok_or(DBError::UnknownName { name })
    }

    async fn update_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        description: &str,
        config: &Option<ServiceConfig>,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;
        // `service_id` needs to exist
        if s.services.get(&(tenant_id, service_id)).is_none() {
            return Err(DBError::UnknownService { service_id }.into());
        }

        let c = s
            .services
            .get_mut(&(tenant_id, service_id))
            .ok_or(DBError::UnknownService { service_id })?;
        c.description = description.to_owned();
        if let Some(config) = config {
            c.config = config.clone();
        }
        Ok(())
    }

    async fn delete_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;
        s.services
            .remove(&(tenant_id, service_id))
            .ok_or(DBError::UnknownService { service_id })?;
        // TODO: deletion cascading / not allowing because foreign keys still exist
        Ok(())
    }
}
