use super::{
    storage::Storage, AttachedConnector, ConnectorDescr, ConnectorId, DBError, PipelineId,
    PipelineRevision, PipelineStatus, ProgramDescr, ProgramId, ProgramStatus, ProjectDB, Revision,
    Version,
};
use super::{
    ApiKeyDescr, ApiKeyId, ApiPermission, Pipeline, PipelineDescr, PipelineRuntimeState,
    ProgramSchema,
};
use crate::auth::{self, TenantId, TenantRecord};
use crate::compiler::ProgramConfig;
use crate::config::CompilationProfile;
use crate::db::pipeline::convert_bigint_to_time;
use crate::db::service::{ServiceProbeDescr, ServiceProbeId};
use crate::db::{ServiceDescr, ServiceId};
use crate::prober::service::{
    ServiceProbeRequest, ServiceProbeResponse, ServiceProbeStatus, ServiceProbeType,
};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use deadpool_postgres::Transaction;
use openssl::sha::{self};
use pipeline_types::config::{ConnectorConfig, TransportConfigVariant};
use pipeline_types::service::{KafkaService, ServiceConfig};
use pipeline_types::{
    config::{ResourceConfig, RuntimeConfig},
    program_schema::Relation,
};
use pretty_assertions::assert_eq;
use proptest::test_runner::{Config, TestRunner};
use proptest::{bool, prelude::*};
use proptest_derive::Arbitrary;
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
    name: kafka_input
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
        )
        .await
        .unwrap();
    let rows = handle.db.list_programs(tenant_id, false).await.unwrap();
    let expected = ProgramDescr {
        program_id: res.0,
        name: "test1".to_string(),
        description: "program desc".to_string(),
        version: res.1,
        status: ProgramStatus::Pending,
        schema: None,
        code: None,
        config: ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
        },
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
        status: ProgramStatus::Pending,
        schema: None,
        code: Some("ignored".to_string()),
        config: ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
        },
    };
    let actual = rows.get(0).unwrap();
    assert_eq!(1, rows.len());
    assert_eq!(&expected, actual);

    let rows = handle.db.all_programs().await.unwrap();
    let expected = ProgramDescr {
        program_id: res.0,
        name: "test1".to_string(),
        description: "program desc".to_string(),
        version: res.1,
        status: ProgramStatus::Pending,
        schema: None,
        code: None,
        config: ProgramConfig {
            profile: Some(CompilationProfile::Unoptimized),
        },
    };
    let (_, actual) = rows.get(0).unwrap();
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
        )
        .await
        .unwrap();
    let _ = handle
        .db
        .update_program(
            tenant_id,
            program_id,
            &Some("test2".to_string()),
            &Some("different desc".to_string()),
            &Some("create table t2(c2 integer);".to_string()),
            &None,
            &None,
            &None,
            None,
            None,
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
            &Some("updated_test1".to_string()),
            &Some("some new description".to_string()),
            &None,
            &None,
            &None,
            &None,
            None,
            None,
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_by_id(tenant_id, program_id, false)
        .await
        .unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle
        .db
        .get_program_by_name(tenant_id, "test1", false, None)
        .await
        .unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle
        .db
        .get_program_by_name(tenant_id, "test2", false, None)
        .await;
    assert!(desc.is_err());
}

#[tokio::test]
async fn pipeline_config() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    handle
        .db
        .new_connector(
            tenant_id,
            Uuid::now_v7(),
            "a",
            "b",
            &test_connector_config(),
            None,
        )
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_name: "a".to_string(),
        relation_name: "".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    let _ = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            &None,
            "1",
            "2",
            &rc,
            &Some(vec![ac.clone()]),
            None,
        )
        .await
        .unwrap();
    let res = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(1, res.len());
    let config = res.get(0).unwrap();
    assert_eq!("1", config.descriptor.name);
    assert_eq!("2", config.descriptor.description);
    assert_eq!(rc, config.descriptor.config);
    assert_eq!(None, config.descriptor.program_name);
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
        )
        .await
        .unwrap();

    handle
        .db
        .set_program_status_guarded(tenant_id, uid2, v2, ProgramStatus::Pending)
        .await
        .unwrap();
    handle
        .db
        .set_program_status_guarded(tenant_id, uid1, v1, ProgramStatus::Pending)
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_by_id(tenant_id, program_id, false)
        .await
        .unwrap();
    assert_eq!(ProgramStatus::Pending, desc.status);
    handle
        .db
        .set_program_status_guarded(
            tenant_id,
            program_id,
            desc.version,
            ProgramStatus::CompilingRust,
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_by_id(tenant_id, program_id, false)
        .await
        .unwrap();
    assert_eq!(ProgramStatus::CompilingRust, desc.status);
}

#[tokio::test]
async fn duplicate_attached_conn_name() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    handle
        .db
        .new_connector(
            tenant_id,
            Uuid::now_v7(),
            "a",
            "b",
            &test_connector_config(),
            None,
        )
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_name: "a".to_string(),
        relation_name: "".to_string(),
    };
    let ac2 = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_name: "a".to_string(),
        relation_name: "".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    let _ = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            &None,
            "1",
            "2",
            &rc,
            &Some(vec![ac, ac2]),
            None,
        )
        .await
        .expect_err("duplicate attached connector name");
}

#[tokio::test]
async fn update_conn_name() {
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
            None,
        )
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_name: "a".to_string(),
        relation_name: "".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            &None,
            "1",
            "2",
            &rc,
            &Some(vec![ac]),
            None,
        )
        .await
        .unwrap();
    handle
        .db
        .update_connector(
            tenant_id,
            connector_id,
            &Some("not-a"),
            &Some("b"),
            &None,
            None,
        )
        .await
        .unwrap();
    let pipeline = handle
        .db
        .get_pipeline_by_name(tenant_id, "1")
        .await
        .unwrap();
    assert_eq!(
        "not-a".to_string(),
        pipeline
            .descriptor
            .attached_connectors
            .get(0)
            .unwrap()
            .connector_name
    );
}

#[tokio::test]
async fn save_api_key() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    // Attempt several key generations and validations
    for i in 1..10 {
        let api_key = auth::generate_api_key();
        handle
            .db
            .store_api_key_hash(
                tenant_id,
                Uuid::now_v7(),
                &format!("foo-{}", i),
                &api_key,
                vec![ApiPermission::Read, ApiPermission::Write],
            )
            .await
            .unwrap();
        let scopes = handle.db.validate_api_key(&api_key).await.unwrap();
        assert_eq!(tenant_id, scopes.0);
        assert_eq!(&ApiPermission::Read, scopes.1.get(0).unwrap());
        assert_eq!(&ApiPermission::Write, scopes.1.get(1).unwrap());

        let api_key_2 = auth::generate_api_key();
        let err = handle.db.validate_api_key(&api_key_2).await.unwrap_err();
        assert!(matches!(err, DBError::InvalidKey));
    }
}

/// A Function that commits twice and checks the second time errors, returns
/// revision of first commit.
async fn commit_check(handle: &DbHandle, tenant_id: TenantId, pipeline_id: PipelineId) -> Revision {
    let new_revision_id = Uuid::now_v7();
    let r = handle
        .db
        .create_pipeline_deployment(new_revision_id, tenant_id, pipeline_id)
        .await
        .unwrap();
    assert_eq!(r.0, new_revision_id);

    // We get an error the 2nd time since nothing changed
    let e = handle
        .db
        .create_pipeline_deployment(new_revision_id, tenant_id, pipeline_id)
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
        .new_program(
            tenant_id,
            Uuid::now_v7(),
            "",
            "",
            "",
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
        )
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
            &Some("".to_string()),
            "",
            "",
            &rc,
            &None,
            None,
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
            &ProgramConfig {
                profile: Some(CompilationProfile::Unoptimized),
            },
            None,
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
                inputs: vec![Relation::new("t1", false, vec![])],
                outputs: vec![Relation::new("v1", false, vec![])],
            },
        )
        .await
        .unwrap();
    let config1 = test_connector_config();
    let mut config2 = config1.clone();
    config2.max_queued_records = config1.max_queued_records + 5;

    let connector_id1: ConnectorId = handle
        .db
        .new_connector(tenant_id, Uuid::now_v7(), "a", "b", &config1, None)
        .await
        .unwrap();
    let mut ac1 = AttachedConnector {
        name: "ac1".to_string(),
        is_input: true,
        connector_name: "a".to_string(),
        relation_name: "t1".to_string(),
    };
    handle
        .db
        .new_connector(tenant_id, Uuid::now_v7(), "d", "e", &config2, None)
        .await
        .unwrap();
    let mut ac2 = AttachedConnector {
        name: "ac2".to_string(),
        is_input: false,
        connector_name: "d".to_string(),
        relation_name: "v1".to_string(),
    };
    let rc = RuntimeConfig::from_yaml("");
    let (pipeline_id, _version) = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            &Some("test1".to_string()),
            "1",
            "2",
            &rc,
            &Some(vec![ac1.clone(), ac2.clone()]),
            None,
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
            &None,
            &None,
            &Some("only schema matters--this isn't compiled2".to_string()),
            &None,
            &None,
            &None,
            None,
            None,
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
                    Relation::new("t1", false, vec![]),
                    Relation::new("t2", false, vec![]),
                ],
                outputs: vec![Relation::new("v1", false, vec![])],
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
                inputs: vec![Relation::new("tnew1", false, vec![])],
                outputs: vec![Relation::new("vnew1", false, vec![])],
            },
        )
        .await
        .unwrap();
    // This doesn't work because the connectors reference (now invalid) tables:
    assert!(handle
        .db
        .create_pipeline_deployment(Uuid::now_v7(), tenant_id, pipeline_id)
        .await
        .is_err());
    ac1.is_input = true;
    ac1.relation_name = "tnew1".into();
    let gp_config = RuntimeConfig {
        workers: 1,
        cpu_profiler: true,
        storage: false,
        tcp_metrics_exporter: false,
        min_batch_size_records: 0,
        max_buffering_delay_usecs: 0,
        resources: ResourceConfig::default(),
        min_storage_rows: None,
    };
    handle
        .db
        .update_pipeline(
            tenant_id,
            pipeline_id,
            &Some("test1".into()),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone(), ac2.clone()]),
            None,
        )
        .await
        .unwrap();
    // This doesn't work because the ac2 still references a wrong table
    assert!(handle
        .db
        .create_pipeline_deployment(Uuid::now_v7(), tenant_id, pipeline_id)
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
            &Some("test1".into()),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone(), ac2.clone()]),
            None,
        )
        .await
        .unwrap();

    // Now we can commit again
    let r3 = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r2, r3, "we got a new revision");

    // If we change the connector we can commit again:
    let mut config3 = config2.clone();
    config3.max_queued_records += 5;
    handle
        .db
        .update_connector(
            tenant_id,
            connector_id1,
            &Some("a"),
            &Some("b"),
            &Some(config3),
            None,
        )
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
            &Some("test1".into()),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone(), ac2.clone()]),
            None,
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
            &Some("test1".into()),
            "1",
            "2",
            &Some(gp_config.clone()),
            &Some(vec![ac1.clone()]),
            None,
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
            &Some("test1".into()),
            "1",
            "2",
            &Some(RuntimeConfig {
                workers: gp_config.workers + 1,
                ..gp_config
            }),
            &Some(vec![ac1.clone()]),
            None,
        )
        .await
        .unwrap();
    let r6 = commit_check(&handle, tenant_id, pipeline_id).await;
    assert_ne!(r5, r6, "we got a new revision");
}

#[tokio::test]
async fn service_name_change() {
    let _r = env_logger::try_init();

    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;

    let some_config = ServiceConfig::Kafka(KafkaService {
        bootstrap_servers: vec![
            format!("example.example.example:1234"),
            "example.example:5678".to_string(),
        ],
        options: Default::default(),
    });

    handle
        .db
        .new_service(
            tenant_id,
            Uuid::now_v7(),
            "service1",
            "service1 description",
            &some_config,
            None,
        )
        .await
        .unwrap();
    handle
        .db
        .new_service(
            tenant_id,
            Uuid::now_v7(),
            "service2",
            "service1 description",
            &some_config,
            None,
        )
        .await
        .unwrap();
    match handle
        .db
        .update_service_by_name(tenant_id, "service1", &Some("service2"), &None, &None)
        .await
    {
        Ok(_) => {
            panic!("Expected duplicate name error but instead got success")
        }
        Err(err) => match err {
            DBError::DuplicateName => {}
            _ => {
                panic!("Expected duplicate name error but instead got: {:?}", err);
            }
        },
    }
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
                    name: kafka_input
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
                    name: kafka_input
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

/// Generate a service type
pub(crate) fn limited_service_config() -> impl Strategy<Value = ServiceConfig> {
    any::<u8>().prop_map(|byte| {
        ServiceConfig::Kafka(KafkaService {
            bootstrap_servers: vec![
                format!("example.example{byte}.example:1234"),
                "example.example:5678".to_string(),
            ],
            options: Default::default(),
        })
    })
}

/// Generate an optional service type
pub(crate) fn limited_option_service_config() -> impl Strategy<Value = Option<ServiceConfig>> {
    any::<Option<u8>>().prop_map(|byte| {
        byte.map(|b| {
            ServiceConfig::Kafka(KafkaService {
                bootstrap_servers: vec![format!("example.com:{b}")],
                options: Default::default(),
            })
        })
    })
}

/// Generate an optional list limit
pub(crate) fn limited_option_list_limit() -> impl Strategy<Value = Option<u32>> {
    any::<Option<u8>>().prop_map(|byte| byte.map(|b| (b % 5) as u32))
}

/// Generate an optional service probe request type including possibly invalid
pub(crate) fn limited_option_service_probe_type() -> impl Strategy<Value = Option<ServiceProbeType>>
{
    any::<Option<u8>>().prop_map(|byte| {
        byte.map(|b| {
            if b <= 127 {
                ServiceProbeRequest::TestConnectivity.probe_type()
            } else {
                ServiceProbeRequest::KafkaGetTopics.probe_type()
            }
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
        bool,
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
        storage: config.4,
        tcp_metrics_exporter: false,
        resources: ResourceConfig {
            cpu_cores_min: config.5,
            cpu_cores_max: config.6,
            memory_mb_min: config.7,
            memory_mb_max: config.8,
            storage_mb_max: config.9,
            storage_class: None,
        },
        min_storage_rows: None,
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
            bool,
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
            storage: config.4,
            tcp_metrics_exporter: false,
            resources: ResourceConfig {
                cpu_cores_min: config.5,
                cpu_cores_max: config.6,
                memory_mb_min: config.7,
                memory_mb_max: config.8,
                storage_mb_max: config.9,
                storage_class: None,
            },
            min_storage_rows: None,
        })
    })
}

/// Generate different program configurations
pub(crate) fn program_config() -> impl Strategy<Value = ProgramConfig> {
    any::<(bool, bool)>().prop_map(|config| ProgramConfig {
        profile: if config.0 {
            None
        } else {
            if config.1 {
                Some(CompilationProfile::Unoptimized)
            } else {
                Some(CompilationProfile::Optimized)
            }
        },
    })
}

/// Generate different program configurations
pub(crate) fn option_program_config() -> impl Strategy<Value = Option<ProgramConfig>> {
    any::<Option<(bool, bool)>>().prop_map(|c| {
        c.map(|config| ProgramConfig {
            profile: if config.0 {
                None
            } else {
                if config.1 {
                    Some(CompilationProfile::Unoptimized)
                } else {
                    Some(CompilationProfile::Optimized)
                }
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
        #[proptest(strategy = "program_config()")] ProgramConfig,
    ),
    UpdateProgram(
        TenantId,
        ProgramId,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<Version>,
        #[proptest(strategy = "option_program_config()")] Option<ProgramConfig>,
    ),
    GetProgramById(TenantId, ProgramId, bool),
    GetProgramByName(TenantId, String, bool),
    SetProgramStatusGuarded(TenantId, ProgramId, Version, ProgramStatus),
    SetProgramSchema(TenantId, ProgramId, ProgramSchema),
    DeleteProgram(TenantId, String),
    AllPrograms,
    NextJob,
    NewPipeline(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        Option<String>,
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
        Option<String>,
        String,
        String,
        #[proptest(strategy = "option_runtime_config()")] Option<RuntimeConfig>,
        Option<Vec<AttachedConnector>>,
    ),
    UpdatePipelineRuntimeState(TenantId, PipelineId, PipelineRuntimeState),
    SetPipelineDesiredStatus(TenantId, PipelineId, PipelineStatus),
    DeletePipeline(TenantId, String),
    GetPipelineById(TenantId, PipelineId),
    GetPipelineByName(TenantId, String),
    GetPipelineDescrById(TenantId, PipelineId),
    GetPipelineDescrByName(TenantId, String),
    GetPipelineRuntimeStateById(TenantId, PipelineId),
    GetPipelineRuntimeStateByName(TenantId, String),
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
        Option<String>,
        Option<String>,
        #[proptest(strategy = "limited_option_connector()")] Option<ConnectorConfig>,
    ),
    DeleteConnector(TenantId, String),
    NewService(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        #[proptest(strategy = "limited_service_config()")] ServiceConfig,
    ),
    ListServices(TenantId, Option<String>),
    GetServiceById(TenantId, ServiceId),
    GetServiceByName(TenantId, String),
    UpdateService(
        TenantId,
        ServiceId,
        Option<String>,
        Option<String>,
        #[proptest(strategy = "limited_option_service_config()")] Option<ServiceConfig>,
    ),
    DeleteService(TenantId, String),
    NewServiceProbe(
        TenantId,
        ServiceId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        ServiceProbeRequest,
        #[cfg_attr(test, proptest(value = "Utc::now()"))] DateTime<Utc>,
    ),
    UpdateServiceProbeSetRunning(
        TenantId,
        ServiceProbeId,
        #[cfg_attr(test, proptest(value = "Utc::now()"))] DateTime<Utc>,
    ),
    UpdateServiceProbeSetFinished(
        TenantId,
        ServiceProbeId,
        ServiceProbeResponse,
        #[cfg_attr(test, proptest(value = "Utc::now()"))] DateTime<Utc>,
    ),
    NextServiceProbe,
    GetServiceProbe(TenantId, ServiceId, ServiceProbeId),
    ListServiceProbes(
        TenantId,
        ServiceId,
        #[proptest(strategy = "limited_option_list_limit()")] Option<u32>,
        #[proptest(strategy = "limited_option_service_probe_type()")] Option<ServiceProbeType>,
    ),
    ListApiKeys(TenantId),
    GetApiKey(TenantId, String),
    DeleteApiKey(TenantId, String),
    StoreApiKeyHash(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        Vec<ApiPermission>,
    ),
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
                            StorageAction::NewProgram(tenant_id, id, name, description, code, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_program(tenant_id, id, &name, &description, &code, &config, None).await;
                                let impl_response =
                                    handle.db.new_program(tenant_id, id, &name, &description, &code, &config, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateProgram(tenant_id, program_id, name, description, code, guard, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model
                                    .update_program(tenant_id, program_id, &name, &description, &code, &None, &None, &config, guard, None)
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_program(tenant_id, program_id, &name, &description, &code, &None, &None, &config, guard, None)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetProgramById(tenant_id,program_id, with_code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_program_by_id(tenant_id, program_id, with_code).await;
                                let impl_response =
                                    handle.db.get_program_by_id(tenant_id, program_id, with_code).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetProgramByName(tenant_id, name, with_code) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_program_by_name(tenant_id, &name, with_code, None).await;
                                let impl_response = handle.db.get_program_by_name(tenant_id, &name, with_code, None).await;
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
                            StorageAction::DeleteProgram(tenant_id, program_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_program(tenant_id, &program_name).await;
                                let impl_response = handle.db.delete_program(tenant_id, &program_name).await;
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
                                let model_response = model.get_pipeline_by_name(tenant_id, &name).await;
                                let impl_response = handle.db.get_pipeline_by_name(tenant_id, &name).await;
                                compare_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineDescrById(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_descr_by_id(tenant_id, pipeline_id, None).await;
                                let impl_response = handle.db.get_pipeline_descr_by_id(tenant_id, pipeline_id, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineDescrByName(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_descr_by_name(tenant_id, &name, None).await;
                                let impl_response = handle.db.get_pipeline_descr_by_name(tenant_id, &name, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineRuntimeStateById(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_runtime_state_by_id(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_runtime_state_by_id(tenant_id, pipeline_id).await;
                                compare_pipeline_runtime_state(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineRuntimeStateByName(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_runtime_state_by_name(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.get_pipeline_runtime_state_by_name(tenant_id, &pipeline_name).await;
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
                            StorageAction::NewPipeline(tenant_id, id, program_name, name, description, config, connectors) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_pipeline(tenant_id, id, &program_name, &name, &description, &config, &connectors.clone(), None).await;
                                let impl_response =
                                    handle.db.new_pipeline(tenant_id, id, &program_name, &name, &description, &config, &connectors, None).await;
                                    check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdatePipeline(tenant_id, pipeline_id, program_name, name, description, config, connectors) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model
                                    .update_pipeline(tenant_id, pipeline_id, &program_name, &name, &description, &config, &connectors.clone(), None)
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_pipeline(tenant_id, pipeline_id, &program_name, &name, &description, &config, &connectors, None)
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
                            StorageAction::DeletePipeline(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_pipeline(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.delete_pipeline(tenant_id, &pipeline_name).await;
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
                                    model.new_connector(tenant_id, id, &name, &description, &config, None).await;
                                let impl_response =
                                    handle.db.new_connector(tenant_id, id, &name, &description, &config, None).await;
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
                                let model_response = model.get_connector_by_name(tenant_id, &name, None).await;
                                let impl_response = handle.db.get_connector_by_name(tenant_id, &name, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateConnector(tenant_id,connector_id, name, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.update_connector(tenant_id, connector_id, &name.as_deref(), &description.as_deref(), &config, None).await;
                                let impl_response =
                                    handle.db.update_connector(tenant_id, connector_id, &name.as_deref(), &description.as_deref(), &config, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteConnector(tenant_id, connector_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_connector(tenant_id, &connector_name).await;
                                let impl_response = handle.db.delete_connector(tenant_id, &connector_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListApiKeys(tenant_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_api_keys(tenant_id).await.unwrap();
                                let mut impl_response = handle.db.list_api_keys(tenant_id).await.unwrap();
                                impl_response.sort_by(|a, b| a.name.cmp(&b.name));
                                assert_eq!(model_response, impl_response);
                            },
                            StorageAction::GetApiKey(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_api_key(tenant_id, &name).await;
                                let impl_response = handle.db.get_api_key(tenant_id, &name).await;
                                check_responses(i, model_response, impl_response);
                            },
                            StorageAction::DeleteApiKey(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_api_key(tenant_id, &name).await;
                                let impl_response = handle.db.delete_api_key(tenant_id, &name).await;
                                check_responses(i, model_response, impl_response);
                            },
                            StorageAction::StoreApiKeyHash(tenant_id, id, name, key, permissions) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.store_api_key_hash(tenant_id, id, &name, &key, permissions.clone()).await;
                                let impl_response = handle.db.store_api_key_hash(tenant_id, id, &name, &key, permissions.clone()).await;
                                check_responses(i, model_response, impl_response);
                            },
                            StorageAction::ValidateApiKey(tenant_id,key) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.validate_api_key(&key).await;
                                let impl_response = handle.db.validate_api_key(&key).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::CreatePipelineRevision(new_revision_id, tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.create_pipeline_deployment(new_revision_id, tenant_id, pipeline_id).await;
                                let impl_response = handle.db.create_pipeline_deployment(new_revision_id, tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetCommittedPipeline(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_deployment(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_deployment(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NewService(tenant_id, id, name, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.new_service(tenant_id, id, &name, &description, &config, None).await;
                                let impl_response =
                                    handle.db.new_service(tenant_id, id, &name, &description, &config, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListServices(tenant_id, filter_config_type) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_services(tenant_id, &filter_config_type.as_deref()).await.unwrap();
                                let mut impl_response = handle.db.list_services(tenant_id, &filter_config_type.as_deref()).await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.service_id.cmp(&b.service_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::GetServiceById(tenant_id, service_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_service_by_id(tenant_id, service_id, None).await;
                                let impl_response = handle.db.get_service_by_id(tenant_id, service_id, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetServiceByName(tenant_id, name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_service_by_name(tenant_id, &name, None).await;
                                let impl_response = handle.db.get_service_by_name(tenant_id, &name, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateService(tenant_id, service_id, name, description, config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response =
                                    model.update_service(tenant_id, service_id, &name.as_deref(), &description.as_deref(), &config, None).await;
                                let impl_response =
                                    handle.db.update_service(tenant_id, service_id, &name.as_deref(), &description.as_deref(), &config, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteService(tenant_id, service_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_service(tenant_id, &service_name).await;
                                let impl_response = handle.db.delete_service(tenant_id, &service_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NewServiceProbe(tenant_id, service_id, id, request, created_at) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.new_service_probe(tenant_id, service_id, id, request.clone(), &created_at, None).await;
                                let impl_response = handle.db.new_service_probe(tenant_id, service_id, id, request.clone(), &created_at, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateServiceProbeSetRunning(tenant_id, service_probe_id, started_at) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.update_service_probe_set_running(tenant_id, service_probe_id, &started_at).await;
                                let impl_response = handle.db.update_service_probe_set_running(tenant_id, service_probe_id, &started_at).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateServiceProbeSetFinished(tenant_id, service_probe_id, response, finished_at) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.update_service_probe_set_finished(tenant_id, service_probe_id, response.clone(), &finished_at).await;
                                let impl_response = handle.db.update_service_probe_set_finished(tenant_id, service_probe_id, response.clone(), &finished_at).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NextServiceProbe => {
                                let model_response = model.next_service_probe().await;
                                let impl_response = handle.db.next_service_probe().await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetServiceProbe(tenant_id, service_id, service_probe_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_service_probe(tenant_id, service_id, service_probe_id, None).await;
                                let impl_response = handle.db.get_service_probe(tenant_id, service_id, service_probe_id, None).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListServiceProbes(tenant_id, service_id, limit, probe_type) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_service_probes(tenant_id, service_id, limit, probe_type.clone(), None).await;
                                let impl_response = handle.db.list_service_probes(tenant_id, service_id, limit, probe_type.clone(), None).await;
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
    pub api_keys: BTreeMap<(TenantId, String), (ApiKeyId, String, Vec<ApiPermission>)>,
    pub connectors: BTreeMap<(TenantId, ConnectorId), ConnectorDescr>,
    pub services: BTreeMap<(TenantId, ServiceId), ServiceDescr>,
    pub service_probes: BTreeMap<(TenantId, ServiceProbeId), (ServiceProbeDescr, ServiceId)>,
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
        config: &ProgramConfig,
        _txn: Option<&Transaction<'_>>,
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
                    status: ProgramStatus::Pending,
                    schema: None,
                    version,
                    code: Some(program_code.to_owned()),
                    config: config.clone(),
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
        program_name: &Option<String>,
        program_description: &Option<String>,
        program_code: &Option<String>,
        status: &Option<ProgramStatus>,
        schema: &Option<ProgramSchema>,
        config: &Option<ProgramConfig>,
        guard: Option<Version>,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<super::Version> {
        let mut s = self.lock().await;
        let (program_descr, _) = s
            .programs
            .get(&(tenant_id, program_id))
            .ok_or(DBError::UnknownProgram { program_id })?;
        let program_descr = program_descr.clone();

        let (program, _) = s.programs.get(&(tenant_id, program_id)).unwrap();
        if guard.is_some_and(|g| g.0 != program.version.0) {
            return Err(DBError::OutdatedProgramVersion {
                latest_version: program.version,
            });
        }

        if s.programs
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1 .0.clone())
            .any(|p| {
                program_name.as_ref().is_some_and(|n| *n == p.name) && p.program_id != program_id
            })
        {
            return Err(DBError::DuplicateName);
        }

        // This is an artifact of the test code. In the database,
        // pipelines reference programs by their IDs, and program_queries use
        // a join to turn IDs into names to produce a PipelineDescr.
        // The test code here however represents pipelines using a PipelineDescr,
        // so we have to manually propagate program name updates to all pipelines
        // that were referencing it.
        if program_name.is_some() {
            s.pipelines.iter_mut().for_each(|((tid, _), p)| {
                // Find pipeline descriptors with attached programs, and check if it is
                // referring to the program whose name we're updating
                if let Some(ref name) = p.descriptor.program_name {
                    if *tid == tenant_id && *name == program_descr.clone().name {
                        p.descriptor.program_name = program_name.clone();
                    }
                }
            });
        }

        s.programs
            .get_mut(&(tenant_id, program_id))
            .map(|(p, e)| {
                let cur_code = p.code.clone().unwrap();
                if let Some(name) = program_name {
                    p.name = name.to_owned();
                }
                if let Some(desc) = program_description {
                    p.description = desc.to_owned();
                }
                if let Some(schema) = schema {
                    p.schema = Some(schema.clone());
                }
                let mut has_config_changed = false;
                if let Some(config) = config {
                    if p.config != *config {
                        has_config_changed = true;
                    }
                    p.config = config.clone();
                }
                if let Some(status) = status {
                    p.status = status.clone();
                    // Reset schema when program status is set to Pending,
                    // this is used to signal the compiler
                    if p.status == ProgramStatus::Pending {
                        p.schema = None;
                    }
                }
                // If the code is updated, it overrides the schema and status
                // changes back to Pending.
                let mut has_code_changed = false;
                if let Some(code) = program_code {
                    if *code != cur_code {
                        p.code = program_code.to_owned();
                        has_code_changed = true;
                    }
                }
                if has_code_changed || has_config_changed {
                    p.version.0 += 1;
                    p.schema = None;
                    p.status = ProgramStatus::Pending;
                }
                if !has_code_changed && (status.is_some() || schema.is_some()) {
                    *e = SystemTime::now();
                }
                p.version
            })
            // This cannot fail because we already
            // checked whether the program exists
            .ok_or(DBError::UnknownProgram { program_id })
    }

    async fn get_program_by_id(
        &self,
        tenant_id: TenantId,
        program_id: super::ProgramId,
        with_code: bool,
    ) -> DBResult<ProgramDescr> {
        let s = self.lock().await;
        Ok(s.programs
            .get(&(tenant_id, program_id))
            .map(|(p, _)| p.clone())
            .map(|p| ProgramDescr {
                code: if with_code { p.code.clone() } else { None },
                ..p
            })
            .ok_or(DBError::UnknownProgram { program_id })?)
    }

    async fn get_program_by_name(
        &self,
        tenant_id: TenantId,
        program_name: &str,
        with_code: bool,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<ProgramDescr> {
        let s = self.lock().await;
        Ok(s.programs
            .iter()
            .filter(|k: &(&(TenantId, ProgramId), &ProgramData)| k.0 .0 == tenant_id)
            .map(|k| k.1 .0.clone())
            .find(|p| p.name == program_name)
            .map(|p| ProgramDescr {
                code: if with_code { p.code.clone() } else { None },
                ..p
            })
            .ok_or(DBError::UnknownProgramName {
                program_name: program_name.to_string(),
            })?)
    }

    async fn delete_program(&self, tenant_id: TenantId, program_name: &str) -> DBResult<()> {
        let mut s = self.lock().await;
        // Foreign key delete:
        let program = &s
            .programs
            .iter()
            .find(|c| c.0 .0 == tenant_id && c.1 .0.name == program_name)
            .ok_or(DBError::UnknownProgramName {
                program_name: program_name.to_string(),
            })?
            .1
             .0;
        let program_id = program.program_id;

        let found = s
            .pipelines
            .iter()
            .filter(|&c| c.0 .0 == tenant_id)
            .any(|c| c.1.descriptor.program_name == Some(program_name.to_string()));

        if found {
            Err(DBError::ProgramInUseByPipeline {
                program_name: program_name.to_string(),
            })
        } else {
            s.programs
                .remove(&(tenant_id, program_id))
                .map(|_| ())
                .ok_or(DBError::UnknownProgramName {
                    program_name: program_name.to_string(),
                })?;

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

    async fn create_pipeline_deployment(
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

        if let Some(ref program_name) = pipeline.descriptor.program_name {
            let program_data = s
                .programs
                .iter()
                .find(|e| e.0 .0 == tenant_id && e.1 .0.name == *program_name)
                .ok_or(DBError::UnknownProgramName {
                    program_name: program_name.to_string(),
                })?
                .1;
            let connectors = s
                .connectors
                .values()
                .filter(|c| {
                    pipeline
                        .descriptor
                        .attached_connectors
                        .iter()
                        .any(|ac| ac.connector_name == c.name)
                })
                .cloned()
                .collect::<Vec<ConnectorDescr>>();
            let mut services_for_connectors = vec![];
            for connector in &connectors {
                services_for_connectors.push(
                    s.services
                        .values()
                        .filter(|s| {
                            connector
                                .config
                                .transport
                                .service_names()
                                .iter()
                                .any(|attached_service_name| *attached_service_name == s.name)
                        })
                        .cloned()
                        .collect::<Vec<ServiceDescr>>(),
                );
            }

            let pipeline = pipeline.descriptor.clone();
            let connectors = connectors.clone();
            let services_for_connectors = services_for_connectors.clone();
            let program_data = program_data.clone();

            // Gives an answer if the relevant configuration state for the
            // pipeline has changed since our last commit.
            let prev = s.history.get(&(tenant_id, pipeline_id));
            let (has_changed, next_revision) = match prev {
                Some(prev_revision) => {
                    // TODO: the revision ID is taken into account when doing the comparison,
                    //       which means that it will always differ
                    let new_revision = PipelineRevision::new(
                        Revision(new_revision_id),
                        pipeline.clone(),
                        connectors.clone(),
                        services_for_connectors.clone(),
                        program_data.0.clone(),
                    );
                    let prev_serialized = serde_json::to_string(&prev_revision).unwrap();
                    let new_serialized = serde_json::to_string(&new_revision).unwrap();
                    let detected_changes = prev_serialized != new_serialized;
                    if detected_changes {
                        (detected_changes, Revision(new_revision_id))
                    } else {
                        (detected_changes, prev_revision.revision)
                    }
                }
                None => (true, Revision(new_revision_id)),
            };

            if has_changed {
                PipelineRevision::validate(
                    &pipeline,
                    &connectors,
                    &services_for_connectors,
                    &program_data.0,
                )?;
                s.history.insert(
                    (tenant_id, pipeline_id),
                    PipelineRevision::new(
                        next_revision,
                        pipeline,
                        connectors,
                        services_for_connectors,
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

    async fn get_pipeline_deployment(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> DBResult<PipelineRevision> {
        let s = self.lock().await;
        let _p = s
            .pipelines
            .get(&(tenant_id, pipeline_id))
            // .ok_or(DBError::UnknownPipeline { pipeline_id })?;
            .ok_or(DBError::NoRevisionAvailable { pipeline_id })?;
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
        program_name: &Option<String>,
        pipeline_name: &str,
        pipeline_description: &str,
        config: &RuntimeConfig,
        // TODO: not clear why connectors is an option here
        connectors: &Option<Vec<AttachedConnector>>,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<(super::PipelineId, super::Version)> {
        let mut s = self.lock().await;
        let db_connectors = s.connectors.clone();

        // Model the foreign key constraint on `program_id`
        if let Some(program_name) = program_name {
            s.programs
                .iter()
                .find(|entry| entry.0 .0 == tenant_id && entry.1 .0.name == *program_name)
                .ok_or(DBError::UnknownProgramName {
                    program_name: program_name.to_string(),
                })?;
        };
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

        let mut new_acs: Vec<AttachedConnector> = vec![];
        if let Some(connectors) = connectors {
            for ac in connectors {
                // Check that all attached connectors point to a valid
                // connector_name
                if !db_connectors
                    .iter()
                    .any(|entry| entry.0 .0 == tenant_id && entry.1.name == ac.connector_name)
                {
                    return Err(DBError::UnknownConnectorName {
                        connector_name: ac.connector_name.to_string(),
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
                    program_name: program_name.clone(),
                    name: pipeline_name.to_owned(),
                    description: pipeline_description.to_owned(),
                    config: config.clone(),
                    attached_connectors: new_acs,
                    version: Version(1),
                },
                state: PipelineRuntimeState {
                    pipeline_id,
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
        program_name: &Option<String>,
        pipeline_name: &str,
        pipeline_description: &str,
        config: &Option<RuntimeConfig>,
        connectors: &Option<Vec<AttachedConnector>>,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<Version> {
        let mut s = self.lock().await;
        let db_connectors = s.connectors.clone();

        // pipeline must exist
        s.pipelines
            .get_mut(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?;

        // Model the foreign key constraint on `program_id`
        if let Some(program_name) = program_name {
            s.programs
                .iter()
                .find(|entry| entry.0 .0 == tenant_id && entry.1 .0.name == *program_name)
                .ok_or(DBError::UnknownProgramName {
                    program_name: program_name.to_string(),
                })?;
        };
        let mut new_acs: Vec<AttachedConnector> = vec![];
        if let Some(connectors) = connectors {
            for ac in connectors {
                // Check that all attached connectors point to a valid
                // connector_name
                if !db_connectors
                    .iter()
                    .any(|entry| entry.0 .0 == tenant_id && entry.1.name == ac.connector_name)
                {
                    return Err(DBError::UnknownConnectorName {
                        connector_name: ac.connector_name.to_string(),
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
        let c = &mut s
            .pipelines
            .get_mut(&(tenant_id, pipeline_id))
            .ok_or(DBError::UnknownPipeline { pipeline_id })?
            .descriptor;

        c.attached_connectors = new_acs;
        c.name = pipeline_name.to_owned();
        c.program_name = program_name.clone();
        c.description = pipeline_description.to_owned();
        c.version = c.version.increment();
        c.config = config.clone().unwrap_or(c.config.clone());
        Ok(c.version)
    }

    async fn attached_connector_is_input(
        &self,
        _tenant_id: TenantId,
        _pipeline_id: PipelineId,
        _name: &str,
    ) -> DBResult<bool> {
        todo!()
    }

    async fn delete_pipeline(&self, tenant_id: TenantId, pipeline_name: &str) -> DBResult<()> {
        let pipeline = self.get_pipeline_by_name(tenant_id, pipeline_name).await?;
        let mut s = self.lock().await;
        let _r = s
            .history
            .remove(&(tenant_id, pipeline.descriptor.pipeline_id));
        s.pipelines
            .remove(&(tenant_id, pipeline.descriptor.pipeline_id));
        Ok(())
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
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<super::PipelineDescr> {
        self.get_pipeline_by_id(tenant_id, pipeline_id)
            .await
            .map(|p| p.descriptor)
    }

    async fn get_pipeline_runtime_state_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<PipelineRuntimeState, DBError> {
        self.get_pipeline_by_id(tenant_id, pipeline_id)
            .await
            .map(|p| p.state)
    }

    async fn get_pipeline_runtime_state_by_name(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineRuntimeState, DBError> {
        self.get_pipeline_by_name(tenant_id, pipeline_name)
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
        name: &str,
    ) -> DBResult<super::Pipeline> {
        let s = self.lock().await;
        s.pipelines
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|p| p.descriptor.name == name)
            .ok_or(DBError::UnknownPipelineName {
                pipeline_name: name.to_string(),
            })
    }

    async fn get_pipeline_descr_by_name(
        &self,
        tenant_id: TenantId,
        name: &str,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<super::PipelineDescr> {
        self.get_pipeline_by_name(tenant_id, &name)
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
        _txn: Option<&Transaction<'_>>,
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
        name: &str,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<ConnectorDescr> {
        let s = self.lock().await;
        s.connectors
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|c| c.name == name)
            .ok_or(DBError::UnknownConnectorName {
                connector_name: name.to_string(),
            })
    }

    async fn update_connector(
        &self,
        tenant_id: TenantId,
        connector_id: super::ConnectorId,
        connector_name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ConnectorConfig>,
        _txn: Option<&Transaction<'_>>,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        // `connector_id` needs to exist
        if s.connectors.get(&(tenant_id, connector_id)).is_none() {
            return Err(DBError::UnknownConnector { connector_id }.into());
        }
        // UNIQUE constraint on name
        if let Some(connector_name) = connector_name {
            if let Some(c) = s
                .connectors
                .iter()
                .filter(|k| k.0 .0 == tenant_id)
                .map(|k| k.1)
                .find(|c| &c.name == connector_name)
                .cloned()
            {
                if c.connector_id != connector_id {
                    return Err(DBError::DuplicateName.into());
                }
            }
        }

        // Each service of the connector must exist
        if let Some(config) = config {
            for attached_service_name in config.transport.service_names() {
                s.services
                    .iter()
                    .filter(|k| k.0 .0 == tenant_id)
                    .map(|k| k.1.clone())
                    .find(|c| c.name == *attached_service_name)
                    .ok_or(DBError::UnknownName {
                        name: attached_service_name.clone(),
                    })?;
            }
        }

        let c = s
            .connectors
            .get_mut(&(tenant_id, connector_id))
            .ok_or(DBError::UnknownConnector { connector_id })?;
        if let Some(name) = connector_name {
            c.name = name.to_string();
        }
        if let Some(description) = description {
            c.description = description.to_string();
        }
        if let Some(config) = config {
            c.config = config.clone();
        }
        // Change the connector name for all pipelines that have it attached
        if let Some(name) = connector_name {
            s.pipelines.values_mut().for_each(|pipeline| {
                for ac in &mut pipeline.descriptor.attached_connectors {
                    if ac.connector_name == name.to_string() {
                        *ac = AttachedConnector {
                            name: ac.name.clone(),
                            is_input: ac.is_input,
                            connector_name: name.to_string(),
                            relation_name: ac.connector_name.clone(),
                        };
                    }
                }
            });
        }
        Ok(())
    }

    async fn delete_connector(&self, tenant_id: TenantId, connector_name: &str) -> DBResult<()> {
        let mut s = self.lock().await;
        let connector_id = s
            .connectors
            .iter()
            .find(|c| c.0 .0 == tenant_id && c.1.name == connector_name)
            .map(|entry| entry.0 .1)
            .ok_or(DBError::UnknownConnectorName {
                connector_name: connector_name.to_string(),
            })?;
        s.connectors
            .remove(&(tenant_id, connector_id))
            .ok_or(DBError::UnknownConnector { connector_id })?;
        s.pipelines.values_mut().for_each(|c| {
            c.descriptor
                .attached_connectors
                .retain(|c| c.name != connector_name);
        });
        Ok(())
    }

    async fn list_api_keys(&self, tenant_id: TenantId) -> DBResult<Vec<ApiKeyDescr>> {
        let s = self.lock().await;
        Ok(s.api_keys
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| ApiKeyDescr {
                id: k.1 .0,
                name: k.0 .1.clone(),
                scopes: k.1 .2.clone(),
            })
            .collect())
    }

    async fn get_api_key(&self, tenant_id: TenantId, name: &str) -> DBResult<ApiKeyDescr> {
        let s = self.lock().await;
        s.api_keys.get(&(tenant_id, name.to_string())).map_or(
            Err(DBError::UnknownApiKey {
                name: name.to_string(),
            }),
            |k| {
                Ok(ApiKeyDescr {
                    id: k.0,
                    name: name.to_string(),
                    scopes: k.2.clone(),
                })
            },
        )
    }

    async fn delete_api_key(&self, tenant_id: TenantId, name: &str) -> DBResult<()> {
        let mut s = self.lock().await;
        s.api_keys
            .remove(&(tenant_id, name.to_string()))
            .map(|_| ())
            .ok_or(DBError::UnknownApiKey {
                name: name.to_string(),
            })
    }

    async fn store_api_key_hash(
        &self,
        tenant_id: TenantId,
        id: Uuid,
        name: &str,
        key: &str,
        permissions: Vec<ApiPermission>,
    ) -> DBResult<()> {
        let mut s = self.lock().await;
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        if s.api_keys.iter().any(|k| k.1 .0 == ApiKeyId(id)) {
            return Err(DBError::unique_key_violation("api_key_pkey"));
        }
        if s.api_keys.contains_key(&(tenant_id, name.to_string())) {
            return Err(DBError::DuplicateName);
        }
        if s.api_keys.iter().any(|k| k.1 .1 == hash) {
            return Err(DBError::duplicate_key());
        }
        s.api_keys.insert(
            (tenant_id, name.to_string()),
            (ApiKeyId(id), hash, permissions),
        );
        Ok(())
    }

    async fn validate_api_key(&self, key: &str) -> DBResult<(TenantId, Vec<ApiPermission>)> {
        let s = self.lock().await;
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let record: Vec<(TenantId, Vec<ApiPermission>)> = s
            .api_keys
            .iter()
            .filter(|k| k.1 .1 == hash)
            .map(|k| (k.0 .0, k.1 .2.clone()))
            .collect();
        assert!(record.len() <= 1);
        let record = record.get(0);
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
        _txn: Option<&Transaction<'_>>,
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
                config_type: config.config_type(),
            },
        );
        Ok(service_id)
    }

    async fn list_services(
        &self,
        tenant_id: TenantId,
        filter_config_type: &Option<&str>,
    ) -> Result<Vec<ServiceDescr>, DBError> {
        let s = self.lock().await;
        match filter_config_type {
            None => Ok(s
                .services
                .iter()
                .filter(|k| k.0 .0 == tenant_id)
                .map(|k| k.1.clone())
                .collect()),
            Some(filter_config_type) => Ok(s
                .services
                .iter()
                .filter(|k| k.0 .0 == tenant_id && k.1.config_type == *filter_config_type)
                .map(|k| k.1.clone())
                .collect()),
        }
    }

    async fn get_service_by_id(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        _txn: Option<&Transaction<'_>>,
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
        name: &str,
        _txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceDescr, DBError> {
        let s = self.lock().await;
        s.services
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|c| c.name == name)
            .ok_or(DBError::UnknownServiceName {
                service_name: name.to_string(),
            })
    }

    async fn update_service(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        name: &Option<&str>,
        description: &Option<&str>,
        config: &Option<ServiceConfig>,
        _txn: Option<&Transaction<'_>>,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;

        // `service_id` needs to exist
        if s.services.get(&(tenant_id, service_id)).is_none() {
            return Err(DBError::UnknownService { service_id }.into());
        }

        // The new name cannot already be in use by another service
        if let Some(name) = name {
            if let Some(c) = s
                .services
                .iter()
                .filter(|k| k.0 .0 == tenant_id)
                .map(|k| k.1)
                .find(|c| &c.name == name)
                .cloned()
            {
                if c.service_id != service_id {
                    return Err(DBError::DuplicateName.into());
                }
            }
        }

        // Update the service
        let c = s
            .services
            .get_mut(&(tenant_id, service_id))
            .ok_or(DBError::UnknownService { service_id })?;
        if let Some(name) = name {
            c.name = name.to_string();
        }
        if let Some(description) = description {
            c.description = description.to_string()
        }
        if let Some(config) = config {
            c.config = config.clone();
            c.config_type = config.config_type();
        }
        // Change the service name for all connectors that have it in their transport
        if let Some(name) = name {
            s.connectors.values_mut().for_each(|connector| {
                for used_service_name in &mut connector.config.transport.service_names() {
                    if used_service_name == name {
                        *used_service_name = name.to_string()
                    }
                }
            });
        }
        Ok(())
    }

    async fn delete_service(&self, tenant_id: TenantId, service_name: &str) -> Result<(), DBError> {
        let mut s = self.lock().await;
        // Retrieve the service identifier and check the name exists
        let ((_, service_id), _) = s
            .services
            .iter()
            .find(|c| c.0 .0 == tenant_id && c.1.name == service_name)
            .ok_or(DBError::UnknownServiceName {
                service_name: service_name.to_string(),
            })?;
        let service_id = service_id.clone();

        // Cascade: remove any service probes of the service
        s.service_probes.retain(|_, (_, id)| *id != service_id);

        // Find all connectors which have the service
        let connectors_with_the_service: Vec<ConnectorDescr> = s
            .connectors
            .iter()
            .filter(|k| {
                k.1.config
                    .transport
                    .service_names()
                    .contains(&service_name.to_string())
            })
            .map(|k| k.1.clone())
            .collect();

        for connector_descr in connectors_with_the_service {
            // Remove connector from all the pipelines
            s.pipelines.values_mut().for_each(|c| {
                c.descriptor
                    .attached_connectors
                    .retain(|c| c.name != connector_descr.name);
            });
            // Remove connector
            s.connectors
                .remove(&(tenant_id, connector_descr.connector_id))
                .ok_or(DBError::UnknownConnector {
                    connector_id: connector_descr.connector_id,
                })?;
        }

        // Remove the service
        s.services
            .remove(&(tenant_id, service_id.clone()))
            .ok_or(DBError::UnknownService {
                service_id: service_id.clone(),
            })?;

        Ok(())
    }

    async fn new_service_probe(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        id: Uuid,
        request: ServiceProbeRequest,
        created_at: &DateTime<Utc>,
        _txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceProbeId, DBError> {
        let mut s = self.lock().await;
        let service_probe_id = ServiceProbeId(id);

        // Primary key constraint violation
        if s.service_probes.keys().any(|k| k.1 == service_probe_id) {
            return Err(DBError::unique_key_violation("service_probe_pkey"));
        }

        // Service must exist
        if !s
            .services
            .keys()
            .any(|k| k.0 == tenant_id && k.1 == service_id)
        {
            return Err(DBError::UnknownService { service_id });
        }

        s.service_probes.insert(
            (tenant_id, service_probe_id),
            (
                ServiceProbeDescr {
                    service_probe_id,
                    status: ServiceProbeStatus::Pending,
                    request: request.clone(),
                    probe_type: request.probe_type().clone(),
                    response: None,
                    // Convert to timestamp to second precision
                    created_at: convert_bigint_to_time("", created_at.clone().timestamp()).unwrap(),
                    started_at: None,
                    finished_at: None,
                },
                service_id,
            ),
        );
        Ok(service_probe_id)
    }

    async fn update_service_probe_set_running(
        &self,
        tenant_id: TenantId,
        service_probe_id: ServiceProbeId,
        started_at: &DateTime<Utc>,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;

        // Service probe must exist
        if s.service_probes
            .get(&(tenant_id, service_probe_id))
            .is_none()
        {
            return Err(DBError::UnknownServiceProbe { service_probe_id }.into());
        }

        // Update the service probe
        let c = s
            .service_probes
            .get_mut(&(tenant_id, service_probe_id))
            .ok_or(DBError::UnknownServiceProbe { service_probe_id })?;
        c.0.status = ServiceProbeStatus::Running;
        c.0.started_at = Some(convert_bigint_to_time("", started_at.clone().timestamp()).unwrap());

        Ok(())
    }

    async fn update_service_probe_set_finished(
        &self,
        tenant_id: TenantId,
        service_probe_id: ServiceProbeId,
        response: ServiceProbeResponse,
        finished_at: &DateTime<Utc>,
    ) -> Result<(), DBError> {
        let mut s = self.lock().await;

        // Service probe must exist
        if s.service_probes
            .get(&(tenant_id, service_probe_id))
            .is_none()
        {
            return Err(DBError::UnknownServiceProbe { service_probe_id }.into());
        }

        // Update the service probe
        let c = s
            .service_probes
            .get_mut(&(tenant_id, service_probe_id))
            .ok_or(DBError::UnknownServiceProbe { service_probe_id })?;
        c.0.status = match response {
            ServiceProbeResponse::Success(_) => ServiceProbeStatus::Success,
            ServiceProbeResponse::Error(_) => ServiceProbeStatus::Failure,
        };
        c.0.response = Some(response);
        c.0.finished_at =
            Some(convert_bigint_to_time("", finished_at.clone().timestamp()).unwrap());

        Ok(())
    }

    async fn next_service_probe(
        &self,
    ) -> Result<Option<(ServiceProbeId, TenantId, ServiceProbeRequest, ServiceConfig)>, DBError>
    {
        let s = self.lock().await;

        // Sort ascending on (timestamp, id)
        // Sorting by created_at is at second granularity
        let mut values: Vec<(&(TenantId, ServiceProbeId), &(ServiceProbeDescr, ServiceId))> =
            Vec::from_iter(s.service_probes.iter());
        values.sort_by(|(_, (descr1, _)), (_, (descr2, _))| {
            (descr1.created_at, descr1.service_probe_id)
                .cmp(&(descr2.created_at, descr2.service_probe_id))
        });

        values
            .iter()
            .find(|(_, (probe_descr, _))| {
                probe_descr.status == ServiceProbeStatus::Pending
                    || probe_descr.status == ServiceProbeStatus::Running
            })
            .map(
                |((tenant_id, service_probe_id), (probe_descr, service_id))| {
                    Ok(Some((
                        *service_probe_id,
                        *tenant_id,
                        probe_descr.request.clone(),
                        s.services
                            .get(&(*tenant_id, *service_id))
                            .cloned()
                            .ok_or(DBError::UnknownService {
                                service_id: *service_id,
                            })?
                            .config,
                    )))
                },
            )
            .unwrap_or(Ok(None))
    }

    async fn get_service_probe(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        service_probe_id: ServiceProbeId,
        _txn: Option<&Transaction<'_>>,
    ) -> Result<ServiceProbeDescr, DBError> {
        let s = self.lock().await;

        // Service probe must exist
        let service_probe = s
            .service_probes
            .get(&(tenant_id, service_probe_id))
            .cloned()
            .ok_or(DBError::UnknownServiceProbe { service_probe_id })?;

        // The service identifier must match to that of the probe
        if service_probe.1 != service_id {
            return Err(DBError::UnknownServiceProbe { service_probe_id });
        }

        Ok(service_probe.0)
    }

    async fn list_service_probes(
        &self,
        tenant_id: TenantId,
        service_id: ServiceId,
        limit: Option<u32>,
        probe_type: Option<ServiceProbeType>,
        _txn: Option<&Transaction<'_>>,
    ) -> Result<Vec<ServiceProbeDescr>, DBError> {
        let s = self.lock().await;

        // A service with that name must exist, else return empty
        let service = s
            .services
            .iter()
            .filter(|k| k.0 .0 == tenant_id)
            .map(|k| k.1.clone())
            .find(|c| c.service_id == service_id);
        if service.is_none() {
            return Ok(vec![]);
        }
        let service = service.unwrap();

        // Retrieve all probes
        let mut list: Vec<ServiceProbeDescr> = s
            .service_probes
            .iter()
            .filter(|k| k.0 .0 == tenant_id && k.1 .1 == service.service_id)
            .map(|(_, (descr, _))| descr.clone())
            .collect();

        // Sort descending on (timestamp, id)
        list.sort_by(|descr1, descr2| {
            (descr2.created_at, descr2.service_probe_id)
                .cmp(&(descr1.created_at, descr1.service_probe_id))
        });

        // Filter out leaving a single request type
        if let Some(probe_type) = probe_type {
            list.retain(|descr| descr.probe_type == probe_type);
        }

        // Limit list size
        if let Some(limit) = limit {
            while list.len() > limit as usize {
                list.pop();
            }
        }

        Ok(list)
    }
}
