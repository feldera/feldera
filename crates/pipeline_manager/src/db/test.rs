use crate::auth::{generate_api_key, TenantRecord};
use crate::db::error::DBError;
use crate::db::storage::Storage;
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::api_key::{ApiKeyDescr, ApiKeyId, ApiPermission};
use crate::db::types::common::Version;
use crate::db::types::pipeline::{
    validate_deployment_desired_status_transition, validate_deployment_status_transition,
    ExtendedPipelineDescr, PipelineDescr, PipelineId, PipelineStatus,
};
use crate::db::types::program::{
    validate_program_status_transition, CompilationProfile, ProgramConfig, ProgramInfo,
    ProgramStatus, SqlCompilerMessage,
};
use crate::db::types::tenant::TenantId;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use openssl::sha;
use pipeline_types::config::{PipelineConfig, ResourceConfig, RuntimeConfig};
use pipeline_types::error::ErrorResponse;
use pipeline_types::program_schema::ProgramSchema;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use proptest_derive::Arbitrary;
use serde_json::json;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::vec;
use tokio::sync::Mutex;
use uuid::Uuid;

struct DbHandle {
    db: StoragePostgres,
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

#[cfg(feature = "pg-embed")]
async fn test_setup() -> DbHandle {
    let (conn, _temp_dir) = setup_pg().await;

    DbHandle {
        db: conn,
        _temp_dir,
    }
}

#[cfg(feature = "pg-embed")]
pub(crate) async fn setup_pg() -> (StoragePostgres, tempfile::TempDir) {
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
    let conn = StoragePostgres::connect_inner(&db_uri, Some(pg))
        .await
        .unwrap();
    conn.run_migrations().await.unwrap();
    (conn, _temp_dir)
}

#[cfg(not(feature = "pg-embed"))]
async fn test_setup() -> DbHandle {
    let (conn, config) = setup_pg().await;
    DbHandle { db: conn, config }
}

#[cfg(not(feature = "pg-embed"))]
pub(crate) async fn setup_pg() -> (StoragePostgres, tokio_postgres::Config) {
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
    let conn = StoragePostgres::with_config(config.clone()).await.unwrap();
    conn.run_migrations().await.unwrap();

    (conn, config)
}

//////////////////////////////////////////////////////////////////////////////
/////                        DATA GENERATORS                             /////

/// Generate uuids but limits the the randomness to the first bits.
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

// Value aliases for proptest mapping functions
type PipelineNamePropVal = u8;
// This had to be a struct because there is a limit on the number
// of tuple fields for the automatic implementation of Arbitrary.
#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct RuntimeConfigPropVal {
    val0: u16,
    val1: bool,
    val2: u64,
    val3: u64,
    val4: bool,
    val5: bool,
    val6: String,
    val7: Option<u64>,
    val8: Option<u64>,
    val9: Option<u64>,
    val10: Option<u64>,
    val11: Option<u64>,
    val12: Option<String>,
    val13: Option<usize>,
}
type ProgramConfigPropVal = (bool, bool);
type ProgramInfoPropVal = ();

/// Generates a limited pipeline name.
fn map_val_to_limited_pipeline_name(val: PipelineNamePropVal) -> String {
    let limited_val = val % 4;
    format!("pipeline-{limited_val}")
}

/// Generates a limited runtime configuration.
fn map_val_to_limited_runtime_config(val: RuntimeConfigPropVal) -> RuntimeConfig {
    RuntimeConfig {
        workers: val.val0,
        cpu_profiler: val.val1,
        min_batch_size_records: val.val2,
        max_buffering_delay_usecs: val.val3,
        storage: val.val4,
        tracing: val.val5,
        tracing_endpoint_jaeger: val.val6,
        resources: ResourceConfig {
            cpu_cores_min: val.val7,
            cpu_cores_max: val.val8,
            memory_mb_min: val.val9,
            memory_mb_max: val.val10,
            storage_mb_max: val.val11,
            storage_class: val.val12,
        },
        min_storage_bytes: val.val13,
    }
}

/// Generates a limited program configuration.
fn map_val_to_limited_program_config(val: ProgramConfigPropVal) -> ProgramConfig {
    ProgramConfig {
        profile: if val.0 {
            None
        } else {
            if val.1 {
                Some(CompilationProfile::Unoptimized)
            } else {
                Some(CompilationProfile::Optimized)
            }
        },
    }
}

/// Generates a limited program information.
fn map_val_to_limited_program_info(_val: ProgramInfoPropVal) -> ProgramInfo {
    ProgramInfo {
        schema: ProgramSchema {
            inputs: vec![],
            outputs: vec![],
        },
        input_connectors: Default::default(),
        output_connectors: Default::default(),
    }
}

/// Generates pipeline name limited to only 4 variants.
/// This is prevent that only the "pipeline not found" is encountered.
fn limited_pipeline_name() -> impl Strategy<Value = String> {
    any::<PipelineNamePropVal>().prop_map(map_val_to_limited_pipeline_name)
}

/// Generates different runtime configurations.
fn limited_pipeline_descr() -> impl Strategy<Value = PipelineDescr> {
    any::<(
        PipelineNamePropVal,
        String,
        RuntimeConfigPropVal,
        String,
        ProgramConfigPropVal,
    )>()
    .prop_map(|val| PipelineDescr {
        name: map_val_to_limited_pipeline_name(val.0),
        description: val.1,
        runtime_config: map_val_to_limited_runtime_config(val.2),
        program_code: val.3,
        program_config: map_val_to_limited_program_config(val.4),
    })
}

/// Generates different optional runtime configurations.
fn limited_option_runtime_config() -> impl Strategy<Value = Option<RuntimeConfig>> {
    any::<Option<RuntimeConfigPropVal>>().prop_map(|val| val.map(map_val_to_limited_runtime_config))
}

/// Generates different optional program configurations.
fn limited_option_program_config() -> impl Strategy<Value = Option<ProgramConfig>> {
    any::<Option<ProgramConfigPropVal>>().prop_map(|val| val.map(map_val_to_limited_program_config))
}

/// Generates different optional program information.
fn limited_program_info() -> impl Strategy<Value = ProgramInfo> {
    any::<ProgramInfoPropVal>().prop_map(map_val_to_limited_program_info)
}

/// Generates different pipeline configurations.
fn limited_pipeline_config() -> impl Strategy<Value = PipelineConfig> {
    any::<(PipelineId, RuntimeConfigPropVal, ProgramInfoPropVal)>().prop_map(|val| {
        let runtime_config = map_val_to_limited_runtime_config(val.1);
        let program_info = map_val_to_limited_program_info(val.2);
        PipelineConfig {
            global: runtime_config,
            name: Some(format!("pipeline-{}", val.0)),
            storage_config: None,
            inputs: program_info.input_connectors,
            outputs: program_info.output_connectors,
        }
    })
}

/// Generates different error responses.
fn limited_error_response() -> impl Strategy<Value = ErrorResponse> {
    any::<u8>().prop_map(|val| ErrorResponse {
        message: "This is an example error response".to_string(),
        error_code: Cow::from("SomeExampleError"),
        details: json!({
            "extra-info": val
        }),
    })
}

//////////////////////////////////////////////////////////////////////////////
/////                       HAND-WRITTEN TESTS                           /////

#[tokio::test]
async fn create_tenant() {
    let handle = test_setup().await;
    let tenant_id_1 = handle
        .db
        .get_or_create_tenant_id(Uuid::now_v7(), "x".to_string(), "y".to_string())
        .await
        .unwrap();
    let tenant_id_2 = handle
        .db
        .get_or_create_tenant_id(Uuid::now_v7(), "x".to_string(), "y".to_string())
        .await
        .unwrap();
    let tenant_id_3 = handle
        .db
        .get_or_create_tenant_id(Uuid::now_v7(), "x".to_string(), "y".to_string())
        .await
        .unwrap();
    assert_eq!(tenant_id_1, tenant_id_2);
    assert_eq!(tenant_id_2, tenant_id_3);
}

#[tokio::test]
async fn save_api_key() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    // Attempt several key generations and validations
    for i in 1..10 {
        let api_key = generate_api_key();
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

        let api_key_2 = generate_api_key();
        let err = handle.db.validate_api_key(&api_key_2).await.unwrap_err();
        assert!(matches!(err, DBError::InvalidApiKey));
    }
}

// #[tokio::test]
// async fn program_creation() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let res = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "ignored",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     let rows = handle.db.list_programs(tenant_id, false).await.unwrap();
//     let expected = ProgramDescr {
//         program_id: res.0,
//         name: "test1".to_string(),
//         description: "program desc".to_string(),
//         version: res.1,
//         status: ProgramStatus::Pending,
//         schema: None,
//         code: None,
//         config: ProgramConfig {
//             profile: Some(CompilationProfile::Unoptimized),
//         },
//     };
//     let actual = rows.get(0).unwrap();
//     assert_eq!(1, rows.len());
//     assert_eq!(&expected, actual);
//
//     let rows = handle.db.list_programs(tenant_id, true).await.unwrap();
//     let expected = ProgramDescr {
//         program_id: res.0,
//         name: "test1".to_string(),
//         description: "program desc".to_string(),
//         version: res.1,
//         status: ProgramStatus::Pending,
//         schema: None,
//         code: Some("ignored".to_string()),
//         config: ProgramConfig {
//             profile: Some(CompilationProfile::Unoptimized),
//         },
//     };
//     let actual = rows.get(0).unwrap();
//     assert_eq!(1, rows.len());
//     assert_eq!(&expected, actual);
//
//     let rows = handle.db.all_programs().await.unwrap();
//     let expected = ProgramDescr {
//         program_id: res.0,
//         name: "test1".to_string(),
//         description: "program desc".to_string(),
//         version: res.1,
//         status: ProgramStatus::Pending,
//         schema: None,
//         code: None,
//         config: ProgramConfig {
//             profile: Some(CompilationProfile::Unoptimized),
//         },
//     };
//     let (_, actual) = rows.get(0).unwrap();
//     assert_eq!(1, rows.len());
//     assert_eq!(&expected, actual);
// }
//
// #[tokio::test]
// async fn duplicate_program() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let _ = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "ignored",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await;
//     let res = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "ignored",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .expect_err("Expecting unique violation");
//     let expected = DBError::DuplicateName;
//     assert_eq!(format!("{}", res), format!("{}", expected));
// }
//
// #[tokio::test]
// async fn program_code() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let (program_id, _) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "create table t1(c1 integer);",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     let result = handle
//         .db
//         .get_program_by_id(tenant_id, program_id, true)
//         .await
//         .unwrap();
//     assert_eq!("test1", result.name);
//     assert_eq!("program desc", result.description);
//     assert_eq!(
//         "create table t1(c1 integer);".to_owned(),
//         result.code.unwrap()
//     );
// }
//
// #[tokio::test]
// async fn update_program() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let (program_id, _) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "create table t1(c1 integer);",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     let _ = handle
//         .db
//         .update_program(
//             tenant_id,
//             program_id,
//             &Some("test2".to_string()),
//             &Some("different desc".to_string()),
//             &Some("create table t2(c2 integer);".to_string()),
//             &None,
//             &None,
//             &None,
//             None,
//             None,
//         )
//         .await;
//     let descr = handle
//         .db
//         .get_program_by_id(tenant_id, program_id, true)
//         .await
//         .unwrap();
//     assert_eq!("test2", descr.name);
//     assert_eq!("different desc", descr.description);
//     assert_eq!("create table t2(c2 integer);", descr.code.unwrap());
//
//     let _ = handle
//         .db
//         .update_program(
//             tenant_id,
//             program_id,
//             &Some("updated_test1".to_string()),
//             &Some("some new description".to_string()),
//             &None,
//             &None,
//             &None,
//             &None,
//             None,
//             None,
//         )
//         .await;
//     let results = handle.db.list_programs(tenant_id, false).await.unwrap();
//     assert_eq!(1, results.len());
//     let row = results.get(0).unwrap();
//     assert_eq!("updated_test1", row.name);
//     assert_eq!("some new description", row.description);
// }
//
// #[tokio::test]
// async fn program_queries() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let (program_id, _) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "create table t1(c1 integer);",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     let desc = handle
//         .db
//         .get_program_by_id(tenant_id, program_id, false)
//         .await
//         .unwrap();
//     assert_eq!("test1", desc.name);
//     let desc = handle
//         .db
//         .get_program_by_name(tenant_id, "test1", false, None)
//         .await
//         .unwrap();
//     assert_eq!("test1", desc.name);
//     let desc = handle
//         .db
//         .get_program_by_name(tenant_id, "test2", false, None)
//         .await;
//     assert!(desc.is_err());
// }
//
// #[tokio::test]
// async fn pipeline_config() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     handle
//         .db
//         .new_connector(
//             tenant_id,
//             Uuid::now_v7(),
//             "a",
//             "b",
//             &test_connector_config(),
//             None,
//         )
//         .await
//         .unwrap();
//     let ac = AttachedConnector {
//         name: "foo".to_string(),
//         is_input: true,
//         connector_name: "a".to_string(),
//         relation_name: "".to_string(),
//     };
//     let rc = RuntimeConfig::from_yaml("");
//     let _ = handle
//         .db
//         .new_pipeline(
//             tenant_id,
//             Uuid::now_v7(),
//             &None,
//             "1",
//             "2",
//             &rc,
//             &Some(vec![ac.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//     let res = handle.db.list_pipelines(tenant_id).await.unwrap();
//     assert_eq!(1, res.len());
//     let config = res.get(0).unwrap();
//     assert_eq!("1", config.descriptor.name);
//     assert_eq!("2", config.descriptor.description);
//     assert_eq!(rc, config.descriptor.config);
//     assert_eq!(None, config.descriptor.program_name);
//     let connectors = &config.descriptor.attached_connectors;
//     assert_eq!(1, connectors.len());
//     let ac_ret = connectors.get(0).unwrap().clone();
//     assert_eq!(ac, ac_ret);
// }
//
// #[tokio::test]
// async fn project_pending() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let (uid1, v1) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "project desc",
//             "ignored",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     let (uid2, v2) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test2",
//             "project desc",
//             "ignored",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//
//     handle
//         .db
//         .set_program_status_guarded(tenant_id, uid2, v2, ProgramStatus::Pending)
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_status_guarded(tenant_id, uid1, v1, ProgramStatus::Pending)
//         .await
//         .unwrap();
//     let (_, id, _version) = handle.db.next_job().await.unwrap().unwrap();
//     assert_eq!(id, uid2);
//     let (_, id, _version) = handle.db.next_job().await.unwrap().unwrap();
//     assert_eq!(id, uid2);
//     // Maybe next job should set the status to something else
//     // so it won't get picked up twice?
// }
//
// #[tokio::test]
// async fn update_status() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let (program_id, _) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "create table t1(c1 integer);",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     let desc = handle
//         .db
//         .get_program_by_id(tenant_id, program_id, false)
//         .await
//         .unwrap();
//     assert_eq!(ProgramStatus::Pending, desc.status);
//     handle
//         .db
//         .set_program_status_guarded(
//             tenant_id,
//             program_id,
//             desc.version,
//             ProgramStatus::CompilingRust,
//         )
//         .await
//         .unwrap();
//     let desc = handle
//         .db
//         .get_program_by_id(tenant_id, program_id, false)
//         .await
//         .unwrap();
//     assert_eq!(ProgramStatus::CompilingRust, desc.status);
// }
//
// #[tokio::test]
// async fn duplicate_attached_conn_name() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     handle
//         .db
//         .new_connector(
//             tenant_id,
//             Uuid::now_v7(),
//             "a",
//             "b",
//             &test_connector_config(),
//             None,
//         )
//         .await
//         .unwrap();
//     let ac = AttachedConnector {
//         name: "foo".to_string(),
//         is_input: true,
//         connector_name: "a".to_string(),
//         relation_name: "".to_string(),
//     };
//     let ac2 = AttachedConnector {
//         name: "foo".to_string(),
//         is_input: true,
//         connector_name: "a".to_string(),
//         relation_name: "".to_string(),
//     };
//     let rc = RuntimeConfig::from_yaml("");
//     let _ = handle
//         .db
//         .new_pipeline(
//             tenant_id,
//             Uuid::now_v7(),
//             &None,
//             "1",
//             "2",
//             &rc,
//             &Some(vec![ac, ac2]),
//             None,
//         )
//         .await
//         .expect_err("duplicate attached connector name");
// }
//
// #[tokio::test]
// async fn update_conn_name() {
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//     let connector_id = handle
//         .db
//         .new_connector(
//             tenant_id,
//             Uuid::now_v7(),
//             "a",
//             "b",
//             &test_connector_config(),
//             None,
//         )
//         .await
//         .unwrap();
//     let ac = AttachedConnector {
//         name: "foo".to_string(),
//         is_input: true,
//         connector_name: "a".to_string(),
//         relation_name: "".to_string(),
//     };
//     let rc = RuntimeConfig::from_yaml("");
//     handle
//         .db
//         .new_pipeline(
//             tenant_id,
//             Uuid::now_v7(),
//             &None,
//             "1",
//             "2",
//             &rc,
//             &Some(vec![ac]),
//             None,
//         )
//         .await
//         .unwrap();
//     handle
//         .db
//         .update_connector(
//             tenant_id,
//             connector_id,
//             &Some("not-a"),
//             &Some("b"),
//             &None,
//             None,
//         )
//         .await
//         .unwrap();
//     let pipeline = handle
//         .db
//         .get_pipeline_by_name(tenant_id, "1")
//         .await
//         .unwrap();
//     assert_eq!(
//         "not-a".to_string(),
//         pipeline
//             .descriptor
//             .attached_connectors
//             .get(0)
//             .unwrap()
//             .connector_name
//     );
// }
//
//
// /// A Function that commits twice and checks the second time errors, returns
// /// revision of first commit.
// async fn commit_check(handle: &DbHandle, tenant_id: TenantId, pipeline_id: PipelineId) -> Revision {
//     let new_revision_id = Uuid::now_v7();
//     let r = handle
//         .db
//         .create_pipeline_deployment(new_revision_id, tenant_id, pipeline_id)
//         .await
//         .unwrap();
//     assert_eq!(r.0, new_revision_id);
//
//     // We get an error the 2nd time since nothing changed
//     let e = handle
//         .db
//         .create_pipeline_deployment(new_revision_id, tenant_id, pipeline_id)
//         .await
//         .unwrap_err();
//     match e {
//         DBError::RevisionNotChanged => r,
//         e => {
//             // We should get a RevisionNotChanged error here since we
//             // didn't change anything inbetween
//             panic!("unexpected error trying to create revision 2nd time: {}", e)
//         }
//     }
// }
//
//
// #[tokio::test]
// async fn versioning_no_change_no_connectors() {
//     let _r = env_logger::try_init();
//
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//
//     let (program_id, _) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "",
//             "",
//             "",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_status_guarded(tenant_id, program_id, Version(1), ProgramStatus::Success)
//         .await
//         .unwrap();
//     let rc = RuntimeConfig::from_yaml("");
//     let (pipeline_id, _version) = handle
//         .db
//         .new_pipeline(
//             tenant_id,
//             Uuid::now_v7(),
//             &Some("".to_string()),
//             "",
//             "",
//             &rc,
//             &None,
//             None,
//         )
//         .await
//         .unwrap();
//     let _r = handle
//         .db
//         .set_program_schema(
//             tenant_id,
//             program_id,
//             ProgramSchema {
//                 inputs: vec![],
//                 outputs: vec![],
//             },
//         )
//         .await
//         .unwrap();
//     let _r1: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
// }
//
// #[tokio::test]
// async fn versioning() {
//     let _r = env_logger::try_init();
//
//     let handle = test_setup().await;
//     let tenant_id = TenantRecord::default().id;
//
//     let (program_id, _) = handle
//         .db
//         .new_program(
//             tenant_id,
//             Uuid::now_v7(),
//             "test1",
//             "program desc",
//             "only schema matters--this isn't compiled",
//             &ProgramConfig {
//                 profile: Some(CompilationProfile::Unoptimized),
//             },
//             None,
//         )
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_status_guarded(tenant_id, program_id, Version(1), ProgramStatus::Success)
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_schema(
//             tenant_id,
//             program_id,
//             ProgramSchema {
//                 inputs: vec![Relation::new("t1", false, vec![], false)],
//                 outputs: vec![Relation::new("v1", false, vec![], false)],
//             },
//         )
//         .await
//         .unwrap();
//     let config1 = test_connector_config();
//     let mut config2 = config1.clone();
//     config2.max_queued_records = config1.max_queued_records + 5;
//
//     let connector_id1: ConnectorId = handle
//         .db
//         .new_connector(tenant_id, Uuid::now_v7(), "a", "b", &config1, None)
//         .await
//         .unwrap();
//     let mut ac1 = AttachedConnector {
//         name: "ac1".to_string(),
//         is_input: true,
//         connector_name: "a".to_string(),
//         relation_name: "t1".to_string(),
//     };
//     handle
//         .db
//         .new_connector(tenant_id, Uuid::now_v7(), "d", "e", &config2, None)
//         .await
//         .unwrap();
//     let mut ac2 = AttachedConnector {
//         name: "ac2".to_string(),
//         is_input: false,
//         connector_name: "d".to_string(),
//         relation_name: "v1".to_string(),
//     };
//     let rc = RuntimeConfig::from_yaml("");
//     let (pipeline_id, _version) = handle
//         .db
//         .new_pipeline(
//             tenant_id,
//             Uuid::now_v7(),
//             &Some("test1".to_string()),
//             "1",
//             "2",
//             &rc,
//             &Some(vec![ac1.clone(), ac2.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//     let r1: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
//
//     // If we change the program we need to adjust the connectors before we can
//     // commit again:
//     let new_version = handle
//         .db
//         .update_program(
//             tenant_id,
//             program_id,
//             &None,
//             &None,
//             &Some("only schema matters--this isn't compiled2".to_string()),
//             &None,
//             &None,
//             &None,
//             None,
//             None,
//         )
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_status_guarded(tenant_id, program_id, Version(2), ProgramStatus::Success)
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_schema(
//             tenant_id,
//             program_id,
//             ProgramSchema {
//                 inputs: vec![
//                     Relation::new("t1", false, vec![], false),
//                     Relation::new("t2", false, vec![], false),
//                 ],
//                 outputs: vec![Relation::new("v1", false, vec![], false)],
//             },
//         )
//         .await
//         .unwrap();
//     let r2: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
//     assert_ne!(r1, r2, "we got a new revision");
//
//     handle
//         .db
//         .set_program_status_guarded(tenant_id, program_id, new_version, ProgramStatus::Success)
//         .await
//         .unwrap();
//     handle
//         .db
//         .set_program_schema(
//             tenant_id,
//             program_id,
//             ProgramSchema {
//                 inputs: vec![Relation::new("tnew1", false, vec![], false)],
//                 outputs: vec![Relation::new("vnew1", false, vec![], false)],
//             },
//         )
//         .await
//         .unwrap();
//     // This doesn't work because the connectors reference (now invalid) tables:
//     assert!(handle
//         .db
//         .create_pipeline_deployment(Uuid::now_v7(), tenant_id, pipeline_id)
//         .await
//         .is_err());
//     ac1.is_input = true;
//     ac1.relation_name = "tnew1".into();
//     let gp_config = RuntimeConfig {
//         workers: 1,
//         cpu_profiler: true,
//         storage: false,
//         tracing: false,
//         tracing_endpoint_jaeger: "".to_string(),
//         min_batch_size_records: 0,
//         max_buffering_delay_usecs: 0,
//         resources: ResourceConfig::default(),
//         min_storage_bytes: None,
//     };
//     handle
//         .db
//         .update_pipeline(
//             tenant_id,
//             pipeline_id,
//             &Some("test1".into()),
//             "1",
//             "2",
//             &Some(gp_config.clone()),
//             &Some(vec![ac1.clone(), ac2.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//     // This doesn't work because the ac2 still references a wrong table
//     assert!(handle
//         .db
//         .create_pipeline_deployment(Uuid::now_v7(), tenant_id, pipeline_id)
//         .await
//         .is_err());
//     // Let's fix ac2
//     ac2.is_input = false;
//     ac2.relation_name = "vnew1".into();
//     handle
//         .db
//         .update_pipeline(
//             tenant_id,
//             pipeline_id,
//             &Some("test1".into()),
//             "1",
//             "2",
//             &Some(gp_config.clone()),
//             &Some(vec![ac1.clone(), ac2.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//
//     // Now we can commit again
//     let r3 = commit_check(&handle, tenant_id, pipeline_id).await;
//     assert_ne!(r2, r3, "we got a new revision");
//
//     // If we change the connector we can commit again:
//     let mut config3 = config2.clone();
//     config3.max_queued_records += 5;
//     handle
//         .db
//         .update_connector(
//             tenant_id,
//             connector_id1,
//             &Some("a"),
//             &Some("b"),
//             &Some(config3),
//             None,
//         )
//         .await
//         .unwrap();
//     let r4 = commit_check(&handle, tenant_id, pipeline_id).await;
//     assert_ne!(r3, r4, "we got a new revision");
//
//     // If we change the attached connectors we can commit again:
//     ac1.name = "xxx".into();
//     handle
//         .db
//         .update_pipeline(
//             tenant_id,
//             pipeline_id,
//             &Some("test1".into()),
//             "1",
//             "2",
//             &Some(gp_config.clone()),
//             &Some(vec![ac1.clone(), ac2.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//     let r5: Revision = commit_check(&handle, tenant_id, pipeline_id).await;
//     assert_ne!(r4, r5, "we got a new revision");
//
//     // If we remove an ac that's also a change:
//     handle
//         .db
//         .update_pipeline(
//             tenant_id,
//             pipeline_id,
//             &Some("test1".into()),
//             "1",
//             "2",
//             &Some(gp_config.clone()),
//             &Some(vec![ac1.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//     let r6 = commit_check(&handle, tenant_id, pipeline_id).await;
//     assert_ne!(r5, r6, "we got a new revision");
//
//     // And if we change the pipeline config itself that's a change:
//     handle
//         .db
//         .update_pipeline(
//             tenant_id,
//             pipeline_id,
//             &Some("test1".into()),
//             "1",
//             "2",
//             &Some(RuntimeConfig {
//                 workers: gp_config.workers + 1,
//                 ..gp_config
//             }),
//             &Some(vec![ac1.clone()]),
//             None,
//         )
//         .await
//         .unwrap();
//     let r6 = commit_check(&handle, tenant_id, pipeline_id).await;
//     assert_ne!(r5, r6, "we got a new revision");
// }
//

//////////////////////////////////////////////////////////////////////////////
/////                           PROP TESTS                               /////

/// Actions we can do on the Storage trait.
#[derive(Debug, Clone, Arbitrary)]
enum StorageAction {
    // API keys
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
    // Pipelines
    ListPipelines(TenantId),
    GetPipeline(TenantId, String),
    GetPipelineById(TenantId, PipelineId),
    NewPipeline(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        #[proptest(strategy = "limited_pipeline_descr()")] PipelineDescr,
    ),
    NewOrUpdatePipeline(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        #[proptest(strategy = "limited_pipeline_name()")] String,
        #[proptest(strategy = "limited_pipeline_descr()")] PipelineDescr,
    ),
    UpdatePipeline(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
        Option<String>,
        Option<String>,
        #[proptest(strategy = "limited_option_runtime_config()")] Option<RuntimeConfig>,
        Option<String>,
        #[proptest(strategy = "limited_option_program_config()")] Option<ProgramConfig>,
    ),
    DeletePipeline(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    TransitProgramStatusToPending(TenantId, PipelineId, Version),
    TransitProgramStatusToCompilingSql(TenantId, PipelineId, Version),
    TransitProgramStatusToCompilingRust(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_program_info()")] ProgramInfo,
    ),
    TransitProgramStatusToSuccess(TenantId, PipelineId, Version, String),
    TransitProgramStatusToSqlError(TenantId, PipelineId, Version, Vec<SqlCompilerMessage>),
    TransitProgramStatusToRustError(TenantId, PipelineId, Version, String),
    TransitProgramStatusToSystemError(TenantId, PipelineId, Version, String),
    SetDeploymentDesiredStatusRunning(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    SetDeploymentDesiredStatusPaused(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    SetDeploymentDesiredStatusShutdown(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    TransitDeploymentStatusToProvisioning(
        TenantId,
        PipelineId,
        #[proptest(strategy = "limited_pipeline_config()")] PipelineConfig,
    ),
    TransitDeploymentStatusToInitializing(TenantId, PipelineId, String),
    TransitDeploymentStatusToRunning(TenantId, PipelineId),
    TransitDeploymentStatusToPaused(TenantId, PipelineId),
    TransitDeploymentStatusToShuttingDown(TenantId, PipelineId),
    TransitDeploymentStatusToShutdown(TenantId, PipelineId),
    TransitDeploymentStatusToFailed(
        TenantId,
        PipelineId,
        #[proptest(strategy = "limited_error_response()")] ErrorResponse,
    ),
    ListPipelineIdsAcrossAllTenants,
    ListPipelinesAcrossAllTenants,
    GetNextPipelineProgramToCompile,
    IsPipelineProgramInUse(PipelineId, Version),
}

/// Alias for a result from the database.
type DBResult<T> = Result<T, DBError>;

/// Converts the pipeline to a variant with constant timestamps, which
/// is useful for comparison between model and result which will generate
/// different timestamps.
fn convert_pipeline_with_constant_timestamps(
    mut pipeline: ExtendedPipelineDescr,
) -> ExtendedPipelineDescr {
    let timestamp = Utc.timestamp_nanos(0);
    pipeline.created_at = timestamp;
    pipeline.program_status_since = timestamp;
    pipeline.deployment_status_since = timestamp;
    pipeline
}

/// Check database responses by direct comparison.
fn check_responses<T: Debug + PartialEq>(
    step: usize,
    result_model: DBResult<T>,
    result_impl: DBResult<T>,
) {
    match (result_model, result_impl) {
        (Ok(val_model), Ok(val_impl)) => assert_eq!(
            val_model, val_impl,
            "mismatch detected with model (left) and right (impl)"
        ),
        (Err(err_model), Ok(val_impl)) => {
            panic!("step({step}): model returned error: {err_model:?}, but impl returned result: {val_impl:?}");
        }
        (Ok(val_model), Err(err_impl)) => {
            panic!("step({step}): model returned result: {val_model:?}, but impl returned error: {err_impl:?}");
        }
        (Err(err_model), Err(err_impl)) => {
            assert_eq!(
                err_model.to_string(),
                err_impl.to_string(),
                "step({step}): error return mismatch (left = model, right = impl)"
            );
        }
    }
}

/// Ignores timestamps.
fn check_response_pipeline(
    step: usize,
    mut result_model: DBResult<ExtendedPipelineDescr>,
    mut result_impl: DBResult<ExtendedPipelineDescr>,
) {
    result_model = result_model.map(convert_pipeline_with_constant_timestamps);
    result_impl = result_impl.map(convert_pipeline_with_constant_timestamps);
    check_responses(step, result_model, result_impl);
}

/// Ignores timestamps.
fn check_response_pipeline_with_created(
    step: usize,
    mut result_model: DBResult<(bool, ExtendedPipelineDescr)>,
    mut result_impl: DBResult<(bool, ExtendedPipelineDescr)>,
) {
    result_model = result_model
        .map(|(created, pipeline)| (created, convert_pipeline_with_constant_timestamps(pipeline)));
    result_impl = result_impl
        .map(|(created, pipeline)| (created, convert_pipeline_with_constant_timestamps(pipeline)));
    check_responses(step, result_model, result_impl);
}

/// Ignores timestamps.
fn check_response_optional_pipeline_with_tenant_id(
    step: usize,
    mut result_model: DBResult<Option<(TenantId, ExtendedPipelineDescr)>>,
    mut result_impl: DBResult<Option<(TenantId, ExtendedPipelineDescr)>>,
) {
    result_model = result_model.map(|v| {
        v.map(|(tenant_id, pipeline)| {
            (
                tenant_id,
                convert_pipeline_with_constant_timestamps(pipeline),
            )
        })
    });
    result_impl = result_impl.map(|v| {
        v.map(|(tenant_id, pipeline)| {
            (
                tenant_id,
                convert_pipeline_with_constant_timestamps(pipeline),
            )
        })
    });
    check_responses(step, result_model, result_impl);
}

/// Ignores timestamps.
fn check_response_pipelines(
    step: usize,
    mut result_model: DBResult<Vec<ExtendedPipelineDescr>>,
    mut result_impl: DBResult<Vec<ExtendedPipelineDescr>>,
) {
    result_model = result_model.map(|mut v| {
        v.sort_by(|p1, p2| p1.id.cmp(&p2.id));
        v.into_iter()
            .map(convert_pipeline_with_constant_timestamps)
            .collect()
    });
    result_impl = result_impl.map(|mut v| {
        v.sort_by(|p1, p2| p1.id.cmp(&p2.id));
        v.into_iter()
            .map(convert_pipeline_with_constant_timestamps)
            .collect()
    });
    check_responses(step, result_model, result_impl);
}

/// Ignores timestamps and ordering.
fn check_response_pipelines_with_tenant_id(
    step: usize,
    mut result_model: DBResult<Vec<(TenantId, ExtendedPipelineDescr)>>,
    mut result_impl: DBResult<Vec<(TenantId, ExtendedPipelineDescr)>>,
) {
    result_model = result_model.map(|mut v| {
        v.sort_by(|(t1, p1), (t2, p2)| (t1, p1.id).cmp(&(t2, p2.id)));
        v.into_iter()
            .map(|(tenant_id, pipeline)| {
                (
                    tenant_id,
                    convert_pipeline_with_constant_timestamps(pipeline),
                )
            })
            .collect()
    });
    result_impl = result_impl.map(|mut v| {
        v.sort_by(|(t1, p1), (t2, p2)| (t1, p1.id).cmp(&(t2, p2.id)));
        v.into_iter()
            .map(|(tenant_id, pipeline)| {
                (
                    tenant_id,
                    convert_pipeline_with_constant_timestamps(pipeline),
                )
            })
            .collect()
    });
    check_responses(step, result_model, result_impl);
}

/// Ignores ordering.
fn check_response_pipeline_ids_with_tenant_id(
    step: usize,
    mut result_model: DBResult<Vec<(TenantId, PipelineId)>>,
    mut result_impl: DBResult<Vec<(TenantId, PipelineId)>>,
) {
    result_model = result_model.map(|mut v| {
        v.sort_by(|(t1, p1), (t2, p2)| (t1, p1).cmp(&(t2, p2)));
        v
    });
    result_impl = result_impl.map(|mut v| {
        v.sort_by(|(t1, p1), (t2, p2)| (t1, p1).cmp(&(t2, p2)));
        v
    });
    check_responses(step, result_model, result_impl);
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
            |actions: Vec<StorageAction>| {
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

                            StorageAction::ListPipelines(tenant_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_pipelines(tenant_id).await;
                                let impl_response = handle.db.list_pipelines(tenant_id).await;
                                check_response_pipelines(i, model_response, impl_response);
                            }
                            StorageAction::GetPipeline(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.get_pipeline(tenant_id, &pipeline_name).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineById(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_by_id(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_by_id(tenant_id, pipeline_id).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::NewPipeline(tenant_id, new_id, pipeline_descr) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.new_pipeline(tenant_id, new_id, pipeline_descr.clone()).await;
                                let impl_response = handle.db.new_pipeline(tenant_id, new_id, pipeline_descr.clone()).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::NewOrUpdatePipeline(tenant_id, new_id, original_name, pipeline_descr) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.new_or_update_pipeline(tenant_id, new_id, &original_name, pipeline_descr.clone()).await;
                                let impl_response = handle.db.new_or_update_pipeline(tenant_id, new_id, &original_name, pipeline_descr.clone()).await;
                                check_response_pipeline_with_created(i, model_response, impl_response);
                            }
                            StorageAction::UpdatePipeline(tenant_id, original_name, name, description, runtime_config, program_code, program_config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.update_pipeline(tenant_id, &original_name, &name, &description, &runtime_config, &program_code, &program_config).await;
                                let impl_response = handle.db.update_pipeline(tenant_id, &original_name, &name, &description, &runtime_config, &program_code, &program_config).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::DeletePipeline(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.delete_pipeline(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.delete_pipeline(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToPending(tenant_id, pipeline_id, program_version_guard) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_pending(tenant_id, pipeline_id, program_version_guard).await;
                                let impl_response = handle.db.transit_program_status_to_pending(tenant_id, pipeline_id, program_version_guard).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToCompilingSql(tenant_id, pipeline_id, program_version_guard) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_compiling_sql(tenant_id, pipeline_id, program_version_guard).await;
                                let impl_response = handle.db.transit_program_status_to_compiling_sql(tenant_id, pipeline_id, program_version_guard).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToCompilingRust(tenant_id, pipeline_id, program_version_guard, program_info) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_compiling_rust(tenant_id, pipeline_id, program_version_guard, &program_info).await;
                                let impl_response = handle.db.transit_program_status_to_compiling_rust(tenant_id, pipeline_id, program_version_guard, &program_info).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToSuccess(tenant_id, pipeline_id, program_version_guard, program_binary_url) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_success(tenant_id, pipeline_id, program_version_guard, &program_binary_url).await;
                                let impl_response = handle.db.transit_program_status_to_success(tenant_id, pipeline_id, program_version_guard, &program_binary_url).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToSqlError(tenant_id, pipeline_id, program_version_guard, internal_sql_error) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_sql_error(tenant_id, pipeline_id, program_version_guard, internal_sql_error.clone()).await;
                                let impl_response = handle.db.transit_program_status_to_sql_error(tenant_id, pipeline_id, program_version_guard, internal_sql_error.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToRustError(tenant_id, pipeline_id, program_version_guard, internal_rust_error) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_rust_error(tenant_id, pipeline_id, program_version_guard, &internal_rust_error).await;
                                let impl_response = handle.db.transit_program_status_to_rust_error(tenant_id, pipeline_id, program_version_guard, &internal_rust_error).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToSystemError(tenant_id, pipeline_id, program_version_guard, internal_system_error) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_system_error(tenant_id, pipeline_id, program_version_guard, &internal_system_error).await;
                                let impl_response = handle.db.transit_program_status_to_system_error(tenant_id, pipeline_id, program_version_guard, &internal_system_error).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetDeploymentDesiredStatusRunning(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.set_deployment_desired_status_running(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.set_deployment_desired_status_running(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetDeploymentDesiredStatusPaused(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.set_deployment_desired_status_paused(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.set_deployment_desired_status_paused(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetDeploymentDesiredStatusShutdown(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.set_deployment_desired_status_shutdown(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.set_deployment_desired_status_shutdown(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToProvisioning(tenant_id, pipeline_id, pipeline_config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_provisioning(tenant_id, pipeline_id, pipeline_config.clone()).await;
                                let impl_response = handle.db.transit_deployment_status_to_provisioning(tenant_id, pipeline_id, pipeline_config.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToInitializing(tenant_id, pipeline_id, deployment_location) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_initializing(tenant_id, pipeline_id, &deployment_location).await;
                                let impl_response = handle.db.transit_deployment_status_to_initializing(tenant_id, pipeline_id, &deployment_location).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToRunning(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_running(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.transit_deployment_status_to_running(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToPaused(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_paused(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.transit_deployment_status_to_paused(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToShuttingDown(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_shutting_down(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.transit_deployment_status_to_shutting_down(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToShutdown(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_shutdown(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.transit_deployment_status_to_shutdown(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentStatusToFailed(tenant_id, pipeline_id, deployment_error) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_status_to_failed(tenant_id, pipeline_id, &deployment_error).await;
                                let impl_response = handle.db.transit_deployment_status_to_failed(tenant_id, pipeline_id, &deployment_error).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelineIdsAcrossAllTenants => {
                                let model_response = model.list_pipeline_ids_across_all_tenants().await;
                                let impl_response = handle.db.list_pipeline_ids_across_all_tenants().await;
                                check_response_pipeline_ids_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelinesAcrossAllTenants => {
                                let model_response = model.list_pipelines_across_all_tenants().await;
                                let impl_response = handle.db.list_pipelines_across_all_tenants().await;
                                check_response_pipelines_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::GetNextPipelineProgramToCompile => {
                                let model_response = model.get_next_pipeline_program_to_compile().await;
                                let impl_response = handle.db.get_next_pipeline_program_to_compile().await;
                                check_response_optional_pipeline_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::IsPipelineProgramInUse(pipeline_id, program_version) => {
                                let model_response = model.is_pipeline_program_in_use(pipeline_id, program_version).await;
                                let impl_response = handle.db.is_pipeline_program_in_use(pipeline_id, program_version).await;
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

/// Model of the database to which its operations are compared.
#[derive(Debug, Default)]
struct DbModel {
    pub tenants: BTreeMap<TenantId, TenantRecord>,
    pub api_keys: BTreeMap<(TenantId, String), (ApiKeyId, String, Vec<ApiPermission>)>,
    pub pipelines: BTreeMap<(TenantId, PipelineId), ExtendedPipelineDescr>,
}

#[async_trait]
trait ModelHelpers {
    /// Fetches the existing pipeline, checks the version guard matches,
    /// checks the transition is valid. Returns the pipeline.
    async fn help_transit_program_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        new_status: &ProgramStatus,
    ) -> Result<ExtendedPipelineDescr, DBError>;

    /// Fetches the existing pipeline, checks the program is compiled,
    /// and checks the transition is valid. Returns the pipeline.
    async fn help_transit_deployment_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        new_status: &PipelineStatus,
    ) -> Result<ExtendedPipelineDescr, DBError>;

    /// Fetches the existing pipeline, checks the transition is valid,
    /// and checks the program is compiled. Returns the pipeline.
    async fn help_transit_deployment_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        new_desired_status: &PipelineStatus,
    ) -> Result<ExtendedPipelineDescr, DBError>;
}

#[async_trait]
impl ModelHelpers for Mutex<DbModel> {
    async fn help_transit_program_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        new_status: &ProgramStatus,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        // Fetch existing pipeline
        let pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;

        // Pipeline must be shutdown
        if !pipeline.is_fully_shutdown() {
            return Err(DBError::CannotUpdateNonShutdownPipeline);
        }

        // Check version guard
        if pipeline.program_version != program_version_guard {
            return Err(DBError::OutdatedProgramVersion {
                latest_version: pipeline.program_version,
            });
        }

        // Check transition
        validate_program_status_transition(&pipeline.program_status, &new_status)?;

        // Return fetched pipeline
        Ok(pipeline)
    }

    async fn help_transit_deployment_status(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        new_status: &PipelineStatus,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        // Fetch existing pipeline
        let pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;

        // Check program is compiled
        if pipeline.program_status.has_failed_to_compile() {
            return Err(DBError::ProgramFailedToCompile);
        }
        if !pipeline.program_status.is_fully_compiled() {
            return Err(DBError::ProgramNotYetCompiled);
        }

        // Check transition
        validate_deployment_status_transition(&pipeline.deployment_status, &new_status)?;

        // Return fetched pipeline
        Ok(pipeline)
    }

    async fn help_transit_deployment_desired_status(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        new_desired_status: &PipelineStatus,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        // Fetch existing pipeline
        let pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;

        // Check transition
        validate_deployment_desired_status_transition(
            &pipeline.deployment_status,
            &pipeline.deployment_desired_status,
            new_desired_status,
        )?;

        // Check program is compiled
        if *new_desired_status != PipelineStatus::Shutdown {
            if pipeline.program_status.has_failed_to_compile() {
                return Err(DBError::ProgramFailedToCompile);
            }
            if !pipeline.program_status.is_fully_compiled() {
                return Err(DBError::ProgramNotYetCompiled);
            }
        }

        // Return fetched pipeline
        Ok(pipeline)
    }
}

#[async_trait]
impl Storage for Mutex<DbModel> {
    async fn check_connection(&self) -> Result<(), DBError> {
        panic!("The connection check is not part of the proptest");
    }

    async fn get_or_create_tenant_id(
        &self,
        _new_id: Uuid,
        _tenant_name: String,
        _provider: String,
    ) -> DBResult<TenantId> {
        panic!("For model-based tests, we generate the TenantID using proptest, as opposed to generating a claim that we then get or create an ID for");
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
            None => Err(DBError::InvalidApiKey),
        }
    }

    async fn list_pipelines(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<ExtendedPipelineDescr>, DBError> {
        Ok(self
            .lock()
            .await
            .pipelines
            .iter()
            .filter(|(key, _)| key.0 == tenant_id)
            .map(|(_, pipeline)| pipeline.clone())
            .collect())
    }

    async fn get_pipeline(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        self.lock()
            .await
            .pipelines
            .iter()
            .filter(|((tid, _), _)| *tid == tenant_id)
            .map(|(_, pipeline)| pipeline.clone())
            .find(|pipeline| pipeline.name == name)
            .ok_or(DBError::UnknownPipelineName {
                pipeline_name: name.to_string(),
            })
    }

    async fn get_pipeline_by_id(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        self.lock()
            .await
            .pipelines
            .get(&(tenant_id, pipeline_id))
            .cloned()
            .ok_or(DBError::UnknownPipeline { pipeline_id })
    }

    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        new_id: Uuid,
        pipeline: PipelineDescr,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut state = self.lock().await;

        // Constraint: UUID is unique
        if state.pipelines.keys().any(|(_, pid)| pid.0 == new_id) {
            return Err(DBError::unique_key_violation("pipeline_pkey"));
        }

        // Constraint: name is unique
        if let Some(_) = state
            .pipelines
            .iter()
            .filter(|((tid, _), _)| *tid == tenant_id)
            .map(|(_, p)| p)
            .find(|p| p.name == pipeline.name)
        {
            return Err(DBError::DuplicateName.into());
        }

        // Create extended descriptor
        let pipeline_id = PipelineId(new_id);
        let now = Utc::now();
        let extended_pipeline = ExtendedPipelineDescr {
            id: pipeline_id,
            name: pipeline.name,
            description: pipeline.description,
            version: Version(1),
            created_at: now,
            runtime_config: pipeline.runtime_config,
            program_code: pipeline.program_code,
            program_config: pipeline.program_config,
            program_version: Version(1),
            program_status: ProgramStatus::Pending,
            program_status_since: now,
            program_info: None,
            program_binary_url: None,
            deployment_status: PipelineStatus::Shutdown,
            deployment_status_since: now,
            deployment_desired_status: PipelineStatus::Shutdown,
            deployment_error: None,
            deployment_config: None,
            deployment_location: None,
        };

        // Insert into state
        state
            .pipelines
            .insert((tenant_id, pipeline_id), extended_pipeline.clone());

        // Return the extended pipeline descriptor
        Ok(extended_pipeline)
    }

    async fn new_or_update_pipeline(
        &self,
        tenant_id: TenantId,
        new_id: Uuid,
        original_name: &str,
        pipeline: PipelineDescr,
    ) -> Result<(bool, ExtendedPipelineDescr), DBError> {
        match self.get_pipeline(tenant_id, original_name).await {
            Ok(_) => Ok((
                false,
                self.update_pipeline(
                    tenant_id,
                    original_name,
                    &Some(pipeline.name),
                    &Some(pipeline.description),
                    &Some(pipeline.runtime_config),
                    &Some(pipeline.program_code),
                    &Some(pipeline.program_config),
                )
                .await?,
            )),
            Err(e) => match e {
                DBError::UnknownPipelineName { .. } => {
                    if original_name != pipeline.name {
                        return Err(DBError::CannotRenameNonExistingPipeline);
                    }
                    Ok((true, self.new_pipeline(tenant_id, new_id, pipeline).await?))
                }
                _ => Err(e),
            },
        }
    }

    async fn update_pipeline(
        &self,
        tenant_id: TenantId,
        original_name: &str,
        name: &Option<String>,
        description: &Option<String>,
        runtime_config: &Option<RuntimeConfig>,
        program_code: &Option<String>,
        program_config: &Option<ProgramConfig>,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        // Fetch existing pipeline
        let mut pipeline = self.get_pipeline(tenant_id, original_name).await?;

        // Pipeline must be shutdown
        if !pipeline.is_fully_shutdown() {
            return Err(DBError::CannotUpdateNonShutdownPipeline);
        }

        // Update the base fields
        let mut version_increment = false;
        let mut program_version_increment = false;
        if let Some(name) = name {
            // Constraint: name is unique
            if let Some(_) = self
                .lock()
                .await
                .pipelines
                .iter()
                .filter(|((tid, _), _)| *tid == tenant_id)
                .map(|(_, p)| p)
                .find(|p| p.name == *name && p.id != pipeline.id)
            {
                return Err(DBError::DuplicateName.into());
            }

            if *name != pipeline.name {
                version_increment = true;
            }
            pipeline.name = name.clone();
        }
        if let Some(description) = description {
            if *description != pipeline.description {
                version_increment = true;
            }
            pipeline.description = description.clone();
        }
        if let Some(runtime_config) = runtime_config {
            if *runtime_config != pipeline.runtime_config {
                version_increment = true;
            }
            pipeline.runtime_config = runtime_config.clone();
        }
        if let Some(program_code) = program_code {
            if *program_code != pipeline.program_code {
                version_increment = true;
                program_version_increment = true;
            }
            pipeline.program_code = program_code.clone();
        }
        if let Some(program_config) = program_config {
            if *program_config != pipeline.program_config {
                version_increment = true;
                program_version_increment = true;
            }
            pipeline.program_config = program_config.clone();
        }

        // Next, update any fields that are a consequence of base field changes

        // Version changed: increment it
        if version_increment {
            pipeline.version = Version(pipeline.version.0 + 1);
        }

        // Program changed
        if program_version_increment {
            pipeline.program_version = Version(pipeline.program_version.0 + 1);
            pipeline.program_status = ProgramStatus::Pending;
            pipeline.program_status_since = Utc::now();
            pipeline.program_info = None;
            pipeline.program_binary_url = None;
        }

        // Insert into state (will overwrite)
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());

        // Return the final extended pipeline descriptor
        Ok(pipeline)
    }

    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        // Fetch existing pipeline
        let pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;

        // Pipeline must be shutdown
        if !pipeline.is_fully_shutdown() {
            return Err(DBError::CannotDeleteNonShutdownPipeline);
        }

        // Delete from state
        self.lock()
            .await
            .pipelines
            .remove(&(tenant_id, pipeline.id));
        Ok(pipeline.id)
    }

    async fn transit_program_status_to_pending(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::Pending;
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_info = None;
        pipeline.program_binary_url = None;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_compiling_sql(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::CompilingSql;
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_compiling_rust(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        program_info: &ProgramInfo,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::CompilingRust;
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_info = Some(program_info.clone());
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_success(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        program_binary_url: &str,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::Success;
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_binary_url = Some(program_binary_url.to_string());
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_sql_error(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        internal_sql_error: Vec<SqlCompilerMessage>,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::SqlError(internal_sql_error);
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_rust_error(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        internal_rust_error: &str,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::RustError(internal_rust_error.to_string());
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_system_error(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        internal_system_error: &str,
    ) -> Result<(), DBError> {
        let new_status = ProgramStatus::SystemError(internal_system_error.to_string());
        let mut pipeline = self
            .help_transit_program_status(tenant_id, pipeline_id, program_version_guard, &new_status)
            .await?;
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn set_deployment_desired_status_running(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        let new_desired_status = PipelineStatus::Running;
        let mut pipeline = self
            .help_transit_deployment_desired_status(tenant_id, pipeline_name, &new_desired_status)
            .await?;
        pipeline.deployment_desired_status = new_desired_status;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn set_deployment_desired_status_paused(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        let new_desired_status = PipelineStatus::Paused;
        let mut pipeline = self
            .help_transit_deployment_desired_status(tenant_id, pipeline_name, &new_desired_status)
            .await?;
        pipeline.deployment_desired_status = new_desired_status;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn set_deployment_desired_status_shutdown(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        let new_desired_status = PipelineStatus::Shutdown;
        let mut pipeline = self
            .help_transit_deployment_desired_status(tenant_id, pipeline_name, &new_desired_status)
            .await?;
        pipeline.deployment_desired_status = new_desired_status;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_provisioning(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        deployment_config: PipelineConfig,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::Provisioning;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        pipeline.deployment_config = Some(deployment_config);
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_initializing(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        deployment_location: &str,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::Initializing;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        pipeline.deployment_location = Some(deployment_location.to_string());
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_running(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::Running;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_paused(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::Paused;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_shutting_down(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::ShuttingDown;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_shutdown(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::Shutdown;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        pipeline.deployment_config = None;
        pipeline.deployment_location = None;
        pipeline.deployment_error = None;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_deployment_status_to_failed(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        deployment_error: &ErrorResponse,
    ) -> Result<(), DBError> {
        let new_status = PipelineStatus::Failed;
        let mut pipeline = self
            .help_transit_deployment_status(tenant_id, pipeline_id, &new_status)
            .await?;
        pipeline.deployment_status = new_status;
        pipeline.deployment_status_since = Utc::now();
        pipeline.deployment_error = Some(deployment_error.clone());
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn list_pipeline_ids_across_all_tenants(
        &self,
    ) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
        Ok(self
            .lock()
            .await
            .pipelines
            .iter()
            .map(|((tid, pid), _)| (*tid, *pid))
            .collect())
    }

    async fn list_pipelines_across_all_tenants(
        &self,
    ) -> Result<Vec<(TenantId, ExtendedPipelineDescr)>, DBError> {
        Ok(self
            .lock()
            .await
            .pipelines
            .iter()
            .map(|((tid, _), pipeline)| (*tid, pipeline.clone()))
            .collect())
    }

    async fn get_next_pipeline_program_to_compile(
        &self,
    ) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut pipelines: Vec<(TenantId, ExtendedPipelineDescr)> = self
            .lock()
            .await
            .pipelines
            .iter()
            .filter(|(_, p)| p.program_status == ProgramStatus::Pending)
            .map(|((tid, _), pipeline)| (*tid, pipeline.clone()))
            .collect();
        if pipelines.is_empty() {
            return Ok(None);
        }
        pipelines.sort_by(|(_, p1), (_, p2)| {
            (p1.program_status_since, p1.id).cmp(&(p2.program_status_since, p2.id))
        });
        let chosen = pipelines.get(0).unwrap().clone(); // Already checked for empty
        Ok(Some((chosen.0, chosen.1)))
    }

    async fn is_pipeline_program_in_use(
        &self,
        pipeline_id: PipelineId,
        program_version: Version,
    ) -> Result<bool, DBError> {
        let state = self.lock().await;
        let pipeline = state
            .pipelines
            .iter()
            .find(|((_, pid), _)| *pid == pipeline_id)
            .map(|(_, pipeline)| pipeline);
        if let Some(pipeline) = pipeline {
            Ok(pipeline.program_version == program_version)
        } else {
            Ok(false)
        }
    }
}
