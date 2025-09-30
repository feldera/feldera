use crate::api::lifecycle_events::PipelineLifecycleEvent;
use crate::api::support_data_collector::SupportBundleData;
use crate::auth::{generate_api_key, TenantRecord};
use crate::db::error::DBError;
use crate::db::storage::{ExtendedPipelineDescrRunner, Storage};
use crate::db::storage_postgres::StoragePostgres;
use crate::db::types::api_key::{ApiKeyDescr, ApiKeyId, ApiPermission};
use crate::db::types::pipeline::{
    ExtendedPipelineDescr, ExtendedPipelineDescrMonitoring, PipelineDescr, PipelineId,
};
use crate::db::types::program::{
    generate_pipeline_config, validate_program_status_transition, CompilationProfile,
    ProgramConfig, ProgramError, ProgramInfo, ProgramStatus, RustCompilationInfo,
    SqlCompilationInfo,
};
use crate::db::types::resources_status::{
    validate_resources_desired_status_transition, validate_resources_status_transition,
    ResourcesDesiredStatus, ResourcesStatus,
};
use crate::db::types::storage::{validate_storage_status_transition, StorageStatus};
use crate::db::types::tenant::TenantId;
use crate::db::types::utils::{
    validate_deployment_config, validate_name, validate_program_config, validate_program_info,
    validate_runtime_config,
};
use crate::db::types::version::Version;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use feldera_types::config::{FtConfig, PipelineConfig, ResourceConfig, RuntimeConfig};
use feldera_types::error::ErrorResponse;
use feldera_types::program_schema::ProgramSchema;
use feldera_types::runtime_status::{ExtendedRuntimeStatus, RuntimeDesiredStatus, RuntimeStatus};
use log::info;
use openssl::sha;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use proptest_derive::Arbitrary;
use serde_json::json;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::Duration;
use std::vec;
use tokio::sync::Mutex;
use tokio::time::sleep;
use uuid::Uuid;

struct DbHandle {
    db: StoragePostgres,
    #[cfg(feature = "postgresql_embedded")]
    _temp_dir: tempfile::TempDir,
    #[cfg(not(feature = "postgresql_embedded"))]
    config: tokio_postgres::Config,
}

impl Drop for DbHandle {
    #[cfg(feature = "postgresql_embedded")]
    fn drop(&mut self) {
        // We drop `pg` before the temp dir gets deleted (which will
        // shut down postgres). Otherwise, postgres logs an error that the
        // directory is already gone during shutdown which could be
        // confusing for a developer.
        if let Some(pg) = self.db.pg_inst.as_mut() {
            let _r = async {
                pg.stop().await.unwrap();
            };
        }
    }

    #[cfg(not(feature = "postgresql_embedded"))]
    fn drop(&mut self) {
        use postgres_openssl::TlsStream;
        use tokio_postgres::{tls::NoTlsStream, Connection, Socket};
        enum ConnWrapper {
            Tls(Connection<Socket, TlsStream<Socket>>),
            NoTls(Connection<Socket, NoTlsStream>),
        }

        let _r = async {
            let db_name = self.config.get_dbname().unwrap_or("");

            // This command cannot be executed while connected to the target
            // database. Thus, we make a new connection.
            let mut config = self.config.clone();
            config.dbname("");
            let (client, conn) = if let Some(tls_connector) =
                self.db.db_config.tls_connector().expect("Can't setup tls")
            {
                let (client, conn) = config.connect(tls_connector).await.unwrap();
                (client, ConnWrapper::Tls(conn))
            } else {
                let (client, conn) = config.connect(tokio_postgres::NoTls).await.unwrap();
                (client, ConnWrapper::NoTls(conn))
            };

            tokio::spawn(async move {
                match conn {
                    ConnWrapper::Tls(c) => {
                        if let Err(e) = c.await {
                            eprintln!("connection error: {}", e);
                        }
                    }
                    ConnWrapper::NoTls(c) => {
                        if let Err(e) = c.await {
                            eprintln!("connection error: {}", e);
                        }
                    }
                }
            });

            client
                .execute(format!("DROP DATABASE {} FORCE", db_name).as_str(), &[])
                .await
                .unwrap();
        };
    }
}

#[cfg(feature = "postgresql_embedded")]
async fn test_setup() -> DbHandle {
    let (conn, _temp_dir) = setup_pg().await;

    DbHandle {
        db: conn,
        _temp_dir,
    }
}

#[cfg(feature = "postgresql_embedded")]
pub(crate) async fn setup_pg() -> (StoragePostgres, tempfile::TempDir) {
    use crate::config::DatabaseConfig;
    use std::net::TcpListener;

    // Install the test database in a temporary directory and bound to a free port
    let mut attempt = 1;
    let (_temp_dir, pg) = loop {
        // Find a free port
        let port = {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("Failed to bind to port 0");
            listener
                .local_addr()
                .map(|l| l.port())
                .expect("Unable to retrieve local socket address")
        };

        // Wait a small amount of time for the port to be unbound again
        sleep(Duration::from_millis(10)).await;

        // Attempt Postgres installation on that port
        let _temp_dir = tempfile::tempdir().unwrap();
        let temp_path = _temp_dir.path();
        match crate::db::pg_setup::install(temp_path.into(), false, Some(port)).await {
            Ok(pg) => break (_temp_dir, pg),
            Err(e) => {
                info!("Unable to install test database on port {port} ({} attempts left) -- port might have become occupied in the meanwhile. Original error: {e}", 10 - attempt);
                sleep(Duration::from_millis(100)).await;
            }
        }
        if attempt >= 10 {
            panic!("Unable to install test database: gave up after several attempts");
        }
        attempt += 1;
    };
    let db_uri = pg.settings().url("postgres").clone();
    let db_config = DatabaseConfig::new(db_uri, None);
    let conn = StoragePostgres::initialize(&db_config, Some(pg))
        .await
        .unwrap();
    conn.run_migrations().await.unwrap();
    (conn, _temp_dir)
}

#[cfg(not(feature = "postgresql_embedded"))]
async fn test_setup() -> DbHandle {
    let (conn, config) = setup_pg().await;
    DbHandle { db: conn, config }
}

#[cfg(not(feature = "postgresql_embedded"))]
pub(crate) async fn setup_pg() -> (StoragePostgres, tokio_postgres::Config) {
    use crate::config::DatabaseConfig;
    let db_config = DatabaseConfig::new("postgres-pg-client-embed".to_string(), None);
    let mut config = db_config.tokio_postgres_config().expect("Can't get config");
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
    let conn = StoragePostgres::connect(&db_config).await.unwrap();
    conn.run_migrations().await.unwrap();

    (conn, config)
}

//////////////////////////////////////////////////////////////////////////////
/////                        DATA GENERATORS                             /////

/// Generates UUIDs but limits the randomness to the first bits.
///
/// This ensures that we have a good chance of generating a UUID that is already
/// in the database -- useful for testing error conditions.
pub(crate) fn limited_uuid() -> impl Strategy<Value = Uuid> {
    vec![any::<u8>()].prop_map(|mut bytes| {
        // prepend a bunch of zero bytes so the buffer is big enough for
        // building an uuid
        bytes.resize(16, 0);
        // restrict any::<u8> (0..255) to 1..4 this enforces more
        // interesting scenarios for testing (and we start at 1 because shaving
        bytes[0] &= 0b11;
        // an uuid of 0 is invalid and postgres will treat it as NULL
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
#[derive(Clone, Debug, PartialEq, PartialOrd)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
struct RuntimeConfigPropVal {
    invalid0: u8,
    val0: u16,
    val1: bool,
    val2: u64,
    val3: u64,
    val4: bool,
    val5: bool,
    val6: String,
    val7: Option<f64>,
    val8: Option<f64>,
    val9: Option<u64>,
    val10: Option<u64>,
    val11: Option<u64>,
    val12: Option<String>,
    val13: Option<u64>,
    val14: Option<u64>,
    val15: bool,
    val16: Option<u64>,
    val17: Option<u64>,
}
type ProgramConfigPropVal = (u8, bool, bool, bool);
type ProgramInfoPropVal = (u8, u8, u8);

/// Generates a limited pipeline name.
fn map_val_to_limited_pipeline_name(val: PipelineNamePropVal) -> String {
    let limited_val = val % 5;
    if limited_val == 0 {
        "".to_string() // An invalid pipeline name
    } else {
        format!("pipeline-{limited_val}")
    }
}

/// Generates a limited runtime configuration (1/8 is invalid).
fn map_val_to_limited_runtime_config(val: RuntimeConfigPropVal) -> serde_json::Value {
    if val.invalid0 % 8 == 0 {
        json!({ "workers": "abc" }) // An invalid runtime configuration
    } else {
        serde_json::to_value(RuntimeConfig {
            workers: val.val0,
            cpu_profiler: val.val1,
            min_batch_size_records: val.val2,
            max_buffering_delay_usecs: val.val3,
            storage: val.val4.then(Default::default),
            fault_tolerance: FtConfig::default(),
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
            clock_resolution_usecs: val.val13,
            pin_cpus: Vec::new(),
            provisioning_timeout_secs: val.val14,
            max_parallel_connector_init: None,
            init_containers: None,
            checkpoint_during_suspend: val.val15,
            http_workers: val.val16,
            io_workers: val.val17,
            dev_tweaks: BTreeMap::new(),
            logging: None,
        })
        .unwrap()
    }
}

/// Generates a limited program configuration (1/8 is invalid).
fn map_val_to_limited_program_config(val: ProgramConfigPropVal) -> serde_json::Value {
    if val.0 % 8 == 0 {
        json!({ "profile": 111 }) // An invalid program configuration
    } else {
        serde_json::to_value(ProgramConfig {
            profile: if val.1 {
                None
            } else if val.2 {
                Some(CompilationProfile::Unoptimized)
            } else {
                Some(CompilationProfile::Optimized)
            },
            cache: val.3,
            runtime_version: None,
        })
        .unwrap()
    }
}

/// Generates a limited program information (1/8 is invalid).
fn map_val_to_limited_program_info(val: ProgramInfoPropVal) -> serde_json::Value {
    if val.0 % 8 == 0 {
        json!({ "schema": 222 }) // An invalid program information
    } else {
        serde_json::to_value(ProgramInfo {
            schema: ProgramSchema {
                inputs: vec![],
                outputs: vec![],
            },
            main_rust: format!("main-rust-{}", val.1),
            udf_stubs: format!("udf-stubs-{}", val.2),
            input_connectors: BTreeMap::new(),
            output_connectors: BTreeMap::new(),
            dataflow: serde_json::Value::Null,
        })
        .unwrap()
    }
}

/// Generates pipeline name limited to only 5 variants.
/// This is to prevent that only the "pipeline not found" is encountered.
fn limited_pipeline_name() -> impl Strategy<Value = String> {
    any::<PipelineNamePropVal>().prop_map(map_val_to_limited_pipeline_name)
}

/// Generates platform version limited to only 3 possibilities (v0, v1, v2).
/// This is to prevent that platform versions being very often different,
/// which is not the usual case.
fn limited_platform_version() -> impl Strategy<Value = String> {
    any::<u8>().prop_map(|val| {
        let val1 = val % 10; // Map to 0, 1, ..., 9
        if val1 < 8 {
            // Approximately 80% of the time it is "v0"
            "v0".to_string()
        } else {
            // Then 10% for "v1" and 10% for "v2"
            format!("v{}", val1 - 7)
        }
    })
}

/// Generates program binary source checksum limited to 4 values
/// such that it is possible they sometimes collide.
fn limited_program_binary_source_checksum() -> impl Strategy<Value = String> {
    any::<u8>().prop_map(|val| {
        format!("source_checksum_{}", val % 4) // 0, 1, 2, 3
    })
}

/// Generates program binary integrity checksum limited to 4 values
/// such that it is possible they sometimes collide.
fn limited_program_binary_integrity_checksum() -> impl Strategy<Value = String> {
    any::<u8>().prop_map(|val| {
        format!("integrity_checksum_{}", val % 4) // 0, 1, 2, 3
    })
}

/// Generates different pipeline descriptors.
fn limited_pipeline_descr() -> impl Strategy<Value = PipelineDescr> {
    any::<(
        PipelineNamePropVal,
        String,
        RuntimeConfigPropVal,
        String,
        String,
        String,
        ProgramConfigPropVal,
    )>()
    .prop_map(|val| PipelineDescr {
        name: map_val_to_limited_pipeline_name(val.0),
        description: val.1,
        runtime_config: map_val_to_limited_runtime_config(val.2),
        program_code: val.3,
        udf_rust: val.4,
        udf_toml: val.5,
        program_config: map_val_to_limited_program_config(val.6),
    })
}

/// Generates different optional pipeline names.
fn limited_option_pipeline_name() -> impl Strategy<Value = Option<String>> {
    any::<Option<PipelineNamePropVal>>().prop_map(|val| val.map(map_val_to_limited_pipeline_name))
}

/// Generates different optional runtime configurations.
fn limited_option_runtime_config() -> impl Strategy<Value = Option<serde_json::Value>> {
    any::<Option<RuntimeConfigPropVal>>().prop_map(|val| val.map(map_val_to_limited_runtime_config))
}

/// Generates different optional program configurations.
fn limited_option_program_config() -> impl Strategy<Value = Option<serde_json::Value>> {
    any::<Option<ProgramConfigPropVal>>().prop_map(|val| val.map(map_val_to_limited_program_config))
}

/// Generates different SQL compilation information.
fn limited_sql_compilation_info() -> impl Strategy<Value = SqlCompilationInfo> {
    any::<u8>().prop_map(|v| SqlCompilationInfo {
        exit_code: (v % 4) as i32,
        messages: vec![],
    })
}

/// Generates different Rust compilation information.
fn limited_rust_compilation_info() -> impl Strategy<Value = RustCompilationInfo> {
    any::<(u8, u8, u8)>().prop_map(|v| RustCompilationInfo {
        exit_code: (v.0 % 4) as i32,
        stdout: format!("stdout-{}", v.1),
        stderr: format!("stderr-{}", v.1),
    })
}

/// Generates different program information.
fn limited_program_info() -> impl Strategy<Value = serde_json::Value> {
    any::<ProgramInfoPropVal>().prop_map(map_val_to_limited_program_info)
}

/// Generates different pipeline configurations.
fn limited_pipeline_config() -> impl Strategy<Value = serde_json::Value> {
    any::<(u8, PipelineId, RuntimeConfigPropVal, ProgramInfoPropVal)>().prop_map(|mut val| {
        if val.0 % 8 == 0 {
            json!({ "name": 222 }) // An invalid pipeline configuration
        } else {
            val.2.invalid0 = 1; // Prevent it from being invalid
            let runtime_config = map_val_to_limited_runtime_config(val.2);
            val.3 .0 = 1; // Prevent it from being invalid
            let program_info: ProgramInfo =
                serde_json::from_value(map_val_to_limited_program_info(val.3)).unwrap();
            serde_json::to_value(PipelineConfig {
                global: serde_json::from_value(runtime_config).unwrap(),
                name: Some(format!("pipeline-{}", val.1)),
                storage_config: None,
                secrets_dir: None,
                inputs: program_info.input_connectors,
                outputs: program_info.output_connectors,
            })
            .unwrap()
        }
    })
}

/// Generates different suspend information.
fn limited_optional_suspend_info() -> impl Strategy<Value = Option<serde_json::Value>> {
    any::<bool>().prop_map(|is_some| if is_some { Some(json!({})) } else { None })
}

/// Generates different error responses.
fn limited_optional_error_response() -> impl Strategy<Value = Option<ErrorResponse>> {
    any::<(bool, u8)>().prop_map(|(is_some, val)| {
        if is_some {
            Some(ErrorResponse {
                message: "This is an example error response".to_string(),
                error_code: Cow::from("SomeExampleError"),
                details: json!({
                    "extra-info": val
                }),
            })
        } else {
            None
        }
    })
}

/// Generates deployment location limited to only 5 variants.
fn limited_deployment_location() -> impl Strategy<Value = String> {
    any::<u16>().prop_map(|val| format!("example-deployment-location-{}", val % 5))
}

/// Generates deployment identifier.
fn limited_deployment_id() -> impl Strategy<Value = Uuid> {
    any::<u128>().prop_map(Uuid::from_u128)
}

/// Generates a random runtime status based on the value.
fn map_val_to_runtime_status(val: u64) -> RuntimeStatus {
    match val % 8 {
        0 => RuntimeStatus::Unavailable,
        1 => RuntimeStatus::Standby,
        2 => RuntimeStatus::Initializing,
        3 => RuntimeStatus::Bootstrapping,
        4 => RuntimeStatus::Replaying,
        5 => RuntimeStatus::Paused,
        6 => RuntimeStatus::Running,
        7 => RuntimeStatus::Suspended,
        _ => panic!("Should only go until 7"),
    }
}

/// Generates a random runtime desired status based on the value.
fn map_val_to_runtime_desired_status(val: u64) -> RuntimeDesiredStatus {
    match val % 5 {
        0 => RuntimeDesiredStatus::Unavailable,
        1 => RuntimeDesiredStatus::Standby,
        2 => RuntimeDesiredStatus::Paused,
        3 => RuntimeDesiredStatus::Running,
        4 => RuntimeDesiredStatus::Suspended,
        _ => panic!("Should only go until 4"),
    }
}

/// Generates runtime desired status spanning all of its variants.
fn arbitrary_runtime_desired_status() -> impl Strategy<Value = RuntimeDesiredStatus> {
    any::<u64>().prop_map(map_val_to_runtime_desired_status)
}

/// Generates extended runtime status spanning all of its variants.
fn limited_extended_runtime_status() -> impl Strategy<Value = ExtendedRuntimeStatus> {
    any::<(u64, u16, u64)>().prop_map(|(v1, v2, v3)| {
        let runtime_status = map_val_to_runtime_status(v1);
        let runtime_status_details = format!("runtime-status-details-{v2}");
        let runtime_desired_status = map_val_to_runtime_desired_status(v3);
        ExtendedRuntimeStatus {
            runtime_status,
            runtime_status_details,
            runtime_desired_status,
        }
    })
}

//////////////////////////////////////////////////////////////////////////////
/////                          MANUAL TESTS                              /////

/// Creation and retrieval of tenants.
#[tokio::test]
async fn tenant_creation() {
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
    let tenant_id_4 = handle
        .db
        .get_or_create_tenant_id(Uuid::now_v7(), "z".to_string(), "y".to_string())
        .await
        .unwrap();
    let tenant_id_5 = handle
        .db
        .get_or_create_tenant_id(Uuid::now_v7(), "x".to_string(), "z".to_string())
        .await
        .unwrap();
    assert_eq!(tenant_id_1, tenant_id_2);
    assert_eq!(tenant_id_2, tenant_id_3);
    assert_ne!(tenant_id_3, tenant_id_4);
    assert_ne!(tenant_id_4, tenant_id_5);
    assert_ne!(tenant_id_3, tenant_id_5);
}

/// Creation, deletion and validation of API keys.
#[tokio::test]
async fn api_key_store_and_validation() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    // Attempt several key generations and validations
    for i in 1..10 {
        // Create API key
        let api_key = generate_api_key();
        let api_key_name = &format!("foo-{}", i);
        handle
            .db
            .store_api_key_hash(
                tenant_id,
                Uuid::now_v7(),
                api_key_name,
                &api_key,
                vec![ApiPermission::Read, ApiPermission::Write],
            )
            .await
            .unwrap();
        let scopes = handle.db.validate_api_key(&api_key).await.unwrap();
        assert_eq!(tenant_id, scopes.0);
        assert_eq!(&ApiPermission::Read, scopes.1.first().unwrap());
        assert_eq!(&ApiPermission::Write, scopes.1.get(1).unwrap());

        // Delete API key
        handle
            .db
            .delete_api_key(tenant_id, api_key_name)
            .await
            .unwrap();

        // Deleted API key is no longer valid
        assert!(matches!(
            handle.db.validate_api_key(&api_key).await.unwrap_err(),
            DBError::InvalidApiKey
        ));

        // Deleting again results in an error
        assert!(
            matches!(handle.db.delete_api_key(tenant_id, api_key_name).await.unwrap_err(), DBError::UnknownApiKey { name } if &name == api_key_name)
        );

        // Non-existing API key
        let api_key_2 = generate_api_key();
        let err = handle.db.validate_api_key(&api_key_2).await.unwrap_err();
        assert!(matches!(err, DBError::InvalidApiKey));
    }
}

/// Creation of pipelines.
#[tokio::test]
async fn pipeline_creation() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let new_descriptor = PipelineDescr {
        name: "test1".to_string(),
        description: "Test description".to_string(),
        runtime_config: json!({
            "workers": 123
        }),
        program_code: "".to_string(),
        udf_rust: "".to_string(),
        udf_toml: "".to_string(),
        program_config: json!({
            "profile": "unoptimized"
        }),
    };
    let new_result = handle
        .db
        .new_pipeline(tenant_id, Uuid::now_v7(), "v0", new_descriptor.clone())
        .await
        .unwrap();
    let rows = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(rows.len(), 1);
    let actual = rows.first().unwrap();
    assert_eq!(new_result, actual.clone());

    // Core fields
    assert_eq!(actual.name, new_descriptor.name);
    assert_eq!(actual.description, new_descriptor.description);
    assert_eq!(
        actual.runtime_config,
        serde_json::to_value(
            serde_json::from_value::<RuntimeConfig>(new_descriptor.runtime_config).unwrap()
        )
        .unwrap()
    );
    assert_eq!(actual.program_code, new_descriptor.program_code);
    assert_eq!(
        actual.program_config,
        serde_json::to_value(
            serde_json::from_value::<ProgramConfig>(new_descriptor.program_config).unwrap()
        )
        .unwrap()
    );

    // Core metadata fields
    // actual.id
    // actual.created_at
    assert_eq!(actual.version, Version(1));

    // System-decided program fields
    assert_eq!(actual.program_version, Version(1));
    assert_eq!(actual.program_status, ProgramStatus::Pending);
    // actual.program_status_since
    assert_eq!(actual.program_info, None);

    // System-decided deployment fields
    assert_eq!(actual.deployment_resources_status, ResourcesStatus::Stopped);
    // actual.deployment_status_since
    assert_eq!(
        actual.deployment_resources_desired_status,
        ResourcesDesiredStatus::Stopped
    );
    assert_eq!(actual.deployment_error, None);
    assert_eq!(actual.deployment_config, None);
    assert_eq!(actual.deployment_location, None);
}

/// Retrieval of pipelines.
#[tokio::test]
async fn pipeline_retrieval() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;

    // Initially empty list
    let rows = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(rows.len(), 0);
    assert!(matches!(
        handle
            .db
            .get_pipeline(tenant_id, "test1")
            .await
            .unwrap_err(),
        DBError::UnknownPipelineName { pipeline_name } if pipeline_name == "test1"
    ));
    let non_existing_pipeline_id = PipelineId(Uuid::now_v7());
    assert!(matches!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, non_existing_pipeline_id)
            .await
            .unwrap_err(),
        DBError::UnknownPipeline { pipeline_id } if pipeline_id == non_existing_pipeline_id
    ));

    // Create one pipeline
    let pipeline1 = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "test1".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t1".to_string(),
                program_config: serde_json::to_value(ProgramConfig {
                    profile: Some(CompilationProfile::Unoptimized),
                    cache: true,
                    runtime_version: None,
                })
                .unwrap(),
            },
        )
        .await
        .unwrap();
    assert_eq!("test1", pipeline1.name);

    // Check pipeline1
    let rows = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows.first().unwrap().name, "test1");
    assert_eq!(
        pipeline1,
        handle.db.get_pipeline(tenant_id, "test1").await.unwrap()
    );
    assert_eq!(
        pipeline1,
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
    );

    // Create another pipeline
    let pipeline2 = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "test2".to_string(),
                description: "d2".to_string(),
                runtime_config: json!({}),
                program_code: "c2".to_string(),
                udf_rust: "r2".to_string(),
                udf_toml: "t2".to_string(),
                program_config: serde_json::to_value(ProgramConfig {
                    profile: Some(CompilationProfile::Unoptimized),
                    cache: false,
                    runtime_version: None,
                })
                .unwrap(),
            },
        )
        .await
        .unwrap();

    // Check list of two
    let rows = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.first().unwrap().name, "test1");
    assert_eq!(rows.get(1).unwrap().name, "test2");
    assert_eq!(rows, vec![pipeline1.clone(), pipeline2.clone()]);
    assert_eq!(
        pipeline2.clone(),
        handle.db.get_pipeline(tenant_id, "test2").await.unwrap()
    );
    assert_eq!(
        pipeline2.clone(),
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline2.id)
            .await
            .unwrap()
    );

    // Delete pipeline1
    handle.db.delete_pipeline(tenant_id, "test1").await.unwrap();
    let rows = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert_eq!(rows, vec![pipeline2.clone()]);
    assert!(matches!(
        handle
            .db
            .get_pipeline(tenant_id, "test1")
            .await
            .unwrap_err(),
        DBError::UnknownPipelineName { pipeline_name } if pipeline_name == "test1"
    ));
    assert!(matches!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap_err(),
        DBError::UnknownPipeline { pipeline_id } if pipeline_id == pipeline1.id
    ));

    // Delete pipeline2
    handle.db.delete_pipeline(tenant_id, "test2").await.unwrap();
    let rows = handle.db.list_pipelines(tenant_id).await.unwrap();
    assert!(matches!(
        handle
            .db
            .get_pipeline(tenant_id, "test2")
            .await
            .unwrap_err(),
        DBError::UnknownPipelineName { pipeline_name } if pipeline_name == "test2"
    ));
    assert!(matches!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline2.id)
            .await
            .unwrap_err(),
        DBError::UnknownPipeline { pipeline_id } if pipeline_id == pipeline2.id
    ));
    assert_eq!(rows, vec![]);
}

/// Progression of versions across edits.
#[tokio::test]
async fn pipeline_versioning() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t1".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();

    // Initially, versions are 1
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.version, Version(1));
    assert_eq!(current.program_version, Version(1));
    assert_eq!(current.refresh_version, Version(1));

    // Edit without changes should not affect versions
    handle
        .db
        .update_pipeline(
            tenant_id, "example", &None, &None, "v0", &None, &None, &None, &None, &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.version, Version(1));
    assert_eq!(current.program_version, Version(1));
    assert_eq!(current.refresh_version, Version(1));

    // Edit program with the same content should have no effect
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &None,
            "v0",
            &None,
            &Some("c1".to_string()),
            &Some("r1".to_string()),
            &Some("t1".to_string()),
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.version, Version(1));
    assert_eq!(current.program_version, Version(1));
    assert_eq!(current.refresh_version, Version(1));

    // Edit description with the same content should have no effect
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &Some("d1".to_string()),
            "v0",
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.version, Version(1));
    assert_eq!(current.program_version, Version(1));
    assert_eq!(current.refresh_version, Version(1));

    // Edit program -> increment version, program version and refresh_version
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &None,
            "v0",
            &None,
            &Some("c2".to_string()),
            &None,
            &None,
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.program_code, "c2".to_string());
    assert_eq!(current.version, Version(2));
    assert_eq!(current.program_version, Version(2));
    assert_eq!(current.refresh_version, Version(2));

    // Edit UDF -> increment version, program version and refresh_version
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &None,
            "v0",
            &None,
            &None,
            &Some("r2".to_string()),
            &None,
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.udf_rust, "r2".to_string());
    assert_eq!(current.version, Version(3));
    assert_eq!(current.program_version, Version(3));
    assert_eq!(current.refresh_version, Version(3));

    // Edit TOML -> increment version, program version and refresh_version
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &None,
            "v0",
            &None,
            &None,
            &None,
            &Some("t2".to_string()),
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.udf_toml, "t2".to_string());
    assert_eq!(current.version, Version(4));
    assert_eq!(current.program_version, Version(4));
    assert_eq!(current.refresh_version, Version(4));

    // Edit description -> increment version and refresh_version
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &Some("d2".to_string()),
            "v0",
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.description, "d2".to_string());
    assert_eq!(current.version, Version(5));
    assert_eq!(current.program_version, Version(4));
    assert_eq!(current.refresh_version, Version(5));

    // Edit program configuration -> increment version, program version and refresh_version
    let new_program_config = serde_json::to_value(ProgramConfig {
        profile: Some(CompilationProfile::Dev),
        cache: false,
        runtime_version: None,
    })
    .unwrap();
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &None,
            &None,
            "v0",
            &None,
            &None,
            &None,
            &None,
            &Some(new_program_config.clone()),
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example").await.unwrap();
    assert_eq!(current.program_config, new_program_config);
    assert_eq!(current.version, Version(6));
    assert_eq!(current.program_version, Version(5));
    assert_eq!(current.refresh_version, Version(6));

    // Edit name -> increment version and refresh_version
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example",
            &Some("example2".to_string()),
            &None,
            "v0",
            &None,
            &None,
            &None,
            &None,
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example2").await.unwrap();
    assert_eq!(current.name, "example2".to_string());
    assert_eq!(current.version, Version(7));
    assert_eq!(current.program_version, Version(5));
    assert_eq!(current.refresh_version, Version(7));

    // Edit runtime configuration -> increment version and refresh_version
    let new_runtime_config = serde_json::to_value(RuntimeConfig {
        workers: 100,
        storage: None,
        fault_tolerance: FtConfig::default(),
        cpu_profiler: false,
        tracing: false,
        tracing_endpoint_jaeger: "".to_string(),
        min_batch_size_records: 0,
        max_buffering_delay_usecs: 0,
        resources: ResourceConfig {
            cpu_cores_min: None,
            cpu_cores_max: None,
            memory_mb_min: None,
            memory_mb_max: None,
            storage_mb_max: None,
            storage_class: None,
        },
        clock_resolution_usecs: None,
        pin_cpus: Vec::new(),
        provisioning_timeout_secs: None,
        max_parallel_connector_init: None,
        init_containers: None,
        checkpoint_during_suspend: true,
        http_workers: None,
        io_workers: None,
        dev_tweaks: BTreeMap::new(),
        logging: None,
    })
    .unwrap();
    handle
        .db
        .update_pipeline(
            tenant_id,
            "example2",
            &None,
            &None,
            "v0",
            &Some(new_runtime_config.clone()),
            &None,
            &None,
            &None,
            &None,
        )
        .await
        .unwrap();
    let current = handle.db.get_pipeline(tenant_id, "example2").await.unwrap();
    assert_eq!(current.runtime_config, new_runtime_config);
    assert_eq!(current.version, Version(8));
    assert_eq!(current.program_version, Version(5));
    assert_eq!(current.refresh_version, Version(8));
}

/// If the name of a pipeline already exists, it should return an error.
#[tokio::test]
async fn pipeline_duplicate() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t1".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();

    let error = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example".to_string(),
                description: "d2".to_string(),
                runtime_config: json!({}),
                program_code: "c2".to_string(),
                udf_rust: "r2".to_string(),
                udf_toml: "t2".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .expect_err("Expecting unique violation");
    let expected = DBError::DuplicateName;
    assert_eq!(format!("{}", error), format!("{}", expected));
}

/// Program compilation by picking up the next program and transitioning the program status.
#[tokio::test]
async fn pipeline_program_compilation() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;

    // There is no program to compile
    assert_eq!(
        handle.db.get_next_sql_compilation("v0").await.unwrap(),
        None
    );
    assert_eq!(
        handle.db.get_next_rust_compilation("v0").await.unwrap(),
        None
    );

    // Create two pipelines
    let pipeline1 = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example1".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t1".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();
    let pipeline2 = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example2".to_string(),
                description: "d2".to_string(),
                runtime_config: json!({}),
                program_code: "c2".to_string(),
                udf_rust: "r2".to_string(),
                udf_toml: "t2".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();

    // Initially, the next program to compile is the one with program status being pending the longest
    assert_eq!(
        handle.db.get_next_sql_compilation("v0").await.unwrap(),
        Some((tenant_id, pipeline1.clone()))
    );

    // "Compile" the program of pipeline1
    handle
        .db
        .transit_program_status_to_compiling_sql(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_sql_compiled(
            tenant_id,
            pipeline1.id,
            Version(1),
            &SqlCompilationInfo {
                exit_code: 0,
                messages: vec![],
            },
            &serde_json::to_value(ProgramInfo {
                schema: ProgramSchema {
                    inputs: vec![],
                    outputs: vec![],
                },
                main_rust: "".to_string(),
                udf_stubs: "".to_string(),
                input_connectors: BTreeMap::new(),
                output_connectors: BTreeMap::new(),
                dataflow: serde_json::Value::Null,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        handle
            .db
            .get_next_rust_compilation("v0")
            .await
            .unwrap()
            .unwrap()
            .1
            .id,
        pipeline1.id
    );
    handle
        .db
        .transit_program_status_to_compiling_rust(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_success(
            tenant_id,
            pipeline1.id,
            Version(1),
            &RustCompilationInfo {
                exit_code: 0,
                stdout: "".to_string(),
                stderr: "".to_string(),
            },
            "abc",
            "123",
        )
        .await
        .unwrap();

    // Next up, it should be pipeline2
    assert_eq!(
        handle.db.get_next_sql_compilation("v0").await.unwrap(),
        Some((tenant_id, pipeline2.clone()))
    );

    // "Compile" the program of pipeline2, but end in error
    handle
        .db
        .transit_program_status_to_system_error(
            tenant_id,
            pipeline2.id,
            Version(1),
            "Some system error",
        )
        .await
        .unwrap();
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline2.id)
            .await
            .unwrap()
            .refresh_version,
        Version(2)
    );

    // There should be nothing left to compile
    assert_eq!(
        handle.db.get_next_sql_compilation("v0").await.unwrap(),
        None
    );
    assert_eq!(
        handle.db.get_next_rust_compilation("v0").await.unwrap(),
        None
    );
}

/// Deployment of a pipeline by starting it and progressing through various deployment statuses.
#[tokio::test]
async fn pipeline_deployment() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let pipeline1 = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example1".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t2".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();

    // "Compile" the program of pipeline1
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
            .refresh_version,
        Version(1)
    );
    handle
        .db
        .transit_program_status_to_compiling_sql(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_sql_compiled(
            tenant_id,
            pipeline1.id,
            Version(1),
            &SqlCompilationInfo {
                exit_code: 0,
                messages: vec![],
            },
            &serde_json::to_value(ProgramInfo {
                schema: ProgramSchema {
                    inputs: vec![],
                    outputs: vec![],
                },
                main_rust: "".to_string(),
                udf_stubs: "".to_string(),
                input_connectors: BTreeMap::new(),
                output_connectors: BTreeMap::new(),
                dataflow: serde_json::Value::Null,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
            .refresh_version,
        Version(2)
    );
    handle
        .db
        .transit_program_status_to_compiling_rust(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_success(
            tenant_id,
            pipeline1.id,
            Version(1),
            &RustCompilationInfo {
                exit_code: 0,
                stdout: "".to_string(),
                stderr: "".to_string(),
            },
            "def",
            "123",
        )
        .await
        .unwrap();
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
            .refresh_version,
        Version(3)
    );

    // "Deploy" pipeline1
    handle
        .db
        .set_deployment_resources_desired_status_provisioned(
            tenant_id,
            "example1",
            RuntimeDesiredStatus::Paused,
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioning(
            tenant_id,
            pipeline1.id,
            Version(1),
            Uuid::nil(),
            serde_json::to_value(generate_pipeline_config(
                pipeline1.id,
                &serde_json::from_value(pipeline1.runtime_config.clone()).unwrap(),
                &BTreeMap::default(),
                &BTreeMap::default(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Running,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Running,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Running,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Running,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .set_deployment_resources_desired_status_stopped(tenant_id, "example1")
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopping(
            tenant_id,
            pipeline1.id,
            Version(1),
            None,
            Some(json!({})),
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopped(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .set_deployment_resources_desired_status_provisioned(
            tenant_id,
            "example1",
            RuntimeDesiredStatus::Paused,
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioning(
            tenant_id,
            pipeline1.id,
            Version(1),
            Uuid::nil(),
            serde_json::to_value(generate_pipeline_config(
                pipeline1.id,
                &serde_json::from_value(pipeline1.runtime_config).unwrap(),
                &BTreeMap::default(),
                &BTreeMap::default(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .set_deployment_resources_desired_status_stopped(tenant_id, "example1")
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopping(
            tenant_id,
            pipeline1.id,
            Version(1),
            None,
            None,
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopped(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
}

/// Pipeline can only transition from `Stopped` to `Provisioning` if the version guard
/// presented matches.
#[tokio::test]
async fn pipeline_provision_version_guard() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;

    // Create pipeline (v1)
    let pipeline = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example1".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t2".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();

    // Transition the program_status of the pipeline to become Success
    handle
        .db
        .transit_program_status_to_compiling_sql(tenant_id, pipeline.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_sql_compiled(
            tenant_id,
            pipeline.id,
            Version(1),
            &SqlCompilationInfo {
                exit_code: 0,
                messages: vec![],
            },
            &serde_json::to_value(ProgramInfo {
                schema: ProgramSchema {
                    inputs: vec![],
                    outputs: vec![],
                },
                main_rust: "".to_string(),
                udf_stubs: "".to_string(),
                input_connectors: BTreeMap::new(),
                output_connectors: BTreeMap::new(),
                dataflow: serde_json::Value::Null,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_compiling_rust(tenant_id, pipeline.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_success(
            tenant_id,
            pipeline.id,
            Version(1),
            &RustCompilationInfo {
                exit_code: 0,
                stdout: "".to_string(),
                stderr: "".to_string(),
            },
            "def",
            "123",
        )
        .await
        .unwrap();

    // Update pipeline to v2
    handle
        .db
        .update_pipeline(
            tenant_id,
            &pipeline.name,
            &None,
            &None,
            "v0",
            &Some(
                serde_json::to_value(RuntimeConfig {
                    workers: 10,
                    storage: None,
                    fault_tolerance: FtConfig::default(),
                    cpu_profiler: false,
                    tracing: false,
                    tracing_endpoint_jaeger: "".to_string(),
                    min_batch_size_records: 0,
                    max_buffering_delay_usecs: 0,
                    resources: Default::default(),
                    clock_resolution_usecs: None,
                    pin_cpus: Vec::new(),
                    provisioning_timeout_secs: None,
                    max_parallel_connector_init: None,
                    init_containers: None,
                    checkpoint_during_suspend: false,
                    dev_tweaks: BTreeMap::new(),
                    http_workers: None,
                    io_workers: None,
                    logging: None,
                })
                .unwrap(),
            ),
            &None,
            &None,
            &None,
            &None,
        )
        .await
        .unwrap();

    // Any deployment status transition happening on the incorrect version should fail
    assert!(matches!(
        handle.db
              .transit_deployment_resources_status_to_provisioning(
                  tenant_id,
                  pipeline.id,
                  Version(1),
                  Uuid::nil(),
                  serde_json::to_value(generate_pipeline_config(
                      pipeline.id,
                      &serde_json::from_value(pipeline.runtime_config.clone()).unwrap(),
                      &BTreeMap::default(),
                      &BTreeMap::default(),
                  )).unwrap(),
              )
              .await.unwrap_err(),
        DBError::OutdatedPipelineVersion { outdated_version, latest_version } if outdated_version == Version(1) && latest_version == Version(2)));
    assert!(matches!(
        handle.db
              .transit_deployment_resources_status_to_provisioned(tenant_id, pipeline.id, Version(1), "location1", ExtendedRuntimeStatus {
            runtime_status: RuntimeStatus::Initializing,
            runtime_status_details: "".to_string(),
            runtime_desired_status: RuntimeDesiredStatus::Paused,
        })
              .await.unwrap_err(),
        DBError::OutdatedPipelineVersion { outdated_version, latest_version } if outdated_version == Version(1) && latest_version == Version(2)
    ));
    assert!(matches!(
        handle.db
              .transit_deployment_resources_status_to_provisioned(tenant_id, pipeline.id, Version(1), "location1", ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
        })
              .await.unwrap_err(),
        DBError::OutdatedPipelineVersion { outdated_version, latest_version } if outdated_version == Version(1) && latest_version == Version(2)
    ));
    assert!(matches!(
        handle.db
              .transit_deployment_resources_status_to_provisioned(tenant_id, pipeline.id, Version(1), "location1", ExtendedRuntimeStatus {
            runtime_status: RuntimeStatus::Paused,
            runtime_status_details: "".to_string(),
            runtime_desired_status: RuntimeDesiredStatus::Paused,
        })
              .await.unwrap_err(),
        DBError::OutdatedPipelineVersion { outdated_version, latest_version } if outdated_version == Version(1) && latest_version == Version(2)
    ));
    assert!(matches!(
        handle.db
              .transit_deployment_resources_status_to_stopping(tenant_id, pipeline.id, Version(1), None, None)
              .await.unwrap_err(),
        DBError::OutdatedPipelineVersion { outdated_version, latest_version } if outdated_version == Version(1) && latest_version == Version(2)
    ));
    assert!(matches!(
        handle.db
              .transit_deployment_resources_status_to_stopped(tenant_id, pipeline.id, Version(1))
              .await.unwrap_err(),
        DBError::OutdatedPipelineVersion { outdated_version, latest_version } if outdated_version == Version(1) && latest_version == Version(2)
    ));

    // This is the correct version and should work
    handle
        .db
        .transit_deployment_resources_status_to_provisioning(
            tenant_id,
            pipeline.id,
            Version(2),
            Uuid::now_v7(),
            serde_json::to_value(generate_pipeline_config(
                pipeline.id,
                &serde_json::from_value(pipeline.runtime_config.clone()).unwrap(),
                &BTreeMap::default(),
                &BTreeMap::default(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
}

/// Tracking pipeline lifecycle events through various deployment statuses.
#[tokio::test]
async fn pipeline_lifecycle_events() {
    let handle = test_setup().await;
    let tenant_id = TenantRecord::default().id;
    let pipeline1 = handle
        .db
        .new_pipeline(
            tenant_id,
            Uuid::now_v7(),
            "v0",
            PipelineDescr {
                name: "example1".to_string(),
                description: "d1".to_string(),
                runtime_config: json!({}),
                program_code: "c1".to_string(),
                udf_rust: "r1".to_string(),
                udf_toml: "t2".to_string(),
                program_config: json!({}),
            },
        )
        .await
        .unwrap();

    assert!(handle.db.cleanup_pipeline_lifecycle_events(0).await.is_ok());

    // verify there are no events as we just cleaned up
    assert!(handle
        .db
        .get_pipeline_lifecycle_events(tenant_id, &pipeline1.name, 10)
        .await
        .is_ok_and(|e| e.is_empty()));

    // "Compile" the program of pipeline1
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
            .refresh_version,
        Version(1)
    );
    handle
        .db
        .transit_program_status_to_compiling_sql(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_sql_compiled(
            tenant_id,
            pipeline1.id,
            Version(1),
            &SqlCompilationInfo {
                exit_code: 0,
                messages: vec![],
            },
            &serde_json::to_value(ProgramInfo {
                schema: ProgramSchema {
                    inputs: vec![],
                    outputs: vec![],
                },
                main_rust: "".to_string(),
                udf_stubs: "".to_string(),
                input_connectors: BTreeMap::new(),
                output_connectors: BTreeMap::new(),
                dataflow: serde_json::Value::Null,
            })
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
            .refresh_version,
        Version(2)
    );
    handle
        .db
        .transit_program_status_to_compiling_rust(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .transit_program_status_to_success(
            tenant_id,
            pipeline1.id,
            Version(1),
            &RustCompilationInfo {
                exit_code: 0,
                stdout: "".to_string(),
                stderr: "".to_string(),
            },
            "def",
            "123",
        )
        .await
        .unwrap();
    assert_eq!(
        handle
            .db
            .get_pipeline_by_id(tenant_id, pipeline1.id)
            .await
            .unwrap()
            .refresh_version,
        Version(3)
    );

    // "Deploy" pipeline1
    handle
        .db
        .set_deployment_resources_desired_status_provisioned(
            tenant_id,
            "example1",
            RuntimeDesiredStatus::Paused,
        )
        .await
        .unwrap();

    handle
        .db
        .transit_deployment_resources_status_to_provisioning(
            tenant_id,
            pipeline1.id,
            Version(1),
            Uuid::nil(),
            serde_json::to_value(generate_pipeline_config(
                pipeline1.id,
                &serde_json::from_value(pipeline1.runtime_config.clone()).unwrap(),
                &BTreeMap::default(),
                &BTreeMap::default(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Running,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Running,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Running,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .set_deployment_resources_desired_status_stopped(tenant_id, "example1")
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopping(
            tenant_id,
            pipeline1.id,
            Version(1),
            Some(ErrorResponse {
                message: "never gonna give you up".into(),
                error_code: "epic failure".into(),
                details: json!({}),
            }),
            None,
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopped(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();
    handle
        .db
        .set_deployment_resources_desired_status_provisioned(
            tenant_id,
            "example1",
            RuntimeDesiredStatus::Paused,
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioning(
            tenant_id,
            pipeline1.id,
            Version(1),
            Uuid::nil(),
            serde_json::to_value(generate_pipeline_config(
                pipeline1.id,
                &serde_json::from_value(pipeline1.runtime_config).unwrap(),
                &BTreeMap::default(),
                &BTreeMap::default(),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Initializing,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_provisioned(
            tenant_id,
            pipeline1.id,
            Version(1),
            "location1",
            ExtendedRuntimeStatus {
                runtime_status: RuntimeStatus::Paused,
                runtime_status_details: "".to_string(),
                runtime_desired_status: RuntimeDesiredStatus::Paused,
            },
        )
        .await
        .unwrap();
    handle
        .db
        .set_deployment_resources_desired_status_stopped(tenant_id, "example1")
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopping(
            tenant_id,
            pipeline1.id,
            Version(1),
            None,
            None,
        )
        .await
        .unwrap();
    handle
        .db
        .transit_deployment_resources_status_to_stopped(tenant_id, pipeline1.id, Version(1))
        .await
        .unwrap();

    let mut events = handle
        .db
        .get_pipeline_lifecycle_events(tenant_id, &pipeline1.name, 20)
        .await
        .unwrap()
        .into_iter();

    // First deployment cycle
    assert_eq!(
        Some("provisioning".to_string()),
        events.next().map(|s| s.deployment_resources_status)
    );
    assert_eq!(
        Some((
            "provisioned".to_string(),
            Some("initializing".to_string()),
            Some("paused".to_string())
        )),
        events.next().map(|s| (
            s.deployment_resources_status,
            s.deployment_runtime_status,
            s.deployment_runtime_desired_status
        ))
    );
    assert_eq!(
        Some((
            "provisioned".to_string(),
            Some("paused".to_string()),
            Some("paused".to_string())
        )),
        events.next().map(|s| (
            s.deployment_resources_status,
            s.deployment_runtime_status,
            s.deployment_runtime_desired_status
        ))
    );
    assert_eq!(
        Some((
            "provisioned".to_string(),
            Some("paused".to_string()),
            Some("running".to_string())
        )),
        events.next().map(|s| (
            s.deployment_resources_status,
            s.deployment_runtime_status,
            s.deployment_runtime_desired_status
        ))
    );
    assert_eq!(
        Some((
            "provisioned".to_string(),
            Some("running".to_string()),
            Some("running".to_string())
        )),
        events.next().map(|s| (
            s.deployment_resources_status,
            s.deployment_runtime_status,
            s.deployment_runtime_desired_status
        ))
    );
    assert_eq!(
        Some((
            "stopping".to_string(),
            Some(
                r#"{"message":"never gonna give you up","error_code":"epic failure","details":{}}"#
                    .to_string()
            )
        )),
        events
            .next()
            .map(|s| (s.deployment_resources_status, s.info))
    );
    assert_eq!(
        Some((
            "stopped".to_string(),
            Some(
                r#"{"message":"never gonna give you up","error_code":"epic failure","details":{}}"#
                    .to_string()
            )
        )),
        events
            .next()
            .map(|s| (s.deployment_resources_status, s.info))
    );

    // Second deployment cycle
    assert_eq!(
        Some("provisioning".to_string()),
        events.next().map(|s| s.deployment_resources_status)
    );
    assert_eq!(
        Some((
            "provisioned".to_string(),
            Some("initializing".to_string()),
            Some("paused".to_string())
        )),
        events.next().map(|s| (
            s.deployment_resources_status,
            s.deployment_runtime_status,
            s.deployment_runtime_desired_status
        ))
    );
    assert_eq!(
        Some((
            "provisioned".to_string(),
            Some("paused".to_string()),
            Some("paused".to_string())
        )),
        events.next().map(|s| (
            s.deployment_resources_status,
            s.deployment_runtime_status,
            s.deployment_runtime_desired_status
        ))
    );
    assert_eq!(
        Some("stopping".to_string()),
        events.next().map(|s| s.deployment_resources_status)
    );
    assert_eq!(
        Some(("stopped".to_string(), None)),
        events
            .next()
            .map(|s| (s.deployment_resources_status, s.info))
    );

    assert!(handle.db.cleanup_pipeline_lifecycle_events(0).await.is_ok());

    // verify there are no events as we just cleaned up
    assert!(handle
        .db
        .get_pipeline_lifecycle_events(tenant_id, &pipeline1.name, 10)
        .await
        .is_ok_and(|e| e.is_empty()));
}
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
    ListPipelinesForMonitoring(TenantId),
    GetPipeline(TenantId, String),
    GetPipelineForMonitoring(TenantId, String),
    GetPipelineById(TenantId, PipelineId),
    GetPipelineByIdForMonitoring(TenantId, PipelineId),
    GetPipelineByIdForRunner(
        TenantId,
        PipelineId,
        #[proptest(strategy = "limited_platform_version()")] String,
        bool,
    ),
    NewPipeline(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        #[proptest(strategy = "limited_platform_version()")] String,
        #[proptest(strategy = "limited_pipeline_descr()")] PipelineDescr,
    ),
    NewOrUpdatePipeline(
        TenantId,
        #[proptest(strategy = "limited_uuid()")] Uuid,
        #[proptest(strategy = "limited_pipeline_name()")] String,
        #[proptest(strategy = "limited_platform_version()")] String,
        #[proptest(strategy = "limited_pipeline_descr()")] PipelineDescr,
    ),
    UpdatePipeline(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
        #[proptest(strategy = "limited_option_pipeline_name()")] Option<String>,
        Option<String>,
        #[proptest(strategy = "limited_platform_version()")] String,
        #[proptest(strategy = "limited_option_runtime_config()")] Option<serde_json::Value>,
        Option<String>,
        Option<String>,
        Option<String>,
        #[proptest(strategy = "limited_option_program_config()")] Option<serde_json::Value>,
    ),
    DeletePipeline(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    TransitProgramStatusToPending(TenantId, PipelineId, Version),
    TransitProgramStatusToCompilingSql(TenantId, PipelineId, Version),
    TransitProgramStatusToSqlCompiled(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_sql_compilation_info()")] SqlCompilationInfo,
        #[proptest(strategy = "limited_program_info()")] serde_json::Value,
    ),
    TransitProgramStatusToCompilingRust(TenantId, PipelineId, Version),
    TransitProgramStatusToSuccess(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_rust_compilation_info()")] RustCompilationInfo,
        #[proptest(strategy = "limited_program_binary_source_checksum()")] String,
        #[proptest(strategy = "limited_program_binary_integrity_checksum()")] String,
    ),
    TransitProgramStatusToSqlError(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_sql_compilation_info()")] SqlCompilationInfo,
    ),
    TransitProgramStatusToRustError(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_rust_compilation_info()")] RustCompilationInfo,
    ),
    TransitProgramStatusToSystemError(TenantId, PipelineId, Version, String),
    SetDeploymentResourcesDesiredStatusProvisioned(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
        #[proptest(strategy = "arbitrary_runtime_desired_status()")] RuntimeDesiredStatus,
    ),
    SetDeploymentResourcesDesiredStatusStoppedIfNotProvisioned(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    TransitDeploymentResourcesStatusToProvisioning(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_deployment_id()")] Uuid,
        #[proptest(strategy = "limited_pipeline_config()")] serde_json::Value,
    ),
    TransitDeploymentResourcesStatusToProvisioned(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_deployment_location()")] String,
        #[proptest(strategy = "limited_extended_runtime_status()")] ExtendedRuntimeStatus,
    ),
    TransitDeploymentResourcesStatusToStopping(
        TenantId,
        PipelineId,
        Version,
        #[proptest(strategy = "limited_optional_error_response()")] Option<ErrorResponse>,
        #[proptest(strategy = "limited_optional_suspend_info()")] Option<serde_json::Value>,
    ),
    TransitDeploymentResourcesStatusToStopped(TenantId, PipelineId, Version),
    TransitStorageStatusToClearing(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    TransitStorageStatusToCleared(TenantId, PipelineId),
    IncrementNotifyCounter(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
    ),
    ListPipelineIdsAcrossAllTenants,
    ListPipelinesAcrossAllTenantsForMonitoring,
    ClearOngoingSqlCompilation(#[proptest(strategy = "limited_platform_version()")] String),
    GetNextSqlCompilation(#[proptest(strategy = "limited_platform_version()")] String),
    ClearOngoingRustCompilation(#[proptest(strategy = "limited_platform_version()")] String),
    GetNextRustCompilation(#[proptest(strategy = "limited_platform_version()")] String),
    ListPipelineProgramsAcrossAllTenants,
    /// Get pipeline lifecycle events for a tenant and pipeline name.
    GetPipelineLifecycleEvents(
        TenantId,
        #[proptest(strategy = "limited_pipeline_name()")] String,
        u32,
    ),
    /// Cleanup pipeline lifecycle events older than the specified age in days
    CleanupPipelineLifecycleEvents(u16),
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
    pipeline.deployment_resources_status_since = timestamp;
    pipeline.deployment_resources_desired_status_since = timestamp;
    pipeline.deployment_runtime_status_since =
        pipeline.deployment_runtime_status_since.map(|_| timestamp);
    pipeline.deployment_runtime_desired_status_since = pipeline
        .deployment_runtime_desired_status_since
        .map(|_| timestamp);
    pipeline
}

/// Convert pipeline monitoring descriptor for test comparison.
/// See for explanation: [`convert_pipeline_with_constant_timestamps`].
fn convert_pipeline_for_monitoring_with_constant_timestamps(
    mut pipeline: ExtendedPipelineDescrMonitoring,
) -> ExtendedPipelineDescrMonitoring {
    let timestamp = Utc.timestamp_nanos(0);
    pipeline.created_at = timestamp;
    pipeline.program_status_since = timestamp;
    pipeline.deployment_resources_status_since = timestamp;
    pipeline.deployment_resources_desired_status_since = timestamp;
    pipeline.deployment_runtime_status_since =
        pipeline.deployment_runtime_status_since.map(|_| timestamp);
    pipeline.deployment_runtime_desired_status_since = pipeline
        .deployment_runtime_desired_status_since
        .map(|_| timestamp);
    pipeline
}

/// Convert pipeline runner descriptor for test comparison.
/// See for explanation: [`convert_pipeline_with_constant_timestamps`].
fn convert_pipeline_for_runner_with_constant_timestamps(
    pipeline: ExtendedPipelineDescrRunner,
) -> ExtendedPipelineDescrRunner {
    match pipeline {
        ExtendedPipelineDescrRunner::Monitoring(pipeline) => {
            ExtendedPipelineDescrRunner::Monitoring(
                convert_pipeline_for_monitoring_with_constant_timestamps(pipeline),
            )
        }
        ExtendedPipelineDescrRunner::Complete(pipeline) => ExtendedPipelineDescrRunner::Complete(
            convert_pipeline_with_constant_timestamps(pipeline),
        ),
    }
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
            "mismatch detected with model (left) and impl (right)"
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

/// Compares model response to that of the database implementation
/// when the type is `ExtendedPipelineDescr`.
fn check_response_pipeline(
    step: usize,
    mut result_model: DBResult<ExtendedPipelineDescr>,
    mut result_impl: DBResult<ExtendedPipelineDescr>,
) {
    result_model = result_model.map(convert_pipeline_with_constant_timestamps);
    result_impl = result_impl.map(convert_pipeline_with_constant_timestamps);
    check_responses(step, result_model, result_impl);
}

/// Compares model response to that of the database implementation
/// when the type is `ExtendedPipelineDescrMonitoring`.
fn check_response_pipeline_for_monitoring(
    step: usize,
    mut result_model: DBResult<ExtendedPipelineDescrMonitoring>,
    mut result_impl: DBResult<ExtendedPipelineDescrMonitoring>,
) {
    result_model = result_model.map(convert_pipeline_for_monitoring_with_constant_timestamps);
    result_impl = result_impl.map(convert_pipeline_for_monitoring_with_constant_timestamps);
    check_responses(step, result_model, result_impl);
}

/// Compares model response to that of the database implementation
/// when the type is `ExtendedPipelineDescrRunner`.
fn check_response_pipeline_for_runner(
    step: usize,
    mut result_model: DBResult<ExtendedPipelineDescrRunner>,
    mut result_impl: DBResult<ExtendedPipelineDescrRunner>,
) {
    result_model = result_model.map(convert_pipeline_for_runner_with_constant_timestamps);
    result_impl = result_impl.map(convert_pipeline_for_runner_with_constant_timestamps);
    check_responses(step, result_model, result_impl);
}

/// Compares model response to that of the database implementation
/// when the type is (created, `ExtendedPipelineDescrRunner`).
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

/// Compares model response to that of the database implementation
/// when the type is `Option<(TenantId, ExtendedPipelineDescr)>`.
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

/// Compares model response to that of the database implementation
/// when the type is `Vec<ExtendedPipelineDescr>` (ignores order).
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

/// Compares model response to that of the database implementation
/// when the type is `Vec<ExtendedPipelineDescrMonitoring>` (ignores order).
fn check_response_pipelines_for_monitoring(
    step: usize,
    mut result_model: DBResult<Vec<ExtendedPipelineDescrMonitoring>>,
    mut result_impl: DBResult<Vec<ExtendedPipelineDescrMonitoring>>,
) {
    result_model = result_model.map(|mut v| {
        v.sort_by(|p1, p2| p1.id.cmp(&p2.id));
        v.into_iter()
            .map(convert_pipeline_for_monitoring_with_constant_timestamps)
            .collect()
    });
    result_impl = result_impl.map(|mut v| {
        v.sort_by(|p1, p2| p1.id.cmp(&p2.id));
        v.into_iter()
            .map(convert_pipeline_for_monitoring_with_constant_timestamps)
            .collect()
    });
    check_responses(step, result_model, result_impl);
}

/// Compares model response to that of the database implementation
/// when the type is `Vec<(TenantId, ExtendedPipelineDescrMonitoring)>` (ignores order).
fn check_response_pipelines_for_monitoring_with_tenant_id(
    step: usize,
    mut result_model: DBResult<Vec<(TenantId, ExtendedPipelineDescrMonitoring)>>,
    mut result_impl: DBResult<Vec<(TenantId, ExtendedPipelineDescrMonitoring)>>,
) {
    result_model = result_model.map(|mut v| {
        v.sort_by(|(t1, p1), (t2, p2)| (t1, p1.id).cmp(&(t2, p2.id)));
        v.into_iter()
            .map(|(tenant_id, pipeline)| {
                (
                    tenant_id,
                    convert_pipeline_for_monitoring_with_constant_timestamps(pipeline),
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
                    convert_pipeline_for_monitoring_with_constant_timestamps(pipeline),
                )
            })
            .collect()
    });
    check_responses(step, result_model, result_impl);
}

/// Compares model response to that of the database implementation
/// when the type is `Vec<(TenantId, PipelineId)>` (ignores order).
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
    if let std::collections::btree_map::Entry::Vacant(e) = m.tenants.entry(tenant_id) {
        let rec = TenantRecord {
            id: tenant_id,
            tenant: Uuid::now_v7().to_string(),
            provider: Uuid::now_v7().to_string(),
        };
        e.insert(rec.clone());
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
#[allow(clippy::field_reassign_with_default)]
fn db_impl_behaves_like_model() {
    let _r = env_logger::try_init();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let handle = runtime.block_on(async { test_setup().await });

    // We use the lower-level proptest API `TestRunner` here because if we use
    // `proptest!` it was very difficult to get drop() called on `handle` (I
    // tried putting it in Tokio OnceCell, std OnceCell, and static_init with
    // Finaly); none of those options called drop on handle when the program
    // ended which meant the DB would not shut down after this test.
    let mut config = Config::default();
    config.max_shrink_iters = u32::MAX;
    config.source_file = Some("src/db/test.rs");
    let mut runner = TestRunner::new(config);
    let res = runner
        .run(
            &prop::collection::vec(any::<StorageAction>(), 0..256),
            |actions: Vec<StorageAction>| {
                let model = Mutex::new(DbModel {
                    tenants: BTreeMap::new(),
                    api_keys: BTreeMap::new(),
                    pipelines: BTreeMap::new(),
                    lifecycle_events: BTreeMap::new(),
                });
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
                            StorageAction::GetPipelineLifecycleEvents(tenant_id, pipeline_name, max_events) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_lifecycle_events(tenant_id, &pipeline_name, max_events).await;
                                let impl_response = handle.db.get_pipeline_lifecycle_events(tenant_id, &pipeline_name, max_events).await;

                                // remove event_id, recorded_at as they can't be same in db and model
                                // just verify the events and info is recorded correctly
                                let extract_relevant_info = |events: Vec<PipelineLifecycleEvent>| {
                                    events.into_iter().map(|e| (e.deployment_resources_status, e.deployment_runtime_status, e.deployment_runtime_desired_status, e.info)).collect::<Vec<_>>()
                                };


                                let model_response = model_response.map(extract_relevant_info);
                                let impl_response= impl_response.map(extract_relevant_info);
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::CleanupPipelineLifecycleEvents(older_than_days) => {
                                let model_response = model.cleanup_pipeline_lifecycle_events(older_than_days).await;
                                let impl_response = handle.db.cleanup_pipeline_lifecycle_events(older_than_days).await;
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

                            StorageAction::ListPipelines(tenant_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_pipelines(tenant_id).await;
                                let impl_response = handle.db.list_pipelines(tenant_id).await;
                                check_response_pipelines(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelinesForMonitoring(tenant_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.list_pipelines_for_monitoring(tenant_id).await;
                                let impl_response = handle.db.list_pipelines_for_monitoring(tenant_id).await;
                                check_response_pipelines_for_monitoring(i, model_response, impl_response);
                            }
                            StorageAction::GetPipeline(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.get_pipeline(tenant_id, &pipeline_name).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineForMonitoring(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_for_monitoring(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.get_pipeline_for_monitoring(tenant_id, &pipeline_name).await;
                                check_response_pipeline_for_monitoring(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineById(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_by_id(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_by_id(tenant_id, pipeline_id).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineByIdForMonitoring(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_by_id_for_monitoring(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_by_id_for_monitoring(tenant_id, pipeline_id).await;
                                check_response_pipeline_for_monitoring(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineByIdForRunner(tenant_id, pipeline_id, platform_version, provision_called) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.get_pipeline_by_id_for_runner(tenant_id, pipeline_id, &platform_version, provision_called).await;
                                let impl_response = handle.db.get_pipeline_by_id_for_runner(tenant_id, pipeline_id, &platform_version, provision_called).await;
                                check_response_pipeline_for_runner(i, model_response, impl_response);
                            }
                            StorageAction::NewPipeline(tenant_id, new_id, platform_version, pipeline_descr) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.new_pipeline(tenant_id, new_id, &platform_version, pipeline_descr.clone()).await;
                                let impl_response = handle.db.new_pipeline(tenant_id, new_id, &platform_version, pipeline_descr.clone()).await;
                                check_response_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::NewOrUpdatePipeline(tenant_id, new_id, original_name, platform_version, pipeline_descr) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.new_or_update_pipeline(tenant_id, new_id, &original_name, &platform_version, pipeline_descr.clone()).await;
                                let impl_response = handle.db.new_or_update_pipeline(tenant_id, new_id, &original_name, &platform_version, pipeline_descr.clone()).await;
                                check_response_pipeline_with_created(i, model_response, impl_response);
                            }
                            StorageAction::UpdatePipeline(tenant_id, original_name, name, description, platform_version, runtime_config, program_code, udf_rust, udf_toml, program_config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.update_pipeline(tenant_id, &original_name, &name, &description, &platform_version, &runtime_config, &program_code, &udf_rust, &udf_toml, &program_config).await;
                                let impl_response = handle.db.update_pipeline(tenant_id, &original_name, &name, &description, &platform_version, &runtime_config, &program_code, &udf_rust, &udf_toml, &program_config).await;
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
                            StorageAction::TransitProgramStatusToSqlCompiled(tenant_id, pipeline_id, program_version_guard, sql_compilation, program_info) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_sql_compiled(tenant_id, pipeline_id, program_version_guard, &sql_compilation, &program_info).await;
                                let impl_response = handle.db.transit_program_status_to_sql_compiled(tenant_id, pipeline_id, program_version_guard, &sql_compilation, &program_info).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToCompilingRust(tenant_id, pipeline_id, program_version_guard) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_compiling_rust(tenant_id, pipeline_id, program_version_guard).await;
                                let impl_response = handle.db.transit_program_status_to_compiling_rust(tenant_id, pipeline_id, program_version_guard).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToSuccess(tenant_id, pipeline_id, program_version_guard, rust_compilation, program_binary_source_checksum, program_binary_integrity_checksum) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_success(tenant_id, pipeline_id, program_version_guard, &rust_compilation, &program_binary_source_checksum, &program_binary_integrity_checksum).await;
                                let impl_response = handle.db.transit_program_status_to_success(tenant_id, pipeline_id, program_version_guard, &rust_compilation, &program_binary_source_checksum, &program_binary_integrity_checksum).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToSqlError(tenant_id, pipeline_id, program_version_guard, sql_compilation) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_sql_error(tenant_id, pipeline_id, program_version_guard, &sql_compilation).await;
                                let impl_response = handle.db.transit_program_status_to_sql_error(tenant_id, pipeline_id, program_version_guard, &sql_compilation).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToRustError(tenant_id, pipeline_id, program_version_guard, rust_compilation) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_rust_error(tenant_id, pipeline_id, program_version_guard, &rust_compilation).await;
                                let impl_response = handle.db.transit_program_status_to_rust_error(tenant_id, pipeline_id, program_version_guard, &rust_compilation).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitProgramStatusToSystemError(tenant_id, pipeline_id, program_version_guard, system_error) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_program_status_to_system_error(tenant_id, pipeline_id, program_version_guard, &system_error).await;
                                let impl_response = handle.db.transit_program_status_to_system_error(tenant_id, pipeline_id, program_version_guard, &system_error).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetDeploymentResourcesDesiredStatusProvisioned(tenant_id, pipeline_name, initial) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.set_deployment_resources_desired_status_provisioned(tenant_id, &pipeline_name, initial).await;
                                let impl_response = handle.db.set_deployment_resources_desired_status_provisioned(tenant_id, &pipeline_name, initial).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetDeploymentResourcesDesiredStatusStoppedIfNotProvisioned(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.set_deployment_resources_desired_status_stopped_if_not_provisioned(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.set_deployment_resources_desired_status_stopped_if_not_provisioned(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentResourcesStatusToProvisioning(tenant_id, pipeline_id, version_guard, deployment_id, pipeline_config) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_resources_status_to_provisioning(tenant_id, pipeline_id, version_guard, deployment_id, pipeline_config.clone()).await;
                                let impl_response = handle.db.transit_deployment_resources_status_to_provisioning(tenant_id, pipeline_id, version_guard, deployment_id, pipeline_config.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentResourcesStatusToProvisioned(tenant_id, pipeline_id, version_guard, deployment_location, extended_runtime_status) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_resources_status_to_provisioned(tenant_id, pipeline_id, version_guard, &deployment_location, extended_runtime_status.clone()).await;
                                let impl_response = handle.db.transit_deployment_resources_status_to_provisioned(tenant_id, pipeline_id, version_guard, &deployment_location, extended_runtime_status).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentResourcesStatusToStopping(tenant_id, pipeline_id, version_guard, deployment_error, suspend_info) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_resources_status_to_stopping(tenant_id, pipeline_id, version_guard, deployment_error.clone(), suspend_info.clone()).await;
                                let impl_response = handle.db.transit_deployment_resources_status_to_stopping(tenant_id, pipeline_id, version_guard, deployment_error, suspend_info).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitDeploymentResourcesStatusToStopped(tenant_id, pipeline_id, version_guard) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_deployment_resources_status_to_stopped(tenant_id, pipeline_id, version_guard).await;
                                let impl_response = handle.db.transit_deployment_resources_status_to_stopped(tenant_id, pipeline_id, version_guard).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitStorageStatusToClearing(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_storage_status_to_clearing_if_not_cleared(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.transit_storage_status_to_clearing_if_not_cleared(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::TransitStorageStatusToCleared(tenant_id, pipeline_id) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.transit_storage_status_to_cleared(tenant_id, pipeline_id).await;
                                let impl_response = handle.db.transit_storage_status_to_cleared(tenant_id, pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::IncrementNotifyCounter(tenant_id, pipeline_name) => {
                                create_tenants_if_not_exists(&model, &handle, tenant_id).await.unwrap();
                                let model_response = model.increment_notify_counter(tenant_id, &pipeline_name).await;
                                let impl_response = handle.db.increment_notify_counter(tenant_id, &pipeline_name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelineIdsAcrossAllTenants => {
                                let model_response = model.list_pipeline_ids_across_all_tenants().await;
                                let impl_response = handle.db.list_pipeline_ids_across_all_tenants().await;
                                check_response_pipeline_ids_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelinesAcrossAllTenantsForMonitoring => {
                                let model_response = model.list_pipelines_across_all_tenants_for_monitoring().await;
                                let impl_response = handle.db.list_pipelines_across_all_tenants_for_monitoring().await;
                                check_response_pipelines_for_monitoring_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::ClearOngoingSqlCompilation(platform_version) => {
                                let model_response = model.clear_ongoing_sql_compilation(&platform_version).await;
                                let impl_response = handle.db.clear_ongoing_sql_compilation(&platform_version).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetNextSqlCompilation(platform_version) => {
                                let model_response = model.get_next_sql_compilation(&platform_version).await;
                                let impl_response = handle.db.get_next_sql_compilation(&platform_version).await;
                                check_response_optional_pipeline_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::ClearOngoingRustCompilation(platform_version) => {
                                let model_response = model.clear_ongoing_rust_compilation(&platform_version).await;
                                let impl_response = handle.db.clear_ongoing_rust_compilation(&platform_version).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetNextRustCompilation(platform_version) => {
                                let model_response = model.get_next_rust_compilation(&platform_version).await;
                                let impl_response = handle.db.get_next_rust_compilation(&platform_version).await;
                                check_response_optional_pipeline_with_tenant_id(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelineProgramsAcrossAllTenants => {
                                let model_response = model.list_pipeline_programs_across_all_tenants().await;
                                let impl_response = handle.db.list_pipeline_programs_across_all_tenants().await;
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
#[derive(Debug)]
struct DbModel {
    pub tenants: BTreeMap<TenantId, TenantRecord>,
    pub api_keys: BTreeMap<(TenantId, String), (ApiKeyId, String, Vec<ApiPermission>)>,
    pub pipelines: BTreeMap<(TenantId, PipelineId), ExtendedPipelineDescr>,
    pub lifecycle_events: BTreeMap<(TenantId, PipelineId), Vec<PipelineLifecycleEvent>>,
}

#[async_trait]
trait ModelHelpers {
    #[allow(clippy::too_many_arguments)]
    async fn validate_and_apply_pipeline_update(
        &self,
        is_compiler_update: bool,
        tenant_id: TenantId,
        original_name: &str,
        name: &Option<String>,
        description: &Option<String>,
        platform_version: &str,
        runtime_config: &Option<serde_json::Value>,
        program_code: &Option<String>,
        udf_rust: &Option<String>,
        udf_toml: &Option<String>,
        program_config: &Option<serde_json::Value>,
    ) -> Result<ExtendedPipelineDescr, DBError>;

    async fn record_lifecycle_event(&self, tenant_id: TenantId, pipeline: &ExtendedPipelineDescr);
}

#[async_trait]
impl ModelHelpers for Mutex<DbModel> {
    /// Helps update the pipeline user fields by checking all the constraints and applying the
    /// relevant changes.
    async fn validate_and_apply_pipeline_update(
        &self,
        is_compiler_update: bool,
        tenant_id: TenantId,
        original_name: &str,
        name: &Option<String>,
        description: &Option<String>,
        platform_version: &str,
        runtime_config: &Option<serde_json::Value>,
        program_code: &Option<String>,
        udf_rust: &Option<String>,
        udf_toml: &Option<String>,
        program_config: &Option<serde_json::Value>,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        if let Some(name) = name {
            validate_name(name)?;
        }
        if let Some(runtime_config) = runtime_config {
            validate_runtime_config(runtime_config, false).map_err(|e| {
                DBError::InvalidRuntimeConfig {
                    value: runtime_config.clone(),
                    error: e,
                }
            })?;
        }
        if let Some(program_config) = program_config {
            validate_program_config(program_config, false).map_err(|e| {
                DBError::InvalidProgramConfig {
                    value: program_config.clone(),
                    error: e,
                }
            })?;
        }

        // Fetch existing pipeline
        let mut pipeline = self.get_pipeline(tenant_id, original_name).await?;

        // Pipeline must be stopped
        if !matches!(
            (
                pipeline.deployment_resources_status,
                pipeline.deployment_resources_desired_status,
                is_compiler_update
            ),
            (ResourcesStatus::Stopped, ResourcesDesiredStatus::Stopped, _)
                | (
                    ResourcesStatus::Stopped,
                    ResourcesDesiredStatus::Provisioned,
                    true
                )
        ) {
            return Err(DBError::UpdateRestrictedToStopped);
        }

        // While stopped, certain fields are not allowed to be updated when storage is not cleared
        if pipeline.storage_status != StorageStatus::Cleared {
            let mut not_allowed = vec![];
            if name.as_ref().is_some_and(|v| *v != pipeline.name) {
                not_allowed.push("`name`")
            }
            if description
                .as_ref()
                .is_some_and(|v| *v != pipeline.description)
            {
                not_allowed.push("`description`")
            }
            // `platform_version` can be updated
            // Some fields of `runtime_config` are not allowed to be updated
            if let Some(runtime_config) = &runtime_config {
                if runtime_config.get("workers") != pipeline.runtime_config.get("workers") {
                    not_allowed.push("`runtime_config.workers`");
                }
                if runtime_config.get("storage") != pipeline.runtime_config.get("storage") {
                    not_allowed.push("`runtime_config.storage`");
                }
                if runtime_config.get("fault_tolerance")
                    != pipeline.runtime_config.get("fault_tolerance")
                {
                    not_allowed.push("`runtime_config.fault_tolerance`");
                }
                if runtime_config
                    .get("resources")
                    .map(|v| v.get("storage_mb_max"))
                    != pipeline
                        .runtime_config
                        .get("resources")
                        .map(|v| v.get("storage_mb_max"))
                {
                    not_allowed.push("`runtime_config.resources.storage_mb_max`");
                }
            }
            if program_code
                .as_ref()
                .is_some_and(|v| *v != pipeline.program_code)
            {
                not_allowed.push("`program_code`")
            }
            if udf_rust.as_ref().is_some_and(|v| *v != pipeline.udf_rust) {
                not_allowed.push("`udf_rust`")
            }
            if udf_toml.as_ref().is_some_and(|v| *v != pipeline.udf_toml) {
                not_allowed.push("`udf_toml`")
            }
            if program_config
                .as_ref()
                .is_some_and(|v| *v != pipeline.program_config)
            {
                not_allowed.push("`program_config`")
            }
            if !not_allowed.is_empty() {
                return Err(DBError::EditRestrictedToClearedStorage {
                    not_allowed: not_allowed.iter_mut().map(|s| s.to_string()).collect(),
                });
            }
        }

        // Update the base fields
        let mut version_increment = false;
        let mut program_version_increment = false;
        if let Some(name) = name {
            // Constraint: name is unique
            if self
                .lock()
                .await
                .pipelines
                .iter()
                .filter(|((tid, _), _)| *tid == tenant_id)
                .map(|(_, p)| p)
                .any(|p| p.name == *name && p.id != pipeline.id)
            {
                return Err(DBError::DuplicateName);
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
        if *platform_version != pipeline.platform_version {
            version_increment = true;
            program_version_increment = true;
        }
        pipeline.platform_version = platform_version.to_string();
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
        if let Some(udf_rust) = udf_rust {
            if *udf_rust != pipeline.udf_rust {
                version_increment = true;
                program_version_increment = true;
            }
            pipeline.udf_rust = udf_rust.clone();
        }
        if let Some(udf_toml) = udf_toml {
            if *udf_toml != pipeline.udf_toml {
                version_increment = true;
                program_version_increment = true;
            }
            pipeline.udf_toml = udf_toml.clone();
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
            pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
        }

        // Program changed
        if program_version_increment {
            assert!(version_increment); // Whenever program_version is incremented, version should also have been
            pipeline.program_version = Version(pipeline.program_version.0 + 1);
            pipeline.program_status = ProgramStatus::Pending;
            pipeline.program_status_since = Utc::now();
            pipeline.program_error = ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: None,
            };
            pipeline.program_info = None;
            pipeline.program_binary_source_checksum = None;
            pipeline.program_binary_integrity_checksum = None;
        }

        // Insert into state (will overwrite)
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());

        // Return the final extended pipeline descriptor
        Ok(pipeline)
    }

    /// Helper to insert a pipeline lifecycle event into the model.
    async fn record_lifecycle_event(&self, tenant_id: TenantId, pipeline: &ExtendedPipelineDescr) {
        let deployment_resources_status = pipeline.deployment_resources_status.to_string();
        let deployment_runtime_status = pipeline
            .deployment_runtime_status
            .map(super::types::pipeline::runtime_status_to_string);
        let deployment_runtime_desired_status = pipeline
            .deployment_runtime_desired_status
            .map(super::types::pipeline::runtime_desired_status_to_string);

        let info = pipeline
            .deployment_error
            .as_ref()
            .map(|e| serde_json::to_string(e).unwrap());
        // .or_else(|| {
        //     pipeline
        //         .suspend_info
        //         .as_ref()
        //         .map(|e| serde_json::to_string(e).unwrap())
        // });
        let event = PipelineLifecycleEvent {
            event_id: Uuid::now_v7(),
            deployment_resources_status,
            deployment_runtime_status,
            deployment_runtime_desired_status,
            info,
            recorded_at: Utc::now().naive_utc(),
        };
        self.lock()
            .await
            .lifecycle_events
            .entry((tenant_id, pipeline.id))
            .or_default()
            .push(event);
    }
}

/// Converts the complete extended descriptor to one with only fields relevant to monitoring.
fn convert_descriptor_to_monitoring(
    pipeline: ExtendedPipelineDescr,
) -> ExtendedPipelineDescrMonitoring {
    ExtendedPipelineDescrMonitoring {
        id: pipeline.id,
        name: pipeline.name.clone(),
        description: pipeline.description.clone(),
        created_at: pipeline.created_at,
        version: pipeline.version,
        platform_version: pipeline.platform_version.clone(),
        program_version: pipeline.program_version,
        program_status: pipeline.program_status,
        program_status_since: pipeline.program_status_since,
        deployment_error: pipeline.deployment_error.clone(),
        deployment_location: pipeline.deployment_location.clone(),
        refresh_version: pipeline.refresh_version,
        storage_status: pipeline.storage_status,
        deployment_id: pipeline.deployment_id,
        deployment_initial: pipeline.deployment_initial,
        deployment_resources_status: pipeline.deployment_resources_status,
        deployment_resources_status_since: pipeline.program_status_since,
        deployment_resources_desired_status: pipeline.deployment_resources_desired_status,
        deployment_resources_desired_status_since: pipeline
            .deployment_resources_desired_status_since,
        deployment_runtime_status: pipeline.deployment_runtime_status,
        deployment_runtime_status_since: pipeline.deployment_runtime_status_since,
        deployment_runtime_desired_status: pipeline.deployment_runtime_desired_status,
        deployment_runtime_desired_status_since: pipeline.deployment_runtime_desired_status_since,
    }
}

// Performs general validation of moving from one program status to the next.
fn validate_new_program_status(
    pipeline: &ExtendedPipelineDescr,
    program_version_guard: Version,
    new_status: ProgramStatus,
) -> Result<(), DBError> {
    // Check version guard
    if pipeline.program_version != program_version_guard {
        return Err(DBError::OutdatedProgramVersion {
            outdated_version: program_version_guard,
            latest_version: pipeline.program_version,
        });
    }

    // Check transition
    validate_program_status_transition(pipeline.program_status, new_status)?;

    // Pipeline must be stopped
    if pipeline.deployment_resources_status != ResourcesStatus::Stopped {
        return Err(DBError::ProgramStatusUpdateRestrictedToStopped);
    }

    Ok(())
}

// Performs general validation of moving from one deployment resources status to the next.
fn validate_new_deployment_resources_status(
    pipeline: &ExtendedPipelineDescr,
    version_guard: Version,
    new_status: ResourcesStatus,
) -> Result<(), DBError> {
    // Version guard
    if pipeline.version != version_guard {
        return Err(DBError::OutdatedPipelineVersion {
            outdated_version: version_guard,
            latest_version: pipeline.version,
        });
    }

    // Check transition
    validate_resources_status_transition(
        pipeline.storage_status,
        pipeline.deployment_resources_status,
        new_status,
    )?;

    // Check program is compiled if needed
    if !matches!(
        (pipeline.deployment_resources_status, new_status),
        (ResourcesStatus::Stopped, ResourcesStatus::Stopping)
            | (ResourcesStatus::Stopping, ResourcesStatus::Stopped)
    ) && pipeline.program_status != ProgramStatus::Success
    {
        return Err(DBError::TransitionRequiresCompiledProgram {
            current: pipeline.deployment_resources_status,
            transition_to: new_status,
        });
    }

    Ok(())
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

    async fn get_tenant_name(&self, tenant_id: TenantId) -> Result<String, DBError> {
        let s = self.lock().await;
        s.tenants
            .get(&tenant_id)
            .map(|tenant| tenant.tenant.clone())
            .ok_or(DBError::UnknownTenant { tenant_id })
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
        validate_name(name)?;
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
        let record = record.first();
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

    async fn list_pipelines_for_monitoring(
        &self,
        tenant_id: TenantId,
    ) -> Result<Vec<ExtendedPipelineDescrMonitoring>, DBError> {
        self.list_pipelines(tenant_id).await.map(|pipelines| {
            pipelines
                .iter()
                .map(|v| convert_descriptor_to_monitoring(v.clone()))
                .collect()
        })
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

    async fn get_pipeline_for_monitoring(
        &self,
        tenant_id: TenantId,
        name: &str,
    ) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
        self.get_pipeline(tenant_id, name)
            .await
            .map(convert_descriptor_to_monitoring)
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

    async fn get_pipeline_by_id_for_monitoring(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<ExtendedPipelineDescrMonitoring, DBError> {
        self.get_pipeline_by_id(tenant_id, pipeline_id)
            .await
            .map(convert_descriptor_to_monitoring)
    }

    async fn get_pipeline_by_id_for_runner(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        platform_version: &str,
        provision_called: bool,
    ) -> Result<ExtendedPipelineDescrRunner, DBError> {
        let pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let is_ready_compiled = pipeline.program_status == ProgramStatus::Success
            && pipeline.platform_version == platform_version;
        if matches!(
            (
                pipeline.deployment_resources_status,
                pipeline.deployment_resources_desired_status,
                is_ready_compiled,
                provision_called
            ),
            (
                ResourcesStatus::Stopped,
                ResourcesDesiredStatus::Provisioned,
                true,
                _,
            ) | (
                ResourcesStatus::Provisioning,
                ResourcesDesiredStatus::Provisioned,
                _,
                false,
            ),
        ) {
            Ok(ExtendedPipelineDescrRunner::Complete(pipeline))
        } else {
            Ok(ExtendedPipelineDescrRunner::Monitoring(
                convert_descriptor_to_monitoring(pipeline),
            ))
        }
    }

    async fn new_pipeline(
        &self,
        tenant_id: TenantId,
        new_id: Uuid,
        platform_version: &str,
        pipeline: PipelineDescr,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        let mut state = self.lock().await;

        validate_name(&pipeline.name)?;
        validate_runtime_config(&pipeline.runtime_config, false).map_err(|e| {
            DBError::InvalidRuntimeConfig {
                value: pipeline.runtime_config.clone(),
                error: e,
            }
        })?;
        validate_program_config(&pipeline.program_config, false).map_err(|e| {
            DBError::InvalidProgramConfig {
                value: pipeline.program_config.clone(),
                error: e,
            }
        })?;

        // Constraint: UUID is unique
        if state.pipelines.keys().any(|(_, pid)| pid.0 == new_id) {
            return Err(DBError::unique_key_violation("pipeline_pkey"));
        }

        // Constraint: name is unique
        if state
            .pipelines
            .iter()
            .filter(|((tid, _), _)| *tid == tenant_id)
            .map(|(_, p)| p)
            .any(|p| p.name == pipeline.name)
        {
            return Err(DBError::DuplicateName);
        }

        // Create extended descriptor
        let pipeline_id = PipelineId(new_id);
        let now = Utc::now();
        let extended_pipeline = ExtendedPipelineDescr {
            id: pipeline_id,
            name: pipeline.name,
            description: pipeline.description,
            created_at: now,
            version: Version(1),
            platform_version: platform_version.to_string(),
            runtime_config: pipeline.runtime_config,
            program_code: pipeline.program_code,
            udf_rust: pipeline.udf_rust,
            udf_toml: pipeline.udf_toml,
            program_config: pipeline.program_config,
            program_version: Version(1),
            program_status: ProgramStatus::Pending,
            program_status_since: now,
            program_error: ProgramError {
                sql_compilation: None,
                rust_compilation: None,
                system_error: None,
            },
            program_info: None,
            program_binary_source_checksum: None,
            program_binary_integrity_checksum: None,
            deployment_error: None,
            deployment_config: None,
            deployment_location: None,
            refresh_version: Version(1),
            suspend_info: None,
            storage_status: StorageStatus::Cleared,
            deployment_id: None,
            deployment_initial: None,
            deployment_resources_status: ResourcesStatus::Stopped,
            deployment_resources_status_since: now,
            deployment_resources_desired_status: ResourcesDesiredStatus::Stopped,
            deployment_resources_desired_status_since: now,
            deployment_runtime_status: None,
            deployment_runtime_status_since: None,
            deployment_runtime_desired_status: None,
            deployment_runtime_desired_status_since: None,
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
        platform_version: &str,
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
                    platform_version,
                    &Some(pipeline.runtime_config),
                    &Some(pipeline.program_code),
                    &Some(pipeline.udf_rust),
                    &Some(pipeline.udf_toml),
                    &Some(pipeline.program_config),
                )
                .await?,
            )),
            Err(e) => match e {
                DBError::UnknownPipelineName { .. } => {
                    if original_name != pipeline.name {
                        return Err(DBError::CannotRenameNonExistingPipeline);
                    }
                    Ok((
                        true,
                        self.new_pipeline(tenant_id, new_id, platform_version, pipeline)
                            .await?,
                    ))
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
        platform_version: &str,
        runtime_config: &Option<serde_json::Value>,
        program_code: &Option<String>,
        udf_rust: &Option<String>,
        udf_toml: &Option<String>,
        program_config: &Option<serde_json::Value>,
    ) -> Result<ExtendedPipelineDescr, DBError> {
        self.validate_and_apply_pipeline_update(
            false,
            tenant_id,
            original_name,
            name,
            description,
            platform_version,
            runtime_config,
            program_code,
            udf_rust,
            udf_toml,
            program_config,
        )
        .await
    }

    async fn delete_pipeline(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        // Validate
        let pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        if pipeline.deployment_resources_status != ResourcesStatus::Stopped
            || pipeline.deployment_resources_desired_status != ResourcesDesiredStatus::Stopped
        {
            return Err(DBError::DeleteRestrictedToFullyStopped);
        }
        if pipeline.storage_status != StorageStatus::Cleared {
            return Err(DBError::DeleteRestrictedToClearedStorage);
        }

        let mut state = self.lock().await;
        // Delete from state
        state.pipelines.remove(&(tenant_id, pipeline.id));
        // remove events for the pipeline
        state.lifecycle_events.remove(&(tenant_id, pipeline.id));

        Ok(pipeline.id)
    }

    async fn transit_program_status_to_pending(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::Pending;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_error = ProgramError {
            sql_compilation: None,
            rust_compilation: None,
            system_error: None,
        };
        pipeline.program_info = None;
        pipeline.program_binary_source_checksum = None;
        pipeline.program_binary_integrity_checksum = None;
        pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
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
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::CompilingSql;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn transit_program_status_to_sql_compiled(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        program_version_guard: Version,
        sql_compilation: &SqlCompilationInfo,
        program_info: &serde_json::Value,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::SqlCompiled;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;
        validate_program_info(program_info).map_err(|e| DBError::InvalidProgramInfo {
            value: program_info.clone(),
            error: e,
        })?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_error = ProgramError {
            sql_compilation: Some(sql_compilation.clone()),
            rust_compilation: None,
            system_error: None,
        };
        pipeline.program_info = Some(program_info.clone());
        pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
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
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::CompilingRust;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
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
        rust_compilation: &RustCompilationInfo,
        program_binary_source_checksum: &str,
        program_binary_integrity_checksum: &str,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::Success;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_error = ProgramError {
            sql_compilation: pipeline.program_error.sql_compilation,
            rust_compilation: Some(rust_compilation.clone()),
            system_error: None,
        };
        pipeline.program_binary_source_checksum = Some(program_binary_source_checksum.to_string());
        pipeline.program_binary_integrity_checksum =
            Some(program_binary_integrity_checksum.to_string());
        pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
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
        sql_compilation: &SqlCompilationInfo,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::SqlError;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_error = ProgramError {
            sql_compilation: Some(sql_compilation.clone()),
            rust_compilation: None,
            system_error: None,
        };
        pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
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
        rust_compilation: &RustCompilationInfo,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::RustError;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_error = ProgramError {
            sql_compilation: pipeline.program_error.sql_compilation,
            rust_compilation: Some(rust_compilation.clone()),
            system_error: None,
        };
        pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
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
        system_error: &str,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = ProgramStatus::SystemError;
        validate_new_program_status(&pipeline, program_version_guard, new_status)?;

        // Apply changes: update
        pipeline.program_status = new_status;
        pipeline.program_status_since = Utc::now();
        pipeline.program_error = ProgramError {
            sql_compilation: pipeline.program_error.sql_compilation,
            rust_compilation: pipeline.program_error.rust_compilation,
            system_error: Some(system_error.to_string()),
        };
        pipeline.refresh_version = Version(pipeline.refresh_version.0 + 1);
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn set_deployment_resources_desired_status_provisioned(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        initial: RuntimeDesiredStatus,
    ) -> Result<PipelineId, DBError> {
        // Validate
        let mut pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        let new_resources_desired_status = ResourcesDesiredStatus::Provisioned;
        validate_resources_desired_status_transition(
            pipeline.deployment_resources_status,
            pipeline.deployment_resources_desired_status,
            new_resources_desired_status,
        )?;
        if pipeline.deployment_initial.is_some_and(|v| v != initial) {
            return Err(DBError::InitialImmutableUnlessStopped);
        }
        let is_file_backend = pipeline
            .runtime_config
            .get("storage")
            .and_then(|v| v.get("backend"))
            .and_then(|v| v.get("name"))
            .map(|v| v.as_str() == Some("file"))
            .unwrap_or(false);
        let is_sync_configured = pipeline
            .runtime_config
            .get("storage")
            .and_then(|v| v.get("backend"))
            .and_then(|v| v.get("config"))
            .and_then(|v| v.get("sync"))
            .map(|v| !v.is_null())
            .unwrap_or(false);
        if initial == RuntimeDesiredStatus::Standby && (!is_file_backend || !is_sync_configured) {
            return Err(DBError::InitialStandbyNotAllowed);
        }

        // Apply changes: update
        pipeline.deployment_initial = Some(initial);
        pipeline.deployment_resources_desired_status = new_resources_desired_status;
        pipeline.deployment_resources_desired_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(pipeline.id)
    }

    async fn set_deployment_resources_desired_status_stopped_if_not_provisioned(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(bool, PipelineId), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        if pipeline.deployment_resources_status != ResourcesStatus::Provisioned {
            let new_resources_desired_status = ResourcesDesiredStatus::Stopped;
            validate_resources_desired_status_transition(
                pipeline.deployment_resources_status,
                pipeline.deployment_resources_desired_status,
                new_resources_desired_status,
            )?;

            // Apply changes: update
            if pipeline.deployment_resources_status == ResourcesStatus::Stopped {
                pipeline.deployment_initial = None;
            } else {
                // Retain: pipeline.deployment_initial
            }
            pipeline.deployment_resources_desired_status = new_resources_desired_status;
            pipeline.deployment_resources_desired_status_since = Utc::now();
            self.lock()
                .await
                .pipelines
                .insert((tenant_id, pipeline.id), pipeline.clone());
            Ok((true, pipeline.id))
        } else {
            Ok((false, pipeline.id))
        }
    }

    async fn set_deployment_resources_desired_status_stopped(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        // Validate
        let mut pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        let new_resources_desired_status = ResourcesDesiredStatus::Stopped;
        validate_resources_desired_status_transition(
            pipeline.deployment_resources_status,
            pipeline.deployment_resources_desired_status,
            new_resources_desired_status,
        )?;

        // Apply changes: update
        if pipeline.deployment_resources_status == ResourcesStatus::Stopped {
            pipeline.deployment_initial = None;
        } else {
            // Retain: pipeline.deployment_initial
        }
        pipeline.deployment_resources_desired_status = new_resources_desired_status;
        pipeline.deployment_resources_desired_status_since = Utc::now();
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(pipeline.id)
    }

    async fn transit_deployment_resources_status_to_provisioning(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        deployment_id: Uuid,
        deployment_config: serde_json::Value,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        validate_storage_status_transition(
            pipeline.deployment_resources_status,
            pipeline.storage_status,
            StorageStatus::InUse,
        )?;
        let new_resources_status = ResourcesStatus::Provisioning;
        validate_new_deployment_resources_status(&pipeline, version_guard, new_resources_status)?;
        validate_deployment_config(&deployment_config).map_err(|e| {
            DBError::InvalidDeploymentConfig {
                value: deployment_config.clone(),
                error: e,
            }
        })?;

        // Apply changes
        pipeline.storage_status = StorageStatus::InUse;
        pipeline.deployment_id = Some(deployment_id);
        // Retain: pipeline.deployment_initial
        pipeline.deployment_config = Some(deployment_config);
        pipeline.deployment_location = None;
        pipeline.deployment_error = None;
        pipeline.suspend_info = None;
        pipeline.deployment_resources_status = new_resources_status;
        pipeline.deployment_resources_status_since = Utc::now();
        // Retain: pipeline.deployment_resources_desired_status
        // Retain: pipeline.deployment_resources_desired_status_since
        pipeline.deployment_runtime_status = None;
        pipeline.deployment_runtime_status_since = None;
        pipeline.deployment_runtime_desired_status = None;
        pipeline.deployment_runtime_desired_status_since = None;

        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        self.record_lifecycle_event(tenant_id, &pipeline).await;
        Ok(())
    }

    async fn transit_deployment_resources_status_to_provisioned(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        deployment_location: &str,
        extended_runtime_status: ExtendedRuntimeStatus,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_resources_status = ResourcesStatus::Provisioned;
        let new_runtime_status = extended_runtime_status.runtime_status;
        let new_runtime_desired_status = extended_runtime_status.runtime_desired_status;
        validate_new_deployment_resources_status(&pipeline, version_guard, new_resources_status)?;

        // Apply changes
        // Retain: pipeline.deployment_id
        // Retain: pipeline.deployment_initial
        // Retain: pipeline.deployment_config
        pipeline.deployment_location = Some(deployment_location.to_string());
        pipeline.deployment_error = None;
        pipeline.suspend_info = None;
        pipeline.deployment_resources_status = new_resources_status;
        pipeline.deployment_resources_status_since = Utc::now();
        // Retain: pipeline.deployment_resources_desired_status
        // Retain: pipeline.deployment_resources_desired_status_since
        pipeline.deployment_runtime_status = Some(new_runtime_status);
        pipeline.deployment_runtime_status_since = Some(Utc::now());
        pipeline.deployment_runtime_desired_status = Some(new_runtime_desired_status);
        pipeline.deployment_runtime_desired_status_since = Some(Utc::now());
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        self.record_lifecycle_event(tenant_id, &pipeline).await;
        Ok(())
    }

    async fn transit_deployment_resources_status_to_stopping(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
        deployment_error: Option<ErrorResponse>,
        suspend_info: Option<serde_json::Value>,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_resources_status = ResourcesStatus::Stopping;
        validate_new_deployment_resources_status(&pipeline, version_guard, new_resources_status)?;

        // Apply changes
        pipeline.deployment_id = None;
        pipeline.deployment_initial = None;
        pipeline.deployment_config = None;
        pipeline.deployment_location = None;
        pipeline.deployment_error = deployment_error;
        pipeline.suspend_info = suspend_info;
        pipeline.deployment_resources_status = new_resources_status;
        pipeline.deployment_resources_status_since = Utc::now();
        pipeline.deployment_resources_desired_status = ResourcesDesiredStatus::Stopped;
        pipeline.deployment_resources_desired_status_since = Utc::now();
        pipeline.deployment_runtime_status = None;
        pipeline.deployment_runtime_status_since = None;
        pipeline.deployment_runtime_desired_status = None;
        pipeline.deployment_runtime_desired_status_since = None;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        self.record_lifecycle_event(tenant_id, &pipeline).await;
        Ok(())
    }

    async fn transit_deployment_resources_status_to_stopped(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
        version_guard: Version,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_resources_status = ResourcesStatus::Stopped;
        validate_new_deployment_resources_status(&pipeline, version_guard, new_resources_status)?;

        // Apply changes
        pipeline.deployment_id = None;
        pipeline.deployment_initial = None;
        pipeline.deployment_config = None;
        pipeline.deployment_location = None;
        // Retain: pipeline.deployment_error
        // Retain: pipeline.suspend_info
        pipeline.deployment_resources_status = new_resources_status;
        pipeline.deployment_resources_status_since = Utc::now();
        pipeline.deployment_resources_desired_status = ResourcesDesiredStatus::Stopped;
        pipeline.deployment_resources_desired_status_since = Utc::now();
        pipeline.deployment_runtime_status = None;
        pipeline.deployment_runtime_status_since = None;
        pipeline.deployment_runtime_desired_status = None;
        pipeline.deployment_runtime_desired_status_since = None;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        self.record_lifecycle_event(tenant_id, &pipeline).await;
        Ok(())
    }

    async fn transit_storage_status_to_clearing_if_not_cleared(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<PipelineId, DBError> {
        // Validate
        let mut pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        if pipeline.storage_status != StorageStatus::Cleared {
            let new_status = StorageStatus::Clearing;
            validate_storage_status_transition(
                pipeline.deployment_resources_status,
                pipeline.storage_status,
                new_status,
            )?;

            // Apply changes
            pipeline.storage_status = new_status;
            self.lock()
                .await
                .pipelines
                .insert((tenant_id, pipeline.id), pipeline.clone());
        }
        Ok(pipeline.id)
    }

    async fn transit_storage_status_to_cleared(
        &self,
        tenant_id: TenantId,
        pipeline_id: PipelineId,
    ) -> Result<(), DBError> {
        // Validate
        let mut pipeline = self.get_pipeline_by_id(tenant_id, pipeline_id).await?;
        let new_status = StorageStatus::Cleared;
        validate_storage_status_transition(
            pipeline.deployment_resources_status,
            pipeline.storage_status,
            new_status,
        )?;

        // Apply changes
        pipeline.storage_status = new_status;
        self.lock()
            .await
            .pipelines
            .insert((tenant_id, pipeline.id), pipeline.clone());
        Ok(())
    }

    async fn increment_notify_counter(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
    ) -> Result<(), DBError> {
        let _pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        // The notify_counter field in the database is used solely as a notification mechanism for
        // LISTEN calls on the pipeline table. As such, it should have no effect on the state of the
        // pipeline and the logic surrounding it. It is also not exposed to the user.
        Ok(())
    }

    async fn list_pipeline_ids_across_all_tenants(
        &self,
    ) -> Result<Vec<(TenantId, PipelineId)>, DBError> {
        let mut result: Vec<(TenantId, PipelineId)> = self
            .lock()
            .await
            .pipelines
            .iter()
            .map(|((tid, pid), _)| (*tid, *pid))
            .collect();
        result.sort_by(|(_, p1), (_, p2)| p1.cmp(p2));
        Ok(result)
    }

    async fn list_pipelines_across_all_tenants_for_monitoring(
        &self,
    ) -> Result<Vec<(TenantId, ExtendedPipelineDescrMonitoring)>, DBError> {
        let mut result: Vec<(TenantId, ExtendedPipelineDescrMonitoring)> = self
            .lock()
            .await
            .pipelines
            .iter()
            .map(|((tid, _), pipeline)| (*tid, convert_descriptor_to_monitoring(pipeline.clone())))
            .collect();
        result.sort_by(|(_, p1), (_, p2)| p1.id.cmp(&p2.id));
        Ok(result)
    }

    async fn clear_ongoing_sql_compilation(&self, platform_version: &str) -> Result<(), DBError> {
        let pipelines = self
            .list_pipelines_across_all_tenants_for_monitoring()
            .await?;
        for (tid, pipeline) in pipelines {
            if pipeline.deployment_resources_status == ResourcesStatus::Stopped {
                if pipeline.platform_version == platform_version {
                    if pipeline.program_status == ProgramStatus::CompilingSql {
                        self.transit_program_status_to_pending(
                            tid,
                            pipeline.id,
                            pipeline.program_version,
                        )
                        .await?;
                    }
                } else if pipeline.program_status == ProgramStatus::Pending
                    || pipeline.program_status == ProgramStatus::CompilingSql
                {
                    self.validate_and_apply_pipeline_update(
                        true,
                        tid,
                        &pipeline.name,
                        &None,
                        &None,
                        platform_version,
                        &None,
                        &None,
                        &None,
                        &None,
                        &None,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn get_next_sql_compilation(
        &self,
        platform_version: &str,
    ) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut pipelines: Vec<(TenantId, ExtendedPipelineDescr)> = self
            .lock()
            .await
            .pipelines
            .iter()
            .filter(|(_, p)| {
                p.deployment_resources_status == ResourcesStatus::Stopped
                    && p.program_status == ProgramStatus::Pending
                    && p.platform_version == platform_version
            })
            .map(|((tid, _), pipeline)| (*tid, pipeline.clone()))
            .collect();
        if pipelines.is_empty() {
            return Ok(None);
        }
        pipelines.sort_by(|(_, p1), (_, p2)| {
            (p1.program_status_since, p1.id).cmp(&(p2.program_status_since, p2.id))
        });
        let chosen = pipelines.first().unwrap().clone(); // Already checked for empty
        Ok(Some((chosen.0, chosen.1)))
    }

    async fn clear_ongoing_rust_compilation(&self, platform_version: &str) -> Result<(), DBError> {
        let pipelines = self
            .list_pipelines_across_all_tenants_for_monitoring()
            .await?;
        for (tid, pipeline) in pipelines {
            if pipeline.deployment_resources_status == ResourcesStatus::Stopped {
                if pipeline.platform_version == platform_version {
                    if pipeline.program_status == ProgramStatus::CompilingRust {
                        let pipeline_complete = self.get_pipeline_by_id(tid, pipeline.id).await?;
                        self.transit_program_status_to_sql_compiled(
                            tid,
                            pipeline.id,
                            pipeline.program_version,
                            &pipeline_complete.program_error.sql_compilation.unwrap(),
                            &pipeline_complete.program_info.unwrap(),
                        )
                        .await?;
                    }
                } else if pipeline.program_status == ProgramStatus::SqlCompiled
                    || pipeline.program_status == ProgramStatus::CompilingRust
                {
                    self.validate_and_apply_pipeline_update(
                        true,
                        tid,
                        &pipeline.name,
                        &None,
                        &None,
                        platform_version,
                        &None,
                        &None,
                        &None,
                        &None,
                        &None,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn get_next_rust_compilation(
        &self,
        platform_version: &str,
    ) -> Result<Option<(TenantId, ExtendedPipelineDescr)>, DBError> {
        let mut pipelines: Vec<(TenantId, ExtendedPipelineDescr)> = self
            .lock()
            .await
            .pipelines
            .iter()
            .filter(|(_, p)| {
                p.deployment_resources_status == ResourcesStatus::Stopped
                    && p.program_status == ProgramStatus::SqlCompiled
                    && p.platform_version == platform_version
            })
            .map(|((tid, _), pipeline)| (*tid, pipeline.clone()))
            .collect();
        if pipelines.is_empty() {
            return Ok(None);
        }
        pipelines.sort_by(|(_, p1), (_, p2)| {
            (p1.program_status_since, p1.id).cmp(&(p2.program_status_since, p2.id))
        });
        let chosen = pipelines.first().unwrap().clone(); // Already checked for empty
        Ok(Some((chosen.0, chosen.1)))
    }

    async fn list_pipeline_programs_across_all_tenants(
        &self,
    ) -> Result<Vec<(PipelineId, Version, String, String)>, DBError> {
        let mut checksums: Vec<(PipelineId, Version, String, String)> = self
            .lock()
            .await
            .pipelines
            .values()
            .filter(|pipeline| pipeline.program_status == ProgramStatus::Success)
            .map(|pipeline| {
                (
                    pipeline.id,
                    pipeline.program_version,
                    pipeline.program_binary_source_checksum.clone().unwrap(),
                    pipeline.program_binary_integrity_checksum.clone().unwrap(),
                )
            })
            .collect();
        checksums.sort_by(|p1, p2| p1.0.cmp(&p2.0));
        Ok(checksums)
    }

    async fn get_support_bundle_data(
        &self,
        _tenant_id: TenantId,
        _pipeline_name: &str,
        _how_many: u64,
    ) -> Result<(ExtendedPipelineDescrMonitoring, Vec<SupportBundleData>), DBError> {
        unimplemented!()
    }

    async fn get_pipeline_lifecycle_events(
        &self,
        tenant_id: TenantId,
        pipeline_name: &str,
        max_events: u32,
    ) -> Result<Vec<PipelineLifecycleEvent>, DBError> {
        let pipeline = self.get_pipeline(tenant_id, pipeline_name).await?;
        let mut events = self
            .lock()
            .await
            .lifecycle_events
            .get(&(tenant_id, pipeline.id))
            .cloned()
            .unwrap_or_default();

        // sort event by recorded_at in ascending order
        // similar to `ORDER BY p.recorded_at ASC`
        events.sort_by(|e1, e2| e1.recorded_at.cmp(&e2.recorded_at));

        // LIMIT max_events;
        events.truncate(max_events as usize);

        Ok(events)
    }

    async fn cleanup_pipeline_lifecycle_events(&self, retention_days: u16) -> Result<u64, DBError> {
        // similar to query:
        // DELETE FROM pipeline_lifecycle_events WHERE recorded_at < NOW() - make_interval(days => $1)
        let cutoff_time = Utc::now() - Duration::from_secs(retention_days as u64 * 24 * 60 * 60);
        let mut total_deleted = 0;

        let mut state = self.lock().await;
        state.lifecycle_events.retain(|_, events| {
            let original_len = events.len();
            events.retain(|event| event.recorded_at.and_utc() >= cutoff_time);
            total_deleted += (original_len - events.len()) as u64;
            !events.is_empty()
        });

        Ok(total_deleted)
    }
}
