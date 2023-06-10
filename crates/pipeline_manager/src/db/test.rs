use super::{
    storage::Storage, AttachedConnector, ConnectorDescr, ConnectorId, DBError, PipelineId,
    PipelineStatus, ProgramDescr, ProgramId, ProgramStatus, ProjectDB, Version,
};
use super::{ApiPermission, PipelineDescr};
use crate::auth;
use anyhow::Result as AnyResult;
use async_trait::async_trait;
use openssl::sha::{self};
use pretty_assertions::assert_eq;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use proptest_derive::Arbitrary;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::time::SystemTime;
use std::vec;
use tokio::sync::Mutex;
use uuid::Uuid;

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
            let db_name = self.config.get_dbname().unwrap_or("").clone();

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
    let conn = ProjectDB::connect_inner(&db_uri, &Some("".to_string()), Some(pg))
        .await
        .unwrap();
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
    let conn = ProjectDB::with_config(config.clone(), &Some("".to_string()))
        .await
        .unwrap();

    (conn, config)
}

#[tokio::test]
async fn program_creation() {
    let handle = test_setup().await;
    let res = handle
        .db
        .new_program(Uuid::now_v7(), "test1", "program desc", "ignored")
        .await
        .unwrap();
    let rows = handle.db.list_programs().await.unwrap();
    assert_eq!(1, rows.len());
    let expected = ProgramDescr {
        program_id: res.0,
        name: "test1".to_string(),
        description: "program desc".to_string(),
        version: res.1,
        status: ProgramStatus::None,
        schema: None,
    };
    let actual = rows.get(0).unwrap();
    assert_eq!(&expected, actual);
}

#[tokio::test]
async fn duplicate_program() {
    let handle = test_setup().await;
    let _ = handle
        .db
        .new_program(Uuid::now_v7(), "test1", "program desc", "ignored")
        .await;
    let res = handle
        .db
        .new_program(Uuid::now_v7(), "test1", "program desc", "ignored")
        .await
        .expect_err("Expecting unique violation");
    let expected = anyhow::anyhow!(DBError::DuplicateName);
    assert_eq!(format!("{}", res), format!("{}", expected));
}

#[tokio::test]
async fn program_reset() {
    let handle = test_setup().await;
    handle
        .db
        .new_program(Uuid::now_v7(), "test1", "program desc", "ignored")
        .await
        .unwrap();
    handle
        .db
        .new_program(Uuid::now_v7(), "test2", "program desc", "ignored")
        .await
        .unwrap();
    handle.db.reset_program_status().await.unwrap();
    let results = handle.db.list_programs().await.unwrap();
    for p in results {
        assert_eq!(ProgramStatus::None, p.status);
        assert_eq!(None, p.schema); //can't check for error fields directly
    }
    let results = handle
        .db
        .pool
        .get()
        .await
        .unwrap()
        .query(
            "SELECT * FROM program WHERE status != '' OR error != '' OR schema != ''",
            &[],
        )
        .await;
    assert_eq!(0, results.unwrap().len());
}

#[tokio::test]
async fn program_code() {
    let handle = test_setup().await;
    let (program_id, _) = handle
        .db
        .new_program(
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let results = handle.db.program_code(program_id).await.unwrap();
    assert_eq!("test1", results.0.name);
    assert_eq!("program desc", results.0.description);
    assert_eq!("create table t1(c1 integer);".to_owned(), results.1);
}

#[tokio::test]
async fn update_program() {
    let handle = test_setup().await;
    let (program_id, _) = handle
        .db
        .new_program(
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
            program_id,
            "test2",
            "different desc",
            &Some("create table t2(c2 integer);".to_string()),
        )
        .await;
    let (descr, code) = handle.db.program_code(program_id).await.unwrap();
    assert_eq!("test2", descr.name);
    assert_eq!("different desc", descr.description);
    assert_eq!("create table t2(c2 integer);", code);

    let _ = handle
        .db
        .update_program(program_id, "updated_test1", "some new description", &None)
        .await;
    let results = handle.db.list_programs().await.unwrap();
    assert_eq!(1, results.len());
    let row = results.get(0).unwrap();
    assert_eq!("updated_test1", row.name);
    assert_eq!("some new description", row.description);
}

#[tokio::test]
async fn program_queries() {
    let handle = test_setup().await;
    let (program_id, _) = handle
        .db
        .new_program(
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_if_exists(program_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle.db.lookup_program("test1").await.unwrap().unwrap();
    assert_eq!("test1", desc.name);
    let desc = handle.db.lookup_program("test2").await.unwrap();
    assert!(desc.is_none());
}

#[tokio::test]
async fn program_config() {
    let handle = test_setup().await;
    let connector_id = handle
        .db
        .new_connector(Uuid::now_v7(), "a", "b", "c")
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_id,
        config: "".to_string(),
    };
    let _ = handle
        .db
        .new_pipeline(Uuid::now_v7(), None, "1", "2", "3", &Some(vec![ac.clone()]))
        .await
        .unwrap();
    let res = handle.db.list_pipelines().await.unwrap();
    assert_eq!(1, res.len());
    let config = res.get(0).unwrap();
    assert_eq!("1", config.name);
    assert_eq!("2", config.description);
    assert_eq!("3", config.config);
    assert_eq!(None, config.program_id);
    let connectors = &config.attached_connectors;
    assert_eq!(1, connectors.len());
    let ac_ret = connectors.get(0).unwrap().clone();
    assert_eq!(ac, ac_ret);
}

#[tokio::test]
async fn project_pending() {
    let handle = test_setup().await;
    let (uid1, _v) = handle
        .db
        .new_program(Uuid::now_v7(), "test1", "project desc", "ignored")
        .await
        .unwrap();
    let (uid2, _v) = handle
        .db
        .new_program(Uuid::now_v7(), "test2", "project desc", "ignored")
        .await
        .unwrap();

    handle
        .db
        .set_program_status(uid2, ProgramStatus::Pending)
        .await
        .unwrap();
    handle
        .db
        .set_program_status(uid1, ProgramStatus::Pending)
        .await
        .unwrap();
    let (id, _version) = handle.db.next_job().await.unwrap().unwrap();
    assert_eq!(id, uid2);
    let (id, _version) = handle.db.next_job().await.unwrap().unwrap();
    assert_eq!(id, uid2);
    // Maybe next job should set the status to something else
    // so it won't get picked up twice?
}

#[tokio::test]
async fn update_status() {
    let handle = test_setup().await;
    let (program_id, _) = handle
        .db
        .new_program(
            Uuid::now_v7(),
            "test1",
            "program desc",
            "create table t1(c1 integer);",
        )
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_if_exists(program_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ProgramStatus::None, desc.status);
    handle
        .db
        .set_program_status(program_id, ProgramStatus::CompilingRust)
        .await
        .unwrap();
    let desc = handle
        .db
        .get_program_if_exists(program_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ProgramStatus::CompilingRust, desc.status);
}

#[tokio::test]
async fn duplicate_attached_conn_name() {
    let handle = test_setup().await;
    let connector_id = handle
        .db
        .new_connector(Uuid::now_v7(), "a", "b", "c")
        .await
        .unwrap();
    let ac = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_id,
        config: "".to_string(),
    };
    let ac2 = AttachedConnector {
        name: "foo".to_string(),
        is_input: true,
        connector_id,
        config: "".to_string(),
    };
    let _ = handle
        .db
        .new_pipeline(Uuid::now_v7(), None, "1", "2", "3", &Some(vec![ac, ac2]))
        .await
        .expect_err("duplicate attached connector name");
}

#[tokio::test]
async fn save_api_key() {
    let handle = test_setup().await;
    // Attempt several key generations and validations
    for _ in 1..10 {
        let api_key = auth::generate_api_key();
        handle
            .db
            .store_api_key_hash(
                api_key.clone(),
                vec![ApiPermission::Read, ApiPermission::Write],
            )
            .await
            .unwrap();
        let scopes = handle.db.validate_api_key(api_key.clone()).await.unwrap();
        assert_eq!(&ApiPermission::Read, scopes.get(0).unwrap());
        assert_eq!(&ApiPermission::Write, scopes.get(1).unwrap());

        let api_key_2 = auth::generate_api_key();
        let failure = handle.db.validate_api_key(api_key_2).await.unwrap_err();
        let err = failure.downcast_ref::<DBError>();
        assert!(matches!(err, Some(DBError::InvalidKey)));
    }
}

/// Generate uuids but limits the the randomess to the first 8 bytes.
///
/// This ensures that we have a good chance of generating a uuid that is already
/// in the database -- useful for testing error conditions.
pub(crate) fn limited_uuid() -> impl Strategy<Value = Uuid> {
    vec![any::<u8>()].prop_map(|mut bytes| {
        // prepend a bunch of zero bytes so the buffer is big enough for
        // building a uuid
        bytes.resize(16, 0);
        // restrict the any::<u8> (0..255) to 1..16 this enforces more
        // interesting scenarios for testing (and we start at 1 because shaving
        bytes[0] &= 0b1111;
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

/// Actions we can do on the Storage trait.
#[derive(Debug, Clone, Arbitrary)]
enum StorageAction {
    ResetProgramStatus,
    ListPrograms,
    ProgramCode(ProgramId),
    NewProgram(
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        String,
    ),
    UpdateProgram(ProgramId, String, String, Option<String>),
    GetProgramIfExists(ProgramId),
    LookupProgram(String),
    SetProgramStatus(ProgramId, ProgramStatus),
    SetProgramStatusGuarded(ProgramId, Version, ProgramStatus),
    SetProgramSchema(ProgramId, String),
    DeleteProgram(ProgramId),
    NextJob,
    NewPipeline(
        #[proptest(strategy = "limited_uuid()")] Uuid,
        Option<ProgramId>,
        String,
        String,
        String,
        Option<Vec<AttachedConnector>>,
    ),
    UpdatePipeline(
        PipelineId,
        Option<ProgramId>,
        String,
        String,
        Option<String>,
        Option<Vec<AttachedConnector>>,
    ),
    PipelineSetDeployed(PipelineId, u16),
    SetPipelineStatus(PipelineId, PipelineStatus),
    DeletePipeline(PipelineId),
    GetPipelineById(PipelineId),
    GetPipelineByName(String),
    ListPipelines,
    NewConnector(
        #[proptest(strategy = "limited_uuid()")] Uuid,
        String,
        String,
        String,
    ),
    ListConnectors,
    GetConnectorById(ConnectorId),
    GetConnectorByName(String),
    UpdateConnector(ConnectorId, String, String, Option<String>),
    DeleteConnector(ConnectorId),
    StoreApiKeyHash(String, Vec<ApiPermission>),
    ValidateApiKey(String),
}

fn check_responses<T: Debug + PartialEq>(step: usize, model: AnyResult<T>, impl_: AnyResult<T>) {
    match (model, impl_) {
        (Ok(mr), Ok(ir)) => assert_eq!(mr, ir),
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
fn compare_pipeline(step: usize, model: AnyResult<PipelineDescr>, impl_: AnyResult<PipelineDescr>) {
    match (model, impl_) {
        (Ok(mut mr), Ok(mut ir)) => {
            mr.created = None;
            ir.created = None;
            assert_eq!(mr, ir);
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

/// Compare everything except tne `created` field which gets set inside the DB.
fn compare_pipelines(
    mut model_response: Vec<PipelineDescr>,
    mut impl_response: Vec<PipelineDescr>,
) {
    assert_eq!(
        model_response
            .iter_mut()
            .map(|p| p.created = None)
            .collect::<Vec<_>>(),
        impl_response
            .iter_mut()
            .map(|p| p.created = None)
            .collect::<Vec<_>>()
    );
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
                            StorageAction::ResetProgramStatus => {
                                let model_response = model.reset_program_status().await;
                                let impl_response = handle.db.reset_program_status().await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListPrograms => {
                                let model_response = model.list_programs().await.unwrap();
                                let mut impl_response = handle.db.list_programs().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.program_id.cmp(&b.program_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::ProgramCode(program_id) => {
                                let model_response = model.program_code(program_id).await;
                                let impl_response = handle.db.program_code(program_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NewProgram(id, name, description, code) => {
                                let model_response =
                                    model.new_program(id, &name, &description, &code).await;
                                let impl_response =
                                    handle.db.new_program(id, &name, &description, &code).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateProgram(program_id, name, description, code) => {
                                let model_response = model
                                    .update_program(program_id, &name, &description, &code)
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_program(program_id, &name, &description, &code)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetProgramIfExists(program_id) => {
                                let model_response = model.get_program_if_exists(program_id).await;
                                let impl_response =
                                    handle.db.get_program_if_exists(program_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::LookupProgram(name) => {
                                let model_response = model.lookup_program(&name).await;
                                let impl_response = handle.db.lookup_program(&name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProgramStatus(program_id, status) => {
                                let model_response =
                                    model.set_program_status(program_id, status.clone()).await;
                                let impl_response =
                                    handle.db.set_program_status(program_id, status.clone()).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProgramStatusGuarded(program_id, version, status) => {
                                let model_response = model
                                    .set_program_status_guarded(program_id, version, status.clone())
                                    .await;
                                let impl_response = handle
                                    .db
                                    .set_program_status_guarded(program_id, version, status.clone())
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetProgramSchema(program_id, schema) => {
                                let model_response =
                                    model.set_program_schema(program_id, schema.clone()).await;
                                let impl_response =
                                    handle.db.set_program_schema(program_id, schema).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteProgram(program_id) => {
                                let model_response = model.delete_program(program_id).await;
                                let impl_response = handle.db.delete_program(program_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::NextJob => {
                                let model_response = model.next_job().await;
                                let impl_response = handle.db.next_job().await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineById(pipeline_id) => {
                                let model_response = model.get_pipeline_by_id(pipeline_id).await;
                                let impl_response = handle.db.get_pipeline_by_id(pipeline_id).await;
                                compare_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::GetPipelineByName(name) => {
                                let model_response = model.get_pipeline_by_name(name.clone()).await;
                                let impl_response = handle.db.get_pipeline_by_name(name).await;
                                compare_pipeline(i, model_response, impl_response);
                            }
                            StorageAction::ListPipelines => {
                                let model_response = model.list_pipelines().await.unwrap();
                                let mut impl_response = handle.db.list_pipelines().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.pipeline_id.cmp(&b.pipeline_id));
                                compare_pipelines(model_response, impl_response);
                            }
                            StorageAction::NewPipeline(id, program_id, name, description, config, connectors) => {
                                let model_response =
                                    model.new_pipeline(id, program_id, &name, &description, &config, &connectors.clone()).await;
                                let impl_response =
                                    handle.db.new_pipeline(id, program_id, &name, &description, &config, &connectors).await;
                                    check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdatePipeline(pipeline_id, program_id, name, description, config, connectors) => {
                                let model_response = model
                                    .update_pipeline(pipeline_id, program_id, &name, &description, &config, &connectors.clone())
                                    .await;
                                let impl_response = handle
                                    .db
                                    .update_pipeline(pipeline_id, program_id, &name, &description, &config, &connectors)
                                    .await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::PipelineSetDeployed(pipeline_id, port) => {
                                let model_response = model.set_pipeline_deployed(pipeline_id, port).await;
                                let impl_response = handle.db.set_pipeline_deployed(pipeline_id, port).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::SetPipelineStatus(pipeline_id, status) => {
                                let model_response = model.set_pipeline_status(pipeline_id, status).await;
                                let impl_response = handle.db.set_pipeline_status(pipeline_id, status).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeletePipeline(pipeline_id) => {
                                let model_response = model.delete_pipeline(pipeline_id).await;
                                let impl_response = handle.db.delete_pipeline(pipeline_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::ListConnectors => {
                                let model_response = model.list_connectors().await.unwrap();
                                let mut impl_response = handle.db.list_connectors().await.unwrap();
                                // Impl does not guarantee order of rows returned by SELECT
                                impl_response.sort_by(|a, b| a.connector_id.cmp(&b.connector_id));
                                assert_eq!(model_response, impl_response);
                            }
                            StorageAction::NewConnector(id, name, description, config) => {
                                let model_response =
                                    model.new_connector(id, &name, &description, &config).await;
                                let impl_response =
                                    handle.db.new_connector(id, &name, &description, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetConnectorById(connector_id) => {
                                let model_response = model.get_connector_by_id(connector_id).await;
                                let impl_response = handle.db.get_connector_by_id(connector_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::GetConnectorByName(name) => {
                                let model_response = model.get_connector_by_name(name.clone()).await;
                                let impl_response = handle.db.get_connector_by_name(name).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::UpdateConnector(connector_id, name, description, config) => {
                                let model_response =
                                    model.update_connector(connector_id, &name, &description, &config).await;
                                let impl_response =
                                    handle.db.update_connector(connector_id, &name, &description, &config).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::DeleteConnector(connector_id) => {
                                let model_response = model.delete_connector(connector_id).await;
                                let impl_response = handle.db.delete_connector(connector_id).await;
                                check_responses(i, model_response, impl_response);
                            }
                            StorageAction::StoreApiKeyHash(key, permissions) => {
                                let model_response = model.store_api_key_hash(key.clone(), permissions.clone()).await;
                                let impl_response = handle.db.store_api_key_hash(key.clone(), permissions.clone()).await;
                                check_responses(i, model_response, impl_response);
                            },
                            StorageAction::ValidateApiKey(key) => {
                                let model_response = model.validate_api_key(key.clone()).await;
                                let impl_response = handle.db.validate_api_key(key.clone()).await;
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

/// Our model of the database (uses btrees for tables).
#[derive(Debug, Default)]
struct DbModel {
    // `programs` Format is: (program, code, created)
    pub programs: BTreeMap<ProgramId, (ProgramDescr, String, SystemTime)>,
    pub pipelines: BTreeMap<PipelineId, PipelineDescr>,
    pub api_keys: BTreeMap<String, Vec<ApiPermission>>,
    pub connectors: BTreeMap<ConnectorId, ConnectorDescr>,
}

#[async_trait]
impl Storage for Mutex<DbModel> {
    async fn reset_program_status(&self) -> anyhow::Result<()> {
        self.lock()
            .await
            .programs
            .values_mut()
            .for_each(|(p, _, _e)| {
                p.status = ProgramStatus::None;
                p.schema = None;
            });

        Ok(())
    }

    async fn list_programs(&self) -> anyhow::Result<Vec<ProgramDescr>> {
        Ok(self
            .lock()
            .await
            .programs
            .values()
            .map(|(p, _, _)| p.clone())
            .collect())
    }

    async fn program_code(
        &self,
        program_id: super::ProgramId,
    ) -> anyhow::Result<(ProgramDescr, String)> {
        self.lock()
            .await
            .programs
            .get(&program_id)
            .map(|(p, c, _e)| (p.clone(), c.clone()))
            .ok_or(anyhow::anyhow!(DBError::UnknownProgram(program_id)))
    }

    async fn new_program(
        &self,
        id: Uuid,
        program_name: &str,
        program_description: &str,
        program_code: &str,
    ) -> anyhow::Result<(super::ProgramId, super::Version)> {
        let mut s = self.lock().await;
        if s.programs.keys().any(|k| k.0 == id) {
            return Err(anyhow::anyhow!(DBError::UniqueKeyViolation("program_pkey")));
        }
        if s.programs.values().any(|(p, _, _)| p.name == program_name) {
            return Err(anyhow::anyhow!(DBError::DuplicateName));
        }

        let program_id = ProgramId(id);
        let version = Version(1);

        s.programs.insert(
            program_id,
            (
                ProgramDescr {
                    program_id,
                    name: program_name.to_owned(),
                    description: program_description.to_owned(),
                    status: ProgramStatus::None,
                    schema: None,
                    version,
                },
                program_code.to_owned(),
                SystemTime::now(),
            ),
        );

        Ok((program_id, version))
    }

    async fn update_program(
        &self,
        program_id: super::ProgramId,
        program_name: &str,
        program_description: &str,
        program_code: &Option<String>,
    ) -> anyhow::Result<super::Version> {
        let mut s = self.lock().await;
        if !s.programs.contains_key(&program_id) {
            return Err(anyhow::anyhow!(DBError::UnknownProgram(program_id)));
        }

        if s.programs
            .values()
            .any(|(p, _, _)| p.name == program_name && p.program_id != program_id)
        {
            return Err(anyhow::anyhow!(DBError::DuplicateName));
        }

        s.programs
            .get_mut(&program_id)
            .map(|(p, cur_code, _e)| {
                p.name = program_name.to_owned();
                p.description = program_description.to_owned();
                if let Some(code) = program_code {
                    if code != cur_code {
                        *cur_code = code.to_owned();
                        p.version.0 += 1;
                        p.schema = None;
                        p.status = ProgramStatus::None;
                    }
                }
                p.version
            })
            .ok_or(anyhow::anyhow!(DBError::UnknownProgram(program_id)))
    }

    async fn get_program_if_exists(
        &self,
        program_id: super::ProgramId,
    ) -> anyhow::Result<Option<ProgramDescr>> {
        Ok(self
            .lock()
            .await
            .programs
            .get(&program_id)
            .map(|(p, _, _)| p.clone()))
    }

    async fn lookup_program(&self, program_name: &str) -> anyhow::Result<Option<ProgramDescr>> {
        Ok(self
            .lock()
            .await
            .programs
            .values()
            .find(|(p, _, _)| p.name == program_name)
            .map(|(p, _, _)| p.clone()))
    }

    async fn set_program_status(
        &self,
        program_id: super::ProgramId,
        status: ProgramStatus,
    ) -> anyhow::Result<()> {
        let _r = self
            .lock()
            .await
            .programs
            .get_mut(&program_id)
            .map(|(p, _, t)| {
                p.status = status;
                *t = SystemTime::now();
                // TODO: It's a bit odd that this function also resets the schema
                p.schema = Some("".to_string());
            });

        Ok(())
    }

    async fn set_program_status_guarded(
        &self,
        program_id: super::ProgramId,
        expected_version: super::Version,
        status: ProgramStatus,
    ) -> anyhow::Result<()> {
        self.lock()
            .await
            .programs
            .get_mut(&program_id)
            .map(|(p, _, t)| {
                if p.version == expected_version {
                    p.status = status;
                    *t = SystemTime::now();
                }
            })
            .ok_or(anyhow::anyhow!(DBError::UnknownProgram(program_id)))
    }

    async fn set_program_schema(
        &self,
        program_id: super::ProgramId,
        schema: String,
    ) -> anyhow::Result<()> {
        let _r = self
            .lock()
            .await
            .programs
            .get_mut(&program_id)
            .map(|(p, _, _)| {
                p.schema = Some(schema);
            });

        Ok(())
    }

    async fn delete_program(&self, program_id: super::ProgramId) -> anyhow::Result<()> {
        let mut s = self.lock().await;

        s.programs
            .remove(&program_id)
            .map(|_| ())
            .ok_or(anyhow::anyhow!(DBError::UnknownProgram(program_id)))?;
        // Foreign key delete:
        s.pipelines.retain(|_, c| c.program_id != Some(program_id));

        Ok(())
    }

    async fn next_job(&self) -> anyhow::Result<Option<(super::ProgramId, super::Version)>> {
        let s = self.lock().await;
        let mut values = Vec::from_iter(s.programs.values());
        values.sort_by(|(_, _, t1), (_, _, t2)| t1.cmp(t2));

        values
            .iter()
            .find(|(p, _, _)| p.status == ProgramStatus::Pending)
            .map(|(p, _, _)| Ok(Some((p.program_id, p.version))))
            .unwrap_or(Ok(None))
    }

    async fn new_pipeline(
        &self,
        id: Uuid,
        program_id: Option<super::ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &str,
        // TODO: not clear why connectors is an option here
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> anyhow::Result<(super::PipelineId, super::Version)> {
        let mut s = self.lock().await;
        let db_connectors = s.connectors.clone();

        if s.pipelines.keys().any(|k| k.0 == id) {
            return Err(anyhow::anyhow!(DBError::UniqueKeyViolation(
                "pipeline_pkey"
            )));
        }
        // UNIQUE constraint on name
        if s.pipelines.values().any(|c| c.name == pipline_name) {
            return Err(DBError::DuplicateName.into());
        }
        // Model the foreign key constraint on `program_id`
        if let Some(program_id) = program_id {
            if !s.programs.contains_key(&program_id) {
                return Err(anyhow::anyhow!(DBError::UnknownProgram(program_id)));
            }
        }

        let mut new_acs: Vec<AttachedConnector> = vec![];
        if let Some(connectors) = connectors {
            for ac in connectors {
                if new_acs.iter().any(|nac| nac.name == ac.name) {
                    return Err(anyhow::anyhow!(DBError::DuplicateName));
                }

                // We ensure that in all pipelines there is no attached
                // connector with this name UNIQUE constraint on table
                let pipelines = s.pipelines.values().clone();
                if pipelines
                    .flat_map(|p| p.attached_connectors.clone())
                    .collect::<Vec<_>>()
                    .iter()
                    .any(|eac| eac.name == ac.name)
                {
                    return Err(anyhow::anyhow!(DBError::DuplicateName));
                }

                // Check that all attached connectors point to a valid
                // connector_id
                if !db_connectors.contains_key(&ac.connector_id) {
                    return Err(anyhow::anyhow!(DBError::UnknownConnector(ac.connector_id)));
                }

                new_acs.push(ac.clone());
            }
        }

        let pipeline_id = PipelineId(id);
        let version = Version(1);
        s.pipelines.insert(
            pipeline_id,
            PipelineDescr {
                pipeline_id,
                program_id,
                name: pipline_name.to_owned(),
                description: pipeline_description.to_owned(),
                config: config.to_owned(),
                attached_connectors: new_acs,
                version: Version(1),
                port: 0,
                created: None,
                status: PipelineStatus::Shutdown,
            },
        );

        Ok((pipeline_id, version))
    }

    async fn update_pipeline(
        &self,
        pipeline_id: PipelineId,
        program_id: Option<ProgramId>,
        pipline_name: &str,
        pipeline_description: &str,
        config: &Option<String>,
        connectors: &Option<Vec<AttachedConnector>>,
    ) -> anyhow::Result<Version> {
        let mut s = self.lock().await;
        let db_connectors = s.connectors.clone();
        let db_programs = s.programs.clone();

        // pipeline must exist
        s.pipelines
            .get_mut(&pipeline_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownPipeline(pipeline_id)))?;
        // UNIQUE constraint on name
        if let Some(c) = s
            .pipelines
            .values()
            .find(|c| c.name == pipline_name)
            .cloned()
        {
            if c.pipeline_id != pipeline_id {
                return Err(DBError::DuplicateName.into());
            }
        }

        let mut new_acs: Vec<AttachedConnector> = vec![];
        if let Some(connectors) = connectors {
            for ac in connectors {
                if new_acs.iter().any(|nac| nac.name == ac.name) {
                    return Err(anyhow::anyhow!(DBError::DuplicateName));
                }

                // We ensure that in all pipelines there is no attached
                // connector with this name UNIQUE constraint on table
                let pipelines = s.pipelines.values().clone();
                if pipelines
                    .flat_map(|p| p.attached_connectors.clone())
                    .collect::<Vec<_>>()
                    .iter()
                    .any(|eac| eac.name == ac.name)
                {
                    return Err(anyhow::anyhow!(DBError::DuplicateName));
                }

                // Check that all attached connectors point to a valid
                // connector_id
                if !db_connectors.contains_key(&ac.connector_id) {
                    return Err(anyhow::anyhow!(DBError::UnknownConnector(ac.connector_id)));
                }

                new_acs.push(ac.clone());
            }
        }
        // Check program exists foreign key constraint
        if let Some(program_id) = program_id {
            if !db_programs.contains_key(&program_id) {
                return Err(anyhow::anyhow!(DBError::UnknownProgram(program_id)));
            }
        }

        let c = s
            .pipelines
            .get_mut(&pipeline_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownPipeline(pipeline_id)))?;

        // Foreign key constraint on `program_id`
        if let Some(program_id) = program_id {
            if db_programs.contains_key(&program_id) {
                c.program_id = Some(program_id);
            } else {
                return Err(anyhow::anyhow!(DBError::UnknownProgram(program_id)));
            }
        } else {
            c.program_id = None;
        }

        c.attached_connectors = new_acs;
        c.name = pipline_name.to_owned();
        c.description = pipeline_description.to_owned();
        c.version = c.version.increment();
        if let Some(config) = config {
            c.config = config.clone();
        }

        Ok(c.version)
    }

    async fn delete_config(&self, pipeline_id: super::PipelineId) -> anyhow::Result<()> {
        self.lock()
            .await
            .pipelines
            .remove(&pipeline_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownPipeline(pipeline_id)))?;

        Ok(())
    }

    async fn attached_connector_is_input(&self, _name: &str) -> anyhow::Result<bool> {
        todo!()
    }

    async fn set_pipeline_deployed(
        &self,
        pipeline_id: super::PipelineId,
        port: u16,
    ) -> anyhow::Result<()> {
        let mut s = self.lock().await;
        let p = s
            .pipelines
            .get_mut(&pipeline_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownPipeline(pipeline_id)))?;

        p.port = port;
        p.status = PipelineStatus::Deployed;

        Ok(())
    }

    async fn set_pipeline_status(
        &self,
        pipeline_id: super::PipelineId,
        status: PipelineStatus,
    ) -> anyhow::Result<bool> {
        let mut s = self.lock().await;

        Ok(s.pipelines
            .get_mut(&pipeline_id)
            .map(|p| {
                p.status = status;
                true
            })
            .unwrap_or(false))
    }

    async fn delete_pipeline(&self, pipeline_id: super::PipelineId) -> anyhow::Result<bool> {
        let mut s = self.lock().await;

        // TODO: Our APIs sometimes are not consistent we return a bool here but
        // other calls fail silently on delete/lookups
        Ok(s.pipelines
            .remove(&pipeline_id)
            .map(|_| true)
            .unwrap_or(false))
    }

    async fn get_pipeline_by_id(
        &self,
        pipeline_id: super::PipelineId,
    ) -> anyhow::Result<super::PipelineDescr> {
        self.lock()
            .await
            .pipelines
            .get(&pipeline_id)
            .cloned()
            .ok_or(anyhow::anyhow!(DBError::UnknownPipeline(pipeline_id)))
    }

    async fn get_pipeline_by_name(&self, name: String) -> anyhow::Result<super::PipelineDescr> {
        self.lock()
            .await
            .pipelines
            .values()
            .find(|p| p.name == name)
            .cloned()
            .ok_or(anyhow::anyhow!(DBError::UnknownName(name)))
    }

    async fn list_pipelines(&self) -> anyhow::Result<Vec<super::PipelineDescr>> {
        Ok(self.lock().await.pipelines.values().cloned().collect())
    }

    async fn new_connector(
        &self,
        id: Uuid,
        name: &str,
        description: &str,
        config: &str,
    ) -> anyhow::Result<super::ConnectorId> {
        let mut s = self.lock().await;
        if s.connectors.keys().any(|k| k.0 == id) {
            return Err(anyhow::anyhow!(DBError::UniqueKeyViolation(
                "connector_pkey"
            )));
        }
        if s.connectors.values().any(|c| c.name == name) {
            return Err(DBError::DuplicateName.into());
        }

        let connector_id = super::ConnectorId(id);
        s.connectors.insert(
            connector_id,
            ConnectorDescr {
                connector_id,
                name: name.to_owned(),
                description: description.to_owned(),
                config: config.to_owned(),
            },
        );
        Ok(connector_id)
    }

    async fn list_connectors(&self) -> anyhow::Result<Vec<ConnectorDescr>> {
        Ok(self.lock().await.connectors.values().cloned().collect())
    }

    async fn get_connector_by_id(
        &self,
        connector_id: super::ConnectorId,
    ) -> anyhow::Result<ConnectorDescr> {
        self.lock()
            .await
            .connectors
            .get(&connector_id)
            .cloned()
            .ok_or(anyhow::anyhow!(DBError::UnknownConnector(connector_id)))
    }

    async fn get_connector_by_name(&self, name: String) -> anyhow::Result<ConnectorDescr> {
        self.lock()
            .await
            .connectors
            .values()
            .find(|c| c.name == name)
            .cloned()
            .ok_or(anyhow::anyhow!(DBError::UnknownName(name)))
    }

    async fn update_connector(
        &self,
        connector_id: super::ConnectorId,
        connector_name: &str,
        description: &str,
        config: &Option<String>,
    ) -> anyhow::Result<()> {
        let mut s = self.lock().await;
        // `connector_id` needs to exist
        if s.connectors.get(&connector_id).is_none() {
            return Err(DBError::UnknownConnector(connector_id).into());
        }
        // UNIQUE constraint on name
        if let Some(c) = s
            .connectors
            .values()
            .find(|c| c.name == connector_name)
            .cloned()
        {
            if c.connector_id != connector_id {
                return Err(DBError::DuplicateName.into());
            }
        }

        let c = s
            .connectors
            .get_mut(&connector_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownConnector(connector_id)))?;
        c.name = connector_name.to_owned();
        c.description = description.to_owned();
        if let Some(config) = config {
            c.config = config.clone();
        }
        Ok(())
    }

    async fn delete_connector(&self, connector_id: super::ConnectorId) -> anyhow::Result<()> {
        let mut s = self.lock().await;
        s.connectors
            .remove(&connector_id)
            .ok_or(anyhow::anyhow!(DBError::UnknownConnector(connector_id)))?;
        s.pipelines.values_mut().for_each(|c| {
            c.attached_connectors
                .retain(|c| c.connector_id != connector_id);
        });
        Ok(())
    }

    async fn store_api_key_hash(
        &self,
        key: String,
        permissions: Vec<ApiPermission>,
    ) -> AnyResult<()> {
        let mut s = self.lock().await;
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        if let btree_map::Entry::Vacant(e) = s.api_keys.entry(hash) {
            e.insert(permissions);
            Ok(())
        } else {
            Err(anyhow::anyhow!(DBError::DuplicateKey))
        }
    }

    async fn validate_api_key(&self, key: String) -> AnyResult<Vec<ApiPermission>> {
        let s = self.lock().await;
        let mut hasher = sha::Sha256::new();
        hasher.update(key.as_bytes());
        let hash = openssl::base64::encode_block(&hasher.finish());
        let record = s.api_keys.get(&hash);
        match record {
            Some(record) => Ok(record.clone()),
            None => Err(anyhow::anyhow!(DBError::InvalidKey)),
        }
    }
}
